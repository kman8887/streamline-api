from concurrent.futures import ThreadPoolExecutor
import datetime
import pandas as pd
import psycopg
from psycopg.rows import dict_row
from pymongo import UpdateOne
from tqdm import tqdm
from psycopg.types.json import Jsonb
from concurrent.futures import ThreadPoolExecutor, as_completed
from common.utils.utils import DB_CONFIG, time_it, user_recommendations


@time_it
def store_predictions(predicted_df: pd.DataFrame):
    predicted_df["cf_score"] = predicted_df["cf_score"].round(9)
    predicted_df["content_score"] = predicted_df["content_score"].round(9)
    predicted_df["final_score"] = predicted_df["final_score"].round(9)

    predicted_df.reset_index

    internal_preds = predicted_df[predicted_df["user_id"].str.isnumeric()]
    external_preds = predicted_df[predicted_df["user_id"].str.startswith("lb_")]

    __save_internal_predictions_to_postgres(internal_preds)

    # Save external (optional)
    if not external_preds.empty:
        __save_external_recommendations_to_mongo_grouped(external_preds)


@time_it
def __save_internal_predictions_to_postgres(df: pd.DataFrame):
    with psycopg.connect(**DB_CONFIG, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            batch = [
                (
                    int(row["user_id"]),
                    row["movie_id"],
                    row["final_score"],
                    row["cf_score"],
                    row["content_score"],
                    build_hybrid_explanation(
                        row["cf_score"], row.get("explanation", [])
                    ),
                    datetime.datetime.now(tz=datetime.timezone.utc),
                )
                for _, row in df.iterrows()
            ]

            cur.executemany(
                """
                INSERT INTO user_recommendations (
                    user_id, movie_id, predicted_score, cf_score, content_score, explanation, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, movie_id)
                DO UPDATE SET 
                    predicted_score = EXCLUDED.predicted_score,
                    cf_score = EXCLUDED.cf_score,
                    content_score = EXCLUDED.content_score,
                    explanation = EXCLUDED.explanation,
                    updated_at = EXCLUDED.updated_at;
                """,
                batch,
            )
        conn.commit()

    print(f"PostgreSQL: {len(df)} rows modified")


def build_hybrid_explanation(
    cf_score: float, content_features: list[dict] | None
) -> Jsonb | None:
    """
    Builds a hybrid explanation combining CF and content-based insights.
    """
    if not content_features:
        return None

    # Collaborative filtering explanation
    if cf_score != -999:
        cf_msg = f"Based on collaborative filtering — similar users liked this movie (score: {cf_score:.2f})"
    else:
        cf_msg = "Collaborative filtering data was not available; recommendation is based on movie features"

    # Build top features (limit + fallback)
    summary = "Based on your preference for: " + ", ".join(
        f.get("feature", "N/A") for f in content_features[:3]
    )

    return Jsonb(
        {
            "cf": cf_msg,
            "content": {"summary": summary, "top_features": content_features},
        }
    )


@time_it
def __save_external_recommendations_to_mongo_grouped(
    df: pd.DataFrame, batch_size=10000, max_workers=8
):
    total_modified = 0
    total_inserted = 0

    batches = [df.iloc[i : i + batch_size] for i in range(0, len(df), batch_size)]

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(write_batch, batch) for batch in batches]
        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Writing to MongoDB"
        ):
            inserted, modified = future.result()
            total_inserted += inserted
            total_modified += modified

    print(f"Mongo (parallel): {total_inserted} inserted, {total_modified} updated.")


def write_batch(batch_df: pd.DataFrame):
    ops = []
    now = datetime.datetime.now(tz=datetime.timezone.utc)

    for row in batch_df.itertuples(index=False):
        ops.append(
            UpdateOne(
                {"user_id": row.user_id, "movie_id": row.movie_id},
                {
                    "$set": {
                        "final_score": row.final_score,
                        "cf_score": row.cf_score,
                        "content_score": row.content_score,
                        "updated_at": now,
                    }
                },
                upsert=True,
            )
        )
    if ops:
        result = user_recommendations.bulk_write(ops, ordered=False)
        return result.upserted_count, result.modified_count
    return 0, 0
