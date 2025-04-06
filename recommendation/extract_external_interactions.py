import csv
import pandas as pd
import psycopg
from common.utils.utils import DB_CONFIG, letterboxd_interactions

RAW_PATH = "./data/external_interactions_raw.csv"
FINAL_PATH = "./data/external_interactions_transformed.csv"


def dump_letterboxd_interactions_to_csv():
    cursor = letterboxd_interactions.find(
        {},
        {
            "_id": 0,
            "letterboxd_movie_id": 1,
            "interaction_type": 1,
            "rating_val": 1,
            "letterboxd_username": 1,
        },
    )

    with open(RAW_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "letterboxd_username",
                "letterboxd_movie_id",
                "interaction_type",
                "rating_val",
            ],
        )
        writer.writeheader()
        for doc in cursor:
            writer.writerow(doc)


def load_letterboxd_to_movie_id_map():
    with psycopg.connect(**DB_CONFIG, row_factory=psycopg.rows.dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id AS movie_id, letterboxd_movie_id FROM movies WHERE letterboxd_movie_id IS NOT NULL"
            )
            return {
                row["letterboxd_movie_id"]: row["movie_id"] for row in cur.fetchall()
            }


def transform_external_csv():
    lb_map = load_letterboxd_to_movie_id_map()
    chunk_iter = pd.read_csv(RAW_PATH, chunksize=100_000)

    with open(FINAL_PATH, "w", newline="", encoding="utf-8") as out_file:
        writer = csv.DictWriter(
            out_file, fieldnames=["user_id", "movie_id", "interaction_type", "rating"]
        )
        writer.writeheader()

        for chunk in chunk_iter:
            chunk["movie_id"] = chunk["letterboxd_movie_id"].map(lb_map)
            chunk = chunk[chunk["movie_id"].notnull()]
            chunk["user_id"] = "lb_" + chunk["letterboxd_username"]
            chunk = chunk[
                ["user_id", "movie_id", "interaction_type", "rating_val"]
            ].rename(columns={"rating_val": "rating"})
            chunk.to_csv(out_file, mode="a", index=False, header=False)


def run_pipeline():
    print("🟡 Dumping MongoDB to CSV...")
    dump_letterboxd_interactions_to_csv()

    print("🔄 Transforming CSV with internal movie_id...")
    transform_external_csv()

    print("✅ External interactions updated.")


if __name__ == "__main__":
    run_pipeline()
