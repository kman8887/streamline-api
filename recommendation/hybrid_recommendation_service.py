from typing import List
import numpy as np
import pandas as pd
from common.utils import azure_blob
from common.utils.utils import time_it
from movies.movies_service import get_movies_metadata
from recommendation import (
    recommendation_storing_service,
    content_based_filtering_service,
    collaborative_filtering_service,
    ratings_matrix as rating_matrix_service,
)
from recommendation.evaluation import evaluate_topk_metrics
from dotenv import load_dotenv

load_dotenv()


@time_it
def get_hybrid_filtering():
    split_for_evaluation = False
    (
        raw_ratings_sparse,
        centered_ratings_sparse,
        user_id_lookup,
        movie_id_lookup,
        test_df,
    ) = rating_matrix_service.create_ratings_matrix(
        split_for_evaluation=split_for_evaluation, k=1
    )  # shape sparse array [n_users x n_movies]; {user_id: row_index}; {movie_id: col_index}

    movies_metadata = pd.DataFrame(get_movies_metadata())

    cf_df, movie_features = (
        collaborative_filtering_service.get_collaborative_filtering_model(
            centered_ratings_sparse, user_id_lookup, movie_id_lookup
        )
    )

    if not split_for_evaluation:
        # Only use internal users (numeric user_ids) for content-based filtering
        internal_user_ids = [uid for uid in user_id_lookup if uid.isnumeric()]
        internal_user_indices = [user_id_lookup[uid] for uid in internal_user_ids]

        raw_ratings_sparse = raw_ratings_sparse[internal_user_indices]
        user_id_lookup = {uid: i for i, uid in enumerate(internal_user_ids)}

    cbf_df, tfidf_vectorizer, tfidf_matrix, tfidf_movie_id_to_index = (
        content_based_filtering_service.get_content_based_filtering_model(
            raw_ratings_sparse, user_id_lookup, movie_id_lookup, movies_metadata
        )
    )

    cbf_df["user_id"] = cbf_df["user_id"].astype(str)
    cf_df["user_id"] = cf_df["user_id"].astype(str)

    cbf_df["movie_id"] = cbf_df["movie_id"].astype(str)
    cf_df["movie_id"] = cf_df["movie_id"].astype(str)

    print(cbf_df["content_score"].describe())
    print("---------------------------------------------------")
    print(cf_df["cf_score"].describe())

    hybrid_df = merge_scores(cbf_df, cf_df)

    hybrid_df = normalize_per_user(hybrid_df, ["content_score", "cf_score"])

    print(hybrid_df["content_score"].describe())
    print("---------------------------------------------------")
    print(hybrid_df["cf_score"].describe())

    hybrid_df = compute_hybrid_scores(hybrid_df)

    print("---------------------------------------------------")
    print(hybrid_df["raw_final_score"].describe())

    hybrid_df = apply_quality_boost(hybrid_df, movies_metadata)

    print("---------------------------------------------------")
    print(hybrid_df["final_score"].describe())

    # hybrid_df["quality_boost_final_score"] = hybrid_df["final_score"]

    # hybrid_df = normalize_per_user(hybrid_df, ["final_score"])

    print("---------------------------------------------------")
    print(hybrid_df["final_score"].describe())

    if test_df is not None:
        metrics = evaluate_topk_metrics(hybrid_df, test_df, k=10)
        print("Evaluation Metrics:")
        for metric, value in metrics.items():
            print(f"   {metric}: {value}")

    recommendation_storing_service.store_predictions(hybrid_df)

    item_feature_matrix = tfidf_matrix.toarray()

    baseline_recs = content_based_filtering_service.build_baseline_recs(
        cbf_df
    )  # Top movies overall or diverse

    azure_blob.save_all_artifacts(
        tfidf_vectorizer=tfidf_vectorizer,
        item_feature_matrix=item_feature_matrix,
        item_feature_matrix_movie_id_lookup=tfidf_movie_id_to_index,
        movie_features=movie_features,
        movie_features_movie_id_lookup=movie_id_lookup,
        baseline_recs=baseline_recs,
        movies_metadata=movies_metadata,
    )


@time_it
def generate_user_hybrid_recommendations(user_id: str):
    print(f"Generating hybrid recs for user: {user_id}")
    artifacts = azure_blob.load_artifacts()

    raw_ratings, centered_ratings = rating_matrix_service.get_rating_matrix_for_user(
        user_id
    )  # Dict of {movie_id: rating}

    if len(raw_ratings) < 5:
        raise ValueError("Not enough ratings to generate recommendations.")

    content_scores = content_based_filtering_service.get_new_user_content_score(
        raw_ratings, user_id, artifacts
    )
    cf_scores = collaborative_filtering_service.get_new_user_cf_scores(
        centered_ratings, user_id, artifacts
    )  # shape: {movie_id: cf_score}

    hybrid_df = merge_scores(content_scores, cf_scores)

    hybrid_df = normalize_per_user(hybrid_df, ["content_score", "cf_score"])

    hybrid_df = compute_hybrid_scores(hybrid_df)

    hybrid_df = apply_quality_boost(hybrid_df, artifacts["movies_metadata"])

    hybrid_df["quality_boost_final_score"] = hybrid_df["final_score"]

    hybrid_df = normalize_per_user(hybrid_df, ["final_score"])

    recommendation_storing_service.store_predictions(hybrid_df)


@time_it
def apply_quality_boost(
    hybrid_df: pd.DataFrame, metadata: pd.DataFrame
) -> pd.DataFrame:
    merged = hybrid_df.merge(
        metadata[["movie_id", "popularity", "vote_count", "vote_average"]],
        on="movie_id",
        how="left",
    )

    # Fill and clip
    merged["popularity"] = merged["popularity"].clip(0, 100).fillna(0)
    merged["popularity"] = merged["popularity"].astype(float)
    merged["vote_count"] = merged["vote_count"].clip(0, 1000).fillna(0)
    merged["vote_average"] = merged["vote_average"].fillna(0)

    # Weighted vote
    C = float(metadata["vote_average"].mean())
    m = float(metadata["vote_count"].quantile(0.70))
    v = merged["vote_count"].astype(float)
    R = merged["vote_average"].astype(float)
    # Bayesian
    merged["weighted_rating"] = (v / (v + m)) * R + (m / (v + m)) * C
    merged["rating_score"] = merged["weighted_rating"] / 10

    print("---------------------------------------------------")
    print(merged["rating_score"].describe())

    print("---------------------------------------------------")
    print(merged["popularity"].describe())

    # Soft Cap
    merged["popularity_score"] = cap_boost(merged["popularity"], 40)  # 0 to ~1

    print("---------------------------------------------------")
    print(merged["popularity_score"].describe())

    merged["popularity_score"] = merged["popularity_score"].astype(float)
    merged["rating_score"] = merged["rating_score"].astype(float)

    # Blend into adjusted final score
    merged["final_score"] = (
        0.75 * merged["raw_final_score"]
        + 0.05 * merged["popularity_score"]
        + 0.1 * merged["rating_score"]
    )

    return merged


def cap_boost(x, threshold):
    return np.tanh(x / threshold)


@time_it
def merge_scores(cbf_df: pd.DataFrame, cf_df: pd.DataFrame):
    internal_cbf_df = cbf_df[cbf_df["user_id"].str.isnumeric()]
    internal_cf_df = cf_df[cf_df["user_id"].str.isnumeric()]

    return pd.merge(
        internal_cbf_df,
        internal_cf_df,
        on=["user_id", "movie_id"],
        how="outer",
        sort=False,
    )


@time_it
def normalize_per_user(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    def normalize(group):
        for column in columns:
            min_val = group[column].min()
            max_val = group[column].max()
            if max_val - min_val == 0:
                group[column] = 0.0  # Could also use 1.0 depending on your use case
            else:
                group[column] = (group[column] - min_val) / (max_val - min_val)
        return group

    return df.groupby("user_id", group_keys=False).apply(normalize)


@time_it
def compute_hybrid_scores(hybrid_df: pd.DataFrame, alpha: float = 0.7) -> pd.DataFrame:
    cf = hybrid_df["cf_score"].to_numpy()
    cb = hybrid_df["content_score"].to_numpy()

    has_cf = ~np.isnan(cf)

    final: np.ndarray = cb.copy()

    final[has_cf] = get_normal_final_scores(alpha, has_cf, cf, cb)
    final[
        ~has_cf
    ] *= 0.8  # Scaling where only content based scores are available cause it leads to obscure recommendations

    hybrid_df["raw_final_score"] = final
    return hybrid_df


@time_it
def get_normal_final_scores(
    alpha: float, has_cf: np.ndarray, cf: np.ndarray, cb: np.ndarray
):
    return alpha * cf[has_cf] + (1 - alpha) * cb[has_cf]


def run_recommender():
    get_hybrid_filtering()


if __name__ == "__main__":
    run_recommender()
