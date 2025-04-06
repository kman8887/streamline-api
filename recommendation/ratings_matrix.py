import datetime
import numpy as np
import pandas as pd
from user_movie_interactions import user_movie_interaction_service
from common.utils.utils import time_it
from scipy.sparse import csr_matrix

LIKED_THRESHOLD = 7


@time_it
def create_ratings_matrix(
    path="./data/external_interactions_transformed.csv", split_for_evaluation=False, k=1
) -> tuple[csr_matrix, dict, dict, pd.DataFrame | None]:
    df_external = df_external = pd.read_csv(path)

    df_internal = pd.DataFrame(
        user_movie_interaction_service.get_all_user_interactions()
    )

    if split_for_evaluation:
        # Subset df_external for testing
        subset_length = len(df_external) // 50  # Get 1/50th of the rows
        df_external = df_external.iloc[:subset_length]

    df_all = pd.concat([df_internal, df_external], ignore_index=True)

    df_all["user_id"] = df_all["user_id"].astype(str)
    df_all["movie_id"] = df_all["movie_id"].astype(str)

    df_all = __compute_first_interaction_score(df_all)

    df_all = __add_time_decay(df_all)

    df_grouped = (
        df_all.groupby(["user_id", "movie_id"])["adjusted_score"]
        .sum()
        .reset_index()
        .rename(columns={"adjusted_score": "final_score"})
    )

    df_grouped["final_score"] = df_grouped["final_score"].astype(float)

    if split_for_evaluation:
        positive_interactions = df_grouped[df_grouped["final_score"] >= LIKED_THRESHOLD]
        split_df, test_df = leave_k_out_split(positive_interactions, k=k)
        # use split_df for training, but ALSO include negative/neutral data to build the model
        df_grouped = df_grouped[~df_grouped.index.isin(test_df.index)]

    else:
        test_df = None

    user_ids = df_grouped["user_id"].unique().tolist()
    movie_ids = df_grouped["movie_id"].unique().tolist()

    user_id_lookup = {uid: i for i, uid in enumerate(user_ids)}
    movie_id_lookup = {mid: i for i, mid in enumerate(movie_ids)}

    # Build sparse matrix
    # row_indices = df_grouped["user_id"].map(user_id_lookup).values
    # col_indices = df_grouped["movie_id"].map(movie_id_lookup).values

    df_grouped = __normalize_scores(df_grouped)

    non_zero_centered = df_grouped["centered_score"] != 0
    centered_data = df_grouped[non_zero_centered]

    # Exclude zero from raw scores
    non_zero_raw = df_grouped["final_score"] != 0
    raw_data = df_grouped[non_zero_raw]

    centered_ratings_sparse = csr_matrix(
        (
            centered_data["centered_score"],
            (
                centered_data["user_id"].map(user_id_lookup),
                centered_data["movie_id"].map(movie_id_lookup),
            ),
        ),
        shape=(len(user_ids), len(movie_ids)),
    )

    # Raw matrix (for CBF)
    raw_ratings_sparse = csr_matrix(
        (
            raw_data["final_score"],
            (
                raw_data["user_id"].map(user_id_lookup),
                raw_data["movie_id"].map(movie_id_lookup),
            ),
        ),
        shape=(len(user_ids), len(movie_ids)),
    )

    print(df_grouped["centered_score"].describe())
    print("---------------------------------------------------")
    print(df_grouped["final_score"].describe())
    print("---------------------------------------------------")
    print(centered_data["centered_score"].describe())
    print("---------------------------------------------------")
    print(raw_data["final_score"].describe())

    return (
        raw_ratings_sparse,
        centered_ratings_sparse,
        user_id_lookup,
        movie_id_lookup,
        test_df,
    )  # shape sparse array [n_users x n_movies]; {user_id: row_index}; {movie_id: col_index}


@time_it
def get_rating_matrix_for_user(
    user_id: int,
) -> dict[str, float]:
    user_ratings_df = pd.DataFrame(
        user_movie_interaction_service.get_users_interactions(user_id)
    )

    user_ratings_df["user_id"] = user_ratings_df["user_id"].astype(str)
    user_ratings_df["movie_id"] = user_ratings_df["movie_id"].astype(str)

    user_ratings_df = __compute_first_interaction_score(user_ratings_df)
    user_ratings_df = __add_time_decay(user_ratings_df)

    user_ratings = (
        user_ratings_df.groupby(["user_id", "movie_id"])["adjusted_score"]
        .sum()
        .reset_index()
        .rename(columns={"adjusted_score": "final_score"})
    )

    raw_ratings = dict(zip(user_ratings["movie_id"], user_ratings["final_score"]))

    user_ratings = __normalize_scores(
        user_ratings
    )  # SHAPE: columns = [user_id, movie_id, final_score]

    centered_ratings = dict(
        zip(user_ratings["movie_id"], user_ratings["centered_score"])
    )

    return raw_ratings, centered_ratings  # shape: {movie_id: final_score}


def leave_k_out_split(df: pd.DataFrame, k: int = 1, seed: int = 42):
    np.random.seed(seed)
    test_rows = []

    for user_id, group in df.groupby("user_id"):
        if len(group) > k:
            test_sample = group.sample(k)
            test_rows.append(test_sample)

    test_df = pd.concat(test_rows)
    train_df = df.drop(index=test_df.index)

    return train_df, test_df


@time_it
def __compute_first_interaction_score(df_all: pd.DataFrame) -> pd.DataFrame:
    df_all["interaction_score"] = 0.0  # default

    # Rating: use actual rating value
    rating_mask = df_all["interaction_type"] == "RATING"
    df_all.loc[rating_mask, "interaction_score"] = (
        df_all.loc[rating_mask, "rating"].astype(float) - 5
    )

    # LIKE
    like_mask = df_all["interaction_type"] == "LIKE"
    df_all.loc[like_mask, "interaction_score"] = 2

    # WATCHED
    watched_mask = df_all["interaction_type"] == "WATCHED"
    df_all.loc[watched_mask, "interaction_score"] = 1.0

    # REVIEW
    review_mask = df_all["interaction_type"] == "REVIEW"
    df_all.loc[review_mask, "interaction_score"] = 1.2

    df_all["interaction_score"] = df_all["interaction_score"].astype(float)
    return df_all


@time_it
def __add_time_decay(user_interactions: pd.DataFrame) -> pd.DataFrame:
    now = datetime.datetime.now(tz=datetime.timezone.utc)

    user_interactions["created_at"] = pd.to_datetime(
        user_interactions["created_at"]
    ).dt.tz_localize("UTC", ambiguous="NaT", nonexistent="NaT")

    user_interactions["created_at"] = user_interactions["created_at"].fillna(now)

    user_interactions["days_ago"] = (
        now - user_interactions["created_at"]
    ).dt.days.clip(lower=0)

    decay_rate = 0.97
    user_interactions["time_decay"] = decay_rate ** user_interactions["days_ago"]
    # user_interactions["recency_boost"] = 2 / (
    #     1 + np.exp(user_interactions["days_ago"] / 10)
    # )

    user_interactions["adjusted_score"] = (
        user_interactions["interaction_score"]
        * user_interactions["time_decay"]
        # * user_interactions["recency_boost"]
    ).astype(float)

    return user_interactions


@time_it
def __normalize_scores(df_grouped: pd.DataFrame) -> pd.DataFrame:
    user_means = df_grouped.groupby("user_id")["final_score"].transform("mean")
    df_grouped["centered_score"] = df_grouped["final_score"] - user_means

    # Normalize the scores for cf specifically since users ratings range can change
    return df_grouped
