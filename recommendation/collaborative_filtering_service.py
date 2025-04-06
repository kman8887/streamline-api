import numpy as np
import pandas as pd
from sklearn.decomposition import TruncatedSVD
from scipy.sparse import csr_matrix
from common.utils.utils import time_it


@time_it
def get_collaborative_filtering_model(
    ratings_sparse: csr_matrix,
    user_id_lookup: dict[str, int],
    movie_id_lookup: dict[str, int],
) -> tuple[pd.DataFrame, np.ndarray]:
    """
    Get collaborative filtering model using SVD.
    :param ratings_sparse: Sparse matrix of user-item interactions
    :param user_id_lookup: Dictionary mapping user IDs to row indices
    :param movie_id_lookup: Dictionary mapping movie IDs to column indices
    :return: DataFrame with user_id, movie_id, and cf_score; movie_features array [k x n_movies]
    """

    # Apply SVD
    svd = TruncatedSVD(n_components=50)
    user_features = svd.fit_transform(
        ratings_sparse
    )  # shape: [n_users x k] Where k is the number of components i.e 50
    movie_features = svd.components_  # shape: [k x n_movies]

    # Predict ratings (dot product)
    predicted_ratings = (
        user_features @ movie_features
    )  # shape: [n_users x n_movies] but this time for all pairs

    user_ids = list(user_id_lookup.keys())  # all user_ids
    movie_ids = list(movie_id_lookup.keys())  # all movie_ids

    predicted_df = pd.DataFrame(
        predicted_ratings, index=user_ids, columns=movie_ids
    )  # shape: df index [n_users], columns [n_movies] values: predicted ratings

    cf_df = (
        predicted_df.stack().reset_index()
    )  # makes it so there is a new row for each user_id, movie_id pair rather than one row with all movies as columns
    cf_df.columns = ["user_id", "movie_id", "cf_score"]

    return cf_df, movie_features  # shape: row = [user_id, movie_id, cf_score]


@time_it
def get_new_user_cf_scores(user_ratings: dict[str, float], user_id: int, artifacts):
    # user_ratings: {movie_id: score}
    movie_features = artifacts[
        "movie_features"
    ]  # shape: [k x n_movies] where k is the number of components i.e 50 Got from SVD before, is too expensive to retrain for single user
    movie_id_lookup_cf = artifacts[
        "movie_features_movie_id_lookup"
    ]  # {movie_id: col_index}

    user_vector = np.zeros(
        movie_features.shape[0]
    )  # shape: [k x 1] this gets number of rows, so we can do the matrix multi with the new user, we set to zero as initial no features
    count = 0

    # Iterate over user ratings and gets the movie feature for each rating
    # and adds it to the user vector
    # Then we divide by the number of ratings to get the average
    # This is the same as the user features we got from SVD
    # but we are not going to use SVD for new users, so we just get the features
    # for the movies they rated

    for mid, rating in user_ratings.items():
        if mid in movie_id_lookup_cf:
            idx = movie_id_lookup_cf[mid]
            user_vector += rating * movie_features[:, idx]
            count += 1

    if count == 0:
        return {}

    user_vector /= count
    scores = (user_vector @ movie_features).reshape(
        1, -1
    )  # do matrix multiplication with the movie features Shape: [1 x n_movies]

    movie_ids = list(movie_id_lookup_cf.keys())  # get the movie id for each movie
    # cf_scores = {
    #     movie_ids[i]: float(scores[i]) for i in range(len(scores))
    # }  # shape: {movie_id: cf_score} For len(scores) = n_movies, we get the id at that index and score.

    predicted_df = pd.DataFrame(
        scores, index=[user_id], columns=movie_ids
    )  # shape: df index [n_users], columns [n_movies] values: predicted ratings

    cf_df = (
        predicted_df.stack().reset_index()
    )  # makes it so there is a new row for each user_id, movie_id pair rather than one row with all movies as columns
    cf_df.columns = ["user_id", "movie_id", "cf_score"]

    cf_df["user_id"] = cf_df["user_id"].astype(str)

    return cf_df
