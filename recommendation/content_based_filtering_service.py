import numpy as np
import pandas as pd
from common.utils.utils import time_it
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm
from scipy.sparse import csr_matrix


@time_it
def get_content_based_filtering_model(
    ratings_sparse: csr_matrix,
    user_id_lookup: dict[str, int],
    movie_id_lookup: dict[str, int],
    movies_metadata: pd.DataFrame,
):
    """_summary_

    Args:
        :param ratings_sparse: Sparse matrix of user-item interactions
        :param user_id_lookup: Dictionary mapping user IDs to row indices
        :param movie_id_lookup: Dictionary mapping movie IDs to column indices

    Returns:
        _type_: _description_
    """

    movies_metadata["metadata"] = __build_metadata_vectorized(
        movies_metadata
    )  # Just big list of features for each movie

    # TF-IDF on metadata
    tfidf_vectorizer, tfidf_matrix, tfidf_movie_id_to_index = __tf_idf_on_metadata(
        movies_metadata
    )  # tfidf_matrix shape: [n_movies x n_features], gets features from metadata for each movie

    ratings_matrix_index_to_mid = list(
        movie_id_lookup.keys()
    )  # reverses so we can map from index to movie_id, for the ratings matrix

    # Creates User feature vectors from the top 10 movies they have rated
    user_profiles = {}
    for user_id, user_idx in tqdm(
        user_id_lookup.items(), desc="Building user profiles"
    ):
        user_row: np.ndarray = ratings_sparse.getrow(user_idx).toarray().flatten()
        top_rated_indices = user_row.argsort()[-10:][
            ::-1
        ]  # top 10 movies indices, we get users top rated ten movies to use for user's feature vector. This is for speed
        top_rated_movie_ids = [
            ratings_matrix_index_to_mid[i] for i in top_rated_indices
        ]  # get movie_ids since ratings indices do not map to metadata matrix indices

        tfidf_indices = [
            tfidf_movie_id_to_index[mid]
            for mid in top_rated_movie_ids
            if mid in tfidf_movie_id_to_index
        ]  # we get the indices of the movies in the tfidf matrix, this is to get the feature vector for the user

        if tfidf_indices:
            user_vector = tfidf_matrix[tfidf_indices].toarray().mean(axis=0)
            user_profiles[user_id] = (
                user_vector  # shape: [1 x n_features] this is the user profile, we take the mean of the top 10 movies they rated to get a feature vector for the user
            )

    user_profiles_index_to_user_ids = list(user_profiles.keys())
    tfidf_index_to_movie_ids = list(tfidf_movie_id_to_index.keys())

    user_matrix = np.stack(list(user_profiles.values()))
    similarity_matrix = cosine_similarity(
        user_matrix, tfidf_matrix
    )  # get similarity between each users top 10 and all movies

    feature_names = tfidf_vectorizer.get_feature_names_out()
    movie_vectors = tfidf_matrix.toarray()

    content_scores = []
    for user_idx, user_id in tqdm(
        enumerate(user_profiles_index_to_user_ids),
        desc="Scoring and explaining top movies",
    ):
        user_vector = user_profiles[user_id].reshape(1, -1)  # shape: [1 x n_features]
        scores = similarity_matrix[user_idx]

        create_final_content_score(
            content_scores,
            scores,
            user_vector,
            feature_names,
            movie_vectors,
            tfidf_index_to_movie_ids,
            user_id,
        )

    # content_scores is now a list of dicts with user_id, movie_id, content_score, and explanation
    cbf_df = pd.DataFrame(content_scores)

    return cbf_df, tfidf_vectorizer, tfidf_matrix, tfidf_movie_id_to_index


@time_it
def get_new_user_content_score(user_ratings: dict[str, float], user_id, artifacts):
    tfidf_vectorizer = artifacts["tfidf_vectorizer"]
    item_matrix: np.ndarray = artifacts["item_feature_matrix"]
    movie_id_lookup: dict[str, int] = artifacts["item_feature_matrix_movie_id_lookup"]

    vectors = []
    weights = []

    for movie_id, score in user_ratings.items():
        if movie_id in movie_id_lookup:
            idx = movie_id_lookup[movie_id]
            vectors.append(item_matrix[idx])
            weights.append(score)
    # goes through and gets weights(Rating Score) and features for each movie the user rated

    if not vectors:
        return None

    vectors = np.array(vectors)  # shape: [n_user_ratings x n_features]
    weights = np.array(weights).reshape(
        -1, 1
    )  # shape: [n_user_ratings x 1] but treats as 1d array, we do this so we treat it as a 2d array
    user_vector = (vectors * weights).sum(
        axis=0
    ) / weights.sum()  # We weight the user vector here based on rating score

    # user_vector is now [n_features x 1]

    # user_vector reshape to [1 x n_features]
    scores = cosine_similarity(user_vector.reshape(1, -1), item_matrix)[0]

    feature_names = tfidf_vectorizer.get_feature_names_out()
    tfidf_index_to_movie_ids = list(movie_id_lookup.keys())

    content_scores = []
    create_final_content_score(
        content_scores,
        scores,
        user_vector,
        feature_names,
        item_matrix,
        tfidf_index_to_movie_ids,
        user_id,
    )

    cbf_df = pd.DataFrame(content_scores)

    cbf_df["user_id"] = cbf_df["user_id"].astype(str)

    return cbf_df


def create_final_content_score(
    content_scores,
    user_scores,
    user_vector,
    feature_names,
    movie_vectors,
    tfidf_index_to_movie_ids,
    user_id,
    top_n_explain=100,
):
    # Find the indices of top-N scores (for explanation)
    top_expl_idx_set = set(
        np.argpartition(user_scores, -top_n_explain)[-top_n_explain:]
    )

    for movie_idx, score in tqdm(enumerate(user_scores), desc="Getting explanation"):
        explanation = None

        if movie_idx in top_expl_idx_set:
            contribution = user_vector.flatten() * movie_vectors[movie_idx]
            top_feat_idx = contribution.argsort()[-10:][::-1]
            explanation = [
                {"feature": feature_names[j], "score": float(contribution[j])}
                for j in top_feat_idx
                if contribution[j] > 0
            ]

        content_scores.append(
            {
                "user_id": user_id,
                "movie_id": tfidf_index_to_movie_ids[movie_idx],
                "content_score": float(score),
                "explanation": explanation,
            }
        )


def explain_recommendation(
    user_id: str,
    movie_id: str,
    user_profiles: dict,
    movie_id_lookup: dict,
    tfidf_matrix,
    tfidf_vectorizer: TfidfVectorizer,
    top_n: int = 5,
) -> list[dict]:
    """
    Returns the top N feature contributions explaining why a movie was recommended to a user.
    """
    if user_id not in user_profiles or movie_id not in movie_id_lookup:
        return []

    feature_names = tfidf_vectorizer.get_feature_names_out()
    user_vector = user_profiles[user_id]
    movie_index = movie_id_lookup[movie_id]
    movie_vector = tfidf_matrix[movie_index].toarray().flatten()

    contribution = user_vector * movie_vector  # element-wise dot product

    # Get top N contributing features
    top_indices = contribution.argsort()[-top_n:][::-1]

    explanation = [
        {"feature": feature_names[i], "score": float(contribution[i])}
        for i in top_indices
        if contribution[i] > 0
    ]

    return explanation


def build_baseline_recs(cbf_df: pd.DataFrame, top_n=100):
    return (
        cbf_df.groupby("movie_id")["content_score"]
        .mean()
        .sort_values(ascending=False)
        .head(top_n)
        .reset_index()
    )


@time_it
def __build_metadata_vectorized(df: pd.DataFrame) -> pd.Series:
    def clean_array(col, repeat=1):
        col = col.fillna("").apply(list)
        return col.apply(
            lambda lst: (
                (
                    " ".join(
                        [" ".join([f"{val.strip().replace(' ', '_')}" for val in lst])]
                        * repeat
                    )
                    + " "
                )
                if lst
                else ""
            )
        )

    def clean_feature(series, repeat=1):
        return (
            series.fillna("")
            .astype(str)
            .str.strip()
            .str.replace(r"\s+", "_", regex=True)
            .apply(lambda x: (x + " ") * repeat if x else "")
        )

    # Replace nulls and spaces
    lang = clean_feature(df["original_language"])
    decade = clean_feature(df["release_decade"])

    runtime = (
        df["runtime"]
        .fillna(0)
        .astype(float)
        .floordiv(10)
        .astype(int)
        .astype(str)
        .add("_mins ")
    )

    genres = clean_array(df["genres"])
    tags = clean_array(df["tags"])
    director = clean_array(df["director"], repeat=3)
    writer = clean_array(df["writer"], repeat=2)
    cast = clean_array(df["top_2_cast"])

    return lang + decade + runtime + genres + tags + director + writer + cast


@time_it
def __tf_idf_on_metadata(movies_metadata):
    tfidf_vectorizer = TfidfVectorizer(
        stop_words="english", min_df=0.003, max_df=0.5, max_features=5000
    )
    tfidf_matrix = tfidf_vectorizer.fit_transform(
        movies_metadata["metadata"]
    )  # shape: [n_movies x n_features]

    movie_id_lookup = dict(
        zip(movies_metadata["movie_id"], range(len(movies_metadata)))
    )  # {movie_id: col_index}

    return tfidf_vectorizer, tfidf_matrix, movie_id_lookup
