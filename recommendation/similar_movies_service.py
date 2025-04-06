import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from movies.movies_service import get_movies_metadata


def get_similarity_matrix():
    movies_metadata = pd.DataFrame(get_movies_metadata())

    movies_metadata["metadata"] = (
        movies_metadata[["genres", "tags", "overview"]].fillna("").agg(" ".join, axis=1)
    )

    # TF-IDF on metadata
    tfidf = TfidfVectorizer(stop_words="english")
    tfidf_matrix = tfidf.fit_transform(movies_metadata["metadata"])

    # Cosine similarity between movies
    cosine_sim = cosine_similarity(tfidf_matrix, tfidf_matrix)

    # Mapping
    movie_indices = pd.Series(movies_metadata.index, index=movies_metadata["id"])

    return cosine_sim, movie_indices, movies_metadata


def get_similar_movies(movie_id, top_n=10):
    cosine_sim, movie_indices, movies_metadata = get_similarity_matrix()

    idx = movie_indices[movie_id]
    sim_scores = list(enumerate(cosine_sim[idx]))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    sim_indices = [i[0] for i in sim_scores[1 : top_n + 1]]

    return movies_metadata.iloc[sim_indices]["id"].tolist()
