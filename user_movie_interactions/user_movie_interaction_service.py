from typing import List
import psycopg

from common.utils.utils import DB_CONFIG, time_it
from recommendation.model.user_movie_interaction import UserMovieInteraction
from users.model.movie_rating_request import MovieRatingRequest
from movies.model.user_movie_interaction_type import UserMovieInteractionType
from psycopg.rows import dict_row


def toggle_user_interaction(
    movie_id: str, user_id: int, value: bool, type: UserMovieInteractionType
):
    select_query = """
    SELECT id
    FROM user_movie_interactions
    WHERE user_id = %s AND movie_id = %s AND interaction_type = %s AND active = TRUE;
    """
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(select_query, (user_id, movie_id, type))
            interaction = cur.fetchone()
            if interaction and not value:
                soft_delete_user_interaction(interaction[0])
            elif not interaction and value:
                add_user_interaction(user_id, movie_id, type)
            else:
                return


def soft_delete_user_interaction(interaction_id: int):
    delete_query = """
    UPDATE user_movie_interactions
    SET active = FALSE, updated_at = NOW()
    WHERE id = %s;
    """
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(delete_query, [interaction_id])


def add_user_interaction(user_id: int, movie_id: str, type: UserMovieInteractionType):
    user_interaction_query = """
    INSERT INTO user_movie_interactions (user_id, movie_id, interaction_type, created_at, updated_at)
    VALUES (%s, %s, %s, NOW(), NOW()) RETURNING id;
    """
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(user_interaction_query, (user_id, movie_id, type))
            return cur.fetchone()[0]


def bulk_rate_movies(user_id: int, movies: List[MovieRatingRequest]):
    user_interaction_query = """
    INSERT INTO user_movie_interactions (user_id, movie_id, interaction_type, rating, created_at, updated_at)
    VALUES (%s, %s, 'RATING', %s, NOW(), NOW()) RETURNING id;
    """

    select_query = """
    SELECT id
    FROM user_movie_interactions
    WHERE user_id = %s AND movie_id IN (%s) AND interaction_type = 'RATING' AND active = TRUE;
    """

    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                for movie in movies:
                    cur.execute(select_query, (user_id, movie.id))
                    interaction = cur.fetchall()
                    if interaction:
                        for t in interaction:
                            soft_delete_user_interaction(t[0])

                    cur.execute(
                        user_interaction_query, (user_id, movie.id, movie.rating)
                    )

    except Exception as e:
        print(f"Error in bulk_rate_movies: {e}")
        raise


def toggle_movie_watchlist_value(movie_id: str, user_id: int, value: bool):
    insert_query = """
    INSERT INTO user_movie_list (movie_id, user_id, added_at)
    VALUES (%s, %s, NOW())
    ON CONFLICT (movie_id, user_id) DO NOTHING;
    """

    delete_query = """
    DELETE FROM user_movie_list
    WHERE movie_id = %s
    AND user_id = %s;
    """

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            if value:
                cur.execute(insert_query, (movie_id, user_id))
            else:
                cur.execute(delete_query, (movie_id, user_id))


@time_it
def get_all_user_interactions() -> List[UserMovieInteraction]:
    select_query = """
    SELECT user_id, movie_id, rating, interaction_type, created_at
    FROM user_movie_interactions umi
    INNER JOIN movies ON umi.movie_id = movies.id
    WHERE active = TRUE;
    """
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(select_query)
            user_interactions = [UserMovieInteraction(**row) for row in cur.fetchall()]

    return user_interactions


@time_it
def get_users_interactions(user_id: int) -> List[UserMovieInteraction]:
    select_query = """
    SELECT user_id, movie_id, rating, interaction_type, created_at
    FROM user_movie_interactions umi
    INNER JOIN movies ON umi.movie_id = movies.id
    WHERE active = TRUE
    AND user_id = %s;
    """
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(select_query, [user_id])
            user_interaction = [UserMovieInteraction(**row) for row in cur.fetchall()]

    return user_interaction
