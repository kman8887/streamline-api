import datetime
import psycopg
from common.utils.utils import DB_CONFIG
from psycopg.rows import dict_row


def get_recommendations(user_id: str, movie_id="6798244c6243f72901adb45e"):
    query = """
    SELECT predicted_score
    FROM user_recommendations ur
    WHERE ur.user_id =  %s
    AND ur.movie_id = %s
    """

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, [user_id, movie_id])

            predicted_score = cur.fetchone()

    return predicted_score


def get_last_recommendation_update(user_id: str) -> datetime.datetime:
    query = """
    SELECT MAX(updated_at) AS last_update
    FROM user_recommendations ur
    WHERE ur.user_id =  %s
    """

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, [user_id])

            result = cur.fetchone()

    last_update_raw = result["last_update"]

    if last_update_raw is not None:
        if last_update_raw.tzinfo is None:
            # Naive → make it UTC-aware
            last_update = last_update_raw.replace(tzinfo=datetime.timezone.utc)
        else:
            # Already aware → normalize to UTC
            last_update = last_update_raw.astimezone(datetime.timezone.utc)
    else:
        last_update = None

    return last_update
