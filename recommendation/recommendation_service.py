import psycopg
from common.utils.utils import DB_CONFIG
from psycopg.rows import dict_row


def get_recommendations(user_id: str, movie_id="6798244c6243f72901adb45e"):
    print(user_id, movie_id)

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

    print(predicted_score)
    return predicted_score
