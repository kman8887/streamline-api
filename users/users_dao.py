import json
import psycopg
from common.utils.utils import DB_CONFIG
from users.user import User, UserDetails


def getUserDetails(id: int) -> UserDetails:
    query = """
    SELECT 
        id,
        username,
        real_name,
        avatar,
        region,
        auth_id,
        creation_date,
        languages,
        (SELECT avg(umi.rating) 
        FROM user_movie_interactions umi 
        WHERE umi.user_id = u.id
        AND umi.active = true
        AND umi.interaction_type = 'RATING') AS avg_rating,
        
        (SELECT count(umi.id) 
        FROM user_movie_interactions umi 
        WHERE umi.user_id = u.id
        AND umi.active = true
        AND umi.interaction_type = 'RATING') AS total_ratings,
        
        (SELECT count(umi.id) 
        FROM user_movie_interactions umi 
        WHERE umi.user_id = u.id
        AND umi.active = true
        AND umi.interaction_type = 'LIKE') AS total_likes,
        
        (SELECT count(r.id) 
            FROM movie_reviews r 
            WHERE r.user_id = u.id
            AND r.active = true) AS total_reviews
        
    FROM users u
    WHERE id = %s;
    """
    with psycopg.connect(
        **DB_CONFIG, row_factory=psycopg.rows.class_row(UserDetails)
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query, [id])
            return cur.fetchone()


def getUserFromAccessToken(auth_id) -> User:
    with psycopg.connect(**DB_CONFIG, row_factory=psycopg.rows.class_row(User)) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE auth_id = (%s)", [auth_id])
            return cur.fetchone()


def createUser(user):
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (username, real_name, avatar, region, auth_id, creation_date, languages)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
                """,
                list(user.values()),
            )
            return cur.fetchone()


def updateUser(user_id: int, languages: list[str], region: str):
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE users 
                SET region = %s, languages = %s
                WHERE id = %s
                RETURNING id;
                """,
                [region, json.dumps(languages), user_id],
            )
            return cur.fetchone()


def updateUserWatchProviders(id: int, watch_providers: list[int]):
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Step 1: Delete existing watch providers for the user
            cur.execute(
                """
                DELETE FROM user_watch_providers
                WHERE user_id = %s;
                """,
                (id,),
            )

            # Step 2: Insert new watch providers
            if watch_providers:
                cur.executemany(
                    """
                    INSERT INTO user_watch_providers (user_id, watch_provider_id)
                    VALUES (%s, %s);
                    """,
                    [(id, wp) for wp in watch_providers],
                )

            # Commit the transaction
            conn.commit()


def getUserWatchProviders(id: int):
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT watch_provider_id
                FROM user_watch_providers 
                WHERE user_id = %s;
                """,
                (id,),
            )
            return [row[0] for row in cur.fetchall()]
