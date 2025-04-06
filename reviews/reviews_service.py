from dataclasses import asdict
from typing import List, Optional, Tuple
from bson import ObjectId
from flask import g, jsonify, make_response, request
import psycopg

from common.utils.utils import movies_db, users_db, DB_CONFIG

from reviews.review import Review, ReviewLike, ReviewWithMovieDetails
from reviews.review_filter_params import ReviewFilterParams
from security.guards import (
    unauthorized_error,
)


def get_review_filter_params(
    movie_id: str = None, user_id: int = None
) -> ReviewFilterParams:
    """
    Extracts review filter parameters from request arguments.

    :return: ReviewFilterParams object containing filter values.
    """
    if movie_id is None and user_id is None:
        raise Exception("No ID provided to filter reviews")

    page_num: int = request.args.get("pn", type=int, default=0)
    page_size: int = request.args.get("ps", type=int, default=10)
    page_start: int = page_size * page_num

    rating_from = request.args.get("ratingFrom", type=float)
    rating_to = request.args.get("ratingTo", type=float)

    if rating_from == 0 and rating_to == 10:
        rating_to = None
        rating_from = None

    order_by, order_direction = get_review_sorting()

    return ReviewFilterParams(
        movie_id=movie_id,
        user_id=user_id,
        rating_from=rating_from,
        rating_to=rating_to,
        order_by=order_by,
        order_direction=order_direction,
        limit_rows=page_size,
        offset_rows=page_start,
    )


def get_review_sorting() -> Tuple[str, str]:
    """
    Extracts sorting preferences from request arguments.

    :return: Tuple containing (order_by, order_direction)
    """
    sort_arg: Optional[str] = request.args.get("sort")

    if sort_arg:
        if ":" in sort_arg:
            field, direction = sort_arg.split(":")
            return field, "DESC" if direction == "-1" else "ASC"
        else:
            return sort_arg, "ASC"

    return "like_count", "DESC"  # Default sorting


def add_review(
    movie_id: str, user_id: int, review_text: str, rating: Optional[float]
) -> int:
    review_query = """
    INSERT INTO movie_reviews (movie_id, user_id, review_text, rating, created_at)
    VALUES (%s, %s, %s, %s, NOW()) RETURNING id;
    """

    deactivate_rating_query = """
    UPDATE user_movie_interactions 
    SET active = FALSE 
    WHERE movie_id = %s AND user_id = %s AND interaction_type = 'RATING' AND active = TRUE;
    """

    insert_rating_query = """
    INSERT INTO user_movie_interactions (user_id, movie_id, interaction_type, rating, active, created_at)
    VALUES (%s, %s, 'RATING', %s, TRUE, NOW())
    RETURNING id;
    """
    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            # Insert review
            cur.execute(review_query, (movie_id, user_id, review_text, rating))
            review_id = cur.fetchone()[0]

            # If a rating is provided, ensure only one active rating exists
            if rating is not None:
                cur.execute(
                    deactivate_rating_query, (movie_id, user_id)
                )  # Deactivate old ratings
                cur.execute(
                    insert_rating_query, (user_id, movie_id, rating)
                )  # Insert new rating

    return review_id


def toggle_like_review(review_id, user_id, value: bool):
    insert_query = """
    INSERT INTO movie_review_likes (review_id, user_id)
    VALUES (%s, %s)
    ON CONFLICT (review_id, user_id) DO NOTHING;
    """

    delete_query = """
    DELETE FROM movie_review_likes
    WHERE review_id = %s
    AND user_id = %s;
    """

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            if value:
                cur.execute(insert_query, (review_id, user_id))
            else:
                cur.execute(delete_query, (review_id, user_id))


def get_user_movie_rating(movie_id: str, user_id: int) -> Optional[float]:
    """
    Fetches the user's rating for the given movie from the database.

    :return: The rating if it exists, otherwise None.
    """
    query = """
    SELECT rating FROM user_movie_interactions
    WHERE movie_id = %s AND user_id = %s AND interaction_type = 'RATING' AND active = true;
    """

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (movie_id, user_id))
            result = cur.fetchone()
            return result[0] if result else None


def get_filtered_reviews(params: ReviewFilterParams, user_id: int) -> List[Review]:
    """
    Fetches filtered movie reviews from PostgreSQL.
    """
    query = """
    SELECT * FROM get_filtered_reviews(
        %(movie_id)s, %(user_id)s, %(rating_from)s::numeric, %(rating_to)s::numeric,
        %(order_by)s, %(order_direction)s,
        %(limit_rows)s, %(offset_rows)s
    )
    """

    liked_query = """
    SELECT * FROM movie_review_likes
    WHERE user_id = %s
    AND review_id = ANY(%s)
    """
    review_likes = None

    with psycopg.connect(**DB_CONFIG, row_factory=psycopg.rows.dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                asdict(params),
            )
            reviews = [Review(**row) for row in cur.fetchall()]

            if user_id is not None:
                review_ids = [review.review_id for review in reviews]
                cur.execute(liked_query, (user_id, review_ids))

                review_likes = [ReviewLike(**row) for row in cur.fetchall()]

    def set_is_review_liked(review):
        review.isReviewLiked = review.review_id in [
            like.review_id for like in review_likes
        ]
        return review

    if review_likes is not None:
        return list(map(set_is_review_liked, reviews))
    else:
        return reviews


def get_filtered_reviews_with_movie_details(
    params: ReviewFilterParams, user_id: int
) -> List[ReviewWithMovieDetails]:
    """
    Fetches filtered movie reviews from PostgreSQL.
    """
    query = """
    SELECT * FROM get_filtered_reviews_with_movie_details(
        %(movie_id)s, %(user_id)s, %(rating_from)s::numeric, %(rating_to)s::numeric,
        %(order_by)s, %(order_direction)s,
        %(limit_rows)s, %(offset_rows)s
    )
    """

    liked_query = """
    SELECT * FROM movie_review_likes
    WHERE user_id = %s
    AND review_id = ANY(%s)
    """
    review_likes = None

    with psycopg.connect(**DB_CONFIG, row_factory=psycopg.rows.dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                asdict(params),
            )
            reviews = [ReviewWithMovieDetails(**row) for row in cur.fetchall()]

            if user_id is not None:
                review_ids = [review.review_id for review in reviews]
                cur.execute(liked_query, (user_id, review_ids))

                review_likes = [ReviewLike(**row) for row in cur.fetchall()]

    def set_is_review_liked(review):
        review.isReviewLiked = review.review_id in [
            like.review_id for like in review_likes
        ]
        return review

    if review_likes is not None:
        return list(map(set_is_review_liked, reviews))
    else:
        return reviews


def count_filtered_reviews(params: ReviewFilterParams) -> int:
    """
    Fetches the total count of reviews for a movie based on filters.

    :param movie_id: The ID of the movie.
    :param rating_from: Minimum rating filter.
    :param rating_to: Maximum rating filter.
    :return: Total number of matching reviews.
    """
    query = """
    SELECT count_filtered_reviews(%(movie_id)s, %(user_id)s, %(rating_from)s::numeric, %(rating_to)s::numeric)
    """

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                query,
                asdict(params),
            )
            return cur.fetchone()[0]


def isOwnReviewOrHasPermission(permission, review_id):
    access_token = g.get("access_token")
    if not access_token:
        return make_response(jsonify(unauthorized_error), 401)

    token_permissions = access_token.get("permissions")

    if not token_permissions:
        return isOwnReview(access_token, review_id)

    required_permissions_set = set([permission])
    token_permissions_set = set(token_permissions)

    if not required_permissions_set.issubset(token_permissions_set):
        return isOwnReview(access_token, review_id)

    return


def isOwnReview(access_token, review_id):
    auth_id = access_token.get("sub")

    user = users_db.find_one({"auth_id": auth_id})

    if user:
        id = user.get("_id")
        results = movies_db.count_documents(
            {"reviews._id": ObjectId(review_id), "reviews.user_id": str(id)}
        )

        if results > 0:
            return
        else:
            return make_response(jsonify(unauthorized_error), 401)


def getReviewsPipeline(filters, sort, page_start, page_size, match_id_pipeline):
    pipeline = [
        {"$project": {"reviews": 1, "_id": 0}},
        {"$unwind": "$reviews"},
        {"$match": filters},
        {
            "$addFields": {
                "reviews.funnyCount": {
                    "$size": {"$ifNull": ["$reviews.found_funny", []]}
                },
                "reviews.helpfulCount": {
                    "$size": {"$ifNull": ["$reviews.found_helpful", []]}
                },
                "reviews.notHelpfulCount": {
                    "$size": {"$ifNull": ["$reviews.found_not_helpful", []]}
                },
            }
        },
        {"$sort": sort},
        {
            "$unset": [
                "reviews.funnyCount",
                "reviews.helpfulCount",
                "reviews.notHelpfulCount",
            ]
        },
        {
            "$facet": {
                "data": [
                    {"$skip": page_start},
                    {"$limit": page_size},
                    {"$group": {"_id": "null", "reviews": {"$push": "$reviews"}}},
                ],
                "pageInfo": [{"$count": "totalRecords"}],
            }
        },
    ]

    pipeline.insert(0, match_id_pipeline)

    return pipeline
