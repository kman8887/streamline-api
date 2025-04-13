import json
import traceback
from typing import Any, Dict, Optional
from bson import ObjectId
from flask import Blueprint, g, jsonify, make_response, request
from movies.model.movie import Movie
from movies.model.user_movie_interaction_type import UserMovieInteractionType
from movies.movies_service import (
    get_distinct_watch_providers,
    get_movie_details,
    get_filter_params,
    get_distinct_genres_tags_and_watch_providers,
)
import movies.movies_service as movies_service
import users.users_service as users_service
from common.utils.utils import movies_db
from common.utils.context_service import get_user_context
from reviews.reviews_service import (
    add_review,
    count_filtered_reviews,
    get_filtered_reviews,
    get_review_filter_params,
    get_user_movie_rating,
    getReviewsPipeline,
)
from security.guards import (
    authorization_guard,
    context_provider,
    extract_headers,
    permissions_guard,
    games_permissions,
)
from user_movie_interactions.user_movie_interaction_service import (
    toggle_movie_watchlist_value,
    toggle_user_interaction,
)
from users.users_service import getUserFromAccessToken

bp_name = "movies"
bp_url_prefix = "/api/v1.0/movies"
bp = Blueprint(bp_name, __name__, url_prefix=bp_url_prefix)


@bp.route("", methods=["GET"])
@context_provider
@extract_headers
def show_all_movies() -> Any:
    context_user_id = None
    context = get_user_context()
    if context is not None:
        context_user_id = context.user.id
    try:
        # @dataclass annotation to create classes with attributes
        # also remoeber -> str for result type type hinting
        params = get_filter_params(context_user_id)
        params.user_id_param = context_user_id

        include_user = context_user_id is not None

        result = movies_service.get_movies_and_count_cached(params, include_user)

        return make_response(jsonify(result), 200)

    except Exception as e:
        print(e)
        return make_response(500)


@bp.route("/onboarding", methods=["GET"])
@authorization_guard
def get_onboarding_movies() -> Any:
    try:
        user = users_service.getUserFromAccessToken()
        if user is None:
            return make_response(jsonify({"error": "Invalid user"}), 404)
        print(user)

        result = movies_service.get_onboarding_movies(user.id)

        return make_response(jsonify(result), 200)

    except Exception as e:
        print(e)
        return make_response(500)


@bp.route("/watchlist", methods=["GET"])
@authorization_guard
def get_watchlist() -> Any:
    try:
        user = users_service.getUserFromAccessToken()
        if user is None:
            return make_response(jsonify({"error": "Invalid user"}), 404)
        print(user)
        result = movies_service.get_watchlist_movies(user.id)

        return make_response(jsonify(result), 200)

    except Exception as e:
        print(e)
        return make_response(500)


@bp.route("/filters", methods=["GET"])
@extract_headers
def get_filters() -> Any:
    """API endpoint to fetch available genres and tags."""
    region = g.region
    result = get_distinct_genres_tags_and_watch_providers(region)
    return make_response(jsonify(result), 200)


@bp.route("/watch-providers", methods=["GET"])
@extract_headers
def get_watch_providers() -> Any:
    """API endpoint to fetch available watch_providers for users region."""
    region = g.region

    result = get_distinct_watch_providers(region)
    return make_response(jsonify(result), 200)


@bp.route("/<string:id>", methods=["GET"])
@context_provider
@extract_headers
def show_one_movie(id):
    # Need to implement passing user context data through, timezone and region, and language
    region = g.region

    context_user_id = None
    context = get_user_context()
    if context is not None:
        context_user_id = context.user.id

    movie: Movie = get_movie_details(id, user_id=context_user_id, region=region)

    if movie is not None:
        return make_response(jsonify(movie), 200)
    else:
        return make_response(jsonify({"error": "Invalid movie ID"}), 404)


@bp.route("/<string:movie_id>/like", methods=["POST"])
@authorization_guard
def toggle_movie_like(movie_id):
    body = json.loads(request.get_data().decode("utf-8"))
    user = getUserFromAccessToken()

    if user is None or body is None:
        return make_response(jsonify({"error": "Invalid body or user"}), 404)

    isLiked: bool = bool(body["isLiked"])

    if isLiked is None:
        return make_response(jsonify({"error": "Invalid body"}), 400)

    toggle_user_interaction(movie_id, user.id, isLiked, UserMovieInteractionType.LIKE)

    return jsonify({"message": "Like toggled"}), 201


@bp.route("/<string:movie_id>/watch", methods=["POST"])
@authorization_guard
def toggle_movie_watch(movie_id):
    body = json.loads(request.get_data().decode("utf-8"))
    user = getUserFromAccessToken()

    if user is None or body is None:
        return make_response(jsonify({"error": "Invalid body or user"}), 404)

    isWatched: bool = bool(body["isWatched"])

    if isWatched is None:
        return make_response(jsonify({"error": "Invalid body"}), 400)

    toggle_user_interaction(
        movie_id, user.id, isWatched, UserMovieInteractionType.WATCHED
    )

    return jsonify({"message": "Watch toggled"}), 201


@bp.route("/<string:movie_id>/watchList", methods=["POST"])
@authorization_guard
def toggle_movie_watchlist(movie_id):
    body = json.loads(request.get_data().decode("utf-8"))
    user = getUserFromAccessToken()

    if user is None or body is None:
        return make_response(jsonify({"error": "Invalid body or user"}), 404)

    isInWatchList: bool = bool(body["isInWatchList"])

    if isInWatchList is None:
        return make_response(jsonify({"error": "Invalid body"}), 400)

    toggle_movie_watchlist_value(movie_id, user.id, isInWatchList)

    return jsonify({"message": "Watch List toggled"}), 201


@bp.route("/<string:movie_id>/reviews", methods=["GET"])
@context_provider
def fetch_all_reviews(movie_id) -> Dict[str, Any]:
    context_user_id = None
    context = get_user_context()
    if context is not None:
        context_user_id = context.user.id
    try:
        params = get_review_filter_params(movie_id=movie_id)

        reviews = get_filtered_reviews(params, user_id=context_user_id)

        total_count = count_filtered_reviews(params)

        return (
            jsonify(
                {
                    "reviews": [review.__dict__ for review in reviews],
                    "total_count": total_count,
                }
            ),
            200,
        )
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"error": "Error fetching reviews"}), 500


@bp.route("/<string:id>/reviews", methods=["POST"])
@authorization_guard
def createReview(id):
    body = json.loads(request.get_data().decode("utf-8"))
    user = getUserFromAccessToken()

    if user is None or body is None:
        return make_response(jsonify({"error": "Invalid body or user"}), 404)

    review_text: str = str(body["text"]).strip()
    rating: Optional[float] = None
    if "rating" in body and body["rating"] is not None:
        rating = float(body["rating"])
    else:
        rating = get_user_movie_rating(movie_id=id, user_id=user.id)

        if rating is None:
            return (
                jsonify({"error": "A rating is required before submitting a review"}),
                400,
            )

    review_id = add_review(id, user.id, review_text, rating)

    return jsonify({"message": "Review added", "review_id": review_id}), 201
