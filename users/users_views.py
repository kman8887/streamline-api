from typing import Any, Dict
from flask import Blueprint, g, make_response, jsonify, request
from common.utils.context_service import get_user_context
from reviews.reviews_service import (
    count_filtered_reviews,
    get_filtered_reviews_with_movie_details,
    get_review_filter_params,
)

from schema.user_schema import CreateUserRequestSchema, CreateUserResponseSchema
from security.guards import authorization_guard, context_provider

from security.guards import (
    authorization_guard,
    create_users_permissions,
    unauthorized_error,
)

import user_movie_interactions.user_movie_interaction_service as user_movie_interaction_service
import users.users_service as users_service

bp_name = "users"
bp_url_prefix = "/api/v1.0/users"
bp = Blueprint(bp_name, __name__, url_prefix=bp_url_prefix)


@bp.route("", methods=["POST"], endpoint="createUser")
@authorization_guard
def createUser():
    access_token = g.get("access_token")

    if not access_token:
        return make_response(jsonify(unauthorized_error), 401)

    token_permissions = access_token.get("permissions")

    if not token_permissions:
        return users_service.createSelfUser(access_token)

    required_permissions_set = set([create_users_permissions.create])
    token_permissions_set = set(token_permissions)

    if not required_permissions_set.issubset(token_permissions_set):
        return users_service.createSelfUser(access_token)

    return make_response(jsonify({"message": "Not Implemented Yet"}), 200)


@bp.route("/<int:id>", methods=["PUT"])
@authorization_guard
def updateUser(id):
    access_token = g.get("access_token")

    if not access_token:
        return make_response(jsonify(unauthorized_error), 401)

    return users_service.updateUser(access_token, id)


@bp.route("/<int:id>", methods=["GET"])
def getUser(id):
    user = users_service.getUserDetails(id)
    if user is not None:
        return make_response(jsonify(user), 200)
    else:
        return make_response(jsonify({"error": "Invalid user ID"}), 404)


@bp.route("/<int:id>/watch-providers", methods=["GET"])
def getUserWatchProviders(id):
    try:
        watchProviders = users_service.getUserWatchProviders(id)
        return make_response(jsonify({"watchProviders": watchProviders}), 200)
    except Exception as e:
        print(f"Error fetching reviews: {e}")
        return jsonify({"error": "Error fetching watch providers"}), 500


@bp.route("/<int:user_id>/reviews", methods=["GET"])
@context_provider
def fetch_user_reviews(user_id: int) -> Dict[str, Any]:
    context_user_id = None
    try:
        context = get_user_context()

        if context is not None:
            context_user_id = context.user.id

        params = get_review_filter_params(user_id=user_id)

        reviews = get_filtered_reviews_with_movie_details(
            params, user_id=context_user_id
        )

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
        print(f"Error fetching reviews: {e}")
        return jsonify({"error": "Error fetching reviews"}), 500


@bp.route("/<int:user_id>/bulk-rate", methods=["POST"])
@authorization_guard
def rateMovies(user_id: int):
    try:
        movies = (
            users_service.get_bulk_movie_rating_request_body()
        )  # Parse and validate the request body
        user = users_service.getUserFromAccessToken()

        if user is None:
            return make_response(jsonify({"error": "Invalid user"}), 404)
        if user.id != user_id:
            return make_response(jsonify({"error": "No Permissions"}), 403)

        # Process the movies list
        user_movie_interaction_service.bulk_rate_movies(user_id, movies)

        return make_response(jsonify({"message": "Movies rated successfully"}), 200)
    except ValueError as e:
        return make_response(jsonify({"error": str(e)}), 400)
