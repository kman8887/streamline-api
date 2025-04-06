import threading
from bson import ObjectId
from flask import Blueprint, g, make_response, jsonify, request
from common.utils import azure_blob
from queryService import get_review_filters, get_review_sorting
from reviews.reviews_service import getReviewsPipeline
import json

from security.guards import authorization_guard

from security.guards import (
    authorization_guard,
    generate_recommendation_permissions,
    unauthorized_error,
)

import user_movie_interactions.user_movie_interaction_service as user_movie_interaction_service
import users.users_service as users_service
from common.utils.utils import movies_db
import recommendation.recommendation_service as recommendation_service
import recommendation.hybrid_recommendation_service as hybrid_recommendation_service


bp_name = "recommendation"
bp_url_prefix = "/api/v1.0/recommendation"
bp = Blueprint(bp_name, __name__, url_prefix=bp_url_prefix)


@bp.route("/<int:user_id>", methods=["GET"])
def getRecommendations(user_id):
    movie_id = request.args.get("movie_id")

    if movie_id is None:
        return make_response(jsonify({"error": "Invalid Movie Id"}), 400)

    recommendation = recommendation_service.get_recommendations(user_id, movie_id)

    if recommendation is None or len(recommendation) == 0:
        recommendation = get_baseline_recs()

    print(recommendation)

    return make_response(
        jsonify(recommendation),
        200,
    )


@bp.route("/generate", methods=["GET"])
@authorization_guard
def generate_recommendations(user_id: int):
    access_token = g.get("access_token")

    if not access_token:
        return make_response(jsonify(unauthorized_error), 401)

    token_permissions = access_token.get("permissions")

    user_id = users_service.getUserFromAccessToken().id
    if not user_id:
        return make_response(jsonify(unauthorized_error), 401)

    def async_task():
        hybrid_recommendation_service.generate_user_hybrid_recommendations(str(user_id))

    if token_permissions:
        required_permissions_set = set([generate_recommendation_permissions.create])
        token_permissions_set = set(token_permissions)

        if required_permissions_set.issubset(token_permissions_set):
            return make_response(jsonify({"message": "Not Implemented Yet"}), 200)

    threading.Thread(target=async_task).start()

    return make_response(jsonify({"status": "started"}), 200)


def get_baseline_recs(movie_id: str):
    artifacts = azure_blob.load_artifacts()
    return artifacts["baseline_recs"][movie_id]
