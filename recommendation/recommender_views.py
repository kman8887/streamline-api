import datetime
import threading
from flask import Blueprint, g, make_response, jsonify, request
from common.utils import azure_blob

from security.guards import authorization_guard

from security.guards import (
    authorization_guard,
    unauthorized_error,
)

import users.users_service as users_service
import recommendation.recommendation_service as recommendation_service
import recommendation.hybrid_recommendation_service as hybrid_recommendation_service
from common.utils.utils import cache


bp_name = "recommendation"
bp_url_prefix = "/api/v1.0/recommendation"
bp = Blueprint(bp_name, __name__, url_prefix=bp_url_prefix)
LOCK_EXPIRY_SECONDS = 30 * 60


@bp.route("/<int:user_id>", methods=["GET"])
def getRecommendations(user_id):
    movie_id = request.args.get("movie_id")

    if movie_id is None:
        return make_response(jsonify({"error": "Invalid Movie Id"}), 400)

    recommendation = recommendation_service.get_recommendations(user_id, movie_id)

    if recommendation is None or len(recommendation) == 0:
        recommendation = get_baseline_recs(movie_id)

    if recommendation is None:
        return make_response(jsonify({"error": "No recommendations found"}), 404)

    return make_response(
        jsonify(recommendation),
        200,
    )


@bp.route("/generate", methods=["GET"])
@authorization_guard
def generate_recommendations():
    print("Generating recommendations for user:")
    access_token = g.get("access_token")

    if not access_token:
        return make_response(jsonify(unauthorized_error), 401)

    user_id = users_service.getUserFromAccessToken().id

    if not user_id:
        return make_response(jsonify(unauthorized_error), 401)

    last_updated = recommendation_service.get_last_recommendation_update(user_id)

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    print(f"Last updated: {last_updated}, Now: {now}")
    if last_updated is not None and last_updated > (now - (datetime.timedelta(days=1))):
        print("Recommendations already generated recently: " + str(user_id))
        return make_response(
            jsonify({"error": "Recommendations already generated recently"}), 200
        )

    lock_key = f"user:{user_id}:recommendation_lock"

    if cache.get(lock_key):
        print("Already Processing User: " + str(user_id))
        return make_response(jsonify({"status": "already_processing"}), 200)

    cache.set(lock_key, True, timeout=LOCK_EXPIRY_SECONDS)

    def async_task():
        try:
            hybrid_recommendation_service.generate_user_hybrid_recommendations(
                str(user_id)
            )
        finally:
            cache.delete(lock_key)
            print(f"Lock released for user {user_id}")

    print("Generating recommendations for user:" + str(user_id))

    threading.Thread(target=async_task).start()

    return make_response(jsonify({"status": "started"}), 200)


def get_baseline_recs(movie_id: str):
    artifacts = azure_blob.load_artifacts()
    return artifacts["baseline_recs"].get(movie_id)
