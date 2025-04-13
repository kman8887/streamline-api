import json
from flask import Blueprint, jsonify, make_response, request
import reviews.reviews_service as reviews_service
from security.guards import authorization_guard
from users.users_service import getUserFromAccessToken


bp_name = "reviews"
bp_url_prefix = "/api/v1.0/reviews"
bp = Blueprint(bp_name, __name__, url_prefix=bp_url_prefix)


@bp.route("/<string:review_id>/like", methods=["POST"])
@authorization_guard
def updateReviewReaction(review_id):
    try:
        body = json.loads(request.get_data().decode("utf-8"))
        user = getUserFromAccessToken()

        if user is None or body is None:
            return make_response(jsonify({"error": "Invalid body or user"}), 404)

        isLiked: bool = bool(body["isLiked"])

        if isLiked is None:
            return make_response(jsonify({"error": "Invalid body"}), 400)

        reviews_service.toggle_like_review(review_id, user.id, isLiked)

        return make_response(jsonify({}), 201)

    except Exception:
        return make_response(jsonify({"error": "Invalid review ID"}), 404)
