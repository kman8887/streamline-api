import json
from bson import ObjectId
from flask import Blueprint, g, jsonify, make_response, request

from common.utils.utils import movies_db, users_db
import reviews.reviews_service as reviews_service
from security.guards import authorization_guard, reviews_permissions
from users.users_service import getUserFromAccessToken


bp_name = "reviews"
bp_url_prefix = "/api/v1.0/reviews"
bp = Blueprint(bp_name, __name__, url_prefix=bp_url_prefix)


@bp.route("/<string:review_id>", methods=["PUT"])
@authorization_guard
def updateReview(review_id):
    error_response = reviews_service.isOwnReviewOrHasPermission(
        reviews_permissions.update, review_id
    )
    if error_response:
        return error_response
    else:
        body = json.loads(request.get_data().decode("utf-8"))

        text = body.get("text")
        hours = body.get("hours")
        isRecommended = body.get("isRecommended")

        if text is None or hours is None or isRecommended is None:
            return make_response(jsonify({"error": "Missing Body Params"}), 400)
        else:
            updated_review = {
                "reviews.$.text": text,
                "reviews.$.hours": hours,
                "reviews.$.isRecommended": isRecommended,
            }

            result = movies_db.update_one(
                {"reviews._id": ObjectId(review_id)}, {"$set": updated_review}
            )

            if result.matched_count == 1:
                return make_response(jsonify({"review_id": review_id}), 200)
            else:
                return make_response(jsonify({"error": "Invalid review ID"}), 404)


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


@bp.route("/<string:review_id>", methods=["DELETE"])
@authorization_guard
def delete_review(review_id):
    error_response = reviews_service.isOwnReviewOrHasPermission(
        reviews_permissions.delete, review_id
    )
    if error_response:
        return error_response
    else:
        review_query = {"reviews._id": ObjectId(review_id)}
        review_projection = {"reviews.$": 1, "review_count": 1}
        game = movies_db.find_one(review_query, review_projection)

        if game:
            review = game.get("reviews")[0]
            user_id = review["user_id"]

            found_funny = -len(review.get("found_funny", []))
            found_helpful = -len(review.get("found_helpful", []))
            found_not_helpful = -len(review.get("found_not_helpful", []))

            movies_db.update_one(
                {"reviews._id": ObjectId(review_id)},
                {
                    "$pull": {"reviews": {"_id": ObjectId(review_id)}},
                    "$inc": {"review_count": -1},
                },
            )

            users_db.update_one(
                {"_id": ObjectId(user_id)},
                {
                    "$inc": {
                        "totalReviews": -1,
                        "totalFoundFunny": found_funny,
                        "totalFoundHelpful": found_helpful,
                        "totalFoundNotHelpful": found_not_helpful,
                    },
                },
            )
            return make_response(jsonify({}), 204)

        else:
            return make_response(jsonify({"error": "Invalid review ID"}), 404)
