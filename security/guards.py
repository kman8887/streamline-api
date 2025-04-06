from functools import wraps
from http import HTTPStatus
from types import SimpleNamespace

from flask import request, g

from security.auth0_service import auth0_service
from common.utils.utils import json_abort

unauthorized_error = {"message": "Requires authentication"}

invalid_request_error = {
    "error": "invalid_request",
    "error_description": "Authorization header value must follow this format: Bearer access-token",
    "message": "Requires authentication",
}

create_users_permissions = SimpleNamespace(create="create:users")
generate_recommendation_permissions = SimpleNamespace(create="create:recommendation")
reviews_permissions = SimpleNamespace(delete="delete:reviews", update="update:reviews")
games_permissions = SimpleNamespace(create="create:games", delete="delete:games")


def __get_bearer_token_from_request():
    authorization_header = request.headers.get("Authorization", None)

    if not authorization_header:
        json_abort(HTTPStatus.UNAUTHORIZED, unauthorized_error)
        return

    authorization_header_elements = authorization_header.split()

    if len(authorization_header_elements) != 2:
        json_abort(HTTPStatus.BAD_REQUEST, invalid_request_error)
        return

    auth_scheme = authorization_header_elements[0]
    bearer_token = authorization_header_elements[1]

    if not (auth_scheme and auth_scheme.lower() == "bearer"):
        json_abort(HTTPStatus.UNAUTHORIZED, unauthorized_error)
        return

    if not bearer_token:
        json_abort(HTTPStatus.UNAUTHORIZED, unauthorized_error)
        return

    return bearer_token


def context_provider(function):
    @wraps(function)
    def decorator(*args, **kwargs):
        if request.headers.get("Authorization", None) is None:
            return function(*args, **kwargs)
        else:
            token = __get_bearer_token_from_request()
            validated_token = auth0_service.validate_jwt(token)

            g.access_token = validated_token

            return function(*args, **kwargs)

    return decorator


def authorization_guard(function):
    @wraps(function)
    def decorator(*args, **kwargs):
        token = __get_bearer_token_from_request()
        validated_token = auth0_service.validate_jwt(token)

        g.access_token = validated_token

        return function(*args, **kwargs)

    return decorator


def permissions_guard(required_permissions=None):
    def decorator(function):
        @wraps(function)
        def wrapper(*args, **kwargs):
            access_token = g.get("access_token")

            if not access_token:
                json_abort(401, unauthorized_error)
                return

            if required_permissions is None:
                return function(*args, **kwargs)

            if not isinstance(required_permissions, list):
                json_abort(500, {"message": "Internal Server Error"})

            token_permissions = access_token.get("permissions")

            if not token_permissions:
                json_abort(403, {"message": "Permission denied"})

            required_permissions_set = set(required_permissions)
            token_permissions_set = set(token_permissions)

            if not required_permissions_set.issubset(token_permissions_set):
                json_abort(403, {"message": "Permission denied"})

            return function(*args, **kwargs)

        return wrapper

    return decorator


def extract_headers(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Extract headers with defaults
        g.region = request.headers.get("X-Region", "GB")  # Default to "GB"
        g.timezone = request.headers.get("X-Timezone", "UTC")  # Default to "UTC"
        g.languages = request.headers.get("X-Languages", "en").split(
            ","
        )  # Default to ["en"]

        # Call the original function
        return func(*args, **kwargs)

    return wrapper
