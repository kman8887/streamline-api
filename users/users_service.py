import datetime
import json
from typing import List
import requests
import urllib.parse
from flask import g, make_response, jsonify, current_app as app, request
from users.model.movie_rating_request import MovieRatingRequest
from users.user import User, UserDetails
import users.users_dao as users_dao


def createSelfUser(access_token):
    auth_id = access_token.get("sub")

    usersFound = users_dao.getUserFromAccessToken(auth_id)
    if usersFound != None:
        return make_response(jsonify({"error": "User already created"}), 409)
    else:
        user = __getUserInfo(auth_id)

        created_user_id = users_dao.createUser(user)
        if created_user_id != None and len(created_user_id) > 0:
            created_user_id = created_user_id[0]
            user["id"] = str(created_user_id)
            __updateAppMetadata(auth_id, str(created_user_id))
            return make_response(jsonify(user), 201)
        else:
            return make_response(jsonify({"Error": "Failed to insert new user"}), 500)


def updateUser(access_token, user_id: int):
    auth_id = access_token.get("sub")

    user = users_dao.getUserFromAccessToken(auth_id)
    body = json.loads(request.get_data().decode("utf-8"))

    if user is None or body is None:
        return make_response(jsonify({"error": "Invalid body or user"}), 404)
    else:
        languages = body.get("language", None)
        region = body.get("region", None)
        watch_providers = body.get("watch_providers", None)

        if not isinstance(languages, list):
            languages = [languages]
        if not isinstance(watch_providers, list):
            watch_providers = [watch_providers]

        __updateUserMetadata(auth_id, languages, region)
        users_dao.updateUser(user_id, languages, region)
        users_dao.updateUserWatchProviders(user_id, watch_providers)

        return make_response(jsonify(user), 201)


def getUserDetails(id: int) -> UserDetails:
    return users_dao.getUserDetails(id)


def getUserWatchProviders(id: int):
    return users_dao.getUserWatchProviders(id)


def getUserFromAccessToken() -> User:
    access_token = g.get("access_token")
    auth_id = access_token.get("sub")

    return users_dao.getUserFromAccessToken(auth_id)


def get_bulk_movie_rating_request_body() -> List[MovieRatingRequest]:
    try:
        body = request.get_json()
        if not isinstance(body, list):
            raise ValueError("Request body must be a list of movies.")

        movies = []
        for movie in body:
            # Validate required fields
            if not all(
                key in movie
                for key in ["id", "title", "release_date", "poster_path", "rating"]
            ):
                raise ValueError(
                    "Each movie must include 'id', 'title', 'release_date', 'poster_path', and 'rating'."
                )

            # Validate rating range
            if not (0 <= movie["rating"] <= 10):
                raise ValueError(
                    f"Rating for movie '{movie['title']}' must be between 0 and 10."
                )

            # Create a MovieRatingRequest instance
            movies.append(MovieRatingRequest(**movie))

        return movies
    except (TypeError, ValueError) as e:
        raise ValueError(f"Invalid request body: {e}")


def __getUserInfo(auth_id):
    token = app.config["MGMT_API_ACCESS_TOKEN"]
    userId = urllib.parse.quote(auth_id)

    url = f"https://dev-n20bicxbia0qquf1.eu.auth0.com/api/v2/users/{userId}"

    payload = {}
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json()

    region, languages = None, None

    if data.get("user_metadata"):
        if data["user_metadata"].get("region"):
            region = data["user_metadata"]["region"]
        if data["user_metadata"].get("language"):
            languages = data["user_metadata"]["language"]
            if isinstance(languages, str):
                languages = [languages]  # Convert single string to a list
            languages = json.dumps(languages)

    return {
        "username": data["nickname"],
        "real_name": data["name"],
        "avatar": data["picture"],
        "region": region,
        "auth_id": auth_id,
        "creation_date": datetime.datetime.now(tz=datetime.timezone.utc),
        "languages": languages,
    }


def __updateUserMetadata(auth_id, languages, region):
    token = app.config["MGMT_API_ACCESS_TOKEN"]
    userId = urllib.parse.quote(auth_id)

    url = f"https://dev-n20bicxbia0qquf1.eu.auth0.com/api/v2/users/{userId}"

    payload = json.dumps({"user_metadata": {"language": languages, "region": region}})
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    return requests.request("PATCH", url, headers=headers, data=payload)


def __updateAppMetadata(auth_id, psql_id):
    token = app.config["MGMT_API_ACCESS_TOKEN"]
    userId = urllib.parse.quote(auth_id)

    url = f"https://dev-n20bicxbia0qquf1.eu.auth0.com/api/v2/users/{userId}"

    payload = json.dumps({"app_metadata": {"psql_id": psql_id}})
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    return requests.request("PATCH", url, headers=headers, data=payload)
