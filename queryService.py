from collections import OrderedDict
from flask import request


def get_movie_filters():
    (
        search,
        ratingFrom,
        ratingTo,
        genres,
        release_date,
        tags,
        language,
    ) = (
        None,
        None,
        None,
        None,
        None,
        None,
        "en",
    )

    if request.args.get("search"):
        search = request.args.get("search")
    if request.args.get("ratingFrom"):
        ratingFrom = int(request.args.get("ratingFrom"))
    if request.args.get("ratingTo"):
        ratingTo = int(request.args.get("ratingTo"))
    if request.args.getlist("genre"):
        genres = request.args.getlist("genre", type=int)
    if request.args.get("release_date"):
        release_date = request.args.get("release_date")
    if request.args.get("tags"):
        tags = request.args.getlist("tags", type=int)
    if request.args.get("language"):
        language = request.args.get("language")

    filter = {}
    conditions = []

    if search is not None:
        conditions.append(
            {
                "$or": [
                    {"original_title": {"$regex": search, "$options": "i"}},
                    {"credits.cast.name": {"$regex": search, "$options": "i"}},
                    {"credits.crew.name": {"$regex": search, "$options": "i"}},
                ]
            }
        )

    if ratingFrom is not None and ratingTo is not None:
        filter["vote_average"] = {"$gte": ratingFrom, "$lte": ratingTo}

    filter["original_language"] = language

    if genres is not None:
        filter["genres.id"] = {"$all": genres}
    if release_date is not None:
        filter["release_date"] = {"$regex": release_date, "$options": "i"}
    if tags is not None:
        filter["keywords.id"] = {"$all": tags}

    if conditions:
        filter["$and"] = conditions

    return filter


def get_movie_sorting():
    sort: OrderedDict = {}

    if request.args.get("sort"):
        sortArgs = request.args.get("sort").split(",")

        for sortArg in sortArgs:
            if sortArg.find(":") > -1:
                field, direction = sortArg.split(":")
                sort[field] = int(direction)
            else:
                sort[sortArg] = 1
    if len(sort) == 0:
        sort["popularity"] = -1

    sort["_id"] = -1
    return sort


def get_review_filters(user_id):
    (isRecommended) = None

    if request.args.get("isRecommended"):
        isRecommended = eval(request.args.get("isRecommended").capitalize())

    filter = {}

    if user_id is not None:
        filter["reviews.user_id"] = user_id

    if isRecommended is not None:
        filter["reviews.isRecommended"] = isRecommended

    return filter


def get_review_sorting():
    sort: OrderedDict = {}

    if request.args.get("sort"):
        sortArgs = request.args.get("sort").split(",")

        for sortArg in sortArgs:
            if sortArg.find(":") > -1:
                field, direction = sortArg.split(":")
                sort["reviews." + field] = int(direction)
            else:
                sort[sortArg] = 1
    if not "reviews.date" in sort:
        sort["reviews.date"] = -1
    return sort
