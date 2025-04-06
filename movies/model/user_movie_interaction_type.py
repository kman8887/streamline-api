from enum import Enum


class UserMovieInteractionType(Enum):
    RATING = "RATING"
    LIKE = "LIKE"
    WATCHED = "WATCHED"
