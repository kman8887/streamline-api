from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from movies.model.user_movie_interaction_type import UserMovieInteractionType


@dataclass
class UserMovieInteraction:
    id: int
    user_id: int
    movie_id: str
    interaction_type: str
    created_at: str


@dataclass
class Review(UserMovieInteraction):
    review_text: str


@dataclass
class Rating(UserMovieInteraction):
    rating: float


@dataclass
class UserMovieInteractionData:
    review: Review
    rating: Rating
    watched: bool
    liked: bool


@dataclass
class MovieDetailsUserInteraction:
    id: int
    type: UserMovieInteractionType  # 'WATCHED', 'RATING', 'REVIEW', 'LIKE'
    rating: Optional[float] = None
    created_at: Optional[datetime] = None
