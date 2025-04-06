from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Review:
    review_id: int
    user_id: int
    username: str
    avatar: Optional[str]
    review_text: str
    created_at: str  # Using str for ISO 8601 timestamp handling
    like_count: int
    rating: Optional[float]
    isReviewLiked: Optional[bool] = None


@dataclass
class ReviewWithMovieDetails:
    review_id: int
    user_id: int
    username: str
    avatar: Optional[str]
    review_text: str
    created_at: str  # Using str for ISO 8601 timestamp handling
    like_count: int
    rating: Optional[float]
    movie_id: str
    title: str
    poster_path: str
    isReviewLiked: Optional[bool] = None


@dataclass
class ReviewLike:
    id: int
    review_id: int
    user_id: int
    created_at: str  # Using str for ISO 8601 timestamp handling


@dataclass
class ReviewResponse:
    reviews: List[Review]
    total_count: int
