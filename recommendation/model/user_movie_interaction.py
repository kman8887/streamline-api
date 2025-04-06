from dataclasses import dataclass
from typing import List


@dataclass
class UserMovieInteraction:
    user_id: int
    movie_id: str
    interaction_type: str
    created_at: str
    rating: int


@dataclass
class MovieMetadata:
    movie_id: str
    title: str
    popularity: float
    vote_average: float
    vote_count: int
    original_language: str
    release_decade: str
    runtime: str
    genres: List[str]
    tags: List[str]
    director: List[str]
    writer: List[str]
    top_2_cast: List[str]
