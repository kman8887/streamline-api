from dataclasses import dataclass
from typing import List, Optional

from movies.model.genre import Genre
from movies.model.tag import Tag
from movies.model.user_movie_interactions import MovieDetailsUserInteraction
from movies.model.watch_provider import WatchProvider


@dataclass
class Movie:
    id: str
    title: str
    vote_average: float
    vote_count: int
    release_date: Optional[str]
    genres: List[Genre]
    tags: List[Tag]
    backdrop_path: Optional[str]
    tagline: Optional[str]
    overview: Optional[str]
    runtime: Optional[int]
    status: Optional[str]
    original_language: Optional[str]
    watch_providers: List[WatchProvider]
    director: List[str]
    writer: List[str]
    top_cast: List[str]
    user_interactions: Optional[List[MovieDetailsUserInteraction]] = None
