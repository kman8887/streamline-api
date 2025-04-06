from typing import Any, List, Optional
from dataclasses import dataclass

from movies.model.genre import Genre


@dataclass
class MovieData:
    id: str
    title: str
    vote_average: float
    release_date: Optional[str]
    genres: List[Genre]
    poster_path: Optional[str]
    backdrop_path: Optional[str]
    recommendation_score: Optional[float] = None


@dataclass
class WatchlistMovie:
    id: str
    title: str
    vote_average: float
    release_date: Optional[str]
    poster_path: Optional[str]
    backdrop_path: Optional[str]
    recommendation_score: Optional[float] = None
