from dataclasses import dataclass


@dataclass
class MovieRatingRequest:
    id: str
    title: str
    release_date: str  # Use ISO 8601 string format for dates
    poster_path: str
    rating: float
