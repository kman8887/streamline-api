from dataclasses import dataclass
from typing import Optional


@dataclass
class ReviewFilterParams:
    movie_id: str
    user_id: int
    rating_from: Optional[float] = None
    rating_to: Optional[float] = None
    order_by: str = "like_count"
    order_direction: str = "DESC"
    limit_rows: int = 10
    offset_rows: int = 0
