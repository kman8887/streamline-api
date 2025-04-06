from typing import List, Optional
from dataclasses import dataclass


@dataclass
class MoviesFilterParams:
    search: Optional[str] = None
    genre_ids: Optional[List[int]] = None
    tag_ids: Optional[List[int]] = None
    release_date_from: Optional[str] = None
    release_date_to: Optional[str] = None
    rating_from: Optional[float] = None
    rating_to: Optional[float] = None
    watch_provider_ids: Optional[List[int]] = None
    region_filter: str = "GB"
    status_filter: Optional[str] = None
    languages: List[str] = "en"
    order_by: str = "popularity"
    order_direction: str = "DESC"
    limit_rows: int = 12
    offset_rows: int = 0
    user_id_param: Optional[int] = None
