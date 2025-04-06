from dataclasses import dataclass
from typing import List


@dataclass
class User:
    id: int
    username: str
    real_name: str
    avatar: str
    region: str
    auth_id: str
    creation_date: str
    languages: List[str]


@dataclass
class UserDetails:
    id: int
    username: str
    real_name: str
    avatar: str
    region: str
    languages: List[str]
    auth_id: str
    creation_date: str
    avg_rating: int
    total_ratings: int
    total_likes: int
    total_reviews: int
