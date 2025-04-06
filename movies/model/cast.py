from dataclasses import dataclass
from typing import List


@dataclass
class Cast:
    id: int
    name: str
    original_name: str
    profile_path: str
    cast_order: int
    characters: List[str]
