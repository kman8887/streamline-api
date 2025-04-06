from dataclasses import dataclass
from typing import List


@dataclass
class Cast:
    id: int
    name: str
    original_name: str
    profile_path: str
    department: str
    job: str
