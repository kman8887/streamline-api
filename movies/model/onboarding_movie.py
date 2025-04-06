from dataclasses import dataclass
from typing import Optional


@dataclass
class OnboardingMovie:
    id: str
    title: str
    release_date: Optional[str]
    poster_path: Optional[str]
