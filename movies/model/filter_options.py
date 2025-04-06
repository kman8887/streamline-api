from dataclasses import dataclass
from typing import List
from movies.model.filter_option import FilterOption


@dataclass
class FilterOptions:
    genres: List[FilterOption]
    tags: List[FilterOption]
    watch_providers: List[FilterOption]
