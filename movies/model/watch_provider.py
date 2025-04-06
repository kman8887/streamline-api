from dataclasses import dataclass

from movies.model.watch_provider_type import WatchProviderType


@dataclass
class WatchProvider:
    id: int
    name: str
    logo_path: str
    priority: int
    type: WatchProviderType
