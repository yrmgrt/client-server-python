import threading
from typing import Dict, Any


class MetadataMap:
    def __init__(self):
        self.metadata_map: Dict[str, Any] = {}
        self.lock = threading.Lock()

    def get_metadata(self, key: str) -> Any:
        return self.metadata_map.get(key)

    def set_metadata(self, key: str, value: Any) -> None:
        with self.lock:
            self.metadata_map[key] = value

    def delete_metadata(self, key: str) -> None:
        with self.lock:
            if key in self.metadata_map:
                del self.metadata_map[key]

    def get_metadata_map(self) -> Dict[str, Any]:
        return self.metadata_map.copy()

    def clear_metadata(self) -> None:
        with self.lock:
            self.metadata_map.clear()

    def get_metadata_size(self) -> int:
        return len(self.metadata_map)

    def get_metadata_keys(self) -> list:
        return list(self.metadata_map.keys())

    def get_metadata_values(self) -> list:
        return list(self.metadata_map.values())

    def get_metadata_entries(self) -> list:
        return list(self.metadata_map.items())

    def has_metadata(self, key: str) -> bool:
        return key in self.metadata_map


metadata_map = MetadataMap()
