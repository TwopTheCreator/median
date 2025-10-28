import bisect
from typing import Any, List, Dict, Optional


class Index:
    """Simple in-memory ordered index for one field."""

    def __init__(self):
        self.keys: List[Any] = []
        self.records: List[Dict[str, Any]] = []

    def insert(self, key: Any, record: Dict[str, Any]) -> None:
        pos = bisect.bisect_left(self.keys, key)
        if pos < len(self.keys) and self.keys[pos] == key:
            self.records[pos] = record
        else:
            self.keys.insert(pos, key)
            self.records.insert(pos, record)

    def delete(self, key: Any) -> bool:
        pos = bisect.bisect_left(self.keys, key)
        if pos < len(self.keys) and self.keys[pos] == key:
            self.keys.pop(pos)
            self.records.pop(pos)
            return True
        return False

    def range(self, start: Any, end: Any) -> List[Dict[str, Any]]:
        left = bisect.bisect_left(self.keys, start)
        right = bisect.bisect_right(self.keys, end)
        return self.records[left:right]

    def get(self, key: Any) -> Optional[Dict[str, Any]]:
        pos = bisect.bisect_left(self.keys, key)
        if pos < len(self.keys) and self.keys[pos] == key:
            return self.records[pos]
        return None
