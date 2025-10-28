from typing import Dict, Any, List, Optional
from ._engine import Engine


class MedianClient:
    """Tiny database client."""

    def __init__(self, path: str = "./median_db"):
        self.engine = Engine(path)

    def insert(self, doc: Dict[str, Any]) -> str:
        """Insert a document and return its _id."""
        return self.engine.insert(doc)

    def get(self, pk: str) -> Optional[Dict[str, Any]]:
        """Fetch by primary key."""
        return self.engine.find_by_id(pk)

    def update(self, pk: str, doc: Dict[str, Any]) -> bool:
        """Replace document with given _id."""
        return self.engine.update(pk, doc)

    def delete(self, pk: str) -> bool:
        """Delete by primary key."""
        return self.engine.delete(pk)

    def filter_range(self, field: str, start: Any, end: Any) -> List[Dict[str, Any]]:
        """Range query on indexed field."""
        return self.engine.find_range(field, start, end)

    def create_index(self, field: str):
        """Create an index on field."""
        self.engine.create_index(field)
