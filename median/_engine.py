import uuid
from typing import Dict, Any, List, Optional, Set
from ._storage import Storage, COMPACTION_THRESHOLD
from ._index import Index


class Engine:
    def __init__(self, path: str):
        self.storage = Storage(path)
        self.indexes: Dict[str, Index] = {}
        self.deleted: Set[str] = set()
        self.count = 0
        self._load()

    # ---------- internal ----------
    def _load(self):
        for rec in self.storage.scan():
            pk = rec["_id"]
            if rec.get("_action") == "delete":
                self.deleted.add(pk)
            else:
                self.deleted.discard(pk)
                self._index_record(rec)
            self.count += 1
        if self.count > COMPACTION_THRESHOLD:
            self._run_compaction()

    def _index_record(self, rec: Dict[str, Any]):
        for field, idx in self.indexes.items():
            if field in rec:
                idx.insert(rec[field], rec)

    def _unindex_record(self, rec: Dict[str, Any]):
        for field, idx in self.indexes.items():
            if field in rec:
                idx.delete(rec[field])

    def _run_compaction(self):
        latest = {}
        for rec in self.storage.scan():
            pk = rec["_id"]
            if rec.get("_action") != "delete":
                latest[pk] = rec
        self.storage.compact(latest)
        self.count = len(latest)

    # ---------- public ----------
    def create_index(self, field: str):
        if field in self.indexes:
            return
        self.indexes[field] = Index()
        for rec in self.storage.scan():
            if rec.get("_action") != "delete" and field in rec:
                self.indexes[field].insert(rec[field], rec)

    def insert(self, doc: Dict[str, Any]) -> str:
        pk = doc.get("_id") or str(uuid.uuid4())
        doc["_id"] = pk
        if pk in self.deleted:
            self.deleted.remove(pk)
        self.storage.append(doc)
        self._index_record(doc)
        self.count += 1
        return pk

    def find_by_id(self, pk: str) -> Optional[Dict[str, Any]]:
        if pk in self.deleted:
            return None
        for rec in self.storage.scan():
            if rec["_id"] == pk and rec.get("_action") != "delete":
                return rec
        return None

    def find_range(self, field: str, start: Any, end: Any) -> List[Dict[str, Any]]:
        if field not in self.indexes:
            self.create_index(field)
        return self.indexes[field].range(start, end)

    def update(self, pk: str, doc: Dict[str, Any]) -> bool:
        existing = self.find_by_id(pk)
        if not existing:
            return False
        self._unindex_record(existing)
        doc["_id"] = pk
        self.storage.append(doc)
        self._index_record(doc)
        return True

    def delete(self, pk: str) -> bool:
        if self.find_by_id(pk) is None:
            return False
        self.storage.append({"_id": pk, "_action": "delete"})
        self.deleted.add(pk)
        return True
