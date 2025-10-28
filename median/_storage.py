import json
import os
import shutil
import threading
from typing import Dict, Any, List, Optional

LOG_EXTENSION = ".log"
COMPACTION_THRESHOLD = 5_000   # entries


class Storage:
    """Thread-safe append-only log with periodic compaction."""

    def __init__(self, path: str):
        self.path = path
        self.lock = threading.RLock()
        self.log_file = os.path.join(path, "data" + LOG_EXTENSION)
        self.tmp_file = os.path.join(path, "data.tmp")
        os.makedirs(path, exist_ok=True)
        if not os.path.exists(self.log_file):
            open(self.log_file, "ab").close()

    def append(self, record: Dict[str, Any]) -> None:
        with self.lock:
            with open(self.log_file, "ab") as f:
                f.write(json.dumps(record, separators=(",", ":")).encode() + b"\n")

    def scan(self) -> List[Dict[str, Any]]:
        """Return all records in insertion order."""
        with self.lock:
            with open(self.log_file, "rb") as f:
                return [json.loads(line) for line in f if line.strip()]

    def compact(self, latest: Dict[str, Dict[str, Any]]) -> None:
        """Rewrite log keeping only the latest version of each pk."""
        with self.lock:
            with open(self.tmp_file, "wb") as f:
                for rec in latest.values():
                    f.write(json.dumps(rec, separators=(",", ":")).encode() + b"\n")
            shutil.move(self.tmp_file, self.log_file)
