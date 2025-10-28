import tempfile, shutil, os
from median import MedianClient

def test_crud():
    tmp = tempfile.mkdtemp()
    try:
        db = MedianClient(tmp)
        pk = db.insert({"name": "Alice", "age": 30})
        assert db.get(pk)["name"] == "Alice"
        db.update(pk, {"name": "Alice", "age": 31})
        assert db.get(pk)["age"] == 31
        db.delete(pk)
        assert db.get(pk) is None
    finally:
        shutil.rmtree(tmp)
