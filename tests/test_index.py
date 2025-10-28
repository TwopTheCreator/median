import tempfile, shutil
from median import MedianClient

def test_range_query():
    tmp = tempfile.mkdtemp()
    try:
        db = MedianClient(tmp)
        for age in range(10, 50, 5):
            db.insert({"age": age, "name": f"user{age}"})
        db.create_index("age")
        res = db.filter_range("age", 20, 30)
        assert len(res) == 3
        assert {r["age"] for r in res} == {20, 25, 30}
    finally:
        shutil.rmtree(tmp)
