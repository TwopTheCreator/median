import time, tempfile, shutil
from median import MedianClient

def bench():
    tmp = tempfile.mkdtemp()
    db = MedianClient(tmp)
    start = time.perf_counter()
    for i in range(10_000):
        db.insert({"i": i, "data": "x" * 100})
    elapsed = time.perf_counter() - start
    print(f"10 k inserts in {elapsed:.2f}s  -> {10_000/elapsed:.0f} ops/sec")
    shutil.rmtree(tmp)

if __name__ == "__main__":
    bench()
