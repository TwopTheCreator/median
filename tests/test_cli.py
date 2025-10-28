import subprocess, tempfile, shutil, os, json

def test_cli_insert_get():
    tmp = tempfile.mkdtemp()
    try:
        db_path = os.path.join(tmp, "db")
        doc = '{"name": "Bob", "score": 42}'
        out = subprocess.check_output(["python", "-m", "median", "--db", db_path, "insert", doc], text=True)
        pk = out.strip()
        out = subprocess.check_output(["python", "-m", "median", "--db", db_path, "get", pk], text=True)
        assert json.loads(out)["name"] == "Bob"
    finally:
        shutil.rmtree(tmp)
