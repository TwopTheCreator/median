import argparse
import json
import sys
from .client import MedianClient


def main():
    parser = argparse.ArgumentParser(prog="median", description="MedianDB CLI")
    parser.add_argument("--db", default="./median_db", help="database folder")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_insert = sub.add_parser("insert")
    p_insert.add_argument("doc", help="JSON document to insert")

    p_get = sub.add_parser("get")
    p_get.add_argument("pk", help="_id to fetch")

    p_update = sub.add_parser("update")
    p_update.add_argument("pk", help="_id to update")
    p_update.add_argument("doc", help="new JSON document")

    p_delete = sub.add_parser("delete")
    p_delete.add_argument("pk", help="_id to delete")

    p_query = sub.add_parser("query")
    p_query.add_argument("field")
    p_query.add_argument("start", help="range start (inclusive)")
    p_query.add_argument("end", help="range end (inclusive)")

    args = parser.parse_args()
    db = MedianClient(args.db)

    if args.cmd == "insert":
        doc = json.loads(args.doc)
        print(db.insert(doc))
    elif args.cmd == "get":
        print(json.dumps(db.get(args.pk) or {}))
    elif args.cmd == "update":
        print(db.update(args.pk, json.loads(args.doc)))
    elif args.cmd == "delete":
        print(db.delete(args.pk))
    elif args.cmd == "query":
        for rec in db.filter_range(args.field, args.start, args.end):
            print(json.dumps(rec))


if __name__ == "__main__":
    main()
