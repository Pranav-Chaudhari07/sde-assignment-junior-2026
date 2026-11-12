"""CLI for Assignment A.

Usage (from the repo root, after ``pip install -r requirements.txt``):

    python -m solution.pipeline ingest data/drop_1
    python -m solution.pipeline ingest data/drop_2
    python -m solution.pipeline report --out reports/

Options: ``--db`` (default ``au.db`` in the current directory),
``--schemas`` (default: auto-discovered ``assignment-a-data/schemas``).
Re-running ``ingest`` on the same drop changes nothing.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import db as dbmod
from .ingest import Ingestor, Schemas, find_schemas_dir
from .pii import PiiProtector
from .reports import generate_reports


def _build(db_path: str, schemas_dir: str | None) -> Ingestor:
    schemas = Schemas(find_schemas_dir() if schemas_dir is None
                      else Path(schemas_dir))
    conn = dbmod.connect(db_path)
    return Ingestor(conn, schemas, PiiProtector(db_path))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="solution.pipeline")
    parser.add_argument("--db", default="au.db",
                        help="SQLite database file (default: au.db)")
    parser.add_argument("--schemas", default=None,
                        help="schemas directory (default: auto-discovered)")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest_p = sub.add_parser("ingest", help="ingest one or more drops")
    ingest_p.add_argument("drops", nargs="+", help="drop directory/ies")

    report_p = sub.add_parser("report", help="write the five report files")
    report_p.add_argument("--out", required=True, help="output directory")

    args = parser.parse_args(argv)
    ingestor = _build(args.db, args.schemas)
    try:
        if args.command == "ingest":
            for drop in args.drops:
                result = ingestor.ingest_drop(Path(drop))
                for path, stats in result["files"]:
                    if stats.get("skipped"):
                        print(f"skip (already ingested): {path}")
                    else:
                        print(f"ingested: {path} {stats}")
            print("ingest done.")
        elif args.command == "report":
            written = generate_reports(ingestor.conn, args.out)
            for name, count in written.items():
                print(f"wrote {name}: {count} row(s)")
            print(f"reports written to {args.out}/")
    finally:
        ingestor.conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
