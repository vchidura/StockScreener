#!/usr/bin/env python3
"""Drop expired whole partitions of the ordinary-retention relations (HOT_TTL and KEPT_3M).

DRY RUN by default: lists the partitions that would be dropped and the catalogs that would be pruned
and issues no DDL and no DML. --execute detaches (CONCURRENTLY) and drops each partition whose whole
range ended at or before today minus its policy window, then prunes the catalog rows that went with
them; see backend/realtime/partition_retention.py for the exact window rule. A failure on one
partition, policy or catalog raises an alarm in the report and the run continues; the exit code is 1
when there is any alarm. HISTORY_PLAN, SESSION_WINDOW, ARCHIVE_FIRST, DEFERRED and BOUNDED relations
are never touched (the archive worker owns ARCHIVE_FIRST).

For now only test_realtime_v2_* databases are accepted; stocks_realtime_v2 is refused. This script
makes no provider request and never prints credentials. While security is deferred it connects as the
database owner; the hardened phase runs it as the archivist principal.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import validate_target_database_v2  # noqa: E402
from realtime.partition_retention import (  # noqa: E402
    ELIGIBLE_RELATIONS,
    PartitionRetentionExecutor,
    PartitionRetentionRepository,
    PostgresPartitionRetentionRepository,
)

DISPOSABLE_PREFIX = "test_realtime_v2_"


def validate_target(database: str, confirmation: str) -> str:
    target = validate_target_database_v2(database, confirmation, allow_disposable=True)
    if not target.startswith(DISPOSABLE_PREFIX):
        raise ValueError(f"only {DISPOSABLE_PREFIX}* databases are accepted for now")
    return target


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--execute", action="store_true", help="drop partitions (default is a dry run)")
    parser.add_argument("--as-of-date", type=date.fromisoformat, default=None,
                        help="evaluate the windows as of this UTC date (never later than today)")
    parser.add_argument("--relation", action="append", choices=ELIGIBLE_RELATIONS, default=None,
                        help="limit the run to this relation (repeatable)")
    parser.add_argument("--include-kept", action="store_true", help="list the kept partitions by name")
    args = parser.parse_args(argv)
    try:
        validate_target(args.database, args.confirm_database_name)
    except ValueError as error:
        parser.error(str(error))
    return args


def _settings() -> dict[str, str]:
    load_dotenv(BACKEND_DIR / ".env", override=True)
    names = ("DB_HOST", "DB_PORT", "POSTGRES_ADMIN_USER", "POSTGRES_ADMIN_PASSWORD")
    settings = {name: os.getenv(name, "").strip() for name in names}
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError("requires: " + ", ".join(missing))
    return settings


def _connect(settings: dict[str, str], database: str):
    return psycopg2.connect(
        dbname=database, user=settings["POSTGRES_ADMIN_USER"], password=settings["POSTGRES_ADMIN_PASSWORD"],
        host=settings["DB_HOST"], port=settings["DB_PORT"], application_name="run_realtime_v2_retention",
        options="-c timezone=UTC")


def main(argv=None, *, repository: PartitionRetentionRepository | None = None) -> int:
    args = parse_args(argv)
    if repository is None:
        settings = _settings()
        repository = PostgresPartitionRetentionRepository(lambda: _connect(settings, args.database))
    try:
        report = PartitionRetentionExecutor(repository).run(
            execute=args.execute, as_of_date=args.as_of_date, relations=args.relation)
    except ValueError as error:
        print(f"refused: {error}", file=sys.stderr)
        return 2
    print(json.dumps(report.as_dict(include_kept=args.include_kept), indent=2))
    if not args.execute:
        print("No database changes made. Add --execute.")
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
