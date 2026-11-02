#!/usr/bin/env python3
"""Check one database-fenced Advanced V3 service heartbeat."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import EXPECTED_V2_DATABASE_NAME  # noqa: E402


UTC = timezone.utc


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-dsn-file", type=Path, required=True)
    parser.add_argument("--role", required=True)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    dsn = args.app_dsn_file.read_text(encoding="utf-8").strip()
    if not dsn:
        raise RuntimeError("Advanced V3 health DSN is unavailable")
    checked_at = datetime.now(UTC)
    with psycopg2.connect(dsn) as connection:
        connection.set_session(readonly=True)
        with connection.cursor() as cursor:
            cursor.execute(
                f"SET LOCAL ROLE {EXPECTED_V2_DATABASE_NAME}_app"
            )
            cursor.execute(
                "SELECT runtime_kind,state,health,lease_until,"
                "observed_at FROM rt.advanced_realtime_service_states "
                "WHERE role_name=%s",
                (args.role,),
            )
            row = cursor.fetchone()
    status = (
        "UNAVAILABLE"
        if row is None
        else "STALE"
        if row[3] <= checked_at
        else str(row[2])
    )
    print(json.dumps({
        "role": args.role,
        "status": status,
        "runtime_kind": None if row is None else row[0],
        "state": None if row is None else row[1],
        "lease_until_utc": (
            None if row is None else row[3].isoformat()
        ),
        "observed_at_utc": (
            None if row is None else row[4].isoformat()
        ),
    }, sort_keys=True))
    return 0 if status == "HEALTHY" else 1


if __name__ == "__main__":
    raise SystemExit(main())
