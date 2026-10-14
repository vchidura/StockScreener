#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import closing
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import (
    plan_realtime_migrations,
    validate_target_database,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan or apply the independent Advanced realtime migrations."
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--allow-disposable-target",
        action="store_true",
        help="Allow a database whose name begins with test_realtime_.",
    )
    args = parser.parse_args()
    validate_target_database(
        args.database,
        args.confirm_database_name,
        allow_disposable=args.allow_disposable_target,
    )
    return args


def _settings() -> dict[str, str]:
    load_dotenv(BACKEND_DIR / ".env", override=True)
    names = (
        "DB_HOST",
        "DB_PORT",
        "POSTGRES_ADMIN_USER",
        "POSTGRES_ADMIN_PASSWORD",
    )
    settings = {name: os.getenv(name, "").strip() for name in names}
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError(
            "realtime migration runner requires: " + ", ".join(missing)
        )
    return settings


def _read_applied(connection) -> dict[str, str]:
    with connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '10s'")
        cursor.execute("SELECT to_regclass('rt.schema_migrations')")
        if cursor.fetchone()[0] is None:
            connection.rollback()
            return {}
        cursor.execute(
            "SELECT version, checksum_sha256 FROM rt.schema_migrations "
            "ORDER BY version"
        )
        applied = {version: checksum for version, checksum in cursor.fetchall()}
    connection.rollback()
    return applied


def main() -> int:
    args = parse_args()
    settings = _settings()
    connection_settings = {
        "dbname": args.database,
        "user": settings["POSTGRES_ADMIN_USER"],
        "password": settings["POSTGRES_ADMIN_PASSWORD"],
        "host": settings["DB_HOST"],
        "port": settings["DB_PORT"],
        "application_name": "apply_realtime_migrations",
        "options": "-c timezone=UTC",
    }
    with closing(psycopg2.connect(**connection_settings)) as connection:
        plan = plan_realtime_migrations(
            args.database,
            _read_applied(connection),
        )
        print(
            json.dumps(
                {
                    "database": plan.database_name,
                    "mode": "APPLY" if args.apply else "PLAN",
                    "schema_sha256": plan.schema_sha256,
                    "applied_versions": plan.applied_versions,
                    "pending_versions": tuple(
                        migration.version for migration in plan.pending
                    ),
                },
                indent=2,
            )
        )
        if not args.apply:
            print("No migrations applied. Add --apply after reviewing this plan.")
            return 0
        for migration in plan.pending:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL statement_timeout = '30s'")
                    cursor.execute("SET LOCAL lock_timeout = '5s'")
                    cursor.execute(migration.sql)
                    cursor.execute(
                        "INSERT INTO rt.schema_migrations "
                        "(version, checksum_sha256) VALUES (%s, %s)",
                        (migration.version, migration.checksum_sha256),
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise
    print(
        f"REALTIME_MIGRATIONS_APPLIED database={args.database} "
        f"count={len(plan.pending)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())