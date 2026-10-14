#!/usr/bin/env python3
"""Print the V2-10 reference-copy PLAN (no executor exists; nothing is copied).

Provisions a disposable test_realtime_v2_* database from backend/realtime/sql_v2, reads its catalog
to derive the copy scope, foreign-key order, columns, sequences and parity statements, prints the plan
as JSON and always drops the database. It never connects to stocks_realtime_v1 or stocks_realtime_v2
and makes no provider request. Copying reference data from v1 needs separate, explicit authorization.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import closing
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import (  # noqa: E402
    SECURITY_MODE_DEFERRED,
    V2_MIGRATIONS_DIR,
    load_realtime_migrations,
    validate_target_database_v2,
)
from realtime.database_access_v2 import DISPOSABLE_V2_PREFIX  # noqa: E402
from realtime.reference_copy_plan import build_reference_copy_plan  # noqa: E402


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, help="disposable name, test_realtime_v2_*")
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--output", help="also write the plan JSON to this path")
    parser.add_argument("--execute", action="store_true", help="refused: the copy executor is not built")
    args = parser.parse_args(argv)
    if args.execute:
        parser.error("the reference-copy executor is not built; V2-10 needs explicit authorization first")
    validate_target_database_v2(args.database, args.confirm_database_name, allow_disposable=True,
                                security_mode=SECURITY_MODE_DEFERRED)
    # This script creates and drops the database it is given: only a disposable name is acceptable.
    if not args.database.startswith(DISPOSABLE_V2_PREFIX):
        raise ValueError(f"the plan builds a disposable database; the name must start with {DISPOSABLE_V2_PREFIX}")
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
        dbname=database, user=settings["POSTGRES_ADMIN_USER"],
        password=settings["POSTGRES_ADMIN_PASSWORD"], host=settings["DB_HOST"],
        port=settings["DB_PORT"], application_name="plan_reference_copy", options="-c timezone=UTC")


def main(argv=None) -> int:
    args = parse_args(argv)
    settings = _settings()
    created = False
    try:
        with closing(_connect(settings, "postgres")) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname=%s)", (args.database,))
                if cursor.fetchone()[0]:
                    raise RuntimeError("disposable database already exists; refusing ownership")
                cursor.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0 ENCODING 'UTF8'")
                               .format(sql.Identifier(args.database)))
        created = True
        with closing(_connect(settings, args.database)) as connection:
            for migration in load_realtime_migrations(V2_MIGRATIONS_DIR):
                with connection.cursor() as cursor:
                    cursor.execute(migration.sql)
                connection.commit()
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
                plan = build_reference_copy_plan(cursor)
            connection.rollback()
        text = json.dumps(plan, indent=2, default=str)
        if args.output:
            Path(args.output).write_text(text + "\n", encoding="utf-8")
        print(text)
        return 0
    finally:
        if created:
            with closing(_connect(settings, "postgres")) as connection:
                connection.autocommit = True
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                                   "WHERE datname=%s AND pid <> pg_backend_pid()", (args.database,))
                    cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(args.database)))


if __name__ == "__main__":
    raise SystemExit(main())
