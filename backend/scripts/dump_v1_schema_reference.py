#!/usr/bin/env python3
"""Dump the final v1 realtime schema from a disposable database as a V2 design reference.

Creates a test_realtime_ database, applies the v1 migration chain, runs pg_dump
(schema only), then drops the database. It never connects to stocks_realtime_v1,
makes no provider request and keeps no data.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import load_realtime_migrations, validate_target_database

PG_DUMP = Path(r"C:\Program Files\PostgreSQL\17\bin\pg_dump.exe")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    validate_target_database(args.database, args.confirm_database_name, allow_disposable=True)
    if not args.database.startswith("test_realtime_"):
        parser.error("only test_realtime_ disposable databases are accepted")
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
        port=settings["DB_PORT"], application_name="dump_v1_schema_reference",
        options="-c timezone=UTC",
    )


def main() -> int:
    args = parse_args()
    if not args.execute:
        print(json.dumps({"database": args.database, "mode": "PLAN",
                          "actions": ("CREATE", "MIGRATE_V1", "PG_DUMP_SCHEMA", "DROP")}))
        print("No database changes made. Add --execute.")
        return 0
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
        migrations = load_realtime_migrations()
        with closing(_connect(settings, args.database)) as connection:
            for migration in migrations:
                with connection.cursor() as cursor:
                    cursor.execute(migration.sql)
                    cursor.execute(
                        "INSERT INTO rt.schema_migrations (version, checksum_sha256) VALUES (%s, %s)",
                        (migration.version, migration.checksum_sha256))
                connection.commit()
        environment = {**os.environ, "PGPASSWORD": settings["POSTGRES_ADMIN_PASSWORD"]}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        result = subprocess.run(
            [str(PG_DUMP), "--schema-only", "--no-owner", "--no-privileges", "--no-comments",
             "-h", settings["DB_HOST"], "-p", settings["DB_PORT"], "-U", settings["POSTGRES_ADMIN_USER"],
             "-d", args.database, "-f", str(args.output)],
            env=environment, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            raise RuntimeError("pg_dump failed: " + result.stderr[:500])
        print(json.dumps({"status": "DUMPED", "migrations": len(migrations),
                          "output": str(args.output), "bytes": args.output.stat().st_size}))
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
