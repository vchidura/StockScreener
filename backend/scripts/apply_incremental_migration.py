#!/usr/bin/env python3
"""Apply one incremental SQL migration with the configured PostgreSQL administrator."""
from __future__ import annotations

import argparse
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

load_dotenv(BACKEND_DIR / ".env")

from initialize_database import configure_runtime_role  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("migration", type=Path)
    parser.add_argument(
        "--verify-table",
        help="Table that the restricted runtime role must be able to access after migration.",
    )
    return parser.parse_args()


def _settings() -> dict[str, str]:
    names = (
        "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT",
        "POSTGRES_ADMIN_USER", "POSTGRES_ADMIN_PASSWORD",
    )
    result = {name: os.getenv(name, "").strip() for name in names}
    missing = [name for name, value in result.items() if not value]
    if missing:
        raise RuntimeError("incremental migration requires: " + ", ".join(missing))
    return result


def main() -> int:
    args = _parse_args()
    migration = args.migration.resolve()
    if migration.parent != BACKEND_DIR / "migrations" or migration.suffix != ".sql":
        raise RuntimeError("migration must be a SQL file directly under backend/migrations")
    settings = _settings()
    version = migration.stem
    with psycopg2.connect(
        dbname=settings["DB_NAME"],
        user=settings["POSTGRES_ADMIN_USER"],
        password=settings["POSTGRES_ADMIN_PASSWORD"],
        host=settings["DB_HOST"],
        port=settings["DB_PORT"],
        options="-c timezone=UTC",
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(migration.read_text(encoding="utf-8"))
            cursor.execute(
                "INSERT INTO public.schema_migrations (version) VALUES (%s) "
                "ON CONFLICT (version) DO NOTHING",
                (version,),
            )
    configure_runtime_role(settings)
    with closing(psycopg2.connect(
        dbname=settings["DB_NAME"], user=settings["DB_USER"],
        password=settings["DB_PASSWORD"], host=settings["DB_HOST"],
        port=settings["DB_PORT"], options="-c timezone=UTC",
    )) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM public.schema_migrations "
                "WHERE version = %s)",
                (version,),
            )
            registered = bool(cursor.fetchone()[0])
            table_available = None
            if args.verify_table:
                cursor.execute(
                    "SELECT to_regclass(%s) IS NOT NULL",
                    (f"public.{args.verify_table}",),
                )
                table_available = bool(cursor.fetchone()[0])
    print(
        f"MIGRATION_APPLIED version={version} registered={registered} "
        f"runtime_table_available={table_available}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())