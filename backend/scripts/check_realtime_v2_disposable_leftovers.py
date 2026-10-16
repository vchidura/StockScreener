#!/usr/bin/env python3
"""Report (and with --drop remove) disposable V2 test databases and roles left behind by a killed test run.

Only names that disposable V2 tests create are considered: databases `test_realtime_v2_*` and roles `test_rt_*`.
It never touches stocks_realtime_v1 or stocks_realtime_v2 and makes no provider request. Exit status 1 when
anything is left (after --drop: when anything could not be removed).
"""
from __future__ import annotations

import argparse
import sys
from contextlib import closing
from pathlib import Path

from psycopg2 import sql

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts.validate_realtime_v2_schema import _connect, _settings  # noqa: E402

DATABASE_PATTERN = r"test\_realtime\_v2\_%"
ROLE_PATTERN = r"test\_rt\_%"


def find_leftovers(cursor) -> tuple[list[str], list[str]]:
    cursor.execute("SELECT datname FROM pg_database WHERE datname LIKE %s ORDER BY 1", (DATABASE_PATTERN,))
    databases = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT rolname FROM pg_roles WHERE rolname LIKE %s ORDER BY 1", (ROLE_PATTERN,))
    return databases, [row[0] for row in cursor.fetchall()]


def drop_leftovers(cursor, databases: list[str], roles: list[str]) -> None:
    for name in databases:
        cursor.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                       "WHERE datname = %s AND pid <> pg_backend_pid()", (name,))
        cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(name)))
    for name in roles:
        cursor.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(name)))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--drop", action="store_true", help="drop what is found (databases first, then roles)")
    args = parser.parse_args()
    with closing(_connect(_settings(), "postgres")) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            databases, roles = find_leftovers(cursor)
            print(f"databases: {databases or 'none'}")
            print(f"roles: {roles or 'none'}")
            if args.drop and (databases or roles):
                drop_leftovers(cursor, databases, roles)
                databases, roles = find_leftovers(cursor)
                print(f"after drop: databases {databases or 'none'}, roles {roles or 'none'}")
    return 1 if databases or roles else 0


if __name__ == "__main__":
    raise SystemExit(main())
