#!/usr/bin/env python3
"""Create or recreate a PostgreSQL database and install the canonical baseline."""
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

from stock_screener.schema import install_or_adopt_schema  # noqa: E402


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--recreate",
        action="store_true",
        help="Drop and recreate DB_NAME before installing the baseline.",
    )
    result.add_argument(
        "--confirm-database-name",
        help="Required with --recreate and must exactly equal DB_NAME.",
    )
    return result


def _required_environment() -> dict[str, str]:
    names = (
        "DB_NAME",
        "DB_USER",
        "DB_PASSWORD",
        "DB_HOST",
        "DB_PORT",
        "POSTGRES_ADMIN_USER",
        "POSTGRES_ADMIN_PASSWORD",
    )
    settings = {name: os.getenv(name, "").strip() for name in names}
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError(
            "database initialization requires: " + ", ".join(missing)
        )
    if settings["DB_USER"] == settings["POSTGRES_ADMIN_USER"]:
        raise RuntimeError("DB_USER must differ from POSTGRES_ADMIN_USER")
    return settings


def _admin_connection(settings: dict[str, str], database: str):
    return psycopg2.connect(
        dbname=database,
        user=settings["POSTGRES_ADMIN_USER"],
        password=settings["POSTGRES_ADMIN_PASSWORD"],
        host=settings["DB_HOST"],
        port=settings["DB_PORT"],
    )


def ensure_database(settings: dict[str, str], *, recreate: bool) -> str:
    database_name = settings["DB_NAME"]
    with closing(_admin_connection(settings, "postgres")) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = %s)",
                (database_name,),
            )
            exists = bool(cursor.fetchone()[0])
            if recreate and exists:
                cursor.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = %s AND pid <> pg_backend_pid()",
                    (database_name,),
                )
                cursor.execute(
                    sql.SQL("DROP DATABASE {}").format(sql.Identifier(database_name))
                )
                exists = False
            if not exists:
                cursor.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name))
                )
                return "CREATED"
    return "EXISTING"


def configure_runtime_role(settings: dict[str, str]) -> None:
    runtime_user = settings["DB_USER"]
    runtime_password = settings["DB_PASSWORD"]
    database_name = settings["DB_NAME"]

    with closing(_admin_connection(settings, "postgres")) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = %s)",
                (runtime_user,),
            )
            if not cursor.fetchone()[0]:
                cursor.execute(
                    sql.SQL(
                        "CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB "
                        "NOCREATEROLE NOREPLICATION PASSWORD %s"
                    ).format(sql.Identifier(runtime_user)),
                    (runtime_password,),
                )
            cursor.execute(
                sql.SQL(
                    "ALTER ROLE {} WITH LOGIN NOSUPERUSER NOCREATEDB "
                    "NOCREATEROLE NOREPLICATION PASSWORD %s"
                ).format(sql.Identifier(runtime_user)),
                (runtime_password,),
            )
            cursor.execute(
                sql.SQL("ALTER ROLE {} SET timezone TO 'UTC'").format(
                    sql.Identifier(runtime_user)
                )
            )
            cursor.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    sql.Identifier(database_name), sql.Identifier(runtime_user)
                )
            )

    with closing(_admin_connection(settings, database_name)) as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            for statement in (
                sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(
                    sql.Identifier(runtime_user)
                ),
                sql.SQL(
                    "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES "
                    "IN SCHEMA public TO {}"
                ).format(sql.Identifier(runtime_user)),
                sql.SQL(
                    "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES "
                    "IN SCHEMA public TO {}"
                ).format(sql.Identifier(runtime_user)),
                sql.SQL("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO {}").format(
                    sql.Identifier(runtime_user)
                ),
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
                    "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {}"
                ).format(sql.Identifier(runtime_user)),
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
                    "GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {}"
                ).format(sql.Identifier(runtime_user)),
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
                    "GRANT EXECUTE ON FUNCTIONS TO {}"
                ).format(sql.Identifier(runtime_user)),
                sql.SQL("REVOKE CREATE ON SCHEMA public FROM {}").format(
                    sql.Identifier(runtime_user)
                ),
            ):
                cursor.execute(statement)


def main() -> int:
    args = parser().parse_args()
    settings = _required_environment()
    if args.recreate and args.confirm_database_name != settings["DB_NAME"]:
        raise RuntimeError(
            "--recreate requires --confirm-database-name equal to DB_NAME"
        )
    if not args.recreate and args.confirm_database_name is not None:
        raise RuntimeError("--confirm-database-name is valid only with --recreate")

    database_state = ensure_database(settings, recreate=args.recreate)
    with closing(_admin_connection(settings, settings["DB_NAME"])) as connection:
        baseline_state = install_or_adopt_schema(connection)

    configure_runtime_role(settings)
    print(
        f"DATABASE_INITIALIZED database={settings['DB_NAME']} "
        f"database_state={database_state} baseline_state={baseline_state} "
        f"runtime_role={settings['DB_USER']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
