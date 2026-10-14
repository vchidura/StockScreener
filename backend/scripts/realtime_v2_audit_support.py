"""Read-only plumbing shared by the V2 realtime audit scripts.

An audit opens one REPEATABLE READ read-only transaction with a statement timeout, verifies the
V2 database identity, and always rolls back. It never writes, calls a provider, or prints settings.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

EXPECTED_LINEAGE = "advanced_realtime_v2"
STATEMENT_TIMEOUT = "30s"
ADMIN_SETTING_NAMES = ("DB_HOST", "DB_PORT", "POSTGRES_ADMIN_USER", "POSTGRES_ADMIN_PASSWORD")


def admin_settings(env_file: Path) -> dict[str, str]:
    if not env_file.is_file():
        raise FileNotFoundError("realtime audit environment file is missing")
    load_dotenv(env_file, override=True)
    settings = {name: os.getenv(name, "").strip() for name in ADMIN_SETTING_NAMES}
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError("realtime audit requires: " + ", ".join(missing))
    return settings


def connect(database: str, settings: dict[str, str], *, application_name: str):
    return psycopg2.connect(
        dbname=database,
        user=settings["POSTGRES_ADMIN_USER"],
        password=settings["POSTGRES_ADMIN_PASSWORD"],
        host=settings["DB_HOST"],
        port=settings["DB_PORT"],
        application_name=application_name,
        options="-c timezone=UTC",
        connect_timeout=5,
    )


@contextmanager
def read_only_transaction(connection, database: str, *, statement_timeout: str = STATEMENT_TIMEOUT):
    """Yield (cursor, control) inside a read-only snapshot of the named V2 database; roll back on exit."""
    connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT set_config('statement_timeout', %s, true)", (statement_timeout,))
            cursor.execute(
                "SELECT current_database()::text AS database, current_setting('transaction_read_only') AS read_only,"
                "current_setting('transaction_isolation') AS isolation, clock_timestamp() AS checked_at"
            )
            control = dict(cursor.fetchone())
            cursor.execute("SELECT database_name::text AS database, lineage_name FROM rt.database_identity")
            identity = cursor.fetchone()
            if control["database"] != database or control["read_only"] != "on":
                raise RuntimeError("audit target/read-only identity mismatch")
            if (identity["database"], identity["lineage_name"]) != (database, EXPECTED_LINEAGE):
                raise RuntimeError("audit database lineage mismatch")
            control["lineage"] = identity["lineage_name"]
            yield cursor, control
    finally:
        connection.rollback()


def require_new_workspace_report(path: Path, workspace_root: Path) -> Path:
    resolved = path.resolve()
    if resolved.exists() or not resolved.is_relative_to(workspace_root.resolve()):
        raise ValueError("audit requires a new workspace-local report path")
    return resolved


def query_rows(cursor, statement: str, parameters=()) -> tuple[dict, ...]:
    cursor.execute(statement, parameters)
    return tuple(dict(row) for row in cursor.fetchall())
