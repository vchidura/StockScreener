#!/usr/bin/env python3
"""Provision a V2 realtime database from backend/realtime/sql_v2 (security deferred by default).

PLAN (default) reads only: it reports the migrations and whether the target exists. --execute
creates the database if absent (an existing one needs --resume), applies pending migrations with
checksum verification, then runs the structural catalog checks. Security is a separate, final
phase: the default deferred mode creates no role or grant and leaves the runtime on the owner.

Targets: test_realtime_v2_* with --allow-disposable, or stocks_realtime_v2 which additionally
needs --authorization-reference naming the explicit approval. stocks_realtime_v1 is never accepted.
This script makes no provider request.
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
    EXPECTED_V2_DATABASE_NAME,
    PHASE_SECURITY,
    SECURITY_MODE_DEFERRED,
    SECURITY_MODES_V2,
    V2_MIGRATIONS_DIR,
    load_realtime_migrations,
    plan_realtime_migrations,
    v2_provisioning_phases,
    validate_target_database_v2,
)
from realtime.security_v2 import (  # noqa: E402
    build_role_names_v2,
    current_security_phase_v2,
    harden_database_v2,
)
from scripts.validate_realtime_v2_schema import catalog_checks  # noqa: E402


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--allow-disposable", action="store_true")
    parser.add_argument("--security-mode", choices=SECURITY_MODES_V2, default=SECURITY_MODE_DEFERRED)
    parser.add_argument("--resume", action="store_true", help="apply pending migrations to an existing database")
    parser.add_argument("--authorization-reference", default="")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(args.database, args.confirm_database_name, allow_disposable=args.allow_disposable,
                                security_mode=args.security_mode)
    if args.database == EXPECTED_V2_DATABASE_NAME and args.execute and not args.authorization_reference.strip():
        parser.error(f"provisioning {EXPECTED_V2_DATABASE_NAME} requires --authorization-reference")
    if args.security_mode != SECURITY_MODE_DEFERRED and args.database == EXPECTED_V2_DATABASE_NAME:
        parser.error("hardening stocks_realtime_v2 is a separate step: harden_realtime_v2_database.py")
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
        host=settings["DB_HOST"], port=settings["DB_PORT"], application_name="provision_realtime_v2",
        options="-c timezone=UTC")


def _database_exists(settings, database: str) -> bool:
    with closing(_connect(settings, "postgres")) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = %s)", (database,))
            return bool(cursor.fetchone()[0])


def _applied(connection) -> dict[str, str]:
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass('rt.schema_migrations')")
        if cursor.fetchone()[0] is None:
            return {}
        cursor.execute("SELECT version, checksum_sha256 FROM rt.schema_migrations")
        return {version: checksum for version, checksum in cursor.fetchall()}


def main(argv=None) -> int:
    args = parse_args(argv)
    migrations = load_realtime_migrations(V2_MIGRATIONS_DIR)
    phases = v2_provisioning_phases(args.security_mode)
    settings = _settings()
    exists = _database_exists(settings, args.database)
    report = {
        "database": args.database, "mode": "EXECUTE" if args.execute else "PLAN",
        "security_mode": args.security_mode, "phases": list(phases), "database_exists": exists,
        "migrations": [migration.version for migration in migrations],
    }
    if not args.execute:
        print(json.dumps(report, indent=2))
        print("No database changes made. Add --execute.")
        return 0
    if exists and not args.resume:
        raise RuntimeError("database already exists; pass --resume to apply pending migrations")
    if not exists:
        with closing(_connect(settings, "postgres")) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0 ENCODING 'UTF8'")
                               .format(sql.Identifier(args.database)))
    with closing(_connect(settings, args.database)) as connection:
        plan = plan_realtime_migrations(args.database, _applied(connection), migrations)
        for migration in plan.pending:
            with connection.cursor() as cursor:
                cursor.execute(migration.sql)
                cursor.execute("INSERT INTO rt.schema_migrations (version, checksum_sha256) VALUES (%s, %s)",
                               (migration.version, migration.checksum_sha256))
            connection.commit()
        with connection.cursor() as cursor:
            checks = catalog_checks(cursor)
        connection.rollback()
        failures = [check for check in checks if not check[1].startswith("PASS")]
        report.update(applied_now=[migration.version for migration in plan.pending],
                      schema_sha256=plan.schema_sha256, structure={"checks": len(checks), "failures": failures})
        if PHASE_SECURITY in phases and not failures:
            if not args.database.startswith("test_realtime_v2_"):
                raise RuntimeError("use harden_realtime_v2_database.py for stocks_realtime_v2")
            roles = build_role_names_v2("test_rt_" + args.database[len("test_realtime_v2_"):])
            _created, audit = harden_database_v2(connection, roles)
            security_failures = [item for item in audit if not item[1].startswith("PASS")]
            report["security"] = {"checks": len(audit), "failures": security_failures}
        report["security_phase"] = current_security_phase_v2(connection)
        connection.rollback()
    ok = not failures and not report.get("security", {}).get("failures")
    report["status"] = "PROVISIONED" if ok else "FAILED"
    print(json.dumps(report, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
