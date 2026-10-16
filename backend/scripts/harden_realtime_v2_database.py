#!/usr/bin/env python3
"""Final security phase for an existing V2 database: roles, grants, exact-privilege audit, ledger.

Run this only after the pipeline is validated with the runtime connected as the owner (security
deferred). PLAN (default) is read-only and reports the ledger phase and which roles exist.
--execute ensures the NOLOGIN admin, app and api roles (re-runnable; existing roles must
match the policy), applies the registry grants, audits exact privileges and records AUDITED only
on a clean audit. A failed audit leaves the ledger at APPLIED, which a hardened runtime refuses.

Targets: test_realtime_v2_* with --allow-disposable, or stocks_realtime_v2 with
--authorization-reference naming the explicit approval. stocks_realtime_v1 is never accepted.
LOGIN on the app and api roles and their credentials are NOT created here: that credential model is a
separate, explicit decision. This script makes no provider request and prints no credential.
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

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import EXPECTED_V2_DATABASE_NAME, SECURITY_MODE_HARDENED, validate_target_database_v2  # noqa: E402
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
    parser.add_argument("--role-prefix", help="default: stocks_realtime_v2, or test_rt_<suffix> for a disposable database")
    parser.add_argument("--authorization-reference", default="")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(args.database, args.confirm_database_name, allow_disposable=args.allow_disposable,
                                security_mode=SECURITY_MODE_HARDENED)
    if args.database == EXPECTED_V2_DATABASE_NAME:
        args.role_prefix = args.role_prefix or EXPECTED_V2_DATABASE_NAME
        if args.execute and not args.authorization_reference.strip():
            parser.error(f"hardening {EXPECTED_V2_DATABASE_NAME} requires --authorization-reference")
    else:
        args.role_prefix = args.role_prefix or "test_rt_" + args.database[len("test_realtime_v2_"):]
        if not args.role_prefix.startswith("test_rt_"):
            parser.error("a disposable database must use test_rt_ role names")
    build_role_names_v2(args.role_prefix)
    return args


def _connect(database: str):
    load_dotenv(BACKEND_DIR / ".env", override=True)
    names = ("DB_HOST", "DB_PORT", "POSTGRES_ADMIN_USER", "POSTGRES_ADMIN_PASSWORD")
    settings = {name: os.getenv(name, "").strip() for name in names}
    missing = [name for name, value in settings.items() if not value]
    if missing:
        raise RuntimeError("requires: " + ", ".join(missing))
    return psycopg2.connect(
        dbname=database, user=settings["POSTGRES_ADMIN_USER"], password=settings["POSTGRES_ADMIN_PASSWORD"],
        host=settings["DB_HOST"], port=settings["DB_PORT"], application_name="harden_realtime_v2",
        options="-c timezone=UTC")


def main(argv=None) -> int:
    args = parse_args(argv)
    roles = build_role_names_v2(args.role_prefix)
    with closing(_connect(args.database)) as connection:
        phase = current_security_phase_v2(connection)
        with connection.cursor() as cursor:
            cursor.execute("SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)", (list(roles.all_names),))
            present = sorted(row[0] for row in cursor.fetchall())
            checks = catalog_checks(cursor)
        connection.rollback()
        structure_failures = [item for item in checks if not item[1].startswith("PASS")]
        report = {"database": args.database, "mode": "EXECUTE" if args.execute else "PLAN",
                  "role_prefix": roles.prefix, "phase_before": phase, "roles_present": len(present),
                  "roles_expected": len(roles.all_names), "structure_failures": structure_failures}
        if not args.execute:
            print(json.dumps(report, indent=2))
            print("No database changes made. Add --execute.")
            return 0 if not structure_failures else 1
        if structure_failures:
            report["status"] = "REFUSED_STRUCTURE_FAILED"
            print(json.dumps(report, indent=2))
            return 1
        created, audit = harden_database_v2(connection, roles)
        failures = [item for item in audit if not item[1].startswith("PASS")]
        report.update(roles_created=len(created), audit={"checks": len(audit), "failures": failures},
                      phase_after=current_security_phase_v2(connection),
                      status="HARDENED" if not failures else "FAILED_AUDIT_LEFT_APPLIED")
        connection.rollback()
    print(json.dumps(report, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
