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

from realtime.migrations import validate_target_database
from realtime.security import (
    apply_database_security,
    audit_database_security,
    build_role_names,
    ensure_capability_roles,
    grant_principal_memberships,
    security_policy_sha256,
    validate_production_roles,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan or apply the realtime database security policy."
    )
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--role-prefix", default="stocks_realtime")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--allow-locked-principals", action="store_true")
    args = parser.parse_args()
    validate_target_database(args.database, args.confirm_database_name)
    build_role_names(args.role_prefix)
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
            "realtime security runner requires: " + ", ".join(missing)
        )
    return settings


def main() -> int:
    args = parse_args()
    roles = build_role_names(args.role_prefix)
    print(
        json.dumps(
            {
                "database": args.database,
                "mode": "APPLY" if args.apply else "PLAN",
                "policy_sha256": security_policy_sha256(),
                "capability_roles": roles.capability_names,
                "required_login_roles": roles.principal_names,
                "creates_login_credentials": False,
                "allow_locked_principals": args.allow_locked_principals,
            },
            indent=2,
        )
    )
    if not args.apply:
        print(
            "No roles or grants changed. Create the three LOGIN principals through "
            "the approved secret-management path, then add --apply."
        )
        return 0
    settings = _settings()
    with closing(
        psycopg2.connect(
            dbname=args.database,
            user=settings["POSTGRES_ADMIN_USER"],
            password=settings["POSTGRES_ADMIN_PASSWORD"],
            host=settings["DB_HOST"],
            port=settings["DB_PORT"],
            application_name="apply_realtime_security",
            options="-c timezone=UTC",
        )
    ) as connection:
        try:
            ensure_capability_roles(connection, roles)
            validate_production_roles(
                connection,
                roles,
                allow_locked_principals=args.allow_locked_principals,
            )
            grant_principal_memberships(connection, roles)
            apply_database_security(connection, roles)
            report = audit_database_security(connection, roles)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
    print(json.dumps({"status": "SECURITY_APPLIED", **report}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())