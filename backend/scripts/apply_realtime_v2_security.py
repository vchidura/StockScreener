#!/usr/bin/env python3
"""Audit or reapply the hardened stocks_realtime_v2 role policy."""
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

from realtime.migrations import (  # noqa: E402
    SECURITY_MODE_HARDENED,
    validate_target_database_v2,
)
from realtime.security_v2 import (  # noqa: E402
    audit_database_security_v2,
    build_role_names_v2,
    harden_database_v2,
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--role-prefix", default="stocks_realtime_v2")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode=SECURITY_MODE_HARDENED,
    )
    build_role_names_v2(args.role_prefix)
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
            "V2 security runner requires: " + ", ".join(missing)
        )
    return settings


def main(argv=None) -> int:
    args = parse_args(argv)
    settings = _settings()
    roles = build_role_names_v2(args.role_prefix)
    with closing(psycopg2.connect(
        dbname=args.database,
        user=settings["POSTGRES_ADMIN_USER"],
        password=settings["POSTGRES_ADMIN_PASSWORD"],
        host=settings["DB_HOST"],
        port=settings["DB_PORT"],
        application_name="apply_realtime_v2_security",
        options="-c timezone=UTC",
    )) as connection:
        if args.apply:
            created, audit = harden_database_v2(connection, roles)
        else:
            created = []
            audit = audit_database_security_v2(connection, roles)
            connection.rollback()
    passed = all(status.startswith("PASS") for _, status in audit)
    print(json.dumps({
        "database": args.database,
        "mode": "APPLY" if args.apply else "AUDIT",
        "created_roles": created,
        "passed": passed,
        "audit": audit,
    }, indent=2))
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
