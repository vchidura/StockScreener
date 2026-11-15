#!/usr/bin/env python3
"""Check (and if needed register) the size-independent Advanced runtime profile, as the admin role.

G102 R62 (Nov 15 2026): migration 053 registers the size-independent STOCKS AM runtime profile ACTIVE, so a database
with 053 applied needs nothing from this tool. It is the deploy check: it reports every STOCKS runtime profile row,
verifies the size-independent one against the source catalog, and re-registers plus activates it with exactly the
evidence migration 053 uses if it is missing. It refuses a database without 053: before 053 the schema bounds profile
sizes at 400, below the profile's coverage (the universe-limits ceiling).

DRY RUN BY DEFAULT: the transaction is rolled back unless --commit is given. Credentials come from --env-file and are
never printed.

    python -B scripts/register_realtime_v2_advanced_runtime_profile.py \
        --database stocks_realtime_v2 --confirm-database-name stocks_realtime_v2 [--commit]
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from dotenv import dotenv_values
from psycopg2 import sql

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.advanced_ingress_repository import (  # noqa: E402
    activate_advanced_runtime_profile_cursor,
    register_advanced_runtime_profile_cursor,
)
from realtime.advanced_ingress_runtime_profile import (  # noqa: E402
    load_advanced_ingress_runtime_catalog,
    size_independent_profile_registration,
)
from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.relation_registry import ADMIN_ROLE  # noqa: E402
from realtime.security_v2 import build_role_names_v2  # noqa: E402

MIGRATION_VERSION = "053_g102_size_independent_runtime_profile"
DEFAULT_POLICY = BACKEND_DIR / "realtime" / "policies" / "advanced_equity_ingress_v1.json"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--env-file", type=Path, default=BACKEND_DIR / ".env")
    parser.add_argument("--ingress-policy-path", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--commit", action="store_true", help="commit the registration (default: roll back)")
    args = parser.parse_args(argv)
    validate_target_database_v2(args.database, args.confirm_database_name, security_mode="hardened")
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("the runtime profile belongs to stocks_realtime_v2")
    if not args.env_file.resolve().is_file():
        parser.error("--env-file must exist")
    return args


def _required(settings, name):
    value = str(settings.get(name) or "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _profile_rows(cursor):
    cursor.execute(
        "SELECT runtime_profile_sha256,runtime_profile_version,universe_target,ladder_position,status,"
        "configuration_sha256,runtime_policy_sha256,activation_approval_reference "
        "FROM rt.advanced_ingress_runtime_profiles WHERE feed='STOCKS' ORDER BY universe_target,created_at"
    )
    return [
        {
            "runtime_profile_sha256": str(row[0]),
            "runtime_profile_version": str(row[1]),
            "universe_target": int(row[2]),
            "ladder_position": int(row[3]),
            "status": str(row[4]),
            "configuration_sha256": str(row[5]),
            "runtime_policy_sha256": str(row[6]),
            "activation_approval_reference": row[7],
        }
        for row in cursor.fetchall()
    ]


def run(args) -> dict:
    catalog = load_advanced_ingress_runtime_catalog(args.ingress_policy_path)
    expected = size_independent_profile_registration(catalog)
    settings = dotenv_values(args.env_file.resolve())
    connection = psycopg2.connect(
        dbname=args.database,
        user=_required(settings, "POSTGRES_ADMIN_USER"),
        password=_required(settings, "POSTGRES_ADMIN_PASSWORD"),
        host=_required(settings, "DB_HOST"),
        port=_required(settings, "DB_PORT"),
        application_name="register_realtime_v2_advanced_runtime_profile",
        options="-c timezone=UTC",
    )
    report = {
        "mode": "COMMIT" if args.commit else "DRY_RUN",
        "expected": {key: value for key, value in expected.items() if key != "profile_document"},
        "credentials_exposed": False,
    }
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(build_role_names_v2(args.database).name(ADMIN_ROLE)))
            )
            cursor.execute("SELECT EXISTS (SELECT 1 FROM rt.schema_migrations WHERE version=%s)", (MIGRATION_VERSION,))
            report["migration_053_applied"] = bool(cursor.fetchone()[0])
            if not report["migration_053_applied"]:
                raise RuntimeError(
                    "migration 053 is not applied: apply it (it registers the profile) instead of this tool"
                )
            report["before"] = _profile_rows(cursor)
            cursor.execute(
                "SELECT status,profile_document FROM rt.advanced_ingress_runtime_profiles "
                "WHERE runtime_profile_sha256=%s",
                (expected["runtime_profile_sha256"],),
            )
            stored = cursor.fetchone()
            actions = []
            if stored is None or stored[0] == "CANDIDATE":
                register_advanced_runtime_profile_cursor(
                    cursor,
                    catalog.profile,
                    runtime_policy_sha256=expected["runtime_policy_sha256"],
                    ladder_position=0,
                    previous_universe_target=None,
                )
                actions.append("REGISTERED" if stored is None else "ALREADY_CANDIDATE")
                activate_advanced_runtime_profile_cursor(
                    cursor,
                    expected["runtime_profile_sha256"],
                    stability_evidence_sha256=expected["stability_evidence_sha256"],
                    capacity_evidence_sha256=expected["capacity_evidence_sha256"],
                    approval_reference=expected["activation_approval_reference"],
                    activated_at=datetime.now(timezone.utc),
                )
                actions.append("ACTIVATED")
            elif stored[0] != "ACTIVE":
                raise RuntimeError(f"the size-independent runtime profile is {stored[0]}, not ACTIVE")
            else:
                actions.append("ALREADY_ACTIVE")
            cursor.execute(
                "SELECT runtime_profile_version,universe_target,configuration_sha256,runtime_policy_sha256,status,"
                "profile_document FROM rt.advanced_ingress_runtime_profiles WHERE runtime_profile_sha256=%s",
                (expected["runtime_profile_sha256"],),
            )
            row = cursor.fetchone()
            report["verified"] = bool(
                row is not None
                and str(row[0]) == expected["runtime_profile_version"]
                and int(row[1]) == expected["universe_target"]
                and str(row[2]) == expected["configuration_sha256"]
                and str(row[3]) == expected["runtime_policy_sha256"]
                and str(row[4]) == "ACTIVE"
                and canonical_sha256(row[5]) == expected["runtime_profile_sha256"]
            )
            report["actions"] = actions
            report["after"] = _profile_rows(cursor)
        if args.commit and report["verified"]:
            connection.commit()
            report["committed"] = True
        else:
            connection.rollback()
            report["committed"] = False
    finally:
        connection.close()
    return report


def main(argv=None) -> int:
    report = run(parse_args(argv))
    print(json.dumps(report, indent=2, default=str))
    return 0 if report.get("verified") else 1


if __name__ == "__main__":
    raise SystemExit(main())
