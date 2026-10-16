#!/usr/bin/env python3
"""Copy the V2-10 reference scope from stocks_realtime_v1 to stocks_realtime_v2.

PLAN is read-only. --execute requires the explicit authorization reference, keeps the V1
transaction read-only, requires every target relation to be empty, copies every catalog column
verbatim, advances copied sequences and commits only after count/checksum parity succeeds.
"""
from __future__ import annotations

import argparse
import json
import sys
from contextlib import closing
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.migrations import EXPECTED_V2_DATABASE_NAME, validate_target_database_v2  # noqa: E402
from realtime.reference_copy import execute_reference_copy  # noqa: E402
from realtime.reference_copy_plan import SOURCE_DATABASE, build_reference_copy_plan  # noqa: E402
from scripts.realtime_v2_audit_support import admin_settings, connect  # noqa: E402


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--authorization-reference", default="")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(args.database, args.confirm_database_name)
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("the reference-copy executor targets only stocks_realtime_v2")
    if args.execute and not args.authorization_reference.strip():
        parser.error("copying production reference data requires --authorization-reference")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    settings = admin_settings(BACKEND_DIR / ".env")
    with closing(connect(args.database, settings, application_name="plan_v2_reference_copy")) as target:
        with target.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            plan = build_reference_copy_plan(cursor)
        target.rollback()
    if not args.execute:
        print(json.dumps({
            "mode": "PLAN",
            "source": SOURCE_DATABASE,
            "target": args.database,
            "relations": [row["relation"] for row in plan["relations"]],
            "relation_count": len(plan["relations"]),
            "self_referencing": [
                row["relation"] for row in plan["relations"] if row["self_referencing"]
            ],
            "provider_requests": 0,
            "database_writes": 0,
        }, sort_keys=True))
        return 0
    with (
        closing(connect(SOURCE_DATABASE, settings, application_name="copy_v2_reference_source")) as source,
        closing(connect(args.database, settings, application_name="copy_v2_reference_target")) as target,
    ):
        report = execute_reference_copy(source, target, plan)
    print(json.dumps({
        "mode": "EXECUTE",
        **report,
        "provider_requests": 0,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
