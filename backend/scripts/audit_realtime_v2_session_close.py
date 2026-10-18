#!/usr/bin/env python3
"""Read-only provider-free audit of exact V2 five-minute session-close coverage."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID

from dotenv import load_dotenv
from psycopg2.pool import SimpleConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
WORKSPACE_DIR = BACKEND_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.database_access_v2 import (  # noqa: E402
    API_ROLE,
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    SECURITY_MODE_HARDENED,
    validate_target_database_v2,
)
from realtime.session_close_coverage import (  # noqa: E402
    DEFAULT_FINALIZATION_GRACE_SECONDS,
    DEFAULT_PROVIDER_DELAY_SECONDS,
    SessionCloseEligibilityPolicy,
    audit_session_close_cursor,
    persist_session_close_coverage_cursor,
)
from scripts.realtime_script_support import write_report  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--provider-tier",
        choices=("MASSIVE_DEVELOPER", "MASSIVE_ADVANCED"),
        default="MASSIVE_DEVELOPER",
    )
    parser.add_argument("--known-by", type=datetime.fromisoformat)
    parser.add_argument(
        "--provider-delay-seconds",
        type=int,
        default=DEFAULT_PROVIDER_DELAY_SECONDS,
    )
    parser.add_argument(
        "--finalization-grace-seconds",
        type=int,
        default=DEFAULT_FINALIZATION_GRACE_SECONDS,
    )
    parser.add_argument("--member-limit", type=int, default=50_000)
    parser.add_argument("--missing-range-limit", type=int, default=2_000)
    parser.add_argument(
        "--allow-late-identity-continuity",
        action="store_true",
    )
    parser.add_argument("--report-path", type=Path)
    parser.add_argument("--persist", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode=SECURITY_MODE_HARDENED,
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("session-close audit targets only stocks_realtime_v2")
    if args.known_by is None:
        args.known_by = datetime.now(timezone.utc)
    if args.known_by.tzinfo is None or args.known_by.utcoffset() is None:
        parser.error("--known-by must be timezone-aware")
    args.known_by = args.known_by.astimezone(timezone.utc)
    if (
        args.provider_delay_seconds != DEFAULT_PROVIDER_DELAY_SECONDS
        or args.finalization_grace_seconds != DEFAULT_FINALIZATION_GRACE_SECONDS
    ):
        parser.error("session-close eligibility policy is pinned to 900 seconds plus 30 seconds")
    if args.execute != args.persist:
        parser.error("durable close evidence requires --persist and --execute together")
    if args.report_path is not None:
        args.report_path = args.report_path.resolve()
        if args.report_path.exists():
            parser.error("--report-path must be new")
        if not args.report_path.is_relative_to(WORKSPACE_DIR):
            parser.error("--report-path must be workspace-local")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    eligibility_policy = SessionCloseEligibilityPolicy(
        args.provider_delay_seconds,
        args.finalization_grace_seconds,
    )
    load_dotenv(BACKEND_DIR / ".env", override=True)
    role = APP_ROLE if args.persist else API_ROLE
    dsn_name = "REALTIME_V2_APP_DSN" if args.persist else "REALTIME_V2_API_DSN"
    dsn = os.getenv(dsn_name, "").strip()
    if not dsn:
        raise RuntimeError(f"session-close audit requires {dsn_name}")
    pool = SimpleConnectionPool(1, 1, dsn=dsn)
    try:
        provider = V2ConnectionProvider(
            {role: pool},
            V2AccessConfig(
                security_mode=SECURITY_MODE_HARDENED,
                role_prefix="stocks_realtime_v2",
                statement_timeout_ms=5_000,
            ),
        )
        database = provider.verify()
        if args.persist:
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                unit.cursor.execute(
                    "SELECT EXISTS ("
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema='rt' "
                    "AND table_name='session_close_audits' "
                    "AND column_name='input_sha256') "
                    "AND EXISTS (SELECT 1 FROM pg_catalog.pg_trigger "
                    "WHERE tgrelid=to_regclass('rt.session_close_current') "
                    "AND tgname='trg_rt_session_close_current_order' "
                    "AND tgenabled IN ('O','A') AND NOT tgisinternal)"
                )
                if not unit.cursor.fetchone()[0]:
                    raise RuntimeError(
                        "session-close persistence requires migration "
                        "013_realtime_session_close_pointer_order"
                    )
        with provider.transaction(
            role,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            coverage = audit_session_close_cursor(
                unit.cursor,
                cohort_id=args.cohort_id,
                provider_tier=args.provider_tier,
                session_date=args.session_date,
                known_by=args.known_by,
                eligibility_policy=eligibility_policy,
                member_limit=args.member_limit,
                missing_range_limit=args.missing_range_limit,
                allow_late_identity_continuity=(
                    args.allow_late_identity_continuity
                ),
            )
        persistence = None
        if args.persist:
            with provider.transaction(APP_ROLE) as unit:
                persistence = persist_session_close_coverage_cursor(
                    unit.cursor, coverage
                )
        report = {
            "version": "v2-session-close-audit-v1",
            "database": database,
            **coverage.as_dict(),
            "persisted": args.persist,
            "audit_id": (
                str(persistence.audit_id) if persistence is not None else None
            ),
            "current_audit_id": (
                str(persistence.current_audit_id)
                if persistence is not None else None
            ),
            "current_status": (
                persistence.current_status if persistence is not None else None
            ),
            "pointer_advanced": (
                persistence.pointer_advanced if persistence is not None else False
            ),
            "database_writes": 1 if args.persist else 0,
        }
        if args.report_path is not None:
            write_report(args.report_path, report)
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if coverage.status == "COMPLETE" else 2
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
