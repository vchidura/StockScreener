#!/usr/bin/env python3
"""Plan or explicitly execute bounded native recent-session repair; never publish."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID

import exchange_calendars
from dotenv import load_dotenv
from psycopg2.pool import ThreadedConnectionPool

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.data.polygon_http import RequestsPolygonHttpTransport  # noqa: E402
from realtime.database_access_v2 import API_ROLE, APP_ROLE, V2AccessConfig, V2ConnectionProvider  # noqa: E402
from realtime.equity_recent_repair import (  # noqa: E402
    NativeRepairPlan, execute_recent_native_repair, recent_completed_sessions,
    validate_native_repair_budget, AuthorizedNativeRepairTransport,
)
from realtime.migrations import EXPECTED_V2_DATABASE_NAME, validate_target_database_v2  # noqa: E402
from scripts.realtime_script_support import PacedPolygonTransport, write_report  # noqa: E402
from scripts.run_realtime_v2_developer_window import _load_scope  # noqa: E402


def _repair_identity_cutoff(session: date) -> datetime:
    calendar = exchange_calendars.get_calendar("XNYS")
    return calendar.session_close(session.isoformat()).to_pydatetime()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--through-session", type=date.fromisoformat, required=True)
    parser.add_argument("--repair-sessions", type=int, required=True)
    parser.add_argument("--authorized-until", type=datetime.fromisoformat, required=True)
    parser.add_argument("--maximum-provider-requests", type=int, required=True)
    parser.add_argument("--maximum-response-bytes", type=int, required=True)
    parser.add_argument("--response-byte-limit", type=int, default=2 * 1024 * 1024)
    parser.add_argument("--minimum-spacing-seconds", type=float, default=0.5)
    parser.add_argument("--timeout-seconds", type=float, default=30)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--provider-authorization-reference", required=True)
    parser.add_argument("--expected-cost", required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(args.database, args.confirm_database_name)
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("native repair targets only stocks_realtime_v2")
    if (
        not 1 <= args.repair_sessions <= 10
        or args.authorized_until.tzinfo is None
        or args.authorized_until.utcoffset() is None
        or not 0.5 <= args.minimum_spacing_seconds <= 60
        or not 1 <= args.timeout_seconds <= 60
        or not all(value.strip() for value in (
            args.authorization_reference, args.provider_authorization_reference, args.expected_cost,
        ))
    ):
        parser.error("repair scope, aware deadline, pacing and authorization are required")
    args.report_path = args.report_path.resolve()
    if args.report_path.exists():
        parser.error("repair report must be new")
    return args


def main(argv=None):
    args = parse_args(argv)
    load_dotenv(BACKEND_DIR / ".env", override=True)
    role = APP_ROLE if args.execute else API_ROLE
    dsn = os.getenv("REALTIME_V2_APP_DSN" if args.execute else "REALTIME_V2_API_DSN", "").strip()
    if not dsn:
        raise RuntimeError(f"native repair requires the {role} DSN")
    pool = ThreadedConnectionPool(1, 2, dsn=dsn, options="-c timezone=UTC")
    try:
        provider = V2ConnectionProvider(
            {role: pool},
            V2AccessConfig(security_mode="hardened", role_prefix=EXPECTED_V2_DATABASE_NAME),
        )
        provider.verify()
        now = datetime.now(timezone.utc)
        sessions = recent_completed_sessions(
            args.through_session, args.repair_sessions, known_by=now
        )
        if args.execute:
            identities, _ = _load_scope(
                provider,
                args.cohort_id,
                _repair_identity_cutoff(sessions[-1]),
            )
        else:
            with provider.transaction(API_ROLE, read_only=True) as unit:
                unit.cursor.execute(
                    "SELECT manifest.stock_count FROM rt.cohorts cohort "
                    "JOIN rt.subscription_manifests manifest USING (manifest_id) "
                    "WHERE cohort.cohort_id=%s", (str(args.cohort_id),),
                )
                row = unit.cursor.fetchone()
            if row is None:
                raise ValueError("native repair cohort is not retained")
            requests_required = validate_native_repair_budget(
                int(row[0]), len(sessions), args.maximum_provider_requests,
                args.maximum_response_bytes, args.response_byte_limit,
            )
            report = {
                "mode": "PLAN", "sessions": [str(value) for value in sessions],
                "native_intervals": ["5m", "15m", "30m"],
                "requests_required": requests_required, "provider_requests": 0,
                "database_writes": 0, "adjusted_history": "RETAINED_ONLY",
                "release_advanced": False,
            }
            write_report(args.report_path, report)
            print(json.dumps(report, sort_keys=True))
            return 0
        plan = NativeRepairPlan(
            args.cohort_id, identities, sessions, args.authorized_until,
            args.maximum_provider_requests, args.maximum_response_bytes,
            args.response_byte_limit, args.authorization_reference,
            args.provider_authorization_reference, args.expected_cost,
        )
        api_key = os.getenv("POLYGON_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("native repair requires POLYGON_API_KEY")
        transport = PacedPolygonTransport(
            AuthorizedNativeRepairTransport(
                RequestsPolygonHttpTransport(
                    api_key, timeout_seconds=args.timeout_seconds,
                    maximum_response_bytes=args.response_byte_limit,
                ),
                plan.authorized_until, lambda: datetime.now(timezone.utc),
            ),
            minimum_interval_seconds=args.minimum_spacing_seconds,
        )
        report = execute_recent_native_repair(
            provider, transport, plan, clock=lambda: datetime.now(timezone.utc)
        )
        write_report(args.report_path, report)
        print(json.dumps(report, sort_keys=True))
        return 0 if report["status"] == "COMPLETE" else 2
    finally:
        pool.closeall()


if __name__ == "__main__":
    raise SystemExit(main())
