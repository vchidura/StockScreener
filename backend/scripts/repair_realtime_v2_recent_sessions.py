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
from realtime.contracts import EQUITY_VALIDATION_UNIVERSE  # noqa: E402
from realtime.database_access_v2 import API_ROLE, APP_ROLE, V2AccessConfig, V2ConnectionProvider  # noqa: E402
from realtime.equity_validation_source import EquityValidationIdentity  # noqa: E402
from realtime.equity_recent_repair import (  # noqa: E402
    NativeRepairPlan, execute_recent_native_repair, recent_completed_sessions,
    validate_native_repair_budget, AuthorizedNativeRepairTransport,
)
from realtime.migrations import EXPECTED_V2_DATABASE_NAME, validate_target_database_v2  # noqa: E402
from realtime.reference_continuity import (  # noqa: E402
    classify_reference_identity_continuity,
)
from realtime.ticker_projection import _security  # noqa: E402
from scripts.realtime_script_support import PacedPolygonTransport, write_report  # noqa: E402
from scripts.run_realtime_v2_developer_window import _load_scope  # noqa: E402


def _repair_identity_cutoff(session: date) -> datetime:
    calendar = exchange_calendars.get_calendar("XNYS")
    return calendar.session_close(session.isoformat()).to_pydatetime()


def _load_scope_with_identity_continuity(
    provider,
    cohort_id,
    cutoff,
    *,
    known_by,
):
    with provider.transaction(APP_ROLE, read_only=True, dict_cursor=True) as unit:
        cursor = unit.cursor
        cursor.execute(
            "SELECT instrument.instrument_id,instrument.security_id "
            "FROM rt.cohorts AS cohort "
            "JOIN rt.subscription_members AS member USING(manifest_id) "
            "JOIN rt.instruments AS instrument USING(instrument_id) "
            "WHERE cohort.cohort_id=%s AND member.channel='AM' "
            "AND instrument.asset_class='EQUITY' "
            "ORDER BY instrument.instrument_id",
            (str(cohort_id),),
        )
        members = tuple(cursor.fetchall())
        identities = []
        evidence = []
        for member in members:
            cursor.execute(
                "SELECT revision.*,revision.revision_id "
                "AS security_revision_id "
                "FROM ref.security_revisions AS revision "
                "WHERE revision.security_id=%s "
                "AND revision.observed_at<=%s "
                "ORDER BY revision.observed_at DESC,"
                "revision.revision_id DESC LIMIT 1",
                (str(member["security_id"]), cutoff),
            )
            prior = cursor.fetchone()
            cursor.execute(
                "SELECT revision.*,revision.revision_id "
                "AS security_revision_id "
                "FROM ref.security_revisions AS revision "
                "WHERE revision.security_id=%s "
                "AND revision.observed_at<=%s "
                "ORDER BY revision.observed_at DESC,"
                "revision.revision_id DESC LIMIT 1",
                (str(member["security_id"]), known_by),
            )
            latest = cursor.fetchone()
            if prior is None or latest is None:
                raise ValueError(
                    "late identity continuity requires pre/post revisions"
                )
            mode = classify_reference_identity_continuity(
                prior,
                latest,
                cutoff,
            )
            cursor.execute(
                "SELECT count(*) FROM ref.security_revisions "
                "WHERE security_id=%s AND observed_at>%s "
                "AND observed_at<%s AND ticker<>%s",
                (
                    str(member["security_id"]),
                    prior["observed_at"],
                    latest["observed_at"],
                    latest["ticker"],
                ),
            )
            if int(cursor.fetchone()["count"]) != 0:
                raise ValueError(
                    "late identity continuity contains conflicting ticker evidence"
                )
            selected = prior if mode == "DIRECT_AT_CUTOFF" else latest
            identities.append(
                EquityValidationIdentity(
                    selected["ticker"],
                    _security(selected),
                    int(member["instrument_id"]),
                )
            )
            evidence.append((
                int(member["instrument_id"]),
                selected["ticker"],
                mode,
                str(prior["revision_id"]),
                prior["payload_sha256"],
                prior["observed_at"],
                prior["effective_from"],
                prior["effective_to"],
                str(latest["revision_id"]),
                latest["payload_sha256"],
                latest["observed_at"],
                latest["effective_from"],
                latest["effective_to"],
            ))
    by_ticker = {row.ticker: row for row in identities}
    if set(by_ticker) != set(EQUITY_VALIDATION_UNIVERSE.equity_tickers):
        raise ValueError("late identity continuity is not the exact nine-name universe")
    evidence_by_instrument = {row[0]: row for row in evidence}
    ordered = tuple(
        by_ticker[ticker]
        for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers
    )
    return ordered, tuple(
        evidence_by_instrument[row.instrument_id]
        for row in ordered
    )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--through-session", type=date.fromisoformat, required=True)
    parser.add_argument("--repair-sessions", type=int, required=True)
    parser.add_argument(
        "--interval",
        action="append",
        choices=("1m", "5m", "15m", "30m"),
        dest="intervals",
    )
    parser.add_argument("--authorized-until", type=datetime.fromisoformat, required=True)
    parser.add_argument("--maximum-provider-requests", type=int, required=True)
    parser.add_argument("--maximum-response-bytes", type=int, required=True)
    parser.add_argument("--response-byte-limit", type=int, default=2 * 1024 * 1024)
    parser.add_argument("--minimum-spacing-seconds", type=float, default=0.5)
    parser.add_argument("--timeout-seconds", type=float, default=30)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--provider-authorization-reference", required=True)
    parser.add_argument("--expected-cost", required=True)
    parser.add_argument(
        "--allow-late-identity-continuity",
        action="store_true",
    )
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
    args.intervals = tuple(
        dict.fromkeys(args.intervals or ("5m", "15m", "30m"))
    )
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
            cutoff = _repair_identity_cutoff(sessions[-1])
            try:
                identities, _ = _load_scope(
                    provider,
                    args.cohort_id,
                    cutoff,
                )
                identity_continuity = ()
            except ValueError:
                if not args.allow_late_identity_continuity:
                    raise
                identities, identity_continuity = (
                    _load_scope_with_identity_continuity(
                        provider,
                        args.cohort_id,
                        cutoff,
                        known_by=now,
                    )
                )
        else:
            identity_continuity = ()
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
                native_intervals=args.intervals,
            )
            report = {
                "mode": "PLAN", "sessions": [str(value) for value in sessions],
                "native_intervals": list(args.intervals),
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
            args.intervals,
            identity_continuity,
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
