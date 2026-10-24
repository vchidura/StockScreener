#!/usr/bin/env python3
"""Plan or persist exact bounded V2 corporate-action coverage."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import UniverseManifest, canonical_sha256  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.reference_adapters import (  # noqa: E402
    _corporate_action_coverage_fact,
    _corporate_action_fact,
    _security_id,
)
from realtime.reference_repository import (  # noqa: E402
    persist_reference_facts_cursor,
)
from realtime.reference_service import ReferenceSeedRequest  # noqa: E402
from realtime.reference_transports import (  # noqa: E402
    MassiveReferenceTransport,
    ReferenceTransportLimits,
    RequestsReferenceHttpTransport,
)
from scripts.realtime_script_support import write_report  # noqa: E402
from scripts.refresh_realtime_v2_references import (  # noqa: E402
    PacedAccountingReferenceTransport,
)


UTC = timezone.utc


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--cohort-id", type=UUID, required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--window-start", type=date.fromisoformat, required=True)
    parser.add_argument("--window-end", type=date.fromisoformat, required=True)
    parser.add_argument("--ticker", action="append", required=True)
    parser.add_argument("--authorized-until", type=datetime.fromisoformat, required=True)
    parser.add_argument("--maximum-provider-requests", type=int, required=True)
    parser.add_argument("--response-byte-limit", type=int, required=True)
    parser.add_argument("--total-byte-ceiling", type=int, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("corporate-action repair targets stocks_realtime_v2")
    args.tickers = tuple(sorted({
        value.strip().upper()
        for value in args.ticker
        if value.strip()
    }))
    if args.tickers != ("QQQ", "SPY"):
        parser.error("V3 corporate-action repair requires exact QQQ/SPY")
    if (
        args.window_end < args.window_start
        or args.authorized_until.tzinfo is None
        or args.authorized_until.utcoffset() is None
        or args.maximum_provider_requests != 4
        or args.response_byte_limit != 5_000_000
        or args.total_byte_ceiling != 20_000_000
        or not args.authorization_reference.strip()
    ):
        parser.error("corporate-action repair scope or budget is invalid")
    args.authorized_until = args.authorized_until.astimezone(UTC)
    args.report_path = args.report_path.resolve()
    if args.report_path.exists():
        parser.error("--report-path must be new")
    return args


def _provider():
    values = dotenv_values(BACKEND_DIR / ".env")
    dsn = str(
        os.getenv("REALTIME_V2_APP_DSN")
        or values.get("REALTIME_V2_APP_DSN")
        or ""
    ).strip()
    if not dsn:
        raise RuntimeError("REALTIME_V2_APP_DSN is required")
    pool = ThreadedConnectionPool(1, 2, dsn=dsn, options="-c timezone=UTC")
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(
            security_mode="hardened",
            role_prefix=EXPECTED_V2_DATABASE_NAME,
        ),
    )
    provider.verify()
    return provider, pool


def _validate_security_scope(provider, cohort_id, tickers, known_by):
    with provider.transaction(APP_ROLE, read_only=True) as unit:
        unit.cursor.execute(
            "SELECT DISTINCT ON (revision.ticker) revision.ticker,"
            "instrument.security_id FROM rt.cohorts AS cohort "
            "JOIN rt.subscription_members AS member USING(manifest_id) "
            "JOIN rt.instruments AS instrument USING(instrument_id) "
            "JOIN ref.security_revisions AS revision "
            "ON revision.security_id=instrument.security_id "
            "WHERE cohort.cohort_id=%s AND member.channel='AM' "
            "AND revision.ticker=ANY(%s) "
            "AND revision.observed_at<=%s "
            "ORDER BY revision.ticker,revision.observed_at DESC,"
            "revision.revision_id DESC",
            (str(cohort_id), list(tickers), known_by),
        )
        stored = {
            str(ticker): str(security_id)
            for ticker, security_id in unit.cursor.fetchall()
        }
    expected = {
        ticker: str(_security_id(ticker))
        for ticker in tickers
    }
    if stored != expected:
        raise ValueError("corporate-action security identity changed")
    return expected


def main(argv=None) -> int:
    args = parse_args(argv)
    checked_at = datetime.now(UTC)
    if not checked_at <= args.authorized_until <= checked_at + timedelta(hours=1):
        raise ValueError("corporate-action authorization is outside its TTL")
    plan = {
        "contract": "realtime-v2-corporate-action-repair-v1",
        "mode": "EXECUTE" if args.execute else "PLAN",
        "database": args.database,
        "cohort_id": str(args.cohort_id),
        "session_date": args.session_date.isoformat(),
        "window": (
            args.window_start.isoformat(),
            args.window_end.isoformat(),
        ),
        "tickers": args.tickers,
        "endpoints": ("/stocks/v1/splits", "/stocks/v1/dividends"),
        "maximum_provider_requests": args.maximum_provider_requests,
        "response_byte_limit": args.response_byte_limit,
        "total_byte_ceiling": args.total_byte_ceiling,
        "authorization_reference": args.authorization_reference,
    }
    if not args.execute:
        report = {
            **plan,
            "status": "PLANNED",
            "provider_requests": 0,
            "database_writes": 0,
            "semantic_sha256": canonical_sha256(plan),
        }
        write_report(args.report_path, report)
        print(json.dumps(report, default=str, sort_keys=True))
        return 0
    values = dotenv_values(BACKEND_DIR / ".env")
    api_key = str(
        os.getenv("POLYGON_API_KEY")
        or values.get("POLYGON_API_KEY")
        or ""
    ).strip()
    if not api_key:
        raise RuntimeError("corporate-action repair requires POLYGON_API_KEY")
    provider, pool = _provider()
    try:
        security_ids = _validate_security_scope(
            provider,
            args.cohort_id,
            args.tickers,
            checked_at,
        )
        transport = PacedAccountingReferenceTransport(
            RequestsReferenceHttpTransport(timeout_seconds=30),
            maximum_requests=args.maximum_provider_requests,
            minimum_interval_seconds=0.5,
            authorized_until=args.authorized_until,
        )
        massive = MassiveReferenceTransport(
            api_key,
            transport,
            limits=ReferenceTransportLimits(
                maximum_requests=args.maximum_provider_requests,
                maximum_response_bytes=args.response_byte_limit,
                maximum_total_bytes=args.total_byte_ceiling,
                maximum_rows=10_000,
                maximum_pages=1,
            ),
        )
        rows = {}
        for ticker in args.tickers:
            rows[(ticker, "SPLIT")] = massive.fetch_splits(
                ticker,
                args.window_start,
                args.window_end,
            )
            rows[(ticker, "DIVIDEND")] = massive.fetch_dividends(
                ticker,
                args.window_start,
                args.window_end,
            )
        snapshot = massive.snapshot()
        if snapshot.provider_requests != args.maximum_provider_requests:
            raise RuntimeError("corporate-action request count changed")
        observed_at = datetime.now(UTC)
        request = ReferenceSeedRequest(
            UniverseManifest("advanced-options-v3-corporate-actions-v1", args.tickers, ()),
            args.session_date,
            observed_at,
            observed_at,
            args.authorized_until,
        )
        facts = []
        for ticker in args.tickers:
            for action_type in ("SPLIT", "DIVIDEND"):
                members = rows[(ticker, action_type)]
                if any(
                    str(row.get("ticker") or "").upper() != ticker
                    for row in members
                ):
                    raise ValueError(
                        "corporate-action response identity changed"
                    )
                facts.extend(
                    _corporate_action_fact(
                        ticker,
                        action_type,
                        row,
                        request,
                    )
                    for row in members
                )
                facts.append(_corporate_action_coverage_fact(
                    ticker,
                    action_type,
                    members,
                    args.window_start,
                    args.window_end,
                    request,
                ))
        with provider.transaction(APP_ROLE) as unit:
            persist_reference_facts_cursor(unit.cursor, tuple(facts))
    finally:
        pool.closeall()
    report = {
        **plan,
        "status": "COMPLETE",
        "security_ids": security_ids,
        "provider_requests": snapshot.provider_requests,
        "provider_response_bytes": snapshot.response_bytes,
        "provider_rows": snapshot.rows,
        "database_writes": len(facts),
        "fact_sha256": canonical_sha256(tuple(facts)),
        "credentials_exposed": False,
    }
    report["semantic_sha256"] = canonical_sha256(report)
    write_report(args.report_path, report)
    print(json.dumps(report, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
