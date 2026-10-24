#!/usr/bin/env python3
"""Plan or persist one bounded V2 Treasury curve refresh."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import dotenv_values
from psycopg2.pool import ThreadedConnectionPool


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import (  # noqa: E402
    EQUITY_VALIDATION_UNIVERSE,
    canonical_sha256,
)
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.reference_adapters import TreasuryReferenceAdapter  # noqa: E402
from realtime.reference_repository import (  # noqa: E402
    persist_reference_facts_cursor,
)
from realtime.reference_service import ReferenceSeedRequest  # noqa: E402
from realtime.reference_transports import (  # noqa: E402
    ReferenceTransportLimits,
    RequestsReferenceHttpTransport,
    TreasuryReferenceTransport,
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
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--authorized-until", type=datetime.fromisoformat, required=True)
    parser.add_argument("--maximum-provider-requests", type=int, required=True)
    parser.add_argument("--response-byte-limit", type=int, required=True)
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
        parser.error("Treasury repair targets stocks_realtime_v2")
    if (
        args.authorized_until.tzinfo is None
        or args.authorized_until.utcoffset() is None
        or args.maximum_provider_requests != 1
        or args.response_byte_limit != 2_000_000
        or not args.authorization_reference.strip()
    ):
        parser.error("Treasury repair scope or budget is invalid")
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


def main(argv=None) -> int:
    args = parse_args(argv)
    checked_at = datetime.now(UTC)
    if not checked_at <= args.authorized_until <= checked_at + timedelta(hours=1):
        raise ValueError("Treasury authorization is outside its TTL")
    plan = {
        "contract": "realtime-v2-treasury-curve-repair-v1",
        "mode": "EXECUTE" if args.execute else "PLAN",
        "database": args.database,
        "session_date": args.session_date.isoformat(),
        "source": "us_treasury_par_yield_v1",
        "maximum_provider_requests": args.maximum_provider_requests,
        "response_byte_limit": args.response_byte_limit,
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
        print(json.dumps(report, sort_keys=True))
        return 0
    accounting = PacedAccountingReferenceTransport(
        RequestsReferenceHttpTransport(timeout_seconds=30),
        maximum_requests=1,
        minimum_interval_seconds=0.5,
        authorized_until=args.authorized_until,
    )
    transport = TreasuryReferenceTransport(
        accounting,
        limits=ReferenceTransportLimits(
            maximum_requests=1,
            maximum_response_bytes=args.response_byte_limit,
            maximum_total_bytes=args.response_byte_limit,
            maximum_rows=1_000,
            maximum_pages=1,
        ),
    )
    request = ReferenceSeedRequest(
        EQUITY_VALIDATION_UNIVERSE,
        args.session_date,
        checked_at,
        checked_at,
        args.authorized_until,
    )
    generation = TreasuryReferenceAdapter(
        transport.fetch_xml,
        metrics=transport,
    ).collect(request)
    if generation.provider_requests != 1 or not generation.facts:
        raise RuntimeError("Treasury generation identity changed")
    rate_dates = {
        dict(fact.values)["rate_date"]
        for fact in generation.facts
    }
    if (
        len(rate_dates) != 1
        or next(iter(rate_dates)) > args.session_date
    ):
        raise ValueError("Treasury curve is not causal for the session")
    provider, pool = _provider()
    try:
        with provider.transaction(APP_ROLE) as unit:
            persist_reference_facts_cursor(
                unit.cursor,
                generation.facts,
            )
    finally:
        pool.closeall()
    report = {
        **plan,
        "status": "COMPLETE",
        "rate_date": next(iter(rate_dates)).isoformat(),
        "provider_requests": generation.provider_requests,
        "provider_response_bytes": generation.response_bytes,
        "database_writes": len(generation.facts),
        "fact_sha256": canonical_sha256(generation.facts),
        "credentials_exposed": False,
    }
    report["semantic_sha256"] = canonical_sha256(report)
    write_report(args.report_path, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
