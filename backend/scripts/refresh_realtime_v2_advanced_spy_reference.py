#!/usr/bin/env python3
"""Append one bounded SPY-only reference generation for Advanced equity."""
from __future__ import annotations

import argparse
import json
import os
import sys
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from realtime.contracts import UniverseManifest  # noqa: E402
from options.calendar import OptionExchangeCalendar  # noqa: E402
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.reference_repository import persist_reference_result  # noqa: E402
from realtime.reference_seed import SeedStatus  # noqa: E402
from realtime.reference_service import ReferenceSeedRequest  # noqa: E402
from realtime.reference_transports import (  # noqa: E402
    ReferenceTransportLimits,
    RequestsReferenceHttpTransport,
    build_live_reference_service,
)
from scripts.realtime_script_support import write_report  # noqa: E402
from scripts.realtime_v2_audit_support import (  # noqa: E402
    admin_settings,
    connect,
)
from scripts.refresh_realtime_v2_references import (  # noqa: E402
    PacedAccountingReferenceTransport,
)


MAXIMUM_REQUESTS = 12
MAXIMUM_TOTAL_BYTES = 12_000_000
MAXIMUM_RESPONSE_BYTES = 3_000_000
MINIMUM_INTERVAL_SECONDS = 0.5
SEED_SCOPE = "ADVANCED_SPY_REFERENCES"
UNIVERSE = UniverseManifest(
    "advanced-equity-spy-reference-v1",
    ("SPY",),
    (),
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--authorized-session-date", type=date.fromisoformat)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("SPY reference refresh targets only stocks_realtime_v2")
    authorized_session = args.authorized_session_date or date(2026, 10, 16)
    if args.session_date != authorized_session:
        parser.error("--session-date differs from the explicitly authorized session")
    try:
        OptionExchangeCalendar("XNYS").session_open(args.session_date)
    except ValueError as error:
        parser.error(str(error))
    args.authorized_session_date = authorized_session
    if not args.authorization_reference.strip():
        parser.error("--authorization-reference cannot be blank")
    args.report_path = args.report_path.resolve()
    if args.report_path.exists():
        parser.error("--report-path must be new")
    return args


def main(argv=None):
    args = parse_args(argv)
    plan = {
        "mode": "EXECUTE" if args.execute else "PLAN",
        "database": args.database,
        "scope": SEED_SCOPE,
        "session_date": args.session_date.isoformat(),
        "authorized_session_date": args.authorized_session_date.isoformat(),
        "authorization_reference": args.authorization_reference,
        "symbols": ("SPY",),
        "maximum_requests": MAXIMUM_REQUESTS,
        "maximum_total_bytes": MAXIMUM_TOTAL_BYTES,
        "maximum_response_bytes": MAXIMUM_RESPONSE_BYTES,
        "request_in_flight": 1,
        "minimum_request_interval_seconds": MINIMUM_INTERVAL_SECONDS,
        "publication_advanced": False,
    }
    if not args.execute:
        print(json.dumps({
            **plan,
            "provider_requests": 0,
            "database_writes": 0,
        }, sort_keys=True))
        return 0
    settings = admin_settings(BACKEND_DIR / ".env")
    load_dotenv(BACKEND_DIR / ".env", override=True)
    massive_key = os.getenv("POLYGON_API_KEY", "").strip()
    finnhub_key = os.getenv("FINNHUB_API_KEY", "").strip()
    if not massive_key or not finnhub_key:
        raise RuntimeError(
            "SPY reference refresh requires configured provider keys"
        )
    started_at = datetime.now(timezone.utc)
    transport = PacedAccountingReferenceTransport(
        RequestsReferenceHttpTransport(),
        maximum_requests=MAXIMUM_REQUESTS,
        minimum_interval_seconds=MINIMUM_INTERVAL_SECONDS,
    )
    service = build_live_reference_service(
        massive_api_key=massive_key,
        finnhub_api_key=finnhub_key,
        etf_tickers=frozenset({"SPY"}),
        http=transport,
        massive_base_url="https://api.massive.com",
        limits=ReferenceTransportLimits(
            maximum_requests=MAXIMUM_REQUESTS,
            maximum_response_bytes=MAXIMUM_RESPONSE_BYTES,
            maximum_total_bytes=MAXIMUM_TOTAL_BYTES,
            maximum_rows=50_000,
            maximum_pages=8,
        ),
        include_fundamentals=False,
    )
    result = service.run_once(
        ReferenceSeedRequest(
            UNIVERSE,
            args.session_date,
            started_at,
            started_at + timedelta(minutes=1),
            started_at + timedelta(hours=6),
        )
    )
    response_bytes = sum(
        row["response_bytes"] for row in transport.requests
    )
    if (
        result.manifest.status is not SeedStatus.READY
        or result.provider_requests != len(transport.requests)
        or not 1 <= result.provider_requests <= MAXIMUM_REQUESTS
        or response_bytes > MAXIMUM_TOTAL_BYTES
        or any(
            row["status_code"] != 200
            for row in transport.requests
        )
        or not any(
            member.kind.value == "SECURITY"
            and member.key == "SPY"
            for member in result.manifest.members
        )
    ):
        raise RuntimeError("SPY reference refresh failed its exact contract")
    with closing(
        connect(
            args.database,
            settings,
            application_name="refresh_v2_advanced_spy_reference",
        )
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COALESCE(MAX(generation),0)+1 "
                "FROM rt.seed_current WHERE scope=%s",
                (SEED_SCOPE,),
            )
            generation = int(cursor.fetchone()[0])
        connection.rollback()
        try:
            persist_reference_result(
                connection,
                result,
                generation=generation,
                scope=SEED_SCOPE,
            )
        except Exception as error:
            failure = {
                **plan,
                "status": "DATABASE_WRITE_FAILED",
                "failure_code": type(error).__name__,
                "provider_requests": result.provider_requests,
                "response_bytes": response_bytes,
                "request_log": transport.requests,
                "database_writes": 0,
                "credentials_exposed": False,
                "publication_advanced": False,
                "production_bar_writes": 0,
            }
            write_report(args.report_path, failure)
            raise
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM ref.security_revisions "
                "WHERE ticker='SPY' AND active "
                "AND effective_from<=clock_timestamp() "
                "AND (effective_to IS NULL "
                "OR effective_to>clock_timestamp())"
            )
            effective_count = int(cursor.fetchone()[0])
        connection.rollback()
    report = {
        **plan,
        "status": "READY",
        "provider_requests": result.provider_requests,
        "response_bytes": response_bytes,
        "generation": generation,
        "member_count": len(result.manifest.members),
        "effective_spy_revisions": effective_count,
        "request_log": transport.requests,
        "credentials_exposed": False,
        "publication_advanced": False,
        "production_bar_writes": 0,
    }
    if effective_count < 1:
        raise RuntimeError("SPY reference refresh did not create current identity")
    write_report(args.report_path, report)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
