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

from realtime.contracts import canonical_sha256  # noqa: E402
from realtime.database_access_v2 import (  # noqa: E402
    APP_ROLE,
    V2AccessConfig,
    V2ConnectionProvider,
)
from realtime.migrations import (  # noqa: E402
    EXPECTED_V2_DATABASE_NAME,
    validate_target_database_v2,
)
from realtime.reference_repository import (  # noqa: E402
    persist_reference_facts_cursor,
)
from realtime.treasury_refresh import (  # noqa: E402
    BoundedTreasuryHttp,
    TREASURY_RESPONSE_BYTE_LIMIT,
    TreasuryAuthorizationFence,
    collect_treasury_facts,
    required_treasury_rate_date,
    verify_treasury_curve_cursor,
)
from scripts.realtime_script_support import write_report  # noqa: E402


UTC = timezone.utc


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--confirm-database-name", required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--required-rate-date", type=date.fromisoformat)
    parser.add_argument("--authorized-until", type=datetime.fromisoformat)
    parser.add_argument("--maximum-provider-requests", type=int)
    parser.add_argument("--response-byte-limit", type=int)
    parser.add_argument("--authorization-reference")
    parser.add_argument("--report-path", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true")
    mode.add_argument("--verify", action="store_true",
                      help="Read stored evidence only; no provider calls, writes or migrations")
    args = parser.parse_args(argv)
    validate_target_database_v2(
        args.database,
        args.confirm_database_name,
        security_mode="hardened",
    )
    if args.database != EXPECTED_V2_DATABASE_NAME:
        parser.error("Treasury repair targets stocks_realtime_v2")
    expected_date = required_treasury_rate_date(args.session_date)
    if args.required_rate_date is not None and args.required_rate_date != expected_date:
        parser.error("--required-rate-date must be the exact prior XNYS session")
    args.required_rate_date = expected_date
    if not args.verify and (
        args.authorized_until is None
        or args.authorized_until.tzinfo is None
        or args.authorized_until.utcoffset() is None
        or args.maximum_provider_requests != 1
        or args.response_byte_limit != TREASURY_RESPONSE_BYTE_LIMIT
        or not (args.authorization_reference or "").strip()
        or args.report_path is None
    ):
        parser.error("Treasury repair scope or budget is invalid")
    if args.authorized_until is not None:
        args.authorized_until = args.authorized_until.astimezone(UTC)
    if args.report_path is not None:
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
    if args.verify:
        provider, pool = _provider()
        try:
            with provider.transaction(
                APP_ROLE, read_only=True, isolation_level="REPEATABLE READ",
            ) as unit:
                report = verify_treasury_curve_cursor(
                    unit.cursor, session_date=args.session_date, checked_at=checked_at,
                )
        finally:
            pool.closeall()
        report.update({
            "mode": "VERIFY", "database": args.database,
            "provider_requests": 0, "database_writes": 0, "migrations": 0,
        })
        if args.report_path is not None:
            write_report(args.report_path, report)
        print(json.dumps(report, sort_keys=True))
        return 0 if report["status"] == "VERIFIED" else 1
    if not checked_at <= args.authorized_until <= checked_at + timedelta(hours=1):
        raise ValueError("Treasury authorization is outside its TTL")
    plan = {
        "contract": "realtime-v2-treasury-curve-repair-v1",
        "mode": "EXECUTE" if args.execute else "PLAN",
        "database": args.database,
        "session_date": args.session_date.isoformat(),
        "required_rate_date": args.required_rate_date.isoformat(),
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
    provider, pool = _provider()
    try:
        http = BoundedTreasuryHttp(authorized_until=args.authorized_until)
        facts = collect_treasury_facts(
            required_rate_date=args.required_rate_date, http=http,
            checked_at=lambda: datetime.now(UTC),
        )
        with provider.transaction(
            APP_ROLE, fence=TreasuryAuthorizationFence(args.authorized_until),
        ) as unit:
            persist_reference_facts_cursor(unit.cursor, facts)
            verification = verify_treasury_curve_cursor(
                unit.cursor, session_date=args.session_date, checked_at=datetime.now(UTC),
            )
            if verification["status"] != "VERIFIED":
                raise ValueError("Persisted Treasury curve did not verify")
    finally:
        pool.closeall()
    report = {
        **plan,
        "status": "COMPLETE",
        "rate_date": args.required_rate_date.isoformat(),
        "provider_requests": http.requests,
        "provider_response_bytes": http.response_bytes,
        "database_writes": len(facts),
        "fact_sha256": canonical_sha256(facts),
        "verification": verification,
        "credentials_exposed": False,
    }
    report["semantic_sha256"] = canonical_sha256(report)
    write_report(args.report_path, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
