#!/usr/bin/env python3
"""Fetch one bounded, late-observed ETF reference as-of one prior session."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.data.polygon_http import (  # noqa: E402
    PolygonResponseTooLarge,
    RequestsPolygonHttpTransport,
)
from realtime.equity_recent_repair import (  # noqa: E402
    AuthorizedNativeRepairTransport,
)
from scripts.realtime_script_support import write_report  # noqa: E402


CONTRACT = "v2-dated-equity-reference-v1"
URL = "https://api.massive.com/v3/reference/tickers/{ticker}"


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ticker", required=True)
    parser.add_argument("--session-date", type=date.fromisoformat, required=True)
    parser.add_argument("--authorized-until", type=datetime.fromisoformat, required=True)
    parser.add_argument("--maximum-provider-requests", type=int, required=True)
    parser.add_argument("--response-byte-limit", type=int, required=True)
    parser.add_argument("--authorization-reference", required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args(argv)
    if (
        args.authorized_until.tzinfo is None
        or args.authorized_until.utcoffset() is None
        or args.maximum_provider_requests != 1
        or args.response_byte_limit != 1_000_000
        or not args.authorization_reference.strip()
    ):
        parser.error("dated ETF reference scope or budget is invalid")
    args.ticker = args.ticker.strip().upper()
    if not args.ticker or not args.ticker.isascii() or not args.ticker.isalnum():
        parser.error("--ticker must be one exact ASCII symbol")
    args.authorized_until = args.authorized_until.astimezone(timezone.utc)
    args.report_path = args.report_path.resolve()
    if args.report_path.exists():
        parser.error("--report-path must be new")
    return args


def validate_result(result, *, ticker, session_date):
    if not isinstance(result, dict):
        raise ValueError("dated ETF reference result is unavailable")
    returned_ticker = str(result.get("ticker") or "").strip().upper()
    security_type = str(result.get("type") or "").strip().upper()
    cik = str(result.get("cik") or "").strip()
    if (
        returned_ticker != ticker
        or security_type not in {"ETF", "ETV"}
        or not (
            result.get("composite_figi")
            or result.get("share_class_figi")
            or (cik.isdigit() and cik.lstrip("0"))
        )
    ):
        raise ValueError("dated ETF reference identity is invalid")
    return {
        **result,
        "ticker": returned_ticker,
        "type": security_type,
        "source_as_of_date": session_date.isoformat(),
    }


def main(argv=None):
    args = parse_args(argv)
    plan = {
        "contract": CONTRACT,
        "mode": "EXECUTE" if args.execute else "PLAN",
        "ticker": args.ticker,
        "source_as_of_date": args.session_date.isoformat(),
        "maximum_provider_requests": 1,
        "response_byte_limit": args.response_byte_limit,
        "authorization_reference": args.authorization_reference,
    }
    if not args.execute:
        report = {
            **plan,
            "status": "PLANNED",
            "provider_requests": 0,
            "database_writes": 0,
        }
        write_report(args.report_path, report)
        print(json.dumps(report, sort_keys=True))
        return 0
    load_dotenv(BACKEND_DIR / ".env", override=True)
    api_key = os.getenv("POLYGON_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("dated ETF reference requires POLYGON_API_KEY")
    transport = AuthorizedNativeRepairTransport(
        RequestsPolygonHttpTransport(
            api_key,
            timeout_seconds=30,
            maximum_response_bytes=args.response_byte_limit,
        ),
        args.authorized_until,
        lambda: datetime.now(timezone.utc),
    )
    try:
        response = transport.get(
            URL.format(ticker=args.ticker),
            {"date": args.session_date.isoformat()},
        )
    except PolygonResponseTooLarge:
        raise RuntimeError("dated ETF reference exceeded its response limit")
    observed_at = datetime.now(timezone.utc)
    if response.status_code != 200:
        raise RuntimeError(
            f"dated ETF reference returned HTTP {response.status_code}"
        )
    payload = json.loads(response.body)
    if not isinstance(payload, dict) or payload.get("next_url"):
        raise ValueError("dated ETF reference response is invalid")
    result = validate_result(
        payload.get("results"),
        ticker=args.ticker,
        session_date=args.session_date,
    )
    report = {
        **plan,
        "status": "PASS",
        "observed_at": observed_at.isoformat(),
        "response_bytes": len(response.body),
        "response_sha256": hashlib.sha256(response.body).hexdigest(),
        "result": result,
        "provider_requests": 1,
        "database_writes": 0,
        "credentials_exposed": False,
    }
    write_report(args.report_path, report)
    print(json.dumps({
        key: value
        for key, value in report.items()
        if key != "result"
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
