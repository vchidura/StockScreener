#!/usr/bin/env python3
"""Measure the live Finnhub earnings contract without persisting provider data."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import time


BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv

load_dotenv(BACKEND_DIR / ".env", override=True)

from market_events import (  # noqa: E402
    FINNHUB_MAX_WINDOW_DAYS,
    FINNHUB_REQUEST_INTERVAL_SECONDS,
    FinnhubEarningsClient,
    FinnhubRateLimitError,
)
from scripts.run_market_event_worker import (  # noqa: E402
    HORIZON_DAYS,
    LOOKBACK_DAYS,
    POLL_SECONDS,
    active_company_tickers,
)


FIELDS = ("symbol", "date", "year", "quarter", "hour")
SAMPLE_FIELDS = (
    "symbol", "date", "hour", "year", "quarter",
    "epsActual", "epsEstimate", "revenueActual", "revenueEstimate",
)
OPTION_STOCKS = frozenset({
    "AAPL", "AMD", "AMZN", "GOOGL", "META",
    "MSFT", "NVDA", "PLTR", "SOFI", "TSLA",
})
VALID_HOURS = frozenset({"bmo", "amc", "dmh", ""})
VALIDATION_REQUEST_INTERVAL_SECONDS = 2.1


def _canonical_sha256(rows: tuple[dict, ...]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _row_problems(row: dict) -> tuple[str, ...]:
    problems = []
    symbol = row.get("symbol")
    if not isinstance(symbol, str) or not symbol.strip():
        problems.append("symbol")
    try:
        date.fromisoformat(str(row.get("date") or ""))
    except ValueError:
        problems.append("date")
    if not isinstance(row.get("year"), int):
        problems.append("year")
    quarter = row.get("quarter")
    if not isinstance(quarter, int) or quarter not in range(1, 5):
        problems.append("quarter")
    hour = row.get("hour")
    if hour is not None and str(hour).lower() not in VALID_HOURS:
        problems.append("hour")
    return tuple(problems)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--cross-check-daily",
        action="store_true",
        help="Compare the global window with paced one-day requests.",
    )
    result.add_argument(
        "--cross-check-global",
        action="store_true",
        help="Compare one global response with the bounded production requests.",
    )
    result.add_argument(
        "--show-sample",
        action="store_true",
        help="Include three sanitized representative earnings rows.",
    )
    return result


def main() -> int:
    args = parser().parse_args()
    api_key = os.getenv("FINNHUB_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("FINNHUB_API_KEY is not configured")
    observed_at = datetime.now(timezone.utc)
    start = (observed_at - timedelta(days=LOOKBACK_DAYS)).date()
    end = (observed_at + timedelta(days=HORIZON_DAYS)).date()
    company_tickers = frozenset(active_company_tickers(observed_at))
    started = time.monotonic()
    rows = FinnhubEarningsClient(api_key).fetch(start, end)
    elapsed = time.monotonic() - started
    normalized = tuple(dict(row) for row in rows)
    global_cross_check = None
    if args.cross_check_global:
        time.sleep(VALIDATION_REQUEST_INTERVAL_SECONDS)
        global_rows = tuple(dict(row) for row in FinnhubEarningsClient(
            api_key
        )._fetch_chunk(start, end))
        bounded_facts = Counter(
            json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
            for row in normalized
        )
        global_facts = Counter(
            json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
            for row in global_rows
        )
        global_cross_check = {
            "global_row_count": len(global_rows),
            "missing_from_bounded": sum((global_facts - bounded_facts).values()),
            "extra_in_bounded": sum((bounded_facts - global_facts).values()),
        }
    eligible_rows = tuple(
        row for row in normalized
        if str(row.get("symbol") or "").strip().upper() in company_tickers
    )
    problems = Counter(
        problem for row in eligible_rows for problem in _row_problems(row)
    )
    invalid_symbols = sorted({
        str(row.get("symbol") or "<missing>").strip().upper()
        for row in eligible_rows
        if _row_problems(row)
    })
    identities = [
        (
            str(row.get("symbol") or "").strip().upper(),
            row.get("year"),
            row.get("quarter"),
        )
        for row in eligible_rows
    ]
    duplicate_identities = sum(
        count - 1 for count in Counter(identities).values() if count > 1
    )
    samples = []
    if args.show_sample:
        for expected_hour in ("bmo", "amc", "<missing>"):
            candidates = [
                row for row in eligible_rows
                if str(row.get("hour") or "<missing>").lower() == expected_hour
            ]
            candidates.sort(key=lambda row: (
                str(row.get("symbol") or "").upper() not in OPTION_STOCKS,
                str(row.get("date") or ""),
                str(row.get("symbol") or ""),
            ))
            if candidates:
                samples.append({field: candidates[0].get(field) for field in SAMPLE_FIELDS})
    request_days = (end - start).days + 1
    bounded_calls = (
        request_days + FINNHUB_MAX_WINDOW_DAYS - 1
    ) // FINNHUB_MAX_WINDOW_DAYS
    calls_per_poll = bounded_calls + (1 if bounded_calls > 1 else 0)
    daily_cross_check = None
    if args.cross_check_daily:
        daily_rows = []
        next_request_at = time.monotonic() + VALIDATION_REQUEST_INTERVAL_SECONDS
        current = start
        rate_limit = None
        attempted_requests = 0
        while current <= end:
            remaining = next_request_at - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)
            try:
                attempted_requests += 1
                daily_rows.extend(
                    FinnhubEarningsClient(api_key).fetch(current, current)
                )
            except FinnhubRateLimitError as exc:
                rate_limit = {
                    "date": current,
                    "retry_after_seconds": exc.retry_after_seconds,
                }
                break
            next_request_at = time.monotonic() + VALIDATION_REQUEST_INTERVAL_SECONDS
            current += timedelta(days=1)
        global_facts = Counter(
            json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
            for row in normalized
        )
        daily_facts = Counter(
            json.dumps(row, sort_keys=True, separators=(",", ":"), default=str)
            for row in daily_rows
        )
        daily_cross_check = {
            "request_count": attempted_requests,
            "expected_request_count": request_days,
            "minimum_request_interval_seconds": VALIDATION_REQUEST_INTERVAL_SECONDS,
            "row_count": len(daily_rows),
            "missing_from_global": sum((daily_facts - global_facts).values()),
            "extra_in_global": sum((global_facts - daily_facts).values()),
            "rate_limit": rate_limit,
        }
    parsed_dates = []
    outside_window = 0
    for row in normalized:
        try:
            parsed = date.fromisoformat(str(row.get("date") or ""))
        except ValueError:
            continue
        parsed_dates.append(parsed)
        outside_window += not start <= parsed <= end
    report = {
        "status": "PASS" if not problems and not duplicate_identities else "FAIL",
        "observed_at": observed_at,
        "request": {
            "start": start,
            "end": end,
            "elapsed_seconds": round(elapsed, 3),
            "calls_per_poll": calls_per_poll,
            "poll_seconds": POLL_SECONDS,
            "maximum_calls_per_day": (
                (86400 + POLL_SECONDS - 1) // POLL_SECONDS
            ) * calls_per_poll,
            "free_tier_limit_per_minute": 60,
            "inter_call_delay_seconds": (
                FINNHUB_REQUEST_INTERVAL_SECONDS if calls_per_poll > 1 else 0
            ),
        },
        "response": {
            "row_count": len(normalized),
            "sha256": _canonical_sha256(normalized),
            "minimum_date": min(parsed_dates) if parsed_dates else None,
            "maximum_date": max(parsed_dates) if parsed_dates else None,
            "outside_requested_window": outside_window,
            "unique_symbols": len({
                str(row.get("symbol") or "").strip().upper()
                for row in normalized if row.get("symbol")
            }),
            "hour_distribution": dict(sorted(Counter(
                str(row.get("hour") or "<missing>").lower()
                for row in normalized
            ).items())),
            "daily_cross_check": daily_cross_check,
            "global_cross_check": global_cross_check,
        },
        "eligible_universe": {
            "company_count": len(company_tickers),
            "response_row_count": len(eligible_rows),
            "response_symbol_count": len({
                str(row.get("symbol") or "").strip().upper()
                for row in eligible_rows
            }),
            "hour_distribution": dict(sorted(Counter(
                str(row.get("hour") or "<missing>").lower()
                for row in eligible_rows
            ).items())),
            "problem_counts": dict(sorted(problems.items())),
            "invalid_symbols": invalid_symbols,
            "duplicate_identity_rows": duplicate_identities,
        },
        "contract": {
            "required_fields": FIELDS,
            "nullable_fields": ["hour"],
            "stable_identity": ["symbol", "year", "quarter"],
            "global_equals_bounded": True,
            "pagination_documented": False,
            "provider_revision_timestamp_available": False,
            "provider_confirmation_flag_available": False,
        },
        "sample": samples if args.show_sample else None,
    }
    if daily_cross_check and (
        daily_cross_check["rate_limit"]
        or daily_cross_check["missing_from_global"]
        or daily_cross_check["extra_in_global"]
    ):
        report["status"] = "FAIL"
    if global_cross_check and (
        global_cross_check["missing_from_bounded"]
        or global_cross_check["extra_in_bounded"]
    ):
        report["status"] = "FAIL"
    print(json.dumps(report, indent=2, default=str))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())