#!/usr/bin/env python3
"""Admit expired option contracts so their historical marks have somewhere to live.

The contract catalog records what the pipeline observed in a live chain, so it only
reaches back as far as the pipeline has been running. The daily fact table's foreign key
points at it, which means a multi-year mark backfill cannot be written until the older
contracts exist as catalog rows.

What this changes and what it does not: no schema change, no existing row is modified,
and no market fact is invented. Strike, expiration and type come from the same provider
endpoint whether learned live or retrospectively. What differs is only the observation
metadata, and it is recorded rather than blurred:

  - `eligibility_status` is EXPIRED, never VALIDATED_ACTIVE, so `get_by_ticker` returns
    None and no live cycle can select these contracts.
  - `exclusion_reasons` carries HISTORICAL_REFERENCE_BACKFILL, so a backfilled contract
    stays distinguishable from one that was observed live and later expired.
  - `first_observed_at` is when this ran. Catalog reads gate on it, so a replay of any
    earlier decision cannot see these contracts, which is correct: they were not known.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from urllib.parse import urlparse

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from http_client import get_session  # noqa: E402
from options.calendar import OptionExchangeCalendar  # noqa: E402
from options.config import load_option_runtime_configuration  # noqa: E402
from options.domain import AssetType, ContractType  # noqa: E402
from options.repositories.catalog import (  # noqa: E402
    HistoricalContractAdmission,
    OptionContractCatalogRepository,
)

BASE_URL = "https://api.polygon.io"
ALLOWED_HOSTS = frozenset({"api.polygon.io", "api.massive.com"})
DEFAULT_OUTPUT = BACKEND_DIR.parent / "docs" / "option_historical_catalog_admission.json"
IV_CONTEXT_HORIZON_DAYS = (7, 21, 45)

SQL_ASSET_TYPES = """
    SELECT DISTINCT ticker, asset_type FROM option_universe_members
"""

SQL_EXISTING_TICKERS = """
    SELECT contract_ticker FROM option_contract_catalog WHERE underlying = ANY(%s)
"""

SQL_ANCHOR_CLOSE = """
    SELECT close_price
    FROM equity_bar_revisions
    WHERE interval = '1d' AND is_final AND session_scope = 'RTH' AND adjusted = false
      AND ticker = %s AND session_date <= %s
    ORDER BY session_date DESC
    LIMIT 1
"""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--underlyers", default=None, help="Comma-separated subset.")
    parser.add_argument("--months", type=int, default=24, help="Monthly expirations sampled.")
    parser.add_argument(
        "--weekly-expirations",
        type=int,
        default=0,
        help="Add this many recent Friday expirations to the monthly sample.",
    )
    parser.add_argument(
        "--strikes",
        type=int,
        default=21,
        help="Maximum contracts per expiration across horizon anchors and both sides.",
    )
    parser.add_argument("--moneyness-band", type=float, default=0.10)
    parser.add_argument(
        "--window-days",
        type=int,
        default=60,
        help="Sessions before expiry the admitted terms are asserted to cover.",
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def _monthly_expirations(count: int, latest: date) -> list[date]:
    return OptionExchangeCalendar.monthly_expirations(count, latest)


def _select_anchor_contracts(
    results: tuple[dict[str, Any], ...],
    anchors: tuple[float, ...],
    maximum_contracts: int,
) -> tuple[dict[str, Any], ...]:
    if not anchors or maximum_contracts < 1:
        return ()
    eligible = tuple(
        item for item in results
        if isinstance(item.get("strike_price"), (int, float))
        and str(item.get("contract_type", "")).upper() in ("CALL", "PUT")
    )
    queues = [
        sorted(
            (
                item for item in eligible
                if str(item["contract_type"]).upper() == contract_type
            ),
            key=lambda item: (
                abs(float(item["strike_price"]) - anchor),
                float(item["strike_price"]),
            ),
        )
        for anchor in anchors
        for contract_type in ("CALL", "PUT")
    ]
    selected: dict[str, dict[str, Any]] = {}
    while len(selected) < maximum_contracts:
        advanced = False
        for queue in queues:
            while queue and queue[0].get("ticker") in selected:
                queue.pop(0)
            if not queue:
                continue
            item = queue.pop(0)
            ticker = item.get("ticker")
            if not ticker:
                continue
            selected[ticker] = item
            advanced = True
            if len(selected) >= maximum_contracts:
                break
        if not advanced:
            break
    return tuple(selected.values())


def _retry_after_seconds(value: str | None, fallback: float) -> float:
    try:
        parsed = float(value) if value is not None else fallback
    except ValueError:
        return fallback
    return max(parsed, 0.0)


def _get_results(
    session: requests.Session,
    api_key: str,
    path: str,
    params: dict[str, Any],
    *,
    attempts: int = 6,
) -> tuple[tuple[dict[str, Any], ...], int]:
    url = f"{BASE_URL}{path}"
    request_params = dict(params)
    rows: list[dict[str, Any]] = []
    requests_used = 0
    seen_urls: set[str] = set()
    while url:
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.hostname not in ALLOWED_HOSTS:
            raise ValueError("Polygon pagination URL must remain on an approved HTTPS host")
        if url in seen_urls:
            raise ValueError("Polygon pagination repeated a request URL")
        seen_urls.add(url)
        delay = 2.0
        for attempt in range(1, attempts + 1):
            response = session.get(
                url,
                params=request_params,
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=30,
                allow_redirects=False,
            )
            requests_used += 1
            if response.status_code == 200:
                break
            if response.status_code not in (429, 502, 503, 504) or attempt == attempts:
                raise requests.HTTPError(
                    f"Polygon option references failed with HTTP {response.status_code}",
                    response=response,
                )
            retry_after = _retry_after_seconds(
                response.headers.get("Retry-After"), delay
            )
            print(
                f"  polygon {response.status_code}; retrying in {retry_after:.0f}s",
                flush=True,
            )
            time.sleep(retry_after)
            delay = min(max(delay * 2, retry_after), 60.0)
        try:
            payload = response.json()
        except ValueError as exc:
            raise ValueError("Polygon option references returned invalid JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("Polygon option references must be a JSON object")
        results = payload.get("results") or []
        if not isinstance(results, list) or any(not isinstance(row, dict) for row in results):
            raise ValueError("Polygon option references have an invalid results array")
        rows.extend(results)
        next_url = payload.get("next_url")
        url = str(next_url) if next_url else ""
        request_params = {}
    return tuple(rows), requests_used


def main() -> int:
    args = _parse_args()
    if args.months < 0 or args.weekly_expirations < 0:
        raise ValueError("expiration sample counts cannot be negative")
    if args.months == 0 and args.weekly_expirations == 0:
        raise ValueError("at least one expiration sample is required")
    if args.strikes < 2:
        raise ValueError("at least two contracts per expiration are required")
    configuration = load_option_runtime_configuration()
    api_key = configuration.settings.polygon_api_key.get_secret_value()
    calendar = OptionExchangeCalendar()
    http = get_session()

    with get_db_cursor() as cursor:
        cursor.execute(SQL_ASSET_TYPES)
        asset_types = {row["ticker"]: row["asset_type"] for row in cursor.fetchall()}

    underlyers = (
        tuple(v.strip().upper() for v in args.underlyers.split(",") if v.strip())
        if args.underlyers
        else tuple(sorted(asset_types))
    )

    with get_db_cursor() as cursor:
        cursor.execute(SQL_EXISTING_TICKERS, (list(underlyers),))
        known = {row["contract_ticker"] for row in cursor.fetchall()}

    requests_used = 0
    admissions: list[HistoricalContractAdmission] = []
    per_underlying: dict[str, int] = defaultdict(int)
    already_known = 0

    for underlying in underlyers:
        asset_type = AssetType(asset_types.get(underlying, "STOCK"))
        expirations = set(
            _monthly_expirations(args.months, date.today())
            if args.months
            else ()
        )
        if args.weekly_expirations:
            expirations.update(
                calendar.weekly_expirations(
                    args.weekly_expirations,
                    date.today(),
                )
            )
        for expiration in sorted(expirations):
            window_start = expiration - timedelta(days=args.window_days)
            anchors = []
            with get_db_cursor() as cursor:
                for horizon_days in IV_CONTEXT_HORIZON_DAYS:
                    cursor.execute(
                        SQL_ANCHOR_CLOSE,
                        (underlying, expiration - timedelta(days=horizon_days)),
                    )
                    row = cursor.fetchone()
                    if row:
                        anchors.append(float(row["close_price"]))
            if not anchors:
                continue

            results, request_count = _get_results(
                http,
                api_key,
                "/v3/reference/options/contracts",
                {
                    "underlying_ticker": underlying,
                    "expiration_date": expiration.isoformat(),
                    "expired": "true",
                    "strike_price.gte": (
                        f"{min(anchors) * (1 - args.moneyness_band):.2f}"
                    ),
                    "strike_price.lte": (
                        f"{max(anchors) * (1 + args.moneyness_band):.2f}"
                    ),
                    "limit": "1000",
                    "sort": "strike_price",
                    "order": "asc",
                },
            )
            requests_used += request_count

            selected = _select_anchor_contracts(
                results,
                tuple(anchors),
                args.strikes,
            )
            for item in selected:
                ticker = item.get("ticker")
                if not ticker:
                    continue
                if ticker in known:
                    already_known += 1
                    continue
                admissions.append(
                    HistoricalContractAdmission(
                        contract_ticker=ticker,
                        underlying=underlying,
                        asset_type=asset_type,
                        contract_type=ContractType(str(item["contract_type"]).upper()),
                        expiration_date=expiration,
                        strike=Decimal(str(item["strike_price"])),
                        # Terms held over the window the marks will cover; the market
                        # axis, distinct from when the contract became known to us.
                        valid_from=datetime.combine(
                            window_start, datetime.min.time(), tzinfo=timezone.utc
                        ),
                        valid_to=calendar.expiration_cutoff(expiration),
                        shares_per_contract=int(item.get("shares_per_contract") or 100),
                        primary_exchange=item.get("primary_exchange"),
                    )
                )
                per_underlying[underlying] += 1

    print(f"{len(admissions)} contracts to admit ({already_known} already catalogued)")
    for underlying, count in sorted(per_underlying.items()):
        print(f"  {underlying:<6} {count}")
    print(f"reference requests used: {requests_used}")

    if not args.apply:
        print("\nDry run. Re-run with --apply to admit.")
        return 0

    repository = OptionContractCatalogRepository()
    admitted = repository.admit_historical_contracts(
        admissions, datetime.now(timezone.utc)
    )
    print(f"\nadmitted {len(admitted)} contracts as EXPIRED")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "months": args.months,
        "weekly_expirations": args.weekly_expirations,
        "maximum_contracts_per_expiration": args.strikes,
        "iv_context_horizon_days": IV_CONTEXT_HORIZON_DAYS,
        "moneyness_band": args.moneyness_band,
        "reference_requests": requests_used,
        "already_catalogued": already_known,
        "admitted": len(admitted),
        "admitted_by_underlying": dict(sorted(per_underlying.items())),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
