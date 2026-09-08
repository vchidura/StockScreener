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
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from options.calendar import OptionExchangeCalendar  # noqa: E402
from options.config import load_option_runtime_configuration  # noqa: E402
from options.domain import AssetType, ContractType  # noqa: E402
from options.repositories.catalog import (  # noqa: E402
    HistoricalContractAdmission,
    OptionContractCatalogRepository,
)

BASE_URL = "https://api.polygon.io"
DEFAULT_OUTPUT = BACKEND_DIR.parent / "docs" / "option_historical_catalog_admission.json"

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
    parser.add_argument("--strikes", type=int, default=21, help="Strikes per expiration.")
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


def _get(session: requests.Session, api_key: str, path: str, params: dict[str, Any]):
    response = session.get(
        f"{BASE_URL}{path}", params={**params, "apiKey": api_key}, timeout=30
    )
    try:
        return response.status_code, response.json()
    except ValueError:
        return response.status_code, {}


def main() -> int:
    args = _parse_args()
    configuration = load_option_runtime_configuration()
    api_key = configuration.settings.polygon_api_key.get_secret_value()
    calendar = OptionExchangeCalendar()
    http = requests.Session()

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
        for expiration in _monthly_expirations(args.months, date.today()):
            window_start = expiration - timedelta(days=args.window_days)
            with get_db_cursor() as cursor:
                cursor.execute(SQL_ANCHOR_CLOSE, (underlying, expiration))
                row = cursor.fetchone()
            if not row:
                continue
            anchor = float(row["close_price"])

            status, payload = _get(
                http,
                api_key,
                "/v3/reference/options/contracts",
                {
                    "underlying_ticker": underlying,
                    "expiration_date": expiration.isoformat(),
                    "expired": "true",
                    "strike_price.gte": f"{anchor * (1 - args.moneyness_band):.2f}",
                    "strike_price.lte": f"{anchor * (1 + args.moneyness_band):.2f}",
                    "limit": "250",
                },
            )
            requests_used += 1
            results = payload.get("results") if isinstance(payload, dict) else None
            if status != 200 or not isinstance(results, list):
                continue

            ranked = sorted(
                (
                    item
                    for item in results
                    if isinstance(item.get("strike_price"), (int, float))
                    and str(item.get("contract_type", "")).upper() in ("CALL", "PUT")
                ),
                key=lambda item: abs(float(item["strike_price"]) - anchor),
            )
            for item in ranked[: args.strikes]:
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
        "strikes_per_expiration": args.strikes,
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
