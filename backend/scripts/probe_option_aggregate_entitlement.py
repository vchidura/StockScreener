#!/usr/bin/env python3
"""Probe whether the Polygon entitlement can supply historical option aggregates.

Read-only, no database writes. Answers one question: can an implied-volatility history
be manufactured under this platform's own solver by fetching per-contract daily bars for
contracts that have already expired?

Three things must all hold for that backfill to be possible:
  1. expired contracts are enumerable from the reference endpoint, otherwise the
     point-in-time contract universe for a past date cannot be reconstructed;
  2. daily aggregates are returned for an expired contract ticker;
  3. the lookback reaches far enough back to cover a usable number of sessions.

Contracts are chosen near the money using this platform's own underlying closes, because
Polygon daily option aggregates are built from trades: a session with no print produces
no bar at all. Probing deep-in-the-money strikes measures illiquidity, not entitlement.
The number that matters is session coverage, not whether any bar came back.

Anything less and the implied-versus-realized study stays gated on calendar time.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor  # noqa: E402
from options.config import load_option_runtime_configuration  # noqa: E402

BASE_URL = "https://api.polygon.io"
DEFAULT_OUTPUT = BACKEND_DIR.parent / "docs" / "option_aggregate_entitlement_probe.json"

SQL_CLOSE_NEAR = """
    SELECT session_date, close_price
    FROM equity_bar_revisions
    WHERE interval = '1d' AND is_final AND session_scope = 'RTH' AND adjusted = false
      AND ticker = %s AND session_date <= %s
    ORDER BY session_date DESC
    LIMIT 1
"""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--underlyer", default="SPY")
    parser.add_argument(
        "--lookback-years",
        default="0.5,1,2,3,3.5,4,4.5,5",
        help="Expiration ages to test, in years before today.",
    )
    parser.add_argument("--contracts-per-probe", type=int, default=3)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def _underlying_close(ticker: str, on_or_before: date) -> tuple[date, float] | None:
    with get_db_cursor() as cursor:
        cursor.execute(SQL_CLOSE_NEAR, (ticker, on_or_before))
        row = cursor.fetchone()
    if not row:
        return None
    return row["session_date"], float(row["close_price"])


class PolygonProbe:
    def __init__(self, api_key: str) -> None:
        self._session = requests.Session()
        self._api_key = api_key
        self.request_count = 0

    def get(self, path: str, params: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        self.request_count += 1
        response = self._session.get(
            f"{BASE_URL}{path}",
            params={**params, "apiKey": self._api_key},
            timeout=30,
        )
        try:
            payload = response.json()
        except ValueError:
            payload = {"raw": response.text[:400]}
        return response.status_code, payload


def _expired_contracts(
    probe: PolygonProbe, underlyer: str, expired_before: date, limit: int
) -> tuple[int, list[dict[str, Any]], str | None]:
    status, payload = probe.get(
        "/v3/reference/options/contracts",
        {
            "underlying_ticker": underlyer,
            "expired": "true",
            "expiration_date.lte": expired_before.isoformat(),
            "expiration_date.gte": (expired_before - timedelta(days=21)).isoformat(),
            "limit": str(limit),
            "sort": "expiration_date",
            "order": "desc",
        },
    )
    results = payload.get("results") if isinstance(payload, dict) else None
    if status != 200 or not isinstance(results, list):
        message = payload.get("message") or payload.get("error") if isinstance(payload, dict) else None
        return status, [], str(message) if message else None
    return status, results, None


def _daily_aggregates(
    probe: PolygonProbe, contract_ticker: str, start: date, end: date
) -> dict[str, Any]:
    status, payload = probe.get(
        f"/v2/aggs/ticker/{contract_ticker}/range/1/day/{start.isoformat()}/{end.isoformat()}",
        {"adjusted": "true", "sort": "asc", "limit": "5000"},
    )
    results = payload.get("results") if isinstance(payload, dict) else None
    # Weekday count is a close enough denominator for a coverage ratio at this scale.
    weekdays = sum(
        1
        for offset in range((end - start).days + 1)
        if (start + timedelta(days=offset)).weekday() < 5
    )
    record: dict[str, Any] = {
        "contract": contract_ticker,
        "http_status": status,
        "provider_status": payload.get("status") if isinstance(payload, dict) else None,
        "bar_count": len(results) if isinstance(results, list) else 0,
        "weekday_count": weekdays,
    }
    record["session_coverage"] = record["bar_count"] / weekdays if weekdays else 0.0
    if isinstance(payload, dict) and (payload.get("message") or payload.get("error")):
        record["message"] = str(payload.get("message") or payload.get("error"))
    if isinstance(results, list) and results:
        first, last = results[0], results[-1]
        # Stamped at the session open in UTC; local-time conversion would shift the date.
        record["first_bar"] = datetime.fromtimestamp(first["t"] / 1000, tz=timezone.utc).date().isoformat()
        record["last_bar"] = datetime.fromtimestamp(last["t"] / 1000, tz=timezone.utc).date().isoformat()
        record["fields_present"] = sorted(first.keys())
        # A settlement-style close plus volume is the minimum an IV solve needs.
        record["has_close"] = "c" in first
        record["has_volume"] = "v" in first
        record["total_volume"] = sum(int(row.get("v", 0)) for row in results)
    return record


def main() -> int:
    args = _parse_args()
    configuration = load_option_runtime_configuration()
    api_key = configuration.settings.polygon_api_key.get_secret_value()
    if not api_key:
        print("POLYGON_API_KEY is not configured.")
        return 1

    probe = PolygonProbe(api_key)
    today = date.today()
    report: dict[str, Any] = {
        "underlyer": args.underlyer,
        "probed_on": today.isoformat(),
        "reference_endpoint": {},
        "aggregate_endpoint": {},
    }

    print(f"Probing Polygon option-aggregate entitlement for {args.underlyer}\n")
    for years_text in args.lookback_years.split(","):
        years = float(years_text.strip())
        target = today - timedelta(days=int(years * 365))
        label = f"{years:g}y"

        status, contracts, message = _expired_contracts(
            probe, args.underlyer, target, 250
        )
        reference = {
            "target_expiration_on_or_before": target.isoformat(),
            "http_status": status,
            "contract_count": len(contracts),
            "message": message,
        }
        report["reference_endpoint"][label] = reference
        if not contracts:
            print(f"  {label:<4} reference: HTTP {status} — no expired contracts ({message or 'no message'})")
            report["aggregate_endpoint"][label] = []
            continue

        spot = _underlying_close(args.underlyer, target)
        if spot is None:
            print(f"  {label:<4} no local underlying close near {target}; skipping")
            report["aggregate_endpoint"][label] = []
            continue
        spot_session, spot_price = spot
        reference["reference_spot"] = spot_price
        reference["reference_spot_session"] = spot_session.isoformat()

        # Near-the-money contracts are the ones an implied-volatility history needs.
        ranked = sorted(
            (
                contract
                for contract in contracts
                if isinstance(contract.get("strike_price"), (int, float))
            ),
            key=lambda contract: abs(float(contract["strike_price"]) - spot_price),
        )
        print(
            f"  {label:<4} reference: HTTP {status}, {len(contracts)} expired contracts,"
            f" spot {spot_price:.2f} on {spot_session}"
        )
        aggregates = []
        for contract in ranked[: args.contracts_per_probe]:
            ticker = contract.get("ticker")
            expiration = contract.get("expiration_date")
            if not ticker or not expiration:
                continue
            expiration_date = date.fromisoformat(expiration)
            record = _daily_aggregates(
                probe, ticker, expiration_date - timedelta(days=60), expiration_date
            )
            record["strike"] = contract.get("strike_price")
            record["contract_type"] = contract.get("contract_type")
            record["expiration_date"] = expiration
            record["moneyness"] = float(contract["strike_price"]) / spot_price
            aggregates.append(record)
            detail = (
                f"{record['bar_count']:>3} bars / {record['weekday_count']} weekdays"
                f"  coverage {record['session_coverage']:.0%}"
                if record["bar_count"]
                else f"NONE ({record.get('message', record.get('provider_status'))})"
            )
            print(f"         {ticker:<28} HTTP {record['http_status']}  {detail}")
        report["aggregate_endpoint"][label] = aggregates

    deepest = None
    for label, records in report["aggregate_endpoint"].items():
        if any(record.get("bar_count", 0) > 0 for record in records):
            deepest = label
    coverage_by_label = {
        label: max((record.get("session_coverage", 0.0) for record in records), default=0.0)
        for label, records in report["aggregate_endpoint"].items()
    }
    report["deepest_lookback_with_bars"] = deepest
    report["best_session_coverage_by_lookback"] = coverage_by_label
    report["request_count"] = probe.request_count

    print()
    if deepest:
        best = max(coverage_by_label.values(), default=0.0)
        print(f"Daily aggregates for expired near-the-money contracts reach back to at least {deepest}.")
        print(f"Best near-the-money session coverage observed: {best:.0%}")
        if best < 0.8:
            print(
                "WARNING: coverage is well below one bar per session. These aggregates are"
                " trade-based, so a backfill would recover only the sessions a contract"
                " actually traded — a liquidity-selected sample, not a continuous history."
            )
    else:
        print("VERDICT: no historical option aggregates returned. Half B stays gated on calendar time.")
    print(f"Requests used: {probe.request_count}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
