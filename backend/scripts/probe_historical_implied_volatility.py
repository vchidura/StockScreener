#!/usr/bin/env python3
"""Reconstruct implied volatility from historical settlement closes, in memory.

Read-only: no database writes, no schema change, nothing to clean up afterwards. It
answers the one question the entitlement probes could not, and the one that decides
whether the implied-volatility backfill is worth a change to the contract catalog.

The question is whether a daily settlement close solves to a sensible implied volatility
at all. Settlement closes are stale, struck across a wide range, and drawn from whatever
traded that session rather than from a quote. If they do not converge, or the resulting
smile does not hold together, then persisting them would buy nothing and the whole
implied-versus-realized line of work stops here instead of after a migration.

Contract terms come from the reference endpoint rather than the catalog, because the
catalog only knows contracts the pipeline has seen live. Spot comes from this platform's
own equity bars, UNADJUSTED: strikes are nominal contract terms and pairing them with a
split-adjusted close would silently corrupt every moneyness and every solve.
"""
from __future__ import annotations

import argparse
import json
import statistics
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
from options.analytics.greeks import (  # noqa: E402
    OptionValuationInput,
    solve_local_greeks,
)
from options.calendar import OptionExchangeCalendar  # noqa: E402
from options.config import load_option_runtime_configuration  # noqa: E402
from options.domain import ContractType  # noqa: E402

BASE_URL = "https://api.polygon.io"
DEFAULT_OUTPUT = BACKEND_DIR.parent / "docs" / "option_historical_iv_probe.json"

SQL_SESSION_CLOSES = """
    SELECT DISTINCT ON (session_date) session_date, close_price
    FROM equity_bar_revisions
    WHERE interval = '1d' AND is_final AND session_scope = 'RTH' AND adjusted = false
      AND ticker = %s AND session_date BETWEEN %s AND %s
    ORDER BY session_date, created_at DESC
"""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--underlyers", default="SPY,NVDA")
    parser.add_argument("--months", type=int, default=6, help="Monthly expirations sampled.")
    parser.add_argument("--strikes", type=int, default=9, help="Strikes per expiration.")
    parser.add_argument("--moneyness-band", type=float, default=0.08)
    parser.add_argument("--window-days", type=int, default=45)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


class Probe:
    def __init__(self, api_key: str) -> None:
        self._session = requests.Session()
        self._api_key = api_key
        self.request_count = 0

    def get(self, path: str, params: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        self.request_count += 1
        response = self._session.get(
            f"{BASE_URL}{path}", params={**params, "apiKey": self._api_key}, timeout=30
        )
        try:
            return response.status_code, response.json()
        except ValueError:
            return response.status_code, {}


def _monthly_expirations(count: int, latest: date) -> list[date]:
    return OptionExchangeCalendar.monthly_expirations(count, latest)


def _session_closes(ticker: str, start: date, end: date) -> dict[date, float]:
    with get_db_cursor() as cursor:
        cursor.execute(SQL_SESSION_CLOSES, (ticker, start, end))
        return {row["session_date"]: float(row["close_price"]) for row in cursor.fetchall()}


def _contracts(probe: Probe, underlyer: str, expiration: date, spot: float, band: float, limit: int):
    status, payload = probe.get(
        "/v3/reference/options/contracts",
        {
            "underlying_ticker": underlyer,
            "expiration_date": expiration.isoformat(),
            "expired": "true",
            "strike_price.gte": f"{spot * (1 - band):.2f}",
            "strike_price.lte": f"{spot * (1 + band):.2f}",
            "limit": "250",
        },
    )
    results = payload.get("results") if isinstance(payload, dict) else None
    if status != 200 or not isinstance(results, list):
        return []
    ranked = sorted(
        (item for item in results if isinstance(item.get("strike_price"), (int, float))),
        key=lambda item: abs(float(item["strike_price"]) - spot),
    )
    return ranked[:limit]


def _bars(probe: Probe, contract: str, start: date, end: date) -> dict[date, float]:
    status, payload = probe.get(
        f"/v2/aggs/ticker/{contract}/range/1/day/{start.isoformat()}/{end.isoformat()}",
        {"adjusted": "true", "sort": "asc", "limit": "5000"},
    )
    results = payload.get("results") if isinstance(payload, dict) else None
    if status != 200 or not isinstance(results, list):
        return {}
    return {
        datetime.fromtimestamp(row["t"] / 1000, tz=timezone.utc).date(): float(row["c"])
        for row in results
        if isinstance(row.get("t"), int) and isinstance(row.get("c"), (int, float))
    }


def main() -> int:
    args = _parse_args()
    configuration = load_option_runtime_configuration()
    api_key = configuration.settings.polygon_api_key.get_secret_value()
    probe = Probe(api_key)
    calendar = OptionExchangeCalendar()
    rate = float(configuration.settings.risk_free_rate)
    dividend = float(configuration.settings.default_dividend_yield)

    report: dict[str, Any] = {
        "probed_on": date.today().isoformat(),
        "risk_free_rate": rate,
        "dividend_yield": dividend,
        "underlyers": {},
    }

    for underlyer in (v.strip().upper() for v in args.underlyers.split(",") if v.strip()):
        print(f"\n=== {underlyer} ===")
        solved: list[dict[str, Any]] = []
        attempted = 0
        no_bar = 0
        below_intrinsic = 0

        for expiration in _monthly_expirations(args.months, date.today()):
            window_start = expiration - timedelta(days=args.window_days)
            closes = _session_closes(underlyer, window_start, expiration)
            if not closes:
                continue
            anchor = closes[sorted(closes)[len(closes) // 2]]
            contracts = _contracts(
                probe, underlyer, expiration, anchor, args.moneyness_band, args.strikes
            )
            if not contracts:
                continue

            inputs: list[OptionValuationInput] = []
            meta: list[dict[str, Any]] = []
            for contract in contracts:
                strike = float(contract["strike_price"])
                is_call = str(contract.get("contract_type", "")).lower() == "call"
                marks = _bars(probe, contract["ticker"], window_start, expiration)
                if not marks:
                    no_bar += 1
                    continue
                for session, mark in marks.items():
                    spot = closes.get(session)
                    if spot is None or mark <= 0:
                        continue
                    attempted += 1
                    intrinsic = max(spot - strike, 0.0) if is_call else max(strike - spot, 0.0)
                    if mark < intrinsic:
                        below_intrinsic += 1
                        continue
                    years = max(
                        (calendar.expiration_cutoff(expiration)
                         - datetime.combine(session, datetime.min.time(), tzinfo=timezone.utc)
                         ).total_seconds() / (365.0 * 24 * 3600),
                        1e-6,
                    )
                    inputs.append(
                        OptionValuationInput(
                            contract_type=ContractType.CALL if is_call else ContractType.PUT,
                            spot=Decimal(str(spot)),
                            strike=Decimal(str(strike)),
                            model_mark=Decimal(str(mark)),
                            time_to_expiration_years=float(years),
                            risk_free_rate=rate,
                            dividend_yield=dividend,
                        )
                    )
                    meta.append(
                        {
                            "session": session,
                            "expiration": expiration,
                            "strike": strike,
                            "spot": spot,
                            "is_call": is_call,
                            "moneyness": strike / spot,
                            "dte": (expiration - session).days,
                        }
                    )

            if not inputs:
                continue
            for result, detail in zip(solve_local_greeks(tuple(inputs)), meta):
                solved.append(
                    {
                        **detail,
                        "converged": result.converged,
                        "iv": result.local_iv,
                        "failure": result.failure_reason.value if result.failure_reason else None,
                    }
                )

        converged = [row for row in solved if row["converged"] and row["iv"]]
        if not solved:
            print("  no solvable observations")
            continue

        atm = [row for row in converged if abs(row["moneyness"] - 1.0) <= 0.01]
        failures: dict[str, int] = defaultdict(int)
        for row in solved:
            if not row["converged"]:
                failures[row["failure"] or "UNKNOWN"] += 1

        by_bucket: dict[str, list[float]] = defaultdict(list)
        for row in converged:
            if row["moneyness"] < 0.97:
                by_bucket["below_0.97"].append(row["iv"])
            elif row["moneyness"] <= 1.03:
                by_bucket["atm_0.97_1.03"].append(row["iv"])
            else:
                by_bucket["above_1.03"].append(row["iv"])

        summary = {
            "attempted": attempted,
            "solved": len(solved),
            "converged": len(converged),
            "convergence_rate": len(converged) / len(solved),
            "contracts_with_no_bar": no_bar,
            "marks_below_intrinsic": below_intrinsic,
            "failures": dict(failures),
            "atm_iv_median": statistics.median([row["iv"] for row in atm]) if atm else None,
            "atm_observations": len(atm),
            "iv_by_moneyness_bucket": {
                bucket: {"count": len(values), "median_iv": statistics.median(values)}
                for bucket, values in sorted(by_bucket.items())
            },
            "iv_range": [min(row["iv"] for row in converged), max(row["iv"] for row in converged)]
            if converged
            else None,
        }
        report["underlyers"][underlyer] = summary

        print(f"  attempted {attempted}, solved {len(solved)}, converged {len(converged)}"
              f" ({summary['convergence_rate']:.1%})")
        print(f"  marks below intrinsic: {below_intrinsic}   contracts with no bar: {no_bar}")
        if summary["atm_iv_median"] is not None:
            print(f"  ATM IV median {summary['atm_iv_median']:.1%} from {len(atm)} observations")
        for bucket, stats in summary["iv_by_moneyness_bucket"].items():
            print(f"    {bucket:<16} n={stats['count']:>5}  median IV {stats['median_iv']:.1%}")
        if failures:
            print(f"  failures: {dict(failures)}")

    report["request_count"] = probe.request_count
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(f"\nRequests used: {probe.request_count}")
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
