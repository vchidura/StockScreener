#!/usr/bin/env python3
"""Measure whether a per-session at-the-money implied volatility can be reconstructed.

Read-only, no database writes. This is the gate on the historical implied-volatility
backfill. `probe_option_aggregate_entitlement.py` established that daily aggregates are
entitled for roughly four years, but those aggregates are trade-based: a session with no
print produces no bar, and near-the-money per-contract coverage was only about 69%.

Per-contract coverage is the wrong number to size a backfill against. What matters is
whether, on each session, AT LEAST ONE near-the-money contract printed, because a single
strike is enough to solve one implied volatility and a few bracketing strikes are enough
to interpolate an at-the-money value. That union across strikes is what this measures.

For each monthly expiration in the window the script takes the strikes nearest this
platform's own underlying close, pulls their daily bars, and reports the fraction of
sessions on which at least one, and at least three, of them traded.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
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
from options.calendar import OptionExchangeCalendar  # noqa: E402
from options.config import load_option_runtime_configuration  # noqa: E402

BASE_URL = "https://api.polygon.io"
DEFAULT_OUTPUT = BACKEND_DIR.parent / "docs" / "option_session_iv_coverage.json"

# Three bracketing strikes is the practical minimum for interpolating an ATM value
# rather than reading a single off-the-money contract and calling it the money.
INTERPOLATION_STRIKE_COUNT = 3

SQL_SESSIONS = """
    SELECT DISTINCT ON (session_date) session_date, close_price
    FROM equity_bar_revisions
    WHERE interval = '1d' AND is_final AND session_scope = 'RTH' AND adjusted = false
      AND ticker = %s AND session_date BETWEEN %s AND %s
    ORDER BY session_date, created_at DESC
"""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--underlyers", default="SPY", help="Comma-separated.")
    parser.add_argument("--months", type=int, default=24, help="Monthly expirations to sample.")
    parser.add_argument("--strikes-per-expiration", type=int, default=5)
    parser.add_argument("--window-days", type=int, default=60, help="Sessions before expiry to score.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser.parse_args()


def _third_friday(year: int, month: int) -> date:
    return OptionExchangeCalendar.third_friday(year, month)


def _monthly_expirations(count: int, latest: date) -> list[date]:
    return OptionExchangeCalendar.monthly_expirations(count, latest)


class PolygonProbe:
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
            return response.status_code, {"raw": response.text[:400]}


def _sessions(ticker: str, start: date, end: date) -> list[tuple[date, float]]:
    with get_db_cursor() as cursor:
        cursor.execute(SQL_SESSIONS, (ticker, start, end))
        return [(row["session_date"], float(row["close_price"])) for row in cursor.fetchall()]


def _contracts_near_money(
    probe: PolygonProbe, underlyer: str, expiration: date, spot: float, limit: int
) -> list[dict[str, Any]]:
    status, payload = probe.get(
        "/v3/reference/options/contracts",
        {
            "underlying_ticker": underlyer,
            "expiration_date": expiration.isoformat(),
            "expired": "true",
            "contract_type": "call",
            "strike_price.gte": f"{spot * 0.96:.2f}",
            "strike_price.lte": f"{spot * 1.04:.2f}",
            "limit": "100",
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


def _bar_sessions(probe: PolygonProbe, contract: str, start: date, end: date) -> set[date]:
    status, payload = probe.get(
        f"/v2/aggs/ticker/{contract}/range/1/day/{start.isoformat()}/{end.isoformat()}",
        {"adjusted": "true", "sort": "asc", "limit": "5000"},
    )
    results = payload.get("results") if isinstance(payload, dict) else None
    if status != 200 or not isinstance(results, list):
        return set()
    # Daily bars are stamped at the session open in UTC; converting in local time would
    # shift the calendar date and silently misalign every session west of Greenwich.
    return {
        datetime.fromtimestamp(row["t"] / 1000, tz=timezone.utc).date() for row in results
    }


def _measure(probe: PolygonProbe, underlyer: str, args: argparse.Namespace) -> dict[str, Any]:
    expirations = _monthly_expirations(args.months, date.today())
    # Keyed by session then expiration: monthly windows overlap, so a global tally would
    # count the same session twice and make the bracketing test meaningless.
    per_session: dict[date, dict[date, int]] = defaultdict(lambda: defaultdict(int))
    scored_sessions: set[date] = set()
    contracts_probed = 0
    expirations_used = 0

    for expiration in expirations:
        window_start = expiration - timedelta(days=args.window_days)
        sessions = _sessions(underlyer, window_start, expiration)
        if not sessions:
            continue
        # Anchor strike selection to the middle of the window, not to expiry, so the
        # sample is not chosen with hindsight about where the underlying finished.
        anchor_spot = sessions[len(sessions) // 2][1]
        contracts = _contracts_near_money(
            probe, underlyer, expiration, anchor_spot, args.strikes_per_expiration
        )
        if not contracts:
            continue
        expirations_used += 1
        session_dates = {session for session, _ in sessions}
        scored_sessions |= session_dates
        for contract in contracts:
            contracts_probed += 1
            traded = _bar_sessions(probe, contract["ticker"], window_start, expiration)
            for session in traded & session_dates:
                per_session[session][expiration] += 1

    if not scored_sessions:
        return {"scored_sessions": 0}

    best_per_session = {
        session: max(per_session[session].values(), default=0) for session in scored_sessions
    }
    with_any = sum(1 for count in best_per_session.values() if count >= 1)
    with_bracket = sum(
        1 for count in best_per_session.values() if count >= INTERPOLATION_STRIKE_COUNT
    )
    return {
        "expirations_used": expirations_used,
        "contracts_probed": contracts_probed,
        "strikes_per_expiration": args.strikes_per_expiration,
        "scored_sessions": len(scored_sessions),
        "sessions_with_any_print": with_any,
        "sessions_with_bracketing_strikes": with_bracket,
        "single_strike_coverage": with_any / len(scored_sessions),
        "interpolation_coverage": with_bracket / len(scored_sessions),
        "mean_strikes_printing_per_session": sum(best_per_session.values()) / len(best_per_session),
        "uncovered_sessions": sorted(
            session.isoformat() for session, count in best_per_session.items() if count == 0
        )[:20],
        "first_session": min(scored_sessions).isoformat(),
        "last_session": max(scored_sessions).isoformat(),
    }


def main() -> int:
    args = _parse_args()
    configuration = load_option_runtime_configuration()
    api_key = configuration.settings.polygon_api_key.get_secret_value()
    if not api_key:
        print("POLYGON_API_KEY is not configured.")
        return 1

    probe = PolygonProbe(api_key)
    underlyers = tuple(value.strip().upper() for value in args.underlyers.split(",") if value.strip())
    report: dict[str, Any] = {
        "measured_on": date.today().isoformat(),
        "months_sampled": args.months,
        "window_days": args.window_days,
        "underlyers": {},
    }

    for underlyer in underlyers:
        print(f"Measuring session-level ATM coverage for {underlyer} ...")
        measurement = _measure(probe, underlyer, args)
        report["underlyers"][underlyer] = measurement
        if not measurement.get("scored_sessions"):
            print("  no sessions scored")
            continue
        print(
            f"  {measurement['scored_sessions']} sessions"
            f" ({measurement['first_session']} .. {measurement['last_session']})"
            f" from {measurement['expirations_used']} expirations,"
            f" {measurement['contracts_probed']} contracts"
        )
        print(
            f"  at least one strike printed : {measurement['single_strike_coverage']:.1%}"
            f"  ({measurement['sessions_with_any_print']}/{measurement['scored_sessions']})"
        )
        print(
            f"  at least {INTERPOLATION_STRIKE_COUNT} strikes printed:"
            f" {measurement['interpolation_coverage']:.1%}"
            f"  ({measurement['sessions_with_bracketing_strikes']}/{measurement['scored_sessions']})"
        )
        print(
            f"  mean strikes printing per session: "
            f"{measurement['mean_strikes_printing_per_session']:.2f}"
            f" of {measurement['strikes_per_expiration']}"
        )

    report["request_count"] = probe.request_count
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(f"\nRequests used: {probe.request_count}")
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
