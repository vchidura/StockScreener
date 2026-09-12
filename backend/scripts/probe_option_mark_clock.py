#!/usr/bin/env python3
"""Probe Polygon to establish what clock each option mark timestamp belongs to.

Read-only: no database writes, no persistence. Answers one question — is
`day.last_updated` (used as `mark_market_data_time` during normalization) a market-event
timestamp or a publication timestamp, and how does it relate to the underlying minute
bar clock?

Run during market hours for the decisive answer. Outside market hours the fields still
reveal their relationship to each other, but the delay behaviour is not exercised.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

load_dotenv(BACKEND_DIR / ".env")

BASE_URL = "https://api.polygon.io"
UTC = timezone.utc


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--underlyer", default="SPY")
    parser.add_argument("--contracts", type=int, default=25)
    parser.add_argument("--output", type=Path, default=None)
    return parser.parse_args()


def _epoch(value: Any) -> datetime | None:
    """Polygon mixes nanosecond and millisecond epochs; match the normalizer's rule."""
    if value in (None, 0):
        return None
    number = int(value)
    seconds = number / 1e9 if number > 1e15 else number / 1e3
    return datetime.fromtimestamp(seconds, tz=UTC)


def _get(session: requests.Session, path: str, params: dict[str, str]) -> dict[str, Any]:
    response = session.get(f"{BASE_URL}{path}", params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def main() -> int:
    args = _parse_args()
    api_key = os.getenv("POLYGON_API_KEY", "").strip()
    if not api_key:
        print("POLYGON_API_KEY is not configured", file=sys.stderr)
        return 2

    session = requests.Session()
    session.headers["Authorization"] = f"Bearer {api_key}"
    probed_at = datetime.now(UTC)

    chain = _get(
        session,
        f"/v3/snapshot/options/{args.underlyer}",
        {"limit": str(args.contracts), "order": "asc", "sort": "ticker"},
    )
    results = chain.get("results") or []
    if not results:
        print("no chain results returned", file=sys.stderr)
        return 1

    # Latest underlying minute bar, the same clock normalization aligns against.
    # The window spans several days so the probe still resolves a bar over a weekend.
    aggregates = _get(
        session,
        f"/v2/aggs/ticker/{args.underlyer}/range/1/minute/"
        f"{(probed_at - timedelta(days=7)).date()}/{probed_at.date()}",
        {"adjusted": "true", "sort": "desc", "limit": "1"},
    )
    bar = (aggregates.get("results") or [{}])[0]
    latest_bar_time = _epoch(bar.get("t"))

    rows: list[dict[str, Any]] = []
    for item in results:
        day = item.get("day") or {}
        last_trade = item.get("last_trade") or {}
        underlying = item.get("underlying_asset") or {}
        day_updated = _epoch(day.get("last_updated"))
        trade_time = _epoch(last_trade.get("sip_timestamp"))
        rows.append(
            {
                "ticker": (item.get("details") or {}).get("ticker"),
                "day_volume": day.get("volume"),
                "day_close": day.get("close"),
                "day_last_updated": day_updated,
                "last_trade_sip": trade_time,
                "underlying_last_updated": _epoch(underlying.get("last_updated")),
                "underlying_timeframe": underlying.get("timeframe"),
                "day_timeframe": day.get("timeframe"),
            }
        )

    print(f"probed_at                 {probed_at.isoformat()}")
    print(f"underlyer                 {args.underlyer}")
    print(f"latest underlying bar     {latest_bar_time}")
    if latest_bar_time:
        print(
            f"bar age at probe          "
            f"{(probed_at - latest_bar_time).total_seconds():.1f}s"
        )
    print(f"contracts returned        {len(rows)}\n")
    trade_times = [row["last_trade_sip"] for row in rows if row["last_trade_sip"]]
    print(f"contracts with SIP time   {len(trade_times)}")
    if trade_times:
        newest_trade = max(trade_times)
        print(f"oldest last trade SIP     {min(trade_times)}")
        print(f"newest last trade SIP     {newest_trade}")
        print(
            f"newest trade age          "
            f"{(probed_at - newest_trade).total_seconds():.1f}s\n"
        )

    header = (
        f"{'ticker':<24}{'vol':>8}  {'day.last_updated':<26}"
        f"{'last_trade.sip':<26}{'updated-trade':>14}"
    )
    print(header)
    print("-" * len(header))
    deltas: list[float] = []
    distinct_updated: set[datetime] = set()
    for row in rows[:15]:
        delta = (
            (row["day_last_updated"] - row["last_trade_sip"]).total_seconds()
            if row["day_last_updated"] and row["last_trade_sip"]
            else None
        )
        if delta is not None:
            deltas.append(delta)
        if row["day_last_updated"]:
            distinct_updated.add(row["day_last_updated"])
        print(
            f"{str(row['ticker']):<24}{str(row['day_volume']):>8}  "
            f"{str(row['day_last_updated']):<26}{str(row['last_trade_sip']):<26}"
            f"{'' if delta is None else f'{delta:>13.1f}s'}"
        )

    for row in rows:
        if row["day_last_updated"]:
            distinct_updated.add(row["day_last_updated"])

    print("\n--- interpretation signals ---")
    print(f"day.timeframe             {rows[0]['day_timeframe']}")
    print(f"underlying.timeframe      {rows[0]['underlying_timeframe']}")
    print(f"underlying.last_updated   {rows[0]['underlying_last_updated']}")
    print(
        f"distinct day.last_updated {len(distinct_updated)} across {len(rows)} contracts"
    )
    if deltas:
        print(
            f"day.last_updated - last_trade.sip: median "
            f"{statistics.median(deltas):.1f}s, min {min(deltas):.1f}s, "
            f"max {max(deltas):.1f}s"
        )
    if latest_bar_time:
        gaps = [
            (row["day_last_updated"] - latest_bar_time).total_seconds()
            for row in rows
            if row["day_last_updated"]
        ]
        if gaps:
            print(
                f"day.last_updated - latest bar:     median "
                f"{statistics.median(gaps):.1f}s, min {min(gaps):.1f}s, "
                f"max {max(gaps):.1f}s"
            )
    print(
        "\nA publication clock shows few distinct day.last_updated values clustered near "
        "the probe time and diverging from last_trade.sip.\nA market-event clock shows "
        "day.last_updated tracking last_trade.sip per contract."
    )

    if args.output:
        args.output.write_text(
            json.dumps(
                {
                    "probed_at": probed_at,
                    "underlyer": args.underlyer,
                    "latest_underlying_bar": latest_bar_time,
                    "contracts": rows,
                },
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
