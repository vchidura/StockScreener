#!/usr/bin/env python3
"""Ingest a split-adjusted daily bar lineage from Polygon grouped daily.

Canonical daily bars are DERIVED from unadjusted 30m, so splits propagate as
false gaps into every higher interval. This writes an independent
`adjusted = true` daily lineage. `equity_canonical_bars` pins `adjusted = false`,
so the UI and options paths are unaffected; research reads it explicitly via
`list_final_after(..., adjusted=True)`.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import exchange_calendars
import pandas as pd
import requests
from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from equity.domain import BarAvailabilityMode, DecisionWatermark
from equity.polygon import PolygonEquityClient, normalize_grouped_daily_bars
from equity.repositories import (
    EquityBarRepository,
    EquityReferenceRepository,
    EquityUniverseRepository,
)
from research.gics_sectors import (
    ALTERNATE_MARKET_ETF,
    BROAD_MARKET_ETF,
    SECTOR_BENCHMARK_ETF,
)

# Every outcome needs a market and a sector leg, so these must exist in the same
# lineage as the subjects even though they are never study subjects themselves.
BENCHMARK_TICKERS = frozenset({
    BROAD_MARKET_ETF, ALTERNATE_MARKET_ETF, *SECTOR_BENCHMARK_ETF.values(),
})

# equity_bar_revisions stores prices and volume as numeric(20,8).
NUMERIC_LIMIT = Decimal(10) ** 12
PRICE_FIELDS = ("open_price", "high_price", "low_price", "close_price", "volume", "vwap")


def storable(bar) -> bool:
    """Cumulative reverse splits can push back-adjusted prices past the column range.

    Mullen Automotive reaches ~1.7e13 per share in 2021 after roughly 1:1e12 of
    cumulative reverse splits. The series is arithmetically fine - returns are
    ratios - but it cannot be stored, so the affected bars are skipped and
    reported rather than silently rounded.
    """
    for field in PRICE_FIELDS:
        value = getattr(bar, field, None)
        if value is not None and abs(Decimal(value)) >= NUMERIC_LIMIT:
            return False
    return True


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--start", required=True, help="YYYY-MM-DD")
    result.add_argument("--end", required=True, help="YYYY-MM-DD")
    result.add_argument("--calendar", default="XNYS")
    result.add_argument(
        "--include-non-common", action="store_true",
        help="Keep ETFs and ETVs; excluded by default because their benchmarks "
             "are self-referential",
    )
    result.add_argument("--limit-sessions", type=int)
    result.add_argument(
        "--from-reconstructed-universes", action="store_true",
        help="Cover every member of every reconstructed universe run instead of "
             "the latest live universe. Required before a point-in-time study, "
             "whose cohort is far wider than the live universe.",
    )
    result.add_argument(
        "--policy-version", default="liquid_us_common_stocks_v2",
        help="Universe policy to draw members from with "
             "--from-reconstructed-universes",
    )
    result.add_argument("--apply", action="store_true")
    result.add_argument("--output", type=Path)
    return result


def reconstructed_union(policy_version: str) -> tuple[str, ...]:
    from database import get_db_cursor

    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT member.ticker
            FROM equity_universe_members member
            JOIN equity_universe_runs run
              ON run.universe_run_id = member.universe_run_id
            WHERE run.availability_mode = 'HISTORICAL_RECONSTRUCTED'
              AND run.policy_version = %s
            ORDER BY member.ticker
            """,
            (policy_version,),
        )
        return tuple(row["ticker"] for row in cursor.fetchall())


def with_backoff(operation, *, attempts: int = 6):
    delay = 2.0
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except requests.HTTPError as error:
            status = getattr(error.response, "status_code", None)
            if status not in (429, 502, 503, 504) or attempt == attempts:
                raise
            print(f"    polygon {status}; retrying in {delay:.0f}s", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 60.0)
    raise RuntimeError("unreachable")


def main() -> int:
    arguments = parser().parse_args()
    start = date.fromisoformat(arguments.start)
    end = date.fromisoformat(arguments.end)
    observed_at = datetime.now(timezone.utc)
    watermark = DecisionWatermark(observed_at, observed_at)

    universe_repository = EquityUniverseRepository()
    if arguments.from_reconstructed_universes:
        tickers = reconstructed_union(arguments.policy_version)
        if not tickers:
            raise SystemExit(
                "no reconstructed universe members found for "
                f"{arguments.policy_version}; run "
                "prepare_historical_signal_research.py --persist first"
            )
    else:
        universe = universe_repository.get_latest_as_of(watermark)
        if universe is None:
            raise SystemExit("no live universe run is available")
        tickers = universe_repository.member_tickers(universe["universe_run_id"])
    securities = EquityReferenceRepository().list_securities_as_of(
        tuple(sorted(set(tickers) | set(BENCHMARK_TICKERS))), watermark
    )
    if not arguments.include_non_common:
        # Benchmarks are ETFs, so the common-stock filter would drop them and
        # leave every alpha column null.
        securities = [
            row for row in securities
            if row.security_type == "CS" or row.ticker in BENCHMARK_TICKERS
        ]
    security_ids = {row.ticker: row.security_id for row in securities}
    missing_benchmarks = sorted(BENCHMARK_TICKERS - set(security_ids))
    if missing_benchmarks:
        raise SystemExit(
            "no security reference for benchmark tickers: "
            f"{', '.join(missing_benchmarks)}; alpha cannot be computed without them"
        )

    calendar = exchange_calendars.get_calendar(arguments.calendar)
    sessions = [
        pd.Timestamp(value).date()
        for value in calendar.sessions_in_range(pd.Timestamp(start), pd.Timestamp(end))
    ]
    if arguments.limit_sessions:
        sessions = sessions[:arguments.limit_sessions]

    report = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "sessions": len(sessions),
        "universe_members": len(tickers),
        "member_source": (
            "RECONSTRUCTED_UNIVERSES" if arguments.from_reconstructed_universes
            else "LATEST_LIVE_UNIVERSE"
        ),
        "securities_selected": len(security_ids),
        "common_stock_only": not arguments.include_non_common,
        "mode": "APPLY" if arguments.apply else "DRY_RUN",
    }
    if not arguments.apply:
        report["note"] = "nothing fetched or written; re-run with --apply"
        print(json.dumps(report, indent=2))
        return 0

    client = PolygonEquityClient()
    bar_repository = EquityBarRepository()
    normalized = inserted = empty_sessions = 0
    unentitled: list[str] = []
    out_of_range: dict[str, int] = {}
    for position, session_date in enumerate(sessions, 1):
        try:
            rows = with_backoff(
                lambda: client.fetch_grouped_daily(session_date, adjusted=True)
            )
        except requests.HTTPError as error:
            if getattr(error.response, "status_code", None) != 403:
                raise
            unentitled.append(session_date.isoformat())
            continue
        if not rows:
            empty_sessions += 1
            continue
        bars = normalize_grouped_daily_bars(
            rows,
            session_date=session_date,
            security_ids=security_ids,
            observed_at=observed_at,
            ingestion_segment_id=None,
            availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
            adjusted=True,
            calendar_name=arguments.calendar,
        )
        normalized += len(bars)
        storable_bars = []
        for row in bars:
            if storable(row):
                storable_bars.append(row)
            else:
                out_of_range[row.ticker] = out_of_range.get(row.ticker, 0) + 1
        inserted += bar_repository.persist(tuple(storable_bars))
        if position % 50 == 0:
            print(f"  {position}/{len(sessions)} sessions, {inserted} bars inserted",
                  flush=True)

    report.update({
        "bars_normalized": normalized,
        "bars_inserted": inserted,
        "sessions_without_rows": empty_sessions,
        "sessions_not_entitled": len(unentitled),
        "bars_out_of_range": sum(out_of_range.values()),
        "tickers_out_of_range": dict(sorted(out_of_range.items())),
        "first_entitled_session": (
            sessions[len(unentitled)].isoformat()
            if unentitled and len(unentitled) < len(sessions) else None
        ),
    })
    rendered = json.dumps(report, indent=2)
    if arguments.output:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
