#!/usr/bin/env python3
"""Replay predeclared intraday scanners over canonical 30m bars into research events.

Fixed-cohort exploratory research only. The bootstrapped 30m history covers the
current universe membership, so results carry survivorship and selection bias and
never establish historical-universe performance.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pandas as pd
from dotenv import load_dotenv


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
load_dotenv(BACKEND_DIR / ".env")

from equity.domain import DecisionWatermark
from equity.intraday_research import build_intraday_event
from equity.repositories import EquityBarRepository, EquityUniverseRepository
from research.gics_sectors import (
    BROAD_MARKET_ETF,
    resolve_benchmark_ticker,
    sector_benchmark_ticker,
)
from research.intraday_scanners import (
    INTRADAY_DETECTOR_POLICY,
    INTRADAY_SCANNER_REGISTRY,
    INTRADAY_SCANNER_VERSIONS,
    BenchmarkContext,
    benchmark_context,
    build_intraday_scanner_events,
)


DEFAULT_INTERVAL = "30m"
MAXIMUM_BARS_PER_TICKER = 60000


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--output", type=Path, required=True)
    result.add_argument("--summary", type=Path)
    result.add_argument("--start", help="First session YYYY-MM-DD")
    result.add_argument("--end", help="Last session YYYY-MM-DD")
    result.add_argument("--interval", default=DEFAULT_INTERVAL, choices=(DEFAULT_INTERVAL,))
    result.add_argument(
        "--scanner",
        action="append",
        dest="scanners",
        choices=sorted(INTRADAY_SCANNER_REGISTRY),
        help="Repeatable; defaults to every registered intraday scanner",
    )
    result.add_argument("--ticker", action="append", dest="tickers")
    result.add_argument("--limit-tickers", type=int)
    result.add_argument("--progress-every", type=int, default=25)
    return result


def _frame(bars) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "bar_revision_id": bar.bar_revision_id,
            "session_date": bar.session_date,
            "bar_start": bar.bar_start,
            "bar_end": bar.bar_end,
            "open": float(bar.open_price),
            "high": float(bar.high_price),
            "low": float(bar.low_price),
            "close": float(bar.close_price),
            "volume": float(bar.volume),
            "vwap": float(bar.vwap) if bar.vwap is not None else None,
        }
        for bar in bars
    ])


def _load_frames(
    repository: EquityBarRepository,
    tickers: list[str],
    *,
    interval: str,
    after: datetime,
    available_by: datetime,
) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    for ticker in tickers:
        bars = repository.list_final_after(
            ticker,
            interval,
            after=after,
            available_by=available_by,
            limit=MAXIMUM_BARS_PER_TICKER,
        )
        frames[ticker] = _frame(bars)
    return frames


def main(argv: list[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    scanners = tuple(arguments.scanners or sorted(INTRADAY_SCANNER_REGISTRY))
    available_by = datetime.now(timezone.utc)
    start_date = (
        date.fromisoformat(arguments.start) if arguments.start
        else date(1970, 1, 1)
    )
    end_date = date.fromisoformat(arguments.end) if arguments.end else None
    after = datetime.combine(start_date, time.min, tzinfo=timezone.utc) - timedelta(seconds=1)

    universe_repository = EquityUniverseRepository()
    universe = universe_repository.get_latest_as_of(
        DecisionWatermark(market_time=available_by, observed_time=available_by)
    )
    if universe is None:
        raise SystemExit("no live universe run is available")
    universe_run_id = UUID(str(universe["universe_run_id"]))
    universe_policy_version = str(universe["policy_version"])
    securities = universe_repository.members_for_replay(universe_run_id)
    if arguments.tickers:
        requested = {value.upper() for value in arguments.tickers}
        securities = tuple(row for row in securities if row.ticker in requested)
    if arguments.limit_tickers:
        securities = securities[:arguments.limit_tickers]
    if not securities:
        raise SystemExit("universe cohort resolved to zero securities")

    bar_repository = EquityBarRepository()
    benchmark_tickers = sorted({BROAD_MARKET_ETF} | {
        sector_benchmark_ticker(security.sector) for security in securities
    })
    benchmark_frames = _load_frames(
        bar_repository, benchmark_tickers,
        interval=arguments.interval, after=after, available_by=available_by,
    )
    benchmarks: dict[str, BenchmarkContext] = {
        ticker: benchmark_context(ticker, frame)
        for ticker, frame in benchmark_frames.items()
        if not frame.empty
    }
    missing_benchmarks = sorted(set(benchmark_tickers) - set(benchmarks))

    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    direction_counts: dict[str, int] = {}
    diagnostics: dict[str, int] = {}
    skipped_tickers: list[str] = []
    total_events = 0
    total_bars = 0
    first_signal = None
    last_signal = None

    with arguments.output.open("w", encoding="utf-8", newline="\n") as handle:
        for position, security in enumerate(securities, 1):
            frame = _load_frames(
                bar_repository, [security.ticker],
                interval=arguments.interval, after=after, available_by=available_by,
            )[security.ticker]
            if end_date is not None and not frame.empty:
                frame = frame[frame["session_date"] <= end_date]
            if frame.empty:
                skipped_tickers.append(security.ticker)
                continue
            total_bars += len(frame)
            market_ticker = resolve_benchmark_ticker(security.ticker, BROAD_MARKET_ETF)
            sector_ticker = resolve_benchmark_ticker(
                security.ticker, sector_benchmark_ticker(security.sector)
            )
            events, ticker_diagnostics = build_intraday_scanner_events(
                security.ticker,
                frame,
                market=benchmarks.get(market_ticker),
                sector=benchmarks.get(sector_ticker),
                scanners=scanners,
            )
            for key, value in ticker_diagnostics.items():
                diagnostics[key] = diagnostics.get(key, 0) + value
            for detection in events:
                event = build_intraday_event(
                    detection=detection,
                    universe_run_id=universe_run_id,
                    universe_policy_version=universe_policy_version,
                    interval=arguments.interval,
                )
                handle.write(json.dumps(event.as_dict(), sort_keys=True) + "\n")
                total_events += 1
                counts[event.source_name] = counts.get(event.source_name, 0) + 1
                lane = f"{event.source_name}:{'LONG' if event.direction == 1 else 'SHORT'}"
                direction_counts[lane] = direction_counts.get(lane, 0) + 1
                first_signal = min(first_signal or event.signal_time, event.signal_time)
                last_signal = max(last_signal or event.signal_time, event.signal_time)
            if arguments.progress_every and position % arguments.progress_every == 0:
                print(
                    f"processed {position}/{len(securities)} tickers, "
                    f"{total_events} events",
                    flush=True,
                )

    summary = {
        "available_by": available_by.isoformat(),
        "bars_scanned": total_bars,
        "detector_policy": dict(INTRADAY_DETECTOR_POLICY),
        "diagnostics": dict(sorted(diagnostics.items())),
        "direction_counts": dict(sorted(direction_counts.items())),
        "event_counts": dict(sorted(counts.items())),
        "events": total_events,
        "first_signal_time": first_signal.isoformat() if first_signal else None,
        "interval": arguments.interval,
        "last_signal_time": last_signal.isoformat() if last_signal else None,
        "limitations": [
            "FIXED_COHORT_EXPLORATORY",
            "SURVIVORSHIP_BIASED_UNIVERSE",
            "NO_POINT_IN_TIME_INTRADAY_MEMBERSHIP",
        ],
        "missing_benchmark_bars": missing_benchmarks,
        "output": str(arguments.output),
        "scanners": {name: INTRADAY_SCANNER_VERSIONS[name] for name in scanners},
        "securities": len(securities),
        "skipped_tickers_without_bars": skipped_tickers,
        "universe_policy_version": universe_policy_version,
        "universe_run_id": str(universe_run_id),
    }
    rendered = json.dumps(summary, indent=2, sort_keys=True)
    if arguments.summary:
        arguments.summary.parent.mkdir(parents=True, exist_ok=True)
        arguments.summary.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
