"""
One-time backfill: recompute market/sector ETF-based alpha for existing scanner_event_outcomes rows.

Replaces the old equal-weight tracked-universe benchmark with real ETF benchmarks (SPY for market
alpha, the mapped Sector SPDR/SMH/IGV ETF for sector alpha) for rows that were evaluated before
migrations/014_scanner_etf_benchmark.sql. Only benchmark_return, alpha_return, net_alpha_return,
market_benchmark_ticker, sector_benchmark_ticker, sector_benchmark_return, sector_alpha_return, and
sector_net_alpha_return are touched — raw_return/signed_return/net_signed_return/mae/mfe/first_hit
are pure price-action and untouched. See docs/SCANNER_ENHANCEMENTS_BACKLOG.md items 4/6/7/8/9.

Usage:
    python scripts/backfill_scanner_benchmark.py --interval 1d
    python scripts/backfill_scanner_benchmark.py --interval 1d --dry-run --batch-size 500
"""
import argparse
import logging
import sys
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import pandas as pd  # noqa: E402

from database import get_db_cursor  # noqa: E402
from research.gics_sectors import (
    ALTERNATE_MARKET_ETF, BROAD_MARKET_ETF, resolve_benchmark_ticker, sector_benchmark_ticker,
)  # noqa: E402
from research.scanner_events import _batched_forward_bars, _etf_forward_return  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("backfill-scanner-benchmark")


def _event_batches(interval: str, batch_size: int) -> list[list[dict]]:
    """All events for `interval` that have at least one outcome row, chunked by event."""
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT DISTINCT e.event_id, e.ticker, e.direction, e.signal_time
            FROM scanner_events e
            JOIN scanner_event_outcomes o ON o.event_id = e.event_id
            WHERE e.interval = %s
            ORDER BY e.event_id
        """, (interval,))
        events = [dict(row) for row in cur.fetchall()]
    return [events[i:i + batch_size] for i in range(0, len(events), batch_size)]


def _sector_map(tickers: list[str]) -> dict[str, str | None]:
    with get_db_cursor() as cur:
        cur.execute(
            "SELECT ticker, sector FROM selected_tickers WHERE ticker = ANY(%s)",
            (tickers,),
        )
        return {row["ticker"]: row["sector"] for row in cur.fetchall()}


def backfill_batch(interval: str, events: list[dict], dry_run: bool) -> int:
    event_ids = [event["event_id"] for event in events]
    tickers = sorted({str(event["ticker"]) for event in events})
    sector_by_ticker = _sector_map(tickers)
    market_ticker_by_ticker = {
        ticker: resolve_benchmark_ticker(ticker, BROAD_MARKET_ETF) for ticker in tickers
    }
    sector_ticker_by_ticker = {
        ticker: resolve_benchmark_ticker(ticker, sector_benchmark_ticker(sector_by_ticker.get(ticker)))
        for ticker in tickers
    }
    etf_tickers_needed = sorted({
        BROAD_MARKET_ETF, ALTERNATE_MARKET_ETF,
        *market_ticker_by_ticker.values(), *sector_ticker_by_ticker.values(),
    })
    earliest_signal = min(event["signal_time"] for event in events)
    etf_bars = _batched_forward_bars(
        interval, [{"ticker": t, "signal_time": earliest_signal} for t in etf_tickers_needed]
    )

    with get_db_cursor() as cur:
        cur.execute("""
            SELECT outcome_id, event_id, horizon_bars, signed_return, net_signed_return
            FROM scanner_event_outcomes WHERE event_id = ANY(%s)
        """, (event_ids,))
        outcomes = [dict(row) for row in cur.fetchall()]

    events_by_id = {event["event_id"]: event for event in events}
    updates = []
    for outcome in outcomes:
        event = events_by_id.get(outcome["event_id"])
        if event is None:
            continue
        direction = int(event["direction"])
        signal_time = pd.Timestamp(event["signal_time"])
        horizon = int(outcome["horizon_bars"])
        signed_return = float(outcome["signed_return"])
        # cost is embedded in the already-stored, benchmark-independent net_signed_return.
        cost = signed_return - float(outcome["net_signed_return"])

        benchmark = _etf_forward_return(etf_bars.get(market_ticker_by_ticker.get(str(event["ticker"]), BROAD_MARKET_ETF)), signal_time, horizon)
        alpha = signed_return - direction * benchmark if benchmark is not None else None
        net_alpha = alpha - cost if alpha is not None else None

        sector_ticker = sector_ticker_by_ticker.get(str(event["ticker"]), BROAD_MARKET_ETF)
        sector_benchmark = _etf_forward_return(etf_bars.get(sector_ticker), signal_time, horizon)
        sector_alpha = signed_return - direction * sector_benchmark if sector_benchmark is not None else None
        sector_net_alpha = sector_alpha - cost if sector_alpha is not None else None

        updates.append((
            benchmark, alpha, net_alpha, market_ticker_by_ticker.get(str(event["ticker"]), BROAD_MARKET_ETF),
            sector_ticker, sector_benchmark, sector_alpha, sector_net_alpha,
            outcome["outcome_id"],
        ))

    if not dry_run and updates:
        with get_db_cursor() as cur:
            cur.executemany("""
                UPDATE scanner_event_outcomes SET
                    benchmark_return = %s, alpha_return = %s, net_alpha_return = %s,
                    market_benchmark_ticker = %s, sector_benchmark_ticker = %s,
                    sector_benchmark_return = %s, sector_alpha_return = %s,
                    sector_net_alpha_return = %s
                WHERE outcome_id = %s
            """, updates)
    return len(updates)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill ETF-based scanner alpha")
    parser.add_argument("--interval", required=True, choices=["1d", "1wk", "1h"])
    parser.add_argument("--batch-size", type=int, default=1000, help="Events per batch")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    batches = _event_batches(args.interval, args.batch_size)
    total_events = sum(len(batch) for batch in batches)
    logger.info("Backfilling %s | %s events in %s batches", args.interval, total_events, len(batches))

    total_updated = 0
    for index, batch in enumerate(batches, start=1):
        updated = backfill_batch(args.interval, batch, args.dry_run)
        total_updated += updated
        logger.info("Batch %s/%s | events=%s | outcomes updated=%s", index, len(batches), len(batch), updated)

    logger.info("Done | interval=%s | outcome rows %s=%s", args.interval,
                "would update" if args.dry_run else "updated", total_updated)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
