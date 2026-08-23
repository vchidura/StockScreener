"""
Historical backfill from Polygon.io (massive.com) via the REST aggregates API.

Backfills stock_prices_daily, stock_prices_hourly, and stock_prices_intraday
using PolygonProvider. Requires POLYGON_API_KEY in backend/.env (Stocks
Starter plan: unlimited calls, 5 years history, 15-minute delayed data).

For very large one-time historical loads (multi-year, full universe), Polygon's
S3 Flat Files are more efficient than paginated REST calls — see
https://massive.com/docs/flat-files for the bulk CSV-over-S3 alternative.
This script uses the REST aggregates endpoint, which is simpler to operate
and sufficient for incremental/targeted backfills.

Usage:
    # Backfill 5 years of daily candles for all active tickers
    python scripts/backfill_polygon.py --dataset daily --days 1825

    # Backfill 60 days of hourly candles for specific tickers
    python scripts/backfill_polygon.py --dataset hourly --days 60 --tickers AAPL,MSFT

    # Backfill 5m intraday for the last 7 days
    python scripts/backfill_polygon.py --dataset intraday --days 7

    # Preview without writing to the database
    python scripts/backfill_polygon.py --dataset daily --days 30 --dry-run
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = BACKEND_DIR / "scripts"
for p in (str(BACKEND_DIR), str(SCRIPTS_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

from dotenv import load_dotenv
load_dotenv(BACKEND_DIR / ".env")

from database import get_selected_tickers
from providers.polygon_provider import PolygonProvider
from update_daily_prices import get_existing_dates, upsert_prices
from update_hourly_prices import upsert_hourly_prices
from update_intraday_prices import upsert_intraday

BATCH_SIZE = 25  # tickers processed (and progress-logged) per batch


def _batches(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def backfill_daily(provider: PolygonProvider, tickers: list[str], days: int, dry_run: bool) -> None:
    end_dt = datetime.now(timezone.utc) + timedelta(days=1)
    start_dt = end_dt - timedelta(days=days + 15)
    total_inserted = 0

    for batch_num, batch in enumerate(_batches(tickers, BATCH_SIZE), 1):
        print(f"[daily] Batch {batch_num} ({len(batch)} tickers)...")
        ticker_data = provider.fetch_daily(batch, days=days)
        for ticker in batch:
            df = ticker_data.get(ticker)
            if df is None or df.empty:
                continue
            if dry_run:
                print(f"  [{ticker}] would upsert {len(df)} daily bars")
                continue
            existing = get_existing_dates(
                ticker, start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")
            )
            inserted, _ = upsert_prices(ticker, df, existing)
            total_inserted += inserted
    print(f"[daily] Done — {total_inserted} rows inserted" if not dry_run else "[daily] Dry run complete")


def backfill_hourly(provider: PolygonProvider, tickers: list[str], days: int, dry_run: bool) -> None:
    total_inserted = 0
    for batch_num, batch in enumerate(_batches(tickers, BATCH_SIZE), 1):
        print(f"[hourly] Batch {batch_num} ({len(batch)} tickers)...")
        ticker_data = provider.fetch_hourly(batch, days=days)
        for ticker in batch:
            df = ticker_data.get(ticker)
            if df is None or df.empty:
                continue
            if dry_run:
                print(f"  [{ticker}] would upsert {len(df)} hourly bars")
                continue
            total_inserted += upsert_hourly_prices(ticker, df)
    print(f"[hourly] Done — {total_inserted} rows inserted" if not dry_run else "[hourly] Dry run complete")


def backfill_intraday(provider: PolygonProvider, tickers: list[str], days: int, dry_run: bool) -> None:
    total_inserted = 0
    for batch_num, batch in enumerate(_batches(tickers, BATCH_SIZE), 1):
        print(f"[intraday] Batch {batch_num} ({len(batch)} tickers)...")
        ticker_data = provider.fetch_intraday(batch, "5m", days=days)
        for ticker in batch:
            df = ticker_data.get(ticker)
            if df is None or df.empty:
                continue
            if dry_run:
                print(f"  [{ticker}] would upsert {len(df)} 5m bars")
                continue
            total_inserted += upsert_intraday(ticker, df, "5m")
    print(f"[intraday] Done — {total_inserted} rows inserted" if not dry_run else "[intraday] Dry run complete")


def main():
    parser = argparse.ArgumentParser(description="Backfill historical prices from Polygon.io")
    parser.add_argument("--dataset", choices=["daily", "hourly", "intraday", "all"], default="daily")
    parser.add_argument("--days", type=int, default=365, help="Days of history to backfill")
    parser.add_argument("--tickers", type=str, default=None, help="Comma-separated tickers")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing to the database")
    args = parser.parse_args()

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = get_selected_tickers(active_only=True)
        if not tickers:
            print("No active tickers in selected_tickers table.")
            sys.exit(1)

    provider = PolygonProvider()
    datasets = ["daily", "hourly", "intraday"] if args.dataset == "all" else [args.dataset]

    for dataset in datasets:
        if dataset == "daily":
            backfill_daily(provider, tickers, args.days, args.dry_run)
        elif dataset == "hourly":
            backfill_hourly(provider, tickers, args.days, args.dry_run)
        elif dataset == "intraday":
            backfill_intraday(provider, tickers, args.days, args.dry_run)


if __name__ == "__main__":
    main()
