"""Intraday price updater — fetch 5m/1m candles into stock_prices_intraday.

Used by run_scheduler.py (job_intraday_5m) every 5 min during market hours.
Also supports standalone backfill and continuous modes.

Usage:
    python scripts/update_intraday_prices.py                      # 5m, last 7 days
    python scripts/update_intraday_prices.py --interval 1m        # 1m candles
    python scripts/update_intraday_prices.py --days 3             # last 3 days
    python scripts/update_intraday_prices.py --tickers AAPL,MSFT  # specific tickers
    python scripts/update_intraday_prices.py --continuous         # 5m every 5 min during market hours
"""

import argparse
import math
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
import logging

import pandas as pd
import requests
import yfinance as yf

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor, get_selected_tickers, is_valid_ohlcv, VALID_INTRADAY_INTERVALS

BATCH_SIZE = 50
YAHOO_DIRECT_DELAY = 0.5  # seconds between Yahoo Chart API calls

# Canonical timezone — all intraday timestamps stored as US/Eastern
ET = ZoneInfo("America/New_York")

# Regular-session 5-min bars: 9:30 .. 15:55 ET = 78 bars per day
# Minute must be a multiple of the interval (5 -> 0,5,10,..,55)
REGULAR_SESSION_START = (9, 30)   # 9:30 AM ET
REGULAR_SESSION_END   = (15, 55)  # 3:55 PM ET (last 5-min bar)


def fetch_intraday_yahoo_direct(tickers: list[str], interval: str, days: int) -> dict[str, pd.DataFrame]:
    """Fetch intraday candles via Yahoo Chart API directly (no yfinance, no rate limit).

    Returns dict of ticker -> DataFrame with columns: open, high, low, close, volume.
    All validations: NaN/Inf rejection, future timestamp rejection, consecutive error breaker.
    """
    end_ts = int(time.time()) + 86400
    max_days = {"1m": 7, "5m": 60}
    request_days = min(days + 3, max_days.get(interval, days))
    start_ts = end_ts - 86400 * request_days
    now_et = datetime.now(ET)
    interval_minutes = int(interval.removesuffix("m"))
    result: dict[str, pd.DataFrame] = {}
    total = len(tickers)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    consecutive_errors = 0

    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i}/{total}] {ticker}...", end=" ", flush=True)

        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
            f"?period1={start_ts}&period2={end_ts}&interval={interval}"
        )

        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 429:
                print("rate-limited, waiting 30s...", end=" ", flush=True)
                time.sleep(30)
                resp = requests.get(url, headers=headers, timeout=15)

            if resp.status_code != 200:
                print(f"HTTP {resp.status_code}")
                consecutive_errors += 1
                if consecutive_errors >= 20:
                    print("\n  Too many consecutive errors. Stopping.")
                    break
                continue

            chart_result = resp.json().get("chart", {}).get("result", [None])[0]
            if not chart_result or "timestamp" not in chart_result:
                print("no data")
                continue

            timestamps = chart_result["timestamp"]
            quote = chart_result["indicators"]["quote"][0]

            rows = []
            rejected = 0
            for j, ts in enumerate(timestamps):
                o = quote["open"][j]
                h = quote["high"][j]
                l = quote["low"][j]
                c = quote["close"][j]
                v = quote["volume"][j]
                if any(x is None for x in (o, h, l, c)):
                    continue
                o, h, l, c = float(o), float(h), float(l), float(c)
                v = int(v) if v else 0
                # Reject NaN/Inf
                if any(math.isnan(x) or math.isinf(x) for x in (o, h, l, c)):
                    rejected += 1
                    continue
                # Convert UTC timestamp to ET
                dt_utc = datetime.utcfromtimestamp(ts).replace(tzinfo=ZoneInfo("UTC"))
                dt_et = dt_utc.astimezone(ET)
                # Reject future timestamps
                if dt_et > now_et:
                    rejected += 1
                    continue
                if not is_valid_intraday_bar(dt_et, interval_minutes):
                    rejected += 1
                    continue
                rows.append({
                    "datetime": dt_et,
                    "open": o, "high": h, "low": l, "close": c, "volume": v,
                })

            if rejected:
                print(f"({rejected} rejected)", end=" ")

            if rows:
                df = pd.DataFrame(rows).set_index("datetime").sort_index()
                result[ticker] = df
                print(f"{len(rows)} rows")
                consecutive_errors = 0
            else:
                print("no valid rows")

        except Exception as e:
            print(f"error: {e}")
            consecutive_errors += 1
            if consecutive_errors >= 20:
                print("\n  Too many consecutive errors. Stopping.")
                break

        if i < total:
            time.sleep(YAHOO_DIRECT_DELAY)

    return result


def fetch_intraday_yfinance(tickers: list[str], interval: str, days: int) -> dict[str, pd.DataFrame]:
    """Batch-download intraday candles from Yahoo Finance."""
    end = datetime.utcnow() + timedelta(days=1)
    start = end - timedelta(days=days + 2)
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    result: dict[str, pd.DataFrame] = {}

    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i : i + BATCH_SIZE]
        symbols = " ".join(batch)
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(tickers) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  Batch {batch_num}/{total_batches} ({len(batch)} tickers)...")

        # Brief pause between batches to avoid Yahoo throttling
        if i > 0:
            time.sleep(3)

        data = None
        for attempt in range(1, 4):
            try:
                data = yf.download(
                    symbols,
                    start=start_str,
                    end=end_str,
                    interval=interval,
                    auto_adjust=True,
                    progress=False,
                    threads=True,
                )
                if not data.empty:
                    break
                # Empty result may mean rate-limited silently
                print(f"    Empty result, retry {attempt}/3...")
            except Exception as e:
                print(f"    Attempt {attempt}/3 error: {e}")

            wait = 15 * attempt  # 15s, 30s, 45s
            print(f"    Waiting {wait}s before retry...")
            time.sleep(wait)

        if data is None or data.empty:
            print(f"    Batch {batch_num} failed after 3 retries, skipping.")
            continue

        for ticker in batch:
            try:
                df = pd.DataFrame({
                    "open":   data[("Open",   ticker)],
                    "high":   data[("High",   ticker)],
                    "low":    data[("Low",    ticker)],
                    "close":  data[("Close",  ticker)],
                    "volume": data[("Volume", ticker)],
                }).dropna(subset=["close"])
                if not df.empty:
                    result[ticker] = df
            except (KeyError, TypeError):
                pass

    return result


def normalize_to_et(ts) -> datetime:
    """Normalize any timestamp to US/Eastern.

    - TZ-aware (e.g. UTC from Yahoo): convert to ET
    - TZ-naive: localize as ET
    """
    if hasattr(ts, 'to_pydatetime'):
        ts = ts.to_pydatetime()
    if ts.tzinfo is not None:
        return ts.astimezone(ET)
    return ts.replace(tzinfo=ET)


def is_valid_intraday_bar(ts_et: datetime, interval_minutes: int) -> bool:
    """Return True if the timestamp is a valid regular-session bar.

    Checks: weekday, within 9:30-15:55 ET, timestamp on the interval boundary.
    """
    if ts_et.weekday() > 4:
        return False
    hm = (ts_et.hour, ts_et.minute)
    if hm < REGULAR_SESSION_START or hm > REGULAR_SESSION_END:
        return False
    if ts_et.minute % interval_minutes != 0 or ts_et.second != 0 or ts_et.microsecond != 0:
        return False
    return True


def _filter_regular_hours_intraday(df: pd.DataFrame, interval_minutes: int) -> pd.DataFrame:
    """Normalize timestamps to ET and keep only valid regular-session bars."""
    if df.empty:
        return df

    new_idx = pd.DatetimeIndex([normalize_to_et(ts) for ts in df.index])
    df.index = new_idx

    # Drop duplicates after TZ conversion
    df = df[~df.index.duplicated(keep='last')]

    mask = pd.Series(
        [is_valid_intraday_bar(ts, interval_minutes) for ts in df.index],
        index=df.index,
    )
    df = df[mask]
    return df.sort_index()


def upsert_intraday(ticker: str, df: pd.DataFrame, interval_label: str) -> int:
    """Upsert intraday rows into stock_prices_intraday.

    Normalizes to ET, filters to regular session, and validates interval.
    """
    # Validate interval
    if interval_label not in VALID_INTRADAY_INTERVALS:
        print(f"  [{ticker}] Rejected invalid interval '{interval_label}'")
        return 0

    # Parse interval minutes from label (e.g. '5m' -> 5, '15m' -> 15)
    interval_minutes = int(interval_label.replace('m', '').replace('min', ''))

    # Normalize timestamps and filter to regular session
    df = _filter_regular_hours_intraday(df, interval_minutes)
    if df.empty:
        return 0

    from psycopg2.extras import execute_values

    rows = []
    for idx, row in df.iterrows():
        ts_et = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
        vol = int(row["volume"]) if pd.notna(row["volume"]) else 0
        o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
        # Reject NaN/Inf values
        if any(math.isnan(x) or math.isinf(x) for x in (o, h, l, c)):
            print(f"  [{ticker}] Skipping bar with NaN/Inf at {ts_et}")
            continue
        if not is_valid_ohlcv(o, h, l, c, vol):
            print(f"  [{ticker}] Skipping bar with invalid OHLCV at {ts_et}")
            continue
        rows.append((ticker, ts_et, interval_label, o, h, l, c, vol))

    if not rows:
        return 0

    with get_db_cursor(dict_cursor=False) as cur:
        execute_values(
            cur,
            """
            INSERT INTO stock_prices_intraday
                (ticker, datetime, interval, open_price, high, low, close_price, volume)
            VALUES %s
            ON CONFLICT (ticker, datetime, interval) DO UPDATE SET
                open_price  = EXCLUDED.open_price,
                high        = EXCLUDED.high,
                low         = EXCLUDED.low,
                close_price = EXCLUDED.close_price,
                volume      = EXCLUDED.volume
            """,
            rows,
        )
    return len(rows)


# US Market hours (Eastern Time)
def is_market_hours() -> bool:
    """Check if US stock market is currently open."""
    now_et = datetime.now(ET)
    if now_et.weekday() > 4:
        return False
    return 9 <= now_et.hour < 16


def run_continuous(tickers: list[str], interval: str = "5m", interval_minutes: int = 5):
    """Run continuously — fetch intraday candles every N minutes during market hours."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("intraday-backfill")

    log.info(f"Starting continuous mode: {interval} candles every {interval_minutes} min")
    log.info(f"Tracking {len(tickers)} tickers")
    log.info("Press Ctrl+C to stop\n")

    while True:
        try:
            if is_market_hours():
                log.info("=" * 50)
                log.info(f"Market OPEN — fetching {interval} candles at {datetime.now().strftime('%H:%M:%S')}")
                ticker_data = fetch_intraday_yfinance(tickers, interval, days=1)
                total = 0
                success = 0
                for ticker in tickers:
                    df = ticker_data.get(ticker)
                    if df is not None and not df.empty:
                        total += upsert_intraday(ticker, df, interval)
                        success += 1
                log.info(f"Done — {total} candles upserted ({success}/{len(tickers)} tickers)")
            else:
                log.info(f"Market CLOSED — skipping ({datetime.now().strftime('%H:%M:%S')})")

            log.info(f"Next update in {interval_minutes} minutes...\n")
            time.sleep(interval_minutes * 60)

        except KeyboardInterrupt:
            log.info("\nStopping continuous mode...")
            break


def main():
    parser = argparse.ArgumentParser(description="Backfill / continuous intraday candles from Yahoo Finance")
    parser.add_argument("--tickers", type=str, default=None,
                        help="Comma-separated tickers. Default: all active")
    parser.add_argument("--interval", type=str, default="5m",
                        choices=["1m", "5m"],
                        help="Candle interval (default: 5m)")
    parser.add_argument("--provider", type=str, default="yahoo_direct",
                        choices=["yahoo_direct", "yahoo"],
                        help="Data provider: yahoo_direct (default) or yahoo (yfinance)")
    parser.add_argument("--days", type=int, default=7,
                        help="Days to backfill (default: 7)")
    parser.add_argument("--continuous", action="store_true",
                        help="Run continuously every 5 min during market hours")
    parser.add_argument("--update-interval", type=int, default=5,
                        help="Update frequency in minutes for continuous mode (default: 5)")
    parser.add_argument("--force", action="store_true",
                        help="Run even if market is closed")
    args = parser.parse_args()

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = get_selected_tickers(active_only=True)
        if not tickers:
            print("No active tickers.")
            return

    if args.continuous:
        run_continuous(tickers, args.interval, args.update_interval)
        return

    if not args.force and not is_market_hours():
        print("Market is closed. Use --force to run anyway, or --continuous for scheduled mode.")

    # yfinance limits: 1m max 7 days, 5m max 60 days, 15m max 60 days
    max_days = {"1m": 7, "5m": 60, "15m": 60}
    days = min(args.days, max_days.get(args.interval, 7))

    print(f"Backfilling {args.interval} candles | {len(tickers)} tickers | last {days} days | provider={args.provider}")
    print(f"Table: stock_prices_intraday (interval={args.interval})\n")

    if args.provider == "yahoo_direct":
        print("Downloading from Yahoo Chart API (direct)...")
        ticker_data = fetch_intraday_yahoo_direct(tickers, args.interval, days)
    else:
        print("Downloading from Yahoo Finance (yfinance)...")
        ticker_data = fetch_intraday_yfinance(tickers, args.interval, days)
    print(f"  Received data for {len(ticker_data)} / {len(tickers)} tickers\n")

    total_inserted = 0
    success = 0
    failed = []

    for ticker in tickers:
        df = ticker_data.get(ticker)
        if df is None or df.empty:
            failed.append(ticker)
            continue
        try:
            n = upsert_intraday(ticker, df, args.interval)
            total_inserted += n
            success += 1
            if n > 0:
                print(f"  [{ticker}] +{n} candles")
        except Exception as e:
            print(f"  [{ticker}] ERROR: {e}")
            failed.append(ticker)

    print(f"\nDone | {success}/{len(tickers)} tickers | +{total_inserted} candles inserted")
    if failed:
        print(f"No data: {', '.join(failed[:20])}{'...' if len(failed) > 20 else ''}")


if __name__ == "__main__":
    main()
