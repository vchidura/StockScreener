"""
Hourly price updater — fetches hourly candles for all active tickers.

Supports Yahoo Finance (default, no API key, batch download) and
Twelve Data (rate-limited, requires API key) providers.

Usage:
    # Run once with Yahoo Finance (default)
    python scripts/update_hourly_prices.py

    # Run with Twelve Data
    python scripts/update_hourly_prices.py --provider twelvedata

    # Run with built-in scheduler (continuous hourly loop)
    python scripts/update_hourly_prices.py --continuous

    # Specific tickers only
    python scripts/update_hourly_prices.py --tickers AAPL,MSFT,NVDA

    # Backfill last N days of hourly data
    python scripts/update_hourly_prices.py --backfill --days 30

Schedule via Windows Task Scheduler or cron (every hour, market hours):
  Cron: 30 10-16 * * 1-5 cd /path/to/backend && python scripts/update_hourly_prices.py

Environment variables:
    TWELVEDATA_API_KEY  - required only for --provider twelvedata
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict
import logging

import math

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

from database import get_db_cursor, get_selected_tickers, is_valid_ohlcv
from http_client import get_session

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("hourly-updater")

TWELVE_DATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")
RATE_LIMIT_SLEEP = 8  # seconds between calls (free tier: 8 req/min)
TD_BATCH_SIZE = 8    # symbols per TwelveData batch request
YF_BATCH_SIZE = 50   # yfinance handles batches efficiently
YAHOO_DIRECT_DELAY = 0.5  # seconds between Yahoo Chart API calls
YAHOO_HOURLY_MAX_DAYS = 730

# Canonical timezone — all hourly timestamps stored as US/Eastern
ET = ZoneInfo("America/New_York")

# US Market regular-session hourly bar start times (Eastern): 09:30 .. 15:30
# These are the 7 valid bar timestamps per trading day.
VALID_HOURLY_MINUTES = 30  # all hourly bars start at :30
VALID_HOURLY_HOURS = {9, 10, 11, 12, 13, 14, 15}

# US Market hours (Eastern Time)
MARKET_OPEN_HOUR = 9
MARKET_CLOSE_HOUR = 16


def normalize_to_et(ts) -> datetime:
    """Normalize any timestamp to US/Eastern, the canonical TZ for hourly data.

    - TZ-aware (e.g. UTC from Yahoo): convert to ET
    - TZ-naive (e.g. ET from TwelveData): localize as ET
    """
    if hasattr(ts, 'to_pydatetime'):
        ts = ts.to_pydatetime()
    if ts.tzinfo is not None:
        return ts.astimezone(ET)
    # Naive timestamps from TwelveData are already ET
    return ts.replace(tzinfo=ET)


def is_valid_hourly_bar(ts_et: datetime) -> bool:
    """Return True if the timestamp is a valid regular-session hourly bar.

    Valid bars: weekday, minute == 30, hour in {9..15}.
    """
    if ts_et.weekday() > 4:  # weekend
        return False
    if ts_et.minute != VALID_HOURLY_MINUTES:
        return False
    if ts_et.hour not in VALID_HOURLY_HOURS:
        return False
    return True


def is_market_hours() -> bool:
    """Check if US stock market is currently open."""
    now_et = datetime.now(ET)
    if now_et.weekday() > 4:
        return False
    return MARKET_OPEN_HOUR <= now_et.hour < MARKET_CLOSE_HOUR


def fetch_hourly_candles(ticker: str, days: int = 5) -> pd.DataFrame:
    """Fetch hourly candles from Twelve Data (single ticker)."""
    if not TWELVE_DATA_API_KEY:
        logger.error("TWELVEDATA_API_KEY not set")
        return pd.DataFrame()

    end = datetime.utcnow()
    start = end - timedelta(days=days + 2)  # buffer for weekends

    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": ticker,
        "interval": "1h",
        "start_date": start.strftime("%Y-%m-%d %H:%M:%S"),
        "end_date": end.strftime("%Y-%m-%d %H:%M:%S"),
        "apikey": TWELVE_DATA_API_KEY,
    }

    try:
        resp = get_session().get(url, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as e:
        logger.error(f"[{ticker}] API request failed: {e}")
        return pd.DataFrame()

    if payload.get("status") == "error":
        logger.warning(f"[{ticker}] API error: {payload.get('message', 'unknown')}")
        return pd.DataFrame()

    values = payload.get("values")
    if not values:
        return pd.DataFrame()

    rows = []
    for item in values:
        try:
            rows.append({
                "datetime": pd.to_datetime(item["datetime"]),
                "open": float(item["open"]),
                "high": float(item["high"]),
                "low": float(item["low"]),
                "close": float(item["close"]),
                "volume": int(float(item.get("volume", 0))),
            })
        except (ValueError, KeyError) as e:
            logger.warning(f"[{ticker}] Parse error for {item}: {e}")

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).set_index("datetime").sort_index()
    return _filter_regular_hours_hourly(df)


def fetch_hourly_yahoo_direct(tickers: list[str], days: int = 5) -> dict[str, pd.DataFrame]:
    """Fetch hourly candles via Yahoo Chart API directly (no yfinance, no rate limit).

    Returns dict of ticker -> DataFrame with columns: open, high, low, close, volume.
    All validations: NaN/Inf rejection, future date rejection, consecutive error breaker.
    """
    end_ts = int(time.time()) + 3600
    request_days = min(days + 3, YAHOO_HOURLY_MAX_DAYS)
    start_ts = end_ts - 86400 * request_days
    now_et = datetime.now(ET)
    result: dict[str, pd.DataFrame] = {}
    total = len(tickers)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    consecutive_errors = 0

    for i, ticker in enumerate(tickers, 1):
        logger.info(f"  [{i}/{total}] {ticker}...")

        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
            f"?period1={start_ts}&period2={end_ts}&interval=1h"
        )

        try:
            resp = get_session().get(url, headers=headers, timeout=15)
            if resp.status_code == 429:
                logger.warning(f"  [{ticker}] rate-limited, waiting 30s...")
                time.sleep(30)
                resp = get_session().get(url, headers=headers, timeout=15)

            if resp.status_code != 200:
                logger.warning(f"  [{ticker}] HTTP {resp.status_code}")
                consecutive_errors += 1
                if consecutive_errors >= 20:
                    logger.error("Too many consecutive errors. Stopping.")
                    break
                continue

            chart_result = resp.json().get("chart", {}).get("result", [None])[0]
            if not chart_result or "timestamp" not in chart_result:
                logger.warning(f"  [{ticker}] no data")
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
                rows.append({
                    "datetime": dt_et,
                    "open": o, "high": h, "low": l, "close": c, "volume": v,
                })

            if rejected:
                logger.debug(f"  [{ticker}] {rejected} rows rejected (NaN/Inf/future)")

            if rows:
                df = pd.DataFrame(rows).set_index("datetime").sort_index()
                df = _filter_regular_hours_hourly(df)
                if not df.empty:
                    result[ticker] = df
                    logger.info(f"  [{ticker}] {len(df)} valid hourly bars")
                    consecutive_errors = 0
                else:
                    logger.warning(f"  [{ticker}] no valid bars after filtering")
            else:
                logger.warning(f"  [{ticker}] no valid rows")

        except Exception as e:
            logger.error(f"  [{ticker}] error: {e}")
            consecutive_errors += 1
            if consecutive_errors >= 20:
                logger.error("Too many consecutive errors. Stopping.")
                break

        if i < total:
            time.sleep(YAHOO_DIRECT_DELAY)

    return result


def fetch_hourly_yfinance(tickers: list[str], days: int = 5) -> dict[str, pd.DataFrame]:
    """Batch-download hourly candles from Yahoo Finance.

    Returns dict of ticker -> DataFrame with columns: open, high, low, close, volume.
    Note: yfinance free tier supports hourly up to ~730 days of history.
    """
    # yfinance requires period strings for intraday intervals
    # max period for 1h interval is 730 days, but we cap to requested days
    end = datetime.utcnow() + timedelta(hours=1)
    request_days = min(days + 2, YAHOO_HOURLY_MAX_DAYS)
    start = end - timedelta(days=request_days)
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    result: dict[str, pd.DataFrame] = {}

    for i in range(0, len(tickers), YF_BATCH_SIZE):
        batch = tickers[i : i + YF_BATCH_SIZE]
        symbols = " ".join(batch)
        try:
            data = yf.download(
                symbols,
                start=start_str,
                end=end_str,
                interval="1h",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
        except Exception as e:
            logger.error(f"  yfinance batch error: {e}")
            continue

        if data.empty:
            continue

        # yfinance 1.2.0: always MultiIndex columns (Price, Ticker)
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
                    result[ticker] = _filter_regular_hours_hourly(df)
            except (KeyError, TypeError):
                pass

    return result


def _filter_regular_hours_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize timestamps to ET and keep only valid regular-session hourly bars."""
    if df.empty:
        return df

    # Normalize index to ET
    new_idx = pd.DatetimeIndex([normalize_to_et(ts) for ts in df.index])
    df.index = new_idx

    # Drop duplicates that appear after TZ conversion (same bar from different sources)
    df = df[~df.index.duplicated(keep='last')]

    # Keep only regular-session bars: weekday, minute==30, hour in 9..15
    mask = pd.Series(
        [is_valid_hourly_bar(ts) for ts in df.index],
        index=df.index,
    )
    before = len(df)
    df = df[mask]
    dropped = before - len(df)
    if dropped:
        logger.debug(f"  Filtered {dropped} non-regular-session hourly bars")

    return df.sort_index()


def upsert_hourly_prices(ticker: str, df: pd.DataFrame) -> int:
    """Upsert hourly candle rows into stock_prices_hourly.

    Timestamps are already normalized to ET by _filter_regular_hours_hourly().
    """
    if df.empty:
        return 0

    from psycopg2.extras import execute_values

    rows = []
    for idx, row in df.iterrows():
        ts = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
        # Final safety check: skip non-regular-session bars
        ts_et = normalize_to_et(ts)
        if not is_valid_hourly_bar(ts_et):
            logger.warning(f"[{ticker}] Skipping invalid hourly bar: {ts_et}")
            continue
        o, h, l, c = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])
        v = int(row["volume"])
        # Reject NaN/Inf values
        if any(math.isnan(x) or math.isinf(x) for x in (o, h, l, c)):
            logger.warning(f"[{ticker}] Skipping bar with NaN/Inf at {ts_et}")
            continue
        if not is_valid_ohlcv(o, h, l, c, v):
            logger.warning(f"[{ticker}] Skipping bar with invalid OHLCV at {ts_et}")
            continue
        rows.append((ticker, ts_et, o, h, l, c, v))

    if not rows:
        return 0

    try:
        with get_db_cursor(dict_cursor=False) as cursor:
            execute_values(
                cursor,
                """
                INSERT INTO stock_prices_hourly
                    (ticker, datetime, open_price, high, low, close_price, volume)
                VALUES %s
                ON CONFLICT (ticker, datetime) DO UPDATE SET
                    open_price  = EXCLUDED.open_price,
                    high        = EXCLUDED.high,
                    low         = EXCLUDED.low,
                    close_price = EXCLUDED.close_price,
                    volume      = EXCLUDED.volume
                """,
                rows,
            )
    except Exception as e:
        logger.error(f"[{ticker}] DB error during batch upsert: {e}")
        return 0

    return len(rows)


def update_tickers(tickers: List[str], days: int = 5, provider: str = "yahoo") -> tuple:
    """Update hourly prices for all tickers using the specified provider."""
    success = 0
    failed = 0
    total_rows = 0

    if provider in ("yahoo", "yahoo_direct"):
        if provider == "yahoo_direct":
            logger.info(f"Downloading hourly data from Yahoo Chart API ({len(tickers)} tickers)...")
            ticker_data = fetch_hourly_yahoo_direct(tickers, days)
        else:
            logger.info(f"Downloading hourly data from Yahoo Finance ({len(tickers)} tickers, batch={YF_BATCH_SIZE})...")
            ticker_data = fetch_hourly_yfinance(tickers, days)
        logger.info(f"  Received data for {len(ticker_data)} / {len(tickers)} tickers")

        for ticker in tickers:
            df = ticker_data.get(ticker)
            if df is None or df.empty:
                failed += 1
                continue
            try:
                n = upsert_hourly_prices(ticker, df)
                total_rows += n
                success += 1
                if n > 0:
                    logger.info(f"  [{ticker}] {n} hourly candles upserted")
            except Exception as e:
                logger.error(f"  [{ticker}] Error: {e}")
                failed += 1
    else:
        # Twelve Data: one ticker at a time with rate limiting
        for i, ticker in enumerate(tickers, 1):
            logger.info(f"[{ticker}] ({i}/{len(tickers)}) fetching hourly data...")
            try:
                df = fetch_hourly_candles(ticker, days)
                if df.empty:
                    logger.warning(f"[{ticker}] No data")
                    failed += 1
                    continue
                n = upsert_hourly_prices(ticker, df)
                total_rows += n
                success += 1
                logger.info(f"[{ticker}] {n} hourly candles upserted")
            except Exception as e:
                logger.error(f"[{ticker}] Error: {e}")
                failed += 1
            # Rate limiting (skip on last ticker)
            if i < len(tickers):
                time.sleep(RATE_LIMIT_SLEEP)

    return success, failed, total_rows


def run_continuous(tickers: List[str], interval_minutes: int = 60, provider: str = "yahoo"):
    """Run continuously with built-in scheduler."""
    logger.info(f"Starting continuous mode: update every {interval_minutes} minutes (provider={provider})")
    logger.info(f"Tracking {len(tickers)} tickers")
    logger.info("Press Ctrl+C to stop\n")

    while True:
        try:
            if is_market_hours():
                logger.info("=" * 50)
                logger.info(f"Market OPEN - running hourly update at {datetime.now().strftime('%H:%M:%S')}")
                success, failed, total = update_tickers(tickers, days=2, provider=provider)
                logger.info(f"Completed: {success} updated, {failed} failed, {total} rows")
            else:
                logger.info(f"Market CLOSED - skipping ({datetime.now().strftime('%H:%M:%S')})")

            logger.info(f"Next update in {interval_minutes} minutes...\n")
            time.sleep(interval_minutes * 60)

        except KeyboardInterrupt:
            logger.info("\nStopping continuous mode...")
            break


def main():
    parser = argparse.ArgumentParser(description="Hourly price updater")
    parser.add_argument("--tickers", type=str, default=None,
                        help="Comma-separated tickers. Default: all active selected_tickers")
    parser.add_argument("--provider", type=str, default="yahoo_direct",
                        choices=["yahoo_direct", "yahoo", "twelvedata"],
                        help="Data provider: yahoo_direct (default), yahoo (yfinance), or twelvedata")
    parser.add_argument("--continuous", action="store_true",
                        help="Run continuously with hourly intervals")
    parser.add_argument("--interval", type=int, default=60,
                        help="Update interval in minutes for continuous mode (default: 60)")
    parser.add_argument("--backfill", action="store_true",
                        help="Backfill historical hourly data")
    parser.add_argument("--days", type=int, default=5,
                        help="Number of days to fetch (default: 5)")
    parser.add_argument("--force", action="store_true",
                        help="Run even if market is closed")
    args = parser.parse_args()

    provider = args.provider

    # Check API key only when using twelvedata
    if provider == "twelvedata" and not TWELVE_DATA_API_KEY:
        logger.error("TWELVEDATA_API_KEY environment variable not set!")
        logger.error("Get a free key at: https://twelvedata.com/")
        sys.exit(1)

    # Get tickers
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = get_selected_tickers(active_only=True)
        if not tickers:
            logger.error("No active tickers in selected_tickers table.")
            sys.exit(1)

    logger.info(f"Hourly Price Updater | {len(tickers)} tickers | provider={provider}")

    if args.continuous:
        run_continuous(tickers, args.interval, provider)
    else:
        # Single run mode
        if not args.force and not args.backfill and not is_market_hours():
            logger.warning("Market is closed. Use --force to run anyway.")
            sys.exit(0)

        success, failed, total = update_tickers(tickers, args.days, provider)
        logger.info(f"Done | {success} updated, {failed} failed, {total} rows")


if __name__ == "__main__":
    main()
