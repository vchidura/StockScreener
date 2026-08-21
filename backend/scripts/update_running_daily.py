"""
Running daily candle updater — keeps today's daily bar fresh during market hours.

Fetches the latest quotes for all active tickers and upserts today's
running candle in stock_prices_daily. Overwritten by update_daily_prices.py
at end-of-day with the final bar.

Used by run_scheduler.py (job_daily_candle) every 5 min during market hours.

Usage:
    # Run once with Yahoo Finance (default)
    python scripts/update_intraday_prices.py --force

    # Run with Twelve Data
    python scripts/update_intraday_prices.py --provider twelvedata --force

    # Run with built-in scheduler (continuous 15-min loop)
    python scripts/update_intraday_prices.py --continuous

    # Specific tickers only
    python scripts/update_intraday_prices.py --tickers AAPL,MSFT,NVDA

Schedule via Windows Task Scheduler or cron (every 15 min, market hours):
  Windows Task Scheduler:
    Trigger: Daily, repeat every 15 minutes
    Start: 9:30 AM ET, Stop: 4:00 PM ET (weekdays only)
    Action: python scripts/update_intraday_prices.py

Environment variables:
    TWELVEDATA_API_KEY  - required only for --provider twelvedata
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
import logging

import pandas as pd
import requests
import yfinance as yf

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor, get_selected_tickers, is_valid_ohlcv

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("running-daily-updater")

TWELVE_DATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")
RATE_LIMIT_SLEEP = 8  # seconds between Twelve Data calls (8 req/min free tier)
TD_BATCH_SIZE = 8    # Twelve Data supports up to 8 symbols per batch request
YF_BATCH_SIZE = 50   # yfinance handles batches efficiently

# US Market hours (Eastern Time)
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MIN = 30
MARKET_CLOSE_HOUR = 16
MARKET_CLOSE_MIN = 0


def is_market_hours() -> bool:
    """Check if US stock market is currently open (approximate, no holiday check)."""
    try:
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
    except ImportError:
        import pytz
        et = pytz.timezone("America/New_York")
    
    now_et = datetime.now(et)
    
    # Weekday check (Monday=0, Friday=4)
    if now_et.weekday() > 4:
        return False
    
    # Time check
    market_open = now_et.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MIN, second=0)
    market_close = now_et.replace(hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MIN, second=0)
    
    return market_open <= now_et <= market_close


def fetch_batch_quotes(tickers: List[str]) -> Dict[str, Dict]:
    """
    Fetch real-time quotes for multiple tickers in a single API call.
    Returns dict: {ticker: {open, high, low, close, volume, datetime}}
    """
    if not TWELVE_DATA_API_KEY:
        logger.error("TWELVEDATA_API_KEY not set")
        return {}
    
    url = "https://api.twelvedata.com/quote"
    params = {
        "symbol": ",".join(tickers),
        "apikey": TWELVE_DATA_API_KEY,
    }
    
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"API request failed: {e}")
        return {}
    
    results = {}
    
    # Single ticker returns dict directly, multiple returns {ticker: data}
    if len(tickers) == 1:
        data = {tickers[0]: data}
    
    for ticker, quote in data.items():
        if isinstance(quote, dict) and quote.get("status") != "error":
            try:
                results[ticker] = {
                    "open": float(quote.get("open", 0)),
                    "high": float(quote.get("high", 0)),
                    "low": float(quote.get("low", 0)),
                    "close": float(quote.get("close", 0)),
                    "volume": int(float(quote.get("volume", 0))),
                    "datetime": quote.get("datetime", datetime.now().strftime("%Y-%m-%d")),
                }
            except (ValueError, TypeError) as e:
                logger.warning(f"[{ticker}] Parse error: {e}")
    
    return results


def fetch_yfinance_quotes(tickers: List[str]) -> Dict[str, Dict]:
    """Batch-fetch latest quotes from Yahoo Finance.
    Returns dict: {ticker: {open, high, low, close, volume, datetime}}
    """
    result: Dict[str, Dict] = {}
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    # Use period='1d' to get today's data
    for i in range(0, len(tickers), YF_BATCH_SIZE):
        batch = tickers[i : i + YF_BATCH_SIZE]
        symbols = " ".join(batch)
        try:
            data = yf.download(
                symbols,
                period="1d",
                interval="1d",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
        except Exception as e:
            logger.error(f"  yfinance batch error: {e}")
            continue

        if data.empty:
            continue

        for ticker in batch:
            try:
                o = float(data[("Open",   ticker)].iloc[-1])
                h = float(data[("High",   ticker)].iloc[-1])
                lo = float(data[("Low",    ticker)].iloc[-1])
                c = float(data[("Close",  ticker)].iloc[-1])
                v = int(data[("Volume", ticker)].iloc[-1])
                dt = data.index[-1]
                dt_str = dt.strftime("%Y-%m-%d") if hasattr(dt, 'strftime') else today_str
                if pd.notna(c):
                    result[ticker] = {
                        "open": o, "high": h, "low": lo, "close": c,
                        "volume": v, "datetime": dt_str,
                    }
            except (KeyError, TypeError, IndexError):
                pass

    return result


def upsert_intraday_price(ticker: str, data: Dict) -> bool:
    """Upsert today's candle into stock_prices_daily."""
    try:
        if not is_valid_ohlcv(data["open"], data["high"], data["low"], data["close"], data.get("volume", 0)):
            logger.warning(f"[{ticker}] Skipping daily bar with invalid OHLCV")
            return False
        # Parse date from API response
        dt_str = data["datetime"]
        if " " in dt_str:
            dt_str = dt_str.split(" ")[0]  # Extract date part
        ts = datetime.strptime(dt_str, "%Y-%m-%d")
        # Reject weekends
        if ts.weekday() >= 5:
            return False
        
        with get_db_cursor(dict_cursor=False) as cursor:
            cursor.execute(
                """
                INSERT INTO stock_prices_daily
                    (ticker, datetime, open_price, high, low, close_price, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, datetime) DO UPDATE SET
                    open_price  = EXCLUDED.open_price,
                    high        = EXCLUDED.high,
                    low         = EXCLUDED.low,
                    close_price = EXCLUDED.close_price,
                    volume      = EXCLUDED.volume
                """,
                (
                    ticker,
                    ts,
                    data["open"],
                    data["high"],
                    data["low"],
                    data["close"],
                    data["volume"],
                ),
            )
        return True
    except Exception as e:
        logger.error(f"[{ticker}] DB upsert failed: {e}")
        return False


def update_tickers(tickers: List[str], provider: str = "yahoo") -> tuple:
    """
    Update prices for all tickers using the specified provider.
    Returns (success_count, fail_count).
    """
    success = 0
    failed = 0

    if provider == "yahoo":
        logger.info(f"Fetching quotes from Yahoo Finance ({len(tickers)} tickers, batch={YF_BATCH_SIZE})...")
        quotes = fetch_yfinance_quotes(tickers)
        logger.info(f"  Received quotes for {len(quotes)} / {len(tickers)} tickers")
        for ticker in tickers:
            if ticker in quotes:
                if upsert_intraday_price(ticker, quotes[ticker]):
                    success += 1
                else:
                    failed += 1
            else:
                failed += 1
    else:
        # Twelve Data: batch of 8 with rate limiting
        for i in range(0, len(tickers), TD_BATCH_SIZE):
            batch = tickers[i:i + TD_BATCH_SIZE]
            batch_num = (i // TD_BATCH_SIZE) + 1
            total_batches = (len(tickers) + TD_BATCH_SIZE - 1) // TD_BATCH_SIZE

            logger.info(f"Batch {batch_num}/{total_batches}: {', '.join(batch)}")

            quotes = fetch_batch_quotes(batch)

            for ticker in batch:
                if ticker in quotes:
                    if upsert_intraday_price(ticker, quotes[ticker]):
                        success += 1
                    else:
                        failed += 1
                else:
                    logger.warning(f"[{ticker}] No quote data received")
                    failed += 1

            # Rate limiting between batches (not after last batch)
            if i + TD_BATCH_SIZE < len(tickers):
                time.sleep(RATE_LIMIT_SLEEP)

    return success, failed


def run_continuous(tickers: List[str], interval_minutes: int = 15, provider: str = "yahoo"):
    """Run continuously with built-in scheduler."""
    logger.info(f"Starting continuous mode: update every {interval_minutes} minutes (provider={provider})")
    logger.info(f"Tracking {len(tickers)} tickers")
    logger.info("Press Ctrl+C to stop\n")

    while True:
        try:
            if is_market_hours():
                logger.info("=" * 50)
                logger.info(f"Market is OPEN - running update at {datetime.now().strftime('%H:%M:%S')}")
                success, failed = update_tickers(tickers, provider)
                logger.info(f"Completed: {success} updated, {failed} failed")
            else:
                logger.info(f"Market CLOSED - skipping update ({datetime.now().strftime('%H:%M:%S')})")

            # Sleep until next interval
            logger.info(f"Next update in {interval_minutes} minutes...\n")
            time.sleep(interval_minutes * 60)

        except KeyboardInterrupt:
            logger.info("\nStopping continuous mode...")
            break


def main():
    parser = argparse.ArgumentParser(description="Intraday price updater (15-min intervals)")
    parser.add_argument("--tickers", type=str, default=None,
                        help="Comma-separated tickers. Default: all active selected_tickers")
    parser.add_argument("--provider", type=str, default="yahoo",
                        choices=["yahoo", "twelvedata"],
                        help="Data provider: yahoo (default) or twelvedata")
    parser.add_argument("--continuous", action="store_true",
                        help="Run continuously with 15-min intervals")
    parser.add_argument("--interval", type=int, default=15,
                        help="Update interval in minutes for continuous mode (default: 15)")
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

    logger.info(f"Intraday Price Updater | {len(tickers)} tickers | provider={provider}")

    if args.continuous:
        run_continuous(tickers, args.interval, provider)
    else:
        # Single run mode
        if not args.force and not is_market_hours():
            logger.warning("Market is closed. Use --force to run anyway.")
            sys.exit(0)

        success, failed = update_tickers(tickers, provider)
        logger.info(f"Done | {success} updated, {failed} failed")


if __name__ == "__main__":
    main()
