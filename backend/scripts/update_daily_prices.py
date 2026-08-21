"""
Daily price update job — runs independently of the portal.

Fetches the latest daily candles for all active selected_tickers and
upserts them into stock_prices_daily. Supports Yahoo Direct (default),
yfinance, and Twelve Data providers.

Usage:
    python scripts/update_daily_prices.py                           # yahoo_direct (default)
    python scripts/update_daily_prices.py --provider yahoo_direct   # yahoo chart API (no rate limit)
    python scripts/update_daily_prices.py --provider twelvedata     # twelve data
    python scripts/update_daily_prices.py --provider yahoo          # yfinance (rate limited)
    python scripts/update_daily_prices.py --tickers AAPL,MSFT
    python scripts/update_daily_prices.py --days 5                  # fetch last 5 days
    python scripts/update_daily_prices.py --days 30                 # backfill last 30 days
    python scripts/update_daily_prices.py --continuous              # run daily after market close

Schedule via Windows Task Scheduler or cron to run once after market close.
  Example (weekdays at 5:30 PM ET):
    30 17 * * 1-5  cd /path/to/backend && python scripts/update_daily_prices.py
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv
load_dotenv(BACKEND_DIR / ".env")

from database import get_db_cursor, get_selected_tickers, is_valid_ohlcv

BATCH_SIZE = 20  # smaller batches to avoid Yahoo rate limits
YF_BATCH_SLEEP = 3  # seconds between yfinance batches
TWELVE_DATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")
RATE_LIMIT_SLEEP = 8  # seconds between Twelve Data calls (8 req/min free tier)


def get_existing_dates(ticker: str, start_date: str, end_date: str) -> set:
    """Return the set of dates already in DB for this ticker in the given range."""
    with get_db_cursor(dict_cursor=False) as cur:
        cur.execute(
            """SELECT datetime::date FROM stock_prices_daily
               WHERE ticker = %s AND datetime >= %s AND datetime <= %s""",
            (ticker, start_date, end_date),
        )
        return {row[0] for row in cur.fetchall()}


TWELVE_BATCH_SIZE = 8   # free tier: 8 API credits/min, each symbol = 1 credit
TWELVE_BATCH_WAIT = 62  # seconds to wait between batches (full minute + buffer)


def _twelvedata_batch_call(symbols: str, params: dict, max_retries: int = 3) -> dict | None:
    """Twelve Data batch API call with rate-limit retry logic.
    When multiple symbols are requested, the response is keyed by symbol name.
    """
    url = "https://api.twelvedata.com/time_series"
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:
            print(f"HTTP error: {e}")
            return None

        # Single-ticker response has "status" at top level
        if payload.get("status") == "error":
            msg = payload.get("message", "")
            if "API credits" in msg or "rate" in msg.lower():
                wait = 62  # wait full minute for credits to reset
                print(f"\n    Rate-limited, waiting {wait}s (attempt {attempt}/{max_retries})...", flush=True)
                time.sleep(wait)
                continue
            print(f"API error: {msg}")
            return None

        return payload

    print("failed after retries")
    return None


def _parse_twelvedata_values(values: list) -> pd.DataFrame | None:
    """Parse Twelve Data values list into a DataFrame with validation."""
    import math
    today = datetime.utcnow().date()
    rows = []
    for item in values:
        try:
            dt = pd.to_datetime(item["datetime"])
            # Strip any timezone info → naive UTC midnight for daily candles
            if dt.tzinfo is not None:
                dt = dt.tz_localize(None)
            # Reject future dates
            if dt.date() > today:
                continue
            o = float(item["open"])
            h = float(item["high"])
            l = float(item["low"])
            c = float(item["close"])
            v = int(float(item.get("volume", 0)))
            # Reject NaN / Inf values
            if any(math.isnan(x) or math.isinf(x) for x in (o, h, l, c)):
                continue
            rows.append({
                "datetime": dt,
                "Open": o, "High": h, "Low": l, "Close": c,
                "Volume": v,
            })
        except (ValueError, KeyError):
            pass
    if rows:
        return pd.DataFrame(rows).set_index("datetime").sort_index()
    return None


def fetch_daily_twelvedata(tickers: list[str], days: int) -> dict[str, pd.DataFrame]:
    """Fetch daily candles from Twelve Data in batches (up to 8 symbols per request).

    Free tier: 8 API credits/minute. Each symbol in a batch = 1 credit.
    So we send 8 symbols per batch, then wait 62s for the next minute window.
    """
    if not TWELVE_DATA_API_KEY:
        print("  ERROR: TWELVEDATA_API_KEY not set. Get a free key at https://twelvedata.com/")
        return {}

    end = datetime.utcnow() + timedelta(days=1)
    start = end - timedelta(days=days + 10)
    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")
    result: dict[str, pd.DataFrame] = {}

    total = len(tickers)
    total_batches = (total + TWELVE_BATCH_SIZE - 1) // TWELVE_BATCH_SIZE
    est_minutes = total_batches  # ~1 min per batch
    print(f"  Batching {total} tickers in groups of {TWELVE_BATCH_SIZE} "
          f"({total_batches} batches, ~{est_minutes} min)")

    for batch_num, i in enumerate(range(0, total, TWELVE_BATCH_SIZE), 1):
        batch = tickers[i : i + TWELVE_BATCH_SIZE]
        symbols_str = ",".join(batch)
        print(f"  [Batch {batch_num}/{total_batches}] {symbols_str}...", end=" ", flush=True)

        params = {
            "symbol": symbols_str,
            "interval": "1day",
            "start_date": start_str,
            "end_date": end_str,
            "apikey": TWELVE_DATA_API_KEY,
        }

        payload = _twelvedata_batch_call(symbols_str, params)
        if payload is None:
            print("failed")
            # Wait before next batch
            if batch_num < total_batches:
                time.sleep(TWELVE_BATCH_WAIT)
            continue

        got = 0
        skipped_interval = 0
        if len(batch) == 1:
            # Single ticker: response has "values" at top level
            # Verify interval matches what we requested
            meta = payload.get("meta", {})
            resp_interval = meta.get("interval", "1day")
            if resp_interval != "1day":
                print(f"WRONG interval '{resp_interval}', expected '1day' — skipping")
            else:
                values = payload.get("values")
                if values:
                    df = _parse_twelvedata_values(values)
                    if df is not None and not df.empty:
                        result[batch[0]] = df
                        got += 1
        else:
            # Multi-ticker: response is keyed by symbol
            for ticker in batch:
                ticker_data = payload.get(ticker, {})
                if ticker_data.get("status") == "error":
                    continue
                # Verify interval matches
                meta = ticker_data.get("meta", {})
                resp_interval = meta.get("interval", "1day")
                if resp_interval != "1day":
                    skipped_interval += 1
                    continue
                values = ticker_data.get("values")
                if values:
                    df = _parse_twelvedata_values(values)
                    if df is not None and not df.empty:
                        result[ticker] = df
                        got += 1

        msg = f"{got}/{len(batch)} OK"
        if skipped_interval:
            msg += f" ({skipped_interval} wrong interval)"
        print(msg)

        # Wait for next minute window (skip after last batch)
        if batch_num < total_batches:
            print(f"    Waiting {TWELVE_BATCH_WAIT}s for rate limit reset...", flush=True)
            time.sleep(TWELVE_BATCH_WAIT)

    return result


YAHOO_DIRECT_DELAY = 0.5  # seconds between Yahoo Chart API calls (be polite)


def fetch_daily_yahoo_direct(tickers: list[str], days: int) -> dict[str, pd.DataFrame]:
    """Fetch daily candles via Yahoo Chart API directly (no yfinance, no rate limit).

    Returns a dict of ticker -> DataFrame with columns: Open, High, Low, Close, Volume.
    """
    import math
    end_ts = int(time.time()) + 86400
    start_ts = end_ts - 86400 * (days + 10)  # buffer for weekends/holidays
    today = datetime.utcnow().date()

    result: dict[str, pd.DataFrame] = {}
    total = len(tickers)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    consecutive_errors = 0

    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i}/{total}] {ticker}...", end=" ", flush=True)

        url = (f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
               f"?period1={start_ts}&period2={end_ts}&interval=1d")

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
                    print(f"\n  Too many consecutive errors. Stopping.")
                    break
                continue

            chart_result = resp.json().get("chart", {}).get("result", [None])[0]
            if not chart_result or "timestamp" not in chart_result:
                print("no data")
                continue

            timestamps = chart_result["timestamp"]
            quote = chart_result["indicators"]["quote"][0]

            rows = []
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
                    continue
                h = max(h, o, c)
                l = min(l, o, c)
                dt = datetime.utcfromtimestamp(ts)
                # Strip timezone, reject future dates
                if dt.date() > today:
                    continue
                rows.append({"datetime": dt, "Open": o, "High": h, "Low": l, "Close": c, "Volume": v})

            if rows:
                df = pd.DataFrame(rows).set_index("datetime").sort_index()
                result[ticker] = df
                print(f"{len(rows)} rows ({df.index.min().date()}..{df.index.max().date()})")
                consecutive_errors = 0
            else:
                print("no valid rows")

        except Exception as e:
            print(f"error: {e}")
            consecutive_errors += 1
            if consecutive_errors >= 20:
                print(f"\n  Too many consecutive errors. Stopping.")
                break

        # Small delay to be polite (skip on last)
        if i < total:
            time.sleep(YAHOO_DIRECT_DELAY)

    return result


def fetch_daily_yfinance(tickers: list[str], days: int) -> dict[str, pd.DataFrame]:
    """Download daily candles from Yahoo Finance one ticker at a time with rate-limit handling.

    Returns a dict of ticker -> DataFrame with columns: Open, High, Low, Close, Volume.
    """
    try:
        from yfinance.exceptions import YFRateLimitError
    except ImportError:
        YFRateLimitError = Exception

    end = datetime.utcnow() + timedelta(days=1)
    start = end - timedelta(days=days + 10)  # buffer for weekends/holidays

    start_str = start.strftime("%Y-%m-%d")
    end_str = end.strftime("%Y-%m-%d")

    result: dict[str, pd.DataFrame] = {}
    total = len(tickers)
    consecutive_rate_limits = 0

    for i, ticker in enumerate(tickers, 1):
        print(f"  [{i}/{total}] {ticker}...", end=" ", flush=True)

        df = None
        for attempt in range(1, 4):
            try:
                t = yf.Ticker(ticker)
                df = t.history(start=start_str, end=end_str, interval="1d", auto_adjust=True)
                consecutive_rate_limits = 0
                break
            except (YFRateLimitError, Exception) as e:
                err = str(e).lower()
                if "rate" in err or "too many" in err or isinstance(e, YFRateLimitError):
                    consecutive_rate_limits += 1
                    wait = 60 * attempt
                    print(f"rate-limited, waiting {wait}s (attempt {attempt}/3)...", end=" ", flush=True)
                    time.sleep(wait)
                else:
                    print(f"error: {e}")
                    break

        if consecutive_rate_limits >= 10:
            print(f"\n  Too many consecutive rate limits ({consecutive_rate_limits}). "
                  f"Yahoo Finance is blocking requests. Try again later.")
            break

        if df is not None and not df.empty:
            # Standardize column names
            df = df[["Open", "High", "Low", "Close", "Volume"]].dropna(subset=["Close"])
            if not df.empty:
                result[ticker] = df
                print(f"{len(df)} rows")
            else:
                print("no data")
        else:
            print("no data")

        # Small delay between tickers to be polite
        if i < total:
            time.sleep(2)

    return result


def upsert_prices(ticker: str, df: pd.DataFrame, existing_dates: set) -> tuple[int, int]:
    """Upsert daily candle rows into stock_prices_daily. Returns (inserted, skipped)."""
    import math
    today = datetime.utcnow().date()
    inserted = 0
    skipped = 0
    rejected = 0
    with get_db_cursor(dict_cursor=False) as cursor:
        for idx, row in df.iterrows():
            dt = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
            # Strip timezone if present → store as naive UTC midnight
            if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)
            dt_date = dt.date() if hasattr(dt, "date") else dt

            # Skip dates already in the database
            if dt_date in existing_dates:
                skipped += 1
                continue

            # Skip weekend dates (Sat=5, Sun=6)
            if hasattr(dt_date, 'weekday') and dt_date.weekday() >= 5:
                rejected += 1
                continue

            # Reject future dates
            if dt_date > today:
                rejected += 1
                continue

            vol = int(row["Volume"]) if pd.notna(row["Volume"]) else 0
            o, h, l, c = float(row["Open"]), float(row["High"]), float(row["Low"]), float(row["Close"])

            # Reject NaN / Inf values
            if any(math.isnan(x) or math.isinf(x) for x in (o, h, l, c)):
                rejected += 1
                continue

            # Validate OHLCV (price sanity: positive, H>=L, H>=O/C, L<=O/C)
            if not is_valid_ohlcv(o, h, l, c, vol):
                rejected += 1
                continue

            # Normalize datetime to midnight (date only, no time component)
            dt_normalized = datetime(dt_date.year, dt_date.month, dt_date.day)

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
                (ticker, dt_normalized, o, h, l, c, vol),
            )
            inserted += 1
    if rejected > 0:
        print(f"    [{ticker}] {rejected} rows rejected (weekend/future/invalid)")
    return inserted, skipped


# US Market hours (Eastern Time)
MARKET_CLOSE_HOUR = 16


def is_market_hours() -> bool:
    """Check if US stock market is currently open."""
    try:
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
    except ImportError:
        import pytz
        et = pytz.timezone("America/New_York")

    now_et = datetime.now(et)
    if now_et.weekday() > 4:
        return False
    return 9 <= now_et.hour < MARKET_CLOSE_HOUR


def run_continuous(tickers: list[str], days: int, provider: str):
    """Run continuously — waits for market close each day, then updates daily prices."""
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log = logging.getLogger("daily-updater")

    log.info(f"Starting continuous mode: daily update after market close (provider={provider})")
    log.info(f"Tracking {len(tickers)} tickers")
    log.info("Press Ctrl+C to stop\n")

    last_update_date = None

    while True:
        try:
            try:
                from zoneinfo import ZoneInfo
                et = ZoneInfo("America/New_York")
            except ImportError:
                import pytz
                et = pytz.timezone("America/New_York")

            now_et = datetime.now(et)
            today = now_et.date()

            # Run once after market close (4:15 PM – 6:00 PM ET) on weekdays
            if (now_et.weekday() < 5
                    and now_et.hour >= 16 and now_et.hour < 18
                    and (now_et.hour > 16 or now_et.minute >= 15)
                    and last_update_date != today):

                log.info("=" * 50)
                log.info(f"Market CLOSED — running daily update at {now_et.strftime('%H:%M:%S')} ET")

                end_dt = datetime.utcnow() + timedelta(days=1)
                start_dt = end_dt - timedelta(days=days + 10)
                start_str = start_dt.strftime("%Y-%m-%d")
                end_str = end_dt.strftime("%Y-%m-%d")

                if provider == "twelvedata":
                    ticker_data = fetch_daily_twelvedata(tickers, days)
                elif provider == "yahoo_direct":
                    ticker_data = fetch_daily_yahoo_direct(tickers, days)
                else:
                    ticker_data = fetch_daily_yfinance(tickers, days)

                total_inserted = 0
                success = 0
                for ticker in tickers:
                    df = ticker_data.get(ticker)
                    if df is not None and not df.empty:
                        try:
                            existing = get_existing_dates(ticker, start_str, end_str)
                            inserted, _ = upsert_prices(ticker, df, existing)
                            total_inserted += inserted
                            success += 1
                        except Exception as e:
                            log.error(f"  [{ticker}] ERROR: {e}")

                log.info(f"Done — {success}/{len(tickers)} tickers | +{total_inserted} new rows")
                last_update_date = today
            else:
                status = "OPEN" if is_market_hours() else "CLOSED"
                log.info(f"Market {status} ({now_et.strftime('%A %H:%M')} ET) — waiting for post-close window...")

            time.sleep(60)  # check every minute

        except KeyboardInterrupt:
            log.info("\nStopping continuous mode...")
            break


def main():
    parser = argparse.ArgumentParser(description="Daily price updater → DB")
    parser.add_argument("--tickers", type=str, default=None,
                        help="Comma-separated tickers. Default: all active selected_tickers")
    parser.add_argument("--days", type=int, default=5,
                        help="Number of calendar days to look back (default: 5)")
    parser.add_argument("--provider", type=str, default="yahoo_direct",
                        choices=["yahoo_direct", "yahoo", "twelvedata"],
                        help="Data provider: yahoo_direct (default), yahoo (yfinance), or twelvedata")
    parser.add_argument("--continuous", action="store_true",
                        help="Run continuously — update daily after each market close")
    parser.add_argument("--force", action="store_true",
                        help="Run even if market is still open")
    args = parser.parse_args()

    if args.provider == "twelvedata" and not TWELVE_DATA_API_KEY:
        print("ERROR: TWELVEDATA_API_KEY not set. Get a free key at https://twelvedata.com/")
        return

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = get_selected_tickers(active_only=True)
        if not tickers:
            print("No active tickers in selected_tickers table.")
            return

    if args.continuous:
        run_continuous(tickers, args.days, args.provider)
        return

    end_dt = datetime.utcnow() + timedelta(days=1)
    start_dt = end_dt - timedelta(days=args.days + 10)
    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = end_dt.strftime("%Y-%m-%d")

    provider = args.provider
    print(f"Updating {len(tickers)} tickers | last {args.days} days | source={provider}")
    if provider == "yahoo_direct":
        est_min = len(tickers) * YAHOO_DIRECT_DELAY / 60
        print(f"Yahoo Chart API direct | ~{est_min:.0f} min (no rate limit)")
    elif provider == "yahoo":
        print(f"yfinance single-ticker downloads with 2s delay")
    else:
        batches = (len(tickers) + TWELVE_BATCH_SIZE - 1) // TWELVE_BATCH_SIZE
        print(f"Twelve Data batch mode: {TWELVE_BATCH_SIZE} symbols/batch, "
              f"{batches} batches, ~{batches} min total")
    print()

    # Step 1: Download from selected provider
    if provider == "twelvedata":
        if not TWELVE_DATA_API_KEY:
            print("ERROR: TWELVEDATA_API_KEY not set. Get a free key at https://twelvedata.com/")
            return
        print("Downloading from Twelve Data...")
        ticker_data = fetch_daily_twelvedata(tickers, args.days)
    elif provider == "yahoo_direct":
        print("Downloading from Yahoo Chart API (direct)...")
        ticker_data = fetch_daily_yahoo_direct(tickers, args.days)
    else:
        print("Downloading from Yahoo Finance (yfinance)...")
        ticker_data = fetch_daily_yfinance(tickers, args.days)
    print(f"  Received data for {len(ticker_data)} / {len(tickers)} tickers\n")

    # Step 2: Upsert only missing days into DB
    total_inserted = 0
    total_skipped = 0
    success = 0
    failed = []

    for i, ticker in enumerate(tickers, 1):
        df = ticker_data.get(ticker)
        if df is None or df.empty:
            failed.append(ticker)
            continue

        try:
            existing = get_existing_dates(ticker, start_str, end_str)
            inserted, skipped = upsert_prices(ticker, df, existing)
            total_inserted += inserted
            total_skipped += skipped
            success += 1
            if inserted > 0:
                print(f"  [{ticker}] +{inserted} new rows (skipped {skipped} existing)")
        except Exception as e:
            print(f"  [{ticker}] ERROR: {e}")
            failed.append(ticker)

    print(f"\nDone | {success}/{len(tickers)} tickers | +{total_inserted} new rows | {total_skipped} skipped (already in DB)")
    if failed:
        print(f"No data: {', '.join(failed)}")


if __name__ == "__main__":
    main()
