"""Polygon.io (massive.com) provider — unlimited API calls on Stocks Starter+.

Uses the aggregates endpoint for daily/hourly/intraday bars:
  GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}

Docs: https://polygon.io/docs/rest/stocks/aggregates/custom-bars
Note: Stocks Starter plan data is 15-minute delayed, not real-time.
"""

import os
import sys
import time
from datetime import datetime, timedelta, timezone, time as dtime
from pathlib import Path

import pandas as pd
import requests

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from dotenv import load_dotenv
load_dotenv(BACKEND_DIR / ".env")

from .base import PriceProvider

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")
POLYGON_BASE_URL = "https://api.polygon.io"
REQUEST_TIMEOUT = 30
REQUEST_DELAY = 0.1  # unlimited calls on Starter+, small delay is just politeness

ET = ZoneInfo("America/New_York")
# Existing schema/validation (update_hourly_prices.is_valid_hourly_bar) requires
# bars anchored at :30 past the hour (9:30, 10:30, ..., 15:30 ET), matching Yahoo's
# convention. Polygon's native "1 hour" aggregates are :00-anchored (9:00, 10:00,
# ...), so they're resampled here from minute aggregates instead of used directly.
SESSION_OPEN = dtime(9, 30)
SESSION_CLOSE = dtime(16, 0)


def _get_aggs(ticker: str, multiplier: int, timespan: str, start: str, end: str) -> list[dict]:
    """Fetch all pages of aggregate bars for one ticker, one date range."""
    if not POLYGON_API_KEY:
        raise RuntimeError(
            "POLYGON_API_KEY not set. Add it to backend/.env after subscribing at massive.com."
        )

    url = f"{POLYGON_BASE_URL}/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{start}/{end}"
    params = {"adjusted": "true", "sort": "asc", "limit": 50000, "apiKey": POLYGON_API_KEY}
    results: list[dict] = []

    while url:
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            print(f"  [{ticker}] Polygon request failed: {e}")
            break

        if resp.status_code == 429:
            print(f"  [{ticker}] Rate-limited, waiting 15s...")
            time.sleep(15)
            continue
        if resp.status_code != 200:
            print(f"  [{ticker}] Polygon HTTP {resp.status_code}: {resp.text[:200]}")
            break

        payload = resp.json()
        results.extend(payload.get("results", []) or [])

        next_url = payload.get("next_url")
        if next_url:
            url = next_url
            params = {"apiKey": POLYGON_API_KEY}
        else:
            url = None

    return results


def _bars_to_daily_df(bars: list[dict]) -> pd.DataFrame:
    """Convert raw Polygon bars to a daily DataFrame (Yahoo/yfinance column convention)."""
    if not bars:
        return pd.DataFrame()
    rows = []
    for bar in bars:
        rows.append({
            "datetime": pd.to_datetime(bar["t"], unit="ms", utc=True),
            "Open": bar["o"], "High": bar["h"], "Low": bar["l"],
            "Close": bar["c"], "Volume": bar.get("v") or 0,
        })
    return pd.DataFrame(rows).set_index("datetime").sort_index()


def _bars_to_intraday_df(bars: list[dict]) -> pd.DataFrame:
    """Convert raw Polygon bars to an hourly/intraday DataFrame (lowercase column convention)."""
    if not bars:
        return pd.DataFrame()
    rows = []
    for bar in bars:
        rows.append({
            "datetime": pd.to_datetime(bar["t"], unit="ms", utc=True),
            "open": bar["o"], "high": bar["h"], "low": bar["l"],
            "close": bar["c"], "volume": bar.get("v") or 0,
        })
    df = pd.DataFrame(rows).set_index("datetime").sort_index()
    # Defend against duplicate bars from pagination overlap
    return df[~df.index.duplicated(keep="last")]


def _bucket_start(ts_et: datetime) -> datetime:
    """Round down to the start of the 60-min window anchored at 9:30 ET."""
    day_open = ts_et.replace(hour=SESSION_OPEN.hour, minute=SESSION_OPEN.minute, second=0, microsecond=0)
    minutes_in = int((ts_et - day_open).total_seconds() // 60)
    return day_open + timedelta(minutes=60 * (minutes_in // 60))


def _resample_minutes_to_session_hourly(minute_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate 1-minute bars into 9:30-anchored 60-min bars (matches Yahoo's hourly convention)."""
    if minute_df.empty:
        return minute_df

    df = minute_df.copy()
    df.index = [ts.tz_convert(ET) for ts in df.index]
    df = df[[SESSION_OPEN <= ts.time() < SESSION_CLOSE for ts in df.index]]
    if df.empty:
        return df

    df["_bucket"] = [_bucket_start(ts) for ts in df.index]
    hourly = df.groupby("_bucket").agg(
        open=("open", "first"), high=("high", "max"),
        low=("low", "min"), close=("close", "last"),
        volume=("volume", "sum"),
    )
    hourly.index.name = "datetime"
    return hourly.sort_index()


class PolygonProvider(PriceProvider):
    name = "polygon"

    def fetch_daily(self, tickers: list[str], days: int) -> dict[str, pd.DataFrame]:
        end = datetime.now(timezone.utc) + timedelta(days=1)
        start = end - timedelta(days=days + 10)
        start_str, end_str = start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

        result: dict[str, pd.DataFrame] = {}
        for i, ticker in enumerate(tickers, 1):
            print(f"  [{i}/{len(tickers)}] {ticker} (daily)...", end=" ", flush=True)
            bars = _get_aggs(ticker, 1, "day", start_str, end_str)
            df = _bars_to_daily_df(bars)
            if not df.empty:
                result[ticker] = df
                print(f"{len(df)} bars")
            else:
                print("no data")
            time.sleep(REQUEST_DELAY)
        return result

    def fetch_hourly(self, tickers: list[str], days: int) -> dict[str, pd.DataFrame]:
        """Fetch minute aggregates and resample into 9:30-anchored hourly bars.

        Polygon's native 1-hour aggregates are :00-anchored, which doesn't match
        this schema's :30-anchored hourly convention — see module docstring.
        """
        end = datetime.now(timezone.utc) + timedelta(days=1)
        start = end - timedelta(days=days + 2)
        start_str, end_str = start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

        result: dict[str, pd.DataFrame] = {}
        for i, ticker in enumerate(tickers, 1):
            print(f"  [{i}/{len(tickers)}] {ticker} (hourly)...", end=" ", flush=True)
            bars = _get_aggs(ticker, 1, "minute", start_str, end_str)
            minute_df = _bars_to_intraday_df(bars)
            df = _resample_minutes_to_session_hourly(minute_df)
            if not df.empty:
                result[ticker] = df
                print(f"{len(df)} bars")
            else:
                print("no data")
            time.sleep(REQUEST_DELAY)
        return result

    def fetch_intraday(self, tickers: list[str], interval: str, days: int) -> dict[str, pd.DataFrame]:
        minutes = int(interval.removesuffix("m"))
        end = datetime.now(timezone.utc) + timedelta(days=1)
        start = end - timedelta(days=days + 1)
        start_str, end_str = start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

        result: dict[str, pd.DataFrame] = {}
        for i, ticker in enumerate(tickers, 1):
            print(f"  [{i}/{len(tickers)}] {ticker} ({interval})...", end=" ", flush=True)
            bars = _get_aggs(ticker, minutes, "minute", start_str, end_str)
            df = _bars_to_intraday_df(bars)
            if not df.empty:
                result[ticker] = df
                print(f"{len(df)} bars")
            else:
                print("no data")
            time.sleep(REQUEST_DELAY)
        return result

    def supports_intraday(self) -> bool:
        return True
