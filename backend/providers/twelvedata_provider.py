"""Twelve Data provider — rate-limited (8 req/min free tier), no 5m support."""

import sys
import time
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = BACKEND_DIR / "scripts"
for p in (str(BACKEND_DIR), str(SCRIPTS_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

from update_daily_prices import fetch_daily_twelvedata
from update_hourly_prices import fetch_hourly_candles

from .base import PriceProvider

TD_RATE_LIMIT_SLEEP = 8  # seconds between single-ticker calls (free tier: 8 req/min)


class TwelveDataProvider(PriceProvider):
    name = "twelvedata"

    def fetch_daily(self, tickers: list[str], days: int) -> dict[str, pd.DataFrame]:
        return fetch_daily_twelvedata(tickers, days)

    def fetch_hourly(self, tickers: list[str], days: int) -> dict[str, pd.DataFrame]:
        result: dict[str, pd.DataFrame] = {}
        for i, ticker in enumerate(tickers, 1):
            df = fetch_hourly_candles(ticker, days)
            if not df.empty:
                result[ticker] = df
            if i < len(tickers):
                time.sleep(TD_RATE_LIMIT_SLEEP)
        return result

    def fetch_intraday(self, tickers: list[str], interval: str, days: int) -> dict[str, pd.DataFrame]:
        return {}

    def supports_intraday(self) -> bool:
        return False
