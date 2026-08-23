"""Yahoo Finance provider — wraps the existing Yahoo Chart API fetchers.

No API key, no rate limit, batch requests. Recommended default provider.
"""

import sys
from pathlib import Path

import pandas as pd

BACKEND_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = BACKEND_DIR / "scripts"
for p in (str(BACKEND_DIR), str(SCRIPTS_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

from update_daily_prices import fetch_daily_yahoo_direct
from update_hourly_prices import fetch_hourly_yahoo_direct
from update_intraday_prices import fetch_intraday_yahoo_direct

from .base import PriceProvider


class YahooProvider(PriceProvider):
    name = "yahoo"

    def fetch_daily(self, tickers: list[str], days: int) -> dict[str, pd.DataFrame]:
        return fetch_daily_yahoo_direct(tickers, days)

    def fetch_hourly(self, tickers: list[str], days: int) -> dict[str, pd.DataFrame]:
        return fetch_hourly_yahoo_direct(tickers, days)

    def fetch_intraday(self, tickers: list[str], interval: str, days: int) -> dict[str, pd.DataFrame]:
        return fetch_intraday_yahoo_direct(tickers, interval, days)

    def supports_intraday(self) -> bool:
        return True
