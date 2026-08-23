"""Common interface every market-data provider must implement.

DataFrame contract (matches the existing yahoo_direct fetch functions):
  - index: pandas Timestamp per bar (tz-aware UTC or ET; downstream upsert
    functions normalize timezone before writing)
  - fetch_daily(): columns "Open", "High", "Low", "Close", "Volume" (capitalized,
    matches yfinance/Yahoo Chart convention used by upsert_prices())
  - fetch_hourly()/fetch_intraday(): columns "open", "high", "low", "close",
    "volume" (lowercase, matches upsert_hourly_prices()/upsert_intraday())
"""

from abc import ABC, abstractmethod

import pandas as pd


class PriceProvider(ABC):
    """Base class for a market-data ingestion provider."""

    name: str = "base"

    @abstractmethod
    def fetch_daily(self, tickers: list[str], days: int) -> dict[str, pd.DataFrame]:
        """Return {ticker: DataFrame} of daily OHLCV bars covering the last `days` days."""

    @abstractmethod
    def fetch_hourly(self, tickers: list[str], days: int) -> dict[str, pd.DataFrame]:
        """Return {ticker: DataFrame} of hourly OHLCV bars covering the last `days` days."""

    @abstractmethod
    def fetch_intraday(self, tickers: list[str], interval: str, days: int) -> dict[str, pd.DataFrame]:
        """Return {ticker: DataFrame} of intraday (1m/5m) OHLCV bars."""

    def supports_intraday(self) -> bool:
        """Whether this provider can reasonably serve the 5m scheduler job."""
        return True
