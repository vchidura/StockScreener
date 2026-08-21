from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, date


class StockPrice(BaseModel):
    datetime: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


class GapResult(BaseModel):
    ticker: str
    gap_type: str
    gap_low: float
    gap_high: float
    last_close: float
    gap_diff: float
    gap_date: Optional[date] = None


class ScanResult(BaseModel):
    scan_datetime: datetime
    strategy: str
    results: List[GapResult]
    total_count: int


class ScreenerRequest(BaseModel):
    tickers: Optional[List[str]] = None
    strategy: str = "gap"
    interval: str = "1D"


class TickerInfo(BaseModel):
    ticker: str
    name: Optional[str] = None
    sector: Optional[str] = None
    market_cap: Optional[float] = None


class ChartDataRequest(BaseModel):
    ticker: str
    interval: str = "1D"
    days: int = 365
