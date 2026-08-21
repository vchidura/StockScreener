from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional, Any, Callable
from datetime import datetime
import asyncio
import logging
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor

from models import GapResult, ScanResult, ScreenerRequest, StockPrice
from database import (
    get_distinct_tickers,
    get_selected_tickers,
    get_stock_data,
    get_latest_scan_results,
    get_latest_price_date,
    get_latest_quote,
    create_scan_results_table,
    create_selected_tickers_table,
    save_scan_results,
    get_tickers_overview,
    get_bulk_price_data,
    get_hourly_data,
    get_intraday_data,
)
from screeners import (
    scan_gap_strategies,
    scan_fair_value_gaps,
    scan_moving_average_crossover,
    scan_momentum_pullback,
    scan_bearish_bounce,
    scan_fibonacci,
    calculate_fibonacci_swing_pct,
    download_historical_data,
    bulk_load_dataframes,
    analyze_market_regime,
    clear_bulk_cache,
)

app = FastAPI(
    title="Stock Screener API",
    description="API for scanning stocks using various trading strategies",
    version="1.0.0",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("stock-screener-api")

DEFAULT_TEST_TICKERS = ["AAPL", "MSFT", "AMZN", "NVDA", "TSLA"]


def _days_for_swing_pct(pct: float) -> int:
    """Return data window size (calendar days) based on Fibonacci swing %.
    Higher swing % requires more history to find major pivots; lower swing %
    finds small pivots quickly so fewer days suffice.
    Floor is 300 cal-days (~210 trading rows) for Momentum/Bearish."""
    if pct >= 10:
        return 1100
    if pct >= 7:
        return 600
    if pct >= 5:
        return 400
    return 300

# CORS middleware for React frontend
# Production: Set CORS_ORIGINS env var (comma-separated)
cors_origins = os.getenv(
    "CORS_ORIGINS", "http://localhost:5173,http://localhost:5174,http://localhost:5175,http://localhost:3000,http://localhost:80"
).split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Thread pool for parallel scanning — sized to CPU cores
# I/O-bound work (DB queries) benefits from 2× core count
_num_workers = min(os.cpu_count() * 2, 32)
executor = ThreadPoolExecutor(max_workers=_num_workers)
logger.info("ThreadPoolExecutor | workers=%s | cpu_cores=%s", _num_workers, os.cpu_count())

# ── Result-level TTL cache ──────────────────────────────────────────────
# Caches computed endpoint results keyed by (endpoint+params).
# Force refresh (?refresh=true) bypasses and also invalidates bulk_load_dataframes.
_result_cache: dict[str, tuple[float, Any]] = {}
_result_cache_lock = threading.Lock()

# TTL in seconds per cache-key prefix
_CACHE_TTLS: dict[str, int] = {
    "tickers":          86400,   # 24h
    "latest_price":     21600,   # 6h
    "strategies":       86400,   # 24h
    "market_regime":    43200,   # 12h
    "overview":         86400,   # 24h
    "gaps":             14400,   # 4h
    "fvg":              14400,   # 4h
    "ma":               7200,    # 2h
    "momentum":         7200,    # 2h
    "bearish":          7200,    # 2h
    "fibonacci":        21600,   # 6h
    "scan_all":         43200,   # 12h
    "streak":           86400,   # 24h
    "streak_summary":   86400,   # 24h
    "trade_setup":      14400,   # 4h
    "chart_daily":      86400,   # 24h
    "chart_intraday":   60,      # 1min; market-hours data updates every 5min
    "quote":            60,      # newest stored candle
    "prices":           86400,   # 24h
    "calibration":      900,     # 15min; layers land at 9:25/9:30/9:35 AM ET
}


def _get_cached(key: str, prefix: str) -> Any | None:
    """Return cached value if present and not expired, else None."""
    ttl = _CACHE_TTLS.get(prefix, 3600)
    with _result_cache_lock:
        if key in _result_cache:
            ts, val = _result_cache[key]
            if time.time() - ts < ttl:
                logger.info("Cache HIT | key=%s", key)
                return val
            del _result_cache[key]
    return None


def _set_cached(key: str, value: Any) -> None:
    """Store a value in the result cache."""
    with _result_cache_lock:
        _result_cache[key] = (time.time(), value)


def _invalidate_prefix(prefix: str) -> None:
    """Remove all cache entries whose key starts with prefix."""
    with _result_cache_lock:
        to_del = [k for k in _result_cache if k.startswith(prefix)]
        for k in to_del:
            del _result_cache[k]


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.perf_counter()
    logger.info("Request started | %s %s", request.method, request.url.path)
    response = await call_next(request)
    duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
    logger.info(
        "Request completed | %s %s | status=%s | duration_ms=%s",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response


@app.on_event("startup")
async def startup_event():
    """Initialize database tables on startup."""
    create_scan_results_table()
    create_selected_tickers_table()
    logger.info("Startup complete | tables initialized")


def _load_intraday_frames(tickers: List[str], interval: str, limit: int = 200) -> dict:
    """
    Load price data from the appropriate table based on interval.
    Returns dict mapping ticker -> DataFrame with DatetimeIndex and lowercase columns,
    matching the format of bulk_load_dataframes.
    """
    import pandas as pd
    
    frames = {}
    data = get_bulk_price_data(tickers, interval, limit)
    
    for ticker, rows in data.items():
        if rows:
            df = pd.DataFrame(rows)
            df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
            df.set_index('datetime', inplace=True)
            if 'ticker' in df.columns:
                df = df.drop(columns=['ticker'])
            df.columns = [col.lower() for col in df.columns]
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df.columns:
                    df[col] = df[col].astype(float)
            df = df.sort_index()
            frames[ticker] = df
    
    return frames


def resolve_tickers(tickers: Optional[str] = None, limit: Optional[int] = None) -> List[str]:
    """
    Resolve scan tickers in priority order:
    1) explicit tickers query param
    2) active rows in selected_tickers table
    3) small default list for initial testing
    """
    if tickers:
        resolved = [t.strip().upper() for t in tickers.split(",") if t.strip()]
        source = "request"
    else:
        selected = get_selected_tickers(active_only=True)
        if selected:
            resolved = [t.upper() for t in selected]
            source = "selected_tickers"
        else:
            resolved = DEFAULT_TEST_TICKERS.copy()
            source = "fallback_default"

    if limit is not None:
        resolved = resolved[:limit]

    logger.info("Ticker selection | source=%s | count=%s", source, len(resolved))
    return resolved


@app.get("/")
async def root():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.get("/api/tickers", response_model=List[str])
async def list_tickers(refresh: bool = False):
    """List all available tickers in the database."""
    cache_key = "tickers"
    if not refresh:
        cached = _get_cached(cache_key, "tickers")
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()
    selected = get_selected_tickers(active_only=True)
    result = selected if selected else get_distinct_tickers()
    _set_cached(cache_key, result)
    return result


@app.get("/api/latest-price-date")
async def latest_price_date(refresh: bool = False):
    """Return the latest trading date available in the database."""
    cache_key = "latest_price"
    if not refresh:
        cached = _get_cached(cache_key, "latest_price")
        if cached is not None:
            return cached
    date = get_latest_price_date()
    result = {"latest_date": date}
    _set_cached(cache_key, result)
    return result


@app.get("/api/stock/{ticker}/prices")
async def get_ticker_prices(ticker: str, days: int = Query(default=365, ge=1, le=3650), refresh: bool = False):
    """Get historical price data for a ticker."""
    cache_key = f"prices_{ticker.upper()}_{days}"
    if not refresh:
        cached = _get_cached(cache_key, "prices")
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()
    data = get_stock_data(ticker.upper(), days)
    if not data:
        raise HTTPException(status_code=404, detail=f"No data found for ticker {ticker}")
    _set_cached(cache_key, data)
    return data


@app.get("/api/stock/{ticker}/quote")
async def get_ticker_quote(ticker: str, refresh: bool = False):
    """Get the newest stored price, independent of chart interval."""
    symbol = ticker.upper()
    cache_key = f"quote_{symbol}"
    if not refresh:
        cached = _get_cached(cache_key, "quote")
        if cached is not None:
            return cached
    quote = get_latest_quote(symbol)
    if quote is None:
        raise HTTPException(status_code=404, detail=f"No price found for ticker {ticker}")
    _set_cached(cache_key, quote)
    return quote


@app.get("/api/stock/{ticker}/chart")
async def get_chart_data(ticker: str, period: str = "1y", interval: str = "1d", refresh: bool = False):
    """Get chart data for a ticker. Uses DB first, falls back to Yahoo."""
    prefix = "chart_daily" if interval in ("1d", "1wk") else "chart_intraday"
    cache_key = f"{prefix}_{ticker.upper()}_{interval}_{period}"
    if not refresh:
        cached = _get_cached(cache_key, prefix)
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()
    df = download_historical_data(
        ticker.upper(),
        period=period,
        interval=interval,
        prefer_db=True,
    )
    if df is None or df.empty:
        # Return empty array for intraday intervals (data may not be available yet)
        # Only 404 for daily/weekly where data should always exist
        if interval not in ("1d", "1wk"):
            return []
        raise HTTPException(status_code=404, detail=f"No data found for ticker {ticker}")

    calendar_period_days = {
        "1mo": 30,
        "3mo": 90,
        "6mo": 180,
        "1y": 365,
        "2y": 730,
        "4y": 1460,
        "5y": 1825,
    }
    if period in calendar_period_days:
        import pandas as pd

        cutoff = (df.index.max() - pd.Timedelta(days=calendar_period_days[period])).normalize()
        df = df[df.index >= cutoff]
    
    # Convert to list of OHLCV records
    df.columns = [col.lower() for col in df.columns]
    records = []
    for idx, row in df.iterrows():
        records.append({
            "time": int(idx.timestamp()),
            "open": round(float(row["open"]), 2),
            "high": round(float(row["high"]), 2),
            "low": round(float(row["low"]), 2),
            "close": round(float(row["close"]), 2),
            "volume": int(row["volume"]) if "volume" in row else 0
        })
    _set_cached(cache_key, records)
    return records


@app.get("/api/scan/gaps")
async def scan_gaps(
    tickers: Optional[str] = None,
    scan_date: Optional[str] = None,
    interval: str = Query(default="1d", regex="^(5m|15m|30m|1h|1d)$"),
    refresh: bool = False,
):
    """
    Scan for gap strategies.
    Pass tickers as comma-separated string, or leave empty to scan all.
    Pass scan_date as YYYY-MM-DD to run the scan as of that historical date.
    interval: 5m, 15m, 30m, 1h, 1d (default)
    """
    cache_key = f"gaps_{interval}_{scan_date or 'latest'}"
    if not refresh:
        cached = _get_cached(cache_key, "gaps")
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()

    ticker_list = resolve_tickers(tickers)

    logger.info("Gap scan started | tickers=%s | scan_date=%s | interval=%s", len(ticker_list), scan_date or 'latest', interval)
    scan_datetime = datetime.now()

    # Single bulk DB query → in-memory processing
    loop = asyncio.get_event_loop()
    if interval == "1d":
        frames = await loop.run_in_executor(executor, bulk_load_dataframes, ticker_list, 365, scan_date)
    else:
        frames = await loop.run_in_executor(executor, _load_intraday_frames, ticker_list, interval, 365)

    results = []
    for ticker in ticker_list:
        df = frames.get(ticker)
        if df is not None and len(df) >= 20:
            gaps = scan_gap_strategies(ticker, df)
            results.extend(gaps)

    # Group results by gap_type
    grouped: dict = {}
    for r in results:
        gap_type = r["gap_type"]
        if gap_type not in grouped:
            grouped[gap_type] = []
        grouped[gap_type].append(r)

    logger.info(
        "Gap scan completed | scanned=%s | signals=%s | groups=%s",
        len(ticker_list),
        len(results),
        len(grouped),
    )

    result = {
        "scan_datetime": scan_datetime.isoformat(),
        "interval": interval,
        "total_scanned": len(ticker_list),
        "total_signals": len(results),
        "results_by_type": grouped,
        "results": results
    }
    _set_cached(cache_key, result)
    return result


@app.get("/api/scan/fvg")
async def scan_fvg_endpoint(
    tickers: Optional[str] = None,
    scan_date: Optional[str] = None,
    lookback: int = Query(default=50, ge=10, le=200),
    interval: str = Query(default="1d", regex="^(5m|15m|30m|1h|1d)$"),
    refresh: bool = False,
):
    """Scan for Fair Value Gaps (3-candle imbalance zones).

    Identifies bullish and bearish FVGs with mitigation status and streak analysis.
    
    interval: 5m, 15m, 30m, 1h, 1d (default)
    lookback: Number of bars to scan (default 50)
    """
    cache_key = f"fvg_{interval}_{scan_date or 'latest'}_{lookback}"
    if not refresh:
        cached = _get_cached(cache_key, "fvg")
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()

    ticker_list = resolve_tickers(tickers)

    logger.info("FVG scan started | tickers=%s | scan_date=%s | interval=%s | lookback=%s",
                len(ticker_list), scan_date or 'latest', interval, lookback)
    scan_datetime = datetime.now()

    loop = asyncio.get_event_loop()
    if interval == "1d":
        frames = await loop.run_in_executor(executor, bulk_load_dataframes, ticker_list, max(365, lookback + 50), scan_date)
    else:
        frames = await loop.run_in_executor(executor, _load_intraday_frames, ticker_list, interval, max(200, lookback + 50))

    results = []
    for ticker in ticker_list:
        df = frames.get(ticker)
        if df is not None and len(df) >= 20:
            fvgs = scan_fair_value_gaps(ticker, df, lookback=lookback)
            results.extend(fvgs)

    # Group by fvg_type
    grouped: dict = {}
    for r in results:
        fvg_type = r["fvg_type"]
        if fvg_type not in grouped:
            grouped[fvg_type] = []
        grouped[fvg_type].append(r)

    logger.info("FVG scan completed | scanned=%s | signals=%s", len(ticker_list), len(results))

    result = {
        "scan_datetime": scan_datetime.isoformat(),
        "interval": interval,
        "lookback": lookback,
        "total_scanned": len(ticker_list),
        "total_signals": len(results),
        "results_by_type": grouped,
        "results": results,
    }
    _set_cached(cache_key, result)
    return result


@app.get("/api/scan/ma-crossover")
async def scan_ma_crossover(
    tickers: Optional[str] = None,
    short_period: int = Query(default=9, ge=2, le=50),
    long_period: int = Query(default=21, ge=5, le=200),
    scan_date: Optional[str] = None,
    interval: str = Query(default="1d", regex="^(5m|15m|30m|1h|1d)$"),
    refresh: bool = False,
):
    """
    Scan for moving average crossover signals.
    
    Args:
        interval: Timeframe for analysis
            - '1d': Daily candles (default)
            - '1h': Hourly candles
            - '30m'/'15m'/'5m': Intraday candles
    """
    cache_key = f"ma_{interval}_{scan_date or 'latest'}_{short_period}_{long_period}"
    if not refresh:
        cached = _get_cached(cache_key, "ma")
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()

    ticker_list = resolve_tickers(tickers)

    logger.info(
        "MA scan started | tickers=%s | short=%s | long=%s | scan_date=%s | interval=%s",
        len(ticker_list),
        short_period,
        long_period,
        scan_date or 'latest',
        interval,
    )

    loop = asyncio.get_event_loop()
    
    # Load data based on interval
    if interval == "1d":
        frames = await loop.run_in_executor(executor, bulk_load_dataframes, ticker_list, 500, scan_date)
    else:
        # For intraday, use the appropriate data loader
        frames = await loop.run_in_executor(
            executor, 
            _load_intraday_frames, 
            ticker_list, 
            interval, 
            max(long_period * 3, 200)  # Enough candles for MA calculation
        )

    results = []
    for ticker in ticker_list:
        df = frames.get(ticker)
        if df is not None and len(df) >= long_period + 5:
            r = scan_moving_average_crossover(ticker, df, short_period, long_period, interval=interval)
            if r:
                # Add interval info to result
                r["interval"] = interval
                results.append(r)

    # Group by signal type
    grouped: dict = {}
    for r in results:
        sig = r["signal"]
        if sig not in grouped:
            grouped[sig] = []
        grouped[sig].append(r)

    # Sort each group: crossovers by days_since_cross, others by ma_spread_pct
    for sig, items in grouped.items():
        if "Crossover" in sig or "Recent" in sig:
            items.sort(key=lambda x: x.get("days_since_cross") or 0)
        else:
            items.sort(key=lambda x: abs(x.get("ma_spread_pct") or 0), reverse=True)

    logger.info(
        "MA scan completed | scanned=%s | signals=%s | groups=%s",
        len(ticker_list),
        len(results),
        {k: len(v) for k, v in grouped.items()},
    )

    result = {
        "scan_datetime": datetime.now().isoformat(),
        "interval": interval,
        "total_scanned": len(ticker_list),
        "total_signals": len(results),
        "short_period": short_period,
        "long_period": long_period,
        "results_by_signal": grouped,
        "results": results,
    }
    _set_cached(cache_key, result)
    return result


@app.get("/api/scan/momentum-pullback")
async def scan_momentum(
    tickers: Optional[str] = None,
    scan_date: Optional[str] = None,
    interval: str = Query(default="1d", regex="^(5m|15m|30m|1h|1d)$"),
    refresh: bool = False,
):
    """Scan for elite momentum pullback setups.

    Finds stocks in strong uptrends (EMA stack + weekly confirmation + above
    200 SMA) that are experiencing a temporary pullback into the buy zone
    (stochastic < 40, ADX 20-60, within 1 ATR of EMA 21).
    
    interval: 5m, 15m, 30m, 1h, 1d (default)
    """
    cache_key = f"momentum_{interval}_{scan_date or 'latest'}"
    if not refresh:
        cached = _get_cached(cache_key, "momentum")
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()

    ticker_list = resolve_tickers(tickers)

    logger.info("Momentum pullback scan started | tickers=%s | scan_date=%s | interval=%s", len(ticker_list), scan_date or 'latest', interval)

    loop = asyncio.get_event_loop()
    if interval == "1d":
        frames = await loop.run_in_executor(executor, bulk_load_dataframes, ticker_list, 500, scan_date)
    else:
        frames = await loop.run_in_executor(executor, _load_intraday_frames, ticker_list, interval, 500)

    min_bars = 100 if interval in ("5m", "15m", "30m") else (200 if interval == "1h" else 210)
    results = []
    for ticker in ticker_list:
        df = frames.get(ticker)
        if df is not None and len(df) >= min_bars:
            r = scan_momentum_pullback(ticker, df, interval=interval)
            if r:
                results.append(r)

    # Sort by score descending (best setups first)
    results.sort(key=lambda x: x["score"], reverse=True)

    logger.info(
        "Momentum pullback scan completed | scanned=%s | signals=%s",
        len(ticker_list),
        len(results),
    )

    result = {
        "scan_datetime": datetime.now().isoformat(),
        "interval": interval,
        "total_scanned": len(ticker_list),
        "total_signals": len(results),
        "results": results,
    }
    _set_cached(cache_key, result)
    return result


@app.get("/api/scan/bearish-bounce")
async def scan_bearish_bounce_endpoint(
    tickers: Optional[str] = None,
    scan_date: Optional[str] = None,
    interval: str = Query(default="1d", regex="^(5m|15m|30m|1h|1d)$"),
    refresh: bool = False,
):
    """Scan for bearish bounce setups.

    Finds stocks in confirmed downtrends bouncing up toward resistance
    (stochastic > 60, ADX 15-55, within 2 ATR of EMA 21).
    
    interval: 5m, 15m, 30m, 1h, 1d (default)
    """
    cache_key = f"bearish_{interval}_{scan_date or 'latest'}"
    if not refresh:
        cached = _get_cached(cache_key, "bearish")
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()

    ticker_list = resolve_tickers(tickers)

    logger.info("Bearish bounce scan started | tickers=%s | scan_date=%s | interval=%s", len(ticker_list), scan_date or 'latest', interval)

    loop = asyncio.get_event_loop()
    if interval == "1d":
        frames = await loop.run_in_executor(executor, bulk_load_dataframes, ticker_list, 500, scan_date)
    else:
        frames = await loop.run_in_executor(executor, _load_intraday_frames, ticker_list, interval, 500)

    min_bars = 100 if interval in ("5m", "15m", "30m") else (200 if interval == "1h" else 210)
    results = []
    for ticker in ticker_list:
        df = frames.get(ticker)
        if df is not None and len(df) >= min_bars:
            r = scan_bearish_bounce(ticker, df, interval=interval)
            if r:
                results.append(r)

    results.sort(key=lambda x: x["score"], reverse=True)

    logger.info(
        "Bearish bounce scan completed | scanned=%s | signals=%s",
        len(ticker_list),
        len(results),
    )

    result = {
        "scan_datetime": datetime.now().isoformat(),
        "interval": interval,
        "total_scanned": len(ticker_list),
        "total_signals": len(results),
        "results": results,
    }
    _set_cached(cache_key, result)
    return result


@app.get("/api/scan/fibonacci")
async def scan_fibonacci_endpoint(
    tickers: Optional[str] = None,
    scan_date: Optional[str] = None,
    min_swing_pct: float = Query(default=5.0, ge=3.0, le=15.0),
    interval: str = Query(default="1d", regex="^(5m|15m|30m|1h|1d)$"),
    refresh: bool = False,
):
    """Scan for Fibonacci retracement levels using zigzag pivot detection.
    
    interval: 5m, 15m, 30m, 1h, 1d (default)
    """
    cache_key = f"fibonacci_{interval}_{scan_date or 'latest'}_{min_swing_pct}"
    if not refresh:
        cached = _get_cached(cache_key, "fibonacci")
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()

    ticker_list = resolve_tickers(tickers)

    logger.info("Fibonacci scan started | tickers=%s | scan_date=%s | min_swing=%.1f%% | interval=%s",
                len(ticker_list), scan_date or 'latest', min_swing_pct, interval)

    loop = asyncio.get_event_loop()
    if interval == "1d":
        frames = await loop.run_in_executor(executor, bulk_load_dataframes, ticker_list, 1100, scan_date)
    else:
        frames = await loop.run_in_executor(executor, _load_intraday_frames, ticker_list, interval, 1100)

    results = []
    for ticker in ticker_list:
        df = frames.get(ticker)
        if df is not None and len(df) >= 50:
            r = scan_fibonacci(ticker, df, min_swing_pct)
            if r:
                results.append(r)

    # Sort: near-level signals first, then by absolute distance
    signal_priority = {"Near": 0, "Between": 1, "Below": 2, "Above": 2}
    results.sort(key=lambda x: (
        signal_priority.get(x["signal"].split()[0], 3),
        abs(x["distance_pct"]),
    ))

    logger.info("Fibonacci scan completed | scanned=%s | signals=%s",
                len(ticker_list), len(results))

    result = {
        "scan_datetime": datetime.now().isoformat(),
        "interval": interval,
        "total_scanned": len(ticker_list),
        "total_signals": len(results),
        "min_swing_pct": min_swing_pct,
        "results": results,
    }
    _set_cached(cache_key, result)
    return result


@app.get("/api/market-regime")
async def market_regime_endpoint(refresh: bool = False):
    """Analyze SPY + QQQ to determine overall market regime."""
    cache_key = "market_regime"
    if not refresh:
        cached = _get_cached(cache_key, "market_regime")
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()
    loop = asyncio.get_event_loop()
    idx_frames = await loop.run_in_executor(executor, bulk_load_dataframes, ["SPY", "QQQ"], 1600)
    spy_df = idx_frames.get("SPY")
    qqq_df = idx_frames.get("QQQ")
    regime = analyze_market_regime(spy_df, qqq_df)
    _set_cached(cache_key, regime)
    return regime


@app.get("/api/scan/all")
async def scan_all(
    tickers: Optional[str] = None,
    scan_date: Optional[str] = None,
    min_swing_pct: float = Query(default=5.0, ge=3.0, le=15.0),
    refresh: bool = False,
):
    """Run all 5 scans in a single request with one shared data load.
    Returns combined results keyed by strategy name."""
    cache_key = f"scan_all_{scan_date or 'latest'}_{min_swing_pct}"
    if not refresh:
        cached = _get_cached(cache_key, "scan_all")
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()

    ticker_list = resolve_tickers(tickers)
    scan_datetime = datetime.now()
    data_days = _days_for_swing_pct(min_swing_pct)
    logger.info("Combined scan started | tickers=%s | scan_date=%s | days=%s (swing_pct=%.1f)",
                len(ticker_list), scan_date or 'latest', data_days, min_swing_pct)

    loop = asyncio.get_event_loop()
    # Load user tickers at normal days; load SPY/QQQ at 1600 days for weekly 200 SMA
    frames = await loop.run_in_executor(executor, bulk_load_dataframes, ticker_list, data_days, scan_date)
    idx_frames = await loop.run_in_executor(executor, bulk_load_dataframes, ["SPY", "QQQ"], 1600, scan_date)
    frames.update(idx_frames)

    gap_results = []
    ma_results = []
    momentum_results = []
    bearish_results = []
    fib_results = []

    for ticker in ticker_list:
        df = frames.get(ticker)
        if df is None:
            continue
        if len(df) >= 20:
            gap_results.extend(scan_gap_strategies(ticker, df))
        if len(df) >= 26:
            r = scan_moving_average_crossover(ticker, df)
            if r:
                ma_results.append(r)
        if len(df) >= 210:
            r = scan_momentum_pullback(ticker, df)
            if r:
                momentum_results.append(r)
            r2 = scan_bearish_bounce(ticker, df)
            if r2:
                bearish_results.append(r2)
        if len(df) >= 50:
            r = scan_fibonacci(ticker, df, min_swing_pct)
            if r:
                fib_results.append(r)

    # Sort MA by signal priority
    ma_signal_order = {"Bullish Crossover": 0, "Bearish Crossover": 1, "Recent Bullish": 2, "Recent Bearish": 3}
    ma_results.sort(key=lambda x: ma_signal_order.get(x["signal"], 5))

    # Sort momentum/bearish by score desc
    momentum_results.sort(key=lambda x: x.get("score", 0), reverse=True)
    bearish_results.sort(key=lambda x: x.get("score", 0), reverse=True)

    # Sort fib by proximity
    signal_priority = {"Near": 0, "Between": 1, "Below": 2, "Above": 2}
    fib_results.sort(key=lambda x: (signal_priority.get(x["signal"].split()[0], 3), abs(x["distance_pct"])))

    # Market regime analysis using SPY + QQQ
    spy_df = frames.get("SPY")
    qqq_df = frames.get("QQQ")
    regime = analyze_market_regime(spy_df, qqq_df)

    logger.info("Combined scan completed | gaps=%s | ma=%s | momentum=%s | bearish=%s | fib=%s | regime=%s",
                len(gap_results), len(ma_results), len(momentum_results), len(bearish_results), len(fib_results), regime["regime"])

    result = {
        "scan_datetime": scan_datetime.isoformat(),
        "total_scanned": len(ticker_list),
        "market_regime": regime,
        "gaps": {"total_signals": len(gap_results), "results": gap_results},
        "ma_crossover": {"total_signals": len(ma_results), "results": ma_results},
        "momentum_pullback": {"total_signals": len(momentum_results), "results": momentum_results},
        "bearish_bounce": {"total_signals": len(bearish_results), "results": bearish_results},
        "fibonacci": {"total_signals": len(fib_results), "results": fib_results, "min_swing_pct": min_swing_pct},
    }
    _set_cached(cache_key, result)
    return result


@app.get("/api/tickers/overview")
async def tickers_overview(scan_date: Optional[str] = None, refresh: bool = False):
    """Return OHLC + daily MAs + true weekly MAs for all selected tickers.
    Pass scan_date as YYYY-MM-DD for historical data."""
    cache_key = f"overview_{scan_date or 'latest'}"
    if not refresh:
        cached = _get_cached(cache_key, "overview")
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()

    selected = get_selected_tickers(active_only=True)
    if not selected:
        selected = get_distinct_tickers()
    logger.info("Tickers overview requested | count=%s | scan_date=%s", len(selected), scan_date or 'latest')

    loop = asyncio.get_event_loop()
    results = await loop.run_in_executor(executor, get_tickers_overview, selected, scan_date)
    _set_cached(cache_key, results)
    return results


@app.get("/api/scan/streak")
async def scan_streak_endpoint(
    strategy: str = Query(..., description="Strategy: gaps, ma-crossover, momentum-pullback, bearish-bounce, fibonacci"),
    days: int = Query(default=5, ge=2, le=10),
    tickers: Optional[str] = None,
    short_period: int = Query(default=9, ge=2, le=50),
    long_period: int = Query(default=21, ge=5, le=200),
    refresh: bool = False,
):
    """Run a strategy scan across the last N trading days and return ticker consistency.

    Shows which tickers appeared in the scan results on each of the last N days,
    sorted by how many days they matched (most consistent first).
    """
    valid = {"gaps", "ma-crossover", "momentum-pullback", "bearish-bounce", "fibonacci"}
    if strategy not in valid:
        raise HTTPException(status_code=400, detail=f"Invalid strategy. Must be one of: {', '.join(sorted(valid))}")

    cache_key = f"streak_{strategy}_{days}_{short_period}_{long_period}"
    if not refresh:
        cached = _get_cached(cache_key, "streak")
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()

    ticker_list = resolve_tickers(tickers)
    min_rows_map = {
        "gaps": 20,
        "ma-crossover": long_period + 5,
        "momentum-pullback": 210,
        "bearish-bounce": 210,
        "fibonacci": 50,
    }
    min_rows = min_rows_map[strategy]

    logger.info(
        "Streak scan started | strategy=%s | days=%s | tickers=%s",
        strategy, days, len(ticker_list),
    )

    loop = asyncio.get_event_loop()
    load_days = 1100 if strategy == "fibonacci" else 520
    frames = await loop.run_in_executor(executor, bulk_load_dataframes, ticker_list, load_days)

    # Find last N unique trading dates across all tickers
    date_set: set = set()
    for df in frames.values():
        date_set.update(df.index.normalize().to_list())
    if not date_set:
        return {
            "strategy": strategy, "streak_days": 0, "scan_dates": [],
            "total_scanned": 0, "total_with_signals": 0, "results": [],
        }

    trading_dates = sorted(date_set, reverse=True)[:days]

    # For each trading day, truncate frames and run the scan
    ticker_dates: dict = {}  # {ticker: set of date strings}
    # For gaps strategy: collect rich per-day detail
    gap_daily_detail: dict = {}  # {ticker: {date_str: {gap_types, gap_count, ...}}}
    # For ma-crossover strategy: collect rich per-day detail
    ma_daily_detail: dict = {}  # {ticker: {date_str: {signal, ma_spread_pct, ...}}}
    # For fibonacci strategy: collect rich per-day detail
    fib_daily_detail: dict = {}  # {ticker: {date_str: {signal, nearest_level, ...}}}

    for target_dt in trading_dates:
        date_str = str(target_dt.date()) if hasattr(target_dt, 'date') else str(target_dt)[:10]
        for ticker, df in frames.items():
            truncated = df[df.index <= target_dt]
            if len(truncated) < min_rows:
                continue

            matched = False
            try:
                if strategy == "gaps":
                    gap_results = scan_gap_strategies(ticker, truncated)
                    matched = bool(gap_results)
                    if matched:
                        # Collect rich detail for each gap signal on this day
                        gap_types = [r["gap_type"] for r in gap_results]
                        last_close = gap_results[0]["last_close"]
                        # Gap age: trading days between gap_date and target_dt
                        gap_ages = []
                        gap_distances = []
                        for r in gap_results:
                            gd = r.get("gap_date", "")
                            if gd:
                                try:
                                    from datetime import datetime as _dt
                                    gap_dt = _dt.strptime(gd, "%Y-%m-%d")
                                    target_plain = target_dt.date() if hasattr(target_dt, 'date') else target_dt
                                    if hasattr(target_plain, 'strftime'):
                                        age = (target_plain - gap_dt.date()).days
                                    else:
                                        age = 0
                                    gap_ages.append(age)
                                except Exception:
                                    gap_ages.append(None)
                            # Distance from price to gap zone midpoint (%)
                            mid = (r["gap_low"] + r["gap_high"]) / 2
                            if mid > 0:
                                dist = round((last_close - mid) / mid * 100, 2)
                            else:
                                dist = 0.0
                            gap_distances.append(dist)
                        # Volume ratio: last day volume vs 20-day avg
                        vol_ratio = None
                        if "volume" in truncated.columns:
                            vol_arr = truncated["volume"].values
                            if len(vol_arr) >= 20:
                                avg20 = float(vol_arr[-20:].mean())
                                if avg20 > 0:
                                    vol_ratio = round(float(vol_arr[-1]) / avg20, 2)
                        # Freshest gap age
                        valid_ages = [a for a in gap_ages if a is not None]
                        freshest_age = min(valid_ages) if valid_ages else None
                        # Nearest gap distance (absolute)
                        nearest_dist = min(gap_distances, key=abs) if gap_distances else None
                        # Detect new gaps (formed on this specific day)
                        new_gap_count = sum(1 for a in valid_ages if a == 0)

                        if ticker not in gap_daily_detail:
                            gap_daily_detail[ticker] = {}
                        gap_daily_detail[ticker][date_str] = {
                            "gap_types": gap_types,
                            "gap_count": len(gap_results),
                            "freshest_gap_age": freshest_age,
                            "nearest_distance_pct": nearest_dist,
                            "new_gaps": new_gap_count,
                            "volume_ratio": vol_ratio,
                            "last_close": round(last_close, 2),
                        }
                elif strategy == "ma-crossover":
                    r = scan_moving_average_crossover(ticker, truncated, short_period, long_period)
                    matched = r is not None and r["signal"] in (
                        "Bullish Crossover", "Recent Bullish", "Bearish Crossover", "Recent Bearish"
                    )
                    if matched and r is not None:
                        # Volume ratio
                        vol_ratio = None
                        if "volume" in truncated.columns:
                            vol_arr = truncated["volume"].values
                            if len(vol_arr) >= 20:
                                avg20 = float(vol_arr[-20:].mean())
                                if avg20 > 0:
                                    vol_ratio = round(float(vol_arr[-1]) / avg20, 2)
                        if ticker not in ma_daily_detail:
                            ma_daily_detail[ticker] = {}
                        ma_daily_detail[ticker][date_str] = {
                            "signal": r["signal"],
                            "ma_spread_pct": r["ma_spread_pct"],
                            "price_vs_short_pct": r["price_vs_short_pct"],
                            "price_vs_long_pct": r["price_vs_long_pct"],
                            "days_since_cross": r["days_since_cross"],
                            "price_change_since_cross_pct": r.get("price_change_since_cross_pct"),
                            "volume_ratio": vol_ratio,
                            "markers": r.get("markers", []),
                            "last_close": r["last_close"],
                            "weekly_signal": r.get("weekly_signal"),
                            "weekly_spread_pct": r.get("weekly_spread_pct"),
                        }
                elif strategy == "momentum-pullback":
                    r = scan_momentum_pullback(ticker, truncated)
                    matched = r is not None
                elif strategy == "bearish-bounce":
                    r = scan_bearish_bounce(ticker, truncated)
                    matched = r is not None
                elif strategy == "fibonacci":
                    r = scan_fibonacci(ticker, truncated)
                    matched = r is not None and r["signal"] != "Between Levels"
                    if matched and r is not None:
                        vol_ratio = None
                        if "volume" in truncated.columns:
                            vol_arr = truncated["volume"].values
                            if len(vol_arr) >= 20:
                                avg20 = float(vol_arr[-20:].mean())
                                if avg20 > 0:
                                    vol_ratio = round(float(vol_arr[-1]) / avg20, 2)
                        if ticker not in fib_daily_detail:
                            fib_daily_detail[ticker] = {}
                        fib_daily_detail[ticker][date_str] = {
                            "signal": r["signal"],
                            "nearest_level": r["nearest_level"],
                            "distance_pct": r["distance_pct"],
                            "retracement_pct": r["retracement_pct"],
                            "zone": r["zone"],
                            "trend_direction": r["trend_direction"],
                            "swing_high": r["swing_high"],
                            "swing_low": r["swing_low"],
                            "swing_size_pct": r["swing_size_pct"],
                            "last_close": r["last_close"],
                            "volume_ratio": vol_ratio,
                        }
            except Exception:
                pass

            if matched:
                if ticker not in ticker_dates:
                    ticker_dates[ticker] = set()
                ticker_dates[ticker].add(date_str)

    scan_dates = [str(d.date()) if hasattr(d, 'date') else str(d)[:10] for d in trading_dates]
    num_days = len(trading_dates)

    results = []
    for ticker, dates_matched in ticker_dates.items():
        matched_count = len(dates_matched)
        entry = {
            "ticker": ticker,
            "days_matched": matched_count,
            "total_days": num_days,
            "consistency": round(matched_count / num_days * 100),
            "dates_matched": sorted(dates_matched),
        }

        # Enrich gap results with analysis data
        if strategy == "gaps" and ticker in gap_daily_detail:
            daily = gap_daily_detail[ticker]
            sorted_dates = sorted(daily.keys())

            # 1. Freshness: based on freshest gap age on the most recent day
            latest_day = sorted_dates[-1] if sorted_dates else None
            latest_detail = daily.get(latest_day, {})
            freshest_age = latest_detail.get("freshest_gap_age")
            if freshest_age is not None:
                if freshest_age <= 3:
                    freshness = "Fresh"
                elif freshest_age <= 15:
                    freshness = "Aging"
                else:
                    freshness = "Stale"
            else:
                freshness = "Unknown"

            # 2. Fill progress: track nearest_distance_pct across days
            distances = [daily[d].get("nearest_distance_pct") for d in sorted_dates if daily[d].get("nearest_distance_pct") is not None]
            if len(distances) >= 2:
                abs_first = abs(distances[0])
                abs_last = abs(distances[-1])
                if abs_last < abs_first * 0.8:
                    fill_progress = "Converging"
                elif abs_last > abs_first * 1.2:
                    fill_progress = "Diverging"
                else:
                    fill_progress = "Stable"
            else:
                fill_progress = "N/A"

            # 3. New gaps formed in the streak window
            total_new = sum(daily[d].get("new_gaps", 0) for d in sorted_dates)

            # 4. Gap type transitions (simplified: dominant type per day)
            type_sequence = []
            for d in sorted_dates:
                types = daily[d].get("gap_types", [])
                # Classify the day
                has_in_gap = any("In Gap" in t for t in types)
                has_unfilled = any("Unfilled" in t for t in types)
                has_support = any("At Support" in t for t in types)
                has_resist = any("At Resistance" in t for t in types)
                if has_in_gap:
                    type_sequence.append("In Gap")
                elif has_support or has_resist:
                    type_sequence.append("At Edge")
                elif has_unfilled:
                    type_sequence.append("Unfilled")
                else:
                    type_sequence.append("Signal")
            # Detect if status changed
            unique_statuses = list(dict.fromkeys(type_sequence))
            if len(unique_statuses) == 1:
                transition_summary = f"Steady ({unique_statuses[0]})"
            else:
                transition_summary = " → ".join(type_sequence)

            # 5. Volume: average volume ratio across streak days
            vol_ratios = [daily[d].get("volume_ratio") for d in sorted_dates if daily[d].get("volume_ratio") is not None]
            avg_vol_ratio = round(sum(vol_ratios) / len(vol_ratios), 2) if vol_ratios else None

            entry["gap_analysis"] = {
                "freshest_gap_age": freshest_age,
                "freshness": freshness,
                "fill_progress": fill_progress,
                "fill_distances": distances,
                "new_gaps_in_window": total_new,
                "type_sequence": type_sequence,
                "transition_summary": transition_summary,
                "avg_volume_ratio": avg_vol_ratio,
                "daily_details": {d: daily[d] for d in sorted_dates},
            }

        # Enrich MA crossover results with analysis data
        if strategy == "ma-crossover" and ticker in ma_daily_detail:
            daily = ma_daily_detail[ticker]
            sorted_dates = sorted(daily.keys())

            # 1. Direction: consistency of bullish vs bearish signals
            signals = [daily[d]["signal"] for d in sorted_dates]
            bullish_count = sum(1 for s in signals if "Bullish" in s)
            bearish_count = sum(1 for s in signals if "Bearish" in s)
            if bullish_count > 0 and bearish_count == 0:
                direction = "Bullish"
            elif bearish_count > 0 and bullish_count == 0:
                direction = "Bearish"
            else:
                direction = "Mixed"

            # 2. Spread trend: widening vs narrowing MA spread
            spreads = [daily[d]["ma_spread_pct"] for d in sorted_dates if daily[d].get("ma_spread_pct") is not None]
            if len(spreads) >= 2:
                abs_first = abs(spreads[0])
                abs_last = abs(spreads[-1])
                if abs_last > abs_first * 1.2:
                    spread_trend = "Widening"
                elif abs_last < abs_first * 0.8:
                    spread_trend = "Narrowing"
                else:
                    spread_trend = "Stable"
            else:
                spread_trend = "N/A"

            # 3. Price momentum since crossover
            pct_changes = [daily[d].get("price_change_since_cross_pct") for d in sorted_dates if daily[d].get("price_change_since_cross_pct") is not None]
            if len(pct_changes) >= 2:
                first_chg = pct_changes[0]
                last_chg = pct_changes[-1]
                # Both same sign and magnitude increasing = accelerating
                if direction == "Bullish":
                    if last_chg > first_chg + 1:
                        price_momentum = "Accelerating"
                    elif last_chg < first_chg - 1:
                        price_momentum = "Stalling"
                    else:
                        price_momentum = "Steady"
                elif direction == "Bearish":
                    if last_chg < first_chg - 1:
                        price_momentum = "Accelerating"
                    elif last_chg > first_chg + 1:
                        price_momentum = "Stalling"
                    else:
                        price_momentum = "Steady"
                else:
                    price_momentum = "Choppy"
            else:
                price_momentum = "N/A"

            # 4. Signal flow: signal transitions across days
            signal_sequence = signals
            unique_signals = list(dict.fromkeys(signal_sequence))
            if len(unique_signals) == 1:
                signal_flow = f"Steady ({unique_signals[0]})"
            else:
                signal_flow = " → ".join(signal_sequence)

            # 5. Volume: average volume ratio
            vol_ratios = [daily[d].get("volume_ratio") for d in sorted_dates if daily[d].get("volume_ratio") is not None]
            avg_vol_ratio = round(sum(vol_ratios) / len(vol_ratios), 2) if vol_ratios else None

            # Latest crossover age
            latest_day = sorted_dates[-1]
            latest_days_since_cross = daily[latest_day].get("days_since_cross")

            # Latest markers
            latest_markers = daily[latest_day].get("markers", [])

            # 6. Weekly alignment: compare daily direction vs weekly signal
            weekly_signals = [daily[d].get("weekly_signal") for d in sorted_dates if daily[d].get("weekly_signal")]
            weekly_spreads = [daily[d].get("weekly_spread_pct") for d in sorted_dates if daily[d].get("weekly_spread_pct") is not None]
            latest_weekly_signal = daily[latest_day].get("weekly_signal")
            latest_weekly_spread = daily[latest_day].get("weekly_spread_pct")

            # Classify weekly alignment
            if not latest_weekly_signal:
                weekly_alignment = "N/A"
            else:
                daily_bullish = direction in ("Bullish",)
                daily_bearish = direction in ("Bearish",)
                wk_bullish = latest_weekly_signal in ("W-Above", "W-Bullish Cross")
                wk_bearish = latest_weekly_signal in ("W-Below", "W-Bearish Cross")
                if daily_bullish and wk_bullish:
                    weekly_alignment = "Confirmed Bullish"
                elif daily_bearish and wk_bearish:
                    weekly_alignment = "Confirmed Bearish"
                elif daily_bullish and wk_bearish:
                    weekly_alignment = "Counter-trend Bullish"
                elif daily_bearish and wk_bullish:
                    weekly_alignment = "Counter-trend Bearish"
                elif direction == "Mixed":
                    weekly_alignment = "Mixed"
                else:
                    weekly_alignment = "Neutral"

            # Weekly spread trend
            if len(weekly_spreads) >= 2:
                wk_abs_first = abs(weekly_spreads[0])
                wk_abs_last = abs(weekly_spreads[-1])
                if wk_abs_last > wk_abs_first * 1.2:
                    weekly_spread_trend = "Widening"
                elif wk_abs_last < wk_abs_first * 0.8:
                    weekly_spread_trend = "Narrowing"
                else:
                    weekly_spread_trend = "Stable"
            else:
                weekly_spread_trend = "N/A"

            entry["ma_analysis"] = {
                "direction": direction,
                "spread_trend": spread_trend,
                "spreads": spreads,
                "price_momentum": price_momentum,
                "price_changes": pct_changes,
                "signal_sequence": signal_sequence,
                "signal_flow": signal_flow,
                "avg_volume_ratio": avg_vol_ratio,
                "days_since_cross": latest_days_since_cross,
                "markers": latest_markers,
                "weekly_alignment": weekly_alignment,
                "weekly_signal": latest_weekly_signal,
                "weekly_spread_pct": latest_weekly_spread,
                "weekly_spreads": weekly_spreads,
                "weekly_spread_trend": weekly_spread_trend,
                "daily_details": {d: daily[d] for d in sorted_dates},
            }

        # Enrich Fibonacci results with analysis data
        if strategy == "fibonacci" and ticker in fib_daily_detail:
            daily = fib_daily_detail[ticker]
            sorted_dates = sorted(daily.keys())

            # 1. Level consistency: is the ticker holding near the same fib level?
            levels = [daily[d]["nearest_level"] for d in sorted_dates]
            from collections import Counter
            level_counts = Counter(levels)
            dominant_level = level_counts.most_common(1)[0][0] if level_counts else "N/A"
            dominant_count = level_counts.most_common(1)[0][1] if level_counts else 0
            if dominant_count == len(levels):
                level_stability = "Locked"
            elif dominant_count >= len(levels) * 0.6:
                level_stability = "Sticky"
            else:
                level_stability = "Drifting"

            # 2. Distance convergence: is price moving toward or away from the nearest level?
            distances = [daily[d]["distance_pct"] for d in sorted_dates]
            abs_distances = [abs(d) for d in distances]
            if len(abs_distances) >= 2:
                if abs_distances[-1] < abs_distances[0] * 0.7:
                    proximity_trend = "Converging"
                elif abs_distances[-1] > abs_distances[0] * 1.3:
                    proximity_trend = "Diverging"
                else:
                    proximity_trend = "Hovering"
            else:
                proximity_trend = "N/A"

            # 3. Retracement depth trend: is the retracement getting deeper or shallower?
            retrace_pcts = [daily[d]["retracement_pct"] for d in sorted_dates]
            if len(retrace_pcts) >= 2:
                if retrace_pcts[-1] > retrace_pcts[0] + 5:
                    depth_trend = "Deepening"
                elif retrace_pcts[-1] < retrace_pcts[0] - 5:
                    depth_trend = "Recovering"
                else:
                    depth_trend = "Stable"
            else:
                depth_trend = "N/A"

            # 4. Trend consistency
            trends = [daily[d]["trend_direction"] for d in sorted_dates]
            up_count = sum(1 for t in trends if t == "uptrend_retracement")
            down_count = sum(1 for t in trends if t == "downtrend_retracement")
            if up_count > 0 and down_count == 0:
                trend_consistency = "Steady Uptrend"
            elif down_count > 0 and up_count == 0:
                trend_consistency = "Steady Downtrend"
            else:
                trend_consistency = "Pivoting"

            # 5. Signal flow
            signals = [daily[d]["signal"] for d in sorted_dates]
            unique_signals = list(dict.fromkeys(signals))
            if len(unique_signals) == 1:
                signal_flow = f"Steady ({unique_signals[0]})"
            else:
                signal_flow = " → ".join(signals)

            # 6. Volume
            vol_ratios = [daily[d].get("volume_ratio") for d in sorted_dates if daily[d].get("volume_ratio") is not None]
            avg_vol_ratio = round(sum(vol_ratios) / len(vol_ratios), 2) if vol_ratios else None

            # 7. Swing stability: did the swing high/low change across days?
            swing_highs = [daily[d]["swing_high"] for d in sorted_dates]
            swing_lows = [daily[d]["swing_low"] for d in sorted_dates]
            pivot_stable = len(set(swing_highs)) == 1 and len(set(swing_lows)) == 1

            entry["fib_analysis"] = {
                "dominant_level": dominant_level,
                "level_stability": level_stability,
                "level_sequence": levels,
                "proximity_trend": proximity_trend,
                "distances": distances,
                "depth_trend": depth_trend,
                "retrace_pcts": retrace_pcts,
                "trend_consistency": trend_consistency,
                "signal_flow": signal_flow,
                "signal_sequence": signals,
                "avg_volume_ratio": avg_vol_ratio,
                "pivot_stable": pivot_stable,
                "daily_details": {d: daily[d] for d in sorted_dates},
            }

        results.append(entry)

    results.sort(key=lambda x: (-x["days_matched"], x["ticker"]))

    logger.info(
        "Streak scan completed | strategy=%s | dates=%s | tickers_with_signals=%s",
        strategy, num_days, len(results),
    )

    result = {
        "strategy": strategy,
        "streak_days": num_days,
        "scan_dates": scan_dates,
        "total_scanned": len(ticker_list),
        "total_with_signals": len(results),
        "results": results,
    }
    _set_cached(cache_key, result)
    return result


@app.get("/api/scan/streak-summary")
async def scan_streak_summary_endpoint(
    days: int = Query(default=3, ge=2, le=10),
    fib_swing_pct: float = Query(default=5.0, ge=3.0, le=15.0),
    refresh: bool = False,
):
    """Run all 5 strategies across last N trading days and return per-ticker summary.

    Returns a map of ticker -> {strategy: consistency%} for multi-strategy badges.
    """
    cache_key = f"streak_summary_{days}_{fib_swing_pct}"
    if not refresh:
        cached = _get_cached(cache_key, "streak_summary")
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()

    ticker_list = resolve_tickers(None)
    data_days = _days_for_swing_pct(fib_swing_pct)
    logger.info("Streak summary scan started | days=%s | tickers=%s | data_days=%s (swing_pct=%.1f)",
                days, len(ticker_list), data_days, fib_swing_pct)

    loop = asyncio.get_event_loop()
    frames = await loop.run_in_executor(executor, bulk_load_dataframes, ticker_list, data_days)

    date_set: set = set()
    for df in frames.values():
        date_set.update(df.index.normalize().to_list())
    if not date_set:
        return {"days": 0, "scan_dates": [], "total_tickers": 0, "tickers_with_signals": 0, "summary": {}}

    trading_dates = sorted(date_set, reverse=True)[:days]
    num_days = len(trading_dates)

    strategies = ["gaps", "ma-crossover", "momentum-pullback", "bearish-bounce", "fibonacci"]
    min_rows_map = {"gaps": 20, "ma-crossover": 26, "momentum-pullback": 210, "bearish-bounce": 210, "fibonacci": 50}

    ticker_strategy_dates: dict = {}
    fib_details: dict = {}
    for target_dt in trading_dates:
        for ticker, df in frames.items():
            truncated = df[df.index <= target_dt]
            for strat in strategies:
                if len(truncated) < min_rows_map[strat]:
                    continue
                matched = False
                try:
                    if strat == "gaps":
                        matched = bool(scan_gap_strategies(ticker, truncated))
                    elif strat == "ma-crossover":
                        r = scan_moving_average_crossover(ticker, truncated, 9, 21)
                        matched = r is not None and r.get("signal", "") in (
                            "Bullish Crossover", "Recent Bullish", "Bearish Crossover", "Recent Bearish"
                        )
                    elif strat == "momentum-pullback":
                        matched = scan_momentum_pullback(ticker, truncated) is not None
                    elif strat == "bearish-bounce":
                        matched = scan_bearish_bounce(ticker, truncated) is not None
                    elif strat == "fibonacci":
                        r = scan_fibonacci(ticker, truncated, min_swing_pct=fib_swing_pct)
                        matched = r is not None and r.get("signal", "") != "Between Levels"
                        if matched:
                            fib_details.setdefault(ticker, {})[
                                str(target_dt.date()) if hasattr(target_dt, 'date') else str(target_dt)[:10]
                            ] = {
                                "signal": r["signal"],
                                "trend": r["trend_direction"],
                                "nearest_level": r["nearest_level"],
                                "distance_pct": round(r["distance_pct"], 2),
                                "retracement_pct": round(r["retracement_pct"], 1),
                            }
                except Exception:
                    pass
                if matched:
                    ticker_strategy_dates.setdefault(ticker, {}).setdefault(strat, set()).add(
                        str(target_dt.date()) if hasattr(target_dt, 'date') else str(target_dt)[:10]
                    )

    summary = {}
    for ticker, strat_dates in ticker_strategy_dates.items():
        entry: dict = {strat: round(len(dates) / num_days * 100) for strat, dates in strat_dates.items()}
        if ticker in fib_details:
            latest_date = max(fib_details[ticker].keys())
            entry["fib_detail"] = fib_details[ticker][latest_date]
        summary[ticker] = entry

    scan_dates = [str(d.date()) if hasattr(d, 'date') else str(d)[:10] for d in trading_dates]
    logger.info(
        "Streak summary completed | days=%s | tickers_with_signals=%s",
        num_days, len(summary),
    )
    result = {
        "days": num_days,
        "scan_dates": scan_dates,
        "total_tickers": len(ticker_list),
        "tickers_with_signals": len(summary),
        "summary": summary,
    }
    _set_cached(cache_key, result)
    return result


@app.get("/api/stock/{ticker}/trade-setup")
async def get_trade_setup(ticker: str, interval: str = "1d", refresh: bool = False):
    """
    Comprehensive trade setup analysis for a single ticker.
    Runs all strategies and synthesizes momentum, direction, entry/exit criteria,
    level retest detection with candle highs/lows, and multi-timeframe EMA alignment.

    interval: '1d' (daily), '1h' (hourly), '30m', '15m', '5m'
    """
    cache_key = f"trade_setup_v7_{ticker.upper()}_{interval}"
    if not refresh:
        cached = _get_cached(cache_key, "trade_setup")
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()

    import numpy as np
    import pandas as pd

    ticker = ticker.upper()
    valid_intervals = {"1d", "1h", "30m", "15m", "5m"}
    if interval not in valid_intervals:
        interval = "1d"

    loop = asyncio.get_event_loop()

    # Load primary data based on interval
    if interval == "1d":
        frames = await loop.run_in_executor(executor, bulk_load_dataframes, [ticker], 1600)
        df = frames.get(ticker)
    elif interval == "1h":
        rows = await loop.run_in_executor(executor, get_hourly_data, ticker, 500, None)
        if not rows or len(rows) < 50:
            raise HTTPException(status_code=404, detail=f"Insufficient hourly data for {ticker}")
        df = pd.DataFrame(rows)
        df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
        df.set_index('datetime', inplace=True)
        if 'ticker' in df.columns:
            df = df.drop(columns=['ticker'])
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns:
                df[col] = df[col].astype(float)
    else:
        # 5m, 15m, 30m — use SQL-based aggregation (same as scan endpoints)
        periods = 500 if interval == "5m" else 500
        frames = await loop.run_in_executor(executor, _load_intraday_frames, [ticker], interval, periods)
        df = frames.get(ticker)

    if df is None or len(df) < 50:
        raise HTTPException(status_code=404, detail=f"Insufficient data for {ticker} ({interval})")

    # Also load hourly data for multi-timeframe retest analysis (only when on daily)
    df_hourly = None
    if interval == "1d":
        hourly_rows = await loop.run_in_executor(executor, get_hourly_data, ticker, 200, None)
        if hourly_rows and len(hourly_rows) >= 20:
            df_hourly = pd.DataFrame(hourly_rows)
            df_hourly['datetime'] = pd.to_datetime(df_hourly['datetime'], utc=True)
            df_hourly.set_index('datetime', inplace=True)
            if 'ticker' in df_hourly.columns:
                df_hourly = df_hourly.drop(columns=['ticker'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df_hourly.columns:
                    df_hourly[col] = df_hourly[col].astype(float)

    last_close = float(df.iloc[-1]["close"])
    last_date = df.index[-1].strftime("%Y-%m-%d") if hasattr(df.index[-1], "strftime") else str(df.index[-1])

    # --- Run all strategies ---
    gaps = scan_gap_strategies(ticker, df) if len(df) >= 20 else []
    fvgs = scan_fair_value_gaps(ticker, df) if len(df) >= 20 else []
    ma = scan_moving_average_crossover(ticker, df) if len(df) >= 26 else None
    pullback = scan_momentum_pullback(ticker, df) if len(df) >= 210 else None
    bounce = scan_bearish_bounce(ticker, df) if len(df) >= 210 else None
    fib_swing_pct = calculate_fibonacci_swing_pct(df, interval)
    fib = scan_fibonacci(ticker, df, min_swing_pct=fib_swing_pct) if len(df) >= 50 else None

    # --- Compute core technicals ---
    c = df["close"].values.astype(float)
    h = df["high"].values.astype(float)
    l = df["low"].values.astype(float)
    v = df["volume"].values.astype(float) if "volume" in df.columns else np.zeros(len(c))

    # EMAs (8, 21, 50)
    def _ema(arr, span):
        alpha = 2.0 / (span + 1)
        out = np.empty_like(arr, dtype=float)
        out[0] = arr[0]
        for i in range(1, len(arr)):
            out[i] = alpha * arr[i] + (1 - alpha) * out[i - 1]
        return out

    ema8 = _ema(c, 8)
    ema21 = _ema(c, 21)
    ema50_arr = _ema(c, 50)

    ema8_val = round(float(ema8[-1]), 2)
    ema21_val = round(float(ema21[-1]), 2)
    ema50_ema = round(float(ema50_arr[-1]), 2)

    # EMA alignment check (8 > 21 > 50 = fully bullish stacked)
    ema_bullish_stack = ema8[-1] > ema21[-1] > ema50_arr[-1]
    ema_bearish_stack = ema8[-1] < ema21[-1] < ema50_arr[-1]
    if ema_bullish_stack:
        ema_alignment = "Bullish Stack"
        ema_alignment_detail = "8 EMA > 21 EMA > 50 EMA — short-term momentum aligned with trend"
    elif ema_bearish_stack:
        ema_alignment = "Bearish Stack"
        ema_alignment_detail = "8 EMA < 21 EMA < 50 EMA — bearish momentum aligned with downtrend"
    elif ema8[-1] > ema21[-1]:
        ema_alignment = "Short-term Bullish"
        ema_alignment_detail = "8 EMA > 21 EMA but not fully stacked — short-term momentum up, trend uncertain"
    elif ema8[-1] < ema21[-1]:
        ema_alignment = "Short-term Bearish"
        ema_alignment_detail = "8 EMA < 21 EMA but not fully stacked — short-term weakness, trend uncertain"
    else:
        ema_alignment = "Neutral"
        ema_alignment_detail = "EMAs converging — no clear alignment"

    # Price-to-EMA distances
    dist_to_8ema = round((last_close - ema8[-1]) / ema8[-1] * 100, 2)
    dist_to_21ema = round((last_close - ema21[-1]) / ema21[-1] * 100, 2)

    # RSI(14)
    delta = np.diff(c)
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    avg_gain = np.mean(gain[-14:])
    avg_loss = np.mean(loss[-14:])
    rsi = round(100 - 100 / (1 + avg_gain / avg_loss), 1) if avg_loss > 0 else 100.0

    # ATR(14)
    tr = np.maximum(h[1:] - l[1:], np.maximum(np.abs(h[1:] - c[:-1]), np.abs(l[1:] - c[:-1])))
    atr14 = float(np.mean(tr[-14:])) if len(tr) >= 14 else float(np.mean(tr))

    # Stochastic %K(14,3)
    if len(c) >= 17:
        highs_14 = np.array([np.max(h[i-14:i]) for i in range(14, len(h)+1)])
        lows_14 = np.array([np.min(l[i-14:i]) for i in range(14, len(l)+1)])
        raw_k = np.where(highs_14 - lows_14 > 0, (c[13:] - lows_14) / (highs_14 - lows_14) * 100, 50)
        stoch_k = round(float(np.mean(raw_k[-3:])), 1) if len(raw_k) >= 3 else round(float(raw_k[-1]), 1)
    else:
        stoch_k = 50.0

    # VWAP (Volume-Weighted Average Price — trailing 20 day)
    vwap_window = min(20, len(c))
    if np.sum(v[-vwap_window:]) > 0:
        vwap = round(float(np.sum(c[-vwap_window:] * v[-vwap_window:]) / np.sum(v[-vwap_window:])), 2)
    else:
        vwap = round(float(np.mean(c[-vwap_window:])), 2)
    price_vs_vwap = "Above" if last_close > vwap else "Below"

    # MAs
    ma10 = float(np.mean(c[-10:])) if len(c) >= 10 else None
    ma20 = float(np.mean(c[-20:])) if len(c) >= 20 else None
    ma50 = float(np.mean(c[-50:])) if len(c) >= 50 else None
    ma100 = float(np.mean(c[-100:])) if len(c) >= 100 else None
    ma200 = float(np.mean(c[-200:])) if len(c) >= 200 else None

    # ATR%
    atr_pct = round(atr14 / last_close * 100, 2)

    # --- Golden Cross / Death Cross Detection ---
    golden_cross = None
    if ma50 is not None and ma200 is not None and len(c) >= 201:
        # Check last 20 bars for a crossing event
        ma50_arr = np.array([float(np.mean(c[max(0, i-49):i+1])) for i in range(max(0, len(c)-20), len(c))])
        ma200_arr = np.array([float(np.mean(c[max(0, i-199):i+1])) for i in range(max(0, len(c)-20), len(c))])
        for j in range(1, len(ma50_arr)):
            if ma50_arr[j] > ma200_arr[j] and ma50_arr[j-1] <= ma200_arr[j-1]:
                golden_cross = {"type": "Golden Cross", "bars_ago": len(ma50_arr) - 1 - j,
                                "detail": f"50 SMA crossed above 200 SMA {len(ma50_arr) - 1 - j} bars ago — bullish"}
            elif ma50_arr[j] < ma200_arr[j] and ma50_arr[j-1] >= ma200_arr[j-1]:
                golden_cross = {"type": "Death Cross", "bars_ago": len(ma50_arr) - 1 - j,
                                "detail": f"50 SMA crossed below 200 SMA {len(ma50_arr) - 1 - j} bars ago — bearish"}
        # Also check current relative position if no recent cross
        if golden_cross is None:
            if ma50 > ma200:
                golden_cross = {"type": "Above (Bullish)", "bars_ago": None,
                                "detail": "50 SMA above 200 SMA — bullish structure, no recent cross"}
            else:
                golden_cross = {"type": "Below (Bearish)", "bars_ago": None,
                                "detail": "50 SMA below 200 SMA — bearish structure, no recent cross"}

    # --- Hourly EMAs for multi-timeframe ---
    hourly_ema8 = None
    hourly_ema21 = None
    hourly_ema_alignment = None
    if df_hourly is not None and len(df_hourly) >= 21:
        ch = df_hourly["close"].values.astype(float)
        h_ema8 = _ema(ch, 8)
        h_ema21 = _ema(ch, 21)
        hourly_ema8 = round(float(h_ema8[-1]), 2)
        hourly_ema21 = round(float(h_ema21[-1]), 2)
        if h_ema8[-1] > h_ema21[-1]:
            hourly_ema_alignment = "Bullish"
        elif h_ema8[-1] < h_ema21[-1]:
            hourly_ema_alignment = "Bearish"
        else:
            hourly_ema_alignment = "Neutral"

    # --- Level Retest Detection ---
    # Collect key levels to check for retests
    key_levels = []

    # MA levels
    if ma50:
        key_levels.append({"price": ma50, "name": "50 SMA", "source": "Moving Average"})
    if ma200:
        key_levels.append({"price": ma200, "name": "200 SMA", "source": "Moving Average"})
    key_levels.append({"price": ema8_val, "name": "8 EMA", "source": "EMA"})
    key_levels.append({"price": ema21_val, "name": "21 EMA", "source": "EMA"})
    key_levels.append({"price": vwap, "name": "VWAP(20)", "source": "VWAP"})

    # Gap edges
    for g in gaps[:5]:
        key_levels.append({"price": g["gap_high"], "name": f'Gap High ({g["gap_type"][:3]})', "source": "Gap"})
        key_levels.append({"price": g["gap_low"], "name": f'Gap Low ({g["gap_type"][:3]})', "source": "Gap"})

    # FVG zone edges
    for f_item in fvgs[:5]:
        key_levels.append({"price": f_item["fvg_high"], "name": f'FVG High ({f_item["fvg_type"][:4]})', "source": "FVG"})
        key_levels.append({"price": f_item["fvg_low"], "name": f'FVG Low ({f_item["fvg_type"][:4]})', "source": "FVG"})

    # Fibonacci levels
    if fib:
        for tgt_list in [fib.get("support_targets", []), fib.get("resistance_targets", [])]:
            for ft in tgt_list[:3]:
                key_levels.append({"price": ft.get("price", 0), "name": f'Fib {ft.get("level", "?")}', "source": "Fibonacci"})

    # Detect retests on daily timeframe (last 5 candles)
    def _detect_retests(close_arr, high_arr, low_arr, levels, lookback=5, tolerance_pct=0.5):
        """
        Check if any of the last `lookback` candles retested a key level.
        A retest = the candle's high or low came within tolerance_pct of the level.
        Returns list of retest events with candle highs/lows.
        """
        retests = []
        n = len(close_arr)
        if n < lookback:
            lookback = n
        seen_levels = set()
        for i in range(n - lookback, n):
            candle_high = float(high_arr[i])
            candle_low = float(low_arr[i])
            candle_close = float(close_arr[i])
            for lv in levels:
                lvl_price = lv["price"]
                if lvl_price <= 0:
                    continue
                tol = lvl_price * tolerance_pct / 100
                level_key = f'{lv["name"]}_{lvl_price:.2f}'
                if level_key in seen_levels:
                    continue
                touched = False
                touch_type = ""
                # Did the candle wick touch the level?
                if abs(candle_low - lvl_price) <= tol:
                    touched = True
                    touch_type = "Low touched"
                elif abs(candle_high - lvl_price) <= tol:
                    touched = True
                    touch_type = "High touched"
                elif candle_low <= lvl_price <= candle_high:
                    touched = True
                    touch_type = "Pierced"
                if touched:
                    # Did price bounce (close away from level)?
                    bounce_pct = round((candle_close - lvl_price) / lvl_price * 100, 2)
                    held = abs(bounce_pct) >= 0.1  # closed at least 0.1% away
                    seen_levels.add(level_key)
                    retests.append({
                        "level_name": lv["name"],
                        "level_price": round(lvl_price, 2),
                        "source": lv["source"],
                        "candle_high": round(candle_high, 2),
                        "candle_low": round(candle_low, 2),
                        "candle_close": round(candle_close, 2),
                        "touch_type": touch_type,
                        "held": held,
                        "bounce_pct": bounce_pct,
                        "bars_ago": n - 1 - i,
                    })
        return retests

    daily_retests = _detect_retests(c, h, l, key_levels, lookback=5, tolerance_pct=0.5)

    # Detect retests on hourly timeframe
    hourly_retests = []
    if df_hourly is not None and len(df_hourly) >= 10:
        ch = df_hourly["close"].values.astype(float)
        hh = df_hourly["high"].values.astype(float)
        lh = df_hourly["low"].values.astype(float)
        hourly_retests = _detect_retests(ch, hh, lh, key_levels, lookback=10, tolerance_pct=0.3)

    # --- Momentum Assessment ---
    if ma50 and ma200:
        if last_close > ma50 and ma50 > ma200:
            momentum = "Strong Uptrend"
            momentum_detail = "Price > 50MA > 200MA — full bullish alignment"
        elif last_close > ma50 and ma50 < ma200:
            momentum = "Recovery"
            momentum_detail = "Price above 50MA but 50MA still below 200MA — early recovery or bear rally"
        elif last_close < ma50 and ma50 > ma200:
            momentum = "Weakening"
            momentum_detail = "Price fell below 50MA but MAs still bullish — pullback or trend break starting"
        elif last_close < ma50 and ma50 < ma200:
            momentum = "Strong Downtrend"
            momentum_detail = "Price < 50MA < 200MA — full bearish alignment"
        else:
            momentum = "Sideways"
            momentum_detail = "MAs tangled — no clear directional trend"
    elif ma50:
        momentum = "Uptrend" if last_close > ma50 else "Downtrend"
        momentum_detail = f"Price {'above' if last_close > ma50 else 'below'} 50MA (insufficient data for 200MA)"
    else:
        momentum = "Insufficient Data"
        momentum_detail = "Not enough history to assess trend structure"

    # Refine with RSI
    if rsi < 30:
        rsi_state = "Oversold"
    elif rsi > 70:
        rsi_state = "Overbought"
    elif rsi < 40:
        rsi_state = "Weak"
    elif rsi > 60:
        rsi_state = "Strong"
    else:
        rsi_state = "Neutral"

    # Trend consistency (directional days / 14)
    up_days = sum(1 for i in range(-14, 0) if c[i] > c[i-1]) if len(c) >= 15 else 0
    trend_consistency = round(max(up_days, 14 - up_days) / 14 * 100, 0)

    # --- Direction Bias ---
    bull_signals = 0
    bear_signals = 0
    signal_reasons = []

    # MA signals
    if ma:
        if ma["signal"] in ("Bullish Crossover", "Recent Bullish", "Above MA"):
            bull_signals += 2 if "Crossover" in ma["signal"] else 1
            signal_reasons.append(f'MA: {ma["signal"]} ({ma.get("days_since_cross", "?")}d ago)')
        elif ma["signal"] in ("Bearish Crossover", "Recent Bearish", "Below MA"):
            bear_signals += 2 if "Crossover" in ma["signal"] else 1
            signal_reasons.append(f'MA: {ma["signal"]} ({ma.get("days_since_cross", "?")}d ago)')
        # Weekly alignment bonus
        if ma.get("weekly_signal") in ("W-Above", "W-Bullish Cross"):
            bull_signals += 1
            signal_reasons.append(f'Weekly: {ma["weekly_signal"]}')
        elif ma.get("weekly_signal") in ("W-Below", "W-Bearish Cross"):
            bear_signals += 1
            signal_reasons.append(f'Weekly: {ma["weekly_signal"]}')

    # EMA alignment signal
    if ema_bullish_stack:
        bull_signals += 1
        signal_reasons.append("EMA Stack: 8 > 21 > 50 (bullish alignment)")
    elif ema_bearish_stack:
        bear_signals += 1
        signal_reasons.append("EMA Stack: 8 < 21 < 50 (bearish alignment)")

    # Price vs 8 EMA (immediate momentum)
    if last_close > ema8[-1] * 1.005:
        bull_signals += 1
        signal_reasons.append(f"Price above 8 EMA by {dist_to_8ema}%")
    elif last_close < ema8[-1] * 0.995:
        bear_signals += 1
        signal_reasons.append(f"Price below 8 EMA by {abs(dist_to_8ema)}%")

    # VWAP bias
    if last_close > vwap:
        bull_signals += 1
        signal_reasons.append(f"Price above VWAP(20) ${vwap}")
    else:
        bear_signals += 1
        signal_reasons.append(f"Price below VWAP(20) ${vwap}")

    # Multi-timeframe EMA alignment (hourly matches daily = stronger signal)
    if hourly_ema_alignment:
        if hourly_ema_alignment == "Bullish" and ema_alignment in ("Bullish Stack", "Short-term Bullish"):
            bull_signals += 1
            signal_reasons.append("Multi-TF: Hourly 8/21 EMA aligns bullish with daily")
        elif hourly_ema_alignment == "Bearish" and ema_alignment in ("Bearish Stack", "Short-term Bearish"):
            bear_signals += 1
            signal_reasons.append("Multi-TF: Hourly 8/21 EMA aligns bearish with daily")

    # Only qualified non-Fibonacci level families contribute directional votes.
    # Fibonacci retests remain available below as structural context.
    for rt in daily_retests:
        if rt["held"] and rt["bounce_pct"] > 0 and rt["source"] in ("Moving Average", "Gap", "FVG"):
            bull_signals += 1
            signal_reasons.append(f'Retest Held: {rt["level_name"]} (${rt["level_price"]}, bounced +{rt["bounce_pct"]}%)')
            break  # Count only 1 retest signal
    for rt in daily_retests:
        if rt["held"] and rt["bounce_pct"] < 0 and rt["source"] in ("Moving Average", "Gap", "FVG"):
            bear_signals += 1
            signal_reasons.append(f'Retest Rejected: {rt["level_name"]} (${rt["level_price"]}, rejected {rt["bounce_pct"]}%)')
            break

    # Momentum pullback (bullish signal)
    if pullback:
        grade = pullback.get("grade", "C")
        bull_signals += 2 if grade in ("A+", "A") else 1
        signal_reasons.append(f'Momentum Pullback: Grade {grade} (score {pullback.get("score", 0)})')

    # Bearish bounce (bearish signal)
    if bounce:
        grade = bounce.get("grade", "C")
        bear_signals += 2 if grade in ("A+", "A") else 1
        signal_reasons.append(f'Bearish Bounce: Grade {grade} (score {bounce.get("score", 0)})')

    # Gap support/resistance
    support_gaps = [g for g in gaps if "Support" in g.get("gap_type", "")]
    resistance_gaps = [g for g in gaps if "Resistance" in g.get("gap_type", "")]
    if support_gaps:
        bull_signals += 1
        nearest = min(support_gaps, key=lambda g: abs(g["last_close"] - g["gap_high"]))
        signal_reasons.append(f'Gap Support: ${nearest["gap_low"]:.2f}–${nearest["gap_high"]:.2f}')
    if resistance_gaps:
        bear_signals += 1
        nearest = min(resistance_gaps, key=lambda g: abs(g["last_close"] - g["gap_low"]))
        signal_reasons.append(f'Gap Resistance: ${nearest["gap_low"]:.2f}–${nearest["gap_high"]:.2f}')

    # FVG signals
    bull_fvgs = [f for f in fvgs if f.get("fvg_type") == "Bullish FVG" and f.get("status") == "Unmitigated"]
    bear_fvgs = [f for f in fvgs if f.get("fvg_type") == "Bearish FVG" and f.get("status") == "Unmitigated"]
    if bull_fvgs:
        bull_signals += 1
        signal_reasons.append(f'Bullish FVGs: {len(bull_fvgs)} unmitigated demand zone(s)')
    if bear_fvgs:
        bear_signals += 1
        signal_reasons.append(f'Bearish FVGs: {len(bear_fvgs)} unmitigated supply zone(s)')

    # RSI tilt
    if rsi < 35:
        bull_signals += 1
        signal_reasons.append(f'RSI: {rsi} (oversold — bounce potential)')
    elif rsi > 65:
        bear_signals += 1
        signal_reasons.append(f'RSI: {rsi} (overbought — pullback risk)')

    # Golden / Death Cross signal
    if golden_cross:
        if golden_cross["type"] == "Golden Cross":
            bull_signals += 2
            signal_reasons.append(f'Golden Cross: {golden_cross["detail"]}')
        elif golden_cross["type"] == "Death Cross":
            bear_signals += 2
            signal_reasons.append(f'Death Cross: {golden_cross["detail"]}')

    total_signals = bull_signals + bear_signals
    if total_signals == 0:
        direction = "Neutral"
        conviction = "None"
    elif bull_signals > bear_signals:
        ratio = bull_signals / total_signals
        direction = "Bullish"
        conviction = "High" if ratio >= 0.75 else "Moderate" if ratio >= 0.6 else "Low"
    elif bear_signals > bull_signals:
        ratio = bear_signals / total_signals
        direction = "Bearish"
        conviction = "High" if ratio >= 0.75 else "Moderate" if ratio >= 0.6 else "Low"
    else:
        direction = "Neutral"
        conviction = "Conflicted"

    # --- Entry Criteria ---
    entries = []
    if direction == "Bullish":
        # EMA retest entry
        if abs(dist_to_8ema) < 0.5:
            entries.append({
                "strategy": "8 EMA Retest",
                "condition": f"Price at 8 EMA (${ema8_val}) — pullback entry in trend",
                "price_zone": f"${ema8_val}",
                "strength": "Strong" if ema_bullish_stack else "Moderate",
            })
        elif abs(dist_to_21ema) < 0.8:
            entries.append({
                "strategy": "21 EMA Retest",
                "condition": f"Price near 21 EMA (${ema21_val}) — deeper pullback entry",
                "price_zone": f"${ema21_val}",
                "strength": "Strong" if ema_bullish_stack else "Moderate",
            })
        if pullback and pullback.get("grade") in ("A+", "A", "B+"):
            entries.append({
                "strategy": "Momentum Pullback",
                "condition": f'Stoch %K at {pullback.get("stoch_k", "?")} (oversold in uptrend)',
                "price_zone": f'Near EMA21 ~${pullback.get("ema21", 0):.2f}' if pullback.get("ema21") else "Near EMA21",
                "strength": pullback.get("grade", "?"),
            })
        if bull_fvgs:
            nearest_fvg = max(bull_fvgs, key=lambda f: f["fvg_low"])
            entries.append({
                "strategy": "FVG Demand Zone",
                "condition": f'Price enters ${nearest_fvg["fvg_low"]:.2f}–${nearest_fvg["fvg_high"]:.2f}',
                "price_zone": f'${nearest_fvg["fvg_low"]:.2f}–${nearest_fvg["fvg_high"]:.2f}',
                "strength": "Unmitigated" if nearest_fvg.get("trend_aligned") else "Counter-trend",
            })
        if support_gaps:
            nearest_gap = min(support_gaps, key=lambda g: abs(g["last_close"] - g["gap_high"]))
            entries.append({
                "strategy": "Gap Support",
                "condition": f'Price tests gap zone at ${nearest_gap["gap_low"]:.2f}–${nearest_gap["gap_high"]:.2f}',
                "price_zone": f'${nearest_gap["gap_low"]:.2f}–${nearest_gap["gap_high"]:.2f}',
                "strength": "Unfilled" if "Unfilled" in nearest_gap.get("gap_type", "") else "Filled retest",
            })
        if fib and fib.get("trend_direction") == "uptrend_retracement":
            fib_targets = fib.get("support_targets", [])
            if fib_targets:
                nearest_fib = fib_targets[0]
                entries.append({
                    "strategy": "Fibonacci Support",
                    "condition": f'Near {nearest_fib.get("level", "?")} at ${nearest_fib.get("price", 0):.2f}',
                    "price_zone": f'${nearest_fib.get("price", 0):.2f}',
                    "strength": nearest_fib.get("level", "?"),
                })
    elif direction == "Bearish":
        # EMA rejection entry
        if abs(dist_to_8ema) < 0.5:
            entries.append({
                "strategy": "8 EMA Rejection",
                "condition": f"Price at 8 EMA (${ema8_val}) — rejection/short entry in downtrend",
                "price_zone": f"${ema8_val}",
                "strength": "Strong" if ema_bearish_stack else "Moderate",
            })
        elif abs(dist_to_21ema) < 0.8:
            entries.append({
                "strategy": "21 EMA Rejection",
                "condition": f"Price near 21 EMA (${ema21_val}) — deeper bounce rejection",
                "price_zone": f"${ema21_val}",
                "strength": "Strong" if ema_bearish_stack else "Moderate",
            })
        if bounce and bounce.get("grade") in ("A+", "A", "B+"):
            entries.append({
                "strategy": "Bearish Bounce",
                "condition": f'Stoch %K at {bounce.get("stoch_k", "?")} (overbought in downtrend)',
                "price_zone": f'Near EMA21 ~${bounce.get("ema21", 0):.2f}' if bounce.get("ema21") else "Near EMA21",
                "strength": bounce.get("grade", "?"),
            })
        if bear_fvgs:
            nearest_fvg = min(bear_fvgs, key=lambda f: f["fvg_high"])
            entries.append({
                "strategy": "FVG Supply Zone",
                "condition": f'Price enters ${nearest_fvg["fvg_low"]:.2f}–${nearest_fvg["fvg_high"]:.2f}',
                "price_zone": f'${nearest_fvg["fvg_low"]:.2f}–${nearest_fvg["fvg_high"]:.2f}',
                "strength": "Unmitigated" if nearest_fvg.get("trend_aligned") else "Counter-trend",
            })
        if resistance_gaps:
            nearest_gap = min(resistance_gaps, key=lambda g: abs(g["last_close"] - g["gap_low"]))
            entries.append({
                "strategy": "Gap Resistance",
                "condition": f'Price tests gap zone at ${nearest_gap["gap_low"]:.2f}–${nearest_gap["gap_high"]:.2f}',
                "price_zone": f'${nearest_gap["gap_low"]:.2f}–${nearest_gap["gap_high"]:.2f}',
                "strength": "Unfilled" if "Unfilled" in nearest_gap.get("gap_type", "") else "Filled retest",
            })
        if fib and fib.get("trend_direction") == "downtrend_retracement":
            fib_targets = fib.get("resistance_targets", [])
            if fib_targets:
                nearest_fib = fib_targets[0]
                entries.append({
                    "strategy": "Fibonacci Resistance",
                    "condition": f'Near {nearest_fib.get("level", "?")} at ${nearest_fib.get("price", 0):.2f}',
                    "price_zone": f'${nearest_fib.get("price", 0):.2f}',
                    "strength": nearest_fib.get("level", "?"),
                })

    # Add retest-based entries (any direction)
    for rt in daily_retests[:2]:
        if rt["held"] and rt["bars_ago"] <= 2:
            rt_dir = "bounced" if rt["bounce_pct"] > 0 else "rejected"
            entries.append({
                "strategy": f"Level Retest ({rt['source']})",
                "condition": f'{rt["level_name"]} at ${rt["level_price"]} — {rt["touch_type"]}, {rt_dir} {abs(rt["bounce_pct"])}%',
                "price_zone": f'H:${rt["candle_high"]} / L:${rt["candle_low"]}',
                "strength": "Strong" if abs(rt["bounce_pct"]) > 0.5 else "Moderate",
            })

    # --- Exit / Target Levels ---
    targets = []
    stops = []
    if fib:
        for rt in fib.get("resistance_targets", [])[:2]:
            targets.append({"level": rt.get("level", "?"), "price": round(rt.get("price", 0), 2), "source": "Fibonacci"})
        for st in fib.get("support_targets", [])[:2]:
            stops.append({"level": st.get("level", "?"), "price": round(st.get("price", 0), 2), "source": "Fibonacci"})
        for ext in fib.get("upside_extensions", []):
            targets.append({"level": ext.get("level", "?"), "price": round(ext.get("price", 0), 2), "source": "Fib Extension"})
    if resistance_gaps:
        nearest_res = min(resistance_gaps, key=lambda g: abs(g["last_close"] - g["gap_low"]))
        targets.append({"level": "Gap Resistance", "price": round(nearest_res["gap_low"], 2), "source": "Gap"})
    if support_gaps:
        nearest_sup = min(support_gaps, key=lambda g: abs(g["last_close"] - g["gap_high"]))
        stops.append({"level": "Gap Support", "price": round(nearest_sup["gap_low"], 2), "source": "Gap"})

    # Preserve known price barriers before considering synthetic ATR levels.
    prior_bar_count = min(252, len(h) - 1)
    if prior_bar_count > 0:
        prior_slice = slice(-(prior_bar_count + 1), -1)
        prior_high = float(np.max(h[prior_slice]))
        prior_low = float(np.min(l[prior_slice]))
        range_label = "52-Week" if interval == "1d" else f"{prior_bar_count}-Bar"
        if prior_high > last_close:
            targets.append({"level": f"Prior {range_label} High", "price": round(prior_high, 2), "source": "Price Action"})
        if prior_low < last_close:
            stops.append({"level": f"Prior {range_label} Low", "price": round(prior_low, 2), "source": "Price Action"})

    # ATR-based stops/targets
    targets.append({"level": "ATR Target (2R)", "price": round(last_close + 2 * atr14, 2), "source": "ATR"})
    stops.append({"level": "ATR Stop (1R)", "price": round(last_close - atr14, 2), "source": "ATR"})

    # --- Key stop levels (curated — only crucial indicators) ---
    # Nearest significant MA/EMA below price (pick closest one from each group)
    ema_below = [(v, n) for v, n in [(ema8_val, "8 EMA"), (ema21_val, "21 EMA"), (ema50_ema, "50 EMA")] if v < last_close]
    if ema_below:
        nearest_ema = max(ema_below, key=lambda x: x[0])  # closest below = highest value
        stops.append({"level": nearest_ema[1], "price": nearest_ema[0], "source": "EMA"})

    sma_below = [(v, n) for v, n in [(ma50, "50 SMA"), (ma200, "200 SMA")] if v and v < last_close]
    if sma_below:
        nearest_sma = max(sma_below, key=lambda x: x[0])
        stops.append({"level": nearest_sma[1], "price": round(nearest_sma[0], 2), "source": "SMA"})

    # Most recent swing low only
    swing_lookback = min(20, len(l) - 2)
    for i in range(len(l) - 1, max(0, len(l) - swing_lookback) - 1, -1):
        if i > 0 and i < len(l) - 1 and l[i] < l[i-1] and l[i] < l[i+1] and float(l[i]) < last_close:
            stops.append({"level": f"Swing Low ({len(l) - 1 - i}b ago)", "price": round(float(l[i]), 2), "source": "Price Action"})
            break  # only the most recent one

    # VWAP only if it's meaningfully below price (> 0.5% away to avoid noise)
    if vwap < last_close and (last_close - vwap) / vwap * 100 > 0.5:
        stops.append({"level": "VWAP(20)", "price": vwap, "source": "VWAP"})

    # De-duplicate stops by price
    seen_prices = set()
    unique_stops = []
    for s in stops:
        price_key = round(s["price"], 2)
        if price_key not in seen_prices:
            seen_prices.add(price_key)
            unique_stops.append(s)
    stops = unique_stops

    # Sort targets ascending, stops descending
    targets.sort(key=lambda t: t["price"])
    stops.sort(key=lambda s: s["price"], reverse=True)

    # --- Timing Assessment ---
    if ma and ma.get("days_since_cross") is not None:
        days_since = ma["days_since_cross"]
        if days_since <= 3:
            timing = "Immediate"
            timing_detail = f'Fresh MA crossover {days_since}d ago — entry window open now'
        elif days_since <= 7:
            timing = "This Week"
            timing_detail = f'Recent crossover {days_since}d ago — still early but monitor price action'
        else:
            timing = "Watchlist"
            timing_detail = f'Crossover was {days_since}d ago — wait for pullback to MA or new catalyst'
    elif any(rt["held"] and rt["bars_ago"] <= 1 for rt in daily_retests):
        timing = "Immediate"
        timing_detail = "Active level retest — price testing key level right now"
    elif pullback and pullback.get("grade") in ("A+", "A"):
        timing = "Immediate"
        timing_detail = f'A-grade pullback setup active — optimal entry window'
    elif bounce and bounce.get("grade") in ("A+", "A"):
        timing = "Immediate"
        timing_detail = f'A-grade bearish bounce active — optimal short window'
    elif rsi < 30 or rsi > 70:
        timing = "Near-term"
        timing_detail = f'RSI at extreme ({rsi}) — mean reversion likely within 1–5 days'
    elif abs(dist_to_8ema) < 0.3:
        timing = "This Week"
        timing_detail = f"Price hugging 8 EMA — decision point imminent"
    else:
        timing = "Watchlist"
        timing_detail = "No immediate catalyst — add to watchlist and wait for entry trigger"

    # --- Duration Estimate ---
    if ma and ma.get("ma_spread_pct") is not None:
        spread = abs(ma["ma_spread_pct"])
        if spread >= 3:
            duration = "Extended (weeks to months)"
            duration_detail = f'Wide MA spread ({spread:.1f}%) — strong trend likely continues'
        elif spread >= 1:
            duration = "Medium (days to weeks)"
            duration_detail = f'Moderate MA spread ({spread:.1f}%) — trend has room but watch for narrowing'
        else:
            duration = "Short (1–5 days)"
            duration_detail = f'Narrow MA spread ({spread:.1f}%) — crossover or reversal imminent'
    else:
        duration = "Unknown"
        duration_detail = "Insufficient MA data to estimate duration"

    # --- Confluence Score ---
    confluence_count = len(signal_reasons)
    if confluence_count >= 7:
        confluence_grade = "A+"
    elif confluence_count >= 5:
        confluence_grade = "A"
    elif confluence_count >= 4:
        confluence_grade = "B+"
    elif confluence_count >= 3:
        confluence_grade = "B"
    elif confluence_count >= 2:
        confluence_grade = "B-"
    else:
        confluence_grade = "C"

    result = {
        "ticker": ticker,
        "last_close": round(last_close, 2),
        "date": last_date,
        "technicals": {
            "rsi": rsi,
            "rsi_state": rsi_state,
            "stoch_k": stoch_k,
            "atr": round(atr14, 2),
            "atr_pct": atr_pct,
            "ma10": round(ma10, 2) if ma10 else None,
            "ma20": round(ma20, 2) if ma20 else None,
            "ma50": round(ma50, 2) if ma50 else None,
            "ma100": round(ma100, 2) if ma100 else None,
            "ma200": round(ma200, 2) if ma200 else None,
            "ema8": ema8_val,
            "ema21": ema21_val,
            "ema50": ema50_ema,
            "vwap": vwap,
            "price_vs_vwap": price_vs_vwap,
            "dist_to_8ema": dist_to_8ema,
            "dist_to_21ema": dist_to_21ema,
            "trend_consistency": trend_consistency,
        },
        "ema_alignment": {
            "daily": ema_alignment,
            "daily_detail": ema_alignment_detail,
            "hourly": hourly_ema_alignment,
            "hourly_ema8": hourly_ema8,
            "hourly_ema21": hourly_ema21,
            "multi_tf_agree": (hourly_ema_alignment == "Bullish" and ema_alignment in ("Bullish Stack", "Short-term Bullish"))
                           or (hourly_ema_alignment == "Bearish" and ema_alignment in ("Bearish Stack", "Short-term Bearish"))
                           if hourly_ema_alignment else None,
        },
        "level_retests": {
            "daily": daily_retests[:5],
            "hourly": hourly_retests[:5],
        },
        "momentum": {
            "state": momentum,
            "detail": momentum_detail,
        },
        "direction": {
            "bias": direction,
            "conviction": conviction,
            "bull_signals": bull_signals,
            "bear_signals": bear_signals,
        },
        "golden_cross": golden_cross,
        "interval": interval,
        "signals": signal_reasons,
        "entries": entries,
        "targets": targets[:5],
        "stops": stops[:5],
        "timing": {
            "urgency": timing,
            "detail": timing_detail,
        },
        "duration": {
            "estimate": duration,
            "detail": duration_detail,
        },
        "confluence": {
            "grade": confluence_grade,
            "count": confluence_count,
        },
        "strategy_results": {
            "ma_crossover": {"signal": ma["signal"], "spread_pct": ma.get("ma_spread_pct"), "days_since_cross": ma.get("days_since_cross"), "weekly_signal": ma.get("weekly_signal"), "markers": ma.get("markers", [])} if ma else None,
            "momentum_pullback": {"grade": pullback.get("grade"), "score": pullback.get("score")} if pullback else None,
            "bearish_bounce": {"grade": bounce.get("grade"), "score": bounce.get("score")} if bounce else None,
            "gaps": {"support_count": len(support_gaps), "resistance_count": len(resistance_gaps)} if gaps else None,
            "fvg": {"bull_unmitigated": len(bull_fvgs), "bear_unmitigated": len(bear_fvgs), "total": len(fvgs)} if fvgs else None,
            "fibonacci": {
                "scoring_role": "structural_context_only",
                "signal": fib.get("signal"),
                "trend_direction": fib.get("trend_direction"),
                "swing_basis": fib.get("swing_basis"),
                "swing_detection_pct": fib.get("swing_detection_pct"),
                "swing_high": fib.get("swing_high"),
                "swing_low": fib.get("swing_low"),
                "swing_high_date": fib.get("swing_high_date"),
                "swing_low_date": fib.get("swing_low_date"),
                "swing_size_pct": fib.get("swing_size_pct"),
                "developing_pivot": fib.get("developing_pivot"),
                "active_leg": fib.get("active_leg"),
                "confirmed_legs": fib.get("confirmed_legs", []),
                "nearest_level": fib.get("nearest_level"),
                "nearest_level_price": fib.get("nearest_level_price"),
                "distance_pct": fib.get("distance_pct"),
                "retracement_pct": fib.get("retracement_pct"),
                "levels": [
                    {"name": "23.6%", "price": fib.get("fib_236")},
                    {"name": "38.2%", "price": fib.get("fib_382")},
                    {"name": "50.0%", "price": fib.get("fib_500")},
                    {"name": "61.8%", "price": fib.get("fib_618")},
                    {"name": "78.6%", "price": fib.get("fib_786")},
                ],
            } if fib else None,
        }
    }
    _set_cached(cache_key, result)
    return result


@app.get("/api/strategies")
async def list_strategies():
    """List all available screening strategies."""
    return {
        "strategies": [
            {
                "id": "gaps",
                "name": "Gap Strategies",
                "description": "Identifies unfilled gaps and potential support/resistance levels",
                "endpoint": "/api/scan/gaps"
            },
            {
                "id": "ma-crossover",
                "name": "Moving Average Crossover",
                "description": "Detects bullish/bearish MA crossovers (9/21 EMA)",
                "endpoint": "/api/scan/ma-crossover",
                "params": ["short_period", "long_period"]
            },
            {
                "id": "momentum-pullback",
                "name": "Momentum Pullback",
                "description": "Finds elite stocks in strong uptrends with optimal pullback entry opportunities",
                "endpoint": "/api/scan/momentum-pullback"
            },
            {
                "id": "bearish-bounce",
                "name": "Bearish Bounce",
                "description": "Finds stocks in confirmed downtrends bouncing into resistance for short/exit setups",
                "endpoint": "/api/scan/bearish-bounce"
            }
        ]
    }


@app.get("/api/daily-recommendations")
async def get_daily_recommendations(trade_date: Optional[str] = None):
    """Get today's or specified date's daily recommendations (Top 10 Bull + Top 10 Bear)"""
    try:
        from datetime import datetime, timedelta
        from database import get_db_cursor
        
        # If no date provided, use today's date
        if not trade_date:
            from database import _ET
            trade_date = datetime.now(_ET).date().isoformat()
        
        with get_db_cursor() as ctx:
            cur = ctx.__enter__()
            
            # Get ALL Bull recommendations for the date, sorted by confidence
            cur.execute("""
                SELECT 
                    rec_id,
                    trade_date,
                    ticker,
                    recommendation_type,
                    predicted_confidence_pct,
                    rank_in_category,
                    calibration_sources
                FROM daily_recommendations
                WHERE trade_date = %s AND recommendation_type = 'BULL'
                ORDER BY predicted_confidence_pct DESC
                LIMIT 10
            """, (trade_date,))
            
            bull_recs = []
            for idx, row in enumerate(cur.fetchall(), 1):
                bull_recs.append({
                    "rec_id": row['rec_id'],
                    "ticker": row['ticker'],
                    "direction": "BULL",
                    "confidence": float(row['predicted_confidence_pct'] or 0),
                    "rank": idx,  # Re-rank based on sorted order
                    "calibration_sources": row['calibration_sources'] or ""
                })
            
            # Get ALL Bear recommendations for the date, sorted by confidence
            cur.execute("""
                SELECT 
                    rec_id,
                    trade_date,
                    ticker,
                    recommendation_type,
                    predicted_confidence_pct,
                    rank_in_category,
                    calibration_sources
                FROM daily_recommendations
                WHERE trade_date = %s AND recommendation_type = 'BEAR'
                ORDER BY predicted_confidence_pct DESC
                LIMIT 10
            """, (trade_date,))
            
            bear_recs = []
            for idx, row in enumerate(cur.fetchall(), 1):
                bear_recs.append({
                    "rec_id": row['rec_id'],
                    "ticker": row['ticker'],
                    "direction": "BEAR",
                    "confidence": float(row['predicted_confidence_pct'] or 0),
                    "rank": idx,  # Re-rank based on sorted order
                    "calibration_sources": row['calibration_sources'] or ""
                })
            
            ctx.__exit__(None, None, None)
            
            return {
                "trade_date": trade_date,
                "bull_recommendations": bull_recs,
                "bear_recommendations": bear_recs,
                "total_bull": len(bull_recs),
                "total_bear": len(bear_recs)
            }
    except Exception as e:
        logger.error(f"Daily recommendations error: {e}")
        return {
            "error": str(e),
            "bull_recommendations": [],
            "bear_recommendations": []
        }


# ============================================================================
# 5-LAYER CALIBRATION API ROUTE (For Enhanced Trade Setup)
# ============================================================================

_PATTERN_KEYS = ("breakout", "vwap", "volatility", "trend", "rs", "calendar")


def _fetch_calibration_bundle(ticker: str, trade_date: str, perf_days: int) -> dict:
    """Load all 5 calibration layers for a ticker over one DB connection.

    Each layer resolves the most recent row at or before trade_date so panels stay
    populated on weekends and before the 9:35 AM generator run.
    """
    import json
    from database import get_db_cursor

    symbol = ticker.upper()
    bundle: dict = {
        "ticker": symbol,
        "requested_date": trade_date,
        "pattern_scores": None,
        "priors": {},
        "analogs": None,
        "recommendation": None,
        "performance": None,
        "baseline": None,
    }

    with get_db_cursor() as cur:
        # ── Layer 1: opening pattern scores (9:25 AM) ──────────────────────
        cur.execute(
            """
            SELECT trade_date, sector,
                   breakout_score, vwap_score, volatility_score,
                   trend_score, rs_score, calendar_score,
                   breakout_fired, vwap_fired, volatility_fired,
                   trend_fired, rs_fired, calendar_fired,
                   primary_regime, sector_regime, market_breadth_score,
                   analog_match_count, analog_win_rate
            FROM opening_pattern_scores
            WHERE ticker = %s AND trade_date <= %s
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            (symbol, trade_date),
        )
        row = cur.fetchone()
        if row:
            fired = {k: bool(row[f"{k}_fired"]) for k in _PATTERN_KEYS}
            bundle["pattern_scores"] = {
                "trade_date": str(row["trade_date"]),
                "sector": row["sector"],
                "patterns": {k: int(row[f"{k}_score"] or 0) for k in _PATTERN_KEYS},
                "fired": fired,
                "fired_count": sum(fired.values()),
                "primary_regime": row["primary_regime"],
                "sector_regime": row["sector_regime"],
                "market_breadth_score": row["market_breadth_score"],
                "analog_match_count": row["analog_match_count"] or 0,
                "analog_win_rate": float(row["analog_win_rate"]) if row["analog_win_rate"] is not None else None,
            }

        # ── Layer 2: win-rate priors (Friday EOD) ──────────────────────────
        # DISTINCT ON keeps the newest effective_date per pattern.
        cur.execute(
            """
            SELECT DISTINCT ON (pattern_name)
                   pattern_name, historical_win_rate, sample_size,
                   confidence_multiplier, lookback_days, effective_date
            FROM pattern_win_rate_priors
            ORDER BY pattern_name, effective_date DESC
            """
        )
        for r in cur.fetchall():
            bundle["priors"][r["pattern_name"]] = {
                "win_rate": float(r["historical_win_rate"] or 0),
                "sample_size": r["sample_size"] or 0,
                "confidence_multiplier": float(r["confidence_multiplier"] or 1.0),
                "lookback_days": r["lookback_days"] or 0,
                "effective_date": str(r["effective_date"]),
            }

        # ── Layer 3: analog matches (9:30 AM) ──────────────────────────────
        cur.execute(
            """
            SELECT current_trade_date, analog_count, analog_accuracy,
                   analog_details, analog_confidence_boost, current_sector_regime
            FROM pattern_analog_matches
            WHERE current_ticker = %s AND current_trade_date <= %s
            ORDER BY current_trade_date DESC
            LIMIT 1
            """,
            (symbol, trade_date),
        )
        row = cur.fetchone()
        if row:
            details = row["analog_details"]
            if isinstance(details, str):
                try:
                    details = json.loads(details)
                except ValueError:
                    details = []
            if not isinstance(details, list):
                details = []
            bundle["analogs"] = {
                "trade_date": str(row["current_trade_date"]),
                "analog_count": row["analog_count"] or 0,
                "analog_accuracy": float(row["analog_accuracy"] or 0),
                "confidence_boost": int(row["analog_confidence_boost"] or 0),
                "sector_regime": row["current_sector_regime"],
                "similar_days": details[:5],
            }

        # ── Layer 4: calibrated recommendation (9:35 AM) ───────────────────
        # A ticker can hold both a BULL and a BEAR row; surface the stronger one.
        cur.execute(
            """
            SELECT trade_date, recommendation_type, predicted_confidence_pct,
                   predicted_return_pct, rank_in_category, signal_grade,
                   breakout_score, vwap_score, volatility_score,
                   trend_score, rs_score, calendar_score,
                   recommended_entry, recommended_stop, recommended_target_1,
                   risk_reward_ratio,
                   pattern_priors_applied, analog_matching_applied,
                   confidence_before_calibration, confidence_after_calibration,
                   calibration_sources
            FROM daily_recommendations
            WHERE ticker = %s AND trade_date <= %s
            ORDER BY trade_date DESC, predicted_confidence_pct DESC NULLS LAST
            LIMIT 1
            """,
            (symbol, trade_date),
        )
        row = cur.fetchone()
        if row:
            before = row["confidence_before_calibration"]
            after = row["confidence_after_calibration"]
            bundle["recommendation"] = {
                "trade_date": str(row["trade_date"]),
                "direction": row["recommendation_type"],
                "predicted_confidence": int(row["predicted_confidence_pct"] or 0),
                "predicted_return_pct": float(row["predicted_return_pct"]) if row["predicted_return_pct"] is not None else None,
                "rank": row["rank_in_category"],
                "signal_grade": row["signal_grade"],
                "pattern_scores": {k: int(row[f"{k}_score"] or 0) for k in _PATTERN_KEYS},
                "levels": {
                    "entry": float(row["recommended_entry"]) if row["recommended_entry"] is not None else None,
                    "stop": float(row["recommended_stop"]) if row["recommended_stop"] is not None else None,
                    "target_1": float(row["recommended_target_1"]) if row["recommended_target_1"] is not None else None,
                    "risk_reward": float(row["risk_reward_ratio"]) if row["risk_reward_ratio"] is not None else None,
                },
                "calibration": {
                    "pattern_priors_applied": bool(row["pattern_priors_applied"]),
                    "analog_matching_applied": bool(row["analog_matching_applied"]),
                    "confidence_before": int(before) if before is not None else None,
                    "confidence_after": int(after) if after is not None else None,
                    "sources": row["calibration_sources"] or "",
                },
            }

        # ── Layer 5: realised performance (4:05 PM) ────────────────────────
        # Only resolved calls count; returns are signed by direction so a
        # correct BEAR (price fell) reads as a gain rather than a loss.
        perf_sql = """
            SELECT COUNT(*) AS total_recs,
                   COUNT(*) FILTER (WHERE recommendation_correct) AS correct_recs,
                   COUNT(*) FILTER (WHERE recommendation_type = 'BULL') AS bull_recs,
                   COUNT(*) FILTER (WHERE recommendation_type = 'BULL' AND recommendation_correct) AS bull_correct,
                   COUNT(*) FILTER (WHERE recommendation_type = 'BEAR') AS bear_recs,
                   COUNT(*) FILTER (WHERE recommendation_type = 'BEAR' AND recommendation_correct) AS bear_correct,
                   ROUND(AVG(CASE WHEN recommendation_type = 'BEAR'
                                  THEN -actual_return_pct ELSE actual_return_pct END), 2) AS avg_return_directional
            FROM daily_recommendations
            WHERE trade_date <= %s
              AND trade_date > %s::date - make_interval(days => %s)
              AND recommendation_correct IS NOT NULL
        """

        cur.execute(perf_sql + " AND ticker = %s", (trade_date, trade_date, perf_days, symbol))
        row = cur.fetchone()
        total = (row["total_recs"] or 0) if row else 0
        bull_n = (row["bull_recs"] or 0) if row else 0
        bear_n = (row["bear_recs"] or 0) if row else 0
        bundle["performance"] = {
            "period_days": perf_days,
            "total_recs": total,
            "correct_recs": (row["correct_recs"] or 0) if row else 0,
            "win_rate": round((row["correct_recs"] or 0) / total * 100, 2) if total else 0.0,
            "bull_stats": {
                "count": bull_n,
                "win_rate": round((row["bull_correct"] or 0) / bull_n * 100, 2) if bull_n else 0.0,
            },
            "bear_stats": {
                "count": bear_n,
                "win_rate": round((row["bear_correct"] or 0) / bear_n * 100, 2) if bear_n else 0.0,
            },
            "returns": {
                "avg_return_directional": float(row["avg_return_directional"]) if row and row["avg_return_directional"] is not None else None,
            },
        }

        # System-wide baseline over the same window — the per-ticker sample is
        # far too small to stand on its own.
        cur.execute(
            perf_sql.replace(
                "COUNT(*) AS total_recs",
                "COUNT(*) AS total_recs, COUNT(DISTINCT ticker) AS ticker_count",
            ),
            (trade_date, trade_date, perf_days),
        )
        row = cur.fetchone()
        base_total = (row["total_recs"] or 0) if row else 0
        bundle["baseline"] = {
            "total_recs": base_total,
            "correct_recs": (row["correct_recs"] or 0) if row else 0,
            "win_rate": round((row["correct_recs"] or 0) / base_total * 100, 2) if base_total else 0.0,
            "ticker_count": (row["ticker_count"] or 0) if row else 0,
            "avg_return_directional": float(row["avg_return_directional"]) if row and row["avg_return_directional"] is not None else None,
        }

    return bundle


@app.get("/api/stock/{ticker}/calibration")
async def get_ticker_calibration(
    ticker: str,
    trade_date: Optional[str] = None,
    perf_days: int = Query(default=30, ge=1, le=365),
    refresh: bool = False,
):
    """All 5 calibration layers for a ticker in a single round trip."""
    from database import _ET

    symbol = ticker.upper()
    if not trade_date:
        trade_date = datetime.now(_ET).date().isoformat()

    cache_key = f"calibration_{symbol}_{trade_date}_{perf_days}"
    if not refresh:
        cached = _get_cached(cache_key, "calibration")
        if cached is not None:
            return cached

    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            executor, _fetch_calibration_bundle, symbol, trade_date, perf_days
        )
    except Exception:
        logger.exception("Calibration fetch failed | ticker=%s | date=%s", symbol, trade_date)
        raise HTTPException(status_code=500, detail="Failed to load calibration data")

    _set_cached(cache_key, result)
    return result


# ============================================================================
# CROSS-SECTIONAL SIGNAL (validated momentum model)
# ============================================================================

@app.get("/api/signals/cross-sectional")
async def cross_sectional_signals(
    trade_date: Optional[str] = None,
    side: Optional[str] = Query(default=None, regex="^(LONG|SHORT|FLAT)$"),
    sector: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=500),
):
    """Latest cross-sectional momentum signal. Defaults to the most recent date stored.

    Passing `sector` ranks within that sector instead of against the whole universe,
    which trades roughly 6pp of annual return for materially lower concentration.
    """
    from database import get_db_cursor

    try:
        with get_db_cursor() as cur:
            if not trade_date:
                cur.execute("SELECT MAX(trade_date) AS d FROM cross_sectional_signals")
                row = cur.fetchone()
                if not row or not row["d"]:
                    return {"trade_date": None, "results": [], "detail": "No signal generated yet"}
                trade_date = row["d"].isoformat()

            params: list = [trade_date]
            sector_clause = ""
            if sector:
                sector_clause = "AND sector = %s"
                params.append(sector)
            side_clause = ""
            if side and not sector:
                side_clause = "AND side = %s"
                params.append(side)
            params.append(limit)

            # SHORT wants the most negative scores, so invert the ordering.
            order = "ASC" if side == "SHORT" else "DESC"
            # Rank before filtering, or the rank would describe the filtered subset.
            cur.execute(f"""
                WITH scored AS (
                    SELECT s.ticker, s.model_version, s.horizon_days, s.neutral_score,
                           s.percentile, s.decile, s.side, s.universe_size,
                           t.sector,
                           RANK() OVER (PARTITION BY t.sector
                                        ORDER BY s.neutral_score DESC) AS sector_rank,
                           COUNT(*) OVER (PARTITION BY t.sector) AS sector_size
                    FROM cross_sectional_signals s
                    LEFT JOIN selected_tickers t ON t.ticker = s.ticker
                    WHERE s.trade_date = %s
                )
                SELECT * FROM scored
                WHERE TRUE {sector_clause} {side_clause}
                ORDER BY neutral_score {order}
                LIMIT %s
            """, params)
            results = [dict(r) for r in cur.fetchall()]

        for r in results:
            r["neutral_score"] = float(r["neutral_score"]) if r["neutral_score"] is not None else None
            r["percentile"] = float(r["percentile"]) if r["percentile"] is not None else None

        return {
            "trade_date": trade_date,
            "count": len(results),
            "scope": "sector" if sector else "universe",
            "sector": sector,
            "results": results,
        }
    except Exception:
        logger.exception("Cross-sectional signal query failed")
        raise HTTPException(status_code=500, detail="Failed to load cross-sectional signals")


@app.get("/api/signals/sectors")
async def cross_sectional_sectors():
    """Sectors that have signal coverage, for the selector."""
    from database import get_db_cursor

    try:
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT t.sector, COUNT(*) AS tickers
                FROM cross_sectional_signals s
                JOIN selected_tickers t ON t.ticker = s.ticker
                WHERE s.trade_date = (SELECT MAX(trade_date) FROM cross_sectional_signals)
                  AND t.sector IS NOT NULL
                GROUP BY t.sector
                ORDER BY t.sector
            """)
            return {"sectors": [dict(r) for r in cur.fetchall()]}
    except Exception:
        logger.exception("Sector list query failed")
        raise HTTPException(status_code=500, detail="Failed to load sectors")


@app.get("/api/discovery/states")
async def market_discovery_states(
    trade_date: Optional[str] = None,
    state: Optional[str] = Query(
        default=None,
        regex="^(CONTINUATION|REVERSAL_WATCH|EMERGING_REVERSAL|REVERSAL_CONFIRMED|CONFLICT|LAGGARD|NEUTRAL)$",
    ),
    sector: Optional[str] = None,
    limit: int = Query(default=100, ge=1, le=500),
):
    """Latest shadow discovery states. Reversal states are not recommendations."""
    from database import get_db_cursor

    try:
        with get_db_cursor() as cur:
            if not trade_date:
                cur.execute("SELECT MAX(trade_date) AS d FROM market_discovery_states")
                row = cur.fetchone()
                if not row or not row["d"]:
                    return {"trade_date": None, "results": [], "summary": {}}
                trade_date = row["d"].isoformat()
            clauses = ["d.trade_date = %s"]
            params: list = [trade_date]
            if state:
                clauses.append("d.state = %s")
                params.append(state)
            if sector:
                clauses.append("t.sector = %s")
                params.append(sector)
            params.append(limit)
            cur.execute(f"""
                SELECT d.ticker, d.model_version, d.state, d.validation_status,
                       d.activity_percentile, d.echo_percentile,
                       d.older_momentum_percentile, d.long_momentum_percentile,
                       d.recent_21d_percentile, d.recent_21d_return,
                       d.recent_5d_return, d.close_price, d.sma_20, d.sma_50,
                       d.higher_swing_high, d.higher_swing_low, d.evidence,
                       d.evidence ->> 'trend_state' AS trend_state,
                       d.evidence ->> 'extension_risk' AS extension_risk,
                       d.evidence ->> 'reversal_trigger' AS reversal_trigger,
                       d.evidence ->> 'position_guidance' AS position_guidance,
                       t.sector
                FROM market_discovery_states d
                LEFT JOIN selected_tickers t ON t.ticker = d.ticker
                WHERE {' AND '.join(clauses)}
                ORDER BY
                    CASE d.state
                      WHEN 'REVERSAL_CONFIRMED' THEN 1
                      WHEN 'EMERGING_REVERSAL' THEN 2
                      WHEN 'REVERSAL_WATCH' THEN 3
                      WHEN 'CONTINUATION' THEN 4
                      WHEN 'CONFLICT' THEN 5
                      WHEN 'LAGGARD' THEN 6 ELSE 7 END,
                    d.recent_21d_percentile DESC NULLS LAST,
                    d.echo_percentile DESC NULLS LAST
                LIMIT %s
            """, params)
            results = [dict(row) for row in cur.fetchall()]
            summary_sector_clause = "AND t.sector = %s" if sector else ""
            summary_params: list = [trade_date]
            if sector:
                summary_params.append(sector)
            cur.execute(f"""
                SELECT d.state, COUNT(*) AS count
                FROM market_discovery_states d
                LEFT JOIN selected_tickers t ON t.ticker = d.ticker
                WHERE d.trade_date = %s {summary_sector_clause}
                GROUP BY d.state ORDER BY d.state
            """, summary_params)
            summary = {row["state"]: row["count"] for row in cur.fetchall()}
        return {"trade_date": trade_date, "summary": summary,
                "count": len(results), "results": results}
    except Exception:
        logger.exception("Market discovery query failed")
        raise HTTPException(status_code=500, detail="Failed to load market discovery states")


@app.get("/api/stock/{ticker}/discovery-state")
async def ticker_discovery_state(ticker: str, limit: int = Query(default=30, ge=1, le=252)):
    """Latest discovery state and recent transitions for one ticker."""
    from database import get_db_cursor

    symbol = ticker.upper()
    try:
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT trade_date, state, validation_status, activity_percentile,
                       echo_percentile, recent_21d_percentile, recent_21d_return,
                      recent_5d_return, evidence,
                      evidence ->> 'trend_state' AS trend_state,
                      evidence ->> 'extension_risk' AS extension_risk,
                      evidence ->> 'reversal_trigger' AS reversal_trigger,
                      evidence ->> 'position_guidance' AS position_guidance
                FROM market_discovery_states
                WHERE ticker = %s
                ORDER BY trade_date DESC LIMIT %s
            """, (symbol, limit))
            history = [dict(row) for row in cur.fetchall()]
        return {"ticker": symbol, "state": history[0] if history else None,
                "history": history}
    except Exception:
        logger.exception("Ticker discovery query failed | ticker=%s", symbol)
        raise HTTPException(status_code=500, detail="Failed to load ticker discovery state")


@app.get("/api/scanner-events/summary")
async def scanner_event_summary(
    interval: Optional[str] = Query(default=None, regex="^(1d|1wk|1h)$"),
    discovery_state: Optional[str] = None,
    min_periods: int = Query(default=20, ge=1, le=1000),
):
    """Horizon-spaced scanner utility metrics. No result is a recommendation."""
    try:
        from research.scanner_events import ensure_tables, event_summary
        ensure_tables()
        return {"results": event_summary(interval, discovery_state, min_periods)}
    except Exception:
        logger.exception("Scanner event summary query failed")
        raise HTTPException(status_code=500, detail="Failed to load scanner event summary")


@app.get("/api/scanner-events/qualification")
async def scanner_event_qualification(
    interval: Optional[str] = Query(default=None, regex="^(1d|1wk|1h)$"),
):
    """Aggregate full-history qualification results. No result is a recommendation."""
    try:
        from research.scanner_events import (
            OUTCOME_ENTRY_MODEL, ensure_tables, qualification_report,
        )
        ensure_tables()
        results = qualification_report(interval)
        return {
            "entry_model": OUTCOME_ENTRY_MODEL,
            "gates": {
                "minimum_events": 100,
                "minimum_independent_periods": 40,
                "minimum_alpha_t_stat": 2.0,
                "requires_positive_early_late_alpha": True,
                "maximum_false_discovery_rate": 0.05,
                "minimum_calibration_oos_periods": 100,
                "maximum_brier_score": 0.25,
                "maximum_expected_calibration_error": 0.05,
            },
            "results": results,
        }
    except Exception:
        logger.exception("Scanner qualification query failed")
        raise HTTPException(status_code=500, detail="Failed to load scanner qualification")


@app.get("/api/scanner-events/backlog")
async def scanner_event_backlog():
    """Pending and evaluated event/horizon pairs."""
    try:
        from research.scanner_events import ensure_tables, pending_outcome_counts
        ensure_tables()
        return {"results": pending_outcome_counts()}
    except Exception:
        logger.exception("Scanner event backlog query failed")
        raise HTTPException(status_code=500, detail="Failed to load scanner event backlog")


@app.get("/api/scanner-events")
async def scanner_events_endpoint(
    interval: Optional[str] = Query(default=None, regex="^(1d|1wk|1h)$"),
    limit: int = Query(default=100, ge=1, le=500),
):
    """Latest shadow scanner setup lifecycles and completed outcomes."""
    try:
        from research.scanner_events import ensure_tables, recent_events
        ensure_tables()
        return {"results": recent_events(interval, limit)}
    except Exception:
        logger.exception("Scanner event list query failed")
        raise HTTPException(status_code=500, detail="Failed to load scanner events")


@app.get("/api/scanner-events/latest-by-ticker")
async def latest_ticker_scanner_signals(
    interval: Optional[str] = Query(default=None, regex="^(1d|1wk|1h)$"),
    limit: int = Query(default=500, ge=1, le=500),
):
    """Latest observed scanner signal per ticker. Signals are not recommendations."""
    try:
        from research.scanner_events import ensure_tables, latest_ticker_signals
        ensure_tables()
        return {"results": latest_ticker_signals(interval, limit)}
    except Exception:
        logger.exception("Latest ticker scanner signals query failed")
        raise HTTPException(status_code=500, detail="Failed to load latest ticker scanner signals")


@app.get("/api/scanner-events/sector-performance")
async def scanner_sector_performance(
    sessions: int = Query(default=1),
):
    """Equal-weight sector performance over 1, 5, 10, or 21 sessions."""
    if sessions not in (1, 5, 10, 21):
        raise HTTPException(status_code=422, detail="sessions must be 1, 5, 10, or 21")
    try:
        from research.scanner_events import latest_sector_performance
        return {"sessions": sessions, "results": latest_sector_performance(sessions)}
    except Exception:
        logger.exception("Scanner sector performance query failed")
        raise HTTPException(status_code=500, detail="Failed to load sector performance")


@app.get("/api/stock/{ticker}/scanner-events")
async def ticker_scanner_events(
    ticker: str, limit: int = Query(default=50, ge=1, le=252)
):
    """Scanner setup lifecycles and outcomes for one ticker."""
    try:
        from research.scanner_events import ensure_tables, ticker_events
        ensure_tables()
        return {"ticker": ticker.upper(), "events": ticker_events(ticker, limit)}
    except Exception:
        logger.exception("Ticker scanner event query failed | ticker=%s", ticker)
        raise HTTPException(status_code=500, detail="Failed to load ticker scanner events")


@app.get("/api/stock/{ticker}/cross-sectional-signal")
async def ticker_cross_sectional_signal(ticker: str, trade_date: Optional[str] = None):
    """Where this ticker sits in the cross-section, plus its recent history."""
    from database import get_db_cursor

    symbol = ticker.upper()
    try:
        with get_db_cursor() as cur:
            if trade_date:
                cur.execute("""
                    SELECT trade_date, model_version, horizon_days, raw_score, neutral_score,
                           percentile, decile, side, universe_size
                    FROM cross_sectional_signals
                    WHERE ticker = %s AND trade_date <= %s
                    ORDER BY trade_date DESC LIMIT 1
                """, (symbol, trade_date))
            else:
                cur.execute("""
                    SELECT trade_date, model_version, horizon_days, raw_score, neutral_score,
                           percentile, decile, side, universe_size
                    FROM cross_sectional_signals
                    WHERE ticker = %s
                    ORDER BY trade_date DESC LIMIT 1
                """, (symbol,))
            row = cur.fetchone()
            if not row:
                return {"ticker": symbol, "signal": None}

            cur.execute("""
                SELECT trade_date, decile, percentile, side
                FROM cross_sectional_signals
                WHERE ticker = %s
                ORDER BY trade_date DESC LIMIT 30
            """, (symbol,))
            history = [
                {
                    "trade_date": h["trade_date"].isoformat(),
                    "decile": h["decile"],
                    "percentile": float(h["percentile"]) if h["percentile"] is not None else None,
                    "side": h["side"],
                }
                for h in cur.fetchall()
            ]

        return {
            "ticker": symbol,
            "signal": {
                "trade_date": row["trade_date"].isoformat(),
                "model_version": row["model_version"],
                "horizon_days": row["horizon_days"],
                "raw_score": float(row["raw_score"]) if row["raw_score"] is not None else None,
                "neutral_score": float(row["neutral_score"]) if row["neutral_score"] is not None else None,
                "percentile": float(row["percentile"]) if row["percentile"] is not None else None,
                "decile": row["decile"],
                "side": row["side"],
                "universe_size": row["universe_size"],
            },
            "history": history,
        }
    except Exception:
        logger.exception("Ticker cross-sectional signal query failed | ticker=%s", symbol)
        raise HTTPException(status_code=500, detail="Failed to load cross-sectional signal")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
