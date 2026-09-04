from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from equity.api import (
    current_chart_bar_projection,
    current_pattern_watch_projection,
    current_trade_setup_projection,
    router as equity_materialization_router,
)
from equity.technicals import (
    assess_momentum,
    compute_ema_confirmation,
    compute_trade_setup_technicals,
    detect_golden_cross,
    detect_level_retests,
    detect_setup_candlesticks,
    exponential_moving_average,
)
from equity.portal_snapshots import current as current_equity_portal_snapshot
from typing import List, Optional, Any, Callable
from datetime import datetime, timedelta, timezone
import copy
import asyncio
import logging
import math
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor

from models import GapResult, ScanResult, ScreenerRequest, StockPrice
from database import (
    get_distinct_tickers,
    get_selected_tickers,
    get_stock_data,
    get_latest_price_date,
    get_latest_quote,
    require_canonical_schema,
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
from research.price_structures import analyze_price_structures
from research.forming_patterns import detect_forming_patterns, summarize_cross_frame_patterns
from research.price_channels import detect_price_channel
from options.api import router as options_router
from equity.setup_composition import (
    compose_fibonacci_context,
    compose_setup_confluence,
    compose_setup_direction,
    compose_setup_duration,
    compose_setup_timing,
    compose_trade_levels,
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
MATERIALIZED_30M_SETUP_ENABLED = os.getenv(
    "EQUITY_MATERIALIZED_30M_SETUP_ENABLED", "true"
).lower() == "true"
MATERIALIZED_1H_SETUP_ENABLED = os.getenv(
    "EQUITY_MATERIALIZED_1H_SETUP_ENABLED", "true"
).lower() == "true"
MATERIALIZED_1D_SETUP_ENABLED = os.getenv(
    "EQUITY_MATERIALIZED_1D_SETUP_ENABLED", "true"
).lower() == "true"
MATERIALIZED_1WK_SETUP_ENABLED = os.getenv(
    "EQUITY_MATERIALIZED_1WK_SETUP_ENABLED", "true"
).lower() == "true"
MATERIALIZED_1MO_SETUP_ENABLED = os.getenv(
    "EQUITY_MATERIALIZED_1MO_SETUP_ENABLED", "true"
).lower() == "true"
MATERIALIZED_PATTERN_WATCH_ENABLED = os.getenv(
    "EQUITY_MATERIALIZED_PATTERN_WATCH_ENABLED", "true"
).lower() == "true"
MATERIALIZED_PORTAL_SNAPSHOTS_ENABLED = os.getenv(
    "EQUITY_MATERIALIZED_PORTAL_SNAPSHOTS_ENABLED", "true"
).lower() == "true"
MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED = os.getenv(
    "EQUITY_MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED", "true"
).lower() == "true"
API_PROCESS_STARTED_AT = datetime.now(timezone.utc)

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
app.include_router(equity_materialization_router)

app.include_router(options_router)

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
CHART_PATTERN_DETAIL_CACHE = "chart_patterns_v9"
CHART_PATTERN_TICKER_CACHE = "chart_pattern_ticker_v7"
CHART_PATTERN_SCAN_CACHE = "chart_pattern_scan_v7"
PRICE_CHANNEL_CACHE = "price_channel_v1"

# TTL in seconds per cache-key prefix
_CACHE_TTLS: dict[str, int] = {
    "tickers":          86400,   # 24h
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
    # The newest bar keeps moving while the market is open, so anything derived from it
    # must expire faster than the bar it is built on.
    "trade_setup":      900,     # 15min fallback
    "trade_setup_1h":   300,     # 5min; the live hour is still forming
    "trade_setup_1d":   900,     # 15min
    "trade_setup_1wk":  900,     # 15min; the in-progress week resamples every session
    "trade_setup_1mo":  900,     # 15min; context resampled from the daily frame
    "trade_setup_multi": 300,    # 5min; coordinates all four setup intervals
    "chart_daily":      900,     # 15min; today's daily bar is still forming
    "chart_intraday":   60,      # 1min; market-hours data updates every 5min
    "chart_pattern_scan": 300,   # 5min; on-demand universe research view
    "chart_pattern_ticker": 60,  # 1min; cross-frame reading includes intraday bars
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


def _portal_snapshot_payload(snapshot_type: str):
    projection = current_equity_portal_snapshot(snapshot_type)
    if projection is None:
        raise HTTPException(
            status_code=503,
            detail=f"Published {snapshot_type} portal snapshot is unavailable",
        )
    if not projection["is_fresh"]:
        raise HTTPException(
            status_code=503,
            detail=f"Published {snapshot_type} portal snapshot is stale",
        )
    return projection["payload"]


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
    """Require the administrator-installed canonical database baseline."""
    require_canonical_schema()
    logger.info("Startup complete | canonical equity schema required")


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


PATTERN_WATCH_INTERVALS = ("5m", "15m", "30m", "1h", "1d", "1wk")
PATTERN_FRAME_ROWS = 301


def _load_pattern_frames(tickers: List[str], interval: str) -> dict:
    """Load one consistent detector window, including the newest excluded row."""
    if interval in ("1d", "1wk"):
        days = 2500 if interval == "1wk" else 500
        frames = bulk_load_dataframes(tickers, days)
        if interval == "1wk":
            frames = {
                ticker: frame.resample("W-FRI").agg({
                    "open": "first", "high": "max", "low": "min",
                    "close": "last", "volume": "sum",
                }).dropna(subset=["close"])
                for ticker, frame in frames.items()
            }
    else:
        frames = _load_intraday_frames(tickers, interval, PATTERN_FRAME_ROWS)
    return {
        ticker: frame.tail(PATTERN_FRAME_ROWS)
        for ticker, frame in frames.items()
        if frame is not None and not frame.empty
    }


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


@app.get("/api/health")
async def api_health():
    """Report API liveness and canonical equity storage readiness."""
    from database import get_db_cursor

    with get_db_cursor() as cursor:
        cursor.execute(
            """
                        SELECT role.rolsuper AS role_is_superuser,
                                     current_user = pg_get_userbyid(database.datdba)
                                             AS role_owns_database,
                                     EXISTS (
                                             SELECT 1
                                             FROM pg_class relation
                                             JOIN pg_namespace namespace
                                                 ON namespace.oid = relation.relnamespace
                                             WHERE namespace.nspname = 'public'
                                                 AND relation.relkind IN ('r', 'p', 'v', 'm', 'S')
                                                 AND relation.relowner = role.oid
                                     ) AS role_owns_public_relations,
                   to_regclass('public.equity_bar_revisions') IS NOT NULL
                       AS bar_revisions_ready,
                   to_regclass('public.equity_current_bar_projection') IS NOT NULL
                       AS bar_projection_ready,
                   to_regclass('public.equity_canonical_bars') IS NOT NULL
                       AS canonical_view_ready,
                   to_regclass('public.stock_prices_daily') IS NULL
                       AND to_regclass('public.stock_prices_hourly') IS NULL
                       AND to_regclass('public.stock_prices_intraday') IS NULL
                                             AS legacy_public_relations_absent,
                                     (
                                             SELECT COUNT(*) = 20
                                             FROM equity_portal_current_projections projection
                                             JOIN equity_portal_snapshots snapshot
                                                 ON snapshot.snapshot_id = projection.snapshot_id
                                             CROSS JOIN equity_portal_source_state source
                                             WHERE source.singleton = TRUE
                                                 AND snapshot.source_generation = source.generation
                                     ) AS portal_snapshots_ready
                        FROM pg_roles role
                        CROSS JOIN pg_database database
            WHERE role.rolname = current_user
                            AND database.datname = current_database()
            """
        )
        storage = dict(cursor.fetchone())
    storage["restricted_role_ready"] = not any((
        storage.pop("role_is_superuser"),
        storage.pop("role_owns_database"),
        storage.pop("role_owns_public_relations"),
    ))
    canonical_ready = all(
        value for key, value in storage.items() if key != "restricted_role_ready"
    )
    production = os.getenv("APP_ENV", "development").lower() == "production"
    ready = canonical_ready and (storage["restricted_role_ready"] or not production)
    return {
        "status": "healthy" if ready else "degraded",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "canonical_storage": storage,
    }


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


def _build_ticker_confluence_zones(setups: dict[str, dict]) -> list[dict]:
    daily = setups.get("1d") or next(iter(setups.values()), None)
    if not daily:
        return []
    current_price = float(daily["last_close"])
    daily_atr = float(daily["technicals"].get("atr") or 0)
    tolerance = max(current_price * 0.003, daily_atr * 0.25)
    references = []

    def family_of(source: str) -> str:
        value = source.lower()
        if "fib" in value:
            return "fibonacci"
        if "volume pivot" in value:
            return "volume_pivot"
        if "ema" in value or "sma" in value or "moving" in value:
            return "moving_average"
        if "gap" in value or "fvg" in value:
            return "gap_fvg"
        if "vwap" in value:
            return "vwap"
        return "price_action"

    def add_reference(interval: str, label: str, low: float, high: float,
                      source: str, family: str | None = None,
                      qualifier: str | None = None):
        if not all(math.isfinite(value) and value > 0 for value in (low, high)):
            return
        lo, hi = sorted((float(low), float(high)))
        references.append({
            "interval": interval,
            "label": label,
            "low": round(lo, 2),
            "high": round(hi, 2),
            "price": round((lo + hi) / 2, 2),
            "source": source,
            "family": family or family_of(source),
            "qualifier": qualifier,
        })

    for interval, setup in setups.items():
        technicals = setup["technicals"]
        for label, key, family in (
            ("EMA 8", "ema8", "moving_average"),
            ("EMA 21", "ema21", "moving_average"),
            ("EMA 50", "ema50", "moving_average"),
            ("SMA 50", "ma50", "moving_average"),
            ("SMA 100", "ma100", "moving_average"),
            ("SMA 200", "ma200", "moving_average"),
            ("VWAP 20", "vwap", "vwap"),
        ):
            value = technicals.get(key)
            if value is not None:
                add_reference(interval, label, value, value, label, family)

        for level in setup.get("targets", []) + setup.get("stops", []):
            label = level["level"]
            if level["source"] == "ATR":
                atr = float(technicals.get("atr") or 0)
                multiple = abs(float(level["price"]) - float(setup["last_close"])) / atr if atr > 0 else 0
                sign = "+" if float(level["price"]) >= float(setup["last_close"]) else "−"
                label = f"ATR {sign}{multiple:.0f}×"
            add_reference(
                interval, label, level["price"], level["price"],
                level["source"],
            )
        for zone in setup.get("zones", []):
            add_reference(
                interval, zone["name"], zone["low"], zone["high"], zone["source"],
                qualifier=zone.get("qualifier"),
            )

        fibonacci = setup.get("strategy_results", {}).get("fibonacci")
        if fibonacci:
            for level in fibonacci.get("active_leg", {}).get("levels", []):
                add_reference(
                    interval, f"Provisional Fib {level['name']}",
                    level["price"], level["price"], "Fibonacci provisional",
                    "fibonacci",
                )
            for level in fibonacci.get("target_levels", []):
                add_reference(
                    interval, f"Confirmed Fib {level['name']}",
                    level["price"], level["price"], "Fibonacci confirmed",
                    "fibonacci",
                )

    unique = {}
    for reference in references:
        key = (
            reference["interval"], reference["family"], reference["label"],
            reference["low"], reference["high"],
        )
        unique[key] = reference
    references = sorted(unique.values(), key=lambda item: item["price"])

    clusters = []
    for reference in references:
        candidates = [
            cluster for cluster in clusters
            if abs(reference["price"] - cluster["center"]) <= tolerance
        ]
        if not candidates:
            clusters.append({
                "low": reference["low"],
                "high": reference["high"],
                "center": reference["price"],
                "references": [reference],
            })
            continue
        cluster = min(
            candidates, key=lambda item: abs(reference["price"] - item["center"])
        )
        cluster["low"] = min(cluster["low"], reference["low"])
        cluster["high"] = max(cluster["high"], reference["high"])
        cluster["references"].append(reference)
        cluster["center"] = sum(
            item["price"] for item in cluster["references"]
        ) / len(cluster["references"])

    zones = []
    for cluster in clusters:
        low, high = cluster["low"], cluster["high"]
        midpoint = (low + high) / 2
        families = sorted({item["family"] for item in cluster["references"]})
        intervals = sorted(
            {item["interval"] for item in cluster["references"]},
            key=lambda value: {"1h": 0, "1d": 1, "1wk": 2}.get(value, 9),
        )
        if low <= current_price <= high:
            role = "ACTIVE"
        elif high < current_price:
            role = "SUPPORT"
        else:
            role = "RESISTANCE"
        if len(intervals) >= 2 and len(families) >= 2:
            strength = "STRONG_CONFLUENCE"
        elif len(families) >= 2 or len(intervals) >= 2:
            strength = "CONFLUENCE"
        else:
            strength = "SINGLE_REFERENCE"

        confirmations = []
        for interval, setup in setups.items():
            for pattern in setup.get("candlestick_patterns", []):
                if pattern["high"] >= low - tolerance and pattern["low"] <= high + tolerance:
                    confirmations.append({**pattern, "interval": interval})

        zones.append({
            "low": round(low, 2),
            "high": round(high, 2),
            "midpoint": round(midpoint, 2),
            "distance_pct": round((midpoint - current_price) / current_price * 100, 2),
            "role": role,
            "strength": strength,
            "intervals": intervals,
            "families": families,
            "references": cluster["references"],
            "confirmations": confirmations,
        })
    return sorted(zones, key=lambda zone: abs(zone["distance_pct"]))


@app.get("/api/stock/{ticker}/trade-setup/multi")
async def get_multi_trade_setup(ticker: str, refresh: bool = False):
    """Return synchronized 1h/1d/1wk/1mo setups and confluence."""
    symbol = ticker.upper()
    materialized_intervals = _materialized_setup_intervals()
    cache_key = (
        f"trade_setup_multi_v13_{symbol}_"
        f"{','.join(materialized_intervals) or 'legacy'}"
    )
    if not refresh:
        cached = _get_cached(cache_key, "trade_setup_multi")
        if cached is not None:
            return _with_materialized_setups(cached)
    if refresh:
        clear_bulk_cache()
        _invalidate_prefix(f"trade_setup_v22_{symbol}_")
        _invalidate_prefix(cache_key)

    import pandas as pd

    loop = asyncio.get_event_loop()
    daily_frames, hourly_rows = await asyncio.gather(
        loop.run_in_executor(executor, bulk_load_dataframes, [symbol], 4000),
        loop.run_in_executor(executor, get_hourly_data, symbol, 500, None),
    )
    shared_daily = daily_frames.get(symbol)
    shared_hourly = None
    if hourly_rows:
        shared_hourly = pd.DataFrame(hourly_rows)
        shared_hourly["datetime"] = pd.to_datetime(shared_hourly["datetime"], utc=True)
        shared_hourly.set_index("datetime", inplace=True)
        if "ticker" in shared_hourly.columns:
            shared_hourly = shared_hourly.drop(columns=["ticker"])
        for column in ("open", "high", "low", "close", "volume"):
            if column in shared_hourly.columns:
                shared_hourly[column] = shared_hourly[column].astype(float)
    shared_frames = {"daily": shared_daily, "hourly": shared_hourly}

    intervals = tuple(
        interval for interval in ("1h", "1d", "1wk", "1mo")
        if interval not in materialized_intervals
    )
    responses = await asyncio.gather(
        *(_compute_trade_setup(symbol, interval, False, shared_frames) for interval in intervals),
        return_exceptions=True,
    )
    setups = {}
    errors = {}
    setup_sources = {}
    for interval, response in zip(intervals, responses):
        if isinstance(response, Exception):
            errors[interval] = str(response)
        else:
            setups[interval] = response
            setup_sources[interval] = "LEGACY_REQUEST_TIME"

    result = {
        "ticker": symbol,
        "setups": setups,
        "errors": errors,
        "setup_sources": setup_sources,
        "confluence_zones": _build_ticker_confluence_zones({
            interval: setup for interval, setup in setups.items() if interval != "1mo"
        }),
        "as_of": datetime.now(timezone.utc).isoformat(),
    }
    _set_cached(cache_key, result)
    return _with_materialized_setups(result)


def _materialized_setup_intervals() -> tuple[str, ...]:
    return tuple(
        interval for interval, enabled in (
            ("30m", MATERIALIZED_30M_SETUP_ENABLED),
            ("1h", MATERIALIZED_1H_SETUP_ENABLED),
            ("1d", MATERIALIZED_1D_SETUP_ENABLED),
            ("1wk", MATERIALIZED_1WK_SETUP_ENABLED),
            ("1mo", MATERIALIZED_1MO_SETUP_ENABLED),
        )
        if enabled
    )


def _with_materialized_setups(legacy_result: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(legacy_result)
    intervals = _materialized_setup_intervals()
    if not intervals:
        return result
    read_metrics = result.setdefault("setup_read_metrics", {})
    for interval in intervals:
        result["setups"].pop(interval, None)
        result["setup_sources"].pop(interval, None)
        result["errors"].pop(interval, None)
        projection = current_trade_setup_projection(result["ticker"], interval)
        metrics = {
            "process_started_at": API_PROCESS_STARTED_AT.isoformat(),
            "read_mode": "MATERIALIZED",
            "source": "MATERIALIZED_CURRENT_PROJECTION",
        }
        if projection is None:
            result["errors"][interval] = (
                f"Published materialized {interval} setup unavailable"
            )
            read_metrics[interval] = {**metrics, "status": "MISSING"}
            continue
        metrics.update({
            "analysis_run_id": str(projection["analysis_run_id"]),
            "evidence_id": str(projection["evidence_id"]),
            "expected_market_time": projection["expected_market_time"].isoformat(),
            "market_time": projection["market_time"].isoformat(),
            "observed_at": projection["observed_at"].isoformat(),
            "projection_read_latency_ms": projection["read_latency_ms"],
            "published_at": projection["published_at"].isoformat(),
            "staleness_seconds": projection["staleness_seconds"],
            "status": "READY" if projection["is_fresh"] else "STALE",
        })
        read_metrics[interval] = metrics
        if not projection["is_fresh"]:
            result["errors"][interval] = (
                f"Published materialized {interval} setup is stale for the latest completed slot"
            )
            continue
        result["setups"][interval] = projection["payload"]
        result["setup_sources"][interval] = "MATERIALIZED_CURRENT_PROJECTION"
    result["confluence_zones"] = _build_ticker_confluence_zones({
        interval: setup
        for interval, setup in result["setups"].items()
        if interval != "1mo"
    })
    return result


@app.get("/api/latest-price-date")
async def latest_price_date(refresh: bool = False):
    """Return the latest canonical daily date without process-local caching."""
    return {"latest_date": get_latest_price_date()}


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
    """Get chart bars from exact materialized inputs or the legacy price store."""
    prefix = "chart_daily" if interval in ("1d", "1wk") else "chart_intraday"
    materialized_chart = (
        MATERIALIZED_PATTERN_WATCH_ENABLED
        and interval in ("5m", "15m", "30m", "1h", "1d", "1wk")
    ) or (
        interval == "30m" and (
            MATERIALIZED_30M_SETUP_ENABLED
        )
    )
    source_key = "materialized" if materialized_chart else "legacy"
    cache_key = f"{prefix}_{ticker.upper()}_{interval}_{period}_{source_key}"
    if not refresh and not materialized_chart:
        cached = _get_cached(cache_key, prefix)
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()
    if materialized_chart:
        calendar_period_days = {
            "1d": 1, "5d": 5, "1mo": 30, "3mo": 90,
            "6mo": 180, "1y": 365, "2y": 730,
            "4y": 1460, "5y": 1825,
        }
        requested_days = calendar_period_days.get(period, 365)
        bars_per_day = {
            "5m": 78, "15m": 26, "30m": 13, "1h": 7, "1d": 1,
        }.get(
            interval, 1
        )
        requested_bars = (
            requested_days // 7 + 2
            if interval == "1wk"
            else requested_days * bars_per_day
        )
        projection = current_chart_bar_projection(
            ticker,
            interval,
            limit=requested_bars,
        )
        if projection is None and interval in ("1d", "1wk"):
            materialized_chart = False
            cache_key = f"{prefix}_{ticker.upper()}_{interval}_{period}_legacy"
        elif projection is None or not projection["is_fresh"]:
            raise HTTPException(
                status_code=503,
                detail={
                    "code": "MATERIALIZED_CHART_BARS_UNAVAILABLE",
                    "interval": interval,
                    "reason": "MISSING" if projection is None else "STALE",
                },
            )
        if materialized_chart:
            bars = projection["bars"]
            if bars:
                cutoff = (
                    bars[-1]["bar_start"] - timedelta(days=requested_days)
                ).replace(hour=0, minute=0, second=0, microsecond=0)
                bars = tuple(row for row in bars if row["bar_start"] >= cutoff)
            records = [{
                "time": int(row["bar_start"].timestamp()),
                "open": round(float(row["open_price"]), 2),
                "high": round(float(row["high_price"]), 2),
                "low": round(float(row["low_price"]), 2),
                "close": round(float(row["close_price"]), 2),
                "volume": int(row["volume"]),
            } for row in bars]
            return records
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


def _pattern_reference_close(frame) -> float | None:
    """Return the last valid close before the deliberately excluded newest bar."""
    if frame is None or len(frame) < 2 or "close" not in frame.columns:
        return None
    for value in reversed(frame.iloc[:-1]["close"].tolist()):
        try:
            close = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(close):
            return round(close, 2)
    return None


def _materialized_pattern_report(interval: str, ticker: str | None = None) -> dict:
    report = current_pattern_watch_projection(interval, ticker=ticker)
    if not report["is_fresh"]:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "MATERIALIZED_PATTERN_WATCH_UNAVAILABLE",
                "interval": interval,
                "expected_market_time": report["expected_market_time"].isoformat(),
                "market_times": [value.isoformat() for value in report["market_times"]],
            },
        )
    return report


def _sort_pattern_watch_rows(rows: list[dict], intervals=PATTERN_WATCH_INTERVALS) -> None:
    readiness_rank = {"AT_EDGE": 0, "NEAR_EDGE": 1, "FORMING": 2}
    grade_rank = {"STRONG_GEOMETRY": 0, "VALID_GEOMETRY": 1}
    rows.sort(key=lambda row: (
        readiness_rank[row["pattern"]["readiness"]],
        grade_rank[row["pattern"]["grade"]],
        row["pattern"]["edge_distance_atr"],
        -(row["pattern"]["upper_touches"] + row["pattern"]["lower_touches"]),
        intervals.index(row["interval"]),
    ))


@app.get("/api/stock/{ticker}/chart-patterns")
async def get_chart_patterns(
    ticker: str,
    interval: str = Query(default="1d", regex="^(1m|5m|15m|30m|1h|1d|1wk)$"),
    refresh: bool = False,
):
    """Current chart-only formations for one interval, using completed bars."""
    symbol = ticker.upper()
    if MATERIALIZED_PATTERN_WATCH_ENABLED:
        report = _materialized_pattern_report(interval, symbol)
        return {
            "ticker": symbol,
            "interval": interval,
            "status": "FORMING_RESEARCH",
            "last_close": report["last_closes"].get(symbol),
            "patterns": [row["pattern"] for row in report["results"]],
            "computed_at": report["computed_at"],
            "read_source": "MATERIALIZED_CURRENT_PROJECTION",
            "read_metrics": {
                "analysis_run_id": report["analysis_run_id"],
                "expected_market_time": report["expected_market_time"].isoformat(),
                "projection_read_latency_ms": report["read_latency_ms"],
            },
        }
    prefix = "chart_daily" if interval in ("1d", "1wk") else "chart_intraday"
    cache_key = f"{CHART_PATTERN_DETAIL_CACHE}_{symbol}_{interval}"
    if not refresh:
        cached = _get_cached(cache_key, prefix)
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()
        _invalidate_prefix(cache_key)
        _invalidate_prefix(f"{CHART_PATTERN_TICKER_CACHE}_{symbol}")

    frame = _load_pattern_frames([symbol], interval).get(symbol)
    patterns = detect_forming_patterns(frame) if frame is not None else []
    result = {
        "ticker": symbol,
        "interval": interval,
        "status": "FORMING_RESEARCH",
        "last_close": _pattern_reference_close(frame),
        "patterns": patterns,
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }
    _set_cached(cache_key, result)
    return result


@app.get("/api/stock/{ticker}/price-channel")
async def get_price_channel(
    ticker: str,
    interval: str = Query(default="1d", regex="^(5m|15m|30m|1h|1d|1wk)$"),
    refresh: bool = False,
):
    """Primary active directional price channel for one completed-bar interval."""
    symbol = ticker.upper()
    if MATERIALIZED_PATTERN_WATCH_ENABLED:
        report = _materialized_pattern_report(interval, symbol)
        return {
            "ticker": symbol,
            "interval": interval,
            "status": "CHANNEL_RESEARCH",
            "last_close": report["last_closes"].get(symbol),
            "channel": report["channels"].get(symbol),
            "computed_at": report["computed_at"],
            "read_source": "MATERIALIZED_CURRENT_PROJECTION",
            "read_metrics": {
                "analysis_run_id": report["analysis_run_id"],
                "expected_market_time": report["expected_market_time"].isoformat(),
                "projection_read_latency_ms": report["read_latency_ms"],
            },
        }
    prefix = "chart_daily" if interval in ("1d", "1wk") else "chart_intraday"
    cache_key = f"{PRICE_CHANNEL_CACHE}_{symbol}_{interval}"
    if not refresh:
        cached = _get_cached(cache_key, prefix)
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()
        _invalidate_prefix(cache_key)

    frame = _load_pattern_frames([symbol], interval).get(symbol)
    channel = detect_price_channel(frame) if frame is not None else None
    result = {
        "ticker": symbol,
        "interval": interval,
        "status": "CHANNEL_RESEARCH",
        "last_close": _pattern_reference_close(frame),
        "channel": channel,
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }
    _set_cached(cache_key, result)
    return result


@app.get("/api/chart-patterns/ticker/{ticker}")
async def scan_ticker_chart_patterns(ticker: str, refresh: bool = False):
    """Find current forming patterns for one ticker across Pattern Watch intervals."""
    symbol = ticker.upper()
    if MATERIALIZED_PATTERN_WATCH_ENABLED:
        reports = [
            _materialized_pattern_report(interval, symbol)
            for interval in PATTERN_WATCH_INTERVALS
        ]
        rows = [row for report in reports for row in report["results"]]
        _sort_pattern_watch_rows(rows)
        return {
            "interval": "all",
            "scanned": 1,
            "matched_tickers": 1 if rows else 0,
            "results": rows,
            "cross_frame": summarize_cross_frame_patterns(rows),
            "computed_at": max(
                report["computed_at"] for report in reports if report["computed_at"]
            ),
            "read_source": "MATERIALIZED_CURRENT_PROJECTION",
            "read_metrics": {
                report_interval: {
                    "analysis_run_id": report["analysis_run_id"],
                    "expected_market_time": report["expected_market_time"].isoformat(),
                    "projection_read_latency_ms": report["read_latency_ms"],
                }
                for report_interval, report in zip(PATTERN_WATCH_INTERVALS, reports)
            },
        }
    cache_key = f"{CHART_PATTERN_TICKER_CACHE}_{symbol}"
    if not refresh:
        cached = _get_cached(cache_key, "chart_pattern_ticker")
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()
        _invalidate_prefix(cache_key)
        _invalidate_prefix(f"{CHART_PATTERN_DETAIL_CACHE}_{symbol}_")
        _invalidate_prefix(f"{PRICE_CHANNEL_CACHE}_{symbol}_")

    intervals = PATTERN_WATCH_INTERVALS
    responses, channel_responses = await asyncio.gather(
        asyncio.gather(*(
        get_chart_patterns(symbol, interval, False)
        for interval in intervals
        )),
        asyncio.gather(*(
            get_price_channel(symbol, interval, False)
            for interval in intervals
        )),
    )
    channel_by_interval = {
        response["interval"]: response["channel"]
        for response in channel_responses
    }

    from database import get_db_cursor
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT sector FROM selected_tickers WHERE ticker = %s",
            (symbol,),
        )
        sector_row = cursor.fetchone()
    sector = sector_row["sector"] if sector_row else None

    rows = [
        {
            "ticker": symbol,
            "sector": sector,
            "interval": response["interval"],
            "last_close": response["last_close"],
            "pattern": pattern,
            "channel": channel_by_interval.get(response["interval"]),
        }
        for response in responses
        for pattern in response["patterns"]
    ]
    _sort_pattern_watch_rows(rows, intervals)
    result = {
        "interval": "all",
        "scanned": 1,
        "matched_tickers": 1 if rows else 0,
        "results": rows,
        "cross_frame": summarize_cross_frame_patterns(rows),
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }
    _set_cached(cache_key, result)
    return result


@app.get("/api/chart-patterns/scan")
async def scan_chart_patterns(
    interval: str = Query(default="1d", regex="^(5m|15m|30m|1h|1d|1wk)$"),
    limit: int = Query(default=1000, ge=1, le=1500),
    refresh: bool = False,
):
    """Find current forming chart patterns across the active ticker universe."""
    if MATERIALIZED_PATTERN_WATCH_ENABLED:
        report = _materialized_pattern_report(interval)
        rows = report["results"][:limit]
        _sort_pattern_watch_rows(rows, (interval,))
        return {
            "interval": interval,
            "scanned": report["scanned"],
            "matched_tickers": len({row["ticker"] for row in rows}),
            "results": rows,
            "cross_frame": None,
            "computed_at": report["computed_at"],
            "read_source": "MATERIALIZED_CURRENT_PROJECTION",
            "read_metrics": {
                "analysis_run_id": report["analysis_run_id"],
                "expected_market_time": report["expected_market_time"].isoformat(),
                "projection_read_latency_ms": report["read_latency_ms"],
            },
        }
    cache_key = f"{CHART_PATTERN_SCAN_CACHE}_{interval}_{limit}"
    if not refresh:
        cached = _get_cached(cache_key, "chart_pattern_scan")
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()
        _invalidate_prefix(f"{CHART_PATTERN_SCAN_CACHE}_")
        _invalidate_prefix(f"{CHART_PATTERN_DETAIL_CACHE}_")
        _invalidate_prefix(f"{CHART_PATTERN_TICKER_CACHE}_")
        _invalidate_prefix(f"{PRICE_CHANNEL_CACHE}_")

    selected = get_selected_tickers(active_only=True)
    if not selected:
        return {
            "interval": interval, "scanned": 0, "matched_tickers": 0,
            "results": [], "cross_frame": None,
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }

    loop = asyncio.get_event_loop()
    frames = await loop.run_in_executor(
        executor, _load_pattern_frames, selected, interval,
    )

    from database import get_db_cursor
    with get_db_cursor() as cursor:
        cursor.execute(
            "SELECT ticker, sector FROM selected_tickers WHERE ticker = ANY(%s)",
            (selected,),
        )
        sector_by_ticker = {
            row["ticker"]: row["sector"] for row in cursor.fetchall()
        }

    frame_items = [
        (ticker, frame) for ticker, frame in frames.items()
        if frame is not None and not frame.empty
    ]
    pattern_detections = await asyncio.gather(*(
        loop.run_in_executor(executor, detect_forming_patterns, frame)
        for _, frame in frame_items
    ))
    channel_frame_items = [
        (ticker, frame)
        for (ticker, frame), patterns in zip(frame_items, pattern_detections)
        if patterns
    ]
    channel_detections = await asyncio.gather(*(
        loop.run_in_executor(executor, detect_price_channel, frame)
        for _, frame in channel_frame_items
    ))
    channel_by_ticker = {
        ticker: channel
        for (ticker, _), channel in zip(channel_frame_items, channel_detections)
    }
    rows = []
    for (ticker, frame), patterns in zip(frame_items, pattern_detections):
        channel = channel_by_ticker.get(ticker)
        for pattern in patterns:
            rows.append({
                "ticker": ticker,
                "sector": sector_by_ticker.get(ticker),
                "interval": interval,
                "last_close": _pattern_reference_close(frame),
                "pattern": pattern,
                "channel": channel,
            })
    readiness_rank = {"AT_EDGE": 0, "NEAR_EDGE": 1, "FORMING": 2}
    grade_rank = {"STRONG_GEOMETRY": 0, "VALID_GEOMETRY": 1}
    rows.sort(key=lambda row: (
        readiness_rank[row["pattern"]["readiness"]],
        grade_rank[row["pattern"]["grade"]],
        row["pattern"]["edge_distance_atr"],
        -(row["pattern"]["upper_touches"] + row["pattern"]["lower_touches"]),
        row["ticker"],
    ))
    matched_tickers = len({row["ticker"] for row in rows})
    rows = rows[:limit]
    result = {
        "interval": interval,
        "scanned": len(frames),
        "matched_tickers": matched_tickers,
        "results": rows,
        "cross_frame": None,
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }
    _set_cached(cache_key, result)
    return result


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
    if (
        MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED
        and tickers is None and scan_date is None and interval == "1d"
    ):
        return _portal_snapshot_payload("SCAN_GAPS_1D")
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
            gaps = scan_gap_strategies(ticker, df, interval=interval)
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
    if (
        MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED
        and tickers is None and scan_date is None
        and interval == "1d" and lookback == 50
    ):
        return _portal_snapshot_payload("SCAN_FVG_1D_50")
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
    if (
        MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED
        and tickers is None and scan_date is None and interval == "1d"
        and short_period == 9 and long_period == 21
    ):
        return _portal_snapshot_payload("SCAN_MA_1D_9_21")
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
    if (
        MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED
        and tickers is None and scan_date is None and interval == "1d"
    ):
        return _portal_snapshot_payload("SCAN_MOMENTUM_1D")
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
    if (
        MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED
        and tickers is None and scan_date is None and interval == "1d"
    ):
        return _portal_snapshot_payload("SCAN_BEARISH_1D")
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
    if (
        MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED
        and tickers is None and scan_date is None and interval == "1d"
        and min_swing_pct == 5.0
    ):
        return _portal_snapshot_payload("SCAN_FIBONACCI_1D_5")
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
    if MATERIALIZED_PORTAL_SNAPSHOTS_ENABLED:
        return _portal_snapshot_payload("MARKET_REGIME")
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
    if (
        MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED
        and tickers is None and scan_date is None and min_swing_pct == 5.0
    ):
        return _portal_snapshot_payload("SCAN_ALL_1D_5")
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
    if MATERIALIZED_PORTAL_SNAPSHOTS_ENABLED and scan_date is None:
        return _portal_snapshot_payload("TICKER_OVERVIEW")
    cache_key = f"overview_v2_{scan_date or 'latest'}"
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
    if (
        MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED
        and days == 5 and tickers is None
        and short_period == 9 and long_period == 21
    ):
        snapshot_type = {
            "gaps": "STREAK_GAPS_5",
            "ma-crossover": "STREAK_MA_5",
            "momentum-pullback": "STREAK_MOMENTUM_5",
            "bearish-bounce": "STREAK_BEARISH_5",
            "fibonacci": "STREAK_FIBONACCI_5",
        }[strategy]
        return _portal_snapshot_payload(snapshot_type)

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
    if (
        MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED
        and days == 3 and fib_swing_pct == 5.0
    ):
        return _portal_snapshot_payload("STREAK_SUMMARY_3_5")
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
    """Public single-timeframe setup endpoint."""
    return await _compute_trade_setup(ticker, interval, refresh)


async def _compute_trade_setup(ticker: str, interval: str = "1d",
                               refresh: bool = False,
                               shared_frames: Optional[dict] = None):
    """
    Comprehensive trade setup analysis for a single ticker.
    Runs all strategies and synthesizes momentum, direction, entry/exit criteria,
    level retest detection with candle highs/lows, and multi-timeframe EMA alignment.

    interval: '1mo' (monthly), '1wk' (weekly), '1d' (daily), '1h' (hourly), '30m', '15m', '5m'
    """
    cache_key = f"trade_setup_v22_{ticker.upper()}_{interval}"
    if not refresh:
        cached = _get_cached(cache_key, f"trade_setup_{interval}")
        if cached is not None:
            return cached
    if refresh:
        clear_bulk_cache()
        _invalidate_prefix(f"trade_setup_multi_v11_{ticker.upper()}")

    import numpy as np
    import pandas as pd

    ticker = ticker.upper()
    valid_intervals = {"1mo", "1wk", "1d", "1h", "30m", "15m", "5m"}
    if interval not in valid_intervals:
        interval = "1d"

    loop = asyncio.get_event_loop()

    shared_daily = shared_frames.get("daily") if shared_frames else None
    shared_hourly = shared_frames.get("hourly") if shared_frames else None

    # Load primary data based on interval
    if interval == "1d":
        if shared_daily is not None:
            df = shared_daily.tail(1600).copy()
        else:
            frames = await loop.run_in_executor(executor, bulk_load_dataframes, [ticker], 1600)
            df = frames.get(ticker)
    elif interval in ("1wk", "1mo"):
        if shared_daily is not None:
            daily_df = shared_daily
        else:
            frames = await loop.run_in_executor(executor, bulk_load_dataframes, [ticker], 4000)
            daily_df = frames.get(ticker)
        if daily_df is None or len(daily_df) < 60:
            raise HTTPException(status_code=404, detail=f"Insufficient daily history for {ticker} to build weekly bars")
        resample_rule = "W-FRI" if interval == "1wk" else "ME"
        grouped = daily_df.resample(resample_rule)
        df = grouped.agg({
            "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
        }).dropna(subset=["close"])
        source_dates = grouped["close"].apply(lambda values: values.index[-1] if len(values) else pd.NaT)
        df.index = pd.DatetimeIndex(source_dates.loc[df.index])
    elif interval == "1h":
        if shared_hourly is not None:
            df = shared_hourly.copy()
        else:
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

    # One step down from the selected interval, used to confirm or contradict it.
    confirm_interval = {"1mo": "1wk", "1wk": "1d", "1d": "1h", "1h": "1d"}.get(interval, "1h")
    df_confirm = None
    if confirm_interval == "1d":
        if shared_daily is not None:
            candidate = shared_daily.tail(400).copy()
        else:
            confirm_frames = await loop.run_in_executor(executor, bulk_load_dataframes, [ticker], 400)
            candidate = confirm_frames.get(ticker)
        if candidate is not None and len(candidate) >= 20:
            df_confirm = candidate
    elif confirm_interval == "1wk":
        candidate = daily_df.resample("W-FRI").agg({
            "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
        }).dropna(subset=["close"])
        if len(candidate) >= 20:
            df_confirm = candidate.tail(200).copy()
    else:
        if shared_hourly is not None and len(shared_hourly) >= 20:
            df_confirm = shared_hourly.tail(200).copy()
        else:
            confirm_rows = await loop.run_in_executor(executor, get_hourly_data, ticker, 200, None)
            if confirm_rows and len(confirm_rows) >= 20:
                df_confirm = pd.DataFrame(confirm_rows)
                df_confirm['datetime'] = pd.to_datetime(df_confirm['datetime'], utc=True)
                df_confirm.set_index('datetime', inplace=True)
                if 'ticker' in df_confirm.columns:
                    df_confirm = df_confirm.drop(columns=['ticker'])
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    if col in df_confirm.columns:
                        df_confirm[col] = df_confirm[col].astype(float)

    last_close = float(df.iloc[-1]["close"])
    last_date = df.index[-1].strftime("%Y-%m-%d") if hasattr(df.index[-1], "strftime") else str(df.index[-1])

    # --- Run all strategies ---
    gaps = (
        scan_gap_strategies(ticker, df, interval=interval)
        if len(df) >= 20 else []
    )
    fvgs = scan_fair_value_gaps(ticker, df) if len(df) >= 20 else []
    ma = scan_moving_average_crossover(ticker, df, interval=interval) if len(df) >= 26 else None
    pullback = scan_momentum_pullback(ticker, df, interval=interval) if len(df) >= 210 else None
    bounce = scan_bearish_bounce(ticker, df, interval=interval) if len(df) >= 210 else None
    fib_swing_pct = calculate_fibonacci_swing_pct(df, interval)
    fib = scan_fibonacci(ticker, df, min_swing_pct=fib_swing_pct) if len(df) >= 50 else None

    technicals = compute_trade_setup_technicals(df, interval)
    c = technicals.close
    h = technicals.high
    l = technicals.low
    v = technicals.volume
    ema8 = technicals.ema8
    ema_bullish_stack = technicals.ema_bullish_stack
    ema_bearish_stack = technicals.ema_bearish_stack
    ema_alignment = technicals.ema_alignment
    ema_alignment_detail = technicals.ema_alignment_detail
    ema8_val = technicals.ema8_value
    ema21_val = technicals.ema21_value
    ema50_ema = technicals.ema50_value
    macd_value = technicals.macd
    macd_signal = technicals.macd_signal
    macd_histogram = technicals.macd_histogram
    macd_histogram_previous = technicals.macd_histogram_previous
    macd_state = technicals.macd_state
    dist_to_8ema = technicals.distance_to_ema8
    dist_to_21ema = technicals.distance_to_ema21
    rsi = technicals.rsi
    atr14 = technicals.atr14
    vwap = technicals.vwap
    ma50 = technicals.ma50
    ma200 = technicals.ma200

    structure_fibonacci_levels = []
    if fib:
        structure_fibonacci_levels.extend(fib.get("retracement_levels", []))
        structure_fibonacci_levels.extend(
            fib.get("active_leg", {}).get("levels", [])
            if fib.get("active_leg") else []
        )
    structure_analysis = analyze_price_structures(df, structure_fibonacci_levels)
    structural_patterns = structure_analysis["patterns"]
    volume_pivot_zones = structure_analysis["volume_pivot_zones"]

    candle_patterns = detect_setup_candlesticks(df)
    golden_cross = detect_golden_cross(technicals)
    confirmation = compute_ema_confirmation(df_confirm)
    confirm_ema8 = confirmation.ema8
    confirm_ema21 = confirmation.ema21
    confirm_ema_alignment = confirmation.alignment

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

    primary_retests = detect_level_retests(
        c, h, l, key_levels, lookback=5, tolerance_pct=0.5
    )

    # Detect retests on the confirmation timeframe
    confirm_retests = []
    if df_confirm is not None and len(df_confirm) >= 10:
        ch = df_confirm["close"].values.astype(float)
        hh = df_confirm["high"].values.astype(float)
        lh = df_confirm["low"].values.astype(float)
        confirm_retests = detect_level_retests(
            ch, hh, lh, key_levels, lookback=10, tolerance_pct=0.3
        )

    momentum_assessment = assess_momentum(technicals)
    momentum = momentum_assessment["state"]
    momentum_detail = momentum_assessment["detail"]

    direction_composition = compose_setup_direction(
        interval=interval,
        technicals=technicals,
        confirmation=confirmation,
        confirmation_interval=confirm_interval,
        primary_retests=primary_retests,
        moving_average=ma,
        momentum_pullback=pullback,
        bearish_bounce=bounce,
        gaps=gaps,
        fair_value_gaps=fvgs,
        golden_cross=golden_cross,
        volume_pivot_zones=volume_pivot_zones,
    )
    direction = direction_composition.direction
    conviction = direction_composition.conviction
    bull_signals = direction_composition.bull_signals
    bear_signals = direction_composition.bear_signals
    signal_reasons = list(direction_composition.signal_reasons)
    support_gaps = list(direction_composition.support_gaps)
    resistance_gaps = list(direction_composition.resistance_gaps)
    bull_fvgs = list(direction_composition.bull_fvgs)
    bear_fvgs = list(direction_composition.bear_fvgs)
    zones = list(direction_composition.zones)

    trade_levels = compose_trade_levels(
        interval=interval,
        technicals=technicals,
        direction=direction_composition,
        primary_retests=primary_retests,
        momentum_pullback=pullback,
        bearish_bounce=bounce,
        fibonacci=fib,
    )
    entries = list(trade_levels.entries)
    targets = list(trade_levels.targets)
    stops = list(trade_levels.stops)

    timing = compose_setup_timing(
        technicals=technicals,
        moving_average=ma,
        primary_retests=primary_retests,
        momentum_pullback=pullback,
        bearish_bounce=bounce,
    )
    duration = compose_setup_duration(ma)
    confluence = compose_setup_confluence(signal_reasons)

    result = {
        "ticker": ticker,
        "last_close": round(last_close, 2),
        "date": last_date,
        "technicals": technicals.payload(),
        "candlestick_patterns": candle_patterns,
        "structural_patterns": structural_patterns,
        "ema_alignment": {
            "primary": ema_alignment,
            "primary_detail": ema_alignment_detail,
            "confirm_interval": confirm_interval,
            "confirm": confirm_ema_alignment,
            "confirm_ema8": confirm_ema8,
            "confirm_ema21": confirm_ema21,
            "multi_tf_agree": (confirm_ema_alignment == "Bullish" and ema_alignment in ("Bullish Stack", "Short-term Bullish"))
                           or (confirm_ema_alignment == "Bearish" and ema_alignment in ("Bearish Stack", "Short-term Bearish"))
                           if confirm_ema_alignment else None,
        },
        "level_retests": {
            "primary": primary_retests[:5],
            "confirm": confirm_retests[:5],
            "confirm_interval": confirm_interval,
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
        "zones": zones,
        "targets": targets[:5],
        "stops": stops[:5],
        "timing": timing,
        "duration": duration,
        "confluence": confluence,
        "strategy_results": {
            "ma_crossover": {"signal": ma["signal"], "spread_pct": ma.get("ma_spread_pct"), "days_since_cross": ma.get("days_since_cross"), "weekly_signal": ma.get("weekly_signal"), "markers": ma.get("markers", [])} if ma else None,
            "momentum_pullback": {"grade": pullback.get("grade"), "score": pullback.get("score")} if pullback else None,
            "bearish_bounce": {"grade": bounce.get("grade"), "score": bounce.get("score")} if bounce else None,
            "gaps": {"support_count": len(support_gaps), "resistance_count": len(resistance_gaps)} if gaps else None,
            "fvg": {"bull_unmitigated": len(bull_fvgs), "bear_unmitigated": len(bear_fvgs), "total": len(fvgs)} if fvgs else None,
            "fibonacci": compose_fibonacci_context(fib),
        }
    }
    # Survives caching, so the UI can show how old this read actually is.
    result["computed_at"] = datetime.now(timezone.utc).isoformat()
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
    interval: Optional[str] = Query(default=None, regex="^(1d|1wk|1h|30m)$"),
    discovery_state: Optional[str] = None,
    min_periods: int = Query(default=20, ge=1, le=1000),
):
    """Horizon-spaced scanner utility metrics. No result is a recommendation."""
    try:
        from equity.scanner_research import event_summary
        return {
            "results": event_summary(interval, discovery_state, min_periods),
            "read_source": "CANONICAL_EQUITY_RESEARCH",
        }
    except HTTPException:
        raise
    except Exception:
        logger.exception("Scanner event summary query failed")
        raise HTTPException(status_code=500, detail="Failed to load scanner event summary")


@app.get("/api/scanner-events/qualification")
async def scanner_event_qualification(
    interval: Optional[str] = Query(default=None, regex="^(1d|1wk|1h|30m)$"),
):
    """Aggregate full-history qualification results. No result is a recommendation."""
    try:
        from equity.scanner_research import (
            OUTCOME_ENTRY_MODEL, qualification_report,
        )
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
            "read_source": "CANONICAL_EQUITY_RESEARCH",
        }
    except HTTPException:
        raise
    except Exception:
        logger.exception("Scanner qualification query failed")
        raise HTTPException(status_code=500, detail="Failed to load scanner qualification")


@app.get("/api/scanner-events/backlog")
async def scanner_event_backlog():
    """Pending and evaluated event/horizon pairs."""
    try:
        from equity.scanner_research import pending_outcome_counts
        return {
            "results": pending_outcome_counts(),
            "read_source": "CANONICAL_EQUITY_RESEARCH",
        }
    except Exception:
        logger.exception("Scanner event backlog query failed")
        raise HTTPException(status_code=500, detail="Failed to load scanner event backlog")


@app.get("/api/scanner-events")
async def scanner_events_endpoint(
    interval: Optional[str] = Query(default=None, regex="^(1d|1wk|1h|30m)$"),
    limit: int = Query(default=100, ge=1, le=500),
):
    """Latest shadow scanner setup lifecycles and completed outcomes."""
    try:
        from equity.scanner_research import recent_events
        return {
            "results": recent_events(interval, limit),
            "read_source": "CANONICAL_EQUITY_RESEARCH",
        }
    except Exception:
        logger.exception("Scanner event list query failed")
        raise HTTPException(status_code=500, detail="Failed to load scanner events")


@app.get("/api/scanner-events/latest-by-ticker")
async def latest_ticker_scanner_signals(
    interval: Optional[str] = Query(default=None, regex="^(1d|1wk|1h|30m)$"),
    limit: int = Query(default=500, ge=1, le=500),
    sessions: int = Query(default=10, ge=1, le=252),
    hourly_sessions: int = Query(default=2, ge=1, le=30),
):
    """Latest observed scanner signal per ticker, within the last `sessions` daily/weekly sessions
    or `hourly_sessions` days for hourly signals."""
    try:
        from equity.scanner_research import latest_ticker_signals
        return {
            "results": latest_ticker_signals(interval, limit, sessions, hourly_sessions),
            "read_source": "CANONICAL_EQUITY_RESEARCH",
        }
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
    if MATERIALIZED_PORTAL_SNAPSHOTS_ENABLED:
        return {
            "sessions": sessions,
            "results": _portal_snapshot_payload(f"SECTOR_PERFORMANCE_{sessions}"),
        }
    try:
        from equity.sector_research import latest_sector_performance
        return {"sessions": sessions, "results": latest_sector_performance(sessions)}
    except Exception:
        logger.exception("Scanner sector performance query failed")
        raise HTTPException(status_code=500, detail="Failed to load sector performance")


@app.get("/api/sector-intelligence")
async def sector_intelligence_endpoint(leader_limit: int = Query(default=5, ge=1, le=20)):
    """Sector rollup: rotation across horizons, discovery-state mix, and cross-sectional skew."""
    if MATERIALIZED_PORTAL_SNAPSHOTS_ENABLED and leader_limit == 5:
        return _portal_snapshot_payload("SECTOR_INTELLIGENCE")
    try:
        from equity.sector_research import sector_intelligence
        return sector_intelligence(leader_limit)
    except Exception:
        logger.exception("Sector intelligence query failed")
        raise HTTPException(status_code=500, detail="Failed to load sector intelligence")


@app.get("/api/stock/{ticker}/scanner-events")
async def ticker_scanner_events(
    ticker: str,
    limit: int = Query(default=100, ge=1, le=252),
    daily_sessions: int = Query(default=21, ge=1, le=252),
    hourly_sessions: int = Query(default=5, ge=1, le=30),
):
    """Scanner lifecycles from recent daily/weekly and hourly session windows."""
    try:
        from equity.scanner_research import ticker_events
        return {
            "ticker": ticker.upper(),
            "daily_sessions": daily_sessions,
            "hourly_sessions": hourly_sessions,
            "events": ticker_events(ticker, limit, daily_sessions, hourly_sessions),
            "read_source": "CANONICAL_EQUITY_RESEARCH",
        }
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
