import os
import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor
from decimal import Decimal
from dotenv import load_dotenv
from contextlib import contextmanager

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

load_dotenv()

# Canonical timezone for all intraday/hourly timestamps
_ET = ZoneInfo("America/New_York")

# Allowed interval values for intraday table
VALID_INTRADAY_INTERVALS = {"1m", "5m"}


def is_valid_ohlcv(open_price, high, low, close_price, volume) -> bool:
    """Return True if OHLCV values are sane for a stock candle."""
    try:
        o, h, l, c = float(open_price), float(high), float(low), float(close_price)
        v = int(volume)
    except (TypeError, ValueError):
        return False
    if o <= 0 or h <= 0 or l <= 0 or c <= 0:
        return False
    if h < l:
        return False
    if h < o or h < c:
        return False
    if l > o or l > c:
        return False
    if v < 0:
        return False
    return True

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "stocks_db"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

# Fail fast if required credentials are missing
if not DB_CONFIG["user"] or not DB_CONFIG["password"]:
    raise ValueError(
        "DB_USER and DB_PASSWORD environment variables are required. "
        "Set them in a .env file or export them in your shell."
    )

# Connection pool — reuses connections instead of creating new TCP connections per query
_pool = psycopg2.pool.ThreadedConnectionPool(
    minconn=2,
    maxconn=20,
    options="-c timezone=UTC",
    **DB_CONFIG,
)


@contextmanager
def get_db_connection():
    """Context manager for database connections using connection pool."""
    conn = _pool.getconn()
    try:
        yield conn
    finally:
        _pool.putconn(conn, close=bool(conn.closed))


@contextmanager
def get_db_cursor(dict_cursor=True):
    """Context manager for database cursors."""
    with get_db_connection() as conn:
        cursor_factory = RealDictCursor if dict_cursor else None
        cursor = conn.cursor(cursor_factory=cursor_factory)
        try:
            yield cursor
            conn.commit()
        except Exception:
            if not conn.closed:
                conn.rollback()
            raise
        finally:
            if not cursor.closed:
                cursor.close()


def get_distinct_tickers():
    """Fetch active tickers from the configured universe projection."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT ticker
            FROM selected_tickers
            WHERE is_active = TRUE
            ORDER BY ticker
            """
        )
        return [row["ticker"] for row in cursor.fetchall()]


def get_latest_price_date() -> str:
    """Returns the latest trading date available in the database as YYYY-MM-DD."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT MAX(session_date) AS latest_date
            FROM equity_bar_revisions
            WHERE interval = '1d'
              AND session_scope = 'RTH'
              AND adjusted = FALSE
              AND is_final = TRUE
            """
        )
        row = cursor.fetchone()
        return str(row["latest_date"]) if row and row["latest_date"] else None


def get_latest_quote(ticker: str) -> dict | None:
    """Return the newest canonical final price and change from the prior daily close."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            WITH visible AS (
                SELECT DISTINCT ON (interval, bar_start)
                       close_price, bar_end AS as_of, session_date AS trade_date,
                       interval AS source, source_kind, bar_start
                FROM equity_bar_revisions
                WHERE ticker = %s
                  AND interval IN ('1m', '5m', '15m', '30m', '1h', '1d')
                  AND session_scope = 'RTH'
                  AND adjusted = FALSE
                  AND is_final = TRUE
                  AND COALESCE(replay_available_at, system_observed_at) <= NOW()
                ORDER BY interval, bar_start,
                         CASE
                             WHEN source_kind = 'RECONCILED' THEN 0
                             WHEN interval IN ('1m', '5m', '15m', '30m')
                                  AND source_kind = 'NATIVE_REST' THEN 1
                             WHEN interval IN ('1m', '5m', '15m', '30m')
                                  AND source_kind = 'REALTIME_STREAM' THEN 2
                             WHEN interval IN ('1h', '1d')
                                  AND source_kind = 'DERIVED' THEN 1
                             WHEN source_kind = 'NATIVE_REST' THEN 2
                             WHEN source_kind = 'DERIVED' THEN 3
                             ELSE 4
                         END,
                         COALESCE(replay_available_at, system_observed_at) DESC,
                         created_at DESC
            ), latest AS (
                SELECT close_price, as_of, trade_date, source
                FROM visible
                ORDER BY as_of DESC,
                         CASE source
                             WHEN '1m' THEN 1 WHEN '5m' THEN 2 WHEN '15m' THEN 3
                             WHEN '30m' THEN 4 WHEN '1h' THEN 5 ELSE 6
                         END
                LIMIT 1
            ), previous AS (
                SELECT DISTINCT ON (session_date) session_date, close_price
                FROM equity_bar_revisions
                WHERE ticker = %s
                  AND interval = '1d'
                  AND session_scope = 'RTH'
                  AND adjusted = FALSE
                  AND is_final = TRUE
                  AND session_date < (SELECT trade_date FROM latest)
                  AND COALESCE(replay_available_at, system_observed_at) <= NOW()
                ORDER BY session_date DESC,
                         CASE
                             WHEN source_kind = 'RECONCILED' THEN 0
                             WHEN source_kind = 'DERIVED' THEN 1
                             WHEN source_kind = 'NATIVE_REST' THEN 2
                             ELSE 3
                         END,
                         COALESCE(replay_available_at, system_observed_at) DESC,
                         created_at DESC
                LIMIT 1
            )
            SELECT latest.close_price AS price, latest.as_of, latest.trade_date,
                   latest.source, previous.close_price AS previous_close
            FROM latest
            LEFT JOIN previous ON TRUE
            """,
            (ticker.upper(), ticker.upper()),
        )
        row = cursor.fetchone()

    if not row:
        return None

    price = float(row["price"])
    previous_close = float(row["previous_close"]) if row["previous_close"] is not None else None
    change = price - previous_close if previous_close is not None else None
    return {
        "ticker": ticker.upper(),
        "price": price,
        "previous_close": previous_close,
        "change": change,
        "change_percent": (change / previous_close * 100) if previous_close else None,
        "as_of": row["as_of"],
        "trade_date": row["trade_date"],
        "source": row["source"],
    }


def get_stock_data(ticker: str, days: int = 365):
    """Fetch canonical finalized daily data for a ticker."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            WITH canonical AS (
                SELECT DISTINCT ON (session_date)
                       session_date AS trade_date, open_price AS open,
                       high_price AS high, low_price AS low,
                       close_price AS close, volume
                FROM equity_bar_revisions
                WHERE ticker = %s
                  AND interval = '1d'
                  AND session_scope = 'RTH'
                  AND adjusted = FALSE
                  AND is_final = TRUE
                  AND session_date >= CURRENT_DATE - %s
                  AND COALESCE(replay_available_at, system_observed_at) <= NOW()
                ORDER BY session_date,
                         CASE
                             WHEN source_kind = 'RECONCILED' THEN 0
                             WHEN source_kind = 'DERIVED' THEN 1
                             WHEN source_kind = 'NATIVE_REST' THEN 2
                             ELSE 3
                         END,
                         COALESCE(replay_available_at, system_observed_at) DESC,
                         created_at DESC
            )
            SELECT trade_date AS datetime, open, high, low, close, volume
            FROM canonical
            ORDER BY trade_date ASC
            """,
            (ticker.upper(), days),
        )
        return cursor.fetchall()


def get_bulk_stock_data(tickers: list, days: int = 365, end_date: str = None) -> dict:
    """Fetch OHLCV data for multiple tickers in a single query (one row per day).
    Returns {ticker: [rows...]} dict.
    If end_date is provided (YYYY-MM-DD), data window ends on that date instead of today."""
    if not tickers:
        return {}
    with get_db_cursor() as cursor:
        cursor.execute(
            """
                        WITH canonical AS (
                                SELECT DISTINCT ON (ticker, session_date)
                                             ticker, session_date AS trade_date, open_price AS open,
                                             high_price AS high, low_price AS low,
                                             close_price AS close, volume
                                FROM equity_bar_revisions
                WHERE ticker = ANY(%s)
                                    AND interval = '1d'
                                    AND session_scope = 'RTH'
                                    AND adjusted = FALSE
                                    AND is_final = TRUE
                                    AND session_date <= COALESCE(%s, CURRENT_DATE)::date
                                    AND session_date >= COALESCE(%s, CURRENT_DATE)::date - %s
                                    AND COALESCE(replay_available_at, system_observed_at) <= NOW()
                                ORDER BY ticker, session_date,
                                                 CASE
                                                         WHEN source_kind = 'RECONCILED' THEN 0
                                                         WHEN source_kind = 'DERIVED' THEN 1
                                                         WHEN source_kind = 'NATIVE_REST' THEN 2
                                                         ELSE 3
                                                 END,
                                                 COALESCE(replay_available_at, system_observed_at) DESC,
                                                 created_at DESC
            )
            SELECT ticker, trade_date AS datetime, open, high, low, close, volume
                        FROM canonical
            ORDER BY ticker, trade_date ASC
            """,
                        ([ticker.upper() for ticker in tickers], end_date, end_date, days),
        )
        rows = cursor.fetchall()

    result: dict = {}
    for row in rows:
        t = row["ticker"]
        if t not in result:
            result[t] = []
        result[t].append(row)
    return result


def require_canonical_schema() -> None:
    """Fail closed when the canonical baseline has not been installed."""
    required = (
        "schema_migrations",
        "selected_tickers",
        "equity_bar_revisions",
        "equity_current_bar_projection",
        "equity_evidence",
        "option_contract_catalog",
    )
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT relation_name
            FROM unnest(%s::text[]) AS relation_name
            WHERE to_regclass('public.' || relation_name) IS NULL
            ORDER BY relation_name
            """,
            (list(required),),
        )
        missing = [row["relation_name"] for row in cursor.fetchall()]
    if missing:
        raise RuntimeError(
            "canonical database schema is not installed; missing: "
            + ", ".join(missing)
        )
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT EXISTS (
                       SELECT 1 FROM schema_migrations WHERE version = %s
                   ) AS installed,
                   EXISTS (
                       SELECT 1
                       FROM pg_proc
                       WHERE oid = to_regprocedure(
                           'public.ensure_option_market_data_partitions(date)'
                       )
                         AND prosecdef = TRUE
                         AND proconfig @> ARRAY['search_path=pg_catalog, public']
                   ) AS partition_maintenance_ready
            """,
            ("000_canonical_schema",),
        )
        schema = cursor.fetchone()
        installed = bool(schema["installed"])
    if not installed:
        raise RuntimeError(
            "canonical database schema version is not recorded: 000_canonical_schema"
        )
    if not schema["partition_maintenance_ready"]:
        raise RuntimeError(
            "canonical option partition maintenance is not installed securely"
        )


def get_selected_tickers(active_only: bool = True):
    """Returns selected tickers from selected_tickers table."""
    with get_db_cursor() as cursor:
        if active_only:
            cursor.execute(
                """
                SELECT ticker
                FROM selected_tickers
                WHERE is_active = TRUE
                ORDER BY ticker
                """
            )
        else:
            cursor.execute(
                """
                SELECT ticker
                FROM selected_tickers
                ORDER BY ticker
                """
            )
        return [row["ticker"] for row in cursor.fetchall()]


def _to_float(val):
    """Convert Decimal or other numeric types to float for JSON serialization."""
    if val is None:
        return None
    if isinstance(val, Decimal):
        return float(val)
    return val


def get_tickers_overview(tickers: list[str], end_date: str = None) -> list[dict]:
    """
    Compute an overview row per ticker from DB daily data:
      - Latest OHLCV + prev close
      - Daily SMAs: 20, 50, 200
      - True weekly SMAs: 50w, 200w
      - 20-day avg volume (for relative volume)
      - 52-week high
    All computed in SQL for efficiency.
    """
    if not tickers:
        return []

    query = """
    WITH daily AS (
        SELECT DISTINCT ON (ticker, session_date)
            ticker, session_date AS trade_date, open_price,
            high_price AS high, low_price AS low, close_price, volume
        FROM equity_bar_revisions
        WHERE ticker = ANY(%s)
          AND interval = '1d'
          AND session_scope = 'RTH'
          AND adjusted = FALSE
          AND is_final = TRUE
          AND session_date <= COALESCE(%s, CURRENT_DATE)::date
          AND session_date >= COALESCE(%s, CURRENT_DATE)::date - 1600
          AND COALESCE(replay_available_at, system_observed_at) <= NOW()
        ORDER BY ticker, session_date,
                 CASE
                     WHEN source_kind = 'RECONCILED' THEN 0
                     WHEN source_kind = 'DERIVED' THEN 1
                     WHEN source_kind = 'NATIVE_REST' THEN 2
                     ELSE 3
                 END,
                 COALESCE(replay_available_at, system_observed_at) DESC,
                 created_at DESC
    ),
    ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) AS day_rank
        FROM daily
    ),
    latest AS (
        SELECT ticker, trade_date, open_price, high, low, close_price, volume
        FROM ranked WHERE day_rank = 1
    ),
    prev AS (
        SELECT ticker, close_price AS prev_close
        FROM ranked WHERE day_rank = 2
    ),
    daily_mas AS (
        SELECT
            ticker,
            AVG(CASE WHEN day_rank <= 20 THEN close_price END) AS sma_20,
            AVG(CASE WHEN day_rank <= 50 THEN close_price END) AS sma_50,
            AVG(CASE WHEN day_rank <= 200 THEN close_price END) AS sma_200,
            AVG(CASE WHEN day_rank <= 20 THEN volume END) AS avg_vol_20
        FROM ranked
        GROUP BY ticker
    ),
    high_52w AS (
        SELECT ticker, MAX(high) AS high_52w
        FROM ranked WHERE day_rank <= 252
        GROUP BY ticker
    ),
    low_52w AS (
        SELECT ticker, MIN(low) AS low_52w
        FROM ranked WHERE day_rank <= 252
        GROUP BY ticker
    ),
    canonical_weekly AS (
        SELECT DISTINCT ON (ticker, bar_start)
            ticker, bar_start, close_price
        FROM equity_bar_revisions
        WHERE ticker = ANY(%s)
          AND interval = '1wk'
          AND session_scope = 'RTH'
          AND adjusted = FALSE
          AND is_final = TRUE
          AND session_date <= COALESCE(%s, CURRENT_DATE)::date
          AND COALESCE(replay_available_at, system_observed_at) <= NOW()
        ORDER BY ticker, bar_start,
                 CASE
                     WHEN source_kind = 'RECONCILED' THEN 0
                     WHEN source_kind = 'DERIVED' THEN 1
                     WHEN source_kind = 'NATIVE_REST' THEN 2
                     ELSE 3
                 END,
                 COALESCE(replay_available_at, system_observed_at) DESC,
                 created_at DESC
    ),
    weekly_close AS (
        SELECT ticker, close_price,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY bar_start DESC) AS week_rank
        FROM canonical_weekly
    ),
    weekly_mas AS (
        SELECT
            ticker,
            AVG(CASE WHEN week_rank <= 50 THEN close_price END) AS wsma_50,
            AVG(CASE WHEN week_rank <= 200 THEN close_price END) AS wsma_200
        FROM weekly_close
        GROUP BY ticker
    )
    SELECT
        l.ticker,
        st.sector,
        l.trade_date,
        l.open_price,
        l.high,
        l.low,
        l.close_price,
        l.volume,
        p.prev_close,
        d.sma_20, d.sma_50, d.sma_200,
        d.avg_vol_20,
        h.high_52w,
        lo.low_52w,
        w.wsma_50, w.wsma_200
    FROM latest l
    LEFT JOIN selected_tickers st ON st.ticker = l.ticker
    LEFT JOIN prev p ON l.ticker = p.ticker
    LEFT JOIN daily_mas d ON l.ticker = d.ticker
    LEFT JOIN high_52w h ON l.ticker = h.ticker
    LEFT JOIN low_52w lo ON l.ticker = lo.ticker
    LEFT JOIN weekly_mas w ON l.ticker = w.ticker
    ORDER BY l.ticker
    """

    with get_db_cursor() as cursor:
        cursor.execute("SET LOCAL work_mem = '64MB'")
        normalized_tickers = [ticker.upper() for ticker in tickers]
        cursor.execute(
            query,
            (normalized_tickers, end_date, end_date, normalized_tickers, end_date),
        )
        rows = cursor.fetchall()

    results = []
    for row in rows:
        close_val = _to_float(row["close_price"])
        prev_close_val = _to_float(row["prev_close"])
        sma_200_val = _to_float(row["sma_200"])
        avg_vol_20_val = _to_float(row["avg_vol_20"])
        high_52w_val = _to_float(row["high_52w"])
        low_52w_val = _to_float(row["low_52w"])
        volume_val = int(row["volume"]) if row["volume"] else 0

        # Daily % change
        chg_pct = round((close_val - prev_close_val) / prev_close_val * 100, 2) if close_val and prev_close_val else None
        # % distance from 200 SMA
        dist_200 = round((close_val - sma_200_val) / sma_200_val * 100, 2) if close_val and sma_200_val else None
        # % distance from 200W MA
        wsma_200_val = _to_float(row["wsma_200"])
        dist_200w = round((close_val - wsma_200_val) / wsma_200_val * 100, 2) if close_val and wsma_200_val else None
        # Relative volume
        rel_vol = round(volume_val / avg_vol_20_val, 2) if avg_vol_20_val and avg_vol_20_val > 0 else None
        # % from 52-week high
        pct_from_high = round((close_val - high_52w_val) / high_52w_val * 100, 2) if close_val and high_52w_val else None
        # % from 52-week low
        pct_from_low = round((close_val - low_52w_val) / low_52w_val * 100, 2) if close_val and low_52w_val else None

        results.append({
            "ticker": row["ticker"],
            "sector": row["sector"],
            "date": row["trade_date"].isoformat() if row["trade_date"] else None,
            "open": _to_float(row["open_price"]),
            "high": _to_float(row["high"]),
            "low": _to_float(row["low"]),
            "close": close_val,
            "volume": volume_val,
            "chg_pct": chg_pct,
            "rel_vol": rel_vol,
            "high_52w": round(high_52w_val, 2) if high_52w_val else None,
            "pct_from_high": pct_from_high,
            "low_52w": round(low_52w_val, 2) if low_52w_val else None,
            "pct_from_low": pct_from_low,
            "sma_20": round(_to_float(row["sma_20"]), 2) if row["sma_20"] else None,
            "sma_50": round(_to_float(row["sma_50"]), 2) if row["sma_50"] else None,
            "sma_200": round(sma_200_val, 2) if sma_200_val else None,
            "dist_200": dist_200,
            "wsma_50": round(_to_float(row["wsma_50"]), 2) if row["wsma_50"] else None,
            "wsma_200": round(wsma_200_val, 2) if wsma_200_val else None,
            "dist_200w": dist_200w,
        })
    return results


# =============================================================================
# HOURLY DATA FUNCTIONS (true hourly candles)
# =============================================================================

_CANONICAL_INTERVAL_ALIASES = {
    "1min": "1m", "5min": "5m", "15min": "15m", "30min": "30m",
    "1hour": "1h", "1day": "1d",
}


def _canonical_interval_rows(
    tickers: list[str],
    interval: str,
    periods: int,
    end_datetime=None,
) -> dict[str, list]:
    if not tickers:
        return {}
    canonical_interval = _CANONICAL_INTERVAL_ALIASES.get(interval, interval)
    if canonical_interval not in {"1m", "5m", "15m", "30m", "1h", "1wk", "1mo"}:
        raise ValueError(f"unsupported canonical interval: {interval}")
    if periods <= 0:
        raise ValueError("periods must be positive")
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            WITH canonical AS (
                SELECT DISTINCT ON (ticker, bar_start)
                       ticker, bar_start AS datetime, open_price AS open,
                       high_price AS high, low_price AS low,
                       close_price AS close, volume
                FROM equity_bar_revisions
                WHERE ticker = ANY(%s)
                  AND interval = %s
                  AND session_scope = 'RTH'
                  AND adjusted = FALSE
                  AND is_final = TRUE
                  AND bar_end <= COALESCE(%s, NOW())
                  AND COALESCE(replay_available_at, system_observed_at) <= NOW()
                ORDER BY ticker, bar_start,
                         CASE
                             WHEN source_kind = 'RECONCILED' THEN 0
                             WHEN interval IN ('1m', '5m', '15m', '30m')
                                  AND source_kind = 'NATIVE_REST' THEN 1
                             WHEN interval IN ('1m', '5m', '15m', '30m')
                                  AND source_kind = 'REALTIME_STREAM' THEN 2
                             WHEN interval IN ('1h', '1wk', '1mo')
                                  AND source_kind = 'DERIVED' THEN 1
                             WHEN source_kind = 'NATIVE_REST' THEN 2
                             WHEN source_kind = 'DERIVED' THEN 3
                             ELSE 4
                         END,
                         COALESCE(replay_available_at, system_observed_at) DESC,
                         created_at DESC
            ), ranked AS (
                SELECT canonical.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY ticker ORDER BY datetime DESC
                       ) AS recency_rank
                FROM canonical
            )
            SELECT ticker, datetime, open, high, low, close, volume
            FROM ranked
            WHERE recency_rank <= %s
            ORDER BY ticker, datetime
            """,
            (
                [ticker.upper() for ticker in tickers], canonical_interval,
                end_datetime, periods,
            ),
        )
        rows = cursor.fetchall()
    result = {}
    for row in rows:
        result.setdefault(row["ticker"], []).append(row)
    return result

def get_hourly_data(ticker: str, periods: int = 100, end_datetime=None) -> list:
    """
    Fetches hourly price data for a ticker.
    
    Args:
        ticker: Stock symbol
        periods: Number of hourly candles to fetch
        end_datetime: End datetime (default: now)
    """
    return _canonical_interval_rows(
        [ticker], "1h", periods, end_datetime
    ).get(ticker.upper(), [])


def get_bulk_hourly_data(tickers: list, periods: int = 100, end_datetime=None) -> dict:
    """
    Fetch hourly data for multiple tickers in a single query.
    Returns: {ticker: [rows...]} dict
    """
    return _canonical_interval_rows(tickers, "1h", periods, end_datetime)


# =============================================================================
# INTRADAY DATA FUNCTIONS
# =============================================================================


def get_intraday_data(
    ticker: str,
    interval: str = "5min",
    periods: int = 100,
    end_datetime=None
) -> list:
    """
    Fetches intraday price data for a ticker.
    
    Args:
        ticker: Stock symbol
        interval: '1min', '5min', '15min', '1hour'
        periods: Number of candles to fetch
        end_datetime: End datetime (default: now)
    
    Returns:
        List of dicts with datetime, open, high, low, close, volume
    """
    return _canonical_interval_rows(
        [ticker], interval, periods, end_datetime
    ).get(ticker.upper(), [])


def get_bulk_intraday_data(
    tickers: list,
    interval: str = "5min",
    periods: int = 100,
    end_datetime=None
) -> dict:
    """
    Fetch intraday data for multiple tickers in a single query.
    
    Returns:
        {ticker: [rows...]} dict
    """
    return _canonical_interval_rows(tickers, interval, periods, end_datetime)


# =============================================================================
# UNIFIED CANONICAL QUERY LAYER
# =============================================================================

def get_price_data(
    ticker: str,
    interval: str = "1day",
    periods: int = 100,
    end_datetime=None
) -> list:
    """
    Unified price query - routes to appropriate table based on interval.
    
    Args:
        ticker: Stock symbol
        interval: '1min', '5min', '15min', '1hour', '1day'
        periods: Number of candles to fetch
        end_datetime: End datetime (default: now)
    
    Returns:
        List of dicts with datetime, open, high, low, close, volume
    """
    if interval == "1day":
        return get_stock_data(ticker, days=periods)
    elif interval == "1hour":
        return get_hourly_data(ticker, periods, end_datetime)
    else:
        # 1min, 5min, 15min
        return get_intraday_data(ticker, interval, periods, end_datetime)


def get_bulk_price_data(
    tickers: list,
    interval: str = "1d",
    periods: int = 100,
    end_datetime=None
) -> dict:
    """
    Unified bulk price query - routes to appropriate table based on interval.
    
    Args:
        tickers: List of stock symbols
        interval: '5m', '15m', '30m', '1h', '1d'
        periods: Number of candles per ticker
        end_datetime: End datetime (default: now)
    
    Returns:
        {ticker: [rows...]} dict
    """
    if interval in ("1d", "1day"):
        return get_bulk_stock_data(tickers, days=periods, end_date=end_datetime)
    elif interval in ("1h", "1hour"):
        return get_bulk_hourly_data(tickers, periods, end_datetime)
    elif interval in ("15m", "30m"):
        return _aggregate_from_5m(tickers, interval, periods, end_datetime)
    else:
        # 5m (or 1m) — query intraday table directly
        return get_bulk_intraday_data(tickers, interval, periods, end_datetime)


def _aggregate_from_5m(
    tickers: list,
    target_interval: str,
    periods: int = 100,
    end_datetime=None
) -> dict:
    """Compatibility wrapper returning exact canonical target-interval bars."""
    if target_interval not in ("15m", "30m"):
        raise ValueError("target_interval must be 15m or 30m")
    return _canonical_interval_rows(
        tickers, target_interval, periods, end_datetime
    )
