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
    """Fetches distinct tickers from the database."""
    with get_db_cursor() as cursor:
        cursor.execute("SELECT DISTINCT ticker FROM stock_prices_daily ORDER BY ticker")
        return [row["ticker"] for row in cursor.fetchall()]


def get_latest_price_date() -> str:
    """Returns the latest trading date available in the database as YYYY-MM-DD."""
    with get_db_cursor() as cursor:
        cursor.execute("SELECT MAX(datetime::date) AS latest_date FROM stock_prices_daily")
        row = cursor.fetchone()
        return str(row["latest_date"]) if row and row["latest_date"] else None


def get_latest_quote(ticker: str) -> dict | None:
    """Return the newest stored price and its change from the prior daily close."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            WITH daily AS (
                SELECT close_price, datetime AS as_of, datetime::date AS trade_date,
                       'daily' AS source, 1 AS priority
                FROM stock_prices_daily
                WHERE ticker = %s
                ORDER BY datetime DESC
                LIMIT 1
            ), hourly AS (
                SELECT close_price, datetime AS as_of,
                       (datetime AT TIME ZONE 'America/New_York')::date AS trade_date,
                       '1h' AS source, 2 AS priority
                FROM stock_prices_hourly
                WHERE ticker = %s
                ORDER BY datetime DESC
                LIMIT 1
            ), intraday AS (
                SELECT close_price, datetime AS as_of,
                       (datetime AT TIME ZONE 'America/New_York')::date AS trade_date,
                       '5m' AS source, 3 AS priority
                FROM stock_prices_intraday
                WHERE ticker = %s AND interval = '5m'
                ORDER BY datetime DESC
                LIMIT 1
            ), latest AS (
                SELECT * FROM (
                    SELECT * FROM daily
                    UNION ALL SELECT * FROM hourly
                    UNION ALL SELECT * FROM intraday
                ) prices
                ORDER BY trade_date DESC, as_of DESC, priority DESC
                LIMIT 1
            )
            SELECT latest.close_price AS price, latest.as_of, latest.trade_date,
                   latest.source, previous.close_price AS previous_close
            FROM latest
            LEFT JOIN LATERAL (
                SELECT close_price
                FROM stock_prices_daily
                WHERE ticker = %s AND datetime::date < latest.trade_date
                ORDER BY datetime DESC
                LIMIT 1
            ) previous ON TRUE
            """,
            (ticker, ticker, ticker, ticker),
        )
        row = cursor.fetchone()

    if not row:
        return None

    price = float(row["price"])
    previous_close = float(row["previous_close"]) if row["previous_close"] is not None else None
    change = price - previous_close if previous_close is not None else None
    return {
        "ticker": ticker,
        "price": price,
        "previous_close": previous_close,
        "change": change,
        "change_percent": (change / previous_close * 100) if previous_close else None,
        "as_of": row["as_of"],
        "trade_date": row["trade_date"],
        "source": row["source"],
    }


def get_stock_data(ticker: str, days: int = 365):
    """Fetches stock price data for a ticker (one row per day)."""
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            WITH ranked AS (
                SELECT datetime::date AS trade_date,
                       open_price AS open, high, low, close_price AS close, volume,
                       ROW_NUMBER() OVER (
                           PARTITION BY datetime::date
                           ORDER BY volume DESC
                       ) AS rn
                FROM stock_prices_daily
                WHERE ticker = %s AND datetime >= NOW() - INTERVAL '%s days'
            )
            SELECT trade_date AS datetime, open, high, low, close, volume
            FROM ranked WHERE rn = 1
            ORDER BY trade_date ASC
            """,
            (ticker, days),
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
            WITH ranked AS (
                SELECT ticker, datetime::date AS trade_date,
                       open_price AS open, high, low, close_price AS close, volume,
                       ROW_NUMBER() OVER (
                           PARTITION BY ticker, datetime::date
                           ORDER BY volume DESC
                       ) AS rn
                FROM stock_prices_daily
                WHERE ticker = ANY(%s)
                  AND datetime::date <= COALESCE(%s, CURRENT_DATE)::date
                  AND datetime >= COALESCE(%s, CURRENT_DATE)::date - INTERVAL '%s days'
            )
            SELECT ticker, trade_date AS datetime, open, high, low, close, volume
            FROM ranked WHERE rn = 1
            ORDER BY ticker, trade_date ASC
            """,
            (tickers, end_date, end_date, days),
        )
        rows = cursor.fetchall()

    result: dict = {}
    for row in rows:
        t = row["ticker"]
        if t not in result:
            result[t] = []
        result[t].append(row)
    return result


def get_latest_scan_results():
    """Fetches the latest gap scan results from a results table if exists."""
    try:
        with get_db_cursor() as cursor:
            cursor.execute(
                """
                SELECT * FROM gap_scan_results 
                WHERE scan_date = (SELECT MAX(scan_date) FROM gap_scan_results)
                ORDER BY gap_type, ticker
                """
            )
            return cursor.fetchall()
    except Exception:
        return []


def create_scan_results_table():
    """Creates the gap_scan_results table if it doesn't exist."""
    with get_db_cursor(dict_cursor=False) as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS gap_scan_results (
                id SERIAL PRIMARY KEY,
                scan_date TIMESTAMP DEFAULT NOW(),
                ticker TEXT NOT NULL,
                gap_type TEXT NOT NULL,
                gap_low REAL,
                gap_high REAL,
                last_close REAL,
                gap_diff REAL,
                gap_date DATE,
                UNIQUE (scan_date, ticker, gap_type)
            );
            CREATE INDEX IF NOT EXISTS idx_scan_date ON gap_scan_results (scan_date DESC);
            CREATE INDEX IF NOT EXISTS idx_gap_type ON gap_scan_results (gap_type);
            """
        )


def create_selected_tickers_table():
    """Creates selected_tickers table used to control scanner scope."""
    with get_db_cursor(dict_cursor=False) as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS selected_tickers (
                id SERIAL PRIMARY KEY,
                ticker TEXT NOT NULL UNIQUE,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_selected_tickers_active
                ON selected_tickers (is_active, ticker);
            """
        )


def migrate_selected_tickers_metadata():
    """Add metadata columns to selected_tickers if they don't exist."""
    columns = {
        "asset_type":        "TEXT",                    # 'Stock' or 'ETF'
        "sector":            "TEXT",                    # GICS sector
        "industry":          "TEXT",                    # GICS industry
        "market_cap":        "BIGINT",                  # market cap in USD
        "market_cap_group":  "TEXT",                    # Mega/Large/Mid/Small/Micro
        "beta":              "DOUBLE PRECISION",        # beta vs market
        "avg_volume_90d":    "BIGINT",                  # 90-day avg daily volume
        "float_shares":      "BIGINT",                  # float shares outstanding
        "short_percent":     "DOUBLE PRECISION",        # short interest %
        "institutional_pct": "DOUBLE PRECISION",        # institutional ownership %
        "dividend_yield":    "DOUBLE PRECISION",        # annual dividend yield
        "pe_ratio":          "DOUBLE PRECISION",        # trailing P/E ratio
        "exchange":          "TEXT",                    # NYSE, NASDAQ, etc.
        "metadata_updated":  "TIMESTAMP",              # when metadata was last refreshed
    }
    with get_db_cursor(dict_cursor=False) as cursor:
        for col_name, col_type in columns.items():
            cursor.execute(
                f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'selected_tickers' AND column_name = '{col_name}'
                    ) THEN
                        ALTER TABLE selected_tickers ADD COLUMN {col_name} {col_type};
                    END IF;
                END $$;
                """
            )
        # Index on sector and market_cap_group for fast filtering
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_selected_tickers_sector ON selected_tickers (sector);"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_selected_tickers_cap_group ON selected_tickers (market_cap_group);"
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


def save_scan_results(results: list, scan_datetime):
    """Saves scan results to the database."""
    with get_db_cursor(dict_cursor=False) as cursor:
        for result in results:
            cursor.execute(
                """
                INSERT INTO gap_scan_results (scan_date, ticker, gap_type, gap_low, gap_high, last_close, gap_diff, gap_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (scan_date, ticker, gap_type) DO UPDATE SET
                    gap_low = EXCLUDED.gap_low,
                    gap_high = EXCLUDED.gap_high,
                    last_close = EXCLUDED.last_close,
                    gap_diff = EXCLUDED.gap_diff,
                    gap_date = EXCLUDED.gap_date
                """,
                (
                    scan_datetime,
                    result["ticker"],
                    result["gap_type"],
                    result["gap_low"],
                    result["gap_high"],
                    result["last_close"],
                    result["gap_diff"],
                    result["gap_date"],
                ),
            )


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
        SELECT
            ticker,
            datetime::date AS trade_date,
            open_price,
            high,
            low,
            close_price,
            volume,
            ROW_NUMBER() OVER (PARTITION BY ticker, datetime::date ORDER BY datetime DESC) AS rn
        FROM stock_prices_daily
        WHERE ticker = ANY(%s)
          AND datetime::date <= COALESCE(%s, CURRENT_DATE)::date
          AND datetime >= COALESCE(%s, CURRENT_DATE)::date - INTERVAL '1600 days'
    ),
    deduped AS (
        SELECT * FROM daily WHERE rn = 1
    ),
    ranked AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY trade_date DESC) AS day_rank
        FROM deduped
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
    -- For weekly SMAs we need weekly closes (last trading day of each ISO week)
    weekly AS (
        SELECT
            ticker,
            EXTRACT(ISOYEAR FROM trade_date) AS iso_year,
            EXTRACT(WEEK FROM trade_date) AS iso_week,
            close_price,
            ROW_NUMBER() OVER (
                PARTITION BY ticker, EXTRACT(ISOYEAR FROM trade_date), EXTRACT(WEEK FROM trade_date)
                ORDER BY trade_date DESC
            ) AS wrn
        FROM deduped
    ),
    weekly_close AS (
        SELECT ticker, iso_year, iso_week, close_price,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY iso_year DESC, iso_week DESC) AS week_rank
        FROM weekly WHERE wrn = 1
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
    LEFT JOIN prev p ON l.ticker = p.ticker
    LEFT JOIN daily_mas d ON l.ticker = d.ticker
    LEFT JOIN high_52w h ON l.ticker = h.ticker
    LEFT JOIN low_52w lo ON l.ticker = lo.ticker
    LEFT JOIN weekly_mas w ON l.ticker = w.ticker
    ORDER BY l.ticker
    """

    with get_db_cursor() as cursor:
        cursor.execute(query, (tickers, end_date, end_date))
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

def create_hourly_table():
    """Creates the stock_prices_hourly table for TRUE hourly candles."""
    with get_db_cursor(dict_cursor=False) as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_prices_hourly (
                id BIGSERIAL PRIMARY KEY,
                ticker VARCHAR(10) NOT NULL,
                datetime TIMESTAMPTZ NOT NULL,
                open_price DECIMAL(12, 4) NOT NULL,
                high DECIMAL(12, 4) NOT NULL,
                low DECIMAL(12, 4) NOT NULL,
                close_price DECIMAL(12, 4) NOT NULL,
                volume BIGINT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                
                CONSTRAINT uq_hourly_ticker_datetime UNIQUE (ticker, datetime)
            );
            
            CREATE INDEX IF NOT EXISTS idx_hourly_ticker_datetime 
                ON stock_prices_hourly (ticker, datetime DESC);
            CREATE INDEX IF NOT EXISTS idx_hourly_datetime 
                ON stock_prices_hourly (datetime DESC);
            """
        )


def get_hourly_data(ticker: str, periods: int = 100, end_datetime=None) -> list:
    """
    Fetches hourly price data for a ticker.
    
    Args:
        ticker: Stock symbol
        periods: Number of hourly candles to fetch
        end_datetime: End datetime (default: now)
    """
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT datetime, open_price as open, high, low, 
                   close_price as close, volume
            FROM stock_prices_hourly
            WHERE ticker = %s
              AND datetime <= COALESCE(%s, NOW())
            ORDER BY datetime DESC
            LIMIT %s
            """,
            (ticker, end_datetime, periods),
        )
        rows = cursor.fetchall()
    return list(reversed(rows))


def get_bulk_hourly_data(tickers: list, periods: int = 100, end_datetime=None) -> dict:
    """
    Fetch hourly data for multiple tickers in a single query.
    Returns: {ticker: [rows...]} dict
    """
    if not tickers:
        return {}
    
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            WITH ranked AS (
                SELECT ticker, datetime, open_price as open, high, low,
                       close_price as close, volume,
                       ROW_NUMBER() OVER (
                           PARTITION BY ticker 
                           ORDER BY datetime DESC
                       ) AS rn
                FROM stock_prices_hourly
                WHERE ticker = ANY(%s)
                  AND datetime <= COALESCE(%s, NOW())
            )
            SELECT ticker, datetime, open, high, low, close, volume
            FROM ranked
            WHERE rn <= %s
            ORDER BY ticker, datetime ASC
            """,
            (tickers, end_datetime, periods),
        )
        rows = cursor.fetchall()
    
    result = {}
    for row in rows:
        t = row["ticker"]
        if t not in result:
            result[t] = []
        result[t].append(row)
    return result


def cleanup_old_hourly_data(retention_days: int = 90) -> int:
    """Delete hourly data older than retention_days."""
    with get_db_cursor(dict_cursor=False) as cursor:
        cursor.execute(
            """
            DELETE FROM stock_prices_hourly 
            WHERE datetime < NOW() - INTERVAL '%s days'
            """,
            (retention_days,)
        )
        return cursor.rowcount if hasattr(cursor, 'rowcount') else 0


# =============================================================================
# INTRADAY DATA FUNCTIONS (for future real-time scaling)
# =============================================================================

def create_intraday_table():
    """Creates the stock_prices_intraday table if it doesn't exist."""
    with get_db_cursor(dict_cursor=False) as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_prices_intraday (
                id BIGSERIAL PRIMARY KEY,
                ticker VARCHAR(10) NOT NULL,
                datetime TIMESTAMPTZ NOT NULL,
                interval VARCHAR(5) NOT NULL DEFAULT '5min',
                open_price DECIMAL(12, 4) NOT NULL,
                high DECIMAL(12, 4) NOT NULL,
                low DECIMAL(12, 4) NOT NULL,
                close_price DECIMAL(12, 4) NOT NULL,
                volume BIGINT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                
                CONSTRAINT uq_intraday_ticker_datetime_interval 
                    UNIQUE (ticker, datetime, interval)
            );
            
            CREATE INDEX IF NOT EXISTS idx_intraday_ticker_datetime 
                ON stock_prices_intraday (ticker, datetime DESC);
            CREATE INDEX IF NOT EXISTS idx_intraday_datetime 
                ON stock_prices_intraday (datetime DESC);
            CREATE INDEX IF NOT EXISTS idx_intraday_interval_datetime 
                ON stock_prices_intraday (interval, datetime DESC);
            """
        )


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
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT datetime, open_price as open, high, low, 
                   close_price as close, volume
            FROM stock_prices_intraday
            WHERE ticker = %s
              AND interval = %s
              AND datetime <= COALESCE(%s, NOW())
            ORDER BY datetime DESC
            LIMIT %s
            """,
            (ticker, interval, end_datetime, periods),
        )
        rows = cursor.fetchall()
    
    # Reverse to chronological order
    return list(reversed(rows))


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
    if not tickers:
        return {}
    
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            WITH ranked AS (
                SELECT ticker, datetime, open_price as open, high, low,
                       close_price as close, volume,
                       ROW_NUMBER() OVER (
                           PARTITION BY ticker 
                           ORDER BY datetime DESC
                       ) AS rn
                FROM stock_prices_intraday
                WHERE ticker = ANY(%s)
                  AND interval = %s
                  AND datetime <= COALESCE(%s, NOW())
            )
            SELECT ticker, datetime, open, high, low, close, volume
            FROM ranked
            WHERE rn <= %s
            ORDER BY ticker, datetime ASC
            """,
            (tickers, interval, end_datetime, periods),
        )
        rows = cursor.fetchall()
    
    result = {}
    for row in rows:
        t = row["ticker"]
        if t not in result:
            result[t] = []
        result[t].append(row)
    return result


def upsert_intraday_price(
    ticker: str,
    dt,
    interval: str,
    open_price: float,
    high: float,
    low: float,
    close_price: float,
    volume: int
) -> bool:
    """
    Upsert a single intraday candle.
    Normalizes timestamp to US/Eastern before inserting.
    """
    try:
        from datetime import datetime as _dt
        # Validate interval
        if interval not in VALID_INTRADAY_INTERVALS:
            return False
        # Validate OHLCV
        if not is_valid_ohlcv(open_price, high, low, close_price, volume):
            return False
        # Normalize to ET
        if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
            dt = dt.astimezone(_ET)
        elif isinstance(dt, _dt):
            dt = dt.replace(tzinfo=_ET)

        with get_db_cursor(dict_cursor=False) as cursor:
            cursor.execute(
                """
                INSERT INTO stock_prices_intraday
                    (ticker, datetime, interval, open_price, high, low, close_price, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, datetime, interval) DO UPDATE SET
                    open_price  = EXCLUDED.open_price,
                    high        = EXCLUDED.high,
                    low         = EXCLUDED.low,
                    close_price = EXCLUDED.close_price,
                    volume      = EXCLUDED.volume
                """,
                (ticker, dt, interval, open_price, high, low, close_price, volume),
            )
        return True
    except Exception:
        return False


def cleanup_old_intraday_data(retention_days: int = 7) -> int:
    """
    Delete intraday data older than retention_days.
    
    Returns:
        Number of rows deleted
    """
    with get_db_cursor(dict_cursor=False) as cursor:
        cursor.execute(
            """
            DELETE FROM stock_prices_intraday 
            WHERE datetime < NOW() - INTERVAL '%s days'
            """,
            (retention_days,)
        )
        # Note: For psycopg2, rowcount gives us the deleted count
        return cursor.rowcount if hasattr(cursor, 'rowcount') else 0


def get_intraday_stats() -> list:
    """
    Get statistics about intraday data by interval.
    
    Returns:
        List of dicts with interval, ticker_count, total_rows, oldest/newest data
    """
    with get_db_cursor() as cursor:
        cursor.execute(
            """
            SELECT 
                interval,
                COUNT(DISTINCT ticker) AS ticker_count,
                COUNT(*) AS total_rows,
                MIN(datetime) AS oldest_data,
                MAX(datetime) AS newest_data
            FROM stock_prices_intraday
            GROUP BY interval
            ORDER BY interval
            """
        )
        return cursor.fetchall()


# =============================================================================
# UNIFIED QUERY LAYER (routes to appropriate table based on interval)
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
        # Query daily table (stock_prices_daily)
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
        # Aggregate from 5m base data
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
    """
    Aggregate 5m candles into 15m or 30m candles using SQL.
    Groups 5m bars by floored timestamps (e.g. 3 bars → 15m, 6 bars → 30m).
    """
    if not tickers:
        return {}

    # minutes per target bar
    minutes = 15 if target_interval == "15m" else 30
    # fetch more 5m bars to produce enough aggregated bars
    raw_limit = periods * (minutes // 5) + 10

    with get_db_cursor() as cursor:
        cursor.execute(
            """
            WITH base AS (
                SELECT ticker, datetime, open_price, high, low, close_price, volume,
                       ROW_NUMBER() OVER (
                           PARTITION BY ticker ORDER BY datetime DESC
                       ) AS rn
                FROM stock_prices_intraday
                WHERE ticker = ANY(%(tickers)s)
                  AND interval = '5m'
                  AND datetime <= COALESCE(%(end_dt)s, NOW())
            ),
            filtered AS (
                SELECT * FROM base WHERE rn <= %(raw_limit)s
            ),
            bucketed AS (
                SELECT ticker,
                       date_trunc('hour', datetime)
                         + (FLOOR(EXTRACT(MINUTE FROM datetime) / %(minutes)s) * %(minutes)s)
                         * INTERVAL '1 minute' AS bucket,
                       open_price, high, low, close_price, volume, datetime
                FROM filtered
            ),
            aggregated AS (
                SELECT ticker, bucket AS datetime,
                       (ARRAY_AGG(open_price ORDER BY datetime ASC))[1] AS open,
                       MAX(high) AS high,
                       MIN(low) AS low,
                       (ARRAY_AGG(close_price ORDER BY datetime DESC))[1] AS close,
                       SUM(volume) AS volume,
                       ROW_NUMBER() OVER (
                           PARTITION BY ticker ORDER BY bucket DESC
                       ) AS rn
                FROM bucketed
                GROUP BY ticker, bucket
            )
            SELECT ticker, datetime, open, high, low, close, volume
            FROM aggregated
            WHERE rn <= %(periods)s
            ORDER BY ticker, datetime ASC
            """,
            {
                "tickers": tickers,
                "end_dt": end_datetime,
                "raw_limit": raw_limit,
                "minutes": minutes,
                "periods": periods,
            },
        )
        rows = cursor.fetchall()

    result = {}
    for row in rows:
        t = row["ticker"]
        if t not in result:
            result[t] = []
        result[t].append(row)
    return result
