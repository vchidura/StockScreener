# Database Scaling Strategy for Real-Time Intraday Data

## Overview

This document outlines the recommended approach to scale the stock screener database 
for real-time intraday signals while maintaining the existing daily data infrastructure.

---

## Current State (Clarification)

**Important**: Despite the table name, `stock_prices_hourly` stores **daily** candles (1 row per day).
The scripts use `interval: "1day"` from data providers.

| Table | Actual Interval | Retention | ~Rows/Ticker |
|-------|-----------------|-----------|--------------|
| `stock_prices_hourly` | **Daily** (1 per day) | 4+ years | ~1,000 |

## Target State: 3-Tier Architecture

| Table | Interval | Retention | Use Case |
|-------|----------|-----------|----------|
| `stock_prices_daily` | 1 day | Forever | Swing trades, weekly SMAs, 200-day MA |
| `stock_prices_hourly` | 1 hour | 90 days | Intraday trend, hourly EMAs, momentum |
| `stock_prices_intraday` | 1-5 min | 7-30 days | Scalping, real-time entry/exit signals |

---

## Why Hourly Data Matters

Hourly candles are valuable for:
- **Intraday trend analysis**: 8-hour, 21-hour EMA stacking
- **Gap analysis**: Pre-market/post-market price action
- **Entry timing**: Confirm daily signals with hourly structure
- **Stop placement**: More precise support/resistance levels

**Storage math**: 500 tickers × 7 hourly candles/day × 90 days = ~315K rows (manageable)

---

## Recommended Approach: 3 Separate Tables

### Why Separate Tables?

1. **Different retention**: Daily forever, hourly 90 days, intraday 7-30 days
2. **Different indexing**: Each optimized for its query patterns
3. **Query optimization**: Daily strategies don't scan hourly/intraday rows
4. **Maintenance**: Independent VACUUM/cleanup schedules

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                     3-TIER DATA ARCHITECTURE                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   ┌──────────────────┐   ┌──────────────────┐   ┌────────────────┐ │
│   │ stock_prices     │   │ stock_prices     │   │ stock_prices   │ │
│   │ _daily           │   │ _hourly          │   │ _intraday      │ │
│   │                  │   │                  │   │                │ │
│   │ 1 candle/day     │   │ 7 candles/day    │   │ 78+ candles/day│ │
│   │ Keep forever     │   │ 90-day retention │   │ 7-30 days      │ │
│   │ ~1K rows/ticker  │   │ ~630 rows/ticker │   │ ~5K+ rows/tick │ │
│   └────────┬─────────┘   └────────┬─────────┘   └───────┬────────┘ │
│            │                      │                      │         │
│            └──────────────────────┼──────────────────────┘         │
│                                   │                                 │
│                    ┌──────────────▼──────────────┐                 │
│                    │    Unified Query Layer      │                 │
│                    │    get_price_data(interval) │                 │
│                    └─────────────────────────────┘                 │
│                                   │                                 │
│                    ┌──────────────▼──────────────┐                 │
│                    │      Strategy Engine        │                 │
│                    │  (Gap, MA, Fib, Momentum)   │                 │
│                    └─────────────────────────────┘                 │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Phase 1: Rename + Add Hourly Table (No TimescaleDB)

### Step 1: Rename existing table

```sql
-- Rename to reflect actual content (daily candles)
ALTER TABLE stock_prices_hourly RENAME TO stock_prices_daily;

-- Update any views/constraints if needed
-- (Run this to identify dependencies first)
SELECT * FROM pg_depend WHERE refclassid = 'pg_class'::regclass 
    AND refobjid = 'stock_prices_hourly'::regclass;
```

### Step 2: Create hourly prices table

```sql
-- Hourly candles (7 per trading day)
CREATE TABLE stock_prices_hourly (
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

-- Indexes for hourly queries
CREATE INDEX idx_hourly_ticker_datetime ON stock_prices_hourly (ticker, datetime DESC);
CREATE INDEX idx_hourly_datetime ON stock_prices_hourly (datetime DESC);
```

### Step 3: Create intraday prices table

```sql
-- Intraday prices (1-min, 5-min, 15-min candles)
CREATE TABLE stock_prices_intraday (
    id BIGSERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    datetime TIMESTAMPTZ NOT NULL,
    interval VARCHAR(5) NOT NULL DEFAULT '5min',  -- '1min', '5min', '15min'
    open_price DECIMAL(12, 4) NOT NULL,
    high DECIMAL(12, 4) NOT NULL,
    low DECIMAL(12, 4) NOT NULL,
    close_price DECIMAL(12, 4) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT uq_intraday_ticker_datetime_interval 
        UNIQUE (ticker, datetime, interval)
);

-- Indexes for intraday queries
CREATE INDEX idx_intraday_ticker_datetime ON stock_prices_intraday (ticker, datetime DESC);
CREATE INDEX idx_intraday_interval_datetime ON stock_prices_intraday (interval, datetime DESC);
```

### Retention Jobs (run daily)

```sql
-- Delete hourly data older than 90 days
DELETE FROM stock_prices_hourly WHERE datetime < NOW() - INTERVAL '90 days';

-- Delete intraday data older than 7 days
DELETE FROM stock_prices_intraday 
WHERE datetime < NOW() - INTERVAL '7 days';
```

---

## Phase 2: TimescaleDB (Recommended for Scale)

### Installation

```bash
# Docker (add to docker-compose.yml)
services:
  db:
    image: timescale/timescaledb:latest-pg15
    # ... rest of config

# Or install extension on existing PostgreSQL
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

### Schema with Hypertables

```sql
-- Enable TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Intraday hypertable (auto-partitioned by time)
CREATE TABLE stock_prices_intraday (
    datetime TIMESTAMPTZ NOT NULL,
    ticker VARCHAR(10) NOT NULL,
    interval VARCHAR(5) NOT NULL DEFAULT '5min',
    open_price DECIMAL(12, 4) NOT NULL,
    high DECIMAL(12, 4) NOT NULL,
    low DECIMAL(12, 4) NOT NULL,
    close_price DECIMAL(12, 4) NOT NULL,
    volume BIGINT NOT NULL
);

-- Convert to hypertable (chunks by day)
SELECT create_hypertable('stock_prices_intraday', 'datetime',
    chunk_time_interval => INTERVAL '1 day'
);

-- Add unique constraint
ALTER TABLE stock_prices_intraday 
    ADD CONSTRAINT uq_intraday UNIQUE (ticker, datetime, interval);

-- Compression policy (compress chunks older than 2 days)
ALTER TABLE stock_prices_intraday SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'ticker,interval'
);
SELECT add_compression_policy('stock_prices_intraday', INTERVAL '2 days');

-- Retention policy (auto-delete after 30 days)
SELECT add_retention_policy('stock_prices_intraday', INTERVAL '30 days');

-- Continuous aggregate: 5-min candles from 1-min data
CREATE MATERIALIZED VIEW stock_prices_5min
WITH (timescaledb.continuous) AS
SELECT
    ticker,
    time_bucket('5 minutes', datetime) AS datetime,
    FIRST(open_price, datetime) AS open_price,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close_price, datetime) AS close_price,
    SUM(volume) AS volume
FROM stock_prices_intraday
WHERE interval = '1min'
GROUP BY ticker, time_bucket('5 minutes', datetime);
```

---

## Phase 3: Real-Time Layer (Redis)

For sub-second signal detection:

```python
# Redis for live quotes (pseudo-code)
import redis

r = redis.Redis(host='localhost', port=6379)

# Store latest quote
def update_live_quote(ticker: str, data: dict):
    r.hset(f"quote:{ticker}", mapping={
        "price": data["close"],
        "high": data["high"],
        "low": data["low"],
        "volume": data["volume"],
        "timestamp": datetime.now().isoformat()
    })
    r.expire(f"quote:{ticker}", 300)  # 5-min TTL

# Get latest quote
def get_live_quote(ticker: str) -> dict:
    return r.hgetall(f"quote:{ticker}")

# Pub/sub for real-time alerts
def publish_signal(signal: dict):
    r.publish("signals", json.dumps(signal))
```

---

## Unified Query Layer

Create a database service that queries appropriate table based on timeframe:

```python
# database.py additions

def get_price_data(
    ticker: str, 
    interval: str = "1day",
    periods: int = 100,
    end_date: datetime = None
) -> List[Dict]:
    """
    Unified price query - routes to appropriate table.
    
    Args:
        ticker: Stock symbol
        interval: '1min', '5min', '15min', '1hour', '1day'
        periods: Number of candles to fetch
        end_date: End date (default: now)
    """
    if interval == "1day":
        return _query_daily(ticker, periods, end_date)
    else:
        return _query_intraday(ticker, interval, periods, end_date)


def _query_daily(ticker: str, periods: int, end_date: datetime) -> List[Dict]:
    with get_db_cursor() as cursor:
        cursor.execute("""
            SELECT datetime, open_price as open, high, low, 
                   close_price as close, volume
            FROM stock_prices_daily
            WHERE ticker = %s
              AND datetime <= COALESCE(%s, NOW())
            ORDER BY datetime DESC
            LIMIT %s
        """, (ticker, end_date, periods))
        return cursor.fetchall()


def _query_intraday(
    ticker: str, 
    interval: str, 
    periods: int, 
    end_date: datetime
) -> List[Dict]:
    with get_db_cursor() as cursor:
        cursor.execute("""
            SELECT datetime, open_price as open, high, low,
                   close_price as close, volume
            FROM stock_prices_intraday
            WHERE ticker = %s
              AND interval = %s
              AND datetime <= COALESCE(%s, NOW())
            ORDER BY datetime DESC
            LIMIT %s
        """, (ticker, interval, end_date, periods))
        return cursor.fetchall()
```

---

## Strategy Engine Updates

Modify screeners to work with both daily and intraday data:

```python
# screeners.py updates

def scan_gap_strategies(
    tickers: List[str],
    interval: str = "1day",  # NEW: support '5min', '15min', etc.
    **kwargs
) -> List[GapResult]:
    """Run gap scan on specified interval."""
    
    if interval == "1day":
        # Use existing bulk_load_dataframes
        frames = bulk_load_dataframes(tickers, days=30)
    else:
        # Load intraday data
        frames = bulk_load_intraday_dataframes(
            tickers, 
            interval=interval, 
            periods=100
        )
    
    # ... rest of strategy logic unchanged
```

---

## Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA INGESTION                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Twelve Data API / Polygon.io / Alpaca                             │
│         │                                                           │
│         ▼                                                           │
│  ┌─────────────────┐                                               │
│  │ update_intraday │ ──(1-min candles)──▶ stock_prices_intraday    │
│  │   _prices.py    │                      (TimescaleDB hypertable) │
│  └─────────────────┘                              │                │
│         │                                         │                │
│         │                           ┌─────────────┴─────────────┐  │
│         │                           │ Continuous Aggregates     │  │
│         │                           │ (5min, 15min, 1hour)      │  │
│         │                           └─────────────┬─────────────┘  │
│         │                                         │                │
│         ▼                                         ▼                │
│  ┌─────────────────┐                     Rollup Job (EOD)          │
│  │ update_daily    │ ◀───────────────────────────┘                 │
│  │   _prices.py    │                                               │
│  └─────────────────┘                                               │
│         │                                                           │
│         ▼                                                           │
│  stock_prices_daily (existing table)                               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                      REAL-TIME SIGNALS                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  WebSocket Feed (Polygon/Alpaca)                                   │
│         │                                                           │
│         ▼                                                           │
│  ┌─────────────────┐     ┌─────────────────┐                       │
│  │ Redis (Live)    │────▶│ Signal Detector │                       │
│  │ Latest quotes   │     │ (Background)    │                       │
│  └─────────────────┘     └────────┬────────┘                       │
│                                   │                                 │
│                                   ▼                                 │
│                          WebSocket to UI                            │
│                          (Real-time alerts)                         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Migration Path

### Step 1: Rename existing table (optional but recommended)
```sql
ALTER TABLE stock_prices_hourly RENAME TO stock_prices_daily;
```

### Step 2: Create intraday table
```sql
CREATE TABLE stock_prices_intraday (...);
```

### Step 3: Update scripts
- `update_intraday_prices.py` → writes to `stock_prices_intraday`
- `update_daily_prices.py` → continues writing to `stock_prices_daily`

### Step 4: Update screeners
- Add `interval` parameter
- Use `get_bulk_price_data(tickers, interval)` unified query

### Step 5: Add retention jobs
```bash
# Cron: daily at 2 AM - cleanup old data
0 2 * * * psql -c "SELECT cleanup_old_hourly_data(90);"   -- 90-day retention
0 2 * * * psql -c "SELECT cleanup_old_intraday_data(7);"  -- 7-day retention
```

---

## Cost Estimates (Storage)

| Scenario | Daily | Hourly | Intraday (5-min) | Storage |
|----------|-------|--------|------------------|---------|
| 500 tickers, daily only | 126K rows | - | - | ~50 MB |
| + hourly (90 days) | 126K | 315K | - | ~150 MB |
| + 5-min intraday (7 days) | 126K | 315K | 2.7M | ~500 MB |
| + 1-min intraday (7 days) | 126K | 315K | 13.6M | ~2 GB |
| 1000 tickers, full setup | 252K | 630K | 5.4M | ~1 GB |
| With TimescaleDB compression | - | - | - | 50-70% less |

---

## Recommendation

**For your current scale (hundreds of tickers)**:
1. Start with **Phase 1** (rename table + add hourly + intraday tables)
2. Use hourly data for intraday trend analysis (90-day retention)
3. Use 5-min candles for real-time entry timing (7-day retention)

**When you scale to 1000+ tickers or need 1-min data**:
1. Migrate to **TimescaleDB** (Phase 2)
2. Use continuous aggregates for 1-min → 5-min → hourly → daily rollups
3. Add compression for cost savings

**For true real-time (sub-second)**:
1. Add **Redis** layer (Phase 3)
2. WebSocket for live signal push to UI

---

## Scripts Summary

| Script | Interval | Target Table | Schedule |
|--------|----------|--------------|----------|
| `update_daily_prices.py` | 1 day | stock_prices_daily | Daily 5:30 PM ET |
| `update_hourly_prices.py` | 1 hour | stock_prices_hourly | Hourly during market |
| `update_intraday_prices.py` | 15 min | stock_prices_daily (today's candle) | Every 15 min |

**Note**: The current `update_intraday_prices.py` updates the daily table with latest price.
For true 5-min candles, modify to write to `stock_prices_intraday` with `interval='5min'`.
