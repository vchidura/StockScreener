-- Stock Screener Database Migration: Add Hourly + Intraday Tables
-- Run this script to add multi-timeframe data support
--
-- Current state: stock_prices_hourly contains DAILY data (misleading name)
-- After migration: 3 tables for daily, hourly, and intraday data
--
-- Usage:
--   psql -U your_user -d stocks_db -f migrations/001_add_intraday_table.sql

BEGIN;

-- =============================================================================
-- Step 1: Rename existing table to reflect actual content (DAILY candles)
-- =============================================================================
-- Note: This renames the table. Update your code to use stock_prices_daily
-- or create a view: CREATE VIEW stock_prices_hourly AS SELECT * FROM stock_prices_daily;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'stock_prices_hourly') 
       AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'stock_prices_daily') THEN
        ALTER TABLE stock_prices_hourly RENAME TO stock_prices_daily;
        RAISE NOTICE 'Renamed stock_prices_hourly → stock_prices_daily';
    ELSE
        RAISE NOTICE 'Table rename skipped (already done or source not found)';
    END IF;
END $$;

-- Create backward-compatible view (optional - remove after code migration)
CREATE OR REPLACE VIEW stock_prices_hourly AS SELECT * FROM stock_prices_daily;

-- =============================================================================
-- Step 2: Create TRUE hourly prices table
-- =============================================================================
CREATE TABLE IF NOT EXISTS stock_prices_hourly_new (
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
    ON stock_prices_hourly_new (ticker, datetime DESC);
CREATE INDEX IF NOT EXISTS idx_hourly_datetime 
    ON stock_prices_hourly_new (datetime DESC);

-- =============================================================================
-- Step 3: Create intraday prices table (1-min, 5-min candles)
-- =============================================================================
CREATE TABLE IF NOT EXISTS stock_prices_intraday (
    id BIGSERIAL,
    ticker VARCHAR(10) NOT NULL,
    datetime TIMESTAMPTZ NOT NULL,
    interval VARCHAR(5) NOT NULL DEFAULT '5min',  -- '1min', '5min', '15min', '1hour'
    open_price DECIMAL(12, 4) NOT NULL,
    high DECIMAL(12, 4) NOT NULL,
    low DECIMAL(12, 4) NOT NULL,
    close_price DECIMAL(12, 4) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Primary key on id for fast inserts
    PRIMARY KEY (id),
    
    -- Unique constraint for upserts
    CONSTRAINT uq_intraday_ticker_datetime_interval 
        UNIQUE (ticker, datetime, interval)
);

CREATE INDEX IF NOT EXISTS idx_intraday_ticker_datetime 
    ON stock_prices_intraday (ticker, datetime DESC);
CREATE INDEX IF NOT EXISTS idx_intraday_datetime 
    ON stock_prices_intraday (datetime DESC);
CREATE INDEX IF NOT EXISTS idx_intraday_interval_datetime 
    ON stock_prices_intraday (interval, datetime DESC);
CREATE INDEX IF NOT EXISTS idx_intraday_ticker_interval 
    ON stock_prices_intraday (ticker, interval, datetime DESC);

-- =============================================================================
-- Step 4: Create retention cleanup functions
-- =============================================================================
CREATE OR REPLACE FUNCTION cleanup_old_hourly_data(retention_days INTEGER DEFAULT 90)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM stock_prices_hourly_new 
    WHERE datetime < NOW() - (retention_days || ' days')::INTERVAL;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % old hourly rows', deleted_count;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cleanup_old_intraday_data(retention_days INTEGER DEFAULT 7)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM stock_prices_intraday 
    WHERE datetime < NOW() - (retention_days || ' days')::INTERVAL;
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % old intraday rows', deleted_count;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Step 5: Create helpful views

-- View: Latest intraday price per ticker
CREATE OR REPLACE VIEW v_latest_intraday AS
SELECT DISTINCT ON (ticker, interval)
    ticker,
    interval,
    datetime,
    open_price,
    high,
    low,
    close_price,
    volume
FROM stock_prices_intraday
ORDER BY ticker, interval, datetime DESC;

-- View: Intraday data stats
CREATE OR REPLACE VIEW v_intraday_stats AS
SELECT 
    interval,
    COUNT(DISTINCT ticker) AS ticker_count,
    COUNT(*) AS total_rows,
    MIN(datetime) AS oldest_data,
    MAX(datetime) AS newest_data,
    pg_size_pretty(pg_relation_size('stock_prices_intraday')) AS table_size
FROM stock_prices_intraday
GROUP BY interval;

COMMIT;

-- Informational output
DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Migration completed successfully!';
    RAISE NOTICE '';
    RAISE NOTICE 'New table: stock_prices_intraday';
    RAISE NOTICE 'New function: cleanup_old_intraday_data(days)';
    RAISE NOTICE 'New views: v_latest_intraday, v_intraday_stats';
    RAISE NOTICE '';
    RAISE NOTICE 'To cleanup old data manually:';
    RAISE NOTICE '  SELECT cleanup_old_intraday_data(7);';
    RAISE NOTICE '';
    RAISE NOTICE 'To check stats:';
    RAISE NOTICE '  SELECT * FROM v_intraday_stats;';
    RAISE NOTICE '========================================';
END $$;
