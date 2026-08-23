-- Standardize stock_prices_daily.datetime to TIMESTAMPTZ, matching
-- stock_prices_hourly and stock_prices_intraday. Existing values are naive
-- UTC midnight, so reinterpreting them "AT TIME ZONE 'UTC'" preserves the
-- same instant while adding timezone awareness.
BEGIN;

ALTER TABLE stock_prices_daily
    ALTER COLUMN datetime TYPE TIMESTAMPTZ
    USING datetime AT TIME ZONE 'UTC';

COMMIT;
