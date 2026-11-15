-- Universe sizing 3.3 (Nov 15, WP-B): a member minute without a trade inside a session. The gap repair records the
-- minutes the provider's full-session answer has no bar for (the stream had none either) on the repaired gap row, and
-- the Equity input gate accepts exactly those minutes instead of failing the whole window. Read and written only with
-- REALTIME_V2_CONFIRMED_EMPTY_MINUTES=ON (realtime/confirmed_empty_minutes.py); with the switch off nothing touches it.
-- A constant default makes the column a catalog-only change (no table rewrite).
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE rt.stream_gaps
    ADD COLUMN provider_confirmed_empty_starts timestamptz[] NOT NULL DEFAULT '{}'::timestamptz[];

ALTER TABLE rt.stream_gaps
    ADD CONSTRAINT stream_gaps_confirmed_empty_within_gap CHECK (
        cardinality(provider_confirmed_empty_starts) = 0
        OR (
            gap_end IS NOT NULL
            AND gap_start <= ALL (provider_confirmed_empty_starts)
            AND gap_end > ALL (provider_confirmed_empty_starts)
        )
    ) NOT VALID;
