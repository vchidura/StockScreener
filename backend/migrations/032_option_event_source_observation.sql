-- Keep provider publication time separate from actual system receipt time.

ALTER TABLE option_market_events
    ADD COLUMN IF NOT EXISTS source_observed_at timestamp with time zone;

ALTER TABLE option_event_calendar_coverage
    ADD COLUMN IF NOT EXISTS source_observed_at timestamp with time zone;

COMMENT ON COLUMN option_market_events.source_observed_at IS
    'Timestamp declared by the source document; first_observed_at is actual system receipt.';
COMMENT ON COLUMN option_event_calendar_coverage.source_observed_at IS
    'Timestamp declared by the source document; first_observed_at is actual system receipt.';