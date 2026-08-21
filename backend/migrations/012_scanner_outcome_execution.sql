-- Record actionable next-bar-open execution separately from the signal-bar setup price.

ALTER TABLE scanner_event_outcomes
    ADD COLUMN IF NOT EXISTS entry_time TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS entry_price DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS entry_model VARCHAR(32) NOT NULL DEFAULT 'signal_close_v1';

COMMENT ON COLUMN scanner_event_outcomes.entry_time IS
    'Execution timestamp; next completed bar after the close-based signal.';
COMMENT ON COLUMN scanner_event_outcomes.entry_price IS
    'Actionable next-bar open used for return and risk calculations.';
COMMENT ON COLUMN scanner_event_outcomes.entry_model IS
    'Versioned execution assumption used to compute this derived outcome.';
