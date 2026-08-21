-- Idempotent occurrence ledger for live capture and historical scanner replay.

CREATE TABLE IF NOT EXISTS scanner_event_occurrences (
    occurrence_id       BIGSERIAL PRIMARY KEY,
    event_id            BIGINT NOT NULL REFERENCES scanner_events(event_id) ON DELETE CASCADE,
    signal_time         TIMESTAMPTZ NOT NULL,
    trade_date          DATE NOT NULL,
    trigger_type        VARCHAR(48) NOT NULL,
    discovery_state     VARCHAR(32),
    entry_price         DOUBLE PRECISION NOT NULL,
    atr_at_signal       DOUBLE PRECISION,
    reference_level     DOUBLE PRECISION,
    stop_price          DOUBLE PRECISION,
    target_price        DOUBLE PRECISION,
    risk_per_share      DOUBLE PRECISION,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    captured_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (event_id, signal_time)
);

CREATE INDEX IF NOT EXISTS idx_scanner_occurrences_time
    ON scanner_event_occurrences (signal_time DESC);
CREATE INDEX IF NOT EXISTS idx_scanner_occurrences_trade_date
    ON scanner_event_occurrences (trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_scanner_occurrences_event
    ON scanner_event_occurrences (event_id, signal_time);

COMMENT ON TABLE scanner_event_occurrences IS
    'One idempotent row per observed scanner lifecycle timestamp, including historical replay.';
