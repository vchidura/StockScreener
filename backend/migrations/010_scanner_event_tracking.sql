-- Append-only shadow scanner events with incremental horizon outcomes.

CREATE TABLE IF NOT EXISTS scanner_events (
    event_id            BIGSERIAL PRIMARY KEY,
    event_key           VARCHAR(160) NOT NULL UNIQUE,
    scanner_name        VARCHAR(64)  NOT NULL,
    scanner_version     VARCHAR(32)  NOT NULL,
    interval            VARCHAR(8)   NOT NULL,
    ticker              VARCHAR(16)  NOT NULL,
    signal_time         TIMESTAMPTZ  NOT NULL,
    last_seen_at        TIMESTAMPTZ  NOT NULL,
    occurrence_count    INTEGER      NOT NULL DEFAULT 1,
    trade_date          DATE         NOT NULL,
    direction           SMALLINT     NOT NULL CHECK (direction IN (-1, 1)),
    trigger_type        VARCHAR(48)  NOT NULL,
    discovery_state     VARCHAR(32),
    validation_status   VARCHAR(32)  NOT NULL DEFAULT 'UNVALIDATED_TIMING',
    entry_price         DOUBLE PRECISION NOT NULL,
    atr_at_signal       DOUBLE PRECISION,
    reference_level     DOUBLE PRECISION,
    stop_price          DOUBLE PRECISION,
    target_price        DOUBLE PRECISION,
    risk_per_share      DOUBLE PRECISION,
    round_trip_cost_bps REAL NOT NULL DEFAULT 4.0,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    captured_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scanner_events_time
    ON scanner_events (signal_time DESC);
CREATE INDEX IF NOT EXISTS idx_scanner_events_ticker
    ON scanner_events (ticker, signal_time DESC);
CREATE INDEX IF NOT EXISTS idx_scanner_events_scanner
    ON scanner_events (scanner_name, interval, signal_time DESC);
CREATE INDEX IF NOT EXISTS idx_scanner_events_state
    ON scanner_events (discovery_state, signal_time DESC);

CREATE TABLE IF NOT EXISTS scanner_event_outcomes (
    outcome_id          BIGSERIAL PRIMARY KEY,
    event_id            BIGINT NOT NULL REFERENCES scanner_events(event_id) ON DELETE CASCADE,
    horizon_bars        SMALLINT NOT NULL CHECK (horizon_bars > 0),
    bars_observed       SMALLINT NOT NULL,
    exit_time           TIMESTAMPTZ NOT NULL,
    exit_price          DOUBLE PRECISION NOT NULL,
    raw_return          DOUBLE PRECISION NOT NULL,
    signed_return       DOUBLE PRECISION NOT NULL,
    net_signed_return   DOUBLE PRECISION NOT NULL,
    benchmark_return    DOUBLE PRECISION,
    alpha_return        DOUBLE PRECISION,
    net_alpha_return    DOUBLE PRECISION,
    mae_pct             DOUBLE PRECISION,
    mfe_pct             DOUBLE PRECISION,
    mae_r               DOUBLE PRECISION,
    mfe_r               DOUBLE PRECISION,
    stop_hit            BOOLEAN NOT NULL DEFAULT FALSE,
    target_hit          BOOLEAN NOT NULL DEFAULT FALSE,
    first_hit           VARCHAR(16) NOT NULL DEFAULT 'NONE'
                        CHECK (first_hit IN ('STOP', 'TARGET', 'SAME_BAR', 'NONE')),
    evaluated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (event_id, horizon_bars)
);

CREATE INDEX IF NOT EXISTS idx_scanner_outcomes_event
    ON scanner_event_outcomes (event_id, horizon_bars);
CREATE INDEX IF NOT EXISTS idx_scanner_outcomes_evaluated
    ON scanner_event_outcomes (evaluated_at DESC);

COMMENT ON TABLE scanner_events IS
    'Deduplicated shadow scanner triggers. No row is a trading recommendation.';
COMMENT ON TABLE scanner_event_outcomes IS
    'Incremental forward outcomes, MAE/MFE and stop/target sequencing by event horizon.';
