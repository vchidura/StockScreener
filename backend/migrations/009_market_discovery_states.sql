-- Shadow discovery states: continuation and reversal lanes remain separate.

CREATE TABLE IF NOT EXISTS market_discovery_states (
    discovery_id               BIGSERIAL PRIMARY KEY,
    trade_date                 DATE        NOT NULL,
    ticker                     VARCHAR(16) NOT NULL,
    model_version              VARCHAR(32) NOT NULL,
    state                      VARCHAR(32) NOT NULL,
    validation_status          VARCHAR(32) NOT NULL,
    activity_percentile        DOUBLE PRECISION,
    echo_percentile            DOUBLE PRECISION,
    older_momentum_percentile  DOUBLE PRECISION,
    long_momentum_percentile   DOUBLE PRECISION,
    recent_21d_percentile      DOUBLE PRECISION,
    recent_21d_return          DOUBLE PRECISION,
    recent_5d_return           DOUBLE PRECISION,
    close_price                DOUBLE PRECISION,
    sma_20                     DOUBLE PRECISION,
    sma_50                     DOUBLE PRECISION,
    higher_swing_high          BOOLEAN,
    higher_swing_low           BOOLEAN,
    evidence                   JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (trade_date, ticker, model_version)
);

CREATE INDEX IF NOT EXISTS idx_discovery_date_state
    ON market_discovery_states (trade_date DESC, state);
CREATE INDEX IF NOT EXISTS idx_discovery_ticker_date
    ON market_discovery_states (ticker, trade_date DESC);

COMMENT ON TABLE market_discovery_states IS
    'Daily shadow classifications. CONTINUATION is candidate alpha; reversal states are discovery-only.';
