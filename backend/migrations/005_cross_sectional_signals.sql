-- Cross-sectional signal produced by the validated momentum model.
-- One row per (trade_date, ticker, model_version).

CREATE TABLE IF NOT EXISTS cross_sectional_signals (
    signal_id       SERIAL PRIMARY KEY,
    trade_date      DATE         NOT NULL,
    ticker          VARCHAR(16)  NOT NULL,
    model_version   VARCHAR(32)  NOT NULL,
    horizon_days    SMALLINT     NOT NULL,

    raw_score       DOUBLE PRECISION,   -- standardized feature blend
    neutral_score   DOUBLE PRECISION,   -- residual after beta/size/vol/sector
    percentile      DOUBLE PRECISION,   -- 0..1 rank within the day's universe
    decile          SMALLINT,           -- 1 (worst) .. 10 (best)
    side            VARCHAR(5),         -- LONG / SHORT / FLAT

    universe_size   INTEGER      NOT NULL,
    created_at      TIMESTAMPTZ  DEFAULT NOW(),

    CONSTRAINT uq_xs_signal UNIQUE (trade_date, ticker, model_version)
);

CREATE INDEX IF NOT EXISTS idx_xs_signals_date       ON cross_sectional_signals (trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_xs_signals_ticker     ON cross_sectional_signals (ticker, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_xs_signals_date_side  ON cross_sectional_signals (trade_date DESC, side);

COMMENT ON TABLE cross_sectional_signals IS
    'Validated cross-sectional momentum signal. Rebalance horizon is horizon_days; '
    'only decile 10 (LONG) and decile 1 (SHORT) are actionable.';
COMMENT ON COLUMN cross_sectional_signals.neutral_score IS
    'Score after removing beta / liquidity / volatility exposure and sector means. '
    'This is the tradable number; raw_score still contains risk exposure.';
