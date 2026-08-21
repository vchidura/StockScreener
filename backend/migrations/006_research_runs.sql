-- Append-only log of alpha research runs, so signal decay is visible over time.

CREATE TABLE IF NOT EXISTS research_runs (
    run_id          SERIAL PRIMARY KEY,
    run_at          TIMESTAMPTZ  DEFAULT NOW(),

    features        TEXT         NOT NULL,
    horizon_days    SMALLINT     NOT NULL,
    rebalance_days  SMALLINT     NOT NULL,
    cost_bps        REAL         NOT NULL,
    neutralised     BOOLEAN      NOT NULL DEFAULT TRUE,

    data_start      DATE,
    data_end        DATE,
    rows_used       INTEGER,
    tickers         INTEGER,
    test_periods    INTEGER,

    ic_mean         DOUBLE PRECISION,
    ic_ir           DOUBLE PRECISION,
    ic_t_stat       DOUBLE PRECISION,
    decile_spread   DOUBLE PRECISION,

    ls_net_return   DOUBLE PRECISION,
    ls_net_sharpe   DOUBLE PRECISION,
    turnover        DOUBLE PRECISION,
    base_return     DOUBLE PRECISION,
    base_sharpe     DOUBLE PRECISION,

    verdict         VARCHAR(32),
    checks_passed   SMALLINT,
    checks_total    SMALLINT
);

CREATE INDEX IF NOT EXISTS idx_research_runs_at       ON research_runs (run_at DESC);
CREATE INDEX IF NOT EXISTS idx_research_runs_features ON research_runs (features, run_at DESC);

COMMENT ON TABLE research_runs IS
    'One row per run_alpha_research.py execution. Re-running the same feature set '
    'over time makes IC decay visible; compare ic_mean and ls_net_sharpe by run_at.';
