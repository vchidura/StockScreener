-- Long-only attribution and explicit universe identity for research runs.

ALTER TABLE research_runs
    ADD COLUMN IF NOT EXISTS activity_filter VARCHAR(32) NOT NULL DEFAULT 'none',
    ADD COLUMN IF NOT EXISTS top_net_return DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS top_net_sharpe DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS top_turnover DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS top_alpha_return DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS top_alpha_t_stat DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS top_alpha_sharpe DOUBLE PRECISION;

COMMENT ON COLUMN research_runs.activity_filter IS
    'Eligible-universe filter applied before neutralisation: none, liquidity, or composite.';
COMMENT ON COLUMN research_runs.top_alpha_return IS
    'Annualized top-decile net return minus the eligible-universe return.';
COMMENT ON COLUMN research_runs.top_alpha_t_stat IS
    'T-stat of the per-period top-decile net return relative to its eligible universe.';
