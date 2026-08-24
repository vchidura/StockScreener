-- Sector/index ETF benchmarks for scanner outcome alpha, replacing the self-referential
-- universe-average benchmark. See docs/SCANNER_ENHANCEMENTS_BACKLOG.md items 4/6/7/9.

ALTER TABLE scanner_event_outcomes
    ADD COLUMN IF NOT EXISTS market_benchmark_ticker  VARCHAR(8),
    ADD COLUMN IF NOT EXISTS sector_benchmark_ticker   VARCHAR(8),
    ADD COLUMN IF NOT EXISTS sector_benchmark_return   DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS sector_alpha_return        DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS sector_net_alpha_return    DOUBLE PRECISION;

COMMENT ON COLUMN scanner_event_outcomes.benchmark_return IS
    'Market benchmark (SPY, or QQQ fallback) forward return, next_bar_open_v2-aligned. '
    'Prior to 2026-08-23 this held the equal-weight tracked-universe average instead.';
COMMENT ON COLUMN scanner_event_outcomes.alpha_return IS
    'signed_return minus market benchmark_return (see benchmark_return comment for definition history).';
COMMENT ON COLUMN scanner_event_outcomes.net_alpha_return IS
    'alpha_return minus round-trip cost.';
COMMENT ON COLUMN scanner_event_outcomes.market_benchmark_ticker IS
    'Which instrument backed benchmark_return/alpha_return/net_alpha_return for this row (SPY or QQQ).';
COMMENT ON COLUMN scanner_event_outcomes.sector_benchmark_ticker IS
    'Sector SPDR/SMH/IGV ETF used for sector_benchmark_return, or SPY when the ticker is an ETF '
    'or unclassified (see research/gics_sectors.py SECTOR_BENCHMARK_ETF).';
COMMENT ON COLUMN scanner_event_outcomes.sector_benchmark_return IS
    'Sector ETF forward return, next_bar_open_v2-aligned, same horizon as this outcome row.';
COMMENT ON COLUMN scanner_event_outcomes.sector_alpha_return IS
    'signed_return minus sector_benchmark_return: idiosyncratic alpha net of sector rotation.';
COMMENT ON COLUMN scanner_event_outcomes.sector_net_alpha_return IS
    'sector_alpha_return minus round-trip cost.';
