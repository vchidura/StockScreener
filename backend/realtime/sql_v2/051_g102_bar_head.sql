-- G102 design section 4 (A4): bars keyed by what they are.
--
-- rt.bar_current is keyed by cohort, so every new cohort starts an empty key space and readers stitch history back
-- together through rt.cohort_history_bindings (G83: an overnight read found 7 of 283 bars). rt.bar_head holds one
-- current bar per (instrument, interval, bar start, session scope, adjustment basis), chosen across all writers by
-- the selection policy `bar-selection-v1` (provider tier, then source precedence, then the later observation; see
-- equity_bar_repository.BAR_SELECTION_POLICY). The values are copied in, so most reads need no join with the
-- partitioned rt.bar_revisions, which stays the source of truth. The writing cohort is provenance only.
-- ON DELETE CASCADE follows the catalog, as rt.bar_current does, so retention keeps working.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '300s';

CREATE TABLE rt.bar_head (
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    interval varchar(8) NOT NULL,
    bar_start timestamptz NOT NULL,
    session_scope varchar(16) NOT NULL,
    adjustment_basis varchar(24) NOT NULL,
    bar_revision_id uuid NOT NULL REFERENCES rt.bar_catalog(bar_revision_id) ON DELETE CASCADE,
    provider_tier varchar(24) NOT NULL,
    source_kind varchar(24) NOT NULL,
    session_date date NOT NULL,
    bar_end timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    is_final boolean NOT NULL,
    open_price numeric(20,8) NOT NULL,
    high_price numeric(20,8) NOT NULL,
    low_price numeric(20,8) NOT NULL,
    close_price numeric(20,8) NOT NULL,
    volume numeric(28,8) NOT NULL,
    vwap numeric(20,8),
    transaction_count bigint,
    payload_sha256 char(64) NOT NULL,
    selection_policy varchar(32) NOT NULL,
    written_by_cohort_id uuid,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (instrument_id, interval, bar_start, session_scope, adjustment_basis),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (interval IN ('1m', '5m', '15m', '30m', '1h', '1d', '1wk', '1mo')),
    CHECK (session_scope IN ('RTH', 'EXTENDED', 'FULL_DAY')),
    CHECK (adjustment_basis IN ('RAW_UNADJUSTED', 'ADJUSTED')),
    CHECK (source_kind IN ('NATIVE_REST', 'REALTIME_STREAM', 'DERIVED', 'RECONCILED')),
    CHECK (bar_end > bar_start AND observed_at >= bar_end),
    CHECK (open_price > 0 AND high_price > 0 AND low_price > 0 AND close_price > 0),
    CHECK (high_price >= GREATEST(open_price, close_price, low_price)),
    CHECK (low_price <= LEAST(open_price, close_price, high_price)),
    CHECK (volume >= 0 AND (vwap IS NULL OR vwap > 0)),
    CHECK (transaction_count IS NULL OR transaction_count >= 0),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (selection_policy = 'bar-selection-v1')
) WITH (fillfactor = 80, autovacuum_analyze_scale_factor = 0, autovacuum_analyze_threshold = 5000);

-- Session reads (screening, coverage counts) and the cascade from the catalog.
CREATE INDEX idx_rt_bar_head_session ON rt.bar_head (session_date, interval, instrument_id);
CREATE INDEX idx_rt_bar_head_revision ON rt.bar_head (bar_revision_id);

-- Point-in-time reads for reruns (design 4.6) and the backfill find a key's revisions without the cohort.
CREATE INDEX idx_rt_bar_revision_key_end ON rt.bar_revisions (instrument_id, interval, bar_end DESC);
