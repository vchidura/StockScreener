SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

-- Evidence identity catalog: owns (cohort, evidence_key) idempotency and is the FK target
-- for pointers and bodies. Rows are pruned with the monthly partition they describe.
CREATE TABLE rt.evidence_catalog (
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    evidence_id uuid NOT NULL,
    evidence_key text NOT NULL,
    provider_tier varchar(24) NOT NULL,
    evidence_type varchar(32) NOT NULL,
    interval varchar(8),
    security_id uuid NOT NULL REFERENCES ref.securities(security_id),
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    market_time timestamptz NOT NULL,
    durable_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, evidence_id),
    UNIQUE (cohort_id, evidence_key),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (interval IS NULL OR interval IN ('1m', '5m', '15m', '30m', '1h', '1d', '1wk', '1mo'))
);

CREATE INDEX idx_rt_evidence_catalog_prune ON rt.evidence_catalog (durable_at);
CREATE INDEX idx_rt_evidence_catalog_security
    ON rt.evidence_catalog (cohort_id, security_id, interval, evidence_type, market_time DESC);

CREATE TABLE rt.evidence_source_window_catalog (
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    source_window_id uuid NOT NULL,
    source_window_sha256 char(64) NOT NULL,
    interval varchar(8) NOT NULL,
    market_time timestamptz NOT NULL,
    durable_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, source_window_id),
    UNIQUE (cohort_id, source_window_sha256),
    CHECK (source_window_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (interval IN ('30m', '1h', '1d', '1wk', '1mo'))
);

CREATE INDEX idx_rt_evidence_source_window_catalog_prune
    ON rt.evidence_source_window_catalog (durable_at);

CREATE TABLE rt.equity_evidence (
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    evidence_id uuid NOT NULL,
    evidence_key text NOT NULL,
    lifecycle_key text,
    evidence_type varchar(32) NOT NULL,
    evidence_role varchar(24) NOT NULL,
    security_id uuid NOT NULL REFERENCES ref.securities(security_id),
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    ticker varchar(32) NOT NULL,
    interval varchar(8),
    direction smallint,
    lifecycle_status varchar(24) NOT NULL,
    strength double precision,
    market_time timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    valid_until timestamptz,
    source_name varchar(96) NOT NULL,
    source_version varchar(96) NOT NULL,
    payload_schema_version varchar(32) NOT NULL,
    analysis_run_id uuid,
    latest_bar_revision_id uuid,
    security_revision_id uuid,
    fundamental_report_ids uuid[] NOT NULL DEFAULT ARRAY[]::uuid[],
    source_revision_ids uuid[] NOT NULL DEFAULT ARRAY[]::uuid[],
    quality_state varchar(24) NOT NULL,
    quality_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    qualification_revision_id uuid,
    source_window_id uuid,
    source_window_role varchar(16),
    payload jsonb NOT NULL,
    payload_sha256 char(64) NOT NULL,
    materialization_input_sha256 char(64) NOT NULL,
    durable_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, evidence_id, durable_at),
    FOREIGN KEY (cohort_id, evidence_id) REFERENCES rt.evidence_catalog(cohort_id, evidence_id),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (ticker = upper(ticker) AND btrim(ticker) <> ''),
    CHECK (interval IS NULL OR interval IN ('1m', '5m', '15m', '30m', '1h', '1d', '1wk', '1mo')),
    CHECK (direction IS NULL OR direction IN (-1, 0, 1)),
    CHECK (strength IS NULL OR (strength >= 0 AND strength <= 1)),
    CHECK (observed_at >= market_time),
    CHECK (valid_until IS NULL OR valid_until > market_time),
    CHECK (jsonb_typeof(payload) = 'object'),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (materialization_input_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (
        (source_window_id IS NULL AND source_window_role IS NULL)
        OR (source_window_id IS NOT NULL
            AND (source_window_role IS NULL OR source_window_role IN ('PRIMARY', 'CONFIRMATION')))
    )
) PARTITION BY RANGE (durable_at);

CREATE INDEX idx_rt_equity_evidence_security_time
    ON rt.equity_evidence (cohort_id, security_id, market_time DESC);
CREATE INDEX idx_rt_equity_evidence_technical_asof
    ON rt.equity_evidence (
        cohort_id, security_id, interval, evidence_type, source_version,
        market_time DESC, observed_at DESC, evidence_id
    )
    WHERE interval IS NOT NULL
      AND evidence_type IN ('FEATURE_SNAPSHOT', 'TRADE_SETUP');

CREATE TABLE rt.equity_evidence_inputs (
    cohort_id uuid NOT NULL,
    evidence_id uuid NOT NULL,
    input_role varchar(32) NOT NULL,
    input_order integer NOT NULL,
    bar_revision_id uuid NOT NULL,
    provider_tier varchar(24) NOT NULL,
    input_sha256 char(64) NOT NULL,
    durable_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, evidence_id, input_role, input_order, durable_at),
    FOREIGN KEY (cohort_id, evidence_id) REFERENCES rt.evidence_catalog(cohort_id, evidence_id),
    CHECK (input_role IN ('PRIMARY_BAR', 'CONFIRMATION_BAR')),
    CHECK (input_order >= 0),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (input_sha256 ~ '^[0-9a-f]{64}$')
) PARTITION BY RANGE (durable_at);

-- dependency_evidence_id is a soft reference: a dependency can be older than its parent
-- and may already be pruned when the parent is still online.
CREATE TABLE rt.equity_evidence_dependencies (
    cohort_id uuid NOT NULL,
    evidence_id uuid NOT NULL,
    dependency_order integer NOT NULL,
    dependency_evidence_id uuid NOT NULL,
    durable_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, evidence_id, dependency_order, durable_at),
    FOREIGN KEY (cohort_id, evidence_id) REFERENCES rt.evidence_catalog(cohort_id, evidence_id),
    CHECK (dependency_order >= 0),
    CHECK (evidence_id <> dependency_evidence_id)
) PARTITION BY RANGE (durable_at);

CREATE INDEX idx_rt_equity_evidence_dependency_reverse
    ON rt.equity_evidence_dependencies (cohort_id, dependency_evidence_id, evidence_id);

CREATE TABLE rt.equity_evidence_source_windows (
    cohort_id uuid NOT NULL,
    source_window_id uuid NOT NULL,
    provider_tier varchar(24) NOT NULL,
    security_id uuid NOT NULL REFERENCES ref.securities(security_id),
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    interval varchar(8) NOT NULL,
    market_time timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    primary_bar_revision_ids uuid[] NOT NULL,
    confirmation_bar_revision_ids uuid[] NOT NULL DEFAULT ARRAY[]::uuid[],
    source_window_sha256 char(64) NOT NULL,
    durable_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, source_window_id, durable_at),
    FOREIGN KEY (cohort_id, source_window_id)
        REFERENCES rt.evidence_source_window_catalog(cohort_id, source_window_id),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (interval IN ('30m', '1h', '1d', '1wk', '1mo')),
    CHECK (observed_at >= market_time),
    CHECK (cardinality(primary_bar_revision_ids) > 0),
    CHECK (source_window_sha256 ~ '^[0-9a-f]{64}$')
) PARTITION BY RANGE (durable_at);

CREATE TABLE rt.feature_snapshots (
    snapshot_id uuid NOT NULL,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    bundle_sha256 char(64) NOT NULL REFERENCES rt.feature_definitions(bundle_sha256),
    generation bigint NOT NULL,
    source_cutoff timestamptz NOT NULL,
    computed_at timestamptz NOT NULL,
    valid_until timestamptz,
    quality varchar(24) NOT NULL,
    state_sha256 char(64) NOT NULL,
    input_watermarks jsonb NOT NULL,
    source_evidence_id uuid NOT NULL,
    durable_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (snapshot_id, durable_at),
    UNIQUE (cohort_id, instrument_id, bundle_sha256, generation, durable_at),
    FOREIGN KEY (cohort_id, source_evidence_id)
        REFERENCES rt.evidence_catalog(cohort_id, evidence_id),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (generation > 0),
    CHECK (computed_at >= source_cutoff),
    CHECK (valid_until IS NULL OR valid_until > source_cutoff),
    CHECK (quality IN ('COMPLETE', 'DEGRADED', 'RESEARCH_ONLY', 'STALE', 'FAILED')),
    CHECK (state_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(input_watermarks) = 'object')
) PARTITION BY RANGE (durable_at);

CREATE INDEX idx_rt_feature_snapshot_lookup
    ON rt.feature_snapshots (cohort_id, instrument_id, bundle_sha256, generation DESC);

-- snapshot_id is a soft reference; the writer enforces generation monotonicity here.
CREATE TABLE rt.feature_current (
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    bundle_sha256 char(64) NOT NULL REFERENCES rt.feature_definitions(bundle_sha256),
    generation bigint NOT NULL,
    snapshot_id uuid NOT NULL,
    source_cutoff timestamptz NOT NULL,
    computed_at timestamptz NOT NULL,
    valid_until timestamptz,
    quality varchar(24) NOT NULL,
    state_sha256 char(64) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, instrument_id, bundle_sha256),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (generation > 0),
    CHECK (computed_at >= source_cutoff),
    CHECK (valid_until IS NULL OR valid_until > source_cutoff),
    CHECK (quality IN ('COMPLETE', 'DEGRADED', 'RESEARCH_ONLY', 'STALE', 'FAILED')),
    CHECK (state_sha256 ~ '^[0-9a-f]{64}$')
) WITH (fillfactor = 80);

CREATE TABLE rt.equity_evidence_current (
    cohort_id uuid NOT NULL,
    provider_tier varchar(24) NOT NULL,
    security_id uuid NOT NULL,
    instrument_id bigint NOT NULL,
    evidence_type varchar(32) NOT NULL,
    lifecycle_key text NOT NULL,
    model_variant varchar(96) NOT NULL,
    interval varchar(8),
    evidence_id uuid NOT NULL,
    market_time timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    payload_sha256 char(64) NOT NULL,
    quality_state varchar(24) NOT NULL,
    valid_until timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, security_id, evidence_type, lifecycle_key, model_variant),
    FOREIGN KEY (cohort_id, evidence_id)
        REFERENCES rt.evidence_catalog(cohort_id, evidence_id) ON DELETE CASCADE,
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (btrim(lifecycle_key) <> ''),
    CHECK (btrim(model_variant) <> ''),
    CHECK (interval IS NULL OR interval IN ('1m', '5m', '15m', '30m', '1h', '1d', '1wk', '1mo')),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (observed_at >= market_time),
    CHECK (valid_until IS NULL OR valid_until > market_time)
) WITH (fillfactor = 80);

CREATE UNIQUE INDEX uq_rt_equity_technical_current
    ON rt.equity_evidence_current (
        cohort_id, security_id, interval, evidence_type, model_variant
    )
    WHERE interval IS NOT NULL
      AND evidence_type IN ('FEATURE_SNAPSHOT', 'TRADE_SETUP');

-- Decision inputs are retained by value in retained_input; ids are soft references so a
-- long-lived decision never blocks pruning of short-lived evidence, bars or marks.
CREATE TABLE rt.detector_event_inputs (
    event_id uuid NOT NULL REFERENCES rt.detector_events(event_id),
    input_role varchar(48) NOT NULL,
    input_key text NOT NULL,
    source_kind varchar(32) NOT NULL,
    feature_snapshot_id uuid,
    bar_revision_id uuid,
    option_mark_evidence_id uuid,
    source_record_id uuid,
    journal_generation uuid,
    stream_session_id uuid,
    source_offset bigint,
    input_sha256 char(64) NOT NULL,
    available_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    retained_input jsonb NOT NULL,
    PRIMARY KEY (event_id, input_role, input_key),
    CHECK (btrim(input_role) <> '' AND btrim(input_key) <> ''),
    CHECK (source_kind IN ('FEATURE_SNAPSHOT', 'BAR_REVISION', 'OPTION_MARK', 'REFERENCE', 'JOURNAL_EVENT')),
    CHECK (source_offset IS NULL OR source_offset > 0),
    CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (received_at >= available_at),
    CHECK (jsonb_typeof(retained_input) = 'object'),
    CHECK (
        num_nonnulls(feature_snapshot_id, bar_revision_id, option_mark_evidence_id, source_record_id)
        + CASE WHEN journal_generation IS NOT NULL AND stream_session_id IS NOT NULL
            AND source_offset IS NOT NULL THEN 1 ELSE 0 END = 1
    ),
    CHECK (
        (journal_generation IS NULL AND stream_session_id IS NULL AND source_offset IS NULL)
        OR (journal_generation IS NOT NULL AND stream_session_id IS NOT NULL AND source_offset IS NOT NULL)
    )
);

CREATE TRIGGER trg_rt_evidence_catalog_update
    BEFORE UPDATE ON rt.evidence_catalog
    FOR EACH ROW EXECUTE FUNCTION rt.reject_catalog_update();
CREATE TRIGGER trg_rt_evidence_source_window_catalog_update
    BEFORE UPDATE ON rt.evidence_source_window_catalog
    FOR EACH ROW EXECUTE FUNCTION rt.reject_catalog_update();
CREATE TRIGGER trg_rt_equity_evidence_immutable
    BEFORE UPDATE OR DELETE ON rt.equity_evidence
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_equity_evidence_inputs_immutable
    BEFORE UPDATE OR DELETE ON rt.equity_evidence_inputs
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_equity_evidence_dependencies_immutable
    BEFORE UPDATE OR DELETE ON rt.equity_evidence_dependencies
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_equity_evidence_source_windows_immutable
    BEFORE UPDATE OR DELETE ON rt.equity_evidence_source_windows
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_feature_snapshots_immutable
    BEFORE UPDATE OR DELETE ON rt.feature_snapshots
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_detector_event_inputs_immutable
    BEFORE UPDATE OR DELETE ON rt.detector_event_inputs
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_detector_event_inputs_no_truncate
    BEFORE TRUNCATE ON rt.detector_event_inputs
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
