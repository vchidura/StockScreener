SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE rt.feature_definitions (
    bundle_sha256 char(64) PRIMARY KEY,
    bundle_version varchar(96) NOT NULL,
    feature_schema_version varchar(64) NOT NULL,
    dependency_spec jsonb NOT NULL,
    policy_sha256 char(64) NOT NULL,
    code_sha256 char(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (bundle_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (code_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (btrim(bundle_version) <> '' AND btrim(feature_schema_version) <> ''),
    CHECK (jsonb_typeof(dependency_spec) = 'object')
);

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
    payload jsonb NOT NULL,
    payload_sha256 char(64) NOT NULL,
    materialization_input_sha256 char(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, evidence_id),
    UNIQUE (cohort_id, evidence_key),
    FOREIGN KEY (latest_bar_revision_id, cohort_id, provider_tier)
        REFERENCES rt.bar_revisions(bar_revision_id, cohort_id, provider_tier),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (ticker = upper(ticker) AND btrim(ticker) <> ''),
    CHECK (interval IS NULL OR interval IN ('1m', '5m', '15m', '30m', '1h', '1d', '1wk', '1mo')),
    CHECK (direction IS NULL OR direction IN (-1, 0, 1)),
    CHECK (strength IS NULL OR (strength >= 0 AND strength <= 1)),
    CHECK (observed_at >= market_time),
    CHECK (valid_until IS NULL OR valid_until > market_time),
    CHECK (jsonb_typeof(payload) = 'object'),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (materialization_input_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX idx_rt_equity_evidence_security_time
    ON rt.equity_evidence (cohort_id, security_id, market_time DESC);

CREATE TABLE rt.equity_evidence_inputs (
    cohort_id uuid NOT NULL,
    evidence_id uuid NOT NULL,
    input_role varchar(32) NOT NULL,
    input_order integer NOT NULL,
    bar_revision_id uuid NOT NULL,
    provider_tier varchar(24) NOT NULL,
    input_sha256 char(64) NOT NULL,
    PRIMARY KEY (cohort_id, evidence_id, input_role, input_order),
    FOREIGN KEY (cohort_id, evidence_id)
        REFERENCES rt.equity_evidence(cohort_id, evidence_id),
    FOREIGN KEY (bar_revision_id, cohort_id, provider_tier)
        REFERENCES rt.bar_revisions(bar_revision_id, cohort_id, provider_tier),
    CHECK (input_role IN ('PRIMARY_BAR', 'CONFIRMATION_BAR')),
    CHECK (input_order >= 0),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (input_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE rt.feature_snapshots (
    snapshot_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL,
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
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (snapshot_id, cohort_id, provider_tier, instrument_id, bundle_sha256),
    UNIQUE (cohort_id, instrument_id, bundle_sha256, generation),
    FOREIGN KEY (cohort_id, source_evidence_id)
        REFERENCES rt.equity_evidence(cohort_id, evidence_id),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (generation > 0),
    CHECK (computed_at >= source_cutoff),
    CHECK (valid_until IS NULL OR valid_until > source_cutoff),
    CHECK (quality IN ('COMPLETE', 'DEGRADED', 'RESEARCH_ONLY', 'STALE', 'FAILED')),
    CHECK (state_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(input_watermarks) = 'object')
);

CREATE INDEX idx_rt_feature_snapshot_lookup
    ON rt.feature_snapshots (cohort_id, instrument_id, bundle_sha256, generation DESC);

CREATE TABLE rt.feature_current (
    cohort_id uuid NOT NULL,
    provider_tier varchar(24) NOT NULL,
    instrument_id bigint NOT NULL,
    bundle_sha256 char(64) NOT NULL,
    generation bigint NOT NULL,
    snapshot_id uuid NOT NULL,
    source_cutoff timestamptz NOT NULL,
    computed_at timestamptz NOT NULL,
    valid_until timestamptz,
    quality varchar(24) NOT NULL,
    state_sha256 char(64) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, instrument_id, bundle_sha256),
    FOREIGN KEY (snapshot_id, cohort_id, provider_tier, instrument_id, bundle_sha256)
        REFERENCES rt.feature_snapshots(
            snapshot_id, cohort_id, provider_tier, instrument_id, bundle_sha256
        ),
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
    evidence_id uuid NOT NULL,
    market_time timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, security_id, evidence_type, lifecycle_key),
    FOREIGN KEY (cohort_id, evidence_id)
        REFERENCES rt.equity_evidence(cohort_id, evidence_id),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (btrim(lifecycle_key) <> ''),
    CHECK (observed_at >= market_time)
) WITH (fillfactor = 80);

CREATE TRIGGER trg_rt_feature_definitions_immutable BEFORE UPDATE OR DELETE ON rt.feature_definitions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_feature_definitions_no_truncate BEFORE TRUNCATE ON rt.feature_definitions FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_equity_evidence_immutable BEFORE UPDATE OR DELETE ON rt.equity_evidence FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_equity_evidence_no_truncate BEFORE TRUNCATE ON rt.equity_evidence FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_equity_evidence_inputs_immutable BEFORE UPDATE OR DELETE ON rt.equity_evidence_inputs FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_equity_evidence_inputs_no_truncate BEFORE TRUNCATE ON rt.equity_evidence_inputs FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_feature_snapshots_immutable BEFORE UPDATE OR DELETE ON rt.feature_snapshots FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_feature_snapshots_no_truncate BEFORE TRUNCATE ON rt.feature_snapshots FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();