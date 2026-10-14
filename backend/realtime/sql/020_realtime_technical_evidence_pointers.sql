SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE rt.equity_evidence_source_windows (
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
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
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, source_window_id),
    UNIQUE (cohort_id, source_window_sha256),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (interval IN ('30m', '1h', '1d', '1wk', '1mo')),
    CHECK (observed_at >= market_time),
    CHECK (cardinality(primary_bar_revision_ids) > 0),
    CHECK (source_window_sha256 ~ '^[0-9a-f]{64}$')
);

ALTER TABLE rt.equity_evidence
    ADD COLUMN source_window_id uuid,
    ADD COLUMN source_window_role varchar(16),
    ADD FOREIGN KEY (cohort_id, source_window_id)
        REFERENCES rt.equity_evidence_source_windows(cohort_id, source_window_id),
    ADD CONSTRAINT rt_equity_evidence_source_window_check CHECK (
        (source_window_id IS NULL AND source_window_role IS NULL)
        OR (source_window_id IS NOT NULL
            AND (source_window_role IS NULL
                OR source_window_role IN ('PRIMARY', 'CONFIRMATION')))
    );

CREATE TABLE rt.equity_evidence_dependencies (
    cohort_id uuid NOT NULL,
    evidence_id uuid NOT NULL,
    dependency_order integer NOT NULL,
    dependency_evidence_id uuid NOT NULL,
    PRIMARY KEY (cohort_id, evidence_id, dependency_order),
    UNIQUE (cohort_id, dependency_evidence_id, evidence_id),
    FOREIGN KEY (cohort_id, evidence_id)
        REFERENCES rt.equity_evidence(cohort_id, evidence_id),
    FOREIGN KEY (cohort_id, dependency_evidence_id)
        REFERENCES rt.equity_evidence(cohort_id, evidence_id),
    CHECK (dependency_order >= 0),
    CHECK (evidence_id <> dependency_evidence_id)
);

ALTER TABLE rt.equity_evidence_current
    ADD COLUMN interval varchar(8),
    ADD COLUMN model_variant varchar(96),
    ADD COLUMN payload_sha256 char(64),
    ADD COLUMN quality_state varchar(24),
    ADD COLUMN valid_until timestamptz;

UPDATE rt.equity_evidence_current AS current
SET interval = evidence.interval,
    model_variant = evidence.source_version,
    payload_sha256 = evidence.payload_sha256,
    quality_state = evidence.quality_state,
    valid_until = evidence.valid_until
FROM rt.equity_evidence AS evidence
WHERE evidence.cohort_id = current.cohort_id
  AND evidence.evidence_id = current.evidence_id;

ALTER TABLE rt.equity_evidence_current
    ALTER COLUMN model_variant SET NOT NULL,
    ALTER COLUMN payload_sha256 SET NOT NULL,
    ALTER COLUMN quality_state SET NOT NULL,
    DROP CONSTRAINT equity_evidence_current_pkey,
    ADD PRIMARY KEY (
        cohort_id, security_id, evidence_type, lifecycle_key, model_variant
    ),
    ADD CONSTRAINT rt_equity_evidence_current_interval_check
        CHECK (interval IS NULL OR interval IN (
            '1m', '5m', '15m', '30m', '1h', '1d', '1wk', '1mo'
        )),
    ADD CONSTRAINT rt_equity_evidence_current_model_variant_check
        CHECK (btrim(model_variant) <> ''),
    ADD CONSTRAINT rt_equity_evidence_current_payload_sha256_check
        CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT rt_equity_evidence_current_validity_check
        CHECK (valid_until IS NULL OR valid_until > market_time);

CREATE UNIQUE INDEX uq_rt_equity_technical_current
    ON rt.equity_evidence_current (
        cohort_id, security_id, interval, evidence_type, model_variant
    )
    WHERE interval IS NOT NULL
      AND evidence_type IN ('FEATURE_SNAPSHOT', 'TRADE_SETUP');

CREATE INDEX idx_rt_equity_evidence_technical_asof
    ON rt.equity_evidence (
        cohort_id, security_id, interval, evidence_type, source_version,
        market_time DESC, observed_at DESC, evidence_id
    )
    WHERE interval IS NOT NULL
      AND evidence_type IN ('FEATURE_SNAPSHOT', 'TRADE_SETUP');

CREATE TRIGGER trg_rt_equity_evidence_source_windows_immutable BEFORE UPDATE OR DELETE ON rt.equity_evidence_source_windows FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_equity_evidence_source_windows_no_truncate BEFORE TRUNCATE ON rt.equity_evidence_source_windows FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_equity_evidence_dependencies_immutable BEFORE UPDATE OR DELETE ON rt.equity_evidence_dependencies FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_equity_evidence_dependencies_no_truncate BEFORE TRUNCATE ON rt.equity_evidence_dependencies FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();