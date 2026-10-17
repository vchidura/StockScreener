SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.cohort_history_bindings (
    target_cohort_id uuid PRIMARY KEY REFERENCES rt.cohorts(cohort_id),
    source_cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    source_provider_tier varchar(24) NOT NULL,
    history_before timestamptz NOT NULL,
    source_manifest_sha256 char(64) NOT NULL,
    authorization_reference varchar(160) NOT NULL,
    binding_sha256 char(64) NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (target_cohort_id <> source_cohort_id),
    CHECK (source_provider_tier IN ('MASSIVE_DEVELOPER', 'MASSIVE_ADVANCED')),
    CHECK (source_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (binding_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (btrim(authorization_reference) <> '')
);

CREATE INDEX idx_rt_cohort_history_source
    ON rt.cohort_history_bindings (
        source_cohort_id,
        history_before,
        target_cohort_id
    );

CREATE TRIGGER trg_rt_cohort_history_bindings_immutable
    BEFORE UPDATE OR DELETE ON rt.cohort_history_bindings
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_cohort_history_bindings_no_truncate
    BEFORE TRUNCATE ON rt.cohort_history_bindings
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
