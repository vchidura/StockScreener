SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.advanced_options_equity_input_bindings (
    target_options_window_id uuid PRIMARY KEY
        REFERENCES rt.window_runs(window_id)
        DEFERRABLE INITIALLY DEFERRED,
    target_cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    source_cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    source_cohort_sha256 char(64) NOT NULL,
    source_manifest_id uuid NOT NULL
        REFERENCES rt.subscription_manifests(manifest_id),
    source_manifest_sha256 char(64) NOT NULL,
    source_profile_id uuid NOT NULL
        REFERENCES rt.capability_profiles(profile_id),
    source_profile_sha256 char(64) NOT NULL,
    equity_window_id uuid NOT NULL REFERENCES rt.window_runs(window_id),
    equity_receipt_id uuid NOT NULL REFERENCES rt.cycle_receipts(receipt_id),
    equity_cutoff timestamptz NOT NULL,
    reference_generation bigint NOT NULL,
    universe_sha256 char(64) NOT NULL,
    input_sha256 char(64) NOT NULL,
    build_id varchar(96) NOT NULL,
    maximum_age_seconds integer NOT NULL,
    authorization_reference varchar(160) NOT NULL,
    binding_sha256 char(64) NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (target_cohort_id <> source_cohort_id),
    CHECK (source_cohort_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (source_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (source_profile_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (universe_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (binding_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (reference_generation > 0),
    CHECK (maximum_age_seconds > 0),
    CHECK (btrim(build_id) <> ''),
    CHECK (btrim(authorization_reference) <> '')
);

CREATE INDEX idx_rt_advanced_options_equity_input_source
    ON rt.advanced_options_equity_input_bindings (
        source_cohort_id,
        equity_cutoff,
        target_cohort_id
    );

CREATE INDEX idx_rt_advanced_options_equity_input_target
    ON rt.advanced_options_equity_input_bindings (
        target_cohort_id,
        target_options_window_id
    );

CREATE INDEX idx_rt_advanced_options_equity_input_receipt
    ON rt.advanced_options_equity_input_bindings (
        equity_receipt_id,
        target_cohort_id
    );

CREATE TRIGGER trg_rt_advanced_options_equity_input_bindings_immutable
    BEFORE UPDATE OR DELETE
    ON rt.advanced_options_equity_input_bindings
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_equity_input_bindings_no_truncate
    BEFORE TRUNCATE
    ON rt.advanced_options_equity_input_bindings
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
