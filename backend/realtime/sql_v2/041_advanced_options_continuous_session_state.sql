SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.advanced_options_continuous_sessions (
    session_binding_id uuid PRIMARY KEY,
    session_binding_sha256 char(64) NOT NULL UNIQUE,
    session_date date NOT NULL UNIQUE,
    desired_revision_id uuid NOT NULL
        REFERENCES rt.realtime_desired_session_snapshots(revision_id),
    desired_snapshot_sha256 char(64) NOT NULL,
    desired_manifest_id uuid NOT NULL
        REFERENCES rt.subscription_manifests(manifest_id),
    desired_cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    options_cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    plan_id uuid NOT NULL REFERENCES rt.service_plans(plan_id),
    session_authorization_id uuid NOT NULL UNIQUE
        REFERENCES rt.advanced_options_session_acquisition_authorizations(
            authorization_id
        ),
    reference_state_id uuid NOT NULL
        REFERENCES rt.advanced_options_reference_states(state_id),
    reference_state_sha256 char(64) NOT NULL,
    source_selection_sha256 char(64) NOT NULL,
    package_sha256 char(64) NOT NULL UNIQUE,
    acquisition_binding_id uuid NOT NULL UNIQUE,
    acquisition_binding_sha256 char(64) NOT NULL UNIQUE,
    execution_bundle_sha256 char(64) NOT NULL UNIQUE,
    prepared_at timestamptz NOT NULL,
    acquisition_start timestamptz NOT NULL,
    decision_stop timestamptz NOT NULL,
    package_document jsonb NOT NULL,
    authority_bundle_document jsonb NOT NULL,
    acquisition_binding_document jsonb NOT NULL,
    session_binding_document jsonb NOT NULL,
    execution_bundle_document jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (desired_cohort_id <> options_cohort_id),
    CHECK (prepared_at <= acquisition_start),
    CHECK (acquisition_start < decision_stop),
    CHECK (session_date = (prepared_at AT TIME ZONE 'UTC')::date),
    CHECK (session_date = (acquisition_start AT TIME ZONE 'UTC')::date),
    CHECK (session_date = (decision_stop AT TIME ZONE 'UTC')::date),
    CHECK (session_binding_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (desired_snapshot_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (reference_state_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (source_selection_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (package_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (acquisition_binding_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (execution_bundle_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(package_document) = 'object'),
    CHECK (jsonb_typeof(authority_bundle_document) = 'object'),
    CHECK (jsonb_typeof(acquisition_binding_document) = 'object'),
    CHECK (jsonb_typeof(session_binding_document) = 'object'),
    CHECK (jsonb_typeof(execution_bundle_document) = 'object')
);

CREATE INDEX idx_rt_advanced_options_continuous_session_current
    ON rt.advanced_options_continuous_sessions (
        decision_stop DESC,
        acquisition_start DESC
    );

CREATE TABLE rt.advanced_options_downstream_bindings (
    session_binding_id uuid NOT NULL
        REFERENCES rt.advanced_options_continuous_sessions(
            session_binding_id
        ),
    sequence smallint NOT NULL,
    binding_id uuid NOT NULL UNIQUE,
    binding_sha256 char(64) NOT NULL UNIQUE,
    previous_binding_sha256 char(64),
    target_window_id uuid NOT NULL UNIQUE
        REFERENCES rt.window_runs(window_id)
        DEFERRABLE INITIALLY DEFERRED,
    equity_receipt_id uuid NOT NULL
        REFERENCES rt.cycle_receipts(receipt_id),
    finalized_cutoff timestamptz NOT NULL,
    options_cutoff timestamptz NOT NULL,
    binding_document jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (session_binding_id, sequence),
    UNIQUE (session_binding_id, sequence, binding_sha256),
    UNIQUE (session_binding_id, binding_sha256),
    CHECK (sequence BETWEEN 1 AND 78),
    CHECK (finalized_cutoff <= options_cutoff),
    CHECK (
        options_cutoff
        <= finalized_cutoff + make_interval(secs => 300)
    ),
    CHECK (binding_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (
        previous_binding_sha256 IS NULL
        OR previous_binding_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CHECK (jsonb_typeof(binding_document) = 'object')
);

CREATE INDEX idx_rt_advanced_options_downstream_binding_cutoff
    ON rt.advanced_options_downstream_bindings (
        session_binding_id,
        finalized_cutoff DESC
    );

CREATE TABLE rt.advanced_options_downstream_heads (
    session_binding_id uuid PRIMARY KEY
        REFERENCES rt.advanced_options_continuous_sessions(
            session_binding_id
        ),
    sequence smallint NOT NULL DEFAULT 0,
    binding_sha256 char(64),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (
        session_binding_id,
        sequence,
        binding_sha256
    ) REFERENCES rt.advanced_options_downstream_bindings (
        session_binding_id,
        sequence,
        binding_sha256
    ) DEFERRABLE INITIALLY DEFERRED,
    CHECK (sequence BETWEEN 0 AND 78),
    CHECK (
        (sequence = 0 AND binding_sha256 IS NULL)
        OR (
            sequence > 0
            AND binding_sha256 ~ '^[0-9a-f]{64}$'
        )
    )
) WITH (fillfactor = 80);

CREATE TRIGGER trg_rt_advanced_options_continuous_sessions_immutable
    BEFORE UPDATE OR DELETE
    ON rt.advanced_options_continuous_sessions
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_continuous_sessions_no_truncate
    BEFORE TRUNCATE
    ON rt.advanced_options_continuous_sessions
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_downstream_bindings_immutable
    BEFORE UPDATE OR DELETE
    ON rt.advanced_options_downstream_bindings
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_downstream_bindings_no_truncate
    BEFORE TRUNCATE
    ON rt.advanced_options_downstream_bindings
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
