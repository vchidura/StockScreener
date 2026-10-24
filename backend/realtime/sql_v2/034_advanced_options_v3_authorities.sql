SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.advanced_options_session_acquisition_authorizations (
    authorization_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    session_date date NOT NULL,
    authorization_reference varchar(160) NOT NULL UNIQUE,
    admission_id uuid NOT NULL UNIQUE,
    admission_sha256 char(64) NOT NULL UNIQUE,
    readiness_sha256 char(64) NOT NULL UNIQUE,
    slot_set_member_sha256 char(64) NOT NULL,
    provider_budget_sha256 char(64) NOT NULL,
    socket_budget_sha256 char(64) NOT NULL,
    journal_generation uuid NOT NULL UNIQUE,
    journal_identity_sha256 char(64) NOT NULL UNIQUE,
    prepared_at timestamptz NOT NULL,
    acquisition_start timestamptz NOT NULL,
    capture_boundary timestamptz NOT NULL,
    acquisition_stop timestamptz NOT NULL,
    authorization_sha256 char(64) NOT NULL UNIQUE,
    authorization_document jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (prepared_at <= acquisition_start),
    CHECK (acquisition_start <= capture_boundary),
    CHECK (capture_boundary < acquisition_stop),
    CHECK (session_date = (prepared_at AT TIME ZONE 'UTC')::date),
    CHECK (session_date = (capture_boundary AT TIME ZONE 'UTC')::date),
    CHECK (admission_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (readiness_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (slot_set_member_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (provider_budget_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (socket_budget_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (journal_identity_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (authorization_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (btrim(authorization_reference) <> '')
);

CREATE TABLE rt.advanced_options_window_cycle_authorizations (
    authorization_id uuid PRIMARY KEY,
    session_authorization_id uuid NOT NULL REFERENCES
        rt.advanced_options_session_acquisition_authorizations(
            authorization_id
        ),
    target_window_id uuid NOT NULL UNIQUE REFERENCES rt.window_runs(window_id)
        DEFERRABLE INITIALLY DEFERRED,
    target_cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    authorization_reference varchar(160) NOT NULL UNIQUE,
    prepared_at timestamptz NOT NULL,
    history_binding_sha256 char(64) NOT NULL,
    equity_binding_sha256 char(64) NOT NULL,
    equity_receipt_id uuid NOT NULL REFERENCES rt.cycle_receipts(receipt_id),
    equity_cutoff timestamptz NOT NULL,
    equity_receipt_created_at timestamptz NOT NULL,
    options_cutoff timestamptz NOT NULL,
    age_policy_version varchar(96) NOT NULL,
    age_policy_sha256 char(64) NOT NULL,
    maximum_age_seconds integer NOT NULL,
    evidence_sha256 char(64) NOT NULL,
    build_id varchar(96) NOT NULL,
    publication_authorization_reference varchar(160),
    authorization_sha256 char(64) NOT NULL UNIQUE,
    authorization_document jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (equity_cutoff <= equity_receipt_created_at),
    CHECK (equity_receipt_created_at <= prepared_at),
    CHECK (prepared_at <= options_cutoff),
    CHECK (
        options_cutoff <= equity_cutoff
            + make_interval(secs => maximum_age_seconds)
    ),
    CHECK (maximum_age_seconds > 0),
    CHECK (history_binding_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (equity_binding_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (age_policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (authorization_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (btrim(authorization_reference) <> ''),
    CHECK (btrim(age_policy_version) <> ''),
    CHECK (btrim(build_id) <> ''),
    CHECK (
        publication_authorization_reference IS NULL
        OR btrim(publication_authorization_reference) <> ''
    )
);

CREATE TABLE rt.advanced_options_authority_receipts (
    receipt_id uuid PRIMARY KEY,
    authority_kind varchar(32) NOT NULL,
    session_authorization_id uuid UNIQUE REFERENCES
        rt.advanced_options_session_acquisition_authorizations(
            authorization_id
        ),
    window_cycle_authorization_id uuid UNIQUE REFERENCES
        rt.advanced_options_window_cycle_authorizations(authorization_id),
    authority_sha256 char(64) NOT NULL,
    effect_kind varchar(32) NOT NULL,
    effect_identity_sha256 char(64) NOT NULL,
    consumed_at timestamptz NOT NULL,
    receipt_sha256 char(64) NOT NULL UNIQUE,
    receipt_document jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (
            authority_kind = 'SESSION_ACQUISITION'
            AND session_authorization_id IS NOT NULL
            AND window_cycle_authorization_id IS NULL
            AND effect_kind IN ('PROVIDER_REQUEST', 'PROVIDER_SOCKET')
        )
        OR
        (
            authority_kind = 'WINDOW_CYCLE'
            AND session_authorization_id IS NULL
            AND window_cycle_authorization_id IS NOT NULL
            AND effect_kind IN ('TARGET_WINDOW_WRITE', 'TARGET_WINDOW_STAGE')
        )
    ),
    CHECK (authority_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (effect_identity_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (receipt_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TRIGGER trg_rt_advanced_options_session_acquisition_authorizations_immutable
    BEFORE UPDATE OR DELETE
    ON rt.advanced_options_session_acquisition_authorizations
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_session_acquisition_authorizations_no_truncate
    BEFORE TRUNCATE
    ON rt.advanced_options_session_acquisition_authorizations
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_window_cycle_authorizations_immutable
    BEFORE UPDATE OR DELETE
    ON rt.advanced_options_window_cycle_authorizations
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_window_cycle_authorizations_no_truncate
    BEFORE TRUNCATE
    ON rt.advanced_options_window_cycle_authorizations
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_authority_receipts_immutable
    BEFORE UPDATE OR DELETE
    ON rt.advanced_options_authority_receipts
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_authority_receipts_no_truncate
    BEFORE TRUNCATE
    ON rt.advanced_options_authority_receipts
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
