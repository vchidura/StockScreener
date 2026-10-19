SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.advanced_options_reference_states (
    state_id uuid PRIMARY KEY,
    state_sha256 char(64) NOT NULL UNIQUE,
    state_version varchar(64) NOT NULL,
    reference_generation bigint NOT NULL UNIQUE,
    session_date date NOT NULL,
    known_at timestamptz NOT NULL,
    valid_until timestamptz NOT NULL,
    reference_policy_sha256 char(64) NOT NULL,
    contract_scope_sha256 char(64) NOT NULL
        REFERENCES rt.option_flow_contract_scopes(contract_scope_sha256),
    underlyings text[] NOT NULL,
    member_count integer NOT NULL,
    state_document jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (session_date, reference_generation),
    CHECK (
        state_version = 'advanced-options-reference-state-v1'
        AND state_sha256 ~ '^[0-9a-f]{64}$'
        AND reference_generation > 0
        AND known_at < valid_until
        AND reference_policy_sha256 ~ '^[0-9a-f]{64}$'
        AND contract_scope_sha256 ~ '^[0-9a-f]{64}$'
        AND cardinality(underlyings) > 0
        AND member_count > 0
        AND jsonb_typeof(state_document) = 'object'
    )
);

CREATE TABLE rt.advanced_options_reference_state_members (
    state_id uuid NOT NULL
        REFERENCES rt.advanced_options_reference_states(state_id),
    member_kind varchar(64) NOT NULL,
    member_key text NOT NULL,
    member_sha256 char(64) NOT NULL,
    availability varchar(16) NOT NULL,
    member_document jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (state_id, member_kind, member_key),
    CHECK (
        btrim(member_kind) <> ''
        AND btrim(member_key) <> ''
        AND member_sha256 ~ '^[0-9a-f]{64}$'
        AND availability IN ('AVAILABLE', 'UNAVAILABLE')
        AND jsonb_typeof(member_document) = 'object'
    )
);

CREATE TABLE rt.advanced_options_reference_session_freezes (
    session_date date PRIMARY KEY,
    state_id uuid NOT NULL
        REFERENCES rt.advanced_options_reference_states(state_id),
    first_cycle_id uuid NOT NULL UNIQUE,
    consumed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (session_date, state_id)
);

CREATE TABLE rt.advanced_options_reference_cycle_bindings (
    cycle_id uuid PRIMARY KEY,
    session_date date NOT NULL,
    state_id uuid NOT NULL,
    consumed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (session_date, state_id)
        REFERENCES rt.advanced_options_reference_session_freezes (
            session_date,
            state_id
        )
);

CREATE TRIGGER trg_rt_advanced_options_reference_states_immutable
    BEFORE UPDATE OR DELETE ON rt.advanced_options_reference_states
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_advanced_options_reference_states_no_truncate
    BEFORE TRUNCATE ON rt.advanced_options_reference_states
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_advanced_options_reference_state_members_immutable
    BEFORE UPDATE OR DELETE ON rt.advanced_options_reference_state_members
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_advanced_options_reference_state_members_no_truncate
    BEFORE TRUNCATE ON rt.advanced_options_reference_state_members
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_advanced_options_reference_session_freezes_immutable
    BEFORE UPDATE OR DELETE ON rt.advanced_options_reference_session_freezes
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_advanced_options_reference_session_freezes_no_truncate
    BEFORE TRUNCATE ON rt.advanced_options_reference_session_freezes
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_advanced_options_reference_cycle_bindings_immutable
    BEFORE UPDATE OR DELETE ON rt.advanced_options_reference_cycle_bindings
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_advanced_options_reference_cycle_bindings_no_truncate
    BEFORE TRUNCATE ON rt.advanced_options_reference_cycle_bindings
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
