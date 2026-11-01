SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE rt.advanced_options_reference_states
    DROP CONSTRAINT advanced_options_reference_states_check,
    ADD CONSTRAINT advanced_options_reference_states_check
        CHECK (
            state_version IN (
                'advanced-options-reference-state-v1',
                'advanced-options-reference-state-v2'
            )
            AND state_sha256 ~ '^[0-9a-f]{64}$'
            AND reference_generation > 0
            AND known_at < valid_until
            AND reference_policy_sha256 ~ '^[0-9a-f]{64}$'
            AND contract_scope_sha256 ~ '^[0-9a-f]{64}$'
            AND cardinality(underlyings) > 0
            AND member_count > 0
            AND jsonb_typeof(state_document) = 'object'
        );

ALTER TABLE rt.window_runs
    DROP CONSTRAINT window_runs_state_check,
    ADD CONSTRAINT window_runs_state_check
        CHECK (
            state IN (
                'PENDING','RUNNING','COMPLETE','FAILED','MISSED',
                'RETAINED_GAP','UNAVAILABLE'
            )
        );

ALTER TABLE rt.stage_runs
    DROP CONSTRAINT stage_runs_state_check,
    ADD CONSTRAINT stage_runs_state_check
        CHECK (
            state IN (
                'PENDING','RUNNING','COMPLETE','FAILED',
                'BLOCKED_PREREQ','BLOCKED_POLICY','MISSED',
                'DEFERRED','UNAVAILABLE'
            )
        );

DROP TRIGGER trg_rt_advanced_options_v3_stage_effect_authorized
    ON rt.stage_runs;
CREATE TRIGGER trg_rt_advanced_options_v3_stage_effect_authorized
    BEFORE INSERT
    ON rt.stage_runs
    FOR EACH ROW
    WHEN (NEW.state <> 'UNAVAILABLE')
    EXECUTE FUNCTION rt.validate_advanced_options_v3_stage_effect();

CREATE TABLE rt.advanced_options_selection_generations (
    selection_generation_id uuid PRIMARY KEY,
    selection_generation_sha256 char(64) NOT NULL UNIQUE,
    session_binding_id uuid NOT NULL UNIQUE
        REFERENCES rt.advanced_options_continuous_sessions(
            session_binding_id
        ),
    session_date date NOT NULL,
    desired_revision_id uuid NOT NULL
        REFERENCES rt.realtime_desired_session_snapshots(revision_id),
    desired_snapshot_sha256 char(64) NOT NULL,
    selection_policy_version text NOT NULL,
    selection_policy_sha256 char(64) NOT NULL,
    flow_policy_sha256 char(64) NOT NULL,
    admission_profile_version text NOT NULL,
    admission_profile_sha256 char(64) NOT NULL,
    allocation_sha256 char(64) NOT NULL,
    broad_underlyings text[] NOT NULL,
    flow_underlyings text[] NOT NULL,
    capacity_document jsonb NOT NULL,
    allocation_document jsonb NOT NULL,
    selection_document jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (cardinality(broad_underlyings) > 0),
    CHECK (flow_underlyings <@ broad_underlyings),
    CHECK (selection_generation_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (desired_snapshot_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (selection_policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (flow_policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (admission_profile_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (allocation_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(capacity_document) = 'object'),
    CHECK (jsonb_typeof(allocation_document) = 'object'),
    CHECK (jsonb_typeof(selection_document) = 'object')
);

CREATE TABLE rt.advanced_options_selection_members (
    selection_generation_id uuid NOT NULL
        REFERENCES rt.advanced_options_selection_generations(
            selection_generation_id
        ),
    contract_id bigint NOT NULL,
    contract_ticker text NOT NULL,
    underlying text NOT NULL,
    flow_eligible boolean NOT NULL,
    member_sha256 char(64) NOT NULL,
    member_document jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (selection_generation_id, contract_id),
    UNIQUE (selection_generation_id, contract_ticker),
    CHECK (contract_id > 0),
    CHECK (member_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(member_document) = 'object')
);

CREATE INDEX idx_rt_advanced_options_selection_member_underlying
    ON rt.advanced_options_selection_members (
        selection_generation_id,
        underlying,
        flow_eligible
    );

CREATE TABLE rt.advanced_options_selection_coverage (
    selection_generation_id uuid NOT NULL
        REFERENCES rt.advanced_options_selection_generations(
            selection_generation_id
        ),
    underlying text NOT NULL,
    role text NOT NULL,
    state text NOT NULL,
    planned_group_count integer NOT NULL,
    admitted_group_count integer NOT NULL,
    planned_contract_count integer NOT NULL,
    admitted_contract_count integer NOT NULL,
    ready_contract_count integer NOT NULL,
    missing_oi_contract_count integer NOT NULL,
    reason_codes text[] NOT NULL,
    rejected_group_samples text[] NOT NULL,
    coverage_document jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (selection_generation_id, underlying, role),
    CHECK (state IN ('FULL','PARTIAL','UNCOVERED','UNAVAILABLE')),
    CHECK (planned_group_count >= admitted_group_count),
    CHECK (admitted_group_count >= 0),
    CHECK (planned_contract_count >= admitted_contract_count),
    CHECK (admitted_contract_count >= ready_contract_count),
    CHECK (ready_contract_count >= 0),
    CHECK (missing_oi_contract_count >= 0),
    CHECK (jsonb_typeof(coverage_document) = 'object')
);

CREATE TABLE rt.advanced_options_underlying_readiness (
    selection_generation_id uuid NOT NULL
        REFERENCES rt.advanced_options_selection_generations(
            selection_generation_id
        ),
    underlying text NOT NULL,
    catalog_state text NOT NULL,
    open_interest_state text NOT NULL,
    history_state text NOT NULL,
    ready boolean NOT NULL,
    reason_codes text[] NOT NULL,
    readiness_document jsonb NOT NULL,
    observed_at timestamptz NOT NULL,
    PRIMARY KEY (selection_generation_id, underlying),
    CHECK (
        catalog_state IN (
            'AVAILABLE','WARMING','FAILED','UNAVAILABLE','NOT_REQUIRED'
        )
    ),
    CHECK (
        open_interest_state IN (
            'AVAILABLE','WARMING','FAILED','UNAVAILABLE','NOT_REQUIRED'
        )
    ),
    CHECK (
        history_state IN (
            'AVAILABLE','WARMING','FAILED','UNAVAILABLE','NOT_REQUIRED'
        )
    ),
    CHECK (jsonb_typeof(readiness_document) = 'object')
) WITH (fillfactor = 80);

CREATE TABLE rt.advanced_options_physical_subscriptions (
    selection_generation_id uuid NOT NULL
        REFERENCES rt.advanced_options_selection_generations(
            selection_generation_id
        ),
    subscription_id uuid NOT NULL,
    account_alias text NOT NULL,
    provider_cluster text NOT NULL,
    owner_id uuid NOT NULL UNIQUE,
    journal_generation uuid NOT NULL UNIQUE,
    member_sha256 char(64) NOT NULL,
    contract_tickers text[] NOT NULL,
    subscription_document jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (selection_generation_id, subscription_id),
    UNIQUE (selection_generation_id),
    CHECK (provider_cluster = 'OPTIONS'),
    CHECK (cardinality(contract_tickers) > 0),
    CHECK (member_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(subscription_document) = 'object')
);

CREATE TABLE rt.advanced_options_physical_subscription_states (
    selection_generation_id uuid NOT NULL,
    subscription_id uuid NOT NULL,
    state text NOT NULL,
    message_count bigint NOT NULL,
    byte_count bigint NOT NULL,
    reconnect_count integer NOT NULL,
    gap_document jsonb NOT NULL,
    reason_codes text[] NOT NULL,
    observed_at timestamptz NOT NULL,
    PRIMARY KEY (selection_generation_id, subscription_id),
    FOREIGN KEY (selection_generation_id, subscription_id)
        REFERENCES rt.advanced_options_physical_subscriptions(
            selection_generation_id,
            subscription_id
        ),
    CHECK (
        state IN ('PLANNED','WARMING','READY','DEGRADED','FAILED')
    ),
    CHECK (message_count >= 0),
    CHECK (byte_count >= 0),
    CHECK (reconnect_count >= 0),
    CHECK (jsonb_typeof(gap_document) = 'array')
) WITH (fillfactor = 80);

CREATE TABLE rt.advanced_options_subscription_shards (
    selection_generation_id uuid NOT NULL,
    shard_id uuid NOT NULL,
    shard_slot integer NOT NULL,
    physical_subscription_id uuid NOT NULL,
    member_sha256 char(64) NOT NULL,
    contract_tickers text[] NOT NULL,
    shard_document jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (selection_generation_id, shard_id),
    UNIQUE (selection_generation_id, shard_slot),
    FOREIGN KEY (
        selection_generation_id,
        physical_subscription_id
    ) REFERENCES rt.advanced_options_physical_subscriptions (
        selection_generation_id,
        subscription_id
    ),
    CHECK (shard_slot > 0),
    CHECK (cardinality(contract_tickers) > 0),
    CHECK (member_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(shard_document) = 'object')
);

CREATE TABLE rt.advanced_options_shard_states (
    selection_generation_id uuid NOT NULL,
    shard_id uuid NOT NULL,
    state text NOT NULL,
    message_count bigint NOT NULL,
    byte_count bigint NOT NULL,
    reason_codes text[] NOT NULL,
    observed_at timestamptz NOT NULL,
    PRIMARY KEY (selection_generation_id, shard_id),
    FOREIGN KEY (selection_generation_id, shard_id)
        REFERENCES rt.advanced_options_subscription_shards(
            selection_generation_id,
            shard_id
        ),
    CHECK (
        state IN ('PLANNED','WARMING','READY','DEGRADED','FAILED')
    ),
    CHECK (message_count >= 0),
    CHECK (byte_count >= 0)
) WITH (fillfactor = 80);

CREATE TABLE rt.advanced_options_consumer_states (
    selection_generation_id uuid NOT NULL,
    shard_id uuid NOT NULL,
    consumer_name text NOT NULL,
    state_version text NOT NULL,
    committed_offset bigint NOT NULL,
    event_watermark_ns numeric(30,0),
    state_document jsonb NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (
        selection_generation_id,
        shard_id,
        consumer_name
    ),
    FOREIGN KEY (selection_generation_id, shard_id)
        REFERENCES rt.advanced_options_subscription_shards(
            selection_generation_id,
            shard_id
        ),
    CHECK (committed_offset >= 0),
    CHECK (
        event_watermark_ns IS NULL
        OR event_watermark_ns >= 0
    ),
    CHECK (jsonb_typeof(state_document) = 'object')
) WITH (fillfactor = 80);

CREATE TABLE rt.advanced_options_detector_routing_states (
    selection_generation_id uuid PRIMARY KEY
        REFERENCES rt.advanced_options_selection_generations(
            selection_generation_id
        ),
    allocation_sha256 char(64) NOT NULL,
    committed_event_offset bigint NOT NULL,
    state_sha256 char(64) NOT NULL,
    state_document jsonb NOT NULL,
    updated_at timestamptz NOT NULL,
    CHECK (allocation_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (committed_event_offset >= 0),
    CHECK (state_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(state_document) = 'object')
) WITH (fillfactor = 80);

CREATE TRIGGER trg_rt_advanced_options_selection_generations_immutable
    BEFORE UPDATE OR DELETE
    ON rt.advanced_options_selection_generations
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_selection_generations_no_truncate
    BEFORE TRUNCATE
    ON rt.advanced_options_selection_generations
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_selection_members_immutable
    BEFORE UPDATE OR DELETE
    ON rt.advanced_options_selection_members
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_selection_members_no_truncate
    BEFORE TRUNCATE
    ON rt.advanced_options_selection_members
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_selection_coverage_immutable
    BEFORE UPDATE OR DELETE
    ON rt.advanced_options_selection_coverage
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_selection_coverage_no_truncate
    BEFORE TRUNCATE
    ON rt.advanced_options_selection_coverage
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_physical_subscriptions_immutable
    BEFORE UPDATE OR DELETE
    ON rt.advanced_options_physical_subscriptions
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_physical_subscriptions_no_truncate
    BEFORE TRUNCATE
    ON rt.advanced_options_physical_subscriptions
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_subscription_shards_immutable
    BEFORE UPDATE OR DELETE
    ON rt.advanced_options_subscription_shards
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_subscription_shards_no_truncate
    BEFORE TRUNCATE
    ON rt.advanced_options_subscription_shards
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
