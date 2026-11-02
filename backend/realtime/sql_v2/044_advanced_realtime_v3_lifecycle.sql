SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.advanced_options_preparation_sessions (
    revision_id uuid PRIMARY KEY
        REFERENCES rt.realtime_desired_revisions(revision_id),
    session_date date NOT NULL UNIQUE,
    desired_snapshot_sha256 char(64),
    desired_cohort_id uuid REFERENCES rt.cohorts(cohort_id),
    status varchar(32) NOT NULL,
    current_stage varchar(32) NOT NULL,
    option_underlyings text[] NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    failure_streak integer NOT NULL DEFAULT 0,
    next_retry_at timestamptz NOT NULL,
    terminal_reason varchar(160),
    last_error varchar(500),
    checked_at timestamptz NOT NULL,
    activated_session_binding_id uuid
        REFERENCES rt.advanced_options_continuous_sessions(
            session_binding_id
        ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        status IN (
            'WAITING_DESIRED',
            'WAITING_EQUITY',
            'PREPARING',
            'READY',
            'ACTIVE',
            'DEGRADED',
            'FAILED'
        )
    ),
    CHECK (
        current_stage IN (
            'EQUITY_READINESS',
            'CATALOG_QUOTE',
            'REFERENCE_OI',
            'HISTORY',
            'CAPACITY_SHARDS',
            'SESSION_ACTIVATION',
            'COMPLETE'
        )
    ),
    CHECK (cardinality(option_underlyings) > 0),
    CHECK (attempts >= 0 AND failure_streak >= 0),
    CHECK (
        (status = 'FAILED') = (terminal_reason IS NOT NULL)
    ),
    CHECK (
        (status = 'ACTIVE')
        = (activated_session_binding_id IS NOT NULL)
    ),
    CHECK (
        desired_snapshot_sha256 IS NULL
        OR desired_snapshot_sha256 ~ '^[0-9a-f]{64}$'
    )
) WITH (fillfactor = 80);

CREATE INDEX idx_rt_advanced_options_preparation_due
    ON rt.advanced_options_preparation_sessions (
        next_retry_at,
        session_date,
        revision_id
    )
    WHERE status IN (
        'WAITING_DESIRED',
        'WAITING_EQUITY',
        'PREPARING',
        'DEGRADED'
    );

CREATE TABLE rt.advanced_options_preparation_work (
    revision_id uuid NOT NULL
        REFERENCES rt.advanced_options_preparation_sessions(revision_id),
    intent_id uuid NOT NULL
        REFERENCES rt.realtime_desired_member_intents(intent_id),
    ticker varchar(32) NOT NULL,
    stage varchar(32) NOT NULL,
    stage_order smallint NOT NULL,
    status varchar(16) NOT NULL,
    watermark jsonb NOT NULL DEFAULT '{}'::jsonb,
    evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    attempts integer NOT NULL DEFAULT 0,
    failure_streak integer NOT NULL DEFAULT 0,
    next_retry_at timestamptz NOT NULL,
    lease_owner uuid,
    lease_epoch bigint NOT NULL DEFAULT 0,
    lease_until timestamptz,
    terminal_reason varchar(160),
    blocked_reason varchar(160),
    provider_request_count bigint NOT NULL DEFAULT 0,
    response_bytes bigint NOT NULL DEFAULT 0,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (revision_id, intent_id, stage),
    UNIQUE (revision_id, ticker, stage),
    CHECK (ticker = upper(ticker) AND btrim(ticker) <> ''),
    CHECK (
        stage IN (
            'CATALOG_QUOTE',
            'REFERENCE_OI',
            'HISTORY',
            'CAPACITY_SHARDS',
            'SESSION_ACTIVATION'
        )
    ),
    CHECK (
        (stage = 'CATALOG_QUOTE' AND stage_order = 10)
        OR (stage = 'REFERENCE_OI' AND stage_order = 20)
        OR (stage = 'HISTORY' AND stage_order = 30)
        OR (stage = 'CAPACITY_SHARDS' AND stage_order = 40)
        OR (stage = 'SESSION_ACTIVATION' AND stage_order = 50)
    ),
    CHECK (
        status IN (
            'PENDING',
            'RUNNING',
            'RETRY',
            'COMPLETE',
            'BLOCKED',
            'FAILED'
        )
    ),
    CHECK (
        jsonb_typeof(watermark) = 'object'
        AND jsonb_typeof(evidence) = 'object'
    ),
    CHECK (
        attempts >= 0
        AND failure_streak >= 0
        AND lease_epoch >= 0
        AND provider_request_count >= 0
        AND response_bytes >= 0
    ),
    CHECK (
        (status = 'RUNNING')
        = (lease_owner IS NOT NULL AND lease_until IS NOT NULL)
    ),
    CHECK (
        status = 'RUNNING'
        OR (lease_owner IS NULL AND lease_until IS NULL)
    ),
    CHECK (
        (status = 'FAILED') = (terminal_reason IS NOT NULL)
    ),
    CHECK (
        (status = 'BLOCKED') = (blocked_reason IS NOT NULL)
    ),
    CHECK (
        (status IN ('COMPLETE', 'BLOCKED', 'FAILED'))
        = (completed_at IS NOT NULL)
    )
) WITH (fillfactor = 80);

CREATE INDEX idx_rt_advanced_options_preparation_work_due
    ON rt.advanced_options_preparation_work (
        next_retry_at,
        stage_order,
        revision_id,
        ticker
    )
    WHERE status IN ('PENDING', 'RETRY', 'RUNNING');

CREATE TABLE rt.advanced_options_preparation_evidence (
    revision_id uuid NOT NULL,
    intent_id uuid NOT NULL,
    stage varchar(32) NOT NULL,
    attempt integer NOT NULL,
    owner_id uuid NOT NULL,
    owner_epoch bigint NOT NULL,
    watermark_before jsonb NOT NULL,
    watermark_after jsonb NOT NULL,
    evidence jsonb NOT NULL,
    evidence_sha256 char(64) NOT NULL,
    provider_request_ids uuid[] NOT NULL DEFAULT ARRAY[]::uuid[],
    provider_request_count integer NOT NULL,
    response_bytes bigint NOT NULL,
    completed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (revision_id, intent_id, stage),
    UNIQUE (evidence_sha256),
    FOREIGN KEY (revision_id, intent_id, stage)
        REFERENCES rt.advanced_options_preparation_work(
            revision_id,
            intent_id,
            stage
        ),
    CHECK (attempt >= 1 AND owner_epoch >= 1),
    CHECK (
        jsonb_typeof(watermark_before) = 'object'
        AND jsonb_typeof(watermark_after) = 'object'
        AND jsonb_typeof(evidence) = 'object'
    ),
    CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (
        provider_request_count = cardinality(provider_request_ids)
        AND provider_request_count >= 0
        AND response_bytes >= 0
    )
);

CREATE TRIGGER trg_rt_advanced_options_preparation_evidence_immutable
    BEFORE UPDATE OR DELETE
    ON rt.advanced_options_preparation_evidence
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_preparation_evidence_no_truncate
    BEFORE TRUNCATE
    ON rt.advanced_options_preparation_evidence
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TABLE rt.advanced_realtime_service_states (
    role_name varchar(64) PRIMARY KEY,
    runtime_kind varchar(16) NOT NULL,
    owner_id uuid NOT NULL,
    owner_epoch bigint NOT NULL,
    process_identity varchar(200) NOT NULL,
    state varchar(24) NOT NULL,
    health varchar(24) NOT NULL,
    lease_until timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    detail jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (btrim(role_name) <> ''),
    CHECK (runtime_kind IN ('NATIVE', 'COMPOSE')),
    CHECK (owner_epoch >= 1),
    CHECK (btrim(process_identity) <> ''),
    CHECK (
        state IN (
            'STARTING',
            'RUNNING',
            'WAITING',
            'DEGRADED',
            'FAILED_TERMINAL',
            'DRAINING',
            'STOPPED'
        )
    ),
    CHECK (
        health IN (
            'STARTING',
            'HEALTHY',
            'DEGRADED',
            'UNHEALTHY',
            'STOPPED'
        )
    ),
    CHECK (lease_until >= observed_at),
    CHECK (jsonb_typeof(detail) = 'object')
) WITH (fillfactor = 80);
