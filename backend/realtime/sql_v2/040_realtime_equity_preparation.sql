SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.realtime_desired_member_intents (
    intent_id uuid PRIMARY KEY,
    revision_id uuid NOT NULL REFERENCES rt.realtime_desired_revisions(revision_id),
    ticker varchar(32) NOT NULL,
    member_kind varchar(24) NOT NULL,
    priority smallint NOT NULL,
    enabled boolean NOT NULL,
    requested_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (revision_id, ticker, member_kind),
    CHECK (ticker = upper(ticker) AND btrim(ticker) <> ''),
    CHECK (member_kind IN ('EQUITY', 'SECTOR_ETF', 'OPTION_UNDERLYING')),
    CHECK (priority BETWEEN 1 AND 32767)
);

CREATE TABLE rt.realtime_desired_intent_readiness (
    intent_id uuid PRIMARY KEY
        REFERENCES rt.realtime_desired_member_intents(intent_id),
    status varchar(16) NOT NULL,
    reasons text[] NOT NULL DEFAULT ARRAY[]::text[],
    checked_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (status IN ('WARMING', 'READY', 'FAILED')),
    CHECK (
        (status = 'READY' AND cardinality(reasons) = 0)
        OR status = 'WARMING'
        OR (status = 'FAILED' AND cardinality(reasons) > 0)
    )
);

CREATE TABLE rt.realtime_desired_member_bindings (
    intent_id uuid PRIMARY KEY
        REFERENCES rt.realtime_desired_member_intents(intent_id),
    revision_id uuid NOT NULL REFERENCES rt.realtime_desired_revisions(revision_id),
    security_id uuid NOT NULL REFERENCES ref.securities(security_id),
    security_revision_id uuid NOT NULL
        REFERENCES ref.security_revisions(revision_id),
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    identity_sha256 char(64) NOT NULL UNIQUE,
    source_lineage jsonb NOT NULL,
    resolved_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (identity_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(source_lineage) = 'object')
);

CREATE TABLE rt.realtime_equity_preparation_work (
    revision_id uuid NOT NULL,
    intent_id uuid NOT NULL
        REFERENCES rt.realtime_desired_member_intents(intent_id),
    security_id uuid REFERENCES ref.securities(security_id),
    instrument_id bigint REFERENCES rt.instruments(instrument_id),
    member_kind varchar(24) NOT NULL,
    ticker varchar(32) NOT NULL,
    stage varchar(32) NOT NULL,
    stage_order smallint NOT NULL,
    status varchar(16) NOT NULL,
    target jsonb NOT NULL,
    watermark jsonb NOT NULL DEFAULT '{}'::jsonb,
    coverage jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_lineage jsonb NOT NULL DEFAULT '{}'::jsonb,
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
    PRIMARY KEY (
        revision_id,
        intent_id,
        stage
    ),
    UNIQUE (revision_id, ticker, member_kind, stage),
    CHECK (ticker = upper(ticker) AND btrim(ticker) <> ''),
    CHECK (member_kind IN ('EQUITY', 'SECTOR_ETF')),
    CHECK (
        stage IN (
            'REFERENCE',
            'ADJUSTED_DAILY',
            'NATIVE_INTRADAY',
            'DERIVED_INTERVALS',
            'TECHNICAL_WARMUP'
        )
    ),
    CHECK (
        (stage = 'REFERENCE' AND stage_order = 10)
        OR (stage = 'ADJUSTED_DAILY' AND stage_order = 20)
        OR (stage = 'NATIVE_INTRADAY' AND stage_order = 30)
        OR (stage = 'DERIVED_INTERVALS' AND stage_order = 40)
        OR (stage = 'TECHNICAL_WARMUP' AND stage_order = 50)
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
        jsonb_typeof(target) = 'object'
        AND jsonb_typeof(watermark) = 'object'
        AND jsonb_typeof(coverage) = 'object'
        AND jsonb_typeof(source_lineage) = 'object'
    ),
    CHECK (
        attempts >= 0
        AND failure_streak >= 0
        AND lease_epoch >= 0
        AND provider_request_count >= 0
        AND response_bytes >= 0
    ),
    CHECK (
        (security_id IS NULL AND instrument_id IS NULL)
        OR (security_id IS NOT NULL AND instrument_id IS NOT NULL)
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
        (status IN ('COMPLETE', 'FAILED'))
        = (terminal_reason IS NOT NULL)
    ),
    CHECK (
        (status = 'BLOCKED')
        = (blocked_reason IS NOT NULL)
    ),
    CHECK (
        (status IN ('COMPLETE', 'BLOCKED', 'FAILED'))
        = (completed_at IS NOT NULL)
    )
) WITH (fillfactor = 80);

CREATE INDEX idx_rt_equity_preparation_due
    ON rt.realtime_equity_preparation_work (
        next_retry_at,
        stage_order,
        revision_id,
        ticker
    )
    WHERE status IN ('PENDING', 'RETRY', 'RUNNING');

CREATE INDEX idx_rt_equity_preparation_member
    ON rt.realtime_equity_preparation_work (
        revision_id,
        intent_id,
        ticker,
        member_kind,
        stage_order
    );

CREATE TABLE rt.realtime_equity_preparation_chunks (
    chunk_id uuid PRIMARY KEY,
    chunk_sha256 char(64) NOT NULL UNIQUE,
    revision_id uuid NOT NULL,
    intent_id uuid NOT NULL,
    security_id uuid,
    member_kind varchar(24) NOT NULL,
    stage varchar(32) NOT NULL,
    attempt integer NOT NULL,
    watermark_before jsonb NOT NULL,
    watermark_after jsonb NOT NULL,
    coverage_after jsonb NOT NULL,
    source_lineage jsonb NOT NULL,
    provider_requests integer NOT NULL,
    response_bytes bigint NOT NULL,
    fact_count integer NOT NULL,
    committed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (
        revision_id,
        intent_id,
        stage
    ) REFERENCES rt.realtime_equity_preparation_work(
        revision_id,
        intent_id,
        stage
    ),
    CHECK (chunk_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (member_kind IN ('EQUITY', 'SECTOR_ETF')),
    CHECK (attempt >= 1),
    CHECK (
        jsonb_typeof(watermark_before) = 'object'
        AND jsonb_typeof(watermark_after) = 'object'
        AND jsonb_typeof(coverage_after) = 'object'
        AND jsonb_typeof(source_lineage) = 'object'
    ),
    CHECK (
        provider_requests >= 0
        AND response_bytes >= 0
        AND fact_count >= 0
    )
);

CREATE INDEX idx_rt_equity_preparation_chunk_member
    ON rt.realtime_equity_preparation_chunks (
        revision_id,
        intent_id,
        member_kind,
        stage,
        committed_at
    );

CREATE TRIGGER trg_rt_equity_preparation_chunks_immutable
    BEFORE UPDATE OR DELETE ON rt.realtime_equity_preparation_chunks
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_equity_preparation_chunks_no_truncate
    BEFORE TRUNCATE ON rt.realtime_equity_preparation_chunks
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_desired_member_intents_immutable
    BEFORE UPDATE OR DELETE ON rt.realtime_desired_member_intents
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_desired_member_intents_no_truncate
    BEFORE TRUNCATE ON rt.realtime_desired_member_intents
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_desired_member_bindings_immutable
    BEFORE UPDATE OR DELETE ON rt.realtime_desired_member_bindings
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_desired_member_bindings_no_truncate
    BEFORE TRUNCATE ON rt.realtime_desired_member_bindings
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
