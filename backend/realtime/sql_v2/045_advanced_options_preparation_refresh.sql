SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE rt.advanced_options_preparation_work
    ADD COLUMN retry_reason varchar(160);

ALTER TABLE rt.advanced_options_preparation_work
    ADD COLUMN expiration_state varchar(16),
    ADD COLUMN expired_at timestamptz,
    ADD COLUMN expiration_reason varchar(160);

ALTER TABLE rt.advanced_options_preparation_work
    ADD CONSTRAINT advanced_options_preparation_work_retry_reason_check
    CHECK (retry_reason IS NULL OR status = 'RETRY');

ALTER TABLE rt.advanced_options_preparation_work
    ADD CONSTRAINT advanced_options_preparation_work_expiration_check
    CHECK (
        (
            expiration_state IS NULL
            AND expired_at IS NULL
            AND expiration_reason IS NULL
        )
        OR (
            expiration_state IN ('EXPIRED', 'SUPERSEDED')
            AND expired_at IS NOT NULL
            AND btrim(expiration_reason) <> ''
            AND status = 'BLOCKED'
        )
    );

ALTER TABLE rt.advanced_options_preparation_sessions
    ADD COLUMN expiration_state varchar(16),
    ADD COLUMN expired_at timestamptz,
    ADD COLUMN expiration_reason varchar(160);

ALTER TABLE rt.advanced_options_preparation_sessions
    ADD CONSTRAINT advanced_options_preparation_sessions_expiration_check
    CHECK (
        (
            expiration_state IS NULL
            AND expired_at IS NULL
            AND expiration_reason IS NULL
        )
        OR (
            expiration_state IN ('EXPIRED', 'SUPERSEDED')
            AND expired_at IS NOT NULL
            AND btrim(expiration_reason) <> ''
            AND activated_session_binding_id IS NULL
        )
    );

CREATE TABLE rt.advanced_options_preparation_evidence_history (
    revision_id uuid NOT NULL,
    intent_id uuid NOT NULL,
    stage varchar(32) NOT NULL,
    attempt integer NOT NULL,
    owner_id uuid NOT NULL,
    owner_epoch bigint NOT NULL,
    watermark_before jsonb NOT NULL,
    watermark_after jsonb NOT NULL,
    evidence jsonb NOT NULL,
    evidence_sha256 char(64) NOT NULL UNIQUE,
    provider_request_ids uuid[] NOT NULL DEFAULT ARRAY[]::uuid[],
    provider_request_count integer NOT NULL,
    response_bytes bigint NOT NULL,
    completed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (revision_id, intent_id, stage, attempt),
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

INSERT INTO rt.advanced_options_preparation_evidence_history (
    revision_id,
    intent_id,
    stage,
    attempt,
    owner_id,
    owner_epoch,
    watermark_before,
    watermark_after,
    evidence,
    evidence_sha256,
    provider_request_ids,
    provider_request_count,
    response_bytes,
    completed_at,
    created_at
)
SELECT
    revision_id,
    intent_id,
    stage,
    attempt,
    owner_id,
    owner_epoch,
    watermark_before,
    watermark_after,
    evidence,
    evidence_sha256,
    provider_request_ids,
    provider_request_count,
    response_bytes,
    completed_at,
    created_at
FROM rt.advanced_options_preparation_evidence;

CREATE TRIGGER trg_rt_advanced_options_preparation_evidence_history_immutable
    BEFORE UPDATE OR DELETE
    ON rt.advanced_options_preparation_evidence_history
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_preparation_evidence_history_no_truncate
    BEFORE TRUNCATE
    ON rt.advanced_options_preparation_evidence_history
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TABLE rt.advanced_options_preparation_refresh_requests (
    request_id uuid PRIMARY KEY,
    revision_id uuid NOT NULL,
    source_intent_id uuid NOT NULL,
    source_stage varchar(32) NOT NULL,
    source_attempt integer NOT NULL,
    affected_underlyings text[] NOT NULL,
    reason_code varchar(160) NOT NULL,
    request_document jsonb NOT NULL,
    request_sha256 char(64) NOT NULL UNIQUE,
    requested_at timestamptz NOT NULL,
    retry_at timestamptz NOT NULL,
    backoff_seconds integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (revision_id, source_intent_id, source_attempt),
    FOREIGN KEY (revision_id, source_intent_id, source_stage)
        REFERENCES rt.advanced_options_preparation_work(
            revision_id,
            intent_id,
            stage
        ),
    CHECK (
        source_stage = 'CAPACITY_SHARDS'
        AND source_attempt >= 1
        AND cardinality(affected_underlyings) > 0
    ),
    CHECK (
        btrim(reason_code) <> ''
        AND jsonb_typeof(request_document) = 'object'
        AND request_sha256 ~ '^[0-9a-f]{64}$'
        AND retry_at > requested_at
        AND backoff_seconds BETWEEN 30 AND 300
    )
);

CREATE TRIGGER trg_rt_advanced_options_preparation_refresh_requests_immutable
    BEFORE UPDATE OR DELETE
    ON rt.advanced_options_preparation_refresh_requests
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_preparation_refresh_requests_no_truncate
    BEFORE TRUNCATE
    ON rt.advanced_options_preparation_refresh_requests
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
