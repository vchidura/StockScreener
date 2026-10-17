SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE rt.stream_sessions
    ADD COLUMN journal_generation uuid,
    ADD COLUMN normalizer_version varchar(96),
    ADD COLUMN authorization_reference varchar(160),
    ADD COLUMN budget_scope_sha256 char(64),
    ADD COLUMN connection_attempt integer,
    ADD COLUMN connection_ceiling integer,
    ADD COLUMN decoded_byte_ceiling bigint,
    ADD COLUMN received_frame_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN received_byte_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN admitted_market_message_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN admitted_market_byte_count bigint NOT NULL DEFAULT 0,
    ADD COLUMN last_raw_offset bigint NOT NULL DEFAULT 0,
    ADD COLUMN last_event_offset bigint NOT NULL DEFAULT 0,
    ADD COLUMN event_time_watermark timestamptz,
    ADD COLUMN heartbeat_at timestamptz;

ALTER TABLE rt.stream_sessions
    ADD CONSTRAINT stream_sessions_advanced_scope_check CHECK (
        (
            journal_generation IS NULL
            AND normalizer_version IS NULL
            AND authorization_reference IS NULL
            AND budget_scope_sha256 IS NULL
            AND connection_attempt IS NULL
            AND connection_ceiling IS NULL
            AND decoded_byte_ceiling IS NULL
        )
        OR
        (
            journal_generation IS NOT NULL
            AND btrim(normalizer_version) <> ''
            AND btrim(authorization_reference) <> ''
            AND budget_scope_sha256 ~ '^[0-9a-f]{64}$'
            AND connection_attempt > 0
            AND connection_ceiling > 0
            AND connection_attempt <= connection_ceiling
            AND decoded_byte_ceiling > 0
        )
    ),
    ADD CONSTRAINT stream_sessions_advanced_usage_check CHECK (
        received_frame_count >= 0
        AND received_byte_count >= 0
        AND admitted_market_message_count >= 0
        AND admitted_market_byte_count >= 0
        AND admitted_market_message_count <= received_frame_count
        AND admitted_market_byte_count <= received_byte_count
        AND last_raw_offset >= 0
        AND last_event_offset >= 0
        AND (
            decoded_byte_ceiling IS NULL
            OR received_byte_count <= decoded_byte_ceiling
        )
    ),
    ADD CONSTRAINT stream_sessions_advanced_heartbeat_check CHECK (
        heartbeat_at IS NULL OR heartbeat_at >= started_at
    );

CREATE UNIQUE INDEX uq_rt_stream_journal_generation
    ON rt.stream_sessions (journal_generation)
    WHERE journal_generation IS NOT NULL;

CREATE INDEX idx_rt_stream_budget_scope
    ON rt.stream_sessions (
        budget_scope_sha256,
        started_at,
        session_id
    )
    WHERE budget_scope_sha256 IS NOT NULL;
