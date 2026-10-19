SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.advanced_options_rolling_checkpoints (
    state_id uuid PRIMARY KEY,
    state_sha256 char(64) NOT NULL UNIQUE,
    state_version varchar(64) NOT NULL,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    session_date date NOT NULL,
    journal_generation uuid NOT NULL,
    owner_epoch bigint NOT NULL,
    committed_offset bigint NOT NULL,
    committed_at timestamptz NOT NULL,
    market_state_checkpoint_sha256 char(64) NOT NULL,
    flow_checkpoint_sha256 char(64) NOT NULL,
    state_document jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (
        cohort_id,
        session_date,
        journal_generation,
        owner_epoch,
        committed_offset
    ),
    CHECK (
        state_version = 'advanced-options-rolling-state-v1'
        AND state_sha256 ~ '^[0-9a-f]{64}$'
        AND owner_epoch > 0
        AND committed_offset >= 0
        AND market_state_checkpoint_sha256 ~ '^[0-9a-f]{64}$'
        AND flow_checkpoint_sha256 ~ '^[0-9a-f]{64}$'
        AND jsonb_typeof(state_document) = 'object'
    )
);

CREATE INDEX idx_rt_advanced_options_rolling_checkpoint_latest
    ON rt.advanced_options_rolling_checkpoints (
        cohort_id,
        session_date,
        journal_generation,
        owner_epoch DESC,
        committed_offset DESC
    );

CREATE TRIGGER trg_rt_advanced_options_rolling_checkpoints_immutable
    BEFORE UPDATE OR DELETE ON rt.advanced_options_rolling_checkpoints
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_advanced_options_rolling_checkpoints_no_truncate
    BEFORE TRUNCATE ON rt.advanced_options_rolling_checkpoints
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
