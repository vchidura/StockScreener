SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.advanced_journal_recovery_events (
    recovery_event_id uuid PRIMARY KEY,
    journal_generation uuid NOT NULL REFERENCES
        rt.advanced_journal_registry(journal_generation),
    plan_id uuid NOT NULL REFERENCES rt.advanced_ingress_plans(plan_id),
    event_type varchar(16) NOT NULL,
    recovery_contract varchar(64) NOT NULL,
    plan_report_sha256 char(64) NOT NULL,
    journal_files_sha256 char(64) NOT NULL,
    raw_committed_offset bigint NOT NULL,
    normalized_committed_offset bigint NOT NULL,
    bridge_committed_offset bigint NOT NULL,
    persisted_bars bigint NOT NULL,
    created_at timestamptz NOT NULL,
    UNIQUE (journal_generation, event_type),
    CHECK (
        event_type IN ('PREPARED', 'RECOVERED')
        AND recovery_contract = 'advanced-journal-root-recovery-v1'
        AND plan_report_sha256 ~ '^[0-9a-f]{64}$'
        AND journal_files_sha256 ~ '^[0-9a-f]{64}$'
        AND raw_committed_offset > 0
        AND normalized_committed_offset > 0
        AND normalized_committed_offset <= raw_committed_offset
        AND bridge_committed_offset >= 0
        AND bridge_committed_offset <= normalized_committed_offset
        AND persisted_bars >= 0
        AND (
            (
                event_type = 'PREPARED'
                AND bridge_committed_offset = 0
                AND persisted_bars = 0
            )
            OR
            (
                event_type = 'RECOVERED'
                AND bridge_committed_offset
                    = normalized_committed_offset
            )
        )
    )
);

CREATE INDEX idx_rt_advanced_journal_recovery_events_plan
    ON rt.advanced_journal_recovery_events (
        plan_id,
        event_type,
        created_at
    );

CREATE TRIGGER trg_rt_advanced_journal_recovery_events_immutable
    BEFORE UPDATE OR DELETE ON rt.advanced_journal_recovery_events
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_advanced_journal_recovery_events_no_truncate
    BEFORE TRUNCATE ON rt.advanced_journal_recovery_events
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
