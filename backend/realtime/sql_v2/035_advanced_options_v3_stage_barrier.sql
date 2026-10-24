SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE rt.stage_catalog
    ADD COLUMN external_prerequisites text[] NOT NULL
        DEFAULT ARRAY[]::text[],
    ADD CONSTRAINT stage_catalog_external_prerequisites_check
        CHECK (
            external_prerequisites <@ ARRAY[
                'TARGET_WINDOW_E_BINDING',
                'OPTIONS_RESIDENT_COMPLETION'
            ]::text[]
        );

INSERT INTO rt.stage_catalog (
    stage_id,
    version,
    domain,
    prerequisites,
    execution_lane,
    schedule_kind,
    gates_strategy,
    deadline_offset_seconds,
    external_prerequisites
) VALUES
    ('REFERENCE','stages_v5','shared',ARRAY[]::text[],'data','INTERVAL',false,NULL,ARRAY[]::text[]),
    ('EQUITY_INPUT_GATE','stages_v5','equity',ARRAY['REFERENCE'],'data','WINDOW',false,120,ARRAY[]::text[]),
    ('EQUITY_MATERIALIZE','stages_v5','equity',ARRAY['EQUITY_INPUT_GATE'],'data','WINDOW',false,120,ARRAY[]::text[]),
    ('STOCK_ALERTS','stages_v5','equity',ARRAY['EQUITY_MATERIALIZE'],'alert','WINDOW',false,120,ARRAY[]::text[]),
    ('EQUITY_PROJECTION','stages_v5','equity',ARRAY['EQUITY_MATERIALIZE'],'portal','COALESCED',false,300,ARRAY[]::text[]),
    ('PORTAL_PUBLISH','stages_v5','equity',ARRAY['EQUITY_PROJECTION'],'portal','COALESCED',false,300,ARRAY[]::text[]),
    ('STRATEGY_PUBLISH','stages_v5','equity',ARRAY['EQUITY_MATERIALIZE'],'portal','COALESCED',false,300,ARRAY[]::text[]),
    ('OPTIONS_ACQUIRE','stages_v5','options',ARRAY['REFERENCE'],'options','SESSION',false,NULL,ARRAY[]::text[]),
    ('OPTIONS_ADMISSION','stages_v5','options',ARRAY['OPTIONS_ACQUIRE'],'options','SESSION',false,NULL,ARRAY[]::text[]),
    ('OPTIONS_REFERENCE','stages_v5','options',ARRAY['OPTIONS_ACQUIRE'],'options','SESSION',false,NULL,ARRAY[]::text[]),
    ('OPTIONS_FLOW','stages_v5','options',ARRAY['OPTIONS_ADMISSION','OPTIONS_REFERENCE'],'options','CONTINUOUS',false,NULL,ARRAY[]::text[]),
    ('OPTIONS_INPUT_GATE','stages_v5','options',ARRAY['OPTIONS_REFERENCE'],'options','WINDOW',false,NULL,ARRAY['TARGET_WINDOW_E_BINDING','OPTIONS_RESIDENT_COMPLETION']),
    ('OPTIONS_CYCLE','stages_v5','options',ARRAY['OPTIONS_INPUT_GATE'],'options','WINDOW',true,NULL,ARRAY[]::text[]),
    ('OPTIONS_PROJECTION','stages_v5','options',ARRAY['OPTIONS_CYCLE'],'options','COALESCED',false,NULL,ARRAY[]::text[]),
    ('OPTIONS_PUBLISH','stages_v5','options',ARRAY['OPTIONS_PROJECTION'],'options','COALESCED',false,NULL,ARRAY[]::text[]);

ALTER TABLE rt.stage_runs
    ADD COLUMN retry_not_before timestamptz,
    ADD COLUMN original_deadline timestamptz,
    ADD COLUMN maximum_attempts integer,
    DROP CONSTRAINT stage_runs_state_check,
    ADD CONSTRAINT stage_runs_state_check
        CHECK (
            state IN (
                'PENDING',
                'RUNNING',
                'COMPLETE',
                'FAILED',
                'BLOCKED_PREREQ',
                'BLOCKED_POLICY',
                'MISSED',
                'DEFERRED'
            )
        ),
    ADD CONSTRAINT stage_runs_deferred_contract_check
        CHECK (
            state <> 'DEFERRED'
            OR (
                cardinality(reason_codes) > 0
                AND retry_not_before IS NOT NULL
                AND original_deadline IS NOT NULL
                AND maximum_attempts IS NOT NULL
                AND maximum_attempts >= attempt
                AND finished_at IS NOT NULL
                AND retry_not_before <= original_deadline
            )
        ),
    ADD CONSTRAINT stage_runs_retry_bounds_check
        CHECK (
            maximum_attempts IS NULL
            OR maximum_attempts >= attempt
        );

ALTER TABLE rt.stage_readiness_current
    DROP CONSTRAINT stage_readiness_current_status_check,
    ADD CONSTRAINT stage_readiness_current_status_check
        CHECK (
            status IN (
                'READY',
                'DEGRADED',
                'BLOCKED',
                'WAITING_INPUT'
            )
        );

CREATE TABLE rt.advanced_options_resident_completions (
    target_window_id uuid NOT NULL REFERENCES rt.window_runs(window_id)
        DEFERRABLE INITIALLY DEFERRED,
    target_cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    member varchar(16) NOT NULL,
    journal_generation uuid NOT NULL,
    owner_epoch bigint NOT NULL,
    ending_offset bigint NOT NULL,
    evidence_sha256 char(64) NOT NULL,
    evidence_document jsonb NOT NULL,
    status varchar(16) NOT NULL,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    completed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (target_window_id, member),
    CHECK (member IN ('INGRESS', 'MARKET_STATE', 'FLOW')),
    CHECK (owner_epoch > 0),
    CHECK (ending_offset >= 0),
    CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(evidence_document) = 'object'),
    CHECK (status IN ('COMPLETE', 'FAILED')),
    CHECK (
        (status = 'COMPLETE' AND cardinality(reason_codes) = 0)
        OR
        (status = 'FAILED' AND cardinality(reason_codes) > 0)
    )
);

CREATE INDEX idx_rt_advanced_options_resident_completion_scope
    ON rt.advanced_options_resident_completions (
        target_cohort_id,
        target_window_id,
        journal_generation
    );

CREATE TRIGGER trg_rt_advanced_options_resident_completions_immutable
    BEFORE UPDATE OR DELETE
    ON rt.advanced_options_resident_completions
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_advanced_options_resident_completions_no_truncate
    BEFORE TRUNCATE
    ON rt.advanced_options_resident_completions
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE FUNCTION rt.validate_advanced_options_window_cycle_receipt()
RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'rt'
    AS $$
DECLARE
    required_session_authorization_id uuid;
BEGIN
    IF NEW.authority_kind <> 'WINDOW_CYCLE' THEN
        RETURN NEW;
    END IF;
    SELECT session_authorization_id
    INTO required_session_authorization_id
    FROM rt.advanced_options_window_cycle_authorizations
    WHERE authorization_id = NEW.window_cycle_authorization_id;
    IF required_session_authorization_id IS NULL
       OR NOT EXISTS (
            SELECT 1
            FROM rt.advanced_options_authority_receipts
            WHERE session_authorization_id = required_session_authorization_id
              AND authority_kind = 'SESSION_ACQUISITION'
       ) THEN
        RAISE EXCEPTION
            'window cycle authority requires a consumed session acquisition authority'
            USING ERRCODE = 'RT002';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_rt_advanced_options_window_cycle_receipt_requires_session
    BEFORE INSERT
    ON rt.advanced_options_authority_receipts
    FOR EACH ROW EXECUTE FUNCTION
        rt.validate_advanced_options_window_cycle_receipt();

CREATE FUNCTION rt.validate_advanced_options_v3_stage_effect()
RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'rt'
    AS $$
DECLARE
    required_journal_generation uuid;
BEGIN
    IF NEW.stage_version = 'stages_v5'
       AND NEW.stage_id IN (
            'OPTIONS_INPUT_GATE',
            'OPTIONS_CYCLE',
            'OPTIONS_PROJECTION',
            'OPTIONS_PUBLISH'
       )
       AND NOT EXISTS (
            SELECT 1
            FROM rt.advanced_options_window_cycle_authorizations AS authority
            JOIN rt.advanced_options_authority_receipts AS receipt
              ON receipt.window_cycle_authorization_id =
                 authority.authorization_id
            WHERE authority.target_window_id = NEW.window_id
              AND receipt.authority_kind = 'WINDOW_CYCLE'
       ) THEN
        RAISE EXCEPTION
            'stages_v5 target-window effect requires a consumed cycle authority'
            USING ERRCODE = 'RT003';
    END IF;
    IF NEW.stage_version = 'stages_v5'
       AND NEW.stage_id = 'OPTIONS_INPUT_GATE' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM rt.advanced_options_equity_input_bindings
            WHERE target_options_window_id = NEW.window_id
        ) THEN
            RAISE EXCEPTION
                'stages_v5 input gate requires the target-window Equity binding'
                USING ERRCODE = 'RT004';
        END IF;
        SELECT session.journal_generation
        INTO required_journal_generation
        FROM rt.advanced_options_window_cycle_authorizations AS cycle
        JOIN rt.advanced_options_session_acquisition_authorizations AS session
          ON session.authorization_id = cycle.session_authorization_id
        WHERE cycle.target_window_id = NEW.window_id;
        IF required_journal_generation IS NULL
           OR (
                SELECT count(*)
                FROM rt.advanced_options_resident_completions
                WHERE target_window_id = NEW.window_id
                  AND journal_generation = required_journal_generation
                  AND status = 'COMPLETE'
           ) <> 3
           OR EXISTS (
                SELECT 1
                FROM rt.advanced_options_resident_completions
                WHERE target_window_id = NEW.window_id
                  AND (
                    journal_generation <> required_journal_generation
                    OR status = 'FAILED'
                  )
           ) THEN
            RAISE EXCEPTION
                'stages_v5 input gate requires exact resident completion'
                USING ERRCODE = 'RT005';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_rt_advanced_options_v3_stage_effect_authorized
    BEFORE INSERT
    ON rt.stage_runs
    FOR EACH ROW EXECUTE FUNCTION
        rt.validate_advanced_options_v3_stage_effect();
