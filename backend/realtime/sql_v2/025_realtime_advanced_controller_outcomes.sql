SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE rt.advanced_ingress_plans
    ADD COLUMN terminal_outcome varchar(40),
    ADD CONSTRAINT advanced_ingress_plan_terminal_outcome_check CHECK (
        terminal_outcome IS NULL
        OR terminal_outcome IN (
            'SESSION_COMPLETE',
            'AUTHORIZATION_EXHAUSTED',
            'BLOCKED_CONFIGURATION',
            'BLOCKED_ENTITLEMENT',
            'BLOCKED_GAP',
            'FAILED_INFRASTRUCTURE'
        )
    ),
    ADD CONSTRAINT advanced_ingress_plan_terminal_evidence_check CHECK (
        (
            status IN ('READY', 'ACTIVE')
            AND terminal_outcome IS NULL
            AND completed_at IS NULL
        )
        OR
        (
            status IN ('COMPLETE', 'BLOCKED', 'FAILED')
            AND terminal_outcome IS NOT NULL
            AND completed_at IS NOT NULL
        )
    );

CREATE FUNCTION rt.enforce_advanced_ingress_terminal_outcome()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    IF OLD.terminal_outcome IS NOT NULL AND (
        NEW.terminal_outcome,
        NEW.completed_at
    ) IS DISTINCT FROM (
        OLD.terminal_outcome,
        OLD.completed_at
    ) THEN
        RAISE EXCEPTION 'advanced ingress terminal evidence is immutable';
    END IF;
    IF OLD.terminal_outcome IS NULL
       AND NEW.terminal_outcome IS NOT NULL
       AND NEW.status NOT IN ('COMPLETE', 'BLOCKED', 'FAILED') THEN
        RAISE EXCEPTION 'terminal outcome requires a terminal plan status';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_ingress_plan_terminal_outcome
    BEFORE UPDATE ON rt.advanced_ingress_plans
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_ingress_terminal_outcome();
