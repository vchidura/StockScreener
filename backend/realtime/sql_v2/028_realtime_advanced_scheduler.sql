SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.advanced_scheduler_sessions (
    scheduler_session_id uuid PRIMARY KEY,
    authorization_id uuid NOT NULL REFERENCES
        rt.advanced_stream_authorizations(authorization_id),
    ingress_plan_id uuid REFERENCES rt.advanced_ingress_plans(plan_id),
    cohort_id uuid REFERENCES rt.cohorts(cohort_id),
    session_date date NOT NULL,
    first_authorized_cutoff timestamptz NOT NULL,
    session_close_at timestamptz NOT NULL,
    drain_deadline timestamptz NOT NULL,
    status varchar(16) NOT NULL,
    reason_code varchar(64) NOT NULL,
    next_action varchar(160) NOT NULL,
    last_discovered_cutoff timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    UNIQUE (authorization_id, session_date),
    CHECK (
        first_authorized_cutoff <= session_close_at
        AND drain_deadline >= session_close_at
        AND btrim(reason_code) <> ''
        AND btrim(next_action) <> ''
        AND status IN ('ACTIVE', 'DRAINING', 'IDLE', 'BLOCKED')
        AND (
            completed_at IS NULL
            OR status IN ('IDLE', 'BLOCKED')
        )
    )
);

CREATE TABLE rt.advanced_window_retries (
    retry_id uuid PRIMARY KEY,
    scheduler_session_id uuid NOT NULL REFERENCES
        rt.advanced_scheduler_sessions(scheduler_session_id),
    cutoff timestamptz NOT NULL,
    attempt integer NOT NULL,
    window_key varchar(64) NOT NULL,
    window_id uuid,
    retry_not_before timestamptz NOT NULL,
    status varchar(16) NOT NULL,
    reason_code varchar(64) NOT NULL,
    reused_input_receipt boolean NOT NULL DEFAULT false,
    reused_portal_receipt boolean NOT NULL DEFAULT false,
    report_relative_path varchar(320) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    UNIQUE (scheduler_session_id, cutoff, attempt),
    UNIQUE (scheduler_session_id, window_key),
    UNIQUE (scheduler_session_id, report_relative_path),
    CHECK (
        attempt >= 0
        AND btrim(window_key) <> ''
        AND btrim(reason_code) <> ''
        AND btrim(report_relative_path) <> ''
        AND left(report_relative_path, 1) NOT IN ('/', '\')
        AND report_relative_path !~ '(^|[\\/])\.\.([\\/]|$)'
        AND status IN (
            'PLANNED',
            'RUNNING',
            'COMPLETE',
            'FAILED',
            'BLOCKED',
            'SKIPPED'
        )
        AND (
            completed_at IS NULL
            OR status IN ('COMPLETE', 'FAILED', 'BLOCKED', 'SKIPPED')
        )
    )
);

CREATE INDEX idx_rt_advanced_window_retry_due
    ON rt.advanced_window_retries (
        status,
        retry_not_before,
        cutoff,
        attempt
    )
    WHERE status IN ('PLANNED', 'FAILED', 'BLOCKED');

CREATE FUNCTION rt.enforce_advanced_scheduler_session_change()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'advanced scheduler sessions are durable evidence';
    END IF;
    IF (
        NEW.scheduler_session_id,
        NEW.authorization_id,
        NEW.session_date,
        NEW.first_authorized_cutoff,
        NEW.session_close_at,
        NEW.drain_deadline,
        NEW.created_at
    ) IS DISTINCT FROM (
        OLD.scheduler_session_id,
        OLD.authorization_id,
        OLD.session_date,
        OLD.first_authorized_cutoff,
        OLD.session_close_at,
        OLD.drain_deadline,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'advanced scheduler session identity is immutable';
    END IF;
    IF NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'advanced scheduler clock cannot move backward';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_scheduler_sessions_durable
    BEFORE UPDATE OR DELETE ON rt.advanced_scheduler_sessions
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_scheduler_session_change();

CREATE FUNCTION rt.enforce_advanced_window_retry_change()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'advanced window retries are durable evidence';
    END IF;
    IF (
        NEW.retry_id,
        NEW.scheduler_session_id,
        NEW.cutoff,
        NEW.attempt,
        NEW.window_key,
        NEW.retry_not_before,
        NEW.report_relative_path,
        NEW.created_at
    ) IS DISTINCT FROM (
        OLD.retry_id,
        OLD.scheduler_session_id,
        OLD.cutoff,
        OLD.attempt,
        OLD.window_key,
        OLD.retry_not_before,
        OLD.report_relative_path,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'advanced window retry identity is immutable';
    END IF;
    IF NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'advanced window retry clock cannot move backward';
    END IF;
    IF OLD.status IN ('COMPLETE','SKIPPED')
       AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'terminal successful retry evidence is immutable';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_window_retries_durable
    BEFORE UPDATE OR DELETE ON rt.advanced_window_retries
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_window_retry_change();
