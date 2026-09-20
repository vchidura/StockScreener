CREATE TABLE IF NOT EXISTS public.option_alert_plans (
    plan_id uuid PRIMARY KEY,
    candidate_id uuid NOT NULL REFERENCES public.option_strategy_candidates(candidate_id),
    exposure_key text NOT NULL CHECK (exposure_key ~ '^[0-9a-f]{64}$'),
    plan_sha256 text NOT NULL UNIQUE CHECK (plan_sha256 ~ '^[0-9a-f]{64}$'),
    payload_text text NOT NULL,
    decision_at timestamptz NOT NULL,
    entry_deadline timestamptz NOT NULL,
    exit_deadline timestamptz NOT NULL,
    stored_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (isfinite(decision_at) AND isfinite(entry_deadline) AND isfinite(exit_deadline)),
    CHECK (decision_at < entry_deadline AND entry_deadline < exit_deadline),
    CHECK (plan_sha256 = encode(sha256(convert_to(payload_text, 'UTF8')), 'hex')),
    CHECK ((jsonb_typeof(payload_text::jsonb) = 'object'
        AND payload_text::jsonb->>'version' = 'option_alert_plan_v1'
        AND payload_text::jsonb->>'state' = 'UNPUBLISHED_INDICATIVE_PLAN'
        AND (payload_text::jsonb->>'candidate_id')::uuid = candidate_id
        AND (payload_text::jsonb->>'decision_at')::timestamptz = decision_at
        AND (payload_text::jsonb->>'entry_deadline')::timestamptz = entry_deadline
        AND (payload_text::jsonb->>'exit_deadline')::timestamptz = exit_deadline
        AND payload_text::jsonb->'execution_permission' = 'false'::jsonb
        AND payload_text::jsonb->'paper_position_created' = 'false'::jsonb
        AND payload_text::jsonb->'published_at' = 'null'::jsonb
        AND payload_text::jsonb->'fill' = 'null'::jsonb) IS TRUE)
);
CREATE INDEX IF NOT EXISTS option_alert_plans_exposure_idx ON public.option_alert_plans(exposure_key);

CREATE SEQUENCE IF NOT EXISTS public.option_alert_publication_sequence;
CREATE TABLE IF NOT EXISTS public.option_alert_publication_events (
    event_id uuid PRIMARY KEY,
    sequence bigint NOT NULL UNIQUE,
    plan_id uuid NOT NULL REFERENCES public.option_alert_plans(plan_id),
    event_type text NOT NULL CHECK (event_type IN ('PUBLISHED', 'OBSERVED', 'INVALIDATED', 'EXPIRED')),
    request_key text NOT NULL CHECK (length(btrim(request_key)) BETWEEN 1 AND 200),
    request_text text NOT NULL,
    recorded_at timestamptz NOT NULL,
    UNIQUE (plan_id, request_key),
    CHECK ((jsonb_typeof(request_text::jsonb) = 'object'
        AND request_text::jsonb->>'version' = 'option_alert_publication_v1'
        AND (request_text::jsonb->>'plan_id')::uuid = plan_id
        AND request_text::jsonb->>'event' = event_type
        AND request_text::jsonb->'execution_permission' = 'false'::jsonb) IS TRUE)
);
ALTER SEQUENCE public.option_alert_publication_sequence OWNED BY public.option_alert_publication_events.sequence;
CREATE INDEX IF NOT EXISTS option_alert_events_plan_idx ON public.option_alert_publication_events(plan_id, sequence DESC);
CREATE UNIQUE INDEX IF NOT EXISTS option_alert_one_publication_idx ON public.option_alert_publication_events(plan_id) WHERE event_type = 'PUBLISHED';
CREATE UNIQUE INDEX IF NOT EXISTS option_alert_one_terminal_idx ON public.option_alert_publication_events(plan_id) WHERE event_type IN ('INVALIDATED', 'EXPIRED');

CREATE OR REPLACE FUNCTION public.guard_option_alert_publication() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    parent public.option_alert_plans%ROWTYPE;
    previous_type text;
    source_time timestamptz;
BEGIN
    SELECT * INTO parent FROM public.option_alert_plans WHERE plan_id = NEW.plan_id;
    IF NOT FOUND THEN RAISE EXCEPTION 'option alert plan missing'; END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('option-alert:' || parent.exposure_key, 0));
    SELECT * INTO parent FROM public.option_alert_plans WHERE plan_id = NEW.plan_id FOR UPDATE;
    NEW.recorded_at := clock_timestamp();
    NEW.sequence := nextval('public.option_alert_publication_sequence');
    SELECT event_type INTO previous_type FROM public.option_alert_publication_events
        WHERE plan_id = NEW.plan_id ORDER BY sequence DESC LIMIT 1;
    IF NEW.recorded_at < parent.decision_at THEN RAISE EXCEPTION 'option alert event precedes decision'; END IF;
    IF previous_type IN ('INVALIDATED', 'EXPIRED') THEN RAISE EXCEPTION 'terminal option alert cannot reopen'; END IF;
    IF NEW.event_type = 'PUBLISHED' THEN
        IF previous_type IS NOT NULL OR NEW.recorded_at >= parent.entry_deadline THEN
            RAISE EXCEPTION 'option alert publication requires a new unexpired plan';
        END IF;
        IF EXISTS (
            SELECT 1 FROM public.option_alert_plans AS other
            JOIN LATERAL (
                SELECT event_type FROM public.option_alert_publication_events
                WHERE plan_id = other.plan_id ORDER BY sequence DESC LIMIT 1
            ) AS latest ON TRUE
            WHERE other.exposure_key = parent.exposure_key AND latest.event_type IN ('PUBLISHED', 'OBSERVED')
        ) THEN RAISE EXCEPTION 'active option exposure already published'; END IF;
    ELSE
        IF previous_type IS NULL THEN RAISE EXCEPTION 'option alert must be published before lifecycle events'; END IF;
        IF NEW.event_type = 'EXPIRED' THEN
            IF NEW.recorded_at < parent.entry_deadline THEN RAISE EXCEPTION 'option alert cannot expire early'; END IF;
        ELSIF NEW.recorded_at >= parent.entry_deadline THEN
            RAISE EXCEPTION 'expired entry window only accepts expiration';
        END IF;
    END IF;
    IF NEW.event_type IN ('OBSERVED', 'INVALIDATED') THEN
        IF NOT (jsonb_typeof(NEW.request_text::jsonb->'source_ids') = 'array'
                AND jsonb_array_length(NEW.request_text::jsonb->'source_ids') > 0) IS TRUE THEN
            RAISE EXCEPTION 'option alert event requires source IDs';
        END IF;
        source_time := (NEW.request_text::jsonb->>'source_available_at')::timestamptz;
        IF source_time IS NULL OR source_time < parent.decision_at OR source_time > NEW.recorded_at THEN
            RAISE EXCEPTION 'option alert event source is not causal';
        END IF;
    END IF;
    IF NEW.event_type = 'INVALIDATED' AND COALESCE(length(btrim(NEW.request_text::jsonb->>'reason')), 0) = 0 THEN
        RAISE EXCEPTION 'option alert invalidation requires a reason';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS option_alert_publication_guard ON public.option_alert_publication_events;
CREATE TRIGGER option_alert_publication_guard BEFORE INSERT ON public.option_alert_publication_events
    FOR EACH ROW EXECUTE FUNCTION public.guard_option_alert_publication();

CREATE OR REPLACE FUNCTION public.reject_option_alert_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'option alert plans and publication evidence are immutable';
END;
$$;
DROP TRIGGER IF EXISTS option_alert_plans_immutable ON public.option_alert_plans;
CREATE TRIGGER option_alert_plans_immutable BEFORE UPDATE OR DELETE ON public.option_alert_plans
    FOR EACH ROW EXECUTE FUNCTION public.reject_option_alert_mutation();
DROP TRIGGER IF EXISTS option_alert_events_immutable ON public.option_alert_publication_events;
CREATE TRIGGER option_alert_events_immutable BEFORE UPDATE OR DELETE ON public.option_alert_publication_events
    FOR EACH ROW EXECUTE FUNCTION public.reject_option_alert_mutation();
DROP TRIGGER IF EXISTS option_alert_plans_no_truncate ON public.option_alert_plans;
CREATE TRIGGER option_alert_plans_no_truncate BEFORE TRUNCATE ON public.option_alert_plans
    FOR EACH STATEMENT EXECUTE FUNCTION public.reject_option_alert_mutation();
DROP TRIGGER IF EXISTS option_alert_events_no_truncate ON public.option_alert_publication_events;
CREATE TRIGGER option_alert_events_no_truncate BEFORE TRUNCATE ON public.option_alert_publication_events
    FOR EACH STATEMENT EXECUTE FUNCTION public.reject_option_alert_mutation();