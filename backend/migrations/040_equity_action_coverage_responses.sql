ALTER TABLE public.equity_corporate_action_coverage
    ADD COLUMN IF NOT EXISTS security_id uuid,
    ADD COLUMN IF NOT EXISTS response_action_count integer,
    ADD COLUMN IF NOT EXISTS response_sha256 character(64);

ALTER TABLE public.equity_corporate_action_coverage
    DROP CONSTRAINT IF EXISTS ck_equity_action_coverage_response,
    ADD CONSTRAINT ck_equity_action_coverage_response CHECK (
        (security_id IS NULL AND response_action_count IS NULL AND response_sha256 IS NULL)
        OR (security_id IS NOT NULL AND response_action_count IS NOT NULL
            AND response_action_count >= 0 AND response_sha256 IS NOT NULL
            AND response_sha256 ~ '^[0-9a-f]{64}$')
    );

CREATE OR REPLACE FUNCTION public.guard_equity_action_coverage_response() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.response_action_count IS NOT NULL
       OR (TG_OP = 'UPDATE' AND NEW.response_action_count IS NOT NULL) THEN
        RAISE EXCEPTION 'response-bound action coverage is immutable; insert a new observation';
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_guard_equity_action_coverage_response ON public.equity_corporate_action_coverage;
CREATE TRIGGER trg_guard_equity_action_coverage_response
    BEFORE UPDATE OR DELETE ON public.equity_corporate_action_coverage
    FOR EACH ROW EXECUTE FUNCTION public.guard_equity_action_coverage_response();

CREATE OR REPLACE FUNCTION public.guard_equity_action_response_member() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.equity_corporate_action_coverage
               WHERE response_action_count IS NOT NULL
                 AND (coverage_id = OLD.coverage_id OR (TG_OP = 'UPDATE' AND coverage_id = NEW.coverage_id))) THEN
        RAISE EXCEPTION 'response-bound action membership is immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_guard_equity_action_response_member ON public.equity_corporate_action_coverage_members;
CREATE TRIGGER trg_guard_equity_action_response_member
    BEFORE UPDATE OR DELETE ON public.equity_corporate_action_coverage_members
    FOR EACH ROW EXECUTE FUNCTION public.guard_equity_action_response_member();

CREATE OR REPLACE FUNCTION public.validate_equity_action_coverage_response() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE coverage public.equity_corporate_action_coverage%ROWTYPE;
        linked_count bigint; matching_count bigint;
BEGIN
    SELECT * INTO coverage FROM public.equity_corporate_action_coverage
    WHERE coverage_id = COALESCE(NEW.coverage_id, OLD.coverage_id);
    IF coverage.response_action_count IS NOT NULL THEN
        SELECT COUNT(*), COUNT(*) FILTER (
            WHERE action.security_id = coverage.security_id AND action.ticker = coverage.ticker
              AND action.action_type = coverage.action_type
              AND action.effective_date BETWEEN coverage.window_start AND coverage.window_end
        ) INTO linked_count, matching_count
        FROM public.equity_corporate_action_coverage_members AS member
        LEFT JOIN public.equity_corporate_actions AS action USING (corporate_action_id)
        WHERE member.coverage_id = coverage.coverage_id;
        IF linked_count <> coverage.response_action_count OR matching_count <> linked_count THEN
            RAISE EXCEPTION 'action coverage response members are incomplete or mismatched';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_validate_equity_action_coverage_response ON public.equity_corporate_action_coverage;
CREATE CONSTRAINT TRIGGER trg_validate_equity_action_coverage_response
    AFTER INSERT OR UPDATE ON public.equity_corporate_action_coverage DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION public.validate_equity_action_coverage_response();
DROP TRIGGER IF EXISTS trg_validate_equity_action_response_members ON public.equity_corporate_action_coverage_members;
CREATE CONSTRAINT TRIGGER trg_validate_equity_action_response_members
    AFTER INSERT OR UPDATE OR DELETE ON public.equity_corporate_action_coverage_members DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION public.validate_equity_action_coverage_response();

CREATE OR REPLACE FUNCTION public.guard_equity_response_bound_action() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.equity_corporate_action_coverage_members AS member
               JOIN public.equity_corporate_action_coverage AS coverage USING (coverage_id)
               WHERE member.corporate_action_id = OLD.corporate_action_id
                 AND coverage.response_action_count IS NOT NULL) THEN
        RAISE EXCEPTION 'response-bound corporate action is immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_guard_equity_response_bound_action ON public.equity_corporate_actions;
CREATE TRIGGER trg_guard_equity_response_bound_action
    BEFORE UPDATE OR DELETE ON public.equity_corporate_actions
    FOR EACH ROW EXECUTE FUNCTION public.guard_equity_response_bound_action();