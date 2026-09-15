ALTER TABLE public.equity_universe_runs
    ADD COLUMN IF NOT EXISTS supersedes_universe_run_id uuid,
    ADD COLUMN IF NOT EXISTS revision_published_at timestamp with time zone,
    ADD COLUMN IF NOT EXISTS revision_sha256 character(64),
    ADD COLUMN IF NOT EXISTS revision_reason text;

ALTER TABLE public.equity_universe_runs
    DROP CONSTRAINT IF EXISTS ck_equity_universe_revision,
    ADD CONSTRAINT ck_equity_universe_revision CHECK (
        (supersedes_universe_run_id IS NULL AND revision_published_at IS NULL
         AND revision_sha256 IS NULL AND revision_reason IS NULL)
        OR
        (supersedes_universe_run_id IS NOT NULL AND supersedes_universe_run_id <> universe_run_id
         AND revision_published_at IS NOT NULL AND revision_published_at >= observed_at
         AND revision_sha256 IS NOT NULL AND revision_sha256 ~ '^[0-9a-f]{64}$'
         AND revision_reason IS NOT NULL AND length(trim(revision_reason)) > 0
         AND availability_mode = 'HISTORICAL_RECONSTRUCTED' AND mode = 'REPLAY'
         AND status = 'COMPLETE' AND expected_members = admitted_members)
    ),
    DROP CONSTRAINT IF EXISTS fk_equity_universe_revision_parent,
    ADD CONSTRAINT fk_equity_universe_revision_parent FOREIGN KEY (supersedes_universe_run_id)
        REFERENCES public.equity_universe_runs (universe_run_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_equity_universe_revision_successor
    ON public.equity_universe_runs (supersedes_universe_run_id)
    WHERE supersedes_universe_run_id IS NOT NULL;

CREATE OR REPLACE FUNCTION public.guard_equity_universe_revision() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE parent public.equity_universe_runs%ROWTYPE;
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.supersedes_universe_run_id IS DISTINCT FROM OLD.supersedes_universe_run_id THEN
        RAISE EXCEPTION 'universe revisions must be inserted, never converted in place';
    END IF;
    IF TG_OP <> 'INSERT' THEN
        IF OLD.supersedes_universe_run_id IS NOT NULL OR EXISTS (
            SELECT 1 FROM public.equity_universe_runs WHERE supersedes_universe_run_id = OLD.universe_run_id
        ) THEN
            RAISE EXCEPTION 'published universe revision lineage is immutable';
        END IF;
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    END IF;
    IF NEW.supersedes_universe_run_id IS NOT NULL THEN
        SELECT * INTO STRICT parent FROM public.equity_universe_runs
        WHERE universe_run_id = NEW.supersedes_universe_run_id FOR UPDATE;
        IF parent.availability_mode <> 'HISTORICAL_RECONSTRUCTED'
           OR parent.status NOT IN ('COMPLETE', 'DEGRADED')
           OR NEW.policy_version <> parent.policy_version OR NEW.policy_sha256 <> parent.policy_sha256
           OR NEW.effective_from <> parent.effective_from
           OR NEW.replay_available_at IS DISTINCT FROM parent.replay_available_at
           OR NEW.revision_published_at <= COALESCE(parent.revision_published_at, parent.observed_at)
           OR NEW.revision_published_at > clock_timestamp() THEN
            RAISE EXCEPTION 'universe revision must preserve parent policy/session/replay time and follow its publication';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_guard_equity_universe_revision ON public.equity_universe_runs;
CREATE TRIGGER trg_guard_equity_universe_revision
    BEFORE INSERT OR UPDATE OR DELETE ON public.equity_universe_runs
    FOR EACH ROW EXECUTE FUNCTION public.guard_equity_universe_revision();

CREATE OR REPLACE FUNCTION public.guard_equity_universe_revision_member() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF EXISTS (SELECT 1 FROM public.equity_universe_runs WHERE supersedes_universe_run_id = NEW.universe_run_id) THEN
            RAISE EXCEPTION 'superseded universe members are immutable';
        END IF;
        RETURN NEW;
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.equity_universe_runs
        WHERE (universe_run_id = OLD.universe_run_id AND supersedes_universe_run_id IS NOT NULL)
           OR supersedes_universe_run_id = OLD.universe_run_id
           OR (TG_OP = 'UPDATE' AND universe_run_id = NEW.universe_run_id AND supersedes_universe_run_id IS NOT NULL)
           OR (TG_OP = 'UPDATE' AND supersedes_universe_run_id = NEW.universe_run_id)
    ) THEN
        RAISE EXCEPTION 'published universe revision members are immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_guard_equity_universe_revision_member ON public.equity_universe_members;
CREATE TRIGGER trg_guard_equity_universe_revision_member
    BEFORE INSERT OR UPDATE OR DELETE ON public.equity_universe_members
    FOR EACH ROW EXECUTE FUNCTION public.guard_equity_universe_revision_member();

CREATE OR REPLACE FUNCTION public.validate_equity_universe_revision_members() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE run_id uuid; run public.equity_universe_runs%ROWTYPE;
        member_count bigint; ticker_count bigint;
BEGIN
    run_id := COALESCE(NEW.universe_run_id, OLD.universe_run_id);
    SELECT * INTO run FROM public.equity_universe_runs WHERE universe_run_id = run_id;
    IF run.supersedes_universe_run_id IS NOT NULL THEN
        SELECT COUNT(*), COUNT(DISTINCT ticker) INTO member_count, ticker_count
        FROM public.equity_universe_members WHERE universe_run_id = run_id;
        IF member_count <> run.admitted_members OR ticker_count <> member_count THEN
            RAISE EXCEPTION 'universe revision membership is incomplete or ambiguous';
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_validate_equity_universe_revision_count ON public.equity_universe_runs;
CREATE CONSTRAINT TRIGGER trg_validate_equity_universe_revision_count
    AFTER INSERT OR UPDATE ON public.equity_universe_runs DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION public.validate_equity_universe_revision_members();
DROP TRIGGER IF EXISTS trg_validate_equity_universe_revision_members ON public.equity_universe_members;
CREATE CONSTRAINT TRIGGER trg_validate_equity_universe_revision_members
    AFTER INSERT OR UPDATE OR DELETE ON public.equity_universe_members DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION public.validate_equity_universe_revision_members();

CREATE OR REPLACE VIEW public.equity_original_universe_runs AS
    SELECT * FROM public.equity_universe_runs WHERE supersedes_universe_run_id IS NULL;