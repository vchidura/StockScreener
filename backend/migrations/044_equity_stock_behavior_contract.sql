SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE public.equity_context_snapshots
    ADD COLUMN IF NOT EXISTS context_kind text NOT NULL DEFAULT 'LEGACY',
    ADD COLUMN IF NOT EXISTS behavior_schema_version text,
    ADD COLUMN IF NOT EXISTS behavior_definition_sha256 text,
    ADD COLUMN IF NOT EXISTS behavior_computed_at timestamptz,
    ADD COLUMN IF NOT EXISTS behavior_payload_text text,
    ADD COLUMN IF NOT EXISTS behavior_payload_sha256 text;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'public.equity_context_snapshots'::regclass
                   AND conname = 'ck_equity_context_behavior_contract') THEN
        ALTER TABLE public.equity_context_snapshots ADD CONSTRAINT ck_equity_context_behavior_contract CHECK ((
            (context_kind = 'LEGACY'
             AND behavior_schema_version IS NULL AND behavior_definition_sha256 IS NULL
             AND behavior_computed_at IS NULL
             AND behavior_payload_text IS NULL AND behavior_payload_sha256 IS NULL)
            OR
            (context_kind = 'STOCK_BEHAVIOR'
             AND behavior_schema_version = 'stock_behavior_v1'
             AND strategy_horizon = 'OPTIONS_SWING_V1'
             AND behavior_definition_sha256 ~ '^[0-9a-f]{64}$'
             AND behavior_payload_sha256 ~ '^[0-9a-f]{64}$'
             AND status = 'DEGRADED' AND qualified_direction IS NULL
             AND direction_qualification_id IS NULL AND direction_evidence_id IS NULL
             AND isfinite(market_time) AND isfinite(behavior_computed_at)
             AND isfinite(observed_at) AND isfinite(valid_until)
             AND market_time <= behavior_computed_at AND behavior_computed_at <= observed_at
             AND observed_at < valid_until
             AND octet_length(behavior_payload_text) BETWEEN 2 AND 262144
             AND behavior_payload_sha256 = encode(sha256(convert_to(behavior_payload_text, 'UTF8')), 'hex')
               AND jsonb_typeof(behavior_payload_text::jsonb) = 'object'
               AND behavior_payload_text::jsonb->>'schema_version' = behavior_schema_version
               AND behavior_payload_text::jsonb->>'definition_sha256' = behavior_definition_sha256
               AND behavior_payload_text::jsonb->>'profile' = strategy_horizon
               AND behavior_payload_text::jsonb->>'policy_sha256' = context_policy_sha256
               AND (behavior_payload_text::jsonb->>'security_id')::uuid = security_id
               AND (behavior_payload_text::jsonb->>'security_revision_id')::uuid = security_revision_id
               AND behavior_payload_text::jsonb->>'ticker' = ticker
               AND (behavior_payload_text::jsonb->>'market_time')::timestamptz = market_time
               AND (behavior_payload_text::jsonb->>'computed_at')::timestamptz = behavior_computed_at
               AND (behavior_payload_text::jsonb->>'available_at')::timestamptz = observed_at
               AND (behavior_payload_text::jsonb->>'valid_until')::timestamptz = valid_until
               AND behavior_payload_text::jsonb->>'availability_mode' IN ('PROSPECTIVE_RECEIPT', 'RECONSTRUCTED')
               AND behavior_payload_text::jsonb->'execution_permission' = 'false'::jsonb
               AND jsonb_typeof(behavior_payload_text::jsonb->'components') = 'array'
               AND jsonb_array_length(behavior_payload_text::jsonb->'components') BETWEEN 1 AND 18
               AND jsonb_typeof(behavior_payload_text::jsonb->'required_components') = 'array'
               AND jsonb_array_length(behavior_payload_text::jsonb->'required_components') BETWEEN 1 AND 18)
           ) IS TRUE) NOT VALID;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.guard_equity_behavior_context() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.context_kind = 'STOCK_BEHAVIOR' THEN
            NEW.created_at := clock_timestamp();
            IF NEW.observed_at > NEW.created_at THEN
                RAISE EXCEPTION 'behavior context cannot be received in the future';
            END IF;
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.context_kind = 'STOCK_BEHAVIOR' THEN
        RAISE EXCEPTION 'stock behavior evidence is immutable';
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.context_kind IS DISTINCT FROM OLD.context_kind THEN
        RAISE EXCEPTION 'legacy context cannot be reinterpreted as stock behavior';
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_guard_equity_behavior_context ON public.equity_context_snapshots;
CREATE TRIGGER trg_guard_equity_behavior_context BEFORE INSERT OR UPDATE OR DELETE
    ON public.equity_context_snapshots FOR EACH ROW EXECUTE FUNCTION public.guard_equity_behavior_context();

CREATE OR REPLACE FUNCTION public.guard_equity_behavior_evidence_link() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE parent_kind text; parent_created_here boolean; source_contract_ok boolean;
BEGIN
    SELECT context_kind, xmin::text = pg_current_xact_id()::text
    INTO parent_kind, parent_created_here
    FROM public.equity_context_snapshots
    WHERE equity_context_snapshot_id = COALESCE(NEW.equity_context_snapshot_id, OLD.equity_context_snapshot_id);
    IF parent_kind = 'STOCK_BEHAVIOR' THEN
        IF TG_OP = 'INSERT' AND parent_created_here THEN
            SELECT EXISTS (
                SELECT 1 FROM public.equity_evidence
                WHERE evidence_id = NEW.evidence_id AND (
                    (source_name = 'STOCK_BEHAVIOR_ADJUSTED_DAILY'
                     AND source_version = 'provider_adjusted_daily_history_v1'
                     AND payload_schema_version = 'stock_behavior_adjusted_daily_source_v1')
                    OR
                    (source_name = 'STOCK_BEHAVIOR_RAW_SOURCE'
                     AND source_version = 'behavior_feature_source_v1'
                     AND payload_schema_version = 'stock_behavior_raw_source_v1')
                )
            ) INTO source_contract_ok;
            IF source_contract_ok THEN RETURN NEW; END IF;
            RAISE EXCEPTION 'stock behavior links require dedicated immutable source evidence';
        END IF;
        RAISE EXCEPTION 'stock behavior evidence links are immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_guard_equity_behavior_evidence_link ON public.equity_context_evidence;
CREATE TRIGGER trg_guard_equity_behavior_evidence_link BEFORE INSERT OR UPDATE OR DELETE
    ON public.equity_context_evidence FOR EACH ROW EXECUTE FUNCTION public.guard_equity_behavior_evidence_link();

CREATE OR REPLACE FUNCTION public.guard_equity_behavior_source_evidence() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM public.equity_context_evidence AS link
        JOIN public.equity_context_snapshots AS context USING (equity_context_snapshot_id)
        WHERE link.evidence_id = OLD.evidence_id AND context.context_kind = 'STOCK_BEHAVIOR'
    ) THEN
        RAISE EXCEPTION 'stock behavior source evidence is immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_guard_equity_behavior_source_evidence ON public.equity_evidence;
CREATE TRIGGER trg_guard_equity_behavior_source_evidence BEFORE UPDATE OR DELETE
    ON public.equity_evidence FOR EACH ROW EXECUTE FUNCTION public.guard_equity_behavior_source_evidence();

CREATE OR REPLACE FUNCTION public.guard_equity_behavior_evidence_truncate() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.equity_context_snapshots WHERE context_kind = 'STOCK_BEHAVIOR') THEN
        RAISE EXCEPTION 'stock behavior evidence prevents evidence truncation';
    END IF;
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS trg_guard_equity_behavior_link_truncate ON public.equity_context_evidence;
CREATE TRIGGER trg_guard_equity_behavior_link_truncate BEFORE TRUNCATE ON public.equity_context_evidence
    FOR EACH STATEMENT EXECUTE FUNCTION public.guard_equity_behavior_evidence_truncate();
DROP TRIGGER IF EXISTS trg_guard_equity_behavior_source_truncate ON public.equity_evidence;
CREATE TRIGGER trg_guard_equity_behavior_source_truncate BEFORE TRUNCATE ON public.equity_evidence
    FOR EACH STATEMENT EXECUTE FUNCTION public.guard_equity_behavior_evidence_truncate();

CREATE OR REPLACE FUNCTION public.guard_equity_behavior_truncate() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.equity_context_snapshots WHERE context_kind = 'STOCK_BEHAVIOR') THEN
        RAISE EXCEPTION 'stock behavior evidence prevents context truncation';
    END IF;
    RETURN NULL;
END;
$$;
DROP TRIGGER IF EXISTS trg_guard_equity_behavior_truncate ON public.equity_context_snapshots;
CREATE TRIGGER trg_guard_equity_behavior_truncate BEFORE TRUNCATE ON public.equity_context_snapshots
    FOR EACH STATEMENT EXECUTE FUNCTION public.guard_equity_behavior_truncate();