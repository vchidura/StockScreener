SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS public.option_stock_behavior_assessments (
    assessment_id uuid PRIMARY KEY,
    candidate_id uuid NOT NULL REFERENCES public.option_strategy_candidates(candidate_id),
    candidate_identity character(64) NOT NULL CHECK (candidate_identity ~ '^[0-9a-f]{64}$'),
    matrix_id uuid NOT NULL REFERENCES public.option_analysis_runs(matrix_id),
    stock_snapshot_id uuid REFERENCES public.equity_context_snapshots(equity_context_snapshot_id),
    underlying varchar(16) NOT NULL,
    option_strategy_version varchar(64) NOT NULL,
    option_strategy_policy_sha256 character(64) NOT NULL CHECK (option_strategy_policy_sha256 ~ '^[0-9a-f]{64}$'),
    option_configuration_sha256 character(64) NOT NULL CHECK (option_configuration_sha256 ~ '^[0-9a-f]{64}$'),
    option_market_policy_sha256 character(64) NOT NULL CHECK (option_market_policy_sha256 ~ '^[0-9a-f]{64}$'),
    option_analysis_policy_sha256 character(64) NOT NULL CHECK (option_analysis_policy_sha256 ~ '^[0-9a-f]{64}$'),
    option_market_time timestamptz NOT NULL,
    option_observed_at timestamptz NOT NULL,
    stock_market_cutoff timestamptz NOT NULL,
    detector_policy_version varchar(64) NOT NULL,
    detector_policy_sha256 character(64) NOT NULL CHECK (detector_policy_sha256 ~ '^[0-9a-f]{64}$'),
    decision_at timestamptz NOT NULL,
    disposition varchar(24) NOT NULL CHECK (
        disposition IN ('ELIGIBLE_RESEARCH', 'BLOCKED', 'UNAVAILABLE', 'NOT_APPLICABLE')
    ),
    payload_text text NOT NULL,
    payload_sha256 character(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (isfinite(option_market_time) AND isfinite(option_observed_at)
        AND isfinite(stock_market_cutoff) AND isfinite(decision_at) AND isfinite(recorded_at)),
    CHECK (option_market_time <= option_observed_at AND option_observed_at <= decision_at),
    CHECK (stock_market_cutoff <= decision_at),
    CHECK (octet_length(payload_text) BETWEEN 2 AND 262144),
    CHECK (payload_sha256 = encode(sha256(convert_to(payload_text, 'UTF8')), 'hex')),
    CHECK ((
        jsonb_typeof(payload_text::jsonb) = 'object'
        AND payload_text::jsonb->>'schema_version' = 'option_stock_behavior_assessment_v1'
        AND (payload_text::jsonb->>'candidate_id')::uuid = candidate_id
        AND payload_text::jsonb->>'candidate_identity_sha256' = candidate_identity
        AND (payload_text::jsonb->>'matrix_id')::uuid = matrix_id
        AND payload_text::jsonb->>'underlyer' = underlying
        AND payload_text::jsonb->>'option_strategy_version' = option_strategy_version
        AND payload_text::jsonb->>'option_strategy_policy_sha256' = option_strategy_policy_sha256
        AND payload_text::jsonb->>'option_configuration_sha256' = option_configuration_sha256
        AND payload_text::jsonb->>'option_market_policy_sha256' = option_market_policy_sha256
        AND payload_text::jsonb->>'option_analysis_policy_sha256' = option_analysis_policy_sha256
        AND (payload_text::jsonb->>'option_market_time')::timestamptz = option_market_time
        AND (payload_text::jsonb->>'option_observed_at')::timestamptz = option_observed_at
        AND (payload_text::jsonb->>'stock_market_cutoff')::timestamptz = stock_market_cutoff
        AND payload_text::jsonb->>'detector_policy_version' = detector_policy_version
        AND payload_text::jsonb->>'detector_policy_sha256' = detector_policy_sha256
        AND (payload_text::jsonb->>'decision_at')::timestamptz = decision_at
        AND payload_text::jsonb->>'disposition' = disposition
        AND payload_text::jsonb->'assessment_only' = 'true'::jsonb
        AND payload_text::jsonb->'execution_permission' = 'false'::jsonb
        AND CASE
            WHEN stock_snapshot_id IS NULL
            THEN payload_text::jsonb->'stock_snapshot_id' = 'null'::jsonb
            ELSE (payload_text::jsonb->>'stock_snapshot_id')::uuid = stock_snapshot_id
        END
    ) IS TRUE)
);

CREATE INDEX IF NOT EXISTS idx_option_stock_behavior_candidate
    ON public.option_stock_behavior_assessments(candidate_id, decision_at DESC, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_option_stock_behavior_snapshot
    ON public.option_stock_behavior_assessments(stock_snapshot_id)
    WHERE stock_snapshot_id IS NOT NULL;

CREATE OR REPLACE FUNCTION public.guard_option_stock_behavior_assessment() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    candidate public.option_strategy_candidates%ROWTYPE;
    stock_context public.equity_context_snapshots%ROWTYPE;
    source_underlying text;
    source_market_time timestamptz;
    source_observed_time timestamptz;
    source_scheduled_cycle timestamptz;
    source_configuration_sha256 text;
    source_market_policy_sha256 text;
    source_analysis_policy_sha256 text;
BEGIN
    SELECT * INTO candidate
    FROM public.option_strategy_candidates
    WHERE candidate_id = NEW.candidate_id;
    IF NOT FOUND THEN RAISE EXCEPTION 'stock behavior assessment candidate is missing'; END IF;
    IF candidate.candidate_identity <> NEW.candidate_identity
       OR candidate.matrix_id <> NEW.matrix_id
       OR candidate.underlying <> NEW.underlying
         OR candidate.strategy_version <> NEW.option_strategy_version
         OR candidate.policy_sha256 <> NEW.option_strategy_policy_sha256
         OR candidate.market_data_time <> NEW.option_market_time
         OR candidate.observed_time <> NEW.option_observed_at
         OR candidate.observed_time <> NEW.decision_at THEN
        RAISE EXCEPTION 'stock behavior assessment candidate identity or clocks disagree';
    END IF;
    SELECT analysis.underlying, analysis.market_time, analysis.observed_time,
           ingestion.scheduled_cycle, ingestion.configuration_sha256,
           ingestion.policy_sha256, analysis.policy_sha256
    INTO source_underlying, source_market_time, source_observed_time,
         source_scheduled_cycle, source_configuration_sha256,
         source_market_policy_sha256, source_analysis_policy_sha256
    FROM public.option_analysis_runs AS analysis
    JOIN public.option_ingestion_runs AS ingestion USING (batch_id)
    WHERE analysis.matrix_id = NEW.matrix_id;
    IF NOT FOUND
       OR source_underlying <> NEW.underlying
       OR source_market_time <> NEW.option_market_time
       OR source_observed_time <> NEW.option_observed_at
       OR source_scheduled_cycle <> NEW.stock_market_cutoff
       OR source_configuration_sha256 <> NEW.option_configuration_sha256
       OR source_market_policy_sha256 <> NEW.option_market_policy_sha256
       OR source_analysis_policy_sha256 <> NEW.option_analysis_policy_sha256 THEN
        RAISE EXCEPTION 'stock behavior assessment matrix lineage disagrees';
    END IF;
    IF NEW.stock_snapshot_id IS NOT NULL THEN
        SELECT * INTO stock_context
        FROM public.equity_context_snapshots
        WHERE equity_context_snapshot_id = NEW.stock_snapshot_id;
        IF NOT FOUND OR stock_context.context_kind <> 'STOCK_BEHAVIOR'
           OR stock_context.ticker <> NEW.underlying
           OR stock_context.market_time > NEW.decision_at
           OR stock_context.observed_at > NEW.decision_at
           OR stock_context.valid_until <= NEW.decision_at THEN
            RAISE EXCEPTION 'stock behavior assessment source context is invalid';
        END IF;
    END IF;
    NEW.recorded_at := clock_timestamp();
    IF NEW.recorded_at < NEW.decision_at THEN
        RAISE EXCEPTION 'stock behavior assessment cannot be recorded before its decision';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_guard_option_stock_behavior_assessment
    ON public.option_stock_behavior_assessments;
CREATE TRIGGER trg_guard_option_stock_behavior_assessment
    BEFORE INSERT ON public.option_stock_behavior_assessments
    FOR EACH ROW EXECUTE FUNCTION public.guard_option_stock_behavior_assessment();

CREATE OR REPLACE FUNCTION public.reject_option_stock_behavior_assessment_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'option stock behavior assessments are immutable';
END;
$$;
DROP TRIGGER IF EXISTS trg_option_stock_behavior_assessment_immutable
    ON public.option_stock_behavior_assessments;
CREATE TRIGGER trg_option_stock_behavior_assessment_immutable
    BEFORE UPDATE OR DELETE ON public.option_stock_behavior_assessments
    FOR EACH ROW EXECUTE FUNCTION public.reject_option_stock_behavior_assessment_mutation();
DROP TRIGGER IF EXISTS trg_option_stock_behavior_assessment_no_truncate
    ON public.option_stock_behavior_assessments;
CREATE TRIGGER trg_option_stock_behavior_assessment_no_truncate
    BEFORE TRUNCATE ON public.option_stock_behavior_assessments
    FOR EACH STATEMENT EXECUTE FUNCTION public.reject_option_stock_behavior_assessment_mutation();