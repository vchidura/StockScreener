SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS public.option_o3_credit_observations (
    evaluation_id uuid PRIMARY KEY,
    dataset_id varchar(80) NOT NULL,
    run_id uuid NOT NULL,
    scheduled_cycle timestamptz NOT NULL,
    session_date date NOT NULL,
    selector_sha256 char(64) NOT NULL CHECK (selector_sha256 ~ '^[0-9a-f]{64}$'),
    candidate_id uuid NOT NULL REFERENCES public.option_strategy_candidates(candidate_id),
    matrix_id uuid NOT NULL REFERENCES public.option_analysis_runs(matrix_id),
    recurrence_sha256 char(64) NOT NULL CHECK (recurrence_sha256 ~ '^[0-9a-f]{64}$'),
    selected_at timestamptz NOT NULL,
    payload_text text NOT NULL CHECK (octet_length(payload_text) <= 65536),
    payload_sha256 char(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (dataset_id,run_id,recurrence_sha256),
    CHECK (scheduled_cycle <= selected_at AND selected_at <= recorded_at),
    CHECK (session_date = (scheduled_cycle AT TIME ZONE 'America/New_York')::date),
    CHECK (payload_sha256 = encode(sha256(convert_to(payload_text,'UTF8')),'hex')),
    CHECK ((payload_text::jsonb->>'schema_version') = 'option_o3_credit_evaluation_evidence_v1'),
    CHECK ((payload_text::jsonb->>'dataset_id') = dataset_id),
    CHECK ((payload_text::jsonb->>'run_id')::uuid = run_id),
    CHECK ((payload_text::jsonb->>'selector_sha256') = selector_sha256),
    CHECK ((payload_text::jsonb->>'selection_status') = 'OBSERVATION'),
    CHECK ((payload_text::jsonb->>'selection_reason') = 'CREDIT_SHADOW_ONLY'),
    CHECK ((payload_text::jsonb->>'evidence_mode') = 'PROSPECTIVE_RECEIPT'),
    CHECK ((payload_text::jsonb->'observation'->>'detector_id') = 'O3'),
    CHECK ((payload_text::jsonb->'observation'->>'candidate_id')::uuid = candidate_id),
    CHECK ((payload_text::jsonb->'observation'->>'matrix_id')::uuid = matrix_id),
    CHECK ((payload_text::jsonb->'observation'->>'scheduled_cycle')::timestamptz = scheduled_cycle),
    CHECK ((payload_text::jsonb->>'recurrence_sha256') = recurrence_sha256)
);
CREATE INDEX IF NOT EXISTS idx_option_o3_credit_observations_session
    ON public.option_o3_credit_observations (dataset_id,session_date,scheduled_cycle);
CREATE INDEX IF NOT EXISTS idx_option_o3_credit_observations_run
    ON public.option_o3_credit_observations (run_id);

CREATE OR REPLACE FUNCTION public.guard_option_o3_credit_observation() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    payload jsonb := NEW.payload_text::jsonb;
    observation jsonb := payload->'observation';
    candidate public.option_strategy_candidates%ROWTYPE;
BEGIN
    SELECT * INTO candidate FROM public.option_strategy_candidates WHERE candidate_id=NEW.candidate_id;
    IF NOT COALESCE(
       payload->>'schema_version'='option_o3_credit_evaluation_evidence_v1'
       AND payload->>'dataset_id'=NEW.dataset_id
       AND (payload->>'run_id')::uuid=NEW.run_id
       AND payload->>'selector_sha256'=NEW.selector_sha256
       AND payload->>'selection_status'='OBSERVATION'
       AND payload->>'selection_reason'='CREDIT_SHADOW_ONLY'
       AND payload->>'evidence_mode'='PROSPECTIVE_RECEIPT'
       AND (payload->>'selected_at')::timestamptz=NEW.selected_at
       AND payload->>'recurrence_sha256'=NEW.recurrence_sha256
       AND observation->>'schema_version'='option_o3_credit_observation_v1'
       AND observation->>'detector_id'='O3'
       AND observation->>'origin'='OPTIONS_FIRST'
       AND observation->>'disposition'='INDICATIVE_MODEL_MARK_ONLY'
       AND observation->'publication_permission'='false'::jsonb
       AND observation->'execution_permission'='false'::jsonb
       AND (observation->>'scheduled_cycle')::timestamptz=NEW.scheduled_cycle
       AND (observation->>'matrix_id')::uuid=NEW.matrix_id
       AND (observation->>'candidate_id')::uuid=NEW.candidate_id
       AND (observation->>'decision_at')::timestamptz<=NEW.selected_at
       AND FOUND
       AND candidate.matrix_id=NEW.matrix_id
       AND candidate.candidate_identity=observation->>'candidate_identity_sha256'
       AND candidate.strategy_name='SPREAD_RANGE_LOCATOR'
       AND candidate.structure_type IN ('PUT_CREDIT_VERTICAL','CALL_CREDIT_VERTICAL')
       AND candidate.observed_time<=NEW.selected_at, false) THEN
        RAISE EXCEPTION 'O3 credit observation must preserve shadow identity, candidate and clocks';
    END IF;
    NEW.recorded_at := clock_timestamp();
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS option_o3_credit_observation_guard ON public.option_o3_credit_observations;
CREATE TRIGGER option_o3_credit_observation_guard BEFORE INSERT ON public.option_o3_credit_observations
    FOR EACH ROW EXECUTE FUNCTION public.guard_option_o3_credit_observation();
DROP TRIGGER IF EXISTS option_o3_credit_observations_immutable ON public.option_o3_credit_observations;
CREATE TRIGGER option_o3_credit_observations_immutable BEFORE UPDATE OR DELETE ON public.option_o3_credit_observations
    FOR EACH ROW EXECUTE FUNCTION public.reject_option_alert_mutation();
DROP TRIGGER IF EXISTS option_o3_credit_observations_no_truncate ON public.option_o3_credit_observations;
CREATE TRIGGER option_o3_credit_observations_no_truncate BEFORE TRUNCATE ON public.option_o3_credit_observations
    FOR EACH STATEMENT EXECUTE FUNCTION public.reject_option_alert_mutation();