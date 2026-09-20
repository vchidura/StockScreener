SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS public.option_detector_evaluations (
    evaluation_id uuid PRIMARY KEY,
    dataset_id varchar(80) NOT NULL,
    run_id uuid NOT NULL,
    scheduled_cycle timestamptz NOT NULL,
    session_date date NOT NULL,
    selector_sha256 char(64) NOT NULL CHECK (selector_sha256 ~ '^[0-9a-f]{64}$'),
    detector_id varchar(16) NOT NULL CHECK (detector_id IN ('O1','O2','S1','S2')),
    candidate_id uuid REFERENCES public.option_strategy_candidates(candidate_id),
    matrix_id uuid NOT NULL REFERENCES public.option_analysis_runs(matrix_id),
    recurrence_sha256 char(64) NOT NULL CHECK (recurrence_sha256 ~ '^[0-9a-f]{64}$'),
    selection_status varchar(24) NOT NULL CHECK (selection_status IN ('SELECTED','NOT_SELECTED','REPEAT','OBSERVATION')),
    selection_reason varchar(64) NOT NULL,
    selected_at timestamptz NOT NULL,
    payload_text text NOT NULL CHECK (octet_length(payload_text) <= 65536),
    payload_sha256 char(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (dataset_id,run_id,detector_id,recurrence_sha256),
    CHECK ((detector_id='O2' AND selection_status='OBSERVATION' AND candidate_id IS NULL)
        OR (detector_id<>'O2' AND selection_status<>'OBSERVATION' AND candidate_id IS NOT NULL)),
    CHECK (scheduled_cycle <= selected_at AND selected_at <= recorded_at),
    CHECK (session_date = (scheduled_cycle AT TIME ZONE 'America/New_York')::date),
    CHECK (payload_sha256 = encode(sha256(convert_to(payload_text,'UTF8')),'hex')),
    CHECK ((payload_text::jsonb->>'dataset_id') = dataset_id),
    CHECK ((payload_text::jsonb->>'run_id')::uuid = run_id),
    CHECK ((payload_text::jsonb->>'selector_sha256') = selector_sha256),
    CHECK ((payload_text::jsonb->>'selection_status') = selection_status),
    CHECK ((payload_text::jsonb->>'selection_reason') = selection_reason),
    CHECK ((payload_text::jsonb->>'evidence_mode') = 'PROSPECTIVE_RECEIPT'),
    CHECK ((payload_text::jsonb->'package'->>'candidate_id')::uuid = candidate_id),
    CHECK ((payload_text::jsonb->'package'->>'matrix_id')::uuid = matrix_id),
    CHECK ((payload_text::jsonb->'package'->>'detector_id') = detector_id),
    CHECK ((payload_text::jsonb->'package'->>'recurrence_sha256') = recurrence_sha256)
);
CREATE INDEX IF NOT EXISTS idx_option_detector_evaluations_session
    ON public.option_detector_evaluations (dataset_id,session_date,scheduled_cycle);
CREATE INDEX IF NOT EXISTS idx_option_detector_evaluations_run
    ON public.option_detector_evaluations (run_id);
CREATE OR REPLACE FUNCTION public.guard_option_detector_evaluation() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    payload jsonb := NEW.payload_text::jsonb;
    package jsonb := payload->'package';
    candidate public.option_strategy_candidates%ROWTYPE;
BEGIN
    IF NEW.detector_id='O2' THEN
        IF NOT COALESCE(payload->>'schema_version'='option_surface_evaluation_evidence_v1'
           AND payload->>'dataset_id'=NEW.dataset_id AND (payload->>'run_id')::uuid=NEW.run_id
           AND payload->>'selector_sha256'=NEW.selector_sha256
           AND payload->>'selection_status'='OBSERVATION' AND NEW.selection_status='OBSERVATION'
           AND payload->>'selection_reason'='OBSERVATION_ONLY' AND NEW.selection_reason='OBSERVATION_ONLY'
           AND payload->>'evidence_mode'='PROSPECTIVE_RECEIPT'
           AND (payload->>'selected_at')::timestamptz=NEW.selected_at
           AND payload->>'recurrence_sha256'=NEW.recurrence_sha256
           AND (payload->'observation'->>'scheduled_cycle')::timestamptz=NEW.scheduled_cycle
           AND (payload->'observation'->>'matrix_id')::uuid=NEW.matrix_id
           AND payload->'observation'->>'detector_id'='O2'
           AND payload->'observation'->>'output_kind'='OBSERVATION'
           AND payload->'observation'->>'package_status'='NOT_APPLICABLE'
           AND (payload->'observation'->>'decision_at')::timestamptz<=NEW.selected_at
           AND NEW.candidate_id IS NULL, false) THEN
            RAISE EXCEPTION 'surface evaluation must preserve observation-only identity and clocks';
        END IF;
        NEW.recorded_at := clock_timestamp();
        RETURN NEW;
    END IF;
    IF NOT COALESCE(payload->>'schema_version'='option_detector_selection_evidence_v1'
       AND payload->>'dataset_id'=NEW.dataset_id AND (payload->>'run_id')::uuid=NEW.run_id
       AND payload->>'selector_sha256'=NEW.selector_sha256
       AND payload->>'selection_status'=NEW.selection_status
       AND payload->>'selection_reason'=NEW.selection_reason
       AND payload->>'evidence_mode'='PROSPECTIVE_RECEIPT'
       AND (payload->>'selected_at')::timestamptz=NEW.selected_at
       AND (package->>'scheduled_cycle')::timestamptz=NEW.scheduled_cycle
       AND (package->>'candidate_id')::uuid=NEW.candidate_id
       AND (package->>'matrix_id')::uuid=NEW.matrix_id
       AND package->>'detector_id'=NEW.detector_id
       AND package->>'recurrence_sha256'=NEW.recurrence_sha256
       AND package->>'evidence_mode'='PROSPECTIVE_RECEIPT'
       AND package->>'status'='QUALIFIED_INDICATIVE'
       AND (package->>'decision_at')::timestamptz<=NEW.selected_at
       AND NEW.selected_at<(package->>'entry_deadline')::timestamptz
       AND package->>'plan_sha256'=encode(sha256(convert_to(payload->>'plan_payload_text','UTF8')),'hex'), false) THEN
        RAISE EXCEPTION 'detector evaluation payload does not match its identity or original clocks';
    END IF;
    SELECT * INTO candidate FROM public.option_strategy_candidates WHERE candidate_id=NEW.candidate_id;
    IF NOT FOUND OR candidate.matrix_id<>NEW.matrix_id
       OR candidate.candidate_identity IS DISTINCT FROM package->>'candidate_identity_sha256'
       OR candidate.observed_time>NEW.selected_at OR candidate.valid_until IS NULL
       OR (package->>'entry_deadline')::timestamptz>candidate.valid_until THEN
        RAISE EXCEPTION 'detector evaluation candidate reference mismatch';
    END IF;
    NEW.recorded_at := clock_timestamp();
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS option_detector_evaluation_guard ON public.option_detector_evaluations;
CREATE TRIGGER option_detector_evaluation_guard BEFORE INSERT ON public.option_detector_evaluations
    FOR EACH ROW EXECUTE FUNCTION public.guard_option_detector_evaluation();
DROP TRIGGER IF EXISTS option_detector_evaluations_immutable ON public.option_detector_evaluations;
CREATE TRIGGER option_detector_evaluations_immutable BEFORE UPDATE OR DELETE ON public.option_detector_evaluations
    FOR EACH ROW EXECUTE FUNCTION public.reject_option_alert_mutation();
DROP TRIGGER IF EXISTS option_detector_evaluations_no_truncate ON public.option_detector_evaluations;
CREATE TRIGGER option_detector_evaluations_no_truncate BEFORE TRUNCATE ON public.option_detector_evaluations
    FOR EACH STATEMENT EXECUTE FUNCTION public.reject_option_alert_mutation();