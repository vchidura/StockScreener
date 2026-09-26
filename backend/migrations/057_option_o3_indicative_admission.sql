SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

DO $$
DECLARE
    item record;
BEGIN
    IF EXISTS (SELECT 1 FROM public.option_o3_credit_observations) THEN
        RAISE EXCEPTION 'O3 admission migration requires an empty prospective relation';
    END IF;
    FOR item IN
        SELECT conname FROM pg_constraint
        WHERE conrelid='public.option_o3_credit_observations'::regclass
          AND contype='c'
          AND (pg_get_constraintdef(oid) LIKE '%selection_status%'
               OR pg_get_constraintdef(oid) LIKE '%selection_reason%')
    LOOP
        EXECUTE format('ALTER TABLE public.option_o3_credit_observations DROP CONSTRAINT %I', item.conname);
    END LOOP;
END;
$$;

ALTER TABLE public.option_o3_credit_observations
    ADD CONSTRAINT option_o3_credit_selected_status
        CHECK ((payload_text::jsonb->>'selection_status') = 'SELECTED'),
    ADD CONSTRAINT option_o3_credit_admission_reason
        CHECK ((payload_text::jsonb->>'selection_reason') = 'O3_INDICATIVE_ADMISSION');

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
       AND payload->>'selection_status'='SELECTED'
       AND payload->>'selection_reason'='O3_INDICATIVE_ADMISSION'
       AND payload->>'evidence_mode'='PROSPECTIVE_RECEIPT'
       AND (payload->>'selected_at')::timestamptz=NEW.selected_at
       AND payload->>'recurrence_sha256'=NEW.recurrence_sha256
       AND observation->>'schema_version'='option_o3_credit_observation_v1'
       AND observation->>'detector_id'='O3'
       AND observation->>'origin'='OPTIONS_FIRST'
       AND observation->>'disposition'='QUALIFIED_INDICATIVE'
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
        RAISE EXCEPTION 'O3 indicative alert must preserve admission identity, candidate and clocks';
    END IF;
    NEW.recorded_at := clock_timestamp();
    RETURN NEW;
END;
$$;
