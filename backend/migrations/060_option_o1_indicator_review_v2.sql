SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE OR REPLACE FUNCTION public.guard_option_o1_indicator_observation() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    payload jsonb := NEW.payload_text::jsonb;
    observation jsonb := payload->'observation';
BEGIN
    IF NOT COALESCE(
       payload->>'schema_version'='option_o1_indicator_evaluation_evidence_v1'
       AND payload->>'dataset_id'=NEW.dataset_id
       AND (payload->>'run_id')::uuid=NEW.run_id
       AND payload->>'selector_sha256'=NEW.selector_sha256
       AND payload->>'selection_status'='OBSERVATION'
       AND payload->>'selection_reason'='INDICATOR_SHADOW_ONLY'
       AND payload->>'evidence_mode'='PROSPECTIVE_RECEIPT'
       AND (payload->>'selected_at')::timestamptz=NEW.selected_at
       AND payload->>'recurrence_sha256'=NEW.recurrence_sha256
       AND ((observation->>'schema_version'='option_o1_indicator_observation_v1'
             AND observation->'policy'->>'version'='option_participation_indicator_review_v1')
         OR (observation->>'schema_version'='option_o1_indicator_observation_v2'
             AND observation->'policy'->>'version'='option_participation_indicator_review_v2'))
       AND observation->>'detector_id'='O1'
       AND observation->>'output_kind'='OBSERVATION'
       AND observation->>'package_status'='NOT_ASSESSED'
       AND observation->>'outcome_status'='NOT_YET_MEASURED'
       AND (observation->>'scheduled_cycle')::timestamptz=NEW.scheduled_cycle
       AND (observation->>'matrix_id')::uuid=NEW.matrix_id
       AND (observation->>'decision_at')::timestamptz<=NEW.selected_at
       AND observation->'policy'->'changes_admission'='false'::jsonb
       AND observation->'execution_permission'='false'::jsonb
       AND EXISTS(SELECT 1 FROM public.option_analysis_runs WHERE matrix_id=NEW.matrix_id), false) THEN
        RAISE EXCEPTION 'O1 indicator observation must preserve shadow identity, policy and clocks';
    END IF;
    NEW.recorded_at := clock_timestamp();
    RETURN NEW;
END;
$$;
INSERT INTO public.schema_migrations(version) VALUES ('060_option_o1_indicator_review_v2') ON CONFLICT DO NOTHING;
