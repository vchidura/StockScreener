SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.option_outcome_unavailable_evidence) THEN
        RAISE EXCEPTION 'unavailable leg contract migration requires an empty ledger';
    END IF;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.option_outcome_unavailable_evidence'::regclass
          AND conname='ck_option_outcome_unavailable_payload_arrays'
    ) THEN
        ALTER TABLE public.option_outcome_unavailable_evidence
            ADD CONSTRAINT ck_option_outcome_unavailable_payload_arrays CHECK ((
                payload_text::jsonb->'required_contract_ids'
                    = to_jsonb(required_contract_ids)
                AND payload_text::jsonb->'observed_contract_ids'
                    = to_jsonb(observed_contract_ids)
                AND payload_text::jsonb->'missing_contract_ids'
                    = to_jsonb(missing_contract_ids)
                AND payload_text::jsonb->'source_batch_id' = 'null'::jsonb
                AND payload_text::jsonb->'reason_codes'
                    = '["COHERENT_PACKAGE_MARKS_UNAVAILABLE"]'::jsonb
            ) IS TRUE);
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.guard_option_outcome_unavailable_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    candidate_identity_value text;
    candidate_contract_ids bigint[];
    valuation_policy_matches boolean;
    expected_missing_ids bigint[];
BEGIN
    SELECT candidate_identity INTO candidate_identity_value
    FROM public.option_strategy_candidates WHERE candidate_id=NEW.candidate_id;
    IF NOT FOUND OR candidate_identity_value <> NEW.candidate_identity THEN
        RAISE EXCEPTION 'outcome unavailable evidence candidate identity disagrees';
    END IF;
    SELECT array_agg(contract_id ORDER BY leg_index),
           bool_and(valuation_policy_sha256=NEW.valuation_policy_sha256)
    INTO candidate_contract_ids, valuation_policy_matches
    FROM public.option_candidate_legs WHERE candidate_id=NEW.candidate_id;
    IF candidate_contract_ids IS NULL
       OR candidate_contract_ids <> NEW.required_contract_ids
       OR valuation_policy_matches IS NOT TRUE THEN
        RAISE EXCEPTION 'outcome unavailable evidence package legs or policy disagree';
    END IF;
    IF NOT NEW.observed_contract_ids <@ NEW.required_contract_ids THEN
        RAISE EXCEPTION 'outcome unavailable observed contracts are not package legs';
    END IF;
    SELECT COALESCE(array_agg(contract_id ORDER BY ordinal), ARRAY[]::bigint[])
    INTO expected_missing_ids
    FROM unnest(NEW.required_contract_ids) WITH ORDINALITY AS item(contract_id, ordinal)
    WHERE NOT contract_id=ANY(NEW.observed_contract_ids);
    IF expected_missing_ids <> NEW.missing_contract_ids THEN
        RAISE EXCEPTION 'outcome unavailable missing contracts are not exact';
    END IF;
    NEW.recorded_at := clock_timestamp();
    IF NEW.recorded_at < NEW.availability_deadline THEN
        RAISE EXCEPTION 'outcome unavailable evidence cannot precede its deadline';
    END IF;
    RETURN NEW;
END;
$$;