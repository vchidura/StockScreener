SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE OR REPLACE FUNCTION public.option_package_assessment_matches_candidate(
    assessment_candidate_id uuid,
    assessment_candidate_identity text,
    assessment_matrix_id uuid,
    assessment_valuation_policy_sha256 text,
    assessment_status text,
    assessment_package jsonb,
    assessment_assessed_at timestamptz
) RETURNS boolean
LANGUAGE plpgsql STABLE AS $$
DECLARE
    candidate public.option_strategy_candidates%ROWTYPE;
    package_breakevens numeric[];
BEGIN
    SELECT * INTO candidate FROM public.option_strategy_candidates
    WHERE candidate_id=assessment_candidate_id;
    IF NOT FOUND
       OR candidate.candidate_identity <> assessment_candidate_identity
       OR candidate.matrix_id <> assessment_matrix_id
       OR candidate.observed_time <> assessment_assessed_at THEN
        RETURN false;
    END IF;
    IF assessment_status='UNAVAILABLE' THEN
        RETURN assessment_package IS NULL;
    END IF;
    IF assessment_status <> 'READY' OR assessment_package IS NULL
       OR candidate.status <> 'SELECTED'
       OR candidate.candidate_kind NOT IN ('SINGLE_CONTRACT', 'MULTI_LEG') THEN
        RETURN false;
    END IF;
    SELECT COALESCE(array_agg(value::numeric ORDER BY ordinal), ARRAY[]::numeric[])
    INTO package_breakevens
    FROM jsonb_array_elements_text(assessment_package->'breakevens')
         WITH ORDINALITY AS item(value, ordinal);
    IF (assessment_package->>'candidate_id')::uuid <> candidate.candidate_id
       OR assessment_package->>'candidate_identity_sha256' <> candidate.candidate_identity
       OR (assessment_package->>'option_matrix_id')::uuid <> candidate.matrix_id
       OR assessment_package->>'underlyer' <> candidate.underlying
       OR assessment_package->>'strategy_name' <> candidate.strategy_name
       OR assessment_package->>'strategy_version' <> candidate.strategy_version
       OR assessment_package->>'strategy_policy_sha256' <> candidate.policy_sha256
       OR assessment_package->>'structure' <> candidate.structure_type
       OR (assessment_package->>'market_time')::timestamptz <> candidate.market_data_time
       OR (assessment_package->>'observed_at')::timestamptz <> candidate.observed_time
           OR (CASE WHEN candidate.valid_until IS NULL
               THEN assessment_package->'valid_until' <> 'null'::jsonb
               ELSE (assessment_package->>'valid_until')::timestamptz <> candidate.valid_until END)
       OR (assessment_package->>'net_premium')::numeric IS DISTINCT FROM candidate.net_premium
       OR (assessment_package->>'collateral_required')::numeric IS DISTINCT FROM candidate.collateral_required
       OR (assessment_package->>'capital_at_risk')::numeric IS DISTINCT FROM candidate.capital_at_risk
       OR (assessment_package->>'maximum_profit')::numeric IS DISTINCT FROM candidate.maximum_profit
       OR (assessment_package->>'maximum_loss')::numeric IS DISTINCT FROM candidate.maximum_loss
       OR package_breakevens IS DISTINCT FROM candidate.breakevens THEN
        RETURN false;
    END IF;
    IF jsonb_array_length(assessment_package->'ordered_legs') <> (
        SELECT count(*) FROM public.option_candidate_legs
        WHERE candidate_id=candidate.candidate_id
    ) OR EXISTS (
        SELECT 1
        FROM jsonb_array_elements(assessment_package->'ordered_legs')
             WITH ORDINALITY AS item(package_leg, ordinal)
        LEFT JOIN public.option_candidate_legs AS leg
          ON leg.candidate_id=candidate.candidate_id
         AND leg.leg_index=item.ordinal-1
        WHERE leg.candidate_id IS NULL
           OR (item.package_leg->>'leg_index')::integer <> leg.leg_index
           OR (item.package_leg->>'snapshot_id')::uuid <> leg.snapshot_id
           OR (item.package_leg->>'contract_id')::bigint <> leg.contract_id
           OR item.package_leg->>'contract_ticker' <> leg.contract_ticker
           OR item.package_leg->>'side' <> leg.side
           OR (item.package_leg->>'ratio')::integer <> leg.ratio
           OR (item.package_leg->>'multiplier')::integer <> leg.multiplier
           OR (item.package_leg->>'expiration_date')::date <> leg.expiration_date
           OR (item.package_leg->>'strike')::numeric <> leg.strike
           OR item.package_leg->>'contract_type' <> leg.contract_type
           OR (item.package_leg->>'spot')::numeric <> leg.spot
           OR (item.package_leg->>'entry_mark')::numeric IS DISTINCT FROM leg.model_mark
           OR (item.package_leg->>'time_to_expiration_years')::double precision <> leg.time_to_expiration_years
           OR (item.package_leg->>'risk_free_rate')::double precision <> leg.risk_free_rate
           OR (item.package_leg->>'dividend_yield')::double precision <> leg.dividend_yield
           OR (item.package_leg->>'entry_iv')::double precision IS DISTINCT FROM leg.local_iv
           OR (item.package_leg->>'source_market_time')::timestamptz <> leg.source_market_time
           OR item.package_leg->>'mark_source' <> leg.mark_source
           OR item.package_leg->>'model_version' <> leg.model_version
           OR item.package_leg->>'valuation_policy_version' IS DISTINCT FROM leg.valuation_policy_version
           OR item.package_leg->>'valuation_policy_sha256' IS DISTINCT FROM leg.valuation_policy_sha256
           OR item.package_leg->>'valuation_policy_sha256' <> assessment_valuation_policy_sha256
    ) THEN
        RETURN false;
    END IF;
    RETURN true;
EXCEPTION WHEN OTHERS THEN
    RETURN false;
END;
$$;

LOCK TABLE public.option_package_assessments IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM public.option_package_assessments
        WHERE NOT public.option_package_assessment_matches_candidate(
            candidate_id, candidate_identity, matrix_id,
            valuation_policy_sha256, assessment_status,
            package_payload_text::jsonb, assessed_at
        )
    ) THEN
        RAISE EXCEPTION 'existing option package assessment disagrees with candidate';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.guard_option_package_assessment() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT public.option_package_assessment_matches_candidate(
        NEW.candidate_id, NEW.candidate_identity, NEW.matrix_id,
        NEW.valuation_policy_sha256, NEW.assessment_status,
        NEW.package_payload_text::jsonb, NEW.assessed_at
    ) THEN
        RAISE EXCEPTION 'option package assessment candidate or package disagrees';
    END IF;
    NEW.recorded_at := clock_timestamp();
    IF NEW.recorded_at < NEW.assessed_at THEN
        RAISE EXCEPTION 'option package assessment cannot be recorded before assessment';
    END IF;
    RETURN NEW;
END;
$$;