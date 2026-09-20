SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS public.option_outcome_unavailable_evidence (
    evidence_id uuid PRIMARY KEY,
    candidate_id uuid NOT NULL REFERENCES public.option_strategy_candidates(candidate_id),
    candidate_identity character(64) NOT NULL CHECK (candidate_identity ~ '^[0-9a-f]{64}$'),
    valuation_policy_sha256 character(64) NOT NULL CHECK (valuation_policy_sha256 ~ '^[0-9a-f]{64}$'),
    measurement_type varchar(16) NOT NULL CHECK (
        measurement_type IN ('15MIN', '30MIN', '60MIN', 'CLOSE', 'NEXT_OPEN')
    ),
    checkpoint_at timestamptz NOT NULL CHECK (isfinite(checkpoint_at)),
    availability_deadline timestamptz NOT NULL CHECK (
        isfinite(availability_deadline) AND availability_deadline >= checkpoint_at
    ),
    required_contract_ids bigint[] NOT NULL CHECK (cardinality(required_contract_ids) BETWEEN 1 AND 4),
    observed_contract_ids bigint[] NOT NULL DEFAULT ARRAY[]::bigint[],
    missing_contract_ids bigint[] NOT NULL CHECK (cardinality(missing_contract_ids) >= 1),
    availability_policy_version varchar(64) NOT NULL,
    availability_policy_sha256 character(64) NOT NULL CHECK (availability_policy_sha256 ~ '^[0-9a-f]{64}$'),
    payload_text text NOT NULL CHECK (octet_length(payload_text) BETWEEN 2 AND 262144),
    payload_sha256 character(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(recorded_at)),
    CHECK (payload_sha256 = encode(sha256(convert_to(payload_text, 'UTF8')), 'hex')),
    CHECK ((
        jsonb_typeof(payload_text::jsonb) = 'object'
        AND payload_text::jsonb->>'schema_version' = 'option_outcome_measurement_assessment_v1'
        AND (payload_text::jsonb->>'candidate_id')::uuid = candidate_id
        AND payload_text::jsonb->>'candidate_identity_sha256' = candidate_identity
        AND payload_text::jsonb->>'valuation_policy_sha256' = valuation_policy_sha256
        AND payload_text::jsonb->>'measurement_type' = measurement_type
        AND (payload_text::jsonb->>'checkpoint_at')::timestamptz = checkpoint_at
        AND (payload_text::jsonb->>'evaluated_at')::timestamptz = availability_deadline
        AND (payload_text::jsonb->>'availability_deadline')::timestamptz = availability_deadline
        AND payload_text::jsonb->>'status' = 'UNAVAILABLE'
        AND payload_text::jsonb->>'availability_policy_version' = availability_policy_version
        AND payload_text::jsonb->>'availability_policy_sha256' = availability_policy_sha256
        AND payload_text::jsonb->'research_only' = 'true'::jsonb
        AND payload_text::jsonb->'execution_permission' = 'false'::jsonb
    ) IS TRUE)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_option_outcome_unavailable_contract
    ON public.option_outcome_unavailable_evidence(
        candidate_id, measurement_type, valuation_policy_sha256,
        availability_policy_sha256
    );

CREATE OR REPLACE FUNCTION public.guard_option_outcome_unavailable_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE candidate_identity_value text;
BEGIN
    SELECT candidate_identity INTO candidate_identity_value
    FROM public.option_strategy_candidates WHERE candidate_id=NEW.candidate_id;
    IF NOT FOUND OR candidate_identity_value <> NEW.candidate_identity THEN
        RAISE EXCEPTION 'outcome unavailable evidence candidate identity disagrees';
    END IF;
    NEW.recorded_at := clock_timestamp();
    IF NEW.recorded_at < NEW.availability_deadline THEN
        RAISE EXCEPTION 'outcome unavailable evidence cannot precede its deadline';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_guard_option_outcome_unavailable_evidence
    ON public.option_outcome_unavailable_evidence;
CREATE TRIGGER trg_guard_option_outcome_unavailable_evidence
    BEFORE INSERT ON public.option_outcome_unavailable_evidence
    FOR EACH ROW EXECUTE FUNCTION public.guard_option_outcome_unavailable_evidence();

CREATE OR REPLACE FUNCTION public.reject_option_outcome_unavailable_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'option outcome unavailable evidence is immutable';
END;
$$;
DROP TRIGGER IF EXISTS trg_option_outcome_unavailable_immutable
    ON public.option_outcome_unavailable_evidence;
CREATE TRIGGER trg_option_outcome_unavailable_immutable
    BEFORE UPDATE OR DELETE ON public.option_outcome_unavailable_evidence
    FOR EACH ROW EXECUTE FUNCTION public.reject_option_outcome_unavailable_mutation();
DROP TRIGGER IF EXISTS trg_option_outcome_unavailable_no_truncate
    ON public.option_outcome_unavailable_evidence;
CREATE TRIGGER trg_option_outcome_unavailable_no_truncate
    BEFORE TRUNCATE ON public.option_outcome_unavailable_evidence
    FOR EACH STATEMENT EXECUTE FUNCTION public.reject_option_outcome_unavailable_mutation();