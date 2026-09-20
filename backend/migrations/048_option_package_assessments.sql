SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS public.option_package_assessments (
    assessment_id uuid PRIMARY KEY,
    candidate_id uuid NOT NULL REFERENCES public.option_strategy_candidates(candidate_id),
    candidate_identity character(64) NOT NULL CHECK (candidate_identity ~ '^[0-9a-f]{64}$'),
    matrix_id uuid NOT NULL REFERENCES public.option_analysis_runs(matrix_id),
    valuation_policy_sha256 character(64) NOT NULL CHECK (valuation_policy_sha256 ~ '^[0-9a-f]{64}$'),
    assessment_status varchar(16) NOT NULL CHECK (assessment_status IN ('READY', 'UNAVAILABLE')),
    package_terms_sha256 character(64) CHECK (package_terms_sha256 ~ '^[0-9a-f]{64}$'),
    package_payload_text text CHECK (
        package_payload_text IS NULL
        OR octet_length(package_payload_text) BETWEEN 2 AND 262144
    ),
    assessed_at timestamptz NOT NULL CHECK (isfinite(assessed_at)),
    payload_text text NOT NULL CHECK (octet_length(payload_text) BETWEEN 2 AND 262144),
    payload_sha256 character(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp() CHECK (isfinite(recorded_at)),
    CHECK ((assessment_status = 'READY') = (
        package_terms_sha256 IS NOT NULL AND package_payload_text IS NOT NULL
    )),
    CHECK (
        package_terms_sha256 IS NULL
        OR package_terms_sha256 = encode(
            sha256(convert_to(package_payload_text, 'UTF8')), 'hex'
        )
    ),
    CHECK (payload_sha256 = encode(sha256(convert_to(payload_text, 'UTF8')), 'hex')),
    CHECK ((
        jsonb_typeof(payload_text::jsonb) = 'object'
        AND payload_text::jsonb->>'schema_version' = 'option_package_assessment_v1'
        AND (payload_text::jsonb->>'candidate_id')::uuid = candidate_id
        AND payload_text::jsonb->>'candidate_identity_sha256' = candidate_identity
        AND (payload_text::jsonb->>'option_matrix_id')::uuid = matrix_id
        AND payload_text::jsonb->>'valuation_policy_sha256' = valuation_policy_sha256
        AND payload_text::jsonb->>'status' = assessment_status
        AND (payload_text::jsonb->>'assessed_at')::timestamptz = assessed_at
        AND payload_text::jsonb->'research_only' = 'true'::jsonb
        AND payload_text::jsonb->'execution_permission' = 'false'::jsonb
        AND CASE
            WHEN package_terms_sha256 IS NULL THEN
                payload_text::jsonb->'package' = 'null'::jsonb
                AND payload_text::jsonb->'package_terms_sha256' = 'null'::jsonb
            ELSE
                payload_text::jsonb->>'package_terms_sha256' = package_terms_sha256
                AND payload_text::jsonb->'package' = package_payload_text::jsonb
        END
    ) IS TRUE)
);

CREATE INDEX IF NOT EXISTS idx_option_package_assessment_candidate
    ON public.option_package_assessments(candidate_id, assessed_at DESC, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_option_package_assessment_status
    ON public.option_package_assessments(
        valuation_policy_sha256, assessment_status, assessed_at DESC
    );

CREATE OR REPLACE FUNCTION public.guard_option_package_assessment() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE candidate public.option_strategy_candidates%ROWTYPE;
BEGIN
    SELECT * INTO candidate FROM public.option_strategy_candidates
    WHERE candidate_id = NEW.candidate_id;
    IF NOT FOUND
       OR candidate.candidate_identity <> NEW.candidate_identity
       OR candidate.matrix_id <> NEW.matrix_id
       OR candidate.observed_time <> NEW.assessed_at THEN
        RAISE EXCEPTION 'option package assessment candidate identity or clock disagrees';
    END IF;
    NEW.recorded_at := clock_timestamp();
    IF NEW.recorded_at < NEW.assessed_at THEN
        RAISE EXCEPTION 'option package assessment cannot be recorded before assessment';
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_guard_option_package_assessment
    ON public.option_package_assessments;
CREATE TRIGGER trg_guard_option_package_assessment
    BEFORE INSERT ON public.option_package_assessments
    FOR EACH ROW EXECUTE FUNCTION public.guard_option_package_assessment();

CREATE OR REPLACE FUNCTION public.reject_option_package_assessment_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'option package assessments are immutable';
END;
$$;
DROP TRIGGER IF EXISTS trg_option_package_assessment_immutable
    ON public.option_package_assessments;
CREATE TRIGGER trg_option_package_assessment_immutable
    BEFORE UPDATE OR DELETE ON public.option_package_assessments
    FOR EACH ROW EXECUTE FUNCTION public.reject_option_package_assessment_mutation();
DROP TRIGGER IF EXISTS trg_option_package_assessment_no_truncate
    ON public.option_package_assessments;
CREATE TRIGGER trg_option_package_assessment_no_truncate
    BEFORE TRUNCATE ON public.option_package_assessments
    FOR EACH STATEMENT EXECUTE FUNCTION public.reject_option_package_assessment_mutation();