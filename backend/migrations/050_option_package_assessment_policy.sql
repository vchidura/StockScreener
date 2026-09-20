SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.option_package_assessments) THEN
        RAISE EXCEPTION 'package assessment policy migration requires an empty ledger';
    END IF;
END;
$$;

ALTER TABLE public.option_package_assessments
    ADD COLUMN IF NOT EXISTS assessment_policy_version varchar(64),
    ADD COLUMN IF NOT EXISTS assessment_policy_sha256 character(64);

ALTER TABLE public.option_package_assessments
    ALTER COLUMN assessment_policy_version SET NOT NULL,
    ALTER COLUMN assessment_policy_sha256 SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='public.option_package_assessments'::regclass
          AND conname='ck_option_package_assessment_policy'
    ) THEN
        ALTER TABLE public.option_package_assessments
            ADD CONSTRAINT ck_option_package_assessment_policy CHECK ((
                assessment_policy_version <> ''
                AND assessment_policy_sha256 ~ '^[0-9a-f]{64}$'
                AND payload_text::jsonb->>'assessment_policy_version'
                    = assessment_policy_version
                AND payload_text::jsonb->>'assessment_policy_sha256'
                    = assessment_policy_sha256
            ) IS TRUE);
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_option_package_assessment_policy
    ON public.option_package_assessments(
        assessment_policy_sha256, assessed_at, recorded_at
    );