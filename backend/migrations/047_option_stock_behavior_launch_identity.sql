SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.option_stock_behavior_assessments) THEN
        RAISE EXCEPTION 'launch identity migration requires an empty assessment ledger';
    END IF;
END;
$$;

ALTER TABLE public.option_stock_behavior_assessments
    ADD COLUMN IF NOT EXISTS launch_id varchar(64),
    ADD COLUMN IF NOT EXISTS launch_manifest_sha256 character(64);

ALTER TABLE public.option_stock_behavior_assessments
    ALTER COLUMN launch_id SET NOT NULL,
    ALTER COLUMN launch_manifest_sha256 SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.option_stock_behavior_assessments'::regclass
          AND conname = 'ck_option_stock_behavior_launch_contract'
    ) THEN
        ALTER TABLE public.option_stock_behavior_assessments
            ADD CONSTRAINT ck_option_stock_behavior_launch_contract CHECK ((
                launch_id ~ '^[a-z0-9][a-z0-9-]{7,63}$'
                AND launch_manifest_sha256 ~ '^[0-9a-f]{64}$'
                AND payload_text::jsonb->>'launch_id' = launch_id
                AND payload_text::jsonb->>'launch_manifest_sha256' = launch_manifest_sha256
            ) IS TRUE);
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_option_stock_behavior_launch
    ON public.option_stock_behavior_assessments
    (launch_manifest_sha256, decision_at, recorded_at);