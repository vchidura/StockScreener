BEGIN;
DO $$
DECLARE prior_expression text;
BEGIN
    SELECT pg_get_expr(conbin, conrelid) INTO prior_expression
    FROM pg_constraint WHERE conrelid='public.equity_portal_snapshots'::regclass
      AND conname='ck_equity_portal_snapshot_type';
    IF prior_expression IS NULL THEN
        RAISE EXCEPTION 'Expected portal snapshot type constraint is missing';
    END IF;
    IF position('SCREENING_DAILY_V1' in prior_expression) = 0 THEN
        ALTER TABLE public.equity_portal_snapshots DROP CONSTRAINT ck_equity_portal_snapshot_type;
        EXECUTE 'ALTER TABLE public.equity_portal_snapshots ADD CONSTRAINT ck_equity_portal_snapshot_type CHECK ('
            || prior_expression || ' OR snapshot_type = ''SCREENING_DAILY_V1'')';
    END IF;
END $$;
INSERT INTO public.schema_migrations(version) VALUES ('041_screening_snapshot_type') ON CONFLICT DO NOTHING;
COMMIT;