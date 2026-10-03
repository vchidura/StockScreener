SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';

DO $$
DECLARE coverage_constraint record;
BEGIN
    FOR coverage_constraint IN
        SELECT conname FROM pg_constraint
        WHERE conrelid='public.option_board_publications'::regclass
          AND contype='c'
          AND pg_get_expr(conbin, conrelid)='(covered_underlying_count = expected_underlying_count)'
    LOOP
        EXECUTE format('ALTER TABLE public.option_board_publications DROP CONSTRAINT %I', coverage_constraint.conname);
    END LOOP;
END;
$$;

ALTER TABLE public.option_board_publications
    ADD CONSTRAINT option_board_publications_versioned_coverage CHECK (
        covered_underlying_count > 0
        AND covered_underlying_count <= expected_underlying_count
        AND (
            (selector_version <> 'option_detector_run_v3'
             AND covered_underlying_count = expected_underlying_count)
            OR (
                selector_version = 'option_detector_run_v3'
                AND selection_evidence->>'kind' = 'DUAL_ORIGIN_COMPLETE_RUN'
                AND (selection_evidence->>'payload_text')::jsonb->>'schema_version' = 'option_detector_run_v3'
                AND jsonb_array_length((selection_evidence->>'payload_text')::jsonb->'expected_underlyers') = expected_underlying_count
                AND jsonb_array_length((selection_evidence->>'payload_text')::jsonb->'source_matrices') = covered_underlying_count
                AND jsonb_array_length((selection_evidence->>'payload_text')::jsonb->'unavailable_underlyers') = expected_underlying_count - covered_underlying_count
            ) IS TRUE
        )
    );

INSERT INTO public.schema_migrations(version) VALUES ('062_option_detector_partial_coverage') ON CONFLICT DO NOTHING;