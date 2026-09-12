-- Current Board revisions are unique within both strategy and runtime configuration.

DO $$
DECLARE
    legacy_constraint record;
BEGIN
    FOR legacy_constraint IN
                SELECT conname AS constraint_name
                FROM pg_constraint
                WHERE conrelid = 'option_board_publications'::regclass
                    AND contype = 'u'
                    AND pg_get_constraintdef(oid) =
                            'UNIQUE (scheduled_cycle, selector_sha256, strategy_policy_sha256)'
    LOOP
        EXECUTE format(
            'ALTER TABLE option_board_publications DROP CONSTRAINT %I',
            legacy_constraint.constraint_name
        );
    END LOOP;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'option_board_publications'::regclass
          AND conname = 'option_board_publications_cycle_configuration_key'
    ) THEN
        ALTER TABLE option_board_publications
            ADD CONSTRAINT option_board_publications_cycle_configuration_key
            UNIQUE (
                scheduled_cycle, selector_sha256,
                strategy_policy_sha256, configuration_sha256
            );
    END IF;
END
$$;