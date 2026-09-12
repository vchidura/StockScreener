-- Permit immutable strategy-context revisions when strategy policy identity changes.

ALTER TABLE option_context_snapshots
    DROP CONSTRAINT IF EXISTS option_context_snapshots_matrix_id_key;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'option_context_snapshots'::regclass
          AND conname = 'option_context_snapshots_matrix_policy_key'
    ) THEN
        ALTER TABLE option_context_snapshots
            ADD CONSTRAINT option_context_snapshots_matrix_policy_key
            UNIQUE (matrix_id, policy_sha256);
    END IF;
END
$$;