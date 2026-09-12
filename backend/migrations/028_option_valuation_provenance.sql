-- Immutable valuation-policy provenance for normalized marks and derived legs.

ALTER TABLE option_chain_snapshots
    ADD COLUMN IF NOT EXISTS valuation_policy_version character varying(64),
    ADD COLUMN IF NOT EXISTS valuation_policy_sha256 character(64);

ALTER TABLE option_candidate_legs
    ADD COLUMN IF NOT EXISTS valuation_policy_version character varying(64),
    ADD COLUMN IF NOT EXISTS valuation_policy_sha256 character(64);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'option_chain_snapshots'::regclass
          AND conname = 'option_chain_snapshots_valuation_policy_check'
    ) THEN
        ALTER TABLE option_chain_snapshots
            ADD CONSTRAINT option_chain_snapshots_valuation_policy_check
            CHECK ((valuation_policy_version IS NULL) = (valuation_policy_sha256 IS NULL));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'option_chain_snapshots'::regclass
          AND conname = 'option_chain_snapshots_valuation_policy_sha256_check'
    ) THEN
        ALTER TABLE option_chain_snapshots
            ADD CONSTRAINT option_chain_snapshots_valuation_policy_sha256_check
            CHECK (valuation_policy_sha256 IS NULL OR valuation_policy_sha256 ~ '^[0-9a-f]{64}$');
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'option_candidate_legs'::regclass
          AND conname = 'option_candidate_legs_valuation_policy_check'
    ) THEN
        ALTER TABLE option_candidate_legs
            ADD CONSTRAINT option_candidate_legs_valuation_policy_check
            CHECK ((valuation_policy_version IS NULL) = (valuation_policy_sha256 IS NULL));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'option_candidate_legs'::regclass
          AND conname = 'option_candidate_legs_valuation_policy_sha256_check'
    ) THEN
        ALTER TABLE option_candidate_legs
            ADD CONSTRAINT option_candidate_legs_valuation_policy_sha256_check
            CHECK (valuation_policy_sha256 IS NULL OR valuation_policy_sha256 ~ '^[0-9a-f]{64}$');
    END IF;
END
$$;