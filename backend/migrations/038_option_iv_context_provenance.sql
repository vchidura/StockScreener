ALTER TABLE option_iv_context_snapshots
    DROP CONSTRAINT IF EXISTS option_iv_context_snapshots_matrix_id_expiration_bucket_key,
    ADD COLUMN IF NOT EXISTS settlement_valuation_policy_version character varying(64),
    ADD COLUMN IF NOT EXISTS settlement_valuation_policy_sha256 character(64),
    ADD COLUMN IF NOT EXISTS rate_source character varying(64),
    ADD COLUMN IF NOT EXISTS rate_observation_ids uuid[] NOT NULL DEFAULT ARRAY[]::uuid[],
    ADD COLUMN IF NOT EXISTS dividend_source character varying(64),
    ADD COLUMN IF NOT EXISTS dividend_coverage_ids uuid[] NOT NULL DEFAULT ARRAY[]::uuid[],
    ADD COLUMN IF NOT EXISTS dividend_action_ids uuid[] NOT NULL DEFAULT ARRAY[]::uuid[],
    ADD COLUMN IF NOT EXISTS history_availability_mode character varying(32)
        NOT NULL DEFAULT 'HISTORICAL_RECONSTRUCTED';

ALTER TABLE option_iv_context_snapshots
    ADD CONSTRAINT uq_option_iv_context_policy
        UNIQUE (
            matrix_id, expiration_bucket, calculation_version,
            settlement_valuation_policy_sha256
        ),
    ADD CONSTRAINT ck_option_iv_context_policy_sha CHECK (
        settlement_valuation_policy_sha256 IS NULL
        OR settlement_valuation_policy_sha256 ~ '^[0-9a-f]{64}$'
    ),
    ADD CONSTRAINT ck_option_iv_context_history_mode CHECK (
        history_availability_mode = 'HISTORICAL_RECONSTRUCTED'
    );