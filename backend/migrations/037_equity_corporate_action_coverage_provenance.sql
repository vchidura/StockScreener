ALTER TABLE equity_corporate_action_coverage
    ADD COLUMN IF NOT EXISTS availability_mode character varying(32)
        NOT NULL DEFAULT 'LIVE_OBSERVED',
    ADD COLUMN IF NOT EXISTS replay_available_at timestamp with time zone;

ALTER TABLE equity_corporate_action_coverage
    DROP CONSTRAINT IF EXISTS ck_equity_corporate_action_coverage_availability,
    ADD CONSTRAINT ck_equity_corporate_action_coverage_availability CHECK (
        availability_mode IN ('LIVE_OBSERVED', 'HISTORICAL_RECONSTRUCTED')
        AND (
            (availability_mode = 'LIVE_OBSERVED' AND replay_available_at IS NULL)
            OR (
                availability_mode = 'HISTORICAL_RECONSTRUCTED'
                AND replay_available_at IS NOT NULL
                AND replay_available_at <= first_observed_at
            )
        )
    );