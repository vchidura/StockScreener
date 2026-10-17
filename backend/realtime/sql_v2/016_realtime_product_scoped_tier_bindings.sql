SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE rt.provider_tier_cohorts
    DROP CONSTRAINT provider_tier_cohorts_check;

ALTER TABLE rt.provider_tier_cohorts
    ALTER COLUMN options_tier DROP NOT NULL,
    ADD CONSTRAINT provider_tier_cohorts_stocks_tier_check CHECK (
        stocks_tier = provider_tier
    ),
    ADD CONSTRAINT provider_tier_cohorts_options_tier_check CHECK (
        options_tier IS NULL
        OR options_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')
    );
