SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE rt.seed_manifest_members
    DROP CONSTRAINT seed_manifest_members_reference_kind_check;

ALTER TABLE rt.seed_manifest_members
    ADD CONSTRAINT seed_manifest_members_reference_kind_check
    CHECK (reference_kind IN (
        'SECURITY', 'FUNDAMENTALS', 'OPTION_CATALOG',
        'CORPORATE_ACTION_COVERAGE', 'OI_COVERAGE',
        'EARNINGS_COVERAGE', 'FOMC_COVERAGE', 'RATE_CURVE'
    ));
