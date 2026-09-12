-- Distinguish nominal-strike-compatible settlement marks from legacy adjusted marks.

ALTER TABLE option_daily_contract_facts
    DROP CONSTRAINT IF EXISTS option_daily_contract_facts_mark_source_check;

ALTER TABLE option_daily_contract_facts
    DROP CONSTRAINT IF EXISTS ck_option_daily_mark_valuation_pair,
    DROP CONSTRAINT IF EXISTS ck_option_daily_mark_adjustment_provenance,
    DROP CONSTRAINT IF EXISTS ck_option_daily_mark_valuation_sha256,
    DROP CONSTRAINT IF EXISTS ck_option_daily_mark_policy_contract;

ALTER TABLE option_daily_contract_facts
    ALTER COLUMN mark_source TYPE character varying(48),
    ADD COLUMN IF NOT EXISTS mark_adjusted boolean,
    ADD COLUMN IF NOT EXISTS mark_valuation_policy_version character varying(64),
    ADD COLUMN IF NOT EXISTS mark_valuation_policy_sha256 character(64);

ALTER TABLE option_daily_contract_facts
    ADD CONSTRAINT option_daily_contract_facts_mark_source_check
    CHECK (mark_source IS NULL OR mark_source IN (
        'PROVIDER_DAILY_AGGREGATE',
        'PROVIDER_DAILY_AGGREGATE_UNADJUSTED'
    ));

ALTER TABLE option_daily_contract_facts
    ADD CONSTRAINT ck_option_daily_mark_valuation_pair
    CHECK (
        (mark_valuation_policy_version IS NULL)
        = (mark_valuation_policy_sha256 IS NULL)
    ),
    ADD CONSTRAINT ck_option_daily_mark_adjustment_provenance
    CHECK (mark_adjusted IS NULL OR mark_close IS NOT NULL),
    ADD CONSTRAINT ck_option_daily_mark_valuation_sha256
    CHECK (
        mark_valuation_policy_sha256 IS NULL
        OR mark_valuation_policy_sha256 ~ '^[0-9a-f]{64}$'
    ),
    ADD CONSTRAINT ck_option_daily_mark_policy_contract
    CHECK (
        mark_source IS NULL
        OR (
            mark_source = 'PROVIDER_DAILY_AGGREGATE'
            AND mark_adjusted IS NULL
            AND mark_valuation_policy_version IS NULL
        )
        OR (
            mark_source = 'PROVIDER_DAILY_AGGREGATE_UNADJUSTED'
            AND mark_adjusted = FALSE
            AND mark_valuation_policy_version IS NOT NULL
            AND mark_valuation_policy_sha256 IS NOT NULL
        )
    );

COMMENT ON COLUMN option_daily_contract_facts.mark_adjusted IS
    'NULL for legacy rows with unknown request semantics; FALSE for nominal-strike-compatible marks.';