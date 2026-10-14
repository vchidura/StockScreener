SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE rt.subscription_manifests
    ADD COLUMN manifest_kind varchar(32) NOT NULL DEFAULT 'MARKET_OPTIONS';

ALTER TABLE rt.subscription_manifests
    DROP CONSTRAINT subscription_manifests_check,
    DROP CONSTRAINT subscription_manifests_check1,
    DROP CONSTRAINT subscription_manifests_check2;

ALTER TABLE rt.subscription_manifests
    ADD CONSTRAINT subscription_manifests_manifest_kind_check
        CHECK (manifest_kind IN ('MARKET_OPTIONS', 'EQUITY_VALIDATION')),
    ADD CONSTRAINT subscription_manifests_option_underlying_count_check CHECK (
        (
            manifest_kind = 'MARKET_OPTIONS'
            AND option_underlying_count BETWEEN 1 AND stock_count
        ) OR (
            manifest_kind = 'EQUITY_VALIDATION'
            AND option_underlying_count = 0
        )
    ),
    ADD CONSTRAINT subscription_manifests_option_quote_contract_count_check CHECK (
        (
            manifest_kind = 'MARKET_OPTIONS'
            AND option_quote_contract_count BETWEEN option_underlying_count AND 1000
        ) OR (
            manifest_kind = 'EQUITY_VALIDATION'
            AND option_quote_contract_count = 0
        )
    ),
    ADD CONSTRAINT subscription_manifests_member_count_check CHECK (
        (
            manifest_kind = 'MARKET_OPTIONS'
            AND member_count >= stock_count * 3 + option_quote_contract_count * 2
        ) OR (
            manifest_kind = 'EQUITY_VALIDATION'
            AND member_count = stock_count * 3
        )
    );

ALTER TABLE rt.seed_current
    DROP CONSTRAINT seed_current_scope_check,
    ADD CONSTRAINT seed_current_scope_check CHECK (
        scope IN ('MARKET_REFERENCES', 'EQUITY_VALIDATION_REFERENCES')
    );