SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE rt.seed_current
    DROP CONSTRAINT seed_current_scope_check,
    ADD CONSTRAINT seed_current_scope_check CHECK (
        scope IN (
            'MARKET_REFERENCES',
            'EQUITY_VALIDATION_REFERENCES',
            'ADVANCED_SPY_REFERENCES'
        )
    );
