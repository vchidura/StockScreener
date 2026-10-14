SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE ref.security_revisions
    ADD COLUMN active boolean,
    ADD COLUMN cik varchar(32),
    ADD COLUMN composite_figi varchar(32),
    ADD COLUMN share_class_figi varchar(32),
    ADD COLUMN sic_code varchar(16),
    ADD COLUMN sic_description text,
    ADD COLUMN sector text,
    ADD COLUMN industry text,
    ADD COLUMN list_date date,
    ADD COLUMN delisted_date date,
    ADD COLUMN weighted_shares numeric(28,8),
    ADD COLUMN free_float numeric(28,8),
    ADD COLUMN free_float_percent double precision,
    ADD COLUMN market_cap numeric(28,8);

ALTER TABLE ref.security_revisions
    ADD CONSTRAINT ck_ref_security_weighted_shares
        CHECK (weighted_shares IS NULL OR weighted_shares >= 0),
    ADD CONSTRAINT ck_ref_security_free_float
        CHECK (free_float IS NULL OR free_float >= 0),
    ADD CONSTRAINT ck_ref_security_free_float_percent
        CHECK (free_float_percent IS NULL OR (free_float_percent >= 0 AND free_float_percent <= 100)),
    ADD CONSTRAINT ck_ref_security_market_cap
        CHECK (market_cap IS NULL OR market_cap >= 0),
    ADD CONSTRAINT ck_ref_security_delisted_date
        CHECK (delisted_date IS NULL OR list_date IS NULL OR delisted_date >= list_date);
