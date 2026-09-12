-- Immutable settlement marks keyed by valuation policy; daily facts remain the compatibility projection.

CREATE TABLE IF NOT EXISTS option_daily_contract_mark_revisions (
    mark_revision_id uuid PRIMARY KEY,
    contract_id bigint NOT NULL REFERENCES option_contract_catalog(contract_id),
    settlement_session date NOT NULL,
    underlying character varying(16) NOT NULL,
    mark_open numeric(20,8),
    mark_high numeric(20,8),
    mark_low numeric(20,8),
    mark_close numeric(20,8) NOT NULL,
    mark_volume bigint,
    mark_transaction_count bigint,
    mark_source character varying(48) NOT NULL,
    mark_adjusted boolean NOT NULL,
    mark_observed_at timestamp with time zone NOT NULL,
    valuation_policy_version character varying(64) NOT NULL,
    valuation_policy_sha256 character(64) NOT NULL,
    payload_sha256 character(64) NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    UNIQUE (contract_id, settlement_session, valuation_policy_sha256),
    CHECK (mark_close > 0),
    CHECK (mark_open IS NULL OR mark_open > 0),
    CHECK (mark_high IS NULL OR mark_high > 0),
    CHECK (mark_low IS NULL OR mark_low > 0),
    CHECK (mark_high IS NULL OR mark_low IS NULL OR mark_high >= mark_low),
    CHECK (mark_volume IS NULL OR mark_volume >= 0),
    CHECK (mark_transaction_count IS NULL OR mark_transaction_count >= 0),
    CHECK (valuation_policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS idx_option_daily_mark_revisions_research
    ON option_daily_contract_mark_revisions
        (valuation_policy_sha256, underlying, settlement_session DESC);

COMMENT ON TABLE option_daily_contract_mark_revisions IS
    'Immutable option settlement marks; multiple valuation policies may coexist per contract/session.';