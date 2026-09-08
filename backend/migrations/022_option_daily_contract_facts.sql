-- Daily per-contract provider facts: open interest and settlement marks.
--
-- WHY THIS TABLE EXISTS
-- Open interest is the one input that cannot be acquired retroactively at any
-- entitlement tier. It is published only as "the quantity held at the end of the last
-- trading day" on the live chain snapshot, which accepts no as-of parameter. Every
-- session that is not captured is lost permanently. Today it survives only inside
-- option_raw_batch_pages JSON, because its only normalized home lives in the derived
-- layer and is purged with it.
--
-- Settlement marks share the same grain and the same retention rules, so they live
-- here too rather than in a second table. They are backfillable from historical daily
-- aggregates, so their columns stay NULL until a backfill runs. Rows may therefore
-- carry either column group or both, which is why neither is NOT NULL.
--
-- THE AS-OF DISTINCTION
-- settlement_session is the session the open interest DESCRIBES; observed_at is when it
-- was captured. Capturing during session T yields the settlement of session T-1. Storing
-- the capture date as the fact date would shift every value one session, and gamma
-- exposure is built entirely on open interest, so every profile would shift with it.

CREATE TABLE IF NOT EXISTS option_daily_contract_facts (
    contract_id bigint NOT NULL REFERENCES option_contract_catalog(contract_id),
    settlement_session date NOT NULL,
    underlying character varying(16) NOT NULL,

    open_interest bigint,
    open_interest_source character varying(32),
    open_interest_observed_at timestamp with time zone,
    open_interest_batch_id uuid,

    mark_open numeric(20,8),
    mark_high numeric(20,8),
    mark_low numeric(20,8),
    mark_close numeric(20,8),
    mark_volume bigint,
    mark_transaction_count bigint,
    mark_source character varying(32),
    mark_observed_at timestamp with time zone,

    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),

    PRIMARY KEY (contract_id, settlement_session),

    -- A row must assert something; an empty row is a bug, not a fact.
    CHECK (open_interest IS NOT NULL OR mark_close IS NOT NULL),
    CHECK (open_interest IS NULL OR open_interest >= 0),
    CHECK (
        (open_interest IS NULL)
        = (open_interest_source IS NULL)
    ),
    CHECK (
        (open_interest IS NULL)
        = (open_interest_observed_at IS NULL)
    ),
    CHECK (open_interest_source IS NULL OR open_interest_source IN (
        'PROVIDER_CHAIN_SNAPSHOT'
    )),
    CHECK (mark_source IS NULL OR mark_source IN (
        'PROVIDER_DAILY_AGGREGATE'
    )),
    CHECK ((mark_close IS NULL) = (mark_source IS NULL)),
    CHECK ((mark_close IS NULL) = (mark_observed_at IS NULL)),
    CHECK (mark_open IS NULL OR mark_open > 0),
    CHECK (mark_high IS NULL OR mark_high > 0),
    CHECK (mark_low IS NULL OR mark_low > 0),
    CHECK (mark_close IS NULL OR mark_close > 0),
    CHECK (mark_volume IS NULL OR mark_volume >= 0),
    CHECK (mark_transaction_count IS NULL OR mark_transaction_count >= 0),
    CHECK (
        mark_high IS NULL OR mark_low IS NULL OR mark_high >= mark_low
    )
);

-- Session sweep for the change-in-open-interest detector, which compares one settlement
-- session against the prior one across a whole underlying at once.
CREATE INDEX IF NOT EXISTS idx_option_daily_facts_session
    ON option_daily_contract_facts (underlying, settlement_session)
    INCLUDE (contract_id, open_interest);

-- Per-contract history for a single series, used by backfills and by replay.
CREATE INDEX IF NOT EXISTS idx_option_daily_facts_contract
    ON option_daily_contract_facts (contract_id, settlement_session DESC);

-- Partial index so the marks backfill can find sessions it has not yet populated
-- without scanning the open-interest-only rows that will always dominate the table.
CREATE INDEX IF NOT EXISTS idx_option_daily_facts_missing_mark
    ON option_daily_contract_facts (underlying, settlement_session)
    WHERE mark_close IS NULL;

COMMENT ON TABLE option_daily_contract_facts IS
    'Provider evidence, one row per contract per settlement session. Retained across '
    'derived-layer purges: historical open interest is not purchasable at any tier.';

COMMENT ON COLUMN option_daily_contract_facts.settlement_session IS
    'The session the open interest describes, not the session it was captured in.';
