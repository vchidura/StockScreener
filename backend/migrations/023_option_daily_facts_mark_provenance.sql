-- Tie every mark value to its provenance.
--
-- Migration 022 tied only mark_close to mark_source, so a volume, transaction count or
-- open/high/low could be written with no declared source and no observed-at. Every other
-- value in this table states where it came from; these did not have to. That matters
-- most for volume, because the same quantity is available from two places with different
-- finality: the live chain snapshot carries a running intraday total, while the daily
-- aggregate carries the settled figure. A value with no provenance cannot be told apart.

ALTER TABLE option_daily_contract_facts
    ADD CONSTRAINT ck_option_daily_facts_mark_values_have_source
    CHECK (
        mark_source IS NOT NULL
        OR (
            mark_open IS NULL
            AND mark_high IS NULL
            AND mark_low IS NULL
            AND mark_volume IS NULL
            AND mark_transaction_count IS NULL
        )
    );
