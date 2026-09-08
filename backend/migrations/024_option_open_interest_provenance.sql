-- Make the settlement/observation distinction enforceable and retain OI corrections.

ALTER TABLE option_daily_contract_facts
    ADD COLUMN open_interest_observed_session date,
    ADD COLUMN open_interest_revised_value bigint,
    ADD COLUMN open_interest_revised_observed_at timestamp with time zone,
    ADD COLUMN open_interest_revision_count integer NOT NULL DEFAULT 0;

UPDATE option_daily_contract_facts
SET open_interest_observed_session =
        (open_interest_observed_at AT TIME ZONE 'America/New_York')::date
WHERE open_interest IS NOT NULL;

ALTER TABLE option_daily_contract_facts
    ADD CONSTRAINT ck_option_daily_facts_oi_observed_session
    CHECK (
        (open_interest IS NULL) = (open_interest_observed_session IS NULL)
        AND (
            open_interest IS NULL
            OR settlement_session < open_interest_observed_session
        )
    ),
    ADD CONSTRAINT ck_option_daily_facts_oi_revision
    CHECK (
        open_interest_revision_count >= 0
        AND (open_interest_revised_value IS NULL)
            = (open_interest_revised_observed_at IS NULL)
        AND (open_interest_revision_count = 0)
            = (open_interest_revised_value IS NULL)
        AND (
            open_interest_revised_value IS NULL
            OR open_interest_revised_value >= 0
        )
        AND (
            open_interest_revised_observed_at IS NULL
            OR open_interest_revised_observed_at >= open_interest_observed_at
        )
    );

COMMENT ON COLUMN option_daily_contract_facts.open_interest_observed_session IS
    'Exchange session in which the prior-session OI settlement was observed.';

COMMENT ON COLUMN option_daily_contract_facts.open_interest IS
    'First observed value, retained unchanged for causal replay.';

COMMENT ON COLUMN option_daily_contract_facts.open_interest_revised_value IS
    'Latest conflicting provider observation; NULL when every observation agreed.';