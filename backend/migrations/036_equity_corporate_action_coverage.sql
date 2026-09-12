CREATE TABLE IF NOT EXISTS equity_corporate_action_coverage (
    coverage_id uuid PRIMARY KEY,
    action_type character varying(24) NOT NULL,
    ticker character varying(32) NOT NULL,
    window_start date NOT NULL,
    window_end date NOT NULL,
    source character varying(64) NOT NULL,
    source_key character varying(256) NOT NULL,
    first_observed_at timestamp with time zone NOT NULL,
    availability_mode character varying(32) NOT NULL,
    replay_available_at timestamp with time zone,
    payload_sha256 character(64) NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    UNIQUE (source, source_key, first_observed_at),
    CHECK (action_type IN ('SPLIT', 'DIVIDEND')),
    CHECK (window_end >= window_start),
    CHECK (
        availability_mode IN ('LIVE_OBSERVED', 'HISTORICAL_RECONSTRUCTED')
        AND (
            (availability_mode = 'LIVE_OBSERVED' AND replay_available_at IS NULL)
            OR (
                availability_mode = 'HISTORICAL_RECONSTRUCTED'
                AND replay_available_at IS NOT NULL
                AND replay_available_at <= first_observed_at
            )
        )
    ),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS idx_equity_corporate_action_coverage_asof
    ON equity_corporate_action_coverage
        (source, action_type, ticker, first_observed_at DESC);

CREATE TABLE IF NOT EXISTS equity_corporate_action_coverage_members (
    coverage_id uuid NOT NULL
        REFERENCES equity_corporate_action_coverage(coverage_id) ON DELETE CASCADE,
    corporate_action_id uuid NOT NULL
        REFERENCES equity_corporate_actions(corporate_action_id),
    PRIMARY KEY (coverage_id, corporate_action_id)
);

COMMENT ON TABLE equity_corporate_action_coverage IS
    'Complete provider windows needed before corporate-action absence is meaningful.';
COMMENT ON TABLE equity_corporate_action_coverage_members IS
    'Exact corporate-action revisions present in a complete provider observation.';