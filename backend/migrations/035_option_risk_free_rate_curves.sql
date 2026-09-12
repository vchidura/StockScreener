-- Point-in-time Treasury par-yield observations for maturity-matched option valuation.

CREATE TABLE IF NOT EXISTS option_risk_free_rate_observations (
    rate_observation_id uuid PRIMARY KEY,
    rate_date date NOT NULL,
    tenor_days integer NOT NULL CHECK (tenor_days > 0),
    annual_rate double precision NOT NULL
        CHECK (annual_rate > -0.10 AND annual_rate < 1.0),
    source character varying(64) NOT NULL,
    source_key character varying(128) NOT NULL,
    source_observed_at timestamp with time zone,
    first_observed_at timestamp with time zone NOT NULL,
    availability_mode character varying(32) NOT NULL
        CHECK (availability_mode IN ('LIVE_OBSERVED', 'HISTORICAL_RECONSTRUCTED')),
    replay_available_at timestamp with time zone,
    payload_sha256 character(64) NOT NULL
        CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    UNIQUE (source, source_key, first_observed_at),
    CHECK (
        (availability_mode = 'LIVE_OBSERVED' AND replay_available_at IS NULL)
        OR (
            availability_mode = 'HISTORICAL_RECONSTRUCTED'
            AND replay_available_at IS NOT NULL
            AND replay_available_at <= first_observed_at
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_option_rate_curve_asof
    ON option_risk_free_rate_observations
        (source, rate_date DESC, first_observed_at DESC, tenor_days);

COMMENT ON TABLE option_risk_free_rate_observations IS
    'Append-only Treasury par-yield facts; historical downloads remain explicitly reconstructed.';