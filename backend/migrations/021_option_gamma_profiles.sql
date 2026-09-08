-- Chain-level gamma exposure profiles.
-- Scalars are real columns so research and EXPLAIN ANALYZE never pay TOAST
-- decompression; only the per-strike curve lives in JSONB.

CREATE TABLE IF NOT EXISTS option_gamma_profiles (
    gamma_profile_id uuid PRIMARY KEY,
    matrix_id uuid NOT NULL REFERENCES option_analysis_runs(matrix_id),
    underlying character varying(16) NOT NULL,
    scope character varying(16) NOT NULL,
    market_data_time timestamp with time zone NOT NULL,
    first_observed_at timestamp with time zone NOT NULL,
    spot numeric(20,8) NOT NULL,

    dealer_convention character varying(40) NOT NULL,
    volatility_assumption character varying(32) NOT NULL,
    shares_per_contract integer NOT NULL,
    gamma_policy_version character varying(64) NOT NULL,
    gamma_policy_sha256 character(64) NOT NULL,

    -- Signed aggregates: interpretation under dealer_convention.
    net_gamma_shares_per_point double precision NOT NULL,
    net_gamma_notional_per_percent double precision NOT NULL,

    -- Unsigned aggregates: measurements, independent of any convention.
    absolute_gamma_notional_per_percent double precision NOT NULL,
    call_gamma_notional_per_percent double precision NOT NULL,
    put_gamma_notional_per_percent double precision NOT NULL,

    flip_spot numeric(20,8),
    regime_at_spot character varying(20) NOT NULL,
    sign_change_count integer NOT NULL,
    flip_search_low_spot numeric(20,8) NOT NULL,
    flip_search_high_spot numeric(20,8) NOT NULL,
    flip_grid_points integer NOT NULL,
    flip_reasons text[] NOT NULL DEFAULT ARRAY[]::text[],

    peak_gamma_strike numeric(20,8),
    strike_count integer NOT NULL,
    contributing_contract_count integer NOT NULL,
    eligible_contract_count integer NOT NULL,
    coverage_fraction double precision NOT NULL,

    strike_profile jsonb NOT NULL DEFAULT '[]'::jsonb,
    quality_reasons text[] NOT NULL DEFAULT ARRAY[]::text[],
    created_at timestamp with time zone NOT NULL DEFAULT now(),

    UNIQUE (matrix_id, scope, gamma_policy_sha256),
    CHECK (spot > 0),
    CHECK (shares_per_contract > 0),
    CHECK (scope IN ('TOTAL', 'ZERO_DTE', 'WEEKLY', 'MONTHLY')),
    CHECK (dealer_convention IN (
        'DEALER_LONG_CALLS_SHORT_PUTS', 'DEALER_SHORT_CALLS_LONG_PUTS'
    )),
    CHECK (volatility_assumption IN ('STICKY_STRIKE')),
    CHECK (regime_at_spot IN (
        'POSITIVE_GAMMA', 'NEGATIVE_GAMMA', 'UNDETERMINED'
    )),
    CHECK (gamma_policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (flip_spot IS NULL OR flip_spot > 0),
    CHECK (flip_search_high_spot >= flip_search_low_spot),
    CHECK (flip_grid_points >= 0),
    CHECK (sign_change_count >= 0),
    CHECK (absolute_gamma_notional_per_percent >= 0),
    CHECK (call_gamma_notional_per_percent >= 0),
    CHECK (put_gamma_notional_per_percent >= 0),
    CHECK (peak_gamma_strike IS NULL OR peak_gamma_strike > 0),
    CHECK (strike_count >= 0),
    CHECK (contributing_contract_count >= 0),
    CHECK (eligible_contract_count >= 0),
    CHECK (contributing_contract_count <= eligible_contract_count),
    CHECK (coverage_fraction >= 0 AND coverage_fraction <= 1),
    CHECK (jsonb_typeof(strike_profile) = 'array'),
    CHECK (jsonb_array_length(strike_profile) = strike_count)
);

-- Portal read: newest profile per underlying and scope for one policy.
CREATE INDEX IF NOT EXISTS idx_option_gamma_profiles_latest
    ON option_gamma_profiles (
        gamma_policy_sha256, underlying, scope, market_data_time DESC
    );

-- Matrix drill-down and FK maintenance.
CREATE INDEX IF NOT EXISTS idx_option_gamma_profiles_matrix
    ON option_gamma_profiles (matrix_id, scope);

-- Research cohorts: regime and flip behaviour over time.
CREATE INDEX IF NOT EXISTS idx_option_gamma_profiles_regime
    ON option_gamma_profiles (
        gamma_policy_sha256, regime_at_spot, market_data_time DESC
    )
    INCLUDE (underlying, scope, net_gamma_notional_per_percent, flip_spot);
