-- Polygon options Phase 2 strategy, candidate, signal, and decision-evidence schema.
-- Developer quote fields remain null. Paper execution remains outside this migration.

ALTER TABLE option_work_items
    DROP CONSTRAINT IF EXISTS option_work_items_stage_check;
ALTER TABLE option_work_items
    ADD CONSTRAINT option_work_items_stage_check CHECK (
        stage IN (
            'NORMALIZE', 'ANALYZE', 'STRATEGY', 'ARCHIVE',
            'TRADE_BACKFILL', 'CLASSIFY_TRADES'
        )
    );

CREATE TABLE IF NOT EXISTS option_strategy_registry (
    strategy_name              VARCHAR(64) NOT NULL,
    strategy_version           VARCHAR(32) NOT NULL,
    display_name               VARCHAR(96) NOT NULL,
    strategy_archetype         VARCHAR(48) NOT NULL,
    persona_tags               TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    allowed_structure_types    TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    allowed_risk_classes       TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    presentation_metadata      JSONB NOT NULL DEFAULT '{}'::jsonb,
    policy_sha256              CHAR(64) NOT NULL CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    enabled                    BOOLEAN NOT NULL DEFAULT TRUE,
    effective_from             TIMESTAMPTZ NOT NULL,
    effective_to               TIMESTAMPTZ,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (strategy_name, strategy_version),
    CHECK (cardinality(persona_tags) > 0),
    CHECK (cardinality(allowed_structure_types) > 0),
    CHECK (cardinality(allowed_risk_classes) > 0),
    CHECK (jsonb_typeof(presentation_metadata) = 'object'),
    CHECK (effective_to IS NULL OR effective_to > effective_from)
);

CREATE TABLE IF NOT EXISTS option_market_events (
    market_event_id            UUID PRIMARY KEY,
    event_type                 VARCHAR(32) NOT NULL CHECK (
        event_type IN ('EARNINGS', 'FED_RATE_DECISION')
    ),
    affected_underlying        VARCHAR(16),
    scheduled_time             TIMESTAMPTZ NOT NULL,
    source                     VARCHAR(64) NOT NULL,
    source_key                 VARCHAR(256) NOT NULL,
    announcement_time          TIMESTAMPTZ,
    first_observed_at          TIMESTAMPTZ NOT NULL,
    revised_observed_at        TIMESTAMPTZ,
    confidence                 VARCHAR(24) NOT NULL CHECK (
        confidence IN ('CONFIRMED', 'ESTIMATED', 'UNKNOWN')
    ),
    status                     VARCHAR(24) NOT NULL CHECK (
        status IN ('SCHEDULED', 'COMPLETED', 'CANCELED', 'REVISED')
    ),
    payload_sha256             CHAR(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source, source_key, first_observed_at),
    CHECK (announcement_time IS NULL OR first_observed_at >= announcement_time),
    CHECK (revised_observed_at IS NULL OR revised_observed_at >= first_observed_at),
    CHECK (
        (event_type = 'FED_RATE_DECISION' AND affected_underlying IS NULL) OR
        (event_type = 'EARNINGS' AND affected_underlying IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_option_market_events_context
    ON option_market_events (event_type, affected_underlying, scheduled_time, first_observed_at);

CREATE TABLE IF NOT EXISTS option_context_snapshots (
    context_snapshot_id        UUID PRIMARY KEY,
    matrix_id                  UUID NOT NULL UNIQUE REFERENCES option_analysis_runs(matrix_id),
    underlying                 VARCHAR(16) NOT NULL,
    market_data_time           TIMESTAMPTZ NOT NULL,
    observed_time              TIMESTAMPTZ NOT NULL,
    status                     VARCHAR(24) NOT NULL CHECK (
        status IN ('COMPLETE', 'DEGRADED', 'FAILED')
    ),
    daily_close                NUMERIC(20,8),
    daily_ema_50               NUMERIC(20,8),
    daily_ema_50_input_bars    INTEGER CHECK (daily_ema_50_input_bars IS NULL OR daily_ema_50_input_bars >= 0),
    daily_window_start         TIMESTAMPTZ,
    hourly_close               NUMERIC(20,8),
    hourly_ema_20              NUMERIC(20,8),
    hourly_ema_20_input_bars   INTEGER CHECK (hourly_ema_20_input_bars IS NULL OR hourly_ema_20_input_bars >= 0),
    hourly_window_start        TIMESTAMPTZ,
    five_minute_close          NUMERIC(20,8),
    five_minute_ema            NUMERIC(20,8),
    five_minute_volume         BIGINT,
    five_minute_volume_mean    DOUBLE PRECISION,
    five_minute_input_bars     INTEGER CHECK (five_minute_input_bars IS NULL OR five_minute_input_bars >= 0),
    five_minute_window_start   TIMESTAMPTZ,
    trend_state                VARCHAR(24) CHECK (
        trend_state IS NULL OR trend_state IN ('BULLISH', 'BEARISH', 'NEUTRAL')
    ),
    earnings_blackout_state    VARCHAR(32) NOT NULL,
    fed_blackout_state         VARCHAR(32) NOT NULL,
    quote_spread_state         VARCHAR(32) NOT NULL DEFAULT 'NOT_AVAILABLE',
    reason_codes               TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    source_bar_keys            JSONB NOT NULL DEFAULT '[]'::jsonb,
    policy_version             VARCHAR(64) NOT NULL,
    policy_sha256              CHAR(64) NOT NULL CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (observed_time >= market_data_time),
    CHECK (jsonb_typeof(source_bar_keys) = 'array')
);

CREATE INDEX IF NOT EXISTS idx_option_context_snapshots_lookup
    ON option_context_snapshots (underlying, market_data_time DESC, observed_time DESC);

CREATE TABLE IF NOT EXISTS option_iv_context_snapshots (
    iv_context_id              UUID PRIMARY KEY,
    matrix_id                  UUID NOT NULL REFERENCES option_analysis_runs(matrix_id),
    underlying                 VARCHAR(16) NOT NULL,
    expiration_bucket         VARCHAR(16) NOT NULL,
    current_comparable_iv      DOUBLE PRECISION,
    lookback_start_date        DATE,
    lookback_end_date          DATE,
    sample_count               INTEGER CHECK (sample_count IS NULL OR sample_count >= 0),
    coverage_fraction          DOUBLE PRECISION CHECK (
        coverage_fraction IS NULL OR (coverage_fraction >= 0 AND coverage_fraction <= 1)
    ),
    minimum_iv                 DOUBLE PRECISION,
    maximum_iv                 DOUBLE PRECISION,
    range_position_rank        DOUBLE PRECISION CHECK (
        range_position_rank IS NULL OR (range_position_rank >= 0 AND range_position_rank <= 1)
    ),
    empirical_percentile       DOUBLE PRECISION CHECK (
        empirical_percentile IS NULL OR (empirical_percentile >= 0 AND empirical_percentile <= 1)
    ),
    calculation_version        VARCHAR(64) NOT NULL,
    first_observed_time        TIMESTAMPTZ NOT NULL,
    null_reason_codes          TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (matrix_id, expiration_bucket),
    CHECK (lookback_end_date IS NULL OR lookback_start_date IS NULL OR lookback_end_date >= lookback_start_date),
    CHECK (maximum_iv IS NULL OR minimum_iv IS NULL OR maximum_iv >= minimum_iv)
);

CREATE TABLE IF NOT EXISTS option_decision_evidence (
    evidence_id                UUID PRIMARY KEY,
    matrix_id                  UUID NOT NULL REFERENCES option_analysis_runs(matrix_id),
    decision_type              VARCHAR(24) NOT NULL CHECK (
        decision_type IN ('CANDIDATE', 'SIGNAL', 'SUPPRESSION')
    ),
    strategy_name              VARCHAR(64) NOT NULL,
    strategy_version           VARCHAR(32) NOT NULL,
    normalized_legs            JSONB NOT NULL DEFAULT '[]'::jsonb,
    underlying_mark            NUMERIC(20,8),
    market_data_time           TIMESTAMPTZ NOT NULL,
    source_observation_time    TIMESTAMPTZ NOT NULL,
    context_snapshot_id        UUID REFERENCES option_context_snapshots(context_snapshot_id),
    context                    JSONB NOT NULL DEFAULT '{}'::jsonb,
    rank_components            JSONB NOT NULL DEFAULT '{}'::jsonb,
    trigger_values             JSONB NOT NULL DEFAULT '{}'::jsonb,
    quality_flags              TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    policy_sha256              CHAR(64) NOT NULL CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    raw_file_ids               UUID[] NOT NULL DEFAULT ARRAY[]::UUID[],
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (source_observation_time >= market_data_time),
    CHECK (jsonb_typeof(normalized_legs) = 'array'),
    CHECK (jsonb_typeof(context) = 'object'),
    CHECK (jsonb_typeof(rank_components) = 'object'),
    CHECK (jsonb_typeof(trigger_values) = 'object')
);

CREATE TABLE IF NOT EXISTS option_strategy_candidates (
    candidate_id               UUID PRIMARY KEY,
    candidate_identity         CHAR(64) NOT NULL UNIQUE CHECK (candidate_identity ~ '^[0-9a-f]{64}$'),
    matrix_id                  UUID NOT NULL REFERENCES option_analysis_runs(matrix_id),
    strategy_name              VARCHAR(64) NOT NULL,
    strategy_version           VARCHAR(32) NOT NULL,
    underlying                 VARCHAR(16) NOT NULL,
    candidate_kind             VARCHAR(32) NOT NULL CHECK (
        candidate_kind IN ('RESEARCH_ONLY', 'SINGLE_CONTRACT', 'MULTI_LEG')
    ),
    strategy_archetype         VARCHAR(48) NOT NULL,
    persona_tags               TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    structure_type             VARCHAR(64) NOT NULL,
    structure_risk_class       VARCHAR(32) NOT NULL CHECK (
        structure_risk_class IN (
            'RESEARCH_CONTEXT', 'CASH_SECURED',
            'DEFINED_RISK_CREDIT', 'PREMIUM_AT_RISK_DEBIT'
        )
    ),
    expiration_date            DATE,
    candidate_rank             INTEGER NOT NULL CHECK (candidate_rank > 0),
    status                     VARCHAR(16) NOT NULL CHECK (
        status IN ('SELECTED', 'SUPPRESSED', 'REJECTED')
    ),
    primary_metric_name        VARCHAR(64),
    primary_metric_value       DOUBLE PRECISION,
    rank_components            JSONB NOT NULL DEFAULT '{}'::jsonb,
    primary_evidence           JSONB NOT NULL DEFAULT '{}'::jsonb,
    net_premium                NUMERIC(20,8),
    collateral_required        NUMERIC(20,8) CHECK (collateral_required IS NULL OR collateral_required >= 0),
    capital_at_risk            NUMERIC(20,8) CHECK (capital_at_risk IS NULL OR capital_at_risk >= 0),
    maximum_profit             NUMERIC(20,8),
    maximum_loss               NUMERIC(20,8) CHECK (maximum_loss IS NULL OR maximum_loss >= 0),
    return_on_collateral       DOUBLE PRECISION,
    return_on_risk             DOUBLE PRECISION,
    breakevens                 NUMERIC(20,8)[] NOT NULL DEFAULT ARRAY[]::NUMERIC(20,8)[],
    execution_eligibility      VARCHAR(24) CHECK (
        execution_eligibility IS NULL OR
        execution_eligibility IN ('PAPER_PROXY', 'LIVE_CANDIDATE')
    ),
    reason_codes               TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    management_policy_version  VARCHAR(64),
    management_policy          JSONB NOT NULL DEFAULT '{}'::jsonb,
    policy_sha256              CHAR(64) NOT NULL CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    model_version              VARCHAR(64) NOT NULL,
    iv_context_id              UUID REFERENCES option_iv_context_snapshots(iv_context_id),
    context_snapshot_id         UUID REFERENCES option_context_snapshots(context_snapshot_id),
    decision_evidence_id        UUID NOT NULL REFERENCES option_decision_evidence(evidence_id),
    market_data_time            TIMESTAMPTZ NOT NULL,
    observed_time               TIMESTAMPTZ NOT NULL,
    valid_until                 TIMESTAMPTZ,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (matrix_id, strategy_name, strategy_version, candidate_rank),
    FOREIGN KEY (strategy_name, strategy_version)
        REFERENCES option_strategy_registry(strategy_name, strategy_version),
    CHECK (cardinality(persona_tags) > 0),
    CHECK (jsonb_typeof(rank_components) = 'object'),
    CHECK (jsonb_typeof(primary_evidence) = 'object'),
    CHECK (jsonb_typeof(management_policy) = 'object'),
    CHECK (observed_time >= market_data_time),
    CHECK (valid_until IS NULL OR valid_until > market_data_time),
    CHECK (status = 'SELECTED' OR execution_eligibility IS NULL),
    CHECK (candidate_kind <> 'RESEARCH_ONLY' OR execution_eligibility IS NULL),
    CHECK (maximum_loss IS NULL OR maximum_loss > 0)
);

CREATE INDEX IF NOT EXISTS idx_option_strategy_candidates_list
    ON option_strategy_candidates (
        status, strategy_archetype, underlying, expiration_date, candidate_rank
    );
CREATE INDEX IF NOT EXISTS idx_option_strategy_candidates_matrix
    ON option_strategy_candidates (matrix_id, strategy_name, candidate_rank);

CREATE TABLE IF NOT EXISTS option_candidate_legs (
    candidate_id               UUID NOT NULL REFERENCES option_strategy_candidates(candidate_id),
    leg_index                  INTEGER NOT NULL CHECK (leg_index >= 0),
    snapshot_id                UUID NOT NULL REFERENCES option_snapshot_fact_keys(snapshot_id),
    contract_id                BIGINT NOT NULL REFERENCES option_contract_catalog(contract_id),
    contract_ticker            VARCHAR(64) NOT NULL,
    side                       VARCHAR(4) NOT NULL CHECK (side IN ('BUY', 'SELL')),
    ratio                      INTEGER NOT NULL CHECK (ratio > 0),
    multiplier                 INTEGER NOT NULL CHECK (multiplier > 0),
    expiration_date            DATE NOT NULL,
    strike                     NUMERIC(20,8) NOT NULL CHECK (strike > 0),
    contract_type              VARCHAR(4) NOT NULL CHECK (contract_type IN ('CALL', 'PUT')),
    spot                       NUMERIC(20,8) NOT NULL CHECK (spot > 0),
    time_to_expiration_years   DOUBLE PRECISION NOT NULL CHECK (time_to_expiration_years > 0),
    risk_free_rate             DOUBLE PRECISION NOT NULL,
    dividend_yield             DOUBLE PRECISION NOT NULL,
    model_mark                 NUMERIC(20,8) CHECK (model_mark IS NULL OR model_mark > 0),
    local_iv                   DOUBLE PRECISION,
    local_delta                DOUBLE PRECISION,
    local_gamma                DOUBLE PRECISION,
    local_theta_per_day        DOUBLE PRECISION,
    local_vega_per_vol_point   DOUBLE PRECISION,
    local_rho_per_rate_point   DOUBLE PRECISION,
    source_market_time         TIMESTAMPTZ NOT NULL,
    mark_source                VARCHAR(40) NOT NULL,
    model_version              VARCHAR(64) NOT NULL,
    quality_flags              TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    quote_bid                  NUMERIC(20,8),
    quote_ask                  NUMERIC(20,8),
    quote_bid_size             INTEGER,
    quote_ask_size             INTEGER,
    quote_midpoint             NUMERIC(20,8),
    quote_spread_midpoint      DOUBLE PRECISION,
    quote_sequence             BIGINT,
    quote_time                 TIMESTAMPTZ,
    underlying_quote_time      TIMESTAMPTZ,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (candidate_id, leg_index),
    CHECK (quote_bid IS NULL OR quote_bid >= 0),
    CHECK (quote_ask IS NULL OR quote_ask >= 0),
    CHECK (quote_bid IS NULL OR quote_ask IS NULL OR quote_bid <= quote_ask),
    CHECK (quote_midpoint IS NULL OR quote_midpoint >= 0),
    CHECK (quote_spread_midpoint IS NULL OR quote_spread_midpoint >= 0)
);

ALTER TABLE option_candidate_legs
        ADD COLUMN IF NOT EXISTS model_version VARCHAR(64);
UPDATE option_candidate_legs AS legs
SET model_version = candidates.model_version
FROM option_strategy_candidates AS candidates
WHERE legs.candidate_id = candidates.candidate_id
    AND legs.model_version IS NULL;
ALTER TABLE option_candidate_legs
        ALTER COLUMN model_version SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_option_candidate_legs_contract
    ON option_candidate_legs (contract_id, source_market_time DESC);

CREATE TABLE IF NOT EXISTS option_signal_suppressions (
    suppression_id             UUID PRIMARY KEY,
    candidate_id               UUID NOT NULL UNIQUE REFERENCES option_strategy_candidates(candidate_id),
    strategy_name              VARCHAR(64) NOT NULL,
    strategy_version           VARCHAR(32) NOT NULL,
    decision_time              TIMESTAMPTZ NOT NULL,
    failed_gate_codes          TEXT[] NOT NULL,
    configuration_version      VARCHAR(64) NOT NULL,
    input_provenance           JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (cardinality(failed_gate_codes) > 0),
    CHECK (jsonb_typeof(input_provenance) = 'object')
);

CREATE TABLE IF NOT EXISTS option_scenario_results (
    scenario_result_id         UUID PRIMARY KEY,
    candidate_id               UUID REFERENCES option_strategy_candidates(candidate_id),
    snapshot_id                UUID REFERENCES option_snapshot_fact_keys(snapshot_id),
    scenario_key               VARCHAR(96) NOT NULL,
    spot_shock_fraction        DOUBLE PRECISION NOT NULL,
    iv_shock_fraction          DOUBLE PRECISION NOT NULL,
    time_fraction_remaining    DOUBLE PRECISION NOT NULL CHECK (
        time_fraction_remaining >= 0 AND time_fraction_remaining <= 1
    ),
    repriced_value             NUMERIC(20,8),
    profit_loss                NUMERIC(20,8),
    delta                      DOUBLE PRECISION,
    gamma                      DOUBLE PRECISION,
    theta_per_day              DOUBLE PRECISION,
    vega_per_vol_point         DOUBLE PRECISION,
    terminal                   BOOLEAN NOT NULL,
    assumptions                JSONB NOT NULL DEFAULT '{}'::jsonb,
    quality_flags              TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    model_version              VARCHAR(64) NOT NULL,
    policy_sha256              CHAR(64) NOT NULL CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK ((candidate_id IS NULL) <> (snapshot_id IS NULL)),
    CHECK (jsonb_typeof(assumptions) = 'object'),
    UNIQUE (candidate_id, scenario_key),
    UNIQUE (snapshot_id, scenario_key)
);

CREATE TABLE IF NOT EXISTS option_signal_events (
    event_id                   UUID PRIMARY KEY,
    idempotency_key            CHAR(64) NOT NULL UNIQUE CHECK (idempotency_key ~ '^[0-9a-f]{64}$'),
    source_candidate_id        UUID NOT NULL UNIQUE REFERENCES option_strategy_candidates(candidate_id),
    underlying                 VARCHAR(16) NOT NULL,
    strategy_name              VARCHAR(64) NOT NULL,
    strategy_version           VARCHAR(32) NOT NULL,
    market_data_time           TIMESTAMPTZ NOT NULL,
    observed_time              TIMESTAMPTZ NOT NULL,
    action                     VARCHAR(4) NOT NULL CHECK (action IN ('BUY', 'SELL')),
    net_premium                NUMERIC(20,8) NOT NULL,
    stop_loss                  NUMERIC(20,8),
    take_profit                NUMERIC(20,8),
    trailing_activation        NUMERIC(20,8),
    trailing_distance          NUMERIC(20,8),
    valid_until                TIMESTAMPTZ NOT NULL,
    confidence                 DOUBLE PRECISION,
    data_quality               VARCHAR(32) NOT NULL,
    execution_eligibility      VARCHAR(24) CHECK (
        execution_eligibility IS NULL OR
        execution_eligibility IN ('PAPER_PROXY', 'LIVE_CANDIDATE')
    ),
    status                     VARCHAR(16) NOT NULL CHECK (
        status IN ('PENDING', 'READY', 'BLOCKED', 'EXPIRED')
    ),
    blocked_reasons            TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    occurrence_count           INTEGER NOT NULL DEFAULT 1 CHECK (occurrence_count > 0),
    expected_leg_count         INTEGER NOT NULL CHECK (expected_leg_count > 0),
    metadata                   JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (observed_time >= market_data_time),
    CHECK (valid_until > market_data_time),
    CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
    CHECK (jsonb_typeof(metadata) = 'object'),
    CHECK (status <> 'READY' OR execution_eligibility IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_option_signal_events_search
    ON option_signal_events (underlying, market_data_time DESC, status, strategy_name);

CREATE TABLE IF NOT EXISTS option_signal_legs (
    event_id                   UUID NOT NULL REFERENCES option_signal_events(event_id),
    leg_index                  INTEGER NOT NULL CHECK (leg_index >= 0),
    contract_id                BIGINT NOT NULL REFERENCES option_contract_catalog(contract_id),
    contract_ticker            VARCHAR(64) NOT NULL,
    action                     VARCHAR(4) NOT NULL CHECK (action IN ('BUY', 'SELL')),
    ratio                      INTEGER NOT NULL CHECK (ratio > 0),
    multiplier                 INTEGER NOT NULL CHECK (multiplier > 0),
    model_mark                 NUMERIC(20,8) NOT NULL CHECK (model_mark > 0),
    local_iv                   DOUBLE PRECISION NOT NULL,
    local_gamma                DOUBLE PRECISION NOT NULL,
    expiration_date            DATE NOT NULL,
    strike                     NUMERIC(20,8) NOT NULL CHECK (strike > 0),
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, leg_index)
);

CREATE TABLE IF NOT EXISTS option_signal_occurrences (
    occurrence_id              UUID PRIMARY KEY,
    event_id                   UUID NOT NULL REFERENCES option_signal_events(event_id),
    market_data_time           TIMESTAMPTZ NOT NULL,
    observed_time              TIMESTAMPTZ NOT NULL,
    source_batch_id            UUID NOT NULL REFERENCES option_ingestion_runs(batch_id),
    mark_diagnostics           JSONB NOT NULL DEFAULT '{}'::jsonb,
    trigger_diagnostics        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (event_id, market_data_time),
    CHECK (observed_time >= market_data_time),
    CHECK (jsonb_typeof(mark_diagnostics) = 'object'),
    CHECK (jsonb_typeof(trigger_diagnostics) = 'object')
);

CREATE OR REPLACE FUNCTION enforce_option_signal_leg_completeness()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    actual_leg_count INTEGER;
BEGIN
    IF NEW.status <> 'READY' THEN
        RETURN NEW;
    END IF;
    SELECT COUNT(*) INTO actual_leg_count
    FROM option_signal_legs
    WHERE event_id = NEW.event_id;
    IF actual_leg_count <> NEW.expected_leg_count THEN
        RAISE EXCEPTION 'ready option signal % expected % legs but found %',
            NEW.event_id, NEW.expected_leg_count, actual_leg_count;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_option_signal_leg_completeness ON option_signal_events;
CREATE CONSTRAINT TRIGGER trg_option_signal_leg_completeness
AFTER INSERT OR UPDATE OF status, expected_leg_count ON option_signal_events
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION enforce_option_signal_leg_completeness();

CREATE TABLE IF NOT EXISTS option_flow_windows (
    flow_window_id             UUID PRIMARY KEY,
    matrix_id                  UUID NOT NULL REFERENCES option_analysis_runs(matrix_id),
    underlying                 VARCHAR(16) NOT NULL,
    window_start               TIMESTAMPTZ NOT NULL,
    window_end                 TIMESTAMPTZ NOT NULL,
    watermark_time             TIMESTAMPTZ NOT NULL,
    late_event_count           INTEGER NOT NULL DEFAULT 0 CHECK (late_event_count >= 0),
    corrected_event_count      INTEGER NOT NULL DEFAULT 0 CHECK (corrected_event_count >= 0),
    distinct_contract_count    INTEGER NOT NULL DEFAULT 0 CHECK (distinct_contract_count >= 0),
    distinct_exchange_count    INTEGER NOT NULL DEFAULT 0 CHECK (distinct_exchange_count >= 0),
    qualifying_print_count     INTEGER NOT NULL DEFAULT 0 CHECK (qualifying_print_count >= 0),
    total_notional             NUMERIC(24,8) NOT NULL DEFAULT 0 CHECK (total_notional >= 0),
    call_notional              NUMERIC(24,8) NOT NULL DEFAULT 0 CHECK (call_notional >= 0),
    put_notional               NUMERIC(24,8) NOT NULL DEFAULT 0 CHECK (put_notional >= 0),
    otm_call_print_count       INTEGER NOT NULL DEFAULT 0 CHECK (otm_call_print_count >= 0),
    detector_version           VARCHAR(64) NOT NULL,
    contributing_event_keys    JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (window_end >= window_start),
    CHECK (watermark_time >= window_end),
    CHECK (jsonb_typeof(contributing_event_keys) = 'array')
);

CREATE INDEX IF NOT EXISTS idx_option_flow_windows_lookup
    ON option_flow_windows (underlying, window_end DESC, detector_version);

CREATE TABLE IF NOT EXISTS option_volatility_surfaces (
    volatility_surface_id      UUID PRIMARY KEY,
    matrix_id                  UUID NOT NULL REFERENCES option_analysis_runs(matrix_id),
    underlying                 VARCHAR(16) NOT NULL,
    expiration_date            DATE NOT NULL,
    contract_type              VARCHAR(4) NOT NULL CHECK (contract_type IN ('CALL', 'PUT')),
    window_start               TIMESTAMPTZ NOT NULL,
    window_end                 TIMESTAMPTZ NOT NULL,
    input_count                INTEGER NOT NULL CHECK (input_count >= 0),
    minimum_strike             NUMERIC(20,8),
    maximum_strike             NUMERIC(20,8),
    fit_model                  VARCHAR(64) NOT NULL,
    fit_version                VARCHAR(64) NOT NULL,
    fit_diagnostics            JSONB NOT NULL DEFAULT '{}'::jsonb,
    residual_distribution      JSONB NOT NULL DEFAULT '{}'::jsonb,
    coefficients               JSONB NOT NULL DEFAULT '{}'::jsonb,
    quality_reasons            TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (matrix_id, expiration_date, contract_type, fit_version),
    CHECK (window_end >= window_start),
    CHECK (maximum_strike IS NULL OR minimum_strike IS NULL OR maximum_strike >= minimum_strike),
    CHECK (jsonb_typeof(fit_diagnostics) = 'object'),
    CHECK (jsonb_typeof(residual_distribution) = 'object'),
    CHECK (jsonb_typeof(coefficients) = 'object')
);

CREATE TABLE IF NOT EXISTS option_signal_decay_outcomes (
    outcome_id                 UUID PRIMARY KEY,
    event_id                   UUID REFERENCES option_signal_events(event_id),
    candidate_id               UUID NOT NULL REFERENCES option_strategy_candidates(candidate_id),
    measurement_type           VARCHAR(16) NOT NULL CHECK (
        measurement_type IN ('15MIN', '30MIN', '60MIN', 'CLOSE', 'NEXT_OPEN')
    ),
    market_time                TIMESTAMPTZ NOT NULL,
    observed_time              TIMESTAMPTZ NOT NULL,
    mark                       NUMERIC(20,8),
    net_return                 NUMERIC(20,8),
    availability_flag          VARCHAR(32) NOT NULL,
    quality_flags              TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (candidate_id, measurement_type),
    CHECK (observed_time >= market_time)
);

CREATE TABLE IF NOT EXISTS option_recommendation_validity_events (
    validity_event_id          UUID PRIMARY KEY,
    signal_id                  UUID NOT NULL REFERENCES option_signal_events(event_id),
    candidate_id               UUID NOT NULL REFERENCES option_strategy_candidates(candidate_id),
    prior_state                VARCHAR(24),
    new_state                  VARCHAR(24) NOT NULL CHECK (
        new_state IN (
            'PENDING', 'ACTIVE', 'SUSPENDED', 'INVALIDATED',
            'EXPIRED', 'SUPERSEDED', 'CONSUMED'
        )
    ),
    market_time                TIMESTAMPTZ NOT NULL,
    observed_time              TIMESTAMPTZ NOT NULL,
    evaluated_at               TIMESTAMPTZ NOT NULL,
    valid_through              TIMESTAMPTZ,
    reason_codes               TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    leg_quote_versions         JSONB NOT NULL DEFAULT '[]'::jsonb,
    underlying_version         VARCHAR(128),
    account_version            VARCHAR(128),
    analysis_version           VARCHAR(128) NOT NULL,
    scenario_version           VARCHAR(128) NOT NULL,
    policy_sha256              CHAR(64) NOT NULL CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    token_hash                 CHAR(64) CHECK (token_hash IS NULL OR token_hash ~ '^[0-9a-f]{64}$'),
    transition_evidence        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (observed_time >= market_time),
    CHECK (evaluated_at >= observed_time),
    CHECK (valid_through IS NULL OR valid_through > market_time),
    CHECK (jsonb_typeof(leg_quote_versions) = 'array'),
    CHECK (jsonb_typeof(transition_evidence) = 'object'),
    CHECK (new_state <> 'ACTIVE' OR (token_hash IS NOT NULL AND valid_through IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS idx_option_validity_events_signal
    ON option_recommendation_validity_events (signal_id, evaluated_at DESC);

CREATE TABLE IF NOT EXISTS option_recommendation_validity_current (
    signal_id                  UUID PRIMARY KEY REFERENCES option_signal_events(event_id),
    candidate_id               UUID NOT NULL REFERENCES option_strategy_candidates(candidate_id),
    validity_event_id          UUID NOT NULL UNIQUE REFERENCES option_recommendation_validity_events(validity_event_id),
    version                    BIGINT NOT NULL CHECK (version > 0),
    state                      VARCHAR(24) NOT NULL CHECK (
        state IN (
            'PENDING', 'ACTIVE', 'SUSPENDED', 'INVALIDATED',
            'EXPIRED', 'SUPERSEDED', 'CONSUMED'
        )
    ),
    token_hash                 CHAR(64) CHECK (token_hash IS NULL OR token_hash ~ '^[0-9a-f]{64}$'),
    valid_through              TIMESTAMPTZ,
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (state <> 'ACTIVE' OR (token_hash IS NOT NULL AND valid_through IS NOT NULL))
);