-- Polygon Options Developer market-data, universe, and durable-work foundation.
-- Strategy, signal, paper-execution, Advanced, and broker tables are intentionally excluded.

CREATE TABLE IF NOT EXISTS option_contract_catalog (
    contract_id             BIGSERIAL PRIMARY KEY,
    contract_ticker         VARCHAR(64) NOT NULL UNIQUE,
    underlying              VARCHAR(16) NOT NULL,
    asset_type              VARCHAR(8) NOT NULL CHECK (asset_type IN ('STOCK', 'ETF')),
    first_observed_at       TIMESTAMPTZ NOT NULL,
    catalog_admitted_at     TIMESTAMPTZ,
    expired_at              TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (catalog_admitted_at IS NULL OR catalog_admitted_at >= first_observed_at),
    CHECK (expired_at IS NULL OR expired_at >= catalog_admitted_at)
);

CREATE INDEX IF NOT EXISTS idx_option_contract_catalog_underlying
    ON option_contract_catalog (underlying, expired_at, contract_ticker);

CREATE TABLE IF NOT EXISTS option_contract_catalog_versions (
    catalog_version_id      BIGSERIAL PRIMARY KEY,
    contract_id             BIGINT NOT NULL REFERENCES option_contract_catalog(contract_id),
    provider                VARCHAR(32) NOT NULL,
    provider_version        VARCHAR(64),
    provider_contract_type  VARCHAR(32) NOT NULL,
    contract_type           VARCHAR(4) CHECK (contract_type IN ('CALL', 'PUT')),
    expiration_date         DATE NOT NULL,
    strike                  NUMERIC(20,8) NOT NULL CHECK (strike > 0),
    provider_exercise_style VARCHAR(32) NOT NULL,
    exercise_style          VARCHAR(16) CHECK (exercise_style IN ('AMERICAN', 'EUROPEAN')),
    shares_per_contract     INTEGER NOT NULL CHECK (shares_per_contract > 0),
    primary_exchange        VARCHAR(32),
    correction              VARCHAR(32),
    additional_underlyings  JSONB NOT NULL DEFAULT '[]'::jsonb,
    adjustment_metadata     JSONB NOT NULL DEFAULT '{}'::jsonb,
    eligibility_status      VARCHAR(32) NOT NULL CHECK (
        eligibility_status IN (
            'VALIDATED_ACTIVE',
            'REJECTED_UNSUPPORTED',
            'EXPIRED'
        )
    ),
    exclusion_reasons       TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    valid_from              TIMESTAMPTZ NOT NULL,
    valid_to                TIMESTAMPTZ,
    first_observed_at       TIMESTAMPTZ NOT NULL,
    revised_observed_at     TIMESTAMPTZ,
    refreshed_at            TIMESTAMPTZ NOT NULL,
    payload_sha256          CHAR(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (contract_id, provider, payload_sha256, first_observed_at),
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    CHECK (first_observed_at >= valid_from),
    CHECK (revised_observed_at IS NULL OR revised_observed_at >= first_observed_at),
    CHECK (jsonb_typeof(additional_underlyings) = 'array'),
    CHECK (jsonb_typeof(adjustment_metadata) = 'object'),
    CHECK (
        eligibility_status <> 'VALIDATED_ACTIVE' OR
        (contract_type IS NOT NULL AND exercise_style = 'AMERICAN' AND shares_per_contract = 100)
    )
);

CREATE INDEX IF NOT EXISTS idx_option_catalog_versions_context
    ON option_contract_catalog_versions (
        contract_id,
        valid_from DESC,
        first_observed_at DESC,
        revised_observed_at DESC
    );
CREATE INDEX IF NOT EXISTS idx_option_catalog_versions_expiration
    ON option_contract_catalog_versions (expiration_date, eligibility_status);

CREATE TABLE IF NOT EXISTS option_contract_discoveries (
    discovery_id            BIGSERIAL PRIMARY KEY,
    contract_ticker         VARCHAR(64) NOT NULL,
    underlying              VARCHAR(16) NOT NULL,
    state                   VARCHAR(32) NOT NULL CHECK (
        state IN (
            'UNKNOWN_REFERENCE',
            'REFERENCE_PENDING',
            'VALIDATED_ACTIVE',
            'REJECTED_UNSUPPORTED',
            'REFERENCE_UNAVAILABLE',
            'WATCHLIST_ACTIVE'
        )
    ),
    source_batch_id         UUID NOT NULL,
    source_page_number      INTEGER NOT NULL CHECK (source_page_number > 0),
    first_observed_at       TIMESTAMPTZ NOT NULL,
    last_attempted_at       TIMESTAMPTZ,
    resolved_at             TIMESTAMPTZ,
    activate_after          TIMESTAMPTZ,
    contract_id             BIGINT REFERENCES option_contract_catalog(contract_id),
    raw_details             JSONB NOT NULL DEFAULT '{}'::jsonb,
    reason_codes            TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (contract_ticker, source_batch_id),
    CHECK (jsonb_typeof(raw_details) = 'object'),
    CHECK (resolved_at IS NULL OR resolved_at >= first_observed_at),
    CHECK (activate_after IS NULL OR activate_after >= first_observed_at),
    CHECK (
        state NOT IN ('VALIDATED_ACTIVE', 'WATCHLIST_ACTIVE') OR
        (contract_id IS NOT NULL AND resolved_at IS NOT NULL AND activate_after IS NOT NULL)
    ),
    CHECK (
        state NOT IN ('REJECTED_UNSUPPORTED', 'REFERENCE_UNAVAILABLE') OR
        resolved_at IS NOT NULL
    )
);

CREATE INDEX IF NOT EXISTS idx_option_discoveries_pending
    ON option_contract_discoveries (state, first_observed_at)
    WHERE state IN ('UNKNOWN_REFERENCE', 'REFERENCE_PENDING', 'REFERENCE_UNAVAILABLE');

CREATE OR REPLACE FUNCTION enforce_option_new_series_transition()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.state = OLD.state THEN
        RETURN NEW;
    END IF;
    IF NOT (
        (OLD.state = 'UNKNOWN_REFERENCE' AND NEW.state = 'REFERENCE_PENDING') OR
        (OLD.state = 'REFERENCE_PENDING' AND NEW.state IN (
            'VALIDATED_ACTIVE', 'REJECTED_UNSUPPORTED', 'REFERENCE_UNAVAILABLE'
        )) OR
        (OLD.state = 'REFERENCE_UNAVAILABLE' AND NEW.state = 'REFERENCE_PENDING') OR
        (OLD.state = 'VALIDATED_ACTIVE' AND NEW.state = 'WATCHLIST_ACTIVE')
    ) THEN
        RAISE EXCEPTION 'invalid option new-series transition: % -> %', OLD.state, NEW.state;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_option_new_series_transition ON option_contract_discoveries;
CREATE TRIGGER trg_option_new_series_transition
BEFORE UPDATE OF state ON option_contract_discoveries
FOR EACH ROW
EXECUTE FUNCTION enforce_option_new_series_transition();

CREATE TABLE IF NOT EXISTS option_universe_runs (
    run_id                  UUID PRIMARY KEY,
    mode                    VARCHAR(16) NOT NULL CHECK (mode IN ('fixed', 'ranked')),
    as_of_session           DATE NOT NULL,
    effective_from          DATE NOT NULL,
    status                  VARCHAR(24) NOT NULL CHECK (
        status IN ('PENDING', 'RUNNING', 'COMPLETE', 'DEGRADED', 'FAILED')
    ),
    completeness_fraction   DOUBLE PRECISION CHECK (
        completeness_fraction IS NULL OR
        (completeness_fraction >= 0 AND completeness_fraction <= 1)
    ),
    configuration_json      JSONB NOT NULL,
    configuration_sha256    CHAR(64) NOT NULL CHECK (configuration_sha256 ~ '^[0-9a-f]{64}$'),
    started_at              TIMESTAMPTZ NOT NULL,
    completed_at            TIMESTAMPTZ,
    first_observed_at       TIMESTAMPTZ NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (effective_from >= as_of_session),
    CHECK (completed_at IS NULL OR completed_at >= started_at),
    CHECK (jsonb_typeof(configuration_json) = 'object')
);

CREATE TABLE IF NOT EXISTS option_universe_candidates (
    run_id                  UUID NOT NULL REFERENCES option_universe_runs(run_id),
    ticker                  VARCHAR(16) NOT NULL,
    asset_type              VARCHAR(8) NOT NULL CHECK (asset_type IN ('STOCK', 'ETF')),
    raw_metrics             JSONB NOT NULL DEFAULT '{}'::jsonb,
    component_ranks         JSONB NOT NULL DEFAULT '{}'::jsonb,
    total_score             DOUBLE PRECISION,
    eligible                BOOLEAN NOT NULL,
    exclusion_reasons       TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    candidate_rank          INTEGER CHECK (candidate_rank IS NULL OR candidate_rank > 0),
    first_observed_at       TIMESTAMPTZ NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, ticker),
    CHECK (jsonb_typeof(raw_metrics) = 'object'),
    CHECK (jsonb_typeof(component_ranks) = 'object')
);

CREATE TABLE IF NOT EXISTS option_universe_members (
    member_id               BIGSERIAL PRIMARY KEY,
    effective_from          DATE NOT NULL,
    ticker                  VARCHAR(16) NOT NULL,
    asset_type              VARCHAR(8) NOT NULL CHECK (asset_type IN ('STOCK', 'ETF')),
    source_run_id           UUID NOT NULL REFERENCES option_universe_runs(run_id),
    member_rank             INTEGER CHECK (member_rank IS NULL OR member_rank > 0),
    score                   DOUBLE PRECISION,
    activated_at            TIMESTAMPTZ NOT NULL,
    deactivated_at          TIMESTAMPTZ,
    first_observed_at       TIMESTAMPTZ NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (effective_from, ticker),
    CHECK (deactivated_at IS NULL OR deactivated_at > activated_at),
    CHECK (first_observed_at >= activated_at)
);

CREATE INDEX IF NOT EXISTS idx_option_universe_members_context
    ON option_universe_members (ticker, effective_from DESC, first_observed_at DESC);

CREATE TABLE IF NOT EXISTS option_ingestion_runs (
    batch_id                UUID PRIMARY KEY,
    provider                VARCHAR(32) NOT NULL,
    underlying              VARCHAR(16) NOT NULL,
    asset_type              VARCHAR(8) NOT NULL CHECK (asset_type IN ('STOCK', 'ETF')),
    scheduled_cycle         TIMESTAMPTZ NOT NULL,
    request_filter_sha256   CHAR(64) NOT NULL CHECK (request_filter_sha256 ~ '^[0-9a-f]{64}$'),
    policy_version          VARCHAR(64) NOT NULL,
    policy_sha256           CHAR(64) NOT NULL CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    configuration_sha256    CHAR(64) NOT NULL CHECK (configuration_sha256 ~ '^[0-9a-f]{64}$'),
    status                  VARCHAR(24) NOT NULL CHECK (
        status IN ('FETCHING', 'COMPLETE', 'FAILED', 'QUARANTINED')
    ),
    page_count              INTEGER NOT NULL DEFAULT 0 CHECK (page_count >= 0),
    terminal_page_received  BOOLEAN NOT NULL DEFAULT FALSE,
    received_row_count      INTEGER NOT NULL DEFAULT 0 CHECK (received_row_count >= 0),
    catalog_row_count       INTEGER NOT NULL DEFAULT 0 CHECK (catalog_row_count >= 0),
    retained_row_count      INTEGER NOT NULL DEFAULT 0 CHECK (retained_row_count >= 0),
    rejected_counts         JSONB NOT NULL DEFAULT '{}'::jsonb,
    unknown_reference_count INTEGER NOT NULL DEFAULT 0 CHECK (unknown_reference_count >= 0),
    request_ids             TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    latency_ms              BIGINT CHECK (latency_ms IS NULL OR latency_ms >= 0),
    retry_count             INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    error_category          VARCHAR(64),
    failure_reason          TEXT,
    market_data_time        TIMESTAMPTZ,
    first_observed_at       TIMESTAMPTZ,
    started_at              TIMESTAMPTZ NOT NULL,
    completed_at            TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (provider, underlying, scheduled_cycle, request_filter_sha256),
    CHECK (jsonb_typeof(rejected_counts) = 'object'),
    CHECK (completed_at IS NULL OR completed_at >= started_at),
    CHECK (
        status <> 'COMPLETE' OR
        (terminal_page_received AND completed_at IS NOT NULL AND failure_reason IS NULL)
    ),
    CHECK (
        status NOT IN ('FAILED', 'QUARANTINED') OR
        (completed_at IS NOT NULL AND failure_reason IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_option_ingestion_runs_cycle
    ON option_ingestion_runs (scheduled_cycle DESC, underlying);
CREATE INDEX IF NOT EXISTS idx_option_ingestion_runs_status
    ON option_ingestion_runs (status, started_at);

CREATE TABLE IF NOT EXISTS option_raw_batch_pages (
    batch_id                UUID NOT NULL REFERENCES option_ingestion_runs(batch_id),
    page_number             INTEGER NOT NULL CHECK (page_number > 0),
    request_id              VARCHAR(128),
    redacted_request        JSONB NOT NULL DEFAULT '{}'::jsonb,
    response_gzip           BYTEA NOT NULL,
    payload_sha256          CHAR(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    row_count               INTEGER NOT NULL CHECK (row_count >= 0),
    byte_count              BIGINT NOT NULL CHECK (byte_count >= 0),
    received_at             TIMESTAMPTZ NOT NULL,
    terminal_page           BOOLEAN NOT NULL,
    request_filter_sha256   CHAR(64) NOT NULL CHECK (request_filter_sha256 ~ '^[0-9a-f]{64}$'),
    request_cursor_sha256   CHAR(64) CHECK (
        request_cursor_sha256 IS NULL OR request_cursor_sha256 ~ '^[0-9a-f]{64}$'
    ),
    next_cursor_sha256      CHAR(64) CHECK (
        next_cursor_sha256 IS NULL OR next_cursor_sha256 ~ '^[0-9a-f]{64}$'
    ),
    validation_status       VARCHAR(16) NOT NULL CHECK (validation_status IN ('VALID', 'INVALID')),
    validation_error        TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, page_number),
    CHECK (jsonb_typeof(redacted_request) = 'object'),
    CHECK (page_number <> 1 OR request_cursor_sha256 IS NULL),
    CHECK (
        validation_status = 'INVALID' OR
        (terminal_page AND next_cursor_sha256 IS NULL) OR
        (NOT terminal_page AND next_cursor_sha256 IS NOT NULL)
    ),
    CHECK (
        (validation_status = 'VALID' AND validation_error IS NULL) OR
        (validation_status = 'INVALID' AND validation_error IS NOT NULL)
    )
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_option_discovery_raw_page'
          AND conrelid = 'option_contract_discoveries'::regclass
    ) THEN
        ALTER TABLE option_contract_discoveries
            ADD CONSTRAINT fk_option_discovery_raw_page
            FOREIGN KEY (source_batch_id, source_page_number)
            REFERENCES option_raw_batch_pages(batch_id, page_number);
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS option_work_items (
    work_id                 UUID PRIMARY KEY,
    stage                   VARCHAR(24) NOT NULL CHECK (
        stage IN (
            'NORMALIZE', 'ANALYZE', 'ARCHIVE', 'TRADE_BACKFILL', 'CLASSIFY_TRADES'
        )
    ),
    subject_id              VARCHAR(128) NOT NULL,
    business_key            VARCHAR(256) NOT NULL UNIQUE,
    status                  VARCHAR(24) NOT NULL CHECK (
        status IN ('PENDING', 'CLAIMED', 'RETRY', 'COMPLETED', 'TERMINAL_FAILED')
    ),
    lease_owner             VARCHAR(128),
    lease_expires_at        TIMESTAMPTZ,
    attempt_count           INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    maximum_attempts        INTEGER NOT NULL CHECK (maximum_attempts > 0),
    next_attempt_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error              TEXT,
    payload                 JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at            TIMESTAMPTZ,
    CHECK (attempt_count <= maximum_attempts),
    CHECK (jsonb_typeof(payload) = 'object'),
    CHECK (
        (status = 'CLAIMED' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL) OR
        (status <> 'CLAIMED' AND lease_owner IS NULL AND lease_expires_at IS NULL)
    ),
    CHECK (status <> 'COMPLETED' OR completed_at IS NOT NULL),
    CHECK (status <> 'TERMINAL_FAILED' OR last_error IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_option_work_items_claim
    ON option_work_items (stage, status, next_attempt_at, created_at)
    WHERE status IN ('PENDING', 'RETRY');
CREATE INDEX IF NOT EXISTS idx_option_work_items_expired_lease
    ON option_work_items (lease_expires_at)
    WHERE status = 'CLAIMED';

CREATE TABLE IF NOT EXISTS option_scheduler_instances (
    instance_id             UUID PRIMARY KEY,
    configuration_sha256    CHAR(64) NOT NULL CHECK (configuration_sha256 ~ '^[0-9a-f]{64}$'),
    policy_sha256           CHAR(64) NOT NULL CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    process_id              INTEGER NOT NULL CHECK (process_id > 0),
    host_name               VARCHAR(255) NOT NULL,
    status                  VARCHAR(24) NOT NULL CHECK (
        status IN ('STARTING', 'READ_ONLY', 'LEADER', 'DEGRADED', 'STOPPED')
    ),
    acquired_at             TIMESTAMPTZ,
    last_heartbeat_at       TIMESTAMPTZ NOT NULL,
    stopped_at              TIMESTAMPTZ,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (stopped_at IS NULL OR stopped_at >= last_heartbeat_at)
);

CREATE INDEX IF NOT EXISTS idx_option_scheduler_instances_heartbeat
    ON option_scheduler_instances (status, last_heartbeat_at DESC);

CREATE TABLE IF NOT EXISTS option_snapshot_fact_keys (
    snapshot_id                 UUID PRIMARY KEY,
    contract_id                 BIGINT NOT NULL REFERENCES option_contract_catalog(contract_id),
    provider                    VARCHAR(32) NOT NULL,
    market_data_time            TIMESTAMPTZ NOT NULL,
    normalized_payload_sha256   CHAR(64) NOT NULL CHECK (
        normalized_payload_sha256 ~ '^[0-9a-f]{64}$'
    ),
    first_observed_at           TIMESTAMPTZ NOT NULL,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (contract_id, provider, market_data_time, normalized_payload_sha256),
    CHECK (first_observed_at >= market_data_time)
);

CREATE TABLE IF NOT EXISTS option_chain_snapshots (
    snapshot_id                 UUID NOT NULL,
    contract_id                 BIGINT NOT NULL REFERENCES option_contract_catalog(contract_id),
    contract_ticker             VARCHAR(64) NOT NULL,
    underlying                  VARCHAR(16) NOT NULL,
    asset_type                  VARCHAR(8) NOT NULL CHECK (asset_type IN ('STOCK', 'ETF')),
    provider                    VARCHAR(32) NOT NULL,
    batch_id                    UUID NOT NULL REFERENCES option_ingestion_runs(batch_id),
    contract_type               VARCHAR(4) NOT NULL CHECK (contract_type IN ('CALL', 'PUT')),
    expiration_date             DATE NOT NULL,
    expiration_cutoff           TIMESTAMPTZ NOT NULL,
    calendar_dte                INTEGER NOT NULL CHECK (calendar_dte >= 0),
    time_to_expiration_years    DOUBLE PRECISION NOT NULL CHECK (time_to_expiration_years > 0),
    strike                      NUMERIC(20,8) NOT NULL CHECK (strike > 0),
    shares_per_contract         INTEGER NOT NULL CHECK (shares_per_contract > 0),
    exercise_style              VARCHAR(16) NOT NULL,
    spot                        NUMERIC(20,8) NOT NULL CHECK (spot > 0),
    spot_market_data_time       TIMESTAMPTZ NOT NULL,
    bid                         NUMERIC(20,8) CHECK (bid IS NULL OR bid >= 0),
    ask                         NUMERIC(20,8) CHECK (ask IS NULL OR ask >= 0),
    midpoint                    NUMERIC(20,8) CHECK (midpoint IS NULL OR midpoint >= 0),
    display_mark                NUMERIC(20,8) CHECK (display_mark IS NULL OR display_mark >= 0),
    model_mark                  NUMERIC(20,8) CHECK (model_mark IS NULL OR model_mark > 0),
    mark_market_data_time       TIMESTAMPTZ NOT NULL,
    mark_source                 VARCHAR(40) NOT NULL CHECK (
        mark_source IN (
            'DEVELOPER_ALIGNED_AGG_CLOSE',
            'ADVANCED_NBBO_MIDPOINT',
            'BROKER_NBBO_MIDPOINT',
            'DISPLAY_DAY_CLOSE',
            'DISPLAY_DAY_VWAP'
        )
    ),
    day_volume                 BIGINT CHECK (day_volume IS NULL OR day_volume >= 0),
    open_interest              BIGINT CHECK (open_interest IS NULL OR open_interest >= 0),
    market_data_time           TIMESTAMPTZ NOT NULL,
    first_observed_at          TIMESTAMPTZ NOT NULL,
    revised_observed_at        TIMESTAMPTZ,
    data_delay_seconds         DOUBLE PRECISION NOT NULL CHECK (data_delay_seconds >= 0),
    local_iv                   DOUBLE PRECISION,
    local_gamma                DOUBLE PRECISION,
    local_delta                DOUBLE PRECISION,
    local_theta_per_day        DOUBLE PRECISION,
    local_vega_per_vol_point   DOUBLE PRECISION,
    local_rho_per_rate_point   DOUBLE PRECISION,
    intrinsic_value            NUMERIC(20,8) CHECK (intrinsic_value IS NULL OR intrinsic_value >= 0),
    extrinsic_value            NUMERIC(20,8),
    single_contract_breakeven  NUMERIC(20,8),
    provider_iv                DOUBLE PRECISION,
    provider_gamma             DOUBLE PRECISION,
    risk_free_rate             DOUBLE PRECISION NOT NULL,
    dividend_yield             DOUBLE PRECISION NOT NULL,
    iv_converged               BOOLEAN NOT NULL,
    iv_solver                  VARCHAR(16),
    iv_iteration_count         INTEGER NOT NULL CHECK (iv_iteration_count >= 0),
    iv_price_error             DOUBLE PRECISION CHECK (iv_price_error IS NULL OR iv_price_error >= 0),
    iv_failure_reason          VARCHAR(64),
    model_version              VARCHAR(64) NOT NULL,
    quality_flags              TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    raw_payload_sha256         CHAR(64) NOT NULL CHECK (raw_payload_sha256 ~ '^[0-9a-f]{64}$'),
    normalized_payload_sha256  CHAR(64) NOT NULL CHECK (normalized_payload_sha256 ~ '^[0-9a-f]{64}$'),
    revision                   INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    policy_version             VARCHAR(64) NOT NULL,
    policy_sha256              CHAR(64) NOT NULL CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (snapshot_id) REFERENCES option_snapshot_fact_keys(snapshot_id),
    PRIMARY KEY (snapshot_id, first_observed_at),
    UNIQUE (
        contract_id,
        provider,
        market_data_time,
        normalized_payload_sha256,
        first_observed_at
    ),
    CHECK (first_observed_at >= market_data_time),
    CHECK (first_observed_at >= spot_market_data_time),
    CHECK (first_observed_at >= mark_market_data_time),
    CHECK (revised_observed_at IS NULL OR revised_observed_at >= first_observed_at),
    CHECK (
        (iv_converged AND local_iv IS NOT NULL AND iv_solver IS NOT NULL AND iv_failure_reason IS NULL) OR
        (NOT iv_converged AND local_iv IS NULL AND iv_failure_reason IS NOT NULL)
    )
) PARTITION BY RANGE (first_observed_at);

CREATE INDEX IF NOT EXISTS idx_option_chain_snapshots_context
    ON option_chain_snapshots (
        underlying,
        market_data_time DESC,
        first_observed_at DESC,
        revised_observed_at DESC
    );
CREATE INDEX IF NOT EXISTS idx_option_chain_snapshots_batch
    ON option_chain_snapshots (batch_id, contract_id);

CREATE TABLE IF NOT EXISTS option_trade_events (
    trade_event_id          UUID NOT NULL,
    provider                VARCHAR(32) NOT NULL,
    contract_id             BIGINT NOT NULL REFERENCES option_contract_catalog(contract_id),
    contract_ticker         VARCHAR(64) NOT NULL,
    underlying              VARCHAR(16) NOT NULL,
    sip_timestamp           TIMESTAMPTZ NOT NULL,
    sequence_number         BIGINT NOT NULL CHECK (sequence_number >= 0),
    participant_timestamp   TIMESTAMPTZ,
    first_observed_at       TIMESTAMPTZ NOT NULL,
    revised_observed_at     TIMESTAMPTZ,
    exchange                INTEGER,
    conditions              INTEGER[] NOT NULL DEFAULT ARRAY[]::INTEGER[],
    correction              INTEGER,
    provider_trade_id       VARCHAR(128),
    price                   NUMERIC(20,8) NOT NULL CHECK (price > 0),
    size                    BIGINT NOT NULL CHECK (size > 0),
    shares_per_contract     INTEGER NOT NULL CHECK (shares_per_contract > 0),
    notional                NUMERIC(24,8) NOT NULL CHECK (notional > 0),
    payload_sha256          CHAR(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    raw_batch_id            UUID NOT NULL REFERENCES option_ingestion_runs(batch_id),
    classification_status   VARCHAR(24) NOT NULL CHECK (
        classification_status IN (
            'PENDING',
            'INCLUDED',
            'EXCLUDED',
            'SUPERSEDED',
            'CANCELED',
            'UNKNOWN'
        )
    ),
    classification_reasons  TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    semantics_version       VARCHAR(64),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trade_event_id, sip_timestamp),
    UNIQUE NULLS NOT DISTINCT (
        provider,
        contract_id,
        sip_timestamp,
        sequence_number,
        participant_timestamp,
        payload_sha256
    ),
    CHECK (first_observed_at >= sip_timestamp),
    CHECK (revised_observed_at IS NULL OR revised_observed_at >= first_observed_at),
    CHECK (notional = price * size * shares_per_contract)
) PARTITION BY RANGE (sip_timestamp);

CREATE INDEX IF NOT EXISTS idx_option_trade_events_contract_cursor
    ON option_trade_events (contract_id, sip_timestamp DESC, sequence_number DESC);
CREATE INDEX IF NOT EXISTS idx_option_trade_events_observed
    ON option_trade_events (first_observed_at DESC);

CREATE TABLE IF NOT EXISTS option_trade_cursors (
    provider                    VARCHAR(32) NOT NULL,
    contract_id                 BIGINT NOT NULL REFERENCES option_contract_catalog(contract_id),
    completed_sip_timestamp     TIMESTAMPTZ NOT NULL,
    completed_sequence_number   BIGINT NOT NULL CHECK (completed_sequence_number >= 0),
    overlap_seconds             INTEGER NOT NULL CHECK (overlap_seconds >= 0),
    latest_complete_request_id  VARCHAR(128),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (provider, contract_id)
);

CREATE TABLE IF NOT EXISTS option_provider_trade_semantics (
    semantics_id            BIGSERIAL PRIMARY KEY,
    provider                VARCHAR(32) NOT NULL,
    semantics_version       VARCHAR(64) NOT NULL,
    condition_code          INTEGER,
    correction_code         INTEGER,
    behavior                VARCHAR(24) NOT NULL CHECK (
        behavior IN ('INCLUDE', 'EXCLUDE', 'SUPERSEDE', 'CANCEL', 'UNKNOWN')
    ),
    contributes_volume      BOOLEAN NOT NULL,
    contributes_notional    BOOLEAN NOT NULL,
    effective_from          TIMESTAMPTZ NOT NULL,
    effective_to            TIMESTAMPTZ,
    first_observed_at       TIMESTAMPTZ NOT NULL,
    configuration_sha256    CHAR(64) NOT NULL CHECK (configuration_sha256 ~ '^[0-9a-f]{64}$'),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE NULLS NOT DISTINCT (
        provider,
        semantics_version,
        condition_code,
        correction_code,
        effective_from
    ),
    CHECK (condition_code IS NOT NULL OR correction_code IS NOT NULL),
    CHECK (effective_to IS NULL OR effective_to > effective_from),
    CHECK (first_observed_at >= effective_from)
);

CREATE TABLE IF NOT EXISTS option_trade_watchlist (
    contract_id             BIGINT NOT NULL REFERENCES option_contract_catalog(contract_id),
    reason                  VARCHAR(24) NOT NULL CHECK (
        reason IN ('FILTERED', 'CANDIDATE', 'WORKING_ORDER', 'OPEN_POSITION')
    ),
    active_from             TIMESTAMPTZ NOT NULL,
    active_to               TIMESTAMPTZ,
    first_observed_at       TIMESTAMPTZ NOT NULL,
    source_id               VARCHAR(128) NOT NULL,
    backfill_from           TIMESTAMPTZ,
    backfill_through        TIMESTAMPTZ,
    backfill_status         VARCHAR(16) NOT NULL DEFAULT 'NOT_REQUIRED' CHECK (
        backfill_status IN ('NOT_REQUIRED', 'PENDING', 'RUNNING', 'COMPLETE', 'FAILED')
    ),
    backfill_error          TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (contract_id, reason, source_id, active_from),
    CHECK (active_to IS NULL OR active_to > active_from),
    CHECK (first_observed_at >= active_from),
    CHECK (
        (backfill_from IS NULL AND backfill_through IS NULL AND backfill_status = 'NOT_REQUIRED') OR
        (
            backfill_from IS NOT NULL AND backfill_through IS NOT NULL AND
            backfill_through >= backfill_from AND backfill_status <> 'NOT_REQUIRED'
        )
    ),
    CHECK (backfill_status <> 'FAILED' OR backfill_error IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_option_trade_watchlist_active
    ON option_trade_watchlist (contract_id, active_from, active_to);

CREATE TABLE IF NOT EXISTS option_raw_file_manifests (
    file_id                 UUID PRIMARY KEY,
    event_type              VARCHAR(24) NOT NULL CHECK (event_type IN ('SNAPSHOT', 'TRADE', 'QUOTE')),
    market_date             DATE NOT NULL,
    underlying              VARCHAR(16) NOT NULL,
    market_hour             SMALLINT NOT NULL CHECK (market_hour BETWEEN 0 AND 23),
    object_key              TEXT NOT NULL UNIQUE,
    schema_version          INTEGER NOT NULL CHECK (schema_version > 0),
    row_count               BIGINT NOT NULL CHECK (row_count >= 0),
    minimum_source_time     TIMESTAMPTZ,
    maximum_source_time     TIMESTAMPTZ,
    byte_size               BIGINT NOT NULL CHECK (byte_size >= 0),
    payload_sha256          CHAR(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    creation_status         VARCHAR(24) NOT NULL CHECK (
        creation_status IN ('WRITING', 'COMPLETE', 'MISSING', 'CORRUPT', 'DELETED')
    ),
    retention_class         VARCHAR(64) NOT NULL,
    deleted_at              TIMESTAMPTZ,
    deletion_reason         TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        minimum_source_time IS NULL OR maximum_source_time IS NULL OR
        maximum_source_time >= minimum_source_time
    ),
    CHECK (
        creation_status <> 'DELETED' OR
        (deleted_at IS NOT NULL AND deletion_reason IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_option_raw_manifests_partition
    ON option_raw_file_manifests (event_type, market_date, underlying, market_hour);

CREATE TABLE IF NOT EXISTS option_retention_holds (
    hold_id                 UUID PRIMARY KEY,
    scope_type              VARCHAR(24) NOT NULL CHECK (
        scope_type IN ('OBJECT', 'TABLE', 'PARTITION', 'FILE', 'DECISION', 'INCIDENT')
    ),
    selector                JSONB NOT NULL,
    reason                  TEXT NOT NULL,
    actor                   VARCHAR(128) NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at              TIMESTAMPTZ,
    released_at             TIMESTAMPTZ,
    released_by             VARCHAR(128),
    release_reason          TEXT,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (jsonb_typeof(selector) = 'object'),
    CHECK (expires_at IS NULL OR expires_at > created_at),
    CHECK (
        (released_at IS NULL AND released_by IS NULL AND release_reason IS NULL) OR
        (released_at IS NOT NULL AND released_by IS NOT NULL AND release_reason IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_option_retention_holds_active
    ON option_retention_holds (expires_at, hold_id)
    WHERE released_at IS NULL;

CREATE TABLE IF NOT EXISTS option_analysis_runs (
    matrix_id                   UUID PRIMARY KEY,
    batch_id                    UUID NOT NULL REFERENCES option_ingestion_runs(batch_id),
    underlying                  VARCHAR(16) NOT NULL,
    market_time                 TIMESTAMPTZ NOT NULL,
    observed_time               TIMESTAMPTZ NOT NULL,
    status                      VARCHAR(32) NOT NULL CHECK (
        status IN (
            'PENDING',
            'RUNNING',
            'COMPLETE',
            'INCOMPLETE_MATRIX',
            'REFERENCE_DRIFT_FAILED',
            'MODEL_QUALITY_FAILED',
            'FAILED'
        )
    ),
    received_contract_count     INTEGER NOT NULL DEFAULT 0 CHECK (received_contract_count >= 0),
    eligible_contract_count     INTEGER NOT NULL DEFAULT 0 CHECK (eligible_contract_count >= 0),
    unknown_reference_count     INTEGER NOT NULL DEFAULT 0 CHECK (unknown_reference_count >= 0),
    iv_attempt_count             INTEGER NOT NULL DEFAULT 0 CHECK (iv_attempt_count >= 0),
    iv_converged_count           INTEGER NOT NULL DEFAULT 0 CHECK (iv_converged_count >= 0),
    iv_convergence_fraction      DOUBLE PRECISION CHECK (
        iv_convergence_fraction IS NULL OR
        (iv_convergence_fraction >= 0 AND iv_convergence_fraction <= 1)
    ),
    quality_reasons             TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    chain_health                JSONB NOT NULL DEFAULT '{}'::jsonb,
    policy_version              VARCHAR(64) NOT NULL,
    policy_sha256               CHAR(64) NOT NULL CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    model_version               VARCHAR(64) NOT NULL,
    started_at                  TIMESTAMPTZ NOT NULL,
    completed_at                TIMESTAMPTZ,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (batch_id),
    CHECK (observed_time >= market_time),
    CHECK (iv_converged_count <= iv_attempt_count),
    CHECK (completed_at IS NULL OR completed_at >= started_at),
    CHECK (jsonb_typeof(chain_health) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_option_analysis_runs_context
    ON option_analysis_runs (underlying, market_time DESC, observed_time DESC);

CREATE TABLE IF NOT EXISTS option_expiration_analytics (
    matrix_id                   UUID NOT NULL REFERENCES option_analysis_runs(matrix_id),
    expiration_date             DATE NOT NULL,
    fractional_maturity_years   DOUBLE PRECISION NOT NULL CHECK (fractional_maturity_years >= 0),
    forward_price               NUMERIC(20,8) CHECK (forward_price IS NULL OR forward_price > 0),
    atm_iv                      DOUBLE PRECISION,
    call_25_delta_iv            DOUBLE PRECISION,
    put_25_delta_iv             DOUBLE PRECISION,
    call_skew_25_delta          DOUBLE PRECISION,
    put_skew_25_delta           DOUBLE PRECISION,
    risk_reversal_25_delta      DOUBLE PRECISION,
    interpolation_diagnostics   JSONB NOT NULL DEFAULT '{}'::jsonb,
    put_volume                  BIGINT CHECK (put_volume IS NULL OR put_volume >= 0),
    call_volume                 BIGINT CHECK (call_volume IS NULL OR call_volume >= 0),
    put_open_interest           BIGINT CHECK (put_open_interest IS NULL OR put_open_interest >= 0),
    call_open_interest          BIGINT CHECK (call_open_interest IS NULL OR call_open_interest >= 0),
    breadth                     INTEGER CHECK (breadth IS NULL OR breadth >= 0),
    concentration_metrics       JSONB NOT NULL DEFAULT '{}'::jsonb,
    wall_clusters               JSONB NOT NULL DEFAULT '[]'::jsonb,
    term_change                 DOUBLE PRECISION,
    term_slope                  DOUBLE PRECISION,
    quality_reasons             TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (matrix_id, expiration_date),
    CHECK (jsonb_typeof(interpolation_diagnostics) = 'object'),
    CHECK (jsonb_typeof(concentration_metrics) = 'object'),
    CHECK (jsonb_typeof(wall_clusters) = 'array')
);

CREATE OR REPLACE FUNCTION ensure_option_market_data_partitions(p_month_start DATE)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    month_start_utc TIMESTAMPTZ;
    next_month_utc TIMESTAMPTZ;
    month_suffix TEXT;
    snapshot_partition TEXT;
    trade_partition TEXT;
BEGIN
    IF p_month_start <> date_trunc('month', p_month_start)::DATE THEN
        RAISE EXCEPTION 'p_month_start must be the first day of a month';
    END IF;

    month_start_utc := p_month_start::TIMESTAMP AT TIME ZONE 'UTC';
    next_month_utc := (p_month_start + INTERVAL '1 month')::TIMESTAMP AT TIME ZONE 'UTC';
    month_suffix := to_char(p_month_start, 'YYYYMM');
    snapshot_partition := 'option_chain_snapshots_y' || month_suffix;
    trade_partition := 'option_trade_events_y' || month_suffix;

    PERFORM pg_advisory_xact_lock(
        hashtextextended('option-market-data-partition:' || month_suffix, 0)
    );

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF option_chain_snapshots FOR VALUES FROM (%L) TO (%L)',
        snapshot_partition,
        month_start_utc,
        next_month_utc
    );
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF option_trade_events FOR VALUES FROM (%L) TO (%L)',
        trade_partition,
        month_start_utc,
        next_month_utc
    );
END;
$$;

CREATE OR REPLACE FUNCTION option_market_data_partitions_ready(p_at TIMESTAMPTZ)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
AS $$
    SELECT
        to_regclass(
            'option_chain_snapshots_y' || to_char(p_at AT TIME ZONE 'UTC', 'YYYYMM')
        ) IS NOT NULL
        AND
        to_regclass(
            'option_trade_events_y' || to_char(p_at AT TIME ZONE 'UTC', 'YYYYMM')
        ) IS NOT NULL;
$$;

SELECT ensure_option_market_data_partitions(date_trunc('month', CURRENT_DATE)::DATE);
SELECT ensure_option_market_data_partitions(
    (date_trunc('month', CURRENT_DATE) + INTERVAL '1 month')::DATE
);

COMMENT ON TABLE option_contract_catalog IS
    'Stable option contract identities; provider/reference revisions are append-only version rows.';
COMMENT ON TABLE option_ingestion_runs IS
    'One idempotent provider/underlying/scheduled-cycle ingestion slot.';
COMMENT ON TABLE option_raw_batch_pages IS
    'Immutable raw REST pages; COMPLETE is permitted only after repository page-chain validation.';
COMMENT ON TABLE option_chain_snapshots IS
    'Provider-neutral immutable option snapshots partitioned by first observation time.';
COMMENT ON TABLE option_trade_events IS
    'Immutable option trade facts partitioned by SIP event time.';
COMMENT ON TABLE option_work_items IS
    'At-least-once durable work leases; database business keys enforce idempotent effects.';