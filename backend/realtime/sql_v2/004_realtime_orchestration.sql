SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

-- Independent stages. A stage declares its own prerequisites; an Options stage can
-- never be a prerequisite of an equity, reference or portal stage.
CREATE TABLE rt.stage_catalog (
    stage_id varchar(32) NOT NULL,
    version varchar(32) NOT NULL,
    domain varchar(12) NOT NULL,
    prerequisites text[] NOT NULL DEFAULT ARRAY[]::text[],
    capability_role varchar(32) NOT NULL,
    principal_profile varchar(32) NOT NULL,
    schedule_kind varchar(16) NOT NULL,
    gates_strategy boolean NOT NULL DEFAULT false,
    PRIMARY KEY (stage_id, version),
    CHECK (stage_id = upper(stage_id) AND btrim(stage_id) <> ''),
    CHECK (btrim(version) <> ''),
    CHECK (domain IN ('shared', 'equity', 'options')),
    CHECK (NOT (stage_id = ANY (prerequisites))),
    CHECK (principal_profile IN ('runtime_data', 'runtime_options')),
    CHECK ((domain = 'options') = (principal_profile = 'runtime_options')),
    CHECK (schedule_kind IN ('INTERVAL', 'WINDOW', 'SESSION', 'COALESCED', 'CONTINUOUS')),
    CHECK (NOT gates_strategy OR domain = 'options')
);

CREATE FUNCTION rt.validate_stage_catalog() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'rt'
    AS $$
DECLARE
    prerequisite text;
    prerequisite_domain varchar;
BEGIN
    FOREACH prerequisite IN ARRAY NEW.prerequisites LOOP
        SELECT domain INTO prerequisite_domain FROM rt.stage_catalog
        WHERE stage_id = prerequisite AND version = NEW.version;
        IF prerequisite_domain IS NULL THEN
            RAISE EXCEPTION 'stage % prerequisite % is not defined earlier in version %',
                NEW.stage_id, prerequisite, NEW.version;
        END IF;
        IF NEW.domain <> 'options' AND prerequisite_domain = 'options' THEN
            RAISE EXCEPTION 'non-options stage % cannot depend on options stage %',
                NEW.stage_id, prerequisite;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_rt_stage_catalog_validate
    BEFORE INSERT ON rt.stage_catalog
    FOR EACH ROW EXECUTE FUNCTION rt.validate_stage_catalog();

INSERT INTO rt.stage_catalog
    (stage_id, version, domain, prerequisites, capability_role, principal_profile, schedule_kind, gates_strategy)
VALUES
    ('REFERENCE', 'stages_v1', 'shared', ARRAY[]::text[], 'references', 'runtime_data', 'INTERVAL', false),
    ('EQUITY_INGEST', 'stages_v1', 'equity', ARRAY['REFERENCE'], 'ingress', 'runtime_data', 'WINDOW', false),
    ('EQUITY_EVIDENCE', 'stages_v1', 'equity', ARRAY['EQUITY_INGEST'], 'compute', 'runtime_data', 'WINDOW', false),
    ('PORTAL_PUBLISH', 'stages_v1', 'equity', ARRAY['EQUITY_EVIDENCE'], 'publication', 'runtime_data', 'COALESCED', false),
    ('STRATEGY_PUBLISH', 'stages_v1', 'equity', ARRAY['EQUITY_EVIDENCE'], 'publication', 'runtime_data', 'COALESCED', false),
    ('STOCK_ALERTS', 'stages_v1', 'equity', ARRAY['EQUITY_EVIDENCE'], 'compute', 'runtime_data', 'WINDOW', false),
    ('OPTIONS_ACQUIRE', 'stages_v1', 'options', ARRAY['REFERENCE'], 'options_ingress', 'runtime_options', 'WINDOW', false),
    ('OPTIONS_ADMISSION', 'stages_v1', 'options', ARRAY['OPTIONS_ACQUIRE'], 'options_compute', 'runtime_options', 'SESSION', false),
    ('OPTIONS_CYCLE', 'stages_v1', 'options', ARRAY['OPTIONS_ADMISSION'], 'options_compute', 'runtime_options', 'WINDOW', true),
    ('OPTIONS_FLOW', 'stages_v1', 'options', ARRAY['REFERENCE'], 'options_compute', 'runtime_options', 'CONTINUOUS', false);

CREATE TRIGGER trg_rt_stage_catalog_immutable
    BEFORE UPDATE OR DELETE ON rt.stage_catalog
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_stage_catalog_no_truncate
    BEFORE TRUNCATE ON rt.stage_catalog
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

-- The approved scope of one resident run. Startup validates only the enabled stages.
CREATE TABLE rt.service_plans (
    plan_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    session_date date NOT NULL,
    stage_catalog_version varchar(32) NOT NULL,
    enabled_stages text[] NOT NULL,
    config_sha256 char(64) NOT NULL,
    deadline timestamptz NOT NULL,
    scope_reference text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (cohort_id, session_date, config_sha256),
    CHECK (cardinality(enabled_stages) > 0),
    CHECK (config_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (btrim(scope_reference) <> '')
);

CREATE FUNCTION rt.validate_service_plan() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'rt'
    AS $$
DECLARE
    stage text;
BEGIN
    FOREACH stage IN ARRAY NEW.enabled_stages LOOP
        IF NOT EXISTS (
            SELECT 1 FROM rt.stage_catalog
            WHERE stage_id = stage AND version = NEW.stage_catalog_version
        ) THEN
            RAISE EXCEPTION 'service plan stage % is not in catalog version %',
                stage, NEW.stage_catalog_version;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_rt_service_plans_validate
    BEFORE INSERT ON rt.service_plans
    FOR EACH ROW EXECUTE FUNCTION rt.validate_service_plan();
CREATE TRIGGER trg_rt_service_plans_immutable
    BEFORE UPDATE OR DELETE ON rt.service_plans
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_service_plans_no_truncate
    BEFORE TRUNCATE ON rt.service_plans
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

-- Window state is an aggregate; it never stops sibling stages.
CREATE TABLE rt.window_runs (
    window_id uuid PRIMARY KEY,
    plan_id uuid NOT NULL REFERENCES rt.service_plans(plan_id),
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    window_key varchar(64) NOT NULL,
    release_at timestamptz NOT NULL,
    cutoff timestamptz NOT NULL,
    deadline timestamptz NOT NULL,
    state varchar(16) NOT NULL DEFAULT 'PENDING',
    owner_id uuid,
    owner_epoch bigint NOT NULL DEFAULT 0,
    lease_until timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (cohort_id, window_key),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (btrim(window_key) <> ''),
    CHECK (cutoff <= release_at AND release_at < deadline),
    CHECK (state IN ('PENDING', 'RUNNING', 'COMPLETE', 'FAILED', 'MISSED', 'RETAINED_GAP')),
    CHECK (owner_epoch >= 0),
    CHECK ((state <> 'RUNNING') OR (owner_id IS NOT NULL AND owner_epoch > 0 AND lease_until IS NOT NULL))
) WITH (fillfactor = 80);

CREATE INDEX idx_rt_window_run_open
    ON rt.window_runs (cohort_id, release_at)
    WHERE state IN ('PENDING', 'RUNNING');

CREATE TABLE rt.cycle_receipts (
    receipt_id uuid PRIMARY KEY,
    window_id uuid NOT NULL REFERENCES rt.window_runs(window_id),
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    receipt_kind varchar(16) NOT NULL,
    cutoff timestamptz NOT NULL,
    reference_generation bigint NOT NULL,
    universe_sha256 char(64) NOT NULL,
    input_sha256 char(64) NOT NULL,
    build_id varchar(96) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (window_id, receipt_kind),
    CHECK (receipt_kind IN ('EQUITY_CYCLE', 'OPTIONS_CYCLE')),
    CHECK (reference_generation > 0),
    CHECK (universe_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (btrim(build_id) <> '')
);

CREATE TABLE rt.stage_runs (
    stage_run_id uuid PRIMARY KEY,
    window_id uuid NOT NULL REFERENCES rt.window_runs(window_id),
    stage_id varchar(32) NOT NULL,
    stage_version varchar(32) NOT NULL,
    attempt integer NOT NULL DEFAULT 1,
    state varchar(16) NOT NULL,
    blocked_by_stage varchar(32),
    receipt_id uuid REFERENCES rt.cycle_receipts(receipt_id),
    started_at timestamptz,
    finished_at timestamptz,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (stage_id, stage_version) REFERENCES rt.stage_catalog(stage_id, version),
    UNIQUE (window_id, stage_id, attempt),
    CHECK (attempt >= 1),
    CHECK (state IN (
        'PENDING', 'RUNNING', 'COMPLETE', 'FAILED',
        'BLOCKED_PREREQ', 'BLOCKED_POLICY', 'MISSED'
    )),
    CHECK (blocked_by_stage IS NULL OR blocked_by_stage <> stage_id),
    CHECK ((state = 'BLOCKED_PREREQ') = (blocked_by_stage IS NOT NULL)),
    CHECK (state NOT IN ('FAILED', 'BLOCKED_PREREQ', 'BLOCKED_POLICY', 'MISSED')
        OR cardinality(reason_codes) > 0),
    CHECK (state <> 'COMPLETE' OR finished_at IS NOT NULL),
    CHECK (finished_at IS NULL OR started_at IS NULL OR finished_at >= started_at)
) WITH (fillfactor = 80);

CREATE INDEX idx_rt_stage_run_window ON rt.stage_runs (window_id, stage_id, attempt DESC);

-- A stage can be BLOCKED_PREREQ only by a declared prerequisite that did not complete
-- in the same window, so a failed Options stage cannot block an unrelated stage.
CREATE FUNCTION rt.validate_stage_run() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'rt'
    AS $$
DECLARE
    prior_state varchar;
BEGIN
    IF NEW.state = 'BLOCKED_PREREQ' THEN
        IF NOT EXISTS (
            SELECT 1 FROM rt.stage_catalog
            WHERE stage_id = NEW.stage_id AND version = NEW.stage_version
              AND NEW.blocked_by_stage = ANY (prerequisites)
        ) THEN
            RAISE EXCEPTION 'stage % is not blocked by a declared prerequisite (%)',
                NEW.stage_id, NEW.blocked_by_stage;
        END IF;
        SELECT state INTO prior_state FROM rt.stage_runs
        WHERE window_id = NEW.window_id AND stage_id = NEW.blocked_by_stage
        ORDER BY attempt DESC LIMIT 1;
        IF prior_state IS NULL
           OR prior_state NOT IN ('FAILED', 'BLOCKED_PREREQ', 'BLOCKED_POLICY', 'MISSED') THEN
            RAISE EXCEPTION 'prerequisite % has not failed or been blocked in this window',
                NEW.blocked_by_stage;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_rt_stage_runs_validate
    BEFORE INSERT OR UPDATE ON rt.stage_runs
    FOR EACH ROW EXECUTE FUNCTION rt.validate_stage_run();

-- Write fence for stage runners. A stage's ordinary writes call this as the last statement of their
-- transaction (V2ConnectionProvider.transaction with a StageRunFence): it fails with SQLSTATE RT001
-- unless the stage run is still RUNNING, which rolls the whole transaction back. The row lock
-- (FOR SHARE) is held through COMMIT, so the coordinator's timeout (an UPDATE of the same row)
-- either lands before the check and the write is rejected, or waits for the commit that was
-- legitimately made while RUNNING. The lock is taken only at the end, so a hung runner that never
-- reaches its commit cannot delay the coordinator. SECURITY DEFINER: stage roles hold no privilege
-- on rt.stage_runs.
CREATE FUNCTION rt.fence_stage_run(p_stage_run_id uuid) RETURNS void
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'rt'
    AS $$
BEGIN
    PERFORM 1 FROM rt.stage_runs WHERE stage_run_id = p_stage_run_id AND state = 'RUNNING' FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'stage run % is not RUNNING; its writes are rejected', p_stage_run_id
            USING ERRCODE = 'RT001';
    END IF;
END;
$$;

REVOKE ALL ON FUNCTION rt.fence_stage_run(uuid) FROM PUBLIC;

CREATE TABLE rt.stage_readiness_current (
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    stage_id varchar(32) NOT NULL,
    stage_version varchar(32) NOT NULL,
    status varchar(16) NOT NULL,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    evidence_ref text,
    checked_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, stage_id),
    FOREIGN KEY (stage_id, stage_version) REFERENCES rt.stage_catalog(stage_id, version),
    CHECK (status IN ('READY', 'DEGRADED', 'BLOCKED')),
    CHECK ((status = 'READY' AND cardinality(reason_codes) = 0) OR status <> 'READY'),
    CHECK ((status <> 'READY' AND cardinality(reason_codes) > 0) OR status = 'READY')
) WITH (fillfactor = 80);

CREATE TABLE rt.release_current (
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    receipt_id uuid NOT NULL REFERENCES rt.cycle_receipts(receipt_id),
    cutoff timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, provider_tier),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER'))
) WITH (fillfactor = 80);

CREATE TRIGGER trg_rt_cycle_receipts_immutable
    BEFORE UPDATE OR DELETE ON rt.cycle_receipts
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_cycle_receipts_no_truncate
    BEFORE TRUNCATE ON rt.cycle_receipts
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

-- Account-wide provider request ledger: shared by every lane so pacing, completeness
-- and ambiguous attempts are visible in one place. Rows are pruned after three months.
CREATE TABLE rt.provider_request_ledger (
    request_id uuid PRIMARY KEY,
    account_alias varchar(64) NOT NULL,
    lane varchar(24) NOT NULL,
    cohort_id uuid REFERENCES rt.cohorts(cohort_id),
    window_id uuid REFERENCES rt.window_runs(window_id),
    stage_run_id uuid REFERENCES rt.stage_runs(stage_run_id),
    endpoint_class varchar(48) NOT NULL,
    request_sha256 char(64) NOT NULL,
    attempt integer NOT NULL DEFAULT 1,
    state varchar(16) NOT NULL DEFAULT 'RESERVED',
    reserved_at timestamptz NOT NULL,
    started_at timestamptz,
    completed_at timestamptz,
    http_status integer,
    outcome varchar(16),
    response_bytes bigint,
    row_count integer,
    pages_fetched integer,
    requested_from timestamptz,
    requested_to timestamptz,
    price_basis varchar(40) NOT NULL DEFAULT 'NOT_APPLICABLE',
    completeness varchar(16) NOT NULL DEFAULT 'NOT_APPLICABLE',
    failure_code varchar(64),
    payload_id uuid,
    CHECK (lane IN (
        'EQUITY_CURRENT', 'EQUITY_HISTORY', 'OPTIONS_CHAIN', 'OPTIONS_ALIGNMENT',
        'REFERENCE', 'PRE_SESSION_STAGING'
    )),
    CHECK (request_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (attempt >= 1),
    CHECK (state IN ('RESERVED', 'COMPLETE', 'FAILED', 'AMBIGUOUS')),
    CHECK (outcome IS NULL OR outcome IN ('OK', 'HTTP_429', 'HTTP_ERROR', 'TIMEOUT', 'INVALID')),
    CHECK (state NOT IN ('COMPLETE', 'FAILED') OR (completed_at IS NOT NULL AND outcome IS NOT NULL)),
    CHECK (state <> 'FAILED' OR failure_code IS NOT NULL),
    CHECK (completed_at IS NULL OR started_at IS NULL OR completed_at >= started_at),
    CHECK (requested_to IS NULL OR requested_from IS NULL OR requested_to >= requested_from),
    CHECK (price_basis IN ('NOT_APPLICABLE', 'RAW_FINALIZED_MINUTE_CLOSE_V1', 'ADJUSTED_BAR_START_V1')),
    CHECK (completeness IN ('COMPLETE', 'INCOMPLETE', 'NOT_APPLICABLE')),
    CHECK (completeness <> 'INCOMPLETE' OR failure_code IS NOT NULL),
    CHECK (response_bytes IS NULL OR response_bytes >= 0),
    CHECK (pages_fetched IS NULL OR pages_fetched >= 0)
) WITH (fillfactor = 80);

CREATE INDEX idx_rt_provider_request_pacing
    ON rt.provider_request_ledger (account_alias, reserved_at DESC);
CREATE INDEX idx_rt_provider_request_window
    ON rt.provider_request_ledger (window_id, lane)
    WHERE window_id IS NOT NULL;
CREATE INDEX idx_rt_provider_request_open
    ON rt.provider_request_ledger (reserved_at)
    WHERE state IN ('RESERVED', 'AMBIGUOUS');

-- Incremental acquisition: last complete bar per cohort, instrument and interval, with a
-- terminal state so one no-trade minute does not fail a whole window.
CREATE TABLE rt.acquisition_watermarks (
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    interval varchar(8) NOT NULL,
    source_kind varchar(24) NOT NULL,
    last_complete_bar_end timestamptz NOT NULL,
    overlap_policy varchar(64) NOT NULL,
    terminal_state varchar(16) NOT NULL,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    request_id uuid REFERENCES rt.provider_request_ledger(request_id),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, instrument_id, interval, source_kind),
    CHECK (interval IN ('1m', '5m', '15m', '30m', '1h', '1d', '1wk', '1mo')),
    CHECK (source_kind IN ('NATIVE_REST', 'REALTIME_STREAM', 'DERIVED', 'RECONCILED')),
    CHECK (btrim(overlap_policy) <> ''),
    CHECK (terminal_state IN ('COMPLETE', 'NO_TRADE', 'GAP')),
    CHECK (terminal_state = 'COMPLETE' OR cardinality(reason_codes) > 0)
) WITH (fillfactor = 80);

-- DATA-01: genuine depth shortages stay visible per allocation and never gate other stages.
CREATE TABLE rt.option_strategy_admissions (
    admission_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    session_date date NOT NULL,
    policy_sha256 char(64) NOT NULL,
    underlyer varchar(32) NOT NULL,
    target_dte integer NOT NULL,
    side varchar(4) NOT NULL,
    required_depth integer NOT NULL,
    qualifying_count integer NOT NULL,
    status varchar(8) NOT NULL,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    evidence_ref text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (cohort_id, session_date, policy_sha256, underlyer, target_dte, side),
    CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (underlyer = upper(underlyer) AND btrim(underlyer) <> ''),
    CHECK (target_dte > 0),
    CHECK (side IN ('CALL', 'PUT')),
    CHECK (required_depth > 0 AND qualifying_count >= 0),
    CHECK (status IN ('READY', 'BLOCKED')),
    CHECK (status <> 'READY' OR qualifying_count >= required_depth),
    CHECK (status <> 'BLOCKED' OR cardinality(reason_codes) > 0)
);

CREATE TRIGGER trg_rt_option_strategy_admissions_immutable
    BEFORE UPDATE OR DELETE ON rt.option_strategy_admissions
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_strategy_admissions_no_truncate
    BEFORE TRUNCATE ON rt.option_strategy_admissions
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

-- Verified session archive files (alerts, evaluations, outcome addenda). A partition is
-- dropped only when every session in it has verified archives and no open pin.
CREATE TABLE rt.session_archives (
    archive_id uuid PRIMARY KEY,
    session_date date NOT NULL,
    kind varchar(24) NOT NULL,
    horizon_close_date date,
    uri text NOT NULL,
    sha256 char(64) NOT NULL,
    row_count bigint NOT NULL,
    schema_version varchar(64) NOT NULL,
    build_id varchar(96) NOT NULL,
    verified_at timestamptz NOT NULL,
    UNIQUE (session_date, kind, sha256),
    CHECK (kind IN ('STOCK_ALERTS', 'OPTION_ALERTS', 'DETECTOR_EVALUATIONS', 'OPTION_FLOW_EVENTS',
                    'OPTION_FLOW_CLUSTERS', 'OUTCOMES')),
    CHECK ((kind = 'OUTCOMES') = (horizon_close_date IS NOT NULL)),
    CHECK (btrim(uri) <> '' AND btrim(schema_version) <> '' AND btrim(build_id) <> ''),
    CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (row_count >= 0)
);

CREATE TRIGGER trg_rt_session_archives_immutable
    BEFORE UPDATE OR DELETE ON rt.session_archives
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_session_archives_no_truncate
    BEFORE TRUNCATE ON rt.session_archives
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

-- RT-05: bounded, redacted evidence for why an underlyer produced zero candidates.
CREATE TABLE rt.normalization_diagnostics (
    diagnostic_id uuid NOT NULL,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    stage_run_id uuid REFERENCES rt.stage_runs(stage_run_id),
    underlyer varchar(32) NOT NULL,
    request_ids uuid[] NOT NULL DEFAULT ARRAY[]::uuid[],
    requested_from timestamptz,
    requested_to timestamptz,
    returned_from timestamptz,
    returned_to timestamptz,
    bar_count integer NOT NULL,
    pages_fetched integer NOT NULL,
    pagination_complete boolean NOT NULL,
    bar_acquisition varchar(32) NOT NULL,
    price_basis varchar(40) NOT NULL,
    observation_min timestamptz,
    observation_max timestamptz,
    raw_rows integer NOT NULL,
    usable_mark_count integer NOT NULL,
    candidates integer NOT NULL,
    rejection_counts jsonb NOT NULL DEFAULT '{}'::jsonb,
    rejection_samples jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (diagnostic_id, created_at),
    CHECK (underlyer = upper(underlyer) AND btrim(underlyer) <> ''),
    CHECK (bar_count >= 0 AND pages_fetched >= 0 AND raw_rows >= 0 AND usable_mark_count >= 0 AND candidates >= 0),
    CHECK (bar_acquisition IN ('FETCHED', 'SKIPPED_NO_USABLE_MARKS')),
    CHECK (price_basis IN ('NOT_APPLICABLE', 'RAW_FINALIZED_MINUTE_CLOSE_V1', 'ADJUSTED_BAR_START_V1')),
    CHECK (jsonb_typeof(rejection_counts) = 'object'),
    CHECK (jsonb_typeof(rejection_samples) = 'array'),
    CHECK (octet_length(rejection_samples::text) <= 16384),
    CHECK (requested_to IS NULL OR requested_from IS NULL OR requested_to >= requested_from),
    CHECK (observation_max IS NULL OR observation_min IS NULL OR observation_max >= observation_min),
    -- Every zero-candidate underlyer must carry an explicit reason.
    CHECK (candidates > 0 OR rejection_counts <> '{}'::jsonb)
) PARTITION BY RANGE (created_at);

CREATE INDEX idx_rt_normalization_diagnostic_underlyer
    ON rt.normalization_diagnostics (cohort_id, underlyer, created_at DESC);

CREATE TRIGGER trg_rt_normalization_diagnostics_immutable
    BEFORE UPDATE OR DELETE ON rt.normalization_diagnostics
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
