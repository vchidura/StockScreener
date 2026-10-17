SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE rt.stream_gaps
    ADD COLUMN plan_id uuid REFERENCES rt.advanced_ingress_plans(plan_id),
    ADD COLUMN last_durable_watermark timestamptz,
    ADD COLUMN reconnect_at timestamptz,
    ADD COLUMN first_post_reconnect_event_at timestamptz,
    ADD COLUMN first_post_reconnect_offset bigint,
    ADD COLUMN continuity_class varchar(40),
    ADD COLUMN classification_sha256 char(64),
    ADD COLUMN operator_alert_required boolean NOT NULL DEFAULT false,
    ADD CONSTRAINT stream_gaps_reconnect_evidence_check CHECK (
        reconnect_at IS NULL OR reconnect_at >= gap_start
    ),
    ADD CONSTRAINT stream_gaps_first_post_event_check CHECK (
        (
            first_post_reconnect_event_at IS NULL
            AND first_post_reconnect_offset IS NULL
        )
        OR
        (
            reconnect_at IS NOT NULL
            AND first_post_reconnect_event_at IS NOT NULL
            AND first_post_reconnect_offset >= 0
        )
    ),
    ADD CONSTRAINT stream_gaps_continuity_class_check CHECK (
        continuity_class IS NULL
        OR continuity_class IN (
            'LOCAL_OFFSET_DISCONTINUITY',
            'PROVIDER_TIME_OUTAGE',
            'NO_TRADE_INTERVAL',
            'UNKNOWN_CONTINUITY'
        )
    ),
    ADD CONSTRAINT stream_gaps_classification_sha_check CHECK (
        classification_sha256 IS NULL
        OR classification_sha256 ~ '^[0-9a-f]{64}$'
    );

CREATE TABLE rt.advanced_gap_repairs (
    repair_id uuid PRIMARY KEY,
    gap_id uuid NOT NULL UNIQUE REFERENCES rt.stream_gaps(gap_id),
    plan_id uuid NOT NULL REFERENCES rt.advanced_ingress_plans(plan_id),
    allocation_kind varchar(24) NOT NULL,
    instrument_id bigint REFERENCES rt.instruments(instrument_id),
    ticker varchar(32) NOT NULL,
    requested_from timestamptz NOT NULL,
    requested_to timestamptz NOT NULL,
    request_id uuid UNIQUE REFERENCES rt.provider_request_ledger(request_id),
    scope_sha256 char(64) NOT NULL UNIQUE,
    reserved_response_bytes bigint NOT NULL,
    status varchar(16) NOT NULL,
    sealed_payload_id uuid,
    canonical_coverage_sha256 char(64),
    reason_code varchar(64),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    reserved_at timestamptz,
    sealed_at timestamptz,
    replayed_at timestamptz,
    verified_at timestamptz,
    repaired_at timestamptz,
    CHECK (allocation_kind IN ('LIVE_GAP', 'STARTUP_BACKFILL')),
    CHECK (
        ticker = upper(ticker)
        AND btrim(ticker) <> ''
        AND requested_to > requested_from
        AND reserved_response_bytes > 0
        AND scope_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CHECK (
        status IN (
            'PLANNED',
            'RESERVED',
            'SEALED',
            'REPLAYED',
            'VERIFIED',
            'REPAIRED',
            'BLOCKED'
        )
    ),
    CHECK (
        canonical_coverage_sha256 IS NULL
        OR canonical_coverage_sha256 ~ '^[0-9a-f]{64}$'
    )
);

CREATE TABLE rt.advanced_repair_payloads (
    payload_id uuid PRIMARY KEY,
    repair_id uuid NOT NULL UNIQUE REFERENCES
        rt.advanced_gap_repairs(repair_id),
    request_id uuid NOT NULL UNIQUE REFERENCES
        rt.provider_request_ledger(request_id),
    payload_sha256 char(64) NOT NULL,
    payload_bytes bigint NOT NULL,
    body bytea NOT NULL,
    sealed_at timestamptz NOT NULL,
    CHECK (
        payload_sha256 ~ '^[0-9a-f]{64}$'
        AND payload_bytes > 0
        AND payload_bytes = octet_length(body)
        AND payload_bytes <= 134217728
    )
);

CREATE TRIGGER trg_rt_advanced_repair_payloads_immutable
    BEFORE DELETE OR UPDATE ON rt.advanced_repair_payloads
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TABLE rt.advanced_gap_dependencies (
    gap_id uuid NOT NULL REFERENCES rt.stream_gaps(gap_id),
    window_id uuid NOT NULL REFERENCES rt.window_runs(window_id),
    status varchar(16) NOT NULL,
    blocked_at timestamptz NOT NULL,
    released_at timestamptz,
    PRIMARY KEY (gap_id, window_id),
    CHECK (status IN ('BLOCKED', 'RELEASED')),
    CHECK (
        (status = 'BLOCKED' AND released_at IS NULL)
        OR
        (
            status = 'RELEASED'
            AND released_at IS NOT NULL
            AND released_at >= blocked_at
        )
    )
);

CREATE TABLE rt.advanced_gap_alerts (
    alert_id uuid PRIMARY KEY,
    gap_id uuid NOT NULL UNIQUE REFERENCES rt.stream_gaps(gap_id),
    reason_code varchar(64) NOT NULL,
    severity varchar(16) NOT NULL,
    status varchar(16) NOT NULL,
    created_at timestamptz NOT NULL,
    resolved_at timestamptz,
    CHECK (
        btrim(reason_code) <> ''
        AND severity IN ('WARNING', 'CRITICAL')
        AND status IN ('OPEN', 'RESOLVED')
        AND (
            (status = 'OPEN' AND resolved_at IS NULL)
            OR
            (
                status = 'RESOLVED'
                AND resolved_at IS NOT NULL
                AND resolved_at >= created_at
            )
        )
    )
);

CREATE TABLE rt.advanced_backfill_runs (
    backfill_id uuid PRIMARY KEY,
    plan_id uuid NOT NULL REFERENCES rt.advanced_ingress_plans(plan_id),
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    session_dates date[] NOT NULL,
    universe_target integer NOT NULL,
    request_ceiling integer NOT NULL,
    decoded_byte_ceiling bigint NOT NULL,
    expected_cost varchar(96) NOT NULL,
    scope_sha256 char(64) NOT NULL UNIQUE,
    state varchar(24) NOT NULL,
    reason_code varchar(64) NOT NULL,
    next_action varchar(160) NOT NULL,
    requests_reserved integer NOT NULL DEFAULT 0,
    bytes_reserved bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    CHECK (
        cardinality(session_dates) BETWEEN 1 AND 2
        AND universe_target BETWEEN 1 AND 400
        AND request_ceiling >= 0
        AND decoded_byte_ceiling >= 0
        AND requests_reserved BETWEEN 0 AND request_ceiling
        AND bytes_reserved BETWEEN 0 AND decoded_byte_ceiling
        AND scope_sha256 ~ '^[0-9a-f]{64}$'
        AND btrim(expected_cost) <> ''
        AND btrim(reason_code) <> ''
        AND btrim(next_action) <> ''
    ),
    CHECK (
        state IN (
            'BACKFILL_PLANNED',
            'BACKFILL_RUNNING',
            'BACKFILL_BLOCKED',
            'BACKFILL_COMPLETE',
            'WAIT_NEXT_SESSION'
        )
    ),
    CHECK (
        completed_at IS NULL
        OR state IN ('BACKFILL_COMPLETE', 'WAIT_NEXT_SESSION')
    )
);

CREATE TABLE rt.advanced_backfill_work (
    work_id uuid PRIMARY KEY,
    backfill_id uuid NOT NULL REFERENCES
        rt.advanced_backfill_runs(backfill_id),
    session_date date NOT NULL,
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    ticker varchar(32) NOT NULL,
    requested_from timestamptz NOT NULL,
    requested_to timestamptz NOT NULL,
    expected_raw_1m integer NOT NULL,
    expected_raw_5m integer NOT NULL,
    request_id uuid UNIQUE REFERENCES rt.provider_request_ledger(request_id),
    status varchar(16) NOT NULL,
    coverage_sha256 char(64),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    UNIQUE (backfill_id, session_date, instrument_id, requested_from, requested_to),
    CHECK (
        ticker = upper(ticker)
        AND btrim(ticker) <> ''
        AND requested_to > requested_from
        AND expected_raw_1m > 0
        AND expected_raw_5m > 0
        AND status IN (
            'MISSING',
            'RESERVED',
            'SEALED',
            'PERSISTED',
            'VERIFIED',
            'BLOCKED'
        )
        AND (
            coverage_sha256 IS NULL
            OR coverage_sha256 ~ '^[0-9a-f]{64}$'
        )
    )
);

CREATE TABLE rt.advanced_backfill_releases (
    release_id uuid PRIMARY KEY,
    backfill_id uuid NOT NULL UNIQUE REFERENCES
        rt.advanced_backfill_runs(backfill_id),
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    coverage_sha256 char(64) NOT NULL,
    job_barrier_sha256 char(64) NOT NULL,
    published_at timestamptz NOT NULL,
    platform_pointer_advanced boolean NOT NULL DEFAULT false,
    CHECK (
        coverage_sha256 ~ '^[0-9a-f]{64}$'
        AND job_barrier_sha256 ~ '^[0-9a-f]{64}$'
        AND platform_pointer_advanced = false
    )
);

CREATE TRIGGER trg_rt_advanced_backfill_releases_immutable
    BEFORE DELETE OR UPDATE ON rt.advanced_backfill_releases
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE FUNCTION rt.enforce_advanced_gap_repair_change()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    old_rank integer;
    new_rank integer;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'advanced gap repairs are durable evidence';
    END IF;
    IF (
        NEW.repair_id,
        NEW.gap_id,
        NEW.plan_id,
        NEW.allocation_kind,
        NEW.instrument_id,
        NEW.ticker,
        NEW.requested_from,
        NEW.requested_to,
        NEW.scope_sha256,
        NEW.reserved_response_bytes,
        NEW.created_at
    ) IS DISTINCT FROM (
        OLD.repair_id,
        OLD.gap_id,
        OLD.plan_id,
        OLD.allocation_kind,
        OLD.instrument_id,
        OLD.ticker,
        OLD.requested_from,
        OLD.requested_to,
        OLD.scope_sha256,
        OLD.reserved_response_bytes,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'advanced gap repair scope is immutable';
    END IF;
    old_rank = array_position(
        ARRAY['PLANNED','RESERVED','SEALED','REPLAYED','VERIFIED','REPAIRED'],
        OLD.status
    );
    new_rank = array_position(
        ARRAY['PLANNED','RESERVED','SEALED','REPLAYED','VERIFIED','REPAIRED'],
        NEW.status
    );
    IF NEW.status <> 'BLOCKED'
       AND OLD.status <> NEW.status
       AND (old_rank IS NULL OR new_rank IS NULL OR new_rank < old_rank) THEN
        RAISE EXCEPTION 'advanced gap repair status cannot move backward';
    END IF;
    IF OLD.status IN ('REPAIRED','BLOCKED') AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'terminal advanced gap repair is immutable';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_gap_repairs_durable
    BEFORE UPDATE OR DELETE ON rt.advanced_gap_repairs
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_gap_repair_change();

CREATE FUNCTION rt.enforce_advanced_backfill_run_change()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'advanced backfill runs are durable evidence';
    END IF;
    IF (
        NEW.backfill_id,
        NEW.plan_id,
        NEW.cohort_id,
        NEW.session_dates,
        NEW.universe_target,
        NEW.request_ceiling,
        NEW.decoded_byte_ceiling,
        NEW.expected_cost,
        NEW.scope_sha256,
        NEW.created_at
    ) IS DISTINCT FROM (
        OLD.backfill_id,
        OLD.plan_id,
        OLD.cohort_id,
        OLD.session_dates,
        OLD.universe_target,
        OLD.request_ceiling,
        OLD.decoded_byte_ceiling,
        OLD.expected_cost,
        OLD.scope_sha256,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'advanced backfill scope is immutable';
    END IF;
    IF NEW.requests_reserved < OLD.requests_reserved
       OR NEW.bytes_reserved < OLD.bytes_reserved
       OR NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'advanced backfill counters cannot reset';
    END IF;
    IF OLD.state = 'BACKFILL_PLANNED'
       AND NEW.state IN (
           'BACKFILL_PLANNED',
           'BACKFILL_RUNNING',
           'BACKFILL_BLOCKED',
           'BACKFILL_COMPLETE'
       ) THEN
        RETURN NEW;
    END IF;
    IF OLD.state = 'BACKFILL_RUNNING'
       AND NEW.state IN (
           'BACKFILL_RUNNING',
           'BACKFILL_BLOCKED',
           'BACKFILL_COMPLETE'
       ) THEN
        RETURN NEW;
    END IF;
    IF OLD.state = 'BACKFILL_COMPLETE'
       AND NEW.state IN ('BACKFILL_COMPLETE', 'WAIT_NEXT_SESSION') THEN
        RETURN NEW;
    END IF;
    IF OLD.state = 'BACKFILL_BLOCKED'
       AND NEW.state = 'BACKFILL_BLOCKED' THEN
        RETURN NEW;
    END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'invalid advanced backfill transition % -> %',
        OLD.state, NEW.state;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_backfill_runs_durable
    BEFORE UPDATE OR DELETE ON rt.advanced_backfill_runs
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_backfill_run_change();

CREATE FUNCTION rt.enforce_advanced_backfill_work_change()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    old_rank integer;
    new_rank integer;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'advanced backfill work is durable evidence';
    END IF;
    IF (
        NEW.work_id,
        NEW.backfill_id,
        NEW.session_date,
        NEW.instrument_id,
        NEW.ticker,
        NEW.requested_from,
        NEW.requested_to,
        NEW.expected_raw_1m,
        NEW.expected_raw_5m,
        NEW.created_at
    ) IS DISTINCT FROM (
        OLD.work_id,
        OLD.backfill_id,
        OLD.session_date,
        OLD.instrument_id,
        OLD.ticker,
        OLD.requested_from,
        OLD.requested_to,
        OLD.expected_raw_1m,
        OLD.expected_raw_5m,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'advanced backfill work identity is immutable';
    END IF;
    old_rank = array_position(
        ARRAY['MISSING','RESERVED','SEALED','PERSISTED','VERIFIED'],
        OLD.status
    );
    new_rank = array_position(
        ARRAY['MISSING','RESERVED','SEALED','PERSISTED','VERIFIED'],
        NEW.status
    );
    IF NEW.status <> 'BLOCKED'
       AND OLD.status <> NEW.status
       AND (old_rank IS NULL OR new_rank IS NULL OR new_rank < old_rank) THEN
        RAISE EXCEPTION 'advanced backfill work cannot move backward';
    END IF;
    IF OLD.status IN ('VERIFIED','BLOCKED') AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'terminal advanced backfill work is immutable';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_backfill_work_durable
    BEFORE UPDATE OR DELETE ON rt.advanced_backfill_work
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_backfill_work_change();

CREATE FUNCTION rt.enforce_advanced_gap_dependency_change()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'advanced gap dependencies are durable evidence';
    END IF;
    IF (NEW.gap_id,NEW.window_id,NEW.blocked_at) IS DISTINCT FROM
       (OLD.gap_id,OLD.window_id,OLD.blocked_at) THEN
        RAISE EXCEPTION 'advanced gap dependency identity is immutable';
    END IF;
    IF OLD.status = 'BLOCKED' AND NEW.status = 'RELEASED' THEN
        RETURN NEW;
    END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'invalid advanced gap dependency transition % -> %',
        OLD.status, NEW.status;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_gap_dependencies_durable
    BEFORE UPDATE OR DELETE ON rt.advanced_gap_dependencies
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_gap_dependency_change();

CREATE FUNCTION rt.enforce_advanced_gap_alert_change()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'advanced gap alerts are durable evidence';
    END IF;
    IF (
        NEW.alert_id,
        NEW.gap_id,
        NEW.reason_code,
        NEW.severity,
        NEW.created_at
    ) IS DISTINCT FROM (
        OLD.alert_id,
        OLD.gap_id,
        OLD.reason_code,
        OLD.severity,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'advanced gap alert identity is immutable';
    END IF;
    IF OLD.status = 'OPEN' AND NEW.status = 'RESOLVED' THEN
        RETURN NEW;
    END IF;
    IF NEW IS NOT DISTINCT FROM OLD THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'invalid advanced gap alert transition % -> %',
        OLD.status, NEW.status;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_gap_alerts_durable
    BEFORE UPDATE OR DELETE ON rt.advanced_gap_alerts
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_gap_alert_change();
