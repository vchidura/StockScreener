SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE rt.screening_generations (
    generation_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    session_date date NOT NULL,
    source_manifest_sha256 char(64) NOT NULL,
    expected_members integer NOT NULL,
    actual_members integer NOT NULL,
    status varchar(16) NOT NULL,
    generated_at timestamptz NOT NULL,
    manifest jsonb NOT NULL,
    UNIQUE (cohort_id, session_date, source_manifest_sha256),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (source_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (expected_members > 0 AND actual_members BETWEEN 0 AND expected_members),
    CHECK (status IN ('COMPLETE', 'DEGRADED', 'FAILED')),
    CHECK ((status = 'COMPLETE') = (actual_members = expected_members)),
    CHECK (jsonb_typeof(manifest) = 'object')
);

CREATE TABLE rt.screening_rows (
    generation_id uuid NOT NULL REFERENCES rt.screening_generations(generation_id),
    security_id uuid NOT NULL REFERENCES ref.securities(security_id),
    ticker varchar(32) NOT NULL,
    eligible boolean NOT NULL,
    payload_sha256 char(64) NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY (generation_id, security_id),
    CHECK (ticker = upper(ticker) AND btrim(ticker) <> ''),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(payload) = 'object')
);

CREATE INDEX idx_rt_screening_row_ticker
    ON rt.screening_rows (generation_id, ticker);

CREATE TABLE rt.screening_current (
    cohort_id uuid PRIMARY KEY,
    generation_id uuid NOT NULL REFERENCES rt.screening_generations(generation_id),
    session_date date NOT NULL,
    generated_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
) WITH (fillfactor = 80);

CREATE TABLE rt.detector_datasets (
    dataset_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    dataset_key text NOT NULL,
    session_date date NOT NULL,
    status varchar(16) NOT NULL,
    detector_ids text[] NOT NULL,
    source_manifest_sha256 char(64) NOT NULL,
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    summary jsonb NOT NULL,
    UNIQUE (cohort_id, dataset_key),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (btrim(dataset_key) <> '' AND cardinality(detector_ids) > 0),
    CHECK (status IN ('RUNNING', 'COMPLETE', 'DEGRADED', 'FAILED')),
    CHECK ((status = 'RUNNING') = (completed_at IS NULL)),
    CHECK (completed_at IS NULL OR completed_at >= started_at),
    CHECK (source_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(summary) = 'object')
);

CREATE INDEX idx_rt_detector_dataset_session
    ON rt.detector_datasets (cohort_id, session_date DESC, started_at DESC);
CREATE INDEX idx_rt_detector_dataset_latest
    ON rt.detector_datasets (cohort_id, started_at DESC, dataset_id);

CREATE TABLE rt.detector_evaluations (
    evaluation_id uuid PRIMARY KEY,
    dataset_id uuid NOT NULL REFERENCES rt.detector_datasets(dataset_id),
    event_id uuid NOT NULL REFERENCES rt.detector_events(event_id),
    detector_id varchar(64) NOT NULL,
    underlyer varchar(32) NOT NULL,
    selection_status varchar(24) NOT NULL,
    observed_at timestamptz NOT NULL,
    payload_sha256 char(64) NOT NULL,
    payload jsonb NOT NULL,
    UNIQUE (dataset_id, event_id),
    CHECK (underlyer = upper(underlyer) AND btrim(underlyer) <> ''),
    CHECK (selection_status IN ('SELECTED', 'NOT_SELECTED', 'REPEAT', 'OBSERVATION')),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(payload) = 'object')
);

CREATE INDEX idx_rt_detector_evaluation_time
    ON rt.detector_evaluations (dataset_id, observed_at DESC, evaluation_id);
CREATE INDEX idx_rt_detector_evaluation_filter
    ON rt.detector_evaluations (
        dataset_id, detector_id, underlyer, observed_at DESC, evaluation_id
    );

CREATE TABLE rt.alert_management_assessments (
    assessment_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    alert_id uuid NOT NULL,
    assessed_at timestamptz NOT NULL,
    status varchar(24) NOT NULL,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    payload_sha256 char(64) NOT NULL,
    payload jsonb NOT NULL,
    UNIQUE (cohort_id, alert_id, assessed_at),
    CHECK (status IN ('READY', 'UNAVAILABLE', 'EXPIRED', 'TARGET', 'STOP')),
    CHECK ((status = 'READY' AND cardinality(reason_codes) = 0) OR status <> 'READY'),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(payload) = 'object')
);

CREATE TABLE rt.alert_management_current (
    cohort_id uuid NOT NULL,
    alert_id uuid NOT NULL,
    assessment_id uuid NOT NULL REFERENCES rt.alert_management_assessments(assessment_id),
    assessed_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, alert_id)
) WITH (fillfactor = 80);

CREATE TABLE rt.portal_snapshots (
    snapshot_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    snapshot_type varchar(48) NOT NULL,
    snapshot_key text NOT NULL,
    source_generation bigint NOT NULL,
    source_manifest_sha256 char(64) NOT NULL,
    source_manifest jsonb NOT NULL,
    payload_sha256 char(64) NOT NULL,
    payload jsonb NOT NULL,
    generated_at timestamptz NOT NULL,
    valid_until timestamptz,
    UNIQUE (cohort_id, snapshot_type, snapshot_key, source_manifest_sha256, payload_sha256),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (btrim(snapshot_type) <> '' AND btrim(snapshot_key) <> ''),
    CHECK (source_generation > 0),
    CHECK (source_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(source_manifest) = 'object'),
    CHECK (jsonb_typeof(payload) = 'object'),
    CHECK (valid_until IS NULL OR valid_until > generated_at)
);

CREATE TABLE rt.portal_snapshot_current (
    cohort_id uuid NOT NULL,
    snapshot_type varchar(48) NOT NULL,
    snapshot_key text NOT NULL,
    snapshot_id uuid NOT NULL REFERENCES rt.portal_snapshots(snapshot_id),
    source_generation bigint NOT NULL,
    generated_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, snapshot_type, snapshot_key),
    CHECK (source_generation > 0)
) WITH (fillfactor = 80);

CREATE TABLE rt.alert_current (
    alert_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    event_id uuid NOT NULL UNIQUE REFERENCES rt.detector_events(event_id),
    alert_kind varchar(24) NOT NULL,
    lifecycle varchar(24) NOT NULL,
    as_of timestamptz NOT NULL,
    valid_until timestamptz,
    projection_schema varchar(64) NOT NULL,
    projection_sha256 char(64) NOT NULL,
    projection jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (alert_kind IN ('STOCK', 'OPTION')),
    CHECK (btrim(lifecycle) <> '' AND btrim(projection_schema) <> ''),
    CHECK (valid_until IS NULL OR valid_until > as_of),
    CHECK (projection_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(projection) = 'object')
) WITH (fillfactor = 80);

CREATE INDEX idx_rt_alert_current_kind_time
    ON rt.alert_current (cohort_id, alert_kind, as_of DESC, alert_id);
CREATE INDEX idx_rt_alert_current_stock_open
    ON rt.alert_current (cohort_id, as_of DESC, alert_id)
    WHERE alert_kind = 'STOCK' AND lifecycle = 'OPEN';

CREATE TABLE rt.alert_outbox (
    outbox_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    event_id uuid NOT NULL REFERENCES rt.detector_events(event_id),
    destination varchar(64) NOT NULL,
    message_kind varchar(32) NOT NULL,
    payload_sha256 char(64) NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    next_attempt_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    lease_owner uuid,
    lease_epoch bigint NOT NULL DEFAULT 0,
    lease_until timestamptz,
    delivered_at timestamptz,
    failure_code varchar(64),
    abandoned_at timestamptz,
    UNIQUE (event_id, destination, message_kind),
    CHECK (btrim(destination) <> '' AND btrim(message_kind) <> ''),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(payload) = 'object'),
    CHECK (attempts >= 0 AND lease_epoch >= 0),
    CHECK ((lease_owner IS NULL) = (lease_until IS NULL)),
    CHECK (num_nonnulls(delivered_at, abandoned_at) <= 1),
    CHECK (delivered_at IS NULL OR delivered_at >= created_at),
    CHECK (abandoned_at IS NULL OR (
        abandoned_at >= created_at AND failure_code IS NOT NULL
    )),
    CHECK (failure_code IS NULL OR btrim(failure_code) <> ''),
    CHECK ((delivered_at IS NULL AND abandoned_at IS NULL) OR lease_owner IS NULL)
);

CREATE INDEX idx_rt_alert_outbox_pending
    ON rt.alert_outbox (cohort_id, next_attempt_at, outbox_id)
    WHERE delivered_at IS NULL AND abandoned_at IS NULL;

CREATE TRIGGER trg_rt_screening_generations_immutable BEFORE UPDATE OR DELETE ON rt.screening_generations FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_screening_generations_no_truncate BEFORE TRUNCATE ON rt.screening_generations FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_screening_rows_immutable BEFORE UPDATE OR DELETE ON rt.screening_rows FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_screening_rows_no_truncate BEFORE TRUNCATE ON rt.screening_rows FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_detector_datasets_immutable BEFORE UPDATE OR DELETE ON rt.detector_datasets FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_detector_datasets_no_truncate BEFORE TRUNCATE ON rt.detector_datasets FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_detector_evaluations_immutable BEFORE UPDATE OR DELETE ON rt.detector_evaluations FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_detector_evaluations_no_truncate BEFORE TRUNCATE ON rt.detector_evaluations FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_alert_management_assessments_immutable BEFORE UPDATE OR DELETE ON rt.alert_management_assessments FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_alert_management_assessments_no_truncate BEFORE TRUNCATE ON rt.alert_management_assessments FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_portal_snapshots_immutable BEFORE UPDATE OR DELETE ON rt.portal_snapshots FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_portal_snapshots_no_truncate BEFORE TRUNCATE ON rt.portal_snapshots FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();