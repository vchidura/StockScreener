SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.stock_alert_result_instances (
    instance_id text PRIMARY KEY,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (btrim(instance_id) <> ''),
    CHECK (jsonb_typeof(payload) = 'object')
);

CREATE TABLE rt.stock_alert_result_records (
    instance_id text NOT NULL
        REFERENCES rt.stock_alert_result_instances(instance_id),
    kind text NOT NULL,
    record_id text NOT NULL,
    payload_sha256 char(64) NOT NULL,
    payload jsonb NOT NULL,
    imported_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (instance_id, kind, record_id),
    CHECK (
        kind IN (
            'publication',
            'publication_evidence',
            'plan',
            'observation',
            'context'
        )
    ),
    CHECK (btrim(record_id) <> ''),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(payload) = 'object')
);

CREATE TABLE rt.stock_alert_result_revisions (
    instance_id text NOT NULL
        REFERENCES rt.stock_alert_result_instances(instance_id),
    alert_id text NOT NULL,
    source_as_of timestamptz NOT NULL,
    payload_sha256 char(64) NOT NULL,
    payload jsonb NOT NULL,
    imported_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (instance_id, alert_id, payload_sha256),
    CHECK (btrim(alert_id) <> ''),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(payload) = 'object')
);

CREATE INDEX idx_rt_stock_alert_result_latest
    ON rt.stock_alert_result_revisions (
        instance_id,
        alert_id,
        source_as_of DESC,
        imported_at DESC
    );

CREATE TABLE rt.stock_alert_result_streams (
    stream text PRIMARY KEY,
    instance_id text
        REFERENCES rt.stock_alert_result_instances(instance_id),
    source_as_of timestamptz,
    source_sha256 char(64),
    metadata jsonb,
    checked_at timestamptz NOT NULL,
    imported_at timestamptz,
    error text,
    CHECK (stream IN ('intraday', 'swing')),
    CHECK (
        source_sha256 IS NULL
        OR source_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CHECK (metadata IS NULL OR jsonb_typeof(metadata) = 'object'),
    CHECK (
        (instance_id IS NULL) = (source_as_of IS NULL)
    ),
    CHECK (
        (instance_id IS NULL) = (source_sha256 IS NULL)
    ),
    CHECK (
        (instance_id IS NULL) = (metadata IS NULL)
    ),
    CHECK (
        (instance_id IS NULL) = (imported_at IS NULL)
    )
);

CREATE TRIGGER trg_rt_stock_alert_result_instances_immutable
    BEFORE UPDATE OR DELETE
    ON rt.stock_alert_result_instances
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_stock_alert_result_instances_no_truncate
    BEFORE TRUNCATE
    ON rt.stock_alert_result_instances
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_stock_alert_result_records_immutable
    BEFORE UPDATE OR DELETE
    ON rt.stock_alert_result_records
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_stock_alert_result_records_no_truncate
    BEFORE TRUNCATE
    ON rt.stock_alert_result_records
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_stock_alert_result_revisions_immutable
    BEFORE UPDATE OR DELETE
    ON rt.stock_alert_result_revisions
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_stock_alert_result_revisions_no_truncate
    BEFORE TRUNCATE
    ON rt.stock_alert_result_revisions
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
