SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.stock_alert_result_storage_identities (
    instance_id text NOT NULL,
    kind text NOT NULL,
    record_id text NOT NULL,
    source_payload_sha256 char(64) NOT NULL,
    storage_payload_sha256 char(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (instance_id, kind, record_id),
    FOREIGN KEY (instance_id, kind, record_id)
        REFERENCES rt.stock_alert_result_records (
            instance_id,
            kind,
            record_id
        ),
    CHECK (source_payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (storage_payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TRIGGER trg_rt_stock_alert_storage_identities_immutable
    BEFORE UPDATE OR DELETE
    ON rt.stock_alert_result_storage_identities
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_stock_alert_storage_identities_no_truncate
    BEFORE TRUNCATE
    ON rt.stock_alert_result_storage_identities
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
