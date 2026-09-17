CREATE TABLE IF NOT EXISTS stock_alert_result_instances (
    instance_id text PRIMARY KEY,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS stock_alert_result_records (
    instance_id text NOT NULL REFERENCES stock_alert_result_instances(instance_id),
    kind text NOT NULL CHECK (kind IN ('publication', 'publication_evidence', 'plan', 'observation', 'context')),
    record_id text NOT NULL,
    payload_sha256 text NOT NULL,
    payload jsonb NOT NULL,
    imported_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (instance_id, kind, record_id)
);
CREATE TABLE IF NOT EXISTS stock_alert_result_revisions (
    instance_id text NOT NULL REFERENCES stock_alert_result_instances(instance_id),
    alert_id text NOT NULL,
    source_as_of timestamptz NOT NULL,
    payload_sha256 text NOT NULL,
    payload jsonb NOT NULL,
    imported_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (instance_id, alert_id, payload_sha256)
);
CREATE INDEX IF NOT EXISTS stock_alert_result_latest_idx ON stock_alert_result_revisions
    (instance_id, alert_id, source_as_of DESC, imported_at DESC);
CREATE TABLE IF NOT EXISTS stock_alert_result_streams (
    stream text PRIMARY KEY CHECK (stream IN ('intraday', 'swing')),
    instance_id text REFERENCES stock_alert_result_instances(instance_id),
    source_as_of timestamptz,
    source_sha256 text,
    metadata jsonb,
    checked_at timestamptz NOT NULL,
    imported_at timestamptz,
    error text
);
CREATE OR REPLACE FUNCTION stock_alert_results_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Retained alert result evidence is immutable';
END;
$$;
DROP TRIGGER IF EXISTS stock_alert_results_no_rewrite ON stock_alert_result_records;
CREATE TRIGGER stock_alert_results_no_rewrite BEFORE UPDATE OR DELETE ON stock_alert_result_records
    FOR EACH ROW EXECUTE FUNCTION stock_alert_results_immutable();
DROP TRIGGER IF EXISTS stock_alert_revisions_no_rewrite ON stock_alert_result_revisions;
CREATE TRIGGER stock_alert_revisions_no_rewrite BEFORE UPDATE OR DELETE ON stock_alert_result_revisions
    FOR EACH ROW EXECUTE FUNCTION stock_alert_results_immutable();
DROP TRIGGER IF EXISTS stock_alert_instances_no_rewrite ON stock_alert_result_instances;
CREATE TRIGGER stock_alert_instances_no_rewrite BEFORE UPDATE OR DELETE ON stock_alert_result_instances
    FOR EACH ROW EXECUTE FUNCTION stock_alert_results_immutable();