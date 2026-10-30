SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.canonical_stock_alert_batches (
    batch_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    window_id uuid NOT NULL UNIQUE REFERENCES rt.window_runs(window_id),
    input_receipt_id uuid NOT NULL UNIQUE
        REFERENCES rt.cycle_receipts(receipt_id),
    cutoff timestamptz NOT NULL,
    receipt_input_sha256 char(64) NOT NULL,
    detector_input_sha256 char(64) NOT NULL,
    detector_sha256 char(64) NOT NULL
        REFERENCES rt.detector_versions(detector_sha256),
    status varchar(16) NOT NULL,
    evaluation_count integer NOT NULL,
    match_count integer NOT NULL,
    no_match_count integer NOT NULL,
    unavailable_count integer NOT NULL,
    error_count integer NOT NULL,
    batch_sha256 char(64) NOT NULL UNIQUE,
    batch_document jsonb NOT NULL,
    completed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (receipt_input_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (detector_input_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (batch_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (status = 'COMPLETE'),
    CHECK (jsonb_typeof(batch_document) = 'object'),
    CHECK (
        evaluation_count >= 0
        AND match_count >= 0
        AND no_match_count >= 0
        AND unavailable_count >= 0
        AND error_count >= 0
        AND evaluation_count =
            match_count + no_match_count + unavailable_count + error_count
    ),
    CHECK (completed_at >= cutoff)
);

CREATE TABLE rt.canonical_stock_alert_evaluations (
    evaluation_id uuid PRIMARY KEY,
    batch_id uuid NOT NULL
        REFERENCES rt.canonical_stock_alert_batches(batch_id),
    event_id uuid NOT NULL UNIQUE REFERENCES rt.detector_events(event_id),
    security_id uuid NOT NULL,
    instrument_id bigint NOT NULL,
    ticker varchar(32) NOT NULL,
    disposition varchar(16) NOT NULL,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    input_sha256 char(64) NOT NULL,
    alert_id uuid REFERENCES rt.alert_current(alert_id),
    evaluation_sha256 char(64) NOT NULL UNIQUE,
    evaluation_payload jsonb NOT NULL,
    observed_at timestamptz NOT NULL,
    UNIQUE (batch_id, security_id),
    CHECK (ticker = upper(ticker) AND btrim(ticker) <> ''),
    CHECK (disposition IN ('MATCH', 'NO_MATCH', 'UNAVAILABLE', 'ERROR')),
    CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (evaluation_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(evaluation_payload) = 'object'),
    CHECK (
        (disposition = 'NO_MATCH' AND alert_id IS NULL)
        OR (disposition <> 'NO_MATCH' AND alert_id IS NOT NULL)
    )
);

CREATE FUNCTION rt.validate_canonical_stock_alert_batch()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, rt
AS $$
DECLARE
    retained record;
BEGIN
    SELECT receipt.window_id, receipt.cohort_id, receipt.receipt_kind,
           receipt.cutoff, receipt.input_sha256
      INTO retained
      FROM rt.cycle_receipts AS receipt
     WHERE receipt.receipt_id = NEW.input_receipt_id;
    IF retained IS NULL
       OR retained.receipt_kind <> 'INPUT_READY'
       OR retained.window_id <> NEW.window_id
       OR retained.cohort_id <> NEW.cohort_id
       OR retained.cutoff <> NEW.cutoff
       OR retained.input_sha256 <> NEW.receipt_input_sha256 THEN
        RAISE EXCEPTION
            'Canonical Stock Alert batch does not match INPUT_READY receipt';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_rt_canonical_stock_alert_batch_scope
    BEFORE INSERT ON rt.canonical_stock_alert_batches
    FOR EACH ROW EXECUTE FUNCTION rt.validate_canonical_stock_alert_batch();

CREATE INDEX idx_rt_canonical_stock_alert_batch_session
    ON rt.canonical_stock_alert_batches(cohort_id, cutoff DESC);

CREATE INDEX idx_rt_canonical_stock_alert_evaluation_batch
    ON rt.canonical_stock_alert_evaluations(
        batch_id, disposition, ticker, evaluation_id
    );

CREATE TRIGGER trg_rt_canonical_stock_alert_batches_immutable
    BEFORE UPDATE OR DELETE ON rt.canonical_stock_alert_batches
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_canonical_stock_alert_batches_no_truncate
    BEFORE TRUNCATE ON rt.canonical_stock_alert_batches
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_canonical_stock_alert_evaluations_immutable
    BEFORE UPDATE OR DELETE ON rt.canonical_stock_alert_evaluations
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_canonical_stock_alert_evaluations_no_truncate
    BEFORE TRUNCATE ON rt.canonical_stock_alert_evaluations
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
