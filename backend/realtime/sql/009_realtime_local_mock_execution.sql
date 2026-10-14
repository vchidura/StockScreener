SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE rt.local_mock_requests (
    request_id uuid PRIMARY KEY,
    alert_id uuid NOT NULL REFERENCES rt.alert_current(alert_id),
    candidate_id uuid NOT NULL REFERENCES rt.option_packages(candidate_id),
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    candidate_identity_sha256 char(64) NOT NULL,
    requested_at timestamptz NOT NULL,
    request_origin varchar(24) NOT NULL,
    policy_version varchar(96) NOT NULL,
    policy_sha256 char(64) NOT NULL,
    policy jsonb NOT NULL,
    earnings_status varchar(16) NOT NULL,
    earnings_reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    evidence_batch_id uuid NOT NULL,
    evidence_count smallint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (request_origin = 'PORTAL_MANUAL'),
    CHECK (candidate_identity_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (btrim(policy_version) <> '' AND policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(policy) = 'object'),
    CHECK (earnings_status IN ('CLEAR', 'BLOCKED', 'UNAVAILABLE')),
    CHECK (
        (earnings_status = 'CLEAR' AND cardinality(earnings_reason_codes) = 0)
        OR (earnings_status <> 'CLEAR' AND cardinality(earnings_reason_codes) > 0)
    ),
    CHECK (evidence_count BETWEEN 1 AND 8)
);

CREATE INDEX idx_rt_local_mock_request_alert
    ON rt.local_mock_requests (alert_id, requested_at DESC, request_id);
CREATE INDEX idx_rt_local_mock_request_candidate
    ON rt.local_mock_requests (candidate_id, requested_at DESC, request_id);

CREATE TABLE rt.local_mock_request_inputs (
    request_id uuid NOT NULL REFERENCES rt.local_mock_requests(request_id),
    input_index smallint NOT NULL,
    evidence_id uuid NOT NULL REFERENCES rt.option_mark_evidence(evidence_id),
    PRIMARY KEY (request_id, input_index),
    UNIQUE (request_id, evidence_id),
    CHECK (input_index BETWEEN 0 AND 7)
);

CREATE TABLE rt.local_mock_dispositions (
    disposition_id uuid PRIMARY KEY,
    request_id uuid NOT NULL UNIQUE REFERENCES rt.local_mock_requests(request_id),
    status varchar(24) NOT NULL,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    gross_cash_flow numeric(28,8),
    estimated_fees numeric(20,8),
    net_cash_flow numeric(28,8),
    execution_permission boolean NOT NULL DEFAULT false,
    decided_at timestamptz NOT NULL,
    CHECK (status IN ('MOCK_FILLED', 'MOCK_REJECTED', 'MOCK_UNAVAILABLE')),
    CHECK (execution_permission = false),
    CHECK (
        (
            status = 'MOCK_FILLED'
            AND cardinality(reason_codes) = 0
            AND num_nonnulls(gross_cash_flow, estimated_fees, net_cash_flow) = 3
        ) OR (
            status <> 'MOCK_FILLED'
            AND cardinality(reason_codes) > 0
            AND num_nonnulls(gross_cash_flow, estimated_fees, net_cash_flow) = 0
        )
    ),
    CHECK (estimated_fees IS NULL OR estimated_fees >= 0)
);

CREATE TABLE rt.local_mock_fills (
    fill_id uuid PRIMARY KEY,
    request_id uuid NOT NULL REFERENCES rt.local_mock_requests(request_id),
    leg_index smallint NOT NULL,
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    contract_id bigint NOT NULL REFERENCES ref.option_contracts(contract_id),
    contract_ticker varchar(64) NOT NULL,
    side varchar(8) NOT NULL,
    quantity integer NOT NULL,
    multiplier integer NOT NULL,
    bid numeric(20,8) NOT NULL,
    ask numeric(20,8) NOT NULL,
    fill_price numeric(20,8) NOT NULL,
    fee numeric(20,8) NOT NULL,
    source_evidence_id uuid NOT NULL REFERENCES rt.option_mark_evidence(evidence_id),
    UNIQUE (request_id, leg_index),
    FOREIGN KEY (request_id, source_evidence_id)
        REFERENCES rt.local_mock_request_inputs(request_id, evidence_id),
    CHECK (leg_index BETWEEN 0 AND 7),
    CHECK (contract_ticker = upper(contract_ticker) AND btrim(contract_ticker) <> ''),
    CHECK (side IN ('BUY', 'SELL')),
    CHECK (quantity > 0 AND multiplier > 0),
    CHECK (bid > 0 AND ask >= bid AND fill_price > 0 AND fee >= 0)
);

CREATE TRIGGER trg_rt_local_mock_requests_immutable BEFORE UPDATE OR DELETE ON rt.local_mock_requests FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_local_mock_requests_no_truncate BEFORE TRUNCATE ON rt.local_mock_requests FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_local_mock_request_inputs_immutable BEFORE UPDATE OR DELETE ON rt.local_mock_request_inputs FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_local_mock_request_inputs_no_truncate BEFORE TRUNCATE ON rt.local_mock_request_inputs FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_local_mock_dispositions_immutable BEFORE UPDATE OR DELETE ON rt.local_mock_dispositions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_local_mock_dispositions_no_truncate BEFORE TRUNCATE ON rt.local_mock_dispositions FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_local_mock_fills_immutable BEFORE UPDATE OR DELETE ON rt.local_mock_fills FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_local_mock_fills_no_truncate BEFORE TRUNCATE ON rt.local_mock_fills FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();