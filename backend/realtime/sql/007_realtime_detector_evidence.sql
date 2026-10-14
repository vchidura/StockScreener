SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE rt.detector_versions (
    detector_sha256 char(64) PRIMARY KEY,
    detector_id varchar(64) NOT NULL,
    version varchar(96) NOT NULL,
    code_sha256 char(64) NOT NULL,
    policy_sha256 char(64) NOT NULL,
    valuation_sha256 char(64),
    requirements jsonb NOT NULL,
    output_kind varchar(32) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (detector_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (code_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (valuation_sha256 IS NULL OR valuation_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (btrim(detector_id) <> '' AND btrim(version) <> ''),
    CHECK (jsonb_typeof(requirements) = 'object'),
    CHECK (output_kind IN ('OBSERVATION', 'SIGNAL', 'ALERT', 'ASSESSMENT'))
);

CREATE TABLE rt.detector_events (
    event_id uuid PRIMARY KEY,
    dedup_sha256 char(64) NOT NULL UNIQUE,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    detector_sha256 char(64) NOT NULL REFERENCES rt.detector_versions(detector_sha256),
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    trigger_identity text NOT NULL,
    trigger_at timestamptz NOT NULL,
    decision_at timestamptz NOT NULL,
    valid_until timestamptz,
    disposition varchar(24) NOT NULL,
    direction smallint,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    readiness_epoch bigint NOT NULL,
    evidence_sha256 char(64) NOT NULL,
    payload_schema varchar(64) NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (dedup_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (btrim(trigger_identity) <> ''),
    CHECK (decision_at >= trigger_at),
    CHECK (valid_until IS NULL OR valid_until > trigger_at),
    CHECK (direction IS NULL OR direction IN (-1, 0, 1)),
    CHECK (readiness_epoch > 0),
    CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (btrim(payload_schema) <> '' AND jsonb_typeof(payload) = 'object')
);

CREATE INDEX idx_rt_detector_event_instrument_time
    ON rt.detector_events (cohort_id, instrument_id, decision_at DESC);
CREATE INDEX idx_rt_detector_event_detector_time
    ON rt.detector_events (cohort_id, detector_sha256, decision_at DESC);

CREATE TABLE rt.detector_event_inputs (
    event_id uuid NOT NULL REFERENCES rt.detector_events(event_id),
    input_role varchar(48) NOT NULL,
    input_key text NOT NULL,
    source_kind varchar(32) NOT NULL,
    feature_snapshot_id uuid,
    bar_revision_id uuid,
    option_mark_evidence_id uuid,
    source_record_id uuid,
    journal_generation uuid,
    stream_session_id uuid,
    source_offset bigint,
    input_sha256 char(64) NOT NULL,
    available_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    retained_input jsonb NOT NULL,
    PRIMARY KEY (event_id, input_role, input_key),
    FOREIGN KEY (feature_snapshot_id) REFERENCES rt.feature_snapshots(snapshot_id),
    FOREIGN KEY (bar_revision_id) REFERENCES rt.bar_revisions(bar_revision_id),
    FOREIGN KEY (option_mark_evidence_id) REFERENCES rt.option_mark_evidence(evidence_id),
    CHECK (btrim(input_role) <> '' AND btrim(input_key) <> ''),
    CHECK (source_kind IN ('FEATURE_SNAPSHOT', 'BAR_REVISION', 'OPTION_MARK', 'REFERENCE', 'JOURNAL_EVENT')),
    CHECK (source_offset IS NULL OR source_offset > 0),
    CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (received_at >= available_at),
    CHECK (jsonb_typeof(retained_input) = 'object'),
    CHECK (
        num_nonnulls(feature_snapshot_id, bar_revision_id, option_mark_evidence_id, source_record_id)
        + CASE WHEN journal_generation IS NOT NULL AND stream_session_id IS NOT NULL
            AND source_offset IS NOT NULL THEN 1 ELSE 0 END = 1
    ),
    CHECK (
        (journal_generation IS NULL AND stream_session_id IS NULL AND source_offset IS NULL)
        OR (journal_generation IS NOT NULL AND stream_session_id IS NOT NULL AND source_offset IS NOT NULL)
    )
);

CREATE TABLE rt.detector_state (
    detector_sha256 char(64) NOT NULL REFERENCES rt.detector_versions(detector_sha256),
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    state_key text NOT NULL,
    revision bigint NOT NULL,
    state_schema varchar(64) NOT NULL,
    state_sha256 char(64) NOT NULL,
    state_payload jsonb NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (detector_sha256, cohort_id, state_key),
    CHECK (btrim(state_key) <> '' AND btrim(state_schema) <> ''),
    CHECK (revision > 0),
    CHECK (state_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(state_payload) = 'object')
) WITH (fillfactor = 80);

CREATE TRIGGER trg_rt_detector_versions_immutable BEFORE UPDATE OR DELETE ON rt.detector_versions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_detector_versions_no_truncate BEFORE TRUNCATE ON rt.detector_versions FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_detector_events_immutable BEFORE UPDATE OR DELETE ON rt.detector_events FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_detector_events_no_truncate BEFORE TRUNCATE ON rt.detector_events FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_detector_event_inputs_immutable BEFORE UPDATE OR DELETE ON rt.detector_event_inputs FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_detector_event_inputs_no_truncate BEFORE TRUNCATE ON rt.detector_event_inputs FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();