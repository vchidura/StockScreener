SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.session_close_audits (
    audit_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    session_date date NOT NULL,
    interval varchar(8) NOT NULL,
    cohort_effective_at timestamptz NOT NULL,
    manifest_id uuid NOT NULL REFERENCES rt.subscription_manifests(manifest_id),
    manifest_sha256 char(64) NOT NULL,
    universe_sha256 char(64) NOT NULL,
    session_open timestamptz NOT NULL,
    session_close timestamptz NOT NULL,
    known_by timestamptz NOT NULL,
    instrument_count integer NOT NULL,
    expected_per_instrument integer NOT NULL,
    expected_members integer NOT NULL,
    complete_members integer NOT NULL,
    no_trade_members integer NOT NULL DEFAULT 0,
    missing_members integer NOT NULL,
    missing_range_count integer NOT NULL,
    status varchar(16) NOT NULL,
    expected_manifest_sha256 char(64) NOT NULL,
    coverage_sha256 char(64) NOT NULL,
    query_microseconds bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (audit_id, cohort_id, provider_tier, session_date),
    UNIQUE (cohort_id, provider_tier, session_date, known_by, coverage_sha256),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (interval = '5m'),
    CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (universe_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (expected_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (coverage_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (session_open < session_close AND known_by >= session_close),
    CHECK (cohort_effective_at <= session_open),
    CHECK (instrument_count > 0 AND expected_per_instrument > 0),
    CHECK (expected_members = instrument_count * expected_per_instrument),
    CHECK (complete_members >= 0 AND no_trade_members >= 0 AND missing_members >= 0),
    CHECK (expected_members = complete_members + no_trade_members + missing_members),
    CHECK (missing_range_count >= 0 AND missing_range_count <= missing_members),
    CHECK (status IN ('COMPLETE', 'INCOMPLETE')),
    CHECK ((status = 'COMPLETE') = (missing_members = 0)),
    CHECK (query_microseconds >= 0)
);

CREATE INDEX idx_rt_session_close_audit_session
    ON rt.session_close_audits (cohort_id, provider_tier, session_date DESC, known_by DESC);

CREATE TABLE rt.session_close_anomalies (
    audit_id uuid NOT NULL REFERENCES rt.session_close_audits(audit_id),
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    ticker varchar(32) NOT NULL,
    reason_code varchar(64) NOT NULL,
    requested_from timestamptz NOT NULL,
    requested_to timestamptz NOT NULL,
    missing_members integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (audit_id, instrument_id, requested_from),
    CHECK (ticker = upper(ticker) AND btrim(ticker) <> ''),
    CHECK (reason_code = 'FINAL_BAR_UNAVAILABLE'),
    CHECK (requested_from < requested_to),
    CHECK (missing_members > 0),
    CHECK (requested_to - requested_from = missing_members * interval '5 minutes')
);

CREATE INDEX idx_rt_session_close_anomaly_instrument
    ON rt.session_close_anomalies (instrument_id, requested_from DESC);

CREATE TABLE rt.session_close_current (
    cohort_id uuid NOT NULL,
    provider_tier varchar(24) NOT NULL,
    session_date date NOT NULL,
    audit_id uuid NOT NULL,
    known_by timestamptz NOT NULL,
    status varchar(16) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, provider_tier, session_date),
    FOREIGN KEY (audit_id, cohort_id, provider_tier, session_date)
        REFERENCES rt.session_close_audits(audit_id, cohort_id, provider_tier, session_date),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (status IN ('COMPLETE', 'INCOMPLETE'))
) WITH (fillfactor = 80);

CREATE TRIGGER trg_rt_session_close_audits_immutable
    BEFORE UPDATE OR DELETE ON rt.session_close_audits
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_session_close_audits_no_truncate
    BEFORE TRUNCATE ON rt.session_close_audits
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_session_close_anomalies_immutable
    BEFORE UPDATE OR DELETE ON rt.session_close_anomalies
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_session_close_anomalies_no_truncate
    BEFORE TRUNCATE ON rt.session_close_anomalies
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
