SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE ref.fundamental_reports (
    report_id uuid PRIMARY KEY,
    security_id uuid NOT NULL REFERENCES ref.securities(security_id),
    security_revision_id uuid REFERENCES ref.security_revisions(revision_id),
    cik varchar(32),
    accession_number varchar(64),
    form_type varchar(32),
    timeframe varchar(32) NOT NULL,
    fiscal_year integer,
    fiscal_quarter integer,
    period_end date NOT NULL,
    filing_date date,
    availability_time timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    source varchar(64) NOT NULL,
    source_key varchar(256) NOT NULL,
    metrics jsonb NOT NULL,
    raw_payload jsonb NOT NULL,
    payload_sha256 char(64) NOT NULL,
    quality_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    UNIQUE (source, source_key, payload_sha256),
    CHECK (timeframe IN ('quarterly', 'annual', 'trailing_twelve_months')),
    CHECK (fiscal_quarter IS NULL OR fiscal_quarter BETWEEN 1 AND 4),
    CHECK (observed_at >= availability_time),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(metrics) = 'object'),
    CHECK (jsonb_typeof(raw_payload) = 'object')
);

CREATE INDEX idx_ref_fundamental_reports_asof
    ON ref.fundamental_reports (security_id, availability_time DESC, observed_at DESC);

CREATE TABLE ref.fundamental_report_coverage (
    coverage_id uuid PRIMARY KEY,
    security_id uuid NOT NULL REFERENCES ref.securities(security_id),
    window_start date NOT NULL,
    window_end date NOT NULL,
    source varchar(64) NOT NULL,
    report_count integer NOT NULL,
    response_sha256 char(64) NOT NULL,
    checked_at timestamptz NOT NULL,
    valid_until timestamptz NOT NULL,
    UNIQUE (security_id, window_start, window_end, source, checked_at),
    CHECK (window_end >= window_start),
    CHECK (report_count >= 0),
    CHECK (response_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (valid_until > checked_at)
);

CREATE INDEX idx_ref_fundamental_coverage_asof
    ON ref.fundamental_report_coverage (security_id, checked_at DESC);

CREATE TRIGGER trg_ref_fundamental_reports_immutable
    BEFORE UPDATE OR DELETE ON ref.fundamental_reports
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_fundamental_reports_no_truncate
    BEFORE TRUNCATE ON ref.fundamental_reports
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_fundamental_coverage_immutable
    BEFORE UPDATE OR DELETE ON ref.fundamental_report_coverage
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_fundamental_coverage_no_truncate
    BEFORE TRUNCATE ON ref.fundamental_report_coverage
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
