SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE ref.corporate_action_revisions (
    action_id uuid PRIMARY KEY,
    security_id uuid NOT NULL REFERENCES ref.securities(security_id),
    ticker varchar(32) NOT NULL,
    action_type varchar(24) NOT NULL,
    effective_date date NOT NULL,
    declaration_date date,
    ex_date date,
    record_date date,
    pay_date date,
    cash_amount numeric(28,8),
    split_from numeric(28,8),
    split_to numeric(28,8),
    new_ticker varchar(32),
    source varchar(64) NOT NULL,
    source_key varchar(256) NOT NULL,
    observed_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    payload_sha256 char(64) NOT NULL,
    raw_payload jsonb NOT NULL,
    supersedes_action_id uuid REFERENCES ref.corporate_action_revisions(action_id),
    UNIQUE (source, source_key, observed_at, payload_sha256),
    CHECK (ticker = upper(ticker) AND btrim(ticker) <> ''),
    CHECK (action_type IN ('SPLIT', 'DIVIDEND', 'SYMBOL_CHANGE', 'SPINOFF', 'MERGER', 'OTHER')),
    CHECK (cash_amount IS NULL OR cash_amount > 0),
    CHECK ((split_from IS NULL) = (split_to IS NULL)),
    CHECK (split_from IS NULL OR (split_from > 0 AND split_to > 0)),
    CHECK (received_at >= observed_at),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(raw_payload) = 'object')
);

CREATE INDEX idx_ref_corporate_action_security_date
    ON ref.corporate_action_revisions (security_id, effective_date, observed_at DESC);

CREATE TABLE ref.corporate_action_coverage (
    coverage_id uuid PRIMARY KEY,
    security_id uuid NOT NULL REFERENCES ref.securities(security_id),
    action_type varchar(24) NOT NULL,
    window_start date NOT NULL,
    window_end date NOT NULL,
    source varchar(64) NOT NULL,
    source_key varchar(256) NOT NULL,
    response_action_count integer NOT NULL,
    response_sha256 char(64) NOT NULL,
    observed_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    payload_sha256 char(64) NOT NULL,
    UNIQUE (source, source_key, observed_at, payload_sha256),
    CHECK (action_type IN ('SPLIT', 'DIVIDEND')),
    CHECK (window_end >= window_start),
    CHECK (response_action_count >= 0),
    CHECK (received_at >= observed_at),
    CHECK (response_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE ref.oi_baselines (
    baseline_id uuid PRIMARY KEY,
    contract_id bigint NOT NULL REFERENCES ref.option_contracts(contract_id),
    settlement_session date NOT NULL,
    open_interest bigint NOT NULL,
    source varchar(64) NOT NULL,
    date_basis varchar(64) NOT NULL,
    observed_session date NOT NULL,
    observed_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    payload_sha256 char(64) NOT NULL,
    supersedes_baseline_id uuid REFERENCES ref.oi_baselines(baseline_id),
    UNIQUE (source, contract_id, settlement_session, observed_at, payload_sha256),
    CHECK (open_interest >= 0),
    CHECK (settlement_session < observed_session),
    CHECK (received_at >= observed_at),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX idx_ref_oi_baseline_contract_session
    ON ref.oi_baselines (contract_id, settlement_session DESC, observed_at DESC);

CREATE TABLE ref.market_event_revisions (
    revision_id uuid PRIMARY KEY,
    security_id uuid REFERENCES ref.securities(security_id),
    event_key varchar(256) NOT NULL,
    event_type varchar(32) NOT NULL,
    event_date date NOT NULL,
    announced_at timestamptz,
    event_timezone varchar(64) NOT NULL,
    time_precision varchar(16) NOT NULL,
    status varchar(16) NOT NULL,
    source varchar(64) NOT NULL,
    observed_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    valid_until timestamptz NOT NULL,
    payload_sha256 char(64) NOT NULL,
    supersedes_revision_id uuid REFERENCES ref.market_event_revisions(revision_id),
    UNIQUE (source, event_key, observed_at, payload_sha256),
    CHECK (event_type IN ('EARNINGS', 'FED_RATE_DECISION')),
    CHECK ((event_type = 'EARNINGS') = (security_id IS NOT NULL)),
    CHECK (time_precision IN ('CONFIRMED', 'ESTIMATED', 'DATE_ONLY', 'UNKNOWN')),
    CHECK (status IN ('SCHEDULED', 'COMPLETED', 'CANCELED', 'REVISED')),
    CHECK (received_at >= observed_at),
    CHECK (valid_until > received_at),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX idx_ref_market_event_security_date
    ON ref.market_event_revisions (security_id, event_date, observed_at DESC);

CREATE TABLE ref.market_event_coverage (
    coverage_id uuid PRIMARY KEY,
    security_id uuid REFERENCES ref.securities(security_id),
    event_type varchar(32) NOT NULL,
    window_start date NOT NULL,
    window_end date NOT NULL,
    source varchar(64) NOT NULL,
    verdict varchar(32) NOT NULL,
    checked_at timestamptz NOT NULL,
    valid_until timestamptz NOT NULL,
    evidence_sha256 char(64) NOT NULL,
    UNIQUE (source, event_type, security_id, window_start, window_end, checked_at),
    CHECK (event_type IN ('EARNINGS', 'FED_RATE_DECISION')),
    CHECK ((event_type = 'EARNINGS') = (security_id IS NOT NULL)),
    CHECK (window_end >= window_start),
    CHECK (verdict IN ('COVERED', 'KNOWN_NO_EVENT', 'NOT_APPLICABLE', 'UNAVAILABLE')),
    CHECK (valid_until > checked_at),
    CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE ref.risk_free_rate_observations (
    observation_id uuid PRIMARY KEY,
    rate_date date NOT NULL,
    tenor_days integer NOT NULL,
    annual_rate numeric(12,10) NOT NULL,
    source varchar(64) NOT NULL,
    source_key varchar(128) NOT NULL,
    source_observed_at timestamptz,
    received_at timestamptz NOT NULL,
    payload_sha256 char(64) NOT NULL,
    UNIQUE (source, source_key, received_at),
    CHECK (tenor_days > 0),
    CHECK (annual_rate > -0.10 AND annual_rate < 1.0),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX idx_ref_rate_curve_asof
    ON ref.risk_free_rate_observations
        (source, rate_date DESC, received_at DESC, tenor_days);

CREATE TABLE rt.seed_manifests (
    seed_id uuid PRIMARY KEY,
    seed_sha256 char(64) NOT NULL UNIQUE,
    universe_sha256 char(64) NOT NULL,
    session_date date NOT NULL,
    status varchar(16) NOT NULL,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    member_count integer NOT NULL,
    created_at timestamptz NOT NULL,
    effective_at timestamptz NOT NULL,
    valid_until timestamptz NOT NULL,
    source_generations jsonb NOT NULL,
    CHECK (seed_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (universe_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (status IN ('READY', 'UNAVAILABLE')),
    CHECK ((status = 'READY' AND cardinality(reason_codes) = 0) OR status <> 'READY'),
    CHECK ((status = 'UNAVAILABLE' AND cardinality(reason_codes) > 0) OR status <> 'UNAVAILABLE'),
    CHECK (member_count > 0),
    CHECK (created_at <= effective_at AND effective_at < valid_until),
    CHECK (jsonb_typeof(source_generations) = 'object')
);

CREATE TABLE rt.seed_manifest_members (
    seed_id uuid NOT NULL REFERENCES rt.seed_manifests(seed_id),
    reference_kind varchar(40) NOT NULL,
    reference_key varchar(256) NOT NULL,
    source_id text NOT NULL,
    payload_sha256 char(64) NOT NULL,
    status varchar(16) NOT NULL,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    observed_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    PRIMARY KEY (seed_id, reference_kind, reference_key),
    CHECK (reference_kind IN (
        'SECURITY', 'OPTION_CATALOG', 'CORPORATE_ACTION_COVERAGE',
        'OI_COVERAGE', 'EARNINGS_COVERAGE', 'FOMC_COVERAGE', 'RATE_CURVE'
    )),
    CHECK (btrim(reference_key) <> '' AND btrim(source_id) <> ''),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (status IN ('READY', 'UNAVAILABLE', 'NOT_APPLICABLE')),
    CHECK ((status = 'READY' AND cardinality(reason_codes) = 0) OR status <> 'READY'),
    CHECK ((status = 'UNAVAILABLE' AND cardinality(reason_codes) > 0) OR status <> 'UNAVAILABLE'),
    CHECK (received_at >= observed_at)
);

CREATE TABLE rt.seed_current (
    scope varchar(40) PRIMARY KEY,
    seed_id uuid NOT NULL REFERENCES rt.seed_manifests(seed_id),
    generation bigint NOT NULL,
    effective_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (scope = 'MARKET_REFERENCES'),
    CHECK (generation > 0)
) WITH (fillfactor = 80);

CREATE INDEX idx_rt_seed_manifest_session
    ON rt.seed_manifests (session_date DESC, effective_at DESC);

CREATE TRIGGER trg_ref_corporate_action_revisions_immutable BEFORE UPDATE OR DELETE ON ref.corporate_action_revisions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_corporate_action_revisions_no_truncate BEFORE TRUNCATE ON ref.corporate_action_revisions FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_corporate_action_coverage_immutable BEFORE UPDATE OR DELETE ON ref.corporate_action_coverage FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_corporate_action_coverage_no_truncate BEFORE TRUNCATE ON ref.corporate_action_coverage FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_oi_baselines_immutable BEFORE UPDATE OR DELETE ON ref.oi_baselines FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_oi_baselines_no_truncate BEFORE TRUNCATE ON ref.oi_baselines FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_market_event_revisions_immutable BEFORE UPDATE OR DELETE ON ref.market_event_revisions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_market_event_revisions_no_truncate BEFORE TRUNCATE ON ref.market_event_revisions FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_market_event_coverage_immutable BEFORE UPDATE OR DELETE ON ref.market_event_coverage FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_market_event_coverage_no_truncate BEFORE TRUNCATE ON ref.market_event_coverage FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_risk_free_rate_observations_immutable BEFORE UPDATE OR DELETE ON ref.risk_free_rate_observations FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_risk_free_rate_observations_no_truncate BEFORE TRUNCATE ON ref.risk_free_rate_observations FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_seed_manifests_immutable BEFORE UPDATE OR DELETE ON rt.seed_manifests FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_seed_manifests_no_truncate BEFORE TRUNCATE ON rt.seed_manifests FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_seed_manifest_members_immutable BEFORE UPDATE OR DELETE ON rt.seed_manifest_members FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_seed_manifest_members_no_truncate BEFORE TRUNCATE ON rt.seed_manifest_members FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();