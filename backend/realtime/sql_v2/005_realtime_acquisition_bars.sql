SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

-- Raw provider payloads: content-addressed and chunked so no row approaches a size cap.
CREATE TABLE rt.raw_payloads (
    payload_id uuid NOT NULL,
    arrived_at timestamptz NOT NULL,
    content_sha256 char(64) NOT NULL,
    byte_length bigint NOT NULL,
    chunk_count integer NOT NULL,
    content_encoding varchar(16) NOT NULL DEFAULT 'IDENTITY',
    request_id uuid,
    PRIMARY KEY (payload_id, arrived_at),
    CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (byte_length >= 0),
    CHECK (chunk_count >= 1),
    CHECK (content_encoding IN ('IDENTITY', 'GZIP'))
) PARTITION BY RANGE (arrived_at);

CREATE TABLE rt.raw_payload_chunks (
    payload_id uuid NOT NULL,
    arrived_at timestamptz NOT NULL,
    chunk_no integer NOT NULL,
    body bytea NOT NULL,
    PRIMARY KEY (payload_id, arrived_at, chunk_no),
    CHECK (chunk_no >= 0),
    CHECK (octet_length(body) BETWEEN 1 AND 1048576)
) PARTITION BY RANGE (arrived_at);

-- Tier-neutral acquisition batches. The batch row is the idempotency catalog: a replayed
-- provider batch conflicts here before any fact row is written.
CREATE TABLE rt.acquisition_batches (
    acquisition_batch_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    fact_kind varchar(32) NOT NULL,
    fact_schema_version varchar(96) NOT NULL,
    provider varchar(64) NOT NULL,
    provider_batch_id uuid NOT NULL,
    request_id uuid REFERENCES rt.provider_request_ledger(request_id),
    started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    fact_count integer NOT NULL,
    batch_payload_sha256 char(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (cohort_id, fact_kind, fact_schema_version, provider, provider_batch_id),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (fact_kind IN ('EQUITY_BAR', 'OPTION_SNAPSHOT')),
    CHECK (btrim(provider) <> '' AND btrim(fact_schema_version) <> ''),
    CHECK (completed_at >= started_at),
    CHECK (fact_count BETWEEN 1 AND 1000),
    CHECK (batch_payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX idx_rt_acquisition_batch_completed
    ON rt.acquisition_batches (cohort_id, fact_kind, completed_at, acquisition_batch_id);

CREATE TABLE rt.acquisition_facts (
    acquisition_batch_id uuid NOT NULL,
    fact_index integer NOT NULL,
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    provider_revision_id uuid NOT NULL,
    market_time timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    raw_payload_sha256 char(64) NOT NULL,
    normalized_payload_sha256 char(64) NOT NULL,
    structured_fact jsonb NOT NULL,
    PRIMARY KEY (acquisition_batch_id, fact_index, observed_at),
    CHECK (fact_index BETWEEN 0 AND 999),
    CHECK (market_time <= observed_at),
    CHECK (raw_payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (normalized_payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(structured_fact) = 'object')
) PARTITION BY RANGE (observed_at);

CREATE INDEX idx_rt_acquisition_fact_instrument
    ON rt.acquisition_facts (instrument_id, market_time DESC);

-- Bars: a small unpartitioned identity catalog owns lookup and composite-FK targets;
-- the partitioned body holds the full revision. Cross-lifecycle references to bars are
-- soft (id) and verified in the writing transaction.
CREATE TABLE rt.bar_catalog (
    bar_revision_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    interval varchar(8) NOT NULL,
    bar_start timestamptz NOT NULL,
    bar_end timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    session_date date NOT NULL,
    UNIQUE (bar_revision_id, cohort_id, provider_tier),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (interval IN ('1m', '5m', '15m', '30m', '1h', '1d', '1wk', '1mo')),
    CHECK (bar_end > bar_start AND observed_at >= bar_end)
);

CREATE INDEX idx_rt_bar_catalog_instrument_end
    ON rt.bar_catalog (cohort_id, instrument_id, interval, bar_end DESC);

CREATE TABLE rt.bar_revisions (
    bar_revision_id uuid NOT NULL REFERENCES rt.bar_catalog(bar_revision_id),
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    security_id uuid NOT NULL REFERENCES ref.securities(security_id),
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    ticker varchar(32) NOT NULL,
    interval varchar(8) NOT NULL,
    session_date date NOT NULL,
    session_scope varchar(16) NOT NULL,
    adjustment_basis varchar(24) NOT NULL,
    bar_start timestamptz NOT NULL,
    bar_end timestamptz NOT NULL,
    open_price numeric(20,8) NOT NULL,
    high_price numeric(20,8) NOT NULL,
    low_price numeric(20,8) NOT NULL,
    close_price numeric(20,8) NOT NULL,
    volume numeric(28,8) NOT NULL,
    vwap numeric(20,8),
    transaction_count bigint,
    source_kind varchar(24) NOT NULL,
    availability_mode varchar(32) NOT NULL,
    is_final boolean NOT NULL,
    observed_at timestamptz NOT NULL,
    replay_available_at timestamptz,
    provider_published_at timestamptz,
    ingestion_segment_id uuid,
    journal_generation uuid,
    stream_session_id uuid REFERENCES rt.stream_sessions(session_id),
    source_offset bigint,
    source_revision_ids uuid[] NOT NULL DEFAULT ARRAY[]::uuid[],
    supersedes_revision_id uuid,
    payload_sha256 char(64) NOT NULL,
    quality_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bar_revision_id, observed_at),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (ticker = upper(ticker) AND btrim(ticker) <> ''),
    CHECK (interval IN ('1m', '5m', '15m', '30m', '1h', '1d', '1wk', '1mo')),
    CHECK (session_scope IN ('RTH', 'EXTENDED', 'FULL_DAY')),
    CHECK (adjustment_basis IN ('RAW_UNADJUSTED', 'ADJUSTED')),
    CHECK (bar_end > bar_start AND observed_at >= bar_end),
    CHECK (open_price > 0 AND high_price > 0 AND low_price > 0 AND close_price > 0),
    CHECK (high_price >= GREATEST(open_price, close_price, low_price)),
    CHECK (low_price <= LEAST(open_price, close_price, high_price)),
    CHECK (volume >= 0 AND (vwap IS NULL OR vwap > 0)),
    CHECK (transaction_count IS NULL OR transaction_count >= 0),
    CHECK (source_kind IN ('NATIVE_REST', 'REALTIME_STREAM', 'DERIVED', 'RECONCILED')),
    CHECK (availability_mode IN ('LIVE_OBSERVED', 'HISTORICAL_RECONSTRUCTED')),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (
        (
            provider_tier = 'MASSIVE_ADVANCED'
            AND source_kind = 'REALTIME_STREAM'
            AND ingestion_segment_id IS NULL
            AND journal_generation IS NOT NULL
            AND stream_session_id IS NOT NULL
            AND source_offset > 0
            AND cardinality(source_revision_ids) = 0
        ) OR (
            provider_tier = 'MASSIVE_DEVELOPER'
            AND source_kind = 'NATIVE_REST'
            AND ingestion_segment_id IS NOT NULL
            AND journal_generation IS NULL
            AND stream_session_id IS NULL
            AND source_offset IS NULL
            AND cardinality(source_revision_ids) = 0
        ) OR (
            source_kind IN ('DERIVED', 'RECONCILED')
            AND ingestion_segment_id IS NOT NULL
            AND journal_generation IS NULL
            AND stream_session_id IS NULL
            AND source_offset IS NULL
            AND cardinality(source_revision_ids) > 0
        )
    )
) PARTITION BY RANGE (observed_at);

CREATE INDEX idx_rt_bar_revision_instrument_end
    ON rt.bar_revisions (cohort_id, instrument_id, interval, bar_end DESC);

-- Current revision per bar identity. Rows are removed with their catalog row when the
-- bar falls out of the retention horizon.
CREATE TABLE rt.bar_current (
    cohort_id uuid NOT NULL,
    provider_tier varchar(24) NOT NULL,
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    interval varchar(8) NOT NULL,
    bar_start timestamptz NOT NULL,
    session_scope varchar(16) NOT NULL,
    adjustment_basis varchar(24) NOT NULL,
    source_kind varchar(24) NOT NULL,
    bar_revision_id uuid NOT NULL,
    bar_end timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, instrument_id, interval, bar_start, session_scope, adjustment_basis),
    FOREIGN KEY (bar_revision_id, cohort_id, provider_tier)
        REFERENCES rt.bar_catalog(bar_revision_id, cohort_id, provider_tier) ON DELETE CASCADE,
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (interval IN ('1m', '5m', '15m', '30m', '1h', '1d', '1wk', '1mo')),
    CHECK (session_scope IN ('RTH', 'EXTENDED', 'FULL_DAY')),
    CHECK (adjustment_basis IN ('RAW_UNADJUSTED', 'ADJUSTED')),
    CHECK (source_kind IN ('NATIVE_REST', 'REALTIME_STREAM', 'DERIVED', 'RECONCILED')),
    CHECK (bar_end > bar_start AND observed_at >= bar_end)
) WITH (fillfactor = 80);

-- Materialization jobs are keyed by the full dependency closure so the same closure is
-- never computed twice and a changed policy or build is a new job.
CREATE TABLE rt.materialization_jobs (
    job_id uuid PRIMARY KEY,
    closure_sha256 char(64) NOT NULL UNIQUE,
    job_kind varchar(40) NOT NULL,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    interval varchar(8),
    source_revision_id uuid,
    primary_window_id uuid,
    confirmation_window_ids uuid[] NOT NULL DEFAULT ARRAY[]::uuid[],
    bundle_sha256 char(64),
    policy_sha256 char(64),
    build_id varchar(96),
    status varchar(16) NOT NULL,
    available_at timestamptz NOT NULL,
    lease_owner uuid,
    lease_epoch bigint NOT NULL DEFAULT 0,
    lease_until timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    failure_code varchar(64),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (closure_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (bundle_sha256 IS NULL OR bundle_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (policy_sha256 IS NULL OR policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (btrim(job_kind) <> ''),
    CHECK (interval IS NULL OR interval IN ('1m', '5m', '15m', '30m', '1h', '1d', '1wk', '1mo')),
    CHECK (num_nonnulls(source_revision_id, primary_window_id) >= 1),
    CHECK (status IN ('PENDING', 'LEASED', 'COMPLETE', 'FAILED', 'BLOCKED', 'EXPIRED', 'SUPERSEDED')),
    CHECK (lease_epoch >= 0 AND attempts >= 0),
    CHECK ((status = 'LEASED') = (lease_owner IS NOT NULL AND lease_until IS NOT NULL)),
    CHECK (lease_until IS NULL OR lease_until > available_at)
) WITH (fillfactor = 80);

CREATE INDEX idx_rt_materialization_job_pending
    ON rt.materialization_jobs (available_at, job_id)
    WHERE status = 'PENDING';
CREATE INDEX idx_rt_materialization_job_prune
    ON rt.materialization_jobs (updated_at)
    WHERE status IN ('COMPLETE', 'FAILED', 'EXPIRED', 'SUPERSEDED');

-- The universe barrier compares exact expected members, input hashes and outcomes.
CREATE TABLE rt.job_barriers (
    barrier_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    window_id uuid NOT NULL REFERENCES rt.window_runs(window_id),
    barrier_kind varchar(32) NOT NULL,
    universe_sha256 char(64) NOT NULL,
    expected_count integer NOT NULL,
    state varchar(12) NOT NULL DEFAULT 'OPEN',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    UNIQUE (window_id, barrier_kind),
    CHECK (btrim(barrier_kind) <> ''),
    CHECK (universe_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (expected_count > 0),
    CHECK (state IN ('OPEN', 'COMPLETE', 'FAILED')),
    CHECK ((state = 'OPEN') = (completed_at IS NULL))
) WITH (fillfactor = 80);

CREATE TABLE rt.job_barrier_members (
    barrier_id uuid NOT NULL REFERENCES rt.job_barriers(barrier_id),
    member_key varchar(128) NOT NULL,
    job_id uuid REFERENCES rt.materialization_jobs(job_id) ON DELETE SET NULL,
    expected_input_sha256 char(64) NOT NULL,
    outcome varchar(16),
    outcome_input_sha256 char(64),
    PRIMARY KEY (barrier_id, member_key),
    CHECK (btrim(member_key) <> ''),
    CHECK (expected_input_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (outcome IS NULL OR outcome IN ('COMPLETE', 'FAILED', 'BLOCKED', 'MISSING')),
    CHECK (outcome_input_sha256 IS NULL OR outcome_input_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK ((outcome IS NULL) = (outcome_input_sha256 IS NULL) OR outcome IN ('FAILED', 'BLOCKED', 'MISSING'))
) WITH (fillfactor = 80);

-- The 7-day job prune deletes jobs; the member keeps its recorded outcome with job_id cleared.
CREATE INDEX idx_rt_job_barrier_member_job
    ON rt.job_barrier_members (job_id) WHERE job_id IS NOT NULL;

CREATE TRIGGER trg_rt_acquisition_batches_immutable
    BEFORE UPDATE OR DELETE ON rt.acquisition_batches
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_acquisition_batches_no_truncate
    BEFORE TRUNCATE ON rt.acquisition_batches
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_acquisition_facts_immutable
    BEFORE UPDATE OR DELETE ON rt.acquisition_facts
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_bar_catalog_update
    BEFORE UPDATE ON rt.bar_catalog
    FOR EACH ROW EXECUTE FUNCTION rt.reject_catalog_update();
CREATE TRIGGER trg_rt_bar_revisions_immutable
    BEFORE UPDATE OR DELETE ON rt.bar_revisions
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_raw_payloads_immutable
    BEFORE UPDATE OR DELETE ON rt.raw_payloads
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_raw_payload_chunks_immutable
    BEFORE UPDATE OR DELETE ON rt.raw_payload_chunks
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
