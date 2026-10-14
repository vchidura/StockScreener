SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE rt.quote_current (
    provider varchar(32) NOT NULL,
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    source_session date NOT NULL,
    source_sequence bigint,
    event_time_raw bigint NOT NULL,
    event_time_unit varchar(8) NOT NULL,
    event_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    bid numeric(20,8),
    ask numeric(20,8),
    bid_size bigint,
    ask_size bigint,
    bid_exchange integer,
    ask_exchange integer,
    quality varchar(16) NOT NULL,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    session_id uuid NOT NULL REFERENCES rt.stream_sessions(session_id),
    journal_generation uuid NOT NULL,
    shard_id smallint NOT NULL,
    source_offset bigint NOT NULL,
    payload_sha256 char(64) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider, instrument_id),
    CHECK (btrim(provider) <> ''),
    CHECK (source_sequence IS NULL OR source_sequence >= 0),
    CHECK (event_time_raw >= 0),
    CHECK (event_time_unit IN ('s', 'ms', 'us', 'ns')),
    CHECK (bid_size IS NULL OR bid_size >= 0),
    CHECK (ask_size IS NULL OR ask_size >= 0),
    CHECK (quality IN ('GOOD', 'ONE_SIDED', 'LOCKED', 'CROSSED', 'STALE', 'INVALID', 'UNCLASSIFIED')),
    CHECK (shard_id >= 0 AND source_offset > 0),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK ((quality = 'GOOD' AND cardinality(reason_codes) = 0) OR quality <> 'GOOD')
) WITH (fillfactor = 80);

CREATE TABLE rt.trade_current (
    provider varchar(32) NOT NULL,
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    source_session date NOT NULL,
    source_sequence bigint,
    event_time_raw bigint NOT NULL,
    event_time_unit varchar(8) NOT NULL,
    event_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    price numeric(20,8),
    size numeric(28,8),
    exchange integer,
    conditions integer[] NOT NULL DEFAULT ARRAY[]::integer[],
    eligibility varchar(16) NOT NULL,
    eligibility_policy_version varchar(96) NOT NULL,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    session_id uuid NOT NULL REFERENCES rt.stream_sessions(session_id),
    journal_generation uuid NOT NULL,
    shard_id smallint NOT NULL,
    source_offset bigint NOT NULL,
    payload_sha256 char(64) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider, instrument_id),
    CHECK (btrim(provider) <> ''),
    CHECK (source_sequence IS NULL OR source_sequence >= 0),
    CHECK (event_time_raw >= 0),
    CHECK (event_time_unit IN ('s', 'ms', 'us', 'ns')),
    CHECK (eligibility IN ('INCLUDED', 'EXCLUDED', 'CANCELED', 'SUPERSEDED', 'UNKNOWN', 'INVALID')),
    CHECK (btrim(eligibility_policy_version) <> ''),
    CHECK (shard_id >= 0 AND source_offset > 0),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$')
) WITH (fillfactor = 80);

CREATE TABLE rt.bar_revisions (
    bar_revision_id uuid PRIMARY KEY,
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
    supersedes_revision_id uuid REFERENCES rt.bar_revisions(bar_revision_id),
    payload_sha256 char(64) NOT NULL,
    quality_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (bar_revision_id, cohort_id, provider_tier),
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
);

CREATE INDEX idx_rt_bar_revision_instrument_end
    ON rt.bar_revisions (cohort_id, instrument_id, interval, bar_end DESC);

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
    PRIMARY KEY (
        cohort_id, instrument_id, interval, bar_start,
        session_scope, adjustment_basis
    ),
    FOREIGN KEY (bar_revision_id, cohort_id, provider_tier)
        REFERENCES rt.bar_revisions(bar_revision_id, cohort_id, provider_tier),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (interval IN ('1m', '5m', '15m', '30m', '1h', '1d', '1wk', '1mo')),
    CHECK (session_scope IN ('RTH', 'EXTENDED', 'FULL_DAY')),
    CHECK (adjustment_basis IN ('RAW_UNADJUSTED', 'ADJUSTED')),
    CHECK (source_kind IN ('NATIVE_REST', 'REALTIME_STREAM', 'DERIVED', 'RECONCILED')),
    CHECK (bar_end > bar_start AND observed_at >= bar_end)
) WITH (fillfactor = 80);

CREATE TABLE rt.consumer_checkpoints (
    consumer_name varchar(96) NOT NULL,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    journal_generation uuid NOT NULL,
    shard_id smallint NOT NULL,
    shard_map_sha256 char(64) NOT NULL,
    owner_epoch bigint NOT NULL,
    committed_offset bigint NOT NULL,
    checkpoint_id uuid NOT NULL UNIQUE,
    state_codec varchar(96) NOT NULL,
    state_hash char(64) NOT NULL,
    state_payload bytea NOT NULL,
    committed_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer_name, cohort_id, journal_generation, shard_id),
    CHECK (btrim(consumer_name) <> '' AND btrim(state_codec) <> ''),
    CHECK (shard_id >= 0 AND owner_epoch > 0 AND committed_offset >= 0),
    CHECK (shard_map_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (state_hash ~ '^[0-9a-f]{64}$'),
    CHECK (octet_length(state_payload) BETWEEN 1 AND 1048576)
) WITH (fillfactor = 80);

CREATE INDEX idx_rt_consumer_checkpoint_retention
    ON rt.consumer_checkpoints (journal_generation, shard_id, committed_offset);

CREATE TABLE rt.materialization_jobs (
    job_id uuid PRIMARY KEY,
    dedup_sha256 char(64) NOT NULL UNIQUE,
    job_kind varchar(40) NOT NULL,
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    source_revision_id uuid NOT NULL,
    bundle_sha256 char(64),
    status varchar(16) NOT NULL,
    available_at timestamptz NOT NULL,
    lease_owner uuid,
    lease_epoch bigint NOT NULL DEFAULT 0,
    lease_until timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    failure_code varchar(64),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (dedup_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (bundle_sha256 IS NULL OR bundle_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (btrim(job_kind) <> ''),
    CHECK (status IN ('PENDING', 'LEASED', 'COMPLETE', 'FAILED', 'BLOCKED')),
    CHECK (lease_epoch >= 0 AND attempts >= 0),
    CHECK ((status = 'LEASED') = (lease_owner IS NOT NULL AND lease_until IS NOT NULL)),
    CHECK (lease_until IS NULL OR lease_until > available_at)
);

CREATE INDEX idx_rt_materialization_job_pending
    ON rt.materialization_jobs (available_at, job_id)
    WHERE status = 'PENDING';

CREATE TABLE rt.archive_segments (
    segment_id uuid PRIMARY KEY,
    journal_generation uuid NOT NULL,
    shard_id smallint NOT NULL,
    first_offset bigint NOT NULL,
    last_offset bigint NOT NULL,
    row_count bigint NOT NULL,
    first_event_at timestamptz NOT NULL,
    last_event_at timestamptz NOT NULL,
    uri text NOT NULL,
    checksum_sha256 char(64) NOT NULL,
    schema_version varchar(64) NOT NULL,
    verified_at timestamptz NOT NULL,
    UNIQUE (journal_generation, shard_id, first_offset, last_offset),
    CHECK (shard_id >= 0),
    CHECK (first_offset > 0 AND last_offset >= first_offset),
    CHECK (row_count > 0 AND row_count <= last_offset - first_offset + 1),
    CHECK (last_event_at >= first_event_at),
    CHECK (btrim(uri) <> '' AND btrim(schema_version) <> ''),
    CHECK (checksum_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX idx_rt_archive_segment_retention
    ON rt.archive_segments (journal_generation, shard_id, last_offset);

CREATE TRIGGER trg_rt_archive_segments_immutable BEFORE UPDATE OR DELETE ON rt.archive_segments FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_archive_segments_no_truncate BEFORE TRUNCATE ON rt.archive_segments FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_bar_revisions_immutable BEFORE UPDATE OR DELETE ON rt.bar_revisions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_bar_revisions_no_truncate BEFORE TRUNCATE ON rt.bar_revisions FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();