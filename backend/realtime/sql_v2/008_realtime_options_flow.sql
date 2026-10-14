SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

-- Provider facts, as fetched from GET /v3/reference/conditions and the exchanges reference.
-- A new fetch is a new payload hash; rows are never edited.
CREATE TABLE ref.trade_condition_codes (
    provider varchar(32) NOT NULL,
    asset_class varchar(12) NOT NULL,
    data_type varchar(24) NOT NULL,
    condition_id integer NOT NULL,
    abbreviation varchar(64),
    name text NOT NULL,
    description text,
    condition_type varchar(40) NOT NULL,
    legacy boolean NOT NULL DEFAULT false,
    exchange integer,
    update_rules jsonb NOT NULL DEFAULT '{}'::jsonb,
    sip_mapping jsonb NOT NULL DEFAULT '{}'::jsonb,
    fetched_at timestamptz NOT NULL,
    payload_sha256 char(64) NOT NULL,
    PRIMARY KEY (provider, asset_class, data_type, condition_id, payload_sha256),
    CHECK (asset_class IN ('stocks', 'options', 'crypto', 'fx')),
    CHECK (btrim(provider) <> '' AND btrim(data_type) <> '' AND btrim(name) <> ''),
    CHECK (condition_id >= 0),
    CHECK (jsonb_typeof(update_rules) = 'object' AND jsonb_typeof(sip_mapping) = 'object'),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE ref.exchange_codes (
    provider varchar(32) NOT NULL,
    asset_class varchar(12) NOT NULL,
    exchange_id integer NOT NULL,
    name text NOT NULL,
    mic varchar(16),
    exchange_type varchar(32),
    fetched_at timestamptz NOT NULL,
    payload_sha256 char(64) NOT NULL,
    PRIMARY KEY (provider, asset_class, exchange_id, payload_sha256),
    CHECK (btrim(provider) <> '' AND btrim(name) <> ''),
    CHECK (exchange_id >= 0),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$')
);

-- Our reviewed interpretation of a provider condition code for one semantics version.
-- A code absent from the reviewed version fails closed as UNKNOWN in the engine.
CREATE TABLE ref.option_condition_semantics (
    semantics_version varchar(64) NOT NULL,
    condition_id integer NOT NULL,
    treatment varchar(12) NOT NULL,
    volume_eligible boolean NOT NULL,
    is_cancel boolean NOT NULL DEFAULT false,
    is_late boolean NOT NULL DEFAULT false,
    is_multi_leg boolean NOT NULL DEFAULT false,
    is_cross boolean NOT NULL DEFAULT false,
    is_sweep boolean NOT NULL DEFAULT false,
    is_floor boolean NOT NULL DEFAULT false,
    source_payload_sha256 char(64) NOT NULL,
    reviewed_at timestamptz NOT NULL,
    review_note text,
    PRIMARY KEY (semantics_version, condition_id),
    CHECK (btrim(semantics_version) <> ''),
    CHECK (treatment IN ('INCLUDED', 'EXCLUDED', 'CANCELED')),
    CHECK (treatment <> 'CANCELED' OR is_cancel),
    CHECK (treatment <> 'INCLUDED' OR (volume_eligible AND NOT is_cancel)),
    CHECK (source_payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TRIGGER trg_ref_trade_condition_codes_immutable
    BEFORE UPDATE OR DELETE ON ref.trade_condition_codes
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_trade_condition_codes_no_truncate
    BEFORE TRUNCATE ON ref.trade_condition_codes
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_exchange_codes_immutable
    BEFORE UPDATE OR DELETE ON ref.exchange_codes
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_exchange_codes_no_truncate
    BEFORE TRUNCATE ON ref.exchange_codes
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_option_condition_semantics_immutable
    BEFORE UPDATE OR DELETE ON ref.option_condition_semantics
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_ref_option_condition_semantics_no_truncate
    BEFORE TRUNCATE ON ref.option_condition_semantics
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

-- Filtered option prints (size or premium threshold passed). Cumulative volume is
-- accumulated from every eligible print before filtering; only the survivors land here.
-- Contract references are by ticker (soft): T.* covers contracts that never become
-- instruments. The quote is copied by value and must not postdate the trade (no lookahead).
CREATE TABLE rt.option_flow_events (
    event_id uuid NOT NULL,
    event_time timestamptz NOT NULL,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    session_date date NOT NULL,
    provider_tier varchar(24) NOT NULL,
    contract_ticker varchar(64) NOT NULL,
    contract_id bigint,
    underlyer varchar(32) NOT NULL,
    source_seq bigint NOT NULL,
    exchange_id integer,
    price numeric(20,8) NOT NULL,
    size bigint NOT NULL,
    multiplier integer NOT NULL DEFAULT 100,
    premium numeric(28,8) NOT NULL,
    conditions integer[] NOT NULL DEFAULT ARRAY[]::integer[],
    condition_semantics_version varchar(64) NOT NULL,
    trade_treatment varchar(12) NOT NULL,
    is_multi_leg boolean NOT NULL DEFAULT false,
    side_class varchar(32) NOT NULL,
    side_confidence varchar(8) NOT NULL,
    quote_bid numeric(20,8),
    quote_ask numeric(20,8),
    quote_bid_size bigint,
    quote_ask_size bigint,
    quote_time timestamptz,
    quote_seq bigint,
    cumulative_volume_before bigint,
    cumulative_volume_after bigint,
    volume_baseline_kind varchar(24) NOT NULL,
    prior_open_interest bigint,
    open_interest_as_of date,
    volume_exceeds_prior_oi boolean NOT NULL DEFAULT false,
    opening_inference varchar(16) NOT NULL DEFAULT 'NOT_ASSESSED',
    spread_position numeric(6,5),
    opening_score numeric(5,4),
    opening_reasons text[] NOT NULL DEFAULT ARRAY[]::text[],
    journal_generation uuid,
    shard_id smallint,
    source_offset bigint,
    -- A cancel or correction is a new revision of the same print (revision >= 2) that points at the
    -- revision it supersedes, so a provider that reuses the original sequence and time cannot collide.
    revision integer NOT NULL DEFAULT 1,
    supersedes_event_id uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (event_id, event_time),
    UNIQUE (contract_ticker, source_seq, event_time, revision),
    CHECK (revision >= 1 AND ((revision = 1) = (supersedes_event_id IS NULL))),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (contract_ticker = upper(contract_ticker) AND btrim(contract_ticker) <> ''),
    CHECK (underlyer = upper(underlyer) AND btrim(underlyer) <> ''),
    CHECK (source_seq >= 0 AND price > 0 AND size > 0 AND multiplier > 0 AND premium >= 0),
    CHECK (trade_treatment IN ('INCLUDED', 'EXCLUDED', 'CANCELED', 'UNKNOWN')),
    CHECK (side_class IN (
        'AT_OR_ABOVE_ASK', 'AT_OR_BELOW_BID', 'INSIDE_SPREAD', 'QUOTE_CROSSED_OR_LOCKED',
        'QUOTE_STALE', 'NO_QUOTE', 'AGGRESSION_UNAVAILABLE_NO_QUOTE', 'EXCLUDED_COMPLEX', 'NOT_CLASSIFIED'
    )),
    CHECK (side_confidence IN ('HIGH', 'MEDIUM', 'LOW', 'NONE')),
    CHECK (side_class NOT IN ('AT_OR_ABOVE_ASK', 'AT_OR_BELOW_BID', 'INSIDE_SPREAD')
        OR (quote_bid IS NOT NULL AND quote_ask IS NOT NULL AND quote_time IS NOT NULL
            AND side_confidence <> 'NONE')),
    CHECK (quote_time IS NULL OR quote_time <= event_time),
    CHECK (quote_bid IS NULL OR quote_ask IS NULL OR quote_bid >= 0 AND quote_ask >= 0),
    CHECK (volume_baseline_kind IN ('SESSION_COMPLETE', 'SNAPSHOT_RECONCILED', 'WARMING', 'INCOMPLETE')),
    CHECK (cumulative_volume_after IS NULL OR cumulative_volume_before IS NULL
        OR cumulative_volume_after >= cumulative_volume_before),
    -- The factual flag needs a prior open interest and a complete volume baseline.
    CHECK (NOT volume_exceeds_prior_oi
        OR (prior_open_interest IS NOT NULL AND open_interest_as_of IS NOT NULL
            AND cumulative_volume_after > prior_open_interest
            AND volume_baseline_kind IN ('SESSION_COMPLETE', 'SNAPSHOT_RECONCILED'))),
    CHECK (opening_inference IN ('NOT_ASSESSED', 'LIKELY_OPENING', 'UNLIKELY_OPENING', 'INDETERMINATE')),
    CHECK (opening_inference <> 'LIKELY_OPENING' OR volume_exceeds_prior_oi),
    -- Spread position exists only for an inside-spread print; the score and its reasons only for an assessed one.
    CHECK (spread_position IS NULL OR (spread_position BETWEEN 0 AND 1 AND side_class = 'INSIDE_SPREAD')),
    CHECK (opening_score IS NULL OR (opening_score BETWEEN 0 AND 1 AND opening_inference <> 'NOT_ASSESSED')),
    CHECK (opening_inference = 'NOT_ASSESSED' OR cardinality(opening_reasons) > 0),
    CHECK (source_offset IS NULL OR source_offset > 0)
) PARTITION BY RANGE (event_time);

CREATE INDEX idx_rt_option_flow_event_contract
    ON rt.option_flow_events (cohort_id, contract_ticker, event_time DESC);
CREATE INDEX idx_rt_option_flow_event_underlyer
    ON rt.option_flow_events (cohort_id, underlyer, event_time DESC);

-- Revisioned time-window clusters per contract and side. A cancel or late print produces a
-- new revision that supersedes the previous one; rows are never edited.
CREATE TABLE rt.option_flow_clusters (
    cluster_id uuid NOT NULL,
    revision integer NOT NULL,
    session_date date NOT NULL,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    contract_ticker varchar(64) NOT NULL,
    underlyer varchar(32) NOT NULL,
    window_seconds integer NOT NULL,
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    side_class varchar(32) NOT NULL,
    print_count integer NOT NULL,
    total_size bigint NOT NULL,
    total_premium numeric(28,8) NOT NULL,
    distinct_exchanges integer NOT NULL,
    first_event_time timestamptz NOT NULL,
    last_event_time timestamptz NOT NULL,
    status varchar(12) NOT NULL,
    policy_sha256 char(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cluster_id, revision, session_date),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (revision >= 1),
    CHECK (contract_ticker = upper(contract_ticker) AND underlyer = upper(underlyer)),
    CHECK (window_seconds > 0 AND window_end - window_start = window_seconds * interval '1 second'),
    CHECK (side_class IN (
        'AT_OR_ABOVE_ASK', 'AT_OR_BELOW_BID', 'INSIDE_SPREAD', 'MIXED', 'NO_QUOTE',
        'AGGRESSION_UNAVAILABLE_NO_QUOTE', 'NOT_CLASSIFIED'
    )),
    -- A cluster whose prints were all cancelled is stored as an explicit RETRACTED revision (no prints),
    -- so readers never keep showing a cluster that has no active print.
    CHECK (
        (status = 'RETRACTED' AND revision > 1 AND print_count = 0 AND total_size = 0 AND total_premium = 0
            AND distinct_exchanges = 0)
        OR (status <> 'RETRACTED' AND print_count >= 1 AND total_size >= print_count AND total_premium >= 0
            AND distinct_exchanges >= 1 AND distinct_exchanges <= print_count)
    ),
    CHECK (first_event_time >= window_start AND last_event_time <= window_end
        AND last_event_time >= first_event_time),
    CHECK (status IN ('PROVISIONAL', 'FINAL', 'RETRACTED')),
    CHECK (policy_sha256 ~ '^[0-9a-f]{64}$')
) PARTITION BY RANGE (session_date);

CREATE INDEX idx_rt_option_flow_cluster_contract
    ON rt.option_flow_clusters (cohort_id, contract_ticker, window_start DESC);

-- Cumulative eligible volume per contract and session. The accumulator is trusted only
-- when its baseline is complete; a restart or stream gap makes it WARMING or INCOMPLETE
-- until it is reconciled with the REST snapshot volume at a single watermark.
CREATE TABLE rt.option_volume_checkpoints (
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    session_date date NOT NULL,
    contract_ticker varchar(64) NOT NULL,
    cumulative_volume bigint NOT NULL,
    last_source_seq bigint NOT NULL,
    last_event_time timestamptz,
    baseline_kind varchar(24) NOT NULL,
    snapshot_volume bigint,
    snapshot_watermark_seq bigint,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, session_date, contract_ticker),
    CHECK (contract_ticker = upper(contract_ticker) AND btrim(contract_ticker) <> ''),
    CHECK (cumulative_volume >= 0 AND last_source_seq >= 0),
    CHECK (baseline_kind IN ('SESSION_COMPLETE', 'SNAPSHOT_RECONCILED', 'WARMING', 'INCOMPLETE')),
    CHECK (baseline_kind <> 'SNAPSHOT_RECONCILED'
        OR (snapshot_volume IS NOT NULL AND snapshot_watermark_seq IS NOT NULL)),
    CHECK (snapshot_volume IS NULL OR snapshot_volume >= 0)
) WITH (fillfactor = 80);

CREATE INDEX idx_rt_option_volume_checkpoint_prune
    ON rt.option_volume_checkpoints (session_date);

-- Raw provider discovery pages. The batch and coverage rows are the permanent evidence (they keep
-- the payload and evidence digests); the page bodies are replay material only, so they follow the
-- raw-payload HOT_TTL window and are pruned by detaching whole daily partitions. An options cycle
-- input carries discovery batch ids and hashes, never another copy of these bytes.
CREATE TABLE rt.option_discovery_pages (
    discovery_batch_id uuid NOT NULL REFERENCES rt.option_discovery_batches(discovery_batch_id),
    page_number integer NOT NULL,
    row_count integer NOT NULL,
    response_bytes bytea NOT NULL,
    payload_sha256 char(64) NOT NULL,
    received_at timestamptz NOT NULL,
    terminal boolean NOT NULL,
    request_filter_sha256 char(64) NOT NULL,
    request_cursor_sha256 char(64),
    request_id varchar(160),
    next_cursor_sha256 char(64),
    request_metadata jsonb NOT NULL,
    PRIMARY KEY (discovery_batch_id, page_number, received_at),
    CHECK (NOT terminal OR next_cursor_sha256 IS NULL),
    CHECK (next_cursor_sha256 IS NULL OR next_cursor_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (page_number BETWEEN 1 AND 40),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (request_cursor_sha256 IS NULL OR request_cursor_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (request_filter_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(request_metadata) = 'object'),
    CHECK (octet_length(response_bytes) BETWEEN 1 AND 10485760),
    CHECK (row_count BETWEEN 0 AND 10000)
) PARTITION BY RANGE (received_at);

CREATE TRIGGER trg_rt_option_flow_events_immutable
    BEFORE UPDATE OR DELETE ON rt.option_flow_events
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_flow_clusters_immutable
    BEFORE UPDATE OR DELETE ON rt.option_flow_clusters
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_discovery_pages_immutable
    BEFORE UPDATE OR DELETE ON rt.option_discovery_pages
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_discovery_pages_no_truncate
    BEFORE TRUNCATE ON rt.option_discovery_pages
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
