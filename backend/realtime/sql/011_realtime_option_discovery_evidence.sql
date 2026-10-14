SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE rt.option_discovery_batches (
    discovery_batch_id uuid PRIMARY KEY,
    underlying_security_id uuid NOT NULL REFERENCES ref.securities(security_id),
    profile_id uuid NOT NULL REFERENCES rt.capability_profiles(profile_id),
    profile_sha256 char(64) NOT NULL,
    universe_sha256 char(64) NOT NULL,
    provider_tier varchar(24) NOT NULL,
    provider varchar(64) NOT NULL,
    underlyer varchar(32) NOT NULL,
    scheduled_cycle timestamptz NOT NULL,
    expiration_from date NOT NULL,
    expiration_through date NOT NULL,
    maximum_dte integer NOT NULL,
    spot_price numeric(20,8) NOT NULL,
    strike_corridor_fraction numeric(10,8) NOT NULL,
    strike_min numeric(20,8) NOT NULL,
    strike_max numeric(20,8) NOT NULL,
    request_filter_sha256 char(64) NOT NULL,
    policy_sha256 char(64) NOT NULL,
    started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    page_count integer NOT NULL,
    row_count integer NOT NULL,
    response_bytes bigint NOT NULL,
    evidence_sha256 char(64) NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (
        profile_id, underlying_security_id, scheduled_cycle, request_filter_sha256
    ),
    CHECK (profile_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (universe_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (btrim(provider) <> ''),
    CHECK (underlyer = upper(underlyer) AND btrim(underlyer) <> ''),
    CHECK (expiration_through >= expiration_from),
    CHECK (maximum_dte BETWEEN 1 AND 365),
    CHECK (spot_price > 0),
    CHECK (
        strike_corridor_fraction > 0
        AND strike_corridor_fraction < 1
    ),
    CHECK (strike_min > 0 AND strike_max >= strike_min),
    CHECK (strike_min = spot_price * (1 - strike_corridor_fraction)),
    CHECK (strike_max = spot_price * (1 + strike_corridor_fraction)),
    CHECK (request_filter_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (completed_at >= started_at),
    CHECK (page_count BETWEEN 1 AND 40),
    CHECK (row_count BETWEEN 0 AND 10000),
    CHECK (response_bytes BETWEEN 1 AND 67108864),
    CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX idx_rt_option_discovery_underlying_cycle
    ON rt.option_discovery_batches (
        underlying_security_id, scheduled_cycle DESC, discovery_batch_id
    );

CREATE TABLE rt.option_discovery_pages (
    discovery_batch_id uuid NOT NULL
        REFERENCES rt.option_discovery_batches(discovery_batch_id),
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
    PRIMARY KEY (discovery_batch_id, page_number),
    CHECK (page_number BETWEEN 1 AND 40),
    CHECK (row_count BETWEEN 0 AND 10000),
    CHECK (octet_length(response_bytes) BETWEEN 1 AND 10485760),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (request_filter_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (
        request_cursor_sha256 IS NULL
        OR request_cursor_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CHECK (
        next_cursor_sha256 IS NULL
        OR next_cursor_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CHECK (jsonb_typeof(request_metadata) = 'object'),
    CHECK (NOT terminal OR next_cursor_sha256 IS NULL)
);

CREATE TABLE rt.option_discovery_coverage (
    discovery_batch_id uuid NOT NULL
        REFERENCES rt.option_discovery_batches(discovery_batch_id),
    dimension varchar(32) NOT NULL,
    disposition varchar(12) NOT NULL,
    comparison varchar(4) NOT NULL,
    boundary varchar(64) NOT NULL,
    reason_code varchar(64),
    PRIMARY KEY (
        discovery_batch_id, dimension, disposition, comparison
    ),
    CHECK (dimension IN ('EXPIRATION_DATE', 'STRIKE_PRICE')),
    CHECK (disposition IN ('INCLUDED', 'EXCLUDED')),
    CHECK (
        (disposition = 'INCLUDED' AND comparison IN ('GTE', 'LTE'))
        OR (disposition = 'EXCLUDED' AND comparison IN ('LT', 'GT'))
    ),
    CHECK (btrim(boundary) <> ''),
    CHECK (
        (disposition = 'INCLUDED' AND reason_code IS NULL)
        OR (
            disposition = 'EXCLUDED'
            AND reason_code IN (
                'OUTSIDE_DISCOVERY_WINDOW',
                'OUTSIDE_STRIKE_CORRIDOR'
            )
        )
    )
);

CREATE TRIGGER trg_rt_option_discovery_batches_immutable BEFORE UPDATE OR DELETE ON rt.option_discovery_batches FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_discovery_batches_no_truncate BEFORE TRUNCATE ON rt.option_discovery_batches FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_discovery_pages_immutable BEFORE UPDATE OR DELETE ON rt.option_discovery_pages FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_discovery_pages_no_truncate BEFORE TRUNCATE ON rt.option_discovery_pages FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_discovery_coverage_immutable BEFORE UPDATE OR DELETE ON rt.option_discovery_coverage FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_discovery_coverage_no_truncate BEFORE TRUNCATE ON rt.option_discovery_coverage FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();