SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

-- The manifest is the equity universe plus the set of option underlyings. Option
-- contracts are not manifest members; they live in versioned slot sets under a cohort.
CREATE TABLE rt.subscription_manifests (
    manifest_id uuid PRIMARY KEY,
    manifest_sha256 char(64) NOT NULL UNIQUE,
    universe_sha256 char(64) NOT NULL,
    session_date date NOT NULL,
    effective_at timestamptz NOT NULL,
    profile_id uuid NOT NULL REFERENCES rt.capability_profiles(profile_id),
    manifest_kind varchar(32) NOT NULL,
    stock_count integer NOT NULL,
    option_underlying_count integer NOT NULL,
    member_count integer NOT NULL,
    allocation_policy_sha256 char(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (universe_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (allocation_policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (manifest_kind IN ('MARKET_OPTIONS', 'EQUITY_VALIDATION')),
    CHECK (stock_count > 0),
    CHECK (
        (manifest_kind = 'MARKET_OPTIONS' AND option_underlying_count BETWEEN 1 AND stock_count)
        OR (manifest_kind = 'EQUITY_VALIDATION' AND option_underlying_count = 0)
    ),
    -- At least one declared channel per stock; each stock declares its own channels
    -- (for example AM only), so no fixed multiplier is imposed.
    CHECK (member_count >= stock_count)
);

CREATE INDEX idx_rt_subscription_manifest_session
    ON rt.subscription_manifests (session_date DESC, effective_at DESC);

CREATE TABLE rt.subscription_members (
    manifest_id uuid NOT NULL REFERENCES rt.subscription_manifests(manifest_id),
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    channel varchar(4) NOT NULL,
    priority smallint NOT NULL,
    purpose varchar(24) NOT NULL,
    option_underlying boolean NOT NULL DEFAULT false,
    PRIMARY KEY (manifest_id, channel, instrument_id),
    CHECK (channel IN ('T', 'Q', 'AM')),
    CHECK (priority BETWEEN 1 AND 32767),
    CHECK (purpose IN ('PRIMARY', 'RETAINED_ALERT', 'POSITION'))
);

CREATE INDEX idx_rt_subscription_member_instrument
    ON rt.subscription_members (instrument_id, manifest_id);

-- One version per fixed-slot set (pre-session plus refreshes). A rotation inserts a new
-- version under the same cohort; it never creates a new cohort.
CREATE TABLE rt.slot_set_versions (
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    version integer NOT NULL,
    kind varchar(16) NOT NULL,
    effective_at timestamptz NOT NULL,
    policy_sha256 char(64) NOT NULL,
    member_sha256 char(64) NOT NULL,
    primary_contract_count integer NOT NULL,
    reserve_contract_count integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, version),
    CHECK (version > 0),
    CHECK (kind IN ('PRE_SESSION', 'REFRESH')),
    CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (member_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (primary_contract_count >= 0 AND reserve_contract_count >= 0),
    -- Provider cap on option quote subscriptions per connection.
    CHECK (primary_contract_count + reserve_contract_count <= 1000)
);

CREATE TABLE rt.slot_set_members (
    cohort_id uuid NOT NULL,
    version integer NOT NULL,
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    channel varchar(4) NOT NULL,
    purpose varchar(24) NOT NULL,
    priority smallint NOT NULL,
    catalog_version_id bigint NOT NULL REFERENCES ref.option_contract_versions(version_id),
    PRIMARY KEY (cohort_id, version, channel, instrument_id),
    FOREIGN KEY (cohort_id, version) REFERENCES rt.slot_set_versions(cohort_id, version),
    CHECK (channel IN ('T', 'Q')),
    CHECK (purpose IN ('PRIMARY', 'RESERVE', 'RETAINED_ALERT', 'POSITION')),
    CHECK (priority BETWEEN 1 AND 32767)
);

CREATE INDEX idx_rt_slot_set_member_instrument
    ON rt.slot_set_members (instrument_id, cohort_id, version);

-- The single pointer to the active slot set; activation is make-before-break.
CREATE TABLE rt.slot_set_current (
    cohort_id uuid PRIMARY KEY,
    version integer NOT NULL,
    activated_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (cohort_id, version) REFERENCES rt.slot_set_versions(cohort_id, version)
) WITH (fillfactor = 80);

CREATE TRIGGER trg_rt_subscription_manifests_immutable
    BEFORE UPDATE OR DELETE ON rt.subscription_manifests
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_subscription_manifests_no_truncate
    BEFORE TRUNCATE ON rt.subscription_manifests
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_subscription_members_immutable
    BEFORE UPDATE OR DELETE ON rt.subscription_members
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_subscription_members_no_truncate
    BEFORE TRUNCATE ON rt.subscription_members
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_slot_set_versions_immutable
    BEFORE UPDATE OR DELETE ON rt.slot_set_versions
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_slot_set_versions_no_truncate
    BEFORE TRUNCATE ON rt.slot_set_versions
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_slot_set_members_immutable
    BEFORE UPDATE OR DELETE ON rt.slot_set_members
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_slot_set_members_no_truncate
    BEFORE TRUNCATE ON rt.slot_set_members
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
