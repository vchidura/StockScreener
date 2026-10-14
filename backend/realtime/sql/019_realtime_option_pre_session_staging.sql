SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE rt.option_pre_session_runs (
    run_id uuid PRIMARY KEY,
    profile_id uuid NOT NULL REFERENCES rt.capability_profiles(profile_id),
    profile_sha256 char(64) NOT NULL CHECK (profile_sha256 ~ '^[0-9a-f]{64}$'),
    provider_tier varchar(24) NOT NULL CHECK (provider_tier = 'MASSIVE_DEVELOPER'),
    universe_sha256 char(64) NOT NULL CHECK (universe_sha256 ~ '^[0-9a-f]{64}$'),
    policy_sha256 char(64) NOT NULL CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    session_date date NOT NULL,
    scheduled_cycle timestamptz NOT NULL,
    effective_at timestamptz NOT NULL,
    collection_deadline timestamptz NOT NULL,
    maximum_provider_requests integer NOT NULL CHECK (maximum_provider_requests BETWEEN 1 AND 100000),
    maximum_response_bytes bigint NOT NULL CHECK (maximum_response_bytes BETWEEN 1 AND 68719476736),
    expected_underlyings text[] NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (cardinality(expected_underlyings) BETWEEN 1 AND 1000),
    CHECK (scheduled_cycle <= collection_deadline AND effective_at <= scheduled_cycle)
);

CREATE TABLE rt.option_pre_session_work (
    run_id uuid NOT NULL REFERENCES rt.option_pre_session_runs(run_id),
    underlyer varchar(32) NOT NULL,
    security_id uuid NOT NULL REFERENCES ref.securities(security_id),
    status varchar(16) NOT NULL CHECK (status IN ('PENDING','CLAIMED','COMPLETE','BLOCKED')),
    owner_id uuid,
    owner_epoch bigint NOT NULL DEFAULT 0 CHECK (owner_epoch >= 0),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 3),
    provider_requests integer NOT NULL DEFAULT 0 CHECK (provider_requests >= 0),
    response_bytes bigint NOT NULL DEFAULT 0 CHECK (response_bytes >= 0),
    lease_expires_at timestamptz,
    completed_at timestamptz,
    reason text,
    PRIMARY KEY (run_id, underlyer),
    CHECK (underlyer = upper(underlyer) AND btrim(underlyer) <> ''),
    CHECK (status <> 'CLAIMED' OR (owner_id IS NOT NULL AND owner_epoch > 0 AND lease_expires_at IS NOT NULL))
) WITH (fillfactor = 80);

CREATE INDEX idx_rt_option_pre_session_work_pending ON rt.option_pre_session_work (run_id,status,underlyer)
    WHERE status IN ('PENDING','CLAIMED');

CREATE TABLE rt.option_pre_session_account_usage (
    run_id uuid PRIMARY KEY REFERENCES rt.option_pre_session_runs(run_id),
    provider_requests integer NOT NULL DEFAULT 0 CHECK (provider_requests >= 0),
    response_bytes bigint NOT NULL DEFAULT 0 CHECK (response_bytes >= 0),
    next_request_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
) WITH (fillfactor = 80);

CREATE TABLE rt.option_pre_session_packages (
    run_id uuid NOT NULL,
    underlyer varchar(32) NOT NULL,
    security_id uuid NOT NULL REFERENCES ref.securities(security_id),
    package_payload_text text NOT NULL CHECK (octet_length(package_payload_text) BETWEEN 1 AND 67108864),
    package_sha256 char(64) NOT NULL CHECK (package_sha256 ~ '^[0-9a-f]{64}$'),
    provider_requests integer NOT NULL CHECK (provider_requests >= 0),
    response_bytes bigint NOT NULL CHECK (response_bytes >= 0),
    completed_at timestamptz NOT NULL,
    PRIMARY KEY (run_id, underlyer),
    FOREIGN KEY (run_id, underlyer) REFERENCES rt.option_pre_session_work(run_id, underlyer)
);

CREATE TABLE rt.option_pre_session_finalizations (
    run_id uuid PRIMARY KEY REFERENCES rt.option_pre_session_runs(run_id),
    selection_id uuid NOT NULL UNIQUE REFERENCES rt.option_manifest_selection_audits(selection_id),
    package_set_sha256 char(64) NOT NULL CHECK (package_set_sha256 ~ '^[0-9a-f]{64}$'),
    provider_requests integer NOT NULL CHECK (provider_requests >= 0),
    response_bytes bigint NOT NULL CHECK (response_bytes >= 0),
    finalized_at timestamptz NOT NULL
);

CREATE TRIGGER trg_rt_option_pre_session_runs_immutable BEFORE UPDATE OR DELETE ON rt.option_pre_session_runs
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_pre_session_runs_no_truncate BEFORE TRUNCATE ON rt.option_pre_session_runs
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_pre_session_packages_immutable BEFORE UPDATE OR DELETE ON rt.option_pre_session_packages
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_pre_session_packages_no_truncate BEFORE TRUNCATE ON rt.option_pre_session_packages
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_pre_session_finalizations_immutable BEFORE UPDATE OR DELETE ON rt.option_pre_session_finalizations
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_pre_session_finalizations_no_truncate BEFORE TRUNCATE ON rt.option_pre_session_finalizations
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();