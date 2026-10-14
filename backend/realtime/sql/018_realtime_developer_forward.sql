SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE rt.developer_forward_inputs (
    input_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL,
    provider_tier varchar(24) NOT NULL CHECK (provider_tier = 'MASSIVE_DEVELOPER'),
    scope_sha256 char(64) NOT NULL CHECK (scope_sha256 ~ '^[0-9a-f]{64}$'),
    selection_sha256 char(64) NOT NULL,
    configuration_sha256 char(64) NOT NULL,
    reference_sha256 char(64) NOT NULL,
    action_sha256 char(64) NOT NULL,
    cutoff timestamptz NOT NULL,
    available_after timestamptz NOT NULL,
    deadline timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    payload_text text NOT NULL CHECK (octet_length(payload_text) BETWEEN 1 AND 67108864),
    payload_sha256 char(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    UNIQUE (cohort_id, provider_tier, scope_sha256),
    UNIQUE (input_id, cohort_id, provider_tier),
    FOREIGN KEY (cohort_id, provider_tier) REFERENCES rt.provider_tier_cohorts(cohort_id, provider_tier),
    CHECK (cutoff <= available_after AND available_after <= received_at AND received_at <= deadline)
);

CREATE TABLE rt.developer_forward_work (
    input_id uuid PRIMARY KEY REFERENCES rt.developer_forward_inputs(input_id),
    status varchar(16) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'CLAIMED', 'COMPLETE', 'BLOCKED')),
    owner_id uuid,
    owner_epoch bigint NOT NULL DEFAULT 0 CHECK (owner_epoch >= 0),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 3),
    lease_expires_at timestamptz,
    completed_at timestamptz,
    reason text,
    CHECK (status <> 'CLAIMED' OR (owner_id IS NOT NULL AND owner_epoch > 0 AND lease_expires_at IS NOT NULL))
) WITH (fillfactor = 80);

CREATE INDEX idx_rt_developer_forward_pending ON rt.developer_forward_work (status, input_id)
    WHERE status IN ('PENDING', 'CLAIMED');

CREATE TABLE rt.developer_forward_artifacts (
    artifact_id uuid PRIMARY KEY,
    input_id uuid NOT NULL,
    cohort_id uuid NOT NULL,
    provider_tier varchar(24) NOT NULL CHECK (provider_tier = 'MASSIVE_DEVELOPER'),
    artifact_kind varchar(32) NOT NULL CHECK (artifact_kind IN ('MATRICES', 'CONTEXT', 'PACKAGES', 'FLOW', 'SCREENER', 'ALERTS')),
    artifact_key text NOT NULL,
    scope_sha256 char(64) NOT NULL,
    payload_text text NOT NULL CHECK (octet_length(payload_text) BETWEEN 1 AND 67108864),
    payload_sha256 char(64) NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    computed_at timestamptz NOT NULL,
    UNIQUE (input_id, artifact_kind, artifact_key),
    UNIQUE (artifact_id, input_id),
    FOREIGN KEY (input_id, cohort_id, provider_tier)
        REFERENCES rt.developer_forward_inputs(input_id, cohort_id, provider_tier)
);

CREATE TABLE rt.developer_forward_releases (
    release_id uuid PRIMARY KEY,
    input_id uuid NOT NULL UNIQUE REFERENCES rt.developer_forward_inputs(input_id),
    cohort_id uuid NOT NULL,
    provider_tier varchar(24) NOT NULL CHECK (provider_tier = 'MASSIVE_DEVELOPER'),
    scope_sha256 char(64) NOT NULL,
    manifest jsonb NOT NULL CHECK (jsonb_typeof(manifest) = 'object'),
    manifest_sha256 char(64) NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    published_at timestamptz NOT NULL,
    valid_until timestamptz NOT NULL,
    UNIQUE (release_id, cohort_id, provider_tier),
    FOREIGN KEY (input_id, cohort_id, provider_tier)
        REFERENCES rt.developer_forward_inputs(input_id, cohort_id, provider_tier),
    CHECK (published_at < valid_until)
);

CREATE TABLE rt.developer_forward_current (
    cohort_id uuid NOT NULL,
    provider_tier varchar(24) NOT NULL CHECK (provider_tier = 'MASSIVE_DEVELOPER'),
    release_id uuid NOT NULL,
    cutoff timestamptz NOT NULL,
    published_at timestamptz NOT NULL,
    PRIMARY KEY (cohort_id, provider_tier),
    FOREIGN KEY (release_id, cohort_id, provider_tier)
        REFERENCES rt.developer_forward_releases(release_id, cohort_id, provider_tier)
) WITH (fillfactor = 80);

CREATE TRIGGER trg_rt_developer_forward_inputs_immutable BEFORE UPDATE OR DELETE ON rt.developer_forward_inputs
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_developer_forward_inputs_no_truncate BEFORE TRUNCATE ON rt.developer_forward_inputs
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_developer_forward_artifacts_immutable BEFORE UPDATE OR DELETE ON rt.developer_forward_artifacts
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_developer_forward_artifacts_no_truncate BEFORE TRUNCATE ON rt.developer_forward_artifacts
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_developer_forward_releases_immutable BEFORE UPDATE OR DELETE ON rt.developer_forward_releases
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_developer_forward_releases_no_truncate BEFORE TRUNCATE ON rt.developer_forward_releases
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();