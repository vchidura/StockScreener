SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE rt.database_security_policies (
    policy_version varchar(64) PRIMARY KEY,
    policy jsonb NOT NULL,
    policy_sha256 char(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (btrim(policy_version) <> ''),
    CHECK (jsonb_typeof(policy) = 'object'),
    CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (policy->>'version' = policy_version),
    CHECK (policy->>'public_access' = 'NONE'),
    CHECK (jsonb_typeof(policy->'capability_roles') = 'array'),
    CHECK (jsonb_typeof(policy->'principal_profiles') = 'object')
);

WITH document(payload) AS (
    VALUES (
        $policy$
        {
          "version": "realtime_security_v1",
          "owner_role": "owner",
          "capability_roles": [
            "control",
            "ingress",
            "compute",
            "references",
            "publication",
            "reader",
            "maintenance"
          ],
          "principal_profiles": {
            "migrator": ["owner"],
            "runtime": [
              "control",
              "ingress",
              "compute",
              "references",
              "publication",
              "maintenance"
            ],
            "api": ["reader"]
          },
          "public_access": "NONE",
          "object_grants": "EXPLICIT_TABLE_ALLOWLIST_V1"
        }
        $policy$::jsonb
    )
)
INSERT INTO rt.database_security_policies (
    policy_version,
    policy,
    policy_sha256
)
SELECT
    payload->>'version',
    payload,
    encode(sha256(convert_to(payload::text, 'UTF8')), 'hex')
FROM document;

CREATE TRIGGER trg_rt_database_security_policies_immutable
    BEFORE UPDATE OR DELETE ON rt.database_security_policies
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_database_security_policies_no_truncate
    BEFORE TRUNCATE ON rt.database_security_policies
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();