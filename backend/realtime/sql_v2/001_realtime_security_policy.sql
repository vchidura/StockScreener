SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

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
    CHECK (jsonb_typeof(policy->'runtime_roles') = 'array'),
    UNIQUE (policy_version, policy_sha256)
);

WITH document(payload) AS (
    VALUES (
        $policy$
        {
          "version": "realtime_security_v3",
          "owner_role": "admin",
          "runtime_roles": ["app", "api"],
          "public_access": "NONE",
          "object_grants": "RELATION_REGISTRY_V2"
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

-- The policy above is data only: this migration creates no role, grant or default privilege.
-- The admin, app and api roles and their grants are a separate hardening phase applied after the
-- pipeline is validated with the runtime connected as the owner. The ledger records the phase; it starts
-- DEFERRED and moves one way: DEFERRED -> APPLIED -> AUDITED (re-applying needs a new audit).
CREATE TABLE rt.security_phase_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    policy_version varchar(64) NOT NULL,
    policy_sha256 char(64) NOT NULL,
    phase varchar(16) NOT NULL,
    recorded_by name NOT NULL DEFAULT current_user,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    detail jsonb NOT NULL DEFAULT '{}'::jsonb,
    FOREIGN KEY (policy_version, policy_sha256)
        REFERENCES rt.database_security_policies (policy_version, policy_sha256),
    CHECK (phase IN ('DEFERRED', 'APPLIED', 'AUDITED')),
    CHECK (jsonb_typeof(detail) = 'object')
);

CREATE FUNCTION rt.check_security_phase_transition() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'rt', 'ref'
    AS $$
DECLARE
    previous varchar(16);
BEGIN
    LOCK TABLE rt.security_phase_events IN SHARE ROW EXCLUSIVE MODE;
    SELECT phase INTO previous FROM rt.security_phase_events ORDER BY event_id DESC LIMIT 1;
    IF NOT (
        (previous IS NULL AND NEW.phase = 'DEFERRED')
        OR (previous = 'DEFERRED' AND NEW.phase = 'APPLIED')
        OR (previous IN ('APPLIED', 'AUDITED') AND NEW.phase IN ('APPLIED', 'AUDITED'))
    ) THEN
        RAISE EXCEPTION 'security phase transition % -> % is not allowed',
            COALESCE(previous, 'NONE'), NEW.phase;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_rt_security_phase_transition
    BEFORE INSERT ON rt.security_phase_events
    FOR EACH ROW EXECUTE FUNCTION rt.check_security_phase_transition();
CREATE TRIGGER trg_rt_security_phase_events_immutable
    BEFORE UPDATE OR DELETE ON rt.security_phase_events
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_security_phase_events_no_truncate
    BEFORE TRUNCATE ON rt.security_phase_events
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

INSERT INTO rt.security_phase_events (policy_version, policy_sha256, phase, detail)
SELECT policy_version, policy_sha256, 'DEFERRED',
       jsonb_build_object('reason', 'roles and grants are applied last, after validation')
FROM rt.database_security_policies
WHERE policy_version = 'realtime_security_v3';
