SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.advanced_stream_authorizations (
    authorization_id uuid PRIMARY KEY,
    authorization_reference varchar(160) NOT NULL UNIQUE,
    scope_sha256 char(64) NOT NULL UNIQUE,
    parent_authorization_id uuid REFERENCES rt.advanced_stream_authorizations(
        authorization_id
    ),
    provider varchar(32) NOT NULL,
    feed varchar(16) NOT NULL,
    channel varchar(16) NOT NULL,
    allowed_universe_targets integer[] NOT NULL,
    valid_from date NOT NULL,
    valid_through date NOT NULL,
    maximum_daily_stream_sessions integer NOT NULL,
    maximum_daily_connection_attempts integer NOT NULL,
    maximum_daily_decoded_bytes bigint NOT NULL,
    maximum_daily_rest_repair_requests integer NOT NULL,
    maximum_daily_rest_repair_decoded_bytes bigint NOT NULL,
    runtime_policy_sha256 char(64) NOT NULL,
    approval_source varchar(96) NOT NULL,
    approval_reference varchar(160) NOT NULL,
    status varchar(16) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    revoked_at timestamptz,
    revocation_reference varchar(160),
    CHECK (authorization_id IS DISTINCT FROM parent_authorization_id),
    CHECK (
        provider = 'massive'
        AND feed = 'STOCKS'
        AND channel = 'AM'
    ),
    CHECK (
        cardinality(allowed_universe_targets) = 1
        AND allowed_universe_targets[1] BETWEEN 1 AND 400
    ),
    CHECK (valid_through >= valid_from),
    CHECK (
        maximum_daily_stream_sessions > 0
        AND maximum_daily_connection_attempts
            >= maximum_daily_stream_sessions
        AND maximum_daily_decoded_bytes > 0
        AND maximum_daily_rest_repair_requests >= 0
        AND maximum_daily_rest_repair_decoded_bytes >= 0
        AND (
            (maximum_daily_rest_repair_requests = 0)
            = (maximum_daily_rest_repair_decoded_bytes = 0)
        )
    ),
    CHECK (
        scope_sha256 ~ '^[0-9a-f]{64}$'
        AND runtime_policy_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CHECK (
        btrim(authorization_reference) <> ''
        AND btrim(approval_source) <> ''
        AND btrim(approval_reference) <> ''
    ),
    CHECK (status IN ('READY', 'REVOKED', 'EXPIRED')),
    CHECK (
        (
            status = 'REVOKED'
            AND revoked_at IS NOT NULL
            AND btrim(revocation_reference) <> ''
        )
        OR
        (
            status <> 'REVOKED'
            AND revoked_at IS NULL
            AND revocation_reference IS NULL
        )
    )
);

CREATE TABLE rt.advanced_stream_authorization_status (
    authorization_id uuid PRIMARY KEY REFERENCES
        rt.advanced_stream_authorizations(authorization_id),
    status varchar(16) NOT NULL,
    valid_from date NOT NULL,
    valid_through date NOT NULL,
    revoked_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (status IN ('READY', 'REVOKED', 'EXPIRED')),
    CHECK (valid_through >= valid_from),
    CHECK (
        (status = 'REVOKED' AND revoked_at IS NOT NULL)
        OR
        (status <> 'REVOKED' AND revoked_at IS NULL)
    )
);

CREATE TABLE rt.advanced_ingress_plans (
    plan_id uuid PRIMARY KEY,
    plan_sha256 char(64) NOT NULL UNIQUE,
    authorization_id uuid NOT NULL REFERENCES
        rt.advanced_stream_authorizations(authorization_id),
    authorization_scope_sha256 char(64) NOT NULL,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    manifest_id uuid NOT NULL REFERENCES
        rt.subscription_manifests(manifest_id),
    session_date date NOT NULL,
    universe_target integer NOT NULL,
    mode varchar(16) NOT NULL,
    runtime_profile_sha256 char(64) NOT NULL,
    runtime_policy_sha256 char(64) NOT NULL,
    authorized_start_at timestamptz NOT NULL,
    authorized_end_at timestamptz NOT NULL,
    maximum_stream_sessions integer NOT NULL,
    maximum_connection_attempts integer NOT NULL,
    maximum_decoded_bytes bigint NOT NULL,
    maximum_rest_repair_requests integer NOT NULL,
    maximum_rest_repair_decoded_bytes bigint NOT NULL,
    status varchar(16) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    activated_at timestamptz,
    completed_at timestamptz,
    UNIQUE (authorization_id, session_date),
    CHECK (
        plan_sha256 ~ '^[0-9a-f]{64}$'
        AND authorization_scope_sha256 ~ '^[0-9a-f]{64}$'
        AND runtime_profile_sha256 ~ '^[0-9a-f]{64}$'
        AND runtime_policy_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CHECK (universe_target BETWEEN 1 AND 400),
    CHECK (mode = 'SHADOW'),
    CHECK (authorized_end_at > authorized_start_at),
    CHECK (
        maximum_stream_sessions > 0
        AND maximum_connection_attempts >= maximum_stream_sessions
        AND maximum_decoded_bytes > 0
        AND maximum_rest_repair_requests >= 0
        AND maximum_rest_repair_decoded_bytes >= 0
        AND (
            (maximum_rest_repair_requests = 0)
            = (maximum_rest_repair_decoded_bytes = 0)
        )
    ),
    CHECK (status IN ('READY', 'ACTIVE', 'COMPLETE', 'BLOCKED', 'FAILED')),
    CHECK (activated_at IS NULL OR activated_at >= created_at),
    CHECK (completed_at IS NULL OR completed_at >= created_at)
);

CREATE TABLE rt.advanced_ingress_plan_usage (
    plan_id uuid PRIMARY KEY REFERENCES rt.advanced_ingress_plans(plan_id),
    stream_session_count integer NOT NULL DEFAULT 0,
    connection_attempt_count integer NOT NULL DEFAULT 0,
    decoded_byte_count bigint NOT NULL DEFAULT 0,
    rest_repair_request_count integer NOT NULL DEFAULT 0,
    rest_repair_decoded_byte_count bigint NOT NULL DEFAULT 0,
    latest_owner_epoch bigint NOT NULL DEFAULT 0,
    latest_stream_session_id uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        stream_session_count >= 0
        AND connection_attempt_count >= stream_session_count
        AND decoded_byte_count >= 0
        AND rest_repair_request_count >= 0
        AND rest_repair_decoded_byte_count >= 0
        AND latest_owner_epoch >= 0
        AND updated_at >= created_at
    ),
    CHECK (
        (latest_owner_epoch = 0 AND latest_stream_session_id IS NULL)
        OR
        (latest_owner_epoch > 0 AND latest_stream_session_id IS NOT NULL)
    )
);

ALTER TABLE rt.stream_sessions
    ADD COLUMN advanced_ingress_plan_id uuid REFERENCES
        rt.advanced_ingress_plans(plan_id);

CREATE INDEX idx_rt_stream_sessions_advanced_ingress_plan
    ON rt.stream_sessions (advanced_ingress_plan_id, owner_epoch)
    WHERE advanced_ingress_plan_id IS NOT NULL;

CREATE FUNCTION rt.enforce_advanced_authorization_change()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'advanced stream authorizations are durable evidence';
    END IF;
    IF (
        NEW.authorization_id,
        NEW.authorization_reference,
        NEW.scope_sha256,
        NEW.parent_authorization_id,
        NEW.provider,
        NEW.feed,
        NEW.channel,
        NEW.allowed_universe_targets,
        NEW.valid_from,
        NEW.valid_through,
        NEW.maximum_daily_stream_sessions,
        NEW.maximum_daily_connection_attempts,
        NEW.maximum_daily_decoded_bytes,
        NEW.maximum_daily_rest_repair_requests,
        NEW.maximum_daily_rest_repair_decoded_bytes,
        NEW.runtime_policy_sha256,
        NEW.approval_source,
        NEW.approval_reference,
        NEW.created_at
    ) IS DISTINCT FROM (
        OLD.authorization_id,
        OLD.authorization_reference,
        OLD.scope_sha256,
        OLD.parent_authorization_id,
        OLD.provider,
        OLD.feed,
        OLD.channel,
        OLD.allowed_universe_targets,
        OLD.valid_from,
        OLD.valid_through,
        OLD.maximum_daily_stream_sessions,
        OLD.maximum_daily_connection_attempts,
        OLD.maximum_daily_decoded_bytes,
        OLD.maximum_daily_rest_repair_requests,
        OLD.maximum_daily_rest_repair_decoded_bytes,
        OLD.runtime_policy_sha256,
        OLD.approval_source,
        OLD.approval_reference,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'advanced stream authorization scope is immutable';
    END IF;
    IF NEW.status = OLD.status THEN
        IF (NEW.revoked_at, NEW.revocation_reference) IS DISTINCT FROM
           (OLD.revoked_at, OLD.revocation_reference) THEN
            RAISE EXCEPTION 'authorization revocation evidence is immutable';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.status = 'READY' AND NEW.status = 'REVOKED' THEN
        RETURN NEW;
    END IF;
    IF OLD.status = 'READY' AND NEW.status = 'EXPIRED' THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'invalid advanced authorization transition % -> %',
        OLD.status, NEW.status;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_stream_authorizations_durable
    BEFORE UPDATE OR DELETE ON rt.advanced_stream_authorizations
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_authorization_change();

CREATE FUNCTION rt.sync_advanced_authorization_status()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO rt.advanced_stream_authorization_status (
        authorization_id,status,valid_from,valid_through,revoked_at,updated_at
    ) VALUES (
        NEW.authorization_id,
        NEW.status,
        NEW.valid_from,
        NEW.valid_through,
        NEW.revoked_at,
        clock_timestamp()
    )
    ON CONFLICT (authorization_id) DO UPDATE SET
        status = EXCLUDED.status,
        valid_from = EXCLUDED.valid_from,
        valid_through = EXCLUDED.valid_through,
        revoked_at = EXCLUDED.revoked_at,
        updated_at = EXCLUDED.updated_at;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_stream_authorization_status
    AFTER INSERT OR UPDATE OF status ON rt.advanced_stream_authorizations
    FOR EACH ROW EXECUTE FUNCTION rt.sync_advanced_authorization_status();

CREATE FUNCTION rt.enforce_advanced_ingress_plan_change()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    authorization_row rt.advanced_stream_authorizations%ROWTYPE;
    resolved_manifest_id uuid;
    resolved_session_date date;
    resolved_stock_count integer;
    resolved_am_count bigint;
    resolved_profile_sha256 char(64);
    resolved_profile_status varchar(16);
    resolved_profile_expires_at timestamptz;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'advanced ingress plans are durable evidence';
    END IF;
    IF TG_OP = 'INSERT' THEN
        SELECT * INTO authorization_row
        FROM rt.advanced_stream_authorizations
        WHERE authorization_id = NEW.authorization_id;
        SELECT
            cohort.manifest_id,
            manifest.session_date,
            manifest.stock_count,
            COUNT(member.instrument_id) FILTER (WHERE member.channel = 'AM'),
            profile.profile_sha256,
            profile.status,
            profile.expires_at
        INTO
            resolved_manifest_id,
            resolved_session_date,
            resolved_stock_count,
            resolved_am_count,
            resolved_profile_sha256,
            resolved_profile_status,
            resolved_profile_expires_at
        FROM rt.cohorts AS cohort
        JOIN rt.subscription_manifests AS manifest USING(manifest_id)
        JOIN rt.capability_profiles AS profile USING(profile_id)
        LEFT JOIN rt.subscription_members AS member USING(manifest_id)
        WHERE cohort.cohort_id = NEW.cohort_id
        GROUP BY
            cohort.manifest_id,
            manifest.session_date,
            manifest.stock_count,
            profile.profile_sha256,
            profile.status,
            profile.expires_at;
        IF authorization_row.authorization_id IS NULL
           OR authorization_row.status <> 'READY'
           OR NEW.authorization_scope_sha256
                <> authorization_row.scope_sha256
           OR NEW.runtime_policy_sha256
                <> authorization_row.runtime_policy_sha256
           OR NEW.universe_target
                <> authorization_row.allowed_universe_targets[1]
           OR NEW.session_date NOT BETWEEN
                authorization_row.valid_from
                AND authorization_row.valid_through
           OR NEW.maximum_stream_sessions
                > authorization_row.maximum_daily_stream_sessions
           OR NEW.maximum_connection_attempts
                > authorization_row.maximum_daily_connection_attempts
           OR NEW.maximum_decoded_bytes
                > authorization_row.maximum_daily_decoded_bytes
           OR NEW.maximum_rest_repair_requests
                > authorization_row.maximum_daily_rest_repair_requests
           OR NEW.maximum_rest_repair_decoded_bytes
                > authorization_row.maximum_daily_rest_repair_decoded_bytes
           OR resolved_manifest_id IS DISTINCT FROM NEW.manifest_id
           OR resolved_session_date IS DISTINCT FROM NEW.session_date
           OR resolved_stock_count IS DISTINCT FROM NEW.universe_target
           OR resolved_am_count IS DISTINCT FROM NEW.universe_target
           OR resolved_profile_sha256 IS DISTINCT FROM
                NEW.runtime_profile_sha256
           OR resolved_profile_status IS DISTINCT FROM 'PASS'
           OR resolved_profile_expires_at < NEW.authorized_end_at
           OR (NEW.authorized_start_at AT TIME ZONE 'America/New_York')::date
                <> NEW.session_date
           OR (NEW.authorized_end_at AT TIME ZONE 'America/New_York')::date
                <> NEW.session_date
           OR NEW.authorized_end_at - NEW.authorized_start_at
                > interval '8 hours'
           OR NEW.status <> 'READY'
           OR NEW.activated_at IS NOT NULL
           OR NEW.completed_at IS NOT NULL THEN
            RAISE EXCEPTION
                'advanced ingress plan exceeds its exact authorization';
        END IF;
        RETURN NEW;
    END IF;
    IF (
        NEW.plan_id,
        NEW.plan_sha256,
        NEW.authorization_id,
        NEW.authorization_scope_sha256,
        NEW.cohort_id,
        NEW.manifest_id,
        NEW.session_date,
        NEW.universe_target,
        NEW.mode,
        NEW.runtime_profile_sha256,
        NEW.runtime_policy_sha256,
        NEW.authorized_start_at,
        NEW.authorized_end_at,
        NEW.maximum_stream_sessions,
        NEW.maximum_connection_attempts,
        NEW.maximum_decoded_bytes,
        NEW.maximum_rest_repair_requests,
        NEW.maximum_rest_repair_decoded_bytes,
        NEW.created_at
    ) IS DISTINCT FROM (
        OLD.plan_id,
        OLD.plan_sha256,
        OLD.authorization_id,
        OLD.authorization_scope_sha256,
        OLD.cohort_id,
        OLD.manifest_id,
        OLD.session_date,
        OLD.universe_target,
        OLD.mode,
        OLD.runtime_profile_sha256,
        OLD.runtime_policy_sha256,
        OLD.authorized_start_at,
        OLD.authorized_end_at,
        OLD.maximum_stream_sessions,
        OLD.maximum_connection_attempts,
        OLD.maximum_decoded_bytes,
        OLD.maximum_rest_repair_requests,
        OLD.maximum_rest_repair_decoded_bytes,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'advanced ingress plan identity is immutable';
    END IF;
    IF NEW.status = OLD.status THEN
        RETURN NEW;
    END IF;
    IF OLD.status = 'READY' AND NEW.status IN ('ACTIVE', 'BLOCKED', 'FAILED')
       OR OLD.status = 'ACTIVE'
          AND NEW.status IN ('COMPLETE', 'BLOCKED', 'FAILED') THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'invalid advanced ingress plan transition % -> %',
        OLD.status, NEW.status;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_ingress_plans_durable
    BEFORE INSERT OR UPDATE OR DELETE ON rt.advanced_ingress_plans
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_ingress_plan_change();

CREATE FUNCTION rt.enforce_advanced_ingress_usage_change()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    maximum_stream_sessions integer;
    maximum_connection_attempts integer;
    maximum_decoded_bytes bigint;
    maximum_rest_repair_requests integer;
    maximum_rest_repair_decoded_bytes bigint;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'advanced ingress usage is durable evidence';
    END IF;
    SELECT
        plan.maximum_stream_sessions,
        plan.maximum_connection_attempts,
        plan.maximum_decoded_bytes,
        plan.maximum_rest_repair_requests,
        plan.maximum_rest_repair_decoded_bytes
    INTO
        maximum_stream_sessions,
        maximum_connection_attempts,
        maximum_decoded_bytes,
        maximum_rest_repair_requests,
        maximum_rest_repair_decoded_bytes
    FROM rt.advanced_ingress_plans AS plan
    WHERE plan.plan_id = NEW.plan_id;
    IF TG_OP = 'INSERT' AND (
        NEW.stream_session_count <> 0
        OR NEW.connection_attempt_count <> 0
        OR NEW.decoded_byte_count <> 0
        OR NEW.rest_repair_request_count <> 0
        OR NEW.rest_repair_decoded_byte_count <> 0
        OR NEW.latest_owner_epoch <> 0
        OR NEW.latest_stream_session_id IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'advanced ingress usage must start at zero';
    END IF;
    IF TG_OP = 'UPDATE' AND (
        NEW.plan_id <> OLD.plan_id
        OR NEW.created_at <> OLD.created_at
        OR NEW.stream_session_count < OLD.stream_session_count
        OR NEW.connection_attempt_count < OLD.connection_attempt_count
        OR NEW.decoded_byte_count < OLD.decoded_byte_count
        OR NEW.rest_repair_request_count < OLD.rest_repair_request_count
        OR NEW.rest_repair_decoded_byte_count
            < OLD.rest_repair_decoded_byte_count
        OR NEW.latest_owner_epoch < OLD.latest_owner_epoch
        OR NEW.updated_at < OLD.updated_at
    ) THEN
        RAISE EXCEPTION 'advanced ingress usage cannot reset or change identity';
    END IF;
    IF NEW.stream_session_count > maximum_stream_sessions
       OR NEW.connection_attempt_count > maximum_connection_attempts
       OR NEW.decoded_byte_count > maximum_decoded_bytes
       OR NEW.rest_repair_request_count > maximum_rest_repair_requests
       OR NEW.rest_repair_decoded_byte_count
            > maximum_rest_repair_decoded_bytes THEN
        RAISE EXCEPTION 'advanced ingress usage exceeds its daily plan';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_ingress_plan_usage_monotonic
    BEFORE INSERT OR UPDATE OR DELETE ON rt.advanced_ingress_plan_usage
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_ingress_usage_change();
