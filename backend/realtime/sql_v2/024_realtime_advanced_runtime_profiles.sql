SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.advanced_ingress_runtime_profiles (
    runtime_profile_sha256 char(64) PRIMARY KEY,
    runtime_profile_version varchar(96) NOT NULL UNIQUE,
    universe_target integer NOT NULL,
    ladder_position integer NOT NULL,
    previous_universe_target integer,
    provider_tier varchar(32) NOT NULL,
    feed varchar(16) NOT NULL,
    channels text[] NOT NULL,
    profile_document jsonb NOT NULL,
    configuration_sha256 char(64) NOT NULL UNIQUE,
    runtime_policy_sha256 char(64) NOT NULL,
    status varchar(16) NOT NULL,
    stability_evidence_sha256 char(64),
    capacity_evidence_sha256 char(64),
    activation_approval_reference varchar(160),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    activated_at timestamptz,
    superseded_at timestamptz,
    CHECK (
        runtime_profile_sha256 ~ '^[0-9a-f]{64}$'
        AND configuration_sha256 ~ '^[0-9a-f]{64}$'
        AND runtime_policy_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CHECK (
        universe_target BETWEEN 1 AND 400
        AND ladder_position >= 0
        AND (
            (ladder_position = 0 AND previous_universe_target IS NULL)
            OR
            (
                ladder_position > 0
                AND previous_universe_target BETWEEN 1 AND 400
                AND previous_universe_target < universe_target
            )
        )
        AND provider_tier = 'MASSIVE_ADVANCED'
        AND feed = 'STOCKS'
        AND channels = ARRAY['AM']::text[]
        AND jsonb_typeof(profile_document) = 'object'
    ),
    CHECK (status IN ('CANDIDATE', 'ACTIVE', 'SUPERSEDED')),
    CHECK (
        (
            status = 'CANDIDATE'
            AND stability_evidence_sha256 IS NULL
            AND capacity_evidence_sha256 IS NULL
            AND activation_approval_reference IS NULL
            AND activated_at IS NULL
            AND superseded_at IS NULL
        )
        OR
        (
            status = 'ACTIVE'
            AND stability_evidence_sha256 ~ '^[0-9a-f]{64}$'
            AND capacity_evidence_sha256 ~ '^[0-9a-f]{64}$'
            AND btrim(activation_approval_reference) <> ''
            AND activated_at IS NOT NULL
            AND superseded_at IS NULL
        )
        OR
        (
            status = 'SUPERSEDED'
            AND stability_evidence_sha256 ~ '^[0-9a-f]{64}$'
            AND capacity_evidence_sha256 ~ '^[0-9a-f]{64}$'
            AND btrim(activation_approval_reference) <> ''
            AND activated_at IS NOT NULL
            AND superseded_at IS NOT NULL
            AND superseded_at >= activated_at
        )
    )
);

CREATE UNIQUE INDEX uq_rt_advanced_ingress_active_profile_target
    ON rt.advanced_ingress_runtime_profiles (universe_target)
    WHERE status = 'ACTIVE';

CREATE TABLE rt.advanced_ingress_cohort_bindings (
    binding_id uuid PRIMARY KEY,
    session_date date NOT NULL,
    universe_target integer NOT NULL,
    runtime_profile_sha256 char(64) NOT NULL REFERENCES
        rt.advanced_ingress_runtime_profiles(runtime_profile_sha256),
    configuration_sha256 char(64) NOT NULL,
    runtime_policy_sha256 char(64) NOT NULL,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    manifest_id uuid NOT NULL REFERENCES
        rt.subscription_manifests(manifest_id),
    approval_reference varchar(160) NOT NULL,
    status varchar(16) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    superseded_at timestamptz,
    CHECK (universe_target BETWEEN 1 AND 400),
    CHECK (
        runtime_profile_sha256 ~ '^[0-9a-f]{64}$'
        AND configuration_sha256 ~ '^[0-9a-f]{64}$'
        AND runtime_policy_sha256 ~ '^[0-9a-f]{64}$'
        AND btrim(approval_reference) <> ''
    ),
    CHECK (status IN ('ACTIVE', 'SUPERSEDED')),
    CHECK (
        (status = 'ACTIVE' AND superseded_at IS NULL)
        OR
        (
            status = 'SUPERSEDED'
            AND superseded_at IS NOT NULL
            AND superseded_at >= created_at
        )
    )
);

CREATE UNIQUE INDEX uq_rt_advanced_ingress_active_cohort_binding
    ON rt.advanced_ingress_cohort_bindings (
        session_date,
        universe_target,
        runtime_profile_sha256,
        runtime_policy_sha256
    )
    WHERE status = 'ACTIVE';

ALTER TABLE rt.advanced_ingress_plans
    ADD COLUMN runtime_profile_version varchar(96),
    ADD COLUMN configuration_sha256 char(64);

DROP TRIGGER trg_rt_advanced_ingress_plans_durable
    ON rt.advanced_ingress_plans;

CREATE OR REPLACE FUNCTION rt.enforce_advanced_ingress_plan_change()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'advanced ingress plans are durable evidence';
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
        NEW.runtime_profile_version,
        NEW.runtime_profile_sha256,
        NEW.configuration_sha256,
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
        OLD.runtime_profile_version,
        OLD.runtime_profile_sha256,
        OLD.configuration_sha256,
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
    BEFORE UPDATE OR DELETE ON rt.advanced_ingress_plans
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_ingress_plan_change();

CREATE FUNCTION rt.enforce_advanced_runtime_profile_change()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'advanced runtime profiles are durable evidence';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'CANDIDATE' THEN
            RAISE EXCEPTION 'new advanced runtime profiles start as CANDIDATE';
        END IF;
        RETURN NEW;
    END IF;
    IF (
        NEW.runtime_profile_sha256,
        NEW.runtime_profile_version,
        NEW.universe_target,
        NEW.ladder_position,
        NEW.previous_universe_target,
        NEW.provider_tier,
        NEW.feed,
        NEW.channels,
        NEW.profile_document,
        NEW.configuration_sha256,
        NEW.runtime_policy_sha256,
        NEW.created_at
    ) IS DISTINCT FROM (
        OLD.runtime_profile_sha256,
        OLD.runtime_profile_version,
        OLD.universe_target,
        OLD.ladder_position,
        OLD.previous_universe_target,
        OLD.provider_tier,
        OLD.feed,
        OLD.channels,
        OLD.profile_document,
        OLD.configuration_sha256,
        OLD.runtime_policy_sha256,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'advanced runtime profile identity is immutable';
    END IF;
    IF NEW.status = OLD.status THEN
        IF (
            NEW.stability_evidence_sha256,
            NEW.capacity_evidence_sha256,
            NEW.activation_approval_reference,
            NEW.activated_at,
            NEW.superseded_at
        ) IS DISTINCT FROM (
            OLD.stability_evidence_sha256,
            OLD.capacity_evidence_sha256,
            OLD.activation_approval_reference,
            OLD.activated_at,
            OLD.superseded_at
        ) THEN
            RAISE EXCEPTION 'advanced profile evidence is immutable';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.status = 'CANDIDATE' AND NEW.status = 'ACTIVE' THEN
        RETURN NEW;
    END IF;
    IF OLD.status = 'ACTIVE' AND NEW.status = 'SUPERSEDED' THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'invalid advanced runtime profile transition % -> %',
        OLD.status, NEW.status;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_ingress_runtime_profiles_durable
    BEFORE INSERT OR UPDATE OR DELETE
    ON rt.advanced_ingress_runtime_profiles
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_runtime_profile_change();

CREATE FUNCTION rt.enforce_advanced_authorization_promotion()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    profile rt.advanced_ingress_runtime_profiles%ROWTYPE;
    parent rt.advanced_stream_authorizations%ROWTYPE;
BEGIN
    SELECT * INTO profile
    FROM rt.advanced_ingress_runtime_profiles
    WHERE universe_target = NEW.allowed_universe_targets[1]
      AND runtime_policy_sha256 = NEW.runtime_policy_sha256
      AND status = 'ACTIVE';
    IF profile.runtime_profile_sha256 IS NULL THEN
        RAISE EXCEPTION
            'standing authorization requires an ACTIVE runtime profile';
    END IF;
    IF profile.ladder_position = 0 THEN
        IF NEW.parent_authorization_id IS NOT NULL THEN
            RAISE EXCEPTION
                'initial ladder authorization cannot have a parent';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.parent_authorization_id IS NULL THEN
        RAISE EXCEPTION 'promotion requires a parent authorization';
    END IF;
    SELECT * INTO parent
    FROM rt.advanced_stream_authorizations
    WHERE authorization_id = NEW.parent_authorization_id;
    IF parent.authorization_id IS NULL
       OR parent.status <> 'READY'
       OR parent.allowed_universe_targets[1]
            <> profile.previous_universe_target
       OR parent.runtime_policy_sha256 <> NEW.runtime_policy_sha256
       OR NEW.valid_from <= parent.valid_through THEN
        RAISE EXCEPTION
            'promotion must follow one ladder rung at the next session';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_stream_authorizations_promotion
    BEFORE INSERT ON rt.advanced_stream_authorizations
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_authorization_promotion();

CREATE FUNCTION rt.enforce_advanced_cohort_binding_change()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    profile rt.advanced_ingress_runtime_profiles%ROWTYPE;
    resolved_manifest_id uuid;
    resolved_session_date date;
    resolved_stock_count integer;
    resolved_am_count bigint;
    resolved_provider_tier varchar(32);
    resolved_profile_status varchar(16);
    resolved_profile_expires_at timestamptz;
    resolved_profile_config_sha256 char(64);
    resolved_cohort_config_sha256 char(64);
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'advanced cohort bindings are durable evidence';
    END IF;
    IF TG_OP = 'INSERT' THEN
        SELECT * INTO profile
        FROM rt.advanced_ingress_runtime_profiles
        WHERE runtime_profile_sha256 = NEW.runtime_profile_sha256;
        SELECT
            cohort.manifest_id,
            manifest.session_date,
            manifest.stock_count,
            COUNT(member.instrument_id) FILTER (WHERE member.channel = 'AM'),
            capability.provider_tier,
            capability.status,
            capability.expires_at,
            capability.config_sha256,
            cohort.configuration_sha256
        INTO
            resolved_manifest_id,
            resolved_session_date,
            resolved_stock_count,
            resolved_am_count,
            resolved_provider_tier,
            resolved_profile_status,
            resolved_profile_expires_at,
            resolved_profile_config_sha256,
            resolved_cohort_config_sha256
        FROM rt.cohorts AS cohort
        JOIN rt.subscription_manifests AS manifest USING(manifest_id)
        JOIN rt.capability_profiles AS capability USING(profile_id)
        LEFT JOIN rt.subscription_members AS member USING(manifest_id)
        WHERE cohort.cohort_id = NEW.cohort_id
        GROUP BY
            cohort.manifest_id,
            manifest.session_date,
            manifest.stock_count,
            capability.provider_tier,
            capability.status,
            capability.expires_at,
            capability.config_sha256,
            cohort.configuration_sha256;
        IF profile.runtime_profile_sha256 IS NULL
           OR profile.status <> 'ACTIVE'
           OR profile.universe_target <> NEW.universe_target
           OR profile.configuration_sha256 <> NEW.configuration_sha256
           OR profile.runtime_policy_sha256 <> NEW.runtime_policy_sha256
           OR resolved_manifest_id IS DISTINCT FROM NEW.manifest_id
           OR resolved_session_date IS DISTINCT FROM NEW.session_date
           OR resolved_stock_count IS DISTINCT FROM NEW.universe_target
           OR resolved_am_count IS DISTINCT FROM NEW.universe_target
           OR resolved_provider_tier IS DISTINCT FROM 'MASSIVE_ADVANCED'
           OR resolved_profile_status IS DISTINCT FROM 'PASS'
           OR resolved_profile_expires_at <= clock_timestamp()
           OR resolved_profile_config_sha256 IS DISTINCT FROM
                NEW.configuration_sha256
           OR resolved_cohort_config_sha256 IS DISTINCT FROM
                NEW.configuration_sha256
           OR NEW.status <> 'ACTIVE'
           OR NEW.superseded_at IS NOT NULL THEN
            RAISE EXCEPTION
                'advanced cohort binding does not match active exact scope';
        END IF;
        RETURN NEW;
    END IF;
    IF (
        NEW.binding_id,
        NEW.session_date,
        NEW.universe_target,
        NEW.runtime_profile_sha256,
        NEW.configuration_sha256,
        NEW.runtime_policy_sha256,
        NEW.cohort_id,
        NEW.manifest_id,
        NEW.approval_reference,
        NEW.created_at
    ) IS DISTINCT FROM (
        OLD.binding_id,
        OLD.session_date,
        OLD.universe_target,
        OLD.runtime_profile_sha256,
        OLD.configuration_sha256,
        OLD.runtime_policy_sha256,
        OLD.cohort_id,
        OLD.manifest_id,
        OLD.approval_reference,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'advanced cohort binding identity is immutable';
    END IF;
    IF OLD.status = 'ACTIVE' AND NEW.status = 'SUPERSEDED' THEN
        RETURN NEW;
    END IF;
    IF NEW.status = OLD.status AND NEW.superseded_at = OLD.superseded_at THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'invalid advanced cohort binding transition % -> %',
        OLD.status, NEW.status;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_ingress_cohort_bindings_durable
    BEFORE INSERT OR UPDATE OR DELETE ON rt.advanced_ingress_cohort_bindings
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_cohort_binding_change();

CREATE FUNCTION rt.enforce_advanced_ingress_plan_profile()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    profile rt.advanced_ingress_runtime_profiles%ROWTYPE;
    binding_count integer;
    bound_cohort_ids uuid[];
    bound_manifest_ids uuid[];
    candidate_count integer;
    candidate_cohort_ids uuid[];
    candidate_manifest_ids uuid[];
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF (
            NEW.runtime_profile_version,
            NEW.configuration_sha256
        ) IS DISTINCT FROM (
            OLD.runtime_profile_version,
            OLD.configuration_sha256
        ) THEN
            RAISE EXCEPTION 'advanced ingress runtime profile is immutable';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.runtime_profile_version IS NULL
       OR NEW.configuration_sha256 IS NULL THEN
        RAISE EXCEPTION 'advanced ingress plan requires a runtime profile';
    END IF;
    SELECT * INTO profile
    FROM rt.advanced_ingress_runtime_profiles
    WHERE runtime_profile_sha256 = NEW.runtime_profile_sha256;
    IF profile.runtime_profile_sha256 IS NULL
       OR profile.status <> 'ACTIVE'
       OR profile.runtime_profile_version <> NEW.runtime_profile_version
       OR profile.universe_target <> NEW.universe_target
       OR profile.configuration_sha256 <> NEW.configuration_sha256
       OR profile.runtime_policy_sha256 <> NEW.runtime_policy_sha256 THEN
        RAISE EXCEPTION
            'advanced ingress plan runtime profile is missing or inactive';
    END IF;
    SELECT
        COUNT(*),
        array_agg(binding.cohort_id),
        array_agg(binding.manifest_id)
    INTO binding_count, bound_cohort_ids, bound_manifest_ids
    FROM rt.advanced_ingress_cohort_bindings AS binding
    WHERE binding.session_date = NEW.session_date
      AND binding.universe_target = NEW.universe_target
      AND binding.runtime_profile_sha256 = NEW.runtime_profile_sha256
      AND binding.configuration_sha256 = NEW.configuration_sha256
      AND binding.runtime_policy_sha256 = NEW.runtime_policy_sha256
      AND binding.status = 'ACTIVE';
    SELECT
        COUNT(*),
        array_agg(candidate.cohort_id),
        array_agg(candidate.manifest_id)
    INTO candidate_count, candidate_cohort_ids, candidate_manifest_ids
    FROM (
        SELECT cohort.cohort_id, cohort.manifest_id
        FROM rt.cohorts AS cohort
        JOIN rt.subscription_manifests AS manifest USING(manifest_id)
        JOIN rt.capability_profiles AS capability USING(profile_id)
        LEFT JOIN rt.subscription_members AS member USING(manifest_id)
        WHERE manifest.session_date = NEW.session_date
          AND manifest.stock_count = NEW.universe_target
          AND capability.provider_tier = 'MASSIVE_ADVANCED'
          AND capability.status = 'PASS'
          AND capability.expires_at >= NEW.authorized_end_at
          AND capability.config_sha256 = NEW.configuration_sha256
          AND cohort.configuration_sha256 = NEW.configuration_sha256
        GROUP BY cohort.cohort_id, cohort.manifest_id, manifest.member_count
        HAVING
            COUNT(member.instrument_id) = NEW.universe_target
            AND COUNT(member.instrument_id) FILTER (
                WHERE member.channel = 'AM'
            ) = NEW.universe_target
    ) AS candidate;
    IF candidate_count = 0 THEN
        RAISE EXCEPTION 'COHORT_NOT_PROVISIONED';
    END IF;
    IF binding_count = 1 THEN
        IF NEW.cohort_id <> bound_cohort_ids[1]
           OR NEW.manifest_id <> bound_manifest_ids[1] THEN
            RAISE EXCEPTION 'advanced ingress plan differs from admin binding';
        END IF;
    ELSIF binding_count > 1 OR candidate_count > 1 THEN
        RAISE EXCEPTION 'COHORT_AMBIGUOUS';
    ELSIF NEW.cohort_id <> candidate_cohort_ids[1]
       OR NEW.manifest_id <> candidate_manifest_ids[1] THEN
        RAISE EXCEPTION 'advanced ingress plan cohort identity changed';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_ingress_plans_exact_profile
    BEFORE INSERT OR UPDATE ON rt.advanced_ingress_plans
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_ingress_plan_profile();
