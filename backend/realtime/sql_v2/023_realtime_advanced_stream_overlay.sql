SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

-- The only Advanced-specific control-plane relations. Downstream execution,
-- repair, backfill, release and alert state stays in the canonical V2 tables.

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
        AND btrim(authorization_reference) <> ''
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

CREATE VIEW rt.advanced_stream_authorization_status AS
SELECT
    authorization_id,
    status,
    valid_from,
    valid_through,
    revoked_at,
    COALESCE(revoked_at, created_at) AS updated_at
FROM rt.advanced_stream_authorizations;

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

CREATE TABLE rt.advanced_ingress_plans (
    plan_id uuid PRIMARY KEY,
    plan_sha256 char(64) NOT NULL UNIQUE,
    authorization_id uuid NOT NULL REFERENCES
        rt.advanced_stream_authorizations(authorization_id),
    authorization_scope_sha256 char(64) NOT NULL,
    service_plan_id uuid REFERENCES rt.service_plans(plan_id),
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    manifest_id uuid NOT NULL REFERENCES
        rt.subscription_manifests(manifest_id),
    cohort_selection_reference varchar(160),
    session_date date NOT NULL,
    universe_target integer NOT NULL,
    mode varchar(16) NOT NULL,
    runtime_profile_version varchar(96) NOT NULL,
    runtime_profile_sha256 char(64) NOT NULL REFERENCES
        rt.advanced_ingress_runtime_profiles(runtime_profile_sha256),
    configuration_sha256 char(64) NOT NULL,
    runtime_policy_sha256 char(64) NOT NULL,
    authorized_start_at timestamptz NOT NULL,
    authorized_end_at timestamptz NOT NULL,
    maximum_stream_sessions integer NOT NULL,
    maximum_connection_attempts integer NOT NULL,
    maximum_decoded_bytes bigint NOT NULL,
    maximum_rest_repair_requests integer NOT NULL,
    maximum_rest_repair_decoded_bytes bigint NOT NULL,
    status varchar(16) NOT NULL,
    terminal_outcome varchar(40),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    activated_at timestamptz,
    completed_at timestamptz,
    UNIQUE (authorization_id, session_date),
    CHECK (
        plan_sha256 ~ '^[0-9a-f]{64}$'
        AND authorization_scope_sha256 ~ '^[0-9a-f]{64}$'
        AND runtime_profile_sha256 ~ '^[0-9a-f]{64}$'
        AND configuration_sha256 ~ '^[0-9a-f]{64}$'
        AND runtime_policy_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CHECK (
        cohort_selection_reference IS NULL
        OR btrim(cohort_selection_reference) <> ''
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
    CHECK (
        terminal_outcome IS NULL
        OR terminal_outcome IN (
            'SESSION_COMPLETE',
            'AUTHORIZATION_EXHAUSTED',
            'BLOCKED_CONFIGURATION',
            'BLOCKED_ENTITLEMENT',
            'BLOCKED_GAP',
            'FAILED_INFRASTRUCTURE'
        )
    ),
    CHECK (
        (
            status IN ('READY', 'ACTIVE')
            AND terminal_outcome IS NULL
            AND completed_at IS NULL
        )
        OR
        (
            status IN ('COMPLETE', 'BLOCKED', 'FAILED')
            AND terminal_outcome IS NOT NULL
            AND completed_at IS NOT NULL
        )
    ),
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

CREATE TABLE rt.advanced_journal_registry (
    journal_generation uuid PRIMARY KEY,
    plan_id uuid NOT NULL REFERENCES rt.advanced_ingress_plans(plan_id),
    stream_session_id uuid NOT NULL UNIQUE REFERENCES
        rt.stream_sessions(session_id),
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    manifest_id uuid NOT NULL REFERENCES
        rt.subscription_manifests(manifest_id),
    session_date date NOT NULL,
    owner_epoch bigint NOT NULL,
    relative_path varchar(320) NOT NULL UNIQUE,
    path_sha256 char(64) NOT NULL,
    metadata_sha256 char(64) NOT NULL UNIQUE,
    journal_codec varchar(96) NOT NULL,
    journal_schema_version integer NOT NULL,
    normalizer_version varchar(96) NOT NULL,
    shard_ids smallint[] NOT NULL,
    shard_map_sha256 char(64) NOT NULL,
    status varchar(16) NOT NULL,
    raw_committed_offset bigint NOT NULL DEFAULT 0,
    normalized_committed_offset bigint NOT NULL DEFAULT 0,
    bridge_committed_offset bigint NOT NULL DEFAULT 0,
    sealed_files_sha256 char(64),
    quarantine_reason varchar(64),
    quarantine_detail varchar(320),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    sealed_at timestamptz,
    drained_at timestamptz,
    quarantined_at timestamptz,
    CHECK (
        owner_epoch > 0
        AND journal_schema_version > 0
        AND cardinality(shard_ids) > 0
        AND raw_committed_offset >= 0
        AND normalized_committed_offset >= 0
        AND bridge_committed_offset >= 0
        AND bridge_committed_offset <= normalized_committed_offset
    ),
    CHECK (
        path_sha256 ~ '^[0-9a-f]{64}$'
        AND metadata_sha256 ~ '^[0-9a-f]{64}$'
        AND shard_map_sha256 ~ '^[0-9a-f]{64}$'
        AND (
            sealed_files_sha256 IS NULL
            OR sealed_files_sha256 ~ '^[0-9a-f]{64}$'
        )
    ),
    CHECK (
        btrim(relative_path) <> ''
        AND left(relative_path, 1) NOT IN ('/', '\')
        AND relative_path !~ '(^|[\\/])\.\.([\\/]|$)'
        AND btrim(journal_codec) <> ''
        AND btrim(normalizer_version) <> ''
    ),
    CHECK (
        status IN (
            'REGISTERED',
            'WRITING',
            'DRAINING',
            'DRAINED',
            'QUARANTINED'
        )
    )
);

CREATE INDEX idx_rt_advanced_journal_discovery
    ON rt.advanced_journal_registry (
        status,
        session_date,
        created_at,
        journal_generation
    );

ALTER TABLE rt.stream_sessions
    ADD COLUMN advanced_ingress_plan_id uuid REFERENCES
        rt.advanced_ingress_plans(plan_id);

CREATE INDEX idx_rt_stream_sessions_advanced_ingress_plan
    ON rt.stream_sessions (advanced_ingress_plan_id, owner_epoch)
    WHERE advanced_ingress_plan_id IS NOT NULL;

ALTER TABLE rt.stream_gaps
    ADD COLUMN plan_id uuid REFERENCES rt.advanced_ingress_plans(plan_id),
    ADD COLUMN last_durable_watermark timestamptz,
    ADD COLUMN reconnect_at timestamptz,
    ADD COLUMN first_post_reconnect_event_at timestamptz,
    ADD COLUMN first_post_reconnect_offset bigint,
    ADD COLUMN continuity_class varchar(40),
    ADD COLUMN classification_sha256 char(64),
    ADD COLUMN repair_id uuid UNIQUE,
    ADD COLUMN repair_scope_sha256 char(64),
    ADD COLUMN repair_status varchar(16),
    ADD COLUMN repair_reserved_response_bytes bigint,
    ADD COLUMN canonical_coverage_sha256 char(64),
    ADD COLUMN operator_alert_required boolean NOT NULL DEFAULT false,
    ADD CONSTRAINT stream_gaps_repair_request_fkey
        FOREIGN KEY (repair_reference)
        REFERENCES rt.provider_request_ledger(request_id),
    ADD CONSTRAINT stream_gaps_reconnect_evidence_check CHECK (
        reconnect_at IS NULL OR reconnect_at >= gap_start
    ),
    ADD CONSTRAINT stream_gaps_first_post_event_check CHECK (
        (
            first_post_reconnect_event_at IS NULL
            AND first_post_reconnect_offset IS NULL
        )
        OR
        (
            reconnect_at IS NOT NULL
            AND first_post_reconnect_event_at IS NOT NULL
            AND first_post_reconnect_offset >= 0
        )
    ),
    ADD CONSTRAINT stream_gaps_continuity_class_check CHECK (
        continuity_class IS NULL
        OR continuity_class IN (
            'LOCAL_OFFSET_DISCONTINUITY',
            'PROVIDER_TIME_OUTAGE',
            'NO_TRADE_INTERVAL',
            'UNKNOWN_CONTINUITY'
        )
    ),
    ADD CONSTRAINT stream_gaps_repair_state_check CHECK (
        repair_status IS NULL
        OR repair_status IN (
            'PLANNED',
            'RESERVED',
            'SEALED',
            'REPLAYED',
            'VERIFIED',
            'REPAIRED',
            'BLOCKED'
        )
    ),
    ADD CONSTRAINT stream_gaps_repair_hash_check CHECK (
        (
            repair_scope_sha256 IS NULL
            AND canonical_coverage_sha256 IS NULL
        )
        OR
        (
            repair_scope_sha256 ~ '^[0-9a-f]{64}$'
            AND (
                canonical_coverage_sha256 IS NULL
                OR canonical_coverage_sha256 ~ '^[0-9a-f]{64}$'
            )
        )
    );

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
    IF OLD.status = 'READY' AND NEW.status IN ('REVOKED', 'EXPIRED') THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'invalid advanced authorization transition % -> %',
        OLD.status, NEW.status;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_stream_authorizations_durable
    BEFORE UPDATE OR DELETE ON rt.advanced_stream_authorizations
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_authorization_change();

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
        RETURN NEW;
    END IF;
    IF OLD.status = 'CANDIDATE' AND NEW.status = 'ACTIVE'
       OR OLD.status = 'ACTIVE' AND NEW.status = 'SUPERSEDED' THEN
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
    profile_row rt.advanced_ingress_runtime_profiles%ROWTYPE;
    parent_row rt.advanced_stream_authorizations%ROWTYPE;
BEGIN
    SELECT * INTO profile_row
    FROM rt.advanced_ingress_runtime_profiles
    WHERE universe_target = NEW.allowed_universe_targets[1]
      AND runtime_policy_sha256 = NEW.runtime_policy_sha256
      AND status = 'ACTIVE';
    IF profile_row.runtime_profile_sha256 IS NULL THEN
        RAISE EXCEPTION
            'standing authorization requires an ACTIVE runtime profile';
    END IF;
    IF profile_row.ladder_position = 0 THEN
        IF NEW.parent_authorization_id IS NOT NULL THEN
            RAISE EXCEPTION
                'initial ladder authorization cannot have a parent';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.parent_authorization_id IS NULL THEN
        RAISE EXCEPTION 'promotion requires a parent authorization';
    END IF;
    SELECT * INTO parent_row
    FROM rt.advanced_stream_authorizations
    WHERE authorization_id = NEW.parent_authorization_id;
    IF parent_row.authorization_id IS NULL
       OR parent_row.status <> 'READY'
       OR parent_row.allowed_universe_targets[1]
            <> profile_row.previous_universe_target
       OR parent_row.runtime_policy_sha256 <> NEW.runtime_policy_sha256
       OR NEW.valid_from <= parent_row.valid_through THEN
        RAISE EXCEPTION
            'promotion must follow one ladder rung at the next session';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_stream_authorizations_promotion
    BEFORE INSERT ON rt.advanced_stream_authorizations
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_authorization_promotion();

CREATE FUNCTION rt.enforce_advanced_ingress_plan_change()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    auth_row rt.advanced_stream_authorizations%ROWTYPE;
    profile_row rt.advanced_ingress_runtime_profiles%ROWTYPE;
    selected_matches integer;
    candidate_count integer;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'advanced ingress plans are durable evidence';
    END IF;
    IF TG_OP = 'INSERT' THEN
        SELECT * INTO auth_row
        FROM rt.advanced_stream_authorizations
        WHERE authorization_id = NEW.authorization_id;
        SELECT * INTO profile_row
        FROM rt.advanced_ingress_runtime_profiles
        WHERE runtime_profile_sha256 = NEW.runtime_profile_sha256;
        SELECT COUNT(*) INTO selected_matches
        FROM (
            SELECT cohort.cohort_id
            FROM rt.cohorts AS cohort
            JOIN rt.subscription_manifests AS manifest USING(manifest_id)
            JOIN rt.capability_profiles AS capability USING(profile_id)
            LEFT JOIN rt.subscription_members AS member USING(manifest_id)
            WHERE cohort.cohort_id = NEW.cohort_id
              AND cohort.manifest_id = NEW.manifest_id
              AND manifest.session_date = NEW.session_date
              AND manifest.stock_count = NEW.universe_target
              AND capability.provider_tier = 'MASSIVE_ADVANCED'
              AND capability.status = 'PASS'
              AND capability.expires_at >= NEW.authorized_end_at
              AND capability.config_sha256 = NEW.configuration_sha256
              AND cohort.configuration_sha256 = NEW.configuration_sha256
            GROUP BY cohort.cohort_id
            HAVING
                COUNT(member.instrument_id) = NEW.universe_target
                AND COUNT(member.instrument_id) FILTER (
                    WHERE member.channel = 'AM'
                ) = NEW.universe_target
        ) AS selected;
        SELECT COUNT(*) INTO candidate_count
        FROM (
            SELECT cohort.cohort_id
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
            GROUP BY cohort.cohort_id
            HAVING
                COUNT(member.instrument_id) = NEW.universe_target
                AND COUNT(member.instrument_id) FILTER (
                    WHERE member.channel = 'AM'
                ) = NEW.universe_target
        ) AS candidate;
        IF auth_row.authorization_id IS NULL
           OR auth_row.status <> 'READY'
           OR NEW.session_date NOT BETWEEN
                auth_row.valid_from AND auth_row.valid_through
           OR NEW.authorization_scope_sha256 <> auth_row.scope_sha256
           OR NEW.runtime_policy_sha256 <> auth_row.runtime_policy_sha256
           OR NEW.universe_target <> auth_row.allowed_universe_targets[1]
           OR NEW.maximum_stream_sessions
                > auth_row.maximum_daily_stream_sessions
           OR NEW.maximum_connection_attempts
                > auth_row.maximum_daily_connection_attempts
           OR NEW.maximum_decoded_bytes
                > auth_row.maximum_daily_decoded_bytes
           OR NEW.maximum_rest_repair_requests
                > auth_row.maximum_daily_rest_repair_requests
           OR NEW.maximum_rest_repair_decoded_bytes
                > auth_row.maximum_daily_rest_repair_decoded_bytes
           OR profile_row.runtime_profile_sha256 IS NULL
           OR profile_row.status <> 'ACTIVE'
           OR profile_row.runtime_profile_version
                <> NEW.runtime_profile_version
           OR profile_row.universe_target <> NEW.universe_target
           OR profile_row.configuration_sha256 <> NEW.configuration_sha256
           OR profile_row.runtime_policy_sha256 <> NEW.runtime_policy_sha256
           OR selected_matches <> 1
           OR candidate_count = 0
           OR (
                candidate_count > 1
                AND NEW.cohort_selection_reference IS NULL
           )
           OR NEW.status <> 'READY'
           OR NEW.terminal_outcome IS NOT NULL
           OR NEW.activated_at IS NOT NULL
           OR NEW.completed_at IS NOT NULL THEN
            RAISE EXCEPTION
                'advanced ingress plan exceeds or ambiguously resolves scope';
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
        NEW.cohort_selection_reference,
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
        OLD.cohort_selection_reference,
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
    IF OLD.terminal_outcome IS NOT NULL
       AND (NEW.terminal_outcome,NEW.completed_at) IS DISTINCT FROM
           (OLD.terminal_outcome,OLD.completed_at) THEN
        RAISE EXCEPTION 'advanced ingress terminal evidence is immutable';
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
    limits rt.advanced_ingress_plans%ROWTYPE;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'advanced ingress usage is durable evidence';
    END IF;
    SELECT * INTO limits FROM rt.advanced_ingress_plans
    WHERE plan_id = NEW.plan_id;
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
        RAISE EXCEPTION 'advanced ingress usage cannot reset';
    END IF;
    IF NEW.stream_session_count > limits.maximum_stream_sessions
       OR NEW.connection_attempt_count > limits.maximum_connection_attempts
       OR NEW.decoded_byte_count > limits.maximum_decoded_bytes
       OR NEW.rest_repair_request_count
            > limits.maximum_rest_repair_requests
       OR NEW.rest_repair_decoded_byte_count
            > limits.maximum_rest_repair_decoded_bytes THEN
        RAISE EXCEPTION 'advanced ingress usage exceeds its plan';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_ingress_plan_usage_monotonic
    BEFORE INSERT OR UPDATE OR DELETE ON rt.advanced_ingress_plan_usage
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_ingress_usage_change();

CREATE FUNCTION rt.enforce_advanced_journal_registry_change()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    old_rank integer;
    new_rank integer;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'advanced journal registry is durable evidence';
    END IF;
    IF (
        NEW.journal_generation,
        NEW.plan_id,
        NEW.stream_session_id,
        NEW.cohort_id,
        NEW.manifest_id,
        NEW.session_date,
        NEW.owner_epoch,
        NEW.relative_path,
        NEW.path_sha256,
        NEW.metadata_sha256,
        NEW.journal_codec,
        NEW.journal_schema_version,
        NEW.normalizer_version,
        NEW.shard_ids,
        NEW.shard_map_sha256,
        NEW.created_at
    ) IS DISTINCT FROM (
        OLD.journal_generation,
        OLD.plan_id,
        OLD.stream_session_id,
        OLD.cohort_id,
        OLD.manifest_id,
        OLD.session_date,
        OLD.owner_epoch,
        OLD.relative_path,
        OLD.path_sha256,
        OLD.metadata_sha256,
        OLD.journal_codec,
        OLD.journal_schema_version,
        OLD.normalizer_version,
        OLD.shard_ids,
        OLD.shard_map_sha256,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'advanced journal registry identity is immutable';
    END IF;
    IF (
        NEW.raw_committed_offset < OLD.raw_committed_offset
        OR NEW.normalized_committed_offset
            < OLD.normalized_committed_offset
        OR NEW.bridge_committed_offset < OLD.bridge_committed_offset
        OR NEW.updated_at < OLD.updated_at
    ) THEN
        RAISE EXCEPTION 'advanced journal offsets cannot reset';
    END IF;
    old_rank = CASE OLD.status
        WHEN 'REGISTERED' THEN 1
        WHEN 'WRITING' THEN 2
        WHEN 'DRAINING' THEN 3
        WHEN 'DRAINED' THEN 4
        WHEN 'QUARANTINED' THEN 5
    END;
    new_rank = CASE NEW.status
        WHEN 'REGISTERED' THEN 1
        WHEN 'WRITING' THEN 2
        WHEN 'DRAINING' THEN 3
        WHEN 'DRAINED' THEN 4
        WHEN 'QUARANTINED' THEN 5
    END;
    IF NEW.status <> 'QUARANTINED' AND new_rank < old_rank THEN
        RAISE EXCEPTION 'advanced journal status cannot move backward';
    END IF;
    IF OLD.status IN ('DRAINED', 'QUARANTINED')
       AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'terminal advanced journal evidence is immutable';
    END IF;
    RETURN NEW;
END;
$function$;

CREATE TRIGGER trg_rt_advanced_journal_registry_durable
    BEFORE UPDATE OR DELETE ON rt.advanced_journal_registry
    FOR EACH ROW EXECUTE FUNCTION rt.enforce_advanced_journal_registry_change();
