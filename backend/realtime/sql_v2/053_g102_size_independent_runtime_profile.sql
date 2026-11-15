-- G102 R62 (Nov 15 2026, docs/g102/G102_UNIVERSE_SIZING_20261114.md section 1): universe size without per-size
-- runtime profiles.
--
-- The seven reviewed ladder profiles (1, 9, 25, 50, 100, 200, 400) carry identical settings, and the controller
-- satisfied the promotion ladder every morning with back-dated READY approvals of its own (1 -> 9 -> 25 -> N on the
-- previous sessions). The rows guarded nothing, yet every new size needed an exact-size profile row that the app role
-- cannot write: on Nov 13 the 45-name activation crash-looped the controller until the row was registered as admin.
--
-- 1. A stream plan may pin an ACTIVE profile that covers its size (profile size >= plan size) instead of one of
--    exactly its size. Every other plan rule is unchanged.
-- 2. A standing authorization needs an ACTIVE product profile covering its size; the previous-size parent chain is
--    no longer required. A supplied parent (the pre-053 controller writes one) must still be READY, the same product
--    and policy, and end before the child starts.
-- 3. The size-independent STOCKS AM profile `advanced-equity-ingress-v6` (the same 38 settings, size 5000, ladder
--    position 0) is registered ACTIVE here, so every database (live and freshly provisioned) has it without a separate
--    admin step. Its literals are generated from realtime/policies/advanced_equity_ingress_v1.json and checked by
--    tests/realtime/test_advanced_ingress_size_independent_profile.py.
--
-- Backward compatible: the pre-053 code still works (its exact-size rows and parent chains satisfy the relaxed rules).
-- Stored profiles, approvals and plans are durable evidence and are not changed.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE OR REPLACE FUNCTION rt.enforce_advanced_ingress_plan_change()
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
           -- R62: the profile covers the plan's size (the size-independent profile covers every configured size).
           OR profile_row.universe_target < NEW.universe_target
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

CREATE OR REPLACE FUNCTION rt.enforce_advanced_authorization_promotion()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
DECLARE
    parent_row rt.advanced_stream_authorizations%ROWTYPE;
    expected_channels text[];
BEGIN
    expected_channels := CASE NEW.feed
        WHEN 'STOCKS' THEN ARRAY['AM']::text[]
        WHEN 'OPTIONS' THEN ARRAY['T','Q']::text[]
        ELSE ARRAY[]::text[]
    END;
    IF NOT EXISTS (
        SELECT 1
        FROM rt.advanced_ingress_runtime_profiles
        WHERE universe_target >= NEW.allowed_universe_targets[1]
          AND runtime_policy_sha256 = NEW.runtime_policy_sha256
          AND provider_tier = 'MASSIVE_ADVANCED'
          AND feed = NEW.feed
          AND channels = expected_channels
          AND status = 'ACTIVE'
    ) THEN
        RAISE EXCEPTION
            'standing authorization requires an ACTIVE product runtime profile covering its size';
    END IF;
    IF NEW.parent_authorization_id IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT * INTO parent_row
    FROM rt.advanced_stream_authorizations
    WHERE authorization_id = NEW.parent_authorization_id;
    IF parent_row.authorization_id IS NULL
       OR parent_row.status <> 'READY'
       OR (parent_row.feed, parent_row.channel)
            IS DISTINCT FROM (NEW.feed, NEW.channel)
       OR parent_row.runtime_policy_sha256 <> NEW.runtime_policy_sha256
       OR NEW.valid_from <= parent_row.valid_through THEN
        RAISE EXCEPTION
            'a parent authorization must be READY, the same product and policy, and end before its child starts';
    END IF;
    RETURN NEW;
END;
$function$;

-- Universe limits (Nov 15, docs/g102/G102_UNIVERSE_SIZING_20261114.md section 3.5): the universe size is a setting
-- (REALTIME_UNIVERSE_MAX_NAMES in realtime/universe_limits.py, default 400). The schema keeps only the technical ceiling
-- SCHEMA_MAX_UNIVERSE_NAMES = 5000 instead of pinning 400 on stream plans, stream approvals and runtime profiles, and the
-- size-independent profile below covers that ceiling, so changing the setting needs no migration.
DO $limits$
DECLARE
    item record;
BEGIN
    FOR item IN
        SELECT catalog_constraint.conrelid::regclass AS relation, catalog_constraint.conname AS name
        FROM pg_constraint AS catalog_constraint
        WHERE catalog_constraint.conrelid IN (
                'rt.advanced_ingress_plans'::regclass,
                'rt.advanced_stream_authorizations'::regclass,
                'rt.advanced_ingress_runtime_profiles'::regclass
            )
          AND catalog_constraint.contype = 'c'
          AND pg_get_constraintdef(catalog_constraint.oid) LIKE '%<= 400%'
    LOOP
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', item.relation, item.name);
    END LOOP;
END;
$limits$;

ALTER TABLE rt.advanced_ingress_plans
    ADD CONSTRAINT advanced_ingress_plans_universe_target_check
    CHECK (universe_target BETWEEN 1 AND 5000);

ALTER TABLE rt.advanced_stream_authorizations
    ADD CONSTRAINT advanced_stream_authorizations_allowed_universe_targets_check
    CHECK (
        cardinality(allowed_universe_targets) = 1
        AND allowed_universe_targets[1] BETWEEN 1 AND 5000
    );

ALTER TABLE rt.advanced_ingress_runtime_profiles
    ADD CONSTRAINT advanced_ingress_runtime_profiles_product_scope_check
    CHECK (
        universe_target BETWEEN 1 AND 5000
        AND ladder_position >= 0
        AND (
            (ladder_position = 0 AND previous_universe_target IS NULL)
            OR (
                ladder_position > 0
                AND previous_universe_target BETWEEN 1 AND 5000
                AND previous_universe_target < universe_target
            )
        )
        AND provider_tier = 'MASSIVE_ADVANCED'
        AND (
            (feed = 'STOCKS' AND channels = ARRAY['AM']::text[])
            OR (
                feed = 'OPTIONS'
                AND channels = ARRAY['T','Q']::text[]
            )
        )
        AND jsonb_typeof(profile_document) = 'object'
    );

INSERT INTO rt.advanced_ingress_runtime_profiles (
    runtime_profile_sha256, runtime_profile_version, universe_target,
    ladder_position, previous_universe_target,
    provider_tier, feed, channels, profile_document,
    configuration_sha256, runtime_policy_sha256, status
) VALUES (
    'ae31b5f304d04fd56359547f59551bc0670e0ef35e2cb3fe6d2a265067a23ba7',
    'advanced-equity-ingress-v6',
    5000,
    0,
    NULL,
    'MASSIVE_ADVANCED',
    'STOCKS',
    ARRAY['AM']::text[],
    $profile${"channels":["AM"],"contract":"advanced-equity-ingress-runtime-profile-v1","feed":"STOCKS","provider_tier":"MASSIVE_ADVANCED","settings":{"bridge_batch_limit":1000,"bridge_poll_interval_seconds":1.0,"bridge_statement_timeout_ms":10000,"buffer_byte_limit":8388608,"buffer_event_limit":128,"buffer_max_age_nanoseconds":30000000000,"controller_warmup_seconds":900,"database_statement_timeout_ms":30000,"drain_deadline_seconds":30,"heartbeat_interval_seconds":20,"ingress_poll_interval_seconds":0.1,"journal_batch_bytes":8388608,"journal_batch_messages":100,"journal_read_timeout_seconds":5.0,"maximum_message_bytes":2097152,"minimum_journal_free_bytes":5368709120,"minimum_journal_free_percent":20,"reconnect_backoff_multiplier":2.0,"reconnect_initial_backoff_seconds":0.5,"reconnect_jitter_ceiling_seconds":30.0,"reconnect_maximum_backoff_seconds":30.0,"rest_aggregate_endpoint_class":"MASSIVE_STOCKS_AGGREGATES_1M","rest_eligibility_policy_version":"advanced-real-time-closed-bar-v1","rest_maximum_attempts":2,"rest_provider_delay_seconds":0,"rest_publication_retry_grace_seconds":10,"scheduler_window_retry_initial_backoff_seconds":30,"scheduler_window_retry_max_attempts":3,"scheduler_window_retry_max_backoff_seconds":300,"shutdown_deadline_seconds":30,"stable_live_reset_seconds":60,"stale_after_seconds":60,"websocket_close_timeout_seconds":5,"websocket_library_max_queue":2,"websocket_maximum_control_messages":8,"websocket_open_timeout_seconds":10,"websocket_ping_interval_seconds":20,"websocket_ping_timeout_seconds":10},"universe_target":5000,"version":"advanced-equity-ingress-v6"}$profile$::jsonb,
    'f80ce7f0241114930f857902849ad91217e39d97def37ac0f626f2d6783f53d5',
    '2504ed8ced910fc42a2a5c69a093c2da8f5d9cfd36d3aa7f11b729c815e721dc',
    'CANDIDATE'
) ON CONFLICT (runtime_profile_sha256) DO NOTHING;

UPDATE rt.advanced_ingress_runtime_profiles
SET status = 'ACTIVE',
    stability_evidence_sha256 = '9caf412284a7a2670b30accccf87ad4c54cc39990bcbccf02d8a4dbce673e851',
    capacity_evidence_sha256 = 'a46a22fdd34a315ff6d48c202676c8e9a7daac420680e9428a865ddeb555daa9',
    activation_approval_reference = 'g102-r62-migration-053-size-independent-runtime-profile',
    activated_at = clock_timestamp()
WHERE runtime_profile_sha256 = 'ae31b5f304d04fd56359547f59551bc0670e0ef35e2cb3fe6d2a265067a23ba7'
  AND status = 'CANDIDATE';

DO $check$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM rt.advanced_ingress_runtime_profiles
        WHERE runtime_profile_sha256 = 'ae31b5f304d04fd56359547f59551bc0670e0ef35e2cb3fe6d2a265067a23ba7'
          AND runtime_profile_version = 'advanced-equity-ingress-v6'
          AND universe_target = 5000
          AND configuration_sha256 = 'f80ce7f0241114930f857902849ad91217e39d97def37ac0f626f2d6783f53d5'
          AND runtime_policy_sha256 = '2504ed8ced910fc42a2a5c69a093c2da8f5d9cfd36d3aa7f11b729c815e721dc'
          AND status = 'ACTIVE'
    ) THEN
        RAISE EXCEPTION 'the size-independent runtime profile is not ACTIVE';
    END IF;
END;
$check$;
