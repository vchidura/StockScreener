SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM rt.capability_profiles) THEN
        RAISE EXCEPTION 'preflight identity migration requires an empty capability profile ledger';
    END IF;
END;
$$;

ALTER TABLE rt.capability_profiles
    ADD COLUMN profile_sha256 char(64) NOT NULL UNIQUE,
    ADD COLUMN provider_tier varchar(24) NOT NULL,
    ADD COLUMN security_policy_sha256 char(64) NOT NULL,
    ADD COLUMN preflight_mode varchar(16) NOT NULL,
    ADD COLUMN provider_requests boolean NOT NULL,
    ADD COLUMN action_authorized boolean NOT NULL,
    ADD CONSTRAINT ck_rt_capability_profile_sha256
        CHECK (profile_sha256 ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT ck_rt_capability_profile_provider_tier
        CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    ADD CONSTRAINT ck_rt_capability_profile_security_sha256
        CHECK (security_policy_sha256 ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT ck_rt_capability_profile_passive_mode
        CHECK (
            preflight_mode = 'PASSIVE'
            AND provider_requests = false
            AND action_authorized = false
        );

CREATE TABLE rt.provider_tier_transitions (
    transition_id uuid PRIMARY KEY,
    previous_state varchar(40) NOT NULL,
    target_state varchar(40) NOT NULL,
    transitioned_at timestamptz NOT NULL,
    operator_approved boolean NOT NULL,
    operator_approval_reference text,
    dependencies_warm boolean NOT NULL,
    reason_code varchar(96) NOT NULL,
    from_profile_id uuid REFERENCES rt.capability_profiles(profile_id),
    to_profile_id uuid REFERENCES rt.capability_profiles(profile_id),
    transition_sha256 char(64) NOT NULL UNIQUE,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (btrim(reason_code) <> ''),
    CHECK (transition_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (
        (operator_approved AND btrim(operator_approval_reference) <> '')
        OR (NOT operator_approved AND operator_approval_reference IS NULL)
    ),
    CHECK (
        (previous_state = 'ADVANCED_READY' AND target_state = 'ADVANCED_BLOCKED')
        OR (previous_state = 'ADVANCED_BLOCKED' AND target_state = 'FALLBACK_PENDING_APPROVAL')
        OR (
            previous_state = 'FALLBACK_PENDING_APPROVAL'
            AND target_state = 'DEVELOPER_WARMING'
            AND operator_approved
        )
        OR (
            previous_state = 'DEVELOPER_WARMING'
            AND target_state = 'DEVELOPER_READY'
            AND dependencies_warm
        )
        OR (
            previous_state = 'DEVELOPER_READY'
            AND target_state = 'ADVANCED_WARMING'
            AND operator_approved
        )
        OR (
            previous_state = 'ADVANCED_WARMING'
            AND target_state = 'ADVANCED_READY'
            AND dependencies_warm
        )
    )
);

CREATE INDEX idx_rt_provider_tier_transition_time
    ON rt.provider_tier_transitions (transitioned_at DESC, transition_id);

CREATE TRIGGER trg_rt_provider_tier_transitions_immutable
    BEFORE UPDATE OR DELETE ON rt.provider_tier_transitions
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_provider_tier_transitions_no_truncate
    BEFORE TRUNCATE ON rt.provider_tier_transitions
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();