SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE rt.capability_profiles
    DROP CONSTRAINT ck_rt_capability_profile_passive_mode;

ALTER TABLE rt.capability_profiles
    ADD CONSTRAINT ck_rt_capability_profile_mode CHECK (
        (
            preflight_mode = 'PASSIVE'
            AND provider_requests = false
            AND action_authorized = false
        )
        OR
        (
            preflight_mode = 'ACTIVE'
            AND provider_requests = true
            AND action_authorized = true
        )
    );
