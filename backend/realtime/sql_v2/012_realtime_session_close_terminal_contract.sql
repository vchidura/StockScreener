SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE rt.session_close_audits
    ADD COLUMN eligible_at timestamptz NOT NULL,
    ADD COLUMN eligibility_policy_sha256 char(64) NOT NULL,
    ADD COLUMN input_sha256 char(64) NOT NULL;

ALTER TABLE rt.session_close_audits
    ADD CONSTRAINT session_close_audits_eligible_at_check
        CHECK (eligible_at >= session_close AND known_by >= eligible_at),
    ADD CONSTRAINT session_close_audits_eligibility_policy_sha256_check
        CHECK (eligibility_policy_sha256 ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT session_close_audits_input_sha256_check
        CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
    ADD CONSTRAINT session_close_audits_content_identity_key
        UNIQUE (cohort_id, provider_tier, session_date, coverage_sha256);

CREATE FUNCTION rt.validate_session_close_current() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'rt'
    AS $$
DECLARE
    audit_known_by timestamptz;
    audit_status varchar;
BEGIN
    SELECT known_by, status
      INTO audit_known_by, audit_status
      FROM rt.session_close_audits
     WHERE audit_id = NEW.audit_id
       AND cohort_id = NEW.cohort_id
       AND provider_tier = NEW.provider_tier
       AND session_date = NEW.session_date;
    IF NOT FOUND OR audit_known_by <> NEW.known_by OR audit_status <> NEW.status THEN
        RAISE EXCEPTION 'session-close current pointer does not match its audit';
    END IF;
    IF TG_OP = 'UPDATE'
       AND OLD.status = 'COMPLETE'
       AND NEW.status <> 'COMPLETE' THEN
        RAISE EXCEPTION 'a complete session-close pointer cannot regress';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_rt_session_close_current_validate
    BEFORE INSERT OR UPDATE ON rt.session_close_current
    FOR EACH ROW EXECUTE FUNCTION rt.validate_session_close_current();
