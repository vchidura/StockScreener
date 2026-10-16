SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE FUNCTION rt.validate_session_close_current_order() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'rt'
    AS $$
BEGIN
    IF (NEW.cohort_id, NEW.provider_tier, NEW.session_date)
       IS DISTINCT FROM (OLD.cohort_id, OLD.provider_tier, OLD.session_date) THEN
        RAISE EXCEPTION 'session-close current pointer scope cannot change';
    END IF;
    IF NEW.known_by < OLD.known_by THEN
        RAISE EXCEPTION 'session-close current pointer cannot move backward';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_rt_session_close_current_order
    BEFORE UPDATE ON rt.session_close_current
    FOR EACH ROW EXECUTE FUNCTION rt.validate_session_close_current_order();
