-- G102 design 5.1 (G90): a newer desired revision for the same session date supersedes an Options preparation that
-- was never activated. Migration 045's expiration columns already record SUPERSEDED (with expired_at and the reason);
-- the session date stays unique only among preparations that are neither expired nor superseded, so the new
-- revision's row can be inserted beside the superseded one. Nothing is deleted. Before this, a list change for a date
-- that already had a preparation raised UniqueViolation on every reconcile (162 restarts on Nov 6).
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE rt.advanced_options_preparation_sessions
    DROP CONSTRAINT advanced_options_preparation_sessions_session_date_key;

CREATE UNIQUE INDEX uq_rt_advanced_options_preparation_current_date
    ON rt.advanced_options_preparation_sessions (session_date)
    WHERE expiration_state IS NULL;
