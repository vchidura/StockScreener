SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE rt.session_archives
    DROP CONSTRAINT session_archives_session_date_kind_sha256_key;

ALTER TABLE rt.session_archives
    ADD CONSTRAINT session_archives_build_identity_key
    UNIQUE NULLS NOT DISTINCT (
        session_date,
        kind,
        horizon_close_date,
        sha256,
        build_id
    );
