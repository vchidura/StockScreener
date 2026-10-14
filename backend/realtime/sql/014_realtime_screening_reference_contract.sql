SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE ref.security_revisions
    ADD COLUMN company_name text,
    ADD COLUMN security_type varchar(16);

ALTER TABLE ref.security_revisions
    ADD CONSTRAINT ck_ref_security_type_nonblank
    CHECK (security_type IS NULL OR btrim(security_type) <> '');
