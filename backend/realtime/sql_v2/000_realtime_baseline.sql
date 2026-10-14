SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE SCHEMA ref;
CREATE SCHEMA rt;

CREATE TABLE rt.database_identity (
    lineage_name varchar(64) PRIMARY KEY,
    database_name name NOT NULL DEFAULT current_database(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (lineage_name = 'advanced_realtime_v2')
);

INSERT INTO rt.database_identity (lineage_name)
VALUES ('advanced_realtime_v2');

CREATE TABLE rt.schema_migrations (
    version varchar(96) PRIMARY KEY,
    checksum_sha256 char(64) NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (checksum_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE FUNCTION rt.reject_immutable_change() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'rt', 'ref'
    AS $$
BEGIN
    RAISE EXCEPTION '% is immutable', TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;
END;
$$;

-- Catalog tables are append-only except that whole-lifecycle pruning deletes rows
-- (only the partition_admin capability is granted DELETE on them).
CREATE FUNCTION rt.reject_catalog_update() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog', 'rt', 'ref'
    AS $$
BEGIN
    RAISE EXCEPTION '% rows are immutable; only lifecycle pruning may delete them',
        TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;
END;
$$;
