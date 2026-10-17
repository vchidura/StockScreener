SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

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
    ),
    CHECK (
        (
            status IN ('REGISTERED', 'WRITING')
            AND sealed_files_sha256 IS NULL
            AND sealed_at IS NULL
            AND drained_at IS NULL
            AND quarantined_at IS NULL
            AND quarantine_reason IS NULL
            AND quarantine_detail IS NULL
        )
        OR
        (
            status = 'DRAINING'
            AND sealed_files_sha256 IS NOT NULL
            AND sealed_at IS NOT NULL
            AND drained_at IS NULL
            AND quarantined_at IS NULL
            AND quarantine_reason IS NULL
            AND quarantine_detail IS NULL
        )
        OR
        (
            status = 'DRAINED'
            AND sealed_files_sha256 IS NOT NULL
            AND sealed_at IS NOT NULL
            AND drained_at IS NOT NULL
            AND quarantined_at IS NULL
            AND quarantine_reason IS NULL
            AND quarantine_detail IS NULL
        )
        OR
        (
            status = 'QUARANTINED'
            AND quarantined_at IS NOT NULL
            AND btrim(quarantine_reason) <> ''
            AND btrim(quarantine_detail) <> ''
            AND drained_at IS NULL
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

CREATE TABLE rt.advanced_journal_capacity_events (
    capacity_event_id uuid PRIMARY KEY,
    plan_id uuid NOT NULL REFERENCES rt.advanced_ingress_plans(plan_id),
    journal_root_sha256 char(64) NOT NULL,
    total_bytes bigint NOT NULL,
    free_bytes bigint NOT NULL,
    free_percent numeric(7,4) NOT NULL,
    minimum_free_bytes bigint NOT NULL,
    minimum_free_percent numeric(7,4) NOT NULL,
    status varchar(16) NOT NULL,
    observed_at timestamptz NOT NULL,
    CHECK (
        journal_root_sha256 ~ '^[0-9a-f]{64}$'
        AND total_bytes > 0
        AND free_bytes >= 0
        AND free_bytes <= total_bytes
        AND free_percent BETWEEN 0 AND 100
        AND minimum_free_bytes > 0
        AND minimum_free_percent > 0
        AND minimum_free_percent <= 100
        AND status IN ('PASS', 'BLOCKED')
    )
);

CREATE TRIGGER trg_rt_advanced_journal_capacity_events_immutable
    BEFORE DELETE OR UPDATE ON rt.advanced_journal_capacity_events
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

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
