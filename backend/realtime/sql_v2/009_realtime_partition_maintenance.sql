SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

-- Retention and partition policy for every partitioned relation. Windows are defined in
-- docs/REALTIME_SCHEMA_V2_DESIGN.md (Retention windows). There is deliberately no DEFAULT
-- partition: DETACH ... CONCURRENTLY cannot be used when one exists, and a missed
-- creation must be an alarm, not silent data in a default partition.
CREATE TABLE rt.partition_policies (
    relation_name varchar(96) PRIMARY KEY,
    scheme varchar(8) NOT NULL,
    key_column varchar(64) NOT NULL,
    key_type varchar(12) NOT NULL,
    retention_class varchar(16) NOT NULL,
    window_kind varchar(16) NOT NULL,
    window_value integer NOT NULL,
    pre_create_days integer NOT NULL DEFAULT 14,
    lookback_days integer NOT NULL DEFAULT 3,
    requires_archive boolean NOT NULL DEFAULT false,
    -- Archive kinds (rt.session_archives.kind) that must be verified for every session in a partition
    -- before it may be dropped; non-empty exactly when requires_archive.
    archive_kinds text[] NOT NULL DEFAULT ARRAY[]::text[],
    catalog_relation varchar(96),
    CHECK (relation_name ~ '^rt\.[a-z_]+$'),
    CHECK (scheme IN ('DAILY', 'WEEKLY', 'MONTHLY')),
    CHECK (key_type IN ('timestamptz', 'date')),
    CHECK (retention_class IN ('HOT_TTL', 'SESSION_WINDOW', 'KEPT_3M', 'HISTORY_PLAN', 'ARCHIVE_FIRST')),
    CHECK (window_kind IN ('CALENDAR_DAYS', 'SESSIONS', 'MONTHS')),
    CHECK (window_value > 0),
    CHECK (pre_create_days >= 7 AND lookback_days >= 0),
    CHECK (requires_archive = (cardinality(archive_kinds) > 0)),
    CHECK ((retention_class = 'ARCHIVE_FIRST') = requires_archive),
    CHECK (catalog_relation IS NULL OR catalog_relation ~ '^rt\.[a-z_]+$')
);

-- portal_snapshot_kept holds session-close releases (picker window 21 sessions) AND strategy releases
-- (21 + 4 lookback + 1 = 26 sessions). One table cannot discriminate, so the physical window is the
-- larger 26; the 21-session picker is enforced by the reader, not by retention.
INSERT INTO rt.partition_policies
    (relation_name, scheme, key_column, key_type, retention_class, window_kind, window_value,
     pre_create_days, lookback_days, requires_archive, archive_kinds, catalog_relation)
VALUES
    ('rt.raw_payloads', 'DAILY', 'arrived_at', 'timestamptz', 'HOT_TTL', 'CALENDAR_DAYS', 14, 14, 3, false, ARRAY[]::text[], NULL),
    ('rt.raw_payload_chunks', 'DAILY', 'arrived_at', 'timestamptz', 'HOT_TTL', 'CALENDAR_DAYS', 14, 14, 3, false, ARRAY[]::text[], NULL),
    ('rt.option_discovery_pages', 'DAILY', 'received_at', 'timestamptz', 'HOT_TTL', 'CALENDAR_DAYS', 14, 14, 3, false, ARRAY[]::text[], NULL),
    ('rt.acquisition_facts', 'MONTHLY', 'observed_at', 'timestamptz', 'KEPT_3M', 'MONTHS', 3, 14, 35, false, ARRAY[]::text[], NULL),
    ('rt.bar_revisions', 'MONTHLY', 'observed_at', 'timestamptz', 'HISTORY_PLAN', 'MONTHS', 15, 14, 400, false, ARRAY[]::text[], 'rt.bar_catalog'),
    ('rt.equity_evidence', 'MONTHLY', 'durable_at', 'timestamptz', 'KEPT_3M', 'MONTHS', 3, 14, 35, false, ARRAY[]::text[], 'rt.evidence_catalog'),
    ('rt.equity_evidence_inputs', 'MONTHLY', 'durable_at', 'timestamptz', 'KEPT_3M', 'MONTHS', 3, 14, 35, false, ARRAY[]::text[], NULL),
    ('rt.equity_evidence_dependencies', 'MONTHLY', 'durable_at', 'timestamptz', 'KEPT_3M', 'MONTHS', 3, 14, 35, false, ARRAY[]::text[], NULL),
    ('rt.equity_evidence_source_windows', 'MONTHLY', 'durable_at', 'timestamptz', 'KEPT_3M', 'MONTHS', 3, 14, 35, false, ARRAY[]::text[], 'rt.evidence_source_window_catalog'),
    ('rt.feature_snapshots', 'MONTHLY', 'durable_at', 'timestamptz', 'KEPT_3M', 'MONTHS', 3, 14, 35, false, ARRAY[]::text[], NULL),
    ('rt.normalization_diagnostics', 'MONTHLY', 'created_at', 'timestamptz', 'KEPT_3M', 'MONTHS', 3, 14, 35, false, ARRAY[]::text[], NULL),
    ('rt.portal_snapshot_hot', 'DAILY', 'generated_at', 'timestamptz', 'HOT_TTL', 'CALENDAR_DAYS', 3, 14, 3, false, ARRAY[]::text[], 'rt.snapshot_catalog'),
    ('rt.portal_snapshot_kept', 'WEEKLY', 'session_date', 'date', 'SESSION_WINDOW', 'SESSIONS', 26, 14, 28, false, ARRAY[]::text[], 'rt.snapshot_catalog'),
    ('rt.portal_releases', 'MONTHLY', 'cutoff', 'timestamptz', 'KEPT_3M', 'MONTHS', 3, 14, 35, false, ARRAY[]::text[], NULL),
    ('rt.option_flow_events', 'WEEKLY', 'event_time', 'timestamptz', 'ARCHIVE_FIRST', 'SESSIONS', 26, 14, 28, true, ARRAY['OPTION_FLOW_EVENTS'], NULL),
    ('rt.option_flow_clusters', 'WEEKLY', 'session_date', 'date', 'ARCHIVE_FIRST', 'SESSIONS', 26, 14, 28, true, ARRAY['OPTION_FLOW_CLUSTERS'], NULL);

-- Creates the missing partitions of one policy relation. Runs as the owner so the
-- runtime role never needs CREATE on the schema.
CREATE FUNCTION rt.ensure_partitions(p_relation text, p_today date) RETURNS integer
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'rt'
    AS $$
DECLARE
    policy rt.partition_policies%ROWTYPE;
    cursor_start date;
    cursor_end date;
    range_end date;
    created integer := 0;
    child_name text;
    lower_bound text;
    upper_bound text;
BEGIN
    SELECT * INTO policy FROM rt.partition_policies WHERE relation_name = p_relation;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'no partition policy for %', p_relation;
    END IF;
    cursor_start := CASE policy.scheme
        WHEN 'DAILY' THEN (p_today - policy.lookback_days)
        WHEN 'WEEKLY' THEN date_trunc('week', (p_today - policy.lookback_days)::timestamp)::date
        ELSE date_trunc('month', (p_today - policy.lookback_days)::timestamp)::date
    END;
    range_end := p_today + policy.pre_create_days;
    WHILE cursor_start <= range_end LOOP
        cursor_end := CASE policy.scheme
            WHEN 'DAILY' THEN cursor_start + 1
            WHEN 'WEEKLY' THEN cursor_start + 7
            ELSE (cursor_start + interval '1 month')::date
        END;
        child_name := substr(p_relation, 4) || '_p' || to_char(cursor_start, 'YYYYMMDD');
        IF to_regclass(format('rt.%I', child_name)) IS NULL THEN
            IF policy.key_type = 'date' THEN
                lower_bound := cursor_start::text;
                upper_bound := cursor_end::text;
            ELSE
                lower_bound := (cursor_start::timestamp AT TIME ZONE 'UTC')::text;
                upper_bound := (cursor_end::timestamp AT TIME ZONE 'UTC')::text;
            END IF;
            EXECUTE format(
                'CREATE TABLE rt.%I PARTITION OF %s FOR VALUES FROM (%L) TO (%L)',
                child_name, p_relation, lower_bound, upper_bound
            );
            created := created + 1;
        END IF;
        cursor_start := cursor_end;
    END LOOP;
    RETURN created;
END;
$$;

CREATE FUNCTION rt.ensure_partitions_all(p_today date) RETURNS integer
    LANGUAGE plpgsql
    SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'rt'
    AS $$
DECLARE
    policy_name text;
    created integer := 0;
BEGIN
    FOR policy_name IN SELECT relation_name FROM rt.partition_policies ORDER BY relation_name LOOP
        created := created + rt.ensure_partitions(policy_name, p_today);
    END LOOP;
    RETURN created;
END;
$$;

REVOKE ALL ON FUNCTION rt.ensure_partitions(text, date) FROM PUBLIC;
REVOKE ALL ON FUNCTION rt.ensure_partitions_all(date) FROM PUBLIC;

-- Alarm surface for missed partition creation: a policy relation with fewer than 7 pre-created days
-- (or no partition at all) is an ALARM. Callers (the maintenance loop and the archive worker) must
-- raise it; a missed creation is never silent because there is no default partition.
CREATE FUNCTION rt.partition_health(p_today date)
    RETURNS TABLE (relation_name text, scheme text, horizon_date date, days_ahead integer, status text)
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog', 'rt'
    AS $$
    SELECT policy.relation_name::text, policy.scheme::text, horizon.horizon_date,
           COALESCE(horizon.horizon_date - p_today, -1)::integer,
           CASE WHEN horizon.horizon_date IS NOT NULL AND horizon.horizon_date - p_today >= 7
                THEN 'OK' ELSE 'ALARM' END
    FROM rt.partition_policies policy
    LEFT JOIN LATERAL (
        SELECT max(CASE policy.scheme
                       WHEN 'DAILY' THEN children.start_date + 1
                       WHEN 'WEEKLY' THEN children.start_date + 7
                       ELSE (children.start_date + interval '1 month')::date
                   END) AS horizon_date
        FROM (
            SELECT to_date(substring(child.relname FROM '_p([0-9]{8})$'), 'YYYYMMDD') AS start_date
            FROM pg_inherits link
            JOIN pg_class child ON child.oid = link.inhrelid
            WHERE link.inhparent = to_regclass(policy.relation_name::text)
        ) children
    ) horizon ON true
    ORDER BY policy.relation_name;
$$;

REVOKE ALL ON FUNCTION rt.partition_health(date) FROM PUBLIC;

SELECT rt.ensure_partitions_all(CURRENT_DATE);
