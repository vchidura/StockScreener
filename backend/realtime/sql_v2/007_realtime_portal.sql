SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

-- Snapshot identity catalog (unpartitioned). Bodies are split by retention: per-cycle
-- snapshots are short-lived; session-close releases follow the picker window.
CREATE TABLE rt.snapshot_catalog (
    snapshot_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    snapshot_class varchar(24) NOT NULL,
    snapshot_type varchar(48) NOT NULL,
    snapshot_key text NOT NULL,
    session_date date NOT NULL,
    source_generation bigint NOT NULL,
    manifest_sha256 char(64) NOT NULL,
    payload_sha256 char(64) NOT NULL,
    generated_at timestamptz NOT NULL,
    valid_until timestamptz,
    UNIQUE (cohort_id, snapshot_class, snapshot_type, snapshot_key, manifest_sha256, payload_sha256),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (snapshot_class IN ('EPHEMERAL_PORTAL', 'SESSION_CLOSE')),
    CHECK (btrim(snapshot_type) <> '' AND btrim(snapshot_key) <> ''),
    CHECK (source_generation > 0),
    CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (valid_until IS NULL OR valid_until > generated_at)
);

CREATE INDEX idx_rt_snapshot_catalog_lookup
    ON rt.snapshot_catalog (cohort_id, snapshot_type, snapshot_key, generated_at DESC);
CREATE INDEX idx_rt_snapshot_catalog_session_close
    ON rt.snapshot_catalog (cohort_id, snapshot_type, session_date)
    WHERE snapshot_class = 'SESSION_CLOSE';
CREATE INDEX idx_rt_snapshot_catalog_prune ON rt.snapshot_catalog (generated_at);

-- Per-cycle snapshots (current and previous session only). The manifest is a lineage
-- head, not the embedded bar history.
CREATE TABLE rt.portal_snapshot_hot (
    snapshot_id uuid NOT NULL REFERENCES rt.snapshot_catalog(snapshot_id),
    generated_at timestamptz NOT NULL,
    cohort_id uuid NOT NULL,
    snapshot_type varchar(48) NOT NULL,
    snapshot_key text NOT NULL,
    source_manifest jsonb NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY (snapshot_id, generated_at),
    CHECK (jsonb_typeof(source_manifest) = 'object'),
    CHECK (octet_length(source_manifest::text) <= 16384),
    CHECK (jsonb_typeof(payload) = 'object')
) PARTITION BY RANGE (generated_at);

CREATE INDEX idx_rt_portal_snapshot_hot_lookup
    ON rt.portal_snapshot_hot (cohort_id, snapshot_type, snapshot_key, generated_at DESC);

-- One self-contained session-close release per session and publication type.
CREATE TABLE rt.portal_snapshot_kept (
    snapshot_id uuid NOT NULL REFERENCES rt.snapshot_catalog(snapshot_id),
    session_date date NOT NULL,
    cohort_id uuid NOT NULL,
    snapshot_type varchar(48) NOT NULL,
    snapshot_key text NOT NULL,
    source_manifest jsonb NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY (snapshot_id, session_date),
    CHECK (jsonb_typeof(source_manifest) = 'object'),
    CHECK (jsonb_typeof(payload) = 'object')
) PARTITION BY RANGE (session_date);

CREATE INDEX idx_rt_portal_snapshot_kept_lookup
    ON rt.portal_snapshot_kept (cohort_id, snapshot_type, snapshot_key, session_date DESC);

-- A release names every snapshot of one cutoff, so a reader never mixes cutoffs.
CREATE TABLE rt.portal_releases (
    release_id uuid NOT NULL,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    release_class varchar(16) NOT NULL,
    receipt_id uuid NOT NULL REFERENCES rt.cycle_receipts(receipt_id),
    session_date date NOT NULL,
    cutoff timestamptz NOT NULL,
    universe_sha256 char(64) NOT NULL,
    manifest jsonb NOT NULL,
    manifest_sha256 char(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (release_id, cutoff),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (release_class IN ('INTRADAY', 'SESSION_CLOSE')),
    CHECK (universe_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(manifest) = 'object')
) PARTITION BY RANGE (cutoff);

CREATE INDEX idx_rt_portal_release_session_close
    ON rt.portal_releases (cohort_id, provider_tier, session_date)
    WHERE release_class = 'SESSION_CLOSE';

-- The single atomic pointer the reader follows; release_id is a soft reference that the
-- publisher validates in the publishing transaction.
CREATE TABLE rt.portal_release_current (
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    release_id uuid NOT NULL,
    cutoff timestamptz NOT NULL,
    session_date date NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cohort_id, provider_tier),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER'))
) WITH (fillfactor = 80);

CREATE TRIGGER trg_rt_snapshot_catalog_update
    BEFORE UPDATE ON rt.snapshot_catalog
    FOR EACH ROW EXECUTE FUNCTION rt.reject_catalog_update();
CREATE TRIGGER trg_rt_portal_snapshot_hot_immutable
    BEFORE UPDATE OR DELETE ON rt.portal_snapshot_hot
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_portal_snapshot_kept_immutable
    BEFORE UPDATE OR DELETE ON rt.portal_snapshot_kept
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_portal_releases_immutable
    BEFORE UPDATE OR DELETE ON rt.portal_releases
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
