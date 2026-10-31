SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.realtime_desired_revisions (
    revision_id uuid PRIMARY KEY,
    revision_sha256 char(64) NOT NULL UNIQUE,
    effective_session_date date NOT NULL,
    source_cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    source_slot_version integer,
    selection_policy_reference varchar(200) NOT NULL,
    selection_policy_sha256 char(64) NOT NULL,
    enabled boolean NOT NULL,
    recorded_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (source_cohort_id, source_slot_version)
        REFERENCES rt.slot_set_versions(cohort_id, version),
    CHECK (revision_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (selection_policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (source_slot_version IS NULL OR source_slot_version > 0),
    CHECK (btrim(selection_policy_reference) <> '')
);

CREATE INDEX idx_rt_realtime_desired_revision_effective
    ON rt.realtime_desired_revisions (
        effective_session_date DESC,
        created_at DESC
    );

CREATE TABLE rt.realtime_desired_current (
    scope_key varchar(64) PRIMARY KEY,
    revision_id uuid NOT NULL REFERENCES rt.realtime_desired_revisions(revision_id),
    effective_session_date date NOT NULL,
    revision_recorded_at timestamptz NOT NULL,
    selected_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (btrim(scope_key) <> ''),
    CHECK (selected_at >= revision_recorded_at)
);

CREATE TABLE rt.realtime_desired_members (
    revision_id uuid NOT NULL REFERENCES rt.realtime_desired_revisions(revision_id),
    security_id uuid NOT NULL REFERENCES ref.securities(security_id),
    ticker varchar(32) NOT NULL,
    member_kind varchar(24) NOT NULL,
    priority smallint NOT NULL,
    enabled boolean NOT NULL,
    PRIMARY KEY (revision_id, security_id, member_kind),
    UNIQUE (revision_id, ticker, member_kind),
    CHECK (ticker = upper(ticker) AND btrim(ticker) <> ''),
    CHECK (member_kind IN ('EQUITY', 'SECTOR_ETF', 'OPTION_UNDERLYING')),
    CHECK (priority BETWEEN 1 AND 32767)
);

CREATE TABLE rt.realtime_desired_member_readiness (
    revision_id uuid NOT NULL,
    security_id uuid NOT NULL,
    member_kind varchar(24) NOT NULL,
    status varchar(16) NOT NULL,
    reasons text[] NOT NULL DEFAULT ARRAY[]::text[],
    checked_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (revision_id, security_id, member_kind),
    FOREIGN KEY (revision_id, security_id, member_kind)
        REFERENCES rt.realtime_desired_members(
            revision_id,
            security_id,
            member_kind
        ),
    CHECK (member_kind IN ('EQUITY', 'SECTOR_ETF', 'OPTION_UNDERLYING')),
    CHECK (status IN ('WARMING', 'READY', 'FAILED')),
    CHECK (
        (status = 'READY' AND cardinality(reasons) = 0)
        OR (status = 'WARMING')
        OR (status = 'FAILED' AND cardinality(reasons) > 0)
    )
);

CREATE TABLE rt.realtime_desired_session_snapshots (
    revision_id uuid PRIMARY KEY REFERENCES rt.realtime_desired_revisions(revision_id),
    session_date date NOT NULL,
    manifest_id uuid NOT NULL REFERENCES rt.subscription_manifests(manifest_id),
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    slot_set_version integer,
    snapshot_sha256 char(64) NOT NULL UNIQUE,
    materialized_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (cohort_id, slot_set_version)
        REFERENCES rt.slot_set_versions(cohort_id, version),
    CHECK (slot_set_version IS NULL OR slot_set_version > 0),
    CHECK (snapshot_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TRIGGER trg_rt_realtime_desired_revisions_immutable
    BEFORE UPDATE OR DELETE ON rt.realtime_desired_revisions
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_realtime_desired_revisions_no_truncate
    BEFORE TRUNCATE ON rt.realtime_desired_revisions
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_realtime_desired_members_immutable
    BEFORE UPDATE OR DELETE ON rt.realtime_desired_members
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_realtime_desired_members_no_truncate
    BEFORE TRUNCATE ON rt.realtime_desired_members
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_realtime_desired_session_snapshots_immutable
    BEFORE UPDATE OR DELETE ON rt.realtime_desired_session_snapshots
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_realtime_desired_session_snapshots_no_truncate
    BEFORE TRUNCATE ON rt.realtime_desired_session_snapshots
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
