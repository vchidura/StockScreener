SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE rt.canonical_stock_alert_batches
    ADD COLUMN desired_revision_id uuid
        REFERENCES rt.realtime_desired_session_snapshots(revision_id),
    ADD COLUMN desired_snapshot_sha256 char(64),
    ADD COLUMN membership_sha256 char(64),
    ADD COLUMN membership_document jsonb,
    ADD COLUMN observation_session date,
    ADD COLUMN deadline timestamptz,
    ADD COLUMN late_catch_up boolean NOT NULL DEFAULT false,
    ADD COLUMN deliverable boolean NOT NULL DEFAULT true,
    ADD CONSTRAINT canonical_stock_alert_batch_membership_check CHECK (
        (
            desired_snapshot_sha256 IS NULL
            AND membership_sha256 IS NULL
            AND membership_document IS NULL
            AND observation_session IS NULL
        )
        OR (
            desired_snapshot_sha256 ~ '^[0-9a-f]{64}$'
            AND membership_sha256 ~ '^[0-9a-f]{64}$'
            AND jsonb_typeof(membership_document) = 'object'
            AND observation_session IS NOT NULL
        )
    ),
    ADD CONSTRAINT canonical_stock_alert_batch_delivery_check CHECK (
        deliverable = NOT late_catch_up
        AND (deadline IS NULL OR deadline >= cutoff)
    );

ALTER TABLE rt.canonical_stock_alert_evaluations
    ADD COLUMN desired_revision_id uuid
        REFERENCES rt.realtime_desired_session_snapshots(revision_id),
    ADD COLUMN desired_snapshot_sha256 char(64),
    ADD COLUMN membership_sha256 char(64),
    ADD COLUMN observation_session date,
    ADD COLUMN observation_cutoff timestamptz,
    ADD COLUMN late_catch_up boolean NOT NULL DEFAULT false,
    ADD COLUMN deliverable boolean NOT NULL DEFAULT true,
    ADD CONSTRAINT canonical_stock_alert_evaluation_membership_check CHECK (
        (
            desired_snapshot_sha256 IS NULL
            AND membership_sha256 IS NULL
            AND observation_session IS NULL
            AND observation_cutoff IS NULL
        )
        OR (
            desired_snapshot_sha256 ~ '^[0-9a-f]{64}$'
            AND membership_sha256 ~ '^[0-9a-f]{64}$'
            AND observation_session IS NOT NULL
            AND observation_cutoff IS NOT NULL
        )
    ),
    ADD CONSTRAINT canonical_stock_alert_evaluation_delivery_check CHECK (
        deliverable = NOT late_catch_up
    );

CREATE INDEX idx_rt_canonical_stock_alert_batch_membership
    ON rt.canonical_stock_alert_batches (
        desired_revision_id,
        observation_session,
        cutoff DESC
    );

ALTER TABLE rt.portal_releases
    ADD COLUMN publication_generation bigint,
    ADD COLUMN desired_revision_id uuid
        REFERENCES rt.realtime_desired_session_snapshots(revision_id),
    ADD COLUMN desired_snapshot_sha256 char(64),
    ADD COLUMN membership_sha256 char(64),
    ADD COLUMN publication_disposition varchar(32) NOT NULL DEFAULT 'LEGACY',
    ADD COLUMN deliverable boolean NOT NULL DEFAULT true,
    ADD CONSTRAINT portal_release_c7_identity_check CHECK (
        publication_generation IS NULL
        OR publication_generation > 0
    ),
    ADD CONSTRAINT portal_release_c7_sha_check CHECK (
        (desired_snapshot_sha256 IS NULL OR desired_snapshot_sha256 ~ '^[0-9a-f]{64}$')
        AND (membership_sha256 IS NULL OR membership_sha256 ~ '^[0-9a-f]{64}$')
    ),
    ADD CONSTRAINT portal_release_c7_disposition_check CHECK (
        publication_disposition IN (
            'LEGACY',
            'CURRENT',
            'EQUIVALENT_CURRENT',
            'HISTORICAL_STALE',
            'HISTORICAL_NONCURRENT',
            'SESSION_CLOSE'
        )
    );

ALTER TABLE rt.portal_release_current
    ADD COLUMN publication_generation bigint,
    ADD COLUMN desired_snapshot_sha256 char(64),
    ADD COLUMN membership_sha256 char(64),
    ADD CONSTRAINT portal_release_current_c7_identity_check CHECK (
        publication_generation IS NULL
        OR publication_generation > 0
    ),
    ADD CONSTRAINT portal_release_current_c7_sha_check CHECK (
        (desired_snapshot_sha256 IS NULL OR desired_snapshot_sha256 ~ '^[0-9a-f]{64}$')
        AND (membership_sha256 IS NULL OR membership_sha256 ~ '^[0-9a-f]{64}$')
    );

CREATE TABLE rt.portal_publication_work (
    work_id uuid PRIMARY KEY,
    product_key varchar(64) NOT NULL,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    source_receipt_id uuid NOT NULL REFERENCES rt.cycle_receipts(receipt_id),
    source_receipt_sha256 char(64) NOT NULL,
    release_class varchar(16) NOT NULL,
    session_date date NOT NULL,
    cutoff timestamptz NOT NULL,
    publication_generation bigint NOT NULL,
    desired_revision_id uuid
        REFERENCES rt.realtime_desired_session_snapshots(revision_id),
    desired_snapshot_sha256 char(64),
    membership_sha256 char(64),
    snapshot_ids uuid[] NOT NULL,
    kept_snapshot_ids uuid[] NOT NULL DEFAULT ARRAY[]::uuid[],
    payload_sha256 char(64) NOT NULL,
    work_document jsonb NOT NULL,
    state varchar(16) NOT NULL,
    disposition varchar(32) NOT NULL,
    deliverable boolean NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL,
    lease_owner uuid,
    lease_epoch bigint NOT NULL DEFAULT 0,
    lease_until timestamptz,
    failure_reason varchar(160),
    release_id uuid,
    release_cutoff timestamptz,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (product_key, source_receipt_id, release_class),
    FOREIGN KEY (release_id, release_cutoff)
        REFERENCES rt.portal_releases(release_id, cutoff),
    CHECK (btrim(product_key) <> ''),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (source_receipt_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (release_class IN ('INTRADAY', 'SESSION_CLOSE')),
    CHECK (publication_generation > 0),
    CHECK (desired_snapshot_sha256 IS NULL OR desired_snapshot_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (membership_sha256 IS NULL OR membership_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (cardinality(snapshot_ids) > 0),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(work_document) = 'object'),
    CHECK (state IN ('PENDING', 'CLAIMED', 'RETRY', 'COMPLETE', 'TERMINAL')),
    CHECK (
        disposition IN (
            'PENDING',
            'CURRENT',
            'EQUIVALENT_CURRENT',
            'HISTORICAL_STALE',
            'HISTORICAL_NONCURRENT',
            'FAILED_RETRY',
            'FAILED_TERMINAL'
        )
    ),
    CHECK (attempts >= 0 AND lease_epoch >= 0),
    CHECK (
        (state = 'CLAIMED')
        = (lease_owner IS NOT NULL AND lease_until IS NOT NULL)
    ),
    CHECK (
        state = 'CLAIMED'
        OR (lease_owner IS NULL AND lease_until IS NULL)
    ),
    CHECK (
        (state IN ('RETRY', 'TERMINAL'))
        = (failure_reason IS NOT NULL)
    ),
    CHECK (
        (state = 'COMPLETE')
        = (
            release_id IS NOT NULL
            AND release_cutoff IS NOT NULL
            AND completed_at IS NOT NULL
        )
    )
) WITH (fillfactor = 80);

CREATE INDEX idx_rt_portal_publication_work_due
    ON rt.portal_publication_work (
        cohort_id,
        next_attempt_at,
        cutoff,
        work_id
    )
    WHERE state IN ('PENDING', 'RETRY', 'CLAIMED');

CREATE INDEX idx_rt_portal_publication_work_status
    ON rt.portal_publication_work (
        cohort_id,
        product_key,
        cutoff DESC,
        created_at DESC
    );

CREATE TABLE rt.portal_publication_receipts (
    publication_receipt_id uuid PRIMARY KEY,
    work_id uuid NOT NULL REFERENCES rt.portal_publication_work(work_id),
    attempt integer NOT NULL,
    owner_id uuid NOT NULL,
    owner_epoch bigint NOT NULL,
    payload_sha256 char(64) NOT NULL,
    outcome varchar(32) NOT NULL,
    failure_reason varchar(160),
    retry_at timestamptz,
    release_id uuid,
    release_cutoff timestamptz,
    previous_release_id uuid,
    current_release_id uuid,
    recorded_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (work_id, attempt),
    FOREIGN KEY (release_id, release_cutoff)
        REFERENCES rt.portal_releases(release_id, cutoff),
    CHECK (attempt > 0 AND owner_epoch > 0),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (
        outcome IN (
            'CURRENT',
            'EQUIVALENT_CURRENT',
            'HISTORICAL_STALE',
            'HISTORICAL_NONCURRENT',
            'FAILED_RETRY',
            'FAILED_TERMINAL'
        )
    ),
    CHECK (
        (outcome IN ('FAILED_RETRY', 'FAILED_TERMINAL'))
        = (failure_reason IS NOT NULL)
    ),
    CHECK (
        (outcome = 'FAILED_RETRY') = (retry_at IS NOT NULL)
    )
);

CREATE INDEX idx_rt_portal_publication_receipt_work
    ON rt.portal_publication_receipts (
        work_id,
        attempt DESC
    );

CREATE TRIGGER trg_rt_portal_publication_receipts_immutable
    BEFORE UPDATE OR DELETE ON rt.portal_publication_receipts
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();

CREATE TRIGGER trg_rt_portal_publication_receipts_no_truncate
    BEFORE TRUNCATE ON rt.portal_publication_receipts
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
