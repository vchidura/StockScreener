SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE rt.developer_acquisition_batches (
    acquisition_batch_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    fact_kind varchar(32) NOT NULL,
    fact_schema_version varchar(96) NOT NULL,
    provider varchar(64) NOT NULL,
    provider_batch_id uuid NOT NULL,
    started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    fact_count integer NOT NULL,
    batch_payload_sha256 char(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (
        cohort_id, fact_kind, fact_schema_version, provider, provider_batch_id
    ),
    CHECK (provider_tier = 'MASSIVE_DEVELOPER'),
    CHECK (
        (fact_kind = 'EQUITY_BAR' AND fact_schema_version = 'developer_equity_bar_v1')
        OR (
            fact_kind = 'OPTION_SNAPSHOT'
            AND fact_schema_version = 'developer_option_snapshot_v1'
        )
    ),
    CHECK (btrim(provider) <> ''),
    CHECK (completed_at >= started_at),
    CHECK (fact_count BETWEEN 1 AND 1000),
    CHECK (batch_payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX idx_rt_developer_acquisition_batch_completed
    ON rt.developer_acquisition_batches (
        cohort_id, fact_kind, completed_at, acquisition_batch_id
    );

CREATE TABLE rt.developer_acquisition_facts (
    acquisition_batch_id uuid NOT NULL
        REFERENCES rt.developer_acquisition_batches(acquisition_batch_id),
    fact_index integer NOT NULL,
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    provider_revision_id uuid NOT NULL,
    market_time timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    raw_payload_sha256 char(64) NOT NULL,
    normalized_payload_sha256 char(64) NOT NULL,
    structured_fact jsonb NOT NULL,
    PRIMARY KEY (acquisition_batch_id, fact_index),
    UNIQUE (acquisition_batch_id, instrument_id, provider_revision_id),
    CHECK (fact_index BETWEEN 0 AND 999),
    CHECK (market_time <= observed_at),
    CHECK (raw_payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (normalized_payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(structured_fact) = 'object')
);

CREATE TRIGGER trg_rt_developer_acquisition_batches_immutable BEFORE UPDATE OR DELETE ON rt.developer_acquisition_batches FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_developer_acquisition_batches_no_truncate BEFORE TRUNCATE ON rt.developer_acquisition_batches FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_developer_acquisition_facts_immutable BEFORE UPDATE OR DELETE ON rt.developer_acquisition_facts FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_developer_acquisition_facts_no_truncate BEFORE TRUNCATE ON rt.developer_acquisition_facts FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();