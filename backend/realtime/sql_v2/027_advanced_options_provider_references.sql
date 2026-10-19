SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.option_flow_policies (
    policy_sha256 char(64) PRIMARY KEY,
    policy_version varchar(64) NOT NULL UNIQUE,
    policy_document jsonb NOT NULL,
    provider_contract_sha256 char(64) NOT NULL,
    source_path text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        policy_sha256 ~ '^[0-9a-f]{64}$'
        AND provider_contract_sha256 ~ '^[0-9a-f]{64}$'
        AND jsonb_typeof(policy_document) = 'object'
        AND btrim(source_path) <> ''
    )
);
CREATE TRIGGER trg_rt_option_flow_policies_immutable
    BEFORE UPDATE OR DELETE ON rt.option_flow_policies
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_flow_policies_no_truncate
    BEFORE TRUNCATE ON rt.option_flow_policies
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE rt.option_flow_contract_scopes
    ADD CONSTRAINT option_flow_contract_scopes_policy_fkey
    FOREIGN KEY (advanced_flow_policy_sha256)
    REFERENCES rt.option_flow_policies(policy_sha256)
    NOT VALID;

ALTER TABLE ref.trade_condition_codes
    ADD COLUMN provider_contract_sha256 char(64),
    ADD CONSTRAINT trade_condition_codes_provider_contract_check
    CHECK (
        provider_contract_sha256 IS NULL
        OR provider_contract_sha256 ~ '^[0-9a-f]{64}$'
    );

ALTER TABLE ref.exchange_codes
    ADD COLUMN acronym varchar(32),
    ADD COLUMN locale varchar(16),
    ADD COLUMN operating_mic varchar(16),
    ADD COLUMN participant_id varchar(16),
    ADD COLUMN url text,
    ADD COLUMN provider_contract_sha256 char(64),
    ADD COLUMN exchange_reference_sha256 char(64),
    ADD CONSTRAINT exchange_codes_reference_check
    CHECK (
        (
            provider_contract_sha256 IS NULL
            AND exchange_reference_sha256 IS NULL
        )
        OR (
            provider_contract_sha256 ~ '^[0-9a-f]{64}$'
            AND exchange_reference_sha256 ~ '^[0-9a-f]{64}$'
            AND locale = 'us'
        )
    );

ALTER TABLE rt.option_source_flows
    DROP CONSTRAINT option_source_flows_identity_version_check,
    ADD COLUMN provider_contract_sha256 char(64),
    ADD COLUMN condition_reference_sha256 char(64),
    ADD COLUMN exchange_reference_sha256 char(64),
    ADD CONSTRAINT option_source_flows_identity_version_check
    CHECK (
        (
            identity_version = 2
            AND advanced_flow_policy_sha256 IS NULL
            AND contract_scope_sha256 IS NULL
            AND provider_contract_sha256 IS NULL
            AND condition_reference_sha256 IS NULL
            AND exchange_reference_sha256 IS NULL
        )
        OR (
            identity_version = 3
            AND advanced_flow_policy_sha256 ~ '^[0-9a-f]{64}$'
            AND contract_scope_sha256 ~ '^[0-9a-f]{64}$'
            AND provider_contract_sha256 IS NULL
            AND condition_reference_sha256 IS NULL
            AND exchange_reference_sha256 IS NULL
        )
        OR (
            identity_version = 4
            AND advanced_flow_policy_sha256 ~ '^[0-9a-f]{64}$'
            AND contract_scope_sha256 ~ '^[0-9a-f]{64}$'
            AND provider_contract_sha256 ~ '^[0-9a-f]{64}$'
            AND condition_reference_sha256 ~ '^[0-9a-f]{64}$'
            AND exchange_reference_sha256 ~ '^[0-9a-f]{64}$'
        )
    );

ALTER TABLE rt.option_flow_events
    DROP CONSTRAINT option_flow_events_lineage_check,
    ADD COLUMN provider_contract_sha256 char(64),
    ADD COLUMN condition_reference_sha256 char(64),
    ADD COLUMN exchange_reference_sha256 char(64),
    ADD CONSTRAINT option_flow_events_lineage_check
    CHECK (
        (
            lineage_version = 1
            AND source_flow_id IS NULL
            AND condition_semantics_sha256 IS NULL
            AND flow_policy_sha256 IS NULL
            AND advanced_flow_policy_sha256 IS NULL
            AND contract_scope_sha256 IS NULL
        )
        OR (
            lineage_version = 2
            AND journal_generation IS NOT NULL
            AND source_flow_id IS NOT NULL
            AND condition_semantics_sha256 ~ '^[0-9a-f]{64}$'
            AND flow_policy_sha256 ~ '^[0-9a-f]{64}$'
            AND advanced_flow_policy_sha256 IS NULL
            AND contract_scope_sha256 IS NULL
            AND provider_contract_sha256 IS NULL
        )
        OR (
            lineage_version IN (3, 4)
            AND journal_generation IS NOT NULL
            AND source_flow_id IS NOT NULL
            AND condition_semantics_sha256 ~ '^[0-9a-f]{64}$'
            AND flow_policy_sha256 ~ '^[0-9a-f]{64}$'
            AND advanced_flow_policy_sha256 ~ '^[0-9a-f]{64}$'
            AND contract_scope_sha256 ~ '^[0-9a-f]{64}$'
            AND manifest_sha256 ~ '^[0-9a-f]{64}$'
            AND slot_sha256 ~ '^[0-9a-f]{64}$'
            AND runtime_policy_sha256 ~ '^[0-9a-f]{64}$'
            AND contract_expiration IS NOT NULL
            AND flow_universe_tier IN (
                'ETF',
                'CORE_MEGA_CAP',
                'HIGH_BETA_HIGH_PREMIUM'
            )
            AND flow_dte_lane IN (
                'ULTRA_SHORT',
                'CORE_SWING',
                'POSITION'
            )
            AND (
                (
                    lineage_version = 3
                    AND provider_contract_sha256 IS NULL
                    AND condition_reference_sha256 IS NULL
                    AND exchange_reference_sha256 IS NULL
                )
                OR (
                    lineage_version = 4
                    AND provider_contract_sha256 ~ '^[0-9a-f]{64}$'
                    AND condition_reference_sha256 ~ '^[0-9a-f]{64}$'
                    AND exchange_reference_sha256 ~ '^[0-9a-f]{64}$'
                )
            )
        )
    );

DROP INDEX rt.uq_rt_option_flow_event_source_policy;
CREATE UNIQUE INDEX uq_rt_option_flow_event_source_policy
    ON rt.option_flow_events (
        source_flow_id,
        contract_ticker,
        source_seq,
        event_time,
        revision
    )
    WHERE lineage_version IN (2, 3, 4);

ALTER TABLE rt.option_flow_clusters
    DROP CONSTRAINT option_flow_clusters_lineage_check,
    ADD COLUMN provider_contract_sha256 char(64),
    ADD COLUMN condition_reference_sha256 char(64),
    ADD COLUMN exchange_reference_sha256 char(64),
    ADD CONSTRAINT option_flow_clusters_lineage_check
    CHECK (
        (
            lineage_version = 1
            AND source_flow_id IS NULL
            AND condition_semantics_sha256 IS NULL
            AND flow_policy_sha256 IS NULL
            AND profile IS NULL
        )
        OR (
            lineage_version = 2
            AND source_flow_id IS NOT NULL
            AND condition_semantics_sha256 ~ '^[0-9a-f]{64}$'
            AND flow_policy_sha256 IS NULL
            AND profile IS NULL
            AND provider_contract_sha256 IS NULL
        )
        OR (
            lineage_version IN (3, 4)
            AND source_flow_id IS NOT NULL
            AND condition_semantics_sha256 ~ '^[0-9a-f]{64}$'
            AND flow_policy_sha256 ~ '^[0-9a-f]{64}$'
            AND profile IN ('ULTRA_SHORT', 'SWEEP', 'ACCUMULATION')
            AND flow_universe_tier IN (
                'ETF',
                'CORE_MEGA_CAP',
                'HIGH_BETA_HIGH_PREMIUM'
            )
            AND threshold_mode = 'OR'
            AND minimum_contracts > 0
            AND minimum_print_premium > 0
            AND (contracts_matched OR premium_matched)
            AND minimum_cluster_prints > 0
            AND minimum_distinct_exchanges > 0
            AND minimum_cluster_premium > 0
            AND cumulative_contracts >= 0
            AND qualifies IS NOT NULL
            AND advanced_flow_policy_sha256 ~ '^[0-9a-f]{64}$'
            AND contract_scope_sha256 ~ '^[0-9a-f]{64}$'
            AND manifest_sha256 ~ '^[0-9a-f]{64}$'
            AND slot_sha256 ~ '^[0-9a-f]{64}$'
            AND runtime_policy_sha256 = policy_sha256
            AND (
                (
                    profile = 'ULTRA_SHORT'
                    AND flow_dte_lane = 'ULTRA_SHORT'
                    AND minimum_cumulative_contracts > 0
                )
                OR (
                    profile = 'SWEEP'
                    AND flow_dte_lane = 'CORE_SWING'
                    AND minimum_cumulative_contracts IS NULL
                )
                OR (
                    profile = 'ACCUMULATION'
                    AND flow_dte_lane IN ('CORE_SWING', 'POSITION')
                    AND minimum_cumulative_contracts > 0
                )
            )
            AND (status <> 'RETRACTED' OR NOT qualifies)
            AND (
                (
                    lineage_version = 3
                    AND provider_contract_sha256 IS NULL
                    AND condition_reference_sha256 IS NULL
                    AND exchange_reference_sha256 IS NULL
                )
                OR (
                    lineage_version = 4
                    AND provider_contract_sha256 ~ '^[0-9a-f]{64}$'
                    AND condition_reference_sha256 ~ '^[0-9a-f]{64}$'
                    AND exchange_reference_sha256 ~ '^[0-9a-f]{64}$'
                )
            )
        )
    );

ALTER TABLE rt.detector_datasets
    ADD COLUMN provider_contract_sha256 char(64),
    ADD COLUMN condition_reference_sha256 char(64),
    ADD COLUMN exchange_reference_sha256 char(64),
    ADD CONSTRAINT detector_datasets_provider_reference_check
    CHECK (
        (
            provider_contract_sha256 IS NULL
            AND condition_reference_sha256 IS NULL
            AND exchange_reference_sha256 IS NULL
        )
        OR (
            provider_contract_sha256 ~ '^[0-9a-f]{64}$'
            AND condition_reference_sha256 ~ '^[0-9a-f]{64}$'
            AND exchange_reference_sha256 ~ '^[0-9a-f]{64}$'
        )
    );
