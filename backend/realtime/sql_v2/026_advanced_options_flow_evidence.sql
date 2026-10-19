SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.option_flow_contract_scopes (
    contract_scope_sha256 char(64) PRIMARY KEY,
    scope_version varchar(64) NOT NULL,
    session_date date NOT NULL,
    manifest_sha256 char(64) NOT NULL
        REFERENCES rt.subscription_manifests(manifest_sha256),
    slot_sha256 char(64) NOT NULL,
    advanced_flow_policy_sha256 char(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (
        contract_scope_sha256,
        advanced_flow_policy_sha256
    ),
    UNIQUE (
        contract_scope_sha256,
        advanced_flow_policy_sha256,
        manifest_sha256,
        slot_sha256
    ),
    CHECK (
        scope_version = 'advanced-options-contract-scope-v1'
        AND contract_scope_sha256 ~ '^[0-9a-f]{64}$'
        AND manifest_sha256 ~ '^[0-9a-f]{64}$'
        AND slot_sha256 ~ '^[0-9a-f]{64}$'
        AND advanced_flow_policy_sha256 ~ '^[0-9a-f]{64}$'
    )
);

CREATE TABLE rt.option_flow_contract_scope_members (
    contract_scope_sha256 char(64) NOT NULL
        REFERENCES rt.option_flow_contract_scopes(contract_scope_sha256),
    contract_ticker varchar(64) NOT NULL,
    underlyer varchar(32) NOT NULL,
    expiration date NOT NULL,
    universe_tier varchar(32) NOT NULL,
    dte_lane varchar(16) NOT NULL,
    PRIMARY KEY (contract_scope_sha256, contract_ticker),
    CHECK (
        contract_ticker = upper(contract_ticker)
        AND btrim(contract_ticker) <> ''
        AND underlyer = upper(underlyer)
        AND btrim(underlyer) <> ''
        AND universe_tier IN (
            'ETF',
            'CORE_MEGA_CAP',
            'HIGH_BETA_HIGH_PREMIUM'
        )
        AND dte_lane IN ('ULTRA_SHORT', 'CORE_SWING', 'POSITION')
    )
);

CREATE TRIGGER trg_rt_option_flow_contract_scopes_immutable
    BEFORE UPDATE OR DELETE ON rt.option_flow_contract_scopes
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_flow_contract_scopes_no_truncate
    BEFORE TRUNCATE ON rt.option_flow_contract_scopes
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_flow_contract_scope_members_immutable
    BEFORE UPDATE OR DELETE ON rt.option_flow_contract_scope_members
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_flow_contract_scope_members_no_truncate
    BEFORE TRUNCATE ON rt.option_flow_contract_scope_members
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE rt.option_source_flows
    ADD COLUMN identity_version smallint NOT NULL DEFAULT 2,
    ADD COLUMN advanced_flow_policy_sha256 char(64),
    ADD COLUMN contract_scope_sha256 char(64),
    ADD CONSTRAINT option_source_flows_scope_fkey
    FOREIGN KEY (
        contract_scope_sha256,
        advanced_flow_policy_sha256
    ) REFERENCES rt.option_flow_contract_scopes (
        contract_scope_sha256,
        advanced_flow_policy_sha256
    ),
    ADD CONSTRAINT option_source_flows_advanced_identity_unique
    UNIQUE (
        source_flow_id,
        advanced_flow_policy_sha256,
        contract_scope_sha256
    ),
    ADD CONSTRAINT option_source_flows_identity_version_check
    CHECK (
        (
            identity_version = 2
            AND advanced_flow_policy_sha256 IS NULL
            AND contract_scope_sha256 IS NULL
        )
        OR (
            identity_version = 3
            AND advanced_flow_policy_sha256 ~ '^[0-9a-f]{64}$'
            AND contract_scope_sha256 ~ '^[0-9a-f]{64}$'
        )
    );

ALTER TABLE rt.option_flow_events
    DROP CONSTRAINT option_flow_events_lineage_check,
    ADD COLUMN contract_expiration date,
    ADD COLUMN flow_universe_tier varchar(32),
    ADD COLUMN flow_dte_lane varchar(16),
    ADD COLUMN advanced_flow_policy_sha256 char(64),
    ADD COLUMN contract_scope_sha256 char(64),
    ADD COLUMN manifest_sha256 char(64),
    ADD COLUMN slot_sha256 char(64),
    ADD COLUMN runtime_policy_sha256 char(64),
    ADD CONSTRAINT option_flow_events_advanced_source_fkey
    FOREIGN KEY (
        source_flow_id,
        advanced_flow_policy_sha256,
        contract_scope_sha256
    ) REFERENCES rt.option_source_flows (
        source_flow_id,
        advanced_flow_policy_sha256,
        contract_scope_sha256
    ),
    ADD CONSTRAINT option_flow_events_scope_fkey
    FOREIGN KEY (
        contract_scope_sha256,
        advanced_flow_policy_sha256,
        manifest_sha256,
        slot_sha256
    ) REFERENCES rt.option_flow_contract_scopes (
        contract_scope_sha256,
        advanced_flow_policy_sha256,
        manifest_sha256,
        slot_sha256
    ),
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
        )
        OR (
            lineage_version = 3
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
    WHERE lineage_version IN (2, 3);

CREATE TABLE rt.option_flow_event_profiles (
    event_id uuid NOT NULL,
    event_time timestamptz NOT NULL,
    session_date date NOT NULL,
    profile varchar(16) NOT NULL,
    universe_tier varchar(32) NOT NULL,
    dte_lane varchar(16) NOT NULL,
    threshold_mode varchar(8) NOT NULL,
    minimum_contracts bigint NOT NULL,
    minimum_print_premium numeric(28,8) NOT NULL,
    contracts_matched boolean NOT NULL,
    premium_matched boolean NOT NULL,
    minimum_cluster_prints integer,
    minimum_distinct_exchanges integer,
    minimum_cluster_premium numeric(28,8),
    minimum_cumulative_contracts bigint,
    window_seconds integer,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (event_id, event_time, profile),
    FOREIGN KEY (event_id, event_time)
        REFERENCES rt.option_flow_events(event_id, event_time),
    CHECK (
        profile IN ('ULTRA_SHORT', 'BLOCK', 'SWEEP', 'ACCUMULATION')
        AND universe_tier IN (
            'ETF',
            'CORE_MEGA_CAP',
            'HIGH_BETA_HIGH_PREMIUM'
        )
        AND threshold_mode = 'OR'
        AND minimum_contracts > 0
        AND minimum_print_premium > 0
        AND (contracts_matched OR premium_matched)
        AND (
            (profile = 'ULTRA_SHORT' AND dte_lane = 'ULTRA_SHORT')
            OR (
                profile = 'BLOCK'
                AND dte_lane IN ('CORE_SWING', 'POSITION')
            )
            OR (profile = 'SWEEP' AND dte_lane = 'CORE_SWING')
            OR (
                profile = 'ACCUMULATION'
                AND dte_lane IN ('CORE_SWING', 'POSITION')
            )
        )
        AND (
            (
                profile = 'BLOCK'
                AND minimum_cluster_prints IS NULL
                AND minimum_distinct_exchanges IS NULL
                AND minimum_cluster_premium IS NULL
                AND minimum_cumulative_contracts IS NULL
                AND window_seconds IS NULL
            )
            OR (
                profile <> 'BLOCK'
                AND minimum_cluster_prints > 0
                AND minimum_distinct_exchanges > 0
                AND minimum_cluster_premium > 0
                AND window_seconds > 0
                AND (
                    (
                        profile IN ('ULTRA_SHORT', 'ACCUMULATION')
                        AND minimum_cumulative_contracts > 0
                    )
                    OR (
                        profile = 'SWEEP'
                        AND minimum_cumulative_contracts IS NULL
                    )
                )
            )
        )
    )
) PARTITION BY RANGE (event_time);

CREATE INDEX idx_rt_option_flow_event_profile_session
    ON rt.option_flow_event_profiles (
        session_date,
        profile,
        event_time DESC
    );
CREATE TRIGGER trg_rt_option_flow_event_profiles_immutable
    BEFORE UPDATE OR DELETE ON rt.option_flow_event_profiles
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_flow_event_profiles_no_truncate
    BEFORE TRUNCATE ON rt.option_flow_event_profiles
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE rt.option_flow_clusters
    DROP CONSTRAINT option_flow_clusters_source_flow_fkey,
    DROP CONSTRAINT option_flow_clusters_lineage_check,
    ADD COLUMN flow_policy_sha256 char(64),
    ADD COLUMN profile varchar(16),
    ADD COLUMN flow_universe_tier varchar(32),
    ADD COLUMN flow_dte_lane varchar(16),
    ADD COLUMN threshold_mode varchar(8),
    ADD COLUMN minimum_contracts bigint,
    ADD COLUMN minimum_print_premium numeric(28,8),
    ADD COLUMN contracts_matched boolean,
    ADD COLUMN premium_matched boolean,
    ADD COLUMN minimum_cluster_prints integer,
    ADD COLUMN minimum_distinct_exchanges integer,
    ADD COLUMN minimum_cluster_premium numeric(28,8),
    ADD COLUMN minimum_cumulative_contracts bigint,
    ADD COLUMN cumulative_contracts bigint,
    ADD COLUMN qualifies boolean,
    ADD COLUMN advanced_flow_policy_sha256 char(64),
    ADD COLUMN contract_scope_sha256 char(64),
    ADD COLUMN manifest_sha256 char(64),
    ADD COLUMN slot_sha256 char(64),
    ADD COLUMN runtime_policy_sha256 char(64),
    ADD CONSTRAINT option_flow_clusters_source_flow_fkey
    FOREIGN KEY (
        source_flow_id,
        condition_semantics_sha256,
        flow_policy_sha256
    ) REFERENCES rt.option_source_flows (
        source_flow_id,
        condition_semantics_sha256,
        flow_policy_sha256
    ),
    ADD CONSTRAINT option_flow_clusters_advanced_source_fkey
    FOREIGN KEY (
        source_flow_id,
        advanced_flow_policy_sha256,
        contract_scope_sha256
    ) REFERENCES rt.option_source_flows (
        source_flow_id,
        advanced_flow_policy_sha256,
        contract_scope_sha256
    ),
    ADD CONSTRAINT option_flow_clusters_scope_fkey
    FOREIGN KEY (
        contract_scope_sha256,
        advanced_flow_policy_sha256,
        manifest_sha256,
        slot_sha256
    ) REFERENCES rt.option_flow_contract_scopes (
        contract_scope_sha256,
        advanced_flow_policy_sha256,
        manifest_sha256,
        slot_sha256
    ),
    ADD CONSTRAINT option_flow_clusters_profile_unique
    UNIQUE (cluster_id, revision, session_date, profile),
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
        )
        OR (
            lineage_version = 3
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
        )
    );

CREATE TABLE rt.option_flow_cluster_members (
    cluster_id uuid NOT NULL,
    revision integer NOT NULL,
    session_date date NOT NULL,
    profile varchar(16) NOT NULL,
    event_id uuid NOT NULL,
    event_time timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (
        cluster_id,
        revision,
        session_date,
        profile,
        event_id,
        event_time
    ),
    FOREIGN KEY (
        cluster_id,
        revision,
        session_date,
        profile
    ) REFERENCES rt.option_flow_clusters (
        cluster_id,
        revision,
        session_date,
        profile
    ),
    FOREIGN KEY (event_id, event_time, profile)
        REFERENCES rt.option_flow_event_profiles (
            event_id,
            event_time,
            profile
        )
) PARTITION BY RANGE (session_date);

CREATE INDEX idx_rt_option_flow_cluster_member_event
    ON rt.option_flow_cluster_members (event_id, event_time, profile);
CREATE TRIGGER trg_rt_option_flow_cluster_members_immutable
    BEFORE UPDATE OR DELETE ON rt.option_flow_cluster_members
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_flow_cluster_members_no_truncate
    BEFORE TRUNCATE ON rt.option_flow_cluster_members
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();

ALTER TABLE rt.detector_datasets
    ADD COLUMN flow_identity_version smallint NOT NULL DEFAULT 1,
    ADD COLUMN source_flow_id uuid,
    ADD COLUMN advanced_flow_policy_sha256 char(64),
    ADD COLUMN contract_scope_sha256 char(64),
    ADD COLUMN slot_sha256 char(64),
    ADD COLUMN runtime_policy_sha256 char(64),
    ADD CONSTRAINT detector_datasets_advanced_source_fkey
    FOREIGN KEY (
        source_flow_id,
        advanced_flow_policy_sha256,
        contract_scope_sha256
    ) REFERENCES rt.option_source_flows (
        source_flow_id,
        advanced_flow_policy_sha256,
        contract_scope_sha256
    ),
    ADD CONSTRAINT detector_datasets_scope_fkey
    FOREIGN KEY (
        contract_scope_sha256,
        advanced_flow_policy_sha256,
        source_manifest_sha256,
        slot_sha256
    ) REFERENCES rt.option_flow_contract_scopes (
        contract_scope_sha256,
        advanced_flow_policy_sha256,
        manifest_sha256,
        slot_sha256
    ),
    ADD CONSTRAINT detector_datasets_flow_identity_check
    CHECK (
        (
            flow_identity_version = 1
            AND source_flow_id IS NULL
            AND advanced_flow_policy_sha256 IS NULL
            AND contract_scope_sha256 IS NULL
            AND slot_sha256 IS NULL
            AND runtime_policy_sha256 IS NULL
        )
        OR (
            flow_identity_version = 2
            AND source_flow_id IS NOT NULL
            AND advanced_flow_policy_sha256 ~ '^[0-9a-f]{64}$'
            AND contract_scope_sha256 ~ '^[0-9a-f]{64}$'
            AND slot_sha256 ~ '^[0-9a-f]{64}$'
            AND runtime_policy_sha256 ~ '^[0-9a-f]{64}$'
        )
    );

INSERT INTO rt.partition_policies (
    relation_name,
    scheme,
    key_column,
    key_type,
    retention_class,
    window_kind,
    window_value,
    pre_create_days,
    lookback_days,
    requires_archive,
    archive_kinds,
    catalog_relation
) VALUES
    ('rt.option_flow_event_profiles', 'WEEKLY', 'event_time', 'timestamptz', 'ARCHIVE_FIRST', 'SESSIONS', 26, 14, 28, true, ARRAY['OPTION_FLOW_EVENTS'], NULL),
    ('rt.option_flow_cluster_members', 'WEEKLY', 'session_date', 'date', 'ARCHIVE_FIRST', 'SESSIONS', 26, 14, 28, true, ARRAY['OPTION_FLOW_CLUSTERS'], NULL);

SELECT rt.ensure_partitions(
    'rt.option_flow_event_profiles',
    CURRENT_DATE
);
SELECT rt.ensure_partitions(
    'rt.option_flow_cluster_members',
    CURRENT_DATE
);
