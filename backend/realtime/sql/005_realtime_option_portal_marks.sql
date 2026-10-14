SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE rt.provider_tier_cohorts (
    cohort_id uuid PRIMARY KEY REFERENCES rt.cohorts(cohort_id),
    profile_id uuid NOT NULL REFERENCES rt.capability_profiles(profile_id),
    provider_tier varchar(24) NOT NULL,
    stocks_tier varchar(24) NOT NULL,
    options_tier varchar(24) NOT NULL,
    ready_state varchar(40) NOT NULL,
    source_manifest_sha256 char(64) NOT NULL,
    valuation_policy_sha256 char(64) NOT NULL,
    capabilities text[] NOT NULL,
    disabled_detectors text[] NOT NULL,
    effective_at timestamptz NOT NULL,
    binding_sha256 char(64) NOT NULL UNIQUE,
    UNIQUE (cohort_id, provider_tier),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (stocks_tier = provider_tier AND options_tier = provider_tier),
    CHECK (
        (provider_tier = 'MASSIVE_ADVANCED' AND ready_state = 'ADVANCED_READY')
        OR (provider_tier = 'MASSIVE_DEVELOPER' AND ready_state = 'DEVELOPER_READY')
    ),
    CHECK (source_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (valuation_policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (binding_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (cardinality(capabilities) > 0)
);

CREATE TABLE rt.provider_tier_activations (
    activation_id uuid PRIMARY KEY,
    previous_cohort_id uuid REFERENCES rt.provider_tier_cohorts(cohort_id),
    target_cohort_id uuid NOT NULL REFERENCES rt.provider_tier_cohorts(cohort_id),
    provider_tier varchar(24) NOT NULL,
    transition_ids uuid[] NOT NULL,
    activated_at timestamptz NOT NULL,
    approval_reference text NOT NULL,
    dependencies_warm boolean NOT NULL,
    activation_sha256 char(64) NOT NULL UNIQUE,
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (btrim(approval_reference) <> ''),
    CHECK (dependencies_warm),
    CHECK (activation_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (previous_cohort_id IS NULL OR previous_cohort_id <> target_cohort_id)
);

CREATE TABLE rt.portal_cohort_current (
    scope varchar(24) PRIMARY KEY,
    cohort_id uuid NOT NULL,
    provider_tier varchar(24) NOT NULL,
    activation_id uuid NOT NULL REFERENCES rt.provider_tier_activations(activation_id),
    effective_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (cohort_id, provider_tier)
        REFERENCES rt.provider_tier_cohorts(cohort_id, provider_tier),
    CHECK (scope = 'PLATFORM'),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER'))
) WITH (fillfactor = 80);

CREATE TABLE rt.option_packages (
    candidate_id uuid PRIMARY KEY,
    cohort_id uuid NOT NULL REFERENCES rt.cohorts(cohort_id),
    event_id uuid,
    matrix_id uuid NOT NULL,
    candidate_identity_sha256 char(64) NOT NULL UNIQUE,
    provider_tier varchar(24) NOT NULL,
    entry_market_time timestamptz NOT NULL,
    entry_net_premium numeric(28,8) NOT NULL,
    capital_at_risk numeric(28,8) NOT NULL,
    exit_deadline timestamptz,
    valuation_policy_sha256 char(64) NOT NULL,
    leg_count smallint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (cohort_id, provider_tier)
        REFERENCES rt.provider_tier_cohorts(cohort_id, provider_tier),
    UNIQUE (candidate_id, provider_tier),
    CHECK (candidate_identity_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (valuation_policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (entry_net_premium <> 0 AND capital_at_risk > 0),
    CHECK (leg_count BETWEEN 1 AND 8),
    CHECK (exit_deadline IS NULL OR exit_deadline > entry_market_time)
);

CREATE TABLE rt.option_package_legs (
    candidate_id uuid NOT NULL REFERENCES rt.option_packages(candidate_id),
    leg_index smallint NOT NULL,
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    contract_id bigint NOT NULL REFERENCES ref.option_contracts(contract_id),
    catalog_version_id bigint NOT NULL REFERENCES ref.option_contract_versions(version_id),
    contract_ticker varchar(64) NOT NULL,
    side varchar(4) NOT NULL,
    ratio integer NOT NULL,
    multiplier numeric(20,8) NOT NULL,
    strike numeric(20,8) NOT NULL,
    expiration_date date NOT NULL,
    contract_type varchar(4) NOT NULL,
    entry_mark numeric(20,8) NOT NULL,
    entry_market_time timestamptz NOT NULL,
    valuation_policy_sha256 char(64) NOT NULL,
    PRIMARY KEY (candidate_id, leg_index),
    UNIQUE (candidate_id, instrument_id),
    UNIQUE (candidate_id, contract_id),
    CHECK (leg_index BETWEEN 0 AND 7),
    CHECK (contract_ticker = upper(contract_ticker) AND btrim(contract_ticker) <> ''),
    CHECK (side IN ('BUY', 'SELL')),
    CHECK (ratio > 0 AND multiplier > 0 AND strike > 0 AND entry_mark > 0),
    CHECK (contract_type IN ('CALL', 'PUT')),
    CHECK (valuation_policy_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE rt.option_mark_evidence (
    evidence_id uuid PRIMARY KEY,
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    contract_id bigint NOT NULL REFERENCES ref.option_contracts(contract_id),
    catalog_version_id bigint NOT NULL REFERENCES ref.option_contract_versions(version_id),
    provider_tier varchar(24) NOT NULL,
    source_kind varchar(32) NOT NULL,
    mark_source varchar(48) NOT NULL,
    source_batch_id uuid,
    source_revision_id uuid,
    journal_generation uuid,
    stream_session_id uuid REFERENCES rt.stream_sessions(session_id),
    source_offset bigint,
    market_time timestamptz NOT NULL,
    observed_time timestamptz NOT NULL,
    bid numeric(20,8),
    ask numeric(20,8),
    mark numeric(20,8) NOT NULL,
    payload_sha256 char(64) NOT NULL,
    CHECK (market_time <= observed_time),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (source_kind IN ('ADVANCED_NBBO', 'DEVELOPER_ALIGNED_AGGREGATE')),
    CHECK (mark_source IN ('ADVANCED_NBBO_MIDPOINT', 'DEVELOPER_ALIGNED_AGG_CLOSE')),
    CHECK (mark > 0),
    CHECK (
        (
            provider_tier = 'MASSIVE_ADVANCED'
            AND source_kind = 'ADVANCED_NBBO'
            AND mark_source = 'ADVANCED_NBBO_MIDPOINT'
            AND source_batch_id IS NULL AND source_revision_id IS NULL
            AND journal_generation IS NOT NULL AND stream_session_id IS NOT NULL
            AND source_offset > 0
            AND bid > 0 AND ask >= bid AND mark BETWEEN bid AND ask
        ) OR (
            provider_tier = 'MASSIVE_DEVELOPER'
            AND source_kind = 'DEVELOPER_ALIGNED_AGGREGATE'
            AND mark_source = 'DEVELOPER_ALIGNED_AGG_CLOSE'
            AND source_batch_id IS NOT NULL AND source_revision_id IS NOT NULL
            AND journal_generation IS NULL AND stream_session_id IS NULL
            AND source_offset IS NULL AND bid IS NULL AND ask IS NULL
        )
    ),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE UNIQUE INDEX uq_rt_option_mark_evidence_advanced
    ON rt.option_mark_evidence (journal_generation, source_offset)
    WHERE source_kind = 'ADVANCED_NBBO';
CREATE UNIQUE INDEX uq_rt_option_mark_evidence_developer
    ON rt.option_mark_evidence (source_batch_id, source_revision_id)
    WHERE source_kind = 'DEVELOPER_ALIGNED_AGGREGATE';

CREATE TABLE rt.option_package_mark_revisions (
    mark_id uuid PRIMARY KEY,
    candidate_id uuid NOT NULL REFERENCES rt.option_packages(candidate_id),
    event_id uuid,
    provider_tier varchar(24) NOT NULL,
    valuation_policy_version varchar(96) NOT NULL,
    valuation_policy_sha256 char(64) NOT NULL,
    adapter_version varchar(96) NOT NULL,
    computed_at timestamptz NOT NULL,
    market_time timestamptz NOT NULL,
    observed_time timestamptz NOT NULL,
    entry_net_premium numeric(28,8) NOT NULL,
    exit_net_premium numeric(28,8) NOT NULL,
    gross_pnl numeric(28,8) NOT NULL,
    estimated_cost numeric(28,8) NOT NULL,
    net_pnl numeric(28,8) NOT NULL,
    capital_at_risk numeric(28,8) NOT NULL,
    net_return numeric(24,12) NOT NULL,
    availability_flag varchar(40) NOT NULL,
    quality_flags text[] NOT NULL,
    evidence_batch_id uuid NOT NULL,
    evidence_count smallint NOT NULL,
    management_status varchar(24),
    FOREIGN KEY (candidate_id, provider_tier)
        REFERENCES rt.option_packages(candidate_id, provider_tier),
    UNIQUE (candidate_id, valuation_policy_sha256, evidence_batch_id),
    CHECK (btrim(valuation_policy_version) <> '' AND btrim(adapter_version) <> ''),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (valuation_policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (market_time <= observed_time AND observed_time <= computed_at),
    CHECK (entry_net_premium <> 0 AND estimated_cost >= 0 AND capital_at_risk > 0),
    CHECK (evidence_count BETWEEN 1 AND 8),
    CHECK (cardinality(quality_flags) > 0),
    CHECK (management_status IS NULL OR management_status IN (
        'MONITORING', 'TARGET_MET', 'STOP_LOSS_HIT', 'EXPIRED'
    ))
);

CREATE TABLE rt.option_package_mark_inputs (
    mark_id uuid NOT NULL REFERENCES rt.option_package_mark_revisions(mark_id),
    leg_index smallint NOT NULL,
    evidence_id uuid NOT NULL REFERENCES rt.option_mark_evidence(evidence_id),
    PRIMARY KEY (mark_id, leg_index),
    UNIQUE (mark_id, evidence_id),
    CHECK (leg_index BETWEEN 0 AND 7)
);

CREATE TABLE rt.option_package_mark_current (
    candidate_id uuid NOT NULL REFERENCES rt.option_packages(candidate_id),
    valuation_policy_sha256 char(64) NOT NULL,
    mark_id uuid NOT NULL REFERENCES rt.option_package_mark_revisions(mark_id),
    market_time timestamptz NOT NULL,
    observed_time timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (candidate_id, valuation_policy_sha256),
    CHECK (valuation_policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (market_time <= observed_time)
) WITH (fillfactor = 80);

CREATE INDEX idx_rt_option_mark_evidence_instrument_time
    ON rt.option_mark_evidence (instrument_id, market_time DESC, evidence_id);
CREATE INDEX idx_rt_option_package_mark_revision_candidate_time
    ON rt.option_package_mark_revisions
        (candidate_id, valuation_policy_sha256, market_time DESC, observed_time DESC);

    CREATE TRIGGER trg_rt_provider_tier_cohorts_immutable BEFORE UPDATE OR DELETE ON rt.provider_tier_cohorts FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
    CREATE TRIGGER trg_rt_provider_tier_cohorts_no_truncate BEFORE TRUNCATE ON rt.provider_tier_cohorts FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
    CREATE TRIGGER trg_rt_provider_tier_activations_immutable BEFORE UPDATE OR DELETE ON rt.provider_tier_activations FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
    CREATE TRIGGER trg_rt_provider_tier_activations_no_truncate BEFORE TRUNCATE ON rt.provider_tier_activations FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_packages_immutable BEFORE UPDATE OR DELETE ON rt.option_packages FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_packages_no_truncate BEFORE TRUNCATE ON rt.option_packages FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_package_legs_immutable BEFORE UPDATE OR DELETE ON rt.option_package_legs FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_package_legs_no_truncate BEFORE TRUNCATE ON rt.option_package_legs FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_mark_evidence_immutable BEFORE UPDATE OR DELETE ON rt.option_mark_evidence FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_mark_evidence_no_truncate BEFORE TRUNCATE ON rt.option_mark_evidence FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_package_mark_revisions_immutable BEFORE UPDATE OR DELETE ON rt.option_package_mark_revisions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_package_mark_revisions_no_truncate BEFORE TRUNCATE ON rt.option_package_mark_revisions FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_package_mark_inputs_immutable BEFORE UPDATE OR DELETE ON rt.option_package_mark_inputs FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_package_mark_inputs_no_truncate BEFORE TRUNCATE ON rt.option_package_mark_inputs FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();