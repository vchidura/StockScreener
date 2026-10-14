SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE rt.option_manifest_selection_audits (
    selection_id uuid PRIMARY KEY,
    selection_sha256 char(64) NOT NULL UNIQUE,
    manifest_id uuid REFERENCES rt.subscription_manifests(manifest_id),
    profile_id uuid NOT NULL REFERENCES rt.capability_profiles(profile_id),
    provider_tier varchar(24) NOT NULL,
    universe_sha256 char(64) NOT NULL,
    policy_sha256 char(64) NOT NULL,
    source_evidence_sha256 char(64) NOT NULL,
    session_date date NOT NULL,
    status varchar(16) NOT NULL,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    selected_contract_count integer NOT NULL,
    excluded_contract_count integer NOT NULL,
    coverage_count integer NOT NULL,
    primary_option_quote_contracts integer NOT NULL,
    reserved_option_quote_contracts integer NOT NULL,
    maximum_option_quote_contracts integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (profile_id, session_date, policy_sha256, source_evidence_sha256),
    CHECK (selection_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (provider_tier IN ('MASSIVE_ADVANCED', 'MASSIVE_DEVELOPER')),
    CHECK (universe_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (source_evidence_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (status IN ('READY', 'BLOCKED')),
    CHECK (
        (status = 'READY' AND manifest_id IS NOT NULL AND cardinality(reason_codes) = 0)
        OR
        (status = 'BLOCKED' AND manifest_id IS NULL AND cardinality(reason_codes) > 0)
    ),
    CHECK (selected_contract_count >= 0),
    CHECK (excluded_contract_count >= 0),
    CHECK (coverage_count >= 0),
    CHECK (primary_option_quote_contracts > 0),
    CHECK (reserved_option_quote_contracts >= 0),
    CHECK (maximum_option_quote_contracts BETWEEN 1 AND 1000),
    CHECK (
        primary_option_quote_contracts + reserved_option_quote_contracts
        = maximum_option_quote_contracts
    )
);

CREATE INDEX idx_rt_option_manifest_selection_session
    ON rt.option_manifest_selection_audits (
        session_date DESC, provider_tier, selection_id
    );

CREATE TABLE rt.option_manifest_contract_dispositions (
    selection_id uuid NOT NULL
        REFERENCES rt.option_manifest_selection_audits(selection_id),
    contract_id bigint NOT NULL REFERENCES ref.option_contracts(contract_id),
    catalog_version_id bigint NOT NULL
        REFERENCES ref.option_contract_versions(version_id),
    contract_ticker varchar(64) NOT NULL,
    underlyer varchar(32) NOT NULL,
    expiration_date date NOT NULL,
    calendar_dte integer NOT NULL,
    call_put char(1) NOT NULL,
    strike numeric(20,8) NOT NULL,
    day_volume bigint,
    open_interest bigint,
    disposition varchar(12) NOT NULL,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    PRIMARY KEY (selection_id, contract_id),
    UNIQUE (selection_id, catalog_version_id),
    CHECK (contract_ticker = upper(contract_ticker)),
    CHECK (underlyer = upper(underlyer) AND btrim(underlyer) <> ''),
    CHECK (calendar_dte >= 0),
    CHECK (call_put IN ('C', 'P')),
    CHECK (strike > 0),
    CHECK (day_volume IS NULL OR day_volume >= 0),
    CHECK (open_interest IS NULL OR open_interest >= 0),
    CHECK (disposition IN ('INCLUDED', 'EXCLUDED')),
    CHECK (
        (disposition = 'INCLUDED' AND cardinality(reason_codes) = 0)
        OR
        (disposition = 'EXCLUDED' AND cardinality(reason_codes) > 0)
    )
);

CREATE INDEX idx_rt_option_manifest_disposition_underlying
    ON rt.option_manifest_contract_dispositions (
        selection_id, underlyer, expiration_date, call_put, strike
    );

CREATE TABLE rt.option_manifest_coverage (
    selection_id uuid NOT NULL
        REFERENCES rt.option_manifest_selection_audits(selection_id),
    underlying_security_id uuid NOT NULL REFERENCES ref.securities(security_id),
    underlyer varchar(32) NOT NULL,
    target_dte integer NOT NULL,
    call_put char(1) NOT NULL,
    expiration_date date,
    selected_count integer NOT NULL,
    required_count integer NOT NULL,
    status varchar(16) NOT NULL,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    PRIMARY KEY (selection_id, underlying_security_id, target_dte, call_put),
    CHECK (underlyer = upper(underlyer) AND btrim(underlyer) <> ''),
    CHECK (target_dte > 0),
    CHECK (call_put IN ('C', 'P')),
    CHECK (selected_count >= 0 AND required_count > 0),
    CHECK (selected_count <= required_count),
    CHECK (status IN ('READY', 'UNAVAILABLE')),
    CHECK (
        (
            status = 'READY'
            AND expiration_date IS NOT NULL
            AND selected_count = required_count
            AND cardinality(reason_codes) = 0
        )
        OR
        (
            status = 'UNAVAILABLE'
            AND expiration_date IS NULL
            AND selected_count < required_count
            AND cardinality(reason_codes) > 0
        )
    )
);

CREATE TRIGGER trg_rt_option_manifest_selection_audits_immutable BEFORE UPDATE OR DELETE ON rt.option_manifest_selection_audits FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_manifest_selection_audits_no_truncate BEFORE TRUNCATE ON rt.option_manifest_selection_audits FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_manifest_contract_dispositions_immutable BEFORE UPDATE OR DELETE ON rt.option_manifest_contract_dispositions FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_manifest_contract_dispositions_no_truncate BEFORE TRUNCATE ON rt.option_manifest_contract_dispositions FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_manifest_coverage_immutable BEFORE UPDATE OR DELETE ON rt.option_manifest_coverage FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_manifest_coverage_no_truncate BEFORE TRUNCATE ON rt.option_manifest_coverage FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();