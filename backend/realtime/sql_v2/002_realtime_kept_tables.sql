-- Generated 2026-10-12 from the verified v1 final schema (pg_dump 17.11) by
-- backend/scripts/_generate_v2_kept_baseline.py. These tables keep their v1 definition.
-- Edit by hand only before the V2 freeze point.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';


CREATE TABLE ref.corporate_action_coverage (
    coverage_id uuid NOT NULL,
    security_id uuid NOT NULL,
    action_type character varying(24) NOT NULL,
    window_start date NOT NULL,
    window_end date NOT NULL,
    source character varying(64) NOT NULL,
    source_key character varying(256) NOT NULL,
    response_action_count integer NOT NULL,
    response_sha256 character(64) NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    received_at timestamp with time zone NOT NULL,
    payload_sha256 character(64) NOT NULL,
    CONSTRAINT corporate_action_coverage_action_type_check CHECK (((action_type)::text = ANY ((ARRAY['SPLIT'::character varying, 'DIVIDEND'::character varying])::text[]))),
    CONSTRAINT corporate_action_coverage_check CHECK ((window_end >= window_start)),
    CONSTRAINT corporate_action_coverage_check1 CHECK ((received_at >= observed_at)),
    CONSTRAINT corporate_action_coverage_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT corporate_action_coverage_response_action_count_check CHECK ((response_action_count >= 0)),
    CONSTRAINT corporate_action_coverage_response_sha256_check CHECK ((response_sha256 ~ '^[0-9a-f]{64}$'::text))
);

ALTER TABLE ONLY ref.corporate_action_coverage
    ADD CONSTRAINT corporate_action_coverage_pkey PRIMARY KEY (coverage_id);

ALTER TABLE ONLY ref.corporate_action_coverage
    ADD CONSTRAINT corporate_action_coverage_source_source_key_observed_at_pay_key UNIQUE (source, source_key, observed_at, payload_sha256);


CREATE TABLE ref.corporate_action_revisions (
    action_id uuid NOT NULL,
    security_id uuid NOT NULL,
    ticker character varying(32) NOT NULL,
    action_type character varying(24) NOT NULL,
    effective_date date NOT NULL,
    declaration_date date,
    ex_date date,
    record_date date,
    pay_date date,
    cash_amount numeric(28,8),
    split_from numeric(28,8),
    split_to numeric(28,8),
    new_ticker character varying(32),
    source character varying(64) NOT NULL,
    source_key character varying(256) NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    received_at timestamp with time zone NOT NULL,
    payload_sha256 character(64) NOT NULL,
    raw_payload jsonb NOT NULL,
    supersedes_action_id uuid,
    CONSTRAINT corporate_action_revisions_action_type_check CHECK (((action_type)::text = ANY ((ARRAY['SPLIT'::character varying, 'DIVIDEND'::character varying, 'SYMBOL_CHANGE'::character varying, 'SPINOFF'::character varying, 'MERGER'::character varying, 'OTHER'::character varying])::text[]))),
    CONSTRAINT corporate_action_revisions_cash_amount_check CHECK (((cash_amount IS NULL) OR (cash_amount > (0)::numeric))),
    CONSTRAINT corporate_action_revisions_check CHECK (((split_from IS NULL) = (split_to IS NULL))),
    CONSTRAINT corporate_action_revisions_check1 CHECK (((split_from IS NULL) OR ((split_from > (0)::numeric) AND (split_to > (0)::numeric)))),
    CONSTRAINT corporate_action_revisions_check2 CHECK ((received_at >= observed_at)),
    CONSTRAINT corporate_action_revisions_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT corporate_action_revisions_raw_payload_check CHECK ((jsonb_typeof(raw_payload) = 'object'::text)),
    CONSTRAINT corporate_action_revisions_ticker_check CHECK ((((ticker)::text = upper((ticker)::text)) AND (btrim((ticker)::text) <> ''::text)))
);

ALTER TABLE ONLY ref.corporate_action_revisions
    ADD CONSTRAINT corporate_action_revisions_pkey PRIMARY KEY (action_id);

ALTER TABLE ONLY ref.corporate_action_revisions
    ADD CONSTRAINT corporate_action_revisions_source_source_key_observed_at_pa_key UNIQUE (source, source_key, observed_at, payload_sha256);


CREATE TABLE ref.fundamental_report_coverage (
    coverage_id uuid NOT NULL,
    security_id uuid NOT NULL,
    window_start date NOT NULL,
    window_end date NOT NULL,
    source character varying(64) NOT NULL,
    report_count integer NOT NULL,
    response_sha256 character(64) NOT NULL,
    checked_at timestamp with time zone NOT NULL,
    valid_until timestamp with time zone NOT NULL,
    CONSTRAINT fundamental_report_coverage_check CHECK ((window_end >= window_start)),
    CONSTRAINT fundamental_report_coverage_check1 CHECK ((valid_until > checked_at)),
    CONSTRAINT fundamental_report_coverage_report_count_check CHECK ((report_count >= 0)),
    CONSTRAINT fundamental_report_coverage_response_sha256_check CHECK ((response_sha256 ~ '^[0-9a-f]{64}$'::text))
);

ALTER TABLE ONLY ref.fundamental_report_coverage
    ADD CONSTRAINT fundamental_report_coverage_pkey PRIMARY KEY (coverage_id);

ALTER TABLE ONLY ref.fundamental_report_coverage
    ADD CONSTRAINT fundamental_report_coverage_security_id_window_start_window_key UNIQUE (security_id, window_start, window_end, source, checked_at);


CREATE TABLE ref.fundamental_reports (
    report_id uuid NOT NULL,
    security_id uuid NOT NULL,
    security_revision_id uuid,
    cik character varying(32),
    accession_number character varying(64),
    form_type character varying(32),
    timeframe character varying(32) NOT NULL,
    fiscal_year integer,
    fiscal_quarter integer,
    period_end date NOT NULL,
    filing_date date,
    availability_time timestamp with time zone NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    source character varying(64) NOT NULL,
    source_key character varying(256) NOT NULL,
    metrics jsonb NOT NULL,
    raw_payload jsonb NOT NULL,
    payload_sha256 character(64) NOT NULL,
    quality_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    CONSTRAINT fundamental_reports_check CHECK ((observed_at >= availability_time)),
    CONSTRAINT fundamental_reports_fiscal_quarter_check CHECK (((fiscal_quarter IS NULL) OR ((fiscal_quarter >= 1) AND (fiscal_quarter <= 4)))),
    CONSTRAINT fundamental_reports_metrics_check CHECK ((jsonb_typeof(metrics) = 'object'::text)),
    CONSTRAINT fundamental_reports_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT fundamental_reports_raw_payload_check CHECK ((jsonb_typeof(raw_payload) = 'object'::text)),
    CONSTRAINT fundamental_reports_timeframe_check CHECK (((timeframe)::text = ANY ((ARRAY['quarterly'::character varying, 'annual'::character varying, 'trailing_twelve_months'::character varying])::text[])))
);

ALTER TABLE ONLY ref.fundamental_reports
    ADD CONSTRAINT fundamental_reports_pkey PRIMARY KEY (report_id);

ALTER TABLE ONLY ref.fundamental_reports
    ADD CONSTRAINT fundamental_reports_source_source_key_payload_sha256_key UNIQUE (source, source_key, payload_sha256);


CREATE TABLE ref.market_event_coverage (
    coverage_id uuid NOT NULL,
    security_id uuid,
    event_type character varying(32) NOT NULL,
    window_start date NOT NULL,
    window_end date NOT NULL,
    source character varying(64) NOT NULL,
    verdict character varying(32) NOT NULL,
    checked_at timestamp with time zone NOT NULL,
    valid_until timestamp with time zone NOT NULL,
    evidence_sha256 character(64) NOT NULL,
    CONSTRAINT market_event_coverage_check CHECK ((((event_type)::text = 'EARNINGS'::text) = (security_id IS NOT NULL))),
    CONSTRAINT market_event_coverage_check1 CHECK ((window_end >= window_start)),
    CONSTRAINT market_event_coverage_check2 CHECK ((valid_until > checked_at)),
    CONSTRAINT market_event_coverage_event_type_check CHECK (((event_type)::text = ANY ((ARRAY['EARNINGS'::character varying, 'FED_RATE_DECISION'::character varying])::text[]))),
    CONSTRAINT market_event_coverage_evidence_sha256_check CHECK ((evidence_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT market_event_coverage_verdict_check CHECK (((verdict)::text = ANY ((ARRAY['COVERED'::character varying, 'KNOWN_NO_EVENT'::character varying, 'NOT_APPLICABLE'::character varying, 'UNAVAILABLE'::character varying])::text[])))
);

ALTER TABLE ONLY ref.market_event_coverage
    ADD CONSTRAINT market_event_coverage_pkey PRIMARY KEY (coverage_id);

ALTER TABLE ONLY ref.market_event_coverage
    ADD CONSTRAINT market_event_coverage_source_event_type_security_id_window__key UNIQUE (source, event_type, security_id, window_start, window_end, checked_at);


CREATE TABLE ref.market_event_revisions (
    revision_id uuid NOT NULL,
    security_id uuid,
    event_key character varying(256) NOT NULL,
    event_type character varying(32) NOT NULL,
    event_date date NOT NULL,
    announced_at timestamp with time zone,
    event_timezone character varying(64) NOT NULL,
    time_precision character varying(16) NOT NULL,
    status character varying(16) NOT NULL,
    source character varying(64) NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    received_at timestamp with time zone NOT NULL,
    valid_until timestamp with time zone NOT NULL,
    payload_sha256 character(64) NOT NULL,
    supersedes_revision_id uuid,
    CONSTRAINT market_event_revisions_check CHECK ((((event_type)::text = 'EARNINGS'::text) = (security_id IS NOT NULL))),
    CONSTRAINT market_event_revisions_check1 CHECK ((received_at >= observed_at)),
    CONSTRAINT market_event_revisions_check2 CHECK ((valid_until > received_at)),
    CONSTRAINT market_event_revisions_event_type_check CHECK (((event_type)::text = ANY ((ARRAY['EARNINGS'::character varying, 'FED_RATE_DECISION'::character varying])::text[]))),
    CONSTRAINT market_event_revisions_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT market_event_revisions_status_check CHECK (((status)::text = ANY ((ARRAY['SCHEDULED'::character varying, 'COMPLETED'::character varying, 'CANCELED'::character varying, 'REVISED'::character varying])::text[]))),
    CONSTRAINT market_event_revisions_time_precision_check CHECK (((time_precision)::text = ANY ((ARRAY['CONFIRMED'::character varying, 'ESTIMATED'::character varying, 'DATE_ONLY'::character varying, 'UNKNOWN'::character varying])::text[])))
);

ALTER TABLE ONLY ref.market_event_revisions
    ADD CONSTRAINT market_event_revisions_pkey PRIMARY KEY (revision_id);

ALTER TABLE ONLY ref.market_event_revisions
    ADD CONSTRAINT market_event_revisions_source_event_key_observed_at_payload_key UNIQUE (source, event_key, observed_at, payload_sha256);


CREATE TABLE ref.oi_baselines (
    baseline_id uuid NOT NULL,
    contract_id bigint NOT NULL,
    settlement_session date NOT NULL,
    open_interest bigint NOT NULL,
    source character varying(64) NOT NULL,
    date_basis character varying(64) NOT NULL,
    observed_session date NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    received_at timestamp with time zone NOT NULL,
    payload_sha256 character(64) NOT NULL,
    supersedes_baseline_id uuid,
    CONSTRAINT oi_baselines_check CHECK ((settlement_session < observed_session)),
    CONSTRAINT oi_baselines_check1 CHECK ((received_at >= observed_at)),
    CONSTRAINT oi_baselines_open_interest_check CHECK ((open_interest >= 0)),
    CONSTRAINT oi_baselines_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text))
);

ALTER TABLE ONLY ref.oi_baselines
    ADD CONSTRAINT oi_baselines_pkey PRIMARY KEY (baseline_id);

ALTER TABLE ONLY ref.oi_baselines
    ADD CONSTRAINT oi_baselines_source_contract_id_settlement_session_observed_key UNIQUE (source, contract_id, settlement_session, observed_at, payload_sha256);


CREATE TABLE ref.option_contract_versions (
    version_id bigint NOT NULL,
    contract_id bigint NOT NULL,
    call_put character(1) NOT NULL,
    expiration date NOT NULL,
    strike numeric(20,8) NOT NULL,
    exercise_style character varying(16) NOT NULL,
    multiplier numeric(20,8) NOT NULL,
    deliverables jsonb DEFAULT '[]'::jsonb NOT NULL,
    eligibility character varying(32) NOT NULL,
    effective_from timestamp with time zone NOT NULL,
    effective_to timestamp with time zone,
    observed_at timestamp with time zone NOT NULL,
    payload_sha256 character(64) NOT NULL,
    imported_at timestamp with time zone,
    CONSTRAINT option_contract_versions_call_put_check CHECK ((call_put = ANY (ARRAY['C'::bpchar, 'P'::bpchar]))),
    CONSTRAINT option_contract_versions_check CHECK (((strike > (0)::numeric) AND (multiplier > (0)::numeric))),
    CONSTRAINT option_contract_versions_check1 CHECK (((effective_to IS NULL) OR (effective_to > effective_from))),
    CONSTRAINT option_contract_versions_check2 CHECK ((observed_at >= effective_from)),
    CONSTRAINT option_contract_versions_check3 CHECK (((imported_at IS NULL) OR (imported_at >= observed_at))),
    CONSTRAINT option_contract_versions_deliverables_check CHECK ((jsonb_typeof(deliverables) = 'array'::text)),
    CONSTRAINT option_contract_versions_eligibility_check CHECK (((eligibility)::text = ANY ((ARRAY['VALIDATED_ACTIVE'::character varying, 'REJECTED_UNSUPPORTED'::character varying, 'EXPIRED'::character varying])::text[]))),
    CONSTRAINT option_contract_versions_exercise_style_check CHECK (((exercise_style)::text = ANY ((ARRAY['AMERICAN'::character varying, 'EUROPEAN'::character varying])::text[]))),
    CONSTRAINT option_contract_versions_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_contract_versions_version_id_check CHECK ((version_id > 0))
);

ALTER TABLE ONLY ref.option_contract_versions
    ADD CONSTRAINT option_contract_versions_contract_id_effective_from_payload_key UNIQUE (contract_id, effective_from, payload_sha256);

ALTER TABLE ONLY ref.option_contract_versions
    ADD CONSTRAINT option_contract_versions_pkey PRIMARY KEY (version_id);


CREATE TABLE ref.option_contracts (
    contract_id bigint NOT NULL,
    underlying_security_id uuid NOT NULL,
    provider character varying(32) NOT NULL,
    contract_ticker character varying(64) NOT NULL,
    first_observed_at timestamp with time zone NOT NULL,
    source_origin character varying(64) NOT NULL,
    CONSTRAINT option_contracts_check CHECK (((btrim((provider)::text) <> ''::text) AND (btrim((source_origin)::text) <> ''::text))),
    CONSTRAINT option_contracts_contract_id_check CHECK ((contract_id > 0)),
    CONSTRAINT option_contracts_contract_ticker_check CHECK (((contract_ticker)::text = upper((contract_ticker)::text)))
);

ALTER TABLE ONLY ref.option_contracts
    ADD CONSTRAINT option_contracts_pkey PRIMARY KEY (contract_id);

ALTER TABLE ONLY ref.option_contracts
    ADD CONSTRAINT option_contracts_provider_contract_ticker_key UNIQUE (provider, contract_ticker);


CREATE TABLE ref.risk_free_rate_observations (
    observation_id uuid NOT NULL,
    rate_date date NOT NULL,
    tenor_days integer NOT NULL,
    annual_rate numeric(12,10) NOT NULL,
    source character varying(64) NOT NULL,
    source_key character varying(128) NOT NULL,
    source_observed_at timestamp with time zone,
    received_at timestamp with time zone NOT NULL,
    payload_sha256 character(64) NOT NULL,
    CONSTRAINT risk_free_rate_observations_annual_rate_check CHECK (((annual_rate > '-0.10'::numeric) AND (annual_rate < 1.0))),
    CONSTRAINT risk_free_rate_observations_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT risk_free_rate_observations_tenor_days_check CHECK ((tenor_days > 0))
);

ALTER TABLE ONLY ref.risk_free_rate_observations
    ADD CONSTRAINT risk_free_rate_observations_pkey PRIMARY KEY (observation_id);

ALTER TABLE ONLY ref.risk_free_rate_observations
    ADD CONSTRAINT risk_free_rate_observations_source_source_key_received_at_key UNIQUE (source, source_key, received_at);


CREATE TABLE ref.securities (
    security_id uuid NOT NULL,
    asset_type character varying(12) NOT NULL,
    source_origin character varying(64) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT securities_asset_type_check CHECK (((asset_type)::text = ANY ((ARRAY['STOCK'::character varying, 'ETF'::character varying, 'INDEX'::character varying])::text[]))),
    CONSTRAINT securities_source_origin_check CHECK ((btrim((source_origin)::text) <> ''::text))
);

ALTER TABLE ONLY ref.securities
    ADD CONSTRAINT securities_pkey PRIMARY KEY (security_id);


CREATE TABLE ref.security_revisions (
    revision_id uuid NOT NULL,
    security_id uuid NOT NULL,
    ticker character varying(32) NOT NULL,
    exchange character varying(32),
    calendar_id character varying(64) NOT NULL,
    effective_from timestamp with time zone NOT NULL,
    effective_to timestamp with time zone,
    observed_at timestamp with time zone NOT NULL,
    payload_sha256 character(64) NOT NULL,
    imported_at timestamp with time zone,
    company_name text,
    security_type character varying(16),
    active boolean,
    cik character varying(32),
    composite_figi character varying(32),
    share_class_figi character varying(32),
    sic_code character varying(16),
    sic_description text,
    sector text,
    industry text,
    list_date date,
    delisted_date date,
    weighted_shares numeric(28,8),
    free_float numeric(28,8),
    free_float_percent double precision,
    market_cap numeric(28,8),
    CONSTRAINT ck_ref_security_delisted_date CHECK (((delisted_date IS NULL) OR (list_date IS NULL) OR (delisted_date >= list_date))),
    CONSTRAINT ck_ref_security_free_float CHECK (((free_float IS NULL) OR (free_float >= (0)::numeric))),
    CONSTRAINT ck_ref_security_free_float_percent CHECK (((free_float_percent IS NULL) OR ((free_float_percent >= (0)::double precision) AND (free_float_percent <= (100)::double precision)))),
    CONSTRAINT ck_ref_security_market_cap CHECK (((market_cap IS NULL) OR (market_cap >= (0)::numeric))),
    CONSTRAINT ck_ref_security_type_nonblank CHECK (((security_type IS NULL) OR (btrim((security_type)::text) <> ''::text))),
    CONSTRAINT ck_ref_security_weighted_shares CHECK (((weighted_shares IS NULL) OR (weighted_shares >= (0)::numeric))),
    CONSTRAINT security_revisions_calendar_id_check CHECK ((btrim((calendar_id)::text) <> ''::text)),
    CONSTRAINT security_revisions_check CHECK (((effective_to IS NULL) OR (effective_to > effective_from))),
    CONSTRAINT security_revisions_check1 CHECK ((observed_at >= effective_from)),
    CONSTRAINT security_revisions_check2 CHECK (((imported_at IS NULL) OR (imported_at >= observed_at))),
    CONSTRAINT security_revisions_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT security_revisions_ticker_check CHECK ((((ticker)::text = upper((ticker)::text)) AND (btrim((ticker)::text) <> ''::text)))
);

ALTER TABLE ONLY ref.security_revisions
    ADD CONSTRAINT security_revisions_pkey PRIMARY KEY (revision_id);

ALTER TABLE ONLY ref.security_revisions
    ADD CONSTRAINT security_revisions_security_id_effective_from_payload_sha25_key UNIQUE (security_id, effective_from, payload_sha256);


CREATE TABLE rt.alert_current (
    alert_id uuid NOT NULL,
    cohort_id uuid NOT NULL,
    event_id uuid NOT NULL,
    alert_kind character varying(24) NOT NULL,
    lifecycle character varying(24) NOT NULL,
    as_of timestamp with time zone NOT NULL,
    valid_until timestamp with time zone,
    projection_schema character varying(64) NOT NULL,
    projection_sha256 character(64) NOT NULL,
    projection jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT alert_current_alert_kind_check CHECK (((alert_kind)::text = ANY ((ARRAY['STOCK'::character varying, 'OPTION'::character varying])::text[]))),
    CONSTRAINT alert_current_check CHECK (((btrim((lifecycle)::text) <> ''::text) AND (btrim((projection_schema)::text) <> ''::text))),
    CONSTRAINT alert_current_check1 CHECK (((valid_until IS NULL) OR (valid_until > as_of))),
    CONSTRAINT alert_current_projection_check CHECK ((jsonb_typeof(projection) = 'object'::text)),
    CONSTRAINT alert_current_projection_sha256_check CHECK ((projection_sha256 ~ '^[0-9a-f]{64}$'::text))
)
WITH (fillfactor='80');

ALTER TABLE ONLY rt.alert_current
    ADD CONSTRAINT alert_current_event_id_key UNIQUE (event_id);

ALTER TABLE ONLY rt.alert_current
    ADD CONSTRAINT alert_current_pkey PRIMARY KEY (alert_id);


CREATE TABLE rt.alert_management_assessments (
    assessment_id uuid NOT NULL,
    cohort_id uuid NOT NULL,
    alert_id uuid NOT NULL,
    assessed_at timestamp with time zone NOT NULL,
    status character varying(24) NOT NULL,
    reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    payload_sha256 character(64) NOT NULL,
    payload jsonb NOT NULL,
    CONSTRAINT alert_management_assessments_check CHECK (((((status)::text = 'READY'::text) AND (cardinality(reason_codes) = 0)) OR ((status)::text <> 'READY'::text))),
    CONSTRAINT alert_management_assessments_payload_check CHECK ((jsonb_typeof(payload) = 'object'::text)),
    CONSTRAINT alert_management_assessments_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT alert_management_assessments_status_check CHECK (((status)::text = ANY ((ARRAY['READY'::character varying, 'UNAVAILABLE'::character varying, 'EXPIRED'::character varying, 'TARGET'::character varying, 'STOP'::character varying])::text[])))
);

ALTER TABLE ONLY rt.alert_management_assessments
    ADD CONSTRAINT alert_management_assessments_cohort_id_alert_id_assessed_at_key UNIQUE (cohort_id, alert_id, assessed_at);

ALTER TABLE ONLY rt.alert_management_assessments
    ADD CONSTRAINT alert_management_assessments_pkey PRIMARY KEY (assessment_id);


CREATE TABLE rt.alert_management_current (
    cohort_id uuid NOT NULL,
    alert_id uuid NOT NULL,
    assessment_id uuid NOT NULL,
    assessed_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL
)
WITH (fillfactor='80');

ALTER TABLE ONLY rt.alert_management_current
    ADD CONSTRAINT alert_management_current_pkey PRIMARY KEY (cohort_id, alert_id);


CREATE TABLE rt.alert_outbox (
    outbox_id uuid NOT NULL,
    cohort_id uuid NOT NULL,
    event_id uuid NOT NULL,
    destination character varying(64) NOT NULL,
    message_kind character varying(32) NOT NULL,
    payload_sha256 character(64) NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamp with time zone NOT NULL,
    next_attempt_at timestamp with time zone NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    lease_owner uuid,
    lease_epoch bigint DEFAULT 0 NOT NULL,
    lease_until timestamp with time zone,
    delivered_at timestamp with time zone,
    failure_code character varying(64),
    abandoned_at timestamp with time zone,
    CONSTRAINT alert_outbox_check CHECK (((btrim((destination)::text) <> ''::text) AND (btrim((message_kind)::text) <> ''::text))),
    CONSTRAINT alert_outbox_check1 CHECK (((attempts >= 0) AND (lease_epoch >= 0))),
    CONSTRAINT alert_outbox_check2 CHECK (((lease_owner IS NULL) = (lease_until IS NULL))),
    CONSTRAINT alert_outbox_check3 CHECK ((num_nonnulls(delivered_at, abandoned_at) <= 1)),
    CONSTRAINT alert_outbox_check4 CHECK (((delivered_at IS NULL) OR (delivered_at >= created_at))),
    CONSTRAINT alert_outbox_check5 CHECK (((abandoned_at IS NULL) OR ((abandoned_at >= created_at) AND (failure_code IS NOT NULL)))),
    CONSTRAINT alert_outbox_check6 CHECK ((((delivered_at IS NULL) AND (abandoned_at IS NULL)) OR (lease_owner IS NULL))),
    CONSTRAINT alert_outbox_failure_code_check CHECK (((failure_code IS NULL) OR (btrim((failure_code)::text) <> ''::text))),
    CONSTRAINT alert_outbox_payload_check CHECK ((jsonb_typeof(payload) = 'object'::text)),
    CONSTRAINT alert_outbox_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text))
);

ALTER TABLE ONLY rt.alert_outbox
    ADD CONSTRAINT alert_outbox_event_id_destination_message_kind_key UNIQUE (event_id, destination, message_kind);

ALTER TABLE ONLY rt.alert_outbox
    ADD CONSTRAINT alert_outbox_pkey PRIMARY KEY (outbox_id);


CREATE TABLE rt.archive_segments (
    segment_id uuid NOT NULL,
    journal_generation uuid NOT NULL,
    shard_id smallint NOT NULL,
    first_offset bigint NOT NULL,
    last_offset bigint NOT NULL,
    row_count bigint NOT NULL,
    first_event_at timestamp with time zone NOT NULL,
    last_event_at timestamp with time zone NOT NULL,
    uri text NOT NULL,
    checksum_sha256 character(64) NOT NULL,
    schema_version character varying(64) NOT NULL,
    verified_at timestamp with time zone NOT NULL,
    CONSTRAINT archive_segments_check CHECK (((first_offset > 0) AND (last_offset >= first_offset))),
    CONSTRAINT archive_segments_check1 CHECK (((row_count > 0) AND (row_count <= ((last_offset - first_offset) + 1)))),
    CONSTRAINT archive_segments_check2 CHECK ((last_event_at >= first_event_at)),
    CONSTRAINT archive_segments_check3 CHECK (((btrim(uri) <> ''::text) AND (btrim((schema_version)::text) <> ''::text))),
    CONSTRAINT archive_segments_checksum_sha256_check CHECK ((checksum_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT archive_segments_shard_id_check CHECK ((shard_id >= 0))
);

ALTER TABLE ONLY rt.archive_segments
    ADD CONSTRAINT archive_segments_journal_generation_shard_id_first_offset_l_key UNIQUE (journal_generation, shard_id, first_offset, last_offset);

ALTER TABLE ONLY rt.archive_segments
    ADD CONSTRAINT archive_segments_pkey PRIMARY KEY (segment_id);


CREATE TABLE rt.capability_checks (
    profile_id uuid NOT NULL,
    check_name character varying(64) NOT NULL,
    scope_key character varying(160) NOT NULL,
    verdict character varying(24) NOT NULL,
    required boolean NOT NULL,
    reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    evidence jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT capability_checks_check CHECK (((btrim((check_name)::text) <> ''::text) AND (btrim((scope_key)::text) <> ''::text))),
    CONSTRAINT capability_checks_check1 CHECK ((expires_at > observed_at)),
    CONSTRAINT capability_checks_check2 CHECK (((((verdict)::text = 'PASS'::text) AND (cardinality(reason_codes) = 0)) OR ((verdict)::text <> 'PASS'::text))),
    CONSTRAINT capability_checks_check3 CHECK (((((verdict)::text <> 'PASS'::text) AND (cardinality(reason_codes) > 0)) OR ((verdict)::text = 'PASS'::text))),
    CONSTRAINT capability_checks_check4 CHECK ((((verdict)::text <> 'NOT_APPLICABLE'::text) OR (required = false))),
    CONSTRAINT capability_checks_evidence_check CHECK ((jsonb_typeof(evidence) = 'object'::text)),
    CONSTRAINT capability_checks_verdict_check CHECK (((verdict)::text = ANY ((ARRAY['PASS'::character varying, 'FAIL'::character varying, 'UNAVAILABLE'::character varying, 'EXPIRED'::character varying, 'NOT_APPLICABLE'::character varying])::text[])))
);

ALTER TABLE ONLY rt.capability_checks
    ADD CONSTRAINT capability_checks_pkey PRIMARY KEY (profile_id, check_name, scope_key);


CREATE TABLE rt.capability_profiles (
    profile_id uuid NOT NULL,
    config_sha256 character(64) NOT NULL,
    schema_sha256 character(64) NOT NULL,
    account_alias character varying(64) NOT NULL,
    capabilities_sha256 character(64) NOT NULL,
    universe_sha256 character(64) NOT NULL,
    checked_at timestamp with time zone NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    status character varying(16) NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    profile_sha256 character(64) NOT NULL,
    provider_tier character varying(24) NOT NULL,
    security_policy_sha256 character(64) NOT NULL,
    preflight_mode character varying(16) NOT NULL,
    provider_requests boolean NOT NULL,
    action_authorized boolean NOT NULL,
    CONSTRAINT capability_profiles_account_alias_check CHECK ((btrim((account_alias)::text) <> ''::text)),
    CONSTRAINT capability_profiles_capabilities_sha256_check CHECK ((capabilities_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT capability_profiles_check CHECK ((expires_at > checked_at)),
    CONSTRAINT capability_profiles_config_sha256_check CHECK ((config_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT capability_profiles_schema_sha256_check CHECK ((schema_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT capability_profiles_status_check CHECK (((status)::text = ANY ((ARRAY['PASS'::character varying, 'FAIL'::character varying, 'UNAVAILABLE'::character varying, 'EXPIRED'::character varying])::text[]))),
    CONSTRAINT capability_profiles_universe_sha256_check CHECK ((universe_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT ck_rt_capability_profile_passive_mode CHECK ((((preflight_mode)::text = 'PASSIVE'::text) AND (provider_requests = false) AND (action_authorized = false))),
    CONSTRAINT ck_rt_capability_profile_provider_tier CHECK (((provider_tier)::text = ANY ((ARRAY['MASSIVE_ADVANCED'::character varying, 'MASSIVE_DEVELOPER'::character varying])::text[]))),
    CONSTRAINT ck_rt_capability_profile_security_sha256 CHECK ((security_policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT ck_rt_capability_profile_sha256 CHECK ((profile_sha256 ~ '^[0-9a-f]{64}$'::text))
);

ALTER TABLE ONLY rt.capability_profiles
    ADD CONSTRAINT capability_profiles_config_sha256_schema_sha256_account_ali_key UNIQUE (config_sha256, schema_sha256, account_alias, capabilities_sha256, universe_sha256, checked_at);

ALTER TABLE ONLY rt.capability_profiles
    ADD CONSTRAINT capability_profiles_pkey PRIMARY KEY (profile_id);

ALTER TABLE ONLY rt.capability_profiles
    ADD CONSTRAINT capability_profiles_profile_sha256_key UNIQUE (profile_sha256);


CREATE TABLE rt.cohorts (
    cohort_id uuid NOT NULL,
    cohort_sha256 character(64) NOT NULL,
    manifest_id uuid NOT NULL,
    configuration_sha256 character(64) NOT NULL,
    model_bundle_sha256 character(64) NOT NULL,
    valuation_sha256 character(64) NOT NULL,
    effective_at timestamp with time zone NOT NULL,
    mode character varying(16) NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT cohorts_cohort_sha256_check CHECK ((cohort_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT cohorts_configuration_sha256_check CHECK ((configuration_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT cohorts_mode_check CHECK (((mode)::text = ANY ((ARRAY['SHADOW'::character varying, 'ALERT'::character varying, 'PAPER'::character varying, 'LIVE'::character varying])::text[]))),
    CONSTRAINT cohorts_model_bundle_sha256_check CHECK ((model_bundle_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT cohorts_valuation_sha256_check CHECK ((valuation_sha256 ~ '^[0-9a-f]{64}$'::text))
);

ALTER TABLE ONLY rt.cohorts
    ADD CONSTRAINT cohorts_cohort_sha256_key UNIQUE (cohort_sha256);

ALTER TABLE ONLY rt.cohorts
    ADD CONSTRAINT cohorts_pkey PRIMARY KEY (cohort_id);


CREATE TABLE rt.consumer_checkpoints (
    consumer_name character varying(96) NOT NULL,
    cohort_id uuid NOT NULL,
    journal_generation uuid NOT NULL,
    shard_id smallint NOT NULL,
    shard_map_sha256 character(64) NOT NULL,
    owner_epoch bigint NOT NULL,
    committed_offset bigint NOT NULL,
    checkpoint_id uuid NOT NULL,
    state_codec character varying(96) NOT NULL,
    state_hash character(64) NOT NULL,
    state_payload bytea NOT NULL,
    committed_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT consumer_checkpoints_check CHECK (((btrim((consumer_name)::text) <> ''::text) AND (btrim((state_codec)::text) <> ''::text))),
    CONSTRAINT consumer_checkpoints_check1 CHECK (((shard_id >= 0) AND (owner_epoch > 0) AND (committed_offset >= 0))),
    CONSTRAINT consumer_checkpoints_shard_map_sha256_check CHECK ((shard_map_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT consumer_checkpoints_state_hash_check CHECK ((state_hash ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT consumer_checkpoints_state_payload_check CHECK (((octet_length(state_payload) >= 1) AND (octet_length(state_payload) <= 1048576)))
)
WITH (fillfactor='80');

ALTER TABLE ONLY rt.consumer_checkpoints
    ADD CONSTRAINT consumer_checkpoints_checkpoint_id_key UNIQUE (checkpoint_id);

ALTER TABLE ONLY rt.consumer_checkpoints
    ADD CONSTRAINT consumer_checkpoints_pkey PRIMARY KEY (consumer_name, cohort_id, journal_generation, shard_id);


CREATE TABLE rt.detector_datasets (
    dataset_id uuid NOT NULL,
    cohort_id uuid NOT NULL,
    provider_tier character varying(24) NOT NULL,
    dataset_key text NOT NULL,
    session_date date NOT NULL,
    status character varying(16) NOT NULL,
    detector_ids text[] NOT NULL,
    source_manifest_sha256 character(64) NOT NULL,
    started_at timestamp with time zone NOT NULL,
    completed_at timestamp with time zone,
    summary jsonb NOT NULL,
    CONSTRAINT detector_datasets_check CHECK (((btrim(dataset_key) <> ''::text) AND (cardinality(detector_ids) > 0))),
    CONSTRAINT detector_datasets_check1 CHECK ((((status)::text = 'RUNNING'::text) = (completed_at IS NULL))),
    CONSTRAINT detector_datasets_check2 CHECK (((completed_at IS NULL) OR (completed_at >= started_at))),
    CONSTRAINT detector_datasets_provider_tier_check CHECK (((provider_tier)::text = ANY ((ARRAY['MASSIVE_ADVANCED'::character varying, 'MASSIVE_DEVELOPER'::character varying])::text[]))),
    CONSTRAINT detector_datasets_source_manifest_sha256_check CHECK ((source_manifest_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT detector_datasets_status_check CHECK (((status)::text = ANY ((ARRAY['RUNNING'::character varying, 'COMPLETE'::character varying, 'DEGRADED'::character varying, 'FAILED'::character varying])::text[]))),
    CONSTRAINT detector_datasets_summary_check CHECK ((jsonb_typeof(summary) = 'object'::text))
);

ALTER TABLE ONLY rt.detector_datasets
    ADD CONSTRAINT detector_datasets_cohort_id_dataset_key_key UNIQUE (cohort_id, dataset_key);

ALTER TABLE ONLY rt.detector_datasets
    ADD CONSTRAINT detector_datasets_pkey PRIMARY KEY (dataset_id);


CREATE TABLE rt.detector_evaluations (
    evaluation_id uuid NOT NULL,
    dataset_id uuid NOT NULL,
    event_id uuid NOT NULL,
    detector_id character varying(64) NOT NULL,
    underlyer character varying(32) NOT NULL,
    selection_status character varying(24) NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    payload_sha256 character(64) NOT NULL,
    payload jsonb NOT NULL,
    CONSTRAINT detector_evaluations_payload_check CHECK ((jsonb_typeof(payload) = 'object'::text)),
    CONSTRAINT detector_evaluations_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT detector_evaluations_selection_status_check CHECK (((selection_status)::text = ANY ((ARRAY['SELECTED'::character varying, 'NOT_SELECTED'::character varying, 'REPEAT'::character varying, 'OBSERVATION'::character varying])::text[]))),
    CONSTRAINT detector_evaluations_underlyer_check CHECK ((((underlyer)::text = upper((underlyer)::text)) AND (btrim((underlyer)::text) <> ''::text)))
);

ALTER TABLE ONLY rt.detector_evaluations
    ADD CONSTRAINT detector_evaluations_dataset_id_event_id_key UNIQUE (dataset_id, event_id);

ALTER TABLE ONLY rt.detector_evaluations
    ADD CONSTRAINT detector_evaluations_pkey PRIMARY KEY (evaluation_id);


CREATE TABLE rt.detector_events (
    event_id uuid NOT NULL,
    dedup_sha256 character(64) NOT NULL,
    cohort_id uuid NOT NULL,
    provider_tier character varying(24) NOT NULL,
    detector_sha256 character(64) NOT NULL,
    instrument_id bigint NOT NULL,
    trigger_identity text NOT NULL,
    trigger_at timestamp with time zone NOT NULL,
    decision_at timestamp with time zone NOT NULL,
    valid_until timestamp with time zone,
    disposition character varying(24) NOT NULL,
    direction smallint,
    reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    readiness_epoch bigint NOT NULL,
    evidence_sha256 character(64) NOT NULL,
    payload_schema character varying(64) NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT detector_events_check CHECK ((decision_at >= trigger_at)),
    CONSTRAINT detector_events_check1 CHECK (((valid_until IS NULL) OR (valid_until > trigger_at))),
    CONSTRAINT detector_events_check2 CHECK (((btrim((payload_schema)::text) <> ''::text) AND (jsonb_typeof(payload) = 'object'::text))),
    CONSTRAINT detector_events_dedup_sha256_check CHECK ((dedup_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT detector_events_direction_check CHECK (((direction IS NULL) OR (direction = ANY (ARRAY['-1'::integer, 0, 1])))),
    CONSTRAINT detector_events_evidence_sha256_check CHECK ((evidence_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT detector_events_provider_tier_check CHECK (((provider_tier)::text = ANY ((ARRAY['MASSIVE_ADVANCED'::character varying, 'MASSIVE_DEVELOPER'::character varying])::text[]))),
    CONSTRAINT detector_events_readiness_epoch_check CHECK ((readiness_epoch > 0)),
    CONSTRAINT detector_events_trigger_identity_check CHECK ((btrim(trigger_identity) <> ''::text))
);

ALTER TABLE ONLY rt.detector_events
    ADD CONSTRAINT detector_events_dedup_sha256_key UNIQUE (dedup_sha256);

ALTER TABLE ONLY rt.detector_events
    ADD CONSTRAINT detector_events_pkey PRIMARY KEY (event_id);


CREATE TABLE rt.detector_state (
    detector_sha256 character(64) NOT NULL,
    cohort_id uuid NOT NULL,
    state_key text NOT NULL,
    revision bigint NOT NULL,
    state_schema character varying(64) NOT NULL,
    state_sha256 character(64) NOT NULL,
    state_payload jsonb NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT detector_state_check CHECK (((btrim(state_key) <> ''::text) AND (btrim((state_schema)::text) <> ''::text))),
    CONSTRAINT detector_state_revision_check CHECK ((revision > 0)),
    CONSTRAINT detector_state_state_payload_check CHECK ((jsonb_typeof(state_payload) = 'object'::text)),
    CONSTRAINT detector_state_state_sha256_check CHECK ((state_sha256 ~ '^[0-9a-f]{64}$'::text))
)
WITH (fillfactor='80');

ALTER TABLE ONLY rt.detector_state
    ADD CONSTRAINT detector_state_pkey PRIMARY KEY (detector_sha256, cohort_id, state_key);


CREATE TABLE rt.detector_versions (
    detector_sha256 character(64) NOT NULL,
    detector_id character varying(64) NOT NULL,
    version character varying(96) NOT NULL,
    code_sha256 character(64) NOT NULL,
    policy_sha256 character(64) NOT NULL,
    valuation_sha256 character(64),
    requirements jsonb NOT NULL,
    output_kind character varying(32) NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT detector_versions_check CHECK (((btrim((detector_id)::text) <> ''::text) AND (btrim((version)::text) <> ''::text))),
    CONSTRAINT detector_versions_code_sha256_check CHECK ((code_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT detector_versions_detector_sha256_check CHECK ((detector_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT detector_versions_output_kind_check CHECK (((output_kind)::text = ANY ((ARRAY['OBSERVATION'::character varying, 'SIGNAL'::character varying, 'ALERT'::character varying, 'ASSESSMENT'::character varying])::text[]))),
    CONSTRAINT detector_versions_policy_sha256_check CHECK ((policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT detector_versions_requirements_check CHECK ((jsonb_typeof(requirements) = 'object'::text)),
    CONSTRAINT detector_versions_valuation_sha256_check CHECK (((valuation_sha256 IS NULL) OR (valuation_sha256 ~ '^[0-9a-f]{64}$'::text)))
);

ALTER TABLE ONLY rt.detector_versions
    ADD CONSTRAINT detector_versions_pkey PRIMARY KEY (detector_sha256);


CREATE TABLE rt.feature_definitions (
    bundle_sha256 character(64) NOT NULL,
    bundle_version character varying(96) NOT NULL,
    feature_schema_version character varying(64) NOT NULL,
    dependency_spec jsonb NOT NULL,
    policy_sha256 character(64) NOT NULL,
    code_sha256 character(64) NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT feature_definitions_bundle_sha256_check CHECK ((bundle_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT feature_definitions_check CHECK (((btrim((bundle_version)::text) <> ''::text) AND (btrim((feature_schema_version)::text) <> ''::text))),
    CONSTRAINT feature_definitions_code_sha256_check CHECK ((code_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT feature_definitions_dependency_spec_check CHECK ((jsonb_typeof(dependency_spec) = 'object'::text)),
    CONSTRAINT feature_definitions_policy_sha256_check CHECK ((policy_sha256 ~ '^[0-9a-f]{64}$'::text))
);

ALTER TABLE ONLY rt.feature_definitions
    ADD CONSTRAINT feature_definitions_pkey PRIMARY KEY (bundle_sha256);


CREATE TABLE rt.instruments (
    instrument_id bigint NOT NULL,
    asset_class character varying(12) NOT NULL,
    security_id uuid,
    contract_id bigint,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT instruments_asset_class_check CHECK (((asset_class)::text = ANY ((ARRAY['EQUITY'::character varying, 'OPTION'::character varying])::text[]))),
    CONSTRAINT instruments_check CHECK (((((asset_class)::text = 'EQUITY'::text) AND (security_id IS NOT NULL) AND (contract_id IS NULL)) OR (((asset_class)::text = 'OPTION'::text) AND (security_id IS NULL) AND (contract_id IS NOT NULL))))
);

ALTER TABLE rt.instruments ALTER COLUMN instrument_id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME rt.instruments_instrument_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

ALTER TABLE ONLY rt.instruments
    ADD CONSTRAINT instruments_pkey PRIMARY KEY (instrument_id);


CREATE TABLE rt.local_mock_dispositions (
    disposition_id uuid NOT NULL,
    request_id uuid NOT NULL,
    status character varying(24) NOT NULL,
    reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    gross_cash_flow numeric(28,8),
    estimated_fees numeric(20,8),
    net_cash_flow numeric(28,8),
    execution_permission boolean DEFAULT false NOT NULL,
    decided_at timestamp with time zone NOT NULL,
    CONSTRAINT local_mock_dispositions_check CHECK (((((status)::text = 'MOCK_FILLED'::text) AND (cardinality(reason_codes) = 0) AND (num_nonnulls(gross_cash_flow, estimated_fees, net_cash_flow) = 3)) OR (((status)::text <> 'MOCK_FILLED'::text) AND (cardinality(reason_codes) > 0) AND (num_nonnulls(gross_cash_flow, estimated_fees, net_cash_flow) = 0)))),
    CONSTRAINT local_mock_dispositions_estimated_fees_check CHECK (((estimated_fees IS NULL) OR (estimated_fees >= (0)::numeric))),
    CONSTRAINT local_mock_dispositions_execution_permission_check CHECK ((execution_permission = false)),
    CONSTRAINT local_mock_dispositions_status_check CHECK (((status)::text = ANY ((ARRAY['MOCK_FILLED'::character varying, 'MOCK_REJECTED'::character varying, 'MOCK_UNAVAILABLE'::character varying])::text[])))
);

ALTER TABLE ONLY rt.local_mock_dispositions
    ADD CONSTRAINT local_mock_dispositions_pkey PRIMARY KEY (disposition_id);

ALTER TABLE ONLY rt.local_mock_dispositions
    ADD CONSTRAINT local_mock_dispositions_request_id_key UNIQUE (request_id);


CREATE TABLE rt.local_mock_fills (
    fill_id uuid NOT NULL,
    request_id uuid NOT NULL,
    leg_index smallint NOT NULL,
    instrument_id bigint NOT NULL,
    contract_id bigint NOT NULL,
    contract_ticker character varying(64) NOT NULL,
    side character varying(8) NOT NULL,
    quantity integer NOT NULL,
    multiplier integer NOT NULL,
    bid numeric(20,8) NOT NULL,
    ask numeric(20,8) NOT NULL,
    fill_price numeric(20,8) NOT NULL,
    fee numeric(20,8) NOT NULL,
    source_evidence_id uuid NOT NULL,
    CONSTRAINT local_mock_fills_check CHECK (((quantity > 0) AND (multiplier > 0))),
    CONSTRAINT local_mock_fills_check1 CHECK (((bid > (0)::numeric) AND (ask >= bid) AND (fill_price > (0)::numeric) AND (fee >= (0)::numeric))),
    CONSTRAINT local_mock_fills_contract_ticker_check CHECK ((((contract_ticker)::text = upper((contract_ticker)::text)) AND (btrim((contract_ticker)::text) <> ''::text))),
    CONSTRAINT local_mock_fills_leg_index_check CHECK (((leg_index >= 0) AND (leg_index <= 7))),
    CONSTRAINT local_mock_fills_side_check CHECK (((side)::text = ANY ((ARRAY['BUY'::character varying, 'SELL'::character varying])::text[])))
);

ALTER TABLE ONLY rt.local_mock_fills
    ADD CONSTRAINT local_mock_fills_pkey PRIMARY KEY (fill_id);

ALTER TABLE ONLY rt.local_mock_fills
    ADD CONSTRAINT local_mock_fills_request_id_leg_index_key UNIQUE (request_id, leg_index);


CREATE TABLE rt.local_mock_request_inputs (
    request_id uuid NOT NULL,
    input_index smallint NOT NULL,
    evidence_id uuid NOT NULL,
    CONSTRAINT local_mock_request_inputs_input_index_check CHECK (((input_index >= 0) AND (input_index <= 7)))
);

ALTER TABLE ONLY rt.local_mock_request_inputs
    ADD CONSTRAINT local_mock_request_inputs_pkey PRIMARY KEY (request_id, input_index);

ALTER TABLE ONLY rt.local_mock_request_inputs
    ADD CONSTRAINT local_mock_request_inputs_request_id_evidence_id_key UNIQUE (request_id, evidence_id);


CREATE TABLE rt.local_mock_requests (
    request_id uuid NOT NULL,
    alert_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    cohort_id uuid NOT NULL,
    candidate_identity_sha256 character(64) NOT NULL,
    requested_at timestamp with time zone NOT NULL,
    request_origin character varying(24) NOT NULL,
    policy_version character varying(96) NOT NULL,
    policy_sha256 character(64) NOT NULL,
    policy jsonb NOT NULL,
    earnings_status character varying(16) NOT NULL,
    earnings_reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    evidence_batch_id uuid NOT NULL,
    evidence_count smallint NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT local_mock_requests_candidate_identity_sha256_check CHECK ((candidate_identity_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT local_mock_requests_check CHECK (((btrim((policy_version)::text) <> ''::text) AND (policy_sha256 ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT local_mock_requests_check1 CHECK (((((earnings_status)::text = 'CLEAR'::text) AND (cardinality(earnings_reason_codes) = 0)) OR (((earnings_status)::text <> 'CLEAR'::text) AND (cardinality(earnings_reason_codes) > 0)))),
    CONSTRAINT local_mock_requests_earnings_status_check CHECK (((earnings_status)::text = ANY ((ARRAY['CLEAR'::character varying, 'BLOCKED'::character varying, 'UNAVAILABLE'::character varying])::text[]))),
    CONSTRAINT local_mock_requests_evidence_count_check CHECK (((evidence_count >= 1) AND (evidence_count <= 8))),
    CONSTRAINT local_mock_requests_policy_check CHECK ((jsonb_typeof(policy) = 'object'::text)),
    CONSTRAINT local_mock_requests_request_origin_check CHECK (((request_origin)::text = 'PORTAL_MANUAL'::text))
);

ALTER TABLE ONLY rt.local_mock_requests
    ADD CONSTRAINT local_mock_requests_pkey PRIMARY KEY (request_id);


CREATE TABLE rt.option_discovery_batches (
    discovery_batch_id uuid NOT NULL,
    underlying_security_id uuid NOT NULL,
    profile_id uuid NOT NULL,
    profile_sha256 character(64) NOT NULL,
    universe_sha256 character(64) NOT NULL,
    provider_tier character varying(24) NOT NULL,
    provider character varying(64) NOT NULL,
    underlyer character varying(32) NOT NULL,
    scheduled_cycle timestamp with time zone NOT NULL,
    expiration_from date NOT NULL,
    expiration_through date NOT NULL,
    maximum_dte integer NOT NULL,
    spot_price numeric(20,8) NOT NULL,
    strike_corridor_fraction numeric(10,8) NOT NULL,
    strike_min numeric(20,8) NOT NULL,
    strike_max numeric(20,8) NOT NULL,
    request_filter_sha256 character(64) NOT NULL,
    policy_sha256 character(64) NOT NULL,
    started_at timestamp with time zone NOT NULL,
    completed_at timestamp with time zone NOT NULL,
    page_count integer NOT NULL,
    row_count integer NOT NULL,
    response_bytes bigint NOT NULL,
    evidence_sha256 character(64) NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT option_discovery_batches_check CHECK ((expiration_through >= expiration_from)),
    CONSTRAINT option_discovery_batches_check1 CHECK (((strike_min > (0)::numeric) AND (strike_max >= strike_min))),
    CONSTRAINT option_discovery_batches_check2 CHECK ((strike_min = (spot_price * ((1)::numeric - strike_corridor_fraction)))),
    CONSTRAINT option_discovery_batches_check3 CHECK ((strike_max = (spot_price * ((1)::numeric + strike_corridor_fraction)))),
    CONSTRAINT option_discovery_batches_check4 CHECK ((completed_at >= started_at)),
    CONSTRAINT option_discovery_batches_evidence_sha256_check CHECK ((evidence_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_discovery_batches_maximum_dte_check CHECK (((maximum_dte >= 1) AND (maximum_dte <= 365))),
    CONSTRAINT option_discovery_batches_page_count_check CHECK (((page_count >= 1) AND (page_count <= 40))),
    CONSTRAINT option_discovery_batches_policy_sha256_check CHECK ((policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_discovery_batches_profile_sha256_check CHECK ((profile_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_discovery_batches_provider_check CHECK ((btrim((provider)::text) <> ''::text)),
    CONSTRAINT option_discovery_batches_provider_tier_check CHECK (((provider_tier)::text = ANY ((ARRAY['MASSIVE_ADVANCED'::character varying, 'MASSIVE_DEVELOPER'::character varying])::text[]))),
    CONSTRAINT option_discovery_batches_request_filter_sha256_check CHECK ((request_filter_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_discovery_batches_response_bytes_check CHECK (((response_bytes >= 1) AND (response_bytes <= 67108864))),
    CONSTRAINT option_discovery_batches_row_count_check CHECK (((row_count >= 0) AND (row_count <= 10000))),
    CONSTRAINT option_discovery_batches_spot_price_check CHECK ((spot_price > (0)::numeric)),
    CONSTRAINT option_discovery_batches_strike_corridor_fraction_check CHECK (((strike_corridor_fraction > (0)::numeric) AND (strike_corridor_fraction < (1)::numeric))),
    CONSTRAINT option_discovery_batches_underlyer_check CHECK ((((underlyer)::text = upper((underlyer)::text)) AND (btrim((underlyer)::text) <> ''::text))),
    CONSTRAINT option_discovery_batches_universe_sha256_check CHECK ((universe_sha256 ~ '^[0-9a-f]{64}$'::text))
);

ALTER TABLE ONLY rt.option_discovery_batches
    ADD CONSTRAINT option_discovery_batches_evidence_sha256_key UNIQUE (evidence_sha256);

ALTER TABLE ONLY rt.option_discovery_batches
    ADD CONSTRAINT option_discovery_batches_pkey PRIMARY KEY (discovery_batch_id);

ALTER TABLE ONLY rt.option_discovery_batches
    ADD CONSTRAINT option_discovery_batches_profile_id_underlying_security_id__key UNIQUE (profile_id, underlying_security_id, scheduled_cycle, request_filter_sha256);


CREATE TABLE rt.option_discovery_coverage (
    discovery_batch_id uuid NOT NULL,
    dimension character varying(32) NOT NULL,
    disposition character varying(12) NOT NULL,
    comparison character varying(4) NOT NULL,
    boundary character varying(64) NOT NULL,
    reason_code character varying(64),
    CONSTRAINT option_discovery_coverage_boundary_check CHECK ((btrim((boundary)::text) <> ''::text)),
    CONSTRAINT option_discovery_coverage_check CHECK (((((disposition)::text = 'INCLUDED'::text) AND ((comparison)::text = ANY ((ARRAY['GTE'::character varying, 'LTE'::character varying])::text[]))) OR (((disposition)::text = 'EXCLUDED'::text) AND ((comparison)::text = ANY ((ARRAY['LT'::character varying, 'GT'::character varying])::text[]))))),
    CONSTRAINT option_discovery_coverage_check1 CHECK (((((disposition)::text = 'INCLUDED'::text) AND (reason_code IS NULL)) OR (((disposition)::text = 'EXCLUDED'::text) AND ((reason_code)::text = ANY ((ARRAY['OUTSIDE_DISCOVERY_WINDOW'::character varying, 'OUTSIDE_STRIKE_CORRIDOR'::character varying])::text[]))))),
    CONSTRAINT option_discovery_coverage_dimension_check CHECK (((dimension)::text = ANY ((ARRAY['EXPIRATION_DATE'::character varying, 'STRIKE_PRICE'::character varying])::text[]))),
    CONSTRAINT option_discovery_coverage_disposition_check CHECK (((disposition)::text = ANY ((ARRAY['INCLUDED'::character varying, 'EXCLUDED'::character varying])::text[])))
);

ALTER TABLE ONLY rt.option_discovery_coverage
    ADD CONSTRAINT option_discovery_coverage_pkey PRIMARY KEY (discovery_batch_id, dimension, disposition, comparison);


CREATE TABLE rt.option_manifest_contract_dispositions (
    selection_id uuid NOT NULL,
    contract_id bigint NOT NULL,
    catalog_version_id bigint NOT NULL,
    contract_ticker character varying(64) NOT NULL,
    underlyer character varying(32) NOT NULL,
    expiration_date date NOT NULL,
    calendar_dte integer NOT NULL,
    call_put character(1) NOT NULL,
    strike numeric(20,8) NOT NULL,
    day_volume bigint,
    open_interest bigint,
    disposition character varying(12) NOT NULL,
    reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    CONSTRAINT option_manifest_contract_dispositions_calendar_dte_check CHECK ((calendar_dte >= 0)),
    CONSTRAINT option_manifest_contract_dispositions_call_put_check CHECK ((call_put = ANY (ARRAY['C'::bpchar, 'P'::bpchar]))),
    CONSTRAINT option_manifest_contract_dispositions_check CHECK (((((disposition)::text = 'INCLUDED'::text) AND (cardinality(reason_codes) = 0)) OR (((disposition)::text = 'EXCLUDED'::text) AND (cardinality(reason_codes) > 0)))),
    CONSTRAINT option_manifest_contract_dispositions_contract_ticker_check CHECK (((contract_ticker)::text = upper((contract_ticker)::text))),
    CONSTRAINT option_manifest_contract_dispositions_day_volume_check CHECK (((day_volume IS NULL) OR (day_volume >= 0))),
    CONSTRAINT option_manifest_contract_dispositions_disposition_check CHECK (((disposition)::text = ANY ((ARRAY['INCLUDED'::character varying, 'EXCLUDED'::character varying])::text[]))),
    CONSTRAINT option_manifest_contract_dispositions_open_interest_check CHECK (((open_interest IS NULL) OR (open_interest >= 0))),
    CONSTRAINT option_manifest_contract_dispositions_strike_check CHECK ((strike > (0)::numeric)),
    CONSTRAINT option_manifest_contract_dispositions_underlyer_check CHECK ((((underlyer)::text = upper((underlyer)::text)) AND (btrim((underlyer)::text) <> ''::text)))
);

ALTER TABLE ONLY rt.option_manifest_contract_dispositions
    ADD CONSTRAINT option_manifest_contract_disp_selection_id_catalog_version__key UNIQUE (selection_id, catalog_version_id);

ALTER TABLE ONLY rt.option_manifest_contract_dispositions
    ADD CONSTRAINT option_manifest_contract_dispositions_pkey PRIMARY KEY (selection_id, contract_id);


CREATE TABLE rt.option_manifest_coverage (
    selection_id uuid NOT NULL,
    underlying_security_id uuid NOT NULL,
    underlyer character varying(32) NOT NULL,
    target_dte integer NOT NULL,
    call_put character(1) NOT NULL,
    expiration_date date,
    selected_count integer NOT NULL,
    required_count integer NOT NULL,
    status character varying(16) NOT NULL,
    reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    CONSTRAINT option_manifest_coverage_call_put_check CHECK ((call_put = ANY (ARRAY['C'::bpchar, 'P'::bpchar]))),
    CONSTRAINT option_manifest_coverage_check CHECK (((selected_count >= 0) AND (required_count > 0))),
    CONSTRAINT option_manifest_coverage_check1 CHECK ((selected_count <= required_count)),
    CONSTRAINT option_manifest_coverage_check2 CHECK (((((status)::text = 'READY'::text) AND (expiration_date IS NOT NULL) AND (selected_count = required_count) AND (cardinality(reason_codes) = 0)) OR (((status)::text = 'UNAVAILABLE'::text) AND (expiration_date IS NULL) AND (selected_count < required_count) AND (cardinality(reason_codes) > 0)))),
    CONSTRAINT option_manifest_coverage_status_check CHECK (((status)::text = ANY ((ARRAY['READY'::character varying, 'UNAVAILABLE'::character varying])::text[]))),
    CONSTRAINT option_manifest_coverage_target_dte_check CHECK ((target_dte > 0)),
    CONSTRAINT option_manifest_coverage_underlyer_check CHECK ((((underlyer)::text = upper((underlyer)::text)) AND (btrim((underlyer)::text) <> ''::text)))
);

ALTER TABLE ONLY rt.option_manifest_coverage
    ADD CONSTRAINT option_manifest_coverage_pkey PRIMARY KEY (selection_id, underlying_security_id, target_dte, call_put);


CREATE TABLE rt.option_manifest_selection_audits (
    selection_id uuid NOT NULL,
    selection_sha256 character(64) NOT NULL,
    manifest_id uuid,
    profile_id uuid NOT NULL,
    provider_tier character varying(24) NOT NULL,
    universe_sha256 character(64) NOT NULL,
    policy_sha256 character(64) NOT NULL,
    source_evidence_sha256 character(64) NOT NULL,
    session_date date NOT NULL,
    status character varying(16) NOT NULL,
    reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    selected_contract_count integer NOT NULL,
    excluded_contract_count integer NOT NULL,
    coverage_count integer NOT NULL,
    primary_option_quote_contracts integer NOT NULL,
    reserved_option_quote_contracts integer NOT NULL,
    maximum_option_quote_contracts integer NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT option_manifest_selection_au_maximum_option_quote_contrac_check CHECK (((maximum_option_quote_contracts >= 1) AND (maximum_option_quote_contracts <= 1000))),
    CONSTRAINT option_manifest_selection_au_primary_option_quote_contrac_check CHECK ((primary_option_quote_contracts > 0)),
    CONSTRAINT option_manifest_selection_au_reserved_option_quote_contra_check CHECK ((reserved_option_quote_contracts >= 0)),
    CONSTRAINT option_manifest_selection_audits_check CHECK (((((status)::text = 'READY'::text) AND (manifest_id IS NOT NULL) AND (cardinality(reason_codes) = 0)) OR (((status)::text = 'BLOCKED'::text) AND (manifest_id IS NULL) AND (cardinality(reason_codes) > 0)))),
    CONSTRAINT option_manifest_selection_audits_check1 CHECK (((primary_option_quote_contracts + reserved_option_quote_contracts) = maximum_option_quote_contracts)),
    CONSTRAINT option_manifest_selection_audits_coverage_count_check CHECK ((coverage_count >= 0)),
    CONSTRAINT option_manifest_selection_audits_excluded_contract_count_check CHECK ((excluded_contract_count >= 0)),
    CONSTRAINT option_manifest_selection_audits_policy_sha256_check CHECK ((policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_manifest_selection_audits_provider_tier_check CHECK (((provider_tier)::text = ANY ((ARRAY['MASSIVE_ADVANCED'::character varying, 'MASSIVE_DEVELOPER'::character varying])::text[]))),
    CONSTRAINT option_manifest_selection_audits_selected_contract_count_check CHECK ((selected_contract_count >= 0)),
    CONSTRAINT option_manifest_selection_audits_selection_sha256_check CHECK ((selection_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_manifest_selection_audits_source_evidence_sha256_check CHECK ((source_evidence_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_manifest_selection_audits_status_check CHECK (((status)::text = ANY ((ARRAY['READY'::character varying, 'BLOCKED'::character varying])::text[]))),
    CONSTRAINT option_manifest_selection_audits_universe_sha256_check CHECK ((universe_sha256 ~ '^[0-9a-f]{64}$'::text))
);

ALTER TABLE ONLY rt.option_manifest_selection_audits
    ADD CONSTRAINT option_manifest_selection_aud_profile_id_session_date_polic_key UNIQUE (profile_id, session_date, policy_sha256, source_evidence_sha256);

ALTER TABLE ONLY rt.option_manifest_selection_audits
    ADD CONSTRAINT option_manifest_selection_audits_pkey PRIMARY KEY (selection_id);

ALTER TABLE ONLY rt.option_manifest_selection_audits
    ADD CONSTRAINT option_manifest_selection_audits_selection_sha256_key UNIQUE (selection_sha256);


CREATE TABLE rt.option_mark_evidence (
    evidence_id uuid NOT NULL,
    instrument_id bigint NOT NULL,
    contract_id bigint NOT NULL,
    catalog_version_id bigint NOT NULL,
    provider_tier character varying(24) NOT NULL,
    source_kind character varying(32) NOT NULL,
    mark_source character varying(48) NOT NULL,
    source_batch_id uuid,
    source_revision_id uuid,
    journal_generation uuid,
    stream_session_id uuid,
    source_offset bigint,
    market_time timestamp with time zone NOT NULL,
    observed_time timestamp with time zone NOT NULL,
    bid numeric(20,8),
    ask numeric(20,8),
    mark numeric(20,8) NOT NULL,
    payload_sha256 character(64) NOT NULL,
    CONSTRAINT option_mark_evidence_check CHECK ((market_time <= observed_time)),
    CONSTRAINT option_mark_evidence_check1 CHECK (((((provider_tier)::text = 'MASSIVE_ADVANCED'::text) AND ((source_kind)::text = 'ADVANCED_NBBO'::text) AND ((mark_source)::text = 'ADVANCED_NBBO_MIDPOINT'::text) AND (source_batch_id IS NULL) AND (source_revision_id IS NULL) AND (journal_generation IS NOT NULL) AND (stream_session_id IS NOT NULL) AND (source_offset > 0) AND (bid > (0)::numeric) AND (ask >= bid) AND ((mark >= bid) AND (mark <= ask))) OR (((provider_tier)::text = 'MASSIVE_DEVELOPER'::text) AND ((source_kind)::text = 'DEVELOPER_ALIGNED_AGGREGATE'::text) AND ((mark_source)::text = 'DEVELOPER_ALIGNED_AGG_CLOSE'::text) AND (source_batch_id IS NOT NULL) AND (source_revision_id IS NOT NULL) AND (journal_generation IS NULL) AND (stream_session_id IS NULL) AND (source_offset IS NULL) AND (bid IS NULL) AND (ask IS NULL)))),
    CONSTRAINT option_mark_evidence_mark_check CHECK ((mark > (0)::numeric)),
    CONSTRAINT option_mark_evidence_mark_source_check CHECK (((mark_source)::text = ANY ((ARRAY['ADVANCED_NBBO_MIDPOINT'::character varying, 'DEVELOPER_ALIGNED_AGG_CLOSE'::character varying])::text[]))),
    CONSTRAINT option_mark_evidence_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_mark_evidence_provider_tier_check CHECK (((provider_tier)::text = ANY ((ARRAY['MASSIVE_ADVANCED'::character varying, 'MASSIVE_DEVELOPER'::character varying])::text[]))),
    CONSTRAINT option_mark_evidence_source_kind_check CHECK (((source_kind)::text = ANY ((ARRAY['ADVANCED_NBBO'::character varying, 'DEVELOPER_ALIGNED_AGGREGATE'::character varying])::text[])))
);

ALTER TABLE ONLY rt.option_mark_evidence
    ADD CONSTRAINT option_mark_evidence_pkey PRIMARY KEY (evidence_id);


CREATE TABLE rt.option_package_legs (
    candidate_id uuid NOT NULL,
    leg_index smallint NOT NULL,
    instrument_id bigint NOT NULL,
    contract_id bigint NOT NULL,
    catalog_version_id bigint NOT NULL,
    contract_ticker character varying(64) NOT NULL,
    side character varying(4) NOT NULL,
    ratio integer NOT NULL,
    multiplier numeric(20,8) NOT NULL,
    strike numeric(20,8) NOT NULL,
    expiration_date date NOT NULL,
    contract_type character varying(4) NOT NULL,
    entry_mark numeric(20,8) NOT NULL,
    entry_market_time timestamp with time zone NOT NULL,
    valuation_policy_sha256 character(64) NOT NULL,
    CONSTRAINT option_package_legs_check CHECK (((ratio > 0) AND (multiplier > (0)::numeric) AND (strike > (0)::numeric) AND (entry_mark > (0)::numeric))),
    CONSTRAINT option_package_legs_contract_ticker_check CHECK ((((contract_ticker)::text = upper((contract_ticker)::text)) AND (btrim((contract_ticker)::text) <> ''::text))),
    CONSTRAINT option_package_legs_contract_type_check CHECK (((contract_type)::text = ANY ((ARRAY['CALL'::character varying, 'PUT'::character varying])::text[]))),
    CONSTRAINT option_package_legs_leg_index_check CHECK (((leg_index >= 0) AND (leg_index <= 7))),
    CONSTRAINT option_package_legs_side_check CHECK (((side)::text = ANY ((ARRAY['BUY'::character varying, 'SELL'::character varying])::text[]))),
    CONSTRAINT option_package_legs_valuation_policy_sha256_check CHECK ((valuation_policy_sha256 ~ '^[0-9a-f]{64}$'::text))
);

ALTER TABLE ONLY rt.option_package_legs
    ADD CONSTRAINT option_package_legs_candidate_id_contract_id_key UNIQUE (candidate_id, contract_id);

ALTER TABLE ONLY rt.option_package_legs
    ADD CONSTRAINT option_package_legs_candidate_id_instrument_id_key UNIQUE (candidate_id, instrument_id);

ALTER TABLE ONLY rt.option_package_legs
    ADD CONSTRAINT option_package_legs_pkey PRIMARY KEY (candidate_id, leg_index);


CREATE TABLE rt.option_package_mark_current (
    candidate_id uuid NOT NULL,
    valuation_policy_sha256 character(64) NOT NULL,
    mark_id uuid NOT NULL,
    market_time timestamp with time zone NOT NULL,
    observed_time timestamp with time zone NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT option_package_mark_current_check CHECK ((market_time <= observed_time)),
    CONSTRAINT option_package_mark_current_valuation_policy_sha256_check CHECK ((valuation_policy_sha256 ~ '^[0-9a-f]{64}$'::text))
)
WITH (fillfactor='80');

ALTER TABLE ONLY rt.option_package_mark_current
    ADD CONSTRAINT option_package_mark_current_pkey PRIMARY KEY (candidate_id, valuation_policy_sha256);


CREATE TABLE rt.option_package_mark_inputs (
    mark_id uuid NOT NULL,
    leg_index smallint NOT NULL,
    evidence_id uuid NOT NULL,
    CONSTRAINT option_package_mark_inputs_leg_index_check CHECK (((leg_index >= 0) AND (leg_index <= 7)))
);

ALTER TABLE ONLY rt.option_package_mark_inputs
    ADD CONSTRAINT option_package_mark_inputs_mark_id_evidence_id_key UNIQUE (mark_id, evidence_id);

ALTER TABLE ONLY rt.option_package_mark_inputs
    ADD CONSTRAINT option_package_mark_inputs_pkey PRIMARY KEY (mark_id, leg_index);


CREATE TABLE rt.option_package_mark_revisions (
    mark_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    event_id uuid,
    provider_tier character varying(24) NOT NULL,
    valuation_policy_version character varying(96) NOT NULL,
    valuation_policy_sha256 character(64) NOT NULL,
    adapter_version character varying(96) NOT NULL,
    computed_at timestamp with time zone NOT NULL,
    market_time timestamp with time zone NOT NULL,
    observed_time timestamp with time zone NOT NULL,
    entry_net_premium numeric(28,8) NOT NULL,
    exit_net_premium numeric(28,8) NOT NULL,
    gross_pnl numeric(28,8) NOT NULL,
    estimated_cost numeric(28,8) NOT NULL,
    net_pnl numeric(28,8) NOT NULL,
    capital_at_risk numeric(28,8) NOT NULL,
    net_return numeric(24,12) NOT NULL,
    availability_flag character varying(40) NOT NULL,
    quality_flags text[] NOT NULL,
    evidence_batch_id uuid NOT NULL,
    evidence_count smallint NOT NULL,
    management_status character varying(24),
    CONSTRAINT option_package_mark_revisions_check CHECK (((btrim((valuation_policy_version)::text) <> ''::text) AND (btrim((adapter_version)::text) <> ''::text))),
    CONSTRAINT option_package_mark_revisions_check1 CHECK (((market_time <= observed_time) AND (observed_time <= computed_at))),
    CONSTRAINT option_package_mark_revisions_check2 CHECK (((entry_net_premium <> (0)::numeric) AND (estimated_cost >= (0)::numeric) AND (capital_at_risk > (0)::numeric))),
    CONSTRAINT option_package_mark_revisions_evidence_count_check CHECK (((evidence_count >= 1) AND (evidence_count <= 8))),
    CONSTRAINT option_package_mark_revisions_management_status_check CHECK (((management_status IS NULL) OR ((management_status)::text = ANY ((ARRAY['MONITORING'::character varying, 'TARGET_MET'::character varying, 'STOP_LOSS_HIT'::character varying, 'EXPIRED'::character varying])::text[])))),
    CONSTRAINT option_package_mark_revisions_provider_tier_check CHECK (((provider_tier)::text = ANY ((ARRAY['MASSIVE_ADVANCED'::character varying, 'MASSIVE_DEVELOPER'::character varying])::text[]))),
    CONSTRAINT option_package_mark_revisions_quality_flags_check CHECK ((cardinality(quality_flags) > 0)),
    CONSTRAINT option_package_mark_revisions_valuation_policy_sha256_check CHECK ((valuation_policy_sha256 ~ '^[0-9a-f]{64}$'::text))
);

ALTER TABLE ONLY rt.option_package_mark_revisions
    ADD CONSTRAINT option_package_mark_revisions_candidate_id_valuation_policy_key UNIQUE (candidate_id, valuation_policy_sha256, evidence_batch_id);

ALTER TABLE ONLY rt.option_package_mark_revisions
    ADD CONSTRAINT option_package_mark_revisions_pkey PRIMARY KEY (mark_id);


CREATE TABLE rt.option_packages (
    candidate_id uuid NOT NULL,
    cohort_id uuid NOT NULL,
    event_id uuid,
    matrix_id uuid NOT NULL,
    candidate_identity_sha256 character(64) NOT NULL,
    provider_tier character varying(24) NOT NULL,
    entry_market_time timestamp with time zone NOT NULL,
    entry_net_premium numeric(28,8) NOT NULL,
    capital_at_risk numeric(28,8) NOT NULL,
    exit_deadline timestamp with time zone,
    valuation_policy_sha256 character(64) NOT NULL,
    leg_count smallint NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT option_packages_candidate_identity_sha256_check CHECK ((candidate_identity_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_packages_check CHECK (((entry_net_premium <> (0)::numeric) AND (capital_at_risk > (0)::numeric))),
    CONSTRAINT option_packages_check1 CHECK (((exit_deadline IS NULL) OR (exit_deadline > entry_market_time))),
    CONSTRAINT option_packages_leg_count_check CHECK (((leg_count >= 1) AND (leg_count <= 8))),
    CONSTRAINT option_packages_provider_tier_check CHECK (((provider_tier)::text = ANY ((ARRAY['MASSIVE_ADVANCED'::character varying, 'MASSIVE_DEVELOPER'::character varying])::text[]))),
    CONSTRAINT option_packages_valuation_policy_sha256_check CHECK ((valuation_policy_sha256 ~ '^[0-9a-f]{64}$'::text))
);

ALTER TABLE ONLY rt.option_packages
    ADD CONSTRAINT option_packages_candidate_id_provider_tier_key UNIQUE (candidate_id, provider_tier);

ALTER TABLE ONLY rt.option_packages
    ADD CONSTRAINT option_packages_candidate_identity_sha256_key UNIQUE (candidate_identity_sha256);

ALTER TABLE ONLY rt.option_packages
    ADD CONSTRAINT option_packages_pkey PRIMARY KEY (candidate_id);


CREATE TABLE rt.option_pre_session_account_usage (
    run_id uuid NOT NULL,
    provider_requests integer DEFAULT 0 NOT NULL,
    response_bytes bigint DEFAULT 0 NOT NULL,
    next_request_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT option_pre_session_account_usage_provider_requests_check CHECK ((provider_requests >= 0)),
    CONSTRAINT option_pre_session_account_usage_response_bytes_check CHECK ((response_bytes >= 0))
)
WITH (fillfactor='80');

ALTER TABLE ONLY rt.option_pre_session_account_usage
    ADD CONSTRAINT option_pre_session_account_usage_pkey PRIMARY KEY (run_id);


CREATE TABLE rt.option_pre_session_finalizations (
    run_id uuid NOT NULL,
    selection_id uuid NOT NULL,
    package_set_sha256 character(64) NOT NULL,
    provider_requests integer NOT NULL,
    response_bytes bigint NOT NULL,
    finalized_at timestamp with time zone NOT NULL,
    CONSTRAINT option_pre_session_finalizations_package_set_sha256_check CHECK ((package_set_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_pre_session_finalizations_provider_requests_check CHECK ((provider_requests >= 0)),
    CONSTRAINT option_pre_session_finalizations_response_bytes_check CHECK ((response_bytes >= 0))
);

ALTER TABLE ONLY rt.option_pre_session_finalizations
    ADD CONSTRAINT option_pre_session_finalizations_pkey PRIMARY KEY (run_id);

ALTER TABLE ONLY rt.option_pre_session_finalizations
    ADD CONSTRAINT option_pre_session_finalizations_selection_id_key UNIQUE (selection_id);


CREATE TABLE rt.option_pre_session_packages (
    run_id uuid NOT NULL,
    underlyer character varying(32) NOT NULL,
    security_id uuid NOT NULL,
    package_payload_text text NOT NULL,
    package_sha256 character(64) NOT NULL,
    provider_requests integer NOT NULL,
    response_bytes bigint NOT NULL,
    completed_at timestamp with time zone NOT NULL,
    CONSTRAINT option_pre_session_packages_package_payload_text_check CHECK (((octet_length(package_payload_text) >= 1) AND (octet_length(package_payload_text) <= 67108864))),
    CONSTRAINT option_pre_session_packages_package_sha256_check CHECK ((package_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_pre_session_packages_provider_requests_check CHECK ((provider_requests >= 0)),
    CONSTRAINT option_pre_session_packages_response_bytes_check CHECK ((response_bytes >= 0))
);

ALTER TABLE ONLY rt.option_pre_session_packages
    ADD CONSTRAINT option_pre_session_packages_pkey PRIMARY KEY (run_id, underlyer);


CREATE TABLE rt.option_pre_session_runs (
    run_id uuid NOT NULL,
    profile_id uuid NOT NULL,
    profile_sha256 character(64) NOT NULL,
    provider_tier character varying(24) NOT NULL,
    universe_sha256 character(64) NOT NULL,
    policy_sha256 character(64) NOT NULL,
    session_date date NOT NULL,
    scheduled_cycle timestamp with time zone NOT NULL,
    effective_at timestamp with time zone NOT NULL,
    collection_deadline timestamp with time zone NOT NULL,
    maximum_provider_requests integer NOT NULL,
    maximum_response_bytes bigint NOT NULL,
    expected_underlyings text[] NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT option_pre_session_runs_check CHECK (((scheduled_cycle <= collection_deadline) AND (effective_at <= scheduled_cycle))),
    CONSTRAINT option_pre_session_runs_expected_underlyings_check CHECK (((cardinality(expected_underlyings) >= 1) AND (cardinality(expected_underlyings) <= 1000))),
    CONSTRAINT option_pre_session_runs_maximum_provider_requests_check CHECK (((maximum_provider_requests >= 1) AND (maximum_provider_requests <= 100000))),
    CONSTRAINT option_pre_session_runs_maximum_response_bytes_check CHECK (((maximum_response_bytes >= 1) AND (maximum_response_bytes <= '68719476736'::bigint))),
    CONSTRAINT option_pre_session_runs_policy_sha256_check CHECK ((policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_pre_session_runs_profile_sha256_check CHECK ((profile_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_pre_session_runs_provider_tier_check CHECK (((provider_tier)::text = 'MASSIVE_DEVELOPER'::text)),
    CONSTRAINT option_pre_session_runs_universe_sha256_check CHECK ((universe_sha256 ~ '^[0-9a-f]{64}$'::text))
);

ALTER TABLE ONLY rt.option_pre_session_runs
    ADD CONSTRAINT option_pre_session_runs_pkey PRIMARY KEY (run_id);


CREATE TABLE rt.option_pre_session_work (
    run_id uuid NOT NULL,
    underlyer character varying(32) NOT NULL,
    security_id uuid NOT NULL,
    status character varying(16) NOT NULL,
    owner_id uuid,
    owner_epoch bigint DEFAULT 0 NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    provider_requests integer DEFAULT 0 NOT NULL,
    response_bytes bigint DEFAULT 0 NOT NULL,
    lease_expires_at timestamp with time zone,
    completed_at timestamp with time zone,
    reason text,
    CONSTRAINT option_pre_session_work_attempts_check CHECK (((attempts >= 0) AND (attempts <= 3))),
    CONSTRAINT option_pre_session_work_check CHECK ((((status)::text <> 'CLAIMED'::text) OR ((owner_id IS NOT NULL) AND (owner_epoch > 0) AND (lease_expires_at IS NOT NULL)))),
    CONSTRAINT option_pre_session_work_owner_epoch_check CHECK ((owner_epoch >= 0)),
    CONSTRAINT option_pre_session_work_provider_requests_check CHECK ((provider_requests >= 0)),
    CONSTRAINT option_pre_session_work_response_bytes_check CHECK ((response_bytes >= 0)),
    CONSTRAINT option_pre_session_work_status_check CHECK (((status)::text = ANY ((ARRAY['PENDING'::character varying, 'CLAIMED'::character varying, 'COMPLETE'::character varying, 'BLOCKED'::character varying])::text[]))),
    CONSTRAINT option_pre_session_work_underlyer_check CHECK ((((underlyer)::text = upper((underlyer)::text)) AND (btrim((underlyer)::text) <> ''::text)))
)
WITH (fillfactor='80');

ALTER TABLE ONLY rt.option_pre_session_work
    ADD CONSTRAINT option_pre_session_work_pkey PRIMARY KEY (run_id, underlyer);


CREATE TABLE rt.portal_cohort_current (
    scope character varying(24) NOT NULL,
    cohort_id uuid NOT NULL,
    provider_tier character varying(24) NOT NULL,
    activation_id uuid NOT NULL,
    effective_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT portal_cohort_current_provider_tier_check CHECK (((provider_tier)::text = ANY ((ARRAY['MASSIVE_ADVANCED'::character varying, 'MASSIVE_DEVELOPER'::character varying])::text[]))),
    CONSTRAINT portal_cohort_current_scope_check CHECK (((scope)::text = 'PLATFORM'::text))
)
WITH (fillfactor='80');

ALTER TABLE ONLY rt.portal_cohort_current
    ADD CONSTRAINT portal_cohort_current_pkey PRIMARY KEY (scope);


CREATE TABLE rt.provider_tier_activations (
    activation_id uuid NOT NULL,
    previous_cohort_id uuid,
    target_cohort_id uuid NOT NULL,
    provider_tier character varying(24) NOT NULL,
    transition_ids uuid[] NOT NULL,
    activated_at timestamp with time zone NOT NULL,
    approval_reference text NOT NULL,
    dependencies_warm boolean NOT NULL,
    activation_sha256 character(64) NOT NULL,
    CONSTRAINT provider_tier_activations_activation_sha256_check CHECK ((activation_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT provider_tier_activations_approval_reference_check CHECK ((btrim(approval_reference) <> ''::text)),
    CONSTRAINT provider_tier_activations_check CHECK (((previous_cohort_id IS NULL) OR (previous_cohort_id <> target_cohort_id))),
    CONSTRAINT provider_tier_activations_dependencies_warm_check CHECK (dependencies_warm),
    CONSTRAINT provider_tier_activations_provider_tier_check CHECK (((provider_tier)::text = ANY ((ARRAY['MASSIVE_ADVANCED'::character varying, 'MASSIVE_DEVELOPER'::character varying])::text[])))
);

ALTER TABLE ONLY rt.provider_tier_activations
    ADD CONSTRAINT provider_tier_activations_activation_sha256_key UNIQUE (activation_sha256);

ALTER TABLE ONLY rt.provider_tier_activations
    ADD CONSTRAINT provider_tier_activations_pkey PRIMARY KEY (activation_id);


CREATE TABLE rt.provider_tier_cohorts (
    cohort_id uuid NOT NULL,
    profile_id uuid NOT NULL,
    provider_tier character varying(24) NOT NULL,
    stocks_tier character varying(24) NOT NULL,
    options_tier character varying(24) NOT NULL,
    ready_state character varying(40) NOT NULL,
    source_manifest_sha256 character(64) NOT NULL,
    valuation_policy_sha256 character(64) NOT NULL,
    capabilities text[] NOT NULL,
    disabled_detectors text[] NOT NULL,
    effective_at timestamp with time zone NOT NULL,
    binding_sha256 character(64) NOT NULL,
    CONSTRAINT provider_tier_cohorts_binding_sha256_check CHECK ((binding_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT provider_tier_cohorts_capabilities_check CHECK ((cardinality(capabilities) > 0)),
    CONSTRAINT provider_tier_cohorts_check CHECK ((((stocks_tier)::text = (provider_tier)::text) AND ((options_tier)::text = (provider_tier)::text))),
    CONSTRAINT provider_tier_cohorts_check1 CHECK (((((provider_tier)::text = 'MASSIVE_ADVANCED'::text) AND ((ready_state)::text = 'ADVANCED_READY'::text)) OR (((provider_tier)::text = 'MASSIVE_DEVELOPER'::text) AND ((ready_state)::text = 'DEVELOPER_READY'::text)))),
    CONSTRAINT provider_tier_cohorts_provider_tier_check CHECK (((provider_tier)::text = ANY ((ARRAY['MASSIVE_ADVANCED'::character varying, 'MASSIVE_DEVELOPER'::character varying])::text[]))),
    CONSTRAINT provider_tier_cohorts_source_manifest_sha256_check CHECK ((source_manifest_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT provider_tier_cohorts_valuation_policy_sha256_check CHECK ((valuation_policy_sha256 ~ '^[0-9a-f]{64}$'::text))
);

ALTER TABLE ONLY rt.provider_tier_cohorts
    ADD CONSTRAINT provider_tier_cohorts_binding_sha256_key UNIQUE (binding_sha256);

ALTER TABLE ONLY rt.provider_tier_cohorts
    ADD CONSTRAINT provider_tier_cohorts_cohort_id_provider_tier_key UNIQUE (cohort_id, provider_tier);

ALTER TABLE ONLY rt.provider_tier_cohorts
    ADD CONSTRAINT provider_tier_cohorts_pkey PRIMARY KEY (cohort_id);


CREATE TABLE rt.provider_tier_transitions (
    transition_id uuid NOT NULL,
    previous_state character varying(40) NOT NULL,
    target_state character varying(40) NOT NULL,
    transitioned_at timestamp with time zone NOT NULL,
    operator_approved boolean NOT NULL,
    operator_approval_reference text,
    dependencies_warm boolean NOT NULL,
    reason_code character varying(96) NOT NULL,
    from_profile_id uuid,
    to_profile_id uuid,
    transition_sha256 character(64) NOT NULL,
    recorded_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT provider_tier_transitions_check CHECK (((operator_approved AND (btrim(operator_approval_reference) <> ''::text)) OR ((NOT operator_approved) AND (operator_approval_reference IS NULL)))),
    CONSTRAINT provider_tier_transitions_check1 CHECK (((((previous_state)::text = 'ADVANCED_READY'::text) AND ((target_state)::text = 'ADVANCED_BLOCKED'::text)) OR (((previous_state)::text = 'ADVANCED_BLOCKED'::text) AND ((target_state)::text = 'FALLBACK_PENDING_APPROVAL'::text)) OR (((previous_state)::text = 'FALLBACK_PENDING_APPROVAL'::text) AND ((target_state)::text = 'DEVELOPER_WARMING'::text) AND operator_approved) OR (((previous_state)::text = 'DEVELOPER_WARMING'::text) AND ((target_state)::text = 'DEVELOPER_READY'::text) AND dependencies_warm) OR (((previous_state)::text = 'DEVELOPER_READY'::text) AND ((target_state)::text = 'ADVANCED_WARMING'::text) AND operator_approved) OR (((previous_state)::text = 'ADVANCED_WARMING'::text) AND ((target_state)::text = 'ADVANCED_READY'::text) AND dependencies_warm))),
    CONSTRAINT provider_tier_transitions_reason_code_check CHECK ((btrim((reason_code)::text) <> ''::text)),
    CONSTRAINT provider_tier_transitions_transition_sha256_check CHECK ((transition_sha256 ~ '^[0-9a-f]{64}$'::text))
);

ALTER TABLE ONLY rt.provider_tier_transitions
    ADD CONSTRAINT provider_tier_transitions_pkey PRIMARY KEY (transition_id);

ALTER TABLE ONLY rt.provider_tier_transitions
    ADD CONSTRAINT provider_tier_transitions_transition_sha256_key UNIQUE (transition_sha256);


CREATE TABLE rt.quote_current (
    provider character varying(32) NOT NULL,
    instrument_id bigint NOT NULL,
    source_session date NOT NULL,
    source_sequence bigint,
    event_time_raw bigint NOT NULL,
    event_time_unit character varying(8) NOT NULL,
    event_at timestamp with time zone NOT NULL,
    received_at timestamp with time zone NOT NULL,
    bid numeric(20,8),
    ask numeric(20,8),
    bid_size bigint,
    ask_size bigint,
    bid_exchange integer,
    ask_exchange integer,
    quality character varying(16) NOT NULL,
    reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    session_id uuid NOT NULL,
    journal_generation uuid NOT NULL,
    shard_id smallint NOT NULL,
    source_offset bigint NOT NULL,
    payload_sha256 character(64) NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT quote_current_ask_size_check CHECK (((ask_size IS NULL) OR (ask_size >= 0))),
    CONSTRAINT quote_current_bid_size_check CHECK (((bid_size IS NULL) OR (bid_size >= 0))),
    CONSTRAINT quote_current_check CHECK (((shard_id >= 0) AND (source_offset > 0))),
    CONSTRAINT quote_current_check1 CHECK (((((quality)::text = 'GOOD'::text) AND (cardinality(reason_codes) = 0)) OR ((quality)::text <> 'GOOD'::text))),
    CONSTRAINT quote_current_event_time_raw_check CHECK ((event_time_raw >= 0)),
    CONSTRAINT quote_current_event_time_unit_check CHECK (((event_time_unit)::text = ANY ((ARRAY['s'::character varying, 'ms'::character varying, 'us'::character varying, 'ns'::character varying])::text[]))),
    CONSTRAINT quote_current_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT quote_current_provider_check CHECK ((btrim((provider)::text) <> ''::text)),
    CONSTRAINT quote_current_quality_check CHECK (((quality)::text = ANY ((ARRAY['GOOD'::character varying, 'ONE_SIDED'::character varying, 'LOCKED'::character varying, 'CROSSED'::character varying, 'STALE'::character varying, 'INVALID'::character varying, 'UNCLASSIFIED'::character varying])::text[]))),
    CONSTRAINT quote_current_source_sequence_check CHECK (((source_sequence IS NULL) OR (source_sequence >= 0)))
)
WITH (fillfactor='80');

ALTER TABLE ONLY rt.quote_current
    ADD CONSTRAINT quote_current_pkey PRIMARY KEY (provider, instrument_id);


CREATE TABLE rt.readiness_current (
    scope_type character varying(24) NOT NULL,
    scope_id character varying(160) NOT NULL,
    capability character varying(64) NOT NULL,
    epoch bigint NOT NULL,
    owner_epoch bigint NOT NULL,
    profile_id uuid NOT NULL,
    status character varying(16) NOT NULL,
    reasons text[] DEFAULT ARRAY[]::text[] NOT NULL,
    checked_at timestamp with time zone NOT NULL,
    lease_until timestamp with time zone NOT NULL,
    source_watermark jsonb DEFAULT '{}'::jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT readiness_current_capability_check CHECK ((btrim((capability)::text) <> ''::text)),
    CONSTRAINT readiness_current_check CHECK (((btrim((scope_type)::text) <> ''::text) AND (btrim((scope_id)::text) <> ''::text))),
    CONSTRAINT readiness_current_check1 CHECK (((epoch > 0) AND (owner_epoch > 0))),
    CONSTRAINT readiness_current_check2 CHECK ((lease_until > checked_at)),
    CONSTRAINT readiness_current_check3 CHECK (((((status)::text = 'READY'::text) AND (cardinality(reasons) = 0)) OR ((status)::text <> 'READY'::text))),
    CONSTRAINT readiness_current_check4 CHECK (((((status)::text <> 'READY'::text) AND (cardinality(reasons) > 0)) OR ((status)::text = 'READY'::text))),
    CONSTRAINT readiness_current_source_watermark_check CHECK ((jsonb_typeof(source_watermark) = 'object'::text)),
    CONSTRAINT readiness_current_status_check CHECK (((status)::text = ANY ((ARRAY['READY'::character varying, 'DEGRADED'::character varying, 'BLOCKED'::character varying])::text[])))
)
WITH (fillfactor='80');

ALTER TABLE ONLY rt.readiness_current
    ADD CONSTRAINT readiness_current_pkey PRIMARY KEY (scope_type, scope_id, capability);


CREATE TABLE rt.readiness_transitions (
    transition_id bigint NOT NULL,
    scope_type character varying(24) NOT NULL,
    scope_id character varying(160) NOT NULL,
    capability character varying(64) NOT NULL,
    epoch bigint NOT NULL,
    owner_epoch bigint NOT NULL,
    profile_id uuid NOT NULL,
    previous_status character varying(16),
    status character varying(16) NOT NULL,
    reasons text[] DEFAULT ARRAY[]::text[] NOT NULL,
    transitioned_at timestamp with time zone NOT NULL,
    lease_until timestamp with time zone NOT NULL,
    source_watermark jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT readiness_transitions_check CHECK (((epoch > 0) AND (owner_epoch > 0))),
    CONSTRAINT readiness_transitions_check1 CHECK ((lease_until > transitioned_at)),
    CONSTRAINT readiness_transitions_previous_status_check CHECK (((previous_status IS NULL) OR ((previous_status)::text = ANY ((ARRAY['READY'::character varying, 'DEGRADED'::character varying, 'BLOCKED'::character varying])::text[])))),
    CONSTRAINT readiness_transitions_source_watermark_check CHECK ((jsonb_typeof(source_watermark) = 'object'::text)),
    CONSTRAINT readiness_transitions_status_check CHECK (((status)::text = ANY ((ARRAY['READY'::character varying, 'DEGRADED'::character varying, 'BLOCKED'::character varying])::text[])))
);

ALTER TABLE rt.readiness_transitions ALTER COLUMN transition_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME rt.readiness_transitions_transition_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);

ALTER TABLE ONLY rt.readiness_transitions
    ADD CONSTRAINT readiness_transitions_pkey PRIMARY KEY (transition_id);


CREATE TABLE rt.screening_current (
    cohort_id uuid NOT NULL,
    generation_id uuid NOT NULL,
    session_date date NOT NULL,
    generated_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL
)
WITH (fillfactor='80');

ALTER TABLE ONLY rt.screening_current
    ADD CONSTRAINT screening_current_pkey PRIMARY KEY (cohort_id);


CREATE TABLE rt.screening_generations (
    generation_id uuid NOT NULL,
    cohort_id uuid NOT NULL,
    provider_tier character varying(24) NOT NULL,
    session_date date NOT NULL,
    source_manifest_sha256 character(64) NOT NULL,
    expected_members integer NOT NULL,
    actual_members integer NOT NULL,
    status character varying(16) NOT NULL,
    generated_at timestamp with time zone NOT NULL,
    manifest jsonb NOT NULL,
    CONSTRAINT screening_generations_check CHECK (((expected_members > 0) AND ((actual_members >= 0) AND (actual_members <= expected_members)))),
    CONSTRAINT screening_generations_check1 CHECK ((((status)::text = 'COMPLETE'::text) = (actual_members = expected_members))),
    CONSTRAINT screening_generations_manifest_check CHECK ((jsonb_typeof(manifest) = 'object'::text)),
    CONSTRAINT screening_generations_provider_tier_check CHECK (((provider_tier)::text = ANY ((ARRAY['MASSIVE_ADVANCED'::character varying, 'MASSIVE_DEVELOPER'::character varying])::text[]))),
    CONSTRAINT screening_generations_source_manifest_sha256_check CHECK ((source_manifest_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT screening_generations_status_check CHECK (((status)::text = ANY ((ARRAY['COMPLETE'::character varying, 'DEGRADED'::character varying, 'FAILED'::character varying])::text[])))
);

ALTER TABLE ONLY rt.screening_generations
    ADD CONSTRAINT screening_generations_cohort_id_session_date_source_manifes_key UNIQUE (cohort_id, session_date, source_manifest_sha256);

ALTER TABLE ONLY rt.screening_generations
    ADD CONSTRAINT screening_generations_pkey PRIMARY KEY (generation_id);


CREATE TABLE rt.screening_rows (
    generation_id uuid NOT NULL,
    security_id uuid NOT NULL,
    ticker character varying(32) NOT NULL,
    eligible boolean NOT NULL,
    payload_sha256 character(64) NOT NULL,
    payload jsonb NOT NULL,
    CONSTRAINT screening_rows_payload_check CHECK ((jsonb_typeof(payload) = 'object'::text)),
    CONSTRAINT screening_rows_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT screening_rows_ticker_check CHECK ((((ticker)::text = upper((ticker)::text)) AND (btrim((ticker)::text) <> ''::text)))
);

ALTER TABLE ONLY rt.screening_rows
    ADD CONSTRAINT screening_rows_pkey PRIMARY KEY (generation_id, security_id);


CREATE TABLE rt.seed_current (
    scope character varying(40) NOT NULL,
    seed_id uuid NOT NULL,
    generation bigint NOT NULL,
    effective_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT seed_current_generation_check CHECK ((generation > 0)),
    CONSTRAINT seed_current_scope_check CHECK (((scope)::text = ANY ((ARRAY['MARKET_REFERENCES'::character varying, 'EQUITY_VALIDATION_REFERENCES'::character varying])::text[])))
)
WITH (fillfactor='80');

ALTER TABLE ONLY rt.seed_current
    ADD CONSTRAINT seed_current_pkey PRIMARY KEY (scope);


CREATE TABLE rt.seed_manifest_members (
    seed_id uuid NOT NULL,
    reference_kind character varying(40) NOT NULL,
    reference_key character varying(256) NOT NULL,
    source_id text NOT NULL,
    payload_sha256 character(64) NOT NULL,
    status character varying(16) NOT NULL,
    reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    received_at timestamp with time zone NOT NULL,
    CONSTRAINT seed_manifest_members_check CHECK (((btrim((reference_key)::text) <> ''::text) AND (btrim(source_id) <> ''::text))),
    CONSTRAINT seed_manifest_members_check1 CHECK (((((status)::text = 'READY'::text) AND (cardinality(reason_codes) = 0)) OR ((status)::text <> 'READY'::text))),
    CONSTRAINT seed_manifest_members_check2 CHECK (((((status)::text = 'UNAVAILABLE'::text) AND (cardinality(reason_codes) > 0)) OR ((status)::text <> 'UNAVAILABLE'::text))),
    CONSTRAINT seed_manifest_members_check3 CHECK ((received_at >= observed_at)),
    CONSTRAINT seed_manifest_members_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT seed_manifest_members_reference_kind_check CHECK (((reference_kind)::text = ANY ((ARRAY['SECURITY'::character varying, 'FUNDAMENTALS'::character varying, 'OPTION_CATALOG'::character varying, 'CORPORATE_ACTION_COVERAGE'::character varying, 'OI_COVERAGE'::character varying, 'EARNINGS_COVERAGE'::character varying, 'FOMC_COVERAGE'::character varying, 'RATE_CURVE'::character varying])::text[]))),
    CONSTRAINT seed_manifest_members_status_check CHECK (((status)::text = ANY ((ARRAY['READY'::character varying, 'UNAVAILABLE'::character varying, 'NOT_APPLICABLE'::character varying])::text[])))
);

ALTER TABLE ONLY rt.seed_manifest_members
    ADD CONSTRAINT seed_manifest_members_pkey PRIMARY KEY (seed_id, reference_kind, reference_key);


CREATE TABLE rt.seed_manifests (
    seed_id uuid NOT NULL,
    seed_sha256 character(64) NOT NULL,
    universe_sha256 character(64) NOT NULL,
    session_date date NOT NULL,
    status character varying(16) NOT NULL,
    reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    member_count integer NOT NULL,
    created_at timestamp with time zone NOT NULL,
    effective_at timestamp with time zone NOT NULL,
    valid_until timestamp with time zone NOT NULL,
    source_generations jsonb NOT NULL,
    CONSTRAINT seed_manifests_check CHECK (((((status)::text = 'READY'::text) AND (cardinality(reason_codes) = 0)) OR ((status)::text <> 'READY'::text))),
    CONSTRAINT seed_manifests_check1 CHECK (((((status)::text = 'UNAVAILABLE'::text) AND (cardinality(reason_codes) > 0)) OR ((status)::text <> 'UNAVAILABLE'::text))),
    CONSTRAINT seed_manifests_check2 CHECK (((created_at <= effective_at) AND (effective_at < valid_until))),
    CONSTRAINT seed_manifests_member_count_check CHECK ((member_count > 0)),
    CONSTRAINT seed_manifests_seed_sha256_check CHECK ((seed_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT seed_manifests_source_generations_check CHECK ((jsonb_typeof(source_generations) = 'object'::text)),
    CONSTRAINT seed_manifests_status_check CHECK (((status)::text = ANY ((ARRAY['READY'::character varying, 'UNAVAILABLE'::character varying])::text[]))),
    CONSTRAINT seed_manifests_universe_sha256_check CHECK ((universe_sha256 ~ '^[0-9a-f]{64}$'::text))
);

ALTER TABLE ONLY rt.seed_manifests
    ADD CONSTRAINT seed_manifests_pkey PRIMARY KEY (seed_id);

ALTER TABLE ONLY rt.seed_manifests
    ADD CONSTRAINT seed_manifests_seed_sha256_key UNIQUE (seed_sha256);


CREATE TABLE rt.stream_gaps (
    gap_id uuid NOT NULL,
    session_id uuid NOT NULL,
    instrument_id bigint,
    channel character varying(4),
    gap_start timestamp with time zone NOT NULL,
    gap_end timestamp with time zone,
    journal_generation uuid NOT NULL,
    shard_id smallint NOT NULL,
    last_durable_offset bigint NOT NULL,
    reason character varying(64) NOT NULL,
    recovery_status character varying(24) NOT NULL,
    repair_reference uuid,
    recorded_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT stream_gaps_channel_check CHECK (((channel IS NULL) OR ((channel)::text = ANY ((ARRAY['T'::character varying, 'Q'::character varying, 'AM'::character varying])::text[])))),
    CONSTRAINT stream_gaps_check CHECK (((gap_end IS NULL) OR (gap_end >= gap_start))),
    CONSTRAINT stream_gaps_last_durable_offset_check CHECK ((last_durable_offset >= 0)),
    CONSTRAINT stream_gaps_reason_check CHECK ((btrim((reason)::text) <> ''::text)),
    CONSTRAINT stream_gaps_recovery_status_check CHECK (((recovery_status)::text = ANY ((ARRAY['UNRESOLVED'::character varying, 'WARMING'::character varying, 'AGED_OUT'::character varying, 'REPAIRED'::character varying, 'WAIVED'::character varying])::text[]))),
    CONSTRAINT stream_gaps_shard_id_check CHECK ((shard_id >= 0))
);

ALTER TABLE ONLY rt.stream_gaps
    ADD CONSTRAINT stream_gaps_pkey PRIMARY KEY (gap_id);


CREATE TABLE rt.stream_sessions (
    session_id uuid NOT NULL,
    provider character varying(32) NOT NULL,
    feed character varying(12) NOT NULL,
    manifest_id uuid NOT NULL,
    owner_id uuid NOT NULL,
    owner_epoch bigint NOT NULL,
    started_at timestamp with time zone NOT NULL,
    authenticated_at timestamp with time zone,
    subscribed_at timestamp with time zone,
    ended_at timestamp with time zone,
    status character varying(24) NOT NULL,
    failure_code character varying(64),
    CONSTRAINT stream_sessions_check CHECK (((authenticated_at IS NULL) OR (authenticated_at >= started_at))),
    CONSTRAINT stream_sessions_check1 CHECK (((subscribed_at IS NULL) OR (subscribed_at >= started_at))),
    CONSTRAINT stream_sessions_check2 CHECK (((ended_at IS NULL) OR (ended_at >= started_at))),
    CONSTRAINT stream_sessions_feed_check CHECK (((feed)::text = ANY ((ARRAY['STOCKS'::character varying, 'OPTIONS'::character varying])::text[]))),
    CONSTRAINT stream_sessions_owner_epoch_check CHECK ((owner_epoch > 0)),
    CONSTRAINT stream_sessions_provider_check CHECK ((btrim((provider)::text) <> ''::text)),
    CONSTRAINT stream_sessions_status_check CHECK (((status)::text = ANY ((ARRAY['CONNECTING'::character varying, 'AUTHENTICATING'::character varying, 'SUBSCRIBING'::character varying, 'WARMING'::character varying, 'LIVE'::character varying, 'BACKOFF'::character varying, 'BLOCKED'::character varying, 'STOPPING'::character varying, 'STOPPED'::character varying, 'FAILED'::character varying])::text[])))
);

ALTER TABLE ONLY rt.stream_sessions
    ADD CONSTRAINT stream_sessions_pkey PRIMARY KEY (session_id);


CREATE TABLE rt.trade_current (
    provider character varying(32) NOT NULL,
    instrument_id bigint NOT NULL,
    source_session date NOT NULL,
    source_sequence bigint,
    event_time_raw bigint NOT NULL,
    event_time_unit character varying(8) NOT NULL,
    event_at timestamp with time zone NOT NULL,
    received_at timestamp with time zone NOT NULL,
    price numeric(20,8),
    size numeric(28,8),
    exchange integer,
    conditions integer[] DEFAULT ARRAY[]::integer[] NOT NULL,
    eligibility character varying(16) NOT NULL,
    eligibility_policy_version character varying(96) NOT NULL,
    reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    session_id uuid NOT NULL,
    journal_generation uuid NOT NULL,
    shard_id smallint NOT NULL,
    source_offset bigint NOT NULL,
    payload_sha256 character(64) NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT trade_current_check CHECK (((shard_id >= 0) AND (source_offset > 0))),
    CONSTRAINT trade_current_eligibility_check CHECK (((eligibility)::text = ANY ((ARRAY['INCLUDED'::character varying, 'EXCLUDED'::character varying, 'CANCELED'::character varying, 'SUPERSEDED'::character varying, 'UNKNOWN'::character varying, 'INVALID'::character varying])::text[]))),
    CONSTRAINT trade_current_eligibility_policy_version_check CHECK ((btrim((eligibility_policy_version)::text) <> ''::text)),
    CONSTRAINT trade_current_event_time_raw_check CHECK ((event_time_raw >= 0)),
    CONSTRAINT trade_current_event_time_unit_check CHECK (((event_time_unit)::text = ANY ((ARRAY['s'::character varying, 'ms'::character varying, 'us'::character varying, 'ns'::character varying])::text[]))),
    CONSTRAINT trade_current_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT trade_current_provider_check CHECK ((btrim((provider)::text) <> ''::text)),
    CONSTRAINT trade_current_source_sequence_check CHECK (((source_sequence IS NULL) OR (source_sequence >= 0)))
)
WITH (fillfactor='80');

ALTER TABLE ONLY rt.trade_current
    ADD CONSTRAINT trade_current_pkey PRIMARY KEY (provider, instrument_id);


CREATE TABLE rt.worker_leases (
    resource_key character varying(192) NOT NULL,
    owner_id uuid NOT NULL,
    owner_epoch bigint NOT NULL,
    lease_until timestamp with time zone NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT worker_leases_owner_epoch_check CHECK ((owner_epoch > 0)),
    CONSTRAINT worker_leases_resource_key_check CHECK ((btrim((resource_key)::text) <> ''::text))
)
WITH (fillfactor='80');

ALTER TABLE ONLY rt.worker_leases
    ADD CONSTRAINT worker_leases_pkey PRIMARY KEY (resource_key);

