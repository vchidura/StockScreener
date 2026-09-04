-- Canonical StockScreener schema baseline.
-- Generated from the validated post-cutover PostgreSQL 17 schema on 2026-09-01.
-- Apply only to a new, empty database. Market and research data are ingested separately.

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

BEGIN;

--
-- Name: enforce_option_new_series_transition(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_option_new_series_transition() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.state = OLD.state THEN
        RETURN NEW;
    END IF;
    IF NOT (
        (OLD.state = 'UNKNOWN_REFERENCE' AND NEW.state = 'REFERENCE_PENDING') OR
        (OLD.state = 'REFERENCE_PENDING' AND NEW.state IN (
            'VALIDATED_ACTIVE', 'REJECTED_UNSUPPORTED', 'REFERENCE_UNAVAILABLE'
        )) OR
        (OLD.state = 'REFERENCE_UNAVAILABLE' AND NEW.state = 'REFERENCE_PENDING') OR
        (OLD.state = 'VALIDATED_ACTIVE' AND NEW.state = 'WATCHLIST_ACTIVE')
    ) THEN
        RAISE EXCEPTION 'invalid option new-series transition: % -> %', OLD.state, NEW.state;
    END IF;
    RETURN NEW;
END;
$$;


--
-- Name: enforce_option_signal_leg_completeness(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.enforce_option_signal_leg_completeness() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    actual_leg_count INTEGER;
BEGIN
    IF NEW.status <> 'READY' THEN
        RETURN NEW;
    END IF;
    SELECT COUNT(*) INTO actual_leg_count
    FROM option_signal_legs
    WHERE event_id = NEW.event_id;
    IF actual_leg_count <> NEW.expected_leg_count THEN
        RAISE EXCEPTION 'ready option signal % expected % legs but found %',
            NEW.event_id, NEW.expected_leg_count, actual_leg_count;
    END IF;
    RETURN NEW;
END;
$$;


--
-- Name: ensure_option_market_data_partitions(date); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.ensure_option_market_data_partitions(p_month_start date) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'public'
    AS $$
DECLARE
    current_month DATE;
    month_start_utc TIMESTAMPTZ;
    next_month_utc TIMESTAMPTZ;
    month_suffix TEXT;
    snapshot_partition TEXT;
    trade_partition TEXT;
BEGIN
    IF p_month_start <> date_trunc('month', p_month_start)::DATE THEN
        RAISE EXCEPTION 'p_month_start must be the first day of a month';
    END IF;
    current_month := date_trunc('month', CURRENT_DATE)::DATE;
    IF p_month_start NOT IN (
        current_month,
        (current_month + INTERVAL '1 month')::DATE
    ) THEN
        RAISE EXCEPTION 'partition maintenance is limited to current and next month';
    END IF;

    month_start_utc := p_month_start::TIMESTAMP AT TIME ZONE 'UTC';
    next_month_utc := (p_month_start + INTERVAL '1 month')::TIMESTAMP AT TIME ZONE 'UTC';
    month_suffix := to_char(p_month_start, 'YYYYMM');
    snapshot_partition := 'option_chain_snapshots_y' || month_suffix;
    trade_partition := 'option_trade_events_y' || month_suffix;

    PERFORM pg_advisory_xact_lock(
        hashtextextended('option-market-data-partition:' || month_suffix, 0)
    );

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS public.%I PARTITION OF public.option_chain_snapshots FOR VALUES FROM (%L) TO (%L)',
        snapshot_partition,
        month_start_utc,
        next_month_utc
    );
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS public.%I PARTITION OF public.option_trade_events FOR VALUES FROM (%L) TO (%L)',
        trade_partition,
        month_start_utc,
        next_month_utc
    );
END;
$$;

REVOKE ALL ON FUNCTION public.ensure_option_market_data_partitions(date) FROM PUBLIC;


--
-- Name: mark_equity_portal_source_changed(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.mark_equity_portal_source_changed() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    UPDATE equity_portal_source_state
    SET generation = generation + 1,
        changed_at = NOW()
    WHERE singleton = TRUE;
    RETURN NULL;
END;
$$;


--
-- Name: option_market_data_partitions_ready(timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.option_market_data_partitions_ready(p_at timestamp with time zone) RETURNS boolean
    LANGUAGE sql STABLE
    AS $$
    SELECT
        to_regclass(
            'public.option_chain_snapshots_y' || to_char(p_at AT TIME ZONE 'UTC', 'YYYYMM')
        ) IS NOT NULL
        AND
        to_regclass(
            'public.option_trade_events_y' || to_char(p_at AT TIME ZONE 'UTC', 'YYYYMM')
        ) IS NOT NULL;
$$;


--
-- Name: reject_equity_bar_revision_mutation(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.reject_equity_bar_revision_mutation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    RAISE EXCEPTION
        'equity_bar_revisions is append-only; attempted %', TG_OP
        USING ERRCODE = '55000';
END;
$$;


--
-- Name: validate_equity_current_bar_projection(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.validate_equity_current_bar_projection() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    selected equity_bar_revisions%ROWTYPE;
BEGIN
    SELECT * INTO selected
    FROM equity_bar_revisions
    WHERE bar_revision_id = NEW.selected_bar_revision_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'selected bar revision does not exist: %',
            NEW.selected_bar_revision_id USING ERRCODE = '23503';
    END IF;
    IF NOT selected.is_final THEN
        RAISE EXCEPTION 'selected bar revision is not final: %',
            NEW.selected_bar_revision_id USING ERRCODE = '23514';
    END IF;
    IF (
        selected.ticker,
        selected.interval,
        selected.bar_start,
        selected.bar_end,
        selected.session_scope,
        selected.adjusted
    ) IS DISTINCT FROM (
        NEW.ticker,
        NEW.interval,
        NEW.bar_start,
        NEW.bar_end,
        NEW.session_scope,
        NEW.adjusted
    ) THEN
        RAISE EXCEPTION 'selected bar revision does not match projection identity: %',
            NEW.selected_bar_revision_id USING ERRCODE = '23514';
    END IF;
    IF NEW.observed_at < COALESCE(selected.replay_available_at, selected.system_observed_at) THEN
        RAISE EXCEPTION 'projection precedes selected bar availability: %',
            NEW.selected_bar_revision_id USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: cross_sectional_signals; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cross_sectional_signals (
    signal_id integer NOT NULL,
    trade_date date NOT NULL,
    ticker character varying(16) NOT NULL,
    model_version character varying(32) NOT NULL,
    horizon_days smallint NOT NULL,
    raw_score double precision,
    neutral_score double precision,
    percentile double precision,
    decile smallint,
    side character varying(5),
    universe_size integer NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: cross_sectional_signals_signal_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.cross_sectional_signals_signal_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: cross_sectional_signals_signal_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.cross_sectional_signals_signal_id_seq OWNED BY public.cross_sectional_signals.signal_id;


--
-- Name: daily_recommendations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.daily_recommendations (
    rec_id bigint NOT NULL,
    trade_date date NOT NULL,
    ticker character varying(10) NOT NULL,
    sector character varying(50),
    recommendation_type character varying(20) NOT NULL,
    rank_in_category integer NOT NULL,
    predicted_return_pct numeric(6,3),
    predicted_confidence_pct integer,
    signal_grade character varying(20),
    signal_score integer,
    primary_index character varying(10),
    primary_regime character varying(20),
    sector_etf character varying(10),
    sector_regime character varying(20),
    market_breadth_score integer,
    recommended_position_size_pct numeric(5,3),
    recommended_entry numeric(12,4),
    recommended_stop numeric(12,4),
    recommended_target_1 numeric(12,4),
    recommended_target_2 numeric(12,4),
    risk_reward_ratio numeric(6,2),
    breakout_score integer,
    vwap_score integer,
    volatility_score integer,
    trend_score integer,
    rs_score integer,
    calendar_score integer,
    score_id bigint,
    analog_id bigint,
    pattern_priors_applied boolean DEFAULT false,
    analog_matching_applied boolean DEFAULT false,
    confidence_before_calibration integer,
    confidence_after_calibration integer,
    calibration_sources text,
    auto_trade_enabled boolean DEFAULT false,
    was_executed boolean DEFAULT false,
    execution_price numeric(12,4),
    execution_time timestamp with time zone,
    actual_return_pct numeric(6,3),
    actual_high_pct numeric(6,3),
    actual_low_pct numeric(6,3),
    hit_target_1 boolean,
    hit_target_2 boolean,
    stopped_out boolean,
    recommendation_correct boolean,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    notes text,
    CONSTRAINT daily_recommendations_breakout_score_check CHECK (((breakout_score >= 0) AND (breakout_score <= 100))),
    CONSTRAINT daily_recommendations_calendar_score_check CHECK (((calendar_score >= 0) AND (calendar_score <= 100))),
    CONSTRAINT daily_recommendations_market_breadth_score_check CHECK (((market_breadth_score >= 0) AND (market_breadth_score <= 100))),
    CONSTRAINT daily_recommendations_predicted_confidence_pct_check CHECK (((predicted_confidence_pct >= 0) AND (predicted_confidence_pct <= 100))),
    CONSTRAINT daily_recommendations_recommendation_type_check CHECK (((recommendation_type)::text = ANY (ARRAY[('BULL'::character varying)::text, ('BEAR'::character varying)::text]))),
    CONSTRAINT daily_recommendations_rs_score_check CHECK (((rs_score >= 0) AND (rs_score <= 100))),
    CONSTRAINT daily_recommendations_signal_grade_check CHECK (((signal_grade)::text = ANY (ARRAY[('Excellent'::character varying)::text, ('Good'::character varying)::text, ('Fair'::character varying)::text, ('Weak'::character varying)::text]))),
    CONSTRAINT daily_recommendations_signal_score_check CHECK (((signal_score >= 0) AND (signal_score <= 100))),
    CONSTRAINT daily_recommendations_trend_score_check CHECK (((trend_score >= 0) AND (trend_score <= 100))),
    CONSTRAINT daily_recommendations_volatility_score_check CHECK (((volatility_score >= 0) AND (volatility_score <= 100))),
    CONSTRAINT daily_recommendations_vwap_score_check CHECK (((vwap_score >= 0) AND (vwap_score <= 100)))
);


--
-- Name: daily_recommendations_rec_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.daily_recommendations_rec_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: daily_recommendations_rec_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.daily_recommendations_rec_id_seq OWNED BY public.daily_recommendations.rec_id;


--
-- Name: data_ingestion_failures; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.data_ingestion_failures (
    failure_id bigint NOT NULL,
    dataset character varying(32) NOT NULL,
    ticker character varying(16) NOT NULL,
    trade_date date NOT NULL,
    provider character varying(32) NOT NULL,
    failure_type character varying(64) NOT NULL,
    details text,
    attempts integer DEFAULT 0 NOT NULL,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    resolved_at timestamp with time zone
);


--
-- Name: data_ingestion_failures_failure_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.data_ingestion_failures_failure_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: data_ingestion_failures_failure_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.data_ingestion_failures_failure_id_seq OWNED BY public.data_ingestion_failures.failure_id;


--
-- Name: equity_analysis_members; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_analysis_members (
    analysis_run_id uuid NOT NULL,
    security_id uuid NOT NULL,
    ticker character varying(32) NOT NULL,
    status character varying(24) NOT NULL,
    latest_bar_revision_id uuid,
    source_bar_count integer DEFAULT 0 NOT NULL,
    evidence_count integer DEFAULT 0 NOT NULL,
    lease_owner character varying(128),
    lease_expires_at timestamp with time zone,
    attempt_count integer DEFAULT 0 NOT NULL,
    failure_reason text,
    completed_at timestamp with time zone,
    CONSTRAINT equity_analysis_members_attempt_count_check CHECK ((attempt_count >= 0)),
    CONSTRAINT equity_analysis_members_check CHECK (((((status)::text = 'CLAIMED'::text) AND (lease_owner IS NOT NULL) AND (lease_expires_at IS NOT NULL)) OR (((status)::text <> 'CLAIMED'::text) AND (lease_owner IS NULL) AND (lease_expires_at IS NULL)))),
    CONSTRAINT equity_analysis_members_evidence_count_check CHECK ((evidence_count >= 0)),
    CONSTRAINT equity_analysis_members_source_bar_count_check CHECK ((source_bar_count >= 0)),
    CONSTRAINT equity_analysis_members_status_check CHECK (((status)::text = ANY ((ARRAY['PENDING'::character varying, 'CLAIMED'::character varying, 'COMPLETE'::character varying, 'NO_MATCH'::character varying, 'INSUFFICIENT_DATA'::character varying, 'FAILED'::character varying])::text[])))
);


--
-- Name: equity_analysis_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_analysis_runs (
    analysis_run_id uuid NOT NULL,
    business_key character varying(320) NOT NULL,
    run_purpose character varying(16) NOT NULL,
    "interval" character varying(8) NOT NULL,
    market_time timestamp with time zone NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    universe_run_id uuid NOT NULL,
    model_bundle_version character varying(64) NOT NULL,
    model_bundle_sha256 character(64) NOT NULL,
    expected_members integer NOT NULL,
    completed_members integer DEFAULT 0 NOT NULL,
    no_match_members integer DEFAULT 0 NOT NULL,
    insufficient_members integer DEFAULT 0 NOT NULL,
    failed_members integer DEFAULT 0 NOT NULL,
    status character varying(24) NOT NULL,
    input_sha256 character(64),
    output_sha256 character(64),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    published_at timestamp with time zone,
    invalidated_reason text,
    CONSTRAINT equity_analysis_runs_check CHECK ((observed_at >= market_time)),
    CONSTRAINT equity_analysis_runs_check1 CHECK (((completed_at IS NULL) OR (completed_at >= created_at))),
    CONSTRAINT equity_analysis_runs_check2 CHECK (((published_at IS NULL) OR ((status)::text = ANY ((ARRAY['COMPLETE'::character varying, 'DEGRADED'::character varying])::text[])))),
    CONSTRAINT equity_analysis_runs_completed_members_check CHECK ((completed_members >= 0)),
    CONSTRAINT equity_analysis_runs_expected_members_check CHECK ((expected_members >= 0)),
    CONSTRAINT equity_analysis_runs_failed_members_check CHECK ((failed_members >= 0)),
    CONSTRAINT equity_analysis_runs_input_sha256_check CHECK (((input_sha256 IS NULL) OR (input_sha256 ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT equity_analysis_runs_insufficient_members_check CHECK ((insufficient_members >= 0)),
    CONSTRAINT equity_analysis_runs_interval_check CHECK ((("interval")::text = ANY ((ARRAY['5m'::character varying, '15m'::character varying, '30m'::character varying, '1h'::character varying, '1d'::character varying, '1wk'::character varying, '1mo'::character varying])::text[]))),
    CONSTRAINT equity_analysis_runs_model_bundle_sha256_check CHECK ((model_bundle_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT equity_analysis_runs_no_match_members_check CHECK ((no_match_members >= 0)),
    CONSTRAINT equity_analysis_runs_output_sha256_check CHECK (((output_sha256 IS NULL) OR (output_sha256 ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT equity_analysis_runs_run_purpose_check CHECK (((run_purpose)::text = ANY ((ARRAY['ORIGINAL'::character varying, 'REPLAY'::character varying, 'SHADOW'::character varying])::text[]))),
    CONSTRAINT equity_analysis_runs_status_check CHECK (((status)::text = ANY ((ARRAY['PENDING'::character varying, 'RUNNING'::character varying, 'COMPLETE'::character varying, 'DEGRADED'::character varying, 'FAILED'::character varying])::text[])))
);


--
-- Name: equity_bar_publication_members; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_bar_publication_members (
    publication_id uuid NOT NULL,
    security_id uuid NOT NULL,
    ticker character varying(32) NOT NULL,
    status character varying(16) NOT NULL,
    selected_bar_revision_id uuid,
    reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    CONSTRAINT equity_bar_publication_members_check CHECK (((((status)::text = 'SELECTED'::text) AND (selected_bar_revision_id IS NOT NULL)) OR (((status)::text <> 'SELECTED'::text) AND (selected_bar_revision_id IS NULL)))),
    CONSTRAINT equity_bar_publication_members_status_check CHECK (((status)::text = ANY ((ARRAY['SELECTED'::character varying, 'MISSING'::character varying, 'FAILED'::character varying])::text[])))
);


--
-- Name: equity_bar_publications; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_bar_publications (
    publication_id uuid NOT NULL,
    business_key character varying(384) NOT NULL,
    "interval" character varying(8) NOT NULL,
    market_time timestamp with time zone NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    session_scope character varying(16) NOT NULL,
    adjusted boolean NOT NULL,
    selection_policy_version character varying(64) NOT NULL,
    selection_policy_sha256 character(64) NOT NULL,
    expected_members integer NOT NULL,
    selected_members integer DEFAULT 0 NOT NULL,
    missing_members integer DEFAULT 0 NOT NULL,
    failed_members integer DEFAULT 0 NOT NULL,
    status character varying(16) NOT NULL,
    input_sha256 character(64) NOT NULL,
    output_sha256 character(64),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    published_at timestamp with time zone,
    CONSTRAINT equity_bar_publications_check CHECK ((observed_at >= market_time)),
    CONSTRAINT equity_bar_publications_check1 CHECK ((((selected_members + missing_members) + failed_members) <= expected_members)),
    CONSTRAINT equity_bar_publications_check2 CHECK (((completed_at IS NULL) OR (completed_at >= created_at))),
    CONSTRAINT equity_bar_publications_check3 CHECK (((published_at IS NULL) OR ((status)::text = ANY ((ARRAY['COMPLETE'::character varying, 'DEGRADED'::character varying])::text[])))),
    CONSTRAINT equity_bar_publications_expected_members_check CHECK ((expected_members >= 0)),
    CONSTRAINT equity_bar_publications_failed_members_check CHECK ((failed_members >= 0)),
    CONSTRAINT equity_bar_publications_input_sha256_check CHECK ((input_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT equity_bar_publications_interval_check CHECK ((("interval")::text = ANY ((ARRAY['1m'::character varying, '5m'::character varying, '15m'::character varying, '30m'::character varying, '1h'::character varying, '1d'::character varying, '1wk'::character varying, '1mo'::character varying])::text[]))),
    CONSTRAINT equity_bar_publications_missing_members_check CHECK ((missing_members >= 0)),
    CONSTRAINT equity_bar_publications_output_sha256_check CHECK (((output_sha256 IS NULL) OR (output_sha256 ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT equity_bar_publications_selected_members_check CHECK ((selected_members >= 0)),
    CONSTRAINT equity_bar_publications_selection_policy_sha256_check CHECK ((selection_policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT equity_bar_publications_session_scope_check CHECK (((session_scope)::text = ANY ((ARRAY['RTH'::character varying, 'EXTENDED'::character varying, 'FULL_DAY'::character varying])::text[]))),
    CONSTRAINT equity_bar_publications_status_check CHECK (((status)::text = ANY ((ARRAY['PENDING'::character varying, 'COMPLETE'::character varying, 'DEGRADED'::character varying, 'FAILED'::character varying])::text[])))
);


--
-- Name: equity_bar_revisions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_bar_revisions (
    bar_revision_id uuid NOT NULL,
    security_id uuid NOT NULL,
    ticker character varying(32) NOT NULL,
    "interval" character varying(8) NOT NULL,
    session_date date NOT NULL,
    bar_start timestamp with time zone NOT NULL,
    bar_end timestamp with time zone NOT NULL,
    open_price numeric(20,8) NOT NULL,
    high_price numeric(20,8) NOT NULL,
    low_price numeric(20,8) NOT NULL,
    close_price numeric(20,8) NOT NULL,
    volume numeric(28,8) NOT NULL,
    vwap numeric(20,8),
    transaction_count bigint,
    source_kind character varying(32) NOT NULL,
    availability_mode character varying(32) NOT NULL,
    is_final boolean NOT NULL,
    provider_published_at timestamp with time zone,
    system_observed_at timestamp with time zone NOT NULL,
    replay_available_at timestamp with time zone,
    ingestion_segment_id uuid,
    adjusted boolean DEFAULT false NOT NULL,
    payload_sha256 character(64) NOT NULL,
    source_bar_revision_ids uuid[] DEFAULT ARRAY[]::uuid[] NOT NULL,
    supersedes_bar_revision_id uuid,
    reconciliation_status character varying(24),
    quality_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    session_scope character varying(16) DEFAULT 'RTH'::character varying NOT NULL,
    CONSTRAINT ck_equity_bar_revision_session_scope CHECK (((session_scope)::text = ANY ((ARRAY['RTH'::character varying, 'EXTENDED'::character varying, 'FULL_DAY'::character varying])::text[]))),
    CONSTRAINT equity_bar_revisions_availability_mode_check CHECK (((availability_mode)::text = ANY ((ARRAY['LIVE_OBSERVED'::character varying, 'HISTORICAL_RECONSTRUCTED'::character varying])::text[]))),
    CONSTRAINT equity_bar_revisions_check CHECK ((bar_end > bar_start)),
    CONSTRAINT equity_bar_revisions_check1 CHECK ((high_price >= GREATEST(open_price, close_price, low_price))),
    CONSTRAINT equity_bar_revisions_check2 CHECK ((low_price <= LEAST(open_price, close_price, high_price))),
    CONSTRAINT equity_bar_revisions_check3 CHECK (((NOT is_final) OR (system_observed_at >= bar_end))),
    CONSTRAINT equity_bar_revisions_close_price_check CHECK ((close_price > (0)::numeric)),
    CONSTRAINT equity_bar_revisions_high_price_check CHECK ((high_price > (0)::numeric)),
    CONSTRAINT equity_bar_revisions_interval_check CHECK ((("interval")::text = ANY ((ARRAY['1m'::character varying, '5m'::character varying, '15m'::character varying, '30m'::character varying, '1h'::character varying, '1d'::character varying, '1wk'::character varying, '1mo'::character varying])::text[]))),
    CONSTRAINT equity_bar_revisions_low_price_check CHECK ((low_price > (0)::numeric)),
    CONSTRAINT equity_bar_revisions_open_price_check CHECK ((open_price > (0)::numeric)),
    CONSTRAINT equity_bar_revisions_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT equity_bar_revisions_reconciliation_status_check CHECK (((reconciliation_status IS NULL) OR ((reconciliation_status)::text = ANY ((ARRAY['PENDING'::character varying, 'MATCHED'::character varying, 'CORRECTED'::character varying, 'NATIVE_MISSING'::character varying, 'DERIVED_MISSING'::character varying])::text[])))),
    CONSTRAINT equity_bar_revisions_source_kind_check CHECK (((source_kind)::text = ANY ((ARRAY['NATIVE_REST'::character varying, 'REALTIME_STREAM'::character varying, 'DERIVED'::character varying, 'RECONCILED'::character varying])::text[]))),
    CONSTRAINT equity_bar_revisions_transaction_count_check CHECK (((transaction_count IS NULL) OR (transaction_count >= 0))),
    CONSTRAINT equity_bar_revisions_volume_check CHECK ((volume >= (0)::numeric)),
    CONSTRAINT equity_bar_revisions_vwap_check CHECK (((vwap IS NULL) OR (vwap > (0)::numeric)))
);


--
-- Name: equity_canonical_bars; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.equity_canonical_bars AS
 SELECT bar_revision_id,
    security_id,
    ticker,
    "interval",
    session_date,
    session_scope,
    adjusted,
    bar_start,
    bar_end,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    vwap,
    transaction_count,
    source_kind,
    availability_mode,
    provider_published_at,
    system_observed_at,
    replay_available_at,
    source_bar_revision_ids,
    supersedes_bar_revision_id,
    reconciliation_status,
    quality_codes
   FROM ( SELECT revision.bar_revision_id,
            revision.security_id,
            revision.ticker,
            revision."interval",
            revision.session_date,
            revision.bar_start,
            revision.bar_end,
            revision.open_price,
            revision.high_price,
            revision.low_price,
            revision.close_price,
            revision.volume,
            revision.vwap,
            revision.transaction_count,
            revision.source_kind,
            revision.availability_mode,
            revision.is_final,
            revision.provider_published_at,
            revision.system_observed_at,
            revision.replay_available_at,
            revision.ingestion_segment_id,
            revision.adjusted,
            revision.payload_sha256,
            revision.source_bar_revision_ids,
            revision.supersedes_bar_revision_id,
            revision.reconciliation_status,
            revision.quality_codes,
            revision.created_at,
            revision.session_scope,
            row_number() OVER (PARTITION BY revision.ticker, revision."interval", revision.bar_start, revision.session_scope, revision.adjusted ORDER BY
                CASE
                    WHEN ((revision.source_kind)::text = 'RECONCILED'::text) THEN 0
                    WHEN (((revision."interval")::text = ANY ((ARRAY['1m'::character varying, '5m'::character varying, '15m'::character varying, '30m'::character varying])::text[])) AND ((revision.source_kind)::text = 'NATIVE_REST'::text)) THEN 1
                    WHEN (((revision."interval")::text = ANY ((ARRAY['1m'::character varying, '5m'::character varying, '15m'::character varying, '30m'::character varying])::text[])) AND ((revision.source_kind)::text = 'REALTIME_STREAM'::text)) THEN 2
                    WHEN (((revision."interval")::text = ANY ((ARRAY['1h'::character varying, '1d'::character varying, '1wk'::character varying, '1mo'::character varying])::text[])) AND ((revision.source_kind)::text = 'DERIVED'::text)) THEN 1
                    WHEN ((revision.source_kind)::text = 'NATIVE_REST'::text) THEN 2
                    WHEN ((revision.source_kind)::text = 'DERIVED'::text) THEN 3
                    ELSE 4
                END, COALESCE(revision.replay_available_at, revision.system_observed_at) DESC, revision.created_at DESC) AS canonical_rank
           FROM public.equity_bar_revisions revision
          WHERE ((revision.is_final = true) AND ((revision.session_scope)::text = 'RTH'::text) AND (revision.adjusted = false))) ranked
  WHERE (canonical_rank = 1);


--
-- Name: equity_canonical_daily_bars; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.equity_canonical_daily_bars AS
 SELECT bar_revision_id,
    security_id,
    ticker,
    session_date,
    bar_start AS datetime,
    bar_end,
    open_price,
    high_price AS high,
    low_price AS low,
    close_price,
    volume,
    vwap,
    transaction_count,
    source_kind,
    availability_mode,
    system_observed_at,
    replay_available_at
   FROM public.equity_canonical_bars
  WHERE (("interval")::text = '1d'::text);


--
-- Name: equity_canonical_hourly_bars; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.equity_canonical_hourly_bars AS
 SELECT bar_revision_id,
    security_id,
    ticker,
    session_date,
    bar_start AS datetime,
    bar_end,
    open_price,
    high_price AS high,
    low_price AS low,
    close_price,
    volume,
    vwap,
    transaction_count,
    source_kind,
    availability_mode,
    system_observed_at,
    replay_available_at
   FROM public.equity_canonical_bars
  WHERE (("interval")::text = '1h'::text);


--
-- Name: equity_context_evidence; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_context_evidence (
    equity_context_snapshot_id uuid NOT NULL,
    evidence_id uuid NOT NULL,
    evidence_role character varying(24) NOT NULL,
    ordinal smallint DEFAULT 0 NOT NULL,
    CONSTRAINT equity_context_evidence_evidence_role_check CHECK (((evidence_role)::text = ANY ((ARRAY['REGIME'::character varying, 'DIRECTION'::character varying, 'LOCATION'::character varying, 'TRIGGER'::character varying, 'PARTICIPATION'::character varying, 'RISK'::character varying, 'SETUP'::character varying])::text[]))),
    CONSTRAINT equity_context_evidence_ordinal_check CHECK ((ordinal >= 0))
);


--
-- Name: equity_context_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_context_snapshots (
    equity_context_snapshot_id uuid NOT NULL,
    security_id uuid NOT NULL,
    ticker character varying(32) NOT NULL,
    strategy_horizon character varying(32) NOT NULL,
    market_time timestamp with time zone NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    valid_until timestamp with time zone,
    status character varying(24) NOT NULL,
    universe_run_id uuid,
    security_revision_id uuid,
    fundamental_snapshot_id uuid,
    regime_state character varying(32),
    ema_direction character varying(16),
    qualified_direction character varying(16),
    direction_qualification_id uuid,
    direction_evidence_id uuid,
    direction_horizon character varying(32),
    direction_valid_until timestamp with time zone,
    trigger_state character varying(32),
    trigger_valid_until timestamp with time zone,
    range_forecast_id uuid,
    range_lower numeric(20,8),
    range_upper numeric(20,8),
    range_valid_until timestamp with time zone,
    market_cap numeric(24,4),
    shares_outstanding numeric(24,4),
    free_float numeric(24,4),
    dividend_yield double precision,
    enterprise_value numeric(28,4),
    ebitda numeric(28,4),
    operating_income numeric(28,4),
    free_cash_flow numeric(28,4),
    risk_levels jsonb DEFAULT '{}'::jsonb NOT NULL,
    conflict_state jsonb DEFAULT '{}'::jsonb NOT NULL,
    stale_components jsonb DEFAULT '[]'::jsonb NOT NULL,
    reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    summary jsonb DEFAULT '{}'::jsonb NOT NULL,
    context_policy_version character varying(64) NOT NULL,
    context_policy_sha256 character(64) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT equity_context_snapshots_check CHECK ((observed_at >= market_time)),
    CONSTRAINT equity_context_snapshots_check1 CHECK (((valid_until IS NULL) OR (valid_until > market_time))),
    CONSTRAINT equity_context_snapshots_check2 CHECK (((range_upper IS NULL) OR (range_lower IS NULL) OR (range_upper >= range_lower))),
    CONSTRAINT equity_context_snapshots_check3 CHECK (((qualified_direction IS NULL) OR ((direction_qualification_id IS NOT NULL) AND (direction_evidence_id IS NOT NULL)))),
    CONSTRAINT equity_context_snapshots_conflict_state_check CHECK ((jsonb_typeof(conflict_state) = 'object'::text)),
    CONSTRAINT equity_context_snapshots_context_policy_sha256_check CHECK ((context_policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT equity_context_snapshots_ema_direction_check CHECK (((ema_direction IS NULL) OR ((ema_direction)::text = ANY ((ARRAY['BULLISH'::character varying, 'BEARISH'::character varying, 'NEUTRAL'::character varying])::text[])))),
    CONSTRAINT equity_context_snapshots_qualified_direction_check CHECK (((qualified_direction IS NULL) OR ((qualified_direction)::text = ANY ((ARRAY['BULLISH'::character varying, 'BEARISH'::character varying, 'NEUTRAL'::character varying])::text[])))),
    CONSTRAINT equity_context_snapshots_risk_levels_check CHECK ((jsonb_typeof(risk_levels) = 'object'::text)),
    CONSTRAINT equity_context_snapshots_stale_components_check CHECK ((jsonb_typeof(stale_components) = 'array'::text)),
    CONSTRAINT equity_context_snapshots_status_check CHECK (((status)::text = ANY ((ARRAY['COMPLETE'::character varying, 'DEGRADED'::character varying, 'CONFLICTED'::character varying, 'UNAVAILABLE'::character varying, 'FAILED'::character varying])::text[]))),
    CONSTRAINT equity_context_snapshots_summary_check CHECK ((jsonb_typeof(summary) = 'object'::text))
);


--
-- Name: equity_corporate_actions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_corporate_actions (
    corporate_action_id uuid NOT NULL,
    security_id uuid NOT NULL,
    ticker character varying(32) NOT NULL,
    action_type character varying(24) NOT NULL,
    effective_date date NOT NULL,
    declaration_date date,
    ex_date date,
    record_date date,
    pay_date date,
    cash_amount numeric(20,8),
    split_from numeric(20,8),
    split_to numeric(20,8),
    new_ticker character varying(32),
    source character varying(64) NOT NULL,
    source_key character varying(256) NOT NULL,
    first_observed_at timestamp with time zone NOT NULL,
    revised_observed_at timestamp with time zone,
    payload_sha256 character(64) NOT NULL,
    raw_payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    availability_mode character varying(32) DEFAULT 'LIVE_OBSERVED'::character varying NOT NULL,
    replay_available_at timestamp with time zone,
    CONSTRAINT ck_equity_corporate_action_availability CHECK ((((availability_mode)::text = ANY ((ARRAY['LIVE_OBSERVED'::character varying, 'HISTORICAL_RECONSTRUCTED'::character varying])::text[])) AND ((((availability_mode)::text = 'LIVE_OBSERVED'::text) AND (replay_available_at IS NULL)) OR (((availability_mode)::text = 'HISTORICAL_RECONSTRUCTED'::text) AND (replay_available_at IS NOT NULL) AND (replay_available_at <= first_observed_at))))),
    CONSTRAINT equity_corporate_actions_action_type_check CHECK (((action_type)::text = ANY ((ARRAY['SPLIT'::character varying, 'DIVIDEND'::character varying, 'SYMBOL_CHANGE'::character varying, 'SPINOFF'::character varying, 'MERGER'::character varying, 'OTHER'::character varying])::text[]))),
    CONSTRAINT equity_corporate_actions_check CHECK (((revised_observed_at IS NULL) OR (revised_observed_at >= first_observed_at))),
    CONSTRAINT equity_corporate_actions_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT equity_corporate_actions_raw_payload_check CHECK ((jsonb_typeof(raw_payload) = 'object'::text))
);


--
-- Name: equity_current_bar_projection; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_current_bar_projection (
    ticker character varying(32) NOT NULL,
    "interval" character varying(8) NOT NULL,
    bar_start timestamp with time zone NOT NULL,
    bar_end timestamp with time zone NOT NULL,
    session_scope character varying(16) NOT NULL,
    adjusted boolean NOT NULL,
    selected_bar_revision_id uuid NOT NULL,
    publication_id uuid NOT NULL,
    selection_policy_version character varying(64) NOT NULL,
    selection_policy_sha256 character(64) NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    published_at timestamp with time zone NOT NULL,
    CONSTRAINT equity_current_bar_projection_check CHECK ((bar_end > bar_start)),
    CONSTRAINT equity_current_bar_projection_check1 CHECK ((published_at >= observed_at)),
    CONSTRAINT equity_current_bar_projection_interval_check CHECK ((("interval")::text = ANY ((ARRAY['1m'::character varying, '5m'::character varying, '15m'::character varying, '30m'::character varying, '1h'::character varying, '1d'::character varying, '1wk'::character varying, '1mo'::character varying])::text[]))),
    CONSTRAINT equity_current_bar_projection_selection_policy_sha256_check CHECK ((selection_policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT equity_current_bar_projection_session_scope_check CHECK (((session_scope)::text = ANY ((ARRAY['RTH'::character varying, 'EXTENDED'::character varying, 'FULL_DAY'::character varying])::text[])))
);


--
-- Name: equity_current_projection; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_current_projection (
    ticker character varying(32) NOT NULL,
    interval_key character varying(32) NOT NULL,
    projection_type character varying(32) NOT NULL,
    source_name character varying(96) NOT NULL,
    evidence_id uuid,
    equity_context_snapshot_id uuid,
    analysis_run_id uuid,
    market_time timestamp with time zone NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    published_at timestamp with time zone NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT equity_current_projection_check CHECK ((observed_at >= market_time)),
    CONSTRAINT equity_current_projection_check1 CHECK ((published_at >= observed_at)),
    CONSTRAINT equity_current_projection_check2 CHECK (((evidence_id IS NOT NULL) OR (equity_context_snapshot_id IS NOT NULL))),
    CONSTRAINT equity_current_projection_payload_check CHECK ((jsonb_typeof(payload) = 'object'::text))
);


--
-- Name: equity_evidence; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_evidence (
    evidence_id uuid NOT NULL,
    evidence_key character varying(384) NOT NULL,
    lifecycle_key character varying(320),
    evidence_type character varying(32) NOT NULL,
    evidence_role character varying(24) NOT NULL,
    security_id uuid NOT NULL,
    ticker character varying(32) NOT NULL,
    "interval" character varying(8),
    direction smallint,
    lifecycle_status character varying(24) NOT NULL,
    strength double precision,
    market_time timestamp with time zone NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    valid_until timestamp with time zone,
    source_name character varying(96) NOT NULL,
    source_version character varying(64) NOT NULL,
    payload_schema_version character varying(32) NOT NULL,
    analysis_run_id uuid,
    latest_bar_revision_id uuid,
    security_revision_id uuid,
    fundamental_report_ids uuid[] DEFAULT ARRAY[]::uuid[] NOT NULL,
    source_revision_ids uuid[] DEFAULT ARRAY[]::uuid[] NOT NULL,
    source_window_sha256 character(64),
    quality_state character varying(24) NOT NULL,
    quality_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    qualification_revision_id uuid,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    payload_sha256 character(64) NOT NULL,
    supersedes_evidence_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT equity_evidence_check CHECK ((observed_at >= market_time)),
    CONSTRAINT equity_evidence_check1 CHECK (((valid_until IS NULL) OR (valid_until > market_time))),
    CONSTRAINT equity_evidence_direction_check CHECK (((direction IS NULL) OR (direction = ANY (ARRAY['-1'::integer, 0, 1])))),
    CONSTRAINT equity_evidence_evidence_role_check CHECK (((evidence_role)::text = ANY ((ARRAY['REGIME'::character varying, 'DIRECTION'::character varying, 'LOCATION'::character varying, 'TRIGGER'::character varying, 'PARTICIPATION'::character varying, 'RISK'::character varying, 'SETUP'::character varying])::text[]))),
    CONSTRAINT equity_evidence_evidence_type_check CHECK (((evidence_type)::text = ANY ((ARRAY['FEATURE_SNAPSHOT'::character varying, 'FUNDAMENTAL_SNAPSHOT'::character varying, 'SCANNER_RESULT'::character varying, 'REGIME_SIGNAL'::character varying, 'PATTERN_OBSERVATION'::character varying, 'PRICE_CHANNEL'::character varying, 'TRADE_SETUP'::character varying, 'RANGE_FORECAST'::character varying, 'MARKET_REGIME'::character varying])::text[]))),
    CONSTRAINT equity_evidence_lifecycle_status_check CHECK (((lifecycle_status)::text = ANY ((ARRAY['SNAPSHOT'::character varying, 'MATCH'::character varying, 'FORMING'::character varying, 'AT_EDGE'::character varying, 'CONFIRMED'::character varying, 'INVALIDATED'::character varying, 'EXPIRED'::character varying, 'CONFLICTED'::character varying, 'UNAVAILABLE'::character varying])::text[]))),
    CONSTRAINT equity_evidence_payload_check CHECK ((jsonb_typeof(payload) = 'object'::text)),
    CONSTRAINT equity_evidence_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT equity_evidence_quality_state_check CHECK (((quality_state)::text = ANY ((ARRAY['COMPLETE'::character varying, 'DEGRADED'::character varying, 'RESEARCH_ONLY'::character varying, 'STALE'::character varying, 'FAILED'::character varying])::text[]))),
    CONSTRAINT equity_evidence_source_window_sha256_check CHECK (((source_window_sha256 IS NULL) OR (source_window_sha256 ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT equity_evidence_strength_check CHECK (((strength IS NULL) OR ((strength >= (0)::double precision) AND (strength <= (1)::double precision))))
);


--
-- Name: equity_fundamental_reports; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_fundamental_reports (
    fundamental_report_id uuid NOT NULL,
    security_id uuid NOT NULL,
    security_revision_id uuid,
    cik character varying(16),
    accession_number character varying(32),
    form_type character varying(16),
    timeframe character varying(32) NOT NULL,
    fiscal_year integer,
    fiscal_quarter smallint,
    period_end date NOT NULL,
    filing_date date,
    availability_time timestamp with time zone NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    revenue numeric(28,4),
    gross_profit numeric(28,4),
    operating_income numeric(28,4),
    ebitda numeric(28,4),
    pretax_income numeric(28,4),
    interest_expense numeric(28,4),
    income_taxes numeric(28,4),
    net_income numeric(28,4),
    basic_eps numeric(20,8),
    diluted_eps numeric(20,8),
    basic_weighted_shares numeric(24,4),
    diluted_weighted_shares numeric(24,4),
    research_and_development numeric(28,4),
    selling_general_admin numeric(28,4),
    depreciation_amortization numeric(28,4),
    cash_and_equivalents numeric(28,4),
    short_term_investments numeric(28,4),
    current_assets numeric(28,4),
    current_liabilities numeric(28,4),
    total_assets numeric(28,4),
    current_debt numeric(28,4),
    long_term_debt numeric(28,4),
    total_liabilities numeric(28,4),
    total_equity numeric(28,4),
    operating_cash_flow numeric(28,4),
    capital_expenditures numeric(28,4),
    free_cash_flow numeric(28,4),
    dividends numeric(28,4),
    investing_cash_flow numeric(28,4),
    financing_cash_flow numeric(28,4),
    source character varying(64) NOT NULL,
    source_key character varying(256) NOT NULL,
    payload_sha256 character(64) NOT NULL,
    raw_payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    quality_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    supersedes_report_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT equity_fundamental_reports_check CHECK ((observed_at >= availability_time)),
    CONSTRAINT equity_fundamental_reports_fiscal_quarter_check CHECK (((fiscal_quarter IS NULL) OR ((fiscal_quarter >= 1) AND (fiscal_quarter <= 4)))),
    CONSTRAINT equity_fundamental_reports_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT equity_fundamental_reports_raw_payload_check CHECK ((jsonb_typeof(raw_payload) = 'object'::text)),
    CONSTRAINT equity_fundamental_reports_timeframe_check CHECK (((timeframe)::text = ANY ((ARRAY['quarterly'::character varying, 'annual'::character varying, 'trailing_twelve_months'::character varying])::text[])))
);


--
-- Name: equity_ingestion_segments; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_ingestion_segments (
    ingestion_segment_id uuid NOT NULL,
    provider character varying(64) NOT NULL,
    provider_mode character varying(24) NOT NULL,
    dataset character varying(64) NOT NULL,
    "interval" character varying(8),
    requested_from timestamp with time zone,
    requested_to timestamp with time zone,
    market_watermark timestamp with time zone,
    observed_at timestamp with time zone NOT NULL,
    status character varying(24) NOT NULL,
    record_count bigint DEFAULT 0 NOT NULL,
    byte_count bigint DEFAULT 0 NOT NULL,
    checksum_sha256 character(64),
    archive_uri text,
    gap_details jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT ck_equity_ingestion_segment_provider_mode CHECK (((provider_mode)::text = ANY ((ARRAY['REST'::character varying, 'WEBSOCKET'::character varying, 'FLAT_FILE'::character varying, 'REPLAY'::character varying, 'DERIVED'::character varying])::text[]))),
    CONSTRAINT equity_ingestion_segments_byte_count_check CHECK ((byte_count >= 0)),
    CONSTRAINT equity_ingestion_segments_check CHECK (((requested_to IS NULL) OR (requested_from IS NULL) OR (requested_to >= requested_from))),
    CONSTRAINT equity_ingestion_segments_check1 CHECK (((completed_at IS NULL) OR (completed_at >= created_at))),
    CONSTRAINT equity_ingestion_segments_checksum_sha256_check CHECK (((checksum_sha256 IS NULL) OR (checksum_sha256 ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT equity_ingestion_segments_gap_details_check CHECK ((jsonb_typeof(gap_details) = 'object'::text)),
    CONSTRAINT equity_ingestion_segments_record_count_check CHECK ((record_count >= 0)),
    CONSTRAINT equity_ingestion_segments_status_check CHECK (((status)::text = ANY ((ARRAY['WRITING'::character varying, 'COMPLETE'::character varying, 'DEGRADED'::character varying, 'FAILED'::character varying, 'QUARANTINED'::character varying])::text[])))
);


--
-- Name: equity_outcome_policies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_outcome_policies (
    outcome_policy_id uuid NOT NULL,
    policy_key character varying(160) NOT NULL,
    policy_version character varying(64) NOT NULL,
    evidence_type character varying(32) NOT NULL,
    source_name character varying(96),
    source_version character varying(64),
    "interval" character varying(8),
    direction_contract character varying(32) NOT NULL,
    eligibility_transition character varying(64) NOT NULL,
    entry_model character varying(64) NOT NULL,
    horizons jsonb NOT NULL,
    cost_model jsonb NOT NULL,
    benchmark_policy jsonb NOT NULL,
    ambiguity_policy character varying(48) NOT NULL,
    success_definition jsonb NOT NULL,
    missingness_policy jsonb NOT NULL,
    independence_policy jsonb NOT NULL,
    effective_from timestamp with time zone NOT NULL,
    effective_to timestamp with time zone,
    policy_sha256 character(64) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_equity_outcome_policy_horizons_object CHECK ((jsonb_typeof(horizons) = 'object'::text)),
    CONSTRAINT equity_outcome_policies_ambiguity_policy_check CHECK (((ambiguity_policy)::text = ANY ((ARRAY['CONSERVATIVE_STOP_FIRST'::character varying, 'TARGET_FIRST_SENSITIVITY'::character varying, 'EXCLUDE_AMBIGUOUS_SENSITIVITY'::character varying])::text[]))),
    CONSTRAINT equity_outcome_policies_benchmark_policy_check CHECK ((jsonb_typeof(benchmark_policy) = 'object'::text)),
    CONSTRAINT equity_outcome_policies_check CHECK (((effective_to IS NULL) OR (effective_to > effective_from))),
    CONSTRAINT equity_outcome_policies_cost_model_check CHECK ((jsonb_typeof(cost_model) = 'object'::text)),
    CONSTRAINT equity_outcome_policies_independence_policy_check CHECK ((jsonb_typeof(independence_policy) = 'object'::text)),
    CONSTRAINT equity_outcome_policies_missingness_policy_check CHECK ((jsonb_typeof(missingness_policy) = 'object'::text)),
    CONSTRAINT equity_outcome_policies_policy_sha256_check CHECK ((policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT equity_outcome_policies_success_definition_check CHECK ((jsonb_typeof(success_definition) = 'object'::text))
);


--
-- Name: equity_portal_current_projections; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_portal_current_projections (
    snapshot_type character varying(40) NOT NULL,
    snapshot_id uuid NOT NULL,
    published_at timestamp with time zone NOT NULL
);


--
-- Name: equity_portal_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_portal_snapshots (
    snapshot_id uuid NOT NULL,
    snapshot_type character varying(40) NOT NULL,
    source_generation bigint NOT NULL,
    source_manifest jsonb NOT NULL,
    source_manifest_sha256 character(64) NOT NULL,
    payload jsonb NOT NULL,
    payload_sha256 character(64) NOT NULL,
    generated_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_equity_portal_snapshot_type CHECK (((snapshot_type)::text = ANY ((ARRAY['TICKER_OVERVIEW'::character varying, 'MARKET_REGIME'::character varying, 'SECTOR_PERFORMANCE_1'::character varying, 'SECTOR_PERFORMANCE_5'::character varying, 'SECTOR_PERFORMANCE_10'::character varying, 'SECTOR_PERFORMANCE_21'::character varying, 'SECTOR_INTELLIGENCE'::character varying, 'SCAN_GAPS_1D'::character varying, 'SCAN_FVG_1D_50'::character varying, 'SCAN_MA_1D_9_21'::character varying, 'SCAN_MOMENTUM_1D'::character varying, 'SCAN_BEARISH_1D'::character varying, 'SCAN_FIBONACCI_1D_5'::character varying, 'SCAN_ALL_1D_5'::character varying, 'STREAK_GAPS_5'::character varying, 'STREAK_MA_5'::character varying, 'STREAK_MOMENTUM_5'::character varying, 'STREAK_BEARISH_5'::character varying, 'STREAK_FIBONACCI_5'::character varying, 'STREAK_SUMMARY_3_5'::character varying])::text[]))),
    CONSTRAINT equity_portal_snapshots_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT equity_portal_snapshots_source_generation_check CHECK ((source_generation >= 0)),
    CONSTRAINT equity_portal_snapshots_source_manifest_check CHECK ((jsonb_typeof(source_manifest) = 'object'::text)),
    CONSTRAINT equity_portal_snapshots_source_manifest_sha256_check CHECK ((source_manifest_sha256 ~ '^[0-9a-f]{64}$'::text))
);


--
-- Name: equity_portal_source_state; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_portal_source_state (
    singleton boolean DEFAULT true NOT NULL,
    generation bigint DEFAULT 0 NOT NULL,
    changed_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT equity_portal_source_state_generation_check CHECK ((generation >= 0)),
    CONSTRAINT equity_portal_source_state_singleton_check CHECK (singleton)
);


--
-- Name: equity_qualification_revisions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_qualification_revisions (
    qualification_revision_id uuid NOT NULL,
    source_name character varying(96) NOT NULL,
    source_version character varying(64) NOT NULL,
    "interval" character varying(8),
    direction smallint,
    horizon_key character varying(32) NOT NULL,
    outcome_policy_key character varying(160) NOT NULL,
    evaluation_version character varying(64) NOT NULL,
    qualification_state character varying(24) NOT NULL,
    effective_from timestamp with time zone NOT NULL,
    effective_to timestamp with time zone,
    sample_size integer DEFAULT 0 NOT NULL,
    independent_periods integer DEFAULT 0 NOT NULL,
    mean_net_alpha double precision,
    alpha_t_stat double precision,
    alpha_fdr_q double precision,
    calibrated_probability double precision,
    probability_ci_low double precision,
    probability_ci_high double precision,
    brier_score double precision,
    brier_skill_score double precision,
    expected_calibration_error double precision,
    report_identity text,
    metrics jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_equity_qualification_report_identity CHECK (((report_identity IS NULL) OR (report_identity ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT ck_equity_qualification_research_scope CHECK (((metrics ? 'research_scope'::text) AND ((metrics ->> 'research_scope'::text) = ANY (ARRAY['EQUITY_SIGNAL'::text, 'OPTION_CONDITIONING'::text])))),
    CONSTRAINT equity_qualification_revisions_alpha_fdr_q_check CHECK (((alpha_fdr_q IS NULL) OR ((alpha_fdr_q >= (0)::double precision) AND (alpha_fdr_q <= (1)::double precision)))),
    CONSTRAINT equity_qualification_revisions_calibrated_probability_check CHECK (((calibrated_probability IS NULL) OR ((calibrated_probability >= (0)::double precision) AND (calibrated_probability <= (1)::double precision)))),
    CONSTRAINT equity_qualification_revisions_check CHECK (((effective_to IS NULL) OR (effective_to > effective_from))),
    CONSTRAINT equity_qualification_revisions_check1 CHECK (((probability_ci_low IS NULL) OR (probability_ci_high IS NULL) OR (probability_ci_high >= probability_ci_low))),
    CONSTRAINT equity_qualification_revisions_direction_check CHECK (((direction IS NULL) OR (direction = ANY (ARRAY['-1'::integer, 0, 1])))),
    CONSTRAINT equity_qualification_revisions_independent_periods_check CHECK ((independent_periods >= 0)),
    CONSTRAINT equity_qualification_revisions_metrics_check CHECK ((jsonb_typeof(metrics) = 'object'::text)),
    CONSTRAINT equity_qualification_revisions_qualification_state_check CHECK (((qualification_state)::text = ANY ((ARRAY['ROBUST_PASS'::character varying, 'MONITOR_ONLY'::character varying, 'UNRANKED'::character varying, 'REJECTED'::character varying])::text[]))),
    CONSTRAINT equity_qualification_revisions_sample_size_check CHECK ((sample_size >= 0))
);


--
-- Name: equity_research_outcomes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_research_outcomes (
    outcome_id uuid NOT NULL,
    subject_evidence_id uuid NOT NULL,
    outcome_policy_id uuid NOT NULL,
    horizon_key character varying(32) NOT NULL,
    outcome_revision integer DEFAULT 1 NOT NULL,
    entry_status character varying(32) NOT NULL,
    signal_time timestamp with time zone NOT NULL,
    confirmation_bar_id uuid,
    confirmation_bar_end timestamp with time zone,
    entry_bar_id uuid,
    entry_time timestamp with time zone,
    entry_price numeric(20,8),
    exit_bar_id uuid,
    exit_time timestamp with time zone,
    exit_price numeric(20,8),
    gross_return double precision,
    signed_return double precision,
    estimated_cost double precision,
    net_return double precision,
    market_return double precision,
    sector_return double precision,
    net_alpha double precision,
    sector_net_alpha double precision,
    mae_pct double precision,
    mfe_pct double precision,
    mae_r double precision,
    mfe_r double precision,
    stop_hit boolean,
    target_hit boolean,
    first_hit character varying(24),
    outcome_category character varying(32) NOT NULL,
    is_stale boolean DEFAULT false NOT NULL,
    outcome_available_at timestamp with time zone NOT NULL,
    quality_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    path_bar_ids uuid[] DEFAULT ARRAY[]::uuid[] NOT NULL,
    benchmark_bar_ids uuid[] DEFAULT ARRAY[]::uuid[] NOT NULL,
    supersedes_outcome_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    market_benchmark_ticker character varying(16),
    sector_benchmark_ticker character varying(16),
    CONSTRAINT equity_research_outcomes_check CHECK (((entry_time IS NULL) OR (entry_time > signal_time))),
    CONSTRAINT equity_research_outcomes_check1 CHECK (((confirmation_bar_end IS NULL) OR (entry_time IS NULL) OR (entry_time > confirmation_bar_end))),
    CONSTRAINT equity_research_outcomes_check2 CHECK (((exit_time IS NULL) OR (entry_time IS NULL) OR (exit_time >= entry_time))),
    CONSTRAINT equity_research_outcomes_entry_status_check CHECK (((entry_status)::text = ANY ((ARRAY['ENTERED'::character varying, 'NOT_TRIGGERED'::character varying, 'NO_LIQUID_BAR'::character varying, 'STALE'::character varying, 'UNAVAILABLE'::character varying])::text[]))),
    CONSTRAINT equity_research_outcomes_first_hit_check CHECK (((first_hit IS NULL) OR ((first_hit)::text = ANY ((ARRAY['STOP'::character varying, 'TARGET'::character varying, 'SAME_BAR'::character varying, 'NONE'::character varying])::text[])))),
    CONSTRAINT equity_research_outcomes_outcome_category_check CHECK (((outcome_category)::text = ANY ((ARRAY['WIN'::character varying, 'LOSS'::character varying, 'AMBIGUOUS_SAME_BAR'::character varying, 'NOT_ENTERED'::character varying, 'UNAVAILABLE'::character varying])::text[]))),
    CONSTRAINT equity_research_outcomes_outcome_revision_check CHECK ((outcome_revision > 0))
);


--
-- Name: equity_security_reference_revisions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_security_reference_revisions (
    security_revision_id uuid NOT NULL,
    security_id uuid NOT NULL,
    ticker character varying(32) NOT NULL,
    ticker_root character varying(32),
    ticker_suffix character varying(16),
    security_type character varying(24),
    active boolean NOT NULL,
    list_date date,
    delisted_date date,
    currency_name character varying(16),
    primary_exchange character varying(16),
    round_lot integer,
    company_name text,
    cik character varying(16),
    composite_figi character varying(32),
    share_class_figi character varying(32),
    sic_code character varying(16),
    sic_description text,
    sector character varying(96),
    industry character varying(160),
    description text,
    homepage_url text,
    branding jsonb DEFAULT '{}'::jsonb NOT NULL,
    contact jsonb DEFAULT '{}'::jsonb NOT NULL,
    total_employees bigint,
    share_class_shares numeric(24,4),
    weighted_shares numeric(24,4),
    free_float numeric(24,4),
    free_float_percent double precision,
    market_cap numeric(24,4),
    source character varying(64) NOT NULL,
    source_as_of_date date,
    effective_from timestamp with time zone NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    payload_sha256 character(64) NOT NULL,
    raw_payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    supersedes_revision_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT equity_security_reference_revisions_branding_check CHECK ((jsonb_typeof(branding) = 'object'::text)),
    CONSTRAINT equity_security_reference_revisions_check CHECK ((observed_at >= effective_from)),
    CONSTRAINT equity_security_reference_revisions_check1 CHECK (((delisted_date IS NULL) OR (list_date IS NULL) OR (delisted_date >= list_date))),
    CONSTRAINT equity_security_reference_revisions_contact_check CHECK ((jsonb_typeof(contact) = 'object'::text)),
    CONSTRAINT equity_security_reference_revisions_free_float_check CHECK (((free_float IS NULL) OR (free_float >= (0)::numeric))),
    CONSTRAINT equity_security_reference_revisions_free_float_percent_check CHECK (((free_float_percent IS NULL) OR ((free_float_percent >= (0)::double precision) AND (free_float_percent <= (100)::double precision)))),
    CONSTRAINT equity_security_reference_revisions_market_cap_check CHECK (((market_cap IS NULL) OR (market_cap >= (0)::numeric))),
    CONSTRAINT equity_security_reference_revisions_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT equity_security_reference_revisions_raw_payload_check CHECK ((jsonb_typeof(raw_payload) = 'object'::text)),
    CONSTRAINT equity_security_reference_revisions_round_lot_check CHECK (((round_lot IS NULL) OR (round_lot > 0))),
    CONSTRAINT equity_security_reference_revisions_share_class_shares_check CHECK (((share_class_shares IS NULL) OR (share_class_shares >= (0)::numeric))),
    CONSTRAINT equity_security_reference_revisions_total_employees_check CHECK (((total_employees IS NULL) OR (total_employees >= 0))),
    CONSTRAINT equity_security_reference_revisions_weighted_shares_check CHECK (((weighted_shares IS NULL) OR (weighted_shares >= (0)::numeric)))
);


--
-- Name: equity_universe_members; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_universe_members (
    universe_run_id uuid NOT NULL,
    security_id uuid NOT NULL,
    security_revision_id uuid,
    ticker character varying(32) NOT NULL,
    member_rank integer,
    score double precision,
    effective_from timestamp with time zone NOT NULL,
    effective_to timestamp with time zone,
    first_observed_at timestamp with time zone NOT NULL,
    reasons text[] DEFAULT ARRAY[]::text[] NOT NULL,
    CONSTRAINT equity_universe_members_check CHECK ((first_observed_at >= effective_from)),
    CONSTRAINT equity_universe_members_check1 CHECK (((effective_to IS NULL) OR (effective_to > effective_from))),
    CONSTRAINT equity_universe_members_member_rank_check CHECK (((member_rank IS NULL) OR (member_rank > 0)))
);


--
-- Name: equity_universe_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.equity_universe_runs (
    universe_run_id uuid NOT NULL,
    source character varying(64) NOT NULL,
    mode character varying(24) NOT NULL,
    effective_from timestamp with time zone NOT NULL,
    observed_at timestamp with time zone NOT NULL,
    expected_members integer NOT NULL,
    admitted_members integer DEFAULT 0 NOT NULL,
    status character varying(24) NOT NULL,
    policy_version character varying(64) NOT NULL,
    policy_sha256 character(64) NOT NULL,
    configuration jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    availability_mode character varying(32) DEFAULT 'LIVE_OBSERVED'::character varying NOT NULL,
    replay_available_at timestamp with time zone,
    source_request_sha256 character(64),
    CONSTRAINT ck_equity_universe_availability_mode CHECK ((((availability_mode)::text = ANY ((ARRAY['LIVE_OBSERVED'::character varying, 'HISTORICAL_RECONSTRUCTED'::character varying])::text[])) AND ((((availability_mode)::text = 'LIVE_OBSERVED'::text) AND (replay_available_at IS NULL)) OR (((availability_mode)::text = 'HISTORICAL_RECONSTRUCTED'::text) AND (replay_available_at IS NOT NULL) AND (replay_available_at >= effective_from) AND (replay_available_at <= observed_at))))),
    CONSTRAINT ck_equity_universe_source_request_sha256 CHECK (((source_request_sha256 IS NULL) OR (source_request_sha256 ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT equity_universe_runs_admitted_members_check CHECK ((admitted_members >= 0)),
    CONSTRAINT equity_universe_runs_check CHECK ((observed_at >= effective_from)),
    CONSTRAINT equity_universe_runs_check1 CHECK (((completed_at IS NULL) OR (completed_at >= created_at))),
    CONSTRAINT equity_universe_runs_configuration_check CHECK ((jsonb_typeof(configuration) = 'object'::text)),
    CONSTRAINT equity_universe_runs_expected_members_check CHECK ((expected_members >= 0)),
    CONSTRAINT equity_universe_runs_mode_check CHECK (((mode)::text = ANY ((ARRAY['FIXED'::character varying, 'RANKED'::character varying, 'REPLAY'::character varying])::text[]))),
    CONSTRAINT equity_universe_runs_policy_sha256_check CHECK ((policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT equity_universe_runs_status_check CHECK (((status)::text = ANY ((ARRAY['PENDING'::character varying, 'RUNNING'::character varying, 'COMPLETE'::character varying, 'DEGRADED'::character varying, 'FAILED'::character varying])::text[])))
);


--
-- Name: market_context_daily; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.market_context_daily (
    context_id bigint NOT NULL,
    trade_date date NOT NULL,
    spy_regime character varying(20),
    spy_score integer,
    qqq_regime character varying(20),
    qqq_score integer,
    dia_regime character varying(20),
    dia_score integer,
    iwm_regime character varying(20),
    iwm_score integer,
    broad_market_regime character varying(20),
    growth_market_regime character varying(20),
    breadth_score integer,
    market_consensus_pct integer,
    has_sector_divergence boolean,
    divergence_type character varying(100),
    spy_session_return_pct numeric(6,3),
    qqq_session_return_pct numeric(6,3),
    dia_session_return_pct numeric(6,3),
    iwm_session_return_pct numeric(6,3),
    created_at timestamp with time zone DEFAULT now(),
    CONSTRAINT market_context_daily_breadth_score_check CHECK (((breadth_score >= 0) AND (breadth_score <= 100))),
    CONSTRAINT market_context_daily_dia_score_check CHECK (((dia_score >= 0) AND (dia_score <= 100))),
    CONSTRAINT market_context_daily_iwm_score_check CHECK (((iwm_score >= 0) AND (iwm_score <= 100))),
    CONSTRAINT market_context_daily_qqq_score_check CHECK (((qqq_score >= 0) AND (qqq_score <= 100))),
    CONSTRAINT market_context_daily_spy_score_check CHECK (((spy_score >= 0) AND (spy_score <= 100)))
);


--
-- Name: market_context_daily_context_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.market_context_daily_context_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: market_context_daily_context_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.market_context_daily_context_id_seq OWNED BY public.market_context_daily.context_id;


--
-- Name: market_discovery_states; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.market_discovery_states (
    discovery_id bigint NOT NULL,
    trade_date date NOT NULL,
    ticker character varying(16) NOT NULL,
    model_version character varying(32) NOT NULL,
    state character varying(32) NOT NULL,
    validation_status character varying(32) NOT NULL,
    activity_percentile double precision,
    echo_percentile double precision,
    older_momentum_percentile double precision,
    long_momentum_percentile double precision,
    recent_21d_percentile double precision,
    recent_21d_return double precision,
    recent_5d_return double precision,
    close_price double precision,
    sma_20 double precision,
    sma_50 double precision,
    higher_swing_high boolean,
    higher_swing_low boolean,
    evidence jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: market_discovery_states_discovery_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.market_discovery_states_discovery_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: market_discovery_states_discovery_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.market_discovery_states_discovery_id_seq OWNED BY public.market_discovery_states.discovery_id;


--
-- Name: opening_pattern_scores; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.opening_pattern_scores (
    score_id bigint NOT NULL,
    trade_date date NOT NULL,
    ticker character varying(10) NOT NULL,
    sector character varying(50),
    breakout_score integer,
    vwap_score integer,
    volatility_score integer,
    trend_score integer,
    rs_score integer,
    calendar_score integer,
    breakout_fired boolean DEFAULT false,
    vwap_fired boolean DEFAULT false,
    volatility_fired boolean DEFAULT false,
    trend_fired boolean DEFAULT false,
    rs_fired boolean DEFAULT false,
    calendar_fired boolean DEFAULT false,
    primary_regime character varying(20),
    sector_regime character varying(20),
    market_breadth_score integer,
    analog_match_count integer DEFAULT 0,
    analog_win_rate numeric(5,2),
    created_at timestamp with time zone DEFAULT now(),
    CONSTRAINT opening_pattern_scores_breakout_score_check CHECK (((breakout_score >= 0) AND (breakout_score <= 100))),
    CONSTRAINT opening_pattern_scores_calendar_score_check CHECK (((calendar_score >= 0) AND (calendar_score <= 100))),
    CONSTRAINT opening_pattern_scores_market_breadth_score_check CHECK (((market_breadth_score >= 0) AND (market_breadth_score <= 100))),
    CONSTRAINT opening_pattern_scores_rs_score_check CHECK (((rs_score >= 0) AND (rs_score <= 100))),
    CONSTRAINT opening_pattern_scores_trend_score_check CHECK (((trend_score >= 0) AND (trend_score <= 100))),
    CONSTRAINT opening_pattern_scores_volatility_score_check CHECK (((volatility_score >= 0) AND (volatility_score <= 100))),
    CONSTRAINT opening_pattern_scores_vwap_score_check CHECK (((vwap_score >= 0) AND (vwap_score <= 100)))
);


--
-- Name: opening_pattern_scores_score_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.opening_pattern_scores_score_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: opening_pattern_scores_score_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.opening_pattern_scores_score_id_seq OWNED BY public.opening_pattern_scores.score_id;


--
-- Name: option_analysis_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_analysis_runs (
    matrix_id uuid NOT NULL,
    batch_id uuid NOT NULL,
    underlying character varying(16) NOT NULL,
    market_time timestamp with time zone NOT NULL,
    observed_time timestamp with time zone NOT NULL,
    status character varying(32) NOT NULL,
    received_contract_count integer DEFAULT 0 NOT NULL,
    eligible_contract_count integer DEFAULT 0 NOT NULL,
    unknown_reference_count integer DEFAULT 0 NOT NULL,
    iv_attempt_count integer DEFAULT 0 NOT NULL,
    iv_converged_count integer DEFAULT 0 NOT NULL,
    iv_convergence_fraction double precision,
    quality_reasons text[] DEFAULT ARRAY[]::text[] NOT NULL,
    chain_health jsonb DEFAULT '{}'::jsonb NOT NULL,
    policy_version character varying(64) NOT NULL,
    policy_sha256 character(64) NOT NULL,
    model_version character varying(64) NOT NULL,
    started_at timestamp with time zone NOT NULL,
    completed_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_analysis_runs_chain_health_check CHECK ((jsonb_typeof(chain_health) = 'object'::text)),
    CONSTRAINT option_analysis_runs_check CHECK ((observed_time >= market_time)),
    CONSTRAINT option_analysis_runs_check1 CHECK ((iv_converged_count <= iv_attempt_count)),
    CONSTRAINT option_analysis_runs_check2 CHECK (((completed_at IS NULL) OR (completed_at >= started_at))),
    CONSTRAINT option_analysis_runs_eligible_contract_count_check CHECK ((eligible_contract_count >= 0)),
    CONSTRAINT option_analysis_runs_iv_attempt_count_check CHECK ((iv_attempt_count >= 0)),
    CONSTRAINT option_analysis_runs_iv_converged_count_check CHECK ((iv_converged_count >= 0)),
    CONSTRAINT option_analysis_runs_iv_convergence_fraction_check CHECK (((iv_convergence_fraction IS NULL) OR ((iv_convergence_fraction >= (0)::double precision) AND (iv_convergence_fraction <= (1)::double precision)))),
    CONSTRAINT option_analysis_runs_policy_sha256_check CHECK ((policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_analysis_runs_received_contract_count_check CHECK ((received_contract_count >= 0)),
    CONSTRAINT option_analysis_runs_status_check CHECK (((status)::text = ANY ((ARRAY['PENDING'::character varying, 'RUNNING'::character varying, 'COMPLETE'::character varying, 'INCOMPLETE_MATRIX'::character varying, 'REFERENCE_DRIFT_FAILED'::character varying, 'MODEL_QUALITY_FAILED'::character varying, 'FAILED'::character varying])::text[]))),
    CONSTRAINT option_analysis_runs_unknown_reference_count_check CHECK ((unknown_reference_count >= 0))
);


--
-- Name: option_candidate_legs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_candidate_legs (
    candidate_id uuid NOT NULL,
    leg_index integer NOT NULL,
    snapshot_id uuid NOT NULL,
    contract_id bigint NOT NULL,
    contract_ticker character varying(64) NOT NULL,
    side character varying(4) NOT NULL,
    ratio integer NOT NULL,
    multiplier integer NOT NULL,
    expiration_date date NOT NULL,
    strike numeric(20,8) NOT NULL,
    contract_type character varying(4) NOT NULL,
    spot numeric(20,8) NOT NULL,
    time_to_expiration_years double precision NOT NULL,
    risk_free_rate double precision NOT NULL,
    dividend_yield double precision NOT NULL,
    model_mark numeric(20,8),
    local_iv double precision,
    local_delta double precision,
    local_gamma double precision,
    local_theta_per_day double precision,
    local_vega_per_vol_point double precision,
    local_rho_per_rate_point double precision,
    source_market_time timestamp with time zone NOT NULL,
    mark_source character varying(40) NOT NULL,
    model_version character varying(64) NOT NULL,
    quality_flags text[] DEFAULT ARRAY[]::text[] NOT NULL,
    quote_bid numeric(20,8),
    quote_ask numeric(20,8),
    quote_bid_size integer,
    quote_ask_size integer,
    quote_midpoint numeric(20,8),
    quote_spread_midpoint double precision,
    quote_sequence bigint,
    quote_time timestamp with time zone,
    underlying_quote_time timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_candidate_legs_check CHECK (((quote_bid IS NULL) OR (quote_ask IS NULL) OR (quote_bid <= quote_ask))),
    CONSTRAINT option_candidate_legs_contract_type_check CHECK (((contract_type)::text = ANY ((ARRAY['CALL'::character varying, 'PUT'::character varying])::text[]))),
    CONSTRAINT option_candidate_legs_leg_index_check CHECK ((leg_index >= 0)),
    CONSTRAINT option_candidate_legs_model_mark_check CHECK (((model_mark IS NULL) OR (model_mark > (0)::numeric))),
    CONSTRAINT option_candidate_legs_multiplier_check CHECK ((multiplier > 0)),
    CONSTRAINT option_candidate_legs_quote_ask_check CHECK (((quote_ask IS NULL) OR (quote_ask >= (0)::numeric))),
    CONSTRAINT option_candidate_legs_quote_bid_check CHECK (((quote_bid IS NULL) OR (quote_bid >= (0)::numeric))),
    CONSTRAINT option_candidate_legs_quote_midpoint_check CHECK (((quote_midpoint IS NULL) OR (quote_midpoint >= (0)::numeric))),
    CONSTRAINT option_candidate_legs_quote_spread_midpoint_check CHECK (((quote_spread_midpoint IS NULL) OR (quote_spread_midpoint >= (0)::double precision))),
    CONSTRAINT option_candidate_legs_ratio_check CHECK ((ratio > 0)),
    CONSTRAINT option_candidate_legs_side_check CHECK (((side)::text = ANY ((ARRAY['BUY'::character varying, 'SELL'::character varying])::text[]))),
    CONSTRAINT option_candidate_legs_spot_check CHECK ((spot > (0)::numeric)),
    CONSTRAINT option_candidate_legs_strike_check CHECK ((strike > (0)::numeric)),
    CONSTRAINT option_candidate_legs_time_to_expiration_years_check CHECK ((time_to_expiration_years > (0)::double precision))
);


--
-- Name: option_chain_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_chain_snapshots (
    snapshot_id uuid NOT NULL,
    contract_id bigint NOT NULL,
    contract_ticker character varying(64) NOT NULL,
    underlying character varying(16) NOT NULL,
    asset_type character varying(8) NOT NULL,
    provider character varying(32) NOT NULL,
    batch_id uuid NOT NULL,
    contract_type character varying(4) NOT NULL,
    expiration_date date NOT NULL,
    expiration_cutoff timestamp with time zone NOT NULL,
    calendar_dte integer NOT NULL,
    time_to_expiration_years double precision NOT NULL,
    strike numeric(20,8) NOT NULL,
    shares_per_contract integer NOT NULL,
    exercise_style character varying(16) NOT NULL,
    spot numeric(20,8) NOT NULL,
    spot_market_data_time timestamp with time zone NOT NULL,
    bid numeric(20,8),
    ask numeric(20,8),
    midpoint numeric(20,8),
    display_mark numeric(20,8),
    model_mark numeric(20,8),
    mark_market_data_time timestamp with time zone NOT NULL,
    mark_source character varying(40) NOT NULL,
    day_volume bigint,
    open_interest bigint,
    market_data_time timestamp with time zone NOT NULL,
    first_observed_at timestamp with time zone NOT NULL,
    revised_observed_at timestamp with time zone,
    data_delay_seconds double precision NOT NULL,
    local_iv double precision,
    local_gamma double precision,
    local_delta double precision,
    local_theta_per_day double precision,
    local_vega_per_vol_point double precision,
    local_rho_per_rate_point double precision,
    intrinsic_value numeric(20,8),
    extrinsic_value numeric(20,8),
    single_contract_breakeven numeric(20,8),
    provider_iv double precision,
    provider_gamma double precision,
    risk_free_rate double precision NOT NULL,
    dividend_yield double precision NOT NULL,
    iv_converged boolean NOT NULL,
    iv_solver character varying(16),
    iv_iteration_count integer NOT NULL,
    iv_price_error double precision,
    iv_failure_reason character varying(64),
    model_version character varying(64) NOT NULL,
    quality_flags text[] DEFAULT ARRAY[]::text[] NOT NULL,
    raw_payload_sha256 character(64) NOT NULL,
    normalized_payload_sha256 character(64) NOT NULL,
    revision integer DEFAULT 1 NOT NULL,
    policy_version character varying(64) NOT NULL,
    policy_sha256 character(64) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_chain_snapshots_ask_check CHECK (((ask IS NULL) OR (ask >= (0)::numeric))),
    CONSTRAINT option_chain_snapshots_asset_type_check CHECK (((asset_type)::text = ANY ((ARRAY['STOCK'::character varying, 'ETF'::character varying])::text[]))),
    CONSTRAINT option_chain_snapshots_bid_check CHECK (((bid IS NULL) OR (bid >= (0)::numeric))),
    CONSTRAINT option_chain_snapshots_calendar_dte_check CHECK ((calendar_dte >= 0)),
    CONSTRAINT option_chain_snapshots_check CHECK ((first_observed_at >= market_data_time)),
    CONSTRAINT option_chain_snapshots_check1 CHECK ((first_observed_at >= spot_market_data_time)),
    CONSTRAINT option_chain_snapshots_check2 CHECK ((first_observed_at >= mark_market_data_time)),
    CONSTRAINT option_chain_snapshots_check3 CHECK (((revised_observed_at IS NULL) OR (revised_observed_at >= first_observed_at))),
    CONSTRAINT option_chain_snapshots_check4 CHECK (((iv_converged AND (local_iv IS NOT NULL) AND (iv_solver IS NOT NULL) AND (iv_failure_reason IS NULL)) OR ((NOT iv_converged) AND (local_iv IS NULL) AND (iv_failure_reason IS NOT NULL)))),
    CONSTRAINT option_chain_snapshots_contract_type_check CHECK (((contract_type)::text = ANY ((ARRAY['CALL'::character varying, 'PUT'::character varying])::text[]))),
    CONSTRAINT option_chain_snapshots_data_delay_seconds_check CHECK ((data_delay_seconds >= (0)::double precision)),
    CONSTRAINT option_chain_snapshots_day_volume_check CHECK (((day_volume IS NULL) OR (day_volume >= 0))),
    CONSTRAINT option_chain_snapshots_display_mark_check CHECK (((display_mark IS NULL) OR (display_mark >= (0)::numeric))),
    CONSTRAINT option_chain_snapshots_intrinsic_value_check CHECK (((intrinsic_value IS NULL) OR (intrinsic_value >= (0)::numeric))),
    CONSTRAINT option_chain_snapshots_iv_iteration_count_check CHECK ((iv_iteration_count >= 0)),
    CONSTRAINT option_chain_snapshots_iv_price_error_check CHECK (((iv_price_error IS NULL) OR (iv_price_error >= (0)::double precision))),
    CONSTRAINT option_chain_snapshots_mark_source_check CHECK (((mark_source)::text = ANY ((ARRAY['DEVELOPER_ALIGNED_AGG_CLOSE'::character varying, 'ADVANCED_NBBO_MIDPOINT'::character varying, 'BROKER_NBBO_MIDPOINT'::character varying, 'DISPLAY_DAY_CLOSE'::character varying, 'DISPLAY_DAY_VWAP'::character varying])::text[]))),
    CONSTRAINT option_chain_snapshots_midpoint_check CHECK (((midpoint IS NULL) OR (midpoint >= (0)::numeric))),
    CONSTRAINT option_chain_snapshots_model_mark_check CHECK (((model_mark IS NULL) OR (model_mark > (0)::numeric))),
    CONSTRAINT option_chain_snapshots_normalized_payload_sha256_check CHECK ((normalized_payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_chain_snapshots_open_interest_check CHECK (((open_interest IS NULL) OR (open_interest >= 0))),
    CONSTRAINT option_chain_snapshots_policy_sha256_check CHECK ((policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_chain_snapshots_raw_payload_sha256_check CHECK ((raw_payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_chain_snapshots_revision_check CHECK ((revision > 0)),
    CONSTRAINT option_chain_snapshots_shares_per_contract_check CHECK ((shares_per_contract > 0)),
    CONSTRAINT option_chain_snapshots_spot_check CHECK ((spot > (0)::numeric)),
    CONSTRAINT option_chain_snapshots_strike_check CHECK ((strike > (0)::numeric)),
    CONSTRAINT option_chain_snapshots_time_to_expiration_years_check CHECK ((time_to_expiration_years > (0)::double precision))
)
PARTITION BY RANGE (first_observed_at);


--
-- Name: option_context_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_context_snapshots (
    context_snapshot_id uuid NOT NULL,
    matrix_id uuid NOT NULL,
    underlying character varying(16) NOT NULL,
    market_data_time timestamp with time zone NOT NULL,
    observed_time timestamp with time zone NOT NULL,
    status character varying(24) NOT NULL,
    daily_close numeric(20,8),
    daily_ema_50 numeric(20,8),
    daily_ema_50_input_bars integer,
    daily_window_start timestamp with time zone,
    hourly_close numeric(20,8),
    hourly_ema_20 numeric(20,8),
    hourly_ema_20_input_bars integer,
    hourly_window_start timestamp with time zone,
    five_minute_close numeric(20,8),
    five_minute_ema numeric(20,8),
    five_minute_volume bigint,
    five_minute_volume_mean double precision,
    five_minute_input_bars integer,
    five_minute_window_start timestamp with time zone,
    trend_state character varying(24),
    earnings_blackout_state character varying(32) NOT NULL,
    fed_blackout_state character varying(32) NOT NULL,
    quote_spread_state character varying(32) DEFAULT 'NOT_AVAILABLE'::character varying NOT NULL,
    reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    source_bar_keys jsonb DEFAULT '[]'::jsonb NOT NULL,
    policy_version character varying(64) NOT NULL,
    policy_sha256 character(64) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    equity_context_snapshot_id uuid,
    CONSTRAINT option_context_snapshots_check CHECK ((observed_time >= market_data_time)),
    CONSTRAINT option_context_snapshots_daily_ema_50_input_bars_check CHECK (((daily_ema_50_input_bars IS NULL) OR (daily_ema_50_input_bars >= 0))),
    CONSTRAINT option_context_snapshots_five_minute_input_bars_check CHECK (((five_minute_input_bars IS NULL) OR (five_minute_input_bars >= 0))),
    CONSTRAINT option_context_snapshots_hourly_ema_20_input_bars_check CHECK (((hourly_ema_20_input_bars IS NULL) OR (hourly_ema_20_input_bars >= 0))),
    CONSTRAINT option_context_snapshots_policy_sha256_check CHECK ((policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_context_snapshots_source_bar_keys_check CHECK ((jsonb_typeof(source_bar_keys) = 'array'::text)),
    CONSTRAINT option_context_snapshots_status_check CHECK (((status)::text = ANY ((ARRAY['COMPLETE'::character varying, 'DEGRADED'::character varying, 'FAILED'::character varying])::text[]))),
    CONSTRAINT option_context_snapshots_trend_state_check CHECK (((trend_state IS NULL) OR ((trend_state)::text = ANY ((ARRAY['BULLISH'::character varying, 'BEARISH'::character varying, 'NEUTRAL'::character varying])::text[]))))
);


--
-- Name: option_contract_catalog; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_contract_catalog (
    contract_id bigint NOT NULL,
    contract_ticker character varying(64) NOT NULL,
    underlying character varying(16) NOT NULL,
    asset_type character varying(8) NOT NULL,
    first_observed_at timestamp with time zone NOT NULL,
    catalog_admitted_at timestamp with time zone,
    expired_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_contract_catalog_asset_type_check CHECK (((asset_type)::text = ANY ((ARRAY['STOCK'::character varying, 'ETF'::character varying])::text[]))),
    CONSTRAINT option_contract_catalog_check CHECK (((catalog_admitted_at IS NULL) OR (catalog_admitted_at >= first_observed_at))),
    CONSTRAINT option_contract_catalog_check1 CHECK (((expired_at IS NULL) OR (expired_at >= catalog_admitted_at)))
);


--
-- Name: option_contract_catalog_contract_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.option_contract_catalog_contract_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: option_contract_catalog_contract_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.option_contract_catalog_contract_id_seq OWNED BY public.option_contract_catalog.contract_id;


--
-- Name: option_contract_catalog_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_contract_catalog_versions (
    catalog_version_id bigint NOT NULL,
    contract_id bigint NOT NULL,
    provider character varying(32) NOT NULL,
    provider_version character varying(64),
    provider_contract_type character varying(32) NOT NULL,
    contract_type character varying(4),
    expiration_date date NOT NULL,
    strike numeric(20,8) NOT NULL,
    provider_exercise_style character varying(32) NOT NULL,
    exercise_style character varying(16),
    shares_per_contract integer NOT NULL,
    primary_exchange character varying(32),
    correction character varying(32),
    additional_underlyings jsonb DEFAULT '[]'::jsonb NOT NULL,
    adjustment_metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    eligibility_status character varying(32) NOT NULL,
    exclusion_reasons text[] DEFAULT ARRAY[]::text[] NOT NULL,
    valid_from timestamp with time zone NOT NULL,
    valid_to timestamp with time zone,
    first_observed_at timestamp with time zone NOT NULL,
    revised_observed_at timestamp with time zone,
    refreshed_at timestamp with time zone NOT NULL,
    payload_sha256 character(64) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_contract_catalog_versions_additional_underlyings_check CHECK ((jsonb_typeof(additional_underlyings) = 'array'::text)),
    CONSTRAINT option_contract_catalog_versions_adjustment_metadata_check CHECK ((jsonb_typeof(adjustment_metadata) = 'object'::text)),
    CONSTRAINT option_contract_catalog_versions_check CHECK (((valid_to IS NULL) OR (valid_to > valid_from))),
    CONSTRAINT option_contract_catalog_versions_check1 CHECK ((first_observed_at >= valid_from)),
    CONSTRAINT option_contract_catalog_versions_check2 CHECK (((revised_observed_at IS NULL) OR (revised_observed_at >= first_observed_at))),
    CONSTRAINT option_contract_catalog_versions_check3 CHECK ((((eligibility_status)::text <> 'VALIDATED_ACTIVE'::text) OR ((contract_type IS NOT NULL) AND ((exercise_style)::text = 'AMERICAN'::text) AND (shares_per_contract = 100)))),
    CONSTRAINT option_contract_catalog_versions_contract_type_check CHECK (((contract_type)::text = ANY ((ARRAY['CALL'::character varying, 'PUT'::character varying])::text[]))),
    CONSTRAINT option_contract_catalog_versions_eligibility_status_check CHECK (((eligibility_status)::text = ANY ((ARRAY['VALIDATED_ACTIVE'::character varying, 'REJECTED_UNSUPPORTED'::character varying, 'EXPIRED'::character varying])::text[]))),
    CONSTRAINT option_contract_catalog_versions_exercise_style_check CHECK (((exercise_style)::text = ANY ((ARRAY['AMERICAN'::character varying, 'EUROPEAN'::character varying])::text[]))),
    CONSTRAINT option_contract_catalog_versions_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_contract_catalog_versions_shares_per_contract_check CHECK ((shares_per_contract > 0)),
    CONSTRAINT option_contract_catalog_versions_strike_check CHECK ((strike > (0)::numeric))
);


--
-- Name: option_contract_catalog_versions_catalog_version_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.option_contract_catalog_versions_catalog_version_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: option_contract_catalog_versions_catalog_version_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.option_contract_catalog_versions_catalog_version_id_seq OWNED BY public.option_contract_catalog_versions.catalog_version_id;


--
-- Name: option_contract_discoveries; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_contract_discoveries (
    discovery_id bigint NOT NULL,
    contract_ticker character varying(64) NOT NULL,
    underlying character varying(16) NOT NULL,
    state character varying(32) NOT NULL,
    source_batch_id uuid NOT NULL,
    source_page_number integer NOT NULL,
    first_observed_at timestamp with time zone NOT NULL,
    last_attempted_at timestamp with time zone,
    resolved_at timestamp with time zone,
    activate_after timestamp with time zone,
    contract_id bigint,
    raw_details jsonb DEFAULT '{}'::jsonb NOT NULL,
    reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_contract_discoveries_check CHECK (((resolved_at IS NULL) OR (resolved_at >= first_observed_at))),
    CONSTRAINT option_contract_discoveries_check1 CHECK (((activate_after IS NULL) OR (activate_after >= first_observed_at))),
    CONSTRAINT option_contract_discoveries_check2 CHECK ((((state)::text <> ALL ((ARRAY['VALIDATED_ACTIVE'::character varying, 'WATCHLIST_ACTIVE'::character varying])::text[])) OR ((contract_id IS NOT NULL) AND (resolved_at IS NOT NULL) AND (activate_after IS NOT NULL)))),
    CONSTRAINT option_contract_discoveries_check3 CHECK ((((state)::text <> ALL ((ARRAY['REJECTED_UNSUPPORTED'::character varying, 'REFERENCE_UNAVAILABLE'::character varying])::text[])) OR (resolved_at IS NOT NULL))),
    CONSTRAINT option_contract_discoveries_raw_details_check CHECK ((jsonb_typeof(raw_details) = 'object'::text)),
    CONSTRAINT option_contract_discoveries_source_page_number_check CHECK ((source_page_number > 0)),
    CONSTRAINT option_contract_discoveries_state_check CHECK (((state)::text = ANY ((ARRAY['UNKNOWN_REFERENCE'::character varying, 'REFERENCE_PENDING'::character varying, 'VALIDATED_ACTIVE'::character varying, 'REJECTED_UNSUPPORTED'::character varying, 'REFERENCE_UNAVAILABLE'::character varying, 'WATCHLIST_ACTIVE'::character varying])::text[])))
);


--
-- Name: option_contract_discoveries_discovery_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.option_contract_discoveries_discovery_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: option_contract_discoveries_discovery_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.option_contract_discoveries_discovery_id_seq OWNED BY public.option_contract_discoveries.discovery_id;


--
-- Name: option_decision_evidence; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_decision_evidence (
    evidence_id uuid NOT NULL,
    matrix_id uuid NOT NULL,
    decision_type character varying(24) NOT NULL,
    strategy_name character varying(64) NOT NULL,
    strategy_version character varying(32) NOT NULL,
    normalized_legs jsonb DEFAULT '[]'::jsonb NOT NULL,
    underlying_mark numeric(20,8),
    market_data_time timestamp with time zone NOT NULL,
    source_observation_time timestamp with time zone NOT NULL,
    context_snapshot_id uuid,
    context jsonb DEFAULT '{}'::jsonb NOT NULL,
    rank_components jsonb DEFAULT '{}'::jsonb NOT NULL,
    trigger_values jsonb DEFAULT '{}'::jsonb NOT NULL,
    quality_flags text[] DEFAULT ARRAY[]::text[] NOT NULL,
    policy_sha256 character(64) NOT NULL,
    raw_file_ids uuid[] DEFAULT ARRAY[]::uuid[] NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_decision_evidence_check CHECK ((source_observation_time >= market_data_time)),
    CONSTRAINT option_decision_evidence_context_check CHECK ((jsonb_typeof(context) = 'object'::text)),
    CONSTRAINT option_decision_evidence_decision_type_check CHECK (((decision_type)::text = ANY ((ARRAY['CANDIDATE'::character varying, 'SIGNAL'::character varying, 'SUPPRESSION'::character varying])::text[]))),
    CONSTRAINT option_decision_evidence_normalized_legs_check CHECK ((jsonb_typeof(normalized_legs) = 'array'::text)),
    CONSTRAINT option_decision_evidence_policy_sha256_check CHECK ((policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_decision_evidence_rank_components_check CHECK ((jsonb_typeof(rank_components) = 'object'::text)),
    CONSTRAINT option_decision_evidence_trigger_values_check CHECK ((jsonb_typeof(trigger_values) = 'object'::text))
);


--
-- Name: option_expiration_analytics; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_expiration_analytics (
    matrix_id uuid NOT NULL,
    expiration_date date NOT NULL,
    fractional_maturity_years double precision NOT NULL,
    forward_price numeric(20,8),
    atm_iv double precision,
    call_25_delta_iv double precision,
    put_25_delta_iv double precision,
    call_skew_25_delta double precision,
    put_skew_25_delta double precision,
    risk_reversal_25_delta double precision,
    interpolation_diagnostics jsonb DEFAULT '{}'::jsonb NOT NULL,
    put_volume bigint,
    call_volume bigint,
    put_open_interest bigint,
    call_open_interest bigint,
    breadth integer,
    concentration_metrics jsonb DEFAULT '{}'::jsonb NOT NULL,
    wall_clusters jsonb DEFAULT '[]'::jsonb NOT NULL,
    term_change double precision,
    term_slope double precision,
    quality_reasons text[] DEFAULT ARRAY[]::text[] NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_expiration_analytics_breadth_check CHECK (((breadth IS NULL) OR (breadth >= 0))),
    CONSTRAINT option_expiration_analytics_call_open_interest_check CHECK (((call_open_interest IS NULL) OR (call_open_interest >= 0))),
    CONSTRAINT option_expiration_analytics_call_volume_check CHECK (((call_volume IS NULL) OR (call_volume >= 0))),
    CONSTRAINT option_expiration_analytics_concentration_metrics_check CHECK ((jsonb_typeof(concentration_metrics) = 'object'::text)),
    CONSTRAINT option_expiration_analytics_forward_price_check CHECK (((forward_price IS NULL) OR (forward_price > (0)::numeric))),
    CONSTRAINT option_expiration_analytics_fractional_maturity_years_check CHECK ((fractional_maturity_years >= (0)::double precision)),
    CONSTRAINT option_expiration_analytics_interpolation_diagnostics_check CHECK ((jsonb_typeof(interpolation_diagnostics) = 'object'::text)),
    CONSTRAINT option_expiration_analytics_put_open_interest_check CHECK (((put_open_interest IS NULL) OR (put_open_interest >= 0))),
    CONSTRAINT option_expiration_analytics_put_volume_check CHECK (((put_volume IS NULL) OR (put_volume >= 0))),
    CONSTRAINT option_expiration_analytics_wall_clusters_check CHECK ((jsonb_typeof(wall_clusters) = 'array'::text))
);


--
-- Name: option_flow_windows; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_flow_windows (
    flow_window_id uuid NOT NULL,
    matrix_id uuid NOT NULL,
    underlying character varying(16) NOT NULL,
    window_start timestamp with time zone NOT NULL,
    window_end timestamp with time zone NOT NULL,
    watermark_time timestamp with time zone NOT NULL,
    late_event_count integer DEFAULT 0 NOT NULL,
    corrected_event_count integer DEFAULT 0 NOT NULL,
    distinct_contract_count integer DEFAULT 0 NOT NULL,
    distinct_exchange_count integer DEFAULT 0 NOT NULL,
    qualifying_print_count integer DEFAULT 0 NOT NULL,
    total_notional numeric(24,8) DEFAULT 0 NOT NULL,
    call_notional numeric(24,8) DEFAULT 0 NOT NULL,
    put_notional numeric(24,8) DEFAULT 0 NOT NULL,
    otm_call_print_count integer DEFAULT 0 NOT NULL,
    detector_version character varying(64) NOT NULL,
    contributing_event_keys jsonb DEFAULT '[]'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_flow_windows_call_notional_check CHECK ((call_notional >= (0)::numeric)),
    CONSTRAINT option_flow_windows_check CHECK ((window_end >= window_start)),
    CONSTRAINT option_flow_windows_check1 CHECK ((watermark_time >= window_end)),
    CONSTRAINT option_flow_windows_contributing_event_keys_check CHECK ((jsonb_typeof(contributing_event_keys) = 'array'::text)),
    CONSTRAINT option_flow_windows_corrected_event_count_check CHECK ((corrected_event_count >= 0)),
    CONSTRAINT option_flow_windows_distinct_contract_count_check CHECK ((distinct_contract_count >= 0)),
    CONSTRAINT option_flow_windows_distinct_exchange_count_check CHECK ((distinct_exchange_count >= 0)),
    CONSTRAINT option_flow_windows_late_event_count_check CHECK ((late_event_count >= 0)),
    CONSTRAINT option_flow_windows_otm_call_print_count_check CHECK ((otm_call_print_count >= 0)),
    CONSTRAINT option_flow_windows_put_notional_check CHECK ((put_notional >= (0)::numeric)),
    CONSTRAINT option_flow_windows_qualifying_print_count_check CHECK ((qualifying_print_count >= 0)),
    CONSTRAINT option_flow_windows_total_notional_check CHECK ((total_notional >= (0)::numeric))
);


--
-- Name: option_ingestion_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_ingestion_runs (
    batch_id uuid NOT NULL,
    provider character varying(32) NOT NULL,
    underlying character varying(16) NOT NULL,
    asset_type character varying(8) NOT NULL,
    scheduled_cycle timestamp with time zone NOT NULL,
    request_filter_sha256 character(64) NOT NULL,
    policy_version character varying(64) NOT NULL,
    policy_sha256 character(64) NOT NULL,
    configuration_sha256 character(64) NOT NULL,
    status character varying(24) NOT NULL,
    page_count integer DEFAULT 0 NOT NULL,
    terminal_page_received boolean DEFAULT false NOT NULL,
    received_row_count integer DEFAULT 0 NOT NULL,
    catalog_row_count integer DEFAULT 0 NOT NULL,
    retained_row_count integer DEFAULT 0 NOT NULL,
    rejected_counts jsonb DEFAULT '{}'::jsonb NOT NULL,
    unknown_reference_count integer DEFAULT 0 NOT NULL,
    request_ids text[] DEFAULT ARRAY[]::text[] NOT NULL,
    latency_ms bigint,
    retry_count integer DEFAULT 0 NOT NULL,
    error_category character varying(64),
    failure_reason text,
    market_data_time timestamp with time zone,
    first_observed_at timestamp with time zone,
    started_at timestamp with time zone NOT NULL,
    completed_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_ingestion_runs_asset_type_check CHECK (((asset_type)::text = ANY ((ARRAY['STOCK'::character varying, 'ETF'::character varying])::text[]))),
    CONSTRAINT option_ingestion_runs_catalog_row_count_check CHECK ((catalog_row_count >= 0)),
    CONSTRAINT option_ingestion_runs_check CHECK (((completed_at IS NULL) OR (completed_at >= started_at))),
    CONSTRAINT option_ingestion_runs_check1 CHECK ((((status)::text <> 'COMPLETE'::text) OR (terminal_page_received AND (completed_at IS NOT NULL) AND (failure_reason IS NULL)))),
    CONSTRAINT option_ingestion_runs_check2 CHECK ((((status)::text <> ALL ((ARRAY['FAILED'::character varying, 'QUARANTINED'::character varying])::text[])) OR ((completed_at IS NOT NULL) AND (failure_reason IS NOT NULL)))),
    CONSTRAINT option_ingestion_runs_configuration_sha256_check CHECK ((configuration_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_ingestion_runs_latency_ms_check CHECK (((latency_ms IS NULL) OR (latency_ms >= 0))),
    CONSTRAINT option_ingestion_runs_page_count_check CHECK ((page_count >= 0)),
    CONSTRAINT option_ingestion_runs_policy_sha256_check CHECK ((policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_ingestion_runs_received_row_count_check CHECK ((received_row_count >= 0)),
    CONSTRAINT option_ingestion_runs_rejected_counts_check CHECK ((jsonb_typeof(rejected_counts) = 'object'::text)),
    CONSTRAINT option_ingestion_runs_request_filter_sha256_check CHECK ((request_filter_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_ingestion_runs_retained_row_count_check CHECK ((retained_row_count >= 0)),
    CONSTRAINT option_ingestion_runs_retry_count_check CHECK ((retry_count >= 0)),
    CONSTRAINT option_ingestion_runs_status_check CHECK (((status)::text = ANY ((ARRAY['FETCHING'::character varying, 'COMPLETE'::character varying, 'FAILED'::character varying, 'QUARANTINED'::character varying])::text[]))),
    CONSTRAINT option_ingestion_runs_unknown_reference_count_check CHECK ((unknown_reference_count >= 0))
);


--
-- Name: option_iv_context_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_iv_context_snapshots (
    iv_context_id uuid NOT NULL,
    matrix_id uuid NOT NULL,
    underlying character varying(16) NOT NULL,
    expiration_bucket character varying(16) NOT NULL,
    current_comparable_iv double precision,
    lookback_start_date date,
    lookback_end_date date,
    sample_count integer,
    coverage_fraction double precision,
    minimum_iv double precision,
    maximum_iv double precision,
    range_position_rank double precision,
    empirical_percentile double precision,
    calculation_version character varying(64) NOT NULL,
    first_observed_time timestamp with time zone NOT NULL,
    null_reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_iv_context_snapshots_check CHECK (((lookback_end_date IS NULL) OR (lookback_start_date IS NULL) OR (lookback_end_date >= lookback_start_date))),
    CONSTRAINT option_iv_context_snapshots_check1 CHECK (((maximum_iv IS NULL) OR (minimum_iv IS NULL) OR (maximum_iv >= minimum_iv))),
    CONSTRAINT option_iv_context_snapshots_coverage_fraction_check CHECK (((coverage_fraction IS NULL) OR ((coverage_fraction >= (0)::double precision) AND (coverage_fraction <= (1)::double precision)))),
    CONSTRAINT option_iv_context_snapshots_empirical_percentile_check CHECK (((empirical_percentile IS NULL) OR ((empirical_percentile >= (0)::double precision) AND (empirical_percentile <= (1)::double precision)))),
    CONSTRAINT option_iv_context_snapshots_range_position_rank_check CHECK (((range_position_rank IS NULL) OR ((range_position_rank >= (0)::double precision) AND (range_position_rank <= (1)::double precision)))),
    CONSTRAINT option_iv_context_snapshots_sample_count_check CHECK (((sample_count IS NULL) OR (sample_count >= 0)))
);


--
-- Name: option_market_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_market_events (
    market_event_id uuid NOT NULL,
    event_type character varying(32) NOT NULL,
    affected_underlying character varying(16),
    scheduled_time timestamp with time zone NOT NULL,
    source character varying(64) NOT NULL,
    source_key character varying(256) NOT NULL,
    announcement_time timestamp with time zone,
    first_observed_at timestamp with time zone NOT NULL,
    revised_observed_at timestamp with time zone,
    confidence character varying(24) NOT NULL,
    status character varying(24) NOT NULL,
    payload_sha256 character(64) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_market_events_check CHECK (((announcement_time IS NULL) OR (first_observed_at >= announcement_time))),
    CONSTRAINT option_market_events_check1 CHECK (((revised_observed_at IS NULL) OR (revised_observed_at >= first_observed_at))),
    CONSTRAINT option_market_events_check2 CHECK (((((event_type)::text = 'FED_RATE_DECISION'::text) AND (affected_underlying IS NULL)) OR (((event_type)::text = 'EARNINGS'::text) AND (affected_underlying IS NOT NULL)))),
    CONSTRAINT option_market_events_confidence_check CHECK (((confidence)::text = ANY ((ARRAY['CONFIRMED'::character varying, 'ESTIMATED'::character varying, 'UNKNOWN'::character varying])::text[]))),
    CONSTRAINT option_market_events_event_type_check CHECK (((event_type)::text = ANY ((ARRAY['EARNINGS'::character varying, 'FED_RATE_DECISION'::character varying])::text[]))),
    CONSTRAINT option_market_events_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_market_events_status_check CHECK (((status)::text = ANY ((ARRAY['SCHEDULED'::character varying, 'COMPLETED'::character varying, 'CANCELED'::character varying, 'REVISED'::character varying])::text[])))
);


--
-- Name: option_provider_trade_semantics; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_provider_trade_semantics (
    semantics_id bigint NOT NULL,
    provider character varying(32) NOT NULL,
    semantics_version character varying(64) NOT NULL,
    condition_code integer,
    correction_code integer,
    behavior character varying(24) NOT NULL,
    contributes_volume boolean NOT NULL,
    contributes_notional boolean NOT NULL,
    effective_from timestamp with time zone NOT NULL,
    effective_to timestamp with time zone,
    first_observed_at timestamp with time zone NOT NULL,
    configuration_sha256 character(64) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_provider_trade_semantics_behavior_check CHECK (((behavior)::text = ANY ((ARRAY['INCLUDE'::character varying, 'EXCLUDE'::character varying, 'SUPERSEDE'::character varying, 'CANCEL'::character varying, 'UNKNOWN'::character varying])::text[]))),
    CONSTRAINT option_provider_trade_semantics_check CHECK (((condition_code IS NOT NULL) OR (correction_code IS NOT NULL))),
    CONSTRAINT option_provider_trade_semantics_check1 CHECK (((effective_to IS NULL) OR (effective_to > effective_from))),
    CONSTRAINT option_provider_trade_semantics_check2 CHECK ((first_observed_at >= effective_from)),
    CONSTRAINT option_provider_trade_semantics_configuration_sha256_check CHECK ((configuration_sha256 ~ '^[0-9a-f]{64}$'::text))
);


--
-- Name: option_provider_trade_semantics_semantics_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.option_provider_trade_semantics_semantics_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: option_provider_trade_semantics_semantics_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.option_provider_trade_semantics_semantics_id_seq OWNED BY public.option_provider_trade_semantics.semantics_id;


--
-- Name: option_raw_batch_pages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_raw_batch_pages (
    batch_id uuid NOT NULL,
    page_number integer NOT NULL,
    request_id character varying(128),
    redacted_request jsonb DEFAULT '{}'::jsonb NOT NULL,
    response_gzip bytea NOT NULL,
    payload_sha256 character(64) NOT NULL,
    row_count integer NOT NULL,
    byte_count bigint NOT NULL,
    received_at timestamp with time zone NOT NULL,
    terminal_page boolean NOT NULL,
    request_filter_sha256 character(64) NOT NULL,
    request_cursor_sha256 character(64),
    next_cursor_sha256 character(64),
    validation_status character varying(16) NOT NULL,
    validation_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_raw_batch_pages_byte_count_check CHECK ((byte_count >= 0)),
    CONSTRAINT option_raw_batch_pages_check CHECK (((page_number <> 1) OR (request_cursor_sha256 IS NULL))),
    CONSTRAINT option_raw_batch_pages_check1 CHECK ((((validation_status)::text = 'INVALID'::text) OR (terminal_page AND (next_cursor_sha256 IS NULL)) OR ((NOT terminal_page) AND (next_cursor_sha256 IS NOT NULL)))),
    CONSTRAINT option_raw_batch_pages_check2 CHECK (((((validation_status)::text = 'VALID'::text) AND (validation_error IS NULL)) OR (((validation_status)::text = 'INVALID'::text) AND (validation_error IS NOT NULL)))),
    CONSTRAINT option_raw_batch_pages_next_cursor_sha256_check CHECK (((next_cursor_sha256 IS NULL) OR (next_cursor_sha256 ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT option_raw_batch_pages_page_number_check CHECK ((page_number > 0)),
    CONSTRAINT option_raw_batch_pages_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_raw_batch_pages_redacted_request_check CHECK ((jsonb_typeof(redacted_request) = 'object'::text)),
    CONSTRAINT option_raw_batch_pages_request_cursor_sha256_check CHECK (((request_cursor_sha256 IS NULL) OR (request_cursor_sha256 ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT option_raw_batch_pages_request_filter_sha256_check CHECK ((request_filter_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_raw_batch_pages_row_count_check CHECK ((row_count >= 0)),
    CONSTRAINT option_raw_batch_pages_validation_status_check CHECK (((validation_status)::text = ANY ((ARRAY['VALID'::character varying, 'INVALID'::character varying])::text[])))
);


--
-- Name: option_raw_file_manifests; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_raw_file_manifests (
    file_id uuid NOT NULL,
    event_type character varying(24) NOT NULL,
    market_date date NOT NULL,
    underlying character varying(16) NOT NULL,
    market_hour smallint NOT NULL,
    object_key text NOT NULL,
    schema_version integer NOT NULL,
    row_count bigint NOT NULL,
    minimum_source_time timestamp with time zone,
    maximum_source_time timestamp with time zone,
    byte_size bigint NOT NULL,
    payload_sha256 character(64) NOT NULL,
    creation_status character varying(24) NOT NULL,
    retention_class character varying(64) NOT NULL,
    deleted_at timestamp with time zone,
    deletion_reason text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_raw_file_manifests_byte_size_check CHECK ((byte_size >= 0)),
    CONSTRAINT option_raw_file_manifests_check CHECK (((minimum_source_time IS NULL) OR (maximum_source_time IS NULL) OR (maximum_source_time >= minimum_source_time))),
    CONSTRAINT option_raw_file_manifests_check1 CHECK ((((creation_status)::text <> 'DELETED'::text) OR ((deleted_at IS NOT NULL) AND (deletion_reason IS NOT NULL)))),
    CONSTRAINT option_raw_file_manifests_creation_status_check CHECK (((creation_status)::text = ANY ((ARRAY['WRITING'::character varying, 'COMPLETE'::character varying, 'MISSING'::character varying, 'CORRUPT'::character varying, 'DELETED'::character varying])::text[]))),
    CONSTRAINT option_raw_file_manifests_event_type_check CHECK (((event_type)::text = ANY ((ARRAY['SNAPSHOT'::character varying, 'TRADE'::character varying, 'QUOTE'::character varying])::text[]))),
    CONSTRAINT option_raw_file_manifests_market_hour_check CHECK (((market_hour >= 0) AND (market_hour <= 23))),
    CONSTRAINT option_raw_file_manifests_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_raw_file_manifests_row_count_check CHECK ((row_count >= 0)),
    CONSTRAINT option_raw_file_manifests_schema_version_check CHECK ((schema_version > 0))
);


--
-- Name: option_recommendation_validity_current; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_recommendation_validity_current (
    signal_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    validity_event_id uuid NOT NULL,
    version bigint NOT NULL,
    state character varying(24) NOT NULL,
    token_hash character(64),
    valid_through timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_recommendation_validity_current_check CHECK ((((state)::text <> 'ACTIVE'::text) OR ((token_hash IS NOT NULL) AND (valid_through IS NOT NULL)))),
    CONSTRAINT option_recommendation_validity_current_state_check CHECK (((state)::text = ANY ((ARRAY['PENDING'::character varying, 'ACTIVE'::character varying, 'SUSPENDED'::character varying, 'INVALIDATED'::character varying, 'EXPIRED'::character varying, 'SUPERSEDED'::character varying, 'CONSUMED'::character varying])::text[]))),
    CONSTRAINT option_recommendation_validity_current_token_hash_check CHECK (((token_hash IS NULL) OR (token_hash ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT option_recommendation_validity_current_version_check CHECK ((version > 0))
);


--
-- Name: option_recommendation_validity_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_recommendation_validity_events (
    validity_event_id uuid NOT NULL,
    signal_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    prior_state character varying(24),
    new_state character varying(24) NOT NULL,
    market_time timestamp with time zone NOT NULL,
    observed_time timestamp with time zone NOT NULL,
    evaluated_at timestamp with time zone NOT NULL,
    valid_through timestamp with time zone,
    reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    leg_quote_versions jsonb DEFAULT '[]'::jsonb NOT NULL,
    underlying_version character varying(128),
    account_version character varying(128),
    analysis_version character varying(128) NOT NULL,
    scenario_version character varying(128) NOT NULL,
    policy_sha256 character(64) NOT NULL,
    token_hash character(64),
    transition_evidence jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_recommendation_validity_events_check CHECK ((observed_time >= market_time)),
    CONSTRAINT option_recommendation_validity_events_check1 CHECK ((evaluated_at >= observed_time)),
    CONSTRAINT option_recommendation_validity_events_check2 CHECK (((valid_through IS NULL) OR (valid_through > market_time))),
    CONSTRAINT option_recommendation_validity_events_check3 CHECK ((((new_state)::text <> 'ACTIVE'::text) OR ((token_hash IS NOT NULL) AND (valid_through IS NOT NULL)))),
    CONSTRAINT option_recommendation_validity_events_leg_quote_versions_check CHECK ((jsonb_typeof(leg_quote_versions) = 'array'::text)),
    CONSTRAINT option_recommendation_validity_events_new_state_check CHECK (((new_state)::text = ANY ((ARRAY['PENDING'::character varying, 'ACTIVE'::character varying, 'SUSPENDED'::character varying, 'INVALIDATED'::character varying, 'EXPIRED'::character varying, 'SUPERSEDED'::character varying, 'CONSUMED'::character varying])::text[]))),
    CONSTRAINT option_recommendation_validity_events_policy_sha256_check CHECK ((policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_recommendation_validity_events_token_hash_check CHECK (((token_hash IS NULL) OR (token_hash ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT option_recommendation_validity_events_transition_evidence_check CHECK ((jsonb_typeof(transition_evidence) = 'object'::text))
);


--
-- Name: option_retention_holds; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_retention_holds (
    hold_id uuid NOT NULL,
    scope_type character varying(24) NOT NULL,
    selector jsonb NOT NULL,
    reason text NOT NULL,
    actor character varying(128) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone,
    released_at timestamp with time zone,
    released_by character varying(128),
    release_reason text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_retention_holds_check CHECK (((expires_at IS NULL) OR (expires_at > created_at))),
    CONSTRAINT option_retention_holds_check1 CHECK ((((released_at IS NULL) AND (released_by IS NULL) AND (release_reason IS NULL)) OR ((released_at IS NOT NULL) AND (released_by IS NOT NULL) AND (release_reason IS NOT NULL)))),
    CONSTRAINT option_retention_holds_scope_type_check CHECK (((scope_type)::text = ANY ((ARRAY['OBJECT'::character varying, 'TABLE'::character varying, 'PARTITION'::character varying, 'FILE'::character varying, 'DECISION'::character varying, 'INCIDENT'::character varying])::text[]))),
    CONSTRAINT option_retention_holds_selector_check CHECK ((jsonb_typeof(selector) = 'object'::text))
);


--
-- Name: option_scenario_results; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_scenario_results (
    scenario_result_id uuid NOT NULL,
    candidate_id uuid,
    snapshot_id uuid,
    scenario_key character varying(96) NOT NULL,
    spot_shock_fraction double precision NOT NULL,
    iv_shock_fraction double precision NOT NULL,
    time_fraction_remaining double precision NOT NULL,
    repriced_value numeric(20,8),
    profit_loss numeric(20,8),
    delta double precision,
    gamma double precision,
    theta_per_day double precision,
    vega_per_vol_point double precision,
    terminal boolean NOT NULL,
    assumptions jsonb DEFAULT '{}'::jsonb NOT NULL,
    quality_flags text[] DEFAULT ARRAY[]::text[] NOT NULL,
    model_version character varying(64) NOT NULL,
    policy_sha256 character(64) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_scenario_results_assumptions_check CHECK ((jsonb_typeof(assumptions) = 'object'::text)),
    CONSTRAINT option_scenario_results_check CHECK (((candidate_id IS NULL) <> (snapshot_id IS NULL))),
    CONSTRAINT option_scenario_results_policy_sha256_check CHECK ((policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_scenario_results_time_fraction_remaining_check CHECK (((time_fraction_remaining >= (0)::double precision) AND (time_fraction_remaining <= (1)::double precision)))
);


--
-- Name: option_scheduler_instances; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_scheduler_instances (
    instance_id uuid NOT NULL,
    configuration_sha256 character(64) NOT NULL,
    policy_sha256 character(64) NOT NULL,
    process_id integer NOT NULL,
    host_name character varying(255) NOT NULL,
    status character varying(24) NOT NULL,
    acquired_at timestamp with time zone,
    last_heartbeat_at timestamp with time zone NOT NULL,
    stopped_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_scheduler_instances_check CHECK (((stopped_at IS NULL) OR (stopped_at >= last_heartbeat_at))),
    CONSTRAINT option_scheduler_instances_configuration_sha256_check CHECK ((configuration_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_scheduler_instances_policy_sha256_check CHECK ((policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_scheduler_instances_process_id_check CHECK ((process_id > 0)),
    CONSTRAINT option_scheduler_instances_status_check CHECK (((status)::text = ANY ((ARRAY['STARTING'::character varying, 'READ_ONLY'::character varying, 'LEADER'::character varying, 'DEGRADED'::character varying, 'STOPPED'::character varying])::text[])))
);


--
-- Name: option_signal_decay_outcomes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_signal_decay_outcomes (
    outcome_id uuid NOT NULL,
    event_id uuid,
    candidate_id uuid NOT NULL,
    measurement_type character varying(16) NOT NULL,
    market_time timestamp with time zone NOT NULL,
    observed_time timestamp with time zone NOT NULL,
    mark numeric(20,8),
    net_return numeric(20,8),
    availability_flag character varying(32) NOT NULL,
    quality_flags text[] DEFAULT ARRAY[]::text[] NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    entry_net_premium numeric(20,8),
    exit_net_premium numeric(20,8),
    gross_pnl numeric(20,8),
    estimated_cost numeric(20,8),
    net_pnl numeric(20,8),
    capital_at_risk numeric(20,8),
    valuation_policy_version character varying(64),
    valuation_policy_sha256 character(64),
    source_snapshot_ids uuid[] DEFAULT ARRAY[]::uuid[] NOT NULL,
    source_batch_id uuid,
    CONSTRAINT ck_option_decay_proxy_provenance CHECK ((((availability_flag)::text <> 'RESEARCH_DELAYED_PROXY'::text) OR ((entry_net_premium IS NOT NULL) AND (exit_net_premium IS NOT NULL) AND (gross_pnl IS NOT NULL) AND (estimated_cost IS NOT NULL) AND (net_pnl IS NOT NULL) AND (capital_at_risk > (0)::numeric) AND (valuation_policy_version IS NOT NULL) AND (valuation_policy_sha256 ~ '^[0-9a-f]{64}$'::text) AND (cardinality(source_snapshot_ids) > 0) AND (source_batch_id IS NOT NULL)))),
    CONSTRAINT option_signal_decay_outcomes_check CHECK ((observed_time >= market_time)),
    CONSTRAINT option_signal_decay_outcomes_measurement_type_check CHECK (((measurement_type)::text = ANY ((ARRAY['15MIN'::character varying, '30MIN'::character varying, '60MIN'::character varying, 'CLOSE'::character varying, 'NEXT_OPEN'::character varying])::text[])))
);


--
-- Name: option_signal_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_signal_events (
    event_id uuid NOT NULL,
    idempotency_key character(64) NOT NULL,
    signal_identity_key character(64) NOT NULL,
    source_candidate_id uuid NOT NULL,
    underlying character varying(16) NOT NULL,
    strategy_name character varying(64) NOT NULL,
    strategy_version character varying(32) NOT NULL,
    market_data_time timestamp with time zone NOT NULL,
    observed_time timestamp with time zone NOT NULL,
    action character varying(4) NOT NULL,
    net_premium numeric(20,8) NOT NULL,
    stop_loss numeric(20,8),
    take_profit numeric(20,8),
    trailing_activation numeric(20,8),
    trailing_distance numeric(20,8),
    valid_until timestamp with time zone NOT NULL,
    confidence double precision,
    data_quality character varying(32) NOT NULL,
    execution_eligibility character varying(24),
    status character varying(16) NOT NULL,
    blocked_reasons text[] DEFAULT ARRAY[]::text[] NOT NULL,
    occurrence_count integer DEFAULT 1 NOT NULL,
    expected_leg_count integer NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_signal_events_action_check CHECK (((action)::text = ANY ((ARRAY['BUY'::character varying, 'SELL'::character varying])::text[]))),
    CONSTRAINT option_signal_events_check CHECK ((observed_time >= market_data_time)),
    CONSTRAINT option_signal_events_check1 CHECK ((valid_until > market_data_time)),
    CONSTRAINT option_signal_events_check2 CHECK ((((status)::text <> 'READY'::text) OR (execution_eligibility IS NOT NULL))),
    CONSTRAINT option_signal_events_confidence_check CHECK (((confidence IS NULL) OR ((confidence >= (0)::double precision) AND (confidence <= (1)::double precision)))),
    CONSTRAINT option_signal_events_execution_eligibility_check CHECK (((execution_eligibility IS NULL) OR ((execution_eligibility)::text = ANY ((ARRAY['PAPER_PROXY'::character varying, 'LIVE_CANDIDATE'::character varying])::text[])))),
    CONSTRAINT option_signal_events_expected_leg_count_check CHECK ((expected_leg_count > 0)),
    CONSTRAINT option_signal_events_idempotency_key_check CHECK ((idempotency_key ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_signal_events_signal_identity_key_check CHECK ((signal_identity_key ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_signal_events_metadata_check CHECK ((jsonb_typeof(metadata) = 'object'::text)),
    CONSTRAINT option_signal_events_occurrence_count_check CHECK ((occurrence_count > 0)),
    CONSTRAINT option_signal_events_status_check CHECK (((status)::text = ANY ((ARRAY['PENDING'::character varying, 'READY'::character varying, 'BLOCKED'::character varying, 'EXPIRED'::character varying])::text[])))
);


--
-- Name: option_signal_legs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_signal_legs (
    event_id uuid NOT NULL,
    leg_index integer NOT NULL,
    contract_id bigint NOT NULL,
    contract_ticker character varying(64) NOT NULL,
    action character varying(4) NOT NULL,
    ratio integer NOT NULL,
    multiplier integer NOT NULL,
    model_mark numeric(20,8) NOT NULL,
    local_iv double precision NOT NULL,
    local_gamma double precision NOT NULL,
    expiration_date date NOT NULL,
    strike numeric(20,8) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_signal_legs_action_check CHECK (((action)::text = ANY ((ARRAY['BUY'::character varying, 'SELL'::character varying])::text[]))),
    CONSTRAINT option_signal_legs_leg_index_check CHECK ((leg_index >= 0)),
    CONSTRAINT option_signal_legs_model_mark_check CHECK ((model_mark > (0)::numeric)),
    CONSTRAINT option_signal_legs_multiplier_check CHECK ((multiplier > 0)),
    CONSTRAINT option_signal_legs_ratio_check CHECK ((ratio > 0)),
    CONSTRAINT option_signal_legs_strike_check CHECK ((strike > (0)::numeric))
);


--
-- Name: option_signal_occurrences; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_signal_occurrences (
    occurrence_id uuid NOT NULL,
    event_id uuid NOT NULL,
    source_candidate_id uuid NOT NULL,
    market_data_time timestamp with time zone NOT NULL,
    observed_time timestamp with time zone NOT NULL,
    source_batch_id uuid NOT NULL,
    mark_diagnostics jsonb DEFAULT '{}'::jsonb NOT NULL,
    trigger_diagnostics jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_signal_occurrences_check CHECK ((observed_time >= market_data_time)),
    CONSTRAINT option_signal_occurrences_mark_diagnostics_check CHECK ((jsonb_typeof(mark_diagnostics) = 'object'::text)),
    CONSTRAINT option_signal_occurrences_trigger_diagnostics_check CHECK ((jsonb_typeof(trigger_diagnostics) = 'object'::text))
);


--
-- Name: option_signal_suppressions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_signal_suppressions (
    suppression_id uuid NOT NULL,
    candidate_id uuid NOT NULL,
    strategy_name character varying(64) NOT NULL,
    strategy_version character varying(32) NOT NULL,
    decision_time timestamp with time zone NOT NULL,
    failed_gate_codes text[] NOT NULL,
    configuration_version character varying(64) NOT NULL,
    input_provenance jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_signal_suppressions_failed_gate_codes_check CHECK ((cardinality(failed_gate_codes) > 0)),
    CONSTRAINT option_signal_suppressions_input_provenance_check CHECK ((jsonb_typeof(input_provenance) = 'object'::text))
);


--
-- Name: option_snapshot_fact_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_snapshot_fact_keys (
    snapshot_id uuid NOT NULL,
    contract_id bigint NOT NULL,
    provider character varying(32) NOT NULL,
    market_data_time timestamp with time zone NOT NULL,
    normalized_payload_sha256 character(64) NOT NULL,
    first_observed_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_snapshot_fact_keys_check CHECK ((first_observed_at >= market_data_time)),
    CONSTRAINT option_snapshot_fact_keys_normalized_payload_sha256_check CHECK ((normalized_payload_sha256 ~ '^[0-9a-f]{64}$'::text))
);


--
-- Name: option_strategy_candidates; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_strategy_candidates (
    candidate_id uuid NOT NULL,
    candidate_identity character(64) NOT NULL,
    matrix_id uuid NOT NULL,
    strategy_name character varying(64) NOT NULL,
    strategy_version character varying(32) NOT NULL,
    underlying character varying(16) NOT NULL,
    candidate_kind character varying(32) NOT NULL,
    strategy_archetype character varying(48) NOT NULL,
    persona_tags text[] DEFAULT ARRAY[]::text[] NOT NULL,
    structure_type character varying(64) NOT NULL,
    structure_risk_class character varying(32) NOT NULL,
    expiration_date date,
    candidate_rank integer NOT NULL,
    status character varying(16) NOT NULL,
    primary_metric_name character varying(64),
    primary_metric_value double precision,
    rank_components jsonb DEFAULT '{}'::jsonb NOT NULL,
    primary_evidence jsonb DEFAULT '{}'::jsonb NOT NULL,
    net_premium numeric(20,8),
    collateral_required numeric(20,8),
    capital_at_risk numeric(20,8),
    maximum_profit numeric(20,8),
    maximum_loss numeric(20,8),
    return_on_collateral double precision,
    return_on_risk double precision,
    breakevens numeric(20,8)[] DEFAULT (ARRAY[]::numeric[])::numeric(20,8)[] NOT NULL,
    execution_eligibility character varying(24),
    reason_codes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    management_policy_version character varying(64),
    management_policy jsonb DEFAULT '{}'::jsonb NOT NULL,
    policy_sha256 character(64) NOT NULL,
    model_version character varying(64) NOT NULL,
    iv_context_id uuid,
    context_snapshot_id uuid,
    decision_evidence_id uuid NOT NULL,
    market_data_time timestamp with time zone NOT NULL,
    observed_time timestamp with time zone NOT NULL,
    valid_until timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_strategy_candidates_candidate_identity_check CHECK ((candidate_identity ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_strategy_candidates_candidate_kind_check CHECK (((candidate_kind)::text = ANY ((ARRAY['RESEARCH_ONLY'::character varying, 'SINGLE_CONTRACT'::character varying, 'MULTI_LEG'::character varying])::text[]))),
    CONSTRAINT option_strategy_candidates_candidate_rank_check CHECK ((candidate_rank > 0)),
    CONSTRAINT option_strategy_candidates_capital_at_risk_check CHECK (((capital_at_risk IS NULL) OR (capital_at_risk >= (0)::numeric))),
    CONSTRAINT option_strategy_candidates_check CHECK ((observed_time >= market_data_time)),
    CONSTRAINT option_strategy_candidates_check1 CHECK (((valid_until IS NULL) OR (valid_until > market_data_time))),
    CONSTRAINT option_strategy_candidates_check2 CHECK ((((status)::text = 'SELECTED'::text) OR (execution_eligibility IS NULL))),
    CONSTRAINT option_strategy_candidates_check3 CHECK ((((candidate_kind)::text <> 'RESEARCH_ONLY'::text) OR (execution_eligibility IS NULL))),
    CONSTRAINT option_strategy_candidates_collateral_required_check CHECK (((collateral_required IS NULL) OR (collateral_required >= (0)::numeric))),
    CONSTRAINT option_strategy_candidates_execution_eligibility_check CHECK (((execution_eligibility IS NULL) OR ((execution_eligibility)::text = ANY ((ARRAY['PAPER_PROXY'::character varying, 'LIVE_CANDIDATE'::character varying])::text[])))),
    CONSTRAINT option_strategy_candidates_management_policy_check CHECK ((jsonb_typeof(management_policy) = 'object'::text)),
    CONSTRAINT option_strategy_candidates_maximum_loss_check CHECK (((maximum_loss IS NULL) OR (maximum_loss >= (0)::numeric))),
    CONSTRAINT option_strategy_candidates_maximum_loss_check1 CHECK (((maximum_loss IS NULL) OR (maximum_loss > (0)::numeric))),
    CONSTRAINT option_strategy_candidates_persona_tags_check CHECK ((cardinality(persona_tags) > 0)),
    CONSTRAINT option_strategy_candidates_policy_sha256_check CHECK ((policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_strategy_candidates_primary_evidence_check CHECK ((jsonb_typeof(primary_evidence) = 'object'::text)),
    CONSTRAINT option_strategy_candidates_rank_components_check CHECK ((jsonb_typeof(rank_components) = 'object'::text)),
    CONSTRAINT option_strategy_candidates_status_check CHECK (((status)::text = ANY ((ARRAY['SELECTED'::character varying, 'SUPPRESSED'::character varying, 'REJECTED'::character varying])::text[]))),
    CONSTRAINT option_strategy_candidates_structure_risk_class_check CHECK (((structure_risk_class)::text = ANY ((ARRAY['RESEARCH_CONTEXT'::character varying, 'CASH_SECURED'::character varying, 'DEFINED_RISK_CREDIT'::character varying, 'PREMIUM_AT_RISK_DEBIT'::character varying])::text[])))
);


--
-- Name: option_strategy_registry; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_strategy_registry (
    strategy_name character varying(64) NOT NULL,
    strategy_version character varying(32) NOT NULL,
    display_name character varying(96) NOT NULL,
    strategy_archetype character varying(48) NOT NULL,
    persona_tags text[] DEFAULT ARRAY[]::text[] NOT NULL,
    allowed_structure_types text[] DEFAULT ARRAY[]::text[] NOT NULL,
    allowed_risk_classes text[] DEFAULT ARRAY[]::text[] NOT NULL,
    presentation_metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    policy_sha256 character(64) NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    effective_from timestamp with time zone NOT NULL,
    effective_to timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_strategy_registry_allowed_risk_classes_check CHECK ((cardinality(allowed_risk_classes) > 0)),
    CONSTRAINT option_strategy_registry_allowed_structure_types_check CHECK ((cardinality(allowed_structure_types) > 0)),
    CONSTRAINT option_strategy_registry_check CHECK (((effective_to IS NULL) OR (effective_to > effective_from))),
    CONSTRAINT option_strategy_registry_persona_tags_check CHECK ((cardinality(persona_tags) > 0)),
    CONSTRAINT option_strategy_registry_policy_sha256_check CHECK ((policy_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_strategy_registry_presentation_metadata_check CHECK ((jsonb_typeof(presentation_metadata) = 'object'::text))
);


--
-- Name: option_trade_cursors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_trade_cursors (
    provider character varying(32) NOT NULL,
    contract_id bigint NOT NULL,
    completed_sip_timestamp timestamp with time zone NOT NULL,
    completed_sequence_number bigint NOT NULL,
    overlap_seconds integer NOT NULL,
    latest_complete_request_id character varying(128),
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_trade_cursors_completed_sequence_number_check CHECK ((completed_sequence_number >= 0)),
    CONSTRAINT option_trade_cursors_overlap_seconds_check CHECK ((overlap_seconds >= 0))
);


--
-- Name: option_trade_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_trade_events (
    trade_event_id uuid NOT NULL,
    provider character varying(32) NOT NULL,
    contract_id bigint NOT NULL,
    contract_ticker character varying(64) NOT NULL,
    underlying character varying(16) NOT NULL,
    sip_timestamp timestamp with time zone NOT NULL,
    sequence_number bigint NOT NULL,
    participant_timestamp timestamp with time zone,
    first_observed_at timestamp with time zone NOT NULL,
    revised_observed_at timestamp with time zone,
    exchange integer,
    conditions integer[] DEFAULT ARRAY[]::integer[] NOT NULL,
    correction integer,
    provider_trade_id character varying(128),
    price numeric(20,8) NOT NULL,
    size bigint NOT NULL,
    shares_per_contract integer NOT NULL,
    notional numeric(24,8) NOT NULL,
    payload_sha256 character(64) NOT NULL,
    raw_batch_id uuid NOT NULL,
    classification_status character varying(24) NOT NULL,
    classification_reasons text[] DEFAULT ARRAY[]::text[] NOT NULL,
    semantics_version character varying(64),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_trade_events_check CHECK ((first_observed_at >= sip_timestamp)),
    CONSTRAINT option_trade_events_check1 CHECK (((revised_observed_at IS NULL) OR (revised_observed_at >= first_observed_at))),
    CONSTRAINT option_trade_events_check2 CHECK ((notional = ((price * (size)::numeric) * (shares_per_contract)::numeric))),
    CONSTRAINT option_trade_events_classification_status_check CHECK (((classification_status)::text = ANY ((ARRAY['PENDING'::character varying, 'INCLUDED'::character varying, 'EXCLUDED'::character varying, 'SUPERSEDED'::character varying, 'CANCELED'::character varying, 'UNKNOWN'::character varying])::text[]))),
    CONSTRAINT option_trade_events_notional_check CHECK ((notional > (0)::numeric)),
    CONSTRAINT option_trade_events_payload_sha256_check CHECK ((payload_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_trade_events_price_check CHECK ((price > (0)::numeric)),
    CONSTRAINT option_trade_events_sequence_number_check CHECK ((sequence_number >= 0)),
    CONSTRAINT option_trade_events_shares_per_contract_check CHECK ((shares_per_contract > 0)),
    CONSTRAINT option_trade_events_size_check CHECK ((size > 0))
)
PARTITION BY RANGE (sip_timestamp);


--
-- Name: option_trade_watchlist; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_trade_watchlist (
    contract_id bigint NOT NULL,
    reason character varying(24) NOT NULL,
    active_from timestamp with time zone NOT NULL,
    active_to timestamp with time zone,
    first_observed_at timestamp with time zone NOT NULL,
    source_id character varying(128) NOT NULL,
    backfill_from timestamp with time zone,
    backfill_through timestamp with time zone,
    backfill_status character varying(16) DEFAULT 'NOT_REQUIRED'::character varying NOT NULL,
    backfill_error text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_trade_watchlist_backfill_status_check CHECK (((backfill_status)::text = ANY ((ARRAY['NOT_REQUIRED'::character varying, 'PENDING'::character varying, 'RUNNING'::character varying, 'COMPLETE'::character varying, 'FAILED'::character varying])::text[]))),
    CONSTRAINT option_trade_watchlist_check CHECK (((active_to IS NULL) OR (active_to > active_from))),
    CONSTRAINT option_trade_watchlist_check1 CHECK ((first_observed_at >= active_from)),
    CONSTRAINT option_trade_watchlist_check2 CHECK ((((backfill_from IS NULL) AND (backfill_through IS NULL) AND ((backfill_status)::text = 'NOT_REQUIRED'::text)) OR ((backfill_from IS NOT NULL) AND (backfill_through IS NOT NULL) AND (backfill_through >= backfill_from) AND ((backfill_status)::text <> 'NOT_REQUIRED'::text)))),
    CONSTRAINT option_trade_watchlist_check3 CHECK ((((backfill_status)::text <> 'FAILED'::text) OR (backfill_error IS NOT NULL))),
    CONSTRAINT option_trade_watchlist_reason_check CHECK (((reason)::text = ANY ((ARRAY['FILTERED'::character varying, 'CANDIDATE'::character varying, 'WORKING_ORDER'::character varying, 'OPEN_POSITION'::character varying])::text[])))
);


--
-- Name: option_universe_candidates; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_universe_candidates (
    run_id uuid NOT NULL,
    ticker character varying(16) NOT NULL,
    asset_type character varying(8) NOT NULL,
    raw_metrics jsonb DEFAULT '{}'::jsonb NOT NULL,
    component_ranks jsonb DEFAULT '{}'::jsonb NOT NULL,
    total_score double precision,
    eligible boolean NOT NULL,
    exclusion_reasons text[] DEFAULT ARRAY[]::text[] NOT NULL,
    candidate_rank integer,
    first_observed_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_universe_candidates_asset_type_check CHECK (((asset_type)::text = ANY ((ARRAY['STOCK'::character varying, 'ETF'::character varying])::text[]))),
    CONSTRAINT option_universe_candidates_candidate_rank_check CHECK (((candidate_rank IS NULL) OR (candidate_rank > 0))),
    CONSTRAINT option_universe_candidates_component_ranks_check CHECK ((jsonb_typeof(component_ranks) = 'object'::text)),
    CONSTRAINT option_universe_candidates_raw_metrics_check CHECK ((jsonb_typeof(raw_metrics) = 'object'::text))
);


--
-- Name: option_universe_members; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_universe_members (
    member_id bigint NOT NULL,
    effective_from date NOT NULL,
    ticker character varying(16) NOT NULL,
    asset_type character varying(8) NOT NULL,
    source_run_id uuid NOT NULL,
    member_rank integer,
    score double precision,
    activated_at timestamp with time zone NOT NULL,
    deactivated_at timestamp with time zone,
    first_observed_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_universe_members_asset_type_check CHECK (((asset_type)::text = ANY ((ARRAY['STOCK'::character varying, 'ETF'::character varying])::text[]))),
    CONSTRAINT option_universe_members_check CHECK (((deactivated_at IS NULL) OR (deactivated_at > activated_at))),
    CONSTRAINT option_universe_members_check1 CHECK ((first_observed_at >= activated_at)),
    CONSTRAINT option_universe_members_member_rank_check CHECK (((member_rank IS NULL) OR (member_rank > 0)))
);


--
-- Name: option_universe_members_member_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.option_universe_members_member_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: option_universe_members_member_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.option_universe_members_member_id_seq OWNED BY public.option_universe_members.member_id;


--
-- Name: option_universe_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_universe_runs (
    run_id uuid NOT NULL,
    mode character varying(16) NOT NULL,
    as_of_session date NOT NULL,
    effective_from date NOT NULL,
    status character varying(24) NOT NULL,
    completeness_fraction double precision,
    configuration_json jsonb NOT NULL,
    configuration_sha256 character(64) NOT NULL,
    started_at timestamp with time zone NOT NULL,
    completed_at timestamp with time zone,
    first_observed_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_universe_runs_check CHECK ((effective_from >= as_of_session)),
    CONSTRAINT option_universe_runs_check1 CHECK (((completed_at IS NULL) OR (completed_at >= started_at))),
    CONSTRAINT option_universe_runs_completeness_fraction_check CHECK (((completeness_fraction IS NULL) OR ((completeness_fraction >= (0)::double precision) AND (completeness_fraction <= (1)::double precision)))),
    CONSTRAINT option_universe_runs_configuration_json_check CHECK ((jsonb_typeof(configuration_json) = 'object'::text)),
    CONSTRAINT option_universe_runs_configuration_sha256_check CHECK ((configuration_sha256 ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT option_universe_runs_mode_check CHECK (((mode)::text = ANY ((ARRAY['fixed'::character varying, 'ranked'::character varying])::text[]))),
    CONSTRAINT option_universe_runs_status_check CHECK (((status)::text = ANY ((ARRAY['PENDING'::character varying, 'RUNNING'::character varying, 'COMPLETE'::character varying, 'DEGRADED'::character varying, 'FAILED'::character varying])::text[])))
);


--
-- Name: option_volatility_surfaces; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_volatility_surfaces (
    volatility_surface_id uuid NOT NULL,
    matrix_id uuid NOT NULL,
    underlying character varying(16) NOT NULL,
    expiration_date date NOT NULL,
    contract_type character varying(4) NOT NULL,
    window_start timestamp with time zone NOT NULL,
    window_end timestamp with time zone NOT NULL,
    input_count integer NOT NULL,
    minimum_strike numeric(20,8),
    maximum_strike numeric(20,8),
    fit_model character varying(64) NOT NULL,
    fit_version character varying(64) NOT NULL,
    fit_diagnostics jsonb DEFAULT '{}'::jsonb NOT NULL,
    residual_distribution jsonb DEFAULT '{}'::jsonb NOT NULL,
    coefficients jsonb DEFAULT '{}'::jsonb NOT NULL,
    quality_reasons text[] DEFAULT ARRAY[]::text[] NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT option_volatility_surfaces_check CHECK ((window_end >= window_start)),
    CONSTRAINT option_volatility_surfaces_check1 CHECK (((maximum_strike IS NULL) OR (minimum_strike IS NULL) OR (maximum_strike >= minimum_strike))),
    CONSTRAINT option_volatility_surfaces_coefficients_check CHECK ((jsonb_typeof(coefficients) = 'object'::text)),
    CONSTRAINT option_volatility_surfaces_contract_type_check CHECK (((contract_type)::text = ANY ((ARRAY['CALL'::character varying, 'PUT'::character varying])::text[]))),
    CONSTRAINT option_volatility_surfaces_fit_diagnostics_check CHECK ((jsonb_typeof(fit_diagnostics) = 'object'::text)),
    CONSTRAINT option_volatility_surfaces_input_count_check CHECK ((input_count >= 0)),
    CONSTRAINT option_volatility_surfaces_residual_distribution_check CHECK ((jsonb_typeof(residual_distribution) = 'object'::text))
);


--
-- Name: option_work_items; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.option_work_items (
    work_id uuid NOT NULL,
    stage character varying(24) NOT NULL,
    subject_id character varying(128) NOT NULL,
    business_key character varying(256) NOT NULL,
    status character varying(24) NOT NULL,
    lease_owner character varying(128),
    lease_expires_at timestamp with time zone,
    attempt_count integer DEFAULT 0 NOT NULL,
    maximum_attempts integer NOT NULL,
    next_attempt_at timestamp with time zone DEFAULT now() NOT NULL,
    last_error text,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT option_work_items_attempt_count_check CHECK ((attempt_count >= 0)),
    CONSTRAINT option_work_items_check CHECK ((attempt_count <= maximum_attempts)),
    CONSTRAINT option_work_items_check1 CHECK (((((status)::text = 'CLAIMED'::text) AND (lease_owner IS NOT NULL) AND (lease_expires_at IS NOT NULL)) OR (((status)::text <> 'CLAIMED'::text) AND (lease_owner IS NULL) AND (lease_expires_at IS NULL)))),
    CONSTRAINT option_work_items_check2 CHECK ((((status)::text <> 'COMPLETED'::text) OR (completed_at IS NOT NULL))),
    CONSTRAINT option_work_items_check3 CHECK ((((status)::text <> 'TERMINAL_FAILED'::text) OR (last_error IS NOT NULL))),
    CONSTRAINT option_work_items_maximum_attempts_check CHECK ((maximum_attempts > 0)),
    CONSTRAINT option_work_items_payload_check CHECK ((jsonb_typeof(payload) = 'object'::text)),
    CONSTRAINT option_work_items_stage_check CHECK (((stage)::text = ANY ((ARRAY['NORMALIZE'::character varying, 'ANALYZE'::character varying, 'STRATEGY'::character varying, 'ARCHIVE'::character varying, 'TRADE_BACKFILL'::character varying, 'CLASSIFY_TRADES'::character varying])::text[]))),
    CONSTRAINT option_work_items_status_check CHECK (((status)::text = ANY ((ARRAY['PENDING'::character varying, 'CLAIMED'::character varying, 'RETRY'::character varying, 'COMPLETED'::character varying, 'TERMINAL_FAILED'::character varying])::text[])))
);


--
-- Name: pattern_analog_matches; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pattern_analog_matches (
    analog_id bigint NOT NULL,
    current_trade_date date NOT NULL,
    current_ticker character varying(10) NOT NULL,
    current_breakout_score integer,
    current_vwap_score integer,
    current_volatility_score integer,
    current_trend_score integer,
    current_rs_score integer,
    current_calendar_score integer,
    current_sector_regime character varying(20),
    current_market_breadth integer,
    analog_count integer DEFAULT 0,
    analog_accuracy numeric(5,2),
    analog_details jsonb,
    analog_confidence_boost integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: pattern_analog_matches_analog_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.pattern_analog_matches_analog_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pattern_analog_matches_analog_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.pattern_analog_matches_analog_id_seq OWNED BY public.pattern_analog_matches.analog_id;


--
-- Name: pattern_win_rate_priors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pattern_win_rate_priors (
    prior_id bigint NOT NULL,
    effective_date date NOT NULL,
    pattern_name character varying(50) NOT NULL,
    historical_win_rate numeric(5,2),
    sample_size integer,
    lookback_days integer DEFAULT 60,
    confidence_multiplier numeric(3,2),
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: pattern_win_rate_priors_prior_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.pattern_win_rate_priors_prior_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pattern_win_rate_priors_prior_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.pattern_win_rate_priors_prior_id_seq OWNED BY public.pattern_win_rate_priors.prior_id;


--
-- Name: recommendation_performance_log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.recommendation_performance_log (
    perf_id bigint NOT NULL,
    report_date date NOT NULL,
    bull_recommendations_total integer,
    bull_recommendations_hit integer,
    bull_win_rate_pct numeric(5,2),
    bear_recommendations_total integer,
    bear_recommendations_hit integer,
    bear_win_rate_pct numeric(5,2),
    overall_recommendations_total integer,
    overall_recommendations_hit integer,
    overall_win_rate_pct numeric(5,2),
    win_rate_5day numeric(5,2),
    win_rate_20day numeric(5,2),
    win_rate_monthly numeric(5,2),
    breakout_pattern_accuracy numeric(5,2),
    breakout_pattern_sample_size integer,
    vwap_pattern_accuracy numeric(5,2),
    vwap_pattern_sample_size integer,
    volatility_pattern_accuracy numeric(5,2),
    volatility_pattern_sample_size integer,
    trend_pattern_accuracy numeric(5,2),
    trend_pattern_sample_size integer,
    rs_pattern_accuracy numeric(5,2),
    rs_pattern_sample_size integer,
    calendar_pattern_accuracy numeric(5,2),
    calendar_pattern_sample_size integer,
    signal_75_80_pct_accuracy numeric(5,2),
    signal_75_80_pct_sample_size integer,
    signal_80_90_pct_accuracy numeric(5,2),
    signal_80_90_pct_sample_size integer,
    signal_90_100_pct_accuracy numeric(5,2),
    signal_90_100_pct_sample_size integer,
    patterns_needing_adjustment text,
    recommended_weight_changes text,
    created_at timestamp with time zone DEFAULT now(),
    notes text
);


--
-- Name: recommendation_performance_log_perf_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.recommendation_performance_log_perf_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: recommendation_performance_log_perf_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.recommendation_performance_log_perf_id_seq OWNED BY public.recommendation_performance_log.perf_id;


--
-- Name: research_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.research_runs (
    run_id integer NOT NULL,
    run_at timestamp with time zone DEFAULT now(),
    features text NOT NULL,
    horizon_days smallint NOT NULL,
    rebalance_days smallint NOT NULL,
    cost_bps real NOT NULL,
    neutralised boolean DEFAULT true NOT NULL,
    data_start date,
    data_end date,
    rows_used integer,
    tickers integer,
    test_periods integer,
    ic_mean double precision,
    ic_ir double precision,
    ic_t_stat double precision,
    decile_spread double precision,
    ls_net_return double precision,
    ls_net_sharpe double precision,
    turnover double precision,
    base_return double precision,
    base_sharpe double precision,
    verdict character varying(32),
    checks_passed smallint,
    checks_total smallint,
    activity_filter character varying(32) DEFAULT 'none'::character varying NOT NULL,
    top_net_return double precision,
    top_net_sharpe double precision,
    top_turnover double precision,
    top_alpha_return double precision,
    top_alpha_t_stat double precision,
    top_alpha_sharpe double precision,
    cal_years jsonb,
    cal_year_positive_pct jsonb
);


--
-- Name: research_runs_run_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.research_runs_run_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: research_runs_run_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.research_runs_run_id_seq OWNED BY public.research_runs.run_id;


--
-- Name: sector_regime_daily; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sector_regime_daily (
    sector_regime_id bigint NOT NULL,
    trade_date date NOT NULL,
    sector character varying(50) NOT NULL,
    etf_symbol character varying(10) NOT NULL,
    open_regime character varying(20),
    open_score integer,
    update_time timestamp with time zone,
    current_regime character varying(20),
    current_score integer,
    close_regime character varying(20),
    close_score integer,
    session_return_pct numeric(6,3),
    price_vs_sma40_pct numeric(6,3),
    rsi_14 numeric(5,2),
    atr_20day numeric(12,4),
    volume_vs_sma_20 numeric(6,3),
    created_at timestamp with time zone DEFAULT now(),
    CONSTRAINT sector_regime_daily_close_score_check CHECK (((close_score >= 0) AND (close_score <= 100))),
    CONSTRAINT sector_regime_daily_current_score_check CHECK (((current_score >= 0) AND (current_score <= 100))),
    CONSTRAINT sector_regime_daily_open_score_check CHECK (((open_score >= 0) AND (open_score <= 100)))
);


--
-- Name: sector_regime_daily_sector_regime_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.sector_regime_daily_sector_regime_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: sector_regime_daily_sector_regime_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.sector_regime_daily_sector_regime_id_seq OWNED BY public.sector_regime_daily.sector_regime_id;


--
-- Name: selected_tickers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.selected_tickers (
    id integer NOT NULL,
    ticker text NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp without time zone DEFAULT now(),
    asset_type text,
    sector text,
    industry text,
    market_cap bigint,
    market_cap_group text,
    beta double precision,
    avg_volume_90d bigint,
    float_shares bigint,
    short_percent double precision,
    institutional_pct double precision,
    dividend_yield double precision,
    pe_ratio double precision,
    exchange text,
    metadata_updated timestamp without time zone,
    sic_code text,
    company_name text,
    security_id uuid,
    cik character varying(16),
    composite_figi character varying(32),
    share_class_figi character varying(32),
    weighted_shares_outstanding numeric(24,4),
    metadata_source character varying(64),
    metadata_observed_at timestamp with time zone
);


--
-- Name: selected_tickers_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.selected_tickers_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: selected_tickers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.selected_tickers_id_seq OWNED BY public.selected_tickers.id;


--
-- Name: cross_sectional_signals signal_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cross_sectional_signals ALTER COLUMN signal_id SET DEFAULT nextval('public.cross_sectional_signals_signal_id_seq'::regclass);


--
-- Name: daily_recommendations rec_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.daily_recommendations ALTER COLUMN rec_id SET DEFAULT nextval('public.daily_recommendations_rec_id_seq'::regclass);


--
-- Name: data_ingestion_failures failure_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.data_ingestion_failures ALTER COLUMN failure_id SET DEFAULT nextval('public.data_ingestion_failures_failure_id_seq'::regclass);


--
-- Name: market_context_daily context_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.market_context_daily ALTER COLUMN context_id SET DEFAULT nextval('public.market_context_daily_context_id_seq'::regclass);


--
-- Name: market_discovery_states discovery_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.market_discovery_states ALTER COLUMN discovery_id SET DEFAULT nextval('public.market_discovery_states_discovery_id_seq'::regclass);


--
-- Name: opening_pattern_scores score_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.opening_pattern_scores ALTER COLUMN score_id SET DEFAULT nextval('public.opening_pattern_scores_score_id_seq'::regclass);


--
-- Name: option_contract_catalog contract_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_contract_catalog ALTER COLUMN contract_id SET DEFAULT nextval('public.option_contract_catalog_contract_id_seq'::regclass);


--
-- Name: option_contract_catalog_versions catalog_version_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_contract_catalog_versions ALTER COLUMN catalog_version_id SET DEFAULT nextval('public.option_contract_catalog_versions_catalog_version_id_seq'::regclass);


--
-- Name: option_contract_discoveries discovery_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_contract_discoveries ALTER COLUMN discovery_id SET DEFAULT nextval('public.option_contract_discoveries_discovery_id_seq'::regclass);


--
-- Name: option_provider_trade_semantics semantics_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_provider_trade_semantics ALTER COLUMN semantics_id SET DEFAULT nextval('public.option_provider_trade_semantics_semantics_id_seq'::regclass);


--
-- Name: option_universe_members member_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_universe_members ALTER COLUMN member_id SET DEFAULT nextval('public.option_universe_members_member_id_seq'::regclass);


--
-- Name: pattern_analog_matches analog_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pattern_analog_matches ALTER COLUMN analog_id SET DEFAULT nextval('public.pattern_analog_matches_analog_id_seq'::regclass);


--
-- Name: pattern_win_rate_priors prior_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pattern_win_rate_priors ALTER COLUMN prior_id SET DEFAULT nextval('public.pattern_win_rate_priors_prior_id_seq'::regclass);


--
-- Name: recommendation_performance_log perf_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.recommendation_performance_log ALTER COLUMN perf_id SET DEFAULT nextval('public.recommendation_performance_log_perf_id_seq'::regclass);


--
-- Name: research_runs run_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.research_runs ALTER COLUMN run_id SET DEFAULT nextval('public.research_runs_run_id_seq'::regclass);


--
-- Name: sector_regime_daily sector_regime_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sector_regime_daily ALTER COLUMN sector_regime_id SET DEFAULT nextval('public.sector_regime_daily_sector_regime_id_seq'::regclass);


--
-- Name: selected_tickers id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.selected_tickers ALTER COLUMN id SET DEFAULT nextval('public.selected_tickers_id_seq'::regclass);


--
-- Name: cross_sectional_signals cross_sectional_signals_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cross_sectional_signals
    ADD CONSTRAINT cross_sectional_signals_pkey PRIMARY KEY (signal_id);


--
-- Name: daily_recommendations daily_recommendations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.daily_recommendations
    ADD CONSTRAINT daily_recommendations_pkey PRIMARY KEY (rec_id);


--
-- Name: data_ingestion_failures data_ingestion_failures_dataset_ticker_trade_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.data_ingestion_failures
    ADD CONSTRAINT data_ingestion_failures_dataset_ticker_trade_date_key UNIQUE (dataset, ticker, trade_date);


--
-- Name: data_ingestion_failures data_ingestion_failures_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.data_ingestion_failures
    ADD CONSTRAINT data_ingestion_failures_pkey PRIMARY KEY (failure_id);


--
-- Name: equity_analysis_members equity_analysis_members_analysis_run_id_ticker_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_analysis_members
    ADD CONSTRAINT equity_analysis_members_analysis_run_id_ticker_key UNIQUE (analysis_run_id, ticker);


--
-- Name: equity_analysis_members equity_analysis_members_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_analysis_members
    ADD CONSTRAINT equity_analysis_members_pkey PRIMARY KEY (analysis_run_id, security_id);


--
-- Name: equity_analysis_runs equity_analysis_runs_business_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_analysis_runs
    ADD CONSTRAINT equity_analysis_runs_business_key_key UNIQUE (business_key);


--
-- Name: equity_analysis_runs equity_analysis_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_analysis_runs
    ADD CONSTRAINT equity_analysis_runs_pkey PRIMARY KEY (analysis_run_id);


--
-- Name: equity_bar_publication_members equity_bar_publication_members_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_bar_publication_members
    ADD CONSTRAINT equity_bar_publication_members_pkey PRIMARY KEY (publication_id, ticker);


--
-- Name: equity_bar_publications equity_bar_publications_business_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_bar_publications
    ADD CONSTRAINT equity_bar_publications_business_key_key UNIQUE (business_key);


--
-- Name: equity_bar_publications equity_bar_publications_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_bar_publications
    ADD CONSTRAINT equity_bar_publications_pkey PRIMARY KEY (publication_id);


--
-- Name: equity_bar_revisions equity_bar_revisions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_bar_revisions
    ADD CONSTRAINT equity_bar_revisions_pkey PRIMARY KEY (bar_revision_id);


--
-- Name: equity_context_evidence equity_context_evidence_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_context_evidence
    ADD CONSTRAINT equity_context_evidence_pkey PRIMARY KEY (equity_context_snapshot_id, evidence_id);


--
-- Name: equity_context_snapshots equity_context_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_context_snapshots
    ADD CONSTRAINT equity_context_snapshots_pkey PRIMARY KEY (equity_context_snapshot_id);


--
-- Name: equity_context_snapshots equity_context_snapshots_ticker_strategy_horizon_market_tim_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_context_snapshots
    ADD CONSTRAINT equity_context_snapshots_ticker_strategy_horizon_market_tim_key UNIQUE (ticker, strategy_horizon, market_time, observed_at, context_policy_sha256);


--
-- Name: equity_corporate_actions equity_corporate_actions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_corporate_actions
    ADD CONSTRAINT equity_corporate_actions_pkey PRIMARY KEY (corporate_action_id);


--
-- Name: equity_corporate_actions equity_corporate_actions_source_source_key_first_observed_a_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_corporate_actions
    ADD CONSTRAINT equity_corporate_actions_source_source_key_first_observed_a_key UNIQUE (source, source_key, first_observed_at);


--
-- Name: equity_current_bar_projection equity_current_bar_projection_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_current_bar_projection
    ADD CONSTRAINT equity_current_bar_projection_pkey PRIMARY KEY (ticker, "interval", bar_start, session_scope, adjusted);


--
-- Name: equity_current_projection equity_current_projection_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_current_projection
    ADD CONSTRAINT equity_current_projection_pkey PRIMARY KEY (ticker, interval_key, projection_type, source_name);


--
-- Name: equity_evidence equity_evidence_evidence_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_evidence
    ADD CONSTRAINT equity_evidence_evidence_key_key UNIQUE (evidence_key);


--
-- Name: equity_evidence equity_evidence_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_evidence
    ADD CONSTRAINT equity_evidence_pkey PRIMARY KEY (evidence_id);


--
-- Name: equity_fundamental_reports equity_fundamental_reports_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_fundamental_reports
    ADD CONSTRAINT equity_fundamental_reports_pkey PRIMARY KEY (fundamental_report_id);


--
-- Name: equity_fundamental_reports equity_fundamental_reports_source_source_key_payload_sha256_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_fundamental_reports
    ADD CONSTRAINT equity_fundamental_reports_source_source_key_payload_sha256_key UNIQUE (source, source_key, payload_sha256);


--
-- Name: equity_ingestion_segments equity_ingestion_segments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_ingestion_segments
    ADD CONSTRAINT equity_ingestion_segments_pkey PRIMARY KEY (ingestion_segment_id);


--
-- Name: equity_outcome_policies equity_outcome_policies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_outcome_policies
    ADD CONSTRAINT equity_outcome_policies_pkey PRIMARY KEY (outcome_policy_id);


--
-- Name: equity_outcome_policies equity_outcome_policies_policy_key_policy_version_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_outcome_policies
    ADD CONSTRAINT equity_outcome_policies_policy_key_policy_version_key UNIQUE (policy_key, policy_version);


--
-- Name: equity_portal_current_projections equity_portal_current_projections_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_portal_current_projections
    ADD CONSTRAINT equity_portal_current_projections_pkey PRIMARY KEY (snapshot_type);


--
-- Name: equity_portal_snapshots equity_portal_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_portal_snapshots
    ADD CONSTRAINT equity_portal_snapshots_pkey PRIMARY KEY (snapshot_id);


--
-- Name: equity_portal_snapshots equity_portal_snapshots_snapshot_type_source_manifest_sha25_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_portal_snapshots
    ADD CONSTRAINT equity_portal_snapshots_snapshot_type_source_manifest_sha25_key UNIQUE (snapshot_type, source_manifest_sha256, payload_sha256);


--
-- Name: equity_portal_source_state equity_portal_source_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_portal_source_state
    ADD CONSTRAINT equity_portal_source_state_pkey PRIMARY KEY (singleton);


--
-- Name: equity_qualification_revisions equity_qualification_revision_source_name_source_version_in_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_qualification_revisions
    ADD CONSTRAINT equity_qualification_revision_source_name_source_version_in_key UNIQUE (source_name, source_version, "interval", direction, horizon_key, outcome_policy_key, evaluation_version, effective_from);


--
-- Name: equity_qualification_revisions equity_qualification_revisions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_qualification_revisions
    ADD CONSTRAINT equity_qualification_revisions_pkey PRIMARY KEY (qualification_revision_id);


--
-- Name: equity_research_outcomes equity_research_outcomes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_research_outcomes
    ADD CONSTRAINT equity_research_outcomes_pkey PRIMARY KEY (outcome_id);


--
-- Name: equity_research_outcomes equity_research_outcomes_subject_evidence_id_outcome_policy_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_research_outcomes
    ADD CONSTRAINT equity_research_outcomes_subject_evidence_id_outcome_policy_key UNIQUE (subject_evidence_id, outcome_policy_id, horizon_key, outcome_revision);


--
-- Name: equity_security_reference_revisions equity_security_reference_rev_security_id_source_effective__key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_security_reference_revisions
    ADD CONSTRAINT equity_security_reference_rev_security_id_source_effective__key UNIQUE (security_id, source, effective_from, payload_sha256);


--
-- Name: equity_security_reference_revisions equity_security_reference_revisions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_security_reference_revisions
    ADD CONSTRAINT equity_security_reference_revisions_pkey PRIMARY KEY (security_revision_id);


--
-- Name: equity_universe_members equity_universe_members_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_universe_members
    ADD CONSTRAINT equity_universe_members_pkey PRIMARY KEY (universe_run_id, security_id);


--
-- Name: equity_universe_members equity_universe_members_universe_run_id_ticker_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_universe_members
    ADD CONSTRAINT equity_universe_members_universe_run_id_ticker_key UNIQUE (universe_run_id, ticker);


--
-- Name: equity_universe_runs equity_universe_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_universe_runs
    ADD CONSTRAINT equity_universe_runs_pkey PRIMARY KEY (universe_run_id);


--
-- Name: market_context_daily market_context_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.market_context_daily
    ADD CONSTRAINT market_context_daily_pkey PRIMARY KEY (context_id);


--
-- Name: market_context_daily market_context_daily_trade_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.market_context_daily
    ADD CONSTRAINT market_context_daily_trade_date_key UNIQUE (trade_date);


--
-- Name: market_discovery_states market_discovery_states_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.market_discovery_states
    ADD CONSTRAINT market_discovery_states_pkey PRIMARY KEY (discovery_id);


--
-- Name: market_discovery_states market_discovery_states_trade_date_ticker_model_version_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.market_discovery_states
    ADD CONSTRAINT market_discovery_states_trade_date_ticker_model_version_key UNIQUE (trade_date, ticker, model_version);


--
-- Name: opening_pattern_scores opening_pattern_scores_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.opening_pattern_scores
    ADD CONSTRAINT opening_pattern_scores_pkey PRIMARY KEY (score_id);


--
-- Name: option_analysis_runs option_analysis_runs_batch_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_analysis_runs
    ADD CONSTRAINT option_analysis_runs_batch_id_key UNIQUE (batch_id);


--
-- Name: option_analysis_runs option_analysis_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_analysis_runs
    ADD CONSTRAINT option_analysis_runs_pkey PRIMARY KEY (matrix_id);


--
-- Name: option_candidate_legs option_candidate_legs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_candidate_legs
    ADD CONSTRAINT option_candidate_legs_pkey PRIMARY KEY (candidate_id, leg_index);


--
-- Name: option_chain_snapshots option_chain_snapshots_contract_id_provider_market_data_tim_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_chain_snapshots
    ADD CONSTRAINT option_chain_snapshots_contract_id_provider_market_data_tim_key UNIQUE (contract_id, provider, market_data_time, normalized_payload_sha256, first_observed_at);


--
-- Name: option_chain_snapshots option_chain_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_chain_snapshots
    ADD CONSTRAINT option_chain_snapshots_pkey PRIMARY KEY (snapshot_id, first_observed_at);


--
-- Name: option_context_snapshots option_context_snapshots_matrix_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_context_snapshots
    ADD CONSTRAINT option_context_snapshots_matrix_id_key UNIQUE (matrix_id);


--
-- Name: option_context_snapshots option_context_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_context_snapshots
    ADD CONSTRAINT option_context_snapshots_pkey PRIMARY KEY (context_snapshot_id);


--
-- Name: option_contract_catalog option_contract_catalog_contract_ticker_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_contract_catalog
    ADD CONSTRAINT option_contract_catalog_contract_ticker_key UNIQUE (contract_ticker);


--
-- Name: option_contract_catalog option_contract_catalog_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_contract_catalog
    ADD CONSTRAINT option_contract_catalog_pkey PRIMARY KEY (contract_id);


--
-- Name: option_contract_catalog_versions option_contract_catalog_versi_contract_id_provider_payload__key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_contract_catalog_versions
    ADD CONSTRAINT option_contract_catalog_versi_contract_id_provider_payload__key UNIQUE (contract_id, provider, payload_sha256, first_observed_at);


--
-- Name: option_contract_catalog_versions option_contract_catalog_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_contract_catalog_versions
    ADD CONSTRAINT option_contract_catalog_versions_pkey PRIMARY KEY (catalog_version_id);


--
-- Name: option_contract_discoveries option_contract_discoveries_contract_ticker_source_batch_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_contract_discoveries
    ADD CONSTRAINT option_contract_discoveries_contract_ticker_source_batch_id_key UNIQUE (contract_ticker, source_batch_id);


--
-- Name: option_contract_discoveries option_contract_discoveries_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_contract_discoveries
    ADD CONSTRAINT option_contract_discoveries_pkey PRIMARY KEY (discovery_id);


--
-- Name: option_decision_evidence option_decision_evidence_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_decision_evidence
    ADD CONSTRAINT option_decision_evidence_pkey PRIMARY KEY (evidence_id);


--
-- Name: option_expiration_analytics option_expiration_analytics_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_expiration_analytics
    ADD CONSTRAINT option_expiration_analytics_pkey PRIMARY KEY (matrix_id, expiration_date);


--
-- Name: option_flow_windows option_flow_windows_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_flow_windows
    ADD CONSTRAINT option_flow_windows_pkey PRIMARY KEY (flow_window_id);


--
-- Name: option_ingestion_runs option_ingestion_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_ingestion_runs
    ADD CONSTRAINT option_ingestion_runs_pkey PRIMARY KEY (batch_id);


--
-- Name: option_iv_context_snapshots option_iv_context_snapshots_matrix_id_expiration_bucket_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_iv_context_snapshots
    ADD CONSTRAINT option_iv_context_snapshots_matrix_id_expiration_bucket_key UNIQUE (matrix_id, expiration_bucket);


--
-- Name: option_iv_context_snapshots option_iv_context_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_iv_context_snapshots
    ADD CONSTRAINT option_iv_context_snapshots_pkey PRIMARY KEY (iv_context_id);


--
-- Name: option_market_events option_market_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_market_events
    ADD CONSTRAINT option_market_events_pkey PRIMARY KEY (market_event_id);


--
-- Name: option_market_events option_market_events_source_source_key_first_observed_at_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_market_events
    ADD CONSTRAINT option_market_events_source_source_key_first_observed_at_key UNIQUE (source, source_key, first_observed_at);


--
-- Name: option_provider_trade_semantics option_provider_trade_semanti_provider_semantics_version_co_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_provider_trade_semantics
    ADD CONSTRAINT option_provider_trade_semanti_provider_semantics_version_co_key UNIQUE NULLS NOT DISTINCT (provider, semantics_version, condition_code, correction_code, effective_from);


--
-- Name: option_provider_trade_semantics option_provider_trade_semantics_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_provider_trade_semantics
    ADD CONSTRAINT option_provider_trade_semantics_pkey PRIMARY KEY (semantics_id);


--
-- Name: option_raw_batch_pages option_raw_batch_pages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_raw_batch_pages
    ADD CONSTRAINT option_raw_batch_pages_pkey PRIMARY KEY (batch_id, page_number);


--
-- Name: option_raw_file_manifests option_raw_file_manifests_object_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_raw_file_manifests
    ADD CONSTRAINT option_raw_file_manifests_object_key_key UNIQUE (object_key);


--
-- Name: option_raw_file_manifests option_raw_file_manifests_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_raw_file_manifests
    ADD CONSTRAINT option_raw_file_manifests_pkey PRIMARY KEY (file_id);


--
-- Name: option_recommendation_validity_current option_recommendation_validity_current_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_recommendation_validity_current
    ADD CONSTRAINT option_recommendation_validity_current_pkey PRIMARY KEY (signal_id);


--
-- Name: option_recommendation_validity_current option_recommendation_validity_current_validity_event_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_recommendation_validity_current
    ADD CONSTRAINT option_recommendation_validity_current_validity_event_id_key UNIQUE (validity_event_id);


--
-- Name: option_recommendation_validity_events option_recommendation_validity_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_recommendation_validity_events
    ADD CONSTRAINT option_recommendation_validity_events_pkey PRIMARY KEY (validity_event_id);


--
-- Name: option_retention_holds option_retention_holds_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_retention_holds
    ADD CONSTRAINT option_retention_holds_pkey PRIMARY KEY (hold_id);


--
-- Name: option_scenario_results option_scenario_results_candidate_id_scenario_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_scenario_results
    ADD CONSTRAINT option_scenario_results_candidate_id_scenario_key_key UNIQUE (candidate_id, scenario_key);


--
-- Name: option_scenario_results option_scenario_results_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_scenario_results
    ADD CONSTRAINT option_scenario_results_pkey PRIMARY KEY (scenario_result_id);


--
-- Name: option_scenario_results option_scenario_results_snapshot_id_scenario_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_scenario_results
    ADD CONSTRAINT option_scenario_results_snapshot_id_scenario_key_key UNIQUE (snapshot_id, scenario_key);


--
-- Name: option_scheduler_instances option_scheduler_instances_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_scheduler_instances
    ADD CONSTRAINT option_scheduler_instances_pkey PRIMARY KEY (instance_id);


--
-- Name: option_signal_decay_outcomes option_signal_decay_outcomes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_decay_outcomes
    ADD CONSTRAINT option_signal_decay_outcomes_pkey PRIMARY KEY (outcome_id);


--
-- Name: option_signal_events option_signal_events_idempotency_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_events
    ADD CONSTRAINT option_signal_events_idempotency_key_key UNIQUE (idempotency_key);


--
-- Name: option_signal_events option_signal_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_events
    ADD CONSTRAINT option_signal_events_pkey PRIMARY KEY (event_id);


--
-- Name: option_signal_events option_signal_events_source_candidate_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_events
    ADD CONSTRAINT option_signal_events_source_candidate_id_key UNIQUE (source_candidate_id);


--
-- Name: option_signal_legs option_signal_legs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_legs
    ADD CONSTRAINT option_signal_legs_pkey PRIMARY KEY (event_id, leg_index);


--
-- Name: option_signal_occurrences option_signal_occurrences_event_id_market_data_time_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_occurrences
    ADD CONSTRAINT option_signal_occurrences_event_id_market_data_time_key UNIQUE (event_id, market_data_time);


--
-- Name: option_signal_occurrences option_signal_occurrences_source_candidate_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_occurrences
    ADD CONSTRAINT option_signal_occurrences_source_candidate_id_key UNIQUE (source_candidate_id);


--
-- Name: option_signal_occurrences option_signal_occurrences_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_occurrences
    ADD CONSTRAINT option_signal_occurrences_pkey PRIMARY KEY (occurrence_id);


--
-- Name: option_signal_suppressions option_signal_suppressions_candidate_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_suppressions
    ADD CONSTRAINT option_signal_suppressions_candidate_id_key UNIQUE (candidate_id);


--
-- Name: option_signal_suppressions option_signal_suppressions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_suppressions
    ADD CONSTRAINT option_signal_suppressions_pkey PRIMARY KEY (suppression_id);


--
-- Name: option_snapshot_fact_keys option_snapshot_fact_keys_contract_id_provider_market_data__key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_snapshot_fact_keys
    ADD CONSTRAINT option_snapshot_fact_keys_contract_id_provider_market_data__key UNIQUE (contract_id, provider, market_data_time, normalized_payload_sha256);


--
-- Name: option_snapshot_fact_keys option_snapshot_fact_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_snapshot_fact_keys
    ADD CONSTRAINT option_snapshot_fact_keys_pkey PRIMARY KEY (snapshot_id);


--
-- Name: option_strategy_candidates option_strategy_candidates_candidate_identity_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_strategy_candidates
    ADD CONSTRAINT option_strategy_candidates_candidate_identity_key UNIQUE (candidate_identity);


--
-- Name: option_strategy_candidates option_strategy_candidates_matrix_id_strategy_name_strategy_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_strategy_candidates
    ADD CONSTRAINT option_strategy_candidates_matrix_id_strategy_name_strategy_key UNIQUE (matrix_id, strategy_name, strategy_version, candidate_rank);


--
-- Name: option_strategy_candidates option_strategy_candidates_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_strategy_candidates
    ADD CONSTRAINT option_strategy_candidates_pkey PRIMARY KEY (candidate_id);


--
-- Name: option_strategy_registry option_strategy_registry_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_strategy_registry
    ADD CONSTRAINT option_strategy_registry_pkey PRIMARY KEY (strategy_name, strategy_version);


--
-- Name: option_trade_cursors option_trade_cursors_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_trade_cursors
    ADD CONSTRAINT option_trade_cursors_pkey PRIMARY KEY (provider, contract_id);


--
-- Name: option_trade_events option_trade_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_trade_events
    ADD CONSTRAINT option_trade_events_pkey PRIMARY KEY (trade_event_id, sip_timestamp);


--
-- Name: option_trade_events option_trade_events_provider_contract_id_sip_timestamp_sequ_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_trade_events
    ADD CONSTRAINT option_trade_events_provider_contract_id_sip_timestamp_sequ_key UNIQUE NULLS NOT DISTINCT (provider, contract_id, sip_timestamp, sequence_number, participant_timestamp, payload_sha256);


--
-- Name: option_trade_watchlist option_trade_watchlist_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_trade_watchlist
    ADD CONSTRAINT option_trade_watchlist_pkey PRIMARY KEY (contract_id, reason, source_id, active_from);


--
-- Name: option_universe_candidates option_universe_candidates_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_universe_candidates
    ADD CONSTRAINT option_universe_candidates_pkey PRIMARY KEY (run_id, ticker);


--
-- Name: option_universe_members option_universe_members_effective_from_ticker_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_universe_members
    ADD CONSTRAINT option_universe_members_effective_from_ticker_key UNIQUE (effective_from, ticker);


--
-- Name: option_universe_members option_universe_members_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_universe_members
    ADD CONSTRAINT option_universe_members_pkey PRIMARY KEY (member_id);


--
-- Name: option_universe_runs option_universe_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_universe_runs
    ADD CONSTRAINT option_universe_runs_pkey PRIMARY KEY (run_id);


--
-- Name: option_volatility_surfaces option_volatility_surfaces_matrix_id_expiration_date_contra_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_volatility_surfaces
    ADD CONSTRAINT option_volatility_surfaces_matrix_id_expiration_date_contra_key UNIQUE (matrix_id, expiration_date, contract_type, fit_version);


--
-- Name: option_volatility_surfaces option_volatility_surfaces_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_volatility_surfaces
    ADD CONSTRAINT option_volatility_surfaces_pkey PRIMARY KEY (volatility_surface_id);


--
-- Name: option_work_items option_work_items_business_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_work_items
    ADD CONSTRAINT option_work_items_business_key_key UNIQUE (business_key);


--
-- Name: option_work_items option_work_items_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_work_items
    ADD CONSTRAINT option_work_items_pkey PRIMARY KEY (work_id);


--
-- Name: pattern_analog_matches pattern_analog_matches_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pattern_analog_matches
    ADD CONSTRAINT pattern_analog_matches_pkey PRIMARY KEY (analog_id);


--
-- Name: pattern_win_rate_priors pattern_win_rate_priors_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pattern_win_rate_priors
    ADD CONSTRAINT pattern_win_rate_priors_pkey PRIMARY KEY (prior_id);


--
-- Name: recommendation_performance_log recommendation_performance_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.recommendation_performance_log
    ADD CONSTRAINT recommendation_performance_log_pkey PRIMARY KEY (perf_id);


--
-- Name: recommendation_performance_log recommendation_performance_log_report_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.recommendation_performance_log
    ADD CONSTRAINT recommendation_performance_log_report_date_key UNIQUE (report_date);


--
-- Name: research_runs research_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.research_runs
    ADD CONSTRAINT research_runs_pkey PRIMARY KEY (run_id);


--
-- Name: sector_regime_daily sector_regime_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sector_regime_daily
    ADD CONSTRAINT sector_regime_daily_pkey PRIMARY KEY (sector_regime_id);


--
-- Name: selected_tickers selected_tickers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.selected_tickers
    ADD CONSTRAINT selected_tickers_pkey PRIMARY KEY (id);


--
-- Name: selected_tickers selected_tickers_ticker_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.selected_tickers
    ADD CONSTRAINT selected_tickers_ticker_key UNIQUE (ticker);


--
-- Name: equity_bar_revisions uq_equity_bar_revision_identity; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_bar_revisions
    ADD CONSTRAINT uq_equity_bar_revision_identity UNIQUE (ticker, "interval", bar_start, session_scope, adjusted, source_kind, availability_mode, payload_sha256);


--
-- Name: option_signal_decay_outcomes uq_option_decay_candidate_measurement_policy; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_decay_outcomes
    ADD CONSTRAINT uq_option_decay_candidate_measurement_policy UNIQUE (candidate_id, measurement_type, valuation_policy_sha256);


--
-- Name: cross_sectional_signals uq_xs_signal; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cross_sectional_signals
    ADD CONSTRAINT uq_xs_signal UNIQUE (trade_date, ticker, model_version);


--
-- Name: idx_analogs_date_ticker; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_analogs_date_ticker ON public.pattern_analog_matches USING btree (current_trade_date, current_ticker);


--
-- Name: idx_daily_rec_accuracy; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_daily_rec_accuracy ON public.daily_recommendations USING btree (recommendation_correct, trade_date DESC);


--
-- Name: idx_daily_rec_analog_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_daily_rec_analog_id ON public.daily_recommendations USING btree (analog_id);


--
-- Name: idx_daily_rec_date_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_daily_rec_date_type ON public.daily_recommendations USING btree (trade_date, recommendation_type);


--
-- Name: idx_daily_rec_score_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_daily_rec_score_id ON public.daily_recommendations USING btree (score_id);


--
-- Name: idx_daily_rec_sector; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_daily_rec_sector ON public.daily_recommendations USING btree (sector, trade_date);


--
-- Name: idx_daily_rec_ticker_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_daily_rec_ticker_date ON public.daily_recommendations USING btree (ticker, trade_date);


--
-- Name: idx_discovery_date_state; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_discovery_date_state ON public.market_discovery_states USING btree (trade_date DESC, state);


--
-- Name: idx_discovery_ticker_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_discovery_ticker_date ON public.market_discovery_states USING btree (ticker, trade_date DESC);


--
-- Name: idx_equity_analysis_members_claim; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_analysis_members_claim ON public.equity_analysis_members USING btree (status, lease_expires_at, analysis_run_id) WHERE ((status)::text = ANY ((ARRAY['PENDING'::character varying, 'CLAIMED'::character varying])::text[]));


--
-- Name: idx_equity_analysis_runs_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_analysis_runs_lookup ON public.equity_analysis_runs USING btree ("interval", market_time DESC, observed_at DESC);


--
-- Name: idx_equity_bar_publication_members_revision; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_bar_publication_members_revision ON public.equity_bar_publication_members USING btree (selected_bar_revision_id) WHERE (selected_bar_revision_id IS NOT NULL);


--
-- Name: idx_equity_bar_publications_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_bar_publications_lookup ON public.equity_bar_publications USING btree ("interval", session_scope, adjusted, market_time DESC, observed_at DESC);


--
-- Name: idx_equity_bar_revisions_asof; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_bar_revisions_asof ON public.equity_bar_revisions USING btree (ticker, "interval", bar_end DESC, system_observed_at DESC);


--
-- Name: idx_equity_bar_revisions_logical_identity; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_bar_revisions_logical_identity ON public.equity_bar_revisions USING btree (ticker, "interval", session_scope, adjusted, bar_start DESC, system_observed_at DESC) WHERE (is_final = true);


--
-- Name: idx_equity_bar_revisions_replay; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_bar_revisions_replay ON public.equity_bar_revisions USING btree (availability_mode, "interval", bar_end, replay_available_at) WHERE ((availability_mode)::text = 'HISTORICAL_RECONSTRUCTED'::text);


--
-- Name: idx_equity_bar_revisions_session; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_bar_revisions_session ON public.equity_bar_revisions USING btree ("interval", session_date, ticker);


--
-- Name: idx_equity_context_evidence_reverse; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_context_evidence_reverse ON public.equity_context_evidence USING btree (evidence_id, equity_context_snapshot_id);


--
-- Name: idx_equity_context_snapshots_asof; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_context_snapshots_asof ON public.equity_context_snapshots USING btree (ticker, strategy_horizon, market_time DESC, observed_at DESC);


--
-- Name: idx_equity_corporate_actions_asof; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_corporate_actions_asof ON public.equity_corporate_actions USING btree (security_id, effective_date DESC, first_observed_at DESC);


--
-- Name: idx_equity_corporate_actions_replay; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_corporate_actions_replay ON public.equity_corporate_actions USING btree (ticker, effective_date, availability_mode, COALESCE(replay_available_at, first_observed_at));


--
-- Name: idx_equity_current_bar_projection_history; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_current_bar_projection_history ON public.equity_current_bar_projection USING btree (ticker, "interval", session_scope, adjusted, bar_start DESC);


--
-- Name: idx_equity_current_projection_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_current_projection_type ON public.equity_current_projection USING btree (projection_type, interval_key, published_at DESC);


--
-- Name: idx_equity_evidence_asof; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_evidence_asof ON public.equity_evidence USING btree (ticker, "interval", evidence_type, market_time DESC, observed_at DESC);


--
-- Name: idx_equity_evidence_lifecycle; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_evidence_lifecycle ON public.equity_evidence USING btree (lifecycle_key, market_time DESC) WHERE (lifecycle_key IS NOT NULL);


--
-- Name: idx_equity_evidence_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_evidence_source ON public.equity_evidence USING btree (source_name, source_version, market_time DESC);


--
-- Name: idx_equity_fundamental_reports_accession; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_fundamental_reports_accession ON public.equity_fundamental_reports USING btree (accession_number) WHERE (accession_number IS NOT NULL);


--
-- Name: idx_equity_fundamental_reports_asof; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_fundamental_reports_asof ON public.equity_fundamental_reports USING btree (security_id, availability_time DESC, observed_at DESC);


--
-- Name: idx_equity_fundamental_reports_period; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_fundamental_reports_period ON public.equity_fundamental_reports USING btree (security_id, period_end DESC, timeframe);


--
-- Name: idx_equity_historical_signal_evidence; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_historical_signal_evidence ON public.equity_evidence USING btree (source_name, source_version, observed_at, direction) WHERE ((analysis_run_id IS NULL) AND (quality_codes @> ARRAY['HISTORICAL_RECONSTRUCTED'::text]));


--
-- Name: idx_equity_ingestion_segments_dataset; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_ingestion_segments_dataset ON public.equity_ingestion_segments USING btree (dataset, "interval", market_watermark DESC);


--
-- Name: idx_equity_outcomes_benchmarks; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_outcomes_benchmarks ON public.equity_research_outcomes USING btree (market_benchmark_ticker, sector_benchmark_ticker, outcome_available_at DESC);


--
-- Name: idx_equity_portal_snapshots_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_portal_snapshots_lookup ON public.equity_portal_snapshots USING btree (snapshot_type, generated_at DESC);


--
-- Name: idx_equity_qualification_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_qualification_lookup ON public.equity_qualification_revisions USING btree (source_name, source_version, "interval", direction, horizon_key, effective_from DESC);


--
-- Name: idx_equity_qualification_scope_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_qualification_scope_lookup ON public.equity_qualification_revisions USING btree (((metrics ->> 'research_scope'::text)), source_name, source_version, "interval", direction, horizon_key, effective_from DESC);


--
-- Name: idx_equity_research_outcomes_policy; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_research_outcomes_policy ON public.equity_research_outcomes USING btree (outcome_policy_id, outcome_available_at DESC);


--
-- Name: idx_equity_research_outcomes_subject; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_research_outcomes_subject ON public.equity_research_outcomes USING btree (subject_evidence_id, horizon_key, outcome_revision DESC);


--
-- Name: idx_equity_research_outcomes_supersedes; Type: INDEX; Schema: public; Owner: -
--

-- Supports the self-referential supersedes_outcome_id foreign key; without it bulk deletes scan the table per row.
CREATE INDEX idx_equity_research_outcomes_supersedes ON public.equity_research_outcomes USING btree (supersedes_outcome_id) WHERE (supersedes_outcome_id IS NOT NULL);


--
-- Name: idx_equity_evidence_supersedes; Type: INDEX; Schema: public; Owner: -
--

-- The next four indexes back foreign keys that reference equity_evidence; deletes there are unusable without them.
CREATE INDEX idx_equity_evidence_supersedes ON public.equity_evidence USING btree (supersedes_evidence_id) WHERE (supersedes_evidence_id IS NOT NULL);


--
-- Name: idx_equity_context_snapshots_direction_evidence; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_context_snapshots_direction_evidence ON public.equity_context_snapshots USING btree (direction_evidence_id) WHERE (direction_evidence_id IS NOT NULL);


--
-- Name: idx_equity_context_snapshots_range_forecast; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_context_snapshots_range_forecast ON public.equity_context_snapshots USING btree (range_forecast_id) WHERE (range_forecast_id IS NOT NULL);


--
-- Name: idx_equity_context_snapshots_fundamental_snapshot; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_context_snapshots_fundamental_snapshot ON public.equity_context_snapshots USING btree (fundamental_snapshot_id) WHERE (fundamental_snapshot_id IS NOT NULL);


--
-- Name: idx_equity_current_projection_evidence; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_current_projection_evidence ON public.equity_current_projection USING btree (evidence_id);


--
-- Name: idx_equity_security_reference_cik; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_security_reference_cik ON public.equity_security_reference_revisions USING btree (cik, effective_from DESC) WHERE (cik IS NOT NULL);


--
-- Name: idx_equity_security_reference_security; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_security_reference_security ON public.equity_security_reference_revisions USING btree (security_id, effective_from DESC, observed_at DESC);


--
-- Name: idx_equity_security_reference_ticker; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_security_reference_ticker ON public.equity_security_reference_revisions USING btree (ticker, effective_from DESC, observed_at DESC);


--
-- Name: idx_equity_universe_members_ticker; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_universe_members_ticker ON public.equity_universe_members USING btree (ticker, effective_from DESC, first_observed_at DESC);


--
-- Name: idx_equity_universe_runs_replay; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_equity_universe_runs_replay ON public.equity_universe_runs USING btree (availability_mode, effective_from DESC, COALESCE(replay_available_at, observed_at) DESC) WHERE ((status)::text = ANY ((ARRAY['COMPLETE'::character varying, 'DEGRADED'::character varying])::text[]));


--
-- Name: idx_ingestion_failures_unresolved; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ingestion_failures_unresolved ON public.data_ingestion_failures USING btree (dataset, trade_date DESC) WHERE (resolved_at IS NULL);


--
-- Name: idx_market_context_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_market_context_date ON public.market_context_daily USING btree (trade_date DESC);


--
-- Name: idx_option_analysis_runs_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_analysis_runs_context ON public.option_analysis_runs USING btree (underlying, market_time DESC, observed_time DESC);


--
-- Name: idx_option_candidate_legs_contract; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_candidate_legs_contract ON public.option_candidate_legs USING btree (contract_id, source_market_time DESC);


--
-- Name: idx_option_catalog_versions_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_catalog_versions_context ON public.option_contract_catalog_versions USING btree (contract_id, valid_from DESC, first_observed_at DESC, revised_observed_at DESC);


--
-- Name: idx_option_catalog_versions_expiration; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_catalog_versions_expiration ON public.option_contract_catalog_versions USING btree (expiration_date, eligibility_status);


--
-- Name: idx_option_chain_snapshots_batch; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_chain_snapshots_batch ON ONLY public.option_chain_snapshots USING btree (batch_id, contract_id);


--
-- Name: idx_option_chain_snapshots_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_chain_snapshots_context ON ONLY public.option_chain_snapshots USING btree (underlying, market_data_time DESC, first_observed_at DESC, revised_observed_at DESC);


--
-- Name: idx_option_context_equity_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_context_equity_context ON public.option_context_snapshots USING btree (equity_context_snapshot_id) WHERE (equity_context_snapshot_id IS NOT NULL);


--
-- Name: idx_option_context_snapshots_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_context_snapshots_lookup ON public.option_context_snapshots USING btree (underlying, market_data_time DESC, observed_time DESC);


--
-- Name: idx_option_contract_catalog_underlying; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_contract_catalog_underlying ON public.option_contract_catalog USING btree (underlying, expired_at, contract_ticker);


--
-- Name: idx_option_decay_policy; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_decay_policy ON public.option_signal_decay_outcomes USING btree (valuation_policy_sha256, measurement_type, market_time DESC);


--
-- Name: idx_option_discoveries_pending; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_discoveries_pending ON public.option_contract_discoveries USING btree (state, first_observed_at) WHERE ((state)::text = ANY ((ARRAY['UNKNOWN_REFERENCE'::character varying, 'REFERENCE_PENDING'::character varying, 'REFERENCE_UNAVAILABLE'::character varying])::text[]));


--
-- Name: idx_option_flow_windows_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_flow_windows_lookup ON public.option_flow_windows USING btree (underlying, window_end DESC, detector_version);


--
-- Name: idx_option_ingestion_runs_cycle; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_ingestion_runs_cycle ON public.option_ingestion_runs USING btree (scheduled_cycle DESC, underlying);


--
-- Name: idx_option_ingestion_runs_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_ingestion_runs_status ON public.option_ingestion_runs USING btree (status, started_at);


--
-- Name: idx_option_market_events_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_market_events_context ON public.option_market_events USING btree (event_type, affected_underlying, scheduled_time, first_observed_at);


--
-- Name: idx_option_raw_manifests_partition; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_raw_manifests_partition ON public.option_raw_file_manifests USING btree (event_type, market_date, underlying, market_hour);


--
-- Name: idx_option_retention_holds_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_retention_holds_active ON public.option_retention_holds USING btree (expires_at, hold_id) WHERE (released_at IS NULL);


--
-- Name: idx_option_scheduler_instances_heartbeat; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_scheduler_instances_heartbeat ON public.option_scheduler_instances USING btree (status, last_heartbeat_at DESC);


--
-- Name: idx_option_signal_events_search; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_signal_events_search ON public.option_signal_events USING btree (underlying, market_data_time DESC, status, strategy_name);


--
-- Name: idx_option_signal_events_identity; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_signal_events_identity ON public.option_signal_events USING btree (signal_identity_key, market_data_time DESC);


--
-- Name: idx_option_strategy_candidates_list; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_strategy_candidates_list ON public.option_strategy_candidates USING btree (status, strategy_archetype, underlying, expiration_date, candidate_rank);


--
-- Name: idx_option_strategy_candidates_matrix; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_strategy_candidates_matrix ON public.option_strategy_candidates USING btree (matrix_id, strategy_name, candidate_rank);


--
-- Name: idx_option_trade_events_contract_cursor; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_trade_events_contract_cursor ON ONLY public.option_trade_events USING btree (contract_id, sip_timestamp DESC, sequence_number DESC);


--
-- Name: idx_option_trade_events_observed; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_trade_events_observed ON ONLY public.option_trade_events USING btree (first_observed_at DESC);


--
-- Name: idx_option_trade_watchlist_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_trade_watchlist_active ON public.option_trade_watchlist USING btree (contract_id, active_from, active_to);


--
-- Name: idx_option_universe_members_context; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_universe_members_context ON public.option_universe_members USING btree (ticker, effective_from DESC, first_observed_at DESC);


--
-- Name: idx_option_validity_events_signal; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_validity_events_signal ON public.option_recommendation_validity_events USING btree (signal_id, evaluated_at DESC);


--
-- Name: idx_option_work_items_claim; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_work_items_claim ON public.option_work_items USING btree (stage, status, next_attempt_at, created_at) WHERE ((status)::text = ANY ((ARRAY['PENDING'::character varying, 'RETRY'::character varying])::text[]));


--
-- Name: idx_option_work_items_expired_lease; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_option_work_items_expired_lease ON public.option_work_items USING btree (lease_expires_at) WHERE ((status)::text = 'CLAIMED'::text);


--
-- Name: idx_pattern_scores_date_ticker; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pattern_scores_date_ticker ON public.opening_pattern_scores USING btree (trade_date, ticker);


--
-- Name: idx_pattern_scores_sector; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_pattern_scores_sector ON public.opening_pattern_scores USING btree (sector, trade_date DESC);


--
-- Name: idx_perf_log_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_perf_log_date ON public.recommendation_performance_log USING btree (report_date DESC);


--
-- Name: idx_priors_date_pattern; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_priors_date_pattern ON public.pattern_win_rate_priors USING btree (effective_date DESC, pattern_name);


--
-- Name: idx_research_runs_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_research_runs_at ON public.research_runs USING btree (run_at DESC);


--
-- Name: idx_sector_regime_date_sector; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sector_regime_date_sector ON public.sector_regime_daily USING btree (trade_date DESC, sector);


--
-- Name: idx_selected_tickers_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_selected_tickers_active ON public.selected_tickers USING btree (is_active, ticker);


--
-- Name: idx_selected_tickers_cap_group; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_selected_tickers_cap_group ON public.selected_tickers USING btree (market_cap_group);


--
-- Name: idx_selected_tickers_sector; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_selected_tickers_sector ON public.selected_tickers USING btree (sector);


--
-- Name: idx_xs_signals_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_xs_signals_date ON public.cross_sectional_signals USING btree (trade_date DESC);


--
-- Name: idx_xs_signals_date_side; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_xs_signals_date_side ON public.cross_sectional_signals USING btree (trade_date DESC, side);


--
-- Name: idx_xs_signals_ticker; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_xs_signals_ticker ON public.cross_sectional_signals USING btree (ticker, trade_date DESC);


--
-- Name: uq_option_ingestion_slot_cohort; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_option_ingestion_slot_cohort ON public.option_ingestion_runs USING btree (provider, underlying, scheduled_cycle, request_filter_sha256, policy_sha256, configuration_sha256);


--
-- Name: cross_sectional_signals trg_cross_sectional_equity_portal_source; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_cross_sectional_equity_portal_source AFTER INSERT OR DELETE OR UPDATE OR TRUNCATE ON public.cross_sectional_signals FOR EACH STATEMENT EXECUTE FUNCTION public.mark_equity_portal_source_changed();


--
-- Name: market_discovery_states trg_discovery_states_equity_portal_source; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_discovery_states_equity_portal_source AFTER INSERT OR DELETE OR UPDATE OR TRUNCATE ON public.market_discovery_states FOR EACH STATEMENT EXECUTE FUNCTION public.mark_equity_portal_source_changed();


--
-- Name: equity_bar_publications trg_equity_bar_publications_portal_source; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_equity_bar_publications_portal_source AFTER INSERT ON public.equity_bar_publications FOR EACH ROW WHEN ((((new."interval")::text = ANY ((ARRAY['1d'::character varying, '1wk'::character varying])::text[])) AND ((new.status)::text = ANY ((ARRAY['COMPLETE'::character varying, 'DEGRADED'::character varying])::text[])))) EXECUTE FUNCTION public.mark_equity_portal_source_changed();


--
-- Name: equity_bar_revisions trg_equity_bar_revisions_append_only; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_equity_bar_revisions_append_only BEFORE DELETE OR UPDATE OR TRUNCATE ON public.equity_bar_revisions FOR EACH STATEMENT EXECUTE FUNCTION public.reject_equity_bar_revision_mutation();


--
-- Name: option_contract_discoveries trg_option_new_series_transition; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_option_new_series_transition BEFORE UPDATE OF state ON public.option_contract_discoveries FOR EACH ROW EXECUTE FUNCTION public.enforce_option_new_series_transition();


--
-- Name: option_signal_events trg_option_signal_leg_completeness; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER trg_option_signal_leg_completeness AFTER INSERT OR UPDATE OF status, expected_leg_count ON public.option_signal_events DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.enforce_option_signal_leg_completeness();


--
-- Name: selected_tickers trg_selected_tickers_equity_portal_source; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_selected_tickers_equity_portal_source AFTER INSERT OR DELETE OR UPDATE OR TRUNCATE ON public.selected_tickers FOR EACH STATEMENT EXECUTE FUNCTION public.mark_equity_portal_source_changed();


--
-- Name: equity_current_bar_projection trg_validate_equity_current_bar_projection; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_validate_equity_current_bar_projection BEFORE INSERT OR UPDATE ON public.equity_current_bar_projection FOR EACH ROW EXECUTE FUNCTION public.validate_equity_current_bar_projection();


--
-- Name: daily_recommendations daily_recommendations_analog_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.daily_recommendations
    ADD CONSTRAINT daily_recommendations_analog_id_fkey FOREIGN KEY (analog_id) REFERENCES public.pattern_analog_matches(analog_id);


--
-- Name: daily_recommendations daily_recommendations_score_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.daily_recommendations
    ADD CONSTRAINT daily_recommendations_score_id_fkey FOREIGN KEY (score_id) REFERENCES public.opening_pattern_scores(score_id);


--
-- Name: equity_analysis_members equity_analysis_members_analysis_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_analysis_members
    ADD CONSTRAINT equity_analysis_members_analysis_run_id_fkey FOREIGN KEY (analysis_run_id) REFERENCES public.equity_analysis_runs(analysis_run_id) ON DELETE CASCADE;


--
-- Name: equity_analysis_members equity_analysis_members_latest_bar_revision_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_analysis_members
    ADD CONSTRAINT equity_analysis_members_latest_bar_revision_id_fkey FOREIGN KEY (latest_bar_revision_id) REFERENCES public.equity_bar_revisions(bar_revision_id);


--
-- Name: equity_analysis_runs equity_analysis_runs_universe_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_analysis_runs
    ADD CONSTRAINT equity_analysis_runs_universe_run_id_fkey FOREIGN KEY (universe_run_id) REFERENCES public.equity_universe_runs(universe_run_id);


--
-- Name: equity_bar_publication_members equity_bar_publication_members_publication_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_bar_publication_members
    ADD CONSTRAINT equity_bar_publication_members_publication_id_fkey FOREIGN KEY (publication_id) REFERENCES public.equity_bar_publications(publication_id) ON DELETE CASCADE;


--
-- Name: equity_bar_publication_members equity_bar_publication_members_selected_bar_revision_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_bar_publication_members
    ADD CONSTRAINT equity_bar_publication_members_selected_bar_revision_id_fkey FOREIGN KEY (selected_bar_revision_id) REFERENCES public.equity_bar_revisions(bar_revision_id);


--
-- Name: equity_bar_revisions equity_bar_revisions_ingestion_segment_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_bar_revisions
    ADD CONSTRAINT equity_bar_revisions_ingestion_segment_id_fkey FOREIGN KEY (ingestion_segment_id) REFERENCES public.equity_ingestion_segments(ingestion_segment_id);


--
-- Name: equity_bar_revisions equity_bar_revisions_supersedes_bar_revision_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_bar_revisions
    ADD CONSTRAINT equity_bar_revisions_supersedes_bar_revision_id_fkey FOREIGN KEY (supersedes_bar_revision_id) REFERENCES public.equity_bar_revisions(bar_revision_id);


--
-- Name: equity_context_evidence equity_context_evidence_equity_context_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_context_evidence
    ADD CONSTRAINT equity_context_evidence_equity_context_snapshot_id_fkey FOREIGN KEY (equity_context_snapshot_id) REFERENCES public.equity_context_snapshots(equity_context_snapshot_id) ON DELETE CASCADE;


--
-- Name: equity_context_evidence equity_context_evidence_evidence_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_context_evidence
    ADD CONSTRAINT equity_context_evidence_evidence_id_fkey FOREIGN KEY (evidence_id) REFERENCES public.equity_evidence(evidence_id);


--
-- Name: equity_context_snapshots equity_context_snapshots_direction_evidence_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_context_snapshots
    ADD CONSTRAINT equity_context_snapshots_direction_evidence_id_fkey FOREIGN KEY (direction_evidence_id) REFERENCES public.equity_evidence(evidence_id);


--
-- Name: equity_context_snapshots equity_context_snapshots_direction_qualification_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_context_snapshots
    ADD CONSTRAINT equity_context_snapshots_direction_qualification_id_fkey FOREIGN KEY (direction_qualification_id) REFERENCES public.equity_qualification_revisions(qualification_revision_id);


--
-- Name: equity_context_snapshots equity_context_snapshots_fundamental_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_context_snapshots
    ADD CONSTRAINT equity_context_snapshots_fundamental_snapshot_id_fkey FOREIGN KEY (fundamental_snapshot_id) REFERENCES public.equity_evidence(evidence_id);


--
-- Name: equity_context_snapshots equity_context_snapshots_range_forecast_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_context_snapshots
    ADD CONSTRAINT equity_context_snapshots_range_forecast_id_fkey FOREIGN KEY (range_forecast_id) REFERENCES public.equity_evidence(evidence_id);


--
-- Name: equity_context_snapshots equity_context_snapshots_security_revision_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_context_snapshots
    ADD CONSTRAINT equity_context_snapshots_security_revision_id_fkey FOREIGN KEY (security_revision_id) REFERENCES public.equity_security_reference_revisions(security_revision_id);


--
-- Name: equity_context_snapshots equity_context_snapshots_universe_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_context_snapshots
    ADD CONSTRAINT equity_context_snapshots_universe_run_id_fkey FOREIGN KEY (universe_run_id) REFERENCES public.equity_universe_runs(universe_run_id);


--
-- Name: equity_current_bar_projection equity_current_bar_projection_publication_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_current_bar_projection
    ADD CONSTRAINT equity_current_bar_projection_publication_id_fkey FOREIGN KEY (publication_id) REFERENCES public.equity_bar_publications(publication_id);


--
-- Name: equity_current_bar_projection equity_current_bar_projection_selected_bar_revision_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_current_bar_projection
    ADD CONSTRAINT equity_current_bar_projection_selected_bar_revision_id_fkey FOREIGN KEY (selected_bar_revision_id) REFERENCES public.equity_bar_revisions(bar_revision_id);


--
-- Name: equity_current_projection equity_current_projection_analysis_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_current_projection
    ADD CONSTRAINT equity_current_projection_analysis_run_id_fkey FOREIGN KEY (analysis_run_id) REFERENCES public.equity_analysis_runs(analysis_run_id);


--
-- Name: equity_current_projection equity_current_projection_equity_context_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_current_projection
    ADD CONSTRAINT equity_current_projection_equity_context_snapshot_id_fkey FOREIGN KEY (equity_context_snapshot_id) REFERENCES public.equity_context_snapshots(equity_context_snapshot_id);


--
-- Name: equity_current_projection equity_current_projection_evidence_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_current_projection
    ADD CONSTRAINT equity_current_projection_evidence_id_fkey FOREIGN KEY (evidence_id) REFERENCES public.equity_evidence(evidence_id);


--
-- Name: equity_evidence equity_evidence_analysis_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_evidence
    ADD CONSTRAINT equity_evidence_analysis_run_id_fkey FOREIGN KEY (analysis_run_id) REFERENCES public.equity_analysis_runs(analysis_run_id);


--
-- Name: equity_evidence equity_evidence_latest_bar_revision_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_evidence
    ADD CONSTRAINT equity_evidence_latest_bar_revision_id_fkey FOREIGN KEY (latest_bar_revision_id) REFERENCES public.equity_bar_revisions(bar_revision_id);


--
-- Name: equity_evidence equity_evidence_qualification_revision_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_evidence
    ADD CONSTRAINT equity_evidence_qualification_revision_id_fkey FOREIGN KEY (qualification_revision_id) REFERENCES public.equity_qualification_revisions(qualification_revision_id);


--
-- Name: equity_evidence equity_evidence_security_revision_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_evidence
    ADD CONSTRAINT equity_evidence_security_revision_id_fkey FOREIGN KEY (security_revision_id) REFERENCES public.equity_security_reference_revisions(security_revision_id);


--
-- Name: equity_evidence equity_evidence_supersedes_evidence_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_evidence
    ADD CONSTRAINT equity_evidence_supersedes_evidence_id_fkey FOREIGN KEY (supersedes_evidence_id) REFERENCES public.equity_evidence(evidence_id);


--
-- Name: equity_fundamental_reports equity_fundamental_reports_security_revision_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_fundamental_reports
    ADD CONSTRAINT equity_fundamental_reports_security_revision_id_fkey FOREIGN KEY (security_revision_id) REFERENCES public.equity_security_reference_revisions(security_revision_id);


--
-- Name: equity_fundamental_reports equity_fundamental_reports_supersedes_report_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_fundamental_reports
    ADD CONSTRAINT equity_fundamental_reports_supersedes_report_id_fkey FOREIGN KEY (supersedes_report_id) REFERENCES public.equity_fundamental_reports(fundamental_report_id);


--
-- Name: equity_portal_current_projections equity_portal_current_projections_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_portal_current_projections
    ADD CONSTRAINT equity_portal_current_projections_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES public.equity_portal_snapshots(snapshot_id);


--
-- Name: equity_research_outcomes equity_research_outcomes_confirmation_bar_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_research_outcomes
    ADD CONSTRAINT equity_research_outcomes_confirmation_bar_id_fkey FOREIGN KEY (confirmation_bar_id) REFERENCES public.equity_bar_revisions(bar_revision_id);


--
-- Name: equity_research_outcomes equity_research_outcomes_entry_bar_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_research_outcomes
    ADD CONSTRAINT equity_research_outcomes_entry_bar_id_fkey FOREIGN KEY (entry_bar_id) REFERENCES public.equity_bar_revisions(bar_revision_id);


--
-- Name: equity_research_outcomes equity_research_outcomes_exit_bar_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_research_outcomes
    ADD CONSTRAINT equity_research_outcomes_exit_bar_id_fkey FOREIGN KEY (exit_bar_id) REFERENCES public.equity_bar_revisions(bar_revision_id);


--
-- Name: equity_research_outcomes equity_research_outcomes_outcome_policy_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_research_outcomes
    ADD CONSTRAINT equity_research_outcomes_outcome_policy_id_fkey FOREIGN KEY (outcome_policy_id) REFERENCES public.equity_outcome_policies(outcome_policy_id);


--
-- Name: equity_research_outcomes equity_research_outcomes_subject_evidence_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_research_outcomes
    ADD CONSTRAINT equity_research_outcomes_subject_evidence_id_fkey FOREIGN KEY (subject_evidence_id) REFERENCES public.equity_evidence(evidence_id);


--
-- Name: equity_research_outcomes equity_research_outcomes_supersedes_outcome_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_research_outcomes
    ADD CONSTRAINT equity_research_outcomes_supersedes_outcome_id_fkey FOREIGN KEY (supersedes_outcome_id) REFERENCES public.equity_research_outcomes(outcome_id);


--
-- Name: equity_security_reference_revisions equity_security_reference_revisions_supersedes_revision_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_security_reference_revisions
    ADD CONSTRAINT equity_security_reference_revisions_supersedes_revision_id_fkey FOREIGN KEY (supersedes_revision_id) REFERENCES public.equity_security_reference_revisions(security_revision_id);


--
-- Name: equity_universe_members equity_universe_members_security_revision_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_universe_members
    ADD CONSTRAINT equity_universe_members_security_revision_id_fkey FOREIGN KEY (security_revision_id) REFERENCES public.equity_security_reference_revisions(security_revision_id);


--
-- Name: equity_universe_members equity_universe_members_universe_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.equity_universe_members
    ADD CONSTRAINT equity_universe_members_universe_run_id_fkey FOREIGN KEY (universe_run_id) REFERENCES public.equity_universe_runs(universe_run_id) ON DELETE CASCADE;


--
-- Name: option_contract_discoveries fk_option_discovery_raw_page; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_contract_discoveries
    ADD CONSTRAINT fk_option_discovery_raw_page FOREIGN KEY (source_batch_id, source_page_number) REFERENCES public.option_raw_batch_pages(batch_id, page_number);


--
-- Name: option_analysis_runs option_analysis_runs_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_analysis_runs
    ADD CONSTRAINT option_analysis_runs_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES public.option_ingestion_runs(batch_id);


--
-- Name: option_candidate_legs option_candidate_legs_candidate_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_candidate_legs
    ADD CONSTRAINT option_candidate_legs_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES public.option_strategy_candidates(candidate_id);


--
-- Name: option_candidate_legs option_candidate_legs_contract_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_candidate_legs
    ADD CONSTRAINT option_candidate_legs_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES public.option_contract_catalog(contract_id);


--
-- Name: option_candidate_legs option_candidate_legs_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_candidate_legs
    ADD CONSTRAINT option_candidate_legs_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES public.option_snapshot_fact_keys(snapshot_id);


--
-- Name: option_chain_snapshots option_chain_snapshots_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE public.option_chain_snapshots
    ADD CONSTRAINT option_chain_snapshots_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES public.option_ingestion_runs(batch_id);


--
-- Name: option_chain_snapshots option_chain_snapshots_contract_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE public.option_chain_snapshots
    ADD CONSTRAINT option_chain_snapshots_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES public.option_contract_catalog(contract_id);


--
-- Name: option_chain_snapshots option_chain_snapshots_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE public.option_chain_snapshots
    ADD CONSTRAINT option_chain_snapshots_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES public.option_snapshot_fact_keys(snapshot_id);


--
-- Name: option_context_snapshots option_context_snapshots_equity_context_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_context_snapshots
    ADD CONSTRAINT option_context_snapshots_equity_context_snapshot_id_fkey FOREIGN KEY (equity_context_snapshot_id) REFERENCES public.equity_context_snapshots(equity_context_snapshot_id);


--
-- Name: option_context_snapshots option_context_snapshots_matrix_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_context_snapshots
    ADD CONSTRAINT option_context_snapshots_matrix_id_fkey FOREIGN KEY (matrix_id) REFERENCES public.option_analysis_runs(matrix_id);


--
-- Name: option_contract_catalog_versions option_contract_catalog_versions_contract_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_contract_catalog_versions
    ADD CONSTRAINT option_contract_catalog_versions_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES public.option_contract_catalog(contract_id);


--
-- Name: option_contract_discoveries option_contract_discoveries_contract_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_contract_discoveries
    ADD CONSTRAINT option_contract_discoveries_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES public.option_contract_catalog(contract_id);


--
-- Name: option_decision_evidence option_decision_evidence_context_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_decision_evidence
    ADD CONSTRAINT option_decision_evidence_context_snapshot_id_fkey FOREIGN KEY (context_snapshot_id) REFERENCES public.option_context_snapshots(context_snapshot_id);


--
-- Name: option_decision_evidence option_decision_evidence_matrix_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_decision_evidence
    ADD CONSTRAINT option_decision_evidence_matrix_id_fkey FOREIGN KEY (matrix_id) REFERENCES public.option_analysis_runs(matrix_id);


--
-- Name: option_expiration_analytics option_expiration_analytics_matrix_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_expiration_analytics
    ADD CONSTRAINT option_expiration_analytics_matrix_id_fkey FOREIGN KEY (matrix_id) REFERENCES public.option_analysis_runs(matrix_id);


--
-- Name: option_flow_windows option_flow_windows_matrix_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_flow_windows
    ADD CONSTRAINT option_flow_windows_matrix_id_fkey FOREIGN KEY (matrix_id) REFERENCES public.option_analysis_runs(matrix_id);


--
-- Name: option_iv_context_snapshots option_iv_context_snapshots_matrix_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_iv_context_snapshots
    ADD CONSTRAINT option_iv_context_snapshots_matrix_id_fkey FOREIGN KEY (matrix_id) REFERENCES public.option_analysis_runs(matrix_id);


--
-- Name: option_raw_batch_pages option_raw_batch_pages_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_raw_batch_pages
    ADD CONSTRAINT option_raw_batch_pages_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES public.option_ingestion_runs(batch_id);


--
-- Name: option_recommendation_validity_current option_recommendation_validity_current_candidate_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_recommendation_validity_current
    ADD CONSTRAINT option_recommendation_validity_current_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES public.option_strategy_candidates(candidate_id);


--
-- Name: option_recommendation_validity_current option_recommendation_validity_current_signal_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_recommendation_validity_current
    ADD CONSTRAINT option_recommendation_validity_current_signal_id_fkey FOREIGN KEY (signal_id) REFERENCES public.option_signal_events(event_id);


--
-- Name: option_recommendation_validity_current option_recommendation_validity_current_validity_event_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_recommendation_validity_current
    ADD CONSTRAINT option_recommendation_validity_current_validity_event_id_fkey FOREIGN KEY (validity_event_id) REFERENCES public.option_recommendation_validity_events(validity_event_id);


--
-- Name: option_recommendation_validity_events option_recommendation_validity_events_candidate_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_recommendation_validity_events
    ADD CONSTRAINT option_recommendation_validity_events_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES public.option_strategy_candidates(candidate_id);


--
-- Name: option_recommendation_validity_events option_recommendation_validity_events_signal_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_recommendation_validity_events
    ADD CONSTRAINT option_recommendation_validity_events_signal_id_fkey FOREIGN KEY (signal_id) REFERENCES public.option_signal_events(event_id);


--
-- Name: option_scenario_results option_scenario_results_candidate_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_scenario_results
    ADD CONSTRAINT option_scenario_results_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES public.option_strategy_candidates(candidate_id);


--
-- Name: option_scenario_results option_scenario_results_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_scenario_results
    ADD CONSTRAINT option_scenario_results_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES public.option_snapshot_fact_keys(snapshot_id);


--
-- Name: option_signal_decay_outcomes option_signal_decay_outcomes_candidate_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_decay_outcomes
    ADD CONSTRAINT option_signal_decay_outcomes_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES public.option_strategy_candidates(candidate_id);


--
-- Name: option_signal_decay_outcomes option_signal_decay_outcomes_event_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_decay_outcomes
    ADD CONSTRAINT option_signal_decay_outcomes_event_id_fkey FOREIGN KEY (event_id) REFERENCES public.option_signal_events(event_id);


--
-- Name: option_signal_decay_outcomes option_signal_decay_outcomes_source_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_decay_outcomes
    ADD CONSTRAINT option_signal_decay_outcomes_source_batch_id_fkey FOREIGN KEY (source_batch_id) REFERENCES public.option_ingestion_runs(batch_id);


--
-- Name: option_signal_events option_signal_events_source_candidate_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_events
    ADD CONSTRAINT option_signal_events_source_candidate_id_fkey FOREIGN KEY (source_candidate_id) REFERENCES public.option_strategy_candidates(candidate_id);


--
-- Name: option_signal_legs option_signal_legs_contract_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_legs
    ADD CONSTRAINT option_signal_legs_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES public.option_contract_catalog(contract_id);


--
-- Name: option_signal_legs option_signal_legs_event_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_legs
    ADD CONSTRAINT option_signal_legs_event_id_fkey FOREIGN KEY (event_id) REFERENCES public.option_signal_events(event_id);


--
-- Name: option_signal_occurrences option_signal_occurrences_event_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_occurrences
    ADD CONSTRAINT option_signal_occurrences_event_id_fkey FOREIGN KEY (event_id) REFERENCES public.option_signal_events(event_id);


--
-- Name: option_signal_occurrences option_signal_occurrences_source_candidate_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_occurrences
    ADD CONSTRAINT option_signal_occurrences_source_candidate_id_fkey FOREIGN KEY (source_candidate_id) REFERENCES public.option_strategy_candidates(candidate_id);


--
-- Name: option_signal_occurrences option_signal_occurrences_source_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_occurrences
    ADD CONSTRAINT option_signal_occurrences_source_batch_id_fkey FOREIGN KEY (source_batch_id) REFERENCES public.option_ingestion_runs(batch_id);


--
-- Name: option_signal_suppressions option_signal_suppressions_candidate_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_signal_suppressions
    ADD CONSTRAINT option_signal_suppressions_candidate_id_fkey FOREIGN KEY (candidate_id) REFERENCES public.option_strategy_candidates(candidate_id);


--
-- Name: option_snapshot_fact_keys option_snapshot_fact_keys_contract_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_snapshot_fact_keys
    ADD CONSTRAINT option_snapshot_fact_keys_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES public.option_contract_catalog(contract_id);


--
-- Name: option_strategy_candidates option_strategy_candidates_context_snapshot_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_strategy_candidates
    ADD CONSTRAINT option_strategy_candidates_context_snapshot_id_fkey FOREIGN KEY (context_snapshot_id) REFERENCES public.option_context_snapshots(context_snapshot_id);


--
-- Name: option_strategy_candidates option_strategy_candidates_decision_evidence_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_strategy_candidates
    ADD CONSTRAINT option_strategy_candidates_decision_evidence_id_fkey FOREIGN KEY (decision_evidence_id) REFERENCES public.option_decision_evidence(evidence_id);


--
-- Name: option_strategy_candidates option_strategy_candidates_iv_context_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_strategy_candidates
    ADD CONSTRAINT option_strategy_candidates_iv_context_id_fkey FOREIGN KEY (iv_context_id) REFERENCES public.option_iv_context_snapshots(iv_context_id);


--
-- Name: option_strategy_candidates option_strategy_candidates_matrix_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_strategy_candidates
    ADD CONSTRAINT option_strategy_candidates_matrix_id_fkey FOREIGN KEY (matrix_id) REFERENCES public.option_analysis_runs(matrix_id);


--
-- Name: option_strategy_candidates option_strategy_candidates_strategy_name_strategy_version_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_strategy_candidates
    ADD CONSTRAINT option_strategy_candidates_strategy_name_strategy_version_fkey FOREIGN KEY (strategy_name, strategy_version) REFERENCES public.option_strategy_registry(strategy_name, strategy_version);


--
-- Name: option_trade_cursors option_trade_cursors_contract_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_trade_cursors
    ADD CONSTRAINT option_trade_cursors_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES public.option_contract_catalog(contract_id);


--
-- Name: option_trade_events option_trade_events_contract_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE public.option_trade_events
    ADD CONSTRAINT option_trade_events_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES public.option_contract_catalog(contract_id);


--
-- Name: option_trade_events option_trade_events_raw_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE public.option_trade_events
    ADD CONSTRAINT option_trade_events_raw_batch_id_fkey FOREIGN KEY (raw_batch_id) REFERENCES public.option_ingestion_runs(batch_id);


--
-- Name: option_trade_watchlist option_trade_watchlist_contract_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_trade_watchlist
    ADD CONSTRAINT option_trade_watchlist_contract_id_fkey FOREIGN KEY (contract_id) REFERENCES public.option_contract_catalog(contract_id);


--
-- Name: option_universe_candidates option_universe_candidates_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_universe_candidates
    ADD CONSTRAINT option_universe_candidates_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.option_universe_runs(run_id);


--
-- Name: option_universe_members option_universe_members_source_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_universe_members
    ADD CONSTRAINT option_universe_members_source_run_id_fkey FOREIGN KEY (source_run_id) REFERENCES public.option_universe_runs(run_id);


--
-- Name: option_volatility_surfaces option_volatility_surfaces_matrix_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.option_volatility_surfaces
    ADD CONSTRAINT option_volatility_surfaces_matrix_id_fkey FOREIGN KEY (matrix_id) REFERENCES public.option_analysis_runs(matrix_id);


CREATE TABLE public.schema_migrations (
    version text PRIMARY KEY,
    applied_at timestamp with time zone NOT NULL DEFAULT now()
);

SET search_path = public, pg_catalog;

INSERT INTO public.schema_migrations (version)
VALUES ('000_canonical_schema');

INSERT INTO public.equity_portal_source_state (singleton, generation)
VALUES (TRUE, 0);

SELECT public.ensure_option_market_data_partitions(
    date_trunc('month', CURRENT_DATE)::date
);
SELECT public.ensure_option_market_data_partitions(
    (date_trunc('month', CURRENT_DATE) + INTERVAL '1 month')::date
);

COMMIT;

