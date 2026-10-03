-- Advanced quote hot path: current state, sparse history, and monitor evidence.
-- This migration does not enable provider streaming or broker execution.

CREATE TABLE IF NOT EXISTS public.option_quote_stream_sessions (
    stream_session_id uuid PRIMARY KEY,
    provider varchar(64) NOT NULL,
    capability_profile_sha256 char(64) NOT NULL,
    configured_underlyers text[] NOT NULL,
    started_at timestamptz NOT NULL,
    ended_at timestamptz,
    status varchar(24) NOT NULL,
    disconnect_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (provider = 'polygon_advanced'),
    CHECK (capability_profile_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (cardinality(configured_underlyers) BETWEEN 1 AND 100),
    CHECK (status IN ('CONNECTING', 'CONNECTED', 'DISCONNECTED', 'FAILED')),
    CHECK (ended_at IS NULL OR ended_at >= started_at)
);

CREATE TABLE IF NOT EXISTS public.option_quote_current (
    provider varchar(64) NOT NULL,
    contract_id bigint NOT NULL REFERENCES public.option_contract_catalog(contract_id),
    stream_session_id uuid REFERENCES public.option_quote_stream_sessions(stream_session_id),
    contract_ticker varchar(64) NOT NULL,
    quote_time timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    bid numeric(20,8) NOT NULL,
    ask numeric(20,8) NOT NULL,
    bid_size bigint,
    ask_size bigint,
    conditions text[] NOT NULL DEFAULT ARRAY[]::text[],
    payload_sha256 char(64) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (provider, contract_id),
    CHECK (provider = 'polygon_advanced'),
    CHECK (quote_time <= received_at),
    CHECK (bid >= 0 AND ask >= bid),
    CHECK (bid_size IS NULL OR bid_size >= 0),
    CHECK (ask_size IS NULL OR ask_size >= 0),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS idx_option_quote_current_contract_time
    ON public.option_quote_current (contract_id, quote_time DESC);

CREATE TABLE IF NOT EXISTS public.option_quote_samples (
    quote_sample_id uuid NOT NULL,
    sampled_at timestamptz NOT NULL,
    provider varchar(64) NOT NULL,
    contract_id bigint NOT NULL REFERENCES public.option_contract_catalog(contract_id),
    stream_session_id uuid REFERENCES public.option_quote_stream_sessions(stream_session_id),
    quote_time timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    bid numeric(20,8) NOT NULL,
    ask numeric(20,8) NOT NULL,
    bid_size bigint,
    ask_size bigint,
    sample_reason varchar(24) NOT NULL,
    payload_sha256 char(64) NOT NULL,
    PRIMARY KEY (quote_sample_id, sampled_at),
    UNIQUE (provider, contract_id, quote_time, payload_sha256, sampled_at),
    CHECK (provider = 'polygon_advanced'),
    CHECK (quote_time <= received_at AND received_at <= sampled_at),
    CHECK (bid >= 0 AND ask >= bid),
    CHECK (bid_size IS NULL OR bid_size >= 0),
    CHECK (ask_size IS NULL OR ask_size >= 0),
    CHECK (sample_reason IN ('CHANGE', 'HEARTBEAT', 'DECISION', 'EXIT_MONITOR')),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$')
) PARTITION BY RANGE (sampled_at);

CREATE INDEX IF NOT EXISTS idx_option_quote_samples_contract_time
    ON ONLY public.option_quote_samples (contract_id, quote_time DESC);

CREATE TABLE IF NOT EXISTS public.option_price_monitor_events (
    monitor_event_id uuid PRIMARY KEY,
    candidate_id uuid REFERENCES public.option_strategy_candidates(candidate_id),
    plan_id uuid REFERENCES public.option_alert_plans(plan_id),
    contract_id bigint REFERENCES public.option_contract_catalog(contract_id),
    monitor_key char(64) NOT NULL,
    event_type varchar(24) NOT NULL,
    event_time timestamptz NOT NULL,
    quote_time timestamptz,
    threshold_price numeric(20,8),
    observed_price numeric(20,8),
    payload jsonb NOT NULL,
    payload_sha256 char(64) NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (monitor_key, event_type, event_time, payload_sha256),
    CHECK (event_type IN ('ARMED', 'QUOTE', 'TARGET_MET', 'STOP_LOSS_HIT', 'EXPIRED', 'UNAVAILABLE')),
    CHECK (quote_time IS NULL OR quote_time <= event_time),
    CHECK (threshold_price IS NULL OR threshold_price >= 0),
    CHECK (observed_price IS NULL OR observed_price >= 0),
    CHECK (jsonb_typeof(payload) = 'object'),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS idx_option_price_monitor_events_plan_time
    ON public.option_price_monitor_events (plan_id, event_time DESC)
    WHERE plan_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_option_price_monitor_events_candidate_time
    ON public.option_price_monitor_events (candidate_id, event_time DESC)
    WHERE candidate_id IS NOT NULL;

CREATE OR REPLACE FUNCTION public.ensure_option_market_data_partitions(p_month_start date) RETURNS void
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
    quote_partition TEXT;
BEGIN
    IF p_month_start <> date_trunc('month', p_month_start)::DATE THEN
        RAISE EXCEPTION 'p_month_start must be the first day of a month';
    END IF;
    current_month := date_trunc('month', CURRENT_DATE)::DATE;
    IF p_month_start NOT IN (current_month, (current_month + INTERVAL '1 month')::DATE) THEN
        RAISE EXCEPTION 'partition maintenance is limited to current and next month';
    END IF;
    month_start_utc := p_month_start::TIMESTAMP AT TIME ZONE 'UTC';
    next_month_utc := (p_month_start + INTERVAL '1 month')::TIMESTAMP AT TIME ZONE 'UTC';
    month_suffix := to_char(p_month_start, 'YYYYMM');
    snapshot_partition := 'option_chain_snapshots_y' || month_suffix;
    trade_partition := 'option_trade_events_y' || month_suffix;
    quote_partition := 'option_quote_samples_y' || month_suffix;
    PERFORM pg_advisory_xact_lock(hashtextextended('option-market-data-partition:' || month_suffix, 0));
    EXECUTE format('CREATE TABLE IF NOT EXISTS public.%I PARTITION OF public.option_chain_snapshots FOR VALUES FROM (%L) TO (%L)', snapshot_partition, month_start_utc, next_month_utc);
    EXECUTE format('CREATE TABLE IF NOT EXISTS public.%I PARTITION OF public.option_trade_events FOR VALUES FROM (%L) TO (%L)', trade_partition, month_start_utc, next_month_utc);
    EXECUTE format('CREATE TABLE IF NOT EXISTS public.%I PARTITION OF public.option_quote_samples FOR VALUES FROM (%L) TO (%L)', quote_partition, month_start_utc, next_month_utc);
END;
$$;

REVOKE ALL ON FUNCTION public.ensure_option_market_data_partitions(date) FROM PUBLIC;
SELECT public.ensure_option_market_data_partitions(date_trunc('month', CURRENT_DATE)::date);
SELECT public.ensure_option_market_data_partitions((date_trunc('month', CURRENT_DATE) + INTERVAL '1 month')::date);
INSERT INTO public.schema_migrations(version) VALUES ('059_option_advanced_quote_monitoring') ON CONFLICT DO NOTHING;