CREATE TABLE IF NOT EXISTS option_signal_current_marks (
    candidate_id uuid NOT NULL REFERENCES option_strategy_candidates(candidate_id),
    event_id uuid REFERENCES option_signal_events(event_id),
    market_time timestamp with time zone NOT NULL,
    observed_time timestamp with time zone NOT NULL,
    mark numeric(20,8) NOT NULL,
    net_return numeric(20,8) NOT NULL,
    availability_flag character varying(32) NOT NULL,
    quality_flags text[] NOT NULL DEFAULT ARRAY[]::text[],
    entry_net_premium numeric(20,8) NOT NULL,
    exit_net_premium numeric(20,8) NOT NULL,
    gross_pnl numeric(20,8) NOT NULL,
    estimated_cost numeric(20,8) NOT NULL,
    net_pnl numeric(20,8) NOT NULL,
    capital_at_risk numeric(20,8) NOT NULL CHECK (capital_at_risk > 0),
    valuation_policy_version character varying(64) NOT NULL,
    valuation_policy_sha256 character(64) NOT NULL,
    source_snapshot_ids uuid[] NOT NULL DEFAULT ARRAY[]::uuid[],
    source_batch_id uuid NOT NULL REFERENCES option_ingestion_runs(batch_id),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (candidate_id, valuation_policy_sha256),
    CHECK (observed_time >= market_time),
    CHECK (availability_flag = 'RESEARCH_DELAYED_PROXY'),
    CHECK (valuation_policy_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (cardinality(source_snapshot_ids) > 0)
);

CREATE INDEX IF NOT EXISTS idx_option_current_marks_policy_market
    ON option_signal_current_marks (valuation_policy_sha256, market_time DESC);