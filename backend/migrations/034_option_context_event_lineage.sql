-- Direct, foreign-key-backed event and coverage evidence for every strategy context.

CREATE TABLE IF NOT EXISTS option_context_market_event_evidence (
    context_snapshot_id uuid NOT NULL
        REFERENCES option_context_snapshots(context_snapshot_id) ON DELETE CASCADE,
    market_event_id uuid NOT NULL
        REFERENCES option_market_events(market_event_id),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (context_snapshot_id, market_event_id)
);

CREATE INDEX IF NOT EXISTS idx_option_context_market_event_reverse
    ON option_context_market_event_evidence
        (market_event_id, context_snapshot_id);

CREATE TABLE IF NOT EXISTS option_context_event_coverage_evidence (
    context_snapshot_id uuid NOT NULL
        REFERENCES option_context_snapshots(context_snapshot_id) ON DELETE CASCADE,
    coverage_id uuid NOT NULL
        REFERENCES option_event_calendar_coverage(coverage_id),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (context_snapshot_id, coverage_id)
);

CREATE INDEX IF NOT EXISTS idx_option_context_event_coverage_reverse
    ON option_context_event_coverage_evidence
        (coverage_id, context_snapshot_id);

COMMENT ON TABLE option_context_market_event_evidence IS
    'Exact event revisions visible to and used by an immutable strategy context.';
COMMENT ON TABLE option_context_event_coverage_evidence IS
    'Coverage revisions proving when event absence could safely mean CLEAR.';