-- Point-in-time coverage proves when absence of an event can safely mean CLEAR.

CREATE TABLE IF NOT EXISTS option_event_calendar_coverage (
    coverage_id uuid PRIMARY KEY,
    event_type character varying(32) NOT NULL,
    affected_underlying character varying(16),
    window_start timestamp with time zone NOT NULL,
    window_end timestamp with time zone NOT NULL,
    source character varying(64) NOT NULL,
    source_key character varying(256) NOT NULL,
    first_observed_at timestamp with time zone NOT NULL,
    payload_sha256 character(64) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    UNIQUE (source, source_key, first_observed_at),
    CHECK (window_end > window_start),
    CHECK (event_type IN ('EARNINGS', 'FED_RATE_DECISION')),
    CHECK (
        (event_type = 'FED_RATE_DECISION' AND affected_underlying IS NULL)
        OR (event_type = 'EARNINGS' AND affected_underlying IS NOT NULL)
    ),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS idx_option_event_calendar_coverage_lookup
    ON option_event_calendar_coverage
        (source, event_type, affected_underlying, first_observed_at DESC);

COMMENT ON TABLE option_event_calendar_coverage IS
    'Append-only source coverage needed to distinguish no event from unavailable data.';