-- Durable retry queue for provider rows that remain missing after bounded retries.

CREATE TABLE IF NOT EXISTS data_ingestion_failures (
    failure_id      BIGSERIAL PRIMARY KEY,
    dataset         VARCHAR(32)  NOT NULL,
    ticker          VARCHAR(16)  NOT NULL,
    trade_date      DATE         NOT NULL,
    provider        VARCHAR(32)  NOT NULL,
    failure_type    VARCHAR(64)  NOT NULL,
    details         TEXT,
    attempts        INTEGER      NOT NULL DEFAULT 0,
    first_seen_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_attempt_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ,
    UNIQUE (dataset, ticker, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_ingestion_failures_unresolved
    ON data_ingestion_failures (dataset, trade_date DESC)
    WHERE resolved_at IS NULL;

COMMENT ON TABLE data_ingestion_failures IS
    'Provider rows still missing after bounded scheduler retries; supports later manual repair.';
