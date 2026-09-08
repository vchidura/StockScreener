-- Immutable, queryable execution-gate verdicts for each candidate.

CREATE TABLE IF NOT EXISTS option_candidate_execution_gates (
    candidate_id uuid NOT NULL
        REFERENCES option_strategy_candidates(candidate_id),
    ledger_version character varying(64) NOT NULL,
    gate_name character varying(40) NOT NULL,
    verdict character varying(16) NOT NULL,
    blocking boolean NOT NULL,
    reason_codes text[] NOT NULL DEFAULT ARRAY[]::text[],
    evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    evaluated_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now(),

    PRIMARY KEY (candidate_id, ledger_version, gate_name),
    CHECK (verdict IN ('PASS', 'FAIL', 'UNAVAILABLE')),
    CHECK (blocking = (verdict <> 'PASS')),
    CHECK (jsonb_typeof(evidence) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_option_candidate_execution_gates_verdict
    ON option_candidate_execution_gates
        (gate_name, verdict, evaluated_at DESC);

COMMENT ON TABLE option_candidate_execution_gates IS
    'Versioned execution capability and risk verdicts produced by the strategy engine.';