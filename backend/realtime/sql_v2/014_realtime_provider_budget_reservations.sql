SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

ALTER TABLE rt.provider_request_ledger
    ADD COLUMN budget_scope_sha256 char(64),
    ADD COLUMN budget_session_date date,
    ADD COLUMN authorization_reference varchar(160),
    ADD COLUMN reserved_response_bytes bigint,
    ADD COLUMN account_request_ceiling integer,
    ADD COLUMN account_decoded_byte_ceiling bigint;

ALTER TABLE rt.provider_request_ledger
    ADD CONSTRAINT provider_request_ledger_budget_scope_check CHECK (
        (
            budget_scope_sha256 IS NULL
            AND budget_session_date IS NULL
            AND authorization_reference IS NULL
            AND reserved_response_bytes IS NULL
            AND account_request_ceiling IS NULL
            AND account_decoded_byte_ceiling IS NULL
        )
        OR
        (
            budget_scope_sha256 ~ '^[0-9a-f]{64}$'
            AND budget_session_date IS NOT NULL
            AND btrim(authorization_reference) <> ''
            AND reserved_response_bytes > 0
            AND account_request_ceiling > 0
            AND account_decoded_byte_ceiling > 0
            AND reserved_response_bytes <= account_decoded_byte_ceiling
        )
    );

CREATE INDEX idx_rt_provider_request_budget
    ON rt.provider_request_ledger (
        account_alias,
        budget_session_date,
        reserved_at,
        request_id
    )
    WHERE budget_scope_sha256 IS NOT NULL;
