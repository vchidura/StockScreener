-- G75 (docs/OPTIONS_COVERAGE_TASK.md, the user's decision of Nov 6: breadth + moderate depth): every in-scope option
-- contract's day volume, open interest, NBBO, IV and Greeks, from REST chain snapshots taken at each Options window.
--
-- The stream covers 8-12 contracts per underlying (72 of 9,264 on Nov 6), so O1 and the Options Screener saw almost
-- nothing of the chain. Breadth is captured beside the frozen contract set and never decides a window: a failed or late
-- underlying keeps its previous rows, labelled by its last capture.
--
-- rt.option_breadth_current: one row per contract and session, replaced at each capture (keyed by what it is).
-- rt.option_breadth_captures: one row per underlying and capture, for freshness labels and audit. Each request is in
-- rt.provider_request_ledger; response bodies are not kept (about 15 MB per window at 9 underlyings).
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.option_breadth_current (
    session_date date NOT NULL,
    contract_ticker varchar(40) NOT NULL,
    underlying varchar(16) NOT NULL,
    contract_type varchar(4) NOT NULL,
    expiration_date date NOT NULL,
    strike_price numeric(20,8) NOT NULL,
    day_volume bigint,
    open_interest bigint,
    last_trade_price numeric(20,8),
    last_trade_at timestamptz,
    bid numeric(20,8),
    ask numeric(20,8),
    quote_at timestamptz,
    implied_volatility double precision,
    delta double precision,
    gamma double precision,
    theta double precision,
    vega double precision,
    underlying_price numeric(20,8),
    equity_cutoff timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    PRIMARY KEY (session_date, contract_ticker),
    CHECK (btrim(contract_ticker) <> '' AND btrim(underlying) <> ''),
    CHECK (contract_type IN ('call', 'put')),
    CHECK (strike_price > 0),
    CHECK (day_volume IS NULL OR day_volume >= 0),
    CHECK (open_interest IS NULL OR open_interest >= 0),
    CHECK (bid IS NULL OR bid >= 0),
    CHECK (ask IS NULL OR ask >= 0),
    CHECK (observed_at >= equity_cutoff - interval '1 day')
) WITH (fillfactor = 70);

CREATE INDEX idx_rt_option_breadth_chain
    ON rt.option_breadth_current (session_date, underlying, expiration_date, strike_price);

CREATE TABLE rt.option_breadth_captures (
    capture_id uuid PRIMARY KEY,
    session_date date NOT NULL,
    underlying varchar(16) NOT NULL,
    equity_cutoff timestamptz NOT NULL,
    started_at timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    state varchar(16) NOT NULL,
    pages integer NOT NULL,
    contracts integer NOT NULL,
    skipped_rows integer NOT NULL,
    response_bytes bigint NOT NULL,
    reason varchar(160),
    CHECK (btrim(underlying) <> ''),
    CHECK (state IN ('COMPLETE', 'PARTIAL', 'FAILED')),
    CHECK (pages >= 0 AND contracts >= 0 AND skipped_rows >= 0 AND response_bytes >= 0),
    CHECK (observed_at >= started_at)
);

CREATE INDEX idx_rt_option_breadth_capture_latest
    ON rt.option_breadth_captures (session_date, underlying, observed_at DESC);

-- Captures are evidence: append-only, like the other append relations.
CREATE TRIGGER trg_rt_option_breadth_captures_immutable
    BEFORE UPDATE OR DELETE ON rt.option_breadth_captures
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_breadth_captures_no_truncate
    BEFORE TRUNCATE ON rt.option_breadth_captures
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
