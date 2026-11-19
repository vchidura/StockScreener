-- G106 H1 (docs/g106/G106_TARGET_DESIGN.md sections 7 and 10.1): the daily close chain snapshot. History only has value
-- once it has accumulated (IV rank and percentile, day-over-day open interest change, skew history, outcome research),
-- so each session's final chain is kept here, one row per session and contract.
--
-- rt.option_breadth_current keeps one row per contract and session, replaced at every capture: only the session's last
-- capture survives, and nothing in it is meant as history. After the session close the writer
-- (realtime/option_breadth_daily.py) copies the session's rows here once (INSERT ... SELECT ... ON CONFLICT DO NOTHING),
-- or loads a CSV copy of them (COPY ... WHERE session_date = X of the current table) for a session already pruned there.
--
-- Columns: the current table's, unchanged (observed_at is the row's capture time), plus
--   final_capture    the row belongs to its underlying's last capture of the session (G102 R55: contracts that left the
--                    band in an earlier capture keep an older row in the current table; readers that want the close
--                    chain filter on this);
--   snapshot_at      when the row was copied here;
--   snapshot_source  CURRENT_TABLE (copied from rt.option_breadth_current) or CSV_COPY (loaded from a file copy);
--   source_reference the relation, or the file name and its sha256.
-- Rows are evidence: append-only, like the other append relations (no grant here; the registry and the hardening
-- routine grant SELECT/INSERT to the app role and SELECT to the API role).
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '120s';

CREATE TABLE rt.option_breadth_daily (
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
    local_iv double precision,
    local_delta double precision,
    local_gamma double precision,
    local_theta_per_day double precision,
    local_vega_per_vol_point double precision,
    local_rho_per_rate_point double precision,
    spot numeric(20,8),
    risk_free_rate double precision,
    dividend_yield double precision,
    valuation_basis varchar(24),
    greeks_reason varchar(48),
    equity_cutoff timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    final_capture boolean NOT NULL,
    snapshot_at timestamptz NOT NULL,
    snapshot_source varchar(16) NOT NULL,
    source_reference varchar(160) NOT NULL,
    PRIMARY KEY (session_date, contract_ticker),
    CHECK (valuation_basis IS NULL OR valuation_basis IN ('EXACT_EXPIRY', 'NEAREST_EXPIRY')),
    CHECK (btrim(contract_ticker) <> '' AND btrim(underlying) <> ''),
    CHECK (contract_type IN ('call', 'put')),
    CHECK (strike_price > 0),
    CHECK (day_volume IS NULL OR day_volume >= 0),
    CHECK (open_interest IS NULL OR open_interest >= 0),
    CHECK (bid IS NULL OR bid >= 0),
    CHECK (ask IS NULL OR ask >= 0),
    CHECK (observed_at >= equity_cutoff - interval '1 day'),
    CHECK (snapshot_source IN ('CURRENT_TABLE', 'CSV_COPY')),
    CHECK (btrim(source_reference) <> ''),
    CHECK (snapshot_at >= observed_at)
);

-- Per name and session (IV summary history, skew history): the chain order the current table's index uses.
CREATE INDEX idx_rt_option_breadth_daily_underlying
    ON rt.option_breadth_daily (underlying, session_date, expiration_date, strike_price);
-- Per contract across sessions (day-over-day open interest change, outcome research).
CREATE INDEX idx_rt_option_breadth_daily_contract
    ON rt.option_breadth_daily (contract_ticker, session_date);

CREATE TRIGGER trg_rt_option_breadth_daily_immutable
    BEFORE UPDATE OR DELETE ON rt.option_breadth_daily
    FOR EACH ROW EXECUTE FUNCTION rt.reject_immutable_change();
CREATE TRIGGER trg_rt_option_breadth_daily_no_truncate
    BEFORE TRUNCATE ON rt.option_breadth_daily
    FOR EACH STATEMENT EXECUTE FUNCTION rt.reject_immutable_change();
