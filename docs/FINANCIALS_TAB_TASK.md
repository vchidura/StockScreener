# Task: Financials tab, designed and fed from Massive

Status: open, planned for the weekend of Nov 7-8, 2026. Created Nov 6 at the user's request.
Tracks gap G37 (fundamentals never loaded) in `docs/agent-context/advanced-realtime-integration-gaps.md`.

## Why

The ticker page has a Financials tab (`frontend/src/pages/ticker/FinancialsView.tsx`), but it has never shown data:
`ref.fundamental_reports` and `ref.fundamental_report_coverage` hold 0 rows for all 25 members. A Massive reader
exists (`realtime/reference_transports.py`, `reference_adapters.py`, `normalize_fundamental_reports`) but is switched
off (`include_fundamentals=False` in `scripts/refresh_realtime_v2_references.py`) because the entitlement was unknown.

## Entitlement: confirmed

Massive docs list these endpoints for Stocks Advanced (the user's plan), and one sample request per endpoint for AAPL
on Nov 6 at 13:17 UTC returned HTTP 200 with data (`runner/fundamentals_sample.py`, read-only, nothing stored):

| Endpoint | Cadence | Sample (AAPL) |
|---|---|---|
| `/stocks/financials/v1/income-statements` (quarterly, annual, TTM) | daily, history to 2009 | FY26 Q4 to Sep 26, filed Nov 3: revenue 114.5B, diluted EPS 2.10 |
| `/stocks/financials/v1/balance-sheets` | daily | assets 404.5B, equity 112.1B, cash 36.3B |
| `/stocks/financials/v1/cash-flow-statements` (quarterly, annual, TTM) | daily | TTM operating cash 145.1B, capex -9.0B |
| `/stocks/financials/v1/ratios` | daily (EOD) | Nov 5: P/E 37.8, P/S 10.5, EV/EBITDA 29.4, ROE 1.18, yield 0.31% |
| `/stocks/vX/float` | as needed | free float 99.9% |
| `/stocks/v1/short-interest` | every 2 weeks | 131.6M shares, 3.71 days to cover (Oct 15) |
| `/stocks/v1/short-volume` | daily | 51.5% of volume on Nov 5 |
| `/stocks/v1/dividends` | as needed | next ex-date Nov 12, 0.27, quarterly |

Not yet sampled: `/stocks/filings/vX/index`, which the existing reader requires for availability times.

## Part 1: design (needs the user's review before building)

Proposed sections, top to bottom. ETFs show a short "statements do not apply to ETFs" note plus float, short
interest and dividends.

1. **Key stats** (ratios, daily): market cap, P/E, P/S, P/B, EV/EBITDA, P/FCF, dividend yield, ROE, ROA,
   debt/equity, current and quick ratio. Each with its as-of date.
2. **Income statement**: last 8 quarters and last 5 fiscal years, toggle; TTM column. Revenue, gross profit and margin,
   operating income and margin, net income, diluted EPS, EBITDA, R&D; year-over-year growth.
   Small charts: revenue and EPS by quarter, margins over time.
3. **Balance sheet** (latest quarter and trend): total assets, liabilities, equity, cash plus short-term investments,
   total debt (current plus long-term), net cash or net debt, receivables, inventories.
4. **Cash flow** (TTM and quarterly): operating cash flow, capex, free cash flow (operating cash plus capex),
   FCF margin, dividends paid.
5. **Float and short interest**: free float %, short interest, days to cover, settlement date, daily short-volume ratio.
6. **Dividends**: next ex-date and pay date, amount, frequency, yield.
7. **Filing timing**: latest period end and filing date, next earnings date (from the Finnhub calendar already stored).

Questions for the user: which sections matter most; quarterly or annual as the default; whether short volume is worth
showing daily.

## Part 2: fetch, store and project

- **When:** the preparation role's after-close idle slot, like the earnings and Fed refreshes. Statements and ratios
  once a day after the close, short interest when a new settlement date appears, float and dividends daily.
- **For whom:** the desired revision's members, never a fixed list (lesson of G92). ETFs skip statements.
- **Bounded:** one request per endpoint per batch of tickers (`tickers.any_of`), row and byte limits as in
  `reference_transports.py`; at 25 members that is under 10 requests a day.
- **Store:** statements into `ref.fundamental_reports` and `ref.fundamental_report_coverage` through the existing
  normalizer. Ratios, float, short interest and dividends need a home; decide between new `ref.*` tables and the
  existing corporate-action tables (dividends are already stored there).
- **Point in time:** a report becomes visible at its filing time (`availability_time`), so history views never show a
  filing before it was public. Backfill keeps that rule.
- **Never block a window:** a failed or missing fetch leaves the section "Unavailable", exactly like the earnings
  refresh; nothing in `EQUITY_PROJECTION` may raise on missing fundamentals. Only clock drift raises today.
- **Project:** `ticker_projection._fundamental_projection` already passes reports to the workspace; extend it with
  ratios, float and short interest, and add the new inputs to the publication identity.
- **Portal:** rebuild `FinancialsView.tsx` to the agreed design; label each value with its source and as-of date.

## Part 3: validation

- Unit tests for the normalizer and projection with recorded Massive responses (no provider calls in tests or
  rehearsals).
- Rehearsal on a copy of live: fetch from recorded fixtures, project, read the page; a window must not slow down.
- On live after deploy: compare page values with the stored rows and with Massive for 3 tickers (one stock, one
  financial, one ETF), the same way prices were validated on Nov 5.

## Order

1. Design review with the user (Part 1).
2. Backend fetch and store, rehearsed.
3. Projection and page.
4. Deploy after a close, never mid-session; validate on live.
