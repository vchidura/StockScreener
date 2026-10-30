# Equity model detection backtests

Implemented October 25, 2026. This package implements the remaining model
criteria from the equity research roadmap for **historical detection and gross
forward price returns**. It does not simulate entries, exits, fills, positions,
fees, portfolio allocations or broker orders. Nothing here publishes live alerts.

## Run

From the repository root in PowerShell:

```powershell
.\backend\.venv\Scripts\python.exe backend/scripts/run_equity_detection_backtest.py --list-models
.\backend\.venv\Scripts\python.exe backend/scripts/run_equity_detection_backtest.py --input C:\research\equity-inputs.json.gz --output C:\research\detection-run --models P1 P2 SW1 SW2 SW3_R SW3_B SW3_F D1 D2 C1 C2
```

Omit `--models` to run the 19 default registrations, including R2/B3/F2 at
30m and 1h and both B3 retest intervals. `--start`, `--end` and `--as-of` take
timezone-aware ISO timestamps. `--securities` selects a subset of the explicitly
declared evaluation universe. Input is JSON or gzip JSON. The command makes no
database connections or provider calls.

The checked-in synthetic example can be run without external data:

```powershell
.\backend\.venv\Scripts\python.exe backend/scripts/run_equity_detection_backtest.py --input backend/tests/fixtures/equity_research_position_v1.json --output backend/backups/research-detection-example --models P1 P2
```

This example validates plumbing; it is not market evidence or a profitability test.

Output:

- `manifest.json`: input digest, policy registrations, source-code hashes,
  availability mode, clock range and unique run ID.
- `evaluations.jsonl`: every scheduled decision, gates, input IDs, state,
  disposition and, for a match, frozen reference/geometry/episode identity.
- `outcomes.jsonl`: separate forward measurements and endpoint lineage.
- `summary.json`: counts by model and status, reason counts, available/pending/
  unavailable endpoint counts and mean signed gross price returns.
- `REPORT.md`: coverage overview and interpretation.

Identical reruns may reuse an output directory. Different artifacts are refused
before any output is overwritten. Invalid contracts abort the run; missing
required model evidence produces `UNAVAILABLE` for that lane. A model with no
trigger-resolution inputs gets `NO_DECISION_INPUTS`, not a fabricated zero-signal
successful backtest.

## Model registrations

| IDs | Implemented detection | Timing and intended research horizon |
|---|---|---|
| `R2_30m_*`, `R2_1h_*` | Frozen 20-bar high/low, 1–3 ATR pullback, later recovery; context adds market/sector alignment, directional daily ADX and sector-relative strength; participation adds RVOL and close location; combined adds chase restriction | Intraday; reference-age limit 300 open-bar minutes, at least two subsequent confirmation bars; advisory 240 minutes |
| `B3_30m_*`, `B3_1h_*` | Frozen 10-bar compressed range and next-bar Quality V2 acceptance; context/participation/combined gates | Intraday; advisory 120 minutes |
| `B3_30m_retest`, `B3_1h_retest` | Separate breakout/retest policy, frozen range, three-bar deadline, directional reclaim, participation/context and chase gates | Intraday; advisory 120 minutes |
| `F2_30m_*`, `F2_1h_*` | Frozen 20-bar extension/reentry; combined limits extension to 0.5–1.5 ATR, requires qualified reentry then a further confirmation within the original three-bar deadline | Intraday; advisory 60 minutes |
| `SW3_R`, `SW3_B`, `SW3_F` | New same-session hourly setup followed by later full-hour resumption/acceptance/reversal confirmation; original structural targets; no unconfirmed overnight carry | Swing; advisory 21/10/5 sessions |
| `P1` | Month-end top-20% 12-minus-1 total-return momentum, top-30% sector quality, rising SMA200 and comparable company/liquidity requirements | Position; advisory 252 sessions |
| `P2` | Month-end sector top-quartile value, positive FCF, coverage >3, improving margin, SMA100 and positive 63-session return | Position; advisory 378 sessions |
| `SW1` | Qualified daily sector-relative breakout then hourly retest/reclaim within two sessions; frozen daily ATR | Swing; advisory 15 sessions |
| `SW2` | Original comparable EPS surprise and first-full-session earnings reaction, next-session hourly high breakout, bounded chase | Swing; advisory 20 sessions |
| `D1` | Complete first-15-minute opening range, RVOL and SPY VWAP trend, breakout then retest within three 5m bars; one confirmed episode/session | Intraday, long-only initial trial; advisory 120 minutes |
| `D2` | Certified session VWAP trend, fresh pullback and later reclaim with slot participation; four-bar deadline; half-day-aware cutoff | Intraday, long-only initial trial; advisory 90 minutes |
| `C1` | Actual best-quote event OFI, prior-five-minute normalization, two qualifying nonoverlapping seconds, fresh quote/spread and out-of-sample cost/movement evidence | Scalping, long-only prototype; midpoint diagnostics, advisory 60 seconds |
| `C2` | Five quiet OFI windows, stable mid, two-tick spread, qualified side-specific capture/cost evidence, zero existing inventory | Scalping; midpoint diagnostics, advisory 45 seconds; no passive-fill simulation |

The `*` family variants are `structure`, `context`, `participation`, and
`combined`. There are **37 registrations** total, **19 enabled by the default CLI
model selection**. These are explicit groups of hypotheses, not every possible
single-feature ablation. `structure` uses the new Wilder features and research
timing contracts; it is **not an exact legacy parity baseline**. The original
legacy source and replay remain the unchanged baseline. The new IDs do not make
unused knobs in `legacy_backtest_variants.py` into effective model implementations.

P1/P2, SW1/SW2 and D1/D2 start long-only. R2/B3/F2 and SW3 mirror direction.
C2 accepts the side provided by qualified side-specific evidence. No short-borrow
or cost assumptions are implied by a bearish detection.

Holding horizons and suggested stops/targets describe the research policies.
They are not executed or used to manufacture strategy P&L. P1/P2 deliberately
leave stop and target null because the roadmap has no frozen numeric catastrophe
stop. Trailing exits, monthly position maintenance, net-R/R entry economics,
queue fills and automatic flattening are outside this detection-only release.
Live portal lifecycle/entry-expiry integration is also outside this offline path.

## Frozen input contract

Top-level example (record examples follow):

```json
{
  "schema": "equity_research_dataset_v1",
  "availability_mode": "OBSERVED",
  "evaluation_start": "2026-09-01T13:30:00Z",
  "evaluation_end": "2026-09-30T20:00:01Z",
  "as_of": "2026-11-30T22:00:00Z",
  "evaluation_securities": ["stable-security-id"],
  "bars": [],
  "facts": [],
  "quotes": []
}
```

`HISTORICAL_RECONSTRUCTED` is the other supported availability mode; it is retained
in every run's manifest. It must not be presented as observed forward evidence.
Exporter-supplied reconstruction timestamps must follow a declared methodology.
Do not relabel database creation dates or fiscal period ends as historical
public-availability timestamps. `as_of` is the frozen extract/measurement cutoff.

Each observation requires stable `security_id`, `source_id`, `revision`,
`market_at`, `available_at`, and `received_at` with
`market_at <= available_at <= received_at`. Revisions are selected only from
records actually received by each decision; a future correction cannot rewrite
an earlier detection. Ticker names are not used as identity. A revision tie is
resolved deterministically by revision string; exporters should supply distinct
receipt timestamps where the ordering is material.

### Completed bars

```json
{
  "security_id": "stable-security-id", "source_id": "bar-revision-source", "revision": "1",
  "start": "2026-09-30T13:30:00Z", "market_at": "2026-09-30T13:35:00Z",
  "available_at": "2026-09-30T13:35:00Z", "received_at": "2026-09-30T13:35:00.050Z",
  "interval": "5m", "completed": true,
  "adjustment_id": "certified-common-price-basis", "segment_id": "identity-action-continuity-segment",
  "open": 100, "high": 101, "low": 99, "close": 100.5, "volume": 10000,
  "vwap": 100.2
}
```

Supported intervals: `5m`, `30m`, `1h`, `1d`. Native hourly data must consist of
full 60-minute RTH bars anchored to the XNYS open; the final 30 minutes of a normal
session are not a full hour. Five- and thirty-minute slots also anchor to the
open. Daily bars span exact session open/close. Holidays, DST and early closes
come from the installed exchange calendar. Overnight gaps enter true range;
missing expected bars, identity/segment changes and adjustment changes restart
warmup rather than bridging them. Supply at least 253 daily bars and 200 trigger
bars, plus the historical same-slot volume coverage required by the model.

`vwap` is optional native eligible-trade VWAP, never OHLC typical price or rolling
weighted close. D1's market benchmark and D2's security require it. Session VWAP
needs the complete session prefix; slot/cumulative RVOL use 15–20 prior comparable
sessions, with shortened sessions separated. Daily rank construction additionally
requires a consistent dividend-inclusive `total_return_close` index and a stable
`total_return_basis_id` certifying its construction. Split-adjusted
close alone is insufficient for the P1 total-return rank.

### Versioned facts

```json
{
  "security_id": "stable-security-id", "source_id": "dated-eligibility", "revision": "1",
  "kind": "eligibility", "market_at": "2026-09-30T12:00:00Z",
  "available_at": "2026-09-30T12:00:00Z", "received_at": "2026-09-30T12:00:01Z",
  "valid_until": "2026-09-30T21:00:00Z",
  "methodology": "exporter-eligibility-policy-v1", "input_ids": ["universe-revision", "action-coverage-revision"],
  "values": {"member": true, "actions_covered": true, "price_basis": "SPLIT_ADJUSTED"}
}
```

Required fields by fact kind:

| Kind | Values / applicable models |
|---|---|
| `eligibility` | `member`, `actions_covered`, `price_basis` (`SPLIT_ADJUSTED` or `RAW_CONTINUOUS`); all models |
| `events` | `macro_covered`, `macro_blocked`; all models. Macro blocking means within 15 minutes either side of a covered release. Swing also needs `earnings_covered_through` and `earnings_sessions`, covering its entire declared horizon |
| `benchmarks` | `market_security_id`, `sector_security_id`; qualified point-in-time mapping. Supply those securities' native bars separately, without adding them to the traded universe |
| `relative_strength` | `rs63` (stock minus SPY), `percentile63`, `sector_rs20`, `sector_rs63`; consumed fields depend on model. Must be prior completed daily values for intraday/hourly decisions |
| `reversal_context` | `aligned_breakout`, `material_repricing_event`; F2 context/combined |
| `fundamentals` | `common_stock`, comparable `sector` (exclude `FINANCIALS`/`REIT`), `assets`; P1. P2 also needs `operating_cash_flow`, positive-outflow `capital_expenditure`, `enterprise_value`, `interest_coverage`, `operating_margin`, `prior_operating_margin` |
| `fundamental_ranks` | `cohort_id`, `coverage_complete`, `total_return_12_minus_1_percentile`, `sector_quality_percentile`, `sector_value_percentile`; P1/P2 as applicable. All percentiles are in [0,1] |
| `earnings_reaction` | `announcement_at`, **pre-announcement** `consensus_available_at`, `actual_eps`, `consensus_eps`, `reaction_session`, `pre_announcement_close`, `eps_basis_compatible`; SW2. Preserve original consensus vintage, original announcement and split-compatible USD/share basis |
| `quote_coverage` | `continuous_from`, `venue`; C1/C2 |
| `microstructure_readiness` | `trades_covered`, `fees_qualified`, `acknowledgements_qualified`, `halted`, `material_news`; C1/C2 |
| `microstructure_model` | `forecast_move`, `roundtrip_cost`, `trained_until`, `loss_budget`, all price deltas in dollars/share; C1 |
| `passive_execution` | `venue_depth_qualified`, `execution_reports_qualified`, `cancel_latency_qualified`, `inventory_units`, `side` (`BUY`/`SELL`), `expected_capture`, `cost_and_adverse_selection`, `trained_until`; C2. Qualification flags must be backed by retained source evidence |

These facts are explicit data contracts, not permission to fabricate passing
booleans. The evaluator validates clocks, validity and required values, but does
not independently certify the exporter, reports, benchmark mappings or forecast
calibration. Retain underlying input IDs and methodology versions for audit.
P1/P2's proposed macro panel is not an entry gate; macro stratification can be
joined to exported episodes without changing their detections.

`fundamentals.build_rank_facts(data, dated_population, decision_at)` computes the
documented momentum, sector quality and sector value ranks from raw point-in-time
reports and total-return bars. It requires the **full dated population**, reports
including `gross_profit`, `net_debt`, `ebit`, `equity_market_value`, and the above
fields. Missing eligible evidence blocks cohort construction, without median
imputation. It returns canonical `fundamental_ranks` facts for inclusion in the
frozen input. Small sector counts are retained for review; this prototype sets
no implicit minimum sector size.

### Quotes

Quotes add `bid`, `ask`, `bid_size`, `ask_size`, `tick_size`, integer `sequence`,
`venue`, and `adjustment_id` to the common observation identity/clocks. Locked or
crossed quotes and nonpositive prices/sizes are rejected. D1/D2 require a quote
no more than five seconds old. C1/C2 require at most 100 ms age and a complete
sequenced best-quote history covering the normalization and decision windows.

OFI is computed from price/size additions and removals at the best quotes;
one-second z-scores use only the preceding 300 seconds (sample standard deviation).
A no-event second is zero OFI only within asserted continuous coverage. Sequence
gaps and zero/unstable variance yield unavailable. Forecast training must end
before the entire current 305-second observation window. C2's depth/queue/fill
evidence and calibrated forecast must be supplied externally: this implementation
does not build a queue simulator or train a fill model. Bars cannot stand in for
any of those records.

## Price measurements and limits

All styles get signed 5/10/21-session close comparisons, with the originating
session counted as day zero. Position also gets 63/126/252-session diagnostics.
Intraday gets 15/30/60/120-minute comparisons anchored to the original completed
trigger bar, requiring an exact native 5m endpoint. Scalping gets 1/5/30-second
midpoint markouts from the decision clock, using the first subsequent quote within
one second of the requested endpoint and retaining its actual timestamp.

`direction * (endpoint_price / original_reference_price - 1)` is a price return,
not an executable return or trading win rate. Publication delay is retained, and
an endpoint already preceding the decision is unavailable. Intraday/event horizons
crossing the session close are not applicable. Missing exact endpoints are
unavailable; future endpoints are pending. A different endpoint adjustment ID
blocks the comparison until a producer supplies a proven common basis. No split
factor, dividend, fill or zero return is invented. P1 momentum uses total return,
while the reported alert-price diagnostics exclude dividends, consistent with
the alert-only contract.

This first implementation recomputes eligible feature prefixes for correctness
under revisions. It is suitable for bounded frozen research batches; large
universe/event archives should be partitioned before use. It is not a low-latency
worker or a claim of production-scale performance. Existing legacy result tables
alone cannot supply these additional features. A historical data export must map
the canonical schema explicitly; there is no automatic adapter that relabels
legacy 30m paper results as five-minute or quote-based tests.

## Verification

```powershell
.\backend\.venv\Scripts\python.exe -m pytest backend/tests/test_equity_research_backtest.py backend/tests/test_equity_model_detection.py backend/tests/test_stock_idea_engine.py -q
```

Tests cover causal revisions, exact endpoints, unavailable/mature populations,
legacy package compatibility, Wilder initialization, native VWAP and comparable
volume, family confirmation/retest paths, session expiry, gap invalidation,
fundamental/earnings availability, quote sequencing and scalping evidence gates.

The broader legacy `test_strategy_v2.py` suite also contains seven tests that
import a database pool at module load. Those require a separately configured
legacy test database; the new offline suite and command do not require credentials.
