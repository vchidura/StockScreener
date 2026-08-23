# Model Registry

Living registry of every model/scanner version constant and its weights, plus which script
updates it and when. A promotion or version bump is a single deliberate code change with a
visible git diff — not a silent runtime mutation. See
[SCANNER_EVENT_EVALUATION.md](SCANNER_EVENT_EVALUATION.md) for how these are validated before
promotion, and [SIGNAL_RESEARCH.md](SIGNAL_RESEARCH.md) for how `xsmom-1.0` was derived.

## How promotion actually works (read this first)

- **What's hardcoded**: only `MODEL_VERSION` / `MODEL_WEIGHTS` (and similar version constants
  below) — plain Python literals in source files, not a database row or config file.
- **How a new version goes live**: a human edits the constant in the `.py` file and commits it.
  There is no admin panel, API, or automatic switch — editing the file *is* the promotion
  mechanism.
- **How "proven" is established first**: run the candidate through `run_alpha_research.py`.
  Promotion is only justified when it prints the verdict `ALPHA` (all 5 gates pass — see below).
  `UNDERPOWERED` / `RISK_EXPOSURE` / `NO_SIGNAL` must not be copied into production constants.
- **`run_alpha_research.py` does not feed `xsmom.py` automatically.** It only logs to the
  audit-only `research_runs` table. Copying a validated weight set into `MODEL_WEIGHTS` is a
  manual, deliberate step — never automatic.
- **Is only one model version ever active?** In production, yes — exactly one `MODEL_VERSION` is
  written daily by the scheduler. But the schema's `UNIQUE (trade_date, ticker, model_version)`
  constraint means a second model could write shadow rows under its own version string
  side-by-side, without conflicting — the same pattern already used for `discovery-1.0-shadow`
  and `extension-0.1-shadow`. Nothing does this today; it's a capability, not current behavior.

## Script responsibilities: what runs when

`run_scheduler.py` is the only automated daily orchestrator. Everything else is manual/on-demand
unless explicitly called by one of its jobs.

| Script | Responsibility | Runs when |
|---|---|---|
| `run_scheduler.py` | Orchestrates all daily jobs below; loops during market hours | **Continuous daemon** (start once, runs all day) |
| `update_daily_prices.py` / `update_hourly_prices.py` / `update_intraday_prices.py` / `update_running_daily.py` | Provider fetch + upsert for daily/hourly/5m/running-candle bars | Called by scheduler jobs every 5-60 min during market hours; not run manually in normal operation |
| `validate_eod.py` | Post-close data-quality check (duplicates, NULL/zero OHLCV, missing/sparse sessions) | Called automatically by scheduler after daily close; can also be run standalone for ad-hoc checks |
| `run_scanner_event_pipeline.py` | Captures new composite-scanner events + evaluates matured outcomes | Called daily by scheduler's `job_scanner_events()` |
| `generate_cross_sectional_signal.py` | Scores the universe with `xsmom.py`'s **current** `MODEL_VERSION`/`MODEL_WEIGHTS`, writes `cross_sectional_signals` | Called daily by scheduler's `job_cross_sectional_signal()` |
| `generate_market_discovery.py` | Persists `discovery-1.0-shadow` / `extension-0.1-shadow` states | Called daily by scheduler's `job_market_discovery()` |
| `run_alpha_research.py` | Validates a candidate cross-sectional feature/weight set against the 5-gate contract (`--model ridge` default or `--model lgbm` for non-linear/tree-based) | **Manual only** — run whenever testing a new signal idea; never automatic |
| `generate_meanrev_signal_shadow.py` | Scores/writes the unvalidated `meanrev-1.0-shadow` model to `cross_sectional_signals` under its own `model_version` | **Manual only** — shadow scaffold, not wired into the scheduler or portal |
| `research_scanner_confidence.py` | Regenerates `scanner_confidence_study.json` — FDR-controlled confidence slices for composite scanners | **Manual only** — rerun after adding new slices or enough new outcome history accrues |
| `scan_trend_pullback_daily.py` / `scan_hourly_trend_pullback.py` | Descriptive watch scans (`UNVALIDATED_TIMING`), not scheduler stages | **Manual only**, per README |
| `discover_universe_polygon.py` | One-time/occasional universe (re)build from Polygon reference data | **Manual only** — run to seed or refresh `selected_tickers` |
| `add_more_tickers.py` / `populate_ticker_metadata.py` | Incrementally expand/enrich the ticker universe from existing DB history | **Manual only** |
| `backfill_polygon.py` | Historical daily/hourly/5m backfill via Polygon REST aggregates | **Manual only** — one-off or occasional deep backfill, not a daily job |
| `compute_opening_pattern_scores.py`, `daily_recommendations_generator.py`, `daily_recommendations_tracker.py` | Opening-pattern scoring / daily recommendation tracking | **Manual or external OS cron only** — not called by `run_scheduler.py` or `main.py` |

## Cross-sectional ranking model

| Constant | Value | Defined in |
|---|---|---|
| `MODEL_VERSION` | `xsmom-1.0` | [research/xsmom.py](../backend/research/xsmom.py) |
| `MODEL_FEATURES` | `["mom_12_1"]` | [research/xsmom.py](../backend/research/xsmom.py) |
| `MODEL_WEIGHTS` | `{"mom_12_1": 1.0}` | [research/xsmom.py](../backend/research/xsmom.py) |
| `HORIZON_DAYS` | `21` | [research/xsmom.py](../backend/research/xsmom.py) |
| `N_DECILES` | `10` | [research/xsmom.py](../backend/research/xsmom.py) |

Production only. Written daily to `cross_sectional_signals` by
[generate_cross_sectional_signal.py](../backend/scripts/generate_cross_sectional_signal.py) via
the scheduler's `job_cross_sectional_signal()`. Every consumer must import `MODEL_VERSION` from
`research.xsmom` rather than hardcoding the string literal (fixed in
[scan_trend_pullback_daily.py](../backend/scripts/scan_trend_pullback_daily.py) — see history for
the drift risk this avoided).

To promote a new weighted feature set, it must first clear all 5 gates in
`run_alpha_research.py` (IC t-stat > 2, IC mean > 0.005, monotone deciles, Sharpe beats
always-long, positive net return) — see [SIGNAL_RESEARCH.md](SIGNAL_RESEARCH.md) §3.

## Mean-reversion shadow model (unvalidated scaffold)

| Constant | Value | Defined in |
|---|---|---|
| `MODEL_VERSION` | `meanrev-1.0-shadow` | [research/meanrev.py](../backend/research/meanrev.py) |
| `MODEL_FEATURES` | `["rev_5", "rsi_14"]` | [research/meanrev.py](../backend/research/meanrev.py) |
| `MODEL_WEIGHTS` | `{"rev_5": 1.0, "rsi_14": -1.0}` | [research/meanrev.py](../backend/research/meanrev.py) |
| `HORIZON_DAYS` | `5` | [research/meanrev.py](../backend/research/meanrev.py) |

**Unvalidated — has not cleared `run_alpha_research.py`'s 5-gate contract.** Demonstrates that a
second model can write to `cross_sectional_signals` under its own `model_version` without
conflicting with `xsmom-1.0`, using the same `UNIQUE (trade_date, ticker, model_version)`
schema capability described above. Written only by manually running
[generate_meanrev_signal_shadow.py](../backend/scripts/generate_meanrev_signal_shadow.py) —
**not** wired into `run_scheduler.py` or any portal-facing endpoint. To promote: validate with
`run_alpha_research.py --features rev_5,rsi_14 --horizon 5` (or `--model lgbm`), confirm the
verdict is `ALPHA`, then update the weights in a reviewed commit and drop the `-shadow` suffix.

## Market discovery / current-position overlay

| Constant | Value | Defined in |
|---|---|---|
| `DISCOVERY_MODEL_VERSION` | `discovery-1.0-shadow` | [research/discovery_states.py](../backend/research/discovery_states.py) |
| `POSITION_OVERLAY_VERSION` | `extension-0.1-shadow` | [research/discovery_states.py](../backend/research/discovery_states.py) |

Shadow only — descriptive, does not gate recommendations. Written by
[generate_market_discovery.py](../backend/scripts/generate_market_discovery.py) to
`market_discovery_states`.

## Composite scanner versions

| Scanner | Version | Status |
|---|---|---|
| `structured_trend_pullback` | `1.0` | Monitor only — raw pass, failed FDR |
| `level_retest_rejection` | `1.2` | Collecting |
| `breakout_expansion` | `1.0` | **Demoted 2026-08-22** — was robust, now monitor only (see below) |
| `compression_breakout` | `0.1-shadow` | Monitor only — raw pass, failed FDR; 88% contained in `breakout_expansion` |
| `failed_breakout_reversal` | `0.1-shadow` | Collecting |
| `structure_reversal` | `1.0` | Collecting |
| `sma200_reclaim_rejection` | `1.0` | Insufficient sample — 31 daily signal days vs 100-event gate |

Defined in `SCANNER_VERSIONS` in
[research/composite_scanners.py](../backend/research/composite_scanners.py). Status column
mirrors [SCANNER_EVENT_EVALUATION.md](SCANNER_EVENT_EVALUATION.md)'s Composite Setup Registry —
update both together.

Retirement, demotion and re-specification rules are precommitted in
[SCANNER_EVENT_EVALUATION.md](SCANNER_EVENT_EVALUATION.md#retirement-and-re-specification).
A revised detection threshold is never edited in place; it ships as a new scanner name and version.

Two diagnostics support that section and should be rerun alongside each study:

| Script | Question | Uses outcomes? |
|---|---|---|
| [analyze_scanner_redundancy.py](../backend/scripts/analyze_scanner_redundancy.py) | Are these detectors independent hypotheses? | No — event identity only |
| [analyze_scanner_power.py](../backend/scripts/analyze_scanner_power.py) | Is a null result "no edge" or "not enough periods"? | No — reuses published study statistics |

## Sector classification

Polygon supplies SIC codes; GICS is licensed by S&P/MSCI and is not available here. `sector` is
**derived**, never fetched:

| Column | Source | Example |
|---|---|---|
| `sic_code` | Polygon `sic_code` verbatim | `2834` |
| `industry` | Polygon `sic_description` verbatim | `Pharmaceutical Preparations` |
| `sector` | Derived from `sic_code` by ordered range table | `Health Care` |

Mapping lives in [research/gics_sectors.py](../backend/research/gics_sectors.py); rebuild with
[reclassify_sectors_gics.py](../backend/scripts/reclassify_sectors_gics.py) (`--dry-run` first).
Specific four-digit carve-outs precede broad major-group fallbacks, so `2833-2836` and `8731`
reach Health Care rather than Materials or Industrials.

Information Technology is split in two. Semiconductors are capex-cyclical and software is
subscription-secular, so demeaning one against the other removes the wrong common factor:

| Bucket | SIC ranges |
|---|---|
| `IT - Software & Services` | 7370-7379 |
| `IT - Semiconductors & Hardware` | 3570-3579, 3661-3699, 3820-3829 |

Foreign private issuers file 20-F, so EDGAR assigns them no SIC and Polygon returns none. The
eight affected tickers are hand-assigned in `MANUAL_SECTORS`; they span five sectors, so a single
"Foreign" bucket would have destroyed real information. ETFs receive `sector = 'ETF'` rather than
an operating sector, keeping baskets out of neutralization, breadth and the sector-performance
page.

As of 2026-08-22, over 350 stocks plus 36 ETFs:

| Sector | Count | Breadth published |
|---|---:|---|
| `IT - Semiconductors & Hardware` | 59 | yes |
| `Industrials` | 54 | yes |
| `Financials` | 48 | yes |
| `IT - Software & Services` | 47 | yes |
| `Health Care` | 33 | yes |
| `Consumer Discretionary` | 31 | yes |
| `Utilities` | 17 | no |
| `Materials` | 16 | no |
| `Consumer Staples` | 16 | no |
| `Energy` | 12 | no |
| `Communication Services` | 11 | no |
| `Real Estate` | 6 | no |

`BREADTH_MIN_SECTOR_SIZE = 25` in
[research/regime_context.py](../backend/research/regime_context.py) suppresses `sector_breadth`
below that size. The breadth slices trigger 0.10 from neutral, and a proportion's standard error
is `0.5/sqrt(n)`, so `n >= 25` is the smallest group where `SE <= 0.10`. The threshold is derived,
not tuned. Undersized sectors emit `NaN` and drop out of `sector_breadth_aligned` and
`market_sector_breadth_aligned` rather than contributing mismeasured values.

## Scanner event evaluation contract

| Constant | Value | Defined in |
|---|---|---|
| `OUTCOME_ENTRY_MODEL` | `next_bar_open_v2` | [research/scanner_events.py](../backend/research/scanner_events.py) |
| `HORIZONS` | `{"1d": (5, 10, 21), "1wk": (5, 10, 21), "1h": (7, 21, 35)}` | [research/scanner_events.py](../backend/research/scanner_events.py) |
| `LOOKBACK_DAYS` | `{"1d": 420, "1wk": 1800, "1h": 60}` | [research/scanner_events.py](../backend/research/scanner_events.py) |

`LOOKBACK_DAYS["1h"] = 60` is the **operational** rolling window the live scanner-event pipeline
actually needs — separate from how much raw `stock_prices_hourly` history is kept for research
backtesting (see repo memory / backfill decisions, currently 730 days).

## Latest confidence study snapshot

From [research/scanner_confidence_study.json](../backend/research/scanner_confidence_study.json)
(regenerate via `scripts/research_scanner_confidence.py`), generated 2026-08-23 after the sector
reclassification:

| Field | Value |
|---|---|
| `entry_model` | `next_bar_open_v2` |
| `rank_overlay.model_version` | `xsmom-1.0` |
| Observations | 293,687 matured outcomes; 87.2% fresh rank coverage |
| Report rows | 1,434 across 41 slices |
| Qualification contract | 100 min events, 40 min independent periods, `t > 2` absolute + incremental alpha, positive early/late alpha required |
| FDR control | Benjamini-Hochberg, `q <= 0.05` |
| Raw passes | 3 primaries, 7 filters |
| Robust primary results | **0** |
| Robust filters | **0** |

Three primaries cleared raw gates and none survived correction. Primary rows do not use sector, so
these are unchanged by the reclassification — a useful consistency check on the rerun:

| Scanner | Interval | Dir | Horizon | Events | Periods | Net alpha | `t` | `q` |
|---|---|---|---:|---:|---:|---:|---:|---:|
| `structured_trend_pullback` | `1d` | short | 5 | 3,629 | 222 | +0.588% | 2.39 | 0.402 |
| `compression_breakout` | `1h` | long | 7 | 678 | 287 | +0.484% | 2.56 | 0.394 |
| `breakout_expansion` | `1h` | long | 7 | 3,360 | 463 | +0.250% | 2.15 | 0.463 |

`breakout_expansion` hourly long at 7 bars was the sole `ROBUST_PASS` of the previous study
(`q=0.0147`). On the full 5-year daily / 2-year hourly sample it falls to `q=0.463`, so its
evidence state reverts and its portal probability and live-alpha claims are suppressed. This is a
demotion under
[SCANNER_EVENT_EVALUATION.md](SCANNER_EVENT_EVALUATION.md#retirement-and-re-specification),
reversible on re-qualification.

Seven filters cleared raw gates and none survived correction. The strongest,
`structured_trend_pullback` `1h` long 21-bar `pullback_vwap_reclaim`, reached absolute `q=0.042`
but has incremental `q=0.856` — it inherits its baseline's edge rather than adding conditional
value, so absolute significance alone does not qualify it. Two filters appeared only after the
sector change, both `rank_*` slices, because sector neutralization feeds `xsmom` percentiles;
both carry `q >= 0.51` and are noise-level passes.

The breadth guard cost less sample than expected: `sector_breadth_aligned` retained 92,226 events
after roughly 22% of stocks were suppressed. Its `q` moved slightly the wrong way
(0.456 to 0.481), which is the expected direction if the earlier value reflected noise rather
than signal.

## Confidence slices

Declared in
[research/scanner_confidence.py](../backend/research/scanner_confidence.py). Both
`confidence_slices()` (row-wise, used by tests) and `expand_confidence_slices()` (vectorized, used
by the study) must declare the same slices — the study path is the vectorized one.

| Slice | Condition | First study result |
|---|---|---|
| `hour_am_open` | `1h`, exchange-local signal hour in `{9, 10}` | 36 rows, best `t=2.67`, `q=0.417` |
| `hour_midday` | `1h`, exchange-local signal hour in `{11, 12, 13}` | 36 rows, best `t=1.77`, `q=0.747` |
| `hour_pm_close` | `1h`, exchange-local signal hour in `{14, 15}` | 36 rows, best `t=1.72`, `q=0.766` |
| `daily_trend_aligned` | `rank_fresh` and long + daily `trend_state == UPTREND`, or short + `DOWNTREND` | 72 rows, best `t=2.59`, `q=0.442` |

Hourly `signal_time` is stored as UTC `timestamptz`; the hour slices convert to
`America/New_York` via `_session_hour()` so the buckets survive DST. Comparing the raw UTC hour
would never match these ranges.

## Research run tracking

`research_runs` (audit-only table, no portal exposure) now also stores:

| Column | Purpose |
|---|---|
| `cal_years` | JSONB, net alpha summed per calendar year |
| `cal_year_positive_pct` | JSONB, % of periods with positive alpha per calendar year |

Written by `persist_run()` in [research/evaluate.py](../backend/research/evaluate.py), populated
by [run_alpha_research.py](../backend/scripts/run_alpha_research.py) to catch regime-concentrated
"alpha" before it's mistaken for a stable edge.
