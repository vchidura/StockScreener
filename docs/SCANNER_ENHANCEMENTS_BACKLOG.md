# Scanner Evaluation Enhancements Backlog

> Archived backlog: implementation references below describe the retired scanner-event ledger.
> Canonical sector-primary evidence superseded that runtime in migration 038. Open ideas require a
> new canonical study and must not restore the deleted scripts or tables.
>
> Status 2026-09-03: items 1 and 2 (ticker breadth, top-5 concentration) were re-implemented in the
> canonical runtime and are stored in `equity_qualification_metrics_v3`. Item 3
> (regime-conditioned alpha) was **not** ported and is absent from the canonical runtime, which
> still returns `regime_alpha` empty. Read the "[Implemented]" markers below as applying to the
> retired runtime only.

Running list of methodology gaps identified during portal review. Items 1, 2, 3, 4, 6, 7, 8, 9, 10,
11, 12 were implemented and backfilled on 2026-08-23 (see "Execution checklist" below for what
shipped and how it was verified). Item 5 remains open for a later pass. See
[SCANNER_EVENT_EVALUATION.md](SCANNER_EVENT_EVALUATION.md) for the current registry and gates this
backlog extends.

## Current state (for reference, confirmed from code)

- **Benchmark used for alpha is not an index, and not the real market.** `_benchmark_return()` in
  `research/scanner_events.py` computes the equal-weight average forward return of *our own tracked
  universe* (all tickers in `stock_prices_daily`/`stock_prices_hourly`), not SPY/QQQ and not the
  actual broad market. It's self-referential: the benchmark is built from the same limited,
  curated ticker list being scanned, not an independent market proxy. It nets out *our universe's*
  drift, which is not the same claim as "beats the real market."
- **Real index and sector ETFs are already tracked, with full history since 2021-08-23** — confirmed
  via `selected_tickers`/`stock_prices_daily`: SPY, QQQ, IWM, DIA for broad market, and all 11
  Sector SPDR ETFs (XLB, XLC, XLE, XLF, XLI, XLK, XLP, XLRE, XLU, XLV, XLY) plus SMH/SOXX
  (semiconductors) and IGV (software) — which map cleanly onto this codebase's IT-Software /
  IT-Semiconductors split in `research/gics_sectors.py`. No new data ingestion needed to switch
  benchmarks.
- **Independent periods already collapse same-instant signals.** `qualification_summary()` groups
  observations by `(scanner, version, interval, direction, horizon, signal_time)` before spacing —
  so 50 tickers firing on the same day count as one period, not 50. Good; not a gap.
- **No visibility into breadth of evidence.** `events` (raw ticker-occurrence count) and
  `independent periods` (time-spaced sample count) are the only sample-size signals surfaced.
  Neither reports how many *distinct tickers*, nor how concentrated the sample is.
- **Stability check is a coarse 2-way split.** Only early-half vs. late-half alpha is compared;
  no breakdown by market regime (bull/bear/choppy).

## Proposed enhancements

> Status 2026-08-23: 1, 2, 3, 4, 6, 7, 8, 9, 10, 11 are implemented. 5 is still open.

1. **[Implemented]** Distinct ticker count per scanner/horizon combo.** Added via
   `_ticker_breadth_stats()` in `research/scanner_events.py`; surfaced as `distinct_tickers` in
   `qualification_report()`.

2. **[Implemented]** Concentration flag.** Same `_ticker_breadth_stats()` computes each scanner's
   top-5-ticker share of events, surfaced as `top5_concentration` and shown next to the Evidence
   badge on Scanner Results.

3. **[Implemented]** Regime-conditioned alpha.** Added `_replay_market_regime()` — a vectorized
   replay of `screeners.analyze_market_regime`'s same SMA20/50/200 + RSI rules, collapsed to
   BULL/BEAR/CHOPPY per calendar day from SPY's full daily history — and `_regime_alpha_stats()`,
   surfaced as `regime_alpha` in `qualification_report()`. Diagnostic only, same as sector alpha; not
   a qualification gate. First real finding from it: `level_retest_rejection` long 5-bar has positive
   alpha in BULL (+0.47%, 155 periods) and BEAR (+0.88%, 39 periods) but **negative in CHOPPY**
   (-0.35%, 15 periods) — a trending-market edge that fails sideways, which the old early/late split
   could not have shown.

4. **[Implemented]** Switch the benchmark to real index/sector ETFs instead of our own universe average.**
   `_benchmark_return()`/`_batched_benchmark_returns()` removed; `evaluate_outcomes()` now computes
   market alpha vs. SPY and sector alpha vs. the mapped Sector SPDR/SMH/IGV ETF for every event, via
   `_etf_forward_return()`. This is a cleaner fix than
   constructing an equal-weight sector average from our own tracked tickers:
   - No minimum-names-per-sector floor needed — an ETF represents the whole real sector by
     construction, regardless of how many names of that sector we happen to track.
   - No universe-imbalance distortion — XLRE reflects the real estate sector as a whole, not our
     lopsided 6-name sample of it (see item 5).
   - Answers the practically meaningful question directly: would you have been better off just
     holding the index/sector ETF instead of trading this signal?
   - Keep the current universe-average benchmark available as a secondary diagnostic (it still
     answers "did this beat our other tracked names"), but it should not be the primary alpha claim.

5. **[Open]** Universe sector-balance monitoring.** Separate from the benchmark switch above: the tracked
   universe itself is currently sector-imbalanced (confirmed 2026-08-23 count: IT-Semiconductors 59,
   Industrials 54, Financials 48, IT-Software 47, Health Care 33, Consumer Discretionary 31,
   Utilities 17, Materials 16, Consumer Staples 16, Energy 12, Communication Services 11, Real
   Estate 6 — roughly a 10x spread). This matters less once item 4 ships (ETF benchmarks don't
   depend on our sample sizes), but is still worth surfacing since it affects which sectors have
   enough of *our own* tracked names for other per-sector features (e.g. Sector Intelligence
   leaders/laggards, discovery-state mix) to be meaningful. Deferred; not part of the 2026-08-23 pass.

6. **[Implemented]** Rerun scope for item 4 (confirmed by tracing the schema, not full history).** Switching the
   benchmark does **not** require re-detecting scanner events, and does not require redoing
   `raw_return`/`signed_return`/`net_signed_return`/`mae`/`mfe`/`first_hit` — those columns in
   `scanner_event_outcomes` are pure price-action and independent of benchmark choice. Only
   `benchmark_return`, `alpha_return`, `net_alpha_return` are persisted using the old benchmark and
   need a one-time backfill UPDATE across the ~294K existing outcome rows. Qualification/confidence
   scoring (Robust/Monitor/Unranked, t-stats) is computed live from the outcomes table on every
   request — it is not itself persisted, so it updates automatically once the backfill lands. No
   separate "rerun confidence scoring" step is needed.

7. **[Implemented]** ETF tickers themselves need an explicit benchmark fallback rule.** Confirmed: all 15 tracked
   index/sector ETFs (SPY, QQQ, and the 13 sector/semiconductor/software ETFs) already have their
   own `scanner_events` rows — the scanners run on them too, and that's a useful, deliberate research
   question in its own right (see below), not just an artifact to work around. Their
   `selected_tickers.sector` is the generic `'ETF'` bucket, not one of the 12 GICS-style labels, so a
   naive sector-name lookup won't accidentally self-benchmark an ETF against itself — but this must
   be an explicit rule, not an accident of the current data shape. Any ticker with `sector = 'ETF'`
   or unclassified should default to the SPY/QQQ broad-market benchmark, matching the existing
   "Unclassified / ETF" bucket already used elsewhere in the Scanner Results UI.
   - Final-review refinement: the fallback must also avoid self-benchmarking the broad-market ETF
     itself. `resolve_benchmark_ticker()` now routes SPY's market and fallback-sector legs to QQQ,
     while QQQ and sector ETFs continue to resolve to an alternate benchmark as needed. The live
     backfill verified zero self-benchmarked outcome rows across all intervals.
   - Why evaluate scanners on the ETFs at all: it answers a distinct question from single-stock
     picking — does this technical pattern work as an index/sector *timing* tool (trade the ETF
     directly), separate from whether it works for picking individual names within that sector? A
     scanner that's Robust on XLK/QQQ directly but not on individual semiconductor names would point
     to a market-timing edge, not a stock-picking edge, which is useful to know either way.

8. **[Implemented]** Entry-timing alignment for the ETF benchmark.** The ETF's forward return must be measured with
   the same `next_bar_open_v2` entry convention and the same horizon-bar count as the stock's own
   outcome (not a close-to-close shortcut), or the comparison quietly reintroduces a look-ahead
   mismatch between the two legs of the alpha calculation.

9. **[Implemented]** Decide: one alpha number or two.** Decided and shipped: both. Market alpha (vs. SPY) remains
   the Robust/Monitor/Unranked gate; `mean_sector_alpha`/`sector_alpha_t_stat` (vs. the sector ETF) is
   surfaced as an additional diagnostic in `qualification_report()` and next to the Evidence badge on
   Scanner Results, not a second gate.

10. **[Implemented]** Hourly coverage checked — not actually a gap.** All 15 ETFs have hourly bars only back to
    2024-08-21, but confirmed `stock_prices_hourly` has no data before that date for *any* ticker
    (hourly collection started system-wide then) and the earliest hourly scanner occurrence is
    2024-08-22. So ETF hourly history already covers 100% of the hourly scanner event history that
    exists. No backfill gap for hourly; item 4 can apply to hourly the same as daily/weekly.

11. **[Implemented]** Broad-market instrument choice: SPY + QQQ confirmed sufficient; DIA/IWM confirmed unnecessary.**
    Checked `selected_tickers.market_cap_group` for the tracked universe (excluding ETFs): Large 275,
    Mega 59, Mid 16, **zero** Small/Micro. The universe has no small-cap cohort, so IWM (Russell 2000)
    would have nothing to meaningfully benchmark. DIA (30 mega-caps, price-weighted) is highly
    correlated with SPY's own mega-cap constituents and doesn't represent a distinct segment worth
    the extra column. SPY (broad market) + QQQ (large-cap growth/tech, relevant given IT-Software +
    IT-Semiconductors are ~27% of the universe) are enough, and match the pair already used by the
    existing Market Regime feature (`analyze_market_regime`), so no new plumbing is needed.

12. **[Implemented, 2026-08-23]** Weekly (`1wk`) scanner coverage was empty; backfilled.** Confirmed
    zero rows in `scanner_events` with `interval = '1wk'` prior to this. Ran
    `python scripts/run_scanner_event_pipeline.py --interval 1wk --qualification-start 2021-08-23`,
    then drained remaining outcome evaluation with
    `--evaluate-only --drain-outcomes --evaluation-limit 5000`. Result: 15,427 weekly lifecycles
    captured, 45,724 outcome rows evaluated (99.5%/99.2%/97.7% complete across the 5/10/21-session
    horizons; the remainder are simply too recent to have matured yet, same as daily/hourly always
    carry some pending).
    - **Correction to the original caveat below**: the 5/10/21 horizon for `1wk` is measured in
      trading *days* ("sessions" per the existing UI label), not weeks — a 21-bar weekly horizon is
      ~1 month of trading days, not 5 months of calendar weeks. So the periods ceiling is **not**
      ~12 as originally estimated; observed independent-periods after the backfill range up to 208
      (e.g. `level_retest_rejection` short 5-bar), and several combos already clear 40+ periods even
      at the 21-bar horizon (`level_retest_rejection` 45-51, `breakout_expansion` 45,
      `failed_breakout_reversal` 48, `structured_trend_pullback` long 40).
    - **Qualification result**: 39 combinations, 0 Robust, 0 Monitor, 39 Unranked — nothing qualifies
      yet, but one genuine near-miss: `structured_trend_pullback` short, 21-bar has `t=1.94` (just
      under the >2 gate) with 38 periods (just under the ≥40 gate) — both gates barely missed, a
      real "not yet" result rather than a structural sample-size wall.
    - Original caveat text (kept for the record, since it was wrong about the mechanism): "with ~5
      years of history (~260 weeks), the periods gate (≥40) would only be reachable for the 5-week
      horizon; 10/21-week horizons could never reach 40 periods regardless of ticker count." This
      assumed horizon_bars meant calendar weeks; it does not.

## Suggested batching

Items 1-2 are cheap (query additions, no new tables) and could ship together first. Item 4 (ETF
benchmark switch) is now the recommended way to get sector-neutral alpha — simpler than originally
scoped, since the ETFs are already tracked. Items 3 and 5 are secondary and can follow once 1-2-4
are validated against real qualification output.

## Execution checklist (2026-08-23 — items 1, 2, 3, 4, 6, 7, 8, 9, 10, 11; 5 deferred)

- [x] `research/gics_sectors.py`: `SECTOR_BENCHMARK_ETF` map + `BROAD_MARKET_ETF` + `sector_benchmark_ticker()` (item 4, 7, 11)
- [x] Historical migration 014, now consolidated in `000_canonical_schema.sql`: add `market_benchmark_ticker`, `sector_benchmark_ticker`,
      `sector_benchmark_return`, `sector_alpha_return`, `sector_net_alpha_return` to `scanner_event_outcomes`,
      plus column comments documenting the definition change (item 4, 9). Applied to the live DB.
- [x] `research/scanner_events.py`: removed `_benchmark_return`/`_batched_benchmark_returns` (universe-average),
      added `_etf_forward_return()`, rewrote `evaluate_outcomes()` to compute market (SPY) + sector (mapped ETF,
      SPY fallback for ETF/unclassified tickers) alpha per due event, using the same next-bar-open/horizon-bar
      convention for the ETF leg as the stock leg (item 4, 6, 7, 8, 9)
- [x] Compile-checked `research/scanner_events.py`, `research/gics_sectors.py` — clean
- [x] Applied migration 014 against the live DB — verified all 5 new columns present
- [x] Wrote `scripts/backfill_scanner_benchmark.py`: recomputes `benchmark_return`/`alpha_return`/
      `net_alpha_return` (market) and the new sector_* columns in place, deriving cost from the existing
      `signed_return - net_signed_return` rather than rejoining scanner_events for it; does not touch
      `raw_return`/`signed_return`/`net_signed_return`/`mae_pct`/`mfe_pct`/`mae_r`/`mfe_r`/`first_hit` (item 6)
- [x] Ran the backfill against the live DB: daily 209,455 outcome rows updated (70,201 events), hourly
  84,232 rows updated (28,210 events), and weekly 45,724 rows updated (15,344 events); all intervals
  have 100% market/sector benchmark population.
- [x] `qualification_summary()`/`qualification_report()`: added `distinct_tickers`, `top5_concentration`
      (new `_ticker_breadth_stats()` helper), and `mean_sector_alpha`/`sector_alpha_t_stat` (item 1, 2, 9).
      Verified live: e.g. `breakout_expansion` short/5d shows 383 distinct tickers, 2.4% top-5 concentration.
      Market alpha remains the sole Robust/Monitor/Unranked gate; sector alpha is a diagnostic only.
- [x] `services/api.ts`: added the four new fields to `ScannerQualificationRow`
- [x] Minimal UI: Scanner Results now shows "`N` tickers (`X`% top-5) · sector α `Y`%" under the Evidence
      badge for Robust/Monitor rows
- [x] Spot-checked the ETF fallback rule: QQQ's own events resolve to SPY, and the final review found
  and fixed the SPY edge case (`BROAD_MARKET_ETF == 'SPY'`) with `ALTERNATE_MARKET_ETF == 'QQQ'`;
  the live database now reports zero self-benchmarked rows.
- [x] Deleted all temporary check scripts (`_tmp_*.py`) used during verification
- [x] Re-ran qualification live after the item 4/6/7/8/9 backfill to confirm the pipeline end to end:
      daily 39 combos (0 Robust / 2 Monitor / 37 Unranked), hourly 36 combos (0 Robust / 5 Monitor /
      31 Unranked) — 7 Monitor combos total, consistent with the UI. Found one real, actionable case:
      `level_retest_rejection` long daily 21-bar has market α t=2.14 but sector α t=1.67 (below the >2
      bar), meaning roughly a third of its apparent edge is sector rotation, not stock-specific timing.
- [x] Added `_replay_market_regime()` + `_regime_alpha_stats()` (item 3); verified live — see item 3
      above for the concrete finding (`level_retest_rejection` long 5-bar fails in CHOPPY regimes)
- [x] `services/api.ts`: added `regime_alpha` to `ScannerQualificationRow`; `tsc --noEmit` clean
- [x] Marked items 1, 2, 3, 4, 6, 7, 8, 9, 10, 11 done above; 5 remains open, and item 12 (weekly/`1wk`
  coverage) is complete after the weekly event/outcome backfill
- [x] Re-ran confidence evaluation after the benchmark refresh: 339,411 observations across `1d`, `1h`,
  and `1wk`; 2,091 report rows; 7 qualified primary scanners, 12 qualified confidence filters, and
  0 robust classifications. Final integrity review confirmed all five benchmark columns, complete
  benchmark population, and zero self-benchmarked rows.
- [x] Final methodology review kept the implemented FDR-family rule unchanged: every predeclared
  row enters correction, while 100 events and 40 independent periods remain raw-qualification
  gates only. Corrected the conflicting exclusion text in the evaluation/literature documents;
  no outcome code or stored study result changed.
- [x] Clarified scanner-version ownership: promotion/demotion changes evidence status only. A
  detector version changes only with signal semantics, and a revised detector uses a new scanner
  name and version, so historical evidence cannot be inherited by a revised hypothesis.

## Future enhancements roadmap (post-2026-08-23)

This section compiles the next pass of work after the ETF benchmark migration and weekly backfill.
The highest-impact question raised in review is whether daily/weekly scanners should emit actionable
signals intraday instead of waiting for close-confirmed EOD capture.

### Is provisional intraday daily/weekly worth implementing?

Yes, with strict separation from close-confirmed events.

- Why it is worth doing:
  - Improves action latency for day-trading and tactical execution when a daily/weekly setup starts
    forming before the close.
  - Enables a measurable lead-time experiment: provisional signal hit rate vs. close-confirmed
    signal quality.
- Why it must be isolated:
  - Intraday daily/weekly values can repaint before close; mixing them into the same lifecycle/outcome
    stream as close-confirmed events would contaminate qualification statistics.

### Priority 1: Provisional intraday signal lane (no data mixing)

- [ ] Add provisional interval lanes for in-session actions:
  - `1d_rt` (forming daily bar), and optionally `1wk_rt` (forming weekly bar)
- [ ] Keep canonical close-confirmed lanes unchanged:
  - `1d`, `1h`, `1wk` continue as qualification truth for Robust/Monitor/Unranked.
- [ ] Split identity keys so provisional and confirmed events never collide:
  - Include lane in event identity (for example scanner interval key contains `_rt`).
- [ ] Persist provenance fields on event/outcome rows:
  - `is_provisional` boolean, `confirmation_status` enum (`PROVISIONAL`, `CONFIRMED`, `CANCELED`).
- [ ] Add explicit confirmation step at close:
  - Mark provisional signals that survive to close as confirmed; mark disappeared setups canceled.

### Priority 2: Scheduler and runtime flow hardening

- [ ] Add configurable scanner cadence per lane in scheduler:
  - `1h` intraday hourly (already active), `1d_rt` every 15 minutes during market hours,
    `1wk_rt` every 30-60 minutes.
- [ ] Add lane toggles via environment/CLI flags:
  - Example: enable/disable `1d_rt` and `1wk_rt` independently.
- [ ] Add strict market-hours guards for provisional lanes only.
- [ ] Add cooldown and backlog protection:
  - Skip new captures if previous lane run still in flight; avoid overlap under load.

### Priority 3: Outcome model separation and analytics integrity

- [ ] Evaluate provisional outcomes in a separate model namespace:
  - Do not mix with qualification tables used for close-confirmed confidence gates.
- [ ] Add side-by-side diagnostics:
  - Provisional vs. confirmed win rate, alpha, false-positive (canceled) rate, and lead-time gain.
- [ ] Add migration and replay tooling:
  - Backfill confirmation flags for historical sessions once schema is in place.

### Priority 4: API/UI clarity for actionability

- [ ] Surface lane labels in API responses:
  - Show whether signal is `PROVISIONAL` or `CONFIRMED` and its source lane (`1d_rt`, `1d`, etc.).
- [ ] Add default UI filter to show only confirmed unless user enables provisional mode.
- [ ] Add confidence banner for provisional rows:
  - "Forming bar signal; may invalidate at close".

### Priority 5: Observability and safety controls

- [ ] Add lane-level metrics and logs:
  - captured/evaluated counts, due backlog, run duration, and failure rates per interval lane.
- [ ] Add invariant checks:
  - no cross-lane event_id collisions, no provisional rows in confirmed-only qualification queries.
- [ ] Add canary mode:
  - run provisional lanes on a small ticker cohort first before full-universe enablement.

### Priority 6: Existing open item retained

- [ ] Item 5 from this backlog remains open:
  - Universe sector-balance monitoring for Sector Intelligence interpretability.

### Suggested rollout order

1. Schema + identity isolation (`is_provisional`, lane-specific keys, confirmation status)
2. Scheduler flags + `1d_rt` lane (canary ticker set)
3. API/UI labels + confirmed/provisional filters
4. Metrics + invariants + broader rollout
5. Optional `1wk_rt` lane after `1d_rt` stabilizes
