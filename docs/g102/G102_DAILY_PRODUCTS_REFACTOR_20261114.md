# G102 R78: daily products after the close (Dashboard, strategies and daily frames out of the Equity window)

- **Status:** DESIGNED, Nov 14 2026. The user approved the design; the build happens in a separate session; any deploy
  needs the user's go.
- **Evidence:** `G102_EVIDENCE.md`, the entries from "R75 decision and R75-v2 build" to "Pattern Watch and the other Equity
  pages".
- **Matrix:** row R78.
- **Related:** R73 (cold windows), R77 (the close recovery), R75-v2 (independent of this work).

## 1. Decisions (user, Nov 14)

1. The Dashboard and the daily strategies leave the Equity window. A "daily products" job builds them after the close
   and again before the first window.
2. Hourly strategies are not built for now. The 1h strategy views stay unavailable, as today.
3. The Dashboard page will be redesigned later as a lightweight page composed from whatever products the platform
   persists by then. Until then, its current document shape is kept, produced by the job.

## 2. Current state (measured, 45 names, Nov 13 data)

| Item | Today |
|---|---|
| Dashboard build | Inside PORTAL_PUBLISH. Full reload through a second loader slot (57-60 s, 5.2 GB peak; the scheduler keeps 2 copies of all bars), then a 20.9 s build: screening re-run 11.0 s, daily setups 5.0 s, the six scanners 0.6 s. Published in the same transaction as the ticker workspaces, so a Dashboard error fails the window (G65, R39). |
| Dashboard cadence | A process's first window, every cold window, and the close: Nov 12 twice, Nov 13 eleven times, PORTAL_PUBLISH 87-119 s each (~90 s added to a cold window). Intraday builds carry the previous session's daily analysis, stamped with the window time. |
| Strategy scanner pages | `/api/strategies/daily` returns 503 (no strategy publication exists; STRATEGY_PUBLISH is not in the live graph), so the pages fall back to the Dashboard (`/api/latest-price-date` and `/api/scan/<strategy>`). |
| Stock Persistence, Strategy Overview | Only `/api/strategies/daily`, so unavailable on live. |
| Dormant strategy producer | `materialize_due_realtime_strategies`: a full load per run (60 s, 6.2 GB). Its validation fails on live data ("split/action coverage is unavailable or inconsistent"). With validation stubbed: daily 33.5 s (scanners 0.84 s, setups 7.1 s, ~25 s of per-member scans of every bar). |
| Pattern Watch | Rows come from the workspaces; collecting them takes 0.01 s, published every window. Per-ticker work: ~24 s cold, ~0.2 s warm, ~5-10 s when a 30m or 1h bar completes (estimate), ~40 s at the close. Each pattern run uses the full evidence pipeline. |
| Market Conditions, Sector Intelligence, Market Regime | Built at request time from the workspace summary: 0.34-0.46 s per read. |

## 3. Target design

### 3.1 The Equity window (every 5 minutes)

- Publishes ticker workspaces, the all-tickers summary and Pattern Watch. No Dashboard, no strategies.
- Uses one loader slot, so the scheduler retains one copy of the bars. `dashboard_due` and the R39 switch are retired.
- Pattern Watch:
  - 30m and 1h stay in the projection, because the ticker page uses them too, but they run pattern and price-channel
    detection only, not the full evidence pipeline.
  - The publication is written only when its rows changed (a 30m or 1h bar completed, or the close).
  - 1d, 1wk and 1mo come from the daily products job (phase 5).
- Market Conditions, Sector Intelligence and Market Regime stay request-time views of the summary.

### 3.2 The daily products job

- **Runs:**
  - **After the close:** once the session's close window is COMPLETE. That proves the adjusted and raw daily bars are
    stored for every member and the close screening generation is persisted.
  - **Before the first window:** once the new session's cohort is active (after activation, ~14:15 UTC, before E:14:35).
    It publishes under the new cohort, because publications are per cohort, and it applies overnight corporate actions
    and coverage. If the inputs are unchanged, the content is unchanged.
- **Execution:**
  - A durable job keyed by (cohort, session, product set, input identity): idempotent, with its own deadline and bounded
    retries (`ALREADY_STORED` exists in the strategy producer).
  - It runs in a separate process with CPU and memory limits, never inside a window's budget.
  - A late job never replaces a newer publication (generation and session checks).
  - Close completeness is tracked per product.
  - A failure leaves the previous publications served and raises its own alert. Windows are never affected.
- **Inputs (narrow):**
  - daily bars: 283 adjusted sessions and 20 raw;
  - the last 20 sessions of 1h bars for setup confirmation;
  - members, actions and coverage;
  - the persisted close screening generation (`rt.screening_current`; the screening is not re-run).
  
  Bars are grouped by ticker once.
- **Outputs (published by a short publication transaction with a release entry):**
  1. `DAILY_STRATEGIES_V1`: the six scanners with coverage. Strategy Overview and the six strategy pages read it
     through `/api/strategies/daily`, which the frontend already prefers.
  2. `DAILY_STRATEGY_ROLLING_V1`: the latest session plus retained history, for Stock Persistence and the history
     views. It is assembled from retained publications, which is cheap.
  3. `DASHBOARD_V1`: same shape as today (overview, scan, scanners, setups, context, regime). Its screening and context
     come from the persisted generation, and its scanners and setups from the strategy step.
  4. Phase 5: Pattern Watch's 1d, 1wk and 1mo frames.
- **Target time:** seconds at 45 names, under a minute at 400. Memory target: under 1 GB.

### 3.3 Readers

- `/api/strategies/daily` serves the job's publications.
- `/api/scan/<strategy>` and the Dashboard routes keep their JSON shapes, reading the job's outputs.
- The frontend's Dashboard fallback stays until the page redesign.
- Unchanged frontend contracts: `checkedDailyStrategyPublication` (release type, interval, cohort, session, snapshot
  id) and the coverage checks in `getDailyScannerPublication`.

## 4. Implementation plan (each phase behind a switch, unset = today's behaviour)

Switch: `REALTIME_V2_DAILY_PRODUCTS=JOB`. Unset keeps the Dashboard in the window, as today. The switch is read by the
Equity scheduler (and the window code) and by the job.

| Phase | Work | Main touch points |
|---|---|---|
| 1. Narrow input | A daily-products loader: daily plus 20 sessions of 1h, grouped by ticker, with no full-window load. Decide how 1h lineage is accepted (see section 7). | `screening_repository.py` (a new bar scope variant or a dedicated query), `screening_delta_loader.py` |
| 2. Strategy producer | Revive `materialize_due_realtime_strategies` for `1d`: the narrow input; validation accepts the coverage refreshed before the open (with the coverage rule documented); per-member scans of every bar become grouped lookups (`_validate_strategy_inputs`, `produce_daily_setup_summary` already grouped); rolling history from retained publications. | `strategy_repository.py`, `strategy_projection.py`, `setup_summary_projection.py` |
| 3. Dashboard document from the job | Build `DASHBOARD_V1` from the narrow input, the persisted screening generation and the strategy step's scanners and setups (no second screening). The manifest identity uses the narrow input's lineage. | `dashboard_projection.py`, `dashboard_repository.py` |
| 4. Job and trigger | A durable job with retries and a deadline, run in a limited process. Triggers: close window COMPLETE; cohort active before the first window. Alerting through the fault journal. | `run_realtime_v2_advanced_equity_scheduler.py` (IDLE/BEFORE_OPEN hook, as `_recover_completed_session_window`), a new job module, and the job table or window-run reuse (see section 7) |
| 5. Window change | With the switch set, PORTAL_PUBLISH skips the Dashboard, the second loader slot is never created, and `dashboard_due` is unused. Pattern Watch's 30m and 1h use pattern-and-channel-only computation; the publication is written only when its rows change; 1d, 1wk and 1mo come from the job. | `stage_runners_equity.py` (`portal_publish_runner`), `run_realtime_v2_advanced_equity_window.py`, `ticker_projection.py` |
| 6. Readers | `/api/strategies/daily` resolves the job's release; adapters for `/api/scan/*`. Stock Persistence and Strategy Overview become available. | `strategy_repository.RealtimeDailyStrategyRepository`, `portal_api.py`, `dashboard_repository.py` |

Order: 1, 2, 3 and 4 can be built and rehearsed without touching the window. Phase 5 is the only window change, and
it ships with the switch. Phase 6 follows phase 2.

## 5. Tests

- **Unit tests:**
  - narrow loader scope;
  - grouped lookups equal the old per-member scans;
  - the coverage rule;
  - job idempotency (ALREADY_STORED), retry and deadline;
  - a late job cannot replace a newer publication;
  - the window with the switch set publishes no Dashboard and holds one loader slot;
  - Pattern Watch is written only on change;
  - pattern-only computation produces the same pattern and channel rows as the full pipeline.
- **Existing suites:** dashboard, strategy, pattern watch, ticker projection, stage runners. The 12 failures that already
  fail on live r116 code stay the baseline.

## 6. Rehearsal gates (read-only on live, outside 14:00-21:10 UTC, memory-capped, worktree mounted on the live image)

1. The job on Nov 13's close inputs reproduces the stored Nov 13 close Dashboard's six scanner lists row for row
   (33 gaps, 521 FVG, 45 MA, 4 momentum, 4 bearish, 43 Fibonacci), plus its overview and setup rows. Any difference is
   explained, for example the screening read from the persisted generation.
2. Run time and peak memory are recorded. Targets: seconds, under 1 GB.
3. A pre-open run for a new cohort produces publications that pass `checkedDailyStrategyPublication` and the scanner
   coverage checks. Stock Persistence and Strategy Overview render.
4. Equity windows with the switch set (replayed, read-only): a cold PORTAL_PUBLISH takes seconds, one copy of the bars
   is retained, and the workspaces equal the switch-off run.
5. Pattern Watch: pattern-only rows equal the full-pipeline rows, and it is published only on change.
6. A forced job failure: the previous publications stay served, an alert is raised, and windows are unaffected.

## 7. Open decisions for the build session (recommendation first)

1. **Where the job runs:** a subprocess launched by the Equity scheduler in its IDLE/BEFORE_OPEN phase, with
   `RLIMIT_AS` and CPU limits (no new compose service). The alternative is its own fenced service.
2. **Job durability:** reuse the window-run machinery with a dedicated key (receipts, retries and fencing exist, as for
   the hybrid repair windows). The alternative is a new `materialization_jobs` kind, which needs a migration.
3. **1h lineage for setup confirmation:** accept 1h bars by the lineage the projection already verified, without
   re-reading 5m and 1m sources. The fallback is loading the closure (~25% of the bars, ~15 s), which is acceptable
   after the close.
4. **Strategy release resolution:** confirm how `RealtimeDailyStrategyRepository` resolves the current publication
   (release entry or retained snapshot), and publish accordingly.
5. **Pattern Watch daily frames:** move them in phase 5, or keep them in the projection (they cost nothing on warm
   windows) until the window change.

## 8. Rollout

- Build a candidate (`runner/build_candidate.sh`) only on the user's go. Rehearse with the worktree mounted on the
  live image first.
- Deploy outside the session, on the user's go:
  1. the job and readers with the switch unset;
  2. one after-close run observed;
  3. the switch set before an open.
- Fallback: unset the switch (Dashboard back in the window).

## 9. Not in scope

- **Hourly strategies.** Prerequisites when wanted:
  - a warm {1d, 1h} input in the job process;
  - the projection's per-ticker 1h setup evidence reused (36 s re-materialized today);
  - scanner rows per ticker;
  - one job per completed 1h slot.
  
  Measured as built: ~150 s per run. Expected after: seconds.
- **The Dashboard page redesign:** later, from persisted products (workspaces, summary, Pattern Watch, screening, daily
  strategies, and per-ticker rows with publication manifests: R78's read model).
- **R75-v2 and the wider scaling review:** `REVIEW_PROMPT_SCALING_20261114.md`.
- **30m and 1h Pattern Watch outside the window:** not needed; the warm projection already refreshes them when their
  bars complete.
