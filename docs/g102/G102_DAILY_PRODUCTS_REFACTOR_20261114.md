# G102 R78: daily products after the close (Dashboard, strategies and daily frames out of the Equity window)

- **Status:** LIVE since Nov 14 23:16 UTC (user's go), from worktree `r78-daily-products` (branch
  `r78-daily-products` from `v3-live-fixes` ea76dd5). Committed `f0f722d` on `v3-live-fixes` and `main` (Nov 14
  23:20, user's word); r118's source is branch `r118-api-reader-fix` (`8d6e23d`).
  - Scheduler r119 with `REALTIME_V2_DAILY_PRODUCTS=JOB`, `REALTIME_V2_PATTERN_WATCH=LIGHT` and
    `REALTIME_V2_DAILY_STRATEGY_SESSIONS=1`.
  - API r118 (r102 + the strategy reader fix).
  - Decisions: section 7. Gate results: section 6. Deploy and live check: section 8 and the evidence.
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

**As built (Nov 14, worktree `r78-daily-products`, branch `r78-daily-products` from `v3-live-fixes` ea76dd5; not
committed, not in any image):**

| Phase | Built | Code |
|---|---|---|
| 1 | `load_daily_products_input` / `select_daily_product_bars`: one DISTINCT ON read of adjusted 1d (the portal plan's sessions), raw 1d (90 sessions) and raw 1h (20 sessions) with the window's columns and source precedence; no 1m-30m, no 1wk/1mo, no source closure (decision 3; `source_closure=True` reads it). | `screening_repository.py`, `screening_projection.py` (`source_lineage`) |
| 2 | Per-member scans grouped once (`_validate_strategy_inputs`, the history truncation, the receipts loop); the manifest's `input_sha256` uses the memoized digest; strategy evidence and snapshot persistence split (`write_strategy_evidence`, `persist_strategy_snapshot_cursor`). Product DAILY_STRATEGY_HISTORY_V1 (decision 4). The coverage rule is unchanged and holds on live data (section 6, gate 3). Optional worker pool for the per-member setup evidence (`REALTIME_V2_DAILY_PRODUCTS_WORKERS`, default 1). | `strategy_projection.py`, `strategy_repository.py`, `setup_summary_projection.py` |
| 3 | `produce_realtime_dashboard(inputs, setups=...)` reuses the latest strategy evaluation's setup rows when they cover exactly its members; the source-clock check and `dashboard_inputs` honour accepted lineage. The screening is computed from the narrow input (the persisted generation is not usable for a new cohort: its current generation is an intraday one). | `dashboard_projection.py` |
| 4 | `realtime/daily_products.py` (compute, ALREADY_STORED by input identity, supersede check, one publication transaction attached to the current release), `scripts/run_realtime_v2_daily_products.py` (the limited process; `--dry-run` read-only), `realtime/daily_products_launcher.py` (due work, spawn, poll, retries, deadline, faults), scheduler hook (one call per loop pass). | as named |
| 5 | JOB: `dashboard_due` is never true and PORTAL_PUBLISH never computes the Dashboard (also not at the close), so the second loader slot is never opened. Pattern Watch 30m/1h light frames behind `REALTIME_V2_PATTERN_WATCH=LIGHT` (decision 5). "Publish only on change": not built (decision 5). | `run_realtime_v2_advanced_equity_window.py`, `stage_runners_equity.py`, `ticker_projection.py` |
| 4b | Prepared before the open: PRE_OPEN trigger (`--prepare`: compute, evidence, stored snapshots, no release), publish-only SESSION_COHORT (`prepared_daily_products_cursor`), cutoff = max(now, effective, created), a check after every window, `REALTIME_V2_DAILY_PRODUCTS_DEADLINE_SECONDS` (default 600). | `daily_products.py`, `daily_products_launcher.py`, scheduler |
| 6 | No reader change needed: `/api/strategies/daily` resolves the job's entry from the current release; `/api/scan/*` and the Dashboard routes read the job's DASHBOARD_V1 (same document). Verified through the real readers in gate 3. | none |

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
- **Result (Nov 14, final code):**
  - `tests/realtime/test_daily_products_r78.py`: 51 passed (including the pre-open prepare and publish-only path and
    the Pattern Watch light frames).
  - The 25 touched suites: 916 passed, 10 failed. These are exactly the 10 that fail on ea76dd5 on the same files (2
    daily-scanner, 1 all-tickers, 4 Pattern Watch, 3 ticker vertical slice), with the same errors.

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

**Results (Nov 14 21:07-21:33 UTC, read only, worktree on the r116 image `523cae4a0174`; evidence "R78 build and
rehearsal"):**

| Gate | Result |
|---|---|
| 1 | **PASS.** Nov 13 cohort at its close vs the stored close Dashboard (`3a3533c0`, release cutoff 21:00): all six scanner lists equal row for row in order (33 / 521 / 45 / 4 / 4 / 43), scan lists equal, overview 45 = 45, setup rows 45 = 45, context daily values 45 = 45, members, unavailable, missing producers, basis equal; only `scan_datetime` differs (the clock). The strategies' latest evaluation gives the same six lists. |
| 2 | **Memory PASS, time NOT MET.** Narrow load 4.4-9.3 s (23,085 bars); peak RSS 516-552 MB, VmPeak 847-967 MB (3 GiB address-space limit; BLAS threads 1, else VmPeak 1.7 GB). Compute 94-100 s in one process (2 CPUs): five reconstructed sessions x 45 members of the per-member setup evidence pipeline dominate (~70%); Dashboard 9-12 s with the setups reused. With 4 setup workers: 65 s. "Seconds" needs fewer per-member pipelines per run (section 9). |
| 3 | **PASS.** The Nov 13 cohort building the Nov 12 session at 14:45 UTC (its first window was E:14:40): history over Nov 6-12, 45 members, none unavailable; read back through `RealtimeDailyStrategyRepository` and `RealtimeDashboardRepository` (served snapshots after a jsonb-like round trip, every other query on live) and a Python port of the frontend checks: the five strategy pages, Strategy Overview (`view=evaluations`), Stock Persistence (270 rows = 45 x 6, 1,350 evaluated cells, 5 sessions) and the 11 Dashboard contracts all accepted; status STALE (read on Nov 14 for the Nov 12 session), as it should be. |
| 4 | **PASS.** E:20:45 (cold), 20:50, 20:55 replayed with the switch JOB (known_by Nov 14 02:00): no Dashboard due (cold, close), one loader slot throughout (742,693-743,233 bars). Every fingerprint (45 workspaces, 225 materializations, charts, screening, summary, Pattern Watch) equals the unchanged code (ea76dd5) with the switch unset, once the screening's own code hash (`projection_code_sha256`, which stamps `screening_projection.py`) is mapped to the old file; without that mapping the screening identity differs, as with any change to that file. The unchanged code also equals the R75-v2 02:00 run (`serial.json`) exactly (the replay is deterministic). Cold PORTAL_PUBLISH with the switch (live Nov 13): 0.9-2.2 s (46 windows without the Dashboard) instead of 87-119 s (10 windows with it). |
| 5 | **PASS (light frames; "publish only on change" not built, decision 5).** E:20:55, all 45 members, in one process: the full pipeline vs `REALTIME_V2_PATTERN_WATCH=LIGHT`. Every workspace's patterns, channels and setups are equal, and every Pattern Watch view (30m 11 rows, 1h 15, 1d 7, 1wk 11, 1mo 14) has equal rows, market time and status; 30m/1h become RELEASE_ONLY (0 receipts instead of 45). Pattern evidence runs 225 -> 135, evidence rows 2,659 -> 2,044 (-23%), cold projection compute (load excluded) 87.7 s -> 79.6 s. |
| 6 | **PASS.** A forced failure (session not complete at the cutoff) exits 1 in 0.7 s and writes `FAULT DAILY_PRODUCTS_FAILED` (ERROR, PREVIOUS_PUBLICATIONS_STAY_SERVED); nothing is written. Unit tests: a failure inside the publication transaction leaves no release; the launcher records DAILY_PRODUCTS_ATTEMPT_FAILED, retries 3 times 5 minutes apart and gives up with DAILY_PRODUCTS_GAVE_UP; it kills a run past its deadline; it never raises into the scheduler loop. |

**Candidate r117 (Nov 14 22:33 UTC)**:
- Image `alpha-research-realtime-v3:stable-nov5-r117-229f1e60d7c4cc47` = `sha256:f7d9fc75a520...`, built from this
  worktree with `runner/build_candidate_from.sh` (the `build_candidate.sh` checks: image code == worktree, 969 files;
  role imports).
- Rehearsed read-only on the image's own code:
  - the job through the service wrapper (`run_realtime_v2_container.py --component scheduler --health-probe`, the
    scheduler's environment and secret, the launcher's limits): COMPUTED in 39.5 s, VmPeak 849 MB;
  - gates 1 and 3 again: PASS, 40-41 s each;
  - the pre-open dry run's `workspace` step with JOB and LIGHT: PASS (45 workspaces, 0 unavailable, largest payload
    109 KB of 128 KiB). Its `readiness` step reports the capability profile not yet refreshed for Nov 16, the same on
    the live image: live state before the pre-session refresh, not the image.

## 7. Decisions (build session, Nov 14; were the open decisions)

The recommendations as written in the design are kept in the history of this file. Each decision below cites the code
or the live data it rests on.

1. **Where the job runs: a separate process spawned by the Equity scheduler, which limits itself.** No new compose
   service.
   - The scheduler runs every window in its own process (`run_window` is the window script's `main()`), so the job is
     started without blocking (`subprocess.Popen`) between windows and polled on each loop pass
     (`realtime/daily_products_launcher.py`).
   - No `preexec_fn`: a fork-time hook would copy the scheduler's address space (it holds the warm bars; the R75 fork
     pool did that on Nov 13). The child applies its own limits at start (`scripts/run_realtime_v2_daily_products.py`):
     nice 10, `RLIMIT_AS` 3 GiB, `RLIMIT_CPU` twice the 10-minute deadline, BLAS/OpenMP threads 1. Measured on live
     data: VmPeak 847 MB, maxrss 516 MB.
   - Due work is read from durable state every 60 s: AFTER_CLOSE (lifecycle IDLE/BEFORE_OPEN, the closed session's
     close window COMPLETE) and SESSION_COHORT (the target session's cohort has a current release).
2. **Job durability: the products are the durable record; no new table and no window-run reuse.**
   - `rt.materialization_jobs` requires a bar source (`CHECK num_nonnulls(source_revision_id, primary_window_id) >= 1`)
     and is claimed by the bar workers. `rt.portal_publication_work` requires a source cycle receipt and the snapshot
     ids up front (a publication outbox, not a compute job).
   - A window-run row would enter the scheduler's progress, its coverage audit and the completed-session recovery
     (`load_advanced_scheduler_progress_cursor`, `audit_completed_session_coverage_cursor`, `plan_session_windows`),
     and receipt kinds are fixed by a CHECK on `rt.cycle_receipts` (a migration).
   - Instead:
     - idempotency by input identity: every product carries `daily_products.input_sha256` (cohort, session, members,
       actions, coverage, per-ticker bar lineage heads, product set; not the clock). A run whose current products
       carry the same identity ends ALREADY_STORED after the narrow load, without computing.
     - bounded retries in the scheduler: 3 attempts per (cohort, session), 5 minutes apart, a 10-minute deadline
       (killed past it); "no release to attach to" is not an attempt.
     - faults: DAILY_PRODUCTS_FAILED (the job), DAILY_PRODUCTS_ATTEMPT_FAILED / DAILY_PRODUCTS_GAVE_UP and
       DAILY_PRODUCTS_LAUNCHER_FAILED (the scheduler), through `fault_journal.emit_fault`.
   - **Consequence for the "before the first window" run (revised Nov 14 on the user's question):**
     `rt.portal_releases.receipt_id` references `rt.cycle_receipts`, and a new cohort has no cycle receipt before its
     first window, so only the naming step (the release) has to wait. The run is split:
     - **PRE_OPEN (prepare)**, from activation (~14:15 UTC) to the open: compute everything, write the evidence,
       store both snapshots, no release (invisible).
       - The cutoff is the cohort's effective time (the open) or its creation time, if later (late activation): the
         loader reads a cohort only once it is effective.
     - **SESSION_COHORT (publish-only)**, right after the first window publishes (the scheduler checks after every
       window): a narrow load proves the input identity, the stored products are found by it and named in one release
       attached to the current release. No computing; nothing runs inside or during the first window's work.
     - **Fallback:** without prepared products (activation after the open, as on Nov 13, or a failed preparation), the
       SESSION_COHORT run computes as before.
3. **1h lineage: accepted by stored lineage.** Derived raw daily and raw 1h bars are read without their sources
   (`source_lineage=STORED_ACCEPTED`, an InitVar of the input, out of every hash).
   - On live, raw 1d is derived from 78 5m bars, 1h from 12 5m bars, 5m from 5 1m bars (AAPL, Nov 13).
   - A derived bar must still name its sources, and any sources that are present are verified as before.
   - The window projection keeps verifying every derived aggregate it loads.
   - The narrow input is 23,085 bars (vs ~744k for the window input), loaded in 5.7-8.5 s. The closure fallback exists
     (`--source-closure`).
4. **Strategy release resolution, and the product published.**
   - The frontend reads `/api/strategies/daily` with `read_mode=retained`. The reader
     (`RealtimeDailyStrategyRepository.read`) resolves the newest DAILY_STRATEGIES_V1 / DAILY_STRATEGY_HISTORY_V1 /
     DAILY_STRATEGY_ROLLING_V1 entry of the cohort's current release, else of its 32 latest releases
     (`_latest_release_entries`).
   - A KEPT (SESSION_CLOSE class) strategy snapshot is validated by its embedded hashes only (`self_contained`).
   - The job therefore stores the strategy snapshot KEPT and names it in one INTRADAY release on the current pointer.
     Later window releases carry the entry forward: a window never republishes it.
   - **Product: DAILY_STRATEGY_HISTORY_V1, not DAILY_STRATEGY_ROLLING_V1.**
     - Cohorts are per session (a new Equity cohort every session, Nov 9-13).
     - The rolling producer reads only the same cohort's publications, and the frontend requires every evidence ref
       to carry the publication's cohort. A rolling release would therefore never hold more than one session.
     - The history release re-evaluates the session and the four before it from the retained bars, labelled
       RECONSTRUCTED. Stock Persistence accepts it.
5. **Pattern Watch.**
   - **Daily frames (1d/1wk/1mo) kept in the projection.** The ticker page reads them from the workspace, and warm
     windows reuse them (0.2 s). Moving them changes the workspace contract for no window time.
   - **30m/1h pattern-and-channel only: built, behind its own switch `REALTIME_V2_PATTERN_WATCH=LIGHT`.** It is
     separate from `REALTIME_V2_DAILY_PRODUCTS` so either can be rolled back alone.
     - Each light frame runs `detect_forming_patterns` and `detect_price_channel` on the same 300-bar frame, serialized
       as the evidence payloads were, instead of the full evidence pipeline a second time beside the 500-bar setup.
     - Its rows are not stored as evidence: the view is RELEASE_ONLY (the reader already skips receipt resolution for
       it), and the workspace's evidence index and the cycle's pattern evidence drop those frames together (the parity
       check holds).
     - The producer decides this from the workspaces, not the switch, so rehydration and reconstruction read what the
       cycle produced.
   - **"Publish only when the rows change": not built.** The frontend opens a Pattern Watch row's ticker page pinned to
     the workspace named in that Pattern Watch publication (`PatternWatch.tsx`, `publication.workspaces`). A
     carried-forward publication would open ticker pages up to 30-60 minutes old, to save ~200 KB of storage per window
     and no window time. Revisit with the page redesign, or with a reader that pins the current workspaces.
6. **Switch values (new).** `REALTIME_V2_DAILY_PRODUCTS`:
   - unset/OFF: today's behaviour.
   - STRATEGIES: the job runs and publishes the strategies only. The window still builds the Dashboard, so no window
     changes.
   - JOB: the job publishes the strategies and the Dashboard, and the window builds no Dashboard (also not at the
     close). Two job values keep the window's Dashboard and the job's from ranking against each other in one
     release.

## 8. Rollout

- Build a candidate (`runner/build_candidate.sh`) only on the user's go. Rehearse with the worktree mounted on the
  live image first (done, section 6).
- Deploy outside the session, on the user's go. Only the Equity scheduler image changes; the API needs no change
  (phase 6).
  1. The candidate on the scheduler with the switch unset: identical behaviour (the launcher does nothing; the window
     code path is unchanged when the switch is unset).
  2. `REALTIME_V2_DAILY_PRODUCTS=STRATEGIES` after a close: the after-close run publishes the strategies on the closed
     cohort. Observe it (the `DAILY_PRODUCTS` report line in the scheduler log, `/api/strategies/daily`, Strategy
     Overview, Stock Persistence and the six pages). The window still builds the Dashboard.
  3. `REALTIME_V2_DAILY_PRODUCTS=JOB` before an open: the window drops the Dashboard. The job publishes it with the
     strategies after the close, and prepares them at activation and releases them right after the new cohort's first
     window.
  4. `REALTIME_V2_PATTERN_WATCH=LIGHT` (independent; any time outside a session): Pattern Watch 30m/1h light frames.
     Fallback: unset.
- Fallback: unset the switch (or set STRATEGIES). The window builds the Dashboard again from its next due window (its
  first window after a restart, cold windows, the close). Within a cohort that already holds a job Dashboard, that
  entry keeps ranking above the window's (a release keeps the higher source generation per type). It holds the same
  daily analysis, so nothing is lost. From the next session's cohort the window's Dashboard is the only one.
- **Done Nov 14:**
  - **22:53:** scheduler r117 with JOB and LIGHT (the after-close run published in 55 s). The live check found the API
    refusing `/api/strategies/daily` (the strategy reader required exactly one current reference revision) and the
    5-session release at 32 MB (~3 s per read).
  - **23:15-23:16:** API r118 (r102 + the reader fix) and scheduler r119 with
    `REALTIME_V2_DAILY_STRATEGY_SESSIONS=1` (latest-session release, 4.6 MB). Published in 16.9 s; 30/30 portal reads
    200, strategy reads 0.4-0.9 s.
  - The window-side behaviour (PRE_OPEN, publish-only, no Dashboard in windows, Pattern Watch light) is first seen on
    Monday Nov 16 (handover section 11).
- Optional: `REALTIME_V2_DAILY_PRODUCTS_WORKERS=4` (measured 94 s -> 65 s at 45 names, +~1 GB) and
  `REALTIME_V2_DAILY_PRODUCTS_DEADLINE_SECONDS` (default 600) as the universe grows.

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
- **The job's run time at scale (found in the build; user agreed Nov 14).**
  - Where the time goes (r117, 45 names, quiet VM, one process): 39.5 s in total.
    - Narrow load: 3.2 s.
    - Strategies: 31.7 s (~80%). That is the setup evidence pipeline per member per session (~0.14 s each, 225 runs:
      five sessions x 45 names).
    - Dashboard: 4.5 s (reuses the latest session's setups).
  - Only the latest session is needed by the six strategy pages and Strategy Overview. The four earlier sessions exist
    for Stock Persistence's 5-session grid.
  - Scaled to 400 names (x8.9): ~6 minutes on a quiet VM. The "~15 minutes" first stated came from the 94-100 s runs
    measured beside the test suites. It fits the ~15-minute pre-open slot (activation to open), with thin margin on a
    busy VM.
  - Levers, in order of value:
    1. **Reuse the four earlier sessions** between the after-close run and the next morning's run. The bars are the
       same; they are recomputed only because every evidence row carries the cohort (a new cohort every session).
       Split the materialization into a cohort-free compute and a stamp, and the morning run computes about one
       session (~1-2 minutes at 400). The real fix; a change in the evidence code (`equity/materialization.py`,
       `realtime/equity_materialization_adapter.py`).
    2. **Lighter past sessions.** Persistence needs match/no-match per strategy and ticker (the scanners: ~1.5 s per
       session at 45). The full setup evidence each session carries is required by the release contract
       (`_validate_release`); relaxing it is a contract change. Decide together with the Stock Persistence redesign
       (below).
    3. **Latest session only** (DAILY_STRATEGIES_V1, about a fifth of the strategy time), if the redesigned page does
       not need past sessions.
    4. **Workers** (`REALTIME_V2_DAILY_PRODUCTS_WORKERS`, built; 1.45x with 4) and the deadline
       (`REALTIME_V2_DAILY_PRODUCTS_DEADLINE_SECONDS`, built).
  - Not needed at 45 names. Plan lever 1 before the universe grows past ~150 names.
- **Stock Persistence redesign (user, Nov 14: "it is really not giving a meaningful signal to derive from").**
  - The page shows, per ticker and strategy, which of the last five sessions matched.
  - The redesign decides what the page should show and which history it needs. That also decides the daily products
    job's main cost: levers 2 and 3 above.
  - Until then the job publishes DAILY_STRATEGY_HISTORY_V1 (5 reconstructed sessions) so the page keeps working.
  - Matrix row R79.
- **30m and 1h Pattern Watch outside the window:** not needed; the warm projection already refreshes them when their
  bars complete.

## 10. Review round (Nov 15): five integration findings

An external review raised five findings. All five were checked against the code and live rows (read only) and are
valid. Fixed in worktree `r78-daily-products`: uncommitted, not built, not deployed.

| # | Finding | Checked | Fix |
|---|---|---|---|
| P1 | Prepared products can fail to publish after the first window | Live: a new Equity cohort's first window publishes at generation 13 (13 READY reference seeds, the last Oct 30). The old rule gives products prepared before it generation 1. At the same cutoff the publisher records them HISTORICAL_STALE, the job raises, and every retry finds the same prepared products. With r119, Monday's new cohort would have no Dashboard and no strategies for the whole session. | The job's generation is above the cohort's snapshots and features, its current release, and the READY seed count (a fresh cohort: 14 on live). Prepared products below the current release are not released; the run computes again (in the load, and once more if a window outranks them at publication). Refused before anything is written (`OUTRANKED`). |
| P2-2 | A Pattern Watch switch change breaks recovery | Recovery (rehydration) projected in the current switch. FULL to LIGHT rebuilds other workspaces; LIGHT to FULL names evidence never written. | The stage that projects the cycle records its mode on the receipt (`build_id` + `+pattern-watch-light`; FULL receipts unchanged). Recovery projects in the recorded mode. Reusing a PORTAL_READY receipt compares the producer build without the suffix. |
| P2-3 | No session-close Dashboard is kept | JOB mode: the window builds no close Dashboard, and the job stored its Dashboard HOT only, in an INTRADAY release. The strategy producer's SESSION_CLOSE release was dropped too. | When the products describe the cohort's own session and attach to its closing release (AFTER_CLOSE): the Dashboard is HOT and KEPT, and a SESSION_CLOSE release names the kept Dashboard and the (KEPT) strategy snapshot, as before. |
| P2-4 | The deadline only applies between windows | The scheduler runs a window synchronously; the launcher polls between windows. | A watchdog timer thread kills the run at its deadline; the tick stays as a fallback. |
| P2-5 | A kill reaches only the job, not its workers | `running.kill()` on the parent only; shutdown only closed the pool. | The job starts in its own session (`start_new_session`); deadline, failure and scheduler shutdown kill the process group. Address-space and CPU limits are per process: with N workers up to (N + 1) times the limit, so workers stay at 1 (the default; live does not set it). |

- **Fresh-cohort behaviour (the review's first rehearsal priority), live API, Nov 15 01:05.** For a cohort whose
  release names no products:
  - `/api/latest-price-date`, `/api/tickers/overview` and `/api/scan/all` answer 503 "Dashboard has no retained
    publication for this cohort";
  - `/api/strategies/daily` answers 503 "No retained independent daily strategy release".
  - So "the previous publications stay served" holds for a cohort that has products. A new cohort, which the portal
    reads from the open, has none of its own until a run publishes: normally seconds after its first window. If
    every attempt fails, it has none all session; the operator fallback is the switch (handover section 11).
- **Recovery across a switch change (the second rehearsal priority).** Tests, both directions:
  - the projection under a bound mode equals the original (snapshot ids and pattern materializations);
  - the projection stage records its mode;
  - the publish stage rehydrates in the receipt's mode whatever the switch;
  - a retry reuses a bound receipt.

  A live read-only rehydration of a recorded window under the other switch is part of the candidate rehearsal.
- **Tests:**
  - `test_daily_products_r78.py`: 72 passed (19 new). P1 and P2-3 are tested through the real publisher
    (`publish_portal_release_cursor` over in-memory rows): generations 1, 5 and 8 against 7 reproduce the review's
    table, and the session-close release names the kept products. P2-5 kills a real process tree.
  - The 27 touched suites (the 25 of the build plus the two scheduler suites): 978 passed, 11 failed. Ten are the baseline failures of ea76dd5. The eleventh, test_equity_full_session_services_are_long_lived_until_stopped, fails the same way on the committed 60da29d: it reads docker-compose.realtime-v2.yml from the repository root, which the test container does not mount. No new failures.
- **Residual (by design, unchanged):** a lane that republishes at the same cutoff after the job, below the job's
  generation, records HISTORICAL_STALE and keeps the job's release current (the after-close run already behaved
  this way).

