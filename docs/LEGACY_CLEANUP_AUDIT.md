# Legacy Cleanup Audit

Updated September 15, 2026 after explicit retirement of Stock Research, Dashboard
scanner history, ticker scanner history and the legacy daily replay harness.
Source-only cleanup, not a strategy change or a declaration of alpha.
The actual archive timestamp is retained in its manifest. The current tree is
authoritative over earlier chat summaries, including the older claim that the
multi-model engine had not yet been implemented.

## Result And Scope

### Pre-Commit Review: September 15

The source changes are a reasonable selective checkpoint, NOT a blanket
"stage everything" or production-ready release. The working index currently
contains only the14approved backup-untracking deletions. No implementation was
staged and no commit or new branch was made during this review.

Validation of the31currently changed/new backend test files completed:
647passed,27skipped,14existing warnings,6subtests passed. The skips are explicit
PostgreSQL integration tests; their opt-in migration gate was not enabled. This is
not every unchanged test in the repository or a historical replay. Frontend69tests,
TypeScript and production Vite build passed. Working/staged whitespace checks were
clean at review start; no source merge/rebase or data writes were performed.

One test-only correction was needed: PowerShell's narrow output wrapping caused
13launcher text assertions to fail despite expected process exit/status behavior.
The COMPLETE original [launcher test](../legacy/commit-readiness-2026-09-15/test_start_workers.py)
was copied first with matching SHA-256
`3242c49cd529e822f2ef8d7acb2658a72f85f2c7ed81e96b0665880dd3853bc2`.
The test wrapper now captures raw warning/exception messages instead of host
formatting; all39launcher tests and the full31-file scope then passed. Production
launcher behavior and safety assertions were not relaxed. Local process inspection
uses JSON, and a reusable commit-test task records the exact test scope.

**Commit scope still needs selection:** pending non-backup files totaled about
223MiB at inventory time. In particular,
[equity_signal_scorecard_results.json](equity_signal_scorecard_results.json) is
148,120,483bytes (about141MiB), above GitHub's normal100MiB per-file limit. Also
review the20,410,402-byte [options shadow result](option_equity_shadow_results.json)
and10,740,810-byte [volatility-budget result](equity_momentum_vol_budget_results.json).
These are not excluded by the backend/backups ignore rule. Do not silently delete
them, change frozen hashes, or add broad ignores. Keep large generated reports in
a separately approved evidence archive or use an explicitly chosen LFS policy;
leave them unstaged in a source-only checkpoint until that decision is made.

Recommended staged groups: source/tests and canonical migrations; current UI;
current configuration and human-readable plans; then the intentional backup
untracking together with the updated ignore rule. Verify the selected staged diff
and artifact references before committing. Preserve original sample/config/model
inputs that retained tools actually read. External evidence/backup destinations and
any copied research PDF's redistribution rights remain outside this code check.
No secret/license certification or blanket whole-branch approval is implied.

The agreed [post-commit plan](STOCK_ALERT_CONTEXT_ENHANCEMENT_DESIGN.md#post-commit-delivery-contract)
is baseline/readiness and parity, frozen V2 versus momentum/random/exact xsmom,
separate incremental context trials, prospective review, then coordinated page
migration/old-model retirement. Quality-V2 is not an activated feed or completed
performance study. The same document records the live API's retainedV1policy and
absent workers as of the operational check; committing will not start them.

### Latest Completed Cleanup Batch

After the file-by-file review, the user explicitly requested a NEW root legacy
folder, full copies before trimming working files, and ignoring/untracking all
of `backend/backups`. This supersedes the earlier local-archive-absent state and
selective backup ignore policy; the previous external archive was not touched.

The [new archive](../legacy/README.md) contains eight complete originals totaling
288,401 bytes. Its manifest and all file hashes were verified before and after
cleanup. Working files keep their original import paths with only needed code:

- Discovery keeps `stock_features`, `filter_stocks`, source/accounting constants
  required by retained readers, and `load_snapshot`. Retired snapshot capture,
  transition generation, paper evaluation and mark-writing code/tests are archived.
  Existing compatibility APIs and legacy stored results remain readable.
- The frontend service keeps current Alerts types/client; unused old Screener/
  daily-Alerts clients and their exclusive types are removed.
- The diagnostic module now contains only the ORIGINAL `score_causal_ridge`
  implementation. The retained ridge observer's import and model behavior are
  unchanged. Closed fitting/portfolio experiments and original tests are archived;
  nine focused scorer/CLI tests cover the retained behavior.
- The generic alpha research CLI remains available. Only `--price-diagnostic-config`
  and `--diagnostic-output` were retired; old experiment command examples/tasks
  are historical and must not be used against the trimmed module. No backtest ran.
- [.gitignore](../.gitignore) now ignores the entire `backend/backups/` subtree,
  including JSON, binaries, SQLite and future formats. `git rm --cached` untracked
  14 existing backup paths. All14 remain on disk with identical pre/post SHA-256;
  `git ls-files -- backend/backups` is empty. Their removals are staged in the
  index, not physical deletions. No commit was made. Backend source/tests/docs and
  the new legacy source-copy folder are NOT ignored by that backup rule.

Validation after trimming: current application suite303passed, focused scorer/
observer/discovery26passed (overlaps), frontend69passed, TypeScript and Vite build
passed. The previous suite had306tests; obsolete producer tests were archived and
a snapshot-reader test added, not silently skipped. No worker/provider/database
mutation, model fit, policy promotion or frozen study/hash rewrite occurred.

One-time operational tools/tests remain for a separate reviewed batch. The dated
234-path inventory below describes the PRE-TRIM state; this section is the current
disposition for the files changed in this batch.

The user clarified the archive contract: copy the entire page as-is, then remove
unwanted functionality from the existing working file. Complete Dashboard and
Ticker Detail copies were captured before changes; their working files remain
in place with only the retired functionality removed. No partial-page archive or
replacement stub is used. Shared service/style files were also copied before
their retired sections were removed.

The user moved the FIRST legacy archive outside the repository on September15.
That directory was absent during the earlier validation; its external destination
was not supplied. The new root batch above was created later on explicit request.
Four
manifests cover 26 complete-file entries (777,051 bytes), including earlier copies
and snapshots of shared files. These are not 26 deleted source files. No permanent
archive deletion, commit, database write, provider request, replay or worker
activation was performed. Frozen studies, evidence and runtime data are unchanged.

## Delivered Retirement

- [Persistence page](../frontend/src/pages/PersistencePage.tsx) is at
  `/stocks/persistence`, replacing Stock Research in navigation. Old Stock Research,
  scanner-results, scanner-evaluation and backtest routes redirect there.
- Dashboard scanner-history queries/section and the ticker Scanner History tab,
  queries, expansion state and formatting code are removed. Old ticker `/scanner`
  bookmarks redirect to Overview with the query string preserved.
- Research qualification, latest-by-ticker, summary, backlog, recent-events and
  ticker scanner-events HTTP routes are removed. The sector-performance endpoint,
  Sector Intelligence, ticker analysis, screening and alert endpoints remain.
- Default Persistence reuses five-session published streak snapshots, filters,
  grouping, trajectories, pagination and ticker links. Failed/misaligned source
  windows are unavailable, not zero coverage. No new normalized persistence table,
  alert gate or screening feature was added. Existing publisher/flag behavior stays
  unchanged; this is not a new live scan or a claim of economic edge.
- Legacy daily batch/replay/merge commands and their exclusive test file are
  archived. [Frozen input checks](../backend/research/frozen_daily_study.py) were
  extracted for retained V2 callers. Archive lookups read only allowlisted source
  bytes for checksum verification; no archived code is imported or executed.
- The optional ridge observer is explicitly KEPT by the user's scope choice.
  Current detector, outcome, acquisition, options-context and data foundations stay.

This is NOT a certificate that the whole branch is ready to commit. The initial
Git inventory includes extensive existing backend/frontend modifications and
untracked studies, operational evidence and new implementation. Unrelated options
page changes were not reviewed or modified by this cleanup. File age, a `v1` name,
an unqualified strategy, or location under `research` does not prove dead code.

## Current Product And Required Support

| Group | Owning Files | Keep Reason |
|---|---|---|
| Alert quality V2 | [forward policy](../backend/research/stock_idea_forward.py), [models](../backend/research/stock_idea_models.py), [worker](../backend/scripts/run_stock_idea_worker.py) | Quality version 2 is the new default for planning, with separate state and v1 isolation; no cutover in this task |
| Shared live/replay engine | [engine](../backend/research/stock_idea_engine.py), [replay](../backend/research/stock_idea_replay.py), [evaluation](../backend/research/stock_idea_evaluation.py), [replay CLI](../backend/scripts/run_stock_idea_replay.py) | Current episodes, selection, execution and independent evaluation; forward code imports replay helpers |
| Daily V2 detector definitions | [strategy_v2.py](../backend/research/strategy_v2.py) | Imported by current multi-timeframe models; not an obsolete replacement candidate |
| Forward facts and retained readers | [source reader](../backend/equity/stock_idea_forward_source.py), [alert projection](../backend/research/stock_alerts.py), [view reader](../backend/equity/stock_alert_views.py), [view preparation](../backend/scripts/prepare_stock_alert_view.py) | Current source clocks, forward and historical views; V1 retained evidence remains intentional |
| Alert API and UI | [API](../backend/equity/stock_discovery_api.py), [current page](../frontend/src/pages/StockAlertsPage.tsx), [service](../frontend/src/services/stockDiscovery.ts) | Current alert-view route lives in the older-named API module; shared frontend types remain in the older-named service |
| Alerts styling | [StockDiscoveryPage.css](../frontend/src/pages/StockDiscoveryPage.css) | Explicitly imported by today's Alerts page, although the original TSX page is unrouted |
| Separate stock screening | [screening API](../backend/equity/screening_api.py), [projection](../backend/equity/screening_projection.py), [hourly context](../backend/equity/screening_hourly.py), [gaps](../backend/equity/screening_gaps.py), [pure screening](../backend/research/screening.py), [worker](../backend/scripts/run_screening_worker.py), [preparation](../backend/scripts/prepare_stock_screening.py), [page](../frontend/src/pages/StockScreeningPage.tsx) | Current Screener, not superseded by Stock Alerts; retain library/filter helpers, services and tests too |
| Context enhancement | [current plan](STOCK_ALERT_CONTEXT_ENHANCEMENT_DESIGN.md), [quality review](STOCK_ALERT_REVIEW_2026-09-14.md), [audit utility](../backend/scripts/audit_stock_alert_session.py) | Quality V2 baseline and provenance; Stage 1 readiness producer is still the next planned slice, not obsolete |
| Market/sector/event/options support | [events](../backend/market_events.py), [sector mapping](../backend/research/gics_sectors.py), [IV readiness](../backend/scripts/report_option_iv_context_readiness.py), [IV materializer](../backend/scripts/materialize_option_iv_context.py) | Explicit reuse targets in the approved context plan; retain canonical data, action/reference, options and coverage owners |
| Worker orchestration | [launcher](../backend/scripts/start_workers.ps1), [equity worker](../backend/scripts/run_equity_worker.py), [portal publisher](../backend/scripts/refresh_equity_portal_snapshots.py) | Current documented producers; legacy daily discovery is no longer in the launcher |

Keep the corresponding tests and the current stock-idea pilot/evaluation
configurations. Quality V1, source-readiness V2, daily strategy V2, and forward
quality V2 are different version axes. Do not purge every `v1` policy/artifact or
assume the older daily study evaluated the new quality V2 dispatcher.

## External Archive

The moved directory contains the portable verifier, archive README and these
batches. Each batch manifest records original/destination paths and byte fingerprints.
The values below were verified before relocation, not reread at the external site.

| Batch | Full-File Entries | Bytes |
|---|---:|---:|
| `equity-retirement-2026-09-14` | 3 | 30,549 |
| `stock-research-retirement` | 11 | 561,748 |
| `legacy-daily-harness` | 5 | 90,236 |
| `scanner-history-retirement` | 7 | 94,518 |

All are offline restoration copies, not supported launch paths. The original
Dashboard and Ticker Detail are preserved in full in `stock-research-retirement`;
working pages remain in the repository. To verify the moved copy, run its helper
with the repository interpreter from the repository root (destination is an example):

```powershell
.\backend\.venv\Scripts\python.exe "D:\YourBackup\legacy\manage_archive.py" --verify-all
```

For historical source audits only, set the absolute all-batches archive directory:

```powershell
$env:STOCK_SCREENER_LEGACY_ARCHIVE_ROOT = 'D:\YourBackup\legacy'
```

Do not configure this for ordinary application startup. No destination was inferred
or saved by this task. External-copy integrity remains unverified until the copied
helper is run there. Missing/tampered source evidence still blocks historical audits.

| Archived File | Evidence For Retirement | Intentionally Retained |
|---|---|---|
| Daily discovery worker (first external batch) | Current launcher and README replace it with the multi-model forward worker; no active source/test import found | Existing discovery ledger, helpers, API and tests |
| Original discovery page (first external batch) | No application import; current routes load StockScreeningPage and StockAlertsPage | Shared stylesheet, service, URL behavior and current pages |
| Composite-study launcher (first external batch) | Dated September 3 orchestration, superseded by the strict research work; not called by current launcher/tests | Shared detectors, required historical utilities, manifests and dated walkthrough |

Repository searches included code, tests, documentation and local task references.
Old task strings naming the discovery process are inspection/shutdown patterns,
not imports or launch dependencies, and were not rewritten. External scheduled
tasks, private automation and shell aliases were not audited. Do not infer their
absence from repository search. Existing service processes were left untouched.

## Completed Research But Coupled

These groups are retirement candidates, not a safe recursive delete list.

| Group | Files / Evidence | Blocking Dependency Or Required Decision |
|---|---|---|
| Strict legacy strategy batch and scorecard | External `legacy-daily-harness` batch; retained [outcomes](../backend/scripts/run_historical_signal_outcomes.py), [scorecard CLI](../backend/scripts/report_equity_signal_scorecard.py), [scorecard logic](../backend/research/signal_scorecard.py), [matched controls](../backend/scripts/report_equity_matched_controls.py) | Launcher retired. Daily V2 still imports the report/outcome utilities, so these shared scripts remain. Sample/completion/source checks now live in frozen_daily_study; no retired launcher import remains |
| Completed daily V2 study runner | [run_equity_strategy_v2.py](../backend/scripts/run_equity_strategy_v2.py), [design and results](EQUITY_STRATEGY_V2_RESEARCH_DESIGN.md), [tests](../backend/tests/test_strategy_v2.py) | Current detector definitions must stay. This test file mixes detector and runner/evaluation coverage; moving the runner alone breaks tests. Decide whether future daily study reruns are retired |
| Closed momentum/ridge historical programme | [price diagnostic](../backend/research/price_diagnostic.py), [research CLI](../backend/scripts/run_alpha_research.py), [closeout](EQUITY_RESEARCH_CLOSEOUT_2026-09-12.md), [tests](../backend/tests/test_price_diagnostic.py) | Enrolled paper tracker imports `score_causal_ridge`; its frozen manifests reference earlier artifacts. Keep until the optional observer is explicitly retired or the shared scoring helper is isolated |
| Optional ridge observer | [paper tracker](../backend/research/paper_tracker.py), [entry point](../backend/scripts/run_equity_paper_tracker.py), [study contract](EQUITY_FORWARD_PAPER_STUDY.md) | Still exposed through `start_workers.ps1 -IncludePaperStudy`, README and launcher tests. Dormant is not retired; no observer or ledger removed here |
| Older discovery backend | [pure helpers](../backend/equity/stock_discovery.py), [service](../backend/equity/stock_discovery_service.py), [API](../backend/equity/stock_discovery_api.py), [tests](../backend/tests/test_stock_discovery.py) | Retained `/api/stocks/screener` and `/api/stocks/alerts` readers are still registered. Current `/alert-view` shares the API file. Removing these is an API retirement/split, not a folder move |
| Prior report/config/result files | Momentum, ridge, scorecard, daily V2 and sample manifests under `docs`; original backtest stores under `backend/backups` | References, code/input hashes and reproducibility contracts cross these groups. Do not move individual samples, rewrite frozen hashes, or delete the evidence explaining why promotion was rejected |

## Older Code Still Used Today

- [Materialization](../backend/equity/materialization.py) imports the original
  [screeners](../backend/screeners.py), [composites](../backend/research/composite_scanners.py),
  forming patterns and price structures/channels. These remain current data
  producers even though their individual trading hypotheses were not qualified.
- [Application routes](../frontend/src/App.tsx) still expose Gap, MA, Momentum
  Pullback, Bearish Bounce, Fibonacci and Pattern Watch. Stock Research and scanner
  history are retired; the other modules remain current. Pattern Watch
  is also an intended context capability, not disposable duplicate voting logic.
- [Daily context refresh](../backend/scripts/refresh_daily_signal_context.py)
  imports [cross-sectional ranks](../backend/scripts/generate_cross_sectional_signal.py)
  and [discovery states](../backend/scripts/generate_market_discovery.py), and is
  called by the current equity worker and portal publisher. Their feature/model
  utilities cannot be removed as historical experiments.
- [Qualification](../backend/equity/qualification.py) imports scanner calibration
  and confidence utilities. Unqualified results do not make the qualification
  mechanism or conservative statistical checks obsolete.
- Historical universe/actions/prices, exact-path outcomes, dated references,
  repositories, schema migrations and canonical evidence are shared foundations.
  Do not revert the integrity fixes merely to reduce this branch's diff.

## Hold For Separate Decisions

Standalone alpha/Fibonacci experiments, detector verification, pattern-prior
calibration, daily recommendation tools, old intraday research and destructive
purge commands are NOT certified dead by this review. Lack of a current worker
import is insufficient for manually invoked tools; some retain useful validation
or feed other exposed readers. Keep until each owning feature/study is explicitly
retired, including its tests and task/document references.

One-off storage and screening repairs are operational evidence, not strategy
implementations. Preserve their applied reports and raw recovery media. No repair
was rerun. Options workers, options/equity shadow research, user configuration,
credentials, raw bars and active forward stores are outside this archive batch.

## Backup And Commit Boundary

September15 portability follow-up: routine frontend tests now check working pages
only. Explicit archive integrity checks use the portable helper's `--verify-all`;
all26 entries/777,051bytes passed. Historical source checks accept an absolute
`STOCK_SCREENER_LEGACY_ARCHIVE_ROOT` (all batches) or `source_path(archive_root=...)`.
An explicit wrong location does not fall back; missing evidence and checksum drift
still fail. The application and new multi-model replay do not import archived code.

**Actual directory-absence validation PASSED after the user's manual move:**
306 focused backend tests, 69 frontend tests, TypeScript check and Vite production
build completed with the local legacy directory absent. The previous automatic
rename failure is superseded; no process/permission changes were needed. These
are scoped application/dependency checks, not a full historical replay, database
health check, external-backup verification or every repository test.

1. Keep the moved external archive and verify it using its portable helper as
  described above. The agent has not read or modified the external location.
2. The legacy daily replay launcher is now retired and the ridge observer is kept.
    Further removal of the shared V2 reports/outcome/acquisition utilities is a
    separate decision. No new backtest is needed to perform source cleanup.
3. Keep frozen results, source manifests and operational recovery records distinct
   from runnable legacy scripts. The source archive does not replace a database
   backup or cover retained SQLite/JSON evidence.
4. Review the full diff by concern: canonical integrity/migrations; current engine
   and quality fixes; screening/portal changes; research/evidence; then this cleanup.
   Do not use `git add .`, blanket resets or broad ignores to hide mixed changes.
  The latest approved ignore policy excludes all backup data; its 14 formerly
  tracked entries are staged for index removal only. No commit has been created.

## Verification

- All four complete-file manifests verified against byte lengths and SHA-256.
  Routine regressions now check only working pages; backup verification is explicit
  and portable. Earlier whole-page archive assertions passed before this separation.
- Archive-absent backend task: 306 passed, 14 existing deprecation warnings, 15
  focused files covering Screening, Alerts, V2/replay, retained study contracts,
  launcher, portal, schema and ridge observer. One initial assertion failed because
  PowerShell wrapped "One-shot worker failures"; whitespace normalization fixed
  only that test, preserving nonzero status and no-publication assertions. Full
  focused task then passed. No launcher/runtime behavior was changed.
- Archive-absent frontend model/navigation tests: 69 passed. TypeScript and Vite
  build passed; no Stock Research chunk is emitted. Old archive-helper tasks were
  removed, and the absence-check task now runs retained tests directly. Archived
  tests remain excluded from default pytest discovery.
- Earlier retirement browser check (not rerun after relocation): Persistence loads
  the retained September 8-14 five-session window;
  AAPL filter produces one matching row. No retired API requests on Persistence,
  Dashboard or ticker Overview. Old ticker scanner link redirects to Overview;
  remaining tabs are Overview, Timeframes, Levels, Fibonacci and Financials.
- Desktop1440/mobile390 DOM measurements show no body overflow; the mobile table
  scrolls internally. The screenshot tool returned a stale frame, so mobile visual
  screenshot/pointer certification is not claimed.
- No database readiness, fresh economic study or frozen-study resume was tested.
  Hash checks still reject changed source/artifacts; preserving imports does not
  certify that today's code matches an old frozen run. No frozen hashes rewritten.
  Existing API/Vite were reused without starting/stopping application workers.

  ## File-By-File Usage Review

  Reviewed September15 against `git status --short --untracked-files=all`, excluding
  the generated contents of `backend/backups` from the source-file denominator.
  The working tree mixes this and newer sessions; Git status does not identify which
  conversation authored a change. This is a usage audit of the CURRENT tree, not an
  attribution of every change to this session or a review of every changed line.
  No implementation, database, provider, worker or archive changes were made for this
  review. The only deliverable change is this inventory and its checkpoint.

  Completeness check: 234 unique changed/new/deleted non-backup paths from Git,
  zero missing table rows. A read-only comparison checked table path targets (and
  removed-path identifiers), not merely whether filenames appeared somewhere in
  the document. The ignored local task file and storage retention notes are extra.

  Scope: one row for each changed/new non-backup path returned by that inventory,
  including already-deleted tracked files, plus the ignored local task file. Unchanged
  files were followed only as callers/owners. Ignored caches, raw data, installed
  packages, build outputs and each individual generated backtest shard are NOT
  audited file-by-file here. The separate storage section below is not a purge list.

  Evidence is imports, concrete call sites, current route registration, launcher
  definitions, tests, explicit CLI/config paths and existing study contracts. This
  does NOT prove that a process is running now or that every function in an imported
  file executes in the default configuration. No runtime call profiling was done.
  An uncalled manual command is not automatically obsolete; caller absence is not
  permission to remove data or useful validation.

  ### Status Key

  | Status | Meaning |
  |---|---|
  | APP | Current page, API, producer or shared implementation reached from them; keep |
  | MIXED | Only part is required by current/retained consumers; split before archival |
  | STUDY | Retained offline research/backtest dependency, not the default app |
  | OPT-IN | Explicitly retained optional observer, not a default launcher dependency |
  | OPS | Manual audit/repair/verification; not normal application execution |
  | SETUP | Schema, migration, packaging or test configuration; keep for its lifecycle |
  | TEST | Validation rather than production functionality; retain with its owner |
  | RECORD | Study input/output, operational evidence or explanatory document; not app code |
  | GUIDE | Current product/setup/agent guidance; read by humans/tools rather than the app |
  | REMOVED | Already outside the working tree; no further source move needed |

  ### Findings That Change The Next Cleanup

  1. **Do not remove the open historical-action module wholesale.**
    `run_corporate_action_worker.refresh_corporate_actions` calls
    `EquityCorporateActionRepository.persist_observation`, which calls
    `persist_coverage`, which imports `validate_action_coverage` from
    [historical_actions.py](../backend/equity/historical_actions.py). Its historical
    selection/reread functions are separately used by the manual audit. The filename
    understates its current production role.
  2. **Old discovery contains genuine removable implementation, but is mixed.**
    [screening_projection.py](../backend/equity/screening_projection.py) imports
    `stock_features`; current Alerts' legacy reader uses the source constants.
    The old service's `load_snapshot` serves still-registered compatibility APIs,
    but its `capture_snapshot`, `refresh_marks` and `run_once` producer chain has no
    default worker after retirement. Tests still exercise `evaluate_alert`. Split
    the reader/features from the retired producer before deciding to archive it.
  3. **The retained ridge observer keeps a small part of a large old experiment.**
    [paper_tracker.py](../backend/research/paper_tracker.py) imports
    `score_causal_ridge` from [price_diagnostic.py](../backend/research/price_diagnostic.py).
    Most of the latter is closed momentum/sizing/ridge research, not current app
    behavior. Preserve the scorer and its contract tests, then consider archiving
    the closed experiment functions with their historical tests/configurations.
  4. **Removing the old batch launcher did not retire all offline report code.**
    [run_equity_strategy_v2.py](../backend/scripts/run_equity_strategy_v2.py) still
    imports the scorecard scope loader, matched-control utilities, strict outcome
    reader and historical event type. These are STUDY dependencies, not live V2
    alert dependencies. Their imports passing does not certify old frozen code hashes.
  5. **Some one-time repairs remain only because tests import their helpers.**
    `test_signal_scorecard` imports `verify_reconstruction` and the storage `probe`;
    `test_screening` imports source-gap repair helpers. These can be separated as
    operational tests/tools, not represented as application requirements. Do not
    discard the applied repair records or raw recovery page.
  6. **Some JSON under docs is operational configuration.**
    `forward_config()` reads [stock_idea_pilot_config.json](stock_idea_pilot_config.json)
    before applying forward/quality overrides. It is an APP input despite its pilot
    name. Most other experiment JSON/CSV files are not read by the live application.
  7. **The current intraday replay is not a replacement for every old study.**
    Keep the new engine, interval models, replay/evaluation and their tests. The
    planned quality/context experiment needs a new frozen policy and parity check;
    it must not inherit a result from the old daily or initial intraday studies.

  ### Backend Implementation

  Paths below are under `backend/`. APP means a verified dependency/entry point,
  not a claim that all functions in the file are used on every request.

  | File | Status | Concrete Consumer / Disposition |
  |---|---|---|
  | [equity/api.py](../backend/equity/api.py) | APP | Main router, chart/setup readers, screening and alert freshness helpers |
  | [equity/domain.py](../backend/equity/domain.py) | APP | Canonical entities used by repositories, normalization and workers |
  | [equity/historical_actions.py](../backend/equity/historical_actions.py) | MIXED | Action worker -> repository -> validation; historical selection/reread also serves manual input audit |
  | [equity/historical_prices.py](../backend/equity/historical_prices.py) | STUDY | Direct current caller is research-input audit; reusable pinned price reader, not a default worker |
  | [equity/historical_universe.py](../backend/equity/historical_universe.py) | MIXED | Repository revision selection, historical preparation and audit; not just the retired replay CLI |
  | [equity/outcomes.py](../backend/equity/outcomes.py) | APP | Orchestration and retained V2 studies share exact entry/path/benchmark evaluation |
  | [equity/portal_snapshots.py](../backend/equity/portal_snapshots.py) | APP | Main snapshot readers, portal publisher, screening publication |
  | [equity/qualification.py](../backend/equity/qualification.py) | APP | Repository types and orchestration qualification/context path; do not confuse unqualified results with unused code |
  | [equity/repositories.py](../backend/equity/repositories.py) | APP | Core canonical persistence/readers; contains both operational and research methods |
  | [equity/screening_api.py](../backend/equity/screening_api.py) | APP | Registered by main; current Screener catalog/query/detail endpoints |
  | [equity/screening_gaps.py](../backend/equity/screening_gaps.py) | APP | Screening projection imports `project_gaps` |
  | [equity/screening_hourly.py](../backend/equity/screening_hourly.py) | APP | Screening projection imports `attach_hourly`; worker refresh uses its context contract |
  | [equity/screening_projection.py](../backend/equity/screening_projection.py) | APP | Screening worker/preparation builds current immutable generations |
  | [equity/stock_alert_views.py](../backend/equity/stock_alert_views.py) | APP | Current alert-view endpoint; SHADOW/REPLAY/LEGACY readers and current-price comparison |
  | [equity/stock_discovery.py](../backend/equity/stock_discovery.py) | MIXED | Current `stock_features` and retained constants; old ranking/alert-change/paper helpers mostly compatibility/tests |
  | [equity/stock_discovery_api.py](../backend/equity/stock_discovery_api.py) | MIXED | Current `/alert-view` plus old still-registered `/screener` and `/alerts`; split before retiring compatibility routes |
  | [equity/stock_discovery_service.py](../backend/equity/stock_discovery_service.py) | MIXED | `load_snapshot` used by compatibility API; retired producer/mark chain and tested `evaluate_alert` can be separated |
  | [equity/stock_idea_forward_source.py](../backend/equity/stock_idea_forward_source.py) | APP | Current stock-idea worker's enrollment, readiness and input reads |
  | [main.py](../backend/main.py) | APP | ASGI entry point, remaining ticker/sector/Persistence/options APIs and registered stock routers |
  | [market_events.py](../backend/market_events.py) | APP | Event collectors and option-calendar/model-input consumers; also planned context input |
  | [options/analytics/equity_shadow.py](../backend/options/analytics/equity_shadow.py) | STUDY | Only the separate option/equity shadow report/tests; not a current stock-alert gate |
  | [research/features.py](../backend/research/features.py) | APP | Current daily rank/discovery refresh and xsmom/discovery-state utilities; also old research |
  | [research/frozen_daily_study.py](../backend/research/frozen_daily_study.py) | STUDY | Retained scorecard/matched-control callers use samples/completion/source checks; archive reads are explicit historical evidence |
  | [research/historical_signal_replay.py](../backend/research/historical_signal_replay.py) | MIXED | Historical event type/constants still imported by historical policy owner, outcome tools and daily V2; old replay implementations are not current stock-idea replay |
  | [research/paper_tracker.py](../backend/research/paper_tracker.py) | OPT-IN | `run_equity_paper_tracker`, enabled only with explicit study choice; user chose KEEP |
  | [research/price_diagnostic.py](../backend/research/price_diagnostic.py) | MIXED | Retained observer imports scorer; large remainder is closed experiments/manual CLI/tests |
  | [research/scanner_calibration.py](../backend/research/scanner_calibration.py) | APP | Qualification owner imports timestamp/calibration functions; not proof calibration automatically runs now |
  | [research/screening.py](../backend/research/screening.py) | APP | Current filter/catalog/field/query contracts shared by screening API, projection and UI validation |
  | [research/signal_scorecard.py](../backend/research/signal_scorecard.py) | STUDY | Report utilities, matched controls and their tests; no current portal matrix |
  | [research/stock_alerts.py](../backend/research/stock_alerts.py) | APP | Current API/worker projections and replay views |
  | [research/stock_idea_engine.py](../backend/research/stock_idea_engine.py) | APP | Current forward decisions and replay share candidates, lifecycle, quota and entry rules |
  | [research/stock_idea_evaluation.py](../backend/research/stock_idea_evaluation.py) | STUDY | Current multi-model replay's independent evaluation/statistics, not default forward publication |
  | [research/stock_idea_forward.py](../backend/research/stock_idea_forward.py) | APP | Current worker's quality policy, state, dispatch, persistence and stored-price marks |
  | [research/stock_idea_models.py](../backend/research/stock_idea_models.py) | APP | Current forward and replay adapters; includes quality-V2 confirmation and daily detector calls |
  | [research/stock_idea_replay.py](../backend/research/stock_idea_replay.py) | MIXED | Forward imports scheduling, timing, derivation/execution helpers; manual replay/evaluation orchestration is also here |
  | [research/strategy_v2.py](../backend/research/strategy_v2.py) | APP | Current stock-idea models import daily detector definitions/configurations; not obsolete because old study is complete |
  | [stock_screener/schema.py](../backend/stock_screener/schema.py) | SETUP | Database initialization/materialization setup imports baseline/install functions |

  ### Backend Commands

  These are ALL changed/new remaining command paths in scope. No command was run
  to determine usage: workers, repairs, provider queries and backtests stayed idle
  for this source review. OPS/STUDY does not mean an automatic background job.

  | File | Status | Concrete Consumer / Disposition |
  |---|---|---|
  | [audit_equity_evidence_storage.py](../backend/scripts/audit_equity_evidence_storage.py) | OPS/MIXED | Manual physical-storage audit; `test_signal_scorecard` imports `probe`; move with that operational test if retired |
  | [audit_equity_research_inputs.py](../backend/scripts/audit_equity_research_inputs.py) | OPS | Historical input/identity/action audit; dedicated tests. No default app caller found |
  | [audit_equity_session.py](../backend/scripts/audit_equity_session.py) | OPS | Manual session audit and dated report; no default app caller found. Candidate for operational-tools archive |
  | [audit_stock_alert_session.py](../backend/scripts/audit_stock_alert_session.py) | OPS | Current retained-alert/quality-V2 verification tool cited by active context plan; useful, not live producer |
  | [generate_cross_sectional_signal.py](../backend/scripts/generate_cross_sectional_signal.py) | APP | Imported by `refresh_daily_signal_context`, called by current equity/portal workers |
  | [generate_market_discovery.py](../backend/scripts/generate_market_discovery.py) | APP | Same daily-context refresh path; still powers retained discovery/rank views |
  | [ingest_adjusted_daily_bars.py](../backend/scripts/ingest_adjusted_daily_bars.py) | STUDY/OPS | Historical data acquisition/identity validation; tests and retained studies need it, not default intraday alert producer |
  | [prepare_historical_signal_research.py](../backend/scripts/prepare_historical_signal_research.py) | MIXED | Fresh-data bootstrap invokes it; adjusted importer, paper tracker and diagnostic import `ResponseCache`; cannot archive whole file |
  | [prepare_stock_alert_view.py](../backend/scripts/prepare_stock_alert_view.py) | OPS | Manual construction of retained replay UI view; app consumes its output, not script execution on GET |
  | [prepare_stock_screening.py](../backend/scripts/prepare_stock_screening.py) | APP | Screening worker/explicit launcher publication plus repair's preservation helper |
  | [refresh_daily_signal_context.py](../backend/scripts/refresh_daily_signal_context.py) | APP | Equity worker and portal publisher explicitly call paired rank/discovery refresh |
  | [refresh_equity_portal_snapshots.py](../backend/scripts/refresh_equity_portal_snapshots.py) | APP | Default Portal worker; publishes remaining ticker/sector/Persistence snapshots |
  | [repair_equity_feature_storage.py](../backend/scripts/repair_equity_feature_storage.py) | OPS/MIXED | Known one-row incident repair; only helper imported by mixed scorecard test. Archive executable with incident evidence after test separation |
  | [repair_screening_source_gaps.py](../backend/scripts/repair_screening_source_gaps.py) | OPS/MIXED | Targeted completed source repair and verify mode; three tests inside `test_screening` import its helpers |
  | [report_equity_matched_controls.py](../backend/scripts/report_equity_matched_controls.py) | STUDY/MIXED | Daily V2 imports hash/JSONL/portfolio/price-panel functions; old standalone comparison belongs to completed study |
  | [report_equity_signal_scorecard.py](../backend/scripts/report_equity_signal_scorecard.py) | STUDY/MIXED | Daily V2 and matched controls import `load_batch_scope`; old report CLI is not current UI |
  | [report_option_equity_shadow.py](../backend/scripts/report_option_equity_shadow.py) | STUDY | Separate retained options/equity shadow experiment; no current stock alert selection caller |
  | [run_alpha_research.py](../backend/scripts/run_alpha_research.py) | STUDY | Manual generic alpha/closed price-diagnostic entry point; no default worker import found. Retirement candidate if no future use selected |
  | [run_corporate_action_worker.py](../backend/scripts/run_corporate_action_worker.py) | APP | Current launcher; persists response-bound action observations through shared validator |
  | [run_equity_paper_tracker.py](../backend/scripts/run_equity_paper_tracker.py) | OPT-IN | Launcher `-IncludePaperStudy`; explicitly retained by user |
  | [run_equity_strategy_v2.py](../backend/scripts/run_equity_strategy_v2.py) | STUDY | Older daily V2 study driver, retained for reuse; distinct from current intraday quality-V2 policy/replay |
  | [run_equity_worker.py](../backend/scripts/run_equity_worker.py) | APP | Default equity ingestion/materialization producer |
  | [run_historical_signal_outcomes.py](../backend/scripts/run_historical_signal_outcomes.py) | STUDY/MIXED | Daily V2 imports `FrozenDailyBars`/UTC helper and scorecard imports events; old CLI is not automatic |
  | [run_screening_worker.py](../backend/scripts/run_screening_worker.py) | APP | Default current Screener publisher; not the retired discovery worker |
  | [run_stock_idea_replay.py](../backend/scripts/run_stock_idea_replay.py) | STUDY | Current multi-model freeze/validate/replay CLI; KEEP for future versioned studies |
  | [run_stock_idea_worker.py](../backend/scripts/run_stock_idea_worker.py) | APP | Default current Stock Alerts worker; quality-V2 planning/state isolation |
  | [start_workers.ps1](../backend/scripts/start_workers.ps1) | APP | Documented native launch authority; explicit per-worker, plan and opt-in study paths |
  | [verify_downstream_updates.py](../backend/scripts/verify_downstream_updates.py) | OPS | Manual Screening/Alerts recovery check; no scheduled producer/import found |
  | [verify_stock_screening.py](../backend/scripts/verify_stock_screening.py) | OPS | Manual current Screener generation/history verification; keep useful operational gate |

  ### Schema And Test Configuration

  | File | Status | Concrete Consumer / Disposition |
  |---|---|---|
  | [000_canonical_schema.sql](../backend/migrations/000_canonical_schema.sql) | SETUP | Canonical installation, baseline tests and fresh deployments; no removal based on request traffic |
  | [039_equity_universe_revisions.sql](../backend/migrations/039_equity_universe_revisions.sql) | SETUP | Original/revised universe lineage and repository/audit contracts; retain migration history |
  | [040_equity_action_coverage_responses.sql](../backend/migrations/040_equity_action_coverage_responses.sql) | SETUP | Live action worker's response membership/identity storage; retain |
  | [041_screening_snapshot_type.sql](../backend/migrations/041_screening_snapshot_type.sql) | SETUP | Current screening publication type; retain |
  | [pytest.ini](../pytest.ini) | SETUP | Test discovery excludes offline archive copies; not an application import |
  | Local VS Code task definitions (ignored by Git) | SETUP | Current tests/build/manual commands; labels alone do not prove ongoing usage |

  ### Backend Tests

  Tests are not production imports. Their owning code may be APP, STUDY or OPS.
  Keep tests with retained functionality; relocate old-only tests when that owner is
  retired. Do not make a module look necessary solely because an obsolete test imports
  it. The earlier 306-test pass is regression evidence, not the classification method.

  | File | Status | What Remains Covered |
  |---|---|---|
  | [options/test_equity_shadow.py](../backend/tests/options/test_equity_shadow.py) | TEST/STUDY | Separate options/equity shadow analytics and report SQL |
  | [test_canonical_schema_baseline.py](../backend/tests/test_canonical_schema_baseline.py) | TEST/APP | Canonical schema and surviving source constraints |
  | [test_corporate_action_worker.py](../backend/tests/test_corporate_action_worker.py) | TEST/APP | Current action collection/normalization/coverage contracts |
  | [test_daily_signal_context.py](../backend/tests/test_daily_signal_context.py) | TEST/APP | Current paired daily rank/discovery refresh and no historical rewrites |
  | [test_equity_matched_controls.py](../backend/tests/test_equity_matched_controls.py) | TEST/STUDY | Matched selections and utility functions still imported by daily V2 |
  | [test_equity_materialization_api.py](../backend/tests/test_equity_materialization_api.py) | TEST/APP | Current materialized API reads and freshness |
  | [test_equity_materialization_repositories.py](../backend/tests/test_equity_materialization_repositories.py) | TEST/MIXED | Canonical persistence plus historical revisions/actions; don't archive whole file |
  | [test_equity_outcomes.py](../backend/tests/test_equity_outcomes.py) | TEST/APP | Shared exact-path, identity, benchmark and entry accounting |
  | [test_equity_portal_reads.py](../backend/tests/test_equity_portal_reads.py) | TEST/APP | Surviving portal reads and explicit removed-route boundary |
  | [test_equity_portal_snapshot_worker.py](../backend/tests/test_equity_portal_snapshot_worker.py) | TEST/APP | Current portal publisher and snapshot dependencies |
  | [test_equity_qualification.py](../backend/tests/test_equity_qualification.py) | TEST/APP | Retained qualification contract, not proof any strategy qualifies |
  | [test_equity_research_input_audit.py](../backend/tests/test_equity_research_input_audit.py) | TEST/OPS | Manual historical audit, date/identity/action checks; move with audit if retired |
  | [test_equity_universe_revision_database.py](../backend/tests/test_equity_universe_revision_database.py) | TEST/SETUP | Explicit database integration gates for immutable universe/action lineage; not run by default without opt-in |
  | [test_equity_worker_schedule.py](../backend/tests/test_equity_worker_schedule.py) | TEST/APP | Current worker timing and daily follow-up schedule |
  | [test_historical_signal_replay.py](../backend/tests/test_historical_signal_replay.py) | TEST/MIXED | Retained historical types/contracts plus old replay implementations; split with owner |
  | [test_ingest_adjusted_daily_bars.py](../backend/tests/test_ingest_adjusted_daily_bars.py) | TEST/OPS | Reusable historical acquisition and identity guards |
  | [test_market_events.py](../backend/tests/test_market_events.py) | TEST/APP | Current event clocks, event collection and coverage behavior |
  | [test_paper_tracker.py](../backend/tests/test_paper_tracker.py) | TEST/OPT-IN | Explicitly retained observer and immutable paper contracts |
  | [test_price_diagnostic.py](../backend/tests/test_price_diagnostic.py) | TEST/MIXED | Mostly closed experiment tests; causal scorer coverage still relevant to retained observer |
  | [test_run_historical_signal_outcomes.py](../backend/tests/test_run_historical_signal_outcomes.py) | TEST/STUDY | Retained strict outcome reader and old CLI contracts |
  | [test_scanner_calibration.py](../backend/tests/test_scanner_calibration.py) | TEST/APP | Calibration utilities still imported by qualification owner |
  | [test_screening.py](../backend/tests/test_screening.py) | TEST/MIXED | Current Screener tests PLUS three operational source-gap-repair tests; separate before moving repair script |
  | [test_signal_scorecard.py](../backend/tests/test_signal_scorecard.py) | TEST/MIXED | Retained report/source checks PLUS incident reconstruction/storage-probe tests; separate operational tests |
  | [test_start_workers.py](../backend/tests/test_start_workers.py) | TEST/APP | Current launcher, safety/duplicate/no-publication and opt-in observer behavior |
  | [test_stock_alerts.py](../backend/tests/test_stock_alerts.py) | TEST/APP | Current source-separated readers, prices, navigation data and retained legacy policy |
  | [test_stock_discovery.py](../backend/tests/test_stock_discovery.py) | TEST/MIXED | Current shared features/compatibility reads plus retired producer paper evaluation |
  | [test_stock_idea_engine.py](../backend/tests/test_stock_idea_engine.py) | TEST/APP | Current lifecycle, quota, conflict, ledger and entry rules |
  | [test_stock_idea_forward.py](../backend/tests/test_stock_idea_forward.py) | TEST/APP | Current forward dispatch/quality-V2 and unchanged-original-plan guards |
  | [test_stock_idea_replay.py](../backend/tests/test_stock_idea_replay.py) | TEST/STUDY | Current multi-model replay and shared timing/evaluation helpers |
  | [test_strategy_v2.py](../backend/tests/test_strategy_v2.py) | TEST/MIXED | Current daily detector definitions PLUS older daily-study runner/evaluation tests |
  | [test_trade_setup_intervals.py](../backend/tests/test_trade_setup_intervals.py) | TEST/APP | Remaining ticker/setup interval behavior |

  ### Frontend Implementation And Tests

  All remaining changed/new page modules below have a current import/route. No whole
  unused changed frontend page was identified after the preceding retirement. That
  does not certify every exported helper or every line as used.

  | File | Status | Concrete Consumer / Disposition |
  |---|---|---|
  | [App.tsx](../frontend/src/App.tsx) | APP | Application routes and retired-route redirects |
  | [index.css](../frontend/src/index.css) | APP | Global styles loaded by frontend entry; history-only styles already removed |
  | [layout/AppShell.tsx](../frontend/src/layout/AppShell.tsx) | APP | App wrapper, navigation, page context and date controls |
  | [layout/PageChrome.tsx](../frontend/src/layout/PageChrome.tsx) | APP | Current Screening/Alerts and individual strategy page controls |
  | [layout/StalenessBanner.tsx](../frontend/src/layout/StalenessBanner.tsx) | APP | AppShell freshness banner |
  | [layout/navigation.ts](../frontend/src/layout/navigation.ts) | APP | AppShell navigation/search/breadcrumb definitions |
  | [layout/pageChrome.css](../frontend/src/layout/pageChrome.css) | APP | Imported by PageChrome |
  | [layout/pageContext.ts](../frontend/src/layout/pageContext.ts) | APP | Current pages publish context consumed by AppShell |
  | [pages/Dashboard.tsx](../frontend/src/pages/Dashboard.tsx) | APP | Root route; scanner-history section already removed, other panels remain |
  | [pages/OptionsActivityPage.tsx](../frontend/src/pages/OptionsActivityPage.tsx) | APP | Registered options/activity route; newer-session functionality, not old equity research |
  | [pages/OptionsFlowPage.tsx](../frontend/src/pages/OptionsFlowPage.tsx) | APP | Registered options/flow route; preserve unrelated changes |
  | [pages/OptionsScreenerPage.tsx](../frontend/src/pages/OptionsScreenerPage.tsx) | APP | Registered options/screener route; preserve unrelated changes |
  | [pages/PersistencePage.tsx](../frontend/src/pages/PersistencePage.tsx) | APP | Registered stocks/persistence route and page context |
  | [pages/ScreenLibrary.tsx](../frontend/src/pages/ScreenLibrary.tsx) | APP | StockScreeningPage imports library and Modal |
  | [pages/StockAlertsPage.css](../frontend/src/pages/StockAlertsPage.css) | APP | Imported by current Alerts page |
  | [pages/StockAlertsPage.tsx](../frontend/src/pages/StockAlertsPage.tsx) | APP | Registered stocks/alerts route |
  | [pages/StockDiscoveryPage.css](../frontend/src/pages/StockDiscoveryPage.css) | APP | Current StockAlertsPage still explicitly imports it despite old filename |
  | [pages/StockScreeningPage.css](../frontend/src/pages/StockScreeningPage.css) | APP | Imported by current Screener page |
  | [pages/StockScreeningPage.tsx](../frontend/src/pages/StockScreeningPage.tsx) | APP | Registered stocks/screener route |
  | [pages/TickerDetail.tsx](../frontend/src/pages/TickerDetail.tsx) | APP | TickerWorkspace imports it; scanner history removed, other analysis views retained |
  | [pages/research/PersistenceView.tsx](../frontend/src/pages/research/PersistenceView.tsx) | APP | PersistencePage imports it; directory name does not make it retired |
  | [pages/research/persistence.css](../frontend/src/pages/research/persistence.css) | APP | Imported by PersistenceView |
  | [pages/screeningModel.ts](../frontend/src/pages/screeningModel.ts) | APP | Current Screener/library/filter/formatting contracts and service types |
  | [pages/stockAlertNavigation.ts](../frontend/src/pages/stockAlertNavigation.ts) | APP | Current Alerts source/tab/filter/sort resolution |
  | [pages/stockAlertPresentation.ts](../frontend/src/pages/stockAlertPresentation.ts) | APP | Current Alerts columns, risk geometry and unavailable probability labels |
  | [pages/ticker/TickerWorkspace.tsx](../frontend/src/pages/ticker/TickerWorkspace.tsx) | APP | Registered ticker routes and surviving tabs |
  | [pages/ticker/priceContext.ts](../frontend/src/pages/ticker/priceContext.ts) | APP | TickerDetail imports confluence/Fibonacci current-price calculations |
  | [pages/ticker/ticker.css](../frontend/src/pages/ticker/ticker.css) | APP | Imported by TickerWorkspace; remaining ticker layouts |
  | [pages/ticker/tickerShared.tsx](../frontend/src/pages/ticker/tickerShared.tsx) | APP | Ticker formatting/chart helpers; scanner-specific helper already removed |
  | [pages/useScreenLibrary.ts](../frontend/src/pages/useScreenLibrary.ts) | APP | StockScreeningPage uses saved screens/layouts/workspace state |
  | [services/api.ts](../frontend/src/services/api.ts) | APP | Shared current API client; scanner-history-only contracts already removed |
  | [services/screening.ts](../frontend/src/services/screening.ts) | APP | Current Screener API client |
  | [services/stockDiscovery.ts](../frontend/src/services/stockDiscovery.ts) | MIXED | Current Alerts types/getAlertView required; `getStockScreener`/`getStockAlerts` have definitions but no other matches in current frontend source; candidate unused exports, not a removable service file |
  | [tests/persistencePage.test.mjs](../frontend/tests/persistencePage.test.mjs) | TEST/APP | Current routes/retirement boundaries; no longer needs local archive |
  | [tests/screeningModel.test.mjs](../frontend/tests/screeningModel.test.mjs) | TEST/APP | Current Screener fields, filters, library and layouts |
  | [tests/stockAlertNavigation.test.mjs](../frontend/tests/stockAlertNavigation.test.mjs) | TEST/APP | Current Alerts navigation/presentation semantics |
  | [tests/tickerPriceContext.test.mjs](../frontend/tests/tickerPriceContext.test.mjs) | TEST/APP | Current ticker confluence/Fibonacci price context |

  ### Documents And Agent Guidance

  No application import is required for documentation to be useful. RECORD here
  means a candidate for a historical-document bundle, not safe deletion of evidence.
  File contents were used where needed for ownership/retention, not rereviewed as
  current economic claims or endorsements of old command examples.

  | File | Status | Use / Disposition |
  |---|---|---|
  | [README.md](../README.md) | GUIDE | Current startup/product contract; keep |
  | [.github/copilot-instructions.md](../.github/copilot-instructions.md) | GUIDE | Current agent behavior, not app code |
  | [.github/instructions/windows-execution.instructions.md](../.github/instructions/windows-execution.instructions.md) | GUIDE | Current execution safeguards, not app code |
  | [AGENT_CONTEXT.md](AGENT_CONTEXT.md) | GUIDE | Current checkpoint protocol |
  | [BACKTEST_WALKTHROUGH.md](BACKTEST_WALKTHROUGH.md) | RECORD | Old composite study explanation; already marked historical |
  | [EQUITY_END_OF_DAY_AUDIT_2026-09-14.md](EQUITY_END_OF_DAY_AUDIT_2026-09-14.md) | RECORD | Dated operational audit, not current process status |
  | [EQUITY_EXPLORATORY_BACKTEST_2026-09-12.md](EQUITY_EXPLORATORY_BACKTEST_2026-09-12.md) | RECORD | Closed initial experiment; preserve decision trail |
  | [EQUITY_FORWARD_PAPER_STUDY.md](EQUITY_FORWARD_PAPER_STUDY.md) | GUIDE/OPT-IN | Retained observer contract |
  | [EQUITY_MOMENTUM_BENCHMARK_2026-09-12.md](EQUITY_MOMENTUM_BENCHMARK_2026-09-12.md) | RECORD | Completed benchmark experiment |
  | [EQUITY_MOMENTUM_EXPOSURE_2026-09-12.md](EQUITY_MOMENTUM_EXPOSURE_2026-09-12.md) | RECORD | Completed exposure experiment |
  | [EQUITY_MOMENTUM_REPLICATION_2026-09-12.md](EQUITY_MOMENTUM_REPLICATION_2026-09-12.md) | RECORD | Original sample replication findings |
  | [EQUITY_MOMENTUM_SIZING_2026-09-12.md](EQUITY_MOMENTUM_SIZING_2026-09-12.md) | RECORD | Completed sizing experiment |
  | [EQUITY_MOMENTUM_TREND_2026-09-12.md](EQUITY_MOMENTUM_TREND_2026-09-12.md) | RECORD | Completed market-trend gate experiment |
  | [EQUITY_MOMENTUM_VOL_BUDGET_2026-09-12.md](EQUITY_MOMENTUM_VOL_BUDGET_2026-09-12.md) | RECORD | Completed volatility-budget experiment |
  | [EQUITY_RESEARCH_CLOSEOUT_2026-09-12.md](EQUITY_RESEARCH_CLOSEOUT_2026-09-12.md) | RECORD | Important no-promotion/20% drawdown decision; retain concise closeout even if detailed docs move |
  | [EQUITY_RIDGE_CHALLENGER_2026-09-12.md](EQUITY_RIDGE_CHALLENGER_2026-09-12.md) | RECORD | Closed training study and observer's model provenance |
  | [EQUITY_RIDGE_VALIDATION_2026-09-12.md](EQUITY_RIDGE_VALIDATION_2026-09-12.md) | RECORD | Follow-up validation/caveats |
  | [EQUITY_SIGNAL_EVIDENCE_SCORECARD.md](EQUITY_SIGNAL_EVIDENCE_SCORECARD.md) | RECORD | Completed legacy scorecard and overlap decisions; not a current page |
  | [EQUITY_STRATEGY_V2_RESEARCH_DESIGN.md](EQUITY_STRATEGY_V2_RESEARCH_DESIGN.md) | GUIDE/STUDY | Retained daily V2 definition and results; distinguish from forward quality V2 |
  | [FOUNDATIONS OF TECHNICAL ANALYSIS.pdf](FOUNDATIONS%20OF%20TECHNICAL%20ANALYSIS.pdf) | RECORD | Reference reading, not executed/imported; external-library/archive candidate, check redistribution rights before commit |
  | [LEGACY_CLEANUP_AUDIT.md](LEGACY_CLEANUP_AUDIT.md) | GUIDE | This usage/retirement record; human/tool guidance |
  | [OPTION_EQUITY_ALERT_SHADOW.md](OPTION_EQUITY_ALERT_SHADOW.md) | GUIDE/STUDY | Separate retained shadow contract, not the new stock context policy |
  | [OPTION_PIPELINE_CURRENT_STATE.md](OPTION_PIPELINE_CURRENT_STATE.md) | GUIDE | Options operational documentation; newer-session change, keep |
  | [RESEARCH_HARNESS_ENHANCEMENTS.md](RESEARCH_HARNESS_ENHANCEMENTS.md) | GUIDE/RECORD | Mixed research design and dated evidence; not app input |
  | [STOCK_ALERT_CONTEXT_ENHANCEMENT_DESIGN.md](STOCK_ALERT_CONTEXT_ENHANCEMENT_DESIGN.md) | GUIDE | Next context workstream; planned readiness/annotation work is not implemented by having this file |
  | [STOCK_ALERT_REVIEW_2026-09-14.md](STOCK_ALERT_REVIEW_2026-09-14.md) | RECORD | Quality-V2 baseline review/provenance; preserve |
  | [STOCK_DISCOVERY_AND_ALERTS.md](STOCK_DISCOVERY_AND_ALERTS.md) | GUIDE/RECORD | Current contracts plus marked historical stages |
  | [STOCK_SCREENER_WORKSPACE_DESIGN.md](STOCK_SCREENER_WORKSPACE_DESIGN.md) | GUIDE | Current Screener design |
  | [STOCK_SCREENING_CAPABILITIES.md](STOCK_SCREENING_CAPABILITIES.md) | GUIDE | Current Screener capability reference |
  | [agent-context/downstream-publications.md](agent-context/downstream-publications.md) | GUIDE | Workstream checkpoint, not live process evidence |
  | [agent-context/execution-guidance.md](agent-context/execution-guidance.md) | GUIDE | Execution customization checkpoint |
  | [agent-context/legacy-cleanup.md](agent-context/legacy-cleanup.md) | GUIDE | This cleanup workstream checkpoint |
  | [agent-context/stock-alert-review.md](agent-context/stock-alert-review.md) | GUIDE | Quality review/context next-step checkpoint |

  ### Individual Data And Configuration Files Under Docs

  These are not all unused. APP is an actual forward input; STUDY is a manual study
  input/default or fixture; RECORD is stored evidence/output. The default app does
  not render a report merely because it is in this directory. Preserve parent/child
  hashes and study references before relocating bundles. No record below was
  recomputed, backfilled, modified or certified for freshness during this audit.

  | File | Status | Use / Disposition |
  |---|---|---|
  | [equity_adjusted_identity_preflight.json](equity_adjusted_identity_preflight.json) | RECORD | Dated identity/acquisition preflight evidence |
  | [equity_doc_peak_action_review.json](equity_doc_peak_action_review.json) | RECORD/STUDY | Pinned action-review manifests for manual exact rereads |
  | [equity_doc_peak_correction_review.json](equity_doc_peak_correction_review.json) | RECORD | Dated correction review, not active direction signal |
  | [equity_doc_peak_price_review.json](equity_doc_peak_price_review.json) | RECORD/STUDY | Pinned historical price-review evidence |
  | [equity_doc_peak_transition.json](equity_doc_peak_transition.json) | STUDY | Explicit identity-transition scope for manual audit; not default worker input |
  | [equity_momentum_benchmark_config.json](equity_momentum_benchmark_config.json) | STUDY | Closed diagnostic benchmark configuration |
  | [equity_momentum_benchmark_results.daily.csv](equity_momentum_benchmark_results.daily.csv) | RECORD | Completed daily benchmark output |
  | [equity_momentum_benchmark_results.json](equity_momentum_benchmark_results.json) | RECORD | Completed benchmark report |
  | [equity_momentum_exposure_config.json](equity_momentum_exposure_config.json) | STUDY | Closed exposure experiment configuration |
  | [equity_momentum_exposure_results.daily.csv](equity_momentum_exposure_results.daily.csv) | RECORD | Exposure paths |
  | [equity_momentum_exposure_results.json](equity_momentum_exposure_results.json) | RECORD | Exposure report |
  | [equity_momentum_sizing_config.json](equity_momentum_sizing_config.json) | STUDY | Closed sizing experiment configuration |
  | [equity_momentum_sizing_results.json](equity_momentum_sizing_results.json) | RECORD | Sizing report |
  | [equity_momentum_sizing_results.weights.csv](equity_momentum_sizing_results.weights.csv) | RECORD | Frozen sizing weights |
  | [equity_momentum_trend_config.json](equity_momentum_trend_config.json) | STUDY | Closed trend-gate experiment configuration |
  | [equity_momentum_trend_results.daily.csv](equity_momentum_trend_results.daily.csv) | RECORD | Trend-gated paths |
  | [equity_momentum_trend_results.json](equity_momentum_trend_results.json) | RECORD | Trend-gate report |
  | [equity_momentum_vol_budget_config.json](equity_momentum_vol_budget_config.json) | STUDY | Closed volatility-budget configuration |
  | [equity_momentum_vol_budget_results.daily.csv](equity_momentum_vol_budget_results.daily.csv) | RECORD | Volatility-budget paths |
  | [equity_momentum_vol_budget_results.json](equity_momentum_vol_budget_results.json) | RECORD | Volatility-budget report |
  | [equity_price_diagnostic_config.json](equity_price_diagnostic_config.json) | STUDY/TEST | Closed experiment config; `test_price_diagnostic` reads it |
  | [equity_price_diagnostic_results.csv](equity_price_diagnostic_results.csv) | RECORD/STUDY | Original sample panel for closed analyses |
  | [equity_price_diagnostic_results.json](equity_price_diagnostic_results.json) | STUDY/OPT-IN | Historical report; ridge enrollment verifies original sample-source reports, not a live forecast input after enrollment |
  | [equity_price_diagnostic_results.sample.json](equity_price_diagnostic_results.sample.json) | STUDY | `frozen_daily_study.load_samples` and tests read original 300-name sample |
  | [equity_price_event_sensitivity.json](equity_price_event_sensitivity.json) | RECORD/STUDY | Closed event-sensitivity output and later experiment provenance |
  | [equity_price_event_terms.json](equity_price_event_terms.json) | STUDY | Reviewed event assumptions for closed sensitivity calculation |
  | [equity_price_path_validation.json](equity_price_path_validation.json) | RECORD/STUDY | Original selected-path evidence used in follow-up analyses |
  | [equity_price_replication_comparison.json](equity_price_replication_comparison.json) | RECORD | Original/replication comparison output |
  | [equity_price_replication_config.json](equity_price_replication_config.json) | STUDY | Closed replication settings |
  | [equity_price_replication_path_validation.json](equity_price_replication_path_validation.json) | RECORD/STUDY | Replication selected-path evidence |
  | [equity_price_replication_results.csv](equity_price_replication_results.csv) | RECORD/STUDY | Replication panel |
  | [equity_price_replication_results.json](equity_price_replication_results.json) | STUDY/OPT-IN | Second original sample-source report, enrollment/research provenance |
  | [equity_price_replication_results.sample.json](equity_price_replication_results.sample.json) | STUDY | `frozen_daily_study.load_samples` and tests read second disjoint 300-name sample |
  | [equity_research_identity_review.json](equity_research_identity_review.json) | RECORD | Dated identity review output |
  | [equity_research_input_audit.json](equity_research_input_audit.json) | RECORD | Dated coverage audit, not current runtime readiness |
  | [equity_ridge_challenger_config.json](equity_ridge_challenger_config.json) | STUDY | Frozen closed ridge experiment settings |
  | [equity_ridge_challenger_results.daily.csv](equity_ridge_challenger_results.daily.csv) | RECORD | Closed ridge paths |
  | [equity_ridge_challenger_results.json](equity_ridge_challenger_results.json) | STUDY/OPT-IN | Default source for `run_equity_paper_tracker --enroll`; preserve model provenance |
  | [equity_ridge_challenger_results.predictions.csv](equity_ridge_challenger_results.predictions.csv) | RECORD | Frozen historical scores/selections, not new forward predictions |
  | [equity_ridge_final_sensitivity_config.json](equity_ridge_final_sensitivity_config.json) | STUDY | Closed final sensitivity settings |
  | [equity_ridge_final_sensitivity_results.daily.csv](equity_ridge_final_sensitivity_results.daily.csv) | RECORD | Final sensitivity paths |
  | [equity_ridge_final_sensitivity_results.json](equity_ridge_final_sensitivity_results.json) | RECORD | Final no-promotion evidence |
  | [equity_ridge_path_validation.json](equity_ridge_path_validation.json) | RECORD/STUDY | Frozen selected-path review |
  | [equity_ridge_validation_followup.json](equity_ridge_validation_followup.json) | RECORD | Follow-up price/event validation |
  | [equity_signal_scorecard_config.json](equity_signal_scorecard_config.json) | STUDY | Default retained scorecard report configuration |
  | [equity_signal_scorecard_results.csv](equity_signal_scorecard_results.csv) | RECORD | Completed scorecard output, not current portal input |
  | [equity_signal_scorecard_results.json](equity_signal_scorecard_results.json) | RECORD | Completed scorecard report/provenance |
  | [equity_strategy_v2_config.json](equity_strategy_v2_config.json) | STUDY/TEST | Daily V2 runner/configuration tests, not default forward policy file |
  | [option_equity_shadow_config.json](option_equity_shadow_config.json) | STUDY | Default separate options/equity shadow report configuration |
  | [option_equity_shadow_results.json](option_equity_shadow_results.json) | RECORD | Separate shadow report output, not new stock-selection corroboration |
  | [stock_alert_quality_v2_regression_2026-09-14.json](stock_alert_quality_v2_regression_2026-09-14.json) | RECORD | Retained 42-alert technical regression evidence for active quality baseline |
  | [stock_alert_session_review_2026-09-14.json](stock_alert_session_review_2026-09-14.json) | RECORD | Earlier session review evidence; preserve lineage to final review |
  | [stock_alert_session_review_2026-09-14_final.json](stock_alert_session_review_2026-09-14_final.json) | RECORD | Final session review evidence cited by current design |
  | [stock_idea_evaluation_plan.json](stock_idea_evaluation_plan.json) | STUDY | Default current multi-model replay/evaluation claim contract |
  | [stock_idea_pilot_config.json](stock_idea_pilot_config.json) | APP/MIXED | `forward_config()` actually reads this before forward/quality overrides; also replay default. KEEP until explicitly separated |

  ### Already Removed Tracked Paths

  Git still shows deletions until committed. These are not leftover runnable files.
  Their original paths are recorded below as archive identifiers, not local links.

  | Original Path | Status | Result |
  |---|---|---|
  | `backend/equity/scanner_research.py` | REMOVED | Reader archived after Research/Dashboard/ticker history removal |
  | `backend/scripts/merge_historical_signal_research.py` | REMOVED | Legacy merge command archived |
  | `backend/scripts/run_composite_study.ps1` | REMOVED | Old composite launcher archived |
  | `backend/scripts/run_historical_signal_research.py` | REMOVED | Legacy replay CLI archived |
  | `backend/tests/test_run_historical_signal_research.py` | REMOVED | Legacy replay/batch/merge tests archived |
  | `backend/tests/test_scanner_evidence.py` | REMOVED | Retired reader tests archived |
  | `frontend/src/pages/ScannerResults.tsx` | REMOVED | Retired Stock Research page archived in full |

  Previously untracked retired files (old daily discovery worker, old discovery TSX,
  daily batch launcher and board evidence helper/tests) no longer appear in current
  Git status. Their earlier archived full-file copies are external; this audit did
  not reread that external destination or count them as current implementations.

  ### Retained Storage Is Not Unused Source

  These paths were present in the initial status inventory but excluded from the
  expanded non-backup file matrix. Individual nested output files were not inspected
  one by one. The distinction matters before cleaning untracked files for a commit.

  | Path | Runtime Relationship / Retention |
  |---|---|
  | `backend/backups/equity-paper/` | Enrolled optional observer manifest/ledger; not redundant historical source code |
  | `backend/backups/equity-shadow/` | Current default forward Alerts view/state plus other retained studies; never archive this whole directory as unused |
  | `backend/backups/stock-idea-independent-v1/` | Current default REPLAY Alerts view; pre-enrollment history can fall back to it |
  | `backend/backups/equity-signal-backtests/` | Closed daily studies and frozen input/report dependencies for retained daily V2 tooling; no whole-folder removal |
  | [equity_evidence_page_78960_before_repair.bin](../backend/backups/equity_evidence_page_78960_before_repair.bin) | Original raw recovery media; preserve externally, not an app import |
  | [equity_evidence_storage_audit.json](../backend/backups/equity_evidence_storage_audit.json) | Original incident audit record |
  | [equity_evidence_storage_audit_after_repair.json](../backend/backups/equity_evidence_storage_audit_after_repair.json) | Post-repair evidence |
  | [equity_feature_repair_applied.json](../backend/backups/equity_feature_repair_applied.json) | Applied repair provenance; do not overwrite or rerun |
  | [equity_feature_repair_dry_run.json](../backend/backups/equity_feature_repair_dry_run.json) | Pre-apply reconstruction evidence |
  | [screening_source_gap_repair_v1.json](../backend/backups/screening_source_gap_repair_v1.json) | Retained targeted repair evidence/verification input |

  ### Proposed Next Batches, Not Executed

  - Lowest runtime impact: package dated reports/reference reading and standalone
    completed audits as an external evidence bundle, preserving links, hashes and
    enrollment/sample inputs. Do not include the live pilot config or current views.
  - Separate mixed operational tests/helpers from current scorecard/screening tests,
    then consider moving the one-time storage/source-repair executables with evidence.
  - Split the old discovery capture/mark producer from retained features/compatibility
    readers. Retire its old API routes only with explicit agreement; current Alerts
    still offers a separately labeled legacy-data view.
  - Isolate the observer's causal scorer, then archive closed price-diagnostic
    experiments/tests/config bundles. The user chose to keep the observer itself.
  - Keep current V2 detector/forward/replay/evaluation and required data utilities.
    Separating the older daily V2 runner/report helpers is a further choice, not
    something this audit labels as already unused by all retained tooling.

  No source or test was moved/deleted in this file-by-file review. The next batch
  requires selecting the owning feature/tool set, not deleting every row marked
  non-APP. Earlier tests/build results are unchanged and were not rerun for a
  documentation-only dependency audit.