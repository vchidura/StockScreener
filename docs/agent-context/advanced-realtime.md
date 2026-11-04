# Advanced Realtime V3 - Current Runtime Checkpoint

Updated: 2026-11-03 20:28 UTC - Equity portal recovery deployed and verified; Options processing roles remain stopped; nothing committed.

## Nov 3 Equity portal recovery - Options resume is now eligible

Migration `047_advanced_journal_plan_authority` is applied live with no pending migrations. It removes the
single-table journal-plan FK; registry writes now require the plan to exist in either `advanced_ingress_plans`
or `service_plans`. The bridge image is `e4ab5e9dcf93` (base `d3198c1b`) and changes only
`advanced_journal_registry.py`: Equity replay discovers `STOCKS` streams only. It is healthy with zero restarts.

The falsely quarantined Options journal `d629048a-2187-5bab-abfd-a12787b2df1a` was repaired under exact
plan/stream/cohort/manifest/reason/detail/offset predicates from `QUARANTINED` to `DRAINING`; the quarantine
reason, detail and timestamp remain stored. The stream is now truthfully `STOPPED`, ended at the original
`2026-11-03T15:53:01.329202Z`, with watermark `15:51:50.577Z`; raw/event offsets are `107257/170196`.
MARKET_STATE is checkpointed at `170196`; all nine FLOW shards are checkpointed at `107257`.

The live Options release count and pointer remain zero. Failed 17:35 and expired 17:50 windows were not cleared,
backdated or published. The five Options processing roles are stopped: ingress `b9c2e021b126`, market-state/flow
`f6ea58c8916b`, window `cfd20e717a34`, scheduler `d354ed2dbba1`. Acquisition `25336afa` remains unhealthy but
its durable status is COMPLETE; lifecycle `b61d80cc` remains healthy.

The portal fix was scoped onto the running images: scheduler `7921bc32` from `35808528` plus only
`ticker_projection.py`, API `a1f37347` from `41f819b8` plus only `ticker_repository.py`, and frontend `8e9b58a`
from `060adb55` plus only `PortalIdentityGate.tsx` and its test. API and frontend are healthy with zero restarts.
The scheduler was recreated only after a 20:22:31 UTC read proved zero RUNNING windows/stages. Its old durable
lease caused eight fenced startup attempts before epoch 26 acquired normally; it is stable on the new image, while
the known health probe still reports unhealthy because it contends for the scheduler lease.

The first post-rollout 20:25 Equity window is COMPLETE: INPUT_GATE, MATERIALIZE, PROJECTION and PORTAL_PUBLISH
all completed with fresh receipts. SPY 1m/5m/15m/30m/1h/1d/1wk/1mo reads all return 200. The publication truthfully
reports STALE and serves retained market data; 1m ends 20:24 and the retained 5m prefix ends 20:15. No non-target
container changed. Options ingress/market-state/flow/window/scheduler remain stopped; acquisition/lifecycle remain
unchanged. The Equity prerequisite is now satisfied. To resume Options, keep WINDOW stopped, start
ingress/market-state/flow and scheduler, require all three resident completions for a new cutoff, then start WINDOW
and verify release plus current pointer. Do not retry either expired Options window.

## Live state after the Nov 2 recovery - approved by the user 2026-11-03

Live recovery ran 10:00-10:29 UTC: journals +117/+514 bars, gap repair 9 requests, 15 windows, rollover 36
requests, close attempt 5 COMPLETE. Windows 78/78; all 9 members exact on 1m/5m/15m/30m/1h/raw 1d/adjusted 1d;
real HISTORY stage READY for QQQ and SPY; Treasury Nov 2 VERIFIED (fetched automatically at 21:00:01).
Deployed by `scoped_rollout.py`, other 16 containers unchanged, rollback images in `scoped-rollout-plan.json`:
bridge `d3198c1b` (was `226c8e1c`), scheduler hotfix `ef5fdfb9` (was `61b942e6`), API `41f819b8` (source
`36145771`, was `65d36a00`), frontend `060adb55` (source `49af6ddc`, was `1f319b3f`).

API source fixes in that image: empty `known_by` crash on ticker reads; Pattern Watch session fields; frozen
closing release no longer overwritten by the hot one (All Tickers/Pattern Watch `session_close` reads); positional
cursor for `/realtime/equity-preparation`; `action_evidence_until` ends the action-drift period at the session's
close for current reads, so a nightly refresh no longer takes the portal down. This is a deliberate behaviour change.
Routes through the frontend: 131/153. Remaining: 13 absent paths, 8 needing parameters, `/strategies/daily` with
no release. Browser acceptance is the user's check. `runner/ui_calls.py` holds the calls real pages make;
`preopen_smoke.py` checks stored live state; `api_smoke.py` alone misses `read_mode=session_close`.

Still not deployed, for after the Nov 3 close: controller, ingress, preparation (rollover), lifecycle, full scheduler,
sleep gate. Without preparation's rollover, Nov 4 Options blocks again. Every image except the bridge and API still caps the
history binding chain at 4 scopes (source: 64). The chain gains one per session, so Nov 3 has 3, Nov 4 has 4, and Nov 5
would exceed the cap, which makes this rollout due before the Nov 5 open at the latest. The Options roles also lack the
15-session native context in `advanced_options_equity_input.py`, so OS1/OS2 context may be incomplete today; not verified.

Prior-session stream-derived 5m: the deployed scheduler `ef5fdfb9` fails every Nov 3 window, from the first one, with
`Ticker 5m history source drift`, because Nov 2's 5m are all DERIVED and the check allowed DERIVED for the current
session only. Reproduced read-only by `runner/repro_nov3_window.py`, which runs the projection on real history with
in-memory Nov 3 bars and the binding activation will write. The source fix accepts Advanced DERIVED 5m from any retained
session and verifies each against its 1m parents, using the source closure for sessions older than the 1m chart window.
Image `35808528` (source `1c73c27d`) is built and passes the simulation for 9 tickers at 14:35 and 21:00, including summary,
Pattern Watch and dashboard. Not deployed until the user approves: `scheduler_hotfix_switch.py plan|start|verify --to drift`;
`--to previous` is the rollback. Publishing and real Nov 3 bars stay unproven until the first window.
`test_ticker_projection_state.py` has 2 failures with or without the fix: hard-coded materialization counts of 24 and 22
against 20 and 18.

Options has never activated on live: 2 underlyings (QQQ, SPY), `option_flow_policies` 0 rows until first activation.

Options pre-open validation, 2026-11-03, approved by the user. One websocket handshake at 11:43 UTC: auth accepted, T and
Q subscriptions accepted for one contract, no market events, no retry. `REALTIME_V2_ADVANCED_OPTIONS_READS=ENABLED` since
11:45 UTC on the same API image, recreated by `options_reads_switch.py` (`plan`/`start`/`verify --value`; `--value DISABLED`
is the rollback). `scoped_rollout.py` does not carry the flag, so an API rollout through it turns reads off. Pages read a
published release, never stored T/Q, and show nothing until the first Options window publishes.
`runner/options_selection_dryrun.py` runs the deployed selection on the stored real chain with no writes or provider calls;
`runner/options_open_check.py` tracks capture through release, for 14:32-14:45 UTC. QQQ has one capture round left (64/96
slots), and activation is revision-wide, so a failed QQQ capture leaves Options unactivated for the session. Host
`node.exe` crashes on any script (`0xC0000096`), and 4 of 8 tests in `optionsFlowRealtime`, `optionsScreenerRealtime` and
`optionsAlertsRealtime` cannot load `api.ts` since it imports `./portalIdentity`, so page requests are unverified.

Selection works but favours expensive in-the-money contracts: shortlist and group ranks sort by relative spread first and
use delta only as a late tie-break. On the stored Nov 2 chain, SPY's 8 picks traded 4,755 contracts against 533,230 for its
busiest 8, and 3 of the 16 picks were 3 DTE, which the Flow policy excludes. Expect sparse Flow and Activity. Fix after the
close; volume-first ranking at the open needs prior-session volume, because the chain then holds about 90 seconds of it.

Product gaps the portal correctly reports, not portal bugs. Stock Alerts cannot fire: all 558 Nov 2 evaluations are
UNAVAILABLE, because `canonical_stock_alert_product.py` hard-codes event context to None and has no sector context, and
the detector computes nothing when context is missing. The earlier "0 matches, truthful no-signal" note was wrong;
no signal was evaluated. Batches also stop at 20:00, since recovered windows do not run alerts. Strategies has no
continuous producer: the scheduler never passes `--include-strategies`, which needs a BOUND history report (283
adjusted, 90 raw sessions); the newest release is Oct 27 in an older cohort. So Strategies Overview and Stocks
Persistence stay 503. Both need a user decision and are not planned before the Nov 3 open.

## Nov 2 recovery and Nov 3 readiness - history of the approval

Everything below predates the live run. Evidence and scripts: `backend/.realtime-canary/v3-supervised-20261102/recovery-20261103/`
(`runner/`, `scoped_rollout.py`, `rehearsal/`). Run recovery in containers only: host Python crashed twice
(`0xc0000005`). Backup: `backend/backups/stocks_db_backup_v3-pre-nov2-recovery-20261103_2026-11-02.dump`,
SHA256 `FCAD888E0AAB8C38EB4B36A0809602086640FDFBDB04EDC71CB6AE1C4E0ED3B6`.

Nov 3 Options: `CATALOG_QUOTE` and `REFERENCE_OI` are COMPLETE; `HISTORY` retries with no provider calls
until Nov 2 adjusted and raw daily bars exist. Dividends also need the rollover. QQQ has 64/96 budget slots
used (one capture round left), SPY 32/96. Do not reset them.

New defect, found by rehearsal: a closing window could never complete. Derivation read 5m bars at the stage's
start clock, so the last 15m/30m/1h and raw 1d bars were never derived, and a retry reuses the first
INPUT_READY `known_by`, which predates the daily bars. Fixed in `stage_runners_equity.py`
(`CLOSING_DAILY_INPUTS_PENDING`). Deployed code still has it, so without the hotfix today's close would fail too.

Images, pinned; do not rebuild, because a rebuild changes the ids: recovery `sha256:d3198c1b...` (source `ad1b8476`)
for journals, gap, rollover and bridge. Hotfix `sha256:ef5fdfb9...` (source `74a4a884`) is deployed scheduler
`61b942e6` plus `stage_runners_equity.py` and `ticker_projection.py`, for all windows and the scheduler.

Final rehearsal, finished 09:56 UTC on the disposable copy with these images (fake provider, no live effect):
- PASS: closing window attempt 4 BLOCKED, attempt 5 COMPLETE in 77s; pointer at 21:00. Real HISTORY stage READY for
  QQQ and SPY on lifecycle image `d9dea05d`; dividends valid; all 9 members have both daily bases.
- Windows 76/78: 20:10 and 20:25 timed out in publish and every retry hit `CYCLE_REHYDRATION_MISMATCH`, so they stay
  incomplete. Later windows, the close and Options were unaffected. Cause of the mismatch is not diagnosed; it has
  followed a publish failure each time it has been seen. A timeout in projection recovers on retry.
- Timeouts come from CPU speed swings, not the hotfix: identical work took 57-228 CPU-seconds on the deployed image
  and 34-153 on the hotfix, and a fixed loop took 3-20s. Windows reports a 76% performance limit on the Balanced
  plan, with no thermal throttling. Live windows share the 300s limit, so this also threatens today's session.
- Deployed API: 22/153 routes 200 (baseline 21). 10 routes newly 200 (`/stock/*/sessions`, `/trading-sessions`).
  9 routes stay 503 `action evidence changed; republish required`, and a completed close does not clear them: hot
  releases set `generated_at` to the market cutoff, so any coverage row received later trips the reader until a
  newer release publishes. Expect them back at the first Nov 3 publish, about 14:35-14:40 UTC; not proven. After
  the rollover deploys, this would repeat every night unless the reader changes.
- The working-tree API still returns 503 on 112 routes (`AttributeError` on 100); do not deploy it.

Second rehearsal, finished 06:14 UTC on the disposable copy (fake provider, no live effect):
- PASS: journals, gap repair, all 15 windows, rollover. All 9 members have both daily bases. SPY raw bars are
  15m 26, 30m 13, 1h 7, 5m 78, 1m 390, 1d 1. The real HISTORY stage from lifecycle image `d9dea05d` returned READY
  for QQQ and SPY at a simulated 14:32 UTC, and dividends read `KNOWN_NO_ANNOUNCED_DIVIDEND`. So Options is unblocked.
- FAIL: the 21:00 window is still not COMPLETE (77/78). Attempt 4 BLOCKED as designed and derived the bars, but
  attempt 5 failed in projection with `Ticker 5m history source drift`. `ticker_projection.py:898-909` allows DERIVED
  own-session 5m bars only while `partial_session`, so at the close it demands native bars nothing writes.
- REGRESSION: on the deployed API, 200 routes fell from 21 to 13 of 153 and 9 baseline-200 routes went 503 with
  `action evidence changed; republish required` (`/tickers/overview`, `/scan/all`, `/stocks/alert-view` and others;
  `/trading-sessions` became 200). Readers reject a release once corporate-action rows arrive after it was generated,
  and the rollover's dividend refresh does that until a newer release publishes. A completed closing window after
  the rollover should clear it, and otherwise the first Nov 3 window should. Neither is proven.
- Do not deploy the working-tree API yet: on this copy the recovery image returned 503 `AttributeError` on 100 routes
  (ticker pages) and `KeyError` on 2. Not diagnosed.
- The one unresolved gap is the stream-end marker at 21:00, which the input gate passes. It is not missing data.

The source now validates legitimate DERIVED session 5m at the close; see the source-only checkpoint below.
The hotfix image contains it, `scoped_rollout.py` is pinned to these images, and the final rehearsal above used them.
No live step, deployment or changed live order is authorized by any of this; live still needs explicit approval.

Live order after approval: copy journals afresh and verify hashes; replay both journals (no provider calls);
gap repair 16:35 (9 requests); 15 windows on the hotfix; rollover (36 requests); closing window on the hotfix
(attempt 4 BLOCKED and derives raw daily, which Options needs; attempt 5 COMPLETE); scoped rollout of bridge and
scheduler (re-run `scoped_rollout.py prepare` first; it refuses at or after 14:15 UTC); verify with the `check`
step on lifecycle image `d9dea05d` and compare the API with `baseline/`
(21/153 routes 200 before recovery). If not clean by about 13:00 UTC, deploy only the bridge and scheduler fixes;
Options then stays blocked for Nov 3. Drop `test_realtime_v2_rehearsal_20261103` when finished.

Do not redeploy lifecycle before the Nov 3 close: K=12 changes the budget scope hash and would act like a budget
bypass. Deploy controller, ingress, preparation, lifecycle, the full scheduler and the sleep gate after that close.
Follow-ups: history binding depth 64 needs compaction within about 3 months; `test_equity_interval_derivation_worker.py`
has a pre-existing collection error; the wider regression has not run.

## H1 source-only fixes and remaining automatic readiness

The completed 45-stage/nine-member bootstrap is preserved; do not repeat it or restore the archive wholesale.
The user selected existing PostgreSQL leases/request-ledger ownership rather than a new C4 migration.

- [Native Options context](../../backend/realtime/advanced_options_equity_input.py) now resolves exact
  15-session XNYS raw daily history through bounded bindings, retaining each actual source cohort/bar/hash.
  Incomplete stock-first requirement declarations block their lanes instead of entering ATR with empty inputs.
- [Recurring rollover](../../backend/realtime/advanced_equity_rollover.py) uses a durable revision lease,
  owner/epoch and current-desired-scope fences before I/O and at completion. Every real HTTP request checks
  lease/open-time headroom and live priority. Facts and ledger outcomes commit atomically; ledger completion
  clocks drive restart-safe backoff. Expired reservations are recovered only with matching prior-owner hashes.
  Unknown/legacy unfenced reservations stay BLOCKED for controlled reconciliation; never reset them.
  Quiesce old unfenced rollover callers before deploying this protocol. Lease duration is 10-3600 seconds.
  Manual reports expose BUSY/backoff and only return success with an empty verified remaining work plan.
- [Closing projection](../../backend/realtime/ticker_projection.py) accepts current Advanced derived 5m
  bars at close only after checking exact native minute parents and aggregates. Missing unrelated older
  minute-chart history does not invalidate those current 5m bars.
- Disposable DB checks passed for overlap, completed-work restart, persistent backoff, expired-owner recovery,
  unproven reservations, stale completion, complete mixed-source native assembly/frozen replay, bootstrap/HV
  identity preservation, reference production and Equity input binding. In distinct focused runs, 125 offline
  and 12 disposable DB tests passed, including existing hardened app-role rollover permissions and
  normal/SESSION_CLOSE publication with the real closing projection.
  The full recovery rehearsal remains unrepeated. No live/provider effects, new schema/grants or budget resets.

Broader H1 is still deferred, not completed by these fixes:

1. Extract ordinary verified adoption from the bootstrap-only adapter. Normal C4 residents still disable it;
   never enable its unconditional whole-weekly revalidation for routine onboarding. Preserve native/derived,
   adjustment, causal source and closure checks, and commit reuse through ordinary C4 chunks.
2. Derive a per-product history inventory from enabled detector lookbacks plus portal/Options requirements;
   the current initial targets use a portal plan with fixed 210-daily/200-hourly warmup minima.
3. Rollover now plans absent, split-stale and invalid-coordinate adjusted history over the existing portal
   targets, using the last completed session as the adjustment horizon even for weekly/monthly bars.
   General policy-driven lookbacks beyond these existing targets remain future work.
4. Qualify retained stream-derived 5m across the next session's bindings and minute-parent retention. The close
   fix accepts current-session derived 5m only; prior-session 5m still has the native-source contract. Define
   supported retained derivation evidence explicitly, without relabeling bars or refetching merely for a label.
5. Replace coverage-only technical assurance with durable product/checkpoint evidence bound to exact bar IDs,
   parameter/code versions and through-clocks. Recompute only dependent tails from the earliest changed input
   through existing materialization/evidence owners, then validate real screening/HV/OS/chart/publication reads.
6. Qualify interrupted adoption/repair and rollover with zero repeat spend, unchanged earlier checkpoints,
   bounded requests/bytes/DB work, live-priority deferral and source/adjustment divergence tests. Compact bounded
   history chains before the existing depth-64 limit. These are implementation acceptance, not a daily manual gate.

Refresh only due catalog/OI/dividend/action context; missing OI and deferred sector dependencies stay UNAVAILABLE.
Stored Nov 2 Treasury was separately VERIFIED for Nov 3 (14 tenors, zero fetches); reuse it, not Oct 30 evidence.

## Residual-finding fixes - source only, Nov 3

- Screening's shared depth-64/identity checks were left unchanged.
- Rollover repairs only the first contiguous invalid range, bounded by the configured fact ceiling. It
  supplies that exact range to the existing preparation adapter, including ranges larger than its ordinary
  20-period default. References run before historical work. Lease fencing, request/byte limits and the
  existing per-action attempt/budget scope remain unchanged; exhaustion is still BLOCKED, never reset.
- The user selected a resident overnight Equity scheduler. It is excluded from session-bound sleep roles;
  the open-minus-25-minute recovery stop and other workers' wake windows were not relaxed.
- Legacy Options calls no longer require runtime publication identity. Realtime Options still requires its
  own cohort and never borrows the Equity cohort.
- Retained context reports SMA50 explicitly; EMA50 remains unavailable. Market Conditions labels and
  distances follow the declared basis, while legacy EMA calculations stay unchanged.
- Focused validation: 41 offline backend tests, 3 frontend regressions and 4 disposable DB checks passed.
  The DB repair uses the real preparation source, fixes stale history beyond 20 sessions, preserves valid
  bar IDs and proves both requests retain one budget scope. TypeScript completed cleanly in cached
  `node:20-alpine` with no network/read-only source; host Node again exited -1073741674.
- Recovery-runner editing/rehearsal belongs to the other session by explicit user choice after concurrent
  runner changes were detected. Its fake-provider fence hook was preserved; this session left no runner edits.
  Do not claim runner approval gates or a full recovery rehearsal passed based on these source checks.

Candidate backend/frontend images must include these latest edits before rehearsal; older pins may predate
them. No live repair, provider fetch, deployment, grant/migration, budget reset or bootstrap rerun occurred here.

## Authority and safety

Target `stocks_realtime_v2` only. Supervised approval V3-FIRST-SUPERVISED-20261102
through Nov3 21:15 UTC. Current live pass remains READ-ONLY; the source remediation is not a rollout.
No unscoped Compose up, live-role recreation, deliberate outage, active restore,
legacy runtime, trading/delivery, budget reset or manual READY/watermark edits.
Preserve dirty source, immutable facts/bindings, original clocks and quarantines.
Do not rerun completed bootstrap or infer authority for a bridge fix.
Evidence: [readiness section12, Independent live monitoring](../ADVANCED_REALTIME_V3_FIRST_RUN_READINESS_2026-11-02.md).

## Current observed deployment

- Another session deployed API, scheduler, canonical-stock-alerts and Options
  WINDOW on `alpha-research-realtime-v3@sha256:9ce840961a554612e055cda9d737d2b8a0bbabf508b1c2804454ff4657cd62e9`.
  Frontend `sha256:1f319b3fa2b7cf9f2dae495895806f85c54f6fbfc9ab4143c774cb67c452d04d`.
  Scoped recreation17:36-17:37; deployment evidence:
  `backend/.realtime-canary/v3-supervised-20261102/portal-publication-deploy/`.
- Other15 containers were unchanged by that rollout. Bridge/ingress still226c8e1c;
  Options lifecycle stilld9dea05d. No monitored container restarts17:55-17:59.
- Publication uses continuous-publication and C7 batch publication. No migration
  or new grants; Options reads remain DISABLED. Current identity resolves cohort
  `52b9fef3-beeb-5181-8fe8-e28fb6f2de48`; no build-time cohort required.
- 45/45 C4 prep COMPLETE14:04:13, nine Equity READY before14:15. Schema/security
  verified; 045/046 only, original backups preserved. No new bootstrap/adoption.
  October30 Treasury cachedVERIFIED14tenors, zero monitoring provider fetches.
- Member160requests/2GiB, account64k/512GiB, Options40M/8GiB/12attempts unchanged.

## Five-sample live observations, 17:55-17:59 UTC

1. Real PORTAL_PUBLISH writes completed:17:35retry,17:40,17:45,17:50,17:55.
   Four normal durations187/235/184/166s; all within their300s release budgets.
   Minimum observed deadline headroom~59s. Overrun recovery not demonstrated.
2.42 releases,37 distinct cutoffs; five dashboard/pattern/summary snapshots each,
   45 individual ticker snapshots,37 Stock Alerts snapshots. Pointer17:55 CURRENT.
   37 alert batches,0compute errors,0matches (truthful no-signal).
3.**Serving still FAILS**, including explicit-cohort controls18:01:50:
   workspace summary422 `identity/checksum drift`; dashboard503
   `identity/checksum/clock drift`; Stock Alerts503 `identity/checksum drift`.
   Frontend proxy also422. Correct cohort header proves this is not just routing.
   Do not treat successful writes/current pointer as successful product reads.
4. Same LIVE Stocks sessionf3c9060f, epoch36, remained active; bars/journal advanced.
   Options192-slot/150MB/one-scope ledger unchanged, no new REST reservations;
   API returns enabled=false/ADVANCED_OPTIONS_V2_DISABLED, read pins0 as expected.
5.19 ticker-test failures/baseline are being investigated elsewhere; this monitor
   did not rerun them or assert that they were pre-existing.

## Serving-integrity source remediation

Read-only predicate audit of current releasebaf814b8 at18:10 proved stored
manifest/payload checksums valid. Aggregate source identity589a46c5 exactly matches
the resolved two-scope history chain, but Dashboard/summary/Stock Alerts manifests
omit `history_scope_chain`; readers recompute obsolete single-binding153932b4.
Immutable snapshots/pointers/clocks were not changed. The fix centralizes
bounded history/reference continuity fields across aggregate producers and makes
Stock Alert/strategy readers use the shared exact resolver. Final focused offline
acceptance:10 passed. Scoped rollout changed only API/scheduler/canonical-alerts:
images65d36a00/61b942e6/a1559bbf. Non-target IDs, LIVE Stocks owner epoch36,
Options192/150MB/one-scope ledger and immutable rows stayed unchanged. New18:40
Dashboard/summary/pattern/alert manifests each carry chain length2 and resolve
exactly. Six implicit/explicit controls now return200.

## Confirmed historical gap and remaining live risk

Old journalb0c80d14 DRAINED16:23:02 while its stream481bccd6 ended16:36:17
with CONNECTIONCLOSEDERROR; replacement began16:36:21. Missing ALL9 ticker bars
at bar_start16:22-16:35 inclusive (126 coordinates); adjacent minutes have9.
16:40 all4 attempts correctly blockedADVANCED_INPUT_NOT_READY; not timeouts.
Deployed bridge226c8e1c AND portal image9ce84096 have identical heartbeat-only
orphan-drain logic, no connection-owner lease check. The AM/60s race remains live.
Working-tree changes are not evidence of deployment. No repair/waiver was made.
Any bridge update/recreation requires a separate controlled scope decision.

## Remaining Equity portal source remediation

Source-only remediation now keeps lag separate from corruption. Per-ticker identity
resolution carries the stored `history_scope_chain`; ticker session lists use the
immutable quote trade date/clock and include the current retained session without
mislabeling it as a frozen close. Screening has explicit `current` and `retained`
controls: current still fails on live-head drift, while retained verifies stored
hashes and serves the saved generation with stale metadata. Market Conditions,
Sector Intelligence and Market Regime routes derive truthful degraded views from
the validated retained workspace summary; they do not call providers or legacy
runtime. Retained strategy reads may select an earlier checksum-valid released
entry, and the UI falls back to an exact pinned Dashboard only for an explicit503
missing strategy release, never for identity/shape errors.

The session selector retains prior data across refresh failures. The shared banner
now covers latest ticker, cross-ticker, screening, strategy and enabled Options
surfaces and reports `STALE DATA`, sessions behind, oldest published timestamp,
affected timeframes and retained setup age. Explicit historical selections are not
misreported as live lag. Options reads remain disabled today; enabled-mode status
uses the retained Options release and never an Equity cohort.

Focused offline evidence: the four retained-reader files first ran461pass/3fail;
all three local failures were corrected, then affected screening/ticker selectors
passed. The final combined remediation/identity selection passed33/33. VS Code
diagnostics report no Python/TypeScript errors. The host Node test/build processes and Playwright MCP
both terminate before diagnostics with Windows exit-1073741674, so browser rendering
could not be certified.

No rollout was authorized or performed. The19:51 read-only deployed matrix therefore
still reflects the old images: eight direct-entry HTML routes return200, but ticker
current/retained charts return422 old identity drift, ticker sessions503 clock
mismatch, screening current/retained404 lineage drift, Market/Sector/status routes
404, and strategies503 absent release. Cross-ticker current/retained remain200;
Options disabled status remains200. This is baseline evidence, not a result of the
source changes. Full serving/browser acceptance remains NO until a separately
authorized scoped rollout and post-rollout direct-entry/network/banner matrix.

Existing malformed old manifests remain fail-closed; no snapshot, clock, pointer,
service, container, database, grant, provider data or live worker was changed.
Current monitoring evidence: observing session files/portal-monitor-20261102T1744/,
portal-serving-evidence.json,1640-gap-evidence.json,deployed-drain-source-metadata.json.
No services, code or budgets changed by monitoring. Read-only helpers completed.
Original backups/quarantines retained; volume restore files identical but historical
DB recovery INVALID (3 boundary/2 codec rows). Member reference refresh fuseNov9;
calendars/dividend rollover/daily catch-up and next-session continuity unvalidated.
Close21:00UTC/13:00Pacific, final21:05:05/13:05:05; next XNYS sessionNov3.
Options real publication, full browser/UI, close/drain and accepted recovery remain
UNVALIDATED. No recurring manual bootstrap or new daily admission gate.
