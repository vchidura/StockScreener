# Advanced Equity V2 Autonomous Docker Rollout Tracker

> **Historical V2 rollout evidence and recovery reference.** Its detailed tests and
> failure lessons remain useful, but external authorization/qualification gates do
> not define the current personal local/Docker runtime. Continue from
> [REALTIME_V3_PIPELINE.md](REALTIME_V3_PIPELINE.md).

Updated: 2026-10-19 UTC
Status: P7 PROVIDER-FREE QUALIFIED; P8 ISOLATED DOCKER DATA/PROJECTION QUALIFIED; LIVE QUALIFICATION OPEN
Initial deployment target: Advanced equities, AM channel, projection-only shadow beside the active Developer lane, exact approved universe
Scale path: 1 -> 9 -> 25 -> 50 -> 100 -> 200 -> 400
Current schema baseline: `stocks_realtime_v2` through migration 022

## Purpose

Track every implementation, verification, recovery and deployment gate required
before Advanced Equity V2 can run daily in Docker without routine operator
intervention.

This tracker is implementation authority, not provider authorization. Checking a
box does not authorize a provider connection, reference refresh, REST repair,
cohort activation, platform pointer movement or production deployment.

Detailed design references:

- [Advanced checkpoint](agent-context/advanced-realtime.md)
- [Forward ingestion](ADVANCED_REALTIME_FORWARD_INGESTION.md)
- [Advanced preflight design](ADVANCED_REALTIME_PREFLIGHT_DESIGN.md)
- [Deployment](DEPLOYMENT.md)
- [Context and handoff contract](AGENT_CONTEXT.md)

## Tracker Rules

- `[ ]` not started.
- `[~]` implementation or verification in progress.
- `[x]` complete with evidence linked in the Evidence Index.
- `[!]` blocked; record the exact blocker and owner decision.
- Complete one phase at a time. Do not mark a phase complete from unit tests
  alone when its exit gate requires PostgreSQL, Docker, outage or market-hours
  evidence.
- Preserve failed sessions, windows, plans, gaps and reports. A retry receives a
  new identity.
- Recheck the next migration number before adding schema. Migration 023 is the
  expected number only while 022 remains the live head.
- `docs/` is gitignored; re-read this file immediately before editing and grep
  the changed task IDs afterward.

## Deployment Objective

An authorized long-lived controller must:

1. Determine each XNYS session, including holidays and half days.
2. Generate a bounded daily child authorization from a valid standing approval.
3. Resolve or provision exactly one session/universe cohort.
4. Start one Advanced stocks ingress owner.
5. Recover boundedly from DNS, TCP, WebSocket and container failures.
6. Retain every durable journal byte and replay committed local offsets.
7. Record exact outage gaps; never invent missing bars.
8. Repair gaps only when the standing authorization includes a REST-repair
   request/byte budget.
9. Run due windows from durable database state, not report-file presence.
10. Keep API, frontend, bridge, finalizer and scheduler independently restartable.
11. Stop provider work at the authorized lifecycle boundary, seal the session,
    finish bounded downstream work and remain alive/idle until the next session.
12. On cold start, audit a bounded number of completed sessions, repair only
    exact missing members/ranges within a separate standing backfill budget,
    publish cohort-locally, then enter `WAIT_NEXT_SESSION`.

Autonomous means no routine daily command entry. It does not mean unlimited
retries, automatic scale-up, self-authorization or silent policy expansion.

## Non-Negotiable Invariants

1. One active Advanced stocks owner under a durable epoch/lease fence.
2. Exact cohort, manifest, session date, AM membership and capability profile.
3. Journal durability precedes canonical bar/checkpoint advancement.
4. A WebSocket reconnect does not claim provider replay.
5. Every disconnect interval remains a gap until exact evidence repairs it.
6. Cumulative daily sessions, connection attempts, decoded bytes and REST repair
   requests/bytes cannot reset after process or container restart.
7. Old owner epochs cannot update sessions, checkpoints, jobs, releases or
   authorization counters.
8. Failed/blocked windows remain immutable evidence; retries use new keys.
9. No fallback to Developer provider identity inside an Advanced cohort.
10. Runtime roles cannot migrate schema or mint standing authorizations.
11. Secrets remain outside plans, reports, logs and Docker image layers.
12. The platform cohort pointer is not moved by this rollout until a separate
    cutover gate is authorized.

## Verified Starting State

| Area | Current evidence | Readiness |
|---|---|---|
| Gateway | `AdvancedGateway.run_with_reconnect` has bounded jitter and a focused reconnect test | Partial; ingress calls `run_session`, not reconnect |
| Stream accounting | Migration 017 stores authorization, byte ceiling, counters, heartbeat and journal generation | Partial; per-session, not cumulative daily-plan accounting |
| Ownership | Registration rejects another nonterminal owner and increments owner epoch | Partial; no stale-heartbeat reclamation |
| Journal | SQLite raw/normalized journal and PostgreSQL bridge/checkpoints are durable and replay-safe within one generation | Partial; no automatic crash discovery/drain controller |
| Gaps | Stream gaps block checkpoint advancement | Partial; no automatic reconnect-range classification/repair controller |
| Scheduler | Five-minute windows are calendar-derived and provider-free | Not restart-safe; exits on first failure and trusts report files |
| Supervisor | Starts API, frontend, finalizer, ingress and scheduler | Canary-only; one child failure stops all children |
| Docker | Existing Compose has canonical services and a legacy `equity-stream` profile | Advanced V2 topology not defined; deployment docs forbid enabling that legacy profile |
| Universe | Control-plane domain supports 1-400 exact symbols | Provisioning CLI is SPY-specific; larger daily cohort preparation is incomplete |
| Scale | Versioned manual-gated ladder exists | 50/100/200/400 runtime profiles and outage evidence do not exist |
| Portal | Stored-only hardened API/frontend and repaired multi-interval SPY publication have passed | Not a current-data or autonomous-runtime certification |

## Target Container Topology

| Container | Responsibility | Restart isolation |
|---|---|---|
| `v2-advanced-controller` | Daily authorization/plan, XNYS lifecycle, stale-owner recovery, session supervision and alerts | Independent; remains alive between sessions |
| `v2-advanced-ingress` | One authorized stocks WebSocket owner and durable local journal writer | Controller-managed bounded restart |
| `v2-advanced-bridge` | Discover/drain current and abandoned journals into canonical bars/checkpoints | Independent continuous worker |
| `v2-advanced-window-scheduler` | Durable due-window discovery and provider-free retries | Independent continuous worker |
| `v2-equity-finalizer` | 1m -> 5m and higher-interval jobs | Independent continuous worker |
| `v2-portal-api` | Hardened stored-only V2 API | Independent reader |
| `v2-frontend` | Realtime-only frontend/proxy | Independent reader |
| `v2-maintenance` | Explicit schema migration/validation only | One-shot profile, never resident |

Do not deploy the current process-group Advanced supervisor as the Docker
production topology. It remains useful for bounded local canaries.

## Gap Register

| ID | Gap | Current risk | Owning phase | Status |
|---|---|---|---|---|
| G01 | No durable standing stream authorization or daily child plan | Slim consolidated implementation exists; Docker/runtime qualification pending | P1/P7/P8 | [~] |
| G02 | Cohort/session/universe resolution is manual | Exact plan binding exists and ambiguous routine resolution fails closed; live qualification pending | P2/P8 | [~] |
| G03 | Runtime tuning is mixed with operator authorization | Profiles exist, but operational values remain unqualified candidates | P2/P8 | [~] |
| G04 | Stream bytes/attempts are limited per session, not cumulatively per daily plan | Consolidated cumulative usage passes disposable tests; Docker restart qualification pending | P1/P3/P8 | [~] |
| G05 | Ingress does not call the reconnect helper | Controller retry implementation exists; Docker fault evidence pending | P3/P8 | [~] |
| G06 | No stale-heartbeat owner reclamation | Disposable race/fence proof exists; Docker SIGKILL proof pending | P3/P8 | [~] |
| G07 | No automatic abandoned-journal discovery/drain | Slim registry/replay exists; Docker crash and isolated-error handling pending | P4/LP-01/P8 | [~] |
| G08 | Reconnect gap boundaries are not reconciled automatically | Classification exists on canonical gaps; SIGKILL and multi-generation proof remain | P4/P5/LP-01 | [~] |
| G09 | REST gap repair is not connected to a standing bounded authorization | Consolidated provider-ledger reservation passes tests; provider qualification pending | P5/P8/P9 | [~] |
| G10 | Scheduler uses report files and exits on the first failed window | Canonical DB-driven implementation exists; Docker restart qualification pending | P6/P8 | [~] |
| G11 | Supervisor couples all process lifecycles | Current canary stops siblings; independent P7 topology waits on EW-01 | EW-01/P7 | [!] |
| G12 | No Advanced V2 Docker profile, volumes or health contracts | Existing legacy stream profile is the wrong runtime; P7 waits on EW-01 | P7 | [!] |
| G13 | No long-lived daily lifecycle controller | Provisional controller exists; Docker lifecycle evidence pending | P3/P6/P7 | [~] |
| G14 | Reference/profile/cohort preparation is SPY-specific | Exact 1-400 source contracts exist; only measured targets may become active | P2/P8 | [~] |
| G15 | No measured per-universe ingress runtime profiles | Larger targets could overload buffers, disk or bridge throughput | P8 | [ ] |
| G16 | No automated outage/full-disk/DB-loss exercise | Restart policy is unproven under the failures it must handle | P8 | [ ] |
| G17 | No market-hours autonomous soak | Offline/Docker tests do not prove entitlement, pacing or continuity | P9 | [ ] |
| G18 | No production rollback/backup/operator alarm runbook for Advanced V2 | Deployment cannot be safely promoted or reversed | P9/P10 | [ ] |
| G19 | No cold-start completed-session catch-up | Canonical planning/executor tests exist; live-priority and Docker catch-up proof pending | P5/P6/LP-01/P8 | [~] |
| G20 | Advanced overlay duplicates canonical orchestration/request/release/alert state | Consolidated to five tables plus one view; 111 offline / 15 DB passed | SC-01 | [x] |
| G21 | Startup backfill can prevent live ingress from reaching its authorized start | Warmup-priority source fix and focused test pass | LP-01 | [x] |
| G22 | Controller waits for prior `DRAINING` journal before reconnect | Safe drain no longer blocks reconnect; Docker proof remains in P8 | LP-01/P8 | [~] |
| G23 | Journal pump progress performs synchronous PostgreSQL writes | Bounded asynchronous reporter passes outage simulation; container outage limits remain P8 | LP-01/P8 | [~] |
| G24 | Stale-owner SIGKILL reclamation does not create a gap | Hardened DB proves one pre-admission gap; Docker SIGKILL remains P8 | LP-01/P8 | [~] |
| G25 | Advanced input gate selects one latest stream session | Exact gate proof is multi-generation, but canonical INPUT_READY does not yet bind that digest | LP-01/EW-01 | [~] |
| G26 | Controller requires a child report for failure classification | Exit plus durable state now classifies no-report failures and path collisions receive new identities | LP-01 | [x] |
| G27 | Bridge continuous loop handles quarantine only | Per-journal quarantine/blocked/retryable isolation passes offline; Docker fault proof remains P8 | LP-01/P8 | [~] |
| G28 | Shared pipeline config requires provider/API secrets in non-provider workers | Direct container entrypoints use process-scoped Docker secrets; only provider services receive the provider key and no runtime process loads the shared `.env` by default | LP-01/P7 | [x] |
| G29 | Automated Advanced windows omit `PORTAL_PUBLISH` | Projection-only is the selected initial mode; cohort-local publication and Advanced alerts require the post-shadow P11 owner gate | LP-01/P9/P11 | [~] |
| G30 | Exact Advanced gate evidence is not part of canonical INPUT_READY identity | Provider-free retry can reuse a receipt that does not bind every contributing generation/bar/checkpoint | EW-01 | [!] |
| G31 | `stages_v3` alert/strategy receipt selection falls back to legacy `EQUITY_CYCLE` | Enabling the alert lane would fail instead of consuming INPUT_READY | EW-01/P11 | [!] |
| G32 | Advanced due windows are iterated oldest-first | A restart backlog can delay the latest live cutoff and violate latest-slot recovery | EW-01 | [!] |
| G33 | No full `stages_v3` reconnect-to-projection rehearsal | Component tests do not prove bridge -> exact receipt -> materialization -> screening/projection -> provider-free retry as one contract | EW-01/P8 | [!] |
| G34 | Actual equity supervisor selection is mutually exclusive | The abstract worker plan models Developer-active plus Advanced-warming, but current launch wiring chooses one supervisor and does not prove both lanes can run independently or together | EW-01/P7/P8/P9 | [!] |

## Dependency Order

```text
P0 contracts
  -> P1 authorization and cumulative accounting
  -> P2 policy, cohort and daily-plan preparation
  -> P3 controller, reconnect and stale-owner recovery
  -> P4 journal discovery and crash replay
  -> P5 gap classification and bounded repair
  -> P6 durable scheduler and daily lifecycle
  -> SC-01 schema consolidation gate
  -> LP-01 live-path separation gate
  -> EW-01 canonical end-to-end wiring closure
  -> P7 independent Docker services and health
  -> P8 fault/capacity qualification
  -> P9 market-hours shadow soak
  -> P10 deployment and rollback
  -> P11 cohort-local publication and Advanced alert qualification
```

P3-P6 may be developed in parallel only after P1's durable identities and
counters are fixed. P7 must not encode provisional CLI contracts that P1-P6
intend to remove.

## SC-01 - Advanced Schema Consolidation Gate

Status: [x] COMPLETE. LP-01 is corrective; EW-01 is complete.

The P0-P6 behavioral evidence is retained. Undeployed migrations 023-028 were
replaced by `023_realtime_advanced_stream_overlay`.

### Goal

Keep one canonical data/orchestration model for Developer and Advanced. Add only
the stream-specific state required by an autonomous, bounded Advanced provider
session: standing authorization, daily admission/usage, technical runtime
profile, owner/session fence, minimal journal registry and explicit gap linkage.

`cohort_id` remains required canonical lineage for both tiers. It is not a
process/retry/scaling state and must not be recreated on reconnect.

Standing authorization is not a market-data scaling primitive. It is a narrow
autonomous-provider safety boundary for entitled/costed socket and REST use. It
must not create a separate projection, screening, alert or publication path.

### Required disposition before P7

| Action | Relations/concepts |
|---|---|
| Keep slim | `advanced_stream_authorizations`, `advanced_ingress_plans`, `advanced_ingress_plan_usage`, `advanced_ingress_runtime_profiles`, `advanced_journal_registry` |
| Extend existing | `stream_sessions`, `stream_gaps`; explicitly relate the ingress plan to the downstream canonical service plan |
| Replace with view/projection | `advanced_stream_authorization_status` |
| Merge into provider/payload ledger | `advanced_gap_repairs`, `advanced_repair_payloads` |
| Merge into canonical orchestration/jobs/barriers | `advanced_gap_dependencies`, `advanced_backfill_runs`, `advanced_backfill_work`, `advanced_scheduler_sessions`, `advanced_window_retries` |
| Merge into canonical release/alert/readiness | `advanced_backfill_releases`, `advanced_gap_alerts`, `advanced_journal_capacity_events` |
| Remove | `advanced_ingress_cohort_bindings`; the exact immutable ingress plan is the binding and ambiguity fails closed |

### Exit Gate

- [x] One source-of-truth matrix is reflected in schema, relation registry and repositories.
- [x] Migrations 023-028 are replaced/squashed before any shared/live installation.
- [x] P0-P6 focused tests are remapped to consolidated tables without weakening
  authorization, cumulative budgets, fencing, journal replay or explicit gaps.
- [x] Candidate heartbeat/stale/disk/retry values remain non-production until owner
  approval or P8 measurement.
- [x] P7 service ownership can be stated without two tables owning the same lifecycle.

### Evidence

- Final Advanced overlay: five tables
  (`advanced_stream_authorizations`, `advanced_ingress_runtime_profiles`,
  `advanced_ingress_plans`, `advanced_ingress_plan_usage`,
  `advanced_journal_registry`) plus redacted
  `advanced_stream_authorization_status` view.
- Existing `stream_sessions` / `stream_gaps` are extended; repair requests and
  payloads use `provider_request_ledger` / `raw_payloads`; backfill/scheduler/
  retry/publication reuse canonical plans/windows/stages/receipts/jobs/barriers/
  portal releases.
- Source/schema delta for SC-01: 414 insertions, 3,135 deletions before document
  updates.
- Focused offline regression:
  `111 passed, 5 warnings in 3.83s`.
- Hardened disposable PostgreSQL:
  `15 passed, 5 warnings in 8.80s`.
- Disposable cleanup: `databases: none`; `roles: none`.

## LP-01 - Live Ingress, Recovery And Publication Separation Gate

Status: [~] CORRECTIVE. Source separation and EW-01 downstream receipt wiring
are implemented. P7.4 still owns process-level shared `.env` removal.

The canonical V2 recovery model is retained: immutable bars, provider ledgers,
jobs/barriers, windows/stages/receipts and releases are downstream state.
Advanced adds socket fencing, SQLite journal generations and explicit gaps, but
downstream recovery must not become an unnecessary live-ingress admission gate.

### Required source corrections

- [x] **LP-01.1 Live beats history.** At the live warmup boundary, authorized
  ingress starts even when historical backfill is `BLOCKED` or incomplete.
  Backfill pauses/rate-limits under its own budget; historical-dependent
  publication stays blocked.
- [x] **LP-01.2 Reconnect is independent of bridge drain.** A sealed prior
  journal may remain `DRAINING` while a new fenced generation starts. Only an
  unsafe/quarantined journal, unfenced owner, exhausted authorization or unsafe
  disk blocks reconnect.
- [x] **LP-01.3 SQLite durability survives PostgreSQL outage.** Journal append
  and normalization do not synchronously depend on registry/heartbeat DB writes.
  Status updates use a bounded asynchronous path; local buffer/disk ceilings
  remain hard stops.
- [x] **LP-01.4 SIGKILL creates gap evidence.** Stale-owner reclamation under
  the feed advisory lock inserts an unresolved gap using old generation,
  heartbeat, durable offsets/watermark and reconnect time before admitting the
  replacement owner.
- [~] **LP-01.5 Input gate accepts exact multi-generation windows.** A five-
  minute window may contain bars from several fenced sessions/generations under
  one ingress plan plus exact authorized repair. Every contributing generation
  must have matching checkpoints and no unresolved overlapping gap.
- [x] **LP-01.6 Reports are never failure authority.** Child exit and durable
  DB/session/journal state classify failures even when no report was created.
  Existing derived-path evidence causes a new attempt identity or explicit
  quarantine, not controller termination.
- [x] **LP-01.7 Bridge blocking is isolated.** Missing gap coverage remains
  blocked evidence; corruption is quarantined; transient DB/infrastructure
  failures back off. No one journal condition terminates discovery/drain for
  unrelated journals.
- [x] **LP-01.8 Configuration and secrets are process-scoped.** Only provider
  ingress/controller receives the API key. Bridge/finalizer/scheduler need app
  DB access; API needs API DB access; frontend receives neither provider nor DB
  secrets. Direct container entrypoints load only process-owned Docker secret
  files; the shared `.env` is an operator input to secret preparation, not a
  runtime mount or fallback.
- [x] **LP-01.9 Publication mode is explicit.** Owner selected projection-only
  shadow on 2026-10-17. The scheduler runs through `EQUITY_PROJECTION`, omits
  `PORTAL_PUBLISH` and does not move the platform pointer.

### Canonical recovery compatibility

The contracts in `REALTIME_V2_PIPELINE.md` and
`Canonical-Terminal-State-Recommendations.md` apply unchanged after canonical
bars exist: latest-slot scheduling, immutable revisions, provider-free receipt
reuse, non-starving retry, exact session-close audit, non-regressing terminal
pointer and atomic publication. They do not by themselves implement WebSocket
owner fencing, journal generations or reconnect gaps.

### Exit Gate

- PostgreSQL outage after socket admission continues local journaling until the
  reviewed local safety budget; no synchronous status write kills the pump.
- One transient disconnect reconnects while the prior journal drains and stays
  within cumulative attempts/bytes.
- SIGKILL creates one unresolved gap before the replacement owner is admitted.
- A reconnect inside one five-minute window reaches INPUT_READY after exact
  multi-generation checkpoint/bar/gap proof.
- Blocked historical backfill does not delay the next authorized live start.
- Child failure before report creation leaves the controller alive with a
  durable classified outcome.
- One blocked/corrupt journal does not stop replay of another healthy journal.
- Non-provider services start without `POLYGON_API_KEY`.
- The selected shadow/publication mode is observable and does not move the
  platform pointer.

### Evidence

- Controller admission tests prove blocked historical work delays only the
  pre-warmup interval and a safely sealed `DRAINING` journal does not block a
  new fenced attempt.
- The bounded asynchronous status reporter isolates PostgreSQL heartbeat and
  registry failures from SQLite append/normalization; the existing buffer,
  batch and disk-headroom limits remain hard stops.
- Hardened PostgreSQL proves stale-owner reclamation creates exactly one
  unresolved gap before replacement admission and one five-minute window can
  bind two fenced journal generations with exact bars and shard checkpoints.
- Missing reports are classified from child exit plus durable session/journal
  state. Collided derived paths allocate a new path-attempt identity.
- Replay discovery classifies one corrupt journal as quarantined, one
  unresolved-gap journal as blocked and one infrastructure failure as
  retryable while continuing to a healthy journal.
- Process-role configuration objects and direct container entrypoints require
  only owned values. Compose secret-scoping tests and rendered-config redaction
  prove provider/app/API secrets do not cross process boundaries.
  Projection-only mode is an explicit scheduler argument and launch-plan field.
- Focused offline regression:
  `70 passed, 5 warnings in 3.29s`.
- Hardened disposable PostgreSQL:
  `16 passed, 5 warnings in 8.95s`.
- Disposable cleanup: `databases: none`; `roles: none`.
- No migration, provider request, service start, Docker operation, retention
  operation, platform pointer movement or P7 implementation occurred.

## EW-01 - Canonical End-To-End Wiring Closure Gate

Status: [x] COMPLETE. Stop before P7; P7 remains a separate implementation
phase.

This gate closes integration gaps found after LP-01 component tests. It does not
change the owner-selected projection-only rollout mode and does not authorize
provider access, Docker deployment, cohort-local publication or platform pointer
movement.

### Tasks

- [x] **EW-01.1** Bind the exact Advanced input-gate digest into canonical
  INPUT_READY identity so receipt reuse proves every contributing bar, stream
  session, journal generation, owner epoch, shard checkpoint, repair segment and
  overlapping-gap verdict.
- [x] **EW-01.2** Make `stages_v3` alert and strategy runners consume
  INPUT_READY rather than legacy `EQUITY_CYCLE`. Keep both stages disabled in
  projection-only automation until P11 authorization.
- [x] **EW-01.3** Restore canonical latest-slot behavior: run the latest due
  live cutoff first while retrying older failed/blocked cutoffs with bounded,
  non-starving fairness.
- [x] **EW-01.4** Add one hardened disposable `stages_v3` rehearsal spanning a
  reconnect inside a five-minute window:
  journal replay -> canonical 1m bars/checkpoints -> exact input gate ->
  five-minute/higher-interval materialization -> INPUT_READY ->
  screening/ticker/Pattern Watch projection -> PORTAL_READY.
- [x] **EW-01.5** Restart that rehearsal after INPUT_READY and after
  PORTAL_READY. Prove provider-free receipt reuse names the same exact lineage,
  creates no duplicate logical work and moves no portal or platform pointer.
- [x] **EW-01.6** Add focused V3 lane tests proving alerts and portal projection
  can run independently after materialization, while a blocked/failing lane
  cannot cancel its sibling. Do not enable alert publication in the initial
  projection-only scheduler.
- [x] **EW-01.7** Align the canonical recovery/applicability documents with the
  implemented verdict and record any remaining limitation explicitly rather
  than leaving stale blocker language.
- [x] **EW-01.8** Define one canonical dual-lane contract with three explicit
  modes: Developer-only; Developer-active plus Advanced projection-only shadow;
  and Advanced-only bounded rehearsal/maintenance. Advanced-only is not serving
  authority and cannot replace the Developer publisher without a later cutover.
- [x] **EW-01.9** Add a hardened same-cutoff dual-lane rehearsal with distinct
  Developer `stages_v2` and Advanced `stages_v3` cohorts/plans/windows/jobs/
  barriers/receipts. Prove no cross-cohort claims or lineage mixing, Developer
  publication remains unchanged and Advanced projection creates no release or
  pointer movement.

### Exit Gate

- [x] The canonical INPUT_READY receipt contains or references the exact Advanced
  gate digest, and a mutated contributing generation/checkpoint/bar/gap makes
  provider-free reuse fail closed.
- [x] A reconnecting `stages_v3` window reaches the same durable INPUT_READY and
  PORTAL_READY identities before and after process restart.
- [x] A restart backlog services the latest eligible live cutoff first and still
  retries every older eligible cutoff within a measured fairness bound.
- [x] Projection-only completion persists screening/projection evidence but creates
  no portal release, cohort-local pointer movement or platform pointer movement.
- [x] V3 alert/strategy receipt compatibility is tested even though those stages
  remain disabled until P11.
- [x] Developer-only and side-by-side Developer-active/Advanced-shadow rehearsals
  produce tier-correct, cohort-isolated durable state. Stopping or blocking one
  lane does not stop, retry or republish the other.
- [x] Exactly one serving/platform publisher remains the active Developer cohort;
  Advanced-only rehearsal and Advanced shadow have no serving authority.

### Evidence

- `INPUT_READY.input_sha256` hashes the exact Advanced gate digest, and the
  bounded receipt build identity carries that full digest for recovery. Reuse
  recomputes the gate and the complete canonical input identity; changed bar or
  checkpoint evidence fails closed.
- The scheduler dispatch order is latest due cutoff, oldest eligible retry, then
  the remaining backlog. The oldest eligible retry therefore waits behind at
  most one newer dispatch in a pass.
- The hardened `stages_v3` rehearsal crosses two fenced journal generations
  inside one five-minute window, runs bridge/checkpoints, exact gate, 5m and
  higher derivation, INPUT_READY, screening/ticker/Pattern Watch projection and
  PORTAL_READY. It restarts after INPUT_READY and reuses INPUT_READY/PORTAL_READY
  with identical receipt lineage and unchanged evidence/job counts.
- The projection-only subrehearsal leaves `portal_releases`,
  `portal_release_current`, canonical `release_current` and
  `portal_cohort_current` unchanged. The same-cutoff Developer `stages_v2` and
  Advanced `stages_v3` cohorts retain distinct plans, windows, jobs, barriers
  and receipts while a stopped Advanced lane does not stop Developer completion.
- Advanced alert, strategy and publication CLI flags fail closed until P11.
  Worker topology explicitly models Developer-only, Advanced-only non-serving
  rehearsal and Developer-active plus Advanced projection-only shadow; only the
  Developer cohort can own a serving publisher.
- Focused offline: `112 passed, 19 skipped, 5 warnings in 10.16s`.
- Changed hardened disposable PostgreSQL:
  `3 passed, 7 warnings in 281.55s`.
- Disposable cleanup: `databases: none`; `roles: none`.
- No provider request, service start, Docker operation, migration, retention
  operation, deployment or platform pointer movement occurred. P7 and P11 were
  not implemented.

## P0 - Freeze Runtime Contracts

Status: [~] CONTRACT IMPLEMENTED; schema identity under SC-01 review.

### Tasks

- [x] **P0.1** Record that one standing authorization may generate daily child
  plans only for allowed universe targets, validity dates and maximum budgets.
- [x] **P0.2** Keep scale-up manual-gated. A daily child plan may reuse the
  currently approved target; it cannot move 9 -> 50 -> 200 -> 400 itself.
- [x] **P0.3** Define reconnect identity: every replacement socket owner creates
  a new immutable stream session, journal generation and owner epoch under the
  same daily ingress plan.
- [x] **P0.4** Define cumulative plan counters: stream sessions, connection
  attempts, decoded bytes, REST repair requests and repair bytes.
- [x] **P0.5** Define controller terminal outcomes:
  `SESSION_COMPLETE`, `AUTHORIZATION_EXHAUSTED`, `BLOCKED_CONFIGURATION`,
  `BLOCKED_ENTITLEMENT`, `BLOCKED_GAP`, `FAILED_INFRASTRUCTURE`.
- [x] **P0.6** Define lifecycle deadlines from XNYS calendar, never fixed UTC
  production times.

### Exit Gate

- One reviewed contract test matrix names every state transition, counter and
  identity used by P1-P6.
- No unresolved question changes schema identity.

### P0 Evidence

- `advanced_ingress_plan.py::ADVANCED_INGRESS_CONTRACT_MATRIX` freezes the
  authorization/plan transitions, five cumulative counters, standing/daily/child
  identities, six terminal outcomes, one-admin-approved-target scale gate and
  XNYS lifecycle source. `test_advanced_ingress_plan.py` covers the complete
  matrix and deterministic identity changes.
- Final offline command on 2026-10-17:
  `backend\.venv\Scripts\python.exe -B -m pytest -p backend.tests.realtime.offline_io_guard backend\tests\realtime\test_advanced_ingress_plan.py backend\tests\realtime\test_prepare_realtime_v2_advanced_ingress.py backend\tests\realtime\test_advanced_stream_control.py backend\tests\realtime\test_realtime_v2_advanced_equity_ingress_script.py backend\tests\realtime\test_realtime_v2_advanced_services.py backend\tests\realtime\test_schema_v2_registry.py backend\tests\realtime\test_security_v2_roles.py -q --tb=short --disable-warnings -p no:cacheprovider`
  -> `44 passed in 0.62s`.

## P1 - Durable Authorization, Daily Plans And Cumulative Budgets

Status: [~] IMPLEMENTED/OFFLINE+DB QUALIFIED; authorization/plan schema must be slimmed under SC-01.

### Proposed Schema

Use the next available migration (expected 023 only if still free).

`rt.advanced_stream_authorizations`:

- `authorization_id`, `parent_authorization_id`
- allowed provider/feed/channel and universe targets
- validity interval
- maximum daily stream sessions and connection attempts
- maximum daily decoded bytes
- maximum REST repair requests and decoded bytes
- runtime policy SHA
- approval source/reference and immutable scope SHA
- status: `READY`, `REVOKED`, `EXPIRED`

`rt.advanced_ingress_plans`:

- `plan_id`, `authorization_id`, `cohort_id`, `manifest_id`
- `session_date`, `universe_target`, mode
- runtime profile SHA
- authorized lifecycle timestamps
- exact child budgets
- status: `READY`, `ACTIVE`, `COMPLETE`, `BLOCKED`, `FAILED`
- created/activated/completed timestamps

`rt.advanced_ingress_plan_usage` or an equivalent locked ledger:

- cumulative sessions, attempts, stream bytes
- cumulative repair requests/bytes
- latest owner epoch/session
- monotonic update/check constraints

### Tasks

- [x] **P1.1** Add migration and relation-registry grants: admin creates/revokes
  standing authorizations; app consumes plans/counters; API reads redacted status.
- [x] **P1.2** Implement immutable dataclasses and canonical hashes in a new
  `backend/realtime/advanced_ingress_plan.py`.
- [x] **P1.3** Implement advisory-lock-protected plan admission and monotonic
  counters. Concurrent starts must allow exactly one final session slot.
- [x] **P1.4** Add `prepare_realtime_v2_advanced_ingress.py`; it generates
  `authorization_id`, derived `authorization_reference`, deterministic
  `plan_id`, redacted report and no provider I/O.
- [x] **P1.5** Add revoke/status modes. Ingress cannot create or widen standing
  authorization.
- [x] **P1.6** Preserve legacy stream-session accounting while binding every new
  session to one ingress plan.

### Focused Verification

- Offline identity/validation tests.
- Disposable hardened PostgreSQL tests for grants, concurrency, replay,
  revocation, expiry and counter exhaustion.
- Assert restart cannot reset cumulative bytes or attempts.

### Exit Gate

- Re-running identical preparation returns the same IDs and rows.
- A changed budget/policy/target creates a different plan.
- The app role cannot mint or widen authorization.
- No provider connection occurs during planning.

### P1 Evidence

- Migration `023_realtime_advanced_ingress_plans.sql`, relation-registry grants,
  `advanced_ingress_repository.py`, the prepare/admin/revoke/status CLI and the
  nullable legacy stream-session binding implement the P1 schema and role split.
  Database triggers preserve immutable authorization/plan scope and prevent
  cumulative counter reset or overrun even outside the repository.
- Hardened disposable command on 2026-10-17:
  `$env:STOCKS_V2_DB_TESTS='1'; backend\.venv\Scripts\python.exe -B -m pytest backend\tests\realtime\test_advanced_ingress_plan_v2_db.py -q -s --tb=short --disable-warnings -p no:cacheprovider`
  -> `3 passed in 2.21s`. The test proves admin-only authorization, redacted API
  status, identical preparation idempotency, concurrent final-slot admission,
  restart-safe cumulative bytes/attempts, expiry, revocation and exhaustion.
- Cleanup command:
  `backend\.venv\Scripts\python.exe -B backend\scripts\check_realtime_v2_disposable_leftovers.py`
  -> `databases: none`; `roles: none`.
- The focused CLI test asserts `provider_requests=0`, no approval reference/DSN
  in the report and no admin/provider connection during plan-only preparation.

## P2 - Versioned Runtime Profiles And Exact Cohort Resolution

Status: [~] IMPLEMENTED/OFFLINE+DB QUALIFIED; redundant cohort binding must be removed under SC-01.

### Tasks

- [x] **P2.1** Add
  `backend/realtime/policies/advanced_equity_ingress_v1.json`.
- [x] **P2.2** Store technical settings by measured universe target:
  maximum message bytes, buffer events/bytes, journal batch bounds, bridge
  batch limit, heartbeat and reconnect timings.
- [x] **P2.3** Keep authorization values out of runtime profiles. Profiles may
  state tested maxima; the daily plan stores the approved actual ceiling.
- [x] **P2.4** Extend `RealtimeV2PipelineConfig` with the policy path/version,
  not one environment variable per tuning value.
- [x] **P2.5** Implement exact cohort resolution by session date, target,
  Advanced tier, AM membership, profile status/expiry and policy hashes.
- [x] **P2.6** Require exactly one match. Never select “latest” ambiguously.
- [x] **P2.7** Enforce manifest session date equals plan session date before
  journal creation or provider connection.
- [x] **P2.8** Generalize SPY-only scope preparation to exact selected
  1/9/25/50/100/200/400 manifests without enabling unmeasured targets.
- [x] **P2.9** Derive report/journal paths from plan/session/attempt; retain
  non-overwrite guards.

### Exit Gate

- Routine planning needs session/target/standing authorization, not a cohort ID.
- Routine ingress needs only a plan ID and execute acknowledgement.
- Missing runtime profile or multiple cohort candidates blocks startup.

### P2 Evidence

- `advanced_equity_ingress_v1.json` contains one immutable profile for every
  `1/9/25/50/100/200/400` target. The SHA covers target, Advanced/STOCKS/AM,
  message/buffer/journal/bridge bounds, WebSocket timeouts, reconnect/backoff,
  heartbeat/stale thresholds, poll intervals and shutdown/drain deadlines; it
  excludes secrets, cohort/session/authorization identities and approved
  request/byte budgets.
- Migration `024_realtime_advanced_runtime_profiles.sql` and
  `advanced_ingress_repository.py` implement admin-only
  `CANDIDATE -> ACTIVE -> SUPERSEDED` profiles with stability/capacity evidence,
  one-rung/next-session promotion checks, explicit cohort binding and immutable
  plan profile/configuration/policy hashes. Existing plans accept their
  superseded pinned profile; new plans require the ACTIVE exact SHA.
- Resolution uses session date, target, Advanced tier, exact AM/total members,
  PASS/unexpired capability, runtime/configuration/policy hashes and no
  latest/newest fallback. Zero matches return `COHORT_NOT_PROVISIONED`; multiple
  matches return `COHORT_AMBIGUOUS` until an admin binds one exact cohort.
- Routine preparation no longer accepts a cohort id or runtime-policy override.
  Routine ingress scope accepts only a plan id plus execute acknowledgement;
  cohort, manifest, authorization, budgets and technical settings are loaded
  from the immutable plan/profile before journal creation or provider access.
  Journal/report paths are derived from plan/session/attempt and refuse overwrite.
- Final offline command on 2026-10-17:
  `backend\.venv\Scripts\python.exe -B -m pytest -p backend.tests.realtime.offline_io_guard backend\tests\realtime\test_advanced_ingress_runtime_profile.py backend\tests\realtime\test_advanced_equity_scale.py backend\tests\realtime\test_equity_pipeline_config.py backend\tests\realtime\test_advanced_ingress_plan.py backend\tests\realtime\test_prepare_realtime_v2_advanced_ingress.py backend\tests\realtime\test_realtime_v2_advanced_equity_ingress_script.py backend\tests\realtime\test_realtime_v2_advanced_services.py backend\tests\realtime\test_provision_realtime_v2_advanced_equity_scope.py backend\tests\realtime\test_advanced_equity_control_plane.py backend\tests\realtime\test_advanced_stream_control.py backend\tests\realtime\test_schema_v2_registry.py backend\tests\realtime\test_security_v2_roles.py -q --tb=short --disable-warnings -p no:cacheprovider`
  -> `62 passed in 1.09s`.
- Hardened disposable command:
  `$env:STOCKS_V2_DB_TESTS='1'; backend\.venv\Scripts\python.exe -B -m pytest backend\tests\realtime\test_advanced_ingress_plan_v2_db.py -q -s --tb=short --disable-warnings -p no:cacheprovider`
  -> `5 passed in 2.11s`.
- Cleanup:
  `backend\.venv\Scripts\python.exe -B backend\scripts\check_realtime_v2_disposable_leftovers.py`
  -> `databases: none`; `roles: none`.

## P3 - Daily Controller, Reconnect And Stale-Owner Recovery

Status: [~] IMPLEMENTED/OFFLINE+DB QUALIFIED; controller state ownership must consolidate under SC-01.

### Tasks

- [x] **P3.1** Add a long-lived `run_realtime_v2_advanced_controller.py`.
- [x] **P3.2** Generate daily child plans from standing authorization before the
  authorized warmup boundary; remain alive/idle outside the session.
- [x] **P3.3** Integrate one reconnect authority using full-jitter exponential
  backoff. Do not nest Docker, gateway and controller retry loops.
- [x] **P3.4** Classify transient DNS/TCP/ping/server failures versus terminal
  auth/entitlement/configuration/budget failures.
- [x] **P3.5** Reset transient-attempt streak only after a configured stable LIVE
  interval.
- [x] **P3.6** On reconnect, atomically consume plan counters, fence the prior
  owner and create a new session/generation/epoch.
- [x] **P3.7** Add lease expiry or stale-heartbeat reclamation under the feed
  advisory lock. Reclamation must compare owner epoch and latest heartbeat.
- [x] **P3.8** Refuse reconnect after close/drain deadline, plan exhaustion,
  revocation or intentional shutdown.
- [x] **P3.9** Change low-level ingress to consume `--ingress-plan-id`; retain
  verbose flags only in an explicitly named canary/debug mode.

### Fault Tests

- DNS failure before connect.
- TCP refusal, auth timeout, ping timeout and provider close.
- SIGKILL before terminal session update.
- Two controllers racing to reclaim one stale owner.
- Restart after session/attempt/byte ceiling exhaustion.

### Exit Gate

- One transient outage reconnects within bounds without duplicate owners.
- Stale owner recovery creates a higher epoch and fences every old write.
- Terminal errors stay blocked and do not restart-loop.

### P3 Evidence

- `run_realtime_v2_advanced_controller.py` is the long-lived daily-plan and
  reconnect authority. It prepares deterministic child plans before the
  profile warmup boundary, waits until plan start, remains alive/idle outside
  session scope and refuses launches after drain, revocation, terminal outcome
  or cumulative attempt exhaustion.
- `advanced_ingress_controller.py` classifies DNS/TCP/auth-timeout/ping/provider
  close as transient and explicit auth/entitlement/configuration/gap/budget
  failures as terminal. Full-jitter exponential backoff is profile-bound; the
  transient streak resets only after the configured stable-LIVE duration.
- The gateway retry helper was removed. `run_realtime_v2_advanced_services.py`
  launches the controller rather than low-level ingress, so Docker/supervisor,
  controller and gateway cannot form nested retry loops. Low-level ingress keeps
  routine `--ingress-plan-id --execute`; bounded controls require
  `--debug-once`.
- Stream registration reclaims only a heartbeat-stale owner while holding the
  feed advisory lock and comparing session, owner, epoch, journal generation
  and heartbeat. The replacement atomically consumes the next attempt, creates
  a new session/generation/higher epoch and rejects every old-owner update.
- Migration `025_realtime_advanced_controller_outcomes.sql` persists immutable
  controller terminal outcomes. A terminal plan remains `COMPLETE`, `BLOCKED`
  or `FAILED` across restart and cannot re-enter admission.
- Final offline command on 2026-10-17:
  `backend\.venv\Scripts\python.exe -B -m pytest -p backend.tests.realtime.offline_io_guard backend\tests\realtime\test_advanced_ingress_controller.py backend\tests\realtime\test_realtime_v2_advanced_controller_script.py backend\tests\realtime\test_realtime_v2_advanced_equity_ingress_script.py backend\tests\realtime\test_realtime_v2_advanced_services.py backend\tests\realtime\test_advanced_stream_control.py backend\tests\realtime\test_advanced_ingress_runtime_profile.py backend\tests\realtime\test_advanced_ingress_plan.py backend\tests\realtime\test_prepare_realtime_v2_advanced_ingress.py backend\tests\realtime\test_equity_pipeline_config.py backend\tests\realtime\test_advanced_gateway.py backend\tests\realtime\test_schema_v2_registry.py backend\tests\realtime\test_security_v2_roles.py -q --tb=short --disable-warnings -p no:cacheprovider`
  -> `80 passed in 1.95s`.
- Hardened disposable command:
  `$env:STOCKS_V2_DB_TESTS='1'; backend\.venv\Scripts\python.exe -B -m pytest backend\tests\realtime\test_advanced_ingress_plan_v2_db.py -q -s --tb=short --disable-warnings -p no:cacheprovider`
  -> `7 passed in 2.68s`. This includes two-controller stale-owner racing,
  higher-epoch replacement, old-write fencing, durable terminal blocking and
  restart-safe attempt/byte exhaustion.
- Cleanup:
  `backend\.venv\Scripts\python.exe -B backend\scripts\check_realtime_v2_disposable_leftovers.py`
  -> `databases: none`; `roles: none`.

## P4 - Journal Registry, Discovery And Crash Replay

Status: [~] IMPLEMENTED/OFFLINE+DB QUALIFIED; retain a slim registry and merge capacity state under SC-01.

### Tasks

- [x] **P4.1** Persist a journal registry binding plan/session/generation, path,
  codec/normalizer, checksums, status and durable offsets.
- [x] **P4.2** Make journal paths container-stable beneath one mounted volume.
- [x] **P4.3** Split bridge execution into an independently restartable process.
- [x] **P4.4** On startup, discover incomplete registered journals and drain
  them before declaring the new session caught up.
- [x] **P4.5** Reconcile SQLite offsets, PostgreSQL checkpoints, owner epoch,
  shard map and canonical bars before replay.
- [x] **P4.6** Quarantine missing/corrupt/mismatched journals; never skip them
  and report healthy.
- [x] **P4.7** Add disk headroom alarms and stop admission before the reviewed
  capacity threshold.

### Exit Gate

- SIGKILL after raw commit, normalized commit, bridge commit and checkpoint
  commit each replays to the same canonical state with no duplicate logical bar.
- A lost journal produces explicit blocked evidence.

### P4 Evidence

- Migration `026_realtime_advanced_journal_registry.sql` and
  `advanced_journal_registry.py` persist immutable plan/session/generation,
  container-relative path, codec/schema/normalizer, shard map, path/metadata/
  sealed-file checksums, status and monotonic raw/normalized/bridge offsets.
  Missing or regressed identities cannot be reported healthy.
- Ingress records a durable capacity PASS/BLOCKED event before stream admission,
  registers the journal before starting its writer, updates raw/normalized
  offsets and seals it to `DRAINING`. The target-specific profile owns minimum
  free bytes/percent; either failed threshold blocks admission.
- `run_realtime_v2_advanced_journal_replay.py` is an independently restartable
  discovery/bridge process. `run_realtime_v2_advanced_services.py` launches it
  separately from the controller and low-level ingress. The controller waits
  for registered/draining journals and blocks on quarantine before declaring a
  terminal session outcome.
- `advanced_journal_replay.py` validates the mounted relative path, SQLite
  integrity/generation/schema/checksum, resumes raw-only normalization, then
  reconciles PostgreSQL shard checkpoints, owner epoch, shard map and canonical
  source offsets before idempotent bridge replay. Checkpoint-ahead registry
  recovery advances evidence without duplicating the logical bar.
- Missing, corrupt, checksum-mismatched, offset-regressed or lineage-mismatched
  journals transition to durable `QUARANTINED` evidence. The lost-journal DB
  fault test records `MISSING_JOURNAL`; it is not counted as drained.
- Final offline command on 2026-10-17:
  `backend\.venv\Scripts\python.exe -B -m pytest -p backend.tests.realtime.offline_io_guard backend\tests\realtime\test_advanced_journal_registry.py backend\tests\realtime\test_realtime_v2_advanced_journal_replay_script.py backend\tests\realtime\test_advanced_equity_ingress.py backend\tests\realtime\test_advanced_equity_journal_bridge.py backend\tests\realtime\test_realtime_v2_advanced_equity_bridge_script.py backend\tests\realtime\test_realtime_v2_advanced_equity_ingress_script.py backend\tests\realtime\test_realtime_v2_advanced_services.py backend\tests\realtime\test_advanced_ingress_controller.py backend\tests\realtime\test_realtime_v2_advanced_controller_script.py backend\tests\realtime\test_journal.py backend\tests\realtime\test_normalized_journal.py backend\tests\realtime\test_schema_v2_registry.py backend\tests\realtime\test_security_v2_roles.py -q --tb=short --disable-warnings -p no:cacheprovider`
  -> `76 passed in 2.75s`.
- Hardened disposable command:
  `$env:STOCKS_V2_DB_TESTS='1'; backend\.venv\Scripts\python.exe -B -m pytest backend\tests\realtime\test_advanced_ingress_plan_v2_db.py -q -s --tb=short --disable-warnings -p no:cacheprovider`
  -> `9 passed in 2.84s`. The suite starts from a raw-only SQLite commit,
  resumes normalization, seals a stale writer, commits one canonical bar and
  checkpoint, replays again with zero duplicate bars and separately proves
  durable lost-journal quarantine.
- Cleanup:
  `backend\.venv\Scripts\python.exe -B backend\scripts\check_realtime_v2_disposable_leftovers.py`
  -> `databases: none`; `roles: none`.

## P5 - Gap Classification And Authorized Repair

Status: [~] IMPLEMENTED/OFFLINE+DB QUALIFIED; repair/backfill/release/alert state must consolidate under SC-01.

### Tasks

- [x] **P5.1** Record disconnect start, last durable offset/watermark, reconnect
  time and first post-reconnect market event.
- [x] **P5.2** Distinguish local offset discontinuity, provider-time outage,
  no-trade interval and unknown continuity.
- [x] **P5.3** Keep affected input/window barriers blocked until exact evidence
  resolves the gap.
- [x] **P5.4** Connect eligible gaps to the coordinated provider budget ledger.
- [x] **P5.5** Auto-plan REST repair only when the daily/standing authorization
  includes exact request and decoded-byte budget.
- [x] **P5.6** Reserve before I/O, seal raw response, replay provider-free and
  mark repaired only after exact canonical coverage.
- [x] **P5.7** When repair is unauthorized/exhausted, continue future ingress,
  preserve the gap, block affected windows and raise an operator alert.
- [x] **P5.8** Add a cold-start coverage audit for an approved maximum of two
  completed XNYS sessions. Compare exact cohort members and raw RTH 1m/5m
  membership before any provider I/O.
- [x] **P5.9** Plan only missing ticker/session/range work. A complete SPY row
  set must not be fetched again merely because eight other members are missing.
- [x] **P5.10** Reconcile sealed payloads, request ledgers, journals, canonical
  bars and receipts provider-free before reserving a new request.
- [x] **P5.11** Build the currently missing historical 1m executor. Existing
  `repair_realtime_v2_recent_sessions.py` covers native 5m/15m/30m only and is
  not sufficient for complete 1m charts.
- [x] **P5.12** From exact repaired 1m, derive final 5m/15m/30m/1h/raw-1d
  locally. Refresh adjusted daily/weekly/monthly only through their separate
  authorized basis contract; never label raw derivation adjusted.
- [x] **P5.13** Give startup backfill a distinct cumulative request/byte/cost
  allocation under the standing authorization. It must not consume or reset the
  live stream budget implicitly.
- [x] **P5.14** Re-run exact coverage after persistence. Publish only a new
  cohort-local release after required jobs/barriers complete; never move the
  platform pointer.
- [x] **P5.15** Persist controller states `BACKFILL_PLANNED`,
  `BACKFILL_RUNNING`, `BACKFILL_BLOCKED`, `BACKFILL_COMPLETE` and
  `WAIT_NEXT_SESSION` with exact reasons and next action.

### Exit Gate

- Reconnect alone never closes a gap.
- Exact authorized repair releases only dependent windows.
- Unauthorized repair performs zero provider requests.
- A cold Docker start with SPY complete and the other eight target-9 members
  missing one completed session repairs only those eight, passes exact 1m/5m
  coverage, publishes cohort-locally and enters `WAIT_NEXT_SESSION`.
- The same exercise passes for two missing completed sessions without exceeding
  its exact planned request/byte ceiling.
- When backfill is unauthorized or exhausted, the controller remains
  `BACKFILL_BLOCKED`, serves retained data explicitly and performs zero hidden
  acquisition.

### P5 Evidence

- Migration `027_realtime_advanced_gap_repair.sql` persists disconnect/reconnect
  evidence, deterministic continuity classification, exact repair scope,
  sealed payloads, blocked dependencies, API-readable operator alerts,
  cold-start runs/work, distinct cumulative startup allocation and cohort-local
  releases/controller state.
- `advanced_gap_repair.py` classifies local offset discontinuity, provider-time
  outage, explicit no-trade evidence and unknown continuity. Reconnect evidence
  leaves `recovery_status=UNRESOLVED`; only exact canonical verification changes
  it to `REPAIRED` and releases that gap's dependent windows.
- Eligible repairs bind to the coordinated provider ledger and the plan's
  separate REST request/byte counters. Reservation precedes transport I/O; raw
  payloads are sealed in both canonical payload storage and repair evidence.
  Reruns load sealed content provider-free. Unauthorized/exhausted plans keep
  the gap unresolved, persist a critical operator alert and create no request.
- The historical raw-1m executor contract supports authorized fetch or sealed
  replay and derives only raw-unadjusted `5m/15m/30m/1h/1d` locally.
  Adjusted daily/weekly/monthly remain outside this basis contract.
- Cold-start planning accepts at most two completed XNYS sessions and creates
  work only for exact missing ticker/session/ranges. Hardened target-9 evidence
  with complete SPY and eight missing members produces exactly 8 request scopes
  for one session and 16 for two; SPY is absent from every missing scope and
  retained 5m membership remains complete.
- Backfill request/byte/cost counters are durable and separate from stream
  counters. States persist exact reason/next action through
  `BACKFILL_PLANNED/RUNNING/BLOCKED/COMPLETE/WAIT_NEXT_SESSION`. Unauthorized
  startup remains `BACKFILL_BLOCKED` with zero ledger requests. Completed work
  creates a cohort-local release with `platform_pointer_advanced=false` and
  enters `WAIT_NEXT_SESSION`.
- Final offline command on 2026-10-17:
  `backend\.venv\Scripts\python.exe -B -m pytest -p backend.tests.realtime.offline_io_guard backend\tests\realtime\test_advanced_gap_repair.py backend\tests\realtime\test_gap_repair_plan.py backend\tests\realtime\test_advanced_stream_control.py backend\tests\realtime\test_advanced_ingress_controller.py backend\tests\realtime\test_realtime_v2_advanced_controller_script.py backend\tests\realtime\test_realtime_v2_advanced_equity_ingress_script.py backend\tests\realtime\test_advanced_intraday_repair.py backend\tests\realtime\test_equity_bulk_backfill.py backend\tests\realtime\test_schema_v2_registry.py backend\tests\realtime\test_security_v2_roles.py -q --tb=short --disable-warnings -p no:cacheprovider`
  -> `68 passed, 5 warnings in 3.20s`.
- Hardened disposable command:
  `$env:STOCKS_V2_DB_TESTS='1'; backend\.venv\Scripts\python.exe -B -m pytest backend\tests\realtime\test_advanced_ingress_plan_v2_db.py backend\tests\realtime\test_gap_repair_plan_v2_db.py -q -s --tb=short --disable-warnings -p no:cacheprovider`
  -> `14 passed, 5 warnings in 9.14s`.
- Cleanup:
  `backend\.venv\Scripts\python.exe -B backend\scripts\check_realtime_v2_disposable_leftovers.py`
  -> `databases: none`; `roles: none`.

## P6 - Restart-Safe Scheduler And Daily Lifecycle

Status: [~] IMPLEMENTED/OFFLINE+DB QUALIFIED; duplicate scheduler state must consolidate under SC-01.

### Tasks

- [x] **P6.1** Replace report-file completion authority with durable
  plan/window/stage/receipt state.
- [x] **P6.2** Discover due windows from XNYS calendar and configured first
  authorized cutoff.
- [x] **P6.3** Add durable retry identities and bounded backoff for retryable
  failed/blocked windows.
- [x] **P6.4** Reuse exact INPUT_READY/PORTAL_READY receipts provider-free.
- [x] **P6.5** Keep scheduler alive when a window is input-blocked; later
  complete windows may proceed only when contractually independent.
- [x] **P6.6** Stop creating new windows after close, drain bounded work, persist
  session status and remain idle for the next session.
- [x] **P6.7** Handle holidays, DST and early closes with calendar tests.
- [x] **P6.8** Derive child report paths from window/retry identities; report
  files are evidence, not control state.
- [x] **P6.9** At controller/container startup, run the bounded completed-session
  coverage audit before entering `WAIT_NEXT_SESSION`.
- [x] **P6.10** If backfill remains blocked when the next live warmup boundary
  arrives, prioritize authorized live ingress so missing history does not cause
  another missed session. Keep historical-dependent publications blocked and
  pause/rate-limit backfill under its separate budget.

### Exit Gate

- Restart at every window state reaches the same durable result.
- Existing failed reports never prevent a valid database-identified retry.
- A blocked cutoff does not terminate the scheduler process.
- After successful cold-start backfill with no current session due, the
  controller remains alive/idle and starts the next XNYS session automatically.

### P6 Evidence

- Migration `028_realtime_advanced_scheduler.sql` persists one scheduler
  lifecycle row per authorization/session and immutable retry identities with
  retry timing, receipt reuse flags and derived evidence-report paths.
- `advanced_scheduler.py` derives recovery solely from durable service plans,
  window/stage states and exact INPUT_READY/PORTAL_READY receipts. Receipt reuse
  verifies cohort, cutoff, universe, build and `stages_v3`; report files are
  never read to decide completion or retry.
- Retry window/report identities are deterministic by scheduler session,
  cutoff and attempt. Runtime-profile max attempts and exponential backoff bound
  retries. Existing FAILED/MISSED/RETAINED_GAP state creates a new identity;
  PENDING or lease-expired RUNNING state resumes the same identity.
- The scheduler processes every due cutoff independently. A failed/input-blocked
  cutoff records durable failure and does not terminate the process or prevent a
  later contractually independent cutoff from completing.
- XNYS calendar discovery honors the first authorized cutoff, holidays, DST and
  the exact 42-window half day. After close it creates no new windows, persists
  `DRAINING`, then `IDLE/WAIT_NEXT_SESSION` after the profile drain deadline and
  automatically seeks the next session's exact ingress plan/cohort.
- Startup runs the provider-free, maximum-two-session exact 1m/5m coverage audit.
  A blocked historical backfill is durably changed to
  `LIVE_INGRESS_PRIORITY` / `PAUSE_BACKFILL_KEEP_HISTORY_PUBLICATION_BLOCKED`;
  authorized live windows continue while historical-dependent publication
  remains blocked under the separate backfill budget.
- Final offline command on 2026-10-17:
  `backend\.venv\Scripts\python.exe -B -m pytest -p backend.tests.realtime.offline_io_guard backend\tests\realtime\test_advanced_scheduler.py backend\tests\realtime\test_realtime_v2_advanced_equity_scheduler_script.py backend\tests\realtime\test_realtime_v2_advanced_equity_window_script.py backend\tests\realtime\test_advanced_session_windows.py backend\tests\realtime\test_realtime_v2_advanced_services.py backend\tests\realtime\test_advanced_ingress_runtime_profile.py backend\tests\realtime\test_advanced_gap_repair.py backend\tests\realtime\test_schema_v2_registry.py backend\tests\realtime\test_security_v2_roles.py -q --tb=short --disable-warnings -p no:cacheprovider`
  -> `55 passed, 5 warnings in 2.63s`.
- Hardened disposable command:
  `$env:STOCKS_V2_DB_TESTS='1'; backend\.venv\Scripts\python.exe -B -m pytest backend\tests\realtime\test_advanced_ingress_plan_v2_db.py -q -s --tb=short --disable-warnings -p no:cacheprovider`
  -> `13 passed in 3.23s`. The suite proves restart-idempotent retry identity,
  exact sealed INPUT_READY reuse, later-cutoff independence, durable IDLE
  lifecycle, bounded startup audit and live-over-backfill priority.
- Cleanup:
  `backend\.venv\Scripts\python.exe -B backend\scripts\check_realtime_v2_disposable_leftovers.py`
  -> `databases: none`; `roles: none`.

## P7 - Independent Docker Services And Health Contracts

Status: [~] PROVIDER-FREE QUALIFIED. The provider profile is implemented but
was not started under the authorized provider-free scope. Loaded ingress drain
and controller recovery remain P8 evidence.

### Tasks

- [x] **P7.1** Add a new explicit Compose profile, for example
  `realtime-v2-advanced`; do not reuse legacy `equity-stream`.
- [x] **P7.2** Add controller, ingress, bridge, scheduler, finalizer, V2 API and
  frontend as independent services.
- [x] **P7.3** Mount the journal volume and bounded report/log volume with
  reviewed ownership and permissions.
- [x] **P7.4** Supply DSNs/API keys via process-specific runtime
  secrets/environment, never image layers, launch-plan JSON or one shared
  backend `.env`. Remove `load_dotenv(..., override=True)` and whole-file
  `dotenv_values(...)` from non-owning runtime processes. Prove controller/
  ingress alone receive the provider key, bridge/finalizer/scheduler receive
  only app DSN, API receives only API DSN and frontend receives neither.
- [x] **P7.5** Add health endpoints/checks:
  process liveness, plan readiness, stream heartbeat, data watermark, unresolved
  gaps, latest due window and publication health.
- [x] **P7.6** Set `restart: unless-stopped` only after controller retry loops are
  bounded and normal post-close operation remains alive/idle.
- [x] **P7.7** Ensure ingress failure does not restart API/frontend or other
  workers.
- [~] **P7.8** Add graceful SIGTERM budgets and prove journal/DB drains fit
  Docker stop timeout.
- [x] **P7.9** Update deployment documentation with exact profile commands,
  status checks, stop, backup and rollback.
- [x] **P7.10** Replace the mutually exclusive runtime selector as the
  deployment authority. Keep the existing Developer services independently
  operable and add the Advanced profile so operators can run Developer-only,
  Advanced-only bounded rehearsal or both side by side without changing one
  pipeline enum to disable the other.
- [x] **P7.11** Give each tier separate process names, health state, logs,
  restart scope, provider/account allocation and cohort identity. A Developer
  failure must not restart Advanced, and an Advanced failure must not restart or
  reconfigure Developer.

### Exit Gate

- [x] `docker compose config` passes with no Advanced secrets printed.
- [x] Child-process configuration tests prove no process loads an unowned secret,
  including through a default `.env` fallback.
- [x] Restarting API or bridge leaves unrelated services healthy and preserves
  their container identities.
- [~] Controller and external ingress retry loops are bounded in source/tests;
  live provider recovery remains P8.
- [x] Compose/configuration tests pass for Developer-only, Advanced-only rehearsal
  and Developer-active plus Advanced-shadow modes.
- [x] Side-by-side startup retains the Developer serving publisher and gives
  Advanced zero portal/platform publication authority.

### Evidence

- `docker-compose.realtime-v2.yml` is standalone from the legacy Compose stack.
  Provider-free and provider profiles pass `docker compose config`; rendered
  JSON was checked against all three Docker secret values with
  `COMPOSE_REDACTION_PASS`.
- Direct controller/ingress/bridge/scheduler/window/finalizer/API entrypoints no
  longer load a default shared `.env`. Docker secret files are scoped by
  component; only controller/ingress receive the provider key.
- Image-context audit found `.realtime-canary`, `backups`, `.cache`, tests and
  SQLite files were not all excluded. The unsafe build was stopped before
  export; an earlier oversized image was replaced. Exact 13.78/18.32 GB cache
  layers, the aborted build record and the 19.6 GB image tag were removed.
  Final context is 30.47 MB; the 1.36 GB image proves `/app/.env`,
  `/app/.realtime-canary`, `/app/backups`, `/app/.cache` and `/app/tests` are
  absent.
- Authorized live migration `023_realtime_advanced_stream_overlay` was applied,
  grants were re-hardened with 11/11 checks, then the live database passed 72
  structural and 12 security checks in phase `AUDITED`.
- Deployed provider-free containers: V2 API, frontend, bridge and finalizer.
  All are healthy. API health is `READY`, frontend and stored screening proxy
  return HTTP 200, bridge reports zero quarantined/draining journals, zero lag
  and zero unresolved gaps.
- API and bridge restart rehearsals left sibling container identities unchanged.
  Initial API replacement exposed stale Nginx DNS and a 502; the Advanced proxy
  now re-resolves through Docker DNS, and a second API force-recreate retained
  the frontend identity while the screening proxy returned 200. The Advanced
  cohort has zero pending finalizer jobs and one retained
  `EQUITY_WORKER_CONTRACT_INVALID` job; scheduler health retains 16 historical
  failed windows and no open windows. These are evidence, not restart authority.
- Retained Advanced publication is `DEGRADED` evidence (8 releases, one
  cohort-local pointer) with zero platform pointer. No new publication or
  platform pointer movement occurred. Provider services and scheduler remain
  stopped.
- Focused offline: `85 passed, 5 warnings in 4.82s`. Hardened overlay grant
  check: `1 passed, 5 warnings in 6.24s`. Disposable cleanup: databases and
  roles none.

## P8 - Capacity Profiles And Fault Qualification

Status: [~] PROVIDER-FREE CONTAINERS, OFFLINE CONTRACTS AND ISOLATED SPY
DOCKER DATA/PROJECTION RECREATION QUALIFIED. Full Advanced target-1/target-9
live capacity/fault qualification remains open.

### Tasks

- [~] **P8.1** Build a deterministic recovery rehearsal script instead of
  manual fault commands. Source contracts and isolated Docker cold/recreated-worker
  data/projection are verified; live controller/provider fault orchestration remains open.
- [~] **P8.2** Exercise DNS loss, TCP refusal, socket close, delayed pong,
  malformed/oversized message, full buffer, slow bridge, PostgreSQL outage,
  journal full/corrupt/missing and SIGKILL.
- [~] **P8.3** Measure received/admitted rates, queue age, journal growth,
  bridge lag, finalizer/materialization lag, screening/projection latency,
  window deadline utilization, CPU, RSS, PostgreSQL load and catch-up time.
- [~] **P8.4** Qualify target 1, then 9. Add 25/50/100/200/400 profiles only
  after same-session before/after measurements.
- [~] **P8.5** Verify cumulative budget enforcement across every restart test.
- [x] **P8.6** Verify no disposable database/role or orphan process remains.
- [~] **P8.7** Exercise container cold start with zero, one and two missing
  completed sessions, including partial-member coverage and provider outage
  during backfill.
- [ ] **P8.8** Qualify 1/2/4/8 finalizer/materialization worker counts and the
  alert/portal lane budgets using the EW-01 V3 rehearsal. Do not copy one
  candidate ingress profile across larger targets without measured queue,
  bridge, database and projection evidence.
- [ ] **P8.9** Repeat target-1 and target-9 qualification while the normal
  Developer lane is active. Measure combined provider/account allocation,
  database pools/locks, CPU/RSS/I/O and per-tier window latency; prove either
  tier's outage/backlog cannot starve the other.

### Exit Gate

- Target 9 meets sustained-rate and outage-recovery gates with at least 1.3x
  measured headroom across ingress, bridge, materialization and projection.
- Every target-9 five-minute projection-only window meets its data and portal
  lane deadlines during normal flow and bounded recovery.
- The Developer lane retains its existing cadence, publication correctness and
  latency envelope while Advanced target-9 shadow is running.
- Larger targets remain blocked until individually evidenced.

### P8 Evidence To Date

- Owner resumed provider-free P8 work on 2026-10-19 UTC. The source-only
  `backend/scripts/rehearse_realtime_v2_advanced_recovery.py` defaults to PLAN;
  Initial v1 `--execute --report-path <new-json>` ran a 14-scenario contract matrix
  under PostgreSQL/provider and socket/DNS denial, with a 120-second timeout.
  Windows asyncio's private standard-library socket pair is permitted without
  allowing direct connections. Existing loopback tests retain their default
  behavior because the network guard is opt-in.
- The actual script returned `PASS_OFFLINE`: 30 cases passed across transport
  injection, malformed/oversized input, bounded/stalled handoff, progress-report
  outage, disk admission/SQLITE_FULL, corrupt/missing journal, local
  committed/uncommitted process crashes, gap isolation, scheduler retry and
  authorization bounds. The persistent JSON report records exact case outcomes
  and explicitly sets `live_qualified=false` and `production_ready=false`.
- New fault/runner tests: 20 passed. Existing gateway/container regressions:
  28 passed. Reporting rejects failed, skipped, missing and invalid JUnit
  evidence, nonzero runner exits and timeouts. No provider request, service
  change, PostgreSQL connection, migration, publication or pointer move occurred.
  These are source contracts, not real provider/Docker/PostgreSQL fault,
  cumulative-database-budget, capacity/headroom or market-hours qualification.
- A separately approved app-role READ ONLY / repeatable-read preflight at
  `2026-10-19T05:55:56Z` found zero standing authorizations, registered runtime
  profiles and daily plans. The one-name SPY shadow capability expired October
  17; its Advanced reference seed expired October 16. App/API/provider Docker
  secret files exist and Compose redaction passes, but provider entitlement was
  not contacted. Startup still renders placeholder authorization/cohort/session
  values; controller/ingress/scheduler remain absent and Advanced alerts return
  503. Schema migration-table SELECT is denied to app: this check was explicitly
  left uncertified, with no privilege escalation. No database/provider/service
  mutation occurred. Exact live authorization and reference-refresh scope are next.
- Owner selected reuse of the prior bounded SPY live ingestion/publication
  evidence, not another full-day SPY functional test. Plan only the missing
  Docker controller/scheduler startup, fenced ingress restart, scheduler receipt
  reuse, cumulative-counter preservation and graceful drain checks. These do
  not replace target-9 capacity or P9 soak, and grant no live execution authority.
  Fresh entitlement proof is still subject to the provisioning CLI's four-hour
  TTL; reusing functional evidence does not renew it. The SPY reference refresh
  CLI now supports an explicit matching `--authorized-session-date` for a new
  date while preserving its October 16 legacy guard. Runtime-profile activation requires genuine
  stability/capacity evidence, not fabricated hashes from the offline report.
  Owner-approved planning envelope: existing configured Massive Advanced account,
  SPY/AM only, at most 600 connected seconds, three stream sessions, three
  connection attempts and 10 MiB decoded cumulatively across restarts; zero
  REST repair/backfill. Exact session/start/stop, reference-refresh/entitlement
  budgets and named container actions remain unapproved. Normal daily plans
  span XNYS open through close; the optional bounded canary now hashes and stores
  its exact shorter window in the existing canonical plan fields. The saved
  bounded automation manifest is PLAN ONLY / NOT EXECUTABLE; no live permission
  is inferred from acceptance of these planning limits.
- Bounded source preparation is verified: 93 focused offline checks passed;
  expanded v2 source rehearsal passed 52 cases across 16 scenarios; three
  hardened disposable PostgreSQL checks proved persisted cutoff/counters,
  no widening of the same daily plan, exact end-boundary rejection and normal
  restart/fencing behavior. Disposable databases/roles: none. Compose syntax,
  secret redaction and scoped whitespace checks pass. No live provider request,
  database write, image build, container change, publication or pointer move.
  No schema migration was needed. External ingress can claim a READY plan,
  closing the startup handoff deadlock before admission atomically marks ACTIVE.
  Canary shutdown is reserved inside the bound (30 seconds with the current
  candidate); provider work stops by window-end minus that budget, with monotonic
  fallback and supervisor termination. Scheduler uses the same provider cutoff
  and can drain its last eligible window without scheduling outside the bound.
  Controller canary dates are fixed rather than advanced into another session.
  Actual Docker fault orchestration, fresh live entitlement/reference/profile
  qualification and the exact execution manifest remain open.
- Owner authorized an isolated provider-free qualification, then one fresh
  same-image window after the original deadline. The retained candidate is
  `alpha-research-realtime-v2-p8-qualification:b6ae0b17e9c4426faf54cd2f348fcd20`,
  image `sha256:580b5c01c4fa758977a84617cd42c51acd6a7e1d9d58f0d482fb8bfd8298380a`.
  `scripts/qualify_realtime_v2_advanced_docker.py` and its production-code
  worker replay exact SPY/AM synthetic bytes through SQLite, PostgreSQL bridge,
  finalizer and projection-only `stages_v3`, then recreate the worker for receipt
  reuse. Actual Docker limits were two CPUs / 2 GiB with a read-only root and
  no provider key. The worker used an app-only non-superuser, NOCREATEDB/
  NOCREATEROLE LOGIN, not a bootstrap credential.
- The final report is `PASS_PROVIDER_FREE_DOCKER`: workload 48.766 s; cold
  V3 window 6.006836 s; cold worker CPU 3.421893 s; peak process RSS 336.85 MiB
  and cgroup peak 335.83 MiB. Recreated-worker body took 0.568253 s. The
  14,341 bar / 46 evidence / one screening-generation counts stayed unchanged
  on receipt reuse, with INPUT_READY and PORTAL_READY recovered and zero
  publications, alert/outbox output or prohibited stage execution. Both worker
  containers, temporary credential, database and roles were removed; final
  disposable check found none. All five monitored live container identities
  and start times stayed unchanged. The image and all failed/success reports remain.
- Qualification exposed and fixed the real delta-loader omission of
  `allow_late_identity_continuity`: it now forwards the existing flag unchanged,
  default false, rather than bypassing the identity gate. Final loader/safety
  tests pass 213 / one DB test skipped under the I/O guard; the exact
  restricted-login V3/replay test separately passes on hardened disposable
  PostgreSQL. Initial Docker failure was a missing test-database CONNECT grant
  for NOINHERIT LOGIN; only that owned database now grants CONNECT. Stderr
  retention and case-insensitive Docker absence handling preserve decisive
  failure evidence and verify cleanup. No live permission or profile changed.
- This is genuine controlled-fixture execution evidence, not live qualification:
  one window's 300/6.006836 completion-budget ratio must not be called sustained
  rate headroom. Stable market sessions, actual provider/controller/scheduler
  faults, target-9 capacity, combined live PostgreSQL load, next-session
  operation and P11 alerts/publication remain open. Synthetic control-plane
  fixture states never authorize live runtime-profile activation; the older
  METRICS_INCOMPLETE/HOLD evidence was not overwritten.
- Evidence-to-profile review matched current target-1 profile
  `5156b861f053356ce05bef1c3ca4c4822a8463693e24a0133459e1dec0f6ad9d`,
  policy `2504ed8ced910fc42a2a5c69a093c2da8f5d9cfd36d3aa7f11b729c815e721dc`,
  candidate image580b5c01 and immutable Docker report
  `1B00AAB97A732041E7201D86DCADEA0C8CC0D64A5C8B7AC316B2180A0C755F23`.
  The owner then conditionally approved exact manifest18191A6D for Oct19
  09:30-09:40 America/New_York (13:30-13:40Z), including its explicit bounded
  probe/reference, target-1 metadata, four new-container and supervision scopes.
  That approval requires all compatibility gates; it grants no bypass or migration.
  Standalone operator configuration lacked a policy path. Per-run
  `--ingress-policy-path` / `--ingress-policy-version` and optional target-only
  `sync-profiles --universe-target` avoid shared-environment edits or incidental
  registration of larger targets. Original ladder positions are preserved.
  Fifteen focused tests pass; no provider or live database access occurred.
- The mandatory approved preflight returned BLOCKED. Live head is
  `024_realtime_session_archive_build_identity`; qualified image580b5c01 also
  contains `025_realtime_advanced_options_control_plane`,
  `026_advanced_options_flow_evidence`,
  `027_advanced_options_provider_references`,
  `028_advanced_options_reference_state` and
  `029_advanced_options_rolling_checkpoints`, all absent live. Common checksums
  match and there are no unexpected live versions. The current workspace
  registry additionally differs from the frozen image registry.
  The actual transaction was read-only/repeatable-read, security ledger AUDITED,
  selected/active target-1 profiles zero and stock owners zero. Full role audit/history/
  bridge checks were not certified past the failed schema gate. Source approval/
  schema-plan guards pass 14 tests. Image preflight container was removed;
  zero provider/live-write/profile-activation/canary-start actions occurred.
  Preserve this report and the manifest approval. Do not silently skip Options-
  named migrations, apply unrelated DDL or repin source/image under the old
  approval. Next owner decision is an explicit, tested equity-only compatibility
  contract for live024 versus schema/source/image migration-alignment review.
  No automatic reschedule is allowed if the 13:25Z arm deadline is missed.
- Owner chose review-only equity/live024 assessment. The resulting
  [compatibility plan](ADVANCED_EQUITY_V2_LIVE024_COMPATIBILITY_PLAN.md)
  preserves live024 and the strict gate. All 000-029 source SQL matches the
  qualified image; the workspace additionally includes030 and newer Options
  registry entries. Pending025 changes shared product/promotion/profile/receipt
  semantics;026/027 extend shared detector/reference structures;028/029 are
  Options-state/checkpoint additions. Inspected stock-only owners have no direct
  new Options-flow dependency, but this is not execution-based compatibility proof.
  The stock operator's whole-workspace schema fingerprint and all-registry
  hardening are concrete blockers to honest024 binding. Proposed proof must use
  exact000-024/schema/ACL/operator/runtime hashes, actual native stock metadata
  and V3 replay/recreated-worker tests, and negative unsupported-scope cases.
  No compatibility-code change, qualification container, provider call, live write
  or DDL was performed during this review; the live canary remains blocked.
- Owner cross-workstream rule (2026-10-19): Equity and Options are operationally
  separable, not independently concurrent for qualification. Do not overlap live
  windows or run qualification load in parallel; shared connection/account,
  host and PostgreSQL effects would contaminate Options P7 measurements.
  Reserve a serial slot and confirm the other run's completion/cleanup before
  launch; unknown peer state blocks it. Qualify Equity live024 before claiming
  it is armed. After every Equity source change rerun the current Options v3
  provider-free preflight; any changed bound file requires regenerated/reapproved
  Options artifacts. Preserve frozen manifests and prior evidence. Executor
  fixture tests do not prove a cross-lane lock or supply current live authority.
  This documentation-only coordination step ran no qualification or provider call.
- Read-only nine-name audit for Developer cohort
  `d1baee6f-4ab2-543c-8a86-ce10f8952e69` found every ticker at
  `0/390` raw 1m and `0/78` raw 5m for 2026-10-16. Watermarks, screening and
  served release stopped on 2026-10-15; eight members lacked an overlapping
  October 16 reference revision and all current identities were expired.
- Owner authorized append-only late-observed identity continuity, exact full
  1m+5m recovery and projection-only validation. The reference refresh made 31
  paced Massive/Finnhub requests, received 787,372 bytes in 25.49 seconds and
  produced READY generation 4 for all nine names.
- The native repair contract now supports an immutable interval set. The exact
  full-session plan used 18 Massive requests (nine 1m plus nine 5m), a
  2,097,152-byte per-response limit and 37,748,754-byte cumulative ceiling.
  Actual sealed response bytes were 444,703. Provider-free replay reused all 18
  sealed responses and made zero external requests.
- Every ticker now has `390/390` raw 1m and `78/78` raw 5m through
  2026-10-16 20:00Z. Derived 15m/30m/1h/1d and monthly outcomes completed.
  The explicit late-continuity session-close audit is COMPLETE at 702/702,
  missing ranges zero, provider requests zero.
- Projection-only recovery first exposed missing seed continuity and then the
  exact 283-session adjusted-history gate (`observed=282`). Retained V1 had the
  older revision bodies but not the missing 2026-10-16 adjusted session. Owner
  separately authorized one grouped adjusted-daily request: nine facts were
  materialized from a 1,371,038-byte sealed response. Provider-free replay made
  zero external calls.
- Weekly derivation then completed nine revisions provider-free. The next
  immutable projection-only retry completed INPUT_READY, screening/ticker/
  Pattern Watch evidence and PORTAL_READY in 21.96 seconds with zero provider
  calls. Screening current is COMPLETE for 2026-10-16 with nine of nine members.
  That projection attempt did not enable `PORTAL_PUBLISH`; publication remained
  separately owner-gated.
- Owner then authorized Developer-only publication and replay from source window
  `cdb8a9b5-8d0b-5b97-b2a4-062f9289729c`, INPUT_READY receipt
  `90918af2-187c-4c93-98eb-05c5e684114e` and PORTAL_READY receipt
  `43dfb805-6a68-4dbf-993e-45f7da99b178`. The first attempt failed atomically
  before publication because Dashboard cold rehydration lacked late-continuity
  propagation; read-only evidence proved no release, snapshot, pointer or
  provider-ledger change.
- The corrected publication carries and recomputes the durable late-identity
  continuity digest/known-by proof. Recovery and replay created the same current
  Developer release `a5e403b7-6853-5ff0-8ff3-54d20568d818` at
  2026-10-16 20:00Z. Replay held at 225 catalog / 211 hot / 14 kept snapshots
  and 57 releases, with identical receipt IDs, zero provider requests/bytes and
  no platform pointer (`rt.portal_cohort_current` remains empty).
- After an owner-authorized API-only image reload, the frontend proxy returned
  Screening 200 with 9/9 October 16 rows, retained workspace 200 with 9/9
  members, and retained SPY publication 200. The exact five-session 1m audit
  then found one common gap: all nine tickers were 375/390 on 2026-10-15,
  missing only 19:45-20:00Z (135 bars total).
- Owner authorized the bounded 1m-only repair. It used nine Massive Developer
  requests and 367,075 bytes; the identical replay reused nine sealed responses
  with zero external requests/bytes. The next read-only audit found all 45
  ticker-session memberships COMPLETE at 390/390 with no missing ranges.
- Provider-free projection sealed INPUT_READY
  `22e578bc-e193-4d6f-b4ee-e8eaf24d541e` and PORTAL_READY
  `a3756a7f-903b-49da-a95e-82e25246d204`. Authorized publication/replay produced
  identical current release `6b398d42-dbc0-5697-a3ea-d5bd25af86cd`, stable at
  240 catalog / 223 hot / 17 kept snapshots and 59 releases, with no provider
  calls or platform pointer.
- Frontend-proxied retained SPY charts now return 200 for all eight intervals:
  1m (1,950 points), 5m (390), 15m (130), 30m (65), 1h (35), 1d (252),
  1wk (261) and 1mo (60). Current workspace remains 503 with
  `Ticker action evidence changed; republish required`; no freshness or
  reference gate was weakened.
- Total authorized external reads: 59 requests and 2,970,188 bytes. This
  Developer catch-up is recovery evidence only; it does not qualify Advanced
  target-1/target-9 ingress rates, socket faults, loaded drain timing or
  headroom.

## P9 - Market-Hours Shadow Soak

Status: [ ]

### Tasks

- [ ] **P9.1** Obtain exact provider, account, session, universe, byte, attempt,
  repair and duration authorization.
- [ ] **P9.2** Run at least five stable target-9 sessions, including one
  controlled disconnect/reconnect and one container restart.
- [ ] **P9.3** Include one half-day session or equivalent calendar rehearsal.
- [ ] **P9.4** Audit stream/journal/checkpoint/bar/window/publication identities
  plus INPUT_READY/PORTAL_READY/screening/projection identities and cumulative
  provider accounting after every session.
- [ ] **P9.5** Confirm API/frontend remain stored-only and independently
  available during ingress recovery.
- [ ] **P9.6** Retain failures and compare recovery latency against target.
- [ ] **P9.7** Include one authorized stopped-runtime startup catch-up for a
  partially missing prior session, then observe automatic transition to the next
  live session.
- [ ] **P9.8** Include one reconnect inside a five-minute window and one
  scheduler restart with older failed windows. Confirm exact multi-generation
  receipt reuse, latest-slot priority and bounded non-starving retry.
- [ ] **P9.9** Confirm projection-only soak creates no new portal release or
  cohort/platform pointer movement; API/frontend continue serving the previous
  verified release.
- [ ] **P9.10** Run the soak side by side with the authorized Developer lane.
  Compare tier-qualified canonical bars, screening/projection identities and
  latency without requiring byte-identical provider observations. Prove one
  lane's reconnect, blocked input or restart does not change the other's plan,
  receipts, jobs, release or serving health.

### Exit Gate

- No unaccounted connection/request/byte use.
- No duplicate logical bars, skipped durable offsets or silently closed gaps.
- All eligible windows complete; blocked windows have exact reasons.
- Every projection-only window has exact INPUT_READY and PORTAL_READY evidence;
  no alert/publication readiness is inferred from this soak.
- Developer remains independently operable and serving throughout the Advanced
  shadow soak; Advanced remains independently stoppable and restartable.

## P10 - Deployment, Rollback And Handoff

Status: [ ]

### Tasks

- [ ] **P10.1** Verify backup and restore before enabling the profile.
- [ ] **P10.2** Deploy in shadow mode with no platform cohort pointer change.
- [ ] **P10.3** Verify container health, controller daily state, stream
  heartbeat, journal volume, database watermarks and current release.
- [ ] **P10.4** Exercise rollback: stop Advanced profile, preserve journals and
  DB evidence, leave canonical services unaffected.
- [ ] **P10.5** Document alerts and operator actions for authorization
  exhaustion, stale owner, unresolved gap, journal pressure, scheduler terminal
  failure and reference/profile expiry.
- [ ] **P10.6** Record deployment evidence and update the compact Advanced
  checkpoint.
- [ ] **P10.7** Seek separate authorization for any platform pointer promotion.

### Deployment Done When

- Docker starts before the authorized session and runs through close without
  manual commands.
- Eligible transient failures recover within standing authorization.
- Exhausted/terminal failures stop safely and alert.
- The next trading session is prepared automatically.
- Zero/one/two missing completed sessions are audited and, when authorized,
  repaired before idle wait; unresolved old gaps do not silently block the next
  authorized live ingress.
- A Docker host/process restart does not reset budgets or lose durable journal
  work.
- Rollback is verified and documented.

## P11 - Cohort-Local Publication And Advanced Alert Qualification

Status: [~] DETECTOR-SHADOW CONTRACT IN PROGRESS; PUBLICATION/OUTBOX DISABLED.

P10 may deploy the qualified projection-only shadow. P11 is the earliest phase
that may change that mode. It must not be folded silently into P7/P8 or treated
as platform cutover authority.

Owner decision, 2026-10-20: implement event-driven detector shadow evidence only.
Persist deterministic detector version/event/state/checkpoint evidence atomically,
but create no alert projection or outbox row, dispatch nothing, and move no portal
or platform pointer. The provider-free `advanced-equity-detector-shadow-v1`
contract is implemented and covered by the combined source gate; it is not yet a
scheduled live runtime or live qualification. Provider-free single-shard consumer
wiring now claims a fenced lease, reads finalized stream-derived 1m/5m bar
revisions after its durable checkpoint and atomically persists the shadow batch.
The disposable PostgreSQL proof passed with one event, two retained inputs, state
and checkpoint, zero alert/outbox/pointer rows, restart-safe offset 7 and complete
cleanup. Owner selected the existing canonical S1/S2 stock-bar detectors for the
initial evaluator. They are now separate Advanced shadow identities over current
finalized derived 5m bars; every 5m source resolves to exactly one underlying
journal generation/session and maximum offset, requires the matching `INPUT_READY`
receipt, refuses incomplete cutoff sequences, and each decision retains 15 raw
daily plus 2/3 intraday bar identities. No process lifecycle or live run is armed.

### Tasks

- [x] **P11.1** Owner selected detector shadow only. Keep cohort-local
  `PORTAL_PUBLISH`, outbox creation/dispatch and the platform cohort pointer disabled.
- [~] **P11.2** Implement the Advanced event-driven detector shadow using stable
  detector-event deduplication, detector state and checkpoint evidence in one
  transaction. Provider-free contract, single-shard consumer and S1/S2 evaluator
  bindings complete; process scheduling and live qualification remain. Do not
  substitute the legacy Stock Alerts snapshot runner for this contract.
- [ ] **P11.3** Enable the V3 alert lane and, if authorized, PORTAL_PUBLISH only
  for the exact shadow cohort. Preserve INPUT_READY as the shared causal release
  identity and PORTAL_READY as projection proof.
- [ ] **P11.4** Prove alert and portal lanes run concurrently after
  materialization, independently retry, and merge cohort-local releases without
  dropping screening, ticker, Pattern Watch, Dashboard or alert snapshots.
- [ ] **P11.5** Run hardened restart tests before/after alert event commit,
  PORTAL_READY and atomic publication. Prove no duplicate events/outbox rows,
  no mixed input identity and no pointer regression.
- [ ] **P11.6** Repeat target-9 capacity/fault qualification and a bounded
  market-hours shadow soak with the newly enabled lanes before any promotion
  request.
- [ ] **P11.7** Keep Developer and Advanced publication identities independent
  during cohort-local Advanced publication. Prove Developer can continue
  serving and can be restored without replaying or deleting Advanced evidence.

### Exit Gate

- Exact input -> materialization -> screening/projection -> alert/publication
  lineage is reproducible before and after restart.
- Atomic cohort-local releases contain all independently produced entries and
  never move the platform pointer.
- Alert deduplication, outbox delivery evidence, lane deadlines and target-9
  headroom pass under the same fault matrix used in P8.
- Platform pointer promotion remains a separate, explicit authorization after
  P11; it is not implied by this gate.

## Cross-Phase Verification Matrix

| Scenario | Required evidence |
|---|---|
| Clean daily start | One child plan, exact cohort/manifest, one owner |
| Duplicate controller start | One winner; loser exits/stands by without provider I/O |
| DNS/TCP outage | Bounded attempts/backoff and cumulative counter increments |
| Socket outage after LIVE | New fenced session, retained exact gap, no overlap |
| Container SIGKILL | Stale owner reclaimed only after threshold; old writes fenced |
| Bridge crash | Journal replay reaches identical checkpoints/bars |
| DB outage | Journal continues only within disk/buffer budget; no false durability |
| DB outage after socket admission | SQLite continues without synchronous registry/heartbeat writes until the local safety bound |
| Buffer/journal pressure | Admission stops before data loss; gap/alert retained |
| Reconnect while prior journal drains | New fenced socket generation starts; prior journal remains publication-blocking until caught up |
| Reconnect inside one five-minute window | INPUT_READY accepts exact bars/checkpoints from all contributing fenced generations plus repair |
| Exact receipt reuse after reconnect | INPUT_READY binds the Advanced gate digest; changed generation/bar/checkpoint/gap fails closed |
| Child exits before writing a report | Controller classifies durable state/exit and remains alive |
| Scheduler crash | Durable retry resumes without report collision |
| Scheduler restart with backlog | Latest eligible cutoff runs first; older eligible retries remain non-starving |
| Projection-only V3 end to end | INPUT_READY and PORTAL_READY persist exact lineage; no release or pointer moves |
| Process secret isolation | Each service receives only its owned DSN/key and cannot reload a shared `.env` |
| V3 alert receipt compatibility | Alert/strategy lanes consume INPUT_READY but remain disabled until P11 |
| Concurrent alert and portal lanes | After P11 authorization, independent retries merge one cohort-local release without lost entries |
| Developer-only mode | Existing Developer acquisition/windows/publication run without Advanced processes or configuration |
| Advanced-only rehearsal mode | Advanced can be started/stopped for bounded non-serving rehearsal without Developer or platform-pointer authority |
| Developer plus Advanced shadow | Distinct cohorts/plans/jobs/receipts run together; Developer alone remains serving/publishing |
| Cross-tier fault isolation | Killing/blocking/restarting either tier leaves the other tier's processes, windows, releases and health unchanged |
| Combined capacity | Developer baseline latency remains within its qualified envelope while Advanced target-9 retains measured headroom |
| Unauthorized gap repair | Zero provider requests; affected windows blocked |
| Authorized gap repair | Reserved/sealed exact response; provider-free replay |
| Cold start, eight of nine members missing yesterday | Repair only missing members/ranges, exact coverage pass, cohort-local publication, then idle wait |
| Cold start, two sessions missing | Exact bounded plan, cumulative accounting, catch-up or explicit blocked state |
| Backfill still blocked at next open | Live ingress starts within its own authorization; old dependent publications remain blocked |
| Session close | Drain/seal/status then controller idle |
| Next session | New child plan/cohort resolution without manual CLI |

## Evidence Index

Add one line per completed phase or decisive failure:

| Date | Task/phase | Evidence | Verdict |
|---|---|---|---|
| 2026-10-17 | Initial gap inventory | Current source/tests/docs named above | PLAN ONLY |
| 2026-10-17 | P0-P6 provisional implementation | Phase-local offline and disposable PostgreSQL evidence above | IMPLEMENTED; SCHEMA NOT FINAL |
| 2026-10-17 | SC-01 schema review | Canonical V2 table overlap and disposition in `REALTIME_SCHEMA_V2_DESIGN.md` section 6.0 | CONSOLIDATE |
| 2026-10-17 | SC-01 implementation | One migration; five tables plus one view; 111 offline / 15 DB; no leftovers | PASS; SCHEMA READY |
| 2026-10-17 | LP-01 live-path review | Controller/ingress/bridge/input-gate/config/publication trace | BLOCK P7; FIX CRITICAL PATH |
| 2026-10-17 | Post-LP canonical wiring review | Docs plus ingress/bridge/gate/materialize/projection/screener/alert/publication/scheduler trace; 107 focused tests passed | EW-01 REQUIRED; P7 BLOCKED |
| 2026-10-17 | Retention qualification | Execute gate, explicit ordinary/archive scope, bounded atomic status, restart-safe counters; 225 offline plus one hardened disposable DB test; no leftovers | PASS; NOT LIVE-OPERATED |
| 2026-10-17 | Retention production source wiring | Plan-default service, explicit app/admin secrets and scopes, role/phase verification, advisory leadership, XNYS scheduling and file-only health; 233 offline tests | CODE PASS; NOT DEPLOYED |
| 2026-10-17 | Retention owner decision | Qualified archive/retention base accepted; preflight, secrets/volumes/Compose, live canary/continuous operation and failed operational-evidence policy deferred | ACCEPT BASE; DEFER LIVE ENABLEMENT |
| 2026-10-17 | Retention live-validation source preparation | Read-only exact candidate inventory; disabled/fail-closed Compose profile; explicit admin-secret opt-in and durable volume/health contracts; 255 offline tests | CODE PASS; LIVE CHECK NOT RUN |
| 2026-10-17 | Retention live read-only preflight | Exact authorized check stopped during secret preparation: `REALTIME_V2_ADMIN_DSN` absent; temporary directory absent; no DB connection or fallback | BLOCKED; PROVISION ADMIN DSN |
| 2026-10-17 | Retention live read-only retry | Owner-approved admin DSN persisted; roles/AUDITED/leadership/health passed; 3 hot portal would-drops, 0 archive-first candidates, 3 catalog prune classes planned; no writes; temporary secrets removed | PASS; IMPACT COUNTS NEXT |
| 2026-10-17 | Retention exact-impact source | Candidate partition rows/bytes, exact catalog/direct-cascade counts and latest-session archive source/ledger/outcome/open-alert counts; 256 offline tests | CODE PASS; LIVE COUNTS NOT RUN |
| 2026-10-17 | Retention exact live impact | Three empty hot partitions (73,728 physical bytes); 0 catalog rows/cascades; 0 archive-first candidates; all five 2026-10-16 archive kinds 0 rows/archives; no writes; secrets removed | PASS; CANARY/DOCKER DECISION NEXT |
| 2026-10-17 | Retention Docker configuration | Disabled profile rendered with exact scopes, app/admin-only secrets, dedicated volume, fail-closed authorization and file health; no container/volume created; temporary secrets removed | PASS; NOT DEPLOYED |
| 2026-10-17 | Retention would-create source | SQL-matching daily/weekly/monthly expected ranges plus `to_regclass` absence inventory; 258 offline tests | CODE PASS; LIVE CREATE PLAN NOT RUN |
| 2026-10-17 | Retention live would-create inventory | 21 missing future partitions: nine monthly `p20261101` plus four daily relations x `p20261030`/`31`/`1101`; no weekly missing; no writes; secrets removed | PASS; CANARY CREATE SCOPE DECISION |
| 2026-10-17 | Retention partition scoping | Owner chose explicit required partition relations; per-relation SECURITY DEFINER creation plus global health; production lists all 16; 259 offline tests | CODE PASS; NARROW LIVE PLAN NEXT |
| 2026-10-17 | Retention narrow canary manifest | Exact scope: 3 future hot creates, 3 empty hot drops, 0 catalog rows, one zero-row cluster archive/ledger, 0 archive-first drops; roles/health pass; no writes; secrets removed | PASS; MUTATION AUTHORIZATION NEXT |
| 2026-10-17 | Retention narrow live canary | READY: 3 scoped future creates, 3 empty hot drops, 0 catalog deletes, one verified zero-row cluster archive/ledger, 0 archive-first drops/alarms; post-check clean; status/files retained; secrets removed | PASS; NOT DEPLOYED/CONTINUOUS |
| 2026-10-17 | Retention bounded Docker plan | Rebuild backend only; command-overridden narrow `compose run --rm --no-deps`; dedicated volume retained; expected DB-idempotent archive/status evidence; continuous command prohibited | PLAN ONLY; DOCKER AUTH REQUIRED |
| 2026-10-17 | Retention bounded Docker one-shot | Container path guard fixed for flat `/app` image and focused 22-test suite passed; corrected narrow run was READY with zero partition/drop/catalog effects, but Docker default build ID differed from the qualified canary and created a second immutable zero-row archive identity; volume evidence retained, container removed, secrets removed, siblings healthy | STOPPED; ARCHIVE IDENTITY DECISION REQUIRED |
| 2026-10-17 | Retention archive identity resolution | Owner froze bounded host/Docker qualification at `retention-narrow-canary-v1`; Compose archive build ID now has no silent default; explicit render/redaction and narrow host read-only manifest passed with zero pending effects and two understood historical identities; secrets removed; continuous production identity remains separate | PASS; QUALIFICATION ID FROZEN |
| 2026-10-17 | Corrected retention Docker qualification | Explicit canonical identity; clean immediate precheck; READY with zero partition/drop/catalog effects and unchanged two-row ledger; volume manifest remained stale `retention-archive-v1`; container removed, volume/evidence retained, secrets removed, siblings healthy | STOPPED; ARCHIVE-ROOT RECONCILIATION REQUIRED |
| 2026-10-17 | Archive-root identity guard | Verification requires the requested build/policy identity; a conflicting immutable manifest is preserved but alarms and fails the archive outcome; normal idempotency retained; 138 focused tests | CODE PASS; DOCKER REQUALIFICATION NOT RUN |
| 2026-10-17 | Archive-root identity guard Docker requalification | Rebuilt-image narrow one-shot exited 1 with state ALARM and exact identity-mismatch alarm; zero partition/drop/catalog effects; two-row ledger unchanged; archive/manifest hashes unchanged; container removed, alarm-status volume retained, secrets removed, siblings healthy | PASS; FAIL-CLOSED GUARD QUALIFIED |
| 2026-10-17 | Clean retention qualification volume | Owner chose separate clean volume; explicit volume-name override covered by 10 tests; canonical narrow one-shot created only `alpha-research-realtime-v2-retention-qualified-v1`, wrote canonical zero-row archive/manifest plus READY status, made zero database effects, retained two ledger identities and preserved the original ALARM volume byte-for-byte; container removed, secrets removed, siblings healthy | PASS; ARCHIVE-ROOT RECONCILED FOR QUALIFICATION |
| 2026-10-17 | Continuous retention no-start plan | Proposed separate `retention-production-v1` identity/production volume; found unsafe fallback to preserved ALARM volume; defined fail-closed selection, owner decisions, all-policy read-only/one-shot and observation gates; no secrets, volume, container or database action | PLAN ONLY; PRODUCTION DECISIONS REQUIRED |
| 2026-10-17 | Continuous identity/volume plus all-policy check | Owner approved `retention-production-v1` and absent external production volume; omitted selection now resolves to absent external sentinel; exact all-policy render/redaction passed; read-only manifest found 18 creates, zero drops/catalog/archive candidates/rows and two existing cluster identities; report lacks requested-build ledger counts; secrets removed, production volume absent | PASS; IDENTITY-SPECIFIC LEDGER IMPACT NEXT |
| 2026-10-17 | Continuous identity-specific ledger check | Requested-build counts added with 138 focused tests; all-policy rerun found zero `retention-production-v1` rows for all five kinds, four definite inserts and one cluster insert dependent on SHA collision because ledger UUID excludes build ID; secrets removed, production volume absent, no writes | BLOCKED; LEDGER COLLISION DECISION NEXT |
| 2026-10-17 | Session archive build-ID ledger key | Additive migration and repository UUID include build ID while preserving same-build idempotency and immutable legacy rows; 159 focused archive/retention/schema tests; no database migration or operation | CODE PASS; DISPOSABLE DB NEXT |
| 2026-10-17 | Session archive build-ID hardened disposable | Full chain through migration 024 plus hardened roles; retention cycle, identical-SHA different-build coexistence and same-build idempotency passed 2 tests; cleanup databases none/roles none | PASS; LIVE MIGRATION AUTHORIZATION NEXT |
| 2026-10-17 | Session archive build-ID live migration | Exact preflight allowed only 024; live resume applied only 024 with 13 structural checks; post-check pending none, new constraint present, two rows preserved, AUDITED/security/services healthy; all-policy read-only rerun found 18 creates, zero destructive effects and exact five new production ledger rows; production volume absent, secrets removed | PASS; ALL-POLICY ONE-SHOT AUTHORIZATION NEXT |
| 2026-10-17 | Production retention all-policy one-shot | Immediate manifest unchanged; approved external production volume created; one command-overridden all-policy run READY with exactly 18 creates, five zero-row production archives/ledger rows, zero drops/catalog/failures/alarms; post-check zero remaining effects, seven total ledger rows; container removed, all volumes retained, secrets removed, siblings healthy | PASS; CONTINUOUS STILL UNAUTHORIZED |
| 2026-10-17 | Production retention bounded continuous canary | One supervised immediate continuous cycle healthy/READY with zero creates/drops/failures/alarms, five idempotent outcomes and zero restarts; container stopped/removed after first cycle; post-check zero-impact, production volume READY, secrets removed, siblings healthy | PASS; STANDING OPERATION STILL UNAUTHORIZED |
| 2026-10-17 | Production retention standing enablement | Committed image, durable owner-scoped secrets, approved 3600/7200/2 policy, host resources, manual Docker/file acknowledgement and indefinite evidence retention; final precheck zero-impact; first standing cycle READY with zero effects/restarts; ledger seven/one production identity per kind; leadership lock held, service and siblings healthy | PASS; RUNNING CONTINUOUSLY |
| 2026-10-17 | Retained portal warning UX | Reference drift routed to Reference/Fundamentals; action/market drift stays in chart/analysis; calendar/global stale banners remain scoped; retained content stays rendered; 24 frontend tests and production build | PASS |
| 2026-10-17 | Retained portal frontend redeploy | Explicit realtime/cohort/API Docker build args; frontend-only recreate; proxy/API health 200; live SPY retained chart/quote/setup/context/reference/calendar 200; scoped warnings verified in Overview/Financials; 18 container + 24 frontend tests | PASS; CURRENT-ONLY 503 UNCHANGED |
| 2026-10-17 | Playwright MCP / route smoke | User-local official Chrome-for-Testing restored MCP; ticker/screener/persistence load; Dashboard and Stock Alerts show known unavailable states; legacy V1 Canary remains 0/8 with eight ticker 404s | MCP PASS; LEGACY CANARY GAP |
| 2026-10-19 UTC | P8 provider-free recovery contract runner | Fixed 14-scenario source runner; actual guarded rehearsal 30 passed, new fault/report checks 20 passed, default gateway/container regression 28 passed; persistent JSON explicitly denies live/production qualification; no live operation | PASS_OFFLINE; P8 LIVE GATES OPEN |

Do not paste raw logs into this document. Link bounded reports and record the
measured verdict.

## Open Decisions

Resolve before the owning phase:

1. Standing authorization validity duration and approver source.
2. Target-9 daily decoded-byte/session/attempt ceilings.
3. Whether standing authorization includes automatic bounded REST gap repair.
4. Heartbeat/lease stale threshold.
5. Stable-LIVE interval that resets transient reconnect streak.
6. Journal volume path, capacity and alarm thresholds in Docker.
7. Initial Docker host resource limits.
8. Alert destination and acknowledgement lifecycle.
9. Evidence retention for failed stream sessions, journals, plans and windows
   (deferred with live retention enablement by owner, 2026-10-17).
10. After projection-only P9/P10 evidence, decide whether P11 remains
    projection-only or enables cohort-local `PORTAL_PUBLISH` plus Advanced alert
    shadowing without platform pointer movement.

Defaults must not be invented merely to make fault tests pass.

Resolved owner decisions: cold-start catch-up is capped at two completed
sessions; authorized live ingress has priority over unfinished historical
backfill at the next warmup boundary; SC-01 uses the five-table-plus-view slim
overlay recorded in the canonical schema design; P7/P9/P10 use projection-only
shadow with no portal release or platform pointer movement; the qualified
retention/archive base is accepted and source-only live-validation preparation
resumed, while live secret access, database preflight, Docker validation,
deployment and operation remain separately unauthorized.

## New-Session Implementation Prompt

Copy the prompt below into a new chat:

```text
Advanced Equity V2 autonomous rollout - implement EW-01 canonical end-to-end wiring.

Read the latest request, then only:
- docs/agent-context/advanced-realtime.md
- Rules, Invariants, SC-01, LP-01 and EW-01 sections of
  docs/ADVANCED_EQUITY_V2_AUTONOMOUS_DOCKER_ROLLOUT.md
- "Canonical Recovery Compatibility" in docs/REALTIME_V2_PIPELINE.md
- "Advanced WebSocket Applicability" in
  docs/Canonical-Terminal-State-Recommendations.md
- Windows execution instructions
Do not load prior chats, parked reports or the full large design documents.

Mode: implementation
Checkpoint: advanced-realtime
Objective: complete EW-01 before P7. Preserve the SC-01 five-table-plus-view
schema, LP-01 source separation, owner-selected projection-only mode and the
canonical V2 orchestration/provider/payload/release/alert model.

Required fixes:
1. Bind the exact Advanced input-gate digest into canonical INPUT_READY so
   provider-free reuse proves every contributing bar/session/generation/owner
   epoch/checkpoint/repair/gap verdict.
2. Add a hardened full `stages_v3` reconnect rehearsal through journal replay,
   canonical bars/checkpoints, input gate, five-minute/higher derivation,
   INPUT_READY, screening/ticker/Pattern Watch projection and PORTAL_READY.
3. Restart after INPUT_READY and PORTAL_READY; prove identical receipt lineage,
   no duplicate work and no portal/platform pointer movement.
4. Make `stages_v3` alert and strategy runners consume INPUT_READY rather than
   legacy EQUITY_CYCLE, but keep those stages disabled in projection-only
   automation until P11 authorization.
5. Restore latest-slot-first scheduling with bounded non-starving retries for
   older failed/blocked cutoffs.
6. Add focused V3 lane tests proving alert and portal lanes are independent
   after materialization without enabling publication.
7. Define Developer-only, Advanced-only bounded rehearsal and
   Developer-active plus Advanced projection-only shadow contracts. Keep
   Developer as the sole serving publisher.
8. Add a hardened same-cutoff dual-lane rehearsal proving cohort/plan/window/
   job/barrier/receipt isolation, no cross-tier claims and fault independence.
9. Align the canonical recovery/applicability documents with the verified
   result and retain explicit limitations.

Anchors:
- backend/realtime/stage_runners_advanced_equity.py
- backend/realtime/stage_runners_equity.py
- backend/realtime/stage_runners_alerts.py
- backend/realtime/stage_graph.py
- backend/realtime/stage_orchestrator.py
- backend/realtime/advanced_scheduler.py
- backend/realtime/worker_topology.py
- backend/scripts/run_realtime_v2_equity_services.py
- backend/scripts/run_realtime_v2_advanced_equity_scheduler.py
- backend/scripts/run_realtime_v2_advanced_equity_window.py
- focused Advanced input-gate/materialization/projection/alert/scheduler tests
- focused tier-topology and Developer/Advanced coexistence tests
- hardened V3 disposable rehearsal tests

Allowed: source, focused tests, consolidated migration only if strictly required,
and docs. Use offline tests first, then changed hardened disposable DB tests with
STOCKS_V2_DB_TESTS=1, then the disposable-leftover check.

Do not start providers/services, deploy Docker, move any platform pointer,
operate retention, reintroduce removed SC-01 tables, enable publication/alerts,
or implement P7/P11.

Done when EW-01 has exact offline/DB evidence, the full reconnecting V3 window
and provider-free restart tests pass, latest-slot fairness and all three
Developer/Advanced operating modes are proven, no disposable leftovers remain,
and the tracker/checkpoint/canonical docs are updated. Stop before P7.

Focused verification:
- exact gate digest in INPUT_READY and receipt reuse;
- full `stages_v3` multi-generation projection-only rehearsal;
- restart after INPUT_READY and PORTAL_READY;
- V3 alert/strategy INPUT_READY compatibility;
- concurrent lane failure isolation;
- Developer-only, Advanced-only bounded rehearsal and side-by-side lane modes;
- same-cutoff dual-lane cohort/job/receipt and fault isolation;
- latest-slot-first plus non-starving retry;
- no portal release or pointer movement;
- changed hardened DB files;
- backend/scripts/check_realtime_v2_disposable_leftovers.py.
```
