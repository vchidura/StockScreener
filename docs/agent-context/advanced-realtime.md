# Advanced Realtime V3 - Current Runtime Checkpoint

Updated: 2026-10-31 UTC

## Objective And Mode

Replace the one-session artifact/qualification-driven V3 launcher with a simple
self-reconciling continuous worker for local and Docker development.

Canonical design and assessment:
[REALTIME_V3_PIPELINE.md](../REALTIME_V3_PIPELINE.md).

Mode: C0/C1 committed at `eafe5f7`; C2 committed at `3754337`; C3 implemented
and validated, review pending.

## Settled Runtime Decisions

- Derive the current/next XNYS session and due intervals automatically.
- Do not require software qualification, source/image approval, daily freeze files,
  packages, bundles or manually supplied dates for normal startup.
- Keep PostgreSQL durable state, journal offsets, watermarks and leases as restart
  authority.
- Add Equity, sector ETF and Options members only during market-close maintenance.
- Backfill and warm new members independently before the next market run.
- On a market-hours restart, start live capture first, catch recoverable bars up to
  the current finalized interval and continue latest-interval ingestion.
- Record unrecoverable raw T/Q gaps without blocking unrelated future work.
- Keep provider rate limits, bounded buffers, leadership and visible durability
  errors as ordinary runtime correctness.
- Keep qualification/replay tools as optional diagnostics only.
- Dispatch, broker and trading remain disabled and separate.

## Verified Reuse Points

- Advanced Equity scheduler already discovers durable due windows, continues siblings
  after failure and rolls to the next XNYS session.
- Advanced ingress already has live-first hybrid REST/stream repair primitives.
- Equity and Options journals, bridge/checkpoints, finalization and restart rehydration
  exist.
- Options heartbeat/reconnect/gap accounting and P8 resident roles exist.
- Canonical Stock Alert persistence, portal publication, stored readers, retention
  and archive components exist.

## Main Integration Gaps

- Wire durable desired-state reconciliation into the continuous lifecycle owner.
- Remove fixed initial date/cutoff/authorization arguments from normal resident startup.
- Generalize exact target-9 and exact-four QQQ/SPY assertions across bindings,
  scheduler counts, Stock Alerts and tests.
- Replace package/bundle/binding file mounts as runtime coordination with durable
  database state.
- Integrate mid-session catch-up through publication without pointer regression or
  stale alert delivery.
- Add Options subscription sharding and per-underlying readiness.
- Consolidate the existing continuous Equity path and one-session V3 path under one
  native/Docker lifecycle owner.
- Measure retention, journal rotation, disk growth and backup/restore without making
  them startup gates.

## C0/C1 Completion Evidence

- Provider-free pre-change baseline: 66 focused tests passed.
- `SessionRuntimeStatePort` and `LegacyArtifactSessionStateAdapter` now isolate
  package, bundle, acquisition, pre-open and downstream binding reads plus
  downstream append.
- The adapter delegates to the existing JSON loaders, bundle validator, typed parsers
  and downstream chain store; payload/hash/error, append sequence, replay and atomic
  publication semantics are unchanged.
- The bounded scheduler, acquisition/production CLIs and P8 service, health and
  resident paths use the compatibility seam. The scheduler no longer constructs or
  reads/writes the downstream file store directly.
- Consumer-injection regressions run the bounded scheduler and P8 service with
  in-memory session state while package, bundle, acquisition and downstream artifact
  paths are absent.
- Corrected bounded seam suites: 107 tests passed with 5 existing warnings. Adjacent
  continuous Equity,
  session-operations, production and deployment suites: 50 tests passed.
- Equity ingress remains the STOCKS socket owner; the split Options acquisition
  runtime remains the OPTIONS socket/journal owner. The Equity bridge owns its
  consumer checkpoints and the canonical publisher remains the sole release-pointer
  writer.
- Exact target-9 and exact-four QQQ/SPY validation remains active. No provider,
  schema, Docker, publication or database effects ran.
- C2 leaves the compatibility seam unchanged. Old-path retirement is not started.

## C2 Completion Evidence

- Migration `039_realtime_desired_state` adds append-only desired revisions and role
  members, one scoped mutable current-revision pointer, mutable per-member
  `WARMING`/`READY`/`FAILED` preparation, and an append-only link to canonical
  immutable session objects. It adds no coordinator, provider authority or
  publication pointer.
- The provider-free maintenance command records only identities already present in
  `ref.security_revisions`, `rt.instruments` and a stored manifest/slot-set
  lineage. Invalid or ambiguous identities fail explicitly.
- Current selection is deterministic by effective session, revision timestamp and
  revision ID. Same-session corrections keep prior evidence and replay cannot move
  the pointer backwards.
- Before-open changes target the upcoming XNYS session. Market-hours and after-close
  changes target the next XNYS session; the source/active session is never updated.
- Reconciliation waits while any enabled member is `WARMING`, blocks when an enabled
  source Equity or Options-underlying member is `FAILED`, and may omit only failed
  new or explicitly disabled members. It idempotently creates the existing immutable
  subscription manifest/members, SHADOW cohort, tier binding and PRE_SESSION slot set.
- C2 preparation uses one set-based identity read, two batched desired-state writes
  and one set-based instrument read for up to 400 names. Maintenance timestamps
  default to aware current UTC and retain explicit test/operator overrides.
- The default remains exact target-9 plus exact-four QQQ/SPY T/Q. C2 does not add
  runtime count generalization, sharding, provider work, live sockets, publication,
  Docker changes or old-path retirement.
- Focused offline/schema tests: 39 passed. Final adjacent offline/seam selection:
  109 passed, 1 pre-existing malformed fake negative test deselected and 5 warnings.
  C2 plus adjacent disposable PostgreSQL control-plane and slot-set tests: 21 passed
  with 4 existing warnings.

## C3 Completion Evidence

- The existing Advanced controller is the sole lifecycle leader. It fences one owner
  in `rt.worker_leases`; the existing STOCKS stream owner epoch, journal generation
  and cumulative plan-usage ledgers remain authoritative.
- The provider-free injected-clock planner derives XNYS before-open, market-hours,
  drain and idle phases; current/next sessions; dynamic half-day/DST windows; first
  due cutoff; and catch-up-needed state.
- The controller selects the C2 current/effective immutable snapshot and carries
  unchanged intent into a new immutable revision/snapshot only at a derived session
  boundary. Its internal rollover may explicitly target the planner's active XNYS
  session after offline downtime; operator changes still use C2 market-hours
  deferral. A current revision with no snapshot is reconciled automatically after
  readiness changes; readiness and materialization share one revision advisory lock,
  and concurrent reconcilers reuse first-writer `materialized_at` evidence.
- Target-session desired intent/current selection commits before capability-dependent
  snapshot reconciliation. A REFRESH_REQUIRED rollback therefore retains one durable
  target revision for the refresh resident; success later creates one snapshot/plan.
- Continuous mode derives the session cohort, authorization, date and cutoff.
  Session-scoped finite stream/connection/byte/repair budgets create a deterministic
  internal authorization ladder and daily plan without resetting durable counters.
  A restart pins the existing same-session plan, budgets and usage even if runtime
  configuration changed; the change applies only when preparing the next session.
- Acquisition stops at exchange close. Provider drain, exact final-window deadline
  (`close + finality delay + completion budget`, including half days) and
  next-session preparation are separate: scheduler/finalizer retain today's scope
  through that deadline while the controller may prepare tomorrow.
- Continuous controller mode requires the independent external ingress resident;
  direct provider-child execution remains `legacy-diagnostic` only.
- An independent capability-refresh resident now owns
  `CAPABILITY_REFRESH_REQUIRED:REFERENCE` in native and Compose lineups. On startup,
  desired/profile change, approaching expiry and bounded cadence it checks the exact
  Equity AM scope with one AdvancedGateway subscription-only WebSocket session, then
  reuses the existing Advanced control-plane builder/persistence to append honest
  profile/check evidence. Historical REST success is not capability evidence.
- The probe persists exact accepted/rejected `AM.*` membership plus bounded
  connection/control/request/byte evidence. Partial or rejected membership is never
  PASS. A 400-member scope remains one subscription request/connection.
- Subscription acknowledgement accumulates across bounded frames. Generic success
  confirms only request/control success; PASS requires every requested member
  explicitly accepted with zero rejected or unconfirmed members.
- Probe runtime is bounded by a configurable event-loop timeout and cumulative
  control-byte ceiling, both smaller than the resident lease by validation. Silent
  auth, silent subscription acknowledgement and byte overflow close the socket,
  raise typed probe failures and return to persisted DEGRADED/backoff.
- TTL planning derives the earliest `probe_not_before` whose evidence can cover the
  target XNYS close. Earlier weekend/holiday checks expose `WAITING`/next check and
  make zero provider calls; unchanged fresh scope also makes zero calls.
- Temporary provider failure persists `DEGRADED`, heartbeat, last success and bounded
  next retry in existing readiness/lease state. It does not exit or stop controller,
  ingress, scheduler, bridge or finalizer; successful fake-source refresh unblocks the
  next lifecycle reconciliation without a manual command.
- Ingress and capability probe share one atomic database-visible
  `provider-connection:massive:STOCKS` owner fence. Live ingress blocks probes;
  release/expiry permits one probe, and concurrent claims cannot open two sockets.
- One bounded `window_runs` query uses the existing cohort/window-key index to load
  latest discovered and contiguous-complete progress. Controller/scheduler planning
  starts after that durable prefix; complete sessions have no due cutoff and midday
  catch-up does not rescan earlier completed windows.
- Initial lifecycle claim refuses every unexpired lease, including the same owner
  UUID. Renewal is the only same-process continuation; expired takeover advances the
  durable owner epoch.
- `legacy-diagnostic` retains explicit authorization/cohort/session/cutoff arguments.
  Native and Compose continuous launchers now share capability refresh, controller,
  ingress, bridge, finalizer and scheduler residents; ingress detects a surviving
  live stream before spawning another child.
- Desired state is read on startup, pointer change, session transition and bounded
  supervisor cadence only. The live ingress/message path has no lifecycle,
  calendar, desired-state or leadership query.
- Controller lease renewal runs at one-third of its lease instead of every supervisor
  loop. Capability membership/readiness reloads use retry/expiry or bounded status
  cadence, scheduler caches immutable session scope/windows, and runtime health scopes
  current processing without aggregating historical failures.
- Continuous health reports current acquisition, unfinished processing and future
  preparation separately; active stale streams are `STALE`, failures are `DEGRADED`,
  completed acquisition is `TERMINAL` and future-only preparation is `IDLE`. It also
  exposes capability-refresh owner epoch, heartbeat, last success, TTL waiting and
  next retry.
- Final provider-free offline compatibility selection: 282 passed, 7 skipped
  (including six opt-in PostgreSQL tests), 1 pre-existing malformed delayed-pong
  fake deselected and 5 existing warnings.
- Final owned disposable PostgreSQL selection: 30 passed with 5 existing warnings.
  Evidence includes refresh failure visibility, fake-source capability/check
  persistence, next-reconcile success, readiness/reconcile races, first-writer
  snapshot identity, Friday-to-Monday rollover, post-close plan reuse, budget
  pinning, scheduler scope, ingress-plan compatibility and lease fencing.
- Remaining C4+: remove fixed count ladders and split intended membership from
  per-member/lane readiness in C4; revalidate daily Options/reference evidence and
  account-wide connection capacity in C5/C6; isolate resident/API/frontend failures
  and long-lived recovery in C8. C3 intentionally retains the global WARMING gate.
  No migration or new coordination table was added.

## Exact Anchors

- `backend/realtime/advanced_equity_capability_refresh.py`
- `backend/realtime/advanced_equity_lifecycle.py`
- `backend/scripts/run_realtime_v2_advanced_capability_refresh.py`
- `backend/scripts/run_realtime_v2_advanced_equity_scheduler.py`
- `backend/scripts/run_realtime_v2_advanced_controller.py`
- `backend/scripts/run_realtime_v2_advanced_ingress_service.py`
- `backend/scripts/run_realtime_v2_advanced_services.py`
- `backend/scripts/run_realtime_v2_equity_interval_finalizer.py`
- `backend/realtime/advanced_ingress_controller.py`
- `backend/realtime/advanced_gap_repair.py`
- `backend/realtime/advanced_realtime_session_scheduler.py`
- `backend/realtime/advanced_options_acquisition_runtime.py`
- `backend/realtime/advanced_options_p8_runtime.py`
- `backend/realtime/advanced_realtime_desired_state.py`
- `backend/realtime/advanced_realtime_desired_state_repository.py`
- `backend/realtime/sql_v2/039_realtime_desired_state.sql`
- `backend/scripts/manage_advanced_realtime_desired_state.py`
- `docker-compose.realtime-v2.yml`

Focused completion check: PASS. Provider-free continuous Equity startup needs no
manual session/cohort/cutoff identity, selects the effective C2 snapshot, refuses a
duplicate leader, preserves active stream ownership and durable usage, idles across
close/weekend, rolls a missed session during market hours with catch-up still
required, pins same-session budgets, preserves a full processing drain, serializes
snapshot creation and performs zero provider, production, Docker or publication
effects.
