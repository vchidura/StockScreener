# Advanced Realtime V3 - Current Runtime Checkpoint

Updated: 2026-10-31 UTC

## Objective And Mode

Replace the one-session artifact/qualification-driven V3 launcher with a simple
self-reconciling continuous worker for local and Docker development.

Canonical design and assessment:
[REALTIME_V3_PIPELINE.md](../REALTIME_V3_PIPELINE.md).

Mode: C0/C1 committed at `eafe5f7`; C2 implemented and validated, review pending.

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

## Exact Anchors

- `backend/scripts/run_realtime_v2_advanced_equity_scheduler.py`
- `backend/scripts/run_realtime_v2_advanced_controller.py`
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

Focused completion check: PASS. A provider-free desired revision derives the next
XNYS session, atomically selects one current revision, blocks baseline shrinkage,
replays one immutable manifest/cohort/slot-set snapshot idempotently, defers
market-hours edits without changing the active snapshot, and performs zero provider,
production or publication effects.
