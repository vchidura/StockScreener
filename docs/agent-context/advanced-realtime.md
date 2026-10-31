# Advanced Realtime V3 - Current Runtime Checkpoint

Updated: 2026-10-31 UTC

## Objective And Mode

Replace the one-session artifact/qualification-driven V3 launcher with a simple
self-reconciling continuous worker for local and Docker development.

Canonical design and assessment:
[REALTIME_V3_PIPELINE.md](../REALTIME_V3_PIPELINE.md).

Mode: design consolidation complete; implementation planning next.

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

- Select one desired-universe source and automatically snapshot it per session.
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

## Next Work Package

Start implementation package C0/C1 from the canonical consolidation plan:

1. capture the current provider-free regression baseline and socket/pointer ownership
   map;
2. define one session-state loading port;
3. route the existing file-driven bounded path through a compatibility adapter without
   changing behavior;
4. add database-repository contract tests before moving any runtime owner;
5. keep all current exact target-9/four-contract fixtures passing.

## Exact Anchors

- `backend/scripts/run_realtime_v2_advanced_equity_scheduler.py`
- `backend/scripts/run_realtime_v2_advanced_controller.py`
- `backend/realtime/advanced_ingress_controller.py`
- `backend/realtime/advanced_gap_repair.py`
- `backend/realtime/advanced_realtime_session_scheduler.py`
- `backend/realtime/advanced_options_acquisition_runtime.py`
- `backend/realtime/advanced_options_p8_runtime.py`
- `docker-compose.realtime-v2.yml`

Focused completion check for the next implementation session: existing bounded V3
tests pass through the new compatibility seam with identical typed session state and
no provider, schema, Docker or publication effects.
