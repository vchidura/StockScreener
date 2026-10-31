# Advanced Realtime V3 Production Validation Execution Plan

> **Historical optional validation playbook.** This document does not define normal
> V3 startup, software qualification, membership onboarding or daily operation.
> Current runtime decisions and implementation status are maintained in
> [REALTIME_V3_PIPELINE.md](REALTIME_V3_PIPELINE.md). Fixed target-9, exact-four,
> 78-window and UTC-clock requirements below describe the dated full-session
> validation scope only.

Status: `PLAN_ONLY`
Target: next prospective XNYS session after all implementation and qualification
gates pass
Scope: exact target-9 Equity, canonical V3 Stock Alerts, and exact-four
QQQ/SPY Options

This document is the single execution checklist. It is not provider,
production-write, migration, service, publication, pointer, read-pin, delivery,
broker, or trading authority.

## 1. Required Outcome

The preferred verdict is `PASS_FULL_SESSION_SINGLE_BUILD`:

1. one prospective pre-open acquisition identity;
2. one target-9 STOCKS/AM owner and one exact-four OPTIONS/T/Q owner;
3. unchanged acquisition image, journals, normalization, bridge, finalization,
   authorities, provider budgets, and contract membership through close;
4. 78 exact Equity E cutoffs from 13:35Z through 20:00Z;
5. one canonical nine-member Stock Alert result per E;
6. one Options O per E with `O - E <= 300 seconds`;
7. publication, exact read pin, and stored-reader verification for every
   complete cycle;
8. zero dispatch, delivery, broker, and trading effects;
9. sealed close drain and empty owned cleanup.

The acquisition plane and downstream serving plane have separate verdicts.
Acquisition may retain `PASS_ACQUISITION_FULL_SESSION` after a reviewed
downstream-only transition, but a transition forfeits the single-build verdict.

## 2. Current Starting Point

The 2026-10-28 primary day is terminal `NO_GO` with zero provider, production
service, publication, pointer, and read-pin effects.

The current decisive blocker is architectural:

- receipt-free pre-open acquisition is implemented and qualified;
- the serving P8 control/execution binding still embeds an E receipt;
- downstream residents cannot transition from `READY_WAITING_FOR_E` to exact
  per-E binding without a coordinated binding split;
- no later-session source/image identity may be reused until that split and all
  subsequent edits are freshly qualified.

Migration 038, target authorizations, production package/bundle, host schedule,
provider authorities, publication authorities, and read-pin authorities remain
absent for the next prospective session.

## 3. Execution Ownership

Use one **Session Coordinator** process and one output directory.

The coordinator owns this state machine:

```text
IMPLEMENTATION_REQUIRED
  -> PROVIDER_FREE_QUALIFIED
  -> OWNED_DISPOSABLE_QUALIFIED
  -> DOCKER_QUALIFIED
  -> PREOPEN_FREEZE_GO
  -> ACQUISITION_RUNNING
  -> E_WAITING
  -> CYCLE_RUNNING
  -> PUBLISHED_PINNED_READABLE
  -> CLOSE_DRAIN
  -> CLEANUP_COMPLETE
```

Any terminal failure enters `BLOCKED` or `NO_GO`; it never restarts from an
earlier state by inference. The coordinator atomically writes one compact
status document containing phase, clock, exact process/container IDs, usage,
last completed E/O cycle, decisive blocker, rollback state, and cleanup state.

Never start a second coordinator when the first state is unknown.

## 4. Phase 0 - Complete The Binding Split

### 4.1 Pre-open acquisition binding

Build one immutable acquisition binding containing only:

- session date and XNYS clocks;
- target-9 Equity membership and exact-four Options membership;
- provider/socket budgets;
- owner IDs and journal generations;
- immutable acquisition image/source identity;
- acquisition start, stop, and drain;
- no Equity receipt;
- no detector, projection, publication, pointer, or read-pin authority.

All acquisition residents must start from this binding and report
`READY_WAITING_FOR_E`.

### 4.2 Per-E downstream binding

After each real `INPUT_READY` receipt:

- create one immutable binding naming receipt ID, input SHA, E cutoff, H
  binding, exact Options checkpoints, and `O - E <= 300 seconds`;
- create detector/projection/window authority;
- create publication/read-pin authority only for that E/O cycle;
- attach the downstream binding without replacing acquisition containers,
  owners, journal generation, subscriptions, or budgets;
- support restart/rehydration from durable binding state.

### 4.3 Mandatory split tests

Provider-free tests must prove:

- acquisition starts without an E receipt;
- downstream roles remain `READY_WAITING_FOR_E`;
- E1 attaches without acquisition restart;
- E2 and E3 attach successively;
- same-owner restart rehydrates acquisition and latest downstream binding;
- receipt/package/hash divergence fails closed;
- no prewritten cycle/publication authority exists;
- rollback disables the exact pin before restoring the exact pointer;
- cleanup is empty.

Do not continue to source freeze until this phase is Tier-C qualified.

## 5. Phase 1 - Fresh Qualification

After the final source byte:

1. Run the focused provider-free qualification named by
   `backend/scripts/qualify_advanced_realtime_session_scheduler.py`.
2. Run owned-disposable database qualification, including:
   - binding persistence/replay;
   - three E/O cycles;
   - canonical Stock Alerts;
   - publication/read pin;
   - restart/rehydration;
   - rollback;
   - empty database/role/process/lock cleanup.
3. Use the reusable holistic validation components:
   - `audit_realtime_v2_target9_scanner_warmup.py` for the read-only daily/hourly
     warmup and exact 1h-to-5m lineage gate;
   - `replay_realtime_v2_retained_native_plan.py` for one exact stored-plan
     replay only after its complete retained ledger is verified; `--execute`
     is a production write and requires explicit authority;
   - `validate_advanced_realtime_v3_holistic.py` to aggregate immutable
     qualification, replay, history, scanner, Treasury and readiness reports.
4. Run `validate_advanced_realtime_v3_local_preflight.py` before any image
   build. Its immutable `PASS` report must prove:
   - the target-1 parent is scoped to the prior XNYS session;
   - the target-9 child begins on the target XNYS session;
   - the child session is exactly the next session after the parent;
   - the same-session promotion negative test and the valid promotion
     disposable-database test pass;
   - provider and production effects are zero.
5. Build one immutable backend image.
6. Run actual-role Docker qualification, bound to the exact local-preflight
   semantic and file SHAs, for:
   - Equity controller/ingress/bridge/scheduler/finalizer;
   - canonical Stock Alert acquisition and serving roles;
   - receipt-free exact-four Options acquisition;
   - Options market-state, flow, and window roles;
   - same-container acquisition restart;
   - downstream E attachment;
   - empty cleanup.
7. Record source semantic/file SHA, image ID, Docker semantic/file SHA, source
   file count, tests, and cleanup.
8. Run `git diff --check`.

Any source edit after this gate invalidates the image and all qualification
identities.

### 5.1 Mandatory local-first session harness

Before Docker qualification for any target session:

1. Derive every session clock from the XNYS calendar. Never copy dated parent,
   child, admission, start, stop or drain clocks from an earlier manifest.
2. Run the exact target-9 Equity AM and bounded Options provider captures on the
   host under a fresh grant:
   - one Equity WebSocket session/attempt;
   - one Options WebSocket session/attempt;
   - exact request, message and cumulative decoded-byte ceilings sized for the
     complete capture duration and frozen membership from measured traffic plus
     reviewed headroom;
   - independent bounded buffer bytes/events/maximum age sized from durable
     drain capacity, never reused as the cumulative session ceiling;
   - no reconnect or retry unless separately granted.
3. Persist immutable raw/normalized Equity and Options journals plus every REST
   response artifact. Provider capture never implies DB apply.
4. Replay the sealed inputs through one owned disposable database and host PIDs:
   Equity ingestion, Options acquisition, INGRESS, MARKET_STATE, FLOW, WINDOW,
   canonical Stock Alerts, stored readers, disposable publication/read pin,
   rollback, restart/rehydration and empty cleanup.
5. Run `validate_advanced_realtime_v3_local_holistic.py`. Every named check must
   return true and all replay production-effect counters must remain zero.
6. Bind the local-preflight, provider artifacts, host report and holistic report
   semantic/file SHAs into the next source and Docker qualification.

The provider-backed host run may cover a bounded segment while the scheduler
qualification covers all 78 windows provider-free. Record both coverages
separately; never describe three provider-backed cycles as 78 live cycles.

### 5.2 Massive Options request rules

- Broader-chain discovery uses
  `GET /v3/snapshot/options/{underlyingAsset}` with an exact expiration range,
  strike corridor, response limit and byte ceiling. The endpoint returns at most
  250 rows per page. Pagination is a provider request and must be named in the
  grant; `next_url` under a no-pagination grant is terminal.
- Exact admitted-contract alignment uses `GET /v3/snapshot` with
  `ticker.any_of`, `limit`, `sort=ticker` and `order=asc`.
- Do not send `type=options` with `ticker.any_of`; Massive rejects the
  combination with `Cannot specify tickers and type`.
- A cycle response must contain the exact frozen contract set, no `next_url`,
  valid quotes/OI and a bounded provider/host clock skew. Unquoted or invalid
  candidates are excluded during chain discovery, but any missing admitted
  contract fails exact alignment.
- Persist safe HTTP error content, request ID, started/completed timestamps and
  byte accounting before deciding whether a separately authorized retry exists.

### 5.3 Historical context and post-close replay

Live journals contain only observations after their owner starts. They do not
provide prior bars, technical warmup, rates, corporate actions, catalog history
or prior OI.

For a realistic local validation:

1. Keep production data access read-only.
2. After capture/drain, create one owned disposable V2 database.
3. Copy an as-of-bound target-9 baseline with exact row counts and hashes:
   security/reference identities, adjusted daily bars, native 1m/5m/30m bars,
   derived 15m/30m/1h/1d/1wk/1mo bars, lineage, Treasury, corporate actions,
   option catalog and OI.
4. Bind the sealed live journals and replay every completed segment cycle.
5. Require real historical/technical/model-input outcomes, stored readers,
   disposable publication/read pin, rollback and empty DB/role/PID/lock cleanup.

Use `export_advanced_realtime_v3_post_close_history.py` only with a bound replay
manifest and a `PASS` read-only history-source audit. Its per-relation CSV files,
counts and hashes are the copy authority; it is not a general database clone or
standing permission. If a terminal receipt precedes final journal drain, retain
the sealed tail but replay only through the last completed-cycle offsets and
hash the excluded tail. A sealed provider artifact may supplement missing exact
contract catalog/OI only in the owned disposable database under explicit
authority; never backfill production as part of replay.

Run `validate_advanced_realtime_v3_post_close_replay.py` for the bound failed
segment. It must rematerialize Equity only through the completed-cycle cutoff,
replay the exact Options raw/event offsets, persist every cycle's canonical
Stock Alert batch/evaluations, complete all resident barriers, prove
publication/read-pin rollback and restart/rehydration, and remove every owned
database, role and PID. A replay-only security-continuity revision is permitted
only under explicit authority when the production validity interval is absent;
it must retain the security/ticker identity, carry synthetic provenance and
exist only inside the disposable database.

Do not switch to production runtime writes because a disposable copy is
inconvenient.

### 5.4 Broader-chain and nine-underlying expansion

Exact-four QQQ/SPY evidence does not qualify broader chains or all-nine Options.
Before expansion freeze:

- underlyings and call/put membership;
- expiration/DTE range;
- strike corridor;
- maximum contracts per underlying and total;
- pagination and request ceilings;
- T/Q subscription count;
- per-response/session byte ceilings;
- the measurement window, upper-envelope rate, duration, control margin and
  headroom used to size cumulative session bytes;
- journal, memory, disk and DB-connection capacity;
- missing/oversized-chain failure behavior.

Full-chain never means unbounded. Measure first, freeze a bounded membership,
then repeat local-first and Docker qualification for that exact scope.
Expected full-session traffic plus headroom must fit without touching the hard
ceiling. If it does not, the preflight is `NO_GO`; an unlimited ceiling or a
mid-session increase is not a substitute for durable-drain capacity.

For a QQQ/SPY 0-60 calendar-DTE WebSocket diagnostic, freeze the provider-free
plan with `advanced_options_v3_broad_stream.py`. Require call and put coverage
for both underlyings, explicit matching T/Q membership, fresh usable quotes, the
approved strike corridor and at most 1,000 contracts. Membership above the
quote ceiling is terminal; never rank, truncate or silently drop contracts.
Keep the bounded in-memory buffer separate from the larger finite session
receive-byte grant. Reaching either ceiling is terminal and does not authorize
a reconnect.

Run `qualify_advanced_options_v3_broad_stream_capacity.py` before requesting a
live grant. Structural membership, exact offsets and empty cleanup are
necessary but not sufficient: the fixed 1,000-contract/2,000-subscription load
must also pass the journal p95/p99 and total-elapsed gates. A capacity `FAIL`
blocks provider discovery and WebSocket authorization; do not replace it with
an unmeasured larger buffer or session byte ceiling.

## 6. Phase 2 - Fresh Grant And Pre-Open Freeze

Obtain one exact grant naming:

- operator and approver;
- session date;
- migration 038 apply/hardening;
- target-1 root and prospective target-9 child authorization;
- production profile/cohort/manifest/plan writes;
- host schedule and exact service lifecycle;
- Equity connection attempts and decoded-byte ceiling;
- Equity catch-up decision and exact budget;
- Options connection attempts/reconnects/messages/decoded bytes;
- two pre-session admission requests with response and total byte ceilings;
- 78 exact-four per-E snapshot requests with response and total byte ceilings;
- publication, cohort-local pointer, and exact read pin;
- stop/drain/hard-stop clocks;
- zero migration beyond the named migration, replay, retention, dispatch,
  delivery, broker, and trading effects.

Blank values are `NO_GO`.

Complete the signed freeze record before service start:

- exact source/image/package/bundle hashes;
- active target-9 profile SHA;
- exact cohort/manifest membership and SHAs;
- normalizer/finality/finalizer policies;
- journal generation and free-space proof;
- recomputed Equity/Options budget-scope SHAs;
- exact expected 78-cutoff membership SHA;
- prior pointer and read pin;
- baseline process/container/lease inventory;
- source and Docker qualification hashes.

The machine freeze validator must return `GO`.

## 7. Phase 3 - Pre-Open Production Gates

Schedule with catch-up disabled. A missed pre-open task is a missed session.

| UTC | Gate |
| --- | --- |
| 11:00 | Freeze source and prohibit unrelated edits |
| 11:15 | Verify current source/image qualification |
| 11:30 | Apply and harden migration 038 under exact authority |
| 11:45 | Read-only migration/security/conflict audit |
| 12:00 | Verify exact target-9 H/reference/history windows |
| 12:20 | Perform exact-two-request prospective Options admission |
| 12:40 | Freeze acquisition authorities, package, bundle, and scheduler authorization |
| 13:00 | Render/redact Compose; validate secrets and capacity |
| 13:10 | Start acquisition and downstream waiting residents |
| 13:15 | Require every actual-role health check |
| 13:20 | Verify exact subscriptions, journals, owners, and generations |
| 13:25 | Final `GO`/`NO_GO`; arm scheduler |
| 13:30 | Opening capture boundary |

Final `GO` requires:

- migration/security PASS;
- prospective target-9 authorization hierarchy READY;
- exact history/reference/admission PASS;
- unconsumed provider/session authorities;
- no conflicting process, socket, owner, lease, or container;
- all residents healthy;
- exact freeze report `GO`;
- no E-bound cycle authority before E1.

## 8. Phase 4 - Live Cycle Execution

For every E:

1. verify exact finalized 5m membership;
2. seal one `INPUT_READY`;
3. attach one downstream binding without acquisition restart;
4. persist nine canonical Stock Alert evaluations;
5. reserve/fetch exactly one exact-four Options snapshot;
6. require O by E+300 seconds;
7. require exact resident barrier/checkpoints and gap-free evidence;
8. run all Options lanes with explicit outcomes;
9. publish one cohort-local release;
10. enable one exact read pin;
11. verify stored Equity, Stock Alerts, Options, and ticker readers;
12. verify zero provider/detector/publication work at read time;
13. retain forbidden-effect counters.

Cycle status is `COMPLETE_PUBLISHED_PINNED` only after every item passes.

## 9. Monitoring And Troubleshooting

Monitor every 15 seconds using one read-only status path.

At each sample retain:

- container ID, image ID, health, restart count;
- provider sessions/attempts/reconnects/messages/bytes;
- request/byte reservations and completions;
- journal raw/normalized/bridge offsets and checksums;
- finalizer jobs and current 5m heads;
- latest E/O, age, stage/barrier states;
- publication pointer/read pin;
- stored-reader result;
- disk, memory, connection, and journal capacity.

Response contract:

- first 60 seconds: identify the first failed owner/gate and exact durable state;
- E+120: classify provider, acquisition, finalization, downstream, publication,
  or reader failure;
- E+240: stop unbounded retry and prepare terminal evidence;
- E+300: mark the cycle terminal; never backdate it.

If a child exits without a report, inspect durable closure first. Complete exact
closure may finish the operation idempotently; incomplete closure retains a
diagnostic and fails.

Acquisition-plane fixes after pre-open invalidate full-session acquisition and
wait for the next session. Downstream-only fixes use the controlled-transition
contract at a completed cycle boundary after fresh Tier A/B qualification.

## 10. Rollback And Cleanup

On post-publication failure:

1. disable the exact run-owned read pin;
2. restore the exact prior cohort-local pointer;
3. retain the immutable failed release as diagnostic evidence;
4. stop only run-owned downstream roles;
5. preserve acquisition when a controlled downstream transition remains valid.

At close:

- stop provider acquisition at 20:00Z;
- complete decision drain by 20:05Z;
- seal journal/checkpoint/barrier evidence;
- stop and remove every run-owned service;
- verify no owned database, role, container, volume, network, secret, process,
  lock, stale request reservation, pointer, or pin remains unintentionally.

## 11. Verdicts

- `PASS_FULL_SESSION_SINGLE_BUILD`: all 78 cycles under one image and authority.
- `PASS_ACQUISITION_FULL_SESSION`: opening-to-close acquisition identity exact.
- `PASS_END_TO_END_SEGMENT`: every cycle in an explicit downstream segment
  complete; minimum three consecutive cycles plus restart/read gate.
- `PASS_CONTROLLED_TRANSITION`: acquisition preserved and reviewed downstream
  boundary transition complete.
- `PASS_PARTIAL_SESSION`: every authorized cycle after activation passed, but
  opening-to-close scope was not owned.
- `PASS_DIAGNOSTIC_ONLY`: provider-free/non-serving evidence only.
- `FAIL`: missed/divergent cycle, failed rollback, forbidden effect, or
  non-empty cleanup.
- `INVALID`: late/backdated authority, synthetic receipt, mixed acquisition
  identity, unrecorded transition, or unbound evidence.

## 12. Execution Session Entry Gate

The execution session starts with provider-free work only:

1. verify this plan is still the operating plan;
2. verify the 2026-10-28 `NO_GO` state and empty cleanup;
3. inspect the full binding-split implementation status;
4. run the narrow existing scheduler/canonical-alert qualification;
5. produce and verify the zero-effect local-first clock/manifest preflight;
6. run the bounded provider capture, host/disposable replay and local holistic
   validation described in section 5;
7. complete the post-close historical/technical replay when live journals are
   part of the evidence;
8. implement and qualify missing split behavior;
9. produce a new immutable image and an actual-role Docker report bound to the
   local-preflight report;
10. stop and present exact fresh grant fields.

Only after the user supplies the fresh exact grant may that session apply
migration 038, create authorities/schedules, start services, call providers, or
publish.
