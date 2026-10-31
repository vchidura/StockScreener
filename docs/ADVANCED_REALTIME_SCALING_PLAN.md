# Advanced Realtime Scaling Plan

> **Capacity and scaling reference.** Current runtime lifecycle, membership
> reconciliation and implementation status are maintained in
> [REALTIME_V3_PIPELINE.md](REALTIME_V3_PIPELINE.md). Measurements and finite
> resource limits here inform configuration; they do not create qualification or
> startup gates.

Status: **PLANNED; NINE-NAME COORDINATOR SOURCE-QUALIFIED; SCALE UNPROVEN.
2026-10-10 source-only qualification: the current design does not scale to 400/50;
remediation R0 precedes S1.**

2026-10-12: the schema is rebuilt as a new lineage (`stocks_realtime_v2`, squashed
baseline) instead of migrating v1; see
[REALTIME_SCHEMA_V2_DESIGN.md](REALTIME_SCHEMA_V2_DESIGN.md). R0 and the model changes
that touch storage (M2, M5, M7, M8) are implemented natively on v2 after its repositories
exist; milestones S0-S4, the gates and E1-E12 are unchanged. E13 (Options Flow at 8, 25 and
50 underlyings) was added after the v3 review.

This plan addresses the limits of running reference refresh, ingestion,
materialization, detectors and publication synchronously inside one service process.
It does not authorize provider use, enrollment, role changes, schema changes,
service activation, universe expansion or publication.

## Current Baseline

- One advisory leader executes reference refresh and each due window inline.
- `current` completes before `cycle`; materialization/detectors/publication remain
  one synchronous call chain.
- The old forward/options scheduler is one in-flight request at no more than 2 rps.
  That is an application guard, not a provider or global V2 limit. V2 EQUITY_INGEST
  accepts an injected source; its current Polygon source is exact-eight-name canary only,
  serial inside one source call and bounded by 24 source / 88 handoff requests. No
  200-name V2 acquisition source or approved scale pacing/in-flight budget exists.
- Role pools are currently one connection per INGRESS/COMPUTE/PUBLICATION handle.
- Detector ownership is capped at 13 underlyings per shard.
- The selected-option contract is capped at 1,000 quote slots. At 12 slots per
  option underlying, 50 names require 600 slots and four detector shards; 100 names
  require 1,200 and are rejected by the current contract.
- Reference/fundamental refresh is bounded at 104 requests and 16 MiB. Running it
  inline can delay a due market window.
- Source and synthetic gates prove correctness, not throughput, recovery time or a
  complete monitoring day. The current nine-name selection is blocked and no
  continuous live baseline exists.

## Qualification Outcome (2026-10-10)

Source-only qualification found that the current design does not scale to 400/50. The
evidence, findings F1-F8, corrected workload model, recommended model changes,
current-versus-target diagrams, Advanced start profile and retention design are in
[ADVANCED_REALTIME_ARCHITECTURE_QUALIFICATION_2026-10-10.md](ADVANCED_REALTIME_ARCHITECTURE_QUALIFICATION_2026-10-10.md).
This plan keeps the decisions, milestones, gates, exercises and next work package. An
independent evaluation,
[ADVANCED_REALTIME_ARCHITECTURE_EVALUATION_2026-10-10.md](ADVANCED_REALTIME_ARCHITECTURE_EVALUATION_2026-10-10.md),
reached the same blockers; its verified additions are reconciled in the qualification
document.

Headline numbers from that dated service (nine-name isolated profile, linear to 400
where stated):

- Provider: under its fixed one-in-flight, 2 rps guard, at most 180 requests per window
  (about 126 at the measured rate) against 1,472-2,272 demanded for the old 400/50
  full-cycle model. This is not the V2 target pacing model.
- Portal projection: 3.33 s per ticker-cycle, 1,333 s at 400 names on one process.
- Portal snapshots: about 39.6 TB per year uncompressed, about 1,000x the technical
  evidence relations (38.9 GiB per year).

## Decided Objectives And Scope (2026-10-10)

- **Deployment:** one application host with PostgreSQL on the same machine or LAN/VPC.
  Envelope: **8 vCPU / 32 GiB** (decided 2026-10-12; use the current machine's capacity
  if it were lower, which it is not: Windows 11, i7-1270P with 12 cores / 16 threads,
  47.7 GiB RAM, 652 GiB free on C:). Until a Linux host exists, benchmarks run on this
  machine constrained to 8 logical CPUs and a 32 GiB memory budget and are labelled
  `WINDOWS_CONSTRAINED`, not Linux-qualified. Use is **personal** (decided 2026-10-12), so
  the vendor individual license applies and no business or multi-user display license is
  needed; do not share or redistribute the data. The earlier 25-concurrent / 10 RPS LAN
  target is kept only as a headroom test for E6 and may be lowered; a reverse proxy with
  TLS and authentication is needed only if the API is exposed beyond this machine.
- **Primary objective:** equity and option alerts run in real time as far as the tier
  allows. The Developer alert path has a 120 s completion budget (owner, 2026-10-14);
  the v1 alert worker publishes at boundary + 17 min, and the former 90 s is only a Python
  guard in `session_windows.MAX_BUDGET_SECONDS`, `forward_runtime` and `forward_bootstrap`,
  not a schema rule; owner decision pending). Developer detects on 30-minute boundaries
  (owner, 2026-10-13); Advanced is event-driven, see "Lane Split And Cohort Flow". The
  500 ms p95 / 1,000 ms p99 targets are optimization targets, not blocking gates.
- **Portal:** coalesced publication with p95 data age <= 5 min; not on the alert
  deadline chain. STATUS 2026-10-14: not yet true in the V2 code (the projection runs
  inside EQUITY_EVIDENCE, which STOCK_ALERTS waits on, and the data lane runs one stage
  at a time).
- **Initial equity universe (owner, 2026-10-13):** 200 equities first; 400 follows once
  200 is stable.
- **Tiers:** Advanced streaming is the alert target; Developer is the baseline. Separate
  cohorts, no automatic fallback in either direction.
- **Provider rate:** 2 rps is the old forward/options scheduler guard, not a vendor
  limit and not a global V2 equity-ingest budget. Vendor
  documentation (checked 2026-10-12) says each paid asset class has no REST request limit,
  an unpurchased class is on its 5-requests-per-minute Basic tier, WebSocket connections are
  not counted as requests, and one concurrent WebSocket connection per asset class is
  allowed by default (more only through support). No scale pacing or in-flight number is
  approved until E4 proposes one; the absence of a vendor cap is not a reason to choose
  an unsafe value.
- **Retention:** every page shows the last 21 sessions at session-close granularity
  (hot per-cycle portal data 3 days; strategy releases and online alerts 26 sessions);
  alerts and all detector evaluations are archived per session as JSONL files with outcome
  addenda and are not portal-queryable beyond the picker window; technical evidence 3
  months online with no cold archive (recomputable); retention must never block writes or
  degrade latency as history grows. Windows and rationale:
  [REALTIME_SCHEMA_V2_DESIGN.md](REALTIME_SCHEMA_V2_DESIGN.md#retention-windows-decided-2026-10-12).
- **Recovery:** backup, RPO and RTO are a future plan and relaxed; the design must not
  block them. The alert lane auto-restarts and records explicit gaps (hard
  availability target).
- **Options:** fixed slot-set with three versioned full refreshes per day. Evaluating
  admission at refresh instead of at every receipt is a new policy version and needs
  policy review before use.
- **Equities:** `AM` minute aggregates only to start; stock trades and quotes only for
  a named strategy.

## Target Topology

Keep one service as **coordinator only**. It owns exchange scheduling, advisory
leadership, stage dependencies, budgets, state and stop decisions. Execution moves
to independently fenced workers:

| Stage | Worker Shape | Ownership And Fence |
|---|---|---|
| Reference/fundamentals | One singleton worker | Generation identity; six-hour cadence; no overlap with itself |
| Current equity/context ingestion | Bounded underlying shards | INGRESS claims; one distributed account request/byte/pacing ledger |
| Option package acquisition | Initially 4 workers, reviewed range 1-32 | Per-underlying owner/epoch/lease; immutable package receipt |
| Materialization | Bounded job workers | COMPUTE `SKIP LOCKED` claims; input/artifact completion in one transaction |
| Detectors | Stable shards of at most 13 option underlyings | Versioned shard hash, checkpoint and owner epoch |
| Publication | One active publisher | PUBLICATION role; cohort/cutoff/release compare-and-set; warming readers only |

Workers communicate through retained target-local jobs and immutable receipts, not
in-process queues. Restart reconciles committed state before claiming new work.
Reference refresh may run concurrently with compute only when each cycle remains
bound to its already completed generation hashes.

Split the pipeline into two lanes. The **alert lane** (acquisition, bars, evidence,
detectors, alert release) owns the hard deadline. The **portal lane** (projection and
snapshots) coalesces to the latest input, has its own freshness objective and never
blocks the next window.

STATUS (observed 2026-10-14, code reading, not run): this split does not exist yet in the
V2 runtime. See the next section.

## Lane Split And Cohort Flow (PROPOSED 2026-10-14)

The owner reviewed these drafts on 2026-10-14; the decisions at the end are still open and
the split is not implemented. Normative wording in this section describes a proposal, not an
owner decision, unless a sentence is explicitly labelled **Decided**.

### Stage inventory and runtime today (`stages_v1`, from `stage_graph.py` and `stage_orchestrator.py`)

| Stage | Schedule | Needs | Today |
|---|---|---|---|
| REFERENCE | INTERVAL (6 h, resident thread) | none | fails closed until a source is injected |
| EQUITY_INGEST | WINDOW | REFERENCE | ingress role; fails closed without an injected source; the acquisition producer caps at 88 requests (nine-name canary) |
| EQUITY_EVIDENCE | WINDOW | EQUITY_INGEST | compute role: bar drain, load, projection, evidence and screening generation persisted, EQUITY_CYCLE receipt |
| PORTAL_PUBLISH | COALESCED | EQUITY_EVIDENCE | publication role; rebuilds the cycle from durable rows after a restart |
| STRATEGY_PUBLISH | COALESCED | EQUITY_EVIDENCE | 1d and 1h strategy snapshots |
| STOCK_ALERTS | WINDOW | EQUITY_EVIDENCE | alert snapshot; the context, forward and replay producers are missing |
| OPTIONS_ACQUIRE, OPTIONS_ADMISSION, OPTIONS_CYCLE | WINDOW, SESSION, WINDOW | REFERENCE chain | unbound |
| OPTIONS_FLOW | CONTINUOUS (resident thread) | REFERENCE | bound against a local mock feed only |

How it runs:
- `V2Service.poll_windows` runs due windows one after another, oldest first.
- A window has one deadline (release plus the completion budget). A stage never starts after it,
  and lanes still running at the deadline plus a 30 s grace are cancelled (`STAGE_TIMEOUT`).
- A window runs one thread per lane, data and options, concurrently. The lane is derived from the
  stage `domain` in code; it is not stored in the catalog.
- Inside a lane the orchestrator starts one stage at a time, the first runnable one in catalog
  order. The data lane therefore runs EQUITY_INGEST, EQUITY_EVIDENCE, PORTAL_PUBLISH,
  STRATEGY_PUBLISH, STOCK_ALERTS in sequence, although the last three need only EQUITY_EVIDENCE.
- The unit of work is the whole cohort: each stage processes every ticker before the next stage
  starts. Only the bar drain has batch-level jobs with leases.
- Consequence: STOCK_ALERTS waits for the projection (at 200 names 56 s warm and 283 s cold,
  projection alone), then for PORTAL_PUBLISH and STRATEGY_PUBLISH. A projection that overruns
  the deadline leaves STOCK_ALERTS unstarted (recorded MISSED) or cancelled (`STAGE_TIMEOUT`).
- Confirmed on a disposable database through the real EQUITY_EVIDENCE runner and orchestrator:
  an intraday cutoff fails with `Screening session is not completed at cutoff`. The current
  projection therefore cannot execute the assumed 26 windows per session. Approved replacement
  (owner, 2026-10-14): a versioned as-of projection uses completed intraday bars at the cutoff,
  prior completed-session daily ranks and an explicit partial-session status.
- Observed in the alert stage: it loads with `include_portal_history=True` (1m, 5m, 1wk and 1mo
  history its detectors never use), re-validates the whole cohort, and rescans every bar once per
  member. Measured offline 2026-10-14 (stateless compute, synthetic clones, no database load): 9 names 7.6 s, 50 names 65 s,
  100 names 128 s, 200 names 392 s (6.5 min, 1.96 s per ticker, peak 5.7 GiB) against the 90 s budget (120 s proposed). It
  grows faster than linearly: at 200 names the per-member bar scan and validation take 184 s and the per-member-and-interval
  packet scan 124 s (both O(N squared)), the detectors 84 s. Removing the quadratic loops alone leaves about 3 minutes.

### Cohort flow: ticker sets move through the stages

1. The starter unit of flow is one instrument/interval job. Worker services may claim several jobs
   at once; stable-hash shards are a later optimization only if measurements need them.
2. Each step is a pool of long-lived worker processes claiming the existing
   `rt.materialization_jobs` with leases and `SKIP LOCKED`. The existing job barriers name the exact
   expected cohort. No new queue table is introduced; backpressure is explicit, never a silent drop.
3. A step waits for the whole cohort only where the result needs the whole population.
4. A join publishes what is complete at its deadline and records the missing sets, as the v1 alert
   worker does. It never waits for a straggler.
5. A failed set degrades that set only; its lease expires and it is retried.

| Step | Unit | Whole cohort needed |
|---|---|---|
| Acquire (Developer: bounded REST source; Advanced: journal) | instrument | no |
| Bar drain, finalize, derive | instrument/interval | no |
| Alert lane: per-ticker detector input and incremental state | ticker | no |
| Portal lane: per-ticker technical evidence | ticker | no |
| Alert join: selection, dedup, snapshot, outbox (Developer 120 s proposed) | cohort | yes |
| Portal join: screening rank, context rows, summary, Pattern Watch, release (about 5 min) | cohort | yes |

```text
acquire   [S1][S2][S3][S4]
drain        [S1][S2][S3][S4]
detect          [S1][S2][S3][S4] -> alert join -> publish   (<= alert deadline)
evidence        [S1][S2][S3][S4] -> portal join -> publish  (<= 5 min after the bar closes)
```

Latency becomes acquisition of all sets plus the downstream time of the last set plus the join,
instead of the sum of the full-cohort stage times. Not measured.

Approved direction (owner, 2026-10-14): a new catalog version with an explicit projection
stage and `execution_lane` catalog field, separate alert/portal lanes, a 120 s alert deadline and
a 5-minute portal deadline. Reuse the existing jobs, barriers, receipts and window/stage tables;
do not add a queue or deadline table. The coordinator owns stage runs and barriers; independently
launched worker services own only leased jobs. Start at one worker and measure 1/2/4/8 processes.

Implementation increments, each test-first and usable alone:
1. Shared canonical input boundary. One final raw 1m source per tier feeds persisted 1m bars;
   watermark/completeness finality derives 5m/15m/30m/1h (daily history refresh is separate).
   Ambiguous buckets wait; a lane-deadline miss records GAP/LATE, and a later completion is a new
   immutable revision with its actual availability time. The stage emits a cheap durable
   `INPUT_READY` receipt from canonical bar/coverage heads; it does not load portal history.
2. Worker services. Reuse `rt.materialization_jobs` and job barriers for bar finalization,
   per-security 30m detector work and per-ticker portal projection. Persist output and complete the
   job lease atomically. The stage controller performs only enqueue, barrier and cohort join work.
3. Independent branches. STOCK_ALERTS loads only detector inputs, joins candidates across the
   exact cohort, deduplicates and publishes. EQUITY_PROJECTION loads its own full/delta portal input,
   produces a versioned as-of projection and emits `PORTAL_READY`; PORTAL_PUBLISH consumes it.
   A projection failure or delay cannot change alert output or timing.

Status 2026-10-14, increment 1a built and verified: `stages_v2` stores explicit lanes while
`stages_v1` is unchanged; EQUITY_MATERIALIZE emits `INPUT_READY` without portal
projection, EQUITY_PROJECTION emits exact `PORTAL_READY`, and lane independence is tested. The
generic worker contract reuses materialization-job leases and completes output plus every open
exact-input barrier atomically. Durable per-ticker 1m-to-5m jobs, COMPLETE/GAP/LATE outcomes and a
guarded bounded worker process are built. The alert split is built:
the repository loads only 1d/30m winners, the producer validates and hashes only that domain, and
full-history versus detector-only inputs produce identical snapshots. Bars and packets are grouped
once, removing the two measured cohort rescans; post-change scale timing is not run yet.
INPUT_READY v2 binds the full cohort/profile/manifest/reference/source-receipt scope; PORTAL_READY
proves the projection but both alert and portal snapshots publish into the same INPUT_READY release
bundle. A provider-free two-ticker real-schema rehearsal passes end to end. Work identity is one
ticker/instrument job; workers claim bounded batches and the exact cohort barrier owns the join.
Starter decision (owner, 2026-10-14): INPUT_READY is exact/fail-closed for every cohort member;
partial releases remain a future versioned policy, not an implicit fallback.
Advanced AM finality requires exact journal/session/epoch/offset/watermark/gap proof; Advanced
INPUT_READY binds sessions, checkpoint/state hashes, unresolved gaps and finalizer heads. Intraday
screening implements the versioned partial-session as-of contract. Explicit catalog offsets enforce
120 s data/alert and 300 s portal deadlines without one lane cancelling another. The no-new-table
starter keeps detector selection and portal projection cohort-level because existing schemas hold
only final cohort artifacts; no unsafe draft was introduced.
A guarded provider-free Advanced bridge now reads committed normalized SQLite batches, verifies
session/epoch/gap finality, and atomically persists canonical 1m bars with every shard checkpoint.
A disposable integration proves five AM records through durable 5m finalization and Advanced
INPUT_READY. Fast: 1,823 passed / 151 skipped; DB: 285 passed; no leftovers; no provider call.

### Developer alert lane (PROPOSED)

**Decided cadence (owner, 2026-10-13):** detect on 30-minute boundaries. Approved budget: publish
within 120 s of the data being available (v1: boundary + 17 min). Load only
the detector inputs (30m raw, daily adjusted and raw), group by ticker, keep
per-ticker incremental state, and complete at once on windows with no new 30m boundary. Publish at
the deadline with missing members listed (v1 behaviour). The dedup key is detector, security, 30m
boundary, policy and cohort (`rt.detector_events.dedup_sha256` UNIQUE), so a re-run is harmless.
Per-ticker trigger (owner idea, 2026-10-14): when a ticker's 30m bar is persisted, a bounded worker pool evaluates that
ticker's detectors at once and persists its candidates; one small cohort join then runs the selection. Selection cannot be
per ticker: `select_candidates` keeps at most 3 candidates per model per window ranked by priority (`arm="PRIORITY"`,
`cap=3`; arm `ALL` is a research arm) and enforces the active-position cap, so raising an alert the instant a ticker
qualifies would change the selection policy. Alert latency is then the last ticker's ingest and evaluation plus the join.
The v1 `advance_detectors` is already incremental per security and interval (retained bars, saved per-model state and
ordinal, `daily_processed`: the episode state that prevents duplicate detections), but it deep-copies the whole cohort state
and rebuilds the cohort daily rank context on every call, so a per-ticker entry point is needed and the rank context is
cached per session. V2's `evaluate_alert_inputs` is stateless: it re-evaluates the last 500 bars of every ticker each
window. Use processes, not a thread per ticker (CPU-bound Python), one job in flight per ticker (hash shards), and a
committer that batches transactions (one connection per role handle today); overload degrades explicitly, never drops
silently. Stateless alert compute measured offline (no database load): 9 names 7.6 s, 25 names 25 s, 50 names 65 s, 100
names 128 s, 200 names 392 s (1.96 s per ticker). Incremental (v1 `advance_detectors`, warm state, one new 30m bar per ticker,
no database, one process, measured 2026-10-14): 0.15 s per ticker at 9 and 50 names and 0.22 s at 200 names (a heap-size
effect is likely, as in the projection); a whole-cohort call takes 38-48 s at 200 names (0.19 s per ticker; it deep-copies the
whole state each call) and a cold bootstrap over all history 114 s (a restart). Frame rebuilds are about half of the
per-ticker cost. Not measured: database reads and writes, ingest, the selection join, position updates; the per-ticker slice
computes the rank context over one member, so its cost counts and its output does not. Database side (disposable database,
fixture insert path, no ingest; 9 / 50 names): the current alert-stage full load (`include_portal_history=True`) takes 5.9 s /
33 s (50 microseconds per row, 13.1k rows per ticker, linear, so about 130 s at 200 names by extrapolation: the 200-name
database run was interrupted before it finished and is NOT measured); a
detector-only winners read (30m raw + 1d, 1.5k rows per ticker) 0.7 s / 3.9 s; one boundary's persist of 30m+1h bars 0.02 s for
100 rows and of the rest of the window's 1m/5m/15m bars 0.76 s for 1,900 rows (0.4 ms per row); the delta read of the new
bars (count over the whole candidate set plus the new rows) 0.21 s / 0.46 s for detector-only and 0.31 s / 0.66 s for the
EQUITY_EVIDENCE delta loader. Seeding runs at about 2.3k rows per second (2.6M bars at 200 names is about 19 minutes).
Ingest one shared final raw 1m source (Developer REST or Advanced AM), derive intraday intervals
once and refresh daily adjusted/raw history separately; the alert and portal lanes never issue
duplicate interval fetches. Provider rate/in-flight stays unset until E4. Acceptance tests: a slow
or failing projection changes neither alert outcome nor timing; the detector-only load gives a
byte-identical alert snapshot to the current full load; advancing state across boundaries equals a
fresh evaluation; a mid-window restart gives the same alerts and no duplicate rows; a measurement
at 200 names (load, drain, detect, publish) with 60 s of the 120 s as the target.

### Advanced stream flow (PROPOSED)

**Decided behavior:** Advanced detection is event-driven and deduplicates by immutable event
identity. The starter uses the existing local SQLite WAL journal; a broker requires a measured
limit. Existing 30m detector identities run on finalized 30m bars; 1m/5m feed canonical state and
portal projection but create no new alert identity. Portal projection coalesces the latest complete
input on each finalized 5m boundary. The remaining topology and latency targets below are proposed.

```text
Stocks WS (AM x400) + Options WS (<= 1,000 contracts)
  -> gateway -> bounded buffer -> durable raw journal (ack after flush)
  -> normalizer -> 1m bar facts (REALTIME_STREAM, journal lineage) -> 5m finalize -> derived 15m/30m/1h/1d
  -> rt.bar_revisions -> ALERT lane: resident detector shards, dirty detectors only, one transaction
                          for events + state + outbox + checkpoint -> push
                      -> charts read bars directly
                      -> PORTAL lane: coalescing projector on each 5m boundary, up to 5 min late
```

#### Stream budget taxonomy and sizing

The stream has two different byte controls; they must never share one value:

- **Buffer capacity** bounds live RAM and queue age while the durable journal
  drains. It is intentionally small and cannot be unlimited. Reaching it means
  the consumer is slower than ingress or stalled; continuing would only move
  the failure to process memory, journal age or disk.
- **Cumulative session receive budget** bounds the provider effect authorized
  for one feed/session and supplies auditable request/message/byte accounting.
  It is not a memory limit. It must cover the complete approved session, control
  frames and measured bursts with reviewed headroom.

For a frozen membership and duration, derive the cumulative ceiling from the
measured upper-envelope decoded byte rate:

`ceiling >= upper_envelope_bytes_per_second * authorized_seconds * headroom + control_margin`

Record the capture used, percentile/window definition, duration, membership and
headroom multiplier. The ceiling is invalid if it is copied from the buffer
size, an earlier universe or a shorter capture. Before launch, reserve matching
journal/WAL/disk capacity and require the measured durable drain rate to exceed
the admitted upper envelope. A larger session ceiling cannot repair a slow
normalizer or journal consumer.

Expected traffic must not hit the hard ceiling. Emit early usage warnings and
fail preflight when full-session demand plus headroom does not fit. Change a
ceiling only in a new reviewed grant; never raise it during a running session.
For multi-session services, use a fresh bounded reservation per session rather
than an unlimited lifetime counter. Unexpected scope expansion, runaway
provider traffic or a slow consumer still fails closed with exact received,
admitted and durable offsets.

Targets (proposed in the preflight design, not measured): journal accept p95 25 ms, receipt to
feature state p95 75 ms, last event to committed evidence and outbox p95 500 ms, outbox to portal
push p95 250 ms, finalized bar to lightweight strategy p95 1 s. The portal is published within 5
minutes of each bar closing (p95). A detector that needs a completed 30m bar fires at the boundary
plus seconds (Developer: plus 17 minutes) but not more often; a faster cadence needs new detector
identities on 1m or 5m bars. At 400 names the portal projection projects to about 110 s warm
(0.28 s per ticker, load and persist not measured) and a cold restart to about 9.5 minutes, shown
with its as-of time. Gaps are explicit rows, REST reconcile fills missing minutes, a detector over
a gapped window degrades or fails closed, and a restart replays the journal after the committed
offset. Exists at source level: gateway, buffer, raw journal with `read_after`,
`adapt_advanced_minute_aggregate` (AM to 1m fact), derived-bar adapter, journal columns on bar rows,
a projection that accepts stream-sourced 5m bars, the option-flow consumer, and the dedup, state and
outbox schema. Missing or unverified: the stock normalizer and 1m to 5m finalization, a stream bar
writer under the compute role, a resident equity detector runtime with episode state, stream stages
in the catalog (a new version), equity shard readiness epochs, the portal 5m trigger, push delivery.

### Open decisions (ask one at a time)

exact catalog encoding of lane deadline offsets (no new table); E4-derived account-wide
rate/in-flight budget; benchmark-selected worker counts. Provider-backed implementation remains
unauthorized until E4. New 1m/5m detector identities remain out of scope.

## Advanced Start Profile And Extension Ladder (PROPOSED)

Proposed ADV-S1 start: 400 equities (initial universe 200, owner 2026-10-13) on `AM` only, 50 option underlyings x 12 slots =
600 primary + 400 reserve = 1,000 on one options connection, three explicit versioned
slot rotations per day (make-before-break, open-alert legs pinned), nine-name ramp
first. The extension ladder L1-L7, rotation diagram and rationale are in the
[qualification document](ADVANCED_REALTIME_ARCHITECTURE_QUALIFICATION_2026-10-10.md#advanced-start-profile-and-extension-ladder-proposed).
No step is automatic; each needs its own approval and measured evidence.

## Bar And Technical-Evidence Persistence

Separate retained market facts from derived technical evidence.

### Finalized Bars

Retain finalized native 5-minute equity bars. They remain the lowest common equity
source for:

- 5-minute chart reads;
- deterministic derivation and reconciliation of 15m, 30m, 1h and raw daily bars;
- exact underlying-equity alignment for option quote/matrix and valuation decisions;
- reconstruction of technical indicators under a new model version;
- replay/recovery without another provider request.

Persist finalized derived coarser bars with exact source revision IDs so readers do
not repeatedly aggregate 5-minute history. Five-minute bars are market facts, not a
reason to persist five-minute technical snapshots for every ticker.

For each option decision, retain or reference the exact:

- underlying 5-minute bar revision;
- option quote/matrix revision or package receipt;
- valuation/model policy version and hash;
- decision market time and observed time.

Equity 5-minute bars do not replace option bid/ask, IV, open interest, volume,
Greeks, contract identity or expiration evidence.

### Existing Realtime Evidence Contract

Reuse migration 006; do not create a duplicate technical-evidence table:

- `rt.equity_evidence`: immutable versioned JSONB feature/setup/pattern/channel
  evidence;
- `rt.equity_evidence_inputs`: ordered primary and confirmation bar lineage;
- `rt.equity_evidence_current`: lifecycle-key current evidence pointers;
- `rt.feature_definitions`: bundle, dependency, policy and code identity;
- `rt.feature_snapshots` / `rt.feature_current`: generation snapshots and bounded
  current pointers.

Ticker publication should provider-free dual-write its existing
`RealtimeEquityMaterializationResult` through
`persist_realtime_equity_materialization_cursor`, then build portal snapshots from
the same evidence identities. Keep portal GETs stored-only.

JSONB payloads are intentionally extensible. The existing typed
`payload_schema_version` column is authoritative; do not duplicate it inside JSONB
unless independently exported payloads must be self-describing. Additive indicators
may use a compatible contract only when that schema version permits them; changed
meaning or shape requires a new version. Keep frequently filtered identity,
interval, time, type and quality fields as typed/indexed columns. Do not create one
row per indicator or duplicate candles inside evidence payloads.

### Initial Evidence Cadence

Do not persist 5-minute feature/setup evidence by default. Add it only for a named
strategy or consumer. Initial supported decision intervals are 30m, 1h, 1d, 1wk
and 1mo.

- Persist one `FEATURE_SNAPSHOT` whenever a supported interval completes.
- Initial parity dual-write persists the complete materialization result at each
  supported completed interval. Do not filter to feature/setup rows: setup payloads
  and source IDs reference feature, pattern and scanner evidence.
- A later setup-volume optimization may become event-driven only after defining a
  versioned semantic-state hash and transitive evidence-closure persistence. Literal
  payload hashes are not a useful change detector because market time, prices,
  indicators, levels and evidence IDs legitimately change each completion.
- Persist scanner/pattern/channel/regime/fundamental evidence only when produced by
  their contracts.
- Preserve immutable prior evidence; atomically advance only the relevant current
  pointer after deterministic conflict verification.

For 400 equities, mandatory feature snapshots on a full trading day are initially:

| Interval | Completions | Rows / Day |
|---|---:|---:|
| 30m | 13 | 5,200 |
| 1h | 7 | 2,800 |
| 1d | 1 | 400 |
| **Total** | 21 | **8,400** |

Weekly/monthly rows occur only when those periods complete. Initial parity should
budget another 8,400 setup rows/day plus variable scanner/pattern/channel/regime
dependency rows. Event-driven reduction is a measured follow-up, not the initial
correctness contract.

The current-pointer table is bounded. At 400 tickers, five decision intervals and
two evidence types, expect at most 4,000 pointer rows per cohort/model variant.
Pointer rows remain narrow; technical payloads stay in immutable evidence. Preserve
existing monotonic rules: `rt.equity_evidence_current` advances by
`(market_time, observed_at)` within its identity/provider tier, while
`rt.feature_current` advances by the exact positive producer-plan generation within
cohort/instrument/bundle. Do not invent a read-time/global `source_generation`.

Alert, option and decision consumers do not persist technical evidence ad hoc. The
COMPUTE/materialization owner persists and verifies the complete evidence closure.
Only durable outputs whose result actually consumed that technical evidence retain
one stable top-level evidence reference plus policy/source identity. Setup evidence
owns its transitive feature/pattern/scanner closure; consumers do not copy every
dependency ID. Option outputs that used only spot/quote/matrix facts reference those
market revisions instead of an unrelated setup. Portal/UI reads do not create
consumer bindings. A decision that requires evidence waits for its durable receipt
rather than silently reconstructing or backdating it.

Expose a stable evidence-reference DTO/repository boundary rather than JSONB/table
details. Evidence never points back to alerts/options/decisions, consumers bind an
immutable ID once, and mutable current pointers are used only for discovery. This
keeps dependency direction one-way and avoids cycles.

Before scale acceptance, measure average/p95 JSONB bytes, rows/day, index/WAL
amplification, one-year retained size, pointer update lock time, batch throughput and
partition pruning. Define retention/archival from measured volume; do not infer it
from row counts alone.

## Retention And Non-Degradation Design (PROPOSED)

Storage growth is bounded per data class, and no hot-path query, write or maintenance
job slows down as history accumulates. Retention must never block ingestion or alerts.
The data classes, nine rules (never a blocker, whole-partition lifecycle, archive before
drop, pre-created partitions, prune or bound, history-growth regression, index and
vacuum hygiene, forward-only migration, capacity alarms) and the portal-snapshot
hot/kept split are in the
[qualification document](ADVANCED_REALTIME_ARCHITECTURE_QUALIFICATION_2026-10-10.md#retention-and-non-degradation-design-proposed).
Gates and exercises E6 and E9 below enforce them.

## Scale Milestones

### R0: Efficiency Remediation (Source-Only; Blocks S1)

May run in parallel with S0. No schema change, provider or database access until
E1-E4 pass.

- Replace embedded bars in snapshot manifests with a lineage head (counts, per-interval
  first and last revision, ordered chain hash); reference evidence and serve charts by
  reference.
- Per-ticker incremental projection state with changed-interval gating; remove the
  global input hash and O(T x N) scans.
- Move the portal to a coalescing lane outside the deadline chain, dispatched from a
  durable cycle receipt (cohort, cutoff, reference generation, universe digest).
- Reader: typed identity columns, ETag from the payload hash, compression, summary
  cache and a precomputed Pattern Watch snapshot.
- Define the slot-set version contract and its policy review.
- Exit: E1-E4 accepted on the Linux target. Proposed bar: at most 0.6 s per
  ticker-cycle, manifest at most 10 KB, linear scaling to 400 names, bounded memory.
  Under owner review 2026-10-14: the 0.6 s bar measures the in-memory projection only (no
  load or persist) and was inferred from fitting 400 names in about 5 minutes. The v1
  baseline (worker log, 386 tickers) is charts about 1 minute and technical data within
  5-6 minutes (median) after the data is available; parity with that is the proposed
  portal criterion, and the alert path has its own budget (see "Lane Split And Cohort Flow").

### S0: Nine-Name Live Baseline

- Resolve the immutable strategy-depth blocker without weakening 7/21/45 DTE,
  two-per-side depth, liquidity or deviation policy.
- Complete one bounded canary and one full monitoring day.
- Measure stage latency, requests/bytes, package sizes, database lock time, WAL,
  CPU/memory, publication lag and failure recovery.

### S1: Worker Extraction

- Extract reference, acquisition, materialization, detector and publication stage
  adapters behind the existing coordinator.
- Persist stage/job identity, attempts, owner epoch, lease, deadline, result receipt
  and failure reason. A coordinator crash must not duplicate provider acquisition.
- Keep publication singleton and preserve the current stored-only reader contract.

### S2: Fifty Option Underlyings

- Freeze one reviewed 50-name universe and the 600-slot capacity identity.
- Use four package workers initially and four detector shards of 13/13/13/11.
- Prove the global provider ledger, 1,000-quote cap, pool/connection plan and exact
  complete-universe finalization on synthetic and disposable PostgreSQL fixtures.
- Run a bounded provider feasibility measurement before enrollment. Stop at
  measured request/byte/deadline limits; do not infer capacity from code ceilings.

### S3: Four Hundred Equities / Fifty Option Underlyings

- Keep equities and option subscriptions as separate declared sets; 400 equities
  do not imply 400 option underlyings.
- Version equity ingestion shards and prove native/derived bar finalization,
  feature/evidence materialization and cross-underlying context at one cutoff.
- Benchmark the documented traffic envelope, journal/replay/recovery behavior,
  database contention, archive/WAL growth and one-year evidence retention with at
  least 30% usable storage and connection headroom.
- Introduce Redis/Kafka-style transport only if measured local PostgreSQL/job or
  journal limits justify the operational cost.

### S4: Deployment And Soak

- Run process-crash, stale-lease, provider timeout/429, database restart, publisher
  failover and checkpoint-recovery tests.
- Prove no backdating, duplicate acquisition, cross-shard drift, partial-universe
  READY result or mutable publication overwrite.
- Require one accepted monitoring day at each scale tier before the next expansion.

## Acceptance Gates

| Gate | Required Evidence |
|---|---|
| Correctness | Exact cohort/profile/manifest/config/reference/action/shard hashes; immutable receipts; deterministic replay |
| Latency | Developer alert path completes within its approved 120 s deadline. Advanced bar-close and receipt-to-evidence latency is measured and reported against the preflight targets (optimization targets, not blocking gates). Portal p95 data age <= 5 min |
| Provider | Account-wide request/byte/rate/retry totals include every worker and failed attempt; documented entitlement headroom; deadline-bound demand fits ledger capacity inside the lane's deadline |
| Database | Reviewed total connection ceiling, no pool aliasing, bounded lock waits, WAL/archive capacity and zero unresolved ownership conflicts; p95 read/write latency within 10% across 1/30/100/250 retained days |
| Recovery | Coordinator and worker restart resume committed jobs without provider replay; stale epochs cannot complete or fail work |
| Coverage | Exact universe complete or explicitly BLOCKED; no shrinking to available names and no policy relaxation |
| Operations | Status distinguishes process health, stage completion, data readiness, publication freshness and historical coverage |
| Evidence | Feature/setup IDs and payload hashes match portal releases; exact source-bar lineage; no GET-time materialization |
| Storage | Measured row/payload/index/WAL volume for snapshots, bars, option records, evidence and the Advanced journal stays within per-class reviewed capacity with at least 30% free capacity (capacity >= used / 0.70); retention never blocks writes |

## Work Items

R0 first (source-only, see milestone R0 and the exercise matrix). Then:

1. Define versioned stage/job DTOs and repository operations using the existing
   lease/fencing patterns.
2. Refactor the service to dispatch jobs and observe durable completions instead of
   calling provider/compute stages inline.
3. Add process entry points for acquisition, materialization, detector shards and
   publication; each accepts one role-scoped pool and one bounded job claim.
4. Add connection-budget planning across coordinator and maximum worker counts.
5. Build 9/50 and 400/50 synthetic load fixtures plus disposable PostgreSQL
   concurrency, crash and recovery acceptance.
6. Wire provider-free ticker dual-write into the existing realtime evidence
  repository and prove parity before switching readers.
7. Measure evidence batching, current-pointer contention, partition/retention and
  one-year storage/WAL projections at 9 and 400 equities.
8. Capture measured reports before proposing any provider-backed expansion.

Advanced streaming remains a separate gateway/journal/normalizer lane and is the
alert target; Developer is the baseline. This plan scales the Developer REST service
and shared target-local contracts; it does not turn Developer polling into an Advanced
stream or use either tier as fallback.

## Staged Exercise Matrix

| E | Type | Pass criterion |
|---|---|---|
| E1 | Source-only | Three isolated profiler runs on the 8 vCPU / 32 GiB envelope (Linux when available, otherwise the constrained current machine, labelled `WINDOWS_CONSTRAINED`); variance <= 10% |
| E2 | Source-only | Synthetic 9/50/100/200/400 curve for time, RSS and bytes; per-ticker time within 1.3x of nine names |
| E3 | Source-only | Remediation prototype meets the R0 exit bar |
| E4 | Source-only | Pacing simulator over retained reports with 429 injection proposes rate and in-flight limit; no provider call |
| E5 | Disposable database | Snapshot size and WAL per design at 400 x 26 cycles; identity-query cost with and without typed columns |
| E6 | Disposable database + API | 25 concurrent / 10 RPS with page-load fan-out, cold and warm and a release-swap scan; p95 within 10% across 1/30/100/250 retained days; memory bounded; pool wait and exhaustion measured; ten minutes at 10 RPS (6,000 reads) is a diagnostic, not tail certification |
| E7 | Disposable database | 8,400+ jobs per day with crash and lease expiry; no duplicate completion; barrier exact |
| E8 | Disposable database | Crash after attempt reserve, receipt commit, each evidence chunk and before and after the release compare-and-set; database restart, 429 storm and +/-2 s clock skew; ambiguous attempts recorded, committed work reused, publisher singleton, no partial-universe READY |
| E9 | Disposable database | Partition create, detach and drop under write load without blocking at 1, 3 and 4 months of evidence and 1, 6 and 26 sessions of snapshots and alerts; archive verify-then-drop for alerts and evaluations (file hashes match, open pins block the drop); catalog rows pruned with their partition; schema rollout expand, verify, new writer, parity, new reader with rollback by routing to a retained release |
| E10 | Dry run (Linux) | Service accounts, secret storage, TLS proxy and systemd restart |
| E11 | Source-only | Slot rotation simulation: make-before-break, churn <= reserve, open-alert legs pinned |
| E12 | Exact authorization required | Bounded Advanced capture sizes `AM`, option `Q`/`T` rates and journal volume; only after E11 |
| E13 | Source-only, then disposable database | Options Flow at 8, 25 and 50 option underlyings with the policy chain per underlying (v3 review finding 6). Source-only: one chain validation per underlying per cycle (not U squared), the open-interest pass linear in contracts, and an options-cycle input that carries discovery batch ids and hashes, never another copy of raw response or bar bytes, so its size does not approach the 64 MiB cap; measured offline by `backend/scripts/profile_option_flow_publication.py`. Disposable database: seal-to-READY time, WAL and relation growth per cycle, `rt.option_discovery_pages` partition size and TTL drop, reader p95 for `/api/options/flow`; times that need provider-shaped chains are NOT_CHECKED until a chain fixture of 25/50 underlyings at the configured contract cap exists |

Results to date (2026-10-13; details in the design doc status table): E1 quiet machine, three cold runs spread 3%
(cold 1.30 s per ticker-cycle, warm 0.21 s, one-interval 0.24 s; the 0.6 s bar is met except cold); E2 clean run
2026-10-13 night, quiet machine, after the memory work and `gc.freeze` (9/50/100/200 names: cold 1.31/1.47/1.38/1.42 s,
warm 0.18/0.20/0.21/0.28 s, one-interval 0.21/0.24/0.25/0.33 s per ticker, peak 0.54/2.08/3.95/7.70 GiB; at 200 names warm and
one-interval are 1.56x and 1.57x the nine-name values against the 1.3x bar, cause not diagnosed), 400 names not run (the
initial universe is 200; projected about 15.5 GiB; the earlier contended run and its 25.4 GiB projection are superseded);
E3 payload and manifest bars met (60.6 KB payload and 5,125 B manifest per ticker); E9 partial (the ordinary-retention
executor drops HOT_TTL and KEPT_3M partitions on a disposable database; archive verify-then-drop was already covered);
E13 source-only part measured (options-cycle input 34.8 KB at 50 underlyings), the rest NOT_CHECKED; E5-E8 not
measured at scale (the rehearsal only touches the claim, retry and crash-and-retry paths on two tickers); E10, E11 and
E12 not started.

## Next Work Package (Resume Packet)

```text
Objective: RT-FIX (blockers RT-02/03/04 in code with offline tests), then V2-7/V2-8
Mode: implementation
Checkpoint: advanced-realtime
Design: docs/REALTIME_SCHEMA_V2_DESIGN.md (baseline built; see Implementation status)
Anchors: backend/realtime/polygon_pre_session_source.py,
         backend/options/data/polygon_developer.py (minute-bar reader),
         backend/realtime/option_pre_session_runtime.py,
         docs/REALTIME_PIPELINE_BLOCKERS_2026-10-12.md
Allowed actions: source edits; offline tests; test_realtime_v2_* disposable databases
                 (validate_realtime_v2_schema.py)
Do not change: stocks_realtime_v1, providers, roles on live databases, services,
               frozen cohorts and evidence; no provisioning or freezing of stocks_realtime_v2
Sequencing: R0 (ticker_projection.py lineage head, incremental state) follows V2-8
Done when: the blocker regressions listed in the blockers report pass offline
Focused validation: test_polygon_pre_session_source.py, test_polygon_developer.py,
                    test_option_pre_session_runtime.py, test_normalizer.py
```