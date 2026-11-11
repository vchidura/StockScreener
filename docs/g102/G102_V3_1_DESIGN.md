# V3-1: Options market-state capacity and the window's price input (design)

Matrix row V3-1 (and G75 V4, R32, R33, R34, R35, R37). Status: DESIGN for review, Nov 11 2026. Nothing here is built.
Goal set by the user (Nov 9): reach the V3 Options runtime in as few cycles as possible, with no throwaway P8 work.

## 1. Goal

Run Options with more streamed contracts, either more contracts per underlying (V4: 24 per name, 216 for 9 names) or
more underlyings (23 at 8 each, 184), without the market-state service saturating a core, and make the Options window
price contracts as of the cutoff from stored state, so marks no longer depend on the host clock (R34) or on when the
window happens to run (R35).

Acceptance: at 3x today's record rate, market-state stays under 65% of one core at the busiest minute and under 25%
on average; window marks are identical whether the window runs at the cutoff or minutes later; a restart resumes from
checkpoints with byte-identical state.

## 2. What runs today (measured Nov 11, read-only)

- One `realtime-v3-options-p8-market-state` process; the C6 layout gave **one shard** holding all 72 contracts.
- Nov 10 Options journal (`options-acquisition/47d62b40.../options.sqlite`, 4.7 GB): 6,301,830 normalized records
  (5,849,890 quotes, 451,940 trades); average ~270 records/s over the session, busiest minute 31,347 (~520/s, 18:02).
- Cost per record in the r84 image on one CPU: SQLite row 3.8 us, full decode into `NormalizedJournalRecord` 23.0 us,
  materialize 9.7 us. The day's data work (decoded twice: the shard consumer and the detector router, materialized once)
  is about 350 CPU-seconds, under 2% of a core on average and about 30% at the busiest minute.
- Observed (Nov 9): market-state p50 42%, p90 86%, max 99% of one core.

**Finding:** the data is not what fills the core. The role loop (`advanced_options_p8_roles.py`, market-state branch)
runs up to ~10 iterations a second and on every one: queries the stream registration, runs the resident-window
snapshot (a read transaction plus SQLite offset lookups per waiting window), renews the shard lease, opens a new SQLite
connection for the slice read and another for the router read, and, when records arrived, writes two transactions
(state + C6 progress, routing state). Each transaction adds three setup statements (isolation, role, statement
timeout). The fixed per-iteration cost, not the record volume, is the likely load. The original V3-1 outline (split
the service across processes) would multiply this overhead per process without removing it. **Step 0 confirms this
with a profile before any code.**

## 3. Design

### Step 0 - Measure (no code change)
- Profile the live market-state process during a session: `py-spy record` from a side container sharing its PID
  namespace (read-only sampling, ~2 minutes at a busy and a quiet time). Output: share of time in DB round trips, SQLite
  open/read, decode, materialize, hashing, sleeping.
- Same for the flow role and the window (R35 showed the window CPU-bound).
- Decision gate: if per-iteration overhead is above half of the busy-time CPU, Steps 1-2 come first; if decode or
  materialize dominate, Step 4 moves up.
- Ready (Nov 11, user's yes): `runner/profile_options_roles.sh SECONDS` (py-spy 0.3.14 in a side container, `--nonblocking`,
  market-state, flow and window one after the other, docker CPU every 5 s) and `runner/summarize_profile.py` (share of
  samples by cause). Off-hours test: attaches, 0 samples (services idle at ~0.1% CPU). Runs scheduled for the Nov 11
  session at about 14:49, 15:43, 17:57 and 20:47 UTC, 3 minutes per service.

### Step 1 - Cadence (market-state, then the same pattern in flow and ingress)
- Registration refresh, resident-window snapshot and lease renewal on timers (1 s; lease stays 30 s) instead of every
  iteration; the snapshot only when the committed offset moved or a new window became ready.
- Adaptive idle wait: 0.1 s after a pass that read records, growing to 0.5 s when idle (window completion latency stays
  within ~1 s of the cutoff offset being reached).
- One long-lived read-only SQLite connection per process (WAL), not a connection per read.
- Batch the C6 progress write into the state transaction (already the same transaction) and skip it when unchanged.

### Step 2 - Read once
- One `read_normalized_after` per pass, shared by the shard consumer(s) and the detector router (the router keeps its
  own committed offset; it processes the shared batch from its offset). Halves decode cost.
- Optional: decode lazily (keep SQLite rows; build Decimal fields only for the instruments a shard owns).

### Step 3 - Capacity check at 3x
- Replay a session's journal through the changed role on a copy at a multiple of real speed (copy rehearsal, Section 5)
  and measure CPU per minute against the acceptance numbers. Expected after Steps 1-2: data work at 3x ~33 us x 1,570
  records/s at the busiest minute = ~52% of a core, plus a small fixed cost.

### Step 4 - Shards across processes (only if Step 3 misses the target)
The per-shard leases already exist (`CANONICAL_MARKET_STATE_OWNER:cohort:generation:shardmap:shard`, 30 s), so N
market-state containers can share shards by lease. Needed changes:
- C6 layout with several shards (by underlying) instead of one.
- Each process reads only its shards: `WHERE shard_id IN (...)` with an index on (shard_id, event_offset), and the
  materializer accepts a scanned range (today it requires contiguous offsets across all shards).
- Completion barrier: MARKET_STATE is complete for a window when every shard's stored checkpoint has reached the
  cutoff's event offset (read from the checkpoints), not a process's own minimum.
- The detector router stays in one process (it needs every record) or moves to the flow role.

### Step 5 - The window's price input at the cutoff (R34, R35)
- Today the mark producer reads the latest `rt.quote_current` head and filters it by age against the cutoff
  (`advanced_options_mark_producer.py`), so a slow window or a skewed clock changes the marks.
- New contract: the window takes, per contract, the newest quote at or before the cutoff's **event offset** from the
  market-state checkpoint that satisfied the barrier (the shard state keeps 8 quotes per contract); age is measured in
  market time (cutoff minus quote event time), not against received_at. Same marks whenever the window runs; no clock
  dependence.
- The trigger-time facts on alerts (R33: spot, day volume, prior OI) come from the same as-of state and the rolling
  state, stored with the evaluation when it is created.

### Folded into V3-1 (no P8 patch)
- R32: past-deadline windows closed as MISSED (self-heal MARK_MISSED, R38) once the barrier is checkpoint-based.
- R37: a role stop finishes its current pass and records the stop instead of raising after a fixed bound.
- R35: per-stage run time recorded with each window (input, cycle, projection, publish) for the run-time alarm.

## 4. Order and cycles

| Step | What | Services | Rehearsal | Earliest live |
|---|---|---|---|---|
| 0 | Profile | none (side container, read-only) | none | during a session (needs the user's yes) |
| 1-2 | Cadence, read once | market-state | copy replay of Nov 10, CPU and byte-identical state | the open after the replay passes |
| 3 | Capacity at 3x | none | replay at 3x | with 1-2 |
| 5 | Marks at the cutoff | window | replay of Nov 10 windows: marks and selections vs stored | after 1-2 are live one session |
| 4 | Shards across processes | market-state, C6 layout, barrier | replay with 3 shards | only if 3 misses the target |
| V4 | 24 contracts per name | lifecycle (allocation) | dry run + one session | after 1-3 (and 5) |

More underlyings additionally need B10 (the flow policy lists exactly the 9 names) and a breadth request budget check
(~65 requests per window at 9 names, ~165 at 23), and the user's yes for the universe change.

## 5. Rehearsal method (copy, no provider)

Restore a copy on `g102-test-pg`, copy the Nov 10 Options journal file, start the changed market-state role against
the copy with the Nov 10 registration, let it consume the journal from offset 0 (or at a fixed multiple of real time),
then compare: final shard states and checkpoints byte-identical to what live stored for Nov 10; quote heads identical;
CPU per minute from `docker stats`. For Step 5, rerun Nov 10's windows on the copy and compare marks, selections and
evidence with the stored ones (expected differences only where the old run read a quote after the cutoff).

## 6. Delivery plan (agreed with the user, Nov 11)

**One package.** All current changes (r81-r84: lifecycle R16/R18/R19, window R31, R8-R17, G60, self-heal) and the V3-1
market-state loop go into one backend image for every backend service (also settles G57, the image label). Before the
first deploy: a full-stack "service start run" on a copy (runbook 6: every service on the image against a copy of
live; clean starts, same page reads), and the previous per-service images written to a rollback list. Deploy outside
session hours, on the user's go; Stock Alerts starts with fresh state (its detector version hashes its module).

**V3-1 behind a switch, reversible mid-session.** The new market-state loop (Steps 1-2) ships in the same package behind
`REALTIME_V3_MARKET_STATE_LOOP` on the market-state service: `P8` (today's loop, the default) or `V3`. Both loops write
the same shard states, checkpoints, quote heads and resident completions, so either can resume from the other's
checkpoints. Fallback during a live run: set the switch back to `P8` and recreate only the market-state container
(about a minute; no image change). A whole-image fallback to the rollback list stays the last resort (whole-platform
rollback only on the user's say-so). The old loop is removed (C9) only after the V3 loop has run clean for a few
sessions. Reversibility by step:

| Step | Fallback in a live run |
|---|---|
| 1-2 loop (market-state) | yes: switch back, ~1 minute, resumes from the same checkpoints |
| 5 marks at the cutoff (window) | yes: switch back; the next window runs the old way |
| 4 shards across processes | no: the shard layout is fixed for the session at activation; only from the next session |

**Sequence.**

| When | What |
|---|---|
| Nov 11 session | Today's fixes (r82, on the user's deploy); Step 0 profiles at ~14:49, 15:43, 17:57, 20:47 UTC |
| Before / after the Nov 11 close | Build the package with the V3 loop behind its switch; offline equivalence replay of the Nov 10 journal (old and new read paths give identical shard states); after the close, the full-stack copy rehearsal |
| Before the next open (user's go) | Deploy the package to every backend service, market-state switch on `P8`; run one session |
| The session after | Switch market-state to `V3`, fallback ready; acceptance: CPU under target, window completion on time, identical checkpoints |
| Then | Step 5 (marks at the cutoff) behind its own switch; then V4 depth / more underlyings once capacity is measured (with B10 and the user's universe yes) |

**Status (Nov 11, 08:10 UTC).** Package r85 (`74e2c255d715`) built with the V3 loop behind its switch, ahead of the
plan: the Nov 10 replay reproduces live's final checkpoint on both loops' read paths, and the full-stack copy rehearsal
ran clean (19 services, market-state on V3, 153 routes identical to live). Not deployed; next: the Step 0 profiles in
the Nov 11 session, then the package deploy before the next open on the user's go (switch on P8).

**User's choice (Nov 11, ~08:20 UTC): both loops in the Nov 11 session (one session faster).**

| Time (UTC) | Step | Who |
|---|---|---|
| before 13:30 | `deploy_candidate.ps1 -Step preflight`; `-Step g102-services -Image r85` (every backend service; frontend stays f26); market-state on P8 (no variable set) | user (Claude's deploys are refused by the permission check); rollback list `rollout/pre-r85-images-20261111.txt` saved by Claude |
| 13:31 | pre-open dry run on the live images (now r85) | scheduled |
| 14:49, 15:43 | Step 0 profiles #1-#2: the P8 loop | scheduled |
| ~16:55 | back up `rollout/compose.stable.json`, add `REALTIME_V3_MARKET_STATE_LOOP=V3` to market-state (never before the morning deploy, or market-state would start on V3) | Claude |
| ~17:00 | `-Step hotfix -Service realtime-v3-options-p8-market-state -Image r85` (same image, recreates the one container on V3; the tool waits for 3:00-4:15 of the cycle) | user |
| 17:57, 20:47 | Step 0 profiles #3-#4: the V3 loop (same-day comparison with #1-#2) | scheduled |

Fallback layers: V3 misbehaves -> remove the variable and recreate market-state (P8, ~1-2 min; at most ~10 minutes of
fixing first); r85 misbehaves in a service (not the loop) -> hotfix that service to r84 (r85 minus the loop and the
reader change); worst case -> the rollback list, service by service (whole-platform only on the user's say-so).

**Evidence needed.** Building and checking correctness needs no live run: the Nov 10 journal is a full real session
and a replay shows identical states. The live profiles (Step 0) decide the order of steps, because only real-time pacing
shows the per-iteration overhead (a replay reads 10,000 records per pass and hides it). Acceptance needs one live
session on the V3 loop with the switch ready.

## 7. Risks

- The barrier and checkpoint formats are shared with the window and flow roles; Steps 1-2 keep them unchanged.
- A longer cadence delays window completion by up to the timer (1 s); the window deadline is 300 s.
- Step 4 changes the shard layout and the journal read path; it is the only step with format changes, hence last and
  only if measured necessary.
