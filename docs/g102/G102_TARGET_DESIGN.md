# G102 target design and invariants

Normative. This document says what the platform must do and why; it carries no status. Whether a part is designed,
built, verified or deferred is in `G102_IMPLEMENTATION_MATRIX.md`; how it is rolled out and back is in
`G102_RUNBOOK.md`; what was measured, and when, is in `G102_EVIDENCE.md`. Parent plan:
`docs/REALTIME_SCHEMA_VERSIONING_PLAN.md`. Split from `G102_DETAILED_DESIGN.md` on Nov 10, 2026.

**Context (user, Nov 7, 2026).** The platform is in development. G102 is applied in place on live: no separate
environment, no cutover, no dual write or shadow read. Changes go out between sessions and are tested in the next one.

## 1. Needs

| Need | Met by |
| --- | --- |
| Point in time: an alert or value uses only data known at its cutoff | Every bar revision carries its known-by clocks; reads take a cutoff (INV-3) |
| Session lists (alerts with entry, current price, P/L) | Alerts as lasting rows in `rt.alert_current`, one per session, detector and contract (section 3) |
| Consistent pages | A release pointer per product; later one pointer per page type naming one consistent set of member revisions (A5, B1) |
| History and session selector | Bars keyed by instrument and time, not by cohort (section 4) |
| Audit: what a page showed and from which inputs | Releases, snapshots, receipts; per-member outcomes recorded (section 6) |
| Stable URLs | Date and ticker only (A5) |
| Onboard tickers without re-preparing everyone | Readiness carried over; a new revision supersedes a never-activated preparation (section 5) |
| Correct data | A corrected bar revision moves the head; pages follow (section 4) |
| One bad ticker never fails the others | Per-member outcomes (section 6) |

**Out of scope:** Options on the P8 path built for two underlyings (C13); Options coverage depth (G75 V4, needs the
market-state process split); detector model quality (G101); stream reliability (G41, G60); the close's wait for daily
bars (G103).

## 2. Invariants

Each invariant is testable; the matrix lists its status and test.

- **INV-1 One current bar per key.** `rt.bar_head` holds exactly one row per (instrument, interval, bar start, session
  scope, adjustment basis), whatever cohort or writer produced it.
- **INV-2 Deterministic winner, independent of arrival order.** The head is the first revision of its key under the
  winner tuple of `bar-selection-v1` (4.3). Any order of writes, including a replay of a losing revision, yields the
  same head. The live upsert, the backfill, the as-of fallback and any correction tool use one shared definition of
  the tuple.
- **INV-3 Point in time.** A read at cutoff T returns, per key, the winner among revisions known by T under one
  known-by clock contract: `observed_at`, the write time (`created_at`), `replay_available_at` and
  `provider_published_at` must all be at or before T. A key whose head became known after T is read as it stood at T,
  never dropped.
- **INV-4 One bar repository.** Only `equity_bar_repository.py` names `rt.bar_head`, `rt.bar_current`,
  `rt.bar_revisions` or `rt.bar_catalog` in SQL. A test fails on any other module (no allowlist once A4 is complete).
- **INV-5 Production bars only.** No test, replay, validation or rehearsal cohort writes bars to the live database;
  the writer refuses one.
- **INV-6 Revisions are evidence.** A correction adds a revision; nothing rewrites or deletes a revision except
  retention, and retention never leaves a key without its next-best revision.
- **INV-7 One member never fails the others.** A member's unavailable input yields that member's outcome
  (UNAVAILABLE, DEGRADED) with stable reason codes; every other member publishes. Only programming or contract errors
  fail a stage.
- **INV-8 Pages are served as published.** A later change to an input (reference revision, corporate action, revised
  bar) is labelled on read, not refused; integrity failures (a hash or identity that does not match at the
  publication's own cutoff) still refuse.
- **INV-9 Fallbacks are visible.** An optional read that fails serves its fallback and says so in the response
  (status, reason code, serving source) and in the log with the error; it never hides an outage or a programming error.
- **INV-10 Session state supersedes, never deletes.** A preparation that was never activated is superseded by a newer
  revision for its date; an activated or READY one holds the date. Concurrent writers serialize on the date.

## 3. A0: Options Alerts as a session list (G100)

- One `rt.alert_current` row per (session, detector, contract): `alert_id =
  uuid5(OPTIONS_SESSION_ALERT_NAMESPACE, f"{session_date}|{detector_id}|{contract_id}")`, so a rerun writes the same
  row. Written by the Options window in the same transaction as the ALERTS snapshot.
- The row keeps the first trigger (entry, `first_selected_at`, anchor `event_id`) and the set of distinct trades
  (`event_ids`) and evaluations; `hit_count = len(event_ids)`, `repeat_count = hit_count - 1`, `last_seen_at` the
  latest. Counting sets keeps a rerun and overlapping windows idempotent.
- The anchor event is the earliest trade no other alert holds (`rt.alert_current.event_id` is unique).
- v1 single-contract packages are recognised by their candidate identity
  (`uuid5(NAMESPACE_URL, f"stockscreener:options-alert-v1:{cohort}:{event}")`), and carry the session's valuation
  policy (user, Nov 9).
- Read: session rows first, then release rows they do not cover; first triggered by the release cutoff; `lifecycle`
  CLOSED once the cutoff reaches `valid_until`. A failed session read serves the release rows and says so (INV-9).
- No schema change.

## 4. A4: bars keyed without the cohort

### 4.1 Schema

`rt.bar_head` (migration `051_g102_bar_head.sql`), primary key (instrument_id, interval, bar_start, session_scope,
adjustment_basis); the bar's values copied in (OHLCV, `is_final`, payload hash, `source_kind`, `provider_tier`,
`observed_at`), `selection_policy`, `written_by_cohort_id` (provenance only), `updated_at`. `ON DELETE CASCADE` from
`rt.bar_catalog`. Index `idx_rt_bar_revision_key_end (instrument_id, interval, bar_end DESC)` on the revisions serves
point-in-time reads. The revision stays the source of truth.

### 4.2 Source precedence (`_current_source_precedence`)

5: ADJUSTED 1d NATIVE_REST; ADJUSTED 1wk/1mo DERIVED; RAW 1m/5m NATIVE_REST; RAW 15m/30m/1h/1d DERIVED.
4: RECONCILED. 3: other NATIVE_REST. 2: other DERIVED. 1: everything else (REALTIME_STREAM).

### 4.3 Selection policy `bar-selection-v1` (the winner tuple)

For one key, the winner is the first revision in this order:

1. source precedence, higher first;
2. `observed_at`, later first;
3. provider tier, MASSIVE_ADVANCED (2) over MASSIVE_DEVELOPER (1), only between equal observations;
4. the revision's `created_at`, later first;
5. `bar_revision_id`, ascending.

Precedence comes before the tier: putting the tier first let Advanced weekly bars displace the derived weekly bars the
precedence prefers (evidence Nov 7). The upsert replaces a head only when the incoming revision beats it on the whole
tuple and has the same `bar_end`; a revision never replaces itself. One SQL definition of the tuple serves the upsert,
the backfill, the as-of fallback and correction tools (INV-2).

### 4.4 Writer

`persist_realtime_equity_bars_cursor` inserts `rt.bar_catalog` and `rt.bar_revisions` and upserts `rt.bar_head` in one
transaction. It writes no other bar table. One cohort and tier per batch (provenance). It refuses non-production
cohorts (INV-5).

### 4.5 Read operations (in `equity_bar_repository.py`, the only module that queries bars)

| Operation | Purpose |
| --- | --- |
| range as of T | bars of a range per instrument, interval, basis, scope (charts, screening, history, window inputs) |
| latest as of T | the newest bar before T (spots, detectors, quotes) |
| lineage heads | the current revision of named keys (publication checks) |
| stored coordinates | which minutes or bars exist (gap repair) |
| coverage counts | bars per session against the calendar (preparation, close coverage, finalizer, derivation) |

Every operation takes the cutoff and applies the known-by contract of INV-3; a key corrected after T is read from the
revisions as it stood at T (same tuple, 4.3).

### 4.6 Retired

`rt.bar_current` (unwritten since Nov 8; dropped after the scripts move and the A4 rollback is no longer wanted) and
history bindings (`rt.cohort_history_bindings`): provenance moves into the window attempt and configuration manifest;
nothing creates or resolves binding chains.

### 4.7 Manual correction

A correction is a new revision through the writer (never a direct head update): it wins only on the tuple of 4.3 with
the same `bar_end`; afterwards the head is read back and compared (procedure in the runbook).

## 5. A1: onboarding, statistics and the bridge query

- **G90 supersede.** `UNIQUE(session_date)` on `rt.advanced_options_preparation_sessions` is a unique index on
  `session_date WHERE expiration_state IS NULL` (migration 049). The reconcile marks another revision's never-activated,
  not-READY preparation for the date SUPERSEDED (`SUPERSEDED_BY_REVISION:<new>`) and blocks its open work; a READY or
  activated preparation holds the date (`WAITING_DESIRED`, `OPTIONS_DATE_HELD_BY_REVISION`). Writers take a
  session-date advisory lock and retry a serialization conflict a bounded number of times with a status (INV-10).
- **G91 carry readiness.** A member whose five stages are COMPLETE on identical targets in an earlier revision with
  the same source cohort and effective session is READY at once (origin in `source_lineage.carried_from`); PENDING work
  resumes the earlier progress per stage; FAILED and BLOCKED start again.
- **G99 statistics.** The retention service analyzes the watched bar tables every 300 s and before activation, sets
  per-partition autoanalyze thresholds, and alarms when a watched table's statistics predate activation.
- **B24 bridge check.** Partial index `idx_rt_bar_revision_journal_offset (journal_generation, stream_session_id,
  source_offset) WHERE stream_session_id IS NOT NULL` (migration 050).
- **G94 split lookback.** Preparation and the nightly rollover request corporate actions from the first retained
  adjusted session of the target (the screening range), never the adapter's 180-day default. Dividend coverage stays
  valid through the session's close and its recovery (open + 9 h).

## 6. A2: per-member outcomes

- A member's unavailable input raises a typed `MemberUnavailable(reason_codes, details)`; other exceptions are errors
  (INV-7). Projection loops (screening, ticker pages, strategies, Stock Alerts, Options lanes) catch only
  `MemberUnavailable`.
- Outcomes are rows: `rt.window_member_outcomes (attempt, stage_id, member_key, outcome, reason_codes, details,
  recorded_at)`.
- The job barrier stays open while members are pending; at the deadline it is COMPLETE (all completed), PARTIAL (at
  least one completed, some failed or missed) or FAILED (none completed). Callers treat PARTIAL like COMPLETE and pass
  the failed members on as labels.
- A failed member keeps its last good entity row, labelled STALE or DEGRADED with the failed attempt and the
  last-success cutoff.

### 6.1 Read labels instead of refusals (INV-8)

Charts draw the bars current now over the published range; Screener hourly details return stored values with
`lineage_status` (VERIFIED, CHANGED, STORED, UNAVAILABLE) and a lineage object always present; Dashboard and Stock
Alerts serve with reference and action statuses and section warnings. The `current` read mode used by audits keeps its
strict checks.

## 7. Later steps (each detailed in this document before it is built)

- **A2 rest:** window attempts that supersede (lineage columns on `rt.window_runs`, each attempt's member outcomes);
  ingress plan revisions (an audited reopen; `UNIQUE (authorization_id, session_date)` becomes unique per revision).
- **A3:** detector activations, policy versions and a configuration generation, resolved once per window and
  recorded on the window run; Equity first, Options after the P8-or-V3 path settles.
- **A5:** `rt.portal_page_current (product, page_type, session_date)` with a generation; the API resolves date and
  ticker; no cohort in URLs.
- **A6 (optional):** universe revisions effective at a window boundary.
- **B0:** preparation speed and the provider budget at 400 tickers.
- **B1/B2 (fan-out and warm restart).** Before code, this section must specify: entity revision and current tables for
  ticker pages, Screener rows, strategies and alerts; generation and attempt identity on every entity row; how one page
  pointer names one consistent set of member revisions; claim size, worker count, database pool ceiling, queue-age
  deadline and retry ceiling; how global products (ranking) are computed after per-member work; durable warm state keyed
  by exact input hashes; restart and old/new worker overlap fencing; publication when jobs finish after the barrier
  deadline. `TickerProjectionState` is in-process and not thread-safe, so it cannot be the shared state.
