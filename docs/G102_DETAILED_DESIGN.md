# G102 detailed design

Status: draft, Nov 7 2026 (UTC). Detailed design for `REALTIME_SCHEMA_VERSIONING_PLAN.md`. Sections 3 to 6 (the Monday
scope) are detailed down to tables, functions and files. Section 7 outlines the later steps, which get the same detail
before they are built.

**Context (user, Nov 7):** the platform is in development stage. G102 is applied in place on live: no separate
environment, no cutover, no dual-write or shadow-read ceremony. Build over the weekend, start testing on Monday Nov 9,
then catch up to the Nov 6 level of function. Several alternate designs were built before (P7, the P8 canary, the v3
broad stream), so section 1 checks that this one meets the needs before any code is written.

## 1. Needs check

Each need, the part of the design that meets it, and how it is proven. "Replay" means the recorded Nov 5 and Nov 6
sessions run through the changed code on a copy of live with the real 25 tickers.

### 1.1 Portal requirements

| Need | Met by | Proven by |
| --- | --- | --- |
| Point-in-time values: an alert or value uses only data known at its cutoff | `observed_at` on every bar revision (exists); the window cutoff applied on read; reruns read revisions as of the cutoff (4.6) | Replay: no read row uses a bar observed after its cutoff |
| Session lists (alerts with entry, current price, P/L) | Alerts as lasting rows in `rt.alert_current`, one per session, detector and contract (section 3); current price at read time (exists, v1) | Replay of Nov 6: one row per contract for the whole session, not just the last two windows |
| Consistent pages | The release pointer (exists); later one pointer per page type (A5) | Validator on Monday's pages |
| History and session selector | Bars keyed by instrument and time; no cohort history bindings (section 4) | Counts per ticker, interval and session against the trading calendar |
| Audit: what a page showed and from which inputs | Releases, snapshots and receipts (exist); per-ticker outcomes recorded (section 6) | A page row names its window, cutoff and outcome |
| Stable URLs | Date and ticker only (A5, G70) | Later step |

### 1.2 Change types

| Change | Today | Met by | Proven by |
| --- | --- | --- | --- |
| Onboard tickers | Re-prepares every new member on each list change (G91); a change after Options opens crash-loops (G90); history stitched across cohorts | Readiness carry-over and supersede (section 5); no history bindings (section 4) | Copy: a revision adding tickers prepares only new members; Options revision over an open preparation |
| Add or update a detector | Closed enum, multi-service release (C14) | Detector activations (A3) | Later step |
| Change an Options policy | Frozen for the session (C1) | Policy versions (A3) | Later step |
| Code fix and rerun | Retry "diverges" (B17); terminal states need manual SQL (B2) | Window attempts that supersede (A2, later part) | Later step |
| Correct data | Manual SQL | A corrected bar revision moves the head (section 4) | Copy: insert a corrected revision, the head and the page follow |
| One bad ticker | Fails the whole window (B4, B5, B18) | Per-ticker outcomes (section 6) | Copy: one ticker without split evidence; the other 24 publish |

### 1.3 Measured failures (Nov 5-6) and where each is fixed

| Failure | Fixed in |
| --- | --- |
| Stale statistics, 42 s query, controller loop (B23, G99) | Section 4 (stable keys) and 5.3 (statistics step) |
| Bridge `MAX(source_offset)`, 32% of database time (B24) | 5.4 |
| History loader join, 18% of database time | Section 4 (single-table reads) |
| Options alerts leave the page (G100) | Section 3 |
| Re-preparation on every list change (G91) | 5.2 |
| Options lifecycle crash-loop on a list change (G90, G104) | 5.1 |
| One ticker fails the window (B4) | Section 6 |
| Lineage 503 after a bar is revised (G36, G83) | Section 4 shrinks the cause (one current bar per key); the read-side relaxation is A2's later part |

### 1.4 What this design does not fix

- Options running on the P8 canary path built for two underlyings (C13).
- Options coverage breadth (G75, C16).
- Detector model quality, such as same-day contracts (G101).
- Stream reliability (G41, G60) and the close's wait for daily bars (G103).

**Where time could be wasted:** fitting Options policy versions into the P8 path (A3). If the Options path later moves
from P8 to the v3 design, that part is thrown away. Decide which Options path stays before A3's Options part; the
Equity part of A3 doesn't depend on it.

**Gate:** you review this section before building past the Monday scope.

### 1.5 Code footprint of the live services (measured Nov 7)

Static import closure from the 21 entry points in `docker-compose.realtime-v2.yml` (every import statement, including
those inside functions): `realtime` 248 of 284 modules (162,734 of 175,189 lines), `options` 88 of 102, `equity` 41
of 57, `scripts` 96 of 318. The design generations are woven into the live path (modules named P7: 1, P8: 8, v3: 7,
canary: 2 are reachable), so a clean folder made by copying only the needed files would drop clutter (35 realtime
modules such as the developer cycle, bootstrap, P7 executor and shadow, local mock; about 220 scripts) but not the
mixed designs. Separating those takes code changes: the P8-or-v3 decision (1.4) and G102. Caveats: 7 reachable
modules import dynamically, and some tools run outside the services (for example
`canonical_stock_alert_regeneration.py` through `deploy_candidate.ps1`), so pruning needs the test suite and a role
start check as gates.

## 2. Approach

- **In place:** one migration per step in `backend/realtime/sql_v2/` (049 onwards), applied to live over the weekend,
  when no session runs. Index builds don't need `CONCURRENTLY` then; `lock_timeout` stays 5 s as in migration 021.
- **Old tables stay** (`rt.bar_current`, `rt.cohort_history_bindings`) until Monday's test passes. The bar writer keeps
  updating `rt.bar_current` for one week. That is not a shadow process: it keeps the 18 scripts that still read the old
  table correct, and keeps rollback to the current images a plain image swap.
- **Verification (instead of a proof step):** counts against the trading calendar, and one query listing keys where
  the new head picked a different bar revision than today's reads (4.7).
- **Testing:** unit tests for each changed module; a replay of the Nov 6 session on a copy with the 25 tickers; then
  Monday's live session with the validator after the close.
- **Rollback:** the current images (window r50, API r48 and the others) with the old tables, which stay current.

## 3. A0: Options Alerts as a session list (G100)

**Today.** `_build_advanced_options_page_payloads` (`advanced_options_projection.py:1582`) builds the ALERTS page's
`alert_rows` from the current cycle's evaluations only (`run_rows`, line 1762, grouped per contract by
`_one_alert_per_contract`). Each window evaluates the last 10 minutes of trades, so an alert drops off after about two
windows. The API (`RealtimeOptionsAlertsPageRepository`, `portal_compatibility_repository.py:731`) serves the latest
release's rows (`detector_runs`, line 969) and adds current prices (`_advanced_current_marks`, line 742).

**Existing pieces it uses.** Every Options evaluation already gets a `rt.detector_events` row
(`_persist_native_detector_events_cursor`, `advanced_options_cycle.py:675`, called at line 867).
`rt.alert_current` accepts `alert_kind = 'OPTION'`, its `event_id` references `rt.detector_events`, and the index
`idx_rt_alert_current_kind_time (cohort_id, alert_kind, as_of DESC, alert_id)` serves a session list.

**Schema change:** none.

**Write** (Options window, `persist_advanced_options_projection_cursor`, line 3041, same transaction as the ALERTS
snapshot). For each `alert_rows` entry with a v1 single-contract package:

- `alert_id` = `uuid5(OPTIONS_SESSION_ALERT_NAMESPACE, f"{session_date}|{detector_id}|{contract_id}")`, so a rerun
  writes the same row.
- `_alert_run_row` adds `event_id` (from the evaluation) to its row, so the alert can point at its first trigger.
- Read the existing row `FOR UPDATE`. If none: insert with `cohort_id` (the session's), `event_id` of the first
  trigger, `lifecycle = 'OPEN'`, `as_of` = trigger time, `valid_until` = exit deadline (session close),
  `projection_schema = 'options-alert-session-v1'`, `projection` = the run row plus `evaluation_ids`. If present: keep
  the first-trigger fields (entry, `first_selected_at`, `event_id`); union `evaluation_ids`; set
  `hit_count = len(evaluation_ids)`, `repeat_count = hit_count - 1`, `last_seen_at` = the latest; `as_of` = the latest.
  Counting a set instead of incrementing keeps a rerun idempotent.
- Stored multi-leg packages keep today's per-evaluation rows (unchanged).

**Read** (API, advanced path of `detector_runs`): rows from `rt.alert_current` where `alert_kind = 'OPTION'`, the
release's `cohort_id`, and `triggered_at <= cutoff`, ordered by trigger time, newest first; the same filters
`publication.detector_runs` applies (fields of `DetectorRunQuery`, to confirm); then `_advanced_current_marks` as
today. The lifecycle shows CLOSED when the cutoff is at or past `valid_until`; no writer is needed for the close.

**Files:** `advanced_options_projection.py`, `portal_compatibility_repository.py`, one new test file. Services: Options
window (p8-window) and API.

**Tests:** unit (merge rule, idempotent rerun, ordering, filters); replay of Nov 6's 28 O1 detections: 23 packages,
grouped per contract for the session (NVDA 192.5C: one row with 24 hits).

## 4. A4: bars keyed without the cohort

### 4.1 Today

- One writer: `persist_realtime_equity_bars_cursor` (`equity_bar_repository.py:40`) inserts `rt.bar_catalog`,
  `rt.bar_revisions` and upserts `rt.bar_current` keyed `(cohort_id, instrument_id, interval, bar_start,
  session_scope, adjustment_basis)`.
- Readers pick one bar per key across the current cohort and its history-binding chain:
  `DISTINCT ON (interval, adjustment_basis, bar_start) ... ORDER BY precedence DESC, observed_at DESC, created_at DESC,
  bar_revision_id` (`load_current_bar_groups_cursor`, line 346), always joined to the partitioned `rt.bar_revisions`
  for the values.
- 27 runtime modules reference `rt.bar_current`. Two are metadata (`relation_registry.py`, `security.py`); the other 25
  hold about 45 query sites. Eight walk history scopes (`equity_history.py`, `ticker_repository.py`,
  `screening_projection.py`, `advanced_options_equity_input.py`, `advanced_options_reference_producer.py`,
  `advanced_options_p8_production.py`, `advanced_options_v3_history.py`, `equity_bar_repository.py`). 18 scripts read
  it too.

### 4.2 Schema (migration `049_g102_bar_head.sql`)

```sql
CREATE TABLE rt.bar_head (
    instrument_id bigint NOT NULL REFERENCES rt.instruments(instrument_id),
    interval varchar(8) NOT NULL,
    bar_start timestamptz NOT NULL,
    session_scope varchar(16) NOT NULL,
    adjustment_basis varchar(24) NOT NULL,
    bar_revision_id uuid NOT NULL REFERENCES rt.bar_catalog(bar_revision_id) ON DELETE CASCADE,
    provider_tier varchar(24) NOT NULL,
    source_kind varchar(24) NOT NULL,
    session_date date NOT NULL,
    bar_end timestamptz NOT NULL,
    observed_at timestamptz NOT NULL,
    is_final boolean NOT NULL,
    open_price numeric(20,8) NOT NULL,
    high_price numeric(20,8) NOT NULL,
    low_price numeric(20,8) NOT NULL,
    close_price numeric(20,8) NOT NULL,
    volume numeric(28,8) NOT NULL,
    vwap numeric(20,8),
    transaction_count bigint,
    payload_sha256 char(64) NOT NULL,
    selection_policy varchar(32) NOT NULL,
    written_by_cohort_id uuid,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (instrument_id, interval, bar_start, session_scope, adjustment_basis)
    -- plus the CHECKs of rt.bar_current and rt.bar_revisions for these columns
) WITH (fillfactor = 80, autovacuum_analyze_scale_factor = 0, autovacuum_analyze_threshold = 5000);

CREATE INDEX idx_rt_bar_head_session ON rt.bar_head (session_date, interval, instrument_id);
CREATE INDEX idx_rt_bar_revision_key_end ON rt.bar_revisions (instrument_id, interval, bar_end DESC);
```

- **The values are copied into the head** (OHLCV, `is_final`, payload hash). Most reads become one table instead of a
  join with the partitioned revisions (the history loader's join was 18% of database time). The revision stays the
  source of truth; the head changes only when a new revision wins. `source_revision_ids` is still read by joining
  the revision, as today (`with_sources`).
- `written_by_cohort_id` is provenance only.
- `ON DELETE CASCADE` follows the catalog, as `rt.bar_current` does, so retention keeps working.

### 4.3 Selection policy `bar-selection-v1`

Order for one key: the active provider tier first, then today's source precedence (`_current_source_precedence`),
then the later `observed_at`, then the later `created_at`, then `bar_revision_id`. This is today's read order with
the tier made explicit; today the tier is implied by which cohort a read starts from. The difference query (4.7) shows
where it changes a pick.

### 4.4 Writer

`persist_realtime_equity_bars_cursor` adds an upsert into `rt.bar_head` in the same transaction:
`ON CONFLICT (instrument_id, interval, bar_start, session_scope, adjustment_basis) DO UPDATE ... WHERE` the new row
wins by 4.3 and `bar_end` matches (as today). It keeps the `rt.bar_current` upsert for one week (section 2). The
"one cohort and tier per batch" check stays; it only concerns provenance now.

### 4.5 Read functions (in `equity_bar_repository.py`)

| Function | Replaces | Callers |
| --- | --- | --- |
| `load_bar_range_cursor(cursor, instrument_ids, interval, basis, start, end, scope='RTH', final_only=True)` | `load_current_bar_groups_cursor`, history-scope range reads | `equity_history`, `ticker_repository`, `screening_projection`, Options equity input, reference producer, P8 production, v3 history |
| `load_bar_heads_cursor(cursor, targets, with_sources=False)` | `load_current_bar_heads_cursor` (same signature without cohort and tier) | lineage recomputation (`lineage_head.py`, publication checks) |
| `load_latest_bars_cursor(cursor, instrument_ids, interval, basis, before)` | "latest bar" reads | detectors, portal projection worker, session close coverage |
| `load_stored_minutes_cursor(cursor, instrument_id, session_date)` | gap-repair stored-minute checks (B23's query) | `advanced_gap_repair`, `advanced_stream_gap_repair`, `gap_repair_plan`, `equity_recent_repair` |
| `count_bars_by_session_cursor(cursor, instrument_ids, interval, sessions)` | coverage counts | preparation, `session_close_coverage`, finalizer, derivation worker |

Each caller drops its cohort and history-scope arguments. A test fails if any runtime module other than
`equity_bar_repository.py` names `rt.bar_head`, `rt.bar_current` or `rt.bar_revisions` in SQL (the read boundary).
The 18 scripts stay on `rt.bar_current` for now and move later.

### 4.6 Point-in-time reads for reruns

`load_bar_range_cursor(..., as_of=None)`: live windows pass no `as_of` and read the head. A rerun passes its cutoff;
if any head row in the range has `observed_at > as_of`, that key is read from `rt.bar_revisions` (latest revision with
`observed_at <= as_of`, same order as 4.3). Using the new revision index, this costs one indexed lookup per key, and
only for keys corrected after the cutoff.

### 4.7 Backfill and verification (`runner/g102_bar_head_backfill.py`, after the migration)

1. Fill `rt.bar_head` from `rt.bar_current` joined to `rt.bar_revisions`, one statement per instrument:
   `INSERT ... SELECT DISTINCT ON (key) ... ORDER BY key, <4.3 order>` with
   `ON CONFLICT DO NOTHING`. Idempotent, resumable per instrument.
2. **Counts:** per ticker, interval, basis and session, compare with the trading calendar (1m 390, 5m 78, 15m 26,
   30m 13, 1h 7 per regular session; one 1d bar per session; weekly and monthly per period). List shortfalls.
   Runs before Monday, inside the backfill (minutes): a missing daily bar fails screening's exact-283-session check
   for that ticker (`screening_projection.py:298`).
3. **Differences (can wait until Monday after the close or Tuesday; user, Nov 7):** for each active ticker, run today's history read (`load_current_bar_groups_cursor` with the
   current cohort's history scopes) and the new `load_bar_range_cursor` over the same ranges; list keys where the
   revision id differs. A different pick changes a publication's bar lineage, so pages built on it return
   "lineage changed; republish required" until republished. Review the list before the switch; most should be
   expected (for example, a REST repair bar over a stream bar).
4. Rehearse 1-3 on a copy first, then run on live.

### 4.8 What is retired

History-binding reads (`equity_history._history_scope_cursor`, `_history_binding_scope_cursor`,
`resolve_equity_history_scopes_cursor` and the scope arguments in callers). The table `rt.cohort_history_bindings`
stays, unread, until Monday passes; `scripts/bind_realtime_v2_advanced_history.py` is no longer run at activation.

### 4.9 Services and tests

Rebuilt with the new code: bridge, finalizer, preparation, Equity scheduler, detectors, stock alerts, Options roles
that read Equity bars (window, flow, lifecycle), API. Tests: the existing suites of the 25 converted modules; new
tests for the 5 read functions and the writer's head upsert; the read-boundary test; the replay of Nov 6 on a copy.

## 5. A1: onboarding, statistics and the bridge query

### 5.1 G90: a revision supersedes an open Options preparation

**Today:** `_upsert_session` (`advanced_options_lifecycle.py:368`) resolves conflicts on `revision_id`, but
`rt.advanced_options_preparation_sessions` also has `UNIQUE(session_date)`. A new revision for a date that already
has a preparation raises `UniqueViolation` on every reconcile (162 restarts on Nov 6). The Nov 6 workaround deleted
the stale preparation by hand (`runner/g90_clear_stale_options_prep.sql`).

**Change (migration `050_g102_options_preparation_supersede.sql`):** add a `SUPERSEDED` state with
`superseded_by_revision_id` and `superseded_at`; replace `UNIQUE(session_date)` with a partial unique index
`WHERE state <> 'SUPERSEDED'`. In `_upsert_session`, when the date's preparation belongs to an older revision and was
never activated (not READY or ACTIVE), mark it `SUPERSEDED` and insert the new one in the same transaction. An
activated preparation is never superseded mid-session; the new revision then waits for the next date. The guards are
the same as the Nov 6 script's, but nothing is deleted.

**Monday use:** this is what lets the Options-on-23 revision replace the Nov 9 preparation that opened with 9.

### 5.2 G91: prepare only what changed

**Today:** `_persist_equity_preparation_work_cursor` (`advanced_realtime_desired_state_repository.py`) creates work
per (revision, intent) for every member outside the source cohort, and readiness is per revision, so each list change
re-runs all 5 stages for every new member (about 450 provider requests and 1.5 hours on Nov 6).

**Change (code only):** before creating work for a member, look for READY intent readiness of an earlier revision
with the same security, member kind and identical stage target (`_equity_preparation_targets`: effective session and
history policy). If found, insert the new revision's readiness as READY with reason `CARRIED_FROM:<revision>` and
create no work. In-progress work for an identical target is reused the same way. Prepare only members that are new or
whose target changed.

### 5.3 G99: statistics step in the retention worker

**Today:** `scripts/run_realtime_v2_retention_service.py` runs hourly (`DEFAULT_INTERVAL_SECONDS = 3600`).

**Change:** a statistics check every 300 s inside the same service (its admin connection and leadership lock):

- Watch list: `rt.bar_head`, `rt.bar_catalog`, the current `rt.bar_revisions` partition and its parent, and
  `rt.bar_current` while it is still written.
- ANALYZE a table when its statistics predate the session's activation or more than 5,000 rows changed since the last
  analyze (`pg_stat_user_tables`); ANALYZE partitioned parents after the close.
- Apply `autovacuum_analyze_scale_factor = 0, autovacuum_analyze_threshold = 5000` to each new monthly partition it
  creates.
- Write last-analyzed times to the status file, with an alarm when a watched table's statistics predate activation.

### 5.4 B24: the bridge's journal check

**Today:** `advanced_journal_replay.py:304` runs `SELECT COALESCE(MAX(source_offset),0)` on `rt.bar_revisions` every
tick with no index and no partition bound (32% of database time, up to 2.8 s per call).

**Change:** partial index (in migration 049)
`CREATE INDEX idx_rt_bar_revision_journal_offset ON rt.bar_revisions (journal_generation, stream_session_id,
source_offset) WHERE stream_session_id IS NOT NULL`, and the query bounded by `observed_at >=` the stream session's
start, so it reads only the current partition.

## 6. A2, Monday part: per-ticker outcomes

**Today:** one ticker's bad input raises inside a loop over tickers and fails the whole stage, for example
`Screening split/action evidence unavailable` (`screening_projection.py:313`, B4). The job barrier returns `FAILED` as
soon as any member failed (`materialization_jobs.py:778`).

**Change:**

- **Projection loops** (screening, ticker projection, stock alert evaluation): a per-ticker `ValueError` is caught,
  recorded as that ticker's outcome (`DEGRADED`, with the message as reason code), and the loop continues. The ticker's
  page rows carry a label and its last good values; the other tickers publish.
- **Barrier verdict:** `PARTIAL` when no member is pending, at least one completed and some failed; `FAILED` only
  when none completed. Callers treat `PARTIAL` like `COMPLETE` and pass the failed members on as labels.
- **Record (migration `051_g102_member_outcomes.sql`):** `rt.window_member_outcomes (window_id, stage_id, member_key,
  outcome, reason_codes, recorded_at)`, primary key `(window_id, stage_id, member_key)`; the `rt.job_barriers` state
  check gains `PARTIAL`.
- **Monitoring:** the failure watcher reports DEGRADED tickers per window instead of a failed window.

**Files:** `screening_projection.py`, `ticker_projection.py`, the stock alert evaluation loop,
`materialization_jobs.py` and its 10 barrier callers (to list). **Tests:** a window with one ticker missing split
evidence: 24 publish, one is labelled; the barrier reads `PARTIAL`.

**Built, v1 (Nov 7), simpler than the plan above, using the legacy pattern:**

- `screening_projection.produce_realtime_screening`: a member whose inputs fail a check (`ValueError`) keeps a
  full-shape placeholder row built by `project_security` for an unavailable source member (ineligible, every market
  value missing with `SOURCE_PUBLICATION_MEMBER_UNAVAILABLE`, quality `MEMBER_UNAVAILABLE`), is listed in the
  manifest's existing `source_unavailable_members` with the reason, and has no bar lineage. The row count still
  equals the expected members, so every downstream count check holds. With no member left, it still fails.
- `ticker_projection.project_realtime_ticker_workspace`: a member screening marked unavailable, or whose workspace
  raises a `ValueError`, gets no workspace this window (its partial writes are rolled back) and is recorded in the
  new `unavailable_members` field with stage and reason. The summary accepts a subset of members and lists the
  others in `unavailable_members`.
- `dashboard_projection`: unavailable members are left out of every list (overview, scanners, setups through the
  existing `excluded_tickers`, context, references) and listed in `unavailable_members`.
- `portal_compatibility_repository.hourly_details`: a member without lineage returns its row with no lineage.
- No schema change and no barrier change in v1. A skipped ticker has no page in that window (showing its previous
  page is v2). Strategies and stock alerts keep their own per-member loops (v2).
- **Tests:** 19 fail-closed tests rewritten to the new rule, 2 new; the 17 affected suites show only the 85
  pre-existing environment failures (Starlette test client), same as the untouched baseline.
- **Read-only replay on live inputs** (`runner/g102_per_ticker_replay.py`, Nov 6 14:50 UTC, before the 14:54 G94
  coverage fill): the image's code fails with `Screening split/action evidence unavailable: AMD`, as on Nov 6; the
  patched code completes: 9 workspaces, the 16 members added Nov 6 unavailable with that reason, screening 25 rows
  (9 selected), summary 9 with 16 listed, generation check passed. Note: the generation's `status` still reads
  COMPLETE because placeholders keep the count; `source_unavailable_members` is the signal.

## 7. Later steps (outline; detailed the same way before they are built)

- **A2, rest:** window attempts that supersede (lineage columns on `rt.window_runs`); ingress plan revisions
  (`UNIQUE (authorization_id, session_date)` becomes unique per revision, C12); API read checks that label instead of
  returning 503 (G97, G36, G83).
- **A3:** `rt.detector_activations`, `rt.policy_versions`, `rt.configuration_generation`, a resolver called once per
  window, recorded on the window run. Equity detectors first. Options only after the P8-or-v3 decision (1.4). Today's
  Options detector versions are placeholders (code hash = policy hash = detector hash,
  `advanced_options_cycle.py:684-697`).
- **A5:** `rt.portal_page_current (product, page_type, session_date)` with a generation counter; the API resolves
  date and ticker; the frontend stops sending cohorts (150 mentions in 32 files).
- **A6 (optional):** universe revisions effective at a window boundary.
- **B0:** preparation speed (G89) and the provider budget at 400 tickers.
- **B1:** per-ticker work units through `rt.materialization_jobs` with several projection workers, joined on the
  barrier with per-ticker outcomes (section 6 is its base).
- **B2:** warm restarts: a restarted scheduler loads its warm state from the last window instead of rebuilding.

## 8. Weekend order and Monday test

**First, before G102 work (Monday blocker, not part of G102):** B14a. Nov 6's Options stream session `d42b2d84` is
still LIVE with no end time, and `uq_rt_active_stream_owner` allows one active session per provider and feed, so
Monday's Options start fails as Nov 6's did. Fix: the acquisition role ends its own stream session at the decision
stop, and the P8 roles stop within their bound. Fallback: close the row by hand, with the user's yes, as on Nov 6.

**Fast track (user, Nov 7; plan section 15):** the weekend builds B14a, section 6 (per-ticker outcomes), flags instead
of 503 (A2 part, detail to be written first), section 3 (A0), 5.1-5.4 and the G94 lookback, deployed Sunday night.
Migrations 050 and 051 go in this weekend; the B24 index from 049 goes in on its own; the rest of 049 and section 4
(bars without the cohort) move to Thursday-Friday. Items without detail here (B14a, flags instead of 503, G94) get a
short design note in this file before code.

**Saturday:** B14a; section 6; flags instead of 503; restore one copy of live for the weekend's tests.

**Sunday:** section 3 (A0); 5.1-5.4; G94; unit tests and read-only replays on live inputs; build images; apply the
migrations; deploy; run the pre-open data audit.

**After Monday (estimate, continuous pace; each step deployed outside session hours and tested in the next session):**

| When | Work |
| --- | --- |
| Mon Nov 9, during the session | Watch the test; fix what it finds (code and narrow tests only: heavy tests and copy rehearsals slow live windows) |
| Mon night | Deploy fixes; Options-on-23 gates (G72, CPU limits, G74 item 2); apply the Options-on-23 revision |
| Tue Nov 10 | Options on 23 live, measured under the stop rule (replaces the 11.5x copy rehearsal) |
| Wed Nov 11 | 50-ticker revision if Tuesday is clean |
| Thu-Fri | Section 4 (bars without the cohort); P8-or-v3 decision for Options |
| Nov 16-20 | A3, A2's rest (window attempts, ingress plan revisions), A5, then B0-B2; A6 only if needed |

**Monday checklist:**

- Options starts without a manual fix; at the decision stop the stream session ends and no role restarts with an
  error (B14a).
- Equity window times no worse than Friday's.
- Statistics fresh before the first window (status file); bridge journal check under 50 ms (load profile).
- Options Alerts lists the whole session's alerts, one per contract (Options on 9).
- Any ticker failure shows as DEGRADED for that ticker, not a failed window; no page returns 503 for a reference or
  lineage difference.
- After the close: the close completes; validator on the pages.
