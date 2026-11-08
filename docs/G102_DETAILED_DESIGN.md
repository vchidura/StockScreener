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

### 1.6 Benefit check and completeness audit (Nov 7, 21:00 UTC)

The user asked, before building A2, A3 and A5 for Monday: does each step fix something that broke (not only in
theory), and was anything marked done not actually done? Evidence is from live (read-only), the copy and the code.

**Remaining steps against the measured failures**

| Step | Failure it fixes | Evidence | Verdict |
| --- | --- | --- | --- |
| A2: missed work redone later | B20/G105: a MISSED window leaves a 5m hole for every member; the raw daily bar derives from 5m, so the close fails (`CLOSING_DAILY_INPUTS_PENDING`) and the completed-session recovery stops after one attempt. Nov 6 needed a manual fill (23:46) and a manually run fifth attempt (01:51 Nov 7) | G105 | Build for Monday |
| A2: ingress plan revisions | B2: a blocked plan or quarantined journal ends streaming for the day; Nov 6 needed a database override | B1's trigger is fixed; the terminal rule remains | Build, scoped to an audited reopen |
| A2: window attempts that supersede | B14b, B17: an Options window that failed once can never complete | All on P8 tables, which the P8-or-v3 decision may replace; Equity never needs a past window rerun (the next window publishes), except the close (row 1) | Defer until that decision |
| A2: member outcomes table (052) | None open: outcomes are in the page manifests since fast-track item 2 | | With A2, low urgency |
| A3 | None: the Equity cohort's configuration hash changed once in five sessions (Nov 6); model bundle and valuation never changed; all mid-session changes were code hotfixes (55 on Nov 5-7) and none needed a new cohort. Options configuration changes daily, but its A3 part waits for the P8-or-v3 decision (1.4) | live `rt.cohorts` | Defer until a configuration change needs it |
| A5 | None: `/api/realtime/portal/current` already returns the current Equity and Options cohorts and a session-date-to-cohort map for each, and the frontend uses it; A5 would take `cohort_id` out of URLs (150 mentions in 32 files) | API access log, live response | Defer: a cleanup, not a fix |

Open failures in no A-step: B3, B11, B9c (a service exits and crash-loops instead of waiting with a status); G103 (the
close waits about 40 minutes for daily bars at 25 members, longer at 50); G84 and B25 (the raw daily close is the last
trade before 16:00; Chg% mixes bases); G38 and B19 (no daily strategies producer; Strategies is 503 since Nov 4); B10
(SPY/QQQ literals, before Options on 23).

**Completeness audit**

| Check | Result |
| --- | --- |
| Live images against commits | Every file in the 11 live images equals a committed version; no fix exists only in an image |
| Committed code not in the images | G81 (`backend/options/`): the build copies `realtime`, `scripts`, `equity` and `tests` only. Its commit says live; the register says not deployed. Not in r52 either |
| Frontend | The live f17 build's 96 source files equal the committed source |
| Database | Live (with 049-051) equals a database built from the migrations alone: 1,351 statements each after normalizing how Postgres prints casts; no manual change, nothing missing |
| One-off data fixes | G92 earnings for the 10 added stocks expires Nov 9 13:07 UTC: deferred by the user (N/A; alerts unaffected) |
| Decided or planned for this weekend, not built | G75 Options coverage (user's decision Nov 6 18:50, `OPTIONS_COVERAGE_TASK.md` "for Monday"; not in the fast track); G37 Financials tab (`FINANCIALS_TAB_TASK.md`, design needs review); G84 |
| Done items wired into the services | Fast-track items 2-7 run in the live entry points (the replays used the services' code; retention runs the statistics step every 300 s by default) |

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
- **Old tables stay, unwritten** (`rt.bar_current`, `rt.cohort_history_bindings`). User, Nov 7: Monday runs the target
  design, nothing is written into old tables that don't fit it, and nothing runs in parallel. So the A4 writer updates
  only `rt.bar_head`; `rt.bar_current` keeps its Friday state for rollback and for the scripts not yet converted, and is
  dropped after a week.
- **Verification (instead of a proof step):** counts against the trading calendar, and one query listing keys where
  the new head picked a different bar revision than today's reads (4.7).
- **Testing:** unit tests for each changed module; a replay of the Nov 6 session on a copy with the 25 tickers; then
  Monday's live session with the validator after the close.
- **Rollback:** migrations 049-051 work with the earlier images (no old statement uses the constraint 049 replaces;
  050 and 051 only add). Before the A4 writer has written, rollback is an image swap. After it has written,
  `rt.bar_current` is stale, so first stop the services and run `runner/g102_a4_rollback_fill.py --since <deploy time>`
  (copies every head row changed since then into `rt.bar_current` under its catalog cohort; rehearsed Nov 7), then swap
  the images (4.10).

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

**Built (Nov 7), with three changes from the outline above:**

- **Hits count trades, not evaluations.** Windows overlap (a 10-minute lookback every 5 minutes) and an evaluation id
  includes the window's dataset, so one trade gets two evaluation ids. The row keeps `event_ids` (distinct trades)
  and `evaluation_ids`; `hit_count = len(event_ids)`. A rerun or an overlapping window with no new trade writes
  nothing.
- **The anchor event is the earliest trade no other alert holds.** `rt.alert_current.event_id` is unique, and two
  detectors can fire on one trade; the second alert anchors on its next trade, and one with no free trade stays on the
  window's own rows (logged). No schema change.
- **The read merges, it does not replace:** session rows first, then the release's rows they do not cover (stored
  multi-leg packages, or an alert that could not be stored). A failed lookup (inside a savepoint) serves the release's
  rows as before. Rows are first-triggered by the release cutoff; `lifecycle` reads CLOSED from `valid_until`.

Code: `advanced_options_projection.persist_options_session_alerts_cursor` (called by
`persist_advanced_options_projection_cursor`, same transaction as the ALERTS snapshot),
`load_options_session_alert_rows_cursor`, `AdvancedOptionsAlertsPagePublication.detector_runs(session_rows=...)`;
`RealtimeOptionsAlertsPageRepository._advanced_session_alerts`. **Tests:** `test_options_session_alerts.py` (6); the
16 Options and portal suites around it show no new failure against HEAD. **Replay on live data**
(`runner/g102_g100_replay.py`, read-only, in-memory table): Nov 6 kept 52 O1 detection evaluations on 28 trades over
17 windows (16:29-17:59 UTC; the stored ALERTS windows of that run carry no package rows, so each detection gets a v1
package on its trade's contract). The page showed 0 to 3 alerts per window (none at 17:18 and 17:24); the session list
keeps all 4 contracts from their first trigger: NVDA 192.5C one row with 24 trades (16:22-17:59), QQQ 657P 2, IWM 272C
1, AAPL 340P 1; 28 of 28 trades counted once; every anchor event is in `rt.detector_events` and unique.

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

### 4.2 Schema (migration `051_g102_bar_head.sql`)

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

Order for one key: today's source precedence (`_current_source_precedence`), then the later `observed_at`, then the
provider tier (Advanced over Developer, only between equal observations), then the later `created_at`, then
`bar_revision_id`. This is today's read order with the tier as a tie-break.

**Changed after the rehearsal (Nov 7).** The first version put the tier first. On a copy of live it changed 29 of
200 published chart groups: Advanced provider weekly bars displaced the derived weekly bars the precedence table
prefers (AAPL: 61 weekly bars, 32 with different values), and Advanced derivations displaced Developer REST 5m bars.
With the order above, all 200 groups pick exactly what they were published with (4.10).

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

### 4.10 Rehearsal on a copy of live (Nov 7) and the completion plan

**Copy:** read-only dump of live at 18:02 UTC (1.08 GB, `backend/backups/stocks_db_backup_v3-g102-a4-rehearsal-*`),
restored in 67 s into a throwaway Postgres 17 container on a private Docker network (no route to live): 2.8 GB,
472,812 `rt.bar_current` rows, 762,063 revisions, migrations through 048.

| Step | Result |
| --- | --- |
| Migrations 049-051 with `scripts/apply_realtime_migrations.py` (the live path) | Plan: only 049-051 pending, no checksum drift; apply 2 s |
| Backfill (`scripts/backfill_realtime_v2_bar_head.py`, one transaction per instrument) | 26 instruments, 472,812 cohort rows to 432,684 head rows (40,128 keys duplicated across cohorts) in 17 s; slowest instrument 0.9 s |
| Counts against the history plan (Monday: history through Nov 6) | 25 members x 9 interval groups: all match |
| Ticker charts: today's read (cohort and history chain) vs the head, over every published chart group | 200 of 200 groups verify against their published lineage under both reads (after the 4.3 change) |
| Screening: today's head verification vs the head | today 0 of 25 members verify (G83 reproduced); head 25 of 25 |
| Writer on a real database | `test_bar_head_v2_db.py` and the 19 database suites that write or read bars, run against the throwaway server |

**What remains.** Outside `equity_bar_repository.py`, 38 runtime modules (about 75 statements) name the bar tables,
23 modules carry cohort history-scope parameters (some for manifest identity, not bars), and 30 scripts read the bar
tables. `test_bar_head.py` lists the 38 modules; the list may only shrink, and no new module may query the tables.
Conversion order, each group tested and replayed on the copy before the next:

1. **Page reads** (ticker charts, screening verification, lineage heads, Stock Alerts): `load_bar_heads_cursor` /
   `load_bar_groups_cursor`. Removes G83 from the portal.
2. **Window inputs** (screening loader and delta loader, Equity history, ticker projection inputs, portal projection
   worker, Stock Alerts and detectors): `load_bar_range_cursor` with `as_of`. Gate: a replay of Nov 6 windows on the
   copy gives the same input hashes through the old and the new loaders, or every difference is explained.
3. **Coverage, repair and workers** (session-close coverage, gap repair (B23's query), recent repair, finalizer,
   derivation, preparation, backfill source, bar drain, journal check (B24's query), session recovery, stage runners).
4. **Options readers of Equity bars** (Equity input, native loader, OS1/OS2, P8 production, projection, reference
   producer, v3 history).
5. **Retire history scopes**: remove the scope parameters, stop `bind_realtime_v2_advanced_history.py` at activation;
   `rt.cohort_history_bindings` stays unread for a week.
6. **Scripts** move after the runtime, then `rt.bar_current` is dropped.

**Going live (in place), Sunday Nov 8 after the 20:00 UTC gate, with the user's yes.** Groups 1-4 are built
(`6c01f49`): the writer is head-only and every live reader reads the head. One image for all 19 backend services,
`alpha-research-realtime-v3:stable-nov8-r52-g102a4-e299d39acca41daf` (`3f3a141`; its code equals the commit; all 426
modules import as in the live images; every live image's code traces to a commit, so no image-only fix is lost).
Steps (`deploy_candidate.ps1`): stop writers and Options processing, snapshot, `g102-migrate` (plan, apply 049-051,
harden, backfill, verify), `g102-services` (each service onto the image between windows, the hotfix way), status, the
pre-open audit.

**Hardening after 051 (found Nov 7).** A migration runs as the database superuser, so `rt.bar_head` is created owned by
it with no grants; every bar write by the app role would fail. The first rehearsal ran as admin and missed it. The
deploy runs the hardening routine provisioning used (`harden_database_v2`): rehearsed on a fresh copy first hardened
like live (375 relations, the live audit passing), it changed exactly one object, `rt.bar_head` (admin owner; app
SELECT, INSERT, UPDATE; api SELECT), in 0.1 s, and the audit passes, so the services' `AUDITED` startup check holds.

**Final-image rehearsal on that copy (as the app role, not admin).** Backfill 432,684 rows = the distinct current keys,
17.8 s. App role upserts the head and may not delete; api role reads current and as-of rows and may not write. Nov 6
windows at 6 cutoffs: equal to the old code and to the first A4 run, lineage equal to live 25 of 25. Bar readers: equal
to the first A4 run; against the old code only the 5m reads differ (the 20:55 bar whose pointer moved at 01:48 is
included, 77 bars vs 76). V3 H coverage for Monday (sessions to Nov 6): old code misses Oct 30 to Nov 6 (its fixed
source cohort ends Oct 29), A4 finds 31 of 31 and 15 of 15 (not on the live P8 path). Rollback fill: 500 removed
pointers restored, a rerun changes nothing.

**Not in this image.** G81 (`options/stock_bar_detection.py`: OS1/OS2 accept V3 intraday bars) was committed Nov 6 but
never reached a live image: `build_candidate.sh` copies `realtime`, `scripts`, `equity` and `tests/realtime` only. It
changes Options alert output, so it goes in after a replay, not untested on Monday. The Options OS detectors stay stopped
(G93/B22).

**Superseded Nov 8: the Monday bundle adds G81 and G75 (user's decision).** Image r54
(`stable-nov8-r54-monday-50fabfd0a5266760`, commit `4c91c62`, now with `options/`; `build_candidate.sh` copies it from
now on) and frontend f18. G81 needed four detector fixes found by replaying Nov 6's whole journal (B22 lease stamping,
oldest journal first, bar-end progress coordinate, one checkpoint per detector and instrument; 583fb1b); the Options OS
detectors start again with the deploy. G75 (`OPTIONS_COVERAGE_TASK.md`): breadth capture (migration 052), our Greeks
beside the provider's on every row, built-in Unusual-activity screens (`options/screens.py`). Rehearsed on a fresh copy
hardened like live with the r53 image (r54 adds only the breadth read fix): migrations 049-052 in 0.7 s; hardening
changes only `rt.bar_head` and the two breadth tables; backfill 432,684 rows in 16 s; the validator passes with 052;
breadth capture of the 9 Options underlyings 16,612 contracts in 71 requests and 29 s; the Options Screener served SPY
4,750 rows (8 published, 4,742 breadth) and the Unusual-activity screens 15 calls of 35 and 18 puts of 24, ours and the
provider's IV on every row (breadth: 0.247 vs 0.242, 0.278 vs 0.274). The rehearsal found one bug, fixed in r54: the
breadth read assumed tuple rows while the API reads with a dict cursor.

**Final Monday image r55 (Nov 8 00:50 UTC).** `stable-nov8-r55-monday-6d4968d8e02f63e2` =
`sha256:917680732b9d…`, commit `7e83946` = r54 + B20 (a later window redoes a missed window's 5m bars; the close waits
for every 5m bar; recovery retries up to 4 times, 10 minutes apart) + crash → wait (B3 controller waits on a refused
plan, B11 Options scheduler reports a failed cycle and retries, 20 in a row still exit; B9c P8 roles print the root
cause first). The image differs from r54 in exactly those 6 files; migrations unchanged. Whole rehearsal again on a fresh
copy of the live dump, all with r55's own code: migrations 049-052 in 0.6 s; audit fails after migrate and passes
after harden, which changes only `rt.bar_head` and the two breadth tables; final owners and grants identical to the r54
rehearsal; backfill 432,684 rows in 15.8 s; validator passes with 052; Nov 6 windows at 6 cutoffs and the bar readers
identical to r54, lineage equal to live 25 of 25; OS detectors 3,800 records (OS1 1,900 and OS2 1,900 over 25 tickers,
same as r54); breadth 16,612 contracts in 71 requests, SPY 4,750 rows, Unusual activity 15 calls and 18 puts, ours and
the provider's Greeks on every row; B20 on a clone with the Nov 6 14:45 bars removed: 25 requeued and derived with
identical revision IDs, a second sweep a no-op. One finding, not a Monday change: run beside the window replays (CPU
contention), the OS detector replay failed "detector lease is stale or expired" (a 100-record catch-up slice took over
the 60 s lease); alone it passed. Live only catches up like this after a mid-session restart, committed slices are
kept, and the next run retries; after Monday consider `--lease-seconds 180` or smaller slices.

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

**Built (Nov 7), simpler than the outline:** migration 045 already gave preparation sessions `expiration_state`
(`EXPIRED` or `SUPERSEDED`), `expired_at` and `expiration_reason`, used by the supersede of an *older* date. So
migration `049_g102_options_preparation_supersede.sql` only replaces `UNIQUE(session_date)` (live name
`advanced_options_preparation_sessions_session_date_key`, checked) with a unique index on `session_date WHERE
expiration_state IS NULL`; no new columns. The reconcile (`_supersede_date_preparation_cursor`, before any write)
marks another revision's never-activated, not-READY preparation for the date SUPERSEDED with reason
`SUPERSEDED_BY_REVISION:<new>` and blocks its open work, as the older-date supersede does; nothing is deleted. A READY
or activated preparation holds the date: the reconcile returns `WAITING_DESIRED` with
`OPTIONS_DATE_HELD_BY_REVISION:<old>:<status>` and writes nothing (today the ACTIVE path would also have hit the
constraint). Live now: the Nov 9 preparation (revision `37d20e26`, Options on 9) is PREPARING, so an Options-on-23
revision for Nov 9 supersedes it until it is READY; after that it waits for Nov 10. Tests: 11 new; lifecycle suites no
new failure. The writes were planned with EXPLAIN against the live schema (`runner/g102_sql_explain.py`).

### 5.2 G91: prepare only what changed

**Today:** `_persist_equity_preparation_work_cursor` (`advanced_realtime_desired_state_repository.py`) creates work
per (revision, intent) for every member outside the source cohort, and readiness is per revision, so each list change
re-runs all 5 stages for every new member (about 450 provider requests and 1.5 hours on Nov 6).

**Change (code only):** before creating work for a member, look for READY intent readiness of an earlier revision
with the same security, member kind and identical stage target (`_equity_preparation_targets`: effective session and
history policy). If found, insert the new revision's readiness as READY with reason `CARRIED_FROM:<revision>` and
create no work. In-progress work for an identical target is reused the same way. Prepare only members that are new or
whose target changed.

**Built (Nov 7), adjusted:** a READY readiness row may not carry reasons (table check), so the origin goes into the
member binding's `source_lineage` (`carried_from`). Two conditions were added because preparation writes into the
revision's source cohort: the earlier revision must have the same source cohort and effective session (so its data is
where the new revision reads, and the targets match). `_carried_equity_preparations_cursor`: a member whose five
stages are COMPLETE on identical targets in such a revision is READY at once with no work (completed stages are the
evidence; the earlier readiness is not required). `_resume_equity_preparation_work_cursor`: the new revision's PENDING
work takes the earlier progress per stage on an identical target (COMPLETE stays COMPLETE, RUNNING or RETRY resumes from
its watermark as RETRY), the same copy a promoted rollover revision already makes; claims take only the current
revision's work, so without this the earlier progress was lost. FAILED and BLOCKED stages start again. Tests: 3 new;
desired-state and preparation suites no new failure; both statements checked against the live schema (the current
revision `37d20e26` would carry nothing today: no earlier revision shares its source cohort and date).

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

**Built (Nov 7):** `realtime/statistics_maintenance.py` (`refresh_planner_statistics_cursor`) and
`PlannerStatistics` in the retention service, on its admin connection (the admin role owns `rt.bar_catalog`,
`rt.bar_current`, `rt.bar_revisions` and its partitions; checked on live). Watch list today: `rt.bar_catalog`,
`rt.bar_current` and the month's `rt.bar_revisions` partition (`rt.bar_head` joins with section 4). Activation is 30
minutes before the open. The loop runs the retention cycle hourly as before and the step after each cycle and every
300 s (`--statistics-interval-seconds`, 0 turns it off); a failed step (5 s lock timeout) is reported and retried, never
stopping retention. Status goes to `<status>.statistics.json` and the log. The statistics views are cached per
transaction, so the after-state comes from what the step analyzed; the alarm names a table still predating activation
at the start of a step once two steps have passed (an ANALYZE without ownership only warns). Dry run on live today
(read-only, writes recorded): the first step after the deploy analyzes all three (32,583, 5,786 and 5,786 changes since
the last analyze) and sets their autoanalyze options; the parent has never been analyzed and is after Monday's close.
Tests: 10 new, retention-service tests unchanged.

### 5.4 B24: the bridge's journal check

**Today:** `advanced_journal_replay.py:304` runs `SELECT COALESCE(MAX(source_offset),0)` on `rt.bar_revisions` every
tick with no index and no partition bound (32% of database time, up to 2.8 s per call).

**Change:** partial index (migration `050`, on its own)
`CREATE INDEX idx_rt_bar_revision_journal_offset ON rt.bar_revisions (journal_generation, stream_session_id,
source_offset) WHERE stream_session_id IS NOT NULL`, and the query bounded by `observed_at >=` the stream session's
start, so it reads only the current partition.

**Built (Nov 7):** migration `050_g102_bar_revision_journal_offset.sql` (the index on the partitioned parent, so every
partition and each later one gets it). The `observed_at` bound is left out: with the index each partition answers
from a short index read, and the query stays exact. Measured on live Nov 7 (idle): the check takes 634 ms as a parallel
scan of the October and November partitions (about 750k rows); the build takes seconds.

### 5.5 G94: new members' split lookback

**Today:** the Equity preparation REFERENCE stage builds `MassiveReferenceAdapter` with its default 180-day
corporate-action lookback, while screening needs SPLIT coverage from the first of the 283 retained adjusted sessions
(about 410 days). Bootstrap computed the lookback from the history plan (`forward_bootstrap.py:173`); preparation did
not, so the 16 members added Nov 6 had coverage only from 2026-05-09 and every window failed until a manual fill.

**Built (Nov 7):** `advanced_equity_preparation._reference` passes `corporate_action_lookback_days` = days from the
stage target's `window_start` (the first retained adjusted session, already in the target) to its
`history_through_session`, plus one (at least 180). The coverage end was already `session + 365 days`. Test: a target
from 2025-07-09 requests splits and dividends from 2025-07-08. **Pre-open check** (`runner/g94_coverage_check.py
SESSION`, read-only): every Equity member of the current revision has SPLIT coverage from the session's first retained
adjusted session; for Nov 9 (first 2025-09-24) 25 of 25 pass.

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
- **Record (migration `052_g102_member_outcomes.sql`, not in v1):** `rt.window_member_outcomes (window_id, stage_id, member_key,
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

### 6.1 Flags instead of 503 on page reads (A2 part, G36, G83, G97 class)

**Rule:** a page is served as it was published. When one of its inputs changed after publication (a security
reference revision, corporate-action evidence, a revised bar), the read labels that input and still serves the page;
the next window publishes over it. Integrity failures still refuse: a stored hash or identity that does not match, or
a reference that did not match at the publication's own cutoff.

**Where the portal still refused (checked Nov 7).** In realtime-only builds the portal reads Ticker pages, Strategies
and the Screener in `retained` mode (`frontend/src/services/api.ts:67`, `services/screening.ts:5`). Those reads
already label reference and action changes (`_retained_read_state`), and the Screener's retained query and status do
not verify bars. Four reads still refuse for a later input change:

| Read | Today | Change |
| --- | --- | --- |
| Ticker chart (`ticker_repository._load_chart`, G36) | 503 `Ticker bar lineage changed; republish required` until the bar leaves the 5-session range | Draw the bars current now over the published range (a chart draws bars, and the current ones are the newest known), log once per snapshot and interval; 503 only when no bar is current in the range |
| Screener hourly details (`_compose_hourly_lineage`, G83 class) | 404 `Screening bar lineage changed` | Return the stored hourly values with the lineage composed from current bars and `lineage_status: CHANGED` (`VERIFIED` when the heads match, `UNAVAILABLE` for a member without lineage); the lineage object is always present because the drawer reads `lineage.bars` |
| Dashboard hot publication (`dashboard_repository`) | 503 `Dashboard reference changed` / `action evidence changed` | Serve it; publication gets `reference_status`, `action_status`, `section_warnings` and the warning appended to `scope_note` (shown in bold on the page) |
| Stock Alerts (`stock_alert_repository._snapshot`) | 503 `Realtime alert reference changed` / `action evidence changed` | Serve it; the warning goes into the page's `warnings` (listed on the page) |

**Also:** `screening_projection` sets `source_publication_status: DEGRADED` when any member is unavailable (section
6), so the Screener's existing coverage notice names them (`screeningModel.dailyCoverageNotice` shows only for
`DEGRADED`); `session_status` keeps PARTIAL or COMPLETE.

**Unchanged:** the `current` read mode (the API default for the Screener and Ticker pages, used by audits and the
validator) keeps its strict lineage and reference checks, so an audit still sees exact drift. G83's cause, the
current read's bar lookup missing history held under other cohorts, is removed by section 4. No portal build is
needed: every label lands in a field the page already shows. **Tests:** each of the four reads with a revised bar,
reference or action serves the page with the label; integrity drift still refuses.

**Built (Nov 7):** `ticker_repository._load_chart`, `portal_compatibility_repository.hourly_details`,
`screening_references.hourly_lineage_of`, `dashboard_repository`, `stock_alert_repository`, the new `read_labels.py`,
and `screening_projection` (DEGRADED). **Tests:** 23 new or rewritten (charts 11, hourly details 5, Dashboard 2,
Stock Alerts 2, labels 3); across the 16 affected suites no new failure against the untouched baseline (the
remaining failures are the 259 Starlette test-client errors and 3 older ones, all present at baseline).
**Read-only check on live** (`runner/g102_read_flags_check.py`, Nov 7 17:08 UTC, cohort `59947a23`, portal read
modes, image vs patched code at the same moment): Screener hourly details refuse for all 25 members today (`Screening
bar lineage changed; republish required`, G83's cause: the lookup finds the session cohort's 7 hourly bars); patched,
all 25 serve their stored values with `lineage_status: CHANGED` and the 7 bars. Charts 200 of 200 serve on both (the
AAPL head was corrected by hand earlier, so G36 does not reproduce today). Dashboard and Stock Alerts serve on both:
no reference or action change is pending on live now.

## 7. Later steps (outline; detailed the same way before they are built)

- **A2, rest:** window attempts that supersede (lineage columns on `rt.window_runs`) with each attempt's per-ticker
  outcomes in `rt.window_member_outcomes` (migration 053, since G75 took 052; v1 keeps them only in the page manifests); ingress plan revisions
  (`UNIQUE (authorization_id, session_date)` becomes unique per revision, C12). (The read labels are built: 6.1.)
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
of 503 (6.1), section 3 (A0), 5.1-5.4 and the G94 lookback, deployed Sunday night.
Migrations go in contiguously (the catalog requires it): 049 (G90 supersede), 050 (B24 index) and, by the user's Nov 7
decision to run the target design on Monday, 051 (section 4, bars without the cohort) this weekend; 052 (member outcomes
on the window run) with A2's rest. Items without detail here (B14a, flags instead of 503, G94) get a
short design note in this file before code (flags instead of 503: 6.1).

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
| Nov 16-20 | A3, A2's rest (window attempts with per-ticker outcomes, ingress plan revisions), A5, then B0-B2; A6 only if needed |

**Monday checklist:**

- Options starts without a manual fix; at the decision stop the stream session ends and no role restarts with an
  error (B14a).
- Equity window times no worse than Friday's.
- Statistics fresh before the first window (status file); bridge journal check under 50 ms (load profile).
- Options Alerts lists the whole session's alerts, one per contract (Options on 9).
- Any ticker failure shows as DEGRADED for that ticker, not a failed window; no page returns 503 for a reference or
  lineage difference.
- After the close: the close completes; validator on the pages.
