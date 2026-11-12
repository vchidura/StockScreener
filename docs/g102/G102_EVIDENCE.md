# G102 evidence: rehearsals and live runs, dated

What was measured, where and when. Moved verbatim from `G102_DETAILED_DESIGN.md` (Nov 10, 2026) and extended since.
Normative rules are in `G102_TARGET_DESIGN.md`; statuses in `G102_IMPLEMENTATION_MATRIX.md`; procedures in
`G102_RUNBOOK.md`. Times are UTC.

## Nov 7: needs check, code footprint, builds and replays

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

### A0 session list (G100): built and replayed

Built Nov 7, with three changes from the first outline:

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

### A4: what the bar tables looked like before (Nov 7)

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

### A4: selection order changed after the rehearsal

**Changed after the rehearsal (Nov 7).** The first version put the tier first. On a copy of live it changed 29 of
200 published chart groups: Advanced provider weekly bars displaced the derived weekly bars the precedence table
prefers (AAPL: 61 weekly bars, 32 with different values), and Advanced derivations displaced Developer REST 5m bars.
With the order above, all 200 groups pick exactly what they were published with (4.10).

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

### G90 supersede

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

### G91 carry readiness

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

### G99 statistics

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

### B24 bridge index

**Built (Nov 7):** migration `050_g102_bar_revision_journal_offset.sql` (the index on the partitioned parent, so every
partition and each later one gets it). The `observed_at` bound is left out: with the index each partition answers
from a short index read, and the query stays exact. Measured on live Nov 7 (idle): the check takes 634 ms as a parallel
scan of the October and November partitions (about 750k rows); the build takes seconds.

### G94 split lookback

**Built (Nov 7):** `advanced_equity_preparation._reference` passes `corporate_action_lookback_days` = days from the
stage target's `window_start` (the first retained adjusted session, already in the target) to its
`history_through_session`, plus one (at least 180). The coverage end was already `session + 365 days`. Test: a target
from 2025-07-09 requests splits and dividends from 2025-07-08. **Pre-open check** (`runner/g94_coverage_check.py
SESSION`, read-only): every Equity member of the current revision has SPLIT coverage from the session's first retained
adjusted session; for Nov 9 (first 2025-09-24) 25 of 25 pass.

### Per-ticker outcomes v1

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

### Flags instead of 503

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

## Nov 8: review of bars without the cohort, Monday bundle, service start run

**Status.** Live since Nov 8: migrations 049-052 applied at 20:12 UTC, 19 backend services on r55, Stock Alerts on r56,
frontend f18. No market session has run on it yet. The review of the code found the design sound: corrections add a
revision, only the current row changes, and the selection rule gives the same result in any arrival order (**corrected
Nov 10, 9.4 R1: not for exact ties on precedence, observation and tier, where the upsert lets the later write win**). No live path
needs a change before Monday. The 13:00 UTC pre-open audit (`preopen_smoke.py`, `audit_sessions.py`, `audit_live.ps1`)
does not read `rt.bar_current`.

**What Nov 5-6 lost the days to, and what was added to the rehearsal for it.** The Nov 6 register (B1-B25) has three
triggers: a scope change (Equity 9 to 25, Options 2 to 9; B4, B5, B6, B8, B9, B15, B16), services running over time
(start, leases, restarts, day boundary; B1-B3, B11, B14, B17) and live load (B20, B23, B24). Monday keeps Friday's scope
(Equity 25, Options 9). Added on Nov 8 on top of the replays (4.10):

- **Service start run.** A fresh dump of live (04:41 UTC, read-only), Sunday's database steps with r55 (049-052,
  harden, audit, backfill), then all 19 backend services from `compose.stable.json` on r55 against that copy under
  live's name, inside the isolated test server (from that network live's database host and the internet are
  unreachable; checked), with test credentials, copies of the three volumes and no host ports. All 19 started and
  waited with a status (Options roles READY_WAITING_FOR_SESSION, OS detectors WAITING_FOR_READY_SESSION, Equity
  activation pending 2026-11-09 14:15 UTC, finalizer DESIRED_SNAPSHOT_UNAVAILABLE as live), no restart, no error; the
  statistics step ran; all stopped cleanly (exit 0, about 2 s). The Nov 9 Options preparation (`37d20e26`, Options on 9,
  CATALOG_QUOTE waiting for the provider window) was left as it was by the new reconcile.
- **Portal routes.** The 153 GET routes of `runner/api_smoke.py` against the test API and against live's API: the same
  status on every route (131 OK). Bodies differ on 3: the Options discovery catalog lists the built-in screens (G75);
  Equity preparation (members' states); `/trading-sessions` lists 260 sessions instead of 283 (the route merges each
  cohort's list; with A4 every cohort reads the same bars, so the merge is exactly the requested 260 latest).
- **Not rehearsable on a Sunday:** the provider-window and activation steps on Monday's clock, the stream, and live
  load. They are the same code paths as Friday except the changes replayed above; the pre-open gate covers them.

**Build guard (Nov 8).** `runner/build_candidate.sh` now copies `research/` (Stock Alert and screening rules) and refuses
an image whose code differs from the worktree in any backend code folder; its old check missed committed changes in
folders it did not copy (how G81 in `options/` was missed). r55: all 947 worktree files present and identical.

## Nov 9: first G102 live session

**Monday Nov 9 in one line:** Equity 74 of 78 cutoffs and the close completed by itself (22:01 UTC); Options 53
windows, 20 cycles lost (15 to a quiet contract, 3 to the flow crash and dividend expiry, 2 to the O1 policy mismatch);
6 stock alerts. Monday's fixes went live as r58-r62 during the session; tonight r63 (all 19 backend services) and
frontend f19 went live at 00:59-01:14 UTC after a rehearsal on a fresh copy of live (all 19 services started and waited;
153 routes identical to live; full offline suite: the same 161 failures as an r62 baseline, so no regression).

### Nov 9-10: Design items: done

| Item | Status | Evidence |
| --- | --- | --- |
| Section 3, A0: Options Alerts as a session list (G100) | Live Nov 8; first use with packages Tuesday | O1 package selection was held on Monday (policy mismatch) and is on in r63 |
| Section 4, A4: bars without the cohort | Live Nov 8, ran Monday | All windows read `rt.bar_head`; close complete; no head errors |
| 5.1 G90 supersede | Live | Not exercised Monday (no revision change) |
| 5.2 G91 carry readiness | Live | Not exercised Monday (no list change) |
| 5.3 G99 statistics step | Live, working | `bar_head`, `bar_catalog` and the November partition analyzed 14:02 UTC Monday (before the open) and during the session |
| 5.4 B24 bridge index | Live | No bridge time alarm Monday |
| 5.5 G94 split lookback | Live, extended tonight | Monday's CRWD drop came from the *rollover*, not preparation: its REFERENCE claim started at the completed session, so the adapter's 180 days re-recorded CRWD's July split under a narrow coverage row. r63: the rollover covers from the first retained session; CRWD clean at 01:04 |
| Section 6, per-ticker outcomes v1 | Live, gap fixed | Pattern Watch still required every member (all Equity windows failed 14:35-14:45); fixed r58 |
| 6.1 Flags instead of 503 | Live | One remaining 503 class, see 9.3 (CRWD) |
| B14a stream session end | Live, worked | Nov 6 session `d42b2d84` ended `ENDED_BY_NEXT_SESSION` |
| B20 missed-window sweep; B3, B11, B9c crash to wait | Live (r55) | No crash loop on Monday except the r60 flow fix |
| G75 breadth | Live | Screener since Nov 8; Flow and Activity read it since r63 (SPY 8 to 5,276 contracts, totals equal the database) |
| G81 OS detectors | Live | Lease 180 s; write-conflict retry r58 |

### Nov 9-10: Fixed during and after Monday (r58-r63)

| Fault | Fix |
| --- | --- |
| One dropped ticker failed Pattern Watch and every Equity window | r58 |
| Detector and Stock Alerts crashed on write conflicts | r58, r59 (retry) |
| Options flow crash-loop on an out-of-scope contract | r60 |
| One contract without open interest blocked the O1 lane | r61 |
| O1 package valuation policy mismatch failed whole windows | r62 hold; r63 session policy, unmarkable package recorded without a mark, selection on (env `REALTIME_V3_O1_PACKAGE_SELECTION=DISABLED` turns it off) |
| One quiet contract made the Options snapshot stale (15 cycles) | r63: a quiet minority no longer bounds the snapshot (103 of 103 Monday snapshots pass, 58 before) |
| Dividend coverage expired mid-session | r63: the rollover keeps it valid through the close; refreshed for every member by 01:29 UTC |
| Stock Alerts model filter listed old models | r63 + f19: research rules, primary or also matched |

## Nov 10: r63 and r64 deploys, bar-selection preflight, review checks

**Quiet-contract rule (r63).** All 103 Options exact-contract snapshots of Nov 9 replayed against the rule (read-only,
recorded payloads): 103 pass (58 under the old rule); at most 4 of 72 contracts quiet; at least 90 s of validity left.

**Offline suite (r63 worktree).** 4,697 passed, 161 failed, 128 skipped over every `tests/realtime/test_*.py` without a
database; the same 43 files run on the r62 code (extracted from the r62 image) fail the identical 161 tests (missing
repo files, DB environment variables, older fixtures). No regression.

**r63 service start run on a fresh copy** (`stocks_db_backup_v3-r63-rehearsal-20261109T235304Z.dump`, 2.0 GB, sha256
B53DB541...; restored in 151 s into `test_realtime_v2_r63` on the isolated `g102-test-pg`; only Nov 9's Options journal
copied): host Postgres, the provider and the internet unreachable from the test network; 19 services started, no
restart; Options roles `READY_WAITING_FOR_SESSION`, Equity controller `SESSION_ACTIVATION_PENDING 14:15`; 153 API routes
identical to live (124 OK); Flow SPY 5,276 breadth contracts, Activity all 9 names on breadth, Stock Alerts 5 research
models listed and `model=SW3_B` 3 alerts. Retention exited on `permission denied for schema rt` (the copy was not
hardened; retention code unchanged since r57). Stop exit codes: A/B of r57 and r63 on the copy, both exit 0 in 1 s.

**Docker Desktop wedge, 23:57-00:10.** Docker Desktop's forwarder log (`monitor.log`) stopped after its 23:51:36
rotation; the engine answered 500 or hung, the API and frontend timed out, worker leases stopped at 23:56:59. Recovered
with `docker desktop restart` at 00:10; all 20 live containers back, leases renewing, no errors.

**r63 live (00:59-01:14).** All 19 backend services onto r63 and the frontend onto f19; API routes 124 of 153 OK,
unchanged; the Options scheduler restarted 8 times during the lease handover, then stable. The rollover refreshed
dividend and split coverage for all 25 members by 01:29 (window from 2025-09-24); CRWD's split drift cleared at 01:04.

**Bar-selection preflight (02:19, read-only, first live run's range).** Migration 051 present; 25 active instruments;
358,373 head keys and 645,156 revisions in scope (1m from Nov 2; 5m-1h from Jul 2; 1d from 2025-07-18; 1wk from
2021-11-12; 1mo from 2021-11-30; through Nov 9); 209,614 keys with more than one revision; 1 key tied on precedence,
observation and tier (AAPL 1m RAW RTH 2026-11-02 20:28: revisions `c6092010` and `03d87a6f`, both stream, Advanced,
observed 21:00:01.108992, created 0.3 ms apart by cohort `52b9fef3`); head = canonical winner `c6092010`; 0 mismatched
heads in any key; 0 keys with differing bar ends; 14.5 s.

**r64 (earnings, G92), 01:57-02:10.** Read-only Finnhub probe: 16 of 16 companies answered (NVDA 2026-11-17, CRWD 12-01,
AVGO 12-10, ORCL 12-14, MU 12-15; 11 with no event in 45 days; 9 ETFs not applicable); stored coverage matched it except
AVGO (Dec 9 stored, Dec 10 now). Rule replay on Nov 9 19:00 inputs with the new loader: 16 stocks with earnings facts,
NVDA's SW3_B match no longer fires (earnings inside its horizon). The unavailable rules on 6 tickers are the
sector-context variants (`INCOMPLETE_BENCHMARKS`), not earnings.

**Financials (r65, built, not deployed).** Read-only probe: 16 companies, 1,314 reports in 7.7 s (AAPL FY26 Q4 revenue
114.5B, diluted EPS 2.10). Copy rehearsal with recorded rows: stored 1,330 facts in 1.2 s; window load 46 ms, projection
26 ms, 30 reports kept per company, about 38 KB of workspace per company. Monday's ticker workspaces: 1,805 snapshots,
75 MB, 43 kB each.

**Review of the design (checked against the code).** R1: the head upsert runs for replayed revisions and `_head_replaces`
lets the later write win exact ties. R2: `advanced_options_equity_input.py:366`, `advanced_options_p8_production.py:1357`
and `equity_interval_derivation_worker.py:68` filter the current head by `observed_at <= cutoff` without the as-of
fallback; the allowlist has 38 modules. R3: broad `except ValueError` at `screening_projection.py:362` and
`ticker_projection.py:1360`; barrier FAILED at `materialization_jobs.py:778`. R5: bindings written by 3 modules, read by
`equity_history.py`. R6: `_supersede_date_preparation_cursor` locks existing rows only. R7: 4 silent fallbacks in
`portal_compatibility_repository.py` (session alerts, Screener breadth, Flow breadth, current marks).

## Nov 10 (03:00-04:00): r66 fixes from the review and 4.11

**R1 winner tuple (A4.3, 4.11-4).** The head upsert now compares the full tuple of target 4.3: source precedence,
`observed_at`, tier, then the revisions' own `created_at` (looked up in `rt.bar_revisions` by primary key) and revision
id; a revision never replaces itself. Database tests on the isolated test server (`g102-test-pg`, disposable database),
9 of 9 pass: exact tie (later write wins by `created_at`, a replay of the loser leaves the head); same-revision replay
leaves `updated_at` unchanged; three revisions in both arrival orders reach the same head and the canonical winner; two
writers on one key at READ COMMITTED and at SERIALIZABLE (barrier start, retry on serialization failure) both store their
revision and end on the canonical winner.

**Write delay of bar revisions (input to A4.5).** Read-only on live, Nov 9 session, `created_at - observed_at`: 5m
derived p50 1.1 s, p99 11.4 s, max 53.7 s (2,050 revisions); 15m/30m/1h/1d derived max 12.5-13.1 s; 1m stream p99 1.3 s,
max 1.4 s (9,749, all with `provider_published_at`); 1d reconstructed REST 77-85 s (25, all with
`replay_available_at`). No non-final revisions. So the full four-clock contract would drop, at a known-by shortly after
the cutoff, a bar the Options readers accept today; the readers keep their clock set until A4.5 decides how the live
known-by time relates to the write.

**R2 central known-by read (A4.6).** `load_known_bar_revisions_cursor` in `equity_bar_repository.py`: per key, the head
when its revision is known by the cutoff, else the best revision known by then (same order, same `bar_end`), never
dropped; named clock sets `KNOWN_BY_OBSERVED` and `KNOWN_BY_ALL_CLOCKS`. The Options equity input and P8 spots use it with
`observed_at` (as before); the interval derivation with all four clocks (as before). Database tests: a key corrected
after the cutoff reads as it stood (the old readers returned nothing); all clocks also require the write time; `latest`
reads the last keys as known. Allowlist 38 to 36 modules (Options equity input, P8 production). Unit fakes for the
Options input updated; the 4 DB tests that fail (equity input binding, input-ready, P8 canary, preparation HTTP
failure) fail identically on the r65 code (fixture `cycle_receipts` check), so they do not exercise the change; the
copy rehearsal does.

**R7 fallbacks visible (INV-9).** Session alerts, Screener breadth, Flow breadth and current marks add
`data.optional_reads` entries (`read`, `status` AVAILABLE or FALLBACK, `reason_code`, `serving_source`) and log the error
with its traceback; current marks now run under a savepoint. Portal tests: no failure beyond the baseline.

**4.11-2.** `runner/recovery_steps_nov6.py` counts Advanced 1m heads (125 per window) and `runner/os_replay.py` reads
`rt.bar_head`; `runner/fix_aapl_live.ps1` refuses to run (kept for the record; corrections go by runbook 8).

**Baseline failures fixed on the way.** `test_bar_head.py` boundary test (a docstring in `research_rule_input.py` named
the head table) and `test_equity_bar_repository.py` group-read assertion (whitespace).

## Nov 10 (04:00-04:30): r66 checks on copies of live and the service start run

**Upsert planning cost, and the change it led to.** EXPLAIN ANALYZE of a tie replay on the Nov 9 copy: the first R1
version planned in 6.4 ms (four `created_at` lookups over 15 monthly partitions; execution 0.26 ms) against 0.06 ms on
r65. Reworked: one row comparison with the ids swapped in the last element, and the writer passes the new revision's
write time as a value (it already reads the stored revisions back), leaving one lookup: planning 1.7-2.6 ms.

**Writer replay A/B (copy of Nov 9, one transaction each, rolled back).** 3,001 keys with more than one revision (bar
starts Nov 2-9, all intervals, pseudo-random order) plus the tied AAPL key; 8,257 revisions replayed newest first, so
every older revision arrives after the one that beat it. r65 code: 1 head moved off the canonical winner (AAPL 1m
2026-11-02 20:28, `c6092010` to `03d87a6f`) and 3,000 heads rewritten in place (`updated_at` bumped by a replay of the
same revision); 1.69 ms per revision. r66: 0 moved, 0 rewritten, every head the canonical winner; 3.25 ms per revision.

**Reader parity (copy of Nov 9, read-only), r66 against the r65 SQL.** Options equity input (O3, SPY and QQQ, 78 cutoffs,
known-by 5 s and 60 s after): 312 of 312 identical, p50 5.7 ms against 58.6 ms. P8 selection spots (Monday pre-open plus
78 cutoffs): 79 of 79 identical, p50 5.2 ms against 117.7 ms. Interval derivation sources (25 members, 4 check times):
100 of 100 identical, p50 7.4 ms against 3.3 ms.

**Image.** r66 `sha256:81e236e83b9e` (`stable-nov5-r66-ea959160ca468840`); image code equals the worktree; against r65
exactly six modules differ (`equity_bar_repository`, `equity_interval_derivation_worker`, `advanced_options_p8_production`,
`advanced_options_equity_input`, `portal_compatibility_repository`, `research_rule_input`). Offline suite on the worktree:
159 failed, 4,719 passed, 128 skipped, 1 collection error; no failure outside the baseline, two baseline failures fixed;
the collection error (`test_equity_interval_derivation_worker.py` imports a name removed before r63) is in the r63 run too.

**Service start run on a fresh copy** (`stocks_db_backup_v3-r66-rehearsal-20261110T040723Z.dump`, 1.96 GB, sha256
F02A6426...; dump 235 s, restore 162 s into `test_realtime_v2_r66`; 445,583 head rows; latest window Nov 9 22:01). Network
probe: only `g102-test-pg` reachable. Hardening passed 11 of 11; it resets database grants, so the test logins lost
CONNECT and all 19 services exited on the first start (runbook 6 reordered: harden, then grant). After the grant: 19
running, no restarts; the only error line is the fundamentals refresh failing to reach the provider (expected, no route).
API: 153 routes, 124 OK on the test API and on live, no status differences. Optional reads on the Nov 9 Options session:
Flow `flow_breadth` AVAILABLE from BREADTH (9 underlyings), Screener `screener_breadth` AVAILABLE, Alerts `session_alerts`
AVAILABLE from SESSION_LIST and `current_marks` AVAILABLE from STORED_QUOTES. Stop: 17 exit 0; the controller (waiting for
the 14:15 activation) 137 after the 60 s grace and Options market-state 241, as in the first r63 stop; A/B of the two
services alone on the same copy, r64 and r66: all exit 0 in 1-2 s. Image-independent; a whole-stack stop ordering item.

## Nov 10 (05:30 UTC): register triage; the gap and blocker registers frozen

The gap register (G1-G105, statuses last edited Nov 5-7) and the Nov 6 blocker register (B1-B25) were checked item by
item against later work, live state and read-only checks; the matrix is the only tracker from now on.

**Read-only checks on live.** All 19 backend containers healthy with 50 MB log rotation; Options roles with CPU limits
(2 or 3 cores) (G11, G34, G42, G45). No Equity window row left RUNNING past its deadline (G32). Equity windows since
Nov 9: 127 COMPLETE, 18 FAILED, 4 MISSED, 6 RETAINED_GAP, 3 PENDING; the failures are the 14:35-14:45 openings
(Pattern Watch required every member, fixed r58), 17:05 (CRWD provider-empty minute) and the 21:00 close (completed
22:01, G103). `stage_runs` keeps reason codes, and since B9c the log's first line names the cause (G10). The API's
`REALTIME_V2_BACKEND_IMAGE` label names `84ffaace` while it runs `881c0c72` (G57, open). `/api/stocks/context` 503
`ValueError` on live and r66 (G65, open). The two coordinator scripts now bind the names G17 reported.

**Gap register, 105 items.**

- Addressed (60): G1, G2, G3, G6, G9, G10, G11, G15, G16, G17, G18, G19, G24-G32, G34, G39, G42, G43, G45, G47-G56,
  G58, G59, G61-G64, G66-G69, G71-G73, G76, G78, G80, G82, G85, G88, G95-G98, G105. Most are the Nov 3-5 recovery's
  "fixed in candidate" items: those images went live on Nov 5, and Equity and Options have published every session
  since (Options windows, the Equity close, stock alerts, pages). G68 and G72: the Nov 9 rule (ended gaps publish,
  an `INCOMPLETE` contract no longer stops a window). G80: B11. G25: in-session fixes go in between windows.
- Already matrix rows (19): G37, G38, G75, G81, G84, G90, G91, G92, G94, G99, G100, G102, G103; G36 (A4.3, A2.3),
  G46 (B14a), G74 (quiet contract), G83 (A2.3), G93 (blocker B1 fix r48, A2.5), G104 (G90).
- Folded into matrix rows (8): G5, G14, G44 (A2.4); G23 (A2.2); G70 (A5); G89 (B0); G33 (stack stop); G35 (B20).
- Open, new rows in matrix section 3 (17): G4, G7, G8, G13, G20, G21, G22, G40, G41, G57, G60, G65, G77, G79, G86,
  G87, G101.
- Closed by the user the same day (1): G12. The legacy AlphaResearchPro worker services are turned off; the stack runs
  only outside market hours and serves Model Lab.

**Blocker register.** Fixed live: B4-B8, B9b, B13, B15, B16, B18, B23; B9 (Options' own Equity membership) shipped Nov
6. Matrix rows: B1 and B2 (G93 fix r48; audited reopen A2.5), B3/B9c/B11 (VERIFIED Nov 9; the register's "Open" is
stale), B10, B12 (G92), B14a, B14b and B17 (A2.4), B19 (G38), B20, B24, B25 (G84).

## Nov 10 (07:10 UTC): alpha-screener-pro.com moved to the realtime stack

The domain used to reach the legacy AlphaResearchPro frontend through that stack's token-based Cloudflare Tunnel,
behind Cloudflare Access. The user created a new tunnel for the realtime stack, moved the public hostname to it (target
`http://realtime-v2-advanced-frontend:80`) and started its connector `stock-screener-realtime-v2-cloudflared` on
`alpha-research-realtime-v2-application`. The legacy session retired the old connector (stopped, restart `no`, Compose
profile `retired-legacy-tunnel`, its DEPLOYMENT.md updated). Checks: 4 registered tunnel connections; `/` and
`/api/realtime/health` answer 302 to the Cloudflare Access login; the only connector errors are browser-cancelled
requests. Live pipeline untouched.

## Nov 10 (07:30-08:35 UTC): review of the Nov 9-10 work, data readiness, final r66 + f21 rehearsal

New session; the previous one stood down. Plan (user): r66 + f21 go live before the Nov 10 open.

**Review against the target design (read-only).** Worktree source hash `ea959160ca468840` = the r66 image label, so the
image code is the worktree; against r65 exactly six modules differ (the six named Nov 10 04:30); r63's modules are
byte-identical in r64, r65 and r66, r64's earnings module in r65 and r66, r65's `fundamentals_refresh.py` in r66.
INV-2: `_head_replaces` is one row comparison over the tuple of target 4.3 (precedence, `observed_at`, tier, the
revision's `created_at`, ids swapped so the lower id wins and a revision never beats itself); `_selection_order` has
the same terms; the backfill, `_head_rows_sql`'s as-of fallback and `load_known_bar_revisions_cursor` order by
`_selection_order`; the writer passes the stored revision's `created_at` (the row at the catalog's observation).
INV-3/A4.5: the Options equity input and P8 spots read `KNOWN_BY_OBSERVED`, the interval derivation
`KNOWN_BY_ALL_CLOCKS`, each its previous set. INV-4: 41 realtime modules name the bar tables = owner + 4 metadata + 36
allowlisted (38 before); the diff adds bar-table SQL only in the owner. INV-9: the four fallbacks add
`data.optional_reads` (status, reason code, serving source) and log with the traceback. Findings: matrix rows R8
(derivation test file does not collect), R9 (`coverage_after.sql` checks XLV instead of SMH), R10 (the Financials
refresh runs at the preparation role's first idle slot, not only after the close); G65's 503 is the Dashboard
retained read refusing (`dashboard_repository.py:176`, "identity/checksum/clock drift"), the same refusal behind
`/latest-price-date`, `/tickers/overview`, `/tickers/setup-summary` and `/scan/all` (live since at least Nov 10 00:54,
identical on r66).

**Tests.** Offline suite on the worktree (326 files plus `tests/test_stock_alert_rule_models.py`, run with
`--continue-on-collection-errors`): 159 failed, 4,722 passed, 128 skipped, 1 collection error; the failing set is
identical to the 04:00 r66 run and inside the r62 baseline. DB tests on the isolated test server:
`test_bar_head_v2_db.py` 12 of 12. Frontend f21: 287 tests, 40 failing, the same 40 as f19 (live).

**Data readiness (read-only, 07:31-07:39).** Desired revision `e413210c` (since Nov 9 21:00): 19 EQUITY + 6 SECTOR_ETF
READY, 9 OPTION_UNDERLYING WARMING. Nov 9 close COMPLETE (retry4, 22:01); 74 of 78 Equity cutoffs (14:35-14:45 and
17:05 failed); 1m/5m/15m/30m/1h RTH heads complete for all 25 except CRWD 389 of 390 (17:01, known); 1d RAW and
ADJUSTED final for 25. Options Nov 9: 53 COMPLETE, 2 FAILED, 3 PENDING; 6 stock alerts. Options preparation Nov 10
PREPARING at CATALOG_QUOTE ("provider window is not open; history ready"). Dividend and split coverage refreshed for
25 of 25 at 01:00-01:29 (window from 2025-09-24), dividends valid through the close and recovery, no split drift;
`g94_coverage_check.py 2026-11-10` PASS 25 of 25. Earnings: 16 stocks covered (NVDA 11-17, CRWD 12-01, AVGO 12-09,
ORCL 12-14, MU 12-15), 3 ETFs NOT_APPLICABLE, valid to Nov 11-12; Fed calendar valid to Nov 16; 25 members with active
security revisions. Capability profile PASS until 21:01 UTC (renewed daily about 21:01, as Nov 9). `bar_revisions`
partition through Nov 30; statistics current; 44 of 100 connections; 4.4 GB; host disk 440 GB free. 20 live containers
up, no restarts since the r64 deploy, 20 leases renewing; API errors only the Dashboard refusal above. Tunnel: 4
connections; `alpha-screener-pro.com` 302 to Access. At 08:12 the rollover began renewing security identity (and with
it dividend and split coverage) for the 16 stocks, about 78 s each (identity `effective_to` within the 3-day margin):
normal r63 maintenance.

**Rehearsal on a fresh copy** (`stocks_db_backup_v3-r66b-rehearsal-20261110T080307Z.dump`, 1.96 GB, 259 s, sha256
C2F9F1FD...; restored in 176 s into `test_realtime_v2_r66b`; 445,583 heads; latest window Nov 9 22:01). Renamed,
hardened (11 of 11), then CONNECT granted. Socket probe from the test network: only `g102-test-pg` reachable (live
database host, live API, Massive, Finnhub, DNS blocked). Volumes: reports, retention, stocks journals and Nov 9's
Options folder (3.9 GB, 23 s). 19 services on r66 from `compose.stable.json`: all running after 2.5 min, no restarts,
expected waits (controller `SESSION_ACTIVATION_PENDING 14:15`, Options window `READY_WAITING_FOR_SESSION`, detectors
`WAITING_FOR_READY_SESSION`, lifecycle Nov 10 CATALOG_QUOTE); error lines only the preparation's provider
`ConnectionError` (fundamentals refresh and the rollover's identity renewal, no route) and the six Dashboard refusals
the smoke causes on live too. API: 153 routes, 124 OK on the test API and on live, no status differences; one body
differs (`/options/flow`, +110 bytes: `optional_reads`). Features: Flow `flow_breadth`, Screener `screener_breadth`,
Alerts `session_alerts` and `current_marks` all AVAILABLE on the Nov 9 session; Flow SPY 5,276 breadth contracts,
Activity all 9 underlyings on BREADTH; Stock Alerts 5 research models, `model=SW3_B` 3 alerts; rules replay at Nov 9
19:00 with `earnings_covered` 16 (NVDA's swing match held by its Nov 17 earnings; the N/A path is covered by unit
tests since every stock has coverage); quiet-contract rule through the module: 103 of 103 Nov 9 snapshots usable (58
under the old rule), reload check 103 of 103, at most 4 quiet, at least 78 s headroom; O1 selection enabled in the
window container (no override). f21 on the test network with live's nginx file: every page path served, assets and
the Options, Stock Alerts and TickerWorkspace (Financials) chunks 200, `/api` proxied to the test API. Stop: 17 exit 0;
finalizer and Options lifecycle 241 within 0.3 s of the stop (a child ended by SIGTERM, returncode -15 passed up by the
wrapper), controller 0: the image-independent stack-stop row, a different pair than Nov 10 04:20. After cleanup
(copy renamed back, logins NOLOGIN): writer replay on the fresh copy, 8,257 revisions newest first in one rolled-back
transaction: r66 0 moved, 0 rewritten, 4.6 ms each; r65 1 moved (AAPL 1m 2026-11-02 20:28, `c6092010` to `03d87a6f`)
and 3,000 rewritten. Reader parity: 491 of 491 identical (input 312, spots 79, derivation 100); p50 input 6.0 ms
against 64.9, spots 6.2 against 141.8, derivation 7.8 against 3.2. Financials with the recorded statements (no
provider): 1,330 facts for 16 companies stored in 1.3 s, window load 54 ms, projection 38 ms, 480 reports kept, at most
39 KB per ticker. Images unchanged (r66 `sha256:81e236e83b9e`, f21 `sha256:b4d3c1210837`); `compose.stable.json`
sha256 a4e3fc82... unchanged.

**Rollover identity renewal on live, 08:12-08:32.** The 16 members added Nov 6 (AMD, AVGO, CRWD, GOOGL, INTC, JPM, MSTR,
MU, ORCL, PLTR, SMH, XLC, XLE, XLF, XLK, XLY), about 78 s each, 3 provider requests each, all PROGRESSED; 16 new
security revisions (248 active rows for 25 members). Rechecked at 08:32: dividends 25 of 25 valid through the close and
recovery, no split drift (CRWD's split re-recorded 08:13 under the wide row), `g94_coverage_check.py 2026-11-10` PASS
25 of 25, readiness unchanged (25 READY), Options preparation still PREPARING at CATALOG_QUOTE, 20 leases renewing.

## Nov 10 (09:14-09:40 UTC): r66 + f21 live before the open

User's go (09:1x, "ok, sure"); the user added: mostly avoid rolling back, fix during the live run as on Nov 9.
`deploy_candidate.ps1 -Step preflight` 09:14: no pending migrations (latest 052), audit passed. Live images saved to
`rollout/pre-r66-images-20261110.txt` (19 services r64 `881c0c726ebb`, frontend f19 `dc148c108834`, full ids).
`-Step g102-services -Image sha256:81e236e83b9e...` 09:15:14-09:30:59, one service at a time; `-Step g102-frontend
-Image sha256:b4d3c1210837...` 09:31:14-09:31:53. Checks at 09:32-09:34: 20 services on the new images and healthy; the
Equity scheduler and Stock Alerts restarted 8 times each with `DurableLeaseUnavailable` on their own role lease during the
handover (expected; stable since 09:21 and 09:23); no other error line since 09:15. API 153 routes, 124 OK, no status
difference from 08:16. `optional_reads` on live: Flow `flow_breadth`, Screener `screener_breadth`, Alerts
`session_alerts` and `current_marks` AVAILABLE on the Nov 9 session. 25 READY, Options preparation PREPARING at
CATALOG_QUOTE, 20 leases renewing. Financials: the preparation role (idle) ran the refresh at its first slot, 09:17:19:
16 companies, 1,314 reports, none missing; `ref.fundamental_reports` 1,314 rows, 16 coverage rows. Bridge and
finalizer: no writer errors (no bar writes before the session). Tunnel: 4 connections, no errors since the deploy;
`alpha-screener-pro.com` and `/api/realtime/health` 302 to Access.

### Nov 10 (13:03-13:08 UTC): pre-open audit (runbook 7), read-only

All 20 containers on r66 `81e236e83b9e` / f21 `b4d3c1210837`. Restart counts the same as at 09:34 (Equity scheduler 8,
Stock Alerts 8, every other service 0). 25 Equity members READY (19 EQUITY + 6 SECTOR_ETF); 9 OPTION_UNDERLYING WARMING.
Options preparation PREPARING at CATALOG_QUOTE, "target-session provider window is not open; history ready", failure
streak 0; work rows CATALOG_QUOTE RETRY 9 and the later stages PENDING. 20 leases renewing; the only lease released today
is the rollover's (08:32). Desired members (scratchpad `ready3.sql`): dividend and split coverage for 25 of 25 refreshed
01:00-08:31, dividends valid to 2027-11-09, no split drift; earnings covered for the 19 stocks to 11-11/11-12 (NVDA
11-17, CRWD 12-01, AVGO 12-09, ORCL 12-14, MU 12-15), Fed calendar valid to 11-16. `g94_coverage_check.py 2026-11-10`
PASS 25 of 25. `g102_read_flags_check.py` with r66 on the Nov 9 cohort (no portal cohort is current before activation):
charts 195 OK, 5 refused (4 RETAINED_INTERVAL_UNAVAILABLE, 1 NATIVE_1M_HISTORY_INCOMPLETE); screener 24 rows DEGRADED,
CRWD unavailable (its 389-of-390 Nov 9 minute bars, known); hourly lineage VERIFIED 24; Dashboard still refuses with the
stored publication drift (G65); Stock Alerts OK. `optional_reads` on live `/options/flow`: `flow_breadth` AVAILABLE.
Financials: `REFRESHED` once at 09:17 (16 companies, 1,314 reports), `CURRENT` at every idle slot since;
`ref.fundamental_reports` 16 companies, 1,314 rows. Errors since 09:35: only the API's four Dashboard drift tracebacks
(portal opened 09:57-09:58). `alpha-screener-pro.com` 302. `live_watch.ps1 -DryRun` started detached 13:07:59.

### Nov 10 (14:15-15:05 UTC): activation, first windows, three fixes forward

**Activation (check at 14:22).** Cohort `38dbea9f` created 14:15:05, effective 14:30; Nov 10 ingress plan READY 14:15;
20 leases renewing; no new restarts; no error line in any service since 13:08. Stocks stream `93feee57` LIVE from
14:30:02 (WARMING 14:30:24, LIVE 14:31:26 in the watcher log); bars for all 25 members from 14:30.

**First windows (G13, G40).** 14:35 COMPLETE on its first attempt (188 s), 14:40 COMPLETE (23 s), 14:45 and 14:50
COMPLETE. SPY 1m and 5m charts 0 minutes behind the cutoff at 14:35 and 14:40; no workspace lacked an interval
(`runner/q_lag2.py`); 5m bars stored 16-17 s after they closed. Both rows closed as not reproduced. Bridge and finalizer:
no writer error (the r66 head upsert), bars persisted each cycle. Stock Alerts published every cutoff (19 NO_MATCH).
`optional_reads` live: Flow `flow_breadth` and Screener `screener_breadth` AVAILABLE; Alerts none (no Options session).

**R11, Options CATALOG_QUOTE (r67, lifecycle, 14:43:04).** At the provider window (14:31-14:32) AAPL, AMZN, MSFT, NFLX,
QQQ completed; IWM, NVDA, SPY, TSLA failed `ValueError: P7 transport candidate is invalid`. Read-only probe with the live
code and the same query (`scratchpad/p7_probe.py`): every rejected row is a new product listed under the underlying
(`O:B1VDA261113C00160000` underlying NVDA, bid 0.89 / ask 0.92; NVDA 36 rows, SPY 96, IWM 60, TSLA 90, AAPL 0), whose
root fails the candidate's root-equals-underlying check and with it the whole capture. r67 (`0e7e4f78d954`) rejects such
a row alone (`NON_STANDARD_ROOT`); tests pass (8 failures, all in the morning baseline); NVDA probe on r67: capture OK.
`hotfix -Service realtime-v3-options-lifecycle`, no other service changed.

**R12, Options share exhausted (open, needs the user's yes).** The four names' next attempts (14:46:47-14:47:35) found
the per-underlying share (32 x 3 = 96 slots) used up by their three failed attempts: `PROVIDER_BUDGET_EXHAUSTED`, retry
21:05, preparation DEGRADED. Ledger since 14:25 (`OPTIONS_CHAIN_SNAPSHOT`): 20 COMPLETE OK, 21 FAILED on R11 rows,
503 FAILED `RESERVATION_NOT_USED` (never sent). Options has no Nov 10 authorization; the Nov 9 Options stream `22a41c6b`
is still LIVE (B14a ends it when the Nov 10 stream starts).

**G65, Dashboard refusal (r68, API, 14:53:40).** Read-only probe of the stored 14:35 Dashboard (`scratchpad/dash_probe.py`):
every identity check passes except the member count: 24 tickers, `expected_members` 24, cohort `stock_count` 25. CRWD is
missing from every Dashboard since Nov 9 17:35 (25 through 17:10, 24 from 17:35), after its 17:01 provider-empty minute
(screening "Derived screening source window/basis drift"). r68 (`a2dad713a1eb`) accepts fewer members than the cohort
and labels it (`member_status` PARTIAL, 24 of 25); tests 42 passed. Live 14:53: `/latest-price-date`, `/stocks/context`,
`/tickers/overview`, `/scan/all`, `/tickers/setup-summary` all 200 with 24 rows, status STALE between the hourly
Dashboard publications (no refresh added; the user, 14:5x: skipping the Dashboard refresh is fine).

**R13, workspaces over the bound (r69, Equity scheduler).** Today's windows published 10 ticker workspaces per cutoff
(AAPL, IWM, QQQ, SPY and the 6 sector ETFs) against 25 on Nov 9. Read-only rerun of the 14:50 projection
(`scratchpad/ws_probe.py`, live code): 15 stocks `payload is 133,917-149,057 bytes; a by-reference payload must stay
within 131072`, CRWD at SCREENING. The Financials loaded at 09:17 add about 39 KB per stock (30 reports, 39 fields; AAPL
131,700 bytes with them, 116,819 on Nov 9). r69 (`c18bd575f40d`) keeps the bound and, for a workspace over it, keeps
the newest Financials periods that fit with `fundamental_reports_status` TRIMMED_TO_WORKSPACE_BOUND; unit test added,
tests pass (5 failures, all baseline). The same rerun on r69: 24 workspaces, CRWD only. Financials on AAPL's 14:50
workspace: 30 reports, latest period 2026-09-26 (filed 11-03); ETFs labelled `REALTIME_FUNDAMENTAL_REPORT_PRODUCER_UNAVAILABLE`.

### Nov 10 (15:07-15:36 UTC): Options unblocked (user yes), R16, Options live

User, ~15:07: "yes, lets unblock options". **R12:** r70 (`459c3db1f1ac`) on the Options lifecycle 15:13:02 (the share
query skips `RESERVATION_NOT_USED` slots; unit test; lifecycle tests 57 passed, 1 baseline failure). Then
`runner/r12_live.ps1` (guarded `r12_requeue_options_catalog.sql`: exactly the four rows RETRY / PROVIDER_BUDGET_EXHAUSTED /
due 21:05) moved IWM, NVDA, SPY, TSLA to 15:13:55 (UPDATE 4). CATALOG_QUOTE COMPLETE 15:14:06 (IWM, 12 requests),
15:14:10 (NVDA, 4), 15:14:31 (SPY, 21), 15:14:36 (TSLA, 6): the r67 root filter verified on live data.

**R13 verified.** Equity scheduler on r69 15:08:02 (no restarts). 15:10 window: projection 97 s (cold cache), publish
89 s (the day's evidence for the 14 stocks missing since the open), 189 s in all, 24 workspaces; 15:15, 15:20, 15:25 at
19-22 s.

**R16, CAPACITY_SHARDS (r71, lifecycle, 15:28:03).** From 15:15 every attempt failed `ValueError: activity/momentum
candidate is invalid` and the lifecycle restarted (3 times). Read-only rebuild of the combined capture from the stored
CATALOG_QUOTE evidence (`scratchpad/cap_probe.py`; capture times 14:31-14:32 and 15:14, 9,002 selected contracts): 4
invalid, AMZN 3 and NFLX 1, all deep in-the-money calls with IV near 0.001 and provider delta 1.0000000000004-1.0000000000047.
r71 (`b735a1cee3ab`) treats a delta within 1e-6 of the bound as the bound and drops an out-of-range delta or IV; tests
(79 passed); rebuild on r71: 0 invalid. Live: CAPACITY_SHARDS 9 COMPLETE and SESSION_ACTIVATION COMPLETE by 15:32:47,
preparation ACTIVE, Nov 10 authorization 15:32:47, Options stream `b847f3cc` LIVE 15:32:50, the Nov 9 stream `22a41c6b`
STOPPED `ENDED_BY_NEXT_SESSION` (B14a), first Options window E:15:30 COMPLETE 15:35:51.

Images now: API r68, Equity scheduler r69, Options lifecycle r71, all other services r66, frontend f21.

### Nov 10 (15:36-15:56 UTC): R17 Stock Alerts, R18 O1 packages

**R17 (r72, Stock Alerts, 15:44:03).** From 15:30:30 every batch failed `TypeError: unsupported canonical value: bool_`
in `canonical_stock_alert_research._member_evaluation` (hash of the research-rule state), 29 restarts, no publication for
15:30-15:40. A rule's saved state carried a `numpy.bool_` (the 15:30 batch held the day's first MATCH). r72
(`09a7bf3a07a9`) converts rule results to plain Python values at the rule boundary; new unit test; Stock Alerts tests 47
passed, 1 baseline failure. After 8 lease-handover restarts (expected), 15:45:00-15:45:03 published the missed cutoffs:
15:30 MATCH 1 / NO_MATCH 18, 15:35 and 15:40 NO_MATCH 19.

**R18 (switch, Options window service, 15:48:03).** E:15:35 FAILED in OPTIONS_CYCLE: `ForeignKeyViolation` on
`option_packages_cohort_id_provider_tier_fkey`, key (ccaed5c3 = the Nov 10 Options cohort, MASSIVE_ADVANCED) not in
`rt.provider_tier_cohorts`. `rt.option_packages` has never held a row and no Options-session cohort (Nov 6 b096f5b7,
Nov 9 d338723a, Nov 10 ccaed5c3) is registered there; Nov 9 never selected a package (2,711 NOT_SELECTED, 82
OBSERVATION), so the first SELECTED package today was the first insert. Runbook switch (section 5):
`REALTIME_V3_O1_PACKAGE_SELECTION=DISABLED` on realtime-v3-options-p8-window in `rollout/compose.stable.json` (backup
`compose.stable.before-o1-off-20261110.json`; only that service's environment differs), `hotfix` with the same r66
image, 15:48:03, no restarts. Options windows E:15:44 COMPLETE (O1 COMPLETE_ZERO, 150 evaluations), E:15:50 COMPLETE (219),
the same lane pattern as 50 of 53 Nov 9 windows; Equity 15:45-15:55 COMPLETE in 27-34 s; no traceback in any service
since 15:46.

### Nov 10 (16:20-16:35 UTC): R18, R22, O3 (user: fix, test, deploy)

**R18.** The V3 activation creates the Options cohort (mode LIVE, `advanced_options_automatic_preparation` ~1662) but
never its tier binding, which the P8 enrollment wrote; `rt.option_packages` references `rt.provider_tier_cohorts`. r73
writes `c8_options_tier_binding` at activation (unit test). Today: `runner/r18_register_options_cohort.py ccaed5c3...`
dry run 16:27 (binding `2c9a0174d097`, MASSIVE_ADVANCED / ADVANCED_READY / options MASSIVE_ADVANCED, effective
14:29:59.999999, rolled back); the `--apply` run was held by the session permission check and applied at 16:30:06 on
the user's go. No view reads the table; FKs to it: option_packages, portal_cohort_current, tier activations. O1
selection set back to ENABLED in `compose.stable.json` and the window service recreated on r73.

**R22.** r73 (`6e559f3325c1`) on the Options window service 16:28:04 with a membership diagnostic; first window 16:30:
OS1 members 153, rows 154, missing 0; OS2 members 162, rows 163, missing 0. Duplicate ids in `rt.bar_revisions`
(`(bar_revision_id, observed_at)` key): AAPL 1d 2026-10-19 `5c8495ce` and 2026-10-20 `785a6b5d`, cohort d1baee6f,
each stored at Oct 21 11:01 and 11:05. r74 (`927f2a1a641d`) reads members by id and observed_at; read-only check on
the 16:25 cutoff: old query 154 / 163 rows, new 153 / 162.

**O3.** BLOCKED `CANONICAL_MARKS_UNAVAILABLE`: 29 of the 72 slot contracts have an ADVANCED_NBBO mark today, 43 none;
525 mark rows in all since Nov 9. O3 needs a mark per contract per cycle, which no service produces: market-state work
(V3-1), not a hotfix.

**16:33-16:37.** Options window service: r73 with O1 selection ENABLED 16:33:04, then r74 (`927f2a1a641d`) 16:34:02,
no restarts. First window on r74, E:16:35 COMPLETE (89 s): O1 COMPLETE_ZERO (111 evaluations), **OS1 COMPLETE_ZERO,
OS2 COMPLETE_ZERO** (BLOCKED in every window since Nov 6), O2 UNAVAILABLE (model not accepted), O3 BLOCKED
(CANONICAL_MARKS_UNAVAILABLE; no producer). R22 verified; R18 waits for the first SELECTED O1 package to be stored.

### Nov 10 (17:00 UTC): OS1/OS2 rule review (user request)

Rules (`options/stock_bar_detection.py`, policy `canonical_stock_bar_signal_v2`): band = prior daily close +/- 0.5 x ATR14
(15 RAW daily bars); OS1 previous 5m close inside, latest outside; OS2 outside, pullback on the same side, resume beyond
the band; valid 30 min; an OS invocation needs an O1 flow event on the same underlying and side (CALL up, PUT down), and
the decision needs that contract's participation above baseline. Read-only replay with the live code over today's bars
(`scratchpad/os_replay.py`): 252 cutoff evaluations, no error; 5m bars `DERIVED_FROM_ADVANCED_AM_1M` (28) and
`DERIVED_FROM_CANONICAL_1M` (1), both accepted; 19 stock signals (OS1 up 1, down 7; OS2 down 11). Live evaluations since
r74: OS2 IWM 10 (window 16:35), NVDA 23 (16:45), OS1 SPY 3 (16:55), matching the replay's 16:30, 16:40 and 16:50
signals; all NOT_SELECTED, disposition UNMATCHED, reason `PARTICIPATION_BELOW_BASELINE` (volume/OI about 0.09). No
fault in the rules; one design point recorded as R24 (policy precedence 1h-30m-15m-5m, lane context 5m only).

### Nov 10 (17:19-17:25 UTC): R25 Options STALE banner (user report)

Banner text: "STALE DATA ... 0 sessions behind. Oldest published timestamp: Nov 10, 01:35 PM. Affected timeframes:
options. Retained setup age: 8m." Live `/api/options/flow` at 17:19:47: available, `stale` false, `as_of` and
`generated_at` 17:15, `valid_until` 21:35 UTC (= 01:35 PM PT), `retained_session_date` 2026-11-10. The page
(`fetchOptionsMaterializationStatus`) marks Options STALE when `as_of` is over 5 minutes old and prints `valid_until` as
the oldest published time; with 5-minute releases, cutoffs sometimes 6 minutes apart and 85-106 s windows, every cycle
had 1.5-3 minutes over the threshold. f22 (`2ae2a6a36cb2`): 10-minute threshold, `as_of` shown; frontend build and tests
288 / 243 pass / 40 fail, the identical failure set to f21; the changed and the new test pass.

**R26 (17:27-17:34).** Options Screener built-in tab "Unusual activity: calls" opened with only "Calls" as applied
filters: server screens are applied by id, and the page's query carried only `type` and `sort`; `/api/options/
discovery-catalog` already publishes each screen's `rules`, `caps` and `tiers`. f23 (`7a6f1b8a5657`) shows them in the
Filters side panel (read-only "Built-in screen" section, open) and as chips in the applied-filters row; frontend tests 289
/ 244 pass / 40 fail (the same set as f22); deployed 17:33:02, public domain 302.

**R26 batch deployed on the user's go (17:43-17:4x).** API r75 (`bf21c7faee64`) 17:43:17, no restarts; the catalog
serves `criteria` for both Unusual-activity screens (open_interest >= 500, volume_open_interest_ratio >= 3, calendar_dte
not in 3, 4, day_volume and day_premium by tier); `/latest-price-date`, `/options/flow`, `/stocks/screening/catalog`
200. Frontend f24 (`e9086e71d136`) next. Stock built-ins already fill their fields from the predicate; no change.

### Nov 10 (17:56-18:15 UTC): R27 editable built-in screens deployed; R18 verified with the first Options alerts

User 17:56: every built-in criterion in a field, the tab's purpose above the table; 18:00: "once changes are done deploy
and validate, I authorize". r76 API (`619ead7aa4f0`, `screen_overrides`, `customize_screen`) 18:08:42; on live data:
built-in 62 matched / 20 shown, with its own values as edits identical (same sha256, `customized` true), loosened
128 / 61, unknown field 422. f25 frontend (`cdd9d65f0a31`) 18:13:42; served bundle contains the new panel; public domain
302. **R18 verified:** O1 lane COMPLETE_SELECTED from the 18:05 dataset; 29 SELECTED evaluations, 29 rows in
`rt.option_packages`; the first OPTION rows in `rt.alert_current` (NVDA 17:59, TSLA 18:01, NVDA 18:09, NVDA 18:10). One
breadth capture request timed out at 18:08:36 (7 s, `Timeout`, BREADTH lane); 80 breadth requests then completed
18:11-18:12; window E:18:05 started 18:13:15 instead of about 18:10:30 (its stages took 84 s) and completed.

### Nov 10 (18:08-18:35 UTC): R28 network outage, Options stream backlog, reconnect

Options breadth captures 18:08-18:18 failed (`Timeout`, `ConnectionError`: "Remote end closed connection",
`NameResolutionError` for api.massive.com); DNS and HTTPS from the scheduler worked again by 18:22. Options acquisition
logged `advanced_gateway_pong_late` (pong 60 s late) from 18:16. Options windows E:18:10, 18:15, 18:20, 18:25 stayed
PENDING: `RESIDENT_ROLE_NOT_COMPLETE` for INGRESS, MARKET_STATE, FLOW. Consumers were at the journal head (market-state
3,873,069 / journal 3,873,272; flow 2,150,306 / 2,150,410), but the Options `event_time_watermark` was 461, 457, 470,
464 s behind the clock (18:28-18:30): a fixed backlog on the websocket. Targets need the watermark past the cutoff before
the window deadline (cutoff + 10 min). Reconnect: acquisition hotfix on its r66 image 18:33:28; Options stream LIVE,
connection attempt 3, lag 49 s at 18:34:24 and 0 s at 18:34:45; gap 18:25:35-18:33:33 per contract, ended
(GATEWAY_SESSION_CHANGED); one start-up overload restart 18:34:16.
After the reconnect: E:18:30 FAILED (OPTIONS_CYCLE `ValueError: selected package evidence is incomplete`, the window over
the gap; R31), **E:18:35 COMPLETE 18:42:54**; E:18:10-18:25 still PENDING past their deadlines (R32).

**R29, R30 (r77 API, 18:43:43).** User 18:35: the Options Alerts page showed TSLA 372.5 PUT twice. `/options/alerts/
detector-runs` returned the 18:05 session alert and the 18:10 run's row for the same candidate `0cf47c93`; `repeat_hits` was
hard-coded 0; the stored alert direction was 1 (Bullish) for a bought put (O1 has no direction; `_alert_run_row` defaulted to
+1). r77 (`4629cb87d4ca`): later-run rows fold into the session alert by candidate, real repeat count, O1 direction from the
package legs. Tests: Options alert tests 31 passed. Live 18:44: 4 alerts, no duplicate, repeat_hits 31, TSLA PUT -1, NVDA
CALLs +1. The window writer keeps the old default until the r77 window deploy after the close (the read corrects it).

### Nov 10 (19:10-19:40 UTC): R33 Day History current price and source facts (r78 API, f26 frontend)

User ~19:05: Day History does not fetch the current contract price for P/L and several fields are Unavailable. At 19:10
`/options/alerts/detector-runs` (HISTORY, session rollup) served NVDA 261111C170 with `current_mark` FRESH, market time
18:04:59, age 0: the session list keeps the mark of the window that selected the alert, and `_advanced_current_marks`
skipped every row whose stored mark was FRESH, so it never refreshed. The page showed `-package_price` ("-$645.00").
`original_package.calendar_dte` null for all 4 alerts; legs carry `model_mark` only (no spot, day volume, OI). Source
values exist: `rt.option_flow_events` for the alert's event 39532f87 (NVDA 261111C170, 17:57:58.28): cumulative volume
96,208, prior OI 1,586 (= the O1 volume/OI 60.66); NVDA 1m close 167.81 (bar end 17:57). Lookup cost: flow events 5 ms,
1m close ~35 ms per alert (time-bounded on `observed_at`).
r78 (`9fecfdc35397`): stored mark replaced when the stored quotes are newer, window estimated cost kept (net P/L follows),
age from quote market time, STALE past 900 s; trigger facts (spot, day volume, prior OI) and DTE filled at read, cached by
evaluation id when complete, `source_facts` optional read (INV-9). f26 (`432cd64c2133`): current price per share signed
like the trigger price with the package amount below, age in minutes, O1 stop/target "Not set", management Not applicable.
Tests: Options alert/portal/flow/session suites 177 passed + the stale catalog test updated (`screens` served since
21936b7, failing before this change); new R33 tests pass; frontend 291 tests, 40 failing = the same 40 as f25.
Live 19:38:55: reads session_alerts / current_marks / source_facts AVAILABLE; first read 1.36 s, cached 0.15 s.
NVDA 261111C170 mark 19:38:54 age 1, -171.00 (return -14.3%), spot 167.81, vol 96,208, OI 1,586, DTE 1; TSLA 261120P372.5
-920.00, spot 375.795, vol 5,994, OI 589, DTE 10; NVDA 261120C170 and C175 filled likewise. Frontend served 19:39:55
(Options Alerts chunk has the new labels); public domain 302 (Access).

### Nov 10 (19:40-20:18 UTC): R34 host clock skew unmarked every Options contract; R35 window evidence size (r79, r80 window)

**R34.** O1 SELECTED last at 18:14; after the R28 reconnect every DETECTED evaluation was OBSERVATION (no package): the
package builder refuses an instrument the window's mark producer left unmarked. `rt.option_mark_evidence` per window:
46 (18:05), 40 (18:35), 6 (19:05), 0 after. Slot quotes in `rt.quote_current`: 81 of 86 today's quotes had
`received_at < event_at` (event minus receive median +0.39 s, max +0.49 s), so the producer marked them
`OPTION_NBBO_STALE_OR_NONCAUSAL` (`valuation_advanced_nbbo_v1`: max age 1 s, event must not follow receipt). SNTP from a
container: +0.55 to +0.57 s (time.cloudflare.com, time.google.com); Windows `w32tm /stripchart` +0.55 to +0.58 s;
`w32tm /query /status`: service not started (0x80070426); this session is not an administrator. r79 (`04fd8e69b915`, window
19:59-20:00): skew up to 2 s is taken as received at the event time; the window logs `advanced_options_window_marks`
(marked, unmarked reasons). Tests: mark producer, window, loader, adapter suites 205 passed (3 need the repo-root compose
file, not mounted). Live: 20:00 cutoff marked 33 (39 not fresh), 20:05 marked 35.
**R35.** The first windows with marks again, E:19:55 and E:20:00, FAILED: OPTIONS_CYCLE STAGE_TIMEOUT at 313 s (E:19:50 was
interrupted by the r79 restart; both then MISSED). Window release-to-complete through the day: 78-131 s 16:00-17:15,
158-251 s 17:45-19:49; average evidence document 85 KB (15h), 226 (16h), 413 (17h), 538 (18h), 633 (19h) compressed,
50-90 per window; the window was CPU-bound with no slow query. Each evaluation's evidence embedded every flow observation
the window read (up to 10,000, the session). r80 (`68247aa0720a`, window 20:14-20:15): each evidence carries its own
event's contract observations (underlyer's when the event is not a flow event) plus the package's event. Tests 122
passed. Live: E:20:10 COMPLETE in 138 s (cold start) at 20:17:45; evidence 80-106 KB; 25 SELECTED evaluations
(NVDA 261120C170, NVDA 261111C175, NVDA 261111C170, QQQ 261110P670), 0 selected not stored; OPTION alerts 4 -> 6 (QQQ 670
PUT 0DTE Bearish, NVDA 11/11 175 CALL Bullish). Window restarts 8 on each deploy = start-up `DurableLeaseUnavailable`
until the old lease ends. E:19:50 and E:20:05 stay RUNNING after their owners were replaced (R32).

### Nov 10 (20:17-20:23 UTC): R36 provider disconnect blip, self-recovered

Breadth captures failed: QQQ 20:17:58 `RemoteDisconnected`, SPY 20:19:26 snapshot interval deadline expired, AMZN 20:21:49
`RemoteDisconnected` (ledger ConnectionError 2 -> 4, Timeout 3 -> 4); other underlyers completed in the same cycles. Stocks
stream 93feee57 FAILED 20:22:17 (`ConnectionClosedError`: keepalive ping timeout, 1011, no close frame); new session
0541ceff LIVE 20:22:34; gap 20:22:00-20:22:34 REPAIRED. Options stream LIVE throughout, lag 51 s at 20:22:39, 13 s at
20:23:35. Options windows under r80: E:20:10 138 s, E:20:15 106 s COMPLETE; O1 COMPLETE_SELECTED at 20:20; SELECTED 114,
packages 92, 0 selected not stored. api.massive.com reachable 20:23:58 (1.6 s).
R36 follow-up (20:32): six per-member Stocks AM gaps 20:21:00-20:22:00 on session 0541ceff and the session gap 20:22:00-20:22:34
all REPAIRED by six `ADVANCED_HISTORICAL_1M` requests at 20:25:04-20:25:08 (no operator alert); one more breadth Timeout at
20:23:58 (ledger Timeout 5). Options windows E:20:20 108 s, E:20:25 99 s COMPLETE; O1 SELECTED 163, packages 112, 0 not stored.

### Nov 10 (21:13 UTC): close check, read-only

**Equity.** 77 of 78 cutoffs COMPLETE (0 failed before the close). The close window `advanced-20261110T210000Z` and
retries 1-3 FAILED at 21:00:35, 21:01:17, 21:02:27, 21:04:36: EQUITY_MATERIALIZE `CLOSING_DAILY_INPUTS_PENDING` (projection
and publish blocked behind it), the same path as Nov 9 (COMPLETE at retry4, 22:01). Daily bars at 21:13: RAW_UNADJUSTED
final 25 of 25 (21:00); ADJUSTED final 6 of 25 (AAPL, AMD, AMZN, AVGO, CRWD, GOOGL; latest 21:11). Nov 9's ADJUSTED bars
arrived 21:05-21:56. Close completion is read at 22:06.
**Options.** Windows: 56 COMPLETE, 2 FAILED (15:35 R18, 18:30 R31), 2 MISSED (E:19:55, E:20:00, R35), 4 PENDING past
deadline (E:18:10-18:25, R32), 2 RUNNING with replaced owners (E:19:50, E:20:05, R32). Lanes from 20:20 to the close: O1
COMPLETE_SELECTED, OS1/OS2 COMPLETE_ZERO, O2 UNAVAILABLE (model not accepted), O3 BLOCKED (input evidence absent). O1:
140 packages, 0 selected not stored. Alerts: 9 OPTION (NVDA 261111C170, TSLA 261120P372.5, NVDA 261120C170 149 hits,
NVDA 261120C175 75 hits, QQQ 261110P670 0DTE, NVDA 261111C175, NVDA 261111C172.5, SPY 261110C752 0DTE, TSLA 261111C380),
4 STOCK. Read-time marks at 21:13 from the last quotes (20:59:59, age 845 s, FRESH under 900 s); the two 0DTE packages
ended -87% and -83% gross.
**Streams.** STOCKS 93feee57 14:30-20:22 FAILED (`ConnectionClosedError`, R36), 0541ceff 20:22-21:00 STOPPED; OPTIONS
b847f3cc 15:32, still LIVE at 21:13. Gaps today: 18 Options UNRESOLVED (R28), 7 Stocks REPAIRED (R36).
**After the close.** Fundamentals refresh in the preparation idle slot 21:07: 16 of 16 companies (also 09:xx). Capability
profile `cd7ee87a` PASS, checked 21:01, expires Nov 11 21:01 (previous `1b29f901` expired 21:01).
**Restarts and errors since 14:30.** Stock Alerts 8 (baseline), Options lifecycle 3 (baseline), Options acquisition 1
(18:34 R28), Options window 8 (start-up lease waits after the r79 and r80 deploys), Options FLOW, INGRESS, MARKET_STATE 1
each at 21:06 (R37: role did not stop within its bound at the 21:05 stop). Error lines: scheduler 3 (breadth captures,
R36), acquisition 2, lifecycle 3, the three roles 2 each (R37 traceback). Every other service 0 restarts, 0 errors.
**Matrix.** VERIFIED from this session: A0, A0.1 (session policy), A4.3 (live), A4.6 (three readers live), R7 (live),
G37; header lists the live images per service (r66, r69, r71, r72, r78, r80, f26). New rows R33-R37.

### Nov 10 (22:06 UTC): close completion, read-only

Close `advanced-20261110T210000Z-retry4` COMPLETE 21:41:29 (Nov 9: 22:01); Equity 78 of 78 cutoffs. Daily 1d final for 25
of 25 on both bases (RAW_UNADJUSTED by 21:00, ADJUSTED by 21:36). Options: last windows E:20:40-20:55 COMPLETE (20:46-21:02);
day totals 56 COMPLETE, 2 FAILED (R18, R31), 2 MISSED (R35), 4 PENDING and 2 RUNNING past deadline (R32); Options session
b847f3cc watermark 21:05:00 (row closes when the next session starts, as Nov 9's `ENDED_BY_NEXT_SESSION`). Stock Alerts
last publication the 20:55 cutoff (77 today; Nov 9 also ended at 20:55), Pattern Watch and Dashboard at 21:00. Alerts: 9
OPTION, 4 STOCK. Since 20:50: no new restarts; error lines only the R37 stop-bound tracebacks (FLOW, INGRESS, MARKET_STATE,
2 each at 21:06). All 23 containers up.

### Nov 11 (02:06-02:11 UTC): R20 pre-open dry run built and tested; clock drift

`runner/preopen_dry_run.sh 2026-11-11` (read-only; lifecycle r71, Stock Alerts r72, scheduler r69 images). Clock
(`clock_offset.py`, SNTP from a container): +3.018 s; Windows `w32tm /stripchart` +3.016 to +3.026 s; W32Time Stopped.
Drift 0.39 s per hour since 19:47 (+0.55 s). Readiness: 19 EQUITY, 6 SECTOR_ETF, 9 OPTION_UNDERLYING; Nov 11 Options
preparation `f41f1dea` PREPARING at CATALOG_QUOTE (provider window not open, history ready); cohort not created yet;
capability profile PASS until Nov 11 21:01. Options: captures AAPL 404, AMZN 322, IWM 1,148, MSFT 578, NFLX 84, NVDA 198,
QQQ 2,702, SPY 2,568, TSLA 428 snapshots, 0 rejected rows, 39 provider requests in 17 s; 8,432 candidates, 0 invalid;
allocation `CATALOG_QUOTE_CAPTURE_STALE` for all nine (quotes from the Nov 10 close; expected before the open). Stock Alerts:
the Nov 10 20:55 receipt rebuilt with the RESEARCH rules, 19 evaluations (16 NO_RULE_DUE, 3 BENCHMARK_ONLY) in 3.1 s.
Workspace at Nov 10 20:55: 24 of 25 members (CRWD: derived screening source window drift, as live), largest payload INTC
130,926 bytes of 131,072 (R13 trims to the bound), 83 s. First test run's defects fixed: Docker needs a Windows path for
the env file; the profile check ends at the 21:00 close; Stock Alerts summary by disposition; the workspace step reads the
last intraday cutoff and compares with the live count. Found for Nov 11: the Options cohort is new every session (Nov 6
b096f5b7, Nov 9 d338723a, Nov 10 ccaed5c3) and the lifecycle (r71) does not write its tier binding (r73 does), so the first
O1 package would fail as at Nov 10 15:35 (R18).


### Nov 11 (03:38-04:40 UTC): clock fixed; Options lifecycle and window isolation (r81 code); watch and dry run checks

Clock: the user started the Windows Time service (Running, Automatic, last sync 03:36 UTC, source time.cloudflare.com).
`clock_offset.py` from a container +0.012 s (OK); `w32tm /stripchart` +0.007, +0.001, -0.001 s.

Lifecycle image check: between r71 (live lifecycle) and r80 the only lifecycle-path change is r73's tier binding at
activation (`advanced_options_automatic_preparation.py`, `c8_options_tier_binding` + `persist_tier_cohort_binding_cursor`,
ON CONFLICT DO NOTHING then verified); the other r72-r80 changes are window, mark producer, OS loader, projection, Stock
Alerts research and portal read code. Lifecycle at 03:42: Nov 11 preparation PREPARING at CATALOG_QUOTE, BEFORE_OPEN, 0
provider requests, 0 restarts since 01:56.

R31 cause (read-only, `rt.advanced_options_rolling_checkpoints` Nov 10 18:15-18:50 UTC): first checkpoint after the
18:25-18:33 gap at 18:35 has 64 of 72 contract volume baselines INCOMPLETE, the first one among them; 18:40 4 of 72, 18:45
and 18:50 none. The window's threshold evidence used the first contract's baseline for every evaluation, so a selected
package could not pass and the evidence check failed the window (E:18:30).

r81 code (worktree): R16 isolation (`_isolated_activity_candidates`), R19 in-process retry (`_reconcile_with_retry`), R31
per-package baseline (`threshold_for`, `_baseline_incomplete_instruments`). New tests
`tests/realtime/test_options_lifecycle_isolation_nov11.py` 5 passed; with `test_c8_options_tier_binding.py`,
`test_options_alert_v1_package.py`, `test_advanced_options_lifecycle.py`: 41 passed, 1 failed
(`test_options_preparation_budget_has_local_and_shared_ceilings`, `_capture_after_open_seconds` missing on a test-built
executor; fails the same on HEAD 6bcc19c, baseline).

Watch and dry run: `session_health.sql` adds `OPT_RUNTIME`, `OPT_RUNTIME_ALARM` (over 60% of the release-to-deadline
budget in the last 30 minutes) and `OPT_PAST_DEADLINE`. On Nov 10 data: 20:35-21:00 windows 86-132 s (29-44%); 1 of 11
windows in 19:50-20:20 over 60%; 4 PENDING (from 18:15) and 2 RUNNING (from 19:54) past deadline. Live rows past
deadline Nov 9-10: 7 PENDING, 2 RUNNING. The dry run adds `runner/preopen_prev_session.sql` (previous session's lanes per
detector with WARN for a lane not evaluated in most windows, Options window run time max and p90); test against Nov 9:
O1 COMPLETE_ZERO 50 / UNAVAILABLE 3, O2 UNAVAILABLE 53, O3, OS1, OS2 BLOCKED 53 each (4 WARN lines), COMPLETE windows max
45% / p90 38% of budget.

### Nov 11 (04:09-05:00 UTC): r81 built and rehearsed; deploy blocked by the session's permission check; open items R8-G60

Tests before the build: Options unit tests (107 files, no DB) on the r81 worktree 1,774 passed, 36 failed, 2 skipped;
on HEAD 6bcc19c 1,769 passed, the same 36 failed (P7/P8 canary and deployment fixtures, baseline). r81
`alpha-research-realtime-v3:stable-nov5-r81-1f914c95b980ca5c` = `sha256:4440d08d475fb6ce6e8bcc66c993d62c3dea429156dd962c0a52dc05ecaed7ec`
(image code == worktree, 952 files).

Rehearsal 1, R31 (`runner/r31_replay.py` in r81, read-only on stored windows): Nov 10, 62 windows, incomplete baselines
in 2 (18:35 UTC 64 of 72, first contract INCOMPLETE; 18:40 4 of 72), 348 stored SELECTED packages: old rule 348 pass, r81
unpackages 0 and 348 pass. Nov 9: 55 windows, 1 with an incomplete baseline (1 of 72), 0 SELECTED. PASS: r81 changes no
stored window; it only acts where the old rule failed the window.

Rehearsal 2, the pre-open dry run with every step in r81 (`DRYRUN_IMAGE`, 04:17-04:19): clock +0.068 s OK; readiness PASS
(Options preparation PREPARING at CATALOG_QUOTE, cohort not yet created); Options 9/9 captures, 39 requests, 8,432
candidates, 0 invalid, allocation stale before the open (expected); Stock Alerts 19 evaluations PASS; workspaces 24 of
25 (CRWD by design), largest 130,926 bytes PASS; previous-session lanes printed. Report
`data-audit/preopen-dry-run-2026-11-11-r81-rehearsal.txt`.

Deploy: `deploy_candidate.ps1 -Step hotfix -Service realtime-v3-options-lifecycle -Image sha256:4440d08d...` was refused
by the session's permission check (production deploy); the lifecycle stays on r71 and the window on r80 until the user
runs or allows it.

Open matrix items done in the worktree (r82 code): R8 (test only), R9 (`runner/coverage_preopen.sql`; live 04:21: 25
members incl. SMH, dividends valid through the close 25/25, split newer than coverage 0/25), R10 (docs), R14 (breadth
skips non-standard roots), R15 (summary `unavailable_reasons`, one log line per window), R17 follow-up (one member's
evaluation failure is that member's ERROR), G60 (a spent stream budget is BLOCKED, container unhealthy). Not done: G57
(label is a binding identity checked against images; next single-image deploy), G21 (page retry needs its own ledger
reservation; V3 D9 work).

### Nov 11 (04:53-05:00 UTC): r82 built and rehearsed

Touched-area tests (39 files: Stock Alerts, ticker, screening, scheduler, health, breadth, research, dashboard, equity
interval): r82 worktree 767 passed, 11 failed; HEAD 762 passed, the same 11 failed (baseline; HEAD also cannot collect
`test_equity_interval_derivation_worker.py`, R8). r82 `alpha-research-realtime-v3:stable-nov5-r82-a09b477af93d4491` =
`sha256:ca904df7e54aa07b345fd23e0407c4f3568ca8e4d0710d162e60c50f5fc8f227`; differs from r81 in 7 files (breadth, Stock
Alerts research, fundamentals docstring, ticker projection and repository, ingress health, preparation comment).

Dry run with every step in r82 (04:53-04:56): clock +0.123 s OK; readiness, Options (9/9, 8,432 candidates, 0 invalid),
workspaces (24 of 25, CRWD by design, largest 130,926 bytes) PASS. Stock Alerts PASS with a different result from r81:
19 evaluations, 4 MATCH, 12 NO_MATCH (252 rule checks each), 3 benchmark, 48.8 s. Same step at 04:57 on r81 and on the
live r72 image: 16 NO_RULE_DUE, 3 benchmark, 3.0 s. Cause: the Stock Alerts state is keyed by the detector version
(`rt.detector_state.detector_sha256`), which hashes the rule module's source, so r82's R17 change is a new version with
no saved state and evaluates the whole session from the open. Expected by design; it means a Stock Alerts code change
is deployed before the open or after the close. No member error and no fallback in the rebuild. R31 replay in r82:
Nov 10 348 of 348 SELECTED pass, 0 unpackaged (PASS). Reports `data-audit/preopen-dry-run-2026-11-11-r82-rehearsal.txt`.

### Nov 11 (05:20-06:10 UTC): R21 fault journal and record-only self-heal built (not deployed)

User (Nov 11): implement self-heal and a single fault journal, record-only for the first one or two sessions, then act;
comment where future changes are needed; do not deploy; the watcher keeps monitoring and fixing during the next sessions.

Built: `realtime/fault_journal.py`, `realtime/transient_retry.py`, `realtime/self_heal.py`,
`scripts/run_realtime_v2_self_heal.py`, fault lines at the R15/R16/R17/R19/R31 isolation points and on zero marks;
host `runner/fault_collector.py` and `runner/install_fault_collector.ps1` (not installed). Design choice: crash and
restart detection lives in the collector (Docker state and logs), because the service wrapper `exec`s the service on
Linux and never sees its exit; changing that to a parent process would change signal handling for every service.
Image r83 `sha256:60118b8dc80b` (worktree at 05:50; for the read-only check run only, not deployed).

Collector rehearsal, read-only, scratch output: logs since Nov 10 14:00 of the 21 `stock-screener-realtime-*` containers
-> 24 records, 8 distinct: R19 SerializationFailure x3 (lifecycle 15:43-15:44), R28 BufferFull and GatewayOverload
(acquisition 18:34), R37 x3 (FLOW, INGRESS, MARKET_STATE 21:06), and the lease handovers after the 15:43 and 20:14
deploys (8 each, RECOVERED, "after a deploy"). The window's own Nov 10 failures are not in any log (container recreated
at 20:15); the self-heal checks cover them from the database. Legacy stack containers are excluded.

Self-heal pass in r83 (record-only, read-only, 05:50): WINDOW_PAST_DEADLINE 136 Options windows PENDING/RUNNING past
deadline since Nov 4 (WOULD_ACT MARK_MISSED; R32 is wider than the 9 counted on Nov 10); STREAM_STALE Options stream
b847f3cc open since Nov 10 21:05 (refined to DEGRADED: the next session's registration ends it, B14a); leases, lanes,
preparation and run time clean before the open. Tests: `test_fault_journal_self_heal.py` 10 passed; with the lifecycle,
window package, Stock Alerts and ticker tests 36 passed.
Regression on the final self-heal code (Options + touched areas, 2,551 tests): 2,502 passed, 47 failed, identical to r82
(baseline). Image r84 `sha256:4046b55571da` (refined stale-stream check: the previous session's open stream is
DEGRADED until the next registration ends it) for read-only check runs only, not deployed; pass at 06:10: 136 Options
windows past deadline (WOULD_ACT MARK_MISSED), Nov 10 Options stream open (DEGRADED, B14a), nothing else.

### Nov 11 (06:20-06:50 UTC): V3-1 measurement and design

Read-only benchmark in r84 on the Nov 10 Options journal (`options-acquisition/47d62b40.../options.sqlite`, 4.67 GB):
6,301,830 normalized records (Q 5,849,890, T 451,940), all in shard 0 (72 instruments: the C6 layout had one shard);
busiest minutes 18:02 31,347, 20:59 31,249, 15:46 30,512 records. Per record (10,000-record batch, one CPU): SQLite row
3.8 us, full decode 23.0 us, materialize 9.7 us (per-shard and fan-out equal with one shard). Day estimate: ~350 CPU s
with today's loop (decode twice: consumer and router), ~206 CPU s with one shared read. Against Nov 9's 42% p50 / 86%
p90 of a core, the data work is small; the role loop runs registration, barrier snapshot, lease renew and two SQLite
opens on every iteration (~10/s). Design written: `docs/g102/G102_V3_1_DESIGN.md` (Step 0 profile with py-spy, then
cadence and read-once, capacity replay at 3x, marks at the cutoff offset, shards across processes only if measured
necessary). The window's mark producer reads the latest `rt.quote_current` head at run time (the R34/R35 exposure).

### Nov 11 (06:55-08:10 UTC): V3-1 Steps 1-2 behind a switch; package r85 built and rehearsed (not deployed)

User (Nov 11): one package for every backend service; V3-1 behind a switch so a live run can fall back; build and rehearse.

Code: `REALTIME_V3_MARKET_STATE_LOOP` (`P8` default, `V3`) on the market-state role (`advanced_options_p8_roles.py`
`_run_market_state`); V3: registration refresh and barrier snapshot at most once a second (snapshot also when the offset
moves), lease renewal every 5 s, one shared `read_normalized_after` per pass for the shard consumers and the detector
router (`plan_slice(records=...)`), `PersistentRawJournalReader` (one read-only SQLite connection kept open; reads go
through `RawJournalReader._reader_connection`), idle wait 0.1 s growing to 0.5 s. Tests: consumer equivalence (shared
read = own read, split passes, misaligned records refused, kept-open reader = per-read reader), the loop test
parametrized over P8 and V3 (the P8 call is unchanged), router batch independence already covered by the restart test.

Replay (r85, read-only, Nov 10 journal 6,301,830 records, live's shard map `85b9dab6`): P8 read path (631 passes of
10,000, 275 s) and V3 read path (kept-open connection, 1,234 uneven passes, 252 s) both give state `61d34c5a...` = the
final checkpoint live stored at 21:05 Nov 10 (first run differed only in stream_session_id because the harness skipped
the journal source's mapping to the registered session b847f3cc; with the mapping, identical).

Full-stack copy rehearsal (runbook 6, `scratchpad/r85/rehearse_r85.sh`): live backup `stocks_db_backup_v3-r85-
rehearsal-20261111T073034Z.dump` (3.61 GB, 430 s, sha256 B3046D8D...), restore 353 s into `test_realtime_v2_r85`
(458,483 bar heads, latest window 21:41:29 Nov 10), hardening 11/11 pass, journals 4.4 GB incl. the Nov 10 Options
folder; isolation proven (host, provider and internet unreachable). 19 services on r85, market-state on V3: after 2
minutes all running, 0 restarts, 0 error lines; Options roles READY_WAITING_FOR_SESSION (the loop runs only in a
session). API smoke: 153 routes, 126 OK on both, no route with a different status from live. Stop: 18 exit 0,
Options acquisition 241 (the known whole-stack-stop pattern). Logs in `scratchpad/r85/r85-evidence`.

### Nov 11 (08:41-09:00 UTC): pre-validation and r85 deploy to every backend service (market-state on P8)

New session (handover `HANDOVER-20261111.md`, user's go for this plan). Pre-validation, read-only: r85 `74e2c255d715`
and r84 `4046b55571da` present; worktree source hash recomputed 07e0c9782c563d32 (= r85 label); `compose.stable.json`
market-state has no `REALTIME_V3_MARKET_STATE_LOOP`; clock host `w32tm` +0.25 s, container `clock_offset.py` +0.242 s
(W32Time syncing; 08:58 after its 08:51 sync +0.20 s); targeted tests 28 passed (consumers, self-heal, lifecycle
isolation) and 2 passed (`p8_docker_runtime -k market_state`); live images == `rollout/pre-r85-images-20261111.txt`,
21 containers, 0 restarts. GO.

Deploy: `preflight` 08:44 (no pending migrations, latest 052, audit passed, dry runs PASS, no legacy containers);
`g102-services -Image sha256:74e2c255d715...` 08:44-08:57 (19 services, exit 0). Verify (runbook 3.7) 08:57-08:59:
19 backend services on r85, frontend f26 and cloudflared unchanged; 0 restarts (no lease-handover restarts this time);
0 error lines in any service except the API's Dashboard read (below); 20 leases renewing; Options roles
READY_WAITING_FOR_SESSION, lifecycle PREPARING at CATALOG_QUOTE (provider window not open, history ready); API smoke
153 routes, 126 OK before (r78) and after (r85), no route changed status. The 5 portal 503s (`/latest-price-date`,
`/stocks/context`, `/tickers/overview`, `/tickers/setup-summary`, `/scan/all`: Dashboard retained read "identity/
checksum/clock drift", G65 pattern) are the same on r78 before the deploy; watched at the first Nov 11 Dashboard
publication. First fault-collector pass (`--since 6h`, self-heal in r85): WOULD_ACT MARK_MISSED 136 Options windows past
deadline (R32, known), Nov 10 Options stream b847f3cc open (B14a, known), the API ValueError above, one host SNTP sample
+0.572 s (not repeated: the same function gives +0.195 to +0.229 s a minute later).

### Nov 11 (09:00-09:35 UTC): R39 Dashboard close publication refused (-0.0); page dependency map; r86 on the scheduler

Overnight, five API routes return 503 ("Dashboard stored publication identity/checksum/clock drift"), the same on r78
before the deploy and on r85 after. Read-only probes (r85): every Dashboard Nov 9 20:35 - Nov 10 20:35 passes its
payload checksum; both Nov 10 21:00 rows (`7c6afe09`, `c8214049`) fail; setting exactly three `0.0` leaves back to
`-0.0` (`overview[7].chg_pct`, `scan.fibonacci.results[4].confirmed_legs[0].distance_pct` and its
`scanners.SCAN_FIBONACCI_1D_5` copy) reproduces the stored `payload_sha256` of both. Postgres numeric has no negative
zero. The workspace summary already strips it (`_without_negative_zero`); the Dashboard producer did not.

User: the Dashboard page may go without data, but Tickers Overview and every other page must keep working and
refreshing. Dependency map (frontend f26 realtime build, API r85): the Dashboard is one stored bundle; the API serves
`/tickers/overview`, `/scan/all`, the six `/scan/<scanner>` routes, `/tickers/setup-summary`, `/stocks/context` without a
ticker and `/latest-price-date` from it. Realtime pages that read it: Dashboard; Gaps/FVG, MA Crossover, Momentum
Pullback, Bearish Bounce, Fibonacci (`getDailyScannerPublication`: `/strategies/daily` first, 503 since no strategy
release was ever produced, G38, then the Dashboard's scanners); Sector Intelligence breadth panel. Tickers Overview
reads `/tickers/workspace-publication` (per window) plus live prices; ticker pages, Market Regime/Conditions, Options
and Stock Alerts do not read it. Switching the Dashboard off would break the five strategy pages, so it stays.

Measurements (Nov 10, read-only): intraday Dashboards 14:35-20:35 have byte-identical overview, scan, scanners and
references (daily bars through the previous session; AAPL shows the Nov 9 close all day); only ids, market times and
setup evidence ids change. Equity windows: PORTAL_PUBLISH median 95.6 s (max 143.6) with the Dashboard, 0.9 s without;
window release-to-complete median 170 s vs 44 s (deadline 300 s). The workspace summary's overview refreshes chg% every
window from the live quote, but its close/OHLC/volume come from the 1d chart's last bar (AAPL 350.69, 46.5 M all day,
labelled Nov 10): noted for a later row; the realtime Tickers Overview overlays live prices on top.

r86 `alpha-research-realtime-v3:stable-nov5-r86-410fa33bc3c022ba` = `sha256:ae62fbab8c5c8d49d0f6e50ea8b299028f71699dee33da0307b5666b866518c6`,
differs from r85 in `realtime/dashboard_projection.py` (`without_negative_zero` on member lists and payload before
identities and checksums), `scripts/run_realtime_v2_advanced_equity_window.py` (`REALTIME_V2_DASHBOARD_REFRESH=SESSION`:
no hourly rebuild; first window and close still build) and the new test file. Tests: `test_dashboard_refresh_nov11.py`
9 passed; stage runners, window script, Dashboard/All Tickers/daily scanner vertical slices, manifest bounds, reference
continuity, screening known-by: 391 passed, 3 failed, identical to the r85 worktree baseline. Rehearsal, read-only, the
Nov 10 21:00 Dashboard rebuilt from live inputs (known_by 21:41:29; manifest input_sha256 == stored): r85 checksum does
not survive a jsonb round trip; r86 survives and its payload equals the stored payload. Deploy 09:32-09:33:
`compose.stable.json` scheduler env `REALTIME_V2_DASHBOARD_REFRESH=SESSION` (backup
`compose.stable.before-dashboard-session-20261111.json`), `hotfix -Service realtime-v2-advanced-scheduler -Image r86`;
healthy, 0 restarts, switch in the process environment. Rollback: hotfix the scheduler to r85 (ignores the switch).
`candidates.txt`'s last line is now r86: pass `-Image` on every hotfix (the V3 flip uses r85 explicitly).

### Nov 11 (09:40-10:00 UTC): per-ticker readiness for the Nov 11 run (R40), read-only

`runner/ticker_readiness.py 2026-11-11` in r85 (report `scratchpad/ev/ticker-readiness-2026-11-11.txt`). 25 members
(19 EQUITY incl. SPY/QQQ/IWM, 6 SECTOR_ETF), screening window 2025-09-26..2026-11-10 = 283 adjusted sessions. Every
member: adjusted 1d 283/283; raw 1d 20/20; newest weekly 11-06, monthly 10-30; Nov 10 RTH 1m/5m/15m/30m/1h
390/78/26/13/7; 1h in 20 of the last 20 sessions; split coverage from 2025-09-26 through Nov 11 and dividend coverage
through today's close; earnings coverage valid to Nov 13 21:06 for all 16 stocks; Fed coverage valid to Nov 17;
fundamentals 79-83 reports per stock. Ticker workspaces at the Nov 10 close: 24 of 25.

Gap: CRWD. Intraday holes in the last 20 sessions: only CRWD Nov 9 1m 389/390. Its Nov 9 5m 17:00-17:05 head is DERIVED
from 4 one-minute bars (17:00, 17:02, 17:03, 17:04; no trade in 17:01, the provider has no bar); 15m 17:00 (3 sources),
30m 17:00 (6), 1h 16:30 (12) and raw 1d (78) derive from complete 5m. The screening rule requires contiguous sources, so
CRWD has no screening result and no workspace (summary and Dashboard list it unavailable; the r69 summary stored no
reason, R15 adds it from r82). The raw intraday window is 90 sessions (2026-07-07..2026-11-10), so Nov 9 stays inside it
until 2027-03-22. Informational: the 8 funds have no sector/industry in their reference rows. By design (filtered): page
labels `calendar` and `profile_fields` (static `MISSING_PRODUCERS`, never cleared), fund fundamentals, monthly MA100/MA200.
Options: 9 underlyings, Nov 11 preparation PREPARING at CATALOG_QUOTE (normal before the open; the 13:31 dry run checks
captures and candidates).

### Nov 11 (10:00-10:41 UTC): CRWD quiet minute fixed (r88 on the scheduler)

Provider re-check (read-only, 3 requests, 10:05): CRWD Nov 9 1m aggregates 16:58-17:05 have no 17:01 row; 50+ trades in
17:01 are all odd lots (condition 37, 0-22 shares), excluded by the provider's aggregation; the provider's 5m 17:00 bar
volume 22,615.04 (n 816) equals our derived 5m. CRWD Nov 10 (read-only): 1m 390/390 (389 stream + 1 REST repair at
20:21), 5m/15m/30m/1h 78/26/13/7, raw 1d (derived) and adjusted 1d (provider) same OHLC 224.72/230.42/218.41/225.76,
volume 4.80 M (RTH minute sum) vs 6.15 M (provider daily; the same basis gap on every ticker, AAPL 21.0 M vs 27.9 M);
no CRWD snapshot on Nov 10 (the Nov 9 minute).

The live stream finalizer already derives a 5m window with no-trade minutes by the sparse policy
(`derive_advanced_sparse_minute_window`, only with gap-free MASSIVE_ADVANCED coverage; the bar records
`missing_no_trade_starts`, quality `ADVANCED_NO_TRADE_MINUTES`); two readers refused it: screening (contiguous sources)
and the ticker workspace (exactly 5 sources, dense re-derivation). r87 `68d306c23d57` (screening only) moved CRWD's
refusal to the workspace; r88 `sha256:10cba306c446bc295c01b242c5cc628f03177a342565de068301c82d437ddab4` (= r86 +
`screening_projection.py`, `ticker_projection.py` + 2 tests) accepts them: workspace step on live inputs 25 of 25 (live
24), Nov 10 close Dashboard rebuilt with 25 tickers. Tests: quiet-minute 7 passed (5 fail on r85), sparse verifier 3
passed; 29 files 639 passed, 11 failed = r86 baseline. Deploy 10:38-10:40 (`hotfix -Service realtime-v2-advanced-scheduler
-Image r88`; compose keeps `REALTIME_V2_DASHBOARD_REFRESH=SESSION`): 8 lease-handover restarts
(`DurableLeaseUnavailable`, known), then running and healthy from 10:39:55. Rollback: scheduler to r86 (CRWD out again)
or r85. Opening Equity windows for reference: Nov 10 14:35 188 s of 300 (projection 95 s, Dashboard 91 s), Nov 9 14:50
192 s, Nov 10 15:10 (after a scheduler deploy) 189 s; warm windows 19-31 s.

### Nov 11 (13:31-13:34 UTC): pre-open dry run on the live images: PASS

`runner/preopen_dry_run.sh 2026-11-11` (read-only), images lifecycle/stock-alerts r85 `74e2c255d715`, scheduler r88
`10cba306c446`; 2 min 37 s; log `ev/preopen-dry-run-20261111.log` (scratchpad). **Clock** WARN +0.355 s (host slow;
W32Time RUNNING; r85 tolerance 2 s; 0.20 s at 10:30). **Readiness** PASS: desired members EQUITY 19, OPTION_UNDERLYING
9, SECTOR_ETF 6; Options preparation PREPARING at CATALOG_QUOTE (cohort is created at SESSION_ACTIVATION); capability
profile valid to 21:01. **Options** PASS: 9 captures OK (43 requests, 9,514 snapshots, 0 rejected rows, 0.4-6.6 s
each), candidates 9,106 / 0 invalid, allocation 72 contracts (NFLX 3, QQQ/SPY/TSLA 8, others 9). **Stock Alerts** PASS:
receipt cutoff Nov 10 20:55, 19 evaluations (MATCH 4 - expected after the r85 version reset; NO_MATCH rules evaluated 12;
benchmark only 3), 42.7 s. **Workspaces** PASS: cutoff Nov 10 20:55, 25 of 25 built, 0 unavailable (live published 24 at
that cutoff - CRWD, fixed by r88), 77 s; largest INTC 130,926 / NFLX 130,895 / ORCL 130,801 bytes vs the 131,072 bound
(the Financials fitter trims to the bound, G37; 0 over). **Previous session (Nov 10)**: O1 COMPLETE_SELECTED 11 /
COMPLETE_ZERO 45; O2 UNAVAILABLE 56 (model not accepted) and O3 BLOCKED 56 (no mark producer) - by design, the 2 WARN
lines; OS1/OS2 BLOCKED 11 (before r74 at 16:35) / COMPLETE_ZERO 45; windows COMPLETE 56 (max 93%, p90 58% of budget),
FAILED 2 (R18, R31), MISSED 2 (R35), PENDING 4 and RUNNING 2 past deadline (R32), WARN over 60% in 5 windows - all
known Nov 10 rows. Nothing to fix before the open.

### Nov 11 (14:27-14:31 UTC): detectors health check failing on a query bug (R42)

Watch 14:27: `realtime-v2-advanced-detectors` `Up 6 hours (unhealthy)` (restarts 0); the health log shows
`_detector_health` -> `cursor.execute` `IndexError: tuple index out of range`. Cause: the checkpoint count keeps the LIKE
pattern `'advanced-equity-detector-shadow:ADVANCED_EQUITY_S_:%:0'` as a literal in a parameterized statement (psycopg2
2.9.9 reads the `%`); the code path runs only when a cohort is active, so the container turns unhealthy every session
since 583fb1b (Nov 7), unnoticed before the R21 collector. The detectors themselves run normally (logs IDLE before the
open). Nothing acts on this health (no `depends_on` in `compose.stable.json`, no autoheal). Fix in the worktree: the
pattern is a parameter. Fixed probe on live (one-off r85 container, worktree script, read-only) 14:30:24: `READY`,
expected consumers per journal 50, active journals 1, leases 50 of 50, durable checkpoints 0. Tests (r88 image, worktree
mounted): new placeholder tests 2 passed (2 failed with r85's script), with `test_run_realtime_v2_advanced_options_os_detectors.py`
15 passed. Deploy after the close (probe-only change; no in-session restart of the detectors).

### Nov 11 (14:35-14:44 UTC): first window on r88 (R39, CRWD): checks

Read-only probe (`first_window_probe.py`, r88 image) 14:42: Equity E:14:35 COMPLETE **174 s of 300** (input gate 0,
materialize 2, projection 91, PORTAL_PUBLISH 80 with the Dashboard; Nov 10 first window 188 s); E:14:40 COMPLETE 21 s
(projection 18, PORTAL_PUBLISH 1: no Dashboard, SESSION refresh). **CRWD**: TICKER_WORKSPACE_V1 at 14:35 and 14:40;
workspace summary 14:35 and 14:40 overview 25, unavailable members none (Nov 10 live: 24). **Dashboard** Nov 11 14:35
(`58674ec2`, cohort `effad99e`): stored checksum matches the payload after jsonb, overview 25 rows (Nov 10 21:00 rows
`7c6afe09`/`c8214049` still fail, as recorded for R39; they are no longer the newest). API smoke `r88-first` 14:43
(`ev/api-smoke-r88-first-live-144351.json`): 131 of 153 routes 200 (08:58: 126); `/tickers/overview`, `/scan/all`,
`/latest-price-date`, `/stocks/context`, `/tickers/setup-summary` 503 -> 200; the rest as before (13 x 404 legacy paths,
8 x 422 without parameters, `/strategies/daily` 503 G38). Options attached to E:14:35 and E:14:40 after the first Equity
window (by design: the Options scheduler waits for a COMPLETE window with EQUITY_MATERIALIZE); first Options window all
stages COMPLETE. 15:44 check: E:15:35 (first hourly :35) COMPLETE 37 s, PORTAL_PUBLISH 1 s, no DASHBOARD_V1 after 14:35;
windows 14:40-15:40 21-68 s (Nov 10 median 170 s with the hourly Dashboard), PORTAL_PUBLISH 1 s each.

### Nov 11 (14:57 UTC): watch - lanes normal; retention restarted once (R43)

Lanes at E:14:55: O1 COMPLETE_SELECTED (10 selected, 0 not stored), OS1 and OS2 COMPLETE_ZERO (OS2 BLOCKED
`E_CONTEXT_INCOMPLETE` through 14:45 by design: it needs 3 current-session 5m bars, OS1 needs 2), O2 UNAVAILABLE, O3
BLOCKED `CANONICAL_MARKS_UNAVAILABLE` (design). Options windows 14:40-14:55: 64, 43, 42, 41 s (21%, then 14%), alarms 0;
Options stream `8c3fe766` LIVE, lag 0. Market-state CPU (P8) 29-34%. `realtime-v2-retention` restarted 14:51:45 (exit
0, not OOM, healthy after; first cycle 14:51:47 SUCCESS, 0 alarms); R43. Acquisition memory 155 MiB (14:27) -> 472 MiB
(14:47) -> 651 MiB (14:57): watch the trend.

### Nov 11 (14:49-15:00 UTC): V3-1 Step 0 profile **P8 #1** (market-state on the P8 loop, r85)

`runner/profile_options_roles.sh 180` (py-spy `--nonblocking`, 180 s per role, docker CPU every ~5 s); log
`ev/profile-p8-1.log` (scratchpad); flamegraph data `p8-<role>-20261111T1449.txt`. Session time 14:49-14:58 (open + 20
min, busy). **market-state**: 3,855 samples, 3,844 active (idle/sleeping 0.3%): journal read (SQLite) 54.9%, hash/JSON
25.9%, window stages 7.0%, DB write (state, checkpoints) 3.3%, DB other (psycopg2) 2.9%, record decode 2.9%, materialize
1.5%, detector routing 0.5%, barrier snapshot 0.4%, registration query 0.3%, lease 0.0%; top leaves `<genexpr>` 27.5%,
`read_normalized_after` 16.1%, `iterencode` 10.6%, `_canonical_value` 5.8%; docker CPU mean 46.0%, median 37.6%, max
86.3% (29 samples; 74-86% for ~30 s). **flow**: 960 samples: flow 48.3%, hash/JSON 30.2%, DB other 12.3% (top leaves
`iterencode` 15.8%, `load_downstream_bindings` 12.9%, `transaction` 11.8%); CPU mean 13.1%, median 9.4%, max 61.0%.
**window**: 97 samples: DB other 52.1%, window stages 30.2%, hash/JSON 13.5%; CPU mean 1.6%, median 1.0%. Reading:
market-state never idles on P8, and the SQLite journal read plus hashing take ~81% of its samples (per-iteration
re-read, the Steps 1-2 target); decode and materialize are small (~4%).
15:07 watch: OS1 COMPLETE_SELECTED at E:15:05 (first OS1 signal of the day), O1 14 selected / 7 packages / 0 not stored,
5 OPTION alerts; Options windows 41-50 s (14-17%). Acquisition docker memory 799 MiB is page cache of the journal it
writes: the acquisition process RSS is 234 MB (no container memory limit); not a leak.

### Nov 11 (15:10 UTC): O3 - not evaluated, two input gates (R44; user question)

Read-only. O3 lane reasons: Nov 9 `CANONICAL_MARKS_UNAVAILABLE` 15:05-15:20 then `O3_ACCEPTED_INPUT_EVIDENCE_ABSENT`
15:35-20:17; Nov 10 marks 15:35-18:05 (30 windows), accepted-input 18:10-21:00 (26); Nov 11 marks + E context 14:40-14:45,
marks 14:50-15:10. Marks (`rt.option_mark_evidence`, ADVANCED_NBBO, MASSIVE_ADVANCED): 340 since 14:30, one per contract
per 5-min cycle at :x4:59/:x9:59 for 43-58 contracts; today's slot set (`566cc5a4`, activated 14:29) 72 contracts, 71
with a mark, missing `O:AMZN261127P00272500`; Nov 10's slot set: last first-mark 18:09. The accepted-input gate needs
CONFIRMED `ADVANCED_OPTIONS_O3_INPUT` events at the cutoff; none exist (no producer). O3 has not run on any live day.
15:30 (user: investigate O3): O3 = `o3_options_first_credit_v2` credit verticals (7-30 DTE, legs OI >= 500 / volume >= 50,
the activity contract a leg, canonical stock trend confirms). The developer forward path builds and assesses them; in the
advanced path the only O3 code is the lane's loader (`_load_o3`), fed by `ADVANCED_OPTIONS_O3_INPUT` events that nothing
writes. Live packages: 263 ever, all one leg (O1). Today's slot set: 0 DTE 17, 1-6 DTE 21, 7-30 DTE 34 (17 C / 17 P).

### Nov 11 (15:20-15:44 UTC): R45/R46 Options Alerts tabs and one row per contract and side (r89 API, f27 frontend)

User 15:20 (screenshot 10:14 ET): TSLA 390 CALL listed three times (O1 09:41, OS1 10:05, OS2 10:14 ET), and Latest Run
and Day History show the same rows. Causes: `rt.alert_current` keeps one alert per detector and contract (G100; R29
folds only the same candidate); `AdvancedOptionsPublication.detector_runs` ignored `request.scope`. Change (read side
only): with the session list, rows are grouped by contract(s), side and direction (first trigger is the alert; later
detectors become `confirmations`, `detectors` lists them, hits add up) unless a detector filter is set; LATEST = alerts
triggered or hit in the latest run; HISTORY = alerts first triggered before it, latest run withheld (`withheld_run`) for
the current session (America/New_York date of the read cutoff), as Stock Alerts does; without the session list the
release rows are served unchanged. Frontend: the row label lists the detectors (`O1 + OS1 + OS2 / Bullish`); Day History
names the withheld run. Tests (r88 image, worktree mounted): new `test_options_alert_tabs_nov11.py` 5 + session alert
tests 12 passed; 17 Options alert/portal files 312 passed, 0 failed. r89
`alpha-research-realtime-v3:stable-nov5-r89-6dd26e96c72f0472` = `sha256:4d82ad763a4500a90d04ed0e596a88f8e0a028eade467369fcc35bbfbfaa6190`
(= worktree: r88 + R42 + R45/R46). Shadow API on r89 beside live (no lease; read-only) 15:37: LATEST 2 rows (IWM
283C; SPY 757C with O1 + OS2 + OS1), HISTORY 4 rows (TSLA 390C O1 + OS1 + OS2 8 hits, NVDA 172.5P, SPY 761C, TSLA 390P),
latest run `7778c7bf` withheld; live r85 returned the same 10 rows for both. Deploy: `hotfix -Service
realtime-v2-advanced-api -Image r89` 15:37:54-15:38:40 (healthy); API smoke `r89-api` 131 of 153 (no change); live reads
match the shadow. f27 `sha256:a639097fefaa5937d7e047013d44f56c0927a2f1fafcf6544ad422aa4aa31307` (frontend tests 291:
246 pass, 40 fail = f26, same failing tests); `g102-frontend` refused in protected hours, so `hotfix -Service
realtime-v2-advanced-frontend -Image f27` 15:42:15-15:43:40 (healthy). Domain 302 to Access, tunnel no errors, frontend
and `/api/` proxy 200. Rollback: API to r85 `74e2c255d715`, frontend to f26 `432cd64c2133`.

### Nov 11 (15:44-15:55 UTC): V3-1 Step 0 profile **P8 #2** (market-state on P8, r85; started 15:44 after the r89/f27 deploys)

`profile_options_roles.sh 180`; log `ev/profile-p8-2.log` (scratchpad). **market-state**: 4,142 samples, 4,134 active
(idle 0.2%): journal read (SQLite) 55.1%, hash/JSON 26.4%, window stages 6.6%, DB write 3.6%, record decode 3.0%, DB other
2.9%, materialize 1.4%; CPU mean 48.8%, median 39.5%, max 86.2% (#1: 46.0 / 37.6 / 86.3) - same shape as #1.
**flow**: 1,665 samples: flow 47.9%, hash/JSON 41.2% (#1 30.2%), DB other 6.4%; top leaves `load_downstream_bindings`
27.9% (#1 12.9%), `iterencode` 23.9%, `raw_decode` 10.4%; CPU mean 18.7%, median 13.3%, max 60.7% (#1: 13.1 / 9.4 / 61.0):
flow's per-iteration read of the session's downstream bindings grows with the windows bound so far (P8; not in the V3-1
Steps 1-2 scope, a V3-1 follow-up). **window**: 110 samples: DB other 55.5%, window stages 31.8%; CPU mean 2.3%.

### Nov 11 (15:55-16:18 UTC): API segfaults on the Options Flow breadth read (R47); r85 rollback and r89 back

Watch 15:57: `realtime-v2-advanced-api` (r89) restarted twice, Docker `die` exit 139, no traceback; nginx: both times
`GET /api/options/flow?cohort_id=566cc5a4...` (Options Alerts page, polled every ~2 min) "upstream prematurely closed".
Rolled the API back to r85 15:58:39-15:59:21 (fallback, cause unknown then). Shadow APIs with `PYTHONFAULTHANDLER=1`, the
same read in a loop: r89 segfault after 9 reads (GC during json encode in `breadth_screener_row`), r85 after 7 (psycopg2
`RealDictRow.__setitem__` in fetchall) - present before r89. API back to r89 16:05-16:08 (R45/R46 again). Plain process
probes (`breadth_crash_probe*.py`, read-only): dict cursor + row builder crash at iteration ~12; fetch only 30/30; tuple
cursor + builder 30/30 once, crash once in 40; builder over in-memory rows crash once in 60, 80/80 another time; parts
alone and pickled copies 80/80. Live 16:08-16:17: 5 Flow reads 200, 0 restarts. Intermittent native memory corruption;
diagnosis after the close (R47).
16:27 watch: third R47 crash at 16:21:29 (API r89, `GET /api/options/flow` 502, restarted, healthy); about 1 crash per 7
Flow reads while the Options Alerts page is open (15:45-16:21: 3 of ~20). No Flow reads after 16:21.

### Nov 11 (16:54-17:11 UTC): V3-1 flip - market-state on the V3 loop (r85)

Preconditions held (market-state r85 `74e2c255d715`, Options windows COMPLETE, 16:47 watch). `rollout/compose.stable.json`
backed up to `compose.stable.before-v3-loop-20261111.json`; one key added to `realtime-v3-options-p8-market-state`
environment: `"REALTIME_V3_MARKET_STATE_LOOP": "V3"` (diff: that line only; file kept CRLF, indent 1). `hotfix -Service
realtime-v3-options-p8-market-state -Image r85` 16:54:36-16:59:59 (waited for the quiet part of the cycle; recreate 16:58:00,
started 16:59:02); 8 lease-handover restarts (`DurableLeaseUnavailable`, known), running from 16:59:33; env
`REALTIME_V3_MARKET_STATE_LOOP=V3` in the container. The role waited in `_wait_v3_binding` until the next window's
binding opened at its O cutoff (E:17:00 -> O 17:05:00, E+300 s): `advanced_options_market_state_loop mode=V3` 17:05:00.
Shard lease `advanced-options:566cc5a4...:2026-11-11:market-state` renewing (lease_until 17:05:50 at 17:05:23). First V3
window `4fe84b7b` (O 17:05:00): deferred 1 s on MARKET_STATE, marks at 17:05:17 (40 marked), COMPLETE 54.4 s (watch 63 s
incl. deferral, 21%); lanes O1 COMPLETE_SELECTED, OS1/OS2 COMPLETE_ZERO, O2/O3 as designed; O1 51 packages, 0 not stored.
Second V3 window `f0ac34ae` (O 17:10:00): marks 17:10:10 (38 marked), COMPLETE 40.4 s. market-state docker CPU every ~11 s
17:06:13-17:10:33 (24 samples): mean 18.1%, median 16.8%, 9-25% between windows, 33-35% in the 17:10 window (P8 #1/#2:
mean 46.0/48.8%, median 37.6/39.5%, peaks 86%). 0 tracebacks and no restarts since 17:05. Fallback not needed (backup kept).
17:18-17:26 (R47): the crash no longer reproduces: the dict-cursor probe 30/30 three times (default allocator twice,
`PYTHONMALLOC=malloc` once) and 25/25 under `PYTHONMALLOC=debug` (nothing reported); at 16:09-16:14 it crashed about every
12 reads. Breadth is recaptured every 5 min (all 9 underlyings), so the trigger may be data-dependent. A per-capture cache
would save little (polls every 2 min). Catcher started 17:26 (`r47_catch_loop.sh`, read-only, until 21:05): every 5 min one
breadth read saved to `ev/r47/breadth-current.pkl` and 40 in-memory replays; on a crash the rows are kept for offline replay.

### Nov 11 (17:57-18:08 UTC): V3-1 Step 0 profile **V3 #1** (profile #3; market-state on the V3 loop since 16:59)

`profile_options_roles.sh 180`; log `ev/profile-v3-1.log` (scratchpad); R47 catcher running alongside (one read per 5 min,
`--cpus=1`). **market-state**: 3,234 samples (P8 #2: 4,142; py-spy records active samples only, so ~36% vs ~46% of the
time on CPU): hash/JSON 49.1% (`iterencode` 17.4%, `_canonical_value` 13.3%, `<dictcomp>` 7.0%, `_quote_document` 3.3%),
journal read (SQLite) 22.9% (P8 55%; `read_normalized_after` 5.4%), DB write (state, checkpoints) 11.6% (P8 3.6%), window
stages 10.4%, DB other 2.4%, decode 1.2%, materialize 1.1%; docker CPU mean 38.1%, median 36.9%, max 66.0% (P8 #1/#2 mean
46.0/48.8, median 37.6/39.5, max 86). The re-read is gone; per-record hashing/JSON of quote documents and state writes are
now the cost (docker CPU 17:06-17:10, just after the flip: mean 18.1%). **flow** (P8 loop): CPU mean 32.7%, median 30.0%,
max 56.5% (P8 #1/#2 13.1/18.7): `load_downstream_bindings` 37.9% (#2 27.9%), `iterencode` 29.5%, `raw_decode` 15.8% - the
per-iteration read of all downstream bindings keeps growing with the windows bound. **window**: 441 samples, CPU mean 4.3%
(hash/JSON 40.4%, DB other 30.4%). Options window run time 43-78 s (14-26%) 17:30-18:05.

### Nov 11 (18:15 UTC): Options Flow shows 8 contracts after a failed breadth capture (R48; user question)

`breadth_flow_data` uses breadth for a ticker only when its latest capture is COMPLETE; otherwise that ticker keeps its
streamed contracts (allocation: SPY/QQQ/TSLA 8, NFLX 3, others 9) and, for the selected ticker, the page shows the streamed
matrix ("latest complete matrix"). Captures not COMPLETE today: SPY 16:02:32 (ConnectionError), AMZN 18:06:26
(ConnectionError), SPY 18:13:21 (ConnectTimeout), TSLA 18:13:21 PARTIAL (DEADLINE), all `api.massive.com`; the next
capture 5 min later was COMPLETE each time (AMZN 18:11:31 758). No whole-page breadth failures in the API log since 16:08.

### Nov 11 (18:20-18:35 UTC): R48 Flow keeps the last good chain and warns (r90 API, f28 frontend)

User 18:19: apply before the close, and the page should warn. Change: `load_breadth_screener_rows_cursor` labels each
ticker with its last COMPLETE capture (a failed or partial capture writes no rows over the last good ones) and names a
later capture that was not COMPLETE (`latest_capture_state/at/reason`); `breadth_flow_data` adds `breadth_fallback`
(`selected`, `streamed_only`) and, when the selected ticker has no COMPLETE capture, serves the streamed rows with the
capture label; `_flow_with_breadth` reports `flow_breadth` FALLBACK in that case. Frontend: `breadthCoverageWarning` on the
Flow page ("Full chain unavailable" / "Latest chain capture failed" / "Partial chain coverage"). Tests: new
`test_option_flow_breadth_fallback_nov11.py` 3 and the updated fallback test, with breadth/Flow/portal/screens files 82
passed; frontend f28 294 tests, 249 pass, 40 fail = f27's 40 (3 new pass). r90
`alpha-research-realtime-v3:stable-nov5-r90-09ed5b07443808bf` = `sha256:0e63c082e3bf14cf102b68dec2af7042378003b0ba50d8a6dd1b33cacba5c44b`
(= r89 + R48). Shadow r90 18:28 (all captures COMPLETE): Flow for the 9 tickers identical to live r89. r90 read as of
18:15 on live data: SPY last COMPLETE 18:07:43 (5,128) with latest FAILED 18:13:21 ConnectTimeout; TSLA last COMPLETE
18:07:48 with latest PARTIAL (DEADLINE); AMZN COMPLETE 18:11:31. Deploy: `hotfix` API r90 18:29:41-18:33:40, frontend f28
`sha256:94e17a9762e81fdc2cd4a2093c8928b524563b764bb9dfbc9f64dc7ab20207a9` 18:33:40-18:34:21; both healthy; API smoke
`r90-api` 131 of 153 (no change); domain 302 to Access; tunnel no errors; Flow via the proxy 200. Rollback: API r89, frontend f27.

### Nov 11 (18:47 UTC): Stock Alerts restarted once with exit 0 (R43 repeat; mechanism)

`realtime-v2-canonical-stock-alerts` restarted 18:45:35 (exit 0, not OOM, healthy after; publications 18:30, 18:35, 18:40
before and 18:45 PUBLISHED at 18:45:48 after; no log between 18:40:28 and the restart). Same pattern as retention 14:51:45.
Code: `PostgresLeaseHeartbeat._run` (renew every lease/3 s) sets the wrapper's stop event on a `PsycopgError` or a lease
not renewed; `run_fenced_child` then terminates the child and returns; the container exits 0 and Docker restarts it.
Nothing is logged on that path, which matches both restarts. Fix later: log the reason and retry one renewal (R43).

### Nov 11 (20:47-20:58 UTC): V3-1 Step 0 profile **V3 #2** (profile #4; busy end of day) and the four-profile comparison

`profile_options_roles.sh 180`; log `ev/profile-v3-2.log`. **market-state** (V3 loop): 1,857 samples (active ~21% of the
time): hash/JSON 62.1% (`iterencode` 25.2%, `_canonical_value` 16.3%, `<dictcomp>` 8.1%, `_quote_document` 5.5%,
`_utc` 4.5%), window stages 14.2%, journal read 9.5%, DB write 9.2%, DB other 1.8%, materialize 1.3%, decode 0.3%;
docker CPU mean 23.8%, median 20.6%, max 49.0%. **flow** (P8 loop): 4,756 samples: `load_downstream_bindings` 42.2%,
`iterencode` 28.3%, `raw_decode` 16.1%; CPU mean 43.4%, median 42.1%, max 57.0%. **window**: CPU mean 8.9%, median 1.6%,
max 97.2% (one window's burst); hash/JSON 48.5%.

| Profile (UTC) | market-state loop | market-state CPU mean / median / max | market-state samples | journal read share | hash/JSON share | flow CPU mean |
|---|---|---|---|---|---|---|
| P8 #1 14:49 | P8 | 46.0 / 37.6 / 86.3 | 3,855 | 54.9% | 25.9% | 13.1 |
| P8 #2 15:44 | P8 | 48.8 / 39.5 / 86.2 | 4,142 | 55.1% | 26.4% | 18.7 |
| V3 #1 17:57 | V3 | 38.1 / 36.9 / 66.0 | 3,234 | 22.9% | 49.1% | 32.7 |
| V3 #2 20:47 | V3 | 23.8 / 20.6 / 49.0 | 1,857 | 9.5% | 62.1% | 43.4 |

Reading: V3 removes the per-pass journal re-read (55% -> 10-23%) and halves market-state CPU at the busy close (mean 24%
vs 46-49%, peak 49% vs 86%); what is left is hashing/JSON of quote documents (V3-1.6). Flow (still P8) grows all day,
13% -> 43%, from re-reading every downstream binding (V3-1.F).
R47 catcher result (17:26-21:05): 44 passes, each one breadth read (18,450-18,510 rows) and 40 in-memory replays of
`breadth_screener_row`: 0 crashes (1,760 replays). The live API (r89/r90) had no restart after 16:21. The crash seen
15:55-16:21 (live) and 16:01-16:14 (probes) has not recurred since ~16:15; the last capture's rows are kept in
`ev/r47/breadth-current.pkl` for tonight's offline work.

### Nov 11 (21:52-22:00 UTC): close check and the Step 0 conclusion

**Close.** Equity 78 of 78 cutoffs COMPLETE; the 21:00 close failed four attempts 21:00-21:04
(`CLOSING_DAILY_INPUTS_PENDING`, G103) and completed on retry4 by ~21:45 (Nov 9: 22:01); the close built the Dashboard
(DASHBOARD_V1 21:00; R39 overnight read at 22:05). Daily bars Nov 11: ADJUSTED NATIVE_REST 26 (25 members + META),
RAW_UNADJUSTED DERIVED 25. **Options:** 77 of 77 windows COMPLETE (14:40-20:58), none FAILED, MISSED or PENDING (Nov 10:
56 COMPLETE, 2 FAILED, 2 MISSED, 4 PENDING, 2 RUNNING); run time 39-78 s (13-26%). Lanes for the day: O1
COMPLETE_SELECTED 53 / COMPLETE_ZERO 24; OS1 SELECTED 2 / ZERO 74 / BLOCKED 1 (opening context); OS2 SELECTED 5 / ZERO
70 / BLOCKED 2; O2 UNAVAILABLE 77; O3 BLOCKED 77 (R44). O1 evaluations: 144 SELECTED, 180 OBSERVATION (R31), 5,950 not
selected; 95 packages stored, 0 selected not stored. Alerts: 20 OPTION, 7 STOCK. (`close_check.sh`'s OPTW line counts
other keys: 50 COMPLETE / 13 PENDING; the `E:2026-11-11` windows are the ones above.)

**Faults of the day** (`faults-2026-11-11.md`): all known or now rows - API Dashboard drift before r86 (R39), detectors
health (R42), the close's four attempts (G103), lease-handover restarts after the r88 and V3 deploys, stale Options
streams (B14a), 59 old windows past deadline (R32, down from 136 this morning), O3 not evaluated (R44), API restarts
(R47), retention and Stock Alerts silent restarts (R43), one SNTP timeout. **Clock:** 13 CLOCK_OFFSET records, the latest
+0.650 s at 20:47 (morning +0.26-0.36 s): the host clock is drifting again; under the 2 s tolerance, but check W32Time
before the Nov 12 open (R34).

**Step 0 conclusion** (four profiles; table in the V3 #2 entry and in `G102_V3_1_DESIGN.md` Section 2): on P8 the
per-pass journal re-read (55%) plus hashing (26%) fill market-state, and decode + materialize are ~4%, so the gate gives
Steps 1-2 first and does not move Step 4 up. Steps 1-2 (V3 loop) were already built and switched on at 16:59: the busy
close ran at mean 24% / peak 49% of a core (P8: 46-49% / 86%). Next: V3-1.6 (hashing/JSON 62% on V3), V3-1.F (flow, 13% ->
43% through the session), Step 5, then Step 3 at 3x.
22:05 R39 overnight check: API smoke `close-r86` (`ev/api-smoke-close-r86-live-*.json`) 131 of 153 200;
`/latest-price-date`, `/stocks/context`, `/tickers/overview`, `/tickers/setup-summary`, `/scan/all` 200 after the close
(08:58 this morning: 503 on the Nov 10 close Dashboard). Nov 11 Dashboards `58674ec2` (14:35), `64200ab2` and `56236f2b`
(21:00): stored checksum matches the payload after jsonb for all three. R39 VERIFIED.

### Nov 11 (22:13-22:40 UTC): post-close implementation, items 1-4 (user's go ~19:00 UTC); r91 built

**R43** (`advanced_realtime_fencing.PostgresLeaseHeartbeat`): `_renew` retries one renewal after a database error while
the lease has more than 2 s left (warning `durable_lease_renewal_retry`); a renewal the database refuses is final; before
stopping the role it logs `durable_lease_lost` with the resource, owner, epoch and reason; a failed service-state write is
logged (`durable_lease_state_record_failed`) and renewals continue (it used to end the heartbeat thread). The retention
service and the fenced-role wrapper log the stop signal they receive; retention flushes each line. Tests
`test_lease_heartbeat_r43_nov11.py` 7 passed. **V3-1.F** (`PostgresContinuousSessionStateRepository.
load_downstream_bindings_after`, `_FlowVolumeBaselineReconciler(incremental=...)`, `flow_loop_mode`,
`REALTIME_V3_FLOW_LOOP`, P8 default; the role logs `advanced_options_flow_loop mode=`): the reconciler (refresh every
0.5 s) reads only the bindings after the last one it holds, checked as a continuation (sequence, previous hash, session,
head = tail); a state without the method keeps the full read. Tests `test_flow_incremental_bindings_nov11.py` 6 passed.
**O3** (R44): design `G102_O3_INPUTS_DESIGN.md` (producer step in the window behind `REALTIME_V3_O3_INPUTS`; no
migration; 2-3 nights; not in r91). **R47**: no reproduction (200 more replays of the 21:02 rows under `-X dev`, 0
crashes); mitigation `PYTHONFAULTHANDLER=1` on the API (compose).

Test set (r91 image, worktree mounted; Options alert/portal 17 files + p8 runtime, session state, services, consumers +
new): 425 passed, 6 failed = the compose-file baseline (they need `docker-compose.realtime-v2.yml`, outside the mount;
same on HEAD). r91 `alpha-research-realtime-v3:stable-nov5-r91-357ee2902f360678` =
`sha256:961e3ee8e5a705624e6c7a91c4db33b582565caae0eecb9183dbb064353d5a68` (= r90 + R43 + V3-1.F). Live backup for the
rehearsal: `stocks_db_backup_v3-r91-rehearsal-20261111T222505Z.dump` (4.01 GB, 2,903 entries, 488 s, sha256 5AF2711A...).

### Nov 11 (22:33-22:45 UTC): r91 rehearsal on a copy (Nov 10 and Nov 11 session data)

Restore of the 22:25 backup into `test_realtime_v2_r91` on `g102-test-pg`: 344 s, 471,383 bar heads, latest window
21:38:10 (the Nov 11 close). **V3-1.F on the real chains** (`scratchpad/r91/r91_bindings_equivalence.py`, read-only on the
copy, `r91-evidence/bindings-equivalence.json`): Nov 10 66 bindings, Nov 11 77; the incremental read continued from every
prefix (67 and 78 prefixes) equals the tail of the full read, 0 mismatches; a continuation from a wrong hash is refused;
full read 130.7 ms (Nov 10) and 137.4 ms (Nov 11) vs 3.0 / 3.5 ms for one new binding (at two refreshes a second the
full read costs ~27% of a core at the end of the day, matching the flow role's measured growth). **Service start run**
(`scratchpad/r91/rehearse_r91.sh`, runbook 6): logins, hardening 11/11 pass, volumes (journals 4.5 GB with the Nov 11
Options folder `bf6b9c4d`), compose with all 19 services on r91, market-state `REALTIME_V3_MARKET_STATE_LOOP=V3`, flow
`REALTIME_V3_FLOW_LOOP=V3`, API `PYTHONFAULTHANDLER=1`; isolation proven (host, provider and internet unreachable). After
2.5 minutes all 19 running, 0 restarts, 0 error lines, 18 health checks healthy (detectors included). API smoke: 153 routes,
131 OK on the copy and on live, no route with a different status. Stop: 18 exit 0, Options ingress 241 (the known
whole-stack-stop pattern); the new lines appeared: `retention_stop_signal: SIGTERM`, `fenced_role_stop_signal` (Stock
Alerts). Copy renamed back to `test_realtime_v2_r91`, test logins NOLOGIN, stack removed; logs in `r91/r91-evidence`.
Not covered on the copy (no session there): the flow loop inside a live window and a lease renewal error; covered by the
unit tests and the chain check above.

### Nov 12 (02:50-03:05 UTC): scan of every G102 design for pending items; Nov 12 targets

Handover `HANDOVER-20261112.md` (user: target every pending item for the Nov 12 run). Scope and method in
`G102_PLAN_20261112.md` ("How the scan was done"). Real state: `runner/image_vs_tree.py` - r91 `961e3ee8e5a7` 1,631 files,
1 differs (`tests/options/test_stock_bar_detection.py`, outside the copied `tests/realtime`), so r91 == worktree; live r85
`74e2c255d715` differs in 17 files (R42, R43, V3-1.F, r86-r90 read and scheduler changes), scheduler r88 in 13, API r90 in 6
(R43, V3-1.F). Running 03:15: API r90, scheduler r88, 17 services r85, frontend f28 `94e17a9762e8`, all healthy. Clock
03:20: host +0.04 to +0.14 s (`w32tm /stripchart`, W32Time last sync 22:30 UTC), Docker VM +0.155 s (`clock_offset.py`).
Pipeline C9 (retire duplicate control paths) not done: daily supervisor and `activate-session` still in the image, used by
no compose service; two dated one-time scheduled tasks registered, no next run. Result: 90 pending rows targeted (A 9,
B 23, C 6, D 52) plus the V3-1 umbrella; status corrections R42, Quiet contract, G92, B20, R9, R20, R35, R40, V3-1 Steps 1-2;
new rows A0.2, R21.1, Model Lab, Package assessment, Stock Alerts performance, Screens.

### Nov 12 (03:05-03:52 UTC): build pass for r92 (R47 cache, G20, V3-1.6, A0.2, G22 trace, O3 night 1)

**R47** (`advanced_options_breadth.load_breadth_screener_rows_cursor`): with `REALTIME_V2_FLOW_BREADTH_CACHE=ON` a cheap
per-underlying identity (row count, matched catalog ids, observed_at min/max/sum) is read first; while it is unchanged the
built rows are reused (bounded LRU of 2, lock-protected, each caller gets its own list); a new capture rebuilds and logs
`advanced_options_breadth_rows_built`. Off (default) reads exactly as before (no identity query). Consumers (`breadth_flow_data`,
the Screener query) were checked not to mutate rows. Tests `test_breadth_row_cache_r47_nov12.py` 6 + breadth 8 = 14 passed.
**G20** (`advanced_options_read_pin.restore`): with no previous pin the new pin is disabled (enabled false, disabled_at)
instead of deleted; readers serve only enabled pins (`portal_identity`); a repeat call is a no-op; test file 10 passed.
**V3-1.6** (`advanced_options_consumers._market_state_slice_identity`): profile V3 #2 attribution (raw py-spy file
`data-audit/profiles/p8-market-state-20261111T2047.txt`): `plan_slice` canonical hash of every state 34.7%, `commit_slice`
re-hash 7.3%, checkpoint encode ~17%. `REALTIME_V3_MARKET_STATE_HASH=CHECKPOINT` derives the in-memory slice identity
from the checkpoints (shard, offset, `state_hash` = SHA-256 of the full encoded state); the identity is never stored, so
checkpoints, payloads and states are unchanged (tests: identical in both modes at 1- and 10-record slices, restart
equivalence, a changed checkpoint or a mode switch between plan and commit is refused; 6 new + 13 consumer tests passed).
Micro-benchmark (r91 image, 1 CPU, 72 contracts with 8-quote history): identity 9.13 ms x2 per pass -> 0.004 ms; encode
5.76 ms unchanged. **A0.2**: read rules R29/R45/R46/R48 in target design section 3. **G22** trace (read-only, Nov 11
releases): 221 of 232 INTRADAY releases name an earlier `receipt_id`; the column holds the first receipt of the
publication run (one receipt named by 111 releases, one by 74); every manifest's `source_receipt_ids` holds the receipt at
the release's own cutoff (232 of 232). **O3 (R44)** sized at ~03:15: the window has no 30m trend source (202 completed 30m +
21 daily bars per underlying for `build_canonical_trend_evidence`), no 30m structural levels and no model-valued leg
snapshots (live O1 packages are NBBO-midpoint only): D, Nov 12 night. Night 1 built: `realtime/advanced_options_o3_inputs.py`
(C6 pair rule, cutoff midpoint pricing, activity leg by volume/OI, O3 leg floors, bound 2 per underlying, judged-legs set
for the marks gate), tests 15 passed, not wired. **A4.9**: moved to D (an env-only switch protects nothing; needs a database
setting the writer reads, with the user's yes).

### Nov 12 (03:52-04:25 UTC): r92 built and rehearsed on a copy (Nov 10 and Nov 11 session data); R38 MARK_MISSED on the copy

**Tests** (150 offline realtime files touching the changed areas, `runner/run_tests_ww.sh`, r91 image): worktree 2,243
passed / 118 failed; r91's own code (image `/backend`, same files) 2,225 passed / 108 failed; the 108 are identical; the
10 extra are `test_advanced_options_p8_canary_executor.py` creating `/backend/.realtime-canary` on the read-only worktree
mount (24 of 24 pass with the worktree code copied into a writable container). **r92**
`alpha-research-realtime-v3:stable-nov5-r92-7afc7afe7ec4b57a` = `sha256:5b09f86a847e503ae9ff861f237c68abcc5c5f20a05b16ab14c9a2cb376b91c1`
(= r91 + R47 breadth cache, G20, V3-1.6, O3 night-1 module unwired; image code == worktree, 957 files). **Backup**
`stocks_db_backup_v3-r92-rehearsal-20261112T035242Z.dump` (4.01 GB, 2,903 entries, 426 s, sha256 E4E0291B...). **Copy**
`test_realtime_v2_r92` on `g102-test-pg`: restore 411 s, 471,383 bar heads, latest window 21:38:10 Nov 11 (the close);
logins, hardening 11/11, volumes (journals 4.5 GB with the Nov 11 Options folder `bf6b9c4d`); isolation proven
(host, provider, internet unreachable). **Service start run** (`rehearsal-r92/rehearse_r92.sh`), all 19 services on r92
with every new switch on (flow `REALTIME_V3_FLOW_LOOP=V3`, market-state V3 loop + `REALTIME_V3_MARKET_STATE_HASH=CHECKPOINT`,
API `REALTIME_V2_FLOW_BREADTH_CACHE=ON` + `PYTHONFAULTHANDLER=1`): after 2.5 minutes 19 running, 0 restarts, 0 error lines,
18 healthy (preparation has no health check). **API smoke**: 153 routes, 131 OK on the copy and on live, no route with a
different status. **R47 on the copy**: Flow reads for SPY and TSLA with the cache on 0.09-0.16 s, a second time the same,
then the API alone recreated with the cache off 1.22-1.46 s; bodies equal except the per-request `checked_at` and
`staleness_seconds` (both tickers BREADTH, 25 top contracts). (`advanced_options_breadth_rows_built` is an INFO line the
API's log level hides.) **R38 on the copy**: `run_realtime_v2_self_heal.py --once` with `REALTIME_V2_SELF_HEAL_MODE=ACT`,
`REALTIME_V2_SELF_HEAL_ACTIONS=MARK_MISSED`: acted once, E:% windows past deadline PENDING 144 / RUNNING 2 / MISSED 4 ->
PENDING 87 / MISSED 63 (every Nov 5-10 window; the 87 left are Nov 3-4, outside the check's 7-day look-back by design);
then the Options window, controller, scheduler and lifecycle restarted: controller, scheduler, lifecycle 0 errors; the
window exited once on `DurableLeaseUnavailable` (restart inside the previous holder's 30 s lease; the copy runs with
restart no), started again clean `READY_WAITING_FOR_SESSION`, MISSED windows untouched (the window runner and resident
barrier select only PENDING/RUNNING before their deadline; Equity gap repair selects `advanced-%` keys). **Stop run**: 61 s,
17 exit 0, controller 137 (killed at the 60 s grace) and Options window 241: the known whole-stack-stop pattern (row Stack
stop, image-independent); stop-signal lines (R43) in every fenced role. Copy renamed back, test logins NOLOGIN, stack
removed; logs in `rehearsal-r92/r92-evidence`.

### Nov 12 (04:15-04:45 UTC): portal batch for the user (R49 developing bar, R50 Options Flow display); r93 and f29

User (Nov 12 ~04:12): show the developing bar on the ticker chart for 5m-1mo; on Options Flow a total open interest tile
instead of matched OI change, Include 0 DTE and % on the expiry chart, sortable contract table. Alignment of stored bars
read on live (read-only, SPY): 1d 09:30-16:00 ET; 30m and 1h from 09:30 (last hourly 15:30-16:00); 1wk first session
open to last close (current week and month have no bar until complete). **R49** `realtime/ticker_forming_bar.py`, route
`/api/stock/{ticker}/chart/forming`, read contract `forming` in `ticker_repository.read` (closing view: NONE); tests 16.
Equivalence on the r92 copy (`scratchpad/forming/forming_equivalence.py`, read-only): first run volumes off by 1-4 shares
(fractional 1m volumes truncated per minute) -> summed exactly then truncated once; then 1,250 of 1,250 completed Nov 11
buckets (SPY AAPL NVDA CRWD QQQ TSLA IWM MSFT AMZN NFLX x 5m/15m/30m/1h/1d RAW) equal the stored final bars; weekly
(Nov 6 as today) O/H/L 10 of 10, close/volume differ by the official daily close (closing auction not in 1m bars).
**r93** `alpha-research-realtime-v3:stable-nov5-r93-c99df6aaf5e914da` = `sha256:3473e3f243c04a391876033b5bab1ec5248fa58557db19df3b0a8d521d511ce0`
(= r92 + `ticker_forming_bar.py`, `ticker_repository.py`, `portal_api.py`; image_vs_tree against r92 shows only these).
API alone on r93 against the copy: 153 routes, 131 OK, no route different from live; forming route NONE
`NO_SESSION_IN_PROGRESS` for all seven intervals at 04:40, `interval=1m` 422; 0 error lines. Ticker/portal/bar-boundary
tests 209 passed, 3 failed = the same 3 on r92's own code. **f29** `sha256:4947841493ab3d8ec5ec0cb985a6026b205584f02dfb8e8a8ed9153f5298d6fd`:
302 tests, 257 pass, 40 fail = f28's 40 (one existing assertion moved to the new expiry rows). Both held for the go.

### Nov 12 (04:50-05:05 UTC): r93 full-stack start run; R52 Treasury holiday blocker found and fixed (r94)

**r93 start run** (same copy, all 19 services on r93 `3473e3f243c0`, switches on, the R38 MISSED windows present): 2.5 minutes:
19 running, 0 restarts, 18 healthy; the Options lifecycle's status line carried `"treasury_refresh": {"attempts": 6,
"error_type": "ConnectionError", ...}` (the copy cannot reach the internet; during r92's run the refresh was not yet due).
Stop: 3 s, 19 exit 0. **R52**: live's lifecycle shows the same refresh `BUDGET_EXHAUSTED` for rate date 2026-11-11; live
`ref.risk_free_rate_observations` (source `us_treasury_par_yield_v1`) newest 2026-11-10; Nov 11 2026 was Veterans Day (no
Treasury curve; Oct 12 Columbus Day also absent). The window's reference producer requires the previous XNYS session's
curve: the real `_load_treasury_curve` on the copy returns the Nov 10 curve (14 tenors) for session Nov 11 and raises
`ValueError: Treasury curve identity, freshness, or clocks changed` for session Nov 12; it runs in every Options window
(`advanced_options_p8_window.py` reference stage), so every Nov 12 Options window would fail on the live images and on
r91-r93. The pre-open dry run does not run this producer. Fix in r94 `alpha-research-realtime-v3:stable-nov5-r94-5ab69aad781bd578`
= `sha256:47ad1c82eac6b842dcbab121bf5d889d617be41ac89738838f2600898cd3bb6d` (= r93 + `options/model_inputs.py`,
`realtime/treasury_refresh.py`, `realtime/advanced_options_reference_producer.py`): Treasury market holidays (Columbus Day,
Veterans Day with its observed weekday) are skipped when choosing the curve date; tests 16 new, 419 related passed (3 =
baseline). r94 probe on the copy: session Nov 12 OK, Nov 10 curve, 14 tenors.
**r94 full-stack start run** (same copy, 19 services on r94 `47ad1c82eac6`, switches on): 2.5 minutes: 19 running, 0
restarts, 0 error lines, 18 healthy; lifecycle `treasury_refresh` `required_rate_date` 2026-11-10, `status` VERIFIED
(curve 871cabf6...; no provider request). API smoke 153 routes, 131 OK, identical to a fresh live smoke. Flow with the cache
on twice and off: bodies equal except per-request clocks (0.10-0.20 s vs 1.21-1.68 s). Stop 3 s: 18 exit 0, Options
scheduler 241 (the known whole-stack-stop pattern). Copy renamed back, logins NOLOGIN, stack removed; logs
`rehearsal-r92/r92-evidence/r94-log-*`.

### Nov 12 (05:15-05:30 UTC): R52 widened at the user's direction (reference data used when present)

User: the Treasury curve is reference data; use it if present, otherwise do not stop the pipeline (like earnings N/A).
How it is used today (code read): each Options window's reference state turns the curve into a risk-free rate per expiry
(with the dividend yield) for our Black-Scholes IV and Greeks: streamed contracts (Screener local IV/delta), breadth rows
(already tolerant: empty Greeks and a log line when inputs are missing), O1 package evidence (each leg's valuation input,
model version). Marks (NBBO midpoint), O1 detection, OS1/OS2 and alert entries do not use it. Change (worktree, no image
yet): `_load_treasury_curve` accepts the latest stored curve up to `TREASURY_CURVE_MAXIMUM_STALE_SESSIONS` (5) older than
the expected one and reports it (warning `advanced_options_treasury_curve_stale`, fault `TREASURY_CURVE_STALE` DEGRADED);
older or newer still refuses. Tests `test_treasury_bond_holidays_nov12.py` 22; related 425 passed, 3 = baseline. Real rows
(copy `test_realtime_v2_r92`, read-only, worktree code in the r94 image): stored curves Nov 6, 9, 10 (each received 21:00
UTC); session Nov 11 -> Nov 10; Nov 12 -> Nov 10 (not stale); Nov 11 read at Nov 10 20:30 UTC -> Nov 9 with the fault.

### Nov 12 (08:57-09:25 UTC): ticker-review work merged for r95/f30; tests

The ticker-review session (worktree `ticker-review`, branch `ticker-page-review` from c46f988, uncommitted, last edit
08:53 UTC) delivered TP1, TP2, TP7, TP11, TP14, TP15, OA1, OA2, OF1-OF3 and PH1 (review doc
`G102_TICKER_PAGE_REVIEW_20261112.md` sections 7-9). Its diff applied cleanly to this worktree plus its 6 new files; all
35 files equal the review worktree ignoring line endings. **Backend** (`runner/run_tests_ww.sh`, r94 image, offline; the
31 test files that import the portal API, compatibility repository, Flow breadth, alert valuation or breadth modules,
plus the five Nov 12 files): this tree 864 passed / 10 failed / 60 skipped; c46f988 851 / 10 / 60; the 10 failures are the
same tests by name (daily scanner half-gap x2, pattern watch x4, p7 compose file, ticker vertical slice x3); the 13 extra
passes are `test_option_alert_valuation_nov12.py`. **Frontend** (f29 build image, source mounted): type check clean; 317
tests, 272 pass, 40 fail; the failing tests are the same by name as on c46f988 (302, 257, 40). Secrets scan of the diff,
new files and the review doc: clean.

### Nov 12 (09:22-09:24 UTC): r95 and f30 built from cc676d4

**r95** `alpha-research-realtime-v3:stable-nov5-r95-ef7cf74329986b05` = `sha256:2e67bc4c02fe164cbc955feebd845bc7a4da7a1e4530b8664e016075212df3be`
(= r94 + R52 widened + OA1/OA2 + OF3): role entry points import, policy 2; image code == worktree, 959 files.
**f30** `alpha-research-realtime-v2-frontend:stable-nov5-f30` = `sha256:66a57dac5fc0a26e3372067bc69e49adf9630efe8d0a273a5026258be8f70410`
(= f29 + TP1, TP2, TP7, TP11, TP14, TP15, OA2 cells, OF1-OF3, PH1, Options ticker links): 317 tests, 272 pass, 40 fail,
the same 40 by name as c46f988. Both last lines of their candidate files. Rehearsal, deploy and the session: the new
live session (`HANDOVER-20261112-LIVE.md` in the ops folder).

### Nov 12 (09:26-09:57 UTC): live session takeover - validation, r95 rehearsal (OA1 fails on the API grant), r96

New live session (`HANDOVER-20261112-LIVE.md`). **Validation, read-only.** Live = handover: API r90 `0e63c082e3bf`,
scheduler r88 `10cba306c446`, 17 services r85 `74e2c255d715`, frontend f28 `94e17a9762e8`. r95 vs worktree
(`image_vs_tree.py`): runtime code identical; one test file under `tests/options` differs, 43 base-package leftovers.
Every `Nov 12: A` row is in c46f988 (R52 widened) or cc676d4 (OA1/OA2, OF3, TP1/2/7/11/14/15, PH1). Deploy inputs: last
candidate lines r95 / f30; Docker host 852 GB free, C: 368 GB free, backups folder 42 GB. Live (SQL, read-only):
`ref.risk_free_rate_observations` (`us_treasury_par_yield_v1`) Nov 5, 6, 9, 10 (14 tenors each, received 21:00); Nov 11
close Equity 78 COMPLETE (+4 FAILED retried), Options 77/77 COMPLETE, 1d bars 25/25 RAW and ADJUSTED; Nov 12 Options
preparation PREPARING at CATALOG_QUOTE ("provider window is not open; history ready"). Clock (container SNTP) +0.088 s;
W32Time synced 07:36 UTC. Code read: R52 curve load point in time (`received_at <= known_at`), fault emitter cannot raise;
a stale curve is used with evidence AVAILABLE and no reason code (log + fault only; row R52.1). OA1 under its own
savepoint with an optional read (INV-7/9). OF3's null reaches only the API response. **Found:** `runner/close_check.sh`
ran `data-audit/close_check.sql` hard-coded to 2026-11-05 and reading `rt.bar_current` (the Nov 11 "close check" printed
Nov 5 rows); now date-relative, daily bars from `rt.bar_revisions` final rows (row Close check).
**r95 rehearsal** (copy `test_realtime_v2_r92`, Nov 11 close; `rehearse_r92.sh`, switches ON; isolation probed): 19
running, 0 restarts, 0 error lines, 18 healthy; lifecycle `treasury_refresh` `required_rate_date` 2026-11-10 VERIFIED; API
smoke 153 routes / 131 OK on the copy and on live, 0 routes different; Flow cache ON twice == OFF (SPY, TSLA; 0.11-0.20 s
vs 1.33 s); OF3: 0 `contract_id` 0, null SPY 7 / TSLA 4 of 25; **alerts FAIL**: dataset 01f250a0 (Nov 11) LATEST 2 rows /
HISTORY 14 rows, 0 legs valued, `alert_valuation` FALLBACK `ALERT_VALUATION_UNAVAILABLE`, 4 API traceback lines:
`InsufficientPrivilege: permission denied for table advanced_options_reference_states`. The API role has no SELECT on it
(`relation_registry.py`: `api=False`); live the same (`has_table_privilege`: api f, app t). The review measured OA1 with a
script under another role, not through the API. Stop 61 s: 17 exit 0, ingress 241, flow 137 (SIGTERM logged, killed at
the 60 s grace while waiting for the session; r92's run: flow 0). **r96** `alpha-research-realtime-v3:stable-nov5-r96-4cf548ed9fe43fee`
= `sha256:2aa3e4d958107c9603fcbbfe1318b3a7775d141dfc158125abb0ae9b278121ea` = r95 + `REALTIME_V2_OPTIONS_ALERT_VALUATION`
(OFF default: the detector-runs read skips OA1/OA2 and lists no optional read, as r94); image diff vs r95: only
`portal_compatibility_repository.py` and its test. Tests: OA1 file 14 passed; the 25 files importing the portal
repository / alert valuation / portal API 743 passed, 9 failed (baseline names), 60 skipped. **r96 rehearsal** (same
copy, switches ON): 19 running, 0 restarts, 0 errors, 18 healthy at 2 and 4.5 min; Treasury VERIFIED 2026-11-10; smoke
153/131 == live, 0 different; Flow ON==OFF; OF3 as r95; alerts LATEST 2 / HISTORY 14 rows, `alert_valuation` not listed,
0 API errors; stop 3 s, 18 exit 0, market-state 241 (known pattern). Copy renamed back, test logins NOLOGIN, stack
removed; logs `rehearsal-r92/r92-evidence/r96-log-*`, `r95-log-api-oa1-grant.txt`. Frontend: no localhost setting in the
committed diff or in f30 (the review preview's proxy config is a scratchpad file mounted into `ticker-review-preview`;
f30's two `http://localhost` strings are the router library's URL fallback, identical in f28; nginx config unchanged).
Rollback list `rollout/pre-r96-images-20261112.txt`.

### Nov 12 (09:58-10:01 UTC): pre-open audit trial on today's images (new `runner/preopen_audit.sh`)

One read-only entry point for the 13:00 audit (clock, services, `coverage_preopen.sql`, G94, read flags, R40
readiness; report `data-audit/preopen-audit-SESSION.txt`). Trial before the deploy: clock +0.087 s; MEMBERS 25,
DIVIDEND_THROUGH_CLOSE 25/25, SPLIT_NEWER_THAN_COVERAGE 0, LEASES 20 renewing; G94 PASS (25 covered from 2025-09-29);
readiness: every member 283/283 adjusted, 20/20 raw, Nov 11 RTH 390/78/26/13/7, earnings to Nov 14 21:06, Fed to Nov 18,
workspaces and Dashboard all present at the Nov 11 close, Options preparation PREPARING at CATALOG_QUOTE; ISSUES 19 on 9
tickers = CRWD Nov 9 1m 389/390 (row CRWD empty minute, ends Nov 17) and the 8 funds' reference sector/industry (funds
carry none; accepted). Read flags: the script's default cohort lookup raised before the open (no PLATFORM cohort active at
"now"); the audit now passes the portal's current Equity cohort (`/api/realtime/portal/current`): charts 199 OK, 1 REFUSED
(CRWD 1m `NATIVE_1M_HISTORY_INCOMPLETE`), Screener Nov 11 25 rows COMPLETE, hourly lineage VERIFIED 25, Dashboard READY,
Stock Alerts OK (stale warning before the open). Crons (session-only): 12:45 UTC deploy-deadline check, 13:01 audit,
13:31 dry run.

### Nov 12 (10:03-10:27 UTC): r96 + f30 deployed pre-open (user's go ~10:03) and verified (runbook 3.7)

User (~10:03): "how about deploying now". Rollback list `rollout/pre-r96-images-20261112.txt` (API r90, scheduler r88, 17
services r85, frontend f28). Compose: `PYTHONFAULTHANDLER=1` on the API (`runner/compose_env.py`, backup
`compose.stable.before-pythonfaulthandler-20261112T100541Z.json`, one key, CRLF kept). `-Step preflight` 10:05: no
pending migrations (latest 052), audit passed, no legacy containers. `-Step g102-services -Image r96 sha256:2aa3e4d95810`
10:06:11-10:24:18, exit 0, 19 of 19 services; `-Step g102-frontend -Image f30 sha256:66a57dac5fc0` 10:24:38-10:25:17.
Verification 10:25-10:27: every service on its new image, running, 18 healthy (preparation has no health check);
lease-handover restarts only: capability-refresh, detectors, Advanced scheduler, Options scheduler 8 each, every error
line `DurableLeaseUnavailable` (32 of 32), none after each settled (last 10:20:12); 0 error lines elsewhere since 10:06;
LEASES 20 renewed 10:26:36; API env `PYTHONFAULTHANDLER=1`, switches absent (flow P8, breadth cache OFF, market-state hash
CANONICAL, alert valuation OFF), market-state `REALTIME_V3_MARKET_STATE_LOOP=V3`; API smoke 153 routes / 131 OK, 0 routes
different from the 09:53 pre-deploy live smoke; the frontend serves f30's bundle (`index-C58L8k9b.js`), the portal API
through it 200; alpha-screener-pro.com 302 to Cloudflare Access. In-session checks pending: market-state `mode=V3` and the
first-window B items (plan B list), R52 reference stage with the Nov 10 curve.

### Nov 12 (10:30 UTC): ticker-review preview stopped (user: not needed; "make sure it doesn't cause any live run issues")

Before: the container `ticker-review-preview` (Vite dev server, f29 build image, review worktree source, `127.0.0.1:5190`)
was only a client of the live API on `alpha-research-realtime-v2-application`: no reference in the live compose, the
tunnel routes to `realtime-v2-advanced-frontend:80`, no request in the last hour. `docker stop` at 10:30:41 removed it
(it was started with `--rm`). After: every live service unchanged (21 running, restart counts as at the deploy), API
health 200, frontend -> API 200, alpha-screener-pro.com 302, no cloudflared error. To recreate it if wanted:
`docker run -d --rm --name ticker-review-preview --network alpha-research-realtime-v2-application -p 127.0.0.1:5190:5190
-e VITE_REALTIME_ONLY=true -e VITE_API_BASE_URL=/api -v <ticker-review worktree>/frontend/src:/app/src -v
<scratchpad 83d3ecf7>/preview/vite.preview.config.mjs:/app/vite.preview.config.mjs --entrypoint npx
alpha-research-realtime-v2-frontend:stable-nov5-f29-build vite --config /app/vite.preview.config.mjs`.

### Nov 12 (10:35-10:50 UTC): V3-1.6b market-state checkpoint encode cache (user: "carefully analyze and make the changes to take it today")

Analysis: `encode_materializer_state` (only caller `build_materializer_checkpoint`, only live caller the market-state
consumer's `plan_slice`) encodes every shard's whole state on every pass; the role keeps its states in memory
(`self._states`) and the state advance (`_materialize_partitioned_shard_batch`) carries unchanged quotes and trades over
as the same frozen objects. Change (`realtime/materialization.py`): `REALTIME_V3_MARKET_STATE_ENCODE=CACHED` keeps each
quote/trade object's JSON text from the shard's previous encode (key = the object itself, held strongly, so an id cannot
be reused; never the value: `Decimal("1.5") == Decimal("1.50")` but encodes differently) and assembles the payload with
the same json options; anything else (default `FULL`) runs the unchanged full encode. Self-check: calls 1-20 and every
200th also run the full encode; a difference writes the full bytes, logs `advanced_options_market_state_encode_mismatch`,
emits fault `MARKET_STATE_ENCODE_MISMATCH` (DEGRADED) and turns the cache off for the process. Tests
`test_market_state_encode_cache_nov12.py` 7 passed (12 passes x 2 shards with trades, a BAD quote, Decimal text variants:
bytes and checkpoints identical, restore equal; only new quotes encoded; equal-valued twins not shared; mismatch fallback;
default untouched; byte limit). **Replay** (`scratchpad/v316b/replay_encode.py`, read-only on the copy volume's Nov 11
Options journal `bf6b9c4d`, live's shard map `c867c4c7` reproduced, registered session `8c3fe766`): 11,784 uneven passes
(1-1,000 records), 6,345,604 records, **0 mismatches** between the cached and full bytes; final state
`8d63fc50...` == the checkpoint live stored at the Nov 11 close (462,402 bytes); encode 7.56 -> 3.31 ms per pass (56%
less; quote texts reused 4.34 M, encoded 2.45 M at ~540 records per pass; live passes are smaller).

**Nov 12 ~10:56 UTC, user (going away):** the rest of the day runs in this session without further authorization:
V3-1.6b (r97 build, market-state pre-open hotfix with the switch off, switch on ~18:10 UTC), the planned flips (Flow V3
~15:03, R47 cache ~15:33, V3-1.6 ~16:17), the pre-open audit and dry run, the watcher with fix-forward single-service
hotfixes, the close. Still the user's yes: migrations and grants (OA1/OA2), budgets, universe, ledger, READY/clock/
watermark/window-state edits, self-heal actions, whole-platform rollback, commit/push.

### Nov 12 (10:40-11:06 UTC): V3-1.6b tests, r97 built, copy start, market-state on r97 (switch off) pre-open

**Tests** (r96 image, worktree mounted, offline; the 55 test files touching the materializer, the market-state
consumers or the hash switch, plus the new file): `REALTIME_V3_MARKET_STATE_ENCODE` unset 868 passed / 7 failed / 85
skipped; the whole set again with `CACHED` 868 / 7 / 85, the same 7 by name: daily scanner half-gap x2, ticker vertical
slice x3 (baseline), `test_advanced_options_p8_docker_runtime` compose file (no compose file in the test container) and
`test_realtime_architecture_profile` (90 vs 108 calls), both failing identically on r96's own code. **r97**
`alpha-research-realtime-v3:stable-nov5-r97-8c84c8f22e5c8f8d` = `sha256:cbf553160d29849c9b8fe32beeecab459218cdd451cbab111fabf2a5772862be`
= r96 + V3-1.6b (image diff vs r96: `realtime/materialization.py` and the new test only). **Copy start** (copy
`test_realtime_v2_r92`, isolation probed): market-state on r97 with `REALTIME_V3_MARKET_STATE_HASH=CHECKPOINT` and
`REALTIME_V3_MARKET_STATE_ENCODE=CACHED`, the other 18 on r96 with their switches on: 2.5 minutes 19 running, 0 restarts,
0 errors, 18 healthy; stop 3 s, 18 exit 0, flow 241 (known pattern); copy renamed back, logins NOLOGIN. **Live**
11:05:15-11:05:56: `-Step hotfix -Service realtime-v3-options-p8-market-state -Image r97` (pre-open; switch absent = the
r96 encode): running, healthy, 0 restarts, `READY_WAITING_FOR_SESSION`, env only `REALTIME_V3_MARKET_STATE_LOOP=V3`.
Crons: 16:17 V3-1.6 job now hotfixes with r97; new 18:13 UTC V3-1.6b switch-on (after profile #3) with its checks and
fallback (`compose_env.py ... REALTIME_V3_MARKET_STATE_ENCODE --remove` + hotfix).

### Nov 12 (13:01-13:02 UTC): pre-open audit (`runner/preopen_audit.sh 2026-11-12`), read-only - PASS

Report `data-audit/preopen-audit-2026-11-12.txt`. **R34** CLOCK OK +0.066 s. **Services** 21 running (19 healthy with
checks; preparation and cloudflared have none); images r96 on 18, market-state r97 `cbf553160d29`, frontend f30
`66a57dac5fc0`; restarts 8 only on capability-refresh, detectors, scheduler, options-scheduler, all inside ~90 s of their
r96 creation (10:06-10:20 UTC, lease handover), none since. **R9** 25 members, dividends through close 25/25, splits newer
than coverage 0, 20 leases. **G94** PASS (25/25 from 2025-09-29). **Read flags** (portal Equity cohort `effad99e`):
charts 199 OK + 1 REFUSED (CRWD 1m `NATIVE_1M_HISTORY_INCOMPLETE`, known), screener 2026-11-11 COMPLETE 25 rows, hourly
details 25 OK / VERIFIED, dashboard READY, stock alerts OK (pre-open "retained alert data stale" warning). **R40**
readiness: 283/283 adjusted sessions and 20/20 everywhere; ISSUES 19 on 9 tickers = the accepted set (CRWD Nov 9 1m
389/390 and its two page refusals; sector/industry missing on the 8 funds IWM, QQQ, SMH, SPY, XLC, XLE, XLK, XLY).
Options preparation `PREPARING` at `CATALOG_QUOTE` (history ready); 9 Options underlyings. Next: 13:31 dry run.

### Nov 12 (13:31-13:33 UTC): R20 pre-open dry run (`runner/preopen_dry_run.sh 2026-11-12`), read-only - PASS, GO

Report `data-audit/preopen-dry-run-2026-11-12.txt`, exit 0. CLOCK OK +0.056 s. READINESS PASS (desired 19 Equity, 9
Options underlyings, 6 sector ETFs; capability profile PASS until 11-12 21:01; Options cohort created at activation).
OPTIONS PASS: revision `86179d1f` PREPARING at CATALOG_QUOTE; capture OK on all 9 underlyings (42 provider requests,
rejected rows 0, slowest SPY 11.4 s); candidates 8,640, invalid 0; CAPACITY_SHARDS allocation 72 contracts (NFLX 2, QQQ
8, SPY 8, the rest 9). STOCK_ALERTS PASS (19 evaluations: 16 NO_RULE_DUE, 3 BENCHMARK_ONLY, 3.0 s). WORKSPACE PASS (25/25,
0 unavailable, largest NFLX 130,904 of 131,072 bytes, over 0; 80 s). Previous session: Options windows 77 COMPLETE, run
time max 26% / p90 21% (Nov 10: 5 over 60%); WARN O2 UNAVAILABLE and O3 BLOCKED 77/77 on Nov 11, the same as Nov 10
(56/56), not new. Live images: r96 `2aa3e4d95810` on 18 services, market-state r97 `cbf553160d29`, frontend f30
`66a57dac5fc0`. **GO.**

### Nov 12 (14:33-14:39 UTC): first watch pass - open normal; Options attached after the first Equity window

Watch 14:33 (`scratchpad/ev/watch-1433.txt`): Stocks stream `25103f1c` LIVE from 14:30:01, gaps 0; all services on r96
(market-state r97, frontend f30), 0 restarts since the deploy handover, 0 error lines; OPT_RUNTIME_ALARM 0;
O1_SELECTED_NOT_STORED 0. Options preparation ACTIVE 14:34:21-26 (Nov 11: 14:34:22); continuous session `6f80edb6`
prepared 14:34:13; acquisition holds `massive:options:websocket` from 14:34:23 and writes journal `e3e11560` (54 MB by
14:36). The P8 roles left READY_WAITING_FOR_SESSION at 14:34:21 and attached once Equity `advanced-20261112T143500Z`
COMPLETED at 14:38:16 (**196 s**; Nov 11 174 s, Nov 10 188 s), by design (Options waits for a COMPLETE window with
EQUITY_MATERIALIZE): Options stream `3d3997c3` LIVE 14:38:33, Nov 11's `8c3fe766` STOPPED `ENDED_BY_NEXT_SESSION` at its
last heartbeat (B14a); E:14:35 Options PENDING 14:38:19. **Faults** (first collector pass since Nov 11 21:47): CRASH_LOOP
x3 = the 8 lease-handover restarts of the r96 deploy (10:07-10:20, RECOVERED rows); STREAM_STALE on `8c3fe766` = the
pre-attach minutes, as Nov 11 14:37:44 (B14a/R38 END_STALE_SESSION); WINDOW_PAST_DEADLINE 59 PENDING/RUNNING from Nov 5-10
= R32/R38 MARK_MISSED (record-only, user's yes). No new matrix rows. Note: Nov 11's Options journal registration
`bf6b9c4d` is still DRAINING (normalized 6,345,544) - check it at the close with the Nov 12 one.

### Nov 12 (14:39-14:47 UTC): first-window checks (B items R16/R19, R52, V3 mode, R14, R15, G65, R42) - all PASS

Read-only (session SQL via `runner/audit_live.ps1`, docker logs, `scratchpad/ctx-1441.json`). **(1) R16/R19** lifecycle:
CAPACITY_SHARDS COMPLETE 9/9 (last TSLA 14:34:09), SESSION_ACTIVATION 14:34:11-21, work rows all COMPLETE (REFERENCE_OI,
HISTORY, CATALOG_QUOTE, CAPACITY_SHARDS 9 each, activation 1); preparation ACTIVE/COMPLETE (READY comes at 21:00 for the
next session, as Nov 10/11); no refused-candidate line (dry run: invalid 0) and no SerializationFailure; lifecycle 0
restarts since 10:18. Its `treasury_refresh`: VERIFIED, cached, `required_rate_date` 2026-11-10, curve `871cabf6`.
**(2) R52** Options E:14:35 COMPLETE 14:41:52 (69.3 s attempt, all 7 stages), E:14:40 COMPLETE 14:46:43 (101.7 s); the
reference states at 14:34:13, 14:40, 14:45 (148 members) carry `rate_date` "2026-11-10"; no
`advanced_options_treasury_curve_stale` line, no TREASURY_CURVE_STALE fault (the 6 grep hits were `lane_errors` keys).
Marks: E:14:35 72/72; E:14:40 52, 20 `OPTION_NBBO_STALE_OR_NONCAUSAL` (clock +0.056 s; watch). **(3)** market-state
`advanced_options_market_state_loop mode=V3` 14:40:00 (r97, encode switch off). **(4) R14** 0 `O:B1` roots of 17,814
`rt.option_breadth_current` rows (9 underlyings; 9 captures COMPLETE by 14:39:18). **(5) R15** TICKER_WORKSPACE_V1 25 at
14:35; summary, Dashboard, Stock Alerts, Pattern Watch at 14:35. **(6) G65** `/api/stocks/context` 200, 25 rows,
`expected_members` 25, `eligible_ranked` 25, no `member_status` PARTIAL; status STALE at 14:41 = the hourly 14:35
Dashboard against the 14:40 expected time (`staleness_state`, serveable, by design). **(7) R42** detectors `healthy`,
READY, 50/50 leases, 1 active journal, capability refresh READY (14:40:02). Lanes at E:14:40 as Nov 11: O1
COMPLETE_SELECTED (7 SELECTED / 1,284 NOT_SELECTED, 4 packages, not stored 0), O2 UNAVAILABLE `O2_MODEL_NOT_ACCEPTED`,
O3 BLOCKED `E_CONTEXT_INCOMPLETE` + `ACCEPTED_INPUT_EVIDENCE_ABSENT` (R44), OS1 BLOCKED at E:14:35 then COMPLETE_ZERO, OS2
BLOCKED `E_CONTEXT_INCOMPLETE` (needs 3 session 5m bars). Options run time 112 s / 104 s (37% / 35%; Nov 11 64/43 s),
alarms 0, lag 1 s. Equity E:14:35 196 s, E:14:40 COMPLETE 14:40:40. Matrix: R14, R42, R52, G65 VERIFIED; R15, R16, R19
notes (paths not exercised / check at the close).

### Nov 12 (14:48-14:52 UTC): watch pass and portal B checks (r96 + f30) - all PASS

**Watch 14:48** (`scratchpad/ev/watch-1448.txt`): Equity 3 COMPLETE through 14:45, Options 2 COMPLETE (E:14:45 pending
its cutoff), lanes as at 14:45 (O1 SELECTED 7, not stored 0, 4 packages; O2 `O2_MODEL_NOT_ACCEPTED`; O3/OS2 BLOCKED
`E_CONTEXT_INCOMPLETE`; OS1 COMPLETE_ZERO), 3 Options alerts OPEN, workspaces 25 at 14:35/14:40/14:45, streams LIVE, gaps
0, lag 1 s, run time 35-37%, alarms 0; no restarts since the deploy, 0 error lines; market-state CPU 21.6%, acquisition
451 MiB. Faults: only WINDOW_PAST_DEADLINE again (R32/R38, old sessions).
**Portal** (read-only; files in `scratchpad/portal/`): **R49** `/api/stock/SPY/chart/forming` 5m FORMING bucket
14:45-14:50 (3 minute bars, as of 14:48) -> 14:50-14:55 (as of 14:51); 1d FORMING 18 -> 21 minute bars, volume 3.15 M ->
3.52 M. **R50** Flow SPY `selected_summary.total_open_interest` 8,421,894 (call 3,437,381 / put 4,984,513), 17
expirations (0 DTE 254 contracts); bundle `OptionsFlowPage-B4Fm5EFl.js` has "Total open interest", "Include 0 DTE", "Show
%", `aria-sort`, no "Open interest change". **OF3** SPY `top_contracts` 25: `contract_id` null 12, 0 none, catalog ids
13; 25 unique tickers; breadth COMPLETE 5,046 contracts, `flow_breadth` AVAILABLE. **PH1** `/api/realtime/portal/current`
Equity and Options session 2026-11-12, status CURRENT, cutoffs 14:45 -> 14:50 at 14:51:45; `PageChrome` has "Market
data", "Real-Time", "Session close". **TP** (served `TickerWorkspace-TQQeYRFO.js` / `index-C58L8k9b.js`, image
`66a57dac5fc0`): TP1 no "Analysis timeframe"/"Following chart", `analysis_interval` deleted from the URL; TP7
"Abbreviations"; TP11 `tk-volflow` Participation picture; TP14 `report_limit` 40; TP15 `ticker-table--statement`; TP2
bracket code present (a short plan not inspected on the page). **OA1/OA2 off**: `/options/alerts/detector-runs`
(dataset `47285957`, 2026-11-12) 3 rows, `optional_reads` = session_alerts, current_marks, source_facts, no
`alert_valuation`; API log since 10:20 has no valuation line (the one grep hit is an `/evaluations` URL). Matrix: R49,
R50, OF1-OF3, PH1, TP1, TP2, TP7, TP11, TP14, TP15 VERIFIED; OA1/OA2 notes (off, grant needs the user's yes).

### Nov 12 (14:53-15:03 UTC): V3-1 profile **#1** (market-state V3 loop on r97, encode switch off; Flow still P8)

`runner/profile_options_roles.sh 180` (py-spy `--nonblocking`, 180 s per role); log `scratchpad/profile1.log`; data
`data-audit/profiles/p8-<role>-20261112T1453.txt(.cpu)`. Same time of day as Nov 11 **P8 #1** (14:49, market-state on
P8); same loops as Nov 11 **V3 #1** (17:57). **market-state**: 2,292 active samples (P8 #1 3,844; V3 #1 3,234): journal
read (SQLite) 49.8% (P8 #1 54.9%, V3 #1 22.9%; all under `_run_market_state > read_normalized_after`, 53.1% of stacks vs
24.6% at V3 #1), hash/JSON 29.5% (25.9 / 49.1), window stages 9.2%, DB write 4.0% (3.3 / 11.6), decode 3.0%, materialize
1.8%; top leaves `<genexpr>` 25.8%, `read_normalized_after` 13.9%, `iterencode` 10.8%, `_canonical_value` 7.8%; docker CPU
mean **31.6%**, median 26.9%, max 59.9% (P8 #1 46.0 / 37.6 / 86.3; V3 #1 38.1 / 36.9 / 66.0). Fewer samples and lower
CPU than both; the journal-read share is high again in the opening hour (heavier record flow per pass than at 17:57), so
V3-1.6b (encode) addresses the ~30% hash/JSON part, not the read. **flow** (P8 loop): 1,226 samples: flow 36.9%
(`offsets_at` 24.3%), journal read 25.9%, hash/JSON 25.9% (`iterencode` 14.0%), `load_downstream_bindings` 12.0% (P8 #1
12.9%, V3 #1 37.9%), DB other 7.6%; CPU mean 15.0%, median 10.2%, max 74.0% (P8 #1 13.1 / 9.4 / 61.0). **window**: 1,057
samples (P8 #1 97, V3 #1 441): hash/JSON 71.0% (`iterencode` 28.4%, `_canonical_value` 13.2%, `<dictcomp>` 13.2%,
`persist_flow_package_evidence_cursor` 12.0%), DB write 16.7%, DB other 6.3%; CPU mean 10.0%, median 1.5%, max 98.4% (P8
#1 1.6 mean; V3 #1 4.3). **Stands out (watch, not a fault):** the window role works ~10x the Nov 11 opening samples, in
evidence hashing/persisting (O1 packages: 71 by 15:10), matching Options window run time 27-40% today vs 14-21% on Nov 11
at this hour; alarms 0. Follow-up row if it grows toward 60%.

### Nov 12 (15:03-15:12 UTC): V3-1.F on - Flow on the V3 loop (`REALTIME_V3_FLOW_LOOP=V3`, r96), API checks R25 R29 R45 R46 R48

**Precondition 15:03:20**: Options E:14:35-14:55 all COMPLETE, lanes as designed, Flow 0 restarts / 0 error lines / no
fault rows. **Flip**: `runner/compose_env.py realtime-v3-options-p8-flow REALTIME_V3_FLOW_LOOP V3` 15:03:34 (backup
`rollout/compose.stable.before-realtime_v3_flow_loop-20261112T150334Z.json`; the other env keys unchanged); `-Step hotfix
-Service realtime-v3-options-p8-flow -Image r96` 15:03:49-15:04:33 (old container stopped in ~3 s, not the 60 s grace;
`others_changed` none; log `scratchpad/deploy-flow-v3.log`). **Verify**: env `REALTIME_V3_FLOW_LOOP=V3`, healthy, 0
restarts; `advanced_options_flow_loop mode=V3` 15:05:00; E:15:00 COMPLETE 15:06:43 (run 103 s), E:15:05 COMPLETE 15:11:27
(87 s), E:15:10 COMPLETE 15:16:00; lanes at E:15:10 normal (O1 126 SELECTED / 71 packages / not stored 0). Flow CPU
(`scratchpad/ev/flow-v3-flip-watch.txt`, every ~22 s): 98% for ~40 s of catch-up after the start, then 6-11% between
windows with 60-74% for ~40 s inside each window (P8 before: 7-10% with 71-74% spikes); flat, memory 207 MiB. Fallback
(~1 min): `compose_env.py ... REALTIME_V3_FLOW_LOOP --remove` + the same hotfix.
**API checks** (files `scratchpad/portal/`): **R25** `/options/flow` and `/options/alerts/datasets` every ~22 s
15:12:20-15:18:21 (one full cycle and the 15:10 -> 15:15 cutoff change): `available` true, `stale` false,
`staleness_seconds` 0 in all 34 samples, `valid_until` 21:35. **R29** detector-runs (dataset = latest run `cc42fa44`):
no repeated `session_alert_id` or `candidate_id` in either scope. **R45** LATEST 1 row (NVDA 14:57, 53 hits) vs HISTORY 4
rows (NVDA 14:40 x2, 14:45, 14:57), latest run withheld and named in HISTORY. **R46** one row per contract and side in
both scopes. **R48** SPY, QQQ, TSLA, NFLX, AAPL `coverage_source` BREADTH, captures COMPLETE (5,080 / 5,370 / 1,114 / 390 /
876 contracts), `breadth_fallback` null; all 72 captures today COMPLETE, so the last-good-chain path was not exercised.
Note: the alerts `dataset_id` is the latest run's id and changes every window (47285957 at 14:50, cc42fa44 at 15:10).

### Nov 12 (15:15-15:21 UTC): user "yes for both" - R38 MARK_MISSED acted once on live; R44 O3 night 2 confirmed

User ~15:15 ("yes for both"), answering the watch line that the old past-deadline windows wait on their yes and that O3
is not evaluated (R44). **R38/R32**: before (`scratchpad/ev/mark-missed-before.txt`, 15:18:12): Options windows past
deadline PENDING Nov 3 10, Nov 4 77, Nov 5 13, Nov 6 37, Nov 9 3, Nov 10 4 + RUNNING 2 (MISSED already 4). One pass
`run_realtime_v2_self_heal.py --once` in r96 with `REALTIME_V2_SELF_HEAL_MODE=ACT`, `REALTIME_V2_SELF_HEAL_ACTIONS=
MARK_MISSED` (same mounts and network as the collector) 15:18:30: `acted 1`, ACTED WINDOW_PAST_DEADLINE on **55**
windows (Nov 5 9, Nov 6 37, Nov 9 3, Nov 10 6; guard: `E:` keys, PENDING/RUNNING, deadline + 2 min passed, no live
lease); out/err `scratchpad/ev/mark-missed-act.*`. After (15:18:40): MISSED Nov 5 9, Nov 6 39, Nov 9 3, Nov 10 8; still
PENDING Nov 3 10, Nov 4 77, Nov 5 4 (= 91, older than the check's 7-day look-back; closing them is a one-time wider
window-state edit, asked separately); today 8 COMPLETE + 1 PENDING untouched. Window, controller, schedulers, lifecycle:
0 restarts and 0 error lines after; E:15:15 COMPLETE 15:21:09. The watcher's collector passes stay record-only; a new
WOULD_ACT past-deadline finding today gets the same one-off ACT pass. **R44**: the user's yes confirms night 2 of the O3
input design tonight (no live change now).

### Nov 12 (15:33-15:40 UTC): R47 breadth cache ON on the API (`REALTIME_V2_FLOW_BREADTH_CACHE=ON`, r96)

**Precondition**: API 0 restarts since 10:17:24; Flow V3 settled (E:15:05-15:25 COMPLETE). **Before** (r96 image,
`rehearsal-r92/flow_get.py` against `stock-screener-realtime-v2-advanced-api:8002`, files `scratchpad/r47/before*`):
15:33:23 SPY 2.11 s / TSLA 1.85 s, 15:33:28 SPY 1.87 s / TSLA 1.68 s, BREADTH 25 top contracts, the two reads equal
without the per-request clocks; API smoke 15:33:59 153 routes 131 OK (`scratchpad/ev/api-smoke-r47-before-*.json`).
**Flip**: `compose_env.py realtime-v2-advanced-api REALTIME_V2_FLOW_BREADTH_CACHE ON` 15:34:45 (backup
`rollout/compose.stable.before-realtime_v2_flow_breadth_cache-20261112T153445Z.json`; PYTHONFAULTHANDLER=1 kept);
`-Step hotfix -Service realtime-v2-advanced-api -Image r96` 15:34:46, waited for the quiet part, recreated 15:38:01-03,
done 15:38:40 (`others_changed` none; log `scratchpad/deploy-api-r47.log`). **After**: env has the switch, healthy, 0
restarts; 15:38:48 SPY **2.12 s** (first read builds the cache) / TSLA 0.17 s, 15:38:52 SPY **0.26 s** / TSLA **0.17 s**;
after1 == after2 for both tickers without the clocks (the uncached SPY build equals the cached read, capture 15:36:28);
before vs after differ only because the 15:35 release and a new capture landed in between (release cutoff 15:30 ->
15:35). API smoke 15:39 153 / 131 OK, no route with a different status; API log since 15:38: 0 tracebacks, no
segfault. Fallback (~4 min with the quiet-part wait): `compose_env.py realtime-v2-advanced-api
REALTIME_V2_FLOW_BREADTH_CACHE --remove` + the same hotfix.

User question 15:2x (ticker page warning) and 15:3x (chart refresh): measured for the answer: Equity windows publish
41-71 s after each 5-minute mark (14:35 first window 197 s); the ticker page polls the publication every 30 s
(`LIVE_PUBLICATION_POLL_MS`) and the developing bar every 60 s; 1m stream bars are stored 1.8 s median (p90 2.4 s, max
2.5 s) after the minute ends (750 bars, 15:10-15:40). Proposal (portal batch, not deployed): minute-aligned forming-bar
read and window-aligned publication poll; market warning only past the normal build time.

### Nov 12 (15:41 UTC): watch - normal; host clock +0.615 s (containers +0.08-0.11 s)

Equity 14 COMPLETE through 15:40; Options 12 COMPLETE through E:15:30 (E:15:35 running, E:15:40 pending), lanes as
designed (O1 359 SELECTED / 187 packages / not stored 0); run time 20-29% (15:10-15:35), alarms 0; first STOCK alert of
the day OPEN 15:30; streams LIVE, gaps 0; API 0 restarts since the R47 flip. Faults: CLOCK_OFFSET DEGRADED +0.615 s is
the collector's host (Windows) SNTP check; the services' clock (`runner/clock_offset.py` in a container) reads +0.082,
+0.085, +0.106 s at 15:41; W32Time RUNNING. Under the 1 s action line (R34), no resync asked. Marks per window
41-72 of 72 (unmarked = `OPTION_NBBO_STALE_OR_NONCAUSAL`, quiet contracts; Nov 11 43-58), not clock-driven (Nov 10's skew
unmarked all). LANE_NOT_EVALUATED O3 = R44.

### Nov 12 (15:43-15:53 UTC): V3-1 profile **#2** (Flow on the V3 loop since 15:04; market-state V3 on r97, encode off)

`runner/profile_options_roles.sh 180`; log `scratchpad/profile2.log`; data `data-audit/profiles/p8-<role>-20261112T1543.txt`.
Compared with Nov 11 **P8 #2** (15:44, both roles on P8) and today's **#1** (14:53, Flow still P8). **flow**: **516**
active samples (#1 1,226; P8 #2 1,665): flow 41.9%, hash/JSON 27.9%, DB other 19.6% (`transaction` 18.2%), journal read
**1.6%** (#1 25.9%); in the stacks `load_downstream_bindings` 25.9% at #1 is gone, its V3 replacement
`load_downstream_bindings_after` is 1.4%, and `offsets_at` fell 25.9% -> 1.6%. Docker CPU mean **7.7%**, median 5.7%, max
20.8% (#1 15.0 / 10.2 / 74.0; P8 #2 18.7 / 13.3 / 60.7): about half of #1 and 40% of Nov 11 at this hour, and the
window spike dropped from ~74% to ~21%. **market-state**: 2,024 samples: journal read 46.5% (#1 49.8%), hash/JSON 31.2%,
window stages 9.8%, DB write 5.5%, decode 2.6%, materialize 1.3%; CPU mean 23.2%, median 19.2%, max 49.5% (#1 31.6 / 26.9
/ 59.9; P8 #2 48.8 / 39.5 / 86.2). **window**: 695 samples (#1 1,057; P8 #2 110): hash/JSON 67.1% (`iterencode` 26.0%,
`persist_flow_package_evidence_cursor` 13.2%), DB write 15.1%, DB other 9.8%; CPU mean 5.3%, median 1.0%, max 52.9% (#1
10.0; P8 #2 2.3) - still above Nov 11 (O1 packages 223 by 15:50 vs ~7 at this hour Nov 11), not growing. Options window
run time 19-34% 15:25-15:50. **V3-1.F VERIFIED**: the per-pass read of all downstream bindings is gone and Flow CPU halved.

### Nov 12 (16:00-16:14 UTC): RF1 refresh timing for the ticker chart and Options Flow (user's go); f33 live

User 15:2x-16:0x: how often the ticker page and Options Flow refresh, then "go for chart and flow refresh changes", then
(before deploy) "lets not show this message 'Updating to 10:35 AM ET data…'". **Change** (frontend only; new
`src/utils/refreshTiming.ts`): ticker publication check 40 s after each 5-minute mark, then every 10 s until the window
lands, 30 s when late (4 min) and the old plain 30 s when no live window is expected (before the first window, more
than two windows behind) (`publicationNavigation.ts`); developing candle read 5 s after each minute (`TickerDetail.tsx`,
was every 60 s); Options Flow 50 s after the mark, then every 10 s until both the release and (on BREADTH) the chain
capture are in, 60 s idle (`OptionsFlowPage.tsx`, was every 60 s); the ticker page's "Market data is not current"
warning is not shown while the next window is within its normal build time (150 s after the mark) and shows when it is
late; no "updating" text (user). Measured basis: 1m bars stored 1.8 s median (max 2.5 s) after the minute; Equity
windows 41-71 s after the mark (first 197 s); Options captures 47-130 s, Options windows 60-121 s. **Tests**: new
`tests/refreshTimingNov12.test.mjs` (6); `portalPublicationRegressions.test.mjs` loader compiles the new module and the
running-session test checks the interval function (30 s with no window / an old one). **f33**
`alpha-research-realtime-v2-frontend:stable-nov5-f33` = `sha256:961eef3391e86e60807e8a271f7c48ba86b48d0e8f8f6970b94a99d924dc89c2`:
323 tests, 278 pass, 40 fail = the same 40 by name as f30 (f31/f32 were intermediate builds: f31 before the loader fix,
6 extra failures; f32 with the "updating" text). No "Updating to" in the bundle; `localhost` strings 2 = f30.
**Preview** (f33 with the live nginx config, 127.0.0.1:5185, removed after): index, `/ticker/AAPL`, `/options/flow`,
`/api/realtime/portal/current`, forming read 200. **Live**: `-Step hotfix -Service realtime-v2-advanced-frontend -Image
f33` 16:11:57-16:13:40 (recreated 16:13:01-03, `others_changed` none); serving `index-B22y7vlw.js`, healthy, API proxy
200, public domain 302 to Access. Fallback: the same hotfix with f30 `sha256:66a57dac5fc0a26e3372067bc69e49adf9630efe8d0a273a5026258be8f70410`.

### Nov 12 (16:17-16:27 UTC): V3-1.6 on - market-state slice identity from checkpoints (`REALTIME_V3_MARKET_STATE_HASH=CHECKPOINT`, r97)

**Precondition 16:17:05**: market-state r97 `cbf553160d29`, 0 restarts, env `REALTIME_V3_MARKET_STATE_LOOP=V3`; Flow V3
(15:04) and R47 (15:38) settled, no Flow/API restarts; Options E:14:35-16:05 COMPLETE, lanes as designed. **Flip**:
`compose_env.py realtime-v3-options-p8-market-state REALTIME_V3_MARKET_STATE_HASH CHECKPOINT` 16:17:20 (backup
`rollout/compose.stable.before-realtime_v3_market_state_hash-20261112T161720Z.json`; LOOP=V3 kept); `-Step hotfix
-Service realtime-v3-options-p8-market-state -Image r97` 16:17:21-16:18:39 (recreated 16:18:00-02; `others_changed`
none; log `scratchpad/deploy-ms-v316.log`). **Verify**: env HASH=CHECKPOINT + LOOP=V3, healthy, 0 restarts;
`advanced_options_market_state_loop mode=V3` 16:20:00 (idle 0.8-1.2% CPU until the session loop resumed at the 16:20
boundary, 90% for one sample of catch-up); checkpoint `market-state-materializer-v1` shard 0 at owner epoch 2, offset
3,528,679 and climbing, committed every pass (16:26:58); 0 'identity changed before commit', 0 tracebacks/errors. Options
E:16:15 COMPLETE 16:21:14 (run 75 s, 25%), E:16:20 COMPLETE 16:25:58 (59 s, 20%); lanes at E:16:25 as designed (O1
COMPLETE_SELECTED, not stored 0; O2 UNAVAILABLE; O3 BLOCKED R44; OS1/OS2 COMPLETE_ZERO). Marks 32/72 at 16:20 and 16:25
(40 `OPTION_NBBO_STALE_OR_NONCAUSAL`) - within the afternoon's pre-flip range 33-50 (15:45 33), quiet contracts, not the
flip. **CPU** (`scratchpad/ev/ms-v316-watch.txt`, docker stats every ~17 s): one full cycle 16:21:14-16:26:16 mean
**15.9%**, median 12.1% (between windows 5-15%, inside the window 33-48%); before (profile #2 15:43, same encode): mean
23.2%, median 19.2% (between windows 14-27%, window 41-50%). Profile #3 at 17:57 measures it by cause. Fallback (~1 min,
resumes from checkpoints): `compose_env.py realtime-v3-options-p8-market-state REALTIME_V3_MARKET_STATE_HASH --remove` +
the same hotfix with **r97**.

Nov 12 16:27 UTC watch: normal after V3-1.6 (Equity 23 / Options 22 COMPLETE through 16:25, lanes as designed, run
17-29%, alarms 0, market-state 7% CPU between windows). Options lag one sample 7 s, then 0-1 s (16:27:54-16:28:15).
Host (Windows) SNTP offset +0.833 s (15:41 +0.615 s, drifting); the services' clock in a container +0.116 / +0.131 s:
under the 1 s line, no action; watch the host trend (a host over 1 s gets the user's admin `w32tm /resync /force`).

### Nov 12 (17:59-18:09 UTC): V3-1 profile **#3** (market-state HASH=CHECKPOINT since 16:18, encode cache off; Flow V3)

`runner/profile_options_roles.sh 180`; log `scratchpad/profile3.log`; data `data-audit/profiles/p8-<role>-20261112T1757.txt`.
Compared with Nov 11 **V3 #1** (17:57, same hour) and today's #2 (15:43). **market-state**: 2,062 active samples (#2
2,024; Nov 11 V3 #1 3,234): DB write (state, checkpoints) **26.7%** (#2 5.5%, V3 #1 11.6%), journal read 24.7% (#2
46.5%), **hash/JSON 23.5%** (#2 31.2%, V3 #1 49.1%), window stages 16.4%, DB other 4.1%, materialize 1.5%, decode 1.1%;
CPU mean 26.3%, median 30.4%, max 49.3% (#2 23.2 / 19.2 / 49.5; V3 #1 38.1 / 36.9 / 66.0). Inclusive by function (#2 ->
#3): `_slice_identity` **23.2% -> 0.2%** (V3-1.6 did what it was for: no whole-state re-hash per window), but
`_persist_quote_materialization` 5.9% -> **30.4%**, `decode_materializer_state` 3.7% -> **21.3%**,
`encode_materializer_state` 13.5% -> 22.6%, checkpoint path 17.9% -> 45.7%. The persist step's safety check
(`restore_materializer_checkpoint(checkpoint, shard_map)` then `restored != state`, `materialization_repository.py:67-69`)
decodes the whole checkpoint payload (~0.9 MB text at 16:26) and compares it every persist; with CHECKPOINT mode that
path now carries the cost the hash used to. Net: hashing fell as designed, total market-state CPU did not (26% vs Nov 11
38% at the same hour, about #2's level). New G102 row V3-1.6c (persist-path restore check). V3-1.6b (encode cache, on at
18:13) cuts the encode half of it. **flow** (V3 loop): 545 samples (#2 516), flow 41.7%, hash/JSON 26.2%, DB other
21.3%; CPU mean 7.8%, median 5.9%, max 31.2% (#2 7.7%): stable. **window**: 119 samples (#2 695, #1 1,057), DB other 51.3%
(`load_v3_pending_window_bindings` 20.2%), window stages 34.5%; CPU mean 1.7% (#2 5.3%): fewer O1 packages per window this
hour (O1 COMPLETE_ZERO in several windows). Options window run 13-22% (17:25-18:03), alarms 0.

### Nov 12 (18:13-18:25 UTC): V3-1.6b on - market-state checkpoint encode cache (`REALTIME_V3_MARKET_STATE_ENCODE=CACHED`, r97)

**Precondition 18:13:09**: profile #3 finished 18:09; V3-1.6 on since 16:18 (0 restarts, 0 error/mismatch lines);
Options E:17:55-18:05 COMPLETE, lanes as designed; no market-state fault rows. **Flip**: `compose_env.py
realtime-v3-options-p8-market-state REALTIME_V3_MARKET_STATE_ENCODE CACHED` 18:13:41 (backup
`rollout/compose.stable.before-realtime_v3_market_state_encode-20261112T181341Z.json`); `-Step hotfix -Service
realtime-v3-options-p8-market-state -Image r97` 18:13:42-18:14:22 (recreated 18:13:43-45; `others_changed` none; log
`scratchpad/deploy-ms-v316b.log`). **Verify**: `printenv` in the container: ENCODE=CACHED, HASH=CHECKPOINT, LOOP=V3;
`advanced_options_market_state_loop mode=V3` 18:15:00; 0 `advanced_options_market_state_encode_mismatch` lines, 0
tracebacks/errors, no MARKET_STATE_ENCODE_MISMATCH fault (self-check: first 20 encodes and every 200th compared with
the full encode); checkpoint `market-state-materializer-v1` at owner epoch 3, offset 5,794,949 (18:19:53) -> 5,800,903
(18:20:14), committed every pass. Options E:18:10 COMPLETE 18:15:43, E:18:15 COMPLETE 18:19:16; marks 38 and 22 of 72
(afternoon range 20-46 since 16:25, 17:30 20). **CPU** (`scratchpad/ev/ms-v316b-*.txt`): 18:20:34-18:24:23 (17 samples,
one window inside) mean **7.9%**, median 5.7%, max 38.8%; before: V3-1.6 alone one cycle 16:21-16:26 mean 15.9% /
median 12.1%, profile #3 (17:59-18:02) mean 26.3%; 18:13:09-27 between windows 2.6-17.5%. Profile #4 at 20:47 measures
it by cause. Fallback (~1 min): `compose_env.py realtime-v3-options-p8-market-state REALTIME_V3_MARKET_STATE_ENCODE
--remove` + the same hotfix with r97; a mismatch already falls back to the full encode by itself.

Nov 12 18:35 UTC (user: "lets commit current changes, so after hours work can start cleanly"): secrets scan of the
staged additions (1,530 lines) against the five mounted secret files and key/DSN patterns: 0 hits; no localhost/port
strings in the frontend diff. Commit `ad65946` on `v3-live-fixes` (r96/r97 code and tests, f33 RF1 code and tests,
G102 docs, new `docs/g106`), pushed; `main` fast-forwarded and pushed. Night handover for a new session:
`HANDOVER-20261112-NIGHT.md` (ops folder), summary in `G102_PLAN_20261112.md` "Night plan".

### Nov 12 (18:59-19:07 UTC): OA1/OA2 on live (user's yes ~18:58 "ok, go ahead"): grant, r98 registry, API switch

**Grant** 18:59:24 (admin DSN from the mounted secret, in an r96 container, database name asserted):
`GRANT SELECT ON rt.advanced_options_reference_states TO stocks_realtime_v2_api`; after: SELECT true, INSERT false (the
valuation reads only that table beyond what the API already reads: `option_alert_valuation.py`). **First hotfix refused**
18:59:40: the deploy tool's `validate_integrated_image.py verify-live` (run in the newest candidate image) compares live
privileges with the relation registry: `runtime_privileges_match_registry FAIL ['api:SELECT:rt.advanced_options_reference_states:extra']`;
so every hotfix was blocked while the grant was ahead of the registry (live services do not run this audit; only the
deploy, harden and apply tools do). **r98** `alpha-research-realtime-v3:stable-nov5-r98-bb55ac1fd211281b` =
`sha256:1200f917a4d269e1b9b75d2d5d5162f6442571bfccfdffbf9bfb400f64007009` = r97 + `relation_registry.py` only
(`rt.advanced_options_reference_states` `api=True`; image file diff vs r97: that file only); tests
`test_schema_v2_registry.py`, `test_security_v2_roles.py`, `test_option_alert_valuation_nov12.py` 48 passed,
`test_advanced_options_schema_contract.py` 23 passed; last line of `rollout/candidates.txt`. **Switch**: `compose_env.py
realtime-v2-advanced-api REALTIME_V2_OPTIONS_ALERT_VALUATION ON` 18:59:40 (backup
`compose.stable.before-realtime_v2_options_alert_valuation-20261112T185940Z.json`); `-Step hotfix -Service
realtime-v2-advanced-api -Image r98` 19:02:47-19:03:41 (privilege check passed; recreated 19:03:00-03; `others_changed`
none). **Verify**: env ALERT_VALUATION=ON, BREADTH_CACHE=ON, PYTHONFAULTHANDLER=1; healthy, 0 restarts, 0 tracebacks;
detector-runs LATEST (3 rows) and HISTORY (7 rows) list `alert_valuation` AVAILABLE (`SOURCE_TIME_MODEL`), 0.71 s cold /
0.30 s; legs carry `local_iv`, `local_delta`, `local_gamma`, `model_mark`; `model_exit` target 1 sigma / stop 0.5 sigma to the
21:00 exit with `stock_target` (NVDA 160 C: IV 0.989, delta 0.552, stock target 165.42; AMZN put 18:50: IV 0.392, delta
-0.391). NVDA 155 P (1 DTE) IV 1.420 at its 14:45 trigger; at 19:01 provider 0.978 / ours 1.087 (NVDA chain ~0.90-0.98
today; 0-1 DTE read higher with our time-to-expiry convention, known). API smoke 153 / 131 OK, unchanged. Fallback
(~4 min): `compose_env.py realtime-v2-advanced-api REALTIME_V2_OPTIONS_ALERT_VALUATION --remove` + hotfix r98 (the grant
can stay: it matches r98's registry). Live now: API r98; market-state r97 (r98 is r97 + registry, either is safe);
other backend services r96; frontend f33.

### Nov 12 (19:08-19:18 UTC): provider REST slowdown - chain captures FAILED/PARTIAL; R48 fallback exercised live

Options scheduler (`options_breadth_capture_failed`): 19:08:09 SPY `Connection refused` (Errno 111); 19:10 window
MSFT `RemoteDisconnected`, NVDA read timeout (30 s), QQQ max retries -> captures MSFT, NVDA, QQQ FAILED, SPY and TSLA
PARTIAL (DEADLINE); 19:15 window: AAPL, AMZN, IWM, MSFT, NFLX, NVDA COMPLETE but slow (IWM 74.9 s vs ~7-14 s
before, AMZN 26.9 s), QQQ FAILED, SPY/TSLA PARTIAL 0 s (the capture's window deadline spent). Host SNTP check timed out
the same minutes. Connectivity 19:13: `api.massive.com` reachable from host (0.3-1.4 s) and containers (1.5-5.5 s);
both websockets LIVE, gaps 0, Options lag 0-1 s; Equity windows COMPLETE (56 through 19:10). So a provider/REST-path
slowdown, not ours. **R48 live**: `/options/flow?underlyer=SPY` served BREADTH from the last COMPLETE capture (19:02:07,
5,068 contracts) with `latest_capture_state` PARTIAL, `latest_capture_reason` DEADLINE, at 19:13:27 - the
last-good-chain path the Nov 12 15:12 check could not exercise. The research chain copies keep the last complete rows
per contract (18,180 rows at 19:05/19:10/19:15). No action; next windows watched.
Recovered: the 19:20 window's chain captures were COMPLETE for all 9 names (SPY 46.9 s, IWM 23.3 s, QQQ 17.6 s; still
slower than the morning); Equity 58 / Options 57 COMPLETE through 19:20; streams LIVE, lag 0.

Nov 12 ~19:40 UTC, user: "confirm 1 and 2" - RF1 (f33 refresh timing on the ticker page and Options Flow) and OA1/OA2
(Options Alerts leg IV/Greeks and model target/stop) confirmed in the browser. Matrix: RF1, OA1, OA2 VERIFIED.

### Nov 12 (19:40-19:49 UTC): RF1 extended to Options Activity and Options Alerts; f34 live

User ~19:40: "Still anything could be implemented and take it to LIVE?". Frontend only: Options Activity reads like
Flow (`flowWindowMs`: release + chain capture, 50 s after each mark then 10 s, 60 s idle; was every 60 s); Options
Alerts: the run inventory, the alert list and the evaluations list read 50 s after each mark then every 10 s until the
run lands (`optionRunRefetch` on the envelope's `as_of`, 30 s idle; were every 30 s); the schedule and tier reads keep 30 s.
Test added to `tests/refreshTimingNov12.test.mjs`. **f34** `alpha-research-realtime-v2-frontend:stable-nov5-f34` =
`sha256:f46357d6a9874d824d3928bd626de2ed6b2401d45d4cc52fdfc8c848d4b88cc1`: 324 tests, 279 pass, 40 fail = f30's 40 by
name; `localhost` strings 2 = f30. Preview (live nginx config, 127.0.0.1:5186, removed): `/`, `/options/activity`,
`/options/alerts`, `/options/flow`, `/ticker/AAPL`, API proxy 200. **Live**: `-Step hotfix -Service
realtime-v2-advanced-frontend -Image f34` 19:47:03-19:48:41 (recreated 19:48:01-02); serving `index-DuZ_Tymh.js`,
healthy, API proxy 200, public domain 302 to Access. Fallback: the same hotfix with f33 `961eef3391e8` (or f30).

Nov 12 19:53-19:58 UTC watch note: market-state docker CPU 37-52% for ~2 minutes (19:53:45-19:54:52) while E:19:50 was
pending/processing; back to 1-19% at 19:57-19:58. Checkpoint size unchanged (925,486 bytes at 19:55; 925,040 at 18:19),
record rate 183/s (lower than 18:20). A 60 s py-spy sample 19:56 (`p8-market-state-20261112T1956-spot.txt`): 278
active samples; journal read 32.4%, `_persist_quote_materialization` 29.1% (restore/decode check 21.2%, the V3-1.6c
row), encode 6.5%. Window-time load, not a regression; 0 mismatch/error lines since 18:13.
