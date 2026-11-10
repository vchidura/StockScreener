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
