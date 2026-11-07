# Realtime schema change plan: open to change first, then 400 tickers

Status: plan for review, Nov 7 2026 (UTC). Implements G102 (user decision, Nov 6 ~20:05 UTC: adopt the legacy
versioning model). Once agreed, it replaces the step list at the end of `PLATFORM_STUDY.md` section 5. Nothing here is
built yet.

**Direction (user, Nov 7):** clean, right changes that scale to 400 tickers, but first the system must be open to easy
change. The scaling plan's exact-hash and "complete or BLOCKED" rules were an earlier design choice and no longer apply;
section 9 replaces them (the superseded places are listed at the top of `ADVANCED_REALTIME_SCALING_PLAN.md`).

## 1. Goals, in order

1. **Open to change, at 25 to 50 tickers.** The five change types in study section 3 become routine, in-hours and
   rerunnable, with no manual SQL: onboard tickers; add or update a detector; change an Options policy; ship a code fix
   and rerun the affected windows; correct data.
2. **Scale to 400 tickers** (Options to 50 underlyings) without a redesign. Every choice made for goal 1 is checked
   against 400 now, so nothing is rebuilt later.

## 2. The change in one paragraph

Today a cohort is both a **key** (it leads the primary key or index of the fact and read tables) and a **container**
(each window's release replaces every page). A cohort also bundles four things that change at different speeds: the
universe, the configuration, the model bundle and the valuation. So any change to any of them starts a new key space,
and every value meant to last the session has to be carried forward by every window. After this change:

- **facts** are keyed by what they are (instrument, interval, time);
- **configuration** (universe, detectors, policies) is a set of versions with an effective window boundary;
- **window runs** are attempts that a rerun supersedes, with an outcome per ticker;
- **pages** read entity rows through one pointer per page type.

The cohort stays, one per session, as the recorded identity of the run (ingress plan, journal, receipts), not as a key.

## 3. What the code shows (corrections to the Nov 6 comparison)

Read Nov 7 from `backend/realtime/sql_v2` (49 migrations) and the repositories. Several legacy patterns already exist in
the realtime schema, which makes the change smaller than the Nov 6 summary suggested.

| Concern | Nov 6 summary said | The code shows | Effect on the plan |
| --- | --- | --- | --- |
| Point in time | Realtime gets it from the cohort and window | `rt.bar_revisions` already has the legacy fact shape: `observed_at`, `replay_available_at`, `availability_mode`, `supersedes_revision_id`, `source_revision_ids`, `payload_sha256`, `quality_codes`. Only `rt.bar_catalog`, `rt.bar_current` and the main index carry the cohort | Bars need a new current pointer and index, not a new fact table or a data rewrite |
| Alerts | Realtime carries alerts in each publication | `rt.alert_current` is already an alert entity: key `alert_id`, upsert where a newer `as_of` wins, `alert_kind` STOCK or OPTION. Stock alerts write it (`canonical_stock_alert_repository.py`); the Options cycle writes `rt.detector_events` but not `alert_current` | G100 needs no new table |
| Strategies and detectors | A fixed list in code | `rt.detector_versions` already records each identity (id, version, code hash, policy hash, requirements). Missing: which version runs, from when, in which mode | Add an activation table, not a new registry |
| Universe | One cohort per activation | `rt.realtime_desired_revisions`, `_members` and `_member_readiness` are already revisions, but per session (`effective_session_date`), with no supersedes lineage or reason; the ingress plan is one per authorization and session (C12) | Extend the existing tables as legacy migration 039 did |
| Per-ticker outcome | Not covered | `rt.job_barriers` and `rt.job_barrier_members` already record each member's outcome (COMPLETE, FAILED, BLOCKED, MISSING); the barrier's rule is what turns one member's failure into a failed window | Change the completion rule, not the tables |
| What a cohort is | A universe | `rt.cohorts` binds `manifest_id`, `configuration_sha256`, `model_bundle_sha256` and `valuation_sha256` | Version those separately; the cohort stops changing when one of them does |
| Cost of re-keying | Not measured | One module writes bars (`equity_bar_repository.py`). But 27 runtime modules and 18 scripts query `rt.bar_current` with their own SQL, and 36 and 29 query `rt.bar_catalog` or `rt.bar_revisions`. History reads walk a recursive chain of cohort bindings (`equity_history.py`) | Route reads through one module first; otherwise the re-key changes about 45 files in one release |

Measured on live Nov 6 (study section 5): 60 of 170 `rt` tables carry `cohort_id`, 23 lead their primary key with it,
45 cohorts since Oct 5 and 28 history bindings.

## 4. Why not port the legacy schema as it is

The legacy `stocks_db_refresh` schema proved the versioning model at batch cadence on developer data. It lacks what the
live stream needs, which realtime already has and keeps:

- partitioning and retention by policy (legacy has 4 partitioned tables; realtime's `rt.partition_policies` covers
  bars, evidence, flow, snapshots and releases);
- leases, owner epochs and fenced writes, so concurrent workers can't overwrite each other;
- the stream journal with consumer checkpoints (exactly-once ingestion), the provider request ledger and budgets;
- point-in-time cutoffs per window, and the scheduler as the compute clock.

So the plan takes legacy's **versioning ideas** (revisions with lineage, the canonical choice as a rule, entity rows,
per-page pointers, a registry with effective dates, hashes recorded rather than enforced) and keeps realtime's
**production machinery**.

## 5. Target design

### 5.1 Facts: keyed by what they are

- **New `rt.bar_head`** (working name), replacing `rt.bar_current`. Primary key
  `(instrument_id, interval, bar_start, session_scope, adjustment_basis)`. Columns: the selected `bar_revision_id`
  (foreign key to `rt.bar_catalog`), `provider_tier`, `source_kind`, `bar_end`, `observed_at`,
  `selection_policy_version`, `written_by_cohort_id` (provenance only), `updated_at`; `fillfactor = 80`. A new table
  rather than an altered key, so both can be written side by side and rollback is a setting.
- **Statistics from day one:** per-table analyze settings on `bar_head` (scale factor 0, a fixed row threshold), because
  the default waits for 10% of rows to change, which is the G99 failure. Partition it by interval only if the
  400-ticker rehearsal shows the need.
- **Selection policy v1** reproduces today's choice: the source precedence in
  `equity_bar_repository._current_source_precedence`, then the active provider tier over the other (today the cohort
  implies this), then the later `observed_at`. Each row records the policy version. A policy change is a new version
  plus a recompute job over `rt.bar_revisions`; the facts are untouched. This is legacy's view approach, stored for
  read speed.
- **Indexes:** `rt.bar_revisions (instrument_id, interval, bar_end DESC)` without the cohort, and the B24 partial
  index (G99). Built per partition with `CONCURRENTLY` after a close, then attached to the parent.
- **Point in time:** live windows read `bar_head` (latest known). Reruns and replays read `rt.bar_revisions` as of the
  cutoff (`observed_at <= cutoff`, or `replay_available_at` for reconstructed history). One repository function
  chooses: if every head row in the range was observed by the cutoff, it uses the head; otherwise it reads revisions.
  That is what makes an in-hours rerun produce what the window would have produced.
- **History bindings retire:** history is bars by instrument and time, whatever session wrote them.
- **Second wave, only if the rehearsal shows the cost:** evidence and feature current (`006`, keys lead with the
  cohort) and Options volume checkpoints (`008`).

### 5.2 Configuration: versions with an effective window

Every kind has the same shape: append-only rows with a version, a hash, `effective_at` (a window boundary),
`supersedes_id`, `reason` and `recorded_by`. Triggers protect the history and always allow a new revision, as in
legacy migration 039.

- **Universe:** extend `rt.realtime_desired_revisions` with `supersedes_revision_id`, `reason` and `effective_at` (null
  means the session start). Readiness carries over by security, kind and stage target, so only new members, or members
  whose target changed, are prepared (G91). A new revision supersedes an open preparation instead of crash-looping it
  (G90).
- **Detectors and strategies:** new `rt.detector_activations` (`detector_id`, `detector_sha256` referencing
  `rt.detector_versions`, product, mode `SHADOW` / `LIVE` / `OFF`, `effective_at`, `supersedes_activation_id`,
  `reason`). Each active version's code stays in the image; the executor map is keyed by detector id and version and
  driven by the activations, which replaces the closed `DetectorFamily` enum (C14). A `SHADOW` version writes
  observations only. G101's new Options models land here.
- **Options policies** (selection, flow, catch-up, valuation): new `rt.policy_versions` (`policy_kind`, `version`,
  `policy_sha256`, `payload`, `effective_at`, `supersedes_id`, `reason`). The session binding records the version
  active at each window instead of fixing one for the whole session (C1).
- **Resolver:** one function returns the active configuration at a cutoff (universe revision, activations, policy
  versions, bar selection policy). Each window attempt records what it resolved.
- **Change signal:** `rt.configuration_generation` (one row per scope) is bumped in the same transaction as any new
  version. Workers read it once per window and reload only when it has moved.
- **Cohort:** one per session, the run's identity. A model, policy or valuation change no longer creates one.

### 5.3 Runs: attempts, reruns and per-ticker outcomes

- **Window attempts:** a rerun inserts a new attempt that supersedes the earlier one, with a reason and an actor. A
  difference from the earlier attempt is recorded, not refused (B17, C2). Read rows are entity-keyed, so the rerun
  rewrites the same rows and the earlier values stay in the revision history.
- **Per-ticker outcomes:** the window barrier completes at its deadline with each member's recorded outcome (the
  columns exist). Members that didn't complete are labelled on the page and alarmed; the others publish (C3). This
  replaces "INPUT_READY is exact/fail-closed for every cohort member" (scaling plan, Oct 14).
- **Ingress plan revisions:** `UNIQUE (authorization_id, session_date)` becomes unique per plan revision, so a blocked
  plan can be superseded and a second stream started the same day (C12, B1). A quarantined journal is reopened the
  same way, by an inserted revision rather than a manual override.
- **Read-side checks:** a reference difference at read time labels the row instead of returning 503 (C6, G97).

### 5.4 Read models: entity rows and pointers

- **Alerts:** `rt.alert_current` serves both products. For Options, `alert_id` is derived from session, detector and
  contract, so a rerun writes the same row. The first trigger fixes the entry. The hit count is counted from
  `rt.detector_events`, not incremented, so a rerun gives the same number. Last seen and lifecycle update when `as_of`
  is newer, and the alert closes at session end. Current price and P/L are read at request time, as v1 does. The
  existing index `idx_rt_alert_current_kind_time (cohort_id, alert_kind, as_of DESC, alert_id)` fits the session list.
- **Page pointers:** new `rt.portal_page_current (product, page_type, session_date)` holding the snapshot or release,
  cutoff and a generation counter. The single publisher moves it with compare-and-set on the generation. A window
  republishes only page types whose inputs changed, so the Dashboard can build once or hourly by setting.
- **URLs:** the API resolves session date (and ticker) to the pointer; the cohort leaves URLs and API parameters (G70).
- Releases and snapshots stay as the audit trail.

### 5.5 Integrity: record, label, supersede

- **Keep:** append-only triggers on facts and revisions, payload hashes as idempotency keys, leases and owner epochs.
- **Change:** hashes are recorded on every run and compared in replay; a live difference labels the affected rows and
  the window continues for every other item.

## 6. How workers integrate

Six rules every service follows:

1. **One writer per table**, through one repository module (bars: `equity_bar_repository.py`; alerts: one alert
   repository; page pointers: the publisher).
2. **Idempotent, fenced writes:** upserts with a monotonic guard (newer `as_of`, cutoff or revision wins) and the
   worker's lease epoch, so a stale worker can't overwrite a newer row.
3. **Configuration resolved once per window** at the cutoff and recorded on the attempt.
4. **No raw SQL on fact tables outside the repository.** A test fails the build if one appears.
5. **A refused precondition is a status and a retry**, never a process exit (C7).
6. **Every change ships behind a per-service mode** (`v1`, `dual`, `v2`) switched at a session boundary. Rollback is
   the previous mode, with no data change.

| Service | What changes | Step |
| --- | --- | --- |
| bridge, finalizer, gap repair, preparation (history fetch) | Bar writes already go through the repository; it adds the dual write to `bar_head`. Bridge gets the B24 index | A1, A4 |
| Equity scheduler | Window attempts and per-ticker outcomes; resolver per window; reads through the repository; republishes only changed page types; later the fan-out of per-ticker jobs | A2, A3, A4, A5, B1 |
| advanced-detectors, canonical-stock-alerts | Activations instead of the fixed set; shadow mode; per-ticker outcomes | A2, A3 |
| Options window (p8-window) | Session alert rows in `rt.alert_current` (G100); policy versions at window boundaries; per-underlying outcomes | A0, A2, A3 |
| Options lifecycle, scheduler, acquisition, ingress, flow, market-state | Policy versions instead of session-fixed hashes; a revision supersedes an open preparation (G90) | A1, A3 |
| controller, Equity ingress | Ingress plan revisions (C12); later, subscription changes at a window boundary | A2, A6 |
| preparation | Universe revisions with lineage; readiness carry-over (G91) | A1 |
| publication-retry | Moves page pointers | A5 |
| API, frontend | Options session alert list; labels instead of 503; pointer per page type; no cohort in URLs | A0, A2, A5 |
| retention | Statistics step and per-table analyze settings (G99); `bar_head` recompute jobs; retention of superseded rows | A1, A4 |

## 7. Scaling to 400

- **Window compute is the binding limit, not storage.** Measured: warm windows cost about 0.3 s per ticker (Nov 6),
  which is about 120 s at 400 if run serially; cold windows took 168-263 s at 25 tickers, roughly 7-10 s per ticker,
  which would be 45-70 minutes at 400. The window is 5 minutes and the stop rule trips at 200 s. So 400 needs
  per-ticker work units run by several fenced workers, and restarts that don't cost a cold window (C9). Entity-keyed
  outputs and per-ticker outcomes (Phase A) are what make that split possible.
- **Work units:** the window fans out one job per ticker through `rt.materialization_jobs` (leases and `SKIP LOCKED`
  claims exist) and joins on the barrier with per-ticker outcomes.
- **Storage:** 1-minute bars are about 156,000 per session at 400, about 3.3 million a month before revisions; monthly
  partitions stay workable. `bar_head` grows to roughly 7.5 million rows (today's 468,000 at 25, scaled, before
  duplicates across cohorts collapse), so its analyze settings matter from the start.
- **Reads:** a window writes only the page types that changed; read cost doesn't depend on how many windows ran.
- **Options to 50 underlyings:** a separate declared set from Equity; shard and contract-coverage changes are policy
  versions, not new session identities.
- **Database:** a stable key space keeps statistics valid from session to session; the connection budget is set per
  worker pool as workers multiply.

## 8. Order of work

### Phase A: open to change (25 to 50 tickers)

| # | Step | Schema change | Change types it opens | Gate | When |
| --- | --- | --- | --- | --- | --- |
| A0 | Options Alerts session list (G100) on `rt.alert_current` | None | — (Monday's page) | Copy replay of the Nov 6 session: one row per contract per session, identical after a rerun; live page check | Weekend, for Nov 9 |
| A1 | Readiness carry-over (G91), revision supersedes an open preparation (G90, G104), statistics step and B24 index (G99) | Small; index built after a close | Onboarding | Copy: a 50-member revision prepares only the new 25; load profile before and after | Before the 50-ticker revision after the Nov 9 close |
| A2 | Window attempts with supersede, per-ticker outcomes, ingress plan revisions, labels instead of 503 | Lineage columns on window runs and ingress plans; barrier rule | Code fix and rerun, data correction, one bad ticker | Change gates in section 9 | First, after Options on 23 is stable |
| A3 | Detector activations, policy versions, resolver, configuration recorded per attempt | 3 small tables | Detector add or update, Options policy change | Replay with old and new resolution gives identical output; an O1 version in shadow, then promoted | After A2 |
| A4 | Bar read boundary, then cohort-free `bar_head` | New table and index | Onboarding without history bindings; corrections as revisions | Backfill differences zero; one shadow-read session with zero differences | Read boundary alongside A3; `bar_head` after it |
| A5 | Page pointer per page type; URLs without cohort (G70) | `rt.portal_page_current` | Page changes without a whole-release rebuild | Validator with zero differences; old URLs redirect | After A3 |
| A6 | Universe changes effective at a window boundary | Columns on desired revisions | Onboarding within the session | Copy: add 5 tickers mid-session; no other ticker affected | Only if decision 3 says it's needed |

**Phase A exit:** every change gate in section 9 passes on a copy with the real universe.

### Phase B: scale to 400

| # | Step | Gate |
| --- | --- | --- |
| B1 | Per-ticker work units: window fan-out through the job table, several fenced projection workers | Same outputs as serial on a replayed session; window time falls with worker count |
| B2 | Warm restart and hand-over (C9) | A restarted scheduler's first window is warm |
| B3 | Second-wave re-keys (evidence, feature current, Options volume checkpoints), only if B4 shows the cost | Load profile at the rung that showed it |
| B4 | Ladder: 100, 200, 400 on a copy, then live, one rung per accepted session | Scale gates in section 9 at each rung |

**Why this order:** most Nov 6 blockers came from C1-C3 (study section 2), and reruns with per-ticker outcomes are what
make every other change safe during market hours, so A2 now comes first after the Monday items. Cohort-free bar keys
(A4) come before window-boundary universe changes (A6): with bars still keyed by cohort, an intraday universe change
needs a mid-session cohort switch and a history binding that A4 would then delete.

## 9. Acceptance gates

These replace the scaling plan's Correctness and Coverage gates and the other exact or fail-closed rules listed at the
top of that plan. Its latency, provider, database, recovery, operations and storage gates still apply.

**Change gates** (Phase A exit), each run on a copy with the real universe during a replayed session:

| Change | Passes when |
| --- | --- |
| Onboard tickers | A revision prepares only the new members; no existing ticker is re-prepared; no history binding is created |
| Add or update a detector | A new version runs in shadow, then is promoted at a window boundary; no other service is redeployed and no new cohort is created |
| Change an Options policy | A new version takes effect at the next window; windows keep running |
| Code fix | One service is deployed between windows; an affected window is rerun; its rows are rewritten and the earlier attempt is kept as superseded; no manual SQL |
| Correct data | A corrected bar revision is inserted; the head moves; affected windows are rerun; the page shows the corrected value with its lineage |
| One bad ticker | That ticker is labelled; every other ticker publishes in the same window |

**Correctness:**

- Replay determinism: the same inputs and versions give the same outputs. Hashes are recorded on every run and compared
  in replay, not checked as a gate on the live window.
- Point in time: no read row uses data observed after its cutoff (checked in replay).
- Lineage: every read row names its window attempt, cutoff and configuration versions.

**Coverage:** reported per ticker (published, labelled, missing) with an alarm. The window completes with what is
available.

**Scale** (at 50, 100, 200 and 400):

- Warm window p95 within a budget you set (proposed: 60 s, so a slow hour stays under the 150 s stop rule).
- A restart doesn't produce a cold window.
- Database load and top-statement share compared with the Nov 6 baseline (about 1.2 busy backends; four statements
  were 82% of database time, `data-audit/load_profile.sql`); watched tables' statistics newer than the session start.
- Storage keeps 30% headroom and the provider ledger stays within budget, as in the scaling plan.

## 10. Migration mechanics for bars, the template for other families

1. **Expand:** create `bar_head` and the cohort-free index (`CONCURRENTLY` per partition, after a close; `lock_timeout`
   5 s as in migration 021). Additive only.
2. **Dual write** (mode `dual`): `equity_bar_repository` writes `bar_current` and `bar_head` in the same transaction.
   One writer module, so one change.
3. **Backfill:** a resumable job after the close fills `bar_head` from `rt.bar_revisions` with selection policy v1,
   idempotent by key.
4. **Verify:** for each active member and interval, the bars today's history loader returns (cohort plus binding chain)
   must equal `bar_head`. Every difference is listed; expected ones (for example the same bar under two provider tiers)
   are decided by you before the switch.
5. **Shadow read:** one live session in which the repository reads both, serves the old and logs differences.
6. **Switch** (mode `v2`) at a session boundary. Rollback is mode `dual`, served from `bar_current`, which is still
   being written.
7. **Contract** after two clean weeks: stop writing `bar_current` and creating history bindings, keep both read-only
   for 30 days, then drop them.

## 11. Rehearsal standard for every step

- Unit tests plus the affected suites, run CPU-bounded and outside the session.
- A copy (`runner/copy_tool.ps1`) with the real universe (25 now, 50 next), not the original 9 (C10).
- A replay of a recorded session through old and new code; outputs identical except where the step intends a change.
- Load profile and window times compared with the Nov 6 baseline.
- Portal validator (`runner/portal_validate.py`) with zero unexplained differences.
- Deploy outside the session; the mode switch happens at a session boundary.

## 12. What doesn't change

The stream journal and consumer checkpoints, the bridge, the scheduler as the compute clock, the provider ledger and
budgets, window cutoffs, partition policies, leases and owner epochs, and append-only protection of facts.

## 13. Decisions

1. **Phase A order:** reruns and per-ticker outcomes (A2) before versioned configuration (A3) and bar keys (A4).
   Recommended.
2. **Bar selection policy v1:** today's rule, then active tier, then latest observed. Recommended, because it
   reproduces what the pages show today.
3. **Intraday onboarding (A6):** is "next session, preparing only new members" enough, or must a list change take
   effect within the session? A6 touches the live ingress path. Recommend deciding after A4.
4. **Resolved (user, Nov 7):** the scaling plan's exact-hash and "complete or BLOCKED" rules are dropped; section 9
   replaces them.
5. **Superseded revisions:** keep them for the fact's existing partition retention. Recommended.
6. **Scale budget:** warm window p95 at every rung (proposed 60 s).

## 14. Prerequisites and schedule (estimate, Nov 7; superseded by section 15)

**Before any G102 step except A0 (Monday Nov 9, Options on 23):** the five agreed weekend gates in
`advanced-realtime.md` (G74 item 2 chain freshness on one Options image for all 7 roles; G72 label instead of block;
CPU limits on the Options roles; the 11.5x copy rehearsal with Equity windows running; the Options-on-23 revision
applied over the Nov 9 preparation that opened with 9, G104/G90), plus B14a (Options session close) and the Dashboard
build-once change. G75 breadth goes in only if it is rehearsed on a copy by Sunday evening; otherwise it follows in the
week. If a gate fails, Monday runs the 9 liquid names, as agreed.

**Before the 50-ticker revision (after the Nov 9 close):** G86 check of the new names (renames, listings under 5
years), G94 code fix, G103 close duration measured at 50, and the rung-50 activation check on a copy. G91 is needed
only if the list changes after it is applied.

| Step | Work (working days) | Live-session needs | Earliest finish |
| --- | --- | --- | --- |
| A0 | 1 | Page check in Monday's session | Nov 8 |
| A1 | 3-4 | B24 index built after a close | Nov 13 |
| A2 | 10-12, including 2 of change-gate rehearsal | Deploy outside sessions; mode switch at a session boundary | Nov 27 |
| A3 | 11-13, including 2 of rehearsal | An O1 version in shadow for one session | Dec 11 |
| A4 | Read boundary 5 (alongside A3); `bar_head` 4 | One shadow-read session; contract after two clean weeks, in the background | Dec 18 |
| A5 | 5-6 | Validator after one session | Dec 31 |
| A6 | 5-7, optional | One mid-session add on a copy | If needed |
| B0 | 3-4: preparation speed (G89; 87 min per 16 members today, about 34 h for 375) and provider budget at 400 | — | Early Jan |
| B1 | 8-10 | Same output as serial on a replayed session | Late Jan |
| B2 | 4-5 | Restart during a session on a copy | Late Jan |
| B4 | One rung (100, 200, 400) per accepted session, copy first | Three accepted sessions | Mid Feb |

**Totals:** Phase A about 7-8 weeks (to early January), Phase B about 5-6 weeks (to mid February): about 3 months to
400 tickers live. Assumes one driver, deploys outside sessions and the market holidays (Nov 26, Dec 25, Jan 1).
Biggest uncertainty: A2 and A3 inside the Options P8 lineage (C13), which was built for two underlyings. Ways to
shorten: run A4's read boundary in its own worktree alongside A3, and do A3 for Equity detectors before Options.

**At continuous pace (Claude implementing around the clock, user asked Nov 7):** work time is about 12-15 days for
Phase A and 5-7 days for Phase B. What doesn't compress: one live session per weekday for the shadow, switch and
acceptance checks (about 8 in all), deploys and heavy tests only outside 14:00-21:10 UTC (the copy shares the live
host), overnight preparation for each ladder rung, and the user's yes for each live switch. Pipelined, Phase A
finishes about Dec 1 and 400 tickers go live about Dec 15 (about 5 weeks from Nov 10). With the live interruptions
seen so far (Nov 5 and Nov 6 each took most of a day), plan on 6-7 weeks: late December.

| Step | Work at continuous pace | Live gate | Target |
| --- | --- | --- | --- |
| A1 | 1-1.5 days | Index after a close | Nov 11 |
| A2 | 3-4 days | Mode switch, one session | Nov 17 |
| A3 | 3-4 days | One shadow session, then promote | Nov 24 |
| A4 | 3 days | Backfill after a close, one shadow-read session, switch | Nov 30 |
| A5 | 1.5-2 days | One session with the validator | Dec 1 |
| B0-B2 | 5-7 days | Restart and fan-out on a copy | Dec 7 |
| B4 | Rungs 100, 200, 400 | One accepted session each, preparation overnight | Dec 15 |

## 15. Fast track (user, Nov 7): ready early this week

The user asked for shortcuts to have a testable setup early this week. The shortcut is order, not lower standards:
the small items that cost the most time last week come first; the data-model re-key and the scale work follow. Every
item is a piece of this plan, so nothing is redone later. The platform is in development stage, so changes go to live
in place, and Monday's session is the test (no separate environment, no dual writes, no shadow sessions).

**Weekend Nov 7-8, deployed Sunday night (about 20 hours of work):**

| # | Item | Why first | Plan step |
| --- | --- | --- | --- |
| 1 | B14a: a new Options stream session ends stale ones; P8 roles get 60 s to stop. **Done, deployed Nov 7 08:05 UTC (r51)** | Nov 6's session `d42b2d84` is still LIVE, so Monday's Options start would fail | Monday blocker |
| 2 | Per-ticker outcomes: a ticker's failure is recorded and labelled; the window completes for the rest. **Done Nov 7 (v1: screening, ticker workspaces, summary, Dashboard); replay of Nov 6 14:50 on live inputs: 9 publish, 16 labelled, instead of nothing** | One ticker failed whole windows (B4, B5, B18) | A2 (first part) |
| 3 | Flags instead of 503 on page reads. **Done Nov 7 (design 6.1: charts, Screener hourly details, Dashboard, Stock Alerts); read-only check on live: today's code refuses hourly details for all 25 members, patched code serves all 25 labelled** | Pages blanked on harmless reference or lineage differences (G97, G36, G83) | A2 (part) |
| 4 | Options Alerts as a session list on `rt.alert_current`. **Done Nov 7 (design 3); replay of Nov 6's detections: the page showed 0-3 alerts per window, the session list keeps all 4, NVDA 192.5C one row with 24 trades** | Alerts left the page after two windows (G100) | A0 |
| 5 | A revision supersedes an open Options preparation; readiness carries over. **Done Nov 7 (design 5.1, 5.2; migration 049, applied Sunday with the deploy)** | Crash loop on a list change (G90, G104); everything re-prepared (G91) | A1 |
| 6 | Statistics step in the retention worker; bridge journal index. **Done Nov 7 (design 5.3, 5.4; migration 050, applied Sunday with the deploy)** | Stale statistics (B23, G99); 32% of database time (B24) | A1 |
| 7 | New members' split lookback from the history plan. **Done Nov 7 (design 5.5); Nov 9 coverage check 25 of 25** | Screening needs 283 sessions of split coverage (G94); needed for 50 | A1 |

**Testing shortcuts:** one copy of live restored once on Saturday for all weekend tests; read-only replays on live
inputs where they prove a fix (as on Nov 6); unit tests for each changed module; deploy straight to live Sunday night.

**This week:**

| Day | Work |
| --- | --- |
| Mon Nov 9 | Live test with Equity 25 and Options on 9; fix what it finds. Night: Options-on-23 gates (G72, CPU limits, G74 item 2); apply the Options-on-23 revision (item 5 lets it supersede Tuesday's preparation) |
| Tue Nov 10 | Options on 23 live, measured on live under the stop rule instead of the 11.5x copy rehearsal |
| Wed Nov 11 | 50-ticker revision if Tuesday is clean (G86 check of the new names first) |
| Thu-Fri | A4 (bars without the cohort, designed in `G102_DETAILED_DESIGN.md` section 4); P8-or-v3 decision for Options |

**Next week (Nov 16-20):** A3 (detector and policy versions), A2's rest (window attempts, ingress plan revisions), A5
(page pointers, URLs without cohort), then B0-B2 (needed for 400, not for 50). SPX/NDX (G87) after G102.

**Monday checklist (fast track):**

- Options starts without a manual fix; at the decision stop the stream session ends and no role restarts with an error.
- A ticker failure shows as DEGRADED for that ticker; the window completes.
- No page returns 503 for a reference or lineage difference; affected rows are flagged.
- Options Alerts lists the whole session's alerts, one per contract.
- Statistics fresh before the first window (status file); the bridge journal check under 50 ms.
- After the close: the close completes; the validator runs on the pages.
