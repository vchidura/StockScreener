# Platform study: making in-hours change safe and fast

Status: draft, started Nov 6 2026 18:45 UTC at the user's request. Sections marked *(to deepen)* are being extended
from code reading this weekend. Evidence references: `BLOCKERS_NOV6.md` (B1-B22),
`TICKER_ONBOARDING_GUARDS.md`, and the gap register `docs/agent-context/advanced-realtime-integration-gaps.md` (G-items).

## Why this study

The platform is meant to evolve continuously: onboarding tickers, adding or updating detector models, changing Options
policies, shipping fixes. Each of those should be a routine, quickly validated change during market hours. Instead,
every day this week went in a cycle of finding and fixing one blocker after another, live. The user's goal for this
study: understand the current implementation thoroughly, name the design constraints that make change risky, and set
out how to enhance the platform so changes can be tested rapidly and the system stabilizes.

## 1. How the platform works today *(to deepen)*

Scale: `backend/realtime` has 270 modules (about 169,000 lines; 56 modules for Advanced Options alone), plus 318
scripts, a 103-file `options` package, 380 realtime test files and 93 database migrations. Twenty containers run the
live system.

### Equity (Advanced, Massive stream)

1. **Desired state and cohorts.** A desired revision lists the members (Equity, sector ETF, Option underlying). Before
   the open, preparation brings each new member to READY (reference data, history, intervals), and at 14:15 UTC the
   session snapshot is materialized as a cohort plus manifest (Nov 6: 25 members, cohort `59947a23`).
2. **Ingress plan and stream.** The controller resolves one ingress plan per authorization and session, pinned to the
   cohort, manifest and runtime profile. The ingress role holds the provider socket and writes a raw journal
   (SQLite) per stream attempt.
3. **Bridge.** Replays the journal into canonical 1-minute bars (`rt.bar_revisions`) and checkpoints its progress per
   consumer (`rt.consumer_checkpoints`). Gaps are repaired over REST within a per-plan budget.
4. **Windows (every 5 minutes).** The Equity scheduler runs `EQUITY_INPUT_GATE` → `EQUITY_MATERIALIZE` (5m bars and
   evidence) → `EQUITY_PROJECTION` (screening, ticker workspaces, strategies, summary) → `PORTAL_PUBLISH` (release
   pointer). Projection state stays warm between windows (G85).
5. **Stock alerts.** A separate service evaluates each window's inputs into canonical batches, then publishes the
   alert projection.
6. **Read side.** The API serves published snapshots, re-checking reference and action identity at read time; the
   portal pages poll the release pointer and the session lists.

### Options (Advanced)

1. **Preparation (pre-session):** catalog and quote capture at the provider window (14:31:30), selection plan, slot
   set, acquisition binding, reference state, all hash-bound to the session.
2. **Runtime services:** acquisition (socket), ingress, flow, market-state, window, scheduler, lifecycle. Each Options
   window waits for the Equity window it is bound to, captures a fresh chain snapshot, binds it, and runs the detector
   lanes (O1, O2, O3, OS1, OS2) and the projection.
3. **Alerts:** a detection becomes an alert only when it carries a package (contract legs, entry, risk); until Nov 6
   nothing created packages (G79), so every detection was an observation.
4. **Session state is a chain of hash-bound documents** (`advanced_options_session_state.py`): package, execution
   bundle, acquisition binding, pre-open session binding, then one downstream binding per window. All are fixed when
   the session is prepared.
5. **Design generations side by side.** Modules are named by phase: P7 (selection), P8 (canary, then production), v3
   (broad stream, authority, history). The live path is the **P8 canary lineage**, designed for exactly two
   underlyings (SPY, QQQ) and "exact four" contracts, which was stretched to nine underlyings on Nov 6 with patches
   (B8-B16). The v3 broad-stream path, designed for wider scope, is not the live path.
6. **Detectors are a closed set.** `DetectorFamily` is a fixed enum (`preflight.py`) and the executor map is wired in
   code (`advanced_options_native_adapters.py`); O2 is switched off in code. Adding or changing a model means changing
   the enum, executors, loader, cycle and projection and redeploying several Options services; there is no registry,
   no versioned activation and no shadow-then-promote step.

### Publication and read model

- Windows write snapshots to `rt.snapshot_catalog`: intraday ones as `EPHEMERAL_PORTAL` in `rt.portal_snapshot_hot`,
  session closes as `SESSION_CLOSE` in `rt.portal_snapshot_kept`.
- A release (`rt.portal_releases`) groups a window's snapshots; `rt.portal_release_current` is the pointer per cohort,
  and `rt.portal_cohort_current` the active cohort the portal follows.
- The API resolves the pointer, loads the snapshot, then re-proves identity before serving: manifest and payload
  hashes, security reference revisions, corporate-action evidence since the snapshot (15 "republish required" checks).
  On a difference it refuses with 503 instead of serving the stored snapshot with a label (C6).
- Pages poll the release pointer (`/api/realtime/portal/current`) and per-page session lists; the ticker page and the
  live overlays decide "running session" from the session lists, which is why one wrong flag (B7) blanked every new
  ticker's page.

### How changes are deployed and tested today

- **Images.** Each fix is an overlay image (`FROM` the running image, `COPY` of the changed files). On Nov 6 alone:
  r35-r49 across the Equity scheduler, bridge, API, stock alerts and five Options services. Each overlay is verified
  file by file against the running image before deploy.
- **Deploy.** `deploy_candidate.ps1 -Step hotfix` replaces one service between windows (it waits for minute 3:00-4:15
  of the cycle) and checks that no other container changed; all other steps are refused during the session.
  Rollback is the previous image it prints.
- **Tests.** Unit tests in the image with the patched files mounted (about 4,000 checks available; several suites
  fail the same way with and without a change and are pre-existing). Running the large suites during the session
  slowed Equity windows (Nov 6 17:35: 129 s), so in-hours testing has to be narrow and CPU-bounded.
- **Rehearsal.** A disposable copy (`runner/copy_tool.ps1`) of the live database for full rehearsals; restoring one
  takes long enough that it is used before the session, not during it. On Nov 6 the decisive validations were
  instead read-only runs of the patched code against live inputs (section 4).

### What is already designed for isolation (keep and build on)

- **Stage isolation.** The orchestrator (`stage_orchestrator.py`, `stage_graph.py`) runs stages in lanes by domain;
  Options stages have their own lane and "never gate an equity, reference or portal stage". A failing stage is
  recorded against that stage only and its declared dependents are `BLOCKED_PREREQ`; every write is fenced to its stage
  run and window owner epoch. So isolation exists **between** stages; every whole-window failure this week happened
  **inside** one stage, because a stage succeeds or fails as a whole (C3).
- **Versioned slot sets in Options.** `rt.slot_set_versions` carry a version and an effective time within a session:
  the model Equity lacks for its membership (C11).
- **Receipts and lineage.** Every window seals receipts and evidence; that is what made today's read-only rehearsals
  possible. The same lineage can carry an audited rerun (C2) instead of forbidding one.

### When changes take effect

- **Universe.** A desired revision recorded after the open is effective from the **next** session
  (`advanced_realtime_desired_state.effective_next_session`). The session's cohort, manifest and ingress plan are fixed
  at 14:15 UTC; there is no intraday member change.
- **Preparation of new members** took about 3 h 45 min on Nov 6 (applied 05:40, READY 09:25 UTC) for 16 members:
  history, reference and intervals per member, before activation.
- **Options scope** (underlyings, contracts, slot set, policies) is prepared before the open and bound into the session
  binding and acquisition binding by hash.

### Reference and calendars

Security revisions, corporate actions and coverage, earnings and Fed calendars, fundamentals (not loaded, G37). Daily
refreshes run in the preparation role after the close.

## 2. Why changes cascade: the design constraints

The measured style of the codebase is fail-closed identity binding: in `realtime/` alone there are 4,269
`raise ValueError` checks, about 1,100 "identity/drift/changed/diverged/mismatch/immutable" refusals, 752
`canonical_sha256` identity hashes, 74 database immutability triggers, 37 SPY/QQQ literals and 37 files that use the
fixed nine-ticker `EQUITY_VALIDATION_UNIVERSE`. That style protects data integrity, but combined with the patterns
below it makes every change a potential blocker. Each pattern is evidenced by this week's live failures.

| # | Constraint | What it does | Seen as |
|---|---|---|---|
| C1 | Session-frozen identity | Plans, bindings, packages, windows and publications are hash-bound to what was prepared; any later difference is refused as drift | B9, B9b, B15, B16, B17, B14b, G96 |
| C2 | Terminal-for-the-day states | Blocked plans, quarantined journals and recorded failures are immutable evidence with no audited reopen; a rerun "diverges" | B2, B14b, B17 (two manual DB overrides on Nov 6) |
| C3 | Whole-unit failure | One ticker or one consumer failing a check fails the whole window, plan or service | B4, B5, B1, B18 |
| C4 | Scope literals in runtime code | The original 9 tickers and SPY/QQQ are hard-coded, so growing the universe breaks code that was never a guard | G92, B8, B10, G96 |
| C5 | Preparation and runtime disagree | Preparation accepts what runtime later refuses (lookback windows, split checks, slot timing) | B4/G94, B5/G95, B15 |
| C6 | Strict read-side re-validation | The API re-proves reference identity on every read and refuses on benign overlaps | B6, B6b, G97 |
| C7 | Crash instead of wait | Services exit on a refused precondition; Docker restarts them into a loop and the cause hides in tracebacks | B3, B9c, B11, B21, B22 |
| C8 | One-shot derivation | A missed window's 5m bars are never derived later; one missed window degrades the whole day | B20 |
| C9 | Expensive restarts | A restarted Equity scheduler runs a cold window (168-263 s), which can trip the Options stop rule | Nov 6 18:00 stop rule |
| C11 | Universe changes only at a session boundary | Cohort, manifest and ingress plan are fixed for the session; a revision recorded after the open waits for the next session; new-member preparation takes hours | Onboarding is always overnight; a fix to a new member's data cannot take effect until tomorrow |
| C12 | One plan per authorization per session | `UNIQUE (authorization_id, session_date)` plus `load_pinned_advanced_ingress_plan_cursor` refusing a second plan | After B1 blocked the plan, no second stream could be started for the day |
| C13 | Live path is a stretched canary | Options runs the P8 canary lineage built for SPY/QQQ; the broader v3 design is not wired in | Six layers of fixes to run nine underlyings (B8-B16) |
| C14 | Closed detector set | Enum plus hard-wired executors; no registry, versioning or shadow promotion | Adding or changing a model is a multi-service code release |
| C15 | Database maintenance not tied to change | A new cohort's rows are invisible to planner statistics until autoanalyze crosses 10% of a large table; plans flip to nested loops | Controller restart loop (B23); hourly Dashboard windows ~140 s |
| C16 | Wide-coverage needs not in the live design | Options streams 8 contracts per underlying (72 of 9,264 active contracts within 60 days, 0.8%); the profile caps 12 per underlying | Flow, screeners and volume/OI models see almost none of the market (G75) |
| C10 | Tests that never run the real universe | Unit tests and rehearsals use the original 9 and SPY/QQQ; growth paths were first executed live | Most of the above |

## 3. The change types, today versus target *(to deepen)*

| Change | Today | What broke this week | Target in-hours path |
|---|---|---|---|
| Onboard tickers | Desired revision, preparation of new members, activation at the open | B4, B5, B6, B7, B18, G92, G94, G97 | Onboarding check per member before activation; per-ticker degrade; universe from the desired revision everywhere |
| Add or update a detector model | Code change, image, redeploy of the Options services | B8-B17 (Options on 9) | Shadow lane first (writes observations only, own checkpoints), promotion at a window boundary |
| Change an Options policy | Preparation-time hashes bind the session | B9, B15, B16 | Versioned policy objects activated at a window boundary with recorded lineage, not a new session identity |
| Code hotfix | Overlay image, one-role deploy between windows | Cold restarts (C9), retries diverge (B17) | Warm hand-over; rerun of affected windows allowed with an audit record |
| Data correction | Manual SQL with a before-image (Nov 6: two overrides) | B2, B14a | Audited reopen/rerun commands in the product, not ad-hoc SQL |

## 4. A rapid-test platform *(to deepen)*

What worked on Nov 6 and should become standard tooling:

- **Read-only rehearsal on live inputs.** `runner/project_live_readonly.py` reproduced the CRWD failure and proved the fix
  on the 14:55 live inputs before deploy; `runner/alert_publish_diag.py` named the exact failing hash for stock alerts
  and proved the fix the same way. Generalize: every stage runnable read-only against a live cutoff, deployed and
  undeployed code side by side.
- **Replay of real events.** `runner/options_alert_v1_replay.py` built packages from Nov 5's real detections.
  Generalize: a replay harness for any detector or policy over recorded sessions, with expected outputs.
- **Shadow lanes.** New detectors and policies run beside the live ones, write only their own records, and are
  promoted after a measured period.
- **Onboarding check.** One report per new member and product before activation (coverage windows, splits in raw
  windows, history counts, a dry projection, an Options cycle).
- **Change rehearsal on a copy with the real universe.** Not the original 9.

### Database load during the session (measured Nov 6, 18:57-19:09 UTC, live)

Method: table counters before and after, plus a 1-second sample of every running statement (read-only;
`data-audit/load_profile.sql`). The database averaged about 1.2 busy backends. Four statements took 82% of that time:

| Share | Statement | Cause | Fixed by statistics? |
| --- | --- | --- | --- |
| 32% | Equity bridge journal check, `MAX(source_offset)` on `rt.bar_revisions` every tick (B24) | No index; scans all partitions | No: needs an index and a partition bound |
| 24% | Options binding-chain read (`advanced_options_downstream_heads` with its bindings) | Large results; mostly waiting on the client to read them | No: read less per cycle (C1) |
| 18% | Equity history loader (`equity_history.py`): `bar_revisions` x `bar_catalog` x `bar_current` for today's cohort | Join plan depends on estimates for today's cohort | Yes |
| 8% | Controller gap-repair check `load_stored_minutes_cursor` (B23) | Was 42 s with stale statistics; 0.1 s since 18:44 | Yes |

**Tables that need statistics refreshed during the session** are the large tables read by today's cohort or session,
where PostgreSQL's automatic analyze (10% of rows changed) cannot fire within a session:

| Table | Rows | Changes per hour in session | Automatic analyze would fire after | Note |
| --- | --- | --- | --- | --- |
| `rt.bar_current` | 468k | ~2,000 | ~23 h | Every bar query filters on the cohort first |
| `rt.bar_revisions`, current month partition | 560k | ~2,250 | ~24 h | Same |
| `rt.bar_catalog` | 478k | ~2,250 | ~9 h | At 19:09 still had statistics from 08:16, before today's cohort |
| `rt.bar_revisions` (partitioned parent) | all months | n/a | Never | PostgreSQL never analyzes partitioned parents automatically; none of the 18 parents has ever had statistics |

Second tier, not urgent at this size: `rt.detector_event_inputs` (24k rows, ~3 h) and `rt.provider_request_ledger`
(indexed lookups only). Not needed: the small, busy tables (`stream_sessions`, `advanced_journal_registry`,
`worker_leases`, `quote_current`, `window_runs`, ...) are already re-analyzed every few minutes, and reference tables
loaded at preparation were analyzed after the 14:33 UTC load.

**Proposed statistics step in the maintenance worker** (`run_realtime_v2_retention_service`, already live with an admin
connection, today hourly):

1. Watch list: the four entries above. ANALYZE an entry when its statistics predate the active cohort or more than
   ~5,000 rows changed since the last analyze; ANALYZE partitioned parents after the close. ANALYZE takes about 1 s per
   table and blocks no reads or writes.
2. Check every 5 minutes (one read of `pg_stat_user_tables`), so activation at 14:15 is covered before the first
   window at 14:35; the hourly cycle alone could miss it.
3. Per-table automatic analyze (scale factor 0, threshold about 5,000) on `bar_current`, `bar_catalog` and each bar
   partition, as a safety net. Parents cannot hold these settings, so the worker applies them to each new monthly
   partition it creates.
4. Last-analyzed time per watched table in the worker's status file, with an alarm when a watched table's statistics
   predate the active cohort.

## 5. Window and cohort coupling: keep them as provenance, not as keys or containers

Raised by the user on Nov 6. Measured on live: 60 of 170 `rt` tables carry `cohort_id`, 23 lead their primary key with
it and 47 lead an index with it; 45 cohorts since Oct 5 (one per activation) and 28 history bindings to stitch history
across them. Every window's publication replaces the page's whole content.

**What the coupling provides, and the portal needs:** a point-in-time cutoff (an alert uses only data known at its
trigger); a universe that cannot change mid-session; pages whose values are consistent with each other; an audit of what
a page showed and from which inputs.

**What it costs, traced to the Nov 6 findings:**

| Coupling | Consequence on Nov 6 |
| --- | --- |
| Cohort as the first key of facts (bars, catalog, current) | New key space each activation: stale statistics (B23, G99), history stitched by OR-joins across cohorts (the history loader), new members re-prepared (G91), cohort ids in URLs (G70), reference identity per cohort (G97) |
| Window as the unit of publication (full replacement) | Anything that should last the session must be re-carried every window: Options alerts leave the page (G100); a missed window leaves a hole (B20) |
| Identity as a gate | A difference refuses the whole unit instead of labelling the item (C1, C3); terminal records cannot be rerun (C2) |

**Review of the legacy schema (user, Nov 6).** The `stocks_db_refresh` schema (`backend/migrations/000_canonical_schema.sql`
and `039`-`048`) ran smoothly on developer data, and changes were easy to iterate. It already versions the things the
realtime schema freezes:

| Concern | Legacy (`stocks_db_refresh`) | Realtime v2 today | Decision |
| --- | --- | --- | --- |
| Universe | `equity_universe_runs` + `equity_universe_members` with `effective_from`/`effective_to`; migration 039 adds revisions: a new run `supersedes_universe_run_id` the old one for the same session, with a reason and hash; lineage immutable, revisions inserted | A cohort per activation, frozen for the session; a revision after the open waits for the next session (C11); one plan per session (C12) | **Cohort versioning**: a cohort is a version of membership with lineage; a revision takes effect at the next window boundary |
| Facts (bars) | `equity_bar_revisions` keyed by ticker, interval, bar start, scope and adjusted; append-only with `supersedes_bar_revision_id`; no universe in the key | Bars, catalog and current bars lead with `cohort_id`; history stitched across cohorts by 28 bindings | Bars keyed by security and time; the cohort leaves the key |
| Choosing the canonical bar | A view (`equity_canonical_bars`) ranks revisions by source; changing the policy changes the view, not the data | `bar_current` rows per cohort | A current-bar pointer per security, interval and bar start (as `equity_current_bar_projection`) |
| Point in time | `system_observed_at`, `replay_available_at`, `availability_mode` on every fact | Window cutoff plus cohort binding | Observation times on facts; the window cutoff applies at read |
| Read models | `equity_current_projection` keyed by ticker, interval, projection type and source, upserted | Each window's publication replaces the whole page | Entity-keyed current and session tables, upserted by windows |
| Portal | `equity_portal_snapshots` per page type, built from a `source_generation` counter; `equity_portal_current_projections` holds one current pointer per page type | One release per window for every page; cohort in identities and URLs | A current pointer per page type with a generation; URLs without cohort |
| Alerts | `stock_alert_result_revisions` keyed by a stable `alert_id` and payload hash: an alert is an entity with revisions | Stock alerts carried in each publication; Options alerts per window (G100) | An alert entity per session, with revisions |
| Models and strategies | `option_strategy_registry` keyed by name and version, with `enabled` and `effective_from`/`effective_to`; `equity_qualification_revisions` likewise | Closed enum, hard-wired executors (C14) | A registry with versions and effective windows; shadow, then promote (G101) |
| Hashes | Recorded (`payload_sha256`, `policy_sha256`); triggers guard lineage but allow new revisions | Gates that refuse the window (C1); terminal records cannot be rerun (C2) | Recorded, and supersede by inserting a new revision |

What the realtime platform adds and keeps: the stream journal with checkpoints and the bridge (exactly-once ingestion),
the window scheduler as the compute clock, the provider ledger and budgets, and point-in-time cutoffs. None of these
needs the cohort as a key.

**Target shape.** Keep windows and cohorts as provenance, not as keys or containers:

1. **Facts keyed by what they are.** Bars by instrument, interval and bar time; trades by contract and time. The
   universe becomes membership with valid-from and valid-to; the cohort is recorded on each run, not in each fact key.
2. **Windows compute and upsert into session read models keyed by entity and session.** For example an alert per
   session, detector and contract; a ticker snapshot per ticker and session; a screener row per ticker and session. Each
   row records the window, cutoff and input hash that last wrote it. Rerunning a window rewrites the same keys
   deterministically and keeps the previous version as superseded, so it is rerunnable and still auditable.
3. **A release is a pointer** (cutoff plus versions) over those read models, not a copy of the whole state. Pages stay
   consistent and point-in-time; the API reads rows as of the release.
4. **Identity differences become labels** on the affected rows, not refusals of the window.

| Portal requirement | How the target shape meets it |
| --- | --- |
| Point-in-time values | Every row carries the cutoff that produced it; reads are as of the release |
| Session lists (alerts, P/L) | Rows keyed by session accumulate; current price at read time (as G78 and v1 do) |
| Consistent page | The release pointer |
| History and session selector | Session is a key; no history bindings |
| Audit | Superseded versions plus the run ledger with input hashes |
| Stable URLs | Session and ticker only |

**Incremental path, not a rewrite** (recorded for implementation as G102; user, Nov 6):

1. Options Alerts as a session list (G100), shaped like the legacy alert results: an alert entity per session,
   detector and contract with revisions (first trigger, hits, last seen); the API reads it with v1 read-time marks.
   Small and isolated; for Monday.
2. Statistics step (G99): relief while facts are still cohort-keyed.
3. Strategy and detector registry with versions and effective windows, as `option_strategy_registry` (C14, G101).
4. Cohort versioning: universe revisions with lineage, effective at the next window boundary, as migration 039 (C11,
   C12). Onboarding and list changes stop needing a new session.
5. Cohort-free fact keys for bars with a current-bar pointer, as `equity_bar_revisions` and
   `equity_current_bar_projection`; history bindings retired. The largest migration: design and copy rehearsal after
   Options on 23 is stable.
6. Portal pointers per page type with a generation counter, as `equity_portal_current_projections`; URLs without
   cohort ids (G70).
7. Hashes recorded rather than enforced, and supersede-by-insert for reruns (C1, C2, C3), domain by domain.

**Nov 7: implementation plan for review in `REALTIME_SCHEMA_VERSIONING_PLAN.md`.** From the code it corrects parts of the
comparison above (bar revisions already carry the legacy fact shape; `rt.alert_current` is already an alert entity;
`rt.detector_versions` already records identities), adds a read boundary before the bar re-key (27 runtime modules and
18 scripts read `rt.bar_current` directly), and moves cohort-free bar keys ahead of window-boundary universe changes.
User direction the same day: open to change first (Phase A, 25-50 tickers), then scale to 400 (Phase B); the scaling
plan's exact-hash and "complete or BLOCKED" rules are dropped in favour of the plan's section 9 gates.

## 6. Roadmap *(to deepen: effort and order)*

1. Per-item degrade instead of whole-unit failure (C3), starting with Equity projection, stock alerts and Options
   lanes.
2. Audited reopen and rerun for plans, journals, windows and role completions (C2).
3. Universe and scope from the desired revision everywhere (C4); remove runtime literals.
4. Services report refused preconditions as status and keep running (C7).
5. Window-boundary activation for policies and detector versions (C1).
6. Re-derivable intervals for missed windows (C8); warm hand-over on deploy (C9).
7. The rapid-test tooling in section 4, as part of the product, with real-universe fixtures (C10).
8. Database maintenance tied to change (C15): the statistics step in the maintenance worker and the B24 index.
   Recorded for later implementation (user, Nov 6) as G99 in the gap register.
9. Options model strategies that cover real scenarios (user, Nov 6; G101): same-day expiries, repeated trades on one
   contract, liquidity and trade direction. On Nov 6, 26 of 28 O1 detections were same-day contracts in normal
   expiration-day trading. Needs the registry and replay tooling (C14, section 4) and the breadth layer (C16).
10. Options Alerts as a session list, like Stock Alerts (G100): today an alert leaves the page after about two windows.
