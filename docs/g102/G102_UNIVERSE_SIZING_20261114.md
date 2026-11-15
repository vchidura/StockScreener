# G102 R62/R63 lasting fix: universe size and membership without per-size setup

- **Status:** BUILT + REHEARSED, Nov 15 2026 (worktree `universe-sizing`, branch `universe-sizing` from `v3-live-fixes`
  60da29d; uncommitted), target the Mon Nov 16 session; gate results in `G102_EVIDENCE.md` "Nov 15 (Sun) - Universe
  sizing build" and matrix row R79. Sections 1-3 are the design, section 4 the build and its gates. Any migration,
  grant, live write or deploy needs the user's go and runs outside a session.
- **Goal (user, Nov 14):** a fresh database starts with any universe (for example 1 Equity name and 1 Options
  underlying) and grows or shrinks by changing the desired list only. No per-size registration, no admin step, no
  special case per change.
- **Why:** two Nov 13 activation failures came from per-size or per-membership details that guard nothing today:
  - R62: the controller crash-looped from the 14:15 activation to 14:35 because the 45-name list needed a new
    exact-45 runtime profile row, which its account cannot write.
  - R63: Options never ran from the open because the Options scope fingerprint included the whole Equity list, which
    changed from 25 (preparation, 07:27) to 45 (activation, 14:15).
- **Evidence:** `G102_EVIDENCE.md` Nov 13 (activation check 14:17, R62 fix 14:35:52, first windows, R63 fix forward,
  r104 rollout). **Matrix:** rows R62, R63.

## 1. Equity stream size profile (R62): full removal of per-size profiles

### 1.1 What exists today

| Item | Where | What it does |
|---|---|---|
| Reviewed sizes | `realtime/policies/advanced_equity_ingress_v1.json`, ladder in `advanced_equity_scale_v1.json` | Seven profiles: 1, 9, 25, 50, 100, 200, 400. **All 38 stream settings are identical in all seven** (checked Nov 14); only the name and size differ. |
| Exact-size profile | `AdvancedIngressRuntimeCatalog.profile_for_target` (`advanced_ingress_runtime_profile.py:236`) | Any size not on the list becomes a renamed copy of the next reviewed size: "exact-45-capacity-50" = the 50 settings. |
| Registration | `ensure_exact_advanced_runtime_profile_cursor` (`advanced_ingress_repository.py:313`), called by the controller at activation (`run_realtime_v2_advanced_controller.py:877`) | INSERT and UPDATE on `rt.advanced_ingress_runtime_profiles`. The app role has SELECT only there (`relation_registry.py`: no write flag). Refuses outright for a reviewed size (50, 100, 200, 400) that is not registered. |
| Plan rule | trigger `rt.enforce_advanced_ingress_plan_change` (`sql_v2/023`, ~line 690) | The day's ingress plan must point to an ACTIVE profile whose size equals the plan's size exactly. |
| Ladder rule | trigger `rt.enforce_advanced_authorization_promotion` (`sql_v2/025:101`) | A stream approval for size N needs an ACTIVE profile at exactly N and a READY parent approval at the previous ladder size, effective the session before. |
| Back-dated approvals | `ensure_automatic_stream_authorization_cursor` (`advanced_equity_lifecycle.py:355`) | The controller satisfies the ladder itself every morning: it writes READY approvals for 1, 9 and 25 on the previous sessions, then today's size. Live: 3 per day Nov 9-12, 4 on Nov 13 (1/9/25/45). |
| Scale gate | `advanced_equity_scale.py` (`stable_sessions_before_up: 5`, `MANUAL_GATED`) | Advisory only: used by `scripts/plan_realtime_v2_advanced_universe.py`, not by any live service. |
| Live rows | `rt.advanced_ingress_runtime_profiles` | STOCKS 1 (Oct 19), 9 (Oct 23), 25 (Nov 5), registered by the admin role with the user's approvals; exact-45 (Nov 13, admin, R62 fix). No OPTIONS rows. 50/100/200/400 are not registered. |

**Conclusion:** the per-size rows carry no size-specific setting, and the ladder is satisfied by approvals the
controller writes for itself. Together they protect nothing; their only effect is that every new size needs a row the
running services cannot create. The size-dependent part that matters, the daily stream budgets, comes from
`_continuous_session_budgets` / `automatic_advanced_ingress_budgets` (scaled by the member count) and stays.

### 1.2 Decision (recommended): full removal

One size-independent stream settings profile per feed, registered once. Any size 1-400 runs by changing the desired
list. The interim alternative (grant the app role write access so the controller registers each size) keeps the
ceremony and the back-dated approvals, so it is not the target.

### 1.3 Plan

| Step | Work | Touch points |
|---|---|---|
| 1. Migration 053 (admin, backward compatible) | Plan rule: the profile must be ACTIVE and cover the size (`NEW.universe_target <= profile.universe_target`) instead of equal. Ladder rule: keep "an ACTIVE product profile covering the size"; drop the parent/previous-size chain. The size bound 1-400, the manifest/member-count equalities and the durable-evidence rules stay. | `sql_v2/053_...sql`, replacing the two functions |
| 2. One profile | Catalog v6: one STOCKS profile with the same 38 settings, size 400 (covers 1-400), ladder position 0. Registered ACTIVE once by the admin provisioning step (the tool that registered 1/9/25); on a fresh database, part of provisioning. | `realtime/policies/advanced_equity_ingress_v2.json` (new), provisioning script |
| 3. Code | `profile_for_target(N)` returns the one profile for every N. The controller drops the exact-profile call; the automatic approval is one row for today's size, with no back-dated chain and no `promotion_ladder`. `resolve_advanced_ingress_plan_cursor` checks the one profile. Delete `ensure_exact_advanced_runtime_profile_cursor`, the exact-size derivation, the promotion ladder code and the advisory scale ladder (or keep the planner as advice only). Readers that use the profile's settings (journal replay `stale_after_seconds`, OS detectors' feed/channel filter) are unchanged. | `advanced_ingress_runtime_profile.py`, `advanced_ingress_repository.py`, `advanced_equity_lifecycle.py`, `run_realtime_v2_advanced_controller.py` |
| 4. History | The 1/9/25/45 rows, the back-dated approvals and past plans stay as durable evidence. Nothing is deleted. | none |

### 1.4 Tests

- A disposable-database activation test that runs **as the app role** (today's only test of this path runs as the
  database owner, which is why R62 passed tests): sizes 1, 45, 50 and 400; a shrink 45 -> 25 and a growth 25 -> 100
  between consecutive sessions; a fresh database with 1 name after provisioning.
- A permissions test: every INSERT/UPDATE/DELETE in service code checked against the relation registry's app-role
  grants, with an allowlist for admin tools. A scan on Nov 14 found the R62 path as the only live-path violation.

### 1.5 Rollout

1. Build and test in a worktree; rehearse activation on a database copy as the app role.
2. Outside a session, with the user's go: migration 053 first (the current code still works under the relaxed
   rules), register the v6 profile, then the controller image.
3. Fallback: the previous controller image. The migration needs no rollback.

### 1.6 Until it ships

- Monday Nov 16 at 45 names is safe: exact-45 is ACTIVE.
- If the list size changes before step 3 ships, register the exact-size row as admin **the evening before**
  (`runner/register_exact_profile_nov13.py`, generalized to take the size), never at the 14:15 activation. Sizes 50,
  100, 200 and 400 also need their reviewed row registered first.

## 2. Options universe (R63 and the wider sizing limits)

### 2.1 How the live Options universe is built

- **Live services:** `run_advanced_options_v3_lifecycle.py` (preparation: `advanced_options_lifecycle.py`,
  `advanced_options_automatic_preparation.py`), `run_advanced_options_v3_scheduler.py`
  (`advanced_options_continuous_scheduler.py`), `run_advanced_options_v3_production.py` (P8 ingress, market-state,
  flow, window), `run_advanced_options_v3_acquisition.py`. The Options feed does not use the section 1 tables
  (stream approvals, runtime profiles, ingress plans).
- **Underlyings:** the desired revision's enabled OPTION_UNDERLYING entries (`advanced_options_lifecycle.py:1644-1651`),
  any count; each must also be an enabled Equity member (`advanced_realtime_desired_state.py:96-99`); list cap 400. Live
  (Nov 14): AAPL, AMZN, IWM, MSFT, NFLX, NVDA, QQQ, SPY, TSLA.
- **Each morning** preparation selects contracts per underlying (v2: 8 each; V4: 24 each; budget split per underlying,
  provider ceiling 1,000 contracts per connection) and reads each underlying's Equity history (31 adjusted daily bars,
  the last raw daily bar, the latest 1m bar) by instrument, whoever wrote it.
- **What Options reads from Equity:** only the underlyings. The Options stream subscribes to option contracts only
  (`advanced_gateway._build_subscriptions`), and the Equity context loads only the underlyings
  (`load_advanced_options_equity_context_cursor`).

### 2.2 What limits it today

| # | Limit | Where | Real or artifact | Effect |
|---|---|---|---|---|
| 1 | The acquisition binding must list exactly the 9 October test names; every underlying must be one of them | `advanced_options_acquisition_runtime.py:136-138, 173-174, 351-352` (`EQUITY_VALIDATION_UNIVERSE`, `contracts.py:179`); preparation never passes the real list (`advanced_options_automatic_preparation.py:1559-1570`); checked by `validate_advanced_options_v3_runtime_identity` (`advanced_options_v3_deployment.py:512-516`) in the scheduler, ingress and window | Artifact (target-9 canary) | Any underlying outside SPY, QQQ, IWM, AAPL, MSFT, NVDA, AMZN, TSLA, NFLX fails ingress and every window. Works today only because the live 9 are exactly these. |
| 2 | Flow policy knows only those 9 names | `advanced_options_flow_policy.py:23-33, 416-421` | Artifact | Any other underlying gets flow silently off (`FLOW_V1_UNDERLYING_UNAVAILABLE`, `flow_eligible=False`). |
| 3 | The scope fingerprint holds the whole Equity list | preparation `advanced_options_automatic_preparation.py:1651-1682`; check `advanced_options_control_plane.py:1131-1168`; R63 payload field `advanced_options_p8_production.py:1036, 2116-2146` | Artifact | R63 (Nov 13) and G96 (Nov 6). r104 accepts growth and shrinkage; a desired name not activated on the day would still refuse Options all day. |
| 4 | Each underlying must be an AM member of the previous session's Equity cohort | `advanced_options_automatic_preparation.py:1658-1674` | Artifact (the real need is the underlying's backfilled bars) | A new underlying added with its Equity member makes the **whole** Options preparation retry all day. Options for a new name starts at its second session at best. |
| 5 | At most 50 underlyings, 128 KiB, per window input | `option_cycle_input.py:46-47, 183-185` | Prototype bound | Fails above 50 underlyings. |
| 6 | Every underlying must get at least one admitted contract | `advanced_options_session_state.py:466-480` | The 1,000-contract ceiling is real; failing all of Options is not | Above ~112 underlyings (v2) or ~37 (V4) shares round to zero and activation fails. |
| 7 | Preparation runs one stage at a time on a 2 s poll; catalog capture starts at open + 90 s | `advanced_options_lifecycle.py:1279`, `run_advanced_options_v3_lifecycle.py:293-304` | Scaling | Hundreds of underlyings delay activation. |

**Real requirements (kept):**
- each underlying is an Equity member with backfilled bars;
- at most 1,000 contracts per connection;
- at least 1 underlying (`sql_v2/003`).

A fresh database works on day 1 with a bootstrap cohort (`scripts/provision_realtime_v2_advanced_equity_scope.py`
accepts 1-400 names) and backfilled bars. No previous session is needed, and missing prior open interest is tolerated.

**The user's test case:**
- **Fresh database, 1 Equity name + 1 underlying:** works today only if that name is one of the 9 (limit 1); otherwise
  ingress fails, and flow is off (limit 2).
- **Growing Equity:** works since r104 (limit 3).
- **Adding a new underlying:** blocked by limits 1, 2 and 4.
- **Above ~37/112 or 50 underlyings:** blocked by limits 5 and 6.

### 2.3 Changes

| Step | Change | Removes |
|---|---|---|
| O-1 | The acquisition binding carries the day's underlyings (the Equity names Options reads); checks become "underlyings are in the binding" and "underlyings are Equity members today". `EQUITY_VALIDATION_UNIVERSE` leaves the live path. | Limit 1 |
| O-2 | The scope fingerprint is built from the underlyings only (`build_advanced_options_control_plane_plan`, preparation). This is the form the scheduler already accepts since Nov 6, so preparation and the scheduler always agree. Delete the G96 and R63 special cases and the `control.stored_universe` payload field. | Limit 3 (R63, G96) |
| O-3 | Flow policy gives any underlying a default tier (per-name overrides optional). | Limit 2 |
| O-4 | Preparation requires each underlying's READY Equity binding and backfilled bars, not membership in the previous cohort. An underlying that is not ready is prepared as UNAVAILABLE for the day; the others run. | Limit 4 |
| O-5 | Admission ranks underlyings when the budget cannot give each one a contract; the rest are UNAVAILABLE for the day instead of failing activation. Page or raise the 50-underlying window input when more than 50 are wanted. | Limits 5 and 6 |
| O-6 (later) | Parallel per-underlying preparation. | Limit 7 |

Order: O-1 and O-2 together (both change the binding identity), then O-4, O-3, O-5.

### 2.4 Tests

- A fresh disposable database with 1 Equity name and 1 underlying that is **not** one of the October 9, run through
  preparation, the scheduler, the window and every P8 role.
- Growth, shrinkage and a desired name not activated: the Options fingerprint is unchanged.
- A new underlying on its first day: it is UNAVAILABLE, the others run.
- More underlyings than the contract budget allows: ranked, the rest UNAVAILABLE, activation succeeds.

### 2.5 Rollout

O-1 and O-2 change identities, so they cut over at a preparation boundary: deploy the lifecycle, scheduler,
acquisition and all P8 roles together outside a session, before the 07:27 preparation, with the user's go. Keep r104's
acceptance for one release as the fallback.

### 2.6 Until it ships

Keep the Options underlyings within the October 9 names, and add a new underlying only after its Equity member has run
one session.

## 3. Universe management (user decisions, Nov 15 00:30 UTC)

### 3.1 Decisions

1. **Source of truth:** the desired-state tables (`rt.realtime_desired_*`, kinds EQUITY, SECTOR_ETF,
   OPTION_UNDERLYING) stay what the services read. The operator edits one file in the repo,
   `backend/config/realtime_universe.toml` (Equity names, sector ETFs, Options underlyings, priorities), and one command
   validates it and writes a new desired revision. A status command shows each ticker's readiness per preparation stage
   with the reason when it is not READY.
2. **No cutoff time:** a change may be applied any time before the 14:15 UTC activation. Names READY by then run that
   session; the rest join automatically when READY. A removed name stops live processing from the next session; its
   stored data is kept.
3. **Provider gaps:** a gap the provider confirms is accepted with a recorded reason (no-trade minutes R58, history
   shorter than the depth because of a recent listing, history under a previous ticker G86, a reference record the
   provider does not serve while its bars exist R59). Only an unexplained failure keeps a ticker out, with the reason
   in the status report.
4. **Options capacity:** when the contract ceiling (1,000 per connection) cannot give every underlying a contract,
   underlyings are ranked by the file's priority; the rest are UNAVAILABLE for the day.
5. **Target:** built, tested and rehearsed for the Mon Nov 16 session (user, Nov 15: "can be used for Monday run, I
   trust the rehearsal will validate everything"). The deploy still needs the user's go after the gates pass.
6. **Monday's universe stays at the 45 names and 9 underlyings** (user, Nov 15 ~00:45 UTC); the held-back names (META,
   SNDK, ROKU, DIA, ZS, MA, CRM) are added from a later session. Gate 4's universe change runs on a copy only.

### 3.2 Delisted or inactive tickers (decided Nov 15)

- **Decided once per session, before it starts:** the daily reference refresh before activation checks each member's
  provider reference. A member the provider reports inactive, or with a delisting date on or before the session, is
  **EXCLUDED** for that session automatically: it is left out of the activated list (and Options marks it UNAVAILABLE
  if it is an underlying), with the reason in the status report and the pre-open report. Its stored history is kept.
  If the provider reports it active again, it returns automatically. The universe file is never edited by the system;
  the operator removes the name when convenient.
- **Never inside a session:** the session's membership is fixed at activation (the cohort and its manifest identify the
  session), so exclusion takes effect from the next session. Inside a session a ticker that stops getting data (a
  trading halt, or a delisting effective mid-day) must not block anything: see 3.3.
- A missing reference record alone (R59, ROKU: the reference endpoint 404s while its bars are complete) is not a
  delisting: the ticker runs with a recorded reference fallback.

### 3.3 A ticker with no data inside a session

Today one member's missing minute fails the whole Equity window: the input gate raises for any member without its bar
(`stage_runners_advanced_equity.py:378-385`, `ADVANCED_INPUT_NOT_READY`; Nov 13 E:18:05 failed on a GS no-trade minute
until the repair was waived). Change: when the stream has no bar for a member's minute and the REST repair confirms the
provider has none, the minute is recorded as provider-confirmed empty (the R58 rule, shared with preparation) and the
gate proceeds; the ticker simply shows no new bar, and the report lists it. Behind a switch, rehearsed on a replayed
window.

### 3.4 A ticker onboarded before the session (Options)

A ticker added as an Options underlying whose Equity member is READY before Options selects its contracts (around the
open) is processed like every other underlying that session: contracts, flow, market state, windows and lanes. Options
evaluates each underlying's Equity readiness at contract selection, not at the start of preparation, so a name that
becomes READY during the morning is included. An underlying that is not READY then is UNAVAILABLE for the day only.
OS1/OS2 need 15 raw daily bars; Equity preparation provides them (to confirm in the rehearsal with a new name).

### 3.5 Universe limits in one place (user, Nov 15)

Every universe-size limit is a setting read by one module, `backend/realtime/universe_limits.py`; no other code holds
the numbers (a test, `test_universe_limits.py`, fails if a live module uses 400 as a size bound again).

| Setting (env) | Default | Ceiling | What raising the ceiling needs |
|---|---|---|---|
| `REALTIME_UNIVERSE_MAX_NAMES` | 400 | 5,000 | a migration (the schema bound on stream plans, approvals and runtime profiles) |
| `REALTIME_OPTIONS_MAX_UNDERLYINGS` | 50 | 50 | paging the Options window input (128 KiB) |
| `REALTIME_OPTIONS_MAX_CONTRACTS_PER_CONNECTION` | 1,000 | 1,000 | the provider's limit (also the slot-set CHECK) |

- **Where they are set:** the compose `advanced-environment` anchor passes them to every service
  (`${REALTIME_UNIVERSE_MAX_NAMES:-400}` and so on); `backend/.env.realtime.example` and
  `scripts/configure_realtime_v2_advanced_environment.py` carry them for env files. Defaults equal today's limits, so
  nothing changes until a value is set.
- **Rule in code:** entry points use the setting (the universe file, applying a desired revision, the activation's
  stream approval, the session budgets, resolving the profile for a size, the Options contract set). Checks on stored
  data use the schema ceiling, so lowering the setting never makes earlier sessions unreadable.
- **Schema:** migration 053 (not deployed before this change) relaxes the three 400 bounds to 5,000 and registers the
  size-independent profile `advanced-equity-ingress-v6` covering 5,000, so raising the setting up to 5,000 needs no
  migration and no new profile.
- `scripts/manage_realtime_universe.py check` prints the effective limits; an invalid value fails at first use with the
  setting's name.

## 4. Build for Monday (work packages)

| WP | Scope | Sections |
|---|---|---|
| A | Equity size: migration 053, one profile, controller and automatic approval, plan resolution; app-role activation test; permission test | 1.3, 1.4 |
| B | Preparation and delisting: R58 confirmed-empty minutes, R59 reference fallback, G86 renamed and recently listed tickers, pre-session delisting exclusion, the in-session confirmed-empty minute (switch) | 3.1-3.3 |
| C | Options: O-1 to O-4, O-5 ranking (paging above 50 underlyings later), readiness at contract selection, snapshot `Timeout` as a labelled skip | 2.3, 3.4 |
| D | Universe file, apply and status commands, pre-open report lines | 3.1 |

**Gates before the deploy (all read-only on live or on a copy; none during a session):**
1. Touched suites and the new tests pass; no new failures against the baseline.
2. App-role activation on a disposable database at sizes 1, 45, 52 and 400, a shrink and a growth between sessions,
   with no admin step.
3. Fresh disposable database: 1 Equity name and 1 underlying outside the October 9 through preparation, activation,
   the Options binding and every role's identity check.
4. A universe change through the new command on a copy of live (for example the 45 names plus the later batch, one
   removal, one new underlying): new names reach READY or are kept out with a reason; activation at the new size.
5. Delisting: a member the provider reports inactive is excluded before the session with a reason; nothing else is
   blocked.
6. A replayed Nov 13 window with a confirmed-empty minute passes the input gate.
7. V4 capacity replay on the Nov 13 journal (decides V4 or v2 for Monday).
8. The Monday pre-open dry run on the candidate images.

**Deploy (user's go):** migration 053 and the profile registration (admin), then the images for the controller, the
Equity preparation and scheduler, and every Options service together, before Monday's Options preparation starts.
Fallback: the current images per service (section 2 of the handover); the migration needs no rollback.

## 5. Related items (unchanged by this design)

- R66: gap repair must queue the 5m finalizer for repaired 1m windows so the derived bars follow a late start.
- Test baseline: retire or fix stale tests (for example the 8 `test_advanced_options_p7_shadow` failures: the P7
  harness requires the migration list to end at 031) and mark the rest as expected failures with a reason, so the
  gate becomes "0 failed".
