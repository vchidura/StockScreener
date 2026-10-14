# Advanced Real-Time Implementation Tracker

Forward-ingestion stage: Developer matrix/O1 context, original Flow/Screener and
observation-only Alerts plus indicative O1 package inputs implemented offline.
See [forward-ingestion design](ADVANCED_REALTIME_FORWARD_INGESTION.md)
for ownership, lifecycle, contracts, capacity formulas, recovery and authorization gates.
This does not change the runtime availability statements below.

Status: Full original portal routes and shared left navigation are restored in
realtime mode. Stock Screener and Ticker Workspace are `CODE_COMPLETE_OFFLINE`.
Stock Alerts has supported source/offline API/DOM parity; its durable forward
checkpoint loader, replay cohort and publication context producers remain BLOCKED.
The original Dashboard and All Tickers have supported immutable readers and a
stored-only shell freshness contract, but the current cohort has no durable
`DASHBOARD_V1` publication and remains `DATA_UNAVAILABLE` at runtime.
Options Activity and Flow now use an exact-cohort stored-only unavailable contract;
the selected cohort has zero option underlyings and no complete matrix publication.
Options Screener now has its exact pure catalog and a cohort-bound unavailable
eligible-chain contract; no discovery or alert evidence is relabeled as chain rows.
The original Options Alerts endpoint family now has exact-cohort unavailable
contracts; the validation-only projection is not used as a page substitute.
All Tickers pointer sort/columns passed; refresh is DOM-only. Dashboard and Ticker
pointer interaction is unverified. The eight-ticker option selection remains BLOCKED.
Unsupported market and persistence pages remain visible and
fail closed; their exact missing API contracts are tracked below. Earlier
nine-ticker validation evidence is not full portal parity.
Updated: 2026-10-09.
Design: [Advanced Real-Time Preflight Design](ADVANCED_REALTIME_PREFLIGHT_DESIGN.md).
Plan: [Advanced Real-Time Implementation Plan](ADVANCED_REALTIME_IMPLEMENTATION_PLAN.md).
History: [Verification archive](archive/ADVANCED_REALTIME_VERIFICATION_LOG_2026-10-04.md).
Checkpoint: [Advanced realtime](agent-context/advanced-realtime.md).

This tracker contains only active state and gates. The archive preserves the complete
64-entry verification history and 42-entry dated decision log. Code completion does
not authorize any provider, database, role, migration, service, publication, route,
retained-data, OAuth, preview or order operation.

## Delivery Target And Active Stage

End objective: one isolated nine-ticker Developer vertical slice, including NFLX, that reuses the stable
60-DTE option acquisition/normalization and technical/detector contracts, persists every
stage in `stocks_realtime_v1`, and exposes manually comparable ticker technicals, screening,
stock alerts and option alert evaluations through the standalone realtime API/portal.

Developer is the first delivery sequence only. Shared discovery/evidence/manifests and
downstream contracts remain capability-profile, universe, policy and provider-tier scoped
for the planned Advanced event path and 400/50 expansion; no second options engine is allowed.

This objective does not mean every canonical page. Market Conditions, Discovery, Options
Flow, Dashboard and other pages require a reviewed producer/reader/API/frontend mapping
before they may be claimed. There is never a canonical fallback or cross-database compare.

Current stage: **Developer O1 context and original Activity/Flow/Screener producers
plus forward acquisition/storage/publication/reader wiring are implemented in source.
Disposable PostgreSQL storage/roles accepted; actual target/provider rollout remains gated**.

### Deferred: Database-Driven Universe And Scale Expansion - 2026-10-09

**Status: DEFERRED / NOT_STARTED.** User requested this work only after the initial
nine-ticker monitoring looks good for one complete trading day. This is a tracked
future implementation, not approval to expand/enroll tickers or change runtime now.

**Start gate (NOT_MET):** complete one actual exchange-day Developer monitoring run
for AAPL, AMZN, IWM, MSFT, NFLX, NVDA, QQQ, SPY and TSLA, through the exchange close
plus the 15-minute provider delay and final materialization/publication. Verify
all nine names' supported equity/options coverage, actual source/receipt/publication
clocks, existing expiry/identity/integrity gates, bounded resource usage and original
stored-only UI responses. Failures/missed windows must be visible and accepted,
never backfilled as original observations or hidden as zero results. Manual API/
frontend validation and explicit acceptance of the day's monitoring are required;
process uptime, offline tests or a READY label alone do not satisfy this gate.

**Implementation scope after acceptance:**

- Store approved selected equities and option underlyings in the isolated realtime
  database and load them through effective-dated universe/manifest/cohort membership.
  Presence in reference tables alone must not enroll an instrument. No canonical
  selected-ticker fallback, hardcoded Python watchlist or per-tick universe query.
- Generalize the current eight/nine-name CLI, configuration/cohort bindings, bootstrap
  and worker validation paths. Universe/profile hashes, membership and policy/source
  identities must remain exact; new selections get new immutable enrollments rather
  than rewriting either existing cohort or its evidence.
- Make processing partitions, per-underlying batches, aggregate matrix/serialization
  budgets, provider/account pacing, connections and subscription allocations reviewed
  configuration. Preserve native standard/DTE/liquidity filters and failure gates;
  never truncate eligible chains or weaken policy to satisfy capacity.
- Support intermediate 50 then 100 selected-name configurations and the design target
  of 400 equities /50 option underlyings, with 100 option underlyings a later stage.
  Distinguish equity count from option-underlying and contract/subscription counts.
  At current 16 primary contracts per underlying, 50 uses 800 primary slots while
  100 needs 1,600: review entitlement/allocation before expansion, not silent depth
  reduction or a hardcoded cap increase. Full REST chain coverage remains separate.

**Acceptance criteria:** after this generalization, moving between approved 9/50/100
database-owned selections requires only enrollment/configuration changes, not source
edits. Qualify exact membership, new-cohort isolation, all-name coverage, partition
reconciliation, provider requests/bytes, memory/disk/connection use, latency, expiry
and restart/recovery at each size. Unsupported or over-budget configurations must
fail before activation. 50/100 capability and production readiness are NOT claimed.

Pre-session preparation remains discovery/validation/manifest enrollment work, not
the continuous strategy worker or an automatic DATA_READY/execution switch.

### Read-Only Target Preparation - 2026-10-08

1. User approved only target read-only prerequisite inspection and proposing
  AAPL/AMZN/IWM/MSFT/NVDA/QQQ/SPY/TSLA. Fresh
  [report](developer_forward_target_preflight_20261008_53a7778d_v3.json) verifies exact
  target/read-only identity, 000-017 applied, 018/five tables absent and 11 NOLOGIN roles.
2. Equity-only cohort remains unchanged. An older eight-option cohort has no
  joined tier binding and is not approved for reuse/rewriting/repinning. Each
  proposed ticker has retained historical security revisions; none matched the
  current effective-window query. Current catalog/OI through those identities was
  not found, so exact loader resolution is a gate, not evidence repair permission.
3. [Staged rollout plan](DEVELOPER_FORWARD_TARGET_ROLLOUT_PLAN_2026-10-08.md) separates
  018/security preparation, concrete loader/pool/launcher bindings, new cohort /
  exact full-chain policies/budgets, and one bounded provider/publication/reader
  acceptance before resident launch. Code ceilings are not provider authorization.
4. Separate preparation reporting does not weaken the strict verifier or label
  existing cohort counts as approval. 2 offline tests passed; no target writes,
  grants/unlocks/providers/enrollment/services/publication/backfill/replay/repair
  occurred. Prior SQL/full/frontend evidence stays carried in this planning phase.

### Disposable PostgreSQL Acceptance - 2026-10-08

1. **Authority:** user approved only create/test/drop
  `test_realtime_developer_forward_20261008` and unique `test_rt_forward_20261008_*`
  roles, with existing administrator connection used solely for that isolated
  acceptance. No real cohort, provider, target/canonical DB, principal unlock or
  worker/listener action was authorized or performed.
2. **Runner:** [exact-scope script](../backend/scripts/accept_realtime_developer_forward.py)
  refuses preexisting objects and nonapproved names, preserves failure reports,
  reports no credentials, creates NOLOGIN test roles and exercises permissions via
  SET LOCAL ROLE. Applied source migrations 000-018 only in the disposable target.
3. **Local fixes:** fixture updated for migrated profile identity; owner/admin
  test authority made explicit; calendar fixture aligned to native serving cutoff.
  Real role checking exposed publisher `FOR UPDATE` on compute work, which required
  forbidden write authority. Removed that write lock, retaining completed-status,
  immutable artifacts and atomic publication checks; no grants were widened.
4. **Fresh PASS:** 14 real PostgreSQL checks cover migrations/NOLOGIN roles, denied
  capability operations, simultaneous SKIP LOCKED claims, active lease/stale epoch,
  compute rollback, atomic release/immutable retry, older-pointer rollback, stored
  reader identity/reference/tier/clocks, serving expiry and mutation refusal.
  Cleanup COMPLETE with catalog-confirmed absence of test DB/roles. Exact result:
  [v5 report](developer_forward_disposable_acceptance_20261008_53a7778d_v5.json).
5. **Regression evidence:** corrected narrow offline lifecycle/migration/security
  gate passed 41 tests, 6 warnings, 31.14s. Source/task diagnostics clean. Older full
  293-pass/one-inventory-failure then 33-pass correction and 26/31 frontend evidence
  remains carried; no new all-green full-suite or market-data integrity claim.
6. **Remaining:** acceptance used synthetic SQL/storage fixtures, not complete
  provider facts, detector coverage, pool login isolation, retention/performance
  or deployed target readiness. Next requires exact target migration/grants/pools/
  loaders plus new option-bearing enrollment/policies/full-chain/request/byte/time
  scope, followed by separately approved one provider/publication cycle. Current
  equity-only cohort stays immutable and runtime options remain unavailable.

### Developer Forward Lifecycle Source Wiring - 2026-10-08

1. **Storage/migration:** new unapplied `018_realtime_developer_forward` retains
  immutable credential-free typed source inputs, all six native/original artifacts
  and atomic releases. Mutable bounded work/current ledgers are separately fenced;
  role allowlists distinguish ingress, compute, publisher and stored-only API.
2. **Complete policy scope:** additive `PolicyChainMatrixScope` admits full reviewed
  catalogs with separate matrix/dataset identity; selected manifest remains frozen
  and legacy exact-selected paths remain unchanged. Discovery cannot silently drop
  unknown references or masquerade selected slots as full chain coverage.
3. **Runtime source:** `forward_runtime.py` connects actual-clock release admission,
  native paced paginated REST adapter, target-local supplied context/package loaders,
  immutable persistence, compute claim/owner/epoch/lease/deadline fencing, native
  materialization and separate atomic publisher. No code fetches/repairs on GET.
  Exchange session/early-close cutoffs, 15-minute delay, explicit completion/serving
  budgets, failure-counting retries/Retry-After/timeouts and restart reconciliation.
4. **Readers:** original Flow/Screener/Alerts repositories now resolve one stored
  released artifact group, verify payload/manifest scope/hashes, exact reference/
  action revisions, tier/source watermarks and serving expiry, then use original
  query contracts. Equity-only/current original unavailable responses are preserved.
5. **Verification:** focused 15-test gate passed (6 warnings, 38.85s), including
  recording-store ingress -> complete matrices -> fenced artifact group -> atomic
  release -> stored original API; raw/full-policy distinction, transport budgets,
  retry/timeout accounting, early-close schedules, rollback/owner/expiry/tamper checks.
  Combined gate: 293 passed, one schema-inventory failure, 18 warnings, 2261.30s.
  The source database-validation inventory was updated for 018's five tables;
  focused migration/security rerun passed 33/33 in 0.58s. No full green rerun claim.
  Existing Stock/Options frontend contract suites freshly passed 26/26 and 31/31,
  zero failures/skips; no frontend source/layout or server lifecycle changed.
6. **Not activation:** no DB/provider, migration application, role mutation,
  option-bearing enrollment, worker/listener, durable publication or retained-data
  operation occurred. SQL syntax/concurrency/grants and real capacity/provider/data
  acceptance require explicit disposable/target authorization; no DATA_READY claim.
  Existing current equity-only cohort is untouched. Separate Advanced stream lane,
  complete quote/execution contexts and licensed retention operations stay gated.

### Delayed Indicative Display Correction - 2026-10-08

1. **Clarification:** user requires Developer snapshots to materialize as delayed
  indicative research, like the existing stock/option alert solution. The prior
  prototype conflated display availability with full event/execution readiness.
  That blanket display block is superseded; missing/blocked invalid inputs and
  native thresholds/deadlines remain unchanged.
2. **Original owner:** [Alerts adapter](../backend/realtime/option_alert_projection.py)
  uses native 20-alert selection, evidence/run builders and original review owners.
  Rows preserve original packages/legs/management, missing current marks and
  `event_horizon_status=UNAVAILABLE`. Opt-in display uses a distinct contract-scoped
  dataset; observation-only artifacts are unchanged. Other detector context stays
  explicit unavailable; known event blocks and invalid exact inputs still fail closed.
3. **Clock/history:** market cutoff, actual receipt and display publication remain
  distinct. Header is existing 15-minute delayed/read-only. No input timestamp is
  shifted and default v4 stays deadline-blocked. History takes sealed original
  selections; latest work is selected at actual materialization time, never replayed
  or backdated. Native repeats/hits and immutable source/economic hashes are retained.
4. **Fresh checks:** 14 focused tests passed (5 warnings, 37.32s), including native
  row parity, delayed clocks, unresolved warnings, economics/snapshot/plan/scope/
  deadline rejection, history/repeat identity and no read-side compute. Existing
  Stock Alerts frontend passed 26/26; Options Alerts frontend passed 31/31. Full
  guarded forward/options gate passed 245 tests, 17 warnings, 277.79s. The broad unchanged
  Stock Alerts backend fixture expansion was cancelled without a verdict; no pass claimed.
5. **Boundary:** no stock/option detector, native calendar/policy, API route/reader
  or frontend layout was changed. Real current cohort stays equity-only and options
  remain runtime unavailable; this is offline original-response compatibility, not
  an approved live enrollment/publication. No DB/provider/principal/migration/
  worker/listener/durable or retained-data action occurred. Browser/live rendering
  is NOT_CHECKED; existing frontend tests are not browser evidence.
6. **Next:** original candidate detail/preview research extraction over retained
  native inputs, keeping quotes/execution unavailable. Complete event-context
  expansion remains separate, not a prerequisite for merely displaying native
  indicative rows. Runtime enrollment/provider/schema/role/worker/publication/
  replay/retention operations still require separate exact authorization.

### Native O1 Indicative Package Inputs - 2026-10-08

1. **Source:** [package adapter](../backend/realtime/option_package_context.py)
  reuses native strategy scan, package assessment, raw leg/reference binding,
  stock confirmation, equity materializer/structural binder and dual-origin
  qualifier. Existing O1 context exposes bound native sources whose hashes must
  match its frozen observations; its prior hashes/dataset identity are unchanged.
2. **Policy finding:** carried synthetic OI=100 was below the native package floor
  250; the default v4 candidate deadline also expires at Developer release. Neither
  threshold nor deadline was relaxed. After explicit user approval, a separate
  synthetic cohort/manifest/configuration pins existing technical-forward v2 and
  raw-finalized-spot valuation v2. No policy artifact, default, enrollment or real
  data was changed. The old-policy rejection remains a regression.
3. **Positive but incomplete:** the richer raw fixture contains native completed
  support/resistance, OI=300/day-volume=1200 and exact 1m spot/reference/event facts.
  It produces one `QUALIFIED_INDICATIVE` technical plan, with full native evidence
  and frozen management/holding clocks. Native event-horizon v1 still declares
  EX_DIVIDEND/CORPORATE_ACTION unavailable even when earnings/Fed checks are clear.
  Receipt stays PARTIAL/unavailable, publication/execution false. No alert selector,
  page substitution, candidate API or quote/preview activation was added.
4. **Fresh checks:** 27 focused tests passed (5 warnings, 53.72s). Full restored
  task passed 231 tests (17 warnings, 235.75s): all prior 189, 27 package tests and
  15 existing native raw-basis/technical qualification cases. Includes missing,
  stale/blocked calendars, exact reference/context/spot/structure drift, tiers,
  cutoff/deadline/policy isolation, immutable retries, no read-side work and stable
  dataset / cutoff-specific input-plan identity. Touched diagnostics clean.
5. **Boundary:** eight synthetic names/16 contracts are not production chains,
  allocation, enrollment, capacity or durable readiness. Native/current realtime
  API/readers/frontend, the equity-only cohort and policy files remain unchanged.
  Original pages stay `CODE_COMPLETE_UNAVAILABLE`; no DB/provider, role, migration,
  worker/listener, durable publication, retained-data or retention operation ran.
  Reported stopped/locked state remains NOT_CHECKED; parent timing/storage unchanged.
6. **Next:** review an opt-in versioned complete holding-event contract and
  target-local ex-dividend/corporate-action evidence, retaining legacy v1 and strict
  missing/freshness/deadline gates. Do not publish current indicative packages or
  activate the synthetic policy scope. Durable/schema/security/enrollment/provider/
  service/publication/replay/retention gates still require exact separate approval.

### Original Alerts Review Projection - 2026-10-08

1. **Owner:** [target-local adapter](../backend/realtime/option_alert_projection.py)
  consumes explicit immutable cycle/context receipts, decodes native run/evaluation
  contracts and reconciles record IDs/hashes/status counts, source matrix clocks,
  cohort/tier/manifest/configuration and reference/action scope. No canonical
  repository, configuration file, launch, status journal or quote reader is used.
2. **Original contracts:** native `build_detector_evaluation_review` and
  `build_detector_alert_review` prepare original response views before sealing;
  dataset index follows retained original semantics without fabricating a launch.
  Stored reads validate requests, then filter/page precomputed views. Native O1
  observations remain observations, not candidates or selected alert hits.
3. **Availability:** explicit complete O1 requests may return original evaluations;
  genuine complete zero observations remain available. Partial/missing/stale
  context fails at the requested underlying scope. Detector-runs retains original
  run/observation summaries but stays unavailable for missing package context.
  All-detector/O2/O3/S1/S2 and schedule/candidate/quote/preview remain unavailable;
  absent quotes retain null source clocks and never trigger a provider fallback.
4. **Fresh evidence:** guarded task passed 189 tests, 16 warnings, 75.88s, adding
  44 Alerts checks to the prior 145-test gate. Native parity across all eight sorts,
  filters/paging/history, complete/zero/partial/missing inputs, stale calendar/
  history/windows, native count/hash/clock/reference/action drift, cohort/tier/
  dataset/deadline isolation, request validation, immutable retry and no-read-side
  engine/source work passed. Source/test diagnostics clean; broader evidence stays
  carried for files outside the exact command in the forward-ingestion design.
5. **Limits:** bounded O1-observation slice, not complete Alerts delivery. It does
  not substitute an internal validation response, fabricate package evidence or
  certify target SQL/readiness, production chains/enrollment, retention, capacity
  or live delivery. Original API/repositories/frontend and equity-only cohort were
  unchanged; runtime pages remain `CODE_COMPLETE_UNAVAILABLE`. No runtime/data
  operation ran; stopped/locked state remains reported, NOT_CHECKED.
6. **Next/gates:** target-local O1 package qualification using native strategy and
  qualification owners with exact candidate/reference/snapshot/raw-spot/structural/
  event inputs. Missing dependencies stay unavailable. Durable schema/roles,
  acquisition/enrollment, workers/listeners, publication/replay/recovery and
  retention require separate explicit approval. Parent latency/clock/storage gates
  remain unchanged and unmeasured; Advanced stays a separate streaming workstream.

### Original Eligible-Chain Screener - 2026-10-08

1. **Producer:** [immutable bundle](../backend/realtime/option_screener_projection.py)
  seals native snapshot rows, original mark/Greeks/clock eligibility, matrix
  coverage and source identity once. Flow/Screener reuse the same full-policy-chain
  evidence gate and normalizer reconciliation; no subset or alternate tier/source
  can supply missing coverage. Flow input-hash compatibility is regression-pinned.
2. **Query:** [pure assembly](../backend/options/analytics/eligible_chain_projection.py)
  reuses native request/range validation and preserves original response fields,
  all 12 inclusive filters, contract/session/underlying selection, stable ties,
  nulls-last sorting in both directions, totals and paging. Covered zero matches
  remain available; missing/incompatible inputs remain unavailable. No analysis,
  normalization, fetching or publication occurs in stored queries.
3. **Precision/identity:** exact Decimal filter/sort values live outside original
  JSON rows, whose numeric types still match the original SQL JSON contract.
  Scope/ref/action/tier/deadline fences, immutable retries and stable dataset /
  cutoff-specific artifact identity remain unchanged. Original canonical handler,
  realtime routes/readers, static catalog and frontend were not edited or imported.
4. **Fresh gate:** 145 passed, 16 warnings, 35.77s; reusable guarded process task
  adds 46 Screener/shared-gate checks to the prior 99-test slice. Tests cover original
  assembly and snapshot decoding, positive/zero/partial/missing inputs, all range
  fields, nulls/ties/paging, numeric precision, missing Greeks/source clocks,
  request validation, full-chain/subset discrimination, reference/action drift,
  tier/cohort/deadline isolation, immutable retry/read and adjacent cutoffs.
  Touched source/test diagnostics clean; broader 89-test evidence remains carried.
5. **Boundary:** original current-cohort options pages remain
  `CODE_COMPLETE_UNAVAILABLE`. Eight synthetic names/16 contracts are not enrollment
  or real full-chain evidence. SQL loaders/publications, live reader serving gates,
  acquisition and exact package/event evidence remain missing. No DB/provider,
  worker/listener, role, migration, durable data/publication or replay action ran;
  reported stopped services/locked principals remain NOT_CHECKED.
6. **Next:** original Alerts dataset/run/evaluation projection extraction using
  native immutable evidence, keeping missing package/candidate/quote/preview data
  unavailable. Package-dependent context and each runtime operation require a
  separate bounded authorization. Parent clocks/latency, protected journals,
  archives/checkpoints, one-year input retention and 30% headroom are unchanged.

### Original Activity/Flow Projection - 2026-10-08

1. **Dependency:** original Flow assembly was embedded in the canonical handler.
  [Pure assembly](../backend/options/analytics/flow_projection.py) now preserves
  summaries, ratios, premiums, expiry/strike profiles, top-24 ordering, null OI
  change and all original definitions. Canonical API was read only as a contract
  reference; its SQL, handlers and response shape were not edited or imported.
2. **Coverage:** [offline producer](../backend/realtime/option_flow_projection.py)
  requires target-local Developer evidence, native terminal discovery boundaries/
  hashes/pages and full native-normalizer reconciliation. A selected matrix with
  additional eligible contracts fails closed; absent full-chain proof is explicit
  unavailable, not original-page success. Exact catalog/reference revisions are
  checked even when their economics happen to remain equal.
3. **Contracts:** sealed artifacts contain the original `OptionsEnvelope` body;
  scope, input hash and complete/partial/missing coverage are separate metadata.
  Typed settlement pairs preserve dates/revisions/availability and matched coverage;
  absent OI pairs remain null. Immutable fences/retries, source clocks, stable
  dataset continuity and stored-only reads reuse the existing publication boundary.
4. **Fresh evidence:** reusable guarded process task passed 99 tests, 15 warnings,
  25.40s, adding 27 Flow tests to the prior 72-test gate. Tests read only original
  Python assembly expressions with AST, not canonical runtime/SQL. Synthetic
  positive, actual-zero, partial/missing, extra-member, stale/OI/reference/action/
  tier/deadline, immutable retry and no-GET-work checks passed. Diagnostics clean.
5. **Scope:** no real chain coverage, enrollment, durable publication, SQL role,
  provider or live delivery is certified. Original pages/current equity-only cohort
  remain `CODE_COMPLETE_UNAVAILABLE`; API/readers/frontend were untouched. Existing
  internal projections were not relabeled. No runtime/data action occurred and
  reported stopped services/locked principals remain NOT_CHECKED.
6. **Next:** original eligible-chain Screener extraction and stored-input query
  contracts sharing this full-chain gate; then exact package-dependent Alerts.
  Durable schema/loaders/acquisition, new option-bearing cohorts, providers,
  workers/listeners, roles, publication, replay/recovery and retention each still
  require separate explicit authorization. Parent timing/storage gates unchanged.

### Developer O1 Context Dependency - 2026-10-08

1. **Nearest missing dependency:** the forward seal treated any detector input
  carrier as available, including an empty carrier. The new regression failed
  before correction. Arbitrary carriers no longer certify available context.
2. **Source:** [forward boundary](../backend/realtime/forward_ingestion.py) now
  consumes explicit `stocks_realtime_v1`/Developer evidence receipts for security,
  dated OI, raw final RTH history, split coverage, exchange sessions and prior
  activity. Existing activity/trend/O1 builders and the native collector own all
  calculations and recurrence. This is pure assembly, not a durable SQL loader.
3. **Identity/safety:** context receipts pin exact matrices, source revisions,
  availability and policy hashes. Dataset identity v2 additionally pins native O1
  and calendar policies, stays stable across cutoffs and does not alias durable v1
  data. Missing/stale dependencies are explicit; partial context and matrix
  coverage differ. Immutable retry/drift/deadline/tier/no-fallback fences remain.
4. **Fresh evidence:** 51 focused tests passed (5 warnings, 19.90s). Final reusable
  guarded process task passed 72 tests (15 warnings, 22.12s), including native O1
  and all three original options unavailable endpoint families. Sixteen synthetic
  research observations are NOT alerts, eligible-chain coverage or enrollment.
  Earlier 89-test evidence remains carried for files not rerun in this phase.
5. **Remaining boundary:** package/structural/event-calendar context, O3/S1/S2,
  durable context reads/input retention and original Activity/Flow, Screener and
  Alerts projection extraction remain missing. Internal projections still declare
  `original_page_contracts=false`; the equity-only cohort and original pages remain
  `CODE_COMPLETE_UNAVAILABLE`. No portal/API/native engine source was changed.
6. **Next/authority:** extract the original Activity/Flow owner with parity fixtures
  and full-chain gates, then Screener and package-dependent Alerts. No DB/provider,
  worker/listener, enrollment, migration, role, durable publication, retained-data
  or retention operation ran or is authorized. API/Vite stopped and principals
  locked remain reported, NOT_CHECKED. Parent timing/retention/headroom gates remain
  unchanged and unmeasured. Exact test commands and recovery are in the design.

### Conformance And Screener Foundation - 2026-10-08

1. **Dataset continuity:** stable cohort/tier/manifest/policy dataset ID now spans
  cutoffs; native run IDs and full reference/action/cutoff scope remain distinct.
  The adjacent-cutoff regression failed before correction and passed afterward.
  Cohort, tier, manifest and policy isolation checks remain enforced.
2. **Design reconciliation:** restored parent 25ms p95 journal and 500ms p95 /
  1,000ms p99 receipt-to-alert targets and all original timing boundaries; restored
  1-year alert/input retention, 2-hour protected journal, 7-session raw archive,
  30-session sparse checkpoints and 30% disk headroom. These remain unmeasured
  acceptance criteria, not active policy changes or operational guarantees.
3. **Screener groundwork:** [typed definition/compiler](../backend/research/screener_definition.py)
  reuses native Stock predicates and Options ranges; catalog hashes, strict
  validation and stable semantic hashes support future manual/AI parity.
  [Design](SCREENER_DEFINITION_DESIGN.md) covers workflow and additive definition/
  revision/run/input records. No schema migration, AI endpoint or UI was added.
4. **Verification:** combined offline gate passed 89 tests, 16 warnings, 4.74s,
  including 18 forward-boundary and 30 new screener-definition tests. No provider,
  LLM, DB, worker, role, publication or listener action occurred. Existing browser
  libraries and the equity-only cohort remain unchanged. Exact command is in the
  forward-ingestion design; full runtime and original options-page gaps remain.

### Forward Ingestion Architecture And Offline Probe - 2026-10-07

1. **Audit:** inspected current VS Code tasks/side effects, Developer handoff and
  package-mark consumer, pre-session selection, Advanced gateway/journal/normalizer/
  materializer, stable normalization/analysis/detector ownership, schemas and roles.
  Existing canonical worker groups were not reused or launched.
2. **Architecture:** independent paced Developer REST and journaled Advanced stream
  lanes converge via provenance-bearing adapters into shared versioned engines.
  Design includes references/open/close/OI, partial coverage, gaps/recovery/replay,
  immutable publication fences, role/pool/request/storage budgets and 400/50 gates.
  No Advanced-to-Developer relabeling, cross-tier fallback or GET-side work.
3. **Source:** new `forward_ingestion.py` proves real selector -> typed source ->
  INGRESS -> normalized complete matrices -> native detector dataset/run -> immutable
  internal projections, with an in-memory owner/deadline/conflict fence. Eight
  synthetic underlyings/16 contracts are a small test policy, not enrollment.
4. **Evidence:** final guarded command in the design passed 57 tests, 16 warnings,
  4.32s. Sixteen new tests plus existing original options GET/POST contracts,
  manifest, job-fence, Advanced normalization/replay and native O3 collector checks.
  No provider/DB I/O or listener was used; no SQL/runtime capacity claim follows.
5. **Limits:** `offline_forward_projections_v1` is not an original page response.
  Actual target-local detector context is still missing and explicitly unavailable.
  Advanced full-matrix adapter, durable raw-matrix schema/persistence, scheduler,
  original projection extraction and lane-specific principal isolation are pending.
  Existing 13-underlying detector contract and full-chain versus selected-manifest
  semantics must be resolved before 50-underlying/original-page delivery.
6. **Authority:** existing equity-only cohort remains immutable; current options
  Activity/Flow/Screener/Alerts stay `CODE_COMPLETE_UNAVAILABLE`. No worker, provider,
  migration, publication, DB mutation/query, role or service action ran. Stopped
  API/Vite and locked principals are user-reported, not rechecked this phase.
  Next source slice and each durable rollout gate are separately identified in
  the design; none is implicitly authorized by passing offline tests.

### Individual Ticker Workspace Integration - 2026-10-06

1. **Full contract:** four subscribed ticker views now have target-local stored-only
  contracts for charts, quote, setups, patterns/channels, stock context, extended
  security identity and earnings/Fed calendar. Financials remains canonical-only,
  hidden/redirected in realtime until subscribed. No canonical
  fallback, provider-on-GET, computation-on-GET or incompatible response shim.
2. **Reference/fundamentals:** existing security calls retain extended overview
  fields with zero extra requests. New immutable migrations `015`-`017` support
  optional future fundamental reports, but company-financial provider calls are
  disabled by default until subscribed. Current generation 3 has 38 members because
  it records nine explicit unavailable optional fundamental members; future default
  nine-equity reference runs require 29 members and make zero financial calls.
3. **Calendar:** retained `ref.market_event_*` facts publish the original envelope;
  missing coverage is unavailable rather than clear, and exact coverage produces
  CLEAR/BLOCKED/NOT_APPLICABLE states with no GET-side fetch.
4. **History v2:** five native raw 1m sessions and 60 native adjusted months join
  the unchanged native-5m/derived-intraday and adjusted-weekly contracts. Nine
  tickers require 310 history requests and 122,409 facts (13,601/ticker). Policy
  hash `2a358b31...0860a`; plan hash `83fca24b...d551`. Generic integration maximum
  is 378 requests using the existing 68 reference ceiling. Legacy accepted equity
  validation remains independently fixed at 292 history/332 total requests.
5. **Evidence/state:** ticker backend 18/18; supporting backend 95/95; frontend
  11/11; production build 2,020 modules. Additional focused reference and history
  gates passed 26/26 and 27/27. Source/tests/docs/build only; no provider, DB,
  migration, role, process, publication or browser operation. Applying migrations,
  collecting the new references/history, republishing and browser acceptance remain
  separately authorized runtime work.
6. **Runtime smoke check (2026-10-07):** read-only API/Vite startup and AAPL browser
  traversal rendered all five routes but every ticker contract returned 503 with
  PostgreSQL `UndefinedColumn: cohort.profile_id`; the three known shell routes
  returned 404. The target predates the current baseline and needs a fresh disposable
  bootstrap, not a partial `015`/`016` patch. No chart series, setup, reference,
  calendar or financial rows loaded. Services were stopped; API retained its
  pre-existing LOGIN/NOINHERIT state, runtime/migrator remained NOLOGIN.
7. **Runtime closure (2026-10-07):** smoke blocker superseded. Applied migrations
  `014`-`017`, acquired/persisted v2 history, completed nine-equity reference
  generation 3 and published nine current ticker snapshots. AAPL browser acceptance
  passed daily 90 bars, native 1m 390 bars, populated daily/monthly technical setups,
  five-frame Timeframes, Levels and honest Fibonacci no-swing state. Financials is
  disabled by default after Polygon 403; Finnhub/FRED retain calendar/macro ownership.
  Levels follow-up restored 7 nearest of 23 stored zones by retaining confluence
  for serveable STALE evidence. Monthly-pinned Fibonacci now explicitly falls back
  to the nearest published daily swing and rendered a populated price map with
  1h/30m overlaps. Backend gates 19/19, 65/65, 62/62 and 4/4; frontend 2/2;
  build 2,020 modules.
  API/Vite stopped and all principals relocked NOLOGIN at closure.

### Cross-Surface Pattern Context - 2026-10-08

1. Stock Screener detail and Stock Alert detail now reuse a shared Pattern Watch
  link that preserves publication query identity and opens ticker-specific all-frame
  context. Pattern Watch initializes search/interval from the deep link.
2. This is descriptive navigation only: no screening predicate, alert selection,
  option qualification, score or provider behavior changed. Frontend checks passed
  4/4 and TypeScript/Vite built 2,021 modules.
3. Options enrichment remains deferred until the missing realtime eligible-chain,
  discovery-catalog and detector-alert APIs are implemented; no dead UI shim added.
4. Pattern Watch interval policy now uses `15m`, `30m`, `1h`, `1d`, `1wk`, `1mo`
  for universe scans, defaulting to `1d`. `5m` remains retained and available only
  for exact-ticker/cross-frame research. Monthly projection reuses the 60 native
  adjusted bars and performs no provider acquisition. Realtime tests passed 3/3,
  frontend checks 4/4 and the build completed 2,021 modules. Current immutable
  ticker snapshots still require a separately authorized provider-free republish
  before live monthly Pattern Watch reads are available.
5. Pattern-row ticker links preserve cohort/snapshot identity and initialize the
  clicked chart interval, automatic overlays and exact pattern selection. Browser
  acceptance clicked AAPL's retained 15m rising wedge and verified the selected
  `frame|15m|RISING_WEDGE` plus visible wedge read. Monthly/5Y chart controls are
  ready for the pending monthly republish. Frontend checks passed 5/5 and the
  production build completed 2,022 modules.
6. Monthly chart rendering was not a data gap: the retained API returned 60 bars.
  A background retry could keep `isFetching` true after valid data arrived, while
  the blocking chart scrim still covered the candles. The scrim now appears only
  with zero chart bars. Browser acceptance verified 60 monthly bars, no scrim and
  4,420 painted canvas samples; frontend checks passed 5/5 and the build passed.

### Full Portal Restoration - 2026-10-06

1. **Restored shell:** realtime mode uses the canonical `NAV_GROUPS` and original
  route components, plus the Validation Status item. Global Search, session-date
  context, staleness banner, collapsed flyouts and disabled pinned controls are
  active again. Left-navigation and search destinations preserve current
  cohort/snapshot identity. Missing pages are no longer hidden by the reduced
  realtime shell or replaced with temporary adapters.
2. **Route-present contracts:** Dashboard, All Tickers, Ticker Workspace, Stock
  Screener, Stock Alerts and all five default-daily strategies have matching
  routes in `realtime/portal_api.py`. This is static/offline compatibility only;
  no durable publication or freshness claim follows from route presence.
3. **Shell gaps:** `/trading-sessions` and
  `/equity/materialization-status` are absent. Ticker autocomplete, historical
  session stepping and global staleness reporting therefore remain pending.
4. **Pending pages/endpoints:** Market Conditions:
  `/stocks/market-conditions`; Sector Intelligence: `/sector-intelligence`,
  `/market-regime`; Persistence: `/scan/streak`; Options Activity and Flow: `/options/flow`;
  Options Screener: `/options/discovery-catalog`,
  `/options/eligible-chain/query`; Options Alerts: detector datasets, schedule,
  evaluations, detector runs, candidate detail, current quotes and preview under
  the original `/options/*` contracts. The validation-only
  `/realtime/option-alerts` response is not used as an incompatible substitute.
  Account/auth remains outside realtime scope despite its restored direct route.
5. **Evidence/state:** restored-page frontend checks passed 59 with two guarded
  fixture skips and zero failures; focused pinning regressions passed 9/9.
  TypeScript/Vite production build passed, transforming 2,020 modules without
  warnings. Source/tests/docs and build outputs only; no service, browser,
  provider, database, SQL, role, migration, publication or durable-data action.
  Runtime failure-state and desktop/mobile browser assessment remain `NOT_CHECKED`.

### Options Alerts - 2026-10-08

1. **Selected slice:** loaded the dedicated handoff and mapped the original Latest
  Run, Day History, EOD Review, candidate, quote and preview contracts. The separate
  `/realtime/option-alerts` validation shape remains incompatible and unused.
2. **Accomplished:** exact-cohort stored-only readers now cover datasets, schedule,
  evaluations, detector runs, candidate detail, current quotes and preview with
  original query/body validation. The selected cohort returns one explicit
  `EQUITY_ONLY_COHORT` verdict and typed unavailable dataset/quote data.
3. **Safety/identity:** no empty success, detector compute, candidate reconstruction,
  provider-on-request, write, canonical fallback or validation-response shim exists.
  All seven frontend requests and all tab/filter/deep-link transitions preserve the
  cohort and render the exact unavailable explanation.
4. **Evidence:** focused backend 10/10 and adjacent backend 22/22 passed under the
  offline guard. Focused frontend 33/33 and final options/shared frontend 42/42
  passed. TypeScript/Vite built 2,022 modules. Live 1440x1000 checks passed datasets,
  schedule, Retry/AAPL, archived LATEST/HISTORY, all three tabs and candidate/preview.
5. **State/exclusions:** no option members, datasets, runs, evaluations, alerts,
  candidates, quotes, previews or outcomes are claimed. No provider, worker,
  migration, publication, policy, replay, durable write or canonical access ran.
  API/Vite stopped and all principals relocked.
6. **Next gate:** options-first page restoration is closed at the selected cohort's
  data boundary. Any populated options UI requires a separately approved option-bearing
  cohort plus complete original matrices/detector publications; unavailable contracts
  must not be converted to empty successful rows.

### Options Screener - 2026-10-08

1. **Selected slice:** mapped the original static discovery catalog and POST
  eligible-chain contracts, including screen library, filters, columns, session,
  pagination, details and export behavior.
2. **Accomplished:** the unchanged pure catalog now serves 12 contract filters,
  four categories and eight models. The stored-only eligible-chain reader validates
  original request bodies and cohort/manifest scope, then returns explicit
  `EQUITY_ONLY_COHORT` or `NO_ACTIVE_POLICY_ELIGIBLE_CHAIN` unavailable envelopes.
  No narrower discovery, acquisition, package or alert evidence is substituted.
3. **Identity/UI:** GET and POST preserve cohort identity. Publication scope remains
  outside saved definitions but survives local screens, tabs, built-ins, filters and
  ticker links. Unavailable data renders as unavailable, never no-match success.
4. **Evidence:** focused backend 8/8 and adjacent backend 15/15 passed under the
  offline guard. Final frontend model/shared checks passed 34/34. TypeScript/Vite
  built 2,022 modules. Live 1440x1000 checks passed exact catalog shape, unavailable
  chain, AAPL filter and Unusual Calls library interactions with 1440px containment.
5. **State/exclusions:** selected cohort has no option underlyings or matrices; no
  eligible rows, coverage, details or exports are claimed. No provider, worker,
  migration, publication, durable write or canonical access ran. API/Vite stopped
  and all principals relocked.
6. **Next gate:** Options Alerts. Load its dedicated handoff and audit the original
  detector datasets/schedule/evaluations/runs/candidate/quotes/preview contracts;
  the validation-only realtime option-alert endpoint remains incompatible.

### Options Activity / Flow - 2026-10-08

1. **Selected slice:** inventoried the original shared `/options/flow` request.
  Activity requires cross-underlying complete-matrix summaries; Flow additionally
  requires expiries, strikes, modeled premium activity/Greeks and settled OI change.
2. **Data verdict:** discovery pages, selected manifest dispositions, acquisition
  handoff facts and candidate packages are narrower evidence and are not compatible
  substitutes for complete analyzed matrices. The active equity-validation cohort
  has zero option underlyings. No retained flow rows can be served honestly.
3. **Accomplished:** added exact-cohort stored-only compatibility routing that
  verifies cohort/manifest scope and returns an original unavailable envelope with
  `EQUITY_ONLY_COHORT` or `NO_COMPLETE_MATRIX`. Requests and both pages' navigation
  preserve query identity and display explicit explanations. No empty success,
  provider-on-GET, compute, write, canonical fallback or alert-response shim exists.
4. **Evidence:** focused backend 4/4 and adjacent backend 11/11 passed under the
  offline guard; focused frontend 2/2 and adjacent frontend 7/7 passed. The portal
  built 2,022 modules. Live 1440x1000 checks passed Activity, pointer Flow navigation
  and direct AAPL selection with exact cohort requests and 1440px containment.
5. **State/exclusions:** result is `CODE_COMPLETE_UNAVAILABLE`, not data readiness.
  No matrix summaries, profiles, top contracts or OI change are claimed. No provider,
  worker, migration, publication, durable write or canonical access ran. API/Vite
  were stopped and all principals relocked.
6. **Next gate:** audit Options Screener's original discovery-catalog and
  eligible-chain query contracts. Do not promote discovery evidence into an eligible
  matrix without its original normalization, policy, coverage and cohort ownership.

### Dashboard / All Tickers Shell Status - 2026-10-08

1. **Selected slice:** audited the remaining original shell calls around Dashboard
  and All Tickers. `/tickers` is already backed by `TICKER_WORKSPACE_V1`.
  `/trading-sessions` remains unsupported because retained bar dates are not a
  date-to-Dashboard-publication catalog; exposing them would falsely enable a
  historical control against latest-only page readers.
2. **Accomplished:** added exact-cohort/current-or-pinned
  `/equity/materialization-status` over stored `DASHBOARD_V1`, with daily-only
  clocks, normal freshness/deadline/reference/action gates and explicit inactive
  pipeline state. Frontend status/session requests preserve realtime identity;
  missing status is visible, and Dashboard/All Tickers display the stored-reader
  reason instead of generic 503 text. No empty success or GET-side work was added.
3. **Runtime correction:** live startup exposed obsolete `cohort.profile_id` usage
  in the Dashboard reader. It now binds provider tier through the current completed
  screening generation, matching deployed schema and the accepted ticker reader.
  The remaining 503 is `Dashboard has no retained publication for this cohort`,
  not schema failure.
4. **Evidence:** guarded backend bootstrap/Dashboard/contracts gate passed 72/72.
  Shared frontend shell checks passed 15/15 and the final directly affected file
  passed 4/4. TypeScript/Vite built 2,022 modules. Live desktop checks at 1440x1000
  passed cohort-pinned navigation and refresh, disabled session controls, explicit
  unavailable cards/banner and 1440px document containment. No charts were involved.
5. **State/exclusions:** runtime cohort
  `d1baee6f-4ab2-543c-8a86-ce10f8952e69` has no retained `DASHBOARD_V1`; Dashboard,
  All Tickers rows and successful freshness status remain unavailable. No compute,
  publication, provider, worker, migration, durable write or canonical access ran.
  API/Vite were stopped and all principals relocked after browser validation.
6. **Next gate:** Market Conditions is the next page-order source slice. Durable
  Dashboard publication and a publication-aware trading-session catalog remain
  separate authorization/design gates and must not be inferred from ticker history.

### Original Default-Daily Strategies - 2026-10-06

1. **Planned item:** restore the original Gap/FVG, SMA 9/21, Momentum Pullback,
  Bearish Bounce and 5% Fibonacci pages over one latest completed daily
  exact-cohort Dashboard publication. No variants, history or forward-state work.
2. **Accomplished:** future immutable Dashboard bundles retain all six default
  contracts from the same existing producer pass. Combined Dashboard, overview
  and setup-summary outputs remain unchanged; FVG is retained individually, not
  invented in combined output. Stored-only individual routes require cohort and
  snapshot identities. Gap/FVG form one atomic page read. Defaults/counts/
  groupings/order remain producer-owned; GET never computes, fetches, writes,
  repairs old bundles or uses canonical fallback. Unsupported requests return
  422; missing/incompatible/expired publications return 503, not empty matches.
3. **Evidence:** initial guarded Dashboard anchor passed 31/31; final combined
  daily/Dashboard/All Tickers/shared API/writer and domain regressions passed
  162/162. Real producers emit gaps=1, FVG=4, MA=9, momentum=1, bearish=2,
  Fibonacci=4 over synthetic exact inputs. Valid flat-cohort no-match tests
  retain 9 descriptive MA states (the domain emits one state per eligible
  series); the MA zero-result branch is separately tested with short/ineligible
  inputs, not represented as a qualified daily publication. Shapes/counts/
  grouping/order, row equality, source/reference/action/hash/deadline/expiry
  gates, old bundles, atomic FVG failure and pointer rotation are checked.
4. **Frontend evidence:** 16/16 focused service/cache/keyboard/rendered-HTML and
  canonical/shared-service checks passed. Final canonical and realtime TypeScript/
  Vite builds passed after the keyboard/ticker-key correction; touched tracked
  sources passed whitespace checks. Original routes/layouts/local controls
  are retained. Shared atomic queries carry cohort/snapshot through Dashboard,
  All Tickers and strategy navigation/cache keys; failed refresh preserves prior
  results/provenance/clocks. Unsupported controls and strategy ticker links are
  disabled. Sorts support Enter/Space, controls have labels and expansion uses
  native buttons. Guarded export-only fixtures render real rows in all five
  original pages. Browser tooling unavailable: actual DOM, pointer, desktop and
  mobile visual checks remain NOT_CHECKED; static HTML is not browser evidence.
5. **Basis/exclusions:** existing adjusted OHLCV, volume-sensitive grades/
  classifications and partial-current-week W-FRI resampling are preserved.
  Research-only disclosure states UNVERIFIED raw-volume parity and distinguishes
  scanner adjusted-series volume from Overview raw liquidity. Momentum copy now
  matches ADX15-55/2ATR/RSI30-60 without threshold changes. No probabilities,
  win rates, recommendations, raw-history extension or invented weekly history.
  Publication-matched ticker drilldown remains unavailable; the independent
  TICKER_WORKSPACE reader and ticker access elsewhere are unchanged.
6. **State changed/remaining gates:** source/tests/direct docs, synthetic exports
  and build outputs only. No services/listeners, acquisition, DB queries/writes,
  SQL/migrations, reset, roles, enrollment/activation, durable publication,
  historical import or broker actions; no commits. Raw-volume parity remains
  UNVERIFIED; durable contents, DATA_READY and live delivery are NOT_CHECKED.
  Variants/history/forward-state/full-market/sectors/Pattern Watch/Persistence/
  options/account and ticker-reader changes remain separate authorized scopes.
  Durable materialization or runtime lifecycle still requires exact approval.

7. **Fresh offline browser evidence - 2026-10-06:** OWN integrated browser page,
  intercepted local realtime build/API bytes and default-deny routing; guarded
  export-only synthetic producer counts again 1/4/9/1/2/4 (gaps/FVG/MA/Momentum/
  Bearish/Fibonacci). All five populated pages plus opening-gap/FVG views were
  checked at 1440x1000 and 390x844. Document widths 1440/1425 desktop and 375
  mobile; actual pointer-wheel table scrolling stayed in 374px containers
  (table widths 870/905/867/1003/1003/985px). Pointer tabs/filters/sorts/details,
  Enter/Space sorting/native expansion, desktop MA column preferences and
  disabled strategy-control/ticker gating passed. Sidebar/mobile drawer and
  Dashboard quick navigation preserved explicit pins across simulated rotation.
  All five strategy pointer refresh success/503-retention checks passed,
  including Gap/FVG-only partial failure, paired DOM rows/provenance and mobile
  metadata-503 retention/recovery. Initial missing FVG was unavailable, not zero;
  missing pin had no latest fallback and wrong cohort made no requests.
  All Tickers setup-only 503 retained nine rows/clocks; successful refresh passed.
  Zero application page exceptions/unexpected console errors/external requests;
  eleven console errors were expected intercepted 503s. Existing current build
  was reused; earlier test/build evidence above was not rerun.
8. **Offline acceptance BLOCKED; surgical authorization required:** (a) shared
  [mobile column menu](../frontend/src/layout/pageChrome.css) is off-screen on MA
  (x=-93.5px; Cross date checkbox x=-82.7px), so actual pointer selection fails;
  (b) [shell brand/home link](../frontend/src/layout/AppShell.tsx) at line 258
  drops cohort/snapshot pins and actual pointer navigation resolves newer metadata
  without Refresh; (c) [Dashboard error branches](../frontend/src/pages/Dashboard.tsx)
  at lines 486/493/617 hide 6/3/1 retained gainers/losers/volume rows on scan-only
  503 despite preserved old clocks/pin and a retention alert. Fix only viewport
  anchoring, publication-pinned brand navigation and retained-row rendering;
  preserve canonical behavior, then rerun focused browser acceptance.
  Source/tasks remained read-only; only this active tracker/checkpoint were
  updated and this phase's synthetic scratch exports removed. No service,
  listener, provider, DB/SQL/durable action or commit. Full offline acceptance
  remains blocked, not complete; canonical browser/live/durable gates remain
  NOT_CHECKED and raw-volume parity remains UNVERIFIED.
9. **Closure decision:** user explicitly authorized a fresh, separate
  implementation phase limited to the three surgical fixes in item 8 and
  focused browser rechecks, with all service/data exclusions unchanged.
  No source changes were made in this validation phase; acceptance remains
  BLOCKED until the fixes and their exact browser outcomes are verified.
10. **Authorized surgical closure - 2026-10-06:** all three blockers in item 8
  are fixed and verified. Shared column menu measurement/width clamping now
  keeps mobile MA at x=8..208px (checkbox x=18.8px); actual pointer/keyboard
  selection, show-all/reset, persistence, open-menu resizing and mobile Gap
  column selection passed. Desktop right alignment was preserved.
  Brand navigation retains explicit cohort/snapshot on desktop/mobile across
  simulated latest rotation; wrong-cohort navigation makes zero requests and
  preserves the no-substitution error. Dashboard desktop scan/overview/metadata
  503s and mobile scan-only 503 retain exact 6/3/1 mover/volume rows, clocks and
  URL. Pointer recovery commits latest complete outputs/provenance; nine DOM
  states stayed paired and all nine mobile mover prices matched the simulated
  new publication. Initial 503 without prior data still shows unavailable, not
  no-match success. Four new regressions passed after reproducing three failures
  before edits; fresh focused frontend checks passed 16/16 with no skips and
  guarded real rows. Canonical/realtime TypeScript/Vite builds and scoped source
  whitespace checks passed. Backend 162/162 remains carried, not rerun.
  **Default-daily offline browser acceptance PASSED:** prior broad sweep plus
  fresh focused defect closure, not a claim that the entire sweep was rerun.
  Only the four directly affected source files, new focused regression test and
  active tracker/checkpoint changed. Own page/state, twelve exports and two
  isolated build directories were cleaned; existing build/concurrent changes
  were preserved. No dependency install, task change, services/listeners,
  provider, DB/SQL/durable action or commit. Live/durable/full canonical-browser
  gates remain NOT_CHECKED; raw-volume parity remains UNVERIFIED. All later
  excluded workstreams still require separate chosen scopes.
11. **Selected pending item CLOSED offline - 2026-10-06:** user chose bounded
  publication-matched daily ticker research, leaving the independent full ticker
  workspace unchanged. [Research route](../frontend/src/pages/DailyStrategyTicker.tsx)
  `/strategy-research/:symbol` joins nine existing stored reads under one explicit
  cohort/snapshot; all six default scanner results, overview raw liquidity and
  published daily setup are filtered locally to the selected cohort member.
  Shared ticker buttons wire every original strategy page and both Gap/FVG
  entry points. Capability-gated old/incomplete bundles stay disabled without
  rewriting retained payloads or inventing missing FVG matches. Ticker/cohort/
  snapshot cache identity, guarded deep links and atomic latest refresh preserve
  publication ownership; canonical/full ticker access is unchanged.
  Fresh guarded backend checks passed 155/155 and focused frontend checks 23/23
  with no skips; canonical/realtime TypeScript/Vite builds passed. Actual own-page
  pointer/keyboard checks passed at 1440x1000/390x844, including six entry surfaces,
  ticker-cache isolation, native setup expansion, readable local table scrolling,
  FVG-only 503 retention and complete refresh recovery with five paired DOM states.
  Missing identities/wrong cohort made zero requests; old disabled links made
  zero pointer requests and direct missing research showed an error, not zero.
  No independent ticker/profile/options/calendar reads, services/listeners,
  provider, DB/SQL/durable actions, dependency/task changes or commits. Own page,
  twelve scratch exports and two isolated builds were cleaned; existing outputs
  and concurrent changes were preserved. Remaining browser gaps, variants/history,
  market/sectors and alert forward-state work require separate selected scopes.
  Durable/DATA_READY/live gates remain NOT_CHECKED; raw-volume parity UNVERIFIED.

### Original All Tickers Vertical Slice - 2026-10-06

1. **Planned item:** restore the original All Tickers page only after tracing its
  actual published setup-summary, overview filters/columns, session semantics
  and navigation. Scanner results are not substitutes for daily setups.
2. **Accomplished:** the existing immutable `DASHBOARD_V1` bundle now retains
  daily setup evidence and all 29 original summary fields. The checked-in equity
  materializer uses the existing 283-session adjusted daily history and exact
  recent 20-session raw hourly confirmation. Canonical summary flattening moved
  unchanged to the shared pure ticker contracts; no detector or policy changed.
  The new stored-only `/tickers/setup-summary` route pins the same snapshot as
  overview. Original `/overview`, Dashboard and shell links are restored.
  One atomic frontend query prevents mixed or partially refreshed publications.
3. **Evidence:** final guarded All Tickers/Dashboard/shared API/writer/scanner and
  standalone-import/CLI regressions passed 66/66; focused frontend checks passed
  14/14. Canonical and realtime TypeScript/Vite builds passed. Real, unmocked setup
  producers emit nine synthetic rows. Tests cover exact original shapes/evidence,
  membership, clock/hash/run drift, unsupported intervals/cohorts, pointer
  rotation, freshness/expiry and absent hourly confirmation.
4. **Browser evidence:** guarded fixture exports and static-asset interception
  opened no listener or provider/DB connection. Complete nine-row desktop/mobile
  views fit 1440x1000/390x844 (document widths 1440/375); mobile table scroll stays
  local. A direct deep-link overflow exposed a missing shared table stylesheet,
  now explicitly imported by the original page. Pointer sort/column picker passed
  while visible. Ticker/published-momentum/relative-volume filters passed; increasing
  the gains threshold from 2% to 3% changed five rows to zero. DOM refresh/503
  retention preserved the entire prior table; pointer refresh remains NOT_CHECKED
  because the shared page later became hidden.
5. **Coverage/exclusions:** daily setups are descriptive, not rankings or execution.
  Missing exact hourly slots retain daily values with null agreement; no fabricated
  divergence or weekly history. Overview prices use split-adjusted history with
  current raw quotes/liquidity; this is not extended canonical raw-history parity.
  Sectors and 200-week controls remain gated. Older Dashboard bundles are preserved:
  absent summaries return 503 rather than triggering compute/repair on GET.
6. **State changed/next:** source/tests/docs and synthetic fixture/build outputs
  only. No provider acquisition, services, DB/evidence mutation, reset, migration,
  roles, enrollment, activation, historical import or broker work. Durable
  contents, `DATA_READY` and live parity are NOT_CHECKED. Next bounded source gate:
  original individual scanner pages and their actual stored parameter/interval
  contracts. Any durable publication/restart still needs exact authorization.

### Original Dashboard Vertical Slice - 2026-10-06

1. **Planned item:** trace the original Dashboard before widening into market
  pages, and restore its actual latest-session, overview, combined-scan and
  momentum-context dependencies over immutable realtime-owned publications.
2. **Accomplished:** `dashboard_projection.py` reuses the checked-in
  `compute_default_scanner_snapshots`, index technicals and screening/ranking
  producers. One immutable `DASHBOARD_V1` bundle uses the existing snapshot
  writer; `dashboard_repository.py` serves the four compatible routes without
  canonical imports, provider access or request-time materialization. Latest
  session returns publication metadata; overview/scans/context pin that ID,
  including across current-pointer rotation. Original Dashboard and navigation
  are restored. The existing CLI adds `--surface dashboard`; planning is zero-I/O.
3. **Evidence:** final guarded Dashboard/shared API/writer/scanner/standalone
  import/CLI checks passed 49/49. Frontend checks passed 38/38; TypeScript/Vite
  builds passed. The exact nine-ticker synthetic fixture runs real, unmocked
  owning producers: five gainers, four losers, one unusual-volume name, nine
  MA results, five Fibonacci results, six agreement rows and nine ranked members.
  Unsupported dates/parameters/cohorts, hashes, references/actions, source
  availability/causality, scale, deadlines and expiry fail closed. These are
  deliberately synthetic contracts, not naturally detected/live-market signals.
4. **Browser evidence:** static assets and exported guarded API responses were
  intercepted in an offline browser; no listener or provider/DB connection was
  started. Completed original views fit 1440x1000 and 390x844 (document widths
  1425/375, including vertical scrollbars). DOM scan/refresh, mover filters and
  sorting passed; a simulated 503 displays an alert and preserves all prior
  tables. Sector and unsupported navigation controls are explicitly disabled.
  Pointer checks remain NOT_CHECKED: the shared page was hidden and stability/
  pointer attempts did not establish interaction. DOM evidence is not pointer
  evidence. Dashboard grid tracks now admit narrow viewports without redesign.
5. **Coverage/exclusions:** history is provider split-adjusted, pinned to the
  same-session raw current scale; OHLC/liquidity use actual raw evidence and
  relative volume includes the current session in its 20-session denominator.
  This is an explicit basis contract, not parity with extended canonical raw
  history. Ranking and breadth cover only the exact selected cohort, not the
  full market or sectors. Dated sectors and 200-week history are unavailable.
  Dashboard does not request Market Conditions or Sector Intelligence; their
  rotation/events/macro/fundamentals/full-page producers are not claimed.
  Individual scanner target pages remain gated rather than silently redirecting.
  All Tickers is covered by the current package above. No calendar clearance or
  execution permission is created.
6. **State changed/next:** source/tests/docs and synthetic browser artifacts
  only; no services were started/stopped, no retained DB/evidence was changed,
  and no acquisition, reset, migration, role unlock, enrollment, publication
  activation or broker action occurred. Durable Dashboard contents, `DATA_READY`
  and live parity remain NOT_CHECKED. The All Tickers dependency package is now
  complete offline; individual scanner pages follow separately. Durable materialization/restart
  still requires exact authorization.

### Stock Alerts Supported Vertical Slice - 2026-10-06

1. **Planned item:** restore the original alert page, Latest Run/Day History/Open
  Positions/EOD Review, filters, pagination, plans, trigger evidence and paper
  lifecycle over strictly realtime-owned outputs.
2. **Accomplished:** provider-free `stock_alert_projection.py` executes the original
  feature/detector functions with actual clocks. Explicit cohort-bound retained
  intraday/swing streams use the original selection/lifecycle/result/review
  contracts and immutable `STOCK_ALERTS_V1` snapshots. Stored-only compatible
  readers serve both original `/api/stocks/alert-*` routes; the original page is
  restored and its temporary adapter removed. The default CLI `--surface alerts`
  does not enroll or invent publications and refuses to erase retained streams.
3. **Evidence:** guarded alert contracts passed 31/31, final overwrite guards
  passed 2/2, and shared API/writer/pure EOD checks passed 19/19. An initially included canonical-router test was
  blocked by the mandatory canonical-import guard and then explicitly deselected;
  the guard was not relaxed. Frontend checks passed 35/35; the final focused
  navigation/mobile containment check passed 28/28. TypeScript/Vite build passed.
  The exact nine-ticker fixture executes unmocked owning detectors; separately
  seeded synthetic candidates execute real forward selection and paper lifecycle.
  Selected fixtures are contract proof, not a claim of naturally detected signals.
  API outputs reconcile exactly to the stored/shared domain engine, including
  empty latest intraday runs, nine closed intraday plans, nine open swing plans,
  global pagination and 18 EOD candidates. Negative identity, source clocks,
  causal availability, enrollment/policy, action, checksum and deadline gates pass.
4. **Browser evidence:** all four views measured no document overflow at
  1440x1000 and 390x844. Real pointer tab, plan drawer, EOD drawer, close, sort,
  refresh and previous-page interactions succeeded; model/trade-type selection
  and Escape were checked. Original compatible requests carried the fixed cohort.
  One existing implicit grid track caused 398px document width at 390px; a single
  scoped zero-minimum track fixes it without replacing the layout. A desktop
  loading-state race was rechecked against the actual rendered count.
5. **Coverage/exclusions:** bootstrap evidence remains explicitly UNAVAILABLE,
  not a no-match publication. A durable realtime forward checkpoint loader,
  retained replay cohort and publication rotation/event context producer remain
  missing. REPLAY is 503, never a SHADOW fallback. No calendar is certified clear.
  Stale retained history stays visible; EOD outcome clocks remain at projection
  time, with read time separate. No canonical policy/detector source was changed.
  Durable `DATA_READY`, standalone-reader restart and live Stock Alerts parity
  remain NOT_CHECKED. Options/account/broker remain excluded.
6. **State changed/next:** source/tests/docs only. Owned loopback synthetic
  services were used for acceptance and stopped afterward; no durable service,
  provider acquisition, DB mutation/reset, migration, role unlock, historical
  import, cohort enrollment, activation or broker action. Next source/offline
  package is the original Dashboard and its actual market-input dependencies;
  missing alert runtime producers remain separate explicit gates.

### Ticker Workspace Vertical Slice - 2026-10-06

1. **Planned item:** restore the original workspace, all five view routes, chart
  intervals/periods, technical/detail panels and original service contracts.
2. **Accomplished:** real DB-free equity materialization, shared canonical
  confluence/pattern ordering, exact stored-interval/source reconciliation and
  immutable `TICKER_WORKSPACE_V1` projections. Compatible chart, quote, setup,
  pattern/channel/cross-frame, profile and stock-context readers import no canonical
  database/main modules and perform no provider or request-time materialization.
  The original page/CSS are unchanged. The CLI's `--surface ticker` is provider-free
  and does zero I/O without explicit `--execute` plus exact target confirmation.
3. **Evidence:** exact nine synthetic tickers execute the owning domain functions,
  real snapshot writer, compatibility reader and HTTP contracts. The guarded
  backend gate passed 52/52, including shared screening/API/writer regressions and
  negative basis, lineage, identity, action, coverage, clock, checksum, cohort and
  deadline checks. The final ticker-only gate passed 17/17, adding native 5m
  session-window failure even when long derived intervals are absent. Frontend
  services/routing/models passed 31/31; TypeScript/Vite build passed. All five
  views rendered at 1440x1000 and 390x844 without document overflow. URL interval
  checks covered 5m/15m/30m/1h/1d/1wk and absent 1m. DOM checks verified refresh,
  query-preserving tab navigation and ladder draw/remove state; native select
  interaction changed the chart interval and analysis follower together. Browser
  API resources used only original compatible paths with explicit cohort identity.
4. **State changed:** source/tests/docs only. One unbounded initial test was stopped;
  the corrected producer uses the original 500-bar intraday analysis bound and
  300-finalized-bar pattern window, retaining full charts and lineage. A local CLI
  indentation error was corrected and its non-executing path verified. The temporary
  ticker adapter was removed only after API/DOM parity. Guarded synthetic browser
  services were used only for offline acceptance and were stopped afterward;
  durable runtime services were not touched.
  No DB mutation/reset, migration, role unlock, provider acquisition, historical
  import, canonical service, activation or broker action.
5. **Coverage/exclusions:** 283 adjusted daily and 90 raw sessions from native 5m,
  with retained raw derived 15m/30m/1h/1d and adjusted 1wk/1mo. Nominal quote scale
  reconciles to raw current daily facts. Four setup frames are available; monthly
  lacks the existing 50-bar minimum. Native 1m, extended reference presentation,
  fundamentals and qualified calendar projection producers are missing explicitly.
  No empty calendar is certified clear. Stored reference/action drift requires
  republishing. Real pointer interaction is NOT_CHECKED: integrated browser pages
  remained hidden and pointer stability timed out; the separate browser lacked
  Chrome. DOM activation is handler evidence, not pointer certification.
  Options/account/broker and later portal pages are excluded.
6. **Next gate:** full Stock Alerts source/offline integration in a new workstream.
  Durable ticker `DATA_READY` and live parity remain NOT_CHECKED; existing `014`,
  fresh qualified references/history, the bounded provider-free producer and a
  standalone-reader restart need separate exact authorization.

### Stock Screener Vertical Slice - 2026-10-06

1. **Planned item:** restore the original screener's appearance and behavior over
  strictly realtime-owned producers, storage and the four existing API contracts.
2. **Accomplished:** DB-free daily/gap calculation imports; parameterized split-adjusted
  projection; exact cohort/reference/as-of loader with causal derived-bar validation;
  full generation metadata, values, missing reasons, patterns, gaps, hourly facts,
  coverage and lineage in the existing realtime tables. Added a provider-free,
  explicitly authorized projection CLI. Compatibility handlers reuse the shared
  filter/sort/pagination/new-only engine. Restored the original page unchanged and
  removed only the simplified screener adapter. Full current generations supersede
  wall-clock-dated validation stubs and cannot regress to them.
3. **Evidence:** a guarded 81-test integration/regression gate passed, followed by
  the final narrowed 26-test projection/persistence gate; 46 DB-free canonical
  screening behavior checks and 59 frontend model/library/routing/service checks
  passed. TypeScript/Vite build passed (2,016 modules, 3.36s). The exact nine-ticker
  fixture executes real domain projection and realtime persistence/reader code.
  The original page rendered at 1440x1000 and 390x844 without document overflow,
  including details and the mobile filter drawer. Saved filter reload, sorting,
  new markers and CSV generation were checked. Browser requests used only the four
  compatible screening paths with explicit cohort identity.
4. **State changed:** source/tests/docs and the missing declared HTTP test dependency
  only. Temporary guarded synthetic API/Vite processes were stopped and the created
  browser test screen removed. No database mutation/reset, migration, role unlock,
  provider call, historical import, canonical service, activation or broker action.
  An initial legacy test collection attempted a DB connection and was denied;
  subsequent checks explicitly blocked canonical imports and database/provider I/O.
5. **Coverage/exclusions:** 253 of 283 split-adjusted daily sessions; nominal current
  raw price/volume and 20-session raw discovery-liquidity audit; exact raw derived 1h
  context and source chains. Missing basis/history/reference/split evidence fails
  closed. Gap/hourly details reconcile to the selected generation. The reference
  presentation columns in migration `014` were directly required by this slice;
  no generic REFERENCES/CONTROL_PLANE adapter was implemented. Physical CSV download
  completion was not certified by the integrated browser; generated CSV was verified.
  One separate pre-existing empty-source failure remains in
  `equity/derivation.py:_derive_from_intraday` (`bars[0]`); it is not used by the
  new screening loader and was not changed.
6. **Current follow-up:** Ticker Workspace source/offline integration is complete
  as recorded above; full Stock Alerts is next. Durable screener
  verification separately requires authorization to apply `014`, supply fresh
  qualified realtime references, execute the bounded producer and restart the
  standalone reader. This session did none of those actions; full screener
  `DATA_READY`/live portal parity remains unclaimed.

Stocks-core order remains Stock Screener, Ticker Workspace, Stock Alerts, then
Dashboard/market pages and options only after a valid option cohort. Account/broker
functionality stays excluded. Generic bootstrap backlog below is not the next work item.

- Accomplished durable state: migrations/security, reference generation 2, eight/eight
  identities, 8 discovery batches and one BLOCKED strategy selection; roles re-locked and zero
  portal pointers.
- Retained diagnostic evidence: next-Friday manifest `5f875b8a...7dfe`; it is smoke-only,
  not a generation, strategy manifest or parity result.
- Accomplished implementation: stable engine/orchestration and realtime share one injected
  adapter; applied `011` plus a REFERENCES-only repository retain complete raw pages and
  explicit included/excluded coverage for either tier under an exact profile/universe.
  The one-contract source remains diagnostic only.
- Accomplished selection: stable-admitted candidates produce exact manifests, complete
  included/excluded dispositions and measured Developer/Advanced budgets offline.
- Accomplished persistence: READY plans use existing subscription tables; applied `012`
  retains READY/BLOCKED selection summaries, dispositions and DTE/side coverage. READY Developer
  plans also create a deterministic inert SHADOW cohort in the same CONTROL transaction.
- Accomplished acquisition identity: the current-session READY-audited cohort loads only its exact
  manifest members and INCLUDED catalog versions; the old smoke cohort is ineligible.
- Accomplished ingestion composition: the existing CLI persists exact INGRESS batches then stores
  raw bars/option marks only for cohort/batch-fenced jobs. It does not produce technical features,
  detector evaluations or portal values.
- Current portal checkpoint: **Original default-daily strategy source slice above**.
  Remaining gates are browser interaction/visual proof and separately authorized
  durable publication; variants and ticker-reader pinning are not this slice.
  The retained option/bootstrap backlog does not gate or replace the stocks-core order.
- Authorization: source/offline work only. Provider calls, target mutation, role unlock and process
  execution require new explicit authorization.

| First increment | State | Required proof |
| --- | --- | --- |
| Exact selected inputs | 202×30m/40×1h/50×1d plus selected options `CODE_COMPLETE_OFFLINE` | Exact manifest rows produce eight compute inputs and explicit dependency status |
| Stable compute | Materializer/S1/S2/O1/O3 `CODE_COMPLETE_OFFLINE`; O2 unavailable by policy | Real stable functions build exact-cohort qualified/no-match outputs |
| Stored portal values | `CODE_COMPLETE_OFFLINE`; target rows absent | Existing repositories persist eight ticker pages, screening, stock context and option evaluations |
| Portal inspection | `CODE_COMPLETE_OFFLINE`; startup/data absent | Explicit inert-cohort API/UI renders all four surfaces without activation/fallback |

Only these four rows advance the first increment. Advanced streaming, replay, supervisor, publisher/outbox,
readiness activation, portal pointers, performance, service installation, expansion and frontend
redesign are frozen non-blockers until portal values are visible.

Daily story after the first increment: refresh finite references, run pre-session discovery/selection, then run the same Developer cycle
command once after the reviewed completed-data cutoff. A scheduler may invoke `--once`; no new daemon,
supervisor or worker family is part of the eight-ticker Developer path.

Evolution contract: fixtures are injected into the production `DeveloperCycleRunner`; live Developer
replaces only the source; daily scheduling wraps the same `--once` cycle; activation follows accepted
stored values; Advanced and expansion qualify new adapters/capacity behind the same domain and portal
contracts. No increment replaces the cycle runner or creates a second options engine.

Readiness terms and the mandatory six-part per-step update are defined in the
[Advanced realtime checkpoint](agent-context/advanced-realtime.md). Every tracker update
must identify the planned stage, accomplished layer, executable/durable evidence, state
mutation, coverage/exclusions and one next authorization-gated exit check.

## Confirmed Boundaries

- Preserve checkpoint `e110ed1`, all uncommitted work, the canonical database, current
  workers/routes, and retained history unchanged.
- The target is the separate PostgreSQL database `stocks_realtime_v1`; it now exists,
  is migrated and secured, and is inert with every generated role locked `NOLOGIN`.
- Initial scope remains the reviewed eight-symbol equity/options canary. Developer is
  a reduced delayed/completed-data cohort; Advanced is a distinct reviewed cohort.
- Initial scope cannot leak into shared type identity. Shared repositories cannot embed
  `CANARY_UNIVERSE`, Developer tier, 60 DTE or 15% corridor as universal constraints.
- `010` is intentionally Developer-only REST handoff. Advanced uses exact contract manifests,
  fenced stream sessions, raw journal, normalized events and incremental materialization;
  snapshots and streaming ticks cannot be relabelled into each other's contracts.
- Advanced remains unclaimed until one executable manifest-to-gateway-to-journal-to-
  materializer-to-detector test persists causal evidence. Component tests are insufficient.
- Developer O2 is unavailable by policy; Advanced O2 requires coherent live smile coverage.
  Contract budget, quote-history depth and latency/capacity remain measured gates.
- Stable stock/options functions are reused through versioned adapters. Domain
  repositories remain the DAL; no generic DAL, canonical fallback or cross-database
  join is permitted.
- The eventual cutover is one platform-wide effective-time switch with fresh
  prospective state. Existing history is retained but never copied or reinterpreted.
- Immutable evidence retains cohort, tier, instrument, source identity, market and
  observed clocks, payload hashes and exact causal lineage.

## Retained Backlog (Not First-Increment Gates)

This inventory is retained for later delivery and does not gate the four-step first increment.
`AUTHORIZED` is never inferred from code completion.

| ID | Package | State | Active dependency or gate |
| --- | --- | --- | --- |
| P0 | Contracts and canary identity | `VERIFIED` | None |
| P1 | SQLite raw journal benchmark | `VERIFIED` | Production path/cap selection remains open |
| P2 | Database lineage and roles | `VERIFIED` | Target provisioned; all principals locked after rollback |
| P3 | Passive preflight/readiness | `VERIFIED` | Activation and live evidence unauthorized |
| P4 | Reference bootstrap/services | `VERIFIED` canary seed | Generation 2 `READY`; refresh lifecycle inactive |
| P5 | Provider capability adapters | `CODE_COMPLETE` offline | P4 seed and active entitlement evidence |
| P6 | Materialization/recovery | `CODE_COMPLETE` offline | Disposable migration and measured runtime gates |
| P7 | Earnings/OI/detector adapters | `CODE_COMPLETE` offline | P4/P6 live qualification and retained fixtures |
| P8 | Portal projection/outbox/cutover | `CODE_COMPLETE` offline | Disposable route/publisher rehearsal |
| P9 | Manual local mock | `IN_PROGRESS` | Action role/service/API/UI and payload candidates |
| P10 | Advanced entitlement canary | `BLOCKED` | Separate provider authorization after P0-P8 gates |
| P11 | Expansion to 400/50 | `BLOCKED` | P10 plus capacity/storage qualification |
| P12 | Schwab qualification | `BLOCKED` | Authenticated schemas, account/risk and connectivity approval |

The Developer acquisition handoff and COMPUTE consumer are `CODE_COMPLETE` offline:
`INGRESS` records bounded completed batches/facts; `COMPUTE` discovers them in order,
uses the existing durable job ledger for progress/fencing/retry, and commits selected
domain outputs atomically with job completion. Acquisition has no projection access.

## Schema And Artifacts

- Additive migrations `000-013` are applied to `stocks_realtime_v1`: schema
  `5e94c32d...5973e`, exact 77 relations, zero pending and 33 security checks. All 11 roles
  are `NOLOGIN`, runtime `NOINHERIT`. Discovery/selection tables contain 8/56/64 and 1/0/64 rows;
  the sole current selection is BLOCKED and has no manifest/cohort.
- `009` local-mock request/input/disposition/fill tables are intentionally owner-only
  until a dedicated action capability is reviewed.
- `010` adds immutable completed Developer acquisition batches and versioned
  structured facts with explicit provider batch/revision, clock and hash lineage.
- Applied `011` stores immutable option discovery batches/pages/coverage for either tier,
  bound to an exact capability profile, universe and policy. It does not activate a tier.
- `004-008` define materialization, tiered option marks, feature/equity evidence,
  detector evidence and portal delivery. Workers, publishers and route selection are
  inactive.
- Migration checksums and target identity fail closed. Applied migration files are
  immutable; later changes require a new contiguous numbered migration.
- Current checkpoints are bounded to 1 MiB per shard. Depth 8 measured 776,918 bytes;
  depths 16 and 32 exceeded the cap.
- Runtime worker pools remain planned at `1/1`, API readers at `2/20`, with a
  pre-construction 38-connection ceiling. The runtime principal and generated roles
  exist but rollback set every role to `NOLOGIN`; no runtime pool was constructed.
- Passive preflight derives its exact expected version tuple from the migration catalog,
  now `000-012`; the target passed the latest-schema and security qualification. Later
  additive migrations cannot silently leave it stale.
- Every process pool wired through the realtime provider must prove both
  `current_database() = stocks_realtime_v1` and the `advanced_realtime_v1` identity
  row before the provider is returned. A canonical or wrong-lineage pool fails closed.

## Active Gates

Performance targets remain unverified engineering goals: raw handoff p99 below 1 ms;
durable journal p95/p99 at most 25/100 ms; usable feature state at most 75/200 ms;
committed evidence/outbox at most 500/1,000 ms; portal delivery at most 250/500 ms.
Do not add stage percentiles to claim end-to-end latency.

Before P10/P11, measure the supervised socket-to-durable-to-feature path, one-read
shard fan-out, PostgreSQL dirty-current/checkpoint cadence, pool contention and replay
at 2x sustained plus 5x p99 one-second bursts. Keep at least 30% usable disk headroom.
Production journal/archive path, 50/100/250 GB cap, shard/process count, status root,
service ACLs, provider limits/licensing, exact option manifest and retention funding
remain open.

No operation is authorized for providers, databases, roles, credentials, pools,
migrations, services, workers, publishers, routes, canonical data, retained replay,
OAuth, broker preview or orders. Full-handoff exhaustion must fail closed without
eviction; missing clocks, sides, coverage or lineage remain unavailable rather than
fabricated.

## Implementation Inventory (Not The First-Increment Plan)

The immutable Developer `INGRESS` -> `COMPUTE` handoff is complete offline:

1. `010` stores only atomic completed batches with 1-1,000 typed facts.
2. `developer_equity_bar_v1` and `developer_option_snapshot_v1` preserve cohort,
  `MASSIVE_DEVELOPER` tier, instrument, provider batch/revision, exact source clocks,
  raw/normalized hashes and the complete existing domain contract.
3. `COMPUTE` performs bounded ordered discovery and enqueues deterministic acquisition
  jobs through the existing materialization-job ledger; no `011` schema was needed.
4. A claimed equity job writes through `persist_realtime_equity_bars_cursor`; a claimed
  option job uses the existing Developer package adapter and option-mark repository.
5. Output and job completion share one fenced transaction. Output failure rolls back
  both, leaving retry/terminal handling to the existing job contract.
6. `INGRESS` can append/read handoff rows only; projection reads and writes are denied.
7. A one-pass COMPUTE runner performs bounded discovery/claiming, selects only complete
  unmaterialized Developer option packages, applies retry/block transitions and returns
  one immutable `IDLE`/`COMPLETE`/`DEGRADED` status contract.
8. Current Developer configuration is read-only and resolves exactly the eight approved
  symbols without provider requests. Runtime pool wiring cannot accept the canonical
  database, and passive preflight recognizes source lineage `000-012`; the target matches
  that exact migration catalog.
9. An inert provider-neutral INGRESS producer accepts only completed typed stock/option
  source batches, requires exact eight-symbol equity and option-underlying coverage,
  enforces request/byte/batch/fact bounds and commits all handoff batches together.
10. A standalone FastAPI composition exposes `/api/realtime/*` health, exact canary
   coverage, ticker, screening and alert reads using only verified READER-role realtime
   repositories. It imports no canonical API/database modules and has no fallback.
11. A one-pass PUBLICATION orchestrator atomically promotes all eight ticker snapshots,
  claims outbox rows only under active lease/cohort/readiness fences, delivers through
  an injected sink, and records delivered/retry/abandoned outcomes in separate fenced
  transactions. Sink calls never occur inside a database transaction.
12. Bounded Polygon Developer equity/option source adapters accept only injected HTTP,
  the approved HTTPS host, exact preapproved security/instrument/catalog identities and
  one-page canary responses. Equity bars retain response lineage; options reuse the
  unchanged Developer normalizer and reject missing admitted contracts.
13. A supervisor-facing status contract aggregates separately produced INGRESS,
  COMPUTE, PUBLICATION and PORTAL evidence, blocks stale/failed stages, records the
  dedicated target/no-fallback identity and writes hash-checked redacted JSON atomically
  to an injected path. Stop requests are hash-checked and instance-fenced.
14. The existing frontend has one direct `/realtime-canary` validation route using its
  existing `VITE_API_BASE_URL` and the aggregate realtime endpoint. It adds no feature
  flag, navigation item, canonical endpoint call or duplicate screening/alert workflow.

The actual `stocks_realtime_v1` database is provisioned with `000-012`, reviewed security
and local secret references. Current discovery/selection evidence is summarized in the active stage
and latest update below. A bounded earlier fresh-reference attempt made no durable
writes: the first call stopped at the one-page dividends fence; one authorized retry
allowed 16 pages and 68 total requests but lost completion output. No matching process
remained, and durable status still reported no seed or equity/option identities. A later
authorized retry failed decisively during source collection because the market-wide
dividends payload exceeded the adapter bound; its atomic report recorded `FAILED` with
no values exposed. Rollback set every generated role to `NOLOGIN`. The source now requests
splits/dividends separately for each approved ticker and rejects identity drift; the
corrected plan is 44 calls within the 68-call ceiling and passes 15 focused offline tests.
One authorized corrected retry wrote atomic `RUNNING`, but the shared terminal returned
before completion; later process inventory found no matching process and durable target
status remained empty. That run is execution-wrapper `UNKNOWN`; all roles were re-locked
and its stale report was retained. A separately authorized retry through the dedicated
process task then failed decisively at the Massive request limit while fetching option
open interest. The broad catalog/OI bootstrap cannot fit the 68-total-request envelope;
collection stopped before persistence and all roles were re-locked. Another provider
attempt, larger bound or principal unlock is unauthorized. Offline, catalog selection is
now one deterministic active call from the first page per underlying, followed by one
exact-contract OI snapshot. The resulting 44-request/42-member fake-HTTP plan passes 27
focused adapter/transport/service/seed/script/security tests. Rotated Polygon and Finnhub keys are present and
non-placeholder under the redacted environment check; no values were exposed and the
check made zero database connections and provider requests. Polygon acquisition remains
fake-HTTP qualified only.
The authorized exact-contract process task then completed with 44 provider requests,
963,474 response bytes and a 42-member generation-1 `READY` seed. Durable status matched
the exact eight equities and eight option underlyings. Every generated role was re-locked
after validation; no market acquisition, COMPUTE or service process followed.
The latest authorized sequence added generation 2 with 44 requests, 963,534 bytes, 42 members and
seed `edc77c5b...dd2a6`. Pre-session then persisted 8 batches, 56 pages, 13,528 discovery rows,
10,304,300 discovery bytes and selection `a9dd4ebd...d317` / `037edbf8...2174f`, but all
64 required lanes were `DTE_SIDE_DEPTH_UNAVAILABLE`; no strategy manifest or cohort was created.
The live seed exposed the next control-plane gap: the reference repository does not create
the base runtime identities required by `010`. A deterministic SHADOW plan and staged
repositories now exist offline: REFERENCES creates exact equity/option instruments;
CONTROL creates a 40-member subscription manifest and SHADOW cohort only for a Developer
passive profile that is `UNAVAILABLE/BLOCKED`. No readiness, tier activation or portal
pointer is written. The code is unapplied; provider-tier activation still assumes this
base cohort exists and must not be used to bypass it.
The one-pass CLI and dedicated process task are code-complete. Inert plan output resolved
configuration `0a9f355c...5c60e0`, valuation `2570137d...55e289` and explicit
not-materialized model `04a7dddd...d3ae8c`. The CLI accepts only a non-PASS/BLOCKED
Developer passive profile, stages CONTROL then REFERENCES then CONTROL, emits atomic
redacted evidence and contains no tier/portal activation call. Focused checks pass 33/33;
the authorized task then completed after a read-only effective-window correction. Durable
status verified profile `FAIL/BLOCKED`, 8 EQUITY plus 8 OPTION instruments, 40 members,
one SHADOW cohort and zero portal pointers. It made zero provider requests; all principals
were re-locked.

The one-pass Developer acquisition CLI/task is code-complete offline. It loads only the
blocked SHADOW cohort and exact stored identities and uses existing source/normalizer/
producer contracts. The producer rejects more than 24 requests before persistence;
transport execution is single-concurrency, 12 seconds apart (maximum 5/minute), with zero
retries. The inert plan is 8 equity + 8 underlying bars + 8 option snapshots and no
canonical fallback. Focused source/producer/handoff/script checks initially passed 27/27.
The first authorized task hit two read-only loader defects before provider access; both
were corrected without writes. A later run consumed one equity request and failed closed
because its four-day minute range required pagination; no handoff rows were written and
all roles were re-locked. The runner now derives exactly one completed exchange minute via
`OptionExchangeCalendar` and millisecond range paths while retaining pagination rejection.
Focused calendar/source/producer/handoff/security checks pass 41/41 offline; another live
24-call pass is unauthorized.
The authorized exact-minute retry reached option normalization but failed before the
producer transaction because its single close-minute underlying bar was later than the
option mark timestamp, so no prior aligned spot existed. No handoff row was written and
all roles were re-locked. The option source now requests the admitted snapshot first and
then the exact completed underlying minute derived from that mark timestamp. The 24-call
budget and pagination denial are unchanged; 41 focused checks pass offline. Another live
run is unauthorized.
The authorized snapshot-first retry then produced decisive normalizer reasons before the
producer transaction: the generation-1 admitted call was outside the 15% strike corridor
and failed the existing volume/OI liquidity floor. No handoff row was written and all roles
were re-locked. Policy was not weakened; generation 1 is disqualified for acquisition.
The corrected next-Friday provider-only probe completed with 16 requests, 423,110 bytes,
eight qualified selections and zero database writes. Candidate manifest `5f875b8a...7dfe`
contains one American/100x call per underlying, each within the unchanged 15% corridor
and above observed volume/OI floors. It is retained locally without credentials as
diagnostic smoke evidence only; it is not generation-2 or strategy-complete authority.

Stable engine/orchestration and the inert realtime discovery facade now use
`options.data.polygon_chain_discovery` for one bounded 60-DTE/spot-corridor request plan,
validated pagination, request deduplication, complete `RawOptionBatch` evidence, response/
cursor hashes and page/row/byte ceilings. The result exposes included rows plus explicit
out-of-DTE/strike ranges for later reviewed selection. No persistence module is imported,
and the injected binding persists only through the target-local append repository. `011`
retains raw page bodies and exact included/excluded coverage because existing acquisition
and subscription tables cannot represent pre-admission evidence. One profile-scoped
repository is wired offline; no selector/manifest producer or live subscription is wired.
The one-contract source remains diagnostic-only.
Advanced gateway/journal/normalizer/materializer components exist offline; process and
stream-session composition are absent. API/publisher/status
contracts remain inert; no server, worker, acquisition-provider call or canonical route
changed. Only the bounded fresh-reference provider attempts described below occurred.
Restart/heartbeat/service ownership, P9 action service/API/UI, retained replay and
throughput qualification remain later work.

## First-Increment Blockers

| Blocker | Current state |
| --- | --- |
| Strategy coverage | The immutable current selection remains BLOCKED with no manifest/cohort; its pre-open cycle could not satisfy stable 1,800-second mark freshness |
| Equity validation | Acceptance-verified: pointer/manifest/cohort/instruments 1/1/1/9, 19,557 bars, 27 feature snapshots, nine screening rows and ten portal snapshots |
| Authorization | Further API/portal work and the separate market-hours option attempt each require explicit principal/process authorization |

Not blockers: Advanced gateway/journal, replay, supervisor, publisher/outbox, performance, Windows
service ownership, 400/50 expansion, cutover, Schwab, retention execution and other portal pages.

## Historical Verification (Not First-Increment Gates)

The full dated record is in the [verification archive](archive/ADVANCED_REALTIME_VERIFICATION_LOG_2026-10-04.md).
The current full realtime suite passed 371/371 in 15.59s after the bounded exact-ingestion and raw
bar/option-mark storage composition. The earlier interrupted 167-pass capture remains a dated
historical row below and no longer controls the current regression verdict.

| Date | Slice | Evidence | Result |
| --- | --- | --- | --- |
| 2026-10-04 | P6-P8 offline | `backend/tests/realtime`: 189 passed in 8.58s | Offline regression verified |
| 2026-10-04 | Stable compatibility | 766 passed, 1 skipped; one named unrelated publication-history SQL assertion mismatch | Code complete with known mismatch |
| 2026-10-04 | Data-access optimization | `backend/tests/realtime`: 194 passed in 8.36s | Offline contracts verified |
| 2026-10-04 | Worker DB wiring | `backend/tests/realtime`: 202 passed in 8.64s; no real pool | Offline contracts verified |
| 2026-10-04 | Holistic correction | Fenced job/outbox terminal paths and one-cursor transaction composition | Code complete offline |
| 2026-10-04 | P6 performance pass | 14 focused tests plus prior 213-test capture; exact depth-8 restore measured | Synthetic/offline only |
| 2026-10-04 | P9 mock foundation | 21 focused tests passed in 2.70s; full-suite capture unknown | Code complete backend foundation |
| 2026-10-04 | Latest-schema qualification | 29 focused tests passed in 2.80s; 69-relation inventory exact; full suite stopped at 32% | Code complete offline; full suite `UNKNOWN` |
| 2026-10-04 | Developer acquisition handoff | 36 focused tests passed in 2.64s; diagnostics clean; 71-relation inventory exact; one full-suite capture stopped at 31% | `CODE_COMPLETE` offline; full suite `UNKNOWN` |
| 2026-10-04 | Developer COMPUTE consumer | 36 focused tests passed in 2.69s; diagnostics clean; atomic bar/option output and rollback/retry paths covered; one full-suite capture stopped after 60% plus two tests | `CODE_COMPLETE` offline; full suite `UNKNOWN` |
| 2026-10-04 | Developer acquisition worker | 43 focused tests passed in 2.47s; diagnostics clean; bounded package selection, one-pass claim/process/failure transitions and immutable status covered; full suite not relaunched after repeated capture loss | `CODE_COMPLETE` offline; full suite remains `UNKNOWN` |
| 2026-10-04 | Eight-ticker canary safety alignment | 133 focused tests passed in 3.71s; config check returned exactly eight approved symbols, `polygon_developer`, read-only and zero provider requests; preflight tracks `000-010`; runtime wiring rejects wrong database/lineage | `CODE_COMPLETE` offline; target remains absent |
| 2026-10-04 | Provider-neutral Developer producer | 68 focused producer/handoff/worker/repository/preflight/isolation tests passed in 3.22s; diagnostics clean; fake sources cover exactly all eight tickers and commit stock/option batches in one INGRESS transaction; no provider or database access | `CODE_COMPLETE` offline; Polygon adapters unauthorized/missing |
| 2026-10-04 | Dedicated realtime validation API | 57 focused API/compatibility/projection/route/security tests passed in 3.18s; diagnostics clean; exact eight-ticker status blocks partial coverage; verified READER pool and repeatable-read precede repository queries; no canonical imports/fallback | `CODE_COMPLETE` offline; app not started |
| 2026-10-04 | Bounded portal publisher | 57 focused publisher/projection/API/route/security/topology tests passed in 3.26s; diagnostics clean; exact eight-page promotion, active-cohort claim fences and delivered/retry/abandoned transitions covered with fake sinks | `CODE_COMPLETE` offline; no process/network |
| 2026-10-04 | Polygon Developer source adapters | 66 focused fake-HTTP/producer/handoff/equity/option/normalizer/provider tests passed in 3.33s; diagnostics clean; 24 bounded fake calls yield exact eight equity and eight admitted option facts in one INGRESS transaction | `CODE_COMPLETE` offline; no live provider evidence |
| 2026-10-04 | Canary supervisor/status contract | 47 focused supervisor/source/producer/worker/publisher/API/database/topology tests passed in 3.03s; diagnostics clean; deterministic stage aggregation, stale blocking, atomic redacted status, tamper/size rejection and instance-fenced stop intent covered | `CODE_COMPLETE` offline; no lifecycle action |
| 2026-10-04 | Frontend canary validation route | 2 focused route/isolation tests passed; existing `Build portal` task compiled TypeScript and built 2,014 modules in 3.68s with dedicated 5.45 kB JS/3.36 kB CSS chunks; broader frontend suite 163 passed with one unrelated persistence-page source assertion in untouched files | `CODE_COMPLETE` offline; no server started |
| 2026-10-04 | Disposable `000-010` security rehearsal | Explicitly authorized `test_realtime_canary_20261004` and `test_rt_canary_20261004*` scope: command completed; 71 relations and versions `000-010` verified with no pending migrations; grant/denial audit passed; `finally` dropped database/roles; read-only cleanup check returned `CLEAN`, `database_exists=false`, `existing_roles=[]` | `VERIFIED`; zero residue, no canonical/provider/service access |
| 2026-10-04 | Full realtime backend gate | `backend/tests/realtime`: 277 passed in 9.25s with captured exit 0 | `VERIFIED` offline |
| 2026-10-04 | Actual target provisioning | Authorized isolated test scope: created `stocks_realtime_v1`, three credential-backed principals and eight capability/owner roles; applied `000-010` with zero pending versions; security audit passed every built-in grant/denial check | `VERIFIED`; no canonical access or service start |
| 2026-10-04 | Fresh reference activation and rollback | First bounded attempt stopped at the one-page dividends fence. One separately authorized retry allowed 16 pages/68 total requests; output capture ended without completion, no matching process remained, and durable status remained `seed=null` with zero equity/option identities. All eleven generated roles were then set `NOLOGIN`; runtime remained `NOINHERIT` | `BLOCKED`; no durable reference writes, acquisition/services not started |
| 2026-10-04 | Post-edit full realtime regression | Capture reported 167 passing tests, then ended without duration or exit code; no matching pytest process remained and the run was not replayed. Focused provisioning tests passed 4/4; focused reference tests passed 9/9 | `UNKNOWN` full result; focused checks passed |
| 2026-10-04 | Rotated provider credential check | Purpose-built redacted mode reported Polygon and Finnhub keys present and non-placeholder, `values_exposed=false`, zero database connections and zero provider requests; focused tests passed 3/3 | `VERIFIED` configuration only; no unlock or retry |
| 2026-10-04 | Ticker-scoped reference correction | Authorized retry atomically reported `FAILED`: market-wide dividends exceeded adapter bounds before persistence. All principals were re-locked. Corporate-action requests now carry one approved ticker each, reject identity drift, and produce a 44-call seed plan; 15 focused tests passed in 3.844s | `CODE_COMPLETE` offline; corrected live retry unauthorized |
| 2026-10-04 | Corrected reference retry | Runtime alone was unlocked; retry wrote atomic `RUNNING`, but the shared terminal returned before completion. Fresh process inventory found no matching process; durable status remained `seed=null` with zero identities. All principals were re-locked and the stale report retained | `UNKNOWN` execution wrapper; no durable writes, no acquisition/services |
| 2026-10-04 | Task-based reference retry | Dedicated process task captured exit 1 and atomic `FAILED`: Massive transport exhausted its 64-request provider cap while fetching option open interest, which would exceed the approved 68-total envelope after other sources. Collection failed before persistence; all eleven roles were verified `NOLOGIN`, runtime `NOINHERIT` | `BLOCKED`; exact eight-contract catalog/OI plan required, no retry authorized |
| 2026-10-04 | Exact eight-contract reference plan | Catalog selection admits one deterministic active call from the first page per underlying and fetches one exact-contract OI snapshot; provider identity drift remains rejected. Fake-HTTP plan is 44 requests and 42 seed members. Touched adapter/transport tests passed 7/7; adjacent service/seed/script/security tests passed 20/20 | `CODE_COMPLETE` offline; live task unauthorized |
| 2026-10-04 | Exact-contract live reference seed | Dedicated process task returned `COMPLETE`: 44 provider requests, 963,474 response bytes, 42 members and generation-1 `READY`. Durable status returned exact approved eight-symbol equity and option-underlying sets. All generated roles were re-locked after validation | `VERIFIED`; reference prerequisite complete, acquisition not started |
| 2026-10-04 | Acquisition prerequisite trace | `010` requires `rt.cohorts` and `rt.instruments`; verified seed persistence creates neither instruments nor base subscription/cohort state. Existing tier activation assumes those identities. No market provider call or database mutation followed | `BLOCKED`; minimum SHADOW control-plane bootstrap required |
| 2026-10-04 | SHADOW control-plane bootstrap | Deterministic plan covers 8 equity plus 8 option instruments, 40 subscription members and one base SHADOW cohort. Staged repositories require a non-PASS/BLOCKED Developer passive profile and contain no readiness PASS, tier activation or portal-pointer writes. Focused preflight/security/repository tests passed 30/30 in 1.95s; diagnostics clean | `CODE_COMPLETE` offline; CLI/target apply unauthorized |
| 2026-10-04 | SHADOW bootstrap CLI | Inert plan resolved configuration `0a9f355c...5c60e0`, valuation `2570137d...55e289` and explicit not-materialized model `04a7dddd...d3ae8c`; reported 16 instruments, 40 members, SHADOW, BLOCKED non-PASS profile, zero portal activation and zero provider requests. Dedicated process task added. Focused checks passed 33/33 in 1.99s; source/task diagnostics clean | `CODE_COMPLETE` offline; target mutation unauthorized |
| 2026-10-04 | SHADOW target bootstrap | Initial authorized task failed read-only because finite seed revisions were queried as open-ended; zero writes occurred and principals were re-locked. Effective-window selection was corrected and the same scope completed: profile `FAIL/BLOCKED`, 8 EQUITY, 8 OPTION, 40 members, cohort `04c9bc0a-366d-52d9-ace8-e15a48aafdbd`, SHADOW, zero portal pointers and zero provider requests. All principals re-locked | `VERIFIED`; inert acquisition identities ready |
| 2026-10-04 | Paced Developer acquisition runner | Dedicated smoke CLI/task loads only blocked SHADOW identities and existing Polygon/normalizer/calendar contracts. Local smoke ceiling is 24; generic producer capacity remains 88 for broader adapter work. Plan is 8 equity + 8 underlying bars + 8 option snapshots, concurrency one, 12-second spacing (max 5/minute), zero retries and no canonical fallback. Focused checks passed 27/27 in 2.01s; diagnostics clean | `CODE_COMPLETE` diagnostic smoke path; not strategy-complete |
| 2026-10-04 | First live INGRESS attempt | Two read-only loader defects failed before provider access and were corrected without writes. The next task consumed one equity request, then rejected Polygon pagination from the broad four-day minute range before persistence; all roles were re-locked. Runner now requests exactly the latest completed exchange minute by millisecond range and retains pagination rejection; focused checks passed 41/41 in 2.764s | `BLOCKED` live; no handoff writes, corrected 24-call retry unauthorized |
| 2026-10-04 | Mark-aligned INGRESS correction | Exact-minute retry reached option normalization but failed before persistence because the one close-minute underlying bar followed the admitted option mark and supplied no prior aligned spot. All roles were re-locked. Option source now fetches snapshot first and requests the exact completed minute from its mark timestamp; request count remains 24 and pagination stays denied. Focused checks passed 41/41 in 2.18s | `CODE_COMPLETE` offline; no handoff writes, new live run unauthorized |
| 2026-10-04 | Live contract qualification failure | Snapshot-first retry reached stable normalization and rejected the generation-1 call with `OUTSIDE_STRIKE_CORRIDOR` plus `LIQUIDITY_FLOOR`; no producer transaction or handoff write occurred. All principals were re-locked and policy remained unchanged | `BLOCKED`; stable 60-DTE target-local reuse adapter required |
| 2026-10-04 | First contract candidate probe | Provider-only task used exact completed-minute spot and unchanged contract policy but rejected the 0-60 DTE snapshot because it required pagination; no database access occurred. Probe now fixes expiration to the next weekly Friday while retaining +/-15% strike, American/100x and volume>=20 or OI>=100 gates. Focused tests passed 13/13 in 2.728s | `CODE_COMPLETE` offline; corrected 16-call probe unauthorized |
| 2026-10-04 | Qualified contract candidate probe | Corrected provider-only task completed: 16 requests, 423,110 response bytes, eight next-Friday selections and manifest `5f875b8a...7dfe`; every contract is near spot, American/100x and above observed volume/OI floors. Database writes 0; credential values exposed false | `VERIFIED` diagnostic smoke evidence only; not generation-2 authority |
| 2026-10-04 | Stable-options alignment review | Existing `PolygonDeveloperEngine.get_option_chain` and orchestration already implement 60-DTE corridor acquisition, validated pagination, complete batches, normalization and strategy lanes. Realtime one-contract source was confirmed narrower and may remain only as a smoke substage. Generic producer cap restored to 88; smoke CLI remains locally capped at 24. Focused checks passed 29/29 | `BLOCKED` strategy validation; stable 60-DTE target-local reuse adapter required |
| 2026-10-04 | Shared 60-DTE discovery adapter | Stable engine/orchestration and realtime discovery share one injected bounded adapter. Repetition, hostile URLs, page/row/byte caps, partial rejection, exact clocks/hashes and import isolation pass. Focused command returned 49 passed in 2.46s; diagnostics clean | Stage B `CODE_COMPLETE_OFFLINE`; no provider/database/process/schema state changed |
| 2026-10-04 | Realtime discovery repository binding | Additive `011` defines immutable batch/page/coverage evidence; REFERENCES alone appends. Repository reconciles exact bodies, clocks, hashes, cursors, metadata and 4 included/4 excluded boundaries. Focused aggregate returned 98 passed in 2.71s; diagnostics clean | `CODE_COMPLETE_OFFLINE`; target remains `000-010`, `011` pending, no provider/database/role/process action |
| 2026-10-04 | Advanced-safe discovery correction | Shared scope/repository bind profile, tier, universe and policy; pending `011` admits both tiers. Developer 60/15 and expanded Advanced 45/20 fixtures use one adapter; anti-drift tests reject shared canary/Developer constants. Focused aggregate returned 141 passed in 2.71s | `CODE_COMPLETE_OFFLINE`; Advanced execution still unclaimed, target unchanged |
| 2026-10-04 | Disposable `011` discovery qualification | Owned `test_realtime_discovery_20261004` scope migrated `000-011`: schema `037dccdc...a94c`, 74 relations, zero pending and all security checks. REFERENCES persisted AAPL Developer and AVGO Advanced evidence: 2 batches, 2 pages, 16 coverage rows; provider requests 0. Cleanup check returned `CLEAN`, database absent, roles empty | `VERIFIED` disposable only; actual target remains `000-010`, canonical/provider/process state unchanged |
| 2026-10-04 | Stage C bounded session manifest | Stable-filtered candidates are allocated by exact DTE lane, side, strike depth and liquidity with complete dispositions. Developer selects 128 contracts across 7/21/45/60 lanes with O2 unavailable. Advanced selects 50x16=800 plus 200 reserved and blocks 51 underlyings. Focused aggregate passed 152 in 3.05s | `CODE_COMPLETE_OFFLINE`; no persistence/provider/database/process action |
| 2026-10-04 | Stage C manifest repository binding | Existing subscription tables retain READY members; additive `012` stores immutable READY/BLOCKED summary, dispositions and coverage. CONTROL-only fake repository reconciles exact profile/reference/instrument/member/audit rows and rolls back missing identity/relabel/conflict paths. Focused aggregate passed 158 in 3.20s | `CODE_COMPLETE_OFFLINE`; `012` pending, no database/provider/process action |
| 2026-10-04 | Disposable `012` manifest qualification | Owned `test_realtime_manifest_20261004` scope migrated `000-012`: schema `61c7f885...8992cc`, 77 relations, zero pending and 31 security checks. Discovery retained both tiers; CONTROL persisted `READY=2`/`BLOCKED=2`, 14 members, 6 dispositions and 8 coverage rows; provider requests 0. Initial synthetic-clock failure cleaned fully; corrected replay passed. Final cleanup `CLEAN`, database absent, roles empty | `VERIFIED` disposable only; actual target remains `000-010`, canonical/provider/process state unchanged |
| 2026-10-04 | Target `011-012` apply and security audit | Read-only pre-plan showed only `011-012` pending and 11 locked roles. Applied exactly 2 migrations; security apply with locked-principal mode passed 33 checks; forced re-lock returned `PRINCIPALS_LOCKED`. Read-only target report: schema `61c7f885...8992cc`, versions `000-012`, 77 relations, zero pending, all roles `NOLOGIN`, runtime `NOINHERIT`, all six new evidence tables zero | `SCHEMA_READY` target; no provider/canonical/process action and no discovery/manifest rows |
| 2026-10-04 | Pre-session producer composition | Injected collection validates exact universe, scope/security/cycle/evidence lineage and aggregate bounds before deterministic discovery then selection persistence. Developer 8-underlying path yields 128 READY contracts; Advanced 50-underlying path yields 800. Missing packages/bounds/drift write nothing; full discovery with missing depth persists BLOCKED. Focused aggregate passed 166 in 3.92s | `CODE_COMPLETE_OFFLINE`; target loaders/process databases absent, no provider/database action |
| 2026-10-04 | Pre-session target role wiring | Read-only repeatable-read REFERENCES loader reconciles profile/universe/security identities. One atomic REFERENCES transaction persists exact discovery coverage; a separate CONTROL transaction persists READY/BLOCKED selection. Partial collection writes nothing; REFERENCES and CONTROL failures roll back at their own boundary. Focused aggregate passed 184 in 4.49s | `CODE_COMPLETE_OFFLINE`; stable source adapter/process startup absent, no provider/database action |
| 2026-10-04 | Shared Polygon pre-session source | Injected spot/catalog/bar boundaries feed shared paginated discovery, page-clock parsing and unchanged normalizer into exact catalog-versioned packages. Two-page, hostile URL, unadmitted identity, liquidity/capacity and exact eight-underlying/128-candidate coverage pass. Focused aggregate passed 190 in 4.57s | `CODE_COMPLETE_OFFLINE`; executable task/provider execution absent, no database action |
| 2026-10-04 | Inert pre-session one-pass CLI | Environment-free PLAN reports hash `fa69967f...cc738`, 8 underlyings, 7/21/45/60 DTE, 128 primary/200 reserved, 320 requests, 512 MiB, REFERENCES/CONTROL and zero writes/requests. Injected execution reports redacted READY/BLOCKED and rejects absent runtime/scope-budget drift. Focused aggregate passed 196 in 4.97s | `CODE_COMPLETE_OFFLINE`; runtime factory absent, live execution impossible |
| 2026-10-04 | Production pre-session runtime factory | Execute-only factory loads canary config/secrets, verifies REFERENCES/CONTROL pools and blocked profile, meters one transport and reuses stable spot/catalog/bar plus shared discovery/normalization. REFERENCES now persists complete discovery/catalog/version/instrument packages before CONTROL. PLAN hash unchanged and inert with factory installed. Focused aggregate passed 204 in 4.93s | `CODE_COMPLETE_OFFLINE`; full fake factory execution/provider action absent |
| 2026-10-04 | Manifest-bound Developer acquisition identity | READY selection creates a deterministic inert SHADOW cohort; BLOCKED creates none. Loader requires a current-session READY audit and exact manifest members/dispositions, rejecting missing/extra/duplicate/stale/catalog/cross-manifest drift. Exact tests 17/17, adjacent flow 80/80, shared discovery compatibility 42/42 | `CODE_COMPLETE_OFFLINE`; no provider/database/role/process action |
| 2026-10-04 | Bounded exact ingestion/raw storage | Existing CLI loads identities under CONTROL, persists handoff under INGRESS and stores raw bars/option marks under COMPUTE; discovery/claims fence exact cohort plus producer batch IDs; option jobs require marks; degraded/incomplete output fails closed and O2 stays unavailable. Focused 29/29, vertical-slice aggregate 98/98, full realtime 371/371 | `CODE_COMPLETE_OFFLINE`; technical features, detectors and portal values remain absent |

Rows before the actual target provisioning entry performed no production provider,
database, role/pool, migration, service, worker, publication or route operation. The
authorized target/reference rows changed only the isolated target and generated roles;
canonical database/API/workers/routes, retained history and broker surfaces were untouched.

## Longer-Term Decisions (Not First-Increment Gates)

- Preserve exact source/policy hashes and immutable evidence; never weaken timing,
  identity, readiness, retention or authorization gates to obtain a pass.
- Reuse stable detector implementations and existing domain repositories.
- Keep Developer and Advanced cohorts distinct; O2 remains disabled in Developer.
- Developer-first sequencing must not narrow shared contracts. Advanced readiness requires
  executable causal composition, not schema support, worker declarations or component tests.
- One active publisher and one all-surface route pointer are required at cutover.
- Reference refreshers become immutable seed services with no old-database dependency.
- Evidence/outbox/checkpoint commits remain atomic; publication is at-least-once with
  receiver dedup and explicit retry/abandonment.
- Developer acquisition consumption reuses deterministic materialization jobs; output
  and job completion are one fenced transaction, with no separate progress schema.
- The bounded acquisition runner reuses the existing COMPUTE slow lane and `1/1` pool
  budget; it does not add a process, service, provider call or schema.
- Developer source adapters return bounded completed domain facts; the producer rejects
  missing/unreviewed symbols and never substitutes a generic payload contract.
- Dedicated portal validation uses a separate realtime-only app and `/api/realtime/*`
  namespace; existing canonical `main.py`, routes and environment remain untouched.
- Portal delivery is at-least-once: sink receivers deduplicate by outbox identity, and
  a lost acknowledgement may redeliver but can never silently mark an alert delivered.
- Polygon Developer adapters admit no unknown identity, pagination, hostile host or
  over-limit response; option marks remain quote-free and use the stable normalizer.
- Whole-chain discovery is bounded pre-session REST evidence only. It cannot imply an
  unlimited live subscription or supersede reviewed selected-contract admission.
- Status paths are injected and require separate ACL/root approval; status and stop
  artifacts are bounded, atomic and never constitute process liveness or authority.
- The canary UI is a direct validation route, not a new portal architecture; dedicated
  deployment selects the realtime API through the existing frontend base URL.
- Runtime is `NOINHERIT`; each process assumes one reviewed minimum capability role.
- Realtime pool construction must verify the dedicated database and lineage before any
  role-scoped transaction; changing the canonical API environment is not a cutover.
- `INGRESS` reads only reviewed reference/control/handoff tables and appends only
  stream/gap/lease or Developer handoff evidence; all projections remain denied.
- `009` remains owner-only until a dedicated action capability exists.
- Local mock execution remains deterministic, manual and permanently non-broker.
- Referenced evidence is not removed by generic TTL; retention execution is separate.

Update this tracker only with changed active state, exact focused evidence, current
blockers and authorization. Append historical detail to a new dated archive rather
than growing this file back into a transcript.

## Latest Stage Update

The current six-part milestone is
[Original Default-Daily Strategies](#original-default-daily-strategies---2026-10-06).
Earlier live reader inspection is dated evidence, not current startup or write
authority. Current work remains source/offline; default-daily strategy source/
rendered-HTML proof is not browser or data readiness. Legacy Compose remains canonical, and durable publication or
container deployment requires separately reviewed target/principal/network scope.

### Screening Basis And Interval Plan

1. **Planned item:** resolve 253 raw daily versus 283 adjusted daily ownership and complete the missing
  higher/lower interval derivation contracts without changing the accepted target.
2. **Accomplished:** added a versioned realtime screening selector that uses 253 split-adjusted daily
  sessions for historical indicators and the latest raw daily fact for nominal quote/audit scale. Added
  explicit native-5m derivation for 15m/30m/1h/raw 1d, adjusted-daily 1wk derivation,
  five native 1m sessions and 60 native adjusted monthly bars while preserving cohort,
  tier, instrument, basis and source-bar lineage. Made the equity
  package initializer inert so pure calculations do not construct the retired canonical DB pool.
3. **Evidence:** original v1 derivation/screening/history/source/plan suite passed 30/30 in 7.26s.
  Complete 5m sessions derive 78/26/13/7/1 bars for 5m source and 15m/30m/1h/1d targets; incomplete
  windows are omitted. Current v2 inert plan for 2026-10-02: 310 logical requests,
  13,601 facts/ticker and 122,409 total; it adds 1,950 native 1m bars and replaces
  13 derived months with 60 native adjusted months. Plan `83fca24b...d551`; policy
  `2a358b31...0860a`; zero I/O.
4. **State changed:** source/docs only. No provider request, database connection/write, role, process,
  cohort, manifest, feature, screening or portal state changed. Current durable counts remain unchanged.
5. **Coverage:** split-adjusted 253-session screening depth; raw current scale; 5m-derived intraday
  intervals; adjusted-daily weekly/monthly; identity/basis/lineage rejection. Provider-adjusted volume
  semantics still require explicit qualification before full liquidity-field parity is claimed.
6. **Next gate:** separately authorize the exact plan above for provider qualification and a fresh
  target generation. One ticker response must stay below 50,000 rows; persistence remains batched at
  1,000. Do not add native 15m or duplicate native 30m. The approved bounded nine-ticker execution may
  run only after the resumable bootstrap has tested stage adapters through ACCEPTANCE and LOCK.

### Generic Fresh-Database Bootstrap

1. **Planned item:** treat `stocks_realtime_v1` as disposable integration state and build one generic,
  resumable, idempotent bootstrap for both nine-ticker rehearsal and a separately named 400/50 target.
2. **Accomplished:** added a secret-free JSON manifest, exact target-kind/name guards, ordered stage plan,
  hash-bound atomic checkpoint state machine, injected stage runner, PLAN/INITIALIZE/STATUS plus
  single-stage execution CLI and a plan-only disposable reset contract. Concrete PROVISION reuses or
  creates the three principal roles plus database with rollback; MIGRATIONS applies only pending checked
  files and re-reconciles; SECURITY creates capability roles, accepts locked principals, reapplies grants
  and audits atomically. The CLI advances one stage, retries failures, and refuses REFERENCES. The 5m
  source is universe-injected while accepted collectors stay fixed. Realtime migrations load only the
  dedicated realtime env.
3. **Evidence:** current focused bootstrap/history/reference/foundation gates are
  included in the 95/95 supporting backend pass. Current integration plan has
  9 equities, 378 maximum provider requests and 122,409 facts; history
  `83fca24b...d551`. Reset plan
  `a3eab7db...68b1d7` preserves all 11 roles and requires all 11 rebuild stages. A synthetic frozen
  400/50 production plan passes with 1,483 history requests and 5,440,400 facts, and rejects the integration
  database name.
4. **State changed:** source/docs only. No checkpoint initialized, reset, role change, process, provider
  request, database connection/write, cohort mutation or data deletion occurred.
5. **Coverage:** manifest exact-key/secret rejection; integration/production target separation; stage
  order/side-effect/request bounds; retryable failure checkpoints; atomic/tamper-evident local state;
  production-scale fact/request estimation; disposable reset refusal for production manifests.
6. **Next gate:** parameterize and implement REFERENCES then CONTROL_PLANE against the manifest universe.
  Do not execute foundation stages or reset yet: every stage through ACCEPTANCE/LOCK must rebuild in one
  resumable workflow first. Bounded nine-ticker provider execution follows those adapters; integration
  keeps all bars. Final production requires a reviewed exact 400/50 ticker manifest.
