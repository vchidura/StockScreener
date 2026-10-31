# Advanced Equity And Options V3 Implementation Plan

> **Detailed subsystem implementation reference.** The canonical lifecycle,
> continuous-worker decisions, integration status and work-package order are in
> [REALTIME_V3_PIPELINE.md](REALTIME_V3_PIPELINE.md). Artifact and external
> authorization steps below describe the existing bounded implementation and are
> not normal runtime prerequisites.

Status: design and implementation plan only; no production authority

Implementation status (2026-10-27): the core Equity materialization,
Equity projection/publication, Options input/cycle/projection/publication and stored
read-pin implementations are present. OS1/OS2 stored-reader identity and the
receipt-bound Stock Alerts projection boundary are complete in source. Source closure
is not complete. The remaining non-production work is ordered as follows:

1. select and implement the V3-owned Equity alert product producer: either port the
   legacy forward intraday/swing product to exact V3 inputs or explicitly replace it
   with a separately specified canonical Equity detector product;
2. make that producer emit both retained streams with the exact immutable binding
   declaration already required by the projector;
3. add the chosen producer owners to the V3 session lifecycle and run provider-free
   restart/publication qualification;
4. apply/verify source migrations only in an authorized disposable database; production
   migration application remains excluded.

Production migration application/hardening and live-market qualification remain
separate later gates and are not part of this source-closure sequence.

Updated: 2026-10-23 UTC

## 1. Objective

Implement the runtime described in
[REALTIME_V3_PIPELINE.md](REALTIME_V3_PIPELINE.md) as a smooth daily Advanced
Equity-to-Options workflow that:

- retains exact immutable evidence;
- captures opening Options T/Q flow without waiting for the later Equity cutoff;
- serializes only the decision, package, projection and publication boundaries;
- avoids duplicated preflight/runtime assembly;
- defers transient lag instead of terminally blocking a valid window;
- restarts from durable stage evidence;
- preserves the reviewed option-flow and package-selection contracts;
- exposes measured extension points for later target and contract scaling.

V3 does not authorize provider calls, migrations, service activation, publication,
delivery, broker actions, trading, replay, repair, backfill, archive, or retention
execution.

## 2. Governing References

- Runtime contract: [REALTIME_V3_PIPELINE.md](REALTIME_V3_PIPELINE.md)
- Option flow policy: [ADVANCED_OPTIONS_FLOW_POLICY.md](ADVANCED_OPTIONS_FLOW_POLICY.md)
- Package and strategy evaluation:
  [OPTION_PACKAGE_STRUCTURE_AND_STRATEGY_EVALUATION.md](OPTION_PACKAGE_STRUCTURE_AND_STRATEGY_EVALUATION.md)
- Current V2 pipeline: [REALTIME_V2_PIPELINE.md](REALTIME_V2_PIPELINE.md)
- Current V2 Options plan:
  [OPTIONS_ADVANCED_V2_IMPLEMENTATION_PLAN.md](OPTIONS_ADVANCED_V2_IMPLEMENTATION_PLAN.md)

The V2 documents remain historical/current-state evidence until V3 is implemented and
selected. V3 does not rewrite applied stage catalogs or migrations.

## 3. Final V3 Decisions

### D1 - Acquisition concurrency, decision seriality

Equity STOCKS/AM and authorized Options T/Q acquisition run concurrently during the
session. Options contract discovery/admission is prepared before the opening capture
boundary under its own authority.

The serial dependency begins at the immutable cycle:

```text
completed live Equity E
  -> exact Options cutoff O, where E <= O <= E + 300s
  -> OPTIONS_INPUT_READY
  -> cycle
  -> projection
  -> publication
```

A canary that starts T/Q only after E cannot qualify opening-flow or complete
900-second accumulation behavior.

The opening acquisition authority and the later E-bound cycle authority are separate
immutable grants. The former has no cycle/publication authority; the latter grants no
additional provider scope.

### D2 - Three distinct Equity/Options identities

- `H`: retained Developer cohort for exact historical reference facts.
- `E`: completed target-9 Advanced Equity cohort/window/receipt and exact
  policy-required materialized bar/technical-input identity.
- `O`: target Options window/cutoff and complete Options input closure.

H and E are not collapsed into one cohort or receipt. E and O are not forced equal.

### D3 - Exact source scopes

- Equity source profile: exact active target 9, STOCKS/AM.
- Equity universe: exact nine-name target-9 membership.
- Options underlyings: exact QQQ/SPY for the initial V3 package.
- Options contracts: exact selected catalog versions.
- Options WebSocket members: exact T/Q slot membership.

Generic loaders receive these values from versioned policy. They do not hard-code
target 9, QQQ/SPY, or 300 seconds as universal defaults.

### D4 - Window-scoped E binding

The durable live-Equity binding is keyed to the target Options window/cycle, not only
the Options cohort. It records one exact source E receipt and one explicit age policy.

This supports several daily Options cycles in one cohort without manufacturing a new
cohort per window.

### D5 - Product detector independence and Equity context

Equity S1/S2 and Options detectors are sibling consumers of completed Equity
materialization. Options does not wait for, read, or recreate the Equity S1/S2 event
lifecycle.

Each Options lane/strategy declares its own Equity-context requirements:

1. bind the exact E `INPUT_READY`;
2. load exact required QQQ/SPY bar revisions or already-owned technical evidence visible
   under E;
3. validate security, cohort, session, interval, finality, basis and cutoff;
4. hash the exact membership into the Options input identity;
5. run any required stock confirmation as a pure cycle-local Options adapter.

Each context requirement explicitly names H or E, interval, adjustment basis, session
count/range, finality, known-by clock and missing/degraded behavior. H normally owns
prior-session adjusted reference inputs; E owns current-session finalized context.
Source selection is never inferred from interval alone.

Do not impose one global bar bundle on every Options detector. O1 needs no Equity S1/S2
outcome. O3 and package strategies consume only their versioned policy requirements.
Missing technical context blocks or degrades only the dependent lane.

The first implementation should prefer direct E bars and pure calculations. Add a
separate immutable Equity technical-feature owner only after multiple measured
consumers justify it.

Remove `DetectorFamily.S1` and `DetectorFamily.S2` from required Options detector
membership. Keep Equity detector registry identities product-scoped. If a pure stock
algorithm is reused, expose it as a named, versioned Options context adapter such as
breakout or pullback/resume confirmation, not as an Equity detector event.

The common Options input closure binds E and records lane-context availability.
Missing lane-specific technical context produces an explicit cycle outcome; it does
not block the shared input receipt or unrelated Options lanes.

### D6 - Flow policy is preserved

Consolidation must retain this processing order:

```text
admission
  -> condition treatment
  -> factual cumulative volume
  -> causal NBBO aggression
  -> profile retention
  -> bounded clustering
  -> opening/volume-OI inference
  -> detector/screener filters
  -> exact package construction
```

Small eligible prints update cumulative volume even when no retention profile accepts
them. Flow observations do not create package legs.

### D7 - Algorithmic filter honesty

The seven conceptual filters in the package strategy guide are not all accepted
production lanes.

- O1/O3 remain accepted Options-native lanes.
- Equity S1/S2 remain independent Equity lanes and are not Options prerequisites.
- Breakout or pullback/resume algorithms may be reused by an Options strategy only
  under distinct Options input/policy/evaluation identities.
- O2 remains `UNAVAILABLE/O2_MODEL_NOT_ACCEPTED`.
- Gamma, block, sweep and accumulation flow can be retained as evidence under the
  versioned flow policy.
- Unaccepted IV-surface, dark-pool, VIX/VVIX, parity, multi-day LEAP and sub-50ms models
  remain unavailable or research-only.

V3 must not rename current five-second sweep evidence as the proposed sub-50ms gamma
model, or current intraday accumulation as the proposed multi-day LEAP model.

### D8 - One owner per durable result

- bridge owns Equity bars/checkpoints;
- Equity materialization owns `INPUT_READY`;
- Equity detector owners own S1/S2 events/inputs;
- Options input/context owners read exact E bars/features without owning Equity detector
  events;
- market-state consumer owns current Q/T heads and checkpoints;
- flow consumer owns volume/events/clusters/checkpoints;
- Options reference owner owns one immutable reference generation;
- Options input gate owns rolling/mark/native closure and `OPTIONS_INPUT_READY`;
- cycle owns detector/package/evaluation evidence;
- projection owns stored snapshots and `OPTIONS_PORTAL_READY`;
- canonical publisher owns release/pointer SQL.

No synthetic Equity receipt, parallel closure table, second publisher, or fake target
readiness is added.

### D9 - Transient versus terminal outcomes

Add an explicit bounded deferred state for:

- bridge/finalizer lag;
- market-state/flow checkpoint lag;
- journal drain in progress;
- retryable serialization/deadlock conflicts;
- role completion not yet available.

Policy refusal, authorization absence, immutable divergence, affected unresolved gaps,
or invalid market-data identity remain terminal.

### D10 - Durable restart

Every completed stage can rehydrate from durable rows and receipts. In-memory input,
cycle and projection handoffs remain bounded caches only.

### D11 - Prospective admission and split authority

The opening T/Q slot set is selected before the capture boundary from authorized
pre-session catalog/reference evidence. It cannot depend on later same-session flow.

V3 uses:

- one exact session acquisition authorization/receipt for discovery, admission, T/Q
  capture, provider budgets and journal ownership;
- one later target-window cycle authorization/receipt for exact H/E/O,
  input/cycle/projection and any separately granted publication.

If E is absent, only non-serving captured evidence remains. No cycle grant is created.
If a contract is admitted late, its earlier opening interval remains unavailable unless
separately authorized replay proves exact coverage.

## 4. Current Change Disposition

### Keep

- Actual completed Equity control-plane loading.
- Exact E source report and package hash binding.
- Distinct H/E/O identities.
- Exact reference-session validation.
- Existing market-state and flow consumers.
- Existing reference, rolling, mark, native and input-closure domain contracts.
- Existing canonical portal publisher and atomic pointer behavior.
- Explicit 300-second package policy.

### Replace

- Cohort-primary-key E binding with a target-window binding.
- Mixed H-daily/E-intraday native reconstruction with policy-declared context loaded
  from exact E materialized inputs.
- Post-E activity-ranked selection as the production opening-slot path with prospective
  pre-session admission; retain the former only as an explicitly late canary.
- Dynamic post-provider O selection with one exact package-bound O.
- Repeated runtime preflight/produce/reconstruct sequences with one owner operation per
  stage and durable hash verification.
- Filesystem role polling with a durable database completion barrier.
- In-memory-only stage dependencies with durable rehydration.
- Inert `OPTIONS_PUBLISH` plus outer publication with one truthful stage/publication
  path.

### Remove

- Synthetic Equity plan/window/receipt creation.
- Manual `REFERENCE` or `EQUITY_MATERIALIZE` readiness insertion.
- Raw-15 H as an implicit requirement for every Options native lane.
- Mandatory Equity S1/S2 event completion in the Options readiness contract.
- Generic non-bound 300-second fallback.
- Artificial mixed-lineage detector fixtures.
- Package consumption for a zero-provider transient preflight defer.

## 5. Prospective Schema Work

### 5.1 Live Equity binding

Because migration 033 is uncommitted/unapplied in the current worktree, revise it
before application. If it becomes applied elsewhere, create a new forward migration
instead.

Minimum normalized relation:

```text
binding_id uuid primary key
target_window_id uuid unique not null
target_cohort_id uuid not null
source_equity_cohort_id uuid not null
source_equity_window_id uuid not null
source_equity_receipt_id uuid not null
equity_cutoff timestamptz not null
maximum_age_seconds integer not null
policy_sha256 char(64) not null
authorization_reference varchar(160) not null
source_identity_sha256 char(64) not null
binding_sha256 char(64) unique not null
created_at timestamptz not null
```

Requirements:

- referenced source receipt is `INPUT_READY`, complete, and belongs to the source
  Equity window/cohort;
- target window belongs to the target Options cohort;
- target cutoff and E cutoff are in one session;
- `E <= O`;
- `O - E <= maximum_age_seconds`;
- immutable update/delete/truncate rejection;
- app INSERT/SELECT only as needed;
- no API grant without a concrete stored reader;
- exact replay is a no-op;
- divergent replay fails.

### 5.2 Stage catalog

Add immutable `stages_v5`; never edit `stages_v1`-`stages_v4`.

Required stage semantics:

- shared security/calendar reference followed by session-scoped acquire/admission;
- session-scoped Options reference frozen before opening flow inference;
- resident Options flow depending on exact admission and Options reference;
- input gate with external E binding, checkpoints, rolling, marks and native inputs;
- cycle;
- projection;
- canonical publication.

### 5.3 Deferred stage state

Add the smallest repository/schema support required to record:

- deferred reason;
- next eligible attempt;
- bounded attempt count;
- original deadline.

Do not reuse terminal `BLOCKED_POLICY` for ordinary source lag.

### 5.4 Resident completion barrier

Prefer an existing jobs/barriers relation if its ownership fits. Otherwise add one
small target-window role barrier. Do not add another scheduler or status lifecycle.

Members:

- ingress;
- market state;
- flow.

Each member records generation, owner epoch, ending offset, evidence hash, status and
completion time.

Implementation checkpoint (2026-10-23):

- migration `035_advanced_options_v3_stage_barrier` adds immutable `stages_v5`
  without changing `stages_v1`-`stages_v4`;
- `OPTIONS_INPUT_GATE` has no target-cohort `EQUITY_MATERIALIZE` prerequisite;
  its external authorities are the exact target-window E binding and the durable
  resident completion barrier;
- one append-only `rt.advanced_options_resident_completions` relation owns exact
  `INGRESS`, `MARKET_STATE` and `FLOW` terminal evidence;
- recognized source lag and retryable `40001`/`40P01` conflicts transition
  `RUNNING -> DEFERRED` with readiness `WAITING_INPUT`; an eligible bounded retry
  transitions `DEFERRED -> RUNNING`, then to `COMPLETE`, while exhausted attempts
  transition to terminal `FAILED`;
- a V3 target-window stage effect requires a durable window-cycle receipt, and
  cycle consumption requires the session acquisition receipt first.

The final provider-free integration checkpoint preserves the separate
`stages_v4` P8 path and activates no production plan. An exact target-9
`STOCKS/AM` plus QQQ/SPY `OPTIONS/T/Q` V3 deployment profile now gates the V5
WINDOW role, durable resident barrier and direct canonical publisher. The
profile is disabled by default; no live package, stored-read grant, production
service action, migration, image deployment, delivery or trading authority was
created.

### 5.5 Authorization split

Reuse the existing authorization/package/usage owners where their identities and
one-use semantics fit. Add no parallel scheduler or generic authorization lifecycle.

The durable contracts must distinguish:

- session acquisition scope and consumption at the first provider effect;
- window cycle scope and consumption at the first target-window write/stage effect;
- separately prepared publication authority;
- zero dispatch, delivery, broker and trading authority.

## 6. Runtime Work Packages

### V3-P1 - Correct Equity source contract

1. Require exact target-9 runtime profile and universe.
2. Freeze exact E window/receipt/cutoff.
3. Define versioned Equity-context requirements per Options lane and package strategy.
4. Require each input to name H or E and its exact interval/basis/session/clock shape.
5. Build one set-oriented loader for exact required QQQ/SPY E bars/features while H
   remains owned by the frozen reference stage.
6. Validate bar/feature scope, clocks, basis, finality and membership.
7. Produce one canonical E-context digest for `OPTIONS_INPUT_READY`.

Completion:

- target-1, target-2 and target-25 sources fail;
- real Equity materialization output passes without fixture-only detector lineage;
- absent/delayed Equity S1/S2 does not block O1 or another complete independent Options
  lane;
- missing/divergent QQQ technical context blocks only the lane that declares it;
- an Options failure does not suppress Equity S1/S2.

### V3-P2 - Correct clocks and package policy

1. Split the pre-open session acquisition authorization from the later window cycle
   authorization.
2. Add exact O to the window cycle package.
3. Bind maximum age and policy SHA.
4. Enforce `E <= O <= E + 300s` at preparation, runtime and audit.
5. Remove generic 300-second defaults.
6. Preserve exact-cutoff behavior for paths without explicit age policy.
7. Create the session receipt immediately before the first provider effect.
8. Create the window cycle receipt immediately before the first target-window
   write/stage effect.

Completion:

- E=O and E<O pass;
- 300 seconds passes;
- 301 seconds fails;
- a zero-provider session defer does not consume the session authorization;
- any session provider effect terminally consumes the session authorization;
- a read-only H/E/O cycle defer does not consume the cycle authorization;
- the cycle authorization grants no new provider request/socket scope.

Source status (2026-10-22): the safely combined V3-P1/P2 slice now removes
S1/S2 from Options membership, binds each target Options window to one real E
receipt with an explicit age policy, preserves exact-cutoff behavior for
non-bound readers, carries that identity through P8 package/restart/audit, and
uses typed lane-specific E context. It does not implement the separate
session/window authority split, provider/service actions, `stages_v5`, or any
market-readiness claim.

Source status (2026-10-23): the provider-free split-authority slice adds
immutable session-acquisition and target-window cycle contracts plus durable
one-use receipts under the existing provider-budget, socket-budget,
pre-session manifest, slot-set, reference-state, cohort/window and Equity
receipt identities. Read-only preflight creates no receipt. The session grant
has exact provider/T/Q scope and zero cycle, publication, delivery or trading
scope; the cycle grant binds H/E/O/build/window/evidence, has zero provider or
socket scope and is consumed at the first target-window write/stage effect.
Exact replay is idempotent and divergent replay is refused. Migration 034 is
source-only and has not been applied to production. Runtime provider/socket and
stage call sites are not wired or exercised, and this is not market readiness.

### V3-P3 - Preserve opening and flow identification

1. Prepare exact contract admission and Options reference before the opening capture
   boundary.
2. Prohibit selection of the opening slot set from later same-session flow.
3. Start T/Q acquisition concurrently with Equity under independent authority.
4. Preserve volume-first accounting.
5. Load `advanced-options-flow-v1` through the runtime settings and source-flow hash.
6. Preserve U/B/S/A routing, DTE lanes, thresholds, aggression and completeness.
7. Persist final/provisional/retracted cluster revisions correctly.
8. Freeze opening inference with explicit incomplete states.
9. Record whether the session had complete opening and 900-second coverage.

Completion:

- a small eligible print updates volume but need not create an event;
- OR contract/premium retention matches policy;
- ask/bid/inside/missing/stale/crossed/complex classes are causal;
- opening inference refuses incomplete baselines;
- late-start capture cannot claim complete opening evidence;
- late contract admission cannot claim coverage before its active subscription;
- cluster restart/replay is identical.

Source status (2026-10-23): initial QQQ/SPY prospective admission is versioned
and provider-free. It ranks only complete authorized pre-session catalog,
reference OI, day-liquidity and moneyness evidence; missing exact ranking or
reference inputs return explicit `UNAVAILABLE`. Exact T/Q subscription
readiness and frozen journal/reference identities must precede the capture
boundary. The current activity-ranked P8 package is now explicitly classified
as a post-E late canary and cannot claim opening or 900-second coverage.
Opening-flow runtime inference, resident integration and actual coverage
qualification remain unimplemented.

### V3-P4 - Consolidate Options input ownership

1. Move reference production into its real stage owner.
2. Make one input-gate operation assemble/persist rolling, marks, native inputs and the
   compact closure.
3. Remove out-of-stage preproduction followed by in-stage reconstruction.
4. Replace required Options S1/S2 membership with the per-lane context dependency
   matrix.
5. Separate shared closure failures from typed lane-specific availability.
6. Use one consistent snapshot or explicit source version tokens.
7. Persist one `OPTIONS_INPUT_READY`.
8. Keep bounded retries only for known PostgreSQL transient states.

Completion:

- no duplicate full assembly on the success path;
- exact replay returns the same receipt;
- divergent source tokens fail;
- missing technical context blocks only its declared cycle lane;
- query count is bounded by stage, not by repeated preflight layers.

### V3-P5 - Durable stage restart

Implement rehydration for:

- input gate -> cycle;
- cycle -> projection;
- projection -> publish.

Completion:

- terminate and reconstruct the WINDOW process after every completed stage;
- the next stage succeeds without rerunning prior completed work;
- reconstructed hashes equal uninterrupted execution;
- missing or divergent durable evidence fails closed.

Source status (2026-10-23): `stages_v5` has a provider-free runtime composition
with `OPTIONS_REFERENCE`. The input, cycle and projection handoffs are bounded
caches only: a fresh WINDOW composition rebuilds the exact typed input
closure/receipt, detector cycle/evaluations/packages and portal projection batch
from immutable PostgreSQL rows, then checks the durable receipt hashes. One
owned-disposable exact target-9/QQQ-SPY qualification replaces the WINDOW
composition after completed input gate, cycle and projection, compares receipt,
dataset and snapshot hashes/rows and proves completed stages are not invoked
again. Divergent durable barrier evidence is refused. V5 remains disabled for
any package/bundle/profile other than the exact qualified identity.

### V3-P6 - Truthful publication

1. Wire `OPTIONS_PUBLISH` to the canonical publisher under exact package authority.
2. Keep delivery/dispatch false.
3. Preserve newer Equity entries while adding Options snapshots.
4. Retain stale-pointer fencing and rollback.
5. Remove the second outer publication operation, or rename the stage to an explicit
   handoff if owner approval rejects direct stage invocation.

Completion:

- one stage result corresponds to one release/pointer result;
- replay is idempotent;
- publication failure restores the prior pointer;
- no provider, detector, dispatch or delivery work occurs in readers.

Source status (2026-10-23): V5 `OPTIONS_PUBLISH` now requires an exact separately
prepared publication authority and invokes the canonical publisher directly.
Its typed result names the one immutable release and the equal current pointer.
The canonical transaction preserves newer unrelated entries, fences stale
cutoffs and leaves dispatch/delivery disabled; post-publication validation
failure restores the exact prior pointer. The post-E activity-ranked P8 canary
remains an explicitly separate `stages_v4` late path with its outer publication
operation. The exact owned-disposable qualification preserves the target-9
Equity snapshot, advances one cohort-local pointer, replays without a second
release and restores the prior pointer after a forced post-publish validation
failure. No stored-read, provider, delivery, broker or trading authority was
added.

### V3-P7 - Equity execution smoothness

1. Add bounded deferred handling for finalizer/checkpoint lag.
2. Preserve the same window and deadline.
3. Measure higher-interval derivation contribution to E latency.
4. If material, move non-core interval work behind the core Equity/Options data receipt
   without weakening portal/strategy gates.

Completion:

- a delayed finalizer can complete the same window;
- policy/data failures remain terminal;
- no unbounded polling or retry loop exists.

### V3-P8 - Scaling preparation

Build now:

- detector/shard ownership interfaces;
- set-oriented Equity context and Options evidence queries;
- policy-driven universe/underlyings;
- per-stage telemetry.

Defer implementation until measured:

- multiple Equity bridge shards;
- detector worker pools;
- parallel pure Options calculations;
- wider Options contract sets;
- prewritten portal snapshots with a short pointer transaction.

### V3-P9 - Close the Equity alert-result projection path

The Equity portal path is a sibling product path, not an Options prerequisite. It
must nevertheless be complete and truthful before the combined V3 source can be
called end to end.

1. Add an explicit V3 runtime owner for the Equity Stock Alerts detector/product
   workers and the retained-result projector.
2. Configure that owner from the hardened runtime environment or DSN secret; do not
   depend on an image-local `.env` file.
3. Give the canonical resident bounded access to exact `INPUT_READY`/bar evidence
   and no provider, delivery or execution authority.
4. Persist one immutable binding between the retained result generation and the exact
   Equity cohort, target window, `INPUT_READY` receipt, cutoff, input SHA and detector
   source identity.
5. Require the retained alert policies and enrollment to equal the exact Equity
   manifest membership, and require all result clocks and detector inputs to be
   causal under the bound E receipt.
6. Make `STOCK_ALERTS` block on an unavailable binding and fail on a divergent,
   ambiguous, future or stale binding.
7. Preserve the sibling behavior: an unavailable Stock Alerts result source must not
   invalidate `INPUT_READY`, the core Equity projection, Options input readiness or an
   otherwise complete strategy projection.
8. Include the detector/result owners in the V3 session lifecycle, health/status,
   cleanup and restart evidence.

Completion:

- the V3 session coordinator starts and stops the exact Equity result owners;
- a fresh process reconstructs the retained result binding from PostgreSQL;
- the Stock Alerts snapshot names the exact source binding and E receipt;
- a result from another cohort, window, receipt, cutoff, input SHA or enrollment fails;
- projector configuration contains no image-local `.env` dependency;
- projection performs zero detector or provider work;
- provider-free orchestration tests cover unavailable, stale, divergent, replay and
  successful result sources.

Source status (2026-10-28): `COMPLETE_PROVIDER_FREE`. Migration 038 is rewritten
as the canonical V3 batch/evaluation schema guarded by the exact cohort, window,
`INPUT_READY` receipt, cutoff and receipt input SHA. The resident detector loads
receipt-visible finalized 5m bars, persists detector events/state and exact
evaluation membership, and the `STOCK_ALERTS` stage reads only that canonical
batch. The V3 coordinator owns resident start, health, bounded restart and cleanup.

Market trend is computed from exact receipt-bound SPY bars. Sector trend and
event-risk context are intentionally `UNAVAILABLE` until later accepted owners
are implemented. Those outcomes appear as non-actionable portal alert rows with
reason codes; they never receive trigger/stop/target geometry. `MATCH` alone creates
an actionable alert, while `NO_MATCH` remains retained evaluation evidence.

Legacy migrations 036/037 and the intraday/swing forward workers remain a separate
legacy product and are not referenced by V3 source identity, runtime, binding or
projection. Production migration application and activation remain excluded.

### V3-P10 - Complete OS1/OS2 stored-reader identity

1. Replace legacy `S1`/`S2` literals with `OS1`/`OS2` at every accepted V3 Options
   query, detector dependency, context-availability and display boundary.
2. Keep legacy compatibility only where an explicitly versioned legacy contract
   requires it; never silently alias product identities in a V3 receipt or projection.
3. Ensure the HTTP API, compatibility repository, stored Advanced Options page,
   frontend types and filters accept the same detector set.
4. Preserve lane-local `BLOCKED` and `UNAVAILABLE` outcomes and expose their reason
   codes without reporting false zero-success results.
5. Re-run the legacy compatibility suite as well as the V3 stored-page suite so the
   identity transition does not break retained historical reads.

Completion:

- OS1 and OS2 evaluation and detector-run queries succeed through the stored portal
  API;
- unsupported or absent lanes return typed unavailable results rather than validation
  errors or HTTP 500/503 responses;
- all detector capability allowlists and context-availability maps use the accepted
  product identities;
- focused Options projection, portal, compatibility and frontend presentation tests
  pass with no `S1`/`S2` identity regression.

Source status (2026-10-27): `COMPLETE` for source implementation. Advanced stored
projection/query contracts, capability allowlists and context-availability maps use
OS1/OS2. The shared behavior-review boundary accepts OS1/OS2 while preserving the
explicit legacy S1/S2 dataset identity; it selects one five-detector family per
dataset rather than merging legacy and Advanced identities. The full focused
option-flow suite passes (`148 passed`). Production stored-read activation and
live-market qualification remain separately gated and are intentionally excluded.

## 7. Option Identification Acceptance Matrix

| Layer | Required V3 proof |
| --- | --- |
| Admission | Exact catalog/slot/channel membership; no wildcard subscription |
| Condition semantics | Included/excluded/cancel/unknown behavior is versioned and replay-stable |
| Factual volume | Every eligible print updates cumulative volume before retention |
| Event retention | Versioned `contracts OR premium` rule with tier/DTE/profile evidence |
| Aggression | Causal NBBO, stored class/confidence and quote identity |
| Opening inference | Prior OI, complete volume, quote, conditions, premium and no affected gap |
| Block | One large accepted 5-60 DTE print; no cluster required |
| Sweep | Exact 5-21 DTE members, five-second window, prints/exchanges/premium and directional consistency |
| Accumulation | Exact 180/900s members, exchanges, premium, cumulative contracts and completeness |
| Ultra-short | Exact 0-2 DTE high-threshold evidence; no direction from inside-spread prints |
| Candidate caps | Global/per-underlyer/profile caps and deduplication are deterministic |
| Equity context | Each Options lane binds only its exact E bars/features; no Equity detector-event prerequisite |
| Detector isolation | Equity S1/S2 and Options lanes can complete, block and retry independently |
| Availability scope | Missing lane-specific context yields a typed lane outcome, not a global input-gate failure |
| Detector | Cumulative/contextual filters consume retained evidence without rewriting it |
| Package | Exact ordered legs, marks, payoff, collateral, clocks and source IDs |
| Capability | Observation, indicative, quote-paper and execution remain distinct |

## 8. Focused Qualification

### Provider-free source tests

- policy/hash round trips;
- H/E/O clock boundaries;
- exact target-9 and QQQ/SPY scope;
- exact E materialized bar/feature context loading;
- Equity S1/S2 and Options detector independence;
- factual-volume-before-retention;
- U/B/S/A routing;
- aggression and opening inference;
- cluster finality and late/cancel revisions;
- native/package evidence binding;
- stage rehydration;
- deferred versus terminal outcomes.

### Owned disposable PostgreSQL

- real Equity materialization to Options context/native consumer;
- absent, delayed, complete-zero and failed Equity S1/S2 outcomes do not suppress
  otherwise complete Options lanes;
- Options cycle failure does not alter Equity detector events or checkpoints;
- two Options windows in one cohort with different E receipts;
- exact migration grants and immutability;
- resident completion barrier;
- reference/input/cycle/projection/publication;
- restart after every durable stage;
- rollback after enrollment, input, cycle, projection and publication;
- empty owned database/role/file/process/lock cleanup.

### Isolated process/container

- concurrent Equity AM and Options T/Q capture;
- subscription ready before the exact opening capture boundary;
- bounded queues and disk;
- one Options socket owner;
- resident terminal failure propagated immediately;
- no filesystem-only correctness authority;
- exact stop deadlines and no automatic broad restart.

### Market-hours evidence

A live shadow must state separately:

- provider request/socket effects;
- opening capture completeness;
- 5s/180s/900s cluster coverage;
- E and O cutoffs/age;
- final checkpoint offsets;
- flow candidates before/after caps;
- packages selected/zero/blocked/unavailable;
- publication and cleanup results.

## 9. Performance And Scaling Gates

Measure per stage:

- elapsed and database time;
- query count and rows/bytes read;
- transaction duration and retry count;
- journal/consumer lag;
- queue depth and backlog;
- events/clusters/packages retained;
- snapshot/release row and byte counts.

Initial principles:

- raw append never waits on projection;
- no transaction sleeps;
- no per-instrument transaction loop on the long-term scaling path;
- success-path validation reads each immutable stage result once;
- restart may pay the reconstruction cost;
- parallelize only pure work after one input snapshot is frozen;
- do not tune thresholds to achieve a desired alert count.

## 10. Operational Run Sequence

1. Freeze H, prior OI/Options reference and the exact pre-session contract admission.
2. Create the non-serving session acquisition receipt and authorize/start Equity AM
   and Options T/Q acquisition independently.
3. Capture opening Options evidence while Equity builds E.
4. Complete target-9 E and freeze the exact policy-required QQQ/SPY bar/feature
   identity. Equity S1/S2 continue independently.
5. Create the exact H/E/O window cycle authorization with O no more than 300 seconds
   after E.
6. Complete resident checkpoints and target-window E binding.
7. Run input/cycle/projection and separately granted publication stages using the
   already-frozen Options reference.
8. Stop owned residents and verify cleanup.
9. Retain immutable reports; never reuse an expired or consumed package.

## 11. Definition Of Done

V3 source is not complete until:

- all V2 findings are resolved without synthetic evidence;
- option-flow/package contracts are preserved and linked to their real owners;
- Equity detectors and Options detectors remain independent sibling consumers of E;
- reused stock-analysis algorithms have distinct product policy/input/evaluation
  identities;
- pre-open session acquisition and later E-bound cycle authorization have separate
  immutable scope and consumption receipts;
- opening slot admission is prospective and never claims coverage before subscription;
- opening-flow limitations are explicit;
- transient lag no longer terminally blocks a valid same-window attempt;
- every stage restarts from durable evidence;
- target-9 provider-free daily execution is repeatable and cleanup is empty;
- scaling telemetry supports a measured next target decision;
- the V3-owned Equity result producer/projector is lifecycle-managed and every Stock
  Alerts projection is bound to its exact Equity `INPUT_READY` receipt;
- OS1/OS2 identities are consistent across cycle evidence, projection, stored readers,
  API contracts and portal presentation;
- no production authority is implied by source or disposable qualification.

Provider-free integration checkpoint (2026-10-23):

- retained report
  `backend/.realtime-canary/advanced-options-v3-provider-free-qualification-8a6a937e40583860.json`
  is `PASS`, semantic SHA
  `8a6a937e40583860386517a4573a3f6d0c697f6b2581a1d3e3b0f5b6ed1c04ee`
  and file SHA
  `0fc8d176cf493e94119c32be244072c27087b0d4185ad719d149f64f0d9bc014`;
- 103 focused provider-free package, authority, barrier, restart, publication,
  Docker, role and V4-compatibility tests passed;
- the exact V3 owned-disposable qualification plus split-authority, V5
  publication/restart and separate V4 late-canary qualifications passed four
  disposable database tests with empty database/role/file/process/lock cleanup;
- terminal resident failure, incomplete resident defer, immutable divergence,
  authority consumption and restart-budget exhaustion are fail-closed;
- the Compose profile is inert and disabled by default, reuses exactly four
  roles, requires an immutable image identity and retains `--no-build`.

This is not market readiness. Remaining blockers are read-only production
source/config/overlap/owner/retention evidence, an approved immutable image
already present on the Docker host, an exact prospective live package with
separate session/window/publication authorities, market-hours opening and
900-second capture evidence, and a separately approved stored-read pin/flag
change. No provider call, production DDL/write, production lifecycle action,
image deployment, read enablement, retention action, delivery, trade or
nine-underlying Options expansion occurred.

Pre-market operations preparation (2026-10-23) reaches
`GO_PROVIDER_AUTHORITY_REQUIRED`.
The retained production preflight
`backend/.realtime-canary/advanced-options-v3-operations-preflight-20261023-go2.json`
has semantic SHA
`12edc9ca3d0bb7fdf9783cd1f683afa6611b76bf78746a27a47010ca2119fee9`
and file SHA
`d296e0dc4bd9d305a9b8db672cb4ca7d63a2107628f935ce88c8a7f52cbead41`.
The compact gap and non-executed repair plans are `GAP.json` and
`REPAIR-PLAN.json` in
`backend/.realtime-canary/advanced-options-v3-operations-package-20261023-final`;
their semantic SHAs are
`35fddca1bc2f0c2f88e707343df0f93ab42527b21c34cfc0b946268179a35ea3`
and
`f932bf80aa18bae47bca44c2ca5f24aa7f5936cb0bac92423e5e5cc5441ac22e`.

The bounded repair completed four exact adjusted H session requests, one dated
QQQ identity request, four ticker-scoped corporate-action requests, one
Treasury request and sealed QQQ/SPY snapshot capture/replay. It made no raw H,
fundamental, next-session catalog, replay, delivery or trading request. The
stored admission scope is four contracts: one call and one put for each
underlyer.

The production handoff adapter is provider-free qualified. It deliberately
uses one immutable session package/bundle rather than operator-managed bundle
replacement:

1. `prepare` persists the exact Options acquisition cohort, enrollment,
   four-contract `PRE_SESSION` slot set and session acquisition authority. It
   refuses any pre-existing window-cycle authority.
2. `start-residents` starts only INGRESS, MARKET_STATE and FLOW from that
   bundle before open. They remain session-scoped, continuously advance
   durable raw/normalized/consumer offsets and freeze exact per-window
   three-member barriers without being recreated.
3. `authorize-window` runs only after the exact target-9 `INPUT_READY` receipt
   and typed E binding exist. It deterministically derives `target_window_id`
   from session/cohort/window-key/O, enforces `E <= O <= E+300s`, persists that
   binding and cycle authority, and leaves stored reads disabled.
4. `run-window` starts one session-scoped WINDOW owner. It handles successive
   authorized claims, exact resident barriers, bounded deferred retries,
   restart rehydration, lane-local terminal outcomes and idempotent canonical
   publication. A failed/missed window cannot suppress a later deterministic
   window.
5. `status` derives operator state from existing aggregate lifecycle, stage,
   resident, receipt and pointer relations. Session close seals ingress, drains
   both consumers and proves one final barrier. No status relation is added.
   Stored-read activation remains a separate post-publication action.

The focused provider-free report is `PASS` with semantic SHA
`fde459d6f02deace7075ab3e8e734f9a9f92c744e37200eb9ef36a7d2acb6654`
and file SHA
`19c3cb188fbc71e54e4f3f1aba4d36e3aed3c16719fa0fddb20ded1db4226976`.
The owned Docker handoff used local image
`sha256:2f4e9cd40227c971e2d558e52842f9ea03f7ca7183905530f1633dd7090acc9c`
and bundle SHA
`0a66b46cfc9a5ca2b577b5a7beadcb506c28fa431968c686307e4ebe1da1b619`.
Starting WINDOW preserved all three resident container IDs across three
windows; resident offsets were strictly monotonic, window 2 deferred then
completed and window 3 kept O1 while recording O3 blocked. Cleanup was empty.
Its semantic SHA is
`264339ab73696ffd58ed2f3c224c57211f04226b59187f8f42aaeeb7b0facc3e`
and file SHA is
`dc893f199e5b4ff237df6b50d03014223b5dae67b46f95422cbf158ccbb9d4de`.

Migrations, security, stale-record disposition, target-9
profile/cohort/ingress plan and H/reference facts remain ready. This does not
create a live package or authorize provider, production lifecycle,
publication, stored-read, delivery or trading effects.
