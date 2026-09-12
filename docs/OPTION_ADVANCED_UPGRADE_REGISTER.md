# Option Platform: Advanced Upgrade Decision Register

Status: living register of deferred decisions

As of: 2026-09-08

Related documents:

- [Option Chain Scanner Design](OPTION_CHAIN_SCANNER_DESIGN.md) — normative build spec
- [Option Pipeline Current State](OPTION_PIPELINE_CURRENT_STATE.md) — code-derived behaviour
- [Option Research Design](OPTION_RESEARCH_DESIGN.md) — research methodology

-  https://unusualwhales.com/pricing


## 1. Why this document exists

The platform runs on Polygon Options **Developer**: 15-minute delayed aggregates, no
option quotes, no NBBO, no trade aggressor side. Many decisions were therefore made
*conditionally* — a threshold was set to a placeholder, a gate was left reporting
`UNAVAILABLE`, or a strategy was deferred rather than tuned.

Those decisions are scattered across code, policy files and reason codes. When the
Advanced entitlement and a real-time underlying feed arrive, someone needs a single
list of what can be switched on, what must be re-measured, and what changes identity.
That list is this document.

Two rules govern every row below:

1. **Nothing flips on automatically.** Each item names the flag, policy field or code
   path that must change deliberately.
2. **Identity changes are called out.** Some switches alter a policy hash, which the
   repository guard requires to be paired with a version bump. Those are marked
   **IDENTITY** and will retire the active candidate cohort.

## 2. Entitlement boundary

| Capability | Developer today | Advanced target |
|---|---|---|
| Option aggregates | 15-minute delayed day bars | Real-time |
| Option quotes / NBBO | **None** | Real-time bid/ask/size |
| Option trades | Historical REST, delayed | Real-time WebSocket `T.*` |
| Underlying price | Delayed minute bars | Real-time (separate Stocks entitlement) |
| Greeks / IV | Computed locally | Computed locally (unchanged — see §3.1) |
| Historical aggregates (day + minute) | 4 years, REST and flat files | All history |
| Historical option trades (with condition codes) | 4 years, REST and flat files | All history |
| Historical NBBO quotes | **None** | All history from 2022-03-07 |
| Contract reference with `as_of` | All history (back to 2014-06-02) | All history |
| **Historical open interest** | **None** | **None — see §3.13** |

Provider note: Polygon now trades as **massive.com**; `polygon.io` documentation URLs
redirect. Plan tables cited below are from the per-endpoint docs, not the pricing page.


## 3. Decisions to revisit

### 3.1 Greeks and implied volatility — decided, do not change

**Decision:** internal Black-Scholes stays authoritative. Polygon's real-time Greeks and
IV become a diagnostic and drift alarm only.

**Rationale:** backtest/live parity; vendor methodology is unversioned and would break
immutable replay and policy hashing; Greeks must be consistent with the mark actually
traded; solver cost is negligible at 13-underlyer scale.

**What changes with Advanced:** the *input*, not the model. Recompute local IV from the
NBBO midpoint instead of the aligned day close. That removes the
`maximum_developer_source_age_seconds` (1800) and `PARTIAL_MARK_ALIGNMENT` constraints
that currently suppress most of the chain.

Code: [backend/options/analytics/greeks.py](../backend/options/analytics/greeks.py),
[backend/options/analytics/marks.py](../backend/options/analytics/marks.py).

### 3.2 Valuation policy seam — **implemented for Developer**

`model_mark` selection and outcome valuation now use one immutable `ValuationPolicy`
artifact with a policy hash and `mark_source` provenance, so
`DEVELOPER_ALIGNED_AGG_CLOSE` and `ADVANCED_NBBO_MIDPOINT` are interchangeable and can
run side by side for shadow comparison.

The first policy is `option_valuation_v1`, hash `86a5db37…c7cd`. It preserves current
Developer behavior: a positive aligned aggregate close, maximum source age 1,800 seconds,
maximum option/spot skew 60 seconds, $0.01 intrinsic-price tolerance, $0.65 commission per
contract per side, and unavailable slippage. Normalized snapshots and candidate legs persist
the valuation version/hash. Outcome queues require both entry legs and later exit snapshots
to match that hash and use an allowed source; mixed or legacy null-provenance packages are
not reinterpreted.

The effective market-data policy hash now combines the Developer policy hash and valuation
policy hash (`7c5f8e0d…40fd`). **This intentionally starts a new matrix lineage even though v1
preserves numeric behavior.** Historical rows remain immutable.

Migration 029 also includes `configuration_sha256` in Board publication uniqueness and current
Board reads. The phase2_v4 revision `7f1ae6e2…` remains valid historical evidence under its
original configuration, but it is not advertised as the current Board after the valuation-policy
cutover. A new current Board will publish only after one complete 13-underlying cycle under
configuration `c93e8186…`.

**Serving behavior during that cold start:** current-policy data is always preferred. Until it
exists, Flow, Activity, Research, Candidate Audit, Screener and Opportunity Board may serve the
latest coherent prior-policy cohort only with `serving_mode=HISTORICAL_PREVIOUS_POLICY`; every
portal surface renders an explicit warning. This avoids both a broken empty workspace and silent
cross-policy mixing.

This fallback was validated across all option routes on desktop and 390px mobile with no failed
requests, console errors or document overflow. Migration 029 now removes the exact legacy
three-column Board uniqueness and leaves only the configuration-aware constraint. Board gate
completeness is scoped to `gate_ledger_v2`, so a future ledger revision cannot invalidate older
candidate graphs by making the raw gate-row count exceed six.

Still Advanced-gated: the `ADVANCED_NBBO_MIDPOINT` implementation, side-aware executable
marks/slippage, quote freshness/spread verdicts, and dual-policy shadow comparison.

### 3.3 Execution gate ledger — **persisted and surfaced**

Candidates previously received two hard-coded strings from `_capability_reasons`
(`QUOTE_LIQUIDITY_NOT_AVAILABLE`, `PAPER_RISK_ENGINE_NOT_IMPLEMENTED`) with
`execution_eligibility` hard-coded to `None`. That recorded *that* a candidate was
blocked but not which checks ran, which passed, or which could not be evaluated.

[backend/options/strategies/gates.py](../backend/options/strategies/gates.py) replaces
that with named gates — `CANDIDATE_KIND`, `STRATEGY_CONTEXT`, `EQUITY_DIRECTION`,
`QUOTE_LIQUIDITY`, `RISK_ENGINE`, `READ_ONLY_MODE` — each reporting `PASS`, `FAIL` or
`UNAVAILABLE`, versioned as `gate_ledger_v2`.

**`UNAVAILABLE` is deliberately distinct from `FAIL`.** Unavailable means the check could
not run on this entitlement; failed means it ran and the candidate lost. That distinction
is what lets the Advanced upgrade flip `QUOTE_LIQUIDITY` to a real verdict without
touching a single selector.

Eligibility is now *derived* — `LIVE_CANDIDATE` only when no gate blocks — rather than
asserted. Current state:

```
CANDIDATE_KIND   PASS
STRATEGY_CONTEXT PASS
EQUITY_DIRECTION PASS
QUOTE_LIQUIDITY  UNAVAILABLE  QUOTE_LIQUIDITY_NOT_AVAILABLE
RISK_ENGINE      UNAVAILABLE  PAPER_RISK_ENGINE_NOT_IMPLEMENTED
READ_ONLY_MODE   FAIL         READ_ONLY_RESEARCH
=> eligibility None
```

With quotes, a risk engine, and read-only disabled the same ledger yields
`LIVE_CANDIDATE` with zero blocking gates. That is the intended switch-on path.

Migration 025 adds `option_candidate_execution_gates`, one immutable row per
`(candidate_id, ledger_version, gate_name)`. The engine carries the six evaluated verdicts
through `StrategyScanResult`; the strategy repository writes them in the same transaction
as the candidate. The candidate detail API and Decisions drawer show all verdicts and
reason codes. Older candidates without ledger rows display "not recorded" rather than
reconstructing history under today's capabilities.

**Still to build:** the paper `ExecutionManager` and order/fill/position ledger from
design §13. The first post-purge candidate must also verify the migration 025 foreign-key
path and UI payload against real persisted data.

### 3.4 Quote-dependent gates — blocked, designed

| Gate | Current | With Advanced |
|---|---|---|
| Per-leg quote spread ceiling | Not evaluated | Reject legs above a policy spread fraction |
| Per-leg quoted size floor | Not evaluated | Require size ≥ intended contracts |
| Package marketability | Not evaluated | All legs quotable at one coherent watermark |
| Fill simulation | Next complete snapshot mid | Side-aware bid/ask with explicit slippage |
| Realized P&L | `RESEARCH_DELAYED_PROXY` label | Quote-backed simulated close, then real fills |

Until then every outcome remains labelled `RESEARCH_DELAYED_PROXY` and must never be
presented as executable P&L.

### 3.5 Trade aggressor side and sweep classification

`SWEEP_LIKE_CLUSTER` deliberately records `aggressor_side = None` and
`institutional_owner = None`. Classifying a print as buyer- or seller-initiated requires
the NBBO **at trade time**, which Developer cannot supply.

With Advanced: pair each print against the contemporaneous NBBO, classify at-ask versus
at-bid, and only then may the detector claim directional intent. The current name
("sweep-like") is deliberately hedged and should stay hedged until this lands.

Note that this does **not** require a live subscription to research: historical option
trades with condition codes are already available on Developer (4 years), and the quote
flat files needed to classify them reach back to 2022-03-07 on Advanced. See §3.14.

**IDENTITY** — adding aggressor classification changes selection semantics.

### 3.6 Trade ingestion tuning — **live, opt-in**

Trade ingestion is now wired into the cycle but ships **disabled**:

| Setting | Default | Note |
|---|---|---|
| `OPTION_TRADE_INGESTION_ENABLED` | `false` | Enabling multiplies provider requests per cycle |
| `OPTION_TRADE_WATCHLIST_PER_UNDERLYER` | `15` | Bounds cost; watchlist is OTM calls with observed volume, ranked by day volume |
| `OPTION_TRADE_LOOKBACK_SECONDS` | `3600` | Window requested when no cursor exists |
| `OPTION_TRADE_INGESTION_BUDGET_SECONDS` | `120` | Stops remaining optional pulls for one underlying after the elapsed budget |

These are deliberately **excluded from `fingerprint_payload`**, so tuning them does not
disturb any persisted hash. That is a pragmatic choice, not a correct one: the watchlist
selection rule determines which sweeps are detectable at all, so it **must be moved into
a versioned policy before any measured study depends on sweep detections**.

With Advanced this REST polling path is replaced by the WebSocket `T.*` stream, and the
per-contract cursor becomes a stream sequence watermark.

Enabled in the local `.env` as of 2026-09-05 so the first post-fix session populates
`option_trade_events`; `.env.example` still ships it disabled.

**Timeout resilience 2026-09-10.** Option-trade requests inherit the shared transport's
two transient retries and 30-second timeout. Exhaustion is wrapped as `OptionProviderError`,
reason-coded `TRADE_FETCH_FAILED:OptionProviderError`, and caught per contract; the matrix
continues and the failed contract cursor does not advance, so the next cycle re-requests the
overlap. A 120-second per-underlying budget now stops launching additional optional watchlist
requests after repeated slow failures and records `TRADE_INGESTION_BUDGET_EXCEEDED`. Both knobs
remain operational and outside strategy/configuration identity.

**Validated 2026-09-08.** `massive_options_conditions_v1` classifies official provider
conditions fail-closed: consolidated-volume-eligible conditions are `INCLUDED`, canceled
and non-volume conditions are excluded, and unknown/corrected prints remain `UNKNOWN`.
Aggressor side stays explicitly unavailable. Classification status, reasons and version
persist in PostgreSQL and Parquet schema v2. The current 15-contract × 13-underlyer
watchlist produces 195 distinct REST batches per slot and completed in under four minutes,
inside the 15-minute cadence. This is operationally viable on Developer, but it is roughly
225,000 raw trade rows per slot and must remain a measured capacity input.

### 3.6.1 Execution lag guard — live

A run further behind its slot than `OPTION_MAXIMUM_EXECUTION_LAG_SECONDS` (default 1800)
adds `EXECUTION_LAG_EXCEEDED` and degrades the chain. Because strategies gate on an
*exactly* `COMPLETE` chain, a late run now suppresses with a named reason rather than
selecting from increasingly stale mark/bar pairings.

The default equals `maximum_developer_source_age_seconds`: beyond that point
`_fresh_mark_window` starts dropping otherwise-valid marks, so the two limits describe the
same boundary. A punctual worker run lands near 930s (900s provider delay + 30s grace).

Unlike the trade-ingestion knobs this setting **is** part of `fingerprint_payload`, since
it changes analysis semantics rather than only operational cost.

### 3.7 Gamma exposure

Chain-level gamma profiles are live and persisted per slot across four scopes. Two
deferred decisions:

- **Dealer sign convention** is an unmeasurable assumption. Facts are stored unsigned
  (call and put gamma separately); the sign is applied by
  [gamma_policy_v1.json](../backend/options/policies/gamma_policy_v1.json) with
  ETF → dealers long calls, single stock → dealers short calls. Once outcome data
  exists, **measure which convention predicts** and revise the policy.
- **Wall gates** (`require_gamma_wall`) ship disabled in v1 and enabled in
  `gamma_policy_v2_walls.json`. **IDENTITY** — enabling folds the gamma hash into
  strategy identity, which the repository guard requires to be paired with a
  `strategy_version` bump.

Open interest is prior-session settled, so intraday gamma is stale under any entitlement.
Advanced does not fix this; only OCC intraday data would. Advanced also does not supply
*historical* open interest, which means none of this can be backtested — see §3.13 for the
evidence and the three available responses.

### 3.8 Volatility smile — measured, deprioritised

The persistence study ([option_smile_persistence.json](option_smile_persistence.json))
found that at 15-minute cadence smile distortions are not tradeable: 91.5% of detections
cannot be re-observed one slot later, median |z| retains 40% after one slot, and the
residual sign is a coin flip by lag 2.

Two things to revisit with real-time data:

1. **Cadence.** The relevant horizon for a market-maker-arbitraged kink is seconds. Re-run
   the same reporter against Advanced-cadence matrices before writing off the strategy.
2. **Threshold comparability.** `minimum_absolute_robust_z = 2.5` is not comparable across
   expirations — the same distortion scores 1.64 with 9 strikes and 8.51 with 25, because
   the unweighted fit and MAD are less contaminated as n grows. Confirmed on real data:
   detection rate rises monotonically with group width. Any tuning must normalise for
   strike count first. **IDENTITY** if the threshold or normalisation changes.

Also note `MINIMUM_RESIDUAL_MAD = 1e-9` in
[smile.py](../backend/options/analytics/smile.py): a numerical-stability floor added
after the study found that a near-exact fit produced robust scores above the detection
gate from floating-point noise alone.

### 3.9 Option-to-spot clock alignment — **open defect, blocks measurement**

Measured 2026-09-05 across 116,403 persisted snapshots:

- Only **19.6%** carry a `model_mark`. Of the rest, `OPTION_SPOT_SKEW` accounts for
  **92,656** rejections; `BELOW_INTRINSIC_MARK` 933 and `STALE_MARK` 19 are negligible.
- Signed skew (`mark_market_data_time - spot_market_data_time`) is **positive on
  116,403 of 116,403 rows** — never zero, never negative.
- Percentiles: p10 2.7s, median 603s, **p90 903s** — and 903s is the 900s provider delay.
- Skew *worsens* with liquidity: median 302s at 1–99 day volume (36.3% within the 60s
  gate) versus 843s at 1000+ volume (**4.2%** within the gate). That is the opposite of
  what a stale-last-trade explanation predicts.
- Per batch the newest underlying bar is 19:46:00 while the newest option mark is
  20:01:03.185, whose fractional part tracks the request timestamp.

**Confirmed 2026-09-05** by [probe_option_mark_clock.py](../backend/scripts/probe_option_mark_clock.py):

```
SPY, 25 contracts: day.last_updated - last_trade.sip
                   median 931.6s, min 902.4s, max 958.8s
QQQ, 20 contracts: median 915.1s, min 909.6s, max 939.9s
19 distinct day.last_updated values across 25 SPY contracts
```

`day.last_updated` is **neither** a publication clock nor a market-event clock. It tracks
each contract's own last trade — the values are contract-specific, not clustered at the
probe time — but shifted forward by a constant ~900 seconds, the entitlement delay. In
other words it is a *delayed-release stamp*:

```
day.last_updated  ~=  last_trade.sip_timestamp + 900s
```

Underlying minute bars, by contrast, carry true market timestamps. Normalization uses
`day.last_updated` as `mark_market_data_time`, so every option mark is stamped ~15
minutes in the future relative to the spot clock it is then aligned against.

**The consequence is worse than lost coverage — it is adverse selection.** A contract
only passes the 60-second gate when its *real* last trade happened ~15 minutes before the
newest available bar, because only then does the inflated stamp land near that bar. The
pass rate therefore falls as liquidity rises: 36.3% at 1–99 day volume, 18.2% at 100–999,
**4.2% at 1000+**. The surviving model-valid population is roughly 56% low-volume, 38%
mid, and only 5% high-volume. The pipeline is systematically retaining the *stalest*
option marks and discarding the freshest and most tradeable ones.

**Fix applied 2026-09-05.** `parse_polygon_snapshot` now prefers
`last_trade.sip_timestamp` and falls back to `day.last_updated`, flagging fallback rows
with `MARK_TIME_FROM_RELEASE_STAMP` so their share stays measurable. For a delayed day
aggregate `day.close` *is* the last trade price, so the SIP timestamp is its true market
time; the previous preference order was inverted.

This also repairs bar alignment. `_fresh_mark_window` derives the underlying bar-fetch
window from the actual option mark times rather than from `cycle_time`. With inflated
stamps the window ran up to roughly *now*, but the delayed underlying feed only carries
bars to *now − 900s*, so the newest bar was permanently ~900s behind the newest mark.
With true trade times the newest mark sits at *now − 900s*, which the delayed bar feed
does cover.

**Do not** relax `maximum_option_spot_skew_seconds` as a workaround. That would pair a
mark with a spot up to 15 minutes stale and silently corrupt every IV, Greek and payoff
downstream — and it would not correct the liquidity bias.

**IDENTITY** — the change alters every model mark, local IV and Greek, and therefore
every candidate. The existing cohort is superseded rather than comparable.

**Still to verify:** re-run the probe and a live cycle during market hours to measure the
new model-valid rate and confirm the population shifts toward liquid contracts.

**Consequence for measurement:** package coherence (all legs repriceable at one later
watermark) was 4% at 30 minutes and 121 of 3,028 selected candidates before the fix, and
that cohort was liquidity-biased. S4 should restart from post-fix data.

### 3.10 Point-in-time inputs

| Input | Current | Needed |
|---|---|---|
| Risk-free rate | Dated U.S. Treasury par-yield curve active | Continue resident daily refresh |
| Dividend yield | Coverage-backed discrete cash flows active | Add American early-assignment treatment |
| Event calendar | `public_calendar_v1` active | Finnhub earnings plus official Fed dates, point-in-time |
| Valuation model | European Black-Scholes | American / dividend-aware for ITM puts |

None of these are Advanced-gated. The first three inputs are implemented with direct
provenance and fail-closed coverage. The remaining American-model change is a future
**IDENTITY** change to the market-data policy.

**Event-calendar integration completed and validated 2026-09-11.** Migration 030 adds append-only,
point-in-time coverage facts alongside the existing event ledger. The controlled JSON importer
validates event scope, timestamps, immutable identities and source-key uniqueness before writes.
Strategy context now requires the configured source to cover the complete one-day-prior through
72-hour-forward blackout window before absence can mean `CLEAR`; otherwise earnings/Fed remains
`UNAVAILABLE`. `public_calendar_v1` combines Finnhub earnings with the official Federal Reserve
FOMC calendar. The durable observation contains 350 company earnings-coverage facts, one global
Fed coverage fact, 106 earnings events and one FOMC event. All 13 option contexts rebuilt against
that observation: ten stocks are `CLEAR/CLEAR`, three ETFs are `NOT_APPLICABLE/CLEAR`, and all
13 carry direct coverage lineage (23 links). No event link is expected until an event enters the
decision blackout window.

Migrations 032 and 034 complete causal lineage: source-declared publication time is separate
from actual platform receipt, and every new strategy context links the exact event revisions
and coverage revisions used in its decision. Active and canceled events explain `BLOCKED` or
`CLEAR`; coverage links prove when absence could safely mean clear. Candidate detail exposes
the linked source keys, scheduled/coverage windows, source times and receipt times. Source facts
survive derived-layer purges; context links are rebuilt with the derived graph.

Coverage expires after 12 hours, so a stopped manual calendar worker cannot leave false `CLEAR`
states indefinitely. The worker remains an explicit `calendar` profile until a later real
Finnhub revision/removal observation validates the reconciliation behavior outside fixtures.

### 3.11 IV rank and percentile

`iv_regime` remains hard-coded `None` with reason
`INSUFFICIENT_COMPLETED_SESSION_HISTORY` until materialization passes. The persistence
schema, pure rank/percentile analytics, provenance-complete repository, dry-run materializer,
strict acceptance artifact and dynamic health readiness are implemented. Candidate evidence
is intentionally not wired yet.

Not Advanced-gated. The source is policy-aligned settlement marks plus a current complete
intraday matrix, not `option_expiration_analytics` alone.

**Readiness audited 2026-09-11.** The historical option aggregate path was requesting
`adjusted=true` despite pairing marks with nominal strikes and unadjusted underlying closes.
Future fetches now use `adjusted=false`. `option_settlement_valuation_v1`, hash
`e88adb1a…8d51`, defines the separate settlement source and requires a 252-session lookback,
at least 200 sessions and 80% coverage. Migration 031 persists adjustment mode and settlement
valuation identity without rewriting old facts.

The read-only artifact `docs/option_iv_context_readiness.json` reports settlement-mark
readiness separately from IV-context readiness. Policy reconstruction is complete: 124,052
marks cover all 13 underlyings, with 248-251 marked sessions in the exact latest 252-session
window. Dated Treasury inputs and historical/live dividend coverage also pass. The only
readiness blocker is `COMPARABLE_IV_SERIES_NOT_MATERIALIZED`.

Migration 033 adds immutable `option_daily_contract_mark_revisions` keyed by contract,
settlement session and valuation-policy hash. Corrected unadjusted marks can now coexist with
legacy adjusted facts, repeated writes are idempotent, payload conflicts fail, and backfill
counts report actual inserted revisions instead of attempted rows.

**Measured remaining dependency:** the first strict dry run built 39 records with zero inserts,
but matched-IV sample coverage was only 14-43 sessions for 7D, 7-83 for 21D and 0-41 for
45D. Sensitivity at 6% and 10% ATM bands still produced zero ready contexts, proving this is
not fixed by relabeling farther strikes as ATM. Historical admission had monthly expirations
only and sampled contracts around the future expiry close. It now supports additive weekly
expirations and balanced calls/puts around contemporaneous 7/21/45-day spot anchors. Backfill
only those new contracts, then rerun the dry-run acceptance artifact.

The live Developer collection horizon is now 60 calendar DTE. The prior 45-DTE ceiling could
not bracket a 45-day target on any of 13 latest complete matrices. This is an intentional new
market-policy identity; settlement policy `e88adb1a...8d51` and its 124,052 marks are unchanged.

Migration 032 separates source-declared event time from system receipt time. The importer stamps
actual receipt as `first_observed_at`; source time is retained separately and can no longer make
coverage or events retroactively visible to an earlier decision.

### 3.12 Read model and measurement

`/api/options/candidates` and `/opportunities` rebuild "latest matrix per underlyer" with
CTEs and window functions across multiple joins on every request, including into the
partitioned `option_chain_snapshots`. A correlated prior-contract exclusion originally
made `/opportunities` take 107 seconds after the live cohort grew. It now materializes the
candidate-to-contract set once, resolves each contract's first selected matrix set-wise,
and returns the same first-occurrence board in 0.48 seconds on the full 13-underlying
cohort. The equity module's atomically republished current projections remain the target;
options still has no all-universe publication equivalent.

Index choices for `option_gamma_profiles` were validated only at n=1 rows and are
therefore unproven. `OptionGammaProfileRepository.explain(query_name, params)` exists
precisely so every catalogued read path can be re-measured at volume when the projection
layer lands.

The current Opportunity Board reads the latest committed matrix independently for each
underlying. Strategy persistence commits per underlying, so reads continue during worker
execution but may briefly combine newly processed underlyings with the preceding slot.
The main board query has no polling interval; reloading or remounting it reads the latest
committed slate, while health and performance history poll every 60 seconds. This is an
acceptable read-only research limitation, not an execution contract.

Before an Advanced-backed board can become execution-facing, replace this read path with:

1. an atomically published all-universe board revision or equivalent immutable publication
   identity;
2. a materialized current-board projection so clients do not rerun the ranking joins;
3. event-driven invalidation (WebSocket/SSE) or lightweight revision polling, with manual
   refresh as a fallback;
4. explicit `last published` and background-update states that retain the previous complete
   slate while the next revision is built; and
5. a gate that prevents mixed-revision underlyings from entering any execution consumer.

Advanced entitlement alone must not enable any of these behaviours automatically.

**Performance cohort mismatch confirmed 2026-09-10.** The live Board applies the active
strategy-policy hash, causal leg timestamps, each underlying's latest matrix, prior-contract
exclusion, and then reranks the survivors. The former `OPPORTUNITY_BOARD` performance cohort
did none of that: it selected the lowest raw `candidate_rank` within each matrix/strategy/kind
and followed the first occurrence of that signal. Therefore it did not identify candidates
actually displayed on the Board. In a live check, the Board had two structured survivors at
raw ranks 2 and 4, while the reconstructed 14-day cohort held 1,446 raw rank-leading events.

The API and UI now call this cohort `RANK_LEADERS`, with entry basis
`FIRST_RAW_RANK_LEADER_OCCURRENCE`, `board_membership_exact=false`, and an explicit definition.
Legacy requests for `OPPORTUNITY_BOARD` are accepted only as a backward-compatible alias and
normalize to `RANK_LEADERS`; they do not restore the incorrect claim. Existing P&L remains useful
for raw selector research, but it cannot license Opportunity Board admission or presentation.

Exact historical Board membership cannot be reconstructed as an audit fact after the fact.
Migration 026 should persist an immutable all-universe Board publication header and ordered
candidate membership rows, including selector/policy hashes and source matrix IDs. Exact Board
performance begins prospectively from the first such publication; the current cohort must never
be relabeled or backfilled as though it had been published.

**Implemented 2026-09-10.** Migration 026 is applied. `option_board_publications` requires
a complete configured universe and records source matrices, selector/configuration/strategy
hashes, market/observation/publication times, and funnel evidence. `option_board_members`
records positions 1–3 per underlying/strategy/kind plus raw rank and selection evidence.
`OptionBoardPublicationRepository` owns both the versioned selector and atomic inserts;
`ManualOptionPipeline` invokes it only for full configured-universe cycles. Controlled
underlying subsets never publish. `/opportunities` reads only the latest complete publication,
and `BOARD_PUBLICATIONS` performance reads exact position-one memberships prospectively.

The active market-hours worker was started before this code and therefore cannot execute the
new hook until restart. No historical publication was fabricated; the first live publication
is deliberately pending the next worker restart/complete cycle. The recovery CLI
`publish_option_board.py` is dry-run by default and can publish the newest already-complete
configured-universe cycle with `--apply`.

**Selector optimization to measure next, not change in v1:** v1 preserves the previous global
prior-contract exclusion exactly. Any contract used by any earlier selected candidate under the
active strategy policy excludes every later candidate containing that contract, even across
strategies. This can starve the Board and promote deep raw ranks. Use immutable publication
funnel evidence to compare pre-registered shadow variants:

1. same-strategy contract episode with one- or two-slot disappearance grace (preferred prior);
2. same-session global contract exclusion;
3. immediately-previous-matrix exclusion; and
4. no exclusion control.

Compare survivor count, rank displacement, recurrence, package coherence and baseline-adjusted
outcomes. A winner becomes selector v2 with a new hash; v1 publications remain immutable.

**Shadow study completed 2026-09-10.** `report_option_board_selector_shadow.py` loaded
54,819 candidate observations across three sessions and exactly reproduced the first v1
publication (40/40 members, zero position mismatches). Key selection results:

| Variant | Excluded | Position-one | Structured | Repeat fraction | Median / p90 raw rank |
|---|---:|---:|---:|---:|---:|
| Lifetime global v1 | 90.3% | 1,387 | 516 | 0.0% | 3 / 17 |
| Same-session global | 82.8% | 1,917 | 930 | 12.4% | 2 / 17 |
| Previous matrix | 67.5% | 3,272 | 2,079 | 42.5% | 2 / 9 |
| Same-strategy grace 1 | 70.2% | 3,206 | 2,020 | 34.1% | 2 / 13 |
| Same-strategy grace 2 | 73.9% | 2,983 | 1,821 | 27.8% | 2 / 15 |
| No exclusion | 0% | 4,128 | 2,800 | 70.5% | 1 / 1 |

The delayed-mark outcome rows span only three independent sessions and are explicitly not
baseline-adjusted, so no variant graduates. Keep selector v1, accumulate immutable publications,
and rerun after at least 40 independent periods with exact same-structure controls. Artifact:
`docs/option_board_selector_shadow.json`.

### 3.12.1 Correctness revisions after publication

- **Wheel entry boundary:** new entries now require `calendar_dte > exit_dte`; contracts
   already at/below the management exit boundary produce `NO_WHEEL_CONTRACT_ABOVE_EXIT_DTE`
   instead of selected candidates carrying a warning. This deliberately bumps strategy identity
   to `phase2_v4`, hash `c4a46268…e6bc6`.
- **Directional gate semantics:** gate ledger v2 keeps broad directional observations for
   measurement but reports `EQUITY_DIRECTION = UNAVAILABLE` when a candidate requires direction
   and no qualified equity direction exists. It no longer records a false PASS.
- **Trade timeout containment:** optional trade ingestion has a 120-second per-underlying
   budget and leaves failed contract cursors unchanged for overlap retry.
- **Manual-cycle watermark:** a latest-completed-session run now uses the actual session close
   as `cycle_time`; the previous close-plus-15-minute value violated the exchange-slot contract
   during normalization.
- **Versioned context identity:** migration 027 replaces the one-context-per-matrix constraint
   with `UNIQUE (matrix_id, policy_sha256)`, matching the deterministic context ID and allowing
   immutable strategy-policy replays such as phase2_v3 and phase2_v4 to coexist.
- **Coherent strategy replay:** full-universe `--strategies-only` now selects the newest single
   complete configured-universe scheduled cycle. It no longer combines independently latest
   matrices that can never satisfy immutable Board publication.

Phase2_v4 activation published revision `7f1ae6e2…` for the 2026-09-10 16:45Z cycle:
13/13 underlyers and 238 members. Its prior-contract exclusion count is zero because selector v1
scopes contract history to the active strategy-policy hash. Treat the large first revision as a
version cold start, not evidence that selector quality improved; subsequent v4 publications and
the planned 40-period shadow comparison determine whether this reset behavior needs a selector
version change.

### 3.13 Historical open interest — **not purchasable at any tier**

This is the most consequential constraint in this register, and the one most likely to be
assumed away. **Advanced does not supply historical open interest. Neither does Business.**
Verified 2026-09-05 against the per-endpoint documentation:

- The only two sources of `open_interest` are `GET /v3/snapshot/options/{underlyingAsset}`
  (chain) and `GET /v3/snapshot/options/{underlyingAsset}/{optionContract}` (contract).
  Neither accepts `as_of`, a date, or a timestamp — the contract snapshot's *entire*
  parameter list is `{underlyingAsset, optionContract}`.
- Both endpoints document **"Plan History: Not applicable to this endpoint"** for Basic,
  Starter, Developer, Advanced and Business alike.
- `open_interest` is defined as *"the quantity of this contract held at the end of the
  last trading day"* — always the latest settlement, never a past one.
- No daily-open-interest endpoint exists in the documentation index.
- Options flat files are only Day Aggregates, Minute Aggregates, Quotes and Trades.
  **There is no open-interest flat file at any tier.**
- The pricing-page bullet "Daily open interest" refers to OI being present in the live
  snapshot, not to a historical series. It is the likeliest source of confusion here.

**Consequence.** Every OI-dependent behaviour is forward-accumulation-only, permanently:

| Dependent | Uses OI as | Status |
|---|---|---|
| Gamma exposure, flip point, walls (§3.7) | per-strike OI | Un-backtestable |
| `ZERO_DTE_GAMMA_SQUEEZE` | hard gate `volume/OI >= 1.5` | Un-backtestable |
| `SPREAD_RANGE_LOCATOR` | OI wall clustering | Un-backtestable |
| `VOLUME_OI_ANOMALY` | `volume/OI` ratio | Un-backtestable as defined |

The distinction that matters for the rest: **OI used as a liquidity filter is
substitutable; OI used as the signal is not.** `DIRECTIONAL_LONG_PREMIUM`,
`DIRECTIONAL_DEBIT_SPREAD` and `INCOME_WHEEL` only use OI as a floor or a sort tie-break,
so a volume/transaction-count substitute preserves their intent — as a declared policy
variant with its own hash, not as a silent swap.

**Options, in preference order:**

1. **Demote gamma to context, not signal.** The gamma lens is genuinely informative as a
   regime badge for a human operator; the evidentiary bar for displaying a regime is far
   lower than for triggering a trade. Costs nothing and keeps forward accumulation running.
2. **Accumulate forward.** At roughly one observation per underlyer per session, ~2 years
   to reach useful power. The clock only starts once the pipeline runs reliably.
3. **Buy OI history from another vendor** — ORATS or CBOE DataShop are the realistically
   accessible options; OptionMetrics/IvyDB is institutional. Worth pricing before assuming
   two years of waiting is the only path.

**Implemented 2026-09-06:** migrations 022 and 024 create and harden
`option_daily_contract_facts`, a retained provider-evidence table at
`(contract_id, settlement_session)`. OI capture reads the full raw chain, not the filtered
strategy corridor. Migration 024 records the observation session, enforces
`settlement_session < open_interest_observed_session`, preserves the first value for causal
replay and records later conflicting observations as revisions. Current live state
(2026-09-11) is 96,199 OI facts across six settlement sessions and all 13 configured
underlyers. The writer is operational; independent history remains too short for robust
OI-dependent strategy inference.

### 3.14 Historical backtesting and confidence scoring — **historical replay not built**

There is no replay path. `option_signal_decay_outcomes` measures *forward* only (observe a
candidate, wait, measure at 15/30/60min, close, next open). Nothing reconstructs what a
strategy *would have selected* at a past decision time from information available then.

The equity side already solved the methodology and it should be ported, not reinvented:
[historical_universe.py](../backend/equity/historical_universe.py) for point-in-time
membership, [outcomes.py](../backend/equity/outcomes.py) for entry timing, and
[scanner_confidence.py](../backend/research/scanner_confidence.py) for the gates —
`events >= 100`, `independent_periods >= 40`, `t > 2`, alpha positive in *both* halves,
Benjamini-Hochberg `q <= 0.05`, and incremental alpha over a baseline slice.

**Not Advanced-gated.** Six of eight strategies are backtestable on Developer today with
4 years of depth; the two exceptions are the OI-dependent ones in §3.13.

**The benchmark is the part most likely to be got wrong.** The equity harness measures
*sector-adjusted* alpha. There is no options equivalent unless one is built, and without
it a long-call strategy backtested across a rising market will show large "edge" that is
pure beta. The analogue is a **structure-matched naive baseline** — same session, same DTE
lane, same side, ATM strike, no selection logic — so the claim becomes
`mean(strategy) − mean(naive)`. This belongs in the measurement contract before any
ingestion work, not bolted on afterwards.

**Required history depth.** Independent periods are capped by non-overlap at the holding
horizon after collapsing each decision date to one cross-sectional observation. Power
needs roughly 300–600 of them for a plausible edge at `t > 2`:

| Lane | Horizon | Periods/yr | 6 mo | 1 yr | 2 yr | 4 yr |
|---|---|---|---|---|---|---|
| 0-DTE / intraday | ≤1 day | 252 | 126 | 252 | 504 | 1008 |
| NEAR (≤7 DTE) | 5 days | 50 | 25 | 50 | 100 | 200 |
| SHORT (8–21 DTE) | 10 days | 25 | 12 | 25 | 50 | 100 |
| MEDIUM (22–45 DTE) | 21 days | 12 | 6 | 12 | 24 | 48 |

Six months fails the `>= 40` floor for every lane except intraday. Two years suffices only
for 0-DTE. **Pull the full 4 years** — the medium-DTE lanes of the newest strategies cannot
clear the platform's own gates on less. The "both halves positive" requirement already
provides the regime-drift check.

Cheapest lever: the 13 configured underlyings are mostly correlated index ETFs, so
cross-sectional aggregation collapses them to nearly one observation. **Widening the
backtest universe to 50–100 liquid single names buys more effective sample than adding
years**, and costs only ingestion.

**What Advanced adds here** is quotes, and the leverage is larger than it appears: the
options quotes flat file carries **all history from 2022-03-07**. A single month of
Advanced can backfill roughly four years of NBBO for research — real spreads, realistic
fills, and the aggressor classification of §3.5 — after which the subscription can lapse
until live trading. Confirm the terms permit retaining downloaded files after a downgrade
before relying on this.

### 3.15 Open-interest change (ΔOI) — **research detector live, not a strategy**

No strategy consumes a *change* in open interest. The research detector in
`options/analytics/open_interest_change.py` compares retained settlement facts, classifies
`OPENING` / `UNWINDING` / `UNCHANGED`, ranks absolute changes and exposes revisions and
multi-session gaps as quality reasons. `report_open_interest_change.py` writes the review
artifact. It deliberately sits outside `OptionStrategyEngine.scan()`: OI updates once per
session, so putting it inside the intraday engine would emit the same fact under a new
matrix identity every cycle.

`VOLUME_OI_ANOMALY` is the closest and is explicitly not this. Its own registry entry says
*"volume greater than OI does not imply opening flow"* — correct, because high volume
against standing OI can be entirely intraday churn that ends the session with no new
positions. It measures activity, not positioning.

ΔOI is the only unambiguous evidence that contracts were *opened*, since volume counts
opens, closes and round-trips identically:

- `volume ≈ ΔOI` → predominantly opening — the genuine new-positioning case
- `volume >> ΔOI` → churn, precisely the false positive the current ratio cannot exclude
- `ΔOI < 0` → unwinding

Direction still requires quotes (§3.5), but ΔOI separates new positioning from noise,
which nothing currently does.

**Cadence note:** OI settles once daily and is constant across every intraday cycle, so
this is a once-per-session detector comparing against the prior session — a different
rhythm from every other module.

Volume confirmation is implemented by preferring settled `mark_volume` and falling back
to `max(option_chain_snapshots.day_volume)` with `PROVISIONAL_SESSION_VOLUME`. The current
cohort spans two sessions (2026-09-01 → 2026-09-03), so confirmation is correctly withdrawn:
one session's volume cannot explain a multi-session OI move.

The detector can only be forward-validated and must not enter the auto-trading path on the
strength of the hypothesis alone. Migration 024 also prevents a close-slot cycle from
stamping prior-session OI onto the current settlement session.

### 3.16 Graduation criteria — when research inputs enter signal detection

Four inputs now exist as research surface and none of them touch contract selection. This
section records what must be true before each one does, so the decision is made against a
written trigger rather than against enthusiasm on the day.

**The standing rule.** Selection changes only on measured, out-of-sample evidence. A
signal that has not been differenced against the structure-matched baseline (§3.14) has
not been measured, because a raw positive result in a rising market is direction rather
than selection.

**Record before you gate.** A gate that suppresses candidates destroys the evidence needed
to calibrate it: the suppressed candidates are never observed, so their outcomes cannot be
compared. Every input below is therefore attached as candidate *evidence* first and only
becomes a *gate* once the recorded population says it discriminates. Evidence is not
policy — attaching an observation does not enter `StrategyPolicy.model_dump()` and so does
not move the strategy hash. The identity bump belongs to the later step, when a threshold
is introduced.

**Why the cohort argument is currently void.** The derived layer was purged 2026-09-05, so
there is no live candidate cohort to orphan and `start_read_only` fails every candidate to
`READ_ONLY_RESEARCH` regardless. Identity bumps are cheap in this window and will stop
being cheap once a cohort accumulates. That is an argument for deciding early, not for
gating early.

#### 3.16.1 Realized-volatility forecast → volatility richness

| | |
|---|---|
| Artifact | `vol_forecast_policy_v1.json`, sha `ebcda5516f9c…5d8d91ae` |
| Configuration | 30-minute intraday open-to-close variance, HAR lags (1, 5, 22), horizon 21 sessions, overnight variance excluded |
| Measured | +0.120 R² against per-name climatology, DM p = 0.0005, synthetic control arm −3.0% |
| State | Pinned. Loaded by nothing; `load_volatility_forecast_policy` has no caller |

**Preliminary SPY VRP study completed 2026-09-06.** Historical ATM IV is built from the
nearest calls and puts, then interpolated in total variance to 7/21/45-session maturities.
The 90-day mark backfill produced 13,604 marks over 304 sessions. Matched observations were
64 / 117 / 86, but only **16 / 9 / 4 independent periods** respectively — every lane is
`INSUFFICIENT_INDEPENDENT_PERIODS`. At 21 sessions HAR reduced overlapping premium RMSE
from 0.00936 (climatology) to 0.00836, but richness quintiles were not cleanly monotonic
and the sample cannot support a gate. Artifact: `variance_risk_premium_study.json`.

The study remains explicitly preliminary: static 4% rate, option-mark policy not yet
versioned, SPY-only history, adjusted option aggregates, and no strategy-P&L link. It proves
the matched-horizon calculation is feasible; it does not prove trading edge.

**Realized-volatility lineage hardened 2026-09-08.** The forecast reporter now requires
split/dividend-adjusted daily history for every pooled name rather than silently falling
back per ticker. IWM has no adjusted history in the current database and is explicitly
excluded; the other 12 configured names retain 1,164–1,254 adjusted sessions. The
regenerated 21-session open-to-close result remains +0.120 R² against per-name climatology
with independent DM p = 0.0005. This establishes forecast skill, not option-strategy edge.

*Act when all of:*
1. The variance-risk-premium study reports a positive advantage over the structure-matched
   baseline at `MEASURABLE`, not `INDICATIVE`.
2. That advantage survives excluding earnings sessions on single names (§3.10 — the event
   calendar is still missing, so index names qualify first).
3. Richness has been recorded as candidate evidence for long enough that P&L can be
   segmented by richness bucket across the *whole* candidate population.

*What changes on graduation:* `expected_move` in `DIRECTIONAL_LONG_PREMIUM` and
`DIRECTIONAL_DEBIT_SPREAD` stops being `local_iv * sqrt(T)` and becomes the physical-measure
forecast, which is what makes those gates capable of finding edge at all. Adds
`volatility_richness_state` to `StrategyContextSnapshot`. **IDENTITY** — strategy version
bump required.

*Stop rule:* if the advantage over baseline is not positive at 21 sessions, do not gate on
it at any horizon. Forecast skill was already measured to vanish by 90 sessions and to be
actively negative at 126, so a longer horizon is not a fallback.

#### 3.16.2 Change in open interest (ΔOI)

| | |
|---|---|
| Data | `option_daily_contract_facts`, captured every cycle, 30,212 facts recovered from raw pages |
| Detector | `options/analytics/open_interest_change.py`, report-only |
| State | Research surface. No strategy consumes it |

*Act when all of:*
1. A rolling baseline exists, so "unusual" is defined against that contract's own history
   rather than against an absolute number that means opposite things for SPY and SOFI.
2. Consecutive-session observations have usable volume. The join exists, but the recovered
   cohort spans two sessions and therefore correctly reports `VOLUME_UNAVAILABLE`.
3. Roughly two years of forward accumulation, per §3.13 — ΔOI can never be backtested, so
   there is no shortcut.

*What changes on graduation:* `VOLUME_OI_ANOMALY` replaces its volume-over-open-interest
ratio with the change-based classification, which is the defect its own registry entry
admits to. **IDENTITY** — strategy version bump required.

*Constraint that does not lift:* ΔOI carries no direction. Open interest rises whether the
initiator bought or sold, so a put build is equally consistent with put selling. Direction
requires NBBO at trade time (§3.5) and is Advanced-gated. ΔOI must never be published as
directional, and must remain `RESEARCH_CONTEXT` until §3.14 gates are cleared.

#### 3.16.3 Historical implied volatility → IV rank (§3.11)

| | |
|---|---|
| Data | 124,052 policy-aligned settlement marks across all 13 underlyings |
| Validated | 97.8–99.6% solver convergence, skew and level orderings economically correct |
| State | Raw mark/rate/dividend readiness passes; strict matched-IV history needs additive weekly expirations; persisted contexts remain empty |

*Act when all of:*
1. Marks are backfilled for every configured underlying. **Complete.**
2. The IV series is written under `option_settlement_valuation_v1`, distinct from the
   intraday mark policy. A settlement close and a 15-minute mark are different valuation
   policies and never share one. **Policy/provenance and all-underlying backfill complete.**
3. `option_iv_context_snapshots` is populated and `iv_regime` stops being hard-coded
   `None` with `INSUFFICIENT_COMPLETED_SESSION_HISTORY`.

*What changes on graduation:* IV rank becomes available to every strategy as context, and
`DIRECTIONAL_LONG_PREMIUM` can finally act on the headwind it currently records and
ignores — buying premium into rich IV. **IDENTITY** if it becomes a gate; not if it stays
evidence.

*Caution:* strikes are nominal contract terms. Solving against split-adjusted closes
silently corrupts every moneyness. Unadjusted closes only.

#### 3.16.4 Structure-matched baseline (§3.14)

| | |
|---|---|
| Implementation | `compare_against_baseline` in `options/analytics/performance.py` |
| Definition | Same expiration and complete leg template translated toward ATM, no selection |
| State | Live forward cohort validated; historical replay integration remains unbuilt |

This one is not a signal and never graduates into selection. It is the instrument that
decides whether anything else does, which makes it the prerequisite for 3.16.1 and 3.16.2
rather than a peer of them.

Implemented 2026-09-07: the control translates the candidate's entire leg template by one
common strike offset toward ATM. Contract types, sides, ratios, multipliers and absolute
widths are unchanged. Every translated leg must exist in the candidate's entry batch and
in the outcome service's coherent exit batch; partial packages are excluded. Capital at
risk and P&L are recomputed from the translated package under the same commission policy.
Coverage is reported as exact packages divided by eligible measured outcomes.

Validated 2026-09-08 against the first post-purge cohort: all 617 eligible outcomes found
exact same-structure entry/exit packages and 612 translated controls had valid positive
capital at risk. A later all-strategy report matched 1,855 bounded controls from 1,867
eligible outcomes. Strategy and horizon filters now scope outcomes, coverage, availability,
suppressions and baseline inputs consistently. Mixed-horizon summaries are descriptive
only: their statistic and p-value are unavailable with `MIXED_MEASUREMENT_TYPES`, while
`by_horizon` and `by_strategy_horizon` retain valid single-horizon inference. JSON emits
`null`, never `NaN`, for unavailable statistics.

This validates the live forward measurement contract; it does not supply historical replay
or enough independent sessions to license a strategy. The current cohort still spans one
session and remains `INDICATIVE` / `NOT_ROBUST`.

#### 3.16.5 Equity-style option confidence qualification

`summarize_option_confidence` now applies the equity research contract to structure-matched
pairs grouped by strategy and measurement horizon. Candidate returns and incremental
advantages are first collapsed to one cross-sectional portfolio observation per exchange
session, then non-overlapping periods are selected using exchange-session ordinals.

A group qualifies only when all are true:

- at least 100 paired events and 40 independent periods;
- mean strategy return and mean incremental advantage are positive with `t > 2`;
- both early and late halves are positive for absolute and incremental returns;
- Benjamini-Hochberg FDR is `q <= 0.05` for both tests.

`report_option_performance.py` writes the full statistics and reports
`CONFIDENCE_PASS` / `NOT_QUALIFIED` separately from `ROBUST_PASS` / `NOT_ROBUST`.
This distinction prevents the older `MEASURABLE` sample-size label from being read as
evidence of edge. No strategy consumes the verdict; it is a graduation instrument only.

### 3.17 First post-fix market session validation — **passed 2026-09-08**

`report_live_option_validation.py` now audits one coherent latest-slot cohort. The 10:30 ET
delayed slot passed every check:

- 13/13 analyses `COMPLETE`; 4,947 snapshots, 4,857 model marks and 4,816 local IVs;
- median absolute option/spot skew 31.4 seconds, every persisted row within 60 seconds,
   zero release-stamp fallbacks, and 100% model-mark coverage in the 1,000+ volume bucket;
- 14,484 OI facts across all 13 underlyings, observed 2026-09-08 and correctly assigned
   to the prior settlement 2026-09-04, with zero conflicting revisions;
- all four gamma scopes for every matrix; 12/13 TOTAL profiles at ≥95% coverage;
- 195 distinct contract trade batches, all `COMPLETE`, zero invalid pages and 224,827
   fetched rows; 418,299 classified included trades accumulated during the session;
- 1,202 candidates and exactly 7,212 persisted execution-gate rows (six each), with no
   missing ledgers; the running API returned all six rows for a real candidate;
- the sweep path is active: one `SWEEP_LIKE_CLUSTER` finding and 12 honest
   `NO_QUALIFYING_SWEEP_LIKE_WINDOW` suppressions, replacing the former data-unavailable
   result;
- 617 immutable 15-minute outcomes and 1,314 current marks, with zero noncausal marks.

The first exact-package baseline run matched all 617 outcomes; 612 translated controls had
valid positive capital at risk. The report initially produced cross-sectional p-values from
hundreds of contracts on one date. That was rejected: significance now collapses each
session/horizon to one portfolio observation. With one independent session every strategy
is `NOT_ROBUST` and p-values are unavailable, as required.

Four integration defects were found and fixed during the validation:

1. Same-cycle trades were invisible because analysis froze `observed_time` before trade
    ingestion. The final decision watermark now advances after ingestion.
2. Continuing debit signals failed when `%s::TEXT` parsed a negative Decimal as unary minus
    on text. Metadata values now use `CAST(%s AS TEXT)`.
3. Trade ingestion batch identity omitted the contract ticker, so 15 watchlist contracts
    collided on one `(underlying, slot, filter)` batch. Contract ticker is now part of the
    request-filter hash; all 195 batches complete independently.
4. Provider events remained `PENDING` forever because the semantics repository had no
    classifier. Versioned condition classification is now applied at parse time; retained
    2026-09-08 rows were backfilled under the same policy.

No selector thresholds or strategy identity changed. All signals remain `BLOCKED` by the
recorded event-calendar, quote, risk-engine and read-only gates.

## 4. Ordering when Advanced arrives

1. Persist immutable all-universe Board revisions and exact ordered membership (§3.12),
   then make the Opportunity Board read that projection. This must precede opportunity-
   admission calibration; reconstructed raw rank leaders are not actual Board history.
2. Build the valuation seam (§3.2) and gate ledger (§3.3) **before** buying the
   entitlement, so the switch is configuration rather than surgery. **Completed: gate
   ledger v2 plus migrations 025, 028 and 029.**
3. Build the retained daily open-interest table (§3.13) immediately and independently —
   it is the only input that cannot be acquired retroactively. **Done 2026-09-06**
   (migrations 022 and 024); capture runs every cycle and 30,212 facts were recovered
   from retained raw pages.
4. Build the replay harness and measurement contract (§3.14) on Developer data. This does
   not wait for Advanced, and six of eight strategies can be scored without it.
5. Consider one month of Advanced *for research backfill* (§3.14) — quote flat files back
   to 2022-03-07 — before, and separately from, the live-trading subscription.
6. Stand up the Advanced data engine and options WebSocket; recompute IV from NBBO mid.
7. Run `advanced_shadow` execution alongside the delayed proxy and compare.
8. Only then let quote gates return verdicts and authorise a live adapter.

Automatic trading must remain gated on measured, out-of-sample evidence per
[OPTION_RESEARCH_DESIGN.md](OPTION_RESEARCH_DESIGN.md). Detection improvements do not
substitute for calibration. §3.16 records the specific trigger for each research input
that is currently waiting outside signal detection.

## 4.1 Current pending queue — 2026-09-11

### Buildable now

1. **Historical option replay (§3.14):** port the equity point-in-time replay harness for the
   six non-OI-dependent strategies after policy-aligned settlement marks exist. Forward outcomes
   exist; historical selection replay does not.
2. **Comparable-IV data completion (§3.11):** admit recent weekly expirations with horizon-aligned
   strike sampling, backfill only those newly admitted contracts, and require the dry-run artifact
   to pass all 39 ticker/bucket contexts before applying or wiring evidence.

### Completed this pass

1. **Frontend bundle split:** route-level lazy loading reduced the initial/shared production
   chunk from 980.59 kB to 363.70 kB; the Vite 500 kB chunk warning no longer fires.
2. **Ticker event reference:** `/api/options/calendar/{underlyer}` and the ticker right rail
   expose coverage-aware Earnings/Fed states and upcoming dates without treating absent,
   uncovered events as CLEAR.
3. **Event calendar activation:** `public_calendar_v1` is configured, durably populated and
   consumed by all 13 option contexts with direct coverage lineage and 12-hour freshness.
4. **Point-in-time rates:** 9,169 Treasury observations across 673 dates are persisted and
   interpolated by contract maturity with immutable observation lineage.
5. **Point-in-time dividends:** exact historical/live corporate-action coverage and discrete
   cash-flow conversion are wired; the resident refresh covers all 386 active securities.
6. **Settlement marks:** 124,052 unadjusted, policy-aligned marks span all 13 underlyings.
7. **Comparable-IV foundation:** migrations 038, analytics, repository, dry-run/apply materializer,
   strict artifact and dynamic health readiness are built. Live/persisted acceptance remains gated.

### Data/configuration required

1. **Matched-IV density:** add recent weekly expirations and horizon-anchor contracts. Do not
   refetch the completed monthly contracts; the marks writer skips policy-complete contracts.
2. **Market-hours acceptance:** run the active 60-DTE policy inside the delayed close/intraday
   source-age window, then require a 39/39 dry run before `--apply`.

### Evidence accumulation required

1. **Board/selector:** 13 publications now exist; retain selector v1 until
   at least 40 independent periods support a versioned change.
2. **Outcome calibration:** 21,891 delayed outcomes currently span only four independent
   sessions. Continue forward collection; row count is not a substitute for session count.
3. **OI strategies:** 96,606 OI facts still span only six captured settlements and remain
   descriptive. Continue daily capture or
   price a historical OI vendor before promoting OI-dependent strategies.

### Advanced entitlement required

`ADVANCED_NBBO_MIDPOINT`, quote spread/size gates, aggressor-side classification, quote/trade
WebSockets, side-aware slippage and Advanced shadow comparison remain blocked on quote access.

### Live execution future work

The paper `ExecutionManager`, order/fill/position ledger, portfolio risk and margin engine,
broker adapter and live authorization remain intentionally unbuilt. Read-only mode stays on.

## 5. Change log

| Date | Change |
|---|---|
| 2026-09-05 | Register created. Gamma exposure (§3.7), smile study (§3.8) and opt-in trade ingestion (§3.6) added. |
| 2026-09-05 | Entitlement boundary extended with historical-data rows. Added §3.13 (historical open interest unavailable at every tier — corrects the assumption that Advanced supplies it), §3.14 (backtest harness, confidence gates, required history depth, quote-flat-file backfill route) and §3.15 (ΔOI not detected today). Ordering in §4 revised so the harness and the retained OI table precede any entitlement purchase. |
| 2026-09-06 | Added §3.16, the graduation criteria for four research inputs that now exist outside signal detection: the realized-volatility forecast, ΔOI, historical implied volatility, and the structure-matched baseline. Records the standing rule that inputs are attached as candidate evidence before becoming gates, because a gate censors the population needed to calibrate it. Notes that identity bumps are currently cheap — the derived layer was purged and no cohort exists — which argues for deciding early rather than gating early. §4 step 2 marked done (migration 022, capture live, 30,212 facts recovered). |
| 2026-09-06 | Reconciled implementation state. Migration 024 hardens OI settlement attribution and revision provenance; §3.15 now records the report-only ΔOI detector and implemented volume join. Added preliminary matched-horizon SPY VRP results and their insufficient independent-period verdict. Restricted the existing naive baseline to single-leg structures; multi-leg controls remain required. |
| 2026-09-07 | Replaced the single-contract control with exact translated same-structure packages for long/short singles, verticals, butterflies and condors. Added package coverage reporting and equity-style option confidence qualification with exchange-session non-overlap, both-halves checks and BH-FDR. Live-cohort validation remains due Tuesday. |
| 2026-09-07 | Applied migration 025 and persisted all six versioned execution-gate verdicts transactionally per candidate. Candidate details now expose the ledger and the Decisions drawer replaces hard-coded capability text with recorded verdicts and reasons. |
| 2026-09-08 | Completed first post-fix market-session validation (§3.17). Mark alignment, OI attribution, gamma scopes, contract-scoped trade batches, classified sweep evidence, candidates, gate ledgers, outcomes and current marks all passed. Fixed same-cycle trade visibility, debit-signal continuation casts, trade-batch identity and missing trade classification. First 617 structure baselines are explicitly one-session `NOT_ROBUST`. |
| 2026-09-08 | Hardened performance reporting after review: mixed-horizon inference fails closed, unavailable statistics serialize as JSON `null`, all auxiliary report sections follow strategy/horizon scope, and separate horizon summaries preserve valid inference. Recorded the current Opportunity Board freshness limitation and the atomic publication contract required before an Advanced-backed execution view. |
| 2026-09-08 | Completed pre-commit read-model and research-lineage review. Replaced the Opportunity Board's quadratic prior-contract correlation (107 seconds live) with an equivalent set-based first-selection query (0.48 seconds), and regenerated the realized-volatility artifact using adjusted daily bars only; IWM is explicitly excluded instead of entering the pooled study under a different lineage. |
| 2026-09-10 | Confirmed the reconstructed performance cohort was not actual Opportunity Board membership. Renamed it `RANK_LEADERS`, retained the old parameter only as an explicit alias, and moved prospective immutable Board publication ahead of admission calibration and Advanced work. Migration 026, selector repository, full-cycle publication hook, immutable Board reads, exact prospective performance cohort and Options Screener were implemented. First publication `21af1de7…` covers all 13 names with 40 members. Selector shadow study kept v1; Wheel phase2_v4 and gate ledger v2 correctness changes landed. |
