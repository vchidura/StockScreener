# Research Harness Enhancements

## Signal Evidence Coverage: First Audit Complete

The [Retained Signal Scorecard](EQUITY_SIGNAL_EVIDENCE_SCORECARD.md) now inventories
30 retained source names in 213 source/version/role/timeframe/origin groups, with
312 registered composite-scanner cells and 504 daily 5/10/21-session scorecard rows
across origin and ticker scope. The single damaged PNC feature row was recovered by
the administrator and its exact reconstruction and post-repair readability verified.

Only level-retest rejection currently has a multi-year daily replay in the retained
ledger. Its standalone results do not pass the scorecard's stable-edge/uncertainty
checks; the remaining live cells are absent or have too few independent periods.
Historical policy-date mismatches are disclosed and handled as explicit retrospective
evaluation links, not backdated live authorization. No new qualification or numeric
quality/probability score has been published.

This completes the inventory/retained-evidence slice, **not all-signal backtesting**.
The next bounded work is complete daily detector replay with no-signal controls and
role-appropriate outcome contracts for the uncovered families, as described in the
scorecard guide. The closed portfolio experiments and separately authorized forward
paper/option-shadow work remain unchanged.

## Prospective Observation: Separately Authorized

The user explicitly retained a **20%** prospective drawdown research limit and
authorized the [Forward Paper Study](EQUITY_FORWARD_PAPER_STUDY.md). It is enrolled
for six future cohorts with a fixed saved ridge fit, momentum and SPY, using the
current observed universe. The local watcher is observation-only, not brokerage
execution or live rank publication. The first decision is September 14, 2026;
the planned sixth exit is March 16, 2027. Historical criteria and conclusions are
unchanged. See the forward-study document for readiness, operation and limitations.

## Exploratory Phase: Closed

The [Consolidated Research Closeout](EQUITY_RESEARCH_CLOSEOUT_2026-09-12.md) completes
the final event/execution sensitivity and records **NO-GO for production deployment**.
Retain the frozen ridge candidate for research; do not automatically launch more
historical variants, expand infrastructure or promote live scores. At closeout no
paper tracker had been started; the separately authorized forward phase is documented
above and is not unfinished work from the historical study. The longer-term design below remains preserved,
not an active instruction to implement every proposed slice.

### Original Scope And Completed Experiments

User-approved scope reset on 2026-09-12: pause further production data-infrastructure
work and run a bounded experiment to identify a research direction. Full certification
remains necessary for production claims, but is not a prerequisite for this explicitly
limited price-return diagnostic.

Frozen settings: [equity_price_diagnostic_config.json](equity_price_diagnostic_config.json).
Use the same deterministic 300-ticker sample for momentum, equal-weight controls and
all seven existing daily scanner families, sampled from historical universe membership
rather than current survivors. Historical eligibility still controls entry/exit from
each dated cross-section. Rebalance every 21 XNYS sessions from 2022-09-07; enter at
the next session's open and exit at the 21st holding-session close. Data ends 2026-09-03.
The fixed later evaluation period starts 2025-01-01, with no fitting or tuning on either
period. Earlier scanner studies used these dates, so this is not a pristine holdout.

No dividend-inclusive return, sector-neutral alpha, calibrated probability or
deployability claim is permitted. Momentum follows the existing 12-1 feature; scanners
use existing rules and versions with fresh long matches only on rebalance dates.
Scanner comparisons use the momentum-feature-ready subset for a common control.
Known identity conflicts break histories; unknown historical identity coverage remains
disclosed. Intended weights are fixed before outcome availability is inspected. Keep
unresolved selected weights and report fixed return scenarios (-100%, -25%, 0%, +25%)
alongside observed-only statistics, never presenting imputed values as actual returns.
Cost scenarios are 0/4/10/25 basis points per round trip without trade netting.

The first empirical comparison is complete:
[Exploratory Backtest Results](EQUITY_EXPLORATORY_BACKTEST_2026-09-12.md).
The [Disjoint Momentum Replication](EQUITY_MOMENTUM_REPLICATION_2026-09-12.md) also
shows positive excess, with better leave-best-period stability but substantial downside
and shared market exposure. Seven first-sample gaps have a user-approved, separately
labelled event-price sensitivity; original results remain unchanged.
The [Fixed Momentum Sizing Comparison](EQUITY_MOMENTUM_SIZING_2026-09-12.md) reduced
volatility modestly but increased dollar concentration and sacrificed return, with
mixed risk-adjusted benefit. The [Daily Exposure Comparison](EQUITY_MOMENTUM_EXPOSURE_2026-09-12.md)
finds much larger daily-path drawdowns than holding-boundary estimates. A fixed 75/25
equal-weight/cash control reduces scenario risk more than the tested inverse-volatility
rule, but still has roughly 25-28% later drawdown. The user has accepted a provisional
20% portfolio drawdown research limit as described below; no portfolio is promoted
and no scanner gate is added.
The [Scheduled SPY Trend Filter](EQUITY_MOMENTUM_TREND_2026-09-12.md) has now been
tested as one fixed exposure rule. It missed recoveries, reduced later returns and
still breached the 20% target in both samples. Do not adopt it or automatically tune
neighboring parameters; retain the result as a rejected risk-control hypothesis.
The [Matched Benchmark Comparison](EQUITY_MOMENTUM_BENCHMARK_2026-09-12.md) is complete
under the subsequently accepted objective of beating SPY while respecting the 20%
drawdown research target. At equal 75% exposure, momentum has greater later compounded
price growth than scheduled SPY in both samples, but about 2.7 times the daily
volatility and 25-28% drawdown. Removing the strongest excess-return period leaves a
much smaller advantage. Retain the ranking baseline, not a qualified portfolio;
that benchmark comparison did not test an equal-risk policy.
The subsequent [Fixed Volatility Budget](EQUITY_MOMENTUM_VOL_BUDGET_2026-09-12.md)
uses one 10% annualized budget, 63 trailing returns and a 75% exposure cap for both
momentum and SPY. It brought full-evaluation realized volatility near 10%, but did
not establish a replicated return advantage over budgeted SPY and never beat the
original fixed-SPY hurdle across the declared scenarios. Pause further portfolio
overlay variants; return to bounded stock-selection research with unchanged controls.
Following user approval, the [Two-Feature Ridge Challenger](EQUITY_RIDGE_CHALLENGER_2026-09-12.md)
has been tested with sample-1-only causal expanding fits and the same fitted models
used to score sample 2. It improves later top-basket returns in both samples but
underperforms momentum in the earlier common-date evaluation, has weaker average
rank correlation and still breaches the 20% drawdown target. Retain the frozen model
for validation; no new model variant or production strategy is selected by these results.
The [Targeted Ridge Validation](EQUITY_RIDGE_VALIDATION_2026-09-12.md) now corroborates
all ten large-contributor paths against native provider data and CAR against Yahoo.
The five missing selections comprise three symbol continuations and two halt-related
execution/valuation cases. Recorded successor paths are review-only, with official
versus provider symbol-date caveats; the original returns and models are unchanged.
The final, separately recorded event/execution sensitivity is now complete: reviewed
alias joins, CLSK unfilled cash with no fees/reinvestment, and unchanged QMMM/other
unresolved scenarios are applied consistently to affected portfolios. Later ridge
returns are 89.96% / 78.70% with 22.56% / 24.48% drawdown in the 10-bps zero-missing
illustration. The original models and studies remain unchanged; the target still
fails in both later samples. See the closeout report for the consolidated decision.
The broader contract below remains the eventual certification target, not a gate on this
diagnostic. Existing work is preserved; nothing is reverted or published to live ranks.

### Provisional Drawdown Limit

On 2026-09-12, after the drawdown definition and its use were explained, the user
accepted **20% maximum peak-to-trough portfolio decline** as the next research target.
This is an assessment limit, not a one-day loss limit, per-stock stop, automatic
liquidation rule or promise of future protection.

Assess saved daily-close portfolio paths (including modelled entry/exit costs) in both
samples across development, later and full-history periods, using the already declared
0/4/10/25 bps cost and -100%/-25%/0%/+25% unresolved-return scenarios. Exactly 20% is
within the target; a larger decline breaches it. Report scenario breaches as such, not
as fully observed losses. Passing these scenarios alone cannot certify a strategy.
Fully observed-window checks are supporting diagnostics, not a replacement for the
whole-portfolio assessment.

This limit was chosen after the previous results were examined. It is not a
preregistered gate for those completed experiments, and neither their settings nor
their results are rewritten. No exposure percentage is optimized to pass the target.
Any next risk-policy experiment must be specified separately and compare returns,
market participation and costs as well as drawdown; an all-cash result is not a useful
stock-selection strategy. The user subsequently accepted benchmark-relative performance
against SPY as the return objective. The bounded comparison uses strictly greater
compounded net price return than matched-schedule SPY at the same 75% equity exposure,
alongside the existing drawdown target; no absolute annual return hurdle was chosen.
This controlled scheduled price-return benchmark is not continuous buy-and-hold total
return and does not by itself establish risk-adjusted alpha.

Assessment of the existing daily-path summaries is complete: all three portfolios
breach 20% in both later samples in the 10 bps, zero-missing-terminal-return
illustration. The 75/25 control is closest (25.10% / 28.08%) but still does not meet
the target. All 288 saved scenario-summary cells were checked; none of the portfolios
meets the limit across the full declared scenario set. Details and stress caveats are
in the [Daily Exposure Report](EQUITY_MOMENTUM_EXPOSURE_2026-09-12.md).

## Equity Opportunity Research Contract (2026-09-12)

Status: design reviewed twice; implementation proceeds in the gated slices below.
This section governs the new opportunity-prediction work. The older incident backlog
below remains historical context, not an instruction to build every proposed service,
table or statistic before starting research.

### Mandate and non-goals

- Research identifier: `equity_opportunity_daily_v1`.
- Liquid US common stocks, long-first, using the dated
	`liquid_us_common_stocks_v2` universe rather than today's selected tickers.
- Daily decisions after complete data is available, with market time and actual
	information availability recorded separately. Provider delay is not ignored.
- Primary outcome: 21 XNYS sessions from the next executable open through the close
	of the 21st holding session, net of the declared cost policy. Subtract the matched
	sector return over the identical window for the selection target; retain absolute
	net return and broad-market return as separate diagnostics.
- Five- and ten-session outcomes are predeclared diagnostics, not alternative primary
	endpoints selected after viewing results. Sector fallback is explicit and reported.
- Initial research ranks are not trade approval, option eligibility, a probability,
	or a claim of positive expected absolute return.
- No short strategy, automatic orders, new fundamental/event model, nonlinear model,
	optimized stops/targets, or live rank replacement in the first implementation.

### Separate decisions and their evidence

| Component | Question | Validation target |
|---|---|---|
| Selection | Which eligible stocks have better subsequent returns? | Later-period rank IC, ordered return buckets, net top-bucket portfolio and incremental value over the baseline |
| Timing | Does waiting for a defined trigger improve the original decision? | Net value versus immediate and simple delayed entry, including cash and candidates that never trigger |
| Risk | What exposure is permissible? | Portfolio drawdown, concentration, volatility, liquidity and cost sensitivity at comparable risk |
| Probability | How often does this exact candidate outcome occur? | Causal individual-event calibration, proper scores and improvement over a causal base-rate forecast |

Existing scanner `ROBUST_PASS` remains its existing standalone qualification contract.
It does not automatically qualify a selection model, a timing overlay or a hedge.
Conversely, a scanner may be explored as an input without a standalone pass, but the
combined policy must demonstrate its own out-of-sample value. No unqualified feature
receives an arbitrary confidence bonus. Basket-win probability must never be described
as an individual candidate's probability. Rank and expected-return magnitude require
separate validation; a regression score is initially only a rank.

### Reproducible research data

One working row represents an eligible security at a decision time, including rows
with no scanner match. Record security identity, dated universe and sector provenance,
bar/action lineage and availability mode, feature version/hash, information cutoff,
missingness codes, exact outcome-policy ID/hash, entry/exit times and outcome
availability. Keep unavailable outcomes, invalid entries and non-triggers in coverage
counts; never turn missing data into a loss, zero return or negative pattern match.

Compute historical features from retained canonical inputs under an explicit revision
and split/total-return policy, not by joining today's portal projections backward.
Distinguish reconstructed availability from actual live observation. A 12-1 momentum
feature requires at least 252 prior-session offsets, more than the old 210-bar scanner
warm-up. Audit dividends, splits, delistings, exact session continuity, ticker changes,
and dated sector references before certifying the dataset. No assumption is made that
the currently stored history supports a genuinely untouched holdout.

Reuse canonical bars, universes, evidence, outcomes, policies and the qualification
ledger. Working feature matrices are reproducible temporary research data; a second
permanent feature/outcome database and a new umbrella driver are not prerequisites.
Preserve sufficient model/preprocessing artifacts and provenance to reproduce any
published prediction before deleting working data. Artifact persistence is a later
slice, not permission to use process-local fitted state in production.

### Restricted experiment ladder

1. `M0`: the existing 12-1 momentum ranking, reconstructed on the correct universe,
	 availability and executable-outcome contracts. Compare with the eligible-universe
	 portfolio and market/sector benchmarks. Momentum may genuinely fail locally.
2. `M1`: a small ridge challenger using `mom_12_1` and `rev_5`, with the same
	 predeclared risk/sector controls as the baseline. Feature transforms, intercepts,
	 regularization choices and missingness handling are fitted only within training.
	 Record the complete finite set of settings before the first experiment.
3. `T0/T1`: only after selection is understood, compare immediate entry with an
	 existing versioned orderly-pullback trigger and a simple delayed-entry control.
	 Freeze the waiting window and all trigger thresholds before evaluation. Use a
	 common decision-date endpoint for the primary timing comparison and report cash,
	 non-triggers and exposure differences; post-entry holding returns are diagnostic.
4. Participation, compression and additional geometry enter as individually declared
	 ablations, not a combinatorial threshold search. Event/fundamental features require
	 a separate historical-availability audit. Tree models are deferred challengers.

The first baseline portfolio is equal-weight within the highest-ranked decile,
rebalanced every 21 XNYS sessions on a phase fixed before the run. Daily scores do not
imply daily turnover. Alternate rebalance phases are recorded sensitivity tests, not
selected winners. Portfolio capacity and concentration limits must be fixed before the
portfolio stage; this specification does not imply suitability for any account size.

### Pattern information audit refinement

The review of Lo, Mamaysky and Wang (2000),
[Foundations of Technical Analysis](FOUNDATIONS%20OF%20TECHNICAL%20ANALYSIS.pdf),
adds an information audit after the historical panel and `M0` baseline, before
expanding the model. Distributional information is not evidence of executable alpha.
The primary 21-session selection endpoint and current detectors remain unchanged.

- Freeze a small detector/version, horizon and measurement family before examining
	results. Describe net/sector-relative mean and median, return quantiles and downside
	tails, forward realized volatility/range, and time decay. These are different
	questions, not multiple opportunities to select a favorable promotion criterion.
- Compare with the eligible population, then with contemporaneous controls matched
	on dated sector, momentum, trailing volatility and liquidity. Use only information
	known at the decision for matching. An association is not a causal effect.
- Distinguish detection completion, first confirmation and decision availability.
	Never smooth the full future price series and backdate a signal to an extremum.
- Treat empirical distribution differences as descriptive until dependence-aware
	inference is validated. Resample date blocks and retain cross-stock dependence;
	do not use independent-sample K-S p-values on overlapping conditional/unconditional
	rows. Include all declared tests in multiplicity accounting and keep null controls.
- Do not copy full-subperiod normalization, retrospective volume categories or
	average-capitalization selection into predictors. Fit transforms on past data and
	use point-in-time memberships. The paper's smoothing bandwidth is not a forecast
	probability, and its visual tuning does not justify a new production threshold.
- Compare models with/without a feature on later observations. Univariate absence
	of significance is not proof against a separately declared interaction, but neither
	is a distributional difference permission to promote an unprofitable model.
- Outcomes may justify selection, timing or risk research, or no predictive use.
	This audit adds no production confidence score or replacement qualification gate.

### Validation and publication gates

- Fix date ranges, train/validation/test boundaries, model settings, family membership,
	rebalance phase and cost scenarios after coverage audit and before viewing new
	performance. Until those fields are frozen, results are exploratory only.
- Keep dates intact in temporal folds. Fit transforms on training data only. Every
	training label must be available before the forecast; nominal horizon spacing is
	not a substitute for checking actual entry, exit and information times.
- Use null/permutation and oracle fixtures to test machinery. A failure to reproduce
	momentum is not, by itself, proof of a software bug or permission to tune to the
	expected published sign.
- Treat overlapping returns and same-date stocks as dependent. Use block-resampled
	daily portfolio returns or appropriate clustered/HAC inference for the new study;
	preserve non-overlapping diagnostics and report effective sample limitations.
- Record all attempted variants, missing/underpowered cells and selected settings.
	FDR family membership is declared, not inferred from only the surviving rows.
	The old one-scanner/two-direction/three-horizon/two-mode family has 12 cells, not 16.
- Carry exact outcome identity throughout. Until distinct cost scenarios have separate
	publication identities, reject multiple policy IDs under one policy key rather
	than averaging revisions or silently choosing the newest policy.
- Single-candidate probability requires a separate causal calibration study; the old
	40+100 independent-basket-period gate is not a universal sample-size formula.
- Costs start with a documented research assumption and predeclared stress scenarios,
	not a claim that flat 4 bps is executable. Actual spreads, impact and liquidity
	constraints must be established before deployment. Include dividends and financing
	consistently, without double-counting adjustments.
- Subsequent ranking must improve on the baseline out of sample with positive absolute
	net economics and uncertainty/stability evidence, not only a higher backtest score.
	Previously inspected dates remain development evidence. Genuine forward evidence is
	required before promotion; thresholds are not relaxed to manufacture a pass.
- Persist a new model/evaluation version and immutable forecast-time provenance.
	Publication selection is exact in model/source version, policy, side and horizon.
	Reject stale/missing inputs and allow no qualifying opportunity. Never overwrite
	historical studies or inherit a scanner qualification for a new combined model.

### Double review

Statistical review completed before implementation:

- Resolved the mismatch between portfolio probability and individual outcomes by
	separating selection, timing, risk and probability targets.
- Rejected anomaly replication as a mandatory profitable control; use mechanical
	null/oracle checks and admit the possibility of a locally unsuccessful baseline.
- Kept the existing history exploratory, dependency-aware inference mandatory and
	data-dependent split/settings selection blocked until a coverage audit.
- Kept timing non-triggers and cash in the original candidate cohort, and deferred
	stops/targets so selection and execution effects are not optimized together.

Integration review completed before implementation:

- Retain current public policy keys and immutable publications. First close policy
	mixing with a fail-closed guard and exact-ID provenance, not a schema migration.
- Keep legacy identity-less in-memory analytics compatible, but canonical persisted
	observations must carry policy identity. A supplied identity column cannot contain
	unknown or conflicting identities. Provenance-bearing reports get a new metrics
	version; existing report hashes remain stable for unchanged legacy inputs.
- Reuse the current repository/evaluator/tests. Do not add dependencies, restart
	workers, migrate data, republish studies, alter live ranks or touch options UI.
- Later cost sweeps must explicitly select policy IDs and validate the full hypothesis
	family. The first guard is intentionally not a completed cost-sweep framework.

### Implementation checkpoints

- [x] Design and two review passes recorded.
- [x] Slice 1: reject mixed/unknown policy identities; propagate exact IDs through
	repository reads and retained metrics; verify isolation and legacy determinism.
- [x] Slice 2a: explicit clocks, non-overlapping execution paths, causal calibration
	availability and finite-sample inference for timing-aware qualification.
- [x] Slice 2b: exact probability/qualification display contracts and version/policy/
	horizon matching; no individual-trade probability inherited from a basket.
- [x] Slice 3: daily rank/discovery refresh and freshness guards, tested without
	retroactively rewriting historical predictions.
- [x] Slice 4a: read-only retained-input inventory and readiness report; blockers
	recorded without changing research data or fitting a strategy.
- [x] Slice 4b diagnosis: classify all identity disagreements, reproduce the missing
	membership from retained source inputs, and reject duplicate writes/inconsistent resume.
- [x] Slice 4b importer guard: resolve IDs from exact-date reference caches in both
	member modes and block missing, ambiguous or conflicting historical identities.
- [x] Slice 4b transition proposal: verify DOC/PEAK merger terms against SEC filings
	and replay a source-pinned March 4 universe correction without publishing it.
- [x] Slice 4b universe revisions: additive schema, original-only compatibility reads,
	explicit cutoff/pinned selection and matching study/shard manifests.
- [x] Slice 4b price-reader foundation: exact dated security/bar selection, durable
	price manifests, opt-in security-keyed evaluation and a verified DOC/PEAK price slice.
- [x] Slice 4b action-reader foundation: response-bound SPLIT/DIVIDEND coverage,
	exact action manifests and four retained DOC/PEAK scope observations.
- [ ] Slice 4b: resolve identity, membership, total-return and missingness issues,
	then freeze machine-readable study settings. Current inputs are NOT_CERTIFIED.
- [ ] Slice 5: bounded all-eligible-security feature/outcome panel and `M0` baseline.
- [ ] Slice 5b: bounded pattern information audit with matched controls, explicit
	distributional targets and dependence-aware uncertainty, before feature expansion.
- [ ] Slice 6: `M1` ablation, declared family checks and later-period ranking report.
- [ ] Slice 7: timing controls and constrained portfolio/cost simulation.
- [ ] Slice 8: versioned prospective shadow predictions, operational monitoring and
	separate approval of any live promotion.

### Execution order from this checkpoint

Correctness/freshness foundation is complete through Slice 3, not the predictive system.
The information-audit design is recorded; no pattern audit or new model study has run.

1. Slice 4b (in progress): reconcile the input-audit blockers below, including dated security
	identity, the membership-count mismatch and dividend/delisting coverage. Re-audit
	the corrected lineage before freezing dates, finite settings, costs and test families.
	Ask the user before changing the mandate or silently excluding problematic history.
2. Slice 5: construct the all-eligible-security panel and reproduce `M0`, including
	no-signal controls and explicit missingness. Validate null/oracle and timing tests;
	accept that the economic baseline may fail.
3. Slice 5b: run the bounded pattern information audit using matched controls and
	dependency-aware uncertainty, without converting distributional significance into
	a trading recommendation.
4. Slice 6: test `M1` and predeclared feature ablations against `M0` on later periods;
	retain rejected variants and report uncertainty, turnover and stability.
5. Slice 7: evaluate timing, portfolio cash/concurrent positions, realistic costs,
	concentration and capacity. Confirm account/position constraints before sizing is
	treated as deployable rather than a research simulation.
6. Slice 8: publish immutable shadow predictions and monitor realized outcomes.
	Any live promotion remains a separate decision supported by forward evidence.

### Slice 1 verification

Implemented in `equity/qualification.py` and `equity/repositories.py`:

- Canonical observation reads return `outcome_policy_id` and accept an optional exact
	`outcome_policy_ids` filter in addition to existing source/cohort/key filters.
- Qualification rejects different policy IDs under the same public key before
	missing-return filtering, so incomplete rows cannot hide a conflicting revision.
- Supplied identities are non-null valid UUIDs; persisted observation frames cannot
	omit them. UUID object/string representations normalize to the same identity.
- Identity-bearing reports retain the ID, hash it into cohort/publication identity,
	and use `equity_qualification_metrics_v4`. Legacy identity-less in-memory inputs
	retain their existing v3 metrics and deterministic report hash.
- No policy-key rewrite, schema change, data migration, study publication, live rank
	refresh, worker restart or option/UI change was performed.

Validation on 2026-09-12: 88 qualification, repository and orchestration tests passed;
editor diagnostics and diff whitespace checks passed. An in-memory comparison against
the pre-change evaluator confirmed unchanged legacy fixture report hash and metrics.
A bounded live SELECT verified exact-ID filtering on three intraday outcomes and on
three daily outcomes; daily qualification also completed in memory with exact IDs.

The intraday sample exposed a pre-existing blocker for Slice 2: market time was
`2026-09-01T20:00:00Z`, while actual observation was
`2026-09-02T08:05:00.888689Z`. The query uses observation time as `signal_time`, but
intraday independence sampling requires an exact XNYS bar boundary. Both HEAD and the
modified evaluator raised the same boundary error. Separate market-bar identity,
decision/observation time and actual label availability in the timing slice; do not
snap observation time to a bar and thereby invent an earlier executable decision.

This slice prevents mixing; it does not implement CLI cost sweeps or republish old
reports. A changed canonical evaluation requires a new explicit evaluation version
and reviewed publication time, not reuse of an existing immutable publication key.

### Slice 2a verification and timing contract

The paper-inspired information audit is now part of the design, not an executed
study. No detector thresholds or selection endpoints changed.

Canonical qualification inputs now carry `signal_market_time`, actual observation
as `signal_time`, `entry_time`, `exit_time` and `outcome_available_at`. Persisted
inputs cannot omit these clocks. Supplied values must be known, timezone-aware and
ordered as `market <= decision < entry < exit <= availability`. Partial timestamp
columns are rejected rather than silently falling back to legacy calibration.

Same-market-bar signals form one equal-weight cohort. Its decision time is the latest
member observation, its execution span is the earliest entry through latest exit,
and its label is available only when all member outcomes are available. Reject a
cohort whose earliest entry precedes complete observation; do not backdate a decision
to make it executable. Horizon spacing uses market-bar time and additionally rejects
overlapping execution spans. Non-overlap reduces one source of dependence; it does not
establish IID observations or replace the later block-resampling validation gate.

Timestamp-aware calibration trains on labels whose stored availability is strictly
before each forecast. Equal-timestamp labels are excluded without event-order proof.
The 40-training-period requirement counts known labels, not prior rows. The algorithm
sorts label availability and uses cumulative win counts, rather than rescanning every
historical prefix. Final posterior/alpha summaries describe the supplied matured
sample at its evaluation cutoff, not forecasts retrospectively available at its start.
Historical reconstruction of availability still belongs in the input audit; timestamps
are not silently substituted to manufacture calibration history.

Timing-aware qualifications use two-sided Student-t probabilities with `periods - 1`
degrees of freedom, and record the timing, sampling, inference and basket-probability
contracts in `equity_qualification_metrics_v5`. Exact clocks enter cohort/report hashes.
Timestamp-less in-memory analytics retain their old calibration output and inference
for compatibility; these legacy paths are not certification for canonical live use.
New reports still require an explicit new evaluation version before publication.

Validation on 2026-09-12:

- 123 focused tests and six subtests passed across calibration, qualification,
	repositories, orchestration and outcomes. Coverage includes delayed/out-of-order
	labels, equality boundaries, incomplete clocks, overnight observations, cohort
	availability, actual path overlap and deterministic replay.
- The 40-period `t=2.01` regression now has `p=0.0513846` and `MONITOR_ONLY`, rather
	than the normal-approximation false-positive `ROBUST_PASS`, on the timed path.
- Bounded SELECT-only live probes evaluated three stored intraday and three daily
	outcomes in memory. The previously failing intraday sample retained market time
	`2026-09-01T20:00:00Z` and decision time `2026-09-02T08:05:00.888689Z` and passed.
- An in-memory comparison with the original committed code confirmed identical
	timestamp-less calibration outputs and legacy qualification metrics/report hashes.
- Editor diagnostics passed. No data migrations, study publication, worker restarts,
	live rank changes, model training or frontend edits were performed in this slice.

The earlier Slice 1 intraday clock blocker is resolved by this contract. Exact UI
evidence attachment, rank freshness, input auditing, pattern-information experiments
and predictive/portfolio model development remain separate unfinished checkpoints.

### Slice 2b verification and evidence display

Implemented in `equity/scanner_research.py`, `ScannerResults.tsx` and the shared
`pages/research/scannerEvidence.ts` matcher:

- Qualification reads pin the recorded outcome-policy ID and expose the source
	version, named horizon, policy identity status and publication ID/effective time.
	Different exact policy revisions are not collapsed into a single public-key result.
- Latest-signal reads expose the sector-primary outcome contracts applicable at the
	signal's actual observation time, with policy ID/key, mode and horizon. Source-name
	normalization is consistent between live signals and qualification responses.
- The board has explicit evidence return-policy and horizon selectors. Both filtering
	and rendering use one unique match on source/version, interval, direction, exact
	policy ID/key, horizon key/count and return mode. No best-result or first-calibrated
	fallback is permitted. Conflicting revisions show ambiguous evidence.
- Missing contracts, no applicable horizon, no exact study and API unavailability are
	distinct from an actual `UNRANKED` result. Older API responses lacking new fields
	fail closed rather than inheriting broad scanner evidence.
- Calibration claims require exact policy provenance, the causal timing contract,
	explicit cohort probability target, matching reported/curve OOS counts and the
	existing robust/Brier/ECE gates. The board labels `Cohort P(net win)` and identifies
	the selected horizon/mode; its tooltip explicitly excludes individual-trade meaning.
- Research rows are separated by exact policy ID/key, and duplicated horizon cells
	show ambiguity rather than selecting the first result. Legacy unpinned summaries
	remain visible as historical research but cannot supply an exact board match.
- Evidence is current published research, not a claim that the study was available
	when an older signal originally occurred. The board shows study effective time and
	keeps retrospective evidence distinct from historical decision-time qualification.
- Weekly horizons use weeks, matching the canonical weekly policy rather than the
	retired daily-session interpretation.

Validation on 2026-09-12: 20 backend evidence/API tests and six subtests passed;
17 frontend matcher tests passed using Node's built-in runner and the already-installed
TypeScript compiler. Frontend typecheck and production build passed. The 18 backend
warnings concern existing FastAPI deprecations, not this change.

Read-only SQL/API checks retained 12 current legacy qualification cells, zero pinned
policy identities, and six applicable contracts on each of three sampled signals.
Browser checks used real data and temporary synthetic responses: wrong-version and
wrong-horizon robust evidence was not inherited; an exact 21-session cohort displayed
its probability; selecting the stop/target policy suppressed the directional result.
Desktop (1440px) and mobile (390px) checks/screenshots passed without page-level
overflow. The mobile data table retains its internal horizontal scrolling. Browser
fixtures were removed after testing; no synthetic evidence was persisted.

An isolated development preview uses frontend port 5175 and API port 8002. Existing
services on 5174/8001 and all ingestion workers were left untouched. No migration,
study publication, rank refresh, detector/model change or option-UI edit was performed.
The normal API process must load the updated code before its clients receive these
new metadata fields; until then the updated frontend deliberately shows unavailable
exact evidence. The preview ports are session tooling, not deployment configuration.

Focused verification commands:

```powershell
.\backend\.venv\Scripts\python.exe -m pytest backend/tests/test_scanner_evidence.py backend/tests/test_equity_portal_reads.py -q --tb=short
node --test frontend/tests/scannerEvidence.test.mjs
npm.cmd --prefix frontend run build
```

### Slice 3 verification and optimizations

`scripts/refresh_daily_signal_context.py` coordinates the existing momentum and
discovery generators; it does not introduce a new signal or change thresholds.

- Resolve the latest observable XNYS daily close with the configured provider delay.
	Require a complete, published RTH unadjusted bar cohort for the active selected
	universe, with all current-session input bars visible at the observation cutoff.
- Load a single bounded panel for both generators. Each preserves its own existing
	lookback and scoring functions. Output dates must equal the requested session;
	older fallback dates, duplicate/foreign tickers and inadequate coverage fail closed.
	Feature eligibility retains the existing 90% coverage tolerance and 50-name floor;
	this is separate from the complete input-bar publication requirement.
- Acquire a transaction advisory lock and persist both outputs on the same cursor.
	Failure rolls back the pair. Partial existing snapshots are rejected, not repaired
	by overwriting earlier rows. Complete existing pairs skip loading/scoring/writes.
- Do not synthesize historical predictions for intervening missed dates, replace
	existing date/model rows or invoke the discovery history cleanup on this path.
	Standalone legacy generator commands retain their prior explicit write semantics;
	the new coordinator is the recurring-worker path.
- The equity worker invokes the coordinator after complete daily bar publication and
	before daily analysis. A separate retry path recovers missing context even when
	analysis is already current on restart. Context failures back off without starving
	other intervals. Snapshot rebuilds require matching daily context before computing
	their source manifest and dependent views.
- Current hourly review priority requires the pinned discovery model's state to have
	the latest observable session date. Missing/stale/future-dated context cannot raise
	priority. This remains a descriptive review tier, not predictive qualification.

Validation on 2026-09-12: 58 focused tests and 12 subtests passed. Tests cover exact
shared-panel scoring parity, rollback orchestration, duplicate-date no-ops, incomplete
publications, current-bar coverage, historical-backfill rejection, stale output dates,
restart recovery, execution ordering, failure backoff and delayed/holiday freshness.
Editor diagnostics passed.

The live dry-run passed for 386 published members and 256,296 panel rows. The authorized
latest-session refresh then published 377 `xsmom-1.0` rows and 376 discovery rows for
2026-09-11. Preparation/computation took 11.233 seconds on this host. The immediate
rerun returned `ALREADY_PRESENT` in 0.005 seconds without recomputing. Hashes of all
pre-2026-09-11 rows remained unchanged: 376 historical ranks and 376 historical discovery
states. Current tables now contain 753 and 752 rows respectively; missed sessions were
not backfilled. Read-only sampling of 20 hourly board rows confirmed only current-dated
context contributed review tiers, with missing context remaining unranked.
The context write invalidated the 20 dependent portal snapshots; a one-shot rebuild
completed, and a separate read verified all 20 snapshots fresh afterward. This rebuild
was materially slower than context generation and is a profiling candidate, not a
reason to weaken the data/evidence gates.

Recurring processes already running before this edit must load the updated worker code
on their next restart. No resident ingestion worker was restarted in this slice. The
latest-data refresh is not a qualification publication or evidence of increased alpha.

Operational commands, from the repository root:

```powershell
.\backend\.venv\Scripts\python.exe backend/scripts/refresh_daily_signal_context.py --dry-run
.\backend\.venv\Scripts\python.exe backend/scripts/refresh_daily_signal_context.py
.\backend\.venv\Scripts\python.exe backend/scripts/refresh_equity_portal_snapshots.py
```

Optimization priorities after this checkpoint:

| Candidate | Evidence or issue | Next action |
|---|---|---|
| Shared bounded panel and no-op completed dates | Implemented with scoring parity; 5ms measured rerun | Retain and monitor; no predictive claim |
| Shared engineered features/beta across models | Both models still independently build overlapping features | Profile before refactoring; require exact parity |
| Portal snapshot rebuild | Recomputes multiple scanner/streak surfaces; substantially slower than daily context refresh | Profile reads and repeated feature calculations before adding caches or changing strategies |
| Relative-volume definitions | Basic features include current volume in the 20-bar mean; composite detectors use the preceding mean | Record feature identity; compare as a declared ablation, not a silent replacement |
| Sector context and historical lineage | Current sector map covers 353 names; that is not proof of dated historical coverage | Audit before historical neutralization or predictive claims |
| Session continuity and corporate actions | Current refresh intentionally preserves existing unadjusted feature semantics | Audit splits/dividends/missing sessions before the new baseline study |
| Pullback depth, speed and participation | Existing descriptors are available, but incremental utility is unproven | Pattern information audit and matched timing controls |
| Costs and turnover | Flat costs and arbitrary entry refinements can misstate economics | Freeze cost scenarios and portfolio constraints before tuning |

No strategy parameter optimization, new machine-learning model, threshold sweep or
claimed robust pass was introduced. Correctness and operational speed are measured
separately from strategy efficacy.

### Slice 4a retained-input audit

The repeatable command `scripts/audit_equity_research_inputs.py` uses one PostgreSQL
`REPEATABLE READ, READ ONLY` transaction with per-statement timeouts. It performs no
provider fetches, database writes, model fitting or performance-based selection. The
generated report is [equity_research_input_audit.json](equity_research_input_audit.json).
Command success means the audit executed, not that the dataset is certified.

Measured on 2026-09-12:

| Check | Result | Interpretation |
|---|---|---|
| Universe continuity | 1,034 XNYS sessions, 2022-07-22 through 2026-09-03; no duplicate/missing dates | Structural coverage passes |
| Universe policy | One hash matching `liquid_us_common_stocks_v2` | No detected policy mixture |
| Membership integrity | 2024-03-04 declares 1,504 members but stores 1,503 | Reconcile before freezing the cohort |
| Historical daily lineage | Only split-adjusted; 3,139,383 ticker-sessions, 2,934 tickers, 2021-09-07 through 2026-09-03 | Do not silently mix with unadjusted live history |
| Signal-bar coverage | 1,604,820 of 1,605,114 membership observations have a same-ticker daily bar | 294 missing; availability counts include absent bars |
| Identity alignment | 48,862 member-date rows disagree with the selected bar's security ID | Ticker equality alone is not a safe join |
| Warm-up | 1,520,342 membership observations have 252 contiguous preceding session offsets; none on the first research date | First date reaching 90% warm-up is 2022-09-07, not an automatically approved start date |
| Forward 21-session paths | 7,142 missing contiguous paths among 1,565,472 mature membership observations | Distinct from 39,642 end-of-sample immature observations |
| Benchmarks | All 14 configured ETFs cover all 1,034 study sessions | Necessary coverage passes; prices are still not dividend-inclusive returns |
| Historical reference identities | 3,094 security/ticker identities; 175 lack any reconstructed daily bar for that exact identity | Some may have same-ticker bars under a different ID; investigate rather than discard |
| Sector references | 2,706 have sector data effective by first admission; none retain dedicated `source_as_of_date` | Effective dating is present, but provider-as-known lineage is not independently certified |
| Corporate actions | 24,728 dividend records and 309 split records through 2026-09-03 | Counts do not prove complete per-security negative coverage |
| Action coverage manifests | Overlapping reconstructed coverage starts 2024-07-01, later than the study | Full 2022-2026 completeness is not established |
| Delisting evidence | No latest reference in this cohort is marked inactive/delisted | Delisting settlement/terminal-return coverage remains unproven |

These are membership observations, not independent return samples or trading
opportunities. The audit has not computed strategy returns or inspected a test-set
performance metric. Price-path checks require contiguous exchange-session ordinals;
missing intermediate sessions cannot be hidden by taking the next 21 stored rows.

The identity mismatch is a join-integrity finding, not a claim that every affected
price is wrong. `normalize_security_reference` derives identity from composite FIGI,
then share-class FIGI, then CIK/ticker. Historical provider field changes can therefore
produce different IDs for the same economic security, while genuine ticker reuse can
also produce different securities. Do not bulk-relink IDs or overwrite immutable bars
until those cases are distinguished using dated issuer/share-class evidence.

The bounded first-session diagnostic confirms both categories: AGR and ANSS retain
matching CIK and share-class FIGI across the two records while composite FIGI differs;
BAM and BBBY have different CIK and share-class FIGI. The latter require explicit
corporate-history investigation, not automatic same-ticker relinking. These examples
identify metadata/join ambiguity; they do not independently establish which raw price
observations are economically incorrect.

Likewise, the historical sector backfill requests dated overviews, but the repository
writer does not persist the dedicated source-as-of-date column. The effective date is
not by itself independent proof of the provider's historical classification, and
current manual sector overrides must not become unreviewed retrospective labels.

Readiness remains `NOT_CERTIFIED`; study settings remain `NOT_FROZEN`. Before `M0`:

1. Diagnose the member-count mismatch and security-ID disagreements, then define a
	versioned repair/crosswalk that preserves original lineage and published studies.
2. Quantify missing-price paths by security/event, including terminal delisting and
	corporate-action cases. Predeclare the missingness policy; do not drop losing or
	untradeable names based on future data.
3. Establish dividend-inclusive stock and benchmark returns, or obtain explicit
	approval for a separate price-only diagnostic that cannot claim full net economics.
4. Verify dated sector/action provenance and select the admissible warm-up/maturity
	window based on coverage, not observed strategy performance.
5. Freeze the study configuration only after these decisions are documented and the
	corrected inputs pass the same read-only audit.

The information-audit and model stages remain pending. Input repairs, downloads and
database mutation are not part of the read-only audit itself.
Six focused audit tests passed, editor diagnostics passed, and the live audit completed
under database-enforced read-only snapshot semantics. The tests cover session gaps,
policy/count/availability errors, maturity versus missingness, and the transaction
boundary. The generated JSON records the actual database snapshot cutoff.

```powershell
.\backend\.venv\Scripts\python.exe backend/scripts/audit_equity_research_inputs.py --output docs/equity_research_input_audit.json
.\backend\.venv\Scripts\python.exe -m pytest backend/tests/test_equity_research_input_audit.py -q --tb=short
```

### Slice 4b identity and membership diagnosis

The read-only `--identity-review` mode now covers every mismatched identity group,
not just first-session examples. Its retained output is
[equity_research_identity_review.json](equity_research_identity_review.json).
Measured on 2026-09-12, the counts reconcile to all 48,862 affected member-date rows:

| Diagnostic category | Pair groups | Member-date rows |
|---|---:|---:|
| Same normalized CIK and share-class FIGI, both common stock: identifier-drift candidate | 109 | 36,478 |
| Known issuer, share-class or security-type conflict | 56 | 9,504 |
| Insufficient identity evidence | 17 | 2,880 |

These are diagnostic groups, not independent samples or authorized mappings. Latest
reference metadata does not prove historical continuity. A group's first/last date
does not authorize filling intervening dates or crossing corporate transitions.

The 2024-03-04 membership discrepancy is now reproduced from retained evidence:

- The dated reference cache passes its checksum and the stored source-request hash.
- DOC and PEAK normalize to the same security ID; only PEAK exists in the stored run.
- Replaying the original policy over 20 preceding XNYS sessions of checksum-verified
	unadjusted grouped caches yields 1,504 members and the exact original run UUID.
- DOC is missing at rank 1100. There are no extra stored tickers.
- The member insert's `ON CONFLICT (universe_run_id, security_id) DO NOTHING` could
	silently omit one row while the run declared the input length. New repository guards
	reject duplicate security IDs or tickers before writes; historical resume now rejects
	runs whose stored count differs from the declared count instead of trusting COMPLETE.

This establishes the missing row and collision, not whether the two symbols were
economically distinct on that date. No count adjustment, member insertion, ID relink,
bar rewrite or qualification republication has been performed.

The adjusted-bar importer had a separate confirmed identity defect:
[ingest_adjusted_daily_bars.py](../backend/scripts/ingest_adjusted_daily_bars.py)
selected today's reference IDs once and used that same ticker-to-ID map for every past
session, even with `--from-reconstructed-universes`. The dated-import checkpoint below
replaces this behavior; ordinary import still cannot repair the audited history.
Required repair steps:

1. Replace the current-ID historical mapping with dated, evidence-backed identity
	resolution. Cover warm-up and post-membership outcome dates explicitly; neither
	one current map nor carrying an admission ID indefinitely is adequate.
2. Resolve DOC/PEAK's dated transition and source inconsistency before issuing a
	versioned replacement universe. Preserve the original run, references and hash.
3. Distinguish identifier drift from issuer/share-class transitions with retained dated
	references and raw price/action evidence. Candidate matches are not repair approval;
	conflicting or incomplete evidence must remain explicit, not silently excluded.
4. Apply only reviewed, versioned corrections preserving original bars and studies;
	rerun the input audit before tackling the remaining return/missingness blockers.

The focused repository/audit tests cover duplicate write rejection, count-consistent
resume, classifier conflicts/missing evidence, cache checksum/request/policy failures,
causal eligibility replay, original run-ID reproduction and read-only transactions.
No provider fetches, database mutations or strategy fitting occurred in this checkpoint.
Slice 4b remains open; readiness is `NOT_CERTIFIED` and settings are `NOT_FROZEN`.

```powershell
.\backend\.venv\Scripts\python.exe backend/scripts/audit_equity_research_inputs.py --identity-review --output docs/equity_research_identity_review.json
```

### Slice 4b dated-import checkpoint

The adjusted daily importer now resolves each requested session independently from
checksum-verified `tickers-CS_<date>` and `tickers-ETF_<date>` caches using the existing
reference normalizer. `--include-non-common` additionally requires dated ETV caches.
`--reference-cache-dir` defaults to the existing historical research cache. The
importer itself never fetches references or falls back to today's metadata, an older
admission, or a future reference. Warm-up and post-membership dates have the same
exact-date requirement as study dates; current membership is only an optional ticker
selection source, not identity evidence or a point-in-time universe claim.

Before any price fetch/write, the importer resolves the requested dates, requires
dated benchmark references, rejects ticker-only identities and ambiguous duplicate
references, and checks existing adjusted reconstructed bars under a database-enforced
read-only snapshot. Conflicting stored IDs produce `VERSIONED_IDENTITY_REPAIR_REQUIRED`.
The bar persistence key omits security ID, so re-importing an unchanged provider payload
is not a repair: it could do nothing while leaving the old mapping in place. This
preflight deliberately refuses any retained conflicting IDs, including old revisions;
it is not a supersession or crosswalk implementation.

A failed preflight writes a `BLOCKED` report and returns nonzero, including in dry-run
mode. Successful preflight means only `READY_FOR_PRICE_VALIDATION`, never research
certification. Actual price responses must have exact-date identity for every selected
present ticker, no duplicate selected rows, and complete normalization. Unresolved
prices are not silently omitted. Missing exact-date references for an absent ticker
do not imply a delisting return or permission to fill prices. An unfinished exchange
session is rejected rather than persisted as a final daily bar. Actual price failures
stop before that session's write, but the existing importer remains session-incremental:
previous successful sessions are not rolled back. No production `--apply` was run.

The live bounded preflight for 2024-03-01 through 2024-03-05 is retained in
[equity_adjusted_identity_preflight.json](equity_adjusted_identity_preflight.json).
Three missing dated ETF lists were fetched separately through the existing provider
client/cache (3,252 / 3,254 / 3,253 references). With those inputs:

- The requested universe union contains 2,919 tickers plus 14 benchmarks.
- March 1 and March 5 resolve their references but have 108 and 107 tickers,
	respectively, with existing bar-ID conflicts. These counts cover the ticker union,
	not just the daily admitted membership used by the earlier 48,862-row audit.
- March 4 remains blocked by the DOC/PEAK identity collision.
- No grouped prices were fetched and no database rows were written.

A bounded individual-overview check retained five successful dated responses in the
same cache, separately from the original lists. It stopped at PEAK's 2024-03-05 HTTP
404. The response sequence shows a transition and an inconsistent transition-day
record; it does not authorize a repair:

| Requested Date/Ticker | Returned Name | CIK | Composite FIGI | Share-Class FIGI |
|---|---|---|---|---|
| 2024-03-01 DOC | PHYSICIANS REALTY TRUST | 0001574540 | BBG00JPYTK28 | BBG004MF5B76 |
| 2024-03-04 DOC | Healthpeak Properties, Inc. | 0001574540 | BBG000BKYDP9 | BBG001S5RTS2 |
| 2024-03-05 DOC | Healthpeak Properties, Inc. | 0000765880 | BBG000BKYDP9 | BBG001S5RTS2 |
| 2024-03-01 and 2024-03-04 PEAK | Healthpeak Properties, Inc. | 0000765880 | BBG000BKYDP9 | BBG001S5RTS2 |

All five responses report active status. These are provider reconstructions retrieved
on 2026-09-12, not evidence of what was published on the original dates. March 4 DOC
combines the old issuer CIK with Healthpeak's FIGIs, while PEAK also remains active.
Preserve these contradictory inputs. The source review below establishes the merger
date and stock-conversion terms, but terminal/continuation returns still require
settlement and price-lineage work. Do not change the global
UUID algorithm, adjust the declared count, or insert DOC under a guessed new ID.

The importer regression suite covers exact-date ticker reuse, refusal of past/future
reference fill, cached-source integrity, collision detection, both member modes,
benchmark/non-common handling, missing/duplicate/malformed prices, final-session
timing, dry-run reports and prevention of writes on preflight failure. Shared
normalization, repository persistence semantics and existing reports are unchanged.
Readiness remains `NOT_CERTIFIED`; settings remain `NOT_FROZEN`.

```powershell
.\backend\.venv\Scripts\python.exe backend/scripts/ingest_adjusted_daily_bars.py --start 2024-03-01 --end 2024-03-05 --from-reconstructed-universes --output docs/equity_adjusted_identity_preflight.json
.\backend\.venv\Scripts\python.exe -m pytest backend/tests/test_ingest_adjusted_daily_bars.py backend/tests/test_equity_polygon_ingestion.py backend/tests/test_equity_materialization_repositories.py backend/tests/test_prepare_historical_signal_research.py backend/tests/test_equity_research_input_audit.py -q --tb=short
```

### Slice 4b source-verified transition proposal

The retained manifest [equity_doc_peak_transition.json](equity_doc_peak_transition.json)
separates two events that cannot be represented by one ticker-based relink:

| Event | Verified Terms | Research Treatment |
|---|---|---|
| Physicians Realty merger, March 1, 2024 | Old DOC shares were cancelled and converted into rights to 0.674 Healthpeak common shares each; cash in lieu of fractional shares | Preserve old DOC identity and model consideration for positions held through the merger; do not treat it as a split or zero terminal return |
| Healthpeak symbol change, March 4, 2024 open | Healthpeak expected to begin trading as DOC instead of PEAK | Healthpeak's pre-change history is PEAK, not Physicians Realty DOC; the symbol change alone requires no share-price conversion |

Primary sources reviewed on 2026-09-12:

- [Healthpeak closing 8-K, Items 2.01 and 8.01](https://www.sec.gov/Archives/edgar/data/765880/000110465924029661/tm247564d1_8k.htm).
- [Physicians Realty closing 8-K, Items 2.01, 3.01, 3.03 and 8.01](https://www.sec.gov/Archives/edgar/data/1574540/000110465924029662/tm247597d1_8k.htm).
- [Healthpeak's filed March presentation, page 3](https://www.sec.gov/Archives/edgar/data/765880/000110465924029669/tm247564d2_ex99-1.htm), corroborating the closing and symbol-change dates.

The target filing confirms the delisting request and share cancellation, but does not
give an exact trading-stop time. The NYSE Form 25 web extraction was incomplete and
a direct XML request returned HTTP 403. The manifest therefore leaves the final old
DOC trading session and fractional-cash settlement price null. A stale provider
`active=true` record or an HTTP 404 is not a substitute for those facts.

The audit now accepts `--transition-manifest`. It requires exactly one original run,
checksum/request-verified references, the original policy hash, the complete preceding
20-session XNYS window and exact reproduction of the original run ID before producing
a candidate. The candidate is restricted to the symbol-change effective session. It
checks the reviewed CIK/FIGIs against both source rows, removes stale PEAK from that
day's reference universe, uses a copied Healthpeak reference under DOC, and recomputes
eligibility using copied PEAK price history. Missing PEAK observations cannot fall
back to old DOC. No input dictionaries, retained caches or database rows are changed.

Live read-only result for 2024-03-04 is retained in
[equity_doc_peak_correction_review.json](equity_doc_peak_correction_review.json):

- Original source replay: 1,504 admissions, with the already diagnosed collision and
	missing rank-1100 DOC storage row.
- Corrected candidate: **1,503 distinct listings**, with **Healthpeak DOC at rank 530**,
	20 observed sessions, previous PEAK close **$17.10**, and median daily dollar volume
	**$123,089,185.255**. Stale PEAK is removed from that session's proposed listing set.
- This is not a proposal to insert old DOC as the 1,504th tradable stock. The earlier
	missing-row diagnosis described the original source replay, not economic eligibility.
- Candidate version, UUID, original run ID, policy/reference/history/evidence hashes
	and a hash of the complete computed member manifest are retained. Repeating unchanged
	inputs reproduces the candidate; changing the evidence changes its ID. The outer
	report hash also includes the read-only database snapshot cutoff and is not repeat-stable.

This proposal does not certify the provider's historical prices or as-known metadata.
For example, the copied source retains its provider `last_updated_utc` from December
2024; that metadata is not backdated or asserted to have been available in March.
The result is explicitly `PROPOSAL_ONLY`, `repair_authorized=false`, `NOT_CERTIFIED`.

Publication was blocked at the proposal checkpoint for these integration reasons:

1. The universe schema/readers lacked an explicit supersession-selection contract.
	The universe-revision checkpoint below addresses this prerequisite; no corrected
	universe has been published yet.
2. Existing bars remain under their original IDs. A symbol mapping is not permission
	to bulk-relink ticker-reused history; reviewed bar revisions must retain their originals.
3. Earlier old-DOC positions need the merger consideration, valid successor prices,
	dividends and fractional-cash treatment. No new outcome or strategy return was computed.

Focused tests cover deterministic candidate identity, unchanged input objects, rejection
of unsupported dates/identifiers/issuers/windows, no old-DOC fallback, unambiguous
original selection and refusal when the original run ID cannot be reproduced. The
broader importer/normalizer/repository/preparation tests are retained as integration gates.

```powershell
.\backend\.venv\Scripts\python.exe backend/scripts/audit_equity_research_inputs.py --transition-manifest docs/equity_doc_peak_transition.json --output docs/equity_doc_peak_correction_review.json
```

### Slice 4b universe-revision checkpoint

Migration [039_equity_universe_revisions.sql](../backend/migrations/039_equity_universe_revisions.sql)
has been tested and applied to the local database. Its exact SQL is also included in
the canonical baseline for fresh installations. It adds nullable parent revision ID,
publication time, evidence hash and reason columns. Existing runs remain originals.
No historical universe/member/bar rows were rewritten and no correction was inserted.

Database-enforced rules:

- A correction is a complete reconstructed REPLAY run with complete revision metadata.
- It must retain its parent's policy/version, effective session and replay-availability
	time. Correction publication follows the parent's observation/publication and cannot
	be in the future. Publication time is separate from historical replay availability.
- Each parent has at most one successor; corrections cannot branch or be created by
	converting an original row in place. Published corrections and referenced parents
	cannot be updated or deleted.
- Existing members in that lineage cannot be updated, moved or deleted, and a
	superseded original cannot receive additional members. Deferred constraints verify
	that a correction's final member count is complete and its tickers are distinct.

`equity_original_universe_runs` is an original-only compatibility view. Legacy live,
replay-resume, reference-backfill, importer and audit queries now use that view, so a
future correction cannot silently enter those unversioned paths. Startup schema checks
require the view. Processes retaining old imported code must be restarted before any
future correction publication; no resident worker or server was restarted here.

`EquityUniverseRepository.list_reconstructed_revisions` validates the revision graph
and selects one run per policy/session. Original-only is the default. An explicit aware
`revision_cutoff` selects the last correction published by that cutoff; it never uses
the historical replay clock to backdate correction availability. Exact run-ID pins
select those versions without upgrading them. Pins must cover the requested sessions;
unknown/duplicate pins, multiple roots, branches, policy mixtures and incomplete
selected runs are rejected.

The historical signal CLI adds mutually exclusive `--universe-revision-cutoff` and
`--universe-manifest` options. Reports retain each selected run ID, session, policy hash
and a selection hash before ticker filtering/sharding. A prior report's manifest can be
reused with the same requested date range. Membership reads join only those selected
IDs. Shard merging rejects inconsistent/tampered manifests, mixed legacy/pinned reports,
and events whose universe ID does not match the pin for their date; successful merges
retain the manifest. All-legacy shard reports remain readable, without claiming pins.

Corrected runs are selectable through the repository, but the signal runner explicitly
rejects them until its price reader supports version-aware security identities. This
prevents a corrected Healthpeak DOC universe from being evaluated on old Physicians
Realty DOC prices. The full historical replay also now fails on the known March 4
original count mismatch rather than silently accepting that cohort. No study was run.

Verification on 2026-09-12:

- 167 focused repository, universe, runner/merge, importer, audit and schema tests passed.
- 17 PostgreSQL contract tests passed using explicit opt-in and always-rolled-back
	transactions, including their migration DDL and fixture records.
- The local migration is registered and its view is accessible to the runtime role.
- Runtime inventory remains **1,034 historical originals and zero corrections** for
	`liquid_us_common_stocks_v2`. March 5 default, cutoff and exact-pin reads select the
	same original run. The DOC/PEAK proposal still reproduces the same candidate ID and
	1,503-member manifest through the compatibility view.
- PostgreSQL does not infer base-table primary-key functional dependencies through a
	view. Audit grouping clauses were made explicit after a live query exposed that issue.
- Both full original-input and identity audits then completed under enforced read-only
	transactions: 1,034 sessions, 1,605,114 memberships, the same March 4 count mismatch
	and 48,862 identity disagreements. No original audit findings disappeared through
	revision filtering.

The price-reader foundation is implemented in the checkpoint below. Complete study
price/action selection and the reviewed publication transaction remain pending.
The existing `persist_complete_run` remains an
original-run writer; no generic correction-writing shortcut or live promotion was added.
Old-DOC merger consideration, dividend/cash treatment, the remaining identity groups
and full coverage still need resolution before corrected research inputs are certified.
Readiness remains `NOT_CERTIFIED`; study settings remain `NOT_FROZEN`.

```powershell
.\backend\.venv\Scripts\python.exe backend/scripts/apply_incremental_migration.py backend/migrations/039_equity_universe_revisions.sql --verify-table equity_original_universe_runs
```

The optional database test suite is
[test_equity_universe_revision_database.py](../backend/tests/test_equity_universe_revision_database.py), enabled only when
`TEST_UNIVERSE_REVISION_DATABASE=1`; it uses configured administrator credentials to
test and roll back the migration. No credentials are included in reports.

### Slice 4b security-aware price reader

[historical_prices.py](../backend/equity/historical_prices.py) reads immutable daily
bar revisions using an explicit `(source ticker, session) -> security ID` map and an
identity-evidence hash. It never changes a stored bar's security ID, fills an absent
symbol from another ticker, or picks an older matching ID to hide a newer conflict.
The source cutoff applies to both system observation and database creation time;
historical replay availability remains a separate clock. Exact pins select those
revisions, not the latest available values.

Selection requires final reconstructed RTH bars, the requested adjusted flag, exact
ticker-normalizer lineage, XNYS session-close availability, valid OHLCV, and one
unambiguous source per security/session. Missing requested prices, identity conflicts,
ambiguous revisions and invalid clocks/prices fail closed. The reader preserves raw
bar IDs and source tickers while grouping returned histories by security ID.

The price manifest retains each source ticker/date, security ID, bar revision ID and
payload hash, plus the cutoff and identity-evidence hash. `reread_historical_prices`
validates its checksum and contract, then reproduces the exact selection from retained
rows. Missing pins or mismatched source metadata cannot silently select replacements.
A manifest is a reproducibility contract, not independent proof of economic identity
or provider-as-known availability; the dated identity mapping still requires review.

`evaluate_historical_signals` now has an opt-in security-aware input path. It requires
complete dated member identities, single-security frames, security-keyed action context
and price/action manifest hashes together; mixing them with ticker-only inputs is rejected. It
passes the applicable listing ticker to adapters without changing the history's owner.
Action context is restricted to the same security and no later than the signal date.
Events retain the full evaluated bar-ID window, security ID and price/action manifest hashes;
their new IDs incorporate that provenance. Contiguous-signal suppression cannot merge
different securities merely because their ticker matches. Legacy input behavior remains.

The existing audit command's `--transition-prices` option verifies 20 preceding dated
PEAK references and the reviewed March 4 DOC correction, then reads the associated
prices inside a database-enforced read-only snapshot. It runs no detector or strategy.
Result retained in [equity_doc_peak_price_review.json](equity_doc_peak_price_review.json):

- **21 bars, one Healthpeak security**: 20 PEAK bars from February 2 through March 1,
	2024, followed by DOC on March 4. Old Physicians Realty DOC prices are not substituted.
- Last warm-up close **$17.10**; March 4 close **$16.85**. These are selected prices,
	not an evaluated strategy or dividend-inclusive return.
- All stored bar IDs agree with the reviewed identity map for this bounded slice.
	An exact-pin reread within the audit and another independent read-only transaction
	reproduced the same 21-revision manifest.
- Status is `IDENTITY_ALIGNED_SLICE`, not whole-universe certification. The prior
	48,862 mismatched membership observations elsewhere have not been repaired.

The general study CLI still rejects corrected universe execution. It does not yet
assemble a complete reviewed price/action manifest for every selected security, and
this slice does not supply merger consideration or a forward return path. The legacy
outcome CLI now also rejects security-aware events before persistence/evaluation rather
than silently pricing them through its ticker-only path. No gate was removed merely
because the new reader and evaluator API tests pass.

Verification: **203 focused tests passed**, including legacy adapters/outcomes, exact
price pinning, ticker reuse, future-price truncation for the tested adapter, action
isolation, source-cache failures and blocked outcome routing. Editor diagnostics and
patch checks passed. No provider fetches, database/schema writes, correction publication,
qualification changes, model fitting or worker restarts occurred in this checkpoint.
Readiness remains `NOT_CERTIFIED`; settings remain `NOT_FROZEN`.

```powershell
.\backend\.venv\Scripts\python.exe backend/scripts/audit_equity_research_inputs.py --transition-manifest docs/equity_doc_peak_transition.json --transition-prices --output docs/equity_doc_peak_price_review.json
```

### Slice 4b response-bound action coverage

The existing coverage checksum identified the request scope only, not the response
membership. Such a record cannot distinguish a genuinely empty response from lost
action links. Legacy records remain readable for their existing consumers, but the new
historical action reader rejects them as `LEGACY_SCOPE_ONLY_COVERAGE`; they were not
retroactively upgraded.

Migration [040_equity_action_coverage_responses.sql](../backend/migrations/040_equity_action_coverage_responses.sql)
adds nullable `security_id`, `response_action_count` and `response_sha256` to coverage.
It was tested and applied locally; identical SQL is included in the fresh baseline.
The response hash covers the sorted hashes of all raw provider rows returned for that
ticker/type/window. A zero count plus the empty-response hash is explicit negative
evidence for that snapshot and scope, not a permanent assertion about the security.

New corporate-action worker observations bind those fields to the raw fetched response.
The repository validates scope, issuer identity, raw payload hashes, normalized count,
unique action IDs and unique provider keys before publishing coverage, then validates
the actual stored members. A skipped normalization, missing link, pre-existing action-ID
collision or conflicting provider revision cannot quietly become successful coverage.
`persist_observation` writes actions and coverage in one transaction. Database guards
keep response-bound coverage, membership and referenced actions immutable, prohibit
in-place promotion of legacy records, and defer scope/count validation to transaction
completion. No resident worker was restarted; new recurring capture behavior loads at
its next restart.

[historical_actions.py](../backend/equity/historical_actions.py) selects observations
for explicit dated-security intervals and requested action types at a storage cutoff
or by exact coverage pins. It checks the entire captured response before filtering to
the requested subinterval. It rejects missing coverage, unloaded membership, ambiguous
observations, changed identity, invalid availability, and actions first observed after
their coverage. It does not fall back to an older valid observation to hide a newer
conflict. Coverage/action snapshots and the identity-evidence hash are retained in
`RESPONSE_BOUND_ACTIONS_V1` manifests; exact rereads validate all retained metadata.

The service currently covers only SPLIT and DIVIDEND. It rejects requests to infer
MERGER, SYMBOL_CHANGE, SPINOFF or OTHER completeness from those endpoints. A known
SEC merger or symbol-change fact is positive evidence, not proof of a complete event
inventory. Security-aware event IDs now include the action-manifest hash as well as
the price-manifest hash, so changed action evidence cannot retain an old event identity.

The bounded live action review initially found four missing coverage scopes. Their
responses were fetched separately into the existing historical cache, the pinned price
manifest was reverified, and one atomic transaction appended **four reconstructed
coverage observations and one membership link** on 2026-09-12. The dividend action
already existed with matching identity/payload, so **zero action rows were inserted**.
No original row, universe, price, qualification or prediction was rewritten.

| Dated Security Scope | SPLIT Response | DIVIDEND Response |
|---|---|---|
| Healthpeak PEAK, 2024-02-02 through 2024-03-03 | Explicitly empty | One $0.30 dividend, ex-date February 13, record date February 14, pay date February 26, declared January 31 |
| Healthpeak DOC, 2024-03-04 | Explicitly empty | Explicitly empty |

The PEAK scope includes the calendar days through March 3, not just trading sessions;
it does not fabricate weekend prices. All four selections now report `RESPONSE_BOUND`
with matching exact rereads in
[equity_doc_peak_action_review.json](equity_doc_peak_action_review.json). Coverage
observation time is the actual 2026 retrieval, not a fabricated 2024 publication time.
The response hash and new security binding are not independent provider-as-known proof.

Verification: **226 focused tests** and **27 rollback-only PostgreSQL contract tests**
passed, including atomic rollback, complete/empty response persistence, idempotency,
stored-ID conflict detection, immutable evidence, cutoff/pin selection, source checksum
failures and action-manifest event identity. The real read-only audit passes all four
SPLIT/DIVIDEND scopes; editor diagnostics and patch checks pass.

The overall action review remains `BLOCKED` for unsupported event completeness and
settlement semantics. The general study CLI and legacy outcome guards remain closed
for corrected security-aware studies. Full reviewed input selection, the correction
publication transaction, merger consideration/fractional cash and dividend-inclusive
outcomes still need implementation. Readiness is `NOT_CERTIFIED`; settings remain
`NOT_FROZEN`. No model fitting or strategy-performance run occurred.

```powershell
.\backend\.venv\Scripts\python.exe backend/scripts/apply_incremental_migration.py backend/migrations/040_equity_action_coverage_responses.sql --verify-table equity_corporate_action_coverage
.\backend\.venv\Scripts\python.exe backend/scripts/audit_equity_research_inputs.py --transition-manifest docs/equity_doc_peak_transition.json --transition-actions --output docs/equity_doc_peak_action_review.json
```

## Historical Incident Backlog

Work plan derived from operating the scanner research pipeline end to end on 2026-09-03/04: two
full runs of the `level_retest_rejection` daily study, one of which produced zero publishable
output. Every item below is something that actually happened, not a hypothetical.

Priorities are by consequence, not effort:

- **P0** produced, or would have produced, a wrong or empty result without saying so.
- **P1** cost hours of wall-clock time to a crash or a restart.
- **P2** is throughput.
- **P3** is methodology strength.
- **P4** is ergonomics and retention.

The architectural target - one manifest, one driver, `study_id` on every working row - is described
in the "Study Driver Simplification" section of
[SCANNER_RESEARCH_CONSOLIDATION_DESIGN.md](SCANNER_RESEARCH_CONSOLIDATION_DESIGN.md). This document
is the ordered backlog that gets there.

---

## P0 - Silent wrongness

### 1. Preflight the study before committing to a long run

The first full run spent about 90 minutes evaluating 1,019,802 outcomes and published **zero**
qualification revisions. Cause: benchmark ETFs were missing from the adjusted lineage, so every
`sector_net_alpha` was null and `dropna` emptied the frame. Nothing failed loudly until the very
end.

Add a `preflight` subcommand that refuses to start unless:

- every benchmark ticker resolves in the lineage the study will read;
- the cohort has bar coverage over the requested window in that lineage;
- the entitlement window covers the first requested session;
- the supporting foreign-key indexes exist;
- no policy conflict exists for the declared horizons.

Then, once evaluation begins, verify on the first batch that `sector_net_alpha` is actually
populated and abort within seconds if it is not. A 100-row check would have caught this instantly.

### 2. Benchmarks are not cohort members

`ingest_adjusted_daily_bars.py` filtered to `security_type = 'CS'`, which is correct for study
subjects and silently excluded SPY, QQQ and all twelve sector ETFs - the series alpha is measured
against. `--from-reconstructed-universes` compounded it, because the universe policy is
`security_types=("CS",)` so benchmarks were never candidates.

Fixed by always unioning `BENCHMARK_TICKERS` and hard-failing when a benchmark reference is
missing. Generalise the rule: any ticker required to *score* a study must be ingested regardless of
cohort filters, and its absence must be an error rather than a null.

### 3. Pin the bar lineage explicitly everywhere

`run_historical_signal_research.py` selected daily bars with no `adjusted` predicate. Because
`replay_available_at` is identical across lineages, `DISTINCT ON ... ORDER BY created_at DESC`
would have silently mixed adjusted and unadjusted bars across tickers once both existed - invisible
in any output.

Fixed with an explicit `adjusted = %s` and a `--adjusted` flag, and the run report now records
`bar_lineage`. Audit every remaining bar read for the same omission, and consider making `adjusted`
a required argument rather than a defaulted one.

### 4. Reconcile bar identity with the uniqueness constraint

`normalize_grouped_daily_bars` derives `bar_revision_id` without `adjusted`, while
`EquityBarRepository.persist` upserts on a key that *includes* `adjusted`. The schema intends both
lineages to coexist; the identity prevents it. Polygon returns byte-identical payloads for both
lineages on any unsplit ticker, so the second insert dies on the primary key.

Include `adjusted` in the identity. This renames every existing grouped-daily row, so it belongs in
a rebuild - see [FRESH_DATABASE_SETUP.md](FRESH_DATABASE_SETUP.md). Until then, one lineage per
study, enforced by preflight.

### 5. Do not let defaults write into the permanent record

`--evaluation-version` defaults to `gap_formation_daily_qualification_v1` and is stored verbatim in
the retained qualification revision. A composite study would have been published permanently
labelled as gap formation. `--source-version` has the same gap-shaped default, and
`--horizon-sessions` is accepted then ignored on the composite path.

A guard now rejects gap-prefixed evaluation versions for composite sources. Better: derive these
from the study manifest so they cannot be wrong, and reject arguments a code path ignores rather
than accepting them silently.

### 6. Declare the FDR family, and enforce it

`CompositeScannersDailyAdapter` emits all seven registered scanners. Qualifying the unfiltered
events file would have produced a 112-lane family instead of the declared 12, making a real effect
roughly seven times harder to detect - with no warning.

The runner filters events to the studied scanner at stage 5. Move this into the manifest so the
family size is declared, checked against what is actually present, and recorded in the published
metrics.

### 7. Escape literal `%` in parameterised SQL

`qualification_report` raised `IndexError: tuple index out of range` on every call with an interval
because `'CONTROL\_%'` contains a literal `%` that psycopg2 treats as a placeholder. The
scanner-research endpoint returned 500 to the UI. Needs `%%`, and a raw string so `\_` is not an
invalid Python escape.

Add a lint or test that exercises every parameterised query with arguments; the unit suite passed
throughout because nothing called this path with a database.

---

## P1 - Long runs that fall over

### 8. Retry transport failures, not just HTTP status codes

`prepare_historical_signal_research.py` contained no error handling whatsoever - a search for
`retry|backoff|HTTPError|except|sleep` returned nothing - across roughly 2,300 Polygon calls. It
died at universe 174 of 1,034.

The first fix caught only `requests.HTTPError`; the run then died again on a
`ChunkedEncodingError` when a connection dropped mid-stream. Retry `ChunkedEncodingError`,
`ConnectionError` and `Timeout` alongside 429 and 5xx, and treat 403 as terminal with an actionable
message.

Two related lessons worth keeping: retry code only executes when something has already gone wrong,
so it is never exercised on the happy path and needs its own test. And `time` in that module is
`datetime.time`, so `time.sleep` would have failed precisely when the retry mattered.

### 9. Guard the numeric range on adjusted prices

Cumulative reverse splits can push back-adjusted prices past `numeric(20,8)`. Mullen Automotive has
split roughly 1:1e12, so its 2021 sessions adjust to about 17.5 trillion dollars per share and the
ingest died with `numeric field overflow`.

Now skipped and reported as `bars_out_of_range` with a per-ticker breakdown rather than crashing or
rounding into range. Apply the same treatment to any other numeric column fed by provider data.

### 10. Never swallow a traceback

The study runner used `$ErrorActionPreference = 'Stop'` with `2>&1`, so the first stderr line
became a terminating error and killed the pipeline before `Tee-Object` wrote it. A failure at stage
1 looked like a clean finish at universe 1034 with no error anywhere in the log.

Fixed by scoping `ErrorActionPreference` to `Continue` around the native call and relying on
`$LASTEXITCODE`. Any future runner needs the same care.

### 11. Stream large intermediates

`load_events` read the entire events file with `read_text().splitlines()`, and
`publication_metadata` then read it again via `read_bytes()`. Against the 199 MB filtered file that
is roughly 1.3 GB of peak memory for no reason. Now streamed and hashed in chunks. Better still,
remove the file: keep events in the working table.

---

## P2 - Throughput

### 12. Batch the sector lookup

The outcome loop issues `SELECT * FROM equity_security_reference_revisions WHERE
security_revision_id = ...` once per subject. At 171,804 subjects across six policy/horizon
combinations that is a large multiple of a single batched query, and it is the main reason
evaluation takes about 90 minutes.

### 13. Fix the outcome path cache eviction

`outcome_path_cache_size` is 100,000, but a study of this shape needs roughly 515,000 distinct keys
- subject times three benchmark tickers. When full, the cache calls `.clear()` and discards
everything rather than evicting the oldest entry, so hit rates collapse. Use an LRU, and size it
from the study rather than a constant.

### 14. Index the foreign keys research deletes through

PostgreSQL does not index foreign keys automatically. A 1.9M row purge ran 2,847 seconds without
completing; after adding two indexes the same purge finished in 263 seconds.
`scripts/check_unindexed_foreign_keys.py` reports 49 such keys. Six are now in the canonical schema;
verify the set covers every delete path research uses.

---

## P3 - Methodology

### 15. Reproduce a known anomaly

The single highest-value validation, and not yet done. Run the harness over the same cohort and
window on a documented effect - 12-1 momentum or short-term reversal - and check it recovers the
published sign and rough magnitude.

If it cannot detect momentum, the harness has a defect. If it can, every `UNRANKED` verdict becomes
substantially more credible, because the machinery has been shown to find real effects in data
where the answer is externally known. This is stronger evidence than any additional statistic and
costs one study run.

### 16. Factor-adjusted alpha, not only sector-adjusted

Sector-ETF alpha does not remove size, value, momentum or short-term reversal loadings. A
level-retest scanner very likely loads on short-term reversal, which is a known and freely
harvestable factor. Regressing on Fama-French 5 plus momentum and reporting the intercept *and*
loadings is the institutional standard. Ken French's data library is free, so this is cheap.

### 17. Raise the hurdle, and account for the real search space

Harvey, Liu and Zhu argue `t > 3.0` for a new factor. The current gate is `t > 2` plus BH across the
declared family, but the declared family is far smaller than the search actually performed across
this codebase's history - seven scanners, four intervals, multiple versions. Consider a higher
hurdle, and implement the Deflated Sharpe Ratio and PBO via CSCV, which
[SCANNER_LITERATURE_REVIEW.md](SCANNER_LITERATURE_REVIEW.md) already acknowledges are absent.

### 18. Standard errors that respect overlap

Overlapping horizons induce autocorrelation across periods. Plain t-statistics assume independence
once signals are collapsed by instant. Newey-West or a stationary bootstrap is the standard remedy.

### 19. Costs beyond a flat 4 bps

No spread, market impact, borrow cost or capacity. The short lanes on small caps are the most
exposed: hard-to-borrow fees can invert a marginal result outright.

### 20. Port regime-conditioned alpha

[SCANNER_ENHANCEMENTS_BACKLOG.md](SCANNER_ENHANCEMENTS_BACKLOG.md) records this as implemented, but
that was in the retired `research/scanner_events.py`. It was lost in the migration and
`regime_alpha` is still returned empty. The prior implementation found that this very scanner had
positive alpha in trending regimes and negative alpha in choppy ones - exactly the kind of finding
the current runtime cannot reproduce.

---

## P4 - Retention and ergonomics

### 21. Stamp `study_id` on every working row

Purge is currently keyed by `source_name` prefix, which collides with production for every
composite scanner because live capture and historical replay share detector names. The guard
correctly refuses, so `--exclude-production` was added as a workaround. A `study_id` makes purge
exact, safe and independent of naming.

### 22. Per-study partitions

With `study_id` present, cleanup becomes `DROP PARTITION`: constant time, no foreign-key
revalidation, none of the index work in item 14.

### 23. Recomputation requires marking stale

`list_pending_directional_subjects` skips any subject holding a non-stale outcome, so correcting an
input is invisible until the affected rows are marked stale.
`scripts/mark_research_outcomes_stale.py` now does this by source name. Preflight should detect the
condition and tell the operator, rather than leaving a re-run to silently do nothing.

### 24. Version the study artifacts

`composite_qualification.json` is unversioned, so the next composite study overwrites it with no
supersession trail - unlike the database, which revisions. Include the scanner, version and report
identity in the filename.

### 25. Compute for the future at qualification time

Because working rows are discarded after publication, anything a later consumer needs must be in
`metrics` when the study runs. `equity_qualification_metrics_v3` added breadth, concentration,
Wilson hit-rate interval, sector t-statistic and the tested window. The option-relevant additions
in [OPTION_RESEARCH_DESIGN.md](OPTION_RESEARCH_DESIGN.md) - move-size distribution, realised versus
implied volatility, time to target - should be added *before* the next study we intend to keep,
otherwise every study between now and then is unusable for that purpose.

---

## Suggested order

1. Items 1-7. Nothing else matters if results can be silently wrong.
2. Item 15. Establish that the harness detects a known effect.
3. Items 8-11, then 21 and 23. Make long runs survivable and repeatable.
4. Items 12-14. Turn 90-minute evaluations into minutes.
5. Items 16-20 and 25. Strengthen the claim and future-proof the retained record.
6. Items 22 and 24. Cleanup ergonomics.
