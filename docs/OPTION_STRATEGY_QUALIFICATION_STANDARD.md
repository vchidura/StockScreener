# Option Strategy Qualification Standard

## Purpose

This document defines how AlphaScreener decides whether an option strategy is likely to
work. It is the common qualification contract above individual strategy designs.

A strategy does **not** qualify because its rationale is plausible, it finds many
candidates, its raw return is positive, or its win rate is high. It qualifies only when
the exact structure selected by the strategy produces a repeatable, cost-adjusted,
out-of-sample advantage over an appropriate control.

This standard answers four questions:

1. What claim is the strategy making?
2. What evidence would support or disprove that claim?
3. When is the evidence strong enough to change selection policy?
4. What additional evidence is required before paper or live execution?

Implementation state belongs in
[OPTION_PIPELINE_CURRENT_STATE.md](OPTION_PIPELINE_CURRENT_STATE.md). Detailed research
rationale belongs in [OPTION_RESEARCH_DESIGN.md](OPTION_RESEARCH_DESIGN.md). This document
owns the promotion decision.

## What "Works" Means

The unit being qualified is one immutable cell:

`strategy version x structure x DTE lane x measurement horizon x information set`

Results from different cells must not be pooled to manufacture significance. In
particular, a 7-DTE long call, a 45-DTE debit spread, and a short-premium position are
separate hypotheses even when they share an underlying signal.

A cell works only when all of the following are true:

- the strategy's net return is positive after modeled costs;
- it beats a same-session, same-horizon, same-structure naive control;
- the advantage persists across independent periods and both halves of the sample;
- the result survives correction for all related hypotheses tested;
- data, pricing, capacity, and tail-risk coverage are sufficient for the claim;
- a later untouched or forward-shadow sample confirms the result.

`ROBUST_PASS` is evidence of selection value. It is not authorization to trade.

## Three Claims, Three Tests

Do not use evidence for one claim to certify another.

| Claim | Question | Required comparison |
|---|---|---|
| Detector validity | Does the event identify a different future distribution? | Conditioned events versus matched no-signal or random events |
| Structure selection | Does the selected contract/package add value? | Selected package versus a same-structure naive package |
| Portfolio viability | Can the strategy be operated safely at realistic size? | Shadow portfolio versus declared risk, liquidity, and capacity limits |

A detector may be informative while its selected option loses after premium and costs. A
contract selector may add value while the resulting portfolio remains too concentrated or
illiquid. Each layer must pass separately.

## Qualification Workflow

### 1. Pre-register the hypothesis

Before examining outcomes, record:

- strategy and policy version;
- target population and excluded population;
- signal or detector definition;
- exact contract and structure selection rule;
- DTE lane, holding horizon, exit rule, and invalidation rule;
- primary outcome and same-structure control;
- estimated commissions, spread/slippage model, and capital-at-risk definition;
- minimum coverage, sample, effect, and risk thresholds;
- hypothesis family used for multiple-testing correction;
- stop rule and allowed follow-up analyses.

Changing one of these after seeing results creates a new exploratory version. It cannot be
reported as confirmation of the original hypothesis.

### 2. Prove data and decision integrity

The cell is `NOT_TESTABLE` unless all material inputs are point-in-time and reproducible:

- contract terms, nominal strikes, expiration, multiplier, and exercise style;
- option and underlying marks on a coherent clock;
- valuation-policy version and hash;
- dated rates, dividends, earnings/Fed events, and their coverage evidence;
- source receipt time as distinct from source market time;
- exact candidate, ordered legs, context, policy, and source-matrix lineage;
- expired, delisted, worthless, and otherwise losing contracts retained;
- no future revisions, future universe membership, or future corporate actions visible.

Coverage is part of the result. Missing data may not be converted to a favorable zero,
`CLEAR`, or no-event state.

### 3. Record evidence before adding gates

New inputs such as IV percentile, volatility richness, open-interest change, event state,
or gamma regime first enter every eligible candidate as immutable evidence. Outcomes are
collected across the full evidence distribution.

Do not immediately suppress candidates with the new input. Suppression removes the
counterfactual observations needed to determine whether the proposed gate discriminates.
A threshold becomes policy only after qualification and therefore requires a reviewed
strategy-version change.

### 4. Measure the exact selected package

Evaluate the contract or complete multi-leg package actually selected at entry. Track the
same contracts through a coherent exit batch; do not replace them with later ATM contracts.

Every outcome must include:

- entry and exit net premium;
- gross P&L, estimated cost, net P&L, capital at risk, and net return;
- entry and exit mark policy and source lineage;
- availability and quality flags;
- holding horizon and independent exchange session;
- adverse/favorable excursion and tail-loss measures when available.

Win rate is descriptive only. Always read it with mean return, profit factor, largest loss,
and capital at risk.

### 5. Build the correct controls

The primary control is the same complete structure translated toward ATM within the same
entry matrix, expiration, side, holding window, and valuation policy. Preserve leg types,
sides, ratios, multipliers, and widths. Require all control legs at both entry and exit.

Also use, where applicable:

- matched no-signal dates for detector claims;
- random-contract or random-event controls to characterize the null;
- a lookahead oracle to prove that the harness can detect a deliberately real edge;
- synthetic no-predictability controls for forecast models;
- regime and event exclusions declared before measurement.

Raw strategy return answers whether the package made money. Paired advantage answers
whether the selection rule earned anything beyond market direction or generic option
exposure. Qualification requires both.

### 6. Establish measurability

Collapse correlated candidates to one cross-sectional portfolio observation per exchange
session and horizon. Select non-overlapping periods using exchange-session ordinals. Ten
contracts selected on one date are not ten independent observations.

The current minimum gate for one strategy/horizon cell is:

- at least 100 paired, priced events;
- at least 40 independent non-overlapping periods;
- one valuation-policy cohort;
- acceptable exact-control and outcome-pricing coverage;
- no unresolved causal, timestamp, or package-coherence defect.

Fewer than 30 priced outcomes is `INSUFFICIENT`. A larger but sub-threshold sample is
`INDICATIVE`. Meeting sample floors is `MEASURABLE`, not evidence of edge.

### 7. Require confidence and robustness

A cell receives `CONFIDENCE_PASS` only when:

- mean net strategy return is positive with $t > 2$;
- mean paired advantage over the control is positive with $t > 2$;
- early-half and late-half strategy returns are both positive;
- early-half and late-half paired advantages are both positive.

It receives `ROBUST_PASS` only when Benjamini-Hochberg correction across the declared
hypothesis family gives $q \le 0.05$ for both absolute return and paired advantage.

The review must also show effect size, confidence interval, drawdown/tail behavior, control
coverage, and concentration by ticker, date, and regime. Statistical significance with an
operationally trivial effect or unacceptable tail loss does not qualify.

### 8. Confirm out of sample

Freeze the candidate policy and test it on data not used for discovery or threshold tuning.
Use one of these designs, declared in advance:

- chronological train/validation/test split;
- walk-forward fitting with untouched forward windows;
- prospective shadow collection after policy freeze.

The confirmation sample must preserve the sign of both net return and paired advantage and
must not reveal a new concentration, data-quality, or tail-risk failure. A failed
confirmation returns the cell to research; it is not repaired by repeatedly changing the
sample boundary.

### 9. Validate operational viability

Before execution eligibility, run the frozen cell in shadow or paper mode and verify:

- quote availability and realistic crossing/slippage assumptions;
- order and complete-package fill behavior;
- liquidity, size, open-interest, and participation limits;
- assignment, early-exercise, ex-dividend, pin, and expiration handling;
- position, concentration, margin, maximum-loss, and kill-switch behavior;
- reconciliation against broker or authoritative market state;
- no material degradation from research return to executable return.

These are execution gates. They cannot rescue a strategy that lacks research edge.

## Decision States

| State | Meaning | Allowed use |
|---|---|---|
| `NOT_TESTABLE` | Causal data, pricing, lineage, or controls are incomplete | Engineering and data collection only |
| `OBSERVATIONAL` | Evidence is recorded, but outcomes or independent periods are insufficient | Research display only |
| `MEASURABLE` | Sample and coverage floors are met | Formal statistical review |
| `CONFIDENCE_PASS` | Absolute and incremental results meet uncorrected gates | Await family correction and confirmation |
| `ROBUST_PASS` | Both claims survive FDR and stability checks | Eligible for frozen shadow confirmation |
| `SHADOW_PASS` | Forward operational behavior confirms the result | Eligible for execution-risk review |
| `EXECUTION_ELIGIBLE` | Research, quote, risk, account, and deployment gates all pass | Paper or live mode explicitly authorized by policy |
| `REJECTED` | Pre-registered stop rule or confirmation failure occurred | Stop or redesign as a new hypothesis |

State is attached to the exact qualification cell, never to a strategy name globally.

## Mandatory Stop Rules

Stop, retain the evidence, and do not promote when any of these occurs:

- paired advantage is non-positive at the pre-registered primary horizon;
- the effect reverses between early and late halves;
- significance disappears after family correction;
- performance is explained by one ticker, one session, or one market regime;
- realistic costs remove the advantage;
- exact control, outcome, or quote coverage is below its declared floor;
- tail loss, drawdown, assignment, or capacity exceeds the declared risk limit;
- an oracle cannot pass or a null control appears profitable, indicating a broken harness;
- an untouched confirmation or shadow period fails.

Do not search longer horizons, wider thresholds, or different subsets after failure unless
that follow-up is labeled as a new exploratory hypothesis.

## Strategy Evidence Record

Every qualification report should contain this compact record:

```text
Strategy/version:
Structure and DTE lane:
Primary claim:
Population and exclusions:
Entry/exit and measurement horizon:
Valuation and strategy policy hashes:
Evidence fields available at entry:
Priced events / eligible events:
Independent periods:
Exact-control coverage:
Mean net return / t / q:
Mean paired advantage / t / q:
Early-half and late-half results:
Win rate / profit factor / largest loss / drawdown:
Cost, liquidity, and capacity assumptions:
Ticker/session/regime concentration:
Data-quality and causal-lineage exceptions:
Confirmation or shadow result:
Decision state:
Reasons and stop-rule outcome:
Reviewer and decision date:
```

## Strategy-Specific Evidence

The common standard stays fixed, but each strategy must pre-register evidence appropriate
to its payoff:

| Strategy family | Evidence that must be segmented or controlled |
|---|---|
| Directional long premium | Directional magnitude, time to move, IV percentile/richness, theta, earnings state, breakeven versus realized move |
| Debit spreads | Same as long premium plus width, return on risk, target reachability, and both-leg liquidity |
| Credit spreads and condors | IV/realized-volatility spread, tail loss, gap/event exposure, assignment risk, width, and margin/capital usage |
| Income wheel | Downside and assignment outcomes, collateral return, ex-dividend state, recovery horizon, and concentration |
| Gamma/flow detectors | Incremental move distribution after detection; direction only when supported by point-in-time NBBO evidence |
| Open-interest change | Consecutive settlements, final volume, own-history baseline, and no directional claim without quotes |
| Smile or skew distortion | Persistence at executable cadence, strike-count normalization, neighboring consistency, and quote-aware costs |

A favorable contextual input is not itself a strategy qualification. For example, high IV
may help premium sellers and hurt premium buyers, but that relationship must be demonstrated
within each exact strategy cell against its control.

## Worked Examples

The numbers below are illustrative, not current results or approved thresholds. They show
how evidence from one candidate differs from evidence that qualifies a strategy cell.

### Example 1: Directional long premium in rich IV

```text
Strategy: Directional Long Premium
Structure / lane: Long call / SHORT, 8-21 DTE
Current 21D IV: 42%
Historical IV percentile: 94%
IV regime: RICH
Earnings state: CLEAR
Interpretation: Expensive premium; historical headwind for a premium buyer
Candidate action today: Record as evidence; do not suppress solely from IV rank
```

The qualification study compares all otherwise eligible long calls in a pre-registered
rich-IV bucket with the same strategy in lower-IV buckets and with same-session naive long
calls. Assume the rich-IV cell later reports:

```text
Paired events: 340
Independent periods: 52
Mean net strategy return: -3.1%
Mean advantage over naive long call: -1.4%
Early / late advantage: -1.1% / -1.7%
Decision state: REJECTED for rich-IV entry
```

This would support a new policy version that suppresses or penalizes long-premium entries
in the qualified rich-IV range. The single 94th-percentile observation did not justify the
gate; the repeated negative controlled result did.

### Example 2: Directional debit spread in rich IV

```text
Strategy: Directional Debit Spread
Structure / lane: Call debit vertical / SHORT, 8-21 DTE
Current 21D IV: 42%
Historical IV percentile: 94%
Return on risk: 1.35
Target distance: 0.72 implied standard deviations
Both-leg liquidity: Available
Interpretation: Rich IV is a headwind, but the short wing may offset part of its cost
Candidate action today: Retain and measure separately from a naked long call
```

Suppose the exact-width vertical cell later produces positive net return and positive
paired advantage, both halves remain positive, and both FDR-adjusted tests satisfy
$q \le 0.05$. It may receive `ROBUST_PASS` even if naked long calls in the same IV regime
are rejected. Qualification belongs to the exact structure, not to the bullish thesis or
the IV label globally.

The next step is frozen forward shadow validation. Only after realistic two-leg fills,
costs, and capacity also pass can the cell advance toward execution review.

### Example 3: Short-premium income strategy with high win rate

```text
Strategy: Income Wheel
Structure / lane: Cash-secured put / 22-45 DTE
Current 45D IV: 38%
Historical IV percentile: 87%
Annualized premium yield: 24%
Win rate: 82%
Interpretation: Attractive quoted premium, but potentially compensation for downside risk
Candidate action today: Record IV, event, collateral, and assignment evidence
```

Assume measured outcomes show an 82% win rate but rare losses produce a negative mean
return, profit factor below 1, and worse drawdown than the naive cash-secured-put control.
The cell is `REJECTED`. High win rate and high premium yield do not overcome negative
cost-adjusted expectancy or tail risk.

Conversely, rich IV would become useful ranking evidence only if the strategy demonstrates
positive paired advantage, acceptable assignment/tail behavior, stability, and forward
shadow performance in that IV bucket.

### Example 4: Sweep-like flow detection without directional proof

```text
Detector: Sweep-Like Cluster
Observation: Repeated large call prints across adjacent strikes
Trade classification: Included
Aggressor side: Unavailable without point-in-time NBBO
IV percentile: 76%
Interpretation: Unusual call activity; direction is not established
Candidate action today: Research evidence only
```

The detector first qualifies the claim that subsequent absolute move or volatility differs
from matched no-signal periods. It cannot claim bullish direction merely because calls
traded. A directional strategy based on the detector requires a separate test using
point-in-time quote evidence and then an exact-package comparison against its own control.

If activity predicts larger moves but not their sign, the detector may qualify as context
for a direction-neutral volatility structure while remaining invalid for long calls.

### Example 5: IV evidence is useful but not yet measurable

```text
Strategy: Directional Long Premium
Current 7D IV: 31%
Historical IV percentile: 18%
IV regime: CHEAP
Eligible outcomes: 74
Independent periods: 19
Paired advantage: Positive but unstable
Decision state: OBSERVATIONAL
```

This is a promising research segment, not a qualification pass. It has not reached the
100-event and 40-independent-period floors. The correct action is to continue recording
the complete candidate population, not to lower the thresholds or promote the strategy.

## Governance

- Research reports are immutable artifacts tied to data and policy hashes.
- Exploratory and confirmatory runs are labeled separately.
- Threshold changes create a new strategy version and restart qualification for affected cells.
- A new entitlement or mark source creates a separate valuation cohort; results are not pooled.
- `MEASURABLE`, `ROBUST_PASS`, and execution eligibility remain distinct fields.
- Failed and suppressed candidates remain auditable; absence is never rewritten as success.
- Promotion and rollback decisions record reviewer, date, evidence artifact, and reason.

The default decision under uncertainty is to keep collecting evidence, not to relax the
standard.
