# Two-Feature Ridge Challenger: Better Later Selection, Not Qualified

Follow-up: the [Contributor And Missing-Path Validation](EQUITY_RIDGE_VALIDATION_2026-09-12.md)
corroborates CAR's large gain with a second price source and classifies the five gaps.
The original performance figures below remain unchanged; no event sensitivity or
model refit has been applied.

## Decision

**Retain this exact challenger for targeted validation, not live deployment.** Its
later-period portfolio gains exceed both momentum and matched-schedule SPY in the
training-ticker sample and the disjoint transfer sample. The advantage over momentum
remains positive after removing each sample's strongest paired excess-return period.
Unlike the preceding exposure overlays, this is a change to stock selection.

Important counterevidence remains: it trails momentum in both earlier evaluation
samples, its average rank correlation is lower, and its later drawdowns still exceed
the 20% target. A newly selected CAR path records a very large gain and has not yet
received targeted external price/corporate-action validation. These historical dates
were already examined; the result is not a fresh temporal holdout or statistical
proof of alpha.

The next bounded step is to audit the largest new contributors and the five unresolved
selected positions, preserving this exact model and its results. After that, use a
separately specified confirmation set without choosing new features or penalties from
these outcomes. Do not immediately retune the ridge, shorten its training window, or
add another exposure rule to make the historical drawdown pass.

## Frozen Experiment

- Features: the two values already saved at each decision, `mom_12_1` and `rev_5`.
  No new feature extraction or scanner gate.
- Training source: sample 1 only. Every fit is reused without adaptation to score
  sample 2. The two 300-ticker samples remain disjoint and share market dates.
- Expanding training: require at least 12 completed decision cohorts and 500 observed
  labels. Both training decision and exit date must be strictly before the scoring
  decision. Same-close labels are deliberately excluded, even when reconstructed
  availability would permit using them.
- Target: the observed original next-open/21st-close stock price return minus the
  matching SPY price return. Unresolved training labels are omitted and counted, not
  filled with the evaluation scenarios. This may create nonrandom label-selection bias.
- Equal total training weight per decision cohort, normalized to mean row weight 1.
  Weighted feature means, population standard deviations and label intercept use only
  training data. Fit the existing ridge solver with fixed `alpha=10`, with no search.
  This is a fixed sum-loss penalty; its relative strength decreases as the expanding
  training sample grows. No target or feature winsorization is applied.
- Score every saved feature-ready stock. Select the highest `ceil(0.10 * N)` scores,
  ticker ascending for ties, with no positive-score gate. Future-missing names retain
  their selected weights. Scores are ranking outputs, not calibrated probabilities.
- Keep 75% equity / 25% zero-return cash, equal stock weights, original next-open
  entry and 21-session exit, fixed shares within each holding window, and the same
  0/4/10/25 bps round-trip fees on initial invested notional. No risk overlay is added.

There are **34 fitted models**, each applied to both samples. The first prediction is
2023-10-09 and the last is 2026-07-16. The first 13 decisions are warmup and excluded
from every portfolio, not represented as cash. This leaves **15 development and 19
later windows**. Later controls reproduce the previous benchmark report exactly;
development and full-evaluation results below use the shorter common date range.

The first fit has 1,466 observed rows from 12 cohorts, with latest exit 2023-09-08.
The last has 6,281 rows from 45 cohorts, with latest exit 2026-06-15. Omitted matured
unresolved training labels rise from 8 to 30. No otherwise eligible training row was
excluded for invalid features, and sample 2 contributes no labels or preprocessing.

## Later Results

Cumulative net price returns below span the 19 later windows, January 2025 through
August 2026. They are **not annual returns**. The illustration uses 10 bps costs and
0% terminal returns for unresolved holdings. Stock drawdowns remain scenario estimates
where daily paths are unresolved.

| Portfolio | Cumulative Return | Daily-Path Drawdown | Daily Return Std. Dev. |
|---|---:|---:|---:|
| Ridge, sample 1 | +89.40% | -22.56% | 1.98% |
| Momentum, sample 1 | +48.24% | -25.10% | 2.20% |
| Eligible-stock equal weight, sample 1 | +33.09% | -16.81% | 0.98% |
| Ridge, sample 2 | +79.81% | -23.72% | 2.00% |
| Momentum, sample 2 | +49.79% | -28.08% | 2.19% |
| Eligible-stock equal weight, sample 2 | +23.56% | -17.86% | 0.95% |
| Scheduled SPY, identical samples | +24.88% | -15.07% | 0.82% |

Ridge mean returns per 21-session window are 3.71% / 3.47%, versus momentum's 2.38% /
2.53%. Ridge beats momentum in 10/19 and 11/19 windows, and SPY in 11/19 and 12/19.
The compounded gains are substantial but do not imply improvement in most individual
stock forecasts or a successful drawdown-limited portfolio.

## Stability And Counterevidence

| Comparison | Later Compounded Excess | Without Strongest Excess Period |
|---|---:|---:|
| Sample 1 ridge minus momentum | +41.16 pp | +16.59 pp |
| Sample 2 ridge minus momentum | +30.02 pp | +14.13 pp |
| Sample 1 ridge minus SPY | +64.51 pp | +33.87 pp |
| Sample 2 ridge minus SPY | +54.92 pp | +18.95 pp |

The removed decision is 2026-03-16 in sample 1 and 2025-09-12 in sample 2. Both
portfolios in each pair lose the same window. This is a descriptive return-dependence
check, not an executable exclusion rule; no stitched drawdown is calculated for it.

Earlier common-date performance is substantially weaker:

| Portfolio | Development Cumulative Return | Full Common-Date Cumulative Return |
|---|---:|---:|
| Ridge, sample 1 | +16.53% | +120.71% |
| Momentum, sample 1 | +45.05% | +115.02% |
| Ridge, sample 2 | +25.80% | +126.20% |
| Momentum, sample 2 | +36.08% | +103.83% |
| Scheduled SPY | +22.26% | +52.68% |

The later gain offsets earlier underperformance; it is not stable superiority across
all periods. Average later per-date Spearman rank correlation is **0.0335 / 0.0189**
for ridge versus **0.0358 / 0.0342** for momentum. During development it is
-0.0231 / 0.0344 versus momentum's 0.0589 / 0.0524. These correlations use only
observed outcomes and have not received dependence-aware significance testing.
Improved top-basket returns are not evidence of uniformly better ranking.

## What The Model Selected

The long-term momentum coefficient is negative in the early fits, near zero around
the first half of 2025, and positive in the later fits. The `rev_5` coefficient is
negative throughout. Since the saved `rev_5` feature is **minus the five-day return**,
that negative coefficient favors recent strength, conditional on the long-term score;
it is not a buy-the-pullback result. We did not constrain signs after fitting.

Average basket overlap with original momentum is only 6.5% / 6.1% in development,
rising to 43.1% / 42.3% later. This is not simply the old momentum basket with a few
names adjusted. The experiment changes both learned weighting and the second feature;
without a separate ablation it cannot attribute the improvement to reversal alone.

Selected observed later contributors that merit scrutiny include:

| Sample / Decision | Ticker | Recorded Stock Return | Initial Portfolio Weight | Gross Window Contribution |
|---|---|---:|---:|---:|
| 1 / 2026-03-16 | CAR | +298.40% | 4.41% | +13.16 pp |
| 1 / 2025-09-12 | OKLO | +102.98% | 4.69% | +4.83 pp |
| 1 / 2026-04-15 | INTC | +78.55% | 4.69% | +3.68 pp |
| 2 / 2025-09-12 | RGTI | +187.86% | 4.17% | +7.83 pp |
| 2 / 2025-09-12 | EOSE | +102.01% | 4.17% | +4.25 pp |
| 2 / 2025-11-11 | SGML | +98.31% | 4.17% | +4.10 pp |

Contributions are initial weight times gross return within one window, not additive
attribution of full-period compounded wealth. Saved individual endpoints and daily
paths reproduce, but that checks internal consistency rather than an independent
vendor or corporate-action source. CAR in particular still needs targeted review.
The maximum initial individual-stock weight across all new baskets is 5.77%.

## Missingness And The Twenty Percent Target

There are 521 / 555 selected position-windows, with 2 / 3 unresolved outcomes:

- Sample 1: QMMM at 2025-09-12 and KAR at 2025-12-11.
- Sample 2: CLSK at 2024-11-07, MNMD at 2026-01-13 and SATS at 2026-06-15.

No name is dropped because its future return is missing. Modelled unresolved paths
stay at relative value 1 until the terminal -100%/-25%/0%/+25% assumption is applied.
The later ridge portfolios each have 17/19 fully observed invested windows. Whole-
evaluation risk is a scenario estimate, not a completed economic reconstruction.

At 10 bps and the -100% unresolved-terminal-return stress, later ridge cumulative
returns are **73.90% / 65.07%**, still above the momentum and SPY controls under the
same assumption. Drawdowns are **22.56% / 27.00%**. The stress is not a worst-case
bound on relative returns or future losses, and does not repair unresolved entry,
delisting or corporate-event histories.

| Ridge Sample | Beats SPY | Beats Momentum | Breaches 20% Drawdown | Meets SPY Plus Drawdown Objective |
|---|---:|---:|---:|---:|
| 1 | 32/48 | 28/48 | 32/48 | 0/48 |
| 2 | 44/48 | 32/48 | 32/48 | 12/48 |

Each set of 48 cells combines three overlapping period views, four costs and four
missing assumptions. These counts are not success probabilities. All later and full-
evaluation cells breach 20%; the sample-2 passing cells occur only in development.
Worst scenario drawdown is 22.66% / 27.18%. Neither sample passes the full declared
set, and the target has not been relaxed.

## Reproduction And Verification

- [equity_ridge_challenger_config.json](equity_ridge_challenger_config.json)
- [equity_ridge_challenger_results.json](equity_ridge_challenger_results.json)
- [equity_ridge_challenger_results.predictions.csv](equity_ridge_challenger_results.predictions.csv)
- [equity_ridge_challenger_results.daily.csv](equity_ridge_challenger_results.daily.csv)
- Entry point: `research.price_diagnostic.run_ridge_challenger`.
- VS Code task: **Run frozen two feature ridge challenger**.

Config SHA256: `be1b44081289cca693b274b29d51258c947e0f826ffd4fb5a44af283513ce191`.
Runtime: 6.6 seconds. The fitted transforms, coefficients, intercepts, training-row
hashes and scoring model hashes are retained. The artifacts contain 10,442 candidate
scores, 1,076 selected position-windows, 4,352 unique holding/scenario paths, 384
summary cells, 288 paired comparisons and 5,984 illustrative daily marks.

All 78 focused tests passed, covering same-close and future-label exclusion, sample-2
training isolation, training-only transforms, unresolved labels and selected outcomes,
tie handling and matched benchmark dates. Reconstructing all 34 actual training sets
reproduced their hashes, transformations and coefficients; an independent scikit-learn
ridge calculation matched the existing solver. Saved scores reproduce from the model
artifacts, selected baskets follow score/ticker order, and daily exports reproduce
growth and drawdown. Original controls and source JSON/CSV hashes remain unchanged.

Daily path reads returned 10,918 / 11,618 bar occurrences, using the original cutoffs
2026-09-12T21:42:23.880932+00:00 and 2026-09-12T22:01:58.649331+00:00. Queries were
database-enforced read-only. No external provider requests, database/schema writes,
live ranking updates, production model publication or orders occurred.

The evidence remains a split-adjusted price-return diagnostic, not sector-neutral or
dividend-inclusive certification. Cash interest, taxes, realistic variable slippage
and intraday drawdown extremes are excluded. The shared market dates and earlier
research choices remain relevant sources of selection bias.