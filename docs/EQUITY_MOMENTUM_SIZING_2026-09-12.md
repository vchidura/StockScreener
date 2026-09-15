# Momentum Sizing: A Risk/Return Tradeoff

Follow-up: [Daily Exposure And Drawdown Comparison](EQUITY_MOMENTUM_EXPOSURE_2026-09-12.md)
is complete. Daily-path risk is larger than the boundary-only estimates below; these
earlier results are retained unchanged.

## Decision

**Keep equal-weight momentum as the reference strategy. Retain capped inverse-volatility
weighting as a risk-control challenger, not an automatic replacement.** It consistently
reduces realized period volatility, but gives up return, increases dollar concentration
and target-weight turnover, and does not consistently improve return per unit of risk.
The improvement is not enough to claim a robust or deployable strategy.

The useful next question is whether this reweighting beats a simpler reduction in
equal-weight exposure. Freeze that separate comparison before running it; do not select
a cash allocation using the realized volatility ratios below.

## Frozen Test

- Same saved top-decile momentum holdings, dates and forward returns in both disjoint
  300-ticker samples: 47 holding windows each, with 28 development and 19 later windows.
- Equal weight versus one challenger: inverse trailing volatility, capped at **2/N**
  per holding and proportionally redistributed. No stock replacement or optimization.
- Volatility uses **63 daily close-to-close returns from 64 contiguous XNYS closes**
  ending at the decision close, with sample standard deviation (`ddof=1`). Each sample
  uses its original observed/created-at source cutoff, not the latest prices.
- Both portfolios are **100% invested, long-only and unlevered** at entry. Same next-open
  entry, 21st-session-close exit, full round-trip cost and missing-return scenarios.
  There is no intraperiod rebalance or volatility targeting.
- If any selected name lacks a valid volatility window, the entire basket falls back
  to equal weight and is flagged. **All 1,415 position/date inputs passed; no fallback
  occurred.** The original equal-weight scenario returns reproduced to numerical tolerance.
- The first sample's approved corporate-event treatment is reported separately. Its
  sizing weights are identical to those in the unchanged first-sample test.

## Results

Values are **arithmetic means and standard deviations per 21-session holding period**,
not annualized. The table uses **10 bps round-trip cost** and a **0% return scenario for
unresolved allocated weights**, not an assertion that missing returns were zero.
Drawdown is measured only at holding-period boundaries; intraperiod losses can be worse.

| Sample / Period | Weighting | Mean Return | Period Volatility | Boundary Drawdown |
|---|---|---:|---:|---:|
| Sample 1 / development | Equal | 1.73% | 5.67% | -10.90% |
| Sample 1 / development | Inverse volatility | 1.32% | 4.69% | -10.62% |
| Sample 1 / later | Equal | 3.17% | 10.28% | -18.76% |
| Sample 1 / later | Inverse volatility | 2.90% | 8.88% | -15.75% |
| Sample 2 / development | Equal | 1.16% | 6.12% | -19.24% |
| Sample 2 / development | Inverse volatility | 1.29% | 5.30% | -18.44% |
| Sample 2 / later | Equal | 3.38% | 11.82% | -24.34% |
| Sample 2 / later | Inverse volatility | 2.52% | 9.73% | -21.69% |

Later-period volatility falls approximately **13.6% in sample 1** and **17.7% in
sample 2**. Boundary drawdown improves by **3.02** and **2.65 percentage points**,
respectively. Mean return falls by **0.27 pp** and **0.86 pp** per period.

The simple later mean/standard-deviation ratio moves approximately **0.309 to 0.327**
in sample 1, but **0.286 to 0.259** in sample 2. This descriptive zero-cash-rate ratio
is not a calibrated Sharpe estimate or significance test. It is mixed evidence, not
a consistent risk-adjusted improvement.

Inverse weighting beats equal weighting in **9/19 later windows** for sample 1 and
**7/19** for sample 2. Removing its best paired-difference period leaves mean differences
of **-0.53 pp** and **-1.17 pp**, respectively. No observations were removed from the
primary results. Sample 2's modest development benefit of +0.13 pp also becomes about
zero without its best difference period.

### Approved Event Sensitivity

For sample 1 with the seven separately reviewed event returns, later mean return is
**3.11% equal weight versus 2.83% inverse volatility**, and period volatility is
**10.15% versus 8.76%**. Drawdowns remain **-18.76% versus -15.75%**. There are no
unresolved selected momentum outcomes in this sensitivity, but it still excludes
dividends, actual settlement delays and account-specific fractional treatment.

The risk/return conclusion is unchanged by those seven event assumptions. Original
sample reports, CSVs, scores, selections and event-sensitivity artifacts were not modified.

## Concentration And Cost

Inverse-volatility sizing equalizes a *weighted-volatility proxy*, not dollar weights
or true covariance-based risk contributions. Equal weighting already minimizes dollar
Herfindahl concentration for a fixed number of names.

| Later-Period Average | Sample 1 Equal / Inverse | Sample 2 Equal / Inverse |
|---|---:|---:|
| Largest portfolio weight | 6.16% / 11.00% | 5.60% / 10.53% |
| Dollar-weight Herfindahl index | 0.0616 / 0.0698 | 0.0560 / 0.0661 |
| Weighted-volatility proxy index | 0.0709 / 0.0616 | 0.0712 / 0.0561 |
| Target-weight turnover | 0.281 / 0.345 | 0.282 / 0.354 |

The largest weight anywhere in the runs is **15.74%** in sample 1 and **16.67%** in
sample 2. The 2/N cap is relative to basket size, not an absolute position-risk limit.
Target-weight turnover ignores intervening weight drift; it is not executed turnover.

Both rules pay the same cost on 100% invested notional, so the paired return difference
is unchanged across 0/4/10/25 bps cost scenarios. This does not establish equal real
slippage: higher turnover, different liquidity and concentrated positions may affect it.
Cash exposure is not responsible for the observed reduction in risk.

An example of event risk: AIMC's trailing daily volatility was only about **0.238%** at
the March 2023 decision, giving it **15.38%** instead of **7.69%** equal weight. Its
forward path is unresolved. Low observed volatility does not guarantee safe settlement
or low exposure to a discontinuous event.

## Missingness And Contributor Checks

The original seven selected missing paths and replication six remain in their original
results. Their total allocated weight changes with sizing; it is not renormalized onto
known outcomes. Inverse weighting assigns *more* average unresolved weight in both
later samples: approximately **1.27% versus 1.01%** in sample 1 and **0.312% versus
0.263%** in sample 2.

Under the uniform -100% missing-return stress, later means are **2.16% equal versus
1.63% inverse** in sample 1, and **3.12% versus 2.20%** in sample 2. Respective drawdowns
are **-21.94% versus -20.74%**, and **-29.13% versus -26.27%**. Thus the risk reduction
persists in that stress, but is not a solution to incomplete outcomes.

The paired adverse assignment of the frozen missing-return scenarios gives later
inverse-minus-equal differences of **-0.64 pp** and **-0.91 pp**. These are sensitivity
assumptions, not estimated outcomes or confidence bounds.

The accompanying bounded review checked the replication's six unresolved paths and
six large contributors against fresh, same-provider daily aggregates:

- RGTI, SMMT, ALAB, EOSE, SAGE and RDW entry/exit prices matched. This is endpoint
  agreement within one provider, not independent-vendor verification.
- AIMC stops after March 24, 2023; ISEE and DICE lack their planned terminal session;
  SATS stops after June 23, 2026. Their terminal consideration/continuation remains
  unpriced in this experiment.
- BIIB lacks its intended June 9, 2023 entry session, and CLSK lacks its intended
  November 8, 2024 entry session in both stored and fresh responses. Do not replace
  these with the following open without a separately specified execution rule.

Attribution is consistent with the rule: it avoided some losses in LUNR, RDW and IONQ,
but reduced exposure to winners such as RGTI, METC and AXTI. This explains the tradeoff;
it is not a reason to manually exempt those tickers from sizing.

There are **4,339 / 4,957 volatility-window close occurrences** without contemporaneous
dated membership identity in samples 1 / 2. Stored IDs agree within each window, but
that is not independent issuer verification. Counts include reused close observations
across windows. Prior price-only, historical-reconstruction and holdout caveats remain.

## Next Decision

Do not tune the 63-day window or cap to recover this sample's lost returns. Before
adopting inverse-volatility weights, test whether a simple, separately frozen reduction
in equal-weight exposure offers comparable downside protection with less complexity.
Use daily portfolio paths for the next drawdown comparison so intraperiod risk is
visible. Keep unresolved entry/terminal positions explicit rather than turning that
step into a broad infrastructure project.

No scanner gate, leverage, model-fitting step or live promotion is justified by this test.
The evidence supports momentum as a research baseline and sizing as a tradeoff to
evaluate, not a finished trading system.

## Reproduction

- [equity_momentum_sizing_config.json](equity_momentum_sizing_config.json)
- [equity_momentum_sizing_results.json](equity_momentum_sizing_results.json)
- [equity_momentum_sizing_results.weights.csv](equity_momentum_sizing_results.weights.csv)
- [equity_price_replication_path_validation.json](equity_price_replication_path_validation.json)

Configuration SHA256: `6e9c2bbbca208a9e941b3f24522614b8235c1b4f624ddaaaa28e3a81082c3837`.
The run took approximately **55 seconds**, using 44,032 + 46,528 selected close
observations at the two original storage cutoffs. Full source report/CSV hashes and
per-window bar-ID hashes are retained.

Thirty-two focused tests passed, including trailing-window causality, cutoff SQL,
weight-cap/permutation invariants, fallback, fixed-weight missingness and paired risk
arithmetic. Artifact checks confirm both original baselines reproduce, all baskets
sum to 100%, caps hold, and source artifacts remain byte-for-byte unchanged. Editor
diagnostics and patch checks passed. All database access was read-only; no schema,
data, qualification or live ranking changes were made. Fresh provider checks wrote
local cache responses only.

The executable entry point is `research.price_diagnostic.run_sizing_experiment`, also
available through the VS Code task **Run fixed momentum risk sizing comparison**.
Only one sizing rule was tested; no parameter changes were made after seeing results.