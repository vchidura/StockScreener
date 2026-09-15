# Equity Exploratory Research Closeout

Status: **COMPLETE. NO-GO FOR PRODUCTION DEPLOYMENT.**

This report closes the bounded historical research phase, including its final
event/execution sensitivity. It is the consolidated decision record, not a proposal
for another historical model or parameter experiment.

## Decision

| Decision | Outcome |
|---|---|
| Deploy the tested strategy or promote its scores into live ranking | **NO-GO.** Both later samples still exceed the accepted 20% drawdown research limit. |
| Retain the two-feature ridge candidate | **YES, research only.** Its later selection advantage survives the declared sensitivities, but is not stable across earlier periods. |
| Continue tuning this historical experiment series | **STOP.** No new feature sets, penalties, lookbacks or portfolio overlays are selected. |
| Start live or paper execution as part of this closeout | **NO.** No trading, signal-publication or paper-tracking service has been started. Forward observation would be a separately scoped phase. |

The practical finding is that the existing signals can support a promising research
ranking, but these exercises have not established a deployable portfolio or a
calibrated individual-stock win probability. The final sensitivity does not change
that verdict. The drawdown limit is an assessment target, not a future-loss guarantee.

## What Was Tested

| Exercise | Decision-Useful Finding | Disposition |
|---|---|---|
| Momentum and seven existing scanner families on one historical 300-ticker sample | Momentum provided directional ranking evidence; scheduled scanner filters did not justify mandatory gating. This did not test daily waiting-for-entry policies. | Keep momentum as the reference; no scanner gate added. |
| Disjoint next-300 momentum replication | Directional excess appeared again, but with shared market regimes, tail dependence and material downside. | Useful replication across securities, not independent time periods. |
| Capped inverse-volatility stock weights | Some risk reduction, lower return and greater dollar concentration; no clear winner. | Not adopted. |
| Fixed 75% equity / 25% cash | Lower risk, but daily drawdowns were larger than holding-boundary estimates and still exceeded 20%. | Retained as a simple control, not a qualified strategy. |
| Scheduled SPY 200-session trend gate | Missed recoveries without adequately controlling the selected basket's worst losses. | Rejected as tested. |
| Matched-schedule SPY benchmark | Momentum's larger raw returns came with much larger volatility. | Raw excess was not enough to establish worthwhile risk-adjusted value. |
| Fixed 10% volatility budget for momentum and SPY | Realized risk became more comparable, but momentum did not establish a replicated return advantage. | No further overlay tuning. |
| Two-feature causal ridge challenger | Better later top-basket returns in both samples, weaker earlier performance and average rank correlation. | Retain exact candidate for research, not deployment. |
| Contributor/event validation and final sensitivity | CAR's large gain was corroborated; symbol and halt treatments modestly changed returns but did not remove the drawdown failure. | Historical phase closed. |

## Final Portfolio Results

The final comparison preserves all 34 common evaluation windows: 15 development and
19 later windows. The first 13 original dates remain training warmup and are omitted
from every portfolio. The later windows cover January 2025 through August 2026.

These are **cumulative net price returns across 19 later windows, not annual returns**.
The illustration uses 10 bps round-trip fees on invested notional and a 0% terminal
assumption for remaining unresolved positions. Intended exposure is 75% equity and
25% zero-return cash, with no redistribution of unfilled allocations.

| Portfolio | Original Later Return | Final Sensitivity Return | Final Daily-Path Drawdown |
|---|---:|---:|---:|
| Ridge, sample 1 | +89.40% | +89.96% | -22.56% |
| Momentum, sample 1 | +48.24% | +48.24% | -25.10% |
| Eligible-stock equal weight, sample 1 | +33.09% | +33.13% | -16.81% |
| Ridge, sample 2 | +79.81% | +78.70% | -24.48% |
| Momentum, sample 2 | +49.79% | +48.26% | -28.08% |
| Eligible-stock equal weight, sample 2 | +23.56% | +23.50% | -17.86% |
| Scheduled SPY, identical samples | +24.88% | +24.88% | -15.07% |

The final ridge advantage over consistently treated momentum is **41.72 / 30.44
percentage points**. Removing each pair's strongest excess-return window from both
portfolios leaves **17.06 / 14.57 pp**. Corresponding leave-best-period excess over
SPY is 34.34 / 18.06 pp. These are dependence diagnostics, not trading rules or
drawdowns on artificially joined periods.

The earlier common-date result is not a success across regimes: ridge returns are
16.53% / 25.80%, versus momentum's 45.05% / 36.08% and SPY's 22.26%. The original
ridge rank-correlation and coefficient-instability findings also remain unchanged.
This is evidence of better later top-basket outcomes, not uniformly better forecasts.

## Final Sensitivity Rules

The configuration was recorded before this run. No model, score, constituent selection,
training label or benchmark date was changed.

1. **Same-share symbol continuations:** conditionally use the reviewed recorded
   KAR/OPLN, MNMD/DFTX and SATS/ECHO daily paths. Retain fixed shares and the original
   fees. No interpolation, replacement stock or retroactive signal change is used.
2. **CLSK unavailable entry:** on November 8, 2024, leave its intended allocation in
   cash through the original exit. No entry or exit fees, no delayed buy and no
   redistribution to other stocks. This applies wherever that position was selected.
3. **QMMM and other untreated gaps:** retain the original -100%/-25%/0%/+25% terminal
   scenarios and modelled intervening paths. No executable exit or terminal value is
   invented for QMMM's suspended holding. It is not treated as an unfilled entry.
4. **Apply consistently:** use the same treatment for every affected sample/ticker/
   decision in ridge, momentum and eligible-stock portfolios. SPY is unchanged.

There are ten affected portfolio-position instances: KAR in two portfolios; MNMD in
two; SATS in three; and CLSK in three. Across the 16 cost/missing-return combinations,
160 holding-window records change and **4,192 remain verbatim copies**. CLSK leaves
5% of capital unfilled in both the ridge and momentum baskets for that development
window, reducing effective equity exposure from 75% to 70%; the eligible-stock
control leaves its own smaller original weight in cash.

These are conditional retrospective treatments, not production data corrections.
KAR and SATS official transition dates were not established by the retrieved primary
records. For MNMD, the official DFTX start is January 13, 2026, while the provider's
daily symbol handover is January 15. The final sensitivity retains this discrepancy
and does not relabel the provider date as the legal effective date.

## Risk And Remaining Uncertainty

| Ridge Assessment Across Declared Scenarios | Sample 1 | Sample 2 |
|---|---:|---:|
| Summary cells evaluated | 48 | 48 |
| Cells beating fixed 75/25 scheduled SPY | 32 | 48 |
| Cells breaching 20% drawdown | 32 | 32 |
| Cells meeting both objectives | 0 | 16 |
| Worst scenario drawdown | -22.66% | -24.66% |

All later and full-evaluation cells breach the limit in both samples. Sample 2's
passing cells are only the development period. The 48 cells combine overlapping
period views, four costs and four missing-value assumptions; counts are not success
probabilities or independent trials.

QMMM is the one remaining unresolved ridge position in sample 1. At 10 bps and a
-100% terminal assumption for it, later ridge return is 82.15%, with 22.56% drawdown.
Sample 2 has no unresolved ridge valuations after the conditional joins and no-fill
treatment, so its missing-value scenarios coincide: 78.70% later return and 24.48%
drawdown at 10 bps. This is **not** proof that all its paths are certified: the new
symbol evidence is retrospective and conditional. The controls retain their own
untreated unresolved positions.

The result schema separates `windows_without_unresolved_valuation` from
`fully_observed_original_windows`, and records reviewed-path/no-fill counts. It does
not call an alias join or a hypothetical unfilled-cash allocation an originally
observed complete holding.

Other limits remain material:

- The ticker samples are disjoint, but use the same repeatedly examined market dates.
  A causally fitted walk-forward model does not erase research-selection bias.
- Training still excludes unresolved labels. The final evaluation adjustment does
  not repair those omissions; no refit was performed.
- Price returns exclude dividends, cash interest and taxes. Scheduled SPY has the
  same entry/exit gaps and fees; it is not continuous buy-and-hold total-return SPY.
- Daily closes miss intraday extremes. The cost model does not establish capacity,
  execution reliability or realistic state-dependent slippage.
- This series did not establish calibrated per-stock probabilities, robust causal
  feature attribution or production qualification.

## Completion And Operational State

The finite closeout commitment is fulfilled: one final sensitivity, a consolidated
decision and preserved original evidence. No additional historical variants will be
run as an automatic continuation of this series.

The exact ridge candidate and all controls remain available for a separately scoped
forward-only paper study, but no paper tracker is running and no new live strategy
has been published. That possible future phase is not unfinished work in this phase.
No trading orders, ranking promotion or confidence-probability publication resulted
from the closeout. Earlier unrelated workspace changes remain preserved.

## Reproduction And Evidence

- [equity_ridge_final_sensitivity_config.json](equity_ridge_final_sensitivity_config.json)
- [equity_ridge_final_sensitivity_results.json](equity_ridge_final_sensitivity_results.json)
- [equity_ridge_final_sensitivity_results.daily.csv](equity_ridge_final_sensitivity_results.daily.csv)
- [Original ridge experiment](EQUITY_RIDGE_CHALLENGER_2026-09-12.md)
- [Contributor and event validation](EQUITY_RIDGE_VALIDATION_2026-09-12.md)
- [Benchmark comparison](EQUITY_MOMENTUM_BENCHMARK_2026-09-12.md)
- [Volatility-budget comparison](EQUITY_MOMENTUM_VOL_BUDGET_2026-09-12.md)
- [Original exploratory scanner comparison](EQUITY_EXPLORATORY_BACKTEST_2026-09-12.md)
- [Disjoint momentum replication](EQUITY_MOMENTUM_REPLICATION_2026-09-12.md)

Final config SHA256:
`b2df6d60f17ad395f7f95c14a3b5e2a9077b87216429df655f89e1d6c67f0509`.
Entry point: `research.price_diagnostic.run_final_ridge_sensitivity`.
VS Code task: **Run final frozen ridge event execution sensitivity**.

All 97 focused tests passed, including direct fixed-share parity for contribution
replacement and fee refunds, remaining missing weights, identity/date guards and
recorded alias-path reconstruction. The 3.4-second run used files only: no new database
reads or provider requests. Artifact checks verified frozen model/prediction/source
hashes, 4,352 unique scenario paths, 384 summaries, 288 comparisons, the ten-position
ledger and 5,984 daily marks reproducing reported growth and drawdown. Original
models, predictions, results and source data remain unchanged.