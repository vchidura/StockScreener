# Scheduled SPY Trend Filter: Target Not Met

## Decision

**Do not add this SPY trend gate as tested.** It held cash in some difficult periods,
but also missed recoveries and did not protect against the momentum basket's largest
later drawdown. It fails the provisional 20% portfolio drawdown target in both samples.
Do not shorten the moving average or alter the risk-off allocation after seeing these
results simply to find a passing historical variant.

This is a result about one scheduled rule, not proof that all trend filters are useless.
Momentum remains the stock-ranking research baseline. These tests have not established
a portfolio implementation meeting the agreed downside objective.

## Frozen Rule

At each original 21-session rebalance decision, compare SPY's closing price with the
simple mean of its preceding 200 XNYS session closes, including that day's close:

- Strictly above the average: use the saved 75% equal-weight momentum / 25% cash path.
- At or below the average: hold 100% cash for the entire next holding window.
- Act at the original next-session open, never the signal close. No intraperiod exits,
  daily re-entry, drawdown stop, leverage or changed constituent selection.
- Use each original sample's storage cutoff for SPY prices. Missing or invalid market
  history is an error, not a substitute risk-off signal.
- Cash earns zero. An invested window retains its prior exposure-scaled round-trip
  fees; a cash-only window has no new trades or fees. Prior positions were already
  liquidated and charged at the previous scheduled exit.

The fixed allocation, 200-session window and 20% assessment limit were recorded before
this run, but after earlier strategy results were known. These same market dates are
not an untouched holdout. There was no parameter search or refitting.

## Return And Drawdown

Mean returns are arithmetic averages **per 21-session holding period**, after the
10 bps round-trip charge on invested notional. Unresolved holdings use the existing
0% terminal-return illustration and modelled intervening paths. Whole-history risk is
therefore a scenario estimate, not a fully observed maximum drawdown.

| Sample / Period | Fixed 75/25 Mean | Trend-Gated Mean | Fixed 75/25 Drawdown | Trend-Gated Drawdown |
|---|---:|---:|---:|---:|
| 1 / development | 1.30% | 1.34% | -15.55% | -12.08% |
| 1 / later | 2.38% | 1.99% | -25.10% | -25.10% |
| 2 / development | 0.87% | 0.91% | -21.11% | -19.93% |
| 2 / later | 2.53% | 1.98% | -28.08% | -24.91% |

The rule retained about **84% / 78% of the fixed 75/25 mean return** in the later
samples. This is a ratio of arithmetic means, not cumulative wealth retained.
It slightly improved development means, but its later return sacrifice bought no
maximum-drawdown improvement in sample 1 and an insufficient improvement in sample 2.

Later daily return standard deviation falls from **2.20% to 2.06%** in sample 1 and
**2.19% to 1.98%** in sample 2, partly reflecting more time in cash. The average equity
allocation at entries is **67.1% later**, versus 75% for the static control; they do not
have equal exposure or necessarily equal risk. This is not evidence of new stock alpha.

## When It Held Cash

Both samples have identical SPY signals: **39/47 risk-on windows**, with six cash-only
windows in development and two later. Overall mean entry equity exposure is 62.2%.

Cash decisions were 2022-09-07, 2022-10-06, 2022-11-04, 2022-12-06, 2023-01-06,
2023-03-09, 2025-03-13 and 2025-04-11. In the later period:

| Decision | Fixed 75/25 Return, Sample 1 | Fixed 75/25 Return, Sample 2 | Trend Portfolio |
|---|---:|---:|---|
| 2025-03-13 | -0.67% | +1.18% | Cash; 0% |
| 2025-04-11 | +8.09% | +9.27% | Cash; 0% |

These are the existing cost/missing-return scenario values. The first sample's March
window has one unresolved position; the April rebound windows are fully observed.
The rule's slow scheduled re-entry missed the April 14 through May 13 rebound in both
samples. This is the frozen rule doing what it was designed to do, not an execution bug.

The trend portfolio's later maximum drawdown ends on **2026-07-29** in both samples,
from a **2026-06-30** peak in sample 1 and **2026-05-28** peak in sample 2. It was
invested during those losses. A broad-market trend signal did not adequately control
the risk of this particular selected-stock basket.

## Twenty Percent Assessment

Neither trend portfolio meets the target across the declared period/cost/missing-return
scenarios. Each sample has 48 summary cells (three overlapping period views, four costs,
four missing-return assumptions); these are not independent trials or breach probabilities.

| Sample | Trend Scenario Cells Breaching 20% | Worst Scenario Drawdown |
|---|---:|---:|
| 1 | 33 / 48 | -25.20% |
| 2 | 41 / 48 | -32.31% |

Under the 10 bps, -100% unresolved-terminal-return stress, later means are **1.48% /
1.79%**, and later drawdowns **-25.10% / -28.26%**. Stressed missing paths are neither
observed losses nor bounds on future risk. Cash gating does not resolve those histories.

Risk-on windows keep the original missing-path assumptions unchanged. Risk-off windows
have no invested missing weight because no position is entered, not because the data
became complete. Their underlying unresolved counts are retained in the artifacts.
Of 17 later invested windows, **15 in sample 1 and 16 in sample 2** are fully observed.
The summary's legacy `fully_observed_windows` field also counts cash-only windows as
having no unresolved portfolio value; use `fully_observed_invested_windows` for actual
invested-path coverage. Daily closes still miss intraday extremes.

## Path Forward

Keep the failed rule in the research record and retain the unchanged equal-weight
momentum controls. Do not add this gate to live ranks or automatically try neighboring
moving averages. The 20% target remains a portfolio research objective, not a stop-loss
or guarantee.

Before another risk-policy experiment, specify the return/participation objective
alongside the drawdown target. A lower equity allocation can reduce nominal losses
without improving the strategy, while a new stock-selection rule changes the hypothesis
being tested. This experiment does not justify claiming a passing strategy by either
route. No further variant has been run or selected here.

## Reproduction

- [equity_momentum_trend_config.json](equity_momentum_trend_config.json)
- [equity_momentum_trend_results.json](equity_momentum_trend_results.json)
- [equity_momentum_trend_results.daily.csv](equity_momentum_trend_results.daily.csv)

Config SHA256: `5b9b65d713a36ea4994740c797ea34f287d454989cf8e4793f7eafb9eff0ca03`.
The run reused saved holding paths and read 1,166 retained SPY closes at each sample's
original cutoff. All 47 decisions have 200 complete same-identity closes and the two
sets of source bar IDs and signals match. Original portfolio summaries reproduced.

Forty-seven focused tests passed, including causal trend calculation, equality at the
threshold, missing/ambiguous/invalid history rejection, source cutoffs, next-open timing
and cash-only costs. Artifact checks verified 288 summary cells, 4,512 scenario paths,
matching signals and unchanged source files. Editor diagnostics and patch checks passed.
Database access was read-only; no provider fetches, schema/data mutations, strategy
publication, live ranking changes or orders occurred.

Entry point: `research.price_diagnostic.run_trend_experiment`, available through the
VS Code task **Run frozen SPY trend exposure comparison**. The CSV retains the existing
0%-missing, 10-bps illustrative daily paths, while the JSON contains all scenarios.