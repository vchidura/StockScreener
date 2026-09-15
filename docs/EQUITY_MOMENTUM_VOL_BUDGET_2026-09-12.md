# Portfolio Volatility Budget: Risk Reduced, Return Objective Not Met

Follow-up: the [Two-Feature Ridge Challenger](EQUITY_RIDGE_CHALLENGER_2026-09-12.md)
selection experiment is complete under subsequent user approval. It improves later
returns but does not qualify a portfolio; the findings below remain unchanged.

## Decision

**Do not adopt the tested portfolio as meeting the research objective.** The fixed
volatility budget reduced momentum's risk substantially, with full-evaluation realized
volatility close to 10%. It did not deliver replicated excess return over equally
budgeted SPY, and never beat the original fixed 75/25 SPY hurdle across the declared
period/cost/missing-return scenarios.

The later illustrative drawdowns are smaller than budgeted SPY's. That is a genuine
descriptive difference, not a reason to change the agreed objective after seeing the
results. Sample 2 also breaches the 20% drawdown target under adverse missing-path
scenarios. No portfolio is qualified or promoted.

Pause further cash-weight, moving-average and volatility-target variants for this
momentum baseline. A useful next direction is stock-selection research: one small,
separately frozen walk-forward challenger using existing features, compared with the
unchanged momentum and SPY controls. No feature set or challenger has been selected,
trained or tested in this run. This result does not establish that all momentum
strategies lack value; it rejects this implementation as a demonstrated solution to
the accepted return-and-drawdown objective.

## Frozen Rule

- One 10% annualized volatility budget, with a 75% equity cap and no leverage.
  The 10% number is a round research setting, not fitted to historical drawdowns.
- At each original decision close, use 64 contiguous closes to calculate the
  preceding 63 daily returns for every saved momentum constituent. Keep equal stock
  weights. Calculate the sample standard deviation of their equal-weight daily
  return average, annualized by the square root of 252. This incorporates sample
  covariance; it is not an average of individual stock volatilities.
- Apply exactly the same estimator to SPY. Set equity exposure to the smaller of
  75% and `0.10 / estimated_annualized_full_equity_volatility`.
- Execute at the original next-session open, retain fixed shares for 21 holding
  sessions, and put the remainder in zero-return cash. No intraperiod risk resizing.
- Retain all constituents. The risk-budget calculation holds the whole portfolio
  in cash when a constituent return history or the portfolio estimate is unusable.
  Market identity acquisition is additionally fail-closed. All 188 portfolio
  decisions in this run had valid history; no fallback affected these results.

The trailing equal-weight series is a risk proxy for the current constituent set,
not a claim that those constituents were held or selected throughout the prior 63
sessions. Identities and closes come from each original sample's storage cutoff.
SPY identities are taken from decision-dated market history; no trend signal gates
exposure. Neither realized future volatility nor future returns set allocations.

Saved 75%-exposure wealth increments, costs and unresolved weights are scaled by
`new_exposure / 0.75`. The original fixed-share paths and the 0/4/10/25 bps fee
convention remain intact. Original fixed 75/25 momentum and SPY paths are verbatim
controls. No constituent weights or missing-return assumptions were optimized.

## Later Results

These are cumulative net price returns across the same 19 later holding windows,
January 2025 through August 2026, **not annual returns**. The table uses 10 bps costs
and the 0% unresolved-terminal-return illustration. Stock drawdowns and realized
volatility remain scenario estimates where position paths are unresolved.

| Portfolio | Cumulative Return | Daily-Path Drawdown | Realized Annualized Volatility | Mean Entry Equity Exposure |
|---|---:|---:|---:|---:|
| Budgeted momentum, sample 1 | +16.31% | -8.61% | 11.10% | 24.14% |
| Budgeted momentum, sample 2 | +15.72% | -9.49% | 10.56% | 23.05% |
| Budgeted SPY, identical samples | +16.85% | -13.97% | 11.26% | 65.46% |
| Fixed 75/25 SPY, original hurdle | +24.88% | -15.07% | 12.97% | 75.00% |

Fixed 75/25 momentum previously returned 48.24% / 49.79%, with 25.10% / 28.08%
drawdown and roughly 35% realized annualized volatility. Its high raw returns did
not survive this large reduction in risk as an advantage over the SPY controls.
That is evidence about this policy, not proof of a general risk-adjusted alpha result.

Momentum's exposure cap never binds: all 47 windows per sample forecast 10% portfolio
volatility at entry. SPY hits the cap in 26/47 windows, including 11/19 later windows.
Its mean forecast is therefore 9.24% overall and 9.21% later, not exactly 10%.
Forecast errors and fixed holding periods also prevent exact realized risk equality.
The later realized volatilities are nevertheless much closer than under equal dollars.

## Return Stability

| Portfolio | Development Cumulative Return | Full-Evaluation Cumulative Return | Full-Evaluation Realized Volatility |
|---|---:|---:|---:|
| Budgeted momentum, sample 1 | +19.77% | +39.30% | 10.16% |
| Budgeted momentum, sample 2 | +6.61% | +23.37% | 10.03% |
| Budgeted SPY | +24.80% | +45.82% | 9.98% |

At 10 bps and the zero-missing illustration, momentum trails budgeted SPY in
development, later and full-evaluation views in both samples. Later excess is
-0.54 / -1.13 percentage points of compounded return, with positive excess in only
8/19 and 9/19 windows. Remove the strongest paired excess period, the 2025-09-12
decision, from both series and later compounded excess becomes **-8.15 / -8.27 pp**.
This exclusion is a dependence diagnostic, not a trading rule or a stitched drawdown.

## Target And Stress

The accepted return hurdle remains **fixed 75/25 scheduled SPY**, not the lower-return
budgeted SPY portfolio. Passing the new comparison alone would not pass that original
hurdle. The drawdown research limit remains 20%, not the 10% volatility budget.

| Budgeted Momentum | Beats Fixed SPY | Beats Budgeted SPY | Drawdown Breaches | Worst Scenario Drawdown |
|---|---:|---:|---:|---:|
| Sample 1 | 0/48 | 6/48 | 0/48 | -14.74% |
| Sample 2 | 0/48 | 2/48 | 8/48 | -24.64% |

Each sample has 48 summary cells: three overlapping period views, four costs and
four unresolved-return assumptions. These are not independent trials or probabilities.
Neither momentum portfolio meets the original joint objective in any cell.

At 10 bps and -100% unresolved-terminal-return stress, later momentum returns are
10.86% / 14.46%, with 8.61% / 9.49% drawdown. Sample 2's development drawdown rises
to 24.10%, and its full-evaluation cumulative return falls to 3.52%. The worst
24.64% case above also includes the higher cost assumption. These are stress paths,
not observed total losses or guaranteed bounds. A trailing volatility estimate does
not eliminate unobserved event, entry or terminal-path risk.

The original 7 / 6 unresolved momentum positions remain unresolved. Fully observed
later invested windows remain 16/19 and 18/19; reduced exposure does not repair the
data. Daily closes omit intraday extremes. SPY remains a matched-schedule price-return
control, not continuous buy-and-hold total return. Dividends, cash interest, taxes and
realistic variable slippage are excluded. Both samples reuse previously examined
market dates, so this is not fresh temporal confirmation.

## Artifacts And Checks

- [equity_momentum_vol_budget_config.json](equity_momentum_vol_budget_config.json)
- [equity_momentum_vol_budget_results.json](equity_momentum_vol_budget_results.json)
- [equity_momentum_vol_budget_results.daily.csv](equity_momentum_vol_budget_results.daily.csv)
- Entry point: `research.price_diagnostic.run_volatility_budget_experiment`.
- VS Code task: **Run frozen portfolio volatility budget comparison**.

Config SHA256: `f0a25daec85d31ff216cdb401c25c257b3224687086c47d6da5f6256be63458d`.
The completed run took 25.9 seconds, reading 44,032 / 46,528 momentum close
occurrences and 3,008 SPY close occurrences per sample for risk estimation. Original
source cutoffs remain 2026-09-12T21:42:23.880932+00:00 and
2026-09-12T22:01:58.649331+00:00. Later read-transaction timestamps do not change them.

The initial attempt stopped on a market identity field mapping before results were
produced. That mapping was corrected and a runner-level regression added; no rule
setting changed. All 72 focused tests passed. Artifact checks verified 188 decisions,
6,016 unique holding/scenario paths, 384 summaries, 384 comparisons and 8,272 exported
daily marks. Daily curves reproduce reported growth and drawdown; path, fee and
missing-weight scaling match the saved controls. Source hashes and sizing weights
are unchanged, and fixed-control summaries reproduce.

Database access was read-only. No provider fetches, database/schema writes, model
fits, parameter searches, live ranking changes or orders occurred.