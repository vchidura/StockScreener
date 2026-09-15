# Momentum Benchmark Decision: More Growth, Much More Risk

Follow-up: the [Fixed Volatility Budget](EQUITY_MOMENTUM_VOL_BUDGET_2026-09-12.md)
comparison is complete. It reduced risk but did not meet the original return hurdle;
the results and recommendations below describe the earlier fixed-exposure comparison.

## Decision

**Retain momentum as the stock-ranking research baseline, but do not promote the
current portfolio.** At equal 75% equity exposure, it beats matched-schedule SPY in
both later samples in compounded price return, including the declared adverse missing
terminal-return scenario. Its daily volatility is about 2.7 times SPY's, however,
and its later drawdown still exceeds the accepted 20% research limit.

This establishes additional raw return, not that the additional risk is worthwhile.
The compounded advantage is also sensitive to one shared strong period. The next
useful risk-policy hypothesis would be a single, predeclared causal volatility budget
applied to momentum and SPY alike, with the fixed-exposure results retained as controls.
No volatility target, new exposure rule or tuned parameter has been selected or run
in this comparison. A volatility budget would not guarantee a drawdown ceiling.

The user accepted **outperform SPY while respecting the 20% portfolio drawdown research
target** as the objective before this comparison. We operationalized outperformance
as strictly greater compounded net price return over matching windows, not a chosen
annual return hurdle. This is exploratory evidence on previously examined dates,
not an untouched holdout or production qualification.

## Fixed Comparison

- Same two disjoint 300-ticker historical samples and 47 original holding windows:
  28 development and 19 later, with the later decisions starting 2025-01-10.
- Saved momentum selections and daily wealth paths copied unchanged from the
  [Daily Exposure Comparison](EQUITY_MOMENTUM_EXPOSURE_2026-09-12.md).
- Eligible-stock control: every saved feature-ready member in each sample/date,
  equally weighted, including positions whose future returns are unresolved.
  This is the opportunity set available to momentum, not today's whole market.
- Market control: SPY at the same next-session open through 21st holding-session
  close, with each terminal return checked against the original saved study.
- Every portfolio starts each window at 75% equity and 25% zero-return cash.
  Shares are fixed within a window; no daily rebalancing, leverage or trend gate.
- Identical 0/4/10/25 bps round-trip cost scenarios on initial invested notional,
  half at entry and half at exit, without trade netting. SPY is also charged for
  each scheduled liquidation/re-entry.

**Scheduled SPY is not continuous buy-and-hold SPY.** Every portfolio is in cash
between a scheduled exit close and the next entry open. Price returns exclude
dividends, interest and taxes for every portfolio. The comparison isolates selection
under matching execution assumptions; it does not establish outperformance of a
real passive total-return investment. Equal dollar exposure is not equal risk.

## Later Results

The 19 later windows enter from 2025-01-13 through 2026-07-17 and finish by 2026-08-14.
These are **cumulative returns across those windows, not annual returns**. Values
below use 10 bps costs and the 0% unresolved-terminal-return illustration.

| Portfolio | Cumulative Return | Daily-Path Drawdown | Daily Return Std. Dev. |
|---|---:|---:|---:|
| Momentum, sample 1 | +48.24% | -25.10% | 2.20% |
| Eligible-stock equal weight, sample 1 | +33.09% | -16.81% | 0.98% |
| Momentum, sample 2 | +49.79% | -28.08% | 2.19% |
| Eligible-stock equal weight, sample 2 | +23.56% | -17.86% | 0.95% |
| SPY, identical in both samples | +24.88% | -15.07% | 0.82% |

Mean returns per 21-session window are 2.38% / 2.53% for momentum, 1.57% / 1.18%
for the eligible-stock controls and 1.22% for SPY. Compounded momentum excess over
SPY is **23.35 / 24.91 percentage points**, not a claim of risk-adjusted alpha.
Momentum beats SPY in **14/19** windows in each sample, and the eligible-stock control
in **13/19**. The two samples share market dates and are not independent regime tests.

The eligible-stock portfolios' lower risk alone does not establish a superior
strategy either: their advantage over SPY does not replicate, and both trail SPY
over the full evaluation.

## Strongest-Period Dependence

For each pair, remove the period with the largest momentum-minus-benchmark return
from both series, then recompute returns. In the later evaluation the strongest
excess period is the **2025-09-12 decision** for both samples and both benchmarks.

| Sample / Benchmark | Original Cumulative Excess | Cumulative Excess Without Best Period | Mean Excess Without Best Period |
|---|---:|---:|---:|
| 1 / SPY | +23.35 pp | +1.82 pp | +28.26 bps |
| 2 / SPY | +24.91 pp | +4.82 pp | +52.95 bps |
| 1 / eligible-stock equal weight | +15.15 pp | -3.95 pp | +1.11 bps |
| 2 / eligible-stock equal weight | +26.23 pp | +6.26 pp | +57.33 bps |

The positive residual against SPY is worth recording, but is much smaller than the
headline result. In sample 1, a barely positive arithmetic excess against the
eligible-stock benchmark becomes a negative compounded excess. This is why average
holding-period return alone is insufficient to assess the portfolio.

This is a descriptive dependence check, not a tradable exclusion rule. Removing a
period creates an artificial return series; no drawdown is reported for that series.

## Other Periods

| Portfolio | Development Cumulative Return | Full-Evaluation Cumulative Return |
|---|---:|---:|
| Momentum, sample 1 | +40.17% | +107.79% |
| Eligible-stock equal weight, sample 1 | +13.60% | +51.19% |
| Momentum, sample 2 | +24.06% | +85.83% |
| Eligible-stock equal weight, sample 2 | +8.28% | +33.80% |
| Scheduled SPY | +27.70% | +59.47% |

Momentum's SPY outperformance does not replicate in development: sample 2 trails
by 3.64 percentage points and has a 21.11% scenario drawdown. Full-evaluation momentum
outperformance is positive in both samples, but its maximum daily drawdown remains
25.10% / 28.08%. Period views reset capital independently and overlap; they are not
three independent experiments.

## Missing Paths And Target Assessment

Whole-history stock portfolio paths are **scenario estimates**, not fully observed
loss histories. Originally unresolved positions retain their intended weights and
are held at relative value 1 until the final mark, where the declared -100%/-25%/0%/
+25% terminal assumption is applied. Their partial price histories are not silently
substituted. SPY has complete observed paths with modelled costs in all 47 windows.

The eligible-stock control has 31 / 20 unresolved position-windows in the two samples.
Its fully observed windows number 24/47 and 31/47 overall, but only **5/19 and 10/19
later**. Momentum has 16/19 and 18/19 fully observed later windows. No omitted windows
are stitched into an allegedly observed full-portfolio drawdown. Daily closes also
miss intraday extremes, even where all positions are observed.

At 10 bps and the -100% unresolved-terminal-return stress:

| Portfolio | Later Cumulative Return | Later Drawdown |
|---|---:|---:|
| Momentum, sample 1 | +29.00% | -25.10% |
| Momentum, sample 2 | +43.12% | -28.26% |
| Eligible-stock equal weight, sample 1 | +22.26% | -17.26% |
| Eligible-stock equal weight, sample 2 | +18.03% | -17.86% |
| Scheduled SPY | +24.88% | -15.07% |

Momentum still beats scheduled SPY in this later stress, but fails the drawdown
target. These common missing-return assumptions are not worst-case bounds on relative
performance or future loss.

All declared period/cost/missing-return combinations were assessed:

| Sample / Portfolio | Beats SPY | Breaches 20% Drawdown | Meets Both |
|---|---:|---:|---:|
| 1 / momentum | 40/48 | 36/48 | 12/48 |
| 2 / momentum | 32/48 | 44/48 | 4/48 |
| 1 / eligible-stock equal weight | 12/48 | 0/48 | 12/48 |
| 2 / eligible-stock equal weight | 4/48 | 2/48 | 4/48 |

None meets both objectives across the full scenario set. These overlapping assumption
cells are **not success probabilities**. The 20% target is an assessment limit, not
a stop order, liquidation policy or future guarantee.

## Reproduction And Validation

- [equity_momentum_benchmark_config.json](equity_momentum_benchmark_config.json)
- [equity_momentum_benchmark_results.json](equity_momentum_benchmark_results.json)
- [equity_momentum_benchmark_results.daily.csv](equity_momentum_benchmark_results.daily.csv)
- Entry point: `research.price_diagnostic.run_benchmark_experiment`.
- VS Code task: **Run frozen momentum benchmark comparison**.

Config SHA256: `30de26b2d9765086ee186c12a4a05d7190ee69c27a900b57e22cd39c9f4f3316`.
Runtime: 25.7 seconds. Loaded 140,217 / 149,073 daily bar occurrences for 6,644 /
7,061 eligible-stock positions and 47 SPY windows per sample. Original storage
cutoffs remain 2026-09-12T21:42:23.880932+00:00 and 2026-09-12T22:01:58.649331+00:00.

All source JSON/CSV byte hashes and the saved exposure report hash match. Momentum
paths are verbatim copies; control summaries and all benchmark terminal outcomes
reproduce. The artifacts contain 4,512 unique holding/scenario paths, 288 summary
cells and 288 paired comparisons. The daily CSV contains the 10 bps, 0%-missing
illustration; the JSON retains all scenarios.

The 63 focused tests cover the existing causal/cutoff contracts plus compound-return
arithmetic, removal of the best excess rather than absolute period, missing-weight
retention, cost scaling, changed selection, invalid windows and endpoint mismatch.
Database reads used `REPEATABLE READ, READ ONLY`. No provider calls, schema mutations,
model fits, parameter searches, live rank changes or orders occurred.