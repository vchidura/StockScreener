# Momentum Exposure And Daily Drawdown

Follow-up: the [Scheduled SPY Trend Filter](EQUITY_MOMENTUM_TREND_2026-09-12.md)
has been tested without altering the results below. It did not meet the provisional
20% drawdown target and is not recommended as tested.

## Decision

**Keep momentum as the selection baseline, and prefer simple exposure control over
the tested inverse-volatility reweighting for the next risk-focused comparison.**
The fixed 75% equal-weight / 25% cash allocation reduces daily volatility and drawdown
more consistently, but does so by taking less equity exposure, not by creating alpha.
It is a research control, not a recommended account allocation or optimized risk limit.

The important correction to the earlier risk interpretation is that **daily-close
drawdown is substantially worse than drawdown measured only at 21-session boundaries**.
Even the cash allocation has roughly 25-28% later-period scenario drawdown. A tolerable
drawdown budget needs to be chosen before further exposure changes or live use.

## Frozen Comparison

- Three portfolios: fully invested equal-weight momentum, **75% equal-weight momentum
  plus 25% cash**, and fully invested capped inverse-volatility momentum.
- Same saved holdings and sizing weights in the two disjoint 300-ticker samples,
  with the original next-open entry and 21st-session-close exit. No rescoring,
  replacement stocks, daily rebalancing, optimized cash ratio or leverage.
- Positions have fixed share quantities within each holding window; cash earns zero.
  Exposure returns to its target only at the original scheduled entries.
- Original system-observed and database-creation cutoffs are reused. Known position
  endpoints and both full-exposure portfolio endpoints reproduce the prior results.
- The same 0/4/10/25 bps round-trip cost assumptions are charged on initial invested
  notional. Half the additive fee is deducted at entry and half at exit; it is not a
  broker-level financing, exact share-rounding or turnover-netting simulation.
- The first sample's corporate-event endpoint sensitivity is not used to invent daily
  settlement paths. Both original samples retain their unresolved positions.

## Later-Period Results

The table uses **10 bps round-trip cost** (7.5 bps on total portfolio capital for the
75% allocation) and the **0% terminal-return scenario for unresolved weights**. Mean
returns are per 21-session period, not annualized. Whole-history drawdowns are scenario
estimates where prices are missing; see the explicit path assumptions below.

| Sample | Portfolio | Mean Return | Daily Return Std. Dev. | Daily-Path Drawdown | Boundary-Only Drawdown |
|---|---|---:|---:|---:|---:|
| 1 | Equal weight, 100% | 3.17% | 2.94% | -32.26% | -18.76% |
| 1 | **Equal weight, 75% + cash** | **2.38%** | **2.20%** | **-25.10%** | **-14.12%** |
| 1 | Inverse volatility, 100% | 2.90% | 2.70% | -31.84% | -15.75% |
| 2 | Equal weight, 100% | 3.38% | 2.94% | -36.06% | -24.34% |
| 2 | **Equal weight, 75% + cash** | **2.53%** | **2.19%** | **-28.08%** | **-18.42%** |
| 2 | Inverse volatility, 100% | 2.52% | 2.51% | -33.18% | -21.69% |

In sample 2, the cash allocation produces nearly the same mean return as inverse
volatility with roughly **5.1 percentage points less daily-path drawdown**. In sample 1,
it gives up about **0.52 percentage points** of mean period return versus inverse
volatility for roughly **6.7 points less drawdown**. Neither result makes it universally
better; the objective and acceptable downside matter.

Worst later daily close-to-close portfolio returns in the same scenario:

| Portfolio | Sample 1 | Sample 2 |
|---|---:|---:|
| Equal weight, 100% | -8.69% | -9.53% |
| Equal weight, 75% + cash | -6.66% | -7.27% |
| Inverse volatility, 100% | -8.79% | -10.18% |

Inverse-volatility weighting reduced overall dispersion without improving the worst
daily return in either later sample. Boundary-only statistics had obscured much of
this risk. Daily closes still do not observe intraday peaks, troughs or execution gaps.

### Development Period

| Sample | Portfolio | Mean Period Return | Daily-Path Drawdown |
|---|---|---:|---:|
| 1 | Equal weight, 100% | 1.73% | -20.40% |
| 1 | Equal weight, 75% + cash | 1.30% | -15.55% |
| 1 | Inverse volatility, 100% | 1.32% | -19.35% |
| 2 | Equal weight, 100% | 1.16% | -27.48% |
| 2 | Equal weight, 75% + cash | 0.87% | -21.11% |
| 2 | Inverse volatility, 100% | 1.29% | -25.91% |

The cash control lowers drawdown across both periods and samples, with a corresponding
return reduction. It does not improve stock selection or establish risk-adjusted alpha.

## Observed Risk Versus Assumptions

Sample 1 has **41/47 fully observed holding windows**, including **16/19 later**.
Sample 2 has **42/47**, including **18/19 later**. No constituent is dropped or replaced
because of a missing outcome.

For each position unresolved in the original backtest, the full-history scenario holds
its modelled relative value at 1.0 until the final mark, then applies the predeclared
-100%, -25%, 0% or +25% terminal return. These are **not observed or forward-filled
market prices**, even if a partial path exists. This convention reproduces original
scenario endpoints without claiming knowledge of the intervening path. Its drawdown
estimates are not upper/lower bounds on actual risk.

To avoid relying solely on that convention, fully observed holding windows are also
examined individually. They are not stitched across omitted windows. The worst
peak-to-trough decline *within a fully observed later window* is:

| Portfolio | Sample 1, 16 Windows | Sample 2, 18 Windows |
|---|---:|---:|
| Equal weight, 100% | -23.94% | -27.40% |
| Equal weight, 75% + cash | -18.41% | -20.83% |
| Inverse volatility, 100% | -23.75% | -24.69% |

These figures exclude unresolved windows and are not unbiased whole-strategy maximum
drawdowns. They do confirm substantial intraperiod risk in the observable data and
lower losses for the cash control in these later-window comparisons.

Under the uniform -100% terminal stress for unresolved weights, later daily-path
drawdowns are approximately **-32.26% / -25.10% / -31.84%** in sample 1 and
**-36.62% / -28.26% / -33.18%** in sample 2 (equal / cash control / inverse volatility).
The unchanged sample-1 maxima mean the stressed missing paths did not exceed the
existing peak-to-trough event under this convention, not that missing data is harmless.

## Twenty Percent Research Target

After discussing its meaning, the user accepted a provisional **20% maximum portfolio
drawdown** target on 2026-09-12. This is not a per-stock stop, a one-day loss limit, or
an automatic trading instruction. The target was chosen after these experiments, so
the following is a retrospective assessment, not a preregistered validation result.

All 288 saved summary cells were checked for complete scenario coverage and finite
drawdown values. These comprise two samples, three portfolios, three period views,
four cost levels and four unresolved-return assumptions. They are overlapping views
of the same data, not 288 independent trials or a breach-probability estimate.

| Portfolio | Later Sample 1 Drawdown* | Later Sample 2 Drawdown* | Meets 20% Across Saved Scenarios? |
|---|---:|---:|---|
| Equal weight, 100% | 32.26% | 36.06% | No |
| Equal weight, 75% + cash | 25.10% | 28.08% | No |
| Inverse volatility, 100% | 31.84% | 33.18% | No |

*Decline magnitudes for the existing 10 bps, zero-terminal-missing-return illustration.
Each portfolio already breaches the target in both later samples under this convention.
This does not assert that the unresolved positions actually earned zero.

The largest declines across all saved period/cost/missing-return combinations are
33.79% / 48.58% for full equal weight, 26.24% / 38.73% for the cash control and
34.52% / 53.14% for inverse volatility (sample 1 / sample 2). These stress results
are not actual observed drawdowns, estimated probabilities or future-loss bounds.
The 75/25 control is closest in the illustration but is not accepted under the target.

No new allocation was fitted to obtain a pass, and no old result or configuration was
rewritten. Momentum's selection evidence remains useful for research even though these
portfolio implementations do not meet the downside objective. The next risk policy
must be specified separately and retain return/cost comparisons; a cash-only portfolio
is not evidence of a useful stock-selection strategy.

## Next Research Decision

Use the provisional 20% drawdown target and explicitly state the return/participation
objective for the next risk experiment. Do not choose a finer exposure percentage merely because it looks
best on these already examined dates. The tested 75% allocation still has material
downside, and a historically observed maximum is not a guarantee of future protection.

If the acceptable research drawdown is below roughly 20%, none of the three portfolios
passes the full-history later scenario test in both samples. That would call for a
new, explicitly specified risk policy or a different strategy direction, not automatic
promotion of the cash allocation. Scanner gates and more complex predictive models
remain unmotivated by these findings.

## Artifacts And Validation

- [equity_momentum_exposure_config.json](equity_momentum_exposure_config.json)
- [equity_momentum_exposure_results.json](equity_momentum_exposure_results.json)
- [equity_momentum_exposure_results.daily.csv](equity_momentum_exposure_results.daily.csv)

Config SHA256: `91ed469a7cc4bedf3f55bd6433e443125f5b9f6706a83e7fd6b54f3183e4e820`.
The run took **8.4 seconds** and read 14,369 + 15,237 bar occurrences at the two original
cutoffs. The daily CSV contains the illustrative 0%-missing, 10-bps scenario: one entry
mark plus 21 closes per window, including modelled unresolved weights explicitly.
The JSON retains all 4,512 window/strategy/cost/missing-return scenario combinations.

Thirty-nine focused tests passed, covering fixed shares versus daily rebalancing,
cash/cost scaling, hidden intraperiod drawdown, source-cutoff queries, terminal parity
and correct separation of observed windows from scenario equity histories. Artifact
checks verify 6,204 unique illustrative marks, exposure constraints, unchanged original
study and sizing artifacts, and reproduced endpoints. Editor and patch checks passed.

Entry point: `research.price_diagnostic.run_exposure_experiment`, also available through
the VS Code task **Run frozen daily momentum exposure comparison**. No provider fetches,
database/schema writes, model fitting, qualification updates or live ranking changes
occurred. Dividends, cash yield, detailed settlement and realistic variable slippage
remain outside this exploratory price-return study. Both samples reuse already seen
market dates; this is not independent temporal validation.