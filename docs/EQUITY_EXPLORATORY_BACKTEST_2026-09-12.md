# Exploratory Equity Backtest: First Direction

Follow-up: [Disjoint Momentum Replication](EQUITY_MOMENTUM_REPLICATION_2026-09-12.md)
and separately labelled event sensitivities are complete. The original results below
remain unchanged.

## Decision

**Retain 12-1 momentum as the baseline for a second, disjoint ticker-sample test.
Do not add mandatory scanner filters or promote any strategy yet.** Momentum's average
excess return is positive in both periods, but its later-period mean advantage nearly
disappears without the single best period. The strongest scanner headline is also
dominated by one observation. This is a research direction, not evidence of a robust
deployable strategy.

No model was fitted, no parameter was optimized, and no live qualification/rank changed.
The run took approximately nine minutes and used PostgreSQL read-only transactions.

## Experiment

- Same fixed **300-ticker sample** for every strategy, selected by the frozen hash rule
  from 2,919 historical universe tickers, not today's survivors. Of these, 276 were
  historically eligible on at least one sampled rebalance date.
- **47 non-overlapping 21-session holding periods**. Decisions: 2022-09-07 through
  2026-07-16; final position window: 2026-07-17 open through 2026-08-14 close.
  The planned 2026-08-14 decision is immature at the September 3 data cutoff and excluded
  by time alone, before observing outcomes.
- Development: 28 decision dates before 2025. Later evaluation: 19 dates beginning
  2025-01-10. Period labels follow decision dates; a late-December development holding
  can extend into January. There is no training. Prior studies used these historical
  dates, so the later sample is not a proven untouched holdout.
- Momentum: existing close(t-21) / close(t-252) - 1, equal-weight highest decile.
  Scanners: all seven existing versions, fresh long matches on rebalance dates only.
  Same feature-ready universe for momentum, scanners and the primary equal-weight control.
- Entry at the next XNYS open; exit at the 21st holding-session close. No stops, targets,
  shorts, holding-time optimization, or daily-trigger waiting experiment.
- Split-adjusted **price returns**, not dividend-inclusive total returns or sector-neutral
  alpha. Sample selection and intended weights are fixed before checking forward paths.

## Primary Comparison

All numbers below are **arithmetic mean return per 21-session period**, not annualized
returns. Illustrative scenario: **10 bps round-trip cost and 0% on unresolved allocated
weights**. Imputed returns are not observations. Cash earns zero when a scanner has no
match. A matching date allocates the whole basket equally, even if only one stock matches;
this exposes concentration and is not a deployable sizing policy.

| Strategy | Development | Later | Later Position Observations | Later Active Periods |
|---|---:|---:|---:|---:|
| Feature-ready equal weight | 0.68% | 2.09% | 3,010 | 19/19 |
| **Momentum top decile** | **1.73%** | **3.17%** | **309** | **19/19** |
| Level retest/rejection | 0.41% | 2.44% | 129 | 17/19 |
| Breakout expansion | 8.42% | -0.56% | 30 | 12/19 |
| Failed breakout reversal | -0.63% | 6.62% | 25 | 11/19 |
| Structured trend pullback | 1.92% | -0.73% | 34 | 13/19 |
| Structure reversal | 1.01% | 0.18% | 6 | 4/19 |
| Compression breakout | -0.45% | 1.06% | 2 | 2/19 |
| SMA200 reclaim/rejection | No matches | No matches | 0 | 0/19 |

Same-window SPY price-return control after the same 10 bps cost: **1.21% development**,
**1.62% later**. This is the sampled execution-window control, not SPY buy-and-hold.
The all-retained-member sample control, including names lacking valid momentum, is
0.73% / 2.04% under the same missing-return scenario.

### Momentum: Directionally Useful, Fragile

- Excess versus feature-ready equal weight: **+1.05 percentage points development**,
  **+1.08 points later** per period. Later excess is positive in **13 of 19** periods.
- Mean cross-sectional Spearman rank IC: **0.023 development**, **0.036 later**.
  These are descriptive correlations, not significance claims or probabilities.
- Later best excess period: decision **2025-09-12, +20.25 points**. Worst:
  **2025-10-13, -18.95 points**. Removing the best period as a post-run concentration
  diagnostic leaves only **+0.015 points (1.48 bps)** average excess over the other 18.
  The primary results retain both extremes unchanged.
- Later period-boundary drawdown is approximately **18.8%** in the illustrated scenario;
  intraperiod drawdown could be worse and was not calculated.
- Ranking buckets are not monotonic: later observed gross means in deciles 9 and 10
  are 5.04% and 3.66%. Do not switch to decile 9 after seeing this result.

| Decision Year | Periods | Equal-Weight Mean | Momentum Mean | Difference |
|---|---:|---:|---:|---:|
| 2022, partial | 4 | 0.36% | 0.61% | +0.25 pp |
| 2023 | 12 | 0.86% | 0.64% | -0.22 pp |
| 2024 | 12 | 0.61% | 3.20% | +2.59 pp |
| 2025 | 12 | 1.81% | 2.34% | +0.52 pp |
| 2026, partial | 7 | 2.57% | 4.61% | +2.03 pp |

### Scanners: Do Not Add Filters Yet

Failed breakout reversal's later result includes a **100.81% price return in DOCN**
from the April 15 decision, when it was the only match. Removing that best period as
a diagnostic reduces its later mean to **1.39%**, versus **2.06%** for equal weight on
the same remaining dates. Its development result was negative. Keep the observation
in the primary result; do not mistake it for repeatable scanner value.

Level retest's later headline is modestly above equal weight, but observed excess
versus same-date momentum-decile controls is approximately **-0.10 pp later** and
**-0.57 pp development**. This limited control is not risk/sector neutral and includes
the matches themselves in each bin, but provides no clear incremental selection case.

Mandatory scanner intersections were sparse and did not beat unfiltered momentum in
the later illustrated scenario: momentum + breakout **1.66%** (five positions),
momentum + level retest **1.52%** (nine positions), versus momentum **3.17%**.
Several intersections had no matches. This tests scheduled filtering, not whether
waiting for a daily trigger improves entry timing. Rare/zero-match families are
**under-observed in this design**, not proven useless.

## Costs And Missing Data

Momentum later-period means at round-trip costs 0/4/10/25 bps are **3.27% / 3.23% /
3.17% / 3.02%** in the zero-missing-return scenario. Costs in this range are not the
main uncertainty. Both momentum and the equal-weight control pay a full round trip
each period, so their excess is unchanged by a common cost; real slippage may differ.
Mean changes in target membership weights are 0.28 for later momentum versus 0.057
for equal weight. These are not drift-adjusted executed turnover estimates and costs
are not netted using them.

- 7,005 sampled membership/date observations; **6,644 feature-ready (94.85%)**.
- **131 unresolved outcomes (1.87%)** overall: 99 decision identity/price conflicts,
  31 missing forward paths, and one missing signal bar. No selected unresolved weight
  is removed when calculating the scenario portfolios.
- Among feature-ready names, 31 forward paths are unresolved. Momentum selected seven:
  SUMO, TA, AMAM, GPS in development; ITCI, HEES, COOP later. A missing acquisition,
  rename or delisting path is neither an observed zero nor an observed total loss.
- Uniform -25% returns on every unresolved weight leave momentum excess at **+0.88 pp
  development / +0.98 pp later**. Uniform -100% gives **+0.37 / +0.67 pp**.
- A post-run *adversarial allocation of the frozen scenarios*, assigning -100% to
  unresolved names overweight in momentum and +25% to those overweight in the control,
  reduces excess to only **+0.058 / +0.050 pp**. These are stress assumptions, not
  estimated returns or mathematical bounds on future returns.
- 2,236 sample price rows disagree with available historical membership identity;
  these break feature histories. 157,793 sample/benchmark price rows have no dated
  membership identity for that day. Stable stored IDs are provisional there, not
  independent issuer verification. The frozen rebalance phase does not include the
  previously diagnosed March 4 universe-count discrepancy; no date was removed for it.

The comparison is descriptive and exposure/concentration differ across strategies.
Only 19 later market periods and correlated stocks are available. Dividend omission,
unverified identities, sector/beta exposure, rare matches and prior historical-data
exposure prevent a robust-alpha or live-trading conclusion.

## Next Experiment

1. Resolve the **seven selected missing momentum paths** and inspect the largest
   contributing price paths. This is targeted result validation, not another broad
   infrastructure program.
2. Freeze the next disjoint 300 tickers in the same hash ordering and repeat momentum
   versus equal weight with unchanged dates, horizon and costs. This is cross-security
   replication on the same market history, not new independent temporal evidence.
3. Require acceptable concentration and period stability before adding a model or
   scanner gate. A daily timing experiment would be separately specified; the current
   results do not justify tuning the seven scanners or selecting a new best decile.

## Reproduction

- [Frozen settings](equity_price_diagnostic_config.json)
- [Frozen sample and pre-price cutoff](equity_price_diagnostic_results.sample.json)
- [Full period/scenario results](equity_price_diagnostic_results.json)
- [All sampled membership observations](equity_price_diagnostic_results.csv)

Config SHA256: `7d196fe1529482b566350e577682428ece087730b19f90cffacbd25f3c042dec`.
Source snapshot: `2026-09-12T21:42:23.880932+00:00`, database-enforced read only.
Eleven focused tests cover causal features, identity breaks, exact entry/exit, fixed
weights, null/oracle fixtures and costs. Three sampled scanner histories passed
full-versus-truncated event-key parity. These checks do not certify every detector.

```powershell
.\backend\.venv\Scripts\python.exe backend/scripts/run_alpha_research.py --price-diagnostic-config docs/equity_price_diagnostic_config.json --diagnostic-output docs/equity_price_diagnostic_results.json
```

Reruns select a new database snapshot; compare recorded source hashes before claiming
exact numerical reproduction. No thresholds or settings were changed after performance
was viewed. Leave-one-period and adverse missingness checks above are labelled post-run
diagnostics; they do not replace the frozen primary results.