# Momentum Replication And Targeted Path Validation

Follow-up: [Fixed Momentum Sizing Comparison](EQUITY_MOMENTUM_SIZING_2026-09-12.md)
is complete. The replication results below remain unchanged.

## Decision

**Momentum remains a credible research baseline across two disjoint ticker samples.
The next experiment should test portfolio risk control, not add scanner filters or
train a more complex model yet.** This is descriptive replication, not robust alpha
or a production trading recommendation.

The second sample improves confidence that the first result was not purely a few
chosen stocks. It does not provide independent market history: both samples have the
same best excess-return date, and their later-period excess returns correlate **0.83**.
Momentum exposure and market regime remain important explanations.

## Unchanged Replication

The second sample uses positions 301-600 in the original frozen hash ordering:
**300 tickers, zero overlap**, verified against the first sample manifest before price
performance was calculated. Dates, eligibility, features, execution, cost assumptions,
missing-data scenarios and top-decile selection are unchanged. Scanner computation is
disabled; the replication tests momentum and its equal-weight controls only.

It again contains **47 non-overlapping 21-session windows**: 28 development and 19
later windows. Runtime was approximately **106 seconds**. There are 7,472 membership
observations, 7,061 feature-ready observations and 221 unresolved outcomes overall.
Twenty feature-ready outcomes are unresolved; six were selected by momentum.

The following are **arithmetic mean returns per 21-session period**, after 10 bps
round-trip costs. Remaining unresolved allocated weights receive 0% in this illustrative
scenario. These are not annual returns, dividend-inclusive returns or observed values
for the unresolved positions.

| Run | Development Equal Weight | Development Momentum | Later Equal Weight | Later Momentum |
|---|---:|---:|---:|---:|
| First sample, unchanged | 0.68% | 1.73% | 2.09% | 3.17% |
| **Disjoint replication** | **0.46%** | **1.16%** | **1.57%** | **3.38%** |
| First sample, targeted event sensitivity | 0.68% | 1.70% | 2.09% | 3.11% |

The replication's momentum excess is **+0.71 percentage points development** and
**+1.81 points later**. Later excess is positive in **13 of 19** windows, and mean
momentum rank IC is **0.027 development / 0.034 later**.

| Replication Decision Year | Equal-Weight Mean | Momentum Mean |
|---|---:|---:|
| 2022, partial | -0.52% | 0.43% |
| 2023 | 0.60% | 0.82% |
| 2024 | 0.64% | 1.75% |
| 2025 | 1.36% | 3.85% |
| 2026, partial | 1.94% | 2.58% |

### Risk And Sensitivity

- Removing the replication's best later excess-return window leaves **+0.76 pp** mean
  excess, versus **+0.015 pp** for the unchanged first sample. Both best windows start
  **2025-09-12**. Leave-best-out is a post-run diagnostic, not a replacement result.
- The replication's best and worst later excess windows are approximately **+20.56 pp**
  and **-20.20 pp**. Period-boundary drawdown is **24.3%**; intraperiod drawdown could
  be worse. Later 21-session return standard deviation is **11.82%**.
- Assigning every unresolved position -25% gives replication excess of about **+0.44 pp
  development / +1.82 pp later**. The more adversarial allocation of the frozen
  -100%/+25% scenarios gives **-0.50 pp development / +1.50 pp later**. Missing-data
  assumptions still matter; the later result does not erase development uncertainty.
- Replication momentum's six unresolved positions are AIMC, BIIB, ISEE, DICE, CLSK
  in development and SATS later. They were not silently removed. This pass did not
  repair them or classify every missing path as a corporate event.

## Seven First-Sample Missing Positions

SEC closing filings support four cash acquisitions, one cash-and-stock acquisition
and one stock acquisition. Gap's filing confirms the same issuer trading as GAP;
dated provider prices support the August 22 handover, and the exit-date reference
matches the original CIK and FIGIs. The transition-day GAP reference omitted its CIK;
that missing field was not fabricated or treated as a conflicting issuer.

The user approved a **separate sensitivity**: hold cash at 0% through the original
exit, carry received shares to that exit, and use pro-rata fractional-share value.
Actual settlement delay, cash-in-lieu price, dividends, taxes and account-specific
rounding are not modelled. Merger consideration is valued as a receivable; cash is
not redeployed. Both primary backtest artifacts remain unchanged.

| Original Position | Event During Holding Window | Valuation At Original Exit | Gross Price Sensitivity |
|---|---|---|---:|
| TA, 2023-05-09 | BP acquisition, May 15 | $86 cash versus $86.04 entry | -0.046% |
| SUMO, 2023-05-09 | Acquisition, May 12 | $12.05 cash versus $12.04 entry | +0.083% |
| AMAM, 2024-02-08 | J&J acquisition, March 7 | $28 cash versus $27.80 entry | +0.719% |
| GPS, 2024-08-09 | GPS to GAP | GAP $19.42 close versus $22.29 entry | -12.876% |
| ITCI, 2025-03-13 | J&J acquisition, April 2 | $132 cash versus $131.28 entry | +0.548% |
| HEES, 2025-05-13 | Herc acquisition, June 2 | $78.75 + 0.1287 HRI shares at $119.27, versus $95.06 entry | -1.010% |
| COOP, 2025-09-12 | Rocket acquisition, October 1 | 11 RKT shares at $16.46, versus $223.62 entry | -19.032% |

Unadjusted entry prices match the original stored entry units. Split queries over
the relevant windows found no required share-unit conversion for these tickers or
successors. The sensitivity uses the same selections and weights and applies each
reviewed return consistently to momentum and the equal-weight control.

All seven selected momentum gaps receive values in the sensitivity, but **24 other
feature-ready control outcomes remain unresolved** (nine development, fifteen later).
The sensitivity's later momentum excess is **+1.02 pp** and falls to **+0.013 pp**
without its best period. Resolving these seven cases does not solve first-sample
concentration. This is targeted event handling, not a complete total-return backtest.

## Largest Contributors

Seven observed-return checks matched stored entry and exit prices against fresh
per-ticker daily aggregates from the **same provider**: SOUN, METC (positive and
negative windows), OKLO, ROOT, LUNR and DOCN. The large returns were not discarded
or winsorized. This verifies agreement between endpoints, not independent-vendor
accuracy or full historical corporate-action completeness.

The replication introduces other large contributors, including RGTI and EOSE in the
September 2025 window. These have not received the same fresh endpoint review in this
pass. Combined with shared market dates, this limits the strength of the replication.

## Next Experiment

1. Freeze one simple, unlevered volatility-aware sizing rule versus equal-weight
   momentum, preserving the same stock selections, dates and execution rules. Measure
   drawdown and concentration at comparable exposure; do not search for the best settings.
2. Check the six replication gaps and its largest contributors as bounded data work
   alongside that experiment. No global data-hardening project is required first.
3. Keep scanner gates and higher-complexity models deferred. A better sizing result
   would support further research, not automatically authorize trading.

## Artifacts And Verification

- [equity_price_replication_config.json](equity_price_replication_config.json)
- [equity_price_replication_results.sample.json](equity_price_replication_results.sample.json)
- [equity_price_replication_results.json](equity_price_replication_results.json)
- [equity_price_replication_results.csv](equity_price_replication_results.csv)
- [equity_price_path_validation.json](equity_price_path_validation.json)
- [equity_price_event_terms.json](equity_price_event_terms.json), including the primary filing URLs
- [equity_price_event_sensitivity.json](equity_price_event_sensitivity.json)
- [equity_price_replication_comparison.json](equity_price_replication_comparison.json), ordered first sample, replication, event sensitivity

Replication config SHA256: `2ea649618002cb5253c7c8eccf90909fb657ee9e0222fb09de7724c2e5ebcd88`.
Twenty-one focused tests cover sampling separation, unchanged trading rules, price
timing, missing-weight handling, consideration arithmetic and concentration reporting.
Database reads were read-only. Provider responses were cached locally; no database
rows, schema, qualifications or live rankings were changed.

```powershell
.\backend\.venv\Scripts\python.exe backend/scripts/run_alpha_research.py --price-diagnostic-config docs/equity_price_replication_config.json --diagnostic-output docs/equity_price_replication_results.json
```

These tests reuse market dates already seen in earlier research. They do not establish
sector-neutral alpha, dividend-inclusive performance, realistic capacity or a calibrated
probability. No strategy parameters were changed after viewing replication performance.