# Scanner Literature Review

## Purpose

This review converts external evidence into falsifiable implementation decisions. It does not
treat a published result as validation of this portal's detector, threshold, universe, execution
model, or sample. Every candidate still has to pass point-in-time `next_bar_open_v2` evaluation,
costs, independent-period sampling, temporal stability, and false-discovery correction.

## Evidence Map

| Source | External finding | Portal use | Does not establish |
|---|---|---|---|
| [Lo, Mamaysky, and Wang (2000), Foundations of Technical Analysis](https://www.nber.org/papers/w7613) | Systematic, algorithmic pattern definitions can condition return distributions and avoid subjective chart reading. | Keep detector rules deterministic, point-in-time, symmetric, and versioned. Compare conditional outcomes with the same-date scanner baseline. | That any named chart pattern or portal threshold is profitable. |
| [Lee and Swaminathan (2000), Price Momentum and Trading Volume](https://doi.org/10.2139/ssrn.92589) | Past volume contains information about the magnitude/persistence of momentum and possible later reversal. | Study pullback volume contraction, trigger expansion, and breakout participation as conditioning variables. | That `0.85x` contraction or `1.25x` expansion is universally optimal, or that volume is an independent vote. |
| [Cooper, Gutierrez, and Hameed (2004), Market States and Momentum](https://doi.org/10.1111/j.1540-6261.2004.00665.x) | Momentum outcomes differ materially after positive and negative market states. | Evaluate market and sector breadth alignment point-in-time and report chronological/regime concentration. | That breadth should be a hard production gate or that current sector classifications are historically stable. |
| [Moreira and Muir (2017), Volatility-Managed Portfolios](https://www.nber.org/papers/w22208) | Factor risk varies with volatility, and reducing exposure in high-volatility states improved risk-adjusted outcomes in their samples. | Study trailing market-volatility percentile and report low/high-volatility slices. Use volatility primarily for risk context. | That high volatility predicts scanner direction, or that volatility timing rescues a negative-alpha detector. |
| [Harvey, Liu, and Zhu (2016), ...and the Cross-Section of Expected Returns](https://www.nber.org/papers/w20592) | A conventional `t > 2` hurdle is too weak after extensive factor search; multiple testing raises the required evidence threshold. | Apply Benjamini-Hochberg correction to the complete predeclared baseline/filter family and retain raw passes as monitor-only. | That FDR removes selection bias, survivorship bias, or the need for untouched forward evidence. |
| [Bailey, Borwein, Lopez de Prado, and Zhu (2013), The Probability of Backtest Overfitting](https://doi.org/10.2139/ssrn.2326253) | Repeated strategy selection can overfit historical simulations; ordinary holdout methods can be unreliable for investment backtests. | Freeze feature thresholds before rerunning the study, preserve rejected variants, and require forward monitoring before production promotion. | That the current study directly estimates PBO; it does not yet implement combinatorially symmetric cross-validation. |
| [Brier (1950), Verification of Forecasts Expressed in Terms of Probability](https://doi.org/10.1175/1520-0493%281950%29078%3C0001%3AVOFEIT%3E2.0.CO%3B2) | Probabilistic forecasts must be scored against realized binary outcomes, not judged only by discrimination or confidence labels. | Report walk-forward Brier score and skill versus a 50% forecast for net-positive outcomes. | That a good Brier score proves positive expected alpha after costs. |
| [Guo et al. (2017), On Calibration of Modern Neural Networks](https://proceedings.mlr.press/v70/guo17a.html) | Accuracy and probability calibration are distinct; held-out reliability diagrams and post-hoc calibration are practical diagnostics. | Report a walk-forward reliability curve and expected calibration error. Gate displayed probabilities independently from scanner significance. | That temperature scaling is appropriate here; the portal currently uses an expanding beta-binomial base-rate forecast, not a neural classifier. |

## Predeclared Portal Hypotheses

| Area | Fixed study slice | Rationale and boundary |
|---|---|---|
| Pullback participation | `pullback_volume_contraction`, `pullback_contract_then_expand` | Volume is a conditioning variable; no extra confluence vote. |
| Pullback shape | `pullback_orderly_speed` at at most `0.50 ATR/bar` | Tests whether slower retracement is preferable without changing the trigger. |
| Pullback location | `pullback_vwap_reclaim`, `pullback_near_swing_origin` at at most `1.50 ATR` | Session VWAP is used intraday; aggregate bars use a disclosed rolling 20-bar volume-weighted price. |
| Compression | `compression_tight_band`, `compression_deep_atr_contraction`, `compression_trend_aligned` | The exact detector is a portal hypothesis, not a literature-derived rule. |
| Failed breakout | `failed_breakout_fresh_level`, `failed_breakout_low_follow_through`, `failed_breakout_participation` | Close-back-inside and low subsequent excursion are tested separately. |
| Better features | `pivot_age_10_plus`, `small_overnight_gap`, `level_clustered` | These are metadata filters; they do not add correlated confidence points. |
| Regime | market breadth, sector breadth, combined breadth, low/high volatility | Same-session daily close or strictly prior daily close for hourly signals. |

All thresholds above were fixed before the expanded rerun. A failed slice remains in the report and
must not be silently retuned. New thresholds require a new scanner/study version.

The declared baseline and filter families include every predeclared row even while a row is below
the 100-event or 40-independent-period qualification floor. Those floors determine whether a row
can earn a raw pass; they do not change family membership. This keeps correction independent of
realized scanner frequency. Immature rows remain `UNRANKED` whatever their p-value or q-value.

## Probability Contract

Scanner probability means the estimated frequency of a positive net return at one specific
scanner/version/interval/direction/horizon. It is not the probability that a ticker rises, reaches
its target, or produces positive alpha.

The expanding calibration procedure begins after 40 independent periods. For each later period it
uses only earlier outcomes and a symmetric 20-observation beta prior. The portal reports:

- posterior net-win probability and interval;
- Brier score and skill versus a constant 50% forecast;
- expected calibration error and a reliability curve;
- live expected net alpha and its 95% interval;
- number of genuinely later periods scored.

A probability is display-eligible only when the underlying primary scanner survives FDR, at least
100 later periods were scored, Brier score is below `0.25`, and expected calibration error is at
most `5%`. Passing these diagnostics earns `RESEARCH_CALIBRATED`, not trade approval. Calibration
does not rescue an unranked or monitor-only detector.

## Remaining External-Validity Limits

- The database largely contains today's selected universe, so delisted-symbol survivorship remains.
- Current sector classifications are mapped backward; historical reclassifications are unavailable.
- The sample is short relative to full market cycles, especially for hourly bars.
- Costs are fixed at 4 bps and do not model spread, impact, borrow, or capacity by ticker and regime.
- Literature results use different universes, decades, execution conventions, and portfolio designs.

The correct use of this review is to constrain hypotheses and raise the evidence bar. It is not an
external endorsement of any portal signal.