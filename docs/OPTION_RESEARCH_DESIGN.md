# Option Research Design

What must change in equity research before option analysis can lean on it, and what an
option-native research programme has to look like on its own. This is a research-methodology
document; the normative build specification is
[OPTION_CHAIN_SCANNER_DESIGN.md](OPTION_CHAIN_SCANNER_DESIGN.md) and the implemented behaviour is
described in [OPTION_PIPELINE_CURRENT_STATE.md](OPTION_PIPELINE_CURRENT_STATE.md). The equity
qualification machinery referred to throughout is walked through in
[BACKTEST_WALKTHROUGH.md](BACKTEST_WALKTHROUGH.md).

## Current State, Verified 2026-09-04

```
OPTION_EQUITY_CONTEXT_ENABLED = 'false'
equity qualification revisions              0
equity context snapshots               25,027
  with qualified direction                  0
option strategy candidates              1,922
option signal decay outcomes               95
option_strategy_candidates equity columns: []
```

Three separate things must be true before an equity verdict can influence an option decision, and
none of them is true today: the feature flag is off, the equity-context columns are not present on
`option_strategy_candidates` in this database, and no qualification revision exists so every
context snapshot carries a null `qualified_direction`. The strategy engine fails closed in that
state, which is the correct behaviour.

Note also that only one lane ever reaches options. `_qualification_horizon` maps `1d` to `5d`, so
the 10d and 21d verdicts are reporting-only as far as option analysis is concerned, and only a
three-valued `qualified_direction` crosses the boundary - never alpha, t-statistic or calibrated
probability.

## Why Equity Alpha Is Not Option Edge

Equity qualification scores the *mean* sector-adjusted return over a fixed horizon. An option
position is a claim on the *distribution* and on *timing*, so the two can disagree in both
directions:

- A mean alpha of a few basis points is irrelevant to a long call that must clear premium before it
  profits. Passing the equity gate does not imply option profitability.
- Convexity means a signal with zero mean alpha but elevated realised volatility is a good
  long-gamma candidate. It would score `UNRANKED` here.
- Theta means arrival time matters. A move on day 18 of a 21-day horizon scores identically to the
  same move on day 2 and pays very differently.
- For premium buying the decisive condition is realised volatility after the signal exceeding the
  implied volatility paid at entry. Nothing in the equity harness computes it.

The correct response is not to widen horizons or relax thresholds until a scanner passes. Loosening
a test does not produce a more reliable signal, and it defeats the pre-registration, FDR correction
and control arms that make the verdict meaningful. The correct response is to measure the
quantities options actually depend on.

## Part A: What Equity Research Must Add

These are additions to the equity study, not replacements. Each must be computed **at qualification
time and stored in the qualification metrics**, because the retention model discards evidence and
outcome rows once a study is published - see the retention section of
[SCANNER_RESEARCH_CONSOLIDATION_DESIGN.md](SCANNER_RESEARCH_CONSOLIDATION_DESIGN.md). Anything not
computed then cannot be recovered.

1. **Move-size distribution, not just the mean.** Report `P(|return| >= k * ATR)` conditional on the
   signal against the unconditional base rate, for several `k`. This is the quantity that decides
   whether a long option clears its breakeven.
2. **Realised versus implied volatility at signal.** Realised volatility over the horizon following
   the signal, paired with the at-the-money implied volatility on the signal date. The spread
   between them is the single most decisive premium-buying statistic and is computable from chain
   data already collected.
3. **Time to target and time to stop.** `mae_pct`, `mfe_pct`, `stop_first` and `target_first` are
   already computed per outcome; the missing piece is *when*. Theta makes a day-2 target hit and a
   day-18 target hit different trades.
4. **Directional hit rate at a magnitude threshold**, not only mean alpha - the fraction of signals
   producing a move large enough to matter for a given strike distance.
5. **Deliberate horizon and DTE alignment.** A 5d equity horizon corresponds to weekly contracts. If
   near-dated contracts are wanted, add a short lane because the option structure requires it, and
   measure option outcomes on it - not to give the equity hypothesis a second attempt at passing.

## Part B: Option-Native Research

Option demand carries information that does not exist in equity bars. Conditioning options on an
equity scanner produces a derived signal; measuring option activity directly produces a primary
one. This is the more promising of the two lines and should be run as its own hypothesis family
under the same discipline as the equity studies.

### Demand and activity hypotheses

Each needs a pre-registered, point-in-time definition before any measurement:

- open-interest change per contract, and OI concentration by strike and expiry;
- volume relative to open interest, which separates new positioning from churn;
- sweep and block detection - size, aggressor side, and whether prints cross the spread;
- implied-volatility skew shift, and put/call skew at fixed deltas rather than fixed strikes;
- term-structure change, including front-month versus back-month inversion;
- unusual activity relative to the contract's own history, not to a market-wide constant.

### The measured decision is contract selection

Equity research asks a direction question. Option research must answer a harder one: *which
contract*. Strike, expiry and structure are part of the hypothesis, so the study has to record the
selection rule and evaluate the contract actually chosen - not a synthetic at-the-money proxy.

### Outcomes must be modelled at option cost, not equity cost

The equity study charges a flat 4 bps round trip. That figure is meaningless for options, where the
bid-ask spread frequently exceeds several percent of premium. An option outcome measurement needs:

- fills that cross the quoted spread rather than trading at mid;
- the contract multiplier and commission per contract;
- assignment and early-exercise risk on short legs, including dividend-driven early exercise;
- capacity limited by the contract's own open interest and quoted size.

Ignoring these produces results that look strong and are unreachable in practice.

### Controls and gates

The equity harness earned its credibility from its control arms, and the option study needs the
same:

- a **matched control contract** - same underlying, DTE bucket and moneyness, on a date with no
  signal - which is what `qualify_option_conditioning` already expresses as conditioned versus
  control incremental return;
- a **random-contract control** to establish the null;
- a **lookahead oracle** to prove the harness can detect an edge that genuinely exists.

Independent periods must collapse by signal time *and* underlying: five contracts on one underlying
on one day are one observation, not five. The FDR family must be declared before the run, and it
grows quickly here because strategy, DTE bucket and moneyness bucket all multiply the lane count.

### The promotion rule

A scanner-conditioned contract may be admitted only when **both** its upstream equity cell and its
exact option strategy, DTE and checkpoint cell hold `ROBUST_PASS`. An equity pass alone is never
sufficient, and an option-native signal that passes on its own does not require an equity pass at
all.

## Option-Specific Pitfalls

Distinct from the equity failure modes, and easy to get wrong:

- **Earnings volatility crush.** Implied volatility collapses after the announcement, so a correct
  directional call can still lose. Earnings dates must be point-in-time.
- **Moneyness drift.** A contract selected at-the-money is not at-the-money later; outcome
  attribution must track the contract, not the moneyness bucket.
- **Expiry effects.** Pinning and assignment near expiry distort short-dated results.
- **Survivorship in chains.** Contracts that expired worthless must remain in the sample.
- **Corporate actions.** Splits adjust strikes and multipliers; historical strikes are unadjusted,
  which is why the equity adjusted lineage must not be silently mixed into option analysis.
- **Stale quotes.** A wide or crossed quote late in the session produces fictitious marks; quote
  quality gates must be applied point-in-time.

## Sequencing

1. Add the Part A metrics to equity qualification, so future studies retain them by default.
2. Apply the equity-context migration and populate `option_signal_decay_outcomes` at scale; 95 rows
   is not a study.
3. Run the paired conditioning study for one equity scanner and one option structure.
4. Independently, define and pre-register the first option-demand hypothesis family and run it with
   matched and random controls.
5. Only then consider enabling `OPTION_EQUITY_CONTEXT_ENABLED`, and only for cells where both sides
   hold `ROBUST_PASS`.
