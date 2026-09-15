# Equity Strategy V2 Research Design

Status: **IMPLEMENTED AND BOUNDED COMPARISON COMPLETE. NO STRATEGY PROMOTED.**

## First Study Result

All three research-only state machines and their nine fixed configurations were
implemented, tested and evaluated. The authoritative final pilot and full study
use matching implementation hashes. Earlier development-pilot artifacts are kept
separately and are not performance evidence.

- [Full report](../backend/backups/equity-signal-backtests/strategy-v2-study-v1/report.json)
- [Summary CSV](../backend/backups/equity-signal-backtests/strategy-v2-study-v1/summary.csv)
- [Training selections and test folds](../backend/backups/equity-signal-backtests/strategy-v2-study-v1/folds.json)
- [Final parity pilot](../backend/backups/equity-signal-backtests/strategy-v2-pilot-v2/report.json)

| Measurement | Result |
|---|---:|
| Implemented strategy families / configurations | 3 / 9 |
| Frozen sample names / names with dated membership | 600 / 598 |
| Original composite replay price revisions verified | 638,298 |
| Sample ticker-date RS63 rank records | 310,532 |
| Configuration-specific triggered events / outcome records | 39,133 / 39,133 |
| Events meeting the next-open entry gates | 14,606 |
| Entries rejected for insufficient target room | 18,066 |
| Entries outside the bracket | 4,923 |
| Entries rejected by chase/boundary conditions | 1,489 |
| Unresolved entries | 49 |
| Chronological test folds | 4 |
| Family/direction/fold selection decisions | 24 |
| Decisions with a selected configuration | 1 |

SFIX (sample 1) and GDRX (sample 2) have no dated membership in this study window;
they were not removed on the basis of their returns. Strength ranks were computed
over the complete dated eligible universe before selecting the sample tickers,
with up to 1,928 eligible names in a rank cross-section. Event counts include the
same opportunity under multiple configurations and must not be read as independent
trade counts. Passing an entry gate does not guarantee a complete measured exit.

The test windows are September 10, 2024-March 12, 2025; March 13-September 11, 2025;
September 12, 2025-March 13, 2026; and March 16-September 3, 2026. No test-period
returns or sample-2 returns were used in choosing a configuration for that fold.
The data vintage is nevertheless reconstructed and the historical period was
examined in earlier research; these are not untouched-market claims.

**No v2 strategy qualified for promotion.** Training selected
`failed_extension_reversal_v1:deadline1`, long, only for the first test window.
On its own signal dates at 10 bps, compared with simple momentum selection:

| Sample | Measured Dates | Compared Signal Opportunities | Mean Strategy Return | Mean Incremental Return |
|---|---:|---:|---:|---:|
| Sample 1 | 32 | 63 | -0.252% | -1.080 percentage points |
| Sample 2 | 36 | 70 | -0.337% | +0.957 percentage points |

These are date-equal-weight opportunity means including valid no-fill cash cases,
not compounded portfolios, annualized returns, or fully costed executable shorts.
The positive sample-2 relative mean is not an absolute positive strategy result.
Both samples fall short of the predeclared 40-date/100-signal test coverage gates.
All 108 walk-forward summary cells (including no-selection cells) are classified
`INSUFFICIENT_EVIDENCE`. No-winner folds are recorded as no selection, not stitched
into a purported profitable portfolio or omitted from the research log.

The 324 full-history variant/control/cost/sample rows are descriptive: 162 have
incomplete comparisons, 145 lack stable incremental edge and 17 fail uncertainty
checks. They do not establish a successful alternative to the training-selected
sequence. The remaining work is not to broaden the parameter search automatically.
Keep this completed trial and decide any new hypothesis separately, without
changing these results or deleting the comparison baseline.

Validation: 12 focused tests passed, covering mirrored long/short triggers,
prefix-causal features and events, lifecycle invalidation, entry restrictions,
non-fill/missingness treatment, future-return-independent controls, training-only
selection, deterministic calendar-block resampling and explicit conflicts. The
eight-ticker final pilot matched serial/four-worker event and transition hashes
exactly (624 configuration-specific events and 5,409 control comparison rows).
Full-study checks verified unique event/summary identities, one outcome per
event, code/output hashes, frozen input coverage, no-fill zero-cost handling and
training/test date ordering. No database research facts, live ranks, option gates,
paper records or original detector definitions were changed; nothing was purged.

## Implementation

The three state machines and nine fixed configurations are implemented in
[strategy_v2.py](../backend/research/strategy_v2.py), with an isolated runner in
[run_equity_strategy_v2.py](../backend/scripts/run_equity_strategy_v2.py).
The executable experiment settings are
[equity_strategy_v2_config.json](equity_strategy_v2_config.json). These new modules
do not replace or modify the old scanner registry, detector definitions, outcome
policies, completed evidence, or live option/stock consumers.

The runner verifies the completed strict baseline's code and artifact hashes,
reads the same frozen source cutoff, computes RS63 percentiles over the complete
dated universe before selecting sample tickers, and verifies original composite
replay price revision coverage. EMA/ATR features use contiguous security segments
and a 200-bar warm-up; ATR is an exponentially smoothed true-range series with
alpha 1/14, shifted one session. Long and short trigger mechanics are tested using
mirrored synthetic prices. Serial/parallel detection must agree in pilot mode.

Fixed entry gates wrap the existing conservative plan-outcome engine. Filled but
unresolved paths remain distinct from valid non-entries. Non-entries hold cash
with zero transaction cost in the opportunity-return comparison; missing paths
are not assigned zero returns. Opposed triggers are retained in a conflict
artifact and do not create an additive confidence score. Individual strategy
portfolios are measured separately; no combined allocation system is implemented.

For the first bounded comparison, train only on sample 1, require at least 100
signal opportunities and 40 signal dates with 80% measured-pair coverage, and
select using 10 bps costs against the 12-minus-1-month momentum baseline. All nine
fixed variants and all rejected/no-winner selections remain visible. Test sample 2
under the same training-selected configuration. Same-date controls include an
eligible equal-weight portfolio, a momentum decile, and the nearest non-triggering
stock by RS63 and 20-session volatility percentile, with a 0.20 caliper on each
and replacement. No sector matching is claimed by this implementation.

The 504/126-session fold schedule purges 21 sessions of signal dates before each
test fold and requires training labels to have economically matured before it.
This is not original-vintage historical observation: all prices remain the fixed
reconstructed vintage. Calendar-block confidence intervals preserve overlapping
labels and cross-sectional date cohorts but are conditional on the selected
configuration sequence; they do not fully integrate model-selection uncertainty.
The relatively short test history contains only a small number of 63-session
blocks. Any apparent research candidate still needs new prospective validation.

Manual commands from the repository root (new, empty output directories required):

```powershell
$python = ".\backend\.venv\Scripts\python.exe"
$runner = ".\backend\scripts\run_equity_strategy_v2.py"
$parent = ".\backend\backups\equity-signal-backtests\all-daily-strict-v2"

# Plan-only preview; verifies the parent batch but does not run the new study.
& $python $runner --batch-dir $parent --output-dir .\backend\backups\equity-signal-backtests\strategy-v2-repeat-v1

# To run a separate repeat, choose a NEW output directory; this study already completed.
& $python -u $runner --batch-dir $parent --output-dir .\backend\backups\equity-signal-backtests\strategy-v2-repeat-v1 --workers 4 --execute
```

Add `--pilot` and choose a separate directory for an eight-ticker workflow check.
The pilot repeats detection serially and in parallel, evaluates entry/exit paths,
and builds control portfolios; it does not select profitable configurations. The
full run writes events, setup/execution transitions, input lineage, control
selections, per-date comparisons, fold decisions, summary CSV and report JSON.
All outputs are local research artifacts. There is no database mutation, live
publication, automatic purge or new continuously running worker.

The user authorized a new R&D direction after reviewing the completed daily
baseline and matched-control studies. When asked about deletion scope, the user
chose **keep data until the new strategies are compared**. Nothing in this design
authorizes a database reset, historical purge, live promotion or brokerage order.

## Decision And Evidence

Build three new, separately versioned strategy contracts, each with long and
short hypotheses. Reuse sound price, calendar, identity and event infrastructure;
do not preserve the old practice of treating every indicator or pattern name as
an independent directional vote. This is a redesign of executable hypotheses,
not a claim to have invented or demonstrated a new source of alpha.

The completed studies in [EQUITY_SIGNAL_EVIDENCE_SCORECARD.md](EQUITY_SIGNAL_EVIDENCE_SCORECARD.md)
motivate these choices:

- No existing standalone cell passed the baseline evidence screen.
- Compression and breakout expansion overlap on 383 of 499 compression
  ticker/date/direction keys. Compression is an attribute of expansion, not an
  additional vote. This one-way overlap does not prove identical information.
- Structure reversal overlaps level retest on 59.4% of its detection keys.
  Location, transition and confirmation should have separate meanings.
- Momentum pullback's matched 21-session lift changes sign across samples.
  A pullback condition is not the same as a demonstrated resumption trigger.
- Failed-breakout short had positive matched means in both samples, but only
  21 shared periods. It is a hypothesis lead, not a discovered profitable rule.
- The prior common-grid comparison was underpowered in 346 of 348 primary rows.
  Requiring the two ticker samples to signal on the same dates discarded useful
  evidence. That design remains preserved, but should not govern the next study.

The current outcome data can suggest hypotheses and reveal overlap, execution
problems and weak assumptions. It cannot tell us the outcomes of a changed entry,
stop or exit rule. V2 must be replayed from source inputs under its own identities.
Negative old results are not a license to invert their trade directions.

## Common Contract

These are starting parameters chosen for clarity, not fitted optimal values.
Freeze the final machine-readable configuration before the first v2 result run.
Every parameter, control, horizon and cost scenario counts in the experiment log.

- Initial scope: daily signals on the existing point-in-time liquid-equity
  universe and the original two disjoint ticker samples. Do not mix timeframes.
- Use the strict identity-aware, cutoff-bounded source contract, including exact
  XNYS sessions. The audited universe count discrepancy stays visible. Missing
  identities and unsupported corporate actions are not silently repaired.
- A signal exists only after its complete confirmation bar. Enter at the next
  XNYS open, subject to the declared entry tests. Missing entry is unresolved;
  a valid zero-volume entry is a non-fill. No delayed entry or replacement stock.
- ATR is 14-session Wilder ATR through the bar preceding the applicable anchor
  or trigger, explicitly recorded. All rolling reference ranges exclude the
  breakout/trigger bar. Freeze structural reference prices at setup activation.
- EMA20 and EMA50 use only observed closes and an explicit warm-up contract.
  RS63 is the stock's 63-session close return minus SPY's matched return.
  Its percentile is computed over the dated eligible universe, before observing
  any subsequent returns. Do not use today's constituents or sector labels.
- Long and short are separate evidence cells. Mirror mechanics where sensible,
  but do not assume equal profitability, liquidity, event risk or shortability.
- Live short feasibility requires borrow/locate and cost information. Without
  it, short stock results are signed-return research proxies, not deployability
  proof. A bearish signal is also not automatically an option purchase.
- One fresh event per setup lifecycle. Record SETUP, TRIGGERED, FILLED,
  NOT_FILLED, INVALIDATED and EXPIRED transitions. A setup is not an entry alert.
- Entry requires stop < entry < target for longs, reversed for shorts, and at
  least 1.0 unit of target room per unit of initial stop risk at the actual open.
  This ratio is a trade-plan constraint, not an expectancy or win-probability claim.
- Use fixed initial structural stops and targets in the first version. Intraday
  gaps through a stop use the executable open proxy; ambiguous same-bar stop and
  target hits use the existing conservative stop-first treatment. No partial
  exits, trailing optimization or position scaling in the first comparison.
- Keep 5/10/21-session directional outcomes as diagnostics separately from each
  strategy's actual plan exits. Intraday target/stop times inferred from daily
  bars remain ambiguous; a daily-bar backtest cannot establish precise fill times.

## Three Strategies

### 1. Relative Trend Resumption

Research ID: `relative_trend_resumption_v1`.

Hypothesis: relative leadership/weakness persists, and waiting for a completed
retracement and price resumption may improve entry compared with buying the same
momentum exposure immediately. The extra timing rule must earn its complexity.

Long setup:

1. RS63 is positive and in the top 30% of the dated universe. The close is above
   EMA50 and EMA50 exceeds its value ten sessions earlier.
2. Let A be the highest high in the 20 sessions preceding setup activation,
   with the most recent date breaking ties. Activate on the first close at least
   one prior ATR below A, while no more than ten sessions have elapsed since A.
3. Freeze A and its activation ATR. The pullback must last at least two sessions
   from A, remain no deeper than three activation ATRs, and keep the long trend
   context valid. Expire ten sessions after A or invalidate on a deeper retracement.
4. Trigger on the first completed close above the previous session's high and
   above EMA20. That trigger must precede invalidation/expiry; no alert merely
   because RSI or stochastic is low.

Short setup: negative RS63 in the bottom 30%, close below a declining EMA50;
mirror a rally from the prior 20-session low and require a close below the
previous session's low and EMA20.

Plan: next-open entry; stop beyond the pullback low (long) or bounce high (short)
by 0.10 trigger ATR; target the frozen pre-retracement extreme A; exit at the
first stop/target or the 21st holding-session close. Evaluate the actual opening
gap against the bracket and room-to-risk constraint before admitting a fill.

Distinctive change: explicitly separate relative selection, pullback state,
resumption trigger and structural risk. Multiple EMA/RSI/stochastic scores and
their hand-weighted A+/A grades are not part of this contract.

### 2. Range Breakout Acceptance

Research ID: `range_breakout_acceptance_v1`.

Hypothesis: continued acceptance outside a previously compressed range is more
informative than the first large candle outside it. Waiting can avoid some failed
breakouts but may also worsen entry price; the trade-off must be measured.

Setup:

1. Over the ten completed sessions before the breakout, define upper H, lower L
   and width W = H - L. Require W <= four prior ATRs and positive width.
2. The previous five sessions' mean true range must be <= 0.75 times the previous
   20 sessions' mean true range. Record relative volume, gap, trend and named
   pattern geometry as attributes, not additional hard gates or votes initially.
3. A fresh close beyond H + 0.15 ATR (long), or L - 0.15 ATR (short), arms the
   setup. Freeze H, L, W, breakout ATR and breakout-bar extremes. The preceding
   close must have been within [L, H].
4. Trigger only if the immediately following session also closes beyond the
   same frozen boundary. Otherwise expire the setup. Do not retroactively move
   the boundary or wait indefinitely for a favorable confirmation.

Plan: enter at the next open after acceptance, no farther than one breakout ATR
past the broken boundary. Stop below the lower low of the breakout and acceptance
bars for longs, or above their higher high for shorts, with 0.10 trigger ATR
buffer. Target H + W for longs or L - W for shorts. Apply the actual-open bracket
and room-to-risk constraint. Exit at first stop/target or the 10th holding close.

The range-width target is a declared geometric objective, not evidence that
markets must travel that distance. Volume, compression strength, flags, triangles
and overnight gaps remain attributes for later predeclared ablations.

### 3. Failed Extension Reversal

Research ID: `failed_extension_reversal_v1`.

Hypothesis: a meaningful excursion outside an established range followed by a
confirmed return inside can offer a short-lived reversal toward that range.
Do not label this observed stop-hunting or institutional order flow: daily OHLCV
does not identify the participants or cause.

Short setup:

1. Freeze H, L and midpoint M from the 20 completed sessions before an upside
   extension. The prior close must lie in [L, H].
2. Require a close above H and a high at least 0.50 prior ATR beyond H. This arms
   a failure watch, not a short signal.
3. Within the next three sessions, require a completed close below H - 0.10
   extension ATR and below both its own open and the preceding close.
4. Trigger once, on the first such return. Track the highest high from extension
   through rejection as the stop anchor. Expire if the return does not occur in
   time; do not turn successful breakouts into perpetual reversal candidates.

Long setup: mirror a downside extension, followed by a close above L + 0.10
extension ATR and above its own open and the preceding close.

Plan: next-open entry; stop beyond the episode's extreme by 0.10 trigger ATR;
target the original range midpoint M; reject entries already past the target or
without the required room to risk. Exit at first stop/target or the fifth holding
close. Record trend and relative strength, but do not add outcome-selected regime
filters to this initial version.

This consolidates failed breakouts and some gap/reclaim reversals around one
observable event sequence. SMA200 proximity, FVG/Fibonacci levels and named
reversal shapes are not separately authorized entry rules.

## Conflicts And Application

Retain event-level evidence for all three contracts. Simultaneous same-direction
signals on a ticker do not multiply position size or confidence. Opposed triggers
are explicit conflicts; the initial application view abstains rather than picking
whichever strategy previously had the highest mean return. Research portfolios
for individual strategies remain separate; their performance cannot be summed
into a combined portfolio without a declared allocation/conflict policy.

The eventual application record separates:

- Regime and location facts, including their availability and identity provenance.
- Setup lifecycle and exact trigger condition, strategy/version/direction.
- Earliest entry, invalidation, target, expiry and gap/non-fill restrictions.
- Evidence state, sample/period coverage, uncertainty and qualified policy ID.
- Execution suitability, independently of historical directional evidence.

No manually weighted confluence score or individual-stock win probability is
introduced. A reviewed evidence grade may be added only after its own validation.

For options, stock strategy output is a directional/timing/invalidation hypothesis
with a validity horizon. The existing option engine must separately assess DTE,
payoff, IV/expected move, quotes, event exposure, costs and coherent package marks.
Stock edge does not establish option profitability, and a 21-session signal must
not authorize a short-dated contract simply because direction agrees. Start any
new conditioning comparison in shadow mode; do not change live option gates.

## R&D And Tuning

Fine-tuning is appropriate during R&D, but the distinction between hypothesis
generation and validation remains necessary. Purging data does not erase prior
research exposure or turn reused dates into an untouched holdout.

1. Preserve the old baseline and source inputs. Correct the ADX tie implementation
   under a versioned regression-tested change when working on affected scanners;
   the three new contracts do not use ADX, so that fix is not a prerequisite to
   their detection. Never rewrite the meaning of a completed old study.
2. Implement the three state machines as research-only adapters using existing
   infrastructure. Add prefix-causality tests, mirrored long/short examples,
   lifecycle de-duplication, gap/non-fill cases and exact plan-outcome tests.
   Verify serial/sharded parity on a fixed pilot before a full run.
3. Initial tuning budget: three configurations per family, nine total. Vary
   only the RS63 percentile threshold {60%, 70%, 80%} for resumption, the maximum
   entry chase {0.5, 1.0, 1.5} breakout ATR for acceptance, and failure deadline
   {1, 2, 3} sessions for reversal. Use mirrored thresholds on each side initially;
   any later asymmetric tuning is a separately counted trial. Keep every result.
4. Use blocked, expanding walk-forward development. Start with 504 sessions of
   training and evaluate consecutive 126-session folds. Training labels must be
   available before the fold's first decision; purge overlapping forward windows
   at boundaries using the maximum 21-session horizon. Recompute all transforms
   and any parameter selection from training data only. Sample 2 is a
   cross-sectional check (different tickers), not a clean temporal holdout;
   both samples and these dates have already been examined.
5. A configuration may be selected on sample-1 training folds by mean
   date-equal-weight incremental return against a frozen same-date momentum
   baseline, after predeclared minimum event/date coverage and integrity gates.
   No-trade/no-winner is an allowed selection. Tie-breaking prefers the central
   initial configuration. Selection criteria and coverage gates must be finalized
   in the run configuration before any v2 return is inspected.
6. Match each strategy's signals and controls on that signal date within each
   sample independently. Do not require sample 1 and sample 2 to signal together.
   Controls include eligible equal-weight stocks, simple momentum selection and
   nearby prior-strength/volatility ranks. Define no-signal relative to this
   strategy, not absence of every detector. Select controls without future
   returns, retain unmatched/missing cases, and report sector-matched sensitivity
   only where dated sector provenance is adequate.
7. Retain all eligible signal dates. Estimate uncertainty with calendar-time
   moving-block bootstrap of complete cross-sections, not resampling individual
   trades as if independent. Predeclare a 63-session primary block length and
   2,000 replicates; retain the old non-overlap view only as a diagnostic. Blocks
   must preserve overlapping horizons and common market shocks, and use the same
   resampled dates across compared rules. Effective information is still bounded
   by history length, not the number of detected stocks.
8. Evaluate the declared plan horizon per strategy as primary (21/10/5 sessions),
   the same 5/10/21 directional diagnostics, and predeclared 4/10/25 bps cost
   sensitivities. No borrowing assumption is a substitute for actual shortability.
   Include every tried configuration, side and primary contrast in multiplicity
   accounting; diagnostic horizons cannot replace a failed primary result.
9. Require stable positive incremental behavior on forward test folds and both
   ticker samples, adequate coverage and cost tolerance before a research pass.
   Freeze any survivor for new prospective observation. Do not claim statistical
   independence or reliable probabilities merely because a bootstrap interval is
   positive. No variant is required to pass; stop after the declared budget.

The prior 20% drawdown criterion belongs to a later, fully specified combined
portfolio assessment, not an individual detector-quality threshold. A new
portfolio needs exposure, sizing, overlap and execution rules before comparing
its return and drawdown against SPY.

## Retention And Delivery

Keep source bars, historical universe revisions, security identities, corporate
actions, existing qualifications, option data, repair evidence and paper history.
Keep the completed baseline reports, configurations, exact code provenance and
required rerun inputs until comparison is complete. New output directories and
new event/policy versions are cheaper and safer than deleting the research basis.

After the v2 comparison, propose a scoped cleanup of reproducible historical
working facts. It must have a reviewed exact-ID/table scope, verified archive,
dependent-reference checks and a dry-run count. No blanket `TRUNCATE`, source-data
purge, option deletion or live/paper-state reset is implied by the R&D redesign.

Implementation order is: freeze executable specifications and budgets; implement
and test three research adapters; run one causal/parity pilot; perform the bounded
walk-forward comparison; decide retain/simplify/park; only then discuss application
replacement and cleanup. The isolated research implementation now exists; pilot
and comparison results are recorded separately from the proposed hypotheses.
The current registry, runtime behavior and completed study artifacts are unchanged.