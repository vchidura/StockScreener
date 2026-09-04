# Intraday Scanner Research Design

Status: proposed research; no scanner in this document is currently qualified or available as a
production recommendation.

This document defines the next intraday scanner hypotheses for canonical `30m` bars. It specifies
causal triggers, entry timing, initial outcome horizons, data limitations, and the qualification
contract before implementation. Existing product scanners remain documented in
[STRATEGIES.md](STRATEGIES.md); shared statistical rules live in
[SIGNAL_RESEARCH.md](SIGNAL_RESEARCH.md).

## Research Boundary

The scanners consume finalized regular-session canonical bars and persist immutable evidence. A
browser request must never fetch Polygon data, aggregate a full universe, or create a signal.
Detection belongs in the equity worker after bar publication.

Every hypothesis must obey these rules:

- Evaluate a trigger only after its signal bar is finalized and observable.
- Enter no earlier than the next scheduled `30m` bar open.
- Use the first regular-session `30m` bar, 09:30-10:00 America/New_York, as the opening range.
- Compute same-slot volume baselines from strictly prior sessions, never later bars from the signal
  session. The initial baseline is the median of the prior 20 available same-slot observations.
- Resolve SPY and sector context at or before the signal timestamp.
- Emit one event at the first bar of a contiguous match episode. A later bar that still satisfies
  the same state is continuation evidence, not another recommendation.
- Treat continuation and reversal/failure as separate hypotheses and FDR family members.
- Persist unavailable inputs explicitly. Do not approximate missing VWAP, sector context, entry
  bars, or benchmark bars silently.
- Keep scanner grade, score, and context fields diagnostic until their incremental value qualifies
  under a predeclared study.

## Candidate Priority

| Priority | Scanner | Core trigger | Initial outcome horizons |
|---:|---|---|---|
| 1 | Opening Range Breakout | Close beyond the finalized first `30m` range with relative volume and market confirmation | `+30m`, `+60m`, close, next open |
| 2 | VWAP Reclaim/Rejection | Price crosses and holds above or below session VWAP after an opening displacement | `+30m`, `+60m`, close |
| 3 | Intraday Trend Pullback | EMA trend stack, controlled pullback to EMA/VWAP, then resumption close | `+30m`, `+60m`, `+120m` |
| 4 | Failed Opening Breakout | Break beyond the opening range followed by a finalized close back inside it | `+30m`, `+60m`, close |
| 5 | Relative-Strength Continuation | Stock outperforms SPY and its sector benchmark through the opening hour, then confirms | `+60m`, close, next open |
| 6 | Volatility Compression Expansion | Several contracting `30m` ranges followed by volume-backed expansion | `+30m`, `+60m`, `+120m` |
| 7 | Power-Hour Continuation/Reversal | Final-hour break or rejection relative to VWAP, opening range, and session extremes | close-to-next-open diagnostic, next close |
| 8 | Gap-and-Go / Gap-Fade | Opening gap combined with first-hour acceptance or rejection | `+30m`, `+60m`, close |

These priorities reflect data fit and implementation clarity, not measured profitability. No row is
eligible to influence options, confidence, or position sizing until it publishes a qualifying
revision.

## Recommended First Three

### 1. Opening Range Breakout

The first finalized `30m` bar defines a deterministic regular-session range. Long and short
directions are symmetric, but breakout continuation and failed-breakout reversal are separate
hypotheses.

Initial long trigger:

1. The 09:30-10:00 opening-range bar is finalized.
2. A later finalized `30m` close is strictly above the opening-range high.
3. Signal-bar volume is greater than the median volume for that slot over the prior 20 available
  sessions.
4. SPY and the mapped sector benchmark are not strongly bearish at the signal timestamp.
5. Entry is the next available `30m` open, not the breakout close.

Initial short trigger mirrors these rules below the opening-range low. Persist the opening-range
bar ID, range high/low, breakout distance in ATR units, same-slot volume baseline, SPY/sector bar
IDs, signal bar ID, and next-entry bar ID.

Predeclared variants:

- `OPENING_RANGE_BREAKOUT_CONTINUATION`: first qualified close outside the range.
- `FAILED_OPENING_BREAKOUT_REVERSAL`: first later close back inside after an observed outside break.

Do not count multiple outside closes from one uninterrupted move as independent events.

### 2. VWAP Reclaim/Rejection

This family tests whether a held VWAP cross after opening displacement predicts continuation or
mean reversion. It is not equivalent to merely being above or below VWAP.

Initial bullish reclaim:

1. The opening bar closes below its available session VWAP or records a predeclared downside
   displacement from it.
2. A later finalized bar crosses from below to above the cumulative session VWAP.
3. The bar closes in its upper predeclared range fraction.
4. Relative volume meets the same-slot threshold.
5. Entry is the next available `30m` open.

Bearish rejection mirrors the direction. A reclaim that later fails is a new, separately declared
reversal hypothesis rather than a relabeled losing reclaim.

Canonical `30m` bars preserve Polygon's bar-level VWAP where supplied. The scanner must compute
cumulative session VWAP through finalized bar $t$ as

$$
\operatorname{VWAP}_{session,t} =
\frac{\sum_{i=open}^{t} \operatorname{VWAP}_{i} V_i}
  {\sum_{i=open}^{t} V_i}.
$$

If `30m` is later derived from finer canonical bars, each bar VWAP must first be volume-weighted
from its available source values under a versioned derivation policy. Missing VWAP in any required
session bar makes the candidate unavailable; close-price or typical-price proxies are not
permitted.

Persist the opening displacement, prior and signal VWAP relationship, close-location value,
relative-volume evidence, source bar IDs, and exact VWAP policy version.

### 3. Intraday Trend Pullback

This extends the location concept behind Momentum Pullback and Bearish Bounce into a session-scoped
event. It does not inherit either daily scanner's qualification because the timeframe, trigger,
entry, and outcome distribution differ.

Initial long trigger:

1. Finalized bars establish the predeclared bullish EMA stack and minimum trend age.
2. One or two bars pull back toward the selected EMA or session VWAP without invalidating the
   stack.
3. Pullback volume contracts relative to its same-slot history and the preceding impulse.
4. A finalized resumption bar closes back in the trend direction.
5. Entry is the following `30m` open.

The short form mirrors the stack, pullback, and resumption rules. EMA periods, allowed pullback
depth, ATR tolerance, contraction threshold, and resumption close-location threshold must be fixed
in a versioned detector policy before the first outcome run. Do not select the best parameter set
after seeing returns.

Persist EMA values, trend age, pullback bar IDs, pullback depth, volume contraction, VWAP distance,
resumption bar ID, and invalidation level.

## Remaining Candidate Definitions

### Failed Opening Breakout

Require an observed finalized close outside the opening range followed by the first finalized
close back inside. Enter on the next bar. Keep it separate from Opening Range Breakout so a failed
continuation does not retroactively change the original event label.

### Relative-Strength Continuation

Measure stock return relative to both SPY and a point-in-time sector benchmark through the first
hour. Require a later directional confirmation close and enter on the next bar. Missing historical
sector assignment or benchmark bars makes the event unavailable.

### Volatility Compression Expansion

Define compression from a predeclared number of declining true-range or ATR-normalized `30m`
windows, then require the first range expansion close with same-slot volume confirmation. Direction
comes from the expansion close; do not infer direction from compression alone.

### Power-Hour Continuation/Reversal

Evaluate only bars in the final regular-session hour, respecting early closes. Test continuation
and rejection separately against session VWAP, opening-range boundaries, and session extremes.
Because no same-session `30m` entry remains after the final bar, signal-close to next-open movement
is an overnight gap diagnostic, not executable P&L. The initial executable entry is next open, with
next close as its first return horizon. A close entry requires a separately predeclared and
operationally feasible market-on-close policy.

### Gap-and-Go / Gap-Fade

Anchor the gap to the current session open versus the previous regular-session close, then classify
first-hour acceptance and rejection from finalized bars. Gap-and-go and gap-fade are separate
hypotheses. Do not inherit daily Gap Strategy qualification or use the current gap lifecycle as a
formation-time filter.

## Outcome Contract

Initial studies use two distinct return contracts:

- `DIRECTIONAL_HORIZON`: next executable `30m` open to the named horizon, with signed return,
  benchmark alpha, MAE, and MFE.
- `RECOMMENDATION_PLAN`: next executable entry followed by first predeclared stop or target, else
  the horizon exit, including explicit costs and conservative same-bar stop/target ordering.

`+30m`, `+60m`, and `+120m` mean elapsed regular-session bar boundaries after entry, not arbitrary
wall-clock observations. `close`, `next open`, and `next close` resolve through the XNYS calendar,
including early closes and overnight gaps. Missing entry or exit bars remain unavailable.

For cross-sectional timestamps, aggregate ticker outcomes with equal timestamp weight before
computing inference. Space independent observations by the full outcome horizon on the XNYS
calendar. Qualification requires the repository's predeclared event and period floors, positive
net return and benchmark alpha, t-statistic above 2, positive early and late alpha, and
Benjamini-Hochberg FDR control across the complete declared family. Publication remains
`UNRANKED` until every gate passes.

## Data Limitations

The current five-year `30m` history covers the current 386-member universe and was bootstrapped as
`LIVE_OBSERVED`. It provides useful depth for:

- Fixed-cohort exploratory research.
- Scanner mechanics and causal-timestamp validation.
- Relative comparison among predeclared hypotheses.
- Prospective scanner implementation and collection.

It does not establish performance for the historical liquid-US-stock universe. Testing today's
survivors over prior years introduces survivorship and selection bias, excludes historical members
and delistings, and can overstate executable coverage.

Strong historical qualification requires:

- Point-in-time universe membership at each intraday signal session.
- Split, dividend, symbol-change, merger, and delisting treatment defined for intraday bars and
  outcomes.
- Historical sector assignments and point-in-time sector benchmark mappings.
- Delisting-aware entry and exit outcomes.
- SPY and sector benchmark bars observable at every signal, entry, and exit timestamp.
- Same-slot volume histories computed only from data available before each signal.
- Provider correction and missing-bar policies that preserve exact source revision lineage.

The repository can reconstruct daily research universes, corporate actions, sector references,
and grouped daily bars. Those facts do not by themselves create a reconstructed point-in-time
intraday cohort. The current five-year `30m` bootstrap must therefore remain labeled fixed-cohort
exploratory research unless the intraday historical-input contract is built and validated.

## Implementation Order

1. Register versioned detector and outcome policies for Opening Range Breakout only.
2. Build deterministic event fixtures for normal sessions, early closes, sparse bars, missing VWAP,
   provider corrections, and next-session entries.
3. Run a fixed-cohort mechanics study and publish limitations with every result.
4. Add VWAP Reclaim/Rejection as a separate FDR family member.
5. Add Intraday Trend Pullback only after its parameters and episode boundaries are frozen.
6. Build point-in-time intraday historical inputs before making historical-universe qualification
   claims.
7. Collect prospective evidence under the same versions before any options-context eligibility.

No implementation step authorizes automatic execution. These scanners remain research-only until
their own equity-signal and, where relevant, option-conditioning qualifications pass.

## Implemented Detectors

`backend/research/intraday_scanners.py` registers four frozen `30m` sources under detector policy
`intraday_scanner_policy_v1`. Changing any threshold in `INTRADAY_DETECTOR_POLICY` requires a new
`source_version` because qualification revisions are keyed by `(source_name, source_version)`.

| Source name | Version | Directions |
|---|---|---|
| `INTRADAY_OPENING_RANGE_BREAKOUT_CONTINUATION` | `opening_range_breakout_v1` | long, short |
| `INTRADAY_FAILED_OPENING_BREAKOUT_REVERSAL` | `failed_opening_breakout_v1` | long, short |
| `INTRADAY_VWAP_RECLAIM_REJECTION` | `vwap_reclaim_rejection_v1` | long, short |
| `INTRADAY_TREND_PULLBACK` | `intraday_trend_pullback_v1` | long, short |

Frozen thresholds: ATR period 14; relative-volume multiple 1.0 against the median of the prior 20
same-slot sessions; breakout buffer and invalidation buffer 0.10 ATR; VWAP close-location 0.65 long
and 0.35 short; EMA stack 8/21/55 with a 4-bar minimum trend age; pullback at most 2 bars within
0.50 ATR of EMA21 or session VWAP; resumption close-location 0.60; reward-to-risk 2.0; benchmark
adverse-return tolerance 0.30%.

Both declared return contracts are registered per source: `DIRECTIONAL_HORIZON`
(`directional_outcome_v3_sector`) and `RECOMMENDATION_PLAN` (`recommendation_plan_sector_v1`), each
with sector-primary benchmarking and horizons `{"30m": 1, "60m": 2, "120m": 4}`.

Session-relative `close`, `next open`, and `next close` horizons are not yet implemented. The
outcome evaluator currently accepts fixed bar counts only, so those horizons remain deferred rather
than silently approximated.

Detection is not wired into the live equity worker. Evidence is written as `RESEARCH_ONLY` with
quality codes `INTRADAY_FIXED_COHORT`, `LIVE_OBSERVED_BOOTSTRAP`, `REPLAY_ONLY`, and
`UNQUALIFIED_DIRECTION`, and no `analysis_run_id`.

## Running the Research Pipeline

From `backend/` with the local virtual environment:

```powershell
# 1. Replay detectors over canonical 30m bars into an immutable event file.
#    Omit --start so the 60-bar and 20-session warm-ups consume history rather than signals.
.\.venv\Scripts\python.exe scripts\run_intraday_scanner_research.py `
  --output backups\intraday\intraday_events_v1.jsonl `
  --summary backups\intraday\intraday_events_v1_summary.json

# 2. Persist evidence and evaluate both return contracts at every horizon.
.\.venv\Scripts\python.exe scripts\run_intraday_scanner_outcomes.py `
  --events backups\intraday\intraday_events_v1.jsonl `
  --persist-evidence --evaluate

# 3. Qualify under the repository's predeclared floors and publish the revision.
.\.venv\Scripts\python.exe scripts\run_intraday_scanner_outcomes.py `
  --events backups\intraday\intraday_events_v1.jsonl `
  --qualify --qualification-effective-from 2026-09-02T00:00:00+00:00 `
  --output backups\intraday\intraday_qualification_v1.json
```

Useful flags: `--scanner` (repeatable) restricts step 1 to one family; `--ticker` and
`--limit-tickers` produce quick smoke runs; `--minimum-events` and
`--minimum-independent-periods` are exposed for diagnostics only and must stay at 100 and 40 for
any published claim.

Both scripts are idempotent. Event, evidence, and outcome identities are deterministic `uuid5`
values, so a repeated run over the same input file re-persists nothing new.

Results surface at `GET /api/scanner-events/qualification?interval=30m` and in the Stock Research
scanner view under the 30 minute frame. Every published row carries
`research_cohort = FIXED_COHORT_EXPLORATORY` and the survivorship limitations listed above.

### Harness validation controls

The negative result above is only meaningful if the evaluator can detect a real signal. Two
calibration lanes are registered in `INTRADAY_SCANNER_REGISTRY` with `is_control = True` and run
through the identical policies, evaluation, and qualification path. They are excluded from
`qualification_report` by a `CONTROL\_%` filter, so they never reach the product UI or the options
path, and they can be removed at any time by deleting their rows from `equity_research_outcomes`
then `equity_evidence` and `equity_qualification_revisions`.

- `CONTROL_LOOKAHEAD_ORACLE` (`lookahead_oracle_v1`): direction is the sign of the entry bar's own
  open-to-close move. It deliberately reads the future and must qualify.
- `CONTROL_RANDOM_DIRECTION` (`random_direction_v1`): direction is SHA-256 parity of the signal bar
  id. It carries no information and must not qualify.

Both sample one bar in 32 deterministically. Run over 40 tickers, 2021-09-10 through 2026-09-02:
15,078 oracle and 15,275 random events, 91,055 outcomes, zero pending.

| Lane | Direction | Horizon | Events | Gross | Net | Alpha | Alpha t | State |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| Oracle | long | `30m` | 7,664 | +0.00349 | +0.00309 | +0.00204 | 33.78 | `ROBUST_PASS` |
| Oracle | short | `30m` | 7,412 | +0.00341 | +0.00301 | +0.00197 | 39.19 | `ROBUST_PASS` |
| Oracle | long | `60m` | 7,663 | +0.00340 | +0.00300 | +0.00190 | 15.06 | `ROBUST_PASS` |
| Oracle | short | `60m` | 7,412 | +0.00338 | +0.00298 | +0.00201 | 16.01 | `ROBUST_PASS` |
| Oracle | long | `120m` | 7,662 | +0.00362 | +0.00322 | +0.00203 | 8.20 | `ROBUST_PASS` |
| Oracle | short | `120m` | 7,413 | +0.00351 | +0.00311 | +0.00210 | 8.06 | `ROBUST_PASS` |
| Random | long | `30m` | 7,595 | +0.00007 | -0.00033 | -0.00031 | -5.20 | `UNRANKED` |
| Random | short | `30m` | 7,677 | -0.00008 | -0.00048 | -0.00044 | -7.28 | `UNRANKED` |
| Random | long | `60m` | 7,596 | +0.00012 | -0.00028 | -0.00028 | -2.11 | `UNRANKED` |
| Random | short | `60m` | 7,676 | -0.00013 | -0.00053 | -0.00047 | -3.61 | `UNRANKED` |
| Random | long | `120m` | 7,595 | +0.00026 | -0.00014 | -0.00013 | -0.46 | `UNRANKED` |
| Random | short | `120m` | 7,677 | -0.00047 | -0.00087 | -0.00080 | -3.24 | `UNRANKED` |

All six oracle lanes pass; all six random lanes do not. Three things follow.

First, entry and exit wiring is correct. The oracle reads the bar the evaluator actually enters on,
which the execution-lag audit identified as the second bar after the signal, and scores t = 39. A
misaligned entry would have driven this to zero.

Second, there is no look-ahead leak. Random gross return is within 5 bps of zero and net return is
gross minus exactly one round-trip cost.

Third, the oracle's gross return of 0.35% matches the mean absolute intrabar open-to-close move,
which is what perfect direction capture on a single bar should earn. The magnitude is right, not
merely the significance.

The random lane also settles the interpretation of the scanner results. Its signature at `30m` is
gross near zero, net near minus one cost, and an alpha t of -5.2 to -7.3 driven by precise
measurement of a constant cost drag at large sample. The four scanners produce gross near zero, net
of -0.0003 to -0.0005, and alpha t of -5.3 to -12.5. **The scanners are statistically
indistinguishable from random direction.** Their negative t-statistics are the null signature, not
evidence of a tradable short edge, and reversing their direction would not help.

Two validation techniques remain unimplemented and would further constrain the harness: a
shuffled-label test on real events, and an independent reimplementation of outcome computation
sharing no code with the evaluator.

### Detector verification

The controls above validate the evaluator, not the detectors. A completely broken detector would
produce the same null result, because the oracle fabricates its direction and never consults
detector output. Establishing that the scanners detect what they claim requires a separate check.

`backend/scripts/verify_intraday_detectors.py` recomputes every trigger condition from the
`equity_canonical_bars` view and asserts it against the persisted payloads. It deliberately imports
nothing from `research.intraday_scanners`; a shared implementation agreeing with itself would prove
nothing. Run it with `--tickers N`.

Checks applied per event: `signal_time` equals the signal bar end; no signal on a session's final
bar; no source bar postdates its own signal; stored opening range equals the session's first `30m`
bar high and low; the signal close is beyond the level by at least the 0.10 ATR buffer;
signal volume exceeds an independently recomputed prior-20-session same-slot median; session VWAP
recomputed from bar VWAP and volume matches the stored value; the VWAP cross direction and
close-location thresholds hold; pullback bars show contracted volume and an aligned EMA stack; and
consecutive same-lane episodes are separated by a genuine state break, meaning a return inside the
range for breakouts, a fresh break outside for failed breakouts, and an opposite cross for VWAP.

Result over 16,450 events across 10 tickers:

| Scanner | Events checked | Failures |
|---|---:|---:|
| `INTRADAY_OPENING_RANGE_BREAKOUT_CONTINUATION` | 5,555 | 0 |
| `INTRADAY_VWAP_RECLAIM_REJECTION` | 6,122 | 0 |
| `INTRADAY_TREND_PULLBACK` | 960 | 0 |
| `INTRADAY_FAILED_OPENING_BREAKOUT_REVERSAL` | 3,813 | 89 |

The 89 are a single boundary convention, and `close_still_outside_range` is zero, which pins the
cause exactly: the failed-breakout detector treats a close landing precisely *on* the opening-range
level as back inside, because its test is non-strict. The design says "closes back inside", which
strictly read would exclude the boundary. This affects 2.3% of one scanner's events, does not
change any qualification outcome, and is recorded here rather than silently corrected because
changing it would require a new `source_version`.

With this, the earlier claim that the scanners "carry no information" is supported rather than
assumed. The detectors demonstrably identify the specified patterns, and the evaluator demonstrably
detects signal when signal exists, so the null result is a property of the hypotheses at `30m`
under a one-bar execution lag, not an artefact of either code path.

## Fixed-Cohort Study Result, 2026-09-03

Report identity `585816160198274e0bf905cc1b314ad50954b53a83fe14537fc7d1deb6ef613b`, evaluation
version `intraday_scanner_qualification_v1`, detector policy `intraday_scanner_policy_v1`.

Cohort: 188 tickers, 1,234 sessions, 2021-10-04 through 2026-09-02. 296,694 events produced
1,777,130 outcomes, of which 1,581,525 reached `ENTERED` status. The remaining 11% are explicitly
unavailable rather than silently dropped.

**No scanner qualified. All 48 revisions published `UNRANKED`; zero `ROBUST_PASS`, zero
`MONITOR_ONLY`.** Every event and independent-period floor was cleared by a wide margin, so the
failure is a genuine absence of edge rather than insufficient sample.

Best row in the entire family, by alpha t-statistic:

| Scanner | Direction | Mode | Horizon | Events | Periods | Mean net alpha | Alpha t | FDR q |
|---|---:|---|---:|---:|---:|---:|---:|---:|
| Intraday trend pullback | short | `DIRECTIONAL_HORIZON` | `120m` | 7,060 | 1,966 | +0.0002 | 0.73 | 0.473 |

That is the only lane with positive alpha, and it is statistically indistinguishable from zero. All
47 remaining lanes are negative, 44 of them with alpha t below −2.

The mechanism is visible in the numbers. Mean net return clusters at −0.0003 to −0.0005 across
every family, direction, and horizon, against a predeclared round-trip cost of 4 bps. Gross return
is therefore approximately zero everywhere: these triggers select bars with no measurable
directional information at `30m`, and the cost model converts that into a reliably negative
expectancy. The strongly negative t-statistics scale with sample size — the `30m` horizon, with the
most independent periods, shows the most negative values — which is the signature of detecting a
small constant cost drag with high confidence, not of a tradable short edge.

`RECOMMENDATION_PLAN` is consistently worse than `DIRECTIONAL_HORIZON` at the same horizon,
indicating the 2R bracket at 0.10 ATR invalidation stops out far more often than it reaches target.

### Execution-lag audit

The uniformly cost-sized losses prompted an audit of the evaluation harness itself. Verified
correct against 1.78M outcome rows: the round-trip cost is applied exactly once
(`signed_return - estimated_cost - net_return` residual is 0 to nine decimals); there is no
direction sign error (long and short both show gross near zero and net near minus one cost);
`entry_price` equals the entry bar's open in 891,474 of 891,474 rows; and no outcome exits before
it enters.

The audit did establish one material property of the study that is easy to miss. Canonical `30m`
bars are exactly contiguous — `bar_start(t+1) = bar_end(t)`, confirmed at zero seconds of gap — and
evidence is observed at `bar_end(t)`. The repository requires entry strictly after observation, at
three independent layers: the bar query, the outcome path filter, and the
`equity_research_outcomes_check` constraint `entry_time > signal_time`. The bar opening at the
signal instant is therefore not tradable, and entry falls to the open of the *following* bar.

Measured consequence: 94% of entries occur 30 minutes after the signal bar closes, and about 6%
roll to the next session open. This is a deliberate no-instantaneous-execution rule, not a defect —
it refuses to assume a fill on the closing print. But it means every result above carries a
one-bar execution lag, so the study tests whether signal persists *beyond* the first 30 minutes,
not whether the trigger has immediate directional information. For hypotheses whose edge is
expected to decay within one bar, that distinction matters, and the negative results should be read
as conditional on this conservative assumption.

Relaxing it is a schema-level decision, not a code change, and would require a new entry model and
outcome policy version rather than an in-place edit.

Two further design properties, not defects: the relative-volume gate of `1.0 x` the prior 20-session
same-slot *median* admits roughly half of all bars by construction, so it contributes little
selectivity; and the plan bracket risk of roughly 0.2-0.3 ATR sits well inside the measured mean
adverse excursion of 0.4-0.9%, which is why stops resolve before targets. Entries that gap outside
their bracket are correctly excluded as `NOT_TRIGGERED` rather than mispriced.

### Cohort and benchmark audit

Sector composition is adequate and is not driving the result. The cohort spans 13 sectors with
Financials 15.4%, IT Semiconductors and Hardware 15.2%, and Industrials 13.8%; the top three
account for 44%. Real Estate (2 tickers) and Communication Services (4 tickers) are thin enough
that their sector legs are effectively single-name.

6.4% of events carry no sector classification and fall back to SPY as their sector benchmark, so
`sector_net_alpha` equals `net_alpha` identically for those rows and sector-primary qualification
silently degrades to market-primary. This is a real but modest defect worth fixing before the next
study.

Sector neutralisation is close to a no-op at this timeframe in any case: market and sector alpha
correlate 0.88 to 0.94 with a mean divergence of 0.00002. The choice of sector-primary over
market-primary does not change any conclusion here.

Statistical power was never the binding constraint. Backing the portfolio standard error out of the
observed run gives a minimum detectable alpha near 1.1 bps at the `30m` horizon for this cohort,
against a 4 bps cost hurdle. Power scales as the inverse square root of total events, which implies
roughly 50 tickers as a floor and 100 or more as comfortable for any future intraday study; below
about 10 tickers the minimum detectable effect exceeds the cost hurdle itself, so neither a pass nor
a fail at that size would be informative.

Interpretation and consequences:

- None of these four families is eligible for recommendations, confidence, position sizing, or
  option conditioning. Their published state remains `UNRANKED`.
- The negative results are a property of this frozen parameter set. Re-tuning thresholds against
  these outcomes and re-running would be selecting parameters after seeing returns, which the
  research boundary forbids. Any revised threshold set requires a new `source_version` and must be
  declared before its first outcome run.
- The result is fixed-cohort exploratory and survivorship-biased. It cannot be read as evidence
  about the historical liquid-US-stock universe, but the direction of that bias would if anything
  favour the scanners, which strengthens rather than weakens the negative conclusion.
- Before spending further effort on the remaining five candidates in the priority table, the
  cost-versus-signal ratio above should be treated as the governing constraint: a `30m` trigger
  must clear roughly 4 bps of round-trip cost before any of it is executable.
