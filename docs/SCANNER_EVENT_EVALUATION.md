# Scanner Event Evaluation

## Goal

Measure whether scanner setups add directional or entry-timing value before they can influence
recommendations. All captured events are shadow-only and labeled `UNVALIDATED_TIMING`.

Detailed scanner mechanics belong in [STRATEGIES.md](STRATEGIES.md). This document is the living
registry and integration contract. External evidence and its implementation boundaries are mapped
in [SCANNER_LITERATURE_REVIEW.md](SCANNER_LITERATURE_REVIEW.md).

## Composite Setup Registry

MA crossover, gap, Fibonacci, momentum pullback and bearish bounce do not need separate promotion
lanes. Most describe trend, location or timing. Combine them by role into a small number of
falsifiable setups; add one row when a setup is proposed, implemented or promoted.

| Composite setup | Intervals | Required components | Optional context | Status |
|---|---|---|---|---|
| Structured trend pullback 1.0 | 1d, 1wk, 1h | MA trend + swing continuation + pullback + candle trigger | Gap/FVG/Fibonacci proximity, activity | Collecting |
| Level retest/rejection 1.2 | 1d, 1wk, 1h | Gap/FVG/dynamic Fibonacci level + rejection or reclaim + participation | Trend/discovery alignment | Collecting |
| Breakout expansion 1.0 | 1d, 1wk, 1h | Swing/zone break + range expansion + relative volume + strong close | MA trend, cross-sectional rank | Promising: hourly long, 7 bars |
| Structure reversal 1.0 | 1d, 1wk, 1h | Reversal discovery state + swing structure flip + MA/VWAP reclaim | Gap/FVG level, activity | Collecting |
| SMA200 reclaim rejection 1.0 | 1d, 1wk | Declining SMA200 + fresh reclaim + old-high retest + bearish strong close + participation | Sector/regime alignment | Collecting |
| Compression breakout 0.1-shadow | 1d | Contracted range/ATR and channel + range/volume expansion + strong breakout close | Trend alignment | Failed primary qualification; shadow only |
| Failed-breakout reversal 0.1-shadow | 1d | Fresh pivot break + next-bar close back inside + reversal candle | Level age, participation, breadth | Monitor only; raw pass did not survive FDR |

Statuses: `PLANNED`, `COLLECTING`, `INSUFFICIENT_SAMPLE`, `FAILED`, `PROMISING`, `VALIDATED`,
`DEFERRED`.

## Indicator Roles

Indicators are evidence with distinct jobs, not equal-weight votes.

| Role | Detectable inputs | Purpose |
|---|---|---|
| Candidate/regime gate | `xsmom`, discovery state, sector, SPY/QQQ regime | Decide eligibility and plausible direction |
| Structure | SMA/EMA stack or cross, higher/lower swings, ADX | Establish trend stage and direction |
| Location | SMA20/EMA21, gap, FVG, Fibonacci, VWAP deviation | Identify where entry risk can be defined |
| Trigger | Engulfing/wick candle, strong close, stochastic turn, hourly breakout | Timestamp the event |
| Participation | Relative volume, activity percentile, range expansion | Confirm participation |
| Risk | ATR, invalidation swing or zone edge | Set comparable stop, target and R metrics |

Do not award extra confidence merely because correlated trend indicators agree. MA crossover, EMA
stack and ADX should form one structure gate, not three independent votes. Gap, FVG and Fibonacci
can be stored as location variants so their incremental value can be measured.

## Fibonacci Scoring Evaluation

Fibonacci proximity remains structural location context and does not contribute a directional
vote or confluence point. A point-in-time comparison replayed 400 active tickers from 2024-01-01
through 2026-08-14 using dynamic swing thresholds, next-session-open entry, 4 bps round-trip cost,
and 5/10/21-session exits. It compared the legacy latest completed leg, the latest still-valid
completed leg, and a multi-leg variant capped at one directional vote with conflicts suppressed.

None of the 18 variant/direction/horizon combinations qualified. The strongest latest-valid result
was short at five sessions (+0.101% net alpha, `t=1.47`), while its long alpha was negative at five
and ten sessions. Multi-leg long at 21 sessions returned +0.403% net alpha (`t=1.39`) but had only
31 independent periods and did not meet the significance or sample gate. Multi-leg results were
negative at five and ten sessions for both directions, and the 21-session short result reversed
between chronological halves.

Production implications:

- Confirmed, developing, and historical Fibonacci legs remain visible for structural planning.
- Fibonacci levels may remain descriptive target, stop, and retest references.
- Fibonacci proximity and Fibonacci-only retests add zero directional or confluence points.
- A future scoring proposal must qualify independently by direction and horizon under the same
  next-open evaluation contract.

## Scanner Confidence Study

The expanded study evaluated 242,618 matured `next_bar_open_v2` outcomes and 1,041 baseline/filter
rows. In addition to discovery alignment, participation, breakout-close quality, level source,
candle triggers, and `xsmom-1.0`, it tested pullback duration, volume contraction/expansion,
retracement speed, VWAP reclaim and swing-origin distance; failed-breakout level freshness,
participation and follow-through failure; pivot age, overnight gap/ATR and level clustering; and
point-in-time market breadth, sector breadth and trailing market-volatility percentile. Every
filter was paired with its same scanner/date baseline. Daily context used that session's close;
hourly context used the latest strictly prior daily close. Fresh context covered 233,660 outcomes
(96.3%).

Qualification requires 100 events, 40 horizon-spaced periods, positive alpha with `t > 2`, and
positive early/late alpha. Filter qualification additionally requires positive paired incremental
alpha with `t > 2` and positive early/late incremental alpha. Benjamini-Hochberg correction is
applied across both primary baselines and the complete filter family at `q <= 0.05`.

Primary findings:

- Existing hourly long breakout expansion at seven bars is the only robust primary combination:
  3,263 events, 459 independent periods, +0.290% net alpha, `t=3.68`, and `q=0.0147`.
- Daily short failed-breakout reversal at ten sessions passed the raw primary gates with 3,947
  events, 88 periods, +1.204% net alpha, and `t=2.26`, but failed FDR with `q=0.494`.
- Compression short at ten sessions weakened from +1.958% (`t=2.44`) over 39 initial periods to
  +1.181% (`t=1.76`) over 53 periods after extending the replay to 2023. It did not qualify.
- No pullback-shape, VWAP, swing-origin, failed-breakout, pivot-age, gap, level-cluster, rank,
  breadth, sector or volatility filter survived false-discovery correction. Six filters passed raw
  gates; robust filters remain zero. The strongest raw contextual increment was hourly-short
  structure reversal with aligned sector breadth (+1.058%, `t=3.03`, `q=0.443`). Pullback volume
  contraction (+0.152%, `t=2.20`, `q=0.748`) and pivot age of at least ten bars (+0.807%,
  `t=2.26`, `q=0.736`) also failed the adjusted evidence gate.

Failed-breakout alpha was broad across 399 tickers but concentrated in time: 2024 and 2025 alpha
was -0.19% and -0.53%, while 2026 contributed +6.13% over only 14 periods. This temporal
instability prevents promotion even before untouched forward validation. Sector breadth also has
a residual limitation: historical prices are point-in-time, but current sector classifications
are mapped backward and do not capture classification drift.

Portal evidence states now enforce the research boundary:

- `ROBUST_PASS`: primary gates and baseline FDR pass. This describes one scanner/direction/horizon,
  not a per-trade probability.
- `MONITOR_ONLY`: raw primary pass that did not survive FDR.
- `UNRANKED`: insufficient, negative, or unstable evidence; no confidence claim.

Calibration uses expanding beta-binomial forecasts and is downstream of robust evidence. The sole
eligible baseline has 419 out-of-sample periods, calibrated P(win) of 56.576% (95% interval 52.142%
to 61.010%), Brier score 0.246, Brier skill of +1.5% versus a 50% forecast, and expected
calibration error of 4.376%. Its live expected alpha is +0.290% (95% interval +0.135% to +0.444%).
The portal exposes these metrics and its reliability curve only for `RESEARCH_CALIBRATED` rows;
`MONITOR_ONLY` and `UNRANKED` rows remain `NOT_ELIGIBLE` with probability and live-alpha claims
suppressed. Review Priority remains categorical and no scanner changes recommendations.

## Review Priority

Review priority orders research attention; it is not conviction, qualification, or trade approval.
It is categorical rather than a numerical score because the current evidence supports one
contextual distinction, not precise probability estimates.

A point-in-time probe used 70,739 matured next-open outcomes at the first horizon (five daily bars
or seven hourly bars). Relative volume had no top-versus-bottom alpha separation (`t=-0.15`), range
expansion was weak (`t=1.26`), and same-direction scanner agreement added no separation (`t=0.22`).
Directional discovery-state alignment was useful for hourly events: aligned signals exceeded
opposed signals by 0.298% net alpha (`t=5.06`). The daily spread was negative and insignificant
(`t=-0.55`); weekly outcomes were not available for this test.

A follow-up on the same 70,739 events tested signal-bar volume acceleration using predeclared
decreasing (`<=0.80x`) and increasing (`>=1.25x`) buckets. Portfolio observations were separated
by at least five daily or seven hourly signal timestamps to avoid overlapping outcome horizons.
Daily increasing-minus-decreasing spreads were insignificant against both the prior bar
(+0.256%, `t=1.19`) and prior five-bar average (+0.127%, `t=0.53`). The immediate prior-hour
comparison was also insignificant and reversed sign across chronological halves, consistent with
intraday volume seasonality.

Same-clock hourly normalization behaved better but did not qualify. Against the prior session,
increasing volume exceeded decreasing volume by 0.100% (`t=0.89`) and neutral volume by 0.190%
(`t=1.74`). Against the prior five same-clock sessions, the spreads were +0.208% (`t=1.52`) and
-0.039% (`t=-0.39`). The hourly-long versus-neutral result reached `t=2.02` only in aggregate; its
early and late halves were +0.106% (`t=0.74`) and +0.361% (`t=2.07`). A post-hoc hourly
breakout-long slice showed possible exhaustion: increasing volume trailed decreasing volume by
0.738% (`t=-2.86`), with same-sign early and late spreads but only the late half significant. This
interaction remains a monitoring hypothesis because of multiple testing and limited paired periods.

The portal therefore assigns hourly signals only:

- `HIGHER`: long with continuation, emerging reversal, or confirmed reversal; short with conflict
  or laggard state.
- `LOWER`: the inverse directional combinations.
- `STANDARD`: neutral, missing, or inconclusive discovery context.
- `UNRANKED`: every daily and weekly signal.

Volume and range remain descriptive trigger/participation evidence and may break ties in future
validated models, but neither ordinary relative volume nor volume acceleration currently changes
review priority. Historical primary-pass status remains a separate
scanner/version/interval/direction/horizon qualification result.

## Current-Position Overlay

`extension-0.1-shadow` describes the latest complete daily close independently of signal-time
scanner evidence. It does not alter event direction, qualification, calibrated probability or
hourly Review Priority. A latest scanner row may therefore retain its historical evidence while
showing a separate current extension warning for the ticker.

The overlay reports three independent axes:

- `trend_state`: `UPTREND`, `DOWNTREND` or `NEUTRAL`, based on close/SMA20/SMA50 ordering.
- `extension_risk`: `NORMAL`, `EXTENDED` or `EXHAUSTION_WATCH`.
- `reversal_trigger`: `NONE`, `BEARISH_EARLY`, `BULLISH_EARLY`,
  `BEARISH_CONFIRMED` or `BULLISH_CONFIRMED`.

Extension requires a top/bottom-quintile 21-day move in the trend direction and at least one
predeclared vote from RSI, 1.5-ATR SMA20 distance or seven directional sessions in ten. An
`EXHAUSTION_WATCH` additionally requires deterioration from five-day return, close location or
swing structure. Confirmation requires crossing SMA20 and both swing-structure components in the
opposite direction. Consequently, the intended ideal cases are symmetric: an extended advance
without deterioration remains `EXTENDED / NONE`; weakening after an extended advance becomes
`EXHAUSTION_WATCH / BEARISH_EARLY`; weakening after an extended decline becomes
`EXHAUSTION_WATCH / BULLISH_EARLY`.

The values live in discovery evidence JSON under their own model version. Deployment generates
only the current snapshot; qualification replay and existing historical discovery rows are not
rerun or corrected for this descriptive feature.

## Trading Horizon

- **Swing:** daily candidate and structure; daily or hourly trigger; evaluate at 5/10/21 daily bars
  or 7/21/35 hourly bars.
- **Weekly:** completed Friday bars provide slower structure and triggers; execute at the next
  trading-session open and evaluate after 5/10/21 daily sessions.
- **Day:** daily discovery supplies context, hourly structure supplies direction, and 5m/15m should
  supply the trigger and same-session exit.

The event engine currently supports daily, weekly, and hourly outcomes. Hourly events can evaluate
short swing timing, but true day-trading claims require a future 5m/15m adapter, same-session
horizons, session-close exits and no overnight carry.

## Evaluation Window

An evaluator `--start` date is the first signal/outcome date, not the first required price bar.
Daily evaluation loads 400 earlier calendar days as indicator warm-up; hourly loads 90. Therefore,
`--start 2023-01-01` evaluates from 2023 while using available 2022 daily bars for SMA, ATR and
pivot state. The scanners do not intrinsically require January 2023; use the longest clean period
available and compare early/late subperiods.

Qualification replay reconstructs discovery states for each date and uses symbols with sufficient
stored bars on that date. This removes current-cohort selection from the replay. It cannot recover
delisted or formerly eligible symbols that were never loaded into this database, so residual
survivorship risk remains.

## Dynamic Fibonacci Swings

Composite level-retest 1.2 does not use one static percentage across tickers. It:

1. Confirms local pivots with two bars on each side.
2. Keeps alternating high/low pivots point-in-time.
3. Accepts a reversal only when the move is at least 3 ATR, using ATR known at confirmation.
4. Replaces an unconfirmed same-direction pivot with a more extreme pivot.
5. Derives 23.6%, 38.2%, 50%, 61.8% and 78.6% retracements from the latest accepted pair.

ATR normalization lets a lower-volatility ticker qualify with a smaller percentage move while a
volatile ticker must move farther. The 3-ATR threshold is shared and precommitted rather than tuned
per ticker, reducing overfitting. Swing size in ATR is stored in event metadata for later subgroup
evaluation.

Extensions such as 127.2% and 161.8% are excluded from retest entries; they belong in a separately
evaluated breakout-target rule.

The standalone portal Fibonacci page still uses its user-selectable percentage ZigZag (5% default).
That descriptive scanner remains separate from composite level-retest 1.2 until its UI is migrated
and evaluated under the dynamic method.

## Current Scanner: Structured Trend Pullback 1.0

Bullish rule:

1. SMA20 crossed above SMA50 within 40 bars.
2. A point-in-time confirmed higher swing high exists.
3. Price pulls back 2–15 bars toward SMA20 while remaining above SMA50.
4. Hammer, bullish engulfing, or strong bullish close triggers.

Bearish is the exact inverse. Pivots require two bars on each side; they become available only after
the two right-side bars exist. Broken-swing retests are metadata in version 1.0, not an alternative
entry rule.

## Current Scanner: SMA200 Reclaim Rejection 1.0

This daily and weekly bearish shadow setup requires:

1. SMA200 is below its value 20 sessions earlier.
2. Price crossed from below to above SMA200 one to three sessions earlier.
3. The signal high is within the greater of 0.25 ATR or 1% of the highest high from 5–63 sessions
  earlier.
4. The signal candle is a bearish strong close with either average relative volume or 1.1 ATR
  range participation, and closes below the prior close.
5. The signal close is between 0.50 ATR below and 0.10 ATR above SMA200.

An above-SMA200 close is stored as `WATCH`; a below-SMA200 close is `CONFIRMED`. Both remain
research events and use next-bar-open execution for outcomes. The stop is 0.25 ATR above the
greater of the rejection high or resistance high; the standard target is 2R.

### Rejected extension: intraday SMA200 magnet approach

An August 2024–August 2026 point-in-time study tested whether price approaching the prior completed
daily SMA200 supplied an intraday trade toward the average. It covered 400 active tickers and used
next-hour-open execution, one-hour ATR stops, the SMA200 as target, and 4 bps round-trip cost.
Signals had to begin 0.15–1.00 daily ATR from SMA200, reduce that distance by at least 0.05 daily
ATR on a strong directional hourly candle, and have at least 0.8 relative volume.

- Below-SMA long: 5,077 events across 499 dates; net stop/target/session-close return was -0.056%
  per date (`t=-3.49`).
- Below-SMA long with a rising SMA200: 2,608 events across 484 dates; net return was -0.044%
  (`t=-2.10`). SMA200 was touched in 13.7% of events while the stop was first in 19.6%.
- Above-SMA short: 5,242 events across 498 dates; net return was -0.099% (`t=-6.19`).
  One-hour and session-close directional returns were also negative.

Approach momentum improved the below-SMA long relative to ordinary nearby bars, but absolute
returns remained negative after costs and risk exits. SMA200 can still be useful as structural
location or support/resistance; these results reject treating proximity itself as a symmetric
intraday magnet signal. No scanner was added.

The same precommitted approach rule was also tested on daily and weekly bars using next-session
open execution and 5/10/21-session horizons:

- Daily below-SMA/rising-SMA at 10 sessions initially showed +0.364% net risk-exit return
  (`t=2.30`, 84 independent periods). Under the production fixed-horizon benchmark method it had
  -0.046% net alpha (`t=-0.09`), with early/late alpha of -0.652% / +0.561%. Five- and 21-session
  results were also not stable.
- Weekly below-SMA/rising-SMA had positive raw results, but only eight independent periods at five
  sessions and four at ten sessions. Alpha t-statistics were at most 1.38, and the ten-session
  early/late alpha changed sign.

Daily proximity therefore adds no demonstrated timing alpha, while weekly proximity is
insufficiently sampled. Neither timeframe was added as a scanner. A confirmed reaction at SMA200
remains a different, falsifiable setup from approaching it.

## Shared Adapter Contract

Every scanner must emit the same fields:

```text
scanner_name, scanner_version, interval, ticker, signal_time,
direction, trigger_type, setup_anchor, entry_price, atr_at_signal,
reference_level, stop_price, target_price, discovery_state, metadata
```

Rules:

- `direction` is `1` for bull and `-1` for bear.
- `setup_anchor` must identify one lifecycle, not one repeated trigger bar.
- A semantic rule change requires a new `scanner_version`.
- Adapters use only data available at `signal_time`.
- Adapter `entry_price` is the signal/setup close shown by the portal. Outcome execution uses the
  next available bar's open (`next_bar_open_v2`), because close-based triggers are known only after
  the signal bar completes.
- Each scanner/version is evaluated independently before scanner agreement is tested.

## Capture and Deduplication

Normal EOD capture scans only the latest complete bar; it does not replay history.

| Interval | Universe | Lookback | Batch size |
|---|---|---:|---:|
| Daily | All active tickers | 420 calendar days | 10 |
| Weekly | All active tickers | 1,800 calendar days | 10 |
| Hourly | Discovery cohorts only | 60 calendar days | 10 |

Event identity:

```text
scanner + version + interval + ticker + direction + setup_anchor
```

First appearance inserts one `scanner_events` row. Repeated appearances preserve original entry,
advance `last_seen_at`, and increment `occurrence_count`.

`scanner_event_occurrences` stores one unique `(event_id, signal_time)` row for each live or
replayed appearance. This makes historical replay idempotent while preserving one lifecycle for
an anchored setup that remains visible across adjacent bars. The ledger retains the latest 252
occurrence dates plus the latest row for every ticker and interval. `scanner_events.occurrence_count`
remains a lifetime count, while old occurrence detail can be reconstructed during replay.

## Outcomes

`scanner_event_outcomes` stores one immutable row per event/horizon:

| Interval | Horizons |
|---|---|
| Daily | 5, 10, 21 bars |
| Weekly | 5, 10, 21 trading sessions |
| Hourly | 7, 21, 35 bars |

Only due, missing outcomes are evaluated. Each row records:

- Actionable entry time, entry price and execution-model version
- Direction-adjusted and net return
- Equal-weight active-universe benchmark and net alpha
- MAE/MFE in percent and initial-risk units
- Stop/target hits and first-hit ordering
- 4 bps round-trip cost

`SAME_BAR` means OHLC data cannot determine whether stop or target traded first.

## Evaluation and Promotion

Events sharing a timestamp are equal-weighted into one portfolio observation. Observations are
sampled at least one horizon apart before computing alpha t-stat.

Review dimensions:

```text
scanner/version × interval × discovery state × direction × horizon
```

Minimum promotion gate:

- At least 100 events and 40 independent periods
- Positive net alpha and alpha t-stat > 2
- Positive early and late chronological halves
- Acceptable MAE/MFE and stop-first rate
- Benefit is not concentrated in one sector or regime
- Timing overlays must improve risk-adjusted entry versus immediate candidate entry

Promotion never happens automatically; update the registry and model version after review.

## Scheduler

After successful EOD validation:

```text
Price ingestion → ranking → discovery → evaluate due outcomes → capture daily/hourly events
```

Friday EOD runs also capture `1wk` events from completed Monday–Friday OHLCV aggregates. Weekly
signals remain separate evidence and do not inherit daily or hourly qualification.

A scanner-event failure is isolated and cannot change prices, rankings, discovery states, or
recommendations.

Manual controls:

```powershell
# Evaluate due outcomes, then capture both intervals
.\backend\.venv\Scripts\python.exe -X utf8 -u `
  .\backend\scripts\run_scanner_event_pipeline.py

# Capture or evaluate only
.\backend\.venv\Scripts\python.exe -X utf8 -u `
    .\backend\scripts\run_scanner_event_pipeline.py --capture-only
.\backend\.venv\Scripts\python.exe -X utf8 -u `
    .\backend\scripts\run_scanner_event_pipeline.py --evaluate-only

# Replay the latest 25 complete sessions, then evaluate every matured horizon
.\backend\.venv\Scripts\python.exe -X utf8 -u `
  .\backend\scripts\run_scanner_event_pipeline.py --backfill-sessions 25

# Point-in-time qualification replay; use ticker chunks for long windows
.\backend\.venv\Scripts\python.exe -X utf8 -u `
  .\backend\scripts\run_scanner_event_pipeline.py `
  --qualification-start 2023-03-09 --interval 1d `
  --ticker-offset 0 --ticker-limit 50 --skip-evaluation

# Aggregate qualification report after all chunks and outcomes complete
.\backend\.venv\Scripts\python.exe -X utf8 -u `
  .\backend\scripts\report_scanner_qualification.py
```

## Portal

- **Scanner Evaluation page:** aggregate 48-combination qualification matrix, filters, primary
  gate status, methodology boundary, hourly review priority and recent shadow setups. This replaces
  the unused synthetic Backtest page.
- **Dashboard / Scanner Evidence:** status, sample size, net alpha, t-stat, MAE/MFE, backlog and
  recent events, plus a link to the full evaluation page.
- **Ticker / Scanner Evidence:** lifecycle, first/last seen, occurrence count, entry/stop/target and
  matured outcomes. Setup close is shown separately from next-bar-open outcome execution.
- **Scanner Agreement:** current descriptive votes only; not event-backed recommendations.

## Adding a Strategy

1. Assign each proposed indicator one role: gate, structure, location, trigger, participation or risk.
2. Define one composite hypothesis with direction, trigger, setup anchor, stop and target.
3. Add the composite setup to the registry with status `PLANNED`.
4. Implement an adapter returning the shared contract.
5. Register the adapter in `capture_events()` without creating new outcome tables.
6. Store optional indicators as metadata or named variants to measure incremental value.
7. Capture in shadow mode and verify reruns refresh rather than duplicate.
8. Wait for due horizons; compare the base setup with each optional confirmation.
9. Mark `FAILED`, `PROMISING`, or `VALIDATED`; bump version for any rule change.

## Current State

- All seven composite families are connected to the shared capture/outcome pipeline.
- Weekly capture scanned all 400 active tickers on 2026-08-14 and persisted 43 shadow setups.
  Weekly outcomes and qualification are still collecting.
- SMA200 reclaim rejection 1.0 first captured ZS as the only 2026-08-14 match in the 400-name
  active universe. It remains unqualified and shadow-only.
- Daily capture scans all 400 active tickers. Hourly capture scans reconstructed point-in-time
  discovery cohorts, not today's cohort projected backward.
- Full-history replay, enhanced metadata refresh, occurrence repair, outcome evaluation, context
  reconstruction, FDR qualification and calibration are complete for the daily/hourly study.
- Of 63 primary scanner/direction/horizon rows, one is `ROBUST_PASS`, one is `MONITOR_ONLY`, and 61
  are `UNRANKED`. Of 978 non-baseline rows, six passed raw filter gates and none survived FDR.
- The `extension-0.1-shadow` current-position overlay is available on discovery, ticker detail and
  latest scanner rows without changing historical scanner evidence or qualification.
- All setups remain shadow research. Neither evidence status nor calibrated research output changes
  recommendations or approves a trade.
- Daily/hourly events are not yet explicitly paired as parent/child timing decisions.
- Monthly scanning is deferred. Stored history begins in March 2022: the median active ticker has
  232 weekly bars but only 54 monthly bars. No ticker has the 220 monthly bars needed for SMA200
  plus slope warm-up, and short monthly history produced overly broad retest matches in the probe.

## Full-History Qualification Result

Point-in-time replay completed with zero outstanding outcomes:

| Interval | Window | Sessions | Universe |
|---|---|---:|---|
| Daily | 2023-03-09 to 2026-08-12 | 860 | 400 data-available names |
| Hourly | 2024-08-12 to 2026-08-12 | 502 | 391-name union of reconstructed discovery cohorts |

Of 63 scanner/direction/horizon combinations, 61 failed the raw primary gate. Daily-short
failed-breakout reversal at ten sessions passed raw gates but did not survive FDR (`q=0.494`). The
only robust primary pass was `breakout_expansion` 1.0, hourly long, at seven bars:

- 3,263 events and 459 horizon-spaced periods
- Mean net alpha: +0.290%
- Alpha t-stat: 3.68
- Early/late alpha: +0.145% / +0.434%
- Hit rate: 56.9%
- Benjamini-Hochberg adjusted `q`: 0.0147
- Walk-forward calibrated P(win): 56.576%; live expected alpha: +0.290%
- Largest ticker share: 1.3%; largest sector share: 28.0%
- Positive alpha in 10 of 12 sector groups

The regime check is conditional: alpha was positive in SPY bull (+0.210%) and mixed (+0.336%)
regimes, but negative in bear regimes (-0.047%). No regime filter survived FDR. Therefore this
candidate is a robust research result, not universally `VALIDATED`. Keep it shadow-only and require
an untouched forward period before it can influence recommendations.
