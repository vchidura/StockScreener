# September 14 Stock Alert Review

## Verdict

The user's concern is supported: these are mechanically triggered intraday
setups, not validated high-quality trade recommendations. SPGI and RMD's large
reward/risk values are mathematically correct **at their old trigger prices**,
but are magnified by tight stops and distant mechanical range targets. They do
not describe the probability of reaching those targets or the ratios at entry.

The review also found actionable publication defects: three plans already failed
their entry gate at a newer completed price known before publication, and two
late-session plans had no remaining entry slot under the declared execution policy.
No alert, outcome, policy, provider data or application runtime was changed.

## Scope And Evidence

- All **42 selected trade plans / 40 tickers**, September 14, 2026. Eighteen 30m
  plans and 24 hourly plans; 26 long and 16 short. No daily trade plans were
  published. The closing window was missed, not a negative daily-model result.
- Twelve retained publication records, including empty, retry and missed records.
  Forward enrollment began at 10:50:39 ET, so this is not a full-day-from-open study.
- Retained source-input cutoff: 21:49:57.714664 UTC, after the close. The app remains
  paused. The review used read-only SQLite and the original checkpoint bars, not
  a new provider download, a backdated worker run, or a live API refresh.
- [Detailed final evidence](stock_alert_session_review_2026-09-14_final.json)
  contains every plan, reconstructed geometry, source revision references,
  known-price checks, and SPGI/RMD candle details. An initial narrower diagnostic
  is preserved in [stock_alert_session_review_2026-09-14.json](stock_alert_session_review_2026-09-14.json).
- [Audit utility](../backend/scripts/audit_stock_alert_session.py) reads the ledger
  without using its writer class. Checkpoint/input hashes were unchanged before
  and after both runs. Current model/engine source hashes match those retained in
  the selected publications. No duplicate selected episode IDs were found.
- Scope of validation: model geometry/indicator preconditions, trigger price,
  timing/entry gates and deterministic paper outcome reconstruction. This is not
  an independent vendor-price/corporate-action audit, full incremental lifecycle
  parity certification, spread/borrow validation, or evidence of predictive edge.

## Findings

### 1. Publication Can Admit Already-Ineligible Plans

**High priority implementation defect.** The selection gate checks
`candidate.price`, while forward dispatch can retain an hourly candidate across
the intervening 30m publication. The freshness gate checks member readiness but
does not reprice the pending plan using the newer completed native bar. The
[forward candidate path](../backend/research/stock_idea_forward.py) passes those
records to [select_candidates](../backend/research/stock_idea_engine.py), which
calls `entry_gate(candidate, candidate.price)`.

The existing design explicitly requires current entry gates to remain valid for
a later selection. These failures use prices that were already observed/created
before the original input cutoff, not hindsight from a future fill:

| Plan | Trigger Price | Newer Known Price | Known-Price Failure | Retained Outcome |
|---|---:|---:|---|---|
| HWM short, published 12:22:10 | 226.665 | 228.110 at 12:00 | Back above breakdown boundary 227.57 | No fill: boundary/chase |
| LQD long, published 12:22:10 | 104.410 | 104.545 at 12:00 | Remaining reward/risk 0.73, below 1 | No fill: target room |
| VEA long, published 13:21:22 | 71.985 | 72.120 at 13:00 | Remaining reward/risk 0.62, below 1 | No fill: target room |

All 42 pass the gate evaluated at their retained trigger prices. That does not
negate these three failures at the more recent publication-time information set.
HWM even has a geometrically large ratio near its stop at the newer price; the
broken breakdown boundary still makes it ineligible. Ratio alone is insufficient.

### 2. Two Published Alerts Could Never Enter That Session

**High priority publication/execution mismatch.** RCL short and MA short were
published at 15:58:46 ET. Their triggers had not expired, but
[execution_times](../backend/research/stock_idea_replay.py) requires the first 30m
bar open strictly after publication. There is none before the 16:00 session close.
Both correctly became `NO_FILL / SESSION_CLOSED_BEFORE_ENTRY` in paper accounting.
That fact was deterministic at publication and should be a pre-publication
actionability gate, not only an eventual no-fill result. Real sub-minute trading
possibilities are outside this explicitly retained 30m execution model.

### 3. Acceptance Allows Weak Confirmation And Rewards Small Stop Distance

**Model-design weakness, not a failed implemented rule.** In
[intraday_observations](../backend/research/stock_idea_models.py), acceptance needs
a qualifying breakout followed by the next completed close merely remaining on
the correct side of the frozen boundary. The confirmation need not be a rising
close, favorable candle body, strong closing location, high relative volume, or
aligned daily/hourly trend. Six of today's nine acceptance confirmations moved
against the proposed direction relative to the previous bar's close.

The measured-move target is one full prior ten-bar range beyond the breakout
boundary. The stop instead uses the breakout/confirmation episode extreme plus
0.1 trigger ATR. A retracement toward the stop can therefore *increase* reward/risk
and *decrease* chase, even though confirmation looks less convincing. Acceptance
priority sorts smaller extension first, then larger target-room/risk. This can
prefer precisely the profiles the user finds unappealing.

There is no minimum stop-distance/ATR gate, target feasibility gate for the
120-minute hold, independent obstacle/resistance gate, or calibrated target-hit
probability in this model. SPGI and RMD are the only two of today's alerts above
4x trigger-price reward/risk, and the only two with stop distance below 0.5
activation ATR. That diagnostic threshold is descriptive, not a newly approved rule.

### 4. Alert Timing And Fill Attrition Limit Actionability

Median trigger-to-publication age was 26.66 minutes; eight plans were more than
30 minutes old when first selected. SPGI's 11:30 trigger was selected at 12:22:10,
52.18 minutes later, still inside its original 12:30 expiry. This is permitted
by the current lifetime contract but should not look like a just-triggered quote.

Twenty of 42 plans (47.6%) never filled under the existing policy: fourteen
insufficient-target-room, three boundary/chase failures, one outside-bracket, and
the two no-remaining-entry cases above. These are zero-exposure scenarios, not
twenty losing trades. The next-open filter correctly prevented those paper entries.

## SPGI And RMD

Both were **hourly long breakout-acceptance** alerts based on the 10:30-11:30 ET
confirmation bar. The ten-bar range included September 10-11; it was not a single
small candle's range, but the final acceptance rule was a one-bar condition.

| Quantity | SPGI | RMD |
|---|---:|---:|
| Prior range low / high | 407.44 / 416.38 | 215.97 / 221.29 |
| Range width | 8.94 | 5.32 |
| Trigger/reference price | 417.500 | 221.355 |
| Original stop | 416.2607 | 220.7376 |
| Original target | 425.320 | 226.610 |
| Risk per share at trigger | 1.2393 (0.297%) | 0.6174 (0.279%) |
| Target room at trigger | 7.8200 (1.873%) | 5.2550 (2.374%) |
| Displayed reward/risk | **6.31x** | **8.51x** |
| Stop / activation ATR | 0.412x | 0.386x |
| Target room / activation ATR | 2.602x | 3.287x |
| Simulated entry time / price | 12:30 ET / 418.6700 | 12:00 ET / 222.6106 |
| Reward/risk at simulated entry | **2.76x** | **2.14x** |
| Paper exit | Stop, 14:00 bar boundary | Time cap, 14:00 |
| Net paper return, 10 bps round trip | **-0.6755%** | **+0.1154%** |

For long plans, `reward/risk = (target - price) / (price - stop)`.
SPGI is `7.82 / 1.2393 = 6.31`; RMD is `5.255 / 0.6174 = 8.51`.
No success probability is involved in that calculation.

### SPGI: Bounce Above A Range, Not Broad Trend Confirmation

- Activation 09:30-10:30: open417.00, high423.32, low416.63, close418.31.
  The pre-break range contraction ratio was 0.7287, below the 0.75 threshold,
  and the close cleared the 416.38 boundary by more than 0.15 activation ATR.
- Confirmation 10:30-11:30: open418.545, high420.00, low417.50, close417.50.
  This is a down candle closing at its low, also 0.194% below the preceding close.
  It still passes because 417.50 remains above416.38. The confirmation's own
  contraction ratio had risen to1.2689; the rule only tests contraction before
  the original breakout. Relative volume was0.624x the preceding20-bar mean
  (not time-of-day normalized and not a required gate).
- Stop416.2607 = episode low416.63 minus0.3693 (0.1 trigger ATR).
  Target425.32 = range high416.38 plus range width8.94. The target is above the
  morning's423.32 high and the trigger-hour EMA50 of423.52; these potential
  intervening obstacles do not constrain the model's target.
- Prior completed daily close410.69 was below daily EMA50 426.25, RS63 versus SPY
  was-4.27 percentage points, and retained12-1 momentum was-24.08%. It would fail
  the resumption model's trend contract; acceptance does not require it.
- At publication, the already-visible12:00 close was418.04: remaining ratio4.09x,
  not6.31x. At the12:30 simulated entry418.67 it was2.76x. The paper stop was hit;
  a later favorable trigger-to-session-close change of+0.316% does not undo that
  stop or establish a successful trade. Bar timestamps are not tick-level hit times.

### RMD: Confirmation Nearly On The Boundary

- Activation09:30-10:30: open221.04, high225.82, low220.94, close223.13.
  Pre-break contraction0.6921 and range width3.328 activation ATR satisfy the rule.
- Confirmation10:30-11:30: open222.925, high223.58, low221.30, close221.355.
  The close is only **0.065 above221.29**, a0.0294% margin, and only0.055 above
  the candle low. It fell0.796% from the preceding close. Confirmation relative
  volume was0.606x; hourly EMA50 was223.32, above the trigger price.
- Stop220.7376 = episode low220.94 minus0.2024 (0.1 trigger ATR).
  Target226.61 = range high221.29 plus5.32. That combination, not exceptional
  strength, creates8.51x. Retesting close to the stop compresses the denominator.
- Prior daily close218.26 was below EMA50 219.66, despite positive RS63/76.39th
  percentile. Retained12-1 momentum was-16.40%. This too is not a fully aligned
  resumption setup.
- The later valid entry222.6106 roughly tripled dollar stop risk from0.6174 to
  1.8730 and reduced target room to3.9994: ratio2.14x. It exited at223.09 at14:00
  for+0.1154% net, not at226.61. The target was never reached within the plan.

## All-Alert Outcome Summary

Every selected plan's trigger price matches its completed trigger bar; all
reconstructed geometry checks passed, including all15 required resumption
daily-trend checks. All recorded selected-candidate availability/expiry/dispatch
checks passed. This is narrower than declaring every alert valid at a fresh price.

All42 retained paper states and relevant entry/exit/reason/cost fields agree with
a read-only recomputation from the same retained bars. Of22 entered trades,
10 had positive net returns and12 negative. There were18 time/session exits,
four stops and **zero target exits**. Mean entered-trade net return was-0.0259%;
median-0.0915%. Those are descriptive per-plan averages, not a capital-weighted
portfolio return, independent observations, or reliable win-probability estimates.

| Model | Alerts | No Fill | Entered | Positive Net | Stops | Targets | Mean Net, Entered |
|---|---:|---:|---:|---:|---:|---:|---:|
| Resumption | 15 | 8 | 7 | 5 | 1 | 0 | +0.303% |
| Acceptance | 9 | 4 | 5 | 2 | 1 | 0 | -0.078% |
| Reversal | 18 | 8 | 10 | 3 | 2 | 0 | -0.230% |

Resumption is the least concerning of today's three slices because it requires
daily alignment, but seven fills on one day cannot qualify it. Acceptance and
reversal are deliberately capable of short-horizon countertrend moves; their
names and ratios should not imply a strong daily trend or a validated edge.

### Per-Alert Ledger

R/R is the original trigger-price ratio. Net returns are paper results after
10 bps round-trip cost. No-fill rows had no executed exposure. Times are ET.
The same ticker can appear twice for distinct episodes, intervals or directions.

| Ticker | Side | Model | Interval | Published | R/R | Outcome | Net % |
|---|---|---|---|---|---:|---|---:|
| ABBV | Long | Resumption | 30m | 11:55 | 1.02 | No fill: room | N/A |
| PLD | Short | Acceptance | 1h | 11:55 | 1.23 | No fill: boundary/chase | N/A |
| RMD | Long | Acceptance | 1h | 11:55 | 8.51 | Time exit | +0.115 |
| USO | Long | Acceptance | 1h | 11:55 | 2.02 | No fill: boundary/chase | N/A |
| HON | Long | Reversal | 30m | 11:55 | 2.45 | No fill: room | N/A |
| FCX | Long | Reversal | 1h | 11:55 | 1.80 | Time exit | +0.982 |
| BND | Long | Reversal | 1h | 11:55 | 1.49 | No fill: room | N/A |
| HWM | Short | Acceptance | 1h | 12:22 | 2.27 | No fill: boundary/chase | N/A |
| RIVN | Short | Acceptance | 1h | 12:22 | 2.04 | Time exit | +0.494 |
| SPGI | Long | Acceptance | 1h | 12:22 | 6.31 | Stop | -0.675 |
| TEL | Long | Reversal | 30m | 12:22 | 3.17 | Stop | -0.378 |
| CSX | Long | Reversal | 30m | 12:22 | 1.39 | No fill: room | N/A |
| LQD | Long | Reversal | 1h | 12:22 | 1.35 | No fill: room | N/A |
| U | Long | Resumption | 1h | 12:50 | 1.07 | No fill: room | N/A |
| VEEV | Long | Resumption | 1h | 12:50 | 1.83 | Time exit | +0.645 |
| TMO | Long | Resumption | 1h | 12:50 | 1.08 | No fill: room | N/A |
| USO | Short | Reversal | 1h | 12:50 | 2.39 | No fill: room | N/A |
| SMR | Long | Reversal | 1h | 12:50 | 2.04 | Time exit | -1.015 |
| NU | Long | Reversal | 1h | 12:50 | 1.69 | Time exit | -1.099 |
| CTAS | Long | Resumption | 1h | 13:21 | 1.10 | Time exit | +0.025 |
| WFC | Long | Reversal | 30m | 13:21 | 1.55 | No fill: bracket | N/A |
| VEA | Long | Reversal | 1h | 13:21 | 1.01 | No fill: room | N/A |
| AMAT | Short | Resumption | 30m | 13:57 | 1.26 | Time exit | +0.574 |
| ALNY | Short | Resumption | 30m | 13:57 | 2.20 | Stop | -0.934 |
| SLV | Long | Reversal | 1h | 13:57 | 2.08 | Time exit | -0.161 |
| RKT | Short | Reversal | 30m | 13:57 | 1.77 | Time exit | +0.997 |
| WPM | Long | Reversal | 1h | 13:57 | 1.13 | Time exit | -0.273 |
| ALL | Long | Resumption | 30m | 14:29 | 1.28 | No fill: room | N/A |
| NRG | Short | Resumption | 30m | 14:29 | 1.37 | No fill: room | N/A |
| MDT | Long | Resumption | 30m | 14:29 | 1.09 | No fill: room | N/A |
| BMNR | Long | Acceptance | 30m | 14:29 | 1.61 | Time exit | -0.022 |
| HYG | Long | Reversal | 1h | 14:29 | 1.05 | No fill: room | N/A |
| RKLB | Short | Resumption | 1h | 14:56 | 1.04 | No fill: room | N/A |
| CRDO | Short | Resumption | 30m | 14:56 | 1.85 | Time exit | +1.880 |
| AKAM | Short | Resumption | 1h | 14:56 | 1.97 | Time exit | +0.624 |
| KWEB | Short | Reversal | 30m | 14:56 | 2.59 | Time exit | +0.262 |
| SOFI | Short | Reversal | 1h | 14:56 | 1.18 | Time exit | -0.670 |
| ALNY | Short | Resumption | 1h | 15:26 | 1.73 | Time exit | -0.694 |
| IBIT | Long | Acceptance | 30m | 15:26 | 3.78 | Time exit | -0.301 |
| PCG | Long | Reversal | 30m | 15:26 | 2.01 | Stop | -0.947 |
| RCL | Short | Resumption | 30m | 15:58 | 1.18 | No fill: session ended | N/A |
| MA | Short | Acceptance | 30m | 15:58 | 1.42 | No fill: session ended | N/A |

## Recommended Next Changes, Not Applied

1. Fix publication-time actionability first: gate existing candidates on the latest
   completed, deadline-visible native price, retaining a separate trigger price
   and decision-price timestamp. Do not replace the immutable structural plan.
   Suppress before quota selection when no same-session execution slot remains.
2. Display original/decision/entry reward-risk separately, plus stop distance in ATR,
   target distance, age and time remaining. Do not cap the visible ratio merely to
   hide the issue or equate a large ratio with confidence.
3. Trial one frozen acceptance-quality challenger: require meaningful boundary
   retention/closing location and evaluate stop robustness and intervening obstacles
   against the holding horizon. Daily trend alignment can be a separately labeled
   context or challenger, not a silent transformation of the existing range model.
4. Compare on a bounded, chronological, multi-session dataset using the same
   cohort, availability, execution and missing-data contracts. Include no-fill rate,
   actual-entry geometry, target/stop/time-exit frequencies and calibration. Do not
   tune thresholds to make only SPGI/RMD disappear, rewrite today's records, or
   claim one-day outcomes establish model success or failure.

The application remains paused. This review authorizes no repairs, new backtests,
provider requests, policy activation, alert replacement or service restart.