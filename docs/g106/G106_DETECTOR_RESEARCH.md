# G106 detector research: Options rules vs market condition (Nov 17 2026)

Research session, Nov 17 06:24-06:50 UTC. Read-only: no detector code, no live setting, no deploy. Question from the
user (Nov 17 ~06:30 UTC): correct the Options detection rules (O1, OS1, OS2, and how O1/O3 packages cover sideways
markets) so that alerts follow the market condition: bullish trend, bearish trend, reversal at support/resistance,
sideways.

**Sample warning.** 5 sessions (Nov 10 partial, 11, 12, 13 with 8 Options windows, 16), 9 names, one clean down-trend
day (Nov 16). The structural findings (sections 2, 3.1) hold on every session. The outcome findings (forward moves,
option returns) are small samples. Read them as direction, not as calibration.

Scripts, exports and outputs are in `backend/.realtime-canary/v3-supervised-20261102/recovery-20261104/evidence/detector-research-20261117/`
(gitignored). Reproduce: section 9.

## 0. Summary

1. **The call bias is structural, and it is symmetric.** For each strike, the type that was in the money at the
   prior close carries 0.4-0.75x the open interest per contract of the other type, on all 5 sessions. After a move,
   that type's strikes sit near the new spot, so their vol/OI is the high one.
   - Nov 16 down day, strikes within 2% of spot (DTE 0-45, 9 names): calls had OI 1.01M and vol/OI 8.3; puts had OI
     3.54M and vol/OI 3.5. Without a floor, 464 calls vs 276 puts reach vol/OI >= 3.
   - Nov 11 (gap up) is the mirror: 249 calls vs 417 puts reach it.
   - Near-spot 0DTE vol/OI is 13-78x on every session, for both types. The ratio says nothing on 0DTE.
2. **O1 takes three biases on top of that.**
   - (a) **The watched set is call-heavy.** Calls are 61-65% of watched contracts and 72% of the stored prints with
     DTE >= 1. At the Nov 16 close, 57 watched calls vs 6 watched puts passed ratio >= 3 with the floor. On the full
     near-spot chain the counts were 209 calls vs 227 puts.
   - (b) **The direction is the contract type.** O1's call events were net SOLD on Nov 10, 12 and 16:
     - ask-side premium $3.9M vs bid-side $6.9M on Nov 10;
     - $2.4M vs $3.2M on Nov 12;
     - $2.7M vs $3.4M on Nov 16.
   - (c) **More than half the alerts are 0DTE, and they lose.** The replay of today's rule gives 94 contract alerts;
     49 are 0DTE. Those 49 lost -54% mean / -90% median to the close (win 0.17), and lost on 4 of 5 sessions. The 35
     alerts with DTE >= 1 gained +8% mean / +4% median (win 0.64).
   - Overall, 70% of today's alerts point against the day's intraday move (ex post), and half point against the move
     so far at the trigger.
3. **Aggressor side is the usable direction signal.** Its coverage is thin: 31% of stored premium is ask-side or
   bid-side at HIGH/MEDIUM confidence. Of the premium, 27% is inside the spread, 27% is complex (multi-leg) and 10%
   has a stale quote (by prints, 21% are stale on Nov 16).
   - **Day level:** net bought premium (calls minus puts) had the sign of the intraday move on 19 of 28 name-days with
     >= $250k classified premium. On Nov 16 it was bearish on SPY, QQQ, NVDA, TSLA, IWM and AAPL.
   - **Forward:** a 30-minute imbalance predicts the next 30-60 minutes only weakly (hit 0.52-0.56), and it fades into
     the close.
4. **Stock-first triggers confirmed by aggressor flow are the strongest pattern here.** It holds in 4 of 5 sessions,
   with n of 41-94 per group.
   - **Level breaks** (prior-day high/low, opening range, OI walls) confirmed by same-direction flow in the 20 minutes
     before: +60 min hit 0.63 (n=62), vs 0.40 with no classified flow.
   - **Level rejections** confirmed the same way: 0.61 (n=41) vs 0.41 (n=87).
   - **OS1/OS2 signals** confirmed by flow: 0.63 (n=94). Those over $100k: 0.69 (n=26). Today's O1-ratio confirmation:
     0.59 (n=17).
   - All of these fade into the close.
5. **Sideways has no detector and no neutral structure today.**
   - O3 is a trend-following credit vertical: it is gated on the 30m EMA50 slope sign and needs an activity leg with
     ratio >= 3. It produced 0 evaluations on Nov 10-16 (BLOCKED Nov 10-13, COMPLETE_ZERO Nov 16).
   - AL2 debit verticals are directional.
   - Neither intraday efficiency (no persistence) nor the prior-close dealer gamma sign (which tracks the name, not
     the day) separated range days on this sample.
6. **Recommendations** (section 7), each a switch-guarded policy with RECORD first:
   - **P1:**
     - O1 0DTE gate;
     - O1 aggressor-side direction, plus better side coverage;
     - a symmetric watched set (slot policy);
     - OS v2 with aggressor confirmation (being built).
   - **P2:**
     - an O1 baseline rule (volume vs its own bucket baseline, which needs H1);
     - a reversal lane (level rejection + flow, or the Stock Alerts F2 trigger).
   - **P3:** a regime layer and neutral structures (needs 20+ sessions).
   - **Drop:**
     - contract vol/OI as a direction source (keep it as unusual activity);
     - OI normalised per strike;
     - "failed OS" as a reversal signal.

## 1. Data and method

| Source | What | Rows |
|---|---|---|
| `rt.option_flow_events` (new export `data/flow.csv`) | stored prints Nov 10-16, ADVANCED, with side_class / confidence / quote / cumulative volume / prior OI / spread position | 17,062 (INCLUDED rev 1) |
| `o1lf-20261117/data/eval.csv` | O1 evaluations (DETECTED etc.) joined to their print | 4,299 |
| `o1lf-20261117/data/alerts.csv`, `pkg.csv`, `marks.csv` | session alerts (71 O1, 2 OS1, 6 OS2), packages, 5-min cutoff marks | |
| `research/chains/<date>/close-retained.csv.gz` | final chain capture per session (Nov 9-16): OI, day volume, bid/ask, IV, greeks, spot | 17-19k per session |
| `os-nearmiss-20261117/data/` | 5m RTH bars, daily bars, watched set, OS evaluations, 340 rebuilt OS signals | |
| new `data/lanes_all.csv`, `data/evalcount.csv` | lane states and non-O1 evaluation counts per session | |

Stored prints are only the prints that passed a flow profile (`realtime/policies/advanced_options_flow_v1.json`:
size or premium thresholds by DTE lane and tier). They are only for watched contracts, and **3-4 DTE is never stored**
(the lanes are 0-2, 5-21 and 22-60). Aggressor statistics therefore describe large prints only.

**Day classes** (open-to-close in ATR14 >= 0.5 and >= 40% of the range = UP/DOWN, else RANGE; ATR14 = simple mean of 14
true ranges, RAW daily bars). Each cell is close vs prior close / gap at the open / open-to-close in ATR / efficiency:

| Session | SPY | QQQ | IWM | NVDA | Market reading |
|---|---|---|---|---|---|
| Nov 10 | -0.4% / -0.7% / +0.2 / 0.27 | -0.6% / -0.9% / +0.1 / 0.16 | -0.7% / -0.9% / +0.2 / 0.23 | -5.5% / -2.6% / -0.6 / 0.59 | flat after a small gap down; NVDA, MSFT down |
| Nov 11 | +1.3% / +0.9% / +0.2 / 0.39 | +2.3% / +1.8% / +0.2 / 0.26 | +0.9% / +0.9% / 0.0 / 0.03 | +3.0% / +2.4% / +0.1 / 0.16 | gap up, then range; AAPL, NFLX up; AMZN, MSFT down |
| Nov 12 | -0.3% / -0.8% / +0.3 / 0.34 | -0.3% / -1.5% / +0.5 / 0.53 | -1.6% / -0.6% / -0.6 / 0.72 | -4.5% / **-13.1%** / **+1.6** / 0.82 | gap down, recovery; NVDA reversal at support |
| Nov 13 | -0.4% / +0.4% / -0.5 / 0.39 | -1.2% / +0.1% / -0.5 / 0.51 | -0.3% / +0.2% / -0.3 / 0.31 | -1.9% / +2.3% / -0.7 / 0.61 | mild down (Options: 8 windows only) |
| Nov 16 | -1.9% / -0.6% / -0.7 / **0.82** | -2.7% / -1.3% / -0.5 / 0.79 | -1.6% / -0.5% / -0.6 / 0.68 | -4.1% / -2.3% / -0.3 / 0.35 | down trend, SPY closed at 17% of its range |

Across all 45 name-days: 4 UP, 11 DOWN, 30 RANGE.

**Replay unit.** One alert = the first trigger per (session, contract, direction).
- **Trigger time** = the print's `created_at`.
- **Entry** = the contract's first cutoff mark at or after the trigger, within 15 minutes. Without one, the entry is the
  print's quote mid. That fallback applies to most entries on Nov 10 and Nov 13, where cutoff marks are sparse.
- **Option outcome** = mid to mid:
  - at +30 and +60 minutes: the cutoff marks;
  - at the close: the final chain capture mid (~20:55 UTC).
  - No costs are included.
- **Underlying outcome** = the move signed by the alert direction, from the 5m bar close at the trigger to +30 min,
  +60 min and the close.
- **"With day"** = the alert direction equals the sign of the full-day open-to-close move. This is ex post and
  descriptive only.

The replay of today's rule (R0) reproduces all 67 stored O1 alert contracts. It adds 27 contracts that had a DETECTED
event but no stored package or alert.

## 2. Q1: the structural bias, verified

### 2.1 Near-spot chain (strike within 2% of the capture spot, DTE 0-45, 9 names, final capture)

| Session | C OI | P OI | C vol/OI | P vol/OI | C ratio >= 3 | P ratio >= 3 | C with floor | P with floor |
|---|---|---|---|---|---|---|---|---|
| Nov 10 | 2.00M | 2.36M | 4.89 | 4.23 | 289 | 242 | 210 | 173 |
| Nov 11 (gap up) | 2.92M | 2.08M | 4.37 | **4.86** | 249 | **417** | 163 | 194 |
| Nov 12 | 2.49M | 2.88M | 4.11 | 3.57 | 243 | 293 | 184 | 202 |
| Nov 13 | 2.16M | 3.18M | 5.19 | 3.39 | 311 | 219 | 226 | 168 |
| Nov 16 (down) | **1.01M** | 3.54M | **8.26** | 3.46 | **464** | 276 | 209 | 227 |

Floor = the O1 liquidity floor (AL4: OI >= 100, day volume >= 500).
- **0DTE alone:** near-spot vol/OI is 18.7-77.9 for calls and 12.7-65.2 for puts on every session. The ratio is always
  met.
- **|delta| 0.3-0.7 band, Nov 16:** call OI 1.75M vs put OI 4.33M; call vol/OI 1.20 vs put 0.75; 368 vs 179 contracts
  at ratio >= 3.

### 2.2 Why: moneyness at the prior close (near-spot strikes)

Each strike has one type that was in the money at the prior close. That type has less OI per contract, so its vol/OI is
the high one:

| Session | Formerly-ITM side: contracts, OI/contract, vol/OI, ratio >= 3 (with floor) | Other side: OI/contract, vol/OI, ratio >= 3 (with floor) |
|---|---|---|
| Nov 10 | calls 872, 1,044, 7.38, 193 (129) | puts 1,830, 6.02, 194 (146) |
| Nov 11 | **puts** 1,132, 1,426, 5.80, 367 (159) | calls 2,174, 5.01, 195 (149) |
| Nov 12 | calls 839, 1,387, 3.43, 118 (84) | puts 2,634, 3.89, 196 (146) |
| Nov 13 | calls 843, 1,255, 8.10, 225 (155) | puts 2,827, 4.22, 186 (148) |
| Nov 16 | calls 1,234, **746**, **8.92**, 456 (202) | puts **2,831**, 3.50, 275 (226) |

Two readings:
- **The effect is symmetric.** It favours the type opposite to the move: calls after a down move, puts after an up
  move (Nov 11).
- **On the full chain the AL4 floor removes most of the count imbalance** (Nov 16: 209 vs 227). The floor does not
  remove the ratio's level difference.

### 2.3 What O1 actually sees: watched set and detections

**Watched set** (final slot set per cohort) and the passes among watched contracts at the close (ratio >= 3 with the
floor):

| Session | Watched C / P | Watched C 0DTE / P 0DTE | Watched calls ITM at prior close | Passes C / P |
|---|---|---|---|---|
| Nov 10 | 47 / 25 | 7 / 3 | 35 of 47 | 24 / 9 |
| Nov 11 | 46 / 26 | 11 / 6 | 8 of 46 | 16 / 9 |
| Nov 12 | 46 / 26 | 7 / 3 | 26 of 46 | 21 / 8 |
| Nov 13 | 129 / 87 | 34 / 8 | 70 of 129 | 35 / 4 |
| Nov 16 | 131 / 85 | 21 / 7 | 108 of 131 | **57 / 6** |

Stored prints, DTE >= 1: 11,890 call vs 4,549 put. DTE 0: 527 vs 96.

**Detected O1 contracts** (unique flow events; the brief's "722 call events" counts evaluations, and one event can be
evaluated in two windows): 85 contracts in 5 sessions. 56 of them (66%) were in the money at the prior close, against
50% by construction near spot.
- Nov 16: 21 call contracts (19 formerly ITM; 13 0DTE; 368 events) vs 2 put contracts (NVDA 11/25 167.5P OI 42 and AAPL
  11/27 362.5P OI 27, both below the floor; 8 events).
- Nov 11, after the gap up: all 6 detected put contracts were formerly ITM.

**Conclusion Q1.**
- The hypothesis holds on the full chain, and it is symmetric.
- O1's 24 call / 2 put alerts on Nov 16 come mostly from three things layered on it:
  - the call-heavy watched set, in which the watched puts are the high-OI, formerly-OTM strikes;
  - 0DTE contracts;
  - two thin puts.
- The floor alone would have left 18 calls and 0 puts on Nov 16 (the R0F row in section 4).

## 3. Q2: direction of flow

### 3.1 Coverage of the aggressor side (17,062 stored prints, $307M premium)

| | Share of prints | Share of premium |
|---|---|---|
| Ask-side or bid-side, HIGH/MEDIUM confidence (directional) | 32.7% | 31.2% |
| INSIDE_SPREAD | 32.1% | 26.6% |
| EXCLUDED_COMPLEX (multi-leg) | 19.3% | 26.6% |
| QUOTE_STALE | 12.5% (Nov 13 15.0%, Nov 16 21.3%) | 10.3% |
| Ask-side or bid-side, LOW confidence (not counted) | 496 prints | |

- **Stale quotes:** median quote age at the print is 10 s, p90 36 s. On Nov 16 they concentrate on NVDA (218) and
  NFLX (168).
- **Inside-spread prints:** 75% sit at spread position 0.3-0.7. Counting the >= 0.7 and <= 0.3 ones as leaning would
  raise the directional share of premium from 31.2% to 38.8%.
- `opening_inference` is LIKELY_OPENING for 11.6% of prints.

### 3.2 Day level: net bought premium vs the day

The imbalance is (bought calls - bought puts) / classified premium, where bought = ask-side premium - bid-side premium.

On the 28 name-days with >= $250k classified premium, its sign matched the intraday move 19 times (68%); without 0DTE,
17 of 26 times.

| Session | Name: imbalance (intraday move, ATR) |
|---|---|
| Nov 16 | SPY -0.12 (-0.7), QQQ -0.52 (-0.5), NVDA -0.23 (-0.3), TSLA -0.37 (-0.4), IWM -0.15 (-0.6), AAPL -0.48 (-0.5). Opposite sign, small dollars: AMZN +0.41, MSFT +0.30, NFLX +0.26 |
| Nov 11 | QQQ +0.55, NVDA +0.14, AAPL +0.18, NFLX +0.19. AMZN -1.00, MSFT -0.43 (both down days) |
| Nov 12 | NVDA +0.15 (reversal up), TSLA +0.12 (up), IWM -0.57 (down), AAPL -0.60 |

**O1's own detected events, ask-side vs bid-side premium** (the O1 direction = the contract type):

| Session | Calls (O1 says bullish) | Puts (O1 says bearish) |
|---|---|---|
| Nov 10 | $3.89M vs $6.93M (net sold) | $0.42M vs $0.55M |
| Nov 11 | $1.12M vs $0.53M (net bought) | $1.03M vs $0.63M |
| Nov 12 | $2.40M vs $3.15M (net sold) | $0.43M vs $1.74M (net sold, bullish) |
| Nov 13 | $0.21M vs $0.16M | none |
| Nov 16 | $2.74M vs $3.36M (net sold, bearish) | none classified |

On aggressor evidence, the calls behind O1's Nov 16 bullish alerts were mostly sold.

### 3.3 Forward

Rolling 30-minute look-back at every 5m bar end, 9 names x 5 sessions, 3,240 samples. Each cell is mean signed move % /
hit rate:

| Signal | n | +30 min | +60 min | to close | Per session bull/bear |
|---|---|---|---|---|---|
| Flow imbalance >= 0.3, >= $250k | 237 | +0.05 / 0.52 | +0.13 / 0.56 | -0.07 / 0.47 | Nov 16: 2 / 33 |
| Flow imbalance >= 0.5, >= $250k | 136 | +0.01 / 0.48 | +0.09 / 0.54 | -0.07 / 0.43 | |
| Flow imbalance >= 0.5, >= $1M | 30 | +0.19 / 0.67 | +0.22 / 0.60 | -0.41 / 0.10 | |
| Same, without 0DTE, >= $250k | 117 | 0.00 / 0.48 | +0.08 / 0.52 | -0.13 / 0.43 | |
| O1 type (calls minus puts detected) | 902 | +0.01 / 0.50 | -0.03 / 0.43 | +0.05 / 0.53 | Nov 16: **249 / 8** |

**Conclusion Q2.**
- Classified flow tracks the session's direction: on the day, and in the bearish stream on Nov 16.
- The contract type (O1) does not.
- As a stand-alone forward predictor, flow is weak on this sample.
- It is useful as **confirmation** of a stock-side trigger (section 5.2) and as the **direction** of an activity alert.

## 4. Q3: candidate O1 rules, replayed

All rules except R0 apply the AL4 floor. "Ask-dominant" = the contract's session ask-side premium over stored prints is
>= 2x its bid-side premium and >= $25k (HIGH/MEDIUM confidence only). The bucket baseline (rule B) is the mean over
prior sessions of the median day volume, by underlying x type x DTE bucket (0, 1-2, 3-7, 8-21, 22-60) x OTM bucket
(-5/-2/-1/0/1/2/5%), taken from the final chain captures since Nov 9. "Keep" = stored O1 alert contracts the rule keeps
(of 67).

The u columns are the underlying move signed by direction (mean % / hit). The o columns are the option mid return (mean
/ median / win, with n).

| Rule | Alerts | Bull / bear | Per session (10, 11, 12, 13, 16) | With day | Keep | u +60 | u close | o +60 | o close |
|---|---|---|---|---|---|---|---|---|---|
| R0 today: vol/OI >= 3 | 94 | 76 / 18 | 14/3, 12/6, 6/6, 23/1, 21/2 | 0.30 | 67 | +0.05 / 0.48 | +0.05 / 0.57 | +2% / -2% / 0.42 (66) | -26% / -20% / 0.38 (90) |
| R0F: R0 + floor (AL4) | 84 | 69 / 15 | 14/3, 10/6, 6/6, 21/0, **18/0** | 0.29 | 60 | +0.08 / 0.48 | +0.07 / 0.58 | +2% / -4% / 0.42 (59) | -29% / -43% / 0.36 (81) |
| R0F 0DTE only | 49 | 41 / 8 | 6/1, 7/5, 0/2, 15/0, 13/0 | 0.25 | 36 | +0.03 / 0.48 | -0.09 / 0.53 | -2% / -17% / 0.30 (33) | **-54% / -90% / 0.17 (48)** |
| R0F DTE >= 1 | 35 | 28 / 7 | 8/2, 3/1, 6/4, 6/0, 5/0 | 0.34 | 24 | +0.16 / 0.48 | +0.30 / 0.66 | +8% / +3% / 0.58 (26) | **+8% / +4% / 0.64 (33)** |
| E1: R0F + ask-dominant | 27 | 21 / 6 | 5/2, 6/3, 1/1, 5/0, 4/0 | 0.41 | 19 | +0.15 / 0.48 | +0.07 / 0.59 | +15% / +3% / 0.63 (19) | -13% / -13% / 0.44 (27) |
| E2: R0F + triggering print at ask | 56 | 46 / 10 | 10/3, 7/3, 3/4, 11/0, 15/0 | 0.27 | 48 | +0.14 / 0.53 | +0.09 / 0.55 | +3% / +1% / 0.53 (40) | -33% / -43% / 0.36 (56) |
| S1: R0F + bid-dominant -> "sold", direction = -type | 26 | 6 / 20 | 2/6, 2/1, 2/2, 0/6, 0/5 | **0.85** | 0 | +0.25 / 0.52 | +0.23 / 0.50 | (contract itself, long) -9% | -39% / -64% / 0.32 |
| A1: ask-dominant >= $100k (no ratio) | 43 | 29 / 14 | 5/3, 6/3, 5/4, 8/4, 5/0 | 0.49 | 13 | +0.10 / 0.54 | +0.11 / 0.56 | -6% / -1% / 0.48 (29) | -9% / 0% / 0.50 (42) |
| A2: ask-dominant >= $250k (no ratio) | 25 | 17 / 8 | 2/3, 4/1, 5/2, 5/2, 1/0 | 0.56 | 7 | +0.31 / 0.62 | +0.12 / 0.56 | -2% / +2% / 0.50 (18) | -14% / -4% / 0.36 (25) |
| B1: volume >= 3x bucket baseline | 135 | 84 / 51 | 22/7, 12/7, 12/9, 26/15, **12/13** | 0.49 | 23 | +0.10 / 0.56 | +0.11 / 0.57 | +2% / 0% / 0.53 (85) | +2% / +1% / 0.53 (122) |
| B1 DTE >= 1 | 126 | 76 / 50 | 19/7, 12/6, 12/9, 22/15, 11/13 | 0.50 | 16 | +0.09 / 0.54 | +0.09 / 0.55 | +2% / 0% / 0.54 (84) | +1% / +1% / 0.53 (113) |
| B2: B1 + ask-dominant | 49 | 27 / 22 | 7/3, 5/4, 5/6, 9/6, 1/3 | 0.53 | 6 | **+0.20 / 0.59** | +0.12 / 0.61 | +2% / +1% / 0.59 (29) | **+4% / +2% / 0.60 (45)** |
| B4: volume >= 5x baseline + ask-dominant | 39 | 21 / 18 | 4/3, 4/2, 5/5, 7/5, 1/3 | 0.56 | 4 | +0.13 / 0.62 | -0.01 / 0.49 | 0% / 0% / 0.48 (23) | -1% / 0% / 0.50 (36) |
| C1: volume / strike OI (C+P) >= 1.5 | 81 | 67 / 14 | 12/3, 13/6, 3/5, 23/0, 16/0 | 0.28 | 50 | +0.04 / 0.54 | +0.01 / 0.57 | +2% / -5% / 0.41 (49) | -34% / -43% / 0.24 (75) |
| C3: C1 + ask-dominant | 26 | 20 / 6 | 5/2, 7/3, 0/1, 5/0, 3/0 | 0.42 | 17 | +0.11 / 0.56 | +0.02 / 0.58 | +13% / +3% / 0.62 (16) | -17% / -15% / 0.38 (24) |
| REF: every floor-pass contract | 243 | 178 / 65 | 26/9, 27/12, 19/14, 69/17, 37/13 | 0.37 | 60 | -0.03 / 0.49 | +0.01 / 0.56 | +1% / 0% / 0.48 (151) | -13% / -2% / 0.44 (227) |
| REF DTE >= 1 | 188 | 131 / 57 | | 0.41 | 24 | -0.01 / 0.52 | +0.03 / 0.54 | +4% / +1% / 0.53 (122) | +1% / +1% / 0.53 (175) |

**Option outcomes to the close, by session:**

| Rule | Nov 10 | Nov 11 | Nov 12 | Nov 13 | Nov 16 |
|---|---|---|---|---|---|
| R0F 0DTE | +63% (n=7) | -55% | -85% | -76% | -89% |
| R0F DTE >= 1 | +5% | -8% | +23% | +7% | -3% |
| B2 | +6% | +5% | +2% | +3% | -3% |
| E1 | +37% | -24% | -22% | -7% | -79% (4 call alerts) |

**Move so far at the trigger** (open to the trigger bar close, in ATR; "against" = the alert direction is opposite to a
move of more than 0.25 ATR):
- R0: 47 of 94 alerts were against the move and 8 were with it.
  - Against: -45% mean to the close, underlying +0.01 / 0.57. There is no mean-reversion edge; the option loses its time
    value.
  - With: n=8, +3%.
- B1: balanced (42 with / 48 against / 45 flat).
  - With: +0.32% / 0.57 to the close; option +4%.

**Price floor:** 13 of R0's 94 alerts were entered under $0.25 with a spread of 15% or more, all 0DTE. The AL4 floor
keeps all 13. The 0DTE gate removes all 13 (0 of 35 in R0F DTE >= 1).

**Reading Q3.**
- **Removing 0DTE is the single largest improvement** to today's rule on every measure. It removes, so it is also the
  safest change.
- **Aggressor dominance (E1) is the right direction semantics,** but on its own it does not create bearish alerts on a
  down day. On Nov 16 the watched puts do not pass ratio >= 3, so E1 gave 4 bullish alerts and 0 bearish ones.
- **Volume vs its own bucket baseline (B1/B2) is the only rule that is direction-balanced on the down day** (12/13, and
  1/3 with B2). It is also the only one with non-negative option outcomes across sessions. It replaces today's alerts
  rather than filtering them (it keeps 23 / 6 of 67), and its baseline rests on 1-5 prior captures today.
- **Normalising OI per strike (C)** does not fix the direction (with day 0.28). Drop it.
- **"Sold" dominance (S1)** agrees with the day 85% of the time (n=26). It is evidence that bid-side flow on ratio
  contracts is the bearish read of a "call" alert. Record it as an observation; do not publish it as an alert.

## 5. Q4: market conditions, levels, reversal, sideways

### 5.1 Coverage matrix

| Condition | Stock-side trigger today | Options confirmation / detector today | Package today | What fired Nov 10-16 | Gap | Should fire (target) |
|---|---|---|---|---|---|---|
| Trend up | OS1/OS2 +1 (prior close + 0.5 ATR, 5m); Stock Alerts B3 acceptance, R2 resumption (30m/1h, equity only) | O1 vol/OI >= 3 on a call; OS confirmation = O1 ratio on a same-type watched contract | long call; AL2 call debit vertical (ON since Nov 16 16:49) | O1 calls every day; OS +1 Nov 11 (6 alerts) | O1 calls fire on any day (with day 0.30); OS confirms rarely (21 of 340) | OS1/OS2 or B3/R2 +1 confirmed by bought calls / sold puts (OS v2); O1 call only when ask-dominant |
| Trend down | OS1/OS2 -1; B3/R2 short | O1 on a put | long put; AL2 put vertical | Nov 16: O1 2 put alerts (both under the floor), OS 1 put alert | The down day produced call alerts (24 C / 2 P); watched puts are high-OI and cannot pass ratio | same, mirrored; symmetric watched set; baseline rule B for the put side |
| Reversal at support (bullish) | none on Options; Stock Alerts F2 (failed breakdown, 30m/1h, 20-bar range, ADX < 20 context) on equity | none | | O1 calls after drops by accident (47 of 94 alerts against the move: no edge, -45% option) | no level-aware Options lane | level rejection (PDL, ORL, put wall) or F2 long, plus bought calls / sold puts in the last 20 min; DTE >= 1 call or call vertical |
| Reversal at resistance (bearish) | F2 failed breakout (equity) | none | | none | same | rejection at PDH, ORH or call wall, or F2 short, plus bought puts / sold calls |
| Sideways / range | none (F2's ADX < 20 context only) | O3 is not neutral: a credit vertical in the direction of the 30m EMA50 slope, activity leg ratio >= 3, DTE 7-30, OI >= 500 per leg | O3 credit vertical | **O3: 0 evaluations** (BLOCKED Nov 10-13, COMPLETE_ZERO Nov 16); O2 UNAVAILABLE | no range regime, no neutral structure; AL2 verticals are directional | daily regime (ADX < 20, price between put and call walls, IV/HV >= 1) to a defined-risk neutral structure (iron condor 7-30 DTE); observation first |
| High IV | none (V-RICH, E-EARN designed) | | | NVDA IV/HV 1.13-1.27 Nov 11-16 (likely pre-earnings, not verified); O1 bought NVDA premium into it | no IV-state gate on long premium | V-RICH / E-EARN: credit structures; block or downgrade long-premium O1 packages |
| Low IV | none (V-CHEAP designed, needs 20+ sessions of IV history) | | | IV/HV 0.62-0.97 for the other 8 names (HV20 inflated by the recent swings) | no IV rank history | long premium / debit verticals / calendars |
| Term structure | V-TERM designed | | | | | calendar (later) |

### 5.2 Levels: breaks and rejections, with and without flow (5m bars after the first 30 minutes)

**Levels:** PDH, PDL, PDC (prior day high, low, close), ORH/ORL (opening range = first 30 minutes), CALL_WALL / PUT_WALL
(the largest-OI call strike above, or put strike below, the prior close within 5%, DTE 0-30).
- **Rejection:** the bar trades through the level and closes back.
- **Break:** the close is beyond the level by >= 0.1 x the 5m ATR.
- **Flow bias:** bought premium of the event's direction minus the other side's, over the prints in the 20 minutes up
  to the bar end. No look-ahead.

Each cell is mean signed move % / hit rate:

| Events | n | +30 min | +60 min | to close |
|---|---|---|---|---|
| Rejections, all | 186 | -0.02 / 0.50 | +0.01 / 0.48 | +0.01 / 0.48 |
| Rejection + confirming flow (bias > 0) | 49 | +0.05 / 0.53 | **+0.18 / 0.61** | +0.04 / 0.55 |
| Rejection + bias > $100k | 12 | +0.23 / 0.67 | +0.58 / 0.75 | -0.29 / 0.33 |
| Rejection, no classified flow | 97 | -0.10 / 0.44 | -0.12 / 0.41 | -0.15 / 0.39 |
| Rejection at PDC (all) | 25 | -0.10 / 0.33 | -0.26 / 0.27 | -0.14 / 0.40 |
| Breaks, all | 230 | +0.01 / 0.47 | +0.02 / 0.51 | -0.01 / 0.47 |
| Break + confirming flow | 71 | +0.08 / 0.61 | **+0.16 / 0.63** | -0.04 / 0.49 |
| Break + bias > $25k | 48 | +0.11 / 0.64 | +0.23 / 0.68 | -0.04 / 0.48 |
| Break, no classified flow | 101 | 0.00 / 0.40 | -0.07 / 0.40 | 0.00 / 0.50 |

- **Per session,** the +60 min hit of confirmed vs unconfirmed rejections was:
  - Nov 10: 0.58 vs 0.50;
  - Nov 11: 0.67 vs 0.48;
  - Nov 12: 0.54 vs 0.40;
  - Nov 13: 1.00 (n=3) vs 0.38;
  - Nov 16: 0.50 (n=4) vs 0.41.
- **Breaks show the same pattern,** except on Nov 11-12, where the two groups were level.
- **ETFs and single names:** the same pattern in both.

### 5.3 OS1/OS2 with aggressor confirmation, and a "failed OS" reversal

The 340 rebuilt OS signals come from `os-nearmiss-20261117/out2.json`. Flow bias is measured in the 20 minutes up to the
signal bar's end. Each cell is mean signed move % / hit rate:

| Group | n | +30 min | +60 min | to close | Per session bull/bear (10, 11, 12, 13, 16) |
|---|---|---|---|---|---|
| All OS signals | 340 | 0.00 / 0.54 | +0.03 / 0.58 | -0.09 / 0.43 | |
| Confirmed live (O1 ratio) | 21 | -0.09 / 0.57 | -0.01 / 0.59 (17) | -0.56 / 0.24 | |
| Flow bias > 0 | 104 | +0.03 / 0.63 | **+0.08 / 0.63** | -0.29 / 0.35 | 0/16, 45/1, 3/9, 0/0, **0/30** |
| Flow bias > $100k | 27 | +0.08 / 0.65 | **+0.21 / 0.69** | -0.56 / 0.15 | 0/8, 13/0, 1/2, 0/0, 0/3 |
| Flow bias <= 0 | 236 | -0.02 / 0.50 | +0.01 / 0.56 | 0.00 / 0.47 | |
| Failed OS (back inside the band within 3 bars) -> opposite direction | 30 | -0.09 / 0.40 | -0.19 / 0.41 | -0.30 / 0.47 | |
| Same, with flow bias > 0 | 12 | -0.04 / 0.42 | -0.10 / 0.42 | +0.03 / 0.67 | |

**Reading:**
- Flow confirmation makes OS follow the day: 30 bearish and 0 bullish on Nov 16, 45 bullish and 1 bearish on Nov 11.
- It confirms about 5 times more signals than O1-ratio confirmation, with better 30-60 minute outcomes.
- Every OS group, whatever its confirmation, gives the move back into the close.
- "Failed OS" is not a reversal signal on this sample.

### 5.4 Sideways evidence

- **Range regime from intraday efficiency** (60-minute efficiency ratio at each 5m bar): it does not persist. The next
  60 minutes' efficiency has a median of 0.245 after a past ER < 0.2, and 0.236 after a past ER >= 0.5. The next 60
  minutes' absolute move has a median of 0.07 ATR in every bucket.
- **Dealer gamma at the prior close** (OI-weighted, calls +, puts -, as in `options_gamma_read`):
  - Positive gamma: 24 name-days; 16 range days; median range 0.71 ATR; efficiency 0.60.
  - Negative gamma: 21 name-days; 14 range days; 0.84 ATR; 0.39.
  - The sign is a name effect: AAPL, AMZN, MSFT and TSLA are always positive; IWM, QQQ, SPY, NFLX and NVDA mostly
    negative.
  - It is not a regime switch on this sample.
- **O3:** see the coverage matrix. In its current form it cannot fire on a range day, because it requires a 30m trend
  slope in the spread's direction.
- **IV vs realized** (ATM IV at 20-45 DTE vs HV20):
  - NVDA 1.13-1.27 from Nov 11 (the Nov 10 capture has a bad IV, 0.03).
  - Every other name 0.62-0.97.
  - IV/HV rose on Nov 16, for example SPY 0.67 to 0.86.
  - V-RICH would have fired only on NVDA.

## 6. Answers to the research questions, short

1. **Q1 structural bias.** Verified on all 5 sessions and symmetric. Formerly-ITM contracts carry 0.4-0.75x the OI and
   have the higher vol/OI. 0DTE always passes. The AL4 floor removes most of the count imbalance on the full chain, but
   not in the watched set. The watched set (call-heavy, with high-OI puts) plus 0DTE produce O1's 24/2 on Nov 16.
2. **Q2 direction.** 31% of premium is classifiable (39% with the inside-spread lean). The net bought premium follows
   the day (68% of name-days; bearish on Nov 16). The contract type does not (Nov 16 rolling: 249 bullish vs 8
   bearish). Forward predictive power alone is weak.
3. **Q3 rules.**
   - 0DTE exclusion: strongest, and safe.
   - Aggressor dominance: right semantics, few alerts.
   - Bucket-baseline volume (B1/B2): direction-balanced, and the best outcomes. It needs a baseline history.
   - Strike-OI normalisation and the per-underlying imbalance as a trigger: weak.
4. **Q4 coverage.** Trend: OS plus O1, both mis-confirmed. Reversal and sideways: nothing on Options. High/low IV:
   designed only. Flow-confirmed level events and OS signals are the best-supported addition. Range routing has no
   evidence yet.

## 7. Recommendations (prioritized; each is switch-guarded OFF / RECORD / ON, RECORD first, like the AL4 floor)

The switch and policy names are proposals. None of them is built.

### P1-1 O1 0DTE gate

- **Switch and policy:** `REALTIME_V3_O1_ZERO_DTE` on the Options window. The policy becomes
  `option_participation_origin_v3` = v2 floor + `minimum_dte: 1`.
- **Evidence:** the R0F 0DTE vs DTE >= 1 rows in section 4.
  - 0DTE: 49 of 94 alerts, -54% mean / -90% median to the close, win 0.17, losing on 4 of 5 sessions.
  - DTE >= 1: +8% / +4% / 0.64.
  - The gate also removes all 13 low-price, wide-spread entries.
- **Record:** one line per O1 event with DTE, entry mid, spread %, would-drop, and the +60 min and close marks.
- **ON:** after 2 RECORD sessions, if the dropped set again underperforms. The change only removes alerts, so the risk is
  low.
- **Later:** 0DTE belongs to a dedicated F-0DTE lane (volume vs the 0DTE bucket baseline, GEX/pin context), not to the
  vol/OI rule.

### P1-2 O1 direction from the aggressor side

- **Switch and policy:** `REALTIME_V3_O1_AGGRESSOR_SIDE`, policy `option_participation_origin_v3_aggressor`.
- **Rule:** a DETECTED alert needs ratio >= 3, the floor, and a contract that is ask-dominant over its session's stored
  prints (ask premium >= 2x bid premium and >= $25k, HIGH/MEDIUM confidence). Its direction is the contract type.
  - Bid-dominant: an OBSERVATION `SOLD_PARTICIPATION` with direction -type and no package.
  - Otherwise: an OBSERVATION `SIDE_UNRESOLVED` with direction NONE.
- **Evidence:** E1 vs R0F (with day 0.41 vs 0.29; option +60 min +15% / +3% / 0.63 vs +2% / -4% / 0.42; to the close
  -13% vs -29%). S1: sold-dominant agrees with the day 85% of the time (n=26). On Nov 16, O1's calls were net sold.
- **Record:** per event, the cumulative ask, bid, inside, stale and complex premium; the spread-position lean; the v1
  vs v3 direction; and the outcomes.
- **ON:** after 5 or more RECORD sessions that include an up-trend day and a down-trend day. It must beat R0F at +60
  minutes and be no worse to the close.
- **Companion (P1, flow engine, RECORD metrics first): raise side coverage.**
  - Investigate QUOTE_STALE: 12.5% overall, 21% on Nov 16, mostly NVDA and NFLX, median age 10 s. Check the quote
    subscription for the watched contracts.
  - Add a lean class for inside-spread prints at position >= 0.7 or <= 0.3 (+7.7 points of premium).
  - Decide whether LOW-confidence ask/bid prints (496) count.

### P1-3 Symmetric watched set (slot policy, not the detector)

- **Proposal:** equal call and put slots per underlying, at matched |delta| bands (for example 0.2-0.6) and the same
  expiries.
- **Record:** per window and per type: watched counts, ratio and baseline passes, and stored prints.
- **Evidence:** calls are 61-65% of watched contracts and 72% of stored prints with DTE >= 1. At the Nov 16 close, 57
  watched calls vs 6 watched puts passed ratio >= 3 with the floor; on the full near-spot chain it was 209 vs 227. This
  is the largest single driver of Nov 16's 24 C / 2 P.
- **Owner:** this is capacity-bound (R81, V4), so the live owner decides. Not verified: why the selection is
  call-heavy (the selection code was not reviewed).

### P1-4 OS v2 aggressor confirmation (being built in RECORD by the build agent; do not duplicate)

- **This replay supports it independently.** With flow in the 20 minutes before the signal, OS signals hit 0.63 at +60
  minutes (n=94). Over $100k they hit 0.69 (n=26). O1-ratio confirmation hits 0.59 (n=17). Flow confirmation also
  follows the day (Nov 16 0/30, Nov 11 45/1).
- **Suggestions for the record line:**
  - store the 20-minute bought-premium bias by type;
  - store the +30 and +60 minute and close outcomes;
  - store the level context (PDH/PDL/OR/walls).
- **After RECORD,** decide whether OS packages need a time exit (for example 60 minutes) or a level stop instead of the
  session close. Every OS group faded into the close on these 5 days.

### P2-1 O1 baseline rule

- **Policy and switch:** `option_participation_baseline_v1`, `REALTIME_V3_O1_BASELINE`.
- **Rule:** cumulative day volume >= 3x the prior-session median day volume of the same underlying x type x DTE bucket x
  OTM bucket, plus the floor. Direction comes from the P1-2 side gate (that is rule B2).
- **Evidence:**
  - B1: 135 alerts, 84 bull / 51 bear; Nov 16 12 / 13; with day 0.49; option to the close +2% / +1% / 0.53 vs R0F -29%
    / -43% / 0.36.
  - B2: 49 alerts, 27 / 22; +60 min +0.20% / 0.59; to the close +4% / +2% / 0.60.
- **Record:** next to v1 for at least 10 sessions. It replaces today's alerts rather than filtering them (it keeps
  23 / 6 of 67), so it must not go ON on today's evidence.
- **Needs:**
  - H1 (daily chain history, IMPLEMENTED, not deployed). The retained final captures give only 1-6 prior sessions today.
  - The baseline should be the median over at least 10 sessions, with 0DTE as its own bucket.

### P2-2 Reversal lane

- **Proposal:** "OS3 level rejection", RECORD only, `REALTIME_V3_OS_REVERSAL`.
- **Trigger:** a 5m bar trades through PDH, PDL, ORH, ORL, the call wall or the put wall, and closes back. PDC is
  excluded: rejections there failed, with +60 min hit 0.27 (n=22).
- **Confirmation:** bought premium in the reversal direction in the 20 minutes before; the same measure as OS v2.
- **Alternative trigger:** the Stock Alerts F2 failure family (30m/1h; ADX < 20 context; already researched and
  designed as the failed-breakout rule), consumed from the canonical stock alerts.
- **Package:** a DTE >= 1 long option or an AL2 vertical in the reversal direction.
- **Evidence:** rejections confirmed by flow hit +0.18% / 0.61 at +60 minutes (n=41), against -0.12% / 0.41 with no
  classified flow. The pattern holds in 4 of 5 sessions. "Failed OS" showed no edge (n=30).
- **Record:** at least 10 sessions.

### P3 Sideways and IV regime (design; observation only until H1 has 20+ sessions)

- **R-REGIME per name per day:**
  - daily ADX14 < 20 (the F2 context);
  - price between the put and call OI walls;
  - IV/HV20 >= 1 (V-RICH).
- **Routing:** RANGE_RICH goes to a neutral defined-risk structure (iron condor, 7-30 DTE, short strikes at or beyond
  the walls or about 1 SD, wings 1-2 strikes). Trend days go to the directional lanes.
- **O3 needs re-scoping.** Today it is a trend credit vertical that never ran (0 evaluations in 5 sessions). Either keep
  it as the "trend + rich IV" credit lane, or remove the EMA-slope gate and the ratio activity leg for a range variant.
- **Do not use AL2 debit verticals for sideways markets.**
- **High IV** (NVDA before earnings; date not verified): downgrade O1 long-premium packages, or record an E-EARN flag on
  them.
- **The evidence for intraday range routing is absent** (section 5.4), so nothing should go ON here.

### Drop or defer

- **Contract vol/OI as a direction source.** Keep it as an "unusual activity" observation (U-SCAN, direction NONE).
- **OI normalised per strike (C1-C3):** no change to the direction bias.
- **Per-underlying 30-minute imbalance as a trigger:** weak forward edge (hit 0.52-0.60) and it fades into the close.
  Keep it as a confirmation and context input.
- **"Failed OS" as a reversal trigger:** no edge.
- **Any change built from the to-the-close outcomes:** every directional group fades into the close on this sample.
  Prefer +30/+60 minute horizons in RECORD.

### Needs more data

- **H1:** daily chain history, for the baselines and IV rank (V-CHEAP).
- **H2:** all trades, not just the stored large prints. This would give aggressor coverage below the flow-profile
  thresholds and for 3-4 DTE, which is never stored today.
- **More sessions:** at least one up-trend day and one more down-trend day in RECORD before any direction-changing
  switch goes ON.

## 8. Solid vs weak

**Solid** (structural, on all 5 sessions):
- the formerly-ITM OI asymmetry, which is symmetric for up and down moves;
- near-spot 0DTE vol/OI >= 12.7 every session;
- the watched set is call-heavy, and its ratio passes are call-heavy (24/9, 16/9, 21/8, 35/4, 57/6);
- O1 alerts disagree with the intraday direction (with day 0.30);
- O1's own call events were net sold on 3 of 5 days;
- 0DTE O1 alerts lost to the close on 4 of 5 sessions;
- directional coverage is about 31% of premium;
- O3 produced nothing.

**Weak** (5 sessions, one trend day, n of 25-135 per group):
- every forward-move edge (hit rates 0.55-0.69);
- the B1/B2 and E1 outcome gains;
- the flow confirmation of level events and OS signals (consistent in 4 of 5 sessions, but small n);
- S1 "sold" signals;
- everything about reversal and sideways.

**Not verified:**
- why the watched set is call-heavy;
- the NVDA earnings date (the high IV is consistent with earnings);
- the provider's IV on the Nov 10 NVDA capture (0.03, bad);
- O3's per-candidate reasons on Nov 16 (COMPLETE_ZERO = no candidate reached the executor);
- AL2 vertical outcomes, which were not separated from single-contract outcomes;
- intraday V4 slot reallocations (the watched set is each cohort's final set).

**Measurement limits:**
- Stored prints are a filtered subset: large prints only, watched contracts only, no 3-4 DTE.
- Entries on Nov 10 and Nov 13 mostly use the print quote, because cutoff marks are sparse.
- Option returns are mid to mid, without costs.
- "With day" is ex post.

## 9. Reproduce

The scripts live in `backend/.realtime-canary/v3-supervised-20261102/recovery-20261104/evidence/detector-research-20261117/`
and use plain Python 3.11 with no pandas. They read the CSV exports and need no DB access:

| Script | Content | Output |
|---|---|---|
| `lib.py` | loaders | |
| `sessions.py` | day classes | `out_sessions.json` |
| `structural.py` | Q1 | `out_structural.json` |
| `o1_today.py` | O1 detections by type and moneyness | `out_o1_today.json` |
| `flowdir.py` | Q2 | `out_flowdir.json` |
| `rules.py` | Q3 | `out_rules.json`, `out_rules_detail.json` |
| `context.py` | move so far, price floor, IV/HV | `out_context.json` |
| `levels.py` | levels, gamma, range persistence | `out_levels.json`, `out_level_events.json` |
| `os_flow.py` | OS with flow, failed OS | `out_os_flow.json` |

New read-only exports (`export.py` + `x_flow.sql`, `x_lanes_all.sql`, `x_evalcount.sql`) were run in the Options
scheduler container (statement_timeout 60 s, read-only session) and saved to `data/flow.csv`, `data/lanes_all.csv` and
`data/evalcount.csv`. The other inputs come from `o1lf-20261117/data/`, `os-nearmiss-20261117/data/` and
`research/chains/`.
