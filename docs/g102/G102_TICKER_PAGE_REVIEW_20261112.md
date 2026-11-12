# G102 ticker page review (Nov 12, 2026)

Scope: the realtime portal ticker page (Stocks -> a ticker): Overview (trade plan), Timeframes, Levels, Fibonacci,
Financials. Review session in worktree `StockScreener.worktrees\ticker-review`, branch `ticker-page-review` from
c46f988. No live actions; read-only GETs to the live API and read-only SQL on the copy.

Readers: the live session (merges, builds r95/f30, adds matrix rows) and the user (decides Step 2 and Step 3 items).

## 1. Summary

| Tab | Verdict | Main finding |
|---|---|---|
| Overview / trade plan | **Not reliable** | Every SHORT plan is the bare ATR fallback (37 of 37); 68 of 78 LONG plans pick a stop inside 1 ATR and are labelled "Not tradeable"; "Setup quality" counts bullish and bearish inputs together (108 of 125 setups graded A/A+). |
| Timeframes | Pipeline exact, some definitions non-standard | Served values equal a recomputation from stored bars (6 tickers x 5 intervals x 21 fields, 0 differences). But RSI is a 14-change simple average (up to 26 points from Wilder RSI), ATR is a simple mean, monthly ADX is unconverged, hourly HV uses 6.5 bars/day (there are 7), intraday RVOL ignores time of day (the 15:30 bar reads 2.8-3.6x when it is 0.7-0.9x of its own slot). |
| Levels | Too much, weakly ranked | 18-40 zones per ticker (median 26); 90% of zones within 5% of price are labelled confluence; ATR projections and duplicate Fibonacci references inflate strength; some "zones" are 10-34% of price wide. Each of the 7 shown rows has 4-6 lines of text. |
| Fibonacci | Layout | Two free-form cards and a ladder, each with its own grid; nothing lines up. |
| Financials | Layout and a data bug | Three separate auto-width tables (the label column width differs per table, so period columns shift). The page asks for 12 reports across all timeframes, so AAPL's quarterly table shows Q4 and Q3 only (5 non-consecutive quarters) while the store holds 9 consecutive. |

## 2. Method and data

- Live API, read-only GETs, Nov 12 ~06:00 UTC: `/realtime/portal/current` (Equity cohort effad99e), then for the 25
  tickers in `/tickers`: `workspace-publication`, `trade-setup/multi`, `quote` (retained mode, snapshot pinned). The
  served publication is the Nov 11 close (market_time 2026-11-11T21:00Z), the same point the copy holds.
- Copy `test_realtime_v2_r92` (read-only session): bars only through `realtime.equity_bar_repository.load_bar_range_cursor`,
  the windows the ticker projection uses (1d ADJUSTED last 283, 1wk ADJUSTED last 261, 1mo ADJUSTED last 60, 30m and 1h
  RAW last 500), then (a) the production `compute_trade_setup_technicals` and (b) textbook formulas.
- Frontend plan logic replayed in Python from `tickerPlan.ts` over all 125 served setups.
- Scripts: `docs/g102/ticker-review-tools/` in this worktree (`fetch_live.py`, `plan_replay.py`,
  `recompute_technicals.py`, `fetch_profile.py`). Run in the r94 image; the password file is mounted, never printed.

## 3. Overview: trade plan

### 3.1 How it is computed today (end to end)

1. **Backend, per interval, from final bars only** (`equity/materialization.py` -> `materialize_equity_evidence`):
   1d/1wk/1mo on ADJUSTED bars, 30m/1h on RAW bars, windows above. The developing bar never enters.
2. **Direction** (`equity/setup_composition.py: compose_setup_direction`): a vote of about 15 rules (MA crossover state,
   EMA 8/21/50 stack, price vs EMA 8, price vs VWAP(20), higher-timeframe EMA, retests, pullback/bounce grades, gaps,
   FVGs, RSI < 35 / > 65, golden/death cross). More bull points -> Bullish, more bear -> Bearish, equal -> Neutral.
3. **Levels** (`compose_trade_levels(directional_brackets=True)`): candidates from Fibonacci, gaps, prior 252-bar
   high/low, swing low (20 bars), EMA/SMA below price, VWAP below price, plus ATR fallbacks (1 ATR stop, 2 ATR target).
   With directional brackets a Bullish setup gets `targets` above the close and `stops` below; a **Bearish setup gets
   `targets` below and `stops` above**; Neutral gets none. Top 5 of each are stored.
4. **Frontend** (`TickerDetail.tsx`): the analysis interval is the pin (selector) or the chart interval snapped to the
   nearest published frame at or above it; `tradeSetup = multi.setups[interval]`.
5. **Frontend plan** (`pages/ticker/tickerPlan.ts: buildTradePlan`): entry = `setup.last_close`; LONG takes the nearest
   stop below and nearest target above; SHORT takes **`targets` above the entry as stops and `stops` below as targets**
   (the pre-directional contract), adds its own ATR stop/target.
6. **Frontend verdict** (`evaluatePlan`): blocking checks "Reward covers risk" (>= 1R) and "Stop clears noise"
   (>= 1 ATR); scored checks 2R floor, higher-timeframe EMA agreement, confluence grade A/B, entry still valid (live
   quote within 0.5R); cautions from the published daily context. Computed in the browser, not recorded.

### 3.2 Where it is fragile (measured on the Nov 11 close publication, 125 setups)

| # | Finding | Evidence |
|---|---|---|
| F1 | **SHORT plans ignore every served level.** Step 5 reads the old contract; with directional brackets nothing is above-as-target or below-as-stop, so only the frontend ATR pair is left. Every short is 1.00 ATR stop, 2.00R target, and passes the geometry checks automatically. | 37 of 37 Bearish setups -> ATR/ATR. AMZN 30m: served stop "Prior 252-bar high $275.34", served first target "Swing low $265.68"; page shows ATR stop $267.79 / ATR target $262.99. |
| F2 | **LONG stop is the nearest level, then blocked for being near.** The served stops always include the ATR stop at exactly 1 ATR, so the nearest stop is at most 1 ATR away; the check requires at least 1 ATR. It passes only when the ATR fallback itself is chosen, and then only by rounding luck. | 68 of 78 LONG plans have stop < 1 ATR ("Not tradeable"); of 10 plans that used the ATR stop, 4 fail at 0.99x by rounding. AMD 1d: stop "50 SMA" at 0.06 ATR. |
| F3 | **"Setup quality" grade counts opposing inputs.** `compose_setup_confluence` grades `len(signal_reasons)`, which lists bull and bear reasons together. The plan's "Signal confluence" check passes on any A/B grade. | 108 of 125 setups are A/A+; 97 of those include opposing inputs; all 10 Neutral setups grade A+/A/B+. CRWD 1h: 4 bull vs 4 bear -> "A+ - 8 directional inputs". |
| F4 | **Entry is the last completed bar of the interval.** Weekly entry = last week's close, monthly = last month's close. | AAPL monthly entry $344.63 (Oct 31) vs daily close $355.63; target "Prior 59-month high $345.34" -> 0.02R. |
| F5 | **Short-side candidates are thinner.** EMA/SMA, swing levels and VWAP are only added below price, so shorts never get an EMA/SMA/swing-high/VWAP stop. | `setup_composition.py` lines 592-629. |
| F6 | **Split in the raw window is labelled, not withheld.** `RAW_SPLIT_WINDOW_REVIEW` goes to `missing_producers`; 30m/1h setups are still computed on mixed-scale bars and the page never shows the label. | `ticker_projection.py` ~1053. No split in the window today (CRWD 1:4 on Jul 2 just aged out of the 90 sessions). |
| F7 | **Two definitions, no record.** Level choice is split between `compose_trade_levels` (backend) and `buildTradePlan` (frontend) with different assumptions (F1 is the result); the verdict is not stored, so a plan cannot be audited later or checked against stored bars. | - |
| F8 | RSI definition feeds the vote (RSI < 35 / > 65) and the timing tile. | See 4.2: up to 26 points from the standard. |

Missing levels: if either side has no candidate the page shows "No executable plan" (correct, INV-8). Stale setups
show a "Stale analysis" pill. The developing bar does not enter the plan (correct).

### 3.3 What would make it reliable (proposal TP5)

- **One definition, server-side, once per publication:** a pure `compose_trade_plan(levels, technicals, side, entry)`
  in `equity/setup_composition.py`, stored as `setup.plan` with its inputs: entry and entry basis, the candidate list,
  the chosen stop and target with the rule that chose them, ATR, R multiple. The frontend renders `setup.plan`; only
  the "now" marker and "entry still valid" use the live quote.
- **Symmetric rules:** shorts mirror longs (same candidate families above price).
- **Stop rule:** the nearest structural level at least 1 ATR from the entry (compared at 0.995 so rounding cannot
  decide), else the ATR fallback; first target: the nearest structural level beyond the entry; second target shown.
- **Entry basis:** 30m/1h/1d use the interval's close; 1wk/1mo use the last daily close and say so.
- **Grade:** count only inputs on the bias side (or show agreement `bull/(bull+bear)`); Neutral has no grade.
- **Tests:** a unit test per rule (long/short mirror, stop beyond noise, rounding, missing side, gap levels, split
  label), and a copy check: for every stored setup, the plan's stop and target are among the candidates and on the
  correct side of the entry.
- **Split:** when the raw window contains a split, withhold 30m/1h setups (or show the label on the page).

Quick, frontend-only part (TP2 + TP3) can go in f30: read the directional brackets for SHORT, and apply the stop rule
above in `buildTradePlan`. It changes which plans are "Not tradeable", so TP3 is the user's call.

### 3.3a When "Take" shows, measured (Nov 12 ~07:30 UTC)

The verdict is "Take" only when all six checks pass (reward >= 1R and stop >= 1 ATR are blocking; >= 2R; higher
timeframe EMA agrees; grade A/B; price within 0.5R of the entry) **and** the published daily context is READY and
not countertrend. Risk, reward, ratio, the sentence and the hover list are all computed from the same chosen stop and
target, so they stay consistent; the inputs are the problem.

The page's own `tickerPlan.ts` (with TP2) over all 125 served setups (10 neutral have no plan), live quotes, and the
served daily context:

| Rule | Context as served | Not tradeable | Context unavailable | Wait | Take with care | Take |
|---|---|---|---|---|---|---|
| Today (nearest stop, nearest target) | served | 109 | 6 | 0 | 0 | 0 |
| Today | READY | 109 | - | 4 | 2 | **0** |
| Stop >= 1 ATR (TP3) | READY | 86 | - | 19 | 10 | 0 |
| Stop and target >= 1 ATR | READY | 43 | - | 47 | 25 | 0 |
| Candidate: stop = nearest level 1-3 ATR away, else 1 ATR; first target = nearest level >= 1R away, else a 2R ATR projection | READY | 8 | - | 48 | 38 | 21 |

- **F9 Pre-market context:** `/stocks/context` serves `status: UNAVAILABLE` for all 25 tickers before the open
  although the rows carry the Nov 11 close values (AAPL trend "Bullish Stack"), so off-hours no plan can be "Take".
  To verify in session.
- **F10 Candidate weaknesses:** a 2R ATR projection passes the 2R floor by construction (many "2.00R"); some first
  targets sit very far (XLC 30m 12.7R); the 8 remaining "Not tradeable" are the 1 ATR fallback failing the check at
  0.99x by rounding. Projection targets should be a caution, not a pass; the noise check needs the 0.995 tolerance.
- Reliability claim needs outcomes, not only rules: replay plans from past sessions on the copy (283 daily, 90
  intraday sessions) and measure whether the target or the stop was hit first, per rule and interval.
- Recommendation: TP3 is withdrawn as a separate step; the rules go into TP5 together, with unit tests and the
  outcome replay. TP2 stays (bug fix). Script: `ticker-review-tools/plan_verdicts.mjs`.

### 3.4 Analysis timeframe selector (user request, TP1)

Remove the "Analysis timeframe" select, the "Following chart / Pinned / nearest published" pill and the "Follow chart"
button. The plan always follows the chart interval; on 1m/5m/15m it uses the nearest published frame and the plan
header says so ("... on hourly bars - nearest published to 5m"). The "Stale analysis" pill moves into the panel body.
`analysisPin` leaves `chartPreferences.ts`; an old `analysis_interval` URL parameter is ignored and dropped on the next
write. `EntityRail` keeps receiving the resolved interval for its provenance block.

## 4. Timeframes

### 4.1 What each column shows and where it comes from

| Column | Source field(s) | Definition today |
|---|---|---|
| Bias | `direction.bias` | Direction vote (3.1 step 2) |
| Trend | `ema_alignment.primary`, `technicals.adx`, `trend_consistency` | EMA 8/21/50 stack; ADX < 20 -> no trend; % of up (or down) closes in 14 |
| Structure | `structural_patterns[0]`, `zones` (Volume Pivot) | Newest confirmed H&S / double top / bottom; count of volume pivots (and near a Fib) |
| Direction | `adx`, `plus_di`, `minus_di` | ADX(14), +DI/-DI(14), Wilder-type EWM seeded at the first bar |
| Momentum | `rsi`, `macd_state` | RSI = 100-100/(1+avg gain/avg loss) over the **last 14 changes, simple average**; MACD 12/26/9 |
| Volume flow | `volume_sparkline`, `volume_trend_pct`, `relative_volume`, `volume_slope_state`, `volume_pressure` | Last 8 bars / 20-bar mean; 5-bar mean vs 20-bar mean; last bar / mean of previous 20 bars; slope of the 8; CMF(20) |
| Volatility | `historical_volatility_pct`, `_percentile`, `atr_pct` | HV20 annualized (1h uses 252x6.5); percentile vs last 252 windows; ATR = **simple mean of 14 true ranges** |

### 4.2 Recompute from stored bars (copy r92, Nov 11 close)

Pipeline integrity: AAPL, NVDA, AMZN, SPY, CRWD, TSLA x 30m/1h/1d/1wk/1mo: all 21 served technical fields equal the
production function run on the stored bars (0 differences), and every served last close equals the stored close.

Definitions vs textbook (served / textbook):

| Ticker interval | RSI / Wilder RSI | ATR / Wilder ATR | ADX / Wilder ADX | HV20 / HV20 (7 bars/day) | RVOL / same-slot RVOL |
|---|---|---|---|---|---|
| AAPL 30m | **92.5 / 75.3** | 1.27 / 1.23 | 31.0 / 31.0 | 18.6 / 18.6 | **2.80 / 0.84** |
| AAPL 1h | 69.0 / 69.3 | 2.00 / 2.15 | 18.3 / 18.3 | **17.9 / 18.6** | 1.19 / 0.84 |
| AAPL 1d | 54.9 / 60.4 | 9.58 / 8.65 | 17.5 / 17.5 | 27.5 / 27.5 | 0.67 |
| AAPL 1mo | 75.9 / 72.1 | 31.34 / 29.25 | **32.2 / 25.3** | 21.8 / 21.8 | 0.81 |
| NVDA 30m | **71.0 / 46.2** | 1.92 / 1.99 | 23.7 / 23.7 | 41.4 / 41.4 | 1.09 / 1.32 |
| AMZN 30m | **17.0 / 43.5** | 1.60 / 1.52 | 18.7 / 18.7 | 19.3 / 19.3 | **1.87 / 0.72** |
| AMZN 1wk | 43.7 / 54.0 | **15.73 / 17.50** | 9.0 / 9.0 | 36.4 / 36.4 | 1.47 |
| SPY 30m | **84.2 / 65.6** | 2.00 / 2.01 | 30.7 / 30.7 | 11.7 / 11.7 | **3.60 / 0.83** |
| SPY 1mo | 73.2 / 70.7 | 35.99 / 35.98 | **27.1 / 24.3** | 12.8 / 12.8 | 0.61 |
| CRWD 1wk | 54.7 / 52.4 | **32.15 / 27.78** | 40.9 / 40.9 | 69.4 / 69.4 | 1.11 |
| TSLA 1wk | **60.7 / 46.7** | **31.62 / 35.68** | 11.0 / 11.0 | 48.4 / 48.4 | 1.23 |

(Full 30-row table: `docs/g102/ticker-review-tools/recompute.json`.)

Findings:
- **T1 RSI** is Cutler's simple-average RSI, not Wilder's RSI(14) that charting platforms show; differences reach 26
  points and flip states (AMZN 30m "Oversold" 17.0 vs 43.5; NVDA 30m 71.0 vs 46.2). The chart's RSI pane uses the same
  simple method, so the page agrees with itself but not with any other platform. RSI also feeds the vote and timing.
- **T2 ATR** is a simple 14-bar mean of true range: within about +/-15% of Wilder ATR. It drives stop checks, the ATR
  fallback levels and zone tolerance.
- **T3 Monthly ADX/DI** uses an EWM seeded at the first bar; 60 monthly bars do not converge (ADX 2-7 points high).
  30m/1h/1d/1wk match Wilder exactly.
- **T4 Hourly HV20** annualizes with 252x6.5 bars; hourly bars are 7 per session (09:30 ... 15:30 half bar), so HV is
  about 3.7% low. 30m (252x13) and daily/weekly/monthly are right.
- **T5 Intraday RVOL** compares the last bar with the previous 20 bars of any time of day. After the close the last
  30m/1h bar is 15:30-16:00 (closing auction): it reads 1.9-3.6x while against the same slot it is 0.7-0.9x. The
  "expanding / contracting" 5-vs-20 trend has the same time-of-day bias intraday. Daily/weekly/monthly are fine.
- The "x last completed" caption under RVOL reads as if it were a ratio to the last bar; it is "last bar vs its
  20-bar average".

### 4.3 Volume flow: better presentation (TP11)

Today one cell holds a line sparkline plus four metrics. Proposal: two labelled reads, each with one picture.

```
VOLUME FLOW                                     (1d, completed bars)
Participation  ▂ ▃ ▅ ▂ ▁ ▇ ▃ ▅ ┆ 1×      last bar 0.67× its 20-bar average
               8 bars vs 20-bar average     5 vs 20 bars  -12%  contracting
Pressure       sell ━━━━━━━━┃■■■■━━━━ buy   CMF +0.14  accumulation
```

- Bars (not a line) with a dashed 1x line; the last bar highlighted.
- Pressure as a diverging bar from -1 to +1 (CMF), labelled sell/buy.
- Plain captions; abbreviations explained in the legend (TP7).
- Frontend-only on today's fields. With T5 fixed (TP10) the intraday numbers become meaningful; until then the 30m/1h
  rows carry a "not time-of-day adjusted" note.
- Optional later (backend, additive): up/down volume split of the last 20 bars to color the bars.

### 4.4 Abbreviation legend (TP7)

Hover titles on every abbreviation plus a small collapsible "Abbreviations" block under the matrix and under the
chart legend. Terms: O/H/L/C, Vol, SMA (the chart says "MA 50"; rename to "SMA 50" to match the tiles), EMA, BB
(Bollinger Bands 20, 2 sd), RSI (as currently computed), MACD, ADX, +DI/-DI, ATR, ATR %, HV20, percentile, RVOL, CMF,
VWAP(20), Fib, FVG, R (risk multiple), TF.

## 5. Levels

### 5.1 Inventory (what is shown, where from)

1. **Confirmed price structures** (cards): up to 2 structural patterns on the selected interval + 1 per other
   interval (`setup.structural_patterns`); each card: pattern, neckline status vs live price, confirmation line,
   measured target and distance, invalidation, bars since break; pivots in a hover.
2. **Cross-timeframe zones** (ladder, `multi.confluence_zones` from `equity/ticker_contracts.py`): 7 of 18-40 zones
   chosen in the browser (nearest active, support, resistance, two Fib+volume-pivot zones, then nearest weighted by
   trend and strength). Each row: price or range, distance, role, up to 4 references ("1h 127.2% - 1h Confirmed Fib
   127.2% - 1h ATR +2x"), badges (strength, Fib + volume pivot, every candlestick pattern on any interval overlapping
   the zone), and a note (timeframes, evidence types, qualifier, "+N more references", "no completed-candle
   confirmation"). Click draws the level on the chart.

### 5.2 Findings

- **L1 Volume of text:** 7 rows x 4-6 lines + a structure card grid; references are raw labels.
- **L2 Strength does not discriminate:** of 147 zones within 5% of price, 133 (90%) are CONFLUENCE or STRONG.
- **L3 ATR projections count as evidence** (`family=price_action`): 22 zones exist only because of ATR multiples;
  13 zones change strength without them.
- **L4 Duplicate references:** the same Fibonacci price enters as "127.2%" (Fib Extension) and "Confirmed Fib 127.2%"
  in the same interval (136 cases); they share a family so strength is not changed, but rows list them twice.
- **L5 Wide zones:** greedy clustering plus wide FVG/volume-pivot ranges give 152 zones wider than 2% of price
  (INTC $65.72-$93.00, 34%).
- **L6 Candle "confirmations"** come from any interval's last candlestick pattern overlapping the zone (a 30m doji
  "confirms" a weekly zone).

### 5.3 Proposal (TP12 frontend, TP13 backend)

At a glance: 2-3 levels each side, one line each, with the plan's stop and target marked. On demand: click a row for
its evidence; "Show all N" for the full list.

```
KEY LEVELS  AAPL  $355.63 daily close Nov 11                              [Show all 26]
              Price            Away     ATR    Strength  Why
RESISTANCE R2 $372.10-372.90   +4.7%    1.7    ●●●       Weekly 52-wk high · Daily Fib 161.8%
           R1 $359.10-359.80   +1.0%    0.4    ●●○       Hourly Fib 127.2%                 ◂ plan target
──────────── $355.63 now ────────────────────────────────────────────────────────────────
SUPPORT    S1 $347.80-348.40   -2.1%    0.8    ●●●       Daily EMA 21 · Hourly volume pivot ◂ plan stop
           S2 $339.00          -4.7%    1.7    ●○○       Weekly EMA 8
           S3 $327.40-328.10   -7.9%    2.9    ●●○       Daily SMA 200 · Weekly Fib 38.2%

PATTERNS   Daily double bottom · neckline $340.10 holds · target $372.50 · fails below $318.20
```
(Illustrative prices.) Strength dots: 1 = one source, 2 = two families or two timeframes, 3 = both. "Why" shows the
two strongest references in words (timeframe noun + level), selected interval first.

Backend scoring (TP13): exclude ATR projections from evidence (keep them only as plan fallbacks), collapse same-price
references, cap zone width (0.5 daily ATR or 1% of price; split wider clusters), count a candlestick confirmation only
from the zone's own timeframes on the last completed bar. Changes the zone list on this page only
(`confluence_zones` is ticker-page only), so it needs tests and a copy comparison, not other-page checks.

## 6. Fibonacci and Financials

### 6.1 Fibonacci (TP16)

Structure today: a two-card auto-fit grid ("Confirmed structural leg", "Published provisional move"), each card with
its own label/pill row and free text, then a price-map ladder (116 px price column, then title / meaning / source /
badges). Nothing shares a grid, card heights differ, and numbers sit inside sentences.

Proposal: one table style for both blocks.

```
LEGS              From                    To                      Size     Status
Confirmed         Low  $301.20  Aug 4     High $372.90  Oct 29    23.8%    basis 1d · 252 bars · 5% pivots
Provisional       High $372.90  Oct 29    Low  $340.10  Nov 5     -8.8%    within move · 54% retraced

PRICE MAP   Price      Away     Level                     Role          Also on
            $372.90    +4.9%    Confirmed high (100%)     resistance    1wk
            $360.30    +1.3%    Provisional 61.8%         resistance    1h
            ──── $355.63 now ────
            $352.80    -0.8%    Provisional 50.0%         support       1h 1wk
```
(Illustrative prices.) Fixed column widths, numbers right-aligned with tabular figures, dates in one format.

### 6.2 Financials (TP14, TP15)

Why the columns do not line up: Income statement, Balance sheet and Cash flow are three separate `<table>`s with
`width: 100%` and automatic layout, so each table sizes its label column from its own longest label ("Revenue
growth (YoY)" vs "Cash and short-term investments" vs "Operating cash flow") and the period columns land at
different x positions in each table.

Data bug: `getEquitySecurityProfile` requests `report_limit: 12` across all three timeframes, while the publication
keeps up to 30 (12 quarterly, 6 annual, 12 TTM). AAPL with 12: quarterly Sep-26, Jun-26, Sep-25, Jun-25, Sep-24
(Q4/Q3 only); with 40: nine consecutive quarters. YoY growth then also goes missing for skipped quarters.

Proposal:
- TP14: request `report_limit: 40` (API allows up to 100). Frontend-only, one line.
- TP15: one table, three `<tbody>` sections with a section row each and one period header (sticky); `table-layout:
  fixed` with a fixed label column (about 240 px) and equal period columns; numbers right-aligned with tabular
  figures; a `ratio` format for Current ratio (today a label check inside the percent format); show
  `TRIMMED_TO_WORKSPACE_BOUND` ("showing N of M periods", INV-8; NVDA is trimmed today); treat only ETF security types
  as ETFs (today any type other than `CS` hides statements, which would hide an ADR's).

## 7. Proposed matrix rows (the live session adds them)

| Id | Item | Why | Effort | Risk | Layer | Target |
|---|---|---|---|---|---|---|
| TP1 | Remove the analysis-timeframe selector, pill and pin; plan follows the chart interval; snap note in the plan header; `analysis_interval` URL param dropped | User request | S | Low | Frontend | f30 |
| TP2 | SHORT plan reads the directional brackets (stops above, targets below) | 37/37 short plans are ATR-only and pass automatically | S | Low-med (short verdicts change) | Frontend | f30 |
| TP3 | ~~Stop rule alone~~ folded into TP5: measured, it still leaves 0 "Take" (3.3a) | - | - | - | - | Dropped |
| TP4 | Setup-quality grade counts only bias-side inputs; Neutral has no grade | 108/125 graded A/A+, conflicted setups A+ | S | Med (grade shown on other pages) | Backend | Later, user's call |
| TP5 | Server-side trade plan per interval per publication with inputs recorded; symmetric candidates; weekly/monthly entry on the last daily close; unit tests per rule; copy check | One definition, auditable (F1-F7) | M (1-1.5 d) | Med | Backend + frontend | Next package |
| TP6 | Split inside the raw window: withhold 30m/1h setups or show the label | Mixed-scale intraday setups (F6) | S-M | Low | Backend | Next package |
| TP7 | Abbreviation legend for the Timeframes tab (hover titles + collapsible block); chart legend reverted | User request | S | Low | Frontend | f30 |
| TP8 | RSI -> Wilder RSI(14), ATR -> Wilder ATR(14) (or new fields and switch the ticker page first) | Up to 26 RSI points from the standard | S code, M review | Med (values change on ~10 pages) | Backend | User's call |
| TP9 | Monthly ADX/DI Wilder seeding; hourly HV annualized with 7 bars/day | ADX 2-7 pts high on 1mo; HV 3.7% low on 1h | S | Low | Backend | Next package |
| TP10 | Intraday RVOL and volume trend by time-of-day slot (prior 20 sessions) | 15:30 bar reads 2.8-3.6x vs 0.7-0.9x | S-M | Low-med | Backend | Next package |
| TP11 | Volume flow redesign (participation bars + CMF pressure bar, plain captions) | User request; one cell held five metrics | S-M | Low | Frontend | f30 if time, else next |
| TP12 | Levels "Key levels" table (2-3 per side, plan stop/target marked, expand for evidence, show all) | User: overwhelming | M (0.5-1 d) | Low | Frontend | Next frontend |
| TP13 | Zone scoring: no ATR evidence, collapse duplicates, cap width, own-timeframe candle confirmation | L2-L6 | M | Med (zone list changes, ticker page only) | Backend | Next package |
| TP14 | Financials `report_limit` 12 -> 40 | Non-consecutive quarters | XS | Low | Frontend | f30 |
| TP15 | Financials one aligned table; ratio format; trimmed label; ETF check | User request; INV-8 | S | Low | Frontend | f30 |
| TP16 | ~~Fibonacci legs table + price map on one grid~~ reverted: the original layout matches Levels | - | - | - | Frontend | Dropped |

## 8. Recommended plan

- **f30 (frontend, by ~12:00 UTC):** TP1, TP2, TP7, TP14, TP15, TP16; TP3 and TP11 if the user agrees. All frontend
  only, tested with the type check and node tests; no backend change in r95 from this review.
- **Next package:** TP5 (with TP4 inside it if the user agrees), TP6, TP9, TP10, TP13, then TP12 on top of TP13.
- **User's decision:** TP3 (changes verdicts), TP4 and TP8 (values on other pages change).

## 8a. Options Alerts: IV/Delta/Gamma "Unavailable" and a recommended target (user question, ~08:00 UTC)

Findings (live GETs, dataset 01f250a0, Nov 11 session):
- All 16 O1 alert legs (Latest run 2, Day history 14; DTE 0, 5, 8, 12) carry `local_iv`, `local_delta`,
  `local_gamma`, `time_to_expiration_years`, `quote_bid`, `quote_ask` = null. Not a 0 DTE effect: the alert row and
  plan detail builders in `realtime/advanced_options_projection.py` (~1509-1533 and ~1553-1579) hard-code these to
  None for every package leg; the package itself keeps only `entry_mark`, `entry_market_time` and the policy hash.
- The same module already solves IV and Greeks for contract quotes (`solve_local_greeks` with stored rate and
  dividend inputs, ~330-380); the alert legs never go through it.
- Stop/Target "Not set" is by design: O1 has no management policy (`management_policy: {}`, matrix R33); the page's
  `optionPlanTerms` supports only premium-fraction policies (`stop_loss_fraction`, `take_profit_fraction`).

Proposed rows:
| Id | Item | Effort | Risk | Layer | Target |
|---|---|---|---|---|---|
| OA1 | Solve IV/Delta/Gamma (and theta/vega) for each alert leg at its own source time from the stored entry mark, spot and the session's rate/dividend inputs (point in time, INV-3); a leg the solver cannot value says why (INV-8). Fixes Latest run, Day history and the plan detail together; no frontend change. Check that rate/dividend inputs are retained for past and expired expirations. | M | Med: Options projection path | Backend | Next package, not r95 |
| OA2 | Recommended option target and stop: reprice the option (same IV, time to the alert's exit deadline) at the underlying's next technical level in the alert direction (target) and the level against it (stop); fallback: the implied move to the exit deadline (S x IV x sqrt(t)). Shown as a model estimate with the stock level it assumes, never as a fill. Needs OA1, and the stock levels need TP5 first. Frontend: new fields beside the premium-fraction terms. | M-L | Med | Backend + frontend | After OA1 and TP5 |

### 8b. OA1, OA2 and OF3 implemented (user go ~08:20 UTC; backend + frontend, for the next package, not r95)

Backend (read time, in the API; nothing in the Options window or projection changes):
- New `realtime/option_alert_valuation.py`: OA1 leg IV and Greeks with `solve_local_greeks` from the leg's entry mark,
  the R33 trigger spot and the rate/dividend inputs of the session's `rt.advanced_options_reference_states` row known
  by the leg's mark time (one point-in-time query per read, then cached by evaluation id); reasons
  `SOURCE_SPOT_UNAVAILABLE`, `VALUATION_INPUT_UNAVAILABLE`, `EXPIRED_AT_SOURCE`, `LEG_TERMS_INCOMPLETE` or the solver's.
  OA2 `model_exit` on rows with one bought contract and no management policy: policy `options-alert-model-exit-v1`
  (target 1 sigma, stop 0.5 sigma of the expected move to the exit deadline, valued at the source time with the
  same IV; value if the target is only reached at the exit; breakeven by the exit). Time convention identical to the
  projection's `_time_to_expiration_years` (tested).
- `realtime/portal_compatibility_repository.py`: `_advanced_alert_valuation_reported` after the source facts on the
  detector-runs read, own savepoint, `optional_reads` entry `alert_valuation` (AVAILABLE / FALLBACK
  `ALERT_VALUATION_UNAVAILABLE`); a failure serves the rows unchanged (INV-7, INV-9).
- `realtime/option_flow_breadth.py` (OF3): Flow `top_contracts[].contract_id` is null for a contract outside the
  catalog instead of 0. The Screener's breadth rows keep 0 (the screen engine and its tests use it); a later change.
- Tests: `tests/realtime/test_option_alert_valuation_nov12.py` (12). On real data: the 16 Nov 11 O1 alerts (served
  rows) valued with the copy r92's inputs: 14 lookups, 14 found, every leg has IV/Delta/Gamma (0 DTE IV 15-100%),
  every row a model exit (e.g. NVDA 172.5P at $1.595: target $4.59 if $168.38, stop $0.77 if $175.44, $4.12 if only
  reached at 16:00, breakeven $170.90 = 0.46 sigma). Script `ticker-review-tools/oa_on_copy.py`.
- Note: reward/risk in the model exit is structurally above 1 (convexity, 1 sigma vs 0.5 sigma); it is not a quality
  score and the page does not grade it.

Frontend: `services/api.ts` (`OptionAlertModelExit`, row `model_exit`, Flow `contract_id: number | null`),
`pages/optionsAlertPresentation.ts` (`optionModelExitView`), `pages/OptionsAlertsPage.tsx` (Target/Stop cells for
rows without a rule: model price, the stock level and sigma, the value at the exit / breakeven, "Model estimate /
share / not a fill"; the same cells in the alert detail window). IV/Delta/Gamma need no frontend change. Test:
`tests/optionsAlertPresentation.test.mjs` (+1, and the "Not set" source check updated).

Backend regression (r94 image, offline): the 17 test files that import the changed modules (portal compatibility
repository, Flow breadth, Options alerts, portal API, ticker/screening slices): branch 462 passed / 3 failed, c46f988
462 passed / 3 failed, the same 3 (`test_ticker_vertical_slice.py`: nine-ticker parity, evidence drift [split],
closing picker). Plus the new file: 12 passed.

Deploy: the backend part is API only (no window, schema or projection change); it needs the frontend part to show
the target/stop, and the frontend part shows "Not set" until the API carries `model_exit`. Rehearse on the copy
(Options alerts read of Nov 11) before deploying.

### 8c. One page header on every page (user request, ~09:00 UTC; frontend only)

- `layout/AppShell.tsx`: the header's right side is `HeaderMarketPanel` on every page: "Market data" with the newest
  stored bar time (Eastern) under it, "Mode" with its value under it (Real-Time; "Session close" when a past session
  is selected; a page may name its own, e.g. Stock Alerts in replay reads "Replay"), then the session picker
  (session stepper or the alert-session stepper, unchanged URL logic) or a fixed latest-session date on pages without
  one, and one refresh control (`RefreshButton`): on a past session it returns to the latest (as the All Tickers icon
  did); on the latest it refreshes the portal identity and refetches every query in place, spinning while loading.
- Page `status` items are no longer shown (`pageContext.ts` keeps the field so pages compile; page facts belong in
  the page body). Removed from the header today: Ticker Close, Options "Data" tier, Options Alerts "Read-only /
  Indicative", Dashboard Session/Prices, Screener Session, Market conditions Source session/Market, Persistence
  Window, Sector Market, Strategy Interval/Session, Stock Alerts Data.
- Ticker overview: the close (and day change, "As of" in the hover) is the first chart stat, before Period high
  (`TickerDetail.tsx`); `TickerWorkspace.tsx` no longer reads the quote for the header.
- CSS `layout/AppShell.css` (`.tm-market*`, `.tm-refresh`), `pages/ticker/ticker.css` (close change).
- Market data stamp (user, ~09:30 UTC: "same format on every page, the actual latest timestamp"): each page's session
  list supplied it and they disagree (`/trading-sessions` returns a date only, shown "Wed, Nov 11"; the ticker, All
  Tickers and Pattern Watch lists return a time; strategies 503 pre-market and fall back). The realtime header now
  shows the newest published window's cutoff from `/realtime/portal/current` (Equity's on stock pages, Options' on
  `/options/*`), refreshed by the identity poll every minute, always as `formatMarketStamp` ("Nov 11, 2026, 4:00 PM
  EST"; a date-only value reads as that session's 4:00 PM ET close). Values bold 11px like the earlier stamp.
- Mode rule (user, ~09:50 UTC): "Real-Time" only while the market session runs (`marketSessionOpen`: the current
  publication's session date is today's Eastern date and the clock is 9:30 AM to 4:00 PM ET); "Session close" off
  hours (before the open, evenings, weekends, holidays: the last close is shown) and whenever a past session is
  picked. Fixed a false "past session": links between ticker pages and All Tickers carry `session_date`, and a
  pinned date equal to the newest session counted as past; now only an older date does (also for the refresh icon).
  Known gap: an early-close day (1:00 PM) reads Real-Time until 4:00 PM.
- Ticker close stat: price and change coloured by the day's change, left-aligned under its label, set apart from the
  period stats by a divider.
- Test: `tests/pageHeaderNov12.test.mjs` (5). Full run: type check clean; 316 tests, 271 pass, 40 fail (same 40).

### 8d. Options ticker links (user request, ~10:10 UTC; frontend only)

- A ticker on the Options Screener results and on Options Alerts (alert table and evaluations table) opens its
  Options Flow (`/options/flow?underlyer=X`, helper `optionFlowPath` in `optionsAlertPresentation.ts`). The Screener
  ticker opened the stock ticker page before; the Alerts ticker opened the alert's details, which stay on the
  always-shown View (eye) column. Options Flow opens on its latest session (its own picker steps back).
- Files: `pages/OptionsScreenLibrary.tsx`, `pages/OptionsAlertsPage.tsx`, `pages/OptionsAlertsPage.css`
  (`.oa-row-link` as a link), tests `optionsScreenLibrary.test.mjs`, `optionsScreenerRealtime.test.mjs` (old link
  assertions updated), `pageHeaderNov12.test.mjs` (+1). Full run: 317 tests, 272 pass, 40 fail (same 40).
- "Options Screener not loading" was the local preview only: its proxy passed GET requests only, and the Screener
  reads its results with a POST query (`/options/eligible-chain/query`, a read in a READ ONLY transaction). The
  preview now also passes that and the three Stock Screener POST reads; the live API answers the query normally
  (18,510 contracts at ~10:05 UTC).

## 9. Hand-back (Step 2, Nov 12 ~06:40 UTC)

Branch `ticker-page-review`, worktree `StockScreener.worktrees\ticker-review`, uncommitted (commit on the user's
word). **Frontend only**; no backend file changed, so r95 gets nothing from this branch.

Done: TP1, TP2, TP7 (Timeframes legend and hover titles only), TP11, TP14, TP15 (plus the period buttons, which
used the Options Flow stylesheet's `flow-segmented` class and were unstyled on the ticker page). Reverted on the
user's review (~07:15 UTC): TP16 (the original Fibonacci cards and ladder match the Levels display) and the chart
abbreviation legend with the "MA" -> "SMA" relabel. Not done: TP3 (folded into TP5, see 3.3a), TP12, all backend
rows.

Files:
- `frontend/src/pages/TickerDetail.tsx`: selector bar, pill and pin removed (TP1); stale notice in the panel body;
  snap note in the plan header; Volume flow cell (TP11); RSI/MACD hover titles and the Timeframes legend (TP7).
- `frontend/src/pages/ticker/tickerPlan.ts`: SHORT reads levels by side of the entry (TP2).
- `frontend/src/pages/ticker/chartPreferences.ts`: `analysisPin` removed; `analysis_interval` dropped on write (TP1).
- `frontend/src/pages/ticker/FinancialsView.tsx`, `financialsModel.ts`: one table, `ratio` format, trimmed label,
  fund-type check, period buttons on the ticker page's `tk-segment` control (TP15).
- `frontend/src/services/api.ts`: `FINANCIAL_REPORT_LIMIT = 40` (TP14); profile type gains the trim fields.
- `frontend/src/pages/ticker/tickerShared.tsx`: glossary data; hover titles in the Direction and Volatility cells.
- New: `pages/ticker/VolumeFlow.tsx`, `pages/ticker/tickerGlossary.tsx`.
- `frontend/src/pages/ticker/ticker.css`: analysis bar styles removed; note, legend, volume flow, level table,
  statement table and period-button styles.
- Tests: `tests/tickerPageReviewNov12.test.mjs` (new, 5), `tests/tickerPlan.test.mjs` (+2 short-plan tests),
  `tests/tickerChartPreferences.test.mjs` (pin removed), `tests/tickerWorkspaceRealtime.test.mjs` (report_limit 40).

Tests (f29 build image, source mounted, after the reverts): type check clean; 309 tests, 264 pass, 40 fail. Baseline
on c46f988: 302, 257, 40. The 40 failures are the same tests by name; all 7 new tests pass.

Effect of TP2 on the Nov 11 close setups: 36 of 37 short plans now use at least one structural level (was 0), and
35 of 37 now read "Not tradeable" under the nearest-level stop rule, as 68 of 78 longs already do. TP3 addresses
both sides.

Options Flow page (user request, outside the ticker scope, ~07:45 UTC), proposed rows OF1-OF3, frontend only:
- OF1: "Total open interest" moved next to "Total volume" in the summary tiles.
- OF2: the "N expiries" count beside "Include 0 DTE" / "Show %" had no style rule and took the page's default font;
  it now matches the options beside it (`.flow-expiry-options__count`).
- OF3: "duplicate records" in the high-activity contracts table: the data has no duplicates (every contract ticker
  is unique), but breadth (non-streamed) contracts all carry `contract_id` 0 and the rows were keyed by
  `contract_id` (live GET: SPY 6, QQQ 10, AAPL 6, NVDA 3, TSLA 3 repeated keys of 25 rows). React reuses rows with a
  shared key on re-render and sort, which shows as duplicates. Rows are now keyed by `contract_ticker`. Backend note
  for later: `contract_id` 0 as "no instrument" is a trap for any consumer; null would be safer.
- Files: `frontend/src/pages/OptionsFlowPage.tsx`, `OptionsFlowPage.css`, `tests/optionsFlowDisplayNov12.test.mjs`
  (+1 test). These files came with f29; if the live session edits them too, expect a small merge.
- Tests after OF1-OF3: 310 tests, 265 pass, 40 fail (same 40 by name as the baseline).

Deploy dependency: this branch starts at c46f988, which already carries f29 and its API needs (the developing-candle
route `/stock/{t}/chart/forming` returns 404 on the live API today). So f30 needs the r95 API, which the live session
deploys before 14:00 anyway. The page degrades cleanly without it (no developing candle).

Local preview used for review (not live): Vite dev server in the f29 build image, `http://localhost:5190`, proxy to
the live API for GET/HEAD only (other methods get 404 from the dev server). Container `ticker-review-preview`; stop
it with `docker stop ticker-review-preview`.
