# Stock Screener Portal — Strategies Guide

A comprehensive reference for every trading strategy implemented in the screener, the signals they produce, how to interpret them, and how they combine for higher-conviction decisions.

---

## Table of Contents

1. [Gap Strategies](#1-gap-strategies)
2. [Fair Value Gaps (FVG)](#2-fair-value-gaps-fvg)
3. [Moving Average Crossover](#3-moving-average-crossover)
4. [Momentum Pullback](#4-momentum-pullback)
5. [Bearish Bounce](#5-bearish-bounce)
6. [RSI Screening](#6-rsi-screening)
7. [Fibonacci Retracement](#7-fibonacci-retracement)
8. [Streak Analysis](#8-streak-analysis)
9. [Weekly SMA Integration](#9-weekly-sma-integration)
10. [Markers System](#10-markers-system)
11. [Filters & Presets](#11-filters--presets)
12. [Cross-Strategy Signal Confluence](#12-cross-strategy-signal-confluence)
13. [Practical Playbook](#13-practical-playbook)
14. [Proposed Intraday Scanner Research](#14-proposed-intraday-scanner-research)

---

## 1. Gap Strategies

### What It Detects

The scanner records a gap event when a new session opens significantly above the previous high or below the previous low. It separates formation-time classification from the gap's evolving fill lifecycle. Results are research heuristics and location evidence; they are not qualified probability estimates.

### How Gaps Are Identified

- **Gap Up**: Current open is at least 1% above the previous high.
- **Gap Down**: Current open is at least 1% below the previous low.
- **Formation fills retained**: A gap that returns to the previous close during its formation session is recorded as a same-session fade rather than discarded.
- **ATR filter**: The reported gap zone must be at least 0.5× ATR(14).
- **Intraday boundary rule**: 5m, 15m, 30m, and 1h scans evaluate only the first bar of a new session. Adjacent bars within one session are not treated as opening gaps.
- **Weekly/monthly rule**: Aggregated weekly and monthly candles are not evaluated as session-opening gaps.

### Formation Classification

| Class | Implemented evidence | Interpretation |
|-------|----------------------|----------------|
| Breakaway | Opens beyond the prior 20-bar range with formation volume ≥1.5× the preceding 20-bar average, without an established aligned trend | Potential transition out of a prior range |
| Continuation | Gap direction agrees with the established formation-time 20-bar trend | Potential continuation within an existing trend |
| Exhaustion Watch | Trend-aligned, at least 2 ATR from the 20-bar mean, and formation volume <0.8× average | Extension risk; reversal must still be confirmed by later price action |
| Common | Sideways formation context and no break of the prior 20-bar range | Gap without a strong trend or breakout context |
| Unclassified | Insufficient or conflicting formation evidence | Do not force a directional interpretation |

Classification confidence describes the completeness of rule evidence, not expected profitability.

### Fill Lifecycle

Fill progress is measured from the opening price back to the previous close, matching the conventional gap-fill target.

| Lifecycle | Meaning |
|-----------|---------|
| Open | Price has not moved back into the opening gap |
| Partially Filled | Price moved toward the previous close but has not reached it |
| Filled | Price reached the previous close after the formation session |
| Same-Session Fade | Price reached the previous close during the formation session |
| Failed | Price filled and closed through the previous close against the original gap direction |

### Signal Types

| Signal | When It Fires | What It Means |
|--------|---------------|---------------|
| New Gap Up / Down | A qualifying gap formed on the current scan bar | Inspect class, relative volume, and fill state before forming a thesis |
| Same-Session Fade | The current opening gap reached the previous close in the same session | The opening move did not preserve its gap through the session |
| At Support (Unfilled/Filled Gap Up) | Price is near a surviving or previously crossed gap-up zone | Location evidence for a possible support test; confirmation is required |
| At Resistance (Unfilled/Filled Gap Down) | Price is near a surviving or previously crossed gap-down zone | Location evidence for a possible resistance test; confirmation is required |
| Possible Downside (In Gap Up) | Price is inside a prior gap-up zone | The prior gap is being traversed downward |
| Possible Upside (In Gap Down) | Price is inside a prior gap-down zone | The prior gap is being traversed upward |

### Key Fields

| Field | Description |
|-------|-------------|
| `gap_low` / `gap_high` | Boundaries of the gap zone |
| `gap_pct` | Legacy compatibility alias for `full_gap_pct` |
| `gap_atr_ratio` | Gap size relative to ATR(14) — higher = more significant |
| `gap_date` | When the gap formed |
| `trend` | 50/200 SMA trend context (Bullish / Bearish / Neutral-Bullish / Neutral-Bearish) |
| `entry_direction` | For inside-gap signals: did price enter via rally (from below) or drop (from above) |
| `gap_classification` | Breakaway / Continuation / Exhaustion Watch / Common / Unclassified |
| `classification_reason_codes` | Observable rules that led to the classification |
| `gap_lifecycle` / `fill_pct` | Current fill state and progress toward the previous close |
| `opening_gap_pct` | Open versus the previous close |
| `full_gap_pct` | Open beyond the previous high or low |
| `formation_relative_volume` | Formation volume versus the preceding 20-bar average |
| `gap_age_sessions` | Number of sessions since formation |

### How Gaps Help You Trade

- Use gap class to separate breakout, continuation, extension-risk, and context-poor events.
- Use lifecycle and fill percentage to distinguish a held opening move from a partial fill, completed fill, or failure.
- Treat support and resistance rows as locations to monitor, not unconditional entries.
- Require independent confirmation such as a held/rejected retest, aligned trend, and observed participation.
- Validate each class with forward fill rate, time-to-fill, MFE, MAE, and return studies before promoting it to qualified directional evidence.

### UI: Gap Screener

- **5 tabs**: New Gaps, Support Zones, Resistance Zones, Fills & Fades, and Fair Value Gaps
- **Classification filter**: Breakaway, Continuation, Exhaustion Watch, Common, or Unclassified
- **Age filter**: Defaults to gaps formed within 20 sessions, with 5/60/252-session and all-age options
- **Evidence columns**: Classification, lifecycle, fill percentage, age, and formation relative volume
- **Status badges**: Zone type and proximity (At Edge / Testing / Approaching / Broken)
- **Expandable rows**: Click a ticker to see all its active gaps, not just the primary one
- **Cross-tab ticker search**: Type a ticker and the UI automatically switches to the tab containing it

---

## 2. Fair Value Gaps (FVG)

### What It Detects

Fair Value Gaps (FVGs) are **3-candle imbalance zones** where price moved so aggressively that it left a gap between candle 1 and candle 3 — an area where no two-way trading occurred. These zones represent **institutional order flow**: large buyers or sellers pushed price so hard that the market couldn't fill orders at those levels. Price tends to return to these zones to "mitigate" the imbalance, making them powerful support/resistance levels.

FVGs are a core concept in **ICT (Inner Circle Trader)** methodology and are particularly valuable for intraday trading on 5m and 15m timeframes.

### How FVGs Are Identified

- **Bullish FVG**: `candle[i].low > candle[i-2].high` — a gap between candle 1's high and candle 3's low. This is a **demand zone** (support) — price should come back here to fill buy orders.
- **Bearish FVG**: `candle[i].high < candle[i-2].low` — a gap between candle 1's low and candle 3's high. This is a **supply zone** (resistance) — price should return here to fill sell orders.
- **ATR Filter**: FVG size must be ≥ 0.3× ATR(14) to filter out insignificant micro-gaps.
- **Lookback**: Scans the most recent N candles (default 50). Adjustable via the `lookback` parameter.

### Signal Types

| Signal | When It Fires | What It Means |
|--------|---------------|---------------|
| Bullish FVG — Unmitigated | Demand zone has never been revisited | **Strongest support** — pristine institutional buy zone. Price will likely return here. |
| Bullish FVG — Partially Mitigated | Price touched the top of the zone but didn't reach the midpoint | Some orders filled but demand likely remains below midpoint. |
| Bullish FVG — Mitigated | Price returned through the midpoint of the zone | Demand zone mostly filled — weaker support on retest. |
| Bearish FVG — Unmitigated | Supply zone has never been revisited | **Strongest resistance** — pristine institutional sell zone. |
| Bearish FVG — Partially Mitigated | Price touched the bottom of the zone but didn't reach midpoint | Some sell orders filled but supply likely remains above midpoint. |
| Bearish FVG — Mitigated | Price returned through the midpoint of the zone | Supply zone mostly filled — weaker resistance. |

### Key Fields

| Field | Description |
|-------|-------------|
| `fvg_type` | Bullish FVG or Bearish FVG |
| `status` | Unmitigated / Partially Mitigated / Mitigated |
| `fvg_low` / `fvg_high` | Boundaries of the FVG zone |
| `fvg_size` | Zone width in dollars |
| `fvg_pct` | Zone width as percentage of price |
| `atr_ratio` | FVG size relative to ATR(14) — higher = more significant move |
| `proximity` | Where last close sits relative to the FVG zone: Inside / Near (<2%) / Away |
| `trend` | 50/200 SMA context: Bullish / Bearish / Neutral-Bullish / Neutral-Bearish |
| `trend_aligned` | Whether the FVG direction matches the trend (e.g., Bullish FVG in an uptrend) |
| `gap_date` | When the FVG formed |
| `streak_count` | Consecutive same-direction FVGs from the most recent — measures market structure bias |
| `streak_direction` | Direction of the streak (Bullish or Bearish) |
| `bull_unmitigated` | Total unmitigated bullish FVGs for this ticker |
| `bear_unmitigated` | Total unmitigated bearish FVGs for this ticker |
| `total_fvgs` | Total FVGs detected within the lookback window |

### Mitigation Mechanics

Mitigation is how FVG zones get "filled" as price returns to them:

```
Bullish FVG Zone:  fvg_high ─────────┐
                          │  ZONE  │
                   fvg_low ─────────┘
                          │midpoint│

Unmitigated:     Post-FVG lows never entered zone
Partially Mitig: Post-FVG low reached above midpoint
Mitigated:       Post-FVG low reached midpoint or below
```

- **Unmitigated** = pristine zone, highest probability of reaction
- **Partially Mitigated** = some orders filled, still worth watching
- **Mitigated** = most orders filled, weaker but can still cause reactions on retest

### Streak Analysis: Market Structure Bias

Streak counts consecutive same-direction FVGs from the most recent. This reveals **market structure bias**:

| Streak Pattern | What It Means |
|----------------|---------------|
| ▲ 4x (4 consecutive bullish FVGs) | Sustained buying pressure — strong bullish market structure |
| ▼ 5x (5 consecutive bearish FVGs) | Relentless selling — strong bearish market structure |
| ▲ 1x (no streak) | Mixed environment — no clear directional bias |
| High streak + trend aligned | Highest conviction — structure and trend agree |
| High streak + trend misaligned | Potential trend reversal developing — early warning |

### How FVGs Help You Trade

- **Buy at unmitigated bullish FVG support** with a stop below the zone. The demand zone acts as a floor where institutional buyers left unfilled orders.
- **Short at unmitigated bearish FVG resistance** with a stop above the zone. The supply zone is a ceiling of unfilled sell orders.
- **Trend-aligned FVGs** are highest conviction: a bullish FVG in an uptrend means institutions are buying the dip. A bearish FVG in a downtrend means institutions are selling the rip.
- **FVG size matters**: Higher `atr_ratio` (≥1.0) = more aggressive institutional move = stronger zone.
- **Proximity "Near" or "Inside"** = immediate setup. "Away" = add to watchlist.
- **Use intraday timeframes** (5m, 15m) for precise entry within a daily-timeframe context.
- **Multiple unmitigated FVGs** at similar price levels create a **zone cluster** — extremely strong support/resistance.

### Preset Filters

Three composite preset buttons combine direction, status, streak, and trend filters for common trading scenarios:

| Preset | Filters Applied | Use Case |
|--------|----------------|----------|
| **🟢 High-Prob Bullish** | Direction = Bullish, Status = Unmitigated, Trend-aligned only, Streak ≥ 2 | **Buy the dip** — pristine demand zones in confirmed uptrends with structural bias |
| **🔴 High-Prob Bearish** | Direction = Bearish, Status = Unmitigated, Trend-aligned only, Streak ≥ 2 | **Short/sell the rip** — pristine supply zones in confirmed downtrends with structural bias |
| **📊 Streak Signals** | Streak ≥ 3, any direction, any status | **Market structure** — high-streak tickers revealing strong directional bias regardless of mitigation |

Presets override the individual direction/status filters. Click an active preset again to deactivate it and return to manual filtering.

### How It Complements Gap Strategies

Traditional gaps (Section 1) and FVGs detect related but distinct phenomena:

| Dimension | Traditional Gaps | Fair Value Gaps |
|-----------|------------------|-----------------|
| Detection | Open vs prior high/low | 3-candle imbalance (candle 1 high vs candle 3 low) |
| Formation | Opening price jumps | Can form mid-session on any 3 consecutive candles |
| Frequency | Few per day | Many per session — higher signal density |
| Best Timeframe | 1d (daily) | 5m, 15m (intraday) |
| Methodology | Price action | ICT / Smart Money |
| Both share | Act as support/resistance zones, track fill/mitigation status |

Best practice: Use **daily gap zones** for the big picture (where are the major levels?) and **intraday FVGs** for precise entry timing within those zones.

### UI: Fair Value Gaps Tab

- **4th tab** on the Gap Strategies page (purple accent)
- **Direction filter**: All / ▲ Bullish / ▼ Bearish
- **Status filter**: All / Unmitigated / Partial / Mitigated
- **Preset buttons**: High-Prob Bullish / High-Prob Bearish / Streak Signals
- **Expandable ticker rows**: Click to see all FVGs for that ticker, not just the most recent
- **Streak badge**: Shows `▲ 4x` or `▼ 3x` for streaks ≥ 2
- **Trend-aligned badge**: Purple "Aligned" when FVG direction matches MA trend
- **11 columns**: Ticker, Type/Status, Streak, FVGs (count), Date, FVG Zone, Close, Size $, Size %, Proximity, Trend
- **Sorted by**: Streak count (descending), then most recent date

---

## 3. Moving Average Crossover

### What It Detects

SMA (Simple Moving Average) crossover signals on configurable short/long periods, enriched with weekly timeframe data and structural market markers. This is the most feature-rich screener in the portal.

### How Crossovers Work

Two moving averages are computed — a fast one (short period, default 9) and a slow one (long period, default 21). When the fast crosses above the slow, momentum is shifting bullish. When it crosses below, momentum shifts bearish.

### Signal Types

| Signal | When It Fires | What It Means |
|--------|---------------|---------------|
| Bullish Crossover | Short SMA crossed above long SMA **today** | Fresh bullish momentum shift — earliest entry signal. |
| Bearish Crossover | Short SMA crossed below long SMA **today** | Fresh bearish momentum shift — earliest exit/short signal. |
| Recent Bullish | Bullish crossover occurred within last 5 trading days | Still actionable — you didn't miss the move entirely. |
| Recent Bearish | Bearish crossover within last 5 days | Still actionable for shorts/exits. |
| Above MA | Short SMA above long SMA for >5 days | Established bullish trend — ride it, don't chase entries. |
| Below MA | Short SMA below long SMA for >5 days | Established bearish trend — stay out or short. |

### Weekly SMA Signals

The scanner also computes **true weekly SMAs** by resampling daily closes to weekly candles (Friday close). This gives a higher-timeframe perspective:

| Weekly Signal | When It Fires | What It Means |
|---------------|---------------|---------------|
| W-Bullish Cross | Weekly short SMA crossed above weekly long SMA | Major trend shift — rare and powerful. |
| W-Bearish Cross | Weekly short SMA crossed below weekly long SMA | Major downtrend signal. |
| W-Above | Weekly short SMA above weekly long SMA | Macro uptrend intact. |
| W-Below | Weekly short SMA below weekly long SMA | Macro downtrend intact. |

### Key Data Points

| Field | Description |
|-------|-------------|
| `short_ma` / `long_ma` | Current values of the configured daily SMAs |
| `ma_spread_pct` | Percentage gap between short and long MA — wider = stronger trend |
| `days_since_cross` | How many days since the most recent crossover |
| `crossover_date` | Date of the most recent crossover |
| `price_change_since_cross_pct` | Total price move since the crossover — shows if you're late |
| `weekly_short_ma` / `weekly_long_ma` | True weekly SMA values |
| `weekly_spread_pct` | Weekly MA spread percentage |
| `weekly_signal` | Weekly crossover signal (W-Above, W-Below, W-Bullish Cross, W-Bearish Cross) |
| `markers[]` | Structural markers (Golden Cross, Death Cross, etc.) |

### How MA Crossovers Help You Trade

- **Fresh crossovers** (0–3 days) are the highest-reward entries — you're early.
- **Recent crossovers** (3–5 days) are still good if `price_change_since_cross_pct` is small.
- **Established trends** (Above/Below MA) are for position management — trail stops, don't add new positions.
- **Spread widening** = trend acceleration (stay in). **Spread narrowing** = trend exhaustion (tighten stops).
- **Daily–weekly alignment** (e.g., daily bullish + W-Above) = highest conviction setups.
- **Counter-trend** signals (daily bullish but W-Below) are risky — the weekly trend often wins.

### UI: MA Screener

- **4 tabs**: Bullish Crossover, Bearish Crossover, Bullish Trend, Bearish Trend
- **14 columns**: Ticker, Signal, Markers, Last Close, Short MA, Long MA, Spread %, Days, Cross Date, Since Cross %, W-Short, W-Long, W-Spread %, W-Signal
- **Inline parameters**: Short/Long period inputs in the header bar (no separate card)
- **Cross-tab mode**: When filters are active, all tabs merge into a unified "All Tabs" view

---

## 4. Momentum Pullback

### What It Detects

Stocks in **strong uptrends** that are experiencing a temporary pullback to optimal buy-zone levels. The strategy identifies the "rubber band stretch" — price pulling back toward its moving average in a rising trend, creating a high-probability long entry.

### The 3-Pillar Methodology

All three pillars must pass for a stock to appear in results:

Daily calculations use the latest 210 completed bars, hourly calculations use 200 bars, and
intraday calculations use 100 bars. Fixed windows keep a historical signal reproducible when
older data is later added.

#### Pillar 1: Trend Anchor (Must Pass)

Confirms the stock is in a genuine uptrend, not a random bounce.

- **Daily EMA Stack**: EMA 8 > 21 > 34 > 55 > 89 (at least 2 of 4 pairs aligned)
- **Weekly EMA**: 8 > 21 > 34, OR price above SMA 200
- A stock without trend anchor is **not in an uptrend** — no point looking for pullback entries.

#### Pillar 2: Pullback Zone (Must Pass)

Confirms the stock is actually pulling back (not rallying) and is in a buyable range.

- **Slow Stochastic %K < 40**: Oversold within the uptrend — momentum has cooled
- **ADX 15–55**: Trend is healthy (too low = no trend, too high = blow-off)
- **Price within 2× ATR of EMA 21**: The "rubber band" hasn't snapped — still connected to the trend
- **RSI 30–60**: Not panic-selling but not overbought either

#### Pillar 3: Entry Quality Score (Composite Grade)

Ranks how good the pullback entry is on a 0–100 scale:

| Component | Weight | What It Measures |
|-----------|--------|------------------|
| Stochastic depth | 30% | Deeper oversold = better entry |
| EMA 21 proximity | 25% | Closer to EMA 21 = tighter risk |
| Stack alignment | 20% | More EMAs stacked = stronger trend |
| Relative volume | 15% | Volume pickup = institutional interest |
| ADX sweet spot (30–40) | 10% | Optimal trend strength |

**Grades**: A+ (≥90), A (≥80), B+ (≥70), B (≥60), C (<60)

### How Momentum Pullback Helps You Trade

- **A+ grades** are elite entries — everything lines up. Trade with confidence and size.
- **A/B+ grades** are solid — perhaps one factor is slightly off. Standard position size.
- **B/C grades** are worth monitoring — add to watchlist, wait for improvement.
- **Stochastic depth** tells you how much the rubber band has stretched. Deeper = more snap-back potential.
- **Dist to EMA21 %** tells you how far price has pulled back. You want negative values (price below EMA21) but not too far (rubber band snaps).
- **ADX** in the 30–40 sweet spot means the trend is Goldilocks — not too weak, not blow-off.

### UI: Momentum Pullback Page

- **Methodology card**: Visual 3-pillar display explaining the strategy
- **Grade filter**: A+ / A / B+ / B / C dropdown
- **9 columns**: Ticker, Grade (with score), Close, Volume (with rel. volume badge), RSI, >200 SMA (check + value), Stoch %K, ADX, Dist EMA21 %
- **Color-coded grades**: A+ = green, A = light green, B+ = orange, B = yellow, C = gray

---

## 5. Bearish Bounce

### What It Detects

The mirror image of Momentum Pullback: stocks in **confirmed downtrends** bouncing up toward resistance levels — identifying optimal short-entry or exit-long opportunities.

### The 3-Pillar Methodology (Inverted)

Bearish Bounce uses the same fixed 210/200/100-bar daily/hourly/intraday calculation windows as
Momentum Pullback.

#### Pillar 1: Trend Anchor (Inverted)

- **Daily EMA Stack**: EMA 89 > 55 > 34 > 21 > 8 (inverted — downtrend stack)
- **Weekly EMA**: 34 > 21 > 8 (inverted), OR price below SMA 200

#### Pillar 2: Bounce Zone (Inverted)

- **Slow Stochastic %K > 60**: Overbought within a downtrend — the bounce has run
- **ADX 15–55**: Downtrend is still active
- **Price within 2× ATR of EMA 21**: Bounce hasn't disconnected from the downtrend
- **RSI 40–70**: Bounced but not in a new uptrend

#### Pillar 3: Entry Quality Score (Inverted)

Same component weights, but logic is flipped — higher stochastic = better short (more overbought), positive distance above EMA21 = better short entry.

### How Bearish Bounce Helps You Trade

- **A+ grades** are elite short entries — deeply confirmed downtrend + overextended bounce.
- Use as an exit signal if you're long a stock that appears here — the downtrend is real.
- **Counter-trade caution**: Don't short an A+ Bearish Bounce if the weekly MAs are bullish on the MA screener — the bounce could be a genuine trend reversal.

### UI: Bearish Bounce Page

- Same layout as Momentum Pullback with inverted color scheme
- **<200 SMA** column instead of >200 SMA
- Grades colored in red/dark scheme (bearish palette)

---

## 6. RSI Screening

### What It Detects

Stocks at RSI extremes — either oversold (below threshold, potential bounce) or overbought (above threshold, potential pullback).

### Parameters

| Parameter | Default | Range | Purpose |
|-----------|---------|-------|---------|
| RSI Period | 14 | Configurable | Lookback for RSI computation |
| Oversold Threshold | 30 | Configurable | RSI below this = oversold |
| Overbought Threshold | 70 | Configurable | RSI above this = overbought |

### Signals

| Signal | Rule | Interpretation |
|--------|------|----------------|
| Oversold | RSI < oversold threshold | Selling exhaustion — potential bounce candidate. Combine with trend context. |
| Overbought | RSI > overbought threshold | Buying exhaustion — potential reversal or pullback. |

### How RSI Helps You Trade

- **RSI alone is not a buy/sell signal**. Always combine with trend:
  - RSI oversold + uptrend (200 SMA rising) = buy the dip
  - RSI oversold + downtrend = falling knife, avoid
  - RSI overbought + uptrend = momentum, may continue higher
  - RSI overbought + downtrend = short candidate
- Use RSI as a **confirmation tool**, not a primary trigger.

### UI: RSI Screener

- **Summary cards**: Total scanned, Oversold count (green), Overbought count (red)
- **Signal filter dropdown**: All / Oversold / Overbought
- **5 columns**: Ticker, Signal (badge), RSI, Last Close, Date

---

## 7. Fibonacci Retracement

### What It Detects

After a significant price swing (up or down), stocks tend to **retrace** (pull back) to predictable levels before continuing. Fibonacci retracement identifies these levels using ratios derived from the Fibonacci sequence. When price approaches a Fibonacci level, it often finds support (in uptrends) or resistance (in downtrends) — creating high-probability entry and exit points.

### The Fibonacci Levels

| Level | Ratio | Role |
|-------|-------|------|
| 23.6% | Shallow retracement | Minor pullback — strong trend barely pauses. Fast-moving momentum stocks retrace here. |
| 38.2% | Moderate retracement | Healthy pullback in a strong trend — the most common institutional buy zone. |
| 50.0% | Half retracement | Not a true Fibonacci number, but universally watched. Neutral zone — could continue either way. |
| 61.8% | Deep retracement | The "Golden Ratio" — last line of defense before the trend is questioned. Many reversals happen here. |
| 78.6% | Very deep retracement | If this level breaks, the original trend is likely over. Last-chance entry or early reversal signal. |

### Swing Detection: Zigzag Pivot Algorithm (Option B)

Unlike simple lookback-based methods, the scanner uses a **zigzag pivot algorithm** for more accurate swing identification:

1. **Minimum swing threshold**: A swing high requires price to subsequently drop by at least `min_swing_pct` (default 5%). A swing low requires price to subsequently rise by the same threshold.
2. **Alternating pivots**: Swings must alternate — high, low, high, low — eliminating false signals from noise.
3. **Most recent completed swing**: Fibonacci levels are computed between the last confirmed swing high and swing low.
4. **Swing direction detection**: Determines whether we're in an **uptrend retracement** (swing low → swing high, now pulling back) or a **downtrend retracement** (swing high → swing low, now bouncing).

This approach filters out noise and finds **meaningful** price swings that institutional traders actually react to.

### Choosing the Right Min Swing %

The min swing threshold controls which swings the zigzag algorithm detects. Different thresholds find different timeframes of price structure:

| Min Swing % | What It Catches | Trade Horizon | Best For |
|-------------|----------------|---------------|----------|
| **3–5%** | Small, recent swings | Days to 1–2 weeks | Day trading / short-term swing levels |
| **5–8%** | Medium institutional swings | 1–4 weeks | Standard swing entries (default = 5%) |
| **8–12%** | Major trend pivots | Weeks to months | Position trades, core portfolio entries |
| **12–15%** | Massive multi-month swings | Months | Long-term structural support/resistance |

**Quick-start presets** are available in the UI header: Short-term (3%), Standard (5%), Major (8%), Structural (12%).

**Tips**:
- Start with **Standard (5%)** to get the most broadly useful levels.
- If results show swing dates too far in the past, **decrease** the threshold to find more recent swings.
- If results show too many noisy, small swings, **increase** the threshold to find only meaningful moves.
- Larger swings (higher %) produce more reliable Fibonacci levels because they represent genuine institutional activity.
- For a complete picture, scan at **two thresholds** (e.g., 5% + 10%) — the short-term levels tell you where the current swing trades, the long-term levels show the structural floor/ceiling.

### How Fibonacci Levels Are Computed

**Uptrend retracement** (most recent swing: low → high, price pulling back):
```
Level = Swing High - (Swing High - Swing Low) × Ratio
```
Example: Low = $100, High = $150
- 23.6% = $150 - ($50 × 0.236) = $138.20
- 38.2% = $150 - ($50 × 0.382) = $130.90
- 50.0% = $150 - ($50 × 0.500) = $125.00
- 61.8% = $150 - ($50 × 0.618) = $119.10
- 78.6% = $150 - ($50 × 0.786) = $110.70

**Downtrend retracement** (most recent swing: high → low, price bouncing):
```
Level = Swing Low + (Swing High - Swing Low) × Ratio
```

### Signal Types

| Signal | When It Fires | What It Means |
|--------|---------------|---------------|
| Near Fib 23.6% | Price within 1.5% of the 23.6% level | Shallow pullback — strong momentum. If it holds, trend is very healthy. |
| Near Fib 38.2% | Price within 1.5% of the 38.2% level | **Prime entry zone.** Most institutional buying happens here. |
| Near Fib 50.0% | Price within 1.5% of the 50.0% level | Neutral — trend could continue or reverse. Wait for confirmation. |
| Near Fib 61.8% | Price within 1.5% of the 61.8% level | **Golden ratio support/resistance.** Last strong level before trend failure. |
| Near Fib 78.6% | Price within 1.5% of the 78.6% level | Deep retracement — high risk but high reward if it holds. |
| Between Levels | Price between two Fibonacci levels | In transit — no immediate edge. |
| Below All Levels | Price below 78.6% retracement | Trend has likely failed — original move fully retraced. |

### Key Data Points

| Field | Description |
|-------|-------------|
| `swing_high` / `swing_low` | The identified pivot points defining the Fibonacci range |
| `swing_high_date` / `swing_low_date` | When the pivots occurred |
| `trend_direction` | `uptrend_retracement` (pulling back from a rally) or `downtrend_retracement` (bouncing from a decline) |
| `fib_236` through `fib_786` | The five computed Fibonacci price levels |
| `nearest_level` | Which Fibonacci ratio is closest to current price (e.g., "38.2%") |
| `distance_pct` | How far price is from the nearest level (%) |
| `swing_size_pct` | Total swing magnitude as a percentage — larger swings = more reliable Fibonacci levels |
| `retracement_pct` | How much of the swing has been retraced so far (0% = at the end, 100% = fully retraced) |
| `zone` | Which two Fibonacci levels the price currently sits between (e.g., "38.2% – 50.0%"), or "Below 78.6%" / "Above 23.6%" if outside |
| `support_fibs[]` | 5 Fibonacci support levels computed from the swing high (where price may find support during a decline) |
| `resistance_fibs[]` | 5 Fibonacci resistance levels computed from the swing low (where price may face resistance during a rally) |
| `nearest_support` | Closest support Fibonacci level to current price + distance % |
| `nearest_resistance` | Closest resistance Fibonacci level to current price + distance % |
| `support_targets[]` | Up to 3 support fib levels **below** current price — potential floors if price declines |
| `resistance_targets[]` | Up to 3 resistance fib levels **above** current price — potential ceilings if price rallies |
| `upside_extensions[]` | 127.2% and 161.8% extension prices **above** swing high — upside breakout targets |
| `downside_extensions[]` | 127.2% and 161.8% extension prices **below** swing low — downside breakdown targets |

### Dual-Direction Fibonacci: Support & Resistance

Unlike single-direction systems that only show levels for one scenario, this scanner computes **both support AND resistance** Fibonacci levels simultaneously from the same swing, giving a complete picture:

#### How Both Sets Are Computed

For a swing with High=$200 and Low=$150 (range=$50):

**Support Levels** (from Swing High — where price may find floor during decline):
| Level | Formula | Price |
|-------|---------|-------|
| S 23.6% | $200 - $50 × 0.236 | $188.20 |
| S 38.2% | $200 - $50 × 0.382 | $180.90 |
| S 50.0% | $200 - $50 × 0.500 | $175.00 |
| S 61.8% | $200 - $50 × 0.618 | $169.10 |
| S 78.6% | $200 - $50 × 0.786 | $160.70 |

**Resistance Levels** (from Swing Low — where price may hit ceiling during rally):
| Level | Formula | Price |
|-------|---------|-------|
| R 23.6% | $150 + $50 × 0.236 | $161.80 |
| R 38.2% | $150 + $50 × 0.382 | $169.10 |
| R 50.0% | $150 + $50 × 0.500 | $175.00 |
| R 61.8% | $150 + $50 × 0.618 | $180.90 |
| R 78.6% | $150 + $50 × 0.786 | $189.30 |

**Key insight**: S 38.2% ($180.90) = R 61.8% ($180.90), and S 50% = R 50% = $175. These overlapping levels carry extra significance.

#### Zone

The zone tells you exactly where price sits in the Fibonacci grid — between which two levels. Examples:
- **"38.2% – 50.0%"** → Price is between the 38.2% and 50.0% retracement levels
- **"Below 78.6%"** → Price has retraced past all Fibonacci levels (trend likely failed)
- **"Above 23.6%"** → Price is above all retracement levels (minimal pullback)

#### Support Targets (If Price Declines)

Support fib levels below current price, sorted nearest-first. These are the floors where price may find support.

For price at $172: **S 61.8% ($169.10, -1.7%) → S 78.6% ($160.70, -6.6%)**

#### Resistance Targets (If Price Rallies)

Resistance fib levels above current price, sorted nearest-first. These are the ceilings where price may face selling pressure.

For price at $172: **R 50.0% ($175.00, +1.7%) → R 61.8% ($180.90, +5.2%) → R 78.6% ($189.30, +10.1%)**

#### Fibonacci Extensions (Both Directions)

Extensions project targets beyond the original swing extremes:
- **Upside extensions** (above swing high): 127.2% and 161.8% — rally breakout targets
- **Downside extensions** (below swing low): 127.2% and 161.8% — breakdown targets

#### Practical Example — Stock at $172 (Swing High $200, Swing Low $150)

| Direction | What You See | Decision |
|-----------|--------------|----------|
| **Support below** | S 61.8% at $169 (-1.7%), S 78.6% at $161 (-6.6%) | If price drops, expect support near $169 (golden ratio). Stop below $161. |
| **Resistance above** | R 50% at $175 (+1.7%), R 61.8% at $181 (+5.2%) | If price rallies, expect resistance at $175. Break above $181 = strong bullish. |
| **Upside extension** | 127.2% at $214, 161.8% at $231 | If price breaks above $200 swing high, target $214 then $231. |
| **Downside extension** | 127.2% at $136, 161.8% at $119 | If price breaks below $150 swing low, target $136 then $119. |

### How Fibonacci Helps You Trade

- **38.2% is the bread-and-butter level** — if price pulls back to 38.2% in a strong uptrend, it's the most common institutional buy zone.
- **61.8% is the "make or break" level** — if it holds, expect a strong reversal. If it breaks, the trend is likely over.
- **Uptrend retracements** to Fibonacci support = long entries with stops below the next Fibonacci level.
- **Downtrend retracements** to Fibonacci resistance = short entries with stops above the next Fibonacci level.
- **Swing size matters** — Fibonacci levels from a 20%+ swing are far more reliable than from a 5% swing.
- **Confluence is king** — when a Fibonacci level aligns with a gap zone, SMA, or prior support/resistance, the level becomes very powerful.

### How It Complements Other Strategies

| Combination | What It Tells You |
|-------------|-------------------|
| **MA Bullish Cross + Near Fib 38.2%** | Momentum shifting bullish right at institutional support — high-quality entry |
| **Gap Support + Fibonacci Level** | Two independent support methods agree — doubly strong floor |
| **Momentum Pullback A+ + Fib 38.2–50%** | EMA pullback zone coincides with Fibonacci — validated pullback depth |
| **RSI Oversold + Fib 61.8%** | Selling exhaustion at the golden ratio — high-probability bounce |
| **Below Fib 78.6% + Death Cross** | Fibonacci failed + MA structure broke — strong bearish confirmation |

### Practical Workflow

1. **Scan** — Open the Fibonacci Screener. Filter by trend direction (uptrend retracement for longs, downtrend for shorts).
2. **Identify levels** — Focus on tickers showing "Near Fib 38.2%" or "Near Fib 61.8%" — these are the highest-probability reaction zones.
3. **Expand the row** — Check the 3-panel detail:
   - **Support panel**: Where is the nearest floor if price drops? How far away?
   - **Resistance panel**: Where is the nearest ceiling if price rallies?
   - **Targets & Extensions**: What are the profit targets in both directions?
4. **Check streak** — Open the Streak Panel (bottom of the page) and look at the **Levels** tab:
   - **Locked** tickers (same fib level every day) = price is actively testing the level → highest conviction.
   - **Converging** proximity = test is imminent → get ready to act.
   - **High volume** at the level = institutional interest confirms the zone.
5. **Cross-reference** — Check other screeners for confluence:
   - Is there a **gap zone** at the same price? (Gap Screener → same ticker)
   - Is there a **bullish MA crossover**? (MA Screener → same ticker)
   - Is RSI **oversold** at a support fib? (RSI Screener)
6. **Set entry** — Enter at the fib level with a stop below the next fib level down. Target the nearest resistance fib or extension above.
7. **Monitor via streak** — On subsequent days, use the Fibonacci streak's **Depth** tab to track whether the retracement is recovering (trend resuming → hold) or deepening (move stop or exit).

### UI: Fibonacci Screener

- **Swing presets**: Quick-toggle buttons — Short-term (3%), Standard (5%), Major (8%), Structural (12%) — plus a manual input (3–15%)
- **Signal filter**: All / Near specific levels / Between Levels
- **Trend filter**: All / Uptrend Retracement / Downtrend Retracement
- **Zone filter**: All / specific zone (e.g., "38.2% – 50.0%") — quickly isolate tickers in a particular Fibonacci band
- **Max Distance %**: Only show tickers within N% of the nearest level (e.g., ≤0.5% = right at the level)
- **Retracement % range**: Min–Max inputs to filter by pullback depth (e.g., 30–65% = the prime institutional zone)
- **10 columns**: Expand arrow, Ticker, Signal, Trend, Close, Zone, Nearest Level, Distance %, Retracement %, Swing Size %
- **Color-coded level badges**: Green shades for support, red shades for resistance, gold for the 61.8% golden ratio
- **Expandable detail rows** (click arrow to expand) with 3-panel grid:
  - **🟢 Support Levels (from Swing High)**: All 5 support Fibonacci levels (S 23.6% through S 78.6%) shown high-to-low with prices. Nearest support highlighted green. Levels below current price shown at full opacity (active support); levels above price dimmed (already broken). Shows nearest support level name and distance %.
  - **🔴 Resistance Levels (from Swing Low)**: All 5 resistance Fibonacci levels (R 78.6% through R 23.6%) shown high-to-low with prices. Nearest resistance highlighted red. Levels above current price at full opacity (active resistance); levels below price dimmed. Shows nearest resistance level name and distance %.
  - **🎯 Targets & Extensions**: Support targets below price (green pills with price + %), resistance targets above price (red pills with price + %), upside extensions (127.2%/161.8% above swing high), downside extensions (127.2%/161.8% below swing low) in indigo pills.

---

## 8. Streak Analysis

### What It Is

Streak analysis runs any strategy scanner across the **last N trading days** (2–10) to answer: "How consistently has this ticker been producing signals?" A stock showing gap support for 5 consecutive days is far more significant than one that flashed it once.

### Core Metrics

| Metric | Description |
|--------|-------------|
| `days_matched` | Number of days the ticker produced a signal |
| `total_days` | Total days scanned |
| `consistency` | `days_matched / total_days` — 1.0 = perfect streak |
| `dates_matched[]` | Specific dates where signals appeared |

### Gap Strategy Streak Dimensions

The streak panel provides 6 analytical tabs for gap strategies:

#### Tab 1: Overview (Frequency Grid)

A ticker × date matrix with dots showing which days had signals. Quickly spot perfect streaks (all dots filled) vs. intermittent ones.

#### Tab 2: Freshness

Groups tickers by how old their freshest gap is:

| Group | Gap Age | Trading Implication |
|-------|---------|---------------------|
| Fresh | ≤ 3 days | Most relevant — recently formed gap, price memory strong |
| Aging | 4–15 days | Still relevant but weakening |
| Stale | 15+ days | Gap may have been "forgotten" by the market; lower reliability |

#### Tab 3: Fill Progress

Tracks whether price is moving toward or away from the gap zone:

| Group | Definition | Trading Implication |
|-------|------------|---------------------|
| Converging | Distance to gap decreasing across days | Price approaching zone — imminent test |
| Stable | Distance roughly flat | Holding at current level — watch for catalyst |
| Diverging | Distance increasing | Price moving away — gap less likely to be tested soon |

Includes mini spark charts showing `fill_distances[]` over the streak window.

#### Tab 4: New Gaps

Shows tickers that formed **new gaps** during the streak window — these are the freshest and most actionable zones.

#### Tab 5: Transitions

Tracks how gap status changes day to day. A ticker transitioning from "Approaching" to "At Edge" to "In Gap" tells a developing story.

- **Steady (N)**: Same status every day — stable setup
- **A → B → C**: Evolving status — something is changing

#### Tab 6: Volume

Groups tickers by average volume ratio during the streak:

| Group | Ratio | Implication |
|-------|-------|-------------|
| High Volume | ≥ 1.5× | Institutional interest — gap test is significant |
| Normal Volume | 0.5–1.5× | Average activity |
| Low Volume | < 0.5× | Low conviction — gap test may fail |

### MA Crossover Streak Dimensions

The streak panel provides 6 analytical tabs for MA crossovers:

#### Tab 1: Overview

Same frequency grid as gaps.

#### Tab 2: Direction

Groups tickers by signal consistency:

| Group | Definition | Trading Implication |
|-------|------------|---------------------|
| Bullish | All days showed bullish signals | Confident long setup |
| Bearish | All days showed bearish signals | Confident short/avoid |
| Mixed | Signals flipped directions | Indecision — stay out |

Each row shows a **weekly alignment badge** for higher-timeframe context.

#### Tab 3: Spread

MA spread trend tells you if the trend is strengthening or weakening:

| Spread Trend | Definition | Trading Implication |
|--------------|------------|---------------------|
| Widening | MA spread increasing day over day | Trend accelerating — stay in, trail stops |
| Stable | MA spread roughly flat | Trend cruising — maintain position |
| Narrowing | MA spread decreasing | Trend exhaustion — tighten stops, prepare to exit |

Shows daily and weekly spread with spark charts.

#### Tab 4: Momentum

Price change since crossover evolution:

| Momentum | Definition | Trading Implication |
|----------|------------|---------------------|
| Accelerating | Price gains increasing (positive momentum slope) | Ride the move — add on dips |
| Steady | Consistent gains | Healthy trend — hold |
| Stalling | Gains flattening | Momentum dying — book partial profits |
| Choppy | Erratic changes | No clear edge — reduce position |

#### Tab 5: Signals

Shows **signal type evolution** across the streak window. A ticker going `Bullish Crossover → Recent Bullish → Above MA` tells a healthy progression story.

Includes weekly signal badges for each ticker.

#### Tab 6: Volume

Volume ratio grouping with direction and weekly alignment context, combining volume confirmation with trend direction.

### Fibonacci Streak Dimensions

The streak panel provides 6 analytical tabs for Fibonacci strategy:

#### Tab 1: Overview (Frequency Grid)

Standard ticker × date matrix showing which days had Fibonacci signals (Near level or Below/Above All Levels — excludes "Between Levels" as non-actionable).

#### Tab 2: Levels (Level Consistency)

Groups tickers by how stable their nearest Fibonacci level is across the streak window:

| Group | Definition | Trading Implication |
|-------|------------|---------------------|
| **Locked** | Same fib level every day | Price is testing this level — high probability of resolution |
| **Sticky** | Same level ≥60% of days | Price is hovering near the level — coiling for a move |
| **Drifting** | Different levels across days | Price is moving through levels — no clear test |

Shows dominant level, level flow sequence, and consistency score.

#### Tab 3: Proximity (Distance Trend)

Tracks whether price is converging toward or diverging from the nearest Fibonacci level over the streak window:

| Group | Definition | Trading Implication |
|-------|------------|---------------------|
| **Converging** | Distance shrinking by 30%+ | Level test imminent — prepare for reaction |
| **Hovering** | Distance relatively stable | Price coiling near the level — breakout or bounce coming |
| **Diverging** | Distance growing by 30%+ | Price moving away from the level — missed the test |

#### Tab 4: Depth (Retracement Trend)

Monitors whether the retracement is getting deeper or recovering over time:

| Group | Definition | Trading Implication |
|-------|------------|---------------------|
| **Deepening** | Retracement % increasing by 5%+ | More risk but better entry — watch for hold at next level |
| **Stable** | Retracement % roughly flat | Consolidation — wait for direction confirmation |
| **Recovering** | Retracement % decreasing by 5%+ | Price rebounding — trend resuming |

Also shows whether the zigzag pivot points (swing high/low) stayed stable or shifted during the window.

#### Tab 5: Signals

Full signal flow for each ticker across the streak window. Shows signal transitions (e.g., "Near Fib 38.2% → Near Fib 50.0% → Near Fib 61.8%" = deepening retracement) and trend consistency (Steady Uptrend / Steady Downtrend / Pivoting).

#### Tab 6: Volume

Volume ratio grouping (High ≥1.5x / Normal / Low <0.8x). High volume at a Fibonacci level confirms institutional interest in the zone.

---

## 9. Weekly SMA Integration

### Why Weekly SMAs Matter

Daily SMAs react to daily noise. **Weekly SMAs** (computed from Friday close data) smooth out the noise and reveal the true intermediate/long-term trend. The screener computes two sets of weekly data:

### 7.1 Configurable Weekly Short/Long SMAs

The weekly versions of your configured short/long period SMAs. If you set 9/21 daily, you also get 9-week/21-week SMAs and their crossover signals.

**Weekly signals** (W-Bullish Cross, W-Bearish Cross, W-Above, W-Below) tell you what the **higher timeframe** thinks about the trend.

### 7.2 Fixed 50-Week and 200-Week SMAs (Markers)

Independent of your short/long settings, the scanner always computes:

- **50-Week SMA**: Intermediate-term trend (~1 year of weekly data). Widely watched by swing traders.
- **200-Week SMA**: Long-term secular trend (~4 years of weekly data). Considered the "line in the sand" for generational support/resistance.

### How Daily–Weekly Alignment Works

The streak analysis classifies each ticker's daily–weekly relationship:

| Alignment | Daily Direction | Weekly Signal | Conviction |
|-----------|----------------|---------------|------------|
| **Confirmed Bullish** | Bullish | W-Above or W-Bullish Cross | ⭐⭐⭐ Highest — both timeframes agree |
| **Confirmed Bearish** | Bearish | W-Below or W-Bearish Cross | ⭐⭐⭐ Highest — short with confidence |
| **Counter-trend Bullish** | Bullish | W-Below or W-Bearish Cross | ⭐ Low — daily fights the weekly, likely whipsaw |
| **Counter-trend Bearish** | Bearish | W-Above or W-Bullish Cross | ⭐ Low — daily bearish but weekly still bullish |
| **Mixed** | Mixed direction | Any | ⭐ Low — no clear setup |
| **Neutral** | Any | N/A | N/A — not enough weekly data |

### How to Use Weekly SMAs in Practice

1. **Filter for "Weekly Confirmed"** preset to find the highest-conviction setups.
2. **Avoid "Counter-trend"** signals unless you have strong fundamental reasons.
3. **50W SMA proximity** acts as a magnet — stocks near it tend to bounce or break decisively.
4. **200W SMA proximity** is a generational event — stocks rarely visit it. When they do, it's either a once-in-years buying opportunity (strong company) or the start of a secular decline (broken company).
5. **Weekly spread trend** (Widening/Narrowing) complements daily spread — if daily is widening but weekly is narrowing, the daily move may be a counter-trend rally within a larger decline.

---

## 10. Markers System

### What Markers Are

Markers are **structural signals** derived from fixed 50/200 daily and 50W/200W weekly SMAs. Unlike crossover signals (which use your configurable short/long periods), markers are always computed on these canonical moving averages because they're universally watched by traders worldwide.

### Daily Markers

| Marker | Condition | Significance |
|--------|-----------|--------------|
| **Golden Cross** | 50 SMA crossed above 200 SMA within last 10 days | Major bullish signal. Historically followed by sustained rallies. Rare — happens once per multi-year cycle. |
| **Death Cross** | 50 SMA crossed below 200 SMA within last 10 days | Major bearish signal. Often precedes extended declines. |
| **Above 200 SMA** | Price currently above 200-day SMA | Long-term uptrend. Institutional buyers typically favor stocks above this level. |
| **Below 200 SMA** | Price currently below 200-day SMA | Long-term downtrend. Many institutional mandates prohibit buying below 200 SMA. |
| **Near 200 SMA** | Price within 2% of 200-day SMA | Critical decision zone. Will it bounce (support) or break (acceleration down)? High-conviction move expected. |
| **Near 50 SMA** | Price within 1% of 50-day SMA | Medium-term inflection point. Common area for swing-trade entries. |

### Weekly Markers

| Marker | Condition | Significance |
|--------|-----------|--------------|
| **Above 50W SMA** | Price above 50-week SMA | Intermediate uptrend (~1 year perspective). Swing-trade friendly. |
| **Below 50W SMA** | Price below 50-week SMA | Intermediate downtrend. Caution for any long positions. |
| **Near 50W SMA** | Price within 2% of 50-week SMA | Weekly inflection zone. Often aligns with earnings or macro catalysts. |
| **Above 200W SMA** | Price above 200-week SMA | Secular uptrend. This stock has been rising for years — strong name. |
| **Below 200W SMA** | Price below 200-week SMA | Secular downtrend. Very few quality stocks trade here. Red flag for longs. |
| **Near 200W SMA** | Price within 3% of 200-week SMA | **Generational level.** This SMA is visited once every few years. If the company is fundamentally strong, this could be a career-defining buy. |

### Marker Color Guide (UI)

| Marker | Badge Color | Reasoning |
|--------|-------------|-----------|
| Golden Cross | Gold background, black text | Precious — rare signal |
| Death Cross | Dark navy, white text | Dark/ominous |
| Above 200 SMA | Light green | Healthy |
| Below 200 SMA | Light red | Warning |
| Near 200 SMA | Light orange | Caution/decision |
| Near 50 SMA | Light blue | Informational |
| Above 50W SMA | Teal | Weekly bullish |
| Below 50W SMA | Pink | Weekly bearish |
| Near 50W SMA | Purple | Weekly inflection |
| Above 200W SMA | Deep green | Strong secular |
| Below 200W SMA | Deep red | Secular weakness |
| Near 200W SMA | Amber | Generational event |

---

## 11. Filters & Presets

### FVG Preset Filters

The Fair Value Gaps tab offers 3 preset combos plus manual direction/status filters:

| Preset | Logic | Use Case |
|--------|-------|----------|
| 🟢 High-Prob Bullish | Bullish + Unmitigated + Trend-aligned + Streak ≥ 2 | Buy the dip — pristine demand zones in uptrends with structural confirmation |
| 🔴 High-Prob Bearish | Bearish + Unmitigated + Trend-aligned + Streak ≥ 2 | Short the rip — pristine supply zones in downtrends with structural confirmation |
| 📊 Streak Signals | Any direction + Any status + Streak ≥ 3 | Market structure — high-streak tickers revealing strong directional bias |

Manual filters (Direction + Status) are also available and can be used independently. Activating a preset overrides manual filters; clicking it again deactivates the preset.

### MA Screener Filters

The MA Crossover page offers 4 filter dimensions that stack (AND logic):

#### 1. Ticker Search (Text Input)

Type any ticker symbol. The search is cross-tab aware — if the ticker isn't in the current tab, the UI auto-switches to the tab containing it.

#### 2. Marker Filter (Dropdown)

Select any marker from the "All Markers" dropdown. The dropdown is dynamically populated from the current scan's unique markers.

#### 3. Weekly Filter (Dropdown)

Filter by weekly signal:
- W-Above — weekly uptrend
- W-Below — weekly downtrend
- W-Bullish Cross — fresh weekly bullish crossover
- W-Bearish Cross — fresh weekly bearish crossover

#### 4. Preset Filter (Dropdown)

Pre-built combinations for common screening scenarios:

| Preset | Logic | Use Case |
|--------|-------|----------|
| ✅ Weekly Confirmed | Daily signal direction matches weekly signal direction | Highest conviction trades — both timeframes agree |
| ⚠️ Counter-trend | Daily signal direction opposes weekly signal direction | Risk identification — these trades often whipsaw |
| ⚡ Fresh Cross (≤3d) | `days_since_cross ≤ 3` | Earliest entries — catch crossovers before they run |
| 🗓 Weekly Crossover | Weekly signal is `W-Bullish Cross` or `W-Bearish Cross` | Rare weekly events — major trend shifts |
| 📈 Wide Spread (≥2%) | `ma_spread_pct ≥ 2` | Strong trending stocks — wide gap between MAs |
| 📉 Narrow Spread (<1%) | `ma_spread_pct < 1` | Potential crossover coming — MAs about to converge |

### Cross-Tab Mode

When **any filter** (marker, weekly, or preset) is active, the MA screener automatically switches to **cross-tab mode**:

- Results from all 4 tabs merge into a single "All Tabs" view
- Per-tab badges show filtered counts (e.g., "Bullish Cross: 3")
- Tabs with 0 matches fade out
- A "✕ Clear Filters" button appears to return to normal tab mode
- Clicking any individual tab clears all filters and returns to single-tab view

This eliminates the need to click through each tab when filtering — you see all matching tickers regardless of their signal type.

---

## 12. Cross-Strategy Signal Confluence

### Why Confluence Matters

No single signal is reliable on its own. The real power emerges when multiple independent strategies agree. Here's how to combine signals:

### Bullish Confluence Stack

Best long setups combine:

1. **MA Crossover**: Bullish Crossover or Recent Bullish — momentum shifting up
2. **Weekly Confirmed**: W-Above — higher timeframe agrees
3. **Gap Support**: At Support (Unfilled Gap Up) nearby — institutional floor under price
4. **FVG Support**: Unmitigated Bullish FVG + trend-aligned + streak ≥ 2 — institutional demand zone
5. **Momentum Pullback Grade A+**: Uptrend confirmed + oversold pullback
6. **RSI**: 30–50 — not overbought yet, room to run
7. **Marker**: Above 200 SMA + Above 50W SMA — structural uptrend
8. **Streak**: Perfect consistency (N/N days matched) — signal is persistent, not a one-day blip

**Confluence score**: More signals aligned = higher conviction. 4+ aligned = strong trade.

### Bearish Confluence Stack

Best short/exit setups combine:

1. **MA Crossover**: Bearish Crossover or Recent Bearish
2. **Weekly Confirmed**: W-Below
3. **Gap Resistance**: At Resistance (Unfilled Gap Down) — ceiling above price
4. **FVG Resistance**: Unmitigated Bearish FVG + trend-aligned + streak ≥ 2 — institutional supply zone
5. **Bearish Bounce Grade A+**: Downtrend confirmed + overbought bounce
6. **RSI**: 60–70+ — overbought into resistance
7. **Marker**: Below 200 SMA + Below 50W SMA — structural downtrend
8. **Streak**: Direction = Bearish, Spread = Widening — downtrend accelerating

### Divergence Warning Signals

When strategies disagree, exercise caution:

| Scenario | What's Happening | Recommended Action |
|----------|------------------|--------------------|
| Daily bullish + Weekly bearish | Short-term bounce in a downtrend | Avoid longs or use tight stops |
| MA bullish + Below 200 SMA | Momentum shifting but structure still bearish | Wait for price to clear 200 SMA |
| Gap support + Bearish Bounce A grade | Support zone exists but stock is in downtrend | Expect the gap to eventually break |
| Momentum Pullback B grade + RSI 60+ | Weak pullback signal + not actually oversold | Skip — wait for better setup |
| MA widening + Streak stalling | Trend looks good but momentum dying | Book profits — don't add |
| FVG streak misaligned with trend | Structure shifting but MAs haven't caught up | Early warning — reduce position, don't add |
| Bullish FVG + Bearish gap overhead | Demand zone below but supply zone above | Range-bound — trade the range or wait for resolution |

---

## 13. Practical Playbook

### Daily Workflow

1. **Start with MA Crossover** → Apply "Fresh Cross ≤3d" preset → These are today's actionable crossovers
2. **Apply "Weekly Confirmed"** → Filter to highest-conviction subset
3. **Check markers** → Golden Cross / Death Cross stocks deserve extra attention
4. **Cross-reference with Gap Screener** → Is there gap support/resistance near the crossover level?
5. **Check Momentum Pullback / Bearish Bounce** → Any A+ grades? These are the day's best risk/reward
6. **Run Streak Analysis** → 5-day streak on MA crossover → Focus on "Perfect" (5/5) streaks with Accelerating momentum

### Signal Priority Matrix

| Priority | Signal Combination | Action |
|----------|--------------------|--------|
| **Highest** | Fresh MA Cross + Weekly Confirmed + Gap Support + Unmitigated FVG + A+ Pullback | Full position, defined risk |
| **High** | Fresh MA Cross + Weekly Confirmed + Above 200 SMA | Standard position |
| **High** | High-Prob Bullish/Bearish FVG preset match + Gap zone at same level | Confluence entry — two independent support methods |
| **Medium** | Recent Cross + W-Above + Near 50 SMA | Half position, wait for confirmation |
| **Medium** | Streak Signals preset (≥3) + trend-aligned | Watch for pullback into FVG zone for entry |
| **Low** | Established trend (Above/Below MA) alone | Watch only — too late for entry |
| **Avoid** | Counter-trend + Below 200 SMA + Streak Stalling | High risk of loss |

### Risk Management Rules

- **Never trade counter-trend signals** as primary setups. Use them only to exit existing positions.
- **Near 200W SMA** stocks warrant research — could be generational buy or secular decline.
- **Streak consistency < 60%** means the signal is intermittent — reduce conviction.
- **Wide spread (≥2%) + Narrowing trend** = the move is likely near its end.
- **Inside Gap** positions are inherently risky — set stops at the gap boundary.

### Key Thresholds to Remember

| Metric | Key Level | Meaning |
|--------|-----------|---------|
| MA Spread | 2% | Above = strong trend, below = weak/converging |
| MA Spread | 1% | Below = imminent crossover expected |
| Days Since Cross | 3 | ≤3 = fresh, actionable |
| Days Since Cross | 5 | >5 = entry window closing |
| Gap ATR Ratio | 1.5 | Above = institutional-grade gap |
| RSI | 30/70 | Standard oversold/overbought levels |
| Stochastic %K | 40 (pullback) / 60 (bounce) | Entry zone thresholds |
| ADX | 30–40 | Sweet spot for trend strength |
| EMA21 Distance | 2× ATR | Maximum rubber-band stretch |
| Weekly 50W proximity | ±2% | Near = inflection zone |
| Weekly 200W proximity | ±3% | Near = generational level |
| Volume Ratio | 1.5× | Above = institutional interest |
| Streak Consistency | 80%+ | High reliability signal |

---

## 14. Proposed Intraday Scanner Research

The next intraday research portfolio is designed around finalized canonical `30m` bars. These are
predeclared hypotheses, not implemented or qualified recommendations.

| Priority | Scanner | Initial horizons |
|---:|---|---|
| 1 | Opening Range Breakout | `+30m`, `+60m`, close, next open |
| 2 | VWAP Reclaim/Rejection | `+30m`, `+60m`, close |
| 3 | Intraday Trend Pullback | `+30m`, `+60m`, `+120m` |
| 4 | Failed Opening Breakout | `+30m`, `+60m`, close |
| 5 | Relative-Strength Continuation | `+60m`, close, next open |
| 6 | Volatility Compression Expansion | `+30m`, `+60m`, `+120m` |
| 7 | Power-Hour Continuation/Reversal | close-to-next-open diagnostic, next close |
| 8 | Gap-and-Go / Gap-Fade | `+30m`, `+60m`, close |

Opening Range Breakout, VWAP Reclaim/Rejection, and Intraday Trend Pullback are the recommended
first implementations because their event boundaries and causal next-bar entries fit the stored
`30m` data most directly. Continuation, failure, long, and short variants remain separate study
hypotheses. Repeated matching bars within one episode do not create additional signals.

The five-year `30m` dataset covers the current 386-member cohort and supports mechanics validation
and fixed-cohort exploration. It does not establish historical liquid-universe performance without
point-in-time intraday membership, corporate-action and symbol-change treatment, historical sector
mappings, delisting-aware outcomes, and causally available benchmark bars.

See [INTRADAY_STRATEGIES_DESIGN.md](INTRADAY_STRATEGIES_DESIGN.md) for trigger definitions,
required evidence, outcome timing, FDR qualification, data limitations, and implementation order.

---

*Last updated: September 2026*
