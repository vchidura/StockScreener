# Signal Research & Validation

How predictive signals are validated in this project, what passed, what didn't, and why.

**Status**: `xsmom-1.0` in production since 2026-08-08.
**Owner tooling**: `backend/research/`, `backend/scripts/run_alpha_research.py`

---

## 1. Why this exists

The original Daily Recommendation Engine predicted next-day direction and was never measured
against a baseline. When it finally was, over 258 resolved calls:

| Metric | Value |
|---|---|
| Model accuracy | **44.6%** |
| Up-days in sample ("always long" accuracy) | **55.0%** |
| Calls issued | 191 BEAR vs 67 BULL |
| Avg return in the call's own direction | **−0.32%** |

The model was **10.4 points worse than doing nothing**, and systematically bearish in a rising
market. No transaction costs were modelled.

The root cause was structural, not a tuning problem:

- **Direction prediction fights market beta.** ~55% of large caps rise on any given day, so a
  directional model mostly re-predicts the index and loses to simply holding it.
- **No baseline.** Nothing compared results to always-long, so negative skill went unnoticed.
- **No out-of-sample discipline.** Priors were derived from the same recommendations they scored.

---

## 2. The approach that replaced it

### Predict *relative* return, not direction

Rank the universe cross-sectionally each day and trade the extremes (long top decile, short
bottom decile). Market drift cancels out, so what remains is stock-selection skill.

### Point-in-time features

Every feature for `(ticker, t)` uses data through the **close of day t**. The label is the return
from `close(t)` to `close(t+h)`. This contract is enforced in `research/features.py` and is the
single most important guard against look-ahead.

### Purged walk-forward

Expanding-window training with an embargo between train and test, sized to the label horizon so
overlapping labels can't leak. Implemented in `research/evaluate.py::purged_walk_forward`.

### Neutralization

Predictions are residualized against `beta_60`, `liquidity`, `vol_21` and demeaned within sector.
Whatever survives is not explained by risk exposure. **This is the step that decides most
outcomes** — see §4.

---

## 3. The gate

Five checks, committed before results are seen. A candidate ships only if all five pass.

| # | Check | Rationale |
|---|---|---|
| 1 | IC t-stat > 2 (neutralized, non-overlapping) | Statistical significance |
| 2 | IC mean > 0.005 | Economic relevance |
| 3 | Deciles monotone (D10 > D1) | Rank ordering is real, not a single-bucket artifact |
| 4 | Net L/S Sharpe > always-long Sharpe | Beats the free alternative |
| 5 | Net L/S return positive | Survives transaction costs |

Run it:

```bash
python backend/scripts/run_alpha_research.py \
    --features mom_12_1 --horizon 21 --embargo-days 22 \
    --rebalance-days 21 --cost-bps 2
```

Verdict is one of **ALPHA**, **PROMISING BUT UNDERPOWERED**, **RISK EXPOSURE, NOT ALPHA**, or
**NO SIGNAL**.

`UNDERPOWERED` is reported when the economics pass (neutralized Sharpe beats the benchmark, net
return and decile spread both positive) but fewer than 60 independent periods are available, so
the t-stat gate cannot clear 2. It separates *"the data disproves this"* from *"the data cannot
yet decide"* — those warrant opposite responses.

### Metrics used

- **IC** — daily Spearman correlation between prediction and forward return. Realistic range for
  a genuine daily equity signal is **0.01–0.04**. Anything sustained above 0.10 is suspicious.
- **IC IR** = mean(IC) / std(IC); t-stat = IR × √n.
- **Decile spread** — mean forward return per predicted decile.
- **Net Sharpe** — after turnover-based costs.

Accuracy / hit-rate is deliberately *not* a primary metric: it ignores magnitude and is what
misled the original system.

---

## 4. Results

Universe: 398 tickers, 837 trading days (2023-03-08 → 2026-07-09), 330k ticker-days.
Costs: 2 bps one-way on traded notional. 26 independent 21-day periods.

### Horizon matters more than features

`mom_12_1`, neutralised, net of 2 bps, rebalanced at the holding period:

| Horizon | Net return | Net Sharpe | Always-long Sharpe | Independent periods | Verdict |
|---|---|---|---|---|---|
| 1 day | — | 0.72 | **0.83** | ~800 | below benchmark |
| 5 days | 17.4% | 0.845 | **0.880** | 118 | below benchmark |
| 10 days | 19.0% | **0.956** | 0.853 | 57 | above |
| 21 days | 23.0% | **1.32** | 1.02 | 26 | above |

The signal crosses from unusable to usable **between 5 and 10 days**.

At h=5 the *gross* Sharpe (0.8735) is already below the benchmark (0.8801), so this is not a
transaction-cost problem — there is not enough predictive content at that horizon to begin with.

Note the tension in statistical power: h=5 has 118 independent periods and rejects, while h=21
has 26 and passes. The horizon we can measure most confidently is the one that fails. Quote the
1.32 with that caveat attached.

Momentum, volatility and liquidity are *slow* signals. Sampling them intraday and rebalancing
daily destroys them in turnover.

### Neutralization is the decisive test

| | Raw Sharpe | Neutralized Sharpe | Net return | Turnover | Verdict |
|---|---|---|---|---|---|
| 13-feature model | 1.57 | **0.96** | 14.0% | 2.38× | **Rejected** |
| Momentum only | 1.52 | **1.32** | 23.0% | 1.28× | **Passed (underpowered)** |
| `close_strength` only | 0.32 | **−0.92** | −8.4% | 3.53× | **Rejected** |
| Always long | — | 1.02 | 13.7% | 0 | benchmark |

Raw Sharpes were nearly identical (1.57 vs 1.52) — on raw numbers the 13-feature model looked
*better*. Neutralization revealed its edge was beta and size exposure: stripped of those it fell
**below** the index. Momentum degrades but stays above benchmark, which is the signature of
genuine alpha.

`close_strength` (where the close sits within the day's range) failed all five checks and its
decile spread is **negative** — D10 − D1 = −63 bps per period. Closing strong predicts *lower*
forward returns. It stays in `FEATURE_COLUMNS` as a descriptive input but is not a predictor.

#### Sector neutralization cost about a quarter of the edge

Momentum was measured three times as `ticker_metadata.sector` coverage improved:

| Sector coverage | Net return | Neutralized Sharpe | vs always-long |
|---|---|---|---|
| 1 ticker (skipped entirely) | 30.5% | 1.83 | +0.81 |
| 229 tickers (partial) | 26.3% | 1.46 | +0.44 |
| **392 tickers (current)** | **23.0%** | **1.32** | **+0.30** |

Roughly **28% of the apparent edge was sector rotation** — a semiconductors-long / software-short
tilt — not stock selection. Only the 392-ticker figure should be quoted. The earlier numbers were
produced with neutralization effectively disabled.

#### Sector neutralization equalizes means, not exposure

Demeaning within a sector removes that sector's *average* score. It does nothing about
*dispersion*, so a sector with a wide spread of outcomes can still dominate both tails.

Observed on the live 2026-08-10 book: **31 of the 40 shorts (78%) were Technology**, despite full
sector neutralization. Tech had a bimodal year, so its casualties sat far below the tech mean and
filled the bottom decile.

`xsmom-1.0` is therefore sector-**mean**-neutral, not sector-**exposure**-neutral. Earlier
revisions of this document claimed the latter; that was wrong. Concentration must be managed in
portfolio construction, not assumed away by the neutralization step.

#### The short book is a reversal bet

Because `mom_12_1` ends at t−21, the most recent month is invisible to the model. On 2026-08-10
that left the book positioned against the recent tape:

| Decile | Avg trailing 21-day return |
|---|---|
| **D1 (SHORT)** | **+16.1%** |
| D2–D9 | +0.9% to +5.5% |
| **D10 (LONG)** | **−4.5%** |

Sixteen of the forty shorts were up more than 20% over the previous month — MSFT (+31%),
PLTR (+38%), TEAM (+71%) among them. All were genuine 12-month losers over the measured window
(−26%, −30%, −48%) that had begun rebounding.

This concentration looks like a momentum-crash setup, but the independent-period test does **not**
validate that interpretation. Bottom-decile names that had rallied >20% subsequently returned
−0.36% relative to the universe (t=−0.46); the whole bottom decile was −0.26% (t=−0.50).
Both are noise. The short list is a hedge/watch list, not validated directional alpha.

#### Weights must be explicit and z-scored

Testing a 21-day reversal control exposed a mismatch between research and production:

- `run_alpha_research.py` **fits** coefficients by ridge, so it learns each feature's sign.
- `generate_cross_sectional_signal.py` used `cross[MODEL_FEATURES].mean(axis=1)` — an unweighted
  mean of raw, unstandardised features.

Adding `rev_21` to `MODEL_FEATURES` would therefore have shipped the *opposite* of the validated
model, since `rev_21` is the negation of the 21-day return and needs a negative weight:

| Blend | SHORT avg 21d | LONG avg 21d | Tech in short |
|---|---|---|---|
| `mom_12_1` only (live) | +16.1% | −4.5% | 31/40 |
| `mom + rev_21` (what the old code would build) | +22.3% | −11.5% | 26/40 |
| `mom − rev_21` (what the gate actually fits) | **−9.0%** | **+16.3%** | **15/40** |

The gate scored `mom_12_1,rev_21` at Sharpe 1.32 — indistinguishable from momentum alone. A
single live cross-section showed lower sector concentration after flipping the reversal sign,
but the independent-period conditional tests were insignificant, so no risk-improvement claim is
made and the blend is not promoted.

`compute_signal` now requires a signed weight per feature in `MODEL_WEIGHTS` and z-scores before
blending. Both are no-ops for a single feature, so `xsmom-1.0` output is unchanged (verified: 0
decile or side changes across 396 names).

### Active echo momentum — strongest candidate

`mom_12_6 = P(t−126) / P(t−252) − 1` isolates the older half of the 12-month trend. It is a
continuation signal, not a trend-turn detector. Recent momentum, MA distance and reversal inputs
mostly diluted it.

Activity is used only to define the eligible universe: top 50% each date by the average
cross-sectional rank of `volume_ratio`, `vol_ratio`, and `range_pct`. Echo momentum is then
neutralized and ranked **inside those eligible names**.

| Horizon | Neutral IC | IC t | Net L/S Sharpe | Eligible benchmark | LONG alpha t | Verdict |
|---|---:|---:|---:|---:|---:|---|
| 5 days | 0.0009 | 0.09 | 0.81 | 1.19 | 1.61 | risk exposure |
| 10 days | 0.0158 | 1.15 | 0.90 | 1.10 | 1.67 | risk exposure |
| **21 days** | **0.0425** | **2.64** | **1.44** | **1.21** | **2.15** | **5/5 ALPHA** |

At 21 days the top-decile long portfolio returned 38.3% annualized after costs, with 19.8%
annualized alpha over its eligible universe. Treat that return level cautiously: only 26
independent periods exist, survivorship bias remains, and no capacity model is applied.

The long-leg alpha was positive in both chronological halves (+1.86% and +1.49% per period), but
the late-half t-stat was only 1.27. Full-sample significance does not prove regime invariance.

The activity composite beats a pure liquidity filter on statistical defensibility: liquidity
produced a higher top-decile return, but its neutralized IC t-stat was only 1.69; the composite
cleared 2. Within-sector ranking remains an optional diversification view — it reduced average
max-sector share from 32% to 18% but reduced 21-day alpha from 2.14% to 1.47% per period.

Reproduce the candidate:

```bash
python backend/scripts/run_alpha_research.py --features mom_12_6 \
   --horizon 21 --embargo-days 22 --rebalance-days 21 --cost-bps 2 \
   --activity-filter composite
```

### Standalone feature IC (21-day horizon)

| Feature | IC | t-stat |
|---|---|---|
| `mom_12_6` | 0.0501 | 10.69 |
| `vol_21` | 0.0435 | 6.63 |
| `mom_12_1` | 0.0431 | 7.42 |
| `liquidity` | 0.0337 | 7.55 |
| `dist_ma200` | 0.0294 | 4.78 |
| `range_pct` | 0.0292 | 5.73 |
| `dist_ma50` | 0.0200 | 3.41 |
| `rev_21` | −0.0168 | −3.06 |
| `rsi_14` | 0.0126 | 2.44 |
| `vol_ratio` | 0.0102 | 3.23 |
| `mom_6_1` | 0.0080 | 1.40 |
| `gap_overnight` | −0.0058 | −0.96 |
| `rev_5` | −0.0014 | −0.25 |
| `volume_ratio` | 0.0002 | 0.08 |

`vol_21` and `liquidity` scoring highly is what tipped off the beta-exposure problem: over
2023–2026 that combination is "high-beta megacaps in a bull market".

### Structured trend-pullback pattern — descriptive only

The symmetric setup was defined point-in-time as:

1. SMA20 crossed above/below SMA50 within 40 sessions.
2. A confirmed 2-left/2-right pivot made a higher high/lower low by at least 0.25 ATR.
3. Price pulled back 2–15 bars later to SMA20 or the broken prior swing level without closing
   through SMA50.
4. Hammer/shooting-star, engulfing, or strong directional-close candle triggered the watch.

All pivots are delayed by two bars before they become available; the backtest does not use future
knowledge. Across 400 active tickers since 2023, the base pattern produced 4,065 bull and 3,559
bear triggers. It did **not** predict forward returns:

| Variant | Horizon | Combined alpha | t-stat |
|---|---:|---:|---:|
| Base | 5d | +0.03% | 0.15 |
| Reversal candles only | 5d | +0.29% | 1.64 |
| Base | 21d | −0.72% | −1.25 |
| Strict candle + swing retest | 21d | +1.08% | 1.14 |

No variant clears t=2. The active-echo timing overlay also fails to clear significance: D10 plus
the base bullish trigger returned +3.66% alpha per independent 21-day signal date, but t=1.56;
the strict overlay fell to t=1.03. Bearish overlays were negative.

Therefore the pattern is exposed only as `UNVALIDATED_TIMING`: a structured watch condition and
conflict/explanation layer, not a BUY/SHORT recommendation. Cross-sectional ranking answers
*which names have measured continuation evidence*; this pattern may describe *where price is in
its structure*, but has not shown it can improve entry timing.

#### One-hour bars do not rescue the pattern

The same point-in-time detector was evaluated on 1.44M regular-session hourly rows (09:30–15:30
ET), covering 400 tickers from August 2024. SMA20/SMA50 and all swing/pullback parameters are
hourly periods; 7/21/35 forward bars approximate 1/3/5 sessions.

| Variant | Direction | Horizon | Alpha | t-stat |
|---|---|---:|---:|---:|
| Base | Bull | 7h | +0.02% | 0.36 |
| Base | Bear | 7h | +0.12% | 1.90 |
| Base | Combined | 7h | +0.05% | 1.15 |
| Base | Bear | 21h | +0.36% | 1.75 |
| Reversal candle | Bear | 35h | +0.60% | 1.70 |
| Strict | Combined | 35h | −0.24% | −0.78 |

These figures charge 2 bps one-way on both entry and exit. The earlier one-way-only calculation
made base bearish 7-hour alpha look nominally significant (t=2.20); after correcting costs it is
t=1.90. No variant has significant positive alpha among the 36 tested combinations. The only
|t|>2 result is *negative* alpha for bullish swing retests at 7 hours (−0.16%, t=−2.03), which is
not the requested continuation pattern. Hourly output remains descriptive/watch-only.

---

## 5. Evolving market discovery architecture

Daily discovery separates mature continuation from early reversal. It never combines them into
one weighted score:

| State | Definition | Validation |
|---|---|---|
| `CONTINUATION` | Top-half activity and top-decile neutralized `mom_12_6` | Candidate alpha |
| `REVERSAL_WATCH` | Weak older trend, top-20% 21d return, positive 5d return, above rising SMA20 | Discovery only |
| `EMERGING_REVERSAL` | Reversal watch plus above-median activity | Discovery only |
| `REVERSAL_CONFIRMED` | Emerging reversal plus SMA20>SMA50 and confirmed higher high + higher low | Discovery only |
| `CONFLICT` | Old-trend rank and recent momentum disagree materially | Discovery only |
| `LAGGARD` | Weak older/recent momentum with bearish structure | Discovery only |

The snapshot uses the latest date covering at least 90% of the recent peak universe. This prevents
a partial EOD ingestion from silently omitting reversal candidates. On 2026-08-10 the first shadow
snapshot classified 396 names: PLTR as `EMERGING_REVERSAL`, TEAM as `REVERSAL_CONFIRMED`, and
MSFT as `REVERSAL_WATCH` because its activity rank was below 50%.

States are persisted in `market_discovery_states` under model version
`discovery-1.0-shadow`. Reversal states are deliberately not inserted into
`daily_recommendations`; outcomes must accumulate before they can become a recommendation gate.

---

## 6. Production signal — `xsmom-1.0`

| Property | Value |
|---|---|
| Feature | `mom_12_1` (12-month return, skipping the most recent month) |
| Horizon | 21 trading days |
| Neutralized against | `beta_60`, `liquidity`, `vol_21`, sector |
| Actionable | Decile 10 → `LONG`, decile 1 → `SHORT`, else `FLAT` |
| Universe | ~396 names scored daily |

### Pipeline

```
run_scheduler.py (post-close)
  └─ job_cross_sectional_signal()
       └─ generate_cross_sectional_signal.py
            ├─ research.features.prepare_live_cross_section()   ← shared with research
            ├─ research.evaluate.neutralize()                   ← shared with research
            └─ upsert → cross_sectional_signals
```

**Production reuses the research feature code.** There is no second implementation, so the live
signal cannot silently drift from what was validated.

Runs after daily closes are written, wrapped in try/except so a signal failure cannot break
price ingestion.

### Storage

`cross_sectional_signals`, unique on `(trade_date, ticker, model_version)`, idempotent upsert.
Migration: `backend/migrations/005_cross_sectional_signals.sql`.

Stamping `model_version` means a future `xsmom-2.0` coexists rather than overwriting history.

### API

| Endpoint | Returns |
|---|---|
| `GET /api/signals/cross-sectional?side=LONG&limit=50` | Ranked universe for a date |
| `GET /api/stock/{ticker}/cross-sectional-signal` | One ticker's rank + 30-day history |

### UI

Surfaces as the **Universe rank** tile in Trade Setup, e.g. `LONG · Rank 1 of 396 · decile 10`.

This gives Trade Setup an explicit division of labour:

- **Cross-sectional signal (validated)** → *which* names, *which* direction, *what* horizon
- **Technicals — EMAs, retests, Fibonacci, gaps (descriptive)** → *where* to enter, stop, target

### Manual run

```bash
python backend/scripts/generate_cross_sectional_signal.py --dry-run   # inspect
python backend/scripts/generate_cross_sectional_signal.py             # persist
```

---

## 6. Known limitations

Read these before sizing anything on the signal.

1. **Only 26 independent periods.** IC t-stat is 1.70 (p ≈ 0.10) — below the formal gate. It
   passes on economic checks and matches decades of published momentum research, but it is not
   statistically proven on this data alone. **Treat as monitored, not settled.**
2. **Sector coverage is 392 of 400.** Eight names fall into a residual bucket where demeaning is
   weak. Figures above are beta/size/vol *and* sector neutral. Re-run the gate after any
   universe change — this is what moved Sharpe from 1.83 to 1.32.
3. **Survivorship bias.** The 400 tickers are today's universe; names delisted over 2022–2026 are
   absent, so the backtest never holds a loser to zero. This inflates results.
4. **One regime.** 3.4 years, largely a bull market, no sustained drawdown.
5. **Costs are a flat 2 bps one-way.** No market-impact or capacity modelling.

---

## 7. Adding a new candidate signal

1. Add the feature to `FEATURE_COLUMNS` in `research/features.py`, computed strictly from data
   at or before `close(t)`.
2. Run the gate against it:
   ```bash
   python backend/scripts/run_alpha_research.py --features <name> \
       --horizon 21 --embargo-days 22 --rebalance-days 21 --cost-bps 2
   ```
3. Only on **ALPHA** do you add it to `MODEL_FEATURES` in
   `generate_cross_sectional_signal.py` and bump `MODEL_VERSION`.

Do not adjust thresholds after seeing results — that fits the test to the desired answer.

### Statistical power — how much data a test needs

$$n_{days} = \left(\frac{t \cdot \sigma_{IC}}{\overline{IC}}\right)^2$$

With observed $\sigma_{IC} \approx 0.18$:

| True IC | Days for t > 2 | Calendar |
|---|---|---|
| 0.010 | 1,296 | ~5 yrs |
| 0.020 | 324 | ~15 months |
| 0.030 | 144 | ~7 months |

Testing several patterns at once requires a multiple-comparison correction, which raises the
required `t` and roughly doubles the sample needed.

---

## 8. Status of the 5-layer calibration pipeline

**Not wired to production. Do not schedule it.** Its core concepts were tested and failed.

### Tested and rejected (2026-08-10)

The pipeline's ideas — opening-range breakout, VWAP deviation, intraday volatility, close
strength — were reformulated as cross-sectional features from hourly bars and put through the
gate. This became possible after backfilling `stock_prices_hourly` from Yahoo's full 730-day
window: **499 trading days (2024-08-12 → 2026-08-07), 398 tickers, 1.44M bars**, giving a
detectable IC of 0.016.

Features: `h1_return`, `h1_breakout`, `vwap_dev`, `intraday_vol`, `last_hour_ret` — the five that
encode intraday *path*, which a daily bar cannot express. Horizon 1 day, 231 independent test
periods, 2 bps one-way.

| | L/S raw | L/S neutralised | Always long |
|---|---|---|---|
| Gross return | −21.4% | −15.5% | +17.3% |
| **Net return** | **−38.7%** | **−32.8%** | **+17.3%** |
| Net Sharpe | −1.64 | −1.79 | **+1.37** |
| Turnover | 3.45x | 3.44x | 0 |

**All 5 checks failed.**

### Why: the deciles are U-shaped, not monotone

```
D1   18.58 bps  Sharpe 2.45   <- best
D2   10.25
D3    6.79
D4    0.40
D5    0.48                    <- worst
D6    6.75
D7    5.00
D8    2.80
D9    4.93
D10  12.45      Sharpe 1.75   <- second best
```

Both extremes outperform; the middle is dead. These features measure **how much** a stock moved
intraday, not **which way** it goes next. A long/short spread cannot capture a U-shape — you are
long D10 (12.45) and short D1 (18.58), so the spread is negative by construction. The strategy
loses *because* the signal is symmetric.

This confirms the flaw identified in the original code review: the pipeline fed **magnitude
features into a directional signal**. `volatility_score` and `calendar_score` say nothing about
direction, yet were assigned win rates and confidence multipliers. That was the root error, not a
tuning problem.

What remains is a **volatility/attention premium** — extreme intraday action predicts higher
next-day return regardless of sign. Real, but not tradable long/short, and `vol_21` is already in
the neutralisation set, so it is largely a known exposure rather than new alpha.

Two things make this a credible null rather than an underpowered one:

- **Gross return was already negative** before costs. There was nothing to erode.
- **Turnover of 3.44x** is structural: features built from today's intraday action change
  completely every day, so the book nearly fully rotates each session. Any edge would have to
  clear ~6.9 bps/day of cost.

**Measurement corrections applied before this run:** `close_strength` was moved to the daily
feature set (it is fully determined by daily OHLC, so reconstructing it from hourly only added
noise), and `vwap_dev` / `last_hour_ret` are now anchored to the official daily close rather than
the last 1h bar, which misses the closing auction by ~0.027% on average. The corrections made the
rejection *stronger*, from 4/5 to 5/5 failures.

**Scope of the claim:** this tested these formulations at hourly resolution with a close-of-day
decision. It did not test the exact 5-layer scripts at their intended 9:35 AM timestamp on
5-minute bars. But with negative gross returns and a U-shaped decile profile, a finer-grained
version of the same concepts is unlikely to reverse the result.

### What remains true

The scripts were corrected during this work — look-ahead anchoring, direction-mixing in win-rate
calculation, missing Beta-Binomial shrinkage, and an analog bug scoring unresolved days as losses
— so they are *correct*, just now also *measured and rejected*.

Layer 1 additionally cannot run at its documented 9:25 AM: it reads bars through midday and
`_compute_breakout_score` uses `bars_5m[6:]` (10:00 AM onward). At 9:25 there are no bars, so
every pattern returns the neutral 50. Any future attempt must first fix the timing contract.

**Retained deliberately:** `daily_recommendations` and `recommendation_performance_log`. Those
231 resolved calls are the historical baseline the Model reliability tile reports and the control
group every future signal is measured against. Prices can be re-fetched; a record of past
predictions cannot.

### Maintaining the hourly window

Yahoo serves 1h bars for a rolling 730 days. `job_hourly` requests ~5 days, which keeps the tail
current but never repairs history, so the deep window erodes as the limit rolls forward. A
periodic deep pass keeps it intact:

```bash
python backend/scripts/update_hourly_prices.py --backfill --days 730
```

---

## 9. File map

| Path | Purpose |
|---|---|
| `backend/research/features.py` | Point-in-time features, beta, sector map, live cross-section |
| `backend/research/evaluate.py` | IC, deciles, purged walk-forward, costs, neutralization |
| `backend/scripts/run_alpha_research.py` | The gate — run this before promoting anything |
| `backend/scripts/generate_cross_sectional_signal.py` | Daily production signal |
| `backend/migrations/005_cross_sectional_signals.sql` | Signal table |
