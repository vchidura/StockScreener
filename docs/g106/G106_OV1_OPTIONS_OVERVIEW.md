# G106 OV1: Options Overview (dealer exposure by strike and expiry)

Opened Nov 16 2026 (user). Matrix row OV1 in `G106_IMPLEMENTATION_MATRIX.md`; evidence in `G106_EVIDENCE.md`
(entry "Nov 16 OV1"). Builds on A3 (`/api/options/gamma`, Nov 12) and serves D15 (P-WALL / GEX) as a page. The user
shared two screenshots of a commercial GEX tool as the functional reference: they set **what** the page shows, not its
look, names or styling, which follow the portal's Options pages.

**Phase 1 (this build): Gamma and Delta.** Phase 2 (after the user reviews phase 1): Charm and Vanna (section 10).

## 1. What the page shows

One underlying at a time (the 9 chain-captured names: AAPL, AMZN, IWM, MSFT, NFLX, NVDA, QQQ, SPY, TSLA):

1. **Exposure by strike**: one bar per strike (Net) or call and put parts (Calls vs puts), with spot, gamma flip,
   call wall and put wall marked; a strike window around spot (default +/-5% for index ETFs, +/-10% for stocks); a
   table view of the same numbers.
2. **Strike x expiry heatmap**: strikes high to low, expiries left to right, a diverging colour around zero with the
   signed value in every cell, the spot row marked; Net, Calls or Puts.
3. **Largest levels**: the 10 strikes with the largest net exposure, with call and put parts, share of gross exposure,
   distance from spot and level tags (call wall, put wall, peak gamma).
4. **Exposure by expiry**: net (or call/put) exposure per expiry with its share of the range's gross exposure.
5. **Summary**: net exposure in the chosen range, gamma regime at spot and the flip, call wall, put wall, the share of
   exposure expiring today, the provider-Greeks cross-check.
6. **Data stamp and notes**: capture time, spot, contracts used, strike band and expiries captured, and the limits
   below.

Controls: Underlying; Greek (Gamma, Delta; Charm and Vanna shown disabled until phase 2); View (Net / Calls vs puts);
Weight (Open interest / OI + today's volume, an estimate); Expiries (0 DTE / <= 7 days / All captured); Units (Dollars /
Shares). Underlying, Greek, Weight and Expiries live in the URL (`/options/overview?underlyer=QQQ&greek=delta&...`).

### What the page must not imply

- **No trade signal.** It is a research observation of positioning under a stated hypothesis; nothing on the page
  places, suggests or ranks an order. The API carries `execution_permission: false`. No alert or detector uses these
  numbers until a replay shows value (user, Nov 16).
- **Dealer side is assumed.** Open interest says how many contracts exist, never who holds which side. The convention
  (dealers long calls, short puts) sets every sign; under the opposite convention every sign flips. Walls, shares and
  the strike profile's shape do not depend on it.
- **Open interest is the previous settlement.** Intraday changes show only through volume (the OI + volume weight).
- **SPY is not SPX** (QQQ is not NDX, IWM is not RUT): the ETF's own options, open interest and strikes; levels are
  ETF prices.
- **Not the full chain** (section 2).

## 2. Data and coverage

Source: `rt.option_breadth_current`, one row per contract and session, rewritten by the Options scheduler's chain
capture about every 5 minutes for the 9 names. Per contract: type, expiry, strike, open interest (prior settlement), day
volume, bid/ask, provider IV/delta/gamma/theta/vega, our `local_iv` and local Greeks, spot, rate, dividend yield,
`observed_at`.

- **Coverage is a band**, not the listed chain: strikes within about +/-20% of spot and expiries up to about 60 days
  (measured Nov 12 19:05: SPY 5,068 contracts, strikes 607-910 = -20%/+20%, 17 expiries Nov 12 - Dec 31; NVDA 650
  contracts, 12 expiries). The page states the band and expiries from the capture itself.
- **Latest capture only (R55).** The table keeps rows of contracts that left the band in earlier captures (older
  `observed_at`, older spot). The read uses only the rows sharing the newest `observed_at` and reports how many earlier
  rows it left out (`earlier_capture_rows_excluded`; SPY 64, NVDA 86 at Nov 12 19:05).
- **A failed capture (R48)** writes no rows, so the newest rows are the last COMPLETE capture; when a newer capture did
  not complete, the read says so (`LATEST_CAPTURE_NOT_COMPLETE`, with its state, time and reason).

### Why contracts are skipped (Nov 12 19:05 chain, all 9 names)

The gamma read reported SPY `skipped_contracts` ~940 of ~4,680 on Nov 16. On the recorded Nov 12 chain the same rule
skips SPY 1,269 of 5,068:

| Reason | SPY | QQQ | NVDA | Effect on totals |
|---|---|---|---|---|
| Open interest 0 (new or untraded strikes) | 1,227 | 1,211 | 66 | None: weight 0 adds nothing to an OI total |
| No IV from us or the provider (`BELOW_NO_ARBITRAGE_BOUND`: deep in-the-money midpoint below intrinsic) | 42 | 52 | 1 | 0.11% / 0.15% / 0.06% of open interest left out |
| Our IV missing, provider IV used in the same model | 37 | 81 | 10 | Counted, labelled `PROVIDER_IV_FALLBACK` |

Over the 9 names the uncovered open interest is 0.00-0.22%. The totals can be trusted for coverage; the read reports
`weight_coverage_fraction` and raises `WEIGHT_WITHOUT_GREEKS` below 99%.

## 3. Definitions and formulas

Per contract *i*: open interest OI_i, day volume V_i, spot S (the capture's `spot`, else `underlying_price`),
multiplier 100, dealer sign s_i, and our Black-Scholes-Merton Greeks with continuous dividend yield.

**Greeks (one model).** Gamma and delta are evaluated from our `local_iv` (the provider's IV only where ours is
missing, counted), the capture's spot, the stored rate and dividend yield, and the **capture's own time to expiry**:
`_time_to_expiration_years(expiry, observed_at)` = whole calendar days / 365, or for an expiry today the time to 16:00
New York. That is the convention `local_iv` was solved with, so the read's gamma and delta equal the stored
`local_gamma` and `local_delta` (checked: largest relative difference 2e-15 over the ~16,700 Nov 12 19:05 contracts
with a stored `local_gamma`, all 9 names). A contract
past its expiry close (today's expiries after 16:00 New York) has no Greek and is counted as expired.

**Dealer sign** (`DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS`, `gamma_exposure.dealer_signs`): calls +1, puts -1.

**Weight** w_i: `oi` = OI_i; `oi_volume` = OI_i + V_i. The second assumes every contract traded today opened a position
on the conventional side; it is an upper-bound estimate, labelled `VOLUME_WEIGHT_ESTIMATE`.

| Greek | Per contract (dollars) | Shares | Unit on the page |
|---|---|---|---|
| Gamma | GEX_i = s_i x gamma_i x w_i x 100 x S^2 x 0.01 | GEX_i / S | Dollars of dealer delta change for a 1% move; shares to hedge per 1% move |
| Delta | DEX_i = s_i x delta_i x w_i x 100 x S | DEX_i / S = s_i x delta_i x w_i x 100 | Dollars of dealer delta (delta-equivalent notional); delta-equivalent shares |

Under the convention long calls (delta > 0, sign +) and short puts (delta < 0, sign -) **both carry long delta**: the
delta view is positive on both sides, and a hedged dealer holds that many shares short. This follows from the stated
convention, not from the data.

**Expiry scope**: `0dte` = expiry on the session date; `7d` = 0-7 calendar days (includes today); `all` = every
captured expiry. Calendar days count from the session date.

**Aggregates** (contracts with weight > 0, alive, and a finite Greek, in scope): per strike call part, put part, net =
call + put, call and put open interest; per expiry the same plus gross |exposure| and share; per strike x expiry cell the
call and put parts; totals net, call, put, gross (sum of |GEX_i| or |DEX_i|), net in shares, contributing contracts.

- **Share** of a strike or expiry = its gross exposure / the range's gross exposure.
- **Expiring today share** = gross exposure of 0 DTE contracts / gross exposure of every captured contract (same Greek
  and weight; independent of the scope shown). Zero after 16:00 New York.
- **Largest levels**: strikes by |net| descending, top 10.

**Levels (gamma, in every Greek view)**, from `options.analytics.gamma_exposure` on the same contracts, weights and
times as the bars (`build_gamma_profile` + `detect_gamma_walls`, reused unchanged):

| Level | Definition |
|---|---|
| Gamma flip | Spot level where net dealer gamma changes sign, searched from -20% to +20% of spot on an 81-point grid with gamma recomputed at each level (each contract keeps its IV: sticky strike), linearly interpolated; the crossing nearest spot (`solve_gamma_flip`) |
| Regime at spot | Sign of net gamma at spot (positive: hedging leans against moves; negative: hedging follows them) |
| Call wall / put wall | Strike with the largest call (put) gamma notional, unsigned; distance to spot given |
| Peak gamma | Strike with the largest call + put gamma notional |
| Gamma walls | Strikes holding at least 5% of gross gamma within 10% of spot (`detect_gamma_walls`) |

Consistency: the profile's net gamma equals the bars' net (tested), and the flip equals `solve_gamma_flip` run directly
on the same inputs (tested).

**Provider cross-check.** The same exposure with the provider's gamma/delta on the contracts where both exist (same
weight, sign and S-scaling): `difference_fraction` = (our gross - provider gross) / provider gross, and
`net_difference_fraction` = (our net - provider net) / provider gross (the net alone can sit near zero around the
flip). Above 10% either way the read raises `PROVIDER_CROSS_CHECK_DIVERGES`; the page quotes whichever gap is larger.
The provider's Greeks use its own IV, underlying price and day count, so a few percent is expected.

**Spot against the quotes.** The capture's `spot` is the equity bar at `equity_cutoff`; the option quotes are a minute
or two later, and the provider's `underlying_price` travels with them. Our IV (`local_iv`) is solved at `spot`, so a gap
between the two splits near-money call and put IVs apart, most of all for 0 DTE (Nov 12 QQQ 0 DTE at 11:07 PT: spot
688.00 vs 689.18, call IV 0.53 vs put IV 0.25 at 688, net gap to the provider -15.6% of gross with gross -0.2%).
`coverage.spot_check` reports the spot, the median `underlying_price` of the capture's rows and the gap; from 10 bp
the read raises `SPOT_QUOTE_GAP`. The fix belongs to the capture (follow-ups).

## 4. Quality reasons and states

| Reason | When | Page |
|---|---|---|
| `NO_CHAIN_CAPTURE` | No breadth rows for the name and session | "No chain capture yet" |
| `SPOT_UNAVAILABLE` | Capture has no spot | Unavailable |
| `NO_CONTRACTS_IN_SCOPE` | No expiry in the scope (e.g. 0 DTE on a stock's non-expiry day) | "Nothing expires today", button to <= 7 days |
| `SCOPE_EXPIRED` | Contracts in scope are past their close (0 DTE after 16:00 New York) | "Today's expiries have closed", button to All |
| `NO_EXPOSURE_IN_SCOPE` | Contracts in scope, none with weight and a Greek | Unavailable |
| `LATEST_CAPTURE_NOT_COMPLETE` | A newer capture failed or was partial | Warning; data shown is the last complete capture |
| `WEIGHT_WITHOUT_GREEKS` | Less than 99% of the weight has a Greek | Warning with the share |
| `PROVIDER_CROSS_CHECK_DIVERGES` | Our and the provider's exposure differ by more than 10% (gross, or net as a share of gross) | Warning quoting the larger gap |
| `SPOT_QUOTE_GAP` | The capture's spot is more than 10 bp from the provider's underlying price on the quotes | Warning with both prices: short expiries least reliable |
| `INSUFFICIENT_CONTRACTS`, `INSUFFICIENT_GAMMA_CONTRACTS` | Fewer than 20 contributing contracts (5 for 0 DTE) | Warning: levels are fragile |
| `PROVIDER_IV_FALLBACK` | Some contracts use the provider's IV | Note with the count |
| `VOLUME_WEIGHT_ESTIMATE` | OI + volume weight | Note |
| `NO_FLIP_IN_SEARCH_RANGE`, `MULTIPLE_FLIP_CANDIDATES` | From `solve_gamma_flip` | Note |

API off (switch): the route answers 404 and the page says the read is not switched on yet. An Options publication is
needed for the cohort parameter (as on every Options page).

## 5. API

`GET /api/options/exposure?underlyer=SPY&greek=gamma|delta&weight=oi|oi_volume&scope=0dte|7d|all[&session_date=]`

- Switch `REALTIME_V2_OPTIONS_EXPOSURE_READ` on the API, default OFF (404). Per-Greek flag
  `REALTIME_V2_OPTIONS_EXPOSURE_GREEKS`, default `gamma,delta`; a Greek must be implemented and named to be served
  (charm, vanna answer 422 until phase 2 ships them). Invalid parameters answer 422.
- Code: `backend/realtime/options_exposure_read.py` (read, math, cache), `RealtimeOptionFlowRepository.exposure`
  (`portal_compatibility_repository.py`), route in `portal_api.py`; `black_scholes_delta` added beside
  `black_scholes_gamma` in `options/analytics/greeks.py`.
- Response (`options-exposure-read-v1`): `available`, `reason`, `underlyer`, `session_date`, `observed_at`, `spot`,
  `greek`, `weight`, `scope`, `convention`, `units`, `totals`, `levels`, `strikes[]` (strike, call, put, net, call/put
  OI), `expiries[]`, `grid[]` (sparse `[strike index, expiry index, call, put]`), `largest_levels[]`,
  `expiring_today_share`, `coverage` (contracts, skips by reason, weight coverage, provider-IV count, strike band,
  expiries, `spot_check`), `cross_check`, `capture` (newest capture state), `quality_reasons[]`, `greeks_enabled[]`, `underlyers[]`,
  `definitions`, `execution_permission: false`. Values are whole dollars; shares = dollars / spot on the page.
- **Reads per request**: the session (`max(session_date)` for the name, as the gamma read), the names with captures
  (`DISTINCT underlying` on `rt.option_breadth_captures`), the capture identity (`observed_at, count(*)` grouped, on
  the `(session_date, underlying, ...)` index), the newest capture's state; the rows of the newest capture (about 5,000
  for SPY) only when the capture is not cached.
- **Cache per capture** (like R47): the parsed capture (12 entries) and each Greek/weight/scope result (48 entries) are
  kept per process, keyed by (name, session, newest `observed_at`, row count). A new capture changes the key. Memory
  (measured on SPY, the largest chain): ~0.9 MB per capture, ~0.5 MB per result, so at most ~35 MB per API process.
- Read time (Nov 12 recorded chains, file-backed cursor, this laptop): SPY cold 150-190 ms including parsing 5,068
  rows; each further view 25-110 ms; repeat reads ~30 ms (almost all of it the test cursor scanning 18,000 rows in
  Python). SQL on Postgres (`g102-test-pg`, Nov 13 SPY, EXPLAIN ANALYZE): all five statements on existing indexes,
  about 4 ms of execution per request (capture counts 2.0 ms over 4,872 rows, chain rows 1.5 ms for 4,802 rows once per
  capture, the rest under 1 ms); no new index. Payload: SPY ~100 KB JSON (272 strikes, 2,376 cells), gzipped by the
  portal's nginx.

## 6. Frontend

`frontend/src/pages/OptionsOverviewPage.tsx` (+ `.css`), logic in `optionsExposureModel.ts` (pure, tested), reader
`getOptionExposure` in `services/api.ts` (Options cohort parameters via `optionParams`, refused when Options stored
reads are off, as on the other Options pages). Route `/options/overview` in both route sets; menu entry "Options
Overview" first under Options. Reads when the next chain capture lands (`msUntilWindowRead` on `observed_at`: first
check 50 s after each 5-minute mark), 60 s when none is expected; the previous view stays on screen while a new choice
loads.

- **Chart**: hand-drawn SVG (strikes are categories, not time, so `lightweight-charts` does not fit), recessive grid,
  emphasised zero line, labelled markers stacked so close levels (spot, flip, put wall within a few dollars on SPY) do
  not collide, hover tooltip per strike with calls, puts, net and open interest, and a table view.
- **Colours**: the portal's call-green/put-red pair fails the colour-vision check (deutan delta E 4.8), so exposure
  polarity uses its own pair, validated on the portal's surfaces: dark `#3b8fdc` / `#d4722a`, light `#2b78c2` /
  `#c4611c` (all checks pass). Blue = positive / calls, orange = negative / puts; puts are hatched in the Calls vs puts
  view and every heatmap cell carries its signed value, so colour is never the only cue. Heatmap: neutral midpoint,
  scale ending at the 98th percentile of shown cells.
- **Charm, Vanna** buttons are disabled with a tooltip until the API lists them in `greeks_enabled`.

## 7. Evidence (Nov 16, offline on recorded Nov 12 chains)

Script `runner/ov1_exposure_evidence.py` runs the read through a file-backed cursor answering the same statements over
`research/chains/2026-11-12/{1905,close}.csv.gz` (full copies of `rt.option_breadth_current`). Outputs in
`evidence/ov1-20261116/`. Summary (details in `G106_EVIDENCE.md`):

- **Hand checks, 3 strikes x 3 names x 2 Greeks x 2 captures = 36: all equal to the dollar.** Strikes: call wall
  (call-heavy), put wall (put-heavy), the strike nearest the flip. Hand values from the stored `local_gamma` /
  `local_delta` x OI x 100 x scale x sign; contracts priced with the provider's IV (one at each QQQ strike checked,
  one at NVDA 185) by the closed form.
- SPY 19:02 (spot 758.59): net gamma -$4.72B per 1% (-6.2M shares), flip 760.96 (+0.31%), negative regime, call wall
  775, put wall 760; 16% of gamma expires today. QQQ (688.00): -$2.27B, flip 692.37, walls 700 / 690. NVDA (163.94):
  -$233M, flip 176.50, walls 185 / 160, no 0 DTE (NO_CONTRACTS_IN_SCOPE).
- Provider cross-check (gross): gamma -1.5% / -1.9% / -1.7%, delta -0.1% / -0.2% / +0.7% (SPY / QQQ / NVDA).
- Existing gamma read on the same capture: net within 1-2% (it evaluates gamma at 21:00 UTC / 365.25-day time from the
  read time, not the time `local_iv` was solved with), flips equal within $0.02 (SPY, QQQ) and $0.14 (NVDA).
- Screenshots (after the close, `runner/ov1_screenshots.sh`, real API app over the 19:05 copy, viewport set over
  DevTools): SPY gamma net, SPY delta calls vs puts, QQQ 0 DTE (both warnings), NVDA OI + volume, NVDA 0 DTE empty
  state, SPY light theme, SPY phone 390 px; no page-level horizontal scroll at any width.
- SQL on `g102-test-pg`: about 4 ms of execution per SPY request on existing indexes (section 5).

## 8. Deploy (after a close, with the batched portal release)

1. API image built from a tree with this branch merged; `REALTIME_V2_OPTIONS_EXPOSURE_READ=ON` on
   `realtime-v2-advanced-api` (leave `REALTIME_V2_OPTIONS_EXPOSURE_GREEKS` unset = gamma,delta). Rollback: the switch
   OFF (404), no data change. No migration, no new table, no writer.
2. Frontend in the next batched portal release (memory: batch portal fixes): route `/options/overview`, menu entry
   under Options. The page shows "not switched on yet" until step 1.
3. Check after deploy: `GET /api/options/exposure?underlyer=SPY` 200 with `available: true` during the session; the
   page for SPY, QQQ, NVDA; read time in the API log.

## 9. Follow-ups (not in phase 1)

- **OV1-F2, spot for local Greeks**: the capture solves `local_iv` at the equity bar's spot, which lags the quotes by
  10-60 bp in many captures (Nov 12: 14 of 33 over 10 bp). Using a price taken with the quotes (the provider's
  `underlying_price`, or the put-call parity forward per expiry) would fix 0 DTE IVs and Greeks for OV1 and the
  O-lanes alike. A capture change: needs its own replay before it ships.
- The existing gamma read evaluates gamma at a different time to expiry than `local_iv` was solved with (1-2% on
  totals). Align it with `_time_to_expiration_years(expiry, observed_at)` when A3 is next touched.
- A "holder view" for delta (calls +, puts -, no dealer sign) if the user wants the reference tool's delta reading.
- Intraday history of exposure (per-window snapshots, design 10.1 / H2) to replay levels against moves before any
  detector uses them.

## 10. Phase 2: Charm and Vanna

Same rows, model and inputs (our `local_iv`, the capture's spot, stored rate r and dividend yield q, time to expiry T as
above), closed-form Black-Scholes-Merton second-order Greeks. With
d1 = [ln(S/K) + (r - q + sigma^2/2) T] / (sigma sqrt T), d2 = d1 - sigma sqrt T, phi the normal density, N the normal
distribution:

| Greek | Formula | Unit used |
|---|---|---|
| Vanna = d(delta)/d(sigma) | -e^(-qT) phi(d1) d2 / sigma (calls and puts) | x 0.01: change in delta per 1 vol point |
| Charm (call) = -d(delta)/dT | q e^(-qT) N(d1) - e^(-qT) phi(d1) [2(r-q)T - d2 sigma sqrt T] / (2 T sigma sqrt T) | / 365: change in delta per calendar day |
| Charm (put) | -q e^(-qT) N(-d1) - e^(-qT) phi(d1) [2(r-q)T - d2 sigma sqrt T] / (2 T sigma sqrt T) | / 365 |

Exposures: vanna exposure = s_i x vanna_i x 0.01 x w_i x 100 shares per vol point (x S for dollars); charm exposure =
s_i x charm_i / 365 x w_i x 100 shares per day (x S for dollars). Built as `black_scholes_vanna` / `black_scholes_charm`
beside gamma and delta, tested against finite differences of `black_scholes_delta`; served when added to
`IMPLEMENTED_GREEKS` and named in `REALTIME_V2_OPTIONS_EXPOSURE_GREEKS`. Levels stay gamma levels. 0 DTE contracts near
the money carry very large charm; the page will show the 0 DTE share beside it. No alerts or detectors on any of these
without a replay that shows value.
