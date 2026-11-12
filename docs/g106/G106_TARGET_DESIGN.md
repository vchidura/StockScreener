# G106 target design: Options platform for a 50-ticker universe (full chains, streamed contracts, detectors)

G106 (opened Nov 12 2026 at the user's request) is the Options expansion track, separate from G102, which completes the
work planned through last week (V3-1 runtime, O3 night 2, OA1/OA2, portal rows). Tracker: `G106_IMPLEMENTATION_MATRIX.md`;
plan: `G106_PLAN.md`; evidence: `G106_EVIDENCE.md`; first-day note: `G106_PLAN_20261112.md`. Draft of Nov 12.

User, Nov 12 ~17:35 UTC: a deep analysis of how the platform uses the provider's full option chains and streamed
contracts, and a strategy for a 50-ticker universe covering the scanners and detectors of
`docs/OPTION_PACKAGE_STRUCTURE_AND_STRATEGY_EVALUATION.md`, with more detectors across structures and objectives/styles;
questions welcome. This is a design draft for the user's decisions (section 9). Nothing changes live from it.
Companion: `G106_PLAN_20261112.md` (V4, O1 diversity, evaluation of the two earlier docs).

**Principle: scan wide, watch narrow, confirm with the stock.** Full chains find what is unusual on every contract of
every name; streamed contracts prove how it traded; the stock stream says whether the move supports it. Tradable alerts
need all three; anything less is an observation.

## 1. What the provider gives us (and what to verify)

| Capability | What it is | Limit | Status for us |
|---|---|---|---|
| Options **quotes** stream | Every NBBO change per subscribed contract, real time | **1,000 contracts per connection** (provider docs); one Options connection (a second fails) | Using 72 |
| Options **trades** stream | Every trade per subscribed contract (size, price, exchange, conditions) | Not stated in the docs we hold | Subscribed with quotes for the same 72 |
| **Chain snapshot** (REST) | Per contract: day volume, prior-settlement OI, last trade, NBBO, IV, Greeks; ~250 contracts per request | Our own ceiling 64,000 requests/day (`MASSIVE_ADVANCED_ACCOUNT_REQUEST_CEILING`) | Every 5 min, 0-60 DTE, +/-20% strikes |
| Per-contract trades / quotes history (REST) | Recent trades and quotes of one contract | Request budget | **Not used; entitlement to verify** |
| Contract reference, aggregates | Listings, expiries; minute/day bars per contract | Request budget | Listings used at preparation |
| Stocks stream + REST | All equities' trades/quotes/bars; short interest, float, fundamentals | Stocks entitlement | Live for the Equity universe (25) |
| Index values (VIX, VVIX) | Index feed | **Entitlement unknown** | Not used |

To verify read-only before the design is final (Q5): whether a trades-only subscription can cover more contracts than
the 1,000 quote limit, the per-contract trades/quotes REST entitlement, index entitlement, and whether a second Options
connection can be bought.

## 2. The two data paths today, measured (Nov 12)

| | Full chains (breadth) | Streamed contracts (depth) |
|---|---|---|
| Contracts | 17,827 per window across 9 names (SPY 5,054, QQQ 5,379, IWM 2,381, MSFT 1,236, TSLA 1,115, AAPL 877, AMZN 728, NVDA 657, NFLX 400) | 72, fixed at preparation (C6 v2) |
| Cadence | One snapshot per 5-minute window | Every trade and quote change, real time |
| Cost | ~78 requests and ~17 MB per window; 1.5-14 s per name (sequential); ~2,900 requests by 17:33 | Market-state ~16% of a core (after V3-1.6), flow ~6-8%; journal ~3.4 GB by 17:13 (~8 GB/day) |
| Stored | `rt.option_breadth_current`: one row per contract, overwritten each window (no intraday history) | Raw: journal file on disk. Database: retained flow events, marks, detector evaluations, packages, alerts, checkpoints |
| Knows | Volume, OI, NBBO, IV/Greeks at the snapshot | Each trade's size, time, side (at ask / at bid / inside), sweeps, blocks, clusters |
| Cannot know | Who bought or sold, when inside the 5 minutes, sweeps | Anything about the other ~17,750 contracts |
| Feeds | Options Flow, Activity, Screener, Unusual-activity screens | Options Alerts (O1, OS1, OS2; O3 soon), marks, trade tape |

## 3. Target architecture for 50 names

```text
Layer C  Stock stream (all 50)  -> trend, levels, setups, earnings calendar, HV, short interest
Layer A  Chain scan (all 50, every 5 min, 0-60 DTE +/-20%)
           -> unusual volume/OI, premium leaders, IV surface (term, skew), OI walls / dealer gamma, expected move
           -> OBSERVATIONS (screens, Alerts "observations" section)  +  PROMOTION FLAGS
Layer B  Stream (one connection, 1,000 quote slots)
           core set   ~600  chosen at preparation from yesterday's + pre-open chain activity (V4 buckets, ~12/name)
           promotion  ~250  filled each window from Layer A flags (warm-up, evict quietest, never an alert leg)
           reserve    ~150  second legs of packages, open alerts' legs, confirmations
           -> trade-level evidence: aggression side, sweeps, blocks, clusters, repeat buying
Layer D  Detectors (registry; each declares inputs A/B/C/H, objective, structures, cadence)
           -> candidates -> package builder (structures) -> evaluation ladder -> ranking + caps
           -> TRADABLE ALERTS (needs B evidence + package + direction/range context from C)
Layer H  History (new): daily close chain snapshot per contract; IV per name/tenor; OI day-over-day
           -> IV rank/percentile, OI change (opening confirmation), outcome research
```

The promotion pool is the key new capability: today the streamed set cannot change during the session, so activity on
any of the other contracts can never become a tradable alert. With promotion, Layer A finds the NVDA-like contract on
any name within one window and Layer B starts proving it from the next.

## 4. Capacity for 50 names (estimates; gates measured on a copy)

| Item | 9 names today | 50 names (estimate) | Gate / mitigation |
|---|---|---|---|
| Chain contracts per window | 17.8k | ~45-55k (3 big index ETFs ~13k, ~10 sector ETFs ~8k, ~37 stocks ~30k) | List of 50 (Q1) fixes the number |
| Chain requests | ~78 / window, ~6k / day | ~200-230 / window, ~16-18k / day | Under the 64k/day ceiling; no raise needed |
| Chain capture time | ~50 s sequential | ~3 min sequential | Parallel pages (4-6 workers) to finish in ~60 s; per-name degrade on failure (exists) |
| Chain storage | 18k current rows | ~50k current + ~50k/day close history (~20 MB/day) | New history table (Layer H) |
| Streamed contracts | 72 | 600 core + 250 promotion + 150 reserve = 1,000 | Provider ceiling; more needs a second connection (Q5) |
| Market-state / flow CPU | ~16% / ~7% of a core | messages grow ~8-12x (estimate) -> 1.5-2 cores | V3-1.6b (18:13 today), then V3-1.4 shards across processes; V3-1.3 replay at 3x first |
| Journal | ~8 GB/day | ~60-90 GB/day raw | Retention days for raw journals (Q6), compressed archive, quote conflation for promotion-pool contracts |
| Options window run time | 13-37% of 300 s | must stay under ~50% | Per-detector budgets; window profile (hash/JSON of package evidence is today's largest cost) |

## 5. Detector and scanner catalog

Objectives/styles: **D** directional, **I** income (sell premium, defined risk), **V** volatility, **E** event,
**P** positioning context. Inputs: A chain, B stream, C stock, H history. Output: OBS observation, PKG tradable package.

| Id | Detector / scanner | Style | Inputs | Output and structures | Status | Phase |
|---|---|---|---|---|---|---|
| O1 | Volume/OI participation (day vol >= 3x prior OI + print rules) | D | B (+A for promotion) | PKG long call/put | Live, 72 contracts; cap missing (O1D) | P0 cap, P1 promotion |
| OS1 / OS2 | Stock-led signal -> option package | D | C + B | PKG long call/put | Live | P1 debit-spread variant |
| O3 | Range locator: credit vertical from OI concentration + trend | I | A + B + C | PKG put/call credit vertical | Night 2 tonight (R44) | P0-P1 |
| O2 | IV smile distortion | V | B (A later) | OBS | Running, model not accepted | P3 after calibration |
| U-SCAN | Unusual activity (vol/OI >= 3, prior OI >= 500, tier minimums) | D/P | A | OBS + promotion flag | Screens live | P1 observations section |
| F-SWEEP | Multi-exchange sweep cluster, short-dated | D | B | PKG long option / debit vertical | Flow profile S exists, no detector | P2 |
| F-BLOCK | Large single prints at/through the ask or bid | D | B | PKG / OBS | Profile B exists | P2 |
| F-ACCUM | Same-side buying in one contract across windows | D | B (+H) | PKG swing debit vertical | Profile A exists | P2 |
| F-0DTE | Index 0DTE gamma activity | D | B + C | PKG long option (0DTE) | G101 | P2 with V4 0DTE buckets |
| V-TERM | Term-structure inversion (front IV / back IV > 1.3) | V/E | A | OBS; later calendar spread | New | P1 (cheap) |
| V-SKEW | 25-delta put/call skew extreme vs own history | V/D | A + H | OBS; later risk reversal / vertical | New | P1 obs, P3 with history |
| V-RICH | IV well above HV20/HV30 (variance premium) | I/V | A + C | PKG credit vertical / iron condor (with O3 locator) | New | P2 |
| V-CHEAP | IV below HV or low IV rank | D/V | A + C + H | PKG long option / debit vertical / calendar | Needs ~20+ sessions of history for rank | P3 |
| E-EARN | Earnings: expected move (ATM straddle) vs history, IV crush after | E/I | A + C (calendar) | OBS before; PKG condor/credit after the event | New (calendar live) | P2 |
| P-WALL | OI walls and dealer gamma by strike (pin/magnet levels) | P | A | OBS; feeds O3 and stock targets | New | P1 |
| P-OICHG | Day-over-day OI change confirming opening flow | P | H | Confirms/refutes yesterday's O1 alerts | New (needs Layer H) | P1 start storing, P2 use |
| R-REGIME | Universe put/call volume ratio, average skew, (VIX/VVIX if entitled) | P | A (+index) | Regime filter for all detectors | New | P2 |
| I-WHEEL | Cash-secured put on a name at support with rich IV | I | A + C | PKG cash-secured put (research) | Family registered, no detector | P3 |
| Parked | Put-call parity deviation; silent LEAP accumulation; dark-pool convergence; <50 ms gamma squeeze | - | - | - | Low value or data we lack | Revisit after P3 |

Structures by objective (what the package builder must cover): directional = long call/put, debit verticals; income =
credit verticals, iron condor, cash-secured put; volatility = long straddle/strangle, calendar/diagonal; event = condor
or credit spread after the event, straddle observation before it. Every package keeps the existing rules: exact legs,
signed economics recomputed, execution false, the evaluation ladder (OBSERVATION / INDICATIVE / QUOTE_PAPER /
EXECUTION).

## 6. Alert policy across 50 names

- Two products: **observations** (Layer A; up to 3 per name and 20 per window, ranked by premium) and **tradable alerts**
  (Layer B evidence + package + context; at most 3 new per name per run and 20 per run; one row per contract and side).
- Ranking (flow policy order): complete evidence, aggression confidence and consistency, premium and size, cluster
  strength and exchange diversity, volume/OI, freshness; plus a confirmation bonus when two independent detectors agree
  (O1 + OS2, F-SWEEP + stock breakout).
- Promotion pool priority: contracts flagged by U-SCAN with the highest premium per name, round-robin across names so
  one name cannot take the pool; a promoted contract stays at least 3 windows; open alerts' legs are never evicted.
- Every detector reports its lane state each window (as today), so a silent detector is visible.

## 7. Data model additions

| Addition | Purpose | Size |
|---|---|---|
| `option_breadth_daily` (close snapshot per contract) | IV rank/percentile, OI change, skew history, outcome research | ~50k rows/day |
| IV summary per name/tenor per window (ATM IV 7/30/60 DTE, 25-delta skew, expected move) | V-TERM, V-SKEW, E-EARN, regime; cheap to read | ~50 x 78 rows/day |
| Promotion ledger (contract, window promoted/evicted, reason, slot) | Coverage accounting; replay of what was watched when | ~1-2k rows/day |
| Detector registry rows (inputs, cadence, caps, policy hash) | One place to turn detectors on/off per session | small |

Start Layer H now: history has value only once it has accumulated, so the daily close snapshot should be the first
thing built.

## 8. Phased roadmap

| Phase | Scope | Gate |
|---|---|---|
| P0 (this week) | O1D.1 per-name cap; Layer H daily close snapshot; O3 night 2; V4.1 profile + V4.2 capacity replay (V3-1.3) | Replays on Nov 10-12 data |
| P1 (next week, 9 names) | V4 depth; observations section (U-SCAN, V-TERM, P-WALL); promotion pool design + copy rehearsal with a replayed day; IV summary table | Promotion keeps Options run under 50%, no eviction of alert legs |
| P2 (25 names = the Equity universe) | Chain scan 25 (parallel pages), stream 25 x ~16 + promotion; F-SWEEP/F-BLOCK/F-ACCUM, V-RICH, E-EARN, R-REGIME; market-state shards (V3-1.4); journal retention | CPU and window time on a copy at 25; user's universe yes |
| P3 (50 names) | Chain scan 50, stream 600/250/150; V-CHEAP, V-SKEW with history, I-WHEEL; O2 acceptance | 20+ sessions of Layer H; capacity on a copy at 50 |
| P4 (continuous) | Outcome research per detector in the Model Lab from archived data (hit rate, move after the alert by structure, calibration; no live paper P/L, user Nov 12); retire weak detectors; accept models | Out-of-sample evidence |

## 9. Decisions needed from the user (answered Nov 12, section 10)

1. **The 50 names**: which tickers (the 25 Equity members plus which 25)? How many index/sector ETFs? This sets the
   chain size (about 45-55k contracts per window) and the per-name stream share.
2. **Objective priority**: rank directional intraday (0-2 DTE), directional swing (1-3 weeks), income (defined-risk
   premium selling), volatility/event. This orders P1-P3 and the stream buckets.
3. **Alert volume**: roughly how many tradable alerts per day you want to review (for example 10-30), and whether
   observations should sit on the Alerts page or only in the Screener.
4. **0DTE**: index ETFs only, or every name with daily expiries?
5. **Provider checks**: may I run read-only calls to verify trades-only wide subscription, per-contract trades/quotes
   REST, index entitlement, and the price of a second Options connection?
6. **Storage**: how many days of raw journals to keep at ~60-90 GB/day (disk: C: 368 GB free, Docker volume 852 GB),
   and archive or drop after?
7. **Paper tracking**: should every tradable alert get quote-paper P/L tracking to grade detectors (needed for P4)?
8. **Request budget**: keep the 64,000/day ceiling (enough for 50 names at ~18k/day), or reserve headroom for
   promotion-pool REST trades checks?

## 10. User decisions (Nov 12 ~18:00 UTC) and what they change

| # | Decision | Effect on the plan |
|---|---|---|
| 1 | The 50 names are not chosen yet | Not needed before P3. Design every budget as a per-name formula (chain pages, stream share, promotion share); only the ETF/stock mix moves the estimates (big index ETFs carry ~5k contracts each, single stocks ~400-1,200) |
| 2 | Objective order: directional intraday, directional swing, income, volatility/event | Phases follow it: P1-P2 intraday (O1 + promotion, F-SWEEP, F-BLOCK, F-0DTE), then swing (F-ACCUM, OS1/OS2 debit spreads, P-OICHG), then income (O3, V-RICH, I-WHEEL), then volatility/event (V-TERM, E-EARN, V-SKEW, V-CHEAP) |
| 3 | No alert count limits, but alerts must cover the universe, not concentrate on one name | Fairness instead of totals: at most 3 new alerts per name per run, names served round-robin by rank, a promotion pool shared round-robin; a coverage metric per session (names with an alert or observation per hour) reported by the watcher |
| 4 | 0DTE: indexes yes; stocks on their weekly expiry day | V4 buckets: index ETFs keep the daily 0DTE bucket; single stocks get a 0DTE bucket only on the day one of their expiries ends (Fridays, and other listed weekdays where they exist) |
| 5 | Read-only provider checks allowed; REST calls are unlimited | REST checks after the close (per-contract trades/quotes, index snapshot, rate behaviour). Stream checks (trades-only beyond 1,000, a second connection) never during a session: a second Options connection can disconnect the live one; run them on a weekend with the stream stopped, or ask the provider. Unlimited REST makes the "fetch trades on demand" route cheap: recent trades + quotes of each flagged contract every window give side and urgency without a stream slot |
| 6 | Journals: my recommendation; needed for the Model Lab and backtests? | Yes (see 10.1) |
| 7 | No paper P/L tracking | Detector grading is offline in the Model Lab from archived data (10.1), not live paper positions; P4 changes accordingly |
| 8 | Request headroom: my recommendation | Keep a self-imposed guard against runaway loops, sized well above need: ~30k/day expected at 50 names (chains ~18k, preparation ~6k, on-demand trade checks ~4-6k) -> guard 150k/day plus a per-minute limiter; applying it is a ceiling change at the build that needs it |

### 10.1 Raw journals and research data (recommendation)

The raw journal is the only complete record of every trade with the NBBO it traded against, which is what side
classification, sweeps and every flow detector need; backtesting a new detector means replaying it (as the V3-1.6b
proof replayed Nov 11). Keep three tiers:

| Tier | What | Retention | Size (72 contracts today / ~1,000 at 50 names) |
|---|---|---|---|
| Hot | Raw journal files on the journal volume | 10 sessions | ~8 GB/day / ~60-90 GB/day |
| Archive | The same files compressed (zstd) on an archive disk | 12 months, then review | ~1-2 GB/day / ~10-15 GB/day (~3-4 TB/year at 50 names) |
| Research datasets (Model Lab) | Built nightly from the journal and chains: every trade with its NBBO at trade time and side; 1-minute NBBO bars per contract; every 5-minute chain snapshot (all contracts) and the daily close | Permanent | ~0.3-0.6 GB/day at 50 names (Parquet) |

The research datasets are what backtests read day to day; the archive is for rebuilding them when a detector needs
something new. The per-window chain snapshots (not only the daily close) are worth keeping permanently: they let chain
detectors (U-SCAN, V-TERM, P-WALL, GEX) and the promotion pool be replayed exactly.

## 11. Existing analytics to reuse (not in the live realtime path today)

`backend/options/analytics` already implements most chain analytics; only the older developer ("forward") pipeline
uses them (`realtime/forward_ingestion.py` -> `OptionAnalysisEngine`; gamma profiles were stored in
`option_gamma_profiles`, migration 021, in that pipeline's database, not in the live realtime database). The advanced
realtime path uses only `greeks`, `marks`, `alert_selection`, `eligible_chain_projection`, `flow_projection`,
`contract_filters` and parts of `chain_analysis`.

| Module | What it computes | Where it goes in this plan |
|---|---|---|
| `gamma_exposure` | Per-strike gamma exposure (GEX), dealer-sign convention kept separate as a hypothesis, gamma flip point, gamma walls | Layer A per name per window from breadth rows (OI + our local IV); feeds P-WALL, O3's range locator, 0DTE pin risk (F-0DTE), regime (negative dealer gamma -> trend-day risk for intraday alerts), and target/stop levels next to stock levels on the ticker page |
| `oi_walls` | OI concentration clusters | P-WALL, O3 |
| `open_interest_change` | OI build/unwind classified with volume confirmation, ranked builds | P-OICHG (needs the daily close history, 10.1): confirms yesterday's O1 alerts as opening |
| `iv_context`, `iv_comparability` | Current comparable IV against its lookback (relative IV / IV rank inputs), with comparability rules | V-CHEAP, V-RICH, E-EARN, alert context ("IV at 85th percentile") once history exists |
| `volatility_risk_premium`, `realized_volatility` | Matched-maturity ATM IV vs realized volatility (variance risk premium) | V-RICH / V-CHEAP from day one (needs no IV history: HV comes from stock bars) |
| `smile`, `surface_followup` | Smile residuals, surface follow-up | O2 (running, not accepted), V-SKEW |
| `options/strategies` (`engine`, `gates`, `payoff`, `scenarios`, `registry`) | Strategy gates incl. gamma walls, payoff and scenarios per structure | The package builder for debit/credit verticals, condors, cash-secured puts |

User, Nov 12 18:10: the legacy forward pipeline does not run from this code base, so these modules can be copied
into the realtime path and updated directly; no compatibility with the old pipeline is needed. Wiring them to the breadth rows is mostly adapters (inputs: strike, type, expiry, OI, local IV, rate, dividend), so
GEX, OI walls and the variance risk premium can be P1 observations for all 9 names, before V4.

## 12. Further questions to sharpen the plan

1. **Intraday exits**: should intraday alerts carry a model target and stop (OA2) and expire at the session close?
2. **Liquidity floor for alerts**: minimum OI / day volume / bid-ask spread (for example spread under 10% of mid)?
3. **Price floor**: skip very cheap contracts (for example under $0.20) as lottery tickets, or keep them?
4. **Income structures**: defined risk only (credit verticals, condors), or cash-secured puts too?
5. **Earnings**: suppress directional alerts that span an earnings date, or label them?
6. **Delivery**: portal only, or later phone/e-mail notifications for chosen detectors?
7. **Archive disk**: where should the journal archive live (a second internal disk, external drive, NAS, cloud)?
8. **Model Lab**: run detector backtests nightly on this machine after the close, or on demand?
