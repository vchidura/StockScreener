# G106 implementation matrix

G106 = the Options platform expansion toward a 50-ticker universe (user, Nov 12 2026): scan wide with full chains,
watch narrow with streamed contracts, confirm with the stock; detectors across directional, income, volatility and
event styles; history and research data for the Model Lab. Design: `G106_TARGET_DESIGN.md` (section numbers below).
Plan: `G106_PLAN.md`. Evidence: `G106_EVIDENCE.md` (dated entries). First-day note: `G106_PLAN_20261112.md`.

**Scope split with G102** (user, Nov 12): G102 completes what was planned through last week and is in progress (V3-1
runtime slices incl. V3-1.3 capacity replay and V3-1.4 shards, O3 night 2 (R44), OA1/OA2, R-rows, portal rows); G106
owns everything new for Options coverage, detectors and data. Where a G106 row depends on a G102 row it names it; it
does not duplicate it. Rows moved from G102 on Nov 12: G75 V4 -> B1, O1D -> D1, OPT50 -> this matrix.

**Statuses** as in G102: DESIGNED, IMPLEMENTED (code exists, not proven live), VERIFIED (proven live or on a copy,
evidence named), LIVE (running, verification pending), DEFERRED (reason and trigger given). **Phases** (design 8):
P0 this week, P1 next week (9 names), P2 25 names, P3 50 names, P4 continuous research. Objective order (user):
directional intraday, directional swing, income, volatility/event.

Last updated: 2026-11-12 18:10 UTC (opened).

## 1. Decisions and checks

| ID | Item | Phase | Status | Next |
|---|---|---|---|---|
| Q1 | The 50 names (user: not chosen yet; not needed before P3; budgets written per name) | P3 | OPEN | Choose before P3; the ETF/stock mix sets chain size |
| Q2 | Further questions (design 12): intraday exits, liquidity floor, price floor, income structures, earnings, delivery, archive disk, Model Lab schedule | P0-P1 | OPEN | User answers shape D-rows and H3 |
| C1 | Provider REST checks, read-only: per-contract trades/quotes history, index snapshot (VIX/VVIX), rate behaviour (user: allowed; REST unlimited) | P0 | DESIGNED | After the close Nov 12; result in evidence |
| C2 | Provider stream checks: trades-only subscription beyond the 1,000-quote limit; a second Options connection | P1 | DEFERRED | Never during a session (a second connection can drop the live one); a weekend with the stream stopped, or ask the provider |
| C3 | Request guard 150k/day + per-minute limiter (user: my recommendation; expected ~30k/day at 50 names) | P1 | DESIGNED | Applied with the build that needs it (ceiling change) |

## 2. Data layers

| ID | Item | Design | Phase | Status | Next |
|---|---|---|---|---|---|
| A1 | Chain scan (breadth) for every universe name each window; parallel pages (4-6 workers) to finish in ~60 s at 50 names; per-name degrade | 3, 4 | P2 | LIVE for 9 names (G75, sequential ~50 s) / DESIGNED parallel | Parallel pages before 25 names |
| A2 | IV summary per name and tenor per window (ATM IV 7/30/60 DTE, 25-delta skew, expected move) | 7 | P1 | DESIGNED | Table + writer in the chain scan |
| A3 | Analytics adapters on chain rows, reusing `backend/options/analytics`: gamma exposure (profile, flip, walls), OI walls, variance risk premium, realized vol; IV context once history exists | 11 | P1 | DESIGNED (modules exist; used only by the older forward pipeline) | User, Nov 12 18:10: the legacy pipeline does not run from this code base, so the modules may be copied into the realtime path and changed directly (no compatibility kept for the forward pipeline). Build on chain rows + tests on recorded chains |
| A4 | Observations product: Alerts page section (and Screener), up to 3 per name and 20 per window | 6 | P1 | DESIGNED (was G102 O1D.2) | API read + frontend, portal batch |
| H1 | Daily close chain snapshot per contract (`option_breadth_daily`) | 7, 10.1 | P0 | DESIGNED | First build: history only has value once accumulated |
| H2 | Research datasets, nightly Parquet: every trade with NBBO at trade time and side; 1-minute NBBO bars per contract; every 5-minute chain snapshot; daily close | 10.1 | P1 | DESIGNED | Builder after the close; Model Lab reads it |
| H3 | Raw journal retention: hot 10 sessions, archive zstd 12 months | 10.1 | P1 | DESIGNED | Archive disk (Q2.7) |
| H4 | Promotion ledger (contract, window promoted/evicted, reason, slot) | 7 | P1 | DESIGNED | With B3 |

## 3. Streamed contracts

| ID | Item | Design | Phase | Status | Next |
|---|---|---|---|---|---|
| B1 | V4 depth: C6 profile v4 (index ETFs 0DTE 6 / 1-2 DTE 6 / weeklys 8 / core 4; stocks weeklys 8 / core 12 / ext 4; stock 0DTE bucket on its expiry day; weekday ladder; one call/put quality ranking) | 2 of the first-day note | P0 build, P1 live | DESIGNED (not coded; live is v2, 72 contracts) | V4.1 profile tonight; V4.3 preparation Fri Nov 14 at the earliest / Mon Nov 16 with the user's yes. Depends on G102 V3-1.3 |
| B2 | Stream split at 50 names: core ~600 (preparation), promotion ~250, reserve ~150 | 3, 4 | P3 | DESIGNED | Per-name formula; depends on G102 V3-1.4 shards |
| B3 | Promotion pool: chain flags promote contracts into the stream during the session (warm-up, round-robin across names, minimum 3 windows, never evict alert legs) | 3, 6 | P1 design + copy rehearsal, P2 live | DESIGNED | Design note, then a replayed-day rehearsal |
| B4 | On-demand REST trades/quotes for flagged contracts each window (side and urgency without a stream slot) | 10 #5 | P1 | DESIGNED | After C1 confirms the endpoints |

## 4. Detectors and scanners (design 5)

| ID | Item | Style | Inputs | Phase | Status | Next |
|---|---|---|---|---|---|---|
| D1 | O1 per-name cap: at most 3 new alerts per name per run, policy ranking order; repeat hits uncapped (was G102 O1D.1) | D | B | P0 | DESIGNED | Switch default off; replay Nov 10-12; live with the user's go |
| D2 | O1 over promoted contracts (B3) and on-demand trades (B4) | D | A+B | P1-P2 | DESIGNED | After B3/B4 |
| D3 | F-SWEEP: multi-exchange sweep cluster, short-dated | D intraday | B | P2 | DESIGNED (flow profile S exists) | Detector on retained clusters |
| D4 | F-BLOCK: large single prints through the ask/bid | D intraday | B | P2 | DESIGNED (profile B exists) | |
| D5 | F-0DTE: index 0DTE gamma activity (with GEX pin risk) | D intraday | B+A3+C | P2 | DESIGNED | Needs B1 0DTE buckets |
| D6 | F-ACCUM: same-side buying in one contract across windows | D swing | B+H | P2 | DESIGNED (profile A exists) | |
| D7 | OS1/OS2 debit-spread variant | D swing | C+B | P2 | DESIGNED | Package builder D-structures (AL2) |
| D8 | P-OICHG: day-over-day OI change confirming opening flow (reuse `open_interest_change`) | D swing / P | H1 | P1 store, P2 use | DESIGNED | Needs H1 history |
| D9 | V-RICH: IV well above realized vol -> credit vertical / iron condor (with O3 locator, G102 R44) | I | A3+C | P2 | DESIGNED | |
| D10 | I-WHEEL: cash-secured put at support with rich IV | I | A+C | P3 | DESIGNED | Q2.4 (income structures) |
| D11 | V-TERM: term-structure inversion (front/back IV > 1.3) | V/E | A2 | P1 | DESIGNED | Observation first |
| D12 | V-SKEW: 25-delta skew extremes vs own history | V/D | A2+H | P1 obs, P3 rank | DESIGNED | |
| D13 | E-EARN: earnings expected move vs history; IV crush after | E/I | A2+C | P2 | DESIGNED | Q2.5 (earnings handling) |
| D14 | V-CHEAP: IV below realized / low IV rank -> long premium, debit, calendar | V/D | A3+H | P3 | DESIGNED | 20+ sessions of H1 |
| D15 | P-WALL / GEX: OI walls, dealer gamma by strike, flip level; feeds O3, F-0DTE, ticker-page levels, regime | P | A3 | P1 | DESIGNED | Observation first |
| D16 | R-REGIME: universe put/call volume, average skew, dealer gamma sign, VIX/VVIX if entitled | P | A+C1 | P2 | DESIGNED | Filter for intraday alerts |
| D17 | U-SCAN observations + promotion flags (unusual vol/OI, premium leaders) | D/P | A | P1 | DESIGNED (screens live) | With A4 and B3 |
| D18 | O2 acceptance (smile distortion) | V | B (A later) | P3 | DESIGNED (running, not accepted) | Calibration evidence |
| D19 | Parked: put-call parity, silent LEAP accumulation, dark-pool convergence, <50 ms gamma squeeze | - | - | - | DEFERRED | Revisit after P3 |

## 5. Alert policy, packages, research

| ID | Item | Phase | Status | Next |
|---|---|---|---|---|
| AL1 | Fairness across names (user: no count limits, cover the universe): 3 new per name per run, round-robin by rank, coverage metric per session in the watcher | P0-P1 | DESIGNED | With D1; watcher line |
| AL2 | Package builder structures via `options/strategies` (debit/credit verticals, iron condor, cash-secured put; calendars later) | P2 | DESIGNED (engine exists) | After O3 (G102 R44) |
| AL3 | Model Lab grading per detector from H2 (hit rate, move after alert by structure, calibration); no live paper P/L (user) | P4 | DESIGNED | After H2 |

## 6. Universe

| ID | Item | Phase | Status | Next |
|---|---|---|---|---|
| U1 | 25 names = the Equity universe | P2 | DESIGNED | User's universe yes; G102 B10 (SPY/QQQ literals) first |
| U2 | 50 names | P3 | DESIGNED | Q1 |
