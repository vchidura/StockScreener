# G106 evidence

Dated entries, newest last. Matrix: `G106_IMPLEMENTATION_MATRIX.md`.

### Nov 12 (16:55-18:10 UTC): G106 opened; measured basis; user decisions

**Origin.** During the Nov 12 live session the user asked why Options alerts were all NVDA, whether V4 was in effect,
for an evaluation of `OPTION_PACKAGE_STRUCTURE_AND_STRATEGY_EVALUATION.md` and `ADVANCED_OPTIONS_FLOW_POLICY.md`, then
for a strategy for a 50-ticker universe, then (~18:05) for a separate track like G102. G103-G105 are taken (frozen gap
register; G102 matrix row G103), so the track is G106.

**Measured basis (live, read-only).** Full chains Nov 12: 17,827 contracts per window across 9 names (SPY 5,054, QQQ
5,379, IWM 2,381, MSFT 1,236, TSLA 1,115, AAPL 877, AMZN 728, NVDA 657, NFLX 400), ~78 requests and ~17 MB per
window, 1.5-14 s per name sequential; 2,916 chain requests by 17:33; `rt.option_breadth_current` 18,156 rows (one per
contract, overwritten each window; no intraday history). Streamed: 72 contracts (C6 v2; v4 not coded); journal 3.4 GB
by 17:13; market-state ~16% of a core after G102 V3-1.6. O1 by name to 16:37: evaluations / SELECTED NVDA 4,322 /
504, QQQ 246 / 4, AAPL 381 / 0, AMZN 217 / 0, NFLX 99 / 0, SPY 88 / 0, TSLA 67 / 0, MSFT 15 / 0, IWM 8 / 0; O1 rule
day volume >= 3x prior OI (`minimum_volume_oi_ratio=3.0`); the flow policy's declared caps (3 per underlyer, 20 per
session) are not enforced (only `advanced_options_o3_inputs.py` bounds per name). Provider: Options quotes 1,000
contracts per connection (provider docs as recorded in `ADVANCED_REALTIME_PREFLIGHT_DESIGN.md` 3); one Options
connection. `backend/options/analytics` has gamma exposure, OI walls, OI change, IV context, variance risk premium,
smile; only the older forward pipeline (`realtime/forward_ingestion.py`) uses them.

**User decisions (~18:00).** 50 names not chosen (not needed before P3); objectives: directional intraday, directional
swing, income, volatility/event; alerts: no count limits but cover the universe, not one name; 0DTE: indexes, and
stocks on their weekly expiry day; read-only provider checks allowed, REST unlimited; journals: as recommended (kept for
the Model Lab and backtests); no paper P/L; request headroom: as recommended; gamma exposure and relative IV exist
and should be used wherever they apply. Recorded in `G106_TARGET_DESIGN.md` 10-11.

**User, Nov 12 ~18:10:** the `backend/options/analytics` scripts can be copied and updated directly for A3 (gamma exposure, OI walls, OI change, IV context, variance risk premium); the legacy pipeline does not run through this code base. A3 and design 11 updated.

### Nov 12 (19:06 UTC, market open): C1 provider REST checks, read-only (user's yes)

Probe `scratchpad/provider_probe.py` in an r98 container with the provider key mounted (never printed); output
`scratchpad/ev/provider-probe-1912.txt`. **Per-contract trades** `/v3/trades/{option}`: HTTP 200 for a streamed contract
(NVDA 251113 155 P: newest trade 0.7 s old) and a not-streamed one (SPY 261218 718 P, 20,301 day volume: newest 18 s
old); fields `conditions, exchange, price, size, sip_timestamp, sequence_number`. **Per-contract quotes**
`/v3/quotes/{option}`: HTTP 200, real time, fields `bid/ask price, size, exchange, sip_timestamp`. So trade side and
urgency can be computed for any contract from REST without a stream slot (B4 feasible). **Single-contract snapshot**
`/v3/snapshot/options/{u}/{option}`: 200, 1.7-3.4 s. **Indices** `/v3/snapshot/indices` (I:VIX, I:VVIX, I:SPX): **403
NOT_AUTHORIZED** (not entitled; D16 uses our own SPY/QQQ IV and skew instead). **Rate**: header `x-ratelimit-limit
50000`, remaining 50000; 10 sequential trades reads all 200, latency 0.28 / 1.09 / 4.61 s (min/avg/max): ~50 contracts per
window needs parallel requests (8 workers ~ 15 s). Stream checks (trades-only beyond 1,000; second connection) not run
(C2: never in a session).

### Nov 12 (18:57-19:10 UTC): chain history started; A3 gamma/OI-wall prototype on live chains; AL1 coverage line

**Chain history (H1/H2 head start, read-only, no DB change):** `scratchpad/capture_chains.sh` copies
`rt.option_breadth_current` for the session after each window's chain capture (mark + 150 s) into
`recovery-20261104/research/chains/2026-11-12/HHMM.csv.gz` (ops folder, not in git) until the close, plus `close` at
21:05; first copy 18:57 UTC 18,180 contracts, 2.5 MB (all chain columns incl. provider and our IV/Greeks, spot, rate,
dividend). About 26 copies by the close (~65 MB). **A3 prototype** (`scratchpad/gex_proto.py`, r98 image, network none,
on the 19:05 copy; existing `options/analytics` `build_gamma_profile`, `detect_gamma_walls`, `detect_oi_wall_clusters`;
convention DEALER_LONG_CALLS_SHORT_PUTS as a hypothesis; IV ours else provider; output `scratchpad/ev/gex-proto.txt`):
SPY spot 762.64, net +$3.35B per 1%, POSITIVE, flip 760.95, 0DTE peak 762 (+$2.28B); QQQ 693.57, +$0.92B, POSITIVE,
flip 692.27, 0DTE peak 693; IWM 279.60, -$2.13B, NEGATIVE, flip 283.73; NVDA 166.85, -$0.18B, NEGATIVE, flip 175.42,
gamma walls 170/180/175, OI walls 200/190/185; AAPL +$1.13B POSITIVE (walls 360/355/350); AMZN +$0.73B (270/275/265);
MSFT +$0.62B; TSLA +$0.40B (400/380/390); NFLX -$18.6M NEGATIVE. Runs in seconds for 9 names; the modules work on chain
rows as they are (A3 = adapters + storage + reads). **AL1**: `scratchpad/coverage.sql` added to the watcher (O1 SELECTED
day / last hour and OPTION alerts per name); at 19:10: NVDA 580 / 26 / 5 alerts, AMZN 8 / 8 / 1, QQQ 5 / 0 / 1, AAPL
2 / 1 / 1, others 0.

Nov 12 21:05 UTC: chain history copies finished: 27 files (18:57 start, every window 19:00-21:00, `close` at 21:05),
18,180-18,210 contracts each, 65 MB in `recovery-20261104/research/chains/2026-11-12/` (log `capture.log`). The close
copy equals the 21:00 window's (no capture after 21:00). Input for H1/H2 and the Model Lab.

### Nov 12 night (~22:30 UTC): A3 gamma read implemented (switch off); correction to the 19:07 prototype; stale chain rows

**A3**: `realtime/options_gamma_read.py` (+ `RealtimeOptionFlowRepository.gamma`, route `GET /api/options/gamma?underlyer=`)
behind `REALTIME_V2_OPTIONS_GAMMA_READ` (default OFF: 404). Computes from the stored chain at read time with
`options/analytics` (`build_gamma_profile` TOTAL and ZERO_DTE, `detect_gamma_walls`, `detect_oi_wall_clusters`), cached
per capture; reasons NO_CHAIN_CAPTURE / SPOT_UNAVAILABLE / TOO_FEW_CONTRACTS; `execution_permission` false; convention
stated. Tests `tests/realtime/test_options_gamma_read_nov12.py` 5 passed (incl. a strike-format bug the test caught:
`Decimal.normalize()` printed 100 as `1E+2`). **Real-data check** on the 19:05 chain copy (`scratchpad/v313/gamma-real-1905.txt`):
SPY spot 758.59, net -$4.67B per 1%, NEGATIVE, flip 760.94, peak 760, 0DTE peak 756; QQQ 688.00 NEGATIVE flip 692.37;
NVDA 163.94 NEGATIVE flip 176.64; AAPL 355.69 POSITIVE flip 344.75. **Correction**: the 19:07 prototype (evidence entry
18:57-19:10) said SPY and QQQ POSITIVE with spot just above the flip: it took the spot from the last row read, and the
current-rows table also holds rows from earlier captures (contracts that left the band) with older spots (SPY 762.64 from
16:21 UTC). With only the latest capture's rows (this implementation), SPY and QQQ were NEGATIVE at 19:05. **Stale rows**
in `rt.option_breadth_current` at the close (close copy): AAPL 4, AMZN 24, IWM 16, MSFT 18, NFLX 16, NVDA 82, QQQ 70,
SPY 86, TSLA 38 rows from earlier captures beside the latest capture -> G102 row R55 (Flow and the Screener may read them).

### Nov 13 (00:29-00:45 UTC): B1 V4.1 - C6 profile v4 coded and unit-tested (separate worktree; not in r100)

Worktree `StockScreener.worktrees/v4-profile` (local branch `g106-v4-profile` from `b23fc1c` + the r100 / post-r100
backend changes, so the r100 tree that any morning hotfix is built from stays clean; nothing committed).
- `realtime/policies/advanced_options_activity_momentum_c6_v4.json` (from v3): 24 per underlying (default = maximum),
  discovery to 60 DTE without strategies, shortlists 32/24/8 per side (short / core / extension), bounds doubled,
  `product_buckets`: DAILY_EXPIRY = SAME_DAY 0 DTE 6, NEAR 1-2 DTE 6, WEEKLY (ladder) 8, CORE 6-21 DTE 4;
  WEEKLY_EXPIRY = WEEKLY (ladder) 8, CORE 6-21 DTE 12, EXTENSION 22-60 DTE 4.
- `advanced_options_activity_momentum_c6.py`: version v4 (status `C6_V4_DEPTH_RUNTIME_LOADED`, own basket list with an
  activity EXTENSION basket), v3 behaviour shared through a v3 family (expiry evidence, same-day, payloads), v2/v3's
  fixed 8/12/4/4 checks untouched; product recognition (`v4_product_kind`: at least 4 listed expiries in the next 6
  days = DAILY_EXPIRY); the weekday ladder (`v4_weekly_ladder`: Mon-Wed this week's weekly, Thu this week's and next
  week's, Fri next week's first plus a single stock's own 0DTE; a holiday week keeps its last listed expiry); the bucket
  fill (`_v4_fill_product_buckets`, the queue's quality order, calls and puts together) before the normal rounds;
  activity roles for 22-60 DTE contracts under v4 only.
- Tests: new `test_c6_v4_profile_nov13.py` (7: identity, ladder by weekday and holiday week, product recognition,
  index ETF Monday 6/6/8/4, single stock Monday 8/12/4, single stock Friday next week's + own 0DTE and extension 4);
  existing C6 / selection / preparation / pre-session / session-state / lifecycle suites: 263 passed, 28 skipped (DB),
  the one pre-existing lifecycle failure (`test_options_preparation_budget_has_local_and_shared_ceilings`, same on
  the r100 tree).
- Next (V4.1 done-when): the Nov 12-13 preparation inputs re-run offline with v4 (per-name counts per bucket, roles,
  groups, no 3-4 DTE in O1 lanes); then the preparation's profile switch (V4.3, the user's yes).

### Nov 13 (00:45-00:48 UTC): B1 V4.1 - offline re-run on real data (a Friday Nov 13 preparation)

Input: the Nov 12 close chain copy (`research/chains/2026-11-12/close.csv.gz`, REST, every contract 0-60 DTE within
+/-20% of spot: 17,538 quoted candidates, 9 names) as a v4 preparation for Friday Nov 13 (prepared 23:00 UTC Nov 12);
script `scratchpad/v4_replay_nov13.py` in the v4-profile worktree. Two changes from the first run: the index NEAR bucket
counts listed sessions (`NEXT_LISTED`: Friday's NEAR = Monday and Tuesday, not an empty weekend), and a ladder bucket
with two expiries fills them round-robin, primary first. Result: **216 selected = 24 x 9**; products recognised right
(SPY/QQQ/IWM DAILY_EXPIRY, the six stocks WEEKLY_EXPIRY).

| Name | Product | 0 DTE | 3-4 DTE | 5 DTE | 7 DTE (next weekly) | 6 and 10-21 DTE | 22-60 DTE | calls / puts |
|---|---|---|---|---|---|---|---|---|
| SPY | daily | 6 | 2 | 4 | 8 | 4 (6, 17) | 0 | 14 / 10 |
| QQQ | daily | 6 | 2 | 4 | 8 | 4 | 0 | 17 / 7 |
| IWM | daily | 6 | 2 | 4 | 8 | 4 (6, 14, 21) | 0 | 12 / 12 |
| AAPL | weekly | 4 | 0 | 0 | 5 | 11 | 4 | 16 / 8 |
| AMZN | weekly | 4 | 0 | 0 | 6 | 10 | 4 | 15 / 9 |
| MSFT | weekly | 4 | 0 | 0 | 8 | 8 | 4 | 15 / 9 |
| NFLX | weekly | 4 | 0 | 0 | 7 | 9 | 4 | 14 / 10 |
| NVDA | weekly | 4 | 0 | 0 | 8 | 8 | 4 | 16 / 8 |
| TSLA | weekly | 4 | 0 | 0 | 8 | 8 | 4 | 21 / 3 |

Points for the user's review before V4.3 (none blocks the code):
1. Index NEAR on a Friday is Monday/Tuesday = 3-4 calendar DTE, which the flow policy keeps out of O1 (capture only)
   and the quality ranking places last, so only 2 of 6 were taken; the rounds gave the rest to Wednesday (5 DTE, flow
   eligible). Alternative: NEAR by calendar DTE (empty on Fridays, its slots go to the rounds). Same outcome today.
2. No side quota (Nov 8 decision) leaves TSLA at 21 calls / 3 puts and QQQ 17 / 7 on this day.
3. Stocks on their expiry day: 4 x 0DTE beside next week's weekly (round-robin), per the ladder.
Tests: 8 v4 tests + the C6 / selection suites: 77 passed. Not done yet: the profile switch in the lifecycle (V4.3 with
the user's yes) and a copy rehearsal of a v4 preparation end to end (slot set, ingress plan at 216, market-state).

### Nov 13 (00:48-00:52 UTC): B1 V4.1 - the turn-on path

The lifecycle chooses the profile from `REALTIME_V3_OPTIONS_ADMISSION_PROFILE` (default `c6_activity_momentum_v2`,
`scripts/run_advanced_options_v3_lifecycle.py`); V4.3 = set it to `c6_activity_momentum_v4` for one preparation (the
user's yes; fallback = remove the key, the next preparation is v2). Found while wiring: the P8 market-state role built the
bounded detector router only for v2 (`advanced_options_p8_roles.py`), so a v3 or v4 selection would run without
routing; v4 now joins v2 there (`_ROUTED_ADMISSION_PROFILES`). Also extended to v4: the preparation executor's accepted
versions and its v3-family resolution inputs, the pre-session script's choices and v3 branches, the selection plan's
budget check. Suites (C6, selection, preparation, pre-session, session state, lifecycle, P8 roles / production /
selection source, consumers; 20 files) on the v4-profile worktree: 277 passed, 28 skipped, 1 failed = the pre-existing
lifecycle test. Remaining for V4.1: an end-to-end v4 preparation on a copy (needs the provider's REST for the catalog
and quote capture, so a copy with a provider route: the user's call) or the first v4 preparation itself as V4.3.
- Full `tests/realtime` suite on the v4-profile worktree (00:52-01:08): 158 failures, the identical set to the r99 / b23fc1c
  baseline (no new failure, none fixed). V4.1 code is ready for the Nov 13-15 nights: merge into the main worktree when the
  next package is built, then the end-to-end preparation check and V4.3 on the user's yes.

### Nov 13 (03:18 UTC): B1 V4.1 merged into the G102 worktree (v3-stable-nov5) for the next package

The five C6 files (activity momentum, automatic preparation, P8 roles router, selection, pre-session script) were unchanged in v3-stable-nov5 since `b23fc1c`, so the v4-profile versions were taken as they are, with the v4 policy JSON and `test_c6_v4_profile_nov13.py`. C6, selection, pre-session and lifecycle suites: 112 passed, 1 failed (the baseline's lifecycle budget test). Default profile stays v2; V4.3 (`REALTIME_V3_OPTIONS_ADMISSION_PROFILE`) is the user's yes. The v4-profile worktree is kept as it was.
