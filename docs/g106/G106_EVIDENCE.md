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

### Nov 16 (08:40-09:36 UTC): AL2 - O1 debit verticals built, rehearsed, image r121 (build session, not deployed)

Goal (user, Nov 16): build O1 debit verticals now, rehearse on previous sessions' data, and keep them ready for a
deployment after mid-session today. Design and rule: `G106_AL2_O1_DEBIT_VERTICALS.md`. Branch `al2-o1-debit` (from
d3ade41 = r120 code + docs), image **r121 `sha256:601cc71f9352ebcc38c2a30d6d0429b9dca792d3843457690c34fe41957ce993`**
(source e56ad7182d4fb950; image code vs r120 = the AL2 files + 3 known line-ending-only files; Stock Alert code hashes
unchanged). Deploy note for the live session: `HANDOVER-20261116-LIVE.md` section 13 (RECORD 2-3 windows, then ON, each a
hotfix of `realtime-v3-options-p8-window`; the API stays r120).

- Switch `REALTIME_V3_O1_DEBIT_VERTICAL` (OFF = r120 byte for byte; RECORD = log one `O1_DEBIT_VERTICAL_RECORD` line per
  window and publish today's single contract; ON = the vertical when it qualifies, else today's single contract; any
  other value stops the window service at start).
- Rule `o1-debit-vertical-v1`: long = the event's contract, short = same expiry/type further OTM among the watched
  contracts marked at the cutoff, nearest first; legacy debit-spread limits (width 0.5-10% of spot, reward/risk >= 1,
  leg OI >= 250 and day volume >= 100, DTE <= 60); DTE floor 0 instead of the legacy 1 (O1 exits at the session close;
  replay below). Found on the way: the live window's underlying ticks are `UnderlyingSnapshotTick` (price under
  `evidence`): a reader of trade/quote only would have rejected every vertical as SPOT_UNAVAILABLE (fixed and tested).
- Tests: `test_o1_debit_vertical.py` 21 passed; targeted suites (15 files) 245 passed, 1 failed = the same failure on
  84e04b2; full `tests/realtime` (09:13-09:35, both trees in parallel): 158 failed / 5,278 passed on al2-o1-debit vs 158 failed / 5,257 passed on 84e04b2, the identical failure set (0 new, 0 fixed; +21 = the new tests).
- Replay (`runner/o1dv_replay.py`, isolated test server, stored rows; outputs `evidence/o1dv-20261116/`):
  Nov 12 (V2, 77 windows, 72 contracts): 591 detected events (397 alerts), 0 verticals: no further-OTM watched strike
  431, long leg unmarked 115, reward/risk < 1 44 (in-the-money NVDA calls, 0.55-0.64), short unmarked 1; windows marked
  18-64 of 72. Nov 13 (V4, 216 contracts; the copy has 8 windows, 18:15-19:58):

| Cutoff | Marked/216 | O1 evaluations | Detected | New detected | New alerts | Built | Built alerts | Fallbacks | Debit | Width | R/R |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 18:15:00 | 47 | 69 | 13 | 13 | 3 | 1 | 1 | long unmarked 10, short unmarked 2 | 0.04 | 2.5 | 61.5 |
| 18:19:00 | 113 | 68 | 11 | 1 | 1 | 0 | 0 | short unmarked 1 | - | - | - |
| 18:54:22 | 77 | 135 | 8 | 8 | 6 | 5 | 5 | long unmarked 2, no further OTM 1 | 0.15 | 5 | 32.3 |
| 19:00:00 | 64 | 106 | 12 | 8 | 4 | 0 | 0 | short unmarked 4, long unmarked 4 | - | - | - |
| 19:02:51 | 60 | 111 | 10 | 2 | 0 | 0 | 0 | long unmarked 2 | - | - | - |
| 19:45:00 | 38 | 69 | 16 | 16 | 2 | 0 | 0 | long unmarked 14, short unmarked 2 | - | - | - |
| 19:50:00 | 116 | 87 | 20 | 13 | 5 | 1 | 1 | long unmarked 7, short unmarked 2, no further OTM 2, no short in width 1 | 0.34 | 2.5 | 6.35 |
| 19:58:40 | 60 | 156 | 47 | 42 | 10 | 11 | 10 | long unmarked 31 | 0.10-0.86 | 2.5-5 | 1.9-49 |

  Built (18, 17 of them alerts): NVDA 16, TSLA 2, all 0DTE calls (Friday); debit median 0.15, reward/risk median 32,
  capital at risk $4-$86. With the legacy DTE floor of 1 Nov 13 builds 0 (57 of 103 detected events 0DTE).
- End-to-end on the copy (`runner/o1dv_e2e.py`, switch ON, app role): real `_load_o1` at 19:58:40 -> 156 invocations, 2
  verticals; real O1 executor -> lane COMPLETE, 15 selected, 1 vertical (NVDA 165C/170C, debit 0.10, max profit $490);
  package stored with leg_count 2; session alert written (CALL_DEBIT_VERTICAL, direction +1, 2 legs); API read on r120
  and on r121: 200, the row with both legs and OA1 IV/delta per leg (no API change needed). RECORD from the r121 image at
  18:54:22: 0 used, 94 single contracts unchanged, 50 built and logged. Test rows removed afterwards; copy restored.
- Not exercised: a full Options window cycle with the switch (evidence batch + Alerts publication of a 2-leg O1 package).

### Nov 17 (04:20-05:10 UTC): AL4 / G102 R87 - O1 liquidity floor built and replayed (build session, not committed, not deployed)

Trigger: Nov 16 ~16:00 the Options Alerts page selected AAPL 362.5P 11/27 (prior OI 27, day volume 109, ratio 4.04).
Branch `o1-liquidity-floor` (worktree `StockScreener.worktrees/o1-liquidity-floor`, from main 67d47b9), uncommitted.

**Where O1 events become alerts (no top-N).** `_load_o1` (native loader) reads the last 10 minutes of flow events and
builds each event's package (v1 single contract; AL2 vertical when ON); the O1 executor
(`advanced_options_native_adapters.build_advanced_o1_executor`) runs the detector per event and marks
SELECTED (DETECTED + package + selection enabled) / OBSERVATION / NOT_SELECTED - every detected event with a package is
selected, there is no per-window cap (D1's per-name cap stayed DEFERRED); the projection keeps the earliest selected
event per contract and package variant as the window's alert row (`_one_alert_per_contract`) and the session alert
anchors on the first trigger per session, detector and contract (`persist_options_session_alerts_cursor`). The floor
therefore sits in the executor, as the detector's policy: an event below it is NOT_DETECTED there, and the grouping and
the session alert run on what is left, so a later event on the contract that clears the floor becomes the alert.

**Change (5 files).**
- `options/dual_origin.py`: `ActivityLiquidityFloorPolicy` / `ACTIVITY_LIQUIDITY_FLOOR_POLICY`, version
  `option_participation_origin_v2_liquidity_floor`, sha256 `951693f49bf16d749d351e120c6a3857662734362b814073545880f0099ef265`
  (base v1 `b08daea00fac0e1637a59b3140ee82d0eed1706a4d825f431da21ddddaa8613e`, unchanged): prior-session OI >= 100 and
  day volume at the event >= 500 (the two `ActivitySource` numbers the ratio is made of); reasons
  `O1LF_OPEN_INTEREST_BELOW_POLICY`, `O1LF_DAY_VOLUME_BELOW_POLICY`, missing input `O1LF_OPEN_INTEREST_UNAVAILABLE` /
  `O1LF_DAY_VOLUME_UNAVAILABLE` (fail closed: UNAVAILABLE; v1 already makes such an event UNAVAILABLE, so the floor can
  never loosen anything). `apply_activity_liquidity_floor(source, v1 finding)` -> `LiquidityFloorActivityFinding`
  (schema `option_participation_finding_v2`: the v1 fields, the v2 policy hash and episode, `open_interest`,
  `day_volume`, `liquidity_reasons`); a DETECTED finding below the floor becomes NOT_DETECTED with the floor reasons,
  any other finding keeps its disposition and reasons. `ActivityFinding` checks its policy through a ClassVar (not a
  field: the v1 finding's JSON and hash are unchanged).
- `realtime/advanced_options_o1_liquidity_floor.py` (new): switch `REALTIME_V3_O1_LIQUIDITY_FLOOR` (OFF default /
  RECORD / ON; anything else raises), record documents and log lines.
- `realtime/advanced_options_native_adapters.py`: the O1 executor reads the switch per window. OFF: unchanged code path.
  RECORD: v1 findings and selection unchanged, the floor evaluated per event and logged. ON: the v2 finding is every O1
  evaluation's payload (the detector policy hash changes only when active), events below the floor NOT_SELECTED
  without a package. A floor error never fails the window (RECORD keeps v1; ON leaves the event out, fail closed);
  logging never fails the window. Log lines: `O1_LIQUIDITY_FLOOR_RECORD {json}` per detected event below the floor
  (event, contract, OI, volume, ratio, reasons, without_floor SELECTED/OBSERVATION, with_floor NOT_SELECTED, applied)
  and `O1_LIQUIDITY_FLOOR_WINDOW {json}` per window (detected, below_floor, selected and alerts without/with floor,
  alerts_refilled, reasons, by_underlying, `instead[]` = per contract the dropped alert event and the event selection
  picks instead or null).
- `scripts/run_advanced_options_v3_production.py`: the WINDOW service validates the switch at start (as AL2).
- `tests/realtime/test_o1_liquidity_floor.py` (new, 28 tests).

**Byte parity.** The same flow rows through the real `_load_o1` and the real O1 executor (r121 image, offline): main
67d47b9 (any switch value) and this branch with the switch unset / OFF / RECORD give the identical lane digest
`7dad1b5e...` and input identity `3abf7b0e...`; ON gives `c6df97d2...` (the two thin events NOT_SELECTED, a QQQ-like
OI 152 / volume 165,635 event still SELECTED). The loader's statements and lane identity do not depend on the switch.

**Replay Nov 10-16** (read-only CSV exports of the live DB, statement_timeout 30 s, 4,299 detected O1 evaluations +
packages + 79 session alerts + 15,580 cutoff marks; script, exports and output in
`backend/.realtime-canary/.../recovery-20261104/evidence/o1lf-20261117/` (`replay.py`, `out.json`, `data/*.csv`,
`x_*.sql`), adapted from AL2's `runner/o1dv_replay.py`; `g102-test-pg` not used). Today's selection is rebuilt from the
stored windows (every detected event with a stored package selected; grouping and session anchoring as the projection)
and matches every stored O1 session alert (71 of 71, same anchor events). With a floor the dropped events leave the
selection and the same grouping runs again. Floor inputs = the event's `prior_open_interest` and
`cumulative_volume_after`, exactly the detector's; the replay's floor = the branch code on all 4,299 rows (0 mismatches).

| Session | Stored windows (UTC) | Detected events | Selected events | Alerts today (window rows) | Below floor: events (selected) | Alerts with floor (window rows) | Refills | Dropped alerts |
|---|---|---|---|---|---|---|---|---|
| Nov 10 | 56, 15:35-21:00; O1 selection from 17:56 | 1,040 | 253 | 9 (41) | 0 (0) | 9 (41) | 0 | - |
| Nov 11 | 77, 14:40-20:58 | 154 | 95 | 14 (95) | 24 (8) | 12 (88) | 0 | MSFT 11/16 530C (OI 46, vol 271), SPY 11/23 761C (OI 33, vol 2,548) |
| Nov 12 | 77, 14:40-21:00 | 591 | 457 | 11 (114) | 0 (0) | 11 (114) | 0 | - |
| Nov 13 | 8, 18:15-19:58 only | 103 | 46 | 11 (22) | 0 (0) | 11 (22) | 0 | - |
| Nov 16 | 76, 14:40-20:58 | 376 | 220 | 26 (146) | 85 (15) | 21 (132) | 0 | AAPL 11/27 362.5P (OI 27, vol 109), AMZN 11/27 257.5C (45, 242), NVDA 11/25 150C (84, 287), 157.5C (57, 197), 167.5P (42, 151) |
| Total | | | | 71 | 109 (23) | 64 | 0 | 7 |

Per underlying (alerts today -> with floor): Nov 11 SPY 5->4, MSFT 1->0; Nov 16 NVDA 12->9, AAPL 2->1, AMZN 1->0; all
other names unchanged. Below-floor events by underlying: Nov 11 MSFT 16, SPY 8; Nov 16 NVDA 79, AMZN 4, AAPL 2. No
refill: OI is the prior session's, so a contract below the OI minimum stays below all session, and OI >= 100 with ratio
>= 3 already means volume >= 300 (the volume minimum never bound alone on Nov 10-16). Full replays: Nov 11, 12, 16.
Partial: Nov 10 (windows from 15:35, O1 selection only from 17:56 after the Nov 9 hold), Nov 13 (only 8 windows stored,
18:15-19:58; the window was down most of the day). Nov 9 not replayed (selection held, 0 selected).

Threshold sets (totals over the 5 sessions):

| Set | Alerts (of 71) | Dropped | Refills | Below-floor events (selected) | Notes |
|---|---|---|---|---|---|
| A: OI >= 250, vol >= 100 (AL2 leg policy) | 60 | 11 | 0 | 156 (43) | also drops QQQ 11/11 687P 0DTE (OI 249, vol 95,329), QQQ 11/16 669C 0DTE (OI 152, vol 165,635), SPY 11/19 761C (OI 149, vol 2,771), NFLX 11/27 61C (OI 190, vol 618) |
| **B: OI >= 100, vol >= 500 (implemented)** | **64** | **7** | **0** | **109 (23)** | only contracts with OI <= 84 (and vol <= 2,548) |
| C: A, or vol >= 10,000 | 62 | 9 | 0 | 148 (36) | B + SPY 11/19 761C and NFLX 61C |
| D: OI >= 500, vol >= 1,000 | 53 | 18 | 0 | 227 (78) | drops liquid 0DTE QQQ/MSFT strikes |
| E: vol >= 1,000 and day premium (vol x price x 100) >= $250k | 60 | 11 | 2 | 143 (33) | 6 thin contracts plus 5 cheap 0DTE lottos (AAPL 357.5C, TSLA 387.5C, NFLX 63C, NVDA 170C, QQQ 686C) that are not thin; refills NVDA 11/25 157.5C at 19:43 (vol 1,081) and NVDA 12/04 155C |
| G: vol >= 500 only | 67 | 4 | 2 | 62 (13) | drops AAPL 362.5P, AMZN 257.5C, NVDA 150C/167.5P; keeps SPY 11/23 761C (vol 2,548); refills MSFT 530C (20:16) and NVDA 11/25 157.5C (19:43) later in the day |

Outcomes (entry = the stored package entry mark; exit = the contract's last cutoff mark at or before 21:00 UTC from
`rt.option_mark_evidence`, Nov 13: the last stored window 19:58; mid to mid, no costs; verticals long - short; every
one of the 71 alerts has an exit): set B kept 64: mean -21.1%, median -22.0%, 35.9% up; dropped 7: mean +2.7%, median
-1.1%, 3 of 7 up (AAPL 362.5P +25.3%, SPY 761C +10.4%, NVDA 167.5P +3.8%; NVDA 157.5C -8.6%, 150C -6.1%, AMZN -5.1%,
MSFT -1.1%). A: dropped 11 mean -7.7%; C: dropped 9 mean +5.9%. The dropped alerts are longer-dated, in-the-money,
high-premium contracts whose mid moves little in a session; the kept set holds the 0DTE/short-dated contracts that swing
-100%/+200%. The outcome data (n = 7) does not argue for or against the floor; its case is tradeability (entry spreads
of the dropped: 0.8%-6.9%; at OI 27-84 and volume 109-2,548 a mid fill and a same-day exit are not credible) and the
user's rule. Separate observation for Q2's price floor: 10 of 71 alerts had entry < $0.25 with an entry spread >= 15%
(0DTE lottos); the liquidity floor does not touch them.

Recommendation: B (implemented). It drops exactly the alerts where both OI and volume are low, keeps every 0DTE index
strike (small prior OI, very high volume), needs no premium input the detector does not have, and creates no refill
churn. A (the AL2 leg policy) over-drops liquid 0DTE strikes; C is B plus two arguable drops with an extra rule; a
premium floor measures price, not liquidity.

**Tests** (offline containers on the r121 image, worktree read-only): `tests/realtime/test_o1_liquidity_floor.py` 28 passed (policy and v1
pin, threshold edges 100/99 and 500/499, missing inputs fail closed, the v2 finding's validators, switch values and
start-up check, OFF = v1 payload byte for byte, RECORD = OFF + log lines, ON = v2 on every evaluation, refill through
the real `_load_o1` + executor + `_one_alert_per_contract` + `persist_options_session_alerts_cursor`, floor and
logging failures never fail the window, invalid switch fails the lane, hold switch wins, vertical variant grouping,
loader identity independent of the switch). Neighbours (50 files: dual_origin, native adapters/loader, detector
adapters, AL2, cycle, projection, session alerts, window/runtime/deployment, integration manifest, ...): branch 16
failed / 1,053 passed / 64 skipped vs main 67d47b9 16 / 1,025 / 64, the identical failure set (+28 = the new tests);
the 16 are environmental (6 `test_dual_origin` API tests need a database; 10 need the compose file / frontend outside
the backend mount: with those mounted, the 5 files pass 124/124 on both trees).

**Image note.** r121 = r120 + AL2 (406c9cc). main 67d47b9 adds OA3 (`options_alert_history.py`,
`portal_compatibility_repository.py`: API read), R84 (`corporate_action_drift.py`, `daily_products.py` identity, the
drift SQL in the dashboard/stock-alert/strategy/ticker repositories and `forward_reader.py`) and R85
(`portal_projection_repository._competing_generation` inside `_publish_portal_release`). In the Options window process
(entry script + window modules imported in the r121 image): OA3's module is not loaded; R84's modules are imported
(through `stage_runners_options` -> `stage_runners_equity`) but no Options window stage calls the changed functions;
R85 runs on every Options page release, and is a no-op there: Options releases (Options cohort, provider tier
MASSIVE_ADVANCED) carry only OPTIONS_* snapshots, so `_competing_generation` returns min(previous generation, max entry
generation) = the previous generation; live check (read-only) of the Options pointers Nov 10-16: pointer generation =
release generation = max entry generation (1339 / 1417 / 1495 / 1540 / 1683). The three modified files are identical on
406c9cc and main, and the branch diff applies cleanly to `al2-o1-debit` (`git apply --check`). Either base gives the
same O1 code.

**Deploy note (for the live session).** Service `realtime-v3-options-p8-window` only; the API needs no change (an O1
row's observation payload is JSON; a dropped event shows as NOT_SELECTED with `O1LF_*` reasons). As AL2: RECORD
(`compose_env.py ... REALTIME_V3_O1_LIQUIDITY_FLOOR RECORD` + hotfix of the window on the new image), check 2-3 windows
(`docker logs ... | grep O1_LIQUIDITY_FLOOR_WINDOW`: `applied` false, `below_floor` / `instead[]` plausible, no
`RECORD_FAILED`, no `floor_failures`; O1 lane and alert counts unchanged), then ON (same with ON; `applied` true; new
O1 alerts all OI >= 100; window times). Undo: remove the key (absent = OFF = today) and hotfix; or roll the window back
to its previous image. Switching ON mid-session does not withdraw session alerts already written; it stops new ones
below the floor. Not exercised: a whole live Options window cycle with the switch (no copy run: `g102-test-pg` was
reserved for another session).

**Addendum (Nov 17 05:25-05:50 UTC): thresholds from the environment** (same branch, uncommitted). Options window
service `realtime-v3-options-p8-window`:
- `REALTIME_V3_O1_LIQUIDITY_FLOOR_MIN_OPEN_INTEREST` (default 100), `REALTIME_V3_O1_LIQUIDITY_FLOOR_MIN_DAY_VOLUME`
  (default 500): `ActivityLiquidityFloorPolicy` fields are now ints (>= 0, strict) instead of Literals; the defaults
  give the same hash `951693f4...`; set values make a new policy (hash, v2 episode ids, every v2 evaluation). The v2
  finding now names its thresholds (`floor_minimum_prior_open_interest`, `floor_minimum_day_volume`) and validates its
  hash against them; the window line and every record line carry `policy_sha256` (+ the window line the minimums).
- `REALTIME_V3_O1_DEBIT_VERTICAL_LEG_MIN_OPEN_INTEREST` (default = the file's 250),
  `REALTIME_V3_O1_DEBIT_VERTICAL_LEG_MIN_DAY_VOLUME` (default = the file's 100): written into the AL2 policy document
  before hashing; unset = the file's document (hash `3371465f...` unchanged); the `O1_DEBIT_VERTICAL_RECORD` line now
  carries `policy_sha256`, `minimum_leg_open_interest`, `minimum_leg_day_volume`.
- Validation: unset/blank = default; otherwise ASCII digits only (surrounding blanks allowed); anything else raises with
  the key's name, and the WINDOW service checks all four (plus both mode switches) at start, before its database.
- Read once per process (`o1_liquidity_floor_policy`, `o1_debit_vertical_policy`, cached; the service start reads them):
  one process = one policy; a compose env change needs a container recreate anyway.
- Parity: same flow rows through the real loader + executor: main and branch unset / OFF / RECORD -> lane digest
  `7dad1b5e...` (unchanged); ON -> `8179a725...` (was `c6df97d2...` this morning: the v2 finding gained the two threshold
  fields; the v2 format has never run live); explicit default values = unset.
- Mid-session change (container recreate): floor - new policy hash and episode ids for later evaluations (episode ids
  are payload-only); session alerts are per session/detector/contract, so no second alert for a contract already
  alerted; a looser floor lets a contract alert for the first time at the change (entry at that later event, not
  back-dated); a stricter one does not withdraw alerts, their later events stop adding hits. AL2 legs - the candidate id
  is per event (policy-independent) and a stored package is always reused, so no second package and no ambiguity; new
  verticals carry the new policy hash in their identity; a single-contract alert and a vertical alert on one contract
  can coexist (as already possible). A window whose retry straddles the change diverges (WINDOW_RETRY_DIVERGED), as a
  mode flip does; hence the hotfix in the quiet part of the cycle. No extra guard needed.
- Tests: `test_o1_liquidity_floor.py` 77 passed (adds unset/blank/explicit-default hash stability for both policies,
  set values stamped, 36 invalid-value cases, read once per process, OFF ignores thresholds, ON uses and names them,
  v2 finding validates its own policy, AL2 rule + record line use the leg thresholds, the WINDOW service refuses to
  start on any invalid value and passes the checks with valid ones); `test_o1_debit_vertical.py` 21 passed;
  neighbour suites (50 files): 16 failed / 1,102 passed / 64 skipped vs main 16 / 1,025 / 64,
  the identical (environmental) failure set.

### Nov 16 (17:11-18:00 UTC): OV1 - Options Overview exposure read and page (build session, not deployed)

Design `G106_OV1_OPTIONS_OVERVIEW.md`; worktree `StockScreener.worktrees/ov1-options-overview`, branch
`ov1-options-overview` from main 0830642 (uncommitted). Market in session: no database or provider access, nothing on
the live host beyond light local tests; evidence runs offline on the recorded Nov 12 chain copies
(`research/chains/2026-11-12/{1905,close}.csv.gz`, full `rt.option_breadth_current`) through a file-backed cursor that
answers the read's own statements (`runner/ov1_exposure_evidence.py`; outputs `evidence/ov1-20261116/`).

- Skipped contracts (the gamma read's ~940 of ~4,680 for SPY on Nov 16): on Nov 12 19:05 SPY skips 1,269 of 5,068 =
  1,227 with open interest 0 (no effect on an OI total) + 42 without any IV (`BELOW_NO_ARBITRAGE_BOUND`, deep in the
  money); uncovered open interest 0.00-0.22% across the 9 names. 37 SPY contracts use the provider's IV.
- Our gamma recomputed from `local_iv` at the capture's own time to expiry (`_time_to_expiration_years`, whole days /
  365) equals the stored `local_gamma`: largest relative difference 2e-15 over ~16,700 contracts (9 names).
- Hand checks (stored `local_gamma`/`local_delta` x OI x 100 x scale x dealer sign, summed per strike; 3 strikes per
  name: call wall, put wall, nearest the flip; gamma and delta; 19:05 and the 20:56 close capture): **36 of 36 equal
  to the dollar.**

| Nov 12 19:0x | Spot | Net gamma / 1% | Flip | Regime | Call wall | Put wall | Expiring today | Provider check gamma / delta (gross) | Existing gamma read net / flip |
|---|---|---|---|---|---|---|---|---|---|
| SPY | 758.59 | -$4.72B (-6.2M sh) | 760.96 | negative | 775 | 760 | 16.0% | -1.5% / -0.1% | -$4.66B / 760.94 |
| QQQ | 688.00 | -$2.27B | 692.37 | negative | 700 | 690 | 13.8% | -1.9% / -0.2% | -$2.26B / 692.37 |
| NVDA | 163.94 | -$233M | 176.50 | negative | 185 | 160 | 0% (no 0 DTE) | -1.7% / +0.7% | -$228M / 176.64 |

  The existing gamma read differs by 1-2% on totals because it evaluates gamma at 21:00 UTC / 365.25-day time from the
  read time, not the time `local_iv` was solved with (follow-up in the design, section 9). Close capture (20:56, SPY
  756.75): net -$8.99B, flip 760.18, 0 DTE 19.2% of gamma; provider check -4.6% / -0.6%.
- Read time (file-backed cursor, laptop): SPY cold 153 ms (5,068 rows parsed), further views 27-107 ms, repeat ~32 ms
  (mostly the test cursor scanning 18,000 rows in Python); QQQ cold 193 ms; NVDA 57 ms. Payload SPY ~100 KB JSON (272
  strikes, 2,376 cells, 17 expiries), gzipped by nginx. SQL time: see the after-close entry below.
- Tests: backend new 16 (`test_options_exposure_read_ov1.py`: units and signs by hand, scopes, weights, flip and walls
  reuse, skips, empty/expired scopes, R55 latest capture, cache, failed newer capture, provider cross-check, switch and
  Greek flag, API contract) + neighbours (`test_options_gamma_read_nov12`, greeks, gamma exposure/walls/repository,
  `test_portal_api`, `test_option_flow_realtime`): 276 passed (255 + 21). Frontend: new 8 (`optionsOverviewOv1.test.mjs`) pass;
  full suite 337 tests, 294 pass, 38 fail = all also failing on main (main: 41 fail; 0 new); `tsc --noEmit` clean.

### Nov 16 (21:12-21:40 UTC, after the close): OV1 build, screenshots, SQL time, spot-vs-quotes finding

- Build: `tsc && vite build` clean in the worktree; page chunk `OptionsOverviewPage` 30.4 kB (10.7 kB gzip) + CSS 7.1 kB.
  No image built (the candidate scripts write the live ledgers).
- Screenshots (`evidence/ov1-20261116/`, `runner/ov1_screenshots.sh`: the real API app over the 19:05 chain copy,
  `vite preview` on 127.0.0.1:5199, headless Edge driven over DevTools at exact viewport widths): SPY gamma net all,
  SPY delta calls-vs-puts, QQQ gamma 0 DTE, NVDA gamma OI+volume, NVDA 0 DTE empty state, SPY gamma light theme,
  SPY 7 days calls-vs-puts in shares at 390 px. Every page measured `scrollWidth == viewport` (no page-level horizontal
  scroll); the heatmap and the levels table scroll inside their panels on a phone.
- Found and fixed in the screenshots: (1) "Largest levels" inherited the Flow page's 900 px table floor, so at 1440 px
  the From-spot and Level columns (call wall / put wall / peak gamma tags) were cut off; the two OV1 tables now fit
  their panel. (2) The first phone shot looked cropped: a headless window keeps a minimum width (~500 px), so the shot
  was a crop, not the page; the script now sets the viewport over DevTools (the page was 375/375 px in a probe).
  (3) The cross-check warning quoted the gross gap even when the net gap tripped it (QQQ 0 DTE: "differs by -0.2%");
  it now quotes the larger one.
- **Finding: the capture's spot lags the option quotes.** `spot` in `rt.option_breadth_current` is the equity bar at
  `equity_cutoff`; the quotes are a minute or two later. Nov 12 19:05 copy: 22 of 33 (name, capture) pairs differ from
  the provider's `underlying_price` by more than 5 bp, 14 by more than 10 bp (NVDA up to -62 bp early in the session).
  QQQ 11:07 PT: spot 688.00 vs provider 689.18 and put-call parity 689.12 at 684-692, so near-money 0 DTE calls solve to
  IV 0.42-0.73 and puts to 0.20-0.36 at the same strikes (690-692 puts fall below the no-arbitrage bound: no IV), and
  our 0 DTE gamma splits calls x0.81 / puts x1.42 against the provider (net gap -15.6% of gross, gross -0.2%). SPY at
  the same time: spot = parity forward to 2 cents, call and put IVs agree, ATM gamma within 2% of the provider. The
  levels on longer expiries move little; 0 DTE moves most. OV1 now reports `coverage.spot_check` (spot, median
  `underlying_price` of the capture's rows, gap) and the reason `SPOT_QUOTE_GAP` from 10 bp, shown as a warning on the
  page. Fixing the spot itself belongs to the capture (local Greeks feed the O-lanes too): proposed as follow-up OV1-F2
  in the matrix, not changed here.
- SQL time on `g102-test-pg` (`test_realtime_v2_recovery_20261104r120svc`, Nov 11-13 breadth, 92,042 current rows,
  started for this and stopped after): the read's five statements for SPY Nov 13, all on existing indexes: latest
  session 0.1 ms (backward scan of `idx_rt_option_breadth_chain`), names 0.1-0.7 ms, capture counts 2.0 ms (4,872 rows,
  632 buffers), newest capture state 0.02 ms, chain rows 1.5 ms (4,802 rows, once per capture). About 4 ms of execution
  per request; the whole SPY working set is 632 pages (~5 MB). No new index needed.
- Tests after these changes: backend 17 new + neighbours = 277 passed; frontend OV1 8 pass (one new case for the two
  warnings), `realtimePortal` 12 of 14 (the same 2 fail on main); `tsc` clean.
### Nov 17 night (04:54-05:45 UTC): H1 daily close chain snapshot and A2 IV summary built (worktree `g106-h1-a2`, not committed, not deployed)

**Who writes and who prunes `rt.option_breadth_current`.** Written only by the Options scheduler
(`stock-screener-realtime-v3-options-scheduler`: `run_advanced_options_v3_scheduler.py` -> `_capture_breadth` ->
`advanced_options_breadth.capture_options_breadth` -> `persist_breadth_cursor`): after each attached Options window, one
upsert per underlying keyed (session_date, contract_ticker), so each session keeps only its last capture per contract
(intraday captures are overwritten). Nothing prunes it: the registry class is BOUNDED (the retention executor and the
retention service drop only HOT_TTL/KEPT_3M partitions and refuse BOUNDED relations), the app role has no DELETE on it,
and no code or script deletes from it. Every session since the first capture (Nov 9) is still there and stays until
someone adds a prune. H1 conclusion: the close chains are not at risk tonight (the backfill picks up every retained
session, Nov 17 included, whenever 055 is applied; the CSV copies of Nov 9-16 are a second source); H1's value is the
explicit, immutable history with capture and copy identity, which the current table does not promise to keep.

**H1 build.** Migration `055_g106_option_breadth_daily.sql` (checksum `ebf63d4fa5a3fc77...a941f2b4ce3ab0c24f3cea934016912`;
V2 catalog 56 files, fingerprint `3a3d711dd25387f0`): `rt.option_breadth_daily` = the current table's 32 columns +
`final_capture` (the row is in its underlying's last capture: the R55 rule), `snapshot_at`, `snapshot_source`
(CURRENT_TABLE or CSV_COPY), `source_reference`; PK (session_date, contract_ticker); indexes (underlying, session_date,
expiration_date, strike_price) and (contract_ticker, session_date); append-only triggers (UPDATE, DELETE, TRUNCATE
rejected). Registry `_r("options", "BOUNDED", append=True, api=True)`: app SELECT/INSERT/UPDATE (UPDATE trigger-guarded,
the append convention), api SELECT, the same readers as the current table; listed as a post-024 relation. Writer
`realtime/option_breadth_daily.py`: `snapshot_session_cursor` (INSERT ... SELECT ... ON CONFLICT DO NOTHING, counts per
underlying, missing-row check; refuses a session before its XNYS close + 20 min: 21:20 UTC, 18:20 on Nov 27),
`backfill_retained_cursor`, `load_csv_session_cursor` (header must equal the current table's columns, one session per
file, temp table + COPY), `ClosedSessionSnapshotter` (scheduler hook when the cycle reports WAITING_FOR_SESSION, once per
session per process, retry 5 min, idle 10 min) behind `REALTIME_V3_OPTIONS_BREADTH_DAILY_SNAPSHOT` (default OFF: no
read, no write). CLI `scripts/snapshot_option_breadth_daily.py` (`--session D` | `--backfill-retained` | `--csv F`; dry
run by default, `--apply` writes; DSN file `/run/secrets/realtime_v2_app_dsn`, SET ROLE `stocks_realtime_v2_app`; exit
2 = open session or not a session, nothing written).

**H1 rehearsal (throwaway `sim-postgres:17` on a private network, removed after).** Database from migrations 000-055,
hardened with disposable roles (audit PASS; owner = the admin role; app SELECT/INSERT/UPDATE, api SELECT); the six
`research/chains/<date>/close-retained.csv.gz` copies loaded into the current table as the app role; the CLI as the app
role: dry run 1.6 s, `--backfill-retained --apply` 2.3 s, a second apply 1.0 s with 0 inserted; daily = current for
every session (EXCEPT both ways over all 32 columns: 0 rows); 55 MB for 109,182 rows.

| Session | Rows copied | final_capture (latest capture) | Older-capture rows kept, flagged |
|---|---|---|---|
| Nov 9 | 18,936 | 18,596 | 340 |
| Nov 10 | 18,796 | 18,506 | 290 |
| Nov 11 | 18,510 | 18,172 | 338 |
| Nov 12 | 18,210 | 17,856 | 354 |
| Nov 13 | 17,590 | 17,292 | 298 |
| Nov 16 | 17,140 | 16,724 | 416 |

**A2 build.** `realtime/options_iv_summary_read.py`, per underlying from its latest capture: ours (`local_iv`,
`local_delta`; whole days / 365 like the solver) with the provider's values put through the same steps as a cross-check;
per expiry the existing `chain_analysis.analyze_expirations` (ATM = median comparable IV at the two strikes bracketing the
forward; 25-delta by linear interpolation in delta, gap at most 0.15; IV comparability policy v1); tenors 7/30/60 by
total-variance interpolation between listed expiries at least one day out, flat within 5 days outside them, otherwise
unavailable with the reason; the ATM straddle mid at the strike nearest the forward and the one-sigma move per listed
expiry; put/call volume and OI over every contract. Route `GET /api/options/iv-summary?underlyer=&session_date=` behind
`REALTIME_V2_OPTIONS_IV_SUMMARY_READ` (OFF = 404), cached per capture. `iv_summary_from_daily_cursor` gives the same
summary for a past session from H1 (rehearsal: identical to the current-table read in all 6 cases below). Read time in
the rehearsal: 0.02-0.16 s per capture uncached, under 3 ms cached (a fresh process adds 2-3 s of imports on its first
call, which the API process has already loaded at start).

Evidence (`runner/g106_a2_iv_summary_evidence.py` over the CSV copies; output
`data-audit/g106-h1-a2-20261117/a2-iv-summary-evidence.json`), latest capture per name:

| Session | Name | Spot | Tenor | ATM IV (ours) | Provider ATM | Diff (vol pts) | 25d call | 25d put | RR25 | 1-sigma move | Method |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 11-12 | SPY | 756.75 | 7d | 0.153 | 0.150 | +0.26 | 0.132 | 0.182 | -0.050 | 2.1% | exact |
| 11-12 | SPY | 756.75 | 30d | 0.147 | 0.144 | +0.22 | 0.124 | 0.183 | -0.059 | 4.2% | TV interp |
| 11-12 | SPY | 756.75 | 60d | - | - | - | - | - | - | - | n/a: last expiry 49 d |
| 11-12 | QQQ | 687.81 | 7d | 0.269 | 0.266 | +0.31 | 0.235 | 0.312 | -0.077 | 3.7% | exact |
| 11-12 | QQQ | 687.81 | 30d | 0.244 | 0.242 | +0.18 | 0.213 | 0.288 | -0.075 | 7.0% | TV interp |
| 11-12 | QQQ | 687.81 | 60d | - | - | - | - | - | - | - | n/a: last expiry 49 d |
| 11-12 | NVDA | 165.45 | 7d | 0.891 | 0.895 | -0.38 | 0.876 | 0.949 | -0.073 | 12.3% | TV interp |
| 11-12 | NVDA | 165.45 | 30d | 0.594 | 0.606 | -1.17 | 0.583 | 0.633 | -0.050 | 17.0% | TV interp |
| 11-12 | NVDA | 165.45 | 60d | - | - | - | - | - | - | - | n/a: last expiry 49 d |
| 11-16 | SPY | 739.23 | 7d | 0.208 | 0.206 | +0.18 | 0.186 | 0.235 | -0.050 | 2.9% | exact |
| 11-16 | SPY | 739.23 | 30d | 0.188 | 0.186 | +0.17 | 0.153 | 0.234 | -0.081 | 5.4% | TV interp |
| 11-16 | SPY | 739.23 | 60d | 0.177 | 0.176 | +0.15 | 0.141 | 0.225 | -0.084 | 7.2% | exact |
| 11-16 | QQQ | 661.24 | 7d | 0.342 | 0.339 | +0.28 | 0.312 | 0.371 | -0.058 | 4.7% | exact |
| 11-16 | QQQ | 661.24 | 30d | 0.291 | 0.290 | +0.13 | 0.252 | 0.340 | -0.087 | 8.3% | TV interp |
| 11-16 | QQQ | 661.24 | 60d | 0.265 | 0.262 | +0.28 | 0.229 | 0.315 | -0.086 | 10.8% | exact |
| 11-16 | NVDA | 155.73 | 7d | 0.948 | 0.928 | +1.99 | 0.922 | 0.955 | -0.033 | 13.1% | exact |
| 11-16 | NVDA | 155.73 | 30d | 0.640 | 0.640 | -0.01 | 0.619 | 0.663 | -0.044 | 18.4% | TV interp |
| 11-16 | NVDA | 155.73 | 60d | 0.562 | 0.559 | +0.34 | - | 0.584 | - | 22.8% | exact (25d call outside the band) |

Put/call (volume / OI): Nov 12 SPY 1.09 / 1.45, QQQ 1.05 / 1.01, NVDA 0.83 / 1.26; Nov 16 SPY 1.33 / 1.45, QQQ 1.06 /
1.24, NVDA 0.82 / 1.35. Contracts in the latest capture (priced by us): Nov 12 SPY 5,046 (4,756), QQQ 5,374 (4,923),
NVDA 654 (636); Nov 16 SPY 4,670 (4,363), QQQ 4,738 (4,349), NVDA 650 (574); 56-90 older-capture rows per name left
out. Expected move (straddle at the strike nearest the forward vs one sigma): SPY Nov 16 1 d 1.1% vs 1.3%, 7 d 2.3% vs
2.9%, 60 d 5.7% vs 7.2%; NVDA Nov 16 7 d 10.4% vs 13.1%; the straddle / one-sigma ratio is 0.79-0.80 throughout (theory
sqrt(2/pi) = 0.798). Findings: (1) the 60-day tenor is missing whenever no listed expiry lies within 55-60 days, because
breadth stops at 60 DTE (Nov 12: last listed expiry Dec 31, 49 d); breadth to about 75 DTE would keep it bracketed (more
requests per window; not changed). (2) 25-delta calls at 60 d need strikes beyond +20% for high-IV names (NVDA). (3)
Same-day expiries at the close are left out of the tenors (0DTE ATM 0.68-1.39 three minutes before the close).

**Tests.** Unit: `test_options_iv_summary_g106_a2.py` 10 and `test_option_breadth_daily_g106_h1.py` 10 passed
(hand-checked: 7-day total variance between 5 d at 0.20 and 12 d at 0.30 = 0.2539484; ATM median 0.205; 25-delta 0.23 /
0.28, RR -0.05; straddle 4.5 = 4.5% of spot; flat and unavailable rules; the scheduler hook's timing; route 404 off, 200
on, 422 for a bad name; live plan from 054 has exactly 055 pending). DB (`test_option_breadth_daily_v2_db.py`, throwaway
Postgres): 8 passed (055 + hardening audit, refusal of an open session, CSV loaded once, exact copy once, backfill,
immutability and role denials, A2 from the daily table, CLI dry run / apply / refusals); with the migration and
provisioning DB suites (`test_retired_migration_ledger_v2_db`, `test_realtime_v2_audit_scripts`,
`test_provision_realtime_v2`): 29 passed, 3 failed = the same 3 failing on a clean export of 67d47b9 (21 passed, 3
failed). Full `tests/realtime` (no DB): 113 failed / 5,362 passed / 431 skipped on the worktree vs 152 failed / 5,304
passed on the clean export of 67d47b9; every worktree failure also fails there (0 new); the 39 extra there are
path-dependent (the export has no repository root, e.g. the compose files).

### Nov 17 (05:45-06:00 UTC): OS1/OS2 near misses - how close the unconfirmed stock-first signals came (build session, read-only)

Data: read-only exports of the live DB (statement_timeout 30 s): 1,774 OS evaluations Nov 10-16, the windows and their
lane states, the sessions' flow events (17k prints on watched contracts), watched sets (final slot set per cohort),
cutoff marks, 5m/1d bars of the 9 names, the last chain capture per session (`rt.option_breadth_current`, rows with
volume >= 1.5 x OI), and the Nov 12 5-minute full-chain copies (`research/chains/2026-11-12`, from 18:57). Scripts,
exports and outputs: `backend/.realtime-canary/.../recovery-20261104/evidence/os-nearmiss-20261117/` (`recon.py`,
`nearmiss.py`, `nearmiss2.py`, `chaincheck.py`, `*.json`, `data/`).

**The confirmation rule.** `_load_os` (native loader) runs `detect_canonical_stock_signals` (OS1: last two 5m bars,
inside the prior close +/- 0.5 x ATR14 then outside; OS2: last three, outside / pullback / resume) and pairs each signal
with every O1 input of the window on the same underlying and the matching type (call for +1, put for -1): the O1 inputs
are the prints (flow events) of the last 10 minutes on WATCHED contracts. The OS executor (`assess_canonical_stock_first`)
confirms when the pairing's O1 v1 finding is DETECTED: that contract's day volume at the print / its prior-session OI >= 3
(no liquidity floor); otherwise UNMATCHED `PARTICIPATION_BELOW_BASELINE`; a print received after the decision is
UNAVAILABLE `ACTIVITY_NOT_AVAILABLE_AT_DECISION`. A signal with no such print gets no evaluation at all. Two structural
facts found: (1) the detector reads only the latest 5m bars, so each signal exists in exactly ONE window (every stored
signal: 1 window; the 30-minute `valid_until` never applies); (2) the eligible set is "printed in the 10 minutes before
that one cutoff", not "has participation".

**Rebuild check.** The signals rebuilt from the stored bars for every stored window (`recon.py`) contain all 173 stored
signals (173/173); rebuilt 340. The live confirmation is reproduced exactly (21/21 confirmed, 0 extra), and the alert
model (print quoted + contract marked within 2 s before the cutoff) reproduces the stored package split (17 SELECTED /
17 OBSERVATION among confirmed pairings) and the stored OS session alerts (Nov 11 OS1 2 + OS2 4, Nov 12 OS2 1, Nov 16
OS2 1).

**Nov 13 (no OS evaluation).** Only 8 Options windows are stored (18:15-19:58 UTC; the V4 failures R65/R67-R69 kept the
window from running the rest of the day). OS1: BLOCKED at 18:15 (`OS1_*_E_CONTEXT_INCOMPLETE`), COMPLETE_ZERO in the 7
others; OS2: BLOCKED at 18:15 (E_CONTEXT), 18:19 (`OS2_BAR_MEMBERSHIP_UNAVAILABLE`) and 19:45 (E_CONTEXT), COMPLETE_ZERO
in 5. The bars give 3 signals in those windows, all QQQ bearish: OS2 18:15 (its window 18:19 had 1 pairable put print,
but the lane was BLOCKED), OS2 18:50 and OS1 19:45 (no put print on watched QQQ contracts in the 10 minutes before the
cutoff -> no invocation -> COMPLETE_ZERO). No lane error table exists (`rt.lane_errors` absent); the lane states are in
`rt.detector_datasets.summary->lanes`.

**Funnel (signals):**

| Session | Detector | Signals | Only in BLOCKED windows | No pairable print | Evaluated | Confirmed | Alerts |
|---|---|---|---|---|---|---|---|
| Nov 10 | OS1 / OS2 | 13 / 42 | 4 / 5 | 3 / 20 | 6 / 17 | 1 / 1 | 0 / 0 |
| Nov 11 | OS1 / OS2 | 14 / 110 | 0 / 0 | 3 / 44 | 11 / 66 | 2 / 9 | 2 / 4 |
| Nov 12 | OS1 / OS2 | 9 / 38 | 0 / 0 | 7 / 14 | 2 / 24 | 0 / 7 | 0 / 1 |
| Nov 13 | OS1 / OS2 | 1 / 2 | 0 / 1 | 1 / 1 | 0 / 0 | 0 / 0 | 0 / 0 |
| Nov 16 | OS1 / OS2 | 18 / 93 | 0 / 0 | 8 / 56 | 10 / 37 | 0 / 1 | 0 / 1 |
| Total | | 340 | 10 | 157 | 173 | 21 | 8 |

(Alerts = session alerts; Nov 10's confirmed pairings had no package: OBSERVATION.)

**How close (152 evaluated, unconfirmed):** best pairing ratio / 3 - median 0.15, p75 0.30, p90 0.43, max 0.88; within 10%
of the threshold 0, within 25% 4, within 50% 13 (Nov 11 OS2 8: NVDA 11/13 175C x4 (OI 18,921), QQQ 11/24 695C x3 (OI 483),
AAPL 11/13 352.5C; Nov 16 OS2 5: NVDA 11/27 172.5P x2 (OI 656), QQQ 11/23 670P x3 (OI 474)). The paired contracts are
the busy, high-OI ones (median OI 4,031, median DTE 10): their ratio is structurally low. By underlying (signals /
evaluated / confirmed / >= 0.5x): NVDA 64/61/9/6, QQQ 46/20/2/6, SPY 39/13/5/0, TSLA 34/21/5/0, IWM 48/8/0/0, NFLX
38/16/0/0, MSFT 28/3/0/0, AAPL 24/20/0/1, AMZN 19/11/0/0. O1 activity on the underlying in the same or next window: 69
of the 152 (64 only on the OTHER type), 27 of the 157 never paired (26 other type only).

**What-if (5 sessions; alerts = session alerts by the stored package rule):**

| Eligible set | k = 1.0 | 0.9 | 0.75 | 0.5 |
|---|---|---|---|---|
| L0 = live (prints of the last 10 min) | 21 sig / 8 alerts | 21 / 8 | 25 / 9 | 34 / 12 |
| L0 + O1 floor (OI >= 100, vol >= 500) | 19 / 6 | 19 / 6 | 23 / 7 | 32 / 10 |
| L1-30 (watched contract of the type, ratio at its latest print, print within 30 min) | 41 / 13 | 42 / 14 | 49 / 14 | 61 / 18 |
| L1-30 + floor | 37 / 10 | 38 / 11 | 45 / 11 | 57 / 15 |
| L1-60 | 61 / 14 | 62 / 15 | 70 / 16 | 86 / 20 |
| L1-60 + floor | 52 / 11 | 53 / 12 | 61 / 13 | 77 / 17 |
| L1-day | 91 / 16 | 92 / 17 | 97 / 18 | 113 / 22 |

(L1 alerts need the contract marked at the cutoff: a cutoff-mark package as AL2 builds; L1-60 extras come from NVDA 20,
SPY 13, TSLA 6, IWM 1.) The floor on L0 removes Nov 16's only OS alert (NVDA 11/25 167.5P, OI 42, volume 181: the thin
contract the O1 floor drops) and Nov 11's SPY 11/23 761C (OI 33).

**Watched set vs chain.** Nov 12 intraday (12 signals after 18:57, chain capture at or before the cutoff; contracts of the
type with ratio >= 3, DTE 0-45, strike within 7.5%, OI >= 100, volume >= 500): IWM bearish x6 - 17-24 such puts on the
chain (0DTE 278P/279P: ratio 54-95, 68k-150k contracts), 0-1 watched; NVDA bearish x6 - 18-19, 1 watched (11/13 155P,
which confirmed the two signals whose window had its print); NFLX x2 - none. Close upper bound (last capture; volume only
grows): Nov 16 (V4, 216 watched) - 60 of 110 unconfirmed signals had such contracts only outside the watched set; Nov
10-12 (72 watched) - 8 of 206 (most had a watched one by the close).

**Outcomes** (underlying move in the signal's direction from the trigger close; 5m bar closes; mean / median / share up):

| Group | n | +30 min | +60 min | to the close |
|---|---|---|---|---|
| A confirmed live | 21 | -0.09% / +0.04% / 57% | -0.13% / -0.03% / 48% | -0.56% / -0.34% / 24% |
| B near miss (L0 >= 0.5x) | 13 | +0.01% / -0.03% / 46% | +0.14% / +0.09% / 54% | -0.44% / -0.42% / 15% |
| C L1-60 would confirm (not L0) | 38 | -0.12% / +0.04% / 53% | -0.13% / -0.10% / 39% | -0.51% / -0.47% / 21% |
| D paired, far (< 0.5x), no L1-60 | 111 | +0.01% / +0.02% / 57% | +0.04% / +0.05% / 55% | -0.09% / -0.06% / 49% |
| E never paired, no L1-60 | 157 | +0.01% / -0.01% / 48% | +0.02% / +0.06% / 55% | +0.10% / +0.02% / 50% |

Confirmed and would-be-confirmed signals moved no better than the rest over 30-60 minutes and reversed into the close
on these 5 days (mostly NVDA/SPY/TSLA calls Nov 11, NVDA puts Nov 12); n is small and two days dominate.

**Reading and recommendation.**
- The baseline is not what blocks OS: no unconfirmed signal came within 10% of the ratio, 4 within 25%; 0.75x adds 4
  confirmations / 1 alert, 0.5x 13 / 4, all on high-OI longer-dated contracts. Keep the ratio at 3.
- The pairing is: each signal has one window, and in it only contracts that printed in the last 10 minutes can confirm
  (157 of 340 signals had no such print; the paired prints are the busy high-OI contracts). Confirming with the watched
  contract's day participation (L1-30: ratio >= 3 at its latest print, print within 30 minutes, the O1 floor on the
  confirming contract, packaged at the cutoff mark) nearly doubles confirmations (21 -> 37) and adds 2 alerts (8 -> 10).
  Keeping a signal alive for its 30-minute validity (re-evaluating it in the next windows) is the other half of the same
  gap.
- The watched set is too narrow for index-ETF 0DTE flow (Nov 12 IWM: 17-24 qualifying puts, none watched; Nov 16 V4: 60 of
  110 unconfirmed signals' qualifying contracts unwatched at the close bound): a watched-set (C6/B3 promotion) question,
  not a confirmation-policy one, and capacity-bound (R81).
- Apply the O1 liquidity floor to the OS confirming contract too (today OS uses O1 v1 directly: Nov 16's only OS alert
  was on OI 42 / volume 181).
- Proposed policy version (not built): `canonical_stock_option_participation_v2` = L1-30 + floor + signal persistence for
  its validity, behind `REALTIME_V3_OS_PARTICIPATION` OFF/RECORD/ON on the Options window, RECORD first with one line per
  signal (best L0/L1 ratio, contract, why not confirmed). Given the outcome table, keep it in RECORD until 2+ weeks show
  the confirmed set beating the unconfirmed one; turning it ON on today's evidence would add alerts of the same quality
  as today's OS alerts, which did not beat unconfirmed signals.

Not verified / limits: the watched set is each cohort's final slot set (intraday V4 reallocations not modelled); flow
events are the stored qualifying prints (not every trade); the chain is the last capture of the session except Nov 12
after 18:57 (a capture named HH:MM may finish ~1 minute after its name); outcomes are underlying moves, not option P/L.

**Addendum (Nov 17 06:15-06:50 UTC): AL5 - OS policy v2 built for RECORD (user's go ~06:10), and why Nov 16 had almost no
bearish confirmations** (branch `o1-liquidity-floor`, uncommitted; ON implemented and tested but NOT to be switched on).

Code: `realtime/advanced_options_os_participation.py` (new: switch, policy, participation per contract, the decision,
signal persistence, record lines), `realtime/advanced_options_native_loader.py` (`_load_os` captures its bars and
signals when the switch is not OFF; `_apply_os_participation` + `_os_participation_inputs` after both OS lanes;
`os_participation_package`), `realtime/advanced_options_native_adapters.py` (`AdvancedOptionsLaneInputs.record`, never
compared or hashed; `AdvancedOSParticipationInvocation`; the OS executor logs the record and evaluates v2 invocations),
`scripts/run_advanced_options_v3_production.py` (start-up check), `tests/realtime/test_os_participation.py`.

- Switch `REALTIME_V3_OS_PARTICIPATION` on `realtime-v3-options-p8-window`: OFF (default) = today, nothing read or
  captured; RECORD = OS1/OS2 confirmations, selection, packages, alerts and input identity unchanged, plus one
  `OS_PARTICIPATION_RECORD` line per live signal per window and one `OS_PARTICIPATION_WINDOW` line per lane per window;
  ON = v2's invocations replace the v1 pairings (one per live signal; package = the confirming contract at the cutoff
  mark, v1 single-contract identity, reused once stored); anything else: the WINDOW service refuses to start.
- Policy `canonical_stock_option_participation_v2` (sha256 `00cd2392...` with the default floor and alternative): ratio
  >= 3 at the watched contract's latest print, print within 30 minutes, the O1 liquidity floor (the env-configurable
  floor thresholds) on the confirming contract, signals re-detected from the session's last 9 5m bars as of each 5m
  boundary of the last 30 minutes (alive through `valid_until`; `first_window` marks the window that first saw it).
  Reasons `OSP_NO_WATCHED_PRINT_WITHIN_30_MIN`, `OSP_PARTICIPATION_BELOW_BASELINE`,
  `OSP_CONFIRMING_CONTRACT_BELOW_LIQUIDITY_FLOOR`; read failure `OS_PARTICIPATION_INPUTS_UNAVAILABLE:<error>` (RECORD keeps
  v1 and records it; ON blocks the lane, fail closed).
- Alternative, RECORD ONLY (never applied): candidates that pass the floor with ask-side (at/above ask) premium >=
  `REALTIME_V3_OS_PARTICIPATION_ALT_MIN_ASK_PREMIUM` (default 50,000), ask share of classified (ask + bid) volume >=
  `..._ALT_MIN_ASK_SHARE_PCT` (default 60) and classified volume >= `..._ALT_MIN_CLASSIFIED_VOLUME` (default 200); reasons
  `OSP_ALT_*`.
- Record line: detector, underlyer, direction, signal id and market time, valid_until, first_window; v1 outcome
  (CONFIRMED / UNMATCHED / UNAVAILABLE / NOT_EVALUATED_NO_PAIRING / NOT_LIVE_IN_V1), v1 pairings, best v1 ratio, v1
  selected; v2 outcome, contract, packageable, reasons, candidates and the best contract (ratio, OI, day volume, last
  print age, floor pass and reasons, watched, flow-event volume by aggressor side - ask / bid / inside / complex /
  unclassified - ask share, classified / complex / unclassified shares, flow / ask / bid premium); the same for the
  alternative. Window line: counts v1 vs v2 vs alternative (signals, first-window, confirmed, packageable), v2 reasons,
  policy hash, floor and alternative thresholds, lane status.
- Extra reads (RECORD/ON only, one savepoint): underlying instruments, the last 9 5m bars of the 9 names, the session's
  prints on watched contracts of the underlyings that have a live signal (index cohort/underlyer/time; empty when no
  signal is live), the watched contracts and cutoff marks (AL2's reader). R93 note: up to a few thousand SIRead locks
  more late in the day, and the loader assembles more than once per window: watch the window time.

**Parity (OFF).** Real `_load_os` (fake cursor: receipt + context bars) over real `_load_o1` inputs, then the real OS
executor: main 67d47b9 and the branch (unset / OFF, and with the RECORD capture) give the same OS1 input identity
`a65c96ec...` and lane digest `e90e2ed3...` (2 evaluations: CONFIRMED + UNMATCHED) and the same statements; OS2 (no
signal) `4f53cda1...` / `f5297957...` both. RECORD's lanes, identities and evaluations equal OFF's (tests); the O1 lane
digest stays `7dad1b5e...`.

**Replay through the code** (`replay_v2.py`: `live_signals` + `contract_participation` + `evaluate_os_participation` on
the stored bars, prints received by the cutoff, watched sets and cutoff marks; every stored window with the lane not
BLOCKED): first-window only (as in the near-miss replay) v2 with the floor = 37 confirmed signals / 10 alerts, exactly
the reported L1-30+floor (the first run gave 38: prints that arrived after the cutoff, now excluded as the live
source-offset bound does); with the 30-minute validity: 53 / 13. 342 distinct signals (330 first windows + 12 seen only
after a BLOCKED first window); 1,770 signal-windows.

| Session | Direction | Signals | v1 confirmed / alerts | v2 first window | v2 with 30-min validity | Alternative (record only) |
|---|---|---|---|---|---|---|
| Nov 10 | bear | 56 | 2 / 0 | 2 / 0 | 2 / 0 | 5 / 1 |
| Nov 11 | bull | 105 | 11 / 6 | 22 / 9 | 35 / 12 | 44 / 13 |
| Nov 11 | bear | 19 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Nov 12 | bull | 3 | 0 / 0 | 0 / 0 | 0 / 0 | 3 / 1 |
| Nov 12 | bear | 44 | 7 / 1 | 13 / 1 | 16 / 1 | 4 / 1 |
| Nov 13 | bear | 4 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Nov 16 | bear | 111 | 1 / 1 | 0 / 0 | 0 / 0 | 0 / 0 |

Alternative grid (30-minute validity; bear / bull confirmed of 234 / 108; ask premium $10k-$50k, $100k in brackets): ask
share >= 50%: 34 / 60-61 (33 / 59), 55%: 23 / 60 (23 / 59), 60%: 9 / 47 (9 / 43), 65%: 7 / 46 (7 / 43); the premium hardly
binds, the share does. Defaults kept at $50k / 60% / 200: about as selective as v2 on bulls, and it does not lift bears.

**Nov 16: why so few bearish confirmations** (all 9 names down 1.0-4.1%; 111 bearish signals; v1 confirmed 1 - on the
thin NVDA 11/25 167.5P, OI 42 - v2 and the alternative 0):
- The ratio favours the side opposite to the day's move. Contracts within 3% of spot, DTE 0-7, last capture: Nov 16 puts
  OI 2.97M vs calls 0.93M (3.2x), volume puts 12.6M vs calls 9.8M, volume/OI puts 4.24 vs calls 10.62, contracts with
  ratio >= 3: calls 412 vs puts 280. The up day Nov 11 (7 of 9 up) mirrors it: call OI 1.57M vs put 0.81M, volume/OI
  calls 8.12 vs puts 12.72, ratio >= 3: puts 355 vs calls 212. By name on Nov 16, DTE 1-7 near spot: SPY puts 1.21 vs
  calls 4.25, QQQ 1.19 vs 3.88, NVDA 0.93 vs 3.69, IWM 0.33 vs 1.29 (0DTE is high on both sides).
- The watched set was call-heavy: 131 calls / 85 puts (0DTE 21 / 7). Of the near-spot contracts that cleared ratio >= 3
  and the floor at the close, 53 of 257 calls were watched (21%) but 6 of 254 puts (2%). Flow events on watched contracts:
  calls 160,200 contracts, puts 11,672 (7%); O1 detected 722 call events and 16 put events (2 thin contracts).
- Aggressor side does not rescue it: on watched puts the ask share of classified volume was 0.43 (calls 0.46), and
  there was little put flow to classify.
- So the main lever for bearish coverage is the watched-set allocation (put/call balance following the day's flow,
  G106 B3/C6), not the confirmation rule; v2 and its alternative record the gap per signal from the first RECORD session.

**Tests** (offline, r121 image): `test_os_participation.py` 19 passed (switch values and the WINDOW start check, policy hash
and thresholds, participation by aggressor side, the decision - recency edge, type and underlying, ratio edge, the floor on
the confirming contract, the env floor - the alternative and its reasons, a signal kept alive 29 minutes and expired at
30 while v1 sees it in one window only, RECORD = today's evaluations + one record line per signal + the window line, a
record failure never fails the window, ON evaluations and package, a blocked lane, the loader's RECORD/ON/failure paths and
the real `_os_participation_inputs` over a fake cursor); with `test_o1_liquidity_floor.py` and `test_o1_debit_vertical.py`
117 passed. Neighbours (55 files incl. OS1/OS2 shadow, OS detectors, stock bar detection, native loader/adapters,
dual_origin): branch 17 failed / 1,146 passed / 66 skipped vs main 17 / 1,050 / 66, the identical (environmental) failure
set; +96 = the new tests.

**Operator steps** (can ride in the same pre-open window hotfix as the floor's RECORD):
`python -I runner/compose_env.py realtime-v3-options-p8-window REALTIME_V3_OS_PARTICIPATION RECORD`, then the window
hotfix on the batch image (started at ~X:04:05 of the 5-minute cycle). Check the first windows:
`docker logs --since 15m stock-screener-realtime-v3-options-p8-window 2>&1 | grep OS_PARTICIPATION_WINDOW | tail -4` -
two lines per window (OS1, OS2), `status` OK or LANE_BLOCKED (with the lane's reasons), never
`OS_PARTICIPATION_INPUTS_UNAVAILABLE:*` or `RECORD_FAILED`; OS1/OS2 lane states and alert counts as before; window times.
Undo: `--remove` the key + hotfix (absent = OFF). After 1-2 sessions look at: v2 vs v1 confirmed signals per detector
and direction; the reasons mix (NO_WATCHED_PRINT vs BELOW_BASELINE vs BELOW_FLOOR); best-contract ratios of bearish
signals and how many have no put candidate at all (watched-set gap); `first_window` false confirmations (what the
30-minute validity adds); packageable share; alternative vs v2 by direction; then the underlying's move after
v2-confirmed vs unconfirmed signals before any ON decision.

### Nov 17 (06:50-07:30 UTC): AL6 O1 0DTE gate and AL7 O1 aggressor-side direction built RECORD-first; AL5 flow20 addendum (build session, not committed, not deployed)

Worktree `g102-batch-nov17` (uncommitted integration tree). Research basis: `G106_DETECTOR_RESEARCH.md` sections 3-4 and
7 P1-1 / P1-2 / P1-4. No live change, no `g102-test-pg`.

**Code.**
- `realtime/advanced_options_o1_gates.py` (new): switches, policies, the v3 finding, record documents and the executor
  helper `O1GateRun` (built only when a gate is not OFF; both OFF = no object, today's path).
  - AL6 `REALTIME_V3_O1_ZERO_DTE` (OFF / RECORD / ON), policy `option_participation_min_dte_v1` with
    `REALTIME_V3_O1_MIN_DTE` (default 1; hash `f3fd5fd1...`). DTE = expiration - volume session in calendar days; under
    the minimum: `O1ZD_EXPIRES_TODAY` (DTE 0) or `O1ZD_DTE_BELOW_POLICY` (0 < DTE < minimum).
  - AL7 `REALTIME_V3_O1_AGGRESSOR_SIDE` (OFF / RECORD / ON), policy `option_participation_aggressor_side_v1` with
    `REALTIME_V3_O1_AGGRESSOR_MIN_DOMINANCE_PCT` (default 200) and `REALTIME_V3_O1_AGGRESSOR_MIN_SIDE_PREMIUM` (dollars,
    default 25,000) (hash `df625f64...`). Counted ask / bid = AT_OR_ABOVE_ASK / AT_OR_BELOW_BID at HIGH or MEDIUM, over
    the contract's stored prints this session at or before the event (INCLUDED, revision 1, the window's cohort, source
    flow and committed offset, up to the cutoff). BOUGHT: ask > bid, ask x 100 >= 200 x bid, ask >= $25k (= research
    E1 exactly; 200% = an ask share of the classified premium >= 2/3). SOLD: the mirror (S1). UNCLEAR: no stored print,
    no classified premium, no dominant side, dominant side under $25k, or the prints could not be read.
  - Values: unset / blank = default (built-in policy and hash); digits only; dominance >= 100; read once per process.
- `realtime/advanced_options_native_adapters.py` (O1 executor, additive): the gates run after the floor - floor, then
  0DTE gate, then aggressor - each on the finding the ON gates before it left. RECORD gates change nothing. ON: the
  evaluation payload is the v3 finding `option_participation_finding_v3` (gate policy hash over the base policy: v1, or
  the floor's when the floor is ON; the base finding carried whole; DTE; gate reasons; aggressor evidence; `direction`
  +1 / -1 only for a BOUGHT event under the aggressor gate). 0DTE ON: NOT_DETECTED / NOT_SELECTED (grouping and session
  alert re-run on what is left, as the floor). Aggressor ON: BOUGHT SELECTED as today (direction = type, which the
  projection already derives from the package legs), SOLD / UNCLEAR OBSERVATION without a package. A gate error never
  fails the window (RECORD keeps the finding; ON leaves the event out, `O1GATE_FAILED:<error>`); logging never fails it.
  `AdvancedO1Invocation` gained `aggressor` (the tally; `compare=False`).
- `realtime/advanced_options_native_loader.py` (`_load_o1`, additive): with AL7 RECORD/ON and O1 rows, one guarded read
  (`SAVEPOINT o1_aggressor_inputs`) of the session prints of the contracts whose event reached the ratio (day volume
  >= 3x prior OI; index `(cohort_id, contract_ticker, event_time)`); a failed read rolls back to the savepoint and every
  tally says O1AG_INPUTS_UNAVAILABLE (RECORD records it; ON = UNCLEAR, no package). Input identity: unchanged in OFF /
  RECORD; ON adds each event's tally.
- `scripts/run_advanced_options_v3_production.py`: the WINDOW service checks both switches and the three thresholds at
  start (refuses to start on an invalid value).

**Record lines.**
- `O1_ZERO_DTE_RECORD` (one per window): mode, policy hash, minimum DTE, detected, would_drop (+ selected, + also below the
  floor), selected and alerts without / with the gate, alerts dropped / refilled, reasons, by underlying / type, up to 40
  events (window alerts first: contract, DTE, OI, volume, ratio, entry mark, without / with gate, window_alert,
  below_floor) and up to 30 `instead` rows (lists capped under Docker's 16 KiB line split; counts always complete).
- `O1_AGGRESSOR_RECORD` (one per event the gate sees as detected): contract, type, DTE, today's direction, class, implied
  direction, reasons, prints, ask / bid / classified / inside / complex / stale / low-confidence / other / total premium,
  ask and bid share, coverage share, ratio, OI, volume, entry mark, below_floor, zero_dte_would_drop, without / with
  gate, window_alert.
- `O1_AGGRESSOR_WINDOW`: status (OK / INPUTS_UNAVAILABLE:<error>), thresholds and hash, counts by class, by type x class,
  implied vs today (agrees / opposite / none), unclear reasons, selected and alerts without / with the gate,
  `alerts_all_gates` (floor + 0DTE + aggressor as evaluated), refills, premium of each contract's latest event, by
  underlying, `instead`.

**Parity** (`evidence/al67-flow20-20261117/parity.py`: AL4's `offparity.py` rows through the real `_load_o1` + O1
executor, with a cursor that also answers the AL7 read): main 67d47b9 and the branch with all three switches unset /
OFF / every RECORD combination: lane `7dad1b5e...`, input identity `3abf7b0e...` (AL4 rows); a second row set with two
0DTE contracts: `d9ac80d6...` / `022dda2c...` on main and on the branch OFF / RECORD. Floor ON with both gates RECORD =
floor ON alone (`8179a725...`). The loader issues no extra statement unless AL7 is RECORD/ON.

**Tests.** `tests/realtime/test_o1_gates.py` 51 (switches and loud failure, built-in hashes pinned, thresholds stamped /
invalid / read once, the service start check, DTE reasons, E1 / S1 edges incl. exactly 2x and $25k, ties, low
confidence, causal tallies, v3 finding round trip and tamper refusal, OFF byte parity, RECORD unchanged + line contents,
0DTE ON, aggressor ON incl. the session-alert refill to a later BOUGHT event, order floor -> 0DTE -> aggressor, floor
RECORD flags, the hold switch, gate / logging / read failures, loader statements, prefilter and identity). With the
neighbours (`test_os_participation.py`, `test_o1_liquidity_floor.py`, `test_o1_debit_vertical.py`,
`test_advanced_options_native_adapters.py`): 187 passed. 29 related files (dual_origin, loaders, native loader / adapters,
OS1/OS2 shadow, OS detectors, stock bar detection, P8 docker runtime, deployment, session alerts, alert history,
production script, ...): 6 failed / 839 passed / 52 skipped - the 6 are
`tests/options/test_dual_origin.py` API tests that need DB_USER / DB_PASSWORD and fail identically on main.

**AL7 vs the research, through the code** (`evidence/al67-flow20-20261117/e1_check.py`: `o1_aggressor_tally` +
`classify_aggressor` with the default policy over the research's stored prints, its alert unit = first trigger per
session, contract and direction, with the floor and ratio >= 3). With the research's print order (the contract's prints
up to and including the triggering print by time, then event id) the code gives E1 = 27 (5/2, 6/3, 1/1, 5/0, 4/0) and
S1 = 26 (2/6, 2/1, 2/2, 0/6, 0/5), both exactly the research. The live rule counts every print at or before the event's
time (a same-millisecond sweep is counted whole, not split by event id): E1 = 26, the one difference a Nov 11 bearish
alert decided by a same-timestamp tie. Of the 2,479 prints at ratio + floor: 299 BOUGHT, 811 SOLD, 1,369 UNCLEAR.

**AL5 addendum: flow20** (user, ~07:05 UTC: OS1/OS2 missed the bearish day).
- `realtime/advanced_options_os_participation.py`: per live signal in RECORD and ON, over (cutoff - 20 min, cutoff], all
  watched contracts of the name (both types), revision-1 prints at HIGH/MEDIUM: bullish = ask-side calls + bid-side
  puts, bearish = ask-side puts + bid-side calls (research `os_flow.py` `bias`, the same quantity: bias = net in the
  signal's direction). Confirmed when net > `REALTIME_V3_OS_PARTICIPATION_FLOW20_MIN_NET_PREMIUM` (default 0 = the
  research's `bias > 0`), |imbalance| x 100 >= `..._FLOW20_MIN_IMBALANCE_PCT` (default 0), classified >=
  `..._FLOW20_MIN_CLASSIFIED_PREMIUM` (default 0). The thresholds are their own document (`flow20_sha256`), so the v2
  policy hash stays `00cd2392...`.
- `OS_PARTICIPATION_RECORD` gains `flow20` {window_start, bull / bear / net / classified / total premium, imbalance,
  coverage, prints, confirmed, reasons} and `levels` (from the signal itself, no read: its last 5m close vs the prior
  day's high / low / close in the detector's ATR14; the opening range is not included because the window loads only the
  last 9 5m bars). `OS_PARTICIPATION_WINDOW` gains `flow20_signals` and `flow20_confirmed` per direction (bull / bear),
  the thresholds, their hash and the confirmation basis.
- Unused ON path: `REALTIME_V3_OS_PARTICIPATION_CONFIRMATION=FLOW20` (default RATIO; anything else refuses start) makes
  flow20 the v2 confirmation (contract = the floor-passing candidate of the signal's type with the largest ask-side
  premium; reasons `OSP_FLOW20_NOT_CONFIRMED` / `OSP_FLOW20_NO_CONFIRMING_CONTRACT`) and is stamped into the v2 policy.
- Loader: the AL5 prints read also selects `side_confidence` and `revision` (RECORD/ON only).
- Parity: AL5's `osparity.py` on main and the branch, unset / OFF / RECORD: OS1 `a65c96ec...` / `e90e2ed3...`, OS2
  `4f53cda1...` / `f5297957...` (unchanged). Tests: `test_os_participation.py` 28 (19 + 9 new: tally, record, thresholds
  and hashes, invalid values and the service start, ratio confirmation unchanged with flow20 present, the FLOW20 path,
  lines and window counts, the loader).
- Replay through the code (`evidence/al67-flow20-20261117/replay_flow20.py` over the AL5 exports; 342 signals, stored
  windows, lanes not BLOCKED, prints received by each cutoff):

| Session | Bull signals: v1 confirmed / v2 first window / flow20 first window / flow20 any window | Bear signals: v1 / v2 first / flow20 first / flow20 any |
|---|---|---|
| Nov 10 | 0 | 56: 2 / 2 / 12 / 24 |
| Nov 11 | 105: 11 / 22 / 49 / 71 | 19: 0 / 0 / 1 / 1 |
| Nov 12 | 3: 0 / 0 / 2 / 3 | 44: 7 / 13 / 11 / 21 |
| Nov 13 | 0 | 4: 0 / 0 / 0 / 2 |
| Nov 16 | 0 | 111: 1 / 0 / **30** / 55 |
| Total | 108: 11 / 22 / 51 / 74 | 234: 10 / 15 / 54 / 103 |

  - Measured at the signal bar's end over every stored print (the research's point and data), flow20 gives 0/14, 45/1,
    3/9, 0/0, 0/30 (research 0/16, 45/1, 3/9, 0/0, 0/30) and at > $100k 0/8, 13/0, 1/2, 0/0, 0/3 (research identical,
    27). Per signal the code and the research agree on every signal both sets contain (0 differences,
    `reconcile.py`); the Nov 10 gap is 3 research signals (SPY OS1 16:00, IWM OS2 16:00, NVDA OS2 15:40, all bearish
    with bias > 0) that the code's window loop never sees as live in a non-BLOCKED stored window (2 of the 342 code
    signals are not in the research's 340).
  - At the window cutoff (first window) flow20 confirms 51 / 54, against v2 22 / 15 and v1 11 / 10; on Nov 16 30 of
    the 111 bearish signals vs 0 under v2. Over the 30-minute validity (any window) 74 / 103; outcomes per window are not
    replayed here (the research's +60 min hit 0.63 is at the bar end).
  - Packaging under the unused FLOW20 path is not replayed (it needs the cutoff marks of the chosen contract).

**Operator steps** (same pre-open window hotfix as the floor's and AL5's RECORD; RECORD only):
`python -I runner/compose_env.py realtime-v3-options-p8-window REALTIME_V3_O1_ZERO_DTE RECORD` and
`python -I runner/compose_env.py realtime-v3-options-p8-window REALTIME_V3_O1_AGGRESSOR_SIDE RECORD` (flow20 needs no key:
it rides on `REALTIME_V3_OS_PARTICIPATION=RECORD`), then the window hotfix on the batch image. Thresholds stay unset
(defaults). Check: `docker logs --since 15m stock-screener-realtime-v3-options-p8-window 2>&1 | grep -E
"O1_ZERO_DTE_RECORD|O1_AGGRESSOR_WINDOW" | tail -4` - one of each per window, `mode` RECORD, `applied` false,
aggressor `status` OK (never INPUTS_UNAVAILABLE:* or RECORD_FAILED); O1 lane states, alert counts and window times as
before. Undo: `--remove` the keys + hotfix (absent = OFF).

### Nov 17 (08:45-10:00 UTC): AL8 call/put-balanced V4 watched set built RECORD-first (build session, not committed, not deployed)

Worktree `g102-batch-nov17` (HEAD 05ae39c = image r132, uncommitted, additive). User's yes ~08:30 UTC. Research basis:
`G106_DETECTOR_RESEARCH.md` 2.3 and P1-3. No live change; light read-only exports only (below); no `g102-test-pg`.

**Cause (verified in code, `realtime/advanced_options_activity_momentum_c6.py`).**
- The v4 SHORT_DATED_ACTIVITY basket (0-5 DTE) carries `call_priority: True` (hard-coded basket table for v2-v4).
- `_group_rank` sorts by (basket priority, role priority, **side priority** (call 0 / put 1 when the basket has call
  priority), candidate ranks, ...): every 0-5 DTE call ranks before every 0-5 DTE put of the name. The role priority is
  the same for both sides (PARTICIPATION_CONTEXT is in both groups' roles), so the side priority decides.
- `_v4_fill_product_buckets` fills each bucket first-match from that queue ("no side quota"); the basket rounds
  (12 short / 12 core), the share round and the budget round are first-match too. The per-side safeguards of the older
  profiles do not run for v4: v2 bootstraps one call and one put in each of the short and core baskets, v3 the core
  basket plus the missing-side rule of its near / end-of-week expiry targets, v4 none (`basket_bootstrap = ()`, the
  product buckets replace the expiry targets). Re-checked: the JSON flags `short_basket_put_coverage_when_eligible`,
  `core_basket_put_coverage_when_eligible` and `short_basket_call_preference` are not read by the code at all (they
  only enter the profile hash); the v2-v4 basket table is hard-coded, and its `put_coverage_required_when_available`
  only matters in the legacy profile's bootstrap.
- Effect at 24 per name: puts enter the short-dated buckets only when the calls of that bucket run out. Nov 16 (a
  Monday): 131 C / 85 P; 0-5 DTE 73 / 16; DTE 1-2 37 / 0 (the index ETFs' NEAR bucket 18 / 0; the stocks' 0-2 DTE
  weeklies 30 / 0). `SHORT_DATED_SWEEP_LIKE` (call-only role) only routes the P8 router's `short-dated-sweep-like-v2`
  consumer; it does not change the rank.

**Where the allocation runs.** Only in the Options lifecycle service (`realtime-v3-options-lifecycle`,
`scripts/run_advanced_options_v3_lifecycle.py`): `PostgresAutomaticOptionsPreparationExecutor._capacity` ->
`_allocate_capture` -> `allocate_activity_momentum`, in the CAPACITY_SHARDS stage, once per session (the first
underlying's claim computes it; the others reference its hash). The stage follows CATALOG_QUOTE, which waits for the
capture after the open: Nov 16 CATALOG_QUOTE 14:31:44-14:33:54, CAPACITY_SHARDS 14:35:10 UTC; Nov 13 14:35:26. The
market-state, flow, window, scheduler and API services only read the stored allocation document
(`activity_momentum_allocation_from_payload`). The manual `prepare_advanced_options_v3_pre_session.py` also allocates
(not a service; unchanged, always OFF). At 08:5x UTC today the Nov 17 revision `e9ec92b3...` (16 names) was in
CATALOG_QUOTE RETRY (waiting for the open), so its CAPACITY_SHARDS stage has not run.

**Code.**
- `realtime/advanced_options_slot_side_balance.py` (new): the switch `REALTIME_V3_OPTIONS_SLOT_SIDE_BALANCE`
  (OFF / RECORD / ON, any case, unset / blank = OFF, anything else raises naming the key), the record documents, and
  `allocate_with_side_balance` (the lifecycle's allocation under the switch; recording never raises).
- `advanced_options_activity_momentum_c6.py` (additive): `allocate_activity_momentum(..., side_balance=False)` (v4
  only; True with another profile raises); `_group_rank(neutral_side=False)`; `_v4_fill_product_buckets_side_balanced`;
  `_side_balanced_next` / `_side_counts` / `_group_side`; `_v4_bucket_context` and `v4_contract_bucket` (the record's
  bucket label). The three first-match `next(...)` rounds are the same generator either way; OFF takes `next(rows,
  None)` exactly as before.
- `advanced_options_automatic_preparation.py`: the executor takes `slot_side_balance` (default OFF; not OFF with a
  non-v4 admission profile = configuration invalid); `_allocate_capture` builds the same arguments once (the priority
  read is still one query) and calls `allocate_with_side_balance` (`getattr` default OFF for executors built with
  `__new__`).
- `scripts/run_advanced_options_v3_lifecycle.py`: reads the switch at start (invalid -> ValueError, the service does not
  start), passes it to the executor and prints `OPTIONS_SLOT_SIDE_BALANCE_SETTING {"mode": ...}` to stderr once.

**Balanced allocation (ON, `side_balance=True`).**
- Sort: side priority neutral (the allocation sort only); calls and puts keep one quality ranking (target |delta| band,
  activity, notional, spread, distance, expiry distance).
- Each product bucket in order: the best admissible group leads (either side), followed by the best admissible group of
  the other side at the same expiry (same ranking, so about the same |delta| distance from the target), else of the
  other side at any expiry of the bucket; each side capped at half the bucket (rounded up); the ladder's two expiries
  stay interleaved primary first. A side without supply leaves its slots to the later rounds. (A first version
  interleaved all (expiry, side) lanes round robin; on Nov 16 it spread CORE across every listed expiry and took a deep
  ITM AAPL 11/30 put with OI 0, so it was replaced by the pairs.)
- The basket rounds take the next group of the side with fewer contracts in that basket; the share and budget rounds the
  side with fewer contracts of the name; a mixed group counts for either side; when the wanted side has none, the first
  row as before.
- No put-side sweep role: the role is in the group identity (`group_id`) and in the profile's role contract, so adding it
  would change OFF; balanced puts keep PARTICIPATION_CONTEXT / DIRECTIONAL_MOMENTUM routes, and O1 / OS / flow read
  every watched contract whatever its roles.
- ON is not stamped into the allocation document (its schema is read by market-state, flow, window and the API; a new
  field would need all readers). The stored `short_expiry_evidence.selected_counts` (per expiry and side) and the
  allocation hash show it; the ON log lines say `applied: true`.

**Record lines** (stderr of the lifecycle, RECORD and ON, one allocation per session): one summary
`OPTIONS_SLOT_SIDE_BALANCE_RECORD {part: summary, mode, applied, session_date, profile_version, off / balanced
allocation sha256, contracts {off, balanced} calls / puts, dte_calls_puts (0-5, 1-2, 6-21, 22+), bucket_calls_puts
(product:bucket), added / removed by side, thin_added (+ by OI and by prior volume), thin_policy}` and one line per
underlying `{part: underlying, product, off / balanced calls / puts, buckets {bucket: off / balanced [calls, puts]},
added / removed (contract, side, bucket, DTE, |delta|, prior OI, day volume and its session, thin flags)}`. Largest
replayed line 5.2 KB (Docker splits at 16 KiB). A failure logs `OPTIONS_SLOT_SIDE_BALANCE_RECORD_FAILED {mode,
session_date, error}`; the stage goes on with the OFF allocation (RECORD) or the balanced one (ON, computed first).
Bucket label = the first product bucket (in order) the contract matches, else UNBUCKETED (picked by the basket rounds,
e.g. the stocks' 0-2 DTE weeklies on a Monday).

**Thin picks (record only, not applied).** `thin_oi` = prior OI missing or < 100; `thin_prior_volume` = day volume <
500 only when the provider's day row predates the session. The capture runs after the open, so a contract that traded in
the first minutes carries today's volume (Nov 16: 1,242 of SPY's 4,102 captured snapshots had a day row from the
session): that volume is not judged (`day_volume_session` SESSION).

**Parity (OFF and RECORD = today).** `evidence/al8-slot-balance-20261117/replay.py`: the stored CATALOG_QUOTE captures
of the Nov 13 (`2d1e2667...`) and Nov 16 (`7fabaec0...`) revisions (read-only export, 4.7 MB) through the real
`_combined_capture` and `_allocate_capture` with the stored admission configuration: allocation `a6d0069a...` (Nov 13)
and `2ec07d6e...` (Nov 16) = the stored CAPACITY_SHARDS `allocation_sha256`, and the filtered capture the stage stores
(`semantic_sha256` `78eeddd8...` / `27cffbd5...`, 216 contracts each) = the stored evidence, at HEAD 05ae39c and on
the branch with the switch unset, OFF and RECORD (RECORD prints 20 lines, unset / OFF none). Golden in the tests: the
fixture's OFF allocation `9356dad4...` pinned at HEAD before the change, equal with the flag False and through the
service helper (one allocation call, no `side_balance` argument, no line).

**Tests.** `tests/realtime/test_options_slot_side_balance_al8.py` 37 (golden OFF, flag False and helper parity, the
call-heavy shape, v4-only and boolean flag, `_group_rank` unchanged unless neutral, ON balance per name and count kept,
bucket pass caps per side and the share bound, expiry pairs, pairs from the same ranking, other-side fill without
supply, side-balanced next, the six accepted values and six invalid ones, executor refusal (invalid, non-v4), lifecycle
main passes the switch / refuses an invalid value before building the executor, RECORD = OFF allocation and payload +
three lines with consistent added / removed and thin flags, lines are JSON under 16 KiB, record failure and emit
failure never fail the stage, ON through the helper, ON keeps the balanced allocation when the record fails, helper
refuses non-v4, `_allocate_capture` routes through the helper). Neighbours (16 files: c6, v4 profile, lifecycle,
option slot set + repository, universe sizing x3, lifecycle isolation, v4 conventions, advanced services, treasury
refresh, c8 tier binding, selection c6 / session state DB, write grants): 2 failed / 272 passed / 30 skipped at HEAD
and after (+37 new passed), the same 2 (`test_options_preparation_budget_has_local_and_shared_ceilings`,
`test_adjusted_expired_or_ineligible_catalog_members_are_rejected[7-value5]`). Wider sweep (the 105 non-DB files `tests/realtime/test_(advanced_options|c6|c8|option|options)_*.py`): branch 23 failed / 1,835 passed / 2 skipped (with the 37 new); HEAD 05ae39c (backend exported with `git archive` into a scratch copy) 29 failed / 1,792 passed: all 23 are in HEAD's set (0 new); HEAD's other 6 (acquisition runtime compose, P8 canary authorization x2, v3 deployment x3) read repository-root files the backend-only copy lacks and pass on the branch.

**Replay: OFF vs balanced** (`analyze.py`; 9 names, 24 per name; the Nov 17 names AMD AVGO COIN GOOGL MSTR MU PLTR have
no stored capture before today, so 16 names cannot be replayed; per name the allocation is independent of the name
count at 24 each (budget 384 <= 900)):

| | Nov 16 OFF | Nov 16 balanced | Nov 13 OFF | Nov 13 balanced |
|---|---|---|---|---|
| Watched C / P | 131 / 85 | 107 / 109 | 129 / 87 | 107 / 109 |
| 0-5 DTE C / P | 73 / 16 | 44 / 45 | 46 / 14 | 28 / 30 |
| DTE 1-2 C / P | 37 / 0 | 17 / 17 | 0 / 0 | 0 / 0 |
| 6-21 DTE C / P | 46 / 57 | 51 / 52 | 71 / 61 | 67 / 67 |
| ETFs: SAME_DAY / NEAR / WEEKLY / CORE (C / P) | 10/7, 18/0, 3/3, 8/23 | 8/9, 9/9, 3/3, 15/16 | 10/8, 3/3, 9/10, 8/9 (+9/3 unbucketed) | 9/10, 3/3, 9/10, 9/8 (+5/6) |
| Stocks: WEEKLY / CORE / EXTENSION / 0-2 DTE (C / P) | 12/6, 38/34, 12/12, 30/0 | 9/9, 36/36, 12/12, 15/15 | 42/14, 36/28, 12/12, - | 27/27, 33/33, 12/12, - |
| Per name | 12/12 (QQQ) to 18/6 (MSFT) | 12/12 (IWM 11/13) | 11/13 (SPY) to 18/6 (TSLA) | 12/12 (SPY 11/13) |
| Added / removed (C, P) | | +16 C +40 P / -40 C -16 P | | +12 C +37 P / -34 C -15 P |

Example (Nov 16 SPY, the NEAR bucket): OFF took 6 calls (11/17 749C, 11/18 748C / 751C, ...); balanced 3 + 3
(11/17 747P OI 1,019, 748P OI 1,309, 11/18 750P OI 2,066; |delta| 0.46-0.55), and CORE 5 / 5 instead of 3 / 7.

**Close-chain passes the watched set would cover** (`research/chains/<session>/close-retained.csv.gz`, the 9 names,
ratio >= 3 with the AL4 floor (OI >= 100, day volume >= 500) at the close; the brief's definition is "within 3% of spot,
DTE 0-7", which reproduces today's 53 of 257 / 6 of 254 exactly):

| Nov 16 | Passes C / P | OFF watched C / P | Balanced watched C / P |
|---|---|---|---|
| Within 3%, DTE 0-7 | 257 / 254 | 53 / 6 | **31 / 22** |
| Within 3%, DTE 1-7 | 164 / 161 | 32 / 0 | 16 / 7 |
| Within 2%, DTE 0-45 (research 2.1) | 209 / 227 | 51 / 6 | 29 / 21 |
| All strikes and DTE (research 2.3: 57 / 6) | 429 / 399 | 57 / 6 | 33 / 22 |

Nov 13 (within 3%, DTE 0-7): passes 244 / 215; OFF 34 / 4; balanced 25 / 10. The balanced set multiplies the watched
put passes by 2.5-3.7 and cuts the call passes by a quarter to 40%; the total watched passes fall slightly (Nov 16 59 -> 53 within 3% DTE
0-7; Nov 13 38 -> 35). It does not reach DTE 1-7 put passes much (0 -> 7) because the put lanes' best-ranked contracts
are not the high-ratio ones (formerly-OTM high-OI puts on a down day, research 2.2).

**Thin added contracts** (record flags; Nov 16 / Nov 13): added puts 40 / 37: `thin_oi` 3 / 9, `thin_prior_volume` 7 /
12, either 7 / 14 (32 / 24 of the added puts carried session volume and were not judged on volume). Added calls 16 /
12: either 13 / 10. For scale, OFF's own set: calls 42 of 131 / 49 of 129 and puts 44 of 85 / 39 of 87 thin by the
same flags. A filter on balanced puts only (OI >= 100 and judged prior volume >= 500) would drop 7 of 40 (Nov 16) and
14 of 37 (Nov 13) added puts at 9 names, about 0.8-1.6 per name, i.e. roughly 12-25 at 16 names (estimate; the 7 new
names are not replayed). The OI rule alone drops 3 / 9.

**Operator steps** (RECORD only; the lifecycle, not the window):
1. `python -I runner/compose_env.py realtime-v3-options-lifecycle REALTIME_V3_OPTIONS_SLOT_SIDE_BALANCE RECORD`, then the
   lifecycle hotfix on the batch image. The service reads the switch at start; the allocation runs in the next
   CAPACITY_SHARDS stage. Before ~14:30 UTC (Nov 17's CATALOG_QUOTE waits for the post-open capture, then
   CAPACITY_SHARDS ~14:35) it applies to Nov 17; after that stage has completed it applies to Nov 18.
2. Check right after the restart: `docker logs --since 5m stock-screener-realtime-v3-options-lifecycle 2>&1 | grep
   OPTIONS_SLOT_SIDE_BALANCE_SETTING` = `"mode":"RECORD"`; after the stage: `... | grep -c
   OPTIONS_SLOT_SIDE_BALANCE_RECORD` = 1 + the number of names (17 at 16 names), `applied` false, no
   `OPTIONS_SLOT_SIDE_BALANCE_RECORD_FAILED`; the session's slot set / contract count / CAPACITY_SHARDS evidence as with
   OFF (RECORD stores nothing new).
3. Undo: `--remove` the key + lifecycle hotfix (absent = OFF). ON: not before 1-2 RECORD sessions and a liquidity rule
   decision.

**Not verified.** The 16-name RECORD lines (only in the live stage); ON end-to-end through SESSION_ACTIVATION, the slot
set and the market-state / provider subscription (the allocation document's schema is unchanged and its contract count
the same, but no live or DB-backed run); O1 / OS outcomes on the balanced set (passes counted at the close only, not
alerts); whether the lifecycle restart near the open interrupts a stage lease (the stages were idle / RETRY at 08:5x).

## Nov 17 (Tue) - live: AL4/AL6/AL7 ON, AL8 RECORD, AL9 full-chain scan, AL10 design + value report (times UTC)
- **AL4 floor** ON 15:04 after 4 clean RECORD windows; **AL6 0DTE gate + AL7 aggressor side** ON 15:30 (user's yes after the
  research recap: contract type as direction points against the day ~70%). Day: O1 1 alert contract (NFLX 11/27 60C, 19:38,
  repeated), ~138 OBSERVATION (gates), 6,657 NOT_SELECTED (ratio < 3 on a high-OI watched set: median prior OI ~4,200); OS1 1
  alert (SPY 11/17 738P, 15:52).
- **AL8 RECORD:** stored allocation 213 calls / 171 puts; balanced would be 192/192; 17 record lines, no failures.
- **AL9 full-chain scan (RECORD side process, read-only, 17:49-21:06):** 21,000+ contracts/window, ~1,050-1,250 tradable
  (~100 watched), 10-92 burst candidates/window (0-8 watched). Watched set = 9.3% of tradable 5-min premium (14.8% volume).
- **AL10 design (code map):** the watched set is sealed per session (pointer move fails every window; out-of-set contracts are
  quarantined); target = declared superset + in-place subscribe/unsubscribe; phase 1 (policy + Options scheduler RECORD) built.
- **VALUE REPORT** (`recovery-20261104/evidence/chain-scan-20261117/VALUE_REPORT.md`): pre-open top-24/name by prior OI near the
  money = 32.6% of tradable premium (3.5x watched), one midday re-selection 43%, best static 62.5%, perfect rotation 82% (needs
  ~118 swaps/window); bursts last ~1 window (90%); no outcome edge on one afternoon -> AL10 phases 2-3 no-go/defer; next: better
  selection rule + snapshot alert lane, RECORD-first.
- **H1-lite:** close chain export `recovery-20261104/evidence/chain-daily/2026-11-17.jsonl.gz` (21,232 rows, 16 names).
