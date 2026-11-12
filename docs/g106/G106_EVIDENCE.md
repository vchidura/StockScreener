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
