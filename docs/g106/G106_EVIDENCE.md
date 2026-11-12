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
