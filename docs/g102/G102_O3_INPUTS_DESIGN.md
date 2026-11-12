# O3 inputs in the live (advanced) Options window (design)

Matrix row R44. Status: DESIGN, Nov 11 2026 (night). Nothing here is built. Plan: `G102_PLAN_20261111.md`.

## 1. Problem

O3 has never run live (BLOCKED in every window Nov 5-11). The lane, its loader (`advanced_options_native_loader._load_o3`),
adapter (`advanced_options_native_adapters.build_advanced_o3_executor`, `decode_advanced_o3_invocation_document`) and
projection (O3 credit observations) exist; nothing produces their input. The loader runs O3 only on CONFIRMED
`ADVANCED_OPTIONS_O3_INPUT` detector events at the window's cutoff, each with one `O3_NATIVE_INVOCATION` input whose
retained document (`advanced-options-o3-invocation-v1`) names a stored two-leg package and carries the candidate, the
direction decision, the leg snapshots and a structural invalidation level. All 317 live packages so far are one-leg O1.

## 2. What O3 judges (unchanged, `options/credit_detection.py`, policy `o3_options_first_credit_v2`)

- A credit vertical from the SPREAD_RANGE_LOCATOR family: bullish -> put credit vertical, bearish -> call credit vertical;
  DEFINED_RISK_CREDIT, net premium > 0, bounded payoff, one breakeven.
- 7-30 DTE at entry; each leg open interest >= 500 and day volume >= 50; the activity contract is one of the legs.
- A CONFIRMED options-first direction decision (detector O1, origin OPTIONS_FIRST).
- A structural invalidation price from the stock's technical levels.
- Output: research-only observation (no publication or execution permission), valued on model marks.

## 3. How the developer path produces it (to port)

`realtime/option_package_context.load_forward_technical_package_context`: for every SPREAD_RANGE_LOCATOR credit candidate in
the underlying's analysed matrix whose legs have dated activity sources -> pick the leg with the highest day volume / OI as
the activity -> `assess_options_credit_canonical_trend(activity, stock, direction)` (stock trend must confirm) ->
`bind_direct_structural_levels(levels, ...)` for the invalidation -> `assess_o3_credit(...)`.

## 4. What the live window already has

| Need | Live source |
|---|---|
| Two-leg candidates | Not built. The C6 allocation already reserves SPREAD_RANGE_LOCATOR pair groups (7-30 DTE; Nov 11: 34 of 72 contracts, 17 C / 17 P) |
| Leg marks, bid/ask | Rolling state quotes; `rt.option_mark_evidence` (ADVANCED_NBBO, one per contract per cycle) |
| Model greeks / valuation inputs | Reference state `valuation_inputs` (per underlying and expiry) |
| Day volume, OI per leg | Rolling `contract_volumes`; reference `open_interest` |
| Stock trend | Equity context (H daily bars and E 5m bars already loaded for OS1/OS2; the O3 requirement is 3 E bars) |
| Structural levels | Not loaded live for Options (developer path: technical levels from Equity history) |
| Two-leg package storage | `rt.option_packages` + `rt.option_package_legs` (multi-leg supported; no migration) |
| Input events | `rt.detector_versions`, `rt.detector_events`, `rt.detector_event_inputs` (the v2 DB test writes exactly this shape) |

## 5. Design

**Producer step `O3_INPUTS`, inside the Options window before the native input load** (same window, same cutoff), behind
`REALTIME_V3_O3_INPUTS` on the window service (off by default; off = today's behaviour, O3 BLOCKED as now):

1. For each underlying, build credit-vertical candidates from the watched contracts at 7-30 DTE: pairs of the same expiry
   and side from the C6 pair groups (short leg nearer the money, long wing further out; width within the profile's
   `maximum_center_distance_fraction`), priced from the cutoff's marks (quote midpoints at or before the cutoff, the same
   rule the window marks use) and the session valuation policy; candidate identity hashed like other packages.
2. Activity: the leg with the highest day volume / OI from the rolling state at the cutoff; its dated OI from the
   reference state. Legs below the OI / volume floor are dropped here (O3 would refuse them anyway).
3. Direction: `assess_options_credit_canonical_trend` on the live Equity context (port the stock snapshot builder the
   developer path uses; inputs are the E/H bars already loaded for OS lanes). Not CONFIRMED -> no event.
4. Invalidation: technical levels from the stored daily history (the same producer the developer path calls), bound with
   `bind_direct_structural_levels`; unavailable -> no event, reason recorded.
5. Persist in one transaction: the two-leg package (`rt.option_packages` + legs, stamped with the session valuation
   policy), the `ADVANCED_OPTIONS_O3_INPUT` detector version (once), one CONFIRMED event per candidate at `trigger_at =
   cutoff` with dedup on (cohort, candidate, cutoff) so a window retry writes nothing new, and its `O3_NATIVE_INVOCATION`
   input (retained document `advanced-options-o3-invocation-v1`, mark evidence id of the activity leg).
6. Marks gate in `_load_o3`: check marks only for the legs of the packages O3 judges, instead of all 72 contracts
   (`_has_unmarked_leg` already drops a package with an unmarked leg).
7. Bound: at most N candidates per underlying per window (start N = 2, best by credit / width), so the lane stays small.

**Failure rule:** the producer never fails the window; any error leaves O3 BLOCKED with a reason
(`O3_INPUTS_UNAVAILABLE:<code>`) and O1/OS1/OS2 run as before (INV-7, INV-9).

## 6. Tests and replay

- Unit: candidate pairing and pricing, activity choice, decision gating, idempotent writes, the gate change.
- Replay (copy on `g102-test-pg`, Nov 10 and Nov 11 windows): producer + lanes on stored inputs; O1/OS1/OS2 outputs
  byte-identical to stored; O3 COMPLETE_ZERO or COMPLETE_SELECTED with stored two-leg packages; window run time within budget.
- Compare with the developer path's O3 on the same contracts where both exist (same direction and credit within marks).

## 7. Order and effort

| Night | Work |
|---|---|
| 1 (Nov 12) | Steps 1-2 and 6-7 with unit tests: candidates, activity, marks gate |
| 2 (Nov 13) | Steps 3-5: stock snapshot and levels from live Equity, persistence, events; replay on Nov 10-12 |
| 3 | Rehearsal, package, deploy with the switch off; turn on in a session after two clean windows |

Not in r91 (Nov 12 open). Needs no migration and no universe change.
