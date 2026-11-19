"""G106 D5 follow-up (Nov 18 2026): single-stock same-day (0DTE) contracts in the Options watched set on every day the
stock lists an expiry, RECORD first.

Cause (Nov 18, replay of the real CAPACITY_SHARDS captures of Nov 16 and Nov 18 through `_allocate_capture`): the V4
allocation treats a single stock as a WEEKLY_EXPIRY product. Its buckets (WEEKLY 8 on the weekday ladder, CORE 12, EXTENSION
4) add up to the whole 24-contract share, and the stock's own same-day expiry is in the ladder only on a Friday (today's
expiry shares the WEEKLY bucket with next week's: about 4 contracts). On Mon / Wed single-stock 0DTE got a slot only
when a bucket was short of supply: on a Monday this week's Friday is DTE 4 (a flow-excluded DTE, ranked last in the
shortlist, 2 admitted), so the WEEKLY bucket took 2 and the leftover rounds took 2-3 0DTE; on a Wednesday Friday is DTE 2
(the short target), the WEEKLY bucket fills, the share is used and no 0DTE is watched (Nov 18: 9 single stocks listed a
Nov 18 expiry, 6-10 admitted 0DTE contracts each, none selected). The D5 F0DTE lane only sees watched 0DTE contracts.

`REALTIME_V3_OPTIONS_STOCK_SAME_DAY`, read by the Options lifecycle service (`realtime-v3-options-lifecycle`,
`scripts/run_advanced_options_v3_lifecycle.py`) at start; the allocation runs in its CAPACITY_SHARDS stage
(`PostgresAutomaticOptionsPreparationExecutor._allocate_capture`, once per session, after the open capture), BEFORE AL11
(whatever this switch decided is "today's set" for `REALTIME_V3_OPTIONS_SLOT_SELECTION`, which keeps today's 0DTE count):
| OFF (default, unset, blank) | today's allocation byte for byte: no second allocation, no log line |
| RECORD | the allocation used and everything stored are exactly OFF's; the rule's allocation (same AL8 side-balance flag) is
           computed alongside and only logged: `OPTIONS_STOCK_SAME_DAY_RECORD {json}` lines (one summary, then one per
           underlying: would_add, would_drop, the chosen same-day contracts with |delta| / prior OI); never raises (a
           failure logs `OPTIONS_STOCK_SAME_DAY_RECORD_FAILED`) |
| ON | the rule's allocation is used and stored, the same lines with `applied` true. Any failure (the rule's allocation,
       a per-name contract count that differs from today's) logs `OPTIONS_STOCK_SAME_DAY_RECORD_FAILED` and keeps
       today's allocation: the stage never fails because of this switch |
Any other value (any case accepted for the three) and the lifecycle service refuses to start; a value other than OFF
with a non-v4 admission profile also refuses.

Rule (`allocate_activity_momentum(stock_same_day=True)`, `v4_stock_same_day_plan`): for a WEEKLY_EXPIRY product whose
listed expiries include the session date and whose weekly ladder does not (Mon / Wed; a Friday's ladder already holds
today's expiry, so nothing is added twice; Tue / Thu list none), up to K = 4 same-day contracts (the Friday count:
2 calls + 2 puts, a side short of supply topped up from the other) are taken first, inside the name's own share (24),
from the resolution's admitted single-contract same-day groups that pass the profile's DTE-0 criterion (two-sided dated
quote, spread, strike distance, quote age), by |delta| in the F0DTE band 0.20-0.60 first, nearest 0.40, then prior OI,
then the ticker. They count in the name's WEEKLY bucket (Friday's split: 4 same-day + 4 of this week's weekly), so
CORE / EXTENSION, the total (384 at 16 names), the budget and the shard limits are unchanged; the 4 lowest-ranked
weekly contracts make room. Index ETFs (DAILY_EXPIRY) keep their SAME_DAY bucket and are untouched.
The DTE-0 criterion is re-checked here on purpose: the resolution's admission ends at its same-day last-trading checks for
a DTE-0 contract and does not apply the spread / strike-distance limits to it (Nov 18: AAPL 11/18 357.5P, spread 23%,
|delta| 0.75, was admitted), so `v4_stock_same_day_eligible` applies them.
AL11 (`REALTIME_V3_OPTIONS_SLOT_SELECTION`, after this switch) keeps each name's 0DTE count (4 with this switch ON) but
picks its own 0DTE contracts from the admitted ones by the previous session's volume (Nov 18 replay: 16 of the 50
watched 0DTE contracts differ from this rule's picks, count per name unchanged).
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Callable, Mapping

from .advanced_options_activity_momentum_c6 import (
    V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    V4_STOCK_SAME_DAY_CONTRACTS,
    V4_STOCK_SAME_DAY_MAXIMUM_ABS_DELTA,
    V4_STOCK_SAME_DAY_MINIMUM_ABS_DELTA,
    V4_STOCK_SAME_DAY_PER_SIDE,
    V4_STOCK_SAME_DAY_TARGET_ABS_DELTA,
    ActivityMomentumAllocation,
    _v4_bucket_context,
    v4_contract_bucket,
    v4_stock_same_day_eligible,
    v4_stock_same_day_plan,
)
from .advanced_options_selection_c6 import OptionSide
from .contracts import canonical_sha256

STOCK_SAME_DAY_SETTING = "REALTIME_V3_OPTIONS_STOCK_SAME_DAY"
STOCK_SAME_DAY_RECORD_EVENT = "OPTIONS_STOCK_SAME_DAY_RECORD"
STOCK_SAME_DAY_RECORD_FAILED_EVENT = "OPTIONS_STOCK_SAME_DAY_RECORD_FAILED"
STOCK_SAME_DAY_SETTING_EVENT = "OPTIONS_STOCK_SAME_DAY_SETTING"
STOCK_SAME_DAY_POLICY = MappingProxyType({
    "version": "options-stock-same-day-v1",
    "products": ("WEEKLY_EXPIRY",),
    "when": "SESSION_DATE_LISTED_AND_NOT_IN_WEEKLY_LADDER",
    "same_day_contracts": V4_STOCK_SAME_DAY_CONTRACTS,
    "per_side": V4_STOCK_SAME_DAY_PER_SIDE,
    "short_side": "TOP_UP_FROM_OTHER_SIDE",
    "minimum_abs_delta": str(V4_STOCK_SAME_DAY_MINIMUM_ABS_DELTA),
    "maximum_abs_delta": str(V4_STOCK_SAME_DAY_MAXIMUM_ABS_DELTA),
    "target_abs_delta": str(V4_STOCK_SAME_DAY_TARGET_ABS_DELTA),
    "rank": "IN_BAND_THEN_NEAREST_TARGET_DELTA_THEN_PRIOR_OI_THEN_TICKER",
    "admission": "PROFILE_DTE0_CRITERION_TWO_SIDED_QUOTE_SPREAD_MONEYNESS_QUOTE_AGE",
    "bucket": "COUNTS_IN_WEEKLY_LADDER_BUCKET",
    "share": "UNCHANGED",
})
STOCK_SAME_DAY_POLICY_SHA256 = canonical_sha256(dict(STOCK_SAME_DAY_POLICY))
# Docker splits a log line over 16 KiB: one line per underlying, short contract lists.
_CONTRACT_LIST_LIMIT = 12


class StockSameDayMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"
    ON = "ON"


def stock_same_day_mode(environ: Mapping[str, str] | None = None) -> StockSameDayMode:
    """`REALTIME_V3_OPTIONS_STOCK_SAME_DAY`; unset or blank is OFF. Anything but OFF, RECORD or ON raises."""
    raw = (os.environ if environ is None else environ).get(STOCK_SAME_DAY_SETTING)
    return parse_stock_same_day_mode(raw)


def parse_stock_same_day_mode(raw: object) -> StockSameDayMode:
    if isinstance(raw, StockSameDayMode):
        return raw
    if raw is not None and not isinstance(raw, str):
        raise ValueError(f"{STOCK_SAME_DAY_SETTING} must be OFF, RECORD or ON, not {raw!r}")
    value = (raw or "").strip().upper() or StockSameDayMode.OFF.value
    try:
        return StockSameDayMode(value)
    except ValueError:
        raise ValueError(f"{STOCK_SAME_DAY_SETTING} must be OFF, RECORD or ON, not {raw!r}") from None


def _print_line(event: str, document: Mapping[str, object]) -> None:
    print(f"{event} {json.dumps(document, sort_keys=True, default=str, separators=(',', ':'))}",
          file=sys.stderr, flush=True)


def _selected(allocation: ActivityMomentumAllocation) -> dict[str, object]:
    tickers = set(allocation.selected_contract_tickers)
    return {
        row.contract_ticker: row
        for group in allocation.selected_groups
        for row in group.candidates
        if row.contract_ticker in tickers
    }


def per_underlying_counts(allocation: ActivityMomentumAllocation) -> dict[str, int]:
    return dict(sorted(Counter(row.underlying for row in _selected(allocation).values()).items()))


def _contract(row, *, session_date, bucket=None) -> dict[str, object]:
    document = {
        "contract": row.contract_ticker,
        "side": row.option_side.value,
        "dte": row.dte_for(session_date),
        "abs_delta": None if row.delta is None else str(abs(row.delta).quantize(Decimal("0.001"))),
        "prior_oi": row.prior_open_interest,
    }
    if bucket is not None:
        document["bucket"] = bucket
    return document


def stock_same_day_documents(
    *,
    mode: StockSameDayMode,
    profile,
    resolution,
    session_date,
    off: ActivityMomentumAllocation,
    rule: ActivityMomentumAllocation,
) -> list[dict[str, object]]:
    """The summary document, then one per underlying: the rule's reason, the eligible same-day supply by side, the chosen
    (planned) same-day contracts, same-day counts in today's and the rule's set, and the contracts the rule would add and
    drop (side, DTE, |delta|, prior OI, today's bucket for a drop)."""
    off_rows = _selected(off)
    rule_rows = _selected(rule)
    evidence = {row.underlying: row for row in getattr(resolution, "short_expiry_evidence", ()) or ()}
    groups_by_underlying: dict[str, list] = {}
    for group in getattr(resolution, "groups", ()) or ():
        groups_by_underlying.setdefault(group.underlying, []).append(group)
    underlyings = sorted({row.underlying for row in (*off_rows.values(), *rule_rows.values())})
    common = {
        "mode": mode.value,
        "applied": mode is StockSameDayMode.ON,
        "session_date": str(session_date),
        "policy_version": STOCK_SAME_DAY_POLICY["version"],
        "off_allocation_sha256": off.allocation_sha256,
        "rule_allocation_sha256": rule.allocation_sha256,
    }
    lines = []
    totals: Counter = Counter()
    reasons: Counter = Counter()
    shortfalls: dict[str, int] = {}
    for underlying in underlyings:
        groups = groups_by_underlying.get(underlying, ())
        expiry_evidence = evidence.get(underlying)
        reason, picks = v4_stock_same_day_plan(
            profile, groups=groups, session_date=session_date, expiry_evidence=expiry_evidence)
        product, ladder, ordinals, buckets = _v4_bucket_context(
            profile, groups=groups, session_date=session_date, expiry_evidence=expiry_evidence)
        eligible = {
            row.contract_ticker: row
            for group in groups
            if len(group.candidates) == 1
            for row in group.candidates
            if v4_stock_same_day_eligible(row, profile, session_date)
        }
        mine_off = {ticker: row for ticker, row in off_rows.items() if row.underlying == underlying}
        mine_rule = {ticker: row for ticker, row in rule_rows.items() if row.underlying == underlying}
        added = sorted((mine_rule[t] for t in set(mine_rule) - set(mine_off)),
                       key=lambda row: (row.expiration, row.option_side.value, row.strike))
        dropped = sorted((mine_off[t] for t in set(mine_off) - set(mine_rule)),
                         key=lambda row: (row.expiration, row.option_side.value, row.strike))
        off_same_day = sum(1 for row in mine_off.values() if row.dte_for(session_date) == 0)
        rule_same_day = sum(1 for row in mine_rule.values() if row.dte_for(session_date) == 0)
        reasons[reason] += 1
        if reason == "SHORTFALL":
            shortfalls[underlying] = len(picks)
        totals["would_add"] += len(added)
        totals["would_drop"] += len(dropped)
        totals["off_same_day"] += off_same_day
        totals["rule_same_day"] += rule_same_day
        totals["planned"] += len(picks)

        def bucket_of(row):
            return v4_contract_bucket(row, session_date=session_date, ladder=ladder, ordinals=ordinals,
                                      buckets=buckets)

        lines.append({
            **common,
            "part": "underlying",
            "underlying": underlying,
            "product": product,
            "reason": reason,
            "target": (V4_STOCK_SAME_DAY_CONTRACTS
                       if reason in ("APPLIED", "SHORTFALL", "NO_ELIGIBLE_SAME_DAY") else 0),
            "eligible_same_day": {
                "calls": sum(1 for row in eligible.values() if row.option_side is OptionSide.CALL),
                "puts": sum(1 for row in eligible.values() if row.option_side is OptionSide.PUT),
            },
            "chosen": [_contract(group.candidates[0], session_date=session_date) for group in picks],
            "off_count": len(mine_off),
            "rule_count": len(mine_rule),
            "off_same_day": off_same_day,
            "rule_same_day": rule_same_day,
            "would_add_count": len(added),
            "would_drop_count": len(dropped),
            "would_add": [_contract(row, session_date=session_date) for row in added[:_CONTRACT_LIST_LIMIT]],
            "would_drop": [_contract(row, session_date=session_date, bucket=bucket_of(row))
                           for row in dropped[:_CONTRACT_LIST_LIMIT]],
        })
    off_counts = per_underlying_counts(off)
    rule_counts = per_underlying_counts(rule)
    summary = {
        **common,
        "part": "summary",
        "lines": 1 + len(lines),
        "underlyings": len(underlyings),
        "policy_sha256": STOCK_SAME_DAY_POLICY_SHA256,
        "rule_names": sorted(line["underlying"] for line in lines if line["reason"] in ("APPLIED", "SHORTFALL")),
        "reasons": dict(sorted(reasons.items())),
        "shortfalls": shortfalls,
        "planned_same_day": totals["planned"],
        "same_day": {"off": totals["off_same_day"], "rule": totals["rule_same_day"]},
        "would_add": totals["would_add"],
        "would_drop": totals["would_drop"],
        "contracts": {"off": len(off_rows), "rule": len(rule_rows)},
        "per_name_counts_equal": off_counts == rule_counts,
        "policy": dict(STOCK_SAME_DAY_POLICY),
    }
    return [summary, *lines]


def _emit_failed(emit, mode: StockSameDayMode, session_date, stage: str, error: Exception, **extra) -> None:
    try:
        emit(STOCK_SAME_DAY_RECORD_FAILED_EVENT, {
            "mode": mode.value,
            "session_date": str(session_date),
            "stage": stage,
            **extra,
            "error": f"{type(error).__name__}: {error}"[:500],
        })
    except Exception:  # noqa: BLE001 - recording never fails the stage
        pass


def allocate_with_stock_same_day(
    *,
    mode: StockSameDayMode | str,
    profile,
    resolution,
    session_date,
    arguments: Mapping[str, object],
    allocate_used: Callable[[Mapping[str, object]], ActivityMomentumAllocation],
    allocate_plain: Callable[[Mapping[str, object]], ActivityMomentumAllocation],
    emit: Callable[[str, Mapping[str, object]], None] = _print_line,
) -> ActivityMomentumAllocation:
    """The lifecycle's allocation under the switch. `allocate_used(arguments)` is the AL8 helper (it logs the AL8 lines
    once); `allocate_plain(arguments)` is the same allocation without any line (the same AL8 side-balance flag).
    OFF: `allocate_used(arguments)`, nothing else. RECORD: the same allocation is returned; the rule's
    (`stock_same_day=True`) is computed and logged only. ON: the rule's allocation through `allocate_used`, after a
    check that every name keeps today's contract count; on any failure today's allocation (logged)."""
    mode = parse_stock_same_day_mode(mode)
    if mode is StockSameDayMode.OFF:
        return allocate_used(arguments)
    if profile.version != V4_ACTIVITY_MOMENTUM_PROFILE_VERSION:
        raise ValueError(f"{STOCK_SAME_DAY_SETTING} requires the v4 admission profile")
    rule_arguments = {**arguments, "stock_same_day": True}
    if mode is StockSameDayMode.RECORD:
        used = allocate_used(arguments)
        try:
            rule = allocate_plain(rule_arguments)
            for document in stock_same_day_documents(mode=mode, profile=profile, resolution=resolution,
                                                     session_date=session_date, off=used, rule=rule):
                emit(STOCK_SAME_DAY_RECORD_EVENT, document)
        except Exception as error:  # noqa: BLE001 - RECORD never fails the stage
            _emit_failed(emit, mode, session_date, "RECORD", error)
        return used
    try:
        off = allocate_plain(arguments)
        rule = allocate_plain(rule_arguments)
        off_counts, rule_counts = per_underlying_counts(off), per_underlying_counts(rule)
        if off_counts != rule_counts:
            raise RuntimeError(f"per-name contract counts changed: today {off_counts} rule {rule_counts}")
        used = allocate_used(rule_arguments)
    except Exception as error:  # noqa: BLE001 - ON keeps today's allocation on any failure
        _emit_failed(emit, mode, session_date, "ALLOCATION", error, kept="TODAY")
        return allocate_used(arguments)
    try:
        for document in stock_same_day_documents(mode=mode, profile=profile, resolution=resolution,
                                                 session_date=session_date, off=off, rule=used):
            emit(STOCK_SAME_DAY_RECORD_EVENT, document)
    except Exception as error:  # noqa: BLE001
        _emit_failed(emit, mode, session_date, "RECORD", error)
    return used


def log_stock_same_day_setting(mode: StockSameDayMode) -> None:
    """One start line on the lifecycle service (the operator's check right after the deploy)."""
    try:
        _print_line(STOCK_SAME_DAY_SETTING_EVENT, {
            "mode": mode.value,
            "setting": STOCK_SAME_DAY_SETTING,
            "policy_version": STOCK_SAME_DAY_POLICY["version"],
        })
    except Exception:  # noqa: BLE001
        pass
