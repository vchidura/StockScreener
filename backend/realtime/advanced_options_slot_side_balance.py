"""G106 AL8 (Nov 17 2026): a call/put-balanced V4 Options watched set, RECORD first.

Cause (docs/g106/G106_EVIDENCE.md, Nov 17 AL8): the v4 SHORT_DATED basket has `call_priority`, `_group_rank` puts the
side priority (call 0, put 1) ahead of every quality key, and `_v4_fill_product_buckets` and the later rounds fill
first-match with no side quota, so every 0-5 DTE call ranks before every 0-5 DTE put (Nov 16: 131 calls / 85 puts;
0-5 DTE 73 / 16; DTE 1-2 37 / 0). v2/v3's per-side bootstrap and missing-side logic do not run for v4.

`REALTIME_V3_OPTIONS_SLOT_SIDE_BALANCE`, read by the Options lifecycle service (`realtime-v3-options-lifecycle`,
`scripts/run_advanced_options_v3_lifecycle.py`) at start; the allocation runs in its CAPACITY_SHARDS stage
(`PostgresAutomaticOptionsPreparationExecutor._allocate_capture`, once per session, after the open capture):
| OFF (default, unset, blank) | today's allocation byte for byte: no second allocation, no log line |
| RECORD | the allocation used and everything stored (evidence, allocation document and hash, slot set) are exactly OFF's;
           the balanced allocation is computed alongside and only logged: `OPTIONS_SLOT_SIDE_BALANCE_RECORD {json}`
           lines (one summary, then one per underlying), never stored |
| ON | the balanced allocation (`allocate_activity_momentum(side_balance=True)`) is used and stored; the same lines,
       `applied` true. Implemented and tested; NOT to be switched on before RECORD evidence |
Any other value (any case accepted for the three) and the lifecycle service refuses to start; a value other than OFF
with a non-v4 admission profile also refuses. Recording never fails the stage (a failure logs
`OPTIONS_SLOT_SIDE_BALANCE_RECORD_FAILED`; in ON the stored allocation is still the balanced one, computed first).

Balanced allocation (c6 `side_balance=True`): calls and puts keep one quality ranking (side priority neutral in the
allocation sort only); each product bucket fills its (expiry, side) lanes round robin, each lane in quality order (so
the n-th call and the n-th put are about equally near the |delta| target), each side capped at half the bucket; the
basket rounds and the share / budget rounds take the next group of the side with fewer contracts. No new role: the
call-only `SHORT_DATED_SWEEP_LIKE` role only routes the P8 router's `short-dated-sweep-like-v2` consumer; balanced
puts keep PARTICIPATION_CONTEXT / DIRECTIONAL_MOMENTUM (same rank: the group's role priority is the minimum, i.e.
PARTICIPATION_CONTEXT for both sides), and the O1 / OS / flow lanes read every watched contract whatever its roles.
A put role would change the group identities (roles are in the group id) and the profile's role contract, so OFF
would no longer be byte-identical.

Thin picks (record only, not applied in ON; the AL4 O1 floor's values): a contract is `thin_oi` when its prior OI is
missing or under 100, and `thin_prior_volume` when its capture-day volume is the PRIOR session's (the provider's day
row was last updated before the session date) and under 500; `thin` = either. The capture runs after the open, so a
contract that already traded today carries today's first-minutes volume (`day_volume_session` SESSION): no volume
judgement is made for it (Nov 16: 1,242 of SPY's 4,102 captured snapshots had a day row from the session itself).
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter
from enum import Enum
from typing import Callable, Mapping

from .advanced_options_activity_momentum_c6 import (
    V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    ActivityMomentumAllocation,
    _v4_bucket_context,
    v4_contract_bucket,
)
from .advanced_options_selection_c6 import OptionSide

SLOT_SIDE_BALANCE_SETTING = "REALTIME_V3_OPTIONS_SLOT_SIDE_BALANCE"
SLOT_SIDE_BALANCE_RECORD_EVENT = "OPTIONS_SLOT_SIDE_BALANCE_RECORD"
SLOT_SIDE_BALANCE_RECORD_FAILED_EVENT = "OPTIONS_SLOT_SIDE_BALANCE_RECORD_FAILED"
SLOT_SIDE_BALANCE_SETTING_EVENT = "OPTIONS_SLOT_SIDE_BALANCE_SETTING"
THIN_PICK_POLICY = {
    "minimum_prior_open_interest": 100,
    "minimum_prior_session_volume": 500,
    "volume_judged_only_when_prior_session": True,
    "applied": False,
}
# Docker splits a log line over 16 KiB: one line per underlying (24 contracts each way at most).
_CONTRACT_LIST_LIMIT = 30


class SlotSideBalanceMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"
    ON = "ON"


def slot_side_balance_mode(environ: Mapping[str, str] | None = None) -> SlotSideBalanceMode:
    """`REALTIME_V3_OPTIONS_SLOT_SIDE_BALANCE`; unset or blank is OFF. Anything but OFF, RECORD or ON raises."""
    raw = (os.environ if environ is None else environ).get(SLOT_SIDE_BALANCE_SETTING)
    return parse_slot_side_balance_mode(raw)


def parse_slot_side_balance_mode(raw: object) -> SlotSideBalanceMode:
    if isinstance(raw, SlotSideBalanceMode):
        return raw
    if raw is not None and not isinstance(raw, str):
        raise ValueError(f"{SLOT_SIDE_BALANCE_SETTING} must be OFF, RECORD or ON, not {raw!r}")
    value = (raw or "").strip().upper() or SlotSideBalanceMode.OFF.value
    try:
        return SlotSideBalanceMode(value)
    except ValueError:
        raise ValueError(f"{SLOT_SIDE_BALANCE_SETTING} must be OFF, RECORD or ON, not {raw!r}") from None


def _print_line(event: str, document: Mapping[str, object]) -> None:
    print(f"{event} {json.dumps(document, sort_keys=True, default=str, separators=(',', ':'))}",
          file=sys.stderr, flush=True)


def _sides(rows) -> dict[str, int]:
    counts = Counter(row.option_side for row in rows)
    return {"calls": counts[OptionSide.CALL], "puts": counts[OptionSide.PUT]}


def _selected(allocation: ActivityMomentumAllocation) -> dict[str, object]:
    tickers = set(allocation.selected_contract_tickers)
    return {
        row.contract_ticker: row
        for group in allocation.selected_groups
        for row in group.candidates
        if row.contract_ticker in tickers
    }


def _volume_session(row, session_date) -> str:
    """PRIOR when the capture's day row predates the session (the prior session's volume), SESSION when it is the
    session's own early volume, UNKNOWN without a volume or its clock."""
    if row.completed_session_volume is None or row.volume_as_of is None:
        return "UNKNOWN"
    return "PRIOR" if row.volume_as_of.date() < session_date else "SESSION"


def _thin_flags(row, session_date) -> tuple[bool, bool]:
    thin_oi = (
        row.prior_open_interest is None
        or row.prior_open_interest < THIN_PICK_POLICY["minimum_prior_open_interest"]
    )
    thin_volume = (
        _volume_session(row, session_date) == "PRIOR"
        and row.completed_session_volume < THIN_PICK_POLICY["minimum_prior_session_volume"]
    )
    return thin_oi, thin_volume


def _contract(row, *, session_date, bucket) -> dict[str, object]:
    thin_oi, thin_volume = _thin_flags(row, session_date)
    return {
        "contract": row.contract_ticker,
        "side": row.option_side.value,
        "bucket": bucket,
        "dte": row.dte_for(session_date),
        "abs_delta": None if row.delta is None else str(abs(row.delta)),
        "prior_oi": row.prior_open_interest,
        "day_volume": row.completed_session_volume,
        "day_volume_session": _volume_session(row, session_date),
        "thin_oi": thin_oi,
        "thin_prior_volume": thin_volume,
        "thin": thin_oi or thin_volume,
    }


def slot_side_balance_documents(
    *,
    mode: SlotSideBalanceMode,
    profile,
    resolution,
    session_date,
    off: ActivityMomentumAllocation,
    balanced: ActivityMomentumAllocation,
) -> list[dict[str, object]]:
    """The summary document, then one per underlying: calls / puts in OFF vs balanced per bucket, the contracts the
    balanced set adds and removes (|delta|, DTE, prior OI, capture-day volume and its session, thin flags)."""
    off_rows = _selected(off)
    balanced_rows = _selected(balanced)
    evidence = {row.underlying: row for row in getattr(resolution, "short_expiry_evidence", ()) or ()}
    groups_by_underlying: dict[str, list] = {}
    for group in getattr(resolution, "groups", ()) or ():
        groups_by_underlying.setdefault(group.underlying, []).append(group)
    underlyings = sorted({row.underlying for row in (*off_rows.values(), *balanced_rows.values())})
    common = {
        "mode": mode.value,
        "applied": mode is SlotSideBalanceMode.ON,
        "session_date": str(session_date),
        "profile_version": profile.version,
        "off_allocation_sha256": off.allocation_sha256,
        "balanced_allocation_sha256": balanced.allocation_sha256,
    }
    per_underlying = []
    totals: dict[str, Counter] = {"off": Counter(), "balanced": Counter()}
    by_dte: dict[str, Counter] = {"off": Counter(), "balanced": Counter()}
    by_bucket: dict[str, Counter] = {"off": Counter(), "balanced": Counter()}
    added_total, removed_total, thin_added = Counter(), Counter(), Counter()
    thin_added_oi, thin_added_volume = Counter(), Counter()
    for underlying in underlyings:
        product, ladder, ordinals, buckets = _v4_bucket_context(
            profile,
            groups=groups_by_underlying.get(underlying, ()),
            session_date=session_date,
            expiry_evidence=evidence.get(underlying),
        )

        def bucket_of(row):
            return v4_contract_bucket(row, session_date=session_date, ladder=ladder, ordinals=ordinals,
                                      buckets=buckets)

        counts: dict[str, dict[str, list[int]]] = {}
        for label, rows in (("off", off_rows), ("balanced", balanced_rows)):
            for row in rows.values():
                if row.underlying != underlying:
                    continue
                bucket = bucket_of(row)
                side = 0 if row.option_side is OptionSide.CALL else 1
                counts.setdefault(bucket, {"off": [0, 0], "balanced": [0, 0]})[label][side] += 1
                totals[label][row.option_side] += 1
                dte = row.dte_for(session_date)
                by_dte[label][("0-5" if dte <= 5 else "6-21" if dte <= 21 else "22+", row.option_side)] += 1
                if 1 <= dte <= 2:
                    by_dte[label][("1-2", row.option_side)] += 1
                by_bucket[label][(f"{product}:{bucket}", row.option_side)] += 1
        added = sorted(
            (row for ticker, row in balanced_rows.items() if row.underlying == underlying and ticker not in off_rows),
            key=lambda row: (row.option_side.value, row.expiration, row.strike),
        )
        removed = sorted(
            (row for ticker, row in off_rows.items() if row.underlying == underlying and ticker not in balanced_rows),
            key=lambda row: (row.option_side.value, row.expiration, row.strike),
        )
        for row in added:
            added_total[row.option_side] += 1
            thin_oi, thin_volume = _thin_flags(row, session_date)
            thin_added[row.option_side] += thin_oi or thin_volume
            thin_added_oi[row.option_side] += thin_oi
            thin_added_volume[row.option_side] += thin_volume
        for row in removed:
            removed_total[row.option_side] += 1
        per_underlying.append({
            **common,
            "part": "underlying",
            "underlying": underlying,
            "product": product,
            "off": _sides(row for row in off_rows.values() if row.underlying == underlying),
            "balanced": _sides(row for row in balanced_rows.values() if row.underlying == underlying),
            "buckets": {
                bucket: {"off_calls_puts": value["off"], "balanced_calls_puts": value["balanced"]}
                for bucket, value in sorted(counts.items())
            },
            "added_count": len(added),
            "removed_count": len(removed),
            "added": [_contract(row, session_date=session_date, bucket=bucket_of(row))
                      for row in added[:_CONTRACT_LIST_LIMIT]],
            "removed": [_contract(row, session_date=session_date, bucket=bucket_of(row))
                        for row in removed[:_CONTRACT_LIST_LIMIT]],
        })

    def side_pair(counter, key):
        return [counter[(key, OptionSide.CALL)], counter[(key, OptionSide.PUT)]]

    summary = {
        **common,
        "part": "summary",
        "underlyings": len(underlyings),
        "lines": 1 + len(per_underlying),
        "contracts": {
            label: {"calls": totals[label][OptionSide.CALL], "puts": totals[label][OptionSide.PUT]}
            for label in ("off", "balanced")
        },
        "dte_calls_puts": {
            label: {key: side_pair(by_dte[label], key) for key in ("0-5", "1-2", "6-21", "22+")}
            for label in ("off", "balanced")
        },
        "bucket_calls_puts": {
            label: {
                key: side_pair(by_bucket[label], key)
                for key in sorted({key for key, _side in by_bucket["off"]} | {key for key, _side in by_bucket["balanced"]})
            }
            for label in ("off", "balanced")
        },
        "added": {"calls": added_total[OptionSide.CALL], "puts": added_total[OptionSide.PUT]},
        "removed": {"calls": removed_total[OptionSide.CALL], "puts": removed_total[OptionSide.PUT]},
        "thin_added": {"calls": thin_added[OptionSide.CALL], "puts": thin_added[OptionSide.PUT]},
        "thin_added_oi": {"calls": thin_added_oi[OptionSide.CALL], "puts": thin_added_oi[OptionSide.PUT]},
        "thin_added_prior_volume": {
            "calls": thin_added_volume[OptionSide.CALL],
            "puts": thin_added_volume[OptionSide.PUT],
        },
        "thin_policy": THIN_PICK_POLICY,
    }
    return [summary, *per_underlying]


def allocate_with_side_balance(
    allocate: Callable[..., ActivityMomentumAllocation],
    *,
    mode: SlotSideBalanceMode,
    profile,
    resolution,
    session_date,
    arguments: Mapping[str, object],
    emit: Callable[[str, Mapping[str, object]], None] = _print_line,
) -> ActivityMomentumAllocation:
    """The lifecycle's allocation under the switch. OFF: `allocate(profile, **arguments)`, nothing else. RECORD: the
    same allocation is returned (and stored by the caller); the balanced one is computed and logged only. ON: the
    balanced allocation is returned; OFF's is computed for the log. Recording never raises."""
    mode = parse_slot_side_balance_mode(mode)
    if mode is SlotSideBalanceMode.OFF:
        return allocate(profile, **arguments)
    if profile.version != V4_ACTIVITY_MOMENTUM_PROFILE_VERSION:
        raise ValueError(f"{SLOT_SIDE_BALANCE_SETTING} requires the v4 admission profile")
    if mode is SlotSideBalanceMode.ON:
        used = allocate(profile, **arguments, side_balance=True)
    else:
        used = allocate(profile, **arguments)
    try:
        other = (
            allocate(profile, **arguments, side_balance=True)
            if mode is SlotSideBalanceMode.RECORD
            else allocate(profile, **arguments)
        )
        off, balanced = (used, other) if mode is SlotSideBalanceMode.RECORD else (other, used)
        for document in slot_side_balance_documents(
            mode=mode,
            profile=profile,
            resolution=resolution,
            session_date=session_date,
            off=off,
            balanced=balanced,
        ):
            emit(SLOT_SIDE_BALANCE_RECORD_EVENT, document)
    except Exception as error:  # noqa: BLE001 - recording never fails the stage
        try:
            emit(SLOT_SIDE_BALANCE_RECORD_FAILED_EVENT, {
                "mode": mode.value,
                "session_date": str(session_date),
                "error": f"{type(error).__name__}: {error}"[:500],
            })
        except Exception:  # noqa: BLE001
            pass
    return used


def log_slot_side_balance_setting(mode: SlotSideBalanceMode) -> None:
    """One start line on the lifecycle service (the operator's check right after the hotfix)."""
    try:
        _print_line(SLOT_SIDE_BALANCE_SETTING_EVENT, {"mode": mode.value, "setting": SLOT_SIDE_BALANCE_SETTING})
    except Exception:  # noqa: BLE001
        pass
