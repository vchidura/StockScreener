"""G106 0DTE admission (Nov 18-19 2026): the profile's spread / strike-distance limits for same-day (DTE 0) contracts in the
Options watched-set resolution, RECORD first.

Gap (Nov 18, `evidence/stock-sameday-20261118/REPORT.md` section 1 side finding): in the resolution's admission chain
(`_shortlist_activity_momentum_candidates`) the DTE-0 branch ends at its same-day last-trading checks, and since the chain
is one if/elif sequence the criterion's role limits after it (spread, spot for the strike filter, strike distance,
completed-session volume, prior OI) never run for a DTE-0 contract: Nov 18 AAPL 11/18 357.5P was admitted at a 23%
spread with |delta| 0.75. Index ETF same-day picks (SPY / QQQ / IWM, the DAILY_EXPIRY SAME_DAY bucket) and single-stock
same-day picks (Fridays, and the D5 rule `REALTIME_V3_OPTIONS_STOCK_SAME_DAY`, which re-checks the limits itself) share it.
With the v4 profile the DTE-0 criterion is SHORT_ACTIVITY: spread <= 20%, strike within 7.5% of spot (volume 0 / no OI
floor, so in practice spread and strike distance).

`REALTIME_V3_OPTIONS_SAME_DAY_ADMISSION`, read by the Options lifecycle service (`realtime-v3-options-lifecycle`,
`scripts/run_advanced_options_v3_lifecycle.py`) at start; it acts in the CAPACITY_SHARDS stage
(`PostgresAutomaticOptionsPreparationExecutor._allocate_capture`, once per session, after the open capture) around the
whole allocation: resolution -> AL8 / D5 stock same-day -> AL11 (each with its own switch, unchanged):
| OFF (default, unset, blank) | today's resolution and allocation byte for byte: no second resolution, no log line |
| RECORD | the allocation used and everything stored are exactly OFF's (with its AL8 / D5 / AL11 lines); the limited
           resolution and its allocation (same switches, the previous-session read shared, their lines not printed) are
           computed alongside and only logged: `OPTIONS_SAME_DAY_ADMISSION_RECORD {json}` lines (one summary, then one per
           underlying with a refusal or a change: the 0DTE contracts the limits refuse - watched ones with spread,
           strike distance, |delta| - and the contracts the allocation would add and drop); never raises (a failure logs
           `OPTIONS_SAME_DAY_ADMISSION_RECORD_FAILED`) |
| ON | the limited resolution's allocation is used and stored (its AL8 / D5 / AL11 lines printed, the same RECORD lines
       with `applied` true). Any failure (the limited resolution or allocation, a per-name contract count that differs
       from today's) logs `OPTIONS_SAME_DAY_ADMISSION_RECORD_FAILED` and keeps today's allocation (and prints today's
       AL8 / D5 / AL11 lines): the stage never fails because of this switch |
Any other value (any case accepted for the three) and the lifecycle service refuses to start; a value other than OFF
with a non-v4 admission profile also refuses.

Interplay: AL11 (`REALTIME_V3_OPTIONS_SLOT_SELECTION`) keeps each name's 0DTE count from "today's" allocation and picks
0DTE contracts only from the resolution's admitted ones: under ON both come from the limited resolution (its restricted
re-resolution gets the same flag), so AL11 never brings a refused contract back. D5 picks from the same admitted groups
and re-checks the same limits (no change for its picks). AL8 balances sides over whatever the resolution admitted.

Replay of the real CAPACITY_SHARDS captures (Nov 19 night, `_allocate_capture`, AL8 RECORD as live): OFF / RECORD hash =
live on Nov 13, 16, 17, 18; ON replaced 4 watched 0DTE contracts on Fri Nov 13 (AMZN 257.5C, NFLX 58C / 59C / 61C;
spreads 22-52%), none on Nov 16-18 (1 on Nov 16 with AL11 ON: MSFT 520P); per-name counts unchanged every day.
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Callable, Iterable, Mapping

from .advanced_options_activity_momentum_c6 import (
    V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    ActivityMomentumAllocation,
    ActivityMomentumCandidate,
    same_day_admission_limit_reason,
    v4_product_kind,
)
from .contracts import canonical_sha256

SAME_DAY_ADMISSION_SETTING = "REALTIME_V3_OPTIONS_SAME_DAY_ADMISSION"
SAME_DAY_ADMISSION_RECORD_EVENT = "OPTIONS_SAME_DAY_ADMISSION_RECORD"
SAME_DAY_ADMISSION_RECORD_FAILED_EVENT = "OPTIONS_SAME_DAY_ADMISSION_RECORD_FAILED"
SAME_DAY_ADMISSION_SETTING_EVENT = "OPTIONS_SAME_DAY_ADMISSION_SETTING"
SAME_DAY_ADMISSION_POLICY = MappingProxyType({
    "version": "options-same-day-admission-v1",
    "dte": 0,
    "criterion": "PROFILE_DTE0_CRITERION",
    "checks": ("ROLE_SPREAD_LIMIT", "SPOT_UNAVAILABLE_FOR_STRIKE_FILTER", "ROLE_STRIKE_DISTANCE_LIMIT",
               "ROLE_VOLUME_LIMIT", "ROLE_OPEN_INTEREST_LIMIT"),
    "after": "SAME_DAY_LAST_TRADING_CHECKS",
    "products": ("DAILY_EXPIRY", "WEEKLY_EXPIRY"),
    "fallback": "TODAYS_ALLOCATION_ON_ANY_FAILURE_OR_PER_NAME_COUNT_CHANGE",
})
SAME_DAY_ADMISSION_POLICY_SHA256 = canonical_sha256(dict(SAME_DAY_ADMISSION_POLICY))
# Docker splits a log line over 16 KiB: one line per underlying, short contract lists.
_CONTRACT_LIST_LIMIT = 12

Emit = Callable[[str, Mapping[str, object]], None]


class SameDayAdmissionMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"
    ON = "ON"


def same_day_admission_mode(environ: Mapping[str, str] | None = None) -> SameDayAdmissionMode:
    """`REALTIME_V3_OPTIONS_SAME_DAY_ADMISSION`; unset or blank is OFF. Anything but OFF, RECORD or ON raises."""
    raw = (os.environ if environ is None else environ).get(SAME_DAY_ADMISSION_SETTING)
    return parse_same_day_admission_mode(raw)


def parse_same_day_admission_mode(raw: object) -> SameDayAdmissionMode:
    if isinstance(raw, SameDayAdmissionMode):
        return raw
    if raw is not None and not isinstance(raw, str):
        raise ValueError(f"{SAME_DAY_ADMISSION_SETTING} must be OFF, RECORD or ON, not {raw!r}")
    value = (raw or "").strip().upper() or SameDayAdmissionMode.OFF.value
    try:
        return SameDayAdmissionMode(value)
    except ValueError:
        raise ValueError(f"{SAME_DAY_ADMISSION_SETTING} must be OFF, RECORD or ON, not {raw!r}") from None


def _print_line(event: str, document: Mapping[str, object]) -> None:
    print(f"{event} {json.dumps(document, sort_keys=True, default=str, separators=(',', ':'))}",
          file=sys.stderr, flush=True)


def _selected(allocation: ActivityMomentumAllocation) -> dict[str, ActivityMomentumCandidate]:
    tickers = set(allocation.selected_contract_tickers)
    return {
        row.contract_ticker: row
        for group in allocation.selected_groups
        for row in group.candidates
        if row.contract_ticker in tickers
    }


def per_underlying_counts(allocation: ActivityMomentumAllocation) -> dict[str, int]:
    return dict(sorted(Counter(row.underlying for row in _selected(allocation).values()).items()))


def _quantized(value) -> str | None:
    return None if value is None else str(Decimal(value).quantize(Decimal("0.001")))


def _contract(row: ActivityMomentumCandidate, *, session_date, reason: str | None = None) -> dict[str, object]:
    moneyness = (
        None if row.spot is None or not row.spot
        else abs(row.strike - row.spot) / row.spot
    )
    document = {
        "contract": row.contract_ticker,
        "side": row.option_side.value,
        "dte": row.dte_for(session_date),
        "spread": _quantized(row.relative_spread),
        "strike_distance": _quantized(moneyness),
        "abs_delta": None if row.delta is None else _quantized(abs(row.delta)),
    }
    if reason is not None:
        document["reason"] = reason
    return document


def _rejections(resolution) -> Counter:
    return Counter(dict(getattr(resolution, "rejection_counts", ()) or ()))


def same_day_admission_documents(
    *,
    mode: SameDayAdmissionMode,
    profile,
    session_date,
    off_resolution,
    rule_resolution,
    off: ActivityMomentumAllocation,
    rule: ActivityMomentumAllocation,
) -> list[dict[str, object]]:
    """The summary, then one document per underlying with a refusal or a change: the DTE-0 contracts of today's
    admitted pool (the resolution's shortlist) that the limits refuse (count and reasons; the watched ones listed),
    same-day counts in today's and the rule's set, and the contracts the rule's allocation would add and drop."""
    off_rows = _selected(off)
    rule_rows = _selected(rule)
    evidence = {row.underlying: row for row in getattr(off_resolution, "short_expiry_evidence", ()) or ()}
    refused: dict[str, list[tuple[ActivityMomentumCandidate, str]]] = {}
    admitted_same_day: Counter = Counter()
    for row in getattr(off_resolution, "shortlisted_candidates", ()) or ():
        if row.dte_for(session_date) != 0:
            continue
        admitted_same_day[row.underlying] += 1
        reason = same_day_admission_limit_reason(row, profile, session_date)
        if reason is not None:
            refused.setdefault(row.underlying, []).append((row, reason))
    rejection_delta = _rejections(rule_resolution)
    rejection_delta.subtract(_rejections(off_resolution))
    common = {
        "mode": mode.value,
        "applied": mode is SameDayAdmissionMode.ON,
        "session_date": str(session_date),
        "policy_version": SAME_DAY_ADMISSION_POLICY["version"],
        "off_allocation_sha256": off.allocation_sha256,
        "rule_allocation_sha256": rule.allocation_sha256,
    }
    lines = []
    totals: Counter = Counter()
    refused_reasons: Counter = Counter()
    underlyings = sorted({row.underlying for row in (*off_rows.values(), *rule_rows.values())} | set(refused))
    for underlying in underlyings:
        mine_off = {ticker: row for ticker, row in off_rows.items() if row.underlying == underlying}
        mine_rule = {ticker: row for ticker, row in rule_rows.items() if row.underlying == underlying}
        added = sorted((mine_rule[t] for t in set(mine_rule) - set(mine_off)),
                       key=lambda row: (row.expiration, row.option_side.value, row.strike))
        dropped = sorted((mine_off[t] for t in set(mine_off) - set(mine_rule)),
                         key=lambda row: (row.expiration, row.option_side.value, row.strike))
        mine_refused = sorted(refused.get(underlying, ()), key=lambda item: item[0].contract_ticker)
        watched_refused = [(row, reason) for row, reason in mine_refused if row.contract_ticker in mine_off]
        off_same_day = sum(1 for row in mine_off.values() if row.dte_for(session_date) == 0)
        rule_same_day = sum(1 for row in mine_rule.values() if row.dte_for(session_date) == 0)
        reasons = Counter(reason for _row, reason in mine_refused)
        refused_reasons.update(reasons)
        totals["refused_admitted"] += len(mine_refused)
        totals["refused_watched"] += len(watched_refused)
        totals["would_add"] += len(added)
        totals["would_drop"] += len(dropped)
        totals["off_same_day"] += off_same_day
        totals["rule_same_day"] += rule_same_day
        if not mine_refused and not added and not dropped:
            continue
        lines.append({
            **common,
            "part": "underlying",
            "underlying": underlying,
            "product": v4_product_kind(evidence.get(underlying), session_date),
            "admitted_same_day": admitted_same_day.get(underlying, 0),
            "refused_admitted": len(mine_refused),
            "refused_reasons": dict(sorted(reasons.items())),
            "refused_watched_count": len(watched_refused),
            "refused_watched": [_contract(row, session_date=session_date, reason=reason)
                                for row, reason in watched_refused[:_CONTRACT_LIST_LIMIT]],
            "off_count": len(mine_off),
            "rule_count": len(mine_rule),
            "off_same_day": off_same_day,
            "rule_same_day": rule_same_day,
            "would_add_count": len(added),
            "would_drop_count": len(dropped),
            "would_add": [_contract(row, session_date=session_date) for row in added[:_CONTRACT_LIST_LIMIT]],
            "would_drop": [_contract(row, session_date=session_date) for row in dropped[:_CONTRACT_LIST_LIMIT]],
        })
    off_counts = per_underlying_counts(off)
    rule_counts = per_underlying_counts(rule)
    summary = {
        **common,
        "part": "summary",
        "lines": 1 + len(lines),
        "underlyings": len(underlyings),
        "changed_names": sorted(line["underlying"] for line in lines
                                if line["would_add_count"] or line["would_drop_count"]),
        "policy_sha256": SAME_DAY_ADMISSION_POLICY_SHA256,
        "admitted_same_day": sum(admitted_same_day.values()),
        "refused_admitted": totals["refused_admitted"],
        "refused_reasons": dict(sorted(refused_reasons.items())),
        "refused_watched": totals["refused_watched"],
        "rejection_delta": {key: value for key, value in sorted(rejection_delta.items()) if value},
        "same_day": {"off": totals["off_same_day"], "rule": totals["rule_same_day"]},
        "would_add": totals["would_add"],
        "would_drop": totals["would_drop"],
        "contracts": {"off": len(off_rows), "rule": len(rule_rows)},
        "per_name_counts_equal": off_counts == rule_counts,
        "policy": dict(SAME_DAY_ADMISSION_POLICY),
    }
    return [summary, *lines]


def _emit_failed(emit: Emit, mode: SameDayAdmissionMode, session_date, stage: str, error: Exception,
                 **extra) -> None:
    try:
        emit(SAME_DAY_ADMISSION_RECORD_FAILED_EVENT, {
            "mode": mode.value,
            "session_date": str(session_date),
            "stage": stage,
            **extra,
            "error": f"{type(error).__name__}: {error}"[:500],
        })
    except Exception:  # noqa: BLE001 - recording never fails the stage
        pass


def _replay(emit: Emit, lines: Iterable[tuple[str, Mapping[str, object]]]) -> None:
    for event, document in lines:
        try:
            emit(event, document)
        except Exception:  # noqa: BLE001
            pass


def allocate_with_same_day_admission(
    *,
    mode: SameDayAdmissionMode | str,
    profile,
    session_date,
    run: Callable[[bool, Emit | None], tuple[object, ActivityMomentumAllocation]],
    emit: Emit = _print_line,
) -> ActivityMomentumAllocation:
    """The lifecycle's allocation under the switch. `run(limits, sink)` resolves (with the DTE-0 limits or not) and
    allocates through AL8 / D5 / AL11, sending their lines to `sink` (None = their own print), and returns
    (resolution, allocation). OFF: `run(False, None)` alone. RECORD: that, then the limited run with its lines dropped,
    logged only. ON: both runs buffered; the limited one is used when it succeeds and keeps every name's count (its
    buffered lines printed), else today's (its lines printed, plus a FAILED line)."""
    mode = parse_same_day_admission_mode(mode)
    if mode is SameDayAdmissionMode.OFF:
        return run(False, None)[1]
    if profile.version != V4_ACTIVITY_MOMENTUM_PROFILE_VERSION:
        raise ValueError(f"{SAME_DAY_ADMISSION_SETTING} requires the v4 admission profile")
    if mode is SameDayAdmissionMode.RECORD:
        off_resolution, used = run(False, None)
        try:
            rule_resolution, rule = run(True, lambda _event, _document: None)
            for document in same_day_admission_documents(
                    mode=mode, profile=profile, session_date=session_date, off_resolution=off_resolution,
                    rule_resolution=rule_resolution, off=used, rule=rule):
                emit(SAME_DAY_ADMISSION_RECORD_EVENT, document)
        except Exception as error:  # noqa: BLE001 - RECORD never fails the stage
            _emit_failed(emit, mode, session_date, "RECORD", error)
        return used
    off_lines: list = []
    rule_lines: list = []
    try:
        off_resolution, off = run(False, lambda event, document: off_lines.append((event, document)))
    except Exception:
        _replay(emit, off_lines)  # today's lines as today prints them before the same failure
        raise
    try:
        rule_resolution, rule = run(True, lambda event, document: rule_lines.append((event, document)))
        off_counts, rule_counts = per_underlying_counts(off), per_underlying_counts(rule)
        if off_counts != rule_counts:
            raise RuntimeError(f"per-name contract counts changed: today {off_counts} rule {rule_counts}")
    except Exception as error:  # noqa: BLE001 - ON keeps today's allocation on any failure
        _replay(emit, off_lines)
        _emit_failed(emit, mode, session_date, "ALLOCATION", error, kept="TODAY")
        return off
    _replay(emit, rule_lines)
    try:
        for document in same_day_admission_documents(
                mode=mode, profile=profile, session_date=session_date, off_resolution=off_resolution,
                rule_resolution=rule_resolution, off=off, rule=rule):
            emit(SAME_DAY_ADMISSION_RECORD_EVENT, document)
    except Exception as error:  # noqa: BLE001
        _emit_failed(emit, mode, session_date, "RECORD", error)
    return rule


def log_same_day_admission_setting(mode: SameDayAdmissionMode) -> None:
    """One start line on the lifecycle service (the operator's check right after the deploy)."""
    try:
        _print_line(SAME_DAY_ADMISSION_SETTING_EVENT, {
            "mode": mode.value,
            "setting": SAME_DAY_ADMISSION_SETTING,
            "policy_version": SAME_DAY_ADMISSION_POLICY["version"],
        })
    except Exception:  # noqa: BLE001
        pass
