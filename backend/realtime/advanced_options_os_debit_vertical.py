"""G106 D7 (Nov 18 night): the OS1 / OS2 debit-spread variant, RECORD only.

What it records: for every OS1 / OS2 invocation of a window (a stock-first signal paired with an Options contract: the
v1 pairing with an O1 event's contract, or with REALTIME_V3_OS_PARTICIPATION=ON the contract that confirmed the signal),
the directional debit vertical the O1 rule would build on that contract (`advanced_options_o1_debit_vertical.
select_debit_vertical`: long = the contract, short = same expiry and type further out of the money among the window's
watched contracts marked at the cutoff, nearest strike first; width 0.5%-10% of spot, reward / risk >= 1, leg open
interest / day volume floors, DTE <= 60, the O1 policy file and its environment thresholds), priced at the window's
cutoff marks. Alerts are unchanged: the OS packages stay what they are today (single contract, or the O1 vertical when
REALTIME_V3_O1_DEBIT_VERTICAL=ON gave the O1 event one), nothing here is hashed, stored or published.

`REALTIME_V3_OS_DEBIT_VERTICAL` on the Options window (`realtime-v3-options-p8-window`):
| OFF (default, unset, blank) | nothing: no read, no line (today, byte for byte) |
| RECORD | the native input load computes each OS invocation's vertical (reusing the O1 vertical inputs when that lane
           read them, else one guarded read of the same inputs behind a savepoint), and each OS executor logs one
           `OS_DEBIT_VERTICAL_RECORD {json}` line per window and lane: per invocation its evaluation status (SELECTED /
           OBSERVATION / NOT_SELECTED), today's package structure and the vertical or the reason none qualifies |
Anything else (ON included: not offered yet) and the window service refuses to start. Never fails the window: a read or
build failure becomes the reason on the line.
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Iterable, Mapping, Sequence
from uuid import UUID

OS_DEBIT_VERTICAL_SETTING = "REALTIME_V3_OS_DEBIT_VERTICAL"
OS_DEBIT_VERTICAL_RECORD_EVENT = "OS_DEBIT_VERTICAL_RECORD"
OS_DEBIT_VERTICAL_CONTRACT = "os-debit-vertical-record-v1"
_LIST_LIMIT = 60


class OSDebitVerticalMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"


def os_debit_vertical_mode(environ: Mapping[str, str] | None = None) -> OSDebitVerticalMode:
    """`REALTIME_V3_OS_DEBIT_VERTICAL`; unset or blank is OFF. Anything but OFF or RECORD raises."""
    raw = (os.environ if environ is None else environ).get(OS_DEBIT_VERTICAL_SETTING)
    value = (raw or "").strip().upper() or OSDebitVerticalMode.OFF.value
    try:
        return OSDebitVerticalMode(value)
    except ValueError:
        raise ValueError(f"{OS_DEBIT_VERTICAL_SETTING} must be OFF or RECORD, not {raw!r}") from None


@dataclass(frozen=True, slots=True)
class OSDebitVerticalOutcome:
    """What the vertical rule gives one OS invocation at one cutoff: a vertical, or the stable reason it has none."""

    event_id: UUID
    signal_id: str
    underlyer: str
    direction: int
    pairing: str  # V1_O1_EVENT / V2_PARTICIPATION
    contract_ticker: str | None
    package_structure: str | None  # today's package: SINGLE_CONTRACT / O1_DEBIT_VERTICAL / None
    vertical: object | None = None  # advanced_options_o1_debit_vertical.DebitVertical
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class OSDebitVerticalLane:
    """One lane's outcomes (aligned with the lane's invocations), or the reason the lane has none."""

    outcomes: tuple[OSDebitVerticalOutcome, ...] = ()
    status: str = "OK"
    policy_version: str | None = None
    policy_sha256: str | None = None
    inputs_source: str | None = None  # O1_LANE / OWN_READ / NONE


def _package_structure(package) -> str | None:
    if package is None:
        return None
    from .advanced_options_o1_debit_vertical import is_o1_debit_vertical_package

    if is_o1_debit_vertical_package(package):
        return "O1_DEBIT_VERTICAL"
    legs = getattr(package, "legs", ()) or ()
    return "SINGLE_CONTRACT" if len(legs) == 1 else f"LEGS_{len(legs)}"


def _invocation_view(invocation) -> tuple[str, str | None, str]:
    """(pairing, contract ticker, signal id) of a v1 or v2 OS invocation."""
    signal = invocation.signal
    if hasattr(invocation, "outcome"):  # AdvancedOSParticipationInvocation (AL5 ON)
        outcome = invocation.outcome
        contract = getattr(outcome, "confirmed_contract", None) if getattr(outcome, "confirmed", False) else None
        return "V2_PARTICIPATION", contract, str(signal.signal_id)
    source = invocation.source
    return "V1_O1_EVENT", (None if source is None else str(source.contract_ticker)), str(signal.signal_id)


def os_debit_vertical_outcomes(invocations: Sequence[object], inputs, plan, *, policy,
                               inputs_reason: str | None = None) -> tuple[OSDebitVerticalOutcome, ...]:
    """One outcome per invocation, in order. Never raises on market data (each failure is that invocation's reason)."""
    from .advanced_options_o1_debit_vertical import select_debit_vertical

    outcomes = []
    for invocation in invocations:
        signal = invocation.signal
        pairing, ticker, signal_id = _invocation_view(invocation)
        base = dict(
            event_id=invocation.event_id, signal_id=signal_id, underlyer=str(signal.underlyer),
            direction=int(signal.direction), pairing=pairing, contract_ticker=ticker,
            package_structure=_package_structure(getattr(invocation, "package", None)),
        )
        if ticker is None:
            outcomes.append(OSDebitVerticalOutcome(**base, reason="OSDV_NO_CONTRACT"))
            continue
        if inputs is None:
            outcomes.append(OSDebitVerticalOutcome(**base, reason=inputs_reason or "O1DV_INPUTS_UNAVAILABLE"))
            continue
        try:
            long = inputs.by_ticker.get(ticker.strip().upper())
            result = select_debit_vertical(
                long, inputs.contracts, inputs.marks, inputs.quotes,
                unmarked=plan.unmarked_instrument_ids,
                spot=None if long is None else inputs.spots.get(long.underlying),
                session_date=plan.control_plane.session_date, cutoff=plan.cutoff, policy=policy,
            )
        except (ValueError, TypeError, ArithmeticError, KeyError, AttributeError) as error:
            result = f"O1DV_BUILD_FAILED:{type(error).__name__}"
        if isinstance(result, str):
            outcomes.append(OSDebitVerticalOutcome(**base, reason=result))
        else:
            outcomes.append(OSDebitVerticalOutcome(**base, vertical=result))
    return tuple(outcomes)


def attach_os_debit_verticals(cursor, plan, lanes: Mapping[object, object], *, o1_vertical: Mapping | None = None):
    """RECORD: each OS lane with its outcomes on `os_vertical` (never part of an identity). Returns the lanes in the
    same mapping shape. Reads only when some lane has invocations and the O1 lane did not already read the inputs.
    Never raises: a failure is the lanes' status."""
    from dataclasses import replace

    from .advanced_options_o1_debit_vertical import load_o1_vertical_inputs_guarded, o1_debit_vertical_policy

    result = {}
    try:
        policy = o1_debit_vertical_policy()
        inputs, reason, source = None, None, "NONE"
        if any(lane.invocations for lane in lanes.values()):
            if o1_vertical and o1_vertical.get("inputs") is not None:
                inputs, source = o1_vertical["inputs"], "O1_LANE"
            else:
                inputs, reason = load_o1_vertical_inputs_guarded(cursor, plan)
                source = "OWN_READ"
        for detector, lane in lanes.items():
            if lane.blocked_reason_codes:
                record = OSDebitVerticalLane(status="LANE_BLOCKED", policy_version=policy.version,
                                             policy_sha256=policy.policy_sha256, inputs_source=source)
            else:
                record = OSDebitVerticalLane(
                    outcomes=os_debit_vertical_outcomes(lane.invocations, inputs, plan, policy=policy,
                                                        inputs_reason=reason),
                    policy_version=policy.version, policy_sha256=policy.policy_sha256, inputs_source=source)
            result[detector] = replace(lane, os_vertical=record)
        return result
    except Exception as error:  # noqa: BLE001 - recording never fails the window
        status = f"RECORD_FAILED:{type(error).__name__}"[:80]
        out = {}
        for detector, lane in lanes.items():
            try:
                out[detector] = replace(lane, os_vertical=OSDebitVerticalLane(status=status))
            except Exception:  # noqa: BLE001
                out[detector] = lane
        return out


def os_debit_vertical_document(detector: str, cutoff: datetime, lane: OSDebitVerticalLane,
                               evaluations: Sequence[object] = ()) -> dict:
    """One lane's RECORD document for one window (pure). `evaluations` are the executor's, one per invocation."""
    statuses = [getattr(row, "selection_status", None) for row in evaluations]
    items, reasons, counts = [], Counter(), Counter()
    for index, outcome in enumerate(lane.outcomes):
        status = statuses[index] if index < len(statuses) else None
        counts["invocations"] += 1
        counts[f"status:{status}"] += 1
        item = {
            "event_id": str(outcome.event_id), "signal_id": outcome.signal_id, "underlyer": outcome.underlyer,
            "direction": outcome.direction, "pairing": outcome.pairing, "long": outcome.contract_ticker,
            "status": status, "package_today": outcome.package_structure,
        }
        if outcome.vertical is not None:
            counts["built"] += 1
            counts["built_selected"] += int(status == "SELECTED")
            item["vertical"] = outcome.vertical.record()
        else:
            reasons[outcome.reason] += 1
            item["reason"] = outcome.reason
        items.append(item)
    # Selected first, then built, then the rest, so the cap keeps what matters.
    items.sort(key=lambda row: (row["status"] != "SELECTED", "vertical" not in row, row["underlyer"],
                                str(row["long"]), row["event_id"]))
    return {
        "contract": OS_DEBIT_VERTICAL_CONTRACT, "mode": OSDebitVerticalMode.RECORD.value, "applied": False,
        "execution_permission": False, "detector": detector, "cutoff": cutoff.isoformat(),
        "status": lane.status, "policy": lane.policy_version, "policy_sha256": lane.policy_sha256,
        "inputs": lane.inputs_source,
        "invocations": counts["invocations"], "built": counts["built"], "built_selected": counts["built_selected"],
        "by_status": {key.split(":", 1)[1]: value for key, value in sorted(counts.items())
                      if key.startswith("status:")},
        "rejected": dict(sorted(reasons.items())),
        "items": items[:_LIST_LIMIT],
        "truncated": len(items) > _LIST_LIMIT,
    }


def log_os_debit_vertical_record(detector: str, cutoff: datetime, lane, evaluations: Iterable[object] = ()) -> None:
    """The executor's line (RECORD only: `lane` is None when OFF). Never raises."""
    if lane is None:
        return
    try:
        document = os_debit_vertical_document(detector, cutoff, lane, tuple(evaluations))
    except Exception as error:  # noqa: BLE001
        document = {"contract": OS_DEBIT_VERTICAL_CONTRACT, "mode": OSDebitVerticalMode.RECORD.value,
                    "detector": detector, "status": "RECORD_FAILED",
                    "reason": f"{type(error).__name__}: {error}"[:200]}
    try:
        print(f"{OS_DEBIT_VERTICAL_RECORD_EVENT} {json.dumps(document, sort_keys=True, default=str)}",
              file=sys.stderr, flush=True)
    except Exception:  # noqa: BLE001
        pass


__all__ = [
    "OSDebitVerticalLane", "OSDebitVerticalMode", "OSDebitVerticalOutcome", "OS_DEBIT_VERTICAL_RECORD_EVENT",
    "OS_DEBIT_VERTICAL_SETTING", "attach_os_debit_verticals", "log_os_debit_vertical_record",
    "os_debit_vertical_document", "os_debit_vertical_mode", "os_debit_vertical_outcomes",
]
