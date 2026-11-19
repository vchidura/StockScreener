"""O1 debit-vertical leg marks with a quote-age tolerance, RECORD only (G106 AL2 follow-up, Nov 19 2026).

Why (evidence/eod-20261118/EOD_ANALYSIS.md section 6): on Nov 18, 63% of the O1 debit-vertical rejections were
O1DV_LONG_LEG_UNMARKED (and 6% O1DV_SHORT_LEG_UNMARKED). A leg needs the window's cutoff mark, and the window marks a
contract only when its NBBO is at most 1 s old at the cutoff (`advanced_options_mark_producer.
ADVANCED_NBBO_MAXIMUM_AGE_SECONDS`): a median 154 of the 384 watched contracts were marked at a cutoff, because quiet
contracts have no quote inside the last second.

Switch `REALTIME_V3_O1DV_MARK_TOLERANCE` on the Options window service:
| OFF (default, unset, blank) | nothing at all: no read, no computation, no line |
| RECORD | the window hands this module the journal's quote heads at its barrier offset (the V3-1.5 cutoff read,
           `advanced_options_cutoff_quotes.read_cutoff_quotes`, which the window already makes when
           REALTIME_V3_MARKS_AT_CUTOFF is RECORD or JOURNAL; with OFF the window makes that read for this switch) of
           the selected contracts the mark producer did NOT mark. For each tolerance (5 s and 15 s) such a contract gets
           a leg mark from its head when the head is a usable two-sided quote (GOOD, no reason codes, bid > 0,
           ask >= bid, both sizes > 0, both exchanges, this journal generation / stream session / session date)
           stamped at or before the cutoff and at most that many seconds before it (market time); mark = midpoint,
           as the window's marks. The O1 debit vertical of every O1 event the builder evaluates this window is then
           built again with those marks added (`select_debit_vertical`, same policy, same spots and leg activity), and
           one `O1_DEBIT_VERTICAL_MARKS_RECORD {json}` line per window logs, per tolerance, how many more contracts are
           marked, how many more verticals would be built (events / detected events) and their prices with each leg's
           quote age. |
Any other value fails the window service at start and every window that reads it.

Nothing else changes in RECORD: the O1 package is still the 1 s rule's (today's single contract, or the vertical
REALTIME_V3_O1_DEBIT_VERTICAL=ON builds from the 1 s marks), alerts, packages and everything stored are unchanged.
R31 contracts (volume baseline incomplete at the cutoff) were marked by the producer and stay unmarked here. The record
needs REALTIME_V3_O1_DEBIT_VERTICAL RECORD or ON (with OFF the builder evaluates no O1 event and no line is logged).
Nothing here raises into the window: a failure is a status in the line.
"""
from __future__ import annotations

import json
import os
import sys
import threading
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Callable, Iterable, Mapping, Sequence
from uuid import UUID

O1DV_MARK_TOLERANCE_SETTING = "REALTIME_V3_O1DV_MARK_TOLERANCE"
O1DV_MARKS_RECORD_EVENT = "O1_DEBIT_VERTICAL_MARKS_RECORD"
O1DV_MARK_TOLERANCES_SECONDS = (5, 15)
O1DV_BASE_MARK_AGE_SECONDS = 1  # advanced_options_mark_producer.ADVANCED_NBBO_MAXIMUM_AGE_SECONDS
_RECORD_CONTRACT = "o1-debit-vertical-marks-record-v1"
_LIST_LIMIT = 20
# Docker splits a log line over 16 KiB; the vertical lists are trimmed below this.
_LINE_LIMIT_BYTES = 15_000
_UNMARKED_REASONS = frozenset({"O1DV_LONG_LEG_UNMARKED", "O1DV_SHORT_LEG_UNMARKED"})


class O1DVMarkToleranceMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"


def o1dv_mark_tolerance_mode(environ: Mapping[str, str] | None = None) -> O1DVMarkToleranceMode:
    """The switch value; unset or blank is OFF. Anything but OFF or RECORD raises (fails loudly, never silently)."""
    raw = (os.environ if environ is None else environ).get(O1DV_MARK_TOLERANCE_SETTING)
    value = (raw or "").strip().upper() or O1DVMarkToleranceMode.OFF.value
    try:
        return O1DVMarkToleranceMode(value)
    except ValueError:
        raise ValueError(f"{O1DV_MARK_TOLERANCE_SETTING} must be OFF or RECORD, not {raw!r}") from None


@dataclass(frozen=True, slots=True)
class ToleranceMark:
    """A leg mark taken from the contract's journal quote head at the barrier (duck-types the O3 `LegMark` fields the
    vertical rule reads: bid, ask, mark, market_time)."""

    instrument_id: int
    bid: Decimal
    ask: Decimal
    mark: Decimal
    market_time: datetime
    age_ms: int
    evidence_id: object = None


def quote_mark(
    instrument_id: int,
    row: Mapping[str, object] | None,
    *,
    cutoff: datetime,
    tolerance_seconds: float,
    journal_generation,
    stream_session_ids: Iterable[UUID] | None,
    session_date: date,
) -> ToleranceMark | str:
    """The leg mark from one `rt.quote_current`-shaped head, or the stable reason it is not usable. Never raises."""
    try:
        if row is None:
            return "NO_QUOTE_AT_BARRIER"
        if str(row.get("journal_generation")) != str(journal_generation):
            return "GENERATION_MISMATCH"
        if stream_session_ids is not None:
            sessions = (
                stream_session_ids
                if isinstance(stream_session_ids, frozenset)
                else frozenset(UUID(str(value)) for value in stream_session_ids)
            )
            session = row.get("session_id")
            if session is None or UUID(str(session)) not in sessions:
                return "SESSION_MISMATCH"
        if row.get("source_session") != session_date:
            return "SOURCE_DATE_MISMATCH"
        event_at = row.get("event_at")
        if event_at is None:
            return "NO_EVENT_TIME"
        if event_at > cutoff:
            return "AFTER_CUTOFF"
        age = (cutoff - event_at).total_seconds()
        if age > tolerance_seconds:
            return "OLDER_THAN_TOLERANCE"
        if (
            row.get("quality") != "GOOD"
            or tuple(row.get("reason_codes") or ())
            or any(row.get(key) is None for key in ("bid", "ask", "bid_size", "ask_size", "bid_exchange",
                                                     "ask_exchange"))
        ):
            return "ONE_SIDED_OR_INVALID"
        bid, ask = Decimal(str(row["bid"])), Decimal(str(row["ask"]))
        if (
            not bid.is_finite() or not ask.is_finite() or bid <= 0 or ask <= 0 or ask < bid
            or int(row["bid_size"]) <= 0 or int(row["ask_size"]) <= 0
        ):
            return "CROSSED_OR_NONPOSITIVE"
        return ToleranceMark(
            instrument_id=int(instrument_id), bid=bid, ask=ask, mark=(bid + ask) / Decimal(2),
            market_time=event_at, age_ms=int(round(age * 1000)),
        )
    except (ValueError, TypeError, ArithmeticError, KeyError, AttributeError) as error:
        return f"INVALID_ROW:{type(error).__name__}"


# ------------------------------------------------------------------------------------------------- window state
@dataclass
class O1DVToleranceWindow:
    """One window's tolerance inputs (published by the window after its marks) and the events' rebuilt verticals
    (filled by the O1 loader, read by the executor's record)."""

    cohort_id: str
    cutoff: datetime
    status: str
    unmarked_ids: frozenset[int]
    marks: dict[int, dict[int, ToleranceMark]] = field(default_factory=dict)
    quote_reasons: dict[int, Counter] = field(default_factory=dict)
    events: dict[str, dict[int, object]] = field(default_factory=dict)
    failures: Counter = field(default_factory=Counter)


_LOCK = threading.Lock()
_WINDOW: O1DVToleranceWindow | None = None


def _set_window(window: O1DVToleranceWindow | None) -> None:
    global _WINDOW
    with _LOCK:
        _WINDOW = window


def current_window(cutoff: datetime, cohort_id=None) -> O1DVToleranceWindow | None:
    """This window's state (the process runs one window at a time); None for another cutoff or cohort."""
    with _LOCK:
        window = _WINDOW
    if window is None or window.cutoff != cutoff:
        return None
    if cohort_id is not None and window.cohort_id != str(cohort_id):
        return None
    return window


def build_tolerance_window(
    *,
    cohort_id,
    cutoff: datetime,
    status: str,
    quotes: Mapping[int, Mapping[str, object]] | None,
    unmarked_ids: Iterable[int],
    journal_generation,
    stream_session_ids: Iterable[UUID] | None,
    session_date: date,
    tolerances: Sequence[int] = O1DV_MARK_TOLERANCES_SECONDS,
) -> O1DVToleranceWindow:
    unmarked = frozenset(int(value) for value in unmarked_ids)
    sessions = None if stream_session_ids is None else frozenset(UUID(str(value)) for value in stream_session_ids)
    window = O1DVToleranceWindow(cohort_id=str(cohort_id), cutoff=cutoff, status=status, unmarked_ids=unmarked)
    for tolerance in tolerances:
        added: dict[int, ToleranceMark] = {}
        reasons: Counter = Counter()
        if quotes is not None:
            for instrument_id in sorted(unmarked):
                result = quote_mark(
                    instrument_id, quotes.get(instrument_id), cutoff=cutoff, tolerance_seconds=tolerance,
                    journal_generation=journal_generation, stream_session_ids=sessions, session_date=session_date,
                )
                if isinstance(result, str):
                    reasons[result] += 1
                else:
                    added[instrument_id] = result
        window.marks[int(tolerance)] = added
        window.quote_reasons[int(tolerance)] = reasons
    return window


def publish_o1dv_window_quotes(
    *,
    mode: O1DVMarkToleranceMode,
    cohort_id,
    cutoff: datetime,
    cutoff_read,
    cutoff_status: str,
    read: Callable[[], tuple[object, str]],
    selected_ids: Iterable[int],
    marked_ids: Iterable[int],
    source_flow,
    session_date: date,
) -> O1DVToleranceWindow | None:
    """Window side (RECORD only): the quote heads at the barrier of the selected contracts the mark producer left out.
    `cutoff_read` / `cutoff_status`: the window's V3-1.5 read when it made one (REALTIME_V3_MARKS_AT_CUTOFF RECORD or
    JOURNAL); `read` makes it otherwise. OFF: nothing (the previous window's state is dropped). Never raises."""
    if mode is not O1DVMarkToleranceMode.RECORD:
        _set_window(None)
        return None
    try:
        status = cutoff_status
        if cutoff_read is None and cutoff_status == "OK":
            cutoff_read, status = read()
        marked = {int(value) for value in marked_ids}
        unmarked = [int(value) for value in selected_ids if int(value) not in marked]
        window = build_tolerance_window(
            cohort_id=cohort_id,
            cutoff=cutoff,
            status=status if cutoff_read is not None else (status or "UNAVAILABLE"),
            quotes=None if cutoff_read is None else cutoff_read.quotes,
            unmarked_ids=unmarked,
            journal_generation=source_flow.journal_generation,
            stream_session_ids=source_flow.stream_session_ids,
            session_date=session_date,
        )
    except Exception as error:  # noqa: BLE001 - RECORD never fails the window
        window = O1DVToleranceWindow(
            cohort_id=str(cohort_id), cutoff=cutoff, status=f"PUBLISH_FAILED:{type(error).__name__}"[:200],
            unmarked_ids=frozenset(),
        )
    _set_window(window)
    return window


# ------------------------------------------------------------------------------------------------- loader side
def tolerance_inputs(inputs, plan) -> dict[int, tuple[dict, frozenset[int]]] | None:
    """O1 loader side: per tolerance, (the window's cutoff marks plus the tolerance marks, the unmarked set without the
    newly marked contracts); None when this window published nothing. Never raises."""
    try:
        if inputs is None:
            return None
        window = current_window(plan.cutoff, plan.control_plane.cohort_id)
        if window is None:
            return None
        unmarked = frozenset(int(value) for value in plan.unmarked_instrument_ids)
        out = {}
        for tolerance, added in window.marks.items():
            usable = {key: value for key, value in added.items() if key in unmarked and key not in inputs.marks}
            out[tolerance] = ({**inputs.marks, **usable}, unmarked - usable.keys())
        return out
    except Exception:  # noqa: BLE001
        return None


def record_event_tolerance(event_id, row, base_outcome, inputs, plan, policy, prepared) -> None:
    """O1 loader side (RECORD only): this event's debit vertical built again with each tolerance's marks, kept in the
    window state for the record line. `base_outcome` is the 1 s rule's (None: not evaluated, nothing kept). Never
    raises."""
    if base_outcome is None or prepared is None:
        return
    window = current_window(plan.cutoff, plan.control_plane.cohort_id)
    if window is None:
        return
    from .advanced_options_o1_debit_vertical import select_debit_vertical

    results: dict[int, object] = {}
    try:
        long = inputs.by_ticker.get(str(row["contract_ticker"]).strip().upper())
        spot = None if long is None else inputs.spots.get(long.underlying)
        for tolerance, (marks, unmarked) in prepared.items():
            try:
                results[tolerance] = select_debit_vertical(
                    long, inputs.contracts, marks, inputs.quotes, unmarked=unmarked, spot=spot,
                    session_date=plan.control_plane.session_date, cutoff=plan.cutoff, policy=policy,
                )
            except (ValueError, TypeError, ArithmeticError, KeyError, AttributeError) as error:
                results[tolerance] = f"O1DV_BUILD_FAILED:{type(error).__name__}"
    except Exception as error:  # noqa: BLE001
        window.failures[f"EVENT_FAILED:{type(error).__name__}"] += 1
        return
    with _LOCK:
        window.events[str(event_id)] = results


# ------------------------------------------------------------------------------------------------- record
def _short_reason(reason: str | None) -> str | None:
    return None if reason is None else reason.replace("O1DV_", "")


def _same(a, b) -> bool:
    return (
        a.short.contract_ticker == b.short.contract_ticker
        and a.long_mark.mark == b.long_mark.mark
        and a.short_mark.mark == b.short_mark.mark
    )


def _age_ms(mark, cutoff: datetime) -> int:
    age = getattr(mark, "age_ms", None)
    if age is not None:
        return int(age)
    return int(round((cutoff - mark.market_time).total_seconds() * 1000))


def o1dv_marks_record(
    *,
    cutoff: datetime,
    entries: Sequence[tuple[UUID, str, object]],
    vertical_mode: str,
    policy=None,
    window: O1DVToleranceWindow | None = None,
    tolerances: Sequence[int] = O1DV_MARK_TOLERANCES_SECONDS,
) -> dict:
    """One window's line. `entries`: the executor's (event id, O1 disposition, 1 s outcome) per evaluated O1 event, as
    for O1_DEBIT_VERTICAL_RECORD; the rebuilt verticals come from the window state."""
    counts = Counter()
    base_reasons: Counter = Counter()
    per = {
        int(tolerance): {
            "counts": Counter(), "rejected": Counter(), "verticals": {},
        }
        for tolerance in tolerances
    }
    for event_id, disposition, outcome in entries:
        counts["o1_events"] += 1
        detected = disposition == "DETECTED"
        counts["detected"] += int(detected)
        if outcome is None:
            counts["stored_package_reused"] += 1
            continue
        counts["evaluated"] += 1
        base_built = outcome.vertical is not None
        counts["base_built"] += int(base_built)
        counts["base_built_detected"] += int(base_built and detected)
        if not base_built:
            base_reasons[outcome.reason] += 1
            counts["base_unmarked"] += int(outcome.reason in _UNMARKED_REASONS)
            counts["base_unmarked_detected"] += int(outcome.reason in _UNMARKED_REASONS and detected)
        results = {} if window is None else window.events.get(str(event_id), {})
        if not results:
            counts["not_rebuilt"] += 1
        for tolerance, slot in per.items():
            result = results.get(tolerance)
            if result is None:
                continue
            c = slot["counts"]
            if isinstance(result, str):
                slot["rejected"][result] += 1
                c["lost"] += int(base_built)
                continue
            c["built"] += 1
            c["built_detected"] += int(detected)
            if not base_built:
                c["more_built"] += 1
                c["more_built_detected"] += int(detected)
                key = (outcome.contract_ticker, result.short.contract_ticker)
                item = slot["verticals"].setdefault(key, {
                    "long": outcome.contract_ticker,
                    "short": result.short.contract_ticker,
                    "net_debit": str(result.net_debit),
                    "long_mark": str(result.long_mark.mark),
                    "short_mark": str(result.short_mark.mark),
                    "long_age_ms": _age_ms(result.long_mark, cutoff),
                    "short_age_ms": _age_ms(result.short_mark, cutoff),
                    "width": str(result.width),
                    "reward_risk": str(round(result.return_on_risk, 3)),
                    "capital_at_risk": str(result.capital_at_risk),
                    "base_reason": _short_reason(outcome.reason),
                    "events": 0,
                    "detected": 0,
                })
                item["events"] += 1
                item["detected"] += int(detected)
            elif not _same(outcome.vertical, result):
                c["changed"] += 1
                c["changed_detected"] += int(detected)
    tolerance_docs = {}
    for tolerance, slot in per.items():
        c = slot["counts"]
        verticals = sorted(slot["verticals"].values(), key=lambda item: (-item["detected"], -item["events"],
                                                                          item["long"], item["short"]))
        tolerance_docs[str(tolerance)] = {
            "marked_added": 0 if window is None else len(window.marks.get(tolerance, {})),
            "quote_reasons": {} if window is None else dict(sorted(window.quote_reasons.get(tolerance, {}).items())),
            "built": c["built"],
            "built_detected": c["built_detected"],
            "more_built": c["more_built"],
            "more_built_detected": c["more_built_detected"],
            "changed": c["changed"],
            "changed_detected": c["changed_detected"],
            "lost": c["lost"],
            "rejected": dict(sorted(slot["rejected"].items())),
            "new_verticals": len(verticals),
            "verticals": verticals[:_LIST_LIMIT],
        }
    document = {
        "contract": _RECORD_CONTRACT,
        "mode": O1DVMarkToleranceMode.RECORD.value,
        "applied": False,
        "vertical_mode": str(vertical_mode),
        "cutoff": cutoff.isoformat(),
        "policy": None if policy is None else policy.version,
        "policy_sha256": None if policy is None else policy.policy_sha256,
        "base_maximum_age_seconds": O1DV_BASE_MARK_AGE_SECONDS,
        "tolerances_seconds": [int(value) for value in tolerances],
        "quotes_status": "NOT_PUBLISHED" if window is None else window.status,
        "unmarked_contracts": 0 if window is None else len(window.unmarked_ids),
        "failures": {} if window is None else dict(sorted(window.failures.items())),
        "o1_events": counts["o1_events"],
        "detected": counts["detected"],
        "evaluated": counts["evaluated"],
        "stored_package_reused": counts["stored_package_reused"],
        "not_rebuilt": counts["not_rebuilt"],
        "base": {
            "built": counts["base_built"],
            "built_detected": counts["base_built_detected"],
            "unmarked": counts["base_unmarked"],
            "unmarked_detected": counts["base_unmarked_detected"],
            "rejected": dict(sorted(base_reasons.items())),
        },
        "tolerance": tolerance_docs,
    }
    return _fit(document)


def _fit(document: dict) -> dict:
    """Trim the vertical lists (longest first) until the line fits one Docker log line."""
    def size() -> int:
        return len(json.dumps(document, sort_keys=True, default=str))

    while size() > _LINE_LIMIT_BYTES:
        lists = [doc["verticals"] for doc in document["tolerance"].values() if doc["verticals"]]
        if not lists:
            break
        max(lists, key=len).pop()
        document["verticals_truncated"] = True
    return document


def log_o1dv_marks_record(document: Mapping[str, object]) -> None:
    print(f"{O1DV_MARKS_RECORD_EVENT} {json.dumps(document, sort_keys=True, default=str)}", file=sys.stderr,
          flush=True)


def log_window_o1dv_marks_record(cutoff: datetime, entries, *, vertical_mode, policy_loader) -> None:
    """Executor side: one line per window when the switch is RECORD (OFF: nothing). Never raises."""
    try:
        if o1dv_mark_tolerance_mode() is not O1DVMarkToleranceMode.RECORD:
            return
    except ValueError:
        return  # the window's own read of the switch already failed loudly
    try:
        log_o1dv_marks_record(o1dv_marks_record(
            cutoff=cutoff, entries=entries, vertical_mode=getattr(vertical_mode, "value", vertical_mode),
            policy=policy_loader(), window=current_window(cutoff),
        ))
    except Exception as error:  # noqa: BLE001
        try:
            log_o1dv_marks_record({
                "contract": _RECORD_CONTRACT, "mode": "RECORD", "cutoff": str(cutoff), "status": "RECORD_FAILED",
                "reason": f"{type(error).__name__}: {error}"[:200],
            })
        except Exception:  # noqa: BLE001
            pass


__all__ = [
    "O1DVMarkToleranceMode", "O1DVToleranceWindow", "O1DV_MARKS_RECORD_EVENT", "O1DV_MARK_TOLERANCES_SECONDS",
    "O1DV_MARK_TOLERANCE_SETTING", "ToleranceMark", "build_tolerance_window", "current_window",
    "log_o1dv_marks_record", "log_window_o1dv_marks_record", "o1dv_mark_tolerance_mode", "o1dv_marks_record",
    "publish_o1dv_window_quotes", "quote_mark", "record_event_tolerance", "tolerance_inputs",
]
