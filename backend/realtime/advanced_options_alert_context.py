"""G106 O1 tuning (Nov 18 2026): dealer gamma / delta context of every O1 / OS alert or detection the Options window
selects, RECORD only. No filter: nothing reads these lines; they are the evidence a regime rule would need.

Why (levels study Nov 9-17, small sample): no gamma or delta level was a robust filter for O1 / OS alerts, so the study
recommended recording the regime context on every alert first: the dealer gamma sign, spot vs the gamma flip, the room to
the wall in the alert's direction, the call / put delta tilt and the share of gamma expiring today. The numbers are the
Options Overview page's (OV1, `options_exposure_read.build_exposure_chain` + `compute_exposure`, weight = prior OI,
scope = every expiry), reduced by the scheduler's levels recorder (`advanced_options_levels.levels_document`), on the
window's chain capture (`rt.option_breadth_current`, the capture of the window's Equity cutoff).

`REALTIME_V3_OPTIONS_ALERT_CONTEXT` on the Options window (`realtime-v3-options-p8-window`):
| OFF (default, unset, blank) | nothing: no note, no SQL, no line (today, byte for byte) |
| RECORD | the O1 / OS executors note what they select (O1: SELECTED and OBSERVATION evaluations, i.e. every detection that
           survived the gates that are ON; OS1 / OS2: CONFIRMED evaluations, and with REALTIME_V3_OS_PARTICIPATION
           RECORD/ON each live v2 signal at its first window or when v2 or flow20 confirms it); after the window has run,
           one short read-only transaction (statement timeout 20 s) reads the capture of each noted underlying and
           logs one `OPTIONS_ALERT_CONTEXT_RECORD {json}` line per noted item and one `OPTIONS_ALERT_CONTEXT_WINDOW
           {json}` line per window with notes |
Anything else (any case accepted for the two) and the window service refuses to start. Recording never fails, delays or
changes the window: the read runs after the window's run, every error becomes a line (`status` / `reason`).

Capture rule (no look-ahead past the Equity cutoff): per underlying, the newest capture with equity_cutoff <= the
window's Equity cutoff, COMPLETE; its rows (the page's latest-capture read) must all still be there (rows == the
capture's contract count), else the item says CAPTURE_ROWS_INCOMPLETE (a newer capture is replacing them). `capture`
on each line names the capture used (its Equity cutoff, observed_at, whether it is this window's, age at the cutoff).

Direction context (the alert's direction: O1 = the contract type, call +1 / put -1; OS = the signal's direction):
- `room_to_wall_pct`: bullish = the call wall's distance above spot (call_wall_pct), bearish = the put wall's distance
  below spot (-put_wall_pct); negative = the wall is already behind spot;
- `flip_side`: WITH when spot is above the flip for a bullish alert or below it for a bearish one, AGAINST otherwise;
- `dex_tilt_in_direction`: dex_tilt x direction (> 0: the call / put delta tilt agrees with the alert);
- `gex_sign`, `regime_at_spot`, `zero_dte_share` as the page.
Dealer side is not observable: the page's convention DEALER_LONG_CALLS_SHORT_PUTS is a stated hypothesis.
"""
from __future__ import annotations

import os
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Callable, Iterable, Mapping, Sequence

UTC = timezone.utc

ALERT_CONTEXT_SETTING = "REALTIME_V3_OPTIONS_ALERT_CONTEXT"
ALERT_CONTEXT_RECORD_EVENT = "OPTIONS_ALERT_CONTEXT_RECORD"
ALERT_CONTEXT_WINDOW_EVENT = "OPTIONS_ALERT_CONTEXT_WINDOW"
VERSION = "options-alert-context-record-v1"
STATEMENT_TIMEOUT = "20s"
# Notes waiting for the window's record; a window that never reaches its record (an exception) leaves its notes to the
# next one, so the list is capped.
MAXIMUM_PENDING = 2000

CAPTURES_SQL = (
    "SELECT underlying,equity_cutoff,observed_at,state,contracts FROM rt.option_breadth_captures "
    "WHERE session_date=%s AND underlying=ANY(%s) AND equity_cutoff<=%s "
    "ORDER BY underlying,equity_cutoff DESC,observed_at DESC"
)


class AlertContextMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"


def options_alert_context_mode(environ: Mapping[str, str] | None = None) -> AlertContextMode:
    """`REALTIME_V3_OPTIONS_ALERT_CONTEXT`; unset or blank is OFF. Anything but OFF or RECORD raises."""
    raw = (os.environ if environ is None else environ).get(ALERT_CONTEXT_SETTING)
    value = (raw or "").strip().upper() or AlertContextMode.OFF.value
    try:
        return AlertContextMode(value)
    except ValueError:
        raise ValueError(f"{ALERT_CONTEXT_SETTING} must be OFF or RECORD, not {raw!r}") from None


def prepare_options_alert_context(environ: Mapping[str, str] | None = None) -> AlertContextMode:
    """The window service's start check: the switch (raises when invalid); under RECORD the page's modules load here,
    not in the first window (~3.5 s measured on the scheduler)."""
    mode = options_alert_context_mode(environ)
    if mode is AlertContextMode.RECORD:
        try:
            from options.analytics import gamma_exposure  # noqa: F401

            from . import advanced_options_levels, options_exposure_read  # noqa: F401
        except Exception:  # noqa: BLE001 - the first window then says why
            pass
    return mode


# ---- notes (from the executors, in the window's process) --------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class AlertContextItem:
    """One alert or detection a lane selected in one window."""

    cutoff: datetime
    detector: str  # O1 / OS1 / OS2
    kind: str  # SELECTED / OBSERVATION / SIGNAL (an OS v2 live signal noted for its first window or a confirmation)
    key: str  # O1: the event id; OS: the signal id
    underlyer: str
    direction: int
    contract_ticker: str | None = None
    contract_type: str | None = None
    dte: int | None = None
    detail: Mapping[str, object] = field(default_factory=dict, compare=False)


_LOCK = threading.Lock()
_PENDING: list[AlertContextItem] = []


def _note(items: Iterable[AlertContextItem]) -> None:
    with _LOCK:
        _PENDING.extend(items)
        if len(_PENDING) > MAXIMUM_PENDING:
            del _PENDING[: len(_PENDING) - MAXIMUM_PENDING]


def drain_alert_context_notes() -> list[AlertContextItem]:
    """Every pending note, once (de-duplicated per cutoff, detector, kind and key: a lane run twice notes twice)."""
    with _LOCK:
        pending = list(_PENDING)
        _PENDING.clear()
    seen, unique = set(), []
    for item in pending:
        identity = (item.cutoff, item.detector, item.kind, item.key)
        if identity not in seen:
            seen.add(identity)
            unique.append(item)
    return unique


def _type_direction(contract_type: str | None) -> int:
    return 1 if contract_type == "CALL" else -1


def note_o1_alert_context(cutoff: datetime, evaluations: Sequence, invocations: Sequence) -> None:
    """The O1 executor's note (RECORD only): every SELECTED / OBSERVATION evaluation (a detection the gates that are ON
    kept as an alert or an observation). Never raises; OFF does nothing."""
    try:
        if options_alert_context_mode() is AlertContextMode.OFF:
            return
        by_event = {row.event_id: row for row in invocations}
        items = []
        for evaluation in evaluations:
            if evaluation.selection_status not in ("SELECTED", "OBSERVATION"):
                continue
            invocation = by_event.get(evaluation.event_id)
            source = None if invocation is None else invocation.source
            if source is None:
                continue
            payload = evaluation.payload if isinstance(evaluation.payload, Mapping) else {}
            detail = {"gate_reasons": list(payload.get("gate_reasons") or [])}
            aggressor = payload.get("aggressor")
            if isinstance(aggressor, Mapping):
                detail["aggressor_class"] = aggressor.get("aggressor_class")
            elif getattr(invocation, "aggressor", None) is not None:
                detail["aggressor_class"] = _aggressor_class(invocation.aggressor)
            items.append(AlertContextItem(
                cutoff=cutoff, detector="O1", kind=evaluation.selection_status, key=str(evaluation.event_id),
                underlyer=str(source.underlyer), direction=_type_direction(source.contract_type),
                contract_ticker=source.contract_ticker, contract_type=source.contract_type,
                dte=(source.expiration_date - source.volume_session).days, detail=detail))
        _note(items)
    except Exception:  # noqa: BLE001 - recording never fails the window
        pass


def _aggressor_class(tally) -> str | None:
    try:
        from .advanced_options_o1_gates import classify_aggressor, o1_aggressor_side_policy

        return classify_aggressor(tally, o1_aggressor_side_policy())[0]
    except Exception:  # noqa: BLE001
        return None


def note_os_alert_context(detector: str, cutoff: datetime, evaluations: Sequence, record=None) -> None:
    """The OS executor's note (RECORD only): CONFIRMED evaluations (SELECTED / OBSERVATION) and, with an OS v2 record,
    each live signal at its first window or when v2 or flow20 confirms it. Never raises; OFF does nothing."""
    try:
        if options_alert_context_mode() is AlertContextMode.OFF:
            return
        items = []
        for evaluation in evaluations:
            if evaluation.selection_status not in ("SELECTED", "OBSERVATION"):
                continue
            payload = evaluation.payload if isinstance(evaluation.payload, Mapping) else {}
            signal = payload.get("signal") or {}
            decision = payload.get("decision") or {}
            items.append(AlertContextItem(
                cutoff=cutoff, detector=detector, kind=evaluation.selection_status,
                key=str(signal.get("signal_id") or evaluation.event_id), underlyer=str(evaluation.underlyer),
                direction=int(signal.get("direction") or decision.get("direction") or 0),
                contract_ticker=decision.get("contract"),
                detail={"event_id": str(evaluation.event_id), "evaluation": payload.get("contract"),
                        "signal_market_time": signal.get("market_time")}))
        for outcome in getattr(record, "outcomes", None) or ():
            signal = outcome.signal
            flow20 = bool(outcome.flow20 and outcome.flow20.get("confirmed"))
            if not (signal.first_window or outcome.confirmed or flow20):
                continue
            items.append(AlertContextItem(
                cutoff=cutoff, detector=detector, kind="SIGNAL", key=signal.signal_id, underlyer=signal.underlyer,
                direction=int(signal.direction), contract_ticker=outcome.confirmed_contract,
                detail={"first_window": signal.first_window, "v2_confirmed": outcome.confirmed,
                        "flow20_confirmed": flow20, "signal_market_time": signal.market_time.isoformat()}))
        _note(items)
    except Exception:  # noqa: BLE001 - recording never fails the window
        pass


# ---- the read and the computation -----------------------------------------------------------------------------------------

def _value(row, index: int, name: str):
    return row[name] if isinstance(row, Mapping) else row[index]


def _moment(value) -> datetime | None:
    from .advanced_options_levels import _moment as moment

    return moment(value)


@dataclass
class CaptureRead:
    """One underlying's capture as read: the chain (None when unavailable, with `reason`)."""

    underlying: str
    chain: object | None = None
    reason: str | None = None
    equity_cutoff: datetime | None = None
    observed_at: datetime | None = None
    state: str | None = None
    contracts: int | None = None
    rows: int = 0
    fallback: bool = False


def read_captures_cursor(cursor, *, session_date: date, equity_cutoff: datetime, underlyings: Sequence[str],
                         build: Callable[..., object] | None = None) -> dict[str, CaptureRead]:
    """Per underlying: the newest COMPLETE capture at or before the Equity cutoff and its rows (the page's read). One
    name's failure is that name's reason only (savepoint)."""
    from .advanced_options_levels import GROUPS_SQL, _rows_sql
    from .options_exposure_read import _ROW_NAMES

    if build is None:
        from .options_exposure_read import build_exposure_chain as build
    names = sorted({str(name).strip().upper() for name in underlyings if name})
    result = {name: CaptureRead(name, reason="NO_CAPTURE") for name in names}
    if not names:
        return result
    cursor.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
    cursor.execute(CAPTURES_SQL, (session_date, names, equity_cutoff))
    captures: dict[str, list[tuple]] = {}
    for row in cursor.fetchall():
        name = str(_value(row, 0, "underlying")).strip().upper()
        captures.setdefault(name, []).append((
            _moment(_value(row, 1, "equity_cutoff")), _moment(_value(row, 2, "observed_at")),
            str(_value(row, 3, "state")), _value(row, 4, "contracts")))
    rows_sql = _rows_sql()
    for name in names:
        listed = captures.get(name) or []
        complete = [item for item in listed if item[2] == "COMPLETE" and item[1] is not None]
        if not complete:
            result[name] = CaptureRead(name, reason="NO_COMPLETE_CAPTURE" if listed else "NO_CAPTURE",
                                       state=listed[0][2] if listed else None)
            continue
        capture_cutoff, observed, state, contracts = complete[0]
        read = CaptureRead(name, equity_cutoff=capture_cutoff, observed_at=observed, state=state,
                           contracts=None if contracts is None else int(contracts), fallback=complete[0] is not listed[0])
        result[name] = read
        cursor.execute("SAVEPOINT alert_context_name")
        try:
            cursor.execute(GROUPS_SQL, (session_date, name, observed))
            groups = [(_moment(_value(item, 0, "observed_at")), int(_value(item, 1, "count")))
                      for item in cursor.fetchall()]
            if not groups or groups[0][0] != observed:
                read.reason = "CAPTURE_ROWS_MISSING"
                cursor.execute("RELEASE SAVEPOINT alert_context_name")
                continue
            earlier = sum(count for _observed, count in groups[1:])
            cursor.execute(rows_sql, (session_date, name, observed))
            rows = [dict(item) if isinstance(item, Mapping) else dict(zip(_ROW_NAMES, item))
                    for item in cursor.fetchall()]
            cursor.execute("RELEASE SAVEPOINT alert_context_name")
        except Exception as error:  # noqa: BLE001 - this name only
            cursor.execute("ROLLBACK TO SAVEPOINT alert_context_name")
            read.reason = f"READ:{type(error).__name__}"[:80]
            continue
        read.rows = len(rows)
        if read.contracts is not None and len(rows) != read.contracts:
            read.reason = "CAPTURE_ROWS_INCOMPLETE"
            continue
        try:
            chain = build(rows, underlyer=name, session_date=session_date, observed_at=observed,
                          earlier_capture_rows=earlier)
        except Exception as error:  # noqa: BLE001 - this name only
            read.reason = f"BUILD:{type(error).__name__}"[:80]
            continue
        if chain is None:
            read.reason = "SPOT_UNAVAILABLE"
        else:
            read.chain = chain
    return result


def direction_context(levels: Mapping[str, object], direction: int) -> dict:
    """The levels seen from the alert's direction (see the module doc). Pure."""
    if not levels.get("available") or direction not in (-1, 1):
        return {"direction": direction, "available": False}
    wall = "CALL_WALL" if direction == 1 else "PUT_WALL"
    wall_pct = levels.get("call_wall_pct") if direction == 1 else levels.get("put_wall_pct")
    side = levels.get("spot_vs_flip")
    flip_side = (None if side not in ("ABOVE", "BELOW")
                 else "WITH" if (side == "ABOVE") == (direction == 1) else "AGAINST")
    tilt = levels.get("dex_tilt")
    return {
        "direction": direction, "available": True, "wall": wall,
        "wall_strike": levels.get("call_wall") if direction == 1 else levels.get("put_wall"),
        "room_to_wall_pct": None if wall_pct is None else round(float(wall_pct) * direction, 3),
        "flip_side": flip_side,
        "dex_tilt_in_direction": None if tilt is None else round(float(tilt) * direction, 4),
        "gex_sign": levels.get("gex_sign"), "regime_at_spot": levels.get("regime_at_spot"),
        "zero_dte_share": levels.get("zero_dte_share"),
    }


def alert_context_documents(items: Sequence[AlertContextItem], reads: Mapping[str, CaptureRead], *,
                            session_date: date, equity_cutoff: datetime | None, window_id: str | None,
                            levels: Callable[[object], dict] | None = None) -> tuple[list[dict], dict, Counter]:
    """(one document per item, the per-underlying levels used, reasons). Pure apart from the computation."""
    if levels is None:
        from .advanced_options_levels import levels_document as levels
    computed: dict[str, dict] = {}
    reasons: Counter = Counter()
    for name, read in sorted(reads.items()):
        if read.chain is None:
            computed[name] = {"available": False, "reason": read.reason}
            continue
        started = time.monotonic()
        try:
            computed[name] = {**levels(read.chain), "ms": round((time.monotonic() - started) * 1000.0, 1)}
        except Exception as error:  # noqa: BLE001 - this name only
            computed[name] = {"available": False, "reason": f"COMPUTE:{type(error).__name__}"[:80]}
    documents = []
    for item in items:
        read = reads.get(item.underlyer.strip().upper())
        level = computed.get(item.underlyer.strip().upper(), {"available": False, "reason": "NOT_READ"})
        if not level.get("available"):
            reasons[str(level.get("reason"))] += 1
        capture = None if read is None or read.observed_at is None else {
            "equity_cutoff": None if read.equity_cutoff is None else read.equity_cutoff.isoformat(),
            "observed_at": read.observed_at.isoformat(),
            "this_window": read.equity_cutoff == equity_cutoff,
            "fallback_past_newer_capture": read.fallback,
            "age_at_cutoff_s": round((item.cutoff - read.observed_at).total_seconds(), 1),
            "rows": read.rows, "contracts": read.contracts,
        }
        documents.append({
            "contract": VERSION, "mode": AlertContextMode.RECORD.value, "applied": False,
            "execution_permission": False, "session": str(session_date), "cutoff": item.cutoff.isoformat(),
            "equity_cutoff": None if equity_cutoff is None else equity_cutoff.isoformat(), "window_id": window_id,
            "detector": item.detector, "kind": item.kind, "key": item.key, "underlyer": item.underlyer,
            "direction": item.direction, "contract_ticker": item.contract_ticker,
            "contract_type": item.contract_type, "dte": item.dte, "detail": dict(item.detail),
            "available": bool(level.get("available")), "reason": None if level.get("available") else level.get("reason"),
            "capture": capture,
            "context": direction_context(level, item.direction),
            "levels": level,
        })
    return documents, computed, reasons


# ---- the window's record -----------------------------------------------------------------------------------------------

def _emit(event: str, document: Mapping[str, object]) -> None:
    from .advanced_options_levels import render_line

    print(render_line(event, document), file=sys.stderr, flush=True)


def record_options_alert_context(provider, *, session_date: date, cutoff: datetime, equity_cutoff: datetime | None,
                                 window_id: str | None = None, emit: Callable[[str, Mapping], None] = _emit,
                                 read: Callable[..., dict] = read_captures_cursor) -> dict | None:
    """After the window's run (the Options window, `build_and_run_p8_serial_window`): the notes of this and any
    earlier window, read and logged. OFF: None and nothing else. Never raises."""
    started = time.monotonic()
    try:
        if options_alert_context_mode() is AlertContextMode.OFF:
            return None
    except Exception:  # noqa: BLE001 - the service refuses to start on an invalid value; never fail a window here
        return None
    items: list[AlertContextItem] = []
    head = {"contract": "options-alert-context-window-v1", "mode": AlertContextMode.RECORD.value, "applied": False,
            "session": str(session_date), "cutoff": _iso(cutoff), "equity_cutoff": _iso(equity_cutoff),
            "window_id": window_id}
    try:
        items = drain_alert_context_notes()
        if not items:
            return {"status": "NO_ITEMS"}
        stale = [item for item in items if item.cutoff != cutoff]
        names = sorted({item.underlyer.strip().upper() for item in items})
        from .database_access_v2 import APP_ROLE

        read_started = time.monotonic()
        with provider.transaction(APP_ROLE, read_only=True) as unit:
            reads = read(unit.cursor, session_date=session_date, equity_cutoff=equity_cutoff or cutoff,
                         underlyings=names)
        read_seconds = round(time.monotonic() - read_started, 3)
        documents, computed, reasons = alert_context_documents(
            items, reads, session_date=session_date, equity_cutoff=equity_cutoff, window_id=window_id)
        for document in documents:
            emit(ALERT_CONTEXT_RECORD_EVENT, document)
        summary = {
            **head, "status": "OK", "items": len(items), "earlier_window_items": len(stale),
            "by_detector": dict(sorted(Counter(f"{item.detector}:{item.kind}" for item in items).items())),
            "underlyings": names,
            "available": sum(bool(document["available"]) for document in documents),
            "unavailable_reasons": dict(sorted(reasons.items())),
            "gex_sign": {name: value.get("gex_sign") for name, value in sorted(computed.items())},
            "read_seconds": read_seconds, "seconds": round(time.monotonic() - started, 3),
        }
        emit(ALERT_CONTEXT_WINDOW_EVENT, summary)
        return {"status": "RECORDED", "items": len(items), "available": summary["available"]}
    except Exception as error:  # noqa: BLE001 - recording never fails the window
        try:
            emit(ALERT_CONTEXT_WINDOW_EVENT, {**head, "status": "RECORD_FAILED", "items": len(items),
                                              "reason": f"{type(error).__name__}: {error}"[:200],
                                              "seconds": round(time.monotonic() - started, 3)})
        except Exception:  # noqa: BLE001
            pass
        return {"status": "RECORD_FAILED"}


def _iso(value) -> str | None:
    return None if value is None else value.isoformat()


__all__ = [
    "ALERT_CONTEXT_RECORD_EVENT", "ALERT_CONTEXT_SETTING", "ALERT_CONTEXT_WINDOW_EVENT", "AlertContextItem",
    "AlertContextMode", "CaptureRead", "alert_context_documents", "direction_context", "drain_alert_context_notes",
    "note_o1_alert_context", "note_os_alert_context", "options_alert_context_mode", "prepare_options_alert_context",
    "read_captures_cursor", "record_options_alert_context",
]
