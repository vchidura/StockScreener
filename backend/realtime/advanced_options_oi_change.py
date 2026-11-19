"""G106 D8 / G102 R51 (Nov 19 2026): matched open interest change (P-OICHG), RECORD only, in the Options scheduler.

The open interest a chain capture reports is the prior settlement: it is fixed for the whole session and moves only
overnight. Today's open interest minus the previous session's, matched per contract, is therefore the positions opened
(positive) or closed (negative) during the previous session, the evidence that the previous session's flow was opening
flow (D8) and the matched OI change the Options Flow page never had (R51).

Once per session, after the first window whose chain capture has a COMPLETE name, the lane reads (one short read-only
transaction, statement timeout 20 s, on the pool's other connection):
- the previous session in `rt.option_breadth_daily` (G106 H1: one row per session and contract, written after each
  close; app role SELECT);
- per watched contract (the session's acquisition contracts, the V4 set) today's row in `rt.option_breadth_current`
  (just captured) next to the previous session's daily row: open interest today and before, the change, the change
  as a fraction of the previous open interest, the previous session's volume and the share of it that stayed open, and a
  flag: OPENED_LARGE (change >= `LARGE_CHANGE` contracts and >= `LARGE_FRACTION` of the previous open interest, or no
  previous open interest), CLOSED_LARGE (the same downwards), NEW (no previous row; open interest >= `LARGE_CHANGE`);
- per name, the same over the whole captured chain, aggregated in the database (calls and puts, matched contracts,
  flagged counts) with the `TOP` largest openings.

`REALTIME_V3_OPTIONS_OI_CHANGE`, read by the Options scheduler at start:
| OFF (default, unset, blank) | nothing: no SQL, no line, no key in the cycle result (byte-identical) |
| RECORD | once per session: `OPTIONS_OI_CHANGE_RECORD` per name and one `OPTIONS_OI_CHANGE_RECORD_SESSION`; nothing is
           written, nothing reads it; a failure logs `OPTIONS_OI_CHANGE_RECORD_FAILED` and one DEGRADED fault and is
           retried at the next window (`MAXIMUM_ATTEMPTS` per session) |
Anything else (any case accepted for the two) and the scheduler refuses to start. It never fails the window or the cycle.
A restart repeats the session's record once (same numbers: the inputs do not change during the session).
"""
from __future__ import annotations

import os
import time
from datetime import date, datetime
from enum import Enum
from typing import Callable, Mapping

from .advanced_options_levels import (
    FAULT_SERVICE,
    _breadth_complete,
    _failure_reason,
    _moment,
    _print_line,
)
from .fault_journal import emit_fault

OI_CHANGE_SETTING = "REALTIME_V3_OPTIONS_OI_CHANGE"
OI_CHANGE_SETTING_EVENT = "OPTIONS_OI_CHANGE_SETTING"
OI_CHANGE_RECORD_EVENT = "OPTIONS_OI_CHANGE_RECORD"
OI_CHANGE_SESSION_EVENT = "OPTIONS_OI_CHANGE_RECORD_SESSION"
OI_CHANGE_FAILED_EVENT = "OPTIONS_OI_CHANGE_RECORD_FAILED"
VERSION = "options-oi-change-record-v1"
LARGE_CHANGE = 1000
LARGE_FRACTION = 0.25
TOP = 3
MAXIMUM_ATTEMPTS = 3
STATEMENT_TIMEOUT = "20s"
LIST_LIMIT = 40

PREVIOUS_SQL = "SELECT max(session_date) FROM rt.option_breadth_daily WHERE session_date<%s"
WATCHED_SQL = (
    "SELECT current.contract_ticker,current.underlying,current.contract_type,current.expiration_date,"
    "current.strike_price,current.open_interest AS oi_today,current.day_volume AS volume_today,"
    "daily.contract_ticker IS NOT NULL AS matched,daily.open_interest AS oi_previous,"
    "daily.day_volume AS volume_previous FROM rt.option_breadth_current AS current "
    "LEFT JOIN rt.option_breadth_daily AS daily ON daily.session_date=%s "
    "AND daily.contract_ticker=current.contract_ticker "
    "WHERE current.session_date=%s AND current.contract_ticker=ANY(%s) ORDER BY current.contract_ticker"
)
# The flag rule of `classify`, in SQL, for the chain-wide counts (both open interests known).
_BOTH = "current.open_interest IS NOT NULL AND daily.open_interest IS NOT NULL"
_OPENED = (f"{_BOTH} AND current.open_interest-daily.open_interest>=%(change)s AND (daily.open_interest=0 "
           "OR current.open_interest-daily.open_interest>=%(fraction)s*daily.open_interest)")
_CLOSED = (f"{_BOTH} AND daily.open_interest>0 AND daily.open_interest-current.open_interest>=%(change)s "
           "AND daily.open_interest-current.open_interest>=%(fraction)s*daily.open_interest")
CHAIN_SQL = (
    "SELECT current.underlying,current.contract_type,count(*) AS contracts,"
    "count(daily.contract_ticker) AS matched,"
    f"coalesce(sum(current.open_interest) FILTER (WHERE {_BOTH}),0) AS oi_today,"
    f"coalesce(sum(daily.open_interest) FILTER (WHERE {_BOTH}),0) AS oi_previous,"
    f"count(*) FILTER (WHERE {_OPENED}) AS opened_large,"
    f"count(*) FILTER (WHERE {_CLOSED}) AS closed_large,"
    "count(*) FILTER (WHERE daily.contract_ticker IS NULL AND current.open_interest>=%(change)s) AS new_large "
    "FROM rt.option_breadth_current AS current "
    "LEFT JOIN rt.option_breadth_daily AS daily ON daily.session_date=%(previous)s "
    "AND daily.contract_ticker=current.contract_ticker "
    "WHERE current.session_date=%(session)s GROUP BY current.underlying,current.contract_type "
    "ORDER BY current.underlying,current.contract_type"
)
TOP_SQL = (
    "SELECT underlying,contract_ticker,contract_type,expiration_date,strike_price,oi_today,oi_previous FROM ("
    "SELECT current.underlying,current.contract_ticker,current.contract_type,current.expiration_date,"
    "current.strike_price,current.open_interest AS oi_today,daily.open_interest AS oi_previous,"
    "row_number() OVER (PARTITION BY current.underlying "
    "ORDER BY current.open_interest-daily.open_interest DESC,current.contract_ticker) AS rank "
    "FROM rt.option_breadth_current AS current JOIN rt.option_breadth_daily AS daily "
    "ON daily.session_date=%(previous)s AND daily.contract_ticker=current.contract_ticker "
    f"WHERE current.session_date=%(session)s AND {_BOTH} "
    "AND current.open_interest>daily.open_interest) AS ranked "
    "WHERE rank<=%(top)s ORDER BY underlying,rank"
)


# ---- the switch -------------------------------------------------------------------------------------------------------

class OiChangeMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"


def parse_oi_change_mode(raw: object) -> OiChangeMode:
    if isinstance(raw, OiChangeMode):
        return raw
    if raw is not None and not isinstance(raw, str):
        raise ValueError(f"{OI_CHANGE_SETTING} must be OFF or RECORD, not {raw!r}")
    value = (raw or "").strip().upper() or OiChangeMode.OFF.value
    try:
        return OiChangeMode(value)
    except ValueError:
        raise ValueError(f"{OI_CHANGE_SETTING} must be OFF or RECORD, not {raw!r}") from None


def oi_change_mode(environ: Mapping[str, str] | None = None) -> OiChangeMode:
    """`REALTIME_V3_OPTIONS_OI_CHANGE`; unset or blank is OFF. Anything but OFF or RECORD raises."""
    return parse_oi_change_mode((os.environ if environ is None else environ).get(OI_CHANGE_SETTING))


# ---- the arithmetic ---------------------------------------------------------------------------------------------------

def _int(value) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _text(value) -> str:
    try:
        from decimal import Decimal

        return format(Decimal(str(value)).normalize(), "f")
    except Exception:  # noqa: BLE001
        return str(value)


def _at(row, index: int):
    """A row's column by position (tuple rows, or a dict cursor's rows in select order)."""
    return list(row.values())[index] if isinstance(row, Mapping) else row[index]


def classify(today: int | None, previous: int | None, *, matched: bool, large_change: int = LARGE_CHANGE,
             large_fraction: float = LARGE_FRACTION) -> str | None:
    """The contract's flag: OPENED_LARGE, CLOSED_LARGE, NEW (no previous row) or None. A matched contract needs both
    open interests; an unmatched one is NEW when today's open interest alone is large."""
    if today is None:
        return None
    if not matched:
        return "NEW" if today >= large_change else None
    if previous is None:
        return None
    change = today - previous
    if change >= large_change and (previous == 0 or change >= large_fraction * previous):
        return "OPENED_LARGE"
    if previous > 0 and -change >= large_change and -change >= large_fraction * previous:
        return "CLOSED_LARGE"
    return None


def contract_document(row) -> dict:
    """One watched contract's entry from a WATCHED_SQL row."""
    ticker, underlying, kind, expiry, strike = (_at(row, index) for index in range(5))
    today, volume, matched = _int(_at(row, 5)), _int(_at(row, 6)), bool(_at(row, 7))
    previous = _int(_at(row, 8)) if matched else None
    previous_volume = _int(_at(row, 9)) if matched else None
    change = None if today is None or previous is None else today - previous
    return {
        "contract": str(ticker), "underlying": str(underlying).strip().upper(),
        "type": "C" if str(kind).lower() == "call" else "P",
        "expiry": expiry.isoformat() if hasattr(expiry, "isoformat") else str(expiry), "strike": _text(strike),
        "oi": today, "oi_prev": previous, "change": change,
        "change_fraction": None if change is None or not previous else round(change / previous, 4),
        "volume_prev": previous_volume,
        # The share of the previous session's volume that stayed open (above 1: more opened than the day's prints
        # show, e.g. late prints or exercise and assignment).
        "opened_share": None if change is None or change <= 0 or not previous_volume
        else round(change / previous_volume, 3),
        "volume_today": volume,
        "flag": classify(today, previous, matched=matched),
        "matched": matched,
    }


# ---- the recorder -----------------------------------------------------------------------------------------------------

class OiChangeRecorder:
    """Once per session per process; keeps only which session it recorded and how often it tried."""

    def __init__(
        self,
        mode: OiChangeMode | str,
        *,
        emit: Callable[[str, Mapping[str, object]], None] = _print_line,
        fault: Callable[..., object] = emit_fault,
    ) -> None:
        self.mode = parse_oi_change_mode(mode)
        self._emit, self._fault = emit, fault
        self._done: set[date] = set()
        self._attempts: dict[date, int] = {}

    def start(self) -> None:
        if self.mode is OiChangeMode.OFF:
            return
        self._safe_emit(OI_CHANGE_SETTING_EVENT, {
            "mode": self.mode.value, "setting": OI_CHANGE_SETTING, "version": VERSION, "applied": False,
            "large_change": LARGE_CHANGE, "large_fraction": LARGE_FRACTION, "maximum_attempts": MAXIMUM_ATTEMPTS,
        })

    def record_window(
        self,
        provider,
        *,
        session_date: date,
        equity_cutoff: datetime,
        window_id: str | None,
        watched=(),
        breadth: Mapping[str, object] | None = None,
    ) -> dict[str, object] | None:
        """OFF, already recorded this session, out of attempts, or no COMPLETE capture yet: None (nothing read).
        Otherwise the session's record, logged, and a short summary for the cycle result. Never raises."""
        if self.mode is OiChangeMode.OFF or session_date in self._done:
            return None
        if self._attempts.get(session_date, 0) >= MAXIMUM_ATTEMPTS:
            return None
        hint = _breadth_complete(breadth)
        if hint is not None and not hint:
            return None  # no capture this window: wait for one
        self._attempts[session_date] = self._attempts.get(session_date, 0) + 1
        started = time.monotonic()
        cutoff = _moment(equity_cutoff)
        try:
            result = self._record(provider, session_date=session_date, equity_cutoff=cutoff, window_id=window_id,
                                  watched=tuple(sorted({str(item) for item in watched or ()})), started=started)
        except Exception as error:  # noqa: BLE001 - recording never fails the cycle
            reason = _failure_reason(error)
            detail = f"{type(error).__name__}: {error}"[:300]
            attempt = self._attempts[session_date]
            self._safe_emit(OI_CHANGE_FAILED_EVENT, {
                "mode": self.mode.value, "reason": reason, "error": detail, "session": str(session_date),
                "equity_cutoff": None if cutoff is None else cutoff.isoformat(), "window_id": window_id,
                "attempt": attempt, "retry": attempt < MAXIMUM_ATTEMPTS, "applied": False,
            })
            self._safe_fault(
                kind="OPTIONS_OI_CHANGE_RECORD_FAILED", severity="DEGRADED", recoverable=True,
                action="RETRY_NEXT_WINDOW" if attempt < MAXIMUM_ATTEMPTS else "SKIP_SESSION",
                service=FAULT_SERVICE, key=reason,
                message=f"options OI change record failed at {equity_cutoff}: {detail}"[:300],
                context={"reason": reason, "session": str(session_date), "attempt": attempt},
            )
            return {"status": "FAILED", "mode": self.mode.value, "reason": reason, "attempt": attempt,
                    "seconds": round(time.monotonic() - started, 3)}
        self._done.add(session_date)
        return result

    def _record(self, provider, *, session_date, equity_cutoff, window_id, watched, started):
        from .database_access_v2 import APP_ROLE

        head = {"mode": self.mode.value, "session": str(session_date),
                "equity_cutoff": None if equity_cutoff is None else equity_cutoff.isoformat(),
                "window_id": window_id, "version": VERSION, "applied": False, "execution_permission": False}
        rows, chain_rows, top_rows = [], [], []
        with provider.transaction(APP_ROLE, read_only=True) as unit:
            cursor = unit.cursor
            cursor.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
            cursor.execute(PREVIOUS_SQL, (session_date,))
            found = cursor.fetchone()
            previous = None if not found else _at(found, 0)
            if previous is not None:
                if watched:
                    cursor.execute(WATCHED_SQL, (previous, session_date, list(watched)))
                    rows = cursor.fetchall()
                parameters = {"previous": previous, "session": session_date, "change": LARGE_CHANGE,
                              "fraction": LARGE_FRACTION, "top": TOP}
                cursor.execute(CHAIN_SQL, parameters)
                chain_rows = cursor.fetchall()
                cursor.execute(TOP_SQL, parameters)
                top_rows = cursor.fetchall()
        read_seconds = round(time.monotonic() - started, 3)
        if previous is None:
            summary = {**head, "status": "NO_PREVIOUS_SESSION", "previous_session": None, "watched": len(watched),
                       "read_seconds": read_seconds, "seconds": round(time.monotonic() - started, 3)}
            self._emit(OI_CHANGE_SESSION_EVENT, summary)
            return {"status": "NO_PREVIOUS_SESSION", "mode": self.mode.value, "applied": False,
                    "seconds": summary["seconds"]}
        head["previous_session"] = str(previous)
        head["days_back"] = (session_date - previous).days if isinstance(previous, date) else None

        contracts: dict[str, list[dict]] = {}
        for row in rows:
            document = contract_document(row)
            contracts.setdefault(document.pop("underlying"), []).append(document)
        seen = {item["contract"] for items in contracts.values() for item in items}
        not_in_capture = [ticker for ticker in watched if ticker not in seen]

        chain: dict[str, dict] = {}
        for row in chain_rows:
            name = str(_at(row, 0)).strip().upper()
            side = "call" if str(_at(row, 1)).lower() == "call" else "put"
            entry = chain.setdefault(name, {"contracts": 0, "matched": 0})
            entry["contracts"] += int(_at(row, 2))
            entry["matched"] += int(_at(row, 3))
            entry[f"{side}_oi"], entry[f"{side}_oi_prev"] = int(_at(row, 4)), int(_at(row, 5))
            entry[f"{side}_change"] = int(_at(row, 4)) - int(_at(row, 5))
            for index, key in ((6, "opened_large"), (7, "closed_large"), (8, "new_large")):
                entry[key] = entry.get(key, 0) + int(_at(row, index))
        for row in top_rows:
            name = str(_at(row, 0)).strip().upper()
            today, before = int(_at(row, 5)), int(_at(row, 6))
            expiry = _at(row, 3)
            chain.setdefault(name, {"contracts": 0, "matched": 0}).setdefault("top_opened", []).append({
                "contract": str(_at(row, 1)), "type": "C" if str(_at(row, 2)).lower() == "call" else "P",
                "expiry": expiry.isoformat() if hasattr(expiry, "isoformat") else str(expiry),
                "strike": _text(_at(row, 4)), "oi": today, "oi_prev": before, "change": today - before,
            })

        flagged_total = {"OPENED_LARGE": 0, "CLOSED_LARGE": 0, "NEW": 0}
        flagged_names: dict[str, list[str]] = {}
        for name in sorted(set(contracts) | set(chain)):
            items = contracts.get(name, [])
            flags = {"OPENED_LARGE": 0, "CLOSED_LARGE": 0, "NEW": 0}
            for item in items:
                if item["flag"]:
                    flags[item["flag"]] += 1
                    flagged_total[item["flag"]] += 1
                    flagged_names.setdefault(item["flag"], []).append(item["contract"])
            matched = [item for item in items if item["change"] is not None]
            document = {
                **head, "underlying": name, "watched": len(items), "matched": len(matched),
                "flagged": flags,
                "watched_call_change": sum(item["change"] for item in matched if item["type"] == "C"),
                "watched_put_change": sum(item["change"] for item in matched if item["type"] == "P"),
                "contracts": items, "chain": chain.get(name),
            }
            self._emit(OI_CHANGE_RECORD_EVENT, document)
        summary = {
            **head, "status": "RECORDED", "watched": len(watched), "in_capture": len(seen),
            "matched": sum(1 for items in contracts.values() for item in items if item["change"] is not None),
            "not_in_capture": not_in_capture[:LIST_LIMIT], "not_in_capture_count": len(not_in_capture),
            "flagged": flagged_total,
            "flagged_contracts": {key: sorted(value)[:LIST_LIMIT] for key, value in sorted(flagged_names.items())},
            "names": sorted(set(contracts) | set(chain)),
            "chain_contracts": sum(entry.get("contracts", 0) for entry in chain.values()),
            "chain_matched": sum(entry.get("matched", 0) for entry in chain.values()),
            "attempt": self._attempts.get(session_date, 1),
            "read_seconds": read_seconds, "seconds": round(time.monotonic() - started, 3),
        }
        self._emit(OI_CHANGE_SESSION_EVENT, summary)
        return {"status": "RECORDED", "mode": self.mode.value, "applied": False,
                "previous_session": head["previous_session"], "watched": len(watched), "matched": summary["matched"],
                "flagged": sum(flagged_total.values()), "seconds": summary["seconds"]}

    def _safe_emit(self, event, document) -> None:
        try:
            self._emit(event, document)
        except Exception:  # noqa: BLE001
            pass

    def _safe_fault(self, **kwargs) -> None:
        try:
            self._fault(**kwargs)
        except Exception:  # noqa: BLE001
            pass


def start_oi_change(mode: OiChangeMode | str | None = None, **kwargs) -> OiChangeRecorder | None:
    """The scheduler's start: None for OFF (nothing is logged, nothing runs); otherwise the recorder, after its start
    line."""
    mode = oi_change_mode() if mode is None else parse_oi_change_mode(mode)
    if mode is OiChangeMode.OFF:
        return None
    recorder = OiChangeRecorder(mode, **kwargs)
    recorder.start()
    return recorder
