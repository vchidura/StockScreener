"""G106 D11 V-TERM (Nov 18 night): term-structure inversion as a RECORD observation lane on the Options window.

What it observes: per Options underlying of the window, the 7-day and 30-day ATM implied volatility from the A2 IV
summary read (`options_iv_summary_read.iv_summary_from_breadth_cursor`: our IV on the quote midpoint, tenors by total
variance interpolation between listed expiries, flat within 5 days outside them), on the latest chain capture visible at
the window's cutoff. ratio = ATM IV 7d / ATM IV 30d; the name is INVERTED when ratio > 1.3 (front fear above the back
month: NVDA Nov 16 0.948 / 0.640 = 1.48, Nov 12 1.50; SPY Nov 16 1.11). Observation first (matrix D11): nothing reads
these lines, no alert, no write.

`REALTIME_V3_VTERM` on the Options window (`realtime-v3-options-p8-window`):
| OFF (default, unset, blank) | nothing: no SQL, no line (today, byte for byte) |
| RECORD | after the window's run, one REPEATABLE READ READ ONLY transaction (statement timeout 2 s) reads the IV summary
           of each underlying, sorted, until 2 s have passed (the rest: TIME_BUDGET); one `VTERM_RECORD {json}` line per
           window. Any failure is one `VTERM_RECORD_FAILED {json}` line; it never raises into the window |
Anything else and the window service refuses to start (never silently OFF).

Per name: atm_iv_7d / atm_iv_30d (and their methods: TOTAL_VARIANCE_INTERPOLATION, EXACT_EXPIRY, FLAT_EXTRAPOLATION),
ratio, inverted, the capture's observed_at and its age at the cutoff, the A2 quality reasons of the two tenors. A name
without a usable read says why (NO_CHAIN_CAPTURE, SPOT_UNAVAILABLE, NO_PRICED_CONTRACTS, TENOR_UNAVAILABLE:7|30,
READ:<error>, TIME_BUDGET). A flat-extrapolated tenor still gives a ratio; its method is named, so a reader can drop it.
"""
from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter
from datetime import date, datetime
from enum import Enum
from typing import Callable, Mapping, Sequence

VTERM_SETTING = "REALTIME_V3_VTERM"
VTERM_RECORD_EVENT = "VTERM_RECORD"
VTERM_RECORD_FAILED_EVENT = "VTERM_RECORD_FAILED"
VTERM_CONTRACT = "vterm-record-v1"
VTERM_INVERSION_RATIO = 1.3
FRONT_TENOR_DAYS = 7
BACK_TENOR_DAYS = 30
READ_BUDGET_SECONDS = 2.0
STATEMENT_TIMEOUT_MS = 2000
LINE_BYTE_LIMIT = 15000


class VTermMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"


def vterm_mode(environ: Mapping[str, str] | None = None) -> VTermMode:
    """`REALTIME_V3_VTERM`; unset or blank is OFF. Anything but OFF or RECORD raises."""
    raw = (os.environ if environ is None else environ).get(VTERM_SETTING)
    value = (raw or "").strip().upper() or VTermMode.OFF.value
    try:
        return VTermMode(value)
    except ValueError:
        raise ValueError(f"{VTERM_SETTING} must be OFF or RECORD, not {raw!r}") from None


def prepare_vterm(environ: Mapping[str, str] | None = None) -> VTermMode:
    """The window service's start check (raises when invalid); RECORD loads the IV summary's modules here."""
    mode = vterm_mode(environ)
    if mode is VTermMode.RECORD:
        try:
            from options.analytics import chain_analysis  # noqa: F401

            from . import options_iv_summary_read  # noqa: F401
        except Exception:  # noqa: BLE001 - the first window then says why
            pass
    return mode


def _tenor(summary: Mapping[str, object], days: int) -> Mapping[str, object] | None:
    for row in summary.get("tenors") or ():
        if isinstance(row, Mapping) and row.get("days") == days:
            return row
    return None


def term_entry(name: str, summary: Mapping[str, object] | None, *, cutoff: datetime,
               threshold: float = VTERM_INVERSION_RATIO) -> dict:
    """One name's entry from its IV summary (pure)."""
    if not isinstance(summary, Mapping):
        return {"underlyer": name, "status": "NO_SUMMARY", "inverted": False}
    if not summary.get("available"):
        return {"underlyer": name, "status": str(summary.get("reason") or "UNAVAILABLE"), "inverted": False}
    front, back = _tenor(summary, FRONT_TENOR_DAYS), _tenor(summary, BACK_TENOR_DAYS)
    front_iv = None if front is None else front.get("atm_iv")
    back_iv = None if back is None else back.get("atm_iv")
    observed = summary.get("observed_at")
    age = None
    try:
        age = round((cutoff - datetime.fromisoformat(str(observed))).total_seconds(), 1)
    except (TypeError, ValueError):
        pass
    entry = {
        "underlyer": name,
        "observed_at": observed,
        "capture_age_s": age,
        "atm_iv_7d": front_iv,
        "atm_iv_30d": back_iv,
        "method_7d": None if front is None else front.get("atm_method"),
        "method_30d": None if back is None else back.get("atm_method"),
        "quality_7d": [] if front is None else list(front.get("quality_reasons") or ()),
        "quality_30d": [] if back is None else list(back.get("quality_reasons") or ()),
        "contracts": summary.get("contracts"),
    }
    if front_iv is None or back_iv is None or not float(back_iv) > 0:
        missing = [str(days) for days, value in ((FRONT_TENOR_DAYS, front_iv), (BACK_TENOR_DAYS, back_iv))
                   if value is None or not float(value) > 0]
        return {**entry, "status": "TENOR_UNAVAILABLE:" + "|".join(missing or ["30"]), "ratio": None,
                "inverted": False}
    ratio = float(front_iv) / float(back_iv)
    return {**entry, "status": "OK", "ratio": round(ratio, 4), "inverted": ratio > threshold}


def read_term_summaries_cursor(cursor, *, session_date: date, cutoff: datetime, underlyings: Sequence[str],
                               read: Callable[..., dict] | None = None,
                               clock: Callable[[], float] = time.monotonic,
                               budget_seconds: float = READ_BUDGET_SECONDS) -> dict[str, dict | str]:
    """Per name (sorted, de-duplicated): its IV summary, or the reason it has none. One name's failure is that name's
    only (savepoint); names left when the budget is spent say TIME_BUDGET."""
    if read is None:
        from .options_iv_summary_read import iv_summary_from_breadth_cursor as read
    names = sorted({str(name).strip().upper() for name in underlyings if name and str(name).strip()})
    started = clock()
    result: dict[str, dict | str] = {}
    for name in names:
        remaining = budget_seconds - (clock() - started)
        if remaining <= 0:
            result[name] = "TIME_BUDGET"
            continue
        # Each statement of this name may use what is left of the budget (at least 100 ms, at most 2 s).
        timeout_ms = int(max(100, min(STATEMENT_TIMEOUT_MS, remaining * 1000)))
        cursor.execute(f"SET LOCAL statement_timeout = '{timeout_ms}ms'")
        cursor.execute("SAVEPOINT vterm_name")
        try:
            result[name] = read(cursor, underlyer=name, available_by=cutoff, session_date=session_date)
            cursor.execute("RELEASE SAVEPOINT vterm_name")
        except Exception as error:  # noqa: BLE001 - this name only
            cursor.execute("ROLLBACK TO SAVEPOINT vterm_name")
            result[name] = f"READ:{type(error).__name__}"[:80]
    return result


def vterm_document(summaries: Mapping[str, dict | str], *, session_date: date, cutoff: datetime,
                   window_id: str | None, read_seconds: float, seconds: float,
                   threshold: float = VTERM_INVERSION_RATIO) -> dict:
    """The window's VTERM_RECORD document (pure)."""
    names = []
    for name in sorted(summaries):
        value = summaries[name]
        if isinstance(value, str):
            names.append({"underlyer": name, "status": value, "inverted": False})
        else:
            names.append(term_entry(name, value, cutoff=cutoff, threshold=threshold))
    reasons = Counter(entry["status"] for entry in names if entry["status"] != "OK")
    ratios = [entry for entry in names if entry.get("ratio") is not None]
    return {
        "contract": VTERM_CONTRACT, "mode": VTermMode.RECORD.value, "applied": False, "execution_permission": False,
        "session": str(session_date), "cutoff": cutoff.isoformat(), "window_id": window_id,
        "front_days": FRONT_TENOR_DAYS, "back_days": BACK_TENOR_DAYS, "threshold": threshold,
        "underlyings": len(names), "available": len(ratios),
        "inverted": sorted(entry["underlyer"] for entry in names if entry.get("inverted")),
        "max_ratio": None if not ratios else max(entry["ratio"] for entry in ratios),
        "unavailable": dict(sorted(reasons.items())),
        "budget_exhausted": "TIME_BUDGET" in reasons,
        "read_seconds": round(read_seconds, 3), "seconds": round(seconds, 3),
        "names": names,
    }


def render_vterm_line(event: str, document: Mapping[str, object], limit: int = LINE_BYTE_LIMIT) -> str:
    """`EVENT {json}` under `limit` bytes: the per-name list is cut (marked `truncated`) until it fits."""
    text = json.dumps(document, sort_keys=True, default=str, separators=(",", ":"))
    if len(event) + 1 + len(text.encode("utf-8")) <= limit:
        return f"{event} {text}"
    trimmed = dict(document)
    names = list(trimmed.get("names") or [])
    while names:
        names = names[: len(names) // 2]
        trimmed["names"] = names
        trimmed["truncated"] = True
        text = json.dumps(trimmed, sort_keys=True, default=str, separators=(",", ":"))
        if len(event) + 1 + len(text.encode("utf-8")) <= limit:
            return f"{event} {text}"
    trimmed.pop("names", None)
    trimmed["truncated"] = True
    return f"{event} {json.dumps(trimmed, sort_keys=True, default=str, separators=(',', ':'))}"


def _emit(event: str, document: Mapping[str, object]) -> None:
    print(render_vterm_line(event, document), file=sys.stderr, flush=True)


def record_vterm(provider, *, session_date: date, cutoff: datetime, underlyings: Sequence[str],
                 window_id: str | None = None, emit: Callable[[str, Mapping], None] = _emit,
                 read: Callable[..., dict] | None = None,
                 clock: Callable[[], float] = time.monotonic) -> dict | None:
    """After the window's run (`build_and_run_p8_serial_window`). OFF: None and nothing else. Never raises."""
    started = clock()
    try:
        if vterm_mode() is VTermMode.OFF:
            return None
    except Exception:  # noqa: BLE001 - the service refuses to start on an invalid value; never fail a window here
        return None
    head = {"contract": VTERM_CONTRACT, "mode": VTermMode.RECORD.value, "applied": False,
            "session": str(session_date), "cutoff": cutoff.isoformat() if cutoff is not None else None,
            "window_id": window_id}
    try:
        from .database_access_v2 import APP_ROLE

        read_started = clock()
        with provider.transaction(APP_ROLE, read_only=True, isolation_level="REPEATABLE READ") as unit:
            summaries = read_term_summaries_cursor(
                unit.cursor, session_date=session_date, cutoff=cutoff, underlyings=underlyings, read=read,
                clock=clock)
        read_seconds = clock() - read_started
        document = vterm_document(summaries, session_date=session_date, cutoff=cutoff, window_id=window_id,
                                  read_seconds=read_seconds, seconds=clock() - started)
        emit(VTERM_RECORD_EVENT, document)
        return {"status": "RECORDED", "available": document["available"], "inverted": document["inverted"]}
    except Exception as error:  # noqa: BLE001 - recording never fails the window
        try:
            emit(VTERM_RECORD_FAILED_EVENT, {**head, "status": "RECORD_FAILED",
                                             "reason": f"{type(error).__name__}: {error}"[:200],
                                             "seconds": round(clock() - started, 3)})
        except Exception:  # noqa: BLE001
            pass
        return {"status": "RECORD_FAILED"}


__all__ = [
    "VTERM_INVERSION_RATIO", "VTERM_RECORD_EVENT", "VTERM_RECORD_FAILED_EVENT", "VTERM_SETTING", "VTermMode",
    "prepare_vterm", "read_term_summaries_cursor", "record_vterm", "render_vterm_line", "term_entry",
    "vterm_document", "vterm_mode",
]
