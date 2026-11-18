"""G106 levels and context (A3 / D15 / OV1 follow-up, Nov 18 2026): dealer gamma and delta levels per name per Options
window, RECORD only, in the Options scheduler.

After each window's chain capture (`_capture_breadth`), the recorder reads the window's COMPLETE captures from
`rt.option_breadth_current` and logs, per name, the levels the Options Overview page (OV1, `/api/options/exposure`)
shows for that capture in its default view (weight = prior-settlement open interest, scope = every captured expiry):
spot, net dealer gamma (GEX, dollars per 1% move) and its sign, the gamma flip, call wall, put wall, peak and largest
gamma strikes, net dealer delta (DEX) with its call and put parts, the share of gamma expiring today and the strike
holding most of it, distances from spot in percent, row counts and the page's quality reasons. The numbers come from
the page's own functions (`options_exposure_read.build_exposure_chain` + `compute_exposure`, same row columns, same
latest-capture rule), so a line equals the page for the same capture. No detector reads them yet: they are the evidence
the regime / wall / flip / pin rules of the levels study need (`docs/g106`, D15), recorded live without look-ahead.

`REALTIME_V3_OPTIONS_LEVELS`, read by the Options scheduler (`realtime-v3-options-scheduler`,
`scripts/run_advanced_options_v3_scheduler.py`) at start:
| OFF (default, unset, blank) | nothing: no SQL, no log line, no key in the cycle result (byte-identical) |
| RECORD | after each window's chain capture: one short read-only transaction (statement timeout 20 s) on the pool's
           second connection; `OPTIONS_LEVELS_RECORD` per name and one `OPTIONS_LEVELS_RECORD_WINDOW` line with timings;
           nothing is written, nothing is subscribed, no detector reads it |
| ON | accepted but behaves as RECORD (no consumer exists yet): `OPTIONS_LEVELS_ON_UNAVAILABLE {"reason":"NO_CONSUMER"}`
       and one DEGRADED fault at start |
Anything else (any case accepted for the three) and the scheduler refuses to start.

Dealer side is not observable: the page's convention DEALER_LONG_CALLS_SHORT_PUTS is a stated hypothesis. Under it
both calls and puts add positive dealer delta, so net DEX is never negative; its call / put split (`dex_tilt`) is the
part that moves. Recording never fails the cycle: any error logs `OPTIONS_LEVELS_RECORD_FAILED {reason}` and one
DEGRADED fault; one name's failure is that name's line only.
"""
from __future__ import annotations

import json
import math
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Callable, Mapping

from .fault_journal import emit_fault

UTC = timezone.utc

LEVELS_SETTING = "REALTIME_V3_OPTIONS_LEVELS"
LEVELS_SETTING_EVENT = "OPTIONS_LEVELS_SETTING"
LEVELS_RECORD_EVENT = "OPTIONS_LEVELS_RECORD"
LEVELS_WINDOW_EVENT = "OPTIONS_LEVELS_RECORD_WINDOW"
LEVELS_RECORD_FAILED_EVENT = "OPTIONS_LEVELS_RECORD_FAILED"
LEVELS_ON_UNAVAILABLE_EVENT = "OPTIONS_LEVELS_ON_UNAVAILABLE"
FAULT_SERVICE = "options-scheduler"
VERSION = "options-levels-record-v1"
# The page's default view (`/api/options/exposure` greek=gamma|delta, weight=oi, scope=all).
WEIGHT = "oi"
SCOPE = "all"
TOP_STRIKES = 3
# Docker splits a log line over 16 KiB.
LINE_BYTE_LIMIT = 15_000
STATEMENT_TIMEOUT = "20s"

# Same as the AL10 recorder: the newest capture per name of this window decides whether it is COMPLETE.
CAPTURES_SQL = (
    "SELECT underlying,state,observed_at FROM rt.option_breadth_captures "
    "WHERE session_date=%s AND equity_cutoff=%s ORDER BY underlying,observed_at DESC"
)
# The page's two reads (`exposure_from_breadth_cursor`): capture groups (rows left by earlier captures are counted, not
# used: R55), then the capture's rows.
GROUPS_SQL = (
    "SELECT observed_at,count(*) FROM rt.option_breadth_current WHERE session_date=%s AND underlying=%s "
    "AND observed_at<=%s GROUP BY observed_at ORDER BY observed_at DESC"
)


def _rows_sql() -> str:
    from .options_exposure_read import _ROW_NAMES

    return ("SELECT " + ",".join(_ROW_NAMES) + " FROM rt.option_breadth_current "
            "WHERE session_date=%s AND underlying=%s AND observed_at=%s")


# ---- the switch -------------------------------------------------------------------------------------------------------

class LevelsMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"
    ON = "ON"


def options_levels_mode(environ: Mapping[str, str] | None = None) -> LevelsMode:
    """`REALTIME_V3_OPTIONS_LEVELS`; unset or blank is OFF. Anything but OFF, RECORD or ON raises."""
    return parse_options_levels_mode((os.environ if environ is None else environ).get(LEVELS_SETTING))


def parse_options_levels_mode(raw: object) -> LevelsMode:
    if isinstance(raw, LevelsMode):
        return raw
    if raw is not None and not isinstance(raw, str):
        raise ValueError(f"{LEVELS_SETTING} must be OFF, RECORD or ON, not {raw!r}")
    value = (raw or "").strip().upper() or LevelsMode.OFF.value
    try:
        return LevelsMode(value)
    except ValueError:
        raise ValueError(f"{LEVELS_SETTING} must be OFF, RECORD or ON, not {raw!r}") from None


# ---- log lines --------------------------------------------------------------------------------------------------------

def _dumps(document: Mapping[str, object]) -> str:
    return json.dumps(document, sort_keys=True, default=str, separators=(",", ":"))


def render_line(event: str, document: Mapping[str, object], limit: int = LINE_BYTE_LIMIT) -> str:
    """`EVENT {json}` under `limit` bytes: the longest lists are halved (marked `truncated`) until it fits; at worst only
    the scalar fields stay."""
    text = _dumps(document)
    if len(event) + 1 + len(text.encode("utf-8")) <= limit:
        return f"{event} {text}"
    trimmed = dict(document)
    trimmed["truncated"] = True
    while True:
        text = _dumps(trimmed)
        if len(event) + 1 + len(text.encode("utf-8")) <= limit:
            return f"{event} {text}"
        lists = [(len(_dumps({"v": value})), key) for key, value in trimmed.items()
                 if isinstance(value, list) and value]
        if not lists:
            break
        _size, key = max(lists)
        trimmed[key] = trimmed[key][: len(trimmed[key]) // 2]
    scalars = {key: value for key, value in trimmed.items()
               if isinstance(value, (int, float, bool)) or value is None or (isinstance(value, str) and len(value) < 200)}
    text = _dumps(scalars)
    if len(event) + 1 + len(text.encode("utf-8")) > limit:
        text = _dumps({key: scalars.get(key) for key in ("mode", "session", "equity_cutoff", "underlying")}
                      | {"truncated": True})
    return f"{event} {text}"


def _print_line(event: str, document: Mapping[str, object]) -> None:
    print(render_line(event, document), file=sys.stderr, flush=True)


# ---- helpers ----------------------------------------------------------------------------------------------------------

def _value(row, index: int, name: str):
    return row[name] if isinstance(row, Mapping) else row[index]


def _moment(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _iso(value) -> str | None:
    moment = _moment(value)
    return None if moment is None else moment.isoformat()


def _pct(level, spot: float) -> float | None:
    """Distance of a level from spot, in percent of spot (positive = above spot)."""
    if level is None or not spot:
        return None
    try:
        number = float(level)
    except (TypeError, ValueError):
        return None
    return round((number - spot) / spot * 100.0, 3) if math.isfinite(number) else None


def _sign(value: float | int | None) -> str | None:
    if value is None:
        return None
    return "POSITIVE" if value > 0 else "NEGATIVE" if value < 0 else "ZERO"


def _failure_reason(error: BaseException) -> str:
    name = type(error).__name__
    if name == "PoolError" or "pool exhausted" in str(error).lower():
        return "NO_CONNECTION"
    return name[:60]


def _breadth_complete(breadth) -> set[str] | None:
    """The names the capture summary says are COMPLETE this window; None when it does not say (the database decides)."""
    if not isinstance(breadth, Mapping):
        return None
    if isinstance(breadth.get("status"), str):
        return set()  # FAILED / NO_UNDERLYINGS / AFTER_ACQUISITION_STOP: no capture this window
    return {str(name) for name, value in breadth.items()
            if isinstance(value, Mapping) and value.get("state") == "COMPLETE"}


# ---- the levels of one capture (the page's computation) ---------------------------------------------------------------

def _zero_dte_peak(gamma: Mapping[str, object], spot: float) -> dict | None:
    """The strike holding most of the gamma that expires today (calls + puts, gross), from the page's strike x expiry
    grid of the same result."""
    today = {index for index, item in enumerate(gamma.get("expiries") or ()) if item.get("calendar_dte") == 0}
    if not today:
        return None
    gross: dict[int, float] = {}
    net: dict[int, float] = {}
    for strike_index, expiry_index, call, put in gamma.get("grid") or ():
        if expiry_index in today:
            gross[strike_index] = gross.get(strike_index, 0.0) + abs(call) + abs(put)
            net[strike_index] = net.get(strike_index, 0.0) + call + put
    total = sum(gross.values())
    if not total:
        return None
    best = max(gross, key=lambda index: (gross[index], -index))
    strike = gamma["strikes"][best]["strike"]
    return {"strike": strike, "share_of_0dte": round(gross[best] / total, 4), "net": int(round(net[best])),
            "pct": _pct(strike, spot)}


def levels_document(chain) -> dict:
    """One capture's levels: the OV1 page's gamma and delta views (weight oi, scope all) reduced to the record fields.
    Pure; raises only on a programming error (the caller turns that into the name's failed line)."""
    import numpy as np

    from .options_exposure_read import CONVENTION, compute_exposure

    with np.errstate(all="ignore"):
        gamma = compute_exposure(chain, greek="gamma", weight=WEIGHT, scope=SCOPE)
        delta = compute_exposure(chain, greek="delta", weight=WEIGHT, scope=SCOPE)
    spot = float(chain.spot)
    coverage = gamma.get("coverage") or {}
    skipped = coverage.get("skipped") or {}
    gap = (coverage.get("spot_check") or {}).get("gap_fraction")
    document: dict[str, object] = {
        "observed_at": _iso(chain.observed_at), "spot": gamma.get("spot"),
        "quote_underlying_price": (coverage.get("spot_check") or {}).get("quote_underlying_price"),
        "spot_quote_gap_bp": None if gap is None else round(gap * 10_000, 1),
        "convention": CONVENTION, "weight": WEIGHT, "scope": SCOPE,
        "rows": {
            "capture": coverage.get("capture_contracts", chain.size),
            "contributing": coverage.get("contributing_contracts"),
            "earlier_excluded": coverage.get("earlier_capture_rows_excluded", chain.earlier_capture_rows),
            "zero_weight": skipped.get("zero_weight"), "expired": skipped.get("expired"),
            "no_greek": skipped.get("no_greek"), "provider_iv": coverage.get("provider_iv_contracts"),
            "weight_coverage": coverage.get("weight_coverage_fraction"),
            "expiries": coverage.get("expiry_count"),
        },
    }
    if not gamma.get("available"):
        reasons = list(gamma.get("quality_reasons") or [])
        return {**document, "available": False, "reason": gamma.get("reason"), "quality": reasons}
    totals, levels = gamma["totals"], gamma["levels"]
    flip = levels.get("flip_spot")
    call_wall, put_wall = levels.get("call_wall"), levels.get("put_wall")
    flip_value = None if flip is None else float(flip)
    document.update({
        "available": True, "reason": None,
        "gex": totals["net"], "gex_sign": _sign(totals["net"]),
        "gex_call": totals["call"], "gex_put": totals["put"], "gex_abs": totals["absolute"],
        "gex_net_share": round(totals["net"] / totals["absolute"], 4) if totals["absolute"] else None,
        "gex_shares": totals["net_shares"],
        "regime_at_spot": levels.get("regime_at_spot"),
        "flip": flip, "flip_pct": _pct(flip, spot),
        "spot_vs_flip": None if flip_value is None else "ABOVE" if spot > flip_value
        else "BELOW" if spot < flip_value else "AT",
        "flip_crossings": (levels.get("flip_search") or {}).get("crossings"),
        "call_wall": None if call_wall is None else call_wall["strike"],
        "call_wall_pct": None if call_wall is None else _pct(call_wall["strike"], spot),
        "call_wall_gex": None if call_wall is None else call_wall["gamma_notional_per_percent"],
        "put_wall": None if put_wall is None else put_wall["strike"],
        "put_wall_pct": None if put_wall is None else _pct(put_wall["strike"], spot),
        "put_wall_gex": None if put_wall is None else put_wall["gamma_notional_per_percent"],
        "peak_gamma_strike": levels.get("peak_gamma_strike"),
        "peak_gamma_pct": _pct(levels.get("peak_gamma_strike"), spot),
        "top_gamma": [
            {"strike": item["strike"], "net": item["net"], "share": item["share"],
             "pct": _pct(item["strike"], spot), "tags": item["tags"]}
            for item in (gamma.get("largest_levels") or [])[:TOP_STRIKES]
        ],
        "zero_dte_share": gamma.get("expiring_today_share"),
        "zero_dte_peak": _zero_dte_peak(gamma, spot),
    })
    if delta.get("available"):
        call, put = delta["totals"]["call"], delta["totals"]["put"]
        gross = abs(call) + abs(put)
        document.update({
            "dex": delta["totals"]["net"], "dex_call": call, "dex_put": put,
            "dex_shares": delta["totals"]["net_shares"],
            # Call-heavy > 0, put-heavy < 0; the part of DEX the convention does not fix (net DEX is never negative).
            "dex_tilt": round((abs(call) - abs(put)) / gross, 4) if gross else None,
        })
    else:
        document.update({"dex": None, "dex_call": None, "dex_put": None, "dex_shares": None, "dex_tilt": None,
                         "dex_reason": delta.get("reason")})
    reasons = list(gamma.get("quality_reasons") or [])
    reasons.extend(item for item in (delta.get("quality_reasons") or []) if item not in reasons)
    document["quality"] = reasons
    return document


# ---- the window's read (one short read-only transaction) --------------------------------------------------------------

@dataclass
class LevelsRead:
    captures: dict
    complete: dict
    chains: dict = field(default_factory=dict)
    unavailable: dict = field(default_factory=dict)
    errors: dict = field(default_factory=dict)
    rows: int = 0
    seconds: float = 0.0
    state: str = "READ"


def read_levels_cursor(cursor, *, session_date: date, equity_cutoff: datetime,
                       build: Callable[..., object] | None = None) -> LevelsRead:
    """The window's captures, then per COMPLETE name the page's two reads and its chain (built at once, so only the
    compact arrays are kept). One name's failed read rolls back to its savepoint and is that name's error only."""
    if build is None:
        from .options_exposure_read import build_exposure_chain as build
    started = time.monotonic()
    cursor.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
    cursor.execute(CAPTURES_SQL, (session_date, equity_cutoff))
    captures: dict[str, tuple[str, datetime | None]] = {}
    for row in cursor.fetchall():
        underlying = str(_value(row, 0, "underlying")).strip().upper()
        if underlying not in captures:  # newest first
            captures[underlying] = (str(_value(row, 1, "state")), _moment(_value(row, 2, "observed_at")))
    complete = {name: observed for name, (state, observed) in captures.items()
                if state == "COMPLETE" and observed is not None}
    read = LevelsRead(captures=captures, complete=complete)
    rows_sql = _rows_sql()
    from .options_exposure_read import _ROW_NAMES

    for name in sorted(complete):
        observed = complete[name]
        cursor.execute("SAVEPOINT levels_name")
        try:
            cursor.execute(GROUPS_SQL, (session_date, name, observed))
            groups = [(_moment(_value(item, 0, "observed_at")), int(_value(item, 1, "count")))
                      for item in cursor.fetchall()]
            if not groups or groups[0][0] != observed:
                read.unavailable[name] = "CAPTURE_ROWS_MISSING"
                cursor.execute("RELEASE SAVEPOINT levels_name")
                continue
            earlier = sum(count for _moment_value, count in groups[1:])
            cursor.execute(rows_sql, (session_date, name, observed))
            rows = [dict(item) if isinstance(item, Mapping) else dict(zip(_ROW_NAMES, item))
                    for item in cursor.fetchall()]
            cursor.execute("RELEASE SAVEPOINT levels_name")
        except Exception as error:  # noqa: BLE001 - this name only
            cursor.execute("ROLLBACK TO SAVEPOINT levels_name")
            read.errors[name] = f"READ: {type(error).__name__}: {error}"[:200]
            continue
        read.rows += len(rows)
        try:
            chain = build(rows, underlyer=name, session_date=session_date, observed_at=observed,
                          earlier_capture_rows=earlier)
        except Exception as error:  # noqa: BLE001 - this name only
            read.errors[name] = f"BUILD: {type(error).__name__}: {error}"[:200]
            continue
        if chain is None:
            read.unavailable[name] = "SPOT_UNAVAILABLE"
        else:
            read.chains[name] = chain
    read.seconds = round(time.monotonic() - started, 3)
    return read


# ---- the recorder -----------------------------------------------------------------------------------------------------

class OptionsLevelsRecorder:
    """The scheduler's levels recorder: one per process; it keeps no state between windows."""

    def __init__(
        self,
        mode: LevelsMode | str,
        *,
        emit: Callable[[str, Mapping[str, object]], None] = _print_line,
        fault: Callable[..., object] = emit_fault,
        levels: Callable[[object], dict] = levels_document,
    ) -> None:
        self.mode = parse_options_levels_mode(mode)
        self._emit, self._fault, self._levels = emit, fault, levels

    def start(self) -> None:
        """The start lines (RECORD and ON only). Never raises."""
        if self.mode is LevelsMode.OFF:
            return
        try:
            # The page's modules load here, not in the first window (~3.5 s measured on the r135 image).
            from options.analytics import gamma_exposure  # noqa: F401
            from . import options_exposure_read  # noqa: F401
        except Exception:  # noqa: BLE001 - the window then fails alone and says why
            pass
        self._safe_emit(LEVELS_SETTING_EVENT, {
            "mode": self.mode.value, "setting": LEVELS_SETTING, "version": VERSION, "weight": WEIGHT,
            "scope": SCOPE, "applied": False,
        })
        if self.mode is LevelsMode.ON:
            self._safe_emit(LEVELS_ON_UNAVAILABLE_EVENT, {
                "reason": "NO_CONSUMER", "mode": self.mode.value, "behaves_as": "RECORD",
            })
            self._safe_fault(
                kind="OPTIONS_LEVELS_ON_UNAVAILABLE", severity="DEGRADED", recoverable=True, action="RECORD_ONLY",
                service=FAULT_SERVICE, key="NO_CONSUMER",
                message=f"{LEVELS_SETTING}=ON has no consumer yet; recording only",
                context={"mode": self.mode.value, "version": VERSION},
            )

    def record_window(
        self,
        provider,
        *,
        session_date: date,
        equity_cutoff: datetime,
        window_id: str | None,
        breadth: Mapping[str, object] | None = None,
    ) -> dict[str, object] | None:
        """OFF: None and nothing else. RECORD / ON: the window's levels, logged; a short summary for the cycle result.
        Never raises."""
        if self.mode is LevelsMode.OFF:
            return None
        started = time.monotonic()
        try:
            return self._record(provider, session_date=session_date, equity_cutoff=_moment(equity_cutoff),
                                window_id=window_id, breadth=breadth, started=started)
        except Exception as error:  # noqa: BLE001 - recording never fails the cycle
            reason = _failure_reason(error)
            detail = f"{type(error).__name__}: {error}"[:300]
            self._failed(reason, detail, session_date=session_date, equity_cutoff=equity_cutoff, window_id=window_id)
            return {"status": "FAILED", "mode": self.mode.value, "reason": reason,
                    "seconds": round(time.monotonic() - started, 3)}

    def _record(self, provider, *, session_date, equity_cutoff, window_id, breadth, started):
        from .database_access_v2 import APP_ROLE

        hint = _breadth_complete(breadth)
        if hint is not None and not hint:
            # The capture summary already says no name is COMPLETE this window: nothing to read.
            read = LevelsRead(captures={}, complete={}, state="SKIPPED_NO_COMPLETE_CAPTURE")
        else:
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                read = read_levels_cursor(unit.cursor, session_date=session_date, equity_cutoff=equity_cutoff)
        head = {"mode": self.mode.value, "session": str(session_date),
                "equity_cutoff": None if equity_cutoff is None else equity_cutoff.isoformat(),
                "window_id": window_id, "applied": False, "execution_permission": False}
        compute_started = time.monotonic()
        recorded, unavailable, failed = [], dict(read.unavailable), dict(read.errors)
        regimes: dict[str, list[str]] = {}
        for name in sorted(read.chains):
            name_started = time.monotonic()
            try:
                document = self._levels(read.chains[name])
            except Exception as error:  # noqa: BLE001 - this name only
                failed[name] = f"COMPUTE: {type(error).__name__}: {error}"[:200]
                continue
            document = {**head, "underlying": name, **document,
                        "ms": round((time.monotonic() - name_started) * 1000.0, 1)}
            if document.get("available"):
                recorded.append(name)
                regimes.setdefault(str(document.get("gex_sign")), []).append(name)
            else:
                unavailable[name] = str(document.get("reason"))
            self._emit(LEVELS_RECORD_EVENT, document)
        for name, reason in sorted(read.unavailable.items()):
            self._emit(LEVELS_RECORD_EVENT, {**head, "underlying": name, "available": False, "reason": reason})
        for name, reason in sorted(failed.items()):
            self._emit(LEVELS_RECORD_EVENT, {**head, "underlying": name, "available": False, "reason": "FAILED",
                                             "error": reason})
        compute_seconds = round(time.monotonic() - compute_started, 3)
        not_complete = {name: state for name, (state, _observed) in sorted(read.captures.items())
                        if name not in read.complete}
        summary = {
            **head, "version": VERSION, "read": read.state,
            "captures": len(read.captures), "complete": len(read.complete), "recorded": len(recorded),
            "not_complete": not_complete, "unavailable": unavailable, "failed": sorted(failed),
            "regimes": {key: sorted(value) for key, value in sorted(regimes.items())},
            "rows": read.rows, "read_seconds": read.seconds, "compute_seconds": compute_seconds,
            "seconds": round(time.monotonic() - started, 3),
        }
        self._emit(LEVELS_WINDOW_EVENT, summary)
        if failed:
            self._safe_fault(
                kind="OPTIONS_LEVELS_NAME_FAILED", severity="DEGRADED", recoverable=True, action="SKIP_NAME",
                service=FAULT_SERVICE, key="names",
                message=f"options levels failed for {sorted(failed)} at {head['equity_cutoff']}"[:300],
                context={"names": sorted(failed), "equity_cutoff": head["equity_cutoff"]},
            )
        return {"status": "RECORDED", "mode": self.mode.value, "applied": False, "complete": len(read.complete),
                "recorded": len(recorded), "failed": len(failed), "seconds": summary["seconds"]}

    def _failed(self, reason, detail, *, session_date, equity_cutoff, window_id) -> None:
        cutoff = _moment(equity_cutoff)
        self._safe_emit(LEVELS_RECORD_FAILED_EVENT, {
            "mode": self.mode.value, "reason": reason, "error": detail, "session": str(session_date),
            "equity_cutoff": None if cutoff is None else cutoff.isoformat(), "window_id": window_id,
            "applied": False,
        })
        self._safe_fault(
            kind="OPTIONS_LEVELS_RECORD_FAILED", severity="DEGRADED", recoverable=True, action="SKIP_WINDOW",
            service=FAULT_SERVICE, key=reason,
            message=f"options levels record failed at {equity_cutoff}: {detail}"[:300],
            context={"reason": reason, "session": str(session_date), "equity_cutoff": str(equity_cutoff)},
        )

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


def start_options_levels(mode: LevelsMode | str | None = None, **kwargs) -> OptionsLevelsRecorder | None:
    """The scheduler's start: None for OFF (nothing is logged, nothing runs); otherwise the recorder, after its start
    lines."""
    mode = options_levels_mode() if mode is None else parse_options_levels_mode(mode)
    if mode is LevelsMode.OFF:
        return None
    recorder = OptionsLevelsRecorder(mode, **kwargs)
    recorder.start()
    return recorder
