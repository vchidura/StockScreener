"""G106 D16 R-REGIME (Nov 19 2026): a market regime label per Options window, RECORD only, from the levels the levels
recorder just computed and the A2 IV summary of the index ETFs.

No index entitlement (C1: VIX/VVIX/SPX answer 403), so the regime is read from the chains we capture (matrix D16):
- dealer gamma: the sign of net dealer gamma (GEX) of SPY and QQQ, and how many of the window's names are in negative
  gamma (the levels lines' `gex_sign`);
- the flip: SPY's distance from its gamma flip (`flip_pct`), NEAR_FLIP within `NEAR_FLIP_PCT` percent;
- the term structure: SPY's (and QQQ's) ATM IV 7 days / 30 days from the A2 IV summary (`iv_summary_from_breadth_cursor`,
  the capture the levels used): INVERTED at `INVERTED_RATIO` or more, FLAT from `FLAT_RATIO`, else CONTANGO; the 30-day
  25-delta risk reversal is recorded beside it;
- put/call: the day's put / call volume and open interest over every name's captured chain.
The label (`regime`), in this order: UNKNOWN (no SPY gamma), TRANSITION (SPY near its flip), STRESS (SPY and QQQ in
negative gamma, SPY term inverted), FRAGILE (negative gamma otherwise, or mixed gamma with an inverted term), CALM (SPY and
QQQ in positive gamma, SPY term in contango), else NEUTRAL. The thresholds are a first guess to be judged on the recorded
lines (observation first); nothing reads the label.

`REALTIME_V3_REGIME`, read by the Options scheduler at start:
| OFF (default, unset, blank) | nothing: no line, no SQL; the levels lines and the cycle result are byte-identical |
| RECORD | with `REALTIME_V3_OPTIONS_LEVELS` RECORD or ON: after each window's levels, one short read-only transaction
           (statement timeout 10 s) for the IV summaries of `IV_NAMES`, and one `OPTIONS_REGIME_RECORD` line; nothing is
           written or applied. With the levels switch OFF: one start line saying it is inactive, nothing else |
Anything else (any case accepted for the two) and the scheduler refuses to start. A failed IV read leaves the term
UNKNOWN (the line says why); nothing here fails the levels lines, the window or the cycle.
"""
from __future__ import annotations

import os
import time
from datetime import date, datetime
from enum import Enum
from typing import Callable, Mapping

REGIME_SETTING = "REALTIME_V3_REGIME"
REGIME_SETTING_EVENT = "OPTIONS_REGIME_SETTING"
REGIME_RECORD_EVENT = "OPTIONS_REGIME_RECORD"
VERSION = "options-regime-record-v1"
INDEX_NAMES = ("SPY", "QQQ", "IWM")
IV_NAMES = ("SPY", "QQQ")
NEAR_FLIP_PCT = 0.5
INVERTED_RATIO = 1.0
FLAT_RATIO = 0.95
FRONT_DAYS, BACK_DAYS = 7, 30
STATEMENT_TIMEOUT = "10s"


class RegimeMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"


def parse_regime_mode(raw: object) -> RegimeMode:
    if isinstance(raw, RegimeMode):
        return raw
    if raw is not None and not isinstance(raw, str):
        raise ValueError(f"{REGIME_SETTING} must be OFF or RECORD, not {raw!r}")
    value = (raw or "").strip().upper() or RegimeMode.OFF.value
    try:
        return RegimeMode(value)
    except ValueError:
        raise ValueError(f"{REGIME_SETTING} must be OFF or RECORD, not {raw!r}") from None


def regime_mode(environ: Mapping[str, str] | None = None) -> RegimeMode:
    """`REALTIME_V3_REGIME`; unset or blank is OFF. Anything but OFF or RECORD raises."""
    return parse_regime_mode((os.environ if environ is None else environ).get(REGIME_SETTING))


# ---- the pure parts ---------------------------------------------------------------------------------------------------

def _float(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None


def index_entry(document: Mapping[str, object] | None) -> dict:
    """An index ETF's gamma context from its levels line."""
    if not isinstance(document, Mapping) or not document.get("available"):
        return {"available": False, "reason": None if not isinstance(document, Mapping) else document.get("reason")}
    flip_pct = _float(document.get("flip_pct"))
    if document.get("flip") is None or flip_pct is None:
        flip_state = "NO_FLIP"
    elif abs(flip_pct) <= NEAR_FLIP_PCT:
        flip_state = "NEAR_FLIP"
    else:
        flip_state = "ABOVE_FLIP" if flip_pct < 0 else "BELOW_FLIP"  # flip below spot: spot is above it
    return {
        "available": True, "spot": document.get("spot"), "gex": document.get("gex"),
        "gex_sign": document.get("gex_sign"), "regime_at_spot": document.get("regime_at_spot"),
        "flip": document.get("flip"), "flip_pct": flip_pct, "flip_state": flip_state,
        "call_wall_pct": document.get("call_wall_pct"), "put_wall_pct": document.get("put_wall_pct"),
        "zero_dte_share": document.get("zero_dte_share"), "dex_tilt": document.get("dex_tilt"),
    }


def _tenor(summary: Mapping[str, object], days: int) -> Mapping[str, object] | None:
    return next((item for item in summary.get("tenors") or () if item.get("days") == days), None)


def iv_entry(summary) -> dict:
    """An index ETF's term structure and skew from its A2 IV summary (or why it is missing)."""
    if not isinstance(summary, Mapping):
        return {"available": False, "reason": str(summary) if summary is not None else "NOT_READ"}
    if not summary.get("available"):
        return {"available": False, "reason": summary.get("reason")}
    front, back = _tenor(summary, FRONT_DAYS) or {}, _tenor(summary, BACK_DAYS) or {}
    front_iv, back_iv = _float(front.get("atm_iv")), _float(back.get("atm_iv"))
    ratio = None if front_iv is None or not back_iv else round(front_iv / back_iv, 4)
    term = ("UNKNOWN" if ratio is None else "INVERTED" if ratio >= INVERTED_RATIO
            else "FLAT" if ratio >= FLAT_RATIO else "CONTANGO")
    put_call = summary.get("put_call") or {}
    return {
        "available": True, "observed_at": summary.get("observed_at"), "atm_7d": front_iv, "atm_30d": back_iv,
        "term_ratio": ratio, "term": term, "rr25_30d": _float(back.get("risk_reversal_25_delta")),
        "put_call_volume": put_call.get("volume_ratio"), "put_call_oi": put_call.get("open_interest_ratio"),
    }


def universe_put_call(chains: Mapping[str, object]) -> dict:
    """Put / call day volume and open interest over every name's captured chain (the levels read's chains)."""
    totals = {"put_volume": 0.0, "call_volume": 0.0, "put_oi": 0.0, "call_oi": 0.0}
    names = 0
    for chain in chains.values():
        try:
            calls = chain.is_call
            totals["call_volume"] += float(chain.day_volume[calls].sum())
            totals["put_volume"] += float(chain.day_volume[~calls].sum())
            totals["call_oi"] += float(chain.open_interest[calls].sum())
            totals["put_oi"] += float(chain.open_interest[~calls].sum())
            names += 1
        except Exception:  # noqa: BLE001 - a malformed chain is left out
            continue
    return {
        "names": names,
        "volume_ratio": round(totals["put_volume"] / totals["call_volume"], 4) if totals["call_volume"] else None,
        "oi_ratio": round(totals["put_oi"] / totals["call_oi"], 4) if totals["call_oi"] else None,
        **{key: int(value) for key, value in totals.items()},
    }


def classify(index: Mapping[str, Mapping], iv: Mapping[str, Mapping]) -> dict:
    """The labels and the regime from the index entries and the IV entries (pure)."""
    spy, qqq = index.get("SPY") or {}, index.get("QQQ") or {}
    signs = [entry.get("gex_sign") for entry in (spy, qqq) if entry.get("available")]
    if not spy.get("available"):
        gamma = "UNKNOWN"
    elif all(sign == "NEGATIVE" for sign in signs):
        gamma = "NEGATIVE"
    elif all(sign == "POSITIVE" for sign in signs):
        gamma = "POSITIVE"
    else:
        gamma = "MIXED"
    flip = spy.get("flip_state") if spy.get("available") else "UNKNOWN"
    term = (iv.get("SPY") or {}).get("term") or "UNKNOWN"
    if gamma == "UNKNOWN":
        regime = "UNKNOWN"
    elif flip == "NEAR_FLIP":
        regime = "TRANSITION"
    elif gamma == "NEGATIVE" and term == "INVERTED":
        regime = "STRESS"
    elif gamma == "NEGATIVE" or (gamma == "MIXED" and term == "INVERTED"):
        regime = "FRAGILE"
    elif gamma == "POSITIVE" and term == "CONTANGO":
        regime = "CALM"
    else:
        regime = "NEUTRAL"
    return {"gamma": gamma, "flip": flip, "term": term, "regime": regime}


def regime_document(documents: Mapping[str, Mapping], summaries: Mapping[str, object], chains: Mapping[str, object],
                    ) -> dict:
    """The window's regime document from the levels lines (by name), the IV summaries and the chains (pure)."""
    signs: dict[str, list[str]] = {"NEGATIVE": [], "POSITIVE": [], "ZERO": []}
    for name, document in sorted(documents.items()):
        if document.get("available") and document.get("gex_sign") in signs:
            signs[str(document["gex_sign"])].append(name)
    counted = sum(len(items) for items in signs.values())
    index = {name: index_entry(documents.get(name)) for name in INDEX_NAMES}
    iv = {name: iv_entry(summaries.get(name)) for name in IV_NAMES}
    return {
        "labels": classify(index, iv), "index": index, "iv": iv,
        "gamma_breadth": {"names": counted, "negative": signs["NEGATIVE"], "positive": signs["POSITIVE"],
                          "zero": signs["ZERO"],
                          "negative_share": round(len(signs["NEGATIVE"]) / counted, 4) if counted else None},
        "put_call": universe_put_call(chains),
    }


# ---- the recorder -----------------------------------------------------------------------------------------------------

def _read_summaries(provider, *, session_date: date, documents: Mapping[str, Mapping], read=None) -> tuple[dict, float]:
    """The A2 IV summary of each IV name at its levels capture (one short read-only transaction); a name's failure is
    that name's reason."""
    from .database_access_v2 import APP_ROLE

    if read is None:
        from .options_iv_summary_read import iv_summary_from_breadth_cursor as read
    started = time.monotonic()
    summaries: dict[str, object] = {}
    wanted = [name for name in IV_NAMES if (documents.get(name) or {}).get("observed_at")]
    for name in IV_NAMES:
        if name not in wanted:
            summaries[name] = "NO_LEVELS_CAPTURE"
    if wanted:
        with provider.transaction(APP_ROLE, read_only=True) as unit:
            cursor = unit.cursor
            cursor.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
            for name in wanted:
                cursor.execute("SAVEPOINT regime_name")
                try:
                    observed = datetime.fromisoformat(str(documents[name]["observed_at"]))
                    summaries[name] = read(cursor, underlyer=name, available_by=observed, session_date=session_date)
                    cursor.execute("RELEASE SAVEPOINT regime_name")
                except Exception as error:  # noqa: BLE001 - this name only
                    cursor.execute("ROLLBACK TO SAVEPOINT regime_name")
                    summaries[name] = f"READ_FAILED: {type(error).__name__}"
    return summaries, round(time.monotonic() - started, 3)


class RegimeRecorder:
    """Attached to the levels recorder (it hands over each window's levels lines and chains); keeps no state."""

    def __init__(self, *, read=None) -> None:
        self._read = read

    def record(self, provider, *, head: Mapping[str, object], session_date: date, documents: Mapping[str, Mapping],
               chains: Mapping[str, object]) -> dict:
        """The window's `OPTIONS_REGIME_RECORD` document. Raises only if the transaction itself fails (the caller logs
        that as the line's error)."""
        started = time.monotonic()
        try:
            summaries, read_seconds = _read_summaries(provider, session_date=session_date, documents=documents,
                                                      read=self._read)
            read_error = None
        except Exception as error:  # noqa: BLE001 - the term stays UNKNOWN, the gamma part is still recorded
            summaries, read_seconds = {name: "READ_FAILED" for name in IV_NAMES}, None
            read_error = f"{type(error).__name__}: {error}"[:200]
        document = regime_document(documents, summaries, chains)
        return {**head, "mode": "RECORD", "version": VERSION, **document, "read_error": read_error,
                "read_seconds": read_seconds, "seconds": round(time.monotonic() - started, 3)}


def start_regime(mode: RegimeMode | str | None = None, *, levels_mode=None,
                 emit: Callable[[str, Mapping[str, object]], None] | None = None) -> RegimeRecorder | None:
    """The scheduler's start: None for OFF (no line); RECORD logs its start line and returns the recorder, or None (with
    an inactive start line) when the levels recorder itself is OFF."""
    from .advanced_options_levels import LevelsMode, _print_line, options_levels_mode, parse_options_levels_mode

    mode = regime_mode() if mode is None else parse_regime_mode(mode)
    if mode is RegimeMode.OFF:
        return None
    levels_mode = options_levels_mode() if levels_mode is None else parse_options_levels_mode(levels_mode)
    active = levels_mode is not LevelsMode.OFF
    document = {"mode": mode.value, "setting": REGIME_SETTING, "version": VERSION, "active": active,
                "applied": False, "near_flip_pct": NEAR_FLIP_PCT, "inverted_ratio": INVERTED_RATIO,
                "flat_ratio": FLAT_RATIO, "iv_names": list(IV_NAMES)}
    if not active:
        document["reason"] = "LEVELS_OFF"
    else:
        try:
            # The IV summary's modules load here, not in the first window (as the V-TERM lane does).
            from options.analytics import chain_analysis  # noqa: F401
            from options.domain import ContractType  # noqa: F401

            from . import advanced_options_projection, options_iv_summary_read  # noqa: F401
        except Exception:  # noqa: BLE001 - the first window then says why
            pass
    try:
        (emit or _print_line)(REGIME_SETTING_EVENT, document)
    except Exception:  # noqa: BLE001 - a start line never stops the scheduler
        pass
    return RegimeRecorder() if active else None
