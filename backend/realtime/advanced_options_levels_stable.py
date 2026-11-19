"""G106 D15 follow-up (Nov 19 2026): stable call and put walls, recorded beside the levels recorder's v1 walls.

The levels recorder (`advanced_options_levels`, `REALTIME_V3_OPTIONS_LEVELS`) logs the OV1 page's walls per name per
Options window: the call wall is the strike with the most call gamma and the put wall the strike with the most put gamma,
anywhere in the chain. On Nov 18 (77 windows, EOD_ANALYSIS section 5) that rule flapped between near-equal adjacent
strikes (AMZN put wall 25 changes, IWM call wall 24, AAPL put wall 12) and could put both walls on the same strike at or
below spot (SPY 730/730 late in the day, MU 700/700, AVGO 350/350). No rule may use those walls as they are.

The stable policy (`VERSION`), computed from the same capture and the same per-strike gamma (dollars per 1% move, weight
= prior-settlement open interest, every captured expiry):
- side: a new call wall is searched only among strikes above spot, a new put wall only among strikes below spot (a
  strike exactly at spot is on neither side), and the two walls are never the same strike;
- hold: once a wall is chosen it is kept, window after window, while it still carries gamma and spot has not moved more
  than `BAND` through it, unless the best strike on its side beats it by more than `MARGIN` (its gamma > held gamma x
  (1 + MARGIN));
- a held wall that spot moved through (beyond the band) or that lost its gamma is replaced by the best strike on its
  side.
The held walls live in the recorder's memory for the session (a restart starts FRESH; nothing is stored).

`REALTIME_V3_OPTIONS_LEVELS_STABLE`, read by the Options scheduler at start:
| OFF (default, unset, blank) | nothing: no line, no computation; the levels lines and the cycle result are byte-identical |
| RECORD | with `REALTIME_V3_OPTIONS_LEVELS` RECORD or ON: per name per window one `OPTIONS_LEVELS_STABLE_RECORD` line with
           the v1 walls (and whether they sit on the wrong side or coincide) and the stable walls (with the reason each
           was kept or changed, the best strike on its side, and change counts so far this session for both policies),
           and one `OPTIONS_LEVELS_STABLE_WINDOW` line; no extra SQL (the levels read's chains are reused); nothing is
           written or applied. With the levels switch OFF: one start line saying it is inactive, nothing else |
Anything else (any case accepted for the two) and the scheduler refuses to start. A failure is that name's line only
and never fails the levels line, the window or the cycle.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Callable, Mapping

STABLE_SETTING = "REALTIME_V3_OPTIONS_LEVELS_STABLE"
STABLE_SETTING_EVENT = "OPTIONS_LEVELS_STABLE_SETTING"
STABLE_RECORD_EVENT = "OPTIONS_LEVELS_STABLE_RECORD"
STABLE_WINDOW_EVENT = "OPTIONS_LEVELS_STABLE_WINDOW"
VERSION = "options-levels-stable-v1"
V1_VERSION = "options-levels-record-v1"
# A challenger on the wall's side must carry more than (1 + MARGIN) x the held wall's gamma to replace it.
MARGIN = 0.25
# A held wall is kept while spot is less than BAND (fraction of spot) through it; a new wall is always strictly on its
# side.
BAND = 0.005
TOP_STRIKES = 6

KEPT_BASES = ("BEST", "HELD")
# FRESH: no wall held yet this session (first window, or after a restart); BEST: the held wall is still the best on its
# side; HELD: a challenger is better but within the margin; SWITCHED: a challenger beat the held wall by the margin;
# WRONG_SIDE: spot moved more than BAND through the held wall; NO_GAMMA: the held wall's strike has no gamma of its type
# any more; TAKEN: the other side's wall took the strike; NONE_ON_SIDE: no strike on that side of spot carries gamma.


class StableMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"


def parse_levels_stable_mode(raw: object) -> StableMode:
    if isinstance(raw, StableMode):
        return raw
    if raw is not None and not isinstance(raw, str):
        raise ValueError(f"{STABLE_SETTING} must be OFF or RECORD, not {raw!r}")
    value = (raw or "").strip().upper() or StableMode.OFF.value
    try:
        return StableMode(value)
    except ValueError:
        raise ValueError(f"{STABLE_SETTING} must be OFF or RECORD, not {raw!r}") from None


def levels_stable_mode(environ: Mapping[str, str] | None = None) -> StableMode:
    """`REALTIME_V3_OPTIONS_LEVELS_STABLE`; unset or blank is OFF. Anything but OFF or RECORD raises."""
    return parse_levels_stable_mode((os.environ if environ is None else environ).get(STABLE_SETTING))


# ---- per-strike gamma (the page's numbers, without the page's other work) ---------------------------------------------

@dataclass(frozen=True)
class StrikeGamma:
    """Per strike, ascending: the strike as the page writes it, its value, and the unsigned call and put gamma in
    dollars per 1% move (open interest weight, every expiry): the quantities the page's walls maximise."""

    spot: float
    strikes: tuple[float, ...]
    texts: tuple[str, ...]
    call: tuple[float, ...]
    put: tuple[float, ...]
    # The same without the contracts expiring today (late in the day their gamma piles up next to spot).
    call_ex0dte: tuple[float, ...] = ()
    put_ex0dte: tuple[float, ...] = ()

    def without_0dte(self) -> "StrikeGamma":
        if not self.call_ex0dte and not self.put_ex0dte:
            return self
        return StrikeGamma(self.spot, self.strikes, self.texts, self.call_ex0dte, self.put_ex0dte)

    def value(self, side: str, text: str | None) -> float:
        if text is None:
            return 0.0
        try:
            index = self.texts.index(text)
        except ValueError:
            return 0.0
        return (self.call if side == "call" else self.put)[index]

    def strike(self, text: str) -> float | None:
        try:
            return self.strikes[self.texts.index(text)]
        except ValueError:
            return None


def strike_gamma(chain) -> StrikeGamma:
    """The OV1 page's per-strike call and put gamma for one capture (`compute_exposure` gamma, weight oi, scope all:
    contracts with open interest, a live maturity and a finite gamma), as plain tuples."""
    import numpy as np

    from .options_exposure_read import _unit

    with np.errstate(all="ignore"):
        unit = _unit(chain, "gamma")
        weights = chain.open_interest
        picked = np.flatnonzero((weights > 0) & np.isfinite(chain.maturity) & np.isfinite(unit))
        if not picked.size:
            return StrikeGamma(float(chain.spot), (), (), (), ())
        values = np.abs(unit[picked] * weights[picked])
        calls = chain.is_call[picked]
        later = chain.calendar_dte[picked] != 0
        strikes, index = np.unique(chain.strike[picked], return_inverse=True)
        call = np.bincount(index, weights=np.where(calls, values, 0.0), minlength=strikes.size)
        put = np.bincount(index, weights=np.where(calls, 0.0, values), minlength=strikes.size)
        call_later = np.bincount(index, weights=np.where(calls & later, values, 0.0), minlength=strikes.size)
        put_later = np.bincount(index, weights=np.where(~calls & later, values, 0.0), minlength=strikes.size)
    text_by_strike: dict[float, str] = {}
    for item in picked:
        text_by_strike.setdefault(float(chain.strike[item]), chain.strike_text[item])
    return StrikeGamma(
        spot=float(chain.spot), strikes=tuple(float(item) for item in strikes),
        texts=tuple(text_by_strike[float(item)] for item in strikes),
        call=tuple(float(item) for item in call), put=tuple(float(item) for item in put),
        call_ex0dte=tuple(float(item) for item in call_later), put_ex0dte=tuple(float(item) for item in put_later),
    )


def unconstrained_wall(profile: StrikeGamma, side: str) -> str | None:
    """The v1 rule on these numbers (the page's tie-break: calls the higher strike, puts the lower), for parity."""
    values = profile.call if side == "call" else profile.put
    if not values:
        return None
    if side == "call":
        best = max(range(len(values)), key=lambda index: (values[index], profile.strikes[index]))
    else:
        best = max(range(len(values)), key=lambda index: (values[index], -profile.strikes[index]))
    return profile.texts[best] if values[best] > 0 else None


def best_on_side(profile: StrikeGamma, side: str, *, exclude: str | None = None) -> str | None:
    """The strike with the most gamma of that type strictly on its side of spot (calls above, puts below), other than
    `exclude` (the other side's wall); a tie goes to the strike nearer spot."""
    values = profile.call if side == "call" else profile.put
    spot = profile.spot
    candidates = [index for index, strike in enumerate(profile.strikes)
                  if values[index] > 0 and (strike > spot if side == "call" else strike < spot)
                  and profile.texts[index] != exclude]
    if not candidates:
        return None
    best = max(candidates, key=lambda index: (values[index], -abs(profile.strikes[index] - spot)))
    return profile.texts[best]


def top_strikes(profile: StrikeGamma, side: str, count: int = TOP_STRIKES) -> list[list]:
    """[[strike, gamma $/1%], ...] for the `count` strikes with the most gamma of that type, largest first."""
    values = profile.call if side == "call" else profile.put
    ranked = sorted((index for index in range(len(values)) if values[index] > 0),
                    key=lambda index: (-values[index], profile.strikes[index]))[:count]
    return [[profile.texts[index], int(round(values[index]))] for index in ranked]


def held_problem(profile: StrikeGamma, side: str, held: str, *, band: float = BAND) -> str | None:
    """Why a held wall cannot be kept (NO_GAMMA, WRONG_SIDE), or None. A held wall may sit up to `band` (a fraction of
    spot) on the wrong side of spot: spot trading through a wall is not a reason to move it."""
    strike = profile.strike(held)
    if strike is None or profile.value(side, held) <= 0:
        return "NO_GAMMA"
    spot = profile.spot
    if side == "call":
        return None if strike > spot or (band > 0 and strike >= spot * (1.0 - band)) else "WRONG_SIDE"
    return None if strike < spot or (band > 0 and strike <= spot * (1.0 + band)) else "WRONG_SIDE"


def choose_wall(profile: StrikeGamma, side: str, held: str | None, *, margin: float = MARGIN, band: float = BAND,
                exclude: str | None = None) -> tuple[str | None, str]:
    """(wall, basis) for one side; `held` is the wall kept from the previous window of the session (or None),
    `exclude` the other side's wall (the two never coincide)."""
    best = best_on_side(profile, side, exclude=exclude)
    if held is None:
        return (best, "FRESH") if best is not None else (None, "NONE_ON_SIDE")
    problem = "TAKEN" if held == exclude else held_problem(profile, side, held, band=band)
    if problem is not None:
        return (best, problem) if best is not None else (None, "NONE_ON_SIDE")
    if best is None or best == held:
        return held, "BEST"
    if profile.value(side, best) > profile.value(side, held) * (1.0 + margin):
        return best, "SWITCHED"
    return held, "HELD"


def choose_walls(profile: StrikeGamma, held_call: str | None, held_put: str | None, *, margin: float = MARGIN,
                 band: float = BAND) -> dict[str, tuple[str | None, str]]:
    """Both walls: the call wall first (it may not take a put wall that is still kept), then the put wall (it may not
    take the call wall just chosen)."""
    kept_put = None
    if held_put is not None and held_problem(profile, "put", held_put, band=band) is None:
        kept_put = held_put
    call = choose_wall(profile, "call", held_call, margin=margin, band=band, exclude=kept_put)
    put = choose_wall(profile, "put", held_put, margin=margin, band=band, exclude=call[0])
    return {"call": call, "put": put}


# ---- the tracker ------------------------------------------------------------------------------------------------------

def _pct(strike: float | None, spot: float) -> float | None:
    if strike is None or not spot:
        return None
    return round((strike - spot) / spot * 100.0, 3)


def _side_of(text, spot: float) -> str | None:
    if text is None:
        return None
    try:
        strike = float(text)
    except (TypeError, ValueError):
        return None
    return "ABOVE" if strike > spot else "BELOW" if strike < spot else "AT"


@dataclass
class _NameState:
    windows: int = 0
    v1: dict = field(default_factory=lambda: {"call": None, "put": None})
    v1_changes: dict = field(default_factory=lambda: {"call": 0, "put": 0})
    side_best: dict = field(default_factory=lambda: {"call": None, "put": None})
    side_best_changes: dict = field(default_factory=lambda: {"call": 0, "put": 0})
    stable: dict = field(default_factory=lambda: {"call": None, "put": None})
    stable_changes: dict = field(default_factory=lambda: {"call": 0, "put": 0})


class StableLevels:
    """Held walls per name for the current session (one per scheduler process; a new session starts fresh)."""

    def __init__(self, *, margin: float = MARGIN, band: float = BAND,
                 profile: Callable[[object], StrikeGamma] = strike_gamma) -> None:
        self.margin, self.band = float(margin), float(band)
        self._profile = profile
        self._session: date | None = None
        self._names: dict[str, _NameState] = {}
        self._window: dict[str, list[str]] = {}

    def begin_window(self, session_date: date) -> None:
        if session_date != self._session:
            self._session, self._names = session_date, {}
        self._window = {"v1_changed": [], "stable_changed": [], "v1_wrong_side": [], "v1_coincide": [],
                        "held": [], "recorded": [], "unavailable": [], "failed": []}

    def unavailable(self, name: str, reason: object) -> dict:
        self._window.setdefault("unavailable", []).append(name)
        return {"underlying": name, "available": False, "reason": reason}

    def failed(self, name: str, error: BaseException) -> dict:
        self._window.setdefault("failed", []).append(name)
        return {"underlying": name, "available": False, "reason": "FAILED",
                "error": f"{type(error).__name__}: {error}"[:200]}

    def observe(self, name: str, chain, levels: Mapping[str, object]) -> dict:
        """One name's stable walls for this window, next to the v1 walls of its levels line `levels`. Pure apart from
        the held state; raises only on a programming error (the caller logs that name's failure)."""
        if not levels.get("available"):
            return self.unavailable(name, levels.get("reason"))
        profile = self._profile(chain)
        spot = profile.spot
        state = self._names.setdefault(name, _NameState())
        state.windows += 1
        document: dict[str, object] = {"underlying": name, "available": True, "observed_at": levels.get("observed_at"),
                                       "spot": levels.get("spot"), "margin": self.margin, "band": self.band,
                                       "windows": state.windows}
        v1: dict[str, object] = {}
        stable: dict[str, object] = {}
        chosen_walls = choose_walls(profile, state.stable["call"], state.stable["put"], margin=self.margin,
                                    band=self.band)
        later = profile.without_0dte()
        for side in ("call", "put"):
            wall = levels.get(f"{side}_wall")
            changed = state.v1[side] is not None and wall != state.v1[side]
            state.v1_changes[side] += int(changed)
            state.v1[side] = wall
            v1[f"{side}_wall"] = wall
            v1[f"{side}_side"] = _side_of(wall, spot)
            v1[f"{side}_changed"] = changed

            best = best_on_side(profile, side)
            best_changed = state.side_best[side] is not None and best != state.side_best[side]
            state.side_best_changes[side] += int(best_changed)
            state.side_best[side] = best

            held = state.stable[side]
            chosen, basis = chosen_walls[side]
            chosen_changed = held is not None and chosen != held
            state.stable_changes[side] += int(chosen_changed)
            state.stable[side] = chosen
            chosen_value = profile.value(side, chosen)
            stable.update({
                f"{side}_wall": chosen, f"{side}_wall_gex": int(round(chosen_value)) if chosen else None,
                f"{side}_wall_pct": _pct(profile.strike(chosen), spot) if chosen else None,
                f"{side}_basis": basis, f"{side}_changed": chosen_changed,
                f"{side}_side": _side_of(chosen, spot),
                f"{side}_best": best, f"{side}_best_gex": int(round(profile.value(side, best))) if best else None,
                f"{side}_best_vs_wall": round(profile.value(side, best) / chosen_value, 4)
                if best and chosen and chosen_value > 0 else None,
                # Diagnostic only: the best strike on its side without today's expiry.
                f"{side}_best_ex0dte": best_on_side(later, side),
            })
            if basis == "HELD":
                self._window["held"].append(f"{name}:{side}")
        v1["coincide"] = v1["call_wall"] is not None and v1["call_wall"] == v1["put_wall"]
        v1["wrong_side"] = [side for side, expected in (("call", "ABOVE"), ("put", "BELOW"))
                            if v1[f"{side}_side"] not in (None, expected)]
        v1["changes"] = dict(state.v1_changes)
        stable["changes"] = dict(state.stable_changes)
        stable["side_best_changes"] = dict(state.side_best_changes)
        document.update({"v1": v1, "stable": stable,
                         "strikes": {"count": len(profile.strikes),
                                     "above_spot": sum(1 for strike in profile.strikes if strike > spot),
                                     "below_spot": sum(1 for strike in profile.strikes if strike < spot)},
                         # The largest strikes per type anywhere in the chain (with and without today's expiry), so
                         # another margin, band or an ex-0DTE rule can be replayed from these lines alone.
                         "call_top": top_strikes(profile, "call"), "put_top": top_strikes(profile, "put"),
                         "call_top_ex0dte": top_strikes(later, "call"), "put_top_ex0dte": top_strikes(later, "put")})
        self._window["recorded"].append(name)
        if v1["call_changed"] or v1["put_changed"]:
            self._window["v1_changed"].append(name)
        if stable["call_changed"] or stable["put_changed"]:
            self._window["stable_changed"].append(name)
        if v1["wrong_side"]:
            self._window["v1_wrong_side"].append(name)
        if v1["coincide"]:
            self._window["v1_coincide"].append(name)
        return document

    def window_summary(self) -> dict:
        totals = {"v1": {"call": 0, "put": 0}, "stable": {"call": 0, "put": 0}}
        for state in self._names.values():
            for side in ("call", "put"):
                totals["v1"][side] += state.v1_changes[side]
                totals["stable"][side] += state.stable_changes[side]
        return {"version": VERSION, "v1_version": V1_VERSION, "margin": self.margin, "band": self.band,
                **{key: sorted(value) for key, value in self._window.items()},
                "session_changes": totals}


def start_levels_stable(mode: StableMode | str | None = None, *, levels_mode=None,
                        emit: Callable[[str, Mapping[str, object]], None] | None = None) -> StableLevels | None:
    """The scheduler's start: None for OFF (no line); RECORD logs its start line and returns the tracker, or None (with
    an inactive start line) when the levels recorder itself is OFF."""
    from .advanced_options_levels import LevelsMode, _print_line, options_levels_mode, parse_options_levels_mode

    mode = levels_stable_mode() if mode is None else parse_levels_stable_mode(mode)
    if mode is StableMode.OFF:
        return None
    levels_mode = options_levels_mode() if levels_mode is None else parse_options_levels_mode(levels_mode)
    active = levels_mode is not LevelsMode.OFF
    document = {"mode": mode.value, "setting": STABLE_SETTING, "version": VERSION, "margin": MARGIN, "band": BAND,
                "active": active, "applied": False}
    if not active:
        document["reason"] = "LEVELS_OFF"
    try:
        (emit or _print_line)(STABLE_SETTING_EVENT, document)
    except Exception:  # noqa: BLE001 - a start line never stops the scheduler
        pass
    return StableLevels() if active else None
