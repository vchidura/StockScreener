"""G102 R83-0 (Nov 17): the Equity window carries a shorter 1m/5m/15m history.

Switch `REALTIME_V2_EQUITY_INTRADAY_HISTORY_SESSIONS`, read by the advanced scheduler's ticker cycle
(`ticker_repository._project_ticker_cycle`, also on recovery):

- unset, empty, `OFF` or `FULL`: today's behaviour exactly (no depth is passed anywhere; the loader's SQL, its
  parameters, the source closure and the projection are unchanged);
- `1m:<n1>,5m:<n5>,15m:<n15>` (for example `1m:2,5m:10,15m:22`): the window loads and charts the last n1 sessions
  of 1m bars, n5 of 5m and n15 of 15m (each counted back from the window's session, the session itself included).
  30m and 1h keep 90 sessions, raw daily 90, adjusted daily 283, weekly and monthly unchanged.
  Bounds: 1 <= n1 <= 5 (the history policy's 1m depth), 6 <= n5 <= 90 and 14 <= n15 <= 90: the retired 5m/15m price
  channels read the last 300 bars, which the completed sessions of 6 sessions of 5m and 14 of 15m still hold before
  the first bar of a session, with one half-day among them.
  An invalid value is logged and ignored (today's behaviour), like the shard switch.

Why the source closure is bounded too. Every raw 15m/30m/1h/1d bar is DERIVED from 5m bars and every stream 5m bar
from 1m bars; the window load closes that lineage (`close_source_lineage`) and the projection re-derives each stored
aggregate from its sources. Bounding only the 1m/5m/15m predicate would bring the 5m and 1m bars back as sources of
90 sessions of 30m/1h/raw daily. With a depth, only the derived bars inside the verified depth are closure roots:
5m bars of the 1m sessions (re-derived from their minutes) and 15m/30m/1h/raw daily bars of the 5m sessions
(re-derived from their 5m bars). Older derived bars are accepted by their stored lineage, as the R78 daily-products job
does (`STORED_LINEAGE_ACCEPTED`): they are immutable rows the windows of their own sessions already verified, and they
still name their sources (the lineage heads and chains are unchanged). The window's input is marked accordingly, so
the screening's `_validate_bars` checks every derived bar whose sources are present and skips the others.

What changes when the switch is ON (by design): the 1m/5m/15m chart groups (shorter, so the ticker page's 1m/5m/15m
charts show n1/n5/n15 sessions), their lineage heads in each workspace manifest and the manifest's `input_sha256`, the
screening generation's `input_sha256` and `generation` digest (which every workspace embeds as
`context.cohort.generation`), the cycle receipt's `input_sha256` (`inputs_sha256` over fewer bars) and the summary/Pattern
Watch workspace-set identities that embed them. A 1m chart that a gap in an older session withheld
(NATIVE_1M_HISTORY_INCOMPLETE) is published again once that session is outside the 1m depth. Setups, trade plans,
Pattern Watch rows, evidence (30m..1mo), the 5m/15m channels, the quote and the screening rows are computed from the
same bars as before.

Flip it only with a scheduler restart outside a session: recovery (`rehydrate_realtime_ticker_cycle`) rebuilds a cycle
with the current switch, so a cycle computed in the other mode fails closed (CycleRehydrationMismatch).
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from typing import Mapping

import exchange_calendars

INTRADAY_HISTORY_SWITCH = "REALTIME_V2_EQUITY_INTRADAY_HISTORY_SESSIONS"
INTRADAY_HISTORY_OFF_VALUES = frozenset({"", "OFF", "FULL"})
# (minimum, maximum) sessions per bounded interval; see the module docstring.
INTRADAY_HISTORY_BOUNDS = {"1m": (1, 5), "5m": (6, 90), "15m": (14, 90)}
BOUNDED_INTERVALS = ("1m", "5m", "15m")
_SESSION_MINUTES = {"1m": 1, "5m": 5, "15m": 15}
# Raw derived intervals whose sources are 5m bars (the stream finalizer and the portal history derivation).
_FIVE_MINUTE_DERIVED = frozenset({"15m", "30m", "1h", "1d"})

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class IntradayHistoryDepth:
    """Sessions of raw 1m, 5m and 15m bars the window carries."""

    one_minute: int
    five_minute: int
    fifteen_minute: int

    def __post_init__(self) -> None:
        for interval, value in zip(BOUNDED_INTERVALS, (self.one_minute, self.five_minute, self.fifteen_minute)):
            low, high = INTRADAY_HISTORY_BOUNDS[interval]
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"{interval} intraday history must be {low}..{high} sessions, not {value!r}")

    def sessions(self, interval: str) -> int:
        return {"1m": self.one_minute, "5m": self.five_minute, "15m": self.fifteen_minute}[interval]

    @property
    def label(self) -> str:
        return ",".join(f"{interval}:{self.sessions(interval)}" for interval in BOUNDED_INTERVALS)


def parse_intraday_history_depth(text) -> IntradayHistoryDepth | None:
    """None for the OFF values; otherwise exactly `1m:<n>,5m:<n>,15m:<n>` (any order, spaces allowed). Raises
    ValueError for anything else."""
    raw = "" if text is None else str(text).strip()
    if raw.upper() in INTRADAY_HISTORY_OFF_VALUES:
        return None
    values: dict[str, int] = {}
    for part in raw.split(","):
        interval, separator, count = part.partition(":")
        interval, count = interval.strip().lower(), count.strip()
        if not separator or interval not in INTRADAY_HISTORY_BOUNDS or interval in values:
            raise ValueError(f"{INTRADAY_HISTORY_SWITCH} must be 1m:<n>,5m:<n>,15m:<n>, not {raw!r}")
        if not count.isdigit():
            raise ValueError(f"{INTRADAY_HISTORY_SWITCH} session counts must be integers, not {raw!r}")
        values[interval] = int(count)
    if set(values) != set(BOUNDED_INTERVALS):
        raise ValueError(f"{INTRADAY_HISTORY_SWITCH} must name 1m, 5m and 15m, not {raw!r}")
    return IntradayHistoryDepth(values["1m"], values["5m"], values["15m"])


def intraday_history_depth(environ: Mapping[str, str] | None = None) -> IntradayHistoryDepth | None:
    """The configured depth, or None (today's behaviour) when the switch is unset, OFF or invalid (logged)."""
    raw = (os.environ if environ is None else environ).get(INTRADAY_HISTORY_SWITCH, "")
    try:
        return parse_intraday_history_depth(raw)
    except ValueError as error:
        logger.warning("ignoring %s=%r (%s): full intraday history", INTRADAY_HISTORY_SWITCH, raw, error)
        return None


@lru_cache(maxsize=64)
def _last_sessions(session: date, count: int) -> tuple[date, ...]:
    calendar = exchange_calendars.get_calendar("XNYS")
    if not calendar.is_session(str(session)):
        raise ValueError("Intraday history requires an exchange session")
    first = calendar.session_offset(str(session), -(count - 1))
    return tuple(stamp.date() for stamp in calendar.sessions_in_range(first, str(session)))


@lru_cache(maxsize=256)
def _session_bar_count(sessions: tuple[date, ...], interval: str) -> int:
    """Bars of `interval` in complete `sessions` (the history plan's `_intraday_count`)."""
    calendar = exchange_calendars.get_calendar("XNYS")
    minutes = _SESSION_MINUTES[interval]
    count = 0
    for session in sessions:
        duration = int((calendar.session_close(str(session)) - calendar.session_open(str(session))).total_seconds() // 60)
        if duration <= 0 or duration % minutes:
            raise ValueError(f"XNYS session duration is incompatible with {interval}")
        count += duration // minutes
    return count


@dataclass(frozen=True, slots=True)
class IntradayHistoryWindow:
    """A depth resolved for one window session: the sessions each bounded interval keeps and the verified depth of
    the derived bars. Immutable and comparable (it is part of the loader's identity through `params`)."""

    depth: IntradayHistoryDepth
    session: date
    one_minute_sessions: tuple[date, ...]
    five_minute_sessions: tuple[date, ...]
    fifteen_minute_sessions: tuple[date, ...]

    def sessions(self, interval: str) -> tuple[date, ...]:
        return {"1m": self.one_minute_sessions, "5m": self.five_minute_sessions,
                "15m": self.fifteen_minute_sessions}[interval]

    def start(self, interval: str) -> date:
        return self.sessions(interval)[0]

    @property
    def params(self) -> tuple[date, date, date]:
        """The parameters of `BAR_DEPTH_PREDICATE`, in its order."""
        return self.start("1m"), self.start("5m"), self.start("15m")

    def session_sets(self) -> dict[str, frozenset[date]]:
        return {interval: frozenset(self.sessions(interval)) for interval in BOUNDED_INTERVALS}

    def closure_root(self, row) -> bool:
        """Whether the window re-verifies this bar from its sources (so the loader reads them). Stream 5m bars are
        re-derived from 1m inside the 1m sessions; raw 15m/30m/1h/daily bars from 5m inside the 5m sessions; every
        other bar (native, adjusted, weekly, monthly) as today."""
        if row["adjustment_basis"] != "RAW_UNADJUSTED":
            return True
        interval = row["interval"]
        if interval == "5m":
            return row["session_date"] >= self.one_minute_sessions[0]
        if interval in _FIVE_MINUTE_DERIVED:
            return row["session_date"] >= self.five_minute_sessions[0]
        return True

    def verified(self, facts):
        """The facts of a raw 5m-derived interval inside the 5m sessions (the ones re-derived from 5m)."""
        first = self.five_minute_sessions[0]
        return tuple(fact for fact in facts if fact.revision.session_date >= first)


def resolve_intraday_history_window(session: date, depth: IntradayHistoryDepth) -> IntradayHistoryWindow:
    return IntradayHistoryWindow(
        depth=depth,
        session=session,
        one_minute_sessions=_last_sessions(session, depth.one_minute),
        five_minute_sessions=_last_sessions(session, depth.five_minute),
        fifteen_minute_sessions=_last_sessions(session, depth.fifteen_minute),
    )


class BoundedIntradayCounts:
    """The expected-count view `_intraday_expected_count` reads: the bounded 1m/5m/15m counts over the window's
    sessions, every other interval from the history plan."""

    __slots__ = ("_plan", "_window")

    def __init__(self, plan, window: IntradayHistoryWindow) -> None:
        if tuple(plan.raw_sessions[-window.depth.five_minute:]) != window.five_minute_sessions:
            raise ValueError("Intraday history depth does not match the window's history plan")
        self._plan, self._window = plan, window

    def expected_count(self, interval: str, basis: str) -> int:
        if basis == "RAW_UNADJUSTED" and interval in _SESSION_MINUTES:
            return _session_bar_count(self._window.sessions(interval), interval)
        return self._plan.expected_count(interval, basis)


# Appended to the window's bar candidate predicate (after the portal-history clauses); parameters
# `IntradayHistoryWindow.params`. 1m/5m/15m bars exist only as RAW_UNADJUSTED.
BAR_DEPTH_PREDICATE = (
    "AND NOT (adjustment_basis='RAW_UNADJUSTED' AND ((interval='1m' AND session_date<%s) "
    "OR (interval='5m' AND session_date<%s) OR (interval='15m' AND session_date<%s))) "
)
