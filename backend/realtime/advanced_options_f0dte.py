"""G106 D5 F0DTE: a 0DTE intraday lane on the live Options window (Nov 18 2026).

Why. O1 (day volume >= 3x prior OI) fires on almost every active 0DTE strike, because most 0DTE volume opens and closes
the same day (research P1-1: 49 of 94 replayed O1 alerts were 0DTE, -54% mean / -90% median held to the close); the O1
0DTE gate (AL6) keeps them out of O1. F0DTE detects 0DTE activity with 0DTE rules instead: a burst of volume and premium
in one 5-minute window, bought at the ask, on a liquid strike near the money, with an intraday exit.

Rule (policy `f0dte_intraday_v1`; thresholds below, each in the policy document and its hash). Per window (cutoff c,
window (c - 5 min, c]) and per watched contract that expires this session:
- time: c in [open + 15 min, close - 90 min] (14:45-19:30 UTC on a regular day);
- window volume >= max(3x the contract's own average 5-minute volume since the open, a floor by tier);
- window premium (price x size x multiplier of the volume-eligible prints) >= $100k index ETFs / $50k single stocks;
- aggressor side over the window's prints: BOUGHT = premium at or above the ask >= 2x the premium at or below the bid
  (counted at HIGH/MEDIUM confidence) and >= a minimum; SOLD = the mirror; UNCLEAR otherwise. Direction: a bought call is
  bullish (+1), a bought put bearish (-1). SOLD / UNCLEAR are recorded, never alerted;
- the contract's quote at the cutoff (the window's mark): mid $0.20-$10.00, spread <= 15% of mid; Black-Scholes |delta|
  0.20-0.60 (implied from the mid, the spot at the cutoff and the time to the close);
- recorded only: whether the stock moved the direction's way over the last 5 minutes (window rolling-state spot now vs
  the previous window's).
Caps, in order of bought premium: no repeat on the same contract within 30 min, <= 2 new alerts per underlying in any 60
minutes, <= 6 per window; both directions on one underlying in one window keep the stronger (more bought premium).

Prints. The window's own session journal (the volume the window already reads its cutoff quotes from,
`advanced_options_cutoff_quotes`): every print and quote of each 0DTE contract in the window, up to the window's barrier
offset. A print is placed against the quote in force before its millisecond (quotes stamped in the print's own
millisecond follow it), with the flow role's aggression rules (`option_flow.classifier`, aggression_v1: quote age <= 1 s
HIGH, <= 5 s MEDIUM, older STALE; crossed or locked quotes and multi-leg prints are not counted; cross / floor prints LOW)
and the flow role's condition semantics (volume-eligible prints only). The stored prints (`rt.option_flow_events`) keep
only very large 0DTE prints (flow profile ULTRA_SHORT: >= $500k or 1,000 contracts on an ETF), 9 on all of Nov 17, so they
cannot carry this rule.

Alert (ON). A single-leg long option package (BUY 1), entry limit = the ask at detection (the cutoff mark), management:
target +50% of the premium, stop -35%, time exit 45 minutes after the trigger or 15 minutes before the close (20:45 UTC),
whichever is first (the package's exit deadline). The alert rides the O1 lane of the window (the cycle's detector set is
fixed by the control plane: O1/O2/O3/OS1/OS2, `advanced_options_control_plane`), with its own event id (never a flow
event), payload contract `f0dte-evaluation-v1` and detector id F0DTE on the alert (`advanced_options_projection`).

Switch `REALTIME_V3_F0DTE` on the Options window (`realtime-v3-options-p8-window`):
| OFF (default, unset) | nothing: no read, no line, window output byte for byte today's |
| RECORD | one `F0DTE_RECORD` line per window (every 0DTE contract: metrics, verdict, reasons; the would-alert list), one
             `F0DTE_ALERT_RECORD` line per would-alert, `F0DTE_OUTCOME` lines (+5/+15/+30/+45 min and the managed exit of
             each would-alert this process made); no evaluation, no alert |
| ON | the same lines, and each alert is a SELECTED evaluation with its package in the O1 lane (an F0DTE alert) |
Any other value (any case) and the window service refuses to start. Thresholds can be set with the integer settings
below (unset = the default; digits only, else the service refuses to start); they are read once per process.

Never fails the window: any error -> one `F0DTE_RECORD {"status": "RECORD_FAILED", ...}` line and a DEGRADED fault
(`F0DTE_FAILED`), and the window goes on without F0DTE (ON: no F0DTE alert that window). Contracts the window cannot
package (unmarked, incomplete volume baseline) and sessions without a stored print (the evidence contract carries one)
are rejected with a reason, never alerted.

Calibration (Nov 18 replay of this rule on the session journals of Nov 12, 13, 16, 17; 1-minute outcome marks; tiny
sample): the defaults give 0 / 1 / 8 / 1 alerts (10 in all, 7 of them single-stock calls on the Nov 16 expiry day);
managed exit +10.9% mean (mid; +8.9% exiting at the bid), 5 targets / 4 stops / 1 time. A 1.5x aggressor dominance
or a 2x volume multiple doubled to tripled the count and turned the managed mean negative or thinner. Journal read:
16 contracts, one window, 0.2-0.6 s warm, 1-3.7 s cold.
"""
from __future__ import annotations

import json
import math
from bisect import bisect_right
import os
import sqlite3
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Literal, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import Field

from equity.behavior import Contract

from .advanced_options_o1_liquidity_floor import non_negative_int_setting

F0DTE_SETTING = "REALTIME_V3_F0DTE"
F0DTE_DETECTOR_ID = "F0DTE"
F0DTE_EVALUATION_CONTRACT = "f0dte-evaluation-v1"
F0DTE_CATEGORY = "ZERO_DTE_INTRADAY"
F0DTE_RECORD_EVENT = "F0DTE_RECORD"
F0DTE_ALERT_EVENT = "F0DTE_ALERT_RECORD"
F0DTE_OUTCOME_EVENT = "F0DTE_OUTCOME"
F0DTE_FAULT_KIND = "F0DTE_FAILED"
F0DTE_MANAGEMENT_POLICY_VERSION = "f0dte_intraday_management_v1"
# Docker splits a log line over 16 KiB: the per-contract list is capped (would-alerts first), counts are complete.
_RECORD_CONTRACT_LIMIT = 32
_HORIZONS_MINUTES = (5, 15, 30, 45)
_YEAR_SECONDS = 365.0 * 24 * 3600

# Rejection reasons (a contract can carry several; an alert has none).
F0DTE_OUTSIDE_ENTRY_TIME = "F0DTE_OUTSIDE_ENTRY_TIME"
F0DTE_PRINTS_UNAVAILABLE = "F0DTE_PRINTS_UNAVAILABLE"
F0DTE_NO_PRINTS = "F0DTE_NO_PRINTS"
F0DTE_DAY_VOLUME_UNAVAILABLE = "F0DTE_DAY_VOLUME_UNAVAILABLE"
F0DTE_VOLUME_BELOW_THRESHOLD = "F0DTE_VOLUME_BELOW_THRESHOLD"
F0DTE_PREMIUM_BELOW_THRESHOLD = "F0DTE_PREMIUM_BELOW_THRESHOLD"
F0DTE_SOLD = "F0DTE_SOLD"
F0DTE_AGGRESSOR_UNCLEAR = "F0DTE_AGGRESSOR_UNCLEAR"
F0DTE_QUOTE_UNAVAILABLE = "F0DTE_QUOTE_UNAVAILABLE"
F0DTE_MID_OUTSIDE_RANGE = "F0DTE_MID_OUTSIDE_RANGE"
F0DTE_SPREAD_TOO_WIDE = "F0DTE_SPREAD_TOO_WIDE"
F0DTE_SPOT_UNAVAILABLE = "F0DTE_SPOT_UNAVAILABLE"
F0DTE_DELTA_UNAVAILABLE = "F0DTE_DELTA_UNAVAILABLE"
F0DTE_DELTA_OUTSIDE_RANGE = "F0DTE_DELTA_OUTSIDE_RANGE"
F0DTE_OUTSIDE_UNIVERSE = "F0DTE_OUTSIDE_UNIVERSE"
# The window cannot package it: unmarked at this cutoff or its volume baseline is not complete (R31), the same rule
# as O1's packages; a package there could not carry passing evidence.
F0DTE_UNPACKAGEABLE = "F0DTE_UNPACKAGEABLE"
# No stored flow event in the session yet: the package evidence contract needs one stored print to carry.
F0DTE_NO_FLOW_EVIDENCE = "F0DTE_NO_FLOW_EVIDENCE"
# The window runs too long after its cutoff for a 0DTE entry at the cutoff's ask to still mean anything.
F0DTE_WINDOW_LATE = "F0DTE_WINDOW_LATE"
F0DTE_CAP_REPEAT_CONTRACT = "F0DTE_CAP_REPEAT_CONTRACT"
F0DTE_CAP_UNDERLYING_HOUR = "F0DTE_CAP_UNDERLYING_HOUR"
F0DTE_CAP_WINDOW = "F0DTE_CAP_WINDOW"
F0DTE_CONFLICTING_DIRECTION = "F0DTE_CONFLICTING_DIRECTION"

ASK, BID, INSIDE = "AT_OR_ABOVE_ASK", "AT_OR_BELOW_BID", "INSIDE_SPREAD"


class F0DTEMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"
    ON = "ON"


def f0dte_mode(environ: Mapping[str, str] | None = None) -> F0DTEMode:
    """`REALTIME_V3_F0DTE`; unset or blank is OFF. Anything but OFF, RECORD or ON raises (never silently OFF)."""
    raw = (os.environ if environ is None else environ).get(F0DTE_SETTING)
    value = (raw or "").strip().upper() or F0DTEMode.OFF.value
    try:
        return F0DTEMode(value)
    except ValueError:
        raise ValueError(f"{F0DTE_SETTING} must be OFF, RECORD or ON, not {raw!r}") from None


# --- policy -----------------------------------------------------------------------------------------------------------

class F0DTEPolicy(Contract):
    """G106 D5: the 0DTE intraday lane's rule. Money in whole dollars, prices in cents, shares in percent."""

    version: Literal["f0dte_intraday_v1"] = "f0dte_intraday_v1"
    window_minutes: int = Field(default=5, gt=0, strict=True)
    skip_first_minutes: int = Field(default=15, ge=0, strict=True)
    no_entry_last_minutes: int = Field(default=90, ge=0, strict=True)
    index_underlyings: tuple[str, ...] = ("IWM", "QQQ", "SPY")
    volume_multiple_pct: int = Field(default=300, ge=0, strict=True)
    minimum_window_volume_index: int = Field(default=500, ge=0, strict=True)
    minimum_window_volume_stock: int = Field(default=200, ge=0, strict=True)
    minimum_window_premium_index: int = Field(default=100_000, ge=0, strict=True)
    minimum_window_premium_stock: int = Field(default=50_000, ge=0, strict=True)
    aggressor_dominance_pct: int = Field(default=200, ge=100, strict=True)
    minimum_bought_premium_index: int = Field(default=50_000, ge=0, strict=True)
    minimum_bought_premium_stock: int = Field(default=25_000, ge=0, strict=True)
    fresh_quote_ms: int = Field(default=1_000, ge=0, strict=True)
    stale_quote_ms: int = Field(default=5_000, ge=0, strict=True)
    minimum_mid_cents: int = Field(default=20, ge=0, strict=True)
    maximum_mid_cents: int = Field(default=1_000, ge=0, strict=True)
    maximum_spread_pct: int = Field(default=15, ge=0, strict=True)
    minimum_abs_delta_pct: int = Field(default=20, ge=0, le=100, strict=True)
    maximum_abs_delta_pct: int = Field(default=60, ge=0, le=100, strict=True)
    risk_free_rate_bp: int = Field(default=400, ge=0, strict=True)
    target_pct: int = Field(default=50, gt=0, strict=True)
    stop_pct: int = Field(default=35, gt=0, lt=100, strict=True)
    time_exit_minutes: int = Field(default=45, gt=0, strict=True)
    exit_before_close_minutes: int = Field(default=15, ge=0, strict=True)
    repeat_block_minutes: int = Field(default=30, ge=0, strict=True)
    maximum_window_delay_seconds: int = Field(default=360, gt=0, strict=True)
    maximum_alerts_per_underlying_per_hour: int = Field(default=2, ge=0, strict=True)
    maximum_alerts_per_window: int = Field(default=6, ge=0, strict=True)
    entry_basis: Literal["ASK_AT_DETECTION"] = "ASK_AT_DETECTION"
    print_basis: Literal["SESSION_JOURNAL_PRINTS_OF_THE_WINDOW_QUOTE_BEFORE_THE_PRINT_MILLISECOND"] = (
        "SESSION_JOURNAL_PRINTS_OF_THE_WINDOW_QUOTE_BEFORE_THE_PRINT_MILLISECOND")
    execution_permission: Literal[False] = False

    def is_index(self, underlyer: str) -> bool:
        return underlyer.upper() in self.index_underlyings


F0DTE_POLICY = F0DTEPolicy()

# Integer settings (unset = the default above).
F0DTE_INTEGER_SETTINGS = {
    "REALTIME_V3_F0DTE_VOLUME_MULTIPLE_PCT": "volume_multiple_pct",
    "REALTIME_V3_F0DTE_MIN_WINDOW_VOLUME_INDEX": "minimum_window_volume_index",
    "REALTIME_V3_F0DTE_MIN_WINDOW_VOLUME_STOCK": "minimum_window_volume_stock",
    "REALTIME_V3_F0DTE_MIN_WINDOW_PREMIUM_INDEX": "minimum_window_premium_index",
    "REALTIME_V3_F0DTE_MIN_WINDOW_PREMIUM_STOCK": "minimum_window_premium_stock",
    "REALTIME_V3_F0DTE_MIN_BOUGHT_PREMIUM_INDEX": "minimum_bought_premium_index",
    "REALTIME_V3_F0DTE_MIN_BOUGHT_PREMIUM_STOCK": "minimum_bought_premium_stock",
    "REALTIME_V3_F0DTE_MAX_ALERTS_PER_WINDOW": "maximum_alerts_per_window",
}


def f0dte_policy_from(environ: Mapping[str, str]) -> F0DTEPolicy:
    values = {}
    for key, name in F0DTE_INTEGER_SETTINGS.items():
        value = non_negative_int_setting(environ, key, getattr(F0DTE_POLICY, name))
        if value != getattr(F0DTE_POLICY, name):
            values[name] = value
    return F0DTE_POLICY if not values else F0DTEPolicy(**values)


@lru_cache(maxsize=1)
def f0dte_policy() -> F0DTEPolicy:
    """The process's policy, read once from the environment (the window service calls this at start)."""
    return f0dte_policy_from(os.environ)


# --- inputs -----------------------------------------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class F0DTEContract:
    instrument_id: int
    contract_id: int
    catalog_version_id: int
    contract_ticker: str
    underlyer: str
    contract_type: str  # CALL / PUT
    strike: Decimal
    expiration_date: date
    multiplier: int = 100

    @property
    def direction(self) -> int:
        return 1 if self.contract_type == "CALL" else -1


@dataclass(frozen=True, slots=True)
class F0DTEQuote:
    bid: Decimal
    ask: Decimal
    market_time: datetime | None = None

    @property
    def mid(self) -> Decimal:
        return (self.bid + self.ask) / 2


@dataclass(frozen=True, slots=True)
class F0DTETally:
    """One contract's volume-eligible prints in one window, by where they printed against the quote before them."""

    trades: int = 0
    volume: int = 0
    premium: Decimal = Decimal(0)
    ask_premium: Decimal = Decimal(0)
    bid_premium: Decimal = Decimal(0)
    inside_premium: Decimal = Decimal(0)
    low_confidence_premium: Decimal = Decimal(0)
    stale_premium: Decimal = Decimal(0)
    complex_premium: Decimal = Decimal(0)
    unquoted_premium: Decimal = Decimal(0)
    ask_volume: int = 0
    bid_volume: int = 0
    excluded_trades: int = 0
    first_trade_ms: int | None = None
    last_trade_ms: int | None = None
    last_price: Decimal | None = None
    inputs_reason: str | None = None

    def identity(self) -> dict:
        return {
            "trades": self.trades, "volume": self.volume, "premium": _money(self.premium),
            "ask_premium": _money(self.ask_premium), "bid_premium": _money(self.bid_premium),
            "inside_premium": _money(self.inside_premium), "low_premium": _money(self.low_confidence_premium),
            "stale_premium": _money(self.stale_premium), "complex_premium": _money(self.complex_premium),
            "unquoted_premium": _money(self.unquoted_premium), "ask_volume": self.ask_volume,
            "bid_volume": self.bid_volume, "excluded_trades": self.excluded_trades,
            "inputs_reason": self.inputs_reason,
        }


@dataclass(frozen=True, slots=True)
class F0DTEPriorAlert:
    contract_ticker: str
    underlyer: str
    triggered_at: datetime
    # Read back from a stored alert (ON), so a restarted window process can go on tracking its outcome.
    event_id: str | None = None
    instrument_id: int | None = None
    entry: Decimal | None = None
    target: Decimal | None = None
    stop: Decimal | None = None
    deadline: datetime | None = None
    direction: int | None = None


def _money(value: Decimal | None) -> str | None:
    return None if value is None else str(Decimal(value).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _price(value: Decimal | None) -> str | None:
    return None if value is None else str(Decimal(value).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))


# --- prints: the fold over one contract's journal events ------------------------------------------------------------

@dataclass(slots=True)
class _Acc:
    trades: int = 0
    volume: int = 0
    premium: Decimal = Decimal(0)
    ask: Decimal = Decimal(0)
    bid: Decimal = Decimal(0)
    inside: Decimal = Decimal(0)
    low: Decimal = Decimal(0)
    stale: Decimal = Decimal(0)
    complex: Decimal = Decimal(0)
    unquoted: Decimal = Decimal(0)
    ask_volume: int = 0
    bid_volume: int = 0
    excluded: int = 0
    first: int | None = None
    last: int | None = None
    last_price: Decimal | None = None

    def tally(self) -> F0DTETally:
        return F0DTETally(
            trades=self.trades, volume=self.volume, premium=self.premium, ask_premium=self.ask,
            bid_premium=self.bid, inside_premium=self.inside, low_confidence_premium=self.low,
            stale_premium=self.stale, complex_premium=self.complex, unquoted_premium=self.unquoted,
            ask_volume=self.ask_volume, bid_volume=self.bid_volume, excluded_trades=self.excluded,
            first_trade_ms=self.first, last_trade_ms=self.last, last_price=self.last_price)


@dataclass(frozen=True, slots=True)
class JournalEvent:
    """One normalized journal event of a contract: kind 'T' (print) or 'Q' (quote); time in ms since the epoch."""

    offset: int
    kind: str
    ms: int
    price: Decimal | None = None
    size: int | None = None
    bid: Decimal | None = None
    ask: Decimal | None = None
    conditions: tuple[int, ...] = ()


def _verdict(semantics, conditions: tuple[int, ...]):
    """(volume eligible, multi-leg, cross-or-floor) by the flow role's condition semantics; without semantics every
    print counts as a plain single-leg print (recorded as degraded by the caller)."""
    if semantics is None:
        return True, False, False
    verdict = semantics.classify(tuple(conditions))
    if not verdict.known or not verdict.volume_eligible:
        return False, False, False
    return True, verdict.is_multi_leg, (verdict.is_cross or verdict.is_floor)


def fold_contract_events(
    events: Iterable[JournalEvent],
    *,
    windows: Sequence[tuple[int, int]],
    multiplier: int,
    semantics=None,
    policy: F0DTEPolicy = F0DTE_POLICY,
) -> list[F0DTETally]:
    """The tally of each window (start_ms, end_ms] from one contract's events in (time, offset) order. A print is placed
    against the last quote stamped BEFORE its millisecond (quotes in the print's own millisecond come after it)."""
    accs = [_Acc() for _ in windows]
    if not windows:
        return []
    bounds = sorted(enumerate(windows), key=lambda item: item[1][0])
    starts = [start for _, (start, _end) in bounds]
    quote: tuple[Decimal, Decimal, int] | None = None
    pending: tuple[Decimal, Decimal, int] | None = None
    pending_ms: int | None = None
    mult = Decimal(multiplier)
    for event in events:
        if pending is not None and event.ms != pending_ms:
            quote, pending, pending_ms = pending, None, None
        if event.kind == "Q":
            if event.bid is not None and event.ask is not None:
                pending, pending_ms = (event.bid, event.ask, event.ms), event.ms
            continue
        if event.kind != "T" or event.price is None or not event.size:
            continue
        # The window (start, end] holding the print: the last window starting before it (windows do not overlap).
        slot = bisect_right(starts, event.ms - 1) - 1
        if slot < 0:
            continue
        index, (start, end) = bounds[slot]
        if not start < event.ms <= end:
            continue
        acc = accs[index]
        eligible, multi_leg, negotiated = _verdict(semantics, event.conditions)
        if not eligible:
            acc.excluded += 1
            continue
        size = int(event.size)
        premium = event.price * size * mult
        acc.trades += 1
        acc.volume += size
        acc.premium += premium
        acc.first = event.ms if acc.first is None else acc.first
        acc.last = event.ms
        acc.last_price = event.price
        if multi_leg:
            acc.complex += premium
            continue
        if quote is None:
            acc.unquoted += premium
            continue
        bid, ask, quoted_ms = quote
        if ask <= 0 or bid >= ask:
            acc.unquoted += premium
            continue
        age = event.ms - quoted_ms
        if age > policy.stale_quote_ms:
            acc.stale += premium
            continue
        if event.price >= ask:
            side = ASK
        elif bid > 0 and event.price <= bid:
            side = BID
        else:
            acc.inside += premium
            continue
        if negotiated:
            acc.low += premium
            continue
        if side == ASK:
            acc.ask += premium
            acc.ask_volume += size
        else:
            acc.bid += premium
            acc.bid_volume += size
    return [acc.tally() for acc in accs]


# --- prints: the journal read -----------------------------------------------------------------------------------------

_EVENTS_SQL = (
    "SELECT event_offset,event_kind,source_timestamp,price_text,size_text,bid_text,ask_text,conditions_json "
    "FROM normalized_events INDEXED BY idx_normalized_event_instrument_time "
    "WHERE instrument_id=? AND source_timestamp>? AND source_timestamp<=? {barrier}"
    "ORDER BY source_timestamp,event_offset"
)
_QUOTE_LOOKBACK_MS = 60_000


def _decimal(text) -> Decimal | None:
    if text is None or text == "":
        return None
    value = Decimal(str(text))
    return value if value.is_finite() else None


def iter_journal_events(connection, instrument_id: int, start_ms: int, end_ms: int,
                        barrier_offset: int | None = None) -> Iterable[JournalEvent]:
    """One contract's quotes and prints stamped in (start_ms, end_ms], in (time, offset) order (index walk), up to the
    barrier offset when given. Quotes carry only bid / ask; prints their price, size and conditions."""
    statement = _EVENTS_SQL.format(barrier="" if barrier_offset is None else "AND event_offset<=? ")
    parameters = (instrument_id, start_ms, end_ms) + (() if barrier_offset is None else (barrier_offset,))
    for offset, kind, ms, price, size, bid, ask, conditions in connection.execute(statement, parameters):
        if kind == "Q":
            yield JournalEvent(offset=offset, kind="Q", ms=ms, bid=_decimal(bid), ask=_decimal(ask))
        elif kind == "T":
            size_value = _decimal(size)
            yield JournalEvent(
                offset=offset, kind="T", ms=ms, price=_decimal(price),
                size=None if size_value is None else int(size_value),
                conditions=tuple(int(code) for code in json.loads(conditions or "[]")),
            )


def journal_events(connection, instrument_id: int, start_ms: int, end_ms: int,
                   barrier_offset: int | None = None) -> list[JournalEvent]:
    return list(iter_journal_events(connection, instrument_id, start_ms, end_ms, barrier_offset))


class F0DTEPrintsUnavailable(RuntimeError):
    pass


def read_window_tallies(
    path: str | Path,
    *,
    contracts: Sequence[F0DTEContract],
    window_start: datetime,
    window_end: datetime,
    barrier_offset: int | None,
    journal_generation: UUID | None,
    semantics=None,
    policy: F0DTEPolicy = F0DTE_POLICY,
    time_budget_seconds: float = 20.0,
    timeout_seconds: float = 5.0,
) -> tuple[dict[str, F0DTETally], dict]:
    """Each contract's tally of the window (window_start, window_end] from the session journal (read-only), and read
    stats. Raises F0DTEPrintsUnavailable when the journal cannot be read (missing, other generation, over budget)."""
    from .journal import _connect

    started = time.perf_counter()
    journal = Path(path)
    if not journal.is_file():
        raise F0DTEPrintsUnavailable("JOURNAL_FILE_MISSING")
    over_budget = False

    def _budget() -> int:
        nonlocal over_budget
        over_budget = time.perf_counter() - started > time_budget_seconds
        return 1 if over_budget else 0

    start_ms = int(window_start.timestamp() * 1000)
    end_ms = int(window_end.timestamp() * 1000)
    tallies: dict[str, F0DTETally] = {}
    events_read = 0
    connection = None
    try:
        connection = _connect(journal, timeout_seconds=timeout_seconds, read_only=True)
        connection.set_progress_handler(_budget, 10_000)
        row = connection.execute("SELECT journal_generation FROM journal_metadata WHERE singleton=1").fetchone()
        if journal_generation is not None and (row is None or UUID(str(row[0])) != journal_generation):
            raise F0DTEPrintsUnavailable("JOURNAL_GENERATION_MISMATCH")
        for contract in contracts:
            counted = [0]

            def _counted(rows):
                for row in rows:
                    counted[0] += 1
                    yield row

            tallies[contract.contract_ticker] = fold_contract_events(
                _counted(iter_journal_events(connection, contract.instrument_id, start_ms - _QUOTE_LOOKBACK_MS, end_ms,
                                             barrier_offset)),
                windows=((start_ms, end_ms),), multiplier=contract.multiplier, semantics=semantics, policy=policy)[0]
            events_read += counted[0]
    except F0DTEPrintsUnavailable:
        raise
    except (sqlite3.Error, OSError, ValueError, KeyError, TypeError) as error:
        if over_budget:
            raise F0DTEPrintsUnavailable(f"READ_TIME_BUDGET_EXCEEDED:{time_budget_seconds}s") from error
        raise F0DTEPrintsUnavailable(f"{type(error).__name__}:{error}"[:200]) from error
    finally:
        if connection is not None:
            try:
                connection.set_progress_handler(None, 0)
                connection.close()
            except Exception:  # noqa: BLE001
                pass
    return tallies, {"events_read": events_read, "read_ms": round((time.perf_counter() - started) * 1000, 1)}


# --- Black-Scholes delta from the mid --------------------------------------------------------------------------------

def _ncdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _bs_price(spot: float, strike: float, years: float, rate: float, vol: float, call: bool) -> float:
    root = vol * math.sqrt(years)
    d1 = (math.log(spot / strike) + (rate + 0.5 * vol * vol) * years) / root
    d2 = d1 - root
    discount = math.exp(-rate * years)
    if call:
        return spot * _ncdf(d1) - strike * discount * _ncdf(d2)
    return strike * discount * _ncdf(-d2) - spot * _ncdf(-d1)


def implied_delta(price: Decimal, spot: Decimal, strike: Decimal, years: float, *, call: bool,
                  rate: float = 0.04) -> tuple[float, float] | None:
    """(delta, implied vol) of a European option priced at `price`; None when no volatility in [0.5%, 2000%] prices
    it (a price at or under intrinsic, or above the spot/strike bound)."""
    try:
        p, s, k = float(price), float(spot), float(strike)
    except (TypeError, ValueError):
        return None
    if not (p > 0 and s > 0 and k > 0 and years > 0):
        return None
    low, high = 0.005, 20.0
    f_low = _bs_price(s, k, years, rate, low, call) - p
    f_high = _bs_price(s, k, years, rate, high, call) - p
    if f_low > 0 or f_high < 0:
        return None
    for _ in range(100):
        mid = 0.5 * (low + high)
        if _bs_price(s, k, years, rate, mid, call) - p > 0:
            high = mid
        else:
            low = mid
        if high - low < 1e-6:
            break
    vol = 0.5 * (low + high)
    d1 = (math.log(s / k) + (rate + 0.5 * vol * vol) * years) / (vol * math.sqrt(years))
    delta = _ncdf(d1) if call else _ncdf(d1) - 1.0
    return delta, vol


# --- the rule ---------------------------------------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class F0DTECandidate:
    contract: F0DTEContract
    tally: F0DTETally | None
    day_volume: int | None
    average_window_volume: Decimal | None
    volume_threshold: int | None
    premium_threshold: int
    aggressor_class: str  # BOUGHT / SOLD / UNCLEAR
    quote: F0DTEQuote | None
    spread_pct: Decimal | None
    delta: float | None
    implied_vol: float | None
    spot: Decimal | None
    spot_before: Decimal | None
    stock_move: str  # SAME / OPPOSITE / FLAT / UNKNOWN (for the contract's bought direction)
    reasons: tuple[str, ...]

    @property
    def eligible(self) -> bool:
        return not self.reasons

    @property
    def direction(self) -> int:
        return self.contract.direction

    @property
    def bought_premium(self) -> Decimal:
        return Decimal(0) if self.tally is None else self.tally.ask_premium

    def document(self, *, verdict: str) -> dict:
        tally = self.tally
        quote = self.quote
        return {
            "contract_ticker": self.contract.contract_ticker,
            "underlyer": self.contract.underlyer,
            "type": self.contract.contract_type,
            "strike": str(self.contract.strike),
            "verdict": verdict,
            "reasons": list(self.reasons),
            "aggressor": self.aggressor_class,
            "direction": self.direction if self.aggressor_class == "BOUGHT" else None,
            "trades": None if tally is None else tally.trades,
            "volume": None if tally is None else tally.volume,
            "average_window_volume": None if self.average_window_volume is None else str(
                self.average_window_volume.quantize(Decimal("0.1"))),
            "volume_threshold": self.volume_threshold,
            "day_volume": self.day_volume,
            "premium": None if tally is None else _money(tally.premium),
            "ask_premium": None if tally is None else _money(tally.ask_premium),
            "bid_premium": None if tally is None else _money(tally.bid_premium),
            "inside_premium": None if tally is None else _money(tally.inside_premium),
            "other_premium": None if tally is None else _money(
                tally.low_confidence_premium + tally.stale_premium + tally.complex_premium + tally.unquoted_premium),
            "bid": None if quote is None else _price(quote.bid),
            "ask": None if quote is None else _price(quote.ask),
            "spread_pct": None if self.spread_pct is None else str(self.spread_pct.quantize(Decimal("0.1"))),
            "delta": None if self.delta is None else round(self.delta, 3),
            "iv": None if self.implied_vol is None else round(self.implied_vol, 3),
            "spot": None if self.spot is None else _price(self.spot),
            "strike_from_spot_pct": None if not self.spot else str(
                ((self.contract.strike - self.spot) / self.spot * 100).quantize(Decimal("0.01"))),
            "stock_move_5m": self.stock_move,
        }


@dataclass(frozen=True, slots=True)
class F0DTEWindow:
    cutoff: datetime
    window_start: datetime
    in_entry_time: bool
    candidates: tuple[F0DTECandidate, ...]
    alerts: tuple[F0DTECandidate, ...]
    capped: tuple[tuple[F0DTECandidate, str], ...]


def classify_window_aggressor(tally: F0DTETally | None, *, index: bool,
                              policy: F0DTEPolicy = F0DTE_POLICY) -> str:
    if tally is None or tally.inputs_reason is not None:
        return "UNCLEAR"
    minimum = policy.minimum_bought_premium_index if index else policy.minimum_bought_premium_stock
    ask, bid = tally.ask_premium, tally.bid_premium
    for side, mine, other in (("BOUGHT", ask, bid), ("SOLD", bid, ask)):
        if mine > other and mine * 100 >= policy.aggressor_dominance_pct * other and mine >= minimum and mine > 0:
            return side
    return "UNCLEAR"


def entry_time_bounds(session_open: datetime, session_close: datetime,
                      policy: F0DTEPolicy = F0DTE_POLICY) -> tuple[datetime, datetime]:
    return (session_open + timedelta(minutes=policy.skip_first_minutes),
            session_close - timedelta(minutes=policy.no_entry_last_minutes))


def exit_deadline(cutoff: datetime, session_close: datetime, policy: F0DTEPolicy = F0DTE_POLICY) -> datetime:
    return min(cutoff + timedelta(minutes=policy.time_exit_minutes),
               session_close - timedelta(minutes=policy.exit_before_close_minutes))


def evaluate_f0dte_window(
    *,
    cutoff: datetime,
    session_open: datetime,
    session_close: datetime,
    contracts: Sequence[F0DTEContract],
    tallies: Mapping[str, F0DTETally],
    quotes: Mapping[str, F0DTEQuote],
    day_volumes: Mapping[str, int],
    spot: Mapping[str, Decimal],
    spot_before: Mapping[str, Decimal],
    prior_alerts: Sequence[F0DTEPriorAlert] = (),
    universe: frozenset[str] | None = None,
    prints_reason: str | None = None,
    unpackageable: frozenset[str] = frozenset(),
    flow_evidence_available: bool = True,
    window_late: bool = False,
    policy: F0DTEPolicy = F0DTE_POLICY,
) -> F0DTEWindow:
    """The window's verdict on every 0DTE contract and its alerts after the caps. Pure."""
    window = timedelta(minutes=policy.window_minutes)
    window_start = cutoff - window
    first_entry, last_entry = entry_time_bounds(session_open, session_close, policy)
    in_entry_time = first_entry <= cutoff <= last_entry
    years = max((session_close - cutoff).total_seconds(), 0.0) / _YEAR_SECONDS
    prior_windows = max(int((window_start - session_open) / window), 1)
    candidates = []
    for contract in sorted(contracts, key=lambda row: (row.underlyer, row.contract_ticker)):
        reasons: list[str] = []
        index = policy.is_index(contract.underlyer)
        if not in_entry_time:
            reasons.append(F0DTE_OUTSIDE_ENTRY_TIME)
        if universe is not None and contract.underlyer not in universe:
            reasons.append(F0DTE_OUTSIDE_UNIVERSE)
        if contract.contract_ticker in unpackageable:
            reasons.append(F0DTE_UNPACKAGEABLE)
        if not flow_evidence_available:
            reasons.append(F0DTE_NO_FLOW_EVIDENCE)
        if window_late:
            reasons.append(F0DTE_WINDOW_LATE)
        tally = None if prints_reason is not None else tallies.get(contract.contract_ticker)
        if prints_reason is not None or tally is None or tally.inputs_reason is not None:
            reasons.append(F0DTE_PRINTS_UNAVAILABLE)
        elif tally.trades == 0:
            reasons.append(F0DTE_NO_PRINTS)
        day_volume = day_volumes.get(contract.contract_ticker)
        average = threshold = None
        floor = policy.minimum_window_volume_index if index else policy.minimum_window_volume_stock
        if tally is not None and tally.trades:
            if day_volume is None:
                reasons.append(F0DTE_DAY_VOLUME_UNAVAILABLE)
            else:
                before = max(int(day_volume) - tally.volume, 0)
                average = Decimal(before) / prior_windows
                threshold = max(int((average * policy.volume_multiple_pct / 100).to_integral_value(
                    rounding="ROUND_CEILING")), floor)
                if tally.volume < threshold:
                    reasons.append(F0DTE_VOLUME_BELOW_THRESHOLD)
        premium_floor = policy.minimum_window_premium_index if index else policy.minimum_window_premium_stock
        if tally is not None and tally.trades and tally.premium < premium_floor:
            reasons.append(F0DTE_PREMIUM_BELOW_THRESHOLD)
        aggressor = classify_window_aggressor(tally, index=index, policy=policy) if tally is not None else "UNCLEAR"
        if tally is not None and tally.trades:
            if aggressor == "SOLD":
                reasons.append(F0DTE_SOLD)
            elif aggressor == "UNCLEAR":
                reasons.append(F0DTE_AGGRESSOR_UNCLEAR)
        quote = quotes.get(contract.contract_ticker)
        spread_pct = delta = vol = None
        if quote is None or quote.bid <= 0 or quote.ask <= quote.bid:
            reasons.append(F0DTE_QUOTE_UNAVAILABLE)
        else:
            mid = quote.mid
            spread_pct = (quote.ask - quote.bid) / mid * 100
            if not (Decimal(policy.minimum_mid_cents) / 100 <= mid <= Decimal(policy.maximum_mid_cents) / 100):
                reasons.append(F0DTE_MID_OUTSIDE_RANGE)
            if spread_pct > policy.maximum_spread_pct:
                reasons.append(F0DTE_SPREAD_TOO_WIDE)
            price_now = spot.get(contract.underlyer)
            if price_now is None:
                reasons.append(F0DTE_SPOT_UNAVAILABLE)
            else:
                solved = implied_delta(mid, price_now, contract.strike, years, call=contract.contract_type == "CALL",
                                       rate=policy.risk_free_rate_bp / 10_000)
                if solved is None:
                    reasons.append(F0DTE_DELTA_UNAVAILABLE)
                else:
                    delta, vol = solved
                    if not (policy.minimum_abs_delta_pct <= abs(delta) * 100 <= policy.maximum_abs_delta_pct):
                        reasons.append(F0DTE_DELTA_OUTSIDE_RANGE)
        now, before = spot.get(contract.underlyer), spot_before.get(contract.underlyer)
        if now is None or before is None:
            move = "UNKNOWN"
        elif now == before:
            move = "FLAT"
        else:
            move = "SAME" if (1 if now > before else -1) == contract.direction else "OPPOSITE"
        candidates.append(F0DTECandidate(
            contract=contract, tally=tally, day_volume=None if day_volume is None else int(day_volume),
            average_window_volume=average, volume_threshold=threshold, premium_threshold=premium_floor,
            aggressor_class=aggressor, quote=quote, spread_pct=spread_pct, delta=delta, implied_vol=vol,
            spot=now, spot_before=before, stock_move=move, reasons=tuple(reasons)))
    alerts, capped = _apply_caps(cutoff, [row for row in candidates if row.eligible], prior_alerts, policy)
    return F0DTEWindow(cutoff=cutoff, window_start=window_start, in_entry_time=in_entry_time,
                       candidates=tuple(candidates), alerts=tuple(alerts), capped=tuple(capped))


def _apply_caps(cutoff, eligible, prior_alerts, policy):
    ranked = sorted(eligible, key=lambda row: (-row.bought_premium, row.contract.contract_ticker))
    capped: list[tuple[F0DTECandidate, str]] = []
    # Both directions on one underlying in one window: keep the stronger side.
    best_direction: dict[str, int] = {}
    for row in ranked:
        best_direction.setdefault(row.contract.underlyer, row.direction)
    kept = []
    for row in ranked:
        if row.direction != best_direction[row.contract.underlyer]:
            capped.append((row, F0DTE_CONFLICTING_DIRECTION))
        else:
            kept.append(row)
    repeat_since = cutoff - timedelta(minutes=policy.repeat_block_minutes)
    hour_since = cutoff - timedelta(minutes=60)
    recent = [row for row in prior_alerts if row.triggered_at < cutoff]
    alerts: list[F0DTECandidate] = []
    for row in kept:
        ticker, underlyer = row.contract.contract_ticker, row.contract.underlyer
        if any(prior.contract_ticker == ticker and prior.triggered_at > repeat_since for prior in recent):
            capped.append((row, F0DTE_CAP_REPEAT_CONTRACT))
            continue
        in_hour = sum(prior.underlyer == underlyer and prior.triggered_at > hour_since for prior in recent)
        in_hour += sum(alert.contract.underlyer == underlyer for alert in alerts)
        if in_hour >= policy.maximum_alerts_per_underlying_per_hour:
            capped.append((row, F0DTE_CAP_UNDERLYING_HOUR))
            continue
        if len(alerts) >= policy.maximum_alerts_per_window:
            capped.append((row, F0DTE_CAP_WINDOW))
            continue
        alerts.append(row)
    return alerts, capped


# --- the alert: event, package, evaluation payload --------------------------------------------------------------------

def f0dte_event_id(cohort_id, contract_ticker: str, cutoff: datetime) -> UUID:
    """Never a flow event id: the O1 loader and the session alert's event anchor stay apart from O1's."""
    return uuid5(NAMESPACE_URL, f"stockscreener:f0dte-event:v1:{cohort_id}:{contract_ticker}:{cutoff.isoformat()}")


def f0dte_candidate_id(cohort_id, event_id: UUID) -> UUID:
    return uuid5(NAMESPACE_URL, f"stockscreener:f0dte-package:v1:{cohort_id}:{event_id}")


def management_policy(policy: F0DTEPolicy = F0DTE_POLICY) -> dict:
    """The keys the portal's plan cells read (`optionPlanTerms`): policy_version, stop_loss_fraction,
    take_profit_fraction, maximum_hold_seconds; plus the F0DTE terms."""
    return {
        "policy_version": F0DTE_MANAGEMENT_POLICY_VERSION,
        "strategy_name": F0DTE_CATEGORY,
        "take_profit_fraction": str(Decimal(policy.target_pct) / 100),
        "stop_loss_fraction": str(Decimal(policy.stop_pct) / 100),
        "maximum_hold_seconds": policy.time_exit_minutes * 60,
        "exit_before_close_minutes": policy.exit_before_close_minutes,
        "entry_basis": policy.entry_basis,
        "mark_basis": "NBBO_MIDPOINT",
    }


def f0dte_package(candidate: F0DTECandidate, *, cohort_id: UUID, matrix_id: UUID, cutoff: datetime,
                  session_close: datetime, valuation_policy_sha256: str, policy: F0DTEPolicy = F0DTE_POLICY):
    """The alert's single-leg long package: BUY 1 at the ask at detection, exit deadline = the time exit."""
    from options.domain import ContractType
    from options.strategies.domain import OptionSide

    from .contracts import canonical_sha256
    from .option_portal_adapter import RealtimeOptionPackage, RealtimeOptionPackageLeg
    from .preflight import ProviderTier

    contract, quote = candidate.contract, candidate.quote
    if quote is None or quote.ask <= 0:
        raise ValueError("F0DTE package needs an ask")
    event_id = f0dte_event_id(cohort_id, contract.contract_ticker, cutoff)
    quoted_at = quote.market_time if quote.market_time is not None and quote.market_time <= cutoff else cutoff
    premium = quote.ask * contract.multiplier
    identity = {
        "policy": policy.version, "policy_sha256": policy.sha256, "cohort_id": str(cohort_id),
        "event_id": str(event_id), "contract_id": contract.contract_id,
        "catalog_version_id": contract.catalog_version_id, "entry_mark": str(quote.ask),
        "entry_market_time": quoted_at.isoformat(),
    }
    deadline = exit_deadline(cutoff, session_close, policy)
    return RealtimeOptionPackage(
        candidate_id=f0dte_candidate_id(cohort_id, event_id),
        event_id=event_id,
        matrix_id=matrix_id,
        candidate_identity_sha256=canonical_sha256(identity),
        cohort_id=cohort_id,
        provider_tier=ProviderTier.MASSIVE_ADVANCED,
        entry_market_time=quoted_at,
        # Package premium convention (options/outcomes.py): BUY is a debit, negative.
        entry_net_premium=-premium,
        capital_at_risk=premium,
        exit_deadline=deadline if deadline > quoted_at else None,
        legs=(RealtimeOptionPackageLeg(
            leg_index=0,
            instrument_id=contract.instrument_id,
            contract_id=contract.contract_id,
            catalog_version_id=contract.catalog_version_id,
            contract_ticker=contract.contract_ticker,
            side=OptionSide.BUY,
            ratio=1,
            multiplier=contract.multiplier,
            strike=contract.strike,
            expiration_date=contract.expiration_date,
            contract_type=ContractType.CALL if contract.contract_type == "CALL" else ContractType.PUT,
            entry_mark=quote.ask,
            entry_market_time=quoted_at,
            valuation_policy_sha256=valuation_policy_sha256,
        ),),
    )


def f0dte_payload(candidate: F0DTECandidate, *, cutoff: datetime, session_close: datetime,
                  policy: F0DTEPolicy = F0DTE_POLICY) -> dict:
    """The evaluation payload of an F0DTE alert (the projection reads `contract`, `detector_id`, `category`,
    `direction`, `management_policy`, `entry_limit`, `structure_type`)."""
    quote = candidate.quote
    document = candidate.document(verdict="ALERT")
    document.update({
        "contract": F0DTE_EVALUATION_CONTRACT,
        "detector_id": F0DTE_DETECTOR_ID,
        "category": F0DTE_CATEGORY,
        "policy": policy.version,
        "policy_sha256": policy.sha256,
        "cutoff": cutoff.isoformat(),
        "window_start": (cutoff - timedelta(minutes=policy.window_minutes)).isoformat(),
        "direction": candidate.direction,
        "structure_type": "LONG_CALL" if candidate.contract.contract_type == "CALL" else "LONG_PUT",
        "instrument_id": candidate.contract.instrument_id,
        "entry_limit": _price(quote.ask),
        "entry_limit_per_package": str((quote.ask * candidate.contract.multiplier).quantize(Decimal("0.01"))),
        "target_price": _price(quote.ask * (100 + policy.target_pct) / 100),
        "stop_price": _price(quote.ask * (100 - policy.stop_pct) / 100),
        "exit_deadline": exit_deadline(cutoff, session_close, policy).isoformat(),
        "management_policy": management_policy(policy),
        "execution_permission": False,
    })
    return document


# --- RECORD documents -----------------------------------------------------------------------------------------------

def _row(candidate: F0DTECandidate, verdict: str, reasons: Sequence[str]) -> dict:
    """A contract's compact row on the window line (a line over 16 KiB is split by Docker)."""
    tally, quote = candidate.tally, candidate.quote
    return {
        "ticker": candidate.contract.contract_ticker.removeprefix("O:"),
        "verdict": verdict,
        "reasons": list(reasons),
        "aggressor": candidate.aggressor_class,
        "trades": None if tally is None else tally.trades,
        "volume": None if tally is None else tally.volume,
        "threshold": candidate.volume_threshold,
        "premium": None if tally is None else int(_money(tally.premium)),
        "ask_premium": None if tally is None else int(_money(tally.ask_premium)),
        "bid_premium": None if tally is None else int(_money(tally.bid_premium)),
        "inside_premium": None if tally is None else int(_money(tally.inside_premium)),
        "quote": None if quote is None else f"{quote.bid}/{quote.ask}",
        "spread_pct": None if candidate.spread_pct is None else float(candidate.spread_pct.quantize(Decimal("0.1"))),
        "delta": None if candidate.delta is None else round(candidate.delta, 2),
        "stock_5m": candidate.stock_move,
    }


def f0dte_window_document(result: F0DTEWindow, *, mode: F0DTEMode, policy: F0DTEPolicy, status: str = "OK",
                          prints_reason: str | None = None, read_stats: Mapping | None = None,
                          prior_alerts: int = 0) -> dict:
    """The one F0DTE_RECORD line of a window: counts, the would-alert list and, in the entry time, one compact row per
    0DTE contract with the reasons it was rejected (capped rows carry the cap)."""
    alert_ids = {row.contract.contract_ticker for row in result.alerts}
    capped = {row.contract.contract_ticker: reason for row, reason in result.capped}
    rows = []
    reasons = Counter()
    for row in result.candidates:
        ticker = row.contract.contract_ticker
        verdict = "ALERT" if ticker in alert_ids else "CAPPED" if ticker in capped else (
            "ELIGIBLE" if row.eligible else "REJECTED")
        row_reasons = [capped[ticker]] if ticker in capped else list(row.reasons)
        reasons.update(row_reasons)
        rows.append(_row(row, verdict, row_reasons))
    order = {"ALERT": 0, "CAPPED": 1, "REJECTED": 2, "ELIGIBLE": 3}
    rows.sort(key=lambda item: (order[item["verdict"]], -(item["premium"] or 0), item["ticker"]))
    if not result.in_entry_time:
        rows = []  # nothing was read: every contract is outside the entry time
    return {
        "contract": "f0dte-record-v1",
        "mode": mode.value,
        "status": status,
        "applied": mode is F0DTEMode.ON,
        "policy": policy.version,
        "policy_sha256": policy.sha256,
        "cutoff": result.cutoff.isoformat(),
        "window_start": result.window_start.isoformat(),
        "in_entry_time": result.in_entry_time,
        "prints_reason": prints_reason,
        "read": dict(read_stats or {}),
        "contracts": len(result.candidates),
        "with_prints": sum(1 for row in result.candidates if row.tally is not None and row.tally.trades),
        "by_aggressor": dict(sorted(Counter(row.aggressor_class for row in result.candidates
                                            if row.tally is not None and row.tally.trades).items())),
        "eligible": sum(1 for row in result.candidates if row.eligible),
        "alerts": len(result.alerts),
        "capped": len(result.capped),
        "prior_alerts_considered": prior_alerts,
        "reasons": dict(sorted(reasons.items())),
        "would_alert": [row.contract.contract_ticker for row in result.alerts],
        "rows_listed": min(len(rows), _RECORD_CONTRACT_LIMIT),
        "rows": rows[:_RECORD_CONTRACT_LIMIT],
    }


def _print_line(event: str, document: Mapping[str, object]) -> None:
    print(f"{event} {json.dumps(document, sort_keys=True, default=str)}", file=sys.stderr, flush=True)


# --- outcome tracking (in process) -------------------------------------------------------------------------------------

@dataclass(slots=True)
class _Tracked:
    event_id: str
    contract_ticker: str
    instrument_id: int
    underlyer: str
    direction: int
    entry: Decimal
    triggered_at: datetime
    deadline: datetime
    target: Decimal
    stop: Decimal
    mode: str
    horizons_done: set = field(default_factory=set)
    exit_done: bool = False


class F0DTEState:
    """The process's would-alerts (RECORD) and alerts (ON) of the session: the caps' memory and the outcome tracker."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.session: date | None = None
        self.alerts: dict[tuple[str, str], F0DTEPriorAlert] = {}
        self.tracked: dict[str, _Tracked] = {}
        self.finished: set[str] = set()
        self.windows: dict[str, dict] = {}

    def reset_for(self, session: date) -> None:
        if self.session != session:
            self.session = session
            self.alerts.clear()
            self.tracked.clear()
            self.finished.clear()
            self.windows.clear()

    def remember(self, row: F0DTECandidate, cutoff: datetime) -> None:
        key = (row.contract.contract_ticker, cutoff.isoformat())
        self.alerts.setdefault(key, F0DTEPriorAlert(row.contract.contract_ticker, row.contract.underlyer, cutoff))

    def prior(self) -> list[F0DTEPriorAlert]:
        return list(self.alerts.values())


_STATE = F0DTEState()


def outcome_documents(state: F0DTEState, *, cutoff: datetime, mids: Mapping[int, F0DTEQuote],
                      policy: F0DTEPolicy = F0DTE_POLICY) -> list[dict]:
    """One F0DTE_OUTCOME document per horizon reached (+5/+15/+30/+45 min, the first window at or after it) and one at
    the managed exit (first window whose mid reaches the target or the stop, or the exit deadline). Marks are the
    window's cutoff marks (5-minute resolution); the end-of-day analysis can refine them with rt.option_mark_evidence."""
    documents = []
    for key, row in list(state.tracked.items()):
        if cutoff <= row.triggered_at:
            continue
        quote = mids.get(row.instrument_id)
        mid = None if quote is None else quote.mid
        elapsed = int((cutoff - row.triggered_at).total_seconds() // 60)

        def document(kind, label):
            return {
                "contract": "f0dte-outcome-v1", "mode": row.mode, "kind": kind, "label": label,
                "event_id": row.event_id, "contract_ticker": row.contract_ticker, "underlyer": row.underlyer,
                "direction": row.direction, "triggered_at": row.triggered_at.isoformat(),
                "cutoff": cutoff.isoformat(), "elapsed_minutes": elapsed, "entry": _price(row.entry),
                "bid": None if quote is None else _price(quote.bid), "ask": None if quote is None else _price(quote.ask),
                "mid": None if mid is None else _price(mid),
                "return_mid_pct": None if mid is None else str(((mid - row.entry) / row.entry * 100).quantize(
                    Decimal("0.1"))),
                "return_bid_pct": None if quote is None else str(((quote.bid - row.entry) / row.entry * 100).quantize(
                    Decimal("0.1"))),
            }

        for horizon in _HORIZONS_MINUTES:
            if horizon not in row.horizons_done and elapsed >= horizon:
                row.horizons_done.add(horizon)
                documents.append(document("HORIZON", f"+{horizon}m"))
        if not row.exit_done:
            label = None
            if mid is not None and mid >= row.target:
                label = "TARGET"
            elif mid is not None and mid <= row.stop:
                label = "STOP"
            elif cutoff >= row.deadline:
                label = "TIME"
            if label is not None:
                row.exit_done = True
                documents.append(document("EXIT", label))
        if row.exit_done and len(row.horizons_done) == len(_HORIZONS_MINUTES):
            state.tracked.pop(key, None)
            state.finished.add(key)
    return documents


# --- the window hook ---------------------------------------------------------------------------------------------------

_PRIOR_ALERTS_SQL = (
    "SELECT evaluation.payload->>'contract_ticker' AS contract_ticker,evaluation.underlyer,evaluation.observed_at,"
    "evaluation.event_id::text AS event_id,evaluation.payload->>'instrument_id' AS instrument_id,"
    "evaluation.payload->>'entry_limit' AS entry,evaluation.payload->>'target_price' AS target,"
    "evaluation.payload->>'stop_price' AS stop,evaluation.payload->>'exit_deadline' AS deadline,"
    "evaluation.payload->>'direction' AS direction "
    "FROM rt.detector_datasets AS dataset JOIN rt.detector_evaluations AS evaluation "
    "ON evaluation.dataset_id=dataset.dataset_id AND evaluation.detector_id='O1' "
    "WHERE dataset.cohort_id=%s AND dataset.session_date=%s AND dataset.provider_tier='MASSIVE_ADVANCED' "
    "AND evaluation.selection_status='SELECTED' AND evaluation.payload->>'contract'=%s "
    "AND evaluation.observed_at<%s AND evaluation.observed_at>=%s"
)
# The previous window's rolling state (its underlying ticks): the newest one committed before this window's state.
_SPOT_BEFORE_SQL = (
    "SELECT state_document->'underlying_ticks' AS ticks,committed_at FROM rt.advanced_options_rolling_checkpoints "
    "WHERE cohort_id=%s AND session_date=%s AND committed_at<%s AND committed_at>=%s AND state_id<>%s "
    "ORDER BY committed_at DESC LIMIT 1"
)
_NO_STATE = "00000000-0000-0000-0000-000000000000"
# The window's package evidence reads the cohort's stored prints up to its committed offset (p8 window
# _flow_evidence); an F0DTE evidence carries one of them.
_FLOW_EVIDENCE_SQL = "SELECT 1 FROM rt.option_flow_events WHERE cohort_id=%s AND source_offset<=%s LIMIT 1"


def _tick_price(row) -> Decimal | None:
    """An underlying tick's price: the snapshot evidence's price, else its trade price, else its quote midpoint."""
    evidence = getattr(row, "evidence", None)
    if evidence is not None and getattr(evidence, "price", None) is not None:
        return Decimal(str(evidence.price))
    trade = getattr(row, "trade", None)
    if trade is not None and getattr(trade, "price", None) is not None:
        return Decimal(str(trade.price))
    quote = getattr(row, "quote", None)
    if quote is not None and getattr(quote, "bid", None) and getattr(quote, "ask", None):
        return (Decimal(str(quote.bid)) + Decimal(str(quote.ask))) / 2
    return None


def _stored_prior_alert(item) -> F0DTEPriorAlert:
    """A stored F0DTE alert (its evaluation row and payload); the tracking fields stay None when unreadable."""
    base = F0DTEPriorAlert(str(item["contract_ticker"]), str(item["underlyer"]), item["observed_at"])
    try:
        return F0DTEPriorAlert(
            base.contract_ticker, base.underlyer, base.triggered_at, event_id=str(item["event_id"]),
            instrument_id=int(item["instrument_id"]), entry=Decimal(str(item["entry"])),
            target=Decimal(str(item["target"])), stop=Decimal(str(item["stop"])),
            deadline=datetime.fromisoformat(str(item["deadline"])), direction=int(item["direction"]))
    except (KeyError, TypeError, ValueError, ArithmeticError):
        return base


def _db_reads(provider, *, cohort_id, session_date, cutoff, mode: F0DTEMode, window: timedelta,
              current_state_id=None, current_committed_at=None, committed_offset=None):
    """(spot at the previous window by underlyer, prior F0DTE alerts in the last hour (ON), whether the session has a
    stored flow event up to this window's committed offset). Read-only, small."""
    from .database_access_v2 import APP_ROLE

    spot_before: dict[str, Decimal] = {}
    prior: list[F0DTEPriorAlert] = []
    flow_evidence = False
    before = current_committed_at if current_committed_at is not None else cutoff - window + timedelta(seconds=1)
    with provider.transaction(APP_ROLE, read_only=True, dict_cursor=True) as unit:
        unit.cursor.execute(_SPOT_BEFORE_SQL, (str(cohort_id), session_date, before, cutoff - 3 * window,
                                               str(current_state_id or _NO_STATE)))
        row = unit.cursor.fetchone()
        if row is not None:
            for tick in row["ticks"] or ():
                evidence = tick.get("evidence") or {}
                if evidence.get("ticker") and evidence.get("price") is not None:
                    spot_before[str(evidence["ticker"]).upper()] = Decimal(str(evidence["price"]))
        if mode is F0DTEMode.ON:
            unit.cursor.execute(_PRIOR_ALERTS_SQL, (str(cohort_id), session_date, F0DTE_EVALUATION_CONTRACT, cutoff,
                                                    cutoff - timedelta(minutes=60)))
            for item in unit.cursor.fetchall():
                if item["contract_ticker"]:
                    prior.append(_stored_prior_alert(item))
        if committed_offset is not None:
            unit.cursor.execute(_FLOW_EVIDENCE_SQL, (str(cohort_id), int(committed_offset)))
            flow_evidence = unit.cursor.fetchone() is not None
    return spot_before, prior, flow_evidence


def _contracts_expiring(binding, session_date: date) -> list[F0DTEContract]:
    rows = []
    for row in binding.enrollment.activity_selection.contracts:
        if row.expiration_date != session_date:
            continue
        rows.append(F0DTEContract(
            instrument_id=int(row.instrument_id), contract_id=int(row.contract_id),
            catalog_version_id=int(row.catalog_version_id), contract_ticker=str(row.contract_ticker).upper(),
            underlyer=str(row.underlyer).upper(), contract_type="CALL" if str(row.call_put).upper() == "C" else "PUT",
            strike=Decimal(str(row.strike)), expiration_date=row.expiration_date,
            multiplier=int(Decimal(str(row.multiplier)))))
    return rows


@dataclass(frozen=True, slots=True)
class F0DTEWindowOutput:
    """What the O1 lane takes from this window's F0DTE run (ON): the evaluations to add."""

    cutoff: datetime
    evaluations: tuple = ()


_OUTPUTS: dict[str, F0DTEWindowOutput] = {}
_OUTPUTS_LOCK = threading.Lock()


def _semantics(flow_environment):
    semantics = getattr(flow_environment, "options_flow_semantics", None)
    if semantics is not None:
        return semantics
    from .option_flow.conditions import load_default_live_semantics

    return _cached_semantics(load_default_live_semantics)


@lru_cache(maxsize=1)
def _cached_semantics(loader):
    return loader()


def run_f0dte_window(*, provider, binding, rolling, marks, flow_environment, barrier_offset: int | None,
                     cutoff: datetime, unpackageable_instrument_ids: Iterable[int] = (),
                     state: F0DTEState | None = None, environ: Mapping[str, str] | None = None,
                     read_tallies=read_window_tallies, db_reads=_db_reads,
                     now: datetime | None = None) -> F0DTEWindowOutput | None:
    """The Options window's F0DTE run, before its cycle: reads, decides, logs, and (ON) leaves this window's
    evaluations for the O1 lane (`f0dte_lane_evaluations`). OFF: returns None at once (no read, no line).
    Never raises: an error is one RECORD_FAILED line and a DEGRADED fault."""
    try:
        mode = f0dte_mode(environ)
    except ValueError:
        raise  # an invalid switch fails loudly (the window service refuses to start with one)
    if mode is F0DTEMode.OFF:
        return None
    state = _STATE if state is None else state
    try:
        policy = f0dte_policy() if environ is None else f0dte_policy_from(environ)
        return _run(mode, policy, state, provider=provider, binding=binding, rolling=rolling, marks=marks,
                    flow_environment=flow_environment, barrier_offset=barrier_offset, cutoff=cutoff,
                    unpackageable_instrument_ids=frozenset(int(value) for value in unpackageable_instrument_ids or ()),
                    read_tallies=read_tallies, db_reads=db_reads,
                    now=datetime.now(timezone.utc) if now is None else now)
    except Exception as error:  # noqa: BLE001 - F0DTE never fails the window
        reason = f"{type(error).__name__}: {error}"[:300]
        try:
            _print_line(F0DTE_RECORD_EVENT, {"contract": "f0dte-record-v1", "mode": mode.value,
                                             "status": "RECORD_FAILED", "cutoff": cutoff.isoformat(),
                                             "reason": reason})
            from .fault_journal import emit_fault

            emit_fault(kind=F0DTE_FAULT_KIND, severity="DEGRADED", recoverable=True, action="LANE_SKIPPED",
                       service="options-window", key="f0dte-failed", message=f"F0DTE at {cutoff.isoformat()}: {reason}",
                       context={"cutoff": cutoff.isoformat(), "mode": mode.value})
        except Exception:  # noqa: BLE001
            pass
        return None


@lru_cache(maxsize=4)
def _session_bounds(session_date: date) -> tuple[datetime, datetime]:
    from options.calendar import OptionExchangeCalendar

    calendar = OptionExchangeCalendar()
    return calendar.session_open(session_date), calendar.session_close(session_date)


def _run(mode, policy, state, *, provider, binding, rolling, marks, flow_environment, barrier_offset, cutoff,
         unpackageable_instrument_ids, read_tallies, db_reads, now):
    control = binding.enrollment.control_plane
    session_date = control.session_date
    session_open, session_close = _session_bounds(session_date)
    window = timedelta(minutes=policy.window_minutes)
    with state.lock:
        state.reset_for(session_date)
    contracts = _contracts_expiring(binding, session_date)
    by_instrument = {row.instrument_id: row for row in contracts}
    quotes: dict[str, F0DTEQuote] = {}
    mids: dict[int, F0DTEQuote] = {}
    for mark in marks or ():
        quote = F0DTEQuote(Decimal(mark.bid), Decimal(mark.ask), mark.market_time)
        mids[int(mark.instrument_id)] = quote
        contract = by_instrument.get(int(mark.instrument_id))
        if contract is not None:
            quotes[contract.contract_ticker] = quote
    from .option_flow.volume import COMPLETE_BASELINES

    day_volumes: dict[str, int] = {}
    unpackageable = {row.contract_ticker for row in contracts if row.instrument_id in unpackageable_instrument_ids}
    for row in getattr(rolling, "contract_volumes", ()) or ():
        ticker = str(row.checkpoint.contract_ticker).upper()
        day_volumes[ticker] = int(row.checkpoint.cumulative_volume)
        if row.completeness not in COMPLETE_BASELINES:
            unpackageable.add(ticker)
    spot = {}
    for row in getattr(rolling, "underlying_ticks", ()) or ():
        price = _tick_price(row)
        if price is not None:
            spot[str(row.ticker).upper()] = price
    entry_from, entry_to = entry_time_bounds(session_open, session_close, policy)
    # Reads only where they can matter: the journal only in the entry time, with 0DTE contracts.
    tallies: dict[str, F0DTETally] = {}
    prints_reason = None
    read_stats: dict = {}
    if contracts and entry_from <= cutoff <= entry_to:
        source = getattr(flow_environment, "options_flow_source", None)
        registration = getattr(source, "registration", None)
        if source is None or getattr(source, "path", None) is None:
            prints_reason = "JOURNAL_SOURCE_UNAVAILABLE"
        else:
            try:
                tallies, read_stats = read_tallies(
                    source.path, contracts=contracts, window_start=cutoff - window, window_end=cutoff,
                    barrier_offset=barrier_offset,
                    journal_generation=getattr(registration, "journal_generation", None),
                    semantics=_semantics(flow_environment), policy=policy)
            except F0DTEPrintsUnavailable as error:
                prints_reason = str(error)[:200]
    elif not contracts:
        prints_reason = "NO_0DTE_CONTRACTS"
    else:
        prints_reason = "OUTSIDE_ENTRY_TIME"
    spot_before, prior_db, flow_evidence = db_reads(
        provider, cohort_id=control.cohort_id, session_date=session_date, cutoff=cutoff, mode=mode, window=window,
        current_state_id=getattr(rolling, "state_id", None),
        current_committed_at=getattr(rolling, "committed_at", None),
        committed_offset=getattr(rolling, "committed_offset", None))
    # The session's earlier alerts: the database's (ON, any process) and this process's memory (RECORD would-alerts,
    # ON alerts not yet stored), once each (one alert = one contract at one window cutoff).
    with state.lock:
        merged: dict[tuple[str, str], F0DTEPriorAlert] = {}
        for row in (*prior_db, *state.prior()):
            if row.triggered_at < cutoff:
                merged.setdefault((row.contract_ticker, row.triggered_at.astimezone(timezone.utc).isoformat()), row)
        prior = list(merged.values())
        # ON: a stored alert this process does not track (a restart since) is tracked from here on; the horizons
        # already past are not reported again (the end-of-day analysis has the marks for them).
        if mode is F0DTEMode.ON:
            for row in prior_db:
                if (row.event_id is None or row.event_id in state.tracked or row.event_id in state.finished
                        or row.instrument_id is None or row.deadline is None):
                    continue
                elapsed = (cutoff - row.triggered_at).total_seconds() / 60
                state.tracked[row.event_id] = _Tracked(
                    event_id=row.event_id, contract_ticker=row.contract_ticker, instrument_id=row.instrument_id,
                    underlyer=row.underlyer, direction=row.direction or 0, entry=row.entry,
                    triggered_at=row.triggered_at, deadline=row.deadline, target=row.target, stop=row.stop,
                    mode=mode.value, horizons_done={h for h in _HORIZONS_MINUTES if h < elapsed})
    universe = frozenset(str(name).upper() for name in control.universe.option_underlyings)
    result = evaluate_f0dte_window(
        cutoff=cutoff, session_open=session_open, session_close=session_close, contracts=contracts, tallies=tallies,
        quotes=quotes, day_volumes=day_volumes, spot=spot, spot_before=spot_before, prior_alerts=prior,
        universe=universe, prints_reason=prints_reason, unpackageable=frozenset(unpackageable),
        flow_evidence_available=flow_evidence,
        window_late=(now - cutoff).total_seconds() > policy.maximum_window_delay_seconds, policy=policy)
    evaluations = []
    alert_documents = []
    package_failures = Counter()
    if result.alerts:
        from .advanced_options_cycle import AdvancedOptionsNativeEvaluation
        from .advanced_options_native_loader import session_valuation_policy_sha256

        valuation_sha256 = session_valuation_policy_sha256()
        for row in result.alerts:
            payload = f0dte_payload(row, cutoff=cutoff, session_close=session_close, policy=policy)
            try:
                package = f0dte_package(row, cohort_id=control.cohort_id, matrix_id=rolling.state_id, cutoff=cutoff,
                                        session_close=session_close, valuation_policy_sha256=valuation_sha256,
                                        policy=policy)
            except (ValueError, TypeError, ArithmeticError) as error:
                package_failures[f"F0DTE_PACKAGE_INVALID:{type(error).__name__}"] += 1
                continue
            alert_documents.append({**payload, "mode": mode.value, "applied": mode is F0DTEMode.ON,
                                    "event_id": str(package.event_id), "candidate_id": str(package.candidate_id)})
            if mode is F0DTEMode.ON:
                evaluations.append(AdvancedOptionsNativeEvaluation(
                    package.event_id, row.contract.underlyer, "SELECTED", cutoff, payload, package))
            with state.lock:
                state.remember(row, cutoff)
                state.tracked.setdefault(str(package.event_id), _Tracked(
                    event_id=str(package.event_id), contract_ticker=row.contract.contract_ticker,
                    instrument_id=row.contract.instrument_id, underlyer=row.contract.underlyer,
                    direction=row.direction, entry=row.quote.ask, triggered_at=cutoff,
                    deadline=exit_deadline(cutoff, session_close, policy),
                    target=row.quote.ask * (100 + policy.target_pct) / 100,
                    stop=row.quote.ask * (100 - policy.stop_pct) / 100, mode=mode.value))
    window_document = f0dte_window_document(result, mode=mode, policy=policy, prints_reason=prints_reason,
                                            read_stats=read_stats, prior_alerts=len(prior))
    window_document["delay_seconds"] = round((now - cutoff).total_seconds(), 1)
    if package_failures:
        window_document["package_failures"] = dict(sorted(package_failures.items()))
    with state.lock:
        outcomes = outcome_documents(state, cutoff=cutoff, mids=mids, policy=policy)
    _print_line(F0DTE_RECORD_EVENT, window_document)
    for document in alert_documents:
        _print_line(F0DTE_ALERT_EVENT, document)
    for document in outcomes:
        _print_line(F0DTE_OUTCOME_EVENT, document)
    output = F0DTEWindowOutput(cutoff=cutoff, evaluations=tuple(evaluations))
    with _OUTPUTS_LOCK:
        _OUTPUTS[cutoff.isoformat()] = output
        for key in sorted(_OUTPUTS)[:-4]:
            _OUTPUTS.pop(key, None)
    return output


def f0dte_lane_evaluations(cutoff: datetime) -> tuple:
    """The O1 lane's F0DTE evaluations for this cutoff (ON only; () otherwise). Never raises."""
    try:
        if f0dte_mode() is not F0DTEMode.ON:
            return ()
        with _OUTPUTS_LOCK:
            output = _OUTPUTS.get(cutoff.isoformat())
        return () if output is None else tuple(output.evaluations)
    except Exception:  # noqa: BLE001
        return ()


def is_f0dte_payload(payload) -> bool:
    return isinstance(payload, Mapping) and payload.get("contract") == F0DTE_EVALUATION_CONTRACT


__all__ = [
    "F0DTECandidate", "F0DTEContract", "F0DTEMode", "F0DTEPolicy", "F0DTEPriorAlert", "F0DTEQuote", "F0DTEState",
    "F0DTETally", "F0DTEWindow", "F0DTEWindowOutput", "F0DTE_ALERT_EVENT", "F0DTE_CATEGORY", "F0DTE_DETECTOR_ID",
    "F0DTE_EVALUATION_CONTRACT", "F0DTE_OUTCOME_EVENT", "F0DTE_POLICY", "F0DTE_RECORD_EVENT", "F0DTE_SETTING",
    "JournalEvent", "evaluate_f0dte_window", "f0dte_candidate_id", "f0dte_event_id", "f0dte_lane_evaluations",
    "f0dte_mode", "f0dte_package", "f0dte_payload", "f0dte_policy", "f0dte_policy_from", "fold_contract_events",
    "implied_delta", "is_f0dte_payload", "management_policy", "read_window_tallies", "run_f0dte_window",
]
