"""Generic adapters over reconstructed historical research inputs."""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import date, datetime, timezone
import hashlib
import json
import threading
from typing import Any, Mapping, Protocol, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

import numpy as np
import pandas as pd
import exchange_calendars

from equity.polygon import sha256_json
from research.composite_scanners import (
    COMPOSITE_SCANNER_REGISTRY,
    build_all_scanner_events,
)
from research.forming_patterns import PATTERN_NAMES, detect_forming_patterns
from screeners import (
    identify_gap_down,
    identify_gap_up,
    scan_bearish_bounce,
    scan_gap_strategies,
    scan_momentum_pullback,
)


@dataclass(frozen=True, slots=True)
class HistoricalSignalContext:
    signal_date: date
    signal_time: datetime
    universe_run_id: UUID
    universe_policy_version: str
    corporate_actions: tuple[Mapping[str, Any], ...]
    corporate_actions_by_date: Mapping[
        date, tuple[Mapping[str, Any], ...]
    ] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class HistoricalSignalEvent:
    event_id: UUID
    source_name: str
    source_version: str
    ticker: str
    signal_date: date
    signal_time: datetime
    direction: int
    setup_anchor: str
    universe_run_id: UUID
    universe_policy_version: str
    source_bar_revision_ids: tuple[UUID, ...]
    payload: Mapping[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": str(self.event_id),
            "source_name": self.source_name,
            "source_version": self.source_version,
            "ticker": self.ticker,
            "signal_date": self.signal_date.isoformat(),
            "signal_time": self.signal_time.isoformat(),
            "direction": self.direction,
            "setup_anchor": self.setup_anchor,
            "universe_run_id": str(self.universe_run_id),
            "universe_policy_version": self.universe_policy_version,
            "source_bar_revision_ids": [
                str(value) for value in self.source_bar_revision_ids
            ],
            "payload": dict(self.payload),
        }

    @classmethod
    def from_dict(cls, row: Mapping[str, Any]) -> HistoricalSignalEvent:
        signal_time = pd.Timestamp(row["signal_time"])
        if signal_time.tzinfo is None:
            raise ValueError("historical event signal_time must be timezone-aware")
        return cls(
            event_id=UUID(str(row["event_id"])),
            source_name=str(row["source_name"]),
            source_version=str(row["source_version"]),
            ticker=str(row["ticker"]).upper(),
            signal_date=date.fromisoformat(str(row["signal_date"])),
            signal_time=signal_time.to_pydatetime(),
            direction=int(row["direction"]),
            setup_anchor=str(row["setup_anchor"]),
            universe_run_id=UUID(str(row["universe_run_id"])),
            universe_policy_version=str(row["universe_policy_version"]),
            source_bar_revision_ids=tuple(
                UUID(str(value)) for value in row.get("source_bar_revision_ids", ())
            ),
            payload=dict(row.get("payload") or {}),
        )


class HistoricalSignalAdapter(Protocol):
    source_name: str
    source_version: str
    minimum_bars: int
    excluded_action_types: frozenset[str]

    def candidate_dates(self, frame: pd.DataFrame) -> tuple[date, ...]: ...

    def evaluate(
        self,
        ticker: str,
        frame: pd.DataFrame,
        context: HistoricalSignalContext,
    ) -> tuple[HistoricalSignalEvent, ...]: ...

    def summarize(
        self, events: Sequence[HistoricalSignalEvent]
    ) -> Mapping[str, Any]: ...


@dataclass(frozen=True, slots=True)
class HistoricalReplayResult:
    events: tuple[HistoricalSignalEvent, ...]
    candidate_count: int
    exclusion_counts: Mapping[str, int]
    coverage: tuple[Mapping[str, Any], ...] = ()


DAILY_REPLAY_CONTRACT = "DAILY_IDENTITY_REPLAY_V3"


class GapFormationAdapter:
    source_name = "GAP_FORMATION"
    source_version = "gap_formation_v1"
    minimum_bars = 20
    excluded_action_types = frozenset(("SPLIT", "SYMBOL_CHANGE", "SPINOFF", "MERGER"))

    def candidate_dates(self, frame: pd.DataFrame) -> tuple[date, ...]:
        if len(frame) < 2:
            return ()
        previous_high = frame["high"].shift(1)
        previous_low = frame["low"].shift(1)
        gap_up = (frame["open"] - previous_high) / previous_high >= 0.01
        gap_down = (previous_low - frame["open"]) / previous_low >= 0.01
        candidates = []
        calendar = exchange_calendars.get_calendar("XNYS")
        mask = gap_up.fillna(False) | gap_down.fillna(False)
        for position in np.flatnonzero(mask.to_numpy()):
            signal_date = _date(frame.index[position])
            expected_previous = pd.Timestamp(
                calendar.previous_session(pd.Timestamp(signal_date))
            ).date()
            if position > 0 and _date(frame.index[position - 1]) == expected_previous:
                candidates.append(signal_date)
        return tuple(candidates)

    def evaluate(
        self,
        ticker: str,
        frame: pd.DataFrame,
        context: HistoricalSignalContext,
    ) -> tuple[HistoricalSignalEvent, ...]:
        if len(frame) < self.minimum_bars:
            return ()
        rows = scan_gap_strategies(ticker, frame.copy(), interval="1d")
        latest = frame.iloc[-1]
        source_bar_id = latest.get("bar_revision_id")
        events = []
        for row in rows:
            if row.get("gap_date") != context.signal_date.isoformat():
                continue
            gap_direction = str(row.get("gap_direction") or "")
            original_direction = 1 if gap_direction == "UP" else -1
            fading = row.get("gap_lifecycle") in ("SAME_SESSION_FADE", "FAILED")
            direction = -original_direction if fading else original_direction
            hypothesis = "FADE_REVERSAL" if fading else "FORMATION_HOLD"
            gap_class = str(row.get("gap_classification") or "UNCLASSIFIED")
            if fading:
                source_name = "GAP_FADE_REVERSAL"
                qualification_eligible = True
            elif gap_class == "BREAKAWAY":
                source_name = "GAP_BREAKAWAY_HOLD"
                qualification_eligible = True
            elif gap_class == "CONTINUATION":
                source_name = "GAP_CONTINUATION_HOLD"
                qualification_eligible = True
            else:
                source_name = "GAP_FORMATION_CONTROL"
                qualification_eligible = False
            setup_anchor = f"{ticker}:{context.signal_date}:{gap_direction}"
            identity = sha256_json({
                "anchor": setup_anchor,
                "source_name": source_name,
                "source_version": self.source_version,
                "universe_run_id": str(context.universe_run_id),
            })
            payload = {
                **row,
                "hypothesis": hypothesis,
                "qualification_eligible": qualification_eligible,
                "corporate_action_types": sorted({
                    str(action.get("action_type"))
                    for action in context.corporate_actions
                }),
            }
            events.append(HistoricalSignalEvent(
                event_id=uuid5(NAMESPACE_URL, f"historical-signal-event:{identity}"),
                source_name=source_name,
                source_version=self.source_version,
                ticker=ticker,
                signal_date=context.signal_date,
                signal_time=context.signal_time,
                direction=direction,
                setup_anchor=setup_anchor,
                universe_run_id=context.universe_run_id,
                universe_policy_version=context.universe_policy_version,
                source_bar_revision_ids=(source_bar_id,) if isinstance(source_bar_id, UUID) else (),
                payload=payload,
            ))
        return tuple(events)

    def summarize(
        self, events: Sequence[HistoricalSignalEvent]
    ) -> Mapping[str, Any]:
        class_counts = {}
        hypothesis_counts = {}
        direction_counts = {"LONG": 0, "SHORT": 0}
        lane_counts = {}
        opening_gaps = []
        dividend_context_events = 0
        for event in events:
            gap_class = str(event.payload.get("gap_classification") or "UNKNOWN")
            hypothesis = str(event.payload.get("hypothesis") or "UNKNOWN")
            class_counts[gap_class] = class_counts.get(gap_class, 0) + 1
            hypothesis_counts[hypothesis] = hypothesis_counts.get(hypothesis, 0) + 1
            direction_counts["LONG" if event.direction == 1 else "SHORT"] += 1
            lane_counts[event.source_name] = lane_counts.get(event.source_name, 0) + 1
            opening_gap = event.payload.get("opening_gap_pct")
            if opening_gap is not None:
                opening_gaps.append(float(opening_gap))
            if "DIVIDEND" in event.payload.get("corporate_action_types", ()):
                dividend_context_events += 1
        return {
            "class_counts": dict(sorted(class_counts.items())),
            "direction_counts": direction_counts,
            "dividend_context_events": dividend_context_events,
            "extreme_gap_counts": {
                "at_least_50_pct": sum(value >= 50 for value in opening_gaps),
                "at_least_100_pct": sum(value >= 100 for value in opening_gaps),
            },
            "hypothesis_counts": dict(sorted(hypothesis_counts.items())),
            "lane_counts": dict(sorted(lane_counts.items())),
            "maximum_opening_gap_pct": max(opening_gaps, default=None),
        }


class GapFormationV2Adapter(GapFormationAdapter):
    source_version = "gap_formation_v2"
    minimum_bars = 21
    maximum_bars = 21


class GapBreakawayConfirmationAdapter(GapFormationAdapter):
    source_name = "GAP_BREAKAWAY_CONFIRMATION"
    source_version = "gap_breakaway_confirmation_v2"
    minimum_bars = 22
    maximum_bars = 26
    confirmation_sessions = 5

    def candidate_dates(self, frame: pd.DataFrame) -> tuple[date, ...]:
        positions = {_date(value): position for position, value in enumerate(frame.index)}
        candidates = set()
        for formation_date in super().candidate_dates(frame):
            formation_position = positions[formation_date]
            for position in range(
                formation_position + 1,
                min(formation_position + self.confirmation_sessions + 1, len(frame)),
            ):
                candidates.add(_date(frame.index[position]))
        return tuple(sorted(candidates))

    def evaluate(
        self,
        ticker: str,
        frame: pd.DataFrame,
        context: HistoricalSignalContext,
    ) -> tuple[HistoricalSignalEvent, ...]:
        current_position = len(frame) - 1
        current = frame.iloc[current_position]
        events = []
        for formation_position in range(
            max(20, current_position - self.confirmation_sessions), current_position
        ):
            formation_date = _date(frame.index[formation_position])
            row = _formation_row(ticker, frame, formation_position)
            if row is None or row.get("gap_classification") != "BREAKAWAY":
                continue
            if row.get("gap_lifecycle") in ("SAME_SESSION_FADE", "FAILED"):
                continue
            if _has_excluded_actions_between(
                context, formation_date, context.signal_date,
                self.excluded_action_types,
            ):
                continue
            formation_actions = context.corporate_actions_by_date.get(formation_date, ())
            direction = 1 if row.get("gap_direction") == "UP" else -1
            formation = frame.iloc[formation_position]
            trigger = float(formation["high"] if direction == 1 else formation["low"])
            current_close = float(current["close"])
            confirmed_now = current_close > trigger if direction == 1 else current_close < trigger
            intervening = frame.iloc[formation_position + 1:current_position]
            confirmed_before = (
                bool((intervening["close"] > trigger).any())
                if direction == 1
                else bool((intervening["close"] < trigger).any())
            )
            stop = float(formation["low"] if direction == 1 else formation["high"])
            risk = direction * (current_close - stop)
            if not confirmed_now or confirmed_before or risk <= 0:
                continue
            target = current_close + direction * 2 * risk
            setup_anchor = (
                f"{ticker}:{formation_date}:{row['gap_direction']}:"
                f"confirmed:{context.signal_date}"
            )
            payload = {
                **row,
                "confirmation_sessions": current_position - formation_position,
                "confirmation_trigger": trigger,
                "current_high": float(current["high"]),
                "current_low": float(current["low"]),
                "current_open": float(current["open"]),
                "formation_date": formation_date.isoformat(),
                "hypothesis": "CONFIRMED_BREAKAWAY_CONTINUATION",
                "last_close": current_close,
                "qualification_eligible": True,
                "require_entry_between_stop_target": True,
                "risk_basis": "CONFIRMATION_CLOSE_TO_FORMATION_EXTREME",
                "stop_price": stop,
                "target_price": target,
                "corporate_action_types": sorted(_action_types(formation_actions)),
            }
            events.append(_historical_event(
                adapter=self,
                ticker=ticker,
                context=context,
                direction=direction,
                setup_anchor=setup_anchor,
                source_rows=(formation, current),
                payload=payload,
            ))
        return tuple(events)


class GapEntryFillAdapter(GapFormationAdapter):
    source_name = "GAP_ENTRY_FILL"
    source_version = "gap_entry_fill_v2"
    minimum_bars = 22
    maximum_bars = 81
    maximum_gap_age_sessions = 60

    def candidate_dates(self, frame: pd.DataFrame) -> tuple[date, ...]:
        return tuple(sorted(_gap_entry_dates(frame, self.maximum_gap_age_sessions)))

    def evaluate(
        self,
        ticker: str,
        frame: pd.DataFrame,
        context: HistoricalSignalContext,
    ) -> tuple[HistoricalSignalEvent, ...]:
        current_position = len(frame) - 1
        current = frame.iloc[current_position]
        previous_close = float(frame.iloc[current_position - 1]["close"])
        valid_formations = set(super().candidate_dates(frame))
        events = []
        gaps = [
            *(dict(gap, gap_direction="UP") for gap in identify_gap_up(frame)),
            *(dict(gap, gap_direction="DOWN") for gap in identify_gap_down(frame)),
        ]
        for gap in gaps:
            formation_position = int(gap["index"])
            formation_date = _date(frame.index[formation_position])
            age = current_position - formation_position
            if (
                formation_date not in valid_formations
                or not gap.get("range_gap_survived")
                or age < 1
                or age > self.maximum_gap_age_sessions
            ):
                continue
            if _has_excluded_actions_between(
                context, formation_date, context.signal_date,
                self.excluded_action_types,
            ):
                continue
            formation_actions = context.corporate_actions_by_date.get(formation_date, ())
            current_close = float(current["close"])
            if gap["gap_direction"] == "UP":
                target = float(gap["prev_high"])
                stop = float(gap["gap_low"])
                prior_unfilled = bool(
                    (frame.iloc[formation_position + 1:current_position]["low"] > target).all()
                )
                enters_now = (
                    float(current["low"]) > target
                    and target < current_close < stop
                    and previous_close >= stop
                )
                direction = -1
            else:
                target = float(gap["prev_low"])
                stop = float(gap["gap_high"])
                prior_unfilled = bool(
                    (frame.iloc[formation_position + 1:current_position]["high"] < target).all()
                )
                enters_now = (
                    float(current["high"]) < target
                    and stop < current_close < target
                    and previous_close <= stop
                )
                direction = 1
            if not prior_unfilled or not enters_now:
                continue
            formation_row = _formation_row(ticker, frame, formation_position)
            if formation_row is None:
                continue
            setup_anchor = (
                f"{ticker}:{formation_date}:{gap['gap_direction']}:"
                f"entry:{context.signal_date}"
            )
            payload = {
                **formation_row,
                "current_high": float(current["high"]),
                "current_low": float(current["low"]),
                "current_open": float(current["open"]),
                "entry_date": context.signal_date.isoformat(),
                "formation_date": formation_date.isoformat(),
                "gap_age_sessions_at_entry": age,
                "hypothesis": "GAP_ENTRY_FILL",
                "last_close": current_close,
                "qualification_eligible": True,
                "require_entry_between_stop_target": True,
                "risk_basis": "GAP_NEAR_EDGE_TO_FAR_EDGE",
                "stop_price": stop,
                "target_price": target,
                "target_kind": "RANGE_GAP_FAR_EDGE",
                "corporate_action_types": sorted(_action_types(formation_actions)),
            }
            events.append(_historical_event(
                adapter=self,
                ticker=ticker,
                context=context,
                direction=direction,
                setup_anchor=setup_anchor,
                source_rows=(frame.iloc[formation_position], current),
                payload=payload,
            ))
        return tuple(events)


class MovingAverageCrossoverAdapter:
    source_name = "MA_CROSSOVER_9_21"
    source_version = "ma_crossover_9_21_v1"
    minimum_bars = 22
    maximum_bars = 22
    excluded_action_types = frozenset(("SPLIT", "SYMBOL_CHANGE", "SPINOFF", "MERGER"))
    short_period = 9
    long_period = 21

    def candidate_dates(self, frame: pd.DataFrame) -> tuple[date, ...]:
        close = frame["close"].astype(float)
        short = close.rolling(self.short_period).mean()
        long = close.rolling(self.long_period).mean()
        spread = short - long
        crossed = (
            ((spread > 0) & (spread.shift(1) <= 0))
            | ((spread < 0) & (spread.shift(1) >= 0))
        )
        return tuple(_date(value) for value in frame.index[crossed.fillna(False)])

    def evaluate(
        self,
        ticker: str,
        frame: pd.DataFrame,
        context: HistoricalSignalContext,
    ) -> tuple[HistoricalSignalEvent, ...]:
        if len(frame) < self.minimum_bars:
            return ()
        if _has_excluded_actions(
            context, frame, self.excluded_action_types
        ):
            return ()
        close = frame["close"].astype(float)
        short = close.rolling(self.short_period).mean()
        long = close.rolling(self.long_period).mean()
        current_spread = float(short.iloc[-1] - long.iloc[-1])
        prior_spread = float(short.iloc[-2] - long.iloc[-2])
        if current_spread > 0 and prior_spread <= 0:
            direction = 1
        elif current_spread < 0 and prior_spread >= 0:
            direction = -1
        else:
            return ()
        current = frame.iloc[-1]
        setup_anchor = (
            f"{ticker}:{context.signal_date}:sma{self.short_period}:"
            f"sma{self.long_period}:{direction}"
        )
        return (_historical_event(
            adapter=self,
            ticker=ticker,
            context=context,
            direction=direction,
            setup_anchor=setup_anchor,
            source_rows=(current,),
            payload={
                "hypothesis": "FRESH_MA_CROSSOVER",
                "last_close": float(current["close"]),
                "long_ma": float(long.iloc[-1]),
                "long_period": self.long_period,
                "ma_spread_pct": float(current_spread / long.iloc[-1] * 100),
                "qualification_eligible": True,
                "short_ma": float(short.iloc[-1]),
                "short_period": self.short_period,
                "signal": "BULLISH_CROSSOVER" if direction == 1 else "BEARISH_CROSSOVER",
            },
        ),)

    def summarize(
        self, events: Sequence[HistoricalSignalEvent]
    ) -> Mapping[str, Any]:
        return {
            "direction_counts": {
                "LONG": sum(event.direction == 1 for event in events),
                "SHORT": sum(event.direction == -1 for event in events),
            },
            "hypothesis_counts": {"FRESH_MA_CROSSOVER": len(events)},
            "lane_counts": {self.source_name: len(events)},
        }


class _ProductPullbackAdapter:
    minimum_bars = 210
    maximum_bars = 211
    excluded_action_types = frozenset(("SPLIT", "SYMBOL_CHANGE", "SPINOFF", "MERGER"))
    direction: int
    hypothesis: str
    scanner: Any
    stochastic_minimum: float | None = None
    stochastic_maximum: float | None = None
    rsi_minimum: float
    rsi_maximum: float
    collapse_contiguous_matches = True

    def candidate_dates(self, frame: pd.DataFrame) -> tuple[date, ...]:
        if len(frame) < self.minimum_bars:
            return ()
        close = frame["close"].astype(float)
        low14 = frame["low"].astype(float).rolling(14).min()
        high14 = frame["high"].astype(float).rolling(14).max()
        slow_k = ((close - low14) / (high14 - low14) * 100).rolling(3).mean()
        delta = close.diff()
        gain = delta.where(delta > 0, 0.0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
        rsi = 100 - (100 / (1 + gain / loss))
        candidates = rsi.between(self.rsi_minimum, self.rsi_maximum)
        if self.stochastic_minimum is not None:
            candidates &= slow_k > self.stochastic_minimum
        if self.stochastic_maximum is not None:
            candidates &= slow_k < self.stochastic_maximum
        candidates.iloc[:self.minimum_bars - 1] = False
        return tuple(_date(value) for value in frame.index[candidates.fillna(False)])

    def _matches(self, ticker: str, frame: pd.DataFrame) -> Mapping[str, Any] | None:
        if len(frame) < self.minimum_bars:
            return None
        scanner_frame = frame.tail(self.minimum_bars).copy()
        scanner_frame.index = pd.DatetimeIndex(pd.to_datetime(scanner_frame.index))
        return self.scanner(ticker, scanner_frame, interval="1d")

    def evaluate(
        self,
        ticker: str,
        frame: pd.DataFrame,
        context: HistoricalSignalContext,
    ) -> tuple[HistoricalSignalEvent, ...]:
        if len(frame) < self.minimum_bars:
            return ()
        if _has_excluded_actions(context, frame, self.excluded_action_types):
            return ()
        result = self._matches(ticker, frame)
        if result is None:
            return ()
        current = frame.iloc[-1]
        setup_anchor = f"{ticker}:{context.signal_date}:{self.source_version}"
        return (_historical_event(
            adapter=self,
            ticker=ticker,
            context=context,
            direction=self.direction,
            setup_anchor=setup_anchor,
            source_rows=(current,),
            payload={
                **result,
                "hypothesis": self.hypothesis,
                "indicator_window_bars": self.minimum_bars,
                "last_close": float(current["close"]),
                "qualification_eligible": True,
            },
        ),)

    def summarize(
        self, events: Sequence[HistoricalSignalEvent]
    ) -> Mapping[str, Any]:
        grades: dict[str, int] = {}
        for event in events:
            grade = str(event.payload.get("grade") or "UNKNOWN")
            grades[grade] = grades.get(grade, 0) + 1
        return {
            "direction_counts": {
                "LONG": sum(event.direction == 1 for event in events),
                "SHORT": sum(event.direction == -1 for event in events),
            },
            "grade_counts": dict(sorted(grades.items())),
            "hypothesis_counts": {self.hypothesis: len(events)},
            "lane_counts": {self.source_name: len(events)},
        }


class MomentumPullbackAdapter(_ProductPullbackAdapter):
    source_name = "MOMENTUM_PULLBACK"
    source_version = "momentum_pullback_v2"
    direction = 1
    hypothesis = "EMA_STACK_MOMENTUM_PULLBACK"
    scanner = staticmethod(scan_momentum_pullback)
    stochastic_maximum = 40.0
    rsi_minimum = 30.0
    rsi_maximum = 60.0


class BearishBounceAdapter(_ProductPullbackAdapter):
    source_name = "BEARISH_BOUNCE"
    source_version = "bearish_bounce_v2"
    direction = -1
    hypothesis = "EMA_STACK_BEARISH_BOUNCE"
    scanner = staticmethod(scan_bearish_bounce)
    stochastic_minimum = 60.0
    rsi_minimum = 40.0
    rsi_maximum = 70.0


class CompositeScannersDailyAdapter:
    source_name = "COMPOSITE_SCANNERS"
    source_version = "composite_scanners_daily_v1"
    interval = "1d"
    minimum_bars = 210
    excluded_action_types = frozenset(
        ("SPLIT", "SYMBOL_CHANGE", "SPINOFF", "MERGER")
    )

    def __init__(self) -> None:
        self._cache = threading.local()

    def candidate_dates(self, frame: pd.DataFrame) -> tuple[date, ...]:
        if len(frame) < self.minimum_bars:
            return ()
        events = build_all_scanner_events(
            _composite_panel(frame), interval=self.interval
        )
        self._cache.ticker = str(frame.iloc[-1]["ticker"])
        self._cache.events = events
        if events.empty:
            return ()
        eligible_dates = {
            _date(value) for value in frame.index[self.minimum_bars - 1:]
        }
        return tuple(sorted({
            _date(value) for value in events["date"]
            if _date(value) in eligible_dates
        }))

    def evaluate(
        self,
        ticker: str,
        frame: pd.DataFrame,
        context: HistoricalSignalContext,
    ) -> tuple[HistoricalSignalEvent, ...]:
        if len(frame) < self.minimum_bars:
            return ()
        if _has_excluded_actions(
            context, frame.tail(self.minimum_bars), self.excluded_action_types
        ):
            return ()
        rows = (
            self._cache.events
            if getattr(self._cache, "ticker", None) == ticker else
            build_all_scanner_events(_composite_panel(frame), interval=self.interval)
        )
        if rows.empty:
            return ()
        rows = rows[
            pd.to_datetime(rows["date"]).dt.date.eq(context.signal_date)
        ]
        current = frame.iloc[-1]
        source_bar_id = current.get("bar_revision_id")
        events = []
        for _, row in rows.iterrows():
            source_name = str(row["scanner_name"])
            source_version = str(row["scanner_version"])
            registration = COMPOSITE_SCANNER_REGISTRY.get(source_name)
            if registration is None or registration.source_version != source_version:
                raise ValueError(
                    f"unregistered composite scanner event: {source_name}:{source_version}"
                )
            direction = int(row["direction"])
            setup_anchor = f"{ticker}:{row['setup_anchor']}"
            identity = sha256_json({
                "anchor": setup_anchor,
                "source_name": source_name,
                "source_version": source_version,
                "universe_run_id": str(context.universe_run_id),
            })
            metadata = json.loads(str(row["metadata"] or "{}"))
            reference_price = _optional_float(row.get("entry_price"))
            payload = {
                "atr_at_signal": _optional_float(row.get("atr_at_signal")),
                "corporate_action_types": sorted(
                    _action_types(context.corporate_actions)
                ),
                "entry_price": reference_price,
                "hypothesis": source_name.upper(),
                "last_close": reference_price,
                "metadata": metadata,
                "outcome_modes": list(registration.outcome_modes),
                "qualification_eligible": True,
                "reference_level": _optional_float(row.get("reference_level")),
                "signal_reference_price": reference_price,
                "stop_price": _optional_float(row.get("stop_price")),
                "target_price": _optional_float(row.get("target_price")),
                "trigger_type": str(row["trigger_type"]),
            }
            events.append(HistoricalSignalEvent(
                event_id=uuid5(
                    NAMESPACE_URL, f"historical-signal-event:{identity}"
                ),
                source_name=source_name,
                source_version=source_version,
                ticker=ticker,
                signal_date=context.signal_date,
                signal_time=context.signal_time,
                direction=direction,
                setup_anchor=setup_anchor,
                universe_run_id=context.universe_run_id,
                universe_policy_version=context.universe_policy_version,
                source_bar_revision_ids=(
                    (source_bar_id,) if isinstance(source_bar_id, UUID) else ()
                ),
                payload=payload,
            ))
        return tuple(events)

    def summarize(
        self, events: Sequence[HistoricalSignalEvent]
    ) -> Mapping[str, Any]:
        lane_counts = {}
        for event in events:
            lane_counts[event.source_name] = lane_counts.get(event.source_name, 0) + 1
        return {
            "direction_counts": {
                "LONG": sum(event.direction == 1 for event in events),
                "SHORT": sum(event.direction == -1 for event in events),
            },
            "lane_counts": dict(sorted(lane_counts.items())),
        }


class PatternBoundaryBreakDailyAdapter:
    source_name = "PATTERN_BOUNDARY_BREAK"
    source_version = "forming_patterns_boundary_break_v1"
    interval = "1d"
    minimum_bars = 210
    excluded_action_types = frozenset(
        ("SPLIT", "SYMBOL_CHANGE", "SPINOFF", "MERGER")
    )

    def __init__(self) -> None:
        self._cache = threading.local()

    def candidate_dates(self, frame: pd.DataFrame) -> tuple[date, ...]:
        if len(frame) < self.minimum_bars:
            return ()
        events = _pattern_boundary_breaks(
            frame, start_position=self.minimum_bars - 2
        )
        self._cache.ticker = str(frame.iloc[-1]["ticker"])
        self._cache.events = events
        eligible_dates = {
            _date(value) for value in frame.index[self.minimum_bars - 1:]
        }
        return tuple(sorted({
            row["signal_date"] for row in events
            if row["signal_date"] in eligible_dates
        }))

    def evaluate(
        self,
        ticker: str,
        frame: pd.DataFrame,
        context: HistoricalSignalContext,
    ) -> tuple[HistoricalSignalEvent, ...]:
        rows = (
            self._cache.events
            if getattr(self._cache, "ticker", None) == ticker else
            _pattern_boundary_breaks(
                frame, start_position=self.minimum_bars - 2
            )
        )
        events = []
        for row in rows:
            if row["signal_date"] != context.signal_date:
                continue
            formation_date = datetime.fromtimestamp(
                row["formation_start_time"], timezone.utc
            ).date()
            if _has_excluded_actions_between(
                context,
                formation_date,
                context.signal_date,
                self.excluded_action_types,
            ):
                continue
            source_name = f"PATTERN_{row['pattern_type']}_BOUNDARY_BREAK"
            setup_anchor = (
                f"{ticker}:{self.interval}:{row['pattern_type']}:"
                f"{row['formation_start_time']}:{row['boundary_revision_id']}"
            )
            identity = sha256_json({
                "anchor": setup_anchor,
                "source_name": source_name,
                "source_version": self.source_version,
                "universe_run_id": str(context.universe_run_id),
            })
            events.append(HistoricalSignalEvent(
                event_id=uuid5(
                    NAMESPACE_URL, f"historical-signal-event:{identity}"
                ),
                source_name=source_name,
                source_version=self.source_version,
                ticker=ticker,
                signal_date=context.signal_date,
                signal_time=context.signal_time,
                direction=row["direction"],
                setup_anchor=setup_anchor,
                universe_run_id=context.universe_run_id,
                universe_policy_version=context.universe_policy_version,
                source_bar_revision_ids=tuple(
                    value for value in (
                        row["boundary_revision_id"],
                        row["confirmation_revision_id"],
                    )
                    if isinstance(value, UUID)
                ),
                payload={
                    "boundary_price": row["boundary_price"],
                    "boundary_revision_id": str(row["boundary_revision_id"]),
                    "confirmation_close": row["confirmation_close"],
                    "confirmation_revision_id": str(
                        row["confirmation_revision_id"]
                    ),
                    "formation_snapshot": row["formation_snapshot"],
                    "formation_start_time": row["formation_start_time"],
                    "hypothesis": "CONFIRMED_PATTERN_BOUNDARY_BREAK",
                    "invalidation_price": row["invalidation_price"],
                    "last_close": row["confirmation_close"],
                    "outcome_modes": ["DIRECTIONAL_HORIZON"],
                    "pattern_type": row["pattern_type"],
                    "qualification_eligible": True,
                    "trigger_type": "BOUNDARY_BREAK",
                },
            ))
        return tuple(events)

    def summarize(
        self, events: Sequence[HistoricalSignalEvent]
    ) -> Mapping[str, Any]:
        lane_counts = {}
        for event in events:
            lane_counts[event.source_name] = lane_counts.get(event.source_name, 0) + 1
        return {
            "direction_counts": {
                "LONG": sum(event.direction == 1 for event in events),
                "SHORT": sum(event.direction == -1 for event in events),
            },
            "lane_counts": dict(sorted(lane_counts.items())),
        }


BUILTIN_ADAPTERS: Mapping[str, HistoricalSignalAdapter] = {
    "gap-formation-v1": GapFormationAdapter(),
    "gap-formation-v2": GapFormationV2Adapter(),
    "gap-breakaway-confirmation-v1": GapBreakawayConfirmationAdapter(),
    "gap-breakaway-confirmation-v2": GapBreakawayConfirmationAdapter(),
    "gap-entry-fill-v1": GapEntryFillAdapter(),
    "gap-entry-fill-v2": GapEntryFillAdapter(),
    "ma-crossover-9-21-v1": MovingAverageCrossoverAdapter(),
    "momentum-pullback-v1": MomentumPullbackAdapter(),
    "momentum-pullback-v2": MomentumPullbackAdapter(),
    "bearish-bounce-v1": BearishBounceAdapter(),
    "bearish-bounce-v2": BearishBounceAdapter(),
    "composite-scanners-1d-v1": CompositeScannersDailyAdapter(),
    "pattern-boundary-break-1d-v1": PatternBoundaryBreakDailyAdapter(),
}
GAP_FORMATION_ADAPTER = BUILTIN_ADAPTERS["gap-formation-v1"]
GAP_FORMATION_V2_ADAPTER = BUILTIN_ADAPTERS["gap-formation-v2"]


def _composite_panel(frame: pd.DataFrame) -> pd.DataFrame:
    panel = frame.reset_index()
    index_column = frame.index.name or "index"
    if index_column != "date":
        panel = panel.rename(columns={index_column: "date"})
    if "ticker" not in panel:
        raise ValueError("composite historical frame requires ticker")
    return panel


def _optional_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if np.isfinite(result) else None


def _pattern_boundary_breaks(
    frame: pd.DataFrame,
    *,
    start_position: int = 29,
) -> tuple[dict[str, Any], ...]:
    if len(frame) < 31:
        return ()
    active: dict[tuple[str, int], dict[str, Any]] = {}
    retired: set[tuple[str, int]] = set()
    events = []
    for position in range(max(29, start_position), len(frame)):
        current = frame.iloc[position]
        close = float(current["close"])
        current_revision_id = current.get("bar_revision_id")
        for key, pattern in tuple(active.items()):
            direction = pattern["direction"]
            if direction * (close - pattern["boundary_price"]) > 0:
                events.append({
                    **pattern,
                    "confirmation_close": close,
                    "confirmation_revision_id": current_revision_id,
                    "signal_date": _date(frame.index[position]),
                })
                retired.add(key)
                active.pop(key)
            elif direction * (close - pattern["invalidation_price"]) < 0:
                retired.add(key)
                active.pop(key)

        visible = detect_forming_patterns(
            frame.iloc[:position + 1],
            max_patterns=len(PATTERN_NAMES),
            input_includes_forming_bar=False,
        )
        visible_keys = set()
        for pattern in visible:
            bias = pattern.get("bias")
            direction = 1 if bias == "BULLISH" else -1 if bias == "BEARISH" else 0
            boundary = _optional_float(pattern.get("boundary_price"))
            invalidation = _optional_float(pattern.get("invalidation_price"))
            start_time = pattern.get("start_time")
            if (
                direction == 0 or boundary is None or invalidation is None
                or not isinstance(start_time, int)
            ):
                continue
            key = (str(pattern.get("type")), start_time)
            visible_keys.add(key)
            if key in retired:
                continue
            active[key] = {
                "boundary_price": boundary,
                "boundary_revision_id": current_revision_id,
                "direction": direction,
                "formation_snapshot": dict(pattern),
                "formation_start_time": start_time,
                "invalidation_price": invalidation,
                "pattern_type": key[0],
            }
        for key in set(active) - visible_keys:
            retired.add(key)
            active.pop(key)
    return tuple(events)


def _formation_row(
    ticker: str, frame: pd.DataFrame, formation_position: int
) -> Mapping[str, Any] | None:
    if formation_position < 20:
        return None
    formation = frame.iloc[formation_position - 20:formation_position + 1]
    formation_date = _date(frame.index[formation_position]).isoformat()
    return next(
        (
            row for row in scan_gap_strategies(ticker, formation.copy(), interval="1d")
            if row.get("gap_date") == formation_date
        ),
        None,
    )


def _gap_entry_dates(frame: pd.DataFrame, maximum_age: int) -> set[date]:
    valid_formations = set(GapFormationAdapter().candidate_dates(frame))
    candidates = set()
    gaps = [
        *(dict(gap, gap_direction="UP") for gap in identify_gap_up(frame)),
        *(dict(gap, gap_direction="DOWN") for gap in identify_gap_down(frame)),
    ]
    for gap in gaps:
        formation_position = int(gap["index"])
        if (
            _date(frame.index[formation_position]) not in valid_formations
            or not gap.get("range_gap_survived")
        ):
            continue
        for position in range(
            formation_position + 1,
            min(formation_position + maximum_age + 1, len(frame)),
        ):
            current = frame.iloc[position]
            previous_close = float(frame.iloc[position - 1]["close"])
            if gap["gap_direction"] == "UP":
                target = float(gap["prev_high"])
                stop = float(gap["gap_low"])
                if float(current["low"]) <= target:
                    break
                enters = target < float(current["close"]) < stop and previous_close >= stop
            else:
                target = float(gap["prev_low"])
                stop = float(gap["gap_high"])
                if float(current["high"]) >= target:
                    break
                enters = stop < float(current["close"]) < target and previous_close <= stop
            if enters:
                candidates.add(_date(frame.index[position]))
    return candidates


def _action_types(actions: Sequence[Mapping[str, Any]]) -> set[str]:
    return {str(row.get("action_type")) for row in actions}


def _has_excluded_actions(
    context: HistoricalSignalContext,
    frame: pd.DataFrame,
    excluded_action_types: frozenset[str],
) -> bool:
    frame_dates = {_date(value) for value in frame.index}
    return any(
        action_date in frame_dates
        and bool(_action_types(actions) & excluded_action_types)
        for action_date, actions in context.corporate_actions_by_date.items()
    )


def _has_excluded_actions_between(
    context: HistoricalSignalContext,
    start: date,
    end: date,
    excluded_action_types: frozenset[str],
) -> bool:
    return any(
        start <= action_date <= end
        and bool(_action_types(actions) & excluded_action_types)
        for action_date, actions in context.corporate_actions_by_date.items()
    )


def _historical_event(
    *,
    adapter: HistoricalSignalAdapter,
    ticker: str,
    context: HistoricalSignalContext,
    direction: int,
    setup_anchor: str,
    source_rows: Sequence[pd.Series],
    payload: Mapping[str, Any],
) -> HistoricalSignalEvent:
    identity = sha256_json({
        "anchor": setup_anchor,
        "source_name": adapter.source_name,
        "source_version": adapter.source_version,
        "universe_run_id": str(context.universe_run_id),
    })
    source_ids = tuple(
        value for value in (row.get("bar_revision_id") for row in source_rows)
        if isinstance(value, UUID)
    )
    return HistoricalSignalEvent(
        event_id=uuid5(NAMESPACE_URL, f"historical-signal-event:{identity}"),
        source_name=adapter.source_name,
        source_version=adapter.source_version,
        ticker=ticker,
        signal_date=context.signal_date,
        signal_time=context.signal_time,
        direction=direction,
        setup_anchor=setup_anchor,
        universe_run_id=context.universe_run_id,
        universe_policy_version=context.universe_policy_version,
        source_bar_revision_ids=source_ids,
        payload=dict(payload),
    )


def evaluate_historical_signals(
    adapter: HistoricalSignalAdapter,
    frames_by_ticker: Mapping[str, pd.DataFrame],
    *,
    members_by_session: Mapping[date, frozenset[str]],
    universe_ids_by_session: Mapping[date, UUID],
    universe_policy_version: str,
    actions_by_session_ticker: Mapping[
        tuple[date, str], tuple[Mapping[str, Any], ...]
    ],
    frames_by_security: Mapping[UUID, pd.DataFrame] | None = None,
    member_security_ids: Mapping[tuple[date, str], UUID] | None = None,
    actions_by_session_security: Mapping[tuple[date, UUID], tuple[Mapping[str, Any], ...]] | None = None,
    price_manifest_sha256: str | None = None,
    action_manifest_sha256: str | None = None,
    collapse_matches: bool = True,
    evaluation_states: dict | None = None,
) -> HistoricalReplayResult:
    identity_mode = frames_by_security is not None
    identity_arguments = (member_security_ids, actions_by_session_security, price_manifest_sha256, action_manifest_sha256)
    if identity_mode != all(value is not None for value in identity_arguments) \
            or (not identity_mode and any(value is not None for value in identity_arguments)):
        raise ValueError("security-aware replay requires frames, membership, actions and price provenance together")
    if identity_mode:
        if frames_by_ticker or actions_by_session_ticker:
            raise ValueError("cannot mix ticker-only and security-aware replay inputs")
        for digest in (price_manifest_sha256, action_manifest_sha256):
            if len(digest) != 64 or any(value not in "0123456789abcdef" for value in digest):
                raise ValueError("security-aware replay requires valid price and action manifest hashes")
        expected_members = {(session, ticker) for session, tickers in members_by_session.items() for ticker in tickers}
        if set(member_security_ids) != expected_members:
            raise ValueError("security identity must cover every dated universe member")
        if len({(session, identity) for (session, _), identity in member_security_ids.items()}) != len(member_security_ids):
            raise ValueError("a security cannot have multiple member tickers in one session")
        dates_by_listing = {}
        for (session, ticker), identity in member_security_ids.items():
            dates_by_listing.setdefault((ticker, identity), set()).add(session)
        sources = []
        for (ticker, identity), member_dates in sorted(dates_by_listing.items(), key=lambda item: (item[0][0], str(item[0][1]))):
            if identity not in frames_by_security:
                raise ValueError("missing security price frame")
            source = frames_by_security[identity]
            if source.empty or not source.index.is_unique or "security_id" not in source \
                    or any(value != identity for value in source["security_id"]):
                raise ValueError("price frame must contain exactly one security and unique sessions")
            if not member_dates.issubset({_date(value) for value in source.index}):
                raise ValueError("missing price for a dated universe member")
            sources.append((ticker, identity, source))
    else:
        sources = [(ticker, None, source) for ticker, source in sorted(frames_by_ticker.items())]
    events = []
    candidate_count = 0
    exclusions = {"CORPORATE_ACTION": 0, "NOT_IN_UNIVERSE": 0, "INSUFFICIENT_HISTORY": 0}
    for ticker, identity, source in sources:
        frame = source.sort_index()
        ticker_actions = {
            action_date: actions
            for (action_date, action_key), actions in (
                actions_by_session_security if identity_mode else actions_by_session_ticker
            ).items()
            if action_key == (identity if identity_mode else ticker)
        }
        positions = {_date(value): position for position, value in enumerate(frame.index)}
        for signal_date in adapter.candidate_dates(frame):
            members = members_by_session.get(signal_date)
            universe_run_id = universe_ids_by_session.get(signal_date)
            if members is None or universe_run_id is None or ticker not in members or (
                identity_mode and member_security_ids.get((signal_date, ticker)) != identity
            ):
                exclusions["NOT_IN_UNIVERSE"] += 1
                continue
            position = positions[signal_date]
            truncated = frame.iloc[:position + 1]
            maximum_bars = getattr(adapter, "maximum_bars", None)
            if maximum_bars is not None:
                truncated = truncated.iloc[-int(maximum_bars):]
            if len(truncated) < adapter.minimum_bars:
                exclusions["INSUFFICIENT_HISTORY"] += 1
                if evaluation_states is not None:
                    evaluation_states[(ticker, signal_date)] = "INSUFFICIENT_HISTORY"
                continue
            actions = ticker_actions.get(signal_date, ())
            action_types = {str(row.get("action_type")) for row in actions}
            if action_types & adapter.excluded_action_types:
                exclusions["CORPORATE_ACTION"] += 1
                if evaluation_states is not None:
                    evaluation_states[(ticker, signal_date)] = "CORPORATE_ACTION"
                continue
            candidate_count += 1
            signal_time = truncated.iloc[-1]["bar_end"]
            evaluated = adapter.evaluate(
                ticker,
                truncated,
                HistoricalSignalContext(
                    signal_date=signal_date,
                    signal_time=pd.Timestamp(signal_time).to_pydatetime(),
                    universe_run_id=universe_run_id,
                    universe_policy_version=universe_policy_version,
                    corporate_actions=actions,
                    corporate_actions_by_date=(
                        {action_date: values for action_date, values in ticker_actions.items() if action_date <= signal_date}
                        if identity_mode else ticker_actions
                    ),
                ),
            )
            if evaluation_states is not None:
                evaluation_states[(ticker, signal_date)] = "MATCH" if evaluated else "CANDIDATE_REJECTED"
            for event in evaluated:
                if identity_mode:
                    if event.ticker != ticker or event.signal_date != signal_date or event.universe_run_id != universe_run_id:
                        raise ValueError("adapter event disagrees with security-aware replay context")
                    source_ids = tuple(UUID(str(value)) for value in truncated["bar_revision_id"])
                    digest = sha256_json({"adapter_event_id": str(event.event_id), "security_id": str(identity),
                                          "price_manifest_sha256": price_manifest_sha256,
                                          "action_manifest_sha256": action_manifest_sha256,
                                          "source_bar_revision_ids": [str(value) for value in source_ids]})
                    event = replace(
                        event, event_id=uuid5(NAMESPACE_URL, f"security-historical-signal:{digest}"),
                        source_bar_revision_ids=source_ids,
                        payload=dict(event.payload, security_id=str(identity), price_manifest_sha256=price_manifest_sha256,
                                     action_manifest_sha256=action_manifest_sha256),
                    )
                events.append(event)
    events.sort(key=lambda row: (row.signal_time, row.ticker, row.event_id.hex))
    if collapse_matches and getattr(adapter, "collapse_contiguous_matches", False):
        events = list(_collapse_contiguous_events(events))
    return HistoricalReplayResult(tuple(events), candidate_count, exclusions)


def evaluate_strict_daily_signals(
    adapter, frames_by_ticker, *, members_by_session, universe_ids_by_session,
    universe_policy_version, actions_by_session_ticker, member_security_ids,
    source_cutoff,
):
    cutoff = pd.Timestamp(source_cutoff)
    if cutoff.tzinfo is None:
        raise ValueError("source cutoff must be timezone-aware")
    calendar = exchange_calendars.get_calendar("XNYS")
    coverage, events, states, fingerprints = {}, [], {}, {}
    candidates = 0
    exclusions = {}
    for session, tickers in members_by_session.items():
        for ticker in tickers:
            coverage[(ticker, session)] = dict(
                ticker=ticker, session=session.isoformat(), adapter=adapter.source_name,
                adapter_version=adapter.source_version, universe_run_id=str(universe_ids_by_session[session]),
                security_id=str(member_security_ids[(session, ticker)]), state="MISSING_SIGNAL_BAR",
                warmup_bars=0, detection_count=0, event_count=0, event_ids=[],
            )
    for ticker, source in sorted(frames_by_ticker.items()):
        if not source.index.is_unique:
            raise ValueError(f"duplicate ticker/session prices: {ticker}")
        ticker_actions = {key: actions for key, actions in actions_by_session_ticker.items() if key[1] == ticker}
        frame = source.sort_index().copy()
        segments, segment, previous, identity = [], [], None, None
        digest = hashlib.sha256()
        for session, row in frame.iterrows():
            session = _date(session)
            member_identity = member_security_ids.get((session, ticker))
            valid_session = calendar.is_session(pd.Timestamp(session))
            prices = np.asarray([row[column] for column in ("open", "high", "low", "close", "volume")], dtype=float)
            valid_prices = (np.isfinite(prices).all() and min(prices[:4]) > 0 and prices[4] >= 0
                            and prices[1] >= max(prices[0], prices[2], prices[3])
                            and prices[2] <= min(prices[0], prices[1], prices[3]))
            row_identity = row["security_id"]
            reason = None
            if not valid_session or not valid_prices:
                reason = "INVALID_PRICE_INPUT"
            elif pd.Timestamp(row["bar_start"]) != calendar.session_open(pd.Timestamp(session)) \
                    or pd.Timestamp(row["bar_end"]) != calendar.session_close(pd.Timestamp(session)) \
                    or pd.Timestamp(row["replay_available_at"]) != pd.Timestamp(row["bar_end"]) \
                    or not pd.Timestamp(row["bar_end"]) <= pd.Timestamp(row["system_observed_at"]) <= cutoff \
                    or pd.Timestamp(row["created_at"]) > cutoff:
                reason = "INVALID_PRICE_CLOCK"
            elif row_identity is None or (member_identity is not None and row_identity != member_identity) \
                    or bool(row.get("identity_disagreement", False)):
                reason = "IDENTITY_MISMATCH"
            contiguous = previous is not None and valid_session and calendar.next_session(pd.Timestamp(previous)).date() == session
            if reason or row_identity != identity or not contiguous:
                if segment:
                    segments.append(frame.loc[segment])
                segment, digest = [], hashlib.sha256()
            previous, identity = (session, row_identity) if reason is None else (None, None)
            item = coverage.get((ticker, session))
            if reason:
                if item is not None:
                    item["state"] = reason
                continue
            segment.append(session)
            digest.update((str(row["bar_revision_id"]) + ":" + str(row["payload_sha256"]) + "\n").encode("ascii"))
            fingerprints[(ticker, session)] = digest.hexdigest()
            if item is not None:
                item.update(state="NO_SIGNAL" if len(segment) >= adapter.minimum_bars else "INSUFFICIENT_HISTORY",
                            warmup_bars=len(segment))
        if segment:
            segments.append(frame.loc[segment])
        for valid_frame in segments:
            result = evaluate_historical_signals(
                adapter, {ticker: valid_frame}, members_by_session=members_by_session,
                universe_ids_by_session=universe_ids_by_session, universe_policy_version=universe_policy_version,
                actions_by_session_ticker=ticker_actions, collapse_matches=False,
                evaluation_states=states,
            )
            candidates += result.candidate_count
            for key, count in result.exclusion_counts.items():
                exclusions[key] = exclusions.get(key, 0) + count
            for event in result.events:
                item = coverage[(ticker, event.signal_date)]
                action_rows = [dict(action) for (action_date, action_ticker), actions in sorted(ticker_actions.items())
                               if action_ticker == ticker and action_date <= event.signal_date for action in actions]
                lineage = dict(replay_contract=DAILY_REPLAY_CONTRACT, source_cutoff=cutoff.isoformat(),
                               security_id=item["security_id"], input_prefix_sha256=fingerprints[(ticker, event.signal_date)],
                               action_context_sha256=sha256_json(json.loads(json.dumps(action_rows, default=str))))
                payload = dict(event.payload, **lineage)
                event = replace(event, event_id=uuid5(NAMESPACE_URL, "strict-daily-event:" + sha256_json({
                    "adapter_event_id": str(event.event_id), "payload": payload,
                })), payload=payload)
                events.append(event)
                item["detection_count"] += 1
    for key, state in states.items():
        if key in coverage:
            coverage[key]["state"] = state
    if getattr(adapter, "collapse_contiguous_matches", False):
        events = list(_collapse_contiguous_events(events))
    for event in events:
        item = coverage[(event.ticker, event.signal_date)]
        item["event_count"] += 1
        item["event_ids"].append(str(event.event_id))
    for item in coverage.values():
        if item["detection_count"] and not item["event_count"]:
            item["state"] = "MATCH_CONTINUATION"
    events.sort(key=lambda row: (row.signal_time, row.ticker, row.event_id.hex))
    ordered = tuple(coverage[key] for key in sorted(coverage, key=lambda key: (key[1], key[0])))
    return HistoricalReplayResult(tuple(events), candidates, exclusions, ordered)


def _date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    return pd.Timestamp(value).date()


def _collapse_contiguous_events(
    events: Sequence[HistoricalSignalEvent],
) -> tuple[HistoricalSignalEvent, ...]:
    calendar = exchange_calendars.get_calendar("XNYS")
    kept = []
    last_match: dict[tuple[str, str, int, str | None], date] = {}
    for event in sorted(events, key=lambda row: (row.ticker, row.signal_date, row.event_id.hex)):
        key = (event.source_name, event.ticker, event.direction, event.payload.get("security_id"))
        previous = last_match.get(key)
        expected_previous = pd.Timestamp(
            calendar.previous_session(pd.Timestamp(event.signal_date))
        ).date()
        if previous != expected_previous:
            kept.append(event)
        last_match[key] = event.signal_date
    kept.sort(key=lambda row: (row.signal_time, row.ticker, row.event_id.hex))
    return tuple(kept)