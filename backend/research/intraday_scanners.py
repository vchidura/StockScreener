"""Predeclared intraday scanner detectors for canonical 30m bars.

Every threshold in ``INTRADAY_DETECTOR_POLICY`` is frozen before the first outcome
run. Changing any value requires a new ``source_version`` because qualification
revisions are keyed by ``(source_name, source_version)``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import hashlib
import numpy as np
import pandas as pd


DETECTOR_POLICY_VERSION = "intraday_scanner_policy_v1"
VWAP_POLICY_VERSION = "session_cumulative_bar_vwap_v1"

OPENING_RANGE_SOURCE = "INTRADAY_OPENING_RANGE_BREAKOUT_CONTINUATION"
FAILED_OPENING_RANGE_SOURCE = "INTRADAY_FAILED_OPENING_BREAKOUT_REVERSAL"
VWAP_SOURCE = "INTRADAY_VWAP_RECLAIM_REJECTION"
TREND_PULLBACK_SOURCE = "INTRADAY_TREND_PULLBACK"

# Harness controls. Not hypotheses: they calibrate the evaluator itself.
CONTROL_ORACLE_SOURCE = "CONTROL_LOOKAHEAD_ORACLE"
CONTROL_RANDOM_SOURCE = "CONTROL_RANDOM_DIRECTION"
CONTROL_SAMPLE_MODULUS = 32


@dataclass(frozen=True, slots=True)
class IntradayScannerRegistration:
    source_name: str
    source_version: str
    supported_intervals: tuple[str, ...] = ("30m",)
    outcome_modes: tuple[str, ...] = ("DIRECTIONAL_HORIZON", "RECOMMENDATION_PLAN")
    required_plan_fields: tuple[str, ...] = ("stop_price", "target_price")
    is_control: bool = False


INTRADAY_SCANNER_REGISTRY: dict[str, IntradayScannerRegistration] = {
    registration.source_name: registration
    for registration in (
        IntradayScannerRegistration(OPENING_RANGE_SOURCE, "opening_range_breakout_v1"),
        IntradayScannerRegistration(
            FAILED_OPENING_RANGE_SOURCE, "failed_opening_breakout_v1"
        ),
        IntradayScannerRegistration(VWAP_SOURCE, "vwap_reclaim_rejection_v1"),
        IntradayScannerRegistration(
            TREND_PULLBACK_SOURCE, "intraday_trend_pullback_v1"
        ),
        IntradayScannerRegistration(
            CONTROL_ORACLE_SOURCE, "lookahead_oracle_v1",
            outcome_modes=("DIRECTIONAL_HORIZON",), is_control=True,
        ),
        IntradayScannerRegistration(
            CONTROL_RANDOM_SOURCE, "random_direction_v1",
            outcome_modes=("DIRECTIONAL_HORIZON",), is_control=True,
        ),
    )
}

CONTROL_SOURCES = tuple(
    name for name, registration in INTRADAY_SCANNER_REGISTRY.items()
    if registration.is_control
)

INTRADAY_SCANNER_VERSIONS = {
    name: registration.source_version
    for name, registration in INTRADAY_SCANNER_REGISTRY.items()
}

# Elapsed regular-session 30m bar boundaries after the next executable open.
INTRADAY_OUTCOME_HORIZONS: dict[str, dict[str, int]] = {
    "30m": {"30m": 1, "60m": 2, "120m": 4},
}

INTRADAY_DETECTOR_POLICY: Mapping[str, Any] = {
    "atr_period": 14,
    "benchmark_adverse_return": 0.003,
    "close_location_bearish": 0.35,
    "close_location_bullish": 0.65,
    "detector_policy_version": DETECTOR_POLICY_VERSION,
    "ema_fast": 8,
    "ema_mid": 21,
    "ema_slow": 55,
    "invalidation_buffer_atr": 0.10,
    "minimum_history_bars": 60,
    "opening_range_bars": 1,
    "opening_range_breakout_buffer_atr": 0.10,
    "pullback_max_bars": 2,
    "pullback_touch_atr": 0.50,
    "relative_volume_multiple": 1.0,
    "resumption_close_location": 0.60,
    "reward_to_risk": 2.0,
    "trend_age_bars": 4,
    "volume_baseline_sessions": 20,
    "vwap_policy_version": VWAP_POLICY_VERSION,
}

REQUIRED_COLUMNS = (
    "bar_revision_id", "session_date", "bar_start", "bar_end",
    "open", "high", "low", "close", "volume", "vwap",
)

EVENT_COLUMNS = (
    "scanner_name", "scanner_version", "ticker", "direction", "session_date",
    "signal_time", "signal_bar_id", "setup_anchor", "entry_price", "stop_price",
    "target_price", "atr_at_signal", "reference_level",
    "source_bar_revision_ids", "metadata",
)


@dataclass(frozen=True, slots=True)
class BenchmarkContext:
    """Return from the session's first regular bar open through each bar end."""

    ticker: str
    session_returns: Mapping[pd.Timestamp, float]

    def value(self, signal_time: pd.Timestamp) -> float | None:
        value = self.session_returns.get(signal_time)
        return None if value is None or not np.isfinite(value) else float(value)


def benchmark_context(ticker: str, frame: pd.DataFrame) -> BenchmarkContext:
    """Build session-anchored benchmark returns keyed by finalized bar end."""
    prepared = prepare_intraday_frame(frame)
    if prepared.empty:
        return BenchmarkContext(ticker, {})
    session_open = prepared.groupby("session_date")["open"].transform("first")
    returns = (prepared["close"] - session_open) / session_open
    return BenchmarkContext(
        ticker,
        {
            pd.Timestamp(bar_end): float(value)
            for bar_end, value in zip(prepared["bar_end"], returns)
            if np.isfinite(value)
        },
    )


def prepare_intraday_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize a single-ticker 30m bar frame and derive point-in-time features."""
    missing = [name for name in REQUIRED_COLUMNS if name not in frame.columns]
    if missing:
        raise ValueError(f"intraday frame is missing columns: {missing}")
    prepared = frame.copy()
    prepared["bar_start"] = pd.to_datetime(prepared["bar_start"], utc=True)
    prepared["bar_end"] = pd.to_datetime(prepared["bar_end"], utc=True)
    prepared = prepared.sort_values("bar_start").reset_index(drop=True)
    for column in ("open", "high", "low", "close", "volume", "vwap"):
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
    if prepared.empty:
        return prepared

    prepared["slot"] = prepared.groupby("session_date").cumcount()
    prepared["session_bars"] = prepared.groupby("session_date")["slot"].transform("size")

    high = prepared["high"]
    low = prepared["low"]
    previous_close = prepared["close"].shift(1)
    true_range = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    period = int(INTRADAY_DETECTOR_POLICY["atr_period"])
    prepared["atr"] = true_range.ewm(
        alpha=1.0 / period, adjust=False, min_periods=period
    ).mean()

    span = high - low
    prepared["close_location"] = np.where(
        span > 0, (prepared["close"] - low) / span.replace(0, np.nan), np.nan
    )

    # A missing bar VWAP poisons the rest of its session; proxies are not permitted.
    missing = prepared["vwap"].isna() | prepared["volume"].isna()
    poisoned = missing.groupby(prepared["session_date"]).cummax()
    weighted = (prepared["vwap"] * prepared["volume"]).fillna(0.0)
    numerator = weighted.groupby(prepared["session_date"]).cumsum()
    denominator = prepared["volume"].fillna(0.0).groupby(prepared["session_date"]).cumsum()
    prepared["session_vwap"] = (numerator / denominator.replace(0, np.nan)).mask(poisoned)

    sessions = int(INTRADAY_DETECTOR_POLICY["volume_baseline_sessions"])
    prepared["slot_volume_baseline"] = (
        prepared.groupby("slot")["volume"]
        .transform(lambda values: values.shift(1).rolling(sessions, min_periods=sessions).median())
    )

    closes = prepared["close"]
    for name, key in (("ema_fast", "ema_fast"), ("ema_mid", "ema_mid"), ("ema_slow", "ema_slow")):
        span_length = int(INTRADAY_DETECTOR_POLICY[key])
        prepared[name] = closes.ewm(span=span_length, adjust=False, min_periods=span_length).mean()

    opening = prepared[prepared["slot"] == 0].set_index("session_date")
    prepared["opening_high"] = prepared["session_date"].map(opening["high"])
    prepared["opening_low"] = prepared["session_date"].map(opening["low"])
    prepared["opening_close"] = prepared["session_date"].map(opening["close"])
    prepared["opening_vwap"] = prepared["session_date"].map(opening["session_vwap"])
    prepared["opening_bar_id"] = prepared["session_date"].map(opening["bar_revision_id"])
    return prepared


def build_intraday_scanner_events(
    ticker: str,
    frame: pd.DataFrame,
    *,
    market: BenchmarkContext | None = None,
    sector: BenchmarkContext | None = None,
    scanners: Sequence[str] = (),
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Return predeclared intraday events plus explicit unavailability counts."""
    selected = tuple(scanners) or tuple(INTRADAY_SCANNER_REGISTRY)
    unknown = set(selected) - set(INTRADAY_SCANNER_REGISTRY)
    if unknown:
        raise ValueError(f"unknown intraday scanners: {sorted(unknown)}")
    prepared = prepare_intraday_frame(frame)
    diagnostics: dict[str, int] = {}
    if prepared.empty:
        return [], diagnostics
    events: list[dict[str, Any]] = []
    if OPENING_RANGE_SOURCE in selected:
        events.extend(
            _opening_range_events(ticker, prepared, market, sector, diagnostics)
        )
    if FAILED_OPENING_RANGE_SOURCE in selected:
        events.extend(_failed_opening_range_events(ticker, prepared, diagnostics))
    if VWAP_SOURCE in selected:
        events.extend(_vwap_events(ticker, prepared, diagnostics))
    if TREND_PULLBACK_SOURCE in selected:
        events.extend(_trend_pullback_events(ticker, prepared, diagnostics))
    for control in (CONTROL_ORACLE_SOURCE, CONTROL_RANDOM_SOURCE):
        if control in selected:
            events.extend(_control_events(control, ticker, prepared))
    events.sort(key=lambda row: (row["signal_time"], row["scanner_name"], row["direction"]))
    return events, diagnostics


def _count(diagnostics: dict[str, int], key: str) -> None:
    diagnostics[key] = diagnostics.get(key, 0) + 1


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) else None


def _signal_positions(prepared: pd.DataFrame) -> list[int]:
    """Bars eligible to signal: after the opening bar, before the session close."""
    slot = prepared["slot"].to_numpy()
    session_bars = prepared["session_bars"].to_numpy()
    minimum = int(INTRADAY_DETECTOR_POLICY["minimum_history_bars"])
    opening_bars = int(INTRADAY_DETECTOR_POLICY["opening_range_bars"])
    return [
        position
        for position in range(minimum, len(prepared))
        if slot[position] >= opening_bars and slot[position] < session_bars[position] - 1
    ]


def _plan(direction: int, entry: float, stop: float | None,
          atr: float | None) -> tuple[float | None, float | None]:
    if stop is None or not np.isfinite(stop) or direction * (entry - stop) <= 0:
        if atr is None or not np.isfinite(atr) or atr <= 0:
            return None, None
        stop = entry - direction * atr
    risk = abs(entry - stop)
    if risk <= 0:
        return None, None
    reward = float(INTRADAY_DETECTOR_POLICY["reward_to_risk"])
    return round(float(stop), 8), round(float(entry + direction * reward * risk), 8)


def _event(
    *,
    scanner_name: str,
    ticker: str,
    row: pd.Series,
    direction: int,
    trigger_type: str,
    reference_level: float | None,
    stop: float | None,
    source_bar_ids: Sequence[Any],
    metadata: dict[str, Any],
) -> dict[str, Any] | None:
    entry = _finite(row["close"])
    atr = _finite(row["atr"])
    if entry is None or atr is None or atr <= 0:
        return None
    stop_price, target_price = _plan(direction, entry, stop, atr)
    if stop_price is None or target_price is None:
        return None
    signal_time = pd.Timestamp(row["bar_end"])
    session_date = row["session_date"]
    setup_anchor = (
        f"{ticker}:{session_date}:{scanner_name}:"
        f"{'LONG' if direction == 1 else 'SHORT'}:{signal_time.isoformat()}"
    )
    return {
        "scanner_name": scanner_name,
        "scanner_version": INTRADAY_SCANNER_VERSIONS[scanner_name],
        "ticker": ticker,
        "direction": direction,
        "session_date": session_date,
        "signal_time": signal_time,
        "signal_bar_id": row["bar_revision_id"],
        "setup_anchor": setup_anchor,
        "entry_price": round(entry, 8),
        "stop_price": stop_price,
        "target_price": target_price,
        "atr_at_signal": round(atr, 8),
        "reference_level": (
            round(float(reference_level), 8) if reference_level is not None else None
        ),
        "source_bar_revision_ids": tuple(
            value for value in source_bar_ids if value is not None
        ),
        "metadata": {
            "detector_policy_version": DETECTOR_POLICY_VERSION,
            "trigger_type": trigger_type,
            "slot": int(row["slot"]),
            "session_bars": int(row["session_bars"]),
            "close_location": _finite(row["close_location"]),
            "relative_volume": _finite(
                row["volume"] / row["slot_volume_baseline"]
            ),
            "slot_volume_baseline": _finite(row["slot_volume_baseline"]),
            **metadata,
        },
    }


def _benchmark_ok(
    context: BenchmarkContext | None,
    signal_time: pd.Timestamp,
    direction: int,
    diagnostics: dict[str, int],
    label: str,
) -> tuple[bool, float | None]:
    if context is None:
        _count(diagnostics, f"{label}_context_unavailable")
        return False, None
    value = context.value(signal_time)
    if value is None:
        _count(diagnostics, f"{label}_context_unavailable")
        return False, None
    threshold = float(INTRADAY_DETECTOR_POLICY["benchmark_adverse_return"])
    return direction * value >= -threshold, value


def _opening_range_events(
    ticker: str,
    prepared: pd.DataFrame,
    market: BenchmarkContext | None,
    sector: BenchmarkContext | None,
    diagnostics: dict[str, int],
) -> list[dict[str, Any]]:
    buffer_atr = float(INTRADAY_DETECTOR_POLICY["opening_range_breakout_buffer_atr"])
    invalidation = float(INTRADAY_DETECTOR_POLICY["invalidation_buffer_atr"])
    volume_multiple = float(INTRADAY_DETECTOR_POLICY["relative_volume_multiple"])
    events: list[dict[str, Any]] = []
    outside_state: dict[tuple[Any, int], bool] = {}
    for position in _signal_positions(prepared):
        row = prepared.iloc[position]
        session = row["session_date"]
        close = _finite(row["close"])
        atr = _finite(row["atr"])
        range_high = _finite(row["opening_high"])
        range_low = _finite(row["opening_low"])
        if close is None or atr is None or atr <= 0 or range_high is None or range_low is None:
            continue
        for direction, level in ((1, range_high), (-1, range_low)):
            key = (session, direction)
            outside = direction * (close - level) > 0
            was_outside = outside_state.get(key, False)
            outside_state[key] = outside
            if not outside or was_outside:
                continue
            if direction * (close - level) < buffer_atr * atr:
                continue
            baseline = _finite(row["slot_volume_baseline"])
            if baseline is None:
                _count(diagnostics, "opening_range_volume_baseline_unavailable")
                continue
            if not float(row["volume"]) > volume_multiple * baseline:
                continue
            signal_time = pd.Timestamp(row["bar_end"])
            market_ok, market_return = _benchmark_ok(
                market, signal_time, direction, diagnostics, "market"
            )
            sector_ok, sector_return = _benchmark_ok(
                sector, signal_time, direction, diagnostics, "sector"
            )
            if not market_ok or not sector_ok:
                continue
            event = _event(
                scanner_name=OPENING_RANGE_SOURCE,
                ticker=ticker,
                row=row,
                direction=direction,
                trigger_type=(
                    "opening_range_breakout_long" if direction == 1
                    else "opening_range_breakout_short"
                ),
                reference_level=level,
                stop=level - direction * invalidation * atr,
                source_bar_ids=(row["opening_bar_id"], row["bar_revision_id"]),
                metadata={
                    "opening_range_high": round(range_high, 8),
                    "opening_range_low": round(range_low, 8),
                    "opening_range_bar_id": (
                        str(row["opening_bar_id"])
                        if row["opening_bar_id"] is not None else None
                    ),
                    "breakout_distance_atr": round(
                        direction * (close - level) / atr, 6
                    ),
                    "market_benchmark_ticker": market.ticker if market else None,
                    "market_session_return": market_return,
                    "sector_benchmark_ticker": sector.ticker if sector else None,
                    "sector_session_return": sector_return,
                    "qualification_eligible": True,
                },
            )
            if event is not None:
                events.append(event)
    return events


def _failed_opening_range_events(
    ticker: str,
    prepared: pd.DataFrame,
    diagnostics: dict[str, int],
) -> list[dict[str, Any]]:
    invalidation = float(INTRADAY_DETECTOR_POLICY["invalidation_buffer_atr"])
    volume_multiple = float(INTRADAY_DETECTOR_POLICY["relative_volume_multiple"])
    events: list[dict[str, Any]] = []
    breaks: dict[tuple[Any, int], dict[str, Any]] = {}
    for position in _signal_positions(prepared):
        row = prepared.iloc[position]
        session = row["session_date"]
        close = _finite(row["close"])
        atr = _finite(row["atr"])
        range_high = _finite(row["opening_high"])
        range_low = _finite(row["opening_low"])
        if close is None or atr is None or atr <= 0 or range_high is None or range_low is None:
            continue
        for break_side, level in ((1, range_high), (-1, range_low)):
            key = (session, break_side)
            state = breaks.get(key)
            if break_side * (close - level) > 0:
                if state is None:
                    breaks[key] = {
                        "bar_id": row["bar_revision_id"],
                        "signal_time": pd.Timestamp(row["bar_end"]),
                        "extreme": float(row["high" if break_side == 1 else "low"]),
                        "bars": 1,
                    }
                else:
                    state["bars"] += 1
                    state["extreme"] = (
                        max(state["extreme"], float(row["high"])) if break_side == 1
                        else min(state["extreme"], float(row["low"]))
                    )
                continue
            if state is None:
                continue
            breaks.pop(key, None)
            baseline = _finite(row["slot_volume_baseline"])
            if baseline is None:
                _count(diagnostics, "failed_opening_range_volume_baseline_unavailable")
                continue
            if not float(row["volume"]) > volume_multiple * baseline:
                continue
            direction = -break_side
            extreme = float(state["extreme"])
            stop = extreme - direction * invalidation * atr
            event = _event(
                scanner_name=FAILED_OPENING_RANGE_SOURCE,
                ticker=ticker,
                row=row,
                direction=direction,
                trigger_type=(
                    "failed_upside_breakout" if break_side == 1
                    else "failed_downside_breakout"
                ),
                reference_level=level,
                stop=stop,
                source_bar_ids=(
                    row["opening_bar_id"], state["bar_id"], row["bar_revision_id"],
                ),
                metadata={
                    "opening_range_high": round(range_high, 8),
                    "opening_range_low": round(range_low, 8),
                    "opening_range_bar_id": (
                        str(row["opening_bar_id"])
                        if row["opening_bar_id"] is not None else None
                    ),
                    "break_side": "upside" if break_side == 1 else "downside",
                    "break_bar_id": str(state["bar_id"]),
                    "break_bar_time": state["signal_time"].isoformat(),
                    "break_bars_outside": int(state["bars"]),
                    "break_extreme": round(extreme, 8),
                    "return_inside_atr": round(abs(close - level) / atr, 6),
                    "qualification_eligible": True,
                },
            )
            if event is not None:
                events.append(event)
    return events


def _vwap_events(
    ticker: str,
    prepared: pd.DataFrame,
    diagnostics: dict[str, int],
) -> list[dict[str, Any]]:
    invalidation = float(INTRADAY_DETECTOR_POLICY["invalidation_buffer_atr"])
    volume_multiple = float(INTRADAY_DETECTOR_POLICY["relative_volume_multiple"])
    bullish_location = float(INTRADAY_DETECTOR_POLICY["close_location_bullish"])
    bearish_location = float(INTRADAY_DETECTOR_POLICY["close_location_bearish"])
    events: list[dict[str, Any]] = []
    last_cross: dict[Any, int] = {}
    for position in _signal_positions(prepared):
        row = prepared.iloc[position]
        previous = prepared.iloc[position - 1]
        session = row["session_date"]
        if previous["session_date"] != session:
            continue
        close = _finite(row["close"])
        atr = _finite(row["atr"])
        vwap = _finite(row["session_vwap"])
        previous_vwap = _finite(previous["session_vwap"])
        previous_close = _finite(previous["close"])
        opening_close = _finite(row["opening_close"])
        opening_vwap = _finite(row["opening_vwap"])
        if None in (close, atr, vwap, previous_vwap, previous_close, opening_close, opening_vwap):
            _count(diagnostics, "vwap_session_vwap_unavailable")
            continue
        if atr <= 0:
            continue
        if close > vwap and previous_close < previous_vwap:
            direction = 1
        elif close < vwap and previous_close > previous_vwap:
            direction = -1
        else:
            continue
        if last_cross.get(session) == direction:
            continue
        last_cross[session] = direction
        displacement = (opening_vwap - opening_close) / atr * direction
        if displacement <= 0:
            continue
        location = _finite(row["close_location"])
        if location is None:
            continue
        if direction == 1 and location < bullish_location:
            continue
        if direction == -1 and location > bearish_location:
            continue
        baseline = _finite(row["slot_volume_baseline"])
        if baseline is None:
            _count(diagnostics, "vwap_volume_baseline_unavailable")
            continue
        if not float(row["volume"]) > volume_multiple * baseline:
            continue
        event = _event(
            scanner_name=VWAP_SOURCE,
            ticker=ticker,
            row=row,
            direction=direction,
            trigger_type="vwap_reclaim" if direction == 1 else "vwap_rejection",
            reference_level=vwap,
            stop=vwap - direction * invalidation * atr,
            source_bar_ids=(
                row["opening_bar_id"], previous["bar_revision_id"],
                row["bar_revision_id"],
            ),
            metadata={
                "vwap_policy_version": VWAP_POLICY_VERSION,
                "session_vwap": round(vwap, 8),
                "prior_session_vwap": round(previous_vwap, 8),
                "prior_close": round(previous_close, 8),
                "opening_displacement_atr": round(displacement, 6),
                "opening_close": round(opening_close, 8),
                "opening_vwap": round(opening_vwap, 8),
                "prior_bar_id": str(previous["bar_revision_id"]),
                "qualification_eligible": True,
            },
        )
        if event is not None:
            events.append(event)
    return events


def _trend_pullback_events(
    ticker: str,
    prepared: pd.DataFrame,
    diagnostics: dict[str, int],
) -> list[dict[str, Any]]:
    invalidation = float(INTRADAY_DETECTOR_POLICY["invalidation_buffer_atr"])
    trend_age = int(INTRADAY_DETECTOR_POLICY["trend_age_bars"])
    max_pullback = int(INTRADAY_DETECTOR_POLICY["pullback_max_bars"])
    touch_atr = float(INTRADAY_DETECTOR_POLICY["pullback_touch_atr"])
    resumption_location = float(INTRADAY_DETECTOR_POLICY["resumption_close_location"])
    events: list[dict[str, Any]] = []
    last_emitted: dict[int, int] = {}
    for position in _signal_positions(prepared):
        row = prepared.iloc[position]
        atr = _finite(row["atr"])
        location = _finite(row["close_location"])
        if atr is None or atr <= 0 or location is None:
            continue
        for direction in (1, -1):
            if position - last_emitted.get(direction, -10 ** 9) < trend_age:
                continue
            if not _stack_holds(prepared, position, direction, trend_age):
                continue
            if direction == 1 and location < resumption_location:
                continue
            if direction == -1 and location > 1.0 - resumption_location:
                continue
            pullback = _pullback_window(
                prepared, position, direction, max_pullback, touch_atr
            )
            if pullback is None:
                continue
            impulse_position, pullback_positions = pullback
            resumption_reference = float(
                prepared.iloc[position - 1]["high" if direction == 1 else "low"]
            )
            if direction * (float(row["close"]) - resumption_reference) <= 0:
                continue
            fast = _finite(row["ema_fast"])
            if fast is None or direction * (float(row["close"]) - fast) <= 0:
                continue
            baselines = [
                _finite(prepared.iloc[index]["slot_volume_baseline"])
                for index in pullback_positions
            ]
            if any(value is None for value in baselines):
                _count(diagnostics, "trend_pullback_volume_baseline_unavailable")
                continue
            contracted = all(
                float(prepared.iloc[index]["volume"]) < baseline
                and float(prepared.iloc[index]["volume"])
                < float(prepared.iloc[impulse_position]["volume"])
                for index, baseline in zip(pullback_positions, baselines)
            )
            if not contracted:
                continue
            extremes = [
                float(prepared.iloc[index]["low" if direction == 1 else "high"])
                for index in pullback_positions
            ]
            pivot = min(extremes) if direction == 1 else max(extremes)
            mid = _finite(row["ema_mid"])
            slow = _finite(row["ema_slow"])
            event = _event(
                scanner_name=TREND_PULLBACK_SOURCE,
                ticker=ticker,
                row=row,
                direction=direction,
                trigger_type=(
                    "intraday_trend_pullback_long" if direction == 1
                    else "intraday_trend_pullback_short"
                ),
                reference_level=mid,
                stop=pivot - direction * invalidation * atr,
                source_bar_ids=(
                    prepared.iloc[impulse_position]["bar_revision_id"],
                    *(prepared.iloc[index]["bar_revision_id"] for index in pullback_positions),
                    row["bar_revision_id"],
                ),
                metadata={
                    "ema_fast": fast,
                    "ema_mid": mid,
                    "ema_slow": slow,
                    "trend_age_bars": trend_age,
                    "impulse_bar_id": str(
                        prepared.iloc[impulse_position]["bar_revision_id"]
                    ),
                    "pullback_bar_ids": [
                        str(prepared.iloc[index]["bar_revision_id"])
                        for index in pullback_positions
                    ],
                    "pullback_bars": len(pullback_positions),
                    "pullback_pivot": round(pivot, 8),
                    "pullback_depth_atr": round(
                        abs(float(prepared.iloc[impulse_position][
                            "high" if direction == 1 else "low"
                        ]) - pivot) / atr, 6
                    ),
                    "vwap_distance_atr": (
                        round((float(row["close"]) - float(row["session_vwap"])) / atr, 6)
                        if _finite(row["session_vwap"]) is not None else None
                    ),
                    "invalidation_level": round(pivot, 8),
                    "qualification_eligible": True,
                },
            )
            if event is not None:
                events.append(event)
                last_emitted[direction] = position
    return events


def _entry_position(prepared: pd.DataFrame, position: int) -> int | None:
    """Bar the evaluator will actually enter on.

    Evidence is observed at bar_end(t), which equals bar_start(t+1), and entry
    must begin strictly after observation, so t+2 is the first tradable bar.
    """
    entry = position + 2
    if entry >= len(prepared):
        return None
    if prepared.iloc[entry]["session_date"] != prepared.iloc[position]["session_date"]:
        return None
    return entry


def _control_sampled(bar_revision_id: Any) -> bool:
    digest = hashlib.sha256(str(bar_revision_id).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % CONTROL_SAMPLE_MODULUS == 0


def _control_events(
    scanner_name: str,
    ticker: str,
    prepared: pd.DataFrame,
) -> list[dict[str, Any]]:
    """Deterministic calibration lanes with a known correct answer.

    The oracle reads the entry bar's own return and must qualify. The random
    lane must not. Any other result means the evaluator is wired incorrectly.
    """
    events: list[dict[str, Any]] = []
    for position in _signal_positions(prepared):
        row = prepared.iloc[position]
        if not _control_sampled(row["bar_revision_id"]):
            continue
        entry_position = _entry_position(prepared, position)
        if entry_position is None:
            continue
        entry_row = prepared.iloc[entry_position]
        if scanner_name == CONTROL_ORACLE_SOURCE:
            move = float(entry_row["close"]) - float(entry_row["open"])
            if move == 0:
                continue
            direction = 1 if move > 0 else -1
        else:
            digest = hashlib.sha256(
                f"random-control:{row['bar_revision_id']}".encode("utf-8")
            ).digest()
            direction = 1 if digest[0] % 2 == 0 else -1
        event = _event(
            scanner_name=scanner_name,
            ticker=ticker,
            row=row,
            direction=direction,
            trigger_type=(
                "lookahead_oracle" if scanner_name == CONTROL_ORACLE_SOURCE
                else "random_direction"
            ),
            reference_level=None,
            stop=None,
            source_bar_ids=(row["bar_revision_id"],),
            metadata={
                "control": True,
                "entry_bar_id": str(entry_row["bar_revision_id"]),
                "sample_modulus": CONTROL_SAMPLE_MODULUS,
                "qualification_eligible": True,
            },
        )
        if event is not None:
            events.append(event)
    return events


def _stack_holds(
    prepared: pd.DataFrame, position: int, direction: int, trend_age: int
) -> bool:
    start = position - trend_age + 1
    if start < 0:
        return False
    window = prepared.iloc[start:position + 1]
    fast = window["ema_fast"].to_numpy(dtype=float)
    mid = window["ema_mid"].to_numpy(dtype=float)
    slow = window["ema_slow"].to_numpy(dtype=float)
    if not np.isfinite(fast).all() or not np.isfinite(mid).all() or not np.isfinite(slow).all():
        return False
    if direction == 1:
        return bool(np.all(fast > mid) and np.all(mid > slow))
    return bool(np.all(fast < mid) and np.all(mid < slow))


def _pullback_window(
    prepared: pd.DataFrame,
    position: int,
    direction: int,
    max_pullback: int,
    touch_atr: float,
) -> tuple[int, tuple[int, ...]] | None:
    """Locate the contiguous session-scoped pullback ending at ``position - 1``."""
    session = prepared.iloc[position]["session_date"]
    for length in range(1, max_pullback + 1):
        pullback_positions = tuple(range(position - length, position))
        impulse_position = position - length - 1
        if impulse_position < 0:
            return None
        window = prepared.iloc[impulse_position:position]
        if not (window["session_date"] == session).all():
            return None
        impulse = prepared.iloc[impulse_position]
        valid = True
        for index in pullback_positions:
            bar = prepared.iloc[index]
            reference = prepared.iloc[index - 1]
            atr = _finite(bar["atr"])
            mid = _finite(bar["ema_mid"])
            slow = _finite(bar["ema_slow"])
            vwap = _finite(bar["session_vwap"])
            if atr is None or atr <= 0 or mid is None or slow is None:
                valid = False
                break
            if direction * (float(bar["close"]) - float(reference["close"])) >= 0:
                valid = False
                break
            if direction * (float(bar["close"]) - slow) <= 0:
                valid = False
                break
            extreme = float(bar["low" if direction == 1 else "high"])
            anchors = [mid] + ([vwap] if vwap is not None else [])
            if not any(abs(extreme - anchor) <= touch_atr * atr for anchor in anchors):
                valid = False
                break
        if not valid:
            continue
        if direction * (
            float(impulse["close"]) - float(prepared.iloc[impulse_position - 1]["close"])
            if impulse_position > 0 else 1.0
        ) <= 0:
            continue
        return impulse_position, pullback_positions
    return None
