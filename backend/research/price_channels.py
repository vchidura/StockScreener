from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from research.forming_patterns import _fit_line
from research.price_structures import _confirmed_pivots


def _timestamp(value: Any) -> int:
    return int(pd.Timestamp(value).timestamp())


def _point(frame: pd.DataFrame, index: int, price: float) -> dict:
    return {"time": _timestamp(frame.index[index]), "price": round(float(price), 4)}


def _channel_candidates(
    frame: pd.DataFrame,
    atr: pd.Series,
    pivots: list[dict],
) -> list[dict]:
    candidates: list[dict] = []
    frame_end = len(frame) - 1
    for start_offset in range(max(0, len(pivots) - 14), max(0, len(pivots) - 3)):
        points = pivots[start_offset:]
        highs = [pivot for pivot in points if pivot["type"] == "high"]
        lows = [pivot for pivot in points if pivot["type"] == "low"]
        if len(points) < 5 or len(highs) < 2 or len(lows) < 2:
            continue

        start_index = points[0]["index"]
        last_pivot_index = points[-1]["index"]
        formation_bars = frame_end - start_index + 1
        if formation_bars < 20 or formation_bars > 160 or frame_end - last_pivot_index > 15:
            continue

        local_atr = float(atr.iloc[-1])
        if not math.isfinite(local_atr) or local_atr <= 1e-9:
            continue
        upper = _fit_line(highs, local_atr)
        lower = _fit_line(lows, local_atr)
        if upper is None or lower is None:
            continue

        slope_gap = abs(upper["slope_atr_per_bar"] - lower["slope_atr_per_bar"])
        average_slope_atr = (
            upper["slope_atr_per_bar"] + lower["slope_atr_per_bar"]
        ) / 2
        if slope_gap > 0.03 or abs(average_slope_atr) < 0.02:
            continue

        separate_width_start = (
            upper["slope"] * start_index + upper["intercept"]
            - lower["slope"] * start_index - lower["intercept"]
        )
        separate_width_end = (
            upper["slope"] * frame_end + upper["intercept"]
            - lower["slope"] * frame_end - lower["intercept"]
        )
        if separate_width_start <= 0 or separate_width_end <= 0:
            continue
        width_change = abs(separate_width_end / separate_width_start - 1)
        if width_change > 0.25:
            continue

        common_slope = (upper["slope"] + lower["slope"]) / 2
        upper_intercept = float(np.mean([
            pivot["price"] - common_slope * pivot["index"] for pivot in highs
        ]))
        lower_intercept = float(np.mean([
            pivot["price"] - common_slope * pivot["index"] for pivot in lows
        ]))
        channel_width = upper_intercept - lower_intercept
        width_atr = channel_width / local_atr
        if channel_width <= 0 or width_atr < 1.5 or width_atr > 12:
            continue

        residuals = [
            abs(pivot["price"] - (common_slope * pivot["index"] + upper_intercept))
            for pivot in highs
        ] + [
            abs(pivot["price"] - (common_slope * pivot["index"] + lower_intercept))
            for pivot in lows
        ]
        fit_error_atr = float(np.mean(residuals) / local_atr)
        if fit_error_atr > 0.30:
            continue

        positions = np.arange(start_index, frame_end + 1, dtype=float)
        closes = frame["close"].iloc[start_index:frame_end + 1].to_numpy(dtype=float)
        resistance_values = common_slope * positions + upper_intercept
        support_values = common_slope * positions + lower_intercept
        buffer = local_atr * 0.15
        violations = (closes > resistance_values + buffer) | (closes < support_values - buffer)
        if int(np.sum(violations)) > max(1, int(formation_bars * 0.03)):
            continue

        recent_positions = np.arange(last_pivot_index + 1, frame_end + 1, dtype=float)
        if len(recent_positions):
            recent_closes = frame["close"].iloc[last_pivot_index + 1:frame_end + 1].to_numpy(dtype=float)
            recent_resistance = common_slope * recent_positions + upper_intercept
            recent_support = common_slope * recent_positions + lower_intercept
            if np.any(
                (recent_closes > recent_resistance + buffer)
                | (recent_closes < recent_support - buffer)
            ):
                continue

        resistance_start = common_slope * start_index + upper_intercept
        support_start = common_slope * start_index + lower_intercept
        resistance_end = common_slope * frame_end + upper_intercept
        support_end = common_slope * frame_end + lower_intercept
        latest_close = float(frame["close"].iloc[-1])
        if latest_close > resistance_end + 0.25 * local_atr or latest_close < support_end - 0.25 * local_atr:
            continue

        support_distance_atr = abs(latest_close - support_end) / local_atr
        resistance_distance_atr = abs(resistance_end - latest_close) / local_atr
        if support_distance_atr <= 0.50 and support_distance_atr <= resistance_distance_atr:
            position = "NEAR_SUPPORT"
        elif resistance_distance_atr <= 0.50:
            position = "NEAR_RESISTANCE"
        else:
            position = "MID_CHANNEL"

        channel_type = "RISING_CHANNEL" if average_slope_atr > 0 else "FALLING_CHANNEL"
        touches = len(highs) + len(lows)
        grade = (
            "STRONG_GEOMETRY"
            if len(highs) >= 3 and len(lows) >= 3 and fit_error_atr <= 0.20 and slope_gap <= 0.015
            else "VALID_GEOMETRY"
        )
        candidates.append({
            "type": channel_type,
            "name": "Rising channel" if channel_type == "RISING_CHANNEL" else "Falling channel",
            "bias": "BULLISH" if channel_type == "RISING_CHANNEL" else "BEARISH",
            "grade": grade,
            "position": position,
            "start_time": _timestamp(frame.index[start_index]),
            "end_time": _timestamp(frame.index[frame_end]),
            "formation_bars": formation_bars,
            "upper_touches": len(highs),
            "lower_touches": len(lows),
            "support_price": round(float(support_end), 2),
            "resistance_price": round(float(resistance_end), 2),
            "support_distance_atr": round(float(support_distance_atr), 2),
            "resistance_distance_atr": round(float(resistance_distance_atr), 2),
            "support_distance_pct": round(float(abs(latest_close - support_end) / latest_close * 100), 2) if latest_close > 0 else None,
            "resistance_distance_pct": round(float(abs(resistance_end - latest_close) / latest_close * 100), 2) if latest_close > 0 else None,
            "width_atr": round(float(width_atr), 2),
            "slope_atr_per_bar": round(float(average_slope_atr), 3),
            "fit_error_atr": round(float(fit_error_atr), 2),
            "lines": [
                {
                    "role": "resistance",
                    "points": [
                        _point(frame, start_index, resistance_start),
                        _point(frame, frame_end, resistance_end),
                    ],
                },
                {
                    "role": "support",
                    "points": [
                        _point(frame, start_index, support_start),
                        _point(frame, frame_end, support_end),
                    ],
                },
            ],
            "_score": (
                0 if grade == "STRONG_GEOMETRY" else 1,
                -touches,
                fit_error_atr,
                -formation_bars,
            ),
        })
    return candidates


def detect_price_channel(frame: pd.DataFrame) -> dict | None:
    """Return one active directional channel from completed bars, when geometry is robust."""
    required = {"open", "high", "low", "close", "volume"}
    if frame is None or len(frame) < 31 or not required.issubset(frame.columns):
        return None
    completed = frame.iloc[:-1].tail(300).copy()
    for column in required:
        completed[column] = pd.to_numeric(completed[column], errors="coerce")
    completed = completed.replace([np.inf, -np.inf], np.nan).dropna(subset=list(required))
    completed = completed[
        (completed["open"] > 0)
        & (completed["high"] > 0)
        & (completed["low"] > 0)
        & (completed["close"] > 0)
        & (completed["volume"] >= 0)
        & (completed["high"] >= completed[["open", "close"]].max(axis=1))
        & (completed["low"] <= completed[["open", "close"]].min(axis=1))
    ]
    if len(completed) < 30:
        return None

    previous_close = completed["close"].shift(1)
    true_range = pd.concat([
        completed["high"] - completed["low"],
        (completed["high"] - previous_close).abs(),
        (completed["low"] - previous_close).abs(),
    ], axis=1).max(axis=1)
    atr = true_range.ewm(alpha=1 / 14, adjust=False).mean()
    pivots = _confirmed_pivots(completed, atr, min_move_atr=0.5)
    if len(pivots) < 5:
        return None
    candidates = _channel_candidates(completed, atr, pivots)
    if not candidates:
        return None
    candidates.sort(key=lambda candidate: candidate["_score"])
    selected = dict(candidates[0])
    selected.pop("_score", None)
    return selected