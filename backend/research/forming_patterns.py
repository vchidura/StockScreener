from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from research.price_structures import _confirmed_pivots


PATTERN_NAMES = {
    "ASCENDING_TRIANGLE": "Ascending triangle",
    "DESCENDING_TRIANGLE": "Descending triangle",
    "SYMMETRICAL_TRIANGLE": "Symmetrical triangle",
    "RISING_WEDGE": "Rising wedge",
    "FALLING_WEDGE": "Falling wedge",
    "BULL_PENNANT": "Bull pennant",
    "BEAR_PENNANT": "Bear pennant",
    "BULL_FLAG": "Bull flag",
    "BEAR_FLAG": "Bear flag",
    "CUP_AND_HANDLE": "Cup and handle",
    "HEAD_AND_SHOULDERS": "Head and shoulders",
    "INVERSE_HEAD_AND_SHOULDERS": "Inverse head and shoulders",
    "TRIPLE_TOP": "Triple top",
    "TRIPLE_BOTTOM": "Triple bottom",
}

PATTERN_INTERVAL_ORDER = ("1wk", "1d", "1h", "30m", "15m", "5m")
PATTERN_INTERVAL_TIER = {
    "1wk": "CONTEXT", "1d": "CONTEXT",
    "1h": "SETUP", "30m": "SETUP",
    "15m": "TRIGGER", "5m": "TRIGGER",
}


def _timestamp(value: Any) -> int:
    return int(pd.Timestamp(value).timestamp())


def _point(frame: pd.DataFrame, index: int, price: float) -> dict:
    return {"time": _timestamp(frame.index[index]), "price": round(float(price), 4)}


def _line(frame: pd.DataFrame, role: str, points: list[tuple[int, float]]) -> dict:
    return {
        "role": role,
        "points": [_point(frame, index, price) for index, price in points],
    }


def _fit_line(pivots: list[dict], atr_value: float) -> dict | None:
    if len(pivots) < 2 or not math.isfinite(atr_value) or atr_value <= 0:
        return None
    x_values = np.asarray([pivot["index"] for pivot in pivots], dtype=float)
    prices = np.asarray([pivot["price"] for pivot in pivots], dtype=float)
    if len(np.unique(x_values)) < 2 or not np.isfinite(prices).all():
        return None
    slope, intercept = np.polyfit(x_values, prices, 1)
    residuals = prices - (slope * x_values + intercept)
    return {
        "slope": float(slope),
        "intercept": float(intercept),
        "slope_atr_per_bar": float(slope / atr_value),
        "mean_error_atr": float(np.mean(np.abs(residuals)) / atr_value),
    }


def _classify_convergence(upper_slope: float, lower_slope: float) -> str | None:
    flat = 0.015
    directional = 0.02
    if upper_slope >= directional and lower_slope > upper_slope + 0.01:
        return "RISING_WEDGE"
    if lower_slope <= -directional and upper_slope < lower_slope - 0.01:
        return "FALLING_WEDGE"
    if abs(upper_slope) <= flat and lower_slope >= directional:
        return "ASCENDING_TRIANGLE"
    if upper_slope <= -directional and abs(lower_slope) <= flat:
        return "DESCENDING_TRIANGLE"
    if upper_slope <= -directional and lower_slope >= directional:
        return "SYMMETRICAL_TRIANGLE"
    return None


def _grade(mean_error_atr: float, contraction_pct: float, touches: int) -> str:
    strong = mean_error_atr <= 0.20 and contraction_pct >= 35 and touches >= 5
    return "STRONG_GEOMETRY" if strong else "VALID_GEOMETRY"


def _edge_metrics(close: float, boundary: float, atr_value: float, role: str) -> dict:
    distance = abs(boundary - close)
    distance_atr = distance / atr_value
    readiness = (
        "AT_EDGE" if distance_atr <= 0.25 else
        "NEAR_EDGE" if distance_atr <= 0.75 else
        "FORMING"
    )
    return {
        "readiness": readiness,
        "boundary_role": role,
        "boundary_price": round(float(boundary), 2),
        "edge_distance_atr": round(float(distance_atr), 2),
        "edge_distance_pct": round(float(distance / close * 100), 2) if close > 0 else None,
    }


def summarize_cross_frame_patterns(rows: list[dict]) -> dict:
    """Group candidates by interval without treating duplicate labels as extra votes."""
    readiness_rank = {"AT_EDGE": 0, "NEAR_EDGE": 1, "FORMING": 2}
    grade_rank = {"STRONG_GEOMETRY": 0, "VALID_GEOMETRY": 1}
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        interval = row.get("interval")
        pattern = row.get("pattern")
        if interval not in PATTERN_INTERVAL_ORDER or not isinstance(pattern, dict):
            continue
        grouped.setdefault(interval, []).append(pattern)

    frames = []
    for interval in PATTERN_INTERVAL_ORDER:
        patterns = grouped.get(interval, [])
        if not patterns:
            continue
        patterns.sort(key=lambda pattern: (
            readiness_rank.get(pattern.get("readiness"), 3),
            grade_rank.get(pattern.get("grade"), 2),
            float(pattern.get("edge_distance_atr", math.inf)),
            -int(pattern.get("upper_touches", 0) + pattern.get("lower_touches", 0)),
        ))
        directional = {
            pattern.get("bias") for pattern in patterns
            if pattern.get("bias") in ("BULLISH", "BEARISH")
        }
        primary_bias = patterns[0].get("bias")
        frame_bias = (
            "MIXED" if len(directional) > 1 else
            primary_bias if primary_bias in directional else
            "NEUTRAL"
        )
        frames.append({
            "interval": interval,
            "tier": PATTERN_INTERVAL_TIER[interval],
            "bias": frame_bias,
            "primary_pattern_type": patterns[0].get("type"),
            "primary_pattern_name": patterns[0].get("name"),
            "readiness": patterns[0].get("readiness"),
            "pattern_types": [pattern.get("type") for pattern in patterns],
            "pattern_count": len(patterns),
        })

    directional_frames = [
        frame for frame in frames if frame["bias"] in ("BULLISH", "BEARISH")
    ]
    if any(frame["bias"] == "MIXED" for frame in frames):
        state = "MIXED"
        dominant_bias = None
    elif not directional_frames:
        state = "NEUTRAL"
        dominant_bias = None
    elif len(directional_frames) == 1:
        state = "SINGLE_FRAME"
        dominant_bias = directional_frames[0]["bias"]
    else:
        biases = {frame["bias"] for frame in directional_frames}
        if len(biases) == 1:
            dominant_bias = directional_frames[0]["bias"]
            state = f"ALIGNED_{dominant_bias}"
        else:
            highest = directional_frames[0]
            lower = directional_frames[1:]
            same_tier_opposition = any(
                frame["tier"] == highest["tier"] and frame["bias"] != highest["bias"]
                for frame in lower
            )
            lower_biases = {frame["bias"] for frame in lower}
            if not same_tier_opposition and lower_biases == {
                "BEARISH" if highest["bias"] == "BULLISH" else "BULLISH"
            }:
                state = "COUNTERTREND"
                dominant_bias = highest["bias"]
            else:
                state = "MIXED"
                dominant_bias = None

    return {
        "state": state,
        "dominant_bias": dominant_bias,
        "directional_frames": len(directional_frames),
        "frames": frames,
    }


def _forming_convergences(
    frame: pd.DataFrame,
    atr: pd.Series,
    pivots: list[dict],
) -> list[dict]:
    candidates: list[dict] = []
    frame_end = len(frame) - 1
    start_floor = max(0, len(pivots) - 12)
    for end_offset in range(max(4, len(pivots) - 1), len(pivots) + 1):
        for start_offset in range(start_floor, max(start_floor, end_offset - 3)):
            points = pivots[start_offset:end_offset]
            highs = [pivot for pivot in points if pivot["type"] == "high"]
            lows = [pivot for pivot in points if pivot["type"] == "low"]
            if len(highs) < 2 or len(lows) < 2:
                continue
            start_index = min(pivot["index"] for pivot in points)
            last_pivot_index = max(pivot["index"] for pivot in points)
            formation_bars = frame_end - start_index + 1
            if formation_bars < 6 or formation_bars > 80 or frame_end - last_pivot_index > 15:
                continue
            local_atr = float(atr.iloc[last_pivot_index])
            upper = _fit_line(highs, local_atr)
            lower = _fit_line(lows, local_atr)
            if upper is None or lower is None:
                continue
            pattern_type = _classify_convergence(
                upper["slope_atr_per_bar"], lower["slope_atr_per_bar"]
            )
            if pattern_type is None:
                continue
            fit_error = max(upper["mean_error_atr"], lower["mean_error_atr"])
            if fit_error > 0.45:
                continue

            upper_start = upper["slope"] * start_index + upper["intercept"]
            lower_start = lower["slope"] * start_index + lower["intercept"]
            upper_end = upper["slope"] * frame_end + upper["intercept"]
            lower_end = lower["slope"] * frame_end + lower["intercept"]
            initial_width = upper_start - lower_start
            current_width = upper_end - lower_end
            if initial_width <= 0 or current_width <= local_atr * 0.35:
                continue
            contraction_pct = (1 - current_width / initial_width) * 100
            if contraction_pct < 20 or contraction_pct >= 90:
                continue

            denominator = upper["slope"] - lower["slope"]
            if abs(denominator) < 1e-9:
                continue
            apex_index = (lower["intercept"] - upper["intercept"]) / denominator
            apex_bars_ahead = apex_index - frame_end
            if apex_bars_ahead < 1 or apex_bars_ahead > max(60, formation_bars):
                continue

            positions = np.arange(start_index, frame_end + 1, dtype=float)
            closes = frame["close"].iloc[start_index:frame_end + 1].to_numpy(dtype=float)
            upper_values = upper["slope"] * positions + upper["intercept"]
            lower_values = lower["slope"] * positions + lower["intercept"]
            buffer = local_atr * 0.15
            violations = int(np.sum((closes > upper_values + buffer) | (closes < lower_values - buffer)))
            if violations > 1:
                continue
            recent_positions = np.arange(last_pivot_index + 1, frame_end + 1, dtype=float)
            if len(recent_positions):
                recent_closes = frame["close"].iloc[last_pivot_index + 1:frame_end + 1].to_numpy(dtype=float)
                recent_upper = upper["slope"] * recent_positions + upper["intercept"]
                recent_lower = lower["slope"] * recent_positions + lower["intercept"]
                if np.any((recent_closes > recent_upper + buffer) | (recent_closes < recent_lower - buffer)):
                    continue

            flagpole_atr = None
            bias = "NEUTRAL"
            consolidation_high = float(frame["high"].iloc[start_index:frame_end + 1].max())
            consolidation_low = float(frame["low"].iloc[start_index:frame_end + 1].min())
            if formation_bars <= 20 and start_index >= 4:
                best_move = 0.0
                for lookback in range(3, min(10, start_index) + 1):
                    move = float(frame["close"].iloc[start_index] - frame["close"].iloc[start_index - lookback])
                    if abs(move) > abs(best_move):
                        best_move = move
                measured_flagpole_atr = abs(best_move) / local_atr
                consolidation_height = consolidation_high - consolidation_low
                if measured_flagpole_atr >= 3 and consolidation_height <= abs(best_move) * 0.6:
                    pattern_type = "BULL_PENNANT" if best_move > 0 else "BEAR_PENNANT"
                    bias = "BULLISH" if best_move > 0 else "BEARISH"
                    flagpole_atr = measured_flagpole_atr
            if pattern_type == "ASCENDING_TRIANGLE":
                bias = "BULLISH"
            elif pattern_type == "DESCENDING_TRIANGLE":
                bias = "BEARISH"
            elif pattern_type == "RISING_WEDGE":
                bias = "BEARISH"
            elif pattern_type == "FALLING_WEDGE":
                bias = "BULLISH"

            latest_close = float(frame["close"].iloc[-1])
            if bias == "BULLISH":
                decision_boundary = upper_end
                boundary_role = "resistance"
            elif bias == "BEARISH":
                decision_boundary = lower_end
                boundary_role = "support"
            elif abs(latest_close - upper_end) <= abs(latest_close - lower_end):
                decision_boundary = upper_end
                boundary_role = "resistance"
            else:
                decision_boundary = lower_end
                boundary_role = "support"

            candidates.append({
                "type": pattern_type,
                "name": PATTERN_NAMES[pattern_type],
                "status": "FORMING",
                "bias": bias,
                "grade": _grade(fit_error, contraction_pct, len(points)),
                "start_time": _timestamp(frame.index[start_index]),
                "end_time": _timestamp(frame.index[frame_end]),
                "formation_bars": formation_bars,
                "upper_touches": len(highs),
                "lower_touches": len(lows),
                "contraction_pct": round(float(contraction_pct), 1),
                "apex_bars_ahead": round(float(apex_bars_ahead), 1),
                "fit_error_atr": round(float(fit_error), 2),
                "flagpole_atr": round(float(flagpole_atr), 1) if flagpole_atr is not None else None,
                "invalidation_price": (
                    round(float(lower_end - 0.25 * local_atr), 2)
                    if bias == "BULLISH" else
                    round(float(upper_end + 0.25 * local_atr), 2)
                    if bias == "BEARISH" else None
                ),
                **_edge_metrics(latest_close, decision_boundary, local_atr, boundary_role),
                "lines": [
                    _line(frame, "resistance", [(start_index, upper_start), (frame_end, upper_end)]),
                    _line(frame, "support", [(start_index, lower_start), (frame_end, lower_end)]),
                ],
            })
    return candidates


def _forming_head_shoulders(frame: pd.DataFrame, atr: pd.Series, pivots: list[dict]) -> list[dict]:
    candidates: list[dict] = []
    frame_end = len(frame) - 1
    for offset in range(max(0, len(pivots) - 14), len(pivots) - 4):
        points = pivots[offset:offset + 5]
        types = [point["type"] for point in points]
        bearish = types == ["high", "low", "high", "low", "high"]
        bullish = types == ["low", "high", "low", "high", "low"]
        if not (bearish or bullish):
            continue
        left_shoulder, left_neck, head, right_neck, right_shoulder = points
        if frame_end - right_shoulder["index"] > 15:
            continue
        local_atr = float(atr.iloc[right_shoulder["index"]])
        shoulder_average = (left_shoulder["price"] + right_shoulder["price"]) / 2
        shoulder_tolerance = max(abs(shoulder_average) * 0.02, local_atr)
        neck_average = (left_neck["price"] + right_neck["price"]) / 2
        neck_tolerance = max(abs(neck_average) * 0.025, local_atr)
        head_clearance = max(abs(shoulder_average) * 0.015, local_atr * 0.75)
        if (
            abs(left_shoulder["price"] - right_shoulder["price"]) > shoulder_tolerance
            or abs(left_neck["price"] - right_neck["price"]) > neck_tolerance
        ):
            continue
        if bearish and head["price"] - max(left_shoulder["price"], right_shoulder["price"]) < head_clearance:
            continue
        if bullish and min(left_shoulder["price"], right_shoulder["price"]) - head["price"] < head_clearance:
            continue
        neck_slope = (right_neck["price"] - left_neck["price"]) / (right_neck["index"] - left_neck["index"])
        neck_end = left_neck["price"] + neck_slope * (frame_end - left_neck["index"])
        later_positions = np.arange(right_shoulder["index"] + 1, frame_end + 1, dtype=float)
        later_closes = frame["close"].iloc[right_shoulder["index"] + 1:frame_end + 1].to_numpy(dtype=float)
        later_neckline = left_neck["price"] + neck_slope * (later_positions - left_neck["index"])
        broke_neckline = (
            np.any(later_closes < later_neckline - 0.1 * local_atr)
            if bearish else
            np.any(later_closes > later_neckline + 0.1 * local_atr)
        )
        if broke_neckline:
            continue
        pattern_type = "HEAD_AND_SHOULDERS" if bearish else "INVERSE_HEAD_AND_SHOULDERS"
        candidates.append({
            "type": pattern_type,
            "name": PATTERN_NAMES[pattern_type],
            "status": "FORMING",
            "bias": "BEARISH" if bearish else "BULLISH",
            "grade": "STRONG_GEOMETRY" if abs(left_shoulder["price"] - right_shoulder["price"]) <= shoulder_tolerance * 0.5 else "VALID_GEOMETRY",
            "start_time": _timestamp(frame.index[left_shoulder["index"]]),
            "end_time": _timestamp(frame.index[frame_end]),
            "formation_bars": frame_end - left_shoulder["index"] + 1,
            "upper_touches": 3 if bearish else 2,
            "lower_touches": 2 if bearish else 3,
            "contraction_pct": None,
            "apex_bars_ahead": None,
            "fit_error_atr": None,
            "flagpole_atr": None,
            "invalidation_price": round(float(
                right_shoulder["price"] + 0.25 * local_atr
                if bearish else right_shoulder["price"] - 0.25 * local_atr
            ), 2),
            **_edge_metrics(
                float(frame["close"].iloc[-1]),
                neck_end,
                local_atr,
                "support" if bearish else "resistance",
            ),
            "lines": [
                _line(frame, "neckline", [
                    (left_neck["index"], left_neck["price"]),
                    (frame_end, neck_end),
                ]),
                _line(frame, "structure", [
                    (left_shoulder["index"], left_shoulder["price"]),
                    (head["index"], head["price"]),
                    (right_shoulder["index"], right_shoulder["price"]),
                ]),
            ],
        })
    return candidates


def _forming_flags(frame: pd.DataFrame, atr: pd.Series, pivots: list[dict]) -> list[dict]:
    """Detect short parallel pullback channels after a directional impulse."""
    candidates: list[dict] = []
    frame_end = len(frame) - 1
    start_floor = max(0, len(pivots) - 12)
    for end_offset in range(max(4, len(pivots) - 1), len(pivots) + 1):
        for start_offset in range(start_floor, max(start_floor, end_offset - 3)):
            points = pivots[start_offset:end_offset]
            highs = [pivot for pivot in points if pivot["type"] == "high"]
            lows = [pivot for pivot in points if pivot["type"] == "low"]
            if len(highs) < 2 or len(lows) < 2:
                continue
            start_index = min(pivot["index"] for pivot in points)
            last_pivot_index = max(pivot["index"] for pivot in points)
            formation_bars = frame_end - start_index + 1
            if formation_bars < 6 or formation_bars > 20 or start_index < 4 or frame_end - last_pivot_index > 10:
                continue
            local_atr = float(atr.iloc[last_pivot_index])
            upper = _fit_line(highs, local_atr)
            lower = _fit_line(lows, local_atr)
            if upper is None or lower is None:
                continue
            fit_error = max(upper["mean_error_atr"], lower["mean_error_atr"])
            if fit_error > 0.35 or abs(upper["slope_atr_per_bar"] - lower["slope_atr_per_bar"]) > 0.04:
                continue

            impulse_start = None
            impulse_move = 0.0
            for lookback in range(3, min(10, start_index) + 1):
                move = float(frame["close"].iloc[start_index] - frame["close"].iloc[start_index - lookback])
                if abs(move) > abs(impulse_move):
                    impulse_move = move
                    impulse_start = start_index - lookback
            flagpole_atr = abs(impulse_move) / local_atr
            upper_slope = upper["slope_atr_per_bar"]
            lower_slope = lower["slope_atr_per_bar"]
            bull_flag = impulse_move > 0 and upper_slope <= -0.02 and lower_slope <= -0.02
            bear_flag = impulse_move < 0 and upper_slope >= 0.02 and lower_slope >= 0.02
            if flagpole_atr < 3 or not (bull_flag or bear_flag) or impulse_start is None:
                continue

            upper_start = upper["slope"] * start_index + upper["intercept"]
            lower_start = lower["slope"] * start_index + lower["intercept"]
            upper_end = upper["slope"] * frame_end + upper["intercept"]
            lower_end = lower["slope"] * frame_end + lower["intercept"]
            initial_width = upper_start - lower_start
            current_width = upper_end - lower_end
            consolidation_height = float(
                frame["high"].iloc[start_index:frame_end + 1].max()
                - frame["low"].iloc[start_index:frame_end + 1].min()
            )
            if (
                initial_width <= 0 or current_width <= local_atr * 0.35
                or abs(current_width / initial_width - 1) > 0.35
                or consolidation_height > abs(impulse_move) * 0.6
            ):
                continue

            positions = np.arange(start_index, frame_end + 1, dtype=float)
            closes = frame["close"].iloc[start_index:frame_end + 1].to_numpy(dtype=float)
            upper_values = upper["slope"] * positions + upper["intercept"]
            lower_values = lower["slope"] * positions + lower["intercept"]
            buffer = local_atr * 0.15
            if np.sum((closes > upper_values + buffer) | (closes < lower_values - buffer)) > 1:
                continue
            recent_positions = np.arange(last_pivot_index + 1, frame_end + 1, dtype=float)
            if len(recent_positions):
                recent_closes = frame["close"].iloc[last_pivot_index + 1:frame_end + 1].to_numpy(dtype=float)
                recent_upper = upper["slope"] * recent_positions + upper["intercept"]
                recent_lower = lower["slope"] * recent_positions + lower["intercept"]
                if np.any((recent_closes > recent_upper + buffer) | (recent_closes < recent_lower - buffer)):
                    continue
            latest_close = float(frame["close"].iloc[-1])
            pattern_type = "BULL_FLAG" if bull_flag else "BEAR_FLAG"
            bias = "BULLISH" if bull_flag else "BEARISH"
            decision_boundary = upper_end if bull_flag else lower_end
            boundary_role = "resistance" if bull_flag else "support"
            candidates.append({
                "type": pattern_type,
                "name": PATTERN_NAMES[pattern_type],
                "status": "FORMING",
                "bias": bias,
                "grade": "STRONG_GEOMETRY" if fit_error <= 0.20 and len(points) >= 5 else "VALID_GEOMETRY",
                "start_time": _timestamp(frame.index[start_index]),
                "end_time": _timestamp(frame.index[frame_end]),
                "formation_bars": formation_bars,
                "upper_touches": len(highs),
                "lower_touches": len(lows),
                "contraction_pct": None,
                "apex_bars_ahead": None,
                "fit_error_atr": round(float(fit_error), 2),
                "flagpole_atr": round(float(flagpole_atr), 1),
                "invalidation_price": round(float(
                    lower_end - 0.25 * local_atr if bull_flag else upper_end + 0.25 * local_atr
                ), 2),
                **_edge_metrics(latest_close, decision_boundary, local_atr, boundary_role),
                "lines": [
                    _line(frame, "flagpole", [
                        (impulse_start, float(frame["close"].iloc[impulse_start])),
                        (start_index, float(frame["close"].iloc[start_index])),
                    ]),
                    _line(frame, "resistance", [(start_index, upper_start), (frame_end, upper_end)]),
                    _line(frame, "support", [(start_index, lower_start), (frame_end, lower_end)]),
                ],
            })
    return candidates


def _forming_triples(frame: pd.DataFrame, atr: pd.Series, pivots: list[dict]) -> list[dict]:
    """Detect three comparable tests of support or resistance before neckline break."""
    candidates: list[dict] = []
    frame_end = len(frame) - 1
    for offset in range(max(0, len(pivots) - 14), len(pivots) - 4):
        points = pivots[offset:offset + 5]
        types = [point["type"] for point in points]
        triple_top = types == ["high", "low", "high", "low", "high"]
        triple_bottom = types == ["low", "high", "low", "high", "low"]
        if not (triple_top or triple_bottom):
            continue
        outer = [points[0], points[2], points[4]]
        necks = [points[1], points[3]]
        if frame_end - outer[-1]["index"] > 15:
            continue
        local_atr = float(atr.iloc[outer[-1]["index"]])
        outer_average = sum(point["price"] for point in outer) / 3
        outer_tolerance = max(abs(outer_average) * 0.015, local_atr * 0.75)
        neck_average = sum(point["price"] for point in necks) / 2
        neck_tolerance = max(abs(neck_average) * 0.025, local_atr)
        if (
            max(point["price"] for point in outer) - min(point["price"] for point in outer) > outer_tolerance
            or abs(necks[0]["price"] - necks[1]["price"]) > neck_tolerance
            or abs(outer_average - neck_average) < max(abs(outer_average) * 0.025, local_atr * 1.5)
        ):
            continue
        neck_slope = (necks[1]["price"] - necks[0]["price"]) / (necks[1]["index"] - necks[0]["index"])
        if abs(neck_slope / local_atr) > 0.10:
            continue
        neck_end = necks[0]["price"] + neck_slope * (frame_end - necks[0]["index"])
        later_positions = np.arange(outer[-1]["index"] + 1, frame_end + 1, dtype=float)
        later_closes = frame["close"].iloc[outer[-1]["index"] + 1:frame_end + 1].to_numpy(dtype=float)
        later_neckline = necks[0]["price"] + neck_slope * (later_positions - necks[0]["index"])
        expected_break = (
            np.any(later_closes < later_neckline - 0.1 * local_atr)
            if triple_top else
            np.any(later_closes > later_neckline + 0.1 * local_atr)
        )
        invalidation = (
            max(point["price"] for point in outer) + 0.25 * local_atr
            if triple_top else
            min(point["price"] for point in outer) - 0.25 * local_atr
        )
        opposite_break = (
            np.any(later_closes > invalidation) if triple_top else np.any(later_closes < invalidation)
        )
        if expected_break or opposite_break:
            continue
        pattern_type = "TRIPLE_TOP" if triple_top else "TRIPLE_BOTTOM"
        bias = "BEARISH" if triple_top else "BULLISH"
        boundary_role = "support" if triple_top else "resistance"
        latest_close = float(frame["close"].iloc[-1])
        candidates.append({
            "type": pattern_type,
            "name": PATTERN_NAMES[pattern_type],
            "status": "FORMING",
            "bias": bias,
            "grade": "STRONG_GEOMETRY" if max(point["price"] for point in outer) - min(point["price"] for point in outer) <= outer_tolerance * 0.5 else "VALID_GEOMETRY",
            "start_time": _timestamp(frame.index[outer[0]["index"]]),
            "end_time": _timestamp(frame.index[frame_end]),
            "formation_bars": frame_end - outer[0]["index"] + 1,
            "upper_touches": 3 if triple_top else 2,
            "lower_touches": 2 if triple_top else 3,
            "contraction_pct": None,
            "apex_bars_ahead": None,
            "fit_error_atr": None,
            "flagpole_atr": None,
            "invalidation_price": round(float(invalidation), 2),
            **_edge_metrics(latest_close, neck_end, local_atr, boundary_role),
            "lines": [
                _line(frame, "resistance" if triple_top else "support", [
                    (point["index"], point["price"]) for point in outer
                ]),
                _line(frame, boundary_role, [
                    (necks[0]["index"], necks[0]["price"]),
                    (frame_end, neck_end),
                ]),
            ],
        })
    return candidates


def _forming_cup_handle(frame: pd.DataFrame, atr: pd.Series, pivots: list[dict]) -> list[dict]:
    candidates: list[dict] = []
    frame_end = len(frame) - 1
    for offset in range(max(0, len(pivots) - 12), len(pivots) - 3):
        left_rim, bottom, right_rim, handle_low = pivots[offset:offset + 4]
        if [point["type"] for point in (left_rim, bottom, right_rim, handle_low)] != ["high", "low", "high", "low"]:
            continue
        cup_bars = right_rim["index"] - left_rim["index"]
        handle_bars = frame_end - right_rim["index"]
        if cup_bars < 20 or cup_bars > 150 or handle_bars < 2 or handle_bars > 20:
            continue
        local_atr = float(atr.iloc[handle_low["index"]])
        rim = (left_rim["price"] + right_rim["price"]) / 2
        depth = min(left_rim["price"], right_rim["price"]) - bottom["price"]
        handle_depth = right_rim["price"] - handle_low["price"]
        down_bars = bottom["index"] - left_rim["index"]
        up_bars = right_rim["index"] - bottom["index"]
        symmetry = down_bars / up_bars if up_bars > 0 else math.inf
        if (
            abs(left_rim["price"] - right_rim["price"]) > max(rim * 0.05, local_atr)
            or depth < max(rim * 0.08, local_atr * 4)
            or depth > rim * 0.45
            or not 0.4 <= symmetry <= 2.5
            or handle_depth < local_atr * 0.5
            or handle_depth > depth * 0.5
        ):
            continue
        latest_close = float(frame["close"].iloc[-1])
        handle_closes = frame["close"].iloc[handle_low["index"] + 1:frame_end + 1].to_numpy(dtype=float)
        if np.any(handle_closes > rim + 0.1 * local_atr) or np.any(
            handle_closes < handle_low["price"] - 0.1 * local_atr
        ):
            continue
        candidates.append({
            "type": "CUP_AND_HANDLE",
            "name": PATTERN_NAMES["CUP_AND_HANDLE"],
            "status": "FORMING",
            "bias": "BULLISH",
            "grade": "STRONG_GEOMETRY" if abs(left_rim["price"] - right_rim["price"]) <= max(rim * 0.025, local_atr * 0.5) else "VALID_GEOMETRY",
            "start_time": _timestamp(frame.index[left_rim["index"]]),
            "end_time": _timestamp(frame.index[frame_end]),
            "formation_bars": frame_end - left_rim["index"] + 1,
            "upper_touches": 2,
            "lower_touches": 2,
            "contraction_pct": round(float((1 - handle_depth / depth) * 100), 1),
            "apex_bars_ahead": None,
            "fit_error_atr": None,
            "flagpole_atr": None,
            "invalidation_price": round(float(handle_low["price"] - 0.25 * local_atr), 2),
            **_edge_metrics(latest_close, rim, local_atr, "resistance"),
            "lines": [
                _line(frame, "rim", [(left_rim["index"], rim), (frame_end, rim)]),
                _line(frame, "cup", [
                    (left_rim["index"], left_rim["price"]),
                    (bottom["index"], bottom["price"]),
                    (right_rim["index"], right_rim["price"]),
                ]),
                _line(frame, "handle", [
                    (right_rim["index"], right_rim["price"]),
                    (handle_low["index"], handle_low["price"]),
                    (frame_end, latest_close),
                ]),
            ],
        })
    return candidates


def detect_forming_patterns(
    frame: pd.DataFrame,
    max_patterns: int = 3,
    *,
    input_includes_forming_bar: bool = True,
) -> list[dict]:
    """Return the best current chart-only formations from completed bars.

    Request-time legacy frames include a newest bar that may still be forming. A
    finalized-bar worker sets ``input_includes_forming_bar=False`` so the newest
    sealed bar remains eligible.
    """
    required = {"open", "high", "low", "close", "volume"}
    if frame is None or len(frame) < 30 or not required.issubset(frame.columns):
        return []
    completed = (
        frame.iloc[:-1] if input_includes_forming_bar else frame
    ).tail(300).copy()
    for column in required:
        completed[column] = pd.to_numeric(completed[column], errors="coerce")
    completed = completed.dropna(subset=list(required))
    if len(completed) < 29:
        return []
    previous_close = completed["close"].shift(1)
    true_range = pd.concat([
        completed["high"] - completed["low"],
        (completed["high"] - previous_close).abs(),
        (completed["low"] - previous_close).abs(),
    ], axis=1).max(axis=1)
    atr = true_range.ewm(alpha=1 / 14, adjust=False).mean()
    pivots = _confirmed_pivots(completed, atr, min_move_atr=0.5)
    if len(pivots) < 4:
        return []
    candidates = [
        *_forming_convergences(completed, atr, pivots),
        *_forming_flags(completed, atr, pivots),
        *_forming_head_shoulders(completed, atr, pivots),
        *_forming_triples(completed, atr, pivots),
        *_forming_cup_handle(completed, atr, pivots),
    ]
    readiness_rank = {"AT_EDGE": 0, "NEAR_EDGE": 1, "FORMING": 2}
    grade_rank = {"STRONG_GEOMETRY": 0, "VALID_GEOMETRY": 1}
    candidates.sort(key=lambda item: (
        readiness_rank[item["readiness"]],
        grade_rank[item["grade"]],
        item["edge_distance_atr"],
        -int(item["upper_touches"] + item["lower_touches"]),
        int(item["formation_bars"]),
    ))
    selected: list[dict] = []
    seen_types: set[str] = set()
    for candidate in candidates:
        if candidate["type"] in seen_types:
            continue
        selected.append(candidate)
        seen_types.add(candidate["type"])
        if len(selected) >= max_patterns:
            break
    return selected