from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd


def _timestamp(value: Any) -> str:
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _confirmed_pivots(
    frame: pd.DataFrame,
    atr: pd.Series,
    left: int = 2,
    right: int = 2,
    min_move_atr: float = 0.75,
) -> list[dict]:
    """Return alternating local extrema known only after right-side confirmation."""
    high = frame["high"].to_numpy(dtype=float)
    low = frame["low"].to_numpy(dtype=float)
    raw: list[dict] = []
    for index in range(left, len(frame) - right):
        high_window = high[index - left:index + right + 1]
        low_window = low[index - left:index + right + 1]
        if not (np.isfinite(high_window).all() and np.isfinite(low_window).all()):
            continue
        if high[index] == np.max(high_window) and int(np.argmax(high_window)) == left:
            raw.append({"index": index, "price": high[index], "type": "high"})
        if low[index] == np.min(low_window) and int(np.argmin(low_window)) == left:
            raw.append({"index": index, "price": low[index], "type": "low"})

    raw.sort(key=lambda pivot: pivot["index"])
    pivots: list[dict] = []
    for candidate in raw:
        if pivots and candidate["index"] == pivots[-1]["index"]:
            continue
        if pivots and candidate["type"] == pivots[-1]["type"]:
            more_extreme = (
                candidate["price"] > pivots[-1]["price"]
                if candidate["type"] == "high"
                else candidate["price"] < pivots[-1]["price"]
            )
            if more_extreme:
                pivots[-1] = candidate
            continue
        if pivots:
            local_atr = max(float(atr.iloc[candidate["index"]]), 1e-9)
            minimum_move = max(local_atr * min_move_atr, candidate["price"] * 0.005)
            if abs(candidate["price"] - pivots[-1]["price"]) < minimum_move:
                continue
        pivots.append(candidate)
    return pivots


def _confirmation_index(
    close: np.ndarray,
    start: int,
    trigger: float,
    direction: str,
    buffer: float,
) -> int | None:
    for index in range(start + 1, len(close)):
        if direction == "BEARISH" and close[index] < trigger - buffer:
            return index
        if direction == "BULLISH" and close[index] > trigger + buffer:
            return index
    return None


def _is_active_pattern(
    frame: pd.DataFrame,
    confirmation: int,
    direction: str,
    target: float,
    invalidation: float,
) -> bool:
    if not all(math.isfinite(value) and value > 0 for value in (target, invalidation)):
        return False
    later = frame.iloc[confirmation:]
    if later.empty:
        return True
    if direction == "BEARISH":
        target_hits = np.flatnonzero(later["low"].to_numpy(dtype=float) <= target)
        invalidations = np.flatnonzero(later["close"].to_numpy(dtype=float) >= invalidation)
    else:
        target_hits = np.flatnonzero(later["high"].to_numpy(dtype=float) >= target)
        invalidations = np.flatnonzero(later["close"].to_numpy(dtype=float) <= invalidation)
    first_target = int(target_hits[0]) if len(target_hits) else math.inf
    first_invalidation = int(invalidations[0]) if len(invalidations) else math.inf
    return first_target == math.inf and first_invalidation == math.inf


def _pattern_record(
    frame: pd.DataFrame,
    pivots: list[dict],
    name: str,
    pattern_type: str,
    direction: str,
    neckline: float,
    target: float,
    invalidation: float,
    confirmation: int,
) -> dict:
    return {
        "type": pattern_type,
        "name": name,
        "direction": direction,
        "status": "CONFIRMED",
        "confirmation_time": _timestamp(frame.index[confirmation]),
        "bars_ago": len(frame) - 1 - confirmation,
        "neckline": round(float(neckline), 2),
        "target": round(float(target), 2),
        "invalidation": round(float(invalidation), 2),
        "pivots": [
            {
                "type": pivot["type"],
                "price": round(float(pivot["price"]), 2),
                "time": _timestamp(frame.index[pivot["index"]]),
            }
            for pivot in pivots
        ],
    }


def _detect_structural_patterns(
    frame: pd.DataFrame,
    atr: pd.Series,
    pivots: list[dict],
    max_age_bars: int = 60,
    max_patterns: int = 3,
) -> list[dict]:
    close = frame["close"].to_numpy(dtype=float)
    candidates: list[dict] = []

    for offset in range(max(0, len(pivots) - 18), len(pivots) - 2):
        first, middle, last = pivots[offset:offset + 3]
        local_atr = max(float(atr.iloc[last["index"]]), 1e-9)
        if [first["type"], middle["type"], last["type"]] == ["high", "low", "high"]:
            peaks = (first["price"], last["price"])
            peak = sum(peaks) / 2
            tolerance = max(peak * 0.015, local_atr * 0.75)
            depth = min(peaks) - middle["price"]
            if abs(peaks[0] - peaks[1]) <= tolerance and depth >= max(peak * 0.025, local_atr * 1.5):
                confirmation = _confirmation_index(
                    close, last["index"], middle["price"], "BEARISH", local_atr * 0.1
                )
                target = middle["price"] - depth
                invalidation = max(peaks) + local_atr * 0.25
                if (
                    confirmation is not None
                    and len(frame) - 1 - confirmation <= max_age_bars
                    and all(math.isfinite(value) and value > 0 for value in (target, invalidation))
                    and _is_active_pattern(frame, confirmation, "BEARISH", target, invalidation)
                ):
                    candidates.append(_pattern_record(
                        frame, [first, middle, last], "Double top", "DOUBLE_TOP",
                        "BEARISH", middle["price"], target, invalidation, confirmation,
                    ))
        elif [first["type"], middle["type"], last["type"]] == ["low", "high", "low"]:
            troughs = (first["price"], last["price"])
            trough = sum(troughs) / 2
            tolerance = max(trough * 0.015, local_atr * 0.75)
            height = middle["price"] - max(troughs)
            if abs(troughs[0] - troughs[1]) <= tolerance and height >= max(trough * 0.025, local_atr * 1.5):
                confirmation = _confirmation_index(
                    close, last["index"], middle["price"], "BULLISH", local_atr * 0.1
                )
                target = middle["price"] + height
                invalidation = min(troughs) - local_atr * 0.25
                if (
                    confirmation is not None
                    and len(frame) - 1 - confirmation <= max_age_bars
                    and all(math.isfinite(value) and value > 0 for value in (target, invalidation))
                    and _is_active_pattern(frame, confirmation, "BULLISH", target, invalidation)
                ):
                    candidates.append(_pattern_record(
                        frame, [first, middle, last], "Double bottom", "DOUBLE_BOTTOM",
                        "BULLISH", middle["price"], target, invalidation, confirmation,
                    ))

    for offset in range(max(0, len(pivots) - 20), len(pivots) - 4):
        points = pivots[offset:offset + 5]
        if [point["type"] for point in points] != ["high", "low", "high", "low", "high"]:
            continue
        left_shoulder, left_neck, head, right_neck, right_shoulder = points
        local_atr = max(float(atr.iloc[right_shoulder["index"]]), 1e-9)
        shoulder_average = (left_shoulder["price"] + right_shoulder["price"]) / 2
        shoulder_tolerance = max(shoulder_average * 0.02, local_atr)
        head_clearance = max(shoulder_average * 0.015, local_atr * 0.75)
        neckline = (left_neck["price"] + right_neck["price"]) / 2
        neckline_tolerance = max(neckline * 0.025, local_atr)
        if (
            abs(left_shoulder["price"] - right_shoulder["price"]) > shoulder_tolerance
            or head["price"] - max(left_shoulder["price"], right_shoulder["price"]) < head_clearance
            or abs(left_neck["price"] - right_neck["price"]) > neckline_tolerance
        ):
            continue
        confirmation = _confirmation_index(
            close, right_shoulder["index"], neckline, "BEARISH", local_atr * 0.1
        )
        target = neckline - (head["price"] - neckline)
        invalidation = right_shoulder["price"] + local_atr * 0.25
        if (
            confirmation is not None
            and len(frame) - 1 - confirmation <= max_age_bars
            and all(math.isfinite(value) and value > 0 for value in (target, invalidation))
            and _is_active_pattern(frame, confirmation, "BEARISH", target, invalidation)
        ):
            candidates.append(_pattern_record(
                frame, points, "Head and shoulders", "HEAD_AND_SHOULDERS",
                "BEARISH", neckline, target, invalidation, confirmation,
            ))

    priority = {"HEAD_AND_SHOULDERS": 0, "DOUBLE_TOP": 1, "DOUBLE_BOTTOM": 1}
    candidates.sort(key=lambda item: (item["bars_ago"], priority[item["type"]]))
    selected: list[dict] = []
    used_confirmations: set[tuple[str, str]] = set()
    for candidate in candidates:
        key = (candidate["confirmation_time"], candidate["direction"])
        if key in used_confirmations:
            continue
        selected.append(candidate)
        used_confirmations.add(key)
        if len(selected) >= max_patterns:
            break
    return selected


def _volume_pivot_zones(
    frame: pd.DataFrame,
    atr: pd.Series,
    pivots: list[dict],
    fibonacci_levels: list[dict],
    minimum_volume_ratio: float = 1.25,
    max_age_bars: int = 80,
    max_zones: int = 2,
) -> list[dict]:
    volume = frame["volume"].to_numpy(dtype=float)
    zones: list[dict] = []
    for pivot in reversed(pivots):
        index = pivot["index"]
        bars_ago = len(frame) - 1 - index
        if bars_ago > max_age_bars or index < 20:
            continue
        baseline = float(np.median(volume[index - 20:index]))
        if not math.isfinite(baseline) or baseline <= 0:
            continue
        volume_ratio = float(volume[index] / baseline)
        if not math.isfinite(volume_ratio) or volume_ratio < minimum_volume_ratio:
            continue

        window = frame.iloc[max(0, index - 1):min(len(frame), index + 2)]
        weights = window["volume"].to_numpy(dtype=float)
        typical = (
            window["high"].to_numpy(dtype=float)
            + window["low"].to_numpy(dtype=float)
            + window["close"].to_numpy(dtype=float)
        ) / 3
        weighted_price = float(np.average(typical, weights=weights)) if weights.sum() > 0 else float(typical.mean())
        local_atr = max(float(atr.iloc[index]), 1e-9)
        pivot_row = frame.iloc[index]
        if pivot["type"] == "low":
            low = float(pivot_row["low"])
            high = min(float(pivot_row["high"]), low + local_atr, max(low, weighted_price))
            role = "demand"
        else:
            high = float(pivot_row["high"])
            low = max(float(pivot_row["low"]), high - local_atr, min(high, weighted_price))
            role = "supply"
        if high - low < local_atr * 0.1:
            continue
        later_closes = frame["close"].iloc[index + 1:].to_numpy(dtype=float)
        break_buffer = local_atr * 0.1
        broken = (
            role == "demand" and np.any(later_closes < low - break_buffer)
        ) or (
            role == "supply" and np.any(later_closes > high + break_buffer)
        )
        if broken:
            continue

        overlaps = []
        overlap_names: set[str] = set()
        for level in fibonacci_levels:
            if level.get("price") is None:
                continue
            level_price = float(level["price"])
            if not math.isfinite(level_price) or level_price <= 0:
                continue
            overlap_tolerance = min(local_atr * 0.25, level_price * 0.01)
            if low - overlap_tolerance <= level_price <= high + overlap_tolerance:
                overlap_name = str(level["name"])
                if overlap_name in overlap_names:
                    continue
                overlap_names.add(overlap_name)
                overlaps.append({
                    "name": overlap_name,
                    "price": round(level_price, 2),
                })
        volume_lift_pct = round((volume_ratio - 1) * 100)
        qualifier = (
            f"Pivot volume {volume_ratio:.2f}x its prior 20-bar median "
            f"(+{volume_lift_pct}%)"
        )
        if overlaps:
            qualifier += f"; near Fib {', '.join(level['name'] for level in overlaps[:2])}"
        zones.append({
            "name": f"Volume pivot {role}",
            "low": round(low, 2),
            "high": round(high, 2),
            "source": "Volume Pivot",
            "qualifier": qualifier,
            "pivot_time": _timestamp(frame.index[index]),
            "pivot_type": pivot["type"],
            "volume_ratio": round(volume_ratio, 2),
            "bars_ago": bars_ago,
            "fibonacci_levels": overlaps[:3],
        })
        if len(zones) >= max_zones:
            break
    return zones


def analyze_price_structures(
    frame: pd.DataFrame,
    fibonacci_levels: list[dict] | None = None,
) -> dict:
    """Detect active confirmed structures and elevated-volume pivot zones.

    The newest row is always excluded because it may still be forming. Volume
    zones are an order-flow proxy, not evidence of participant identity.
    """
    empty = {"patterns": [], "volume_pivot_zones": []}
    required = {"open", "high", "low", "close", "volume"}
    if frame is None or len(frame) < 30 or not required.issubset(frame.columns):
        return empty
    completed = frame.iloc[:-1].tail(300).copy()
    if len(completed) < 29:
        return empty
    for column in required:
        completed[column] = pd.to_numeric(completed[column], errors="coerce")
    completed = completed.dropna(subset=list(required))
    if len(completed) < 29:
        return empty

    previous_close = completed["close"].shift(1)
    true_range = pd.concat([
        completed["high"] - completed["low"],
        (completed["high"] - previous_close).abs(),
        (completed["low"] - previous_close).abs(),
    ], axis=1).max(axis=1)
    atr = true_range.ewm(alpha=1 / 14, adjust=False).mean()
    pivots = _confirmed_pivots(completed, atr)
    if len(pivots) < 3:
        return empty
    levels = fibonacci_levels or []
    return {
        "patterns": _detect_structural_patterns(completed, atr, pivots),
        "volume_pivot_zones": _volume_pivot_zones(completed, atr, pivots, levels),
    }