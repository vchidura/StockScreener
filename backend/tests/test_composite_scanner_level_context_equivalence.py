"""Equivalence of the array-based level context against the original scalar-assignment version."""
import numpy as np
import pandas as pd
import pytest

from research import composite_scanners as scanners
from research.composite_scanners import FIB_RETRACEMENT_RATIOS, FIB_SWING_ATR_MULTIPLE, _finite


def _reference_swing_pairs(frame, atr_multiple=FIB_SWING_ATR_MULTIPLE, left=2, right=2):
    size = len(frame)
    result = pd.DataFrame(index=frame.index, data={
        "p1_index": np.full(size, -1, dtype=int), "p1_price": np.full(size, np.nan),
        "p1_type": np.full(size, "", dtype=object), "p2_index": np.full(size, -1, dtype=int),
        "p2_price": np.full(size, np.nan), "p2_type": np.full(size, "", dtype=object),
        "swing_atr": np.full(size, np.nan),
    })
    high = frame["high"].to_numpy(dtype=float)
    low = frame["low"].to_numpy(dtype=float)
    atr = frame["atr_ref"].to_numpy(dtype=float)
    pivots = []
    for confirmation in range(size):
        pivot_index = confirmation - right
        if pivot_index >= left:
            high_window = high[pivot_index - left:pivot_index + right + 1]
            low_window = low[pivot_index - left:pivot_index + right + 1]
            candidates = []
            if np.isfinite(high_window).all() and high[pivot_index] == high_window.max():
                candidates.append(("high", high[pivot_index]))
            if np.isfinite(low_window).all() and low[pivot_index] == low_window.min():
                candidates.append(("low", low[pivot_index]))
            if len(candidates) == 2 and pivots:
                candidates = [value for value in candidates if value[0] != pivots[-1]["type"]]
            for pivot_type, price in candidates[:1]:
                current_atr = atr[confirmation]
                if not np.isfinite(current_atr) or current_atr <= 0:
                    continue
                candidate = {"index": pivot_index, "price": float(price), "type": pivot_type, "atr": float(current_atr)}
                if not pivots:
                    pivots.append(candidate)
                elif pivot_type == pivots[-1]["type"]:
                    more_extreme = (price > pivots[-1]["price"] if pivot_type == "high" else price < pivots[-1]["price"])
                    if more_extreme:
                        pivots[-1] = candidate
                else:
                    threshold_atr = max(current_atr, pivots[-1]["atr"])
                    if abs(price - pivots[-1]["price"]) >= atr_multiple * threshold_atr:
                        pivots.append(candidate)
        if len(pivots) >= 2:
            first, second = pivots[-2:]
            swing_atr = abs(second["price"] - first["price"]) / max(first["atr"], second["atr"])
            for prefix, pivot in (("p1", first), ("p2", second)):
                result.at[frame.index[confirmation], f"{prefix}_index"] = pivot["index"]
                result.at[frame.index[confirmation], f"{prefix}_price"] = pivot["price"]
                result.at[frame.index[confirmation], f"{prefix}_type"] = pivot["type"]
            result.at[frame.index[confirmation], "swing_atr"] = swing_atr
    return result


def _reference_level_context(frame):
    result = pd.DataFrame(index=frame.index)
    size = len(frame)
    for side in ("bull", "bear"):
        result[f"{side}_level"] = np.nan
        result[f"{side}_stop"] = np.nan
        result[f"{side}_level_source"] = ""
        result[f"{side}_level_anchor"] = ""
        result[f"{side}_swing_atr"] = np.nan
        result[f"{side}_level_age_bars"] = np.nan
        result[f"{side}_prior_level_tests"] = np.nan
        result[f"{side}_level_cluster_count"] = 0
    open_ = frame["open"].to_numpy(dtype=float)
    high = frame["high"].to_numpy(dtype=float)
    low = frame["low"].to_numpy(dtype=float)
    close = frame["close"].to_numpy(dtype=float)
    atr = frame["atr_ref"].to_numpy(dtype=float)
    dates = pd.to_datetime(frame["date"]).to_numpy()
    zones = []
    swing_pairs = _reference_swing_pairs(frame)
    for position in range(size):
        current_atr = atr[position]
        if not np.isfinite(current_atr) or current_atr <= 0:
            continue
        candidates = {1: [], -1: []}
        active_zones = []
        for zone in zones:
            if position - zone["position"] > 120:
                continue
            invalid = (close[position] < zone["low"] - 0.10 * current_atr if zone["direction"] == 1
                else close[position] > zone["high"] + 0.10 * current_atr)
            if invalid:
                continue
            active_zones.append(zone)
            overlaps = (high[position] >= zone["low"] - 0.15 * current_atr
                and low[position] <= zone["high"] + 0.15 * current_atr)
            if overlaps:
                reference = zone["high"] if zone["direction"] == 1 else zone["low"]
                candidates[zone["direction"]].append({**zone, "reference": reference,
                    "distance": abs(close[position] - reference)})
        zones = active_zones
        pair = swing_pairs.iloc[position]
        first_position = int(pair["p1_index"])
        second_position = int(pair["p2_index"])
        first_price = _finite(pair["p1_price"])
        second_price = _finite(pair["p2_price"])
        if first_price is not None and second_price is not None and first_position >= 0 and second_position >= 0:
            swing_high = max(first_price, second_price)
            swing_low = min(first_price, second_price)
            direction = 1 if pair["p2_type"] == "high" else -1
            for ratio in FIB_RETRACEMENT_RATIOS:
                level = (swing_high - ratio * (swing_high - swing_low) if direction == 1
                    else swing_low + ratio * (swing_high - swing_low))
                if low[position] <= level + 0.15 * current_atr and high[position] >= level - 0.15 * current_atr:
                    anchor = (f"{pd.Timestamp(dates[first_position]).isoformat()}:"
                        f"{pd.Timestamp(dates[second_position]).isoformat()}:{ratio:.3f}")
                    candidates[direction].append({"source": f"fib_{ratio:.3f}", "anchor": anchor,
                        "position": second_position, "reference": level,
                        "low": level - 0.25 * current_atr, "high": level + 0.25 * current_atr,
                        "distance": abs(close[position] - level), "swing_atr": _finite(pair["swing_atr"])})
        for direction, side in ((1, "bull"), (-1, "bear")):
            if candidates[direction]:
                candidate = min(candidates[direction], key=lambda item: item["distance"])
                reference = float(candidate["reference"])
                origin_position = int(candidate.get("position", position))
                prior_tests = sum(high[test_position] >= reference - 0.15 * atr[test_position]
                    and low[test_position] <= reference + 0.15 * atr[test_position]
                    for test_position in range(origin_position + 1, position)
                    if np.isfinite(atr[test_position]) and atr[test_position] > 0)
                clustered_levels = []
                for item in candidates[direction]:
                    item_reference = float(item["reference"])
                    if abs(item_reference - reference) > 0.50 * current_atr:
                        continue
                    if all(abs(item_reference - existing) > 0.05 * current_atr for existing in clustered_levels):
                        clustered_levels.append(item_reference)
                result.at[frame.index[position], f"{side}_level"] = candidate["reference"]
                result.at[frame.index[position], f"{side}_stop"] = (candidate["low"] - 0.25 * current_atr
                    if direction == 1 else candidate["high"] + 0.25 * current_atr)
                result.at[frame.index[position], f"{side}_level_source"] = candidate["source"]
                result.at[frame.index[position], f"{side}_level_anchor"] = candidate["anchor"]
                result.at[frame.index[position], f"{side}_swing_atr"] = candidate.get("swing_atr")
                result.at[frame.index[position], f"{side}_level_age_bars"] = position - origin_position
                result.at[frame.index[position], f"{side}_prior_level_tests"] = prior_tests
                result.at[frame.index[position], f"{side}_level_cluster_count"] = len(clustered_levels)
        if position >= 1:
            prior_high = high[position - 1]
            prior_low = low[position - 1]
            if open_[position] > prior_high and low[position] > prior_high:
                width = low[position] - prior_high
                if width >= 0.50 * current_atr:
                    zones.append({"direction": 1, "source": "gap", "low": prior_high, "high": low[position],
                        "position": position, "anchor": pd.Timestamp(dates[position]).isoformat()})
            if open_[position] < prior_low and high[position] < prior_low:
                width = prior_low - high[position]
                if width >= 0.50 * current_atr:
                    zones.append({"direction": -1, "source": "gap", "low": high[position], "high": prior_low,
                        "position": position, "anchor": pd.Timestamp(dates[position]).isoformat()})
        if position >= 2:
            if low[position] > high[position - 2]:
                width = low[position] - high[position - 2]
                if width >= 0.30 * current_atr:
                    zones.append({"direction": 1, "source": "fvg", "low": high[position - 2], "high": low[position],
                        "position": position, "anchor": pd.Timestamp(dates[position]).isoformat()})
            if high[position] < low[position - 2]:
                width = low[position - 2] - high[position]
                if width >= 0.30 * current_atr:
                    zones.append({"direction": -1, "source": "fvg", "low": high[position], "high": low[position - 2],
                        "position": position, "anchor": pd.Timestamp(dates[position]).isoformat()})
    return result


def _frame(seed, size, *, gappy=True, missing_atr=False):
    rng = np.random.default_rng(seed)
    close = 100 + np.cumsum(rng.normal(0, 1.2, size))
    open_ = close + rng.normal(0, 0.6, size)
    if gappy:
        jumps = rng.random(size) < 0.08
        open_[jumps] += rng.choice([-4, 4], jumps.sum())
        close[jumps] += np.sign(open_[jumps] - close[jumps]) * 3
    high = np.maximum(open_, close) + rng.random(size)
    low = np.minimum(open_, close) - rng.random(size)
    atr = pd.Series(high - low).rolling(14, min_periods=1).mean().to_numpy()
    if missing_atr:
        atr[:5] = np.nan
        atr[size // 2] = 0.0
    return pd.DataFrame({"date": pd.date_range("2026-01-02 14:30", periods=size, freq="30min", tz="UTC"),
        "open": open_, "high": high, "low": low, "close": close, "atr_ref": atr},
        index=pd.RangeIndex(10, 10 + size))


@pytest.mark.parametrize("seed,size,gappy,missing_atr", [
    (1, 400, True, False), (2, 400, False, False), (3, 250, True, True), (4, 7, True, False), (5, 0, True, False),
    (6, 400, True, True),
])
def test_level_context_matches_original_scalar_implementation(seed, size, gappy, missing_atr):
    frame = _frame(seed, size, gappy=gappy, missing_atr=missing_atr)
    pd.testing.assert_frame_equal(scanners._dynamic_swing_pairs(frame), _reference_swing_pairs(frame))
    result = scanners._level_context(frame)
    pd.testing.assert_frame_equal(result, _reference_level_context(frame))
    if size >= 250:
        assert result["bull_level_source"].str.startswith(("gap", "fvg", "fib")).any()
        assert result["bear_level_source"].str.startswith(("gap", "fvg", "fib")).any()
