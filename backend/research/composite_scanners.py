"""Point-in-time composite scanner detectors for shadow evaluation."""
from __future__ import annotations

import json
from dataclasses import dataclass

import numpy as np
import pandas as pd

from research.trend_pullback import build_trend_pullback_patterns


@dataclass(frozen=True, slots=True)
class CompositeScannerRegistration:
    source_name: str
    source_version: str
    supported_intervals: tuple[str, ...]
    outcome_modes: tuple[str, ...] = (
        "DIRECTIONAL_HORIZON", "RECOMMENDATION_PLAN",
    )
    required_plan_fields: tuple[str, ...] = ("stop_price", "target_price")


_COMMON_INTERVALS = ("30m", "1h", "1d", "1wk")
COMPOSITE_SCANNER_REGISTRY = {
    registration.source_name: registration
    for registration in (
        CompositeScannerRegistration("structured_trend_pullback", "1.0", _COMMON_INTERVALS),
        CompositeScannerRegistration("level_retest_rejection", "1.2", _COMMON_INTERVALS),
        CompositeScannerRegistration("breakout_expansion", "1.0", _COMMON_INTERVALS),
        CompositeScannerRegistration("compression_breakout", "0.1-shadow", _COMMON_INTERVALS),
        CompositeScannerRegistration("failed_breakout_reversal", "0.1-shadow", _COMMON_INTERVALS),
        CompositeScannerRegistration("structure_reversal", "1.0", _COMMON_INTERVALS),
        CompositeScannerRegistration(
            "sma200_reclaim_rejection", "1.0", ("1d", "1wk")
        ),
    )
}
SCANNER_VERSIONS = {
    source_name: registration.source_version
    for source_name, registration in COMPOSITE_SCANNER_REGISTRY.items()
}

COMPOSITE_OUTCOME_HORIZONS = {
    "30m": {"30m": 1, "60m": 2, "120m": 4},
    "1h": {"7h": 7, "21h": 21, "35h": 35},
    "1d": {"5d": 5, "10d": 10, "21d": 21},
    "1wk": {"5wk": 5, "10wk": 10, "21wk": 21},
}

FIB_SWING_ATR_MULTIPLE = 3.0
FIB_RETRACEMENT_RATIOS = (0.236, 0.382, 0.500, 0.618, 0.786)

EVENT_COLUMNS = [
    "scanner_name", "scanner_version", "ticker", "date", "direction",
    "trigger_type", "setup_anchor", "entry_price", "atr_at_signal",
    "reference_level", "stop_price", "target_price", "metadata",
]


def _finite(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if np.isfinite(number) else None


def _point_in_time_vwap(frame: pd.DataFrame) -> tuple[pd.Series, str]:
    """Session VWAP for intraday bars; rolling 20-bar VWAP for aggregate bars."""
    typical = (
        frame["high"].astype(float)
        + frame["low"].astype(float)
        + frame["close"].astype(float)
    ) / 3.0
    volume = frame["volume"].astype(float)
    dates = pd.to_datetime(frame["date"])
    sessions = dates.dt.normalize()
    if bool(sessions.duplicated().any()):
        numerator = (typical * volume).groupby(sessions).cumsum()
        denominator = volume.groupby(sessions).cumsum().replace(0, np.nan)
        return numerator / denominator, "session"
    numerator = (typical * volume).rolling(20, min_periods=5).sum()
    denominator = volume.rolling(20, min_periods=5).sum().replace(0, np.nan)
    return numerator / denominator, "rolling_20_bar"


def _level_cluster_count(row: pd.Series, reference: float,
                         atr: float) -> int:
    """Count distinct point-in-time structural levels near a reference."""
    if not np.isfinite(reference) or not np.isfinite(atr) or atr <= 0:
        return 0
    nearby = [float(reference)]
    for column in (
        "sma20", "sma50", "ema21", "vwap_ref",
        "bull_level", "bear_level", "prior_high", "prior_low",
    ):
        value = _finite(row.get(column))
        if value is None or abs(value - reference) > 0.50 * atr:
            continue
        if all(abs(value - existing) > 0.05 * atr for existing in nearby):
            nearby.append(value)
    return len(nearby)


def _overnight_gap_atr(frame: pd.DataFrame, position: int,
                       atr: float) -> float | None:
    if position <= 0 or not np.isfinite(atr) or atr <= 0:
        return None
    current_date = pd.Timestamp(frame.loc[position, "date"])
    prior_date = pd.Timestamp(frame.loc[position - 1, "date"])
    if current_date.date() == prior_date.date():
        return None
    prior_close = _finite(frame.loc[position - 1, "close"])
    if prior_close is None:
        return None
    return _finite((float(frame.loc[position, "open"]) - prior_close) / atr)


def _trigger_name(row: pd.Series, direction: int) -> str:
    choices = (
        (("bull_hammer", "hammer"), ("bull_engulfing", "bullish_engulfing"),
         ("bull_strong_close", "bullish_strong_close"))
        if direction == 1 else
        (("bear_shooting_star", "shooting_star"),
         ("bear_engulfing", "bearish_engulfing"),
         ("bear_strong_close", "bearish_strong_close"))
    )
    return next((name for column, name in choices if bool(row[column])), "")


def _pivot_anchor(frame: pd.DataFrame, row: pd.Series, direction: int) -> str:
    position = int(row["last_high_index"] if direction == 1 else row["last_low_index"])
    if 0 <= position < len(frame):
        return pd.Timestamp(frame.iloc[position]["date"]).isoformat()
    return pd.Timestamp(row["date"]).isoformat()


def _dynamic_swing_pairs(
    frame: pd.DataFrame, atr_multiple: float = FIB_SWING_ATR_MULTIPLE,
    left: int = 2, right: int = 2,
) -> pd.DataFrame:
    """Latest ATR-filtered alternating pivot pair known at each bar."""
    size = len(frame)
    result = pd.DataFrame(index=frame.index, data={
        "p1_index": np.full(size, -1, dtype=int),
        "p1_price": np.full(size, np.nan),
        "p1_type": np.full(size, "", dtype=object),
        "p2_index": np.full(size, -1, dtype=int),
        "p2_price": np.full(size, np.nan),
        "p2_type": np.full(size, "", dtype=object),
        "swing_atr": np.full(size, np.nan),
    })
    high = frame["high"].to_numpy(dtype=float)
    low = frame["low"].to_numpy(dtype=float)
    atr = frame["atr_ref"].to_numpy(dtype=float)
    pivots: list[dict] = []

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
                candidate = {
                    "index": pivot_index, "price": float(price),
                    "type": pivot_type, "atr": float(current_atr),
                }
                if not pivots:
                    pivots.append(candidate)
                elif pivot_type == pivots[-1]["type"]:
                    more_extreme = (
                        price > pivots[-1]["price"] if pivot_type == "high"
                        else price < pivots[-1]["price"]
                    )
                    if more_extreme:
                        pivots[-1] = candidate
                else:
                    threshold_atr = max(current_atr, pivots[-1]["atr"])
                    if abs(price - pivots[-1]["price"]) >= atr_multiple * threshold_atr:
                        pivots.append(candidate)

        if len(pivots) >= 2:
            first, second = pivots[-2:]
            swing_atr = abs(second["price"] - first["price"]) / max(
                first["atr"], second["atr"]
            )
            for prefix, pivot in (("p1", first), ("p2", second)):
                result.at[frame.index[confirmation], f"{prefix}_index"] = pivot["index"]
                result.at[frame.index[confirmation], f"{prefix}_price"] = pivot["price"]
                result.at[frame.index[confirmation], f"{prefix}_type"] = pivot["type"]
            result.at[frame.index[confirmation], "swing_atr"] = swing_atr
    return result


def _level_context(frame: pd.DataFrame) -> pd.DataFrame:
    """Attach the nearest active gap/FVG/Fibonacci retest level at every bar."""
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
    zones: list[dict] = []
    swing_pairs = _dynamic_swing_pairs(frame)

    for position in range(size):
        current_atr = atr[position]
        if not np.isfinite(current_atr) or current_atr <= 0:
            continue

        candidates: dict[int, list[dict]] = {1: [], -1: []}
        active_zones = []
        for zone in zones:
            if position - zone["position"] > 120:
                continue
            invalid = (
                close[position] < zone["low"] - 0.10 * current_atr
                if zone["direction"] == 1
                else close[position] > zone["high"] + 0.10 * current_atr
            )
            if invalid:
                continue
            active_zones.append(zone)
            overlaps = (
                high[position] >= zone["low"] - 0.15 * current_atr
                and low[position] <= zone["high"] + 0.15 * current_atr
            )
            if overlaps:
                reference = zone["high"] if zone["direction"] == 1 else zone["low"]
                candidates[zone["direction"]].append({
                    **zone,
                    "reference": reference,
                    "distance": abs(close[position] - reference),
                })
        zones = active_zones

        pair = swing_pairs.iloc[position]
        first_position = int(pair["p1_index"])
        second_position = int(pair["p2_index"])
        first_price = _finite(pair["p1_price"])
        second_price = _finite(pair["p2_price"])
        if (first_price is not None and second_price is not None
                and first_position >= 0 and second_position >= 0):
            swing_high = max(first_price, second_price)
            swing_low = min(first_price, second_price)
            direction = 1 if pair["p2_type"] == "high" else -1
            for ratio in FIB_RETRACEMENT_RATIOS:
                level = (
                    swing_high - ratio * (swing_high - swing_low)
                    if direction == 1
                    else swing_low + ratio * (swing_high - swing_low)
                )
                if low[position] <= level + 0.15 * current_atr and high[position] >= level - 0.15 * current_atr:
                    anchor = (
                        f"{pd.Timestamp(dates[first_position]).isoformat()}:"
                        f"{pd.Timestamp(dates[second_position]).isoformat()}:{ratio:.3f}"
                    )
                    candidates[direction].append({
                        "source": f"fib_{ratio:.3f}", "anchor": anchor,
                        "position": second_position,
                        "reference": level,
                        "low": level - 0.25 * current_atr,
                        "high": level + 0.25 * current_atr,
                        "distance": abs(close[position] - level),
                        "swing_atr": _finite(pair["swing_atr"]),
                    })

        for direction, side in ((1, "bull"), (-1, "bear")):
            if candidates[direction]:
                candidate = min(candidates[direction], key=lambda item: item["distance"])
                reference = float(candidate["reference"])
                origin_position = int(candidate.get("position", position))
                prior_tests = sum(
                    high[test_position] >= reference - 0.15 * atr[test_position]
                    and low[test_position] <= reference + 0.15 * atr[test_position]
                    for test_position in range(origin_position + 1, position)
                    if np.isfinite(atr[test_position]) and atr[test_position] > 0
                )
                clustered_levels: list[float] = []
                for item in candidates[direction]:
                    item_reference = float(item["reference"])
                    if abs(item_reference - reference) > 0.50 * current_atr:
                        continue
                    if all(
                        abs(item_reference - existing) > 0.05 * current_atr
                        for existing in clustered_levels
                    ):
                        clustered_levels.append(item_reference)
                result.at[frame.index[position], f"{side}_level"] = candidate["reference"]
                result.at[frame.index[position], f"{side}_stop"] = (
                    candidate["low"] - 0.25 * current_atr
                    if direction == 1 else candidate["high"] + 0.25 * current_atr
                )
                result.at[frame.index[position], f"{side}_level_source"] = candidate["source"]
                result.at[frame.index[position], f"{side}_level_anchor"] = candidate["anchor"]
                result.at[frame.index[position], f"{side}_swing_atr"] = candidate.get("swing_atr")
                result.at[frame.index[position], f"{side}_level_age_bars"] = (
                    position - origin_position
                )
                result.at[frame.index[position], f"{side}_prior_level_tests"] = prior_tests
                result.at[frame.index[position], f"{side}_level_cluster_count"] = len(
                    clustered_levels
                )

        if position >= 1:
            prior_high = high[position - 1]
            prior_low = low[position - 1]
            if open_[position] > prior_high and low[position] > prior_high:
                width = low[position] - prior_high
                if width >= 0.50 * current_atr:
                    zones.append({
                        "direction": 1, "source": "gap", "low": prior_high,
                        "high": low[position], "position": position,
                        "anchor": pd.Timestamp(dates[position]).isoformat(),
                    })
            if open_[position] < prior_low and high[position] < prior_low:
                width = prior_low - high[position]
                if width >= 0.50 * current_atr:
                    zones.append({
                        "direction": -1, "source": "gap", "low": high[position],
                        "high": prior_low, "position": position,
                        "anchor": pd.Timestamp(dates[position]).isoformat(),
                    })
        if position >= 2:
            if low[position] > high[position - 2]:
                width = low[position] - high[position - 2]
                if width >= 0.30 * current_atr:
                    zones.append({
                        "direction": 1, "source": "fvg", "low": high[position - 2],
                        "high": low[position], "position": position,
                        "anchor": pd.Timestamp(dates[position]).isoformat(),
                    })
            if high[position] < low[position - 2]:
                width = low[position - 2] - high[position]
                if width >= 0.30 * current_atr:
                    zones.append({
                        "direction": -1, "source": "fvg", "low": high[position],
                        "high": low[position - 2], "position": position,
                        "anchor": pd.Timestamp(dates[position]).isoformat(),
                    })
    return result


def _event(scanner_name: str, frame: pd.DataFrame, row: pd.Series, direction: int,
           trigger_type: str, setup_anchor: str, reference: float,
           stop: float | None, metadata: dict) -> dict:
    entry = float(row["close"])
    atr = _finite(row["atr_ref"])
    if stop is None or not np.isfinite(stop) or direction * (entry - stop) <= 0:
        stop = entry - direction * atr if atr else None
    risk = abs(entry - stop) if stop is not None else atr
    target = entry + direction * 2 * risk if risk else None
    return {
        "scanner_name": scanner_name,
        "scanner_version": SCANNER_VERSIONS[scanner_name],
        "ticker": str(row["ticker"]),
        "date": row["date"],
        "direction": direction,
        "trigger_type": trigger_type,
        "setup_anchor": setup_anchor,
        "entry_price": entry,
        "atr_at_signal": atr,
        "reference_level": reference,
        "stop_price": stop,
        "target_price": target,
        "metadata": json.dumps(metadata),
    }


def _failed_breakout_reversal_events(frame: pd.DataFrame) -> list[dict]:
    """Emit next-bar failures of fresh closes beyond confirmed swing levels."""
    close = frame["close"].astype(float)
    open_ = frame["open"].astype(float)
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    atr = frame["atr_ref"].astype(float)
    high_level = frame["last_high"].shift(2)
    low_level = frame["last_low"].shift(2)
    high_pivot_index = frame["last_high_index"].shift(2)
    low_pivot_index = frame["last_low_index"].shift(2)

    prior_atr = atr.shift(1)
    pre_break_atr = atr.shift(2)
    upside_failure = (
        high_level.notna()
        & (close.shift(2) <= high_level + 0.05 * pre_break_atr)
        & (close.shift(1) > high_level + 0.05 * prior_atr)
        & (close < high_level)
        & (close < open_)
        & (close < close.shift(1))
        & (frame["close_location"] <= 0.40)
    )
    downside_failure = (
        low_level.notna()
        & (close.shift(2) >= low_level - 0.05 * pre_break_atr)
        & (close.shift(1) < low_level - 0.05 * prior_atr)
        & (close > low_level)
        & (close > open_)
        & (close > close.shift(1))
        & (frame["close_location"] >= 0.60)
    )

    events: list[dict] = []
    for direction, mask, levels, pivot_indices, failed_side in (
        (-1, upside_failure, high_level, high_pivot_index, "upside"),
        (1, downside_failure, low_level, low_pivot_index, "downside"),
    ):
        for position, row in frame[mask].iterrows():
            level = float(levels.loc[position])
            current_atr = float(atr.loc[position])
            breakout_position = position - 1
            pivot_position = int(pivot_indices.loc[position])
            if pivot_position < 0 or not np.isfinite(current_atr) or current_atr <= 0:
                continue
            stop = (
                max(float(high.loc[position]), float(high.loc[breakout_position]))
                + 0.10 * current_atr
                if direction == -1 else
                min(float(low.loc[position]), float(low.loc[breakout_position]))
                - 0.10 * current_atr
            )
            breakout_distance = direction * (
                level - float(close.loc[breakout_position])
            ) / float(prior_atr.loc[position])
            prior_tests = 0
            for test_position in range(pivot_position + 1, breakout_position):
                touched = (
                    high.loc[test_position] >= level - 0.15 * atr.loc[test_position]
                    and low.loc[test_position] <= level + 0.15 * atr.loc[test_position]
                )
                prior_tests += int(touched)
            follow_through_excursion = (
                max(0.0, float(high.loc[position]) - float(high.loc[breakout_position]))
                if failed_side == "upside" else
                max(0.0, float(low.loc[breakout_position]) - float(low.loc[position]))
            ) / current_atr
            events.append(_event(
                "failed_breakout_reversal", frame, row, direction,
                f"{failed_side}_failure:close_back_inside",
                (
                    f"{pd.Timestamp(frame.loc[pivot_position, 'date']).isoformat()}:"
                    f"{pd.Timestamp(frame.loc[breakout_position, 'date']).isoformat()}"
                ),
                level, stop,
                {
                    "failed_side": failed_side,
                    "breakout_distance_atr": _finite(breakout_distance),
                    "return_inside_atr": _finite(abs(float(row["close"]) - level) / current_atr),
                    "level_age_bars": breakout_position - pivot_position,
                    "pivot_age_bars": breakout_position - pivot_position,
                    "prior_level_tests": prior_tests,
                    "follow_through_failed": follow_through_excursion <= 0.10,
                    "follow_through_excursion_atr": _finite(follow_through_excursion),
                    "level_cluster_count": _level_cluster_count(row, level, current_atr),
                    "breakout_volume_ratio": _finite(frame.loc[breakout_position, "volume_ratio"]),
                    "reversal_volume_ratio": _finite(row["volume_ratio"]),
                    "breakout_range_ratio": _finite(frame.loc[breakout_position, "range_ratio"]),
                    "reversal_range_ratio": _finite(row["range_ratio"]),
                    "close_location": _finite(row["close_location"]),
                    "gap_atr": _finite(
                        (float(row["open"]) - float(close.loc[breakout_position]))
                        / current_atr
                    ),
                    "overnight_gap_atr": _overnight_gap_atr(
                        frame, position, current_atr
                    ),
                },
            ))
    return events


def _compression_breakout_events(frame: pd.DataFrame) -> list[dict]:
    """Emit expansion closes out of a contracting ten-bar price channel."""
    close = frame["close"].astype(float)
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    atr = frame["atr_ref"].astype(float)
    range_ratio = frame["range_ratio"].astype(float)
    volume_ratio = frame["volume_ratio"].astype(float)
    close_location = frame["close_location"].astype(float)

    channel_high = high.shift(1).rolling(10, min_periods=10).max()
    channel_low = low.shift(1).rolling(10, min_periods=10).min()
    mean_range_ratio = range_ratio.shift(1).rolling(5, min_periods=5).mean()
    channel_width_atr = (channel_high - channel_low) / atr.replace(0, np.nan)
    atr_contraction = atr / atr.shift(10).replace(0, np.nan)
    compressed = (
        (mean_range_ratio <= 0.75)
        & (channel_width_atr <= 3.0)
        & (atr_contraction <= 0.90)
    )
    bull_breakout = (
        compressed
        & (close > channel_high + 0.10 * atr)
        & (range_ratio >= 1.20)
        & (volume_ratio >= 1.20)
        & (close_location >= 0.75)
    )
    bear_breakout = (
        compressed
        & (close < channel_low - 0.10 * atr)
        & (range_ratio >= 1.20)
        & (volume_ratio >= 1.20)
        & (close_location <= 0.25)
    )

    events: list[dict] = []
    for direction, mask, levels in (
        (1, bull_breakout, channel_high),
        (-1, bear_breakout, channel_low),
    ):
        for position, row in frame[mask].iterrows():
            current_atr = float(atr.loc[position])
            reference = float(levels.loc[position])
            stop = (
                float(row["low"]) - 0.10 * current_atr
                if direction == 1 else
                float(row["high"]) + 0.10 * current_atr
            )
            trend_aligned = (
                float(row["sma20"]) > float(row["sma50"])
                if direction == 1 else
                float(row["sma20"]) < float(row["sma50"])
            )
            events.append(_event(
                "compression_breakout", frame, row, direction,
                "compressed_channel:expansion_close",
                (
                    f"{pd.Timestamp(frame.loc[position - 10, 'date']).isoformat()}:"
                    f"{pd.Timestamp(row['date']).isoformat()}:{direction}"
                ),
                reference, stop,
                {
                    "compression_bars": 10,
                    "compression_mean_range_atr": _finite(mean_range_ratio.loc[position]),
                    "compression_band_atr": _finite(channel_width_atr.loc[position]),
                    "atr_contraction_ratio": _finite(atr_contraction.loc[position]),
                    "volume_ratio": _finite(volume_ratio.loc[position]),
                    "range_ratio": _finite(range_ratio.loc[position]),
                    "close_location": _finite(close_location.loc[position]),
                    "trend_aligned": bool(trend_aligned),
                    "gap_atr": _finite(
                        (float(row["open"]) - float(close.shift(1).loc[position]))
                        / current_atr
                    ),
                    "overnight_gap_atr": _overnight_gap_atr(
                        frame, position, current_atr
                    ),
                },
            ))
    return events


def _pullback_metadata(frame: pd.DataFrame, row: pd.Series,
                       direction: int) -> dict:
    """Describe the completed pullback using only bars available at its trigger."""
    position = int(row.name)
    pivot_position = int(
        row["last_high_index"] if direction == 1 else row["last_low_index"]
    )
    atr = _finite(row["atr_ref"])
    if pivot_position < 0 or pivot_position >= position or not atr or atr <= 0:
        return {}

    pullback = frame.iloc[pivot_position + 1:position + 1]
    pre_pullback = frame.iloc[max(0, pivot_position - 19):pivot_position + 1]
    prior_pullback = pullback.iloc[:-1]
    baseline_volume = _finite(pre_pullback["volume"].astype(float).mean())
    pullback_volume = _finite(prior_pullback["volume"].astype(float).mean())
    trigger_volume = _finite(row["volume"])
    if direction == 1:
        depth = float(row["last_high"]) - float(pullback["low"].min())
        distance = float(row["last_high"]) - float(row["close"])
    else:
        depth = float(pullback["high"].max()) - float(row["last_low"])
        distance = float(row["close"]) - float(row["last_low"])
    bars = position - pivot_position
    prior_close = _finite(frame.loc[position - 1, "close"])
    gap_atr = (
        (float(row["open"]) - prior_close) / atr
        if prior_close is not None else None
    )
    vwap = _finite(row.get("vwap_ref"))
    prior_vwap = _finite(frame.loc[position - 1].get("vwap_ref"))
    vwap_reclaim = (
        vwap is not None and prior_vwap is not None and prior_close is not None
        and direction * (float(row["close"]) - vwap) >= 0
        and direction * (prior_close - prior_vwap) < 0
    )
    cluster_reference = _finite(row.get("sma20")) or float(row["close"])
    return {
        "pullback_bars": bars,
        "pivot_age_bars": bars,
        "pullback_depth_atr": _finite(depth / atr),
        "pullback_speed_atr_per_bar": _finite(depth / atr / bars),
        "swing_origin_distance_atr": _finite(distance / atr),
        "pullback_volume_ratio": _finite(
            pullback_volume / baseline_volume
            if pullback_volume is not None and baseline_volume else None
        ),
        "trigger_volume_ratio": _finite(
            trigger_volume / pullback_volume
            if trigger_volume is not None and pullback_volume else None
        ),
        "close_location": _finite(row["close_location"]),
        "gap_atr": _finite(gap_atr),
        "overnight_gap_atr": _overnight_gap_atr(frame, position, atr),
        "vwap": vwap,
        "vwap_basis": str(row.get("vwap_basis") or ""),
        "vwap_distance_atr": _finite(
            direction * (float(row["close"]) - vwap) / atr
            if vwap is not None else None
        ),
        "vwap_reclaim": bool(vwap_reclaim),
        "level_cluster_count": _level_cluster_count(
            row, cluster_reference, atr
        ),
    }


def _sma200_reclaim_rejection_events(frame: pd.DataFrame) -> list[dict]:
    """Bearish rejection near old resistance after a fresh daily SMA200 reclaim."""
    close = frame["close"].astype(float)
    high = frame["high"].astype(float)
    atr = frame["atr_ref"].astype(float)
    sma200 = close.rolling(200, min_periods=200).mean()
    reclaimed = (close > sma200) & (close.shift(1) <= sma200.shift(1))
    reclaim_age = np.full(len(frame), np.nan)
    for age in (1, 2, 3):
        age_mask = reclaimed.shift(age, fill_value=False).to_numpy() & np.isnan(reclaim_age)
        reclaim_age[age_mask] = age

    # Exclude the latest four bars so the reference represents established resistance.
    resistance_high = high.shift(5).rolling(59, min_periods=59).max()
    resistance_tolerance = np.maximum(0.25 * atr, 0.01 * resistance_high)
    participation = (frame["volume_ratio"] >= 1.0) | (frame["range_ratio"] >= 1.1)
    matches = (
        pd.Series(reclaim_age, index=frame.index).notna()
        & (sma200 < sma200.shift(20))
        & resistance_high.notna()
        & (high >= resistance_high - resistance_tolerance)
        & (high <= resistance_high + resistance_tolerance)
        & frame["bear_strong_close"]
        & participation
        & (close < close.shift(1))
        & (close <= sma200 + 0.10 * atr)
        & (close >= sma200 - 0.50 * atr)
    )

    events: list[dict] = []
    for position, row in frame[matches].iterrows():
        age = int(reclaim_age[position])
        reclaim_position = position - age
        resistance_window = frame.iloc[max(0, position - 63):position - 4]
        resistance_position = int(resistance_window["high"].astype(float).idxmax())
        reclaim_date = pd.Timestamp(frame.iloc[reclaim_position]["date"])
        resistance_date = pd.Timestamp(frame.loc[resistance_position, "date"])
        reference = float(resistance_high.loc[position])
        current_atr = float(atr.loc[position])
        current_sma200 = float(sma200.loc[position])
        state = "CONFIRMED" if float(row["close"]) < current_sma200 else "WATCH"
        stop = max(float(row["high"]), reference) + 0.25 * current_atr
        events.append(_event(
            "sma200_reclaim_rejection", frame, row, -1,
            f"{state.lower()}:bearish_strong_close",
            f"{reclaim_date.isoformat()}:{resistance_date.isoformat()}",
            reference, stop,
            {
                "signal_state": state,
                "sma200": current_sma200,
                "sma200_slope_20_pct": _finite(
                    (current_sma200 / float(sma200.shift(20).loc[position]) - 1.0) * 100.0
                ),
                "reclaim_age": age,
                "reclaim_date": reclaim_date.date().isoformat(),
                "resistance_date": resistance_date.date().isoformat(),
                "resistance_distance_atr": _finite(
                    abs(float(row["high"]) - reference) / current_atr
                ),
                "close_to_sma200_atr": _finite(
                    (float(row["close"]) - current_sma200) / current_atr
                ),
                "volume_ratio": _finite(row["volume_ratio"]),
                "range_ratio": _finite(row["range_ratio"]),
            },
        ))
    return events


def build_structured_scanner_events(patterns: pd.DataFrame) -> pd.DataFrame:
    """Normalize structured trend-pullback triggers into the shared event contract."""
    events: list[dict] = []
    for _, source in patterns.groupby("ticker", sort=False):
        frame = source.sort_values("date").reset_index(drop=True).copy()
        frame["atr_ref"] = frame["atr14"]
        frame["vwap_ref"], vwap_basis = _point_in_time_vwap(frame)
        frame["vwap_basis"] = vwap_basis
        frame["close_location"] = (
            (frame["close"] - frame["low"])
            / (frame["high"] - frame["low"]).replace(0, np.nan)
        )
        for _, row in frame[frame["direction"] != 0].iterrows():
            direction = int(row["direction"])
            reference = _finite(row["prior_high"] if direction == 1 else row["prior_low"])
            anchor = f"{direction}:{_pivot_anchor(frame, row, direction)}:{reference}"
            events.append(_event(
                "structured_trend_pullback", frame, row, direction,
                str(row["trigger_candle"]), anchor, reference, None,
                {
                    "sma20": _finite(row["sma20"]),
                    "sma50": _finite(row["sma50"]),
                    "bull_swing_support": bool(row["bull_swing_support"]),
                    "bear_swing_resistance": bool(row["bear_swing_resistance"]),
                    **_pullback_metadata(frame, row, direction),
                },
            ))
    return pd.DataFrame(events, columns=EVENT_COLUMNS)


def build_composite_scanner_events(
    panel: pd.DataFrame, patterns: pd.DataFrame | None = None,
    interval: str = "1d",
) -> pd.DataFrame:
    """Return normalized event candidates for all composite scanner families."""
    if panel.empty:
        return pd.DataFrame(columns=EVENT_COLUMNS)
    enriched = patterns if patterns is not None else build_trend_pullback_patterns(panel)
    events: list[dict] = []

    for _, source in enriched.groupby("ticker", sort=False):
        frame = source.sort_values("date").reset_index(drop=True).copy()
        close = frame["close"].astype(float)
        high = frame["high"].astype(float)
        low = frame["low"].astype(float)
        frame["ema21"] = close.ewm(span=21, adjust=False).mean()
        frame["atr_ref"] = frame["atr14"].shift(1).fillna(frame["atr14"])
        frame["volume_ratio"] = (
            frame["volume"] / frame["volume"].rolling(20).mean().shift(1).replace(0, np.nan)
        )
        frame["range_ratio"] = (high - low) / frame["atr_ref"].replace(0, np.nan)
        frame["close_location"] = (close - low) / (high - low).replace(0, np.nan)
        levels = _level_context(frame)
        for column in levels:
            frame[column] = levels[column]

        participation = (frame["volume_ratio"] >= 1.0) | (frame["range_ratio"] >= 1.1)
        bull_candle = frame[["bull_hammer", "bull_engulfing", "bull_strong_close"]].any(axis=1)
        bear_candle = frame[["bear_shooting_star", "bear_engulfing", "bear_strong_close"]].any(axis=1)

        if interval in ("1d", "1wk"):
            events.extend(_sma200_reclaim_rejection_events(frame))

        bull_level = (
            frame["bull_level"].notna() & bull_candle & participation
            & (close >= frame["bull_level"] - 0.10 * frame["atr_ref"])
        )
        bear_level = (
            frame["bear_level"].notna() & bear_candle & participation
            & (close <= frame["bear_level"] + 0.10 * frame["atr_ref"])
        )
        for direction, mask, side in ((1, bull_level, "bull"), (-1, bear_level, "bear")):
            for _, row in frame[mask].iterrows():
                source_name = str(row[f"{side}_level_source"])
                events.append(_event(
                    "level_retest_rejection", frame, row, direction,
                    f"{source_name}:{_trigger_name(row, direction)}",
                    f"{source_name}:{row[f'{side}_level_anchor']}",
                    float(row[f"{side}_level"]), _finite(row[f"{side}_stop"]),
                    {"level_source": source_name,
                     "swing_atr": _finite(row[f"{side}_swing_atr"]),
                     "pivot_age_bars": _finite(row[f"{side}_level_age_bars"]),
                     "prior_level_tests": _finite(row[f"{side}_prior_level_tests"]),
                     "level_cluster_count": int(row[f"{side}_level_cluster_count"]),
                     "overnight_gap_atr": _overnight_gap_atr(
                         frame, int(row.name), float(row["atr_ref"])
                     ),
                     "volume_ratio": _finite(row["volume_ratio"]),
                     "range_ratio": _finite(row["range_ratio"])},
                ))

        prior_high = frame["last_high"].shift(1)
        prior_low = frame["last_low"].shift(1)
        prior_high_index = frame["last_high_index"].shift(1)
        prior_low_index = frame["last_low_index"].shift(1)
        bull_breakout = (
            prior_high.notna()
            & (close.shift(1) <= prior_high + 0.05 * frame["atr_ref"])
            & (close > prior_high + 0.10 * frame["atr_ref"])
            & (frame["range_ratio"] >= 1.2) & (frame["volume_ratio"] >= 1.2)
            & (frame["close_location"] >= 0.75)
        )
        bear_breakout = (
            prior_low.notna()
            & (close.shift(1) >= prior_low - 0.05 * frame["atr_ref"])
            & (close < prior_low - 0.10 * frame["atr_ref"])
            & (frame["range_ratio"] >= 1.2) & (frame["volume_ratio"] >= 1.2)
            & (frame["close_location"] <= 0.25)
        )
        for direction, mask in ((1, bull_breakout), (-1, bear_breakout)):
            for _, row in frame[mask].iterrows():
                reference = _finite(prior_high.loc[row.name] if direction == 1 else prior_low.loc[row.name])
                pivot_index = _finite(
                    prior_high_index.loc[row.name]
                    if direction == 1 else prior_low_index.loc[row.name]
                )
                current_atr = float(row["atr_ref"])
                events.append(_event(
                    "breakout_expansion", frame, row, direction, "swing_breakout",
                    _pivot_anchor(frame, row, direction), reference,
                    None, {"volume_ratio": _finite(row["volume_ratio"]),
                           "range_ratio": _finite(row["range_ratio"]),
                           "close_location": _finite(row["close_location"]),
                           "pivot_age_bars": int(row.name - pivot_index)
                           if pivot_index is not None else None,
                           "level_cluster_count": _level_cluster_count(
                               row, reference, current_atr
                           ) if reference is not None else 0,
                           "gap_atr": _finite(
                               (float(row["open"]) - float(close.shift(1).loc[row.name]))
                               / current_atr
                           ),
                           "overnight_gap_atr": _overnight_gap_atr(
                               frame, int(row.name), current_atr
                           )},
                ))

        events.extend(_compression_breakout_events(frame))
        events.extend(_failed_breakout_reversal_events(frame))

        bull_reclaim = (close > frame["ema21"]) & (close.shift(1) <= frame["ema21"].shift(1))
        bear_reclaim = (close < frame["ema21"]) & (close.shift(1) >= frame["ema21"].shift(1))
        bull_reversal = (
            (frame["sma20"].shift(10) < frame["sma50"].shift(10))
            & frame["higher_swing_low"] & bull_reclaim & bull_candle & participation
        )
        bear_reversal = (
            (frame["sma20"].shift(10) > frame["sma50"].shift(10))
            & frame["lower_swing_high"] & bear_reclaim & bear_candle & participation
        )
        for direction, mask in ((1, bull_reversal), (-1, bear_reversal)):
            for _, row in frame[mask].iterrows():
                reference = float(row["ema21"])
                events.append(_event(
                    "structure_reversal", frame, row, direction,
                    f"ema21_reclaim:{_trigger_name(row, direction)}",
                    _pivot_anchor(frame, row, -direction), reference, None,
                    {"ema21": reference,
                     "volume_ratio": _finite(row["volume_ratio"]),
                     "range_ratio": _finite(row["range_ratio"])},
                ))

    if not events:
        return pd.DataFrame(columns=EVENT_COLUMNS)
    return pd.DataFrame(events, columns=EVENT_COLUMNS).sort_values(
        ["date", "scanner_name", "ticker"]
    ).reset_index(drop=True)


def build_all_scanner_events(panel: pd.DataFrame, interval: str = "1d") -> pd.DataFrame:
    """Build the production scanner baseline and every composite candidate once."""
    if panel.empty:
        return pd.DataFrame(columns=EVENT_COLUMNS)
    patterns = build_trend_pullback_patterns(panel)
    frames = [
        build_structured_scanner_events(patterns),
        build_composite_scanner_events(panel, patterns, interval),
    ]
    populated = [frame for frame in frames if not frame.empty]
    if not populated:
        return pd.DataFrame(columns=EVENT_COLUMNS)
    return pd.concat(populated, ignore_index=True).sort_values(
        ["date", "scanner_name", "ticker"]
    ).reset_index(drop=True)