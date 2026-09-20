"""Pure stock behavior metric calculations over finalized OHLC bars."""
from __future__ import annotations

from collections.abc import Sequence
import math
from uuid import UUID

import numpy as np

from .behavior import BehaviorMetric, METRICS, Interval, MetricDefinition
from .technicals import exponential_moving_average


_METRICS_BY_ID = {definition.metric_id: definition for definition in METRICS}
_WILDER_PERIOD = 14
_EMA_SLOPE_BARS = 10


def _price_arrays(
    high: Sequence[float], low: Sequence[float], close: Sequence[float],
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    arrays = tuple(np.asarray(values, dtype=float) for values in (high, low, close))
    if any(values.ndim != 1 or len(values) == 0 for values in arrays):
        raise ValueError("high, low and close must be nonempty one-dimensional arrays")
    if len({len(values) for values in arrays}) != 1:
        raise ValueError("high, low and close must have equal length")
    high_values, low_values, close_values = arrays
    if any(not np.all(np.isfinite(values)) for values in arrays):
        raise ValueError("price arrays must contain only finite values")
    if np.any(low_values <= 0) or np.any(close_values <= 0):
        raise ValueError("low and close prices must be positive")
    if np.any(high_values < low_values) or np.any(high_values < close_values) or np.any(low_values > close_values):
        raise ValueError("price arrays violate OHLC bounds")
    return high_values, low_values, close_values


def _positive_values(values: Sequence[float], name: str) -> np.ndarray:
    result = np.asarray(values, dtype=float)
    if result.ndim != 1 or len(result) == 0:
        raise ValueError(f"{name} must be a nonempty one-dimensional array")
    if not np.all(np.isfinite(result)) or np.any(result <= 0):
        raise ValueError(f"{name} must contain only finite positive values")
    return result


def _volume_values(values: Sequence[float], expected_length: int) -> np.ndarray:
    result = np.asarray(values, dtype=float)
    if result.ndim != 1 or len(result) != expected_length:
        raise ValueError("volume must be one-dimensional and match close length")
    if not np.all(np.isfinite(result)) or np.any(result < 0):
        raise ValueError("volume must contain only finite nonnegative values")
    return result


def _definition(metric_id: str, interval: str) -> MetricDefinition:
    definition = _METRICS_BY_ID[metric_id]
    if interval not in definition.intervals:
        raise ValueError(f"{metric_id} does not support interval {interval}")
    return definition


def _unavailable(definition: MetricDefinition, interval: Interval, sample_count: int, reason: str) -> BehaviorMetric:
    status = "INSUFFICIENT_HISTORY" if sample_count < definition.minimum_samples else "UNAVAILABLE"
    return BehaviorMetric(
        definition=definition, interval=interval, status=status, value=None,
        sample_count=sample_count, reason_codes=(reason,),
    )


def _ready(definition: MetricDefinition, interval: Interval, value: float, sample_count: int) -> BehaviorMetric:
    return BehaviorMetric(
        definition=definition, interval=interval, status="READY",
        value=float(value), sample_count=sample_count,
    )


def _true_range(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    if len(close) < 2:
        return np.empty(0, dtype=float)
    return np.maximum(
        high[1:] - low[1:],
        np.maximum(np.abs(high[1:] - close[:-1]), np.abs(low[1:] - close[:-1])),
    )


def _wilder_mean_seed(values: np.ndarray, period: int) -> np.ndarray:
    if len(values) < period:
        return np.empty(0, dtype=float)
    result = np.empty(len(values) - period + 1, dtype=float)
    result[0] = float(np.mean(values[:period]))
    for index, value in enumerate(values[period:], start=1):
        result[index] = (result[index - 1] * (period - 1) + value) / period
    return result


def _ewm_first_seed(values: np.ndarray, alpha: float) -> np.ndarray:
    result = np.empty_like(values, dtype=float)
    result[0] = values[0]
    for index in range(1, len(values)):
        result[index] = alpha * values[index] + (1 - alpha) * result[index - 1]
    return result


def calculate_trend_metrics(
    high: Sequence[float], low: Sequence[float], close: Sequence[float], interval: Interval,
) -> tuple[BehaviorMetric, BehaviorMetric]:
    """Calculate v1 EMA slope and EWM-DM ADX without classifying trend state."""
    slope_definition = _definition("ema50_slope10_atr", interval)
    adx_definition = _definition("adx14", interval)
    high_values, low_values, close_values = _price_arrays(high, low, close)
    sample_count = len(close_values)
    if sample_count < slope_definition.minimum_samples:
        reason = "INSUFFICIENT_FINALIZED_BARS"
        return (
            _unavailable(slope_definition, interval, sample_count, reason),
            _unavailable(adx_definition, interval, sample_count, reason),
        )

    true_range = _true_range(high_values, low_values, close_values)
    wilder_atr = _wilder_mean_seed(true_range, _WILDER_PERIOD)
    prior_atr = wilder_atr[-2]
    ema50 = exponential_moving_average(close_values, 50)
    if prior_atr <= 0:
        slope = _unavailable(slope_definition, interval, sample_count, "PRIOR_ATR_ZERO")
    else:
        value = (ema50[-1] - ema50[-1 - _EMA_SLOPE_BARS]) / (_EMA_SLOPE_BARS * prior_atr)
        slope = _ready(slope_definition, interval, value, sample_count)

    up_move = np.diff(high_values)
    down_move = -np.diff(low_values)
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    alpha = 1 / _WILDER_PERIOD
    smoothed_range = _ewm_first_seed(true_range, alpha)
    smoothed_plus = _ewm_first_seed(plus_dm, alpha)
    smoothed_minus = _ewm_first_seed(minus_dm, alpha)
    plus_di = np.divide(100 * smoothed_plus, smoothed_range, out=np.zeros_like(smoothed_plus), where=smoothed_range > 0)
    minus_di = np.divide(100 * smoothed_minus, smoothed_range, out=np.zeros_like(smoothed_minus), where=smoothed_range > 0)
    directional_sum = plus_di + minus_di
    dx = np.divide(100 * np.abs(plus_di - minus_di), directional_sum, out=np.zeros_like(directional_sum), where=directional_sum > 0)
    adx = _ready(adx_definition, interval, _ewm_first_seed(dx, alpha)[-1], sample_count)
    return slope, adx


def calculate_atr14_fraction(
    high: Sequence[float], low: Sequence[float], close: Sequence[float], interval: Interval,
) -> BehaviorMetric:
    """Calculate mean-seeded Wilder14 true range divided by the current close."""
    definition = _definition("atr14_fraction", interval)
    high_values, low_values, close_values = _price_arrays(high, low, close)
    sample_count = len(close_values)
    if sample_count < definition.minimum_samples:
        return _unavailable(definition, interval, sample_count, "INSUFFICIENT_FINALIZED_BARS")
    wilder_atr = _wilder_mean_seed(_true_range(high_values, low_values, close_values), _WILDER_PERIOD)
    return _ready(definition, interval, wilder_atr[-1] / close_values[-1], sample_count)


def calculate_momentum_metrics(close: Sequence[float], interval: Interval) -> tuple[BehaviorMetric, BehaviorMetric]:
    """Calculate latest five-bar return and change from the preceding five-bar return."""
    return_definition = _definition("return5", interval)
    change_definition = _definition("momentum_change5", interval)
    close_values = _positive_values(close, "close")
    sample_count = len(close_values)
    if sample_count < return_definition.minimum_samples:
        latest = _unavailable(return_definition, interval, sample_count, "INSUFFICIENT_FINALIZED_BARS")
    else:
        latest = _ready(return_definition, interval, close_values[-1] / close_values[-6] - 1, sample_count)
    if sample_count < change_definition.minimum_samples:
        change = _unavailable(change_definition, interval, sample_count, "INSUFFICIENT_FINALIZED_BARS")
    else:
        recent_return = close_values[-1] / close_values[-6] - 1
        prior_return = close_values[-6] / close_values[-11] - 1
        change = _ready(change_definition, interval, recent_return - prior_return, sample_count)
    return latest, change


def _annualized_rv20(close: np.ndarray) -> float:
    log_returns = np.diff(np.log(close[-21:]))
    return float(np.std(log_returns, ddof=1) * math.sqrt(252))


def calculate_volatility_metrics(
    high: Sequence[float], low: Sequence[float], close: Sequence[float], interval: Interval,
) -> tuple[BehaviorMetric, ...]:
    """Calculate interval ATR/compression and daily-only close-to-close volatility metrics."""
    high_values, low_values, close_values = _price_arrays(high, low, close)
    sample_count = len(close_values)
    results = [calculate_atr14_fraction(high_values, low_values, close_values, interval)]
    if interval == "1d":
        rv_definition = _definition("rv20_cc_annual", interval)
        percentile_definition = _definition("rv20_percentile252", interval)
        if sample_count < rv_definition.minimum_samples:
            results.append(_unavailable(rv_definition, interval, sample_count, "INSUFFICIENT_FINALIZED_BARS"))
        else:
            results.append(_ready(rv_definition, interval, _annualized_rv20(close_values), sample_count))
        if sample_count < percentile_definition.minimum_samples:
            results.append(_unavailable(percentile_definition, interval, sample_count, "INSUFFICIENT_FINALIZED_BARS"))
        else:
            windows = np.lib.stride_tricks.sliding_window_view(np.diff(np.log(close_values)), 20)
            volatility = np.std(windows, axis=1, ddof=1) * math.sqrt(252)
            current = volatility[-1]
            prior = volatility[-253:-1]
            percentile = (np.count_nonzero(prior < current) + 0.5 * np.count_nonzero(prior == current)) / len(prior)
            results.append(_ready(percentile_definition, interval, percentile, sample_count))

    compression_definition = _definition("compression_tr5_20", interval)
    if sample_count < compression_definition.minimum_samples:
        results.append(_unavailable(compression_definition, interval, sample_count, "INSUFFICIENT_FINALIZED_BARS"))
    else:
        prior_true_range = _true_range(high_values, low_values, close_values)[-21:-1]
        baseline = float(np.mean(prior_true_range))
        if baseline <= 0:
            results.append(_unavailable(compression_definition, interval, sample_count, "PRIOR_TRUE_RANGE_ZERO"))
        else:
            results.append(_ready(compression_definition, interval, float(np.mean(prior_true_range[-5:])) / baseline, sample_count))
    return tuple(results)


def calculate_participation_metrics(close: Sequence[float], volume: Sequence[float], interval: Interval) -> tuple[BehaviorMetric, BehaviorMetric]:
    """Calculate daily relative volume and prior-session dollar-liquidity median."""
    rvol_definition = _definition("daily_rvol20", interval)
    dollar_definition = _definition("median_dollar_volume20", interval)
    close_values = _positive_values(close, "close")
    volume_values = _volume_values(volume, len(close_values))
    sample_count = len(close_values)
    if sample_count < rvol_definition.minimum_samples:
        reason = "INSUFFICIENT_FINALIZED_BARS"
        return (
            _unavailable(rvol_definition, interval, sample_count, reason),
            _unavailable(dollar_definition, interval, sample_count, reason),
        )
    prior_volume = volume_values[-21:-1]
    baseline = float(np.mean(prior_volume))
    rvol = (
        _ready(rvol_definition, interval, volume_values[-1] / baseline, sample_count)
        if baseline > 0 else _unavailable(rvol_definition, interval, sample_count, "PRIOR_VOLUME_ZERO")
    )
    dollar_volume = close_values[-21:-1] * prior_volume
    return rvol, _ready(dollar_definition, interval, float(np.median(dollar_volume)), sample_count)


def calculate_location_metrics(
    high: Sequence[float], low: Sequence[float], close: Sequence[float], interval: Interval,
) -> tuple[BehaviorMetric, BehaviorMetric]:
    """Calculate current EMA extension and position against the prior20-bar range."""
    extension_definition = _definition("extension_ema21_atr", interval)
    range_definition = _definition("prior_range20_position", interval)
    high_values, low_values, close_values = _price_arrays(high, low, close)
    sample_count = len(close_values)
    if sample_count < extension_definition.minimum_samples:
        extension = _unavailable(extension_definition, interval, sample_count, "INSUFFICIENT_FINALIZED_BARS")
    else:
        prior_atr = _wilder_mean_seed(_true_range(high_values, low_values, close_values), _WILDER_PERIOD)[-2]
        extension = (
            _ready(extension_definition, interval, (close_values[-1] - exponential_moving_average(close_values, 21)[-1]) / prior_atr, sample_count)
            if prior_atr > 0 else _unavailable(extension_definition, interval, sample_count, "PRIOR_ATR_ZERO")
        )
    if sample_count < range_definition.minimum_samples:
        position = _unavailable(range_definition, interval, sample_count, "INSUFFICIENT_FINALIZED_BARS")
    else:
        prior_low = float(np.min(low_values[-21:-1]))
        prior_high = float(np.max(high_values[-21:-1]))
        position = (
            _ready(range_definition, interval, (close_values[-1] - prior_low) / (prior_high - prior_low), sample_count)
            if prior_high > prior_low else _unavailable(range_definition, interval, sample_count, "PRIOR_RANGE_ZERO")
        )
    return extension, position


def calculate_relative_strength_metric(
    close: Sequence[float], benchmark_close: Sequence[float], benchmark_security_id: UUID, interval: Interval,
) -> BehaviorMetric:
    """Calculate paired20-bar excess close return against one explicit benchmark."""
    definition = _definition("excess_return20", interval)
    close_values = _positive_values(close, "close")
    benchmark_values = _positive_values(benchmark_close, "benchmark_close")
    if len(close_values) != len(benchmark_values):
        raise ValueError("close and benchmark_close must have equal length")
    if not isinstance(benchmark_security_id, UUID):
        raise ValueError("benchmark_security_id must be a UUID")
    sample_count = len(close_values)
    if sample_count < definition.minimum_samples:
        return _unavailable(definition, interval, sample_count, "INSUFFICIENT_PAIRED_FINALIZED_BARS")
    value = close_values[-1] / close_values[-21] - benchmark_values[-1] / benchmark_values[-21]
    return BehaviorMetric(
        definition=definition, interval=interval, status="READY", value=float(value),
        sample_count=sample_count, benchmark_security_id=benchmark_security_id,
    )