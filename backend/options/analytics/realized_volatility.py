"""Realized-volatility estimators and the HAR forecast shared by research and production.

The platform's option gates are all computed under the risk-neutral measure, so nothing
in it can say whether implied volatility is expensive. A realized-volatility forecast is
the physical-measure counterpart: it states what volatility is expected to be, derived
only from underlying price history, independently of what the option market implies.

Estimators are exposed as *daily variance contributions* so a rolling mean turns any of
them into a realized volatility over an arbitrary window. Yang-Zhang is the exception —
it is defined over a window because it needs the dispersion of the overnight and
open-to-close components — so it has its own windowed entry point.

All volatilities returned by this module are annualized and expressed as fractions
(0.25 == 25% annualized), matching `local_iv` on option snapshots.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from enum import Enum

import numpy as np
from scipy.special import ndtr

from options.domain import VarianceEstimator

TRADING_DAYS_PER_YEAR = 252

# A daily variance below this is a flat or halted session, not measurable volatility.
# HAR regresses on log variance, so an exact zero would produce -inf and poison the fit.
# 1e-10 corresponds to a ~0.1 basis-point daily move, far below any real session.
MINIMUM_DAILY_VARIANCE = 1e-10

# A close-to-close return spanning more calendar days than this is not one session's
# move, it is a hole in the history. Treating it as a return manufactures an enormous
# variance out of missing data. Long weekends and holiday closures stay inside 7 days.
MAXIMUM_SESSION_GAP_DAYS = 7

# Below this many intraday bars a session is broken rather than short. Half sessions
# genuinely carry less variance and are kept unscaled, because the forecast target is
# close-to-close variance over the real calendar, not a variance rate per trading hour.
MINIMUM_INTRADAY_BARS = 4

# Rogers-Satchell and Garman-Klass need an open; close-to-close does not.
_GARMAN_KLASS_CONSTANT = 2.0 * math.log(2.0) - 1.0
_PARKINSON_CONSTANT = 1.0 / (4.0 * math.log(2.0))

HAR_LAGS = (1, 5, 22)


@dataclass(frozen=True, slots=True)
class DailyBar:
    session_date: date
    open_price: float
    high_price: float
    low_price: float
    close_price: float


@dataclass(frozen=True, slots=True)
class BarSeries:
    """Ordered, deduplicated daily bars for one ticker."""

    ticker: str
    sessions: tuple[date, ...]
    open_prices: np.ndarray
    high_prices: np.ndarray
    low_prices: np.ndarray
    close_prices: np.ndarray

    def __len__(self) -> int:
        return len(self.sessions)


def build_bar_series(ticker: str, bars: tuple[DailyBar, ...]) -> BarSeries:
    ordered = sorted(bars, key=lambda bar: bar.session_date)
    return BarSeries(
        ticker=ticker,
        sessions=tuple(bar.session_date for bar in ordered),
        open_prices=np.array([bar.open_price for bar in ordered], dtype=float),
        high_prices=np.array([bar.high_price for bar in ordered], dtype=float),
        low_prices=np.array([bar.low_price for bar in ordered], dtype=float),
        close_prices=np.array([bar.close_price for bar in ordered], dtype=float),
    )


def session_gap_mask(series: BarSeries) -> np.ndarray:
    """True where the return into that session spans a hole in the history."""
    mask = np.zeros(len(series), dtype=bool)
    for index in range(1, len(series)):
        span = (series.sessions[index] - series.sessions[index - 1]).days
        mask[index] = span > MAXIMUM_SESSION_GAP_DAYS
    return mask


def daily_variance(series: BarSeries, estimator: VarianceEstimator) -> np.ndarray:
    """Per-session variance. Index 0 is NaN for estimators that need a prior close."""
    high = series.high_prices
    low = series.low_prices
    open_price = series.open_prices
    close = series.close_prices

    if estimator is VarianceEstimator.CLOSE_TO_CLOSE:
        variance = np.full(len(series), np.nan)
        if len(series) > 1:
            log_return = np.log(close[1:] / close[:-1])
            variance[1:] = log_return**2
        variance[session_gap_mask(series)] = np.nan
        return _floor_variance(variance)

    log_high_low = np.log(high / low)
    if estimator is VarianceEstimator.PARKINSON:
        return _floor_variance(_PARKINSON_CONSTANT * log_high_low**2)

    if estimator is VarianceEstimator.GARMAN_KLASS:
        log_close_open = np.log(close / open_price)
        return _floor_variance(
            0.5 * log_high_low**2 - _GARMAN_KLASS_CONSTANT * log_close_open**2
        )

    log_high_close = np.log(high / close)
    log_high_open = np.log(high / open_price)
    log_low_close = np.log(low / close)
    log_low_open = np.log(low / open_price)
    return _floor_variance(
        log_high_close * log_high_open + log_low_close * log_low_open
    )


def _floor_variance(variance: np.ndarray) -> np.ndarray:
    floored = np.where(np.isnan(variance), np.nan, np.maximum(variance, MINIMUM_DAILY_VARIANCE))
    return floored


def rolling_volatility(variance: np.ndarray, window: int) -> np.ndarray:
    """Annualized volatility from a trailing mean of per-session variance.

    Element i uses variance[i - window + 1 : i + 1]; positions with any NaN in that
    span are NaN, so a warmup period never silently borrows a shorter window.
    """
    if window < 1:
        raise ValueError("window must be at least 1")
    result = np.full(variance.shape, np.nan)
    for index in range(window - 1, len(variance)):
        span = variance[index - window + 1 : index + 1]
        if np.isnan(span).any():
            continue
        result[index] = math.sqrt(float(span.mean()) * TRADING_DAYS_PER_YEAR)
    return result


def yang_zhang_volatility(series: BarSeries, window: int) -> np.ndarray:
    """Annualized Yang-Zhang volatility over a trailing window.

    Yang-Zhang adds the overnight gap that Rogers-Satchell and Parkinson ignore, which
    matters for single names where a large share of variance arrives outside the session.
    """
    if window < 2:
        raise ValueError("Yang-Zhang needs a window of at least 2 sessions")
    count = len(series)
    result = np.full(count, np.nan)
    if count < window + 1:
        return result

    overnight = np.full(count, np.nan)
    overnight[1:] = np.log(series.open_prices[1:] / series.close_prices[:-1])
    overnight[session_gap_mask(series)] = np.nan
    open_to_close = np.log(series.close_prices / series.open_prices)
    rogers_satchell = daily_variance(series, VarianceEstimator.ROGERS_SATCHELL)

    factor = 0.34 / (1.34 + (window + 1) / (window - 1))
    for index in range(window, count):
        start = index - window + 1
        overnight_span = overnight[start : index + 1]
        if np.isnan(overnight_span).any():
            continue
        close_span = open_to_close[start : index + 1]
        rs_span = rogers_satchell[start : index + 1]
        variance = (
            float(np.var(overnight_span, ddof=1))
            + factor * float(np.var(close_span, ddof=1))
            + (1.0 - factor) * float(rs_span.mean())
        )
        result[index] = math.sqrt(max(variance, MINIMUM_DAILY_VARIANCE) * TRADING_DAYS_PER_YEAR)
    return result


def forward_realized_volatility(series: BarSeries, horizon: int) -> np.ndarray:
    """Annualized close-to-close volatility over the NEXT `horizon` sessions.

    This is the label an option seller is actually exposed to: the variance realized
    after the position is opened. Element i is only observable at index i + horizon,
    which is what makes the training embargo in `walk_forward_har` necessary.
    """
    if horizon < 1:
        raise ValueError("horizon must be at least 1")
    count = len(series)
    result = np.full(count, np.nan)
    if count < 2:
        return result
    squared = np.full(count, np.nan)
    squared[1:] = np.log(series.close_prices[1:] / series.close_prices[:-1]) ** 2
    squared[session_gap_mask(series)] = np.nan
    for index in range(count - horizon):
        span = squared[index + 1 : index + horizon + 1]
        if np.isnan(span).any():
            continue
        result[index] = math.sqrt(float(span.mean()) * TRADING_DAYS_PER_YEAR)
    return result


def intraday_open_to_close_variance(
    session_open: float, bar_closes: tuple[float, ...]
) -> float:
    """Sum of squared log returns along one session's intraday price path.

    Splits and dividends are applied between sessions, so a within-session path is
    unaffected by them even when the bars themselves are unadjusted.
    """
    if len(bar_closes) < MINIMUM_INTRADAY_BARS or session_open <= 0:
        return float("nan")
    path = np.concatenate(([session_open], np.asarray(bar_closes, dtype=float)))
    if (path <= 0).any():
        return float("nan")
    return float((np.diff(np.log(path)) ** 2).sum())


def combined_daily_variance(
    series: BarSeries, open_to_close: np.ndarray
) -> np.ndarray:
    """Overnight gap plus intraday path variance, aligned to the daily series.

    The overnight leg is taken from the daily bars, which are split-adjusted, while the
    intraday leg comes from the raw intraday path. Mixing them this way is what keeps a
    10-for-1 split from registering as a 90% overnight move.
    """
    if len(open_to_close) != len(series):
        raise ValueError("open_to_close must align with the daily series")
    overnight = np.full(len(series), np.nan)
    if len(series) > 1:
        overnight[1:] = np.log(series.open_prices[1:] / series.close_prices[:-1])
    overnight[session_gap_mask(series)] = np.nan
    return _floor_variance(overnight**2 + open_to_close)


def har_design_matrix(variance: np.ndarray) -> np.ndarray:
    """Columns [intercept, log rv_daily, log rv_weekly, log rv_monthly].

    Log space keeps forecasts positive and matches the right-skewed distribution of
    variance, which is why HAR is conventionally estimated this way.
    """
    columns = [np.ones(len(variance))]
    for lag in HAR_LAGS:
        columns.append(np.log(rolling_volatility(variance, lag)))
    return np.column_stack(columns)


def fit_har(design: np.ndarray, target: np.ndarray) -> np.ndarray | None:
    """Least-squares HAR coefficients, or None when the sample is unusable."""
    usable = np.isfinite(design).all(axis=1) & np.isfinite(target)
    if int(usable.sum()) <= design.shape[1]:
        return None
    coefficients, _, rank, _ = np.linalg.lstsq(design[usable], target[usable], rcond=None)
    if rank < design.shape[1]:
        return None
    return coefficients


@dataclass(frozen=True, slots=True)
class ForecastSample:
    """One out-of-sample forecast and the outcome it was later scored against."""

    index: int
    session_date: date
    actual: float
    har: float
    random_walk: float
    trailing_twenty: float
    climatology: float


def _expanding_climatology(target: np.ndarray) -> np.ndarray:
    """Mean of every log target observed strictly before each index, exponentiated.

    This is the forecast that ignores the current state entirely and predicts the
    long-run average. Beating a noisy trailing window proves only that averaging
    reduces estimator noise; beating climatology is what proves that time variation
    in volatility is being predicted at all.
    """
    finite = np.isfinite(target)
    values = np.where(finite, target, 0.0)
    running_sum = np.concatenate(([0.0], np.cumsum(values)))
    running_count = np.concatenate(([0], np.cumsum(finite.astype(int))))
    result = np.full(len(target), np.nan)
    for index in range(len(target)):
        if running_count[index] == 0:
            continue
        result[index] = math.exp(running_sum[index] / running_count[index])
    return result


def walk_forward_har(
    series: BarSeries,
    estimator: VarianceEstimator | None,
    horizon: int,
    minimum_training: int = 252,
    refit_every: int = 21,
    *,
    variance: np.ndarray | None = None,
) -> tuple[ForecastSample, ...]:
    """Expanding-window HAR forecasts with a strict observability embargo.

    At forecast index `t` the training set contains only observations whose target
    window has already closed (`s + horizon <= t`). Without that embargo the model is
    fitted on outcomes that had not happened yet, which is the standard way a volatility
    backtest manufactures skill.

    Supplying `variance` runs an externally computed series, such as intraday realized
    variance, through this identical harness so the comparison is like for like.
    """
    if variance is None:
        if estimator is None:
            raise ValueError("provide either an estimator or a variance series")
        variance = daily_variance(series, estimator)
    elif len(variance) != len(series):
        raise ValueError("variance must align with the daily series")
    design = har_design_matrix(variance)
    target = np.log(forward_realized_volatility(series, horizon))
    actual = forward_realized_volatility(series, horizon)
    random_walk = rolling_volatility(variance, horizon)
    trailing_twenty = rolling_volatility(
        daily_variance(series, VarianceEstimator.CLOSE_TO_CLOSE), 20
    )
    climatology = _expanding_climatology(target)

    samples: list[ForecastSample] = []
    coefficients: np.ndarray | None = None
    since_refit = refit_every
    for index in range(len(series) - horizon):
        # The training set is always a prefix, so it is sliced rather than masked; a
        # boolean mask would copy the whole design matrix on every iteration.
        observable = max(index - horizon + 1, 0)
        if observable < minimum_training:
            continue
        if since_refit >= refit_every or coefficients is None:
            fitted = fit_har(design[:observable], target[:observable])
            if fitted is not None:
                coefficients = fitted
                since_refit = 0
        since_refit += 1
        if coefficients is None:
            continue
        row = design[index]
        if (
            not np.isfinite(row).all()
            or not np.isfinite(actual[index])
            or not np.isfinite(random_walk[index])
            or not np.isfinite(trailing_twenty[index])
            or not np.isfinite(climatology[observable])
        ):
            continue
        samples.append(
            ForecastSample(
                index=index,
                session_date=series.sessions[index],
                actual=float(actual[index]),
                har=float(math.exp(float(row @ coefficients))),
                random_walk=float(random_walk[index]),
                trailing_twenty=float(trailing_twenty[index]),
                climatology=float(climatology[observable]),
            )
        )
    return tuple(samples)


def qlike(actual: np.ndarray, forecast: np.ndarray) -> np.ndarray:
    """Per-observation QLIKE loss on variance.

    QLIKE is used instead of squared error because it stays consistent when the outcome
    is a noisy proxy for true variance, which realized volatility always is.
    """
    ratio = (actual**2) / (forecast**2)
    return ratio - np.log(ratio) - 1.0


def diebold_mariano(loss_a: np.ndarray, loss_b: np.ndarray, lag: int) -> tuple[float, float]:
    """Diebold-Mariano statistic and two-sided p-value for `loss_a - loss_b`.

    Overlapping horizons make the loss differential autocorrelated, so the variance is
    Newey-West corrected with `lag` lags. A negative statistic favours model A.
    """
    difference = loss_a - loss_b
    count = len(difference)
    if count < 3:
        return float("nan"), float("nan")
    mean = float(difference.mean())
    centered = difference - mean
    variance = float(centered @ centered) / count
    for order in range(1, min(lag, count - 1) + 1):
        covariance = float(centered[order:] @ centered[:-order]) / count
        variance += 2.0 * (1.0 - order / (lag + 1)) * covariance
    if variance <= 0:
        return float("nan"), float("nan")
    statistic = mean / math.sqrt(variance / count)
    p_value = 2.0 * (1.0 - float(ndtr(abs(statistic))))
    return statistic, p_value


def relative_improvement(model_loss: np.ndarray, baseline_loss: np.ndarray) -> float:
    """Fraction of the baseline's loss removed by the model. Negative means worse."""
    baseline_mean = float(baseline_loss.mean())
    if baseline_mean <= 0:
        return float("nan")
    return 1.0 - float(model_loss.mean()) / baseline_mean


def out_of_sample_r2(
    actual: np.ndarray, forecast: np.ndarray, baseline: np.ndarray
) -> float:
    """Squared-error skill against a named baseline rather than against the mean.

    The textbook R2 denominator is the sample mean, which is unavailable out of sample,
    so the baseline forecast takes its place.
    """
    baseline_error = float(((actual - baseline) ** 2).sum())
    if baseline_error <= 0:
        return float("nan")
    return 1.0 - float(((actual - forecast) ** 2).sum()) / baseline_error


def mincer_zarnowitz(actual: np.ndarray, forecast: np.ndarray) -> tuple[float, float, float]:
    """Regress the outcome on the forecast; returns (intercept, slope, r_squared).

    A slope near 1 with an intercept near 0 means the forecast is unbiased across its
    range. A slope well below 1 is the classic signature of an over-reactive forecast:
    it is directionally right but scales its moves too aggressively.
    """
    if len(actual) < 3:
        return float("nan"), float("nan"), float("nan")
    design = np.column_stack([np.ones(len(forecast)), forecast])
    coefficients, _, rank, _ = np.linalg.lstsq(design, actual, rcond=None)
    if rank < 2:
        return float("nan"), float("nan"), float("nan")
    residual = actual - design @ coefficients
    total = actual - actual.mean()
    total_sum = float(total @ total)
    if total_sum <= 0:
        return float(coefficients[0]), float(coefficients[1]), float("nan")
    return (
        float(coefficients[0]),
        float(coefficients[1]),
        1.0 - float(residual @ residual) / total_sum,
    )
