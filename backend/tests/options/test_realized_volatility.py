from __future__ import annotations

import math
from contextlib import contextmanager
from datetime import date, timedelta

import numpy as np
import pytest

from scripts import report_realized_volatility_forecast as volatility_report

from options.analytics.realized_volatility import (
    HAR_LAGS,
    MINIMUM_DAILY_VARIANCE,
    TRADING_DAYS_PER_YEAR,
    DailyBar,
    VarianceEstimator,
    build_bar_series,
    combined_daily_variance,
    daily_variance,
    diebold_mariano,
    fit_har,
    forward_realized_volatility,
    har_design_matrix,
    intraday_open_to_close_variance,
    mincer_zarnowitz,
    out_of_sample_r2,
    qlike,
    relative_improvement,
    rolling_volatility,
    session_gap_mask,
    walk_forward_har,
    yang_zhang_volatility,
)


def _geometric_series(count: int, daily_sigma: float, seed: int = 7) -> tuple[DailyBar, ...]:
    """Bars whose close-to-close volatility is known by construction."""
    generator = np.random.default_rng(seed)
    returns = generator.normal(0.0, daily_sigma, count)
    closes = 100.0 * np.exp(np.cumsum(returns))
    bars = []
    start = date(2020, 1, 1)
    for index, close in enumerate(closes):
        open_price = close * math.exp(-returns[index] / 2)
        high = max(open_price, close) * 1.002
        low = min(open_price, close) * 0.998
        bars.append(
            DailyBar(
                session_date=start + timedelta(days=index),
                open_price=float(open_price),
                high_price=float(high),
                low_price=float(low),
                close_price=float(close),
            )
        )
    return tuple(bars)


def _flat_bars(count: int) -> tuple[DailyBar, ...]:
    start = date(2020, 1, 1)
    return tuple(
        DailyBar(
            session_date=start + timedelta(days=index),
            open_price=50.0,
            high_price=50.0,
            low_price=50.0,
            close_price=50.0,
        )
        for index in range(count)
    )


def test_build_bar_series_orders_by_session():
    bars = _geometric_series(10, 0.01)
    shuffled = tuple(reversed(bars))
    series = build_bar_series("TEST", shuffled)
    assert series.sessions == tuple(bar.session_date for bar in bars)
    assert series.close_prices[0] == pytest.approx(bars[0].close_price)


def test_close_to_close_first_session_is_undefined():
    series = build_bar_series("TEST", _geometric_series(5, 0.01))
    variance = daily_variance(series, VarianceEstimator.CLOSE_TO_CLOSE)
    assert math.isnan(variance[0])
    assert np.isfinite(variance[1:]).all()


def test_close_to_close_recovers_known_volatility():
    daily_sigma = 0.02
    series = build_bar_series("TEST", _geometric_series(4000, daily_sigma))
    variance = daily_variance(series, VarianceEstimator.CLOSE_TO_CLOSE)
    realized = rolling_volatility(variance, 3999)[-1]
    expected = daily_sigma * math.sqrt(TRADING_DAYS_PER_YEAR)
    assert realized == pytest.approx(expected, rel=0.05)


def test_range_estimators_are_positive_and_finite():
    series = build_bar_series("TEST", _geometric_series(200, 0.015))
    for estimator in (
        VarianceEstimator.PARKINSON,
        VarianceEstimator.GARMAN_KLASS,
        VarianceEstimator.ROGERS_SATCHELL,
    ):
        variance = daily_variance(series, estimator)
        assert np.isfinite(variance).all()
        assert (variance > 0).all()


def test_flat_session_is_floored_not_zero():
    series = build_bar_series("TEST", _flat_bars(30))
    for estimator in VarianceEstimator:
        variance = daily_variance(series, estimator)
        finite = variance[np.isfinite(variance)]
        assert (finite >= MINIMUM_DAILY_VARIANCE).all()
    # The floor is what keeps the log-space HAR design matrix finite.
    design = har_design_matrix(daily_variance(series, VarianceEstimator.PARKINSON))
    assert np.isfinite(design[-1]).all()


def test_rolling_volatility_never_borrows_a_short_window():
    variance = np.array([np.nan, 1e-4, 1e-4, 1e-4, 1e-4])
    result = rolling_volatility(variance, 3)
    assert math.isnan(result[0])
    assert math.isnan(result[1])
    assert math.isnan(result[2])
    assert np.isfinite(result[3])


def test_forward_realized_volatility_is_strictly_forward_looking():
    series = build_bar_series("TEST", _geometric_series(60, 0.01))
    horizon = 5
    forward = forward_realized_volatility(series, horizon)
    assert math.isnan(forward[len(series) - 1])
    assert math.isnan(forward[len(series) - horizon])
    closes = series.close_prices
    manual = math.sqrt(
        float(np.mean(np.log(closes[1 : horizon + 1] / closes[0:horizon]) ** 2))
        * TRADING_DAYS_PER_YEAR
    )
    assert forward[0] == pytest.approx(manual)


def test_har_design_matrix_shape_and_warmup():
    series = build_bar_series("TEST", _geometric_series(60, 0.01))
    design = har_design_matrix(daily_variance(series, VarianceEstimator.CLOSE_TO_CLOSE))
    assert design.shape == (60, len(HAR_LAGS) + 1)
    assert (design[:, 0] == 1.0).all()
    assert not np.isfinite(design[max(HAR_LAGS) - 1]).all()
    assert np.isfinite(design[max(HAR_LAGS)]).all()


def test_fit_har_returns_none_without_enough_observations():
    design = np.ones((3, 4))
    target = np.array([1.0, 2.0, 3.0])
    assert fit_har(design, target) is None


def test_fit_har_recovers_a_planted_relationship():
    generator = np.random.default_rng(3)
    design = np.column_stack([np.ones(500), generator.normal(size=(500, 3))])
    truth = np.array([0.5, 1.0, -0.25, 0.75])
    target = design @ truth
    coefficients = fit_har(design, target)
    assert coefficients is not None
    assert coefficients == pytest.approx(truth, rel=1e-8)


def test_walk_forward_never_trains_on_unobserved_outcomes():
    series = build_bar_series("TEST", _geometric_series(900, 0.012))
    horizon = 21
    samples = walk_forward_har(series, VarianceEstimator.PARKINSON, horizon, minimum_training=252)
    assert samples
    # A forecast at index i needs 252 training rows whose targets closed at or before i.
    assert samples[0].index >= 252 + horizon - 1
    assert samples[-1].index < len(series) - horizon


def test_walk_forward_forecasts_are_positive():
    series = build_bar_series("TEST", _geometric_series(900, 0.012))
    samples = walk_forward_har(series, VarianceEstimator.CLOSE_TO_CLOSE, 21)
    assert all(sample.har > 0 for sample in samples)
    assert all(sample.random_walk > 0 for sample in samples)


def test_qlike_is_zero_for_a_perfect_forecast():
    actual = np.array([0.2, 0.3, 0.25])
    assert qlike(actual, actual) == pytest.approx(np.zeros(3))


def test_qlike_penalises_both_directions():
    actual = np.array([0.20])
    assert qlike(actual, np.array([0.10]))[0] > 0
    assert qlike(actual, np.array([0.40]))[0] > 0


def test_relative_improvement_signs():
    baseline = np.array([1.0, 1.0])
    assert relative_improvement(np.array([0.5, 0.5]), baseline) == pytest.approx(0.5)
    assert relative_improvement(np.array([2.0, 2.0]), baseline) == pytest.approx(-1.0)


def test_diebold_mariano_detects_a_consistent_edge():
    generator = np.random.default_rng(11)
    loss_b = generator.normal(1.0, 0.1, 400)
    loss_a = loss_b - 0.05 + generator.normal(0.0, 0.02, 400)
    statistic, p_value = diebold_mariano(loss_a, loss_b, lag=0)
    assert statistic < 0
    assert p_value < 0.01


def test_diebold_mariano_is_undefined_without_dispersion():
    generator = np.random.default_rng(12)
    loss = generator.normal(1.0, 0.1, 400)
    statistic, p_value = diebold_mariano(loss, loss.copy(), lag=0)
    assert math.isnan(statistic)
    assert math.isnan(p_value)


def test_diebold_mariano_widens_with_overlap_correction():
    generator = np.random.default_rng(13)
    autocorrelated = np.convolve(generator.normal(0.0, 0.1, 600), np.ones(20) / 20, mode="same")
    loss_b = 1.0 + generator.normal(0.0, 0.05, 600)
    loss_a = loss_b - 0.01 + autocorrelated
    _, uncorrected = diebold_mariano(loss_a, loss_b, lag=0)
    _, corrected = diebold_mariano(loss_a, loss_b, lag=19)
    assert corrected > uncorrected


def test_yang_zhang_includes_the_overnight_gap():
    start = date(2020, 1, 1)
    bars = []
    price = 100.0
    for index in range(60):
        # Each session opens well away from the prior close but trades in a narrow range.
        open_price = price * (1.03 if index % 2 else 0.97)
        close = open_price * 1.0005
        bars.append(
            DailyBar(
                session_date=start + timedelta(days=index),
                open_price=open_price,
                high_price=max(open_price, close),
                low_price=min(open_price, close),
                close_price=close,
            )
        )
        price = close
    series = build_bar_series("GAP", tuple(bars))
    yang_zhang = yang_zhang_volatility(series, 20)[-1]
    rogers_satchell = rolling_volatility(
        daily_variance(series, VarianceEstimator.ROGERS_SATCHELL), 20
    )[-1]
    assert yang_zhang > rogers_satchell * 5


def test_yang_zhang_rejects_a_degenerate_window():
    series = build_bar_series("TEST", _geometric_series(30, 0.01))
    with pytest.raises(ValueError):
        yang_zhang_volatility(series, 1)


def test_session_gap_is_not_treated_as_a_return():
    bars = list(_geometric_series(40, 0.01))
    # Drop a two-month block; the surviving bars are adjacent but not consecutive.
    kept = tuple(bars[:20]) + tuple(
        DailyBar(
            session_date=bar.session_date + timedelta(days=60),
            open_price=bar.open_price * 3,
            high_price=bar.high_price * 3,
            low_price=bar.low_price * 3,
            close_price=bar.close_price * 3,
        )
        for bar in bars[20:]
    )
    series = build_bar_series("GAPPED", kept)
    mask = session_gap_mask(series)
    assert mask.sum() == 1
    assert mask[20]
    variance = daily_variance(series, VarianceEstimator.CLOSE_TO_CLOSE)
    assert math.isnan(variance[20])
    # The tripling across the hole must not leak into the label either.
    forward = forward_realized_volatility(series, 5)
    assert math.isnan(forward[16])


def test_range_estimators_ignore_session_gaps():
    bars = _geometric_series(10, 0.01)
    spaced = tuple(
        DailyBar(
            session_date=bar.session_date + timedelta(days=30 * index),
            open_price=bar.open_price,
            high_price=bar.high_price,
            low_price=bar.low_price,
            close_price=bar.close_price,
        )
        for index, bar in enumerate(bars)
    )
    series = build_bar_series("SPACED", spaced)
    # Parkinson is intraday only, so a hole between sessions cannot corrupt it.
    assert np.isfinite(daily_variance(series, VarianceEstimator.PARKINSON)).all()


def test_climatology_uses_only_prior_observations():
    series = build_bar_series("TEST", _geometric_series(900, 0.012))
    samples = walk_forward_har(series, VarianceEstimator.PARKINSON, 21)
    assert samples
    assert all(sample.climatology > 0 for sample in samples)
    # Climatology is an expanding mean, so it moves far less than the trailing window.
    climatology = np.array([sample.climatology for sample in samples])
    trailing = np.array([sample.trailing_twenty for sample in samples])
    assert climatology.std() < trailing.std()


def test_har_beats_noisy_baselines_even_without_predictability():
    """The control that makes the real-data number interpretable.

    Volatility is constant here, so nothing is forecastable. HAR still wins against a
    trailing window purely by averaging away estimator noise, which is why a margin over
    the random walk is not evidence of skill.
    """
    series = build_bar_series("CONTROL", _geometric_series(1254, 0.015, seed=42))
    samples = walk_forward_har(series, VarianceEstimator.PARKINSON, 21)
    actual = np.array([sample.actual for sample in samples])
    har = np.array([sample.har for sample in samples])
    random_walk = np.array([sample.random_walk for sample in samples])
    climatology = np.array([sample.climatology for sample in samples])
    assert relative_improvement(qlike(actual, har), qlike(actual, random_walk)) > 0.2
    assert relative_improvement(qlike(actual, har), qlike(actual, climatology)) < 0.2


def test_out_of_sample_r2_signs():
    actual = np.array([1.0, 2.0, 3.0, 4.0])
    baseline = np.array([2.0, 2.0, 2.0, 2.0])
    assert out_of_sample_r2(actual, actual, baseline) == pytest.approx(1.0)
    assert out_of_sample_r2(actual, baseline, baseline) == pytest.approx(0.0)
    worse = baseline + 5.0
    assert out_of_sample_r2(actual, worse, baseline) < 0


def test_out_of_sample_r2_undefined_for_a_perfect_baseline():
    actual = np.array([1.0, 2.0, 3.0])
    assert math.isnan(out_of_sample_r2(actual, actual, actual))


def test_mincer_zarnowitz_recovers_an_unbiased_forecast():
    generator = np.random.default_rng(21)
    forecast = generator.uniform(0.1, 0.6, 300)
    actual = forecast + generator.normal(0.0, 0.01, 300)
    intercept, slope, r_squared = mincer_zarnowitz(actual, forecast)
    assert intercept == pytest.approx(0.0, abs=0.01)
    assert slope == pytest.approx(1.0, abs=0.05)
    assert r_squared > 0.9


def test_mincer_zarnowitz_slope_exposes_an_over_reactive_forecast():
    generator = np.random.default_rng(22)
    actual = generator.uniform(0.1, 0.6, 300)
    # Forecast swings twice as far from the mean as the outcome does.
    over_reactive = actual.mean() + (actual - actual.mean()) * 2.0
    _, slope, _ = mincer_zarnowitz(actual, over_reactive)
    assert slope == pytest.approx(0.5, abs=0.05)


def test_intraday_variance_sums_the_session_path():
    path = (101.0, 102.0, 101.5, 100.75)
    variance = intraday_open_to_close_variance(100.0, path)
    prices = np.array([100.0, 101.0, 102.0, 101.5, 100.75])
    assert variance == pytest.approx(float((np.diff(np.log(prices)) ** 2).sum()))


def test_intraday_variance_rejects_a_broken_session():
    assert math.isnan(intraday_open_to_close_variance(100.0, (101.0, 102.0)))
    assert math.isnan(intraday_open_to_close_variance(0.0, (1.0, 2.0, 3.0, 4.0)))


def test_intraday_variance_is_finer_than_the_open_to_close_return():
    """A round trip has real variance even though it ends where it started."""
    round_trip = (105.0, 110.0, 105.0, 100.0)
    assert intraday_open_to_close_variance(100.0, round_trip) > 0
    # Close-to-close would score this session as flat.
    assert math.log(round_trip[-1] / 100.0) ** 2 == pytest.approx(0.0)


def test_combined_daily_variance_adds_the_overnight_gap():
    bars = _geometric_series(10, 0.01)
    series = build_bar_series("TEST", bars)
    open_to_close = np.full(len(series), 0.0001)
    combined = combined_daily_variance(series, open_to_close)
    overnight = math.log(series.open_prices[1] / series.close_prices[0])
    assert combined[1] == pytest.approx(overnight**2 + 0.0001)
    assert math.isnan(combined[0])


def test_combined_daily_variance_is_immune_to_a_split():
    """The overnight leg comes from adjusted bars, so a 10-for-1 split is invisible."""
    start = date(2020, 1, 1)
    bars = tuple(
        DailyBar(
            session_date=start + timedelta(days=index),
            open_price=100.0,
            high_price=101.0,
            low_price=99.0,
            close_price=100.5,
        )
        for index in range(6)
    )
    series = build_bar_series("SPLIT", bars)
    open_to_close = np.full(len(series), 0.0001)
    combined = combined_daily_variance(series, open_to_close)
    # An unadjusted 10-for-1 gap would give ln(0.1)**2 = 5.3; the adjusted series does not.
    assert float(np.nanmax(combined)) < 0.01


def test_combined_daily_variance_requires_alignment():
    series = build_bar_series("TEST", _geometric_series(10, 0.01))
    with pytest.raises(ValueError):
        combined_daily_variance(series, np.zeros(5))


def test_walk_forward_accepts_an_external_variance_series():
    series = build_bar_series("TEST", _geometric_series(900, 0.012))
    external = daily_variance(series, VarianceEstimator.GARMAN_KLASS)
    from_estimator = walk_forward_har(series, VarianceEstimator.GARMAN_KLASS, 21)
    from_array = walk_forward_har(series, None, 21, variance=external)
    assert len(from_array) == len(from_estimator)
    assert from_array[0].har == pytest.approx(from_estimator[0].har)


def test_walk_forward_rejects_a_misaligned_variance_series():
    series = build_bar_series("TEST", _geometric_series(400, 0.012))
    with pytest.raises(ValueError):
        walk_forward_har(series, None, 21, variance=np.zeros(10))


def test_report_excludes_unadjusted_fallback(monkeypatch):
    start = date(2025, 1, 1)

    class Cursor:
        adjusted = True

        def execute(self, _query, parameters):
            self.adjusted = parameters[0]

        def fetchall(self):
            count = 299 if self.adjusted else 400
            return [
                {
                    "ticker": "TEST",
                    "session_date": start + timedelta(days=index),
                    "open_price": 100,
                    "high_price": 101,
                    "low_price": 99,
                    "close_price": 100,
                }
                for index in range(count)
            ]

    @contextmanager
    def cursor_context():
        yield Cursor()

    monkeypatch.setattr(volatility_report, "get_db_cursor", cursor_context)
    series, provenance = volatility_report._load_series(("TEST",))

    assert series == {}
    assert provenance["TEST"] == {
        "status": "EXCLUDED_INSUFFICIENT_ADJUSTED_HISTORY",
        "adjusted": True,
        "adjusted_sessions": 299,
        "unadjusted_sessions": 400,
        "minimum_sessions": volatility_report.MINIMUM_DAILY_SESSIONS,
    }
    with pytest.raises(ValueError):
        walk_forward_har(series, None, 21)
