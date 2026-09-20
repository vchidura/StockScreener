import numpy as np
import pandas as pd
import pytest
from uuid import uuid4

from equity.behavior import METRICS
from equity.behavior_calculators import (
    calculate_atr14_fraction,
    calculate_location_metrics,
    calculate_momentum_metrics,
    calculate_participation_metrics,
    calculate_relative_strength_metric,
    calculate_trend_metrics,
    calculate_volatility_metrics,
)
from equity.technicals import compute_trade_setup_technicals


def linear_bars(length=200):
    close = np.arange(100.0, 100.0 + length)
    return close + 1, close - 1, close


def test_linear_golden_vector_has_exact_wilder_atr_and_directional_adx():
    high, low, close = linear_bars()
    slope, adx = calculate_trend_metrics(high, low, close, "1d")
    atr = calculate_atr14_fraction(high, low, close, "1d")

    decay = 49 / 51
    ema_latest = close[-1] - 24.5 + decay ** 200 / (2 / 51)
    ema_ten_bars_ago = close[-11] - 24.5 + decay ** 190 / (2 / 51)
    assert slope.value == pytest.approx((ema_latest - ema_ten_bars_ago) / 20)
    assert adx.value == pytest.approx(100.0)
    assert atr.value == pytest.approx(2 / close[-1])
    assert all(metric.sample_count == 200 and metric.status == "READY" for metric in (slope, adx, atr))


def test_adx_recipe_matches_existing_unrounded_ewm_dm_semantics():
    close = 100 + np.sin(np.arange(200) / 4) * 3 + np.arange(200) / 20
    high = close + 1 + np.cos(np.arange(200) / 7) * 0.2
    low = close - 1 - np.sin(np.arange(200) / 8) * 0.2
    _, adx = calculate_trend_metrics(high, low, close, "30m")
    legacy = compute_trade_setup_technicals(
        pd.DataFrame({"high": high, "low": low, "close": close}),
        "30m", input_includes_forming_bar=False,
    )

    assert round(adx.value, 1) == legacy.adx


def test_catalog_minimum_is_enforced_before_mathematical_warmup():
    high, low, close = linear_bars(199)
    slope, adx = calculate_trend_metrics(high, low, close, "1h")
    atr = calculate_atr14_fraction(high, low, close, "1h")

    assert all(metric.status == "INSUFFICIENT_HISTORY" and metric.value is None for metric in (slope, adx, atr))
    assert all(metric.reason_codes == ("INSUFFICIENT_FINALIZED_BARS",) for metric in (slope, adx, atr))


def test_zero_range_is_a_valid_zero_except_for_atr_normalized_slope():
    close = np.full(200, 100.0)
    slope, adx = calculate_trend_metrics(close, close, close, "1d")
    atr = calculate_atr14_fraction(close, close, close, "1d")

    assert slope.status == "UNAVAILABLE" and slope.reason_codes == ("PRIOR_ATR_ZERO",)
    assert adx.status == "READY" and adx.value == 0
    assert atr.status == "READY" and atr.value == 0


def test_dimensionless_metrics_are_invariant_to_uniform_price_scaling():
    high, low, close = linear_bars()
    original = (*calculate_trend_metrics(high, low, close, "30m"), calculate_atr14_fraction(high, low, close, "30m"))
    scaled = (*calculate_trend_metrics(high * 10, low * 10, close * 10, "30m"), calculate_atr14_fraction(high * 10, low * 10, close * 10, "30m"))

    assert [metric.value for metric in scaled] == pytest.approx([metric.value for metric in original])


@pytest.mark.parametrize("invalid", ["nan", "length", "bounds", "nonpositive"])
def test_invalid_price_contracts_are_rejected(invalid):
    high, low, close = linear_bars()
    if invalid == "nan":
        close[-1] = np.nan
    if invalid == "length":
        high = high[:-1]
    if invalid == "bounds":
        high[-1] = close[-1] - 1
    if invalid == "nonpositive":
        low[-1] = 0
    with pytest.raises(ValueError):
        calculate_trend_metrics(high, low, close, "1d")


def test_metric_interval_contract_is_enforced():
    high, low, close = linear_bars()
    with pytest.raises(ValueError, match="does not support interval"):
        calculate_atr14_fraction(high, low, close, "5m")


def test_nonoverlapping_momentum_windows_have_explicit_change():
    close = 100 * 1.01 ** np.arange(11)
    latest, change = calculate_momentum_metrics(close, "1h")

    assert latest.value == pytest.approx(1.01 ** 5 - 1)
    assert change.value == pytest.approx(0)
    short_latest, short_change = calculate_momentum_metrics(close[-6:], "1h")
    assert short_latest.status == "READY"
    assert short_change.status == "INSUFFICIENT_HISTORY"


def test_daily_volatility_uses_current_rv_and_prior252_midrank():
    close = np.full(273, 100.0)
    high = close + 1
    low = close - 1
    metrics = {metric.definition.metric_id: metric for metric in calculate_volatility_metrics(high, low, close, "1d")}

    assert metrics["rv20_cc_annual"].value == 0
    assert metrics["rv20_percentile252"].value == pytest.approx(0.5)
    assert metrics["compression_tr5_20"].value == 1
    assert {metric.definition.metric_id for metric in calculate_volatility_metrics(high, low, close, "1h")} == {
        "atr14_fraction", "compression_tr5_20",
    }


def test_compression_excludes_current_true_range():
    close = np.full(22, 100.0)
    widths = np.array([2.0] + [2.0] * 15 + [4.0] * 5 + [20.0])
    high = close + widths / 2
    low = close - widths / 2
    compression = calculate_volatility_metrics(high, low, close, "30m")[-1]

    assert compression.value == pytest.approx(4 / 2.5)


def test_participation_uses_current_volume_and_prior20_dollar_median():
    close = np.full(21, 100.0)
    volume = np.arange(1.0, 22.0)
    rvol, dollar_volume = calculate_participation_metrics(close, volume, "1d")

    assert rvol.value == pytest.approx(2)
    assert dollar_volume.value == pytest.approx(1050)
    with pytest.raises(ValueError, match="does not support interval"):
        calculate_participation_metrics(close, volume, "1h")


def test_location_uses_current_ema_and_prior_atr_and_range():
    high, low, close = linear_bars()
    extension, position = calculate_location_metrics(high, low, close, "1d")
    ema_decay = 10 / 11
    ema21_latest = close[-1] - 10 + 10 * ema_decay ** 199

    assert extension.value == pytest.approx((close[-1] - ema21_latest) / 2)
    assert position.value == pytest.approx(1)


def test_relative_strength_requires_paired_daily_benchmark():
    benchmark_id = uuid4()
    stock = 100 * 1.02 ** np.arange(21)
    benchmark = 100 * 1.01 ** np.arange(21)
    metric = calculate_relative_strength_metric(stock, benchmark, benchmark_id, "1d")

    assert metric.value == pytest.approx(1.02 ** 20 - 1.01 ** 20)
    assert metric.benchmark_security_id == benchmark_id
    with pytest.raises(ValueError, match="equal length"):
        calculate_relative_strength_metric(stock, benchmark[:-1], benchmark_id, "1d")


def test_zero_denominators_remain_explicit_per_metric():
    close = np.full(200, 100.0)
    extension, position = calculate_location_metrics(close, close, close, "30m")
    rvol, dollar_volume = calculate_participation_metrics(close, np.zeros(200), "1d")

    assert extension.reason_codes == ("PRIOR_ATR_ZERO",)
    assert position.reason_codes == ("PRIOR_RANGE_ZERO",)
    assert rvol.reason_codes == ("PRIOR_VOLUME_ZERO",)
    assert dollar_volume.status == "READY" and dollar_volume.value == 0


@pytest.mark.parametrize("invalid", ["negative", "nan", "length"])
def test_invalid_volume_contract_is_rejected(invalid):
    close = np.full(21, 100.0)
    volume = np.ones(21)
    if invalid == "negative":
        volume[-1] = -1
    if invalid == "nan":
        volume[-1] = np.nan
    if invalid == "length":
        volume = volume[:-1]
    with pytest.raises(ValueError):
        calculate_participation_metrics(close, volume, "1d")


def test_every_v1_catalog_metric_has_a_pure_calculator_path():
    high, low, close = linear_bars(273)
    volume = np.arange(1.0, 274.0)
    calculated = (
        *calculate_trend_metrics(high, low, close, "1d"),
        *calculate_momentum_metrics(close, "1d"),
        *calculate_volatility_metrics(high, low, close, "1d"),
        *calculate_participation_metrics(close, volume, "1d"),
        *calculate_location_metrics(high, low, close, "1d"),
        calculate_relative_strength_metric(close, close, uuid4(), "1d"),
    )

    assert {metric.definition.metric_id for metric in calculated} == {definition.metric_id for definition in METRICS}
    assert all(metric.status == "READY" for metric in calculated)