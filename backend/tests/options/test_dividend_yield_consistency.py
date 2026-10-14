"""Dividend yield / maturity consistency for the local IV solver."""
import math
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics import OptionValuationInput, solve_local_greeks
from options.analytics.greeks import price_and_greeks
from options.data.normalizer import (
    TIMESTAMP_MATURITY_DIVIDEND_MODEL_VERSIONS,
    DeveloperNormalizationInput,
    DeveloperOptionNormalizer,
    parse_polygon_snapshot,
)
from options.domain import ContractType
from options.model_inputs import equivalent_continuous_dividend_yield

EASTERN = timezone(timedelta(hours=-4))


def _scenario(expiry_days, hour, minute, *, cash=1.75, spot=600.0, rate=0.043):
    valuation = datetime(2026, 10, 12, hour, minute, tzinfo=EASTERN)
    cutoff = datetime(2026, 10, 12 + expiry_days, 16, 0, tzinfo=EASTERN)
    maturity = (cutoff - valuation).total_seconds() / (365 * 86400)
    flows = (SimpleNamespace(ex_date=date(2026, 10, 13), cash_amount=Decimal(str(cash))),)
    return valuation, cutoff, maturity, flows, spot, rate


def _iv_error_points(kind, q, maturity, spot, rate, cash, true_vol=0.15):
    present_value = cash * math.exp(-rate / 365.0)
    # The reference market: European option on the dividend-adjusted (prepaid forward) spot, exact timestamp T.
    mark = price_and_greeks(kind, spot=spot - present_value, strike=spot, maturity=maturity, rate=rate,
                            dividend=0.0, volatility=true_vol)[0]
    result = solve_local_greeks((OptionValuationInput(
        kind, Decimal(str(spot)), Decimal(str(spot)), Decimal(repr(mark)), maturity, rate, q),))[0]
    assert result.converged
    return (result.local_iv - true_vol) * 100.0


def test_default_yield_is_unchanged_and_timestamp_maturity_is_opt_in():
    valuation, cutoff, maturity, flows, spot, rate = _scenario(1, 9, 35)
    legacy = equivalent_continuous_dividend_yield(
        spot=Decimal(str(spot)), valuation_date=valuation.date(), expiration_date=cutoff.date(),
        risk_free_rate=rate, cash_flows=flows)
    explicit_legacy = equivalent_continuous_dividend_yield(
        spot=Decimal(str(spot)), valuation_date=valuation.date(), expiration_date=cutoff.date(),
        risk_free_rate=rate, cash_flows=flows, maturity_years=None)
    matched = equivalent_continuous_dividend_yield(
        spot=Decimal(str(spot)), valuation_date=valuation.date(), expiration_date=cutoff.date(),
        risk_free_rate=rate, cash_flows=flows, maturity_years=maturity)
    assert legacy == explicit_legacy
    present_value = 1.75 * math.exp(-rate / 365.0)
    assert matched == pytest.approx(-math.log1p(-present_value / spot) / maturity, rel=1e-12)
    assert matched != pytest.approx(legacy, rel=1e-3)
    assert equivalent_continuous_dividend_yield(
        spot=Decimal(str(spot)), valuation_date=valuation.date(), expiration_date=cutoff.date(),
        risk_free_rate=rate, cash_flows=flows, maturity_years=-1.0) == 0.0


@pytest.mark.parametrize("expiry_days", [1, 2, 3, 5, 10])
def test_matching_the_yield_to_the_solver_maturity_removes_the_dividend_bias(expiry_days):
    valuation, cutoff, maturity, flows, spot, rate = _scenario(expiry_days, 9, 35)
    for matched_maturity in (None, maturity):
        q = equivalent_continuous_dividend_yield(
            spot=Decimal(str(spot)), valuation_date=valuation.date(), expiration_date=cutoff.date(),
            risk_free_rate=rate, cash_flows=flows, maturity_years=matched_maturity)
        errors = [_iv_error_points(kind, q, maturity, spot, rate, 1.75) for kind in (ContractType.CALL, ContractType.PUT)]
        if matched_maturity is None:
            assert max(abs(error) for error in errors) > 0.04  # legacy: biased by the day-count mismatch
            assert errors[0] > 0 > errors[1]  # call IV too high, put IV too low
        else:
            assert max(abs(error) for error in errors) < 1e-4  # only the solver's own price tolerance remains


def test_legacy_bias_is_largest_for_one_day_expiries_early_in_the_session():
    legacy = {}
    for expiry_days, hour in ((1, 9), (1, 15), (10, 9)):
        valuation, cutoff, maturity, flows, spot, rate = _scenario(expiry_days, hour, 35 if hour == 9 else 30)
        q = equivalent_continuous_dividend_yield(
            spot=Decimal(str(spot)), valuation_date=valuation.date(), expiration_date=cutoff.date(),
            risk_free_rate=rate, cash_flows=flows)
        legacy[(expiry_days, hour)] = abs(_iv_error_points(ContractType.PUT, q, maturity, spot, rate, 1.75))
    assert legacy[(1, 9)] > 2.0 > legacy[(1, 15)] > legacy[(10, 9)]


def _normalizer(model_version):
    from options.config import load_option_runtime_configuration

    configuration = load_option_runtime_configuration({"POLYGON_API_KEY": "test-secret"}, BACKEND_DIR)
    return DeveloperOptionNormalizer(configuration.policy, configuration.valuation_policy, model_version=model_version)


def test_v2_model_version_selects_the_timestamp_maturity_yield_in_the_normalizer():
    from options.analytics import UnderlyingMinuteBar
    from tests.options.test_normalizer import MARKET_TIME, OBSERVED_AT, _catalog, _payload

    assert "black_scholes_european_v2" in TIMESTAMP_MATURITY_DIVIDEND_MODEL_VERSIONS
    assert "black_scholes_european_v1" not in TIMESTAMP_MATURITY_DIVIDEND_MODEL_VERSIONS
    raw = parse_polygon_snapshot(_payload(), OBSERVED_AT)
    flows = (SimpleNamespace(ex_date=(raw.option_mark_time + timedelta(days=1)).date(), cash_amount=Decimal("1.0")),)
    item = DeveloperNormalizationInput(
        raw=raw, catalog=_catalog(), underlying_bars=(UnderlyingMinuteBar(Decimal("100"), MARKET_TIME),),
        expiration_cutoff=datetime(2026, 9, 4, 20, 0, tzinfo=timezone.utc), risk_free_rate=0.04, dividend_yield=0.0,
        dividend_coverage_available=True, dividend_cash_flows=flows)
    yields = {}
    for version in ("black_scholes_european_v1", "black_scholes_european_v2"):
        result = _normalizer(version).normalize(uuid4(), (item,))
        assert result.retained_count == 1
        snapshot = result.snapshots[0]
        assert snapshot.model_version == version
        yields[version] = snapshot.dividend_yield
    assert yields["black_scholes_european_v1"] != pytest.approx(yields["black_scholes_european_v2"], rel=1e-4)
