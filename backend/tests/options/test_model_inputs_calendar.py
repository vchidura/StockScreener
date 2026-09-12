import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.calendar import OptionExchangeCalendar
from options.config import load_option_runtime_configuration
from options.domain import DataQualityFlag
from options.data.normalizer import DeveloperNormalizationInput
from tests.options.test_normalizer import MARKET_TIME, _catalog, _payload
from options.data.normalizer import parse_polygon_snapshot


def test_exchange_calendar_uses_regular_early_and_prior_holiday_cutoffs():
    calendar = OptionExchangeCalendar()

    assert calendar.expiration_cutoff(date(2026, 9, 4)) == datetime(
        2026, 9, 4, 20, 0, tzinfo=timezone.utc
    )
    assert calendar.expiration_cutoff(date(2026, 11, 27)) == datetime(
        2026, 11, 27, 18, 0, tzinfo=timezone.utc
    )
    assert calendar.expiration_cutoff(date(2026, 7, 3)) == datetime(
        2026, 7, 2, 20, 0, tzinfo=timezone.utc
    )


def test_model_assumptions_are_fingerprinted_and_missing_dividend_is_explicit():
    baseline = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "secret"}, BACKEND_DIR
    )
    changed = load_option_runtime_configuration(
        {
            "POLYGON_API_KEY": "secret",
            "OPTION_RISK_FREE_RATE": "0.041",
            "OPTION_RISK_FREE_RATE_SOURCE": "manual_config_v2",
        },
        BACKEND_DIR,
    )

    assert baseline.configuration_sha256 != changed.configuration_sha256
    assert baseline.settings.risk_free_rate == Decimal("0.04")
    assert baseline.metadata()["risk_free_rate_source"] == "manual_config_v1"

    raw = parse_polygon_snapshot(_payload(), MARKET_TIME)
    normalization_input = DeveloperNormalizationInput(
        raw=raw,
        catalog=_catalog(),
        underlying_bars=(),
        expiration_cutoff=datetime(2026, 9, 4, 20, 0, tzinfo=timezone.utc),
        risk_free_rate=float(baseline.settings.risk_free_rate),
        dividend_yield=float(baseline.settings.default_dividend_yield),
        input_quality_flags=(DataQualityFlag.DIVIDEND_YIELD_DEFAULTED,),
    )
    assert normalization_input.input_quality_flags == (
        DataQualityFlag.DIVIDEND_YIELD_DEFAULTED,
    )


def test_event_calendar_freshness_is_fingerprinted():
    baseline = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "secret"}, BACKEND_DIR
    )
    changed = load_option_runtime_configuration(
        {
            "POLYGON_API_KEY": "secret",
            "OPTION_EVENT_CALENDAR_MAX_AGE_SECONDS": "21600",
        },
        BACKEND_DIR,
    )

    assert baseline.settings.event_calendar_max_age_seconds == 43200
    assert baseline.configuration_sha256 != changed.configuration_sha256


def test_iv_readiness_requires_dated_model_inputs():
    source = (
        BACKEND_DIR / "scripts" / "report_option_iv_context_readiness.py"
    ).read_text(encoding="utf-8")

    assert "POINT_IN_TIME_RATES_NOT_CONFIGURED" in source
    assert "POINT_IN_TIME_DIVIDENDS_NOT_CONFIGURED" in source
    assert 'risk_free_rate_source != "manual_config_v1"' in source