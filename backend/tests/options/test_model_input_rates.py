from datetime import date, datetime, timezone

import pytest

from options.config import load_option_runtime_configuration
from decimal import Decimal

from options.model_inputs import (
    DividendCashFlow,
    equivalent_continuous_dividend_yield,
    interpolate_rate,
    parse_treasury_curve,
)
from options.orchestration import ManualOptionPipeline


UTC = timezone.utc
OBSERVED_AT = datetime(2026, 9, 11, 12, 0, tzinfo=UTC)
XML = """<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"
      xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
  <updated>2026-09-11T02:03:08Z</updated>
  <entry><content type="application/xml"><m:properties>
    <d:NEW_DATE m:type="Edm.DateTime">2026-09-09T00:00:00</d:NEW_DATE>
    <d:BC_1MONTH m:type="Edm.Double">4.00</d:BC_1MONTH>
    <d:BC_2MONTH m:type="Edm.Double">4.20</d:BC_2MONTH>
  </m:properties></content></entry>
  <entry><content type="application/xml"><m:properties>
    <d:NEW_DATE m:type="Edm.DateTime">2026-09-10T00:00:00</d:NEW_DATE>
    <d:BC_1MONTH m:type="Edm.Double">3.90</d:BC_1MONTH>
    <d:BC_2MONTH m:type="Edm.Double">4.10</d:BC_2MONTH>
  </m:properties></content></entry>
</feed>
"""


def test_treasury_curve_parser_preserves_live_and_reconstructed_provenance():
    rows = parse_treasury_curve(XML, received_at=OBSERVED_AT)

    assert len(rows) == 4
    assert {row.rate_date for row in rows} == {
        date(2026, 9, 9), date(2026, 9, 10),
    }
    latest = [row for row in rows if row.rate_date == date(2026, 9, 10)]
    historical = [row for row in rows if row.rate_date == date(2026, 9, 9)]
    assert all(row.availability_mode == "LIVE_OBSERVED" for row in latest)
    assert all(row.replay_available_at is None for row in latest)
    assert all(
        row.availability_mode == "HISTORICAL_RECONSTRUCTED"
        and row.replay_available_at == OBSERVED_AT
        for row in historical
    )
    assert all(row.source_observed_at == datetime(2026, 9, 11, 2, 3, 8, tzinfo=UTC) for row in rows)


def test_rate_interpolation_is_maturity_matched_and_bounded():
    curve = tuple(
        row for row in parse_treasury_curve(XML, received_at=OBSERVED_AT)
        if row.rate_date == date(2026, 9, 10)
    )

    assert interpolate_rate(curve, 15) == pytest.approx(0.039)
    assert interpolate_rate(curve, 45) == pytest.approx(0.04)
    assert interpolate_rate(curve, 90) == pytest.approx(0.041)


def test_rate_interpolation_rejects_mixed_curve_dates():
    with pytest.raises(ValueError, match="share one rate date"):
        interpolate_rate(parse_treasury_curve(XML, received_at=OBSERVED_AT), 45)


def test_treasury_source_constructs_a_model_input_repository(monkeypatch):
    repository = object()
    monkeypatch.setattr(
        "options.orchestration.OptionModelInputRepository",
        lambda: repository,
    )
    configuration = load_option_runtime_configuration({
        "POLYGON_API_KEY": "secret",
        "OPTION_RISK_FREE_RATE_SOURCE": "us_treasury_par_yield_v1",
    })

    pipeline = ManualOptionPipeline(configuration, object())

    assert pipeline.model_input_repository is repository


def test_manual_rate_source_does_not_construct_a_repository(monkeypatch):
    monkeypatch.setattr(
        "options.orchestration.OptionModelInputRepository",
        lambda: (_ for _ in ()).throw(AssertionError("unexpected repository")),
    )
    configuration = load_option_runtime_configuration({"POLYGON_API_KEY": "secret"})

    pipeline = ManualOptionPipeline(configuration, object())

    assert pipeline.model_input_repository is None


def test_dividend_source_is_fingerprinted_and_constructs_repository(monkeypatch):
    repository = object()
    monkeypatch.setattr(
        "options.orchestration.EquityCorporateActionRepository",
        lambda: repository,
    )
    baseline = load_option_runtime_configuration({"POLYGON_API_KEY": "secret"})
    configured = load_option_runtime_configuration({
        "POLYGON_API_KEY": "secret",
        "OPTION_DIVIDEND_INPUT_SOURCE": "polygon_corporate_actions_v1",
    })

    pipeline = ManualOptionPipeline(configured, object())

    assert configured.configuration_sha256 != baseline.configuration_sha256
    assert pipeline.corporate_action_repository is repository


def test_discrete_dividends_convert_to_expiration_specific_yield():
    flows = (
        DividendCashFlow(date(2026, 10, 1), Decimal("0.25")),
        DividendCashFlow(date(2027, 1, 1), Decimal("0.25")),
    )

    short = equivalent_continuous_dividend_yield(
        spot=Decimal("100"),
        valuation_date=date(2026, 9, 11),
        expiration_date=date(2026, 11, 1),
        risk_free_rate=0.04,
        cash_flows=flows,
    )
    long = equivalent_continuous_dividend_yield(
        spot=Decimal("100"),
        valuation_date=date(2026, 9, 11),
        expiration_date=date(2027, 2, 1),
        risk_free_rate=0.04,
        cash_flows=flows,
    )

    assert short > 0
    assert long > 0
    assert long != pytest.approx(short)


def test_dividend_yield_is_zero_when_no_covered_cash_flow_precedes_expiry():
    assert equivalent_continuous_dividend_yield(
        spot=Decimal("100"),
        valuation_date=date(2026, 9, 11),
        expiration_date=date(2026, 9, 30),
        risk_free_rate=0.04,
        cash_flows=(DividendCashFlow(date(2026, 10, 1), Decimal("0.25")),),
    ) == 0.0