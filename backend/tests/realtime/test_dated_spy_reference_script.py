from datetime import date

import pytest

from scripts.fetch_realtime_v2_dated_spy_reference import validate_result
from realtime.reference_adapters import _security_id


def test_dated_spy_reference_requires_exact_etf_identity():
    result = validate_result(
        {
            "ticker": "spy",
            "type": "ETF",
            "composite_figi": "BBG000BDTBL9",
        },
        ticker="SPY",
        session_date=date(2026, 10, 21),
    )

    assert result["ticker"] == "SPY"
    assert result["source_as_of_date"] == "2026-10-21"
    assert str(_security_id("SPY")) == (
        "50138fad-5b52-5812-8648-d920f8868528"
    )

    with pytest.raises(ValueError, match="identity"):
        validate_result(
            {
                "ticker": "IWM",
                "type": "ETF",
                "composite_figi": "BBG000BDTBL9",
            },
            ticker="QQQ",
            session_date=date(2026, 10, 21),
        )

    assert validate_result(
        {
            "ticker": "qqq",
            "type": "ETF",
            "composite_figi": "BBG000BDTBL9",
        },
        ticker="QQQ",
        session_date=date(2026, 10, 21),
    )["ticker"] == "QQQ"
