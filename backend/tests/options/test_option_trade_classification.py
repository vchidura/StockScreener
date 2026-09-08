from options.data.trade_classification import (
    TRADE_SEMANTICS_VERSION,
    classify_option_trade,
)
from options.domain import TradeClassificationStatus


def test_intermarket_sweep_is_included_without_direction_claim():
    result = classify_option_trade((219,), None)
    assert result.status is TradeClassificationStatus.INCLUDED
    assert result.semantics_version == TRADE_SEMANTICS_VERSION
    assert "PROVIDER_CONSOLIDATED_VOLUME_ELIGIBLE" in result.reasons
    assert "AGGRESSOR_SIDE_UNAVAILABLE" in result.reasons


def test_automatic_and_multileg_volume_conditions_are_included():
    for condition in (208, 209, 210, 227, 232, 248):
        assert classify_option_trade((condition,), None).status is TradeClassificationStatus.INCLUDED


def test_canceled_condition_takes_precedence_over_included_condition():
    result = classify_option_trade((209, 201), None)
    assert result.status is TradeClassificationStatus.CANCELED
    assert result.reasons == ("PROVIDER_CANCELED_CONDITION",)


def test_non_volume_condition_is_excluded():
    result = classify_option_trade((204,), None)
    assert result.status is TradeClassificationStatus.EXCLUDED
    assert result.reasons == ("PROVIDER_NON_VOLUME_CONDITION",)


def test_unknown_condition_fails_closed():
    result = classify_option_trade((999,), None)
    assert result.status is TradeClassificationStatus.UNKNOWN
    assert result.reasons == ("UNKNOWN_TRADE_CONDITION_999",)


def test_missing_condition_fails_closed():
    result = classify_option_trade((), None)
    assert result.status is TradeClassificationStatus.UNKNOWN
    assert result.reasons == ("TRADE_CONDITION_MISSING",)


def test_unmodelled_correction_fails_closed():
    result = classify_option_trade((209,), 1)
    assert result.status is TradeClassificationStatus.UNKNOWN
    assert result.reasons == ("CORRECTION_SEMANTICS_UNAVAILABLE",)
