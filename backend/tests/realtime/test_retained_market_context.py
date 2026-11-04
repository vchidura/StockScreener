import pytest

from realtime.retained_market_context import _prices


@pytest.mark.parametrize("sma", (105, None))
def test_retained_sma_is_not_relabelled_as_an_ema(sma):
    prices = _prices({"close": 110, "sma_50": sma, "chg_pct": 1})
    assert prices["moving_average_basis"] == "SMA50"
    assert prices["sma50"] == sma
    assert prices["sma50_change10"] is None
    assert prices["ema50"] is None
    assert prices["ema50_change10"] is None
