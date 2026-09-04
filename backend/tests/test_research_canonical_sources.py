import inspect

from research import features, trend_pullback


def test_research_feature_loaders_use_canonical_revisions():
    source = inspect.getsource(features)

    assert "equity_bar_revisions" in source
    assert "stock_prices_daily" not in source
    assert "stock_prices_hourly" not in source


def test_trend_pullback_loader_uses_canonical_hourly_revisions():
    source = inspect.getsource(trend_pullback.load_hourly_panel)

    assert "equity_bar_revisions" in source
    assert "session_scope = 'RTH'" in source
    assert "stock_prices_hourly" not in source