import inspect

from options.strategies.context import OptionStrategyContextRepository


def test_option_context_fallback_uses_canonical_point_in_time_bars():
    source = inspect.getsource(OptionStrategyContextRepository.build)

    assert "_CANONICAL_CLOSE_QUERY" in source
    assert "decision_context.market_time" in source
    assert "decision_context.observed_time" in source
    assert "option_event_calendar_coverage" in source
    assert "event_calendar_provider" in source
    assert "event_calendar_max_age_seconds" in source
    assert "stock_prices_daily" not in source
    assert "stock_prices_hourly" not in source