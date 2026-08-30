from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

from options.domain import AssetType, DecisionContext
from options.strategies.context import build_strategy_context
from options.strategies.domain import StrategyContextStatus


UTC = timezone.utc
MARKET_TIME = datetime(2026, 8, 28, 20, 0, tzinfo=UTC)
OBSERVED_TIME = MARKET_TIME + timedelta(minutes=15)
HASH = "a" * 64


def bars(count: int, start: str, step: str):
    start_value = Decimal(start)
    increment = Decimal(step)
    return tuple(
        {
            "datetime": MARKET_TIME - timedelta(days=count - index),
            "close_price": start_value + increment * index,
        }
        for index in range(count)
    )


def test_context_derives_bullish_trend_but_blocks_missing_observation_provenance():
    matrix_id = uuid4()

    result = build_strategy_context(
        matrix_id,
        "SPY",
        AssetType.ETF,
        DecisionContext(MARKET_TIME, OBSERVED_TIME),
        bars(100, "80", "0.2"),
        bars(20, "96", "0.2"),
        event_calendar_available=False,
        bar_observation_times_available=False,
        earnings_blackout=None,
        fed_blackout=None,
        policy_version="phase2_v1",
        policy_sha256=HASH,
    )

    assert result.trend_state == "BULLISH"
    assert result.earnings_blackout_state == "NOT_APPLICABLE"
    assert result.status is StrategyContextStatus.DEGRADED
    assert "BAR_OBSERVATION_TIME_UNAVAILABLE" in result.reason_codes
    assert "FED_CALENDAR_UNAVAILABLE" in result.reason_codes


def test_context_fails_when_required_bar_history_is_incomplete():
    result = build_strategy_context(
        uuid4(),
        "AAPL",
        AssetType.STOCK,
        DecisionContext(MARKET_TIME, OBSERVED_TIME),
        bars(49, "90", "0.1"),
        bars(19, "98", "0.1"),
        event_calendar_available=True,
        bar_observation_times_available=True,
        earnings_blackout=False,
        fed_blackout=False,
        policy_version="phase2_v1",
        policy_sha256=HASH,
    )

    assert result.status is StrategyContextStatus.FAILED
    assert result.trend_state is None
    assert "DAILY_TREND_HISTORY_INSUFFICIENT" in result.reason_codes
    assert "HOURLY_TREND_HISTORY_INSUFFICIENT" in result.reason_codes