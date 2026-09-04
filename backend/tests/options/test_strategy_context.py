from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

from options.domain import AssetType, DecisionContext
from options.strategies.context import build_strategy_context
from options.strategies.domain import StrategyContextStatus
from equity.domain import ContextStatus, EquityContextSnapshot


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


def test_context_maps_equity_lineage_direction_and_fundamentals_separately():
    equity_context = EquityContextSnapshot(
        equity_context_snapshot_id=uuid4(), security_id=uuid4(), ticker="AAPL",
        strategy_horizon="INTRADAY_30M", market_time=MARKET_TIME,
        observed_at=OBSERVED_TIME, valid_until=OBSERVED_TIME + timedelta(minutes=45),
        status=ContextStatus.COMPLETE, universe_run_id=uuid4(),
        security_revision_id=uuid4(), fundamental_snapshot_id=uuid4(),
        regime_state="CONTINUATION", ema_direction="BEARISH",
        qualified_direction="BULLISH", direction_qualification_id=uuid4(),
        direction_evidence_id=uuid4(), direction_horizon="30m",
        direction_valid_until=OBSERVED_TIME + timedelta(minutes=45),
        trigger_state="AT_EDGE", trigger_valid_until=OBSERVED_TIME + timedelta(minutes=15),
        range_forecast_id=None, range_lower=None, range_upper=None,
        range_valid_until=None, market_cap=Decimal("3000000000000"),
        shares_outstanding=Decimal("15000000000"),
        free_float=Decimal("14500000000"), dividend_yield=0.004,
        enterprise_value=Decimal("3050000000000"),
        ebitda=Decimal("120000000000"), operating_income=Decimal("110000000000"),
        free_cash_flow=Decimal("95000000000"), risk_levels_json="{}",
        conflict_state_json="{}", stale_components_json="[]", reason_codes=(),
        summary_json='{"company_name":"Apple Inc."}',
        context_policy_version="equity_context_v1", context_policy_sha256=HASH,
    )

    result = build_strategy_context(
        uuid4(), "AAPL", AssetType.STOCK,
        DecisionContext(MARKET_TIME, OBSERVED_TIME),
        bars(100, "80", "0.2"), bars(20, "96", "0.2"),
        event_calendar_available=True, bar_observation_times_available=True,
        earnings_blackout=False, fed_blackout=False,
        policy_version="phase2_v1", policy_sha256=HASH,
        equity_context=equity_context,
    )

    assert result.trend_state == "BULLISH"
    assert result.qualified_direction == "BULLISH"
    assert result.equity_context_snapshot_id == equity_context.equity_context_snapshot_id
    assert result.company_name == "Apple Inc."
    assert result.dividend_yield == 0.004
    assert result.ebitda == Decimal("120000000000")