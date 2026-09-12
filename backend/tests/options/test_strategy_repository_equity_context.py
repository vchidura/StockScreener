import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4


BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.repositories.strategies import OptionStrategyRepository
from options.strategies.domain import StrategyContextSnapshot, StrategyContextStatus


def test_option_context_persists_equity_context_foreign_key():
    now = datetime(2026, 8, 28, 20, 0, tzinfo=timezone.utc)
    equity_context_id = uuid4()
    context = StrategyContextSnapshot(
        context_snapshot_id=uuid4(), matrix_id=uuid4(), underlyer="AAPL",
        market_data_time=now, observed_time=now,
        status=StrategyContextStatus.COMPLETE,
        daily_close=Decimal("100"), daily_ema_50=Decimal("95"),
        daily_input_bars=100, hourly_close=Decimal("100"),
        hourly_ema_20=Decimal("98"), hourly_input_bars=20,
        trend_state="BULLISH", earnings_blackout_state="CLEAR",
        fed_blackout_state="CLEAR", quote_spread_state="NOT_AVAILABLE",
        reason_codes=(), source_bar_keys=(), policy_version="strategy_v1",
        policy_sha256="a" * 64, equity_context_snapshot_id=equity_context_id,
        equity_context_status="COMPLETE", qualified_direction="BULLISH",
    )
    cursor = MagicMock()

    OptionStrategyRepository._persist_context(cursor, context)

    sql, parameters = cursor.execute.call_args.args
    assert "equity_context_snapshot_id" in sql
    assert "ON CONFLICT (matrix_id, policy_sha256) DO NOTHING" in sql
    assert parameters[-1] == equity_context_id
    assert sql.count("%s") == len(parameters)


def test_option_context_persists_event_and_coverage_lineage():
    now = datetime(2026, 8, 28, 20, 0, tzinfo=timezone.utc)
    market_event_id = uuid4()
    coverage_id = uuid4()
    context = StrategyContextSnapshot(
        context_snapshot_id=uuid4(), matrix_id=uuid4(), underlyer="AAPL",
        market_data_time=now, observed_time=now,
        status=StrategyContextStatus.DEGRADED,
        daily_close=Decimal("100"), daily_ema_50=Decimal("95"),
        daily_input_bars=100, hourly_close=Decimal("100"),
        hourly_ema_20=Decimal("98"), hourly_input_bars=20,
        trend_state="BULLISH", earnings_blackout_state="BLOCKED",
        fed_blackout_state="CLEAR", quote_spread_state="NOT_AVAILABLE",
        reason_codes=("EARNINGS_BLACKOUT",), source_bar_keys=(),
        policy_version="strategy_v1", policy_sha256="a" * 64,
        market_event_ids=(market_event_id,),
        event_coverage_ids=(coverage_id,),
    )
    cursor = MagicMock()

    with patch(
        "options.repositories.strategies.execute_values"
    ) as execute_values:
        OptionStrategyRepository._persist_event_lineage(cursor, context)

    assert execute_values.call_count == 2
    event_sql = execute_values.call_args_list[0].args[1]
    event_values = execute_values.call_args_list[0].args[2]
    coverage_sql = execute_values.call_args_list[1].args[1]
    coverage_values = execute_values.call_args_list[1].args[2]
    assert "option_context_market_event_evidence" in event_sql
    assert event_values == [(context.context_snapshot_id, market_event_id)]
    assert "option_context_event_coverage_evidence" in coverage_sql
    assert coverage_values == [(context.context_snapshot_id, coverage_id)]