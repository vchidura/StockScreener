import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock
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
    assert parameters[-1] == equity_context_id
    assert sql.count("%s") == len(parameters)