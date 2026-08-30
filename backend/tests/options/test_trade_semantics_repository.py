import inspect
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.domain import (
    DecisionContext,
    ProviderTradeSemantics,
    TradeSemanticsBehavior,
)
from options.repositories.trade_semantics import OptionTradeSemanticsRepository


def test_semantics_read_is_point_in_time_and_context_required():
    now = datetime(2026, 8, 29, 20, 0, tzinfo=timezone.utc)
    row = {
        "provider": "polygon",
        "semantics_version": "polygon_v1",
        "condition_code": 1,
        "correction_code": None,
        "behavior": "INCLUDE",
        "contributes_volume": True,
        "contributes_notional": True,
        "effective_from": now,
        "effective_to": None,
        "first_observed_at": now,
        "configuration_sha256": "a" * 64,
    }
    cursor = MagicMock()
    cursor.closed = False
    cursor.fetchone.return_value = row
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    repository = OptionTradeSemanticsRepository(factory)
    context = DecisionContext(now, now)
    result = repository.get("polygon", 1, None, context)

    assert result is not None
    assert result.behavior is TradeSemanticsBehavior.INCLUDE
    sql, parameters = cursor.execute.call_args.args
    assert "effective_from <= %s" in sql
    assert "first_observed_at <= %s" in sql
    assert parameters[-1] == context.observed_time
    signature = inspect.signature(repository.get)
    assert signature.parameters["context"].default is inspect.Parameter.empty