import sys
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import database


UTC = timezone.utc


def test_latest_quote_reads_only_canonical_final_revisions():
    cursor = MagicMock()
    cursor.closed = False
    cursor.fetchone.return_value = {
        "price": 101,
        "previous_close": 100,
        "as_of": datetime(2026, 8, 28, 20, 0, tzinfo=UTC),
        "trade_date": date(2026, 8, 28),
        "source": "5m",
    }

    @contextmanager
    def get_cursor():
        yield cursor

    with patch.object(database, "get_db_cursor", get_cursor):
        result = database.get_latest_quote("aapl")

    sql, parameters = cursor.execute.call_args.args
    assert "equity_bar_revisions" in sql
    assert "session_scope = 'RTH'" in sql
    assert "is_final = TRUE" in sql
    assert "stock_prices_" not in sql
    assert parameters == ("AAPL", "AAPL")
    assert result["ticker"] == "AAPL"
    assert result["change"] == 1.0
    assert result["change_percent"] == 1.0