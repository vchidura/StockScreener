"""R47 (Nov 12): the API process segfaulted in garbage collection while building the ~18,000 breadth rows of the Flow
read, once per page poll. With `REALTIME_V2_FLOW_BREADTH_CACHE=ON` the built rows are kept per process and reused while
the stored rows are unchanged; off (the default) reads exactly as before."""
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from realtime import advanced_options_breadth as breadth

UTC = timezone.utc
AT = datetime(2026, 11, 11, 18, 8, tzinfo=UTC)
SESSION = date(2026, 11, 11)


def _row(ticker, observed=AT):
    return {
        "session_date": SESSION, "contract_ticker": ticker, "underlying": "SPY", "contract_type": "call",
        "expiration_date": date(2026, 11, 20), "strike_price": Decimal("600"), "day_volume": 10,
        "open_interest": 100, "bid": Decimal("1.00"), "ask": Decimal("1.20"), "spot": Decimal("590"),
        "observed_at": observed, "contract_id": 7,
    }


class _Cursor:
    """Dict cursor over rt.option_breadth_current (rows and their identity) and the two capture queries."""

    def __init__(self, rows):
        self.rows, self.sql, self.statements = rows, "", []
        self.description = [("x",)]

    def execute(self, sql, *_args):
        self.sql = sql
        self.statements.append(sql)

    def fetchall(self):
        if "count(*) AS row_count" in self.sql:
            if not self.rows:
                return []
            observed = [row["observed_at"] for row in self.rows]
            return [{
                "underlying": "SPY", "row_count": len(self.rows), "catalog_count": len(self.rows),
                "first_observed": min(observed), "last_observed": max(observed),
                "observed_sum": sum(item.timestamp() for item in observed),
            }]
        if "option_breadth_current" in self.sql:
            return [dict(row) for row in self.rows]
        return [{"underlying": "SPY", "state": "COMPLETE", "observed_at": AT, "contracts": len(self.rows), "reason": None}]

    def builds(self):
        return sum("SELECT breadth.*" in sql for sql in self.statements)

    def identity_reads(self):
        return sum("count(*) AS row_count" in sql for sql in self.statements)


@pytest.fixture(autouse=True)
def _empty_cache():
    breadth._BREADTH_ROW_CACHE.clear()
    yield
    breadth._BREADTH_ROW_CACHE.clear()


def _read(cursor):
    return breadth.load_breadth_screener_rows_cursor(
        cursor, session_date=SESSION, underlyings=("SPY",), available_by=AT)


def test_switch_off_by_default_builds_on_every_read_and_reads_no_identity(monkeypatch):
    monkeypatch.delenv(breadth.BREADTH_ROW_CACHE_SWITCH, raising=False)
    cursor = _Cursor([_row("O:SPY261120C00600000")])
    first, second = _read(cursor), _read(cursor)
    assert first == second
    assert cursor.builds() == 2 and cursor.identity_reads() == 0
    assert not breadth._BREADTH_ROW_CACHE


def test_switch_on_builds_once_while_the_stored_rows_are_unchanged(monkeypatch):
    monkeypatch.setenv(breadth.BREADTH_ROW_CACHE_SWITCH, "ON")
    cursor = _Cursor([_row("O:SPY261120C00600000"), _row("O:SPY261120P00600000")])
    first = _read(cursor)
    second = _read(cursor)
    assert cursor.builds() == 1 and cursor.identity_reads() == 2
    assert first == second  # rows and capture labels
    assert first[0] is not second[0]  # each caller gets its own list


def test_switch_on_and_off_give_identical_results(monkeypatch):
    rows = [_row("O:SPY261120C00600000"), _row("O:SPY261120P00600000")]
    monkeypatch.delenv(breadth.BREADTH_ROW_CACHE_SWITCH, raising=False)
    off = _read(_Cursor(rows))
    monkeypatch.setenv(breadth.BREADTH_ROW_CACHE_SWITCH, "ON")
    cursor = _Cursor(rows)
    _read(cursor)
    assert _read(cursor) == off


def test_a_new_capture_rebuilds(monkeypatch):
    monkeypatch.setenv(breadth.BREADTH_ROW_CACHE_SWITCH, "ON")
    cursor = _Cursor([_row("O:SPY261120C00600000")])
    _read(cursor)
    later = AT.replace(minute=13)
    cursor.rows = [_row("O:SPY261120C00600000", later)]  # same count, newer observed_at
    rows, _ = _read(cursor)
    assert cursor.builds() == 2
    assert rows[0]["market_data_time"] == later.isoformat()


def test_the_cache_holds_a_bounded_number_of_builds(monkeypatch):
    monkeypatch.setenv(breadth.BREADTH_ROW_CACHE_SWITCH, "ON")
    cursor = _Cursor([_row("O:SPY261120C00600000")])
    for minute in range(5):
        cursor.rows = [_row("O:SPY261120C00600000", AT.replace(minute=minute))]
        _read(cursor)
    assert len(breadth._BREADTH_ROW_CACHE) == breadth._BREADTH_ROW_CACHE_SIZE


def test_tuple_cursor_identity(monkeypatch):
    class _TupleCursor(_Cursor):
        def fetchall(self):
            return [tuple(item.values()) if isinstance(item, dict) else item for item in super().fetchall()]

    monkeypatch.setenv(breadth.BREADTH_ROW_CACHE_SWITCH, "ON")
    cursor = _TupleCursor([_row("O:SPY261120C00600000")])
    identity = breadth._breadth_rows_identity(cursor, SESSION, ("SPY",), AT)
    assert identity[0][0] == "SPY" and identity[0][1] == "1"
