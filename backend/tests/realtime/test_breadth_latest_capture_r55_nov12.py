"""R55 (Nov 12 night): `rt.option_breadth_current` keeps rows of contracts that left the band in earlier captures.
`REALTIME_V2_BREADTH_LATEST_CAPTURE_ONLY=ON` reads only each underlying's latest capture; off (the default) reads every
current row exactly as before."""
from datetime import date, datetime, timezone

from realtime import advanced_options_breadth as breadth

AT = datetime(2026, 11, 12, 19, 7, tzinfo=timezone.utc)
SESSION = date(2026, 11, 12)


class _Cursor:
    def __init__(self):
        self.calls = []
        self.description = [("x",)]
        self._sql = ""

    def execute(self, sql, params=()):
        self.calls.append((sql, tuple(params)))
        self._sql = sql

    def fetchall(self):
        return []


def _breadth_query(cursor):
    return next((sql, params) for sql, params in cursor.calls if "SELECT breadth.*" in sql)


def test_off_by_default_reads_every_current_row(monkeypatch):
    monkeypatch.delenv(breadth.BREADTH_LATEST_CAPTURE_SWITCH, raising=False)
    monkeypatch.delenv(breadth.BREADTH_ROW_CACHE_SWITCH, raising=False)
    cursor = _Cursor()
    breadth.load_breadth_screener_rows_cursor(cursor, session_date=SESSION, underlyings=("SPY",), available_by=AT)
    sql, params = _breadth_query(cursor)
    assert "max(latest.observed_at)" not in sql and " IN (SELECT" not in sql and params == (SESSION, ["SPY"], AT)


def test_on_reads_only_the_latest_capture_per_underlying(monkeypatch):
    monkeypatch.setenv(breadth.BREADTH_LATEST_CAPTURE_SWITCH, "ON")
    monkeypatch.delenv(breadth.BREADTH_ROW_CACHE_SWITCH, raising=False)
    cursor = _Cursor()
    breadth.load_breadth_screener_rows_cursor(cursor, session_date=SESSION, underlyings=("SPY", "QQQ"), available_by=AT)
    sql, params = _breadth_query(cursor)
    assert "(breadth.underlying,breadth.observed_at) IN (SELECT latest.underlying,max(latest.observed_at)" in sql
    assert "GROUP BY latest.underlying" in sql and "latest.underlying=breadth.underlying" not in sql  # uncorrelated
    assert params == (SESSION, ["SPY", "QQQ"], AT, SESSION, ["SPY", "QQQ"], AT)
    assert sql.count("%s") == len(params)
