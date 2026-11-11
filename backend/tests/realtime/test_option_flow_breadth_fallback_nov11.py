"""R48 (Nov 11, user): Options Flow showed 8 streamed contracts after one failed breadth capture (SPY 18:13 ConnectTimeout,
TSLA PARTIAL) and said nothing. A failed capture writes no rows over the last good ones, so the read now serves the last
COMPLETE capture and names the later one; when the selected ticker has no COMPLETE capture the page is told it fell back."""
from datetime import date, datetime, timedelta, timezone

from realtime import advanced_options_breadth as breadth
from realtime.option_flow_breadth import BREADTH_FLOW_SOURCE, breadth_flow_data
from tests.realtime.test_option_flow_breadth import COMPLETE, ROWS, _data

UTC = timezone.utc
GOOD = datetime(2026, 11, 11, 18, 8, tzinfo=UTC)
FAILED = GOOD + timedelta(minutes=5)


class _Cursor:
    """rt.option_breadth_current then the two capture queries (latest of any state, latest COMPLETE)."""

    def __init__(self, newest, complete):
        self.newest, self.complete, self.sql = newest, complete, ""
        self.description = [("x",)]

    def execute(self, sql, *_args):
        self.sql = sql

    def fetchall(self):
        if "option_breadth_current" in self.sql:
            return []
        return [dict(row) for row in (self.complete if "state='COMPLETE'" in self.sql else self.newest)]


def capture(underlying, state, at, contracts, reason=None):
    return dict(underlying=underlying, state=state, observed_at=at, contracts=contracts, reason=reason)


def test_a_failed_latest_capture_serves_the_last_complete_one_and_names_the_failure():
    cursor = _Cursor(
        newest=[capture("SPY", "FAILED", FAILED, 0, "ConnectTimeout: ..."), capture("QQQ", "COMPLETE", GOOD, 2400)],
        complete=[capture("SPY", "COMPLETE", GOOD, 2450), capture("QQQ", "COMPLETE", GOOD, 2400)],
    )
    _, captures = breadth.load_breadth_screener_rows_cursor(
        cursor, session_date=date(2026, 11, 11), underlyings=("SPY", "QQQ"), available_by=FAILED)
    assert captures["SPY"]["breadth_state"] == "COMPLETE" and captures["SPY"]["breadth_contracts"] == 2450
    assert captures["SPY"]["breadth_observed_at"] == GOOD.isoformat()
    assert (captures["SPY"]["latest_capture_state"], captures["SPY"]["latest_capture_at"]) == ("FAILED", FAILED.isoformat())
    assert captures["SPY"]["latest_capture_reason"].startswith("ConnectTimeout")
    assert "latest_capture_state" not in captures["QQQ"]


def test_no_complete_capture_keeps_the_latest_one_as_before():
    cursor = _Cursor(newest=[capture("TSLA", "PARTIAL", FAILED, 900, "DEADLINE")], complete=[])
    _, captures = breadth.load_breadth_screener_rows_cursor(
        cursor, session_date=date(2026, 11, 11), underlyings=("TSLA",), available_by=FAILED)
    assert captures["TSLA"]["breadth_state"] == "PARTIAL" and "latest_capture_state" not in captures["TSLA"]


def test_the_flow_page_is_told_which_tickers_fell_back():
    stale = {**COMPLETE, "latest_capture_state": "FAILED", "latest_capture_at": FAILED.isoformat()}
    data = breadth_flow_data(_data(), ROWS, {"SPY": stale, "QQQ": {"breadth_state": "FAILED"}})
    assert data["coverage_source"] == BREADTH_FLOW_SOURCE  # the last good chain for SPY
    assert data["breadth"]["latest_capture_state"] == "FAILED"
    assert data["breadth_fallback"] == {"selected": False, "streamed_only": ["QQQ"]}
    complete = breadth_flow_data(_data(), ROWS, {"SPY": COMPLETE, "QQQ": COMPLETE})
    assert complete["breadth_fallback"] is None
