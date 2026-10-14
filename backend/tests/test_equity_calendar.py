from datetime import datetime, timezone
from pathlib import Path
import sys

import pytest


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.calendar import latest_expected_market_time, market_notification_window, sessions_elapsed


UTC = timezone.utc


@pytest.mark.parametrize("timestamp,enabled", [
    ("2026-10-09T13:29:59+00:00", False),
    ("2026-10-09T13:30:00+00:00", True),
    ("2026-10-09T19:59:59+00:00", True),
    ("2026-10-09T20:00:00+00:00", False),
    ("2026-10-10T15:00:00+00:00", False),
    ("2026-10-11T15:00:00+00:00", False),
    ("2026-07-03T15:00:00+00:00", False),
    ("2026-11-26T15:00:00+00:00", False),
    ("2026-11-27T17:59:59+00:00", True),
    ("2026-11-27T18:00:00+00:00", False),
    ("2026-11-02T14:29:59+00:00", False),
    ("2026-11-02T14:30:00+00:00", True),
])
def test_market_notifications_follow_exchange_hours_holidays_and_dst(timestamp, enabled):
    now = datetime.fromisoformat(timestamp)
    window = market_notification_window(now)
    assert window["notifications_enabled"] is enabled
    assert window["checked_at"] == now.isoformat()
    assert window["calendar"] == "XNYS"
    if enabled:
        assert datetime.fromisoformat(window["session_open"]) <= now < datetime.fromisoformat(window["session_close"])


def test_market_notifications_use_exchange_date_and_reject_naive_clocks():
    window = market_notification_window(datetime(2026, 10, 10, 0, 30, tzinfo=UTC))
    assert window["session"] == "2026-10-09"
    assert window["is_trading_session"] is True
    assert window["notifications_enabled"] is False
    with pytest.raises(ValueError, match="timezone-aware"):
        market_notification_window(datetime(2026, 10, 9, 15))


def test_canonical_notification_window_uses_the_same_calendar_without_database_import(monkeypatch):
    from equity import api
    now = datetime(2026, 11, 26, 15, tzinfo=UTC)

    class FixedClock:
        @staticmethod
        def now(zone):
            return now.astimezone(zone)

    monkeypatch.setattr(api, "datetime", FixedClock)
    assert api.equity_notification_window() == market_notification_window(now)
    assert any(route.path == "/api/equity/notification-window" for route in api.router.routes)


def test_expected_market_time_tracks_completed_normal_session_slots():
    assert latest_expected_market_time(
        datetime(2026, 8, 28, 13, 59, tzinfo=UTC), "30m"
    ) == datetime(2026, 8, 27, 20, 0, tzinfo=UTC)
    assert latest_expected_market_time(
        datetime(2026, 8, 28, 14, 0, tzinfo=UTC), "30m"
    ) == datetime(2026, 8, 28, 14, 0, tzinfo=UTC)
    assert latest_expected_market_time(
        datetime(2026, 8, 28, 20, 30, tzinfo=UTC), "30m"
    ) == datetime(2026, 8, 28, 20, 0, tzinfo=UTC)


def test_expected_market_time_uses_prior_close_on_weekend_and_preopen():
    assert latest_expected_market_time(
        datetime(2026, 8, 30, 20, 0, tzinfo=UTC), "30m"
    ) == datetime(2026, 8, 28, 20, 0, tzinfo=UTC)
    assert latest_expected_market_time(
        datetime(2026, 8, 31, 12, 0, tzinfo=UTC), "30m"
    ) == datetime(2026, 8, 28, 20, 0, tzinfo=UTC)


@pytest.mark.parametrize("timestamp,expected", [
    ("2026-07-03T15:00:00+00:00", "2026-07-02T20:00:00+00:00"),
    ("2026-11-26T15:00:00+00:00", "2026-11-25T21:00:00+00:00"),
    ("2026-10-11T15:00:00+00:00", "2026-10-09T20:00:00+00:00"),
])
def test_closed_days_do_not_advance_freshness_validation(timestamp, expected):
    now, prior_close = datetime.fromisoformat(timestamp), datetime.fromisoformat(expected)
    for interval in ("1d", "1h", "30m"):
        assert latest_expected_market_time(now, interval) == prior_close
        assert sessions_elapsed(prior_close, now) == 0


def test_expected_market_time_caps_at_early_close():
    assert latest_expected_market_time(
        datetime(2026, 11, 27, 20, 0, tzinfo=UTC), "30m"
    ) == datetime(2026, 11, 27, 18, 0, tzinfo=UTC)


def test_expected_hourly_time_includes_session_close_partial():
    assert latest_expected_market_time(
        datetime(2026, 8, 28, 19, 45, tzinfo=UTC), "1h"
    ) == datetime(2026, 8, 28, 19, 30, tzinfo=UTC)
    assert latest_expected_market_time(
        datetime(2026, 8, 28, 20, 0, tzinfo=UTC), "1h"
    ) == datetime(2026, 8, 28, 20, 0, tzinfo=UTC)


def test_expected_daily_and_weekly_times_require_completed_closes():
    assert latest_expected_market_time(
        datetime(2026, 8, 28, 19, 0, tzinfo=UTC), "1d"
    ) == datetime(2026, 8, 27, 20, 0, tzinfo=UTC)
    assert latest_expected_market_time(
        datetime(2026, 8, 28, 20, 0, tzinfo=UTC), "1d"
    ) == datetime(2026, 8, 28, 20, 0, tzinfo=UTC)
    assert latest_expected_market_time(
        datetime(2026, 8, 27, 21, 0, tzinfo=UTC), "1wk"
    ) == datetime(2026, 8, 21, 20, 0, tzinfo=UTC)
    assert latest_expected_market_time(
        datetime(2026, 8, 28, 20, 0, tzinfo=UTC), "1wk"
    ) == datetime(2026, 8, 28, 20, 0, tzinfo=UTC)


def test_expected_monthly_time_requires_final_exchange_session_close():
    assert latest_expected_market_time(
        datetime(2026, 8, 30, 20, 0, tzinfo=UTC), "1mo"
    ) == datetime(2026, 7, 31, 20, 0, tzinfo=UTC)
    assert latest_expected_market_time(
        datetime(2026, 8, 31, 19, 59, tzinfo=UTC), "1mo"
    ) == datetime(2026, 7, 31, 20, 0, tzinfo=UTC)
    assert latest_expected_market_time(
        datetime(2026, 8, 31, 20, 0, tzinfo=UTC), "1mo"
    ) == datetime(2026, 8, 31, 20, 0, tzinfo=UTC)