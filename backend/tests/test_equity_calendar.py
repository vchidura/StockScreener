from datetime import datetime, timezone
from pathlib import Path
import sys


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.calendar import latest_expected_market_time


UTC = timezone.utc


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