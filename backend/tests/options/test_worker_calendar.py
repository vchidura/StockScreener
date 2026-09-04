import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.calendar import OptionExchangeCalendar


UTC = timezone.utc


def test_delayed_slot_waits_for_first_complete_observable_interval():
    calendar = OptionExchangeCalendar()

    assert calendar.latest_delayed_slot(
        datetime(2026, 8, 31, 13, 59, tzinfo=UTC),
        publication_grace=timedelta(0),
    ) is None
    assert calendar.latest_delayed_slot(
        datetime(2026, 8, 31, 14, 0, tzinfo=UTC),
        publication_grace=timedelta(0),
    ) == datetime(2026, 8, 31, 13, 45, tzinfo=UTC)


def test_delayed_slot_is_open_anchored_and_stops_at_close():
    calendar = OptionExchangeCalendar()

    assert calendar.latest_delayed_slot(
        datetime(2026, 8, 31, 15, 7, tzinfo=UTC),
        publication_grace=timedelta(0),
    ) == datetime(2026, 8, 31, 14, 45, tzinfo=UTC)
    assert calendar.latest_delayed_slot(
        datetime(2026, 8, 31, 20, 30, tzinfo=UTC),
        publication_grace=timedelta(0),
    ) == datetime(2026, 8, 31, 20, 0, tzinfo=UTC)


def test_delayed_slot_skips_non_sessions_and_validates_slot_bounds():
    calendar = OptionExchangeCalendar()

    assert calendar.latest_delayed_slot(
        datetime(2026, 8, 30, 16, 0, tzinfo=UTC)
    ) is None
    with pytest.raises(ValueError, match="follow the session open"):
        calendar.session_for_slot(datetime(2026, 8, 31, 13, 30, tzinfo=UTC))