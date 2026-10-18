from datetime import datetime, timedelta, timezone

from scripts.report_realtime_v2_nine_ticker_status import (
    coalesced_missing_ranges,
    expected_starts,
)


UTC = timezone.utc
OPEN = datetime(2026, 10, 16, 13, 30, tzinfo=UTC)
CLOSE = datetime(2026, 10, 16, 20, 0, tzinfo=UTC)


def test_expected_starts_use_exact_closed_membership():
    one_minute = expected_starts(OPEN, CLOSE, 1)
    five_minute = expected_starts(OPEN, CLOSE, 5)

    assert len(one_minute) == 390
    assert one_minute[0] == OPEN
    assert one_minute[-1] == CLOSE - timedelta(minutes=1)
    assert len(five_minute) == 78
    assert five_minute[-1] == CLOSE - timedelta(minutes=5)


def test_missing_ranges_are_coalesced_without_inventing_members():
    expected = expected_starts(OPEN, OPEN + timedelta(minutes=25), 5)
    observed = {expected[0], expected[3]}

    assert coalesced_missing_ranges(expected, observed, 5) == (
        (expected[1], expected[3]),
        (expected[4], OPEN + timedelta(minutes=25)),
    )
