from datetime import datetime, timedelta, timezone

from realtime.gap_repair_plan import _coalesce


def test_gap_ranges_coalesce_only_consecutive_members_per_instrument():
    start = datetime(2026, 10, 15, 13, 30, tzinfo=timezone.utc)

    ranges = _coalesce((
        (1, "AAPL", start),
        (1, "AAPL", start + timedelta(minutes=1)),
        (1, "AAPL", start + timedelta(minutes=3)),
        (2, "MSFT", start),
    ), width=timedelta(minutes=1))

    assert ranges == (
        {
            "instrument_id": 1,
            "ticker": "AAPL",
            "requested_from": start,
            "requested_to": start + timedelta(minutes=2),
            "missing_members": 2,
        },
        {
            "instrument_id": 1,
            "ticker": "AAPL",
            "requested_from": start + timedelta(minutes=3),
            "requested_to": start + timedelta(minutes=4),
            "missing_members": 1,
        },
        {
            "instrument_id": 2,
            "ticker": "MSFT",
            "requested_from": start,
            "requested_to": start + timedelta(minutes=1),
            "missing_members": 1,
        },
    )
