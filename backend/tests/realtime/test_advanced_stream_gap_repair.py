from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from equity.domain import BarSourceKind
from realtime.advanced_stream_gap_repair import (
    StreamGapMember,
    _repair_facts,
    first_missing_minute_range,
    repair_advanced_stream_hole,
)


OPEN = datetime(2026, 11, 2, 14, 30, tzinfo=timezone.utc)
MEMBERS = (1, 2)
HOLE_START = datetime(2026, 11, 2, 16, 35, tzinfo=timezone.utc)
HOLE_END = HOLE_START + timedelta(minutes=2)
MEMBER = StreamGapMember(7, UUID(int=8), "SPY")
SCOPE = SimpleNamespace(cohort_id=UUID(int=9), session_date=date(2026, 11, 2))


def _rest_row(minute):
    return {
        "t": int(minute.timestamp() * 1000),
        "o": 100.0,
        "h": 101.0,
        "l": 99.0,
        "c": 100.5,
        "v": 1000.0,
        "vw": 100.2,
        "n": 10,
    }


def _facts(*minutes):
    return _repair_facts(
        SCOPE,
        MEMBER,
        tuple(_rest_row(minute) for minute in minutes),
        request_id=UUID(int=10),
        hole_start=HOLE_START,
        hole_end=HOLE_END,
        observed_at=HOLE_END + timedelta(hours=1),
    )


def test_repair_keeps_only_the_hole_and_marks_the_rest_source():
    facts, segment = _facts(
        HOLE_START - timedelta(minutes=1),
        HOLE_START,
        HOLE_START + timedelta(minutes=1),
        HOLE_END,
    )

    assert [fact.revision.bar_start for fact in facts] == [
        HOLE_START,
        HOLE_START + timedelta(minutes=1),
    ]
    for fact in facts:
        # The input gate only accepts a REST bar carrying both of these.
        assert fact.revision.source_kind is BarSourceKind.NATIVE_REST
        assert "EXPLICIT_ADVANCED_REST_REPAIR_V1" in fact.revision.quality_codes
        assert fact.revision.ingestion_segment_id == segment
        assert fact.instrument_id == MEMBER.instrument_id
        assert fact.cohort_id == SCOPE.cohort_id


def test_repair_refuses_a_partial_answer_from_the_provider():
    with pytest.raises(ValueError, match="every missing minute"):
        _facts(HOLE_START)


def test_repair_identity_is_stable_for_one_request():
    first, _ = _facts(HOLE_START, HOLE_START + timedelta(minutes=1))
    second, _ = _facts(HOLE_START, HOLE_START + timedelta(minutes=1))

    assert [fact.revision.bar_revision_id for fact in first] == [
        fact.revision.bar_revision_id for fact in second
    ]


@pytest.mark.parametrize(
    ("start", "end", "limit"),
    (
        (HOLE_END, HOLE_START, 1),
        (HOLE_START + timedelta(seconds=1), HOLE_END, 1),
        (HOLE_START, HOLE_END + timedelta(seconds=1), 1),
        (HOLE_START, HOLE_END, 0),
    ),
)
def test_invalid_repair_scope_reaches_neither_database_nor_provider(
    start,
    end,
    limit,
):
    provider = MagicMock()
    client = MagicMock()

    with pytest.raises(ValueError, match="scope is invalid"):
        repair_advanced_stream_hole(
            provider,
            client,
            plan_id=UUID(int=1),
            hole_start=start,
            hole_end=end,
            response_byte_limit=limit,
        )

    provider.transaction.assert_not_called()
    client.fetch_native_bars.assert_not_called()


def _present(*minutes, members=MEMBERS):
    return {
        (member, OPEN + timedelta(minutes=minute))
        for minute in minutes
        for member in members
    }


def test_no_hole_when_every_member_has_every_minute():
    assert first_missing_minute_range(
        _present(0, 1, 2),
        instrument_ids=MEMBERS,
        start=OPEN,
        end=OPEN + timedelta(minutes=3),
    ) is None


def test_mid_session_hole_ends_where_all_members_resume():
    # 2026-11-02: minutes after a false drain were absent, then stored again.
    assert first_missing_minute_range(
        _present(0, 1, 5, 6),
        instrument_ids=MEMBERS,
        start=OPEN,
        end=OPEN + timedelta(minutes=7),
    ) == (OPEN + timedelta(minutes=2), OPEN + timedelta(minutes=5))


def test_hole_running_to_the_boundary_stops_at_the_boundary():
    assert first_missing_minute_range(
        _present(0),
        instrument_ids=MEMBERS,
        start=OPEN,
        end=OPEN + timedelta(minutes=4),
    ) == (OPEN + timedelta(minutes=1), OPEN + timedelta(minutes=4))


def test_one_missing_member_makes_the_minute_a_hole():
    present = _present(0, 2) | _present(1, members=(1,))
    assert first_missing_minute_range(
        present,
        instrument_ids=MEMBERS,
        start=OPEN,
        end=OPEN + timedelta(minutes=3),
    ) == (OPEN + timedelta(minutes=1), OPEN + timedelta(minutes=2))


def test_late_start_is_a_hole_from_the_open():
    assert first_missing_minute_range(
        _present(10, 11),
        instrument_ids=MEMBERS,
        start=OPEN,
        end=OPEN + timedelta(minutes=12),
    ) == (OPEN, OPEN + timedelta(minutes=10))


def test_only_the_earliest_hole_is_returned():
    assert first_missing_minute_range(
        _present(0, 2, 4),
        instrument_ids=MEMBERS,
        start=OPEN,
        end=OPEN + timedelta(minutes=5),
    ) == (OPEN + timedelta(minutes=1), OPEN + timedelta(minutes=2))
