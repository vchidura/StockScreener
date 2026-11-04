from contextlib import nullcontext
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.portal_compatibility_repository import action_evidence_until
from realtime.ticker_repository import (
    PATTERN_WATCH_PUBLICATION_VERSION,
    TICKER_WORKSPACE_SUMMARY_VERSION,
    RealtimeTickerWorkspaceRepository,
    _latest_closing_session,
)


UTC = timezone.utc
CLOSE = datetime(2026, 11, 2, 21, 0, tzinfo=UTC)
SNAPSHOT_ID = UUID(int=2)


@pytest.mark.parametrize(
    ("generated_at", "read_at", "read_mode", "expected"),
    (
        # A live window still rejects corporate actions recorded after it.
        (CLOSE - timedelta(minutes=30), CLOSE - timedelta(minutes=5), "current", CLOSE - timedelta(minutes=5)),
        # Actions recorded before the close still count; the nightly refresh does not.
        (CLOSE - timedelta(hours=1), CLOSE + timedelta(hours=9), "current", CLOSE),
        # The closing publication has no remaining period at all.
        (CLOSE, CLOSE + timedelta(hours=9), "current", CLOSE),
        # Eastern time, the same instant.
        (CLOSE.astimezone(timezone(timedelta(hours=-5))), CLOSE + timedelta(hours=9), "current", CLOSE),
        # A half day closes three hours early.
        (
            datetime(2026, 11, 27, 18, 0, tzinfo=UTC),
            datetime(2026, 11, 28, 3, 0, tzinfo=UTC),
            "current",
            datetime(2026, 11, 27, 18, 0, tzinfo=UTC),
        ),
        # Retained reads keep seeing later actions, so they can warn about them.
        (CLOSE, CLOSE + timedelta(hours=9), "retained", CLOSE + timedelta(hours=9)),
        # Not a session.
        (
            datetime(2026, 11, 1, 21, 0, tzinfo=UTC),
            datetime(2026, 11, 2, 3, 0, tzinfo=UTC),
            "current",
            datetime(2026, 11, 2, 3, 0, tzinfo=UTC),
        ),
    ),
)
def test_actions_recorded_after_a_close_belong_to_the_next_session(
    generated_at,
    read_at,
    read_mode,
    expected,
):
    assert action_evidence_until(generated_at, read_at, read_mode) == expected


def test_stored_action_rows_after_the_close_fall_outside_the_query():
    # The reader's SQL asks for received_at in (generated_at, until].
    until = action_evidence_until(CLOSE, CLOSE + timedelta(hours=13))
    received = CLOSE + timedelta(hours=13, minutes=-1)

    assert not CLOSE < received <= until


def _closing_sessions(snapshot_type, payload, frozen=()):
    reader = RealtimeTickerWorkspaceRepository.__new__(
        RealtimeTickerWorkspaceRepository
    )
    cursor = MagicMock()
    cursor.fetchall.return_value = list(frozen)
    reader._cursor = lambda: nullcontext(cursor)
    reader._begin = lambda _cursor: None
    reader._latest_release_entries = lambda *_args: [
        {"snapshot_id": SNAPSHOT_ID}
    ]
    reader._summary_snapshot = lambda *_args, **_kwargs: {
        "payload": payload,
        "session_date": date(2026, 11, 2),
        "snapshot_id": SNAPSHOT_ID,
        "snapshot_class": "EPHEMERAL_PORTAL",
    }
    return reader._summary_closing_sessions(
        snapshot_type,
        available_by=CLOSE + timedelta(hours=8),
        cohort_id=UUID(int=1),
    )


def test_pattern_watch_sessions_read_fields_its_payload_lacks_from_elsewhere():
    result = _closing_sessions(
        PATTERN_WATCH_PUBLICATION_VERSION,
        {"views": {"1d": {"market_time": CLOSE.isoformat()}}},
    )

    assert result["current_session"] == "2026-11-02"
    assert result["sessions"] == ["2026-11-02"]
    assert result["data_as_of"] == CLOSE.isoformat()


def test_a_closed_session_selects_its_frozen_release_not_the_hot_one():
    # The hot release stays current after the close, and closing reads reject it.
    frozen_id = UUID(int=3)
    result = _closing_sessions(
        TICKER_WORKSPACE_SUMMARY_VERSION,
        {"session": "2026-11-02", "market_time": CLOSE.isoformat()},
        frozen=[{
            "session_date": date(2026, 11, 2),
            "snapshot_id": frozen_id,
            "generated_at": CLOSE,
        }],
    )

    assert result["snapshots"]["2026-11-02"] == str(frozen_id)
    assert result["current_snapshot"] == str(SNAPSHOT_ID)
    assert result["current_is_session_close"] is True
    assert _latest_closing_session(result) == "2026-11-02"


def test_an_open_session_is_not_offered_as_closed():
    result = _closing_sessions(
        TICKER_WORKSPACE_SUMMARY_VERSION,
        {"session": "2026-11-02", "market_time": "2026-11-02T20:55:00+00:00"},
    )

    assert result["snapshots"]["2026-11-02"] == str(SNAPSHOT_ID)
    assert result["current_is_session_close"] is False
    assert _latest_closing_session(result) is None


def test_workspace_summary_sessions_still_read_their_own_fields():
    result = _closing_sessions(
        TICKER_WORKSPACE_SUMMARY_VERSION,
        {"session": "2026-10-30", "market_time": "2026-10-30T20:00:00+00:00"},
    )

    assert result["current_session"] == "2026-10-30"
    assert result["data_as_of"] == "2026-10-30T20:00:00+00:00"
