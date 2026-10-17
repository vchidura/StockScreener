from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import UUID

import pytest

from realtime.advanced_stream_control import (
    AdvancedStreamScope,
    AdvancedStreamUsage,
    record_advanced_stream_gap_cursor,
    register_advanced_stream_session_cursor,
    update_advanced_stream_session_cursor,
)
from realtime.contracts import MarketFeed
from realtime.normalizer import NORMALIZER_VERSION


NOW = datetime(2026, 10, 16, 16, 0, tzinfo=timezone.utc)
COHORT = UUID("11111111-1111-1111-1111-111111111111")
MANIFEST = UUID("22222222-2222-2222-2222-222222222222")
GENERATION = UUID("33333333-3333-3333-3333-333333333333")
OWNER = UUID("44444444-4444-4444-4444-444444444444")
PLAN = UUID("55555555-5555-5555-5555-555555555555")


def _scope():
    return AdvancedStreamScope(
        ingress_plan_id=PLAN,
        cohort_id=COHORT,
        manifest_id=MANIFEST,
        journal_generation=GENERATION,
        owner_id=OWNER,
        feed=MarketFeed.STOCKS,
        normalizer_version=NORMALIZER_VERSION,
        authorization_reference="advanced-equity-v2-20261016",
        connection_attempt=1,
        connection_ceiling=1,
        decoded_byte_ceiling=10 * 1024 * 1024,
        stale_after_seconds=60,
        started_at=NOW,
    )


def test_registration_requires_exact_advanced_am_manifest_and_allocates_epoch():
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        ("MASSIVE_ADVANCED", MANIFEST, 1, 1),
        None,
        (7,),
    )

    with patch(
        "realtime.advanced_stream_control."
        "admit_advanced_ingress_session_cursor"
    ) as admit:
        session = register_advanced_stream_session_cursor(cursor, _scope())

    assert session.owner_epoch == 7
    assert session.journal_generation == GENERATION
    assert admit.call_args.kwargs["plan_id"] == PLAN
    insert = cursor.execute.call_args_list[-1]
    assert "admit_advanced" not in insert.args[0]
    stream_insert = next(
        call
        for call in cursor.execute.call_args_list
        if "INSERT INTO rt.stream_sessions" in call.args[0]
    )
    assert _scope().budget_scope_sha256 in stream_insert.args[1]
    assert str(PLAN) in stream_insert.args[1]


def test_registration_refuses_an_existing_feed_owner():
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        ("MASSIVE_ADVANCED", MANIFEST, 1, 1),
        (UUID(int=9), OWNER, 6, GENERATION, NOW),
    )

    with patch(
        "realtime.advanced_stream_control."
        "admit_advanced_ingress_session_cursor"
    ):
        with pytest.raises(ValueError, match="already active"):
            register_advanced_stream_session_cursor(cursor, _scope())


def test_registration_reclaims_only_an_exact_stale_owner():
    cursor = MagicMock()
    stale = datetime(2026, 10, 16, 15, 58, tzinfo=timezone.utc)
    cursor.fetchone.side_effect = (
        ("MASSIVE_ADVANCED", MANIFEST, 1, 1),
        (UUID(int=9), OWNER, 6, GENERATION, stale),
        (UUID(int=9),),
        (7,),
    )

    with patch(
        "realtime.advanced_stream_control."
        "admit_advanced_ingress_session_cursor"
    ):
        session = register_advanced_stream_session_cursor(cursor, _scope())

    assert session.owner_epoch == 7
    reclaim = next(
        call
        for call in cursor.execute.call_args_list
        if "STALE_OWNER_RECLAIMED" in call.args[0]
    )
    assert "owner_epoch=%s" in reclaim.args[0]
    assert "heartbeat_at=%s" in reclaim.args[0]


def test_session_usage_is_fenced_and_gap_identity_is_replay_safe():
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        ("MASSIVE_ADVANCED", MANIFEST, 1, 1),
        None,
        (1,),
        (PLAN, 0, 0, 0, 0, 0, 0),
        (UUID(int=8),),
    )
    with patch(
        "realtime.advanced_stream_control."
        "admit_advanced_ingress_session_cursor"
    ), patch(
        "realtime.advanced_stream_control."
        "record_advanced_ingress_usage_cursor"
    ) as record_usage:
        session = register_advanced_stream_session_cursor(cursor, _scope())
        usage = AdvancedStreamUsage(4, 500, 2, 300, 2, 2, NOW)

        update_advanced_stream_session_cursor(
            cursor,
            session,
            usage,
            status="STOPPED",
            checked_at=NOW,
            terminal=True,
        )
    assert record_usage.call_args.kwargs["decoded_bytes"] == 500
    first = record_advanced_stream_gap_cursor(
        cursor,
        session,
        gap_start=NOW,
        gap_end=None,
        last_durable_offset=2,
        reason="disconnect",
    )
    repeated = record_advanced_stream_gap_cursor(
        cursor,
        session,
        gap_start=NOW,
        gap_end=None,
        last_durable_offset=2,
        reason="disconnect",
    )

    assert first == repeated
    assert "ON CONFLICT (gap_id) DO NOTHING" in cursor.execute.call_args.args[0]


def test_usage_refuses_market_bytes_above_all_frame_bytes():
    with pytest.raises(ValueError, match="all-frame"):
        AdvancedStreamUsage(1, 10, 1, 11, 0, 0, None)
