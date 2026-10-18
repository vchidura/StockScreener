from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.advanced_scheduler import (
    ADVANCED_WINDOW_BUILD_ID,
    bind_advanced_input_gate_build_id,
    discover_advanced_window_recovery_cursor,
    ensure_advanced_window_retry_cursor,
    order_advanced_due_windows,
)
from realtime.orchestration_repository import CycleReceiptSpec
from realtime.session_windows import advanced_session_windows


UTC = timezone.utc
COHORT = UUID("11111111-1111-1111-1111-111111111111")
SCHEDULER = UUID("22222222-2222-2222-2222-222222222222")
CUTOFF = datetime(2026, 10, 16, 14, 0, tzinfo=UTC)
NOW = CUTOFF + timedelta(minutes=10)


def _recovery(rows):
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    return discover_advanced_window_recovery_cursor(
        cursor,
        cohort_id=COHORT,
        cutoff=CUTOFF,
        checked_at=NOW,
    )


def test_database_state_not_report_files_controls_completion_and_retry():
    failed_window = UUID("33333333-3333-3333-3333-333333333333")
    receipt = UUID("44444444-4444-4444-4444-444444444444")
    recovery = _recovery((
        (
            failed_window,
            "advanced-20261016T140000Z",
            "FAILED",
            None,
            NOW - timedelta(minutes=6),
            "INPUT_READY",
            CUTOFF,
            1,
            "a" * 64,
            "b" * 64,
            bind_advanced_input_gate_build_id(
                ADVANCED_WINDOW_BUILD_ID,
                "c" * 64,
            ),
            COHORT,
            "a" * 64,
            "stages_v3",
            COHORT,
            NOW + timedelta(minutes=5),
        ),
    ))

    assert not recovery.complete
    assert recovery.next_attempt == 1
    assert recovery.retry_not_before <= NOW
    assert recovery.input_receipt == CycleReceiptSpec(
        "INPUT_READY",
        CUTOFF,
        1,
        "a" * 64,
        "b" * 64,
        bind_advanced_input_gate_build_id(
            ADVANCED_WINDOW_BUILD_ID,
            "c" * 64,
        ),
    )
    assert recovery.input_known_by == NOW + timedelta(minutes=5)
    assert recovery.input_gate_sha256 == "c" * 64


def test_complete_durable_window_is_never_retried():
    recovery = _recovery((
        (
            UUID("55555555-5555-5555-5555-555555555555"),
            "advanced-20261016T140000Z",
            "COMPLETE",
            None,
            NOW,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "a" * 64,
            "stages_v3",
            COHORT,
            NOW + timedelta(minutes=5),
        ),
    ))

    assert recovery.complete


def test_retry_identity_derives_report_path_and_receipt_reuse():
    cursor = MagicMock()
    input_receipt = CycleReceiptSpec(
        "INPUT_READY",
        CUTOFF,
        1,
        "a" * 64,
        "b" * 64,
        ADVANCED_WINDOW_BUILD_ID,
    )

    retry = ensure_advanced_window_retry_cursor(
        cursor,
        SCHEDULER,
        cohort_id=COHORT,
        cutoff=CUTOFF,
        attempt=2,
        retry_not_before=NOW,
        input_receipt=input_receipt,
        portal_receipt=None,
    )

    assert retry.window_key.endswith("-retry2")
    assert retry.report_relative_path.endswith("-retry2.json")
    assert retry.reused_input_receipt
    assert not retry.reused_portal_receipt


def test_latest_slot_runs_first_and_oldest_retry_is_bounded_second():
    windows = tuple(
        SimpleNamespace(cutoff=CUTOFF + timedelta(minutes=offset))
        for offset in (5, 15, 10, 0)
    )

    ordered = order_advanced_due_windows(windows)

    assert [row.cutoff for row in ordered] == [
        CUTOFF + timedelta(minutes=15),
        CUTOFF,
        CUTOFF + timedelta(minutes=10),
        CUTOFF + timedelta(minutes=5),
    ]


def test_early_close_dst_and_holiday_follow_xnys_calendar():
    half_day = advanced_session_windows(
        date(2026, 11, 27),
        finality_delay_seconds=5,
        completion_budget_seconds=120,
        serving_lag_seconds=30,
    )
    winter = advanced_session_windows(
        date(2026, 1, 5),
        finality_delay_seconds=5,
        completion_budget_seconds=120,
        serving_lag_seconds=30,
    )
    summer = advanced_session_windows(
        date(2026, 7, 6),
        finality_delay_seconds=5,
        completion_budget_seconds=120,
        serving_lag_seconds=30,
    )

    assert len(half_day) == 42
    assert winter[0].cutoff.hour == 14
    assert summer[0].cutoff.hour == 13
    with pytest.raises(ValueError):
        advanced_session_windows(
            date(2026, 7, 3),
            finality_delay_seconds=5,
            completion_budget_seconds=120,
            serving_lag_seconds=30,
        )
