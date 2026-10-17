from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.advanced_gap_repair import (
    AdvancedGapRepairPlan,
    AdvancedHistoricalMinuteExecutor,
    GapContinuity,
    GapReconnectEvidence,
    classify_gap_continuity,
    plan_advanced_cold_start_cursor,
    publish_cohort_local_backfill_release_cursor,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 17, 14, 30, tzinfo=UTC)
GAP = UUID("11111111-1111-1111-1111-111111111111")
PLAN = UUID("22222222-2222-2222-2222-222222222222")
PRIOR = UUID("33333333-3333-3333-3333-333333333333")
RECONNECT = UUID("44444444-4444-4444-4444-444444444444")


def _evidence(**changes):
    values = {
        "gap_id": GAP,
        "prior_journal_generation": PRIOR,
        "reconnect_journal_generation": RECONNECT,
        "disconnect_at": NOW,
        "reconnect_at": NOW + timedelta(minutes=2),
        "last_durable_offset": 10,
        "first_post_reconnect_offset": 1,
        "last_durable_watermark": NOW - timedelta(minutes=1),
        "first_post_reconnect_event_at": NOW + timedelta(minutes=2),
        "no_trade_evidence": False,
    }
    values.update(changes)
    return GapReconnectEvidence(**values)


def _repair(*, status="PLANNED"):
    return AdvancedGapRepairPlan(
        repair_id=UUID("55555555-5555-5555-5555-555555555555"),
        gap_id=GAP,
        plan_id=PLAN,
        allocation_kind="LIVE_GAP",
        instrument_id=1,
        ticker="SPY",
        requested_from=NOW,
        requested_to=NOW + timedelta(minutes=5),
        scope_sha256="a" * 64,
        reserved_response_bytes=10_000,
        status=status,
    )


def test_reconnect_does_not_close_gap_and_classifies_provider_time_outage():
    classification, digest = classify_gap_continuity(_evidence())

    assert classification is GapContinuity.PROVIDER_TIME_OUTAGE
    assert len(digest) == 64


def test_local_offset_and_no_trade_evidence_are_distinct():
    local, _digest = classify_gap_continuity(
        _evidence(
            reconnect_journal_generation=PRIOR,
            first_post_reconnect_offset=14,
        )
    )
    no_trade, _digest = classify_gap_continuity(
        _evidence(no_trade_evidence=True)
    )

    assert local is GapContinuity.LOCAL_OFFSET_DISCONTINUITY
    assert no_trade is GapContinuity.NO_TRADE_INTERVAL


def test_unauthorized_historical_executor_performs_zero_provider_io():
    transport = MagicMock()
    parser = MagicMock()
    persister = MagicMock()
    deriver = MagicMock()
    executor = AdvancedHistoricalMinuteExecutor(
        transport,
        parser,
        persister,
        deriver,
    )

    result = executor.execute(
        _repair(status="BLOCKED"),
        sealed_body=None,
        seal=MagicMock(),
    )

    assert result.provider_requests == 0
    transport.fetch.assert_not_called()
    parser.assert_not_called()
    persister.assert_not_called()
    deriver.assert_not_called()


def test_executor_reserves_then_seals_before_persisting_and_derives_raw_only():
    order = []
    transport = MagicMock(
        fetch=lambda _repair: order.append("fetch") or b"sealed"
    )
    parser = MagicMock(
        side_effect=lambda _body, _repair: order.append("parse") or (1, 2)
    )
    persister = MagicMock(
        side_effect=lambda facts: order.append("persist") or len(facts)
    )
    deriver = MagicMock(
        side_effect=lambda facts, intervals, basis: order.append(
            (intervals, basis)
        )
    )
    executor = AdvancedHistoricalMinuteExecutor(
        transport,
        parser,
        persister,
        deriver,
    )

    result = executor.execute(
        _repair(),
        sealed_body=None,
        seal=lambda _body: order.append("seal"),
    )

    assert order[:4] == ["fetch", "seal", "parse", "persist"]
    assert order[4] == (
        ("5m", "15m", "30m", "1h", "1d"),
        "RAW_UNADJUSTED",
    )
    assert result.provider_requests == 1
    assert result.persisted_raw_1m == 2
    assert result.adjustment_basis == "RAW_UNADJUSTED"


def test_sealed_payload_replay_is_provider_free():
    transport = MagicMock()
    executor = AdvancedHistoricalMinuteExecutor(
        transport,
        lambda _body, _repair: (1,),
        lambda facts: len(facts),
        lambda _facts, _intervals, _basis: 1,
    )

    result = executor.execute(
        _repair(status="SEALED"),
        sealed_body=b"sealed",
        seal=MagicMock(),
    )

    assert result.provider_requests == 0
    transport.fetch.assert_not_called()


def test_cold_start_plans_only_the_eight_missing_target9_members(monkeypatch):
    cursor = MagicMock()
    cohort = UUID("66666666-6666-6666-6666-666666666666")
    cursor.fetchone.side_effect = (
        (cohort, 9, 8, 80_000),
        ("BACKFILL_PLANNED", "MISSING_RAW_1M", "RESERVE_MISSING_RAW_1M"),
    )
    missing = tuple(
        {
            "instrument_id": instrument_id,
            "ticker": f"T{instrument_id}",
            "requested_from": NOW,
            "requested_to": NOW + timedelta(hours=6, minutes=30),
            "missing_members": 390,
        }
        for instrument_id in range(2, 10)
    )
    monkeypatch.setattr(
        "realtime.advanced_gap_repair.plan_gap_repair_cursor",
        lambda *_args, **_kwargs: {
            "plans": (
                {
                    "session_date": NOW.date(),
                    "interval": "1m",
                    "missing_ranges": missing,
                },
                {
                    "session_date": NOW.date(),
                    "interval": "5m",
                    "missing_ranges": (),
                },
            )
        },
    )

    plan = plan_advanced_cold_start_cursor(
        cursor,
        plan_id=PLAN,
        sessions=(NOW.date(),),
        known_by=NOW,
        request_ceiling=8,
        decoded_byte_ceiling=80_000,
        expected_cost="approved",
    )

    assert len(plan.work) == 8
    assert {item.ticker for item in plan.work} == {
        "T2", "T3", "T4", "T5", "T6", "T7", "T8", "T9"
    }
    assert all(item.ticker != "SPY" for item in plan.work)
    assert plan.state == "BACKFILL_PLANNED"


def test_cold_start_refuses_more_than_two_completed_sessions():
    with pytest.raises(ValueError, match="scope is invalid"):
        plan_advanced_cold_start_cursor(
            MagicMock(),
            plan_id=PLAN,
            sessions=(
                NOW.date(),
                (NOW + timedelta(days=1)).date(),
                (NOW + timedelta(days=2)).date(),
            ),
            known_by=NOW,
            request_ceiling=24,
            decoded_byte_ceiling=240_000,
            expected_cost="approved",
        )


def test_two_session_cold_start_has_exact_sixteen_request_scopes(
    monkeypatch,
):
    cursor = MagicMock()
    cohort = UUID("77777777-7777-7777-7777-777777777777")
    cursor.fetchone.side_effect = (
        (cohort, 9, 16, 160_000),
        ("BACKFILL_PLANNED", "MISSING_RAW_1M", "RESERVE_MISSING_RAW_1M"),
    )
    sessions = (NOW.date(), (NOW + timedelta(days=1)).date())
    plans = []
    for session_date in sessions:
        missing = tuple(
            {
                "instrument_id": instrument_id,
                "ticker": f"T{instrument_id}",
                "requested_from": NOW,
                "requested_to": NOW + timedelta(hours=6, minutes=30),
                "missing_members": 390,
            }
            for instrument_id in range(2, 10)
        )
        plans.extend((
            {
                "session_date": session_date,
                "interval": "1m",
                "missing_ranges": missing,
            },
            {
                "session_date": session_date,
                "interval": "5m",
                "missing_ranges": (),
            },
        ))
    monkeypatch.setattr(
        "realtime.advanced_gap_repair.plan_gap_repair_cursor",
        lambda *_args, **_kwargs: {"plans": tuple(plans)},
    )

    plan = plan_advanced_cold_start_cursor(
        cursor,
        plan_id=PLAN,
        sessions=sessions,
        known_by=NOW,
        request_ceiling=16,
        decoded_byte_ceiling=160_000,
        expected_cost="approved",
    )

    assert len(plan.work) == 16
    assert plan.request_ceiling == 16


def test_backfill_release_is_cohort_local_and_enters_wait_next_session():
    cursor = MagicMock()
    cohort = UUID("88888888-8888-8888-8888-888888888888")
    backfill = UUID("99999999-9999-9999-9999-999999999999")
    cursor.fetchone.side_effect = (
        (cohort, "BACKFILL_COMPLETE"),
        (0,),
        (backfill,),
    )

    release_id = publish_cohort_local_backfill_release_cursor(
        cursor,
        backfill,
        coverage_sha256="b" * 64,
        job_barrier_sha256="c" * 64,
        published_at=NOW,
    )

    assert release_id
    inserts = [
        call for call in cursor.execute.call_args_list
        if "INSERT INTO rt.advanced_backfill_releases" in call.args[0]
    ]
    assert len(inserts) == 1
    assert "platform_pointer_advanced" in inserts[0].args[0]
    updates = [
        call for call in cursor.execute.call_args_list
        if "UPDATE rt.advanced_backfill_runs SET state" in call.args[0]
    ]
    assert updates[-1].args[1][0] == "WAIT_NEXT_SESSION"
