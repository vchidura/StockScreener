from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from uuid import UUID
from unittest.mock import MagicMock

import pytest

from realtime.advanced_equity_preparation import EquityPreparationStage
from realtime.advanced_equity_rollover import (
    EquityRolloverResult,
    EquityRolloverMember,
    EquityRolloverPlan,
    EquityRolloverWork,
    REFERENCE_ACTION,
    _rollover_claim,
    dividend_coverage_refresh_due,
)
from realtime.advanced_realtime_desired_state import DesiredMemberKind
from options.calendar import OptionExchangeCalendar
from realtime.equity_history import EquityHistoryRead, equity_history_coordinates
from realtime.equity_portal_history import build_realtime_equity_portal_history_plan
from realtime import advanced_equity_rollover as rollover
from scripts.run_realtime_v2_advanced_equity_rollover import _result_report


UTC = timezone.utc
TUESDAY_OPEN = datetime(2026, 11, 3, 14, 30, tzinfo=UTC)
MONDAY_OPEN = datetime(2026, 11, 9, 14, 30, tzinfo=UTC)
MEMBER = EquityRolloverMember(
    UUID(int=1),
    "SPY",
    DesiredMemberKind.EQUITY,
    UUID(int=2),
    7,
)
PLAN = EquityRolloverPlan(
    UUID(int=3),
    UUID(int=4),
    date(2026, 11, 3),
    date(2026, 11, 2),
    (MEMBER,),
    (),
)


def test_refresh_is_due_for_the_coverage_that_blocked_november_third():
    assert dividend_coverage_refresh_due(
        datetime(2026, 11, 2, 13, 8, 14, tzinfo=UTC),
        checked_at=datetime(2026, 11, 3, 3, 40, tzinfo=UTC),
        session_open=TUESDAY_OPEN,
    )


def test_a_fresh_refresh_is_not_repeated_on_the_next_poll():
    refreshed = datetime(2026, 11, 2, 21, 6, tzinfo=UTC)
    assert not dividend_coverage_refresh_due(
        refreshed,
        checked_at=refreshed + timedelta(seconds=15),
        session_open=TUESDAY_OPEN,
    )


def test_refresh_waits_when_it_would_expire_before_the_session():
    # Friday evening: anything fetched now would be stale by Monday's open.
    assert not dividend_coverage_refresh_due(
        datetime(2026, 11, 5, 21, 6, tzinfo=UTC),
        checked_at=datetime(2026, 11, 6, 21, 6, tzinfo=UTC),
        session_open=MONDAY_OPEN,
    )


def test_refresh_becomes_due_once_it_would_last_through_the_session_start():
    assert dividend_coverage_refresh_due(
        datetime(2026, 11, 6, 21, 6, tzinfo=UTC),
        checked_at=datetime(2026, 11, 8, 17, 0, tzinfo=UTC),
        session_open=MONDAY_OPEN,
    )


def test_missing_coverage_is_due():
    assert dividend_coverage_refresh_due(
        None,
        checked_at=datetime(2026, 11, 2, 21, 6, tzinfo=UTC),
        session_open=TUESDAY_OPEN,
    )


@pytest.mark.parametrize(
    ("action", "interval"),
    (("ADJUSTED_1D", "1d"), ("ADJUSTED_1WK", "1wk"), ("ADJUSTED_1MO", "1mo")),
)
def test_adjusted_claim_targets_exactly_the_missing_sessions(action, interval):
    checked = datetime(2026, 11, 2, 21, 6, tzinfo=UTC)
    claim = _rollover_claim(
        PLAN,
        EquityRolloverWork(MEMBER, action, (date(2026, 11, 2),)),
        checked,
        1,
        UUID(int=5),
    )

    assert claim.stage is EquityPreparationStage.ADJUSTED_DAILY
    assert claim.target["interval_sessions"] == {interval: ["2026-11-02"]}
    assert claim.source_cohort_id == PLAN.source_cohort_id
    assert claim.instrument_id == MEMBER.instrument_id


def test_reference_claim_uses_the_completed_session_as_its_as_of():
    claim = _rollover_claim(
        PLAN,
        EquityRolloverWork(MEMBER, REFERENCE_ACTION),
        datetime(2026, 11, 2, 21, 6, tzinfo=UTC),
        1,
        UUID(int=5),
    )

    assert claim.stage is EquityPreparationStage.REFERENCE
    assert claim.target["history_through_session"] == "2026-11-02"
    assert "interval_sessions" not in claim.target


def test_rollover_report_keeps_persisted_retry_time_visible():
    checked = datetime(2026, 11, 3, 3, 40, tzinfo=UTC)
    retry = checked + timedelta(seconds=60)
    report = _result_report(EquityRolloverResult(
        "RETRY", checked, next_retry_at=retry, reason="ROLLOVER_BACKOFF",
    ))
    assert report["status"] == "RETRY"
    assert report["next_retry_at"] == retry.isoformat()


def _history_plan(monkeypatch, *, invalid, maximum_facts=1000):
    calendar = OptionExchangeCalendar("XNYS")
    cursor = MagicMock()
    cursor.fetchone.return_value = {
        "revision_id": PLAN.revision_id,
        "source_cohort_id": PLAN.source_cohort_id,
        "effective_session_date": PLAN.target_session_date,
    }
    calls = []

    def history(_cursor, **scope):
        calls.append(scope)
        return EquityHistoryRead(tuple(
            {
                "session_date": session,
                "bar_start": equity_history_coordinates(calendar, scope["interval"], session)[0][0],
                "bar_end": equity_history_coordinates(calendar, scope["interval"], session)[0][1],
                "stale_adjustment_lineage": (scope["interval"], session) in invalid,
            }
            for session in scope["sessions"]
        ), None)

    monkeypatch.setattr(rollover, "load_equity_history_cursor", history)
    monkeypatch.setattr(rollover, "_member_rows_cursor", lambda *_args: (MEMBER,))
    monkeypatch.setattr(rollover, "_reference_work_cursor", lambda *_args, **_kwargs: None)
    result = rollover.plan_equity_rollover_cursor(
        cursor, checked_at=TUESDAY_OPEN - timedelta(hours=5), calendar=calendar,
        maximum_facts_per_chunk=maximum_facts,
    )
    return result, calls


@pytest.mark.parametrize(
    ("interval", "attribute", "action"),
    (
        ("1d", "adjusted_sessions", "ADJUSTED_1D"),
        ("1wk", "weekly_sessions", "ADJUSTED_1WK"),
        ("1mo", "monthly_sessions", "ADJUSTED_1MO"),
    ),
)
def test_rollover_schedules_stale_adjustments_outside_the_old_tail(
    monkeypatch, interval, attribute, action,
):
    history = build_realtime_equity_portal_history_plan(PLAN.completed_session_date)
    stale = getattr(history, attribute)[10]
    plan, calls = _history_plan(monkeypatch, invalid={(interval, stale)})
    assert [(work.action, work.sessions) for work in plan.work] == [(action, (stale,))]
    assert all(call["adjustments_through"] == PLAN.completed_session_date for call in calls)


def test_rollover_batches_only_contiguous_invalid_periods_within_the_fact_bound(monkeypatch):
    history = build_realtime_equity_portal_history_plan(PLAN.completed_session_date)
    invalid = {("1d", session) for session in (
        *history.adjusted_sessions[10:13], history.adjusted_sessions[20],
    )}
    plan, _calls = _history_plan(monkeypatch, invalid=invalid, maximum_facts=2)
    assert plan.work[0].sessions == history.adjusted_sessions[10:12]


def test_rollover_does_not_schedule_usable_history(monkeypatch):
    plan, _calls = _history_plan(monkeypatch, invalid=set())
    assert plan.work == ()
