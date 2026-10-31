import inspect
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest

import realtime.advanced_equity_lifecycle as lifecycle
from realtime.advanced_realtime_desired_state import (
    DesiredMemberIntent,
    DesiredMemberKind,
    DesiredSessionSnapshot,
    DesiredStateRevision,
)
from realtime.advanced_equity_lifecycle import (
    ADVANCED_EQUITY_LIFECYCLE_RESOURCE,
    AdvancedEquityLifecyclePhase,
    AdvancedEquityLifecyclePlanner,
    LifecycleLease,
    claim_lifecycle_lease_cursor,
    EffectiveDesiredSession,
    ensure_automatic_stream_authorization_cursor,
    load_or_roll_effective_desired_session_cursor,
    renew_lifecycle_lease_cursor,
)
from realtime.advanced_ingress_plan import AdvancedIngressBudgets
from scripts import run_realtime_v2_advanced_equity_ingress as ingress


UTC = timezone.utc
OWNER = UUID("11111111-1111-1111-1111-111111111111")


def _revision(session_date, revision_id=UUID(int=10)):
    return DesiredStateRevision(
        revision_id=revision_id,
        revision_sha256="a" * 64,
        effective_session_date=session_date,
        source_cohort_id=UUID(int=20),
        source_slot_version=None,
        selection_policy_reference="test",
        selection_policy_sha256="b" * 64,
        enabled=True,
        recorded_at=datetime(2026, 10, 30, 0, 0, tzinfo=UTC),
        members=(
            DesiredMemberIntent(
                "SPY",
                DesiredMemberKind.EQUITY,
            ),
        ),
        security_ids=(("SPY", UUID(int=21)),),
    )


def _snapshot(session_date, revision_id=UUID(int=10)):
    return DesiredSessionSnapshot(
        revision_id=revision_id,
        session_date=session_date,
        manifest_id=UUID(int=30),
        cohort_id=UUID(int=40),
        slot_set_version=None,
        snapshot_sha256="c" * 64,
        materialized_at=datetime(2026, 10, 30, 0, 1, tzinfo=UTC),
    )


@pytest.mark.parametrize(
    ("now", "phase", "target", "catch_up"),
    (
        (
            datetime(2026, 10, 30, 12, 0, tzinfo=UTC),
            AdvancedEquityLifecyclePhase.BEFORE_OPEN,
            date(2026, 10, 30),
            False,
        ),
        (
            datetime(2026, 10, 30, 16, 0, tzinfo=UTC),
            AdvancedEquityLifecyclePhase.MARKET_HOURS,
            date(2026, 10, 30),
            True,
        ),
        (
            datetime(2026, 10, 30, 20, 1, tzinfo=UTC),
            AdvancedEquityLifecyclePhase.DRAINING,
            date(2026, 10, 30),
            True,
        ),
        (
            datetime(2026, 10, 31, 16, 0, tzinfo=UTC),
            AdvancedEquityLifecyclePhase.IDLE,
            date(2026, 11, 2),
            False,
        ),
        (
            datetime(2026, 7, 3, 16, 0, tzinfo=UTC),
            AdvancedEquityLifecyclePhase.IDLE,
            date(2026, 7, 6),
            False,
        ),
    ),
)
def test_injected_clock_derives_phase_session_and_catch_up(
    now,
    phase,
    target,
    catch_up,
):
    plan = AdvancedEquityLifecyclePlanner(clock=lambda: now).plan(
        drain_seconds=120,
        finality_delay_seconds=5,
        completion_budget_seconds=300,
    )

    assert plan.phase is phase
    assert plan.target_session_date == target
    assert plan.catch_up_needed is catch_up


def test_dst_half_day_and_durable_cutoff_drive_dynamic_plan():
    half_day = AdvancedEquityLifecyclePlanner(
        clock=lambda: datetime(2026, 11, 27, 18, 10, tzinfo=UTC)
    ).plan(
        drain_seconds=900,
        finality_delay_seconds=5,
        completion_budget_seconds=300,
        last_completed_cutoff=datetime(
            2026, 11, 27, 16, 55, tzinfo=UTC
        ),
    )
    winter = AdvancedEquityLifecyclePlanner(
        clock=lambda: datetime(2026, 1, 5, 14, 0, tzinfo=UTC)
    ).plan(
        drain_seconds=120,
        finality_delay_seconds=5,
        completion_budget_seconds=300,
    )
    summer = AdvancedEquityLifecyclePlanner(
        clock=lambda: datetime(2026, 7, 6, 14, 0, tzinfo=UTC)
    ).plan(
        drain_seconds=120,
        finality_delay_seconds=5,
        completion_budget_seconds=300,
    )

    assert half_day.session_close == datetime(
        2026, 11, 27, 18, 0, tzinfo=UTC
    )
    assert half_day.first_due_cutoff == datetime(
        2026, 11, 27, 17, 0, tzinfo=UTC
    )
    assert winter.session_open.hour == 14
    assert summer.session_open.hour == 13


def test_midday_durable_progress_skips_complete_windows_and_keeps_catch_up():
    planner = AdvancedEquityLifecyclePlanner(
        clock=lambda: datetime(2026, 10, 30, 16, 30, tzinfo=UTC)
    )

    plan = planner.plan(
        drain_seconds=30,
        finality_delay_seconds=5,
        completion_budget_seconds=300,
        last_completed_cutoff=datetime(
            2026, 10, 30, 15, 55, tzinfo=UTC
        ),
    )

    assert plan.first_due_cutoff == datetime(
        2026, 10, 30, 16, 0, tzinfo=UTC
    )
    assert plan.catch_up_needed is True


def test_fully_complete_session_has_no_due_cutoff_or_catch_up():
    planner = AdvancedEquityLifecyclePlanner(
        clock=lambda: datetime(2026, 10, 30, 20, 0, 15, tzinfo=UTC)
    )

    plan = planner.plan(
        drain_seconds=30,
        finality_delay_seconds=5,
        completion_budget_seconds=300,
        last_completed_cutoff=datetime(
            2026, 10, 30, 20, 0, tzinfo=UTC
        ),
    )

    assert plan.first_due_cutoff is None
    assert plan.catch_up_needed is False


@pytest.mark.parametrize(
    ("offset_seconds", "phase", "processing", "preparation"),
    (
        (
            1,
            AdvancedEquityLifecyclePhase.DRAINING,
            date(2026, 10, 30),
            date(2026, 11, 2),
        ),
        (
            31,
            AdvancedEquityLifecyclePhase.DRAINING,
            date(2026, 10, 30),
            date(2026, 11, 2),
        ),
        (
            304,
            AdvancedEquityLifecyclePhase.DRAINING,
            date(2026, 10, 30),
            date(2026, 11, 2),
        ),
        (
            306,
            AdvancedEquityLifecyclePhase.IDLE,
            date(2026, 11, 2),
            date(2026, 11, 2),
        ),
    ),
)
def test_close_separates_acquisition_processing_and_preparation(
    offset_seconds,
    phase,
    processing,
    preparation,
):
    checked = datetime(
        2026,
        10,
        30,
        20,
        0,
        tzinfo=UTC,
    ) + timedelta(seconds=offset_seconds)

    plan = AdvancedEquityLifecyclePlanner(
        clock=lambda: checked
    ).plan(
        drain_seconds=30,
        finality_delay_seconds=5,
        completion_budget_seconds=300,
    )

    assert plan.phase is phase
    assert plan.processing_session_date == processing
    assert plan.preparation_session_date == preparation
    assert (
        plan.acquisition_session_date is None
        if phase is AdvancedEquityLifecyclePhase.DRAINING
        else plan.acquisition_session_date == preparation
    )


def test_processing_deadline_uses_exact_final_window_deadline_on_half_day():
    plan = AdvancedEquityLifecyclePlanner(
        clock=lambda: datetime(
            2026, 11, 27, 18, 5, 4, tzinfo=UTC
        )
    ).plan(
        drain_seconds=30,
        finality_delay_seconds=5,
        completion_budget_seconds=300,
    )

    assert plan.session_close == datetime(
        2026, 11, 27, 18, 0, tzinfo=UTC
    )
    assert plan.processing_deadline == datetime(
        2026, 11, 27, 18, 5, 5, tzinfo=UTC
    )
    assert plan.phase is AdvancedEquityLifecyclePhase.DRAINING


@pytest.mark.parametrize("competitor", (OWNER, UUID(int=2)))
def test_active_lifecycle_owner_is_refused_even_for_same_owner_id(
    competitor,
):
    cursor = MagicMock()
    cursor.fetchone.return_value = None

    lease = claim_lifecycle_lease_cursor(
        cursor,
        owner_id=competitor,
        checked_at=datetime(2026, 10, 30, 12, 0, tzinfo=UTC),
        lease_seconds=30,
    )

    assert lease is None
    statement = cursor.execute.call_args.args[0]
    assert "rt.worker_leases.lease_until<=%s" in statement
    assert "rt.worker_leases.owner_id=EXCLUDED.owner_id" not in statement


def test_expired_lifecycle_owner_takeover_advances_epoch():
    cursor = MagicMock()
    lease_until = datetime(2026, 10, 30, 12, 0, 30, tzinfo=UTC)
    cursor.fetchone.return_value = (8, lease_until)

    lease = claim_lifecycle_lease_cursor(
        cursor,
        owner_id=OWNER,
        checked_at=datetime(2026, 10, 30, 12, 0, tzinfo=UTC),
        lease_seconds=30,
    )

    assert lease == LifecycleLease(
        ADVANCED_EQUITY_LIFECYCLE_RESOURCE,
        OWNER,
        8,
        lease_until,
    )
    assert (
        "owner_epoch=rt.worker_leases.owner_epoch+1"
        in cursor.execute.call_args.args[0]
    )


def test_lifecycle_renewal_is_owner_and_epoch_fenced():
    cursor = MagicMock()
    renewed_until = datetime(2026, 10, 30, 12, 1, tzinfo=UTC)
    cursor.fetchone.return_value = (renewed_until,)
    lease = LifecycleLease(
        ADVANCED_EQUITY_LIFECYCLE_RESOURCE,
        OWNER,
        7,
        datetime(2026, 10, 30, 12, 0, 30, tzinfo=UTC),
    )

    renewed = renew_lifecycle_lease_cursor(
        cursor,
        lease,
        checked_at=datetime(2026, 10, 30, 12, 0, 30, tzinfo=UTC),
        lease_seconds=30,
    )

    assert renewed == LifecycleLease(
        ADVANCED_EQUITY_LIFECYCLE_RESOURCE,
        OWNER,
        7,
        renewed_until,
    )
    assert "owner_epoch=%s" in cursor.execute.call_args.args[0]


def test_current_immutable_desired_snapshot_is_selected_without_rollover(
    monkeypatch,
):
    target = date(2026, 10, 30)
    revision = _revision(target)
    snapshot = _snapshot(target)
    effective = SimpleNamespace(
        state="READY",
        revision=revision,
        snapshot=snapshot,
    )
    monkeypatch.setattr(
        lifecycle,
        "load_effective_desired_state_reconciliation_cursor",
        MagicMock(return_value=effective),
    )
    monkeypatch.setattr(
        lifecycle,
        "reconcile_desired_state_revision_cursor",
        MagicMock(return_value=effective),
    )
    record = MagicMock()
    monkeypatch.setattr(
        lifecycle,
        "record_desired_state_rollover_revision_cursor",
        record,
    )
    cursor = MagicMock()
    cursor.fetchone.return_value = (9,)

    selected = load_or_roll_effective_desired_session_cursor(
        cursor,
        target_session_date=target,
        checked_at=datetime(2026, 10, 30, 12, 0, tzinfo=UTC),
    )

    assert selected.revision == revision
    assert selected.snapshot == snapshot
    assert selected.equity_member_count == 9
    record.assert_not_called()
    assert cursor.execute.call_count == 1


def test_missing_effective_snapshot_is_reconciled_automatically(
    monkeypatch,
):
    target = date(2026, 11, 2)
    revision = _revision(target)
    snapshot = _snapshot(target)
    monkeypatch.setattr(
        lifecycle,
        "load_effective_desired_state_reconciliation_cursor",
        MagicMock(return_value=SimpleNamespace(
            state="MISSING_SNAPSHOT",
            revision=revision,
            snapshot=None,
        )),
    )
    reconcile = MagicMock(return_value=SimpleNamespace(
        state="READY",
        revision=revision,
        snapshot=snapshot,
    ))
    monkeypatch.setattr(
        lifecycle,
        "reconcile_desired_state_revision_cursor",
        reconcile,
    )
    cursor = MagicMock()
    cursor.fetchone.return_value = (9,)

    selected = load_or_roll_effective_desired_session_cursor(
        cursor,
        target_session_date=target,
        checked_at=datetime(2026, 11, 2, 12, 0, tzinfo=UTC),
    )

    assert selected.snapshot == snapshot
    assert reconcile.call_count == 1
    assert reconcile.call_args.args == (
        cursor,
        revision.revision_id,
    )
    assert reconcile.call_args.kwargs["materialized_at"] == datetime(
        2026, 11, 2, 12, 0, tzinfo=UTC
    )


def test_next_session_rollover_carries_current_immutable_snapshot(
    monkeypatch,
):
    current_date = date(2026, 10, 30)
    target = date(2026, 11, 2)
    current = _revision(current_date)
    current_snapshot = _snapshot(current_date)
    next_revision = _revision(target, UUID(int=11))
    next_snapshot = _snapshot(target, UUID(int=11))
    monkeypatch.setattr(
        lifecycle,
        "load_effective_desired_state_reconciliation_cursor",
        MagicMock(return_value=SimpleNamespace(
            state="READY",
            revision=current,
            snapshot=current_snapshot,
        )),
    )
    monkeypatch.setattr(
        lifecycle,
        "load_current_desired_state_revision_cursor",
        MagicMock(return_value=current),
    )
    reconcile = MagicMock(side_effect=(
        SimpleNamespace(
            state="READY",
            revision=next_revision,
            snapshot=next_snapshot,
        ),
    ))
    monkeypatch.setattr(
        lifecycle,
        "reconcile_desired_state_revision_cursor",
        reconcile,
    )
    record = MagicMock(return_value=next_revision)
    monkeypatch.setattr(
        lifecycle,
        "record_desired_state_rollover_revision_cursor",
        record,
    )
    cursor = MagicMock()
    cursor.fetchone.return_value = (9,)

    selected = load_or_roll_effective_desired_session_cursor(
        cursor,
        target_session_date=target,
        checked_at=datetime(2026, 11, 2, 16, 0, tzinfo=UTC),
    )

    change = record.call_args.args[1]
    assert change.source_cohort_id == current_snapshot.cohort_id
    assert change.source_slot_version == current_snapshot.slot_set_version
    assert record.call_args.kwargs["effective_session_date"] == target
    assert selected.snapshot == next_snapshot
    assert reconcile.call_count == 1


def test_live_ingress_hot_path_has_no_lifecycle_or_desired_state_dependency():
    hot_path = inspect.getsource(ingress._run_continuous)

    assert "desired_state" not in hot_path
    assert "lifecycle" not in hot_path
    assert "worker_leases" not in hot_path


def test_automatic_authorization_is_finite_and_session_scoped(
    monkeypatch,
):
    target = date(2026, 11, 2)
    desired = EffectiveDesiredSession(
        revision=_revision(target),
        snapshot=_snapshot(target),
        equity_member_count=9,
    )
    persist = MagicMock(
        side_effect=lambda _cursor, authorization, **_kwargs: authorization
    )
    monkeypatch.setattr(
        lifecycle,
        "create_advanced_stream_authorization_cursor",
        persist,
    )
    budgets = AdvancedIngressBudgets(
        stream_sessions=4,
        connection_attempts=12,
        decoded_bytes=1024 * 1024 * 1024,
        rest_repair_requests=18,
        rest_repair_decoded_bytes=18 * (1024 * 1024 + 1),
    )

    authorization = ensure_automatic_stream_authorization_cursor(
        object(),
        desired=desired,
        budgets=budgets,
        runtime_policy_sha256="d" * 64,
        promotion_ladder=(9,),
    )

    assert authorization.valid_from == target
    assert authorization.valid_through == target
    assert authorization.maximum_budgets == budgets
    assert authorization.allowed_universe_targets == (9,)
    assert authorization.parent_authorization_id is None
