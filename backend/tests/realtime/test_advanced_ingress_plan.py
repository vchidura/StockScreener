from datetime import date, datetime, timedelta, timezone
from uuid import UUID

import pytest

from realtime.advanced_ingress_plan import (
    ADVANCED_INGRESS_CONTRACT_MATRIX,
    ADVANCED_INGRESS_CUMULATIVE_COUNTERS,
    ADVANCED_INGRESS_TERMINAL_OUTCOMES,
    AdvancedIngressBudgets,
    AdvancedIngressPlanRequest,
    advanced_ingress_provider_deadline,
    advanced_ingress_window,
    build_advanced_ingress_plan,
    build_advanced_stream_authorization,
)


UTC = timezone.utc
SESSION_DATE = date(2026, 10, 16)
COHORT_ID = UUID("11111111-1111-1111-1111-111111111111")
MANIFEST_ID = UUID("22222222-2222-2222-2222-222222222222")
POLICY_SHA = "a" * 64
PROFILE_SHA = "b" * 64


class _Calendar:
    def session_open(self, session_date):
        assert session_date == SESSION_DATE
        return datetime(2026, 10, 16, 13, 30, tzinfo=UTC)

    def session_close(self, session_date):
        assert session_date == SESSION_DATE
        return datetime(2026, 10, 16, 20, 0, tzinfo=UTC)


def _budgets(*, decoded_bytes=10_000):
    return AdvancedIngressBudgets(3, 4, decoded_bytes, 2, 1_000)


def _authorization(
    *,
    target=9,
    policy=POLICY_SHA,
    approval_reference="owner-ticket-1",
):
    return build_advanced_stream_authorization(
        parent_authorization_id=None,
        allowed_universe_targets=(target,),
        valid_from=SESSION_DATE,
        valid_through=SESSION_DATE,
        maximum_budgets=AdvancedIngressBudgets(
            5,
            8,
            100_000,
            4,
            10_000,
        ),
        runtime_policy_sha256=policy,
        approval_source="operator",
        approval_reference=approval_reference,
    )


def _plan(authorization=None, *, budgets=None, target=9, **window):
    return build_advanced_ingress_plan(
        authorization=authorization or _authorization(),
        cohort_id=COHORT_ID,
        manifest_id=MANIFEST_ID,
        session_date=SESSION_DATE,
        universe_target=target,
        runtime_profile_version=f"advanced-equity-ingress-{target}-v1",
        runtime_profile_sha256=PROFILE_SHA,
        configuration_sha256="e" * 64,
        budgets=budgets or _budgets(),
        calendar=_Calendar(),
        **window,
    )


def test_contract_matrix_freezes_p0_states_counters_and_identities():
    matrix = ADVANCED_INGRESS_CONTRACT_MATRIX

    assert matrix.authorization_transitions == (
        ("READY", "REVOKED"),
        ("READY", "EXPIRED"),
    )
    assert matrix.plan_transitions == (
        ("READY", "ACTIVE"),
        ("READY", "BLOCKED"),
        ("READY", "FAILED"),
        ("ACTIVE", "COMPLETE"),
        ("ACTIVE", "BLOCKED"),
        ("ACTIVE", "FAILED"),
    )
    assert matrix.cumulative_counters == (
        ADVANCED_INGRESS_CUMULATIVE_COUNTERS
    )
    assert matrix.terminal_outcomes == (
        ADVANCED_INGRESS_TERMINAL_OUTCOMES
    )
    assert matrix.child_stream_identity == (
        "plan_id",
        "stream_session_id",
        "journal_generation",
        "owner_id",
        "owner_epoch",
    )
    assert matrix.scale_policy == (
        "ONE_ADMIN_APPROVED_TARGET_PER_AUTHORIZATION"
    )
    assert matrix.lifecycle_calendar == "XNYS"


def test_authorization_and_plan_identity_are_deterministic_and_exact():
    authorization = _authorization()
    repeated_authorization = _authorization()
    first = _plan(authorization)
    repeated = _plan(repeated_authorization)

    assert authorization.authorization_id == (
        repeated_authorization.authorization_id
    )
    assert authorization.authorization_reference == (
        f"advanced-stream:{authorization.authorization_id}"
    )
    assert first.plan_id == repeated.plan_id
    assert first.plan_sha256 == repeated.plan_sha256
    assert first.authorized_start_at == datetime(
        2026,
        10,
        16,
        13,
        30,
        tzinfo=UTC,
    )
    assert first.authorized_end_at == datetime(
        2026,
        10,
        16,
        20,
        0,
        tzinfo=UTC,
    )


def test_changed_budget_policy_or_target_changes_plan_identity():
    baseline = _plan()
    changed_budget = _plan(
        budgets=_budgets(decoded_bytes=20_000),
    )
    changed_policy_value = "c" * 64
    changed_policy = _plan(
        _authorization(
            policy=changed_policy_value,
            approval_reference="owner-ticket-policy",
        ),
    )
    changed_target = _plan(
        _authorization(
            target=50,
            approval_reference="owner-ticket-target",
        ),
        target=50,
    )

    assert len({
        baseline.plan_id,
        changed_budget.plan_id,
        changed_policy.plan_id,
        changed_target.plan_id,
    }) == 4


def test_authorization_cannot_preapprove_an_automatic_scale_ladder():
    with pytest.raises(ValueError, match="one exact manually approved"):
        build_advanced_stream_authorization(
            parent_authorization_id=None,
            allowed_universe_targets=(9, 50),
            valid_from=SESSION_DATE,
            valid_through=SESSION_DATE,
            maximum_budgets=_budgets(),
            runtime_policy_sha256=POLICY_SHA,
            approval_source="operator",
            approval_reference="owner-ticket-1",
        )


def test_daily_plan_cannot_widen_target_or_budget():
    authorization = _authorization()

    with pytest.raises(ValueError, match="manually authorized"):
        _plan(authorization, target=50)
    assert _plan(authorization).runtime_policy_sha256 == (
        authorization.runtime_policy_sha256
    )
    with pytest.raises(ValueError, match="budgets exceed"):
        _plan(
            authorization,
            budgets=AdvancedIngressBudgets(
                6,
                8,
                100_000,
                4,
                10_000,
            ),
        )


def test_canary_bounds_are_hashed_and_restart_cannot_extend_them():
    started = _Calendar().session_open(SESSION_DATE)
    ended = started + timedelta(seconds=600)
    budgets = AdvancedIngressBudgets(3, 3, 10_000, 0, 0)
    authorization = _authorization(target=1)
    first = _plan(
        authorization, target=1, budgets=budgets,
        canary_start_at=started, canary_end_at=ended,
    )
    restarted = _plan(
        authorization, target=1, budgets=budgets,
        canary_start_at=started, canary_end_at=ended,
    )
    widened = _plan(authorization, target=1, budgets=budgets)

    assert first.plan_id == restarted.plan_id
    assert first.authorized_end_at == restarted.authorized_end_at == ended
    assert widened.plan_id != first.plan_id
    assert first.budgets == restarted.budgets == budgets
    assert advanced_ingress_provider_deadline(
        started, ended, drain_seconds=30, shutdown_seconds=30,
    ) == ended - timedelta(seconds=30)


@pytest.mark.parametrize("change", [
    {"canary_end_at": datetime(2026, 10, 16, 13, 40, 1, tzinfo=UTC)},
    {"canary_start_at": datetime(2026, 10, 16, 13, 29, 59, tzinfo=UTC)},
    {"canary_end_at": datetime(2026, 10, 16, 20, 0, 1, tzinfo=UTC)},
    {"canary_end_at": datetime(2026, 10, 16, 13, 30, tzinfo=UTC)},
    {"canary_end_at": datetime(2026, 10, 16, 13, 40)},
    {"canary_end_at": None},
])
def test_canary_rejects_overlong_outside_empty_naive_or_partial_window(change):
    scope = dict(
        session_date=SESSION_DATE, universe_target=1,
        budgets=AdvancedIngressBudgets(3, 3, 10_000, 0, 0),
        canary_start_at=datetime(2026, 10, 16, 13, 30, tzinfo=UTC),
        canary_end_at=datetime(2026, 10, 16, 13, 40, tzinfo=UTC),
        calendar=_Calendar(),
    )
    scope.update(change)
    with pytest.raises(ValueError):
        advanced_ingress_window(**scope)


@pytest.mark.parametrize(("target", "budgets"), [
    (9, AdvancedIngressBudgets(3, 3, 10_000, 0, 0)),
    (1, AdvancedIngressBudgets(4, 4, 10_000, 0, 0)),
    (1, AdvancedIngressBudgets(3, 4, 10_000, 0, 0)),
    (1, AdvancedIngressBudgets(3, 3, 10 * 1024 * 1024 + 1, 0, 0)),
    (1, AdvancedIngressBudgets(3, 3, 10_000, 1, 1)),
])
def test_canary_rejects_expansion_of_the_approved_envelope(target, budgets):
    with pytest.raises(ValueError, match="canary exceeds"):
        advanced_ingress_window(
            session_date=SESSION_DATE, universe_target=target, budgets=budgets,
            canary_start_at=_Calendar().session_open(SESSION_DATE),
            canary_end_at=_Calendar().session_open(SESSION_DATE) + timedelta(seconds=600),
            calendar=_Calendar(),
        )


def test_canary_request_normalizes_timezones_and_keeps_daily_deadline_behavior():
    offset = timezone(timedelta(hours=-4))
    request = AdvancedIngressPlanRequest(
        COHORT_ID, SESSION_DATE, 1, AdvancedIngressBudgets(3, 3, 10_000, 0, 0),
        canary_start_at=datetime(2026, 10, 16, 9, 30, tzinfo=offset),
        canary_end_at=datetime(2026, 10, 16, 9, 40, tzinfo=offset),
    )
    assert request.canary_start_at == _Calendar().session_open(SESSION_DATE)
    opened = _Calendar().session_open(SESSION_DATE)
    closed = _Calendar().session_close(SESSION_DATE)
    assert advanced_ingress_provider_deadline(
        opened, closed, drain_seconds=30, shutdown_seconds=30,
    ) == closed + timedelta(seconds=30)
    with pytest.raises(ValueError, match="shutdown budget"):
        advanced_ingress_provider_deadline(
            opened, opened + timedelta(seconds=30),
            drain_seconds=30, shutdown_seconds=30,
        )


def test_canary_cannot_borrow_options_authorization():
    budgets = AdvancedIngressBudgets(3, 3, 10_000, 0, 0)
    authorization = build_advanced_stream_authorization(
        parent_authorization_id=None, allowed_universe_targets=(1,),
        valid_from=SESSION_DATE, valid_through=SESSION_DATE,
        maximum_budgets=budgets, runtime_policy_sha256=POLICY_SHA,
        approval_source="operator", approval_reference="options-only",
        feed="OPTIONS", channel="T,Q",
    )
    with pytest.raises(ValueError, match="STOCKS/AM"):
        _plan(
            authorization, target=1, budgets=budgets,
            canary_start_at=_Calendar().session_open(SESSION_DATE),
            canary_end_at=_Calendar().session_open(SESSION_DATE) + timedelta(seconds=600),
        )
