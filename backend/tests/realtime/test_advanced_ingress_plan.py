from datetime import date, datetime, timezone
from uuid import UUID

import pytest

from realtime.advanced_ingress_plan import (
    ADVANCED_INGRESS_CONTRACT_MATRIX,
    ADVANCED_INGRESS_CUMULATIVE_COUNTERS,
    ADVANCED_INGRESS_TERMINAL_OUTCOMES,
    AdvancedIngressBudgets,
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


def _plan(authorization=None, *, budgets=None, target=9):
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
