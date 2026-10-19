from datetime import datetime, timedelta, timezone
from random import Random
from uuid import UUID

import pytest

from realtime.advanced_ingress_controller import (
    AdvancedReconnectState,
    HybridBarIdentity,
    HybridStartupState,
    IngressFailureDisposition,
    classify_advanced_ingress_failure,
    full_jitter_backoff_seconds,
    hybrid_repair_identity,
    plan_hybrid_startup,
    reconcile_hybrid_minute_closure,
)

NOW = datetime(2026, 10, 19, 13, 45, tzinfo=timezone.utc)


def test_hybrid_startup_has_no_repair_when_complete_through_stream_start():
    plan = plan_hybrid_startup(
        session_open=NOW - timedelta(minutes=15),
        stream_started_at=NOW,
        checked_at=NOW,
        provider_delay=timedelta(minutes=15),
        complete_through=NOW,
    )

    assert plan.state is HybridStartupState.NO_GAP
    assert plan.repair_start is None


def test_hybrid_startup_waits_without_polling_before_provider_eligibility():
    opened = NOW - timedelta(minutes=5)
    plan = plan_hybrid_startup(
        session_open=opened,
        stream_started_at=NOW,
        checked_at=NOW,
        provider_delay=timedelta(minutes=15),
        complete_through=None,
    )

    assert plan.state is HybridStartupState.WAITING_PROVIDER_ELIGIBILITY
    assert (plan.pending_start, plan.pending_end) == (opened, NOW)
    assert plan.repair_start is None


def test_hybrid_startup_repairs_only_eligible_gap_and_retains_pending_tail():
    opened = NOW - timedelta(minutes=30)
    plan = plan_hybrid_startup(
        session_open=opened,
        stream_started_at=NOW,
        checked_at=NOW,
        provider_delay=timedelta(minutes=15),
        complete_through=opened + timedelta(minutes=5),
    )

    assert plan.state is HybridStartupState.REPAIR_READY
    assert plan.gap_start == opened + timedelta(minutes=5)
    assert plan.repair_start == opened + timedelta(minutes=4)
    assert plan.repair_end == NOW - timedelta(minutes=15)
    assert plan.pending_start == NOW - timedelta(minutes=15)
    assert plan.pending_end == NOW


def test_hybrid_waiting_repair_identity_is_stable_until_full_gap_eligibility():
    opened = NOW - timedelta(minutes=5)
    plan = plan_hybrid_startup(
        session_open=opened,
        stream_started_at=NOW,
        checked_at=NOW,
        provider_delay=timedelta(minutes=15),
        complete_through=None,
    )

    first = hybrid_repair_identity(
        ingress_plan_id=UUID(int=1),
        cohort_id=UUID(int=2),
        startup=plan,
        checked_at=NOW,
        provider_delay=timedelta(minutes=15),
        endpoint_class="MASSIVE_STOCKS_AGGREGATES_1M",
        eligibility_policy_version="conservative-unverified-delay-v1",
    )
    restarted = hybrid_repair_identity(
        ingress_plan_id=UUID(int=1),
        cohort_id=UUID(int=2),
        startup=plan,
        checked_at=NOW + timedelta(minutes=1),
        provider_delay=timedelta(minutes=15),
        endpoint_class="MASSIVE_STOCKS_AGGREGATES_1M",
        eligibility_policy_version="conservative-unverified-delay-v1",
    )

    assert first == restarted
    assert first.release_at == NOW + timedelta(minutes=15)


def test_hybrid_closure_accepts_overlap_but_advances_only_exact_prefix():
    start = NOW - timedelta(minutes=5)
    bars = []
    for minute in range(5):
        bar_start = start + timedelta(minutes=minute)
        for instrument_id in (1, 2):
            if minute == 3 and instrument_id == 2:
                continue
            bars.append(HybridBarIdentity(
                instrument_id,
                bar_start,
                bar_start + timedelta(minutes=1),
                "REALTIME_STREAM",
            ))
    bars.append(HybridBarIdentity(
        1,
        start,
        start + timedelta(minutes=1),
        "NATIVE_REST",
        ("EXPLICIT_ADVANCED_REST_REPAIR_V1",),
    ))

    closure = reconcile_hybrid_minute_closure(
        requested_from=start,
        requested_to=NOW,
        instrument_ids=(1, 2),
        bars=tuple(bars),
    )

    assert closure.complete_through == start + timedelta(minutes=3)
    assert closure.blocked_windows == (NOW,)


def test_hybrid_closure_blocks_only_windows_with_unresolved_members():
    start = NOW - timedelta(minutes=10)
    bars = tuple(
        HybridBarIdentity(
            instrument_id,
            start + timedelta(minutes=minute),
            start + timedelta(minutes=minute + 1),
            "REALTIME_STREAM",
        )
        for minute in range(10)
        for instrument_id in (1, 2)
        if not (minute == 7 and instrument_id == 2)
    )

    closure = reconcile_hybrid_minute_closure(
        requested_from=start,
        requested_to=NOW,
        instrument_ids=(1, 2),
        bars=bars,
    )

    assert closure.complete_through == start + timedelta(minutes=7)
    assert closure.blocked_windows == (NOW,)


@pytest.mark.parametrize(
    ("code", "detail"),
    (
        ("gaierror", "DNS lookup failed"),
        ("ConnectionRefusedError", "TCP refused"),
        ("TimeoutError", "auth timeout"),
        ("TimeoutError", "ping timeout"),
        ("ConnectionClosedError", "provider server close"),
    ),
)
def test_network_and_timeout_failures_are_transient(code, detail):
    failure = classify_advanced_ingress_failure(
        code,
        detail=detail,
    )

    assert failure.disposition is IngressFailureDisposition.TRANSIENT
    assert failure.terminal_outcome == "FAILED_INFRASTRUCTURE"


@pytest.mark.parametrize(
    ("code", "detail", "outcome"),
    (
        (
            "GatewayControlError",
            "ADVANCED_AUTH_REJECTED",
            "BLOCKED_ENTITLEMENT",
        ),
        (
            "GatewayControlError",
            "ADVANCED_SUBSCRIPTION_REJECTED",
            "BLOCKED_ENTITLEMENT",
        ),
        (
            "AdvancedIngressAdmissionError",
            "daily connection budget is exhausted",
            "AUTHORIZATION_EXHAUSTED",
        ),
        (
            "GatewayOverload",
            "ADVANCED_MESSAGE_SIZE_EXCEEDED",
            "BLOCKED_CONFIGURATION",
        ),
        ("ValueError", "UNRESOLVED_GAP", "BLOCKED_GAP"),
    ),
)
def test_entitlement_configuration_gap_and_budget_failures_are_terminal(
    code,
    detail,
    outcome,
):
    failure = classify_advanced_ingress_failure(code, detail=detail)

    assert failure.disposition is IngressFailureDisposition.TERMINAL
    assert failure.terminal_outcome == outcome


def test_transient_streak_resets_only_after_stable_live_interval():
    unstable = classify_advanced_ingress_failure(
        "TimeoutError",
        live_seconds=59,
    )
    stable = classify_advanced_ingress_failure(
        "ConnectionClosedError",
        live_seconds=60,
    )

    state = AdvancedReconnectState(3).after_failure(
        unstable,
        stable_live_reset_seconds=60,
    )
    reset = state.after_failure(
        stable,
        stable_live_reset_seconds=60,
    )

    assert state.transient_attempt_streak == 4
    assert reset.transient_attempt_streak == 1


def test_full_jitter_exponential_backoff_is_bounded():
    first = full_jitter_backoff_seconds(
        AdvancedReconnectState(1),
        initial_seconds=0.5,
        multiplier=2,
        maximum_seconds=30,
        jitter_ceiling_seconds=30,
        random_source=Random(1),
    )
    capped = full_jitter_backoff_seconds(
        AdvancedReconnectState(20),
        initial_seconds=0.5,
        multiplier=2,
        maximum_seconds=30,
        jitter_ceiling_seconds=30,
        random_source=Random(1),
    )

    assert 0 <= first <= 0.5
    assert 0 <= capped <= 30


def test_intentional_shutdown_never_becomes_a_retry():
    failure = classify_advanced_ingress_failure(
        "SIGTERM",
        intentional_shutdown=True,
    )

    assert failure.disposition is IngressFailureDisposition.INTENTIONAL
    assert failure.terminal_outcome is None
