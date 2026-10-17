from random import Random

import pytest

from realtime.advanced_ingress_controller import (
    AdvancedReconnectState,
    IngressFailureDisposition,
    classify_advanced_ingress_failure,
    full_jitter_backoff_seconds,
)


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
