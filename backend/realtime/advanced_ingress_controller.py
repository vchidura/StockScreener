"""Pure retry and terminal-failure contracts for Advanced daily ingress."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from random import Random


class IngressFailureDisposition(str, Enum):
    TRANSIENT = "TRANSIENT"
    TERMINAL = "TERMINAL"
    INTENTIONAL = "INTENTIONAL"


@dataclass(frozen=True, slots=True)
class AdvancedIngressFailure:
    disposition: IngressFailureDisposition
    terminal_outcome: str | None
    failure_code: str
    live_seconds: float


@dataclass(frozen=True, slots=True)
class AdvancedReconnectState:
    transient_attempt_streak: int = 0

    def after_failure(
        self,
        failure: AdvancedIngressFailure,
        *,
        stable_live_reset_seconds: int,
    ) -> AdvancedReconnectState:
        if failure.disposition is not IngressFailureDisposition.TRANSIENT:
            return self
        streak = (
            1
            if failure.live_seconds >= stable_live_reset_seconds
            else self.transient_attempt_streak + 1
        )
        return AdvancedReconnectState(streak)


_ENTITLEMENT_MARKERS = (
    "AUTH_REJECTED",
    "SUBSCRIPTION_REJECTED",
    "NOT_AUTHORIZED",
    "ENTITLEMENT",
    "MAX_CONNECTIONS",
)
_AUTHORIZATION_MARKERS = (
    "AUTHORIZATION_EXHAUSTED",
    "BUDGET",
    "EXHAUSTED",
    "REVOKED",
    "EXPIRED",
)
_GAP_MARKERS = ("BLOCKED_GAP", "UNRESOLVED_GAP")
_CONFIGURATION_MARKERS = (
    "ADVANCED_MESSAGE_SIZE_EXCEEDED",
    "ADVANCED_HANDOFF_",
    "ADVANCED_NON_BYTES_MESSAGE",
    "CONTROL_LIMIT_EXCEEDED",
    "ADVANCED_BRIDGE_FAILED",
    "CONFIGURATION",
    "VALUEERROR",
    "GATEWAYCONTROLERROR",
    "GATEWAYOVERLOAD",
    "ADVANCEDJOURNALDECODEERROR",
    "JOURNAL_DISK_HEADROOM_LOW",
    "JOURNALCAPACITYERROR",
)
_TRANSIENT_MARKERS = (
    "CONNECTIONCLOSED",
    "CONNECTION_CLOSED",
    "CONNECTIONREFUSED",
    "DNS",
    "GAIERROR",
    "OSERROR",
    "TIMEOUT",
    "PING",
    "SERVER_CLOSE",
)


def classify_advanced_ingress_failure(
    failure_code: str,
    *,
    detail: str = "",
    live_seconds: float = 0,
    intentional_shutdown: bool = False,
) -> AdvancedIngressFailure:
    normalized_code = failure_code.strip().upper()
    normalized = f"{normalized_code}:{detail.strip().upper()}"
    if intentional_shutdown:
        return AdvancedIngressFailure(
            IngressFailureDisposition.INTENTIONAL,
            None,
            normalized_code or "INTENTIONAL_SHUTDOWN",
            max(0.0, float(live_seconds)),
        )
    for markers, outcome in (
        (_ENTITLEMENT_MARKERS, "BLOCKED_ENTITLEMENT"),
        (_AUTHORIZATION_MARKERS, "AUTHORIZATION_EXHAUSTED"),
        (_GAP_MARKERS, "BLOCKED_GAP"),
        (_CONFIGURATION_MARKERS, "BLOCKED_CONFIGURATION"),
    ):
        if any(marker in normalized for marker in markers):
            return AdvancedIngressFailure(
                IngressFailureDisposition.TERMINAL,
                outcome,
                normalized_code,
                max(0.0, float(live_seconds)),
            )
    if any(marker in normalized for marker in _TRANSIENT_MARKERS):
        return AdvancedIngressFailure(
            IngressFailureDisposition.TRANSIENT,
            "FAILED_INFRASTRUCTURE",
            normalized_code,
            max(0.0, float(live_seconds)),
        )
    return AdvancedIngressFailure(
        IngressFailureDisposition.TERMINAL,
        "FAILED_INFRASTRUCTURE",
        normalized_code or "UNKNOWN_FAILURE",
        max(0.0, float(live_seconds)),
    )


def full_jitter_backoff_seconds(
    state: AdvancedReconnectState,
    *,
    initial_seconds: float,
    multiplier: float,
    maximum_seconds: float,
    jitter_ceiling_seconds: float,
    random_source: Random | None = None,
) -> float:
    if state.transient_attempt_streak <= 0:
        raise ValueError("transient attempt streak must be positive")
    if (
        initial_seconds <= 0
        or multiplier <= 0
        or maximum_seconds < initial_seconds
        or jitter_ceiling_seconds <= 0
    ):
        raise ValueError("reconnect backoff settings are invalid")
    exponential = initial_seconds * (
        multiplier ** (state.transient_attempt_streak - 1)
    )
    ceiling = min(
        exponential,
        maximum_seconds,
        jitter_ceiling_seconds,
    )
    return (random_source or Random()).uniform(0.0, ceiling)
