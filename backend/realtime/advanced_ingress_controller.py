"""Pure retry and terminal-failure contracts for Advanced daily ingress."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from random import Random
from uuid import NAMESPACE_URL, UUID, uuid5


class IngressFailureDisposition(str, Enum):
    TRANSIENT = "TRANSIENT"
    TERMINAL = "TERMINAL"
    INTENTIONAL = "INTENTIONAL"


class HybridStartupState(str, Enum):
    NO_GAP = "NO_GAP"
    WAITING_PROVIDER_ELIGIBILITY = "WAITING_PROVIDER_ELIGIBILITY"
    REPAIR_READY = "REPAIR_READY"


@dataclass(frozen=True, slots=True)
class HybridStartupPlan:
    state: HybridStartupState
    gap_start: datetime | None
    gap_end: datetime | None
    repair_start: datetime | None
    repair_end: datetime | None
    pending_start: datetime | None
    pending_end: datetime | None


@dataclass(frozen=True, slots=True)
class HybridRepairIdentity:
    repair_id: UUID
    window_key: str
    state: HybridStartupState
    gap_start: datetime
    gap_end: datetime
    repair_start: datetime | None
    repair_end: datetime | None
    release_at: datetime
    endpoint_class: str
    eligibility_policy_version: str
    provider_delay: timedelta


@dataclass(frozen=True, slots=True)
class HybridBarIdentity:
    instrument_id: int
    bar_start: datetime
    bar_end: datetime
    source_kind: str
    quality_codes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class HybridClosure:
    complete_through: datetime
    blocked_windows: tuple[datetime, ...]


def _aware(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value


def plan_hybrid_startup(
    *,
    session_open: datetime,
    stream_started_at: datetime,
    checked_at: datetime,
    provider_delay: timedelta,
    complete_through: datetime | None,
    overlap: timedelta = timedelta(minutes=1),
) -> HybridStartupPlan:
    opened = _aware(session_open, "session_open")
    streamed = _aware(stream_started_at, "stream_started_at")
    checked = _aware(checked_at, "checked_at")
    complete = (
        None
        if complete_through is None
        else _aware(complete_through, "complete_through")
    )
    if provider_delay < timedelta(0) or overlap < timedelta(0):
        raise ValueError("provider delay and overlap must be nonnegative")
    gap_start = max(opened, complete or opened)
    gap_end = max(opened, streamed)
    if gap_end <= gap_start:
        return HybridStartupPlan(
            HybridStartupState.NO_GAP,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    eligible_end = min(gap_end, checked - provider_delay)
    if eligible_end <= gap_start:
        return HybridStartupPlan(
            HybridStartupState.WAITING_PROVIDER_ELIGIBILITY,
            gap_start,
            gap_end,
            None,
            None,
            gap_start,
            gap_end,
        )
    repair_start = max(opened, gap_start - overlap)
    pending_start = eligible_end if eligible_end < gap_end else None
    return HybridStartupPlan(
        HybridStartupState.REPAIR_READY,
        gap_start,
        gap_end,
        repair_start,
        eligible_end,
        pending_start,
        gap_end if pending_start is not None else None,
    )


def hybrid_repair_identity(
    *,
    ingress_plan_id: UUID,
    cohort_id: UUID,
    startup: HybridStartupPlan,
    checked_at: datetime,
    provider_delay: timedelta,
    endpoint_class: str,
    eligibility_policy_version: str,
) -> HybridRepairIdentity | None:
    checked = _aware(checked_at, "checked_at")
    endpoint = endpoint_class.strip()
    policy = eligibility_policy_version.strip()
    if provider_delay < timedelta(0) or not endpoint or not policy:
        raise ValueError("provider delay must be nonnegative")
    if startup.state is HybridStartupState.NO_GAP:
        return None
    if startup.gap_start is None or startup.gap_end is None:
        raise ValueError("hybrid repair requires an exact gap")
    scope = (
        f"{ingress_plan_id}:{cohort_id}:{startup.gap_start.isoformat()}:"
        f"{startup.gap_end.isoformat()}:"
        f"{startup.repair_start.isoformat() if startup.repair_start else '-'}:"
        f"{startup.repair_end.isoformat() if startup.repair_end else '-'}:"
        f"{endpoint}:{policy}:{provider_delay.total_seconds()}"
    )
    repair_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-hybrid-repair:{scope}",
    )
    release_at = (
        checked
        if startup.state is HybridStartupState.REPAIR_READY
        else startup.gap_end + provider_delay
    )
    return HybridRepairIdentity(
        repair_id=repair_id,
        window_key=f"adv-hybrid-{repair_id.hex[:24]}",
        state=startup.state,
        gap_start=startup.gap_start,
        gap_end=startup.gap_end,
        repair_start=startup.repair_start,
        repair_end=startup.repair_end,
        release_at=release_at,
        endpoint_class=endpoint,
        eligibility_policy_version=policy,
        provider_delay=provider_delay,
    )


def reconcile_hybrid_minute_closure(
    *,
    requested_from: datetime,
    requested_to: datetime,
    instrument_ids: tuple[int, ...],
    bars: tuple[HybridBarIdentity, ...],
    window: timedelta = timedelta(minutes=5),
) -> HybridClosure:
    start = _aware(requested_from, "requested_from")
    end = _aware(requested_to, "requested_to")
    members = tuple(sorted(instrument_ids))
    if (
        start >= end
        or (end - start).total_seconds() % 60
        or not members
        or len(set(members)) != len(members)
        or any(type(value) is not int or value <= 0 for value in members)
        or window <= timedelta(0)
        or window.total_seconds() % 60
    ):
        raise ValueError("hybrid closure scope is invalid")
    retained: set[tuple[int, datetime]] = set()
    for bar in bars:
        bar_start = _aware(bar.bar_start, "bar_start")
        bar_end = _aware(bar.bar_end, "bar_end")
        if bar.instrument_id not in members or not start <= bar_start < end:
            continue
        if bar_end != bar_start + timedelta(minutes=1):
            raise ValueError("hybrid closure requires exact one-minute bars")
        if bar.source_kind == "NATIVE_REST":
            if "EXPLICIT_ADVANCED_REST_REPAIR_V1" not in bar.quality_codes:
                raise ValueError(
                    "hybrid closure requires explicit Advanced REST lineage"
                )
        elif bar.source_kind != "REALTIME_STREAM":
            raise ValueError("hybrid closure source is unsupported")
        retained.add((bar.instrument_id, bar_start))
    boundary = start
    while boundary < end and all(
        (instrument_id, boundary) in retained
        for instrument_id in members
    ):
        boundary += timedelta(minutes=1)
    blocked = []
    window_start = start
    while window_start < end:
        window_end = min(window_start + window, end)
        cursor = window_start
        complete = True
        while cursor < window_end:
            if any(
                (instrument_id, cursor) not in retained
                for instrument_id in members
            ):
                complete = False
                break
            cursor += timedelta(minutes=1)
        if not complete:
            blocked.append(window_end)
        window_start = window_end
    return HybridClosure(boundary, tuple(blocked))


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
    "JOURNAL_",
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
    "CHILD_EXIT_",
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
