"""Immutable standing authorization and daily Advanced ingress plan contracts."""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import canonical_sha256


ADVANCED_INGRESS_PROVIDER = "massive"
ADVANCED_INGRESS_FEED = "STOCKS"
ADVANCED_INGRESS_CHANNEL = "AM"
ADVANCED_INGRESS_MODE = "SHADOW"
ADVANCED_AUTHORIZATION_STATUSES = frozenset(
    {"READY", "REVOKED", "EXPIRED"}
)
ADVANCED_INGRESS_PLAN_STATUSES = frozenset(
    {"READY", "ACTIVE", "COMPLETE", "BLOCKED", "FAILED"}
)
ADVANCED_INGRESS_TERMINAL_OUTCOMES = (
    "SESSION_COMPLETE",
    "AUTHORIZATION_EXHAUSTED",
    "BLOCKED_CONFIGURATION",
    "BLOCKED_ENTITLEMENT",
    "BLOCKED_GAP",
    "FAILED_INFRASTRUCTURE",
)
ADVANCED_INGRESS_CUMULATIVE_COUNTERS = (
    "stream_session_count",
    "connection_attempt_count",
    "decoded_byte_count",
    "rest_repair_request_count",
    "rest_repair_decoded_byte_count",
)
ADVANCED_AUTHORIZATION_TRANSITIONS = (
    ("READY", "REVOKED"),
    ("READY", "EXPIRED"),
)
ADVANCED_INGRESS_PLAN_TRANSITIONS = (
    ("READY", "ACTIVE"),
    ("READY", "BLOCKED"),
    ("READY", "FAILED"),
    ("ACTIVE", "COMPLETE"),
    ("ACTIVE", "BLOCKED"),
    ("ACTIVE", "FAILED"),
)


@dataclass(frozen=True, slots=True)
class AdvancedIngressContractMatrix:
    authorization_transitions: tuple[tuple[str, str], ...]
    plan_transitions: tuple[tuple[str, str], ...]
    cumulative_counters: tuple[str, ...]
    standing_authorization_identity: tuple[str, ...]
    daily_plan_identity: tuple[str, ...]
    child_stream_identity: tuple[str, ...]
    terminal_outcomes: tuple[str, ...]
    scale_policy: str
    lifecycle_calendar: str


ADVANCED_INGRESS_CONTRACT_MATRIX = AdvancedIngressContractMatrix(
    authorization_transitions=ADVANCED_AUTHORIZATION_TRANSITIONS,
    plan_transitions=ADVANCED_INGRESS_PLAN_TRANSITIONS,
    cumulative_counters=ADVANCED_INGRESS_CUMULATIVE_COUNTERS,
    standing_authorization_identity=(
        "authorization_id",
        "scope_sha256",
        "authorization_reference",
        "parent_authorization_id",
    ),
    daily_plan_identity=(
        "plan_id",
        "authorization_id",
        "cohort_id",
        "manifest_id",
        "session_date",
        "universe_target",
        "mode",
        "runtime_profile_version",
        "runtime_profile_sha256",
        "configuration_sha256",
        "runtime_policy_sha256",
        "authorized_start_at",
        "authorized_end_at",
        "budgets",
    ),
    child_stream_identity=(
        "plan_id",
        "stream_session_id",
        "journal_generation",
        "owner_id",
        "owner_epoch",
    ),
    terminal_outcomes=ADVANCED_INGRESS_TERMINAL_OUTCOMES,
    scale_policy="ONE_ADMIN_APPROVED_TARGET_PER_AUTHORIZATION",
    lifecycle_calendar="XNYS",
)


def _text(value: str, name: str, maximum: int) -> str:
    normalized = value.strip()
    if not 1 <= len(normalized) <= maximum:
        raise ValueError(f"{name} must contain 1-{maximum} characters")
    return normalized


def _sha256(value: str, name: str) -> str:
    normalized = value.strip().lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef"
        for character in normalized
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return normalized


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _bounded_int(
    value: int,
    name: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(
            f"{name} must be an integer in [{minimum}, {maximum}]"
        )
    return value


@dataclass(frozen=True, slots=True)
class AdvancedIngressBudgets:
    stream_sessions: int
    connection_attempts: int
    decoded_bytes: int
    rest_repair_requests: int
    rest_repair_decoded_bytes: int

    def __post_init__(self) -> None:
        _bounded_int(
            self.stream_sessions,
            "stream_sessions",
            minimum=1,
            maximum=2_147_483_647,
        )
        _bounded_int(
            self.connection_attempts,
            "connection_attempts",
            minimum=self.stream_sessions,
            maximum=2_147_483_647,
        )
        _bounded_int(
            self.decoded_bytes,
            "decoded_bytes",
            minimum=1,
            maximum=9_223_372_036_854_775_807,
        )
        _bounded_int(
            self.rest_repair_requests,
            "rest_repair_requests",
            minimum=0,
            maximum=2_147_483_647,
        )
        _bounded_int(
            self.rest_repair_decoded_bytes,
            "rest_repair_decoded_bytes",
            minimum=0,
            maximum=9_223_372_036_854_775_807,
        )
        if (self.rest_repair_requests == 0) != (
            self.rest_repair_decoded_bytes == 0
        ):
            raise ValueError(
                "REST repair request and byte budgets must both be zero "
                "or both be positive"
            )

    def is_within(self, maximum: AdvancedIngressBudgets) -> bool:
        return all(
            child <= parent
            for child, parent in zip(
                (
                    self.stream_sessions,
                    self.connection_attempts,
                    self.decoded_bytes,
                    self.rest_repair_requests,
                    self.rest_repair_decoded_bytes,
                ),
                (
                    maximum.stream_sessions,
                    maximum.connection_attempts,
                    maximum.decoded_bytes,
                    maximum.rest_repair_requests,
                    maximum.rest_repair_decoded_bytes,
                ),
            )
        )


@dataclass(frozen=True, slots=True)
class AdvancedStreamAuthorization:
    authorization_id: UUID
    authorization_reference: str
    scope_sha256: str
    parent_authorization_id: UUID | None
    allowed_universe_targets: tuple[int, ...]
    valid_from: date
    valid_through: date
    maximum_budgets: AdvancedIngressBudgets
    runtime_policy_sha256: str
    approval_source: str
    approval_reference: str
    provider: str = ADVANCED_INGRESS_PROVIDER
    feed: str = ADVANCED_INGRESS_FEED
    channel: str = ADVANCED_INGRESS_CHANNEL
    status: str = "READY"

    def __post_init__(self) -> None:
        targets = tuple(sorted(set(self.allowed_universe_targets)))
        if (
            targets != self.allowed_universe_targets
            or len(targets) != 1
            or not 1 <= targets[0] <= 400
        ):
            raise ValueError(
                "standing authorization requires one exact manually "
                "approved universe target"
            )
        if (
            not isinstance(self.valid_from, date)
            or not isinstance(self.valid_through, date)
            or self.valid_through < self.valid_from
        ):
            raise ValueError("authorization validity dates are invalid")
        object.__setattr__(
            self,
            "runtime_policy_sha256",
            _sha256(
                self.runtime_policy_sha256,
                "runtime_policy_sha256",
            ),
        )
        object.__setattr__(
            self,
            "approval_source",
            _text(self.approval_source, "approval_source", 96),
        )
        object.__setattr__(
            self,
            "approval_reference",
            _text(self.approval_reference, "approval_reference", 160),
        )
        if (
            self.provider != ADVANCED_INGRESS_PROVIDER
            or self.feed != ADVANCED_INGRESS_FEED
            or self.channel != ADVANCED_INGRESS_CHANNEL
        ):
            raise ValueError("authorization provider/feed/channel is invalid")
        if self.status not in ADVANCED_AUTHORIZATION_STATUSES:
            raise ValueError("authorization status is invalid")
        expected_scope = canonical_sha256(self.scope_identity)
        if self.scope_sha256 != expected_scope:
            raise ValueError("authorization scope SHA does not match its scope")
        expected_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-stream-authorization:{expected_scope}",
        )
        if self.authorization_id != expected_id:
            raise ValueError("authorization ID does not match its scope")
        expected_reference = f"advanced-stream:{expected_id}"
        if self.authorization_reference != expected_reference:
            raise ValueError(
                "authorization reference does not match its authorization"
            )
        if self.parent_authorization_id == self.authorization_id:
            raise ValueError("authorization cannot be its own parent")

    @property
    def scope_identity(self) -> dict[str, object]:
        return {
            "contract": "advanced-stream-authorization-v1",
            "parent_authorization_id": self.parent_authorization_id,
            "provider": self.provider,
            "feed": self.feed,
            "channel": self.channel,
            "allowed_universe_targets": self.allowed_universe_targets,
            "valid_from": self.valid_from,
            "valid_through": self.valid_through,
            "maximum_budgets": self.maximum_budgets,
            "runtime_policy_sha256": self.runtime_policy_sha256,
            "approval_source": self.approval_source,
            "approval_reference": self.approval_reference,
        }

    def with_status(self, status: str) -> AdvancedStreamAuthorization:
        return replace(self, status=status)


def build_advanced_stream_authorization(
    *,
    parent_authorization_id: UUID | None,
    allowed_universe_targets: tuple[int, ...],
    valid_from: date,
    valid_through: date,
    maximum_budgets: AdvancedIngressBudgets,
    runtime_policy_sha256: str,
    approval_source: str,
    approval_reference: str,
) -> AdvancedStreamAuthorization:
    policy = _sha256(runtime_policy_sha256, "runtime_policy_sha256")
    source = _text(approval_source, "approval_source", 96)
    approval = _text(approval_reference, "approval_reference", 160)
    targets = tuple(sorted(set(allowed_universe_targets)))
    scope_identity = {
        "contract": "advanced-stream-authorization-v1",
        "parent_authorization_id": parent_authorization_id,
        "provider": ADVANCED_INGRESS_PROVIDER,
        "feed": ADVANCED_INGRESS_FEED,
        "channel": ADVANCED_INGRESS_CHANNEL,
        "allowed_universe_targets": targets,
        "valid_from": valid_from,
        "valid_through": valid_through,
        "maximum_budgets": maximum_budgets,
        "runtime_policy_sha256": policy,
        "approval_source": source,
        "approval_reference": approval,
    }
    scope_sha256 = canonical_sha256(scope_identity)
    authorization_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-stream-authorization:{scope_sha256}",
    )
    return AdvancedStreamAuthorization(
        authorization_id=authorization_id,
        authorization_reference=f"advanced-stream:{authorization_id}",
        scope_sha256=scope_sha256,
        parent_authorization_id=parent_authorization_id,
        allowed_universe_targets=targets,
        valid_from=valid_from,
        valid_through=valid_through,
        maximum_budgets=maximum_budgets,
        runtime_policy_sha256=policy,
        approval_source=source,
        approval_reference=approval,
    )


@dataclass(frozen=True, slots=True)
class AdvancedIngressPlan:
    plan_id: UUID
    plan_sha256: str
    authorization_id: UUID
    authorization_reference: str
    authorization_scope_sha256: str
    cohort_id: UUID
    manifest_id: UUID
    session_date: date
    universe_target: int
    mode: str
    runtime_profile_version: str
    runtime_profile_sha256: str
    configuration_sha256: str
    runtime_policy_sha256: str
    authorized_start_at: datetime
    authorized_end_at: datetime
    budgets: AdvancedIngressBudgets
    status: str = "READY"

    def __post_init__(self) -> None:
        if not isinstance(self.session_date, date):
            raise TypeError("session_date must be a date")
        if not 1 <= self.universe_target <= 400:
            raise ValueError("universe_target must be in [1, 400]")
        if self.mode != ADVANCED_INGRESS_MODE:
            raise ValueError("Advanced ingress mode must remain SHADOW")
        object.__setattr__(
            self,
            "runtime_profile_version",
            _text(
                self.runtime_profile_version,
                "runtime_profile_version",
                96,
            ),
        )
        object.__setattr__(
            self,
            "runtime_profile_sha256",
            _sha256(
                self.runtime_profile_sha256,
                "runtime_profile_sha256",
            ),
        )
        object.__setattr__(
            self,
            "configuration_sha256",
            _sha256(
                self.configuration_sha256,
                "configuration_sha256",
            ),
        )
        object.__setattr__(
            self,
            "runtime_policy_sha256",
            _sha256(
                self.runtime_policy_sha256,
                "runtime_policy_sha256",
            ),
        )
        object.__setattr__(
            self,
            "authorization_scope_sha256",
            _sha256(
                self.authorization_scope_sha256,
                "authorization_scope_sha256",
            ),
        )
        object.__setattr__(
            self,
            "authorization_reference",
            _text(
                self.authorization_reference,
                "authorization_reference",
                160,
            ),
        )
        started = _utc(self.authorized_start_at, "authorized_start_at")
        ended = _utc(self.authorized_end_at, "authorized_end_at")
        if ended <= started:
            raise ValueError("authorized lifecycle must end after it starts")
        object.__setattr__(self, "authorized_start_at", started)
        object.__setattr__(self, "authorized_end_at", ended)
        if self.status not in ADVANCED_INGRESS_PLAN_STATUSES:
            raise ValueError("ingress plan status is invalid")
        expected_sha = canonical_sha256(self.identity)
        if self.plan_sha256 != expected_sha:
            raise ValueError("plan SHA does not match its identity")
        expected_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-ingress-plan:{expected_sha}",
        )
        if self.plan_id != expected_id:
            raise ValueError("plan ID does not match its identity")

    @property
    def identity(self) -> dict[str, object]:
        return {
            "contract": "advanced-ingress-plan-v1",
            "authorization_id": self.authorization_id,
            "authorization_reference": self.authorization_reference,
            "authorization_scope_sha256": (
                self.authorization_scope_sha256
            ),
            "cohort_id": self.cohort_id,
            "manifest_id": self.manifest_id,
            "session_date": self.session_date,
            "universe_target": self.universe_target,
            "mode": self.mode,
            "runtime_profile_version": self.runtime_profile_version,
            "runtime_profile_sha256": self.runtime_profile_sha256,
            "configuration_sha256": self.configuration_sha256,
            "runtime_policy_sha256": self.runtime_policy_sha256,
            "authorized_start_at": self.authorized_start_at,
            "authorized_end_at": self.authorized_end_at,
            "budgets": self.budgets,
        }


def build_advanced_ingress_plan(
    *,
    authorization: AdvancedStreamAuthorization,
    cohort_id: UUID,
    manifest_id: UUID,
    session_date: date,
    universe_target: int,
    runtime_profile_version: str,
    runtime_profile_sha256: str,
    configuration_sha256: str,
    budgets: AdvancedIngressBudgets,
    calendar=None,
) -> AdvancedIngressPlan:
    if authorization.status != "READY":
        raise ValueError("standing authorization is not READY")
    if not authorization.valid_from <= session_date <= authorization.valid_through:
        raise ValueError("session date is outside standing authorization")
    if universe_target not in authorization.allowed_universe_targets:
        raise ValueError("universe target is not manually authorized")
    if not budgets.is_within(authorization.maximum_budgets):
        raise ValueError("daily plan budgets exceed standing authorization")
    if calendar is None:
        from options.calendar import OptionExchangeCalendar

        calendar = OptionExchangeCalendar("XNYS")
    authorized_start_at = _utc(
        calendar.session_open(session_date),
        "authorized_start_at",
    )
    authorized_end_at = _utc(
        calendar.session_close(session_date),
        "authorized_end_at",
    )
    identity = {
        "contract": "advanced-ingress-plan-v1",
        "authorization_id": authorization.authorization_id,
        "authorization_reference": authorization.authorization_reference,
        "authorization_scope_sha256": authorization.scope_sha256,
        "cohort_id": cohort_id,
        "manifest_id": manifest_id,
        "session_date": session_date,
        "universe_target": universe_target,
        "mode": ADVANCED_INGRESS_MODE,
        "runtime_profile_version": _text(
            runtime_profile_version,
            "runtime_profile_version",
            96,
        ),
        "runtime_profile_sha256": _sha256(
            runtime_profile_sha256,
            "runtime_profile_sha256",
        ),
        "configuration_sha256": _sha256(
            configuration_sha256,
            "configuration_sha256",
        ),
        "runtime_policy_sha256": authorization.runtime_policy_sha256,
        "authorized_start_at": authorized_start_at,
        "authorized_end_at": authorized_end_at,
        "budgets": budgets,
    }
    plan_sha256 = canonical_sha256(identity)
    return AdvancedIngressPlan(
        plan_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-ingress-plan:{plan_sha256}",
        ),
        plan_sha256=plan_sha256,
        authorization_id=authorization.authorization_id,
        authorization_reference=authorization.authorization_reference,
        authorization_scope_sha256=authorization.scope_sha256,
        cohort_id=cohort_id,
        manifest_id=manifest_id,
        session_date=session_date,
        universe_target=universe_target,
        mode=ADVANCED_INGRESS_MODE,
        runtime_profile_version=identity["runtime_profile_version"],
        runtime_profile_sha256=identity["runtime_profile_sha256"],
        configuration_sha256=identity["configuration_sha256"],
        runtime_policy_sha256=authorization.runtime_policy_sha256,
        authorized_start_at=authorized_start_at,
        authorized_end_at=authorized_end_at,
        budgets=budgets,
    )


@dataclass(frozen=True, slots=True)
class AdvancedIngressPlanRequest:
    authorization_id: UUID
    session_date: date
    universe_target: int
    budgets: AdvancedIngressBudgets


@dataclass(frozen=True, slots=True)
class AdvancedIngressUsage:
    plan_id: UUID
    stream_session_count: int
    connection_attempt_count: int
    decoded_byte_count: int
    rest_repair_request_count: int
    rest_repair_decoded_byte_count: int
    latest_owner_epoch: int
    latest_stream_session_id: UUID | None
