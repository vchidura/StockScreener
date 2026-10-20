"""Provider-free resident service topology and health contracts for Options."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Protocol
from uuid import UUID

from .contracts import canonical_sha256
from .orchestration_repository import ServicePlan
from .stage_graph import STAGE_CATALOG_VERSION_V4


OPTIONS_RESIDENT_STAGE_SUBSET = frozenset({
    "REFERENCE",
    "EQUITY_INPUT_GATE",
    "EQUITY_MATERIALIZE",
    "OPTIONS_ACQUIRE",
    "OPTIONS_ADMISSION",
    "OPTIONS_FLOW",
    "OPTIONS_INPUT_GATE",
    "OPTIONS_CYCLE",
    "OPTIONS_PROJECTION",
    "OPTIONS_PUBLISH",
})


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


class AdvancedOptionsProcessKind(str, Enum):
    INGRESS = "INGRESS"
    MARKET_STATE = "MARKET_STATE"
    FLOW = "FLOW"
    WINDOW = "WINDOW"


@dataclass(frozen=True, slots=True)
class AdvancedOptionsProcessSpec:
    kind: AdvancedOptionsProcessKind
    owner_name: str
    lease_resource_key: str

    def __post_init__(self) -> None:
        owner = self.owner_name.strip()
        resource = self.lease_resource_key.strip()
        if (
            not owner
            or not resource
            or len(owner) > 96
            or len(resource) > 160
        ):
            raise ValueError("Advanced Options process identity is invalid")
        object.__setattr__(self, "owner_name", owner)
        object.__setattr__(self, "lease_resource_key", resource)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsResidentPolicy:
    version: str
    heartbeat_interval_seconds: int = 10
    stale_after_seconds: int = 30
    restart_initial_backoff_seconds: int = 1
    restart_maximum_backoff_seconds: int = 30
    restart_failure_limit: int = 5
    restart_failure_window_seconds: int = 600
    shutdown_deadline_seconds: int = 60
    stage_subset: frozenset[str] = OPTIONS_RESIDENT_STAGE_SUBSET

    def __post_init__(self) -> None:
        if (
            not self.version.strip()
            or type(self.heartbeat_interval_seconds) is not int
            or type(self.stale_after_seconds) is not int
            or type(self.restart_initial_backoff_seconds) is not int
            or type(self.restart_maximum_backoff_seconds) is not int
            or type(self.restart_failure_limit) is not int
            or type(self.restart_failure_window_seconds) is not int
            or type(self.shutdown_deadline_seconds) is not int
            or min(
                self.heartbeat_interval_seconds,
                self.stale_after_seconds,
                self.restart_initial_backoff_seconds,
                self.restart_maximum_backoff_seconds,
                self.restart_failure_limit,
                self.restart_failure_window_seconds,
                self.shutdown_deadline_seconds,
            )
            <= 0
        ):
            raise ValueError("Advanced Options resident policy is invalid")
        if (
            self.stale_after_seconds
            < self.heartbeat_interval_seconds * 3
            or self.restart_maximum_backoff_seconds
            < self.restart_initial_backoff_seconds
            or self.stage_subset != OPTIONS_RESIDENT_STAGE_SUBSET
        ):
            raise ValueError("Advanced Options resident bounds changed")

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)

    def restart_delay_seconds(self, failure_number: int) -> int:
        if type(failure_number) is not int or failure_number < 1:
            raise ValueError("failure number must be positive")
        return min(
            self.restart_maximum_backoff_seconds,
            self.restart_initial_backoff_seconds
            * (2 ** (failure_number - 1)),
        )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsResidentPlan:
    plan_id: UUID
    cohort_id: UUID
    session_date: date
    policy: AdvancedOptionsResidentPolicy
    processes: tuple[AdvancedOptionsProcessSpec, ...]
    p7_manifest_id: UUID | None = None
    p7_report_sha256: str | None = None
    activation_authorized: bool = False

    def __post_init__(self) -> None:
        expected = tuple(AdvancedOptionsProcessKind)
        if (
            not isinstance(self.session_date, date)
            or isinstance(self.session_date, datetime)
            or
            tuple(row.kind for row in self.processes) != expected
            or len({row.owner_name for row in self.processes})
            != len(self.processes)
            or len({row.lease_resource_key for row in self.processes})
            != len(self.processes)
        ):
            raise ValueError(
                "Advanced Options resident process ownership is not exact"
            )
        if self.activation_authorized and (
            self.p7_manifest_id is None
            or self.p7_report_sha256 is None
            or len(self.p7_report_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.p7_report_sha256
            )
        ):
            raise ValueError(
                "Advanced Options activation requires exact passing P7 evidence"
            )

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsNonServingQualificationPlan:
    service_plan: ServicePlan
    p7_manifest_id: UUID
    p7_report_sha256: str
    publication_authorized: bool = False
    delivery_authorized: bool = False

    def __post_init__(self) -> None:
        if (
            self.service_plan.stage_catalog_version
            != STAGE_CATALOG_VERSION_V4
            or self.service_plan.enabled_stages
            != OPTIONS_RESIDENT_STAGE_SUBSET
            or len(self.p7_report_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.p7_report_sha256
            )
            or self.publication_authorized
            or self.delivery_authorized
        ):
            raise ValueError(
                "Advanced Options non-serving qualification scope changed"
            )

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsProcessHealth:
    kind: AdvancedOptionsProcessKind
    owner_id: UUID
    owner_epoch: int
    heartbeat_at: datetime
    consecutive_failures: int
    failure_window_started_at: datetime | None
    next_restart_at: datetime | None

    def __post_init__(self) -> None:
        heartbeat = _utc(self.heartbeat_at, "heartbeat_at")
        if (
            type(self.owner_epoch) is not int
            or self.owner_epoch < 1
            or type(self.consecutive_failures) is not int
            or self.consecutive_failures < 0
        ):
            raise ValueError("Advanced Options process health is invalid")
        object.__setattr__(self, "heartbeat_at", heartbeat)
        if self.failure_window_started_at is not None:
            object.__setattr__(
                self,
                "failure_window_started_at",
                _utc(
                    self.failure_window_started_at,
                    "failure_window_started_at",
                ),
            )
        if self.next_restart_at is not None:
            object.__setattr__(
                self,
                "next_restart_at",
                _utc(self.next_restart_at, "next_restart_at"),
            )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsHealthVerdict:
    status: str
    reason_codes: tuple[str, ...]
    checked_at: datetime


def evaluate_advanced_options_process_health(
    health: AdvancedOptionsProcessHealth,
    policy: AdvancedOptionsResidentPolicy,
    *,
    checked_at: datetime,
) -> AdvancedOptionsHealthVerdict:
    checked = _utc(checked_at, "checked_at")
    reasons = []
    if checked - health.heartbeat_at > timedelta(
        seconds=policy.stale_after_seconds
    ):
        reasons.append("HEARTBEAT_STALE")
    if (
        health.failure_window_started_at is not None
        and checked - health.failure_window_started_at
        <= timedelta(seconds=policy.restart_failure_window_seconds)
        and health.consecutive_failures >= policy.restart_failure_limit
    ):
        reasons.append("RESTART_BUDGET_EXHAUSTED")
    if (
        health.next_restart_at is not None
        and checked < health.next_restart_at
    ):
        reasons.append("RESTART_BACKOFF_ACTIVE")
    return AdvancedOptionsHealthVerdict(
        "READY" if not reasons else "UNAVAILABLE",
        tuple(reasons),
        checked,
    )


def build_provider_free_advanced_options_resident_plan(
    *,
    plan_id: UUID,
    cohort_id: UUID,
    session_date,
) -> AdvancedOptionsResidentPlan:
    policy = AdvancedOptionsResidentPolicy(
        "advanced-options-resident-v1",
    )
    prefix = f"advanced-options:{cohort_id}:{session_date}"
    return AdvancedOptionsResidentPlan(
        plan_id=plan_id,
        cohort_id=cohort_id,
        session_date=session_date,
        policy=policy,
        processes=(
            AdvancedOptionsProcessSpec(
                AdvancedOptionsProcessKind.INGRESS,
                "advanced-options-ingress-v1",
                f"{prefix}:ingress",
            ),
            AdvancedOptionsProcessSpec(
                AdvancedOptionsProcessKind.MARKET_STATE,
                "market-state-materializer-v1",
                f"{prefix}:market-state",
            ),
            AdvancedOptionsProcessSpec(
                AdvancedOptionsProcessKind.FLOW,
                "option-flow-stream-v1",
                f"{prefix}:flow",
            ),
            AdvancedOptionsProcessSpec(
                AdvancedOptionsProcessKind.WINDOW,
                "advanced-options-window-v1",
                f"{prefix}:window",
            ),
        ),
    )


def build_advanced_options_non_serving_qualification_plan(
    *,
    plan_id: UUID,
    cohort_id: UUID,
    session_date: date,
    config_sha256: str,
    deadline: datetime,
    p7_manifest_id: UUID,
    p7_report_sha256: str,
) -> AdvancedOptionsNonServingQualificationPlan:
    if (
        len(config_sha256) != 64
        or any(
            character not in "0123456789abcdef"
            for character in config_sha256
        )
        or deadline.tzinfo is None
        or deadline.utcoffset() is None
        or deadline.date() != session_date
    ):
        raise ValueError(
            "Advanced Options qualification service plan is invalid"
        )
    return AdvancedOptionsNonServingQualificationPlan(
        ServicePlan(
            plan_id,
            cohort_id,
            session_date,
            STAGE_CATALOG_VERSION_V4,
            OPTIONS_RESIDENT_STAGE_SUBSET,
            config_sha256,
            deadline.astimezone(timezone.utc),
        ),
        p7_manifest_id,
        p7_report_sha256,
    )


class AdvancedOptionsLeasePort(Protocol):
    def acquire(
        self,
        resource_key: str,
        owner_id: UUID,
        *,
        lease_seconds: int,
        now: datetime,
    ) -> int | None: ...

    def renew(
        self,
        resource_key: str,
        owner_id: UUID,
        owner_epoch: int,
        *,
        lease_seconds: int,
        now: datetime,
    ) -> bool: ...

    def release(
        self,
        resource_key: str,
        owner_id: UUID,
        owner_epoch: int,
    ) -> None: ...


class AdvancedOptionsProcessPort(Protocol):
    def start(self, spec: AdvancedOptionsProcessSpec): ...

    def alive(self, handle) -> bool: ...

    def stop(self, handle, *, deadline_seconds: int) -> None: ...


@dataclass(slots=True)
class _ManagedProcess:
    spec: AdvancedOptionsProcessSpec
    owner_id: UUID
    owner_epoch: int
    handle: object
    heartbeat_at: datetime


class AdvancedOptionsResidentSupervisor:
    """Lease-fenced four-process supervisor with bounded restart policy."""

    def __init__(
        self,
        plan: AdvancedOptionsResidentPlan,
        leases: AdvancedOptionsLeasePort,
        processes: AdvancedOptionsProcessPort,
        *,
        owner_ids: dict[AdvancedOptionsProcessKind, UUID],
    ) -> None:
        if not plan.activation_authorized:
            raise ValueError(
                "Advanced Options resident activation lacks passing P7 authority"
            )
        if set(owner_ids) != set(AdvancedOptionsProcessKind):
            raise ValueError("Advanced Options owner membership is not exact")
        self._plan = plan
        self._leases = leases
        self._processes = processes
        self._owner_ids = dict(owner_ids)
        self._managed: dict[AdvancedOptionsProcessKind, _ManagedProcess] = {}
        self._failures: dict[
            AdvancedOptionsProcessKind,
            list[datetime],
        ] = {kind: [] for kind in AdvancedOptionsProcessKind}
        self._next_restart: dict[
            AdvancedOptionsProcessKind,
            datetime | None,
        ] = {kind: None for kind in AdvancedOptionsProcessKind}

    def _trim_failures(
        self,
        kind: AdvancedOptionsProcessKind,
        now: datetime,
    ) -> list[datetime]:
        boundary = now - timedelta(
            seconds=self._plan.policy.restart_failure_window_seconds
        )
        failures = [
            value for value in self._failures[kind]
            if value >= boundary
        ]
        self._failures[kind] = failures
        return failures

    def _record_failure(
        self,
        kind: AdvancedOptionsProcessKind,
        now: datetime,
    ) -> None:
        failures = self._trim_failures(kind, now)
        failures.append(now)
        self._failures[kind] = failures
        delay = self._plan.policy.restart_delay_seconds(len(failures))
        self._next_restart[kind] = now + timedelta(seconds=delay)

    def _stop(
        self,
        kind: AdvancedOptionsProcessKind,
        *,
        failed_at: datetime | None,
    ) -> None:
        managed = self._managed.pop(kind, None)
        if managed is None:
            return
        try:
            self._processes.stop(
                managed.handle,
                deadline_seconds=(
                    self._plan.policy.shutdown_deadline_seconds
                ),
            )
        finally:
            self._leases.release(
                managed.spec.lease_resource_key,
                managed.owner_id,
                managed.owner_epoch,
            )
        if failed_at is not None:
            self._record_failure(kind, failed_at)

    def tick(self, *, now: datetime) -> None:
        checked = _utc(now, "now")
        for spec in self._plan.processes:
            managed = self._managed.get(spec.kind)
            if managed is not None:
                stale = checked - managed.heartbeat_at > timedelta(
                    seconds=self._plan.policy.stale_after_seconds
                )
                if stale or not self._processes.alive(managed.handle):
                    self._stop(spec.kind, failed_at=checked)
                    continue
                if not self._leases.renew(
                    spec.lease_resource_key,
                    managed.owner_id,
                    managed.owner_epoch,
                    lease_seconds=self._plan.policy.stale_after_seconds,
                    now=checked,
                ):
                    self._stop(spec.kind, failed_at=checked)
                continue
            failures = self._trim_failures(spec.kind, checked)
            restart_at = self._next_restart[spec.kind]
            if (
                len(failures) >= self._plan.policy.restart_failure_limit
                or restart_at is not None
                and checked < restart_at
            ):
                continue
            owner_id = self._owner_ids[spec.kind]
            epoch = self._leases.acquire(
                spec.lease_resource_key,
                owner_id,
                lease_seconds=self._plan.policy.stale_after_seconds,
                now=checked,
            )
            if epoch is None:
                continue
            try:
                handle = self._processes.start(spec)
            except Exception:
                self._leases.release(
                    spec.lease_resource_key,
                    owner_id,
                    epoch,
                )
                self._record_failure(spec.kind, checked)
                raise
            self._managed[spec.kind] = _ManagedProcess(
                spec,
                owner_id,
                epoch,
                handle,
                checked,
            )
            self._next_restart[spec.kind] = None

    def heartbeat(
        self,
        kind: AdvancedOptionsProcessKind,
        owner_id: UUID,
        owner_epoch: int,
        *,
        observed_at: datetime,
    ) -> None:
        managed = self._managed.get(kind)
        observed = _utc(observed_at, "observed_at")
        if (
            managed is None
            or managed.owner_id != owner_id
            or managed.owner_epoch != owner_epoch
            or observed < managed.heartbeat_at
        ):
            raise ValueError("Advanced Options heartbeat ownership changed")
        managed.heartbeat_at = observed

    def stop(self) -> None:
        for kind in tuple(self._managed):
            self._stop(kind, failed_at=None)

    def health(
        self,
        *,
        checked_at: datetime,
    ) -> dict[AdvancedOptionsProcessKind, AdvancedOptionsHealthVerdict]:
        checked = _utc(checked_at, "checked_at")
        verdicts = {}
        for spec in self._plan.processes:
            managed = self._managed.get(spec.kind)
            failures = self._trim_failures(spec.kind, checked)
            if managed is None:
                verdicts[spec.kind] = AdvancedOptionsHealthVerdict(
                    "UNAVAILABLE",
                    (
                        "RESTART_BUDGET_EXHAUSTED"
                        if len(failures)
                        >= self._plan.policy.restart_failure_limit
                        else "PROCESS_NOT_RUNNING",
                    ),
                    checked,
                )
                continue
            verdicts[spec.kind] = evaluate_advanced_options_process_health(
                AdvancedOptionsProcessHealth(
                    spec.kind,
                    managed.owner_id,
                    managed.owner_epoch,
                    managed.heartbeat_at,
                    len(failures),
                    min(failures) if failures else None,
                    self._next_restart[spec.kind],
                ),
                self._plan.policy,
                checked_at=checked,
            )
        return verdicts
