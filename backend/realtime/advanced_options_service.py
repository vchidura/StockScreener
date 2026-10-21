"""Provider-free resident service topology and health contracts for Options."""
from __future__ import annotations

import os
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from threading import Lock
from typing import Protocol
from uuid import UUID

from psycopg2 import Error as PsycopgError

from .contracts import canonical_sha256
from .database_access_v2 import V2ConnectionProvider
from .orchestration_repository import ServicePlan
from .option_source_flow import CANONICAL_OPTION_FLOW_OWNER
from .provider_socket_lease import (
    MASSIVE_OPTIONS_SOCKET_RESOURCE,
    HostProviderSocketLease,
    ProviderSocketLeaseUnavailable,
    default_provider_lock_path,
)
from .stage_graph import STAGE_CATALOG_VERSION_V4


PASSING_P7_MANIFEST_ID = UUID("c1136851-b496-5efe-aaa7-1eb8c8a7efd5")
PASSING_P7_MANIFEST_SHA256 = (
    "8207919b125b973eed9d28dd2ae568ab226cb052f503ba853a4889dc27db4be1"
)
PASSING_P7_REPORT_SHA256 = (
    "4b5a24d95fe037d13a54153adbc7f2c0136469946047af41d8b98225da0f117c"
)
PASSING_P7_SOURCE_COMMIT = "071062a5e6ba585add9e8c59ef58b86d16f0c0ad"
ADVANCED_OPTIONS_COHORT_KIND = "MARKET_OPTIONS"
OPTIONS_NON_SERVING_STAGE_ORDER = (
    "OPTIONS_ACQUIRE",
    "OPTIONS_ADMISSION",
    "OPTIONS_FLOW",
    "OPTIONS_INPUT_GATE",
    "OPTIONS_CYCLE",
    "OPTIONS_PROJECTION",
    "OPTIONS_PUBLISH",
)
OPTIONS_RESIDENT_STAGE_SUBSET = frozenset(
    OPTIONS_NON_SERVING_STAGE_ORDER
)
_RETRYABLE_POSTGRES_SQLSTATES = frozenset({"40001", "40P01"})


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
    p7_manifest_sha256: str | None = None
    p7_report_sha256: str | None = None
    p7_source_commit: str | None = None
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
            or self.p7_manifest_sha256 is None
            or len(self.p7_manifest_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.p7_manifest_sha256
            )
            or self.p7_report_sha256 is None
            or len(self.p7_report_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.p7_report_sha256
            )
            or self.p7_source_commit is None
            or len(self.p7_source_commit) != 40
            or any(
                character not in "0123456789abcdef"
                for character in self.p7_source_commit
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
    p7_manifest_sha256: str
    p7_report_sha256: str
    p7_source_commit: str
    cohort_kind: str = ADVANCED_OPTIONS_COHORT_KIND
    provider_authorized: bool = False
    publication_authorized: bool = False
    release_authorized: bool = False
    pointer_authorized: bool = False
    outbox_authorized: bool = False
    delivery_authorized: bool = False
    execution_authorized: bool = False
    repair_authorized: bool = False
    backfill_authorized: bool = False
    archive_authorized: bool = False
    retention_authorized: bool = False

    def __post_init__(self) -> None:
        if (
            self.service_plan.stage_catalog_version
            != STAGE_CATALOG_VERSION_V4
            or self.service_plan.enabled_stages
            != OPTIONS_RESIDENT_STAGE_SUBSET
            or self.p7_manifest_id != PASSING_P7_MANIFEST_ID
            or self.p7_manifest_sha256 != PASSING_P7_MANIFEST_SHA256
            or len(self.p7_report_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.p7_report_sha256
            )
            or self.p7_report_sha256 != PASSING_P7_REPORT_SHA256
            or self.p7_source_commit != PASSING_P7_SOURCE_COMMIT
            or self.cohort_kind != ADVANCED_OPTIONS_COHORT_KIND
            or any(self.effect_authorizations.values())
        ):
            raise ValueError(
                "Advanced Options non-serving qualification scope changed"
            )

    @property
    def effect_authorizations(self) -> dict[str, bool]:
        return {
            "provider": self.provider_authorized,
            "publication": self.publication_authorized,
            "release": self.release_authorized,
            "pointer": self.pointer_authorized,
            "outbox": self.outbox_authorized,
            "delivery": self.delivery_authorized,
            "execution": self.execution_authorized,
            "repair": self.repair_authorized,
            "backfill": self.backfill_authorized,
            "archive": self.archive_authorized,
            "retention": self.retention_authorized,
        }

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
                MASSIVE_OPTIONS_SOCKET_RESOURCE,
            ),
            AdvancedOptionsProcessSpec(
                AdvancedOptionsProcessKind.MARKET_STATE,
                "market-state-materializer-v1",
                f"{prefix}:market-state",
            ),
            AdvancedOptionsProcessSpec(
                AdvancedOptionsProcessKind.FLOW,
                CANONICAL_OPTION_FLOW_OWNER,
                f"{prefix}:flow",
            ),
            AdvancedOptionsProcessSpec(
                AdvancedOptionsProcessKind.WINDOW,
                "advanced-options-window-v1",
                f"{prefix}:window",
            ),
        ),
    )


def build_owp3_advanced_options_resident_plan(
    *,
    plan_id: UUID,
    cohort_id: UUID,
    session_date: date,
) -> AdvancedOptionsResidentPlan:
    provider_free = build_provider_free_advanced_options_resident_plan(
        plan_id=plan_id,
        cohort_id=cohort_id,
        session_date=session_date,
    )
    return AdvancedOptionsResidentPlan(
        plan_id=provider_free.plan_id,
        cohort_id=provider_free.cohort_id,
        session_date=provider_free.session_date,
        policy=provider_free.policy,
        processes=provider_free.processes,
        p7_manifest_id=PASSING_P7_MANIFEST_ID,
        p7_manifest_sha256=PASSING_P7_MANIFEST_SHA256,
        p7_report_sha256=PASSING_P7_REPORT_SHA256,
        p7_source_commit=PASSING_P7_SOURCE_COMMIT,
        activation_authorized=True,
    )


def build_advanced_options_non_serving_qualification_plan(
    *,
    plan_id: UUID,
    cohort_id: UUID,
    session_date: date,
    config_sha256: str,
    deadline: datetime,
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
        PASSING_P7_MANIFEST_ID,
        PASSING_P7_MANIFEST_SHA256,
        PASSING_P7_REPORT_SHA256,
        PASSING_P7_SOURCE_COMMIT,
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


class AdvancedOptionsPostgresLeasePort:
    """Epoch-fenced ownership in rt.worker_leases with bounded transaction retry."""

    def __init__(
        self,
        database: V2ConnectionProvider,
        *,
        maximum_attempts: int = 3,
        retry_initial_seconds: float = 0.05,
        wait: Callable[[float], None] = time.sleep,
    ) -> None:
        if (
            type(maximum_attempts) is not int
            or maximum_attempts < 1
            or retry_initial_seconds < 0
            or not callable(wait)
        ):
            raise ValueError("Advanced Options PostgreSQL lease retry is invalid")
        self._database = database
        self._maximum_attempts = maximum_attempts
        self._retry_initial_seconds = retry_initial_seconds
        self._wait = wait

    def _execute(self, operation: Callable) -> object:
        for attempt in range(1, self._maximum_attempts + 1):
            try:
                with self._database.transaction() as unit:
                    return operation(unit.cursor)
            except PsycopgError as error:
                if (
                    error.pgcode not in _RETRYABLE_POSTGRES_SQLSTATES
                    or attempt == self._maximum_attempts
                ):
                    raise
                self._wait(
                    self._retry_initial_seconds * (2 ** (attempt - 1))
                )
        raise AssertionError("unreachable PostgreSQL lease retry state")

    def acquire(
        self,
        resource_key: str,
        owner_id: UUID,
        *,
        lease_seconds: int,
        now: datetime,
    ) -> int | None:
        checked = _utc(now, "now")
        if (
            not resource_key.strip()
            or type(lease_seconds) is not int
            or lease_seconds <= 0
        ):
            raise ValueError("Advanced Options lease acquisition is invalid")

        def operation(cursor):
            cursor.execute(
                "INSERT INTO rt.worker_leases "
                "(resource_key,owner_id,owner_epoch,lease_until,updated_at) "
                "VALUES (%s,%s,1,%s,%s) "
                "ON CONFLICT (resource_key) DO UPDATE SET "
                "owner_id=EXCLUDED.owner_id,"
                "owner_epoch=rt.worker_leases.owner_epoch+1,"
                "lease_until=EXCLUDED.lease_until,"
                "updated_at=EXCLUDED.updated_at "
                "WHERE rt.worker_leases.lease_until<=%s "
                "RETURNING owner_epoch",
                (
                    resource_key,
                    str(owner_id),
                    checked + timedelta(seconds=lease_seconds),
                    checked,
                    checked,
                ),
            )
            row = cursor.fetchone()
            return None if row is None else int(row[0])

        return self._execute(operation)

    def renew(
        self,
        resource_key: str,
        owner_id: UUID,
        owner_epoch: int,
        *,
        lease_seconds: int,
        now: datetime,
    ) -> bool:
        checked = _utc(now, "now")
        if (
            type(owner_epoch) is not int
            or owner_epoch < 1
            or type(lease_seconds) is not int
            or lease_seconds <= 0
        ):
            raise ValueError("Advanced Options lease renewal is invalid")

        def operation(cursor):
            cursor.execute(
                "UPDATE rt.worker_leases SET lease_until=%s,updated_at=%s "
                "WHERE resource_key=%s AND owner_id=%s AND owner_epoch=%s "
                "AND lease_until>%s",
                (
                    checked + timedelta(seconds=lease_seconds),
                    checked,
                    resource_key,
                    str(owner_id),
                    owner_epoch,
                    checked,
                ),
            )
            return cursor.rowcount == 1

        return bool(self._execute(operation))

    def release(
        self,
        resource_key: str,
        owner_id: UUID,
        owner_epoch: int,
    ) -> None:
        if type(owner_epoch) is not int or owner_epoch < 1:
            raise ValueError("Advanced Options lease release is invalid")

        def operation(cursor):
            cursor.execute(
                "UPDATE rt.worker_leases SET "
                "lease_until=LEAST(lease_until,clock_timestamp()),"
                "updated_at=clock_timestamp() "
                "WHERE resource_key=%s AND owner_id=%s AND owner_epoch=%s",
                (resource_key, str(owner_id), owner_epoch),
            )

        self._execute(operation)


class AdvancedOptionsHostFencedLeasePort:
    """Adds the mandatory host lock to the PostgreSQL ingress lease."""

    def __init__(
        self,
        postgres: AdvancedOptionsLeasePort,
        *,
        socket_lock_path: Path | None = None,
        cleanup_host_artifacts: bool = False,
    ) -> None:
        self._postgres = postgres
        self._socket_lock_path = (
            default_provider_lock_path()
            if socket_lock_path is None
            else socket_lock_path.resolve()
        )
        self._cleanup_host_artifacts = cleanup_host_artifacts
        self._host_leases: dict[
            tuple[UUID, int],
            HostProviderSocketLease,
        ] = {}
        self._lock = Lock()

    def acquire(
        self,
        resource_key: str,
        owner_id: UUID,
        *,
        lease_seconds: int,
        now: datetime,
    ) -> int | None:
        epoch = self._postgres.acquire(
            resource_key,
            owner_id,
            lease_seconds=lease_seconds,
            now=now,
        )
        if epoch is None or resource_key != MASSIVE_OPTIONS_SOCKET_RESOURCE:
            return epoch
        lease = HostProviderSocketLease(
            self._socket_lock_path,
            resource_key,
            owner_id,
        )
        try:
            lease.acquire(now=now)
        except ProviderSocketLeaseUnavailable:
            self._postgres.release(resource_key, owner_id, epoch)
            return None
        except Exception:
            self._postgres.release(resource_key, owner_id, epoch)
            raise
        with self._lock:
            self._host_leases[(owner_id, epoch)] = lease
        return epoch

    def renew(
        self,
        resource_key: str,
        owner_id: UUID,
        owner_epoch: int,
        *,
        lease_seconds: int,
        now: datetime,
    ) -> bool:
        if resource_key == MASSIVE_OPTIONS_SOCKET_RESOURCE:
            with self._lock:
                lease = self._host_leases.get((owner_id, owner_epoch))
            if lease is None or not lease.acquired:
                self._postgres.release(
                    resource_key,
                    owner_id,
                    owner_epoch,
                )
                return False
        return self._postgres.renew(
            resource_key,
            owner_id,
            owner_epoch,
            lease_seconds=lease_seconds,
            now=now,
        )

    def release(
        self,
        resource_key: str,
        owner_id: UUID,
        owner_epoch: int,
    ) -> None:
        lease = None
        if resource_key == MASSIVE_OPTIONS_SOCKET_RESOURCE:
            with self._lock:
                lease = self._host_leases.pop(
                    (owner_id, owner_epoch),
                    None,
                )
        try:
            if lease is not None:
                lease.release()
        finally:
            self._postgres.release(resource_key, owner_id, owner_epoch)
        if lease is not None and self._cleanup_host_artifacts:
            for path in (lease.metadata_path, lease.path):
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass


class AdvancedOptionsSubprocessPort:
    """Exact command launcher for the four isolated resident owners."""

    def __init__(
        self,
        commands: Mapping[
            AdvancedOptionsProcessKind,
            Sequence[str],
        ],
        *,
        cwd: Path,
        environment: Mapping[str, str],
        popen: Callable[..., subprocess.Popen] = subprocess.Popen,
    ) -> None:
        if set(commands) != set(AdvancedOptionsProcessKind):
            raise ValueError(
                "Advanced Options process command membership is not exact"
            )
        normalized = {
            kind: tuple(str(value).strip() for value in command)
            for kind, command in commands.items()
        }
        if any(
            not command or any(not value for value in command)
            for command in normalized.values()
        ):
            raise ValueError("Advanced Options process command is invalid")
        if not callable(popen):
            raise TypeError("Advanced Options process launcher is invalid")
        self._commands = normalized
        self._cwd = cwd.resolve()
        self._environment = dict(environment)
        self._popen = popen
        self._history: dict[
            AdvancedOptionsProcessKind,
            list[subprocess.Popen],
        ] = {kind: [] for kind in AdvancedOptionsProcessKind}

    def start(self, spec: AdvancedOptionsProcessSpec):
        options = {
            "cwd": self._cwd,
            "env": self._environment,
            "stdin": subprocess.DEVNULL,
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
        }
        if os.name == "nt":
            options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            options["start_new_session"] = True
        handle = self._popen(self._commands[spec.kind], **options)
        self._history[spec.kind].append(handle)
        return handle

    @property
    def process_history(
        self,
    ) -> Mapping[AdvancedOptionsProcessKind, tuple[subprocess.Popen, ...]]:
        return {
            kind: tuple(handles)
            for kind, handles in self._history.items()
        }

    @staticmethod
    def alive(handle) -> bool:
        return handle.poll() is None

    @staticmethod
    def stop(handle, *, deadline_seconds: int) -> None:
        if handle.poll() is not None:
            return
        handle.terminate()
        try:
            handle.wait(timeout=deadline_seconds)
        except subprocess.TimeoutExpired:
            handle.kill()
            handle.wait(timeout=deadline_seconds)


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
                else:
                    managed.heartbeat_at = checked
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
        failure = None
        for kind in tuple(self._managed):
            try:
                self._stop(kind, failed_at=None)
            except Exception as error:
                if failure is None:
                    failure = error
        if failure is not None:
            raise failure

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


class AdvancedOptionsResidentLauncher:
    """Concrete PostgreSQL/host-fenced launcher for the exact four owners."""

    def __init__(
        self,
        plan: AdvancedOptionsResidentPlan,
        database: V2ConnectionProvider,
        processes: AdvancedOptionsProcessPort,
        *,
        owner_ids: Mapping[AdvancedOptionsProcessKind, UUID],
        socket_lock_path: Path | None = None,
        cleanup_host_artifacts: bool = False,
        maximum_postgres_attempts: int = 3,
        postgres_retry_initial_seconds: float = 0.05,
        wait: Callable[[float], None] = time.sleep,
    ) -> None:
        postgres = AdvancedOptionsPostgresLeasePort(
            database,
            maximum_attempts=maximum_postgres_attempts,
            retry_initial_seconds=postgres_retry_initial_seconds,
            wait=wait,
        )
        self._leases = AdvancedOptionsHostFencedLeasePort(
            postgres,
            socket_lock_path=socket_lock_path,
            cleanup_host_artifacts=cleanup_host_artifacts,
        )
        self._supervisor = AdvancedOptionsResidentSupervisor(
            plan,
            self._leases,
            processes,
            owner_ids=dict(owner_ids),
        )

    def start(
        self,
        *,
        now: datetime,
    ) -> dict[AdvancedOptionsProcessKind, AdvancedOptionsHealthVerdict]:
        try:
            self._supervisor.tick(now=now)
        except Exception:
            self._supervisor.stop()
            raise
        health = self._supervisor.health(checked_at=now)
        if any(verdict.status != "READY" for verdict in health.values()):
            self._supervisor.stop()
            raise RuntimeError(
                "Advanced Options resident ownership is incomplete"
            )
        return health

    def tick(
        self,
        *,
        now: datetime,
    ) -> dict[AdvancedOptionsProcessKind, AdvancedOptionsHealthVerdict]:
        self._supervisor.tick(now=now)
        return self._supervisor.health(checked_at=now)

    def heartbeat(
        self,
        kind: AdvancedOptionsProcessKind,
        owner_id: UUID,
        owner_epoch: int,
        *,
        observed_at: datetime,
    ) -> None:
        self._supervisor.heartbeat(
            kind,
            owner_id,
            owner_epoch,
            observed_at=observed_at,
        )

    def stop(self) -> None:
        self._supervisor.stop()
