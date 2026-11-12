"""PostgreSQL fencing for Advanced Realtime V3 runtime owners."""
from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Mapping
from uuid import UUID

from psycopg2 import Error as PsycopgError
from psycopg2.extras import Json

from .database_access_v2 import APP_ROLE, V2ConnectionProvider


UTC = timezone.utc
_LOGGER = logging.getLogger(__name__)
RUNTIME_GROUP_RESOURCE = "advanced-realtime-v3:runtime"
OPTIONS_PHYSICAL_SOCKET_RESOURCE = (
    "advanced-realtime-v3:options-physical-socket"
)
ROLE_RESOURCE_PREFIX = "advanced-realtime-v3:role:"


class DurableLeaseUnavailable(RuntimeError):
    pass


class DurableLeaseLost(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class DurableRuntimeLease:
    resource_key: str
    owner_id: UUID
    owner_epoch: int
    lease_until: datetime

    def __post_init__(self) -> None:
        key = self.resource_key.strip()
        if (
            not key
            or not isinstance(self.owner_id, UUID)
            or self.owner_id.int == 0
            or type(self.owner_epoch) is not int
            or self.owner_epoch < 1
            or self.lease_until.tzinfo is None
            or self.lease_until.utcoffset() is None
        ):
            raise ValueError("durable runtime lease is invalid")
        object.__setattr__(self, "resource_key", key)
        object.__setattr__(
            self,
            "lease_until",
            self.lease_until.astimezone(UTC),
        )


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def role_resource_key(role_name: str) -> str:
    role = role_name.strip()
    if not role or len(role) > 64:
        raise ValueError("Advanced V3 role name is invalid")
    return ROLE_RESOURCE_PREFIX + role


def claim_durable_runtime_lease_cursor(
    cursor,
    *,
    resource_key: str,
    owner_id: UUID,
    checked_at: datetime,
    lease_seconds: int,
) -> DurableRuntimeLease | None:
    checked = _utc(checked_at, "durable lease checked_at")
    key = resource_key.strip()
    if (
        not key
        or not isinstance(owner_id, UUID)
        or owner_id.int == 0
        or not 10 <= lease_seconds <= 3600
    ):
        raise ValueError("durable lease claim scope is invalid")
    lease_until = checked + timedelta(seconds=lease_seconds)
    cursor.execute(
        "INSERT INTO rt.worker_leases ("
        "resource_key,owner_id,owner_epoch,lease_until,updated_at"
        ") VALUES (%s,%s,1,%s,%s) "
        "ON CONFLICT (resource_key) DO UPDATE SET "
        "owner_id=EXCLUDED.owner_id,"
        "owner_epoch=rt.worker_leases.owner_epoch+1,"
        "lease_until=EXCLUDED.lease_until,"
        "updated_at=EXCLUDED.updated_at "
        "WHERE rt.worker_leases.lease_until<=%s "
        "RETURNING owner_epoch,lease_until",
        (
            key,
            str(owner_id),
            lease_until,
            checked,
            checked,
        ),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return DurableRuntimeLease(
        key,
        owner_id,
        int(row[0]),
        _utc(row[1], "stored durable lease_until"),
    )


def renew_durable_runtime_lease_cursor(
    cursor,
    lease: DurableRuntimeLease,
    *,
    checked_at: datetime,
    lease_seconds: int,
) -> DurableRuntimeLease | None:
    checked = _utc(checked_at, "durable lease checked_at")
    if not 10 <= lease_seconds <= 3600:
        raise ValueError("durable lease duration is invalid")
    lease_until = checked + timedelta(seconds=lease_seconds)
    cursor.execute(
        "UPDATE rt.worker_leases SET lease_until=%s,updated_at=%s "
        "WHERE resource_key=%s AND owner_id=%s AND owner_epoch=%s "
        "AND lease_until>%s RETURNING lease_until",
        (
            lease_until,
            checked,
            lease.resource_key,
            str(lease.owner_id),
            lease.owner_epoch,
            checked,
        ),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return DurableRuntimeLease(
        lease.resource_key,
        lease.owner_id,
        lease.owner_epoch,
        _utc(row[0], "stored durable lease_until"),
    )


def release_durable_runtime_lease_cursor(
    cursor,
    lease: DurableRuntimeLease,
    *,
    checked_at: datetime,
) -> bool:
    checked = _utc(checked_at, "durable lease released_at")
    cursor.execute(
        "UPDATE rt.worker_leases SET lease_until=%s,updated_at=%s "
        "WHERE resource_key=%s AND owner_id=%s AND owner_epoch=%s",
        (
            checked,
            checked,
            lease.resource_key,
            str(lease.owner_id),
            lease.owner_epoch,
        ),
    )
    return cursor.rowcount == 1


def assert_durable_runtime_lease_cursor(
    cursor,
    lease: DurableRuntimeLease,
    *,
    checked_at: datetime,
) -> None:
    checked = _utc(checked_at, "durable lease checked_at")
    cursor.execute(
        "SELECT 1 FROM rt.worker_leases "
        "WHERE resource_key=%s AND owner_id=%s AND owner_epoch=%s "
        "AND lease_until>%s",
        (
            lease.resource_key,
            str(lease.owner_id),
            lease.owner_epoch,
            checked,
        ),
    )
    if cursor.fetchone() is None:
        raise DurableLeaseLost(
            f"durable runtime lease lost: {lease.resource_key}"
        )


def record_advanced_realtime_service_state_cursor(
    cursor,
    *,
    role_name: str,
    runtime_kind: str,
    process_identity: str,
    state: str,
    health: str,
    lease: DurableRuntimeLease,
    observed_at: datetime,
    detail: Mapping[str, object] | None = None,
) -> None:
    observed = _utc(observed_at, "service state observed_at")
    if (
        role_name.strip() != role_name
        or runtime_kind not in {"NATIVE", "COMPOSE"}
        or not process_identity.strip()
        or state
        not in {
            "STARTING",
            "RUNNING",
            "WAITING",
            "DEGRADED",
            "FAILED_TERMINAL",
            "DRAINING",
            "STOPPED",
        }
        or health
        not in {
            "STARTING",
            "HEALTHY",
            "DEGRADED",
            "UNHEALTHY",
            "STOPPED",
        }
        or lease.lease_until < observed
    ):
        raise ValueError("Advanced V3 service state is invalid")
    assert_durable_runtime_lease_cursor(
        cursor,
        lease,
        checked_at=observed,
    )
    cursor.execute(
        "INSERT INTO rt.advanced_realtime_service_states ("
        "role_name,runtime_kind,owner_id,owner_epoch,process_identity,"
        "state,health,lease_until,observed_at,detail"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (role_name) DO UPDATE SET "
        "runtime_kind=EXCLUDED.runtime_kind,"
        "owner_id=EXCLUDED.owner_id,"
        "owner_epoch=EXCLUDED.owner_epoch,"
        "process_identity=EXCLUDED.process_identity,"
        "state=EXCLUDED.state,health=EXCLUDED.health,"
        "lease_until=EXCLUDED.lease_until,"
        "observed_at=EXCLUDED.observed_at,"
        "detail=EXCLUDED.detail,updated_at=clock_timestamp() "
        "WHERE rt.advanced_realtime_service_states.observed_at"
        "<=EXCLUDED.observed_at",
        (
            role_name,
            runtime_kind,
            str(lease.owner_id),
            lease.owner_epoch,
            process_identity,
            state,
            health,
            lease.lease_until,
            observed,
            Json(dict(detail or {})),
        ),
    )


class PostgresLeaseHeartbeat:
    def __init__(
        self,
        provider: V2ConnectionProvider,
        *,
        resource_key: str,
        owner_id: UUID,
        lease_seconds: int = 30,
        runtime_kind: str | None = None,
        role_name: str | None = None,
        process_identity: str | None = None,
        stop_event: threading.Event | None = None,
        clock=lambda: datetime.now(UTC),
    ) -> None:
        if not isinstance(provider, V2ConnectionProvider):
            raise TypeError("durable lease provider is invalid")
        if (
            not 10 <= lease_seconds <= 3600
            or not isinstance(owner_id, UUID)
            or owner_id.int == 0
        ):
            raise ValueError("durable lease heartbeat scope is invalid")
        if any(
            value is not None
            for value in (runtime_kind, role_name, process_identity)
        ) and any(
            value is None
            for value in (runtime_kind, role_name, process_identity)
        ):
            raise ValueError(
                "service-state heartbeat identity must be complete"
            )
        self._provider = provider
        self._resource_key = resource_key
        self._owner_id = owner_id
        self._lease_seconds = lease_seconds
        self._runtime_kind = runtime_kind
        self._role_name = role_name
        self._process_identity = process_identity
        self._external_stop = stop_event
        self._clock = clock
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._state_lock = threading.Lock()
        self._state = "STARTING"
        self._health = "STARTING"
        self._detail: dict[str, object] = {}
        self._thread: threading.Thread | None = None
        self.lease: DurableRuntimeLease | None = None

    @property
    def lost(self) -> bool:
        return self._lost.is_set()

    def _record(self) -> None:
        if self._runtime_kind is None or self.lease is None:
            return
        with self._state_lock:
            state = self._state
            health = self._health
            detail = dict(self._detail)
        with self._provider.transaction(APP_ROLE) as unit:
            record_advanced_realtime_service_state_cursor(
                unit.cursor,
                role_name=self._role_name,
                runtime_kind=self._runtime_kind,
                process_identity=self._process_identity,
                state=state,
                health=health,
                lease=self.lease,
                observed_at=self._clock(),
                detail=detail,
            )

    def start(self) -> DurableRuntimeLease:
        if self._thread is not None:
            raise RuntimeError("durable lease heartbeat already started")
        now = self._clock()
        with self._provider.transaction(APP_ROLE) as unit:
            lease = claim_durable_runtime_lease_cursor(
                unit.cursor,
                resource_key=self._resource_key,
                owner_id=self._owner_id,
                checked_at=now,
                lease_seconds=self._lease_seconds,
            )
        if lease is None:
            raise DurableLeaseUnavailable(
                f"durable runtime lease unavailable: "
                f"{self._resource_key}"
            )
        self.lease = lease
        self._record()
        self._thread = threading.Thread(
            target=self._run,
            name="advanced-realtime-v3-lease-heartbeat",
            daemon=True,
        )
        self._thread.start()
        return lease

    def mark_running(self) -> None:
        self.mark_state("RUNNING", "HEALTHY")

    def mark_state(
        self,
        state: str,
        health: str,
        *,
        detail: Mapping[str, object] | None = None,
    ) -> None:
        with self._state_lock:
            self._state = state
            self._health = health
            self._detail = dict(detail or {})
        self._record()

    def _renew_once(self):
        now = self._clock()
        with self._provider.transaction(APP_ROLE) as unit:
            return renew_durable_runtime_lease_cursor(
                unit.cursor,
                self.lease,
                checked_at=now,
                lease_seconds=self._lease_seconds,
            )

    def _renew(self):
        """R43 (Nov 11): one renewal, and one retry after a database error while the lease still has time left. Returns
        (lease or None, reason). A renewal the database refuses (the lease expired or another owner holds it) is final."""
        try:
            renewed = self._renew_once()
        except PsycopgError as error:
            first = f"{type(error).__name__}: {str(error).strip()[:200]}"
            remaining = (self.lease.lease_until - self._clock()).total_seconds()
            if remaining <= 2.0:
                return None, f"RENEWAL_ERROR {first} (lease ends in {remaining:.1f} s, no retry)"
            _LOGGER.warning(
                "durable_lease_renewal_retry resource=%s error=%s lease_ends_in=%.1fs",
                self._resource_key, first, remaining,
            )
            if self._stop.wait(min(2.0, remaining / 2)):
                return self.lease, "STOPPING"
            try:
                renewed = self._renew_once()
            except PsycopgError as retry_error:
                return None, (
                    f"RENEWAL_ERROR {first}; retry {type(retry_error).__name__}: {str(retry_error).strip()[:200]}"
                )
        if renewed is None:
            return None, "LEASE_NOT_RENEWED (expired or held by another owner)"
        return renewed, None

    def _run(self) -> None:
        interval = max(1.0, self._lease_seconds / 3)
        while not self._stop.wait(interval):
            renewed, reason = self._renew()
            if renewed is None:
                # R43: this stops the role (the wrapper ends its child and the container restarts); say why.
                _LOGGER.error(
                    "durable_lease_lost resource=%s owner=%s epoch=%s reason=%s; stopping the role",
                    self._resource_key, self._owner_id,
                    getattr(self.lease, "owner_epoch", None), reason,
                )
                self._lost.set()
                if self._external_stop is not None:
                    self._external_stop.set()
                return
            if reason == "STOPPING":
                return
            self.lease = renewed
            try:
                self._record()
            except PsycopgError as error:
                # The service-state row is advisory; a failed write must not end renewals (it used to end this thread
                # with only a traceback, leaving the role running while its lease ran out).
                _LOGGER.warning(
                    "durable_lease_state_record_failed resource=%s error=%s: %s",
                    self._resource_key, type(error).__name__, str(error).strip()[:200],
                )

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(2.0, self._lease_seconds / 2))
        if self.lease is not None and not self._lost.is_set():
            try:
                self.mark_state("STOPPED", "STOPPED")
            finally:
                with self._provider.transaction(APP_ROLE) as unit:
                    release_durable_runtime_lease_cursor(
                        unit.cursor,
                        self.lease,
                        checked_at=self._clock(),
                    )
        self._thread = None
