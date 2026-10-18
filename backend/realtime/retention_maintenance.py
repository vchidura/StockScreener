"""Execute-gated retention maintenance coordination and durable health status."""
from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable

from .partition_maintenance import PartitionAlarm
from .partition_retention import ELIGIBLE_RELATIONS, PartitionRetentionExecutor, RetentionReport
from .relation_registry import PARTITIONED_RELATIONS, RELATIONS
from .session_archive_worker import (
    STATUS_FAILED,
    ArchiveAlarm,
    ArchiveRunReport,
    DropReport,
    SessionArchiveWorker,
)
from .session_archive import SESSION_KINDS

LOGGER = logging.getLogger(__name__)

STATUS_SCHEMA = "realtime_v2_retention_status_v1"
STATUS_READY = "READY"
STATUS_ALARM = "ALARM"
HEALTH_READY = "READY"
HEALTH_DEGRADED = "DEGRADED"
HEALTH_BLOCKED = "BLOCKED"
MAX_STATUS_BYTES = 64 * 1024
MAX_RECORDED_ALARMS = 32

ARCHIVE_FIRST_RELATIONS = tuple(sorted(
    name for name, spec in RELATIONS.items()
    if spec.partitioned and spec.retention == "ARCHIVE_FIRST"
))


@dataclass(frozen=True, slots=True)
class RetentionMaintenanceRequest:
    as_of_date: date
    as_of_session: date
    archive_session_dates: tuple[date, ...]
    partition_relations: tuple[str, ...] = PARTITIONED_RELATIONS
    retention_relations: tuple[str, ...] = ELIGIBLE_RELATIONS
    archive_relations: tuple[str, ...] = ARCHIVE_FIRST_RELATIONS
    archive_kinds: tuple[str, ...] = SESSION_KINDS

    def __post_init__(self) -> None:
        sessions = tuple(sorted(set(self.archive_session_dates)))
        partition_relations = tuple(sorted(set(self.partition_relations)))
        retention_relations = tuple(sorted(set(self.retention_relations)))
        archive_relations = tuple(sorted(set(self.archive_relations)))
        archive_kinds = tuple(sorted(set(self.archive_kinds)))
        unknown_partitions = sorted(set(partition_relations) - set(PARTITIONED_RELATIONS))
        if unknown_partitions:
            raise ValueError(f"relations are not registered partitioned relations: {unknown_partitions}")
        if not partition_relations:
            raise ValueError("partition_relations cannot be empty")
        unknown_retention = sorted(set(retention_relations) - set(ELIGIBLE_RELATIONS))
        if unknown_retention:
            raise ValueError(f"relations are not ordinary-retention relations: {unknown_retention}")
        unknown_archive = sorted(set(archive_relations) - set(ARCHIVE_FIRST_RELATIONS))
        if unknown_archive:
            raise ValueError(f"relations are not partitioned ARCHIVE_FIRST relations: {unknown_archive}")
        if not retention_relations:
            raise ValueError("retention_relations cannot be empty")
        if not archive_relations:
            raise ValueError("archive_relations cannot be empty")
        unknown_kinds = sorted(set(archive_kinds) - set(SESSION_KINDS))
        if unknown_kinds:
            raise ValueError(f"unsupported archive kinds: {unknown_kinds}")
        if not archive_kinds:
            raise ValueError("archive_kinds cannot be empty")
        if sessions and sessions[-1] > self.as_of_session:
            raise ValueError("archive session dates cannot be later than as_of_session")
        object.__setattr__(self, "archive_session_dates", sessions)
        object.__setattr__(self, "partition_relations", partition_relations)
        object.__setattr__(self, "retention_relations", retention_relations)
        object.__setattr__(self, "archive_relations", archive_relations)
        object.__setattr__(self, "archive_kinds", archive_kinds)

    def as_dict(self) -> dict[str, object]:
        return {
            "as_of_date": self.as_of_date.isoformat(),
            "as_of_session": self.as_of_session.isoformat(),
            "archive_session_dates": [value.isoformat() for value in self.archive_session_dates],
            "partition_relations": list(self.partition_relations),
            "retention_relations": list(self.retention_relations),
            "archive_relations": list(self.archive_relations),
            "archive_kinds": list(self.archive_kinds),
        }


@dataclass(frozen=True, slots=True)
class MaintenanceAlarm:
    stage: str
    code: str
    detail: str
    relation: str | None = None

    def as_dict(self) -> dict[str, str | None]:
        return {
            "stage": self.stage,
            "code": self.code,
            "relation": self.relation,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class RetentionMaintenancePlan:
    request: RetentionMaintenanceRequest

    def as_dict(self) -> dict[str, object]:
        return {
            "mode": "PLAN",
            **self.request.as_dict(),
            "operations": [
                "ensure_partitions_and_check",
                "archive_sessions",
                "drop_archived_partitions",
                "partition_retention",
            ],
            "database_changes": False,
        }


@dataclass(frozen=True, slots=True)
class RetentionMaintenanceStatus:
    checked_at: datetime
    state: str
    last_success_at: datetime | None
    consecutive_failures: int
    request: RetentionMaintenanceRequest
    created_partitions: int
    retention_dropped: int
    retention_failed: int
    archive_outcomes: int
    archive_outcomes_failed: int
    archive_partitions_dropped: int
    alarms: tuple[MaintenanceAlarm, ...] = ()
    alarm_count: int = 0
    schema: str = STATUS_SCHEMA

    def __post_init__(self) -> None:
        object.__setattr__(self, "checked_at", _utc(self.checked_at, "checked_at"))
        if self.last_success_at is not None:
            object.__setattr__(self, "last_success_at", _utc(self.last_success_at, "last_success_at"))
        if self.schema != STATUS_SCHEMA:
            raise ValueError("retention status schema is unsupported")
        if self.state not in {STATUS_READY, STATUS_ALARM}:
            raise ValueError("retention status state is unsupported")
        if self.consecutive_failures < 0:
            raise ValueError("consecutive_failures cannot be negative")
        if self.state == STATUS_READY and self.consecutive_failures != 0:
            raise ValueError("READY retention status cannot have consecutive failures")
        if self.state == STATUS_ALARM and self.consecutive_failures < 1:
            raise ValueError("ALARM retention status requires a consecutive failure")
        if self.last_success_at is not None and self.last_success_at > self.checked_at:
            raise ValueError("last_success_at cannot be later than checked_at")
        counts = (
            self.created_partitions,
            self.retention_dropped,
            self.retention_failed,
            self.archive_outcomes,
            self.archive_outcomes_failed,
            self.archive_partitions_dropped,
            self.alarm_count,
        )
        if any(value < 0 for value in counts):
            raise ValueError("retention status counts cannot be negative")
        if self.alarm_count < len(self.alarms):
            raise ValueError("alarm_count cannot be smaller than the recorded alarms")
        if self.state == STATUS_READY and self.alarm_count:
            raise ValueError("READY retention status cannot have alarms")
        if self.state == STATUS_ALARM and not self.alarm_count:
            raise ValueError("ALARM retention status requires an alarm")

    @property
    def alarms_omitted(self) -> int:
        return self.alarm_count - len(self.alarms)

    def as_dict(self) -> dict[str, object]:
        alarm_codes = Counter(alarm.code for alarm in self.alarms)
        return {
            "schema": self.schema,
            "checked_at": self.checked_at.isoformat(),
            "state": self.state,
            "last_success_at": self.last_success_at.isoformat() if self.last_success_at else None,
            "consecutive_failures": self.consecutive_failures,
            "request": self.request.as_dict(),
            "created_partitions": self.created_partitions,
            "retention_dropped": self.retention_dropped,
            "retention_failed": self.retention_failed,
            "archive_outcomes": self.archive_outcomes,
            "archive_outcomes_failed": self.archive_outcomes_failed,
            "archive_partitions_dropped": self.archive_partitions_dropped,
            "alarm_count": self.alarm_count,
            "alarm_codes": dict(sorted(alarm_codes.items())),
            "alarms": [alarm.as_dict() for alarm in self.alarms],
            "alarms_omitted": self.alarms_omitted,
        }

    @classmethod
    def from_dict(cls, payload: object) -> RetentionMaintenanceStatus:
        if not isinstance(payload, dict) or payload.get("schema") != STATUS_SCHEMA:
            raise ValueError("retention status identity mismatch")
        request = payload.get("request")
        if not isinstance(request, dict):
            raise ValueError("retention status request is invalid")
        raw_alarms = payload.get("alarms")
        if not isinstance(raw_alarms, list):
            raise ValueError("retention status alarms are invalid")
        try:
            return cls(
                checked_at=datetime.fromisoformat(payload["checked_at"]),
                state=payload["state"],
                last_success_at=(
                    datetime.fromisoformat(payload["last_success_at"])
                    if payload.get("last_success_at") else None
                ),
                consecutive_failures=_integer(payload["consecutive_failures"], "consecutive_failures"),
                request=RetentionMaintenanceRequest(
                    as_of_date=date.fromisoformat(request["as_of_date"]),
                    as_of_session=date.fromisoformat(request["as_of_session"]),
                    archive_session_dates=tuple(date.fromisoformat(value) for value in request["archive_session_dates"]),
                    partition_relations=tuple(request["partition_relations"]),
                    retention_relations=tuple(request["retention_relations"]),
                    archive_relations=tuple(request["archive_relations"]),
                    archive_kinds=tuple(request["archive_kinds"]),
                ),
                created_partitions=_integer(payload["created_partitions"], "created_partitions"),
                retention_dropped=_integer(payload["retention_dropped"], "retention_dropped"),
                retention_failed=_integer(payload["retention_failed"], "retention_failed"),
                archive_outcomes=_integer(payload["archive_outcomes"], "archive_outcomes"),
                archive_outcomes_failed=_integer(payload["archive_outcomes_failed"], "archive_outcomes_failed"),
                archive_partitions_dropped=_integer(
                    payload["archive_partitions_dropped"], "archive_partitions_dropped"
                ),
                alarms=tuple(
                    MaintenanceAlarm(
                        stage=value["stage"],
                        code=value["code"],
                        detail=value["detail"],
                        relation=value.get("relation"),
                    )
                    for value in raw_alarms
                ),
                alarm_count=_integer(payload["alarm_count"], "alarm_count"),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"retention status payload is invalid: {error}") from error


def _integer(value: object, name: str) -> int:
    if type(value) is not int:
        raise ValueError(f"{name} must be an integer")
    return value


def _utc(value: datetime, name: str) -> datetime:
    if value.utcoffset() is None:
        raise ValueError(f"{name} must include a time zone")
    return value.astimezone(timezone.utc)


def read_retention_status(path: Path) -> RetentionMaintenanceStatus | None:
    if not path.exists():
        return None
    raw = path.read_bytes()
    if len(raw) > MAX_STATUS_BYTES:
        raise ValueError("retention status exceeds its byte bound")
    return RetentionMaintenanceStatus.from_dict(json.loads(raw))


def write_retention_status(path: Path, status: RetentionMaintenanceStatus) -> None:
    encoded = (json.dumps(status.as_dict(), sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    if len(encoded) > MAX_STATUS_BYTES:
        raise ValueError("retention status exceeds its byte bound")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: str | None = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".tmp", delete=False) as output:
            temporary = output.name
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        for attempt in range(3):
            try:
                os.replace(temporary, path)
                temporary = None
                break
            except PermissionError:
                if attempt == 2:
                    raise
                time.sleep(0.05)
    finally:
        if temporary and Path(temporary).exists():
            Path(temporary).unlink()


@dataclass(frozen=True, slots=True)
class RetentionMaintenanceHealth:
    checked_at: datetime
    state: str
    reason_codes: tuple[str, ...]
    status_checked_at: datetime | None
    last_success_at: datetime | None
    consecutive_failures: int | None
    status_age_seconds: float | None

    def as_dict(self) -> dict[str, object]:
        return {
            "checked_at": self.checked_at.isoformat(),
            "state": self.state,
            "reason_codes": list(self.reason_codes),
            "status_checked_at": self.status_checked_at.isoformat() if self.status_checked_at else None,
            "last_success_at": self.last_success_at.isoformat() if self.last_success_at else None,
            "consecutive_failures": self.consecutive_failures,
            "status_age_seconds": self.status_age_seconds,
        }


def assess_retention_health(
    status: RetentionMaintenanceStatus | None,
    *,
    checked_at: datetime,
    maximum_age_seconds: int,
    failure_threshold: int,
) -> RetentionMaintenanceHealth:
    now = _utc(checked_at, "checked_at")
    if maximum_age_seconds <= 0 or failure_threshold <= 0:
        raise ValueError("retention health thresholds must be positive")
    if status is None:
        return RetentionMaintenanceHealth(
            now, HEALTH_BLOCKED, ("RETENTION_STATUS_MISSING",), None, None, None, None
        )
    age = (now - status.checked_at).total_seconds()
    reasons: list[str] = []
    state = HEALTH_READY
    if age < 0:
        state = HEALTH_BLOCKED
        reasons.append("RETENTION_STATUS_FROM_FUTURE")
    elif age > maximum_age_seconds:
        state = HEALTH_BLOCKED
        reasons.append("RETENTION_STATUS_STALE")
    if status.state == STATUS_ALARM:
        reasons.append("RETENTION_CYCLE_ALARM")
        state = (
            HEALTH_BLOCKED
            if status.consecutive_failures >= failure_threshold
            else HEALTH_DEGRADED
        ) if state != HEALTH_BLOCKED else state
    return RetentionMaintenanceHealth(
        checked_at=now,
        state=state,
        reason_codes=tuple(reasons),
        status_checked_at=status.checked_at,
        last_success_at=status.last_success_at,
        consecutive_failures=status.consecutive_failures,
        status_age_seconds=round(age, 3),
    )


class RetentionMaintenanceCycle:
    def __init__(
        self,
        ensure_partitions: Callable[[date], tuple[int, list[PartitionAlarm]]],
        retention: PartitionRetentionExecutor,
        archive: SessionArchiveWorker,
        status_path: Path | str,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        path = Path(status_path)
        if not path.is_absolute():
            raise ValueError("retention status path must be absolute")
        self._ensure_partitions = ensure_partitions
        self._retention = retention
        self._archive = archive
        self._status_path = path
        self._clock = clock

    @staticmethod
    def plan(request: RetentionMaintenanceRequest) -> RetentionMaintenancePlan:
        return RetentionMaintenancePlan(request)

    def run(
        self,
        request: RetentionMaintenanceRequest,
        *,
        execute: bool = False,
    ) -> RetentionMaintenancePlan | RetentionMaintenanceStatus:
        if not execute:
            return self.plan(request)
        checked_at = _utc(self._clock(), "clock")
        if request.as_of_date > checked_at.date():
            raise ValueError("as_of_date cannot be later than the current UTC date")
        previous = read_retention_status(self._status_path)
        alarms: list[MaintenanceAlarm] = []
        created_partitions = 0
        retention_report: RetentionReport | None = None
        archive_report: ArchiveRunReport | None = None
        drop_report: DropReport | None = None

        try:
            created_partitions, partition_alarms = self._ensure_partitions(checked_at.date())
            if created_partitions < 0:
                raise ValueError("created partition count cannot be negative")
            alarms.extend(
                MaintenanceAlarm(
                    "PARTITION_MAINTENANCE",
                    "PARTITION_HEALTH_ALARM",
                    f"partition horizon ends {alarm.horizon_date}, {alarm.days_ahead} days ahead",
                    alarm.relation_name,
                )
                for alarm in partition_alarms
            )
        except Exception as error:  # noqa: BLE001 - persisted and returned as an explicit alarm
            alarms.append(_exception_alarm("PARTITION_MAINTENANCE", "PARTITION_MAINTENANCE_FAILED", error))

        try:
            archive_report = self._archive.archive_sessions(request.archive_session_dates)
            alarms.extend(_archive_alarm("SESSION_ARCHIVE", alarm) for alarm in archive_report.alarms)
        except Exception as error:  # noqa: BLE001
            alarms.append(_exception_alarm("SESSION_ARCHIVE", "SESSION_ARCHIVE_FAILED", error))

        try:
            drop_report = self._archive.drop_archived_partitions(
                as_of_session=request.as_of_session,
                relations=request.archive_relations,
                health_date=checked_at.date(),
            )
            alarms.extend(_archive_alarm("ARCHIVE_RETENTION", alarm) for alarm in drop_report.all_alarms)
        except Exception as error:  # noqa: BLE001
            alarms.append(_exception_alarm("ARCHIVE_RETENTION", "ARCHIVE_RETENTION_FAILED", error))

        try:
            retention_report = self._retention.run(
                execute=True,
                as_of_date=request.as_of_date,
                relations=request.retention_relations,
            )
            alarms.extend(_archive_alarm("PARTITION_RETENTION", alarm) for alarm in retention_report.all_alarms)
        except Exception as error:  # noqa: BLE001
            alarms.append(_exception_alarm("PARTITION_RETENTION", "PARTITION_RETENTION_FAILED", error))

        succeeded = not alarms
        last_success_at = checked_at if succeeded else previous.last_success_at if previous else None
        consecutive_failures = 0 if succeeded else (previous.consecutive_failures if previous else 0) + 1
        recorded = tuple(alarms[:MAX_RECORDED_ALARMS])
        archive_outcomes = (
            sum(len(session.outcomes) for session in archive_report.sessions)
            if archive_report else 0
        )
        status = RetentionMaintenanceStatus(
            checked_at=checked_at,
            state=STATUS_READY if succeeded else STATUS_ALARM,
            last_success_at=last_success_at,
            consecutive_failures=consecutive_failures,
            request=request,
            created_partitions=created_partitions,
            retention_dropped=len(retention_report.dropped) if retention_report else 0,
            retention_failed=len(retention_report.failed) if retention_report else 0,
            archive_outcomes=archive_outcomes,
            archive_outcomes_failed=(
                sum(
                    outcome.status == STATUS_FAILED
                    for session in archive_report.sessions
                    for outcome in session.outcomes
                )
                if archive_report else 0
            ),
            archive_partitions_dropped=len(drop_report.dropped) if drop_report else 0,
            alarms=recorded,
            alarm_count=len(alarms),
        )
        write_retention_status(self._status_path, status)
        LOGGER.info(
            "retention_maintenance_status %s",
            json.dumps(
                {
                    "state": status.state,
                    "last_success_at": status.as_dict()["last_success_at"],
                    "consecutive_failures": status.consecutive_failures,
                    "alarm_count": status.alarm_count,
                },
                sort_keys=True,
            ),
        )
        return status


def _archive_alarm(stage: str, alarm: ArchiveAlarm) -> MaintenanceAlarm:
    return MaintenanceAlarm(stage, alarm.code, alarm.detail, alarm.relation)


def _exception_alarm(stage: str, code: str, error: BaseException) -> MaintenanceAlarm:
    LOGGER.exception("%s failed", stage.lower())
    return MaintenanceAlarm(stage, code, f"{type(error).__name__}: {str(error)[:240]}")
