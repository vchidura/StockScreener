from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from uuid import UUID

from .acquisition_producer import DeveloperAcquisitionProducerResult
from .acquisition_worker import (
    DeveloperAcquisitionWorkerStatus,
    DeveloperAcquisitionWorkerVerdict,
)
from .contracts import CANARY_UNIVERSE, canonical_sha256
from .migrations import EXPECTED_DATABASE_NAME
from .portal_publisher import PortalPublicationStatus, PortalPublisherVerdict
from .preflight import ProviderTier


CANARY_STATUS_SCHEMA = "advanced_realtime_canary_status_v1"
CANARY_STOP_SCHEMA = "advanced_realtime_canary_stop_v1"
MAX_STATUS_BYTES = 64 * 1024


class CanaryStage(str, Enum):
    INGRESS = "INGRESS"
    COMPUTE = "COMPUTE"
    PUBLICATION = "PUBLICATION"
    PORTAL = "PORTAL"


class CanaryStageState(str, Enum):
    WARMING = "WARMING"
    READY = "READY"
    DEGRADED = "DEGRADED"
    BLOCKED = "BLOCKED"
    FAILED = "FAILED"


class CanarySupervisorState(str, Enum):
    WARMING = "WARMING"
    READY = "READY"
    DEGRADED = "DEGRADED"
    BLOCKED = "BLOCKED"


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class CanaryStageStatus:
    stage: CanaryStage
    state: CanaryStageState
    checked_at: datetime
    input_count: int
    output_count: int
    retry_count: int
    reason_codes: tuple[str, ...]
    evidence_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.stage, CanaryStage) or not isinstance(
            self.state, CanaryStageState
        ):
            raise TypeError("canary stage identity or state is invalid")
        object.__setattr__(self, "checked_at", _utc(self.checked_at, "checked_at"))
        if any(
            type(value) is not int or value < 0
            for value in (self.input_count, self.output_count, self.retry_count)
        ):
            raise ValueError("canary stage counters must be nonnegative")
        if len(self.reason_codes) != len(set(self.reason_codes)):
            raise ValueError("canary stage reason codes must be unique")
        if len(self.evidence_sha256) != 64 or any(
            character not in "0123456789abcdef"
            for character in self.evidence_sha256
        ):
            raise ValueError("canary stage evidence hash must be SHA-256")


@dataclass(frozen=True, slots=True)
class CanarySupervisorStatus:
    instance_id: UUID
    cohort_id: UUID
    provider_tier: ProviderTier
    checked_at: datetime
    state: CanarySupervisorState
    stages: tuple[CanaryStageStatus, ...]
    reason_codes: tuple[str, ...]
    database_target: str = EXPECTED_DATABASE_NAME
    canonical_fallback: bool = False
    schema: str = CANARY_STATUS_SCHEMA

    def __post_init__(self) -> None:
        if self.provider_tier is not ProviderTier.MASSIVE_DEVELOPER:
            raise ValueError("canary supervisor requires the Developer tier")
        if self.database_target != EXPECTED_DATABASE_NAME or self.canonical_fallback:
            raise ValueError("canary supervisor cannot target or fall back to canonical")
        if self.schema != CANARY_STATUS_SCHEMA:
            raise ValueError("canary supervisor status schema is unsupported")
        object.__setattr__(self, "checked_at", _utc(self.checked_at, "checked_at"))
        expected = tuple(CanaryStage)
        if tuple(stage.stage for stage in self.stages) != expected:
            raise ValueError("canary supervisor must contain every stage in order")
        if len(self.reason_codes) != len(set(self.reason_codes)):
            raise ValueError("canary supervisor reason codes must be unique")

    @property
    def status_sha256(self) -> str:
        return canonical_sha256(self)

    def payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "instance_id": str(self.instance_id),
            "cohort_id": str(self.cohort_id),
            "provider_tier": self.provider_tier.value,
            "checked_at": self.checked_at.isoformat(),
            "state": self.state.value,
            "database_target": self.database_target,
            "canonical_fallback": self.canonical_fallback,
            "universe_sha256": CANARY_UNIVERSE.sha256,
            "reason_codes": list(self.reason_codes),
            "stages": [
                {
                    "stage": stage.stage.value,
                    "state": stage.state.value,
                    "checked_at": stage.checked_at.isoformat(),
                    "input_count": stage.input_count,
                    "output_count": stage.output_count,
                    "retry_count": stage.retry_count,
                    "reason_codes": list(stage.reason_codes),
                    "evidence_sha256": stage.evidence_sha256,
                }
                for stage in self.stages
            ],
            "status_sha256": self.status_sha256,
        }


@dataclass(frozen=True, slots=True)
class CanaryStopRequest:
    instance_id: UUID
    requested_at: datetime
    schema: str = CANARY_STOP_SCHEMA

    def __post_init__(self) -> None:
        if self.schema != CANARY_STOP_SCHEMA:
            raise ValueError("canary stop request schema is unsupported")
        object.__setattr__(
            self,
            "requested_at",
            _utc(self.requested_at, "requested_at"),
        )

    @property
    def request_sha256(self) -> str:
        return canonical_sha256(self)

    def payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "instance_id": str(self.instance_id),
            "requested_at": self.requested_at.isoformat(),
            "request_sha256": self.request_sha256,
        }


def producer_stage_status(
    result: DeveloperAcquisitionProducerResult,
    *,
    checked_at: datetime,
) -> CanaryStageStatus:
    return CanaryStageStatus(
        CanaryStage.INGRESS,
        CanaryStageState.READY,
        checked_at,
        result.provider_requests,
        result.equity_fact_count + result.option_fact_count,
        0,
        (),
        canonical_sha256(result),
    )


def compute_stage_status(status: DeveloperAcquisitionWorkerStatus) -> CanaryStageStatus:
    state = {
        DeveloperAcquisitionWorkerVerdict.IDLE: CanaryStageState.WARMING,
        DeveloperAcquisitionWorkerVerdict.COMPLETE: CanaryStageState.READY,
        DeveloperAcquisitionWorkerVerdict.DEGRADED: CanaryStageState.DEGRADED,
    }[status.verdict]
    return CanaryStageStatus(
        CanaryStage.COMPUTE,
        state,
        status.checked_at,
        status.claimed,
        status.output_count,
        status.retried + status.failed + status.blocked,
        status.reason_codes,
        canonical_sha256(status),
    )


def publication_stage_status(status: PortalPublicationStatus) -> CanaryStageStatus:
    state = {
        PortalPublisherVerdict.IDLE: CanaryStageState.WARMING,
        PortalPublisherVerdict.COMPLETE: CanaryStageState.READY,
        PortalPublisherVerdict.DEGRADED: CanaryStageState.DEGRADED,
    }[status.verdict]
    return CanaryStageStatus(
        CanaryStage.PUBLICATION,
        state,
        status.checked_at,
        status.claimed,
        status.promoted_snapshots + status.delivered,
        status.retried + status.abandoned,
        status.reason_codes,
        canonical_sha256(status),
    )


def portal_stage_status(
    payload: dict[str, object],
    *,
    checked_at: datetime,
) -> CanaryStageStatus:
    if payload.get("canonical_fallback") is not False or payload.get(
        "provider_fetch_performed"
    ) is not False:
        raise ValueError("portal status violates isolation")
    expected = set(CANARY_UNIVERSE.equity_tickers)
    ready = {str(value) for value in payload.get("ready_tickers", ())}
    state = (
        CanaryStageState.READY
        if payload.get("status") == "READY" and ready == expected
        else CanaryStageState.BLOCKED
    )
    reasons = tuple(str(value) for value in payload.get("reason_codes", ()))
    if state is CanaryStageState.BLOCKED and not reasons:
        reasons = ("PORTAL_CANARY_NOT_READY",)
    return CanaryStageStatus(
        CanaryStage.PORTAL,
        state,
        checked_at,
        len(expected),
        len(ready),
        0,
        reasons,
        canonical_sha256(payload),
    )


def build_canary_supervisor_status(
    *,
    instance_id: UUID,
    cohort_id: UUID,
    checked_at: datetime,
    stages: tuple[CanaryStageStatus, ...],
    maximum_stage_age_seconds: int = 30,
) -> CanarySupervisorStatus:
    checked = _utc(checked_at, "checked_at")
    if maximum_stage_age_seconds <= 0:
        raise ValueError("maximum stage age must be positive")
    reasons: list[str] = []
    stale = []
    for stage in stages:
        age = (checked - stage.checked_at).total_seconds()
        if age < 0:
            raise ValueError("canary stage status cannot be from the future")
        if age > maximum_stage_age_seconds:
            stale.append(stage.stage)
            reasons.append(f"{stage.stage.value}_STATUS_STALE")
        reasons.extend(
            f"{stage.stage.value}:{reason}" for reason in stage.reason_codes
        )
    states = {stage.state for stage in stages}
    if stale or states & {CanaryStageState.BLOCKED, CanaryStageState.FAILED}:
        state = CanarySupervisorState.BLOCKED
    elif CanaryStageState.DEGRADED in states:
        state = CanarySupervisorState.DEGRADED
    elif states == {CanaryStageState.READY}:
        state = CanarySupervisorState.READY
    else:
        state = CanarySupervisorState.WARMING
    return CanarySupervisorStatus(
        instance_id=instance_id,
        cohort_id=cohort_id,
        provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        checked_at=checked,
        state=state,
        stages=stages,
        reason_codes=tuple(dict.fromkeys(reasons)),
    )


def _atomic_write(path: Path, payload: dict[str, object]) -> None:
    encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )
    if len(encoded) > MAX_STATUS_BYTES:
        raise ValueError("canary status artifact exceeds its byte bound")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def write_canary_status(path: Path, status: CanarySupervisorStatus) -> None:
    _atomic_write(path, status.payload())


def read_canary_status(path: Path) -> CanarySupervisorStatus:
    raw = path.read_bytes()
    if len(raw) > MAX_STATUS_BYTES:
        raise ValueError("canary status artifact exceeds its byte bound")
    payload = json.loads(raw)
    if payload.get("universe_sha256") != CANARY_UNIVERSE.sha256:
        raise ValueError("canary status universe does not match")
    stages = tuple(
        CanaryStageStatus(
            stage=CanaryStage(str(row["stage"])),
            state=CanaryStageState(str(row["state"])),
            checked_at=datetime.fromisoformat(str(row["checked_at"])),
            input_count=int(row["input_count"]),
            output_count=int(row["output_count"]),
            retry_count=int(row["retry_count"]),
            reason_codes=tuple(str(value) for value in row["reason_codes"]),
            evidence_sha256=str(row["evidence_sha256"]),
        )
        for row in payload["stages"]
    )
    status = CanarySupervisorStatus(
        instance_id=UUID(str(payload["instance_id"])),
        cohort_id=UUID(str(payload["cohort_id"])),
        provider_tier=ProviderTier(str(payload["provider_tier"])),
        checked_at=datetime.fromisoformat(str(payload["checked_at"])),
        state=CanarySupervisorState(str(payload["state"])),
        stages=stages,
        reason_codes=tuple(str(value) for value in payload["reason_codes"]),
        database_target=str(payload["database_target"]),
        canonical_fallback=bool(payload["canonical_fallback"]),
        schema=str(payload["schema"]),
    )
    if str(payload.get("status_sha256")) != status.status_sha256:
        raise ValueError("canary status hash does not match")
    return status


def write_canary_stop_request(path: Path, request: CanaryStopRequest) -> None:
    _atomic_write(path, request.payload())


def read_canary_stop_request(
    path: Path,
    *,
    expected_instance_id: UUID,
) -> CanaryStopRequest | None:
    if not path.exists():
        return None
    raw = path.read_bytes()
    if len(raw) > MAX_STATUS_BYTES:
        raise ValueError("canary stop artifact exceeds its byte bound")
    payload = json.loads(raw)
    request = CanaryStopRequest(
        instance_id=UUID(str(payload["instance_id"])),
        requested_at=datetime.fromisoformat(str(payload["requested_at"])),
        schema=str(payload["schema"]),
    )
    if str(payload.get("request_sha256")) != request.request_sha256:
        raise ValueError("canary stop request hash does not match")
    if request.instance_id != expected_instance_id:
        raise ValueError("canary stop request targets another instance")
    return request
