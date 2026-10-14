from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum
import json
import os
from pathlib import Path
from typing import Mapping

from .bootstrap_manifest import (
    BOOTSTRAP_STAGE_ORDER,
    BootstrapStageName,
    RealtimeBootstrapPlan,
)
from .contracts import canonical_json, canonical_sha256


MAXIMUM_CHECKPOINT_BYTES = 1024 * 1024
MAXIMUM_STAGE_EVIDENCE_BYTES = 64 * 1024


class BootstrapStageState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class BootstrapStageCheckpoint:
    name: BootstrapStageName
    state: BootstrapStageState
    attempt: int
    started_at: datetime | None
    finished_at: datetime | None
    evidence: Mapping[str, object]
    evidence_sha256: str | None
    failure_code: str | None


@dataclass(frozen=True, slots=True)
class RealtimeBootstrapCheckpoint:
    schema_version: str
    plan_sha256: str
    manifest_sha256: str
    database: str
    stages: tuple[BootstrapStageCheckpoint, ...]
    updated_at: datetime
    checkpoint_sha256: str


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("bootstrap checkpoint clock must be timezone-aware")
    return value.astimezone(timezone.utc)


def _checkpoint_identity(
    *,
    schema_version: str,
    plan_sha256: str,
    manifest_sha256: str,
    database: str,
    stages: tuple[BootstrapStageCheckpoint, ...],
    updated_at: datetime,
) -> dict[str, object]:
    return {
        "schema_version": schema_version,
        "plan_sha256": plan_sha256,
        "manifest_sha256": manifest_sha256,
        "database": database,
        "stages": stages,
        "updated_at": updated_at,
    }


def create_bootstrap_checkpoint(
    plan: RealtimeBootstrapPlan,
    *,
    now: datetime,
) -> RealtimeBootstrapCheckpoint:
    updated = _utc(now)
    stages = tuple(
        BootstrapStageCheckpoint(
            name=name,
            state=BootstrapStageState.PENDING,
            attempt=0,
            started_at=None,
            finished_at=None,
            evidence={},
            evidence_sha256=None,
            failure_code=None,
        )
        for name in BOOTSTRAP_STAGE_ORDER
    )
    identity = _checkpoint_identity(
        schema_version="realtime-bootstrap-checkpoint-v1",
        plan_sha256=plan.sha256,
        manifest_sha256=plan.manifest_sha256,
        database=plan.database,
        stages=stages,
        updated_at=updated,
    )
    return RealtimeBootstrapCheckpoint(
        **identity,
        checkpoint_sha256=canonical_sha256(identity),
    )


def _validated_evidence(evidence: Mapping[str, object]) -> tuple[dict[str, object], str]:
    normalized = dict(evidence)
    encoded = canonical_json(normalized).encode("ascii")
    if len(encoded) > MAXIMUM_STAGE_EVIDENCE_BYTES:
        raise ValueError("bootstrap stage evidence exceeds its size bound")
    forbidden = ("password", "secret", "token", "api_key", "dsn")
    if any(any(term in str(key).lower() for term in forbidden) for key in normalized):
        raise ValueError("bootstrap stage evidence contains a secret-like key")
    return normalized, canonical_sha256(normalized)


def _replace_stage(
    checkpoint: RealtimeBootstrapCheckpoint,
    replacement: BootstrapStageCheckpoint,
    *,
    now: datetime,
) -> RealtimeBootstrapCheckpoint:
    updated = _utc(now)
    stages = tuple(
        replacement if stage.name is replacement.name else stage
        for stage in checkpoint.stages
    )
    identity = _checkpoint_identity(
        schema_version=checkpoint.schema_version,
        plan_sha256=checkpoint.plan_sha256,
        manifest_sha256=checkpoint.manifest_sha256,
        database=checkpoint.database,
        stages=stages,
        updated_at=updated,
    )
    return RealtimeBootstrapCheckpoint(
        **identity,
        checkpoint_sha256=canonical_sha256(identity),
    )


def next_bootstrap_stage(
    checkpoint: RealtimeBootstrapCheckpoint,
) -> BootstrapStageName | None:
    for stage in checkpoint.stages:
        if stage.state in {BootstrapStageState.PENDING, BootstrapStageState.FAILED}:
            return stage.name
        if stage.state is BootstrapStageState.RUNNING:
            return stage.name
    return None


def start_bootstrap_stage(
    checkpoint: RealtimeBootstrapCheckpoint,
    stage_name: BootstrapStageName,
    *,
    now: datetime,
) -> RealtimeBootstrapCheckpoint:
    expected = next_bootstrap_stage(checkpoint)
    if expected is not stage_name:
        raise ValueError(f"bootstrap stage order changed: expected={expected} requested={stage_name}")
    stage = next(row for row in checkpoint.stages if row.name is stage_name)
    if stage.state not in {BootstrapStageState.PENDING, BootstrapStageState.FAILED}:
        raise ValueError("bootstrap stage cannot start from its current state")
    started = _utc(now)
    return _replace_stage(
        checkpoint,
        replace(
            stage,
            state=BootstrapStageState.RUNNING,
            attempt=stage.attempt + 1,
            started_at=started,
            finished_at=None,
            evidence={},
            evidence_sha256=None,
            failure_code=None,
        ),
        now=started,
    )


def complete_bootstrap_stage(
    checkpoint: RealtimeBootstrapCheckpoint,
    stage_name: BootstrapStageName,
    *,
    evidence: Mapping[str, object],
    now: datetime,
) -> RealtimeBootstrapCheckpoint:
    stage = next(row for row in checkpoint.stages if row.name is stage_name)
    if stage.state is not BootstrapStageState.RUNNING:
        raise ValueError("bootstrap stage must be running before completion")
    normalized, digest = _validated_evidence(evidence)
    finished = _utc(now)
    if stage.started_at is None or finished < stage.started_at:
        raise ValueError("bootstrap stage completion clock is invalid")
    return _replace_stage(
        checkpoint,
        replace(
            stage,
            state=BootstrapStageState.COMPLETE,
            finished_at=finished,
            evidence=normalized,
            evidence_sha256=digest,
        ),
        now=finished,
    )


def fail_bootstrap_stage(
    checkpoint: RealtimeBootstrapCheckpoint,
    stage_name: BootstrapStageName,
    *,
    failure_code: str,
    evidence: Mapping[str, object],
    now: datetime,
) -> RealtimeBootstrapCheckpoint:
    stage = next(row for row in checkpoint.stages if row.name is stage_name)
    if stage.state is not BootstrapStageState.RUNNING:
        raise ValueError("bootstrap stage must be running before failure")
    normalized_code = failure_code.strip().upper()
    if not normalized_code or len(normalized_code) > 64 or not normalized_code.replace("_", "").isalnum():
        raise ValueError("bootstrap failure code is invalid")
    normalized, digest = _validated_evidence(evidence)
    finished = _utc(now)
    if stage.started_at is None or finished < stage.started_at:
        raise ValueError("bootstrap stage failure clock is invalid")
    return _replace_stage(
        checkpoint,
        replace(
            stage,
            state=BootstrapStageState.FAILED,
            finished_at=finished,
            evidence=normalized,
            evidence_sha256=digest,
            failure_code=normalized_code,
        ),
        now=finished,
    )


def checkpoint_json(checkpoint: RealtimeBootstrapCheckpoint) -> str:
    payload = canonical_json(checkpoint)
    if len(payload.encode("ascii")) > MAXIMUM_CHECKPOINT_BYTES:
        raise ValueError("bootstrap checkpoint exceeds its size bound")
    return payload + "\n"


def write_bootstrap_checkpoint(
    path: Path,
    checkpoint: RealtimeBootstrapCheckpoint,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    payload = checkpoint_json(checkpoint).encode("ascii")
    with temporary.open("wb") as output:
        output.write(payload)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)


def load_bootstrap_checkpoint(
    path: Path,
    plan: RealtimeBootstrapPlan,
) -> RealtimeBootstrapCheckpoint:
    if not path.is_file() or path.stat().st_size > MAXIMUM_CHECKPOINT_BYTES:
        raise ValueError("bootstrap checkpoint is unavailable or oversized")
    try:
        value = json.loads(path.read_text(encoding="ascii"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("bootstrap checkpoint is invalid") from error
    try:
        stages = tuple(
            BootstrapStageCheckpoint(
                name=BootstrapStageName(row["name"]),
                state=BootstrapStageState(row["state"]),
                attempt=int(row["attempt"]),
                started_at=(datetime.fromisoformat(row["started_at"].replace("Z", "+00:00")) if row["started_at"] else None),
                finished_at=(datetime.fromisoformat(row["finished_at"].replace("Z", "+00:00")) if row["finished_at"] else None),
                evidence=dict(row["evidence"]),
                evidence_sha256=row["evidence_sha256"],
                failure_code=row["failure_code"],
            )
            for row in value["stages"]
        )
        updated_at = datetime.fromisoformat(value["updated_at"].replace("Z", "+00:00"))
        checkpoint = RealtimeBootstrapCheckpoint(
            schema_version=value["schema_version"],
            plan_sha256=value["plan_sha256"],
            manifest_sha256=value["manifest_sha256"],
            database=value["database"],
            stages=stages,
            updated_at=updated_at,
            checkpoint_sha256=value["checkpoint_sha256"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("bootstrap checkpoint contract is invalid") from error
    identity = _checkpoint_identity(
        schema_version=checkpoint.schema_version,
        plan_sha256=checkpoint.plan_sha256,
        manifest_sha256=checkpoint.manifest_sha256,
        database=checkpoint.database,
        stages=checkpoint.stages,
        updated_at=checkpoint.updated_at,
    )
    if (
        checkpoint.schema_version != "realtime-bootstrap-checkpoint-v1"
        or checkpoint.plan_sha256 != plan.sha256
        or checkpoint.manifest_sha256 != plan.manifest_sha256
        or checkpoint.database != plan.database
        or tuple(stage.name for stage in checkpoint.stages) != BOOTSTRAP_STAGE_ORDER
        or checkpoint.checkpoint_sha256 != canonical_sha256(identity)
    ):
        raise ValueError("bootstrap checkpoint identity or hash changed")
    return checkpoint
