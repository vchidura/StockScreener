from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Protocol

from .bootstrap_checkpoint import (
    BootstrapStageState,
    RealtimeBootstrapCheckpoint,
    complete_bootstrap_stage,
    fail_bootstrap_stage,
    next_bootstrap_stage,
    start_bootstrap_stage,
)
from .bootstrap_manifest import (
    BootstrapStageName,
    RealtimeBootstrapPlan,
    RealtimeBootstrapStagePlan,
)


class BootstrapStageExecutor(Protocol):
    def execute(
        self,
        stage: RealtimeBootstrapStagePlan,
        plan: RealtimeBootstrapPlan,
    ) -> Mapping[str, object]: ...


@dataclass(frozen=True, slots=True)
class BootstrapStageRunResult:
    checkpoint: RealtimeBootstrapCheckpoint
    stage: BootstrapStageName | None
    state: BootstrapStageState | None
    failure_code: str | None


def _stage_plan(
    plan: RealtimeBootstrapPlan,
    name: BootstrapStageName,
) -> RealtimeBootstrapStagePlan:
    return next(stage for stage in plan.stages if stage.name is name)


def _validate_stage_evidence(
    stage: RealtimeBootstrapStagePlan,
    evidence: Mapping[str, object],
) -> dict[str, object]:
    normalized = dict(evidence)
    requests = normalized.get("provider_requests", 0)
    if type(requests) is not int or requests < 0:
        raise ValueError("bootstrap stage provider request evidence is invalid")
    if stage.provider_request_limit == 0 and requests:
        raise ValueError("non-provider bootstrap stage performed provider requests")
    if requests > stage.provider_request_limit:
        raise ValueError("bootstrap stage exceeded its provider request limit")
    if normalized.get("stage") not in {None, stage.name.value}:
        raise ValueError("bootstrap stage evidence identity changed")
    normalized["stage"] = stage.name.value
    normalized["side_effect"] = stage.side_effect
    normalized["provider_requests"] = requests
    return normalized


def run_next_bootstrap_stage(
    plan: RealtimeBootstrapPlan,
    checkpoint: RealtimeBootstrapCheckpoint,
    executor: BootstrapStageExecutor,
    *,
    started_at: datetime,
    finished_at: datetime,
) -> BootstrapStageRunResult:
    if (
        checkpoint.plan_sha256 != plan.sha256
        or checkpoint.manifest_sha256 != plan.manifest_sha256
        or checkpoint.database != plan.database
    ):
        raise ValueError("bootstrap runner plan and checkpoint differ")
    stage_name = next_bootstrap_stage(checkpoint)
    if stage_name is None:
        return BootstrapStageRunResult(checkpoint, None, None, None)
    running = start_bootstrap_stage(
        checkpoint,
        stage_name,
        now=started_at,
    )
    stage = _stage_plan(plan, stage_name)
    try:
        evidence = _validate_stage_evidence(
            stage,
            executor.execute(stage, plan),
        )
        complete = complete_bootstrap_stage(
            running,
            stage_name,
            evidence=evidence,
            now=finished_at,
        )
        return BootstrapStageRunResult(
            complete,
            stage_name,
            BootstrapStageState.COMPLETE,
            None,
        )
    except Exception as error:
        failure_code = type(error).__name__.upper()
        failed = fail_bootstrap_stage(
            running,
            stage_name,
            failure_code=failure_code,
            evidence={
                "stage": stage_name.value,
                "side_effect": stage.side_effect,
                "error_type": type(error).__name__,
            },
            now=finished_at,
        )
        return BootstrapStageRunResult(
            failed,
            stage_name,
            BootstrapStageState.FAILED,
            failure_code,
        )
