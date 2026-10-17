"""Runner bindings for the V2 stage orchestrator.

Each stage is bound to a factory that receives the RunnerEnvironment (the shared V2 database provider
and per-process state) and returns a StageRunner. A stage with no binding is NOT WIRED: enabling it in
a service plan fails closed at start (MissingRunner) instead of being skipped silently. Bind a stage
here when its repository ports run on the V2 schema; the orchestrator, lanes and readiness handling
need no change. A runner writes only through its StageContext (fenced, cancellable transactions).

Bound: all six non-options stages, OPTIONS_ACQUIRE, OPTIONS_ADMISSION and OPTIONS_FLOW.
EQUITY_EVIDENCE and PORTAL_PUBLISH (stage_runners_equity.py),
STOCK_ALERTS and STRATEGY_PUBLISH (stage_runners_alerts.py: compute and publication are separate
databases/roles) and REFERENCE and EQUITY_INGEST (stage_runners_ingest.py). EQUITY_EVIDENCE also drains
the acquisition batches into rt.bar_* first (equity_bar_drain.py), because only `compute` may write bars.
REFERENCE and EQUITY_INGEST call the provider through a source the RunnerEnvironment injects; while none
is configured they are BLOCKED_POLICY (REFERENCE_SOURCE_NOT_CONFIGURED / EQUITY_SOURCE_NOT_CONFIGURED) and
call nothing (`fail_closed_stages` reports them). OPTIONS_FLOW (stage_runners_options.py) is a resident
consumer of an injected journal source (a local mock feed until a gateway is authorized): it is
BLOCKED_POLICY with OPTIONS_FLOW_SOURCE_NOT_CONFIGURED / OPTIONS_FLOW_CONDITIONS_UNPINNED / OPTIONS_FLOW_POLICY_NOT_CONFIGURED
and reads nothing until all three are injected, and it fails closed (REPLAY_DIVERGENCE) when a replayed key
differs from its stored row. OPTIONS_ACQUIRE and OPTIONS_ADMISSION call injected repository-backed actions
and fail closed until those actions are configured. Not bound: OPTIONS_CYCLE, whose durable V2 publication
schema and materialization port are not installed.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping

from .database_access_v2 import V2ConnectionProvider
from .runner_environment import RunnerEnvironment
from .stage_graph import STAGES_V1
from .stage_orchestrator import StageRunner
from .stage_runners_alerts import stock_alerts_runner, strategy_publish_runner
from .stage_runners_advanced_equity import (
    advanced_equity_input_gate_runner,
)
from .stage_runners_equity import (
    equity_evidence_runner,
    equity_materialize_runner,
    equity_projection_runner,
    portal_publish_runner,
)
from .stage_runners_ingest import equity_block_reason, equity_ingest_runner, reference_block_reason, reference_runner
from .stage_runners_options import (
    options_acquire_block_reason,
    options_acquire_runner,
    options_admission_block_reason,
    options_admission_runner,
    options_flow_block_reason,
    options_flow_runner,
)

RunnerFactory = Callable[[RunnerEnvironment], StageRunner]

RUNNER_FACTORIES: dict[str, RunnerFactory] = {
    "REFERENCE": reference_runner,
    "EQUITY_INGEST": equity_ingest_runner,
    "EQUITY_INPUT_GATE": advanced_equity_input_gate_runner,
    "EQUITY_EVIDENCE": equity_evidence_runner,
    "EQUITY_MATERIALIZE": equity_materialize_runner,
    "EQUITY_PROJECTION": equity_projection_runner,
    "PORTAL_PUBLISH": portal_publish_runner,
    "STRATEGY_PUBLISH": strategy_publish_runner,
    "STOCK_ALERTS": stock_alerts_runner,
    "OPTIONS_ACQUIRE": options_acquire_runner,
    "OPTIONS_ADMISSION": options_admission_runner,
    "OPTIONS_FLOW": options_flow_runner,
}

# Wired stages that need an injected source, with the check that decides if they would block.
SOURCE_GATED_STAGES: dict[str, Callable[[RunnerEnvironment], str | None]] = {
    "REFERENCE": reference_block_reason,
    "EQUITY_INGEST": equity_block_reason,
    "OPTIONS_ACQUIRE": options_acquire_block_reason,
    "OPTIONS_ADMISSION": options_admission_block_reason,
    "OPTIONS_FLOW": options_flow_block_reason,
}


def wired_stages(enabled: frozenset[str] | set[str] = frozenset(STAGES_V1)) -> frozenset[str]:
    return frozenset(enabled) & frozenset(RUNNER_FACTORIES)


def unwired_stages(enabled: frozenset[str] | set[str] = frozenset(STAGES_V1)) -> frozenset[str]:
    return frozenset(enabled) - wired_stages(enabled)


def fail_closed_stages(enabled: frozenset[str] | set[str], environment: RunnerEnvironment) -> dict[str, str]:
    """Enabled wired stages that would be BLOCKED_POLICY with this environment, with the reason code."""
    reasons = {stage: check(environment) for stage, check in SOURCE_GATED_STAGES.items() if stage in enabled}
    return {stage: reason for stage, reason in sorted(reasons.items()) if reason is not None}


def build_runners(enabled: frozenset[str] | set[str], database: V2ConnectionProvider | None, *,
                  environment: RunnerEnvironment | None = None) -> Mapping[str, StageRunner]:
    missing = unwired_stages(enabled)
    if missing:
        raise LookupError("stages without a V2 runner binding: " + ", ".join(sorted(missing)))
    environment = environment or RunnerEnvironment(database)
    return {name: RUNNER_FACTORIES[name](environment) for name in enabled}
