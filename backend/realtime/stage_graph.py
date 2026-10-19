"""Independent stage resolution for the V2 orchestrator (RT-01).

Pure functions over the stage catalog: no I/O. The decisions mirror the database rules in
sql_v2/004_realtime_orchestration.sql, so an orchestrator cannot start, block or skip a
stage in a way the database would reject. An Options stage never gates an equity,
reference or portal stage.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping

STAGE_CATALOG_VERSION = "stages_v1"
STAGE_CATALOG_VERSION_V2 = "stages_v2"
STAGE_CATALOG_VERSION_V3 = "stages_v3"
STAGE_CATALOG_VERSION_V4 = "stages_v4"
TERMINAL_STATES = frozenset({"COMPLETE", "FAILED", "BLOCKED_PREREQ", "BLOCKED_POLICY", "MISSED"})
NON_COMPLETING_STATES = frozenset({"FAILED", "BLOCKED_PREREQ", "BLOCKED_POLICY", "MISSED"})


DATA_LANE = "data"
ALERT_LANE = "alert"
PORTAL_LANE = "portal"
OPTIONS_LANE = "options"
EXECUTION_LANES = frozenset({DATA_LANE, ALERT_LANE, PORTAL_LANE, OPTIONS_LANE})


@dataclass(frozen=True, slots=True)
class StageDefinition:
    stage_id: str
    domain: str
    prerequisites: tuple[str, ...]
    execution_lane: str
    schedule_kind: str
    gates_strategy: bool = False
    deadline_offset_seconds: int | None = None

    @property
    def lane(self) -> str:
        return self.execution_lane


# INTERVAL and CONTINUOUS stages are resident loops that report through readiness; the rest run
# once per window and are recorded in stage_runs.
RESIDENT_SCHEDULES = frozenset({"INTERVAL", "CONTINUOUS"})

STAGES_V1: dict[str, StageDefinition] = {
    stage.stage_id: stage for stage in (
        StageDefinition("REFERENCE", "shared", (), DATA_LANE, "INTERVAL"),
        StageDefinition("EQUITY_INGEST", "equity", ("REFERENCE",), DATA_LANE, "WINDOW"),
        StageDefinition("EQUITY_EVIDENCE", "equity", ("EQUITY_INGEST",), DATA_LANE, "WINDOW"),
        StageDefinition("PORTAL_PUBLISH", "equity", ("EQUITY_EVIDENCE",), DATA_LANE, "COALESCED"),
        StageDefinition("STRATEGY_PUBLISH", "equity", ("EQUITY_EVIDENCE",), DATA_LANE, "COALESCED"),
        StageDefinition("STOCK_ALERTS", "equity", ("EQUITY_EVIDENCE",), DATA_LANE, "WINDOW"),
        StageDefinition("OPTIONS_ACQUIRE", "options", ("REFERENCE",), OPTIONS_LANE, "WINDOW"),
        StageDefinition("OPTIONS_ADMISSION", "options", ("OPTIONS_ACQUIRE",), OPTIONS_LANE, "SESSION"),
        StageDefinition("OPTIONS_CYCLE", "options", ("OPTIONS_ADMISSION",), OPTIONS_LANE, "WINDOW", True),
        StageDefinition("OPTIONS_FLOW", "options", ("REFERENCE",), OPTIONS_LANE, "CONTINUOUS"),
    )
}

STAGES_V2: dict[str, StageDefinition] = {
    stage.stage_id: stage for stage in (
        StageDefinition("REFERENCE", "shared", (), DATA_LANE, "INTERVAL"),
        StageDefinition("EQUITY_INGEST", "equity", ("REFERENCE",), DATA_LANE, "WINDOW",
                        deadline_offset_seconds=120),
        StageDefinition("EQUITY_MATERIALIZE", "equity", ("EQUITY_INGEST",), DATA_LANE, "WINDOW",
                        deadline_offset_seconds=120),
        StageDefinition("STOCK_ALERTS", "equity", ("EQUITY_MATERIALIZE",), ALERT_LANE, "WINDOW",
                        deadline_offset_seconds=120),
        StageDefinition("EQUITY_PROJECTION", "equity", ("EQUITY_MATERIALIZE",), PORTAL_LANE, "COALESCED",
                        deadline_offset_seconds=300),
        StageDefinition("PORTAL_PUBLISH", "equity", ("EQUITY_PROJECTION",), PORTAL_LANE, "COALESCED",
                        deadline_offset_seconds=300),
        StageDefinition("STRATEGY_PUBLISH", "equity", ("EQUITY_MATERIALIZE",), PORTAL_LANE, "COALESCED",
                        deadline_offset_seconds=300),
        StageDefinition("OPTIONS_ACQUIRE", "options", ("REFERENCE",), OPTIONS_LANE, "WINDOW"),
        StageDefinition("OPTIONS_ADMISSION", "options", ("OPTIONS_ACQUIRE",), OPTIONS_LANE, "SESSION"),
        StageDefinition("OPTIONS_CYCLE", "options", ("OPTIONS_ADMISSION",), OPTIONS_LANE, "WINDOW", True),
        StageDefinition("OPTIONS_FLOW", "options", ("REFERENCE",), OPTIONS_LANE, "CONTINUOUS"),
    )
}

STAGES_V3: dict[str, StageDefinition] = {
    stage.stage_id: stage for stage in (
        StageDefinition("REFERENCE", "shared", (), DATA_LANE, "INTERVAL"),
        StageDefinition(
            "EQUITY_INPUT_GATE",
            "equity",
            ("REFERENCE",),
            DATA_LANE,
            "WINDOW",
            deadline_offset_seconds=120,
        ),
        StageDefinition(
            "EQUITY_MATERIALIZE",
            "equity",
            ("EQUITY_INPUT_GATE",),
            DATA_LANE,
            "WINDOW",
            deadline_offset_seconds=120,
        ),
        StageDefinition(
            "STOCK_ALERTS",
            "equity",
            ("EQUITY_MATERIALIZE",),
            ALERT_LANE,
            "WINDOW",
            deadline_offset_seconds=120,
        ),
        StageDefinition(
            "EQUITY_PROJECTION",
            "equity",
            ("EQUITY_MATERIALIZE",),
            PORTAL_LANE,
            "COALESCED",
            deadline_offset_seconds=300,
        ),
        StageDefinition(
            "PORTAL_PUBLISH",
            "equity",
            ("EQUITY_PROJECTION",),
            PORTAL_LANE,
            "COALESCED",
            deadline_offset_seconds=300,
        ),
        StageDefinition(
            "STRATEGY_PUBLISH",
            "equity",
            ("EQUITY_MATERIALIZE",),
            PORTAL_LANE,
            "COALESCED",
            deadline_offset_seconds=300,
        ),
    )
}

STAGES_V4: dict[str, StageDefinition] = {
    stage.stage_id: stage for stage in (
        *STAGES_V3.values(),
        StageDefinition(
            "OPTIONS_ACQUIRE",
            "options",
            ("REFERENCE",),
            OPTIONS_LANE,
            "SESSION",
        ),
        StageDefinition(
            "OPTIONS_ADMISSION",
            "options",
            ("OPTIONS_ACQUIRE",),
            OPTIONS_LANE,
            "SESSION",
        ),
        StageDefinition(
            "OPTIONS_FLOW",
            "options",
            ("REFERENCE",),
            OPTIONS_LANE,
            "CONTINUOUS",
        ),
        StageDefinition(
            "OPTIONS_INPUT_GATE",
            "options",
            ("OPTIONS_ADMISSION", "EQUITY_MATERIALIZE"),
            OPTIONS_LANE,
            "WINDOW",
        ),
        StageDefinition(
            "OPTIONS_CYCLE",
            "options",
            ("OPTIONS_INPUT_GATE",),
            OPTIONS_LANE,
            "WINDOW",
            True,
        ),
        StageDefinition(
            "OPTIONS_PROJECTION",
            "options",
            ("OPTIONS_CYCLE",),
            OPTIONS_LANE,
            "COALESCED",
        ),
        StageDefinition(
            "OPTIONS_PUBLISH",
            "options",
            ("OPTIONS_PROJECTION",),
            OPTIONS_LANE,
            "COALESCED",
        ),
    )
}

STAGE_CATALOGS = {
    STAGE_CATALOG_VERSION: STAGES_V1,
    STAGE_CATALOG_VERSION_V2: STAGES_V2,
    STAGE_CATALOG_VERSION_V3: STAGES_V3,
    STAGE_CATALOG_VERSION_V4: STAGES_V4,
}


class Action(str, Enum):
    RUN = "RUN"
    WAIT = "WAIT"
    BLOCK_PREREQ = "BLOCK_PREREQ"
    DONE = "DONE"
    SKIPPED_DISABLED = "SKIPPED_DISABLED"


@dataclass(frozen=True, slots=True)
class Decision:
    stage_id: str
    action: Action
    blocked_by: str | None = None


def _satisfied(
    stage: str,
    states: Mapping[str, str],
    readiness: Mapping[str, str],
    stages: Mapping[str, StageDefinition],
    enabled: frozenset[str] | set[str],
) -> bool:
    """A prerequisite is satisfied by a completed run in this window, or by READY readiness
    for window-independent stages (for example the six-hourly reference refresh)."""
    return states.get(stage) == "COMPLETE" or (
        (
            stages[stage].schedule_kind in RESIDENT_SCHEDULES
            or stage not in enabled
        )
        and readiness.get(stage) == "READY"
        and stage not in states
    )


def decide_window(
    enabled: frozenset[str] | set[str],
    states: Mapping[str, str],
    readiness: Mapping[str, str] | None = None,
    stages: Mapping[str, StageDefinition] = STAGES_V1,
) -> dict[str, Decision]:
    """Return one decision per stage for the current state of a window."""
    readiness = readiness or {}
    unknown = set(enabled) - set(stages)
    if unknown:
        raise ValueError(f"unknown stages: {sorted(unknown)}")
    decisions: dict[str, Decision] = {}
    for stage_id, definition in stages.items():
        if stage_id not in enabled:
            decisions[stage_id] = Decision(stage_id, Action.SKIPPED_DISABLED)
            continue
        own = states.get(stage_id)
        if own in TERMINAL_STATES:
            decisions[stage_id] = Decision(stage_id, Action.DONE)
            continue
        if own == "RUNNING":
            decisions[stage_id] = Decision(stage_id, Action.WAIT)
            continue
        blocking = next((item for item in definition.prerequisites
                         if states.get(item) in NON_COMPLETING_STATES), None)
        if blocking is not None:
            decisions[stage_id] = Decision(stage_id, Action.BLOCK_PREREQ, blocked_by=blocking)
        elif all(
            _satisfied(item, states, readiness, stages, enabled)
            for item in definition.prerequisites
        ):
            decisions[stage_id] = Decision(stage_id, Action.RUN)
        else:
            decisions[stage_id] = Decision(stage_id, Action.WAIT)
    return decisions


def startable_stages(
    enabled: frozenset[str] | set[str],
    readiness: Mapping[str, str],
    stages: Mapping[str, StageDefinition] = STAGES_V1,
) -> frozenset[str]:
    """Stages a resident service may start with. Only a stage's own readiness and its
    declared prerequisites matter; a BLOCKED Options stage never prevents the others. A BLOCKED
    shared prerequisite (REFERENCE) is a real shared-source failure and holds its actual
    dependents; a shared prerequisite need not be enabled in this plan."""
    startable = set()
    for stage_id in enabled:
        definition = stages[stage_id]
        if readiness.get(stage_id) == "BLOCKED":
            continue
        if all(readiness.get(item) != "BLOCKED" and (item in enabled or stages[item].domain == "shared")
               for item in definition.prerequisites):
            startable.add(stage_id)
    return frozenset(startable)


def lanes_required(enabled: frozenset[str] | set[str],
                   stages: Mapping[str, StageDefinition] = STAGES_V1) -> frozenset[str]:
    """Lanes a plan runs; an equity-only plan never opens the options lane."""
    return frozenset(stages[stage_id].lane for stage_id in enabled)
