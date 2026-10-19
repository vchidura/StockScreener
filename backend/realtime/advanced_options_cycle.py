"""P4B native detector lane adapter with explicit complete outcomes."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from .advanced_options_control_plane import AdvancedOptionsControlPlanePlan
from .advanced_options_input import AdvancedOptionsInputGatePlan
from .advanced_options_reference_state import (
    AdvancedOptionsReferenceStateRepository,
)
from .contracts import canonical_json, canonical_sha256
from .option_portal_adapter import RealtimeOptionPackage
from .option_portal_repository import persist_realtime_option_package_cursor
from .option_source_flow import OptionSourceFlowIdentity
from .orchestration_repository import (
    OPTIONS_CYCLE,
    OPTIONS_INPUT_READY,
    CycleReceiptSpec,
    build_options_receipt_spec,
)
from .preflight import DetectorFamily


O2_MODEL_NOT_ACCEPTED = "O2_MODEL_NOT_ACCEPTED"
DETECTOR_EXECUTOR_NOT_CONFIGURED = "DETECTOR_EXECUTOR_NOT_CONFIGURED"
EVALUATION_STATUSES = frozenset({
    "SELECTED",
    "NOT_SELECTED",
    "REPEAT",
    "OBSERVATION",
})


@dataclass(frozen=True, slots=True)
class AdvancedOptionsDatasetFlowIdentity:
    source_flow: OptionSourceFlowIdentity
    advanced_flow_policy_sha256: str
    contract_scope_sha256: str
    manifest_sha256: str
    slot_sha256: str
    runtime_policy_sha256: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.source_flow, OptionSourceFlowIdentity)
            or self.source_flow.identity_version != 3
        ):
            raise ValueError(
                "dataset flow identity requires source-flow v3"
            )
        for name in (
            "advanced_flow_policy_sha256",
            "contract_scope_sha256",
            "manifest_sha256",
            "slot_sha256",
            "runtime_policy_sha256",
        ):
            value = getattr(self, name)
            if (
                not isinstance(value, str)
                or len(value) != 64
                or any(
                    character not in "0123456789abcdef"
                    for character in value
                )
            ):
                raise ValueError(f"{name} must be a SHA-256 digest")
        if (
            self.source_flow.advanced_flow_policy_sha256
            != self.advanced_flow_policy_sha256
            or self.source_flow.contract_scope_sha256
            != self.contract_scope_sha256
        ):
            raise ValueError(
                "dataset flow identity does not match source flow"
            )


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


class NativeLaneState(str, Enum):
    COMPLETE = "COMPLETE"
    BLOCKED = "BLOCKED"
    UNAVAILABLE = "UNAVAILABLE"


class CycleLaneState(str, Enum):
    COMPLETE_SELECTED = "COMPLETE_SELECTED"
    COMPLETE_ZERO = "COMPLETE_ZERO"
    BLOCKED = "BLOCKED"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class AdvancedOptionsNativeEvaluation:
    event_id: UUID
    underlyer: str
    selection_status: str
    observed_at: datetime
    payload: Mapping[str, object]
    package: RealtimeOptionPackage | None = None

    def __post_init__(self) -> None:
        underlyer = self.underlyer.strip().upper()
        status = self.selection_status.strip().upper()
        observed = _utc(self.observed_at, "observed_at")
        if not underlyer or status not in EVALUATION_STATUSES:
            raise ValueError("Advanced Options native evaluation is invalid")
        if status == "SELECTED" and self.package is None:
            raise ValueError("selected native evaluation requires a package")
        canonical_json(self.payload)
        object.__setattr__(self, "underlyer", underlyer)
        object.__setattr__(self, "selection_status", status)
        object.__setattr__(self, "observed_at", observed)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsNativeLaneResult:
    state: NativeLaneState
    reason_codes: tuple[str, ...]
    evaluations: tuple[AdvancedOptionsNativeEvaluation, ...] = ()

    def __post_init__(self) -> None:
        reasons = tuple(reason.strip().upper() for reason in self.reason_codes)
        if any(not reason for reason in reasons):
            raise ValueError("native lane reason code cannot be blank")
        if self.state is NativeLaneState.COMPLETE and reasons:
            raise ValueError("complete native lane cannot have failure reasons")
        if self.state is not NativeLaneState.COMPLETE and not reasons:
            raise ValueError("blocked or unavailable lane requires reasons")
        if self.state is not NativeLaneState.COMPLETE and self.evaluations:
            raise ValueError("incomplete native lane cannot retain evaluations")
        object.__setattr__(self, "reason_codes", reasons)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsDetectorContext:
    input_gate: AdvancedOptionsInputGatePlan
    input_receipt: CycleReceiptSpec
    detector: DetectorFamily
    cutoff: datetime


@dataclass(frozen=True, slots=True)
class AdvancedOptionsDetectorEvaluation:
    evaluation_id: UUID
    dataset_id: UUID
    event_id: UUID
    detector: DetectorFamily
    underlyer: str
    selection_status: str
    observed_at: datetime
    payload_sha256: str
    payload: Mapping[str, object]
    package: RealtimeOptionPackage | None


@dataclass(frozen=True, slots=True)
class AdvancedOptionsLaneOutcome:
    detector: DetectorFamily
    state: CycleLaneState
    reason_codes: tuple[str, ...]
    evaluation_ids: tuple[UUID, ...]
    outcome_sha256: str


@dataclass(frozen=True, slots=True)
class AdvancedOptionsCycleResult:
    dataset_id: UUID
    dataset_key: str
    dataset_status: str
    input_gate: AdvancedOptionsInputGatePlan
    input_receipt: CycleReceiptSpec
    outcomes: tuple[AdvancedOptionsLaneOutcome, ...]
    evaluations: tuple[AdvancedOptionsDetectorEvaluation, ...]
    cycle_sha256: str
    receipt: CycleReceiptSpec


NativeDetectorExecutor = Callable[
    [AdvancedOptionsDetectorContext],
    AdvancedOptionsNativeLaneResult,
]


def run_advanced_options_cycle(
    *,
    control_plane: AdvancedOptionsControlPlanePlan,
    input_gate: AdvancedOptionsInputGatePlan,
    input_receipt: CycleReceiptSpec,
    executors: Mapping[DetectorFamily, NativeDetectorExecutor],
    cutoff: datetime,
    reference_generation: int,
    reference_repository: AdvancedOptionsReferenceStateRepository,
    build_id: str,
) -> AdvancedOptionsCycleResult:
    checked_cutoff = _utc(cutoff, "cutoff")
    reference_state = reference_repository.load_generation(
        reference_generation
    )
    if (
        input_gate.closure.control_plane_plan_id != control_plane.plan_id
        or input_receipt.receipt_kind != OPTIONS_INPUT_READY
        or input_receipt.input_sha256 != input_gate.closure_sha256
        or input_receipt.cutoff != checked_cutoff
        or input_receipt.reference_generation
        != input_gate.closure.reference_generation
        or reference_generation != input_gate.closure.reference_generation
        or reference_state is None
        or reference_state.state_id != input_gate.closure.reference_state_id
        or reference_state.state_sha256
        != input_gate.closure.reference_state_sha256
        or not (
            input_gate.closure.reference_known_at
            <= checked_cutoff
            < input_gate.closure.reference_valid_until
        )
    ):
        raise ValueError("Advanced Options cycle input receipt changed")
    required = tuple(sorted(
        control_plane.manifest_policy.required_detectors,
        key=lambda value: value.value,
    ))
    dataset_key = (
        f"advanced-options:{control_plane.cohort_id}:"
        f"{checked_cutoff.isoformat()}:{input_gate.closure_sha256}"
    )
    dataset_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-options-dataset:{dataset_key}",
    )
    reference_repository.freeze_for_cycle(
        reference_state,
        cycle_id=dataset_id,
        consumed_at=checked_cutoff,
    )
    evaluations = []
    outcomes = []
    for detector in required:
        if detector is DetectorFamily.O2:
            native = AdvancedOptionsNativeLaneResult(
                NativeLaneState.UNAVAILABLE,
                (O2_MODEL_NOT_ACCEPTED,),
            )
        else:
            executor = executors.get(detector)
            native = (
                executor(AdvancedOptionsDetectorContext(
                    input_gate,
                    input_receipt,
                    detector,
                    checked_cutoff,
                ))
                if executor is not None
                else AdvancedOptionsNativeLaneResult(
                    NativeLaneState.BLOCKED,
                    (DETECTOR_EXECUTOR_NOT_CONFIGURED,),
                )
            )
        if not isinstance(native, AdvancedOptionsNativeLaneResult):
            raise TypeError("native detector executor returned an invalid result")
        lane_evaluations = []
        for row in native.evaluations:
            if row.underlyer not in control_plane.universe.option_underlyings:
                raise ValueError("native detector evaluation changed underlying scope")
            if row.package is not None and (
                row.package.cohort_id != control_plane.cohort_id
                or row.package.provider_tier
                != control_plane.binding.options_tier
            ):
                raise ValueError("native detector package changed cohort scope")
            payload_sha256 = canonical_sha256(row.payload)
            evaluation_id = uuid5(
                NAMESPACE_URL,
                "stockscreener:advanced-options-evaluation:"
                f"{dataset_id}:{detector.value}:{row.event_id}:"
                f"{payload_sha256}",
            )
            lane_evaluations.append(AdvancedOptionsDetectorEvaluation(
                evaluation_id,
                dataset_id,
                row.event_id,
                detector,
                row.underlyer,
                row.selection_status,
                row.observed_at,
                payload_sha256,
                row.payload,
                row.package,
            ))
        evaluations.extend(lane_evaluations)
        if native.state is NativeLaneState.BLOCKED:
            state = CycleLaneState.BLOCKED
        elif native.state is NativeLaneState.UNAVAILABLE:
            state = CycleLaneState.UNAVAILABLE
        elif any(
            row.selection_status == "SELECTED"
            for row in lane_evaluations
        ):
            state = CycleLaneState.COMPLETE_SELECTED
        else:
            state = CycleLaneState.COMPLETE_ZERO
        outcome_identity = {
            "detector": detector,
            "state": state,
            "reason_codes": native.reason_codes,
            "evaluation_ids": tuple(
                row.evaluation_id for row in lane_evaluations
            ),
        }
        outcomes.append(AdvancedOptionsLaneOutcome(
            detector,
            state,
            native.reason_codes,
            tuple(row.evaluation_id for row in lane_evaluations),
            canonical_sha256(outcome_identity),
        ))
    cycle_identity = {
        "contract": "advanced-options-cycle-v1",
        "dataset_id": dataset_id,
        "input_receipt_sha256": canonical_sha256(input_receipt),
        "input_closure_sha256": input_gate.closure_sha256,
        "model_bundle_sha256": control_plane.pins.model_bundle_sha256,
        "valuation_policy_sha256": (
            control_plane.pins.valuation_policy_sha256
        ),
        "outcomes": tuple(row.outcome_sha256 for row in outcomes),
        "build_id": build_id,
    }
    cycle_sha256 = canonical_sha256(cycle_identity)
    receipt = build_options_receipt_spec(
        receipt_kind=OPTIONS_CYCLE,
        cutoff=checked_cutoff,
        reference_generation=reference_generation,
        universe_sha256=control_plane.universe.sha256,
        input_sha256=cycle_sha256,
        build_id=build_id,
    )
    dataset_status = (
        "DEGRADED"
        if any(
            row.state in {CycleLaneState.BLOCKED, CycleLaneState.UNAVAILABLE}
            for row in outcomes
        )
        else "COMPLETE"
    )
    return AdvancedOptionsCycleResult(
        dataset_id,
        dataset_key,
        dataset_status,
        input_gate,
        input_receipt,
        tuple(outcomes),
        tuple(evaluations),
        cycle_sha256,
        receipt,
    )


def persist_advanced_options_cycle_cursor(
    cursor,
    cycle: AdvancedOptionsCycleResult,
    *,
    control_plane: AdvancedOptionsControlPlanePlan,
    flow_identity: AdvancedOptionsDatasetFlowIdentity | None = None,
) -> None:
    if flow_identity is not None:
        if not isinstance(
            flow_identity,
            AdvancedOptionsDatasetFlowIdentity,
        ):
            raise TypeError("dataset flow identity is invalid")
        if (
            flow_identity.manifest_sha256
            != cycle.input_gate.closure.manifest_sha256
            or flow_identity.slot_sha256
            != control_plane.slot_set_sha256
        ):
            raise ValueError(
                "dataset flow identity does not match cycle inputs"
            )
    summary = {
        "input_sha256": cycle.input_gate.closure_sha256,
        "cycle_sha256": cycle.cycle_sha256,
        "lanes": [
            {
                "detector": row.detector.value,
                "state": row.state.value,
                "reason_codes": list(row.reason_codes),
                "outcome_sha256": row.outcome_sha256,
            }
            for row in cycle.outcomes
        ],
        "o2_activation": False,
        "flow_identity": (
            None
            if flow_identity is None
            else {
                "source_flow_id": str(
                    flow_identity.source_flow.source_flow_id
                ),
                "advanced_flow_policy_sha256": (
                    flow_identity.advanced_flow_policy_sha256
                ),
                "contract_scope_sha256": (
                    flow_identity.contract_scope_sha256
                ),
                "manifest_sha256": flow_identity.manifest_sha256,
                "slot_sha256": flow_identity.slot_sha256,
                "runtime_policy_sha256": (
                    flow_identity.runtime_policy_sha256
                ),
            }
        ),
    }
    cursor.execute(
        "INSERT INTO rt.detector_datasets ("
        "dataset_id,cohort_id,provider_tier,dataset_key,session_date,"
        "status,detector_ids,source_manifest_sha256,started_at,"
        "completed_at,summary,flow_identity_version,source_flow_id,"
        "advanced_flow_policy_sha256,contract_scope_sha256,slot_sha256,"
        "runtime_policy_sha256) VALUES ("
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (dataset_id) DO NOTHING",
        (
            str(cycle.dataset_id),
            str(control_plane.cohort_id),
            control_plane.binding.provider_tier.value,
            cycle.dataset_key,
            control_plane.session_date,
            cycle.dataset_status,
            [row.detector.value for row in cycle.outcomes],
            cycle.input_gate.closure.manifest_sha256,
            cycle.input_receipt.cutoff,
            cycle.receipt.cutoff,
            canonical_json(summary),
            1 if flow_identity is None else 2,
            (
                None
                if flow_identity is None
                else str(flow_identity.source_flow.source_flow_id)
            ),
            (
                None
                if flow_identity is None
                else flow_identity.advanced_flow_policy_sha256
            ),
            (
                None
                if flow_identity is None
                else flow_identity.contract_scope_sha256
            ),
            None if flow_identity is None else flow_identity.slot_sha256,
            (
                None
                if flow_identity is None
                else flow_identity.runtime_policy_sha256
            ),
        ),
    )
    packages = {
        row.package.candidate_id: row.package
        for row in cycle.evaluations
        if row.package is not None
    }
    for package in packages.values():
        persist_realtime_option_package_cursor(cursor, package)
    for row in cycle.evaluations:
        cursor.execute(
            "INSERT INTO rt.detector_evaluations ("
            "evaluation_id,dataset_id,event_id,detector_id,underlyer,"
            "selection_status,observed_at,payload_sha256,payload"
            ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (evaluation_id) DO NOTHING",
            (
                str(row.evaluation_id),
                str(row.dataset_id),
                str(row.event_id),
                row.detector.value,
                row.underlyer,
                row.selection_status,
                row.observed_at,
                row.payload_sha256,
                canonical_json(row.payload),
            ),
        )
