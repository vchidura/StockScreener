"""Pure Advanced O1/O3/S1/S2 adapters over exact frozen cycle inputs."""
from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Mapping
from uuid import UUID

from options.dual_origin import ActivitySource

from .advanced_options_cycle import (
    AdvancedOptionsDetectorContext,
    AdvancedOptionsNativeEvaluation,
    AdvancedOptionsNativeLaneResult,
    NativeDetectorExecutor,
    NativeLaneState,
)
from .contracts import canonical_sha256
from .detector_adapters import (
    run_canonical_stock_signal_adapter,
    run_o1_adapter,
    run_o3_adapter,
)
from .option_portal_adapter import RealtimeOptionPackage
from .preflight import DetectorFamily, ProviderTier


def _plain(value):
    if hasattr(value, "model_dump"):
        return _plain(value.model_dump(mode="json"))
    if is_dataclass(value):
        return _plain(asdict(value))
    if isinstance(value, Mapping):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_plain(item) for item in value]
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    raise TypeError(
        f"Advanced Options detector payload is unsupported: {type(value).__name__}"
    )


def _input_sha256(
    context: AdvancedOptionsDetectorContext,
) -> str:
    return canonical_sha256({
        "input_closure_sha256": context.input_gate.closure_sha256,
        "input_receipt_sha256": canonical_sha256(context.input_receipt),
        "detector": context.detector,
        "cutoff": context.cutoff,
    })


@dataclass(frozen=True, slots=True)
class AdvancedOptionsLaneInputs:
    invocations: tuple[object, ...] = ()
    blocked_reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        reasons = tuple(
            reason.strip().upper()
            for reason in self.blocked_reason_codes
        )
        if any(not reason for reason in reasons):
            raise ValueError("Advanced Options lane reason cannot be blank")
        if reasons and self.invocations:
            raise ValueError(
                "blocked Advanced Options lane cannot retain invocations"
            )
        object.__setattr__(self, "blocked_reason_codes", reasons)


@dataclass(frozen=True, slots=True)
class AdvancedO1Invocation:
    event_id: UUID
    source: ActivitySource
    package: RealtimeOptionPackage | None = None


@dataclass(frozen=True, slots=True)
class AdvancedO3Invocation:
    event_id: UUID
    candidate: object
    decision: object
    activity_contract_id: int
    snapshots: tuple
    structural_invalidation: Decimal
    scheduled_cycle: datetime
    package: RealtimeOptionPackage


@dataclass(frozen=True, slots=True)
class AdvancedStockInvocation:
    detector: DetectorFamily
    security_id: UUID
    underlyer: str
    bars: tuple
    market_cutoff: datetime
    decision_at: datetime
    session_close: datetime
    packages: Mapping[UUID, RealtimeOptionPackage]

    def __post_init__(self) -> None:
        if self.detector not in {DetectorFamily.S1, DetectorFamily.S2}:
            raise ValueError("Advanced stock invocation requires S1 or S2")


def _blocked_or_none(
    context: AdvancedOptionsDetectorContext,
    expected: DetectorFamily,
    inputs: AdvancedOptionsLaneInputs,
) -> AdvancedOptionsNativeLaneResult | None:
    if context.detector is not expected:
        raise ValueError("Advanced Options executor detector changed")
    if inputs.blocked_reason_codes:
        return AdvancedOptionsNativeLaneResult(
            NativeLaneState.BLOCKED,
            inputs.blocked_reason_codes,
        )
    return None


def build_advanced_o1_executor(
    inputs: AdvancedOptionsLaneInputs,
) -> NativeDetectorExecutor:
    if any(not isinstance(row, AdvancedO1Invocation) for row in inputs.invocations):
        raise TypeError("Advanced O1 invocation is invalid")

    def execute(
        context: AdvancedOptionsDetectorContext,
    ) -> AdvancedOptionsNativeLaneResult:
        blocked = _blocked_or_none(context, DetectorFamily.O1, inputs)
        if blocked is not None:
            return blocked
        evaluations = []
        for invocation in inputs.invocations:
            result = run_o1_adapter(
                invocation.source,
                cohort_id=context.input_gate.closure.cycle_input.cohort_id,
                provider_tier=ProviderTier.MASSIVE_ADVANCED,
                input_sha256=_input_sha256(context),
                market_cutoff=invocation.source.market_time,
                decision_at=invocation.source.received_at,
            )
            finding = result.output
            if finding.disposition == "UNAVAILABLE":
                return AdvancedOptionsNativeLaneResult(
                    NativeLaneState.UNAVAILABLE,
                    tuple(finding.reasons),
                )
            selected = (
                finding.disposition == "DETECTED"
                and invocation.package is not None
            )
            evaluations.append(AdvancedOptionsNativeEvaluation(
                invocation.event_id,
                invocation.source.underlyer,
                (
                    "SELECTED"
                    if selected
                    else "OBSERVATION"
                    if finding.disposition == "DETECTED"
                    else "NOT_SELECTED"
                ),
                finding.decision_at,
                _plain(finding),
                invocation.package if selected else None,
            ))
        return AdvancedOptionsNativeLaneResult(
            NativeLaneState.COMPLETE,
            (),
            tuple(evaluations),
        )

    return execute


def build_advanced_o3_executor(
    inputs: AdvancedOptionsLaneInputs,
) -> NativeDetectorExecutor:
    if any(not isinstance(row, AdvancedO3Invocation) for row in inputs.invocations):
        raise TypeError("Advanced O3 invocation is invalid")

    def execute(
        context: AdvancedOptionsDetectorContext,
    ) -> AdvancedOptionsNativeLaneResult:
        blocked = _blocked_or_none(context, DetectorFamily.O3, inputs)
        if blocked is not None:
            return blocked
        evaluations = []
        for invocation in inputs.invocations:
            if invocation.scheduled_cycle != context.cutoff:
                raise ValueError("Advanced O3 scheduled cycle changed")
            result = run_o3_adapter(
                invocation.candidate,
                invocation.decision,
                cohort_id=context.input_gate.closure.cycle_input.cohort_id,
                provider_tier=ProviderTier.MASSIVE_ADVANCED,
                input_sha256=_input_sha256(context),
                activity_contract_id=invocation.activity_contract_id,
                snapshots=invocation.snapshots,
                structural_invalidation=(
                    invocation.structural_invalidation
                ),
                scheduled_cycle=invocation.scheduled_cycle,
            )
            observation = result.output
            evaluations.append(AdvancedOptionsNativeEvaluation(
                invocation.event_id,
                observation.underlyer,
                "SELECTED",
                observation.decision_at,
                _plain(observation),
                invocation.package,
            ))
        return AdvancedOptionsNativeLaneResult(
            NativeLaneState.COMPLETE,
            (),
            tuple(evaluations),
        )

    return execute


def build_advanced_stock_executor(
    detector: DetectorFamily,
    inputs: AdvancedOptionsLaneInputs,
) -> NativeDetectorExecutor:
    if detector not in {DetectorFamily.S1, DetectorFamily.S2}:
        raise ValueError("Advanced stock executor requires S1 or S2")
    if any(
        not isinstance(row, AdvancedStockInvocation)
        or row.detector is not detector
        for row in inputs.invocations
    ):
        raise TypeError("Advanced stock invocation is invalid")

    def execute(
        context: AdvancedOptionsDetectorContext,
    ) -> AdvancedOptionsNativeLaneResult:
        blocked = _blocked_or_none(context, detector, inputs)
        if blocked is not None:
            return blocked
        evaluations = []
        for invocation in inputs.invocations:
            result = run_canonical_stock_signal_adapter(
                detector_id=detector.value,
                security_id=invocation.security_id,
                underlyer=invocation.underlyer,
                bars=invocation.bars,
                cohort_id=context.input_gate.closure.cycle_input.cohort_id,
                provider_tier=ProviderTier.MASSIVE_ADVANCED,
                input_sha256=_input_sha256(context),
                market_cutoff=invocation.market_cutoff,
                decision_at=invocation.decision_at,
                session_close=invocation.session_close,
            )
            for signal in result.output:
                package = invocation.packages.get(signal.signal_id)
                evaluations.append(AdvancedOptionsNativeEvaluation(
                    signal.signal_id,
                    signal.underlyer,
                    "SELECTED" if package is not None else "OBSERVATION",
                    signal.decision_at,
                    _plain(signal),
                    package,
                ))
        return AdvancedOptionsNativeLaneResult(
            NativeLaneState.COMPLETE,
            (),
            tuple(evaluations),
        )

    return execute


@dataclass(frozen=True, slots=True)
class AdvancedOptionsNativeInputBundle:
    o1: AdvancedOptionsLaneInputs
    o3: AdvancedOptionsLaneInputs
    s1: AdvancedOptionsLaneInputs
    s2: AdvancedOptionsLaneInputs

    def executors(self) -> Mapping[DetectorFamily, NativeDetectorExecutor]:
        return {
            DetectorFamily.O1: build_advanced_o1_executor(self.o1),
            DetectorFamily.O3: build_advanced_o3_executor(self.o3),
            DetectorFamily.S1: build_advanced_stock_executor(
                DetectorFamily.S1,
                self.s1,
            ),
            DetectorFamily.S2: build_advanced_stock_executor(
                DetectorFamily.S2,
                self.s2,
            ),
        }
