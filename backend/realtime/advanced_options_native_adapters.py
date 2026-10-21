"""Pure Advanced O1/O3/S1/S2 adapters over exact frozen cycle inputs."""
from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from types import SimpleNamespace
from typing import Mapping
from uuid import UUID

from options.domain import ContractType, OptionContractSnapshot
from options.dual_origin import (
    ActivitySource,
    LatestCompletedSignalDecision,
)
from options.strategies.domain import (
    CandidateKind,
    CandidateLeg,
    CandidateStatus,
    ExecutionEligibility,
    OptionCandidate,
    OptionSide,
    StructureRiskClass,
    StructureType,
)

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
    if isinstance(value, SimpleNamespace):
        return {
            str(key): _plain(item)
            for key, item in vars(value).items()
        }
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


def advanced_o3_invocation_document(
    invocation: AdvancedO3Invocation,
) -> dict[str, object]:
    if not isinstance(invocation, AdvancedO3Invocation):
        raise TypeError("Advanced O3 invocation is invalid")
    return {
        "contract": "advanced-options-o3-invocation-v1",
        "event_id": str(invocation.event_id),
        "candidate": _plain(invocation.candidate),
        "decision": _plain(invocation.decision),
        "decision_model": (
            "LATEST_COMPLETED_SIGNAL_DECISION"
            if hasattr(invocation.decision, "model_dump")
            else "ATTRIBUTE_RECORD"
        ),
        "activity_contract_id": invocation.activity_contract_id,
        "snapshots": _plain(invocation.snapshots),
        "snapshot_model": (
            "OPTION_CONTRACT_SNAPSHOT"
            if all(hasattr(row, "model_dump") for row in invocation.snapshots)
            else "ATTRIBUTE_RECORD"
        ),
        "structural_invalidation": str(
            invocation.structural_invalidation
        ),
        "scheduled_cycle": invocation.scheduled_cycle.isoformat(),
        "package_candidate_id": str(invocation.package.candidate_id),
    }


def _candidate_leg(value: Mapping[str, object]) -> CandidateLeg:
    return CandidateLeg(
        leg_index=int(value["leg_index"]),
        snapshot_id=UUID(str(value["snapshot_id"])),
        contract_id=int(value["contract_id"]),
        contract_ticker=str(value["contract_ticker"]),
        side=OptionSide(str(value["side"])),
        ratio=int(value["ratio"]),
        multiplier=int(value["multiplier"]),
        expiration_date=date.fromisoformat(str(value["expiration_date"])),
        strike=Decimal(str(value["strike"])),
        contract_type=ContractType(str(value["contract_type"])),
        spot=Decimal(str(value["spot"])),
        time_to_expiration_years=float(value["time_to_expiration_years"]),
        risk_free_rate=float(value["risk_free_rate"]),
        dividend_yield=float(value["dividend_yield"]),
        model_mark=Decimal(str(value["model_mark"])),
        local_iv=float(value["local_iv"]),
        local_delta=float(value["local_delta"]),
        local_gamma=float(value["local_gamma"]),
        local_theta_per_day=float(value["local_theta_per_day"]),
        local_vega_per_vol_point=float(value["local_vega_per_vol_point"]),
        local_rho_per_rate_point=float(value["local_rho_per_rate_point"]),
        source_market_time=datetime.fromisoformat(
            str(value["source_market_time"])
        ),
        mark_source=str(value["mark_source"]),
        model_version=str(value["model_version"]),
        quality_flags=tuple(value["quality_flags"]),
        valuation_policy_version=(
            None
            if value["valuation_policy_version"] is None
            else str(value["valuation_policy_version"])
        ),
        valuation_policy_sha256=(
            None
            if value["valuation_policy_sha256"] is None
            else str(value["valuation_policy_sha256"])
        ),
    )


def _candidate(value: Mapping[str, object]) -> OptionCandidate:
    return OptionCandidate(
        candidate_id=UUID(str(value["candidate_id"])),
        identity_sha256=str(value["identity_sha256"]),
        matrix_id=UUID(str(value["matrix_id"])),
        strategy_name=str(value["strategy_name"]),
        strategy_version=str(value["strategy_version"]),
        underlyer=str(value["underlyer"]),
        candidate_kind=CandidateKind(str(value["candidate_kind"])),
        strategy_archetype=str(value["strategy_archetype"]),
        persona_tags=tuple(value["persona_tags"]),
        structure_type=StructureType(str(value["structure_type"])),
        structure_risk_class=StructureRiskClass(
            str(value["structure_risk_class"])
        ),
        expiration_date=(
            None
            if value["expiration_date"] is None
            else date.fromisoformat(str(value["expiration_date"]))
        ),
        rank=int(value["rank"]),
        status=CandidateStatus(str(value["status"])),
        primary_metric_name=(
            None
            if value["primary_metric_name"] is None
            else str(value["primary_metric_name"])
        ),
        primary_metric_value=(
            None
            if value["primary_metric_value"] is None
            else float(value["primary_metric_value"])
        ),
        rank_components=dict(value["rank_components"]),
        primary_evidence=dict(value["primary_evidence"]),
        legs=tuple(_candidate_leg(row) for row in value["legs"]),
        net_premium=(
            None
            if value["net_premium"] is None
            else Decimal(str(value["net_premium"]))
        ),
        collateral_required=(
            None
            if value["collateral_required"] is None
            else Decimal(str(value["collateral_required"]))
        ),
        capital_at_risk=(
            None
            if value["capital_at_risk"] is None
            else Decimal(str(value["capital_at_risk"]))
        ),
        maximum_profit=(
            None
            if value["maximum_profit"] is None
            else Decimal(str(value["maximum_profit"]))
        ),
        maximum_loss=(
            None
            if value["maximum_loss"] is None
            else Decimal(str(value["maximum_loss"]))
        ),
        return_on_collateral=(
            None
            if value["return_on_collateral"] is None
            else float(value["return_on_collateral"])
        ),
        return_on_risk=(
            None
            if value["return_on_risk"] is None
            else float(value["return_on_risk"])
        ),
        breakevens=tuple(
            Decimal(str(item)) for item in value["breakevens"]
        ),
        execution_eligibility=(
            None
            if value["execution_eligibility"] is None
            else ExecutionEligibility(str(value["execution_eligibility"]))
        ),
        reason_codes=tuple(value["reason_codes"]),
        management_policy_version=(
            None
            if value["management_policy_version"] is None
            else str(value["management_policy_version"])
        ),
        management_policy=dict(value["management_policy"]),
        policy_sha256=str(value["policy_sha256"]),
        model_version=str(value["model_version"]),
        context_snapshot_id=(
            None
            if value["context_snapshot_id"] is None
            else UUID(str(value["context_snapshot_id"]))
        ),
        iv_context_id=(
            None
            if value["iv_context_id"] is None
            else UUID(str(value["iv_context_id"]))
        ),
        market_data_time=datetime.fromisoformat(
            str(value["market_data_time"])
        ),
        observed_time=datetime.fromisoformat(str(value["observed_time"])),
        valid_until=(
            None
            if value["valid_until"] is None
            else datetime.fromisoformat(str(value["valid_until"]))
        ),
    )


def decode_advanced_o3_invocation_document(
    document: Mapping[str, object],
    *,
    package: RealtimeOptionPackage,
) -> AdvancedO3Invocation:
    if document.get("contract") != "advanced-options-o3-invocation-v1":
        raise ValueError("Advanced O3 invocation document version changed")
    if UUID(str(document["package_candidate_id"])) != package.candidate_id:
        raise ValueError("Advanced O3 invocation package changed")
    def namespace(value):
        if isinstance(value, Mapping):
            return SimpleNamespace(**{
                str(key): namespace(item) for key, item in value.items()
            })
        if isinstance(value, list):
            return tuple(namespace(item) for item in value)
        return value

    if document["decision_model"] == "LATEST_COMPLETED_SIGNAL_DECISION":
        decision = LatestCompletedSignalDecision.model_validate(
            document["decision"]
        )
    else:
        decision_document = dict(document["decision"])
        decision_document["decision_at"] = datetime.fromisoformat(
            str(decision_document["decision_at"])
        )
        decision_document["valid_until"] = datetime.fromisoformat(
            str(decision_document["valid_until"])
        )
        activity = dict(decision_document["activity"])
        activity["episode_id"] = UUID(str(activity["episode_id"]))
        decision_document["activity"] = SimpleNamespace(**activity)
        decision = SimpleNamespace(**decision_document)
    snapshots = (
        tuple(
            OptionContractSnapshot.model_validate(row)
            for row in document["snapshots"]
        )
        if document["snapshot_model"] == "OPTION_CONTRACT_SNAPSHOT"
        else tuple(
            SimpleNamespace(**{
                **{
                    str(key): namespace(item)
                    for key, item in row.items()
                },
                "snapshot_id": UUID(str(row["snapshot_id"])),
            })
            for row in document["snapshots"]
        )
    )
    invocation = AdvancedO3Invocation(
        event_id=UUID(str(document["event_id"])),
        candidate=_candidate(document["candidate"]),
        decision=decision,
        activity_contract_id=int(document["activity_contract_id"]),
        snapshots=snapshots,
        structural_invalidation=Decimal(
            str(document["structural_invalidation"])
        ),
        scheduled_cycle=datetime.fromisoformat(
            str(document["scheduled_cycle"])
        ),
        package=package,
    )
    if invocation.candidate.candidate_id != package.candidate_id:
        raise ValueError("Advanced O3 candidate identity changed")
    return invocation


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
    plan_id: UUID | None = None
    input_sha256: str | None = None
    executor_membership_sha256: str | None = None

    def __post_init__(self) -> None:
        identities = (
            self.plan_id,
            self.input_sha256,
            self.executor_membership_sha256,
        )
        if any(value is None for value in identities) != all(
            value is None for value in identities
        ):
            raise ValueError("native input bundle identity is incomplete")
        if self.plan_id is not None:
            for value, name in (
                (self.input_sha256, "input_sha256"),
                (
                    self.executor_membership_sha256,
                    "executor_membership_sha256",
                ),
            ):
                if (
                    len(value) != 64
                    or any(
                        character not in "0123456789abcdef"
                        for character in value
                    )
                ):
                    raise ValueError(f"{name} must be a SHA-256 digest")

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
