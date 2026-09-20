"""Pure, research-only stock behavior gates for option detector assessments."""
from __future__ import annotations

from datetime import datetime
from types import MappingProxyType
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, model_validator

from equity.behavior import (
    Contract,
    Factor,
    Name,
    Sha256,
    StockBehaviorSnapshot,
)
from options.strategies.domain import StructureType


GateVerdict = Literal["PASS", "FAIL", "UNAVAILABLE", "NOT_APPLICABLE"]
GateRequirement = Literal["REQUIRED", "ADVISORY"]
GateComparator = Literal["EQUALS", "GT_ZERO", "LT_ZERO", "PRESENT"]
StockDisposition = Literal[
    "ELIGIBLE_RESEARCH", "BLOCKED", "UNAVAILABLE", "NOT_APPLICABLE"
]


class DetectorTarget(Contract):
    strategy_name: Name
    structure_type: Name
    directional_thesis: Literal["BULLISH", "BEARISH"]


class StockBehaviorGatePolicy(Contract):
    version: Literal["option_stock_behavior_gate_v1"] = "option_stock_behavior_gate_v1"
    profile: Literal["OPTIONS_SWING_V1"] = "OPTIONS_SWING_V1"
    target_horizon: Literal["SWING_1_TO_21_SESSIONS"] = "SWING_1_TO_21_SESSIONS"
    targets: tuple[DetectorTarget, ...] = (
        DetectorTarget(
            strategy_name="DIRECTIONAL_LONG_PREMIUM",
            structure_type="LONG_CALL",
            directional_thesis="BULLISH",
        ),
        DetectorTarget(
            strategy_name="DIRECTIONAL_LONG_PREMIUM",
            structure_type="LONG_PUT",
            directional_thesis="BEARISH",
        ),
        DetectorTarget(
            strategy_name="DIRECTIONAL_DEBIT_SPREAD",
            structure_type="CALL_DEBIT_VERTICAL",
            directional_thesis="BULLISH",
        ),
        DetectorTarget(
            strategy_name="DIRECTIONAL_DEBIT_SPREAD",
            structure_type="PUT_DEBIT_VERTICAL",
            directional_thesis="BEARISH",
        ),
    )
    required_trend_intervals: tuple[Literal["1d", "1h", "30m"], ...] = (
        "1d", "1h", "30m",
    )
    required_liquidity_metric: Literal["median_dollar_volume20"] = (
        "median_dollar_volume20"
    )
    advisory_trend_metric: Literal["adx14"] = "adx14"
    advisory_extension_metric: Literal["extension_ema21_atr"] = (
        "extension_ema21_atr"
    )
    advisory_participation_metric: Literal["daily_rvol20"] = "daily_rvol20"
    advisory_relative_strength_metric: Literal["excess_return20"] = "excess_return20"
    directional_slope_threshold: Literal[0.0] = 0.0
    threshold_policy: Literal["STRUCTURAL_SIGN_ONLY_NO_EMPIRICAL_THRESHOLDS"] = (
        "STRUCTURAL_SIGN_ONLY_NO_EMPIRICAL_THRESHOLDS"
    )


STOCK_BEHAVIOR_GATE_POLICY = StockBehaviorGatePolicy()
STOCK_BEHAVIOR_GATE_POLICIES = MappingProxyType({
    (STOCK_BEHAVIOR_GATE_POLICY.version, STOCK_BEHAVIOR_GATE_POLICY.sha256): STOCK_BEHAVIOR_GATE_POLICY,
})


def resolve_stock_behavior_gate_policy(version: str, policy_sha256: str) -> StockBehaviorGatePolicy:
    try:
        return STOCK_BEHAVIOR_GATE_POLICIES[(version, policy_sha256)]
    except KeyError as exc:
        raise ValueError("unsupported stock behavior gate version/hash") from exc


class StockBehaviorGate(Contract):
    gate_id: Name
    requirement: GateRequirement
    verdict: GateVerdict
    factor: Factor | None = None
    component_key: Name | None = None
    metric_id: Name | None = None
    formula_id: Name | None = None
    comparator: GateComparator
    actual_float: float | None = None
    actual_text: Name | None = None
    threshold_float: float | None = None
    source_evidence_ids: tuple[UUID, ...] = ()
    source_payload_sha256s: tuple[Sha256, ...] = ()
    source_market_times: tuple[AwareDatetime, ...] = ()
    reason_codes: tuple[Name, ...] = ()

    @model_validator(mode="after")
    def validate_gate(self):
        lengths = {
            len(self.source_evidence_ids),
            len(self.source_payload_sha256s),
            len(self.source_market_times),
        }
        if len(lengths) != 1:
            raise ValueError("gate source IDs, hashes and market times must align")
        if self.verdict == "PASS" and self.reason_codes:
            raise ValueError("passing gate cannot carry reasons")
        if self.verdict != "PASS" and not self.reason_codes:
            raise ValueError("non-passing gate requires reasons")
        return self


class StockBehaviorGateAssessment(Contract):
    schema_version: Literal["option_stock_behavior_assessment_v1"] = (
        "option_stock_behavior_assessment_v1"
    )
    candidate_id: UUID
    candidate_identity_sha256: Sha256 | None = None
    matrix_id: UUID
    underlyer: Name
    strategy_name: Name
    structure_type: Name
    directional_thesis: Name | None
    option_strategy_version: Name | None = None
    option_strategy_policy_sha256: Sha256 | None = None
    option_configuration_sha256: Sha256 | None = None
    option_market_policy_sha256: Sha256 | None = None
    option_analysis_policy_sha256: Sha256 | None = None
    option_market_time: AwareDatetime | None = None
    option_observed_at: AwareDatetime | None = None
    stock_market_cutoff: AwareDatetime | None = None
    launch_id: Name | None = None
    launch_manifest_sha256: Sha256 | None = None
    decision_at: AwareDatetime
    entry_deadline: AwareDatetime | None = None
    target_horizon: Name
    target_holding_until: AwareDatetime | None = None
    detector_policy_version: Name
    detector_policy_sha256: Sha256
    stock_snapshot_id: UUID | None = None
    stock_payload_sha256: Sha256 | None = None
    stock_definition_sha256: Sha256 | None = None
    stock_policy_sha256: Sha256 | None = None
    stock_profile: Name | None = None
    behavior_data_status: Name | None = None
    behavior_alignment_state: Name | None = None
    applicable: bool
    gates: tuple[StockBehaviorGate, ...]
    disposition: StockDisposition
    assessment_only: Literal[True] = True
    execution_permission: Literal[False] = False


def evaluate_option_stock_behavior(
    snapshot: StockBehaviorSnapshot | None,
    *,
    candidate_id: UUID,
    matrix_id: UUID,
    underlyer: str,
    strategy_name: str,
    structure_type: StructureType,
    directional_thesis: str | None,
    decision_at: datetime,
    candidate_identity_sha256: str | None = None,
    option_strategy_version: str | None = None,
    option_strategy_policy_sha256: str | None = None,
    option_configuration_sha256: str | None = None,
    option_market_policy_sha256: str | None = None,
    option_analysis_policy_sha256: str | None = None,
    option_market_time: datetime | None = None,
    option_observed_at: datetime | None = None,
    stock_market_cutoff: datetime | None = None,
    launch_id: str | None = None,
    launch_manifest_sha256: str | None = None,
    entry_deadline: datetime | None = None,
    target_holding_until: datetime | None = None,
    unavailable_reason: str = "STOCK_BEHAVIOR_SNAPSHOT_UNAVAILABLE",
    policy: StockBehaviorGatePolicy = STOCK_BEHAVIOR_GATE_POLICY,
) -> StockBehaviorGateAssessment:
    policy = resolve_stock_behavior_gate_policy(policy.version, policy.sha256)
    target = next((
        row for row in policy.targets
        if row.strategy_name == strategy_name
        and row.structure_type == structure_type.value
    ), None)
    common = dict(
        candidate_id=candidate_id, matrix_id=matrix_id,
        underlyer=underlyer.upper(), strategy_name=strategy_name,
        structure_type=structure_type.value,
        directional_thesis=directional_thesis,
        candidate_identity_sha256=candidate_identity_sha256,
        option_strategy_version=option_strategy_version,
        option_strategy_policy_sha256=option_strategy_policy_sha256,
        option_configuration_sha256=option_configuration_sha256,
        option_market_policy_sha256=option_market_policy_sha256,
        option_analysis_policy_sha256=option_analysis_policy_sha256,
        option_market_time=option_market_time,
        option_observed_at=option_observed_at,
        stock_market_cutoff=stock_market_cutoff,
        launch_id=launch_id,
        launch_manifest_sha256=launch_manifest_sha256,
        decision_at=decision_at, target_horizon=policy.target_horizon,
        entry_deadline=entry_deadline,
        target_holding_until=target_holding_until,
        detector_policy_version=policy.version,
        detector_policy_sha256=policy.sha256,
    )
    if target is None:
        return StockBehaviorGateAssessment(
            **common, applicable=False, disposition="NOT_APPLICABLE",
            gates=(StockBehaviorGate(
                gate_id="DETECTOR_SCOPE", requirement="REQUIRED",
                verdict="NOT_APPLICABLE", comparator="EQUALS",
                actual_text=strategy_name,
                reason_codes=("DETECTOR_PROFILE_NOT_REGISTERED",),
            ),),
        )
    if snapshot is None:
        return StockBehaviorGateAssessment(
            **common, applicable=True, disposition="UNAVAILABLE",
            gates=(StockBehaviorGate(
                gate_id="STOCK_BEHAVIOR_SNAPSHOT", requirement="REQUIRED",
                verdict="UNAVAILABLE", comparator="PRESENT",
                reason_codes=(unavailable_reason,),
            ),),
        )
    if snapshot.security_id is None or snapshot.ticker != underlyer.upper():
        raise ValueError("stock behavior snapshot identity does not match the candidate")
    assessment = snapshot.assess_at(decision_at)
    assessed_by_key = {component.key: component for component in assessment.components}
    source_by_key = {component.key: component for component in snapshot.components}
    gates = [StockBehaviorGate(
        gate_id="PROFILE_DATA_READY", requirement="REQUIRED",
        verdict="PASS" if assessment.data_status == "READY" else "UNAVAILABLE",
        comparator="EQUALS", actual_text=assessment.data_status,
        reason_codes=() if assessment.data_status == "READY" else (
            "REGISTERED_PROFILE_NOT_READY",
        ),
    )]
    thesis_matches = directional_thesis == target.directional_thesis
    gates.append(StockBehaviorGate(
        gate_id="DIRECTIONAL_THESIS_STRUCTURE", requirement="REQUIRED",
        verdict="PASS" if thesis_matches else "FAIL", comparator="EQUALS",
        actual_text=directional_thesis or "UNAVAILABLE",
        reason_codes=() if thesis_matches else ("DIRECTIONAL_THESIS_MISMATCH",),
    ))
    comparator = "GT_ZERO" if target.directional_thesis == "BULLISH" else "LT_ZERO"
    for interval in policy.required_trend_intervals:
        gates.append(_metric_gate(
            assessed_by_key, source_by_key,
            gate_id=f"TREND_SLOPE_{interval}", component_key=f"TREND.{interval}",
            metric_id="ema50_slope10_atr", factor="TREND", requirement="REQUIRED",
            comparator=comparator, threshold=policy.directional_slope_threshold,
            mismatch_reason="DIRECTIONAL_SLOPE_MISMATCH",
        ))
        gates.append(_metric_gate(
            assessed_by_key, source_by_key,
            gate_id=f"TREND_STRENGTH_EVIDENCE_{interval}",
            component_key=f"TREND.{interval}", metric_id=policy.advisory_trend_metric,
            factor="TREND", requirement="ADVISORY", comparator="PRESENT",
        ))
        gates.append(_metric_gate(
            assessed_by_key, source_by_key,
            gate_id=f"EXTENSION_EVIDENCE_{interval}",
            component_key=f"LOCATION.{interval}",
            metric_id=policy.advisory_extension_metric, factor="LOCATION",
            requirement="ADVISORY", comparator="PRESENT",
        ))
    gates.append(_metric_gate(
        assessed_by_key, source_by_key,
        gate_id="UNDERLYING_LIQUIDITY_EVIDENCE", component_key="PARTICIPATION.1d",
        metric_id=policy.required_liquidity_metric, factor="PARTICIPATION",
        requirement="REQUIRED", comparator="PRESENT",
    ))
    gates.append(_metric_gate(
        assessed_by_key, source_by_key,
        gate_id="PARTICIPATION_EVIDENCE", component_key="PARTICIPATION.1d",
        metric_id=policy.advisory_participation_metric, factor="PARTICIPATION",
        requirement="ADVISORY", comparator="PRESENT",
    ))
    if underlyer.upper() == "SPY":
        gates.append(StockBehaviorGate(
            gate_id="RELATIVE_STRENGTH_EVIDENCE", requirement="ADVISORY",
            verdict="NOT_APPLICABLE", factor="RELATIVE_STRENGTH",
            component_key="RELATIVE_STRENGTH.1d", metric_id="excess_return20",
            comparator="PRESENT", reason_codes=("BENCHMARK_SELF_COMPARISON",),
        ))
    else:
        gates.append(_metric_gate(
            assessed_by_key, source_by_key,
            gate_id="RELATIVE_STRENGTH_EVIDENCE",
            component_key="RELATIVE_STRENGTH.1d",
            metric_id=policy.advisory_relative_strength_metric,
            factor="RELATIVE_STRENGTH", requirement="ADVISORY", comparator="PRESENT",
        ))
    required = [gate for gate in gates if gate.requirement == "REQUIRED"]
    disposition: StockDisposition = (
        "BLOCKED" if any(gate.verdict == "FAIL" for gate in required)
        else "UNAVAILABLE" if any(gate.verdict == "UNAVAILABLE" for gate in required)
        else "ELIGIBLE_RESEARCH"
    )
    return StockBehaviorGateAssessment(
        **common,
        stock_snapshot_id=snapshot.snapshot_id,
        stock_payload_sha256=snapshot.sha256,
        stock_definition_sha256=snapshot.definition_sha256,
        stock_policy_sha256=snapshot.policy_sha256,
        stock_profile=snapshot.profile,
        behavior_data_status=assessment.data_status,
        behavior_alignment_state=assessment.alignment_state,
        applicable=True, gates=tuple(gates), disposition=disposition,
    )


def _metric_gate(
    assessed_by_key,
    source_by_key,
    *,
    gate_id: str,
    component_key: str,
    metric_id: str,
    factor: Factor,
    requirement: GateRequirement,
    comparator: GateComparator,
    threshold: float | None = None,
    mismatch_reason: str = "METRIC_PREDICATE_FAILED",
) -> StockBehaviorGate:
    assessed = assessed_by_key.get(component_key)
    source_component = source_by_key.get(component_key)
    metric = next((
        row for row in (assessed.metrics if assessed else ())
        if row.definition.metric_id == metric_id
    ), None)
    sources = source_component.sources if source_component else ()
    provenance = dict(
        factor=factor, component_key=component_key, metric_id=metric_id,
        comparator=comparator, threshold_float=threshold,
        source_evidence_ids=tuple(source.evidence_id for source in sources),
        source_payload_sha256s=tuple(source.payload_sha256 for source in sources),
        source_market_times=tuple(source.market_time for source in sources),
    )
    if metric is None or metric.value is None:
        reasons = tuple(assessed.reason_codes if assessed else ()) or (
            "REQUIRED_METRIC_UNAVAILABLE" if requirement == "REQUIRED"
            else "ADVISORY_METRIC_UNAVAILABLE",
        )
        return StockBehaviorGate(
            gate_id=gate_id, requirement=requirement, verdict="UNAVAILABLE",
            reason_codes=reasons, **provenance,
        )
    value = metric.value
    passes = (
        comparator == "PRESENT"
        or comparator == "GT_ZERO" and value > 0
        or comparator == "LT_ZERO" and value < 0
    )
    return StockBehaviorGate(
        gate_id=gate_id, requirement=requirement,
        verdict="PASS" if passes else "FAIL",
        formula_id=metric.definition.formula_id,
        actual_float=value,
        reason_codes=() if passes else (mismatch_reason,),
        **provenance,
    )