"""Unwired hourly acceptance challenger over retained equity-owned evidence."""
from __future__ import annotations

from datetime import datetime
from types import MappingProxyType
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, model_validator

from equity.behavior import Contract, DEFINITION_V1_SHA256, Name, Sha256, StockBehaviorSnapshot
from equity.behavior_setup import (
    DirectSetupSourcePolicy, DirectStockSetupEvidence, PublishedSetupSourcePolicy,
    PublishedStockSetupEvidence, StockSetupEvidence,
)
from options.stock_behavior_gates import DetectorTarget, resolve_stock_behavior_gate_policy
from options.strategies.domain import StructureType
from research.stock_idea_engine import entry_gate

_DIRECTIONAL_TARGETS = resolve_stock_behavior_gate_policy(
    "option_stock_behavior_gate_v1", "f3fddf6aadb5dc9688fbf3dcf4250113a64fb48b0a3b2d35b55400232fe7a8dd",
).targets


class HourlyAcceptancePolicy(Contract):
    version: Literal["option_hourly_acceptance_gate_v1"] = "option_hourly_acceptance_gate_v1"
    behavior_definition_sha256: Literal[DEFINITION_V1_SHA256] = DEFINITION_V1_SHA256
    setup_source_policy_sha256: Sha256
    targets: tuple[DetectorTarget, ...] = _DIRECTIONAL_TARGETS
    setup_interval: Literal["1h"] = "1h"
    setup_model: Literal["acceptance"] = "acceptance"
    minimum_room_risk: Literal[1.0] = 1.0
    maximum_chase_activation_atr: Literal[1.0] = 1.0
    threshold_basis: Literal["STOCK_IDEAS_V1_ENTRY_BASELINE_NOT_EMPIRICALLY_ACCEPTED"] = (
        "STOCK_IDEAS_V1_ENTRY_BASELINE_NOT_EMPIRICALLY_ACCEPTED"
    )
    admission_enabled: Literal[False] = False

    @model_validator(mode="after")
    def validate_targets(self):
        if self.targets != _DIRECTIONAL_TARGETS:
            raise ValueError("hourly acceptance v1 requires its frozen directional structures")
        return self


class PublishedHourlyAcceptancePolicy(HourlyAcceptancePolicy):
    version: Literal["option_hourly_acceptance_gate_v2"] = "option_hourly_acceptance_gate_v2"
    setup_schema_version: Literal["stock_setup_publication_evidence_v1"] = "stock_setup_publication_evidence_v1"
    setup_detector_version: Literal["range_breakout_acceptance_intraday_v2"] = "range_breakout_acceptance_intraday_v2"


class DirectHourlyAcceptancePolicy(PublishedHourlyAcceptancePolicy):
    version: Literal["option_hourly_acceptance_direct_v1"] = "option_hourly_acceptance_direct_v1"
    setup_schema_version: Literal["stock_setup_direct_evidence_v1"] = "stock_setup_direct_evidence_v1"


SETUP_POLICY_CONTRACTS = MappingProxyType({
    "option_hourly_acceptance_gate_v1": HourlyAcceptancePolicy,
    "option_hourly_acceptance_gate_v2": PublishedHourlyAcceptancePolicy,
    "option_hourly_acceptance_direct_v1": DirectHourlyAcceptancePolicy,
})


def load_setup_policy(payload: dict, expected_sha256: str) -> HourlyAcceptancePolicy:
    contract = SETUP_POLICY_CONTRACTS.get(payload.get("version"))
    if contract is None:
        raise ValueError("unsupported setup policy version")
    policy = contract.model_validate(payload)
    if policy.sha256 != expected_sha256:
        raise ValueError("setup policy hash mismatch")
    return policy


class SetupGateCheck(Contract):
    gate_id: Name
    verdict: Literal["PASS", "FAIL", "UNAVAILABLE", "NOT_APPLICABLE"]
    value: float | None = None
    comparator: Literal["PRESENT", "GT_ZERO", "LT_ZERO", "GTE", "LTE", "EQUALS"]
    threshold: float | None = None
    unit: Name | None = None
    reason: Name | None = None


class SetupGateAssessment(Contract):
    schema_version: Literal["option_stock_setup_assessment_v1"] = "option_stock_setup_assessment_v1"
    candidate_id: UUID
    candidate_identity_sha256: Sha256
    matrix_id: UUID
    decision_at: AwareDatetime
    stock_market_cutoff: AwareDatetime
    detector_policy_version: Name
    detector_policy_sha256: Sha256
    stock_snapshot_id: UUID | None
    stock_payload_sha256: Sha256 | None
    setup_payload_sha256: Sha256 | None
    setup_source_policy_sha256: Sha256 | None
    episode_id: Sha256 | None
    checks: tuple[SetupGateCheck, ...]
    disposition: Literal["ELIGIBLE_RESEARCH", "BLOCKED", "UNAVAILABLE", "NOT_APPLICABLE"]
    assessment_only: Literal[True] = True
    publication_permission: Literal[False] = False
    execution_permission: Literal[False] = False


def evaluate_hourly_acceptance(
    snapshot: StockBehaviorSnapshot | None,
    setup: StockSetupEvidence | PublishedStockSetupEvidence | DirectStockSetupEvidence | None,
    *,
    policy: HourlyAcceptancePolicy,
    candidate_id: UUID,
    candidate_identity_sha256: str,
    matrix_id: UUID,
    security_id: UUID,
    underlyer: str,
    strategy_name: str,
    structure_type: StructureType,
    directional_thesis: str,
    market_cutoff: datetime,
    decision_at: datetime,
    trusted_source_policy: PublishedSetupSourcePolicy | DirectSetupSourcePolicy | None = None,
) -> SetupGateAssessment:
    policy = load_setup_policy(policy.model_dump(mode="json"), policy.sha256)
    if market_cutoff.tzinfo is None or decision_at.tzinfo is None or market_cutoff > decision_at:
        raise ValueError("challenger cutoffs must be causal and timezone-aware")
    checks = []
    target = next((row for row in policy.targets
                   if row.strategy_name == strategy_name and row.structure_type == structure_type.value), None)
    if target is None:
        checks.append(SetupGateCheck(gate_id="DETECTOR_SCOPE", verdict="NOT_APPLICABLE",
                                    comparator="EQUALS", reason="DETECTOR_PROFILE_NOT_REGISTERED"))
    else:
        thesis_matches = directional_thesis == target.directional_thesis
        checks.append(SetupGateCheck(gate_id="THESIS", verdict="PASS" if thesis_matches else "FAIL",
                                    comparator="EQUALS", reason=None if thesis_matches else "THESIS_MISMATCH"))
        expected_direction = 1 if target.directional_thesis == "BULLISH" else -1
        if snapshot is None or snapshot.security_id != security_id or snapshot.ticker != underlyer:
            checks.append(SetupGateCheck(gate_id="STOCK_IDENTITY", verdict="UNAVAILABLE",
                                        comparator="EQUALS", reason="STOCK_IDENTITY_UNAVAILABLE"))
        elif snapshot.market_time > market_cutoff:
            checks.append(SetupGateCheck(gate_id="STOCK_CUTOFF", verdict="UNAVAILABLE",
                                        comparator="EQUALS", reason="STOCK_AFTER_MARKET_CUTOFF"))
        else:
            assessed = snapshot.assess_at(decision_at)
            for component_key, metric_id in (("TREND.1d", "ema50_slope10_atr"),
                                              ("PARTICIPATION.1d", "median_dollar_volume20")):
                component = next((row for row in assessed.components if row.key == component_key), None)
                metric = next((row for row in component.metrics if row.definition.metric_id == metric_id), None) if component else None
                value = metric.value if metric is not None and component.status == "READY" else None
                directional = metric_id == "ema50_slope10_atr"
                passes = value is not None and (expected_direction * value > 0 if directional else value > 0)
                checks.append(SetupGateCheck(
                    gate_id=metric_id, verdict="UNAVAILABLE" if value is None else "PASS" if passes else "FAIL",
                    value=value, comparator="LT_ZERO" if directional and expected_direction < 0 else "GT_ZERO",
                    threshold=0., unit="ATR_PER_BAR" if directional else "USD_PER_SESSION",
                    reason="METRIC_UNAVAILABLE" if value is None else None if passes else "METRIC_PREDICATE_FAILED",
                ))
        stock = setup.candidate if setup else None
        publication_policy = isinstance(policy, PublishedHourlyAcceptancePolicy)
        source_contract_matches = (
            isinstance(setup, PublishedStockSetupEvidence)
            and setup.schema_version == policy.setup_schema_version
            and trusted_source_policy is not None
            and trusted_source_policy.sha256 == policy.setup_source_policy_sha256
            and setup.source_policy == trusted_source_policy
            if publication_policy else isinstance(setup, StockSetupEvidence)
        )
        if setup is not None and not source_contract_matches:
            checks.append(SetupGateCheck(gate_id="SETUP_SOURCE_CONTRACT", verdict="UNAVAILABLE",
                                        comparator="EQUALS", reason="TRUSTED_SETUP_SOURCE_POLICY_REQUIRED"))
        elif setup is None or not setup.available_at(decision_at, market_cutoff):
            checks.append(SetupGateCheck(gate_id="SETUP_RECEIPT", verdict="UNAVAILABLE",
                                        comparator="PRESENT", reason="CAUSAL_SETUP_UNAVAILABLE"))
        elif (setup.source.security_id != security_id or stock.ticker != underlyer
              or setup.source.policy_sha256 != policy.setup_source_policy_sha256):
            checks.append(SetupGateCheck(gate_id="SETUP_IDENTITY_POLICY", verdict="UNAVAILABLE",
                                        comparator="EQUALS", reason="SETUP_IDENTITY_POLICY_MISMATCH"))
        elif stock.direction != expected_direction:
            checks.append(SetupGateCheck(gate_id="SETUP_DIRECTION", verdict="FAIL",
                                        comparator="EQUALS", reason="SETUP_DIRECTION_MISMATCH"))
        else:
            risk = stock.direction * (stock.price - stock.stop)
            room = stock.direction * (stock.target - stock.price)
            chase = stock.direction * (stock.price - stock.reference) / stock.activation_atr
            reason = entry_gate(stock, stock.price)
            checks.extend((
                SetupGateCheck(gate_id="CONFIRMED_BRACKET", verdict="PASS" if reason is None else "FAIL",
                               comparator="EQUALS", reason=reason),
                SetupGateCheck(gate_id="TARGET_ROOM", verdict="PASS" if room > 0 else "FAIL",
                               value=room / stock.activation_atr, comparator="GT_ZERO", threshold=0.,
                               unit="ACTIVATION_ATR", reason=None if room > 0 else "TARGET_ROOM_NONPOSITIVE"),
                SetupGateCheck(gate_id="ROOM_RISK", verdict="PASS" if risk > 0 and room >= risk else "FAIL",
                               value=room / risk if risk > 0 else None, comparator="GTE", threshold=policy.minimum_room_risk,
                               unit="RATIO", reason=None if risk > 0 and room >= risk else "INSUFFICIENT_TARGET_ROOM"),
                SetupGateCheck(gate_id="CHASE", verdict="PASS" if 0 <= chase <= policy.maximum_chase_activation_atr else "FAIL",
                               value=chase, comparator="LTE", threshold=policy.maximum_chase_activation_atr,
                               unit="ACTIVATION_ATR", reason=None if 0 <= chase <= 1 else "CHASE_OR_BOUNDARY_FAILED"),
            ))
    disposition = (
        "NOT_APPLICABLE" if target is None else
        "UNAVAILABLE" if any(row.verdict == "UNAVAILABLE" for row in checks) else
        "BLOCKED" if any(row.verdict == "FAIL" for row in checks) else "ELIGIBLE_RESEARCH"
    )
    return SetupGateAssessment(
        candidate_id=candidate_id, candidate_identity_sha256=candidate_identity_sha256,
        matrix_id=matrix_id, decision_at=decision_at, stock_market_cutoff=market_cutoff,
        detector_policy_version=policy.version, detector_policy_sha256=policy.sha256,
        stock_snapshot_id=snapshot.snapshot_id if snapshot else None,
        stock_payload_sha256=snapshot.sha256 if snapshot else None,
        setup_payload_sha256=setup.sha256 if setup else None,
        setup_source_policy_sha256=setup.source.policy_sha256 if setup else None,
        episode_id=setup.candidate.episode_id if setup else None,
        checks=tuple(checks), disposition=disposition,
    )