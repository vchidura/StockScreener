"""Disabled-by-default persisted shadow assessment service for option candidates."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from equity.behavior import DEFINITION_SHA256, OPTIONS_SWING_PROFILE
from equity.domain import DecisionWatermark
from equity.repositories import EquityEvidenceRepository, EquityReferenceRepository
from options.strategies.domain import CandidateStatus, OptionCandidate, StrategyContextSnapshot

from .repositories.stock_behavior_assessments import (
    OptionStockBehaviorAssessmentRepository,
)
from .stock_behavior_gates import (
    STOCK_BEHAVIOR_GATE_POLICY,
    StockBehaviorGateAssessment,
    evaluate_option_stock_behavior,
)
from .stock_behavior_shadow_launch import OptionStockBehaviorShadowLaunch


@dataclass(frozen=True, slots=True)
class StockBehaviorShadowResult:
    status: str
    attempted: int
    inserted: int
    existing: int
    eligible: int
    blocked: int
    unavailable: int


class OptionStockBehaviorShadowService:
    def __init__(
        self,
        *,
        reference_repository: EquityReferenceRepository | None = None,
        evidence_repository: EquityEvidenceRepository | None = None,
        assessment_repository: OptionStockBehaviorAssessmentRepository | None = None,
        launch: OptionStockBehaviorShadowLaunch,
    ) -> None:
        self.reference_repository = reference_repository or EquityReferenceRepository()
        self.evidence_repository = evidence_repository or EquityEvidenceRepository()
        self.assessment_repository = (
            assessment_repository or OptionStockBehaviorAssessmentRepository()
        )
        self.launch = launch

    def assess_and_persist(
        self,
        context: StrategyContextSnapshot,
        candidates: Sequence[OptionCandidate],
    ) -> StockBehaviorShadowResult:
        targets = {
            (row.strategy_name, row.structure_type)
            for row in STOCK_BEHAVIOR_GATE_POLICY.targets
        }
        selected = tuple(
            candidate for candidate in candidates
            if candidate.status is CandidateStatus.SELECTED
            and (candidate.strategy_name, candidate.structure_type.value) in targets
        )
        if len(selected) > self.launch.maximum_candidates_per_matrix:
            raise ValueError("stock behavior shadow candidate bound exceeded")
        if not selected:
            return StockBehaviorShadowResult("NO_APPLICABLE_CANDIDATES", 0, 0, 0, 0, 0, 0)
        if any(
            candidate.matrix_id != context.matrix_id
            or candidate.underlyer != context.underlyer
            or candidate.observed_time != context.observed_time
            for candidate in selected
        ):
            raise ValueError("stock behavior shadow candidate/context identity mismatch")
        lineage = self.assessment_repository.get_matrix_lineage(context.matrix_id)
        if lineage is None:
            raise ValueError("stock behavior shadow matrix lineage is unavailable")
        if (
            lineage.underlying != context.underlyer
            or lineage.market_time != context.market_data_time
            or lineage.observed_time != context.observed_time
        ):
            raise ValueError("stock behavior shadow matrix lineage disagrees with context")
        if context.underlyer not in self.launch.underlyers:
            return StockBehaviorShadowResult("UNDERLYER_NOT_APPROVED", 0, 0, 0, 0, 0, 0)
        if not self.launch.accepts(lineage.scheduled_cycle):
            return StockBehaviorShadowResult("OUTSIDE_LAUNCH_WINDOW", 0, 0, 0, 0, 0, 0)
        usage_since, usage_until = self.launch.usage_bounds(context.observed_time)
        usage = self.assessment_repository.launch_usage(
            usage_since, usage_until, self.launch.sha256,
        )
        if usage.assessment_count >= self.launch.maximum_assessments:
            return StockBehaviorShadowResult("ASSESSMENT_LIMIT_REACHED", 0, 0, 0, 0, 0, 0)
        if usage.payload_bytes >= self.launch.maximum_payload_bytes:
            return StockBehaviorShadowResult("PAYLOAD_LIMIT_REACHED", 0, 0, 0, 0, 0, 0)
        if usage.assessment_count >= self.launch.minimum_assessments_before_rate_stops:
            unavailable_fraction = usage.unavailable_count / usage.assessment_count
            if unavailable_fraction > self.launch.maximum_unavailable_fraction:
                return StockBehaviorShadowResult("UNAVAILABLE_RATE_STOP", 0, 0, 0, 0, 0, 0)
            if (
                usage.p95_decision_lag_seconds is not None
                and usage.p95_decision_lag_seconds
                > self.launch.maximum_p95_decision_lag_seconds
            ):
                return StockBehaviorShadowResult("DECISION_LAG_STOP", 0, 0, 0, 0, 0, 0)
        watermark = DecisionWatermark(lineage.scheduled_cycle, context.observed_time)
        security = self.reference_repository.get_security_as_of(context.underlyer, watermark)
        snapshot = (
            self.evidence_repository.get_behavior_as_of(
                security.security_id, watermark,
                profile=OPTIONS_SWING_PROFILE.name,
                definition_sha256=DEFINITION_SHA256,
                policy_sha256=OPTIONS_SWING_PROFILE.sha256,
            )
            if security is not None else None
        )
        unavailable_reason = (
            "STOCK_SECURITY_IDENTITY_UNAVAILABLE" if security is None
            else "STOCK_BEHAVIOR_SNAPSHOT_UNAVAILABLE"
        )
        assessments = tuple(
            evaluate_option_stock_behavior(
                snapshot,
                candidate_id=candidate.candidate_id,
                candidate_identity_sha256=candidate.identity_sha256,
                matrix_id=candidate.matrix_id,
                underlyer=candidate.underlyer,
                strategy_name=candidate.strategy_name,
                structure_type=candidate.structure_type,
                directional_thesis=candidate.primary_evidence.get("directional_thesis"),
                option_strategy_version=candidate.strategy_version,
                option_strategy_policy_sha256=candidate.policy_sha256,
                option_configuration_sha256=lineage.configuration_sha256,
                option_market_policy_sha256=lineage.market_policy_sha256,
                option_analysis_policy_sha256=lineage.analysis_policy_sha256,
                option_market_time=candidate.market_data_time,
                option_observed_at=candidate.observed_time,
                stock_market_cutoff=lineage.scheduled_cycle,
                launch_id=self.launch.launch_id,
                launch_manifest_sha256=self.launch.sha256,
                decision_at=candidate.observed_time,
                entry_deadline=getattr(candidate, "valid_until", None),
                unavailable_reason=unavailable_reason,
            )
            for candidate in selected
        )
        payload_bytes = sum(len(row.canonical_json().encode("utf-8")) for row in assessments)
        if usage.assessment_count + len(assessments) > self.launch.maximum_assessments:
            return StockBehaviorShadowResult("ASSESSMENT_LIMIT_REACHED", 0, 0, 0, 0, 0, 0)
        if usage.payload_bytes + payload_bytes > self.launch.maximum_payload_bytes:
            return StockBehaviorShadowResult("PAYLOAD_LIMIT_REACHED", 0, 0, 0, 0, 0, 0)
        persisted = self.assessment_repository.persist(assessments)
        return StockBehaviorShadowResult(
            status="RECORDED", attempted=len(assessments), inserted=persisted.inserted,
            existing=persisted.existing,
            eligible=sum(row.disposition == "ELIGIBLE_RESEARCH" for row in assessments),
            blocked=sum(row.disposition == "BLOCKED" for row in assessments),
            unavailable=sum(row.disposition == "UNAVAILABLE" for row in assessments),
        )