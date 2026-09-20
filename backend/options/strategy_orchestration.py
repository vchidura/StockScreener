from __future__ import annotations

import socket
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from options.analytics.analysis_engine import OptionAnalysisSnapshot
from options.analytics.chain_analysis import ChainHealth
from options.analytics.gamma_exposure import ScopedGammaProfile
from options.config import OptionRuntimeConfiguration
from options.domain import AssetType, OptionAnalysisRun, OptionContractSnapshot, WorkStage
from options.evidence_runtime import EvidenceRuntimeMonitor
from options.repositories.strategies import OptionStrategyRepository
from options.repositories.analysis import OptionAnalysisRepository
from options.repositories.gamma import OptionGammaProfileRepository
from options.repositories.snapshots import OptionSnapshotRepository
from options.repositories.trades import OptionTradeRepository
from options.repositories.work_items import OptionWorkItemRepository
from options.package_assessment_service import (
    OptionPackageAssessmentService,
    PackageAssessmentRunResult,
)
from options.stock_behavior_shadow import (
    OptionStockBehaviorShadowService,
    StockBehaviorShadowResult,
)
from options.strategies.context import OptionStrategyContextRepository
from options.strategies.engine import OptionStrategyEngine


@dataclass(frozen=True, slots=True)
class StrategyMatrixResult:
    matrix_id: UUID
    underlyer: str
    status: str
    candidate_count: int
    selected_count: int
    suppressed_count: int
    scenario_count: int
    error: str | None = None
    stock_behavior_shadow_status: str = "DISABLED"
    stock_behavior_assessment_count: int = 0
    stock_behavior_assessment_inserted: int = 0
    stock_behavior_assessment_error: str | None = None
    package_assessment_status: str = "DISABLED"
    package_assessment_count: int = 0
    package_assessment_inserted: int = 0
    package_assessment_error: str | None = None


class OptionStrategyPipeline:
    def __init__(
        self,
        configuration: OptionRuntimeConfiguration,
        *,
        context_repository: OptionStrategyContextRepository | None = None,
        strategy_repository: OptionStrategyRepository | None = None,
        work_repository: OptionWorkItemRepository | None = None,
        analysis_repository: OptionAnalysisRepository | None = None,
        snapshot_repository: OptionSnapshotRepository | None = None,
        trade_repository: OptionTradeRepository | None = None,
        gamma_repository: OptionGammaProfileRepository | None = None,
        engine: OptionStrategyEngine | None = None,
        stock_behavior_shadow_service: OptionStockBehaviorShadowService | None = None,
        package_assessment_service: OptionPackageAssessmentService | None = None,
    ) -> None:
        self.configuration = configuration
        self.evidence_runtime = EvidenceRuntimeMonitor()
        self.context_repository = context_repository or OptionStrategyContextRepository()
        self.strategy_repository = strategy_repository or OptionStrategyRepository()
        self.work_repository = work_repository or OptionWorkItemRepository()
        self.analysis_repository = analysis_repository or OptionAnalysisRepository()
        self.snapshot_repository = snapshot_repository or OptionSnapshotRepository()
        self.trade_repository = trade_repository or OptionTradeRepository()
        self.gamma_repository = gamma_repository or OptionGammaProfileRepository()
        self.stock_behavior_shadow_service = (
            stock_behavior_shadow_service
            or (
                OptionStockBehaviorShadowService(
                    launch=configuration.stock_behavior_shadow_launch,
                )
                if configuration.settings.stock_behavior_shadow_enabled
                else None
            )
        )
        self.package_assessment_service = (
            package_assessment_service
            or (
                OptionPackageAssessmentService(
                    valuation_policy_sha256=configuration.valuation_policy_sha256,
                )
                if configuration.settings.package_assessments_enabled
                else None
            )
        )
        self.engine = engine or OptionStrategyEngine(
            configuration.strategy_policy,
            configuration.strategy_policy_sha256,
            configuration.gamma_policy,
            configuration.gamma_policy_sha256,
            read_only=configuration.settings.start_read_only,
        )

    def process(
        self,
        analysis: OptionAnalysisSnapshot,
        snapshots: tuple[OptionContractSnapshot, ...],
        asset_type: AssetType,
    ) -> StrategyMatrixResult:
        return self.process_matrix(
            analysis.matrix_id,
            analysis.underlying.underlyer,
            analysis.context,
            analysis.chain_health,
            analysis.expirations,
            snapshots,
            asset_type,
            gamma_profiles=analysis.gamma_profiles,
        )

    def run_latest(
        self,
        underlyers: tuple[str, ...] | None = None,
    ) -> tuple[StrategyMatrixResult, ...]:
        requested = underlyers or self.configuration.settings.underlyers
        unknown = set(requested) - set(self.configuration.settings.underlyers)
        if unknown:
            raise ValueError(f"underlyers are not configured: {sorted(unknown)}")
        now = datetime.now(timezone.utc)
        read_context = _read_context(now)
        if set(requested) == set(self.configuration.settings.underlyers):
            runs = self.analysis_repository.list_latest_complete_cycle(
                tuple(requested),
                read_context,
                self.configuration.policy_sha256,
            )
        else:
            runs = tuple(
                run
                for underlyer in requested
                if (run := self.analysis_repository.get_latest(
                    underlyer, read_context
                )) is not None
            )
        results: list[StrategyMatrixResult] = []
        for run in runs:
            underlyer = run.underlyer
            asset_type = (
                AssetType.ETF
                if underlyer in self.configuration.settings.fixed_etf_underlyers
                else AssetType.STOCK
            )
            results.append(self.process_persisted(run, asset_type))
        return tuple(results)

    def process_persisted(
        self,
        run: OptionAnalysisRun,
        asset_type: AssetType,
    ) -> StrategyMatrixResult:
        if run.policy_sha256 != self.configuration.policy_sha256:
            return StrategyMatrixResult(
                run.matrix_id,
                run.underlyer,
                "POLICY_MISMATCH",
                0,
                0,
                0,
                0,
                "analysis market-data policy does not match the running policy",
            )
        snapshots = tuple(
            snapshot
            for snapshot in self.snapshot_repository.list_for_batch(
                run.batch_id,
                run.context,
            )
            if snapshot.model_mark is not None
        )
        expirations = self.analysis_repository.list_expirations(
            run.matrix_id,
            run.context,
        )
        health = ChainHealth(**json.loads(run.chain_health_json))
        return self.process_matrix(
            run.matrix_id,
            run.underlyer,
            run.context,
            health,
            expirations,
            snapshots,
            asset_type,
            gamma_profiles=self.gamma_repository.profiles_by_matrix(
                run.matrix_id,
                self.configuration.gamma_policy_sha256,
            ),
        )

    def process_matrix(
        self,
        matrix_id: UUID,
        underlyer: str,
        decision_context,
        chain_health,
        expirations,
        snapshots: tuple[OptionContractSnapshot, ...],
        asset_type: AssetType,
        gamma_profiles: tuple[ScopedGammaProfile, ...] = (),
    ) -> StrategyMatrixResult:
        business_key = f"strategy:{matrix_id}:{self.engine.strategy_version}"
        work_id = uuid5(NAMESPACE_URL, f"option-work:{business_key}")
        self.work_repository.enqueue(
            work_id,
            WorkStage.STRATEGY,
            str(matrix_id),
            business_key,
            self.configuration.policy.capacity.maximum_work_attempts,
            {
                "matrix_id": str(matrix_id),
                "underlyer": underlyer,
                "strategy_version": self.engine.strategy_version,
            },
        )
        lease_owner = f"strategy:{socket.gethostname()}"
        work_item = self.work_repository.claim_by_business_key(
            business_key,
            lease_owner,
            timedelta(minutes=10),
        )
        if work_item is None:
            existing = self.work_repository.get_by_business_key(business_key)
            if existing is None:
                status = "WORK_NOT_FOUND"
                error = "strategy work item could not be found after claim"
            elif existing.status.value == "TERMINAL_FAILED":
                status = "FAILED"
                error = existing.last_error
            elif existing.status.value == "COMPLETED":
                status = "ALREADY_COMPLETED"
                error = None
            elif existing.status.value == "CLAIMED":
                status = "ALREADY_CLAIMED"
                error = None
            else:
                status = "RETRY_WAIT"
                error = existing.last_error
            return StrategyMatrixResult(
                matrix_id,
                underlyer,
                status,
                0,
                0,
                0,
                0,
                error,
            )
        try:
            context = self.context_repository.build(
                matrix_id,
                underlyer,
                asset_type,
                decision_context,
                event_calendar_provider=(
                    self.configuration.settings.event_calendar_provider
                ),
                event_calendar_max_age_seconds=(
                    self.configuration.settings.event_calendar_max_age_seconds
                ),
                policy_version=self.configuration.strategy_policy.strategy_version,
                policy_sha256=self.configuration.strategy_policy_sha256,
                equity_context_enabled=(
                    self.configuration.settings.equity_context_enabled
                ),
            )
            trades = self.trade_repository.list_for_underlyer(
                underlyer,
                decision_context.market_time - timedelta(hours=8),
                decision_context,
            )
            result = self.engine.scan(
                matrix_id,
                snapshots,
                chain_health,
                expirations,
                context,
                trades,
                gamma_profiles,
            )
            if self.configuration.settings.start_read_only and any(
                candidate.execution_eligibility is not None
                for candidate in result.candidates
            ):
                raise RuntimeError(
                    "read-only mode cannot persist execution-eligible candidates"
                )
            self.strategy_repository.persist(context, result)
            if not self.work_repository.complete(work_item.work_id, lease_owner):
                raise RuntimeError("strategy work lease expired before acknowledgement")
            shadow_result, shadow_error = self._persist_stock_behavior_shadow(
                context, result.candidates,
            )
            package_result, package_error = self._persist_package_assessments(
                result.candidates,
            )
            selected = sum(candidate.status.value == "SELECTED" for candidate in result.candidates)
            suppressed = sum(candidate.status.value != "SELECTED" for candidate in result.candidates)
            return StrategyMatrixResult(
                matrix_id,
                underlyer,
                "COMPLETE",
                len(result.candidates),
                selected,
                suppressed,
                len(result.scenarios),
                stock_behavior_shadow_status=(
                    "FAILED" if shadow_error else
                    shadow_result.status if shadow_result is not None else "DISABLED"
                ),
                stock_behavior_assessment_count=(
                    shadow_result.attempted if shadow_result is not None else 0
                ),
                stock_behavior_assessment_inserted=(
                    shadow_result.inserted if shadow_result is not None else 0
                ),
                stock_behavior_assessment_error=shadow_error,
                package_assessment_status=(
                    "FAILED" if package_error else
                    package_result.status if package_result is not None else "DISABLED"
                ),
                package_assessment_count=(
                    package_result.attempted if package_result is not None else 0
                ),
                package_assessment_inserted=(
                    package_result.inserted if package_result is not None else 0
                ),
                package_assessment_error=package_error,
            )
        except Exception as exc:
            self.work_repository.retry(
                work_item.work_id,
                lease_owner,
                str(exc),
                timedelta(minutes=5),
            )
            return StrategyMatrixResult(
                matrix_id,
                underlyer,
                "RETRY",
                0,
                0,
                0,
                0,
                str(exc),
            )

    def _persist_stock_behavior_shadow(
        self, context, candidates,
    ) -> tuple[StockBehaviorShadowResult | None, str | None]:
        if self.stock_behavior_shadow_service is None:
            return None, None
        try:
            return (
                self._measure_evidence("STOCK_BEHAVIOR", lambda: self.stock_behavior_shadow_service.assess_and_persist(context, candidates)),
                None,
            )
        except Exception as exc:
            return None, f"{type(exc).__name__}: {exc}"

    def _persist_package_assessments(
        self, candidates,
    ) -> tuple[PackageAssessmentRunResult | None, str | None]:
        service = getattr(self, "package_assessment_service", None)
        if service is None:
            return None, None
        try:
            return self._measure_evidence("PACKAGE", lambda: service.assess_and_persist(candidates)), None
        except Exception as exc:
            return None, f"{type(exc).__name__}: {exc}"

    def _measure_evidence(self, stage, callback):
        if not hasattr(self, "evidence_runtime"):
            self.evidence_runtime = EvidenceRuntimeMonitor()
        return self.evidence_runtime.run(stage, callback)

def _read_context(now: datetime):
    from options.domain import DecisionContext

    return DecisionContext(now, now)