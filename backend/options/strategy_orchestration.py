from __future__ import annotations

import socket
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from options.analytics.analysis_engine import OptionAnalysisSnapshot
from options.analytics.chain_analysis import ChainHealth
from options.config import OptionRuntimeConfiguration
from options.domain import AssetType, OptionAnalysisRun, OptionContractSnapshot, WorkStage
from options.repositories.strategies import OptionStrategyRepository
from options.repositories.analysis import OptionAnalysisRepository
from options.repositories.snapshots import OptionSnapshotRepository
from options.repositories.trades import OptionTradeRepository
from options.repositories.work_items import OptionWorkItemRepository
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
        engine: OptionStrategyEngine | None = None,
    ) -> None:
        self.configuration = configuration
        self.context_repository = context_repository or OptionStrategyContextRepository()
        self.strategy_repository = strategy_repository or OptionStrategyRepository()
        self.work_repository = work_repository or OptionWorkItemRepository()
        self.analysis_repository = analysis_repository or OptionAnalysisRepository()
        self.snapshot_repository = snapshot_repository or OptionSnapshotRepository()
        self.trade_repository = trade_repository or OptionTradeRepository()
        self.engine = engine or OptionStrategyEngine(
            configuration.strategy_policy,
            configuration.strategy_policy_sha256,
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
        results: list[StrategyMatrixResult] = []
        for underlyer in requested:
            run = self.analysis_repository.get_latest(underlyer, read_context)
            if run is None:
                continue
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
                event_calendar_available=(
                    self.configuration.settings.event_calendar_provider is not None
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

def _read_context(now: datetime):
    from options.domain import DecisionContext

    return DecisionContext(now, now)