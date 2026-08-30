from __future__ import annotations

import json
import socket
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import NAMESPACE_URL, UUID, uuid5

from options.analytics.analysis_engine import OptionAnalysisEngine
from options.analytics.marks import UnderlyingMinuteBar
from options.calendar import OptionExchangeCalendar
from options.config import OptionRuntimeConfiguration
from options.data.normalizer import (
    DeveloperNormalizationInput,
    DeveloperOptionNormalizer,
    RawDeveloperOptionObservation,
    parse_polygon_snapshot,
)
from options.data.polygon_developer import PolygonDeveloperEngine
from options.domain import (
    AnalysisStatus,
    AssetType,
    DataQualityFlag,
    DecisionContext,
    OptionAnalysisRun,
    OptionUniverseMember,
    OptionUniverseMode,
    UniverseRunStatus,
    reference_drift_failed,
)
from options.repositories.analysis import OptionAnalysisRepository
from options.repositories.catalog import OptionContractCatalogRepository
from options.repositories.ingestion import OptionIngestionRepository
from options.repositories.snapshots import OptionSnapshotRepository
from options.repositories.universe import OptionUniverseRepository
from options.repositories.work_items import OptionWorkItemRepository
from options.strategy_orchestration import OptionStrategyPipeline


@dataclass(frozen=True, slots=True)
class UnderlyingCycleResult:
    underlyer: str
    asset_type: AssetType
    batch_id: UUID | None
    matrix_id: UUID | None
    status: str
    received_count: int
    retained_count: int
    iv_convergence_fraction: float | None
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ManualCycleResult:
    universe_run_id: UUID
    as_of_session: date
    started_at: datetime
    completed_at: datetime
    results: tuple[UnderlyingCycleResult, ...]


class ManualOptionPipeline:
    def __init__(
        self,
        configuration: OptionRuntimeConfiguration,
        engine: PolygonDeveloperEngine,
        *,
        calendar: OptionExchangeCalendar | None = None,
        catalog_repository: OptionContractCatalogRepository | None = None,
        universe_repository: OptionUniverseRepository | None = None,
        ingestion_repository: OptionIngestionRepository | None = None,
        snapshot_repository: OptionSnapshotRepository | None = None,
        analysis_repository: OptionAnalysisRepository | None = None,
        work_repository: OptionWorkItemRepository | None = None,
        normalizer: DeveloperOptionNormalizer | None = None,
        analysis_engine: OptionAnalysisEngine | None = None,
        strategy_pipeline: OptionStrategyPipeline | None = None,
        clock=None,
    ) -> None:
        self.configuration = configuration
        self.engine = engine
        self.calendar = calendar or OptionExchangeCalendar()
        self.catalog_repository = catalog_repository or OptionContractCatalogRepository()
        self.universe_repository = universe_repository or OptionUniverseRepository()
        self.ingestion_repository = ingestion_repository or OptionIngestionRepository()
        self.snapshot_repository = snapshot_repository or OptionSnapshotRepository()
        self.analysis_repository = analysis_repository or OptionAnalysisRepository()
        self.work_repository = work_repository or OptionWorkItemRepository()
        self.normalizer = normalizer or DeveloperOptionNormalizer(configuration.policy)
        self.analysis_engine = analysis_engine or OptionAnalysisEngine(configuration.policy)
        self.strategy_pipeline = strategy_pipeline
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def run_once(
        self,
        underlyers: tuple[str, ...] | None = None,
        *,
        as_of: datetime | None = None,
    ) -> ManualCycleResult:
        started_at = _as_utc(self.clock(), "clock")
        self.work_repository.recover_expired_claims()
        requested = underlyers or self.configuration.settings.underlyers
        unknown = set(requested) - set(self.configuration.settings.underlyers)
        if unknown:
            raise ValueError(f"underlyers are not configured: {sorted(unknown)}")
        as_of = _as_utc(as_of, "as_of") if as_of is not None else started_at
        as_of_session = self.calendar.latest_completed_session(as_of)
        cycle_time = self.calendar.expiration_cutoff(as_of_session) + timedelta(minutes=15)
        universe_run_id = uuid5(
            NAMESPACE_URL,
            (
                f"option-universe:{as_of_session}:"
                f"{self.configuration.configuration_sha256}:{started_at.isoformat()}"
            ),
        )
        universe_members = self._persist_fixed_universe(
            universe_run_id,
            as_of_session,
            requested,
            started_at,
        )
        results = tuple(
            self._run_underlying(
                underlyer,
                universe_members[underlyer],
                as_of_session,
                cycle_time,
            )
            for underlyer in requested
        )
        completed_at = _as_utc(self.clock(), "clock")
        completed_count = sum(result.status in {"COMPLETE", "DEGRADED"} for result in results)
        universe_status = (
            UniverseRunStatus.COMPLETE
            if completed_count == len(results)
            else UniverseRunStatus.DEGRADED
        )
        self.universe_repository.complete_run(
            universe_run_id,
            universe_status,
            completed_count / len(results) if results else 0.0,
            completed_at,
        )
        return ManualCycleResult(
            universe_run_id=universe_run_id,
            as_of_session=as_of_session,
            started_at=started_at,
            completed_at=completed_at,
            results=results,
        )

    def _persist_fixed_universe(
        self,
        run_id: UUID,
        as_of_session: date,
        underlyers: tuple[str, ...],
        observed_at: datetime,
    ) -> dict[str, AssetType]:
        configuration_json = json.dumps(
            {
                "mode": "fixed",
                "underlyers": underlyers,
                "configuration_sha256": self.configuration.configuration_sha256,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        self.universe_repository.create_run(
            run_id,
            OptionUniverseMode.FIXED,
            as_of_session,
            as_of_session,
            configuration_json,
            self.configuration.configuration_sha256,
            observed_at,
            observed_at,
        )
        asset_types = {
            underlyer: (
                AssetType.ETF
                if underlyer in self.configuration.settings.fixed_etf_underlyers
                else AssetType.STOCK
            )
            for underlyer in underlyers
        }
        members = tuple(
            OptionUniverseMember(
                effective_from=as_of_session,
                ticker=underlyer,
                asset_type=asset_types[underlyer],
                source_run_id=run_id,
                member_rank=index,
                score=None,
                activated_at=observed_at,
                deactivated_at=None,
                first_observed_at=observed_at,
            )
            for index, underlyer in enumerate(underlyers, start=1)
        )
        self.universe_repository.activate_members(members)
        return asset_types

    def _run_underlying(
        self,
        underlyer: str,
        asset_type: AssetType,
        as_of_session: date,
        cycle_time: datetime,
    ) -> UnderlyingCycleResult:
        batch_id = None
        work_item = None
        lease_owner = f"manual:{socket.gethostname()}"
        try:
            expiration_through = as_of_session + timedelta(
                days=self.configuration.policy.contract_filter.maximum_dte
            )
            spot = self.engine.get_spot_price(underlyer, cycle_time)
            corridor = self.configuration.policy.contract_filter.strike_corridor_fraction
            strike_min = spot.price * (Decimal("1") - corridor)
            strike_max = spot.price * (Decimal("1") + corridor)
            references = self.engine.list_option_references(
                underlyer,
                as_of_session,
                expiration_through,
                asset_type,
                strike_min,
                strike_max,
            )
            self.catalog_repository.upsert_references(references)
            batch = self.engine.get_option_chain(
                underlyer,
                cycle_time,
                expiration_through,
                strike_min,
                strike_max,
            )
            batch_id = batch.batch_id
            business_key = f"normalize:{batch_id}"
            work_item = self.work_repository.claim_by_business_key(
                business_key,
                lease_owner,
                timedelta(minutes=10),
            )
            if work_item is None:
                raise RuntimeError("normalization work item could not be claimed")
            raw_rows = self._raw_observations(batch)
            catalog_observed_time = max(
                [
                    row.revised_observed_at or row.first_observed_at
                    for row in raw_rows
                ]
                + [
                    reference.revised_observed_at or reference.first_observed_at
                    for reference in references
                ]
                + [_as_utc(self.clock(), "clock")]
            )
            catalog_context = DecisionContext(cycle_time, catalog_observed_time)
            catalog = self.catalog_repository.get_by_tickers(
                (row.contract_ticker for row in raw_rows),
                catalog_context,
            )
            unknown_count = sum(row.contract_ticker not in catalog for row in raw_rows)
            drift_failed = reference_drift_failed(
                unknown_count,
                len(raw_rows),
                maximum_unknown_references=(
                    self.configuration.policy.contract_filter.maximum_unknown_references
                ),
                maximum_unknown_reference_fraction=(
                    self.configuration.policy.contract_filter.maximum_unknown_reference_fraction
                ),
            )
            mark_times = [row.option_mark_time for row in raw_rows if row.option_mark_time]
            bars: tuple[UnderlyingMinuteBar, ...] = ()
            if mark_times:
                bars = self.engine.get_underlying_minute_bars(
                    underlyer,
                    min(mark_times) - timedelta(minutes=1),
                    max(mark_times),
                )
            observed_time = max(
                catalog_observed_time,
                _as_utc(self.clock(), "clock"),
            )
            context = DecisionContext(cycle_time, observed_time)
            normalization_inputs = tuple(
                DeveloperNormalizationInput(
                    raw=row,
                    catalog=catalog[row.contract_ticker],
                    underlying_bars=bars,
                    expiration_cutoff=self.calendar.expiration_cutoff(
                        catalog[row.contract_ticker].expiration_date
                    ),
                    risk_free_rate=float(self.configuration.settings.risk_free_rate),
                    dividend_yield=float(
                        self.configuration.settings.default_dividend_yield
                    ),
                    input_quality_flags=(
                        DataQualityFlag.DIVIDEND_YIELD_DEFAULTED,
                    ),
                    normalized_observed_at=observed_time,
                )
                for row in raw_rows
                if row.contract_ticker in catalog
            )
            normalized = self.normalizer.normalize(batch_id, normalization_inputs)
            self.snapshot_repository.persist(
                normalized.snapshots,
                asset_type,
                self.configuration.policy.policy_version,
                self.configuration.policy_sha256,
            )
            rejected_counts = dict(normalized.rejected_counts)
            if unknown_count:
                rejected_counts["UNKNOWN_REFERENCE"] = unknown_count
            if not normalized.matrix_snapshots:
                raise RuntimeError("normalization produced no retained contracts")
            matrix_market_time = max(
                snapshot.market_data_time
                for snapshot in normalized.matrix_snapshots
            )
            context = DecisionContext(matrix_market_time, observed_time)
            self.ingestion_repository.record_normalization(
                batch_id,
                catalog_row_count=len(catalog),
                retained_row_count=normalized.retained_count,
                rejected_counts=rejected_counts,
                unknown_reference_count=unknown_count,
                market_data_time=matrix_market_time,
                first_observed_at=observed_time,
            )
            matrix_id = uuid5(
                NAMESPACE_URL,
                f"option-matrix:{batch_id}:{self.configuration.policy_sha256}",
            )
            analysis = self.analysis_engine.analyze(
                matrix_id,
                normalized.matrix_snapshots,
                context,
                received_count=normalized.received_count,
                catalog_matched_count=len(catalog),
                unknown_reference_count=unknown_count,
                reference_drift_failed=drift_failed,
                batch_complete=True,
            )
            started_at = _as_utc(self.clock(), "clock")
            running = self._analysis_run(
                analysis,
                batch_id,
                AnalysisStatus.RUNNING,
                started_at,
                None,
            )
            self.analysis_repository.start(running)
            self.analysis_repository.persist_expirations(analysis.expirations)
            terminal_status = self._analysis_status(analysis.chain_health.status)
            completed_at = _as_utc(self.clock(), "clock")
            self.analysis_repository.finish(
                self._analysis_run(
                    analysis,
                    batch_id,
                    terminal_status,
                    started_at,
                    completed_at,
                )
            )
            if self.strategy_pipeline is not None:
                strategy_result = self.strategy_pipeline.process(
                    analysis,
                    normalized.matrix_snapshots,
                    asset_type,
                )
                if strategy_result.status == "RETRY":
                    raise RuntimeError(
                        f"strategy pipeline failed: {strategy_result.error}"
                    )
            if not self.work_repository.complete(work_item.work_id, lease_owner):
                raise RuntimeError("normalization work lease expired before acknowledgement")
            return UnderlyingCycleResult(
                underlyer=underlyer,
                asset_type=asset_type,
                batch_id=batch_id,
                matrix_id=matrix_id,
                status=analysis.chain_health.status,
                received_count=normalized.received_count,
                retained_count=normalized.retained_count,
                iv_convergence_fraction=normalized.iv_convergence_fraction,
                reasons=analysis.chain_health.reasons,
            )
        except Exception as exc:
            if work_item is not None:
                self.work_repository.retry(
                    work_item.work_id,
                    lease_owner,
                    str(exc),
                    timedelta(minutes=5),
                )
            return UnderlyingCycleResult(
                underlyer=underlyer,
                asset_type=asset_type,
                batch_id=batch_id,
                matrix_id=None,
                status="FAILED",
                received_count=0,
                retained_count=0,
                iv_convergence_fraction=None,
                reasons=(type(exc).__name__,),
            )

    @staticmethod
    def _raw_observations(batch) -> tuple[RawDeveloperOptionObservation, ...]:
        rows = []
        for page in batch.pages:
            payload = json.loads(page.response_bytes)
            for row in payload.get("results") or []:
                rows.append(parse_polygon_snapshot(row, page.received_at))
        return tuple(rows)

    def _analysis_run(
        self,
        analysis,
        batch_id: UUID,
        status: AnalysisStatus,
        started_at: datetime,
        completed_at: datetime | None,
    ) -> OptionAnalysisRun:
        health = analysis.chain_health
        return OptionAnalysisRun(
            matrix_id=analysis.matrix_id,
            batch_id=batch_id,
            underlyer=analysis.underlying.underlyer,
            context=analysis.context,
            status=status,
            received_contract_count=health.received_count,
            eligible_contract_count=len(analysis.contracts),
            unknown_reference_count=int(
                health.unknown_reference_fraction * health.received_count
            ),
            iv_attempt_count=sum(
                snapshot.iv_converged or snapshot.iv_failure_reason is not None
                for snapshot in self.snapshot_repository.list_for_batch(
                    batch_id,
                    analysis.context,
                )
                if snapshot.model_mark is not None
            ),
            iv_converged_count=len(analysis.contracts),
            iv_convergence_fraction=health.iv_convergence_fraction,
            quality_reasons=health.reasons,
            chain_health_json=json.dumps(
                asdict(health),
                sort_keys=True,
                separators=(",", ":"),
            ),
            policy_version=self.configuration.policy.policy_version,
            policy_sha256=self.configuration.policy_sha256,
            model_version=self.normalizer.model_version,
            started_at=started_at,
            completed_at=completed_at,
        )

    @staticmethod
    def _analysis_status(status: str) -> AnalysisStatus:
        if status == "COMPLETE" or status == "DEGRADED":
            return AnalysisStatus.COMPLETE
        return AnalysisStatus.MODEL_QUALITY_FAILED


def _as_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)