from __future__ import annotations

import json
import logging
import socket
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Callable, Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from options.analytics.analysis_engine import OptionAnalysisEngine
from options.analytics.marks import UnderlyingMinuteBar
from options.calendar import OptionExchangeCalendar
from options.config import OptionRuntimeConfiguration
from options.model_inputs import (
    DividendCashFlow,
    TREASURY_CURVE_SOURCE,
    interpolate_rate,
)
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
    ContractType,
    DataQualityFlag,
    DecisionContext,
    OptionAnalysisRun,
    OptionContractCatalogEntry,
    OptionContractSnapshot,
    OptionUniverseMember,
    OptionUniverseMode,
    UniverseRunStatus,
    reference_drift_failed,
)
from options.repositories.analysis import OptionAnalysisRepository
from options.repositories.board import (
    BoardPublicationResult,
    OptionBoardPublicationRepository,
)
from options.repositories.catalog import OptionContractCatalogRepository
from options.repositories.daily_facts import (
    DailyOpenInterestRecord,
    OptionDailyFactRepository,
)
from options.repositories.gamma import GammaProfileRecord, OptionGammaProfileRepository
from options.repositories.ingestion import OptionIngestionRepository
from options.repositories.model_inputs import OptionModelInputRepository
from options.repositories.outcomes import OptionOutcomeRepository
from options.repositories.snapshots import OptionSnapshotRepository
from options.repositories.trades import OptionTradeRepository
from options.repositories.universe import OptionUniverseRepository
from options.repositories.work_items import OptionWorkItemRepository
from options.strategy_orchestration import OptionStrategyPipeline
from equity.domain import DecisionWatermark, EvidenceType
from equity.repositories import (
    EquityCorporateActionRepository,
    EquityEvidenceRepository,
)

# Overlap re-requested on the next cycle so a restart cannot skip prints that
# arrived out of order at the watermark boundary.
_TRADE_CURSOR_OVERLAP_SECONDS = 60
LOGGER = logging.getLogger("option-pipeline")


@dataclass(frozen=True, slots=True)
class TradeIngestionResult:
    watchlist_count: int
    requested_count: int
    persisted_count: int
    reasons: tuple[str, ...]


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
    retryable: bool = False


@dataclass(frozen=True, slots=True)
class ManualCycleResult:
    universe_run_id: UUID
    as_of_session: date
    started_at: datetime
    completed_at: datetime
    results: tuple[UnderlyingCycleResult, ...]
    board_publication: BoardPublicationResult | None = None


class TerminalOptionQualityError(RuntimeError):
    pass


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
        gamma_repository: OptionGammaProfileRepository | None = None,
        daily_fact_repository: OptionDailyFactRepository | None = None,
        trade_repository: OptionTradeRepository | None = None,
        work_repository: OptionWorkItemRepository | None = None,
        normalizer: DeveloperOptionNormalizer | None = None,
        analysis_engine: OptionAnalysisEngine | None = None,
        strategy_pipeline: OptionStrategyPipeline | None = None,
        equity_evidence_repository: EquityEvidenceRepository | None = None,
        outcome_repository: OptionOutcomeRepository | None = None,
        board_repository: OptionBoardPublicationRepository | None = None,
        model_input_repository: OptionModelInputRepository | None = None,
        corporate_action_repository: EquityCorporateActionRepository | None = None,
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
        self.gamma_repository = gamma_repository or OptionGammaProfileRepository()
        self.daily_fact_repository = daily_fact_repository or OptionDailyFactRepository()
        self.trade_repository = trade_repository or OptionTradeRepository()
        self.work_repository = work_repository or OptionWorkItemRepository()
        self.normalizer = normalizer or DeveloperOptionNormalizer(
            configuration.policy,
            configuration.valuation_policy,
        )
        self.analysis_engine = analysis_engine or OptionAnalysisEngine(
            configuration.policy, configuration.gamma_policy
        )
        self.strategy_pipeline = strategy_pipeline
        self.outcome_repository = outcome_repository
        self.board_repository = board_repository or OptionBoardPublicationRepository()
        self.model_input_repository = model_input_repository
        if (
            self.model_input_repository is None
            and configuration.settings.risk_free_rate_source == TREASURY_CURVE_SOURCE
        ):
            self.model_input_repository = OptionModelInputRepository()
        self.corporate_action_repository = corporate_action_repository
        if (
            self.corporate_action_repository is None
            and configuration.settings.dividend_input_source is not None
        ):
            self.corporate_action_repository = EquityCorporateActionRepository()
        self.equity_evidence_repository = (
            equity_evidence_repository or EquityEvidenceRepository()
        )
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def latest_due_retry_cycle(self) -> datetime | None:
        return self.work_repository.latest_due_cycle(
            self.configuration.policy_sha256,
            self.configuration.configuration_sha256,
        )

    def run_once(
        self,
        underlyers: tuple[str, ...] | None = None,
        *,
        as_of: datetime | None = None,
        cycle_time: datetime | None = None,
        progress_callback: Callable[[], object] | None = None,
    ) -> ManualCycleResult:
        started_at = _as_utc(self.clock(), "clock")
        self.work_repository.recover_expired_claims()
        requested = underlyers or self.configuration.settings.underlyers
        unknown = set(requested) - set(self.configuration.settings.underlyers)
        if unknown:
            raise ValueError(f"underlyers are not configured: {sorted(unknown)}")
        as_of = _as_utc(as_of, "as_of") if as_of is not None else started_at
        if cycle_time is None:
            as_of_session = self.calendar.latest_completed_session(as_of)
            cycle_time = self.calendar.expiration_cutoff(as_of_session)
        else:
            cycle_time = _as_utc(cycle_time, "cycle_time")
            if cycle_time > as_of:
                raise ValueError("cycle_time cannot be later than as_of")
            as_of_session = self.calendar.session_for_slot(cycle_time)
        universe_run_id = uuid5(
            NAMESPACE_URL,
            (
                f"option-universe:{as_of_session}:{cycle_time.isoformat()}:"
                f"{self.configuration.configuration_sha256}:"
                f"{','.join(requested)}"
            ),
        )
        universe_members = self._persist_fixed_universe(
            universe_run_id,
            as_of_session,
            requested,
            started_at,
        )
        results = []
        for underlyer in requested:
            if progress_callback is not None:
                progress_callback()
            results.append(self._run_underlying(
                underlyer,
                universe_members[underlyer],
                as_of_session,
                cycle_time,
            ))
        if progress_callback is not None:
            progress_callback()
        results = tuple(results)
        completed_at = _as_utc(self.clock(), "clock")
        completed_count = sum(
            result.status in {"COMPLETE", "DEGRADED", "ALREADY_COMPLETED"}
            for result in results
        )
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
        board_publication = None
        if (
            self.strategy_pipeline is not None
            and len(requested) == len(self.configuration.settings.underlyers)
            and set(requested) == set(self.configuration.settings.underlyers)
        ):
            board_publication = self.board_repository.publish_complete_cycle(
                scheduled_cycle=cycle_time,
                as_of_session=as_of_session,
                expected_underlyers=self.configuration.settings.underlyers,
                strategy_policy_sha256=(
                    self.configuration.strategy_policy_sha256
                ),
                configuration_sha256=self.configuration.configuration_sha256,
                published_at=completed_at,
            )
        return ManualCycleResult(
            universe_run_id=universe_run_id,
            as_of_session=as_of_session,
            started_at=started_at,
            completed_at=completed_at,
            results=results,
            board_publication=board_publication,
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
            if self.outcome_repository is not None:
                followup = self.outcome_repository.retained_leg_bounds(
                    underlyer,
                    available_by=_as_utc(self.clock(), "clock"),
                )
                if followup is not None:
                    strike_min = min(strike_min, followup["minimum_strike"])
                    strike_max = max(strike_max, followup["maximum_strike"])
                    expiration_through = max(
                        expiration_through,
                        followup["expiration_through"],
                    )
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
            matrix_id = uuid5(
                NAMESPACE_URL,
                f"option-matrix:{batch_id}:{self.configuration.policy_sha256}",
            )
            business_key = f"normalize:{batch_id}"
            work_item = self.work_repository.claim_by_business_key(
                business_key,
                lease_owner,
                timedelta(minutes=10),
            )
            existing_analysis = self.analysis_repository.get(
                matrix_id,
                DecisionContext(
                    _as_utc(self.clock(), "clock"),
                    _as_utc(self.clock(), "clock"),
                ),
            )
            if work_item is None:
                existing_work = self.work_repository.get_by_business_key(business_key)
                if (
                    existing_work is not None
                    and existing_work.status.value == "COMPLETED"
                    and existing_analysis is not None
                    and existing_analysis.status not in (
                        AnalysisStatus.PENDING, AnalysisStatus.RUNNING,
                    )
                ):
                    self._ensure_persisted_strategy(existing_analysis, asset_type)
                    return self._existing_result(
                        existing_analysis, batch_id, asset_type,
                    )
                raise RuntimeError("normalization work item could not be claimed")
            if (
                existing_analysis is not None
                and existing_analysis.status not in (
                    AnalysisStatus.PENDING, AnalysisStatus.RUNNING,
                )
            ):
                self._ensure_persisted_strategy(existing_analysis, asset_type)
                if not self.work_repository.complete(work_item.work_id, lease_owner):
                    raise RuntimeError("normalization work lease expired before acknowledgement")
                return self._existing_result(existing_analysis, batch_id, asset_type)
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
            bar_window_observed_at = max(
                catalog_observed_time,
                _as_utc(self.clock(), "clock"),
            )
            mark_window = _fresh_mark_window(
                raw_rows,
                bar_window_observed_at,
                self.configuration.valuation_policy.maximum_source_age_seconds,
            )
            bars: tuple[UnderlyingMinuteBar, ...] = ()
            if mark_window is not None:
                bars = self.engine.get_underlying_minute_bars(
                    underlyer,
                    mark_window[0] - timedelta(minutes=1),
                    mark_window[1],
                )
            observed_time = max(
                bar_window_observed_at,
                _as_utc(self.clock(), "clock"),
            )
            context = DecisionContext(cycle_time, observed_time)
            rate_curve = ()
            if self.configuration.settings.risk_free_rate_source == TREASURY_CURVE_SOURCE:
                if self.model_input_repository is None:
                    raise RuntimeError("Treasury rate repository is unavailable")
                rate_curve = self.model_input_repository.latest_curve(
                    source=TREASURY_CURVE_SOURCE,
                    market_date=context.market_time.date(),
                    observed_at=context.observed_time,
                )
                if not rate_curve:
                    raise TerminalOptionQualityError(
                        "risk-free Treasury curve is unavailable"
                    )
            dividend_yield = float(
                self.configuration.settings.default_dividend_yield
            )
            dividend_quality_flags = (
                DataQualityFlag.DIVIDEND_YIELD_DEFAULTED,
            )
            dividend_cash_flows: tuple[DividendCashFlow, ...] = ()
            dividend_coverage_available = False
            if self.configuration.settings.dividend_input_source is not None:
                if self.corporate_action_repository is None:
                    raise RuntimeError("corporate-action repository is unavailable")
                maximum_expiration = (
                    context.market_time.date()
                    + timedelta(
                        days=self.configuration.policy.contract_filter.maximum_dte
                    )
                )
                coverage, dividend_actions = (
                    self.corporate_action_repository.latest_covered_actions(
                        underlyer,
                        "DIVIDEND",
                        window_start=context.market_time.date(),
                        window_end=maximum_expiration,
                        observed_at=context.observed_time,
                        maximum_age=timedelta(
                            seconds=(
                                self.configuration.settings
                                .dividend_input_max_age_seconds
                            )
                        ),
                    )
                )
                if coverage is None:
                    raise TerminalOptionQualityError(
                        "dividend corporate-action coverage is unavailable"
                    )
                dividend_cash_flows = tuple(
                    DividendCashFlow(
                        ex_date=row["ex_date"],
                        cash_amount=Decimal(str(row["cash_amount"])),
                    )
                    for row in dividend_actions
                    if row.get("ex_date") is not None
                    and row.get("cash_amount") is not None
                    and Decimal(str(row["cash_amount"])) > 0
                )
                dividend_quality_flags = ()
                dividend_coverage_available = True
            elif self.configuration.settings.equity_context_enabled:
                fundamental_evidence = self.equity_evidence_repository.list_as_of(
                    underlyer,
                    DecisionWatermark(context.market_time, context.observed_time),
                    evidence_types=(EvidenceType.FUNDAMENTAL_SNAPSHOT,),
                )
                dividend_yield, dividend_quality_flags = _resolve_dividend_yield(
                    fundamental_evidence,
                    dividend_yield,
                )
            normalization_inputs = tuple(
                DeveloperNormalizationInput(
                    raw=row,
                    catalog=catalog[row.contract_ticker],
                    underlying_bars=bars,
                    expiration_cutoff=self.calendar.expiration_cutoff(
                        catalog[row.contract_ticker].expiration_date
                    ),
                    risk_free_rate=(
                        interpolate_rate(
                            rate_curve,
                            max(
                                (
                                    catalog[row.contract_ticker].expiration_date
                                    - context.market_time.date()
                                ).days,
                                1,
                            ),
                        )
                        if rate_curve
                        else float(self.configuration.settings.risk_free_rate)
                    ),
                    dividend_yield=dividend_yield,
                    dividend_cash_flows=dividend_cash_flows,
                    dividend_coverage_available=dividend_coverage_available,
                    input_quality_flags=dividend_quality_flags,
                    normalized_observed_at=observed_time,
                )
                for row in raw_rows
                if row.contract_ticker in catalog
            )
            normalized = self.normalizer.normalize(batch_id, normalization_inputs)
            open_interest_captured = self._capture_open_interest(
                raw_rows, catalog, underlyer, cycle_time, observed_time, batch_id
            )
            self.snapshot_repository.persist(
                normalized.snapshots,
                asset_type,
                self.configuration.policy.policy_version,
                self.configuration.policy_sha256,
            )
            rejected_counts = dict(normalized.rejected_counts)
            if unknown_count:
                rejected_counts["UNKNOWN_REFERENCE"] = unknown_count
            matrix_market_time = (
                max(
                    snapshot.market_data_time
                    for snapshot in normalized.matrix_snapshots
                )
                if normalized.matrix_snapshots
                else None
            )
            self.ingestion_repository.record_normalization(
                batch_id,
                catalog_row_count=len(catalog),
                retained_row_count=normalized.retained_count,
                rejected_counts=rejected_counts,
                unknown_reference_count=unknown_count,
                market_data_time=matrix_market_time,
                first_observed_at=observed_time,
            )
            if not normalized.matrix_snapshots:
                raise TerminalOptionQualityError(
                    "normalization produced no retained contracts"
                )
            trade_ingestion = self._ingest_trades(
                normalized.matrix_snapshots, catalog, matrix_market_time
            )
            decision_observed_time = max(
                observed_time,
                _as_utc(self.clock(), "clock"),
            )
            context = DecisionContext(matrix_market_time, decision_observed_time)
            execution_lag = (
                decision_observed_time - cycle_time
            ).total_seconds()
            execution_lag_exceeded = (
                execution_lag
                > self.configuration.settings.maximum_execution_lag_seconds
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
                asset_type=asset_type,
                execution_lag_exceeded=execution_lag_exceeded,
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
            self._persist_gamma_profiles(
                analysis,
                underlyer,
                matrix_market_time,
                decision_observed_time,
            )
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
                reasons=(
                    analysis.chain_health.reasons
                    + trade_ingestion.reasons
                    + (
                        ()
                        if open_interest_captured
                        else ("NO_SETTLED_OPEN_INTEREST_CAPTURED",)
                    )
                ),
            )
        except Exception as exc:
            retryable = not isinstance(exc, TerminalOptionQualityError)
            if work_item is not None:
                if retryable:
                    self.work_repository.retry(
                        work_item.work_id,
                        lease_owner,
                        str(exc),
                        timedelta(minutes=5),
                    )
                else:
                    self.work_repository.terminal_fail(
                        work_item.work_id,
                        lease_owner,
                        str(exc),
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
                reasons=(type(exc).__name__, str(exc)),
                retryable=retryable,
            )

    def _trade_watchlist(
        self,
        snapshots: tuple[OptionContractSnapshot, ...],
        catalog: Mapping[str, OptionContractCatalogEntry],
    ) -> tuple[OptionContractCatalogEntry, ...]:
        """Bounded set of contracts worth pulling prints for.

        Scoped to the shape the sweep detector can actually use - out-of-the-money
        calls with observed activity - and capped so provider cost stays proportional
        to the universe rather than the chain.
        """
        eligible = [
            snapshot
            for snapshot in snapshots
            if snapshot.contract_type is ContractType.CALL
            and snapshot.strike > snapshot.spot
            and snapshot.day_volume
            and snapshot.contract_ticker in catalog
        ]
        eligible.sort(
            key=lambda snapshot: (
                -(snapshot.day_volume or 0),
                -(snapshot.open_interest or 0),
                snapshot.expiration_date,
                snapshot.contract_id,
            )
        )
        limit = self.configuration.settings.trade_watchlist_per_underlyer
        seen: set[int] = set()
        selected: list[OptionContractCatalogEntry] = []
        for snapshot in eligible:
            if snapshot.contract_id in seen:
                continue
            seen.add(snapshot.contract_id)
            selected.append(catalog[snapshot.contract_ticker])
            if len(selected) >= limit:
                break
        return tuple(selected)

    def _capture_open_interest(
        self,
        raw_rows: tuple[RawDeveloperOptionObservation, ...],
        catalog: Mapping[str, OptionContractCatalogEntry],
        underlyer: str,
        cycle_time: datetime,
        observed_time: datetime,
        batch_id,
    ) -> int:
        """Retain the settled open interest carried by this cycle's chain snapshot.

        Taken from the full raw chain rather than the corridor-filtered matrix: the
        provider already returned every contract, and anything discarded here can never
        be recovered because no endpoint serves open interest for a past session.

        The provider reports the prior session's settled quantity even at the current
        session's closing slot, so wall-clock close detection is not the fact boundary.
        """
        observed_session = self.calendar.session_for_slot(cycle_time)
        settlement_session = self.calendar.previous_session(observed_session)
        records = [
            DailyOpenInterestRecord(
                contract_id=catalog[row.contract_ticker].contract_id,
                settlement_session=settlement_session,
                underlying=underlyer,
                open_interest=row.open_interest,
                observed_at=observed_time,
                observed_session=observed_session,
                batch_id=batch_id,
            )
            for row in raw_rows
            if row.open_interest is not None and row.contract_ticker in catalog
        ]
        return self.daily_fact_repository.persist_open_interest(records)

    def _ingest_trades(
        self,
        snapshots: tuple[OptionContractSnapshot, ...],
        catalog: Mapping[str, OptionContractCatalogEntry],
        market_time: datetime,
    ) -> TradeIngestionResult:
        if not self.configuration.settings.trade_ingestion_enabled:
            return TradeIngestionResult(0, 0, 0, ("TRADE_INGESTION_DISABLED",))
        contracts = self._trade_watchlist(snapshots, catalog)
        if not contracts:
            return TradeIngestionResult(0, 0, 0, ("NO_TRADE_WATCHLIST_CONTRACT",))
        lookback = timedelta(
            seconds=self.configuration.settings.trade_lookback_seconds
        )
        reasons: list[str] = []
        requested = 0
        persisted = 0
        monotonic = getattr(self, "monotonic", time.monotonic)
        started_monotonic = monotonic()
        for contract in contracts:
            if (
                monotonic() - started_monotonic
                >= self.configuration.settings.trade_ingestion_budget_seconds
            ):
                reasons.append("TRADE_INGESTION_BUDGET_EXCEEDED")
                LOGGER.warning(
                    "option trade ingestion budget exceeded underlyer=%s "
                    "budget_seconds=%s completed_contracts=%s watchlist=%s",
                    snapshots[0].underlyer,
                    self.configuration.settings.trade_ingestion_budget_seconds,
                    requested,
                    len(contracts),
                )
                break
            cursor = self.trade_repository.get_cursor("polygon", contract.contract_id)
            try:
                result = self.engine.get_option_trades(
                    contract,
                    market_time - lookback,
                    market_time,
                    cursor,
                )
            except Exception as exc:  # provider failure must not fail the matrix
                reasons.append(f"TRADE_FETCH_FAILED:{type(exc).__name__}")
                LOGGER.warning(
                    "option trade fetch skipped; continuing contract=%s "
                    "underlyer=%s error=%s",
                    contract.contract_ticker,
                    contract.underlyer,
                    type(exc).__name__,
                )
                continue
            requested += 1
            if not result.events:
                continue
            persisted += self.trade_repository.persist(result.events)
            latest = max(
                result.events,
                key=lambda event: (event.sip_timestamp, event.sequence_number),
            )
            if result.complete:
                self.trade_repository.advance_cursor(
                    "polygon",
                    contract.contract_id,
                    latest.sip_timestamp,
                    latest.sequence_number,
                    _TRADE_CURSOR_OVERLAP_SECONDS,
                    result.request_ids[-1] if result.request_ids else None,
                )
            else:
                reasons.append("TRADE_FETCH_INCOMPLETE")
        return TradeIngestionResult(
            len(contracts), requested, persisted, tuple(dict.fromkeys(reasons))
        )

    def _persist_gamma_profiles(        self,
        analysis,
        underlyer: str,
        market_time: datetime,
        observed_time: datetime,
    ) -> None:
        if not analysis.gamma_profiles:
            return
        self.gamma_repository.persist(
            [
                GammaProfileRecord(
                    matrix_id=analysis.matrix_id,
                    underlying=underlyer,
                    scope=scoped.scope,
                    market_data_time=market_time,
                    first_observed_at=observed_time,
                    profile=scoped.profile,
                    gamma_policy_version=(
                        self.configuration.gamma_policy.gamma_policy_version
                    ),
                    gamma_policy_sha256=self.configuration.gamma_policy_sha256,
                )
                for scoped in analysis.gamma_profiles
            ]
        )

    @staticmethod
    def _existing_result(
        analysis: OptionAnalysisRun,
        batch_id: UUID,
        asset_type: AssetType,
    ) -> UnderlyingCycleResult:
        health = json.loads(analysis.chain_health_json)
        return UnderlyingCycleResult(
            underlyer=analysis.underlyer,
            asset_type=asset_type,
            batch_id=batch_id,
            matrix_id=analysis.matrix_id,
            status="ALREADY_COMPLETED",
            received_count=analysis.received_contract_count,
            retained_count=int(health.get("retained_count") or 0),
            iv_convergence_fraction=analysis.iv_convergence_fraction,
            reasons=tuple(analysis.quality_reasons),
        )

    def _ensure_persisted_strategy(
        self,
        analysis: OptionAnalysisRun,
        asset_type: AssetType,
    ) -> None:
        if self.strategy_pipeline is None:
            return
        result = self.strategy_pipeline.process_persisted(analysis, asset_type)
        if result.status not in {"COMPLETE", "ALREADY_COMPLETED"}:
            raise RuntimeError(f"strategy pipeline failed: {result.error}")

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


def _fresh_mark_window(
    rows: tuple[RawDeveloperOptionObservation, ...],
    observed_at: datetime,
    maximum_source_age_seconds: int,
) -> tuple[datetime, datetime] | None:
    observed_at = _as_utc(observed_at, "observed_at")
    earliest = observed_at - timedelta(seconds=maximum_source_age_seconds)
    marks = [
        row.option_mark_time
        for row in rows
        if row.option_mark_time is not None
        and earliest <= row.option_mark_time <= observed_at
    ]
    return (min(marks), max(marks)) if marks else None


def _resolve_dividend_yield(
    fundamental_evidence,
    default_yield: float,
) -> tuple[float, tuple[DataQualityFlag, ...]]:
    ordered = sorted(
        fundamental_evidence,
        key=lambda row: (row.market_time, row.observed_at),
        reverse=True,
    )
    for evidence in ordered:
        payload = json.loads(evidence.payload_json)
        value = payload.get("dividend_yield")
        if value is None:
            continue
        try:
            dividend_yield = float(value)
        except (TypeError, ValueError):
            continue
        if 0 <= dividend_yield <= 1:
            return dividend_yield, ()
    return default_yield, (DataQualityFlag.DIVIDEND_YIELD_DEFAULTED,)


def _as_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)