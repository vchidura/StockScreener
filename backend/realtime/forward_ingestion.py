from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from importlib.metadata import version
from types import MappingProxyType
from uuid import NAMESPACE_URL, UUID, uuid5
from zoneinfo import ZoneInfo

from equity.domain import (
    BarAvailabilityMode, BarSessionScope, BarSourceKind, EquityBarRevision,
    EquityCorporateActionCoverage, SecurityReferenceRevision,
)
from options.analytics.analysis_engine import OptionAnalysisEngine, OptionAnalysisSnapshot
from options.calendar import OptionExchangeCalendar
from options.config import OptionRuntimeConfiguration
from options.data.normalizer import DeveloperNormalizationInput, DeveloperOptionNormalizer, parse_polygon_snapshot
from options.detector_collection import DetectorCycleCollector, DetectorCycleInputs
from options.domain import DecisionContext, OptionContractCatalogEntry, OptionContractSnapshot
from options.dual_origin import ACTIVITY_POLICY, ActivitySource, assess_options_canonical_trend, bind_activity_source, detect_option_participation
from options.intraday_participation import (
    CANONICAL_TREND_ALIGNMENT_POLICY, CANONICAL_TREND_SOURCE_POLICY, O1_INDICATOR_REVIEW_POLICY_V2,
    CanonicalTrendStockEvidence,
    build_canonical_trend_evidence, build_o1_indicator_observation_v2, latest_completed_30m_window,
)
from options.repositories.daily_facts import DailyOpenInterestRecord, OPEN_INTEREST_SOURCE_CHAIN_SNAPSHOT
from options.repositories.stock_behavior_assessments import OptionMatrixAssessmentLineage

from .contracts import canonical_sha256
from .option_session_manifest import (
    ContractSelectionStatus, ManifestSelectionStatus, SelectedContractManifestPlan,
)
from .preflight import DetectorFamily, ProviderTier


FORWARD_CONTRACT = "forward_option_matrix_v1"
DEVELOPER_SOURCE = "developer_rest_matrix_v1"
FORWARD_DATASET_IDENTITY = "forward_detector_dataset_v2"
FORWARD_CONTEXT_CONTRACT = "forward_native_technical_context_v2"


def _aware(value: datetime) -> None:
    if value.utcoffset() is None:
        raise ValueError("forward clocks must be timezone-aware")


def _digest(value: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("forward lineage requires SHA-256 digests")


@dataclass(frozen=True, slots=True)
class ForwardMatrixScope:
    cohort_id: UUID
    selection: SelectedContractManifestPlan
    catalog: tuple[OptionContractCatalogEntry, ...]
    configuration_sha256: str
    reference_sha256: str
    action_sha256: str
    cutoff: datetime
    available_after: datetime
    deadline: datetime

    def __post_init__(self) -> None:
        for clock in (self.cutoff, self.available_after, self.deadline):
            _aware(clock)
        for digest in (self.configuration_sha256, self.reference_sha256, self.action_sha256):
            _digest(digest)
        plan = self.selection
        if plan.status is not ManifestSelectionStatus.READY or plan.manifest is None:
            raise ValueError("forward ingestion requires a READY exact option manifest")
        if not self.cutoff <= self.available_after < self.deadline:
            raise ValueError("forward release/deadline clocks are inconsistent")
        if self.cutoff.astimezone(ZoneInfo("America/New_York")).date() != plan.session_date:
            raise ValueError("forward cutoff is outside the manifest session")
        selected = tuple(row for row in plan.dispositions if row.status is ContractSelectionStatus.INCLUDED)
        by_contract = {row.contract_id: row for row in self.catalog}
        if len(by_contract) != len(self.catalog) or set(by_contract) != {row.contract_id for row in selected}:
            raise ValueError("forward catalog must cover the exact selected manifest")
        for member in selected:
            catalog = by_contract[member.contract_id]
            if (catalog.contract_ticker, catalog.underlyer, catalog.expiration_date,
                catalog.contract_type, catalog.strike) != (
                member.contract_ticker, member.underlyer, member.expiration_date,
                member.contract_type, member.strike,
            ):
                raise ValueError("forward catalog/manifest identity drift")
            if (catalog.valid_from > self.cutoff
                    or catalog.valid_to is not None and catalog.valid_to <= self.cutoff
                    or max(catalog.first_observed_at, catalog.revised_observed_at or catalog.first_observed_at) > self.cutoff):
                raise ValueError("forward reference was not available at the cutoff")
        object.__setattr__(self, "catalog", tuple(sorted(self.catalog, key=lambda row: row.contract_id)))

    @property
    def sha256(self) -> str:
        return canonical_sha256({"version": FORWARD_CONTRACT, "scope": self})

    @property
    def underlyings(self) -> tuple[str, ...]:
        return self.selection.scope.universe.option_underlyings


@dataclass(frozen=True, slots=True)
class DeveloperMatrixPage:
    cursor: str | None
    next_cursor: str | None
    response_sha256: str
    inputs: tuple[DeveloperNormalizationInput, ...]

    def __post_init__(self) -> None:
        _digest(self.response_sha256)


@dataclass(frozen=True, slots=True)
class PolicyChainMatrixScope(ForwardMatrixScope):
    matrix_scope_version: str = "developer_policy_chain_matrix_v1"

    def __post_init__(self):
        if self.matrix_scope_version != "developer_policy_chain_matrix_v1":
            raise ValueError("unknown full policy-chain matrix scope")
        selected_ids = {row.contract_id for row in self.selection.dispositions if row.status is ContractSelectionStatus.INCLUDED}
        selected = tuple(row for row in self.catalog if row.contract_id in selected_ids)
        ForwardMatrixScope(self.cohort_id, self.selection, selected, self.configuration_sha256,
            self.reference_sha256, self.action_sha256, self.cutoff, self.available_after, self.deadline)
        if len({row.contract_id for row in self.catalog}) != len(self.catalog) or len(self.catalog) > 10000:
            raise ValueError("full policy-chain catalog is ambiguous or exceeds row budget")
        for row in self.catalog:
            if (row.underlyer not in self.underlyings or row.valid_from > self.cutoff
                    or row.valid_to is not None and row.valid_to <= self.cutoff
                    or max(row.first_observed_at, row.revised_observed_at or row.first_observed_at) > self.cutoff):
                raise ValueError("full policy-chain reference scope or availability drift")
        object.__setattr__(self, "catalog", tuple(sorted(self.catalog, key=lambda row: row.contract_id)))


@dataclass(frozen=True, slots=True)
class CompletedDeveloperMatrixSource:
    cohort_id: UUID
    selection_sha256: str
    underlyer: str
    cutoff: datetime
    started_at: datetime
    completed_at: datetime
    pages: tuple[DeveloperMatrixPage, ...]


@dataclass(frozen=True, slots=True)
class DeveloperMatrixIngress:
    scope: ForwardMatrixScope
    source: CompletedDeveloperMatrixSource
    inputs: tuple[DeveloperNormalizationInput, ...]

    @property
    def sha256(self) -> str:
        return canonical_sha256({"source_kind": DEVELOPER_SOURCE, "ingress": self})

    @property
    def batch_id(self) -> UUID:
        return uuid5(NAMESPACE_URL, f"{DEVELOPER_SOURCE}:{self.sha256}")


def admit_developer_matrix(
    scope: ForwardMatrixScope, source: CompletedDeveloperMatrixSource,
) -> DeveloperMatrixIngress:
    if type(source) is not CompletedDeveloperMatrixSource:
        raise TypeError("Developer ingress requires a completed REST matrix, never stream events")
    if scope.selection.scope.provider_tier is not ProviderTier.MASSIVE_DEVELOPER:
        raise ValueError("Developer source cannot substitute for another tier")
    if (source.cohort_id != scope.cohort_id
            or source.selection_sha256 != scope.selection.selection_sha256
            or source.cutoff != scope.cutoff or source.underlyer not in scope.underlyings):
        raise ValueError("Developer source scope mismatch")
    for clock in (source.cutoff, source.started_at, source.completed_at):
        _aware(clock)
    if not scope.available_after <= source.started_at <= source.completed_at <= scope.deadline:
        raise ValueError("Developer source missed its release/deadline window")
    if not 1 <= len(source.pages) <= 100:
        raise ValueError("Developer matrix page budget exceeded")
    if sum(len(page.inputs) for page in source.pages) > 10_000:
        raise ValueError("Developer matrix row budget exceeded")
    cursor = None
    visited = set()
    rows = {}
    expected = {row.contract_id: row for row in scope.catalog if row.underlyer == source.underlyer}
    for index, page in enumerate(source.pages):
        if page.cursor != cursor or page.cursor in visited:
            raise ValueError("Developer pagination chain is incomplete or cyclic")
        visited.add(page.cursor)
        cursor = page.next_cursor
        if (cursor is None) != (index == len(source.pages) - 1):
            raise ValueError("Developer pagination requires exactly one terminal page")
        for item in page.inputs:
            if type(item) is not DeveloperNormalizationInput:
                raise TypeError("Developer input cannot contain Advanced normalized events")
            if expected.get(item.catalog.contract_id) != item.catalog:
                raise ValueError("Developer input reference identity drift")
            if parse_polygon_snapshot(json.loads(item.raw.raw_payload_json), item.raw.first_observed_at,
                    item.raw.revised_observed_at) != item.raw:
                raise ValueError("Developer raw payload changed")
            if item.raw.option_mark_time is None or item.raw.option_mark_time > scope.cutoff:
                raise ValueError("Developer source contains a future or missing market clock")
            available = item.normalized_observed_at or item.raw.revised_observed_at or item.raw.first_observed_at
            if not item.raw.option_mark_time <= item.raw.first_observed_at <= available <= source.completed_at:
                raise ValueError("Developer input observation clocks are not causal")
            prior = rows.get(item.catalog.contract_id)
            if prior is not None and prior != item:
                raise ValueError("Developer matrix contains conflicting contract revisions")
            rows[item.catalog.contract_id] = item
    if set(rows) != set(expected) or not rows:
        raise ValueError("Developer matrix is missing exact manifest contracts")
    return DeveloperMatrixIngress(scope, source, tuple(rows[key] for key in sorted(rows)))


@dataclass(frozen=True, slots=True)
class CompleteForwardMatrix:
    ingress: DeveloperMatrixIngress
    snapshots: tuple[OptionContractSnapshot, ...]
    analysis: OptionAnalysisSnapshot
    content_sha256: str

    @property
    def matrix_id(self) -> UUID:
        return self.analysis.matrix_id


def normalize_developer_matrix(
    ingress: DeveloperMatrixIngress, configuration: OptionRuntimeConfiguration,
) -> CompleteForwardMatrix:
    checked = admit_developer_matrix(ingress.scope, ingress.source)
    if checked != ingress or configuration.configuration_sha256 != ingress.scope.configuration_sha256:
        raise ValueError("Developer ingress or configuration changed before normalization")
    normalized = DeveloperOptionNormalizer(configuration.policy, configuration.valuation_policy).normalize(
        ingress.batch_id, ingress.inputs,
    )
    snapshots = normalized.matrix_snapshots
    exact_members = {row.contract_id for row in snapshots} == {row.catalog.contract_id for row in ingress.inputs}
    if not snapshots or (not isinstance(ingress.scope, PolicyChainMatrixScope) and not exact_members):
        raise ValueError("normalization did not produce the exact complete matrix")
    if any(row.bid is not None or row.ask is not None or row.midpoint is not None for row in snapshots):
        raise ValueError("Developer matrix cannot claim Advanced NBBO")
    asset_types = {row.catalog.asset_type for row in ingress.inputs}
    if len(asset_types) != 1:
        raise ValueError("forward matrix must retain one exact underlying asset type")
    context = DecisionContext(ingress.scope.cutoff, ingress.source.completed_at)
    analysis = OptionAnalysisEngine(configuration.policy, configuration.gamma_policy).analyze(
        uuid5(NAMESPACE_URL, f"{FORWARD_CONTRACT}:{ingress.sha256}"), snapshots, context,
        received_count=len(ingress.inputs), catalog_matched_count=len(ingress.inputs),
        unknown_reference_count=0, reference_drift_failed=False, batch_complete=True,
        asset_type=next(iter(asset_types)),
    )
    if analysis.chain_health.status != "COMPLETE":
        raise ValueError("forward matrix analysis is not complete")
    return CompleteForwardMatrix(ingress, snapshots, analysis,
        canonical_sha256((ingress.sha256, snapshots, analysis)))


@dataclass(frozen=True, slots=True)
class ForwardOpenInterestEvidence:
    revision_id: UUID
    record: DailyOpenInterestRecord
    source: str
    recorded_at: datetime
    revision_count: int = 0


@dataclass(frozen=True, slots=True)
class ForwardExchangeSessionEvidence:
    calendar_name: str
    calendar_version: str
    session_date: date
    previous_session: date
    session_open: datetime
    session_close: datetime
    observed_at: datetime
    recorded_at: datetime


@dataclass(frozen=True, slots=True)
class ForwardActivityHistoryEvidence:
    dataset_id: str
    before_cycle: datetime
    directions: tuple[tuple[str, int], ...]
    observed_at: datetime
    recorded_at: datetime


@dataclass(frozen=True, slots=True)
class ForwardDetectorContextSource:
    scope_sha256: str
    matrix_sha256s: tuple[str, ...]
    database_name: str
    provider_tier: ProviderTier
    securities: tuple[SecurityReferenceRevision, ...] = ()
    open_interest: tuple[ForwardOpenInterestEvidence, ...] = ()
    bars: tuple[tuple[EquityBarRevision, datetime], ...] = ()
    split_coverage: tuple[tuple[EquityCorporateActionCoverage, datetime], ...] = ()
    exchange_session: ForwardExchangeSessionEvidence | None = None
    activity_history: ForwardActivityHistoryEvidence | None = None

    def __post_init__(self) -> None:
        _digest(self.scope_sha256)
        for digest in self.matrix_sha256s:
            _digest(digest)
        if self.database_name != "stocks_realtime_v1":
            raise ValueError("detector context must be target-local; canonical fallback prohibited")
        if self.provider_tier is not ProviderTier.MASSIVE_DEVELOPER:
            raise ValueError("Developer detector context cannot substitute another tier")
        if (len(self.securities) > 13 or len(self.split_coverage) > 13
            or len(self.open_interest) > 10_000 or len(self.bars) > 3_000):
            raise ValueError("detector context exceeds the bounded source budget")
        object.__setattr__(self, "matrix_sha256s", tuple(sorted(self.matrix_sha256s)))
        object.__setattr__(self, "securities", tuple(sorted(self.securities, key=lambda row: row.ticker)))
        object.__setattr__(self, "open_interest", tuple(sorted(self.open_interest, key=lambda row: row.record.contract_id)))
        object.__setattr__(self, "bars", tuple(sorted(self.bars, key=lambda row: (row[0].ticker, row[0].interval, row[0].bar_end))))
        object.__setattr__(self, "split_coverage", tuple(sorted(self.split_coverage, key=lambda row: row[0].ticker)))


def forward_context_policy_sha256() -> str:
    return canonical_sha256({"contract": FORWARD_CONTEXT_CONTRACT, "activity": ACTIVITY_POLICY.sha256,
        "trend": CANONICAL_TREND_SOURCE_POLICY.sha256, "alignment": CANONICAL_TREND_ALIGNMENT_POLICY.sha256,
        "review": O1_INDICATOR_REVIEW_POLICY_V2.sha256,
        "calendar_name": "XNYS", "calendar_version": version("exchange_calendars")})


def _context_sha256(source, selected_at, inputs, dependencies) -> str:
    return canonical_sha256({"source": source, "policy_sha256": forward_context_policy_sha256(),
        "selected_at": selected_at, "matrices": inputs.matrices,
        "observations": tuple(row.model_dump(mode="json") for row in inputs.o1_observations),
        "rejections": inputs.rejections, "dependencies": dependencies})


@dataclass(frozen=True, slots=True)
class ForwardDetectorContext:
    source: ForwardDetectorContextSource
    selected_at: datetime
    inputs: DetectorCycleInputs
    dependencies: tuple[tuple[str, tuple[str, ...]], ...]
    content_sha256: str
    activity_sources: tuple[ActivitySource, ...] = ()
    stock_sources: tuple[tuple[str, CanonicalTrendStockEvidence | None], ...] = ()

    @property
    def status(self) -> str:
        if not self.dependencies:
            return "UNAVAILABLE"
        complete = sum(not reasons for _, reasons in self.dependencies)
        return "COMPLETE" if complete == len(self.dependencies) else "PARTIAL" if complete else "UNAVAILABLE"


def load_forward_detector_context(
    scope: ForwardMatrixScope, matrices: tuple[CompleteForwardMatrix, ...],
    configuration: OptionRuntimeConfiguration, source: ForwardDetectorContextSource, *, selected_at: datetime,
) -> ForwardDetectorContext:
    from collections import Counter

    _aware(selected_at)
    if type(source) is not ForwardDetectorContextSource:
        raise TypeError("detector context requires explicit target-local evidence")
    if DetectorFamily.O1 not in scope.selection.policy.required_detectors:
        raise ValueError("manifest does not enable O1 observations")
    if (source.scope_sha256 != scope.sha256 or source.provider_tier is not scope.selection.scope.provider_tier
            or configuration.configuration_sha256 != scope.configuration_sha256
            or not scope.available_after <= selected_at <= scope.deadline):
        raise ValueError("detector context scope, configuration or deadline drift")
    ordered = tuple(sorted(matrices, key=lambda row: row.analysis.underlying.underlyer))
    if not ordered or source.matrix_sha256s != tuple(sorted(row.content_sha256 for row in ordered)):
        raise ValueError("detector context must pin the exact completed matrices")
    if any(row.ingress.scope != scope or row.ingress.source.completed_at > selected_at
            or row.content_sha256 != canonical_sha256((row.ingress.sha256, row.snapshots, row.analysis)) for row in ordered):
        raise ValueError("detector context matrix lineage changed")
    completed = tuple(row.analysis.underlying.underlyer for row in ordered)
    if len(set(completed)) != len(completed):
        raise ValueError("detector context contains duplicate matrices")
    securities = {row.ticker: row for row in source.securities}
    oi = {row.record.contract_id: row for row in source.open_interest}
    coverage = {row.ticker: (row, recorded) for row, recorded in source.split_coverage}
    if (len(securities) != len(source.securities) or len(oi) != len(source.open_interest)
            or len(coverage) != len(source.split_coverage)):
        raise ValueError("detector context contains ambiguous reference revisions")
    expected_contracts = {snapshot.contract_id for matrix in ordered for snapshot in matrix.snapshots}
    if (set(securities) - set(completed) or set(coverage) - set(completed) or set(oi) - expected_contracts
            or any(bar.ticker not in completed for bar, _ in source.bars)):
        raise ValueError("detector context exceeds exact matrix membership")
    calendar = OptionExchangeCalendar()
    session = scope.selection.session_date
    exchange = source.exchange_session
    if exchange is not None:
        for clock in (exchange.session_open, exchange.session_close, exchange.observed_at, exchange.recorded_at):
            _aware(clock)
        if ((exchange.calendar_name, exchange.calendar_version, exchange.session_date, exchange.previous_session,
                exchange.session_open, exchange.session_close) != (calendar.calendar_name, version("exchange_calendars"),
                session, calendar.previous_session(session), calendar.session_open(session), calendar.session_close(session))
                or not exchange.observed_at <= exchange.recorded_at <= selected_at):
            raise ValueError("detector exchange calendar identity or availability drift")
    history = source.activity_history
    if history is not None:
        for clock in (history.before_cycle, history.observed_at, history.recorded_at):
            _aware(clock)
        if (history.dataset_id != forward_detector_dataset_id(scope, configuration) or history.before_cycle != scope.cutoff
                or not history.observed_at <= history.recorded_at <= selected_at
                or len(set(history.directions)) != len(history.directions)
                or any(symbol not in scope.underlyings or direction not in (-1, 1)
                    or type(direction) is not int for symbol, direction in history.directions)):
            raise ValueError("detector activity history dataset or availability drift")
    bars_by_symbol = {symbol: [] for symbol in completed}
    identities = set()
    for bar, recorded_at in source.bars:
        _aware(recorded_at)
        security = securities.get(bar.ticker)
        identity = (bar.ticker, bar.interval, bar.bar_end)
        if identity in identities:
            raise ValueError("detector history contains ambiguous bar revisions")
        identities.add(identity)
        quality_allowed = not bar.quality_codes or (bar.interval in ("1h", "1d")
            and bar.quality_codes == ("DERIVED_FROM_CANONICAL_30M",))
        if (security is None or bar.security_id != security.security_id or bar.interval not in ("5m", "15m", "30m", "1h", "1d")
            or not bar.is_final or bar.adjusted or not quality_allowed
                or bar.session_scope is not BarSessionScope.RTH
                or bar.availability_mode is not BarAvailabilityMode.LIVE_OBSERVED
                or bar.source_kind not in (BarSourceKind.NATIVE_REST, BarSourceKind.DERIVED)
                or bar.source_kind is BarSourceKind.DERIVED and not bar.source_bar_revision_ids
                or bar.bar_end > scope.cutoff or not bar.system_observed_at <= recorded_at <= selected_at):
            raise ValueError("detector history identity, raw basis, source tier or availability drift")
        interval_minutes = {"5m": 5, "15m": 15, "30m": 30, "1h": 60}.get(bar.interval)
        if (bar.bar_start < calendar.session_open(bar.session_date)
                or bar.bar_end > calendar.session_close(bar.session_date)
            or interval_minutes is not None and (bar.bar_start - calendar.session_open(bar.session_date)) % timedelta(minutes=interval_minutes)
            or interval_minutes is not None and bar.bar_end - bar.bar_start != min(
                timedelta(minutes=interval_minutes), calendar.session_close(bar.session_date) - bar.bar_start)
                or bar.interval == "1d" and (bar.bar_start != calendar.session_open(bar.session_date)
                    or bar.bar_end != calendar.session_close(bar.session_date))):
            raise ValueError("detector history is outside exact exchange slots")
        bars_by_symbol[bar.ticker].append({**asdict(bar), "created_at": recorded_at})
    rejected, dependencies, activity, stocks, matrix_sources = Counter(), [], [], {}, []
    for matrix in ordered:
        symbol = matrix.analysis.underlying.underlyer
        security = securities.get(symbol)
        market_time = max(row.market_data_time for row in matrix.snapshots)
        matrix_sources.append(MappingProxyType(dict(underlying=symbol, matrix_id=matrix.matrix_id,
            market_time=market_time, observed_time=matrix.ingress.source.completed_at)))
        reasons = set()
        if history is None:
            reasons.add("ACTIVITY_HISTORY_UNAVAILABLE")
        if exchange is None:
            reasons.add("EXCHANGE_CALENDAR_UNAVAILABLE")
        if security is None:
            reasons.add("STOCK_SECURITY_IDENTITY_UNAVAILABLE")
        elif not security.active or security.effective_from > market_time or security.observed_at > scope.cutoff:
            raise ValueError("detector security reference drift")
        stock = None
        if security is not None and exchange is not None:
            rows = bars_by_symbol[symbol]
            trend = [row for row in rows if row["interval"] == "30m"]
            daily = [row for row in rows if row["interval"] == "1d"]
            split = coverage.get(symbol)
            split_clear = False
            if split is not None:
                receipt, recorded = split
                _aware(recorded)
                if (receipt.security_id != security.security_id or receipt.action_type != "SPLIT"
                        or receipt.availability_mode is not BarAvailabilityMode.LIVE_OBSERVED
                        or not receipt.first_observed_at <= recorded <= selected_at):
                    raise ValueError("detector split reference or availability drift")
                split_clear = bool(trend and receipt.window_start <= trend[0]["session_date"]
                    and receipt.window_end >= session and receipt.response_action_count == 0
                    and selected_at - receipt.first_observed_at <= timedelta(hours=24))
            stock = build_canonical_trend_evidence(security_id=security.security_id, underlyer=symbol,
                session_date=session, required_window=latest_completed_30m_window(market_time), trend_bars=trend,
                daily_bars=daily, split_clear=split_clear, received_at=selected_at, session_close=exchange.session_close)
            reasons.update(stock.reason_codes)
        stocks[symbol] = stock
        lineage = OptionMatrixAssessmentLineage(matrix.matrix_id, symbol, market_time,
            matrix.ingress.source.completed_at, scope.cutoff, configuration.configuration_sha256,
            configuration.policy_sha256, configuration.strategy_policy_sha256)
        for snapshot in matrix.snapshots:
            dated = oi.get(snapshot.contract_id)
            if dated is not None:
                _aware(dated.record.observed_at)
                _aware(dated.recorded_at)
                if (dated.source != OPEN_INTEREST_SOURCE_CHAIN_SNAPSHOT
                    or type(dated.revision_count) is not int or dated.revision_count != 0
                    or not isinstance(dated.revision_id, UUID)
                        or dated.record.underlying != symbol or dated.record.open_interest != snapshot.open_interest
                        or dated.record.observed_session != session or dated.record.batch_id != snapshot.batch_id
                        or type(dated.record.open_interest) is not int or dated.record.open_interest < 0
                        or not dated.record.observed_at <= dated.recorded_at <= selected_at):
                    raise ValueError("detector dated OI identity or availability drift")
            if dated is None:
                reasons.add("DATED_OPEN_INTEREST_UNAVAILABLE")
            if security is None or exchange is None:
                continue
            bound = bind_activity_source(snapshot=snapshot, open_interest_record=dated.record if dated else None,
                lineage=lineage, security=security, recorded_at=max(matrix.ingress.source.completed_at,
                    dated.recorded_at if dated else matrix.ingress.source.completed_at), received_at=selected_at,
                calendar=calendar)
            finding = detect_option_participation(bound, market_cutoff=scope.cutoff, decision_at=selected_at, calendar=calendar)
            if finding.disposition == "UNAVAILABLE":
                reasons.update(finding.reasons)
            elif finding.disposition == "DETECTED":
                activity.append(bound)
        dependencies.append((symbol, tuple(sorted(reasons))))
        rejected.update(reasons)
    regime = {symbol: stock.ema50_slope10_atr for symbol, stock in stocks.items()
        if symbol in ("SPY", "QQQ") and stock is not None and stock.status == "READY"}
    observations = []
    for bound in activity:
        stock = stocks[bound.underlyer]
        direction = 1 if bound.contract_type == "CALL" else -1
        decision = assess_options_canonical_trend(bound, stock, direction=direction,
            market_cutoff=scope.cutoff, decision_at=selected_at)
        if decision.disposition == "UNAVAILABLE":
            dependencies = [(symbol, tuple(sorted(set(reasons) | set(decision.reasons))))
                if symbol == bound.underlyer else (symbol, reasons) for symbol, reasons in dependencies]
        observations.append(build_o1_indicator_observation_v2(bound, decision, stock, scheduled_cycle=scope.cutoff,
            regime=regime, prior_same_direction=None if history is None else (bound.underlyer, direction) in history.directions))
    rejected = Counter(reason for _, reasons in dependencies for reason in reasons)
    inputs = DetectorCycleInputs(matrices=tuple(matrix_sources), o1_observations=tuple(observations),
        rejections=tuple(sorted(rejected.items())))
    dependencies = tuple(dependencies)
    return ForwardDetectorContext(source, selected_at, inputs, dependencies,
        _context_sha256(source, selected_at, inputs, dependencies), tuple(activity), tuple(sorted(stocks.items())))


@dataclass(frozen=True, slots=True)
class OfflineForwardPublication:
    cohort_id: UUID
    provider_tier: ProviderTier
    scope_sha256: str
    dataset_id: str
    payload_json: str

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)

    def read(self, *, cohort_id: UUID, provider_tier: ProviderTier, scope_sha256: str) -> dict:
        if (cohort_id, provider_tier, scope_sha256) != (self.cohort_id, self.provider_tier, self.scope_sha256):
            raise ValueError("stored projection scope mismatch; fallback prohibited")
        return json.loads(self.payload_json)


class OfflineForwardPublicationSlot:
    """In-memory single-cycle fence, not a durable publication repository."""

    def __init__(self, scope: ForwardMatrixScope, owner_epoch: int):
        if type(owner_epoch) is not int or owner_epoch < 1:
            raise ValueError("publication requires a positive owner epoch")
        self.scope = scope
        self.owner_epoch = owner_epoch
        self._retained: OfflineForwardPublication | None = None

    def commit(self, publication: OfflineForwardPublication, *, owner_epoch: int,
               current_scope_sha256: str, checked_at: datetime) -> OfflineForwardPublication:
        _aware(checked_at)
        if (owner_epoch != self.owner_epoch or current_scope_sha256 != self.scope.sha256
                or publication.scope_sha256 != self.scope.sha256
                or publication.cohort_id != self.scope.cohort_id
                or publication.provider_tier is not self.scope.selection.scope.provider_tier):
            raise ValueError("publication lost its scope/owner fence")
        if not self.scope.available_after <= checked_at <= self.scope.deadline:
            raise ValueError("publication missed its deadline fence")
        if self._retained is not None and self._retained != publication:
            raise ValueError("immutable publication conflicts with the sealed cycle")
        self._retained = publication
        return publication


def forward_detector_dataset_id(scope: ForwardMatrixScope, configuration: OptionRuntimeConfiguration) -> str:
    if configuration.configuration_sha256 != scope.configuration_sha256:
        raise ValueError("detector dataset configuration drift")
    return "forward:" + canonical_sha256({
        "version": "forward_policy_chain_dataset_v1" if isinstance(scope, PolicyChainMatrixScope) else FORWARD_DATASET_IDENTITY,
        "matrix_contract": FORWARD_CONTRACT,
        "cohort_id": scope.cohort_id,
        "provider_tier": scope.selection.scope.provider_tier,
        "selection_sha256": scope.selection.selection_sha256,
        "configuration_sha256": scope.configuration_sha256,
        "market_policy_sha256": configuration.policy_sha256,
        "strategy_policy_sha256": configuration.strategy_policy_sha256,
        "valuation_policy_sha256": configuration.valuation_policy.policy_sha256,
        "detector_context_policy_sha256": forward_context_policy_sha256(),
    })


def seal_offline_forward_publication(
    scope: ForwardMatrixScope,
    matrices: tuple[CompleteForwardMatrix, ...],
    configuration: OptionRuntimeConfiguration,
    *,
    selected_at: datetime,
    unavailable_underlyers: tuple[tuple[str, tuple[str, ...]], ...] = (),
    detector_inputs: DetectorCycleInputs | None = None,
    detector_context: ForwardDetectorContext | None = None,
    prior_evaluations,
) -> OfflineForwardPublication:
    _aware(selected_at)
    if not matrices or not scope.available_after <= selected_at <= scope.deadline:
        raise ValueError("publication requires matrices before its deadline")
    if configuration.configuration_sha256 != scope.configuration_sha256:
        raise ValueError("publication configuration drift")
    if tuple(sorted(configuration.settings.underlyers)) != scope.underlyings:
        raise ValueError("detector universe must equal the manifest universe")
    ordered = tuple(sorted(matrices, key=lambda row: row.analysis.underlying.underlyer))
    sources = []
    for matrix in ordered:
        if matrix.ingress.scope != scope or matrix.ingress.source.completed_at > selected_at:
            raise ValueError("publication matrix scope or receipt mismatch")
        if matrix.content_sha256 != canonical_sha256((matrix.ingress.sha256, matrix.snapshots, matrix.analysis)):
            raise ValueError("publication matrix content changed")
        sources.append(dict(underlying=matrix.analysis.underlying.underlyer,
            matrix_id=matrix.matrix_id, market_time=max(row.market_data_time for row in matrix.snapshots),
            observed_time=matrix.ingress.source.completed_at))
    completed = {row["underlying"]: row["matrix_id"] for row in sources}
    missing = tuple(sorted(set(scope.underlyings) - set(completed)))
    if len(completed) != len(ordered) or tuple(name for name, _ in unavailable_underlyers) != missing:
        raise ValueError("publication must freeze exact missing-underlying evidence")
    dataset_id = forward_detector_dataset_id(scope, configuration)
    if detector_context is not None:
        if (detector_inputs is not None or detector_context.selected_at != selected_at
                or detector_context.source.scope_sha256 != scope.sha256
                or detector_context.source.matrix_sha256s != tuple(sorted(row.content_sha256 for row in ordered))
                or detector_context.content_sha256 != _context_sha256(detector_context.source,
                    detector_context.selected_at, detector_context.inputs, detector_context.dependencies)
                or detector_context.inputs.package_inputs or detector_context.inputs.surface_inputs
                or detector_context.inputs.o3_observations or detector_context.inputs.stock_setup_observations):
            raise ValueError("detector context receipt changed or exceeds its capabilities")
        detector_inputs = detector_context.inputs
    if (detector_inputs is not None and detector_inputs.surface_inputs
            and DetectorFamily.O2 in scope.selection.policy.unavailable_detectors):
        raise ValueError("manifest disables O2; tier substitution is prohibited")
    if detector_inputs is None:
        detector_inputs = DetectorCycleInputs(matrices=tuple(sources),
            rejections=(("DETECTOR_CONTEXT_UNAVAILABLE", len(ordered)),))
    if tuple(dict(row) for row in detector_inputs.matrices) != tuple(sources):
        raise ValueError("detector inputs must preserve exact matrix identities and clocks")
    collector = DetectorCycleCollector(lambda **kwargs: detector_inputs, prior_evaluations,
        clock=lambda: selected_at, partial_coverage=True)
    run, records = collector(configuration=configuration, dataset_id=dataset_id,
        scheduled_cycle=scope.cutoff, completed_matrices=completed, started_at=selected_at,
        unavailable_underlyers=unavailable_underlyers)
    if any(record.detector_id not in {family.value for family in scope.selection.policy.required_detectors}
           for record in records):
        raise ValueError("detector evidence exceeds manifest capabilities")
    payload = {
        "schema_version": "offline_forward_projections_v1", "scope_sha256": scope.sha256,
        "provider_tier": scope.selection.scope.provider_tier.value, "source_kind": DEVELOPER_SOURCE,
        "cohort_id": str(scope.cohort_id), "selection_sha256": scope.selection.selection_sha256,
        "reference_sha256": scope.reference_sha256, "action_sha256": scope.action_sha256,
        "cutoff": scope.cutoff.isoformat(), "published_at": selected_at.isoformat(),
        "deadline": scope.deadline.isoformat(), "dataset_id": dataset_id,
        "dataset_identity_version": FORWARD_DATASET_IDENTITY,
        "coverage_status": run.coverage_status, "unavailable_underlyers": unavailable_underlyers,
        "source_sha256s": tuple(matrix.ingress.sha256 for matrix in ordered),
        "flow": tuple({"matrix_id": str(matrix.matrix_id),
            "underlying": asdict(matrix.analysis.underlying),
            "expirations": tuple(asdict(row) for row in matrix.analysis.expirations)} for matrix in ordered),
        "screener": tuple({"matrix_id": str(matrix.matrix_id),
            "snapshots": tuple(asdict(row) for row in matrix.snapshots),
            "contracts": tuple(asdict(row) for row in matrix.analysis.contracts)} for matrix in ordered),
        "detector_context": None if detector_context is None else {
            "contract": FORWARD_CONTEXT_CONTRACT, "status": detector_context.status,
            "content_sha256": detector_context.content_sha256, "policy_sha256": forward_context_policy_sha256(),
            "dependencies": detector_context.dependencies, "capability": "O1_OBSERVATION",
            "package_context": "UNAVAILABLE", "event_calendar": "UNAVAILABLE"},
        "alerts": {"available": False,
            "reason": "PACKAGE_CONTEXT_UNAVAILABLE" if detector_context is not None
                and detector_context.status == "COMPLETE" else "DETECTOR_CONTEXT_UNAVAILABLE",
            "run": run.model_dump(mode="json"), "records": tuple(row.model_dump(mode="json") for row in records)},
        "execution_permission": False, "original_page_contracts": False,
    }
    return OfflineForwardPublication(scope.cohort_id, scope.selection.scope.provider_tier, scope.sha256,
        dataset_id, json.dumps(payload, default=str, sort_keys=True, separators=(",", ":"), allow_nan=False))