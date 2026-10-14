from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta
from typing import ClassVar
from uuid import UUID, NAMESPACE_URL, uuid5

from equity.domain import BarAvailabilityMode, BarSessionScope, BarSourceKind, EquityBarRevision
from equity.materialization import materialize_equity_evidence
from options.alert_qualification import retained_event_horizon
from options.config import OptionRuntimeConfiguration
from options.detector_collection import bind_direct_structural_levels, direct_structural_policy_sha256
from options.domain import OptionContractReference, OptionTradeEvent, TradeClassificationStatus
from options.dual_origin import TECHNICAL_QUALIFICATION_POLICY, assess_options_canonical_trend, qualify_dual_origin_package
from options.event_calendar import OptionEventCalendarBatch
from options.outcome_contracts import assess_option_package
from options.repositories.stock_behavior_assessments import OptionMatrixAssessmentLineage
from options.strategies.domain import StrategyContextSnapshot, StrategyContextStatus
from options.strategies.engine import OptionStrategyEngine

from .contracts import canonical_sha256
from .forward_ingestion import (
    CompleteForwardMatrix, ForwardDetectorContext, ForwardMatrixScope, _aware, _context_sha256,
)
from .options_evidence_identity import (
    OptionsEvidenceIdentity, OptionsEvidenceLineage, carried_identity, options_evidence_admitted,
    require_options_lineage,
)
from .preflight import DetectorFamily, ProviderTier


PACKAGE_CONTEXT_CONTRACT = "forward_o1_indicative_package_v1"
TECHNICAL_PACKAGE_CONTEXT_CONTRACT = "forward_technical_indicative_package_v2"
TRADE_CONTEXT_CONTRACT = "retained_classified_option_trade_context_v1"


@dataclass(frozen=True, slots=True)
class ForwardTradeWindow:
    scope_sha256: str
    matrix_id: UUID
    database_name: str
    provider_tier: ProviderTier
    window_start: datetime
    window_end: datetime
    observed_at: datetime
    recorded_at: datetime
    requested_contract_ids: tuple[int, ...]
    events: tuple[OptionTradeEvent, ...]
    coverage: str = "PARTIAL"
    _LINEAGE: ClassVar[OptionsEvidenceLineage] = OptionsEvidenceLineage.V1_FORWARD

    def __post_init__(self):
        for clock in (self.window_start, self.window_end, self.observed_at, self.recorded_at):
            _aware(clock)
        if (not self.window_start < self.window_end <= self.observed_at <= self.recorded_at
                or self.window_end - self.window_start > timedelta(hours=8)
                or not options_evidence_admitted(self._LINEAGE, database_name=self.database_name,
                    identity=carried_identity(self))
                or self.provider_tier is not ProviderTier.MASSIVE_DEVELOPER
                or self.coverage not in {"COMPLETE", "PARTIAL"} or len(self.events) > 5000
                or not self.requested_contract_ids or len(set(self.requested_contract_ids)) != len(self.requested_contract_ids)
                or any(type(identity) is not int or identity <= 0 for identity in self.requested_contract_ids)):
            raise ValueError("retained trade window target, coverage or bounds are invalid")
        object.__setattr__(self, "requested_contract_ids", tuple(sorted(self.requested_contract_ids)))


@dataclass(frozen=True, slots=True)
class ForwardTradeWindowV2(ForwardTradeWindow):
    """A retained trade window under the V2 lineage. Its trade pages (raw response bytes) have no V2 form:
    the V2 input references stored evidence by id and hash."""

    identity: OptionsEvidenceIdentity = field(kw_only=True)
    _LINEAGE: ClassVar[OptionsEvidenceLineage] = OptionsEvidenceLineage.ADVANCED_REALTIME_V2


@dataclass(frozen=True, slots=True)
class ForwardTradePage:
    contract_id: int
    request_path: str
    request_filter_sha256: str
    response_body: bytes
    received_at: datetime
    next_cursor_sha256: str | None


@dataclass(frozen=True, slots=True)
class ForwardTradeWindowWithPages(ForwardTradeWindow):
    pages: tuple[ForwardTradePage, ...] = ()
    collection_policy_sha256: str = ""

    def __post_init__(self):
        ForwardTradeWindow.__post_init__(self)
        if (len(self.collection_policy_sha256) != 64 or not self.pages
                or any(page.contract_id not in self.requested_contract_ids or page.received_at > self.recorded_at
                    or not page.request_path.startswith("/v3/trades/") or not isinstance(page.response_body, bytes)
                    for page in self.pages)):
            raise ValueError("retained trade raw page receipt or policy is invalid")


def retained_trade_context(scope, matrix, receipt: ForwardTradeWindow | None, *, selected_at,
                           lineage=OptionsEvidenceLineage.V1_FORWARD):
    from collections import Counter
    from options.data.trade_classification import TRADE_SEMANTICS_VERSION, classify_option_trade

    require_options_lineage(lineage)
    missing = dict(contract=TRADE_CONTEXT_CONTRACT, role="ADVISORY", status="UNAVAILABLE",
        reason="TARGET_LOCAL_TRADE_WINDOW_NOT_RETAINED", source="TARGET_LOCAL_RETAINED_EVENTS_ONLY",
        volume=None, transacted_premium=None, aggressor_side=None, chain_volume_used_as_tape=False)
    if receipt is None:
        return missing
    if (not isinstance(receipt, ForwardTradeWindow) or receipt.scope_sha256 != scope.sha256
            or not options_evidence_admitted(lineage, database_name=receipt.database_name, identity=carried_identity(receipt))
            or receipt.matrix_id != matrix.matrix_id or receipt.provider_tier is not scope.selection.scope.provider_tier
            or receipt.window_end != scope.cutoff or receipt.recorded_at > selected_at):
        raise ValueError("retained trade window scope, matrix or receipt drift")
    catalog = {row.contract_id: row for row in scope.catalog if row.underlyer == matrix.analysis.underlying.underlyer}
    if not set(receipt.requested_contract_ids) <= set(catalog):
        raise ValueError("retained trade window exceeds exact matrix catalogue")
    unique, counts = {}, Counter()
    for event in receipt.events:
        reference = catalog.get(event.contract_id)
        if (type(event) is not OptionTradeEvent or reference is None or event.contract_id not in receipt.requested_contract_ids
                or event.provider != reference.provider
                or event.contract_ticker != reference.contract_ticker or event.underlyer != reference.underlyer
                or event.shares_per_contract != reference.shares_per_contract
            or reference.valid_from > event.sip_timestamp
            or reference.valid_to is not None and reference.valid_to <= event.sip_timestamp
            or max(reference.first_observed_at, reference.revised_observed_at or reference.first_observed_at) > receipt.observed_at
                or not receipt.window_start <= event.sip_timestamp <= receipt.window_end
                or not event.first_observed_at <= receipt.observed_at
                or event.revised_observed_at is not None and event.revised_observed_at > receipt.observed_at):
            raise ValueError("retained trade event identity or causal clocks drifted")
        classified = classify_option_trade(event.conditions, event.correction)
        if (event.semantics_version != TRADE_SEMANTICS_VERSION or event.classification_status != classified.status
                or event.classification_reasons != classified.reasons):
            raise ValueError("retained trade classification differs from native semantics")
        if event.event_key in unique:
            if unique[event.event_key].notional != event.notional:
                raise ValueError("retained duplicate trade economics conflict")
            continue
        unique[event.event_key] = event
        counts[classified.status.value] += 1
    included = tuple(event for event in unique.values() if event.classification_status is TradeClassificationStatus.INCLUDED)
    from decimal import Decimal

    classification_complete = counts[TradeClassificationStatus.UNKNOWN.value] == 0
    return dict(missing, status="AVAILABLE" if receipt.coverage == "COMPLETE" and classification_complete else "PARTIAL",
        reason=None if classification_complete else "TRADE_CLASSIFICATION_PARTIAL",
        coverage=receipt.coverage, scope="DECLARED_RETAINED_CONTRACTS_NOT_TOTAL_MARKET", window_start=receipt.window_start,
        window_end=receipt.window_end, observed_at=receipt.observed_at, recorded_at=receipt.recorded_at,
        requested_contract_ids=receipt.requested_contract_ids, unique_event_count=len(unique),
        classification_counts=dict(counts), semantics_version=TRADE_SEMANTICS_VERSION,
        volume=sum(event.size for event in included), transacted_premium=sum((event.notional for event in included), Decimal("0")),
        event_sha256=canonical_sha256(tuple(sorted(((event.trade_event_id, event.payload_sha256)
            for event in unique.values()), key=lambda row: str(row[0])))),
        limitations=["ELIGIBLE_RETAINED_PRINTS_NOT_NET_FLOW", "NO_AGGRESSOR_OR_POSITION_INFERENCE"])


@dataclass(frozen=True, slots=True)
class ForwardPackageReference:
    revision_id: UUID
    contract_id: int
    reference: OptionContractReference


@dataclass(frozen=True, slots=True)
class ForwardPackageCalendar:
    batch: OptionEventCalendarBatch
    recorded_at: datetime

    def __post_init__(self):
        object.__setattr__(self, "batch", replace(self.batch,
            coverage=tuple(sorted(self.batch.coverage, key=lambda row: str(row.coverage_id))),
            events=tuple(sorted(self.batch.events, key=lambda row: str(row.market_event_id)))))


@dataclass(frozen=True, slots=True)
class ForwardO1PackageSource:
    scope_sha256: str
    matrix_sha256: str
    detector_context_sha256: str
    database_name: str
    provider_tier: ProviderTier
    references: tuple[ForwardPackageReference, ...] = ()
    raw_spot_bars: tuple[tuple[EquityBarRevision, datetime], ...] = ()
    structural_bars: tuple[tuple[EquityBarRevision, datetime], ...] = ()
    structural_policy_sha256: str | None = None
    calendars: tuple[ForwardPackageCalendar, ...] = ()
    planned_entry_at: datetime | None = None
    entry_deadline: datetime | None = None
    exit_deadline: datetime | None = None

    def __post_init__(self):
        object.__setattr__(self, "references", tuple(sorted(self.references, key=lambda row: row.contract_id)))
        object.__setattr__(self, "raw_spot_bars", tuple(sorted(self.raw_spot_bars, key=lambda row: (row[0].bar_end, str(row[0].bar_revision_id)))))
        object.__setattr__(self, "structural_bars", tuple(sorted(self.structural_bars, key=lambda row: row[0].bar_end)))
        object.__setattr__(self, "calendars", tuple(sorted(self.calendars, key=lambda row: canonical_sha256(row))))


@dataclass(frozen=True, slots=True)
class ForwardO1PackageSourceV2(ForwardO1PackageSource):
    """Package receipts under the V2 lineage. The detector context they bind to must carry the V2 identity too."""

    identity: OptionsEvidenceIdentity = field(kw_only=True)


_PACKAGE_SOURCE_TYPES = {
    OptionsEvidenceLineage.V1_FORWARD: ForwardO1PackageSource,
    OptionsEvidenceLineage.ADVANCED_REALTIME_V2: ForwardO1PackageSourceV2,
}


@dataclass(frozen=True, slots=True)
class OfflineO1PackageContext:
    scope_sha256: str
    matrix_id: UUID
    payload_json: str
    input_sha256: str

    def read(self, *, scope_sha256: str, matrix_id: UUID) -> dict:
        if (scope_sha256, matrix_id) != (self.scope_sha256, self.matrix_id):
            raise ValueError("package scope mismatch; fallback prohibited")
        return json.loads(self.payload_json)


def _calendar_detail(source, symbol, selected_at):
    coverage, events = [], []
    keys = set()
    for receipt in source.calendars:
        _aware(receipt.recorded_at)
        batch = receipt.batch
        _aware(batch.observed_at)
        if not batch.observed_at <= receipt.recorded_at <= selected_at:
            raise ValueError("package calendar availability drift")
        for rows, output, identity in ((batch.coverage, coverage, "coverage_id"), (batch.events, events, "market_event_id")):
            for row in rows:
                key = (identity, getattr(row, identity))
                if (key in keys or row.source != batch.source or row.affected_underlying not in (None, symbol)
                        or not row.source_observed_at <= row.first_observed_at <= batch.observed_at):
                    raise ValueError("package calendar source, identity or receipt drift")
                keys.add(key)
                output.append(asdict(row))
    return dict(holding_event_evidence=events, holding_event_coverage=coverage, event_coverage_evidence=coverage)


def load_forward_o1_package_context(
    scope: ForwardMatrixScope, matrix: CompleteForwardMatrix, configuration: OptionRuntimeConfiguration,
    detector_context: ForwardDetectorContext, source: ForwardO1PackageSource, *, selected_at: datetime,
    lineage: OptionsEvidenceLineage = OptionsEvidenceLineage.V1_FORWARD,
) -> OfflineO1PackageContext:
    _aware(selected_at)
    if type(source) is not _PACKAGE_SOURCE_TYPES[require_options_lineage(lineage)]:
        raise TypeError("O1 package inputs require explicit target-local receipts")
    if (not options_evidence_admitted(lineage, database_name=source.database_name, identity=carried_identity(source))
            or not options_evidence_admitted(lineage, database_name=detector_context.source.database_name,
                identity=carried_identity(detector_context.source))
            or source.provider_tier is not ProviderTier.MASSIVE_DEVELOPER
            or source.provider_tier is not scope.selection.scope.provider_tier or source.scope_sha256 != scope.sha256
            or source.matrix_sha256 != matrix.content_sha256 or matrix.ingress.scope != scope
            or matrix.content_sha256 not in detector_context.source.matrix_sha256s
            or matrix.ingress.source.completed_at > selected_at or matrix.analysis.chain_health.status != "COMPLETE"
            or matrix.content_sha256 != canonical_sha256((matrix.ingress.sha256, matrix.snapshots, matrix.analysis))
            or configuration.configuration_sha256 != scope.configuration_sha256
            or detector_context.source.scope_sha256 != scope.sha256 or detector_context.selected_at != selected_at
            or source.detector_context_sha256 != detector_context.content_sha256
            or detector_context.content_sha256 != _context_sha256(detector_context.source, selected_at,
                detector_context.inputs, detector_context.dependencies)
            or not scope.available_after <= selected_at <= scope.deadline):
        raise ValueError("O1 package scope/tier/configuration/context/deadline drift")
    if DetectorFamily.O1 not in scope.selection.policy.required_detectors:
        raise ValueError("O1 package source exceeds manifest capability")
    if (len(source.references) > 1000 or len(source.raw_spot_bars) > 1000
            or len(source.structural_bars) > 1000 or len(source.calendars) > 16):
        raise ValueError("O1 package source budget exceeded")
    symbol = matrix.analysis.underlying.underlyer
    security = next((row for row in detector_context.source.securities if row.ticker == symbol), None)
    stock = dict(detector_context.stock_sources).get(symbol)
    activities = {row.snapshot_id: row for row in detector_context.activity_sources if row.underlyer == symbol}
    observations = {row.snapshot_id: row for row in detector_context.inputs.o1_observations if row.underlyer == symbol}
    reasons = set(dict(detector_context.dependencies).get(symbol, ("O1_CONTEXT_UNAVAILABLE",)))
    if security is None or stock is None:
        reasons.add("O1_STOCK_OR_SECURITY_UNAVAILABLE")
    if len(activities) != sum(row.underlyer == symbol for row in detector_context.activity_sources):
        raise ValueError("O1 bound activity sources are ambiguous")
    for identity, activity in activities.items():
        observation = observations.get(identity)
        if (observation is None or activity.sha256 != observation.activity_source_sha256
                or stock is None or stock.sha256 != observation.stock_source_sha256
                or activity.matrix_id != matrix.matrix_id):
            raise ValueError("O1 package native sources changed from frozen observations")
    references = {row.contract_id: row for row in source.references}
    if len(references) != len(source.references):
        raise ValueError("O1 package reference revisions are ambiguous")
    catalogs = {row.contract_id: row for row in scope.catalog if row.underlyer == symbol}
    for row in source.references:
        catalog = catalogs.get(row.contract_id)
        reference = row.reference
        if (catalog is None or not isinstance(row.revision_id, UUID)
                or (reference.contract_ticker, reference.underlyer, reference.asset_type, reference.provider, reference.provider_version,
                    reference.expiration_date, reference.strike, reference.shares_per_contract,
                    reference.primary_exchange,
                    reference.valid_from, reference.valid_to, reference.first_observed_at,
                    reference.revised_observed_at, reference.payload_sha256) !=
                (catalog.contract_ticker, catalog.underlyer, catalog.asset_type, catalog.provider, catalog.provider_version,
                    catalog.expiration_date, catalog.strike, catalog.shares_per_contract,
                    catalog.primary_exchange,
                    catalog.valid_from, catalog.valid_to, catalog.first_observed_at,
                    catalog.revised_observed_at, catalog.payload_sha256)):
            raise ValueError("O1 package exact dated reference drift")
    source_bars = {row.bar_revision_id: (row, recorded) for row, recorded in detector_context.source.bars}
    structural = source.structural_bars
    if any(source_bars.get(row.bar_revision_id) != (row, recorded) or row.ticker != symbol
            or row.interval != "30m" for row, recorded in structural):
        raise ValueError("O1 package structural bars are outside the frozen raw source")
    for bar, recorded in source.raw_spot_bars:
        _aware(recorded)
        if (bar.ticker != symbol or security is None or bar.security_id != security.security_id
                or bar.interval != "1m" or bar.adjusted or not bar.is_final or bar.quality_codes
                or bar.source_kind is not BarSourceKind.NATIVE_REST or bar.session_scope is not BarSessionScope.RTH
                or bar.availability_mode is not BarAvailabilityMode.LIVE_OBSERVED
                or not bar.system_observed_at <= recorded <= selected_at):
            raise ValueError("O1 package raw-spot identity or receipt drift")
    if len({bar.bar_revision_id for bar, _ in source.raw_spot_bars}) != len(source.raw_spot_bars):
        raise ValueError("O1 raw-spot revisions are ambiguous")
    if len({bar.bar_end for bar, _ in structural}) != len(structural):
        raise ValueError("O1 structural source windows are ambiguous")
    event_detail = _calendar_detail(source, symbol, selected_at)
    if not source.calendars:
        reasons.add("HOLDING_EVENT_CALENDAR_UNAVAILABLE")
    if not structural or source.structural_policy_sha256 is None:
        reasons.add("STRUCTURAL_LEVELS_UNAVAILABLE")
    elif source.structural_policy_sha256 != direct_structural_policy_sha256():
        raise ValueError("O1 package structural source policy drift")
    if not source.raw_spot_bars:
        reasons.add("EXACT_RAW_SPOT_UNAVAILABLE")
    if not references:
        reasons.add("EXACT_PACKAGE_REFERENCES_UNAVAILABLE")
    clocks = (source.planned_entry_at, source.entry_deadline, source.exit_deadline)
    if any(clock is None for clock in clocks):
        reasons.add("PACKAGE_HOLDING_CLOCKS_UNAVAILABLE")
    else:
        for clock in clocks:
            _aware(clock)
        if not selected_at < source.planned_entry_at <= source.entry_deadline < source.exit_deadline:
            raise ValueError("O1 package entry/holding clocks are not causal")
    native_context = StrategyContextSnapshot(uuid5(NAMESPACE_URL, f"{PACKAGE_CONTEXT_CONTRACT}:{matrix.matrix_id}"),
        matrix.matrix_id, symbol, matrix.analysis.context.market_time, matrix.analysis.context.observed_time,
        StrategyContextStatus.DEGRADED, None, None, 0, None, None, 0, None,
        "UNAVAILABLE", "UNAVAILABLE", "UNAVAILABLE", ("PACKAGE_CONTEXT_UNAVAILABLE",), (),
        configuration.policy.policy_version, configuration.policy_sha256)
    scan = OptionStrategyEngine(configuration.strategy_policy, configuration.strategy_policy_sha256).scan(
        matrix.matrix_id, matrix.snapshots, matrix.analysis.chain_health, matrix.analysis.expirations, native_context)
    candidates = tuple(row for row in scan.candidates if row.strategy_name == "DIRECTIONAL_LONG_PREMIUM" and row.legs)
    qualified, rejected = [], []
    if not candidates:
        reasons.add("NATIVE_O1_PACKAGE_UNAVAILABLE")
    levels = None
    if not reasons:
        bars = tuple(row for row, _ in sorted(structural, key=lambda value: value[0].bar_end))
        if not bars or bars[-1].bar_end != stock.required_window or bars[-1].bar_revision_id != stock.trend_last_bar_revision_id:
            raise ValueError("O1 structural window differs from exact native stock confirmation")
        if any(snapshot.spot_market_data_time == bars[-1].bar_end and snapshot.spot != bars[-1].close_price for snapshot in matrix.snapshots):
            raise ValueError("O1 exact raw frame/spot basis mismatch")
        materialized = materialize_equity_evidence(analysis_run_id=uuid5(matrix.matrix_id, "o1-structural"),
            security=security, interval="30m", bars=bars, observed_at=selected_at, level_directions=(1, -1))
        levels = dict(interval="30m", market_time=bars[-1].bar_end, computed_at=selected_at,
            bar_revision_ids=tuple(row.bar_revision_id for row in bars), levels=materialized.directional_levels or {})
    by_snapshot = {row.snapshot_id: row for row in matrix.snapshots}
    assessment_lineage = OptionMatrixAssessmentLineage(matrix.matrix_id, symbol, matrix.analysis.context.market_time,
        matrix.analysis.context.observed_time, scope.cutoff, configuration.configuration_sha256,
        configuration.policy_sha256, configuration.strategy_policy_sha256)
    for candidate in candidates:
        if reasons:
            break
        activity = activities.get(candidate.legs[0].snapshot_id)
        if activity is None:
            rejected.append((str(candidate.candidate_id), "DATED_ACTIVITY_SOURCE_UNAVAILABLE"))
            continue
        direction = 1 if candidate.legs[0].contract_type.value == "CALL" else -1
        decision = assess_options_canonical_trend(activity, stock, direction=direction,
            market_cutoff=scope.cutoff, decision_at=selected_at)
        if decision.disposition != "CONFIRMED":
            rejected.append((str(candidate.candidate_id), "NATIVE_O1_STOCK_NOT_CONFIRMED"))
            continue
        snapshots = tuple(by_snapshot[leg.snapshot_id] for leg in candidate.legs)
        if any(row.contract_id not in references for row in snapshots):
            reasons.add("EXACT_PACKAGE_REFERENCES_UNAVAILABLE")
            continue
        matching = [tuple((bar, recorded) for bar, recorded in source.raw_spot_bars
            if bar.bar_end == snapshot.spot_market_data_time and bar.close_price == snapshot.spot) for snapshot in snapshots]
        if any(len(values) != 1 for values in matching):
            reasons.add("EXACT_RAW_SPOT_UNAVAILABLE")
            continue
        event_input = dict(event_detail, candidate=dict(underlying=symbol, observed_time=selected_at),
            source_snapshots=[dict(asdict(row), underlying=row.underlyer) for row in snapshots])
        horizon = retained_event_horizon(event_input, selected_at, source.exit_deadline)
        if any(check["status"] not in ("CLEAR", "NOT_APPLICABLE") for check in horizon["checks"]):
            reasons.add("SUPPORTED_HOLDING_EVENT_EVIDENCE_UNAVAILABLE_OR_BLOCKED")
            continue
        try:
            technical = bind_direct_structural_levels(levels, security=security, direction=direction,
                spot=snapshots[0].spot, received_at=selected_at, valid_until=decision.valid_until)
            package, plan = qualify_dual_origin_package(decision=decision, source=activity, candidate=candidate,
                package_assessment=assess_option_package(candidate, valuation_policy_sha256=configuration.valuation_policy_sha256),
                stock=stock, security=security, lineage=assessment_lineage, snapshots=snapshots,
                references=tuple(references[row.contract_id].reference for row in snapshots),
                raw_bars=tuple(values[0][0] for values in matching), raw_bar_created_ats=tuple(values[0][1] for values in matching),
                source_received_at=selected_at, planned_entry_at=source.planned_entry_at, entry_deadline=source.entry_deadline,
                exit_deadline=source.exit_deadline, entry_limit=-candidate.net_premium,
                valuation_policy=configuration.valuation_policy, event_detail=event_detail,
                technical_evidence=technical, technical_source_policy_sha256=source.structural_policy_sha256)
        except ValueError as error:
            rejected.append((str(candidate.candidate_id), str(error)))
        else:
            qualified.append(dict(candidate_id=str(candidate.candidate_id), candidate=asdict(candidate),
                package=package.model_dump(mode="json"), plan_payload_json=plan.payload_json, plan_sha256=plan.sha256,
                source_snapshots=tuple(asdict(snapshot) for snapshot in snapshots)))
    if qualified:
        reasons.add("COMPLETE_OPTIONS_EVENT_COVERAGE_UNAVAILABLE")
    payload = dict(contract=PACKAGE_CONTEXT_CONTRACT, available=False,
        display_available=bool(qualified), complete_context_available=False,
        status="PARTIAL" if qualified else "UNAVAILABLE", reasons=tuple(sorted(reasons)),
        native_candidate_count=len(candidates), qualified_indicative=qualified, rejected=tuple(rejected),
        scope_sha256=scope.sha256, matrix_id=str(matrix.matrix_id), selected_at=selected_at.isoformat(),
        detector_context_sha256=detector_context.content_sha256, execution_permission=False,
        strategy_policy_sha256=configuration.strategy_policy_sha256,
        valuation_policy_sha256=configuration.valuation_policy_sha256,
        qualification_policy_sha256=TECHNICAL_QUALIFICATION_POLICY.sha256,
        publication_permission=False, original_page_contracts=False)
    serialized = json.dumps(payload, default=lambda value: value.isoformat() if isinstance(value, datetime) else str(value),
        sort_keys=True, separators=(",", ":"), allow_nan=False)
    input_sha256 = canonical_sha256(dict(contract=PACKAGE_CONTEXT_CONTRACT, source=source,
        scope=scope.sha256, matrix=matrix.content_sha256, detector_context=detector_context.content_sha256,
        strategy_policy_sha256=configuration.strategy_policy_sha256,
        valuation_policy_sha256=configuration.valuation_policy_sha256,
        qualification_policy_sha256=TECHNICAL_QUALIFICATION_POLICY.sha256))
    return OfflineO1PackageContext(scope.sha256, matrix.matrix_id, serialized, input_sha256)


def load_forward_technical_package_context(
    scope: ForwardMatrixScope, matrix: CompleteForwardMatrix, configuration: OptionRuntimeConfiguration,
    detector_context: ForwardDetectorContext, source: ForwardO1PackageSource, *, selected_at: datetime,
    trade_window: ForwardTradeWindow | None = None,
    lineage: OptionsEvidenceLineage = OptionsEvidenceLineage.V1_FORWARD,
) -> OfflineO1PackageContext:
    from decimal import Decimal
    from options.alert_plans import TechnicalExitEvidence, TechnicalLevel
    from options.calendar import OptionExchangeCalendar
    from options.credit_detection import O3_CREDIT_POLICY_SHA256, assess_o3_credit
    from options.dual_origin import assess_canonical_stock_first, assess_options_credit_canonical_trend
    from options.stock_bar_detection import STOCK_BAR_SIGNAL_POLICY_SHA256, detect_canonical_stock_signals

    require_options_lineage(lineage)
    enabled = tuple(sorted(family.value for family in scope.selection.policy.required_detectors))
    if not enabled or set(enabled) - {"O1", "O3", "S1", "S2"}:
        raise ValueError("Developer technical packages cannot enable unsupported detectors, including O2")
    original = load_forward_o1_package_context(scope, matrix, configuration, detector_context, source,
        selected_at=selected_at, lineage=lineage)
    payload = original.read(scope_sha256=scope.sha256, matrix_id=matrix.matrix_id)
    symbol = matrix.analysis.underlying.underlyer
    security = next((row for row in detector_context.source.securities if row.ticker == symbol), None)
    stock = dict(detector_context.stock_sources).get(symbol)
    dependencies = {family: [] for family in enabled}
    if "O1" in dependencies:
        dependencies["O1"] = [reason for reason in payload["reasons"]
            if reason not in {"COMPLETE_OPTIONS_EVENT_COVERAGE_UNAVAILABLE", "NATIVE_O1_PACKAGE_UNAVAILABLE"}]
    common = []
    for missing, reason in ((security is None, "STOCK_SECURITY_IDENTITY_UNAVAILABLE"),
            (not source.references, "EXACT_PACKAGE_REFERENCES_UNAVAILABLE")):
        if missing:
            common.append(reason)
    debit_missing = []
    for missing, reason in ((not source.raw_spot_bars, "EXACT_RAW_SPOT_UNAVAILABLE"),
            (not source.calendars, "HOLDING_EVENT_CALENDAR_UNAVAILABLE"),
            (any(clock is None for clock in (source.planned_entry_at, source.entry_deadline, source.exit_deadline)),
                "PACKAGE_HOLDING_CLOCKS_UNAVAILABLE")):
        if missing:
            debit_missing.append(reason)
    for family in {"O1", "S1", "S2"} & set(enabled):
        dependencies[family].extend(debit_missing)
    qualified = list(payload["qualified_indicative"])
    credits, credit_candidates, rejected = [], [], list(payload["rejected"])
    if not common:
        native_context = StrategyContextSnapshot(uuid5(NAMESPACE_URL, f"{PACKAGE_CONTEXT_CONTRACT}:{matrix.matrix_id}"),
            matrix.matrix_id, symbol, matrix.analysis.context.market_time, matrix.analysis.context.observed_time,
            StrategyContextStatus.DEGRADED, None, None, 0, None, None, 0, None,
            "UNAVAILABLE", "UNAVAILABLE", "UNAVAILABLE", ("PACKAGE_CONTEXT_UNAVAILABLE",), (),
            configuration.policy.policy_version, configuration.policy_sha256)
        scan = OptionStrategyEngine(configuration.strategy_policy, configuration.strategy_policy_sha256,
            configuration.gamma_policy, configuration.gamma_policy_sha256).scan(
            matrix.matrix_id, matrix.snapshots, matrix.analysis.chain_health, matrix.analysis.expirations, native_context,
            gamma_profiles=matrix.analysis.gamma_profiles)
        references = {row.contract_id: row.reference for row in source.references}
        snapshots_by_id = {row.snapshot_id: row for row in matrix.snapshots}
        activities = {row.snapshot_id: row for row in detector_context.activity_sources if row.underlyer == symbol}
        dated_oi = {row.record.contract_id for row in detector_context.source.open_interest}
        event_detail = _calendar_detail(source, symbol, selected_at)
        assessment_lineage = OptionMatrixAssessmentLineage(matrix.matrix_id, symbol, matrix.analysis.context.market_time,
            matrix.analysis.context.observed_time, scope.cutoff, configuration.configuration_sha256,
            configuration.policy_sha256, configuration.strategy_policy_sha256)
        signals = ()
        if {"S1", "S2"} & set(enabled):
            bars = tuple((bar, recorded) for bar, recorded in detector_context.source.bars if bar.ticker == symbol)
            coverage = next((receipt for receipt, _ in detector_context.source.split_coverage if receipt.ticker == symbol), None)
            daily = tuple(bar for bar, _ in bars if bar.interval == "1d")
            if (coverage is None or not daily or coverage.response_action_count
                    or coverage.window_start > min(bar.session_date for bar in daily)
                    or coverage.window_end < scope.selection.session_date
                    or selected_at - coverage.first_observed_at > timedelta(hours=24)):
                for family in {"S1", "S2"} & set(enabled):
                    dependencies[family].append("CANONICAL_STOCK_SPLIT_COVERAGE_UNAVAILABLE")
            else:
                try:
                    signals = detect_canonical_stock_signals(security_id=security.security_id, underlyer=symbol,
                        bars=bars, market_cutoff=scope.cutoff, decision_at=selected_at,
                        session_close=OptionExchangeCalendar().session_close(scope.selection.session_date))
                except ValueError:
                    for family in {"S1", "S2"} & set(enabled):
                        dependencies[family].append("CANONICAL_STOCK_SIGNAL_SOURCE_UNAVAILABLE")
        levels = None
        if "O3" in enabled:
            if configuration.strategy_policy.credit is None:
                dependencies["O3"].append("O3_CREDIT_POLICY_UNAVAILABLE")
            elif stock is None or stock.status != "READY":
                dependencies["O3"].append("O3_STOCK_CONFIRMATION_UNAVAILABLE")
            elif not source.structural_bars or source.structural_policy_sha256 is None:
                dependencies["O3"].append("O3_STRUCTURAL_INVALIDATION_UNAVAILABLE")
            else:
                bars = tuple(bar for bar, _ in source.structural_bars)
                if bars[-1].bar_end != stock.required_window or bars[-1].bar_revision_id != stock.trend_last_bar_revision_id:
                    raise ValueError("technical credit structure differs from exact stock confirmation")
                materialized = materialize_equity_evidence(analysis_run_id=uuid5(matrix.matrix_id, "o3-structural"),
                    security=security, interval="30m", bars=bars, observed_at=selected_at, level_directions=(1, -1))
                levels = dict(interval="30m", market_time=bars[-1].bar_end, computed_at=selected_at,
                    bar_revision_ids=tuple(bar.bar_revision_id for bar in bars), levels=materialized.directional_levels or {})
        for candidate in scan.candidates:
            if not candidate.legs:
                continue
            snapshots = tuple(snapshots_by_id[leg.snapshot_id] for leg in candidate.legs)
            if any(snapshot.contract_id not in references for snapshot in snapshots):
                continue
            if ("O3" in enabled and not dependencies["O3"] and levels is not None
                    and candidate.structure_type.value in {"PUT_CREDIT_VERTICAL", "CALL_CREDIT_VERTICAL"}):
                triggers = tuple(activities[snapshot.snapshot_id] for snapshot in snapshots if snapshot.snapshot_id in activities)
                if not triggers or not {snapshot.contract_id for snapshot in snapshots} <= dated_oi:
                    rejected.append((str(candidate.candidate_id), "O3_DATED_ACTIVITY_SOURCE_UNAVAILABLE"))
                    continue
                activity = max(triggers, key=lambda row: (Decimal(row.day_volume) / Decimal(row.open_interest), -row.contract_id))
                direction = 1 if candidate.structure_type.value == "PUT_CREDIT_VERTICAL" else -1
                decision = assess_options_credit_canonical_trend(activity, stock, direction=direction,
                    market_cutoff=scope.cutoff, decision_at=selected_at)
                if decision.disposition != "CONFIRMED":
                    continue
                try:
                    technical = bind_direct_structural_levels(levels, security=security, direction=direction,
                        spot=snapshots[0].spot, received_at=selected_at, valid_until=decision.valid_until)
                    invalidation = next(level.price for level in technical.levels if level.role == "INVALIDATION")
                    observation = assess_o3_credit(candidate, decision, activity_contract_id=activity.contract_id,
                        snapshots=snapshots, structural_invalidation=invalidation, scheduled_cycle=scope.cutoff)
                except ValueError as error:
                    rejected.append((str(candidate.candidate_id), str(error)))
                else:
                    credits.append(observation)
                    credit_candidates.append(asdict(candidate))
            if candidate.strategy_name != "DIRECTIONAL_LONG_PREMIUM":
                continue
            activity = activities.get(candidate.legs[0].snapshot_id)
            if activity is None:
                continue
            matching = [tuple((bar, recorded) for bar, recorded in source.raw_spot_bars
                if bar.bar_end == snapshot.spot_market_data_time and bar.close_price == snapshot.spot) for snapshot in snapshots]
            for signal in signals:
                if signal.detector_id not in enabled or dependencies[signal.detector_id]:
                    continue
                if any(len(values) != 1 for values in matching):
                    dependencies[signal.detector_id].append("EXACT_RAW_SPOT_UNAVAILABLE")
                    continue
                decision = assess_canonical_stock_first(signal, activity, market_cutoff=scope.cutoff, decision_at=selected_at)
                if decision.disposition != "CONFIRMED":
                    continue
                technical = TechnicalExitEvidence(security_id=security.security_id, underlyer=symbol,
                    direction=signal.direction, source_kind="CANONICAL_STOCK_SIGNAL", source_policy_sha256=signal.policy_sha256,
                    source_payload_sha256=signal.sha256, source_revision_ids=tuple(bar.bar_revision_id for bar in signal.bars),
                    interval=signal.interval, market_time=signal.market_time, available_at=signal.available_at,
                    received_at=selected_at, valid_until=signal.valid_until, atr=signal.prior_atr14,
                    levels=(TechnicalLevel(level_id="prior-close", role="INVALIDATION", price=signal.stop),
                        TechnicalLevel(level_id="symmetric-target", role="OPPOSING_STRUCTURE", price=signal.target)))
                try:
                    package, plan = qualify_dual_origin_package(decision=decision, source=activity, candidate=candidate,
                        package_assessment=assess_option_package(candidate, valuation_policy_sha256=configuration.valuation_policy_sha256),
                        stock=stock, security=security, lineage=assessment_lineage, snapshots=snapshots,
                        references=tuple(references[snapshot.contract_id] for snapshot in snapshots),
                        raw_bars=tuple(values[0][0] for values in matching), raw_bar_created_ats=tuple(values[0][1] for values in matching),
                        source_received_at=selected_at, planned_entry_at=source.planned_entry_at,
                        entry_deadline=source.entry_deadline, exit_deadline=source.exit_deadline, entry_limit=-candidate.net_premium,
                        valuation_policy=configuration.valuation_policy, event_detail=event_detail,
                        canonical_stock_signal=signal, technical_evidence=technical,
                        technical_source_policy_sha256=signal.policy_sha256)
                except ValueError as error:
                    rejected.append((str(candidate.candidate_id), str(error)))
                else:
                    qualified.append(dict(candidate_id=str(candidate.candidate_id), candidate=asdict(candidate),
                        package=package.model_dump(mode="json"), plan_payload_json=plan.payload_json, plan_sha256=plan.sha256,
                        source_snapshots=tuple(asdict(snapshot) for snapshot in snapshots)))
    for family in enabled:
        dependencies[family] = sorted(set(dependencies[family] + common))
    payload.update(contract=TECHNICAL_PACKAGE_CONTEXT_CONTRACT, enabled_detectors=enabled,
        valuation_assumption=("EXPLICIT_ZERO_YIELD_RESEARCH" if configuration.valuation_policy.policy_version ==
            "option_valuation_zero_yield_research_v1" else "POLICY_BOUND_SOURCE_INPUTS"),
        detector_dependencies=dependencies, qualified_indicative=qualified,
        o3_observations=tuple(row.model_dump(mode="json") for row in credits), o3_candidates=credit_candidates, rejected=rejected,
        gamma_context=dict(role="ADVISORY", policy_sha256=configuration.gamma_policy_sha256,
            coverage="RETAINED_MATRIX_NOT_TOTAL_MARKET", profiles=tuple(asdict(row) for row in matrix.analysis.gamma_profiles)),
        trade_context=retained_trade_context(scope, matrix, trade_window, selected_at=selected_at, lineage=lineage),
        display_available=bool(qualified or credits), status="PARTIAL" if qualified or credits else "UNAVAILABLE")
    serialized = json.dumps(payload, default=lambda value: value.isoformat() if isinstance(value, datetime) else str(value),
        sort_keys=True, separators=(",", ":"), allow_nan=False)
    if trade_window is None:
        trade_digest = None
    elif lineage is OptionsEvidenceLineage.V1_FORWARD:
        from .forward_storage import encode_forward_artifact
        trade_digest = encode_forward_artifact(trade_window)[1]
    else:
        # V2 binds the window by identity and the stored events' ids and payload hashes, never by their bytes.
        trade_digest = canonical_sha256(dict(identity=trade_window.identity, database_name=trade_window.database_name,
            scope_sha256=trade_window.scope_sha256, matrix_id=trade_window.matrix_id, provider_tier=trade_window.provider_tier,
            window_start=trade_window.window_start, window_end=trade_window.window_end,
            observed_at=trade_window.observed_at, recorded_at=trade_window.recorded_at,
            requested_contract_ids=trade_window.requested_contract_ids, coverage=trade_window.coverage,
            events=tuple(sorted((str(event.trade_event_id), event.payload_sha256) for event in trade_window.events))))
    input_sha256 = canonical_sha256(dict(contract=TECHNICAL_PACKAGE_CONTEXT_CONTRACT, original=original.input_sha256,
        enabled_detectors=enabled, stock_signal_policy=STOCK_BAR_SIGNAL_POLICY_SHA256, credit_policy=O3_CREDIT_POLICY_SHA256,
        gamma_policy_sha256=configuration.gamma_policy_sha256, trade_context_contract=TRADE_CONTEXT_CONTRACT,
        trade_window_sha256=trade_digest))
    return OfflineO1PackageContext(scope.sha256, matrix.matrix_id, serialized, input_sha256)