from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from options.analytics.flow_projection import assemble_option_flow, option_flow_snapshot_rows
from options.calendar import OptionExchangeCalendar
from options.config import OptionRuntimeConfiguration
from options.data.normalizer import DeveloperNormalizationInput, DeveloperOptionNormalizer, parse_polygon_snapshot
from options.data.polygon_chain_discovery import PolygonOptionChainDiscoveryPlan, build_polygon_option_chain_discovery_plan
from options.domain import DataQualityFlag
from options.repositories.daily_facts import OPEN_INTEREST_SOURCE_CHAIN_SNAPSHOT

from .contracts import canonical_sha256
from .forward_ingestion import (
    CompleteForwardMatrix, ForwardMatrixScope, ForwardOpenInterestEvidence, OfflineForwardPublication,
    _aware, forward_detector_dataset_id,
)
from .option_discovery_repository import RealtimeOptionDiscoveryEvidence
from .options_evidence_identity import (
    OptionsEvidenceIdentity, OptionsEvidenceLineage, carried_identity, identity_binding, options_evidence_admitted,
    require_options_lineage,
)
from .preflight import ProviderTier


FLOW_PROJECTION_CONTRACT = "original_option_flow_v1"


@dataclass(frozen=True, slots=True)
class DeveloperFlowChainEvidence:
    scope_sha256: str
    database_name: str
    provider_tier: ProviderTier
    discovery: RealtimeOptionDiscoveryEvidence
    inputs: tuple[DeveloperNormalizationInput, ...]


@dataclass(frozen=True, slots=True)
class DeveloperFlowChainEvidenceV2(DeveloperFlowChainEvidence):
    """The same chain evidence under the V2 lineage. `database_name` is the physical provenance; the
    lineage is the explicit `identity`. A separate type keeps the v1 field set (and its immutable
    forward-input encoding) unchanged and makes each gate reject the other lineage by exact type."""

    identity: OptionsEvidenceIdentity = field(kw_only=True)


_CHAIN_EVIDENCE_TYPES = {
    OptionsEvidenceLineage.V1_FORWARD: DeveloperFlowChainEvidence,
    OptionsEvidenceLineage.ADVANCED_REALTIME_V2: DeveloperFlowChainEvidenceV2,
}


@dataclass(frozen=True, slots=True)
class FlowOpenInterestPair:
    current: ForwardOpenInterestEvidence
    prior: ForwardOpenInterestEvidence


@dataclass(frozen=True, slots=True)
class OfflineOptionFlowPublication(OfflineForwardPublication):
    coverage_json: str
    input_sha256: str

    def coverage(self) -> dict:
        return json.loads(self.coverage_json)


def _validate_chain(scope, matrix, configuration, evidence, selected_at, lineage=OptionsEvidenceLineage.V1_FORWARD):
    if type(evidence) is not _CHAIN_EVIDENCE_TYPES[require_options_lineage(lineage)]:
        raise TypeError("Flow requires explicit full policy-chain evidence, not a selected manifest")
    discovery = evidence.discovery
    batch = discovery.result.batch
    if (not options_evidence_admitted(lineage, database_name=evidence.database_name, identity=carried_identity(evidence))
            or evidence.provider_tier is not ProviderTier.MASSIVE_DEVELOPER
            or evidence.provider_tier is not scope.selection.scope.provider_tier or evidence.scope_sha256 != scope.sha256
            or discovery.scope != scope.selection.scope or batch.underlyer != matrix.analysis.underlying.underlyer
            or batch.scheduled_cycle != scope.cutoff or batch.policy_sha256 != configuration.policy_sha256
            or not scope.available_after <= batch.started_at <= batch.completed_at <= selected_at):
        raise ValueError("Flow chain scope, tier, policy or availability drift; fallback prohibited")
    plan = build_polygon_option_chain_discovery_plan(batch.underlyer, scope.cutoff, discovery.spot_price,
        maximum_dte=configuration.policy.contract_filter.maximum_dte,
        acquisition_maximum_dte=configuration.settings.acquisition_maximum_dte,
        strike_corridor_fraction=configuration.policy.contract_filter.strike_corridor_fraction)
    exact_plan = PolygonOptionChainDiscoveryPlan.build(batch.underlyer, scope.cutoff,
        plan.expiration_through, discovery.strike_min, discovery.strike_max)
    if (batch.request_filter_sha256 != exact_plan.request_filter_sha256
            or discovery.expiration_from != plan.expiration_from or discovery.expiration_through != plan.expiration_through
            or discovery.strike_min != plan.strike_min or discovery.strike_max != plan.strike_max
            or discovery.result.included_ranges != exact_plan.included_ranges or discovery.result.excluded_ranges != exact_plan.excluded_ranges
            or discovery.strike_corridor_fraction != configuration.policy.contract_filter.strike_corridor_fraction):
        raise ValueError("Flow discovery does not cover the complete original policy window")
    RealtimeOptionDiscoveryEvidence(discovery.scope, discovery.underlying_security_id, discovery.result)
    raw_rows, cursor, seen = [], None, set()
    for index, page in enumerate(batch.pages, start=1):
        _aware(page.received_at)
        if (page.page_number != index or page.request_cursor_sha256 != cursor or page.request_cursor_sha256 in seen
                or not batch.started_at <= page.received_at <= batch.completed_at
                or page.terminal != (index == len(batch.pages)) or page.terminal != (page.next_cursor_sha256 is None)):
            raise ValueError("Flow discovery terminal pagination or receipt drift")
        seen.add(page.request_cursor_sha256)
        cursor = page.next_cursor_sha256
        payload = json.loads(page.response_bytes)
        if payload.get("status") != "DELAYED" or not isinstance(payload.get("results"), list):
            raise ValueError("Developer Flow cannot relabel a different source tier")
        if bool(payload.get("next_url")) == page.terminal:
            raise ValueError("Flow raw terminal cursor evidence changed")
        if len(payload["results"]) != page.row_count:
            raise ValueError("Flow discovery page row count changed")
        raw_rows.extend(parse_polygon_snapshot(row, page.received_at) for row in payload["results"])
    if tuple(json.loads(row.raw_payload_json) for row in raw_rows) != discovery.result.rows:
        raise ValueError("Flow retained discovery rows changed after raw capture")
    observed = {row.contract_ticker: row for row in raw_rows}
    supplied = {row.catalog.contract_ticker: row for row in evidence.inputs}
    if len(observed) != len(raw_rows) or len(supplied) != len(evidence.inputs) or set(observed) != set(supplied):
        raise ValueError("Flow full-chain reference/input coverage is incomplete or ambiguous")
    selected_references = {row.contract_id: row for row in scope.catalog}
    reference_ids = set()
    for ticker, item in supplied.items():
        catalog = item.catalog
        if (item.raw != observed[ticker] or catalog.underlyer != batch.underlyer or catalog.contract_id in reference_ids
            or catalog.contract_id in selected_references and catalog != selected_references[catalog.contract_id]
                or catalog.valid_from > scope.cutoff or catalog.valid_to is not None and catalog.valid_to <= scope.cutoff
                or max(catalog.first_observed_at, catalog.revised_observed_at or catalog.first_observed_at) > scope.cutoff
                or item.normalized_observed_at is not None and item.normalized_observed_at > selected_at
                or item.raw.option_mark_time is None or item.raw.option_mark_time > scope.cutoff):
            raise ValueError("Flow full-chain reference, raw input or causal clock drift")
        reference_ids.add(catalog.contract_id)
        if item.raw.day_volume is None or item.raw.open_interest is None:
            raise ValueError("Flow volume/OI evidence unavailable; missing inputs cannot be zero totals")
    normalized = DeveloperOptionNormalizer(configuration.policy, configuration.valuation_policy).normalize(
        matrix.ingress.batch_id, evidence.inputs)
    allowed_exclusions = {"UNSUPPORTED_CONTRACT_TYPE", "UNSUPPORTED_EXERCISE_STYLE", "UNSUPPORTED_MULTIPLIER",
        "ADJUSTED_CONTRACT", "EXPIRED_CONTRACT", "DTE_OUT_OF_RANGE", "OUTSIDE_STRIKE_CORRIDOR", "LIQUIDITY_FLOOR"}
    if any(reason not in allowed_exclusions for reason, count in normalized.rejected_counts if count):
        raise ValueError("Flow normalization evidence unavailable; stale or incomplete input cannot certify a full chain")
    if {row.contract_id for row in normalized.matrix_snapshots} != {row.contract_id for row in matrix.snapshots}:
        raise ValueError("selected-contract subset is not the complete policy-eligible chain")
    if tuple(sorted(normalized.matrix_snapshots, key=lambda row: row.contract_id)) != tuple(sorted(matrix.snapshots, key=lambda row: row.contract_id)):
        raise ValueError("Flow normalized matrix/reference revisions changed")
    if any(row.day_volume is None or row.open_interest is None or row.model_mark is None
            or DataQualityFlag.STALE_MARK in row.quality_flags for row in matrix.snapshots):
        raise ValueError("Flow retained activity evidence unavailable")
    return normalized.received_count


def _oi_changes(scope, matrices, pairs, selected_at):
    calendar = OptionExchangeCalendar()
    session = scope.selection.session_date
    current_session = calendar.previous_session(session)
    prior_session = calendar.previous_session(current_session)
    snapshots = {row.contract_id: row for matrix in matrices for row in matrix.snapshots}
    # Counted once per underlying: the count used to be recomputed over every snapshot for every pair.
    contract_counts = Counter(row.underlyer for row in snapshots.values())
    by_symbol, seen = {}, set()
    for pair in pairs:
        current, prior = pair.current, pair.prior
        row = snapshots.get(current.record.contract_id)
        if row is None or row.contract_id in seen:
            raise ValueError("Flow OI pair is outside exact matrix membership or ambiguous")
        seen.add(row.contract_id)
        for evidence, settlement in ((current, current_session), (prior, prior_session)):
            record = evidence.record
            observed_session = session if evidence is current else current_session
            _aware(record.observed_at)
            _aware(evidence.recorded_at)
            if (evidence.source != OPEN_INTEREST_SOURCE_CHAIN_SNAPSHOT
                    or type(evidence.revision_count) is not int or evidence.revision_count != 0
                    or record.contract_id != row.contract_id or record.underlying != row.underlyer
                    or record.settlement_session != settlement or record.observed_session != observed_session
                    or type(record.open_interest) is not int or record.open_interest < 0
                    or not record.observed_at <= evidence.recorded_at <= selected_at):
                raise ValueError("Flow settled OI lineage, dates or availability drift")
        if current.record.batch_id != row.batch_id or current.record.open_interest != row.open_interest or current.record.observed_at > row.first_observed_at:
            raise ValueError("Flow current OI does not match the original snapshot receipt")
        item = by_symbol.get(row.underlyer)
        if item is None:
            item = by_symbol[row.underlyer] = dict(underlying=row.underlyer, settlement_session=current_session,
                prior_settlement_session=prior_session, matched_contract_count=0,
                current_contract_count=contract_counts[row.underlyer],
                call_open_interest_change=0, put_open_interest_change=0)
        item["matched_contract_count"] += 1
        item[f"{row.contract_type.value.lower()}_open_interest_change"] += current.record.open_interest - prior.record.open_interest
    return tuple(by_symbol[symbol] for symbol in sorted(by_symbol))


def _json_default(value):
    return value.isoformat() if isinstance(value, (date, datetime)) else str(value)


def validate_developer_policy_chain_bundle(
    scope: ForwardMatrixScope, matrices: tuple[CompleteForwardMatrix, ...], configuration: OptionRuntimeConfiguration,
    *, selected_at: datetime, chain_evidence: tuple[DeveloperFlowChainEvidence, ...] = (),
    unavailable_underlyers: tuple[tuple[str, tuple[str, ...]], ...] = (),
    lineage: OptionsEvidenceLineage = OptionsEvidenceLineage.V1_FORWARD,
) -> tuple[tuple[tuple[CompleteForwardMatrix, DeveloperFlowChainEvidence, int], ...], tuple]:
    require_options_lineage(lineage)
    _aware(selected_at)
    if (configuration.configuration_sha256 != scope.configuration_sha256
            or tuple(sorted(configuration.settings.underlyers)) != scope.underlyings
            or not scope.available_after <= selected_at <= scope.deadline
            or scope.selection.scope.provider_tier is not ProviderTier.MASSIVE_DEVELOPER):
        raise ValueError("Flow publication scope, tier, configuration or deadline drift")
    ordered = tuple(sorted(matrices, key=lambda row: row.analysis.underlying.underlyer))
    present = tuple(row.analysis.underlying.underlyer for row in ordered)
    missing = tuple(sorted(set(scope.underlyings) - set(present)))
    if (len(set(present)) != len(present) or set(present) - set(scope.underlyings)
            or tuple(name for name, reasons in unavailable_underlyers) != missing
            or any(not reasons or any(not reason for reason in reasons) for _, reasons in unavailable_underlyers)):
        raise ValueError("Flow publication must freeze exact missing-underlying evidence")
    for matrix in ordered:
        if (matrix.ingress.scope != scope or matrix.ingress.source.completed_at > selected_at
                or matrix.analysis.chain_health.status != "COMPLETE"
                or matrix.content_sha256 != canonical_sha256((matrix.ingress.sha256, matrix.snapshots, matrix.analysis))):
            raise ValueError("Flow matrix content or source receipt changed")
    evidence_by_symbol = {row.discovery.result.batch.underlyer: row for row in chain_evidence}
    if len(evidence_by_symbol) != len(chain_evidence) or set(evidence_by_symbol) - set(present):
        raise ValueError("Flow discovery evidence is outside exact completed matrices")
    accepted, blocked = [], list(unavailable_underlyers)
    for matrix in ordered:
        symbol = matrix.analysis.underlying.underlyer
        evidence = evidence_by_symbol.get(symbol)
        if evidence is None:
            blocked.append((symbol, ("POLICY_ELIGIBLE_CHAIN_COVERAGE_UNAVAILABLE",)))
            continue
        received_count = _validate_chain(scope, matrix, configuration, evidence, selected_at, lineage)
        accepted.append((matrix, evidence, received_count))
    return tuple(accepted), tuple(sorted(blocked))


def developer_policy_chain_input_sha256(scope, matrices, chain_evidence, *, open_interest_pairs=None) -> str:
    payload = dict(scope=scope.sha256,
        matrices=tuple(row.content_sha256 for row in sorted(matrices, key=lambda row: row.analysis.underlying.underlyer)),
        chains=tuple(sorted(canonical_sha256(dict(scope=row.scope_sha256, database_name=row.database_name,
            provider_tier=row.provider_tier, discovery_sha256=row.discovery.evidence_sha256,
            inputs=tuple(sorted(row.inputs, key=lambda item: item.catalog.contract_id)),
            **identity_binding(row))) for row in chain_evidence)))
    if open_interest_pairs is not None:
        payload["oi_pairs"] = tuple(sorted(canonical_sha256(row) for row in open_interest_pairs))
    return canonical_sha256(payload)


@dataclass(frozen=True, slots=True)
class OptionFlowBundle:
    """One cycle's Flow evidence, validated and aggregated once. Every per-symbol publication is a cheap
    view of it: validating the bundle (reparse and renormalize every chain) per symbol made publication
    O(U^2 * C) for U underlyings with C contracts each."""

    scope: ForwardMatrixScope
    configuration: OptionRuntimeConfiguration
    selected_at: datetime
    matrices_present: bool
    summaries: tuple
    details: dict
    changes: tuple
    blocked: tuple
    input_sha256: str
    dataset_id: str


def build_option_flow_bundle(
    scope: ForwardMatrixScope, matrices: tuple[CompleteForwardMatrix, ...], configuration: OptionRuntimeConfiguration,
    *, selected_at: datetime, chain_evidence: tuple[DeveloperFlowChainEvidence, ...] = (),
    open_interest_pairs: tuple[FlowOpenInterestPair, ...] = (),
    unavailable_underlyers: tuple[tuple[str, tuple[str, ...]], ...] = (),
    lineage: OptionsEvidenceLineage = OptionsEvidenceLineage.V1_FORWARD,
) -> OptionFlowBundle:
    members, blocked = validate_developer_policy_chain_bundle(scope, matrices, configuration,
        selected_at=selected_at, chain_evidence=chain_evidence, unavailable_underlyers=unavailable_underlyers,
        lineage=lineage)
    summaries, details, accepted = [], {}, []
    for matrix, evidence, received_count in members:
        symbol = matrix.analysis.underlying.underlyer
        profiles = option_flow_snapshot_rows(matrix.snapshots)
        snapshots = matrix.snapshots
        summaries.append(dict(profiles["summary"], underlying=symbol, asset_type=evidence.inputs[0].catalog.asset_type.value,
            batch_id=matrix.ingress.batch_id, matrix_id=matrix.matrix_id, scheduled_cycle=scope.cutoff,
            market_data_time=max(row.market_data_time for row in snapshots), first_observed_at=max(row.first_observed_at for row in snapshots),
            received_row_count=received_count, retained_row_count=len(snapshots), model_version=snapshots[0].model_version,
            policy_sha256=configuration.policy_sha256, configuration_sha256=configuration.configuration_sha256, current_policy=True))
        details[symbol] = profiles
        accepted.append(matrix)
    return OptionFlowBundle(
        scope, configuration, selected_at, bool(matrices), tuple(summaries), details,
        _oi_changes(scope, tuple(accepted), open_interest_pairs, selected_at), tuple(sorted(blocked)),
        developer_policy_chain_input_sha256(scope, matrices, chain_evidence, open_interest_pairs=open_interest_pairs),
        forward_detector_dataset_id(scope, configuration))


def option_flow_publication(bundle: OptionFlowBundle, underlyer: str | None = None) -> OfflineOptionFlowPublication:
    """The sealed publication for one requested underlying.

    With no `underlyer` the original API default applies (SPY, falling back to the first available
    summary). An explicitly requested underlying that has no accepted chain gets an explicit unavailable
    result for that underlying; it is never replaced by another underlying's profile."""
    scope, configuration, selected_at = bundle.scope, bundle.configuration, bundle.selected_at
    summaries, details, blocked = bundle.summaries, bundle.details, bundle.blocked
    requested = underlyer.strip().upper() if underlyer else "SPY"
    if underlyer is not None and summaries and requested not in details:
        reasons = tuple(sorted(dict(blocked).get(requested, ())))
        payload = dict(available=False, reason=reasons[0] if reasons else "UNDERLYER_NOT_IN_COHORT",
            data_tier="15-MINUTE DELAYED RESEARCH DATA", generated_at=selected_at, as_of=None, observed_at=None,
            policy_sha256=configuration.policy_sha256, model_version=None,
            data=dict(underlyers=[], selected=requested, session_date=scope.selection.session_date,
                reasons=list(reasons)))
    else:
        selected = requested if requested in details else summaries[0]["underlying"] if summaries else requested
        profiles = details.get(selected, dict(expirations=(), strikes=(), top_contracts=()))
        projection = assemble_option_flow(summaries=summaries, open_interest_changes=bundle.changes,
            expirations=profiles["expirations"], strikes=profiles["strikes"], top_contracts=profiles["top_contracts"],
            underlyer=requested, session_date=scope.selection.session_date,
            policy_sha256=configuration.policy_sha256, configuration_sha256=configuration.configuration_sha256)
        payload = dict(available=projection.available, reason=projection.reason if not bundle.matrices_present else
            "POLICY_ELIGIBLE_CHAIN_COVERAGE_UNAVAILABLE" if not summaries else None,
            data_tier="15-MINUTE DELAYED RESEARCH DATA", generated_at=selected_at, as_of=projection.as_of,
            observed_at=projection.observed_at, policy_sha256=configuration.policy_sha256,
            model_version=projection.model_version, data=projection.data)
    coverage = dict(contract=FLOW_PROJECTION_CONTRACT, requested_underlyers=scope.underlyings,
        available_underlyers=tuple(row["underlying"] for row in summaries), unavailable_underlyers=blocked,
        status="MISSING" if not summaries else "PARTIAL" if blocked else "COMPLETE",
        selection_basis="COMPLETE_POLICY_ELIGIBLE_CHAIN", oi_change_underlyers=tuple(row["underlying"] for row in bundle.changes))
    return OfflineOptionFlowPublication(scope.cohort_id, scope.selection.scope.provider_tier, scope.sha256,
        bundle.dataset_id,
        json.dumps(payload, default=_json_default, sort_keys=True, separators=(",", ":"), allow_nan=False),
        json.dumps(coverage, sort_keys=True, separators=(",", ":")), bundle.input_sha256)


def seal_offline_option_flow_publications(
    scope: ForwardMatrixScope, matrices: tuple[CompleteForwardMatrix, ...], configuration: OptionRuntimeConfiguration,
    *, selected_at: datetime, chain_evidence: tuple[DeveloperFlowChainEvidence, ...] = (),
    open_interest_pairs: tuple[FlowOpenInterestPair, ...] = (),
    unavailable_underlyers: tuple[tuple[str, tuple[str, ...]], ...] = (), underlyers: tuple[str, ...] | None = None,
    lineage: OptionsEvidenceLineage = OptionsEvidenceLineage.V1_FORWARD,
) -> dict[str, OfflineOptionFlowPublication]:
    """One publication per underlying (the cohort's by default) from a single validation of the bundle."""
    bundle = build_option_flow_bundle(scope, matrices, configuration, selected_at=selected_at,
        chain_evidence=chain_evidence, open_interest_pairs=open_interest_pairs,
        unavailable_underlyers=unavailable_underlyers, lineage=lineage)
    return {symbol: option_flow_publication(bundle, symbol)
            for symbol in (scope.underlyings if underlyers is None else underlyers)}


def seal_offline_option_flow_publication(
    scope: ForwardMatrixScope, matrices: tuple[CompleteForwardMatrix, ...], configuration: OptionRuntimeConfiguration,
    *, selected_at: datetime, chain_evidence: tuple[DeveloperFlowChainEvidence, ...] = (),
    open_interest_pairs: tuple[FlowOpenInterestPair, ...] = (),
    unavailable_underlyers: tuple[tuple[str, tuple[str, ...]], ...] = (), underlyer: str | None = None,
    lineage: OptionsEvidenceLineage = OptionsEvidenceLineage.V1_FORWARD,
) -> OfflineOptionFlowPublication:
    """One publication. Sealing several underlyings of the same cycle should use
    `seal_offline_option_flow_publications`, which validates the bundle once."""
    return option_flow_publication(build_option_flow_bundle(scope, matrices, configuration,
        selected_at=selected_at, chain_evidence=chain_evidence, open_interest_pairs=open_interest_pairs,
        unavailable_underlyers=unavailable_underlyers, lineage=lineage), underlyer)