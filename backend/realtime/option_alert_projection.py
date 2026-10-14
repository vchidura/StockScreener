from __future__ import annotations

import json
from collections import Counter
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, model_validator

from options.analytics.alert_selection import (
    DetectorSelectionEvidence, DUAL_ORIGIN_SELECTOR_SHA256, O1IndicatorEvaluationEvidence,
    build_detector_run, build_o1_indicator_evidence, build_o3_credit_evidence, build_selection_evidence, load_detector_run, load_evaluation_evidence,
    select_dual_origin_packages, select_o3_credit_observations, O3CreditEvaluationEvidence,
)
from options.alert_plans import FrozenOptionAlertPlan
from options.alert_qualification import retained_candidate
from options.analytics.behavior_review import (
    _sort_detector_records, build_detector_alert_review, build_detector_evaluation_review,
)
from options.config import OptionRuntimeConfiguration
from options.dual_origin import (QualifiedCanonicalStockPackage, QualifiedTechnicalPackage, TECHNICAL_QUALIFICATION_POLICY,
    assess_options_credit_canonical_trend)
from options.credit_detection import O3CreditObservation, assess_o3_credit
from options.outcome_contracts import assess_option_package

from .contracts import canonical_sha256
from .forward_ingestion import (
    DEVELOPER_SOURCE, FORWARD_CONTEXT_CONTRACT, FORWARD_DATASET_IDENTITY, ForwardDetectorContext,
    ForwardMatrixScope, OfflineForwardPublication, _aware, _context_sha256,
    forward_context_policy_sha256, forward_detector_dataset_id,
)
from .preflight import DetectorFamily, ProviderTier
from .options_evidence_identity import (
    OptionsEvidenceIdentity, OptionsEvidenceLineage, carried_identity, identity_binding, options_evidence_admitted,
    require_options_lineage,
)
from .option_package_context import OfflineO1PackageContext, PACKAGE_CONTEXT_CONTRACT, TECHNICAL_PACKAGE_CONTEXT_CONTRACT, TRADE_CONTEXT_CONTRACT


ALERT_REVIEW_CONTRACT = "original_detector_review_inputs_v1"
INDICATIVE_REVIEW_CONTRACT = "original_indicative_detector_review_v1"
LEGACY_TECHNICAL_REVIEW_CONTRACT = "original_technical_detector_review_v1"
TECHNICAL_REVIEW_CONTRACT = "original_technical_detector_review_v3"
TECHNICAL_REVIEW_CONTRACTS = (LEGACY_TECHNICAL_REVIEW_CONTRACT, "original_technical_detector_review_v2", TECHNICAL_REVIEW_CONTRACT)
SORT_FIELDS = ("triggered_at", "run", "underlyer", "detector", "category", "strategy", "rank", "entry_limit")


class DetectorEvaluationQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")
    session_date: date | None = None
    dataset_id: str | None = Field(default=None, min_length=1, max_length=80)
    detector: Literal["O1", "O2", "O3", "S1", "S2"] | None = None
    selection_status: Literal["SELECTED", "NOT_SELECTED", "REPEAT", "OBSERVATION"] | None = None
    underlyer: str | None = None
    sort_by: Literal["triggered_at", "run", "underlyer", "detector", "category", "strategy", "rank", "entry_limit"] = "run"
    sort_order: Literal["asc", "desc"] = "asc"
    limit: int = Field(default=50, ge=1, le=200)
    offset: int = Field(default=0, ge=0)


class DetectorRunQuery(DetectorEvaluationQuery):
    dataset_id: str = Field(min_length=1, max_length=80)
    scope: Literal["LATEST", "HISTORY"] = "LATEST"
    session_rollup: bool = False

    @model_validator(mode="after")
    def validate_rollup(self):
        if self.selection_status is not None:
            raise ValueError("original detector-runs query has no selection-status filter")
        if self.session_rollup and (self.scope != "HISTORY" or self.session_date is None):
            raise ValueError("detector session rollup requires one history session")
        return self


@dataclass(frozen=True, slots=True)
class ForwardAlertCycleSource:
    scope: ForwardMatrixScope
    publication: OfflineForwardPublication
    database_name: str
    provider_tier: ProviderTier
    detector_context: ForwardDetectorContext | None = None
    package_contexts: tuple[OfflineO1PackageContext, ...] = ()


@dataclass(frozen=True, slots=True)
class ForwardAlertCycleSourceV2(ForwardAlertCycleSource):
    """A cycle receipt under the V2 lineage (see `options_evidence_identity`). Its detector context must carry
    the V2 identity too; no V2 detector-context type exists yet, so a V2 receipt carries none today."""

    identity: OptionsEvidenceIdentity = field(kw_only=True)


_CYCLE_SOURCE_TYPES = {
    OptionsEvidenceLineage.V1_FORWARD: ForwardAlertCycleSource,
    OptionsEvidenceLineage.ADVANCED_REALTIME_V2: ForwardAlertCycleSourceV2,
}

class _NativeReviewInputs:
    def __init__(self, cycles, dataset_id):
        self.cycles = cycles
        self.dataset_id = dataset_id

    def completed_runs(self, *, dataset_id, as_of):
        return tuple(run for run, _, _ in self.cycles if run.dataset_id == dataset_id and run.selected_at <= as_of)

    def completed_run(self, *, dataset_id, scheduled_cycle, as_of):
        return next(((run, records) for run, records, _ in self.cycles
            if run.dataset_id == dataset_id and run.scheduled_cycle == scheduled_cycle and run.selected_at <= as_of), None)

    def review_inputs(self, *, as_of, session_date=None, dataset_id=None):
        runs = self.completed_runs(dataset_id=dataset_id or self.dataset_id, as_of=as_of)
        sessions = tuple(sorted({run.scheduled_cycle.astimezone(ZoneInfo("America/New_York")).date() for run in runs}))
        selected = session_date or (sessions[-1] if sessions else None)
        included = tuple(run for run in runs if run.scheduled_cycle.astimezone(ZoneInfo("America/New_York")).date() == selected)
        identities = {run.run_id for run in included}
        records = tuple(record for run, values, _ in self.cycles if run.run_id in identities for record in values)
        return dict(ready=bool(self.cycles), datasets=(self.dataset_id,) if self.cycles else (),
            dataset_id=dataset_id or self.dataset_id, sessions=sessions, session_date=selected, runs=included, records=records)

    def repeat_counts(self, **kwargs):
        raise ValueError("package repeat context unavailable; no default repository allowed")


def _view_key(session, underlyer, detector=None):
    return json.dumps((session, underlyer, detector) if detector is not None else (session, underlyer), separators=(",", ":"))


def _dataset_index(cycles, dataset_id):
    sessions = sorted({run.scheduled_cycle.astimezone(ZoneInfo("America/New_York")).date().isoformat()
        for run, _, _ in cycles})
    current = dataset_id if sessions else None
    return dict(storage_ready=bool(cycles), datasets=[dataset_id] if cycles else [],
        dataset_sessions={dataset_id: sessions} if cycles else {}, sessions=sessions,
        session_datasets={session: dataset_id for session in sessions},
        current_dataset_id=current, default_dataset_id=current,
        current_session_date=sessions[-1] if sessions else None,
        configured_dataset_id=None, configured_effective_from=None)


def _decode_cycle(source, scope, configuration, selected_at, lineage=OptionsEvidenceLineage.V1_FORWARD):
    if type(source) is not _CYCLE_SOURCE_TYPES[require_options_lineage(lineage)]:
        raise TypeError("Alerts require explicit immutable cycle receipts")
    cycle_scope = source.scope
    if (not options_evidence_admitted(lineage, database_name=source.database_name, identity=carried_identity(source))
            or source.provider_tier is not ProviderTier.MASSIVE_DEVELOPER
            or source.provider_tier is not cycle_scope.selection.scope.provider_tier
            or cycle_scope.cohort_id != scope.cohort_id
            or forward_detector_dataset_id(cycle_scope, configuration) != forward_detector_dataset_id(scope, configuration)):
        raise ValueError("Alerts source target/cohort/tier/manifest/policy drift; fallback prohibited")
    publication = source.publication
    payload = publication.read(cohort_id=cycle_scope.cohort_id, provider_tier=source.provider_tier,
        scope_sha256=cycle_scope.sha256)
    if (payload.get("schema_version") != "offline_forward_projections_v1" or payload["source_kind"] != DEVELOPER_SOURCE
            or payload["original_page_contracts"] is not False or payload["execution_permission"] is not False
            or payload["alerts"]["available"] is not False
            or payload["scope_sha256"] != cycle_scope.sha256 or payload["cohort_id"] != str(scope.cohort_id)
            or payload["provider_tier"] != source.provider_tier.value
            or payload["reference_sha256"] != cycle_scope.reference_sha256 or payload["action_sha256"] != cycle_scope.action_sha256
            or payload["selection_sha256"] != cycle_scope.selection.selection_sha256
            or payload["dataset_identity_version"] != FORWARD_DATASET_IDENTITY
            or payload["dataset_id"] != publication.dataset_id or payload["dataset_id"] != forward_detector_dataset_id(scope, configuration)
            or payload["cutoff"] != cycle_scope.cutoff.isoformat() or payload["deadline"] != cycle_scope.deadline.isoformat()):
        raise ValueError("Alerts immutable source receipt identity changed")
    run = load_detector_run(json.dumps(payload["alerts"]["run"]))
    records = tuple(load_evaluation_evidence(json.dumps(row)) for row in payload["alerts"]["records"])
    counts = Counter(record.selection_status for record in records)
    if (run.dataset_id != publication.dataset_id or run.scheduled_cycle != cycle_scope.cutoff
            or run.selected_at.isoformat() != payload["published_at"]
            or not cycle_scope.available_after <= run.selected_at <= cycle_scope.deadline or run.selected_at > selected_at
            or run.configuration_sha256 != configuration.configuration_sha256
            or run.market_policy_sha256 != configuration.policy_sha256
            or run.strategy_policy_sha256 != configuration.strategy_policy_sha256
            or run.expected_underlyers != cycle_scope.underlyings
            or run.coverage_status != payload["coverage_status"]
            or dict(run.selection_counts) != {status: counts[status] for status in ("SELECTED", "NOT_SELECTED", "REPEAT", "OBSERVATION")}
            or tuple((symbol, tuple(reasons)) for symbol, reasons in payload["unavailable_underlyers"]) != run.unavailable_underlyers
            or tuple(sorted(((record.evaluation_id, record.sha256) for record in records), key=lambda value: str(value[0]))) != run.record_sha256s):
        raise ValueError("Alerts completed run does not reconcile with exact native evidence")
    context = source.detector_context
    metadata = payload["detector_context"]
    if (context is None) != (metadata is None):
        raise ValueError("Alerts context receipt missing or substituted")
    if context is not None:
        if (context.source.scope_sha256 != cycle_scope.sha256 or context.selected_at != run.selected_at
                or context.source.database_name != source.database_name or context.source.provider_tier is not source.provider_tier
                or not options_evidence_admitted(lineage, database_name=context.source.database_name,
                    identity=carried_identity(context.source))
                or context.content_sha256 != _context_sha256(context.source, context.selected_at, context.inputs, context.dependencies)
                or metadata["contract"] != FORWARD_CONTEXT_CONTRACT or metadata["capability"] != "O1_OBSERVATION"
                or metadata["content_sha256"] != context.content_sha256 or metadata["policy_sha256"] != forward_context_policy_sha256()
                or metadata["status"] != context.status
                or context.inputs.package_inputs or context.inputs.surface_inputs
                or context.inputs.o3_observations or context.inputs.stock_setup_observations
                or metadata["package_context"] != "UNAVAILABLE" or metadata["event_calendar"] != "UNAVAILABLE"
                or tuple((symbol, tuple(reasons)) for symbol, reasons in metadata["dependencies"]) != context.dependencies
                or tuple(symbol for symbol, _ in context.dependencies) != tuple(symbol for symbol, _ in run.source_matrices)
                or tuple((row["underlying"], row["matrix_id"]) for row in context.inputs.matrices) != run.source_matrices
                or max(row["market_time"] for row in context.inputs.matrices) != run.market_time
                or max(row["observed_time"] for row in context.inputs.matrices) != run.observed_time):
            raise ValueError("Alerts exact detector-context receipt drift")
    observations = {row.sha256 for row in context.inputs.o1_observations} if context is not None else set()
    sources = dict(run.source_matrices)
    for record in records:
        if (not isinstance(record, O1IndicatorEvaluationEvidence) or record.dataset_id != run.dataset_id
                or record.run_id != run.run_id or record.selected_at != run.selected_at
                or record.selector_sha256 != run.selector_sha256 or sources.get(record.observation.underlyer) != record.matrix_id
                or record.observation.sha256 not in observations):
            raise ValueError("Alerts evidence is outside the exact supported O1 source capability")
    dependencies = dict(context.dependencies) if context is not None else {}
    for symbol, reasons in run.unavailable_underlyers if run.partial_coverage else ():
        dependencies[symbol] = tuple(reasons)
    if context is None:
        dependencies.update((symbol, ("DETECTOR_CONTEXT_UNAVAILABLE",)) for symbol in run.expected_underlyers)
    return run, records, dependencies


@dataclass(frozen=True, slots=True)
class OfflineOptionAlertPublication(OfflineForwardPublication):
    input_sha256: str

    def _stored(self, *, cohort_id, provider_tier, scope_sha256):
        stored = self.read(cohort_id=cohort_id, provider_tier=provider_tier, scope_sha256=scope_sha256)
        if stored["contract"] not in (ALERT_REVIEW_CONTRACT, INDICATIVE_REVIEW_CONTRACT, *TECHNICAL_REVIEW_CONTRACTS):
            raise ValueError("stored Alerts contract mismatch")
        return stored

    @staticmethod
    def _envelope(stored, *, available, reason=None, data=None):
        return dict(available=available, reason=reason, data_tier="15-MINUTE DELAYED RESEARCH DATA",
            generated_at=stored["published_at"], as_of=stored.get("market_time", stored["published_at"]) if available else None,
            observed_at=stored.get("observed_time") if available else None,
            policy_sha256=stored["policy_sha256"], model_version=None, data=data or {})

    def datasets(self, **identity):
        stored = self._stored(**identity)
        return self._envelope(stored, available=bool(stored["datasets"]["datasets"]),
            reason=None if stored["datasets"]["datasets"] else "DETECTOR_DATASET_INDEX_UNAVAILABLE", data=stored["datasets"])

    def evaluations(self, request: DetectorEvaluationQuery, **identity):
        request = DetectorEvaluationQuery.model_validate(request.model_dump())
        stored = self._stored(**identity)
        if request.dataset_id not in (None, self.dataset_id):
            return self._envelope(stored, available=False, reason="DETECTOR_DATASET_UNAVAILABLE")
        technical = stored["contract"] in TECHNICAL_REVIEW_CONTRACTS
        indicative = stored["contract"] in (INDICATIVE_REVIEW_CONTRACT, *TECHNICAL_REVIEW_CONTRACTS)
        if technical and request.detector is not None and request.detector not in stored["enabled_detectors"]:
            return self._envelope(stored, available=False, reason="DETECTOR_DISABLED_BY_PROVIDER_TIER")
        if not technical and request.detector != "O1" and not (indicative and request.detector is None):
            return self._envelope(stored, available=False, reason="DETECTOR_CONTEXT_UNAVAILABLE")
        if not indicative and request.selection_status not in (None, "OBSERVATION"):
            return self._envelope(stored, available=False, reason="PACKAGE_CONTEXT_UNAVAILABLE")
        symbol = request.underlyer.strip().upper() if request.underlyer else None
        if symbol is not None and symbol not in stored["underlyers"]:
            return self._envelope(stored, available=False, reason="UNDERLYING_NOT_IN_MANIFEST")
        session = request.session_date.isoformat() if request.session_date else stored["datasets"]["current_session_date"]
        view = stored["evaluation_views"].get(_view_key(session, symbol, request.detector if technical else None))
        if view is None:
            return self._envelope(stored, available=False, reason="DETECTOR_EVALUATION_UNAVAILABLE")
        if not view["available"]:
            return self._envelope(stored, available=False, reason="DETECTOR_CONTEXT_UNAVAILABLE")
        data = deepcopy(view["data"])
        rows = {row["evaluation_id"]: row for row in data["rows"]}
        order = view["orders"][request.sort_by][request.sort_order]
        chosen = [rows[identity] for identity in order if identity in rows
            and (request.selection_status is None or rows[identity]["selection_status"] == request.selection_status)]
        data.update(rows=chosen[request.offset:request.offset + request.limit], total=len(chosen), limit=request.limit, offset=request.offset)
        if indicative:
            data["context_availability"] = (stored["context_availability"] if technical else
                {family: "AVAILABLE_INDICATIVE" if family == "O1" else "UNAVAILABLE" for family in ("O1", "O2", "O3", "S1", "S2")})
        if request.selection_status is not None and not indicative:
            for cell in data["cells"]:
                if cell["selection_status"] != request.selection_status:
                    raise ValueError("stored O1 evaluation cells exceed the observation-only capability")
        return self._envelope(stored, available=True, data=data)

    def detector_runs(self, request: DetectorRunQuery, **identity):
        request = DetectorRunQuery.model_validate(request.model_dump())
        stored = self._stored(**identity)
        if request.dataset_id != self.dataset_id:
            return self._envelope(stored, available=False, reason="DETECTOR_DATASET_UNAVAILABLE")
        technical = stored["contract"] in TECHNICAL_REVIEW_CONTRACTS
        if technical and request.detector is not None and request.detector not in stored["enabled_detectors"]:
            return self._envelope(stored, available=False, reason="DETECTOR_DISABLED_BY_PROVIDER_TIER")
        if not technical and request.detector not in (None, "O1"):
            return self._envelope(stored, available=False, reason="DETECTOR_CONTEXT_UNAVAILABLE")
        symbol = request.underlyer.strip().upper() if request.underlyer else None
        if symbol is not None and symbol not in stored["underlyers"]:
            return self._envelope(stored, available=False, reason="UNDERLYING_NOT_IN_MANIFEST")
        session = request.session_date.isoformat() if request.session_date else stored["datasets"]["current_session_date"]
        view = stored["run_views"].get(_view_key(session, symbol, request.detector if technical else None), {}).get(request.scope)
        if stored["contract"] in (INDICATIVE_REVIEW_CONTRACT, *TECHNICAL_REVIEW_CONTRACTS):
            if view is None:
                return self._envelope(stored, available=False, reason="DETECTOR_ALERTS_UNAVAILABLE")
            if not view["available"]:
                return self._envelope(stored, available=False, reason="PACKAGE_CONTEXT_UNAVAILABLE")
            data = deepcopy(view["data"])
            rows = {row["evaluation_id"]: row for row in data["rows"]}
            order = view["orders"][request.sort_by][request.sort_order]
            ordered = [rows[identity] for identity in order if identity in rows]
            data.update(rows=ordered[request.offset:request.offset + request.limit], total=len(ordered),
                limit=request.limit, offset=request.offset)
            data["context_availability"] = (stored["context_availability"] if technical else
                {family: "AVAILABLE_INDICATIVE" if family == "O1" else "UNAVAILABLE" for family in ("O1", "O2", "O3", "S1", "S2")})
            return self._envelope(stored, available=True, data=data)
        if view:
            view = deepcopy(view)
            view.update(limit=request.limit, offset=request.offset)
        return self._envelope(stored, available=False,
            reason="PACKAGE_CONTEXT_UNAVAILABLE" if view else "DETECTOR_ALERTS_UNAVAILABLE", data=view or {})

    def schedule(self, *, checked_at: datetime, **identity):
        _aware(checked_at)
        stored = self._stored(**identity)
        result = self._envelope(stored, available=False, reason="DETECTOR_LAUNCH_NOT_CONFIGURED",
            data=dict(dataset_id=self.dataset_id, retained_cycle_cutoff=stored["cutoff"],
                completed_run_count=len(stored.get("native_cycles", ())),
                operational_schedule_state="NOT_CONFIGURED", worker_health="NOT_CHECKED"))
        result["generated_at"] = checked_at
        return result

    def candidate(self, candidate_id: UUID, **identity):
        stored = self._stored(**identity)
        detail = stored.get("candidate_details", {}).get(str(candidate_id))
        if detail is None:
            return self._envelope(stored, available=False, reason="CANDIDATE_NOT_FOUND")
        if detail["candidate"]["candidate_id"] != str(candidate_id):
            raise ValueError("stored candidate detail identity mismatch")
        return self._envelope(stored, available=True, data=deepcopy(detail))

    def preview(self, candidate_id: UUID, *, assessed_at: datetime, request: dict, **identity):
        from options.alert_plans import AlertManagementPolicy, preview_indicative_alert_plan

        _aware(assessed_at)
        stored = self._stored(**identity)
        if assessed_at < datetime.fromisoformat(stored["published_at"]):
            raise ValueError("preview cannot precede retained publication")
        detail = stored.get("candidate_details", {}).get(str(candidate_id))
        if detail is None:
            return self._envelope(stored, available=False, reason="CANDIDATE_NOT_FOUND")
        management = request.get("management_policy")
        management = AlertManagementPolicy(**management) if management is not None else None
        result = preview_indicative_alert_plan(deepcopy(detail), decision_at=assessed_at,
            entry_deadline=request.get("entry_deadline"), exit_deadline=request.get("exit_deadline"),
            entry_limit=request.get("entry_limit"), management_policy=management)
        envelope = self._envelope(stored, available=True, data=result)
        envelope["generated_at"] = assessed_at
        return envelope

    def unavailable(self, contract: Literal["schedule", "candidate", "quotes", "preview"], *, requested_contract_ids=(), **identity):
        stored = self._stored(**identity)
        reasons = dict(schedule="DETECTOR_LAUNCH_NOT_CONFIGURED", candidate="CANDIDATE_UNAVAILABLE",
            quotes="ADVANCED_QUOTES_UNAVAILABLE", preview="ASSESSMENT_UNAVAILABLE")
        if contract not in reasons:
            raise ValueError("unsupported unavailable Alerts contract")
        data = {}
        if contract == "quotes":
            if any(type(identity) is not int or identity <= 0 for identity in requested_contract_ids):
                raise ValueError("quote contract IDs must be positive integers")
            data = dict(quotes=[], requested_contract_ids=list(requested_contract_ids), missing_contract_ids=list(requested_contract_ids),
                source="PERSISTED_ADVANCED_QUOTE_CURRENT", provider_fetch_performed=False, execution_permission=False)
        return self._envelope(stored, available=False, reason=reasons[contract], data=data)


def seal_offline_option_alert_publication(
    scope: ForwardMatrixScope, configuration: OptionRuntimeConfiguration, *, selected_at: datetime,
    cycles: tuple[ForwardAlertCycleSource, ...] = (),
    lineage: OptionsEvidenceLineage = OptionsEvidenceLineage.V1_FORWARD,
) -> OfflineOptionAlertPublication:
    require_options_lineage(lineage)
    _aware(selected_at)
    if (configuration.configuration_sha256 != scope.configuration_sha256
            or tuple(sorted(configuration.settings.underlyers)) != scope.underlyings
            or scope.selection.scope.provider_tier is not ProviderTier.MASSIVE_DEVELOPER
            or not scope.available_after <= selected_at <= scope.deadline or len(cycles) > 96):
        raise ValueError("Alerts projection scope, tier, deadline or cycle budget drift")
    if DetectorFamily.O1 not in scope.selection.policy.required_detectors:
        raise ValueError("Alerts O1 projection requires explicit manifest capability")
    ordered = tuple(sorted(cycles, key=lambda source: source.scope.cutoff))
    if len({source.scope.cutoff for source in ordered}) != len(ordered):
        raise ValueError("Alerts immutable cycle retry is ambiguous")
    if ordered and ordered[-1].scope != scope:
        raise ValueError("Alerts projection must pin its latest exact cycle scope")
    decoded = tuple(_decode_cycle(source, scope, configuration, selected_at, lineage) for source in ordered)
    if sum(len(records) for _, records, _ in decoded) > 10000:
        raise ValueError("Alerts evidence exceeds native review record bound")
    today = selected_at.astimezone(ZoneInfo("America/New_York")).date()
    if any(not today - timedelta(days=60) <= run.scheduled_cycle.astimezone(ZoneInfo("America/New_York")).date() <= today
            for run, _, _ in decoded):
        raise ValueError("Alerts review exceeds native sixty-day bound")
    dataset_id = forward_detector_dataset_id(scope, configuration)
    reader = _NativeReviewInputs(decoded, dataset_id)
    datasets = _dataset_index(decoded, dataset_id)
    evaluation_views, run_views = {}, {}
    for session in datasets["sessions"]:
        session_date = date.fromisoformat(session)
        session_cycles = tuple(value for value in decoded if value[0].scheduled_cycle.astimezone(ZoneInfo("America/New_York")).date() == session_date)
        for symbol in (None, *scope.underlyings):
            records = tuple(record for _, values, _ in session_cycles for record in values
                if symbol is None or record.observation.underlyer == symbol)
            ready = all(not reasons for _, _, dependencies in session_cycles
                for name, reasons in dependencies.items() if symbol is None or name == symbol)
            data = build_detector_evaluation_review(as_of=selected_at, session_date=session_date, dataset_id=dataset_id,
                detector="O1", underlyer=symbol, limit=200, repository=reader)
            for offset in range(200, data["total"], 200):
                data["rows"].extend(build_detector_evaluation_review(as_of=selected_at, session_date=session_date,
                    dataset_id=dataset_id, detector="O1", underlyer=symbol, limit=200, offset=offset, repository=reader)["rows"])
            orders = {field: {direction: [str(record.evaluation_id) for record in _sort_detector_records(records, field, direction)]
                for direction in ("asc", "desc")} for field in SORT_FIELDS}
            key = _view_key(session, symbol)
            evaluation_views[key] = dict(available=ready, data=data, orders=orders)
            run_views[key] = {mode: build_detector_alert_review(dataset_id=dataset_id, as_of=selected_at,
                scope=mode, session_date=session_date, detector="O1", underlyer=symbol, repository=reader)
                for mode in ("LATEST", "HISTORY")}
    payload = dict(contract=ALERT_REVIEW_CONTRACT, datasets=datasets, underlyers=scope.underlyings,
        evaluation_views=evaluation_views, run_views=run_views, published_at=selected_at.isoformat(),
        cutoff=scope.cutoff.isoformat(), deadline=scope.deadline.isoformat(), policy_sha256=configuration.policy_sha256,
        source_sha256s=tuple(source.publication.sha256 for source in ordered))
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(serialized.encode("ascii")) > 67108864:
        raise ValueError("Alerts original projection exceeds native payload bound")
    input_sha256 = canonical_sha256(dict(contract=ALERT_REVIEW_CONTRACT, scope=scope.sha256,
        cycles=tuple(dict(scope=source.scope.sha256, publication=source.publication.sha256,
            context=source.detector_context.content_sha256 if source.detector_context else None,
            **identity_binding(source)) for source in ordered)))
    return OfflineOptionAlertPublication(scope.cohort_id, scope.selection.scope.provider_tier, scope.sha256,
        dataset_id, serialized, input_sha256)


def indicative_detector_dataset_id(scope, configuration):
    return "forward:" + canonical_sha256(dict(base_dataset=forward_detector_dataset_id(scope, configuration),
        contract=INDICATIVE_REVIEW_CONTRACT, selector_sha256=DUAL_ORIGIN_SELECTOR_SHA256,
        qualification_policy_sha256=TECHNICAL_QUALIFICATION_POLICY.sha256))


def technical_detector_dataset_id(scope, configuration):
    return "forward:" + canonical_sha256(dict(base_dataset=forward_detector_dataset_id(scope, configuration),
        contract=TECHNICAL_REVIEW_CONTRACT, package_contract=TECHNICAL_PACKAGE_CONTEXT_CONTRACT,
        selector_sha256=DUAL_ORIGIN_SELECTOR_SHA256, qualification_policy_sha256=TECHNICAL_QUALIFICATION_POLICY.sha256,
        gamma_policy_sha256=configuration.gamma_policy_sha256,
        enabled_detectors=tuple(sorted(family.value for family in scope.selection.policy.required_detectors))))


class _IndicativeReviewInputs(_NativeReviewInputs):
    def __init__(self, cycles, dataset_id, originals):
        super().__init__(cycles, dataset_id)
        self.originals = originals

    def repeat_counts(self, *, dataset_id, records, as_of):
        if dataset_id != self.dataset_id:
            raise ValueError("indicative repeats cannot cross datasets")
        retained = tuple(record for _, values, _ in self.cycles for record in values
            if record.selection_status == "REPEAT" and record.selected_at <= as_of)
        result = {}
        for record in records:
            repeats = tuple(row for row in retained if row.first_candidate_id == record.candidate_id
                and row.recurrence_sha256 == record.recurrence_sha256)
            result[record.evaluation_id] = dict(repeats=len(repeats),
                last_seen_at=max((row.selected_at for row in repeats), default=None), confirmation_timeframes=[])
        return result

    def original_packages(self, records, *, as_of):
        result = {}
        for record in records:
            original = self.originals.get(str(record.candidate_id))
            if original is None or record.selected_at > as_of:
                raise ValueError("indicative original package receipt unavailable")
            result[str(record.candidate_id)] = deepcopy(original)
        return result


class _UnavailableMarks:
    def retained_plan_marks(self, *args, **kwargs):
        return dict(ready=False, rows={}, management_status_ready=False)


def _wire_candidate(candidate, legs):
    decoded = dict(candidate, candidate_identity=candidate["identity_sha256"],
        underlying=candidate["underlyer"], candidate_rank=candidate["rank"])
    for field in ("candidate_id", "matrix_id", "context_snapshot_id", "iv_context_id"):
        decoded[field] = UUID(candidate[field]) if candidate[field] else None
    for field in ("market_data_time", "observed_time", "valid_until"):
        decoded[field] = datetime.fromisoformat(candidate[field]) if candidate[field] else None
    decoded["expiration_date"] = date.fromisoformat(candidate["expiration_date"])
    for field in ("net_premium", "collateral_required", "capital_at_risk", "maximum_profit", "maximum_loss"):
        decoded[field] = Decimal(candidate[field]) if candidate[field] is not None else None
    decoded["breakevens"] = tuple(Decimal(value) for value in candidate["breakevens"])
    typed_legs = []
    for leg in legs:
        value = dict(leg, snapshot_id=UUID(leg["snapshot_id"]), expiration_date=date.fromisoformat(leg["expiration_date"]),
            source_market_time=datetime.fromisoformat(leg["source_market_time"]))
        for field in ("strike", "spot", "model_mark"):
            value[field] = Decimal(leg[field])
        typed_legs.append(value)
    return retained_candidate(decoded, typed_legs)


def _bound_wire_leg(leg, captured):
    if captured is None or captured["contract_id"] != leg["contract_id"]:
        raise ValueError("indicative original leg snapshot unavailable")
    aliases = dict(multiplier="shares_per_contract", source_market_time="mark_market_data_time")
    for field in ("contract_id", "contract_ticker", "contract_type", "expiration_date", "strike", "spot", "model_mark",
            "local_iv", "local_delta", "local_gamma", "local_theta_per_day", "local_vega_per_vol_point",
            "local_rho_per_rate_point", "time_to_expiration_years", "risk_free_rate", "dividend_yield",
            "mark_source", "model_version", "quality_flags", "valuation_policy_version", "valuation_policy_sha256",
            "multiplier", "source_market_time"):
        actual, expected = leg[field], captured[aliases.get(field, field)]
        if (datetime.fromisoformat(actual) != datetime.fromisoformat(expected) if field == "source_market_time" else actual != expected):
            raise ValueError("indicative original candidate leg changed from its exact snapshot")
    return dict(leg, batch_id=captured["batch_id"], normalized_payload_sha256=captured["normalized_payload_sha256"],
        day_volume=captured["day_volume"], open_interest=captured["open_interest"], quote_bid=None, quote_ask=None)


def _wire_snapshot(row):
    from options.repositories.snapshots import _snapshot

    values = dict(row, underlying=row["underlyer"])
    for field in ("snapshot_id", "batch_id"):
        values[field] = UUID(values[field])
    values["expiration_date"] = date.fromisoformat(values["expiration_date"])
    for field in ("expiration_cutoff", "spot_market_data_time", "mark_market_data_time", "market_data_time",
            "first_observed_at", "revised_observed_at"):
        values[field] = datetime.fromisoformat(values[field]) if values[field] is not None else None
    for field in ("strike", "spot", "bid", "ask", "midpoint", "display_mark", "model_mark", "intrinsic_value",
            "extrinsic_value", "single_contract_breakeven"):
        values[field] = Decimal(values[field]) if values[field] is not None else None
    return _snapshot(values)


def _decode_credit_observations(payload, source, base_run, snapshots, configuration):
    candidates = {row["candidate_id"]: row for row in payload.get("o3_candidates", ())}
    observations = tuple(O3CreditObservation.model_validate(row) for row in payload.get("o3_observations", ()))
    if len(candidates) != len(payload.get("o3_candidates", ())) or len(observations) != len(candidates):
        raise ValueError("O3 native candidate receipts are ambiguous")
    stocks = dict(source.detector_context.stock_sources)
    activity = {row.contract_id: row for row in source.detector_context.activity_sources}
    for observation in observations:
        candidate = candidates.get(str(observation.candidate_id))
        if ("O3" not in payload["enabled_detectors"] or candidate is None
                or payload["detector_dependencies"]["O3"] or observation.decision_at != base_run.selected_at
                or observation.scheduled_cycle != source.scope.cutoff
                or str(observation.matrix_id) != payload["matrix_id"]
                or candidate["identity_sha256"] != observation.candidate_identity_sha256
                or candidate["policy_sha256"] != configuration.strategy_policy_sha256
                or candidate["underlyer"] != observation.underlyer or candidate["matrix_id"] != payload["matrix_id"]):
            raise ValueError("O3 original candidate identity or clocks drifted")
        legs = [_bound_wire_leg(leg, snapshots.get(leg["snapshot_id"])) for leg in candidate["legs"]]
        contract = _wire_candidate(candidate, legs)
        bound = activity.get(observation.activity_contract_id)
        if bound is None or bound.underlyer != observation.underlyer:
            raise ValueError("O3 original activity source is unavailable")
        decision = assess_options_credit_canonical_trend(bound, stocks.get(observation.underlyer),
            direction=observation.direction, market_cutoff=source.scope.cutoff, decision_at=base_run.selected_at)
        assessment = assess_option_package(contract, valuation_policy_sha256=configuration.valuation_policy_sha256)
        if assessment.status != "READY":
            raise ValueError("O3 native credit economics are invalid")
        recomputed = assess_o3_credit(contract, decision, activity_contract_id=bound.contract_id,
            snapshots=tuple(_wire_snapshot(snapshots[leg["snapshot_id"]]) for leg in candidate["legs"]),
            structural_invalidation=observation.structural_invalidation, scheduled_cycle=source.scope.cutoff)
        if recomputed != observation:
            raise ValueError("O3 retained credit observation differs from exact native sources")
    return observations


def _decode_indicative_packages(source, base_run, configuration):
    contexts = source.package_contexts
    if len(contexts) > 13 or len({receipt.matrix_id for receipt in contexts}) != len(contexts):
        raise ValueError("indicative package matrix receipts are ambiguous or exceed manifest bounds")
    base = source.publication.read(cohort_id=source.scope.cohort_id, provider_tier=source.provider_tier,
        scope_sha256=source.scope.sha256)
    snapshots = {row["snapshot_id"]: row for matrix in base["screener"] for row in matrix["snapshots"]}
    source_matrices = dict(base_run.source_matrices)
    by_matrix = {matrix: symbol for symbol, matrix in base_run.source_matrices}
    packages, plans, originals, ready, credits = [], {}, {}, set(), []
    time_fields = {"expiration_cutoff", "spot_market_data_time", "mark_market_data_time", "market_data_time",
        "first_observed_at", "revised_observed_at"}
    for receipt in contexts:
        if receipt.matrix_id not in by_matrix:
            raise ValueError("indicative package receipt is outside exact source matrices")
        payload = receipt.read(scope_sha256=source.scope.sha256, matrix_id=receipt.matrix_id)
        technical = payload["contract"] == TECHNICAL_PACKAGE_CONTEXT_CONTRACT
        if (payload["contract"] not in (PACKAGE_CONTEXT_CONTRACT, TECHNICAL_PACKAGE_CONTEXT_CONTRACT) or payload["scope_sha256"] != source.scope.sha256
                or payload["matrix_id"] != str(receipt.matrix_id) or payload["selected_at"] != base_run.selected_at.isoformat()
                or source.detector_context is None or payload["detector_context_sha256"] != source.detector_context.content_sha256
                or payload["strategy_policy_sha256"] != configuration.strategy_policy_sha256
                or payload["valuation_policy_sha256"] != configuration.valuation_policy_sha256
                or payload["qualification_policy_sha256"] != TECHNICAL_QUALIFICATION_POLICY.sha256
                or payload["execution_permission"] is not False or payload["original_page_contracts"] is not False
                or payload.get("display_available", False) != bool(payload["qualified_indicative"] or payload.get("o3_observations"))):
            raise ValueError("indicative display receipt policy/clock/capability drift")
        if technical:
            enabled = tuple(sorted(family.value for family in source.scope.selection.policy.required_detectors))
            if (tuple(payload["enabled_detectors"]) != enabled or set(enabled) - {"O1", "O3", "S1", "S2"}
                    or set(payload["detector_dependencies"]) != set(enabled)):
                raise ValueError("technical package detector capability drift")
            if any(not reasons for reasons in payload["detector_dependencies"].values()):
                ready.add(by_matrix[receipt.matrix_id])
            if (payload["gamma_context"]["role"] != "ADVISORY"
                    or payload["gamma_context"]["policy_sha256"] != configuration.gamma_policy_sha256):
                raise ValueError("technical gamma context policy drift")
            trade_context = payload["trade_context"]
            if (trade_context["contract"] != TRADE_CONTEXT_CONTRACT or trade_context["role"] != "ADVISORY"
                    or trade_context["chain_volume_used_as_tape"] is not False
                    or trade_context["aggressor_side"] is not None):
                raise ValueError("technical trade context contract drift")
            credits.extend(_decode_credit_observations(payload, source, base_run, snapshots, configuration))
        elif not payload.get("display_available", False):
            continue
        else:
            ready.add(by_matrix[receipt.matrix_id])
        for row in payload["qualified_indicative"]:
            package_type = (QualifiedCanonicalStockPackage if row["package"]["schema_version"] == "dual_origin_qualified_package_v4"
                else QualifiedTechnicalPackage)
            package = package_type.model_validate(row["package"])
            plan = FrozenOptionAlertPlan(row["plan_payload_json"])
            content = json.loads(plan.payload_json)
            candidate = row["candidate"]
            if (package.detector_id not in (payload["enabled_detectors"] if technical else ("O1",)) or package.matrix_id != receipt.matrix_id
                    or source_matrices.get(package.underlyer) != package.matrix_id
                    or package.scheduled_cycle != source.scope.cutoff or package.decision_at != base_run.selected_at
                    or package.plan_sha256 != plan.sha256 or row["plan_sha256"] != plan.sha256
                    or row["candidate_id"] != str(package.candidate_id) or candidate["candidate_id"] != str(package.candidate_id)
                    or candidate["identity_sha256"] != package.candidate_identity_sha256
                    or candidate["matrix_id"] != str(receipt.matrix_id) or candidate["underlyer"] != package.underlyer
                    or candidate["policy_sha256"] != configuration.strategy_policy_sha256
                    or candidate["execution_eligibility"] is not None
                    or content["execution_permission"] is not False
                    or any(check["status"] not in ("CLEAR", "NOT_APPLICABLE") for check in content["event_horizon"]["checks"])):
                raise ValueError("indicative original package/plan does not reconcile with its native scope")
            leg_sources = {snapshot["snapshot_id"]: snapshot for snapshot in row["source_snapshots"]}
            if len(leg_sources) != len(row["source_snapshots"]):
                raise ValueError("indicative snapshot revisions are ambiguous")
            legs = []
            for leg in candidate["legs"]:
                captured = snapshots.get(leg["snapshot_id"])
                retained = leg_sources.get(leg["snapshot_id"])
                if captured is None or retained is None or captured["contract_id"] != leg["contract_id"]:
                    raise ValueError("indicative original leg snapshot unavailable")
                for key, value in captured.items():
                    actual = retained.get(key)
                    if key in time_fields and value is not None and actual is not None:
                        matches = datetime.fromisoformat(value) == datetime.fromisoformat(actual)
                    else:
                        matches = value == actual
                    if not matches:
                        raise ValueError("indicative retained source snapshot changed")
                legs.append(_bound_wire_leg(leg, captured))
            candidate_contract = _wire_candidate(candidate, legs)
            assessment = assess_option_package(candidate_contract, valuation_policy_sha256=configuration.valuation_policy_sha256)
            if assessment.package_terms_sha256 != content["package_terms_sha256"]:
                raise ValueError("indicative native candidate economics changed from qualified terms")
            originals[str(package.candidate_id)] = dict(status="AVAILABLE", basis="ORIGINAL_CANDIDATE_SNAPSHOTS",
                structure_type=candidate["structure_type"], expiration_date=candidate["expiration_date"],
                calendar_dte=(date.fromisoformat(candidate["expiration_date"]) - source.scope.selection.session_date).days,
                market_data_time=candidate["market_data_time"], observed_time=candidate["observed_time"],
                net_premium=candidate["net_premium"], capital_at_risk=candidate["capital_at_risk"],
                maximum_loss=candidate["maximum_loss"], maximum_profit=candidate["maximum_profit"],
                breakevens=candidate["breakevens"], legs=legs)
            if technical:
                from options.strategies.registry import REGISTRY_BY_NAME

                registration = REGISTRY_BY_NAME[candidate["strategy_name"]]
                originals[str(package.candidate_id)]["detail"] = dict(
                    candidate=dict(candidate, underlying=candidate["underlyer"], candidate_rank=candidate["rank"],
                        candidate_identity=candidate["identity_sha256"], display_name=registration.display_name,
                        presentation_metadata={}, source_contract_id=candidate["legs"][0]["contract_id"],
                        source_contract_ticker=candidate["legs"][0]["contract_ticker"], context_status="DEGRADED",
                        trend_state=None, earnings_blackout_state="UNAVAILABLE", fed_blackout_state="UNAVAILABLE",
                        quote_spread_state="UNAVAILABLE", context_reason_codes=["PACKAGE_CONTEXT_UNAVAILABLE"],
                        normalized_legs=candidate["legs"], decision_context={}, evidence_rank_components=candidate["rank_components"],
                        trigger_values=candidate["primary_evidence"], evidence_quality_flags=[],
                        signal_id=None, signal_status=None, signal_blocked_reasons=None),
                    legs=legs, source_snapshots=[dict(snapshot, underlying=snapshot["underlyer"])
                        for snapshot in row["source_snapshots"]], scenarios=[], execution_gates=[],
                    market_event_evidence=[], event_coverage_evidence=[],
                    quote_liquidity="NOT_AVAILABLE", execution_mode="READ_ONLY_RESEARCH",
                    detail_basis="EXACT_RETAINED_CANDIDATE", section_availability=dict(
                        scenarios="NOT_RETAINED", execution_gates="NOT_RETAINED", events="NOT_RETAINED"),
                    valuation_assumption=payload["valuation_assumption"],
                    advisory_context=dict(gamma=payload["gamma_context"], trades=payload["trade_context"]))
            packages.append(package)
            plans[plan.sha256] = plan
    return tuple(packages), plans, originals, ready, tuple(credits)


def _record_underlyer(record):
    return record.package.underlyer if isinstance(record, DetectorSelectionEvidence) else record.observation.underlyer


def seal_offline_indicative_alert_publication(
    scope: ForwardMatrixScope, configuration: OptionRuntimeConfiguration, *, selected_at: datetime,
    cycles: tuple[ForwardAlertCycleSource, ...], prior_publications: tuple[OfflineOptionAlertPublication, ...] = (),
    lineage: OptionsEvidenceLineage = OptionsEvidenceLineage.V1_FORWARD,
) -> OfflineOptionAlertPublication:
    require_options_lineage(lineage)
    _aware(selected_at)
    if (not scope.available_after <= selected_at <= scope.deadline or len(cycles) != 1 or len(prior_publications) > 95
            or configuration.configuration_sha256 != scope.configuration_sha256
            or tuple(sorted(configuration.settings.underlyers)) != scope.underlyings):
        raise ValueError("indicative display scope/configuration/deadline drift")
    ordered = tuple(sorted(cycles, key=lambda source: source.scope.cutoff))
    if not ordered or ordered[-1].scope != scope or len({source.scope.cutoff for source in ordered}) != len(ordered):
        raise ValueError("indicative display requires its exact latest immutable cycle")
    contracts = {json.loads(receipt.payload_json)["contract"] for source in ordered for receipt in source.package_contexts}
    technical = TECHNICAL_PACKAGE_CONTEXT_CONTRACT in contracts
    if technical and contracts != {TECHNICAL_PACKAGE_CONTEXT_CONTRACT}:
        raise ValueError("technical reviews cannot mix legacy package contracts")
    contract = TECHNICAL_REVIEW_CONTRACT if technical else INDICATIVE_REVIEW_CONTRACT
    dataset_id = technical_detector_dataset_id(scope, configuration) if technical else indicative_detector_dataset_id(scope, configuration)
    enabled = tuple(sorted(family.value for family in scope.selection.policy.required_detectors)) if technical else ("O1",)
    model_ready = {family: set() for family in enabled}
    model_dependencies = {family: {} for family in enabled}
    for source in ordered:
        by_matrix = dict((matrix, symbol) for symbol, matrix in json.loads(source.publication.payload_json)["alerts"]["run"]["source_matrices"])
        for receipt in source.package_contexts:
            retained_package = receipt.read(scope_sha256=source.scope.sha256, matrix_id=receipt.matrix_id)
            if technical:
                symbol = by_matrix[str(receipt.matrix_id)]
                for family, reasons in retained_package["detector_dependencies"].items():
                    model_dependencies[family][symbol] = reasons
                    if not reasons:
                        model_ready[family].add(symbol)
    decoded, originals, prior, prior_o3, prior_o1 = [], {}, {}, set(), set()
    seen_cycles = set()
    for publication in sorted(prior_publications, key=lambda item: json.loads(item.payload_json)["cutoff"]):
        if (publication.cohort_id != scope.cohort_id or publication.provider_tier is not scope.selection.scope.provider_tier
                or publication.dataset_id != dataset_id):
            raise ValueError("indicative history cannot mix cohort/tier/policy/dataset scopes")
        retained = publication.read(cohort_id=scope.cohort_id, provider_tier=scope.selection.scope.provider_tier,
            scope_sha256=publication.scope_sha256)
        if (retained["contract"] != contract
                or retained["configuration_sha256"] != configuration.configuration_sha256
                or retained["policy_sha256"] != configuration.policy_sha256
                or datetime.fromisoformat(retained["published_at"]) > selected_at
                or datetime.fromisoformat(retained["cutoff"]) >= scope.cutoff):
            raise ValueError("indicative history requires older sealed original selections")
        originals.update(retained["original_packages"])
        for cycle in retained["native_cycles"]:
            run = load_detector_run(cycle["run_payload"])
            records = tuple(load_evaluation_evidence(value) for value in cycle["record_payloads"])
            if (run.scheduled_cycle in seen_cycles or run.dataset_id != dataset_id or run.scheduled_cycle >= scope.cutoff
                    or run.configuration_sha256 != configuration.configuration_sha256 or run.selected_at > selected_at
                    or tuple(sorted(((record.evaluation_id, record.sha256) for record in records), key=lambda item: str(item[0]))) != run.record_sha256s):
                raise ValueError("indicative immutable history run/evidence drift")
            seen_cycles.add(run.scheduled_cycle)
            decoded.append((run, records, dict(run.unavailable_underlyers)))
            prior.update((record.recurrence_sha256, record.package) for record in records
                if isinstance(record, DetectorSelectionEvidence) and record.selection_status == "SELECTED")
            prior_o3.update(record.recurrence_sha256 for record in records if isinstance(record, O3CreditEvaluationEvidence))
            prior_o1.update(record.recurrence_sha256 for record in records if isinstance(record, O1IndicatorEvaluationEvidence))
    for source in ordered:
        base_run, observations, dependencies = _decode_cycle(source, scope, configuration, selected_at, lineage)
        packages, plans, captured, ready, credits = _decode_indicative_packages(source, base_run, configuration)
        if not technical:
            ready = {symbol for symbol in ready if not dependencies.get(symbol)}
        if not ready and not technical:
            continue
        matrices = {symbol: matrix for symbol, matrix in base_run.source_matrices if technical or symbol in ready}
        if technical and set(matrices) != set(scope.underlyings):
            raise ValueError("technical selection requires the complete configured matrix cycle")
        selected_o3, o3_rejections = select_o3_credit_observations(credits, prior_o3)
        selection = select_dual_origin_packages(packages, prior, decision_at=selected_at,
            scheduled_cycle=source.scope.cutoff, expected_underlyers=(scope.underlyings if technical else tuple(sorted(ready))), completed_matrices=matrices,
            maximum_new_alerts=20 - len(selected_o3))
        if selection["evidence"]["rejections"].get("ORIGINAL_ENTRY_DEADLINE_OR_DECISION_CLOCK"):
            raise ValueError("indicative selection missed its original deadline; no backdating permitted")
        records = (*build_selection_evidence(selection, plans, dataset_id=dataset_id, selected_at=selected_at),
            *build_o3_credit_evidence(selected_o3, dataset_id=dataset_id, selected_at=selected_at),
            *(build_o1_indicator_evidence(tuple(record.observation for record in observations
                if record.recurrence_sha256 not in prior_o1), dataset_id=dataset_id, selected_at=selected_at) if technical else ()))
        originals.update(captured)
        unavailable = (() if technical else tuple((symbol, ("PACKAGE_CONTEXT_UNAVAILABLE",))
            for symbol in scope.underlyings if symbol not in ready))
        matrix_inputs = tuple(row for row in source.detector_context.inputs.matrices if technical or row["underlying"] in ready)
        run = build_detector_run(configuration=configuration, dataset_id=dataset_id, scheduled_cycle=source.scope.cutoff,
            selected_at=selected_at, matrices=matrix_inputs, records=records,
            rejections=dict(Counter(selection["evidence"]["rejections"]) + Counter(o3_rejections)), unavailable_underlyers=unavailable)
        decoded.append((run, records, {symbol: reasons for symbol, reasons in unavailable}))
        prior.update((record.recurrence_sha256, record.package) for record in records
            if isinstance(record, DetectorSelectionEvidence) and record.selection_status == "SELECTED")
        prior_o3.update(record.recurrence_sha256 for record in records if isinstance(record, O3CreditEvaluationEvidence))
        prior_o1.update(record.recurrence_sha256 for record in records if isinstance(record, O1IndicatorEvaluationEvidence))
    decoded = tuple(decoded)
    today = selected_at.astimezone(ZoneInfo("America/New_York")).date()
    if (len(decoded) > 96 or sum(len(records) for _, records, _ in decoded) > 10000
            or any(not today - timedelta(days=60) <= run.scheduled_cycle.astimezone(ZoneInfo("America/New_York")).date() <= today
                for run, _, _ in decoded)):
        raise ValueError("indicative history exceeds native cycle/record/session bounds")
    reader = _IndicativeReviewInputs(decoded, dataset_id, originals)
    datasets = _dataset_index(decoded, dataset_id)
    evaluation_views, run_views = {}, {}
    for session in datasets["sessions"]:
        session_date = date.fromisoformat(session)
        for symbol, family in ((symbol, family) for symbol in (None, *scope.underlyings)
            for family in ((None, *enabled) if technical else (None,))):
            records = tuple(record for run, values, _ in decoded if run.scheduled_cycle.date() == session_date
            for record in values if (symbol is None or _record_underlyer(record) == symbol)
            and (family is None or record.detector_id == family))
            eligible_runs = tuple(run for run, _, _ in decoded if run.scheduled_cycle.date() == session_date
                and (symbol is None or symbol in dict(run.source_matrices)))
            available = bool(eligible_runs) and (not technical or family is None
                or bool(model_ready[family] if symbol is None else model_ready[family] & {symbol})
                or family == "O1" and any(isinstance(record, O1IndicatorEvaluationEvidence) for record in records))
            data = build_detector_evaluation_review(as_of=selected_at, session_date=session_date, dataset_id=dataset_id,
                detector=family if technical else "O1", underlyer=symbol, limit=200, repository=reader)
            for offset in range(200, data["total"], 200):
                data["rows"].extend(build_detector_evaluation_review(as_of=selected_at, session_date=session_date,
                    dataset_id=dataset_id, detector=family if technical else "O1", underlyer=symbol, limit=200, offset=offset, repository=reader)["rows"])
            orders = {field: {direction: [str(record.evaluation_id) for record in _sort_detector_records(records, field, direction)]
                for direction in ("asc", "desc")} for field in SORT_FIELDS}
            key = _view_key(session, symbol, family)
            evaluation_views[key] = dict(available=available, data=data, orders=orders)
            views = {}
            for mode in ("LATEST", "HISTORY"):
                data = build_detector_alert_review(dataset_id=dataset_id, as_of=selected_at, scope=mode,
                    session_date=session_date, detector=family if technical else "O1", underlyer=symbol, limit=200, repository=reader,
                    source_repository=reader, mark_repository=_UnavailableMarks(), valuation_policy=configuration.valuation_policy)
                active_ids = {row["run_id"] for row in data.get("runs", ())}
                visible = tuple(record for record in records if str(record.run_id) in active_ids and record.selection_status == "SELECTED")
                for offset in range(200, data["total"], 200):
                    data["rows"].extend(build_detector_alert_review(dataset_id=dataset_id, as_of=selected_at, scope=mode,
                        session_date=session_date, detector=family if technical else "O1", underlyer=symbol, limit=200, offset=offset, repository=reader,
                        source_repository=reader, mark_repository=_UnavailableMarks(), valuation_policy=configuration.valuation_policy)["rows"])
                display_orders = {field: {direction: [str(record.evaluation_id) for record in _sort_detector_records(visible, field, direction)]
                    for direction in ("asc", "desc")} for field in SORT_FIELDS}
                views[mode] = dict(available=available, data=data, orders=display_orders)
            run_views[key] = views
    market_time = max((run.market_time for run, _, _ in decoded), default=None)
    observed_time = max((run.observed_time for run, _, _ in decoded), default=None)
    payload = dict(contract=contract, datasets=datasets, underlyers=scope.underlyings,
        evaluation_views=evaluation_views, run_views=run_views, published_at=selected_at.isoformat(),
        market_time=market_time.isoformat() if market_time else None, observed_time=observed_time.isoformat() if observed_time else None,
        cutoff=scope.cutoff.isoformat(), deadline=scope.deadline.isoformat(), policy_sha256=configuration.policy_sha256,
        configuration_sha256=configuration.configuration_sha256, original_packages=originals,
        native_cycles=tuple(dict(run_payload=run.canonical_json(), record_payloads=tuple(record.canonical_json() for record in records))
            for run, records, _ in decoded),
        display_mode="INDICATIVE_RESEARCH", complete_event_context_available=False, execution_permission=False,
        **(dict(enabled_detectors=enabled, detector_dependencies=model_dependencies,
            valuation_assumption=("EXPLICIT_ZERO_YIELD_RESEARCH" if configuration.valuation_policy.policy_version ==
                "option_valuation_zero_yield_research_v1" else "POLICY_BOUND_SOURCE_INPUTS"),
            valuation_policy=configuration.valuation_policy.model_dump(mode="json"),
            valuation_policy_sha256=configuration.valuation_policy_sha256,
            candidate_details={identity: original["detail"] for identity, original in originals.items() if "detail" in original},
            context_availability={family: "AVAILABLE_INDICATIVE" if model_ready.get(family) else "UNAVAILABLE"
            for family in ("O1", "O2", "O3", "S1", "S2")}) if technical else {}))
    serialized = json.dumps(payload, default=lambda value: value.isoformat() if isinstance(value, (date, datetime)) else str(value),
        sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(serialized.encode("ascii")) > 67108864:
        raise ValueError("indicative original review exceeds native payload bound")
    input_sha256 = canonical_sha256(dict(contract=contract, scope=scope.sha256,
        prior_publication_sha256s=tuple(sorted(publication.sha256 for publication in prior_publications)),
        cycles=tuple(dict(source=source.publication.sha256,
            package_contexts=tuple(sorted(canonical_sha256(receipt) for receipt in source.package_contexts)),
            **identity_binding(source)) for source in ordered)))
    return OfflineOptionAlertPublication(scope.cohort_id, scope.selection.scope.provider_tier, scope.sha256,
        dataset_id, serialized, input_sha256)