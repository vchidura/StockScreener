"""Pure dual-origin research evidence; no collection, publication or execution."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import ClassVar, Literal
from uuid import NAMESPACE_URL, UUID, uuid5
from zoneinfo import ZoneInfo

from pydantic import AwareDatetime, Field, model_validator

from equity.behavior import Contract, DEFINITION_V1_SHA256, Name, OPTIONS_SWING_PROFILE, Sha256, StockBehaviorSnapshot
from equity.behavior_setup import (
    DirectSetupSourcePolicy, DirectStockSetupEvidence, DirectResumptionSourcePolicy,
    DirectStockResumptionEvidence, resolve_direct_setup_policy,
)
from options.calendar import OptionExchangeCalendar
from options.outcome_contracts import OptionPackageAssessment, assess_option_package
from options.stock_behavior_gates import StockBehaviorGate, _metric_gate
from options.strategies.domain import OptionCandidate, OptionSide, canonical_json


class ActivityPolicy(Contract):
    version: Literal["option_participation_origin_v1"] = "option_participation_origin_v1"
    minimum_volume_oi_ratio: Literal[3] = 3
    maximum_source_age_seconds: Literal[1800] = 1800
    oi_date_basis: Literal["PREVIOUS_SESSION_BY_PRODUCER_POLICY"] = "PREVIOUS_SESSION_BY_PRODUCER_POLICY"
    direction_basis: Literal["NONE_ACTIVITY_IS_NOT_AGGRESSOR_DIRECTION"] = "NONE_ACTIVITY_IS_NOT_AGGRESSOR_DIRECTION"
    episode_basis: Literal["POLICY_SECURITY_CONTRACT_SESSION"] = "POLICY_SECURITY_CONTRACT_SESSION"
    execution_permission: Literal[False] = False


ACTIVITY_POLICY = ActivityPolicy()


class StockAlignmentPolicy(Contract):
    version: Literal["option_participation_stock_alignment_v1"] = "option_participation_stock_alignment_v1"
    activity_policy_sha256: Literal[ACTIVITY_POLICY.sha256] = ACTIVITY_POLICY.sha256
    behavior_definition_sha256: Literal[DEFINITION_V1_SHA256] = DEFINITION_V1_SHA256
    behavior_profile_sha256: Literal[OPTIONS_SWING_PROFILE.sha256] = OPTIONS_SWING_PROFILE.sha256
    confirmation_basis: Literal["STOCK_SLOPE_SIGN_AND_LIQUIDITY_PRESENCE"] = "STOCK_SLOPE_SIGN_AND_LIQUIDITY_PRESENCE"
    intervals: tuple[Literal["1d", "1h", "30m"], ...] = ("1d", "1h", "30m")
    execution_permission: Literal[False] = False


STOCK_ALIGNMENT_POLICY = StockAlignmentPolicy()


class StockFirstPolicy(Contract):
    version: Literal["stock_acceptance_option_participation_v1"] = "stock_acceptance_option_participation_v1"
    source_policy_sha256: Sha256
    setup_schema: Literal["stock_setup_direct_evidence_v1"] = "stock_setup_direct_evidence_v1"
    setup_detector: Literal["range_breakout_acceptance_intraday_v2"] = "range_breakout_acceptance_intraday_v2"
    setup_interval: Literal["1h"] = "1h"
    target_horizon: Literal["SAME_SESSION"] = "SAME_SESSION"
    activity_policy_sha256: Literal[ACTIVITY_POLICY.sha256] = ACTIVITY_POLICY.sha256
    confirmation_basis: Literal["MATCHED_CONTRACT_PARTICIPATION_NOT_AGGRESSOR_DIRECTION"] = "MATCHED_CONTRACT_PARTICIPATION_NOT_AGGRESSOR_DIRECTION"
    execution_permission: Literal[False] = False


class StockResumptionPolicy(StockFirstPolicy):
    version: Literal["stock_resumption_option_participation_v1"] = "stock_resumption_option_participation_v1"
    setup_schema: Literal["stock_resumption_direct_evidence_v1"] = "stock_resumption_direct_evidence_v1"
    setup_detector: Literal["relative_trend_resumption_intraday_v1"] = "relative_trend_resumption_intraday_v1"


class ActivitySource(Contract):
    schema_version: Literal["option_participation_source_v1"] = "option_participation_source_v1"
    security_id: UUID
    underlyer: Name
    contract_id: int = Field(gt=0, strict=True)
    contract_ticker: Name
    contract_type: Literal["CALL", "PUT"]
    expiration_date: date
    expiration_cutoff: AwareDatetime
    snapshot_id: UUID
    snapshot_payload_sha256: Sha256
    batch_id: UUID
    matrix_id: UUID
    configuration_sha256: Sha256
    market_policy_sha256: Sha256
    volume_session: date
    day_volume: int | None = Field(default=None, ge=0, strict=True)
    open_interest: int | None = Field(default=None, ge=0, strict=True)
    oi_settlement_session: date | None = None
    oi_observed_at: AwareDatetime | None = None
    oi_date_basis: Literal["PREVIOUS_SESSION_BY_PRODUCER_POLICY"] = "PREVIOUS_SESSION_BY_PRODUCER_POLICY"
    market_time: AwareDatetime
    observed_at: AwareDatetime
    recorded_at: AwareDatetime
    received_at: AwareDatetime
    revised_observed_at: AwareDatetime | None = None

    @model_validator(mode="after")
    def validate_source(self):
        if not self.market_time <= self.observed_at <= self.recorded_at <= self.received_at:
            raise ValueError("activity source requires causal recorded and read receipts")
        if self.market_time.astimezone(ZoneInfo("America/New_York")).date() != self.volume_session:
            raise ValueError("activity volume must belong to the source session")
        if self.expiration_date < self.volume_session or self.expiration_cutoff <= self.market_time:
            raise ValueError("activity contract expired at source")
        if self.revised_observed_at is not None and not self.observed_at <= self.revised_observed_at <= self.received_at:
            raise ValueError("activity revision receipt is not causal")
        if self.oi_observed_at is not None and self.oi_observed_at > self.received_at:
            raise ValueError("open interest was not available at read receipt")
        return self


class ActivityFinding(Contract):
    schema_version: Literal["option_participation_finding_v1"] = "option_participation_finding_v1"
    detector_id: Literal["O1"] = "O1"
    origin: Literal["OPTIONS_FIRST"] = "OPTIONS_FIRST"
    detector_policy_sha256: Sha256 = ACTIVITY_POLICY.sha256
    episode_id: UUID
    source_sha256: Sha256
    decision_at: AwareDatetime
    market_cutoff: AwareDatetime
    valid_until: AwareDatetime
    volume_oi_ratio: Decimal | None
    disposition: Literal["DETECTED", "NOT_DETECTED", "UNAVAILABLE"]
    reasons: tuple[Name, ...]
    direction: None = None
    publication_permission: Literal[False] = False
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_finding(self):
        if self.detector_policy_sha256 != ACTIVITY_POLICY.sha256:
            raise ValueError("unsupported activity policy")
        if self.market_cutoff > self.decision_at:
            raise ValueError("activity decision must follow market cutoff")
        if self.volume_oi_ratio is not None and (not self.volume_oi_ratio.is_finite() or self.volume_oi_ratio < 0):
            raise ValueError("activity ratio must be finite and nonnegative")
        if self.disposition == "DETECTED" and (
            self.reasons or self.volume_oi_ratio is None or self.volume_oi_ratio < ACTIVITY_POLICY.minimum_volume_oi_ratio
            or self.decision_at >= self.valid_until
        ):
            raise ValueError("detected activity requires timely positive-OI evidence")
        if self.disposition != "DETECTED" and not self.reasons:
            raise ValueError("non-detection requires reasons")
        return self


def detect_option_participation(
    source: ActivitySource, *, market_cutoff: datetime, decision_at: datetime,
    calendar: OptionExchangeCalendar | None = None,
) -> ActivityFinding:
    if market_cutoff.utcoffset() is None or decision_at.utcoffset() is None or market_cutoff > decision_at:
        raise ValueError("activity cutoffs must be aware and causal")
    source = ActivitySource.model_validate_json(source.canonical_json())
    calendar = calendar or OptionExchangeCalendar()
    session_close = calendar.session_close(source.volume_session)
    if calendar.session_for_slot(source.market_time) != source.volume_session:
        raise ValueError("activity market time is not an exchange slot")
    previous_session = calendar.previous_session(source.volume_session)
    valid_until = min(session_close, source.expiration_cutoff,
        source.market_time + timedelta(seconds=ACTIVITY_POLICY.maximum_source_age_seconds))
    reasons = []
    if source.market_time > market_cutoff or source.received_at > decision_at:
        reasons.append("ACTIVITY_NOT_AVAILABLE_AT_DECISION")
    if source.market_time > session_close:
        reasons.append("ACTIVITY_OUTSIDE_SOURCE_SESSION")
    if decision_at >= valid_until:
        reasons.append("ACTIVITY_EXPIRED")
    if source.revised_observed_at is not None:
        reasons.append("REVISED_ACTIVITY_SOURCE_REQUIRES_VERSIONED_ADAPTER")
    if source.open_interest is None or source.open_interest <= 0:
        reasons.append("POSITIVE_OPEN_INTEREST_REQUIRED")
    if source.oi_settlement_session != previous_session or source.oi_observed_at is None:
        reasons.append("DATED_OPEN_INTEREST_UNAVAILABLE")
    elif source.oi_observed_at > source.observed_at:
        reasons.append("OPEN_INTEREST_AFTER_SNAPSHOT")
    if source.day_volume is None:
        reasons.append("DAY_VOLUME_UNAVAILABLE")
    ratio = None
    if not reasons:
        ratio = Decimal(source.day_volume) / Decimal(source.open_interest)
    disposition = "UNAVAILABLE" if reasons else "DETECTED"
    if ratio is not None and ratio < ACTIVITY_POLICY.minimum_volume_oi_ratio:
        disposition = "NOT_DETECTED"
        reasons.append("PARTICIPATION_BELOW_BASELINE")
    episode_id = uuid5(NAMESPACE_URL,
        f"option-participation:{ACTIVITY_POLICY.sha256}:{source.security_id}:{source.contract_id}:{source.volume_session}")
    return ActivityFinding(episode_id=episode_id, source_sha256=source.sha256,
        decision_at=decision_at, market_cutoff=market_cutoff, valid_until=valid_until,
        volume_oi_ratio=ratio, disposition=disposition, reasons=tuple(reasons))


def bind_activity_source(*, snapshot, open_interest_record, lineage, security,
                         recorded_at, received_at, calendar=None) -> ActivitySource:
    calendar = calendar or OptionExchangeCalendar()
    if (not security.active or security.ticker != snapshot.underlyer
            or security.effective_from > snapshot.market_data_time or security.observed_at > received_at):
        raise ValueError("dated stock security does not match the option source")
    if (snapshot.underlyer != lineage.underlying or snapshot.market_data_time > lineage.market_time
            or snapshot.first_observed_at > lineage.observed_time
            or snapshot.revised_observed_at is not None and snapshot.revised_observed_at > lineage.observed_time):
        raise ValueError("activity snapshot does not match the original matrix clocks")
    session = calendar.session_for_slot(lineage.scheduled_cycle)
    oi_matches = open_interest_record is not None and (
        open_interest_record.contract_id == snapshot.contract_id
        and open_interest_record.underlying == snapshot.underlyer
        and open_interest_record.open_interest == snapshot.open_interest
        and open_interest_record.observed_session == session
    )
    return ActivitySource(security_id=security.security_id, underlyer=snapshot.underlyer,
        contract_id=snapshot.contract_id, contract_ticker=snapshot.contract_ticker,
        contract_type=snapshot.contract_type.value, expiration_date=snapshot.expiration_date,
        expiration_cutoff=snapshot.expiration_cutoff, snapshot_id=snapshot.snapshot_id,
        snapshot_payload_sha256=snapshot.normalized_payload_sha256, batch_id=snapshot.batch_id,
        matrix_id=lineage.matrix_id, configuration_sha256=lineage.configuration_sha256,
        market_policy_sha256=lineage.market_policy_sha256, volume_session=session,
        day_volume=snapshot.day_volume, open_interest=snapshot.open_interest,
        oi_settlement_session=open_interest_record.settlement_session if oi_matches else None,
        oi_observed_at=open_interest_record.observed_at if oi_matches else None,
        market_time=snapshot.market_data_time, observed_at=snapshot.first_observed_at,
        recorded_at=recorded_at, received_at=received_at, revised_observed_at=snapshot.revised_observed_at)


class SignalDecision(Contract):
    schema_version: Literal["dual_origin_signal_decision_v1"] = "dual_origin_signal_decision_v1"
    detector_id: Literal["O1", "S1"]
    origin: Literal["OPTIONS_FIRST", "STOCK_FIRST"]
    origin_id: Name
    origin_detector_version: Name
    origin_policy_sha256: Sha256
    confirmation_policy_version: Name
    confirmation_policy_sha256: Sha256
    confirmation_basis: Name
    security_id: UUID
    underlyer: Name
    direction: Literal[-1, 1]
    market_cutoff: AwareDatetime
    decision_at: AwareDatetime
    valid_until: AwareDatetime
    activity: ActivityFinding | None
    activity_source_sha256: Sha256 | None
    activity_contract_id: int | None
    activity_expiration_date: date | None
    stock_source_sha256: Sha256 | None
    stock_source_policy_sha256: Sha256 | None
    disposition: Literal["CONFIRMED", "CONTRADICTED", "UNMATCHED", "UNAVAILABLE", "NOT_DETECTED"]
    reasons: tuple[Name, ...]
    gates: tuple[StockBehaviorGate, ...] = ()
    package_status: Literal["NOT_ASSESSED"] = "NOT_ASSESSED"
    probability: None = None
    publication_permission: Literal[False] = False
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_decision(self):
        if self.origin != ("OPTIONS_FIRST" if self.detector_id == "O1" else "STOCK_FIRST"):
            raise ValueError("origin route and detector disagree")
        if self.market_cutoff > self.decision_at:
            raise ValueError("signal decision precedes cutoff")
        if self.activity is not None and (
            self.activity_source_sha256 != self.activity.source_sha256
            or self.activity.decision_at != self.decision_at or self.activity.market_cutoff != self.market_cutoff
        ):
            raise ValueError("signal activity identity or clocks disagree")
        if self.disposition == "CONFIRMED" and (
            self.reasons or self.activity is None or self.activity.disposition != "DETECTED"
            or self.stock_source_sha256 is None or self.activity_contract_id is None
            or self.activity_expiration_date is None or self.decision_at >= self.valid_until
        ):
            raise ValueError("confirmation requires both timely source identities")
        if self.disposition != "CONFIRMED" and not self.reasons:
            raise ValueError("nonconfirmation requires reasons")
        if self.detector_id == "O1" and (
            self.origin_detector_version != ACTIVITY_POLICY.version or self.origin_policy_sha256 != ACTIVITY_POLICY.sha256
            or self.confirmation_policy_version != STOCK_ALIGNMENT_POLICY.version
            or self.confirmation_policy_sha256 != STOCK_ALIGNMENT_POLICY.sha256
            or self.confirmation_basis != STOCK_ALIGNMENT_POLICY.confirmation_basis
        ):
            raise ValueError("unsupported O1 decision policy")
        if self.detector_id == "O1" and self.disposition == "CONFIRMED" and (
            len(self.gates) != 4 or any(gate.verdict != "PASS" for gate in self.gates)
            or {gate.gate_id for gate in self.gates} != {"TREND_SLOPE_1d", "TREND_SLOPE_1h", "TREND_SLOPE_30m", "UNDERLYING_LIQUIDITY_EVIDENCE"}
        ):
            raise ValueError("O1 confirmation requires exact stock gates")
        if self.detector_id in ("S1", "S2"):
            if self.stock_source_policy_sha256 is None:
                raise ValueError("S1 requires exact stock source policy")
            policy_type = StockFirstPolicy if self.detector_id == "S1" else StockResumptionPolicy
            policy = policy_type(source_policy_sha256=self.stock_source_policy_sha256)
            if (self.origin_detector_version != policy.setup_detector
                    or self.origin_policy_sha256 != self.stock_source_policy_sha256
                    or self.confirmation_policy_version != policy.version
                    or self.confirmation_policy_sha256 != policy.sha256
                    or self.confirmation_basis != policy.confirmation_basis):
                raise ValueError("unsupported S1 decision policy")
        return self


class ResumptionSignalDecision(SignalDecision):
    schema_version: Literal["dual_origin_signal_decision_v2"] = "dual_origin_signal_decision_v2"
    detector_id: Literal["S2"] = "S2"


def load_signal_decision(value):
    contracts = {"dual_origin_signal_decision_v1": SignalDecision, "dual_origin_signal_decision_v2": ResumptionSignalDecision}
    contract = contracts.get(value.schema_version)
    if contract is None:
        raise ValueError("unsupported signal decision schema")
    return contract.model_validate_json(value.canonical_json())


def _activity_fields(source, finding):
    return dict(activity=finding, activity_source_sha256=source.sha256 if source else None,
        activity_contract_id=source.contract_id if source else None,
        activity_expiration_date=source.expiration_date if source else None)


def assess_options_first(
    source: ActivitySource, stock: StockBehaviorSnapshot | None, *, direction: Literal[-1, 1],
    market_cutoff: datetime, decision_at: datetime, calendar: OptionExchangeCalendar | None = None,
) -> SignalDecision:
    if direction not in (-1, 1) or isinstance(direction, bool):
        raise ValueError("an explicit supported stock thesis direction is required")
    finding = detect_option_participation(source, market_cutoff=market_cutoff, decision_at=decision_at, calendar=calendar)
    reasons, gates = list(finding.reasons), []
    disposition = "CONFIRMED" if finding.disposition == "DETECTED" else finding.disposition
    valid_until = finding.valid_until
    if stock is None:
        reasons.append("STOCK_BEHAVIOR_UNAVAILABLE")
        disposition = "UNAVAILABLE"
    else:
        stock = StockBehaviorSnapshot.model_validate_json(stock.canonical_json())
        valid_until = min(valid_until, stock.valid_until)
        if (stock.security_id != source.security_id or stock.ticker != source.underlyer
                or stock.definition_sha256 != DEFINITION_V1_SHA256 or stock.policy_sha256 != OPTIONS_SWING_PROFILE.sha256):
            reasons.append("STOCK_IDENTITY_OR_POLICY_MISMATCH")
            disposition = "UNAVAILABLE"
        elif stock.market_time > market_cutoff or stock.assess_at(decision_at).data_status != "READY":
            reasons.append("STOCK_NOT_READY_AT_DECISION")
            disposition = "UNAVAILABLE"
        else:
            assessed = {component.key: component for component in stock.assess_at(decision_at).components}
            original = {component.key: component for component in stock.components}
            for interval in STOCK_ALIGNMENT_POLICY.intervals:
                gates.append(_metric_gate(assessed, original, gate_id=f"TREND_SLOPE_{interval}",
                    component_key=f"TREND.{interval}", metric_id="ema50_slope10_atr", factor="TREND",
                    requirement="REQUIRED", comparator="GT_ZERO" if direction == 1 else "LT_ZERO",
                    threshold=0., mismatch_reason="STOCK_DIRECTION_NOT_ALIGNED"))
            gates.append(_metric_gate(assessed, original, gate_id="UNDERLYING_LIQUIDITY_EVIDENCE",
                component_key="PARTICIPATION.1d", metric_id="median_dollar_volume20", factor="PARTICIPATION",
                requirement="REQUIRED", comparator="PRESENT"))
            reasons.extend(reason for gate in gates for reason in gate.reason_codes)
            if any(gate.verdict == "UNAVAILABLE" for gate in gates):
                disposition = "UNAVAILABLE"
            elif disposition == "CONFIRMED" and any(gate.verdict == "FAIL" for gate in gates):
                disposition = "CONTRADICTED"
    if source.contract_type != ("CALL" if direction == 1 else "PUT") and disposition == "CONFIRMED":
        disposition = "UNMATCHED"
        reasons.append("ACTIVITY_CONTRACT_NOT_MATCHED_TO_THESIS")
    return SignalDecision(detector_id="O1", origin="OPTIONS_FIRST", origin_id=str(finding.episode_id),
        origin_detector_version=ACTIVITY_POLICY.version, origin_policy_sha256=ACTIVITY_POLICY.sha256,
        confirmation_policy_version=STOCK_ALIGNMENT_POLICY.version,
        confirmation_policy_sha256=STOCK_ALIGNMENT_POLICY.sha256,
        confirmation_basis=STOCK_ALIGNMENT_POLICY.confirmation_basis, security_id=source.security_id,
        underlyer=source.underlyer, direction=direction, market_cutoff=market_cutoff, decision_at=decision_at,
        valid_until=valid_until, stock_source_sha256=stock.sha256 if stock else None,
        stock_source_policy_sha256=stock.policy_sha256 if stock else None, **_activity_fields(source, finding),
        disposition=disposition, reasons=tuple(sorted(set(reasons))), gates=tuple(gates))


def assess_stock_first(
    setup: DirectStockSetupEvidence, source: ActivitySource | None, *, policy: StockFirstPolicy,
    trusted_source_policy: DirectSetupSourcePolicy, market_cutoff: datetime, decision_at: datetime,
    calendar: OptionExchangeCalendar | None = None,
) -> SignalDecision:
    contracts = {"stock_acceptance_option_participation_v1": (StockFirstPolicy, DirectStockSetupEvidence, DirectSetupSourcePolicy, SignalDecision, "S1"),
        "stock_resumption_option_participation_v1": (StockResumptionPolicy, DirectStockResumptionEvidence, DirectResumptionSourcePolicy, ResumptionSignalDecision, "S2")}
    if policy.version not in contracts:
        raise ValueError("unsupported stock-first policy version")
    policy_type, setup_type, source_type, decision_type, detector_id = contracts[policy.version]
    policy = policy_type.model_validate_json(policy.canonical_json())
    trusted_source_policy = resolve_direct_setup_policy(trusted_source_policy)
    if type(trusted_source_policy) is not source_type:
        raise ValueError("stock-first detector requires its exact trusted source policy")
    setup = setup_type.model_validate_json(setup.canonical_json())
    if policy.source_policy_sha256 != trusted_source_policy.sha256 or setup.source_policy != trusted_source_policy:
        raise ValueError("S1 requires an independently trusted exact source policy")
    if market_cutoff.utcoffset() is None or decision_at.utcoffset() is None or market_cutoff > decision_at:
        raise ValueError("S1 cutoffs must be aware and causal")
    candidate = setup.candidate
    finding = detect_option_participation(source, market_cutoff=market_cutoff, decision_at=decision_at,
        calendar=calendar) if source else None
    reasons = []
    disposition = "CONFIRMED"
    if not setup.available_at(decision_at, market_cutoff):
        reasons.append("STOCK_TRIGGER_NOT_AVAILABLE_AT_DECISION")
        disposition = "UNAVAILABLE"
    if source is None:
        reasons.append("OPTION_PARTICIPATION_UNAVAILABLE")
        disposition = "UNAVAILABLE"
    elif source.security_id != setup.source.security_id or source.underlyer != candidate.ticker:
        reasons.append("CROSS_MARKET_SECURITY_MISMATCH")
        disposition = "UNAVAILABLE"
    elif finding.disposition != "DETECTED":
        reasons.extend(finding.reasons)
        if disposition != "UNAVAILABLE":
            disposition = "UNAVAILABLE" if finding.disposition == "UNAVAILABLE" else "UNMATCHED"
    elif source.contract_type != ("CALL" if candidate.direction == 1 else "PUT"):
        reasons.append("ACTIVITY_CONTRACT_NOT_MATCHED_TO_THESIS")
        if disposition != "UNAVAILABLE":
            disposition = "UNMATCHED"
    return decision_type(detector_id=detector_id, origin="STOCK_FIRST", origin_id=candidate.episode_id,
        origin_detector_version=candidate.policy_version, origin_policy_sha256=trusted_source_policy.sha256,
        confirmation_policy_version=policy.version, confirmation_policy_sha256=policy.sha256,
        confirmation_basis=policy.confirmation_basis, security_id=setup.source.security_id,
        underlyer=candidate.ticker, direction=candidate.direction, market_cutoff=market_cutoff,
        decision_at=decision_at, valid_until=min(setup.source.valid_until, finding.valid_until) if finding else setup.source.valid_until,
        stock_source_sha256=setup.sha256, stock_source_policy_sha256=trusted_source_policy.sha256,
        **_activity_fields(source, finding), disposition=disposition, reasons=tuple(sorted(set(reasons))))


class CandidateHandoffPolicy(Contract):
    version: Literal["dual_origin_candidate_handoff_v1"] = "dual_origin_candidate_handoff_v1"
    status: Literal["CANDIDATE_REFERENCE_NOT_ADMISSION"] = "CANDIDATE_REFERENCE_NOT_ADMISSION"
    category_basis: Literal["CONFIRMED_DIRECTIONAL_THESIS_AND_LONG_OR_DEBIT"] = "CONFIRMED_DIRECTIONAL_THESIS_AND_LONG_OR_DEBIT"
    repeat_basis: Literal["DETECTOR_CONFIRMATION_SELECTOR_EXACT_PACKAGE"] = "DETECTOR_CONFIRMATION_SELECTOR_EXACT_PACKAGE"
    execution_permission: Literal[False] = False


CANDIDATE_HANDOFF_POLICY = CandidateHandoffPolicy()


class CandidateHandoff(Contract):
    schema_version: Literal["dual_origin_candidate_reference_v1"] = "dual_origin_candidate_reference_v1"
    decision_sha256: Sha256
    detector_id: Literal["O1", "S1"]
    origin_id: Name
    policy_sha256: Sha256 = CANDIDATE_HANDOFF_POLICY.sha256
    candidate_id: UUID
    candidate_identity_sha256: Sha256
    matrix_id: UUID
    package_assessment_sha256: Sha256
    package_terms_sha256: Sha256
    exposure_sha256: Sha256
    recurrence_sha256: Sha256
    primary_category: Literal["MOMENTUM"] = "MOMENTUM"
    direction: Literal[-1, 1]
    bound_at: AwareDatetime
    original_entry_deadline: AwareDatetime
    status: Literal["CANDIDATE_REFERENCE_NOT_ADMISSION"] = "CANDIDATE_REFERENCE_NOT_ADMISSION"
    remaining_gates: tuple[Name, ...] = (
        "EXACT_RAW_STOCK_OPTION_PRICE_BASIS", "HOLD_EXPIRY_AND_MANAGEMENT_POLICY",
        "CAPABILITY_AND_EVENT_QUALIFICATION", "WORKER_SELECTION_AND_DURABLE_MEMBERSHIP",
    )
    publication_permission: Literal[False] = False
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_handoff(self):
        if self.policy_sha256 != CANDIDATE_HANDOFF_POLICY.sha256 or self.bound_at >= self.original_entry_deadline:
            raise ValueError("unsupported or late candidate handoff")
        if self.remaining_gates != (
            "EXACT_RAW_STOCK_OPTION_PRICE_BASIS", "HOLD_EXPIRY_AND_MANAGEMENT_POLICY",
            "CAPABILITY_AND_EVENT_QUALIFICATION", "WORKER_SELECTION_AND_DURABLE_MEMBERSHIP",
        ):
            raise ValueError("reference handoff cannot waive remaining qualification gates")
        return self


class ResumptionCandidateHandoff(CandidateHandoff):
    schema_version: Literal["dual_origin_candidate_reference_v2"] = "dual_origin_candidate_reference_v2"
    detector_id: Literal["S2"] = "S2"


def load_candidate_handoff(value):
    contracts = {"dual_origin_candidate_reference_v1": CandidateHandoff, "dual_origin_candidate_reference_v2": ResumptionCandidateHandoff}
    contract = contracts.get(value.schema_version)
    if contract is None:
        raise ValueError("unsupported candidate reference schema")
    return contract.model_validate_json(value.canonical_json())


def bind_confirmed_candidate(
    decision: SignalDecision, source: ActivitySource, candidate: OptionCandidate,
    package_assessment: OptionPackageAssessment,
) -> CandidateHandoff:
    import hashlib

    decision = load_signal_decision(decision)
    source = ActivitySource.model_validate_json(source.canonical_json())
    package_assessment = OptionPackageAssessment.model_validate_json(package_assessment.canonical_json())
    if decision.disposition != "CONFIRMED" or decision.activity_source_sha256 != source.sha256:
        raise ValueError("package reference requires the exact confirmed activity source")
    if detect_option_participation(source, market_cutoff=decision.market_cutoff, decision_at=decision.decision_at) != decision.activity:
        raise ValueError("activity finding does not match its original inputs")
    if (source.security_id != decision.security_id or source.underlyer != decision.underlyer
            or candidate.underlyer != decision.underlyer or candidate.matrix_id != source.matrix_id
            or candidate.market_data_time > decision.market_cutoff or candidate.observed_time > decision.decision_at
            or candidate.valid_until is None or decision.decision_at >= min(candidate.valid_until, decision.valid_until)
            or source.observed_at > candidate.observed_time):
        raise ValueError("package identity or original decision deadline mismatch")
    targets = {"LONG_CALL": (1, "DIRECTIONAL_LONG_PREMIUM", 1), "LONG_PUT": (-1, "DIRECTIONAL_LONG_PREMIUM", 1),
        "CALL_DEBIT_VERTICAL": (1, "DIRECTIONAL_DEBIT_SPREAD", 2), "PUT_DEBIT_VERTICAL": (-1, "DIRECTIONAL_DEBIT_SPREAD", 2)}
    target = targets.get(candidate.structure_type.value)
    if (target is None or target != (decision.direction, candidate.strategy_name, len(candidate.legs))
            or candidate.primary_evidence.get("directional_thesis") != ("BULLISH" if decision.direction == 1 else "BEARISH")):
        raise ValueError("confirmed thesis does not match a supported long/debit package")
    legs = candidate.legs
    expected_type = "CALL" if decision.direction == 1 else "PUT"
    if (legs[0].side != OptionSide.BUY or len(legs) == 2 and legs[1].side != OptionSide.SELL
            or any(leg.contract_type.value != expected_type or leg.ratio != 1 or leg.multiplier != 100 for leg in legs)
            or len({leg.expiration_date for leg in legs}) != 1
            or candidate.net_premium is None or candidate.net_premium >= 0):
        raise ValueError("long/debit leg orientation or economics mismatch")
    if len(legs) == 2 and decision.direction * (legs[1].strike - legs[0].strike) <= 0:
        raise ValueError("debit vertical strike order mismatch")
    long_leg = legs[0]
    if (long_leg.contract_id != source.contract_id or long_leg.snapshot_id != source.snapshot_id
            or long_leg.contract_ticker != source.contract_ticker or long_leg.expiration_date != source.expiration_date):
        raise ValueError("participation must bind the exact original long leg and expiry")
    if package_assessment.status != "READY" or assess_option_package(candidate,
            valuation_policy_sha256=package_assessment.valuation_policy_sha256) != package_assessment:
        raise ValueError("package assessment does not match retained candidate economics")
    exposure = hashlib.sha256(canonical_json(dict(version="dual_origin_exposure_v1",
        security_id=str(decision.security_id), underlyer=candidate.underlyer,
        structure=candidate.structure_type.value,
        legs=tuple(sorted((leg.contract_id, leg.side.value, leg.ratio, leg.multiplier) for leg in legs)),
    )).encode("ascii")).hexdigest()
    recurrence = hashlib.sha256(":".join((decision.detector_id, decision.origin_detector_version,
        decision.origin_policy_sha256, decision.confirmation_policy_sha256, candidate.policy_sha256,
        CANDIDATE_HANDOFF_POLICY.sha256, exposure)).encode("ascii")).hexdigest()
    handoff_type = ResumptionCandidateHandoff if decision.detector_id == "S2" else CandidateHandoff
    return handoff_type(decision_sha256=decision.sha256, detector_id=decision.detector_id,
        origin_id=decision.origin_id, candidate_id=candidate.candidate_id,
        candidate_identity_sha256=candidate.identity_sha256, matrix_id=candidate.matrix_id,
        package_assessment_sha256=package_assessment.sha256, package_terms_sha256=package_assessment.package_terms_sha256,
        exposure_sha256=exposure, recurrence_sha256=recurrence, direction=decision.direction,
        bound_at=decision.decision_at, original_entry_deadline=candidate.valid_until)


def summarize_signal_decisions(decisions, handoffs=(), *, as_of):
    if as_of.utcoffset() is None or len(decisions) > 5000 or len(handoffs) > 5000:
        raise ValueError("signal review requires an aware cutoff and bounded inputs")
    decisions_by_hash = {}
    for original in decisions:
        decision = load_signal_decision(original)
        if decision.decision_at <= as_of:
            decisions_by_hash[decision.sha256] = decision
    references = {}
    for original in handoffs:
        handoff = load_candidate_handoff(original)
        if handoff.bound_at > as_of:
            continue
        decision = decisions_by_hash.get(handoff.decision_sha256)
        if (decision is None or decision.disposition != "CONFIRMED"
                or (handoff.detector_id, handoff.origin_id, handoff.direction, handoff.bound_at)
                != (decision.detector_id, decision.origin_id, decision.direction, decision.decision_at)):
            raise ValueError("signal review handoff has no exact confirmed parent")
        references[handoff.sha256] = handoff
    grouped = defaultdict(list)
    exposures = defaultdict(set)
    for decision in decisions_by_hash.values():
        grouped[(decision.detector_id, decision.origin_detector_version,
            decision.origin_policy_sha256, decision.confirmation_policy_sha256)].append(decision)
    for handoff in references.values():
        exposures[handoff.exposure_sha256].add(handoff.detector_id)
    models = []
    for (detector, version, origin_hash, confirmation_hash), observations in sorted(grouped.items()):
        hashes = {row.sha256 for row in observations}
        packages = [row for row in references.values() if row.decision_sha256 in hashes]
        detected = [row for row in observations if row.detector_id in ("S1", "S2")
            or row.activity is not None and row.activity.disposition == "DETECTED"]
        episodes = {row.origin_id for row in detected}
        models.append(dict(detector_id=detector, origin=observations[0].origin,
            origin_detector_version=version, origin_policy_sha256=origin_hash,
            confirmation_policy_sha256=confirmation_hash, assessments=len(observations),
            origin_episodes=len(episodes), repeated_episode_assessments=len(detected) - len(episodes),
            dispositions=dict(sorted(Counter(row.disposition for row in observations).items())),
            reasons=dict(sorted(Counter(reason for row in observations for reason in row.reasons).items())),
            candidate_references=len(packages), distinct_package_exposures=len({row.exposure_sha256 for row in packages}),
            underlyers=sorted({row.underlyer for row in observations}),
            outcome_status="NOT_BOUND_TO_PROSPECTIVE_MEMBERSHIP", measured_outcomes=None,
            positive_net_marked_return_rate=None, mean_net_return=None, probability=None))
    return dict(version="dual_origin_signal_review_v1", as_of=as_of.isoformat(), models=models,
        cross_model_shared_exposures=sum(len(detectors) > 1 for detectors in exposures.values()),
        repeated_assessments_are_hits=False, outcome_comparison="NOT_AVAILABLE",
        publication_permission=False, execution_permission=False)


class DualOriginQualificationPolicy(Contract):
    version: Literal["dual_origin_indicative_qualification_v1"] = "dual_origin_indicative_qualification_v1"
    capability: Literal["INDICATIVE"] = "INDICATIVE"
    raw_basis: Literal["EXACT_RAW_RTH_CLOSE_STANDARD_100_SHARES"] = "EXACT_RAW_RTH_CLOSE_STANDARD_100_SHARES"
    event_horizon: Literal["KNOWN_EVENTS_BLOCK_UNKNOWN_REMAINS_ADVISORY"] = "KNOWN_EVENTS_BLOCK_UNKNOWN_REMAINS_ADVISORY"
    plan_basis: Literal["ORIGINAL_OR_EXPLICIT_VERSIONED_MANAGEMENT"] = "ORIGINAL_OR_EXPLICIT_VERSIONED_MANAGEMENT"
    maximum_expiry_days: Literal[60] = 60
    execution_permission: Literal[False] = False


DUAL_ORIGIN_QUALIFICATION_POLICY = DualOriginQualificationPolicy()


class TechnicalQualificationPolicy(DualOriginQualificationPolicy):
    version: Literal["dual_origin_technical_qualification_v1"] = "dual_origin_technical_qualification_v1"
    plan_basis: Literal["FROZEN_TECHNICAL_LEVELS_WITH_HARD_DEBIT_LIMITS"] = "FROZEN_TECHNICAL_LEVELS_WITH_HARD_DEBIT_LIMITS"


TECHNICAL_QUALIFICATION_POLICY = TechnicalQualificationPolicy()


class QualifiedDualOriginPackage(Contract):
    expected_qualification_sha256: ClassVar[str] = DUAL_ORIGIN_QUALIFICATION_POLICY.sha256
    schema_version: Literal["dual_origin_qualified_package_v1"] = "dual_origin_qualified_package_v1"
    qualification_policy_sha256: Sha256 = DUAL_ORIGIN_QUALIFICATION_POLICY.sha256
    handoff_sha256: Sha256
    decision_sha256: Sha256
    candidate_id: UUID
    candidate_identity_sha256: Sha256
    matrix_id: UUID
    scheduled_cycle: AwareDatetime
    detector_id: Literal["O1", "S1"]
    underlyer: Name
    direction: Literal[-1, 1]
    candidate_rank: int = Field(strict=True, gt=0)
    primary_category: Literal["MOMENTUM"] = "MOMENTUM"
    exposure_sha256: Sha256
    recurrence_sha256: Sha256
    plan_sha256: Sha256
    source_basis_sha256: Sha256
    decision_at: AwareDatetime
    entry_deadline: AwareDatetime
    exit_deadline: AwareDatetime
    expires_at: AwareDatetime
    event_horizon_status: Literal["UNAVAILABLE", "CLEAR"]
    status: Literal["QUALIFIED_INDICATIVE"] = "QUALIFIED_INDICATIVE"
    evidence_mode: Literal["PROSPECTIVE_RECEIPT"] = "PROSPECTIVE_RECEIPT"
    publication_permission: Literal[False] = False
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_qualification(self):
        if self.qualification_policy_sha256 != self.expected_qualification_sha256:
            raise ValueError("unsupported dual-origin qualification policy")
        if not self.scheduled_cycle <= self.decision_at < self.entry_deadline < self.exit_deadline < self.expires_at:
            raise ValueError("qualified package clocks must preserve decision, entry, exit and expiry order")
        if self.expires_at - self.decision_at > timedelta(days=DUAL_ORIGIN_QUALIFICATION_POLICY.maximum_expiry_days):
            raise ValueError("qualified package expiry exceeds policy scope")
        return self


class QualifiedResumptionPackage(QualifiedDualOriginPackage):
    schema_version: Literal["dual_origin_qualified_package_v2"] = "dual_origin_qualified_package_v2"
    detector_id: Literal["S2"] = "S2"


class QualifiedTechnicalPackage(QualifiedDualOriginPackage):
    expected_qualification_sha256: ClassVar[str] = TECHNICAL_QUALIFICATION_POLICY.sha256
    schema_version: Literal["dual_origin_qualified_package_v3"] = "dual_origin_qualified_package_v3"
    qualification_policy_sha256: Sha256 = TECHNICAL_QUALIFICATION_POLICY.sha256
    detector_id: Literal["O1", "S1", "S2"]


def load_qualified_package(value):
    contracts = {"dual_origin_qualified_package_v1": QualifiedDualOriginPackage,
        "dual_origin_qualified_package_v2": QualifiedResumptionPackage, "dual_origin_qualified_package_v3": QualifiedTechnicalPackage}
    contract = contracts.get(value.schema_version)
    if contract is None:
        raise ValueError("unsupported qualified package schema")
    return contract.model_validate_json(value.canonical_json())


def qualify_dual_origin_package(
    *, decision, source, candidate, package_assessment, stock, security, lineage,
    snapshots, references, raw_bars, raw_bar_created_ats, source_received_at,
    planned_entry_at, entry_deadline, exit_deadline, entry_limit, valuation_policy,
    management_policy=None, setup=None, trusted_source_policy=None, stock_first_policy=None,
    event_detail=None, technical_evidence=None, technical_source_policy_sha256=None,
):
    import hashlib
    from dataclasses import asdict

    from options.alert_plans import FrozenOptionAlertPlan, _canonical, _plan_management
    from options.alert_qualification import (
        AlertInput, AlertInputEvidence, qualify_option_alert, retained_alert_evidence, retained_event_horizon,
    )
    from options.stock_setup_binding import bind_option_leg_basis
    from options.strategies.gates import GateVerdict
    from research.stock_idea_engine import entry_gate

    decision = load_signal_decision(decision)
    if decision.detector_id == "O1":
        recomputed = assess_options_first(source, stock, direction=decision.direction,
            market_cutoff=decision.market_cutoff, decision_at=decision.decision_at)
    else:
        if setup is None or trusted_source_policy is None or stock_first_policy is None:
            raise ValueError("S1 qualification requires exact setup and independently trusted policy")
        recomputed = assess_stock_first(setup, source, policy=stock_first_policy,
            trusted_source_policy=trusted_source_policy, market_cutoff=decision.market_cutoff, decision_at=decision.decision_at)
    if recomputed != decision:
        raise ValueError("qualification decision does not match its source evidence")
    handoff = bind_confirmed_candidate(decision, source, candidate, package_assessment)
    for clock in (source_received_at, planned_entry_at, entry_deadline, exit_deadline):
        if clock.utcoffset() is None:
            raise ValueError("qualification requires aware source and holding clocks")
    if (not source.received_at <= source_received_at <= decision.decision_at < planned_entry_at
            <= entry_deadline < min(candidate.valid_until, decision.valid_until)
            or not planned_entry_at < exit_deadline):
        raise ValueError("qualification source/entry/exit clocks exceed original validity")
    stock_available_at = stock.available_at if decision.detector_id == "O1" else setup.source.received_at
    earliest_market_time = min(source.market_time, candidate.market_data_time,
        stock.market_time if decision.detector_id == "O1" else setup.source.market_time)
    if stock_available_at > source_received_at:
        raise ValueError("qualification predates the stock evidence receipt")
    if (not security.active or security.security_id != decision.security_id or security.ticker != candidate.underlyer
            or security.effective_from > earliest_market_time
            or security.observed_at > source_received_at):
        raise ValueError("qualification dated security identity is unavailable")
    if (lineage.matrix_id != candidate.matrix_id or lineage.underlying != candidate.underlyer
            or lineage.market_time != candidate.market_data_time or lineage.observed_time != candidate.observed_time
            or lineage.configuration_sha256 != source.configuration_sha256
            or lineage.market_policy_sha256 != source.market_policy_sha256):
        raise ValueError("qualification matrix/source lineage mismatch")
    if package_assessment.valuation_policy_sha256 != valuation_policy.policy_sha256:
        raise ValueError("qualification valuation policy mismatch")
    if not snapshots or (
        snapshots[0].batch_id != source.batch_id or snapshots[0].snapshot_id != source.snapshot_id
        or snapshots[0].normalized_payload_sha256 != source.snapshot_payload_sha256
        or snapshots[0].day_volume != source.day_volume or snapshots[0].open_interest != source.open_interest
    ):
        raise ValueError("qualification activity does not match the original long-leg snapshot")
    calendar = OptionExchangeCalendar()
    source_session = candidate.market_data_time.astimezone(ZoneInfo("America/New_York")).date()
    basis = bind_option_leg_basis(candidate=candidate, security=security, snapshots=snapshots,
        references=references, raw_bars=raw_bars, raw_bar_created_ats=raw_bar_created_ats,
        source_received_at=source_received_at, decision_at=decision.decision_at,
        source_session=source_session, valuation_policy=valuation_policy)
    if len({(row.spot, row.spot_at) for row in basis}) != 1:
        raise ValueError("qualification requires coherent stock price basis across all legs")
    expires_at = min(snapshot.expiration_cutoff for snapshot in snapshots)
    if exit_deadline >= expires_at:
        raise ValueError("qualification exit must precede every leg expiration")
    if decision.detector_id in ("S1", "S2"):
        if (source_session != setup.source.market_time.astimezone(ZoneInfo("America/New_York")).date()
                or not calendar.next_session_open(calendar.previous_session(source_session)) <= planned_entry_at
                < exit_deadline <= calendar.session_close(source_session)
                or any(not setup.source.market_time <= row.spot_at <= candidate.market_data_time for row in basis)
                or entry_gate(setup.candidate, float(basis[0].spot)) is not None):
            raise ValueError("stock-first qualification requires original same-session trigger geometry and exit")
    candidate_record = asdict(candidate)
    candidate_record["underlying"] = candidate_record.pop("underlyer")
    snapshot_records = []
    for snapshot in snapshots:
        record = asdict(snapshot)
        record["underlying"] = record.pop("underlyer")
        snapshot_records.append(record)
    detail = dict(candidate=candidate_record, legs=[asdict(leg) for leg in candidate.legs], source_snapshots=snapshot_records)
    event_input = dict(event_detail or {})
    event_input["candidate"] = {"underlying": candidate.underlyer, "observed_time": decision.decision_at}
    event_input["source_snapshots"] = snapshot_records
    event_horizon = retained_event_horizon(event_input, decision.decision_at, exit_deadline)
    if event_horizon["status"] == "BLOCKED":
        raise ValueError("known event blocks the declared holding window")
    evidence = retained_alert_evidence(detail, decision.decision_at, holding_until=exit_deadline)
    evidence[AlertInput.DIRECTION] = AlertInputEvidence(GateVerdict.PASS,
        (f"dual-origin-decision:{decision.sha256}",), decision.decision_at)
    qualification = qualify_option_alert(candidate.strategy_name, candidate.structure_type, evidence, decision.decision_at)
    indicative = next(row for row in qualification["assessments"] if row["capability"] == "INDICATIVE")
    if indicative["status"] != "SATISFIED":
        raise ValueError(f"indicative package inputs are not satisfied: {indicative['blocking_inputs']}")
    technical = None
    qualification_policy = DUAL_ORIGIN_QUALIFICATION_POLICY
    if technical_evidence is not None:
        from options.alert_plans import AlertManagementPolicy, TECHNICAL_EXIT_POLICY, technical_exit_terms, estimate_technical_package_values

        if management_policy is not None:
            raise ValueError("technical policy cannot be mixed with another management policy")
        if technical_evidence.market_time > decision.market_cutoff:
            raise ValueError("technical levels exceed the original decision market cutoff")
        if decision.detector_id in ("S1", "S2"):
            if (technical_evidence.source_kind != "STOCK_SETUP" or technical_evidence.source_payload_sha256 != setup.sha256
                    or technical_evidence.source_policy_sha256 != trusted_source_policy.sha256
                    or technical_evidence.interval != setup.candidate.interval
                    or set(map(str, technical_evidence.source_revision_ids)) != set(setup.candidate.revision_ids)
                    or next(level.price for level in technical_evidence.levels if level.role == "INVALIDATION") != Decimal(str(setup.candidate.stop))
                    or not any(level.role == "OPPOSING_STRUCTURE" and level.price == Decimal(str(setup.candidate.target)) for level in technical_evidence.levels)):
                raise ValueError("technical policy must preserve original stock setup stop/target and source")
        technical = technical_exit_terms(evidence=technical_evidence, trusted_source_policy_sha256=technical_source_policy_sha256,
            security_id=security.security_id, underlyer=candidate.underlyer, direction=decision.direction,
            stock_entry=basis[0].spot, entry_debit=entry_limit, decision_at=decision.decision_at,
            exit_deadline=exit_deadline, session_close=calendar.session_close(source_session))
        technical["package_price_scenarios"] = estimate_technical_package_values(candidate, technical,
            decision_at=decision.decision_at, exit_deadline=exit_deadline)
        management_policy = AlertManagementPolicy(TECHNICAL_EXIT_POLICY.version, candidate.strategy_name,
            Decimal(TECHNICAL_EXIT_POLICY.hard_stop_fraction), Decimal(TECHNICAL_EXIT_POLICY.hard_profit_fraction),
            TECHNICAL_EXIT_POLICY.maximum_hold_seconds, TECHNICAL_EXIT_POLICY.minimum_exit_dte)
        qualification_policy = TECHNICAL_QUALIFICATION_POLICY
    elif technical_source_policy_sha256 is not None:
        raise ValueError("technical source policy requires actual technical evidence")
    management, management_version = _plan_management(candidate_record, decision.decision_at, exit_deadline, management_policy)
    if technical is not None:
        management = {**management, "technical_exit": technical,
            "exit_rule": "FIRST_OBSERVED_TECHNICAL_OR_HARD_PREMIUM_OR_TIME_LIMIT",
            "take_profit_role": "HARD_CEILING_NOT_REQUIRED_MODEL_TARGET"}
    maximum_hold = management.get("maximum_hold_seconds")
    if type(maximum_hold) is not int or maximum_hold <= 0 or exit_deadline > decision.decision_at + timedelta(seconds=maximum_hold):
        raise ValueError("qualification requires an explicit elapsed holding policy")
    exit_dte = management.get("exit_dte")
    if (type(exit_dte) is not int or exit_dte < 1
            or min((leg.expiration_date - exit_deadline.astimezone(ZoneInfo("America/New_York")).date()).days for leg in candidate.legs) < exit_dte):
        raise ValueError("qualification exit violates the explicit exit-DTE policy")
    if (type(entry_limit) is not Decimal or not entry_limit.is_finite() or entry_limit <= 0
            or entry_limit > -candidate.net_premium):
        raise ValueError("entry limit must preserve the original debit basis")
    target_profit = entry_limit * Decimal(str(management["take_profit_fraction"]))
    if technical is None and candidate.maximum_profit is not None and target_profit > candidate.maximum_profit + (-candidate.net_premium - entry_limit):
        raise ValueError("management target exceeds bounded package payoff")
    source_basis_sha256 = hashlib.sha256(_canonical(dict(security=asdict(security),
        lineage=asdict(lineage), activity=source.sha256, legs=[row.model_dump(mode="json") for row in basis],
        source_received_at=source_received_at)).encode("ascii")).hexdigest()
    management_sha256 = hashlib.sha256(_canonical(dict(version=management_version, terms=management)).encode("ascii")).hexdigest()
    recurrence_management = hashlib.sha256(_canonical(dict(version=management_version,
        technical_policy_sha256=technical["policy_sha256"], source_policy_sha256=technical_evidence.source_policy_sha256,
        strategy_name=candidate.strategy_name)).encode("ascii")).hexdigest() if technical else management_sha256
    recurrence = hashlib.sha256(":".join((handoff.recurrence_sha256, recurrence_management,
        qualification_policy.sha256)).encode("ascii")).hexdigest()
    plan = FrozenOptionAlertPlan(_canonical(dict(version="dual_origin_indicative_plan_v2" if technical else "dual_origin_indicative_plan_v1",
        candidate_id=str(candidate.candidate_id), candidate_identity_sha256=candidate.identity_sha256,
        matrix_id=str(candidate.matrix_id), detector_id=decision.detector_id, origin_id=decision.origin_id,
        direction=decision.direction, decision_sha256=decision.sha256, handoff_sha256=handoff.sha256,
        qualification_policy_sha256=qualification_policy.sha256,
        package_terms_sha256=package_assessment.package_terms_sha256, source_basis_sha256=source_basis_sha256,
        source_market_time=candidate.market_data_time, source_observed_at=candidate.observed_time,
        decision_at=decision.decision_at, planned_entry_at=planned_entry_at, entry_deadline=entry_deadline,
        exit_deadline=exit_deadline, entry_limit=str(entry_limit), entry_limit_kind="MAXIMUM_DEBIT",
        entry_limit_units="USD_PER_PACKAGE", management_policy_version=management_version,
        management_policy_sha256=management_sha256, management_policy=management,
        indicative_qualification=indicative, event_horizon=event_horizon,
        remaining_capabilities=["QUOTE_PAPER", "EXECUTION"], publication_permission=False,
        execution_permission=False, fill=None)))
    qualified_type = QualifiedTechnicalPackage if technical else QualifiedResumptionPackage if decision.detector_id == "S2" else QualifiedDualOriginPackage
    qualified = qualified_type(handoff_sha256=handoff.sha256, decision_sha256=decision.sha256,
        candidate_id=candidate.candidate_id, candidate_identity_sha256=candidate.identity_sha256,
        matrix_id=candidate.matrix_id, scheduled_cycle=lineage.scheduled_cycle,
        detector_id=decision.detector_id, underlyer=candidate.underlyer,
        direction=decision.direction, candidate_rank=candidate.rank, exposure_sha256=handoff.exposure_sha256,
        recurrence_sha256=recurrence, plan_sha256=plan.sha256, source_basis_sha256=source_basis_sha256,
        decision_at=decision.decision_at, entry_deadline=entry_deadline, exit_deadline=exit_deadline,
        expires_at=expires_at, event_horizon_status=event_horizon["status"])
    return qualified, plan