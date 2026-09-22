"""Opt-in O1 v2 observations; no collection, publication or package admission."""
from datetime import datetime
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import AwareDatetime, model_validator

from equity.behavior import (
    BehaviorComponent, BehaviorComponentAssessment, BehaviorProfile, Contract,
    DEFINITION_V1_SHA256, Name, Sha256,
)
from options.dual_origin import ACTIVITY_POLICY, ActivityFinding, ActivitySource, detect_option_participation
from options.stock_behavior_gates import StockBehaviorGate, _metric_gate


INTRADAY_PROFILE = BehaviorProfile(
    name="O1_COMPLETED_30M_V2", definition_sha256=DEFINITION_V1_SHA256,
    required_metrics=(("TREND.30m", ("ema50_slope10_atr",)),
                      ("PARTICIPATION.1d", ("median_dollar_volume20",))),
)


class IntradayAlignmentPolicy(Contract):
    version: Literal["option_participation_stock_alignment_v2"] = "option_participation_stock_alignment_v2"
    activity_policy_sha256: Literal[ACTIVITY_POLICY.sha256] = ACTIVITY_POLICY.sha256
    behavior_definition_sha256: Literal[DEFINITION_V1_SHA256] = DEFINITION_V1_SHA256
    behavior_profile_sha256: Literal[INTRADAY_PROFILE.sha256] = INTRADAY_PROFILE.sha256
    confirmation_basis: Literal["COMPLETED_30M_SLOPE_AND_LIQUIDITY_PRESENCE"] = "COMPLETED_30M_SLOPE_AND_LIQUIDITY_PRESENCE"
    market_cutoff_basis: Literal["ORIGINAL_OPTION_MARKET_TIME"] = "ORIGINAL_OPTION_MARKET_TIME"
    validity_basis: Literal["ORIGINAL_REQUIRED_SOURCE_EXPIRY"] = "ORIGINAL_REQUIRED_SOURCE_EXPIRY"
    output_kind: Literal["OBSERVATION"] = "OBSERVATION"
    execution_permission: Literal[False] = False


INTRADAY_ALIGNMENT_POLICY = IntradayAlignmentPolicy()


class IntradayStockEvidence(Contract):
    schema_version: Literal["option_intraday_stock_evidence_v2"] = "option_intraday_stock_evidence_v2"
    profile_sha256: Literal[INTRADAY_PROFILE.sha256] = INTRADAY_PROFILE.sha256
    security_id: UUID
    underlyer: Name
    available_at: AwareDatetime
    components: tuple[BehaviorComponent, ...]
    carrier_snapshot_id: UUID | None = None
    carrier_snapshot_sha256: Sha256 | None = None

    @property
    def market_time(self):
        return max(source.market_time for component in self.components for source in component.sources)

    @model_validator(mode="after")
    def validate_evidence(self):
        keys = tuple(component.key for component in self.components)
        if len(set(keys)) != len(keys) or set(keys) - {key for key, _ in INTRADAY_PROFILE.required_metrics}:
            raise ValueError("intraday evidence accepts only distinct required components")
        for component in self.components:
            if component.metrics and not component.sources:
                raise ValueError("intraday measurements require original source lineage")
            for source in component.sources:
                if source.security_id != self.security_id or source.received_at > self.available_at:
                    raise ValueError("intraday source identity or receipt mismatch")
                if source.availability_mode != "PROSPECTIVE_RECEIPT":
                    raise ValueError("intraday observations require original prospective receipts")
        return self


def bind_intraday_components(snapshot, *, received_at):
    from equity.behavior import StockBehaviorSnapshot
    from equity.behavior_sources import ADJUSTED_DAILY_HISTORY_POLICY, BEHAVIOR_SOURCE_SELECTION_POLICY

    snapshot = StockBehaviorSnapshot.model_validate_json(snapshot.canonical_json())
    if received_at.utcoffset() is None or snapshot.available_at > received_at or snapshot.availability_mode != "PROSPECTIVE_RECEIPT":
        raise ValueError("intraday component receipt must follow original prospective availability")
    selected = []
    for key, required in INTRADAY_PROFILE.required_metrics:
        component = next((row for row in snapshot.components if row.key == key), None)
        if component is None:
            continue
        expected = BEHAVIOR_SOURCE_SELECTION_POLICY if component.interval == "30m" else ADJUSTED_DAILY_HISTORY_POLICY
        metrics = tuple(metric for metric in component.metrics if metric.definition.metric_id in required)
        ready = bool(component.sources) and len(metrics) == len(required) and all(metric.status == "READY" for metric in metrics)
        if any(source.policy_sha256 != expected.sha256 or source.price_basis != (
                "RAW_ACTION_GATED" if component.interval == "30m" else "PROVIDER_SPLIT_ADJUSTED") for source in component.sources):
            ready = False
        selected.append(BehaviorComponent(factor=component.factor, interval=component.interval,
            status="READY" if ready else "UNAVAILABLE", metrics=metrics, sources=component.sources,
            reason_codes=() if ready else ("INTRADAY_REQUIRED_METRIC_OR_POLICY_UNAVAILABLE",)))
    return IntradayStockEvidence(security_id=snapshot.security_id, underlyer=snapshot.ticker,
        available_at=received_at, components=tuple(selected), carrier_snapshot_id=snapshot.snapshot_id,
        carrier_snapshot_sha256=snapshot.sha256)


class IntradayParticipationObservation(Contract):
    schema_version: Literal["option_intraday_participation_observation_v2"] = "option_intraday_participation_observation_v2"
    detector_id: Literal["O1"] = "O1"
    policy: IntradayAlignmentPolicy = INTRADAY_ALIGNMENT_POLICY
    security_id: UUID
    underlyer: Name
    direction: Literal[-1, 1]
    market_cutoff: AwareDatetime
    decision_at: AwareDatetime
    valid_until: AwareDatetime
    activity: ActivityFinding
    stock_source_sha256: Sha256 | None
    gates: tuple[StockBehaviorGate, ...]
    disposition: Literal["CONFIRMED", "CONTRADICTED", "UNMATCHED", "UNAVAILABLE", "NOT_DETECTED"]
    reasons: tuple[Name, ...]
    package_status: Literal["NOT_ASSESSED"] = "NOT_ASSESSED"
    publication_permission: Literal[False] = False
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_observation(self):
        if self.activity.decision_at != self.decision_at or self.activity.market_cutoff != self.market_cutoff:
            raise ValueError("intraday observation and activity clocks disagree")
        if self.valid_until > self.activity.valid_until:
            raise ValueError("intraday confirmation cannot extend option validity")
        if self.disposition == "CONFIRMED":
            if (self.reasons or self.activity.disposition != "DETECTED" or self.stock_source_sha256 is None
                    or self.decision_at >= self.valid_until or len(self.gates) != 2
                    or {gate.gate_id for gate in self.gates} != {"TREND_SLOPE_30m", "UNDERLYING_LIQUIDITY_EVIDENCE"}
                    or any(gate.verdict != "PASS" for gate in self.gates)):
                raise ValueError("intraday confirmation requires exact timely stock gates")
        elif not self.reasons:
            raise ValueError("intraday nonconfirmation requires explicit reasons")
        return self


def assess_intraday_participation(
    source: ActivitySource, stock: IntradayStockEvidence | None, *, direction: Literal[-1, 1],
    market_cutoff: datetime, decision_at: datetime,
) -> IntradayParticipationObservation:
    if direction not in (-1, 1) or isinstance(direction, bool):
        raise ValueError("an explicit stock thesis direction is required")
    source = ActivitySource.model_validate_json(source.canonical_json())
    if market_cutoff.utcoffset() is None or decision_at.utcoffset() is None or market_cutoff > decision_at:
        raise ValueError("intraday cutoffs must be aware and causal")
    cutoff = min(market_cutoff, source.market_time)
    finding = detect_option_participation(source, market_cutoff=cutoff, decision_at=decision_at)
    reasons, gates = list(finding.reasons), []
    disposition = "CONFIRMED" if finding.disposition == "DETECTED" else finding.disposition
    valid_until = finding.valid_until
    if stock is None:
        reasons.append("INTRADAY_STOCK_EVIDENCE_UNAVAILABLE")
        disposition = "UNAVAILABLE"
    else:
        stock = IntradayStockEvidence.model_validate_json(stock.canonical_json())
        if stock.security_id != source.security_id or stock.underlyer != source.underlyer:
            reasons.append("STOCK_IDENTITY_MISMATCH")
            disposition = "UNAVAILABLE"
        else:
            original = {component.key: component for component in stock.components}
            assessed = {}
            for component in stock.components:
                status, missing = component.status, component.reason_codes
                if component.sources:
                    valid_until = min(valid_until, *(origin.valid_until for origin in component.sources))
                if stock.available_at > decision_at:
                    status, missing = "UNAVAILABLE", ("STOCK_NOT_AVAILABLE_AT_DECISION",)
                elif any(origin.market_time > cutoff for origin in component.sources):
                    status, missing = "UNAVAILABLE", ("STOCK_AFTER_OPTION_MARKET_TIME",)
                elif component.interval == "30m" and any(
                    origin.market_time.astimezone(ZoneInfo("America/New_York")).date() != source.volume_session
                    for origin in component.sources
                ):
                    status, missing = "UNAVAILABLE", ("INTRADAY_SOURCE_SESSION_MISMATCH",)
                elif any(decision_at >= origin.valid_until for origin in component.sources):
                    status, missing = "STALE", ("EVIDENCE_EXPIRED_AT_DECISION",)
                assessed[component.key] = BehaviorComponentAssessment(key=component.key, status=status,
                    state=None, metrics=component.metrics if status == "READY" else (), reason_codes=missing)
            gates.append(_metric_gate(assessed, original, gate_id="TREND_SLOPE_30m",
                component_key="TREND.30m", metric_id="ema50_slope10_atr", factor="TREND",
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
    if disposition == "CONFIRMED" and source.contract_type != ("CALL" if direction == 1 else "PUT"):
        disposition = "UNMATCHED"
        reasons.append("ACTIVITY_CONTRACT_NOT_MATCHED_TO_THESIS")
    return IntradayParticipationObservation(security_id=source.security_id, underlyer=source.underlyer,
        direction=direction, market_cutoff=cutoff, decision_at=decision_at, valid_until=valid_until,
        activity=finding, stock_source_sha256=stock.sha256 if stock else None, gates=tuple(gates),
        disposition=disposition, reasons=tuple(sorted(set(reasons))))