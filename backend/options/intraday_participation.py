"""O1 v3 latest-completed confirmation with archived v2 policy compatibility."""
from datetime import datetime, timedelta
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5
from zoneinfo import ZoneInfo

from pydantic import AwareDatetime, model_validator

from equity.behavior import (
    BehaviorComponent, BehaviorComponentAssessment, BehaviorMetric, BehaviorProfile, Contract,
    DEFINITION_V1_SHA256, METRICS_V1, Name, Sha256,
)
from options.dual_origin import ACTIVITY_POLICY, ActivityFinding, ActivitySource, detect_option_participation
from options.stock_behavior_gates import StockBehaviorGate, _metric_gate


INTRADAY_PROFILE_V2 = BehaviorProfile(
    name="O1_COMPLETED_30M_V2", definition_sha256=DEFINITION_V1_SHA256,
    required_metrics=(("TREND.30m", ("ema50_slope10_atr",)),
                      ("PARTICIPATION.1d", ("median_dollar_volume20",))),
)


class IntradayAlignmentPolicy(Contract):
    version: Literal["option_participation_stock_alignment_v2"] = "option_participation_stock_alignment_v2"
    activity_policy_sha256: Literal[ACTIVITY_POLICY.sha256] = ACTIVITY_POLICY.sha256
    behavior_definition_sha256: Literal[DEFINITION_V1_SHA256] = DEFINITION_V1_SHA256
    behavior_profile_sha256: Literal[INTRADAY_PROFILE_V2.sha256] = INTRADAY_PROFILE_V2.sha256
    confirmation_basis: Literal["COMPLETED_30M_SLOPE_AND_LIQUIDITY_PRESENCE"] = "COMPLETED_30M_SLOPE_AND_LIQUIDITY_PRESENCE"
    market_cutoff_basis: Literal["ORIGINAL_OPTION_MARKET_TIME"] = "ORIGINAL_OPTION_MARKET_TIME"
    validity_basis: Literal["ORIGINAL_REQUIRED_SOURCE_EXPIRY"] = "ORIGINAL_REQUIRED_SOURCE_EXPIRY"
    output_kind: Literal["OBSERVATION"] = "OBSERVATION"
    execution_permission: Literal[False] = False


INTRADAY_ALIGNMENT_POLICY_V2 = IntradayAlignmentPolicy()


INTRADAY_PROFILE = BehaviorProfile(
    name="O1_LATEST_AVAILABLE_30M_V3", definition_sha256=DEFINITION_V1_SHA256,
    required_metrics=(("TREND.30m", ("ema50_slope10_atr",)),
                      ("PARTICIPATION.1d", ("median_dollar_volume20",))),
)


class LatestCompletedAlignmentPolicy(Contract):
    version: Literal["option_participation_stock_alignment_v3"] = "option_participation_stock_alignment_v3"
    activity_policy_sha256: Literal[ACTIVITY_POLICY.sha256] = ACTIVITY_POLICY.sha256
    behavior_definition_sha256: Literal[DEFINITION_V1_SHA256] = DEFINITION_V1_SHA256
    behavior_profile_sha256: Literal[INTRADAY_PROFILE.sha256] = INTRADAY_PROFILE.sha256
    confirmation_basis: Literal["LATEST_AVAILABLE_COMPLETED_30M_SLOPE_AND_LIQUIDITY_PRESENCE"] = "LATEST_AVAILABLE_COMPLETED_30M_SLOPE_AND_LIQUIDITY_PRESENCE"
    market_cutoff_basis: Literal["ORIGINAL_OPTION_MARKET_TIME"] = "ORIGINAL_OPTION_MARKET_TIME"
    source_session_basis: Literal["CURRENT_SESSION_OR_PREVIOUS_SESSION_CLOSE"] = "CURRENT_SESSION_OR_PREVIOUS_SESSION_CLOSE"
    validity_basis: Literal["ACTIVITY_AND_DAILY_EXPIRY_WITH_30M_STATE_CARRY"] = "ACTIVITY_AND_DAILY_EXPIRY_WITH_30M_STATE_CARRY"
    output_kind: Literal["OBSERVATION"] = "OBSERVATION"
    execution_permission: Literal[False] = False


INTRADAY_ALIGNMENT_POLICY = LatestCompletedAlignmentPolicy()

O1_INDICATOR_METRICS = (
    ("TREND.30m", "ema50_slope10_atr"),
    ("TREND.30m", "adx14"),
    ("MOMENTUM.30m", "return5"),
    ("MOMENTUM.30m", "momentum_change5"),
    ("VOLATILITY.30m", "atr14_fraction"),
    ("LOCATION.30m", "extension_ema21_atr"),
    ("LOCATION.30m", "prior_range20_position"),
    ("PARTICIPATION.1d", "daily_rvol20"),
    ("PARTICIPATION.1d", "median_dollar_volume20"),
)


class O1IndicatorReviewPolicy(Contract):
    version: Literal["option_participation_indicator_review_v1"] = "option_participation_indicator_review_v1"
    confirmation_policy_sha256: Literal[INTRADAY_ALIGNMENT_POLICY.sha256] = INTRADAY_ALIGNMENT_POLICY.sha256
    metric_ids: tuple[Name, ...] = tuple(metric for _, metric in O1_INDICATOR_METRICS)
    absolute_slope_thresholds: tuple[float, ...] = (0.02, 0.05)
    adx_thresholds: tuple[float, ...] = (15.0, 20.0, 25.0)
    absolute_extension_caps: tuple[float, ...] = (1.5, 2.0, 3.0)
    daily_rvol_thresholds: tuple[float, ...] = (1.0, 1.25, 1.5)
    episode_admission: Literal["FIRST_OBSERVATION_PER_DATASET_EPISODE_DIRECTION"] = "FIRST_OBSERVATION_PER_DATASET_EPISODE_DIRECTION"
    same_elapsed_rvol_status: Literal["UNAVAILABLE_NOT_IMPLEMENTED"] = "UNAVAILABLE_NOT_IMPLEMENTED"
    changes_admission: Literal[False] = False
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_policy(self):
        if (self.metric_ids != tuple(metric for _, metric in O1_INDICATOR_METRICS)
                or self.absolute_slope_thresholds != (0.02, 0.05)
                or self.adx_thresholds != (15.0, 20.0, 25.0)
                or self.absolute_extension_caps != (1.5, 2.0, 3.0)
                or self.daily_rvol_thresholds != (1.0, 1.25, 1.5)):
            raise ValueError("O1 indicator review thresholds are frozen")
        return self


O1_INDICATOR_REVIEW_POLICY = O1IndicatorReviewPolicy()
_O1_METRIC_DEFINITIONS = {definition.metric_id: definition for definition in METRICS_V1}
_O1_COMPONENT_METRICS = tuple(dict.fromkeys(key for key, _ in O1_INDICATOR_METRICS))


class O1IndicatorMeasurement(Contract):
    component_key: Name
    metric_id: Name
    unit: Name
    status: Literal["READY", "UNAVAILABLE"]
    value: float | None = None
    reason_codes: tuple[Name, ...] = ()

    @model_validator(mode="after")
    def validate_measurement(self):
        if (self.component_key, self.metric_id) not in O1_INDICATOR_METRICS:
            raise ValueError("unsupported O1 indicator metric")
        if self.unit != _O1_METRIC_DEFINITIONS[self.metric_id].unit:
            raise ValueError("O1 indicator unit differs from the metric catalog")
        if self.status == "READY" and (self.value is None or self.reason_codes):
            raise ValueError("ready O1 indicator requires a value")
        if self.status == "UNAVAILABLE" and (self.value is not None or not self.reason_codes):
            raise ValueError("unavailable O1 indicator requires reasons")
        return self


class O1ChallengerAssessment(Contract):
    challenger_id: Name
    verdict: Literal["PASS", "FAIL", "UNAVAILABLE"]
    metric_ids: tuple[Name, ...]
    reason_codes: tuple[Name, ...] = ()

    @model_validator(mode="after")
    def validate_assessment(self):
        if (self.verdict == "PASS") == bool(self.reason_codes):
            raise ValueError("O1 challenger verdict and reasons disagree")
        return self


class O1IndicatorObservation(Contract):
    schema_version: Literal["option_o1_indicator_observation_v1"] = "option_o1_indicator_observation_v1"
    detector_id: Literal["O1"] = "O1"
    output_kind: Literal["OBSERVATION"] = "OBSERVATION"
    policy: O1IndicatorReviewPolicy = O1_INDICATOR_REVIEW_POLICY
    scheduled_cycle: AwareDatetime
    matrix_id: UUID
    security_id: UUID
    underlyer: Name
    contract_id: int
    snapshot_id: UUID
    episode_id: UUID
    direction: Literal[-1, 1]
    decision_at: AwareDatetime
    valid_until: AwareDatetime
    activity_source_sha256: Sha256
    stock_source_sha256: Sha256 | None
    baseline_disposition: Literal["CONFIRMED", "CONTRADICTED", "UNMATCHED", "UNAVAILABLE", "NOT_DETECTED"]
    baseline_reasons: tuple[Name, ...]
    volume_oi_ratio: float
    measurements: tuple[O1IndicatorMeasurement, ...]
    challengers: tuple[O1ChallengerAssessment, ...]
    package_status: Literal["NOT_ASSESSED"] = "NOT_ASSESSED"
    outcome_status: Literal["NOT_YET_MEASURED"] = "NOT_YET_MEASURED"
    publication_permission: Literal[False] = False
    execution_permission: Literal[False] = False

    @property
    def recurrence_sha256(self):
        import hashlib

        return hashlib.sha256(
            f"{self.policy.sha256}:{self.episode_id}:{self.direction}".encode("ascii")
        ).hexdigest()

    @model_validator(mode="after")
    def validate_observation(self):
        if (self.scheduled_cycle > self.decision_at or self.decision_at >= self.valid_until
                or len(self.measurements) != len(O1_INDICATOR_METRICS)
                or tuple((row.component_key, row.metric_id) for row in self.measurements) != O1_INDICATOR_METRICS
                or len({row.challenger_id for row in self.challengers}) != len(self.challengers)):
            raise ValueError("O1 indicator observation identity, metrics or clocks mismatch")
        return self


class IntradayStockEvidence(Contract):
    schema_version: Literal["option_intraday_stock_evidence_v3"] = "option_intraday_stock_evidence_v3"
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
        if len(set(keys)) != len(keys) or set(keys) - set(_O1_COMPONENT_METRICS):
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
    required_by_key = dict(INTRADAY_PROFILE.required_metrics)
    for key in _O1_COMPONENT_METRICS:
        required = required_by_key.get(key, ())
        diagnostic = tuple(metric_id for component_key, metric_id in O1_INDICATOR_METRICS if component_key == key)
        component = next((row for row in snapshot.components if row.key == key), None)
        if component is None:
            continue
        expected = BEHAVIOR_SOURCE_SELECTION_POLICY if component.interval == "30m" else ADJUSTED_DAILY_HISTORY_POLICY
        metrics = tuple(metric for metric in component.metrics
            if metric.definition.metric_id in diagnostic and metric.status == "READY")
        ready_ids = {metric.definition.metric_id for metric in metrics}
        ready = bool(component.sources) and (set(required) <= ready_ids if required else bool(metrics))
        if any(source.policy_sha256 != expected.sha256 or source.price_basis != (
                "RAW_ACTION_GATED" if component.interval == "30m" else "PROVIDER_SPLIT_ADJUSTED") for source in component.sources):
            ready = False
        selected.append(BehaviorComponent(factor=component.factor, interval=component.interval,
            status="READY" if ready else "UNAVAILABLE", metrics=metrics, sources=component.sources,
            reason_codes=() if ready else ("INTRADAY_REQUIRED_METRIC_OR_POLICY_UNAVAILABLE",)))
    return IntradayStockEvidence(security_id=snapshot.security_id, underlyer=snapshot.ticker,
        available_at=received_at, components=tuple(selected), carrier_snapshot_id=snapshot.snapshot_id,
        carrier_snapshot_sha256=snapshot.sha256)


def _challenger(challenger_id, metric_ids, measurements, predicate):
    values = {row.metric_id: row.value for row in measurements if row.status == "READY"}
    if any(metric_id not in values for metric_id in metric_ids):
        return O1ChallengerAssessment(challenger_id=challenger_id, verdict="UNAVAILABLE",
            metric_ids=metric_ids, reason_codes=("REQUIRED_INDICATOR_UNAVAILABLE",))
    if predicate(values):
        return O1ChallengerAssessment(challenger_id=challenger_id, verdict="PASS", metric_ids=metric_ids)
    return O1ChallengerAssessment(challenger_id=challenger_id, verdict="FAIL",
        metric_ids=metric_ids, reason_codes=("CHALLENGER_THRESHOLD_NOT_SATISFIED",))


def build_o1_indicator_observation(source, decision, stock, *, scheduled_cycle):
    from options.dual_origin import LatestCompletedSignalDecision, load_signal_decision

    source = ActivitySource.model_validate_json(source.canonical_json())
    decision = load_signal_decision(decision)
    if (not isinstance(decision, LatestCompletedSignalDecision)
            or decision.activity is None or decision.activity.disposition != "DETECTED"
            or decision.activity_source_sha256 != source.sha256
            or decision.activity.volume_oi_ratio is None
            or decision.security_id != source.security_id or decision.underlyer != source.underlyer
            or decision.market_cutoff < source.market_time or scheduled_cycle > decision.decision_at):
        raise ValueError("O1 indicator observation requires exact detected activity and v3 decision")
    stock = IntradayStockEvidence.model_validate_json(stock.canonical_json()) if stock is not None else None
    available = {}
    if stock is not None:
        if stock.security_id != source.security_id or stock.underlyer != source.underlyer:
            raise ValueError("O1 indicator stock identity mismatch")
        available = {metric.definition.metric_id: (component.key, metric)
            for component in stock.components for metric in component.metrics if metric.status == "READY"}
    measurements = []
    for component_key, metric_id in O1_INDICATOR_METRICS:
        found = available.get(metric_id)
        definition = _O1_METRIC_DEFINITIONS[metric_id]
        if found is None or found[0] != component_key:
            measurements.append(O1IndicatorMeasurement(component_key=component_key,
                metric_id=metric_id, unit=definition.unit, status="UNAVAILABLE",
                reason_codes=("METRIC_UNAVAILABLE",)))
        else:
            metric = BehaviorMetric.model_validate(found[1].model_dump())
            measurements.append(O1IndicatorMeasurement(component_key=component_key,
                metric_id=metric_id, unit=definition.unit, status="READY", value=metric.value))
    measurements = tuple(measurements)
    direction = decision.direction
    challengers = [O1ChallengerAssessment(challenger_id="BASELINE_V3",
        verdict="PASS" if decision.disposition == "CONFIRMED" else
            "UNAVAILABLE" if decision.disposition == "UNAVAILABLE" else "FAIL",
        metric_ids=("ema50_slope10_atr", "median_dollar_volume20"),
        reason_codes=() if decision.disposition == "CONFIRMED" else
            tuple(decision.reasons or ("BASELINE_NOT_CONFIRMED",)))]
    challengers.append(_challenger("RETURN5_ALIGNMENT", ("return5",), measurements,
        lambda values: direction * values["return5"] > 0))
    challengers.append(_challenger("MOMENTUM_CHANGE5_ALIGNMENT", ("momentum_change5",), measurements,
        lambda values: direction * values["momentum_change5"] >= 0))
    for threshold in O1_INDICATOR_REVIEW_POLICY.absolute_slope_thresholds:
        challengers.append(_challenger(f"SLOPE_ABS_{str(threshold).replace('.', '_')}",
            ("ema50_slope10_atr",), measurements,
            lambda values, threshold=threshold: abs(values["ema50_slope10_atr"]) >= threshold))
    for threshold in O1_INDICATOR_REVIEW_POLICY.adx_thresholds:
        challengers.append(_challenger(f"ADX14_{int(threshold)}", ("adx14",), measurements,
            lambda values, threshold=threshold: values["adx14"] >= threshold))
    for threshold in O1_INDICATOR_REVIEW_POLICY.absolute_extension_caps:
        challengers.append(_challenger(f"EXTENSION_ABS_MAX_{str(threshold).replace('.', '_')}",
            ("extension_ema21_atr",), measurements,
            lambda values, threshold=threshold: abs(values["extension_ema21_atr"]) <= threshold))
    for threshold in O1_INDICATOR_REVIEW_POLICY.daily_rvol_thresholds:
        challengers.append(_challenger(f"DAILY_RVOL20_{str(threshold).replace('.', '_')}",
            ("daily_rvol20",), measurements,
            lambda values, threshold=threshold: values["daily_rvol20"] >= threshold))
    challengers.append(O1ChallengerAssessment(challenger_id="SAME_ELAPSED_30M_RVOL_1_25",
        verdict="UNAVAILABLE", metric_ids=("same_elapsed_30m_rvol",),
        reason_codes=(O1_INDICATOR_REVIEW_POLICY.same_elapsed_rvol_status,)))
    return O1IndicatorObservation(scheduled_cycle=scheduled_cycle, matrix_id=source.matrix_id,
        security_id=source.security_id, underlyer=source.underlyer, contract_id=source.contract_id,
        snapshot_id=source.snapshot_id, episode_id=decision.activity.episode_id, direction=direction,
        decision_at=decision.decision_at, valid_until=decision.valid_until,
        activity_source_sha256=source.sha256, stock_source_sha256=decision.stock_source_sha256,
        baseline_disposition=decision.disposition, baseline_reasons=decision.reasons,
        volume_oi_ratio=float(decision.activity.volume_oi_ratio), measurements=measurements,
        challengers=tuple(challengers))


class IntradayParticipationObservation(Contract):
    schema_version: Literal["option_intraday_participation_observation_v3"] = "option_intraday_participation_observation_v3"
    detector_id: Literal["O1"] = "O1"
    policy: LatestCompletedAlignmentPolicy = INTRADAY_ALIGNMENT_POLICY
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
    from options.calendar import OptionExchangeCalendar

    calendar = OptionExchangeCalendar()
    cutoff = min(market_cutoff, source.market_time)
    current_window = calendar.latest_delayed_slot(cutoff, interval=timedelta(minutes=30),
        provider_delay=timedelta(0), publication_grace=timedelta(0))
    previous_close = calendar.session_close(calendar.previous_session(source.volume_session))
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
                eligible_30m = component.interval == "30m" and component.sources and all(
                    origin.market_time == previous_close or (
                        current_window is not None
                        and origin.market_time.astimezone(ZoneInfo("America/New_York")).date() == source.volume_session
                        and origin.market_time <= current_window
                    ) for origin in component.sources)
                if component.sources and not eligible_30m:
                    valid_until = min(valid_until, *(origin.valid_until for origin in component.sources))
                if stock.available_at > decision_at:
                    status, missing = "UNAVAILABLE", ("STOCK_NOT_AVAILABLE_AT_DECISION",)
                elif any(origin.market_time > cutoff for origin in component.sources):
                    status, missing = "UNAVAILABLE", ("STOCK_AFTER_OPTION_MARKET_TIME",)
                elif component.interval == "30m" and component.sources and not eligible_30m:
                    status, missing = "UNAVAILABLE", ("LATEST_COMPLETED_30M_UNAVAILABLE",)
                elif not eligible_30m and any(decision_at >= origin.valid_until for origin in component.sources):
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