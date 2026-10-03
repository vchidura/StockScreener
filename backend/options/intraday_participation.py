"""O1 v3 latest-completed confirmation with archived v2 policy compatibility."""
from datetime import date, datetime, timedelta
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5
from zoneinfo import ZoneInfo

from pydantic import AwareDatetime, Field, model_validator

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


class CanonicalTrendSourcePolicy(Contract):
    version: Literal["option_canonical_30m_trend_source_v1"] = "option_canonical_30m_trend_source_v1"
    trend_interval: Literal["30m"] = "30m"
    trend_bars: Literal[200] = 200
    trend_history_bars: Literal[202] = 202
    trend_formula_id: Literal["ema50_delta10_over_10_prior_wilder14_v1"] = "ema50_delta10_over_10_prior_wilder14_v1"
    liquidity_interval: Literal["1d"] = "1d"
    liquidity_bars: Literal[21] = 21
    liquidity_formula_id: Literal["prior20_median_close_times_volume_v1"] = "prior20_median_close_times_volume_v1"
    bar_basis: Literal["FINAL_RTH_UNADJUSTED_LATEST_OBSERVED_REVISION"] = "FINAL_RTH_UNADJUSTED_LATEST_OBSERVED_REVISION"
    split_basis: Literal["NO_SPLIT_IN_TREND_WINDOW_WITH_24H_COVERAGE"] = "NO_SPLIT_IN_TREND_WINDOW_WITH_24H_COVERAGE"
    window_basis: Literal["EXACT_LATEST_COMPLETED_CURRENT_SESSION_30M"] = "EXACT_LATEST_COMPLETED_CURRENT_SESSION_30M"
    opening_window: Literal["UNAVAILABLE_BEFORE_FIRST_COMPLETED_30M"] = "UNAVAILABLE_BEFORE_FIRST_COMPLETED_30M"
    prior_session_carry: Literal[False] = False
    execution_permission: Literal[False] = False


CANONICAL_TREND_SOURCE_POLICY = CanonicalTrendSourcePolicy()


class CanonicalTrendAlignmentPolicy(Contract):
    version: Literal["option_participation_stock_alignment_v4"] = "option_participation_stock_alignment_v4"
    activity_policy_sha256: Literal[ACTIVITY_POLICY.sha256] = ACTIVITY_POLICY.sha256
    source_policy_sha256: Literal[CANONICAL_TREND_SOURCE_POLICY.sha256] = CANONICAL_TREND_SOURCE_POLICY.sha256
    confirmation_basis: Literal["EXACT_CURRENT_30M_SLOPE_AND_DAILY_LIQUIDITY_PRESENCE"] = "EXACT_CURRENT_30M_SLOPE_AND_DAILY_LIQUIDITY_PRESENCE"
    market_cutoff_basis: Literal["ORIGINAL_OPTION_MARKET_TIME"] = "ORIGINAL_OPTION_MARKET_TIME"
    validity_basis: Literal["ACTIVITY_EXPIRY_WITH_EXACT_WINDOW_MATCH"] = "ACTIVITY_EXPIRY_WITH_EXACT_WINDOW_MATCH"
    output_kind: Literal["OBSERVATION"] = "OBSERVATION"
    execution_permission: Literal[False] = False


CANONICAL_TREND_ALIGNMENT_POLICY = CanonicalTrendAlignmentPolicy()


class CanonicalTrendStockEvidence(Contract):
    schema_version: Literal["option_canonical_trend_stock_evidence_v1"] = "option_canonical_trend_stock_evidence_v1"
    source_policy_sha256: Literal[CANONICAL_TREND_SOURCE_POLICY.sha256] = CANONICAL_TREND_SOURCE_POLICY.sha256
    security_id: UUID
    underlyer: Name
    session_date: date
    required_window: AwareDatetime | None
    trend_market_time: AwareDatetime | None
    trend_bar_count: int = Field(ge=0, le=200, strict=True)
    trend_bar_ids_sha256: Sha256 | None = None
    trend_last_bar_revision_id: UUID | None = None
    daily_bar_count: int = Field(ge=0, le=21, strict=True)
    daily_bar_ids_sha256: Sha256 | None = None
    available_at: AwareDatetime
    valid_until: AwareDatetime
    status: Literal["READY", "UNAVAILABLE"]
    reason_codes: tuple[Name, ...] = ()
    ema50_slope10_atr: float | None = None
    slope_history: tuple[float, ...] = ()
    adx14: float | None = None
    return5: float | None = None
    momentum_change5: float | None = None
    atr14_fraction: float | None = None
    extension_ema21_atr: float | None = None
    prior_range20_position: float | None = None
    daily_rvol20: float | None = None
    median_dollar_volume20: float | None = None
    prior_session_close: float | None = None
    session_bars: tuple[tuple[AwareDatetime, float, float, float], ...] = ()

    @property
    def market_time(self):
        return self.trend_market_time or self.available_at

    @model_validator(mode="after")
    def validate_evidence(self):
        if len(self.session_bars) > 14:
            raise ValueError("canonical trend evidence exceeds source bounds")
        if self.status == "READY" and (self.reason_codes or self.ema50_slope10_atr is None
                or self.median_dollar_volume20 is None or self.required_window is None
                or self.trend_market_time != self.required_window
                or self.trend_bar_count != CANONICAL_TREND_SOURCE_POLICY.trend_bars
                or self.daily_bar_count != CANONICAL_TREND_SOURCE_POLICY.liquidity_bars
                or self.trend_bar_ids_sha256 is None or self.daily_bar_ids_sha256 is None
                or self.available_at >= self.valid_until):
            raise ValueError("ready canonical trend evidence requires the exact current window and metrics")
        if self.status == "UNAVAILABLE" and not self.reason_codes:
            raise ValueError("unavailable canonical trend evidence requires reasons")
        return self


def latest_completed_30m_window(market_cutoff):
    from options.calendar import OptionExchangeCalendar

    return OptionExchangeCalendar().latest_delayed_slot(market_cutoff, interval=timedelta(minutes=30),
        provider_delay=timedelta(0), publication_grace=timedelta(0))


def _ids_sha256(rows):
    import hashlib

    if not rows:
        return None
    return hashlib.sha256(",".join(str(row["bar_revision_id"]) for row in rows).encode("ascii")).hexdigest()


def build_canonical_trend_evidence(*, security_id, underlyer, session_date, required_window, trend_bars,
                                   daily_bars, split_clear, received_at, session_close):
    """Build the O1 V35 stock trend from exact canonical 30m/1d bars (rows sorted by bar_end)."""
    from equity.behavior_calculators import (
        calculate_atr14_fraction, calculate_location_metrics, calculate_momentum_metrics,
        calculate_participation_metrics, calculate_trend_metrics,
    )

    policy = CANONICAL_TREND_SOURCE_POLICY
    reasons = []
    if required_window is not None and required_window.astimezone(ZoneInfo("America/New_York")).date() != session_date:
        required_window = None
    if required_window is None:
        reasons.append("OPENING_WINDOW_NO_COMPLETED_30M")
    history = [row for row in trend_bars if required_window is not None and row["bar_end"] <= required_window]
    history = history[-policy.trend_history_bars:]
    if any(max(row["created_at"], row["system_observed_at"]) > received_at for row in (*history, *daily_bars)):
        raise ValueError("canonical trend source bars were not available at receipt")
    if required_window is not None and (not history or history[-1]["bar_end"] != required_window):
        reasons.append("EXACT_CURRENT_30M_UNAVAILABLE")
    if len(history) < policy.trend_bars:
        reasons.append("INSUFFICIENT_30M_HISTORY")
    daily = [row for row in daily_bars if row["session_date"] < session_date][-policy.liquidity_bars:]
    if len(daily) < policy.liquidity_bars:
        reasons.append("INSUFFICIENT_DAILY_HISTORY")
    if not split_clear:
        reasons.append("SPLIT_COVERAGE_UNAVAILABLE_OR_CROSSED")
    metrics = {}
    window = history[-policy.trend_bars:]
    if len(window) == policy.trend_bars:
        high, low, close = ([float(row[key]) for row in window] for key in ("high_price", "low_price", "close_price"))
        slope, adx = calculate_trend_metrics(high, low, close, "30m")
        return5, change5 = calculate_momentum_metrics(close, "30m")
        extension, position = calculate_location_metrics(high, low, close, "30m")
        atr_fraction = calculate_atr14_fraction(high, low, close, "30m")
        for metric in (slope, adx, return5, change5, extension, position, atr_fraction):
            if metric.status == "READY":
                metrics[metric.definition.metric_id] = metric.value
        history_slopes = []
        for offset in (2, 1, 0):
            end = len(history) - offset
            prior = history[max(0, end - policy.trend_bars):end]
            if len(prior) == policy.trend_bars:
                value = calculate_trend_metrics(*([float(row[key]) for row in prior]
                    for key in ("high_price", "low_price", "close_price")), "30m")[0]
                if value.status == "READY":
                    history_slopes.append(value.value)
        metrics["slope_history"] = tuple(history_slopes)
    if len(daily) == policy.liquidity_bars:
        rvol, dollar = calculate_participation_metrics([float(row["close_price"]) for row in daily],
            [float(row["volume"]) for row in daily], "1d")
        for metric in (rvol, dollar):
            if metric.status == "READY":
                metrics[metric.definition.metric_id] = metric.value
    if "ema50_slope10_atr" not in metrics and "INSUFFICIENT_30M_HISTORY" not in reasons:
        reasons.append("TREND_METRIC_UNAVAILABLE")
    if "median_dollar_volume20" not in metrics and "INSUFFICIENT_DAILY_HISTORY" not in reasons:
        reasons.append("LIQUIDITY_METRIC_UNAVAILABLE")
    session = [row for row in history if row["session_date"] == session_date][-14:]
    return CanonicalTrendStockEvidence(security_id=security_id, underlyer=underlyer, session_date=session_date,
        required_window=required_window, trend_market_time=window[-1]["bar_end"] if window else None,
        trend_bar_count=len(window), trend_bar_ids_sha256=_ids_sha256(window),
        trend_last_bar_revision_id=window[-1]["bar_revision_id"] if window else None,
        daily_bar_count=len(daily), daily_bar_ids_sha256=_ids_sha256(daily),
        available_at=received_at, valid_until=max(session_close, received_at + timedelta(seconds=1)),
        status="UNAVAILABLE" if reasons else "READY", reason_codes=tuple(reasons),
        ema50_slope10_atr=metrics.get("ema50_slope10_atr"), slope_history=metrics.get("slope_history", ()),
        adx14=metrics.get("adx14"), return5=metrics.get("return5"), momentum_change5=metrics.get("momentum_change5"),
        atr14_fraction=metrics.get("atr14_fraction"), extension_ema21_atr=metrics.get("extension_ema21_atr"),
        prior_range20_position=metrics.get("prior_range20_position"), daily_rvol20=metrics.get("daily_rvol20"),
        median_dollar_volume20=metrics.get("median_dollar_volume20"),
        prior_session_close=float(daily[-1]["close_price"]) if daily else None,
        session_bars=tuple((row["bar_end"].astimezone(ZoneInfo("UTC")), float(row["high_price"]),
            float(row["low_price"]), float(row["close_price"])) for row in session))


def _trend_gate(stock, direction):
    value = stock.ema50_slope10_atr if stock is not None else None
    if value is None:
        return StockBehaviorGate(gate_id="TREND_SLOPE_30m", requirement="REQUIRED", verdict="UNAVAILABLE",
            factor="TREND", component_key="TREND.30m", metric_id="ema50_slope10_atr",
            comparator="GT_ZERO" if direction == 1 else "LT_ZERO", threshold_float=0.,
            reason_codes=("REQUIRED_METRIC_UNAVAILABLE",))
    passes = direction * value > 0
    return StockBehaviorGate(gate_id="TREND_SLOPE_30m", requirement="REQUIRED", verdict="PASS" if passes else "FAIL",
        factor="TREND", component_key="TREND.30m", metric_id="ema50_slope10_atr",
        formula_id=CANONICAL_TREND_SOURCE_POLICY.trend_formula_id,
        comparator="GT_ZERO" if direction == 1 else "LT_ZERO", actual_float=value, threshold_float=0.,
        reason_codes=() if passes else ("STOCK_DIRECTION_NOT_ALIGNED",))


def _liquidity_gate(stock):
    value = stock.median_dollar_volume20 if stock is not None else None
    if value is None:
        return StockBehaviorGate(gate_id="UNDERLYING_LIQUIDITY_EVIDENCE", requirement="REQUIRED", verdict="UNAVAILABLE",
            factor="PARTICIPATION", component_key="PARTICIPATION.1d", metric_id="median_dollar_volume20",
            comparator="PRESENT", reason_codes=("REQUIRED_METRIC_UNAVAILABLE",))
    return StockBehaviorGate(gate_id="UNDERLYING_LIQUIDITY_EVIDENCE", requirement="REQUIRED", verdict="PASS",
        factor="PARTICIPATION", component_key="PARTICIPATION.1d", metric_id="median_dollar_volume20",
        formula_id=CANONICAL_TREND_SOURCE_POLICY.liquidity_formula_id, comparator="PRESENT", actual_float=value)


def assess_canonical_trend(source, stock, *, direction, market_cutoff, decision_at):
    """Return (finding, disposition, reasons, gates, valid_until) for the V35 O1 stock trend check."""
    if direction not in (-1, 1) or isinstance(direction, bool):
        raise ValueError("an explicit stock thesis direction is required")
    source = ActivitySource.model_validate_json(source.canonical_json())
    if market_cutoff.utcoffset() is None or decision_at.utcoffset() is None or market_cutoff > decision_at:
        raise ValueError("canonical trend cutoffs must be aware and causal")
    finding = detect_option_participation(source, market_cutoff=market_cutoff, decision_at=decision_at)
    reasons = list(finding.reasons)
    disposition = "CONFIRMED" if finding.disposition == "DETECTED" else finding.disposition
    valid_until = finding.valid_until
    gates = ()
    if stock is None:
        reasons.append("CANONICAL_TREND_EVIDENCE_UNAVAILABLE")
        disposition = "UNAVAILABLE"
    else:
        stock = CanonicalTrendStockEvidence.model_validate_json(stock.canonical_json())
        expected = latest_completed_30m_window(min(market_cutoff, source.market_time))
        if expected is not None and expected.astimezone(ZoneInfo("America/New_York")).date() != source.volume_session:
            expected = None
        valid_until = min(valid_until, stock.valid_until)
        if stock.security_id != source.security_id or stock.underlyer != source.underlyer:
            reasons.append("STOCK_IDENTITY_MISMATCH")
            disposition = "UNAVAILABLE"
        elif stock.available_at > decision_at:
            reasons.append("STOCK_NOT_AVAILABLE_AT_DECISION")
            disposition = "UNAVAILABLE"
        elif stock.required_window != expected:
            reasons.append("STOCK_WINDOW_MISMATCH")
            disposition = "UNAVAILABLE"
        elif stock.status != "READY":
            reasons.extend(stock.reason_codes)
            disposition = "UNAVAILABLE"
        else:
            gates = (_trend_gate(stock, direction), _liquidity_gate(stock))
            reasons.extend(reason for gate in gates for reason in gate.reason_codes)
            if any(gate.verdict == "UNAVAILABLE" for gate in gates):
                disposition = "UNAVAILABLE"
            elif disposition == "CONFIRMED" and any(gate.verdict == "FAIL" for gate in gates):
                disposition = "CONTRADICTED"
    if disposition == "CONFIRMED" and source.contract_type != ("CALL" if direction == 1 else "PUT"):
        disposition = "UNMATCHED"
        reasons.append("ACTIVITY_CONTRACT_NOT_MATCHED_TO_THESIS")
    if disposition == "CONFIRMED" and decision_at >= valid_until:
        disposition = "UNAVAILABLE"
        reasons.append("STOCK_EVIDENCE_EXPIRED_AT_DECISION")
    return finding, disposition, tuple(sorted(set(reasons))), gates, valid_until


O1_REVIEW_V2_METRICS = (
    ("TREND.30m", "ema50_slope10_atr", "ATR_PER_BAR"),
    ("TREND.30m", "adx14", "OSCILLATOR_0_100"),
    ("TREND.30m", "slope_sign_age_bars", "BARS"),
    ("MOMENTUM.30m", "return5", "FRACTION"),
    ("MOMENTUM.30m", "momentum_change5", "FRACTION"),
    ("VOLATILITY.30m", "atr14_fraction", "FRACTION"),
    ("LOCATION.30m", "extension_ema21_atr", "ATR"),
    ("LOCATION.30m", "prior_range20_position", "RATIO"),
    ("LOCATION.1d", "prior_close_return", "FRACTION"),
    ("STRUCTURE.30m", "higher_low_reclaim", "BOOLEAN"),
    ("PARTICIPATION.1d", "daily_rvol20", "RATIO"),
    ("PARTICIPATION.1d", "median_dollar_volume20", "USD_PER_SESSION"),
    ("MARKET.30m", "spy_ema50_slope10_atr", "ATR_PER_BAR"),
    ("MARKET.30m", "qqq_ema50_slope10_atr", "ATR_PER_BAR"),
    ("ACTIVITY.O1", "prior_same_direction_activity", "BOOLEAN"),
)


class O1IndicatorReviewPolicyV2(Contract):
    version: Literal["option_participation_indicator_review_v2"] = "option_participation_indicator_review_v2"
    confirmation_policy_sha256: Literal[CANONICAL_TREND_ALIGNMENT_POLICY.sha256] = CANONICAL_TREND_ALIGNMENT_POLICY.sha256
    metric_ids: tuple[Name, ...] = tuple(metric for _, metric, _ in O1_REVIEW_V2_METRICS)
    absolute_slope_thresholds: tuple[float, ...] = (0.02, 0.05)
    adx_thresholds: tuple[float, ...] = (15.0, 20.0, 25.0)
    absolute_extension_caps: tuple[float, ...] = (1.5, 2.0, 3.0)
    daily_rvol_thresholds: tuple[float, ...] = (1.0, 1.25, 1.5)
    trend_flip_hold_bars: Literal[2] = 2
    pullback_location_basis: Literal["CONFIRMED_TREND_AT_OR_BEHIND_PRIOR_SESSION_CLOSE"] = "CONFIRMED_TREND_AT_OR_BEHIND_PRIOR_SESSION_CLOSE"
    rebound_structure_basis: Literal["HIGHER_LOW_OR_LOWER_HIGH_THEN_TWO_CLOSES_BEYOND_PRIOR_CLOSE"] = "HIGHER_LOW_OR_LOWER_HIGH_THEN_TWO_CLOSES_BEYOND_PRIOR_CLOSE"
    activity_persistence_window_seconds: Literal[2700] = 2700
    regime_symbols: tuple[Name, ...] = ("SPY", "QQQ")
    episode_admission: Literal["FIRST_OBSERVATION_PER_DATASET_EPISODE_DIRECTION"] = "FIRST_OBSERVATION_PER_DATASET_EPISODE_DIRECTION"
    changes_admission: Literal[False] = False
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_policy(self):
        if (self.metric_ids != tuple(metric for _, metric, _ in O1_REVIEW_V2_METRICS)
                or self.absolute_slope_thresholds != (0.02, 0.05)
                or self.adx_thresholds != (15.0, 20.0, 25.0)
                or self.absolute_extension_caps != (1.5, 2.0, 3.0)
                or self.daily_rvol_thresholds != (1.0, 1.25, 1.5)
                or self.regime_symbols != ("SPY", "QQQ")):
            raise ValueError("O1 indicator review v2 thresholds are frozen")
        return self


O1_INDICATOR_REVIEW_POLICY_V2 = O1IndicatorReviewPolicyV2()
_O1_REVIEW_V2_UNITS = {(key, metric): unit for key, metric, unit in O1_REVIEW_V2_METRICS}


class O1IndicatorMeasurementV2(Contract):
    component_key: Name
    metric_id: Name
    unit: Name
    status: Literal["READY", "UNAVAILABLE"]
    value: float | None = None
    reason_codes: tuple[Name, ...] = ()

    @model_validator(mode="after")
    def validate_measurement(self):
        if _O1_REVIEW_V2_UNITS.get((self.component_key, self.metric_id)) != self.unit:
            raise ValueError("unsupported O1 v2 indicator metric or unit")
        if self.status == "READY" and (self.value is None or self.reason_codes):
            raise ValueError("ready O1 indicator requires a value")
        if self.status == "UNAVAILABLE" and (self.value is not None or not self.reason_codes):
            raise ValueError("unavailable O1 indicator requires reasons")
        return self


class O1IndicatorObservationV2(Contract):
    schema_version: Literal["option_o1_indicator_observation_v2"] = "option_o1_indicator_observation_v2"
    detector_id: Literal["O1"] = "O1"
    output_kind: Literal["OBSERVATION"] = "OBSERVATION"
    policy: O1IndicatorReviewPolicyV2 = O1_INDICATOR_REVIEW_POLICY_V2
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
    measurements: tuple[O1IndicatorMeasurementV2, ...]
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
                or tuple((row.component_key, row.metric_id) for row in self.measurements)
                    != tuple((key, metric) for key, metric, _ in O1_REVIEW_V2_METRICS)
                or len({row.challenger_id for row in self.challengers}) != len(self.challengers)):
            raise ValueError("O1 v2 indicator observation identity, metrics or clocks mismatch")
        return self


def _higher_low_reclaim(stock, direction):
    bars, prior_close = stock.session_bars, stock.prior_session_close
    if prior_close is None or len(bars) < 3:
        return None
    lows = [bar[2] for bar in bars] if direction == 1 else [-bar[1] for bar in bars]
    closes = [direction * (bar[3] - prior_close) for bar in bars]
    structure = lows[-1] > min(lows[:-1])
    return 1.0 if structure and closes[-1] > 0 and closes[-2] > 0 else 0.0


def build_o1_indicator_observation_v2(source, decision, stock, *, scheduled_cycle, regime, prior_same_direction):
    """Record V35 O1 indicators and observation-only challengers; never changes admission."""
    from options.dual_origin import CanonicalTrendSignalDecision, load_signal_decision

    source = ActivitySource.model_validate_json(source.canonical_json())
    decision = load_signal_decision(decision)
    if (not isinstance(decision, CanonicalTrendSignalDecision)
            or decision.activity is None or decision.activity.disposition != "DETECTED"
            or decision.activity_source_sha256 != source.sha256 or decision.activity.volume_oi_ratio is None
            or decision.security_id != source.security_id or decision.underlyer != source.underlyer
            or decision.market_cutoff < source.market_time or scheduled_cycle > decision.decision_at):
        raise ValueError("O1 v2 indicator observation requires exact detected activity and v6 decision")
    stock = CanonicalTrendStockEvidence.model_validate_json(stock.canonical_json()) if stock is not None else None
    direction = decision.direction
    if stock is not None and (stock.security_id != source.security_id or stock.underlyer != source.underlyer):
        raise ValueError("O1 v2 indicator stock identity mismatch")
    age = None
    if stock is not None and stock.slope_history and stock.slope_history[-1] != 0:
        sign = 1 if stock.slope_history[-1] > 0 else -1
        age = 0
        for value in reversed(stock.slope_history):
            if value * sign <= 0:
                break
            age += 1
    last_close = stock.session_bars[-1][3] if stock is not None and stock.session_bars else None
    values = {} if stock is None else {
        "ema50_slope10_atr": stock.ema50_slope10_atr, "adx14": stock.adx14, "slope_sign_age_bars": age,
        "return5": stock.return5, "momentum_change5": stock.momentum_change5, "atr14_fraction": stock.atr14_fraction,
        "extension_ema21_atr": stock.extension_ema21_atr, "prior_range20_position": stock.prior_range20_position,
        "prior_close_return": (last_close / stock.prior_session_close - 1
            if last_close is not None and stock.prior_session_close else None),
        "higher_low_reclaim": _higher_low_reclaim(stock, direction),
        "daily_rvol20": stock.daily_rvol20, "median_dollar_volume20": stock.median_dollar_volume20,
    }
    values["spy_ema50_slope10_atr"] = regime.get("SPY")
    values["qqq_ema50_slope10_atr"] = regime.get("QQQ")
    values["prior_same_direction_activity"] = None if prior_same_direction is None else float(bool(prior_same_direction))
    measurements = tuple(O1IndicatorMeasurementV2(component_key=key, metric_id=metric, unit=unit,
        status="READY", value=float(values[metric])) if values.get(metric) is not None else
        O1IndicatorMeasurementV2(component_key=key, metric_id=metric, unit=unit, status="UNAVAILABLE",
            reason_codes=("METRIC_UNAVAILABLE",)) for key, metric, unit in O1_REVIEW_V2_METRICS)
    ready = {row.metric_id: row.value for row in measurements if row.status == "READY"}

    def challenger(challenger_id, metric_ids, predicate):
        if any(metric_id not in ready for metric_id in metric_ids):
            return O1ChallengerAssessment(challenger_id=challenger_id, verdict="UNAVAILABLE",
                metric_ids=metric_ids, reason_codes=("REQUIRED_INDICATOR_UNAVAILABLE",))
        if predicate(ready):
            return O1ChallengerAssessment(challenger_id=challenger_id, verdict="PASS", metric_ids=metric_ids)
        return O1ChallengerAssessment(challenger_id=challenger_id, verdict="FAIL",
            metric_ids=metric_ids, reason_codes=("CHALLENGER_THRESHOLD_NOT_SATISFIED",))

    confirmed = decision.disposition == "CONFIRMED"
    challengers = [O1ChallengerAssessment(challenger_id="BASELINE_V4",
        verdict="PASS" if confirmed else "UNAVAILABLE" if decision.disposition == "UNAVAILABLE" else "FAIL",
        metric_ids=("ema50_slope10_atr", "median_dollar_volume20"),
        reason_codes=() if confirmed else tuple(decision.reasons or ("BASELINE_NOT_CONFIRMED",)))]
    challengers.append(challenger("RETURN5_ALIGNMENT", ("return5",), lambda v: direction * v["return5"] > 0))
    challengers.append(challenger("MOMENTUM_CHANGE5_ALIGNMENT", ("momentum_change5",),
        lambda v: direction * v["momentum_change5"] >= 0))
    for threshold in O1_INDICATOR_REVIEW_POLICY_V2.absolute_slope_thresholds:
        challengers.append(challenger(f"SLOPE_ABS_{str(threshold).replace('.', '_')}", ("ema50_slope10_atr",),
            lambda v, threshold=threshold: abs(v["ema50_slope10_atr"]) >= threshold))
    for threshold in O1_INDICATOR_REVIEW_POLICY_V2.adx_thresholds:
        challengers.append(challenger(f"ADX14_{int(threshold)}", ("adx14",),
            lambda v, threshold=threshold: v["adx14"] >= threshold))
    for threshold in O1_INDICATOR_REVIEW_POLICY_V2.absolute_extension_caps:
        challengers.append(challenger(f"EXTENSION_ABS_MAX_{str(threshold).replace('.', '_')}", ("extension_ema21_atr",),
            lambda v, threshold=threshold: abs(v["extension_ema21_atr"]) <= threshold))
    for threshold in O1_INDICATOR_REVIEW_POLICY_V2.daily_rvol_thresholds:
        challengers.append(challenger(f"DAILY_RVOL20_{str(threshold).replace('.', '_')}", ("daily_rvol20",),
            lambda v, threshold=threshold: v["daily_rvol20"] >= threshold))
    challengers.append(challenger("TREND_PULLBACK_LOCATION", ("ema50_slope10_atr", "prior_close_return"),
        lambda v: confirmed and direction * v["prior_close_return"] <= 0))
    challengers.append(challenger("TREND_FLIP_HELD_2_BARS", ("ema50_slope10_atr", "slope_sign_age_bars"),
        lambda v: direction * v["ema50_slope10_atr"] > 0
            and v["slope_sign_age_bars"] >= O1_INDICATOR_REVIEW_POLICY_V2.trend_flip_hold_bars))
    rebound_metrics = ("ema50_slope10_atr", "return5", "momentum_change5", "higher_low_reclaim",
        "prior_same_direction_activity")

    def rebound(v):
        return (direction * v["ema50_slope10_atr"] < 0 and direction * v["return5"] > 0
            and direction * v["momentum_change5"] > 0 and v["higher_low_reclaim"] == 1.0
            and v["prior_same_direction_activity"] == 1.0)

    challengers.append(challenger("REBOUND_LANE_STRUCTURAL", rebound_metrics, rebound))
    challengers.append(challenger("MARKET_REGIME_ALIGNED", ("spy_ema50_slope10_atr", "qqq_ema50_slope10_atr"),
        lambda v: direction * v["spy_ema50_slope10_atr"] > 0 and direction * v["qqq_ema50_slope10_atr"] > 0))
    challengers.append(challenger("REBOUND_LANE_WITH_REGIME", (*rebound_metrics, "spy_ema50_slope10_atr",
        "qqq_ema50_slope10_atr"), lambda v: rebound(v) and direction * v["spy_ema50_slope10_atr"] > 0
            and direction * v["qqq_ema50_slope10_atr"] > 0))
    return O1IndicatorObservationV2(scheduled_cycle=scheduled_cycle, matrix_id=source.matrix_id,
        security_id=source.security_id, underlyer=source.underlyer, contract_id=source.contract_id,
        snapshot_id=source.snapshot_id, episode_id=decision.activity.episode_id, direction=direction,
        decision_at=decision.decision_at, valid_until=decision.valid_until,
        activity_source_sha256=source.sha256, stock_source_sha256=decision.stock_source_sha256,
        baseline_disposition=decision.disposition, baseline_reasons=decision.reasons,
        volume_oi_ratio=float(decision.activity.volume_oi_ratio), measurements=measurements,
        challengers=tuple(challengers))


O1_REVIEW_V3_EXTRA_METRICS = (
    ("OPENING.5m", "minutes_since_open", "MINUTES"),
    ("OPENING.5m", "gap_open_return", "FRACTION"),
    ("OPENING.15m", "opening_range_break_held", "BOOLEAN"),
    ("MARKET.5m", "spy_session_return", "FRACTION"),
    ("MARKET.5m", "qqq_session_return", "FRACTION"),
    ("CONTRACT", "activity_dte", "DAYS"),
    ("CONTRACT", "activity_iv", "ANNUAL_VOL_FRACTION"),
    ("VOLATILITY.1d", "rv20_cc_annual", "ANNUAL_VOL_FRACTION"),
)
O1_REVIEW_V3_METRICS = (*O1_REVIEW_V2_METRICS, *O1_REVIEW_V3_EXTRA_METRICS)
O1_DTE_BUCKETS = ((1, 7), (8, 21), (22, 45), (46, 60))
O1_EXPRESSION_STRATEGIES = ("DIRECTIONAL_LONG_PREMIUM", "DIRECTIONAL_DEBIT_SPREAD")


class O1IndicatorReviewPolicyV3(Contract):
    version: Literal["option_participation_indicator_review_v3"] = "option_participation_indicator_review_v3"
    confirmation_policy_sha256: Literal[CANONICAL_TREND_ALIGNMENT_POLICY.sha256] = CANONICAL_TREND_ALIGNMENT_POLICY.sha256
    base_review_policy_sha256: Literal[O1_INDICATOR_REVIEW_POLICY_V2.sha256] = O1_INDICATOR_REVIEW_POLICY_V2.sha256
    metric_ids: tuple[Name, ...] = tuple(metric for _, metric, _ in O1_REVIEW_V3_METRICS)
    opening_window_minutes: Literal[60] = 60
    opening_range_minutes: Literal[15] = 15
    opening_hold_closes: Literal[2] = 2
    dte_buckets: tuple[tuple[int, int], ...] = O1_DTE_BUCKETS
    expression_strategies: tuple[Name, ...] = O1_EXPRESSION_STRATEGIES
    expression_selection: Literal["LOWEST_RANK_PER_STRATEGY_AND_DTE_BUCKET_MATCHING_DIRECTION"] = "LOWEST_RANK_PER_STRATEGY_AND_DTE_BUCKET_MATCHING_DIRECTION"
    signal_expression_basis: Literal["UNDERLYER_DIRECTION_SIGNAL_SEPARATE_FROM_EXPRESSION_CONTRACT"] = "UNDERLYER_DIRECTION_SIGNAL_SEPARATE_FROM_EXPRESSION_CONTRACT"
    expected_move_basis: Literal["SPOT_TIMES_LONG_LEG_LOCAL_IV_TIMES_SQRT_DTE_OVER_365"] = "SPOT_TIMES_LONG_LEG_LOCAL_IV_TIMES_SQRT_DTE_OVER_365"
    realized_volatility_basis: Literal["PRIOR20_CLOSE_LOG_RETURN_STD_DDOF1_SQRT252"] = "PRIOR20_CLOSE_LOG_RETURN_STD_DDOF1_SQRT252"
    pricing_basis: Literal["ORIGINAL_MODEL_MARKS_NO_QUOTES"] = "ORIGINAL_MODEL_MARKS_NO_QUOTES"
    changes_admission: Literal[False] = False
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_policy(self):
        if (self.metric_ids != tuple(metric for _, metric, _ in O1_REVIEW_V3_METRICS)
                or self.dte_buckets != O1_DTE_BUCKETS or self.expression_strategies != O1_EXPRESSION_STRATEGIES):
            raise ValueError("O1 indicator review v3 policy is frozen")
        return self


O1_INDICATOR_REVIEW_POLICY_V3 = O1IndicatorReviewPolicyV3()
_O1_REVIEW_V3_UNITS = {(key, metric): unit for key, metric, unit in O1_REVIEW_V3_METRICS}


class O1IndicatorMeasurementV3(O1IndicatorMeasurementV2):
    @model_validator(mode="after")
    def validate_measurement(self):
        if _O1_REVIEW_V3_UNITS.get((self.component_key, self.metric_id)) != self.unit:
            raise ValueError("unsupported O1 v3 indicator metric or unit")
        if self.status == "READY" and (self.value is None or self.reason_codes):
            raise ValueError("ready O1 indicator requires a value")
        if self.status == "UNAVAILABLE" and (self.value is not None or not self.reason_codes):
            raise ValueError("unavailable O1 indicator requires reasons")
        return self


class O1MatchedExpression(Contract):
    strategy_name: Name
    structure_type: Name
    dte_bucket: Name
    candidate_id: UUID
    candidate_rank: int
    dte: int
    is_activity_contract: bool
    net_premium: float
    maximum_loss: float | None = None
    maximum_profit: float | None = None
    breakeven: float | None = None
    spot: float
    long_delta: float | None = None
    long_iv: float | None = None
    expected_move: float | None = None
    breakeven_to_expected_move: float | None = None
    theta_per_maximum_loss: float | None = None
    vega_per_maximum_loss: float | None = None
    iv_over_rv20: float | None = None


class O1IndicatorObservationV3(Contract):
    schema_version: Literal["option_o1_indicator_observation_v3"] = "option_o1_indicator_observation_v3"
    detector_id: Literal["O1"] = "O1"
    output_kind: Literal["OBSERVATION"] = "OBSERVATION"
    policy: O1IndicatorReviewPolicyV3 = O1_INDICATOR_REVIEW_POLICY_V3
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
    measurements: tuple[O1IndicatorMeasurementV3, ...]
    challengers: tuple[O1ChallengerAssessment, ...]
    matched_expressions: tuple[O1MatchedExpression, ...] = ()
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
                or tuple((row.component_key, row.metric_id) for row in self.measurements)
                    != tuple((key, metric) for key, metric, _ in O1_REVIEW_V3_METRICS)
                or len({row.challenger_id for row in self.challengers}) != len(self.challengers)
                or len(self.matched_expressions) > len(O1_DTE_BUCKETS) * len(O1_EXPRESSION_STRATEGIES)):
            raise ValueError("O1 v3 indicator observation identity, metrics or clocks mismatch")
        return self


def dte_bucket(dte):
    return next((f"{low:02d}-{high:02d}" for low, high in O1_DTE_BUCKETS if low <= dte <= high), None)


def realized_volatility20(closes):
    import math

    values = [float(value) for value in closes][-21:]
    if len(values) < 21 or any(value <= 0 for value in values):
        return None
    returns = [math.log(current / previous) for previous, current in zip(values, values[1:])]
    mean = sum(returns) / len(returns)
    return math.sqrt(sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)) * math.sqrt(252)


def matched_expressions(candidates, *, underlyer, direction, session_date, activity_contract_ids, rv20):
    """Lowest-rank candidate per expression strategy and DTE bucket for one underlyer direction (shadow only)."""
    import math

    best = {}
    for candidate in candidates:
        if candidate.underlyer != underlyer or candidate.strategy_name not in O1_EXPRESSION_STRATEGIES:
            continue
        long_leg = next((leg for leg in candidate.legs if leg.side.value == "BUY"), None)
        if long_leg is None or long_leg.contract_type.value != ("CALL" if direction == 1 else "PUT"):
            continue
        dte = min((leg.expiration_date - session_date).days for leg in candidate.legs)
        bucket = dte_bucket(dte)
        if bucket is None:
            continue
        key = (candidate.strategy_name, bucket)
        if key in best and best[key].rank <= candidate.rank:
            continue
        best[key] = candidate
    results = []
    for (strategy, bucket), candidate in sorted(best.items()):
        long_leg = next(leg for leg in candidate.legs if leg.side.value == "BUY")
        dte = min((leg.expiration_date - session_date).days for leg in candidate.legs)
        spot = float(long_leg.spot)
        iv = float(long_leg.local_iv) if long_leg.local_iv else None
        expected = spot * iv * math.sqrt(max(dte, 1) / 365) if iv else None
        breakeven = float(candidate.breakevens[0]) if candidate.breakevens else None
        maximum_loss = float(candidate.maximum_loss) if candidate.maximum_loss is not None else None
        signed = [(1 if leg.side.value == "BUY" else -1) * leg.ratio * leg.multiplier for leg in candidate.legs]
        theta = (sum(sign * float(leg.local_theta_per_day) for sign, leg in zip(signed, candidate.legs))
            if all(leg.local_theta_per_day is not None for leg in candidate.legs) else None)
        vega = (sum(sign * float(leg.local_vega_per_vol_point) for sign, leg in zip(signed, candidate.legs))
            if all(leg.local_vega_per_vol_point is not None for leg in candidate.legs) else None)
        results.append(O1MatchedExpression(strategy_name=strategy, structure_type=candidate.structure_type.value,
            dte_bucket=bucket, candidate_id=candidate.candidate_id, candidate_rank=candidate.rank, dte=dte,
            is_activity_contract=long_leg.contract_id in activity_contract_ids,
            net_premium=float(candidate.net_premium), maximum_loss=maximum_loss,
            maximum_profit=float(candidate.maximum_profit) if candidate.maximum_profit is not None else None,
            breakeven=breakeven, spot=spot,
            long_delta=float(long_leg.local_delta) if long_leg.local_delta is not None else None, long_iv=iv,
            expected_move=expected,
            breakeven_to_expected_move=abs(breakeven - spot) / expected if expected and breakeven is not None else None,
            theta_per_maximum_loss=theta / maximum_loss if theta is not None and maximum_loss else None,
            vega_per_maximum_loss=vega / maximum_loss if vega is not None and maximum_loss else None,
            iv_over_rv20=iv / rv20 if iv and rv20 else None))
    return tuple(results)


def opening_measurements(rows, *, session_date, session_open, decision_at, direction, prior_close):
    """Observation-only opening lane from completed canonical 5m/15m bars of the current session."""
    five = sorted((row for row in rows if row["interval"] == "5m" and row["session_date"] == session_date
        and row["bar_end"] <= decision_at),
        key=lambda row: row["bar_end"])
    fifteen = [row for row in rows if row["interval"] == "15m" and row["session_date"] == session_date
        and row["bar_end"] == session_open + timedelta(minutes=15) and row["bar_end"] <= decision_at]
    values = dict(minutes_since_open=max(0.0, (decision_at - session_open).total_seconds() / 60))
    if five and prior_close:
        values["gap_open_return"] = float(five[0]["open_price"]) / prior_close - 1
    if fifteen:
        high, low = float(fifteen[0]["high_price"]), float(fifteen[0]["low_price"])
        after = [row for row in five if row["bar_end"] > session_open + timedelta(minutes=15)]
        if len(after) >= 2 and after[-1]["bar_end"] - after[-2]["bar_end"] == timedelta(minutes=5):
            edge = high if direction == 1 else low
            values["opening_range_break_held"] = float(all(
                direction * (float(row["close_price"]) - edge) > 0 for row in after[-2:]))
    return values


def build_o1_indicator_observation_v3(source, decision, stock, *, scheduled_cycle, regime, prior_same_direction,
                                      extras, expressions):
    """V36: V2 indicators plus opening lane, DTE/volatility context and shadow matched expressions."""
    base = build_o1_indicator_observation_v2(source, decision, stock, scheduled_cycle=scheduled_cycle,
        regime=regime, prior_same_direction=prior_same_direction)
    direction = base.direction
    measurements = tuple(O1IndicatorMeasurementV3.model_validate(row.model_dump()) for row in base.measurements)
    measurements += tuple(O1IndicatorMeasurementV3(component_key=key, metric_id=metric, unit=unit,
        status="READY", value=float(extras[metric])) if extras.get(metric) is not None else
        O1IndicatorMeasurementV3(component_key=key, metric_id=metric, unit=unit, status="UNAVAILABLE",
            reason_codes=("METRIC_UNAVAILABLE",)) for key, metric, unit in O1_REVIEW_V3_EXTRA_METRICS)
    ready = {row.metric_id: row.value for row in measurements if row.status == "READY"}

    def challenger(challenger_id, metric_ids, predicate):
        if any(metric_id not in ready for metric_id in metric_ids):
            return O1ChallengerAssessment(challenger_id=challenger_id, verdict="UNAVAILABLE",
                metric_ids=metric_ids, reason_codes=("REQUIRED_INDICATOR_UNAVAILABLE",))
        if predicate(ready):
            return O1ChallengerAssessment(challenger_id=challenger_id, verdict="PASS", metric_ids=metric_ids)
        return O1ChallengerAssessment(challenger_id=challenger_id, verdict="FAIL",
            metric_ids=metric_ids, reason_codes=("CHALLENGER_THRESHOLD_NOT_SATISFIED",))

    policy = O1_INDICATOR_REVIEW_POLICY_V3
    challengers = list(base.challengers)
    challengers.append(challenger("OPENING_LANE_RANGE_BREAK_HELD", ("minutes_since_open", "opening_range_break_held",
        "spy_session_return"), lambda v: v["minutes_since_open"] <= policy.opening_window_minutes
            and v["opening_range_break_held"] == 1.0 and direction * v["spy_session_return"] > 0))
    challengers.append(challenger("OPENING_GAP_ALIGNED", ("gap_open_return",), lambda v: direction * v["gap_open_return"] > 0))
    challengers.append(challenger("ACTIVITY_DTE_8_PLUS", ("activity_dte",), lambda v: v["activity_dte"] >= 8))
    challengers.append(challenger("ACTIVITY_IV_BELOW_RV20", ("activity_iv", "rv20_cc_annual"),
        lambda v: v["activity_iv"] < v["rv20_cc_annual"]))
    return O1IndicatorObservationV3(**{**base.model_dump(exclude={"schema_version", "policy", "measurements", "challengers"}),
        "measurements": measurements, "challengers": tuple(challengers), "matched_expressions": tuple(expressions)})