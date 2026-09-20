"""O2 source-bound surface observations; no package or trading permissions."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import AwareDatetime, Field, model_validator

from equity.behavior import Contract, DEFINITION_V1_SHA256, Name, OPTIONS_SWING_PROFILE, Sha256, StockBehaviorSnapshot
from options.analytics.smile import SmileInput, fit_smile_groups, qualifying_distortions
from options.calendar import OptionExchangeCalendar
from options.domain import ContractType


class SurfacePolicy(Contract):
    version: Literal["option_local_surface_origin_v1"] = "option_local_surface_origin_v1"
    minimum_strikes: Literal[7] = 7
    minimum_absolute_robust_z: Literal[2.5] = 2.5
    maximum_source_age_seconds: Literal[1800] = 1800
    stock_context: Literal["DAILY_RV20_AND_ATR14_NOT_MATCHED_IV_RV_FORECAST"] = "DAILY_RV20_AND_ATR14_NOT_MATCHED_IV_RV_FORECAST"
    output_kind: Literal["OBSERVATION"] = "OBSERVATION"
    execution_permission: Literal[False] = False


SURFACE_POLICY = SurfacePolicy()


class SurfacePoint(Contract):
    contract_id: int = Field(strict=True, gt=0)
    contract_ticker: Name
    snapshot_id: UUID
    snapshot_sha256: Sha256
    strike: Decimal = Field(gt=0)
    local_iv: float = Field(gt=0, allow_inf_nan=False)


class SurfaceSource(Contract):
    schema_version: Literal["option_local_surface_source_v1"] = "option_local_surface_source_v1"
    security_id: UUID
    underlyer: Name
    matrix_id: UUID
    scheduled_cycle: AwareDatetime
    batch_id: UUID
    configuration_sha256: Sha256
    market_policy_sha256: Sha256
    valuation_policy_sha256: Sha256
    contract_type: Literal["CALL", "PUT"]
    expiration_date: date
    expiration_cutoff: AwareDatetime
    spot: Decimal = Field(gt=0)
    market_time: AwareDatetime
    observed_at: AwareDatetime
    recorded_at: AwareDatetime
    received_at: AwareDatetime
    points: tuple[SurfacePoint, ...] = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def validate_source(self):
        if (not self.market_time <= self.observed_at <= self.recorded_at <= self.received_at
            or not self.market_time <= self.scheduled_cycle <= self.received_at):
            raise ValueError("surface source requires causal actual receipts")
        if self.expiration_cutoff <= self.market_time:
            raise ValueError("surface expired at source")
        if (len({point.contract_id for point in self.points}) != len(self.points)
                or len({point.snapshot_id for point in self.points}) != len(self.points)
                or len({point.strike for point in self.points}) != len(self.points)):
            raise ValueError("surface requires distinct exact contracts, snapshots and strikes")
        if self.points != tuple(sorted(self.points, key=lambda point: (point.strike, point.contract_id))):
            raise ValueError("surface points must use canonical strike order")
        return self


def bind_surface_source(*, snapshots, lineage, security, recorded_at, received_at):
    if not 1 <= len(snapshots) <= 256:
        raise ValueError("surface source exceeds contract bound")
    first = snapshots[0]
    if (not security.active or security.ticker != first.underlyer
            or security.effective_from > first.market_data_time or security.observed_at > received_at):
        raise ValueError("surface dated security identity unavailable")
    if lineage.underlying != first.underlyer or first.market_data_time > lineage.market_time:
        raise ValueError("surface matrix scope mismatch")
    coherent = ("batch_id", "underlyer", "contract_type", "expiration_date", "expiration_cutoff",
        "spot", "spot_market_data_time", "market_data_time", "valuation_policy_sha256", "model_version")
    for snapshot in snapshots:
        if (any(getattr(snapshot, field) != getattr(first, field) for field in coherent)
                or not snapshot.iv_converged or snapshot.quality_flags or snapshot.revised_observed_at is not None
                or snapshot.first_observed_at > lineage.observed_time
                or snapshot.mark_market_data_time != first.market_data_time
                or snapshot.spot_market_data_time != first.market_data_time):
            raise ValueError("surface points require one coherent unrevised valuation batch")
    return SurfaceSource(security_id=security.security_id, underlyer=first.underlyer,
        matrix_id=lineage.matrix_id, scheduled_cycle=lineage.scheduled_cycle, batch_id=first.batch_id,
        configuration_sha256=lineage.configuration_sha256, market_policy_sha256=lineage.market_policy_sha256,
        valuation_policy_sha256=first.valuation_policy_sha256, contract_type=first.contract_type.value,
        expiration_date=first.expiration_date, expiration_cutoff=first.expiration_cutoff, spot=first.spot,
        market_time=first.market_data_time, observed_at=max(row.first_observed_at for row in snapshots),
        recorded_at=recorded_at, received_at=received_at,
        points=tuple(SurfacePoint(contract_id=row.contract_id, contract_ticker=row.contract_ticker,
            snapshot_id=row.snapshot_id, snapshot_sha256=row.normalized_payload_sha256,
            strike=row.strike, local_iv=row.local_iv) for row in sorted(snapshots, key=lambda row: (row.strike, row.contract_id))))


class SurfaceFinding(Contract):
    episode_id: UUID
    contract_id: int = Field(strict=True, gt=0)
    snapshot_id: UUID
    strike: Decimal = Field(gt=0)
    local_iv: float
    fitted_iv: float
    residual: float
    robust_z: float
    neighbors_consistent: Literal[True] = True
    is_edge: Literal[False] = False


class SurfaceDecision(Contract):
    schema_version: Literal["option_local_surface_decision_v1"] = "option_local_surface_decision_v1"
    detector_id: Literal["O2"] = "O2"
    origin: Literal["OPTIONS_FIRST"] = "OPTIONS_FIRST"
    policy_sha256: Sha256 = SURFACE_POLICY.sha256
    source_sha256: Sha256
    security_id: UUID
    underlyer: Name
    matrix_id: UUID
    scheduled_cycle: AwareDatetime
    expiration_date: date
    contract_type: Literal["CALL", "PUT"]
    decision_at: AwareDatetime
    market_cutoff: AwareDatetime
    valid_until: AwareDatetime
    finding_disposition: Literal["DETECTED", "NOT_DETECTED", "UNAVAILABLE"]
    findings: tuple[SurfaceFinding, ...]
    reasons: tuple[Name, ...]
    input_count: int
    coefficients: tuple[float, ...] = ()
    residual_mad: float | None = None
    stock_snapshot_sha256: Sha256 | None
    stock_context_status: Literal["READY", "UNAVAILABLE"]
    stock_metrics: tuple[tuple[Name, float], ...]
    event_context_status: Literal["BLOCKED", "UNAVAILABLE"]
    event_context_sha256: Sha256 | None
    context_until: AwareDatetime | None
    output_kind: Literal["OBSERVATION"] = "OBSERVATION"
    category: Literal["NEUTRAL_VOL"] = "NEUTRAL_VOL"
    direction: None = None
    package_status: Literal["NOT_APPLICABLE"] = "NOT_APPLICABLE"
    probability: None = None
    publication_permission: Literal[False] = False
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_decision(self):
        if self.policy_sha256 != SURFACE_POLICY.sha256 or not self.scheduled_cycle <= self.market_cutoff <= self.decision_at:
            raise ValueError("unsupported surface policy or decision clock")
        if (self.finding_disposition == "DETECTED") != bool(self.findings):
            raise ValueError("surface detection requires qualifying residuals")
        if self.findings and (self.decision_at >= self.valid_until or any(abs(row.robust_z) < 2.5 for row in self.findings)):
            raise ValueError("surface finding requires timely qualifying residuals")
        if self.stock_context_status == "READY" and (self.stock_snapshot_sha256 is None
                or {name for name, _ in self.stock_metrics} != {"rv20_cc_annual", "atr14_fraction"}):
            raise ValueError("surface stock context requires exact volatility metrics")
        return self


def assess_surface_first(source, stock, *, market_cutoff, decision_at, context_until=None, event_detail=None, calendar=None):
    import hashlib
    from options.alert_plans import _canonical
    from options.alert_qualification import retained_event_horizon

    if market_cutoff.utcoffset() is None or decision_at.utcoffset() is None or market_cutoff > decision_at:
        raise ValueError("surface cutoffs must be aware and causal")
    source = SurfaceSource.model_validate_json(source.canonical_json())
    calendar = calendar or OptionExchangeCalendar()
    session = calendar.session_for_slot(source.market_time)
    valid_until = min(calendar.session_close(session), source.expiration_cutoff,
        source.market_time + timedelta(seconds=SURFACE_POLICY.maximum_source_age_seconds))
    reasons, findings, fits = [], [], ()
    unavailable = source.received_at > decision_at or source.market_time > market_cutoff or decision_at >= valid_until
    if source.scheduled_cycle > market_cutoff:
        raise ValueError("surface cycle exceeds market cutoff")
    if unavailable:
        reasons.append("SURFACE_NOT_AVAILABLE_OR_EXPIRED")
    else:
        fits = fit_smile_groups(tuple(SmileInput(point.contract_id, ContractType(source.contract_type),
            source.expiration_date, point.strike, source.spot, point.local_iv) for point in source.points),
            minimum_strikes=SURFACE_POLICY.minimum_strikes)
        by_contract = {point.contract_id: point for point in source.points}
        for fit in fits:
            for residual in qualifying_distortions(fit, minimum_absolute_robust_z=SURFACE_POLICY.minimum_absolute_robust_z):
                point = by_contract[residual.contract_id]
                findings.append(SurfaceFinding(episode_id=uuid5(NAMESPACE_URL,
                    f"surface:{SURFACE_POLICY.sha256}:{source.security_id}:{point.contract_id}:{session}"),
                    contract_id=point.contract_id, snapshot_id=point.snapshot_id, strike=point.strike,
                    local_iv=residual.local_iv, fitted_iv=residual.fitted_iv, residual=residual.residual,
                    robust_z=residual.robust_z))
        if not findings:
            reasons.append("NO_QUALIFYING_LOCAL_SURFACE_DISTORTION")
    metrics = []
    if stock is not None:
        stock = StockBehaviorSnapshot.model_validate_json(stock.canonical_json())
        if (stock.security_id == source.security_id and stock.ticker == source.underlyer
                and stock.definition_sha256 == DEFINITION_V1_SHA256 and stock.policy_sha256 == OPTIONS_SWING_PROFILE.sha256
                and stock.market_time <= market_cutoff and stock.available_at <= decision_at < stock.valid_until):
            component = next((row for row in stock.assess_at(decision_at).components if row.key == "VOLATILITY.1d" and row.status == "READY"), None)
            if component is not None:
                metrics = [(metric.definition.metric_id, metric.value) for metric in component.metrics
                    if metric.definition.metric_id in {"rv20_cc_annual", "atr14_fraction"} and metric.value is not None]
    context_ready = {name for name, _ in metrics} == {"rv20_cc_annual", "atr14_fraction"}
    if not context_ready:
        reasons.append("STOCK_VOLATILITY_CONTEXT_UNAVAILABLE")
    event_status, event_hash = "UNAVAILABLE", None
    if context_until is not None:
        if context_until.utcoffset() is None or not decision_at < context_until <= source.expiration_cutoff:
            raise ValueError("surface event context requires an explicit valid horizon")
        detail = dict(event_detail or {})
        detail.update(candidate={"underlying": source.underlyer, "observed_time": decision_at}, source_snapshots=[])
        event = retained_event_horizon(detail, decision_at, context_until)
        event_status = event["status"]
        event_hash = hashlib.sha256(_canonical(event).encode("ascii")).hexdigest()
    reasons.append("EVENT_CONTEXT_BLOCKED" if event_status == "BLOCKED" else "EVENT_CONTEXT_UNAVAILABLE")
    return SurfaceDecision(source_sha256=source.sha256, security_id=source.security_id, underlyer=source.underlyer,
        matrix_id=source.matrix_id, scheduled_cycle=source.scheduled_cycle,
        expiration_date=source.expiration_date, contract_type=source.contract_type,
        decision_at=decision_at, market_cutoff=market_cutoff, valid_until=valid_until,
        finding_disposition="UNAVAILABLE" if unavailable else "DETECTED" if findings else "NOT_DETECTED",
        findings=tuple(findings), reasons=tuple(reasons), input_count=len(source.points),
        coefficients=fits[0].coefficients if fits else (), residual_mad=fits[0].mad if fits else None,
        stock_snapshot_sha256=stock.sha256 if stock else None, stock_context_status="READY" if context_ready else "UNAVAILABLE",
        stock_metrics=tuple(sorted(metrics)), event_context_status=event_status,
        event_context_sha256=event_hash, context_until=context_until)