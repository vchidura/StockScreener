"""Pure reference gates, geometry and revisioned state transitions."""
from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Context, Decimal, localcontext
from typing import Protocol

from .contracts import (
    CanonicalInput, Decision, Direction, Disposition, FeatureSnapshot, GateStatus,
    GateValue, Phase, Readiness, StateDecision, SuggestedGeometry, Transition,
    canonical_sha256,
)
from .feature_definitions import FEATURE_VERSION
from .feature_engine import FeatureCalculator, compute_features
from .policies import BreakoutPolicy


class ModelEvaluator(Protocol):
    def __call__(self, packet: CanonicalInput, policy: BreakoutPolicy) -> Decision: ...


def state_key(packet: CanonicalInput, policy: BreakoutPolicy) -> str:
    return canonical_sha256({"schema": "equity_episode_key_v1", "security_id": packet.security.security_id,
                             "model": policy.identity, "interval": policy.interval, "adjustment": packet.adjustment})


def _state(packet: CanonicalInput, policy: BreakoutPolicy, disposition: Disposition,
           trigger: str | None, expiry: datetime | None) -> StateDecision:
    prior = packet.prior_state
    key = state_key(packet, policy)
    revision = prior.revision if prior else 0
    if disposition in (Disposition.UNAVAILABLE, Disposition.ERROR):
        return StateDecision(key, revision, prior.phase if prior else Phase.IDLE, Transition.HOLD,
                             prior.trigger_identity if prior else None, prior.valid_until if prior else None,
                             prior.last_input_sha256 if prior else None)
    same_input = prior is not None and prior.last_input_sha256 == packet.observation_sha256
    if same_input:
        return StateDecision(key, revision, prior.phase, Transition.HOLD, prior.trigger_identity,
                             prior.valid_until, prior.last_input_sha256)
    if disposition is Disposition.MATCH:
        transition = Transition.REPLACE if prior and prior.phase is Phase.ACTIVE else Transition.OPEN
        phase = Phase.ACTIVE
    else:
        phase = Phase.IDLE
        transition = Transition.HOLD
        if prior and prior.phase is Phase.ACTIVE:
            transition = Transition.EXPIRE if prior.valid_until and prior.valid_until <= packet.decision_at else Transition.CLOSE
    return StateDecision(key, revision + 1, phase, transition, trigger, expiry, packet.observation_sha256)


def _number(features: FeatureSnapshot, name: str) -> Decimal:
    value = features.values[name]
    if not isinstance(value, Decimal):
        raise ValueError("ready feature must contain a Decimal")
    return value


def _gate(name: str, value: Decimal | bool | None, threshold: Decimal | bool | None, passed: bool | None) -> GateValue:
    status = GateStatus.UNAVAILABLE if passed is None else GateStatus.PASS if passed else GateStatus.FAIL
    return GateValue(name, value, threshold, status)


def evaluate(packet: CanonicalInput, policy: BreakoutPolicy, *,
             feature_calculator: FeatureCalculator = compute_features) -> Decision:
    """Invalid contracts raise; deterministic calculation failures return ERROR.

    A ready observation advances state once. Unavailable/error observations hold
    state; retries of an already applied observation retain the same revision.
    """
    if packet.model != policy.identity:
        raise ValueError("declared model/style/origin/policy identity mismatch")
    if packet.prior_state and packet.prior_state.state_key != state_key(packet, policy):
        raise ValueError("prior state belongs to another model/security/adjustment")
    try:
        features = feature_calculator(packet, policy)
        if features.input_sha256 != packet.observation_sha256 or features.version != FEATURE_VERSION:
            raise ValueError("feature lineage/version mismatch")
        if features.readiness is not Readiness.READY:
            disposition = Disposition.ERROR if features.readiness is Readiness.ERROR else Disposition.UNAVAILABLE
            gates = tuple(_gate(name, None, None, None) for name in ("breakout", "volume", "market", "sector", "event", "validity", "geometry"))
            return Decision(policy.identity, packet.input_sha256, features, disposition, gates, None, None,
                            _state(packet, policy, disposition, None, None), None, features.missing_reasons)
        with localcontext(Context(prec=34)):
            close, high, low = (_number(features, name) for name in ("close", "range_high", "range_low"))
            direction = Direction.LONG if close > high else Direction.SHORT if close < low else None
            sign = Decimal(direction.value) if direction else Decimal(0)
            volume = _number(features, "volume_multiple")
            market = sign * _number(features, "market_trend")
            sector = sign * _number(features, "sector_trend")
            event = features.values["event_risk"]
            if type(event) is not bool:
                raise ValueError("ready event feature must be boolean")
            expiry = packet.bars[-1].clocks.market_end + timedelta(seconds=policy.validity_seconds)
            stop = low if direction is Direction.LONG else high
            target = close + sign * abs(close - stop) * policy.reward_risk
            geometry_ok = direction is not None and target > 0 and sign * (close - stop) > 0
            gates = (
                _gate("breakout", close, high if direction is not Direction.SHORT else low, direction is not None),
                _gate("volume", volume, policy.min_volume_multiple, volume >= policy.min_volume_multiple),
                _gate("market", market, Decimal(0), market > 0),
                _gate("sector", sector, Decimal(0), sector > 0),
                _gate("event", event, False, event is False),
                _gate("validity", Decimal(str((expiry - packet.decision_at).total_seconds())), Decimal(0), expiry > packet.decision_at),
                _gate("geometry", geometry_ok, True, geometry_ok),
            )
            matched = all(gate.status is GateStatus.PASS for gate in gates)
            disposition = Disposition.MATCH if matched else Disposition.NO_MATCH
            trigger = canonical_sha256({"schema": "equity_trigger_v1", "state_key": state_key(packet, policy),
                                       "source": packet.bars[-1].source, "direction": direction}) if matched else None
            geometry = SuggestedGeometry(close, stop, target, direction, packet.decision_at, expiry) if matched and direction else None
            reasons = tuple(f"GATE_{gate.name.upper()}_FAILED" for gate in gates if gate.status is GateStatus.FAIL)
            return Decision(policy.identity, packet.input_sha256, features, disposition, gates,
                            direction if matched else None, trigger,
                            _state(packet, policy, disposition, trigger, expiry if matched else None), geometry, reasons)
    except (ArithmeticError, ValueError, TypeError, KeyError):
        features = FeatureSnapshot(FEATURE_VERSION, packet.observation_sha256, {}, Readiness.ERROR, ("FEATURE_OR_MODEL_CALCULATION_ERROR",))
        return Decision(policy.identity, packet.input_sha256, features, Disposition.ERROR, (), None, None,
                        _state(packet, policy, Disposition.ERROR, None, None), None, features.missing_reasons)
