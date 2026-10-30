"""Provider-free contract, model, state-machine and actual V2 envelope parity."""
from __future__ import annotations

import ast
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, localcontext
import importlib
import inspect
import json
from pathlib import Path
from typing import get_type_hints
from uuid import UUID

import pytest

from equity.model_detection.codec import decode_input
from equity.model_detection.contracts import (
    AdjustmentBasis, Direction, Disposition, GateStatus, Phase, Readiness,
    Transition, canonical_json, canonical_sha256,
)
from equity.model_detection.engine import evaluate, state_key
from equity.model_detection.legacy_adapter import FrozenResearchRecord, run_legacy
from equity.model_detection.policies import BreakoutPolicy
from equity.model_detection.v2_adapter import QualifiedV2Materialization, run_v2


FIXTURE = Path(__file__).parent / "fixtures" / "equity_model_breakout_v1.json"
POLICY = BreakoutPolicy()
COHORT = UUID("22222222-2222-4222-8222-222222222222")
SNAPSHOT = UUID("33333333-3333-4333-8333-333333333333")


@pytest.fixture
def wire():
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def adapters(wire, *, cohort=COHORT, snapshot=SNAPSHOT):
    # Imports belong to the caller. The shared adapter never imports these modules.
    from realtime.detector_adapters import StableDetectorAdapterResult
    from realtime.detector_evidence import RealtimeDetectorInput
    from realtime.preflight import ProviderTier

    digest = canonical_sha256(wire)
    legacy = run_legacy(FrozenResearchRecord("frozen-research-control", wire, digest), POLICY)
    v2 = run_v2(QualifiedV2Materialization(snapshot, wire, digest, True), POLICY,
                cohort_id=cohort, provider_tier=ProviderTier.MASSIVE_ADVANCED,
                result_factory=StableDetectorAdapterResult, input_factory=RealtimeDetectorInput)
    assert isinstance(v2.adapter, StableDetectorAdapterResult)
    assert all(isinstance(item, RealtimeDetectorInput) for item in v2.inputs)
    return legacy, v2


def test_legacy_flat_research_rows_preserve_canonical_fixture(wire):
    rows = []
    for bar in wire["bars"]:
        rows.append({"security_id": bar["source"]["security_id"], "ticker": wire["security"]["ticker"],
                     "source_id": bar["source"]["source_id"], "source_revision": bar["source"]["revision"],
                     "revision_id": bar["source"]["source_id"], "payload_sha256": bar["source"]["payload_sha256"],
                     "bar_start": bar["clocks"]["market_start"], "bar_end": bar["clocks"]["market_end"],
                     "visible_at": bar["clocks"]["available_at"], "system_observed_at": bar["clocks"]["received_at"],
                     "created_at": bar["clocks"]["received_at"], "interval": bar["interval"],
                     "adjustment_basis": bar["adjustment"]["basis"], "action_sha256": bar["adjustment"]["action_sha256"],
                     "adjustment_version": bar["adjustment"]["version"],
                     "completed": bar["completed"], **{key: float(bar[key]) for key in ("open", "high", "low", "close", "volume")}})
    record = FrozenResearchRecord.from_research_rows("flat-legacy", rows, input_context=decode_input(wire))
    legacy = run_legacy(record, POLICY)
    _, v2 = adapters(wire)
    assert legacy.canonical_input == v2.canonical_input
    assert legacy.decision == v2.decision
    rows[-1]["ticker"] = "OTHER"
    with pytest.raises(ValueError, match="identity mismatch"):
        FrozenResearchRecord.from_research_rows("flat-legacy", rows, input_context=decode_input(wire))


def test_frozen_match_exact_adapter_parity(wire):
    legacy, v2 = adapters(wire)
    a, b = legacy.decision, v2.decision
    assert legacy.canonical_input == v2.canonical_input
    assert a == b
    assert a.features.feature_sha256 == b.features.feature_sha256
    assert a.model.policy_sha256 == b.model.policy_sha256 == POLICY.policy_sha256
    assert a.gates == b.gates and all(gate.status is GateStatus.PASS for gate in a.gates)
    assert a.disposition is b.disposition is Disposition.MATCH
    assert a.direction is b.direction is Direction.LONG
    assert a.trigger_identity == b.trigger_identity
    assert a.state.state_key == b.state.state_key
    assert a.state.revision == b.state.revision == 1
    assert a.state.transition is Transition.OPEN
    assert dict(a.features.values) == {
        "range_high": Decimal(103), "range_low": Decimal(99), "close": Decimal(104),
        "volume_multiple": Decimal(2), "market_trend": Decimal("0.01"),
        "sector_trend": Decimal("0.02"), "event_risk": False,
    }
    assert [(gate.name, gate.value, gate.threshold) for gate in a.gates] == [
        ("breakout", Decimal(104), Decimal(103)), ("volume", Decimal(2), Decimal("1.2")),
        ("market", Decimal("0.01"), Decimal(0)), ("sector", Decimal("0.02"), Decimal(0)),
        ("event", False, False), ("validity", Decimal(1797), Decimal(0)), ("geometry", True, True),
    ]
    assert a.geometry == b.geometry
    assert (a.geometry.trigger, a.geometry.stop, a.geometry.target) == (Decimal(104), Decimal(99), Decimal(114))
    assert a.geometry.valid_from == legacy.canonical_input.decision_at
    assert a.valid_until == b.valid_until == datetime(2026, 10, 23, 14, 45, tzinfo=timezone.utc)
    assert not v2.adapter.publication_permission and not v2.adapter.execution_permission
    artifact = json.loads(legacy.to_json())
    assert artifact["decision"] == json.loads(canonical_json(a))
    assert artifact["publication_permission"] is artifact["execution_permission"] is False
    assert v2.state_payload["state_key"] == a.state.state_key
    assert v2.event_payload["trigger_identity"] == a.trigger_identity
    assert v2.inputs[0].feature_snapshot_id == SNAPSHOT
    assert v2.inputs[0].input_sha256 == canonical_sha256(v2.inputs[0].retained_input)


@pytest.mark.parametrize("case", ["no_breakout", "volume", "market", "sector", "event", "expired", "short"])
def test_gate_and_direction_paths_have_adapter_parity(wire, case):
    if case == "no_breakout":
        wire["bars"][-1]["close"] = "102"
    elif case == "volume":
        wire["bars"][-1]["volume"] = "10"
    elif case in ("market", "sector"):
        wire["context"][f"{case}_trend"] = "-0.01"
    elif case == "event":
        wire["context"]["event_risk"] = True
    elif case == "expired":
        wire["decision_at"] = "2026-10-23T14:45:00Z"
    else:
        wire["bars"][-1].update(open="100", high="101", low="97", close="98")
        wire["context"].update(market_trend="-0.01", sector_trend="-0.02")
    a, b = adapters(wire)
    assert a.decision == b.decision
    if case == "short":
        assert a.decision.disposition is Disposition.MATCH
        assert a.decision.direction is Direction.SHORT
        assert (a.decision.geometry.trigger, a.decision.geometry.stop, a.decision.geometry.target) == (Decimal(98), Decimal(103), Decimal(88))
    else:
        assert a.decision.disposition is Disposition.NO_MATCH
        assert a.decision.geometry is a.decision.direction is a.decision.trigger_identity is None
        assert a.decision.reason_codes


@pytest.mark.parametrize("case,reason", [
    ("warmup", "INSUFFICIENT_COMPLETED_BARS"),
    ("late", "INPUT_NOT_YET_AVAILABLE"),
    ("late_event", "INPUT_NOT_YET_AVAILABLE"),
    ("late_context", "INPUT_NOT_YET_AVAILABLE"),
    ("market", "MISSING_MARKET_CONTEXT"),
    ("sector", "MISSING_SECTOR_CONTEXT"),
    ("event", "MISSING_EVENT_CONTEXT"),
    ("zero_volume", "ZERO_VOLUME_BASELINE"),
    ("stale", "STALE_COMPLETED_BAR"),
    ("interval", "INTERVAL_MISMATCH"),
    ("duration", "BAR_DURATION_MISMATCH"),
    ("gap", "NONCONTIGUOUS_WINDOW"),
])
def test_unavailable_is_explicit_and_has_adapter_parity(wire, case, reason):
    if case == "warmup":
        wire["bars"] = wire["bars"][-1:]
    elif case in ("late", "late_event", "late_context"):
        row = wire["bars"][-1] if case == "late" else wire["events"][0] if case == "late_event" else wire["context"]
        row["clocks"]["received_at"] = "2026-10-23T14:20:00Z"
    elif case in ("market", "sector"):
        wire["context"][f"{case}_trend"] = None
    elif case == "event":
        wire["context"]["event_risk"] = None
    elif case == "zero_volume":
        for bar in wire["bars"][:-1]:
            bar["volume"] = "0"
    elif case == "stale":
        wire.update(market_cutoff="2026-10-23T14:25:00Z", decision_at="2026-10-23T14:25:03Z")
    elif case == "interval":
        wire["bars"][-1]["interval"] = "1m"
    elif case == "duration":
        wire["bars"][-1]["clocks"]["market_start"] = "2026-10-23T14:11:00Z"
    else:
        wire["bars"][0]["clocks"]["market_start"] = "2026-10-23T13:55:00Z"
        wire["bars"][0]["clocks"]["market_end"] = "2026-10-23T14:00:00Z"
    a, b = adapters(wire)
    assert a.decision == b.decision
    assert a.decision.disposition is Disposition.UNAVAILABLE
    assert reason in a.decision.reason_codes
    assert a.decision.features.readiness is Readiness.UNAVAILABLE
    assert all(value is None for value in a.decision.features.values.values())
    assert all(gate.status is GateStatus.UNAVAILABLE for gate in a.decision.gates)
    assert a.decision.state.revision == 0
    assert b.adapter.status == "UNAVAILABLE"


@pytest.mark.parametrize("case", ["naive", "hash", "mixed_security", "adjustment", "lineage", "not_completed",
                                  "future", "order", "overlap", "duplicate_source", "negative_volume", "ohlc", "float"])
def test_invalid_input_rejected_at_both_boundaries(wire, case):
    bar = wire["bars"][-1]
    if case == "naive":
        wire["decision_at"] = "2026-10-23T14:15:03"
    elif case == "hash":
        bar["source"]["payload_sha256"] = "Z" * 64
    elif case == "mixed_security":
        bar["source"]["security_id"] = str(COHORT)
    elif case == "adjustment":
        wire["adjustment"]["basis"] = "TOTAL_RETURN"
    elif case == "lineage":
        bar["adjustment"]["action_sha256"] = "0" * 64
    elif case == "not_completed":
        bar["completed"] = False
    elif case == "future":
        wire["market_cutoff"] = "2026-10-23T14:14:00Z"
    elif case == "order":
        wire["bars"].reverse()
    elif case == "overlap":
        bar["clocks"]["market_start"] = "2026-10-23T14:09:00Z"
    elif case == "duplicate_source":
        bar["source"] = deepcopy(wire["bars"][0]["source"])
    elif case == "negative_volume":
        bar["volume"] = "-1"
    elif case == "ohlc":
        bar["high"] = "100"
    else:
        bar["close"] = 104.0
        with pytest.raises(TypeError):
            canonical_sha256(wire)
        with pytest.raises(ValueError):
            decode_input(wire)
        return
    with pytest.raises(ValueError):
        adapters(wire)
    # Explicitly exercise V2 decoding even when the legacy adapter rejects first.
    from realtime.detector_adapters import StableDetectorAdapterResult
    from realtime.detector_evidence import RealtimeDetectorInput
    from realtime.preflight import ProviderTier
    with pytest.raises(ValueError):
        run_v2(QualifiedV2Materialization(SNAPSHOT, wire, canonical_sha256(wire), True), POLICY,
               cohort_id=COHORT, provider_tier=ProviderTier.MASSIVE_ADVANCED,
               result_factory=StableDetectorAdapterResult, input_factory=RealtimeDetectorInput)


def test_state_open_retry_hold_close_expire_replace_and_error(wire):
    packet = decode_input(wire)
    opened = evaluate(packet, POLICY)
    retry = evaluate(replace(packet, prior_state=opened.state.as_prior()), POLICY)
    assert retry.state.revision == opened.state.revision == 1
    assert retry.state.transition is Transition.HOLD
    assert retry.trigger_identity == opened.trigger_identity
    assert retry.features.feature_sha256 == opened.features.feature_sha256
    later = replace(packet, prior_state=opened.state.as_prior(), decision_at=packet.decision_at + timedelta(seconds=1))
    replaced = evaluate(later, POLICY)
    assert replaced.state.revision == 2 and replaced.state.transition is Transition.REPLACE
    no_match = replace(later, context=replace(later.context, event_risk=True))
    closed = evaluate(no_match, POLICY)
    assert closed.state.revision == 2 and closed.state.transition is Transition.CLOSE
    assert closed.state.phase is Phase.IDLE
    expired = evaluate(replace(later, decision_at=opened.valid_until), POLICY)
    assert expired.state.revision == 2 and expired.state.transition is Transition.EXPIRE
    unavailable = evaluate(replace(later, context=replace(later.context, sector_trend=None)), POLICY)
    assert unavailable.state.as_prior() == opened.state.as_prior()

    def broken_features(packet, policy):
        raise ArithmeticError("deterministic reference failure")

    error = evaluate(later, POLICY, feature_calculator=broken_features)
    assert error.disposition is Disposition.ERROR
    assert error.state.as_prior() == opened.state.as_prior()
    assert error.features.readiness is Readiness.ERROR
    with pytest.raises(ValueError, match="prior state"):
        evaluate(replace(later, prior_state=replace(opened.state.as_prior(), state_key="0" * 64)), POLICY)


def test_policy_origin_style_and_adjustment_bind_identity(wire):
    packet = decode_input(wire)
    for name, value in (("origin", "DAILY_SETUP"), ("style", "SWING"), ("version", "2")):
        policy = replace(POLICY, **{name: value})
        assert policy.policy_sha256 != POLICY.policy_sha256
        with pytest.raises(ValueError, match="identity mismatch"):
            evaluate(packet, policy)
        rebound = replace(packet, model=policy.identity)
        assert state_key(rebound, policy) != state_key(packet, POLICY)
    adjusted = replace(packet.adjustment, basis=AdjustmentBasis.SPLIT_ADJUSTED)
    rebound = replace(packet, adjustment=adjusted, bars=tuple(replace(bar, adjustment=adjusted) for bar in packet.bars))
    assert state_key(rebound, POLICY) != state_key(packet, POLICY)
    assert evaluate(rebound, POLICY).disposition is Disposition.MATCH


@pytest.mark.parametrize("location", ["market_cutoff", "market_start", "market_end", "available_at", "received_at", "context", "event"])
def test_every_input_clock_rejects_naive_values(wire, location):
    if location == "market_cutoff":
        wire[location] = wire[location].removesuffix("Z")
    else:
        row = wire["context"] if location == "context" else wire["events"][0] if location == "event" else wire["bars"][-1]
        name = "received_at" if location in ("context", "event") else location
        row["clocks"][name] = row["clocks"][name].removesuffix("Z")
    with pytest.raises(ValueError, match="timezone-aware"):
        decode_input(wire)


def test_error_disposition_is_preserved_by_both_adapters(wire):
    from realtime.detector_adapters import StableDetectorAdapterResult
    from realtime.detector_evidence import RealtimeDetectorInput
    from realtime.preflight import ProviderTier

    def broken_features(packet, policy):
        raise ArithmeticError("calculation error")

    def error_evaluator(packet, policy):
        return evaluate(packet, policy, feature_calculator=broken_features)

    digest = canonical_sha256(wire)
    a = run_legacy(FrozenResearchRecord("error-case", wire, digest), POLICY, evaluator=error_evaluator)
    b = run_v2(QualifiedV2Materialization(SNAPSHOT, wire, digest, True), POLICY,
               cohort_id=COHORT, provider_tier=ProviderTier.MASSIVE_ADVANCED,
               result_factory=StableDetectorAdapterResult, input_factory=RealtimeDetectorInput,
               evaluator=error_evaluator)
    assert a.decision == b.decision
    assert a.decision.disposition is Disposition.ERROR
    assert b.adapter.status == "UNAVAILABLE"
    assert b.event_payload["disposition"] == "ERROR"
    assert not b.adapter.publication_permission and not b.adapter.execution_permission


def test_canonical_identity_and_decimal_context_are_deterministic(wire):
    packet = decode_input(wire)
    baseline = evaluate(packet, POLICY)
    assert canonical_json({"b": Decimal("1.00"), "a": [Decimal("-0"), 1]}) == '{"a":["0",1],"b":"1"}'
    offset = packet.decision_at.astimezone(timezone(timedelta(hours=-7)))
    assert replace(packet, decision_at=offset).input_sha256 == packet.input_sha256
    assert canonical_sha256(dict(reversed(list(wire.items())))) == canonical_sha256(wire)
    with localcontext() as context:
        context.prec = 3
        context.rounding = ROUND_DOWN
        assert evaluate(packet, POLICY) == baseline
    for value in (float("nan"), float("inf"), {1, 2}, {1: "bad-key"}):
        with pytest.raises((TypeError, ValueError)):
            canonical_json(value)
    with pytest.raises(ValueError):
        canonical_json(datetime(2026, 1, 1))


def test_records_and_artifacts_are_immutable_and_hash_verified(wire):
    record = FrozenResearchRecord("frozen", wire, canonical_sha256(wire))
    wire["bars"][-1]["close"] = "999"
    assert record.input_payload["bars"][-1]["close"] == "104"
    with pytest.raises(TypeError):
        record.input_payload["bars"][-1]["close"] = "1"
    packet = decode_input(record.input_payload)
    with pytest.raises(FrozenInstanceError):
        packet.decision_at = packet.market_cutoff
    features = evaluate(packet, POLICY).features
    with pytest.raises(TypeError):
        features.values["close"] = Decimal(1)
    with pytest.raises(ValueError, match="hash mismatch"):
        FrozenResearchRecord("frozen", wire, "0" * 64)
    with pytest.raises(ValueError, match="hash mismatch"):
        QualifiedV2Materialization(SNAPSHOT, wire, "0" * 64, True)
    with pytest.raises(ValueError, match="qualified"):
        QualifiedV2Materialization(SNAPSHOT, wire, canonical_sha256(wire), False)


def test_v2_coordinates_do_not_change_semantics(wire):
    first, a = adapters(wire)
    second, b = adapters(wire, cohort=UUID(int=20), snapshot=UUID(int=21))
    assert a.decision == b.decision == first.decision == second.decision
    assert a.inputs[0].feature_snapshot_id != b.inputs[0].feature_snapshot_id
    assert a.adapter.cohort_id != b.adapter.cohort_id


def test_v2_payloads_fit_existing_state_and_event_contracts_without_persistence(wire):
    from realtime.contracts import canonical_sha256 as realtime_sha256
    from realtime.detector_evidence import RealtimeDetectorState, build_detector_event, build_detector_version

    _, v2 = adapters(wire)
    decision, packet = v2.decision, v2.canonical_input
    version = build_detector_version(detector_id=POLICY.model_id, version=POLICY.version, code_sha256="0" * 64,
                                     policy_sha256=POLICY.policy_sha256, requirements={"interval": POLICY.interval}, output_kind="SIGNAL")
    state = RealtimeDetectorState(version.detector_sha256, COHORT, decision.state.state_key, decision.state.revision,
                                  "equity_model_state_v1", realtime_sha256(v2.state_payload), v2.state_payload, packet.decision_at)
    event = build_detector_event(v2.adapter, version, instrument_id=1, trigger_identity=decision.trigger_identity,
                                 trigger_at=packet.bars[-1].clocks.market_end, decision_at=packet.decision_at,
                                 valid_until=decision.valid_until, disposition=decision.disposition.value,
                                 direction=decision.direction.value, reason_codes=decision.reason_codes, readiness_epoch=1,
                                 payload_schema="equity_model_event_v1", payload=v2.event_payload, inputs=v2.inputs)
    assert state.state_key == decision.state.state_key and state.revision == decision.state.revision
    assert event.trigger_identity == decision.trigger_identity and event.valid_until == decision.valid_until
    assert event.inputs == v2.inputs and event.payload == v2.event_payload


def test_package_import_boundary_syntax_and_type_annotations():
    import equity.model_detection as package

    allowed = {"__future__", "hashlib", "json", "re", "dataclasses", "datetime", "decimal", "enum", "types", "typing", "uuid"}
    for path in Path(package.__file__).parent.glob("*.py"):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        compile(tree, str(path), "exec")
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                assert all(item.name.split(".")[0] in allowed for item in node.names)
            if isinstance(node, ast.ImportFrom) and not node.level:
                assert node.module.split(".")[0] in allowed
        module = importlib.import_module(f"equity.model_detection.{path.stem}")
        for value in vars(module).values():
            if (inspect.isfunction(value) or inspect.isclass(value)) and value.__module__ == module.__name__:
                get_type_hints(value)


def test_frozen_identity_regression(wire):
    decision = evaluate(decode_input(wire), POLICY)
    actual = {"feature": decision.features.feature_sha256, "policy": decision.model.policy_sha256,
              "trigger": decision.trigger_identity, "state_key": decision.state.state_key}
    assert actual == {
        "feature": "2a15bc982380d99d78a5e38b04717f16eddfdb2e02044e5cf67208939213eecc",
        "policy": "327ff5383844db51034cce7073f05fab42a5fec8a9a95c3f26833d9a0200dfe6",
        "trigger": "eabdd6902b2e94cc609afb8fff71001898695a373972cd7a957c33396cd42b48",
        "state_key": "491eabf361c23790aa1f5b25b8432074005602cfd919a94ba703ee6a0ba36b5d",
    }
