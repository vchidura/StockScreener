from datetime import datetime, timedelta
from uuid import uuid4

import pytest

from equity.behavior_producer import produce_stock_behavior_snapshots
from equity.domain import DecisionWatermark
from options.stock_behavior_gates import (
    STOCK_BEHAVIOR_GATE_POLICY,
    STOCK_BEHAVIOR_GATE_POLICIES,
    evaluate_option_stock_behavior,
    resolve_stock_behavior_gate_policy,
)
from options.strategies.domain import StructureType
from test_equity_behavior_coverage import NOW
from test_equity_behavior_producer import (
    ActionRepository,
    BarRepository,
    EvidenceRepository,
    inputs,
)


def current_aapl_snapshot():
    securities, raw_reads, adjusted_reads, coverage = inputs()
    repository = EvidenceRepository(raw_reads, NOW + timedelta(seconds=2))
    result = produce_stock_behavior_snapshots(
        securities,
        watermark=DecisionWatermark(NOW, NOW),
        evidence_repository=repository,
        bar_repository=BarRepository(adjusted_reads),
        corporate_action_repository=ActionRepository(coverage),
        received_at=NOW + timedelta(seconds=1),
        clock=lambda: NOW + timedelta(seconds=2),
    )
    assert result.inserted == 2
    return next(row for row in repository.snapshots.values() if row.ticker == "AAPL")


def assess(snapshot, structure, thesis, strategy="DIRECTIONAL_LONG_PREMIUM", **kwargs):
    return evaluate_option_stock_behavior(
        snapshot,
        candidate_id=uuid4(), matrix_id=uuid4(), underlyer="AAPL",
        strategy_name=strategy, structure_type=structure,
        directional_thesis=thesis, decision_at=NOW + timedelta(seconds=2),
        **kwargs,
    )


def test_directional_call_profile_passes_structural_stock_gates_without_empirical_thresholds():
    result = assess(current_aapl_snapshot(), StructureType.LONG_CALL, "BULLISH")

    assert result.disposition == "ELIGIBLE_RESEARCH"
    assert result.detector_policy_sha256 == STOCK_BEHAVIOR_GATE_POLICY.sha256
    assert result.execution_permission is False and result.assessment_only is True
    required = [gate for gate in result.gates if gate.requirement == "REQUIRED"]
    assert required and all(gate.verdict == "PASS" for gate in required)
    slopes = [gate for gate in required if gate.metric_id == "ema50_slope10_atr"]
    assert len(slopes) == 3 and all(gate.comparator == "GT_ZERO" for gate in slopes)
    assert all(gate.threshold_float == 0 for gate in slopes)
    assert all(gate.source_evidence_ids for gate in slopes)


def test_same_snapshot_blocks_bearish_structure_on_directional_slopes():
    result = assess(current_aapl_snapshot(), StructureType.LONG_PUT, "BEARISH")

    assert result.disposition == "BLOCKED"
    assert sum(gate.verdict == "FAIL" for gate in result.gates) >= 3
    assert all(
        gate.comparator == "LT_ZERO"
        for gate in result.gates if gate.metric_id == "ema50_slope10_atr"
    )


def test_missing_snapshot_is_unavailable_but_non_directional_strategy_is_not_applicable():
    unavailable = assess(None, StructureType.LONG_CALL, "BULLISH")
    assert unavailable.disposition == "UNAVAILABLE"
    assert unavailable.stock_snapshot_id is None

    not_applicable = assess(
        None, StructureType.CASH_SECURED_PUT, None, strategy="INCOME_WHEEL",
    )
    assert not_applicable.disposition == "NOT_APPLICABLE"
    assert not_applicable.applicable is False


def test_thesis_mismatch_blocks_without_mutating_snapshot():
    snapshot = current_aapl_snapshot()
    before = snapshot.canonical_json()
    result = assess(snapshot, StructureType.LONG_CALL, "BEARISH")

    assert result.disposition == "BLOCKED"
    thesis = next(gate for gate in result.gates if gate.gate_id == "DIRECTIONAL_THESIS_STRUCTURE")
    assert thesis.verdict == "FAIL"
    assert snapshot.canonical_json() == before


def test_policy_dispatch_preserves_v1_and_rejects_same_version_edits():
    policy = STOCK_BEHAVIOR_GATE_POLICY
    assert policy.sha256 == "f3fddf6aadb5dc9688fbf3dcf4250113a64fb48b0a3b2d35b55400232fe7a8dd"
    assert resolve_stock_behavior_gate_policy(policy.version, policy.sha256) is policy
    with pytest.raises(ValueError, match="version/hash"):
        resolve_stock_behavior_gate_policy("unknown", policy.sha256)
    with pytest.raises(TypeError):
        STOCK_BEHAVIOR_GATE_POLICIES[(policy.version, "0" * 64)] = policy
    altered = policy.model_copy(update={"required_trend_intervals": ("1d",)})
    with pytest.raises(ValueError, match="version/hash"):
        assess(None, StructureType.LONG_CALL, "BULLISH", policy=altered)


@pytest.mark.parametrize(("mutation", "expected"), [
    (None, "ELIGIBLE_RESEARCH"), ("missing", "UNAVAILABLE"),
    ("late", "UNAVAILABLE"), ("source_policy", "UNAVAILABLE"),
    ("security", "UNAVAILABLE"), ("direction", "BLOCKED"),
    ("room", "BLOCKED"), ("chase", "BLOCKED"),
    ("partial_profile", "ELIGIBLE_RESEARCH"),
])
@pytest.mark.parametrize("direction", [1, -1])
@pytest.mark.parametrize("source_kind", ["lifecycle", "publication", "direct"])
def test_hourly_acceptance_challenger_is_separate_causal_and_non_executing(mutation, expected, direction, source_kind):
    from equity.behavior import StockBehaviorSnapshot
    from equity.behavior_setup import bind_stock_setup
    from options.stock_setup_gates import HourlyAcceptancePolicy, evaluate_hourly_acceptance
    from test_equity_behavior_setup import setup_inputs
    from test_equity_behavior import NOW as BEHAVIOR_NOW, snapshot_payload

    changes = {"target": 100.1} if mutation == "room" else {"reference": 97.} if mutation == "chase" else {}
    if direction == -1:
        changes = {"reference": 101., "stop": 102., "target": 96., **{key: 200 - value for key, value in changes.items()}}
    changes["direction"] = direction
    lifecycle, source = setup_inputs(**changes)
    setup = bind_stock_setup(lifecycle, source)
    trusted_source_policy = None
    if source_kind in ("publication", "direct"):
        from dataclasses import replace
        from uuid import UUID
        from equity.behavior_setup import bind_published_stock_setup
        from research.stock_idea_engine import candidate_record, digest
        from test_equity_behavior_setup import direct_policy, publication_inputs

        stock, instance, publication, trusted_source_policy = publication_inputs()
        stock = replace(stock, **changes)
        publication["candidates"] = {stock.episode_id: candidate_record(stock)}
        publication["dispositions"][0].update(episode_id=stock.episode_id, direction=stock.direction)
        if source_kind == "direct":
            from equity.behavior_setup import bind_direct_stock_setup
            trusted_source_policy = direct_policy(trusted_source_policy)
            setup = bind_direct_stock_setup(instance=instance, publication=publication, record_id=publication["window_key"],
                payload_sha256=digest(publication), received_at=source.received_at,
                episode_id=stock.episode_id, security_id=UUID(stock.security_id), ticker=stock.ticker, policy=trusted_source_policy)
        else:
            setup = bind_published_stock_setup(instance=instance, publication=publication, record_id=publication["window_key"],
                payload_sha256=digest(publication), imported_at=source.received_at - timedelta(seconds=1),
                received_at=source.received_at, episode_id=stock.episode_id, security_id=UUID(stock.security_id),
                ticker=stock.ticker, policy=trusted_source_policy)
        source = setup.source
    payload = snapshot_payload()
    offset = source.received_at - BEHAVIOR_NOW
    for field in ("market_time", "computed_at", "available_at", "valid_until"):
        payload[field] = datetime.fromisoformat(payload[field]) + offset
    payload["security_id"] = str(source.security_id)
    payload["ticker"] = "AAPL"
    for component in payload["components"]:
        component["metrics"][0]["value"] *= direction
        if mutation == "partial_profile" and component["interval"] != "1d":
            component.update(status="UNAVAILABLE", state=None, metrics=[], reason_codes=["MISSING_INPUT"])
        for origin in component["sources"]:
            origin["security_id"] = str(source.security_id)
            for field in ("market_time", "observed_at", "recorded_at", "received_at", "valid_until"):
                origin[field] = datetime.fromisoformat(origin[field]) + offset
    from equity.behavior import BehaviorMetric, BehaviorComponent, METRICS
    component_source = payload["components"][0]["sources"][0]
    payload["components"].append(BehaviorComponent(
        factor="PARTICIPATION", interval="1d", status="READY", sources=(component_source,),
        metrics=(BehaviorMetric(definition=next(row for row in METRICS if row.metric_id == "median_dollar_volume20"),
                                interval="1d", status="READY", value=30000000., sample_count=21),),
    ).model_dump(mode="json"))
    snapshot = StockBehaviorSnapshot.model_validate(payload)
    from options.stock_setup_gates import DirectHourlyAcceptancePolicy, PublishedHourlyAcceptancePolicy
    policy_type = {"publication": PublishedHourlyAcceptancePolicy, "lifecycle": HourlyAcceptancePolicy,
                   "direct": DirectHourlyAcceptancePolicy}[source_kind]
    policy = policy_type(setup_source_policy_sha256="b" * 64 if mutation == "source_policy" else source.policy_sha256)
    result = evaluate_hourly_acceptance(
        snapshot, None if mutation == "missing" else setup, policy=policy,
        candidate_id=uuid4(), candidate_identity_sha256="c" * 64, matrix_id=uuid4(),
        security_id=uuid4() if mutation == "security" else source.security_id,
        underlyer="AAPL", strategy_name="DIRECTIONAL_LONG_PREMIUM",
        structure_type=StructureType.LONG_CALL if direction == 1 else StructureType.LONG_PUT,
        directional_thesis=("BULLISH" if direction == 1 else "BEARISH") if mutation != "direction" else "UNKNOWN",
        market_cutoff=source.received_at,
        decision_at=source.valid_until if mutation == "late" else source.received_at,
        trusted_source_policy=trusted_source_policy,
    )
    assert result.disposition == expected
    assert result.detector_policy_sha256 != STOCK_BEHAVIOR_GATE_POLICY.sha256
    assert result.execution_permission is False and result.publication_permission is False
    assert result.schema_version != "option_stock_behavior_assessment_v1"
    if source_kind in ("publication", "direct"):
        untrusted = evaluate_hourly_acceptance(snapshot, setup, policy=policy,
            candidate_id=uuid4(), candidate_identity_sha256="c" * 64, matrix_id=uuid4(),
            security_id=source.security_id, underlyer="AAPL", strategy_name="DIRECTIONAL_LONG_PREMIUM",
            structure_type=StructureType.LONG_CALL, directional_thesis="BULLISH",
            market_cutoff=source.received_at, decision_at=source.received_at)
        assert untrusted.disposition == "UNAVAILABLE"
        assert any(row.reason == "TRUSTED_SETUP_SOURCE_POLICY_REQUIRED" for row in untrusted.checks)
    if source_kind == "direct":
        wrong_source = evaluate_hourly_acceptance(snapshot, setup,
            policy=PublishedHourlyAcceptancePolicy(setup_source_policy_sha256=source.policy_sha256),
            candidate_id=uuid4(), candidate_identity_sha256="c" * 64, matrix_id=uuid4(),
            security_id=source.security_id, underlyer="AAPL", strategy_name="DIRECTIONAL_LONG_PREMIUM",
            structure_type=StructureType.LONG_CALL, directional_thesis="BULLISH", trusted_source_policy=trusted_source_policy,
            market_cutoff=source.received_at, decision_at=source.received_at)
        assert wrong_source.disposition == "UNAVAILABLE"


def test_setup_policy_requires_frozen_hash_and_rejects_threshold_changes():
    from options.stock_setup_gates import HourlyAcceptancePolicy, load_setup_policy

    policy = HourlyAcceptancePolicy(setup_source_policy_sha256="a" * 64)
    assert load_setup_policy(policy.model_dump(mode="json"), policy.sha256) == policy
    with pytest.raises(ValueError):
        load_setup_policy(policy.model_dump(mode="json"), "0" * 64)
    with pytest.raises(ValueError):
        HourlyAcceptancePolicy(setup_source_policy_sha256="a" * 64, maximum_chase_activation_atr=2.)
    with pytest.raises(ValueError):
        HourlyAcceptancePolicy(setup_source_policy_sha256="a" * 64, targets=())