from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from advanced_options_reference_support import reference_state
from advanced_options_rolling_support import rolling_state
from options.domain import ContractType
from options.strategies.domain import OptionSide
from realtime.advanced_options_cycle import (
    AdvancedOptionsDetectorEvaluation,
    AdvancedOptionsNativeEvaluation,
    run_advanced_options_cycle,
)
from realtime.advanced_options_package_evidence import (
    AdvancedOptionsThresholdEvidence,
    build_cluster_observation,
    build_flow_observation,
    build_flow_package_evidence,
    build_flow_package_evidence_batch,
)
from realtime.contracts import AssetClass
from realtime.materialization import MaterializedQuote
from realtime.option_flow.records import ClusterRow, FlowEvent
from realtime.option_flow.volume import BaselineKind
from realtime.option_portal_adapter import (
    RealtimeOptionPackage,
    RealtimeOptionPackageLeg,
    RealtimePackageMarkStatus,
    materialize_realtime_option_package_mark,
)
from realtime.preflight import DetectorFamily, ProviderTier
from test_option_portal_adapter import _policy
from test_advanced_options_control_plane import _plan as _control_plane
from test_advanced_options_cycle import (
    _complete,
    _gate_and_receipt,
    _reference_repository,
)


UTC = timezone.utc
SESSION = date(2026, 10, 16)
NOW = datetime(2026, 10, 16, 14, 30, tzinfo=UTC)
COHORT = UUID("11111111-1111-1111-1111-111111111111")
GENERATION = UUID("22222222-2222-2222-2222-222222222222")
SOURCE_FLOW = UUID("33333333-3333-3333-3333-333333333333")
EVENT = UUID("44444444-4444-4444-4444-444444444444")
CLUSTER = UUID("55555555-5555-5555-5555-555555555555")
DATASET = UUID("66666666-6666-6666-6666-666666666666")
EVALUATION = UUID("77777777-7777-7777-7777-777777777777")
CANDIDATE = UUID("88888888-8888-8888-8888-888888888888")
MATRIX = UUID("99999999-9999-9999-9999-999999999999")
STREAM = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")


def _flow_event(cohort_id=COHORT):
    return FlowEvent(
        event_id=EVENT,
        event_time=NOW - timedelta(milliseconds=100),
        cohort_id=cohort_id,
        session_date=SESSION,
        provider_tier="MASSIVE_ADVANCED",
        contract_ticker="O:SPY261120C00500000",
        underlyer="SPY",
        source_seq=10,
        exchange_id=1,
        price=Decimal("5"),
        size=100,
        multiplier=100,
        premium=Decimal("50000"),
        conditions=(208,),
        condition_semantics_version="massive_options_conditions_v3",
        trade_treatment="INCLUDED",
        is_multi_leg=False,
        side_class="AT_OR_ABOVE_ASK",
        side_confidence="HIGH",
        quote_bid=Decimal("4.90"),
        quote_ask=Decimal("5.00"),
        quote_bid_size=10,
        quote_ask_size=12,
        quote_time=NOW - timedelta(milliseconds=120),
        quote_seq=9,
        cumulative_volume_before=900,
        cumulative_volume_after=1000,
        volume_baseline_kind=BaselineKind.SESSION_COMPLETE.value,
        prior_open_interest=2500,
        open_interest_as_of=date(2026, 10, 15),
        volume_exceeds_prior_oi=False,
        opening_inference="NOT_ASSESSED",
        spread_position=None,
        opening_score=None,
        opening_reasons=(),
        journal_generation=GENERATION,
        shard_id=0,
        source_offset=11,
        source_flow_id=SOURCE_FLOW,
        condition_semantics_sha256="1" * 64,
        flow_policy_sha256="2" * 64,
        lineage_version=4,
        revision=1,
        supersedes_event_id=None,
        time_ms=int((NOW - timedelta(milliseconds=100)).timestamp() * 1000),
        reasons=("PROVIDER_CONSOLIDATED_VOLUME_ELIGIBLE",),
        is_cross=False,
        is_sweep=False,
        is_floor=False,
        is_late=False,
        matched_profiles=("ACCUMULATION",),
        flow_universe_tier="ETF",
        flow_dte_lane="POSITION",
        contract_expiration=date(2026, 11, 20),
        retention_thresholds=(),
        advanced_flow_policy_sha256="3" * 64,
        contract_scope_sha256="4" * 64,
        manifest_sha256="5" * 64,
        slot_sha256="6" * 64,
        runtime_policy_sha256="7" * 64,
        provider_contract_sha256="8" * 64,
        condition_reference_sha256="9" * 64,
        exchange_reference_sha256="a" * 64,
    )


def _cluster(event):
    return ClusterRow(
        cluster_id=CLUSTER,
        revision=1,
        session_date=SESSION,
        cohort_id=event.cohort_id,
        provider_tier="MASSIVE_ADVANCED",
        contract_ticker=event.contract_ticker,
        underlyer="SPY",
        window_seconds=5,
        window_start=NOW - timedelta(seconds=5),
        window_end=NOW,
        side_class="ASK",
        print_count=1,
        total_size=100,
        total_premium=Decimal("50000"),
        distinct_exchanges=1,
        first_event_time=event.event_time,
        last_event_time=event.event_time,
        status="FINAL",
        policy_sha256="b" * 64,
        source_flow_id=SOURCE_FLOW,
        condition_semantics_sha256="1" * 64,
        lineage_version=4,
        flow_policy_sha256="2" * 64,
        profile="ACCUMULATION",
        flow_universe_tier="ETF",
        flow_dte_lane="POSITION",
        advanced_flow_policy_sha256="3" * 64,
        contract_scope_sha256="4" * 64,
        manifest_sha256="5" * 64,
        slot_sha256="6" * 64,
        runtime_policy_sha256="7" * 64,
        cumulative_contracts=1000,
        qualifies=True,
        member_event_ids=(EVENT,),
        member_event_keys=((EVENT, event.event_time),),
        provider_contract_sha256="8" * 64,
        condition_reference_sha256="9" * 64,
        exchange_reference_sha256="a" * 64,
    )


def _thresholds(*, liquidity_passed=True):
    return AdvancedOptionsThresholdEvidence(
        minimum_cumulative_volume=500,
        observed_cumulative_volume=1000,
        minimum_volume_oi_ratio=Decimal("0.30"),
        observed_volume_oi_ratio=Decimal("0.40"),
        minimum_premium=Decimal("25000"),
        observed_premium=Decimal("50000"),
        accepted_side_classes=("AT_OR_ABOVE_ASK",),
        accepted_confidences=("HIGH",),
        observed_side_class="AT_OR_ABOVE_ASK",
        observed_confidence="HIGH",
        minimum_cluster_prints=1,
        observed_cluster_prints=1,
        minimum_distinct_exchanges=1,
        observed_distinct_exchanges=1,
        minimum_cluster_premium=Decimal("25000"),
        observed_cluster_premium=Decimal("50000"),
        liquidity_policy_sha256="c" * 64,
        liquidity_evidence_sha256="d" * 64,
        liquidity_passed=liquidity_passed,
        maximum_freshness_ms=1000,
        observed_freshness_ms=80,
        completeness=BaselineKind.SESSION_COMPLETE,
        passed=liquidity_passed,
    )


def _package_and_mark(cohort_id=COHORT):
    policy = _policy()
    legs = (
        RealtimeOptionPackageLeg(
            0,
            1,
            101,
            201,
            "O:SPY261120C00500000",
            OptionSide.BUY,
            1,
            100,
            Decimal("500"),
            date(2026, 11, 20),
            ContractType.CALL,
            Decimal("5"),
            NOW,
            policy.policy_sha256,
        ),
        RealtimeOptionPackageLeg(
            1,
            2,
            102,
            202,
            "O:SPY261120P00450000",
            OptionSide.SELL,
            1,
            100,
            Decimal("450"),
            date(2026, 11, 20),
            ContractType.PUT,
            Decimal("2"),
            NOW,
            policy.policy_sha256,
        ),
    )
    package = RealtimeOptionPackage(
        candidate_id=CANDIDATE,
        event_id=EVENT,
        matrix_id=MATRIX,
        candidate_identity_sha256="e" * 64,
        cohort_id=cohort_id,
        provider_tier=ProviderTier.MASSIVE_ADVANCED,
        entry_market_time=NOW,
        entry_net_premium=Decimal("-300"),
        capital_at_risk=Decimal("300"),
        exit_deadline=NOW + timedelta(days=1),
        legs=legs,
    )
    quotes = {}
    for leg, bid, ask in (
        (legs[0], "5.10", "5.30"),
        (legs[1], "1.90", "2.10"),
    ):
        quotes[leg.instrument_id] = MaterializedQuote(
            journal_generation=GENERATION,
            stream_session_id=STREAM,
            instrument_id=leg.instrument_id,
            provider_symbol=leg.contract_ticker,
            source_session=SESSION,
            source_sequence=leg.instrument_id,
            source_timestamp=leg.instrument_id,
            source_timestamp_unit="ms",
            event_at=NOW + timedelta(milliseconds=10),
            received_at=NOW + timedelta(milliseconds=11),
            bid=Decimal(bid),
            ask=Decimal(ask),
            bid_size=10,
            ask_size=11,
            bid_exchange=1,
            ask_exchange=2,
            quality="GOOD",
            reason_codes=(),
            source_offset=leg.instrument_id,
            payload_sha256=f"{leg.instrument_id:064x}",
        )
    result = materialize_realtime_option_package_mark(
        package,
        quotes,
        checked_at=NOW + timedelta(milliseconds=20),
        policy=policy,
    )
    assert result.status is RealtimePackageMarkStatus.READY
    return package, result.mark, policy


def _inputs(cohort_id=COHORT):
    reference = reference_state()
    rolling = rolling_state(
        cohort_id=cohort_id,
        journal_generation=GENERATION,
    )
    event = _flow_event(cohort_id)
    flow = build_flow_observation(
        event,
        reference_state=reference,
        rolling_state=rolling,
    )
    cluster = build_cluster_observation(
        _cluster(event),
        events={EVENT: event},
        watermark=NOW,
        rolling_state=rolling,
    )
    return reference, rolling, event, flow, cluster


def test_flow_and_cluster_observations_bind_exact_frozen_facts():
    reference, rolling, event, flow, cluster = _inputs()

    assert flow.event_id == event.event_id
    assert flow.quote_bid == Decimal("4.90")
    assert flow.quote_ask == Decimal("5.00")
    assert flow.cumulative_volume_after == 1000
    assert flow.reference_state_id == reference.state_id
    assert flow.rolling_state_id == rolling.state_id
    assert cluster.event_ids == (EVENT,)
    assert cluster.source_offsets == (11,)
    assert cluster.exchange_ids == (1,)
    assert cluster.hop_milliseconds == 2500


def test_selected_package_retains_exact_leg_and_model_evidence():
    reference, rolling, _, flow, cluster = _inputs()
    package, mark, policy = _package_and_mark()
    evaluation = AdvancedOptionsDetectorEvaluation(
        EVALUATION,
        DATASET,
        EVENT,
        DetectorFamily.O1,
        "SPY",
        "SELECTED",
        NOW,
        "f" * 64,
        {"native": "O1"},
        package,
    )

    evidence = build_flow_package_evidence(
        evaluation,
        flow_observations=(flow,),
        cluster_observations=(cluster,),
        threshold_evidence=_thresholds(),
        reference_state=reference,
        rolling_state=rolling,
        package_mark=mark,
        model_version="black-scholes-merton-v1",
        model_bundle_sha256="1" * 64,
        valuation_policy_sha256=policy.policy_sha256,
        degraded_reason_codes=("IV_SURFACE_UNAVAILABLE",),
    )

    assert len(evidence.package_leg_evidence) == 2
    assert tuple(row.catalog_version_id for row in evidence.package_leg_evidence) == (
        201,
        202,
    )
    assert all(row.rate_observation_ids for row in evidence.package_leg_evidence)
    assert all(
        row.iv_reason_codes == ("O2_MODEL_NOT_ACCEPTED",)
        for row in evidence.package_leg_evidence
    )
    assert evidence.equity_spot_sha256 == rolling.underlying_ticks[0].sha256
    assert not any((
        evidence.publication_authorized,
        evidence.delivery_authorized,
        evidence.paper_fill_authorized,
        evidence.execution_authorized,
    ))


def test_observation_without_ordered_legs_remains_an_observation():
    reference, rolling, _, flow, cluster = _inputs()
    evaluation = AdvancedOptionsDetectorEvaluation(
        EVALUATION,
        DATASET,
        EVENT,
        DetectorFamily.S2,
        "SPY",
        "OBSERVATION",
        NOW,
        "f" * 64,
        {"native": "S2"},
        None,
    )

    evidence = build_flow_package_evidence(
        evaluation,
        flow_observations=(flow,),
        cluster_observations=(cluster,),
        threshold_evidence=_thresholds(liquidity_passed=False),
        reference_state=reference,
        rolling_state=rolling,
        degraded_reason_codes=("LIQUIDITY_THRESHOLD_NOT_MET",),
    )

    assert evidence.selection_status == "OBSERVATION"
    assert evidence.package is None
    assert evidence.package_mark is None
    assert evidence.package_leg_evidence == ()


def test_selected_evidence_rejects_missing_mark_or_threshold_drift():
    reference, rolling, _, flow, cluster = _inputs()
    package, _, _ = _package_and_mark()
    evaluation = AdvancedOptionsDetectorEvaluation(
        EVALUATION,
        DATASET,
        EVENT,
        DetectorFamily.O1,
        "SPY",
        "SELECTED",
        NOW,
        "f" * 64,
        {"native": "O1"},
        package,
    )

    with pytest.raises(ValueError, match="selected package evidence"):
        build_flow_package_evidence(
            evaluation,
            flow_observations=(flow,),
            cluster_observations=(cluster,),
            threshold_evidence=_thresholds(),
            reference_state=reference,
            rolling_state=rolling,
            package_mark=None,
            model_version="black-scholes-merton-v1",
            model_bundle_sha256="1" * 64,
            valuation_policy_sha256="2" * 64,
        )
    with pytest.raises(ValueError, match="threshold verdict"):
        replace(_thresholds(), passed=False)


def test_evidence_batch_covers_every_cycle_evaluation_exactly():
    control = _control_plane()
    gate, receipt = _gate_and_receipt()
    package, mark, policy = _package_and_mark(control.cohort_id)
    native = AdvancedOptionsNativeEvaluation(
        EVENT,
        "SPY",
        "SELECTED",
        NOW,
        {"native": "O1"},
        package,
    )
    cycle = run_advanced_options_cycle(
        control_plane=control,
        input_gate=gate,
        input_receipt=receipt,
        executors={
            DetectorFamily.O1: lambda _context: _complete(native),
        },
        cutoff=NOW,
        reference_generation=7,
        reference_repository=_reference_repository(),
        build_id="p6d5-build",
    )
    (evaluation,) = cycle.evaluations
    reference, rolling, _, flow, cluster = _inputs(control.cohort_id)
    evidence = build_flow_package_evidence(
        evaluation,
        flow_observations=(flow,),
        cluster_observations=(cluster,),
        threshold_evidence=_thresholds(),
        reference_state=reference,
        rolling_state=rolling,
        package_mark=mark,
        model_version="black-scholes-merton-v1",
        model_bundle_sha256="1" * 64,
        valuation_policy_sha256=policy.policy_sha256,
        degraded_reason_codes=("IV_SURFACE_UNAVAILABLE",),
    )

    batch = build_flow_package_evidence_batch(cycle, (evidence,))

    assert batch.dataset_id == cycle.dataset_id
    assert batch.cycle_sha256 == cycle.cycle_sha256
    assert batch.evidences == (evidence,)
    with pytest.raises(ValueError, match="exact evaluations"):
        build_flow_package_evidence_batch(cycle, ())
