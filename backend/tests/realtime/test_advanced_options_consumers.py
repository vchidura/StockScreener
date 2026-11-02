from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import UUID

import pytest
import realtime.advanced_options_consumers as consumers
import realtime.stage_runners_options as option_runners

from option_flow_runner_support import (
    advanced_scope,
    harness,
    pinned_semantics,
    scenario_feed,
    settings,
)
from realtime.advanced_options_provider_contract import (
    MASSIVE_OPTIONS_PROVIDER_CONTRACT,
    load_default_options_exchange_reference,
)
from option_flow_support import advanced_policy, policy
from test_advanced_options_ingress import _write_options_journal
from test_advanced_options_activity_momentum_c6 import (
    SESSION_DATE,
    _candidate,
    _group,
)
from realtime.advanced_options_activity_momentum_c6 import (
    allocate_activity_momentum,
    load_activity_momentum_profile,
)
from realtime.advanced_options_consumers import (
    AdvancedOptionsBoundedDetectorRouter,
    AdvancedOptionsMarketStateConsumer,
    advanced_options_detector_routing_state_from_payload,
    advanced_options_detector_routing_state_payload,
    persist_advanced_options_market_state_slice_cursor,
)
from realtime.advanced_options_ingress import (
    AdvancedOptionsJournalSource,
    build_advanced_options_journal_registration,
)
from realtime.materialization import (
    MaterializerShardMap,
    restore_materializer_checkpoint,
)
from realtime.contracts import AssetClass, MarketChannel
from realtime.journal import NormalizedJournalRecord
from realtime.normalizer import NORMALIZER_VERSION
from realtime.option_flow.engine import EngineConfig
from realtime.option_flow.volume import VolumeConfig
from realtime.option_source_flow import build_option_source_flow_identity
from realtime.stage_runners_options import (
    OPTIONS_FLOW_SOURCE_IDENTITY_UNPINNED,
    options_flow_block_reason,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 16, 14, 30, tzinfo=UTC)
OWNER = UUID("55555555-5555-5555-5555-555555555555")
FLOW_COHORT = UUID("66666666-6666-6666-6666-666666666666")
FLOW_STREAM = UUID("77777777-7777-7777-7777-777777777777")


def _source(tmp_path):
    control, stream, path, progress = _write_options_journal(tmp_path)
    registration = build_advanced_options_journal_registration(
        control_plane=control,
        root=path.parent,
        path=path,
        stream_session_id=stream,
        owner_epoch=1,
        normalizer_version=NORMALIZER_VERSION,
        shard_ids=(0,),
        shard_map_sha256=_shard_map().sha256,
    )
    registration = replace(
        registration,
        status="DRAINING",
        raw_committed_offset=progress.last_raw_offset,
        normalized_committed_offset=progress.last_event_offset,
    )
    return control, AdvancedOptionsJournalSource(
        root=path.parent,
        registration=registration,
        source_flow=control.source_flow,
    )


def _shard_map():
    return MaterializerShardMap(
        "advanced-options-market-state-v1",
        1,
        ((101, 0),),
    )


def _normalized_record(
    offset,
    ticker,
    event_kind,
    event_at,
    *,
    size=None,
):
    return NormalizedJournalRecord(
        event_offset=offset,
        raw_offset=offset,
        event_index=0,
        normalizer_version=NORMALIZER_VERSION,
        journal_generation=FLOW_STREAM,
        stream_session_id=FLOW_STREAM,
        instrument_id=offset,
        provider_symbol=ticker,
        underlying="SPY",
        asset_class=AssetClass.OPTION,
        shard_id=0,
        event_kind=event_kind,
        source_session=SESSION_DATE,
        source_sequence=offset,
        source_timestamp=int(event_at.timestamp() * 1_000_000_000),
        source_timestamp_unit="ns",
        event_at=event_at,
        price=Decimal("1.05"),
        size=size,
        bid=Decimal("1.00"),
        ask=Decimal("1.10"),
        bid_size=10,
        ask_size=10,
        exchange=11,
        conditions=(),
        quality="ACCEPTED",
        reason_codes=(),
        details_json='{"fixture":true}',
        payload_sha256=f"{offset:064x}",
        received_at=event_at,
        committed_at=event_at,
    )


def _routing_allocation():
    profile = load_activity_momentum_profile()
    first = _candidate("SPY", 0, dte=7)
    second = _candidate("SPY", 1, dte=30)
    third = _candidate("SPY", 2, dte=30)
    directional = _group(
        "SPY",
        0,
        candidates=(first,),
    )
    package = _group(
        "SPY",
        1,
        dte=30,
        role="STRATEGY_REQUIRED",
        candidates=(second, third),
        matching_strategies=("DIRECTIONAL_DEBIT_SPREAD",),
    )
    allocation = allocate_activity_momentum(
        profile,
        session_date=SESSION_DATE,
        rotation_key="router-replay",
        ready_underlyings=("SPY",),
        groups=(directional, package),
    )
    return allocation, first, second, third


def test_bounded_detector_router_restart_matches_unoptimized_replay():
    allocation, first, second, third = _routing_allocation()
    records = (
        _normalized_record(
            1,
            second.contract_ticker,
            MarketChannel.QUOTE,
            NOW,
        ),
        _normalized_record(
            2,
            third.contract_ticker,
            MarketChannel.QUOTE,
            NOW + timedelta(milliseconds=50),
        ),
        _normalized_record(
            3,
            second.contract_ticker,
            MarketChannel.QUOTE,
            NOW + timedelta(milliseconds=100),
        ),
        _normalized_record(
            4,
            second.contract_ticker,
            MarketChannel.TRADE,
            NOW + timedelta(milliseconds=200),
            size=Decimal("7"),
        ),
        _normalized_record(
            5,
            first.contract_ticker,
            MarketChannel.QUOTE,
            NOW + timedelta(milliseconds=300),
        ),
        _normalized_record(
            6,
            first.contract_ticker,
            MarketChannel.TRADE,
            NOW + timedelta(milliseconds=600),
            size=Decimal("5"),
        ),
    )

    uninterrupted = AdvancedOptionsBoundedDetectorRouter(
        allocation=allocation
    )
    assert set(allocation.selected_contract_tickers) == {
        first.contract_ticker,
        second.contract_ticker,
        third.contract_ticker,
    }
    assert tuple(row.event_offset for row in records) == (
        1,
        2,
        3,
        4,
        5,
        6,
    )
    uninterrupted_result = uninterrupted.process(records)

    first_run = AdvancedOptionsBoundedDetectorRouter(
        allocation=allocation
    )
    first_run.process(records[:3])
    restarted = AdvancedOptionsBoundedDetectorRouter(
        allocation=allocation,
        state=advanced_options_detector_routing_state_from_payload(
            advanced_options_detector_routing_state_payload(
                first_run.state
            )
        ),
    )
    restarted_result = restarted.process(records[3:])

    assert restarted_result.state == uninterrupted_result.state
    assert uninterrupted_result.end_offset == 6
    assert dict(uninterrupted_result.state.factual_volume) == {
        first.contract_ticker: Decimal("5"),
        second.contract_ticker: Decimal("7"),
    }
    assert uninterrupted_result.state.irrelevant_event_count == 1
    assert uninterrupted_result.state.cadence_skip_count >= 1
    assert uninterrupted_result.state.readiness_skip_count == 1
    assert uninterrupted_result.quote_cache_size == 3
    assert uninterrupted_result.factual_volume_cache_size == 2
    assert uninterrupted_result.state.scanned_event_count == 6
    assert uninterrupted_result.state.processed_detail_bytes == (
        6 * len('{"fixture":true}'.encode("utf-8"))
    )
    assert uninterrupted_result.state.first_event_ns == int(
        NOW.timestamp() * 1_000_000_000
    )
    assert uninterrupted_result.state.last_event_ns == int(
        (NOW + timedelta(milliseconds=600)).timestamp()
        * 1_000_000_000
    )
    assert uninterrupted_result.route_entry_count <= (
        len(allocation.selected_contract_tickers)
        * (
            len(tuple(
                row
                for row in allocation.consumer_eligibility
                if row.enabled
            ))
            + len(allocation.selected_groups)
        )
        * 2
    )
    counts = dict(
        uninterrupted_result.state.detector_invocation_counts
    )
    assert counts["directional-momentum-v2"] == 2
    assert counts["strategy-package-model-v2"] == 1
    assert "future-50ms-detector" not in counts
    assert "silent-accumulation" not in counts


def test_detector_output_cap_does_not_stop_volume_or_checkpoint():
    allocation, first, second, third = _routing_allocation()
    records = (
        _normalized_record(
            1,
            first.contract_ticker,
            MarketChannel.QUOTE,
            NOW,
        ),
        _normalized_record(
            2,
            second.contract_ticker,
            MarketChannel.QUOTE,
            NOW + timedelta(seconds=1),
        ),
        _normalized_record(
            3,
            first.contract_ticker,
            MarketChannel.QUOTE,
            NOW + timedelta(seconds=2),
        ),
        _normalized_record(
            4,
            first.contract_ticker,
            MarketChannel.TRADE,
            NOW + timedelta(seconds=3),
            size=Decimal("9"),
        ),
    )
    router = AdvancedOptionsBoundedDetectorRouter(
        allocation=allocation
    )

    result = router.process(
        records,
        maximum_output_invocations=0,
    )

    assert result.invocations == ()
    assert result.end_offset == 4
    assert result.state.scanned_event_count == 4
    assert dict(result.state.factual_volume)[
        first.contract_ticker
    ] == Decimal("9")
    assert result.state.suppressed_output_count > 0
    assert sum(
        count
        for _, count in result.state.detector_invocation_counts
    ) > 0


def test_irrelevant_boundary_events_define_replay_rate_window():
    allocation, first, second, third = _routing_allocation()
    records = (
        _normalized_record(
            1,
            second.contract_ticker,
            MarketChannel.TRADE,
            NOW,
            size=Decimal("1"),
        ),
        _normalized_record(
            2,
            first.contract_ticker,
            MarketChannel.QUOTE,
            NOW + timedelta(seconds=1),
        ),
        _normalized_record(
            3,
            first.contract_ticker,
            MarketChannel.TRADE,
            NOW + timedelta(seconds=2),
            size=Decimal("2"),
        ),
        _normalized_record(
            4,
            third.contract_ticker,
            MarketChannel.TRADE,
            NOW + timedelta(seconds=3),
            size=Decimal("3"),
        ),
    )
    uninterrupted = AdvancedOptionsBoundedDetectorRouter(
        allocation=allocation
    )
    uninterrupted.process(records)

    first_run = AdvancedOptionsBoundedDetectorRouter(
        allocation=allocation
    )
    first_run.process(records[:2])
    restarted = AdvancedOptionsBoundedDetectorRouter(
        allocation=allocation,
        state=advanced_options_detector_routing_state_from_payload(
            advanced_options_detector_routing_state_payload(
                first_run.state
            )
        ),
    )
    restarted.process(records[2:])

    assert restarted.state == uninterrupted.state
    assert uninterrupted.state.scanned_event_count == 4
    assert uninterrupted.state.processed_detail_bytes == (
        4 * len('{"fixture":true}'.encode("utf-8"))
    )
    assert uninterrupted.state.irrelevant_event_count == 2
    assert uninterrupted.state.first_event_ns == int(
        NOW.timestamp() * 1_000_000_000
    )
    assert uninterrupted.state.last_event_ns == int(
        (NOW + timedelta(seconds=3)).timestamp()
        * 1_000_000_000
    )
    assert dict(
        uninterrupted.state.detector_invocation_counts
    ) == {"directional-momentum-v2": 2}


def test_all_unrouted_admitted_slice_retains_rate_window_on_restart():
    allocation, _first, second, third = _routing_allocation()
    records = (
        _normalized_record(
            1,
            second.contract_ticker,
            MarketChannel.TRADE,
            NOW,
            size=Decimal("4"),
        ),
        _normalized_record(
            2,
            third.contract_ticker,
            MarketChannel.TRADE,
            NOW + timedelta(seconds=2),
            size=Decimal("6"),
        ),
    )
    uninterrupted = AdvancedOptionsBoundedDetectorRouter(
        allocation=allocation
    )
    uninterrupted.process(records)

    first_run = AdvancedOptionsBoundedDetectorRouter(
        allocation=allocation
    )
    first_run.process(records[:1])
    restarted = AdvancedOptionsBoundedDetectorRouter(
        allocation=allocation,
        state=advanced_options_detector_routing_state_from_payload(
            advanced_options_detector_routing_state_payload(
                first_run.state
            )
        ),
    )
    restarted.process(records[1:])

    assert restarted.state == uninterrupted.state
    assert uninterrupted.state.scanned_event_count == 2
    assert uninterrupted.state.processed_detail_bytes == (
        2 * len('{"fixture":true}'.encode("utf-8"))
    )
    assert uninterrupted.state.irrelevant_event_count == 2
    assert uninterrupted.state.first_event_ns == int(
        NOW.timestamp() * 1_000_000_000
    )
    assert uninterrupted.state.last_event_ns == int(
        (NOW + timedelta(seconds=2)).timestamp()
        * 1_000_000_000
    )
    assert uninterrupted.state.detector_invocation_counts == ()


def test_market_state_restart_and_uninterrupted_replay_are_identical(
    tmp_path,
):
    control, source = _source(tmp_path)
    shard_map = _shard_map()
    uninterrupted = AdvancedOptionsMarketStateConsumer(
        source=source,
        cohort_id=control.cohort_id,
        shard_map=shard_map,
        owner_epoch=1,
        clock=lambda: NOW,
    )
    uninterrupted_slice = uninterrupted.plan_slice(maximum_records=10)
    assert uninterrupted.committed_offset == 0
    uninterrupted.commit_slice(uninterrupted_slice)

    first = AdvancedOptionsMarketStateConsumer(
        source=source,
        cohort_id=control.cohort_id,
        shard_map=shard_map,
        owner_epoch=1,
        clock=lambda: NOW,
    )
    first_slice = first.plan_slice(maximum_records=1)
    first.commit_slice(first_slice)
    restored = tuple(
        restore_materializer_checkpoint(checkpoint, shard_map)
        for checkpoint in first_slice.checkpoints
    )
    restarted = AdvancedOptionsMarketStateConsumer(
        source=source,
        cohort_id=control.cohort_id,
        shard_map=shard_map,
        owner_epoch=1,
        states=restored,
        clock=lambda: NOW,
    )
    second_slice = restarted.plan_slice(maximum_records=10)
    restarted.commit_slice(second_slice)

    assert restarted.states == uninterrupted.states
    assert second_slice.state_sha256 == uninterrupted_slice.state_sha256
    assert restarted.committed_offset == source.last_event_offset() == 2
    state = restarted.states[0]
    assert len(state.quotes) == 1
    assert len(state.trades) == 1
    assert state.quotes[0].provider_symbol == state.trades[0].provider_symbol


def test_market_state_logical_shards_advance_independently(
    tmp_path,
    monkeypatch,
):
    control, source = _source(tmp_path)
    shard_map = MaterializerShardMap(
        "advanced-options-market-state-isolated-v1",
        2,
        ((101, 0), (202, 1)),
    )
    source.registration = replace(
        source.registration,
        shard_ids=(0, 1),
        shard_map_sha256=shard_map.sha256,
    )
    first = AdvancedOptionsMarketStateConsumer(
        source=source,
        cohort_id=control.cohort_id,
        shard_map=shard_map,
        owner_epoch=1,
        owned_shard_ids=(0,),
        clock=lambda: NOW,
    )
    second = AdvancedOptionsMarketStateConsumer(
        source=source,
        cohort_id=control.cohort_id,
        shard_map=shard_map,
        owner_epoch=2,
        owned_shard_ids=(1,),
        clock=lambda: NOW,
    )

    first_slice = first.plan_slice(maximum_records=10)
    second_slice = second.plan_slice(maximum_records=10)
    first.commit_slice(first_slice)

    assert first.committed_offset == source.last_event_offset() == 2
    assert second.committed_offset == 0
    assert first_slice.states[0].shard_id == 0
    assert second_slice.states[0].shard_id == 1
    assert not second_slice.dirty_instrument_ids[0]

    persisted = []
    monkeypatch.setattr(
        consumers,
        "persist_quote_materialization_cursor",
        lambda *args, **kwargs: persisted.append(kwargs),
    )
    persist_advanced_options_market_state_slice_cursor(
        MagicMock(),
        cohort_id=control.cohort_id,
        journal_generation=source.generation,
        planned=first_slice,
        owner_id=OWNER,
        batch_executor=_batch_executor,
    )
    second.commit_slice(second_slice)

    assert len(persisted) == 1
    assert persisted[0]["checkpoint"].shard_id == 0
    assert second.committed_offset == 2


def _batch_executor(cursor, statement, parameters, *, page_size):
    assert page_size == 100
    for row in parameters:
        cursor.execute(statement, row)


def test_market_state_persistence_delegates_to_canonical_current_owner(
    tmp_path,
):
    control, source = _source(tmp_path)
    consumer = AdvancedOptionsMarketStateConsumer(
        source=source,
        cohort_id=control.cohort_id,
        shard_map=_shard_map(),
        owner_epoch=3,
        clock=lambda: NOW,
    )
    planned = consumer.plan_slice(maximum_records=10)
    cursor = MagicMock()
    cursor.fetchone.side_effect = (
        (str(OWNER), 3, True),
        None,
        (str(planned.checkpoints[0].checkpoint_id),),
    )

    persist_advanced_options_market_state_slice_cursor(
        cursor,
        cohort_id=control.cohort_id,
        journal_generation=source.generation,
        planned=planned,
        owner_id=OWNER,
        batch_executor=_batch_executor,
        lease_resource_key="market-state:shared-shard-map",
    )

    statements = tuple(call.args[0] for call in cursor.execute.call_args_list)
    assert any(
        call.args[1][0] == "market-state:shared-shard-map"
        for call in cursor.execute.call_args_list
        if len(call.args) > 1
        and isinstance(call.args[1], tuple)
        and call.args[1]
    )
    quote_index = next(
        index for index, value in enumerate(statements)
        if "INSERT INTO rt.quote_current" in value
    )
    trade_index = next(
        index for index, value in enumerate(statements)
        if "INSERT INTO rt.trade_current" in value
    )
    checkpoint_index = next(
        index for index, value in enumerate(statements)
        if "INSERT INTO rt.consumer_checkpoints" in value
    )
    assert quote_index < checkpoint_index
    assert trade_index < checkpoint_index
    assert all(
        "option_market_state" not in statement
        for statement in statements
    )


class BoundFlowSource:
    def __init__(self, feed, source_flow):
        self._feed = feed
        self.generation = feed.generation
        self.source_flow = source_flow

    def read_after(self, offset, *, limit=1000):
        return self._feed.read_after(offset, limit=limit)

    def last_offset(self):
        return self._feed.last_offset()


def test_flow_runner_persists_v4_source_policy_identity():
    flow_settings = settings()
    semantics = pinned_semantics()
    flow_policy = policy(
        sweep_window_seconds=5,
        minimum_sweep_prints=3,
    )
    feed = scenario_feed()
    exchange_reference = load_default_options_exchange_reference()
    base = EngineConfig(
        cohort_id=FLOW_COHORT,
        provider_tier=flow_settings.provider_tier,
        policy=flow_policy,
        semantics=semantics,
        journal_generation=feed.generation,
        shard_count=flow_settings.shard_count,
        queue_capacity=flow_settings.queue_capacity,
        checkpoint_interval_ms=flow_settings.checkpoint_interval_ms,
        advanced_flow_policy=advanced_policy(),
        contract_scope=advanced_scope(),
        exchange_reference=exchange_reference,
        volume=VolumeConfig(
            history_per_contract=flow_settings.history_per_contract,
            initial_baseline=flow_settings.initial_baseline,
        ),
    )
    source_flow = build_option_source_flow_identity(
        journal_generation=feed.generation,
        stream_session_ids=(FLOW_STREAM,),
        condition_semantics_sha256=semantics.semantics_sha256,
        flow_policy_sha256=base.policy_sha256(),
        advanced_flow_policy_sha256=base.advanced_flow_policy.sha256,
        contract_scope_sha256=base.contract_scope.sha256,
        provider_contract_sha256=(
            MASSIVE_OPTIONS_PROVIDER_CONTRACT.sha256
        ),
        condition_reference_sha256=semantics.pin.payload_sha256,
        exchange_reference_sha256=exchange_reference.sha256,
    )
    source = BoundFlowSource(feed, source_flow)
    world = harness(
        source,
        cohort_id=FLOW_COHORT,
        semantics=semantics,
        flow_policy=flow_policy,
        flow_settings=flow_settings,
    )
    unbound = option_runners._engine_config(
        world.environment,
        flow_settings,
        FLOW_COHORT,
        feed.generation,
        source_flow=None,
    )
    source_flow = build_option_source_flow_identity(
        journal_generation=feed.generation,
        stream_session_ids=(FLOW_STREAM,),
        condition_semantics_sha256=semantics.semantics_sha256,
        flow_policy_sha256=unbound.policy_sha256(),
        advanced_flow_policy_sha256=(
            unbound.advanced_flow_policy.sha256
        ),
        contract_scope_sha256=unbound.contract_scope.sha256,
        provider_contract_sha256=(
            unbound.provider_contract.sha256
        ),
        condition_reference_sha256=(
            semantics.pin.payload_sha256
        ),
        exchange_reference_sha256=(
            unbound.exchange_reference.sha256
        ),
    )
    source.source_flow = source_flow

    assert options_flow_block_reason(world.environment) is None
    outcome = world.tick()
    assert outcome.state == "COMPLETE"
    rows = world.database.event_rows()
    assert rows
    assert {row["lineage_version"] for row in rows} == {"4"}
    assert {row["source_flow_id"] for row in rows} == {
        str(source_flow.source_flow_id)
    }
    assert {row["condition_semantics_sha256"] for row in rows} == {
        source_flow.condition_semantics_sha256
    }
    assert {row["flow_policy_sha256"] for row in rows} == {
        source_flow.flow_policy_sha256
    }


def test_flow_source_identity_fails_closed_before_any_read():
    feed = scenario_feed()
    source = BoundFlowSource(feed, object())
    world = harness(source)

    assert options_flow_block_reason(world.environment) == (
        OPTIONS_FLOW_SOURCE_IDENTITY_UNPINNED
    )
    outcome = world.tick()
    assert outcome.reason_codes == (
        OPTIONS_FLOW_SOURCE_IDENTITY_UNPINNED,
    )


def test_flow_source_policy_pin_mismatch_fails_closed_before_any_read():
    feed = scenario_feed()
    semantics = pinned_semantics()
    source_flow = build_option_source_flow_identity(
        journal_generation=feed.generation,
        stream_session_ids=(FLOW_STREAM,),
        condition_semantics_sha256=semantics.semantics_sha256,
        flow_policy_sha256="f" * 64,
    )
    source = BoundFlowSource(feed, source_flow)
    world = harness(source, semantics=semantics)

    outcome = world.tick()
    assert outcome.reason_codes == (
        OPTIONS_FLOW_SOURCE_IDENTITY_UNPINNED,
    )
    assert world.database.events == []
    assert world.environment.options_flow_state.session is None


def test_market_state_slice_cannot_commit_after_state_moves(tmp_path):
    control, source = _source(tmp_path)
    consumer = AdvancedOptionsMarketStateConsumer(
        source=source,
        cohort_id=control.cohort_id,
        shard_map=_shard_map(),
        owner_epoch=1,
        clock=lambda: NOW,
    )
    planned = consumer.plan_slice(maximum_records=1)
    consumer.commit_slice(planned)

    with pytest.raises(ValueError, match="does not follow"):
        consumer.commit_slice(planned)
