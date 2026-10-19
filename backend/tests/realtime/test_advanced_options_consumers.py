from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import UUID

import pytest
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
from realtime.advanced_options_consumers import (
    AdvancedOptionsMarketStateConsumer,
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
    )

    statements = tuple(call.args[0] for call in cursor.execute.call_args_list)
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
