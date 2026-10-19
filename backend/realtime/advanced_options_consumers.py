"""Provider-free P3B bindings for canonical Advanced Options consumers."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable
from uuid import UUID

from .advanced_options_control_plane import (
    CANONICAL_MARKET_STATE_OWNER,
)
from .advanced_options_ingress import AdvancedOptionsJournalSource
from .contracts import canonical_sha256
from .materialization import (
    MaterializerCheckpoint,
    MaterializerShardMap,
    MaterializerShardState,
    build_materializer_checkpoint,
    dirty_materializer_instruments,
    initial_materializer_state,
    materialize_shard_fanout,
)
from .materialization_repository import (
    persist_quote_materialization_cursor,
)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsMarketStateSlice:
    start_offset: int
    end_offset: int
    record_count: int
    shard_map: MaterializerShardMap
    states: tuple[MaterializerShardState, ...]
    checkpoints: tuple[MaterializerCheckpoint, ...]
    dirty_instrument_ids: tuple[frozenset[int], ...]
    state_sha256: str


class AdvancedOptionsMarketStateConsumer:
    def __init__(
        self,
        *,
        source: AdvancedOptionsJournalSource,
        cohort_id: UUID,
        shard_map: MaterializerShardMap,
        owner_epoch: int,
        states: tuple[MaterializerShardState, ...] | None = None,
        clock: Callable[[], datetime] = (
            lambda: datetime.now(timezone.utc)
        ),
    ) -> None:
        if not isinstance(source, AdvancedOptionsJournalSource):
            raise TypeError("Advanced Options market-state source is invalid")
        if not isinstance(cohort_id, UUID):
            raise TypeError("Advanced Options market-state cohort is invalid")
        if not isinstance(shard_map, MaterializerShardMap):
            raise TypeError("Advanced Options market-state shard map is invalid")
        if type(owner_epoch) is not int or owner_epoch <= 0:
            raise ValueError("Advanced Options market-state owner epoch is invalid")
        if (
            source.registration.shard_map_sha256 != shard_map.sha256
            or source.registration.shard_ids
            != tuple(range(shard_map.shard_count))
        ):
            raise ValueError(
                "Advanced Options market-state shard map is not registered"
            )
        initial = (
            tuple(
                initial_materializer_state(shard_map, shard_id)
                for shard_id in range(shard_map.shard_count)
            )
            if states is None
            else tuple(states)
        )
        if (
            len(initial) != shard_map.shard_count
            or {state.shard_id for state in initial}
            != set(range(shard_map.shard_count))
            or len({state.scanned_offset for state in initial}) != 1
            or any(
                state.shard_map_sha256 != shard_map.sha256
                for state in initial
            )
        ):
            raise ValueError(
                "Advanced Options market-state checkpoints are incomplete"
            )
        self.source = source
        self.cohort_id = cohort_id
        self.shard_map = shard_map
        self.owner_epoch = owner_epoch
        self._states = tuple(sorted(initial, key=lambda state: state.shard_id))
        self._clock = clock

    @property
    def states(self) -> tuple[MaterializerShardState, ...]:
        return self._states

    @property
    def committed_offset(self) -> int:
        return self._states[0].scanned_offset

    def plan_slice(
        self,
        *,
        maximum_records: int = 1000,
    ) -> AdvancedOptionsMarketStateSlice:
        if type(maximum_records) is not int or not 1 <= maximum_records <= 10_000:
            raise ValueError("market-state record bound must be in [1, 10000]")
        records = tuple(self.source.read_normalized_after(
            self.committed_offset,
            limit=maximum_records,
        ))
        next_states = materialize_shard_fanout(
            self._states,
            self.shard_map,
            records,
        )
        dirty = tuple(
            dirty_materializer_instruments(previous, current)
            for previous, current in zip(self._states, next_states)
        )
        committed_at = self._clock()
        checkpoints = tuple(
            build_materializer_checkpoint(
                state,
                consumer_name=CANONICAL_MARKET_STATE_OWNER,
                cohort_id=self.cohort_id,
                journal_generation=self.source.generation,
                owner_epoch=self.owner_epoch,
                committed_at=committed_at,
            )
            for state in next_states
        )
        return AdvancedOptionsMarketStateSlice(
            start_offset=self.committed_offset,
            end_offset=next_states[0].scanned_offset,
            record_count=len(records),
            shard_map=self.shard_map,
            states=next_states,
            checkpoints=checkpoints,
            dirty_instrument_ids=dirty,
            state_sha256=canonical_sha256(next_states),
        )

    def commit_slice(
        self,
        planned: AdvancedOptionsMarketStateSlice,
    ) -> None:
        if not isinstance(planned, AdvancedOptionsMarketStateSlice):
            raise TypeError("Advanced Options market-state slice is invalid")
        if planned.start_offset != self.committed_offset:
            raise ValueError("market-state slice does not follow committed state")
        if (
            tuple(checkpoint.committed_offset for checkpoint in planned.checkpoints)
            != tuple(state.scanned_offset for state in planned.states)
            or planned.shard_map.sha256 != self.shard_map.sha256
            or canonical_sha256(planned.states) != planned.state_sha256
        ):
            raise ValueError("market-state slice identity changed before commit")
        self._states = planned.states


def persist_advanced_options_market_state_slice_cursor(
    cursor,
    *,
    cohort_id: UUID,
    journal_generation: UUID,
    planned: AdvancedOptionsMarketStateSlice,
    owner_id: UUID,
    batch_executor,
) -> None:
    if not isinstance(planned, AdvancedOptionsMarketStateSlice):
        raise TypeError("Advanced Options market-state slice is invalid")
    if not (
        len(planned.states)
        == len(planned.checkpoints)
        == len(planned.dirty_instrument_ids)
        == planned.shard_map.shard_count
    ):
        raise ValueError("market-state slice checkpoint membership is incomplete")
    for state, checkpoint, dirty in zip(
        planned.states,
        planned.checkpoints,
        planned.dirty_instrument_ids,
    ):
        if (
            checkpoint.cohort_id != cohort_id
            or checkpoint.journal_generation != journal_generation
            or checkpoint.consumer_name != CANONICAL_MARKET_STATE_OWNER
        ):
            raise ValueError("market-state persistence scope changed")
        persist_quote_materialization_cursor(
            cursor,
            provider="massive",
            lease_resource_key=(
                f"{CANONICAL_MARKET_STATE_OWNER}:{cohort_id}:"
                f"{journal_generation}:{state.shard_id}"
            ),
            owner_id=owner_id,
            checkpoint=checkpoint,
            state=state,
            shard_map=planned.shard_map,
            dirty_instrument_ids=dirty,
            batch_executor=batch_executor,
        )
