"""Provider-free P3B bindings for canonical Advanced Options consumers."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from types import MappingProxyType
from typing import Callable, Mapping
from uuid import UUID

from psycopg2.extras import Json

from .advanced_options_activity_momentum_c6 import (
    ActivityMomentumAllocation,
)
from .advanced_options_control_plane import (
    CANONICAL_MARKET_STATE_OWNER,
)
from .advanced_options_ingress import AdvancedOptionsJournalSource
from .contracts import MarketChannel, canonical_sha256
from .journal import NormalizedJournalRecord
from .materialization import (
    MaterializerCheckpoint,
    MaterializerShardMap,
    MaterializerShardState,
    build_materializer_checkpoint,
    restore_materializer_checkpoint,
    dirty_materializer_instruments,
    initial_materializer_state,
    materialize_shard_batch,
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
    detector_routing: (
        AdvancedOptionsDetectorRoutingResult | None
    ) = None


@dataclass(frozen=True, slots=True)
class AdvancedOptionsDetectorInvocation:
    consumer_name: str
    routing_key: str
    event_offset: int
    contract_ticker: str
    event_kind: MarketChannel
    event_at: datetime


@dataclass(frozen=True, slots=True)
class AdvancedOptionsDetectorRoutingState:
    allocation_sha256: str
    committed_event_offset: int
    quote_contract_tickers: tuple[str, ...]
    factual_volume: tuple[tuple[str, Decimal], ...]
    cadence_clocks_ns: tuple[tuple[str, int], ...]
    detector_invocation_counts: tuple[tuple[str, int], ...]
    scanned_event_count: int
    processed_detail_bytes: int
    first_event_ns: int | None
    last_event_ns: int | None
    irrelevant_event_count: int
    cadence_skip_count: int
    readiness_skip_count: int
    suppressed_output_count: int

    def __post_init__(self) -> None:
        if (
            len(self.allocation_sha256) != 64
            or self.committed_event_offset < 0
            or any(value < 0 for _, value in self.factual_volume)
            or any(value < 0 for _, value in self.cadence_clocks_ns)
            or any(
                value < 0
                for _, value in self.detector_invocation_counts
            )
            or any(
                type(value) is not int or value < 0
                for value in (
                    self.scanned_event_count,
                    self.processed_detail_bytes,
                    self.irrelevant_event_count,
                    self.cadence_skip_count,
                    self.readiness_skip_count,
                    self.suppressed_output_count,
                )
            )
            or (self.first_event_ns is None)
            != (self.last_event_ns is None)
            or (
                self.first_event_ns is not None
                and (
                    type(self.first_event_ns) is not int
                    or type(self.last_event_ns) is not int
                    or self.first_event_ns < 0
                    or self.last_event_ns < self.first_event_ns
                )
            )
        ):
            raise ValueError(
                "Advanced Options detector routing state is invalid"
            )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsDetectorRoutingResult:
    start_offset: int
    end_offset: int
    record_count: int
    invocations: tuple[AdvancedOptionsDetectorInvocation, ...]
    state: AdvancedOptionsDetectorRoutingState
    route_entry_count: int
    quote_cache_size: int
    factual_volume_cache_size: int


class AdvancedOptionsBoundedDetectorRouter:
    def __init__(
        self,
        *,
        allocation: ActivityMomentumAllocation,
        state: AdvancedOptionsDetectorRoutingState | None = None,
    ) -> None:
        if not isinstance(allocation, ActivityMomentumAllocation):
            raise TypeError(
                "Advanced Options detector allocation is invalid"
            )
        groups = {
            row.group_id: row for row in allocation.selected_groups
        }
        routes: dict[
            tuple[str, MarketChannel],
            set[tuple[str, str]],
        ] = {}
        consumer_cadence: dict[str, int] = {}
        required_inputs: dict[str, frozenset[str]] = {}
        for eligibility in allocation.consumer_eligibility:
            if not eligibility.enabled:
                continue
            consumer_cadence[eligibility.consumer_name] = (
                eligibility.cadence_ms
            )
            required_inputs[eligibility.consumer_name] = frozenset(
                eligibility.required_inputs
            )
            event_kinds = tuple(
                MarketChannel.TRADE
                if value == "T"
                else MarketChannel.QUOTE
                for value in eligibility.event_kinds
            )
            if eligibility.complete_groups_only:
                for group_id in eligibility.group_ids:
                    group = groups[group_id]
                    for ticker in group.contract_tickers:
                        for event_kind in event_kinds:
                            routes.setdefault(
                                (ticker, event_kind),
                                set(),
                            ).add((
                                eligibility.consumer_name,
                                group_id,
                            ))
            else:
                for ticker in eligibility.contract_tickers:
                    for event_kind in event_kinds:
                        routes.setdefault(
                            (ticker, event_kind),
                            set(),
                        ).add((
                            eligibility.consumer_name,
                            ticker,
                        ))
        normalized_routes = {
            key: tuple(sorted(values))
            for key, values in routes.items()
        }
        route_entry_count = sum(
            len(values) for values in normalized_routes.values()
        )
        enabled_consumer_count = len(consumer_cadence)
        maximum_route_entries = (
            len(allocation.selected_contract_tickers)
            * max(
                1,
                enabled_consumer_count
                + len(allocation.selected_groups),
            )
            * 2
        )
        if route_entry_count > maximum_route_entries:
            raise ValueError(
                "Advanced Options detector routing map is unbounded"
            )
        initial = (
            AdvancedOptionsDetectorRoutingState(
                allocation_sha256=allocation.allocation_sha256,
                committed_event_offset=0,
                quote_contract_tickers=(),
                factual_volume=(),
                cadence_clocks_ns=(),
                detector_invocation_counts=(),
                scanned_event_count=0,
                processed_detail_bytes=0,
                first_event_ns=None,
                last_event_ns=None,
                irrelevant_event_count=0,
                cadence_skip_count=0,
                readiness_skip_count=0,
                suppressed_output_count=0,
            )
            if state is None
            else state
        )
        if (
            initial.allocation_sha256
            != allocation.allocation_sha256
            or set(initial.quote_contract_tickers)
            - set(allocation.selected_contract_tickers)
            or {
                ticker for ticker, _ in initial.factual_volume
            }
            - set(allocation.selected_contract_tickers)
        ):
            raise ValueError(
                "Advanced Options detector routing restart changed"
            )
        self.allocation = allocation
        self._groups = MappingProxyType(groups)
        self._routes = MappingProxyType(normalized_routes)
        self._consumer_cadence = MappingProxyType(
            consumer_cadence
        )
        self._required_inputs = MappingProxyType(required_inputs)
        self._state = initial
        self.route_entry_count = route_entry_count

    @property
    def state(self) -> AdvancedOptionsDetectorRoutingState:
        return self._state

    @property
    def routes(self) -> Mapping[
        tuple[str, MarketChannel],
        tuple[tuple[str, str], ...],
    ]:
        return self._routes

    def process(
        self,
        records: tuple[NormalizedJournalRecord, ...],
        *,
        maximum_output_invocations: int | None = None,
    ) -> AdvancedOptionsDetectorRoutingResult:
        if (
            maximum_output_invocations is not None
            and (
                type(maximum_output_invocations) is not int
                or maximum_output_invocations < 0
            )
        ):
            raise ValueError(
                "detector invocation output cap is invalid"
            )
        start_offset = self._state.committed_event_offset
        quote_tickers = set(
            self._state.quote_contract_tickers
        )
        factual_volume = dict(self._state.factual_volume)
        cadence_clocks = dict(self._state.cadence_clocks_ns)
        invocation_counts = dict(
            self._state.detector_invocation_counts
        )
        scanned = self._state.scanned_event_count
        processed_bytes = self._state.processed_detail_bytes
        first_event_ns = self._state.first_event_ns
        last_event_ns = self._state.last_event_ns
        irrelevant = self._state.irrelevant_event_count
        cadence_skips = self._state.cadence_skip_count
        readiness_skips = self._state.readiness_skip_count
        suppressed = self._state.suppressed_output_count
        invocations = []
        previous = start_offset
        admitted = set(self.allocation.selected_contract_tickers)
        for record in records:
            if (
                not isinstance(record, NormalizedJournalRecord)
                or record.event_offset != previous + 1
                or record.provider_symbol not in admitted
            ):
                raise ValueError(
                    "Advanced Options detector routing input changed"
                )
            previous = record.event_offset
            scanned += 1
            processed_bytes += len(
                record.details_json.encode("utf-8")
            )
            event_ns = int(
                record.event_at.timestamp() * 1_000_000_000
            )
            first_event_ns = (
                event_ns
                if first_event_ns is None
                else min(first_event_ns, event_ns)
            )
            last_event_ns = (
                event_ns
                if last_event_ns is None
                else max(last_event_ns, event_ns)
            )
            ticker = record.provider_symbol
            if record.event_kind is MarketChannel.QUOTE:
                quote_tickers.add(ticker)
            elif (
                record.event_kind is MarketChannel.TRADE
                and record.size is not None
            ):
                factual_volume[ticker] = (
                    factual_volume.get(ticker, Decimal("0"))
                    + record.size
                )
            routed = self._routes.get(
                (ticker, record.event_kind),
                (),
            )
            if not routed:
                irrelevant += 1
                continue
            for consumer_name, routing_key in routed:
                inputs = self._required_inputs[consumer_name]
                group = self._groups.get(routing_key)
                if (
                    group is not None
                    and "QUOTE_STATE" in inputs
                    and not set(group.contract_tickers).issubset(
                        quote_tickers
                    )
                ):
                    readiness_skips += 1
                    continue
                if (
                    group is None
                    and "QUOTE_STATE" in inputs
                    and ticker not in quote_tickers
                ):
                    readiness_skips += 1
                    continue
                cadence_key = (
                    f"{consumer_name}:{routing_key}"
                )
                cadence_ns = (
                    self._consumer_cadence[consumer_name]
                    * 1_000_000
                )
                previous_ns = cadence_clocks.get(cadence_key)
                if (
                    previous_ns is not None
                    and event_ns - previous_ns < cadence_ns
                ):
                    cadence_skips += 1
                    continue
                cadence_clocks[cadence_key] = event_ns
                invocation_counts[consumer_name] = (
                    invocation_counts.get(consumer_name, 0) + 1
                )
                invocation = AdvancedOptionsDetectorInvocation(
                    consumer_name=consumer_name,
                    routing_key=routing_key,
                    event_offset=record.event_offset,
                    contract_ticker=ticker,
                    event_kind=record.event_kind,
                    event_at=record.event_at,
                )
                if (
                    maximum_output_invocations is None
                    or len(invocations)
                    < maximum_output_invocations
                ):
                    invocations.append(invocation)
                else:
                    suppressed += 1
        next_state = AdvancedOptionsDetectorRoutingState(
            allocation_sha256=self.allocation.allocation_sha256,
            committed_event_offset=previous,
            quote_contract_tickers=tuple(sorted(quote_tickers)),
            factual_volume=tuple(sorted(factual_volume.items())),
            cadence_clocks_ns=tuple(sorted(cadence_clocks.items())),
            detector_invocation_counts=tuple(sorted(
                invocation_counts.items()
            )),
            scanned_event_count=scanned,
            processed_detail_bytes=processed_bytes,
            first_event_ns=first_event_ns,
            last_event_ns=last_event_ns,
            irrelevant_event_count=irrelevant,
            cadence_skip_count=cadence_skips,
            readiness_skip_count=readiness_skips,
            suppressed_output_count=suppressed,
        )
        self._state = next_state
        return AdvancedOptionsDetectorRoutingResult(
            start_offset=start_offset,
            end_offset=previous,
            record_count=len(records),
            invocations=tuple(invocations),
            state=next_state,
            route_entry_count=self.route_entry_count,
            quote_cache_size=len(quote_tickers),
            factual_volume_cache_size=len(factual_volume),
        )


def advanced_options_detector_routing_state_payload(
    state: AdvancedOptionsDetectorRoutingState,
) -> dict[str, object]:
    unsigned = {
        "schema": "advanced-options-detector-routing-state-v1",
        "allocation_sha256": state.allocation_sha256,
        "committed_event_offset": state.committed_event_offset,
        "quote_contract_tickers": list(
            state.quote_contract_tickers
        ),
        "factual_volume": [
            {"contract_ticker": ticker, "volume": str(volume)}
            for ticker, volume in state.factual_volume
        ],
        "cadence_clocks_ns": [
            {"routing_key": key, "event_ns": value}
            for key, value in state.cadence_clocks_ns
        ],
        "detector_invocation_counts": [
            {"consumer_name": name, "count": count}
            for name, count in state.detector_invocation_counts
        ],
        "scanned_event_count": state.scanned_event_count,
        "processed_detail_bytes": state.processed_detail_bytes,
        "first_event_ns": state.first_event_ns,
        "last_event_ns": state.last_event_ns,
        "irrelevant_event_count": state.irrelevant_event_count,
        "cadence_skip_count": state.cadence_skip_count,
        "readiness_skip_count": state.readiness_skip_count,
        "suppressed_output_count": state.suppressed_output_count,
    }
    return {
        **unsigned,
        "state_sha256": canonical_sha256(unsigned),
    }


def advanced_options_detector_routing_state_from_payload(
    value: object,
) -> AdvancedOptionsDetectorRoutingState:
    if (
        not isinstance(value, Mapping)
        or value.get("schema")
        != "advanced-options-detector-routing-state-v1"
    ):
        raise ValueError(
            "Advanced Options detector routing state changed"
        )
    unsigned = dict(value)
    identity = unsigned.pop("state_sha256", None)
    if identity != canonical_sha256(unsigned):
        raise ValueError(
            "Advanced Options detector routing state identity changed"
        )
    state = AdvancedOptionsDetectorRoutingState(
        allocation_sha256=str(value["allocation_sha256"]),
        committed_event_offset=int(
            value["committed_event_offset"]
        ),
        quote_contract_tickers=tuple(
            str(item) for item in value["quote_contract_tickers"]
        ),
        factual_volume=tuple(
            (
                str(row["contract_ticker"]),
                Decimal(str(row["volume"])),
            )
            for row in value["factual_volume"]
        ),
        cadence_clocks_ns=tuple(
            (
                str(row["routing_key"]),
                int(row["event_ns"]),
            )
            for row in value["cadence_clocks_ns"]
        ),
        detector_invocation_counts=tuple(
            (
                str(row["consumer_name"]),
                int(row["count"]),
            )
            for row in value["detector_invocation_counts"]
        ),
        scanned_event_count=int(value["scanned_event_count"]),
        processed_detail_bytes=int(value["processed_detail_bytes"]),
        first_event_ns=(
            None
            if value.get("first_event_ns") is None
            else int(value["first_event_ns"])
        ),
        last_event_ns=(
            None
            if value.get("last_event_ns") is None
            else int(value["last_event_ns"])
        ),
        irrelevant_event_count=int(value["irrelevant_event_count"]),
        cadence_skip_count=int(value["cadence_skip_count"]),
        readiness_skip_count=int(value["readiness_skip_count"]),
        suppressed_output_count=int(value["suppressed_output_count"]),
    )
    if advanced_options_detector_routing_state_payload(
        state
    ) != dict(value):
        raise ValueError(
            "Advanced Options detector routing payload changed"
        )
    return state


def load_advanced_options_detector_routing_state_cursor(
    cursor,
    *,
    selection_generation_id: UUID,
) -> AdvancedOptionsDetectorRoutingState | None:
    cursor.execute(
        "SELECT state_document "
        "FROM rt.advanced_options_detector_routing_states "
        "WHERE selection_generation_id=%s",
        (str(selection_generation_id),),
    )
    row = cursor.fetchone()
    return (
        None
        if row is None
        else advanced_options_detector_routing_state_from_payload(
            row[0]
        )
    )


def persist_advanced_options_detector_routing_state_cursor(
    cursor,
    *,
    selection_generation_id: UUID,
    state: AdvancedOptionsDetectorRoutingState,
    updated_at: datetime,
) -> None:
    document = advanced_options_detector_routing_state_payload(
        state
    )
    cursor.execute(
        "SELECT allocation_sha256,committed_event_offset,"
        "state_sha256,state_document "
        "FROM rt.advanced_options_detector_routing_states "
        "WHERE selection_generation_id=%s FOR UPDATE",
        (str(selection_generation_id),),
    )
    current = cursor.fetchone()
    if current is None:
        cursor.execute(
            "INSERT INTO rt.advanced_options_detector_routing_states ("
            "selection_generation_id,allocation_sha256,"
            "committed_event_offset,state_sha256,state_document,"
            "updated_at) VALUES (%s,%s,%s,%s,%s,%s)",
            (
                str(selection_generation_id),
                state.allocation_sha256,
                state.committed_event_offset,
                document["state_sha256"],
                Json(document),
                updated_at,
            ),
        )
    else:
        current_allocation = str(current[0])
        current_offset = int(current[1])
        current_state_sha256 = str(current[2])
        current_document = dict(current[3])
        if current_allocation != state.allocation_sha256:
            raise ValueError(
                "Advanced Options detector routing allocation changed"
            )
        if state.committed_event_offset < current_offset:
            raise ValueError(
                "Advanced Options detector routing progress is stale"
            )
        if state.committed_event_offset == current_offset:
            if (
                current_state_sha256 != document["state_sha256"]
                or current_document != document
            ):
                raise ValueError(
                    "Advanced Options detector routing replay diverged"
                )
            return
        cursor.execute(
            "UPDATE rt.advanced_options_detector_routing_states SET "
            "committed_event_offset=%s,state_sha256=%s,"
            "state_document=%s,updated_at=%s "
            "WHERE selection_generation_id=%s "
            "AND allocation_sha256=%s "
            "AND committed_event_offset=%s",
            (
                state.committed_event_offset,
                document["state_sha256"],
                Json(document),
                updated_at,
                str(selection_generation_id),
                state.allocation_sha256,
                current_offset,
            ),
        )
        if cursor.rowcount != 1:
            raise ValueError(
                "Advanced Options detector routing progress changed"
            )
    cursor.execute(
        "SELECT state_document "
        "FROM rt.advanced_options_detector_routing_states "
        "WHERE selection_generation_id=%s",
        (str(selection_generation_id),),
    )
    stored = cursor.fetchone()
    if (
        stored is None
        or json.loads(json.dumps(stored[0], sort_keys=True))
        != json.loads(json.dumps(document, sort_keys=True))
    ):
        raise ValueError(
            "Advanced Options detector routing persistence diverged"
        )


class AdvancedOptionsMarketStateConsumer:
    def __init__(
        self,
        *,
        source: AdvancedOptionsJournalSource,
        cohort_id: UUID,
        shard_map: MaterializerShardMap,
        owner_epoch: int,
        consumer_name: str = CANONICAL_MARKET_STATE_OWNER,
        states: tuple[MaterializerShardState, ...] | None = None,
        detector_router: (
            AdvancedOptionsBoundedDetectorRouter | None
        ) = None,
        owned_shard_ids: tuple[int, ...] | None = None,
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
        name = consumer_name.strip()
        if not name:
            raise ValueError(
                "Advanced Options market-state consumer name is invalid"
            )
        if (
            source.registration.shard_map_sha256 != shard_map.sha256
            or source.registration.shard_ids
            != tuple(range(shard_map.shard_count))
        ):
            raise ValueError(
                "Advanced Options market-state shard map is not registered"
            )
        owned = (
            tuple(range(shard_map.shard_count))
            if owned_shard_ids is None
            else tuple(sorted(set(owned_shard_ids)))
        )
        if (
            not owned
            or len(owned) != len(
                owned_shard_ids
                if owned_shard_ids is not None
                else owned
            )
            or any(
                type(shard_id) is not int
                or not 0 <= shard_id < shard_map.shard_count
                for shard_id in owned
            )
        ):
            raise ValueError(
                "Advanced Options owned shard identities are invalid"
            )
        initial = (
            tuple(
                initial_materializer_state(shard_map, shard_id)
                for shard_id in owned
            )
            if states is None
            else tuple(states)
        )
        if (
            len(initial) != len(owned)
            or {state.shard_id for state in initial}
            != set(owned)
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
        self.consumer_name = name
        self.owned_shard_ids = owned
        self._states = tuple(sorted(initial, key=lambda state: state.shard_id))
        if (
            detector_router is not None
            and (
                owned != tuple(range(shard_map.shard_count))
                or detector_router.state.committed_event_offset
                != self._states[0].scanned_offset
            )
        ):
            raise ValueError(
                "detector routing checkpoint is not aligned"
            )
        self._detector_router = detector_router
        self._clock = clock

    @property
    def states(self) -> tuple[MaterializerShardState, ...]:
        return self._states

    @property
    def committed_offset(self) -> int:
        return self._states[0].scanned_offset

    @property
    def detector_router(
        self,
    ) -> AdvancedOptionsBoundedDetectorRouter | None:
        return self._detector_router

    def plan_slice(
        self,
        *,
        maximum_records: int = 1000,
        records=None,
    ) -> AdvancedOptionsMarketStateSlice:
        """`records`: V3-1 (Nov 11) shared read: the records after this consumer's committed offset that the role
        already read once for every consumer and the detector router (same validation, same order). None reads them
        here, as before. The materializer still requires them to start right after the committed offset."""
        if type(maximum_records) is not int or not 1 <= maximum_records <= 10_000:
            raise ValueError("market-state record bound must be in [1, 10000]")
        if records is None:
            records = tuple(self.source.read_normalized_after(
                self.committed_offset,
                limit=maximum_records,
            ))
        else:
            records = tuple(records)[:maximum_records]
            if records and records[0].event_offset != self.committed_offset + 1:
                raise ValueError("shared market-state records do not follow the committed offset")
        next_states = (
            materialize_shard_fanout(
                self._states,
                self.shard_map,
                records,
            )
            if len(self._states) == self.shard_map.shard_count
            else tuple(
                materialize_shard_batch(
                    state,
                    self.shard_map,
                    records,
                )
                for state in self._states
            )
        )
        dirty = tuple(
            dirty_materializer_instruments(previous, current)
            for previous, current in zip(self._states, next_states)
        )
        committed_at = self._clock()
        checkpoints = tuple(
            build_materializer_checkpoint(
                state,
                consumer_name=self.consumer_name,
                cohort_id=self.cohort_id,
                journal_generation=self.source.generation,
                owner_epoch=self.owner_epoch,
                committed_at=committed_at,
            )
            for state in next_states
        )
        detector_routing = None
        if self._detector_router is not None:
            planned_router = AdvancedOptionsBoundedDetectorRouter(
                allocation=self._detector_router.allocation,
                state=self._detector_router.state,
            )
            detector_routing = planned_router.process(records)
        return AdvancedOptionsMarketStateSlice(
            start_offset=self.committed_offset,
            end_offset=next_states[0].scanned_offset,
            record_count=len(records),
            shard_map=self.shard_map,
            states=next_states,
            checkpoints=checkpoints,
            dirty_instrument_ids=dirty,
            state_sha256=_market_state_slice_identity(next_states, checkpoints),
            detector_routing=detector_routing,
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
            or _market_state_slice_identity(planned.states, planned.checkpoints) != planned.state_sha256
        ):
            raise ValueError("market-state slice identity changed before commit")
        self._states = planned.states
        if planned.detector_routing is not None:
            if self._detector_router is None:
                raise ValueError(
                    "detector routing appeared after planning"
                )
            self._detector_router = AdvancedOptionsBoundedDetectorRouter(
                allocation=self._detector_router.allocation,
                state=planned.detector_routing.state,
            )


MARKET_STATE_HASH_SWITCH = "REALTIME_V3_MARKET_STATE_HASH"


def market_state_hash_mode() -> str:
    """V3-1.6 (Nov 12): `CHECKPOINT` derives a market-state slice's identity from its checkpoints; anything else (the
    default, `CANONICAL`) hashes every shard state's canonical JSON as before."""
    value = os.environ.get(MARKET_STATE_HASH_SWITCH, "CANONICAL").strip().upper()
    return "CHECKPOINT" if value == "CHECKPOINT" else "CANONICAL"


def _market_state_slice_identity(states, checkpoints) -> str:
    """The identity a planned slice is committed under (plan and commit must agree; it is never stored).

    CANONICAL hashes the full canonical JSON of every shard state on each pass (Nov 11 V3 #2 profile: 42% of the
    market-state role's busy samples, plan and commit together). CHECKPOINT hashes what each checkpoint already holds:
    its shard, offset and `state_hash`, the SHA-256 of `encode_materializer_state`, which encodes the whole shard state
    (the checkpoint restores from it). Checkpoints, payloads and stored states are the same in both modes."""
    if market_state_hash_mode() != "CHECKPOINT":
        return canonical_sha256(states)
    if len(checkpoints) != len(states):
        raise ValueError("market-state slice checkpoints do not match its states")
    return canonical_sha256({
        "contract": "advanced-options-market-state-slice-identity-v2",
        "shards": [
            (checkpoint.shard_id, checkpoint.committed_offset, checkpoint.state_hash)
            for checkpoint in checkpoints
        ],
    })


def persist_advanced_options_market_state_slice_cursor(
    cursor,
    *,
    cohort_id: UUID,
    journal_generation: UUID,
    planned: AdvancedOptionsMarketStateSlice,
    owner_id: UUID,
    batch_executor,
    lease_resource_key: str | None = None,
) -> None:
    if not isinstance(planned, AdvancedOptionsMarketStateSlice):
        raise TypeError("Advanced Options market-state slice is invalid")
    state_ids = tuple(state.shard_id for state in planned.states)
    if (
        not planned.states
        or len(planned.states)
        != len(planned.checkpoints)
        or len(planned.states) != len(planned.dirty_instrument_ids)
        or len(state_ids) != len(set(state_ids))
        or any(
            not 0 <= shard_id < planned.shard_map.shard_count
            for shard_id in state_ids
        )
        or tuple(
            checkpoint.shard_id
            for checkpoint in planned.checkpoints
        )
        != state_ids
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
            or checkpoint.consumer_name
            != planned.checkpoints[0].consumer_name
        ):
            raise ValueError("market-state persistence scope changed")
        persist_quote_materialization_cursor(
            cursor,
            provider="massive",
            lease_resource_key=(
                (
                    f"{checkpoint.consumer_name}:{cohort_id}:"
                    f"{journal_generation}:{state.shard_id}"
                )
                if lease_resource_key is None
                else lease_resource_key
            ),
            owner_id=owner_id,
            checkpoint=checkpoint,
            state=state,
            shard_map=planned.shard_map,
            dirty_instrument_ids=dirty,
            batch_executor=batch_executor,
        )


def load_advanced_options_market_state_cursor(
    cursor,
    *,
    consumer_name: str,
    cohort_id: UUID,
    journal_generation: UUID,
    shard_map: MaterializerShardMap,
    shard_ids: tuple[int, ...] | None = None,
) -> tuple[MaterializerShardState, ...] | None:
    name = consumer_name.strip()
    requested_shards = (
        tuple(range(shard_map.shard_count))
        if shard_ids is None
        else tuple(sorted(set(shard_ids)))
    )
    if (
        not name
        or not isinstance(cohort_id, UUID)
        or not isinstance(journal_generation, UUID)
        or not isinstance(shard_map, MaterializerShardMap)
        or not requested_shards
        or len(requested_shards) != len(
            shard_ids if shard_ids is not None else requested_shards
        )
        or any(
            type(shard_id) is not int
            or not 0 <= shard_id < shard_map.shard_count
            for shard_id in requested_shards
        )
    ):
        raise ValueError(
            "Advanced Options market-state restore scope is invalid"
        )
    cursor.execute(
        "SELECT checkpoint_id::text,consumer_name,"
        "cohort_id::text,journal_generation::text,shard_id,"
        "owner_epoch,committed_offset,state_codec,state_hash,"
        "state_payload,committed_at "
        "FROM rt.consumer_checkpoints "
        "WHERE consumer_name=%s AND cohort_id=%s "
        "AND journal_generation=%s AND shard_id=ANY(%s) "
        "ORDER BY shard_id",
        (
            name,
            str(cohort_id),
            str(journal_generation),
            list(requested_shards),
        ),
    )
    rows = cursor.fetchall()
    if not rows:
        return None
    checkpoints = tuple(
        MaterializerCheckpoint(
            checkpoint_id=UUID(str(row[0])),
            consumer_name=str(row[1]),
            cohort_id=UUID(str(row[2])),
            journal_generation=UUID(str(row[3])),
            shard_id=int(row[4]),
            owner_epoch=int(row[5]),
            committed_offset=int(row[6]),
            state_codec=str(row[7]),
            state_hash=str(row[8]),
            state_payload=bytes(row[9]),
            committed_at=row[10],
        )
        for row in rows
    )
    if (
        len(checkpoints) != len(requested_shards)
        or tuple(row.shard_id for row in checkpoints)
        != requested_shards
    ):
        raise ValueError(
            "Advanced Options persisted shard checkpoints are incomplete"
        )
    return tuple(
        restore_materializer_checkpoint(
            checkpoint,
            shard_map,
        )
        for checkpoint in checkpoints
    )
