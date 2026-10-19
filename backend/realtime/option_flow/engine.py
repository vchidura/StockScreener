from __future__ import annotations

import zlib
from collections import deque
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Any, Iterable, Mapping
from uuid import UUID, uuid5

from options.config import FlowStrategyPolicy

from ..advanced_options_flow_policy import (
    AdvancedOptionsContractScope,
    AdvancedOptionsContractScopeEntry,
    AdvancedOptionsFlowPolicy,
)
from ..advanced_options_provider_contract import (
    MASSIVE_OPTIONS_PROVIDER_CONTRACT,
    AdvancedOptionsExchangeReference,
    AdvancedOptionsProviderContract,
)
from ..contracts import canonical_sha256
from ..option_source_flow import OptionSourceFlowIdentity
from .classifier import AggressionPolicy, SideClass, SideConfidence, classify_aggression
from .clock import SessionClock, to_datetime
from .clusters import (
    CANONICAL_ORDER_WINDOWS_SECONDS,
    ClusterConfig,
    ClusterEngine,
    ClusterPrint,
    cluster_bucket,
)
from .conditions import ConditionSemantics, Treatment
from .labels import (
    OpeningInferencePolicy,
    OpeningLabel,
    PriorOpenInterest,
    assess_volume_vs_oi,
    infer_opening,
)
from .messages import MessageRejected, QuoteMessage, TradeMessage, parse_quote, parse_trade, underlyer_of
from .quote_cache import QuoteCache, QuoteCacheConfig
from .records import (
    EngineOutput,
    FlowEvent,
    FlowRetentionEvidence,
    GapRecord,
    ReconcileResult,
)
from .volume import BaselineKind, PrintStatus, VolumeBook, VolumeConfig

DETECTOR_VERSION = "option_flow_stream_v1"
EVENT_NAMESPACE = UUID("2f0a7d52-8c61-4b0e-a3d9-71b4e5c0f9a2")
PROVIDER_TIERS = frozenset({"MASSIVE_ADVANCED", "MASSIVE_DEVELOPER"})
DEFAULT_SHARD_COUNT = 9
_ROUTE_CACHE_LIMIT = 200_000
_NEAR = 1e-9
_SCORE_PLACES = Decimal("0.0001")


def _score(value: float) -> Decimal:
    return Decimal(repr(value)).quantize(_SCORE_PLACES)


class SubmitResult(str, Enum):
    ACCEPTED = "ACCEPTED"
    GAP_RECORDED = "GAP_RECORDED"
    REJECTED = "REJECTED"


@dataclass(frozen=True, slots=True)
class EngineConfig:
    cohort_id: UUID
    provider_tier: str
    policy: FlowStrategyPolicy
    semantics: ConditionSemantics
    journal_generation: UUID | None = None
    source_flow: OptionSourceFlowIdentity | None = None
    shard_count: int = DEFAULT_SHARD_COUNT
    queue_capacity: int = 50_000
    multiplier: int = 100
    minimum_size: int | None = None
    quote_cache: QuoteCacheConfig = QuoteCacheConfig()
    aggression: AggressionPolicy = AggressionPolicy()
    volume: VolumeConfig = VolumeConfig()
    cluster_windows_seconds: tuple[int, ...] = (
        CANONICAL_ORDER_WINDOWS_SECONDS
    )
    allowed_lateness_ms: int = 2_000
    finalized_retention_ms: int = 60_000
    emit_mixed_clusters: bool = True
    likely_opening_threshold: float = 0.6
    quote_universe: frozenset[str] | None = None
    allow_synthetic_semantics: bool = False
    persist_unknown_condition_prints: bool = True
    checkpoint_interval_ms: int = 5_000
    advanced_flow_policy: AdvancedOptionsFlowPolicy | None = None
    contract_scope: AdvancedOptionsContractScope | None = None
    provider_contract: AdvancedOptionsProviderContract = (
        MASSIVE_OPTIONS_PROVIDER_CONTRACT
    )
    exchange_reference: AdvancedOptionsExchangeReference | None = None

    def __post_init__(self) -> None:
        if self.provider_tier not in PROVIDER_TIERS:
            raise ValueError("provider_tier must be MASSIVE_ADVANCED or MASSIVE_DEVELOPER")
        if self.shard_count < 1 or self.queue_capacity < 2 or self.multiplier < 1:
            raise ValueError("shard_count, queue_capacity and multiplier must be positive")
        if self.minimum_size is not None and self.minimum_size < 1:
            raise ValueError("minimum_size must be positive")
        if self.checkpoint_interval_ms < 1:
            raise ValueError("checkpoint_interval_ms must be positive")
        if not self.semantics.is_live_capture and not self.allow_synthetic_semantics:
            raise ValueError("SYNTHETIC_SEMANTICS_NOT_ALLOWED: the condition pin is not a live capture")
        advanced = self.advanced_flow_policy
        scope = self.contract_scope
        if (
            not isinstance(
                self.provider_contract,
                AdvancedOptionsProviderContract,
            )
            or self.provider_contract.sha256
            != MASSIVE_OPTIONS_PROVIDER_CONTRACT.sha256
        ):
            raise ValueError("Massive Options provider contract is invalid")
        if self.exchange_reference is not None:
            if (
                not isinstance(
                    self.exchange_reference,
                    AdvancedOptionsExchangeReference,
                )
                or self.exchange_reference.provider_contract_sha256
                != self.provider_contract.sha256
            ):
                raise ValueError(
                    "exchange reference does not match provider contract"
                )
        if (advanced is None) != (scope is None):
            raise ValueError(
                "Advanced Options flow policy and contract scope must be bound together"
            )
        if advanced is not None:
            if not isinstance(advanced, AdvancedOptionsFlowPolicy):
                raise TypeError("Advanced Options flow policy is invalid")
            if not isinstance(scope, AdvancedOptionsContractScope):
                raise TypeError("Advanced Options contract scope is invalid")
            scope.validate_policy(advanced)
        if self.source_flow is not None:
            if self.journal_generation != self.source_flow.journal_generation:
                raise ValueError("source-flow journal generation does not match")
            if (
                self.semantics.semantics_sha256
                != self.source_flow.condition_semantics_sha256
                or self._configuration_policy_sha256()
                != self.source_flow.flow_policy_sha256
            ):
                raise ValueError(
                    "source-flow condition or policy pin does not match"
                )
            if advanced is not None and (
                self.source_flow.identity_version
                != (4 if self.exchange_reference is not None else 3)
                or self.source_flow.advanced_flow_policy_sha256
                != advanced.sha256
                or self.source_flow.contract_scope_sha256
                != scope.sha256
                or (
                    self.exchange_reference is not None
                    and (
                        self.source_flow.provider_contract_sha256
                        != self.provider_contract.sha256
                        or self.source_flow.condition_reference_sha256
                        != self.semantics.pin.payload_sha256
                        or self.source_flow.exchange_reference_sha256
                        != self.exchange_reference.sha256
                    )
                )
            ):
                raise ValueError(
                    "source-flow Advanced policy or scope pin does not match"
                )

    def cluster_config(self) -> ClusterConfig:
        return ClusterConfig.from_policy(
            self.policy,
            windows_seconds=self.cluster_windows_seconds,
            allowed_lateness_ms=self.allowed_lateness_ms,
            finalized_retention_ms=self.finalized_retention_ms,
            emit_mixed=self.emit_mixed_clusters,
        )

    def inference_policy(self) -> OpeningInferencePolicy:
        return OpeningInferencePolicy(
            minimum_volume_oi_ratio=self.policy.minimum_volume_oi_ratio,
            minimum_print_notional=self.policy.minimum_print_notional,
            likely_threshold=self.likely_opening_threshold,
        )

    def _policy_identity(self) -> dict[str, object]:
        cluster = self.cluster_config()
        return {
            "detector_version": DETECTOR_VERSION,
            "flow_policy": self.policy.model_dump(),
            "minimum_size": self.minimum_size,
            "multiplier": self.multiplier,
            "windows_seconds": cluster.windows_seconds,
            "window_hop": "HALF_WINDOW",
            "allowed_lateness_ms": cluster.allowed_lateness_ms,
            "finalized_retention_ms": cluster.finalized_retention_ms,
            "emit_mixed": cluster.emit_mixed,
            "aggression": self.aggression,
            "inference": self.inference_policy(),
            "semantics_version": self.semantics.version,
            "semantics_sha256": self.semantics.semantics_sha256,
            "advanced_flow_policy": self.advanced_flow_policy,
            "contract_scope": self.contract_scope,
            "provider_contract_sha256": self.provider_contract.sha256,
            "exchange_reference_sha256": (
                None
                if self.exchange_reference is None
                else self.exchange_reference.sha256
            ),
        }

    def _configuration_policy_sha256(self) -> str:
        return canonical_sha256(self._policy_identity())

    def policy_sha256(self) -> str:
        identity = self._policy_identity()
        if self.source_flow is not None:
            identity["source_flow"] = self.source_flow.lineage_identity
        return canonical_sha256(identity)


@dataclass(frozen=True, slots=True)
class GapMarker:
    """A recorded stream gap (rt.stream_gaps terms). contracts=None means every contract."""

    reason: str
    start_ms: int
    end_ms: int
    channel: str | None = None
    contracts: tuple[str, ...] | None = None
    first_offset: int | None = None
    last_offset: int | None = None

    def __post_init__(self) -> None:
        if not self.reason.strip():
            raise ValueError("a gap needs a reason")
        if self.channel not in (None, "T", "Q"):
            raise ValueError("gap channel must be T, Q or None")
        if self.start_ms < 0 or self.end_ms < self.start_ms:
            raise ValueError("gap end must not precede its start")


@dataclass(slots=True)
class EngineStats:
    submitted: int = 0
    queue_dropped: int = 0
    processed: int = 0
    trades: int = 0
    quotes: int = 0
    rejected: int = 0
    rejected_by_reason: dict[str, int] = field(default_factory=dict)
    duplicates: int = 0
    duplicate_conflicts: int = 0
    beyond_history: int = 0
    non_volume_prints: int = 0
    unknown_condition_prints: int = 0
    unknown_condition_codes: dict[int, int] = field(default_factory=dict)
    cancels: int = 0
    cancels_unmatched: int = 0
    counted_prints: int = 0
    covered_prints: int = 0
    late_prints: int = 0
    below_filter: int = 0
    persisted_events: int = 0
    cancel_revisions: int = 0
    gaps_applied: int = 0
    control_items: int = 0

    def accounted(self) -> int:
        """Every submitted message ends up processed, rejected or recorded as a gap drop."""
        return self.processed + self.rejected + self.queue_dropped


class _PendingGap:
    __slots__ = ("channels", "contracts", "count", "first_offset", "last_offset", "start_ms", "end_ms")

    def __init__(self) -> None:
        self.channels: set[str | None] = set()
        self.contracts: set[str] = set()
        self.count = 0
        self.first_offset: int | None = None
        self.last_offset: int | None = None
        self.start_ms: int | None = None
        self.end_ms: int | None = None


class _Control:
    __slots__ = ("kind", "payload")

    def __init__(self, kind: str, payload: Any) -> None:
        self.kind = kind
        self.payload = payload


class _Shard:
    __slots__ = ("index", "queue", "pending", "book", "quotes", "clusters", "last_session",
                 "last_time_ms", "next_checkpoint_ms", "last_offset", "reported_offset")

    def __init__(self, index: int, book: VolumeBook, quotes: QuoteCache, clusters: ClusterEngine) -> None:
        self.index = index
        self.queue: deque = deque()
        self.pending: _PendingGap | None = None
        self.book = book
        self.quotes = quotes
        self.clusters = clusters
        self.last_session: date | None = None
        self.last_time_ms = -1
        self.next_checkpoint_ms: int | None = None
        self.last_offset = 0
        self.reported_offset = 0


class OptionFlowEngine:
    """Parse, classify conditions, accumulate, filter, classify aggression, label, cluster.

    Every tracked eligible print updates its accumulator and quote cache before the size and
    premium filters decide what is persisted. Contracts are sharded by a stable hash of the
    underlyer so per-contract order is preserved. The reader-facing submit never blocks: a
    full shard queue records a gap and marks the affected accumulators INCOMPLETE, so nothing
    is dropped silently. Output depends only on the ordered input.

    Not thread-safe: one consumer loop calls submit and process (a reader thread hands
    messages to that loop through RawMessageBuffer or the journal). To use more cores, run one
    engine per process, each owning a disjoint set of underlyers.
    """

    def __init__(self, config: EngineConfig,
                 prior_open_interest: Mapping[str, PriorOpenInterest] | None = None) -> None:
        self.config = config
        self.stats = EngineStats()
        self._oi: dict[str, PriorOpenInterest] = dict(prior_open_interest or {})
        self._semantics = config.semantics
        self._aggression = config.aggression
        self._inference = config.inference_policy()
        self._cluster_config = config.cluster_config()
        self._policy_sha256 = config.policy_sha256()
        self._source_flow = config.source_flow
        self._advanced_policy = config.advanced_flow_policy
        self._contract_scope = config.contract_scope
        self._provider_contract = config.provider_contract
        self._exchange_reference = config.exchange_reference
        self._advanced_flow_policy_sha256 = (
            None
            if config.advanced_flow_policy is None
            else config.advanced_flow_policy.sha256
        )
        self._contract_scope_sha256 = (
            None
            if config.contract_scope is None
            else config.contract_scope.sha256
        )
        self._provider_contract_sha256 = config.provider_contract.sha256
        self._condition_reference_sha256 = (
            config.semantics.pin.payload_sha256
        )
        self._exchange_reference_sha256 = (
            None
            if config.exchange_reference is None
            else config.exchange_reference.sha256
        )
        self._contract_scope_map = (
            {}
            if config.contract_scope is None
            else config.contract_scope.contract_map
        )
        self._clock = SessionClock()
        self._multiplier = config.multiplier
        self._min_size = config.minimum_size
        self._threshold = config.policy.minimum_print_notional
        threshold = float(self._threshold)
        self._threshold_high = threshold * (1 + _NEAR)
        self._threshold_low = threshold * (1 - _NEAR)
        self._developer = config.provider_tier == "MASSIVE_DEVELOPER"
        self._quote_universe = config.quote_universe
        self._routes: dict[str, tuple[str, int] | None] = {}
        self._shards = tuple(
            _Shard(
                index,
                VolumeBook(config.cohort_id, config.volume),
                QuoteCache(config.quote_cache),
                ClusterEngine(self._cluster_config, cohort_id=config.cohort_id,
                              provider_tier=config.provider_tier, policy_sha256=self._policy_sha256,
                              source_flow=config.source_flow,
                              advanced_flow_policy_sha256=self._advanced_flow_policy_sha256,
                              contract_scope_sha256=self._contract_scope_sha256,
                              manifest_sha256=(
                                  None
                                  if config.contract_scope is None
                                  else config.contract_scope.manifest_sha256
                              ),
                              slot_sha256=(
                                  None
                                  if config.contract_scope is None
                                  else config.contract_scope.slot_sha256
                              ),
                              provider_contract_sha256=self._provider_contract_sha256,
                              condition_reference_sha256=self._condition_reference_sha256,
                              exchange_reference_sha256=self._exchange_reference_sha256,
                              clock=self._clock),
            )
            for index in range(config.shard_count)
        )

    @property
    def policy_sha256(self) -> str:
        return self._policy_sha256

    def set_prior_open_interest(self, values: Mapping[str, PriorOpenInterest]) -> None:
        """Load before processing a session; open interest is not an intraday stream."""
        self._oi = dict(values)

    def shard_of(self, symbol: str) -> int | None:
        route = self._route(symbol)
        return None if route is None else route[1]

    def queued(self) -> int:
        return sum(len(shard.queue) for shard in self._shards)

    def _route(self, symbol: Any) -> tuple[str, int] | None:
        if type(symbol) is not str:
            return None
        route = self._routes.get(symbol, False)
        if route is False:
            scope_entry = self._contract_scope_map.get(symbol)
            if self._contract_scope is not None:
                underlyer = (
                    None if scope_entry is None else scope_entry.underlyer
                )
            else:
                underlyer = underlyer_of(symbol) if symbol.isupper() else None
            route = None if underlyer is None else (
                underlyer, zlib.crc32(underlyer.encode("ascii")) % len(self._shards))
            if len(self._routes) >= _ROUTE_CACHE_LIMIT:
                self._routes.clear()
            self._routes[symbol] = route
        return route

    def submit(self, raw: Mapping[str, Any], offset: int | None = None) -> SubmitResult:
        """Hand one decoded T or Q message to its shard. Never blocks and never raises."""
        stats = self.stats
        stats.submitted += 1
        try:
            symbol = raw.get("sym")
        except AttributeError:
            symbol = None
        route = self._route(symbol)
        if route is None:
            self._count_rejection("INVALID_FIELD_sym")
            return SubmitResult.REJECTED
        shard = self._shards[route[1]]
        queue = shard.queue
        pending = shard.pending
        if len(queue) + (2 if pending is not None else 1) > self.config.queue_capacity:
            self._record_drop(shard, raw, symbol, offset)
            return SubmitResult.GAP_RECORDED
        if pending is not None:
            queue.append((None, _Control("PENDING_GAP", pending)))
            shard.pending = None
        queue.append((raw, offset))
        return SubmitResult.ACCEPTED

    def _record_drop(self, shard: _Shard, raw: Mapping[str, Any], symbol: str, offset: int | None) -> None:
        pending = shard.pending
        if pending is None:
            pending = shard.pending = _PendingGap()
        pending.count += 1
        self.stats.queue_dropped += 1
        pending.channels.add(raw.get("ev") if raw.get("ev") in ("T", "Q") else None)
        pending.contracts.add(symbol)
        if offset is not None:
            if pending.first_offset is None:
                pending.first_offset = offset
            pending.last_offset = offset
        time_ms = raw.get("t")
        if type(time_ms) is int and time_ms >= 0:
            if pending.start_ms is None or time_ms < pending.start_ms:
                pending.start_ms = time_ms
            if pending.end_ms is None or time_ms > pending.end_ms:
                pending.end_ms = time_ms

    def submit_gap(self, marker: GapMarker) -> None:
        """Queue a recorded gap in stream order. Control items bypass the queue capacity."""
        if marker.contracts is None:
            for shard in self._shards:
                shard.queue.append((None, _Control("GAP", marker)))
            return
        grouped: dict[int, list[str]] = {}
        for contract in marker.contracts:
            route = self._route(contract)
            if route is None:
                raise ValueError(f"gap names an unroutable contract: {contract!r}")
            grouped.setdefault(route[1], []).append(contract)
        for index, contracts in sorted(grouped.items()):
            self._shards[index].queue.append((None, _Control("GAP", GapMarker(
                marker.reason, marker.start_ms, marker.end_ms, marker.channel, tuple(sorted(contracts)),
                marker.first_offset, marker.last_offset))))

    def submit_reconcile(self, contract: str, snapshot_volume: int, watermark_seq: int,
                         snapshot_time_ms: int) -> None:
        """Queue a snapshot reconcile in stream order; the result arrives in the output."""
        route = self._route(contract)
        if route is None:
            raise ValueError(f"unroutable contract: {contract!r}")
        session_date = self._clock.session_date(snapshot_time_ms)
        self._shards[route[1]].queue.append((None, _Control("RECONCILE", (
            contract, session_date, snapshot_volume, watermark_seq, snapshot_time_ms))))

    def submit_quote_universe(self, contracts: Iterable[str] | None) -> None:
        """Queue a change of the quote-subscribed contracts (a slot promotion or rotation)."""
        universe = None if contracts is None else frozenset(contracts)
        for shard in self._shards:
            shard.queue.append((None, _Control("QUOTE_UNIVERSE", universe)))

    def submit_time(self, time_ms: int) -> None:
        """Queue an event-time heartbeat so a quiet shard still finalizes its clusters."""
        for shard in self._shards:
            shard.queue.append((None, _Control("TIME", time_ms)))

    def feed(self, raw: Mapping[str, Any], offset: int | None = None) -> EngineOutput:
        """Submit and process one message; the replay and test entry point."""
        self.submit(raw, offset)
        return self.process()

    def process(self, max_messages_per_shard: int | None = None) -> EngineOutput:
        out = EngineOutput()
        for shard in self._shards:
            self._drain(shard, out, max_messages_per_shard)
        return out

    def flush(self, *, final: bool = False) -> EngineOutput:
        """Emit dirty checkpoints; with final, also finalize every cluster (end of session)."""
        out = self.process()
        for shard in self._shards:
            if final:
                shard.clusters.finalize_all(out.clusters)
            out.checkpoints.extend(shard.book.checkpoint_rows())
        return out

    def _count_rejection(self, reason: str) -> None:
        stats = self.stats
        stats.rejected += 1
        stats.rejected_by_reason[reason] = stats.rejected_by_reason.get(reason, 0) + 1

    def _drain(self, shard: _Shard, out: EngineOutput, limit: int | None) -> None:
        queue = shard.queue
        popleft = queue.popleft
        stats = self.stats
        handled = 0
        while queue and (limit is None or handled < limit):
            raw, offset = popleft()
            handled += 1
            if raw is None:
                self._control(shard, offset, out)
                continue
            if offset is not None and offset > shard.last_offset:
                shard.last_offset = offset
            event_type = raw.get("ev")
            try:
                if event_type == "T":
                    self._trade(shard, parse_trade(raw, offset), out)
                    stats.processed += 1
                elif event_type == "Q":
                    self._quote(shard, parse_quote(raw, offset))
                    stats.processed += 1
                else:
                    raise MessageRejected("UNSUPPORTED_EVENT_TYPE")
            except MessageRejected as rejection:
                self._reject(shard, raw, event_type, rejection.reason)
        if shard.last_offset > shard.reported_offset:
            shard.reported_offset = shard.last_offset
            out.offsets[shard.index] = shard.last_offset
        if not queue and shard.pending is not None:
            pending, shard.pending = shard.pending, None
            self._apply_pending(shard, pending, out)

    def _reject(self, shard: _Shard, raw: Mapping[str, Any], event_type: Any, reason: str) -> None:
        self._count_rejection(reason)
        symbol = raw.get("sym")
        if event_type == "T" and type(symbol) is str and shard.last_session is not None:
            # An unreadable print may have been volume; the baseline can no longer be trusted.
            shard.book.mark_incomplete(symbol, shard.last_session, "UNREADABLE_TRADE_MESSAGE",
                                       shard.last_time_ms)

    def _control(self, shard: _Shard, control: _Control, out: EngineOutput) -> None:
        self.stats.control_items += 1
        if control.kind == "PENDING_GAP":
            self._apply_pending(shard, control.payload, out)
        elif control.kind == "GAP":
            marker: GapMarker = control.payload
            channels = {marker.channel} if marker.channel else {"T", "Q"}
            self._apply_gap(shard, channels, None if marker.contracts is None else set(marker.contracts),
                            marker.reason, marker.start_ms, marker.end_ms, marker.first_offset,
                            marker.last_offset, 0, out)
        elif control.kind == "RECONCILE":
            contract, session_date, volume, watermark, snapshot_time = control.payload
            outcome = shard.book.reconcile(contract, session_date, volume, watermark, snapshot_time)
            out.reconciles.append(ReconcileResult(
                contract, session_date, outcome.accepted, outcome.reason, outcome.cumulative,
                outcome.baseline.value, outcome.previous_cumulative))
        elif control.kind == "QUOTE_UNIVERSE":
            self._quote_universe = control.payload
        elif control.kind == "TIME":
            shard.clusters.observe_time(control.payload, out.clusters)

    def _apply_pending(self, shard: _Shard, pending: _PendingGap, out: EngineOutput) -> None:
        start = pending.start_ms if pending.start_ms is not None else max(shard.last_time_ms, 0)
        end = pending.end_ms if pending.end_ms is not None else max(shard.last_time_ms, start)
        channels = {"T", "Q"} if (not pending.channels or None in pending.channels) else set(pending.channels)
        self._apply_gap(shard, channels, set(pending.contracts), "BACKPRESSURE_BUFFER_FULL", start, end,
                        pending.first_offset, pending.last_offset, pending.count, out)

    def _apply_gap(self, shard: _Shard, channels: set[str], contracts: set[str] | None, reason: str,
                   start_ms: int, end_ms: int, first_offset: int | None, last_offset: int | None,
                   dropped: int, out: EngineOutput) -> None:
        self.stats.gaps_applied += 1
        session_date = self._clock.session_date(end_ms)
        if "T" in channels:
            if contracts is None:
                shard.book.mark_all_incomplete(session_date, reason, end_ms)
            else:
                for contract in sorted(contracts):
                    shard.book.mark_incomplete(contract, session_date, reason, end_ms)
        if "Q" in channels:
            if contracts is None:
                shard.quotes.invalidate()
            else:
                for contract in contracts:
                    shard.quotes.invalidate(contract)
        label = "T" if channels == {"T"} else "Q" if channels == {"Q"} else None
        out.gaps.append(GapRecord(
            shard_id=shard.index, channel=label, reason=reason, start_ms=start_ms, end_ms=end_ms,
            first_offset=first_offset, last_offset=last_offset, dropped_messages=dropped,
            contracts=() if contracts is None else tuple(sorted(contracts))))

    def _quote(self, shard: _Shard, message: QuoteMessage) -> None:
        self.stats.quotes += 1
        shard.quotes.update(message)

    def _trade(self, shard: _Shard, message: TradeMessage, out: EngineOutput) -> None:
        stats = self.stats
        stats.trades += 1
        time_ms = message.time_ms
        session_date = self._clock.session_date(time_ms)
        shard.last_session = session_date
        if time_ms > shard.last_time_ms:
            shard.last_time_ms = time_ms
        if (
            self._contract_scope is not None
            and session_date != self._contract_scope.session_date
        ):
            self._count_rejection("CONTRACT_SCOPE_SESSION_MISMATCH")
            return
        shard.clusters.observe_time(time_ms, out.clusters)
        verdict = self._semantics.classify(message.conditions)
        if verdict.treatment is Treatment.INCLUDED:
            self._eligible(shard, message, verdict, session_date, out)
        elif verdict.treatment is Treatment.CANCELED:
            self._cancel(shard, message, verdict, session_date, out)
        elif verdict.treatment is Treatment.EXCLUDED:
            stats.non_volume_prints += 1
        else:
            self._unknown(shard, message, verdict, session_date, out)
        interval = self.config.checkpoint_interval_ms
        if shard.next_checkpoint_ms is None:
            shard.next_checkpoint_ms = time_ms + interval
        elif time_ms >= shard.next_checkpoint_ms:
            shard.next_checkpoint_ms = time_ms + interval
            out.checkpoints.extend(shard.book.checkpoint_rows())

    def _passes_filter(self, price: float, size: int) -> bool:
        """Legacy single-threshold retention used only without Advanced V2 policy."""
        if self._min_size is not None and size >= self._min_size:
            return True
        notional = price * size * self._multiplier
        if notional >= self._threshold_high:
            return True
        if notional < self._threshold_low:
            return False
        return Decimal(repr(price)) * size * self._multiplier >= self._threshold

    def _retention_matches(
        self,
        symbol: str,
        price: float,
        size: int,
    ) -> tuple[FlowRetentionEvidence, ...]:
        policy = self._advanced_policy
        if policy is None:
            return ()
        entry = self._contract_scope_map.get(symbol)
        if entry is None:
            return ()
        premium = Decimal(repr(price)) * size * self._multiplier
        matches: list[FlowRetentionEvidence] = []
        for profile in policy.profiles:
            if entry.dte_lane not in profile.dte_lanes:
                continue
            threshold = profile.threshold_for(entry.universe_tier)
            contracts_matched = size >= threshold.minimum_contracts
            premium_matched = (
                premium >= threshold.minimum_print_premium
            )
            if not (contracts_matched or premium_matched):
                continue
            matches.append(FlowRetentionEvidence(
                profile=profile.kind.value,
                universe_tier=entry.universe_tier.value,
                dte_lane=entry.dte_lane.value,
                threshold_mode=profile.threshold_mode.value,
                minimum_contracts=threshold.minimum_contracts,
                minimum_print_premium=threshold.minimum_print_premium,
                contracts_matched=contracts_matched,
                premium_matched=premium_matched,
                minimum_cluster_prints=threshold.minimum_cluster_prints,
                minimum_distinct_exchanges=(
                    threshold.minimum_distinct_exchanges
                ),
                minimum_cluster_premium=(
                    threshold.minimum_cluster_premium
                ),
                minimum_cumulative_contracts=(
                    threshold.minimum_cumulative_contracts
                ),
                window_seconds=profile.window_for(entry.dte_lane),
            ))
        return tuple(matches)

    def _scope_entry(
        self,
        symbol: str,
    ) -> AdvancedOptionsContractScopeEntry | None:
        return self._contract_scope_map.get(symbol)

    def _underlyer(self, symbol: str) -> str:
        route = self._routes.get(symbol) or self._route(symbol)
        return route[0] if route else symbol

    def _quote_listed(self, symbol: str) -> bool:
        if self._developer:
            return False
        universe = self._quote_universe
        return universe is None or symbol in universe

    def _eligible(self, shard: _Shard, message: TradeMessage, verdict, session_date: date,
                  out: EngineOutput) -> None:
        stats = self.stats
        outcome = shard.book.add_print(message.symbol, session_date, message.seq, message.time_ms,
                                       message.exchange, message.price, message.size)
        status = outcome.status
        if status is PrintStatus.COUNTED:
            stats.counted_prints += 1
        elif status is PrintStatus.COVERED_BY_SNAPSHOT:
            stats.covered_prints += 1
        elif status is PrintStatus.DUPLICATE:
            stats.duplicates += 1
            return
        elif status is PrintStatus.CONFLICT:
            stats.duplicate_conflicts += 1
            return
        else:
            stats.beyond_history += 1
            return
        if outcome.late:
            stats.late_prints += 1
        if self._advanced_policy is not None:
            shard.clusters.update_cumulative(
                message.symbol,
                outcome.after,
                out.clusters,
            )
        matches = self._retention_matches(
            message.symbol,
            message.price,
            message.size,
        )
        retained = (
            bool(matches)
            if self._advanced_policy is not None
            else self._passes_filter(message.price, message.size)
        )
        if not retained:
            stats.below_filter += 1
            return
        event = self._build_event(shard, message, verdict, session_date, outcome.before, outcome.after,
                                  outcome.baseline, outcome.late, matches)
        outcome.record.event = event
        stats.persisted_events += 1
        out.events.append(event)
        bucket = cluster_bucket(event.side_class)
        if (
            bucket is not None
            and (
                bool(event.retention_thresholds)
                or event.premium >= self._threshold
            )
        ):
            shard.clusters.on_print(self._cluster_print(event, bucket), out.clusters)

    def _cluster_print(
        self,
        event: FlowEvent,
        bucket: str,
        *,
        cumulative_contracts: int | None = None,
    ) -> ClusterPrint:
        return ClusterPrint(event.event_id, event.contract_ticker, event.underlyer, bucket, event.time_ms,
                            event.source_seq, event.size, event.premium, event.exchange_id,
                            (
                                event.cumulative_volume_after
                                if cumulative_contracts is None
                                else cumulative_contracts
                            ),
                            event.retention_thresholds)

    def _event_id(
        self,
        contract_ticker: str,
        source_seq: int,
        time_ms: int,
        *,
        revision: int = 1,
    ) -> UUID:
        if self._source_flow is None:
            suffix = "" if revision == 1 else f"|r{revision}"
            identity = (
                f"{self.config.cohort_id}|{contract_ticker}|"
                f"{source_seq}|{time_ms}{suffix}"
            )
        else:
            identity = (
                f"{self._source_flow.source_flow_id}|{contract_ticker}|"
                f"{source_seq}|{time_ms}|r{revision}"
            )
        return uuid5(EVENT_NAMESPACE, identity)

    def _source_flow_values(
        self,
    ) -> tuple[UUID | None, str | None, str | None, int]:
        if self._source_flow is None:
            return None, None, None, 1
        return (
            self._source_flow.source_flow_id,
            self._source_flow.condition_semantics_sha256,
            self._source_flow.flow_policy_sha256,
            self._source_flow.identity_version,
        )

    def _build_event(
        self,
        shard: _Shard,
        message: TradeMessage,
        verdict,
        session_date: date,
        before: int,
        after: int,
        baseline: BaselineKind,
        late: bool,
        retention_thresholds: tuple[FlowRetentionEvidence, ...],
    ) -> FlowEvent:
        symbol = message.symbol
        listed = self._quote_listed(symbol)
        lookup = None
        if listed and not verdict.is_multi_leg:
            lookup = shard.quotes.lookup(symbol, message.time_ms)
        aggression = classify_aggression(message.price, lookup, verdict=verdict, quote_listed=listed,
                                         policy=self._aggression)
        price = Decimal(repr(message.price))
        premium = price * message.size * self._multiplier
        oi = assess_volume_vs_oi(self._oi.get(symbol), session_date, baseline, after)
        opening = infer_opening(oi, after, aggression.side_class, aggression.confidence, premium,
                                self._inference)
        quote = aggression.quote
        reasons = verdict.reasons + aggression.reasons + oi.reasons
        source_flow_id, condition_sha256, flow_sha256, lineage_version = (
            self._source_flow_values()
        )
        scope_entry = self._scope_entry(symbol)
        scope = self._contract_scope
        return FlowEvent(
            event_id=self._event_id(symbol, message.seq, message.time_ms),
            event_time=to_datetime(message.time_ms),
            cohort_id=self.config.cohort_id,
            session_date=session_date,
            provider_tier=self.config.provider_tier,
            contract_ticker=symbol,
            underlyer=self._underlyer(symbol),
            source_seq=message.seq,
            exchange_id=message.exchange,
            price=price,
            size=message.size,
            multiplier=self._multiplier,
            premium=premium,
            conditions=message.conditions,
            condition_semantics_version=self._semantics.version,
            trade_treatment=verdict.treatment.value,
            is_multi_leg=verdict.is_multi_leg,
            side_class=aggression.side_class.value,
            side_confidence=aggression.confidence.value,
            quote_bid=None if quote is None else Decimal(repr(quote.bid)),
            quote_ask=None if quote is None else Decimal(repr(quote.ask)),
            quote_bid_size=None if quote is None else quote.bid_size,
            quote_ask_size=None if quote is None else quote.ask_size,
            quote_time=None if quote is None else to_datetime(quote.time_ms),
            quote_seq=None if quote is None else quote.seq,
            cumulative_volume_before=before,
            cumulative_volume_after=after,
            volume_baseline_kind=baseline.value,
            prior_open_interest=oi.prior_open_interest,
            open_interest_as_of=oi.open_interest_as_of,
            volume_exceeds_prior_oi=oi.volume_exceeds_prior_oi,
            opening_inference=opening.label.value,
            spread_position=aggression.position,
            opening_score=None if opening.score is None else _score(opening.score),
            opening_reasons=opening.reasons,
            journal_generation=self.config.journal_generation,
            shard_id=shard.index,
            source_offset=message.offset if message.offset and message.offset > 0 else None,
            source_flow_id=source_flow_id,
            condition_semantics_sha256=condition_sha256,
            flow_policy_sha256=flow_sha256,
            lineage_version=lineage_version,
            revision=1,
            supersedes_event_id=None,
            time_ms=message.time_ms,
            reasons=reasons,
            is_cross=verdict.is_cross,
            is_sweep=verdict.is_sweep,
            is_floor=verdict.is_floor,
            is_late=late or verdict.is_late,
            matched_profiles=tuple(
                row.profile for row in retention_thresholds
            ),
            flow_universe_tier=(
                None
                if scope_entry is None
                else scope_entry.universe_tier.value
            ),
            flow_dte_lane=(
                None if scope_entry is None else scope_entry.dte_lane.value
            ),
            contract_expiration=(
                None if scope_entry is None else scope_entry.expiration
            ),
            retention_thresholds=retention_thresholds,
            advanced_flow_policy_sha256=self._advanced_flow_policy_sha256,
            contract_scope_sha256=self._contract_scope_sha256,
            manifest_sha256=(
                None if scope is None else scope.manifest_sha256
            ),
            slot_sha256=None if scope is None else scope.slot_sha256,
            runtime_policy_sha256=self._policy_sha256,
            provider_contract_sha256=self._provider_contract_sha256,
            condition_reference_sha256=self._condition_reference_sha256,
            exchange_reference_sha256=self._exchange_reference_sha256,
        )

    def _cancel(self, shard: _Shard, message: TradeMessage, verdict, session_date: date,
                out: EngineOutput) -> None:
        stats = self.stats
        stats.cancels += 1
        outcome = shard.book.cancel_print(message.symbol, session_date, message.seq, message.time_ms,
                                          message.exchange, message.price, message.size)
        if outcome.duplicate:
            stats.duplicates += 1
            return
        original = outcome.matched
        if original is None:
            stats.cancels_unmatched += 1
            return
        previous = original.event
        if previous is None:
            if self._advanced_policy is not None:
                shard.clusters.update_cumulative(
                    message.symbol,
                    outcome.after,
                    out.clusters,
                )
            return
        revision_number = previous.revision + 1
        revision = FlowEvent(
            event_id=self._event_id(
                previous.contract_ticker,
                previous.source_seq,
                previous.time_ms,
                revision=revision_number,
            ),
            event_time=previous.event_time,
            cohort_id=self.config.cohort_id,
            session_date=previous.session_date,
            provider_tier=self.config.provider_tier,
            contract_ticker=previous.contract_ticker,
            underlyer=previous.underlyer,
            source_seq=previous.source_seq,
            exchange_id=previous.exchange_id,
            price=previous.price,
            size=previous.size,
            multiplier=previous.multiplier,
            premium=previous.premium,
            conditions=message.conditions,
            condition_semantics_version=self._semantics.version,
            trade_treatment=Treatment.CANCELED.value,
            is_multi_leg=previous.is_multi_leg,
            side_class=SideClass.NOT_CLASSIFIED.value,
            side_confidence=SideConfidence.UNKNOWN.value,
            quote_bid=None, quote_ask=None, quote_bid_size=None, quote_ask_size=None,
            quote_time=None, quote_seq=None,
            # No volume movement is claimed on a revision row; the checkpoint carries the cumulative.
            cumulative_volume_before=None,
            cumulative_volume_after=None,
            volume_baseline_kind=outcome.baseline.value,
            prior_open_interest=None,
            open_interest_as_of=None,
            volume_exceeds_prior_oi=False,
            opening_inference=OpeningLabel.NOT_ASSESSED.value,
            spread_position=None,
            opening_score=None,
            opening_reasons=(),
            journal_generation=self.config.journal_generation,
            shard_id=shard.index,
            source_offset=message.offset if message.offset and message.offset > 0 else None,
            source_flow_id=previous.source_flow_id,
            condition_semantics_sha256=previous.condition_semantics_sha256,
            flow_policy_sha256=previous.flow_policy_sha256,
            lineage_version=previous.lineage_version,
            revision=revision_number,
            supersedes_event_id=previous.event_id,
            time_ms=previous.time_ms,
            reasons=verdict.reasons + ("SUPERSEDES_PERSISTED_PRINT",),
            is_cross=False, is_sweep=False, is_floor=False, is_late=False,
            matched_profiles=previous.matched_profiles,
            flow_universe_tier=previous.flow_universe_tier,
            flow_dte_lane=previous.flow_dte_lane,
            contract_expiration=previous.contract_expiration,
            retention_thresholds=previous.retention_thresholds,
            advanced_flow_policy_sha256=(
                previous.advanced_flow_policy_sha256
            ),
            contract_scope_sha256=previous.contract_scope_sha256,
            manifest_sha256=previous.manifest_sha256,
            slot_sha256=previous.slot_sha256,
            runtime_policy_sha256=previous.runtime_policy_sha256,
            provider_contract_sha256=(
                previous.provider_contract_sha256
            ),
            condition_reference_sha256=(
                previous.condition_reference_sha256
            ),
            exchange_reference_sha256=(
                previous.exchange_reference_sha256
            ),
        )
        stats.cancel_revisions += 1
        out.events.append(revision)
        bucket = cluster_bucket(previous.side_class)
        if bucket is not None:
            shard.clusters.on_cancel(
                self._cluster_print(
                    previous,
                    bucket,
                    cumulative_contracts=outcome.after,
                ),
                out.clusters,
            )

    def _unknown(self, shard: _Shard, message: TradeMessage, verdict, session_date: date,
                 out: EngineOutput) -> None:
        stats = self.stats
        stats.unknown_condition_prints += 1
        for code in message.conditions:
            if code not in self._semantics.reviewed:
                stats.unknown_condition_codes[code] = stats.unknown_condition_codes.get(code, 0) + 1
        shard.book.mark_incomplete(message.symbol, session_date, "UNKNOWN_TRADE_CONDITION", message.time_ms)
        matches = self._retention_matches(
            message.symbol,
            message.price,
            message.size,
        )
        retained = (
            bool(matches)
            if self._advanced_policy is not None
            else self._passes_filter(message.price, message.size)
        )
        if not self.config.persist_unknown_condition_prints or not retained:
            return
        cv = shard.book.get(message.symbol, session_date)
        volume = cv.cumulative if cv is not None else 0
        price = Decimal(repr(message.price))
        source_flow_id, condition_sha256, flow_sha256, lineage_version = (
            self._source_flow_values()
        )
        scope_entry = self._scope_entry(message.symbol)
        scope = self._contract_scope
        event = FlowEvent(
            event_id=self._event_id(
                message.symbol,
                message.seq,
                message.time_ms,
            ),
            event_time=to_datetime(message.time_ms),
            cohort_id=self.config.cohort_id,
            session_date=session_date,
            provider_tier=self.config.provider_tier,
            contract_ticker=message.symbol,
            underlyer=self._underlyer(message.symbol),
            source_seq=message.seq,
            exchange_id=message.exchange,
            price=price,
            size=message.size,
            multiplier=self._multiplier,
            premium=price * message.size * self._multiplier,
            conditions=message.conditions,
            condition_semantics_version=self._semantics.version,
            trade_treatment=Treatment.UNKNOWN.value,
            is_multi_leg=False,
            side_class=SideClass.NOT_CLASSIFIED.value,
            side_confidence=SideConfidence.UNKNOWN.value,
            quote_bid=None, quote_ask=None, quote_bid_size=None, quote_ask_size=None,
            quote_time=None, quote_seq=None,
            cumulative_volume_before=volume,
            cumulative_volume_after=volume,
            volume_baseline_kind=BaselineKind.INCOMPLETE.value,
            prior_open_interest=None,
            open_interest_as_of=None,
            volume_exceeds_prior_oi=False,
            opening_inference=OpeningLabel.NOT_ASSESSED.value,
            spread_position=None,
            opening_score=None,
            opening_reasons=(),
            journal_generation=self.config.journal_generation,
            shard_id=shard.index,
            source_offset=message.offset if message.offset and message.offset > 0 else None,
            source_flow_id=source_flow_id,
            condition_semantics_sha256=condition_sha256,
            flow_policy_sha256=flow_sha256,
            lineage_version=lineage_version,
            revision=1,
            supersedes_event_id=None,
            time_ms=message.time_ms,
            reasons=verdict.reasons,
            is_cross=False, is_sweep=False, is_floor=False, is_late=False,
            matched_profiles=tuple(row.profile for row in matches),
            flow_universe_tier=(
                None
                if scope_entry is None
                else scope_entry.universe_tier.value
            ),
            flow_dte_lane=(
                None if scope_entry is None else scope_entry.dte_lane.value
            ),
            contract_expiration=(
                None if scope_entry is None else scope_entry.expiration
            ),
            retention_thresholds=matches,
            advanced_flow_policy_sha256=self._advanced_flow_policy_sha256,
            contract_scope_sha256=self._contract_scope_sha256,
            manifest_sha256=(
                None if scope is None else scope.manifest_sha256
            ),
            slot_sha256=None if scope is None else scope.slot_sha256,
            runtime_policy_sha256=self._policy_sha256,
            provider_contract_sha256=self._provider_contract_sha256,
            condition_reference_sha256=self._condition_reference_sha256,
            exchange_reference_sha256=self._exchange_reference_sha256,
        )
        stats.persisted_events += 1
        out.events.append(event)

    def state_digest(self) -> str:
        """Content hash of the accumulators and cluster state; equal after equal replays."""
        return canonical_sha256([
            [shard.book.digest(), shard.clusters.digest_state()] for shard in self._shards
        ])
