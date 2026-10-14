"""Bounded journal-to-engine consumer for the OPTIONS_FLOW stage.

The consumer reads batches from an injected source (the RawJournalReader protocol: `read_after(offset,
limit=...)` returning records with `raw_offset`, `generation`, `stream_session_id`, `message_number` and the
raw frame `payload`), feeds every provider message to `OptionFlowEngine.submit(raw, offset)`, processes the
queues and hands the output to the writer, which flushes on its own rules (1,000 rows or one second).

It never blocks the source: it only reads. A resident tick runs one bounded slice (maximum messages and
seconds, polled between batches together with cancellation) and returns; what it has not read stays in
the journal. Overload is an explicit gap, never an unbounded queue: a message the engine cannot queue is
recorded as a gap by the engine, and the consumer turns the source's own discontinuities (a new gateway
session, lost frames, an offset hole, an undecodable frame) into gap markers in stream order. Every gap
is handed to the gap sink BEFORE the rows of its batch can be flushed, so a crash can lose no gap: on
replay the same gap is recorded again under the same identity.

Rows and the engine checkpoint commit together: the writer's flush transaction calls `_on_flush`, which
writes the checkpoint for the offset the flushed rows cover. Persisted rows therefore never extend past
the stored checkpoint, and a restart replays only rows that were never committed.
"""
from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol
from uuid import UUID

from .checkpoint_state import CheckpointStore, RestoredCheckpoint, baseline_counts
from .engine import GapMarker, OptionFlowEngine, SubmitResult
from .records import EngineOutput, GapRecord, ReconcileResult
from .volume import COMPLETE_BASELINES, BaselineKind
from .writer import FlushReport, OptionFlowWriter

GAP_SESSION_CHANGED = "GATEWAY_SESSION_CHANGED"
GAP_OFFSET_HOLE = "SOURCE_OFFSET_HOLE"
GAP_MESSAGE_HOLE = "SOURCE_MESSAGE_HOLE"
GAP_UNREADABLE_FRAME = "UNREADABLE_FRAME"
GAP_BACKPRESSURE = "BACKPRESSURE_BUFFER_FULL"
_MARKET_EVENTS = ("T", "Q")


class SourceRecord(Protocol):
    raw_offset: int
    generation: UUID
    stream_session_id: UUID
    message_number: int
    payload: bytes


class OptionFlowSource(Protocol):
    """What the consumer needs from a journal. `last_offset` is optional: without it the source tail is
    unknown, so a checkpoint cannot be checked against it and the backlog is not reported."""

    generation: UUID

    def read_after(self, raw_offset: int, *, limit: int = 1000) -> Sequence[SourceRecord]: ...


class OptionFlowSourceError(RuntimeError):
    """The source broke its contract (not monotonic, wrong generation) or could not be read."""

    def __init__(self, code: str, *, error_type: str | None = None) -> None:
        super().__init__(code if error_type is None else f"{code}:{error_type}")
        self.code = code
        self.error_type = error_type


@dataclass(frozen=True, slots=True)
class ConsumerLimits:
    """One resident tick's bound. The V2 service ticks OPTIONS_FLOW every 15 s by default, so a slice must be
    able to carry real traffic: what it does not read stays in the journal for the next tick."""

    batch_size: int = 500
    max_messages: int = 50_000
    max_seconds: float = 5.0

    def __post_init__(self) -> None:
        if self.batch_size < 1 or self.max_messages < 1 or self.max_seconds <= 0:
            raise ValueError("batch size, maximum messages and maximum seconds must be positive")


@dataclass(frozen=True, slots=True)
class GapEntry:
    """One engine gap with the stream identity rt.stream_gaps needs (the engine knows none)."""

    stream_session_id: UUID
    journal_generation: UUID
    shard_id: int
    channel: str | None
    reason: str
    start_ms: int
    end_ms: int
    first_offset: int | None
    last_offset: int | None
    dropped_messages: int
    contracts: tuple[str, ...]

    @property
    def last_durable_offset(self) -> int:
        first = self.first_offset if self.first_offset is not None else self.last_offset
        return max((first or 1) - 1, 0)


GapSink = Callable[[Sequence[GapEntry]], None]


@dataclass(slots=True)
class SliceReport:
    records: int = 0
    messages: int = 0
    control_frames: int = 0
    accepted: int = 0
    gap_recorded: int = 0
    rejected: int = 0
    markers: int = 0
    events: int = 0
    clusters: int = 0
    gap_entries: int = 0
    reconciles: list[ReconcileResult] = field(default_factory=list)
    stopped: str = "NOT_RUN"


@dataclass(slots=True)
class _Delivery:
    out: EngineOutput
    entries: list[GapEntry]
    gaps_done: bool = False


def decode_frame(payload: bytes) -> list[Mapping[str, Any]] | None:
    """The provider messages in one journaled frame, or None when it cannot be read."""
    try:
        decoded = json.loads(payload)
    except (ValueError, UnicodeDecodeError):
        return None
    rows = decoded if isinstance(decoded, list) else [decoded]
    if not all(isinstance(row, Mapping) for row in rows):
        return None
    return rows


def _is_control(message: Mapping[str, Any]) -> bool:
    return str(message.get("ev") or "").lower() == "status"


class OptionFlowConsumer:
    def __init__(
        self,
        engine: OptionFlowEngine,
        source: OptionFlowSource,
        writer_factory: Callable[[Callable[[Any, FlushReport], None]], OptionFlowWriter],
        *,
        generation: UUID,
        checkpoints: CheckpointStore | None = None,
        gap_sink: GapSink | None = None,
        limits: ConsumerLimits = ConsumerLimits(),
        clock: Callable[[], float] = time.monotonic,
        start_offset: int = 0,
        restored: RestoredCheckpoint | None = None,
        warming_reason: str | None = None,
        checkpoint_interval_seconds: float = 0.0,
    ) -> None:
        if start_offset < 0 or checkpoint_interval_seconds < 0:
            raise ValueError("the start offset and the checkpoint interval must be nonnegative")
        self._engine = engine
        self._source = source
        self._generation = generation
        self._checkpoints = checkpoints
        self._gap_sink = gap_sink
        self._limits = limits
        self._clock = clock
        self._checkpoint_interval = checkpoint_interval_seconds
        self._last_checkpoint_at: float | None = None
        self._force_full_checkpoint = False
        self._fed_offset = restored.offset if restored is not None else start_offset
        # Rows are durable through this offset; messages up to it that are fed again (a restore from an
        # older full checkpoint) rebuild state only and never move the stored offsets backwards.
        self._durable_floor = restored.durable_offset if restored is not None else self._fed_offset
        self._flushed_offset = self._durable_floor
        self._checkpointed_offset = self._fed_offset if restored is not None else 0
        self._session_id: UUID | None = None
        self._next_message_number: int | None = None
        self._last_event_ms: int | None = None
        self._gaps_seen = 0
        self.warming_reason = warming_reason
        if restored is not None:
            extras = restored.extras
            session = extras.get("session_id")
            self._session_id = None if session is None else UUID(session)
            self._next_message_number = extras.get("next_message_number")
            self._last_event_ms = extras.get("last_event_ms")
            self._gaps_seen = int(extras.get("gaps_seen", 0))
            self.warming_reason = extras.get("warming_reason") if warming_reason is None else warming_reason
        self._markers: dict[tuple, UUID] = {}
        self._delivery: _Delivery | None = None
        self._needs_flush_retry = False
        self._flush_marker: tuple[int, bool, float] | None = None
        self.messages_total = 0
        self.slices = 0
        self._writer = writer_factory(self._on_flush)

    # --- state ------------------------------------------------------------------------------------

    @property
    def engine(self) -> OptionFlowEngine:
        return self._engine

    @property
    def writer(self) -> OptionFlowWriter:
        return self._writer

    @property
    def fed_offset(self) -> int:
        """The last journal offset fully handed to the engine."""
        return self._fed_offset

    @property
    def flushed_offset(self) -> int:
        """The offset the last successful flush covers (its rows are durable)."""
        return self._flushed_offset

    @property
    def checkpointed_offset(self) -> int:
        return self._checkpointed_offset

    @property
    def gaps_seen(self) -> int:
        return self._gaps_seen

    @property
    def last_event_ms(self) -> int | None:
        return self._last_event_ms

    @property
    def unflushed_rows(self) -> int:
        return self._writer.pending_rows

    def extras(self) -> dict[str, Any]:
        """Consumer state stored with the checkpoint so continuity checks survive a restart."""
        return {
            "session_id": None if self._session_id is None else str(self._session_id),
            "next_message_number": self._next_message_number,
            "last_event_ms": self._last_event_ms,
            "gaps_seen": self._gaps_seen,
            "warming_reason": self.warming_reason,
        }

    def source_tail(self) -> int | None:
        reader = getattr(self._source, "last_offset", None)
        return None if reader is None else int(reader())

    def backlog(self) -> int | None:
        tail = self.source_tail()
        return None if tail is None else max(0, tail - self._fed_offset)

    # --- the slice --------------------------------------------------------------------------------

    def run_slice(self, *, is_cancelled: Callable[[], bool] = lambda: False) -> SliceReport:
        """Read, feed and process until caught up or a bound is hit. Cancellation is polled between batches."""
        report = SliceReport()
        started = self._clock()
        self.slices += 1
        self._retry_pending()
        limits = self._limits
        while True:
            if is_cancelled():
                report.stopped = "CANCELLED"
                break
            if report.messages >= limits.max_messages:
                report.stopped = "MAX_MESSAGES"
                break
            if self._clock() - started >= limits.max_seconds:
                report.stopped = "MAX_SECONDS"
                break
            take = max(1, min(limits.batch_size, limits.max_messages - report.messages))
            try:
                records = self._source.read_after(self._fed_offset, limit=take)
            except Exception as error:  # noqa: BLE001 - only the type is kept
                raise OptionFlowSourceError("SOURCE_READ_FAILED", error_type=type(error).__name__) from None
            if not records:
                report.stopped = "CAUGHT_UP"
                break
            self._process_batch(records, report)
            self._poll()  # the writer's one-second rule is honoured between batches, not only at the end
        self._poll()
        return report

    def _retry_pending(self) -> None:
        if self._delivery is not None:
            self._complete_delivery()
        if self._needs_flush_retry:
            self._writer_call(self._writer.flush, reason="RETRY")

    def _poll(self) -> None:
        if self._delivery is None:
            self._writer_call(self._writer.poll)

    def _process_batch(self, records: Sequence[SourceRecord], report: SliceReport) -> None:
        engine = self._engine
        batch_sessions: dict[int, UUID] = {}
        for record in records:
            offset = record.raw_offset
            if type(offset) is not int or offset <= self._fed_offset:
                raise OptionFlowSourceError("SOURCE_OFFSET_NOT_MONOTONIC")
            if record.generation != self._generation:
                raise OptionFlowSourceError("SOURCE_GENERATION_CHANGED")
            batch_sessions[offset] = record.stream_session_id
            messages = decode_frame(record.payload)
            market = [] if messages is None else [item for item in messages if not _is_control(item)]
            first_time = self._first_time(market)
            self._check_continuity(record, first_time, report)
            if messages is None:
                self._submit_marker(GAP_UNREADABLE_FRAME, record, first_time, offset, offset, report)
            report.control_frames += (0 if messages is None else len(messages) - len(market))
            for message in market:
                report.messages += 1
                result = engine.submit(message, offset)
                if result is SubmitResult.ACCEPTED:
                    report.accepted += 1
                elif result is SubmitResult.GAP_RECORDED:
                    report.gap_recorded += 1
                else:
                    report.rejected += 1
                time_ms = message.get("t")
                if type(time_ms) is int and message.get("ev") in _MARKET_EVENTS and (
                        self._last_event_ms is None or time_ms > self._last_event_ms):
                    self._last_event_ms = time_ms
            self._session_id = record.stream_session_id
            self._next_message_number = record.message_number + 1
            self._fed_offset = offset
            report.records += 1
        self.messages_total += report.messages
        out = engine.process()
        report.events += len(out.events)
        report.clusters += len(out.clusters)
        report.reconciles.extend(out.reconciles)
        self._deliver(out, batch_sessions, report)

    @staticmethod
    def _first_time(messages: Sequence[Mapping[str, Any]]) -> int | None:
        times = [item["t"] for item in messages
                 if item.get("ev") in _MARKET_EVENTS and type(item.get("t")) is int and item["t"] >= 0]
        return min(times) if times else None

    def _check_continuity(self, record: SourceRecord, first_time: int | None, report: SliceReport) -> None:
        if self._session_id is None:
            return
        offset = record.raw_offset
        if offset > self._fed_offset + 1:
            self._submit_marker(GAP_OFFSET_HOLE, record, first_time, self._fed_offset + 1, offset - 1, report)
        if record.stream_session_id != self._session_id:
            self._submit_marker(GAP_SESSION_CHANGED, record, first_time, offset, offset, report)
        elif record.message_number > (self._next_message_number or 0):
            self._submit_marker(GAP_MESSAGE_HOLE, record, first_time, offset, offset, report)
        elif record.message_number < (self._next_message_number or 0):
            raise OptionFlowSourceError("SOURCE_MESSAGE_NUMBER_REGRESSED")

    def _submit_marker(self, reason: str, record: SourceRecord, first_time: int | None, first_offset: int,
                       last_offset: int, report: SliceReport) -> None:
        start = self._last_event_ms if self._last_event_ms is not None else (
            first_time if first_time is not None else self._received_ms(record))
        end = max(start, first_time) if first_time is not None else start
        self._engine.submit_gap(GapMarker(reason, start, end, None, None, first_offset, last_offset))
        self._markers[(reason, start, end, first_offset, last_offset)] = record.stream_session_id
        self._gaps_seen += 1
        report.markers += 1

    @staticmethod
    def _received_ms(record: SourceRecord) -> int:
        """When no market time was ever seen, date the gap by the journal's receive time (0 if the record has none)."""
        received = getattr(record, "received_at", None)
        return max(0, int(received.timestamp() * 1000)) if received is not None else 0

    # --- delivery: gaps first, then rows ------------------------------------------------------------

    def _deliver(self, out: EngineOutput, batch_sessions: Mapping[int, UUID], report: SliceReport) -> None:
        entries = [self._gap_entry(record, batch_sessions) for record in out.gaps]
        self._gaps_seen += sum(1 for record in out.gaps if record.reason == GAP_BACKPRESSURE)
        report.gap_entries += len(entries)
        self._markers.clear()
        self._delivery = _Delivery(out, entries)
        self._complete_delivery()

    def _gap_entry(self, record: GapRecord, batch_sessions: Mapping[int, UUID]) -> GapEntry:
        key = (record.reason, record.start_ms, record.end_ms, record.first_offset, record.last_offset)
        session = self._markers.get(key)
        if session is None and record.last_offset is not None:
            session = batch_sessions.get(record.last_offset)
        if session is None:
            session = self._session_id
        if session is None:
            raise OptionFlowSourceError("GAP_WITHOUT_STREAM_SESSION")
        return GapEntry(session, self._generation, record.shard_id or 0, record.channel, record.reason,
                        record.start_ms or 0, record.end_ms or 0, record.first_offset, record.last_offset,
                        record.dropped_messages, record.contracts)

    def _complete_delivery(self) -> None:
        delivery = self._delivery
        if delivery is None:
            return
        if delivery.entries and not delivery.gaps_done and self._gap_sink is not None:
            self._gap_sink(delivery.entries)
        delivery.gaps_done = True
        self._delivery = None
        self._writer_call(self._writer.add, delivery.out, journal_generation=self._generation)

    def _on_flush(self, cursor: Any, report: FlushReport) -> None:
        covered = self._fed_offset
        store = self._checkpoints
        now = self._clock()
        if store is None or covered < self._durable_floor:
            self._flush_marker = (covered, False, now)
            return
        due = (self._force_full_checkpoint or self._last_checkpoint_at is None
               or now - self._last_checkpoint_at >= self._checkpoint_interval)
        wrote = False
        if due or not store.advance(cursor, offset=covered, shard_count=self._engine.config.shard_count):
            store.write(cursor, self._engine, offset=covered, extras=self.extras())
            wrote = True
        self._flush_marker = (covered, wrote, now)

    def _writer_call(self, function: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        self._flush_marker = None
        try:
            result = function(*args, **kwargs)
        except BaseException:
            self._flush_marker = None
            self._needs_flush_retry = True
            raise
        marker, self._flush_marker = self._flush_marker, None
        if marker is not None:
            covered, wrote, stamp = marker
            self._flushed_offset = max(self._flushed_offset, covered)
            if wrote:
                self._checkpointed_offset = covered
                self._last_checkpoint_at = stamp
                self._force_full_checkpoint = False
            self._needs_flush_retry = False
        return result

    # --- control ------------------------------------------------------------------------------------

    def flush(self, *, reason: str = "FORCED", full: bool = False) -> FlushReport:
        """Force the writer to flush now (rows and the checkpoint offset that covers them). With full, the
        engine state is checkpointed too regardless of the interval."""
        self._retry_pending()
        self._force_full_checkpoint = self._force_full_checkpoint or full
        return self._writer_call(self._writer.flush, reason=reason)

    def finish(self, *, final: bool = True) -> None:
        """End of session: finalize every cluster (with final), hand over the rest and flush with a full
        checkpoint so the next session restores exactly this state."""
        self._retry_pending()
        out = self._engine.flush(final=final)
        report = SliceReport()
        self._deliver(out, {}, report)
        self._force_full_checkpoint = True
        self._writer_call(self._writer.flush, reason="CLOSE")

    def reconcile(self, contract: str, snapshot_volume: int, watermark_seq: int,
                  snapshot_time_ms: int) -> ReconcileResult:
        """Apply a REST snapshot volume at its watermark, in stream order after everything fed so far."""
        self._retry_pending()
        self._engine.submit_reconcile(contract, snapshot_volume, watermark_seq, snapshot_time_ms)
        out = self._engine.process()
        self._deliver(out, {}, SliceReport())
        if not out.reconciles:
            raise RuntimeError("the engine returned no reconcile result")
        return out.reconciles[0]

    def mark_reconciled(self) -> bool:
        """Clear the WARMING/gap report once no contract is WARMING or INCOMPLETE. False: still unreconciled."""
        counts = baseline_counts(self._engine)
        if any(BaselineKind(kind) not in COMPLETE_BASELINES for kind in counts):
            return False
        self.warming_reason = None
        self._gaps_seen = 0
        return True

    def reset_report(self, *, warming_reason: str | None) -> None:
        """A new session starts a new report: yesterday's gaps and warm-up say nothing about today's books."""
        self.warming_reason = warming_reason
        self._gaps_seen = 0
