"""OPTIONS_FLOW runner: the resident stage that turns the journaled option tape into flow rows.

A tick restores (or starts) the engine, runs one bounded consumer slice and reports through readiness.
The source is injected through the RunnerEnvironment and the runner never builds a gateway, websocket
or HTTP client: with no source it is BLOCKED_POLICY (OPTIONS_FLOW_SOURCE_NOT_CONFIGURED) and reads
nothing, and with only the synthetic conditions fixture it is BLOCKED_POLICY
(OPTIONS_FLOW_CONDITIONS_UNPINNED) before any read.

Database access (stage transactions as the `app` role; resident stages have no stage
run, so there is no write fence): reads and writes of the engine output and of the engine checkpoint go
through one transaction per writer flush (events, clusters, volume checkpoints and the checkpoint commit
together); stream gaps are written in their own transaction. Gaps are written before the flush that makes
the state they affect durable.

Start: the latest checkpoint is restored when every check passes (policy, version, configuration, shard
map, hash, state digest, offset not ahead of the source); otherwise the engine starts WARMING from the
checkpointed position (or the source tail when the checkpoint is ahead of the source) and says why. The
open interest map comes from an injected loader, by default ref.oi_baselines for the session; a
contract without a baseline takes the engine's existing no-prior-open-interest path.

Readiness: READY when current; DEGRADED with OPTIONS_FLOW_WARMING:<why>, OPTIONS_FLOW_GAP:<n> and
OPTIONS_FLOW_BACKLOG:<n>; BLOCKED for REPLAY_DIVERGENCE (fail closed, see OptionsFlowState), for a
source or conditions that are not configured, and for failures the next tick may retry.

Divergence policy (decided: fail closed). When the writer reports that a replayed key differs from its
stored row, the runner stops consuming, discards the in-memory engine and writer for the session,
quarantines the uncommitted offset range with the divergent key identifiers (never a payload) and
returns BLOCKED REPLAY_DIVERGENCE. It does not resume by itself; an operator calls
`OptionsFlowState.reset_divergence()` (and a restart of the session restores from the last checkpoint).

Cadence: V2Service ticks a resident stage every `resident_intervals[stage]` seconds after the previous tick
returns (default 0.25 s for OPTIONS_FLOW). A slice carries up to ConsumerLimits.max_messages /
max_seconds of work (50,000 messages or 5 s by default); what it does not read waits in the journal.
The measured consumer rate is in test_option_flow_consumer (`sustained message rate`).
"""
from __future__ import annotations

import hashlib
import logging
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any
from uuid import UUID, uuid5

from options.config import FlowStrategyPolicy
from psycopg2 import Error as PsycopgError
from .option_flow.checkpoint_state import (
    DEFAULT_MAX_STATE_BYTES,
    CheckpointRegression,
    CheckpointRejected,
    CheckpointStore,
    CheckpointTooLarge,
    RestoredCheckpoint,
    prune_sessions,
)
from .option_flow.clock import to_datetime
from .option_flow.consumer import (
    ConsumerLimits,
    GapEntry,
    OptionFlowConsumer,
    OptionFlowSourceError,
    SliceReport,
)
from .option_flow.engine import EngineConfig, OptionFlowEngine
from .option_flow.labels import PriorOpenInterest
from .option_flow.volume import BaselineKind, VolumeConfig
from .option_flow.writer import (
    FLUSH_INTERVAL_SECONDS,
    FLUSH_ROWS,
    OptionFlowReplayDivergence,
    OptionFlowSchemaViolation,
    OptionFlowWriteError,
    OptionFlowWriter,
)
from .runner_environment import RunnerEnvironment
from .stage_orchestrator import StageCancelled, StageContext, StageOutcome, StageRunner
from .stage_runners_equity import STAGE_CANCELLED

logger = logging.getLogger(__name__)

OPTIONS_FLOW_SOURCE_NOT_CONFIGURED = "OPTIONS_FLOW_SOURCE_NOT_CONFIGURED"
OPTIONS_FLOW_CONDITIONS_UNPINNED = "OPTIONS_FLOW_CONDITIONS_UNPINNED"
OPTIONS_FLOW_POLICY_NOT_CONFIGURED = "OPTIONS_FLOW_POLICY_NOT_CONFIGURED"
OPTIONS_FLOW_DATABASE_NOT_CONFIGURED = "OPTIONS_FLOW_DATABASE_NOT_CONFIGURED"
REPLAY_DIVERGENCE = "REPLAY_DIVERGENCE"
OPTIONS_FLOW_SCHEMA_VIOLATION = "OPTIONS_FLOW_SCHEMA_VIOLATION"
OPTIONS_FLOW_WRITE_FAILED = "OPTIONS_FLOW_WRITE_FAILED"
OPTIONS_FLOW_GAP_PERSIST_FAILED = "OPTIONS_FLOW_GAP_PERSIST_FAILED"
OPTIONS_FLOW_CHECKPOINT_TOO_LARGE = "OPTIONS_FLOW_CHECKPOINT_TOO_LARGE"
OPTIONS_FLOW_CHECKPOINT_REGRESSION = "OPTIONS_FLOW_CHECKPOINT_REGRESSION"
OPTIONS_FLOW_SOURCE_FAILED = "OPTIONS_FLOW_SOURCE_FAILED"
OPTIONS_FLOW_OPEN_INTEREST_FAILED = "OPTIONS_FLOW_OPEN_INTEREST_FAILED"
OPTIONS_FLOW_TICK_BUSY = "OPTIONS_FLOW_TICK_BUSY"
OPTIONS_ACQUIRE_NOT_CONFIGURED = "OPTIONS_ACQUIRE_NOT_CONFIGURED"
OPTIONS_ACQUIRE_INCOMPLETE = "OPTIONS_ACQUIRE_INCOMPLETE"
OPTIONS_ADMISSION_NOT_CONFIGURED = "OPTIONS_ADMISSION_NOT_CONFIGURED"
OPTIONS_ADMISSION_INCOMPLETE = "OPTIONS_ADMISSION_INCOMPLETE"
WARMING_NO_CHECKPOINT = "NO_CHECKPOINT"
WARMING_NEW_SESSION = "NEW_SESSION"
_NAMESPACE = UUID("4a8e2b1c-7d03-4f95-a6e8-2c91d0b5f378")


class OpenInterestLoadError(RuntimeError):
    """The open interest map could not be loaded completely; nothing is guessed."""


class GapPersistenceError(RuntimeError):
    """A gap could not be written to rt.stream_gaps (for example its stream session is not registered).
    The batch stays undelivered and is retried on the next tick; no row of it is flushed meanwhile."""


@dataclass(frozen=True, slots=True)
class OptionsAcquireResult:
    run_id: UUID
    expected_underlyings: tuple[str, ...]
    completed_underlyings: tuple[str, ...]
    provider_requests: int
    response_bytes: int

    def __post_init__(self) -> None:
        expected = tuple(sorted(self.expected_underlyings))
        completed = tuple(sorted(self.completed_underlyings))
        if (not expected or len(expected) != len(set(expected))
                or any(not symbol or symbol != symbol.upper() for symbol in expected)
                or len(completed) != len(set(completed))
                or any(symbol not in expected for symbol in completed)
                or type(self.provider_requests) is not int or self.provider_requests < 0
                or type(self.response_bytes) is not int or self.response_bytes < 0):
            raise ValueError("Options acquire result is invalid")
        object.__setattr__(self, "expected_underlyings", expected)
        object.__setattr__(self, "completed_underlyings", completed)

    @property
    def complete(self) -> bool:
        return self.completed_underlyings == self.expected_underlyings

    @property
    def evidence_ref(self) -> str:
        return (
            f"options-acquire:{self.run_id}:"
            f"{len(self.completed_underlyings)}/{len(self.expected_underlyings)}:"
            f"requests={self.provider_requests}:bytes={self.response_bytes}"
        )


@dataclass(frozen=True, slots=True)
class OptionsAdmissionResult:
    selection_id: UUID
    status: str
    reason_codes: tuple[str, ...]
    cohort_id: UUID | None = None
    slot_set_version: int | None = None
    slot_contract_count: int = 0

    def __post_init__(self) -> None:
        status = self.status.strip().upper()
        reasons = tuple(dict.fromkeys(self.reason_codes))
        ready = status == "READY"
        if (status not in {"READY", "BLOCKED"}
                or ready != (not reasons)
                or ready != (self.cohort_id is not None)
                or ready != (self.slot_set_version is not None)
                or (self.slot_set_version is not None and self.slot_set_version <= 0)
                or type(self.slot_contract_count) is not int
                or self.slot_contract_count < 0
                or ready != (self.slot_contract_count > 0)):
            raise ValueError("Options admission result is invalid")
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "reason_codes", reasons)

    @property
    def evidence_ref(self) -> str:
        cohort = str(self.cohort_id) if self.cohort_id is not None else "none"
        version = self.slot_set_version if self.slot_set_version is not None else 0
        return (
            f"options-admission:{self.selection_id}:{self.status}:"
            f"cohort={cohort}:slot={version}:{self.slot_contract_count}"
        )


@dataclass(frozen=True, slots=True)
class OptionsFlowSettings:
    consumer_name: str = "option_flow_stream_v1"
    provider_tier: str = "MASSIVE_ADVANCED"
    shard_count: int = 4
    queue_capacity: int = 50_000
    history_per_contract: int = 512
    initial_baseline: BaselineKind = BaselineKind.WARMING
    limits: ConsumerLimits = ConsumerLimits()
    flush_rows: int = FLUSH_ROWS
    flush_interval_seconds: float = FLUSH_INTERVAL_SECONDS
    owner_epoch: int = 1
    max_checkpoint_bytes: int = DEFAULT_MAX_STATE_BYTES
    # A full engine checkpoint costs O(state) (see the measurement in test_option_flow_checkpoint_state); between
    # full checkpoints a flush only advances the durable row offset. 0 writes the state at every flush.
    checkpoint_interval_seconds: float = 10.0
    checkpoint_interval_ms: int = 5_000
    backlog_degraded_records: int = 10_000
    open_interest_max_rows: int = 500_000


def options_acquire_block_reason(environment: RunnerEnvironment) -> str | None:
    return None if callable(environment.options_acquire_action) else OPTIONS_ACQUIRE_NOT_CONFIGURED


def options_admission_block_reason(environment: RunnerEnvironment) -> str | None:
    return None if callable(environment.options_admission_action) else OPTIONS_ADMISSION_NOT_CONFIGURED


def options_acquire_runner(environment: RunnerEnvironment) -> StageRunner:
    def run(context: StageContext) -> StageOutcome:
        blocked = options_acquire_block_reason(environment)
        if blocked is not None:
            return StageOutcome.blocked(blocked)
        if context.is_cancelled():
            return StageOutcome.failed(STAGE_CANCELLED)
        result = environment.options_acquire_action(context)
        if not isinstance(result, OptionsAcquireResult):
            return StageOutcome.failed(OPTIONS_ACQUIRE_INCOMPLETE)
        if not result.complete:
            return StageOutcome.failed(OPTIONS_ACQUIRE_INCOMPLETE)
        return StageOutcome.complete(evidence_ref=result.evidence_ref)

    return run


def options_admission_runner(environment: RunnerEnvironment) -> StageRunner:
    def run(context: StageContext) -> StageOutcome:
        blocked = options_admission_block_reason(environment)
        if blocked is not None:
            return StageOutcome.blocked(blocked)
        if context.is_cancelled():
            return StageOutcome.failed(STAGE_CANCELLED)
        result = environment.options_admission_action(context)
        if not isinstance(result, OptionsAdmissionResult):
            return StageOutcome.failed(OPTIONS_ADMISSION_INCOMPLETE)
        if result.status == "BLOCKED":
            return StageOutcome("BLOCKED_POLICY", result.reason_codes, None, result.evidence_ref)
        return StageOutcome.complete(evidence_ref=result.evidence_ref)

    return run


@dataclass(frozen=True, slots=True)
class DivergenceQuarantine:
    """What a divergence leaves behind: the uncommitted offset range and the divergent key identifiers.
    No message payload and no row value is kept."""

    table: str
    identities: tuple[tuple, ...]
    first_offset: int
    last_offset: int
    journal_generation: UUID
    consumer_name: str
    session_date: date

    @property
    def evidence_ref(self) -> str:
        digest = hashlib.sha256(repr(self.identities).encode()).hexdigest()[:16]
        return (f"divergence:{self.table}:{str(self.journal_generation)[:8]}:"
                f"{self.first_offset}-{self.last_offset}:{digest}")


@dataclass(frozen=True, slots=True)
class StartInfo:
    mode: str  # RESTORED or WARMING
    reason: str | None
    offset: int
    checkpoint_bytes: int = 0


class StageTransactionConnection:
    """A psycopg2-style connection (cursor/commit/rollback, never autocommit) over the stage's
    transactions, so the writer's flush runs as one stage transaction of one capability role.

    A transaction opens at the first `cursor()` and ends at `commit()` (the access layer commits) or
    `rollback()` (the access layer rolls back). `context` is bound again on every tick."""

    autocommit = False

    class _Rollback(Exception):
        pass

    class _Cursor:
        __slots__ = ("_cursor",)

        def __init__(self, cursor: Any) -> None:
            self._cursor = cursor

        def close(self) -> None:  # the transaction owns the real cursor
            return None

        def __getattr__(self, name: str) -> Any:
            return getattr(self._cursor, name)

    def __init__(self) -> None:
        self.context: StageContext | None = None
        self._manager: Any = None
        self._cursor: StageTransactionConnection._Cursor | None = None

    def bind(self, context: StageContext) -> None:
        self.context = context

    def cursor(self) -> Any:
        if self._manager is None:
            if self.context is None:
                raise RuntimeError("the stage transaction connection is not bound to a stage context")
            manager = self.context.transaction()
            unit = manager.__enter__()
            self._manager, self._cursor = manager, self._Cursor(unit.cursor)
        return self._cursor

    def commit(self) -> None:
        manager, self._manager, self._cursor = self._manager, None, None
        if manager is not None:
            manager.__exit__(None, None, None)

    def rollback(self) -> None:
        manager, self._manager, self._cursor = self._manager, None, None
        if manager is not None:
            error = self._Rollback()
            try:
                manager.__exit__(type(error), error, None)
            except self._Rollback:
                pass


@dataclass(slots=True)
class _Session:
    consumer: OptionFlowConsumer
    connection: StageTransactionConnection
    session_date: date
    start: StartInfo
    generation: UUID
    consumer_name: str


class OptionsFlowState:
    """Per-process OPTIONS_FLOW state shared by ticks: the live session and the divergence quarantine."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.session: _Session | None = None
        self.quarantine: DivergenceQuarantine | None = None
        self.last_report: SliceReport | None = None

    def discard_session(self) -> None:
        """Drop the engine, its writer buffer and its connection without flushing anything."""
        self.session = None

    def reset_divergence(self) -> DivergenceQuarantine | None:
        """The explicit operator reset: clear the quarantine so the next tick restarts the session from
        the last stored checkpoint. Returns the cleared record (None when nothing was quarantined)."""
        with self.lock:
            cleared, self.quarantine = self.quarantine, None
            self.session = None
            return cleared


# --- gap rows (rt.stream_gaps, options_ingress) ----------------------------------------------------

INSTRUMENT_SQL = (
    "SELECT DISTINCT ON (c.contract_ticker) c.contract_ticker, i.instrument_id FROM ref.option_contracts c "
    "JOIN rt.instruments i ON i.contract_id = c.contract_id AND i.asset_class = 'OPTION' "
    "WHERE c.contract_ticker = ANY(%s) ORDER BY c.contract_ticker, i.instrument_id"
)
GAP_SQL = (
    "INSERT INTO rt.stream_gaps (gap_id, session_id, instrument_id, channel, gap_start, gap_end, "
    "journal_generation, shard_id, last_durable_offset, reason, recovery_status) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'UNRESOLVED') ON CONFLICT (gap_id) DO NOTHING"
)


def gap_identity(entry: GapEntry, instrument_id: int | None) -> UUID:
    """Deterministic: a replay records the same gap under the same id (ON CONFLICT DO NOTHING)."""
    parts = (entry.stream_session_id, entry.journal_generation, entry.shard_id, entry.channel or "*", entry.reason,
             entry.start_ms, entry.end_ms, entry.first_offset, entry.last_offset,
             "*" if instrument_id is None else instrument_id)
    return uuid5(_NAMESPACE, "|".join(str(part) for part in parts))


def persist_gap_entries(cursor: Any, entries: Sequence[GapEntry]) -> int:
    """Insert one rt.stream_gaps row per gap and shard. Contracts that are instruments get their own row;
    a gap on all contracts, or on a contract that is not an instrument, gets one row without an
    instrument (the INCOMPLETE volume checkpoints name the contracts). Returns rows inserted."""
    tickers = sorted({contract for entry in entries for contract in entry.contracts})
    instruments: dict[str, int] = {}
    if tickers:
        cursor.execute(INSTRUMENT_SQL, (tickers,))
        instruments = {str(ticker): int(identity) for ticker, identity in cursor.fetchall()}
    inserted = 0
    for entry in entries:
        if len(entry.reason) > 64:
            raise ValueError("a stream gap reason is at most 64 characters")
        targets: list[int | None]
        if not entry.contracts:
            targets = [None]
        else:
            resolved = sorted({instruments[contract] for contract in entry.contracts if contract in instruments})
            targets = list(resolved)
            if any(contract not in instruments for contract in entry.contracts):
                targets.append(None)
        for instrument_id in targets:
            cursor.execute(GAP_SQL, (
                str(gap_identity(entry, instrument_id)), str(entry.stream_session_id), instrument_id, entry.channel,
                to_datetime(entry.start_ms), to_datetime(entry.end_ms), str(entry.journal_generation),
                entry.shard_id, entry.last_durable_offset, entry.reason))
            inserted += cursor.rowcount
    return inserted


# --- prior open interest (ref.oi_baselines) ---------------------------------------------------------

OPEN_INTEREST_SQL = (
    "SELECT DISTINCT ON (c.contract_ticker) c.contract_ticker, b.open_interest, b.settlement_session "
    "FROM ref.oi_baselines b JOIN ref.option_contracts c ON c.contract_id = b.contract_id "
    "WHERE b.observed_session = %s AND b.settlement_session < %s AND b.received_at <= %s "
    "AND NOT EXISTS (SELECT 1 FROM ref.oi_baselines s WHERE s.supersedes_baseline_id = b.baseline_id "
    "AND s.received_at <= %s) "
    "ORDER BY c.contract_ticker, b.observed_at DESC, b.received_at DESC LIMIT %s"
)


def load_prior_open_interest_cursor(cursor: Any, session_date: date, known_at: datetime,
                                    *, max_rows: int) -> dict[str, PriorOpenInterest]:
    """Open interest published before `session_date` and received by `known_at`, per contract ticker.
    A superseded baseline is ignored; more rows than `max_rows` is an error, never a truncation."""
    cursor.execute(OPEN_INTEREST_SQL, (session_date, session_date, known_at, known_at, max_rows + 1))
    rows = cursor.fetchall()
    if len(rows) > max_rows:
        raise OpenInterestLoadError("OPEN_INTEREST_ROW_BOUND_EXCEEDED")
    return {str(ticker): PriorOpenInterest(int(open_interest), settlement) for ticker, open_interest, settlement in rows}


OpenInterestLoader = Callable[[StageContext, date], Mapping[str, PriorOpenInterest]]


def reference_open_interest_loader(max_rows: int) -> OpenInterestLoader:
    def load(context: StageContext, session_date: date) -> Mapping[str, PriorOpenInterest]:
        with context.transaction(read_only=True) as unit:
            return load_prior_open_interest_cursor(unit.cursor, session_date, context.started_at, max_rows=max_rows)

    return load


# --- configuration gates ----------------------------------------------------------------------------

def options_flow_block_reason(environment: RunnerEnvironment) -> str | None:
    """Why OPTIONS_FLOW would be BLOCKED_POLICY with this environment (None: it can run)."""
    source = environment.options_flow_source
    if (source is None or not callable(getattr(source, "read_after", None))
            or not isinstance(getattr(source, "generation", None), UUID)):
        return OPTIONS_FLOW_SOURCE_NOT_CONFIGURED
    semantics = environment.options_flow_semantics
    if semantics is None or not semantics.is_live_capture:
        return OPTIONS_FLOW_CONDITIONS_UNPINNED
    if not isinstance(environment.options_flow_policy, FlowStrategyPolicy):
        return OPTIONS_FLOW_POLICY_NOT_CONFIGURED
    return None


def _engine_factory(environment: RunnerEnvironment, settings: OptionsFlowSettings, context: StageContext,
                    generation: UUID) -> Callable[[], OptionFlowEngine]:
    config = EngineConfig(
        cohort_id=context.plan.cohort_id,
        provider_tier=settings.provider_tier,
        policy=environment.options_flow_policy,
        semantics=environment.options_flow_semantics,
        journal_generation=generation,
        shard_count=settings.shard_count,
        queue_capacity=settings.queue_capacity,
        checkpoint_interval_ms=settings.checkpoint_interval_ms,
        volume=VolumeConfig(history_per_contract=settings.history_per_contract,
                            initial_baseline=settings.initial_baseline),
    )
    return lambda: OptionFlowEngine(config)


def _start_session(environment: RunnerEnvironment, settings: OptionsFlowSettings, context: StageContext,
                   *, rolled_over: bool = False) -> _Session:
    source = environment.options_flow_source
    generation = source.generation
    fresh = _engine_factory(environment, settings, context, generation)
    engine = fresh()
    store = CheckpointStore(consumer_name=settings.consumer_name, cohort_id=context.plan.cohort_id,
                            generation=generation, owner_epoch=settings.owner_epoch,
                            max_state_bytes=settings.max_checkpoint_bytes)
    with context.transaction(read_only=True) as unit:
        rows = store.load(unit.cursor)
    tail_reader = getattr(source, "last_offset", None)
    tail = None if tail_reader is None else int(tail_reader())
    restored: RestoredCheckpoint | None = None
    warming: str | None = None
    start_offset = 0
    if not rows:
        warming = None if settings.initial_baseline is BaselineKind.SESSION_COMPLETE else WARMING_NO_CHECKPOINT
    else:
        try:
            restored = store.restore(engine, rows, source_tail=tail)
        except CheckpointRejected as rejection:
            logger.error("options flow checkpoint rejected: %s (offset %s)", rejection.code, rejection.offset)
            engine = fresh()
            warming = rejection.code
            claimed = rejection.offset
            store.must_write_full = True  # replace the rejected payload, never just advance its row offset
            if claimed is None or (tail is not None and claimed > tail):
                start_offset = tail or 0
                store.allow_regression = True
            else:
                start_offset = claimed
    loader = environment.options_flow_oi_loader or reference_open_interest_loader(settings.open_interest_max_rows)
    engine.set_prior_open_interest(loader(context, context.plan.session_date))
    prune_sessions(engine, context.plan.session_date - timedelta(days=1))
    connection = StageTransactionConnection()
    connection.bind(context)

    def writer_factory(hook):
        return OptionFlowWriter(connection, flush_rows=settings.flush_rows,
                                flush_interval_seconds=settings.flush_interval_seconds,
                                clock=environment.options_flow_clock, in_transaction=hook)

    def gap_sink(entries: Sequence[GapEntry]) -> None:
        try:
            with connection.context.transaction() as unit:
                persist_gap_entries(unit.cursor, entries)
        except PsycopgError as error:
            raise GapPersistenceError(type(error).__name__) from None

    consumer = OptionFlowConsumer(
        engine, source, writer_factory, generation=generation, checkpoints=store, gap_sink=gap_sink,
        limits=settings.limits, clock=environment.options_flow_clock, start_offset=start_offset,
        restored=restored, warming_reason=warming,
        checkpoint_interval_seconds=settings.checkpoint_interval_seconds)
    if rolled_over:
        consumer.reset_report(warming_reason=(
            None if settings.initial_baseline is BaselineKind.SESSION_COMPLETE else WARMING_NEW_SESSION))
    start = StartInfo("RESTORED" if restored is not None else "WARMING", warming,
                      consumer.fed_offset, 0 if restored is None else restored.payload_bytes)
    return _Session(consumer, connection, context.plan.session_date, start, generation, settings.consumer_name)


def _outcome(session: _Session, settings: OptionsFlowSettings) -> StageOutcome:
    consumer = session.consumer
    degraded: list[str] = []
    if consumer.warming_reason:
        degraded.append(f"OPTIONS_FLOW_WARMING:{consumer.warming_reason}")
    if consumer.gaps_seen:
        degraded.append(f"OPTIONS_FLOW_GAP:{consumer.gaps_seen}")
    backlog = consumer.backlog()
    if backlog is not None and backlog > settings.backlog_degraded_records:
        degraded.append(f"OPTIONS_FLOW_BACKLOG:{backlog}")
    evidence = (f"options_flow:{str(session.generation)[:8]}:fed={consumer.fed_offset}:"
                f"flushed={consumer.flushed_offset}:checkpoint={consumer.checkpointed_offset}:"
                f"start={session.start.mode}")
    return StageOutcome.complete(degraded=tuple(degraded), evidence_ref=evidence)


def _quarantine(state: OptionsFlowState, session: _Session, divergence: OptionFlowReplayDivergence) -> StageOutcome:
    consumer = session.consumer
    quarantine = DivergenceQuarantine(
        divergence.table or "UNKNOWN", tuple(divergence.identities), consumer.flushed_offset + 1,
        consumer.fed_offset, session.generation, session.consumer_name, session.session_date)
    state.quarantine = quarantine
    state.discard_session()
    logger.error("options flow replay divergence in %s: %d key(s), offsets %d-%d quarantined; stage blocked",
                 quarantine.table, len(quarantine.identities), quarantine.first_offset, quarantine.last_offset)
    return _divergence_outcome(quarantine)


def _divergence_outcome(quarantine: DivergenceQuarantine) -> StageOutcome:
    return StageOutcome("BLOCKED_POLICY", (REPLAY_DIVERGENCE,), None, quarantine.evidence_ref)


def options_flow_runner(environment: RunnerEnvironment) -> StageRunner:
    def run(context: StageContext) -> StageOutcome:
        blocked = options_flow_block_reason(environment)
        if blocked is not None:
            return StageOutcome.blocked(blocked)
        if context.database is None:
            return StageOutcome.blocked(OPTIONS_FLOW_DATABASE_NOT_CONFIGURED)
        state = environment.options_flow_state
        if not state.lock.acquire(blocking=False):
            return StageOutcome.complete(degraded=(OPTIONS_FLOW_TICK_BUSY,))
        try:
            return _tick(environment, state, context)
        finally:
            state.lock.release()

    return run


def _tick(environment: RunnerEnvironment, state: OptionsFlowState, context: StageContext) -> StageOutcome:
    settings = environment.options_flow_settings or OptionsFlowSettings()
    if state.quarantine is not None:
        return _divergence_outcome(state.quarantine)
    if context.is_cancelled():
        return StageOutcome.failed(STAGE_CANCELLED)
    session = state.session
    rolled_over = False
    try:
        if session is not None and session.session_date != context.plan.session_date:
            session.connection.bind(context)
            session.consumer.finish()
            state.discard_session()
            session = None
            rolled_over = True
        if session is None:
            session = state.session = _start_session(environment, settings, context, rolled_over=rolled_over)
        session.connection.bind(context)
        state.last_report = session.consumer.run_slice(is_cancelled=context.is_cancelled)
    except OptionFlowReplayDivergence as divergence:
        if session is None:
            raise
        return _quarantine(state, session, divergence)
    except StageCancelled:
        return StageOutcome.failed(STAGE_CANCELLED)
    except CheckpointTooLarge:
        return StageOutcome.failed(OPTIONS_FLOW_CHECKPOINT_TOO_LARGE)
    except CheckpointRegression:
        return StageOutcome.failed(OPTIONS_FLOW_CHECKPOINT_REGRESSION)
    except OptionFlowSchemaViolation:
        return StageOutcome.failed(OPTIONS_FLOW_SCHEMA_VIOLATION)
    except OptionFlowWriteError:
        return StageOutcome.failed(OPTIONS_FLOW_WRITE_FAILED)
    except GapPersistenceError:
        return StageOutcome.failed(OPTIONS_FLOW_GAP_PERSIST_FAILED)
    except OptionFlowSourceError as error:
        return StageOutcome.failed(OPTIONS_FLOW_SOURCE_FAILED, error.code)
    except OpenInterestLoadError:
        return StageOutcome.failed(OPTIONS_FLOW_OPEN_INTEREST_FAILED)
    if state.last_report.stopped == "CANCELLED":
        return StageOutcome.failed(STAGE_CANCELLED)
    return _outcome(session, settings)
