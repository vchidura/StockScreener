"""R75-v2 (Nov 14, user): the Equity ticker projection split over long-lived ticker-group workers.

Profile of one cold projection at 45 names (Nov 14, E:20:55 input): ~89% is per-ticker work (interval materialization
48%, bar facts 19%, derived-bar verification 17%); the cross-sectional screening is 7%. Warm windows project in 25-33 s
because `TickerProjectionState` and the delta `ScreeningInputLoader` are carried between windows; cold ones take
123-163 s. R75 (forked pool, Nov 13) re-did the shared prologue in every fork over the whole input, copied the
scheduler heap page by page (the 20:17 VM stall) and threw each fork's state away (every window cold). Replaced.

Design:
- The coordinator (the window's own process) loads the full input with its warm loader as before, computes the shared
  prologue and the screening once (`member_filter=frozenset()`), and merges the groups' results. Summary, Pattern
  Watch, persistence, the Dashboard and publication are unchanged.
- `REALTIME_V2_TICKER_PROJECTION_SHARDS=K` (2-8) starts K worker processes once ("spawn": a fresh interpreter, never
  a copy of the scheduler heap). Worker i owns ticker group i (`shard_tickers`: contiguous blocks of the sorted
  members, so the groups concatenated in order are the serial order). It loads only its group's bars
  (`bar_shard=(i, K)`) and its members' calendar and fundamental rows, with its own delta loader, and projects only
  its group (`member_filter`) with the coordinator's screening (`screening_override`) and its own
  `TickerProjectionState`. Every workspace input is per ticker (a ticker's own rows plus cohort-level manifest fields),
  so each workspace equals the serial one; the parity rehearsal checks it byte for byte.
- Each worker keeps its loader and state between windows with the scheduler's warm rule (`_WarmProjection`): reused
  only for a newer cutoff of the same cohort; an older or equal cutoff (retry, recovery) gets a fresh pair and leaves
  the warm one alone. A worker drops its pair when its own load or projection fails, not when the window fails later
  (publication): its pair is consistent, so the next window's projection stays warm (R73 cascade).
- Workers start loading when the window's projection starts, while the coordinator loads and screens (overlap).
- Bars are read by their clocks (cutoff, known_by), not by the coordinator's snapshot: a row recorded after known_by
  is invisible to every reader, so the coordinator and the workers read the same rows.
- A cancelled window, a dead worker or a protocol fault resets the pool (workers start cold next time). A worker's
  exception fails the cycle as the serial projection's would (after the other workers have answered).
- Unset, 0 or 1: the serial projection, unchanged; no process is started. Rehydration always runs serially.
"""
from __future__ import annotations

import atexit
from dataclasses import dataclass
import logging
import multiprocessing
import os
import threading
import time
import traceback

from .screening_delta_loader import ScreeningInputLoader
from .screening_repository import shard_tickers
from .ticker_projection import (
    RealtimeTickerWorkspaceProjection,
    TickerProjectionCancelled,
    TickerProjectionState,
    project_realtime_ticker_workspace,
)

SHARDS_SWITCH = "REALTIME_V2_TICKER_PROJECTION_SHARDS"
MAX_SHARDS = 8
POLL_SECONDS = 0.25
logger = logging.getLogger(__name__)


def ticker_projection_shards(environment=None) -> int:
    """The configured group count: 2..MAX_SHARDS, else 0 (serial)."""
    raw = str((os.environ if environment is None else environment).get(SHARDS_SWITCH, "")).strip()
    if not raw:
        return 0
    try:
        value = int(raw)
    except ValueError:
        logger.warning("ignoring %s=%r (not an integer): serial projection", SHARDS_SWITCH, raw)
        return 0
    if value < 2:
        return 0
    return min(value, MAX_SHARDS)


class ShardPair:
    """One worker's warm loader and projection state, with the scheduler's `_WarmProjection` rule."""

    def __init__(self, *, loader_factory=ScreeningInputLoader, state_factory=TickerProjectionState):
        self._loader_factory, self._state_factory = loader_factory, state_factory
        self.cohort_id = self.cutoff = self.loader = self.state = None

    def take(self, cohort_id, cutoff):
        if self.cohort_id == cohort_id and self.cutoff is not None and cutoff > self.cutoff:
            return self.loader, self.state, True
        return self._loader_factory(), self._state_factory(), False

    def settle(self, cohort_id, cutoff, loader, state, *, warm, complete):
        if warm:
            if complete:
                self.cutoff = cutoff
            else:
                self.drop()
            return
        if complete and (self.cohort_id != cohort_id or self.cutoff is None or cutoff > self.cutoff):
            self.cohort_id, self.cutoff, self.loader, self.state = cohort_id, cutoff, loader, state

    def drop(self):
        self.cohort_id = self.cutoff = self.loader = self.state = None


def _default_database():
    """The worker's own read-only access, built as the window's `_provider` (hardened app role)."""
    from psycopg2.pool import ThreadedConnectionPool

    from .database_access_v2 import APP_ROLE, V2AccessConfig, V2ConnectionProvider
    from .migrations import EXPECTED_V2_DATABASE_NAME

    dsn = os.environ.get("REALTIME_V2_APP_DSN", "").strip()
    if not dsn:
        raise RuntimeError("ticker shard worker requires REALTIME_V2_APP_DSN")
    pool = ThreadedConnectionPool(1, 1, dsn=dsn, options="-c timezone=UTC")
    provider = V2ConnectionProvider(
        {APP_ROLE: pool},
        V2AccessConfig(security_mode="hardened", role_prefix=EXPECTED_V2_DATABASE_NAME, statement_timeout_ms=300_000),
    )
    provider.verify()

    class _Reader:
        def transaction(self):
            return provider.transaction(APP_ROLE, read_only=True, dict_cursor=True, isolation_level="REPEATABLE READ")

    return _Reader()


def _reference_loaders():
    from .ticker_repository import _load_ticker_calendar_inputs, _load_ticker_fundamental_inputs

    return _load_ticker_calendar_inputs, _load_ticker_fundamental_inputs


class ShardWorker:
    """Worker i of K: `prepare(scope)` loads the group's input, `project(screening)` projects it."""

    def __init__(self, index, count, *, database_factory=_default_database, reference_loaders=_reference_loaders,
                 pair=None):
        self.index, self.count = index, count
        self._database_factory, self._reference_loaders = database_factory, reference_loaders
        self._database = None
        self.pair = ShardPair() if pair is None else pair
        self._cycle = None

    def handle(self, message):
        op, sequence = message[0], message[2]
        if op == "prepare":
            return self.prepare(message[1], sequence)
        if op == "project":
            return self.project(message[1], sequence)
        raise ValueError(f"unknown ticker shard operation {op!r}")

    def _settle(self, *, complete):
        cycle, self._cycle = self._cycle, None
        if cycle is not None:
            self.pair.settle(cycle["cohort_id"], cycle["cutoff"], cycle["loader"], cycle["state"],
                             warm=cycle["warm"], complete=complete)

    def prepare(self, scope, sequence=0):
        if self._cycle is not None:  # prepared and never projected (the coordinator gave up): not complete
            self._settle(complete=False)
        started = time.perf_counter()
        loader, state, warm = self.pair.take(scope["cohort_id"], scope["cutoff"])
        self._cycle = dict(cohort_id=scope["cohort_id"], cutoff=scope["cutoff"], loader=loader, state=state,
                           warm=warm, sequence=sequence)
        try:
            if self._database is None:
                self._database = self._database_factory()
            load_calendar, load_fundamentals = self._reference_loaders()
            with self._database.transaction() as unit:
                cursor = getattr(unit, "cursor", unit)
                inputs = loader.load(cursor, **scope, include_portal_history=True, bar_shard=(self.index, self.count))
                group = shard_tickers([member["ticker"] for member in inputs.members], self.index, self.count)
                members = tuple(member for member in inputs.members if member["ticker"] in frozenset(group))
                events, event_coverage = load_calendar(cursor, members, inputs.cutoff)
                reports, coverage = load_fundamentals(cursor, members, inputs.cutoff)
        except BaseException:
            self._settle(complete=False)
            raise
        report = getattr(loader, "last_report", None)
        self._cycle.update(inputs=inputs, group=group, references=(events, event_coverage, reports, coverage))
        return ("prepared", self.index, group, sequence, dict(
            load_s=round(time.perf_counter() - started, 3), warm=warm, bars=len(inputs.bars),
            load_mode=getattr(report, "mode", None),
        ))

    def project(self, screening, sequence=0):
        cycle = self._cycle
        if cycle is None or "inputs" not in cycle or cycle["sequence"] != sequence:
            raise RuntimeError("ticker shard was not prepared for this cycle")
        started = time.perf_counter()
        events, event_coverage, reports, coverage = cycle["references"]
        try:
            projection = project_realtime_ticker_workspace(
                cycle["inputs"], market_events=events, market_event_coverage=event_coverage,
                fundamental_reports=reports, fundamental_coverage=coverage, state=cycle["state"],
                member_filter=frozenset(cycle["group"]), screening_override=screening,
            )
        except BaseException:
            self._settle(complete=False)
            raise
        stats = dict(cycle["state"].stats)
        self._settle(complete=True)
        # Plain fields, not the dataclass: the screening generation stays with the coordinator.
        part = (projection.snapshots, projection.materializations, projection.pattern_materializations,
                projection.summary_charts, projection.unavailable_members)
        return ("projected", self.index, cycle["group"], sequence, part, dict(
            project_s=round(time.perf_counter() - started, 3), maxrss_mb=_maxrss_mb(), state=stats,
        ))


def _maxrss_mb():
    try:
        import resource

        return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)
    except Exception:  # noqa: BLE001 - informational only
        return None


def _worker_main(connection, index, count):  # pragma: no cover - runs in the spawned process
    _serve(connection, ShardWorker(index, count))


def _serve(connection, worker):
    index = worker.index
    while True:
        try:
            message = connection.recv()
        except (EOFError, OSError):
            return
        if message[0] == "stop":
            return
        try:
            reply = worker.handle(message)
        except BaseException as error:  # noqa: BLE001 - reported to the coordinator, which fails the cycle
            reply = ("error", index, type(error).__name__, str(error)[:2000], traceback.format_exc()[-6000:])
        try:
            connection.send(reply)
        except Exception as error:  # noqa: BLE001 - e.g. an unpicklable result
            connection.send(("error", index, type(error).__name__, str(error)[:2000], traceback.format_exc()[-6000:]))


class ShardWorkerFailed(RuntimeError):
    """A ticker-group worker raised: the cycle fails as a serial projection error would."""


@dataclass
class _Worker:
    process: object
    connection: object


class TickerProjectionShardPool:
    """K long-lived ticker-group workers owned by the window process. One cycle at a time."""

    def __init__(self, count, *, context=None, target=_worker_main):
        if not 2 <= count <= MAX_SHARDS:
            raise ValueError("ticker shard count is out of range")
        self.count = count
        self._context = multiprocessing.get_context("spawn") if context is None else context
        self._target = target
        self._workers: list[_Worker] = []
        self._lock = threading.Lock()
        self._prepared = False
        self._drained = True
        self._sequence = 0
        self.last_cycle = None

    def _ensure(self):
        if len(self._workers) == self.count and all(worker.process.is_alive() for worker in self._workers):
            return
        self.reset()
        for index in range(self.count):
            parent, child = self._context.Pipe(duplex=True)
            process = self._context.Process(
                target=self._target, args=(child, index, self.count), name=f"ticker-shard-{index}", daemon=True,
            )
            process.start()
            child.close()
            self._workers.append(_Worker(process, parent))

    def reset(self):
        workers, self._workers, self._prepared = self._workers, [], False
        for worker in workers:
            try:
                worker.connection.send(("stop",))
            except Exception:  # noqa: BLE001
                pass
        for worker in workers:
            worker.process.join(timeout=2)
            if worker.process.is_alive():
                worker.process.terminate()
                worker.process.join(timeout=5)
            try:
                worker.connection.close()
            except Exception:  # noqa: BLE001
                pass

    def prepare(self, scope):
        """Start every worker's load for this cycle (they run while the coordinator loads and screens)."""
        with self._lock:
            if self._prepared:  # the previous cycle never projected: its replies are still in flight
                self.reset()
            self._ensure()
            self._sequence += 1
            for worker in self._workers:
                worker.connection.send(("prepare", dict(scope), self._sequence))
            self._prepared = True

    def _receive(self, index, should_stop):
        worker = self._workers[index]
        while not worker.connection.poll(POLL_SECONDS):
            if should_stop is not None and should_stop():
                raise TickerProjectionCancelled("ticker cycle was cancelled")
            if not worker.process.is_alive():
                raise ShardWorkerFailed(f"ticker shard {index} exited (code {worker.process.exitcode})")
        try:
            return worker.connection.recv()
        except (EOFError, OSError) as error:
            raise ShardWorkerFailed(f"ticker shard {index} connection lost: {error}") from error

    def project(self, inputs, *, state, should_stop=None):
        """The cycle's projection: the coordinator's prologue and screening, then every group's workspaces."""
        with self._lock:
            if not self._prepared:
                raise RuntimeError("ticker shard pool was not prepared for this cycle")
            self._prepared = False
            self._drained = False
            try:
                return self._project(inputs, state=state, should_stop=should_stop)
            except BaseException:
                # A cancelled or broken cycle leaves replies in flight: start over (the workers start cold). After
                # every reply was read (a worker raised, or no member is available) the workers are idle and keep
                # their pairs (the one that raised dropped its own).
                if not self._drained:
                    self.reset()
                raise

    def _project(self, inputs, *, state, should_stop):
        started = time.perf_counter()
        base = project_realtime_ticker_workspace(inputs, state=state, should_stop=should_stop,
                                                 member_filter=frozenset())
        screened = time.perf_counter()
        tickers = [member["ticker"] for member in inputs.members]
        expected = [shard_tickers(tickers, index, self.count) for index in range(self.count)]
        prepared, parts, failures = {}, {}, []

        def checked(reply, phase, index):
            if reply[0] == "error":
                failures.append(reply)
                return None
            if (reply[0] != phase or reply[1] != index or tuple(reply[2]) != expected[index]
                    or reply[3] != self._sequence):
                raise ShardWorkerFailed(f"ticker shard {index} answered out of protocol: {reply[0]!r}")
            return reply

        # The screening goes to a worker only once it has answered "prepared", i.e. while it waits in recv(): a
        # large send never blocks on a worker that is still loading, and every wait polls for cancellation.
        for index in range(self.count):
            reply = checked(self._receive(index, should_stop), "prepared", index)
            if reply is not None:
                prepared[index] = reply
                self._workers[index].connection.send(("project", base.screening_generation, self._sequence))
        for index in prepared:
            reply = checked(self._receive(index, should_stop), "projected", index)
            if reply is not None:
                parts[index] = reply
        self._drained = True
        if failures:
            first = failures[0]
            raise ShardWorkerFailed(f"ticker shard {first[1]} failed: {first[2]}: {first[3]}\n{first[4]}")
        snapshots, materializations, pattern_materializations, summary_charts, unavailable = [], [], [], {}, []
        for index in range(self.count):
            part_snapshots, part_materializations, part_patterns, part_charts, part_unavailable = parts[index][4]
            snapshots.extend(part_snapshots)
            materializations.extend(part_materializations)
            pattern_materializations.extend(part_patterns)
            summary_charts.update(part_charts)
            unavailable.extend(part_unavailable)
        if unavailable and len(unavailable) == len(inputs.members):
            first = unavailable[0]
            raise ValueError(f"Ticker workspaces have no available member: {first['ticker']}: {first['reason']}")
        self.last_cycle = dict(
            screening_s=round(screened - started, 3), total_s=round(time.perf_counter() - started, 3),
            workers=[dict(prepared[index][4], **parts[index][5], tickers=len(expected[index]))
                     for index in range(self.count)],
        )
        logger.info("ticker_projection_shards cutoff=%s %s", inputs.cutoff.isoformat(), self.last_cycle)
        return RealtimeTickerWorkspaceProjection(
            snapshots=tuple(snapshots), materializations=tuple(materializations),
            pattern_materializations=tuple(pattern_materializations), summary_charts=summary_charts,
            screening_generation=base.screening_generation, unavailable_members=tuple(unavailable),
        )


_POOL: TickerProjectionShardPool | None = None
_POOL_LOCK = threading.Lock()


def shard_pool(environment=None) -> TickerProjectionShardPool | None:
    """The process's pool for the configured group count, or None (serial)."""
    global _POOL
    count = ticker_projection_shards(environment)
    with _POOL_LOCK:
        if count == 0:
            return None
        if _POOL is None or _POOL.count != count:
            if _POOL is not None:
                _POOL.reset()
            _POOL = TickerProjectionShardPool(count)
            atexit.register(_POOL.reset)
        return _POOL
