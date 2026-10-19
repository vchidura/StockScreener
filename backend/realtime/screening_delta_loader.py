"""Retained, incremental bar loading for the screening input (the delta loader).

`load_realtime_screening_input` re-reads every retained bar of the cohort each cycle (124,236 bars for
nine names, about 5.5 million at 400) although a 15-minute cycle changes a few bars per ticker.
`ScreeningInputLoader` keeps the previously loaded bar set in process memory and reads only the rows whose
visibility changed since the previous load. Its result is EXACTLY what the full load returns for the same
(cutoff, known_by) (the full load is the oracle in the tests): the same bars, order and row dicts.

Two clocks
----------
A window has a market cutoff c (a bar must have ended by it: bar_end <= c) and a later known_by k >= c (the
system must have recorded the bar by it: observed_at, created_at, replay_available_at and
provider_published_at <= k). The live runner passes k = window.deadline, so bars fetched and inserted within the
window's completion budget are visible to the window itself; a later insert is a late arrival for the next
window. known_by defaults to the cutoff (the single clock every caller used before, byte-identical SQL and
parameters). Reference, coverage, manifest and every other check keep using the cutoff.

Correctness argument
--------------------
Let V(c, k) be the candidate revisions visible under (c, k): the full load's WHERE clause (cohort, tier, ticker
set, final RTH, history window, the four recording clocks <= k and bar_end <= c).
1. Monotonic: every clock is an immutable column of an immutable row (rt.bar_revisions rejects UPDATE and
   DELETE) compared with c or k, so V(c0, k0) is a subset of V(c1, k1) for c0 <= c1 and k0 <= k1: a revision
   visible earlier stays visible. If either clock moves backwards the subset relation is lost: full reload.
2. The delta SELECT returns exactly V(c1, k1) minus V(c0, k0): the same predicate under (c1, k1) and "some
   recording clock lies after k0 or bar_end lies after c0" (NOT visible under (c0, k0), written clock by clock
   with the operators of the visibility predicate, each on its own clock; a NULL optional clock never blocks
   visibility, so it never makes a row newly visible). bar_end is a clause of its own: with k > c a bar can
   end after c0 yet have been recorded before k0.
3. Re-picking: the winner of a bar identity under (c1, k1) is the best of V(c1, k1) = V(c0, k0) + new, hence
   the better of the retained winner (the best of V(c0, k0), by induction) and the best new row, under the full
   load's total order (source precedence DESC, observed_at DESC, created_at DESC, bar_revision_id). A
   revision beaten earlier can never win later because its beater stays visible; so nothing but the winner
   (and its ordering keys) has to be retained.
4. Source closure: the revisions referenced (transitively) by derived winners are read by id under the same
   clocks; a revision visible earlier stays visible, so rows fetched earlier are reused and only unknown ids
   are queried.
5. Integrity count: the delta statement also counts V(c1, k1) in the same snapshot. Retained rows are exactly
   V(c0, k0), so the count must equal the previous count plus the delta rows. A mismatch means a row became
   queryable without any clock crossing (an insert with old clocks that committed after the previous load's
   snapshot; created_at is the insert time, not the commit time) or was removed: that forces a full reload.
   Without this check such a row would be missed forever. With k after the wall clock of the load, a row
   inserted after the load but before k is such a row too (created_at <= k, yet not loaded): the next load
   then reloads fully instead of missing it.

The row order is the full load's `ORDER BY ticker, interval, adjustment_basis, bar_start`: series are kept in
the database's order and, inside a series, by bar_start; a series that did not exist before forces a full
reload (its place among the database-collated series is not computed here). Source rows follow in discovery
rounds, ordered by bar_revision_id (uuid order equals the order of its canonical text).

A full reload (no reuse) happens when there is no state for the slot, the cutoff or known_by is earlier than the
retained one, either is naive, any part of the identity changed (cohort, tier, ticker set, portal-history
flag, history window, strategy-details flag, session, source generation, manifest identity, members) or the
integrity check or a series check fails. Equal clocks reuse the state (the delta is empty by definition and
the count still guards late commits). Any exception during an incremental load drops the slot, so the next
call reloads. The manifest, members, actions and coverage queries run on every call; only the bar scan is
incremental.

Memory and aliasing
-------------------
At most `max_entries` (default 2) bar sets are retained, one per slot (cohort, tier, portal-history flag and
strategy-details flag; the window, ticker set and so on live in the identity, so a session roll replaces the
slot's entry; a different tier or cohort is a different slot). The retained rows are the very dict objects of
the returned `RealtimeScreeningInput`, so the retention adds only index structures. Measured on 40,434 bars
(`profile_screening_delta_loader.py`): the row dicts cost 2,341 bytes per bar (portal columns), the loader's
index 251 bytes per bar on top (2,592 in total), so nine names (124,236 bars) hold about 0.3 GB and 400 names
(5.5 million) about 14 GB per retained entry; the full load holds the same rows for the length of each cycle.
Because rows are shared between consecutive results, consumers must treat bars as read-only (the existing
consumers copy a bar before changing it).

Cost and indexes (measured on 3 names, 39,450 bars, local PostgreSQL)
---------------------------------------------------------------------
The full load takes 5-7 s, almost all of it shipping and converting 40,419 rows (0.8 s executing, 3.7 s
fetching). A warm delta load takes about 0.3 s whether 0, 5 or 100 bars per ticker are new, because it ships
only the new rows. The database side of the delta statement is two scans of the cohort's partitions: the
integrity count (about 110-160 ms) and the new-row select (about 120 ms). Five single-column indexes on
rt.bar_revisions (observed_at, created_at, bar_end, and partial indexes on replay_available_at and
provider_published_at WHERE NOT NULL) let the planner answer the new-row select with BitmapOr in 7-10 ms, but
the count remains a scan of every candidate row (about 3 microseconds per row), so the loader is O(candidate
rows) on the database side even though it is O(new rows) in rows shipped and Python work. At 400 names the
count alone would be seconds per cycle; removing it needs a writer-side per-cohort commit-ordered sequence
(read `WHERE ingest_seq > last`), or an assumption-based guard (no write transaction in flight at the previous
snapshot and created_at assigned at insert) that this module deliberately does not rely on.

Integration
-----------
`RunnerEnvironment.screening_loader` holds one loader per process; `compute_realtime_ticker_cycle(...,
loader=...)` calls `loader.load(cursor, **scope, include_portal_history=True)` (the scope may carry `known_by`),
and `equity_evidence_runner` passes `known_by=window.deadline`. The loader needs no particular isolation level
(the count is read with the delta, in one statement); the ticker cycle's read-only REPEATABLE READ transaction
is fine. The explicit `allow_late_identity_continuity` scope is forwarded unchanged to the shared input
loader and defaults to false; retained bars never bypass its per-call identity checks. `load` serializes
calls with a lock.
"""
from __future__ import annotations

import threading
import time
from bisect import bisect_left
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime
from itertools import chain
from uuid import UUID

from .screening_projection import RealtimeScreeningInput
from .screening_repository import (
    BAR_IDENTITY_FIELDS,
    BarClocks,
    BarRequest,
    close_source_lineage,
    load_full_bars,
    load_screening_input,
    select_new_bars,
    select_source_bars,
    select_winning_bars,
)

DEFAULT_MAX_ENTRIES = 2


@dataclass(frozen=True, slots=True)
class LoadReport:
    """What the last bar load did. `bar_rows_read` counts rows returned by the bar scan (every winner on a
    full load, only the newly visible revisions on a delta); `source_rows_read` counts source revisions
    fetched by id."""

    mode: str
    reason: str
    bar_rows_read: int
    source_rows_read: int
    retained_bars: int
    seconds: float


class _FullReload(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class _Series:
    """The winners of one (ticker, interval, adjustment_basis), ascending by bar_start."""

    __slots__ = ("starts", "rows", "ranks")

    def __init__(self) -> None:
        self.starts: list[datetime] = []
        self.rows: list[dict] = []
        self.ranks: list[int] = []


def _group(row: dict) -> tuple:
    return row["ticker"], row["interval"], row["adjustment_basis"]


def _outranks(rank: int, row: dict, other_rank: int, other: dict) -> bool:
    """True when `row` precedes `other` in the full load's winner order."""
    if rank != other_rank:
        return rank > other_rank
    if row["observed_at"] != other["observed_at"]:
        return row["observed_at"] > other["observed_at"]
    if row["created_at"] != other["created_at"]:
        return row["created_at"] > other["created_at"]
    return str(row["bar_revision_id"]) < str(other["bar_revision_id"])


class _RetainedBars:
    """The winners visible under `clocks`, their source index and the source closure of the last result."""

    __slots__ = ("identity", "clocks", "visible_total", "group_order", "series", "winner_ids", "source_refs",
                 "unresolved", "closure", "winner_count", "_flat")

    def __init__(self, identity: tuple, clocks: BarClocks, visible_total: int) -> None:
        self.identity = identity
        self.clocks = clocks
        self.visible_total = visible_total
        self.group_order: list[tuple] = []
        self.series: dict[tuple, _Series] = {}
        self.winner_ids: set[str] = set()
        # Source id -> number of retained winners referencing it; `unresolved` is the referenced ids that
        # are not retained winners (the first closure round), kept incrementally.
        self.source_refs: dict[str, int] = {}
        self.unresolved: set[str] = set()
        self.closure: dict[str, dict] = {}
        self.winner_count = 0
        self._flat: tuple | None = None

    @classmethod
    def build(cls, request: BarRequest, ranked: list, visible_total: int) -> "_RetainedBars":
        state = cls(request.identity, request.clocks, visible_total)
        for row, rank in ranked:
            group = _group(row)
            series = state.series.get(group)
            if series is None:
                series = state.series[group] = _Series()
                state.group_order.append(group)
            series.starts.append(row["bar_start"])
            series.rows.append(row)
            series.ranks.append(rank)
            state._register(row)
        state.winner_count = len(ranked)
        return state

    def _register(self, row: dict) -> None:
        revision = str(row["bar_revision_id"])
        self.winner_ids.add(revision)
        self.unresolved.discard(revision)
        for source in row["source_revision_ids"]:
            source = str(source)
            count = self.source_refs.get(source, 0)
            self.source_refs[source] = count + 1
            if count == 0 and source not in self.winner_ids:
                self.unresolved.add(source)

    def _unregister(self, row: dict) -> None:
        revision = str(row["bar_revision_id"])
        self.winner_ids.discard(revision)
        if revision in self.source_refs:
            self.unresolved.add(revision)
        for source in row["source_revision_ids"]:
            source = str(source)
            count = self.source_refs[source] - 1
            if count:
                self.source_refs[source] = count
            else:
                del self.source_refs[source]
                self.unresolved.discard(source)

    def check_mergeable(self, new_rows: list) -> None:
        seen: set[str] = set()
        for row, _ in new_rows:
            if _group(row) not in self.series:
                raise _FullReload("NEW_SERIES")
            revision = str(row["bar_revision_id"])
            if revision in self.winner_ids or revision in seen:
                raise _FullReload("DUPLICATE_REVISION_ID")
            seen.add(revision)

    def merge(self, new_rows: list) -> None:
        # A revision beaten earlier can never win later: every visibility clock is an immutable column compared
        # with a clock that only advances, so visible(c0, k0) is a subset of visible(c1, k1) and the beater
        # stays visible. The winner under (c1, k1) is therefore the better of the retained winner (the best of
        # visible(c0, k0)) and the best new row, so nothing but the winner and its ordering keys is retained.
        for row, rank in new_rows:
            series = self.series[_group(row)]
            start = row["bar_start"]
            index = bisect_left(series.starts, start)
            if index < len(series.starts) and series.starts[index] == start:
                if _outranks(rank, row, series.ranks[index], series.rows[index]):
                    self._unregister(series.rows[index])
                    series.rows[index], series.ranks[index] = row, rank
                    self._register(row)
            else:
                series.starts.insert(index, start)
                series.rows.insert(index, row)
                series.ranks.insert(index, rank)
                self._register(row)
                self.winner_count += 1
        if new_rows:
            self._flat = None

    def winners(self) -> tuple:
        if self._flat is None:
            self._flat = tuple(chain.from_iterable(self.series[group].rows for group in self.group_order))
        return self._flat

    def resolve_sources(self, cursor, request: BarRequest) -> int:
        """Closes the source lineage against the merged winners; returns the number of rows fetched."""
        previous, fetched = self.closure, 0

        def fetch(revision_ids: list[str]) -> list[dict]:
            nonlocal fetched
            absent = [revision for revision in revision_ids if revision not in previous]
            rows = select_source_bars(cursor, request.scope, absent, request.clocks) if absent else []
            fetched += len(rows)
            rows = [*rows, *(previous[revision] for revision in revision_ids if revision in previous)]
            rows.sort(key=lambda row: str(row["bar_revision_id"]))
            return rows

        tail = close_source_lineage(self.unresolved, self.winner_ids, fetch)
        self.closure = {str(row["bar_revision_id"]): row for row in tail}
        return fetched

    def result(self) -> tuple:
        return self.winners() + tuple(self.closure.values()) if self.closure else self.winners()

    def check_invariants(self) -> None:
        """Recomputes the incremental indexes from the winners (used by tests)."""
        rows = [row for group in self.group_order for row in self.series[group].rows]
        refs: dict[str, int] = {}
        for row in rows:
            for source in row["source_revision_ids"]:
                refs[str(source)] = refs.get(str(source), 0) + 1
        ids = {str(row["bar_revision_id"]) for row in rows}
        assert self.winner_ids == ids and len(rows) == self.winner_count
        assert self.source_refs == refs
        assert self.unresolved == {source for source in refs if source not in ids}
        for group in self.group_order:
            starts = self.series[group].starts
            assert starts == sorted(starts) and len(set(starts)) == len(starts)
            assert starts == [row["bar_start"] for row in self.series[group].rows]


class ScreeningInputLoader:
    """Process-local incremental replacement for `load_realtime_screening_input` (same keyword scope, same
    result). Create one per process; `reset()` forgets everything."""

    def __init__(self, *, max_entries: int = DEFAULT_MAX_ENTRIES) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be positive")
        self._max_entries = max_entries
        self._entries: OrderedDict[tuple, _RetainedBars] = OrderedDict()
        self._lock = threading.Lock()
        self.last_report: LoadReport | None = None

    def load(self, cursor, *, cohort_id: UUID, session: date, cutoff: datetime, source_generation: int,
             include_portal_history: bool = False,
             include_strategy_action_details: bool = False,
             known_by: datetime | None = None,
             allow_late_identity_continuity: bool = False) -> RealtimeScreeningInput:
        with self._lock:
            return load_screening_input(
                cursor, cohort_id=cohort_id, session=session, cutoff=cutoff, source_generation=source_generation,
                include_portal_history=include_portal_history,
                include_strategy_action_details=include_strategy_action_details,
                known_by=known_by, bar_loader=self._load_bars,
                allow_late_identity_continuity=allow_late_identity_continuity,
            )

    def reset(self) -> None:
        with self._lock:
            self._entries.clear()
            self.last_report = None

    @property
    def retained_bars(self) -> int:
        """Winners currently retained across all entries (the memory driver)."""
        return sum(state.winner_count for state in self._entries.values())

    def _load_bars(self, cursor, request: BarRequest) -> tuple:
        started = time.perf_counter()
        state = self._entries.get(request.slot)
        reason = self._reuse_blocker(state, request)
        if reason is None:
            try:
                bars, bar_rows, source_rows = self._advance(state, cursor, request)
                self._remember(request.slot, state)
                self.last_report = LoadReport(
                    "delta", "", bar_rows, source_rows, state.winner_count, time.perf_counter() - started)
                return bars
            except _FullReload as signal:
                reason = signal.reason
            except BaseException:
                self._entries.pop(request.slot, None)
                raise
        self._entries.pop(request.slot, None)
        bars, bar_rows, source_rows, retained = self._rebuild(cursor, request)
        self.last_report = LoadReport("full", reason, bar_rows, source_rows, retained, time.perf_counter() - started)
        return bars

    @staticmethod
    def _reuse_blocker(state: _RetainedBars | None, request: BarRequest) -> str | None:
        if state is None:
            return "NO_STATE"
        if state.identity != request.identity:
            for name, old, new in zip(BAR_IDENTITY_FIELDS, state.identity, request.identity):
                if old != new:
                    return f"{name.upper()}_CHANGED"
        if (request.cutoff.tzinfo is None or request.cutoff.utcoffset() is None
                or request.known_by.tzinfo is None or request.known_by.utcoffset() is None):
            return "NAIVE_CUTOFF"
        if request.cutoff < state.clocks.cutoff:
            return "CUTOFF_REGRESSED"
        if request.known_by < state.clocks.known_by:
            return "KNOWN_BY_REGRESSED"
        return None

    def _advance(self, state: _RetainedBars, cursor, request: BarRequest) -> tuple[tuple, int, int]:
        new_rows, visible_total = select_new_bars(cursor, request.scope, state.clocks, request.clocks)
        if visible_total != state.visible_total + len(new_rows):
            raise _FullReload("VISIBLE_SET_DRIFT")
        state.check_mergeable(new_rows)
        state.merge(new_rows)
        state.clocks, state.visible_total = request.clocks, visible_total
        source_rows = state.resolve_sources(cursor, request)
        return state.result(), len(new_rows), source_rows

    def _rebuild(self, cursor, request: BarRequest) -> tuple[tuple, int, int, int]:
        if (request.cutoff.tzinfo is None or request.cutoff.utcoffset() is None
                or request.known_by.tzinfo is None or request.known_by.utcoffset() is None):
            # The caller rejects a naive cutoff after loading; answer exactly as the full load does.
            return load_full_bars(cursor, request), 0, 0, 0
        ranked, visible_total = select_winning_bars(cursor, request.scope, request.clocks, with_state=True)
        if len({str(row["bar_revision_id"]) for row, _ in ranked}) != len(ranked):
            # A repeated revision id is collapsed by the full load; not retainable, so read it as the full
            # load does and keep no state.
            return load_full_bars(cursor, request), len(ranked), 0, 0
        state = _RetainedBars.build(request, ranked, visible_total)
        source_rows = state.resolve_sources(cursor, request)
        self._remember(request.slot, state)
        return state.result(), len(ranked), source_rows, state.winner_count

    def _remember(self, slot: tuple, state: _RetainedBars) -> None:
        self._entries[slot] = state
        self._entries.move_to_end(slot)
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)
