from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import NamedTuple
from uuid import UUID

from ..contracts import canonical_sha256
from .clock import to_datetime
from .records import FlowEvent, VolumeCheckpoint


class BaselineKind(str, Enum):
    SESSION_COMPLETE = "SESSION_COMPLETE"
    SNAPSHOT_RECONCILED = "SNAPSHOT_RECONCILED"
    WARMING = "WARMING"
    INCOMPLETE = "INCOMPLETE"


COMPLETE_BASELINES = frozenset({BaselineKind.SESSION_COMPLETE, BaselineKind.SNAPSHOT_RECONCILED})


class PrintStatus(str, Enum):
    COUNTED = "COUNTED"
    COVERED_BY_SNAPSHOT = "COVERED_BY_SNAPSHOT"
    DUPLICATE = "DUPLICATE"
    CONFLICT = "CONFLICT"
    BEYOND_HISTORY = "BEYOND_HISTORY"


@dataclass(frozen=True, slots=True)
class VolumeConfig:
    history_per_contract: int = 512
    initial_baseline: BaselineKind = BaselineKind.WARMING

    def __post_init__(self) -> None:
        if self.history_per_contract < 1:
            raise ValueError("history_per_contract must be positive")
        if self.initial_baseline not in (BaselineKind.WARMING, BaselineKind.SESSION_COMPLETE):
            raise ValueError("initial_baseline must be WARMING or SESSION_COMPLETE")


class PrintRecord:
    __slots__ = ("seq", "time_ms", "exchange", "price", "size", "counted", "covered", "active",
                 "is_cancel", "orig_seq", "orig_size", "event")

    def __init__(self, seq: int, time_ms: int, exchange: int | None, price: float, size: int,
                 counted: bool, covered: bool, is_cancel: bool = False) -> None:
        self.seq = seq
        self.time_ms = time_ms
        self.exchange = exchange
        self.price = price
        self.size = size
        self.counted = counted
        self.covered = covered
        self.active = not is_cancel
        self.is_cancel = is_cancel
        self.orig_seq: int | None = None
        self.orig_size: int | None = None
        self.event: FlowEvent | None = None


class ContractVolume:
    __slots__ = ("contract", "session_date", "cumulative", "baseline", "reason", "max_seq",
                 "last_time_ms", "snapshot_volume", "snapshot_watermark", "history", "order",
                 "evicted_max_seq", "gap_end_ms")

    def __init__(self, contract: str, session_date: date, baseline: BaselineKind, reason: str | None,
                 gap_end_ms: int | None = None) -> None:
        self.contract = contract
        self.session_date = session_date
        self.cumulative = 0
        self.baseline = baseline
        self.reason = reason
        self.gap_end_ms = gap_end_ms
        self.max_seq = -1
        self.last_time_ms: int | None = None
        self.snapshot_volume: int | None = None
        self.snapshot_watermark: int | None = None
        self.history: dict[tuple[int, int, bool], PrintRecord] = {}
        self.order: deque[tuple[int, int, bool]] = deque()
        self.evicted_max_seq = -1


class PrintOutcome(NamedTuple):
    status: PrintStatus
    before: int
    after: int
    baseline: BaselineKind
    late: bool
    record: PrintRecord | None


class CancelOutcome(NamedTuple):
    duplicate: bool
    matched: PrintRecord | None
    before: int
    after: int
    baseline: BaselineKind
    reason: str | None


class ReconcileOutcome(NamedTuple):
    accepted: bool
    reason: str | None
    cumulative: int
    baseline: BaselineKind
    previous_cumulative: int


class VolumeBook:
    """Cumulative eligible volume per (session, contract).

    Every eligible print is added before any size or premium filter runs. The cumulative is
    trusted only while the baseline is complete:

    - WARMING: started mid-session; counts streamed prints only.
    - SNAPSHOT_RECONCILED: set to the REST snapshot volume at one watermark sequence plus the
      streamed prints after it. A print at or below the watermark is already inside the
      snapshot and is never added again.
    - SESSION_COMPLETE: the stream covered the whole session without a gap.
    - INCOMPLETE: a gap, an unreadable print or an unresolvable cancel; trusted again only
      after a new reconcile.
    """

    def __init__(self, cohort_id: UUID, config: VolumeConfig | None = None) -> None:
        self._cohort_id = cohort_id
        self._config = config or VolumeConfig()
        self._sessions: dict[date, dict[str, ContractVolume]] = {}
        self._new_floor: dict[date, tuple[str, int]] = {}
        self._dirty: dict[tuple[date, str], ContractVolume] = {}
        self._cache_date: date | None = None
        self._cache: dict[str, ContractVolume] = {}

    def _contracts(self, session_date: date) -> dict[str, ContractVolume]:
        if session_date == self._cache_date:
            return self._cache
        contracts = self._sessions.get(session_date)
        if contracts is None:
            contracts = self._sessions[session_date] = {}
        self._cache_date = session_date
        self._cache = contracts
        return contracts

    def _contract(self, contract: str, session_date: date) -> ContractVolume:
        contracts = self._contracts(session_date)
        cv = contracts.get(contract)
        if cv is None:
            floor = self._new_floor.get(session_date)
            if floor is None:
                cv = ContractVolume(contract, session_date, self._config.initial_baseline, None)
            else:
                cv = ContractVolume(contract, session_date, BaselineKind.INCOMPLETE, floor[0], floor[1])
            contracts[contract] = cv
        return cv

    def get(self, contract: str, session_date: date) -> ContractVolume | None:
        return self._sessions.get(session_date, {}).get(contract)

    def add_print(self, contract: str, session_date: date, seq: int, time_ms: int,
                  exchange: int | None, price: float, size: int) -> PrintOutcome:
        cv = self._contract(contract, session_date)
        key = (seq, time_ms, False)
        existing = cv.history.get(key)
        if existing is not None:
            if existing.exchange == exchange and existing.price == price and existing.size == size:
                return PrintOutcome(PrintStatus.DUPLICATE, cv.cumulative, cv.cumulative, cv.baseline, False, existing)
            self._mark(cv, "DUPLICATE_SEQUENCE_CONFLICT", time_ms)
            return PrintOutcome(PrintStatus.CONFLICT, cv.cumulative, cv.cumulative, cv.baseline, False, existing)
        if seq <= cv.evicted_max_seq:
            self._mark(cv, "SEQUENCE_BEYOND_RETAINED_HISTORY", time_ms)
            return PrintOutcome(PrintStatus.BEYOND_HISTORY, cv.cumulative, cv.cumulative, cv.baseline, False, None)
        late = seq < cv.max_seq
        watermark = cv.snapshot_watermark
        covered = watermark is not None and seq <= watermark
        before = cv.cumulative
        if covered:
            status = PrintStatus.COVERED_BY_SNAPSHOT
        else:
            cv.cumulative = before + size
            status = PrintStatus.COUNTED
        record = PrintRecord(seq, time_ms, exchange, price, size, not covered, covered)
        self._remember(cv, key, record)
        if seq > cv.max_seq:
            cv.max_seq = seq
        if cv.last_time_ms is None or time_ms > cv.last_time_ms:
            cv.last_time_ms = time_ms
        self._dirty[(session_date, contract)] = cv
        return PrintOutcome(status, before, cv.cumulative, cv.baseline, late, record)

    def cancel_print(self, contract: str, session_date: date, seq: int, time_ms: int,
                     exchange: int | None, price: float, size: int) -> CancelOutcome:
        """Apply a cancel. It echoes the cancelled print's exchange, price and size (to be
        confirmed against a capture) and is matched to the newest live print that agrees."""
        cv = self._contract(contract, session_date)
        key = (seq, time_ms, True)
        if key in cv.history:
            return CancelOutcome(True, None, cv.cumulative, cv.cumulative, cv.baseline, None)
        watermark = cv.snapshot_watermark
        cancel_covered = watermark is not None and seq <= watermark
        original: PrintRecord | None = None
        for original_key in reversed(cv.order):
            candidate = cv.history[original_key]
            if (not candidate.is_cancel and candidate.active and candidate.size == size
                    and candidate.price == price and candidate.exchange == exchange
                    and candidate.time_ms <= time_ms):
                original = candidate
                break
        before = cv.cumulative
        reason: str | None = None
        record = PrintRecord(seq, time_ms, exchange, price, size, False, cancel_covered, is_cancel=True)
        if original is not None:
            original.active = False
            record.orig_seq = original.seq
            record.orig_size = original.size
            if original.counted or (original.covered and not cancel_covered):
                cv.cumulative = max(0, cv.cumulative - original.size)
        elif not cancel_covered:
            reason = "CANCEL_ORIGINAL_NOT_FOUND"
            if cv.baseline in COMPLETE_BASELINES:
                self._mark(cv, reason, time_ms)
        self._remember(cv, key, record)
        if seq > cv.max_seq:
            cv.max_seq = seq
        if cv.last_time_ms is None or time_ms > cv.last_time_ms:
            cv.last_time_ms = time_ms
        self._dirty[(session_date, contract)] = cv
        return CancelOutcome(False, original, before, cv.cumulative, cv.baseline, reason)

    def _remember(self, cv: ContractVolume, key: tuple[int, int, bool], record: PrintRecord) -> None:
        cv.history[key] = record
        cv.order.append(key)
        if len(cv.order) > self._config.history_per_contract:
            evicted = cv.history.pop(cv.order.popleft())
            if evicted.seq > cv.evicted_max_seq:
                cv.evicted_max_seq = evicted.seq

    def _mark(self, cv: ContractVolume, reason: str, gap_end_ms: int | None = None) -> None:
        cv.baseline = BaselineKind.INCOMPLETE
        cv.reason = reason
        if gap_end_ms is not None and (cv.gap_end_ms is None or gap_end_ms > cv.gap_end_ms):
            cv.gap_end_ms = gap_end_ms
        self._dirty[(cv.session_date, cv.contract)] = cv

    def mark_incomplete(self, contract: str, session_date: date, reason: str,
                        gap_end_ms: int | None = None) -> None:
        self._mark(self._contract(contract, session_date), reason, gap_end_ms)

    def mark_all_incomplete(self, session_date: date, reason: str, gap_end_ms: int) -> int:
        """A gap whose affected contracts are unknown: every contract, including those not yet
        seen this session, loses its complete baseline."""
        previous = self._new_floor.get(session_date)
        self._new_floor[session_date] = (reason, max(gap_end_ms, previous[1]) if previous else gap_end_ms)
        contracts = self._contracts(session_date)
        for cv in contracts.values():
            self._mark(cv, reason, gap_end_ms)
        return len(contracts)

    def reconcile(self, contract: str, session_date: date, snapshot_volume: int,
                  watermark_seq: int, snapshot_time_ms: int) -> ReconcileOutcome:
        """Set the cumulative to the snapshot volume plus the streamed prints after the watermark.

        The watermark is the sequence of the last print the snapshot includes and the snapshot
        time is its event time. It is rejected, leaving state untouched, when the snapshot
        predates a recorded gap (prints in the gap would be missing from both sides) or when
        the retained history cannot prove which streamed prints come after the watermark.
        """
        if type(snapshot_volume) is not int or snapshot_volume < 0:
            raise ValueError("snapshot_volume must be a nonnegative integer")
        if type(watermark_seq) is not int or watermark_seq < 0:
            raise ValueError("watermark_seq must be a nonnegative integer")
        if type(snapshot_time_ms) is not int or snapshot_time_ms < 0:
            raise ValueError("snapshot_time_ms must be a nonnegative integer")
        cv = self._contract(contract, session_date)
        previous = cv.cumulative

        def rejected(reason: str) -> ReconcileOutcome:
            return ReconcileOutcome(False, reason, cv.cumulative, cv.baseline, previous)

        if cv.gap_end_ms is not None and snapshot_time_ms < cv.gap_end_ms:
            return rejected("RECONCILE_SNAPSHOT_PREDATES_GAP")
        if cv.snapshot_watermark is not None and watermark_seq < cv.snapshot_watermark:
            return rejected("RECONCILE_WATERMARK_OLDER_THAN_PREVIOUS")
        if cv.evicted_max_seq > watermark_seq:
            return rejected("RECONCILE_HISTORY_EVICTED")
        streamed = 0
        for record in cv.history.values():
            if record.is_cancel:
                if record.seq > watermark_seq:
                    if record.orig_seq is None:
                        return rejected("RECONCILE_UNMATCHED_CANCEL")
                    if record.orig_seq <= watermark_seq:
                        streamed -= record.orig_size or 0
            elif record.seq > watermark_seq and record.active:
                streamed += record.size
        for record in cv.history.values():
            record.covered = record.seq <= watermark_seq
            record.counted = (not record.is_cancel) and record.active and record.seq > watermark_seq
        cv.cumulative = max(0, snapshot_volume + streamed)
        cv.baseline = BaselineKind.SNAPSHOT_RECONCILED
        cv.reason = None
        cv.snapshot_volume = snapshot_volume
        cv.snapshot_watermark = watermark_seq
        self._dirty[(session_date, contract)] = cv
        return ReconcileOutcome(True, None, cv.cumulative, cv.baseline, previous)

    def checkpoint_rows(self, *, dirty_only: bool = True) -> list[VolumeCheckpoint]:
        if dirty_only:
            selected = list(self._dirty.values())
            self._dirty.clear()
        else:
            selected = [cv for contracts in self._sessions.values() for cv in contracts.values()]
        rows = [
            VolumeCheckpoint(
                cohort_id=self._cohort_id,
                session_date=cv.session_date,
                contract_ticker=cv.contract,
                cumulative_volume=cv.cumulative,
                last_source_seq=max(cv.max_seq, 0),
                last_event_time=None if cv.last_time_ms is None else to_datetime(cv.last_time_ms),
                baseline_kind=cv.baseline.value,
                snapshot_volume=cv.snapshot_volume,
                snapshot_watermark_seq=cv.snapshot_watermark,
            )
            for cv in selected
        ]
        rows.sort(key=lambda row: (row.session_date, row.contract_ticker))
        return rows

    def dirty_count(self) -> int:
        return len(self._dirty)

    def contract_count(self) -> int:
        return sum(len(contracts) for contracts in self._sessions.values())

    def drop_sessions_before(self, session_date: date) -> None:
        for stale in [value for value in self._sessions if value < session_date]:
            del self._sessions[stale]
            self._new_floor.pop(stale, None)
        self._cache_date = None
        self._cache = {}
        self._dirty = {key: cv for key, cv in self._dirty.items() if key[0] >= session_date}

    def digest(self) -> str:
        return canonical_sha256([
            [cv.session_date, cv.contract, cv.cumulative, cv.baseline, cv.reason, cv.max_seq,
             cv.snapshot_volume, cv.snapshot_watermark]
            for _, contracts in sorted(self._sessions.items())
            for _, cv in sorted(contracts.items())
        ])
