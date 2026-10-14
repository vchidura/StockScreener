"""Session archive worker: archive, verify and record alert, evaluation, outcome and option
flow files, then drop an ARCHIVE_FIRST partition only after every session in it is
verifiably archived for every kind its partition policy requires.

Archiving runs beside ingestion and alert generation and never gates them. A failure in any
session or kind becomes an alarm (a structured value plus a log record), the data stays
online, and the remaining sessions and kinds still run. Detach and drop run client-side in
autocommit through the repository, never inside a transaction or a database function.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping, Protocol, Sequence

from .relation_registry import RELATIONS
from .session_archive import (
    ALERT_KINDS,
    KEEP,
    OUTCOMES,
    PIN_ARCHIVE_REFRESH_FAILED,
    PIN_COVERAGE_UNKNOWN,
    PIN_LOOKUP_FAILED,
    PIN_OPEN_HORIZON,
    PIN_OUTCOME_UNVERIFIED,
    PIN_ROWS_OUTSIDE_SESSIONS,
    SESSION_KINDS,
    ArchiveIdentity,
    ArchiveRecord,
    ArchiveRow,
    ArchiveVerificationError,
    DropDecision,
    DropReason,
    PartitionRange,
    Pin,
    archive_directory,
    build_archive,
    decide_partition_drop,
    verify_archive,
    write_archive,
)

LOGGER = logging.getLogger(__name__)

ALARM_SOURCE_READ_FAILED = "SOURCE_READ_FAILED"
ALARM_BUILD_FAILED = "ARCHIVE_BUILD_FAILED"
ALARM_WRITE_FAILED = "ARCHIVE_WRITE_FAILED"
ALARM_VERIFICATION_FAILED = "ARCHIVE_VERIFICATION_FAILED"
ALARM_LEDGER_FAILED = "LEDGER_RECORD_FAILED"
ALARM_PIN_LOOKUP_FAILED = "PIN_LOOKUP_FAILED"
ALARM_PARTITION_LOOKUP_FAILED = "PARTITION_LOOKUP_FAILED"
ALARM_PARTITION_POLICY_UNSUPPORTED = "PARTITION_POLICY_UNSUPPORTED"
ALARM_PARTITION_ROWS_OUTSIDE_SESSIONS = "PARTITION_ROWS_OUTSIDE_SESSIONS"
ALARM_PARTITION_DROP_FAILED = "PARTITION_DROP_FAILED"
ALARM_PARTITION_HEALTH = "PARTITION_HEALTH_ALARM"
ALARM_PARTITION_HEALTH_UNAVAILABLE = "PARTITION_HEALTH_UNAVAILABLE"

STATUS_ARCHIVED = "ARCHIVED"
STATUS_ALREADY_ARCHIVED = "ALREADY_ARCHIVED"
STATUS_FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class AlertHorizon:
    """An alert of a session and the ET date its horizon closed (None while still open)."""

    alert_id: str
    horizon_close_date: date | None


@dataclass(frozen=True, slots=True)
class PartitionInfo:
    relation: str
    name: str
    lower: date
    upper: date
    bound_expression: str
    detach_pending: bool = False

    def as_range(self) -> PartitionRange:
        return PartitionRange(self.relation, self.lower, self.upper)


@dataclass(frozen=True, slots=True)
class PartitionPolicy:
    """A row of rt.partition_policies; archive_kinds are what a drop must find verified."""

    retention_class: str
    window_kind: str
    window_value: int
    requires_archive: bool = False
    archive_kinds: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class PartitionHealth:
    """A row of rt.partition_health(): ALARM when fewer than 7 days of partitions remain."""

    relation_name: str
    scheme: str
    horizon_date: date | None
    days_ahead: int
    status: str


class ArchiveSource(Protocol):
    def read_rows(self, session_date: date, kind: str) -> Sequence[ArchiveRow]: ...

    def count_rows(self, session_date: date, kind: str) -> Mapping[str, int]: ...

    def read_outcomes(self, session_date: date) -> Mapping[date, Sequence[ArchiveRow]]: ...

    def count_outcomes(self, session_date: date, horizon_close_date: date) -> Mapping[str, int]: ...

    def alert_horizons(self, session_date: date) -> Sequence[AlertHorizon]: ...


class ArchiveLedger(Protocol):
    def list_archives(self, session_date: date) -> Sequence[ArchiveRecord]: ...

    def record_archive(self, record: ArchiveRecord) -> bool:
        """Insert a verified record; return False when the same file is already recorded."""


class PartitionAdmin(Protocol):
    def partition_policy(self, relation: str) -> PartitionPolicy:
        """The policy row of the relation; raise when there is none (the drop fails closed)."""

    def partitions(self, relation: str) -> Sequence[PartitionInfo]: ...

    def partition_health(self, as_of_date: date) -> Sequence[PartitionHealth]: ...

    def partition_row_coverage(self, partition: PartitionInfo, sessions: Sequence[date]) -> tuple[int, int]:
        """(rows in the partition, rows whose session date is one of `sessions`)."""

    def sessions_between(self, start: date, end: date) -> Sequence[date]:
        """Exchange sessions in the half-open range [start, end)."""

    def session_window_start(self, as_of_session: date, window_sessions: int) -> date:
        """Oldest session of the most recent `window_sessions` sessions ending at as_of_session."""

    def detach_and_drop(self, partition: PartitionInfo) -> None:
        """DETACH PARTITION ... CONCURRENTLY then DROP, in autocommit."""


class SessionArchiveRepository(ArchiveSource, ArchiveLedger, PartitionAdmin, Protocol):
    pass


@dataclass(frozen=True, slots=True)
class ArchiveAlarm:
    code: str
    detail: str
    session_date: date | None = None
    kind: str | None = None
    horizon_close_date: date | None = None
    relation: str | None = None

    def as_dict(self) -> dict[str, str | None]:
        return {
            "code": self.code,
            "session_date": self.session_date.isoformat() if self.session_date else None,
            "kind": self.kind,
            "horizon_close_date": self.horizon_close_date.isoformat() if self.horizon_close_date else None,
            "relation": self.relation,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class ArchiveOutcome:
    session_date: date
    kind: str
    status: str
    horizon_close_date: date | None = None
    sha256: str | None = None
    row_count: int | None = None
    uri: str | None = None
    revision: bool = False


@dataclass(frozen=True, slots=True)
class SessionArchiveResult:
    session_date: date
    outcomes: tuple[ArchiveOutcome, ...]
    alarms: tuple[ArchiveAlarm, ...]
    pins: tuple[Pin, ...]

    @property
    def ok(self) -> bool:
        return not self.alarms


@dataclass(frozen=True, slots=True)
class ArchiveRunReport:
    sessions: tuple[SessionArchiveResult, ...]

    @property
    def alarms(self) -> tuple[ArchiveAlarm, ...]:
        return tuple(alarm for result in self.sessions for alarm in result.alarms)

    @property
    def ok(self) -> bool:
        return not self.alarms


@dataclass(frozen=True, slots=True)
class PartitionDropResult:
    relation: str
    partition: str
    lower: date
    upper: date
    decision: DropDecision
    dropped: bool
    alarms: tuple[ArchiveAlarm, ...] = ()


@dataclass(frozen=True, slots=True)
class DropReport:
    """`alarms` are run-level (health, policy, lookups); each partition result has its own."""

    partitions: tuple[PartitionDropResult, ...]
    alarms: tuple[ArchiveAlarm, ...] = ()

    @property
    def dropped(self) -> tuple[PartitionDropResult, ...]:
        return tuple(result for result in self.partitions if result.dropped)

    @property
    def all_alarms(self) -> tuple[ArchiveAlarm, ...]:
        return self.alarms + tuple(alarm for result in self.partitions for alarm in result.alarms)

    @property
    def ok(self) -> bool:
        return not self.all_alarms


class _ArchiveFailure(Exception):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


def _describe(error: BaseException) -> str:
    return f"{type(error).__name__}: {str(error)[:200]}"


def _stage(code: str, action: Callable[[], object]):
    try:
        return action()
    except _ArchiveFailure:
        raise
    except ArchiveVerificationError as error:
        raise _ArchiveFailure(code, str(error)[:240]) from error
    except Exception as error:  # noqa: BLE001 - any failure is an alarm, never a crash
        raise _ArchiveFailure(code, _describe(error)) from error


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class _PolicyRefusal(Exception):
    pass


def _required_kinds(policy: PartitionPolicy) -> tuple[str, ...]:
    """The archive kinds a drop needs, or a refusal when the policy is not archive-gated here.

    A policy without requires_archive is ordinary retention (HOT_TTL, KEPT_3M, ...): this
    worker evaluates only session windows and would otherwise find nothing to verify and
    drop the partition on an empty requirement.
    """
    if policy.retention_class != "ARCHIVE_FIRST" or not policy.requires_archive or not policy.archive_kinds:
        raise _PolicyRefusal("partition policy does not require an archive")
    unknown = sorted(set(policy.archive_kinds) - set(SESSION_KINDS))
    if unknown:
        raise _PolicyRefusal(f"partition policy requires archive kinds this worker cannot write: {unknown}")
    if policy.window_kind != "SESSIONS":
        raise _PolicyRefusal(f"unsupported window kind {policy.window_kind}")
    return tuple(policy.archive_kinds)


class SessionArchiveWorker:
    def __init__(
        self,
        repository: SessionArchiveRepository,
        archive_root: Path | str,
        identity: ArchiveIdentity,
        *,
        kinds: Sequence[str] = SESSION_KINDS,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        root = Path(archive_root)
        if not root.is_absolute():
            raise ValueError("archive_root must be an absolute path outside the repository")
        if not kinds or set(kinds) - set(SESSION_KINDS):
            raise ValueError("kinds must be a nonempty subset of the session archive kinds")
        self._repository = repository
        self._root = root
        self._identity = identity
        self._kinds = tuple(kinds)
        self._clock = clock

    def archive_sessions(
        self, session_dates: Iterable[date], kinds: Sequence[str] | None = None,
    ) -> ArchiveRunReport:
        return ArchiveRunReport(tuple(
            self.archive_session(value, kinds) for value in sorted(set(session_dates))))

    def archive_session(
        self, session_date: date, kinds: Sequence[str] | None = None,
    ) -> SessionArchiveResult:
        """Archive the selected kinds of one session (default: the worker's kinds).

        Outcome addenda and alert pins apply only when an alert kind is selected.
        """
        selected = tuple(kinds) if kinds is not None else self._kinds
        outcomes: list[ArchiveOutcome] = []
        alarms: list[ArchiveAlarm] = []
        for kind in selected:
            outcomes.append(self._archive_kind(
                session_date, kind, None, alarms,
                rows=lambda kind=kind: self._repository.read_rows(session_date, kind),
                counts=lambda kind=kind: self._repository.count_rows(session_date, kind),
            ))
        pins = [
            Pin(session_date, PIN_ARCHIVE_REFRESH_FAILED, outcome.kind)
            for outcome in outcomes if outcome.status == STATUS_FAILED
        ]
        if any(kind in ALERT_KINDS for kind in selected):
            verified_horizons = self._archive_outcomes(session_date, outcomes, alarms)
            pins.extend(self._pins(session_date, verified_horizons, alarms))
        return SessionArchiveResult(session_date, tuple(outcomes), tuple(alarms), tuple(pins))

    def check_partition_health(self, health_date: date | None = None) -> tuple[ArchiveAlarm, ...]:
        """One alarm per rt.partition_health() row that is not OK. Never raises."""
        try:
            day = health_date or self._clock().astimezone(timezone.utc).date()
            rows = tuple(self._repository.partition_health(day))
            return tuple(
                self._alarm(
                    ALARM_PARTITION_HEALTH,
                    f"{row.status}: {row.scheme} partitions end {row.horizon_date}, {row.days_ahead} days ahead",
                    relation=row.relation_name,
                )
                for row in rows if row.status != "OK"
            )
        except Exception as error:  # noqa: BLE001 - a health check must never break its caller
            return (self._alarm(ALARM_PARTITION_HEALTH_UNAVAILABLE, _describe(error)),)

    def drop_archived_partitions(
        self, *, as_of_session: date, relations: Sequence[str], health_date: date | None = None,
    ) -> DropReport:
        """Drop each partition that is past the online window, fully archived and unpinned.

        `relations` is explicit and each must be a partitioned ARCHIVE_FIRST relation. Its
        rt.partition_policies row decides what must be verified for every session in a
        partition (archive_kinds); a missing policy row, or one that does not require an
        archive, is refused and alarmed instead of dropped. Each candidate session is
        archived and verified again first, so a stale or missing file blocks the drop.
        Partition health (rt.partition_health) is checked first and its alarms are returned.
        """
        for relation in relations:
            spec = RELATIONS.get(relation)
            if spec is None or spec.retention != "ARCHIVE_FIRST" or not spec.partitioned:
                raise ValueError(f"{relation} is not a partitioned ARCHIVE_FIRST relation")
        results: list[PartitionDropResult] = []
        alarms: list[ArchiveAlarm] = list(self.check_partition_health(health_date))
        for relation in relations:
            try:
                policy = self._repository.partition_policy(relation)
                required = _required_kinds(policy)
                window_start = self._repository.session_window_start(as_of_session, policy.window_value)
                partitions = sorted(self._repository.partitions(relation), key=lambda item: item.lower)
            except _PolicyRefusal as refusal:
                alarms.append(self._alarm(ALARM_PARTITION_POLICY_UNSUPPORTED, str(refusal), relation=relation))
                continue
            except Exception as error:  # noqa: BLE001
                alarms.append(self._alarm(ALARM_PARTITION_LOOKUP_FAILED, _describe(error), relation=relation))
                continue
            for partition in partitions:
                if partition.upper > window_start:
                    continue
                results.append(self._consider_partition(partition, window_start, required))
        return DropReport(tuple(results), tuple(alarms))

    def _consider_partition(
        self, partition: PartitionInfo, window_start: date, required_kinds: tuple[str, ...],
    ) -> PartitionDropResult:
        try:
            sessions = tuple(self._repository.sessions_between(partition.lower, partition.upper))
            session_results = [self.archive_session(value, required_kinds) for value in sessions]
            records = [record for value in sessions for record in self._repository.list_archives(value)]
        except Exception as error:  # noqa: BLE001
            alarm = self._alarm(ALARM_PARTITION_LOOKUP_FAILED, _describe(error), relation=partition.relation)
            decision = DropDecision(KEEP, (DropReason(ALARM_PARTITION_LOOKUP_FAILED),))
            return PartitionDropResult(partition.relation, partition.name, partition.lower, partition.upper,
                                       decision, False, (alarm,))
        alarms = [alarm for result in session_results for alarm in result.alarms]
        pins = [pin for result in session_results for pin in result.pins]
        pins.extend(self._coverage_pins(partition, sessions, alarms))
        decision = decide_partition_drop(
            partition.as_range(), sessions, records, pins,
            required_kinds=required_kinds, window_start=window_start,
        )
        if not decision.drop:
            return PartitionDropResult(partition.relation, partition.name, partition.lower, partition.upper,
                                       decision, False, tuple(alarms))
        try:
            self._repository.detach_and_drop(partition)
        except Exception as error:  # noqa: BLE001
            alarms.append(self._alarm(ALARM_PARTITION_DROP_FAILED, _describe(error),
                                      relation=f"{partition.relation}/{partition.name}"))
            return PartitionDropResult(partition.relation, partition.name, partition.lower, partition.upper,
                                       decision, False, tuple(alarms))
        LOGGER.info("session_archive_partition_dropped %s", json.dumps(
            {"relation": partition.relation, "partition": partition.name,
             "lower": partition.lower.isoformat(), "upper": partition.upper.isoformat()}, sort_keys=True))
        return PartitionDropResult(partition.relation, partition.name, partition.lower, partition.upper,
                                   decision, True, tuple(alarms))

    def _archive_outcomes(
        self, session_date: date, outcomes: list[ArchiveOutcome], alarms: list[ArchiveAlarm],
    ) -> set[date]:
        try:
            groups = self._repository.read_outcomes(session_date)
        except Exception as error:  # noqa: BLE001
            alarms.append(self._alarm(ALARM_SOURCE_READ_FAILED, _describe(error), session_date, OUTCOMES))
            outcomes.append(ArchiveOutcome(session_date, OUTCOMES, STATUS_FAILED))
            return set()
        verified: set[date] = set()
        for horizon in sorted(groups):
            outcome = self._archive_kind(
                session_date, OUTCOMES, horizon, alarms,
                rows=lambda horizon=horizon: groups[horizon],
                counts=lambda horizon=horizon: self._repository.count_outcomes(session_date, horizon),
            )
            outcomes.append(outcome)
            if outcome.status != STATUS_FAILED:
                verified.add(horizon)
        return verified

    def _coverage_pins(
        self, partition: PartitionInfo, sessions: Sequence[date], alarms: list[ArchiveAlarm],
    ) -> list[Pin]:
        """Rows of the partition that belong to no archived session must keep it online."""
        try:
            total, covered = self._repository.partition_row_coverage(partition, sessions)
        except Exception as error:  # noqa: BLE001
            alarms.append(self._alarm(ALARM_PARTITION_LOOKUP_FAILED, _describe(error), relation=partition.relation))
            return [Pin(partition.lower, PIN_COVERAGE_UNKNOWN, partition.name)]
        if total == covered:
            return []
        alarms.append(self._alarm(
            ALARM_PARTITION_ROWS_OUTSIDE_SESSIONS,
            f"{total - covered} of {total} rows in {partition.name} belong to no session in the partition",
            relation=partition.relation,
        ))
        return [Pin(partition.lower, PIN_ROWS_OUTSIDE_SESSIONS, f"{partition.name}:{total - covered}")]

    def _pins(self, session_date: date, verified_horizons: set[date], alarms: list[ArchiveAlarm]) -> list[Pin]:
        try:
            horizons = sorted(self._repository.alert_horizons(session_date), key=lambda item: item.alert_id)
        except Exception as error:  # noqa: BLE001
            alarms.append(self._alarm(ALARM_PIN_LOOKUP_FAILED, _describe(error), session_date))
            return [Pin(session_date, PIN_LOOKUP_FAILED)]
        pins: list[Pin] = []
        for horizon in horizons:
            if horizon.horizon_close_date is None:
                pins.append(Pin(session_date, PIN_OPEN_HORIZON, horizon.alert_id))
            elif horizon.horizon_close_date not in verified_horizons:
                pins.append(Pin(session_date, PIN_OUTCOME_UNVERIFIED, horizon.alert_id))
        return pins

    def _archive_kind(
        self,
        session_date: date,
        kind: str,
        horizon_close_date: date | None,
        alarms: list[ArchiveAlarm],
        *,
        rows: Callable[[], Sequence[ArchiveRow]],
        counts: Callable[[], Mapping[str, int]],
    ) -> ArchiveOutcome:
        try:
            return self._write_verify_record(session_date, kind, horizon_close_date, rows, counts)
        except _ArchiveFailure as failure:
            alarms.append(self._alarm(failure.code, failure.detail, session_date, kind, horizon_close_date))
            return ArchiveOutcome(session_date, kind, STATUS_FAILED, horizon_close_date)

    def _write_verify_record(
        self,
        session_date: date,
        kind: str,
        horizon_close_date: date | None,
        rows: Callable[[], Sequence[ArchiveRow]],
        counts: Callable[[], Mapping[str, int]],
    ) -> ArchiveOutcome:
        source_rows = _stage(ALARM_SOURCE_READ_FAILED, rows)
        built = _stage(ALARM_BUILD_FAILED, lambda: build_archive(
            kind, session_date, source_rows, horizon_close_date=horizon_close_date))
        written = _stage(ALARM_WRITE_FAILED, lambda: write_archive(
            archive_directory(self._root, session_date), built, self._identity))
        # A fresh COUNT after the write: the file must still match what is stored now.
        source_counts = _stage(ALARM_SOURCE_READ_FAILED, counts)
        verified = _stage(ALARM_VERIFICATION_FAILED, lambda: verify_archive(
            written.data_path, written.manifest_path, kind=kind, session_date=session_date,
            horizon_close_date=horizon_close_date, source_counts=source_counts))
        record = ArchiveRecord(
            session_date=session_date, kind=kind, uri=verified.uri, sha256=verified.sha256,
            row_count=verified.row_count, schema_version=verified.schema_version,
            build_id=verified.build_id, verified_at=self._clock(), horizon_close_date=horizon_close_date,
        )
        inserted = _stage(ALARM_LEDGER_FAILED, lambda: self._repository.record_archive(record))
        return ArchiveOutcome(
            session_date, kind, STATUS_ARCHIVED if inserted else STATUS_ALREADY_ARCHIVED,
            horizon_close_date, verified.sha256, verified.row_count, verified.uri, written.revision,
        )

    def _alarm(
        self,
        code: str,
        detail: str,
        session_date: date | None = None,
        kind: str | None = None,
        horizon_close_date: date | None = None,
        *,
        relation: str | None = None,
    ) -> ArchiveAlarm:
        alarm = ArchiveAlarm(code, detail, session_date, kind, horizon_close_date, relation)
        LOGGER.error("session_archive_alarm %s", json.dumps(alarm.as_dict(), sort_keys=True))
        return alarm
