"""Ordinary-retention partition executor: drops WHOLE expired partitions of HOT_TTL and KEPT_3M relations.

Owns only the classes the archive worker refuses: a relation is eligible when the relation registry
marks it partitioned with retention HOT_TTL (calendar-day partitions) or KEPT_3M (calendar-month
partitions) and its rt.partition_policies row equals the registry seed (PARTITION_POLICY_SEEDS) and
does not require an archive. HISTORY_PLAN, SESSION_WINDOW, ARCHIVE_FIRST, DEFERRED and BOUNDED
relations are never touched and an unknown relation is refused before any statement runs.

Window rule (calendar based, evaluated on the UTC date because partitions are cut at UTC midnight):
  CALENDAR_DAYS n  cutoff = today - n days.
  MONTHS m         cutoff = today - m calendar months (day clamped to the month length).
A partition is dropped only when its entire range ends at or before the cutoff (upper <= cutoff). One
that straddles the cutoff, lies inside the window, or contains today or later is kept, so a daily
relation keeps each row between n and n + 1 days and a 3 month relation between 3 and 4 months.

A drop reuses the archive repository's DETACH PARTITION ... CONCURRENTLY, then DROP, in autocommit
(a pending detach is finalized). It never deletes rows and never cascades: a partition that other
tables still reference by a foreign key makes PostgreSQL refuse the detach (the partition stays
attached and readable) and the run raises an alarm. A table that was detached but not dropped by an
interrupted run is finished on the next run once it is expired and holds no row at or after its range.

Catalog rows (rt.evidence_catalog, rt.evidence_source_window_catalog, rt.snapshot_catalog for
EPHEMERAL_PORTAL) are pruned after the drops with one DELETE per catalog: rows older than the cutoff
that no NO ACTION or RESTRICT foreign key still references. A declared ON DELETE CASCADE (the current
evidence pointer) acts as the schema defines it.

A failure never blocks: each partition, detached table, policy and catalog is isolated, raises an
ArchiveAlarm (same shape as the archive worker's) and the run continues. A re-run after a partial
failure is safe. The default is a dry run that issues no DDL and no DML.
"""
from __future__ import annotations

import json
import logging
import re
from calendar import monthrange
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable, Iterator, Protocol, Sequence

from psycopg2 import sql

from .relation_registry import PARTITION_POLICY_SEEDS, RELATIONS
from .session_archive_repository import _BOUND_PATTERN, PostgresSessionArchiveRepository
from .session_archive_worker import (
    ALARM_PARTITION_DROP_FAILED,
    ALARM_PARTITION_LOOKUP_FAILED,
    ALARM_PARTITION_POLICY_UNSUPPORTED,
    ArchiveAlarm,
    PartitionInfo,
)

LOGGER = logging.getLogger(__name__)

# retention class -> (partition scheme, window kind)
ELIGIBLE_CLASSES = {"HOT_TTL": ("DAILY", "CALENDAR_DAYS"), "KEPT_3M": ("MONTHLY", "MONTHS")}
ELIGIBLE_RELATIONS = tuple(sorted(
    name for name, spec in RELATIONS.items()
    if spec.partitioned and spec.retention in ELIGIBLE_CLASSES and name in PARTITION_POLICY_SEEDS
))

ALARM_POLICY_LOOKUP_FAILED = "RETENTION_POLICY_LOOKUP_FAILED"
ALARM_PARTITION_REFERENCED = "RETENTION_PARTITION_REFERENCED"
ALARM_DETACHED_DROP_FAILED = "RETENTION_DETACHED_DROP_FAILED"
ALARM_CATALOG_PRUNE_FAILED = "RETENTION_CATALOG_PRUNE_FAILED"

STATUS_DROPPED = "DROPPED"
STATUS_WOULD_DROP = "WOULD_DROP"
STATUS_KEPT = "KEPT"
STATUS_FAILED = "FAILED"
STATUS_PRUNED = "PRUNED"
STATUS_PLANNED = "PLANNED"

REASON_EXPIRED = "EXPIRED"
REASON_ALREADY_DROPPED = "ALREADY_DROPPED"
REASON_CURRENT_OR_FUTURE = "CURRENT_OR_FUTURE"
REASON_STRADDLES_CUTOFF = "STRADDLES_CUTOFF"
REASON_WITHIN_WINDOW = "WITHIN_WINDOW"
REASON_INVALID_RANGE = "INVALID_RANGE"
REASON_DETACHED_EXPIRED = "DETACHED_EXPIRED"
REASON_DETACHED_NOT_EXPIRED = "DETACHED_NOT_EXPIRED"

FOREIGN_KEY_VIOLATION = "23503"
_UTC = timezone.utc


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    """A row of rt.partition_policies."""

    relation: str
    scheme: str
    key_column: str
    key_type: str
    retention_class: str
    window_kind: str
    window_value: int
    requires_archive: bool = False
    catalog_relation: str | None = None


@dataclass(frozen=True, slots=True)
class DetachedTable:
    """A table left behind by a detach whose DROP failed; its range is derived from its name."""

    relation: str
    name: str
    lower: date
    upper: date


@dataclass(frozen=True, slots=True)
class CatalogRule:
    """How a catalog's rows relate to time: rows with time_column < cutoff and the given equalities."""

    time_column: str
    equals: tuple[tuple[str, str], ...] = ()


# The snapshot catalog is shared with the kept (SESSION_WINDOW) snapshots: only EPHEMERAL_PORTAL rows
# describe hot bodies.
CATALOG_RULES = {
    "rt.evidence_catalog": CatalogRule("durable_at"),
    "rt.evidence_source_window_catalog": CatalogRule("durable_at"),
    "rt.snapshot_catalog": CatalogRule("generated_at", (("snapshot_class", "EPHEMERAL_PORTAL"),)),
}


class PartitionRetentionRepository(Protocol):
    def policies(self) -> Sequence[RetentionPolicy]:
        """Every row of rt.partition_policies."""

    def partitions(self, relation: str) -> Sequence[PartitionInfo]:
        """The attached partitions of a policy relation (bounds at whole UTC days)."""

    def detached_tables(self, policy: RetentionPolicy) -> Sequence[DetachedTable]:
        """Tables of the relation that were detached by an earlier run but never dropped."""

    def detach_and_drop(self, partition: PartitionInfo) -> None:
        """DETACH PARTITION ... CONCURRENTLY then DROP, in autocommit."""

    def drop_detached(self, table: DetachedTable, policy: RetentionPolicy) -> None:
        """Drop a detached table; raise when it holds a row at or after its range."""

    def prune_catalog(self, catalog: str, rule: CatalogRule, before: datetime) -> int:
        """Delete unreferenced catalog rows older than `before`; return the number deleted."""


@dataclass(frozen=True, slots=True)
class PartitionOutcome:
    relation: str
    partition: str
    lower: date
    upper: date
    status: str
    reason: str


@dataclass(frozen=True, slots=True)
class PolicyRetentionResult:
    relation: str
    window_kind: str
    window_value: int
    cutoff: date | None
    partitions: tuple[PartitionOutcome, ...] = ()
    alarms: tuple[ArchiveAlarm, ...] = ()
    scanned: bool = False

    def _with_status(self, status: str) -> tuple[PartitionOutcome, ...]:
        return tuple(item for item in self.partitions if item.status == status)

    @property
    def dropped(self) -> tuple[PartitionOutcome, ...]:
        return self._with_status(STATUS_DROPPED)

    @property
    def would_drop(self) -> tuple[PartitionOutcome, ...]:
        return self._with_status(STATUS_WOULD_DROP)

    @property
    def kept(self) -> tuple[PartitionOutcome, ...]:
        return self._with_status(STATUS_KEPT)

    @property
    def failed(self) -> tuple[PartitionOutcome, ...]:
        return self._with_status(STATUS_FAILED)

    def as_dict(self, *, include_kept: bool = False) -> dict[str, object]:
        reasons: dict[str, int] = {}
        for item in self.kept:
            reasons[item.reason] = reasons.get(item.reason, 0) + 1
        entry: dict[str, object] = {
            "relation": self.relation,
            "window": f"{self.window_value} {self.window_kind}",
            "cutoff": self.cutoff.isoformat() if self.cutoff else None,
            "dropped": [item.partition for item in self.dropped],
            "would_drop": [item.partition for item in self.would_drop],
            "failed": [{"partition": item.partition, "reason": item.reason} for item in self.failed],
            "kept": len(self.kept),
            "kept_reasons": reasons,
        }
        if include_kept:
            entry["kept_partitions"] = [item.partition for item in self.kept]
        return entry


@dataclass(frozen=True, slots=True)
class CatalogPruneOutcome:
    catalog: str
    relations: tuple[str, ...]
    before: date
    status: str
    rows_deleted: int | None = None


@dataclass(frozen=True, slots=True)
class RetentionReport:
    """`alarms` are run-level (policy lookup, refusals, catalog prune); each policy has its own."""

    as_of_date: date
    executed: bool
    policies: tuple[PolicyRetentionResult, ...]
    catalogs: tuple[CatalogPruneOutcome, ...] = ()
    skipped: tuple[tuple[str, str], ...] = ()
    alarms: tuple[ArchiveAlarm, ...] = ()

    @property
    def dropped(self) -> tuple[PartitionOutcome, ...]:
        return tuple(item for result in self.policies for item in result.dropped)

    @property
    def would_drop(self) -> tuple[PartitionOutcome, ...]:
        return tuple(item for result in self.policies for item in result.would_drop)

    @property
    def failed(self) -> tuple[PartitionOutcome, ...]:
        return tuple(item for result in self.policies for item in result.failed)

    @property
    def all_alarms(self) -> tuple[ArchiveAlarm, ...]:
        return self.alarms + tuple(alarm for result in self.policies for alarm in result.alarms)

    @property
    def ok(self) -> bool:
        return not self.all_alarms

    def as_dict(self, *, include_kept: bool = False) -> dict[str, object]:
        return {
            "mode": "EXECUTE" if self.executed else "DRY_RUN",
            "as_of_date": self.as_of_date.isoformat(),
            "ok": self.ok,
            "dropped": len(self.dropped),
            "would_drop": len(self.would_drop),
            "failed": len(self.failed),
            "policies": [result.as_dict(include_kept=include_kept) for result in self.policies],
            "catalogs": [
                {"catalog": item.catalog, "relations": list(item.relations), "before": item.before.isoformat(),
                 "status": item.status, "rows_deleted": item.rows_deleted}
                for item in self.catalogs
            ],
            "skipped": [{"relation": name, "reason": reason} for name, reason in self.skipped],
            "alarms": [alarm.as_dict() for alarm in self.all_alarms],
        }


def subtract_months(day: date, months: int) -> date:
    index = day.year * 12 + (day.month - 1) - months
    year, month_zero = divmod(index, 12)
    month = month_zero + 1
    return date(year, month, min(day.day, monthrange(year, month)[1]))


def retention_cutoff(window_kind: str, window_value: int, as_of: date) -> date:
    """The date at or before which a partition's whole range must end for it to be dropped."""
    if window_value < 1:
        raise ValueError("a retention window must be at least one unit")
    if window_kind == "CALENDAR_DAYS":
        return as_of - timedelta(days=window_value)
    if window_kind == "MONTHS":
        return subtract_months(as_of, window_value)
    raise ValueError(f"unsupported window kind {window_kind}")


def classify_partition(lower: date, upper: date, cutoff: date, as_of: date) -> tuple[bool, str]:
    """(droppable, reason) of a partition [lower, upper)."""
    if upper <= lower:
        return False, REASON_INVALID_RANGE
    if upper > as_of:
        return False, REASON_CURRENT_OR_FUTURE
    if upper <= cutoff:
        return True, REASON_EXPIRED
    if lower < cutoff:
        return False, REASON_STRADDLES_CUTOFF
    return False, REASON_WITHIN_WINDOW


def policy_refusal(policy: RetentionPolicy) -> str | None:
    """Why this policy row is not an ordinary-retention policy the executor may act on (None when it is)."""
    if policy.relation not in ELIGIBLE_RELATIONS:
        return "relation is not a registered HOT_TTL or KEPT_3M partitioned relation"
    seed = PARTITION_POLICY_SEEDS[policy.relation]
    actual = (policy.scheme, policy.retention_class, policy.window_kind, policy.window_value)
    if actual != seed or RELATIONS[policy.relation].retention != policy.retention_class:
        return f"policy row {actual} differs from the registry seed {seed}"
    if policy.requires_archive:
        return "policy requires an archive"
    if ELIGIBLE_CLASSES[policy.retention_class] != (policy.scheme, policy.window_kind):
        return "policy scheme and window kind do not match its retention class"
    if policy.catalog_relation is not None and policy.catalog_relation not in CATALOG_RULES:
        return f"no catalog prune rule for {policy.catalog_relation}"
    return None


def _describe(error: BaseException) -> str:
    primary = getattr(getattr(error, "diag", None), "message_primary", None)
    return f"{type(error).__name__}: {(primary or str(error))[:200]}"


def _midnight_utc(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, tzinfo=_UTC)


def _utc_now() -> datetime:
    return datetime.now(_UTC)


class PartitionRetentionExecutor:
    def __init__(
        self, repository: PartitionRetentionRepository, *, clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._repository = repository
        self._clock = clock

    def run(
        self, *, execute: bool = False, as_of_date: date | None = None, relations: Sequence[str] | None = None,
    ) -> RetentionReport:
        """Drop (or, by default, list) every expired whole partition of the ordinary-retention relations.

        `relations` limits the run; each must be an eligible relation (ValueError otherwise, before any
        statement). `as_of_date` may only move the window earlier than the clock's UTC date.
        """
        today = self._clock().astimezone(_UTC).date()
        as_of = today if as_of_date is None else as_of_date
        if as_of > today:
            raise ValueError("as_of_date cannot be later than the current UTC date")
        targets = self._targets(relations)
        alarms: list[ArchiveAlarm] = []
        try:
            policies = {policy.relation: policy for policy in self._repository.policies()}
        except Exception as error:  # noqa: BLE001 - any failure is an alarm, never a crash
            alarms.append(self._alarm(ALARM_POLICY_LOOKUP_FAILED, _describe(error)))
            return RetentionReport(as_of, execute, (), alarms=tuple(alarms))
        skipped: list[tuple[str, str]] = []
        if relations is None:
            for name, policy in sorted(policies.items()):
                if name in ELIGIBLE_RELATIONS:
                    continue
                if policy.retention_class in ELIGIBLE_CLASSES and not policy.requires_archive:
                    alarms.append(self._alarm(
                        ALARM_PARTITION_POLICY_UNSUPPORTED, "policy relation is not in the relation registry",
                        relation=name))
                else:
                    skipped.append((name, f"{policy.retention_class}: not ordinary retention"))
        results: list[PolicyRetentionResult] = []
        accepted: dict[str, RetentionPolicy] = {}
        for relation in targets:
            policy = policies.get(relation)
            if policy is None:
                alarms.append(self._alarm(ALARM_PARTITION_POLICY_UNSUPPORTED, "no partition policy row",
                                          relation=relation))
                continue
            refusal = policy_refusal(policy)
            if refusal is not None:
                alarms.append(self._alarm(ALARM_PARTITION_POLICY_UNSUPPORTED, refusal, relation=relation))
                continue
            accepted[relation] = policy
            results.append(self._run_policy(policy, as_of, execute))
        catalogs = self._prune_catalogs(accepted, results, execute, alarms)
        return RetentionReport(as_of, execute, tuple(results), tuple(catalogs), tuple(skipped), tuple(alarms))

    @staticmethod
    def _targets(relations: Sequence[str] | None) -> tuple[str, ...]:
        if relations is None:
            return ELIGIBLE_RELATIONS
        for relation in relations:
            if relation not in RELATIONS:
                raise ValueError(f"{relation} is not a known relation")
            if relation not in ELIGIBLE_RELATIONS:
                raise ValueError(f"{relation} is not a partitioned HOT_TTL or KEPT_3M relation")
        return tuple(sorted(set(relations)))

    def _run_policy(self, policy: RetentionPolicy, as_of: date, execute: bool) -> PolicyRetentionResult:
        cutoff = retention_cutoff(policy.window_kind, policy.window_value, as_of)
        try:
            partitions = sorted(self._repository.partitions(policy.relation), key=lambda item: item.lower)
            detached = sorted(self._repository.detached_tables(policy), key=lambda item: item.lower)
        except Exception as error:  # noqa: BLE001
            alarm = self._alarm(ALARM_PARTITION_LOOKUP_FAILED, _describe(error), relation=policy.relation)
            return PolicyRetentionResult(policy.relation, policy.window_kind, policy.window_value, cutoff,
                                         alarms=(alarm,))
        alarms: list[ArchiveAlarm] = []
        outcomes = [self._finish_detached(policy, table, cutoff, execute, alarms) for table in detached]
        outcomes.extend(self._consider(policy, partition, cutoff, as_of, execute, alarms)
                        for partition in partitions)
        return PolicyRetentionResult(policy.relation, policy.window_kind, policy.window_value, cutoff,
                                     tuple(outcomes), tuple(alarms), scanned=True)

    def _consider(
        self, policy: RetentionPolicy, partition: PartitionInfo, cutoff: date, as_of: date, execute: bool,
        alarms: list[ArchiveAlarm],
    ) -> PartitionOutcome:
        droppable, reason = classify_partition(partition.lower, partition.upper, cutoff, as_of)
        if not droppable:
            return self._outcome(partition, STATUS_KEPT, reason)
        if not execute:
            return self._outcome(partition, STATUS_WOULD_DROP, reason)
        try:
            self._repository.detach_and_drop(partition)
        except Exception as error:  # noqa: BLE001
            if self._is_gone(policy, partition.name):
                return self._outcome(partition, STATUS_DROPPED, REASON_ALREADY_DROPPED)
            code = (ALARM_PARTITION_REFERENCED if getattr(error, "pgcode", None) == FOREIGN_KEY_VIOLATION
                    else ALARM_PARTITION_DROP_FAILED)
            alarms.append(self._alarm(code, _describe(error), relation=f"{partition.relation}/{partition.name}"))
            return self._outcome(partition, STATUS_FAILED, code)
        self._log_dropped(partition)
        return self._outcome(partition, STATUS_DROPPED, reason)

    def _finish_detached(
        self, policy: RetentionPolicy, table: DetachedTable, cutoff: date, execute: bool,
        alarms: list[ArchiveAlarm],
    ) -> PartitionOutcome:
        if table.upper > cutoff:
            return self._outcome(table, STATUS_KEPT, REASON_DETACHED_NOT_EXPIRED)
        if not execute:
            return self._outcome(table, STATUS_WOULD_DROP, REASON_DETACHED_EXPIRED)
        try:
            self._repository.drop_detached(table, policy)
        except Exception as error:  # noqa: BLE001
            alarms.append(self._alarm(ALARM_DETACHED_DROP_FAILED, _describe(error),
                                      relation=f"{table.relation}/{table.name}"))
            return self._outcome(table, STATUS_FAILED, ALARM_DETACHED_DROP_FAILED)
        self._log_dropped(table)
        return self._outcome(table, STATUS_DROPPED, REASON_DETACHED_EXPIRED)

    def _is_gone(self, policy: RetentionPolicy, name: str) -> bool:
        """True when a concurrent run already removed the partition entirely (no partition, no leftover table)."""
        try:
            present = {item.name for item in self._repository.partitions(policy.relation)}
            present.update(table.name for table in self._repository.detached_tables(policy))
        except Exception:  # noqa: BLE001
            return False
        return name not in present

    def _prune_catalogs(
        self, accepted: dict[str, RetentionPolicy], results: Sequence[PolicyRetentionResult],
        execute: bool, alarms: list[ArchiveAlarm],
    ) -> list[CatalogPruneOutcome]:
        groups: dict[str, list[PolicyRetentionResult]] = {}
        for result in results:
            catalog = accepted[result.relation].catalog_relation
            if catalog is not None and result.scanned and result.cutoff is not None:
                groups.setdefault(catalog, []).append(result)
        outcomes: list[CatalogPruneOutcome] = []
        for catalog, group in sorted(groups.items()):
            cutoff = min(item.cutoff for item in group if item.cutoff is not None)
            relations = tuple(sorted(item.relation for item in group))
            if not execute:
                outcomes.append(CatalogPruneOutcome(catalog, relations, cutoff, STATUS_PLANNED))
                continue
            try:
                deleted = self._repository.prune_catalog(catalog, CATALOG_RULES[catalog], _midnight_utc(cutoff))
            except Exception as error:  # noqa: BLE001
                alarms.append(self._alarm(ALARM_CATALOG_PRUNE_FAILED, _describe(error), relation=catalog))
                outcomes.append(CatalogPruneOutcome(catalog, relations, cutoff, STATUS_FAILED))
                continue
            LOGGER.info("partition_retention_catalog_pruned %s", json.dumps(
                {"catalog": catalog, "before": cutoff.isoformat(), "rows": deleted}, sort_keys=True))
            outcomes.append(CatalogPruneOutcome(catalog, relations, cutoff, STATUS_PRUNED, deleted))
        return outcomes

    @staticmethod
    def _outcome(item: PartitionInfo | DetachedTable, status: str, reason: str) -> PartitionOutcome:
        return PartitionOutcome(item.relation, item.name, item.lower, item.upper, status, reason)

    @staticmethod
    def _log_dropped(item: PartitionInfo | DetachedTable) -> None:
        LOGGER.info("partition_retention_dropped %s", json.dumps(
            {"relation": item.relation, "partition": item.name, "lower": item.lower.isoformat(),
             "upper": item.upper.isoformat()}, sort_keys=True))

    @staticmethod
    def _alarm(code: str, detail: str, *, relation: str | None = None) -> ArchiveAlarm:
        alarm = ArchiveAlarm(code, detail, relation=relation)
        LOGGER.error("partition_retention_alarm %s", json.dumps(alarm.as_dict(), sort_keys=True))
        return alarm


_DDL_STATEMENT_TIMEOUT = "120s"
_DETACHED_TABLES = (
    "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = %(schema)s AND c.relkind = 'r' AND NOT c.relispartition AND c.relname ~ %(pattern)s "
    "AND NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid) "
    "AND ARRAY(SELECT a.attname::text FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0 "
    "AND NOT a.attisdropped ORDER BY a.attnum) = "
    "ARRAY(SELECT a.attname::text FROM pg_attribute a WHERE a.attrelid = to_regclass(%(parent)s) "
    "AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum)"
)
_REFERENCING_KEYS = (
    "SELECT n.nspname::text, c.relname::text, con.confdeltype::text, "
    "ARRAY(SELECT a.attname::text FROM unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord) "
    "JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum ORDER BY k.ord), "
    "ARRAY(SELECT a.attname::text FROM unnest(con.confkey) WITH ORDINALITY AS k(attnum, ord) "
    "JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.attnum ORDER BY k.ord) "
    "FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE con.contype = 'f' AND con.confrelid = to_regclass(%s) AND con.conparentid = 0 ORDER BY 1, 2, con.oid"
)


def _next_start(scheme: str, lower: date) -> date | None:
    if scheme == "DAILY":
        return lower + timedelta(days=1)
    if scheme == "MONTHLY" and lower.day == 1:
        return date(lower.year + lower.month // 12, lower.month % 12 + 1, 1)
    return None


def _require_whole_utc_days(partition: PartitionInfo) -> None:
    """PartitionInfo carries dates, so a bound with a time of day would be read as its date, too early."""
    match = _BOUND_PATTERN.fullmatch(partition.bound_expression)
    if match is None:
        raise ValueError(f"unsupported partition bound on {partition.name}")
    for text in match.groups():
        if len(text) > 10 and datetime.fromisoformat(text).astimezone(_UTC).time() != time(0):
            raise ValueError(f"{partition.name} has a bound that is not at UTC midnight")


class PostgresPartitionRetentionRepository:
    """`connect` returns a new psycopg2 connection each call (the caller owns credentials).

    Needs the owner (DETACH, DROP) and DELETE on the catalogs: the admin role in the hardened
    phase, the database owner while security is deferred. Partition listing and the detach-then-drop
    DDL are the session archive repository's.
    """

    def __init__(self, connect: Callable[[], Any], *, statement_timeout: str = "300s") -> None:
        self._connect = connect
        self._statement_timeout = statement_timeout
        self._admin = PostgresSessionArchiveRepository(connect, statement_timeout=statement_timeout)

    @contextmanager
    def _read_cursor(self) -> Iterator[Any]:
        with closing(self._connect()) as connection:
            connection.set_session(readonly=True, autocommit=False)
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT set_config('statement_timeout', %s, true)", (self._statement_timeout,))
                    yield cursor
            finally:
                connection.rollback()

    def policies(self) -> Sequence[RetentionPolicy]:
        with self._read_cursor() as cursor:
            cursor.execute(
                "SELECT relation_name, scheme, key_column, key_type, retention_class, window_kind, window_value, "
                "requires_archive, catalog_relation FROM rt.partition_policies ORDER BY relation_name")
            return [RetentionPolicy(row[0], row[1], row[2], row[3], row[4], row[5], int(row[6]), bool(row[7]),
                                    row[8]) for row in cursor.fetchall()]

    def partitions(self, relation: str) -> Sequence[PartitionInfo]:
        found = self._admin.partitions(relation)
        for partition in found:
            _require_whole_utc_days(partition)
        return found

    @staticmethod
    def _detached_parameters(relation: str) -> dict[str, str]:
        schema, _, name = relation.partition(".")
        return {"schema": schema, "pattern": "^" + re.escape(name) + "_p[0-9]{8}$", "parent": relation}

    def detached_tables(self, policy: RetentionPolicy) -> Sequence[DetachedTable]:
        with self._read_cursor() as cursor:
            cursor.execute(_DETACHED_TABLES + " ORDER BY c.relname", self._detached_parameters(policy.relation))
            names = [row[0] for row in cursor.fetchall()]
        tables: list[DetachedTable] = []
        for name in names:
            try:
                lower = datetime.strptime(name[-8:], "%Y%m%d").date()
            except ValueError:
                continue
            upper = _next_start(policy.scheme, lower)
            if upper is not None:
                tables.append(DetachedTable(policy.relation, name, lower, upper))
        return tables

    def detach_and_drop(self, partition: PartitionInfo) -> None:
        self._admin.detach_and_drop(partition)

    def drop_detached(self, table: DetachedTable, policy: RetentionPolicy) -> None:
        schema, _, _ = table.relation.partition(".")
        child = sql.Identifier(schema, table.name)
        bound = table.upper if policy.key_type == "date" else _midnight_utc(table.upper)
        with closing(self._connect()) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute("SET lock_timeout = '10s'")
                cursor.execute(sql.SQL("SET statement_timeout = {}").format(sql.Literal(_DDL_STATEMENT_TIMEOUT)))
                cursor.execute(_DETACHED_TABLES + " AND c.relname = %(name)s",
                               {**self._detached_parameters(table.relation), "name": table.name})
                if cursor.fetchone() is None:
                    raise RuntimeError(f"{table.name} is not a detached partition table of {table.relation}")
                cursor.execute(
                    sql.SQL("SELECT EXISTS (SELECT 1 FROM {} WHERE {} >= %s)").format(
                        child, sql.Identifier(policy.key_column)), (bound,))
                if cursor.fetchone()[0]:
                    raise RuntimeError(f"{table.name} holds rows at or after {table.upper}; kept")
                cursor.execute(sql.SQL("DROP TABLE {}").format(child))

    def prune_catalog(self, catalog: str, rule: CatalogRule, before: datetime) -> int:
        if catalog not in CATALOG_RULES or CATALOG_RULES[catalog] != rule:
            raise ValueError(f"{catalog} has no catalog prune rule")
        schema, _, name = catalog.partition(".")
        with closing(self._connect()) as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT set_config('lock_timeout', '10s', true), "
                                   "set_config('statement_timeout', %s, true)", (self._statement_timeout,))
                    cursor.execute(_REFERENCING_KEYS, (catalog,))
                    references = cursor.fetchall()
                    conditions = [sql.SQL("c.{} < %s").format(sql.Identifier(rule.time_column))]
                    parameters: list[Any] = [before]
                    for column, value in rule.equals:
                        conditions.append(sql.SQL("c.{} = %s").format(sql.Identifier(column)))
                        parameters.append(value)
                    for ref_schema, ref_table, delete_action, ref_columns, key_columns in references:
                        if delete_action not in ("a", "r"):
                            continue
                        match = sql.SQL(" AND ").join(
                            sql.SQL("r.{} = c.{}").format(sql.Identifier(ref_column), sql.Identifier(key_column))
                            for ref_column, key_column in zip(ref_columns, key_columns))
                        conditions.append(sql.SQL("NOT EXISTS (SELECT 1 FROM {} AS r WHERE {})").format(
                            sql.Identifier(ref_schema, ref_table), match))
                    cursor.execute(
                        sql.SQL("DELETE FROM {} AS c WHERE {}").format(
                            sql.Identifier(schema, name), sql.SQL(" AND ").join(conditions)), parameters)
                    deleted = cursor.rowcount
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return deleted
