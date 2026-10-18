"""PostgreSQL repository for the session archive worker (rt schema of the v2 baseline).

Every read of one session runs in one repeatable-read, read-only transaction, so the
archive is a consistent snapshot and never blocks a writer. Detach and drop use their own
autocommit connection. Rows are read with SELECT * so a stored row is archived exactly as
it is stored, including any column added later.
"""
from __future__ import annotations

import re
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, Callable, Iterator, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_REPEATABLE_READ
from psycopg2.extras import RealDictCursor

from .session_archive import (
    DETECTOR_EVALUATIONS,
    OPTION_ALERTS,
    OPTION_FLOW_CLUSTERS,
    OPTION_FLOW_EVENTS,
    STOCK_ALERTS,
    ArchiveRecord,
    ArchiveRow,
    to_jsonable,
)
from .session_archive_worker import AlertHorizon, PartitionHealth, PartitionInfo, PartitionPolicy

MARKET_TIME_ZONE = "America/New_York"
TERMINAL_STATUSES = ("TARGET", "STOP", "EXPIRED")
_HORIZON_COLUMN = "__horizon_close_date"
_BOUND_PATTERN = re.compile(r"^FOR VALUES FROM \('([^']+)'\) TO \('([^']+)'\)$")


def _archive_id(record: ArchiveRecord) -> UUID:
    return uuid5(NAMESPACE_URL, (
        f"realtime-session-archive:{record.session_date}:{record.kind}:"
        f"{record.horizon_close_date}:{record.sha256}:{record.build_id}"
    ))


def _session_window(column: str) -> str:
    """Rows whose timestamp falls on the exchange session's ET calendar day."""
    return (
        f"{column} >= (%(session)s::date)::timestamp AT TIME ZONE '{MARKET_TIME_ZONE}' "
        f"AND {column} < ((%(session)s::date + 1))::timestamp AT TIME ZONE '{MARKET_TIME_ZONE}'"
    )


# An alert belongs to the exchange session (ET calendar day) of its as_of time.
_ALERT_WINDOW = _session_window("a.as_of")
_ALERT_FILTER = f"a.alert_kind = %(alert_kind)s AND {_ALERT_WINDOW}"
_SESSION_EVALUATION_EVENTS = (
    "SELECT v.event_id FROM rt.detector_evaluations v "
    "JOIN rt.detector_datasets d ON d.dataset_id = v.dataset_id WHERE d.session_date = %(session)s"
)
_ALERT_EVENTS = f"SELECT a.event_id FROM rt.alert_current a WHERE {_ALERT_FILTER}"
_ALERT_KINDS = {STOCK_ALERTS: "STOCK", OPTION_ALERTS: "OPTION"}
# The session date of a row of a partitioned relation, for the drop-time coverage check.
_PARTITION_SESSION_DATE = {
    "rt.option_flow_events": f"(x.event_time AT TIME ZONE '{MARKET_TIME_ZONE}')::date",
    "rt.option_flow_clusters": "x.session_date",
}


@dataclass(frozen=True, slots=True)
class _Extract:
    relation: str
    key_columns: tuple[str, ...]
    source: str  # FROM ... WHERE ... naming the selected row alias
    alias: str = "x"


_EXTRACTS: Mapping[str, tuple[_Extract, ...]] = {
    **{
        kind: (
            _Extract("rt.alert_current", ("alert_id",),
                     f"FROM rt.alert_current a WHERE {_ALERT_FILTER}", alias="a"),
            _Extract("rt.detector_events", ("event_id",),
                     f"FROM rt.detector_events x WHERE x.event_id IN ({_ALERT_EVENTS})"),
            _Extract("rt.detector_event_inputs", ("event_id", "input_role", "input_key"),
                     f"FROM rt.detector_event_inputs x WHERE x.event_id IN ({_ALERT_EVENTS})"),
        )
        for kind in _ALERT_KINDS
    },
    DETECTOR_EVALUATIONS: (
        _Extract("rt.detector_datasets", ("dataset_id",),
                 "FROM rt.detector_datasets x WHERE x.session_date = %(session)s"),
        _Extract("rt.detector_evaluations", ("evaluation_id",),
                 "FROM rt.detector_evaluations x WHERE x.dataset_id IN "
                 "(SELECT d.dataset_id FROM rt.detector_datasets d WHERE d.session_date = %(session)s)"),
        _Extract("rt.detector_events", ("event_id",),
                 f"FROM rt.detector_events x WHERE x.event_id IN ({_SESSION_EVALUATION_EVENTS})"),
        _Extract("rt.detector_event_inputs", ("event_id", "input_role", "input_key"),
                 f"FROM rt.detector_event_inputs x WHERE x.event_id IN ({_SESSION_EVALUATION_EVENTS})"),
    ),
    # Every revision is archived: a cancel is a later revision row, never an edit.
    OPTION_FLOW_EVENTS: (
        _Extract("rt.option_flow_events", ("event_time", "contract_ticker", "source_seq", "revision", "event_id"),
                 f"FROM rt.option_flow_events x WHERE {_session_window('x.event_time')}"),
    ),
    OPTION_FLOW_CLUSTERS: (
        _Extract("rt.option_flow_clusters", ("window_start", "cluster_id", "revision"),
                 "FROM rt.option_flow_clusters x WHERE x.session_date = %(session)s"),
    ),
}

# Per alert of the session, the ET date of its first terminal assessment (its horizon close).
_CLOSING_CTE = (
    "WITH closing AS ("
    "SELECT a.alert_id, ((min(m.assessed_at) FILTER (WHERE m.status IN ('TARGET', 'STOP', 'EXPIRED'))) "
    f"AT TIME ZONE '{MARKET_TIME_ZONE}')::date AS horizon_close_date "
    "FROM rt.alert_current a LEFT JOIN rt.alert_management_assessments m ON m.alert_id = a.alert_id "
    f"WHERE {_ALERT_WINDOW} GROUP BY a.alert_id)"
)


def _key_part(value: object) -> str:
    """A key column as text whose string order is its natural order (fixed-width forms)."""
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, int):
        return f"{value:020d}"
    if isinstance(value, (datetime, date)):
        return str(to_jsonable(value))
    return str(value)


@lru_cache(maxsize=1)
def _calendar():
    import exchange_calendars

    return exchange_calendars.get_calendar("XNYS")


def _bound_date(text: str, key_type: str) -> date:
    if key_type == "date":
        return date.fromisoformat(text)
    stamp = datetime.fromisoformat(text)
    if stamp.tzinfo is None:
        raise ValueError("partition timestamp bound has no time zone")
    return stamp.astimezone(timezone.utc).date()


class PostgresSessionArchiveRepository:
    """`connect` returns a new psycopg2 connection each call (the caller owns credentials)."""

    def __init__(self, connect: Callable[[], Any], *, statement_timeout: str = "300s") -> None:
        self._connect = connect
        self._statement_timeout = statement_timeout

    @contextmanager
    def _read_cursor(self) -> Iterator[Any]:
        with closing(self._connect()) as connection:
            connection.set_session(isolation_level=ISOLATION_LEVEL_REPEATABLE_READ, readonly=True,
                                   autocommit=False)
            try:
                with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(f"SET LOCAL statement_timeout = '{self._statement_timeout}'")
                    yield cursor
            finally:
                connection.rollback()

    @staticmethod
    def _parameters(session_date: date, kind: str | None = None) -> dict[str, object]:
        return {"session": session_date, "alert_kind": _ALERT_KINDS.get(kind or "", "")}

    def read_rows(self, session_date: date, kind: str) -> Sequence[ArchiveRow]:
        if kind not in _EXTRACTS:
            raise ValueError(f"unsupported archive kind {kind}")
        parameters = self._parameters(session_date, kind)
        rows: list[ArchiveRow] = []
        with self._read_cursor() as cursor:
            for extract in _EXTRACTS[kind]:
                order = ", ".join(f"{extract.alias}.{column}" for column in extract.key_columns)
                cursor.execute(f"SELECT {extract.alias}.* {extract.source} ORDER BY {order}", parameters)
                for record in cursor.fetchall():
                    rows.append(ArchiveRow(
                        extract.relation,
                        tuple(_key_part(record[column]) for column in extract.key_columns),
                        dict(record),
                    ))
        return rows

    def count_rows(self, session_date: date, kind: str) -> Mapping[str, int]:
        if kind not in _EXTRACTS:
            raise ValueError(f"unsupported archive kind {kind}")
        parameters = self._parameters(session_date, kind)
        counts: dict[str, int] = {}
        with self._read_cursor() as cursor:
            for extract in _EXTRACTS[kind]:
                cursor.execute(f"SELECT count(*) AS n {extract.source}", parameters)
                counts[extract.relation] = int(cursor.fetchone()["n"])
        return counts

    def read_outcomes(self, session_date: date) -> Mapping[date, Sequence[ArchiveRow]]:
        """Every assessment of the alerts whose horizon closed, grouped by close date."""
        groups: dict[date, list[ArchiveRow]] = {}
        with self._read_cursor() as cursor:
            cursor.execute(
                f"{_CLOSING_CTE} SELECT c.horizon_close_date AS {_HORIZON_COLUMN}, m.* "
                "FROM closing c JOIN rt.alert_management_assessments m ON m.alert_id = c.alert_id "
                "WHERE c.horizon_close_date IS NOT NULL "
                "ORDER BY c.horizon_close_date, m.assessed_at, m.assessment_id",
                self._parameters(session_date),
            )
            for record in cursor.fetchall():
                document = dict(record)
                horizon = document.pop(_HORIZON_COLUMN)
                groups.setdefault(horizon, []).append(ArchiveRow(
                    "rt.alert_management_assessments", (str(document["assessment_id"]),), document))
        return groups

    def count_outcomes(self, session_date: date, horizon_close_date: date) -> Mapping[str, int]:
        with self._read_cursor() as cursor:
            cursor.execute(
                f"{_CLOSING_CTE} SELECT count(*) AS n "
                "FROM closing c JOIN rt.alert_management_assessments m ON m.alert_id = c.alert_id "
                "WHERE c.horizon_close_date = %(horizon)s",
                {**self._parameters(session_date), "horizon": horizon_close_date},
            )
            return {"rt.alert_management_assessments": int(cursor.fetchone()["n"])}

    def alert_horizons(self, session_date: date) -> Sequence[AlertHorizon]:
        with self._read_cursor() as cursor:
            cursor.execute(
                f"{_CLOSING_CTE} SELECT alert_id, horizon_close_date FROM closing ORDER BY alert_id",
                self._parameters(session_date),
            )
            return [AlertHorizon(str(row["alert_id"]), row["horizon_close_date"]) for row in cursor.fetchall()]

    def list_archives(self, session_date: date) -> Sequence[ArchiveRecord]:
        with self._read_cursor() as cursor:
            cursor.execute(
                "SELECT session_date, kind, horizon_close_date, uri, sha256, row_count, schema_version, "
                "build_id, verified_at FROM rt.session_archives WHERE session_date = %s "
                "ORDER BY kind, horizon_close_date NULLS FIRST, verified_at, sha256",
                (session_date,),
            )
            return [ArchiveRecord(
                session_date=row["session_date"], kind=row["kind"], uri=row["uri"], sha256=row["sha256"],
                row_count=int(row["row_count"]), schema_version=row["schema_version"],
                build_id=row["build_id"], verified_at=row["verified_at"],
                horizon_close_date=row["horizon_close_date"],
            ) for row in cursor.fetchall()]

    def record_archive(self, record: ArchiveRecord) -> bool:
        if record.verified_at is None:
            raise ValueError("only a verified archive is recorded")
        archive_id = _archive_id(record)
        with closing(self._connect()) as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO rt.session_archives (archive_id, session_date, kind, horizon_close_date, "
                        "uri, sha256, row_count, schema_version, build_id, verified_at) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                        (str(archive_id), record.session_date, record.kind, record.horizon_close_date,
                         record.uri, record.sha256, record.row_count, record.schema_version,
                         record.build_id, record.verified_at),
                    )
                    inserted = cursor.rowcount == 1
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        return inserted

    def partition_policy(self, relation: str) -> PartitionPolicy:
        return self._policy(relation)[0]

    def _policy(self, relation: str) -> tuple[PartitionPolicy, str]:
        with self._read_cursor() as cursor:
            cursor.execute(
                "SELECT retention_class, window_kind, window_value, requires_archive, archive_kinds, key_type "
                "FROM rt.partition_policies WHERE relation_name = %s", (relation,))
            row = cursor.fetchone()
        if row is None:
            raise LookupError(f"no partition policy for {relation}")
        policy = PartitionPolicy(
            row["retention_class"], row["window_kind"], int(row["window_value"]),
            bool(row["requires_archive"]), tuple(row["archive_kinds"] or ()),
        )
        return policy, row["key_type"]

    def partition_health(self, as_of_date: date) -> Sequence[PartitionHealth]:
        with self._read_cursor() as cursor:
            cursor.execute(
                "SELECT relation_name, scheme, horizon_date, days_ahead, status "
                "FROM rt.partition_health(%s) ORDER BY relation_name", (as_of_date,))
            return [PartitionHealth(row["relation_name"], row["scheme"], row["horizon_date"],
                                    int(row["days_ahead"]), row["status"]) for row in cursor.fetchall()]

    def partition_row_coverage(self, partition: PartitionInfo, sessions: Sequence[date]) -> tuple[int, int]:
        expression = _PARTITION_SESSION_DATE.get(partition.relation)
        if expression is None:
            raise LookupError(f"no session date expression for {partition.relation}")
        schema, _, _ = partition.relation.partition(".")
        statement = sql.SQL(
            "SELECT count(*) AS total, count(*) FILTER (WHERE {expression} = ANY(%s::date[])) AS covered "
            "FROM {child} x"
        ).format(expression=sql.SQL(expression), child=sql.Identifier(schema, partition.name))
        with self._read_cursor() as cursor:
            cursor.execute(statement, (list(sessions),))
            row = cursor.fetchone()
        return int(row["total"]), int(row["covered"])

    def partitions(self, relation: str) -> Sequence[PartitionInfo]:
        _, key_type = self._policy(relation)
        schema, _, name = relation.partition(".")
        with self._read_cursor() as cursor:
            cursor.execute(
                "SELECT c.relname AS name, pg_get_expr(c.relpartbound, c.oid) AS bound, "
                "i.inhdetachpending AS pending FROM pg_inherits i "
                "JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent "
                "JOIN pg_namespace pn ON pn.oid = p.relnamespace "
                "WHERE pn.nspname = %s AND p.relname = %s ORDER BY c.relname", (schema, name))
            found = cursor.fetchall()
        partitions: list[PartitionInfo] = []
        for row in found:
            match = _BOUND_PATTERN.fullmatch(row["bound"])
            if match is None:
                raise ValueError(f"unsupported partition bound on {row['name']}")
            partitions.append(PartitionInfo(
                relation, row["name"], _bound_date(match.group(1), key_type),
                _bound_date(match.group(2), key_type), row["bound"], bool(row["pending"]),
            ))
        return partitions

    def sessions_between(self, start: date, end: date) -> Sequence[date]:
        if end <= start:
            return ()
        index = _calendar().sessions_in_range(start.isoformat(), (end - timedelta(days=1)).isoformat())
        return tuple(stamp.date() for stamp in index)

    def session_window_start(self, as_of_session: date, window_sessions: int) -> date:
        if window_sessions < 1:
            raise ValueError("online window must cover at least one session")
        calendar = _calendar()
        session = calendar.date_to_session(as_of_session.isoformat(), direction="previous")
        return calendar.session_offset(session, -(window_sessions - 1)).date()

    def detach_and_drop(self, partition: PartitionInfo) -> None:
        """DETACH PARTITION ... CONCURRENTLY, then DROP, in autocommit.

        A pending detach left by an interrupted earlier attempt is finalized instead. The
        partition and its bound are checked in the catalog immediately before the DDL.
        """
        schema, _, parent_name = partition.relation.partition(".")
        parent = sql.Identifier(schema, parent_name)
        child = sql.Identifier(schema, partition.name)
        with closing(self._connect()) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute("SET lock_timeout = '10s'")
                cursor.execute("SET statement_timeout = '120s'")
                cursor.execute(
                    "SELECT pg_get_expr(c.relpartbound, c.oid), i.inhdetachpending FROM pg_inherits i "
                    "JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_namespace cn ON cn.oid = c.relnamespace "
                    "JOIN pg_class p ON p.oid = i.inhparent JOIN pg_namespace pn ON pn.oid = p.relnamespace "
                    "WHERE pn.nspname = %s AND p.relname = %s AND cn.nspname = %s AND c.relname = %s",
                    (schema, parent_name, schema, partition.name))
                row = cursor.fetchone()
                if row is None:
                    raise RuntimeError(f"{partition.name} is not a partition of {partition.relation}")
                if row[0] != partition.bound_expression:
                    raise RuntimeError(f"{partition.name} bound changed since it was evaluated")
                mode = sql.SQL("FINALIZE") if row[1] else sql.SQL("CONCURRENTLY")
                cursor.execute(sql.SQL("ALTER TABLE {} DETACH PARTITION {} {}").format(parent, child, mode))
                try:
                    cursor.execute(sql.SQL("DROP TABLE {}").format(child))
                except Exception as error:
                    raise RuntimeError(
                        f"{partition.name} was detached but not dropped: {type(error).__name__}") from error
