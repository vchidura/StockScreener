from __future__ import annotations

import io
import time
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Mapping, Sequence
from uuid import UUID

import psycopg2
from psycopg2.extras import Json

from .conditions import ConditionSemantics, SemanticsRow, TradeConditionRow
from .records import ClusterRow, EngineOutput, FlowEvent, VolumeCheckpoint

FLUSH_ROWS = 1000
FLUSH_INTERVAL_SECONDS = 1.0

EVENT_COLUMNS = (
    "event_id", "event_time", "cohort_id", "session_date", "provider_tier", "contract_ticker",
    "underlyer", "source_seq", "exchange_id", "price", "size", "multiplier", "premium", "conditions",
    "condition_semantics_version", "trade_treatment", "is_multi_leg", "side_class", "side_confidence",
    "quote_bid", "quote_ask", "quote_bid_size", "quote_ask_size", "quote_time", "quote_seq",
    "cumulative_volume_before", "cumulative_volume_after", "volume_baseline_kind", "prior_open_interest",
    "open_interest_as_of", "volume_exceeds_prior_oi", "opening_inference", "spread_position", "opening_score",
    "opening_reasons", "journal_generation", "shard_id", "source_offset", "revision", "supersedes_event_id",
)
CLUSTER_COLUMNS = (
    "cluster_id", "revision", "session_date", "cohort_id", "provider_tier", "contract_ticker", "underlyer",
    "window_seconds", "window_start", "window_end", "side_class", "print_count", "total_size",
    "total_premium", "distinct_exchanges", "first_event_time", "last_event_time", "status", "policy_sha256",
)
CHECKPOINT_COLUMNS = (
    "cohort_id", "session_date", "contract_ticker", "cumulative_volume", "last_source_seq",
    "last_event_time", "baseline_kind", "snapshot_volume", "snapshot_watermark_seq",
)
_CHECKPOINT_KEY = ("cohort_id", "session_date", "contract_ticker")
# The stored identity of an event revision and of a cluster revision, as primary or unique keys.
_EVENT_JOINS = (
    "t.event_id = s.event_id AND t.event_time = s.event_time",
    "t.contract_ticker = s.contract_ticker AND t.source_seq = s.source_seq "
    "AND t.event_time = s.event_time AND t.revision = s.revision",
)
_EVENT_IDENTITY = ("contract_ticker", "source_seq", "event_time", "revision")
_CLUSTER_JOINS = ("t.cluster_id = s.cluster_id AND t.revision = s.revision AND t.session_date = s.session_date",)
_CLUSTER_IDENTITY = ("cluster_id", "revision", "session_date")


class OptionFlowWriteError(RuntimeError):
    """A write failed. The transaction was rolled back and the rows stay buffered."""

    def __init__(self, message: str, *, sqlstate: str | None = None, constraint: str | None = None,
                 table: str | None = None) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate
        self.constraint = constraint
        self.table = table


class OptionFlowSchemaViolation(OptionFlowWriteError):
    """A row broke a CHECK, NOT NULL, foreign key, unique, type or missing-partition rule."""


class OptionFlowReplayDivergence(OptionFlowWriteError):
    """A replayed row has the identity of a stored row but different values.

    `identities` holds only the identifying columns (contract, sequence, time, revision or
    cluster id, revision, session); no payload value is kept or printed.
    """

    def __init__(self, table: str, identities: Sequence[tuple]) -> None:
        shown = "; ".join(", ".join(str(part) for part in identity) for identity in identities)
        super().__init__(f"replayed rows differ from the stored rows in {table}: {shown}", table=table)
        # Normalize UUIDs: whether psycopg2 returns UUID or str depends on a process-wide adapter.
        self.identities = tuple(
            tuple(str(part) if isinstance(part, UUID) else part for part in identity) for identity in identities)


def event_values(event: FlowEvent) -> tuple:
    return (
        event.event_id, event.event_time, event.cohort_id, event.session_date, event.provider_tier,
        event.contract_ticker, event.underlyer, event.source_seq, event.exchange_id, event.price,
        event.size, event.multiplier, event.premium, event.conditions, event.condition_semantics_version,
        event.trade_treatment, event.is_multi_leg, event.side_class, event.side_confidence,
        event.quote_bid, event.quote_ask, event.quote_bid_size, event.quote_ask_size, event.quote_time,
        event.quote_seq, event.cumulative_volume_before, event.cumulative_volume_after,
        event.volume_baseline_kind, event.prior_open_interest, event.open_interest_as_of,
        event.volume_exceeds_prior_oi, event.opening_inference, event.spread_position, event.opening_score,
        event.opening_reasons, event.journal_generation, event.shard_id,
        event.source_offset, event.revision, event.supersedes_event_id,
    )


def cluster_values(row: ClusterRow) -> tuple:
    return (
        row.cluster_id, row.revision, row.session_date, row.cohort_id, row.provider_tier,
        row.contract_ticker, row.underlyer, row.window_seconds, row.window_start, row.window_end,
        row.side_class, row.print_count, row.total_size, row.total_premium, row.distinct_exchanges,
        row.first_event_time, row.last_event_time, row.status, row.policy_sha256,
    )


def checkpoint_values(row: VolumeCheckpoint) -> tuple:
    return (
        row.cohort_id, row.session_date, row.contract_ticker, row.cumulative_volume, row.last_source_seq,
        row.last_event_time, row.baseline_kind, row.snapshot_volume, row.snapshot_watermark_seq,
    )


def _escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def _array_literal(items: Sequence[Any]) -> str:
    """A PostgreSQL array literal: bare integers, or double-quoted strings with escapes."""
    parts = []
    for item in items:
        if isinstance(item, int) and not isinstance(item, bool):
            parts.append(str(item))
        else:
            parts.append('"' + str(item).replace("\\", "\\\\").replace('"', '\\"') + '"')
    return "{" + ",".join(parts) + "}"


def _cell(value: Any) -> str:
    if value is None:
        return "\\N"
    if value is True:
        return "t"
    if value is False:
        return "f"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (tuple, list)):
        return _escape(_array_literal(value))
    if isinstance(value, int):
        return str(value)
    return _escape(str(value))


def copy_text(rows: Sequence[Sequence[Any]]) -> str:
    """PostgreSQL COPY text format: tab separated, \\N for NULL, special characters escaped."""
    return "".join("\t".join(_cell(value) for value in row) + "\n" for row in rows)


@dataclass(frozen=True, slots=True)
class FlushReport:
    reason: str
    events_inserted: int
    events_skipped: int
    clusters_inserted: int
    clusters_skipped: int
    checkpoints_written: int
    checkpoints_stale_ignored: int
    committed_offsets: Mapping[tuple[UUID | None, int], int]
    seconds: float


def _translate(error: psycopg2.Error, table: str | None) -> OptionFlowWriteError:
    sqlstate = getattr(error, "pgcode", None)
    constraint = getattr(getattr(error, "diag", None), "constraint_name", None)
    detail = str(error).strip().splitlines()[0] if str(error).strip() else type(error).__name__
    message = f"{type(error).__name__} writing {table or 'option flow rows'}: {detail}"
    schema_rule = isinstance(error, (psycopg2.IntegrityError, psycopg2.DataError, psycopg2.ProgrammingError))
    kind = OptionFlowSchemaViolation if schema_rule else OptionFlowWriteError
    return kind(message, sqlstate=sqlstate, constraint=constraint, table=table)


class OptionFlowWriter:
    """COPY-batch writer for the three option flow tables.

    Rows buffer in memory and flush as one transaction when 1,000 rows are pending or one
    second has passed since the last flush, whichever comes first. Each table is bulk loaded
    with COPY into a temporary staging table, then moved with INSERT ... ON CONFLICT.

    Events and cluster revisions are immutable and idempotent: an identical replay inserts
    nothing, but a replayed row that has the identity of a stored row and different values
    raises OptionFlowReplayDivergence (the whole flush is rolled back), so a different row is
    never silently accepted. Volume checkpoints are mutable state and are upserted, except that
    a checkpoint older than the stored one (lower last_source_seq) is ignored and counted, so
    replaying from an earlier offset cannot move the stored state backwards.

    The journal offsets of the flushed outputs are committed in the same transaction through
    the optional hook, so rows and offsets never disagree. A schema or CHECK violation raises a
    typed error; the buffer is kept and nothing is swallowed.

    The connection must not be in autocommit mode. Gaps and reconcile results are not written
    here (rt.stream_gaps needs the orchestrator's session identity).
    """

    def __init__(
        self,
        connection,
        *,
        flush_rows: int = FLUSH_ROWS,
        flush_interval_seconds: float = FLUSH_INTERVAL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        in_transaction: Callable[[Any, FlushReport], None] | None = None,
    ) -> None:
        if getattr(connection, "autocommit", False):
            raise ValueError("the option flow writer needs a transactional (non-autocommit) connection")
        if flush_rows < 1 or flush_interval_seconds <= 0:
            raise ValueError("flush thresholds must be positive")
        self._connection = connection
        self._flush_rows = flush_rows
        self._interval = flush_interval_seconds
        self._clock = clock
        self._hook = in_transaction
        self._events: list[FlowEvent] = []
        self._clusters: list[ClusterRow] = []
        self._checkpoints: dict[tuple, VolumeCheckpoint] = {}
        self._offsets: dict[tuple[UUID | None, int], int] = {}
        self._last_flush = clock()
        self.committed_offsets: dict[tuple[UUID | None, int], int] = {}
        self.flushes = 0

    @property
    def pending_rows(self) -> int:
        return len(self._events) + len(self._clusters) + len(self._checkpoints)

    def add(self, output: EngineOutput, *, journal_generation: UUID | None = None) -> FlushReport | None:
        """Buffer an engine output; flush now when the row threshold is reached."""
        self._events.extend(output.events)
        self._clusters.extend(output.clusters)
        for row in output.checkpoints:
            self._checkpoints[(row.cohort_id, row.session_date, row.contract_ticker)] = row
        for shard, offset in output.offsets.items():
            key = (journal_generation, shard)
            if offset > self._offsets.get(key, 0):
                self._offsets[key] = offset
        if self.pending_rows >= self._flush_rows:
            return self.flush(reason="ROWS")
        return None

    def poll(self) -> FlushReport | None:
        """Flush when the interval has elapsed. Call from the consumer loop."""
        if self._clock() - self._last_flush >= self._interval and (self.pending_rows or self._offsets):
            return self.flush(reason="INTERVAL")
        return None

    def flush(self, *, reason: str = "FORCED") -> FlushReport:
        started = self._clock()
        events, clusters = list(self._events), list(self._clusters)
        checkpoints = list(self._checkpoints.values())
        cursor = self._connection.cursor()
        table: str | None = None
        try:
            events_inserted = clusters_inserted = checkpoints_written = 0
            if events:
                table = "rt.option_flow_events"
                events_inserted = self._insert(cursor, table, "_stage_option_flow_events", EVENT_COLUMNS,
                                               [event_values(item) for item in events], "ON CONFLICT DO NOTHING")
                self._verify_replays(cursor, table, "_stage_option_flow_events", EVENT_COLUMNS,
                                     _EVENT_IDENTITY, _EVENT_JOINS)
            if clusters:
                table = "rt.option_flow_clusters"
                clusters_inserted = self._insert(cursor, table, "_stage_option_flow_clusters", CLUSTER_COLUMNS,
                                                 [cluster_values(item) for item in clusters],
                                                 "ON CONFLICT DO NOTHING")
                self._verify_replays(cursor, table, "_stage_option_flow_clusters", CLUSTER_COLUMNS,
                                     _CLUSTER_IDENTITY, _CLUSTER_JOINS)
            if checkpoints:
                table = "rt.option_volume_checkpoints"
                updates = ", ".join(f"{name} = EXCLUDED.{name}" for name in CHECKPOINT_COLUMNS[3:])
                checkpoints_written = self._insert(
                    cursor, table, "_stage_option_volume_checkpoints", CHECKPOINT_COLUMNS,
                    [checkpoint_values(item) for item in checkpoints],
                    f"ON CONFLICT ({', '.join(_CHECKPOINT_KEY)}) DO UPDATE SET {updates}, "
                    "updated_at = clock_timestamp() WHERE EXCLUDED.last_source_seq >= t.last_source_seq",
                    alias="t")
            table = None
            offsets = dict(self._offsets)
            report = FlushReport(
                reason=reason,
                events_inserted=events_inserted,
                events_skipped=len(events) - events_inserted,
                clusters_inserted=clusters_inserted,
                clusters_skipped=len(clusters) - clusters_inserted,
                checkpoints_written=checkpoints_written,
                checkpoints_stale_ignored=len(checkpoints) - checkpoints_written,
                committed_offsets=offsets,
                seconds=self._clock() - started,
            )
            if self._hook is not None:
                self._hook(cursor, report)
            self._connection.commit()
        except psycopg2.Error as error:
            self._connection.rollback()
            raise _translate(error, table) from error
        except BaseException:
            self._connection.rollback()
            raise
        finally:
            cursor.close()
        self._events.clear()
        self._clusters.clear()
        self._checkpoints.clear()
        self._offsets.clear()
        for key, offset in offsets.items():
            if offset > self.committed_offsets.get(key, 0):
                self.committed_offsets[key] = offset
        self._last_flush = self._clock()
        self.flushes += 1
        return report

    def close(self) -> FlushReport | None:
        return self.flush(reason="CLOSE") if (self.pending_rows or self._offsets) else None

    @staticmethod
    def _insert(cursor, table: str, stage: str, columns: tuple[str, ...], rows: list[tuple],
                conflict: str, alias: str = "") -> int:
        names = ", ".join(columns)
        cursor.execute(f"CREATE TEMP TABLE {stage} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP")
        cursor.copy_expert(f"COPY {stage} ({names}) FROM STDIN", io.StringIO(copy_text(rows)))
        target = f"{table} AS {alias}" if alias else table
        cursor.execute(f"INSERT INTO {target} ({names}) SELECT {names} FROM {stage} {conflict}")
        return cursor.rowcount

    @staticmethod
    def _verify_replays(cursor, table: str, stage: str, columns: tuple[str, ...],
                        identity: tuple[str, ...], joins: tuple[str, ...]) -> None:
        """Compare every staged row with the stored row that shares its identity.

        ON CONFLICT DO NOTHING keeps the stored row whatever the replay says. An identical
        replay is a no-op; any difference in a stored column (created_at is not compared)
        means the replay diverged and is an error. Staged rows that were just inserted compare
        equal to themselves, so only conflicting rows can differ.
        """
        staged = ", ".join(f"s.{name}" for name in columns)
        stored = ", ".join(f"t.{name}" for name in columns)
        keys = ", ".join(f"s.{name}" for name in identity)
        divergent: list[tuple] = []
        for join in joins:
            cursor.execute(
                f"SELECT {keys} FROM {stage} s JOIN {table} t ON {join} "
                f"WHERE ({staged}) IS DISTINCT FROM ({stored}) LIMIT 5")
            divergent.extend(tuple(row) for row in cursor.fetchall())
        if divergent:
            unique = list(dict.fromkeys(divergent))[:5]
            raise OptionFlowReplayDivergence(table, unique)


def write_condition_reference(connection, semantics: ConditionSemantics, *,
                              provider: str | None = None) -> tuple[int, int]:
    """Insert the provider facts and the reviewed semantics; both tables are immutable, so an
    existing row is left alone. Returns (condition rows inserted, semantics rows inserted)."""
    codes: Sequence[TradeConditionRow] = semantics.trade_condition_rows(provider)
    reviewed: Sequence[SemanticsRow] = semantics.semantics_rows()
    cursor = connection.cursor()
    try:
        inserted_codes = inserted_semantics = 0
        for row in codes:
            cursor.execute(
                "INSERT INTO ref.trade_condition_codes (provider, asset_class, data_type, condition_id, "
                "abbreviation, name, description, condition_type, legacy, exchange, update_rules, sip_mapping, "
                "fetched_at, payload_sha256) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING",
                (row.provider, row.asset_class, row.data_type, row.condition_id, row.abbreviation, row.name,
                 row.description, row.condition_type, row.legacy, row.exchange, Json(dict(row.update_rules)),
                 Json(dict(row.sip_mapping)), row.fetched_at, row.payload_sha256))
            inserted_codes += cursor.rowcount
        for row in reviewed:
            cursor.execute(
                "INSERT INTO ref.option_condition_semantics (semantics_version, condition_id, treatment, "
                "volume_eligible, is_cancel, is_late, is_multi_leg, is_cross, is_sweep, is_floor, "
                "source_payload_sha256, reviewed_at, review_note) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT DO NOTHING",
                (row.semantics_version, row.condition_id, row.treatment, row.volume_eligible, row.is_cancel,
                 row.is_late, row.is_multi_leg, row.is_cross, row.is_sweep, row.is_floor,
                 row.source_payload_sha256, row.reviewed_at, row.review_note))
            inserted_semantics += cursor.rowcount
        connection.commit()
    except psycopg2.Error as error:
        connection.rollback()
        raise _translate(error, "ref condition reference") from error
    except BaseException:
        connection.rollback()
        raise
    finally:
        cursor.close()
    return inserted_codes, inserted_semantics
