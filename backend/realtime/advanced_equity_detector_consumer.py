"""Provider-free resident consumer wiring for Advanced Equity detector shadow."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Mapping
from uuid import UUID

from .advanced_equity_detector_shadow import (
    detector_shadow_consumer_name,
    AdvancedEquityDetectorShadowBatch,
    build_advanced_equity_detector_shadow_batch,
    persist_advanced_equity_detector_shadow_cursor,
)
from .contracts import canonical_sha256
from .detector_adapters import StableDetectorAdapterResult
from .detector_evidence import RealtimeDetectorInput


MAXIMUM_RECORDS_PER_SLICE = 1000


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class AdvancedEquityDetectorSourceRecord:
    bar_revision_id: UUID
    cohort_id: UUID
    security_id: UUID
    instrument_id: int
    ticker: str
    interval: str
    bar_start: datetime
    bar_end: datetime
    observed_at: datetime
    payload_sha256: str
    journal_generation: UUID
    stream_session_id: UUID
    source_offset: int
    retained_bar: Mapping[str, object]

    def __post_init__(self) -> None:
        started = _utc(self.bar_start, "bar_start")
        ended = _utc(self.bar_end, "bar_end")
        observed = _utc(self.observed_at, "observed_at")
        if not started < ended <= observed:
            raise ValueError("detector source bar clocks are not causal")
        if self.instrument_id <= 0 or self.source_offset <= 0:
            raise ValueError("detector source coordinate is invalid")
        if not self.ticker.strip() or self.interval not in {"1m", "5m"}:
            raise ValueError("detector source bar identity is invalid")
        if len(self.payload_sha256) != 64:
            raise ValueError("detector source payload hash is invalid")
        object.__setattr__(self, "bar_start", started)
        object.__setattr__(self, "bar_end", ended)
        object.__setattr__(self, "observed_at", observed)

    @property
    def progress_coordinate(self) -> int:
        """Where a consumer has got to: its checkpoint offset and the detector state's revision.

        1m records are read in journal offset order, so the offset. 5m records are read in bar order, and a stream
        that reconnects within one journal generation starts its offsets again (Nov 6: about 9,338 before the
        14:35 outage, 109 after), so their coordinate is the bar's end in epoch seconds: it rises with every bar and
        is above any journal offset, so a checkpoint written earlier with an offset still moves forward. The record's
        own journal offset stays in its evidence (`detector_inputs`)."""
        if self.interval == "5m":
            return int(self.bar_end.timestamp())
        return self.source_offset

    def detector_inputs(self) -> tuple[RealtimeDetectorInput, ...]:
        lineage = {
            "journal_generation": str(self.journal_generation),
            "stream_session_id": str(self.stream_session_id),
            "source_offset": self.source_offset,
        }
        return (
            RealtimeDetectorInput(
                input_role="BAR_REVISION",
                input_key=str(self.bar_revision_id),
                source_kind="BAR_REVISION",
                input_sha256=self.payload_sha256,
                available_at=self.bar_end,
                received_at=self.observed_at,
                retained_input=self.retained_bar,
                bar_revision_id=self.bar_revision_id,
            ),
            RealtimeDetectorInput(
                input_role="JOURNAL_BOUNDARY",
                input_key=(
                    f"{self.journal_generation}:{self.source_offset}"
                ),
                source_kind="JOURNAL_EVENT",
                input_sha256=canonical_sha256(lineage),
                available_at=self.bar_end,
                received_at=self.observed_at,
                retained_input=lineage,
                journal_generation=self.journal_generation,
                stream_session_id=self.stream_session_id,
                source_offset=self.source_offset,
            ),
        )


@dataclass(frozen=True, slots=True)
class AdvancedEquityDetectorDecision:
    adapter: StableDetectorAdapterResult
    trigger_identity: str
    valid_until: datetime | None
    disposition: str
    direction: int | None
    reason_codes: tuple[str, ...]
    payload_schema: str
    payload: Mapping[str, object]
    state_key: str
    state_revision: int
    state_payload: Mapping[str, object]
    additional_inputs: tuple[RealtimeDetectorInput, ...] = ()


@dataclass(frozen=True, slots=True)
class AdvancedEquityDetectorConsumerConfig:
    cohort_id: UUID
    journal_generation: UUID
    instrument_id: int
    shard_id: int
    shard_map_sha256: str
    owner_id: UUID
    owner_epoch: int
    lease_resource_key: str
    detector_id: str
    detector_version: str
    code_sha256: str
    policy_sha256: str
    requirements: Mapping[str, object]
    output_kind: str
    readiness_epoch: int
    interval: str = "1m"

    def __post_init__(self) -> None:
        if (
            self.shard_id != 0
            or self.instrument_id <= 0
            or self.owner_epoch <= 0
            or self.readiness_epoch <= 0
            or len(self.shard_map_sha256) != 64
            or not self.lease_resource_key.strip()
            or not self.detector_id.strip()
            or not self.detector_version.strip()
            or self.interval not in {"1m", "5m"}
        ):
            raise ValueError(
                "detector shadow consumer requires the qualified single shard"
            )

    @property
    def consumer_name(self) -> str:
        return detector_shadow_consumer_name(
            detector_id=self.detector_id, instrument_id=self.instrument_id, shard_id=self.shard_id,
        )


@dataclass(frozen=True, slots=True)
class AdvancedEquityDetectorSlice:
    start_offset: int
    committed_offset: int
    records: tuple[AdvancedEquityDetectorSourceRecord, ...]
    batches: tuple[AdvancedEquityDetectorShadowBatch, ...]

    def __post_init__(self) -> None:
        offsets = tuple(row.progress_coordinate for row in self.records)
        if len(self.records) != len(self.batches):
            raise ValueError("detector slice record and batch counts differ")
        if offsets and (
            offsets != tuple(sorted(set(offsets)))
            or offsets[0] <= self.start_offset
            or offsets[-1] != self.committed_offset
        ):
            raise ValueError("detector slice offsets are not monotonic")
        if not offsets and self.committed_offset != self.start_offset:
            raise ValueError("empty detector slice cannot advance")


@dataclass(frozen=True, slots=True)
class AdvancedEquityDetectorProgress:
    committed_offset: int
    last_trigger_at: datetime | None


def claim_advanced_equity_detector_lease_cursor(
    cursor,
    *,
    resource_key: str,
    owner_id: UUID,
    checked_at: datetime,
    lease_seconds: int,
) -> int:
    key = resource_key.strip()
    checked = _utc(checked_at, "checked_at")
    if not key or not 30 <= lease_seconds <= 3600:
        raise ValueError("detector shadow lease scope is invalid")
    cursor.execute(
        "INSERT INTO rt.worker_leases "
        "(resource_key,owner_id,owner_epoch,lease_until,updated_at) "
        "VALUES (%s,%s,1,%s,%s) ON CONFLICT (resource_key) DO UPDATE SET "
        "owner_id=EXCLUDED.owner_id,"
        "owner_epoch=CASE WHEN rt.worker_leases.owner_id=EXCLUDED.owner_id "
        "THEN rt.worker_leases.owner_epoch "
        "ELSE rt.worker_leases.owner_epoch+1 END,"
        "lease_until=EXCLUDED.lease_until,updated_at=EXCLUDED.updated_at "
        "WHERE rt.worker_leases.owner_id=EXCLUDED.owner_id "
        "OR rt.worker_leases.lease_until<=%s "
        "RETURNING owner_epoch",
        (
            key,
            str(owner_id),
            checked + timedelta(seconds=lease_seconds),
            checked,
            checked,
        ),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("detector shadow lease is owned by another worker")
    return int(row[0])


def load_advanced_equity_detector_progress_cursor(
    cursor,
    config: AdvancedEquityDetectorConsumerConfig,
) -> AdvancedEquityDetectorProgress:
    cursor.execute(
        "SELECT owner_epoch,committed_offset,shard_map_sha256,state_payload "
        "FROM rt.consumer_checkpoints WHERE consumer_name=%s "
        "AND cohort_id=%s AND journal_generation=%s AND shard_id=%s",
        (
            config.consumer_name,
            str(config.cohort_id),
            str(config.journal_generation),
            config.shard_id,
        ),
    )
    row = cursor.fetchone()
    if row is None:
        return AdvancedEquityDetectorProgress(0, None)
    if int(row[0]) > config.owner_epoch or str(row[2]) != config.shard_map_sha256:
        raise ValueError("detector shadow checkpoint identity changed")
    try:
        payload = json.loads(bytes(row[3]).decode("ascii"))
        trigger = payload.get("trigger_at")
        last_trigger_at = (
            None
            if trigger is None
            else _utc(
                datetime.fromisoformat(
                    str(trigger).replace("Z", "+00:00")
                ),
                "checkpoint trigger_at",
            )
        )
    except (AttributeError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("detector shadow checkpoint payload is invalid") from None
    return AdvancedEquityDetectorProgress(int(row[1]), last_trigger_at)


def load_advanced_equity_detector_offset_cursor(
    cursor,
    config: AdvancedEquityDetectorConsumerConfig,
) -> int:
    return load_advanced_equity_detector_progress_cursor(
        cursor,
        config,
    ).committed_offset


def load_advanced_equity_detector_records_cursor(
    cursor,
    config: AdvancedEquityDetectorConsumerConfig,
    *,
    after_offset: int,
    after_trigger_at: datetime | None,
    maximum_records: int,
) -> tuple[AdvancedEquityDetectorSourceRecord, ...]:
    if (
        type(after_offset) is not int
        or after_offset < 0
        or (
            after_trigger_at is not None
            and (
                after_trigger_at.tzinfo is None
                or after_trigger_at.utcoffset() is None
            )
        )
        or type(maximum_records) is not int
        or not 1 <= maximum_records <= MAXIMUM_RECORDS_PER_SLICE
    ):
        raise ValueError("detector shadow read bounds are invalid")
    cursor.execute(
        "SELECT revision.bar_revision_id,revision.cohort_id,"
        "revision.security_id,revision.instrument_id,revision.ticker,"
        "revision.interval,revision.bar_start,revision.bar_end,"
        "revision.observed_at,revision.payload_sha256,"
        "COALESCE(revision.journal_generation::text,"
        "lineage.journal_generation)::uuid,"
        "COALESCE(revision.stream_session_id::text,"
        "lineage.stream_session_id)::uuid,"
        "COALESCE(revision.source_offset,lineage.source_offset),"
        "jsonb_build_object("
        "'ticker',revision.ticker,'interval',revision.interval,"
        "'bar_start',revision.bar_start,'bar_end',revision.bar_end,"
        "'open',revision.open_price::text,'high',revision.high_price::text,"
        "'low',revision.low_price::text,'close',revision.close_price::text,"
        "'volume',revision.volume::text,'vwap',revision.vwap::text,"
        "'transaction_count',revision.transaction_count,"
        "'quality_codes',revision.quality_codes,"
        "'source_kind',revision.source_kind,"
        "'source_revision_ids',revision.source_revision_ids,"
        "'payload_sha256',revision.payload_sha256"
        ") AS retained_bar "
        # G102 A4: the current bar per key from the head. This consumer reads its own cohort's journal, so the
        # revision's cohort below is the run's identity, not a key of the current bar.
        "FROM rt.bar_head AS current "
        "JOIN rt.bar_revisions AS revision "
        "ON revision.bar_revision_id=current.bar_revision_id "
        "AND revision.observed_at=current.observed_at "
        "AND revision.instrument_id=current.instrument_id "
        "AND revision.interval=current.interval "
        "AND revision.bar_start=current.bar_start "
        "AND revision.session_scope=current.session_scope "
        "AND revision.adjustment_basis=current.adjustment_basis "
        "LEFT JOIN LATERAL ("
        "SELECT MIN(source.journal_generation::text) AS journal_generation,"
        "MIN(source.stream_session_id::text) AS stream_session_id,"
        "MAX(source.source_offset) AS source_offset,"
        "COUNT(DISTINCT source.journal_generation) AS generation_count,"
        "COUNT(DISTINCT source.stream_session_id) AS session_count "
        "FROM rt.bar_revisions AS source "
        "WHERE source.bar_revision_id=ANY(revision.source_revision_ids)"
        ") AS lineage ON revision.source_kind='DERIVED' "
        "WHERE revision.cohort_id=%s AND revision.instrument_id=%s "
        "AND revision.provider_tier='MASSIVE_ADVANCED' "
        "AND revision.is_final AND revision.interval=%s "
        "AND revision.source_kind IN ('REALTIME_STREAM','DERIVED') "
        "AND COALESCE(revision.journal_generation::text,"
        "lineage.journal_generation)=%s "
        "AND (revision.source_kind='REALTIME_STREAM' OR ("
        "lineage.generation_count=1 AND lineage.session_count=1)) "
        "AND ((%s='1m' AND "
        "COALESCE(revision.source_offset,lineage.source_offset)>%s) "
        "OR (%s='5m' AND (%s::timestamptz IS NULL "
        "OR revision.bar_end>%s::timestamptz) AND EXISTS ("
        "SELECT 1 FROM rt.cycle_receipts AS receipt "
        "WHERE receipt.cohort_id=revision.cohort_id "
        "AND receipt.receipt_kind='INPUT_READY' "
        "AND receipt.cutoff=revision.bar_end))) "
        "ORDER BY revision.bar_end,"
        "COALESCE(revision.source_offset,lineage.source_offset) "
        "LIMIT %s",
        (
            str(config.cohort_id),
            config.instrument_id,
            config.interval,
            str(config.journal_generation),
            config.interval,
            after_offset,
            config.interval,
            after_trigger_at,
            after_trigger_at,
            maximum_records,
        ),
    )
    rows = tuple(cursor.fetchall())
    records = tuple(
        AdvancedEquityDetectorSourceRecord(
            bar_revision_id=UUID(str(row[0])),
            cohort_id=UUID(str(row[1])),
            security_id=UUID(str(row[2])),
            instrument_id=int(row[3]),
            ticker=str(row[4]),
            interval=str(row[5]),
            bar_start=row[6],
            bar_end=row[7],
            observed_at=row[8],
            payload_sha256=str(row[9]),
            journal_generation=UUID(str(row[10])),
            stream_session_id=UUID(str(row[11])),
            source_offset=int(row[12]),
            retained_bar=row[13],
        )
        for row in rows
    )
    if any(
        row.cohort_id != config.cohort_id
        or row.journal_generation != config.journal_generation
        for row in records
    ):
        raise ValueError("detector shadow source scope changed")
    if config.interval == "5m" and records:
        prior = after_trigger_at
        complete = []
        for record in records:
            if prior is not None and record.bar_end - prior != timedelta(minutes=5):
                break
            complete.append(record)
            prior = record.bar_end
        records = tuple(complete)
    return records


def plan_advanced_equity_detector_slice_cursor(
    cursor,
    config: AdvancedEquityDetectorConsumerConfig,
    *,
    evaluator: Callable[
        [
            AdvancedEquityDetectorSourceRecord,
            tuple[RealtimeDetectorInput, ...],
        ],
        AdvancedEquityDetectorDecision,
    ],
    maximum_records: int = 100,
    clock: Callable[[], datetime] = (
        lambda: datetime.now(timezone.utc)
    ),
) -> AdvancedEquityDetectorSlice:
    progress = load_advanced_equity_detector_progress_cursor(cursor, config)
    start_offset = progress.committed_offset
    records = load_advanced_equity_detector_records_cursor(
        cursor,
        config,
        after_offset=start_offset,
        after_trigger_at=progress.last_trigger_at,
        maximum_records=maximum_records,
    )
    batches = []
    for record in records:
        source_inputs = record.detector_inputs()
        decision = evaluator(record, source_inputs)
        if not isinstance(decision, AdvancedEquityDetectorDecision):
            raise TypeError("detector shadow evaluator returned an invalid decision")
        inputs = (*source_inputs, *decision.additional_inputs)
        batches.append(build_advanced_equity_detector_shadow_batch(
            adapter=decision.adapter,
            detector_id=config.detector_id,
            detector_version=config.detector_version,
            code_sha256=config.code_sha256,
            policy_sha256=config.policy_sha256,
            requirements=config.requirements,
            output_kind=config.output_kind,
            instrument_id=record.instrument_id,
            trigger_identity=decision.trigger_identity,
            trigger_at=record.bar_end,
            decision_at=clock(),
            valid_until=decision.valid_until,
            disposition=decision.disposition,
            direction=decision.direction,
            reason_codes=decision.reason_codes,
            readiness_epoch=config.readiness_epoch,
            payload_schema=decision.payload_schema,
            payload=decision.payload,
            inputs=inputs,
            state_key=decision.state_key,
            state_revision=decision.state_revision,
            state_payload=decision.state_payload,
            lease_resource_key=config.lease_resource_key,
            owner_id=config.owner_id,
            journal_generation=config.journal_generation,
            shard_id=config.shard_id,
            shard_map_sha256=config.shard_map_sha256,
            owner_epoch=config.owner_epoch,
            committed_offset=record.progress_coordinate,
            committed_at=clock(),
        ))
    committed_offset = (
        records[-1].progress_coordinate if records else start_offset
    )
    return AdvancedEquityDetectorSlice(
        start_offset=start_offset,
        committed_offset=committed_offset,
        records=records,
        batches=tuple(batches),
    )


def persist_advanced_equity_detector_slice_cursor(
    cursor,
    planned: AdvancedEquityDetectorSlice,
) -> None:
    if not isinstance(planned, AdvancedEquityDetectorSlice):
        raise TypeError("Advanced Equity detector slice is invalid")
    for batch in planned.batches:
        persist_advanced_equity_detector_shadow_cursor(cursor, batch)
