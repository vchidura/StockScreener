from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import (
    AssetClass,
    MarketChannel,
    canonical_json,
    canonical_sha256,
)
from .database_access import RealtimeDatabaseRole
from .equity_bar_adapter import (
    AdvancedMinuteFinality,
    adapt_advanced_minute_aggregate,
)
from .equity_bar_repository import persist_realtime_equity_bars_cursor
from .journal import DecoderProgress, NormalizedJournalRecord, RawJournalReader
from .preflight import ProviderTier
from .stage_database_v2 import V2WorkerDatabase


ADVANCED_EQUITY_BRIDGE_CONSUMER = "advanced-equity-am-v1"
ADVANCED_EQUITY_BRIDGE_STATE_CODEC = "advanced_equity_am_checkpoint_v1"
MAX_ADVANCED_EQUITY_BATCH = 1_000


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class AdvancedEquityJournalBridgeConfig:
    cohort_id: UUID
    journal_generation: UUID
    stream_session_id: UUID
    owner_epoch: int
    normalizer_version: str
    shard_ids: tuple[int, ...]
    batch_limit: int = 500
    consumer_name: str = ADVANCED_EQUITY_BRIDGE_CONSUMER

    def __post_init__(self) -> None:
        version = self.normalizer_version.strip()
        consumer = self.consumer_name.strip()
        shards = tuple(sorted(self.shard_ids))
        if type(self.owner_epoch) is not int or self.owner_epoch <= 0:
            raise ValueError("Advanced equity bridge owner epoch must be positive")
        if not version or not 1 <= len(consumer) <= 96:
            raise ValueError("Advanced equity bridge identity cannot be blank")
        if (
            not shards
            or len(shards) != len(set(shards))
            or any(type(value) is not int or value < 0 for value in shards)
        ):
            raise ValueError("Advanced equity bridge shards must be unique nonnegative IDs")
        if not 1 <= self.batch_limit <= MAX_ADVANCED_EQUITY_BATCH:
            raise ValueError("Advanced equity bridge batch limit must be in [1, 1000]")
        object.__setattr__(self, "normalizer_version", version)
        object.__setattr__(self, "consumer_name", consumer)
        object.__setattr__(self, "shard_ids", shards)

    @property
    def shard_map_sha256(self) -> str:
        return canonical_sha256(
            {
                "contract": "advanced-equity-am-shards-v1",
                "consumer_name": self.consumer_name,
                "shard_ids": self.shard_ids,
            }
        )


@dataclass(frozen=True, slots=True)
class AdvancedEquityJournalBatch:
    progress: DecoderProgress
    scanned_records: tuple[NormalizedJournalRecord, ...]
    processable_records: tuple[NormalizedJournalRecord, ...]
    start_offset: int
    scanned_through_offset: int
    event_time_watermark: datetime
    proved_at: datetime
    expected_offsets: Mapping[int, int]
    target_offsets: Mapping[int, int]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "event_time_watermark",
            _utc(self.event_time_watermark, "event_time_watermark"),
        )
        object.__setattr__(self, "proved_at", _utc(self.proved_at, "proved_at"))
        object.__setattr__(self, "expected_offsets", dict(self.expected_offsets))
        object.__setattr__(self, "target_offsets", dict(self.target_offsets))


@dataclass(frozen=True, slots=True)
class AdvancedEquityJournalBridgeStatus:
    checked_at: datetime
    start_offset: int
    committed_offset: int
    scanned_records: int
    persisted_bars: int
    shards_advanced: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "checked_at", _utc(self.checked_at, "checked_at"))
        values = (
            self.start_offset,
            self.committed_offset,
            self.scanned_records,
            self.persisted_bars,
            self.shards_advanced,
        )
        if any(type(value) is not int or value < 0 for value in values):
            raise ValueError("Advanced equity bridge counters must be nonnegative")
        if self.committed_offset < self.start_offset:
            raise ValueError("Advanced equity bridge checkpoint moved backward")


def _validate_checkpoint_offsets(
    config: AdvancedEquityJournalBridgeConfig,
    shard_offsets: Mapping[int, int],
) -> dict[int, int]:
    offsets = dict(shard_offsets)
    if set(offsets) != set(config.shard_ids) or any(
        type(value) is not int or value < 0 for value in offsets.values()
    ):
        raise ValueError("Advanced equity bridge checkpoint shard set is invalid")
    return offsets


def scan_advanced_equity_journal_batch(
    reader: RawJournalReader,
    config: AdvancedEquityJournalBridgeConfig,
    shard_offsets: Mapping[int, int],
) -> AdvancedEquityJournalBatch | None:
    if not callable(getattr(reader, "decoder_progress", None)) or not callable(
        getattr(reader, "read_normalized_after", None)
    ):
        raise TypeError("Advanced equity bridge reader is invalid")
    if not isinstance(config, AdvancedEquityJournalBridgeConfig):
        raise TypeError("Advanced equity bridge config is invalid")
    offsets = _validate_checkpoint_offsets(config, shard_offsets)
    start_offset = min(offsets.values())
    progress = reader.decoder_progress(config.normalizer_version)
    if progress is None:
        return None
    if (
        progress.generation != config.journal_generation
        or progress.normalizer_version != config.normalizer_version
    ):
        raise ValueError("Advanced equity decoder progress identity changed")
    if progress.committed_event_offset < start_offset:
        raise ValueError("Advanced equity decoder progress is behind PostgreSQL")
    fetched = reader.read_normalized_after(start_offset, limit=config.batch_limit)
    rows = tuple(
        row for row in fetched
        if row.event_offset <= progress.committed_event_offset
    )
    if not rows:
        if progress.committed_event_offset > start_offset:
            raise ValueError("Advanced equity committed journal batch is unavailable")
        return None
    expected_event_offsets = tuple(
        range(start_offset + 1, rows[-1].event_offset + 1)
    )
    if tuple(row.event_offset for row in rows) != expected_event_offsets:
        raise ValueError("Advanced equity normalized journal offsets are not contiguous")
    if any(
        row.normalizer_version != config.normalizer_version
        or row.journal_generation != config.journal_generation
        or row.stream_session_id != config.stream_session_id
        for row in rows
    ):
        raise ValueError("Advanced equity normalized journal lineage changed")
    proved_at = _utc(progress.updated_at, "decoder progress updated_at")
    if any(proved_at < row.committed_at for row in rows):
        raise ValueError("Advanced equity decoder progress predates its committed batch")
    configured_shards = set(config.shard_ids)
    processable = []
    for row in rows:
        relevant_am = (
            row.asset_class is AssetClass.EQUITY
            and row.event_kind is MarketChannel.MINUTE_AGGREGATE
        )
        if relevant_am and row.shard_id not in configured_shards:
            raise ValueError("Advanced equity AM record belongs to an unconfigured shard")
        if relevant_am and row.quality != "GOOD":
            raise ValueError("Advanced equity AM record has invalid finality")
        if (
            relevant_am
            and row.event_offset > offsets[row.shard_id]
        ):
            processable.append(row)
    scanned_through = rows[-1].event_offset
    return AdvancedEquityJournalBatch(
        progress=progress,
        scanned_records=rows,
        processable_records=tuple(processable),
        start_offset=start_offset,
        scanned_through_offset=scanned_through,
        event_time_watermark=max(row.event_at for row in rows),
        proved_at=proved_at,
        expected_offsets=offsets,
        target_offsets={
            shard_id: max(offset, scanned_through)
            for shard_id, offset in offsets.items()
        },
    )


def _load_checkpoint_offsets_cursor(
    cursor,
    config: AdvancedEquityJournalBridgeConfig,
) -> dict[int, int]:
    cursor.execute(
        "SELECT shard_id, shard_map_sha256, owner_epoch, committed_offset "
        "FROM rt.consumer_checkpoints WHERE consumer_name=%s AND cohort_id=%s "
        "AND journal_generation=%s AND shard_id=ANY(%s::smallint[])",
        (
            config.consumer_name,
            str(config.cohort_id),
            str(config.journal_generation),
            list(config.shard_ids),
        ),
    )
    rows = tuple(cursor.fetchall())
    if len(rows) != len({int(row[0]) for row in rows}):
        raise ValueError("Advanced equity checkpoints contain duplicate shards")
    offsets = {shard_id: 0 for shard_id in config.shard_ids}
    for shard_id, shard_map_sha256, owner_epoch, committed_offset in rows:
        shard = int(shard_id)
        if shard not in offsets:
            raise ValueError("Advanced equity checkpoint shard is not configured")
        if shard_map_sha256 != config.shard_map_sha256:
            raise ValueError("Advanced equity checkpoint shard map changed")
        if int(owner_epoch) > config.owner_epoch:
            raise ValueError("Advanced equity bridge owner epoch is stale")
        offsets[shard] = int(committed_offset)
    return offsets


def _verify_stream_session_cursor(
    cursor,
    config: AdvancedEquityJournalBridgeConfig,
) -> None:
    cursor.execute(
        "SELECT session.owner_epoch, session.feed, session.manifest_id, "
        "profile.provider_tier "
        "FROM rt.stream_sessions AS session "
        "JOIN rt.cohorts AS cohort ON cohort.manifest_id=session.manifest_id "
        "JOIN rt.subscription_manifests AS manifest "
        "ON manifest.manifest_id=session.manifest_id "
        "JOIN rt.capability_profiles AS profile "
        "ON profile.profile_id=manifest.profile_id "
        "WHERE session.session_id=%s AND cohort.cohort_id=%s",
        (str(config.stream_session_id), str(config.cohort_id)),
    )
    row = cursor.fetchone()
    if (
        row is None
        or int(row[0]) != config.owner_epoch
        or row[1] != "STOCKS"
        or row[3] != ProviderTier.MASSIVE_ADVANCED.value
    ):
        raise ValueError(
            "Advanced equity stream session, cohort or owner epoch is not registered"
        )


def _require_no_unresolved_am_gaps_cursor(
    cursor,
    config: AdvancedEquityJournalBridgeConfig,
    *,
    scanned_through_offset: int,
) -> None:
    cursor.execute(
        "SELECT gap_id FROM rt.stream_gaps WHERE session_id=%s "
        "AND journal_generation=%s AND shard_id=ANY(%s::smallint[]) "
        "AND (channel IS NULL OR channel='AM') "
        "AND recovery_status NOT IN ('REPAIRED', 'WAIVED') "
        "AND last_durable_offset < %s ORDER BY shard_id, last_durable_offset LIMIT 1",
        (
            str(config.stream_session_id),
            str(config.journal_generation),
            list(config.shard_ids),
            scanned_through_offset,
        ),
    )
    if cursor.fetchone() is not None:
        raise ValueError("unresolved Advanced equity AM gap blocks checkpoint advancement")


def _load_security_ids_cursor(
    cursor,
    config: AdvancedEquityJournalBridgeConfig,
    records: tuple[NormalizedJournalRecord, ...],
    *,
    proved_at: datetime,
) -> dict[int, UUID]:
    if not records:
        return {}
    grouped: dict[int, list[NormalizedJournalRecord]] = {}
    for record in records:
        grouped.setdefault(record.instrument_id, []).append(record)
    result = {}
    for instrument_id, own in grouped.items():
        symbols = {row.provider_symbol for row in own}
        if len(symbols) != 1:
            raise ValueError("Advanced equity instrument changed ticker within a batch")
        first_at = min(row.event_at for row in own)
        last_at = max(row.event_at for row in own)
        symbol = next(iter(symbols))
        cursor.execute(
            "SELECT instrument.security_id, reference.ticker "
            "FROM rt.stream_sessions AS session "
            "JOIN rt.subscription_members AS member "
            "ON member.manifest_id=session.manifest_id "
            "JOIN rt.instruments AS instrument USING (instrument_id) "
            "JOIN ref.security_revisions AS reference USING (security_id) "
            "WHERE session.session_id=%s AND instrument.instrument_id=%s "
            "AND instrument.asset_class='EQUITY' AND member.channel='AM' "
            "AND reference.ticker=%s AND reference.effective_from<=%s "
            "AND (reference.effective_to IS NULL OR reference.effective_to>%s) "
            "AND reference.observed_at<=%s "
            "ORDER BY reference.effective_from DESC, reference.observed_at DESC "
            "LIMIT 1",
            (
                str(config.stream_session_id),
                instrument_id,
                symbol,
                first_at,
                last_at,
                proved_at,
            ),
        )
        row = cursor.fetchone()
        if row is None or row[1] != symbol:
            raise ValueError(
                "Advanced equity AM instrument is not an exact registered member"
            )
        result[instrument_id] = UUID(str(row[0]))
    return result


def _checkpoint_state(
    config: AdvancedEquityJournalBridgeConfig,
    batch: AdvancedEquityJournalBatch,
    shard_id: int,
    target_offset: int,
) -> tuple[UUID, str, bytes]:
    payload = canonical_json(
        {
            "version": ADVANCED_EQUITY_BRIDGE_STATE_CODEC,
            "consumer_name": config.consumer_name,
            "cohort_id": config.cohort_id,
            "journal_generation": config.journal_generation,
            "stream_session_id": config.stream_session_id,
            "normalizer_version": config.normalizer_version,
            "shard_id": shard_id,
            "scanned_through_event_offset": target_offset,
            "decoder_committed_raw_offset": batch.progress.committed_raw_offset,
            "decoder_committed_event_offset": batch.progress.committed_event_offset,
            "event_time_watermark": batch.event_time_watermark,
        }
    ).encode("utf-8")
    state_hash = hashlib.sha256(payload).hexdigest()
    checkpoint_id = uuid5(
        NAMESPACE_URL,
        "advanced-equity-am-checkpoint:"
        f"{config.consumer_name}:{config.cohort_id}:{config.journal_generation}:"
        f"{shard_id}:{config.owner_epoch}:{target_offset}:{state_hash}",
    )
    return checkpoint_id, state_hash, payload


def _persist_consumer_checkpoints_cursor(
    cursor,
    config: AdvancedEquityJournalBridgeConfig,
    batch: AdvancedEquityJournalBatch,
    *,
    expected_offsets: Mapping[int, int],
    target_offsets: Mapping[int, int],
) -> int:
    expected = _validate_checkpoint_offsets(config, expected_offsets)
    targets = _validate_checkpoint_offsets(config, target_offsets)
    cursor.execute(
        "SELECT shard_id, shard_map_sha256, owner_epoch, committed_offset "
        "FROM rt.consumer_checkpoints WHERE consumer_name=%s AND cohort_id=%s "
        "AND journal_generation=%s AND shard_id=ANY(%s::smallint[]) FOR UPDATE",
        (
            config.consumer_name,
            str(config.cohort_id),
            str(config.journal_generation),
            list(config.shard_ids),
        ),
    )
    current = {shard_id: 0 for shard_id in config.shard_ids}
    for shard_id, shard_map_sha256, owner_epoch, committed_offset in cursor.fetchall():
        shard = int(shard_id)
        if (
            shard_map_sha256 != config.shard_map_sha256
            or int(owner_epoch) > config.owner_epoch
        ):
            raise ValueError("Advanced equity checkpoint fence rejected the owner")
        current[shard] = int(committed_offset)
    if current != expected:
        raise ValueError("Advanced equity checkpoints changed while scanning the journal")
    advanced = 0
    for shard_id in config.shard_ids:
        target = targets[shard_id]
        if target < expected[shard_id]:
            raise ValueError("Advanced equity checkpoint cannot move backward")
        if target == expected[shard_id]:
            continue
        checkpoint_id, state_hash, payload = _checkpoint_state(
            config, batch, shard_id, target
        )
        cursor.execute(
            "INSERT INTO rt.consumer_checkpoints ("
            "consumer_name, cohort_id, journal_generation, shard_id, "
            "shard_map_sha256, owner_epoch, committed_offset, checkpoint_id, "
            "state_codec, state_hash, state_payload, committed_at) VALUES ("
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (consumer_name, cohort_id, journal_generation, shard_id) "
            "DO UPDATE SET shard_map_sha256=EXCLUDED.shard_map_sha256, "
            "owner_epoch=EXCLUDED.owner_epoch, "
            "committed_offset=EXCLUDED.committed_offset, "
            "checkpoint_id=EXCLUDED.checkpoint_id, "
            "state_codec=EXCLUDED.state_codec, state_hash=EXCLUDED.state_hash, "
            "state_payload=EXCLUDED.state_payload, "
            "committed_at=EXCLUDED.committed_at, updated_at=clock_timestamp() "
            "WHERE rt.consumer_checkpoints.owner_epoch<=EXCLUDED.owner_epoch "
            "AND rt.consumer_checkpoints.committed_offset<EXCLUDED.committed_offset "
            "RETURNING checkpoint_id",
            (
                config.consumer_name,
                str(config.cohort_id),
                str(config.journal_generation),
                shard_id,
                config.shard_map_sha256,
                config.owner_epoch,
                target,
                str(checkpoint_id),
                ADVANCED_EQUITY_BRIDGE_STATE_CODEC,
                state_hash,
                payload,
                batch.proved_at,
            ),
        )
        stored = cursor.fetchone()
        if stored is None or str(stored[0]) != str(checkpoint_id):
            raise ValueError("Advanced equity checkpoint advancement was rejected")
        advanced += 1
    return advanced


def process_advanced_equity_journal_once(
    database: V2WorkerDatabase,
    reader: RawJournalReader,
    config: AdvancedEquityJournalBridgeConfig,
    *,
    checked_at: datetime,
) -> AdvancedEquityJournalBridgeStatus:
    if not isinstance(database, V2WorkerDatabase):
        raise TypeError("Advanced equity bridge requires V2WorkerDatabase")
    if database.process_plan.role is not RealtimeDatabaseRole.COMPUTE:
        raise ValueError("Advanced equity bridge requires the compute role")
    checked = _utc(checked_at, "checked_at")
    with database.transaction(read_only=True) as cursor:
        expected_offsets = _load_checkpoint_offsets_cursor(cursor, config)
    batch = scan_advanced_equity_journal_batch(reader, config, expected_offsets)
    if batch is None:
        offset = min(expected_offsets.values())
        return AdvancedEquityJournalBridgeStatus(checked, offset, offset, 0, 0, 0)
    if checked < batch.proved_at:
        raise ValueError("Advanced equity bridge check time predates decoder proof")
    with database.transaction(isolation_level="SERIALIZABLE") as cursor:
        _verify_stream_session_cursor(cursor, config)
        _require_no_unresolved_am_gaps_cursor(
            cursor,
            config,
            scanned_through_offset=batch.scanned_through_offset,
        )
        security_ids = _load_security_ids_cursor(
            cursor,
            config,
            batch.processable_records,
            proved_at=batch.proved_at,
        )
        finality = AdvancedMinuteFinality(
            journal_generation=config.journal_generation,
            stream_session_id=config.stream_session_id,
            owner_epoch=config.owner_epoch,
            normalized_through_offset=batch.scanned_through_offset,
            event_time_watermark=batch.event_time_watermark,
            proved_at=batch.proved_at,
            gap_free=True,
        )
        facts = tuple(
            adapt_advanced_minute_aggregate(
                record,
                cohort_id=config.cohort_id,
                security_id=security_ids[record.instrument_id],
                finality=finality,
            )
            for record in batch.processable_records
        )
        if facts:
            persist_realtime_equity_bars_cursor(cursor, facts)
        shards_advanced = _persist_consumer_checkpoints_cursor(
            cursor,
            config,
            batch,
            expected_offsets=batch.expected_offsets,
            target_offsets=batch.target_offsets,
        )
    return AdvancedEquityJournalBridgeStatus(
        checked_at=checked,
        start_offset=batch.start_offset,
        committed_offset=batch.scanned_through_offset,
        scanned_records=len(batch.scanned_records),
        persisted_bars=len(facts),
        shards_advanced=shards_advanced,
    )
