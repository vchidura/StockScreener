"""Durable registry, discovery and capacity contracts for Advanced journals."""
from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import UUID

from .contracts import canonical_sha256
from .journal import JOURNAL_CODEC, JOURNAL_SCHEMA_VERSION


class JournalCapacityError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class JournalCapacityObservation:
    root_sha256: str
    total_bytes: int
    free_bytes: int
    free_percent: float
    minimum_free_bytes: int
    minimum_free_percent: int

    @property
    def admitted(self) -> bool:
        return (
            self.free_bytes >= self.minimum_free_bytes
            and self.free_percent >= self.minimum_free_percent
        )


@dataclass(frozen=True, slots=True)
class AdvancedJournalRegistration:
    journal_generation: UUID
    plan_id: UUID
    stream_session_id: UUID
    cohort_id: UUID
    manifest_id: UUID
    session_date: date
    owner_epoch: int
    relative_path: str
    path_sha256: str
    metadata_sha256: str
    journal_codec: str
    journal_schema_version: int
    normalizer_version: str
    shard_ids: tuple[int, ...]
    shard_map_sha256: str
    status: str
    raw_committed_offset: int
    normalized_committed_offset: int
    bridge_committed_offset: int
    sealed_files_sha256: str | None


@dataclass(frozen=True, slots=True)
class AdvancedJournalRecoveryGate:
    registered: int
    writing: int
    draining: int
    quarantined: int


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def journal_relative_path(root: Path, path: Path) -> str:
    resolved_root = root.resolve()
    resolved_path = path.resolve()
    if not resolved_root.is_absolute() or not resolved_root.is_dir():
        raise ValueError("journal root must be an existing absolute directory")
    try:
        relative = resolved_path.relative_to(resolved_root)
    except ValueError as error:
        raise ValueError("journal path must be beneath the mounted root") from error
    if not relative.parts or ".." in relative.parts:
        raise ValueError("journal relative path is invalid")
    return relative.as_posix()


def resolve_registered_journal_path(root: Path, relative_path: str) -> Path:
    normalized = Path(relative_path)
    if normalized.is_absolute() or ".." in normalized.parts:
        raise ValueError("registered journal path is not container-stable")
    resolved_root = root.resolve()
    resolved = (resolved_root / normalized).resolve()
    try:
        resolved.relative_to(resolved_root)
    except ValueError as error:
        raise ValueError("registered journal escaped the mounted root") from error
    return resolved


def measure_journal_capacity(
    root: Path,
    *,
    minimum_free_bytes: int,
    minimum_free_percent: int,
    disk_usage=shutil.disk_usage,
) -> JournalCapacityObservation:
    if (
        type(minimum_free_bytes) is not int
        or minimum_free_bytes <= 0
        or type(minimum_free_percent) is not int
        or not 1 <= minimum_free_percent <= 100
    ):
        raise ValueError("journal capacity thresholds are invalid")
    resolved = root.resolve()
    if not resolved.is_dir():
        raise ValueError("journal capacity root must exist")
    total, _used, free = disk_usage(resolved)
    if total <= 0 or free < 0 or free > total:
        raise ValueError("journal disk usage result is invalid")
    return JournalCapacityObservation(
        root_sha256=canonical_sha256(str(resolved)),
        total_bytes=int(total),
        free_bytes=int(free),
        free_percent=(float(free) * 100.0 / float(total)),
        minimum_free_bytes=minimum_free_bytes,
        minimum_free_percent=minimum_free_percent,
    )


def build_advanced_journal_registration(
    *,
    root: Path,
    path: Path,
    journal_generation: UUID,
    plan_id: UUID,
    stream_session_id: UUID,
    cohort_id: UUID,
    manifest_id: UUID,
    session_date: date,
    owner_epoch: int,
    normalizer_version: str,
    shard_ids: tuple[int, ...],
    shard_map_sha256: str,
) -> AdvancedJournalRegistration:
    relative_path = journal_relative_path(root, path)
    normalized_version = normalizer_version.strip()
    normalized_shards = tuple(sorted(set(shard_ids)))
    if (
        type(owner_epoch) is not int
        or owner_epoch <= 0
        or not normalized_version
        or not normalized_shards
        or normalized_shards != shard_ids
        or any(type(shard) is not int or shard < 0 for shard in shard_ids)
    ):
        raise ValueError("Advanced journal registry identity is invalid")
    path_sha256 = canonical_sha256(relative_path)
    identity = {
        "contract": "advanced-journal-registry-v1",
        "journal_generation": journal_generation,
        "plan_id": plan_id,
        "stream_session_id": stream_session_id,
        "cohort_id": cohort_id,
        "manifest_id": manifest_id,
        "session_date": session_date,
        "owner_epoch": owner_epoch,
        "relative_path": relative_path,
        "path_sha256": path_sha256,
        "journal_codec": JOURNAL_CODEC,
        "journal_schema_version": JOURNAL_SCHEMA_VERSION,
        "normalizer_version": normalized_version,
        "shard_ids": normalized_shards,
        "shard_map_sha256": shard_map_sha256,
    }
    return AdvancedJournalRegistration(
        journal_generation,
        plan_id,
        stream_session_id,
        cohort_id,
        manifest_id,
        session_date,
        owner_epoch,
        relative_path,
        path_sha256,
        canonical_sha256(identity),
        JOURNAL_CODEC,
        JOURNAL_SCHEMA_VERSION,
        normalized_version,
        normalized_shards,
        shard_map_sha256,
        "REGISTERED",
        0,
        0,
        0,
        None,
    )


def _registration_from_row(row) -> AdvancedJournalRegistration:
    return AdvancedJournalRegistration(
        UUID(str(row[0])),
        UUID(str(row[1])),
        UUID(str(row[2])),
        UUID(str(row[3])),
        UUID(str(row[4])),
        row[5],
        int(row[6]),
        str(row[7]),
        str(row[8]),
        str(row[9]),
        str(row[10]),
        int(row[11]),
        str(row[12]),
        tuple(int(value) for value in row[13]),
        str(row[14]),
        str(row[15]),
        int(row[16]),
        int(row[17]),
        int(row[18]),
        str(row[19]) if row[19] is not None else None,
    )


_REGISTRY_SELECT = (
    "SELECT journal_generation,plan_id,stream_session_id,cohort_id,"
    "manifest_id,session_date,owner_epoch,relative_path,path_sha256,"
    "metadata_sha256,journal_codec,journal_schema_version,"
    "normalizer_version,shard_ids,shard_map_sha256,status,"
    "raw_committed_offset,normalized_committed_offset,"
    "bridge_committed_offset,sealed_files_sha256 "
    "FROM rt.advanced_journal_registry "
)


def register_advanced_journal_cursor(
    cursor,
    registration: AdvancedJournalRegistration,
) -> AdvancedJournalRegistration:
    cursor.execute(
        "INSERT INTO rt.advanced_journal_registry ("
        "journal_generation,plan_id,stream_session_id,cohort_id,"
        "manifest_id,session_date,owner_epoch,relative_path,path_sha256,"
        "metadata_sha256,journal_codec,journal_schema_version,"
        "normalizer_version,shard_ids,shard_map_sha256,status"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
        "'REGISTERED') ON CONFLICT (journal_generation) DO NOTHING",
        (
            str(registration.journal_generation),
            str(registration.plan_id),
            str(registration.stream_session_id),
            str(registration.cohort_id),
            str(registration.manifest_id),
            registration.session_date,
            registration.owner_epoch,
            registration.relative_path,
            registration.path_sha256,
            registration.metadata_sha256,
            registration.journal_codec,
            registration.journal_schema_version,
            registration.normalizer_version,
            list(registration.shard_ids),
            registration.shard_map_sha256,
        ),
    )
    cursor.execute(
        _REGISTRY_SELECT + "WHERE journal_generation=%s FOR UPDATE",
        (str(registration.journal_generation),),
    )
    stored = cursor.fetchone()
    if stored is None:
        raise ValueError("Advanced journal registration was not stored")
    result = _registration_from_row(stored)
    if result.metadata_sha256 != registration.metadata_sha256:
        raise ValueError("Advanced journal registration conflicts with identity")
    return result


def mark_advanced_journal_writing_cursor(
    cursor,
    journal_generation: UUID,
    *,
    checked_at: datetime,
) -> None:
    cursor.execute(
        "UPDATE rt.advanced_journal_registry SET status='WRITING',"
        "updated_at=%s WHERE journal_generation=%s "
        "AND status='REGISTERED' RETURNING journal_generation",
        (_utc(checked_at, "checked_at"), str(journal_generation)),
    )
    if cursor.fetchone() is None:
        raise ValueError("Advanced journal could not enter WRITING")


def update_advanced_journal_offsets_cursor(
    cursor,
    journal_generation: UUID,
    *,
    raw_committed_offset: int,
    normalized_committed_offset: int,
    bridge_committed_offset: int | None,
    checked_at: datetime,
) -> None:
    if (
        type(raw_committed_offset) is not int
        or raw_committed_offset < 0
        or type(normalized_committed_offset) is not int
        or normalized_committed_offset < 0
        or (
            bridge_committed_offset is not None
            and (
                type(bridge_committed_offset) is not int
                or bridge_committed_offset < 0
            )
        )
    ):
        raise ValueError("Advanced journal offsets are invalid")
    cursor.execute(
        "UPDATE rt.advanced_journal_registry SET "
        "raw_committed_offset=GREATEST(raw_committed_offset,%s),"
        "normalized_committed_offset="
        "GREATEST(normalized_committed_offset,%s),"
        "bridge_committed_offset=GREATEST("
        "bridge_committed_offset,COALESCE(%s,bridge_committed_offset)),"
        "updated_at=%s WHERE journal_generation=%s "
        "AND status IN ('WRITING','DRAINING') "
        "RETURNING journal_generation",
        (
            raw_committed_offset,
            normalized_committed_offset,
            bridge_committed_offset,
            _utc(checked_at, "checked_at"),
            str(journal_generation),
        ),
    )
    if cursor.fetchone() is None:
        raise ValueError("Advanced journal offset update was rejected")


def seal_advanced_journal_cursor(
    cursor,
    journal_generation: UUID,
    *,
    raw_committed_offset: int,
    normalized_committed_offset: int,
    sealed_files_sha256: str,
    checked_at: datetime,
) -> None:
    cursor.execute(
        "UPDATE rt.advanced_journal_registry SET status='DRAINING',"
        "raw_committed_offset=GREATEST(raw_committed_offset,%s),"
        "normalized_committed_offset="
        "GREATEST(normalized_committed_offset,%s),"
        "sealed_files_sha256=%s,sealed_at=%s,updated_at=%s "
        "WHERE journal_generation=%s "
        "AND status IN ('REGISTERED','WRITING','DRAINING') "
        "RETURNING journal_generation",
        (
            raw_committed_offset,
            normalized_committed_offset,
            sealed_files_sha256,
            _utc(checked_at, "checked_at"),
            _utc(checked_at, "checked_at"),
            str(journal_generation),
        ),
    )
    if cursor.fetchone() is None:
        raise ValueError("Advanced journal seal was rejected")


def mark_advanced_journal_drained_cursor(
    cursor,
    journal_generation: UUID,
    *,
    bridge_committed_offset: int,
    checked_at: datetime,
) -> None:
    cursor.execute(
        "UPDATE rt.advanced_journal_registry SET status='DRAINED',"
        "bridge_committed_offset=GREATEST(bridge_committed_offset,%s),"
        "drained_at=%s,updated_at=%s "
        "WHERE journal_generation=%s AND status='DRAINING' "
        "AND normalized_committed_offset<=%s "
        "RETURNING journal_generation",
        (
            bridge_committed_offset,
            _utc(checked_at, "checked_at"),
            _utc(checked_at, "checked_at"),
            str(journal_generation),
            bridge_committed_offset,
        ),
    )
    if cursor.fetchone() is None:
        raise ValueError("Advanced journal is not fully drained")


def quarantine_advanced_journal_cursor(
    cursor,
    journal_generation: UUID,
    *,
    reason: str,
    detail: str,
    checked_at: datetime,
) -> None:
    reason_code = reason.strip().upper()
    normalized_detail = detail.strip()
    if (
        not 1 <= len(reason_code) <= 64
        or not 1 <= len(normalized_detail) <= 320
    ):
        raise ValueError("Advanced journal quarantine evidence is invalid")
    cursor.execute(
        "UPDATE rt.advanced_journal_registry SET status='QUARANTINED',"
        "quarantine_reason=%s,quarantine_detail=%s,quarantined_at=%s,"
        "updated_at=%s WHERE journal_generation=%s "
        "AND status NOT IN ('DRAINED','QUARANTINED') "
        "RETURNING journal_generation",
        (
            reason_code,
            normalized_detail,
            _utc(checked_at, "checked_at"),
            _utc(checked_at, "checked_at"),
            str(journal_generation),
        ),
    )
    if cursor.fetchone() is None:
        raise ValueError("Advanced journal quarantine was rejected")


def discover_incomplete_advanced_journals_cursor(
    cursor,
) -> tuple[AdvancedJournalRegistration, ...]:
    cursor.execute(
        _REGISTRY_SELECT
        + "WHERE status IN ('REGISTERED','WRITING','DRAINING') "
        "ORDER BY session_date,created_at,journal_generation"
    )
    return tuple(_registration_from_row(row) for row in cursor.fetchall())


def advanced_journal_recovery_gate_cursor(
    cursor,
    plan_id: UUID,
) -> AdvancedJournalRecoveryGate:
    cursor.execute(
        "SELECT "
        "COUNT(*) FILTER (WHERE status='REGISTERED'),"
        "COUNT(*) FILTER (WHERE status='WRITING'),"
        "COUNT(*) FILTER (WHERE status='DRAINING'),"
        "COUNT(*) FILTER (WHERE status='QUARANTINED') "
        "FROM rt.advanced_journal_registry WHERE plan_id=%s",
        (str(plan_id),),
    )
    row = cursor.fetchone()
    return AdvancedJournalRecoveryGate(*(int(value) for value in row))
