from __future__ import annotations

import json
from datetime import date, datetime
from uuid import UUID

from psycopg2.extras import Json

from options.domain import (
    ManifestCreationStatus,
    RawFileEventType,
    RawFileManifest,
    RetentionHold,
)
from options.errors import InvalidBatchTransition

from .base import ConnectionFactory, PostgresRepository


class OptionRetentionRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def record_completed_manifest(self, manifest: RawFileManifest) -> bool:
        if manifest.creation_status is not ManifestCreationStatus.COMPLETE:
            raise ValueError("only a validated complete file can enter the manifest")
        with self._cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO option_raw_file_manifests (
                    file_id, event_type, market_date, underlying, market_hour,
                    object_key, schema_version, row_count, minimum_source_time,
                    maximum_source_time, byte_size, payload_sha256,
                    creation_status, retention_class
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    'COMPLETE', %s
                )
                ON CONFLICT (object_key) DO NOTHING
                """,
                (
                    manifest.file_id,
                    manifest.event_type.value,
                    manifest.market_date,
                    manifest.underlyer,
                    manifest.market_hour,
                    manifest.object_key,
                    manifest.schema_version,
                    manifest.row_count,
                    manifest.minimum_source_time,
                    manifest.maximum_source_time,
                    manifest.byte_size,
                    manifest.payload_sha256,
                    manifest.retention_class,
                ),
            )
            return cursor.rowcount == 1

    def mark_integrity_failure(
        self,
        file_id: UUID,
        status: ManifestCreationStatus,
    ) -> None:
        if status not in (ManifestCreationStatus.MISSING, ManifestCreationStatus.CORRUPT):
            raise ValueError("integrity status must be MISSING or CORRUPT")
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE option_raw_file_manifests
                SET creation_status = %s, updated_at = NOW()
                WHERE file_id = %s AND creation_status <> 'DELETED'
                """,
                (status.value, file_id),
            )
            if cursor.rowcount != 1:
                raise InvalidBatchTransition("manifest is missing or already deleted")

    def get_manifest_by_object_key(self, object_key: str) -> RawFileManifest | None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    file_id, event_type, market_date, underlying, market_hour,
                    object_key, schema_version, row_count, minimum_source_time,
                    maximum_source_time, byte_size, payload_sha256,
                    creation_status, retention_class
                FROM option_raw_file_manifests
                WHERE object_key = %s
                """,
                (object_key,),
            )
            row = cursor.fetchone()
        return _manifest(row) if row else None

    def list_non_deleted_manifests(self) -> tuple[RawFileManifest, ...]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    file_id, event_type, market_date, underlying, market_hour,
                    object_key, schema_version, row_count, minimum_source_time,
                    maximum_source_time, byte_size, payload_sha256,
                    creation_status, retention_class
                FROM option_raw_file_manifests
                WHERE creation_status <> 'DELETED'
                ORDER BY market_date, underlying, market_hour, object_key
                """
            )
            rows = cursor.fetchall()
        return tuple(_manifest(row) for row in rows)

    def list_file_retention_candidates(
        self,
        cutoff_market_date: date,
        assessed_at: datetime,
    ) -> tuple[tuple[RawFileManifest, int], ...]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    manifest.file_id,
                    manifest.event_type,
                    manifest.market_date,
                    manifest.underlying,
                    manifest.market_hour,
                    manifest.object_key,
                    manifest.schema_version,
                    manifest.row_count,
                    manifest.minimum_source_time,
                    manifest.maximum_source_time,
                    manifest.byte_size,
                    manifest.payload_sha256,
                    manifest.creation_status,
                    manifest.retention_class,
                    COUNT(hold.hold_id) AS active_hold_count
                FROM option_raw_file_manifests AS manifest
                LEFT JOIN option_retention_holds AS hold
                  ON hold.released_at IS NULL
                 AND (hold.expires_at IS NULL OR hold.expires_at > %s)
                 AND jsonb_build_object(
                        'file_id', manifest.file_id::TEXT,
                        'object_key', manifest.object_key,
                        'event_type', manifest.event_type,
                        'market_date', manifest.market_date::TEXT,
                        'underlying', manifest.underlying,
                        'market_hour', manifest.market_hour
                     ) @> hold.selector
                WHERE manifest.creation_status = 'COMPLETE'
                  AND manifest.market_date <= %s
                GROUP BY manifest.file_id
                ORDER BY manifest.market_date, manifest.underlying,
                         manifest.market_hour, manifest.object_key
                """,
                (assessed_at, cutoff_market_date),
            )
            rows = cursor.fetchall()
        return tuple(
            (_manifest(row), int(row["active_hold_count"]))
            for row in rows
        )

    def create_hold(self, hold: RetentionHold) -> bool:
        with self._cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO option_retention_holds (
                    hold_id, scope_type, selector, reason, actor,
                    created_at, expires_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (hold_id) DO NOTHING
                """,
                (
                    hold.hold_id,
                    hold.scope_type.value,
                    Json(json.loads(hold.selector_json)),
                    hold.reason,
                    hold.actor,
                    hold.created_at,
                    hold.expires_at,
                ),
            )
            return cursor.rowcount == 1

    def release_hold(
        self,
        hold_id: UUID,
        released_at: datetime,
        released_by: str,
        release_reason: str,
    ) -> None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE option_retention_holds
                SET released_at = %s,
                    released_by = %s,
                    release_reason = %s,
                    updated_at = NOW()
                WHERE hold_id = %s AND released_at IS NULL
                """,
                (released_at, released_by, release_reason, hold_id),
            )
            if cursor.rowcount != 1:
                raise InvalidBatchTransition("retention hold is missing or already released")

    def tombstone_file(
        self,
        file_id: UUID,
        deleted_at: datetime,
        deletion_reason: str,
    ) -> None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE option_raw_file_manifests AS manifest
                SET creation_status = 'DELETED',
                    deleted_at = %s,
                    deletion_reason = %s,
                    updated_at = NOW()
                WHERE manifest.file_id = %s
                  AND manifest.creation_status IN ('COMPLETE', 'MISSING', 'CORRUPT')
                  AND NOT EXISTS (
                      SELECT 1
                      FROM option_retention_holds AS hold
                      WHERE hold.released_at IS NULL
                        AND (hold.expires_at IS NULL OR hold.expires_at > NOW())
                        AND jsonb_build_object(
                            'file_id', manifest.file_id::TEXT,
                            'object_key', manifest.object_key,
                            'event_type', manifest.event_type,
                            'market_date', manifest.market_date::TEXT,
                            'underlying', manifest.underlying,
                            'market_hour', manifest.market_hour
                        ) @> hold.selector
                  )
                """,
                (deleted_at, deletion_reason, file_id),
            )
            if cursor.rowcount != 1:
                raise InvalidBatchTransition(
                    "manifest is missing, already deleted, or protected by an active hold"
                )


def _manifest(row) -> RawFileManifest:
    return RawFileManifest(
        file_id=row["file_id"],
        event_type=RawFileEventType(row["event_type"]),
        market_date=row["market_date"],
        underlyer=row["underlying"],
        market_hour=row["market_hour"],
        object_key=row["object_key"],
        schema_version=row["schema_version"],
        row_count=row["row_count"],
        minimum_source_time=row["minimum_source_time"],
        maximum_source_time=row["maximum_source_time"],
        byte_size=row["byte_size"],
        payload_sha256=row["payload_sha256"],
        creation_status=ManifestCreationStatus(row["creation_status"]),
        retention_class=row["retention_class"],
    )