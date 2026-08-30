from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from options.config import OptionRuntimeConfiguration
from options.repositories.retention import OptionRetentionRepository

from .parquet_archive import (
    ArchiveReconciler,
    BoundedTradeArchiveQueue,
    BufferedTradeArchive,
    ParquetRawMarketArchive,
)


@dataclass(frozen=True, slots=True)
class RawArchiveComponents:
    archive: ParquetRawMarketArchive
    queue: BoundedTradeArchiveQueue
    buffer: BufferedTradeArchive
    reconciler: ArchiveReconciler


def build_raw_archive(
    configuration: OptionRuntimeConfiguration,
    manifest_repository: OptionRetentionRepository,
    *,
    monotonic,
) -> RawArchiveComponents | None:
    if not configuration.settings.raw_archive_enabled:
        return None
    policy = configuration.policy.archive
    archive = ParquetRawMarketArchive(
        configuration.settings.raw_archive_root,
        manifest_repository,
    )
    return RawArchiveComponents(
        archive=archive,
        queue=BoundedTradeArchiveQueue(
            policy.maximum_queue_items,
            policy.maximum_queue_bytes,
        ),
        buffer=BufferedTradeArchive(
            archive,
            maximum_rows=policy.maximum_rows_per_file,
            maximum_age_seconds=policy.maximum_buffer_age_seconds,
            monotonic=monotonic,
        ),
        reconciler=ArchiveReconciler(
            configuration.settings.raw_archive_root,
            manifest_repository,
            partial_grace_period=timedelta(
                seconds=policy.stale_partial_grace_seconds
            ),
        ),
    )