from __future__ import annotations

import gzip
from uuid import UUID

from psycopg2.extras import Json

from options.domain import (
    AssetType,
    BatchStatus,
    PageValidationStatus,
    RawBatchPage,
    RawOptionBatch,
    WorkStage,
)
from options.errors import DuplicateFactConflict, InvalidBatchTransition

from .base import ConnectionFactory, PostgresRepository


class OptionIngestionRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def begin_batch(
        self,
        batch: RawOptionBatch,
        asset_type: AssetType,
        policy_version: str,
        configuration_sha256: str,
    ) -> UUID:
        if batch.status is not BatchStatus.FETCHING or batch.pages:
            raise InvalidBatchTransition("a new ingestion batch must be empty and FETCHING")

        with self._cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO option_ingestion_runs (
                    batch_id,
                    provider,
                    underlying,
                    asset_type,
                    scheduled_cycle,
                    request_filter_sha256,
                    policy_version,
                    policy_sha256,
                    configuration_sha256,
                    status,
                    started_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'FETCHING', %s)
                ON CONFLICT (
                    provider,
                    underlying,
                    scheduled_cycle,
                    request_filter_sha256
                ) DO NOTHING
                RETURNING batch_id
                """,
                (
                    batch.batch_id,
                    batch.provider,
                    batch.underlyer,
                    asset_type.value,
                    batch.scheduled_cycle,
                    batch.request_filter_sha256,
                    policy_version,
                    batch.policy_sha256,
                    configuration_sha256,
                    batch.started_at,
                ),
            )
            inserted = cursor.fetchone()
            if inserted:
                return inserted["batch_id"]
            cursor.execute(
                """
                SELECT
                    batch_id,
                    asset_type,
                    policy_version,
                    policy_sha256,
                    configuration_sha256
                FROM option_ingestion_runs
                WHERE provider = %s
                  AND underlying = %s
                  AND scheduled_cycle = %s
                  AND request_filter_sha256 = %s
                """,
                (
                    batch.provider,
                    batch.underlyer,
                    batch.scheduled_cycle,
                    batch.request_filter_sha256,
                ),
            )
            existing = cursor.fetchone()
            if not existing:
                raise InvalidBatchTransition("ingestion slot conflict could not be resolved")
            expected_cohort = (
                asset_type.value,
                policy_version,
                batch.policy_sha256,
                configuration_sha256,
            )
            actual_cohort = (
                existing["asset_type"],
                existing["policy_version"],
                existing["policy_sha256"],
                existing["configuration_sha256"],
            )
            if actual_cohort != expected_cohort:
                raise DuplicateFactConflict(
                    "ingestion slot belongs to a different policy or configuration cohort"
                )
            return existing["batch_id"]

    def load_batch(self, batch_id: UUID) -> RawOptionBatch | None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    batch_id, provider, underlying, scheduled_cycle,
                    request_filter_sha256, policy_sha256, status, started_at,
                    completed_at, failure_reason
                FROM option_ingestion_runs
                WHERE batch_id = %s
                """,
                (batch_id,),
            )
            batch = cursor.fetchone()
            if not batch:
                return None
            cursor.execute(
                """
                SELECT
                    batch_id, page_number, row_count, response_gzip,
                    payload_sha256, received_at, terminal_page,
                    validation_status, request_filter_sha256,
                    request_cursor_sha256, request_id, next_cursor_sha256,
                    redacted_request
                FROM option_raw_batch_pages
                WHERE batch_id = %s
                ORDER BY page_number
                """,
                (batch_id,),
            )
            pages = tuple(
                RawBatchPage(
                    batch_id=row["batch_id"],
                    page_number=row["page_number"],
                    row_count=row["row_count"],
                    response_bytes=gzip.decompress(row["response_gzip"]),
                    payload_sha256=row["payload_sha256"],
                    received_at=row["received_at"],
                    terminal=row["terminal_page"],
                    validation_status=PageValidationStatus(row["validation_status"]),
                    request_filter_sha256=row["request_filter_sha256"],
                    request_cursor_sha256=row["request_cursor_sha256"],
                    request_id=row["request_id"],
                    next_cursor_sha256=row["next_cursor_sha256"],
                    request_metadata=tuple(
                        sorted(
                            (str(key), str(value))
                            for key, value in row["redacted_request"].items()
                        )
                    ),
                )
                for row in cursor.fetchall()
            )
        return RawOptionBatch(
            batch_id=batch["batch_id"],
            provider=batch["provider"],
            underlyer=batch["underlying"],
            scheduled_cycle=batch["scheduled_cycle"],
            request_filter_sha256=batch["request_filter_sha256"],
            policy_sha256=batch["policy_sha256"],
            status=BatchStatus(batch["status"]),
            pages=pages,
            started_at=batch["started_at"],
            completed_at=batch["completed_at"],
            failure_reason=batch["failure_reason"],
        )

    def persist_page(self, page: RawBatchPage) -> bool:
        compressed = gzip.compress(page.response_bytes, mtime=0)
        request_metadata = dict(page.request_metadata)
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT status
                FROM option_ingestion_runs
                WHERE batch_id = %s
                FOR UPDATE
                """,
                (page.batch_id,),
            )
            batch = cursor.fetchone()
            if not batch:
                raise InvalidBatchTransition("raw page references an unknown batch")
            if batch["status"] != BatchStatus.FETCHING.value:
                raise InvalidBatchTransition("raw pages can be added only while FETCHING")

            cursor.execute(
                """
                INSERT INTO option_raw_batch_pages (
                    batch_id,
                    page_number,
                    request_id,
                    redacted_request,
                    response_gzip,
                    payload_sha256,
                    row_count,
                    byte_count,
                    received_at,
                    terminal_page,
                    request_filter_sha256,
                    request_cursor_sha256,
                    next_cursor_sha256,
                    validation_status,
                    validation_error
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (batch_id, page_number) DO NOTHING
                """,
                (
                    page.batch_id,
                    page.page_number,
                    page.request_id,
                    Json(request_metadata),
                    compressed,
                    page.payload_sha256,
                    page.row_count,
                    len(page.response_bytes),
                    page.received_at,
                    page.terminal,
                    page.request_filter_sha256,
                    page.request_cursor_sha256,
                    page.next_cursor_sha256,
                    page.validation_status.value,
                    None if page.validation_status is PageValidationStatus.VALID else "invalid page",
                ),
            )
            if cursor.rowcount == 1:
                return True

            cursor.execute(
                """
                SELECT
                    payload_sha256,
                    row_count,
                    terminal_page,
                    request_filter_sha256,
                    request_cursor_sha256,
                    next_cursor_sha256,
                    validation_status
                FROM option_raw_batch_pages
                WHERE batch_id = %s AND page_number = %s
                """,
                (page.batch_id, page.page_number),
            )
            existing = cursor.fetchone()
            expected = (
                page.payload_sha256,
                page.row_count,
                page.terminal,
                page.request_filter_sha256,
                page.request_cursor_sha256,
                page.next_cursor_sha256,
                page.validation_status.value,
            )
            actual = (
                existing["payload_sha256"],
                existing["row_count"],
                existing["terminal_page"],
                existing["request_filter_sha256"],
                existing["request_cursor_sha256"],
                existing["next_cursor_sha256"],
                existing["validation_status"],
            )
            if actual != expected:
                raise DuplicateFactConflict("raw page idempotency key has different content")
            return False

    def complete_batch(
        self,
        batch_id: UUID,
        work_id: UUID,
        business_key: str,
        maximum_work_attempts: int,
        maximum_pages: int,
        maximum_rows: int,
        maximum_bytes: int,
        work_stage: WorkStage = WorkStage.NORMALIZE,
    ) -> UUID:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT status, request_filter_sha256
                FROM option_ingestion_runs
                WHERE batch_id = %s
                FOR UPDATE
                """,
                (batch_id,),
            )
            batch = cursor.fetchone()
            if not batch:
                raise InvalidBatchTransition("cannot complete an unknown batch")
            if batch["status"] not in (BatchStatus.FETCHING.value, BatchStatus.COMPLETE.value):
                raise InvalidBatchTransition("only a FETCHING batch can become COMPLETE")

            cursor.execute(
                """
                SELECT
                    page_number,
                    row_count,
                    byte_count,
                    terminal_page,
                    request_filter_sha256,
                    request_cursor_sha256,
                    next_cursor_sha256,
                    validation_status
                FROM option_raw_batch_pages
                WHERE batch_id = %s
                ORDER BY page_number
                FOR UPDATE
                """,
                (batch_id,),
            )
            pages = cursor.fetchall()
            self._validate_complete_pages(
                pages,
                maximum_pages=maximum_pages,
                maximum_rows=maximum_rows,
                maximum_bytes=maximum_bytes,
                request_filter_sha256=batch["request_filter_sha256"],
            )

            page_count = len(pages)
            row_count = sum(page["row_count"] for page in pages)
            cursor.execute(
                """
                UPDATE option_ingestion_runs
                SET status = 'COMPLETE',
                    page_count = %s,
                    terminal_page_received = TRUE,
                    received_row_count = %s,
                    completed_at = COALESCE(completed_at, NOW()),
                    failure_reason = NULL,
                    error_category = NULL,
                    updated_at = NOW()
                WHERE batch_id = %s
                """,
                (page_count, row_count, batch_id),
            )
            cursor.execute(
                """
                INSERT INTO option_work_items (
                    work_id,
                    stage,
                    subject_id,
                    business_key,
                    status,
                    maximum_attempts,
                    next_attempt_at,
                    payload
                ) VALUES (%s, %s, %s, %s, 'PENDING', %s, NOW(), %s)
                ON CONFLICT (business_key) DO NOTHING
                """,
                (
                    work_id,
                    work_stage.value,
                    str(batch_id),
                    business_key,
                    maximum_work_attempts,
                    Json({"batch_id": str(batch_id)}),
                ),
            )
            if cursor.rowcount == 1:
                return work_id
            cursor.execute(
                "SELECT work_id FROM option_work_items WHERE business_key = %s",
                (business_key,),
            )
            existing_work = cursor.fetchone()
            if not existing_work:
                raise InvalidBatchTransition("normalization work conflict could not be resolved")
            return existing_work["work_id"]

    def fail_batch(
        self,
        batch_id: UUID,
        status: BatchStatus,
        error_category: str,
        failure_reason: str,
    ) -> None:
        if status not in (BatchStatus.FAILED, BatchStatus.QUARANTINED):
            raise ValueError("failed batch status must be FAILED or QUARANTINED")
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE option_ingestion_runs
                SET status = %s,
                    error_category = %s,
                    failure_reason = %s,
                    completed_at = NOW(),
                    updated_at = NOW()
                WHERE batch_id = %s AND status = 'FETCHING'
                """,
                (status.value, error_category, failure_reason, batch_id),
            )
            if cursor.rowcount != 1:
                raise InvalidBatchTransition("batch is missing or no longer FETCHING")

    def record_normalization(
        self,
        batch_id: UUID,
        *,
        catalog_row_count: int,
        retained_row_count: int,
        rejected_counts: dict[str, int],
        unknown_reference_count: int,
        market_data_time=None,
        first_observed_at=None,
    ) -> None:
        counts = (catalog_row_count, retained_row_count, unknown_reference_count)
        if any(count < 0 for count in counts) or any(
            count < 0 for count in rejected_counts.values()
        ):
            raise ValueError("normalization counts cannot be negative")
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE option_ingestion_runs
                SET catalog_row_count = %s,
                    retained_row_count = %s,
                    rejected_counts = %s,
                    unknown_reference_count = %s,
                    market_data_time = %s,
                    first_observed_at = %s,
                    updated_at = NOW()
                WHERE batch_id = %s AND status = 'COMPLETE'
                """,
                (
                    catalog_row_count,
                    retained_row_count,
                    Json(rejected_counts),
                    unknown_reference_count,
                    market_data_time,
                    first_observed_at,
                    batch_id,
                ),
            )
            if cursor.rowcount != 1:
                raise InvalidBatchTransition(
                    "normalization telemetry requires a complete ingestion batch"
                )

    @staticmethod
    def _validate_complete_pages(
        pages: list[dict[str, object]],
        *,
        maximum_pages: int,
        maximum_rows: int,
        maximum_bytes: int,
        request_filter_sha256: str,
    ) -> None:
        if not pages:
            raise InvalidBatchTransition("a complete batch requires at least one page")
        page_numbers = [page["page_number"] for page in pages]
        if page_numbers != list(range(1, len(pages) + 1)):
            raise InvalidBatchTransition("raw page chain is not contiguous")
        if any(page["request_filter_sha256"] != request_filter_sha256 for page in pages):
            raise InvalidBatchTransition("raw page chain changed request filters")
        if pages[0]["request_cursor_sha256"] is not None:
            raise InvalidBatchTransition("the first raw page cannot have a request cursor")
        for previous, current in zip(pages, pages[1:]):
            if current["request_cursor_sha256"] != previous["next_cursor_sha256"]:
                raise InvalidBatchTransition("raw page cursor continuity failed")
        cursors = [page["request_cursor_sha256"] for page in pages[1:]]
        if len(cursors) != len(set(cursors)):
            raise InvalidBatchTransition("raw page chain repeated a cursor")
        terminal_pages = [page for page in pages if page["terminal_page"]]
        if terminal_pages != [pages[-1]]:
            raise InvalidBatchTransition("raw page chain lacks one final terminal page")
        if any(page["validation_status"] != PageValidationStatus.VALID.value for page in pages):
            raise InvalidBatchTransition("raw page chain contains an invalid page")
        if len(pages) > maximum_pages:
            raise InvalidBatchTransition("raw page chain exceeds the page cap")
        if sum(int(page["row_count"]) for page in pages) > maximum_rows:
            raise InvalidBatchTransition("raw page chain exceeds the row cap")
        if sum(int(page["byte_count"]) for page in pages) > maximum_bytes:
            raise InvalidBatchTransition("raw page chain exceeds the byte cap")