from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID

from psycopg2.extras import Json

from options.domain import DurableWorkItem, WorkStage, WorkStatus

from .base import ConnectionFactory, PostgresRepository


_WORK_ITEM_COLUMNS = """
    work_id,
    stage,
    subject_id,
    business_key,
    status,
    attempt_count,
    maximum_attempts,
    created_at,
    next_attempt_at,
    lease_owner,
    lease_expires_at,
    last_error,
    completed_at
"""

_CLAIMED_WORK_ITEM_COLUMNS = """
    work.work_id,
    work.stage,
    work.subject_id,
    work.business_key,
    work.status,
    work.attempt_count,
    work.maximum_attempts,
    work.created_at,
    work.next_attempt_at,
    work.lease_owner,
    work.lease_expires_at,
    work.last_error,
    work.completed_at
"""


class OptionWorkItemRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def enqueue(
        self,
        work_id: UUID,
        stage: WorkStage,
        subject_id: str,
        business_key: str,
        maximum_attempts: int,
        payload: dict[str, object] | None = None,
    ) -> UUID:
        with self._cursor() as cursor:
            cursor.execute(
                f"""
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
                RETURNING {_WORK_ITEM_COLUMNS}
                """,
                (
                    work_id,
                    stage.value,
                    subject_id,
                    business_key,
                    maximum_attempts,
                    Json(payload or {}),
                ),
            )
            row = cursor.fetchone()
            if row:
                return row["work_id"]
            cursor.execute(
                "SELECT work_id FROM option_work_items WHERE business_key = %s",
                (business_key,),
            )
            return cursor.fetchone()["work_id"]

    def claim_next(
        self,
        stage: WorkStage,
        lease_owner: str,
        lease_duration: timedelta,
    ) -> DurableWorkItem | None:
        lease_seconds = int(lease_duration.total_seconds())
        if lease_seconds <= 0:
            raise ValueError("lease_duration must be positive")
        with self._cursor() as cursor:
            cursor.execute(
                f"""
                WITH candidate AS (
                    SELECT work_id
                    FROM option_work_items
                    WHERE stage = %s
                      AND status IN ('PENDING', 'RETRY')
                      AND next_attempt_at <= NOW()
                      AND attempt_count < maximum_attempts
                    ORDER BY next_attempt_at, created_at, work_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE option_work_items AS work
                SET status = 'CLAIMED',
                    lease_owner = %s,
                    lease_expires_at = NOW() + (%s * INTERVAL '1 second'),
                    attempt_count = work.attempt_count + 1,
                    updated_at = NOW()
                FROM candidate
                WHERE work.work_id = candidate.work_id
                RETURNING {_CLAIMED_WORK_ITEM_COLUMNS}
                """,
                (stage.value, lease_owner, lease_seconds),
            )
            row = cursor.fetchone()
        return _work_item(row) if row else None

    def claim_by_business_key(
        self,
        business_key: str,
        lease_owner: str,
        lease_duration: timedelta,
    ) -> DurableWorkItem | None:
        lease_seconds = int(lease_duration.total_seconds())
        if lease_seconds <= 0:
            raise ValueError("lease_duration must be positive")
        with self._cursor() as cursor:
            cursor.execute(
                f"""
                UPDATE option_work_items AS work
                SET status = 'CLAIMED',
                    lease_owner = %s,
                    lease_expires_at = NOW() + (%s * INTERVAL '1 second'),
                    attempt_count = work.attempt_count + 1,
                    updated_at = NOW()
                WHERE work.business_key = %s
                  AND work.status IN ('PENDING', 'RETRY')
                  AND work.next_attempt_at <= NOW()
                  AND work.attempt_count < work.maximum_attempts
                RETURNING {_CLAIMED_WORK_ITEM_COLUMNS}
                """,
                (lease_owner, lease_seconds, business_key),
            )
            row = cursor.fetchone()
        return _work_item(row) if row else None

    def get_by_business_key(self, business_key: str) -> DurableWorkItem | None:
        with self._cursor() as cursor:
            cursor.execute(
                f"""
                SELECT {_WORK_ITEM_COLUMNS}
                FROM option_work_items
                WHERE business_key = %s
                """,
                (business_key,),
            )
            row = cursor.fetchone()
        return _work_item(row) if row else None

    def latest_due_cycle(
        self,
        policy_sha256: str,
        configuration_sha256: str,
    ):
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT MAX(pending.scheduled_cycle) AS scheduled_cycle
                FROM (
                    SELECT ingestion.scheduled_cycle
                    FROM option_work_items AS work
                    JOIN option_ingestion_runs AS ingestion
                      ON work.subject_id = ingestion.batch_id::text
                    WHERE work.stage = 'NORMALIZE'
                      AND work.status = 'RETRY'
                      AND work.next_attempt_at <= NOW()
                      AND work.attempt_count < work.maximum_attempts
                      AND ingestion.policy_sha256 = %s
                      AND ingestion.configuration_sha256 = %s
                    UNION ALL
                    SELECT ingestion.scheduled_cycle
                    FROM option_work_items AS work
                    JOIN option_analysis_runs AS analysis
                      ON work.subject_id = analysis.matrix_id::text
                    JOIN option_ingestion_runs AS ingestion
                      ON ingestion.batch_id = analysis.batch_id
                    WHERE work.stage = 'STRATEGY'
                      AND work.status = 'RETRY'
                      AND work.next_attempt_at <= NOW()
                      AND work.attempt_count < work.maximum_attempts
                      AND ingestion.policy_sha256 = %s
                      AND ingestion.configuration_sha256 = %s
                ) AS pending
                """,
                (
                    policy_sha256,
                    configuration_sha256,
                    policy_sha256,
                    configuration_sha256,
                ),
            )
            row = cursor.fetchone()
        return row["scheduled_cycle"] if row else None

    def complete(self, work_id: UUID, lease_owner: str) -> bool:
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE option_work_items
                SET status = 'COMPLETED',
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    completed_at = NOW(),
                    updated_at = NOW()
                WHERE work_id = %s
                  AND status = 'CLAIMED'
                  AND lease_owner = %s
                  AND lease_expires_at > NOW()
                """,
                (work_id, lease_owner),
            )
            return cursor.rowcount == 1

    def retry(
        self,
        work_id: UUID,
        lease_owner: str,
        error: str,
        retry_delay: timedelta,
    ) -> bool:
        retry_seconds = max(0, int(retry_delay.total_seconds()))
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE option_work_items
                SET status = CASE
                        WHEN attempt_count >= maximum_attempts THEN 'TERMINAL_FAILED'
                        ELSE 'RETRY'
                    END,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    next_attempt_at = NOW() + (%s * INTERVAL '1 second'),
                    last_error = %s,
                    updated_at = NOW()
                WHERE work_id = %s
                  AND status = 'CLAIMED'
                  AND lease_owner = %s
                """,
                (retry_seconds, error, work_id, lease_owner),
            )
            return cursor.rowcount == 1

    def terminal_fail(
        self,
        work_id: UUID,
        lease_owner: str,
        error: str,
    ) -> bool:
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE option_work_items
                SET status = 'TERMINAL_FAILED',
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    last_error = %s,
                    updated_at = NOW()
                WHERE work_id = %s
                  AND status = 'CLAIMED'
                  AND lease_owner = %s
                """,
                (error, work_id, lease_owner),
            )
            return cursor.rowcount == 1

    def recover_expired_claims(self) -> int:
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE option_work_items
                SET status = CASE
                        WHEN attempt_count >= maximum_attempts THEN 'TERMINAL_FAILED'
                        ELSE 'RETRY'
                    END,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    next_attempt_at = NOW(),
                    last_error = COALESCE(last_error, 'lease expired'),
                    updated_at = NOW()
                WHERE status = 'CLAIMED'
                  AND lease_expires_at <= NOW()
                """
            )
            return cursor.rowcount


def _work_item(row: dict[str, Any]) -> DurableWorkItem:
    return DurableWorkItem(
        work_id=row["work_id"],
        stage=WorkStage(row["stage"]),
        subject_id=row["subject_id"],
        business_key=row["business_key"],
        status=WorkStatus(row["status"]),
        attempt_count=row["attempt_count"],
        maximum_attempts=row["maximum_attempts"],
        created_at=row["created_at"],
        next_attempt_at=row["next_attempt_at"],
        lease_owner=row["lease_owner"],
        lease_expires_at=row["lease_expires_at"],
        last_error=row["last_error"],
        completed_at=row["completed_at"],
    )