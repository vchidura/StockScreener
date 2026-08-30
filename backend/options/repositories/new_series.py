from __future__ import annotations

import json
from datetime import datetime
from uuid import UUID

from psycopg2.extras import Json

from options.domain import (
    CatalogEligibility,
    NewSeriesResolution,
    NewSeriesState,
    OptionContractReference,
    WorkStage,
)
from options.errors import DuplicateFactConflict, InvalidBatchTransition

from .base import ConnectionFactory, PostgresRepository
from .catalog import _upsert_reference


class OptionNewSeriesRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def record_unknown(
        self,
        contract_ticker: str,
        underlyer: str,
        source_batch_id: UUID,
        source_page_number: int,
        first_observed_at: datetime,
        raw_details_json: str,
    ) -> int:
        raw_details = json.loads(raw_details_json)
        if not isinstance(raw_details, dict):
            raise ValueError("raw_details_json must contain a JSON object")
        with self._cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO option_contract_discoveries (
                    contract_ticker, underlying, state, source_batch_id,
                    source_page_number, first_observed_at, raw_details
                ) VALUES (%s, %s, 'UNKNOWN_REFERENCE', %s, %s, %s, %s)
                ON CONFLICT (contract_ticker, source_batch_id) DO NOTHING
                RETURNING discovery_id
                """,
                (
                    contract_ticker,
                    underlyer,
                    source_batch_id,
                    source_page_number,
                    first_observed_at,
                    Json(raw_details),
                ),
            )
            inserted = cursor.fetchone()
            if inserted:
                return inserted["discovery_id"]
            cursor.execute(
                """
                SELECT discovery_id, underlying, source_page_number, first_observed_at
                FROM option_contract_discoveries
                WHERE contract_ticker = %s AND source_batch_id = %s
                """,
                (contract_ticker, source_batch_id),
            )
            existing = cursor.fetchone()
            if not existing:
                raise DuplicateFactConflict("new-series conflict could not be resolved")
            if (
                existing["underlying"] != underlyer
                or existing["source_page_number"] != source_page_number
                or existing["first_observed_at"] != first_observed_at
            ):
                raise DuplicateFactConflict(
                    "new-series identity resolved to different source evidence"
                )
            return existing["discovery_id"]

    def mark_reference_pending(self, discovery_id: int, attempted_at: datetime) -> bool:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT state
                FROM option_contract_discoveries
                WHERE discovery_id = %s
                FOR UPDATE
                """,
                (discovery_id,),
            )
            row = cursor.fetchone()
            if not row:
                raise InvalidBatchTransition("new-series discovery does not exist")
            if row["state"] == NewSeriesState.REFERENCE_PENDING.value:
                return False
            if row["state"] not in (
                NewSeriesState.UNKNOWN_REFERENCE.value,
                NewSeriesState.REFERENCE_UNAVAILABLE.value,
            ):
                raise InvalidBatchTransition("discovery cannot begin reference lookup")
            cursor.execute(
                """
                UPDATE option_contract_discoveries
                SET state = 'REFERENCE_PENDING',
                    last_attempted_at = %s,
                    resolved_at = NULL,
                    reason_codes = ARRAY[]::TEXT[],
                    updated_at = NOW()
                WHERE discovery_id = %s
                """,
                (attempted_at, discovery_id),
            )
            return True

    def resolve_reference(
        self,
        discovery_id: int,
        reference: OptionContractReference,
        *,
        admission_deadline: datetime | None = None,
        matrix_sealed_at: datetime | None = None,
        next_matrix_time: datetime | None = None,
    ) -> NewSeriesResolution:
        resolved_at = reference.revised_observed_at or reference.first_observed_at
        deferred = matrix_sealed_at is not None or (
            admission_deadline is not None and resolved_at > admission_deadline
        )
        if deferred:
            boundary = matrix_sealed_at or admission_deadline
            if next_matrix_time is None or next_matrix_time <= boundary:
                raise ValueError("deferred admission requires a later next_matrix_time")
            activate_after = next_matrix_time
        else:
            activate_after = resolved_at

        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT contract_ticker, underlying, state, contract_id, activate_after
                FROM option_contract_discoveries
                WHERE discovery_id = %s
                FOR UPDATE
                """,
                (discovery_id,),
            )
            discovery = cursor.fetchone()
            if not discovery:
                raise InvalidBatchTransition("new-series discovery does not exist")
            if discovery["state"] in (
                NewSeriesState.VALIDATED_ACTIVE.value,
                NewSeriesState.REJECTED_UNSUPPORTED.value,
                NewSeriesState.WATCHLIST_ACTIVE.value,
            ):
                return NewSeriesResolution(
                    discovery_id=discovery_id,
                    state=NewSeriesState(discovery["state"]),
                    contract_id=discovery["contract_id"],
                    activate_after=discovery["activate_after"],
                )
            if discovery["state"] != NewSeriesState.REFERENCE_PENDING.value:
                raise InvalidBatchTransition("discovery is not awaiting a reference result")
            if (
                discovery["contract_ticker"] != reference.contract_ticker
                or discovery["underlying"] != reference.underlyer
            ):
                raise DuplicateFactConflict(
                    "reference result does not match the discovered contract"
                )

            contract_id, validation = _upsert_reference(cursor, reference)
            target_state = (
                NewSeriesState.VALIDATED_ACTIVE
                if validation.eligibility_status is CatalogEligibility.VALIDATED_ACTIVE
                else NewSeriesState.REJECTED_UNSUPPORTED
            )
            effective_activation = (
                activate_after if target_state is NewSeriesState.VALIDATED_ACTIVE else None
            )
            cursor.execute(
                """
                UPDATE option_contract_discoveries
                SET state = %s,
                    contract_id = %s,
                    resolved_at = %s,
                    activate_after = %s,
                    reason_codes = %s,
                    updated_at = NOW()
                WHERE discovery_id = %s
                """,
                (
                    target_state.value,
                    contract_id,
                    resolved_at,
                    effective_activation,
                    list(validation.exclusion_reasons),
                    discovery_id,
                ),
            )
            return NewSeriesResolution(
                discovery_id=discovery_id,
                state=target_state,
                contract_id=contract_id,
                activate_after=effective_activation,
            )

    def mark_reference_unavailable(
        self,
        discovery_id: int,
        resolved_at: datetime,
        reason_codes: tuple[str, ...],
    ) -> None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE option_contract_discoveries
                SET state = 'REFERENCE_UNAVAILABLE',
                    resolved_at = %s,
                    reason_codes = %s,
                    updated_at = NOW()
                WHERE discovery_id = %s AND state = 'REFERENCE_PENDING'
                """,
                (resolved_at, list(reason_codes), discovery_id),
            )
            if cursor.rowcount != 1:
                raise InvalidBatchTransition("discovery is not awaiting a reference result")

    def activate_filtered_watchlist(
        self,
        discovery_id: int,
        *,
        active_from: datetime,
        observed_at: datetime,
        session_open: datetime,
        delayed_watermark: datetime,
        work_id: UUID,
        maximum_work_attempts: int,
    ) -> UUID:
        if delayed_watermark < session_open:
            raise ValueError("delayed_watermark cannot precede session_open")
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT state, contract_id, activate_after
                FROM option_contract_discoveries
                WHERE discovery_id = %s
                FOR UPDATE
                """,
                (discovery_id,),
            )
            discovery = cursor.fetchone()
            if not discovery:
                raise InvalidBatchTransition("discovery does not exist")

            business_key = (
                f"trade-backfill:{discovery['contract_id']}:{session_open.isoformat()}"
            )
            if discovery["state"] == NewSeriesState.WATCHLIST_ACTIVE.value:
                cursor.execute(
                    "SELECT work_id FROM option_work_items WHERE business_key = %s",
                    (business_key,),
                )
                existing_work = cursor.fetchone()
                if not existing_work:
                    raise InvalidBatchTransition(
                        "watchlist is active without durable backfill work"
                    )
                return existing_work["work_id"]
            if discovery["state"] != NewSeriesState.VALIDATED_ACTIVE.value:
                raise InvalidBatchTransition("discovery is not ready for watchlist activation")
            if active_from < discovery["activate_after"]:
                raise InvalidBatchTransition("watchlist activation precedes matrix eligibility")

            source_id = f"new-series:{discovery_id}"
            cursor.execute(
                """
                INSERT INTO option_trade_watchlist (
                    contract_id, reason, active_from, first_observed_at, source_id,
                    backfill_from, backfill_through, backfill_status
                ) VALUES (%s, 'FILTERED', %s, %s, %s, %s, %s, 'PENDING')
                ON CONFLICT (contract_id, reason, source_id, active_from) DO NOTHING
                """,
                (
                    discovery["contract_id"],
                    active_from,
                    observed_at,
                    source_id,
                    session_open,
                    delayed_watermark,
                ),
            )
            cursor.execute(
                """
                INSERT INTO option_work_items (
                    work_id, stage, subject_id, business_key, status,
                    maximum_attempts, next_attempt_at, payload
                ) VALUES (%s, %s, %s, %s, 'PENDING', %s, NOW(), %s)
                ON CONFLICT (business_key) DO NOTHING
                RETURNING work_id
                """,
                (
                    work_id,
                    WorkStage.TRADE_BACKFILL.value,
                    str(discovery["contract_id"]),
                    business_key,
                    maximum_work_attempts,
                    Json(
                        {
                            "contract_id": discovery["contract_id"],
                            "from": session_open.isoformat(),
                            "through": delayed_watermark.isoformat(),
                        }
                    ),
                ),
            )
            inserted_work = cursor.fetchone()
            if inserted_work:
                durable_work_id = inserted_work["work_id"]
            else:
                cursor.execute(
                    "SELECT work_id FROM option_work_items WHERE business_key = %s",
                    (business_key,),
                )
                durable_work_id = cursor.fetchone()["work_id"]
            cursor.execute(
                """
                UPDATE option_contract_discoveries
                SET state = 'WATCHLIST_ACTIVE', updated_at = NOW()
                WHERE discovery_id = %s
                """,
                (discovery_id,),
            )
            return durable_work_id