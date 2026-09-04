from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Collection
from uuid import UUID

from psycopg2.extras import Json, execute_values

from options.domain import (
    AssetType,
    DecisionContext,
    OptionUniverseCandidate,
    OptionUniverseMember,
    OptionUniverseMode,
    UniverseRunStatus,
)
from options.errors import DuplicateFactConflict, InvalidBatchTransition

from .base import ConnectionFactory, PostgresRepository


class OptionUniverseRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def create_run(
        self,
        run_id: UUID,
        mode: OptionUniverseMode,
        as_of_session: date,
        effective_from: date,
        configuration_json: str,
        configuration_sha256: str,
        started_at: datetime,
        first_observed_at: datetime,
    ) -> UUID:
        configuration = json.loads(configuration_json)
        if not isinstance(configuration, dict):
            raise ValueError("configuration_json must contain a JSON object")
        if mode is OptionUniverseMode.RANKED and effective_from <= as_of_session:
            raise ValueError("ranked universe reports must become effective after as_of_session")
        if effective_from < as_of_session:
            raise ValueError("effective_from cannot precede as_of_session")
        with self._cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO option_universe_runs (
                    run_id, mode, as_of_session, effective_from, status,
                    configuration_json, configuration_sha256, started_at,
                    first_observed_at
                ) VALUES (%s, %s, %s, %s, 'RUNNING', %s, %s, %s, %s)
                ON CONFLICT (run_id) DO NOTHING
                RETURNING run_id
                """,
                (
                    run_id,
                    mode.value,
                    as_of_session,
                    effective_from,
                    Json(configuration),
                    configuration_sha256,
                    started_at,
                    first_observed_at,
                ),
            )
            inserted = cursor.fetchone()
            if inserted:
                return inserted["run_id"]
            cursor.execute(
                """
                SELECT mode, as_of_session, effective_from, configuration_sha256
                FROM option_universe_runs
                WHERE run_id = %s
                """,
                (run_id,),
            )
            existing = cursor.fetchone()
            expected = (mode.value, as_of_session, effective_from, configuration_sha256)
            actual = (
                existing["mode"],
                existing["as_of_session"],
                existing["effective_from"],
                existing["configuration_sha256"],
            )
            if actual != expected:
                raise DuplicateFactConflict("universe run ID has different immutable inputs")
            return run_id

    def persist_candidates(
        self,
        candidates: Collection[OptionUniverseCandidate],
    ) -> int:
        if not candidates:
            return 0
        values = [
            (
                candidate.run_id,
                candidate.ticker,
                candidate.asset_type.value,
                Json(json.loads(candidate.raw_metrics_json)),
                Json(json.loads(candidate.component_ranks_json)),
                candidate.total_score,
                candidate.eligible,
                list(candidate.exclusion_reasons),
                candidate.candidate_rank,
                candidate.first_observed_at,
            )
            for candidate in candidates
        ]
        with self._cursor() as cursor:
            execute_values(
                cursor,
                """
                INSERT INTO option_universe_candidates (
                    run_id, ticker, asset_type, raw_metrics, component_ranks,
                    total_score, eligible, exclusion_reasons, candidate_rank,
                    first_observed_at
                ) VALUES %s
                ON CONFLICT (run_id, ticker) DO NOTHING
                """,
                values,
                page_size=500,
            )
            return cursor.rowcount

    def activate_members(self, members: Collection[OptionUniverseMember]) -> int:
        if not members:
            return 0
        values = [
            (
                member.effective_from,
                member.ticker,
                member.asset_type.value,
                member.source_run_id,
                member.member_rank,
                member.score,
                member.activated_at,
                member.deactivated_at,
                member.first_observed_at,
            )
            for member in members
        ]
        with self._cursor() as cursor:
            execute_values(
                cursor,
                """
                INSERT INTO option_universe_members (
                    effective_from, ticker, asset_type, source_run_id,
                    member_rank, score, activated_at, deactivated_at,
                    first_observed_at
                ) VALUES %s
                ON CONFLICT (effective_from, ticker) DO NOTHING
                """,
                values,
                page_size=100,
            )
            return cursor.rowcount

    def complete_run(
        self,
        run_id: UUID,
        status: UniverseRunStatus,
        completeness_fraction: float,
        completed_at: datetime,
    ) -> None:
        if status not in (UniverseRunStatus.COMPLETE, UniverseRunStatus.DEGRADED):
            raise ValueError("a successful universe run must finish COMPLETE or DEGRADED")
        if not 0 <= completeness_fraction <= 1:
            raise ValueError("completeness_fraction must be in [0, 1]")
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE option_universe_runs
                SET status = %s,
                    completeness_fraction = %s,
                    completed_at = %s,
                    updated_at = NOW()
                                WHERE run_id = %s
                                    AND (
                                        status = 'RUNNING'
                                        OR (
                                                status = 'DEGRADED'
                                                AND %s = 'COMPLETE'
                                                AND completeness_fraction < %s
                                        )
                                    )
                """,
                                (
                                        status.value,
                                        completeness_fraction,
                                        completed_at,
                                        run_id,
                                        status.value,
                                        completeness_fraction,
                                ),
            )
            if cursor.rowcount != 1:
                cursor.execute(
                    """
                    SELECT status, completeness_fraction
                    FROM option_universe_runs
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                existing = cursor.fetchone()
                if (
                    existing
                    and existing["status"] == status.value
                    and existing["completeness_fraction"] == completeness_fraction
                ):
                    return
                raise InvalidBatchTransition("universe run is missing or not RUNNING")

    def list_active(
        self,
        as_of_session: date,
        context: DecisionContext,
    ) -> tuple[OptionUniverseMember, ...]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (member.ticker)
                    member.effective_from,
                    member.ticker,
                    member.asset_type,
                    member.source_run_id,
                    member.member_rank,
                    member.score,
                    member.activated_at,
                    member.deactivated_at,
                    member.first_observed_at
                FROM option_universe_members AS member
                JOIN option_universe_runs AS run ON run.run_id = member.source_run_id
                WHERE member.effective_from <= %s
                  AND member.activated_at <= %s
                  AND member.first_observed_at <= %s
                  AND (member.deactivated_at IS NULL OR member.deactivated_at > %s)
                  AND run.first_observed_at <= %s
                  AND run.status IN ('COMPLETE', 'DEGRADED')
                ORDER BY
                    member.ticker,
                    member.effective_from DESC,
                    member.first_observed_at DESC,
                    member.member_id DESC
                """,
                (
                    as_of_session,
                    context.observed_time,
                    context.observed_time,
                    context.observed_time,
                    context.observed_time,
                ),
            )
            rows = cursor.fetchall()
        return tuple(_universe_member(row) for row in rows)


def _universe_member(row: dict[str, Any]) -> OptionUniverseMember:
    return OptionUniverseMember(
        effective_from=row["effective_from"],
        ticker=row["ticker"],
        asset_type=AssetType(row["asset_type"]),
        source_run_id=row["source_run_id"],
        member_rank=row["member_rank"],
        score=row["score"],
        activated_at=row["activated_at"],
        deactivated_at=row["deactivated_at"],
        first_observed_at=row["first_observed_at"],
    )