from __future__ import annotations

import json
from typing import Any, Collection
from uuid import UUID

from psycopg2.extras import Json, execute_values

from options.domain import (
    AnalysisStatus,
    DecisionContext,
    OptionAnalysisRun,
    OptionExpirationAnalytics,
)
from options.errors import DuplicateFactConflict, InvalidBatchTransition

from .base import ConnectionFactory, PostgresRepository


class OptionAnalysisRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def start(self, run: OptionAnalysisRun) -> UUID:
        if run.status is not AnalysisStatus.RUNNING:
            raise ValueError("a new analysis run must have RUNNING status")
        with self._cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO option_analysis_runs (
                    matrix_id, batch_id, underlying, market_time, observed_time,
                    status, received_contract_count, eligible_contract_count,
                    unknown_reference_count, iv_attempt_count, iv_converged_count,
                    iv_convergence_fraction, quality_reasons, chain_health,
                    policy_version, policy_sha256, model_version, started_at
                ) VALUES (
                    %s, %s, %s, %s, %s, 'RUNNING', %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (matrix_id) DO NOTHING
                RETURNING matrix_id
                """,
                self._run_values(run)[:-1],
            )
            inserted = cursor.fetchone()
            if inserted:
                return inserted["matrix_id"]
            cursor.execute(
                """
                SELECT batch_id, market_time, observed_time, policy_sha256, model_version
                FROM option_analysis_runs
                WHERE matrix_id = %s
                """,
                (run.matrix_id,),
            )
            existing = cursor.fetchone()
            expected = (
                run.batch_id,
                run.context.market_time,
                run.context.observed_time,
                run.policy_sha256,
                run.model_version,
            )
            actual = (
                existing["batch_id"],
                existing["market_time"],
                existing["observed_time"],
                existing["policy_sha256"],
                existing["model_version"],
            )
            if actual != expected:
                raise DuplicateFactConflict("matrix ID has different immutable analysis inputs")
            return run.matrix_id

    def persist_expirations(
        self,
        analytics: Collection[OptionExpirationAnalytics],
    ) -> int:
        if not analytics:
            return 0
        matrix_ids = {item.matrix_id for item in analytics}
        if len(matrix_ids) != 1:
            raise ValueError("expiration analytics must belong to one matrix")
        matrix_id = next(iter(matrix_ids))
        with self._cursor() as cursor:
            cursor.execute(
                "SELECT status FROM option_analysis_runs WHERE matrix_id = %s FOR UPDATE",
                (matrix_id,),
            )
            row = cursor.fetchone()
            if not row or row["status"] != AnalysisStatus.RUNNING.value:
                raise InvalidBatchTransition("analysis run is not RUNNING")
            values = [
                (
                    item.matrix_id,
                    item.expiration_date,
                    item.fractional_maturity_years,
                    item.forward_price,
                    item.atm_iv,
                    item.call_25_delta_iv,
                    item.put_25_delta_iv,
                    item.call_skew_25_delta,
                    item.put_skew_25_delta,
                    item.risk_reversal_25_delta,
                    Json(json.loads(item.interpolation_diagnostics_json)),
                    item.put_volume,
                    item.call_volume,
                    item.put_open_interest,
                    item.call_open_interest,
                    item.breadth,
                    Json(json.loads(item.concentration_metrics_json)),
                    Json(json.loads(item.wall_clusters_json)),
                    item.term_change,
                    item.term_slope,
                    list(item.quality_reasons),
                )
                for item in analytics
            ]
            execute_values(
                cursor,
                """
                INSERT INTO option_expiration_analytics (
                    matrix_id, expiration_date, fractional_maturity_years,
                    forward_price, atm_iv, call_25_delta_iv, put_25_delta_iv,
                    call_skew_25_delta, put_skew_25_delta,
                    risk_reversal_25_delta, interpolation_diagnostics,
                    put_volume, call_volume, put_open_interest, call_open_interest,
                    breadth, concentration_metrics, wall_clusters, term_change,
                    term_slope, quality_reasons
                ) VALUES %s
                ON CONFLICT (matrix_id, expiration_date) DO NOTHING
                """,
                values,
                page_size=100,
            )
            return cursor.rowcount

    def finish(self, run: OptionAnalysisRun) -> None:
        if run.status in (AnalysisStatus.PENDING, AnalysisStatus.RUNNING):
            raise ValueError("finished analysis requires a terminal status")
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE option_analysis_runs
                SET status = %s,
                    received_contract_count = %s,
                    eligible_contract_count = %s,
                    unknown_reference_count = %s,
                    iv_attempt_count = %s,
                    iv_converged_count = %s,
                    iv_convergence_fraction = %s,
                    quality_reasons = %s,
                    chain_health = %s,
                    completed_at = %s,
                    updated_at = NOW()
                WHERE matrix_id = %s
                  AND batch_id = %s
                  AND policy_sha256 = %s
                  AND model_version = %s
                  AND status = 'RUNNING'
                """,
                (
                    run.status.value,
                    run.received_contract_count,
                    run.eligible_contract_count,
                    run.unknown_reference_count,
                    run.iv_attempt_count,
                    run.iv_converged_count,
                    run.iv_convergence_fraction,
                    list(run.quality_reasons),
                    Json(json.loads(run.chain_health_json)),
                    run.completed_at,
                    run.matrix_id,
                    run.batch_id,
                    run.policy_sha256,
                    run.model_version,
                ),
            )
            if cursor.rowcount != 1:
                raise InvalidBatchTransition("analysis run cannot make the terminal transition")

    def get(
        self,
        matrix_id: UUID,
        context: DecisionContext,
    ) -> OptionAnalysisRun | None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    matrix_id, batch_id, underlying, market_time, observed_time,
                    status, received_contract_count, eligible_contract_count,
                    unknown_reference_count, iv_attempt_count, iv_converged_count,
                    iv_convergence_fraction, quality_reasons, chain_health,
                    policy_version, policy_sha256, model_version, started_at,
                    completed_at
                FROM option_analysis_runs
                WHERE matrix_id = %s
                  AND market_time <= %s
                  AND observed_time <= %s
                """,
                (matrix_id, context.market_time, context.observed_time),
            )
            row = cursor.fetchone()
        return _analysis_run(row) if row else None

    def get_latest(
        self,
        underlyer: str,
        context: DecisionContext,
    ) -> OptionAnalysisRun | None:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    matrix_id, batch_id, underlying, market_time, observed_time,
                    status, received_contract_count, eligible_contract_count,
                    unknown_reference_count, iv_attempt_count, iv_converged_count,
                    iv_convergence_fraction, quality_reasons, chain_health,
                    policy_version, policy_sha256, model_version, started_at,
                    completed_at
                FROM option_analysis_runs
                WHERE underlying = %s
                  AND status NOT IN ('PENDING', 'RUNNING')
                  AND market_time <= %s
                  AND observed_time <= %s
                ORDER BY market_time DESC, observed_time DESC
                LIMIT 1
                """,
                (underlyer, context.market_time, context.observed_time),
            )
            row = cursor.fetchone()
        return _analysis_run(row) if row else None

    def list_expirations(
        self,
        matrix_id: UUID,
        context: DecisionContext,
    ) -> tuple[OptionExpirationAnalytics, ...]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT analytics.*
                FROM option_expiration_analytics AS analytics
                JOIN option_analysis_runs AS run USING (matrix_id)
                WHERE analytics.matrix_id = %s
                  AND run.market_time <= %s
                  AND run.observed_time <= %s
                ORDER BY analytics.expiration_date
                """,
                (matrix_id, context.market_time, context.observed_time),
            )
            rows = cursor.fetchall()
        return tuple(_expiration(row) for row in rows)

    @staticmethod
    def _run_values(run: OptionAnalysisRun) -> tuple[object, ...]:
        return (
            run.matrix_id,
            run.batch_id,
            run.underlyer,
            run.context.market_time,
            run.context.observed_time,
            run.received_contract_count,
            run.eligible_contract_count,
            run.unknown_reference_count,
            run.iv_attempt_count,
            run.iv_converged_count,
            run.iv_convergence_fraction,
            list(run.quality_reasons),
            Json(json.loads(run.chain_health_json)),
            run.policy_version,
            run.policy_sha256,
            run.model_version,
            run.started_at,
            run.completed_at,
        )


def _analysis_run(row: dict[str, Any]) -> OptionAnalysisRun:
    return OptionAnalysisRun(
        matrix_id=row["matrix_id"],
        batch_id=row["batch_id"],
        underlyer=row["underlying"],
        context=DecisionContext(row["market_time"], row["observed_time"]),
        status=AnalysisStatus(row["status"]),
        received_contract_count=row["received_contract_count"],
        eligible_contract_count=row["eligible_contract_count"],
        unknown_reference_count=row["unknown_reference_count"],
        iv_attempt_count=row["iv_attempt_count"],
        iv_converged_count=row["iv_converged_count"],
        iv_convergence_fraction=row["iv_convergence_fraction"],
        quality_reasons=tuple(row["quality_reasons"]),
        chain_health_json=json.dumps(row["chain_health"], sort_keys=True, separators=(",", ":")),
        policy_version=row["policy_version"],
        policy_sha256=row["policy_sha256"],
        model_version=row["model_version"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
    )


def _expiration(row: dict[str, Any]) -> OptionExpirationAnalytics:
    return OptionExpirationAnalytics(
        matrix_id=row["matrix_id"],
        expiration_date=row["expiration_date"],
        fractional_maturity_years=row["fractional_maturity_years"],
        forward_price=row["forward_price"],
        atm_iv=row["atm_iv"],
        call_25_delta_iv=row["call_25_delta_iv"],
        put_25_delta_iv=row["put_25_delta_iv"],
        call_skew_25_delta=row["call_skew_25_delta"],
        put_skew_25_delta=row["put_skew_25_delta"],
        risk_reversal_25_delta=row["risk_reversal_25_delta"],
        interpolation_diagnostics_json=json.dumps(row["interpolation_diagnostics"], sort_keys=True, separators=(",", ":")),
        put_volume=row["put_volume"],
        call_volume=row["call_volume"],
        put_open_interest=row["put_open_interest"],
        call_open_interest=row["call_open_interest"],
        breadth=row["breadth"],
        concentration_metrics_json=json.dumps(row["concentration_metrics"], sort_keys=True, separators=(",", ":")),
        wall_clusters_json=json.dumps(row["wall_clusters"], sort_keys=True, separators=(",", ":")),
        term_change=row["term_change"],
        term_slope=row["term_slope"],
        quality_reasons=tuple(row["quality_reasons"]),
    )