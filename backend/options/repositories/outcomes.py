from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import Any, Sequence
from uuid import UUID

from psycopg2.extras import execute_values

from options.outcomes import OptionDecayOutcome, OptionOutcomeLeg
from options.strategies.domain import OptionSide

from .base import ConnectionFactory, PostgresRepository


OPTION_OUTCOME_RETENTION_DAYS = 60


class OptionOutcomeRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def persist_decay_outcomes(
        self,
        outcomes: Sequence[OptionDecayOutcome],
    ) -> int:
        if not outcomes:
            return 0
        with self._cursor() as cursor:
            inserted = execute_values(
                cursor,
                """
                INSERT INTO option_signal_decay_outcomes (
                    outcome_id, event_id, candidate_id, measurement_type,
                    market_time, observed_time, mark, net_return,
                    availability_flag, quality_flags, entry_net_premium,
                    exit_net_premium, gross_pnl, estimated_cost, net_pnl,
                    capital_at_risk, valuation_policy_version,
                    valuation_policy_sha256, source_snapshot_ids, source_batch_id
                ) VALUES %s
                ON CONFLICT (
                    candidate_id, measurement_type, valuation_policy_sha256
                ) DO NOTHING
                RETURNING outcome_id
                """,
                [
                    (
                        row.outcome_id, row.event_id, row.candidate_id,
                        row.measurement_type, row.market_time, row.observed_time,
                        row.exit_net_premium, row.net_return,
                        row.availability_flag, list(row.quality_flags),
                        row.entry_net_premium, row.exit_net_premium,
                        row.gross_pnl, row.estimated_cost, row.net_pnl,
                        row.capital_at_risk, row.valuation_policy_version,
                        row.valuation_policy_sha256,
                        list(row.source_snapshot_ids), row.source_batch_id,
                    )
                    for row in outcomes
                ],
                fetch=True,
            )
            return len(inserted)

    def retained_leg_bounds(
        self,
        underlyer: str,
        *,
        available_by,
        retention_days: int = OPTION_OUTCOME_RETENTION_DAYS,
    ) -> dict[str, Any] | None:
        if retention_days <= 0:
            raise ValueError("retention_days must be positive")
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT MIN(leg.strike) AS minimum_strike,
                       MAX(leg.strike) AS maximum_strike,
                       MAX(leg.expiration_date) AS expiration_through,
                       COUNT(DISTINCT leg.contract_id) AS contract_count
                FROM option_strategy_candidates AS candidate
                JOIN option_candidate_legs AS leg USING (candidate_id)
                WHERE candidate.underlying = %s
                  AND candidate.status = 'SELECTED'
                  AND candidate.candidate_kind IN ('SINGLE_CONTRACT', 'MULTI_LEG')
                  AND candidate.capital_at_risk > 0
                  AND candidate.market_data_time <= %s
                  AND candidate.observed_time <= %s
                  AND candidate.market_data_time >
                      %s - (%s * INTERVAL '1 day')
                  AND leg.expiration_date >=
                      (%s AT TIME ZONE 'America/New_York')::DATE
                """,
                (
                    underlyer.upper(), available_by, available_by,
                    available_by, retention_days, available_by,
                ),
            )
            row = cursor.fetchone()
        if not row or not row["contract_count"]:
            return None
        return dict(row)

    def list_pending_candidates(
        self,
        *,
        valuation_policy_sha256: str,
        available_by,
        retention_days: int = OPTION_OUTCOME_RETENTION_DAYS,
        limit: int = 1000,
    ) -> tuple[dict[str, Any], ...]:
        if retention_days <= 0 or limit <= 0:
            raise ValueError("retention_days and limit must be positive")
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT candidate.candidate_id, signal.event_id,
                       candidate.market_data_time, candidate.capital_at_risk,
                       COALESCE(
                           ARRAY_AGG(outcome.measurement_type) FILTER (
                               WHERE outcome.outcome_id IS NOT NULL
                           ), ARRAY[]::TEXT[]
                       ) AS completed_measurements
                FROM option_strategy_candidates AS candidate
                                LEFT JOIN option_signal_occurrences AS signal_occurrence
                                    ON signal_occurrence.source_candidate_id = candidate.candidate_id
                                LEFT JOIN option_signal_events AS signal
                                    ON signal.event_id = signal_occurrence.event_id
                LEFT JOIN option_signal_decay_outcomes AS outcome
                  ON outcome.candidate_id = candidate.candidate_id
                 AND outcome.valuation_policy_sha256 = %s
                WHERE candidate.status = 'SELECTED'
                  AND candidate.candidate_kind IN ('SINGLE_CONTRACT', 'MULTI_LEG')
                  AND candidate.capital_at_risk > 0
                  AND candidate.market_data_time <= %s
                  AND candidate.market_data_time > %s - (%s * INTERVAL '1 day')
                GROUP BY candidate.candidate_id, signal.event_id
                                HAVING COUNT(DISTINCT outcome.measurement_type) < 5
                ORDER BY candidate.market_data_time, candidate.candidate_id
                LIMIT %s
                """,
                (
                    valuation_policy_sha256, available_by, available_by,
                    retention_days, limit,
                ),
            )
            return tuple(dict(row) for row in cursor.fetchall())

    def checkpoint_legs(
        self,
        candidate_id: UUID,
        *,
        checkpoint_time,
        available_by,
        maximum_mark_lag: timedelta = timedelta(minutes=15),
    ) -> tuple[OptionOutcomeLeg, ...]:
        if maximum_mark_lag <= timedelta(0):
            raise ValueError("maximum_mark_lag must be positive")
        with self._cursor() as cursor:
            cursor.execute(
                """
                WITH candidate_legs AS (
                    SELECT candidate_id, contract_id, side, ratio, multiplier,
                           model_mark AS entry_mark
                    FROM option_candidate_legs
                    WHERE candidate_id = %s
                ), eligible_batches AS (
                    SELECT snapshot.batch_id,
                           MAX(snapshot.mark_market_data_time) AS market_time,
                           MAX(snapshot.first_observed_at) AS observed_time
                    FROM option_chain_snapshots AS snapshot
                    JOIN candidate_legs AS leg USING (contract_id)
                    WHERE snapshot.model_mark IS NOT NULL
                      AND snapshot.mark_market_data_time >= %s
                      AND snapshot.mark_market_data_time <= %s + %s
                      AND snapshot.first_observed_at <= %s
                    GROUP BY snapshot.batch_id
                    HAVING COUNT(DISTINCT snapshot.contract_id) =
                           (SELECT COUNT(*) FROM candidate_legs)
                    ORDER BY market_time, observed_time, snapshot.batch_id
                    LIMIT 1
                ), selected AS (
                    SELECT DISTINCT ON (leg.contract_id)
                           leg.contract_id, leg.side, leg.ratio, leg.multiplier,
                           leg.entry_mark, snapshot.model_mark AS exit_mark,
                           snapshot.snapshot_id, snapshot.batch_id,
                           snapshot.mark_market_data_time,
                           snapshot.first_observed_at, snapshot.revision
                    FROM candidate_legs AS leg
                    JOIN option_chain_snapshots AS snapshot USING (contract_id)
                    JOIN eligible_batches AS batch USING (batch_id)
                    WHERE snapshot.first_observed_at <= %s
                    ORDER BY leg.contract_id, snapshot.first_observed_at DESC,
                             snapshot.revision DESC, snapshot.snapshot_id
                )
                SELECT * FROM selected ORDER BY contract_id
                """,
                (
                    candidate_id, checkpoint_time, checkpoint_time,
                    maximum_mark_lag, available_by, available_by,
                ),
            )
            rows = cursor.fetchall()
        return tuple(
            OptionOutcomeLeg(
                contract_id=int(row["contract_id"]),
                side=OptionSide(row["side"]),
                ratio=int(row["ratio"]),
                multiplier=int(row["multiplier"]),
                entry_mark=Decimal(row["entry_mark"]),
                exit_mark=Decimal(row["exit_mark"]),
                source_snapshot_id=row["snapshot_id"],
                source_batch_id=row["batch_id"],
                source_market_time=row["mark_market_data_time"],
                source_observed_time=row["first_observed_at"],
            )
            for row in rows
        )