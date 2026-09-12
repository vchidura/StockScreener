from __future__ import annotations

from datetime import date, datetime
from typing import Sequence

from options.model_inputs import RiskFreeRateObservation

from .base import ConnectionFactory, PostgresRepository


class OptionModelInputRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def persist_rates(self, rows: Sequence[RiskFreeRateObservation]) -> int:
        inserted = 0
        if not rows:
            return inserted
        with self._cursor() as cursor:
            for row in rows:
                cursor.execute(
                    """
                    INSERT INTO option_risk_free_rate_observations (
                        rate_observation_id, rate_date, tenor_days, annual_rate,
                        source, source_key, source_observed_at, first_observed_at,
                        availability_mode, replay_available_at, payload_sha256
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (source, source_key, first_observed_at) DO NOTHING
                    RETURNING rate_observation_id
                    """,
                    (
                        row.rate_observation_id, row.rate_date, row.tenor_days,
                        row.annual_rate, row.source, row.source_key,
                        row.source_observed_at, row.first_observed_at,
                        row.availability_mode, row.replay_available_at,
                        row.payload_sha256,
                    ),
                )
                inserted += cursor.fetchone() is not None
        return inserted

    def latest_curve(
        self,
        *,
        source: str,
        market_date: date,
        observed_at: datetime,
        include_reconstructed: bool = False,
    ) -> tuple[RiskFreeRateObservation, ...]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                WITH selected_date AS (
                    SELECT MAX(rate_date) AS rate_date
                    FROM option_risk_free_rate_observations
                    WHERE source = %s
                      AND rate_date <= %s
                      AND first_observed_at <= %s
                      AND (%s OR availability_mode = 'LIVE_OBSERVED')
                ), latest AS (
                    SELECT DISTINCT ON (tenor_days) *
                    FROM option_risk_free_rate_observations
                    WHERE source = %s
                      AND rate_date = (SELECT rate_date FROM selected_date)
                      AND first_observed_at <= %s
                      AND (%s OR availability_mode = 'LIVE_OBSERVED')
                    ORDER BY tenor_days, first_observed_at DESC
                )
                SELECT * FROM latest ORDER BY tenor_days
                """,
                (
                    source, market_date, observed_at, include_reconstructed,
                    source, observed_at, include_reconstructed,
                ),
            )
            return tuple(RiskFreeRateObservation(
                rate_observation_id=row["rate_observation_id"],
                rate_date=row["rate_date"],
                tenor_days=row["tenor_days"],
                annual_rate=float(row["annual_rate"]),
                source=row["source"],
                source_key=row["source_key"],
                source_observed_at=row["source_observed_at"],
                first_observed_at=row["first_observed_at"],
                availability_mode=row["availability_mode"],
                replay_available_at=row["replay_available_at"],
                payload_sha256=row["payload_sha256"],
            ) for row in cursor.fetchall())

    def curves_between(
        self,
        *,
        source: str,
        start: date,
        end: date,
        observed_at: datetime,
    ) -> tuple[RiskFreeRateObservation, ...]:
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT ON (rate_date, tenor_days) *
                FROM option_risk_free_rate_observations
                WHERE source = %s
                  AND rate_date BETWEEN %s AND %s
                  AND COALESCE(replay_available_at, first_observed_at) <= %s
                ORDER BY rate_date, tenor_days, first_observed_at DESC
                """,
                (source, start, end, observed_at),
            )
            return tuple(RiskFreeRateObservation(
                rate_observation_id=row["rate_observation_id"],
                rate_date=row["rate_date"],
                tenor_days=row["tenor_days"],
                annual_rate=float(row["annual_rate"]),
                source=row["source"],
                source_key=row["source_key"],
                source_observed_at=row["source_observed_at"],
                first_observed_at=row["first_observed_at"],
                availability_mode=row["availability_mode"],
                replay_available_at=row["replay_available_at"],
                payload_sha256=row["payload_sha256"],
            ) for row in cursor.fetchall())