from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Sequence
from uuid import UUID

from options.analytics.iv_context import IvContextStatistics

from .base import ConnectionFactory, PostgresRepository


@dataclass(frozen=True, slots=True)
class IvContextRecord:
    iv_context_id: UUID
    matrix_id: UUID
    underlying: str
    expiration_bucket: str
    statistics: IvContextStatistics
    calculation_version: str
    first_observed_time: datetime
    settlement_valuation_policy_version: str
    settlement_valuation_policy_sha256: str
    rate_source: str
    rate_observation_ids: tuple[UUID, ...]
    dividend_source: str
    dividend_coverage_ids: tuple[UUID, ...]
    dividend_action_ids: tuple[UUID, ...]


class OptionIvContextRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def persist(self, records: Sequence[IvContextRecord]) -> int:
        inserted = 0
        with self._cursor() as cursor:
            for row in records:
                values = row.statistics
                cursor.execute(
                    """
                    INSERT INTO option_iv_context_snapshots (
                        iv_context_id, matrix_id, underlying, expiration_bucket,
                        current_comparable_iv, lookback_start_date,
                        lookback_end_date, sample_count, coverage_fraction,
                        minimum_iv, maximum_iv, range_position_rank,
                        empirical_percentile, calculation_version,
                        first_observed_time, null_reason_codes,
                        settlement_valuation_policy_version,
                        settlement_valuation_policy_sha256, rate_source,
                        rate_observation_ids, dividend_source,
                        dividend_coverage_ids, dividend_action_ids,
                        history_availability_mode
                    ) VALUES (
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,'HISTORICAL_RECONSTRUCTED'
                    )
                    ON CONFLICT (
                        matrix_id, expiration_bucket, calculation_version,
                        settlement_valuation_policy_sha256
                    ) DO NOTHING
                    RETURNING iv_context_id
                    """,
                    (
                        row.iv_context_id, row.matrix_id, row.underlying,
                        row.expiration_bucket, values.current_comparable_iv,
                        values.lookback_start_date, values.lookback_end_date,
                        values.sample_count, values.coverage_fraction,
                        values.minimum_iv, values.maximum_iv,
                        values.range_position_rank, values.empirical_percentile,
                        row.calculation_version, row.first_observed_time,
                        list(values.null_reason_codes),
                        row.settlement_valuation_policy_version,
                        row.settlement_valuation_policy_sha256, row.rate_source,
                        list(row.rate_observation_ids), row.dividend_source,
                        list(row.dividend_coverage_ids),
                        list(row.dividend_action_ids),
                    ),
                )
                inserted += cursor.fetchone() is not None
        return inserted