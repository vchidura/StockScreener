from __future__ import annotations

from typing import Any

from options.domain import (
    DecisionContext,
    ProviderTradeSemantics,
    TradeSemanticsBehavior,
)

from .base import ConnectionFactory, PostgresRepository


class OptionTradeSemanticsRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def persist(self, semantics: ProviderTradeSemantics) -> bool:
        with self._cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO option_provider_trade_semantics (
                    provider, semantics_version, condition_code, correction_code,
                    behavior, contributes_volume, contributes_notional,
                    effective_from, effective_to, first_observed_at,
                    configuration_sha256
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (
                    provider, semantics_version, condition_code, correction_code,
                    effective_from
                ) DO NOTHING
                """,
                (
                    semantics.provider,
                    semantics.semantics_version,
                    semantics.condition_code,
                    semantics.correction_code,
                    semantics.behavior.value,
                    semantics.contributes_volume,
                    semantics.contributes_notional,
                    semantics.effective_from,
                    semantics.effective_to,
                    semantics.first_observed_at,
                    semantics.configuration_sha256,
                ),
            )
            return cursor.rowcount == 1

    def get(
        self,
        provider: str,
        condition_code: int | None,
        correction_code: int | None,
        context: DecisionContext,
    ) -> ProviderTradeSemantics | None:
        if condition_code is None and correction_code is None:
            raise ValueError("trade semantics require a condition or correction code")
        with self._cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    provider, semantics_version, condition_code, correction_code,
                    behavior, contributes_volume, contributes_notional,
                    effective_from, effective_to, first_observed_at,
                    configuration_sha256
                FROM option_provider_trade_semantics
                WHERE provider = %s
                  AND condition_code IS NOT DISTINCT FROM %s
                  AND correction_code IS NOT DISTINCT FROM %s
                  AND effective_from <= %s
                  AND (effective_to IS NULL OR effective_to > %s)
                  AND first_observed_at <= %s
                ORDER BY effective_from DESC, first_observed_at DESC, semantics_id DESC
                LIMIT 1
                """,
                (
                    provider,
                    condition_code,
                    correction_code,
                    context.market_time,
                    context.market_time,
                    context.observed_time,
                ),
            )
            row = cursor.fetchone()
        return _semantics(row) if row else None


def _semantics(row: dict[str, Any]) -> ProviderTradeSemantics:
    return ProviderTradeSemantics(
        provider=row["provider"],
        semantics_version=row["semantics_version"],
        condition_code=row["condition_code"],
        correction_code=row["correction_code"],
        behavior=TradeSemanticsBehavior(row["behavior"]),
        contributes_volume=row["contributes_volume"],
        contributes_notional=row["contributes_notional"],
        effective_from=row["effective_from"],
        effective_to=row["effective_to"],
        first_observed_at=row["first_observed_at"],
        configuration_sha256=row["configuration_sha256"],
    )