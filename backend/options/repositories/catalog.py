from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Collection

from psycopg2.extras import Json

from options.domain import (
    AssetType,
    CatalogEligibility,
    ContractType,
    DecisionContext,
    ExerciseStyle,
    OptionContractCatalogEntry,
    OptionContractReference,
    validate_standard_contract,
)
from options.errors import DuplicateFactConflict

from .base import ConnectionFactory, PostgresRepository


_CATALOG_SELECT = """
    SELECT
        c.contract_id,
        c.contract_ticker,
        c.underlying,
        c.asset_type,
        v.provider,
        v.provider_version,
        v.contract_type,
        v.expiration_date,
        v.strike,
        v.exercise_style,
        v.shares_per_contract,
        v.primary_exchange,
        v.eligibility_status,
        v.exclusion_reasons,
        v.valid_from,
        v.valid_to,
        v.first_observed_at,
        v.revised_observed_at,
        v.payload_sha256
    FROM option_contract_catalog AS c
    JOIN option_contract_catalog_versions AS v
      ON v.contract_id = c.contract_id
"""


def _upsert_reference(cursor: Any, reference: OptionContractReference):
    validation = validate_standard_contract(reference)
    admitted_at = None
    if validation.eligibility_status is CatalogEligibility.VALIDATED_ACTIVE:
        admitted_at = reference.revised_observed_at or reference.first_observed_at

    cursor.execute(
        """
        INSERT INTO option_contract_catalog (
            contract_ticker,
            underlying,
            asset_type,
            first_observed_at,
            catalog_admitted_at
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (contract_ticker) DO NOTHING
        RETURNING contract_id
        """,
        (
            reference.contract_ticker,
            reference.underlyer,
            reference.asset_type.value,
            reference.first_observed_at,
            admitted_at,
        ),
    )
    inserted = cursor.fetchone()
    if inserted:
        contract_id = inserted["contract_id"]
    else:
        cursor.execute(
            """
            SELECT contract_id, underlying, asset_type
            FROM option_contract_catalog
            WHERE contract_ticker = %s
            FOR UPDATE
            """,
            (reference.contract_ticker,),
        )
        existing = cursor.fetchone()
        if not existing:
            raise DuplicateFactConflict("catalog conflict could not be resolved")
        if (
            existing["underlying"] != reference.underlyer
            or existing["asset_type"] != reference.asset_type.value
        ):
            raise DuplicateFactConflict(
                "contract ticker resolved to a different underlying or asset type"
            )
        contract_id = existing["contract_id"]
        if admitted_at is not None:
            cursor.execute(
                """
                UPDATE option_contract_catalog
                SET catalog_admitted_at = COALESCE(catalog_admitted_at, %s),
                    updated_at = NOW()
                WHERE contract_id = %s
                """,
                (admitted_at, contract_id),
            )

    cursor.execute(
        """
        INSERT INTO option_contract_catalog_versions (
            contract_id, provider, provider_version,
            provider_contract_type, contract_type, expiration_date, strike,
            provider_exercise_style, exercise_style, shares_per_contract,
            primary_exchange, correction, additional_underlyings,
            adjustment_metadata, eligibility_status, exclusion_reasons,
            valid_from, valid_to, first_observed_at, revised_observed_at,
            refreshed_at, payload_sha256
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (
            contract_id, provider, payload_sha256, first_observed_at
        ) DO NOTHING
        """,
        (
            contract_id,
            reference.provider,
            reference.provider_version,
            reference.provider_contract_type,
            validation.contract_type.value if validation.contract_type else None,
            reference.expiration_date,
            reference.strike,
            reference.provider_exercise_style,
            validation.exercise_style.value if validation.exercise_style else None,
            reference.shares_per_contract,
            reference.primary_exchange,
            reference.correction,
            Json(json.loads(reference.additional_underlyings_json)),
            Json(json.loads(reference.adjustment_metadata_json)),
            validation.eligibility_status.value,
            list(validation.exclusion_reasons),
            reference.valid_from,
            reference.valid_to,
            reference.first_observed_at,
            reference.revised_observed_at,
            reference.refreshed_at,
            reference.payload_sha256,
        ),
    )
    return contract_id, validation


class OptionContractCatalogRepository(PostgresRepository):
    def __init__(self, connection_factory: ConnectionFactory | None = None) -> None:
        super().__init__(connection_factory)

    def upsert_reference(self, reference: OptionContractReference) -> int:
        with self._cursor() as cursor:
            contract_id, _ = _upsert_reference(cursor, reference)
            return contract_id

    def upsert_references(
        self,
        references: Collection[OptionContractReference],
    ) -> tuple[int, ...]:
        with self._cursor() as cursor:
            return tuple(
                _upsert_reference(cursor, reference)[0]
                for reference in references
            )

    def mark_expired(self, contract_id: int, expired_at: datetime) -> bool:
        with self._cursor() as cursor:
            cursor.execute(
                """
                UPDATE option_contract_catalog
                SET expired_at = %s,
                    updated_at = NOW()
                WHERE contract_id = %s
                  AND (expired_at IS NULL OR expired_at > %s)
                """,
                (expired_at, contract_id, expired_at),
            )
            return cursor.rowcount == 1

    def get_by_ticker(
        self,
        contract_ticker: str,
        context: DecisionContext,
    ) -> OptionContractCatalogEntry | None:
        with self._cursor() as cursor:
            cursor.execute(
                _CATALOG_SELECT
                + """
                WHERE c.contract_ticker = %s
                                    AND c.first_observed_at <= %s
                                    AND c.catalog_admitted_at <= %s
                                    AND (c.expired_at IS NULL OR c.expired_at > %s)
                  AND v.valid_from <= %s
                  AND (v.valid_to IS NULL OR v.valid_to > %s)
                  AND COALESCE(v.revised_observed_at, v.first_observed_at) <= %s
                ORDER BY
                    v.valid_from DESC,
                    COALESCE(v.revised_observed_at, v.first_observed_at) DESC,
                    v.catalog_version_id DESC
                LIMIT 1
                """,
                (
                    contract_ticker,
                    context.observed_time,
                    context.observed_time,
                    context.market_time,
                    context.market_time,
                    context.market_time,
                    context.observed_time,
                ),
            )
            row = cursor.fetchone()
        if not row or row["eligibility_status"] != CatalogEligibility.VALIDATED_ACTIVE.value:
            return None
        return _catalog_entry(row)

    def get_by_tickers(
        self,
        contract_tickers: Collection[str],
        context: DecisionContext,
    ) -> dict[str, OptionContractCatalogEntry]:
        tickers = tuple(dict.fromkeys(contract_tickers))
        if not tickers:
            return {}
        with self._cursor() as cursor:
            cursor.execute(
                """
                WITH available AS (
                    SELECT DISTINCT ON (c.contract_id)
                        c.contract_id,
                        c.contract_ticker,
                        c.underlying,
                        c.asset_type,
                        v.provider,
                        v.provider_version,
                        v.contract_type,
                        v.expiration_date,
                        v.strike,
                        v.exercise_style,
                        v.shares_per_contract,
                        v.primary_exchange,
                        v.eligibility_status,
                        v.exclusion_reasons,
                        v.valid_from,
                        v.valid_to,
                        v.first_observed_at,
                        v.revised_observed_at,
                        v.payload_sha256
                    FROM option_contract_catalog AS c
                    JOIN option_contract_catalog_versions AS v
                      ON v.contract_id = c.contract_id
                    WHERE c.contract_ticker = ANY(%s)
                      AND c.first_observed_at <= %s
                      AND c.catalog_admitted_at <= %s
                      AND (c.expired_at IS NULL OR c.expired_at > %s)
                      AND v.valid_from <= %s
                      AND (v.valid_to IS NULL OR v.valid_to > %s)
                      AND COALESCE(v.revised_observed_at, v.first_observed_at) <= %s
                    ORDER BY
                        c.contract_id,
                        v.valid_from DESC,
                        COALESCE(v.revised_observed_at, v.first_observed_at) DESC,
                        v.catalog_version_id DESC
                )
                SELECT *
                FROM available
                WHERE eligibility_status = 'VALIDATED_ACTIVE'
                ORDER BY contract_ticker
                """,
                (
                    list(tickers),
                    context.observed_time,
                    context.observed_time,
                    context.market_time,
                    context.market_time,
                    context.market_time,
                    context.observed_time,
                ),
            )
            rows = cursor.fetchall()
        return {
            row["contract_ticker"]: _catalog_entry(row)
            for row in rows
        }

    def list_eligible(
        self,
        underlyer: str,
        expiration_through: date,
        context: DecisionContext,
    ) -> tuple[OptionContractCatalogEntry, ...]:
        with self._cursor() as cursor:
            cursor.execute(
                                """
                                WITH available AS (
                                        SELECT DISTINCT ON (c.contract_id)
                                                c.contract_id,
                                                c.contract_ticker,
                                                c.underlying,
                                                c.asset_type,
                                                v.provider,
                                                v.provider_version,
                                                v.contract_type,
                                                v.expiration_date,
                                                v.strike,
                                                v.exercise_style,
                                                v.shares_per_contract,
                                                v.primary_exchange,
                                                v.eligibility_status,
                                                v.exclusion_reasons,
                                                v.valid_from,
                                                v.valid_to,
                                                v.first_observed_at,
                                                v.revised_observed_at,
                                                v.payload_sha256
                                        FROM option_contract_catalog AS c
                                        JOIN option_contract_catalog_versions AS v
                                            ON v.contract_id = c.contract_id
                                        WHERE c.underlying = %s
                                            AND c.first_observed_at <= %s
                                            AND c.catalog_admitted_at <= %s
                                            AND (c.expired_at IS NULL OR c.expired_at > %s)
                                            AND v.valid_from <= %s
                                            AND (v.valid_to IS NULL OR v.valid_to > %s)
                                            AND COALESCE(v.revised_observed_at, v.first_observed_at) <= %s
                                        ORDER BY
                                                c.contract_id,
                                                v.valid_from DESC,
                                                COALESCE(v.revised_observed_at, v.first_observed_at) DESC,
                                                v.catalog_version_id DESC
                                )
                                SELECT *
                                FROM available
                                WHERE eligibility_status = 'VALIDATED_ACTIVE'
                                    AND expiration_date BETWEEN %s AND %s
                ORDER BY
                                        expiration_date,
                                        strike,
                                        contract_type,
                                        contract_id
                """,
                (
                    underlyer,
                    context.observed_time,
                    context.observed_time,
                    context.market_time,
                    context.market_time,
                    context.market_time,
                    context.observed_time,
                    context.market_time.date(),
                    expiration_through,
                ),
            )
            rows = cursor.fetchall()
        return tuple(_catalog_entry(row) for row in rows)


def _catalog_entry(row: dict[str, Any]) -> OptionContractCatalogEntry:
    return OptionContractCatalogEntry(
        contract_id=row["contract_id"],
        contract_ticker=row["contract_ticker"],
        underlyer=row["underlying"],
        asset_type=AssetType(row["asset_type"]),
        provider=row["provider"],
        provider_version=row["provider_version"],
        contract_type=ContractType(row["contract_type"]),
        expiration_date=row["expiration_date"],
        strike=row["strike"],
        exercise_style=ExerciseStyle(row["exercise_style"]),
        shares_per_contract=row["shares_per_contract"],
        primary_exchange=row["primary_exchange"],
        eligibility_status=CatalogEligibility(row["eligibility_status"]),
        exclusion_reasons=tuple(row["exclusion_reasons"]),
        valid_from=row["valid_from"],
        valid_to=row["valid_to"],
        first_observed_at=row["first_observed_at"],
        revised_observed_at=row["revised_observed_at"],
        payload_sha256=row["payload_sha256"],
    )