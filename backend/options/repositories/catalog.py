from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
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

# Marks a catalog version learned by asking the reference endpoint about the past rather
# than by observing the contract in a live chain. Without it, a backfilled contract and
# one that was seen live and later expired are indistinguishable.
HISTORICAL_BACKFILL_REASON = "HISTORICAL_REFERENCE_BACKFILL"


@dataclass(frozen=True, slots=True)
class HistoricalContractAdmission:
    """An expired contract admitted so its settlement marks have somewhere to live."""

    contract_ticker: str
    underlying: str
    asset_type: AssetType
    contract_type: ContractType
    expiration_date: date
    strike: Decimal
    valid_from: datetime
    valid_to: datetime
    shares_per_contract: int = 100
    exercise_style: ExerciseStyle = ExerciseStyle.AMERICAN
    provider: str = "polygon"
    primary_exchange: str | None = None

    def payload_sha256(self) -> str:
        payload = json.dumps(
            {
                "contract_ticker": self.contract_ticker,
                "underlying": self.underlying,
                "contract_type": self.contract_type.value,
                "expiration_date": self.expiration_date.isoformat(),
                "strike": str(self.strike),
                "shares_per_contract": self.shares_per_contract,
                "exercise_style": self.exercise_style.value,
                "provider": self.provider,
                "source": HISTORICAL_BACKFILL_REASON,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()



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

    def admit_historical_contracts(
        self,
        admissions: Collection[HistoricalContractAdmission],
        observed_at: datetime,
    ) -> dict[str, int]:
        """Admit expired contracts so historical marks have a contract to hang from.

        Written as `EXPIRED`, never `VALIDATED_ACTIVE`, so `get_by_ticker` cannot return
        them and no live cycle can select them. `first_observed_at` is when the backfill
        ran, which is the truth and is also what keeps these contracts invisible to a
        replay of any earlier decision. `valid_from`/`valid_to` describe when the terms
        held in the market, a separate axis from when they became known.

        `expired_at` stays NULL: the catalog constrains it to be at or after
        `catalog_admitted_at`, so a contract that expired before it was admitted cannot
        carry one. Eligibility is the guard instead.
        """
        admitted: dict[str, int] = {}
        with self._cursor() as cursor:
            for item in admissions:
                cursor.execute(
                    """
                    INSERT INTO option_contract_catalog (
                        contract_ticker, underlying, asset_type,
                        first_observed_at, catalog_admitted_at
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (contract_ticker) DO NOTHING
                    RETURNING contract_id
                    """,
                    (
                        item.contract_ticker,
                        item.underlying,
                        item.asset_type.value,
                        observed_at,
                        observed_at,
                    ),
                )
                inserted = cursor.fetchone()
                if inserted:
                    contract_id = inserted["contract_id"]
                else:
                    cursor.execute(
                        "SELECT contract_id FROM option_contract_catalog"
                        " WHERE contract_ticker = %s",
                        (item.contract_ticker,),
                    )
                    existing = cursor.fetchone()
                    if not existing:
                        raise DuplicateFactConflict(
                            "catalog conflict could not be resolved"
                        )
                    contract_id = existing["contract_id"]

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
                        item.provider,
                        None,
                        item.contract_type.value.lower(),
                        item.contract_type.value,
                        item.expiration_date,
                        item.strike,
                        item.exercise_style.value.lower(),
                        item.exercise_style.value,
                        item.shares_per_contract,
                        item.primary_exchange,
                        None,
                        Json([]),
                        Json({}),
                        CatalogEligibility.EXPIRED.value,
                        [HISTORICAL_BACKFILL_REASON],
                        item.valid_from,
                        item.valid_to,
                        observed_at,
                        None,
                        observed_at,
                        item.payload_sha256(),
                    ),
                )
                admitted[item.contract_ticker] = contract_id
        return admitted

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