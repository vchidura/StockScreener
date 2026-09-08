from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from database import get_db_connection
from options.domain import AssetType, ContractType, DecisionContext
from options.repositories.catalog import (
    HISTORICAL_BACKFILL_REASON,
    HistoricalContractAdmission,
    OptionContractCatalogRepository,
)


@pytest.fixture
def rolled_back_repository(rolled_back_connection):
    factory, connection = rolled_back_connection
    yield OptionContractCatalogRepository(connection_factory=factory), connection


def _admission(ticker: str = "O:TEST240906C00500000") -> HistoricalContractAdmission:
    return HistoricalContractAdmission(
        contract_ticker=ticker,
        underlying="SPY",
        asset_type=AssetType.ETF,
        contract_type=ContractType.CALL,
        expiration_date=date(2024, 9, 6),
        strike=Decimal("500"),
        valid_from=datetime(2024, 7, 8, tzinfo=timezone.utc),
        valid_to=datetime(2024, 9, 6, 20, 0, tzinfo=timezone.utc),
    )


def test_admission_creates_catalog_and_version_rows(rolled_back_repository):
    repository, connection = rolled_back_repository
    observed = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)
    admitted = repository.admit_historical_contracts([_admission()], observed)
    assert len(admitted) == 1
    contract_id = next(iter(admitted.values()))

    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT v.eligibility_status, v.exclusion_reasons, v.contract_type,
                   v.expiration_date, v.strike, c.first_observed_at, c.expired_at
            FROM option_contract_catalog c
            JOIN option_contract_catalog_versions v ON v.contract_id = c.contract_id
            WHERE c.contract_id = %s
            """,
            (contract_id,),
        )
        row = cursor.fetchone()

    assert row[0] == "EXPIRED"
    assert HISTORICAL_BACKFILL_REASON in row[1]
    assert row[2] == "CALL"
    assert row[3] == date(2024, 9, 6)
    assert row[4] == Decimal("500.00000000")
    assert row[5] == observed
    # The catalog constrains expired_at to be at or after admission, so a contract that
    # expired before it was admitted cannot carry one. Eligibility is the guard instead.
    assert row[6] is None


def test_admitted_contracts_are_invisible_to_live_selection(rolled_back_repository):
    """Eligibility is what stops a backfilled contract entering a live cycle."""
    repository, _ = rolled_back_repository
    observed = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)
    admission = _admission()
    repository.admit_historical_contracts([admission], observed)

    context = DecisionContext(
        datetime(2026, 9, 8, 14, 0, tzinfo=timezone.utc),
        datetime(2026, 9, 8, 14, 0, tzinfo=timezone.utc),
    )
    assert repository.get_by_ticker(admission.contract_ticker, context) is None
    assert repository.get_by_tickers([admission.contract_ticker], context) == {}


def test_admission_is_idempotent(rolled_back_repository):
    repository, connection = rolled_back_repository
    observed = datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)
    admission = _admission()
    first = repository.admit_historical_contracts([admission], observed)
    second = repository.admit_historical_contracts([admission], observed)
    assert first == second

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM option_contract_catalog_versions WHERE contract_id = %s",
            (next(iter(first.values())),),
        )
        assert cursor.fetchone()[0] == 1


def test_payload_hash_is_stable_and_terms_sensitive():
    base = _admission()
    assert base.payload_sha256() == _admission().payload_sha256()
    moved = HistoricalContractAdmission(
        contract_ticker=base.contract_ticker,
        underlying=base.underlying,
        asset_type=base.asset_type,
        contract_type=base.contract_type,
        expiration_date=base.expiration_date,
        strike=Decimal("505"),
        valid_from=base.valid_from,
        valid_to=base.valid_to,
    )
    assert moved.payload_sha256() != base.payload_sha256()


def test_admission_does_not_touch_live_catalog_rows(rolled_back_repository):
    repository, connection = rolled_back_repository
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM option_contract_catalog_versions"
            " WHERE eligibility_status = 'VALIDATED_ACTIVE'"
        )
        before = cursor.fetchone()[0]
    repository.admit_historical_contracts(
        [_admission()], datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)
    )
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM option_contract_catalog_versions"
            " WHERE eligibility_status = 'VALIDATED_ACTIVE'"
        )
        assert cursor.fetchone()[0] == before


def test_empty_admission_is_a_no_op(rolled_back_repository):
    repository, _ = rolled_back_repository
    assert repository.admit_historical_contracts(
        [], datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)
    ) == {}
