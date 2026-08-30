import inspect
import sys
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.domain import (
    AssetType,
    CatalogEligibility,
    DecisionContext,
    OptionContractReference,
    validate_standard_contract,
)
from options.repositories.catalog import OptionContractCatalogRepository


def _repository_with_row(row):
    cursor = MagicMock()
    cursor.closed = False
    cursor.fetchone.return_value = row
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor

    @contextmanager
    def connection_factory():
        yield connection

    return OptionContractCatalogRepository(connection_factory), connection, cursor


def test_catalog_read_requires_context_and_enforces_bitemporal_bounds():
    now = datetime(2026, 8, 28, 20, 15, tzinfo=timezone.utc)
    row = {
        "contract_id": 7,
        "contract_ticker": "O:SPY260904C00650000",
        "underlying": "SPY",
        "asset_type": "ETF",
        "provider": "polygon",
        "provider_version": "1",
        "contract_type": "CALL",
        "expiration_date": date(2026, 9, 4),
        "strike": Decimal("650"),
        "exercise_style": "AMERICAN",
        "shares_per_contract": 100,
        "primary_exchange": "X",
        "eligibility_status": "VALIDATED_ACTIVE",
        "exclusion_reasons": [],
        "valid_from": now.replace(minute=0),
        "valid_to": None,
        "first_observed_at": now,
        "revised_observed_at": None,
        "payload_sha256": "a" * 64,
    }
    repository, connection, cursor = _repository_with_row(row)
    context = DecisionContext(market_time=now, observed_time=now)

    entry = repository.get_by_ticker(row["contract_ticker"], context)

    assert entry is not None
    assert entry.contract_id == 7
    sql, parameters = cursor.execute.call_args.args
    assert "v.valid_from <= %s" in sql
    assert "COALESCE(v.revised_observed_at, v.first_observed_at) <= %s" in sql
    assert parameters[-1] == context.observed_time
    connection.commit.assert_called_once_with()

    signature = inspect.signature(repository.get_by_ticker)
    assert signature.parameters["context"].default is inspect.Parameter.empty


def test_missing_catalog_contract_returns_none():
    repository, _, _ = _repository_with_row(None)
    now = datetime(2026, 8, 28, 20, 15, tzinfo=timezone.utc)

    assert repository.get_by_ticker(
        "O:UNKNOWN", DecisionContext(market_time=now, observed_time=now)
    ) is None


def test_eligible_read_selects_latest_visible_revision_before_filtering():
    repository, _, cursor = _repository_with_row(None)
    cursor.fetchall.return_value = []
    now = datetime(2026, 8, 28, 20, 15, tzinfo=timezone.utc)
    context = DecisionContext(market_time=now, observed_time=now)

    assert repository.list_eligible("SPY", date(2026, 9, 30), context) == ()

    sql, parameters = cursor.execute.call_args.args
    assert "SELECT DISTINCT ON (c.contract_id)" in sql
    assert sql.index("SELECT DISTINCT ON") < sql.index("eligibility_status = 'VALIDATED_ACTIVE'")
    assert parameters[1] == context.observed_time
    assert parameters[-1] == date(2026, 9, 30)


def _reference(**overrides):
    now = datetime(2026, 8, 28, 20, 15, tzinfo=timezone.utc)
    values = {
        "contract_ticker": "O:SPY260904C00650000",
        "underlyer": "SPY",
        "asset_type": AssetType.ETF,
        "provider": "polygon",
        "provider_version": "1",
        "provider_contract_type": "call",
        "expiration_date": date(2026, 9, 4),
        "strike": Decimal("650"),
        "provider_exercise_style": "american",
        "shares_per_contract": 100,
        "primary_exchange": "X",
        "correction": None,
        "additional_underlyings_json": "[]",
        "adjustment_metadata_json": "{}",
        "changes_deliverables": False,
        "valid_from": now.replace(minute=0),
        "valid_to": None,
        "first_observed_at": now,
        "revised_observed_at": None,
        "refreshed_at": now,
        "payload_sha256": "a" * 64,
    }
    values.update(overrides)
    return OptionContractReference(**values)


def test_standard_contract_validation_preserves_all_rejection_reasons():
    validation = validate_standard_contract(
        _reference(
            provider_contract_type="other",
            provider_exercise_style="european",
            shares_per_contract=10,
            additional_underlyings_json='[{"ticker":"XYZ"}]',
        )
    )

    assert validation.eligibility_status is CatalogEligibility.REJECTED_UNSUPPORTED
    assert validation.exclusion_reasons == (
        "UNSUPPORTED_CONTRACT_TYPE",
        "UNSUPPORTED_EXERCISE_STYLE",
        "UNSUPPORTED_MULTIPLIER",
        "ADJUSTED_CONTRACT",
    )


def test_catalog_upsert_writes_stable_identity_and_append_only_version():
    repository, connection, cursor = _repository_with_row(None)
    cursor.fetchone.side_effect = [{"contract_id": 7}]

    assert repository.upsert_reference(_reference()) == 7

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert "ON CONFLICT (contract_ticker) DO NOTHING" in statements[0]
    assert "INSERT INTO option_contract_catalog_versions" in statements[1]
    assert "payload_sha256, first_observed_at" in statements[1]
    connection.commit.assert_called_once_with()


def test_bulk_catalog_lookup_uses_one_bitemporal_query():
    repository, _, cursor = _repository_with_row(None)
    cursor.fetchall.return_value = []
    now = datetime(2026, 8, 28, 20, 15, tzinfo=timezone.utc)
    context = DecisionContext(market_time=now, observed_time=now)

    assert repository.get_by_tickers(["O:ONE", "O:TWO", "O:ONE"], context) == {}

    sql, parameters = cursor.execute.call_args.args
    assert "c.contract_ticker = ANY(%s)" in sql
    assert parameters[0] == ["O:ONE", "O:TWO"]
    assert parameters[-1] == context.observed_time


def test_latest_rejected_reference_is_not_returned_as_an_older_valid_contract():
    rejected_row = {
        "eligibility_status": "REJECTED_UNSUPPORTED",
    }
    repository, _, _ = _repository_with_row(rejected_row)
    now = datetime(2026, 8, 28, 20, 15, tzinfo=timezone.utc)

    assert repository.get_by_ticker(
        "O:REJECTED", DecisionContext(market_time=now, observed_time=now)
    ) is None