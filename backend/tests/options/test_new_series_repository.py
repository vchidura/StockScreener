import sys
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.domain import (
    AssetType,
    CatalogEligibility,
    ContractReferenceValidation,
    ContractType,
    ExerciseStyle,
    NewSeriesState,
    OptionContractReference,
)
from options.repositories.new_series import OptionNewSeriesRepository


UTC = timezone.utc


def _repository():
    cursor = MagicMock()
    cursor.closed = False
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    return OptionNewSeriesRepository(factory), connection, cursor


def _reference(observed_at):
    return OptionContractReference(
        contract_ticker="O:SPY260904C00650000",
        underlyer="SPY",
        asset_type=AssetType.ETF,
        provider="polygon",
        provider_version="1",
        provider_contract_type="call",
        expiration_date=date(2026, 9, 4),
        strike=Decimal("650"),
        provider_exercise_style="american",
        shares_per_contract=100,
        primary_exchange="X",
        correction=None,
        additional_underlyings_json="[]",
        adjustment_metadata_json="{}",
        changes_deliverables=False,
        valid_from=observed_at - timedelta(minutes=15),
        valid_to=None,
        first_observed_at=observed_at,
        revised_observed_at=None,
        refreshed_at=observed_at,
        payload_sha256="a" * 64,
    )


def test_resolved_reference_after_seal_activates_at_next_matrix():
    repository, connection, cursor = _repository()
    observed_at = datetime(2026, 8, 28, 20, 15, tzinfo=UTC)
    next_matrix = observed_at + timedelta(minutes=15)
    cursor.fetchone.return_value = {
        "contract_ticker": "O:SPY260904C00650000",
        "underlying": "SPY",
        "state": "REFERENCE_PENDING",
        "contract_id": None,
        "activate_after": None,
    }
    validation = ContractReferenceValidation(
        contract_type=ContractType.CALL,
        exercise_style=ExerciseStyle.AMERICAN,
        eligibility_status=CatalogEligibility.VALIDATED_ACTIVE,
        exclusion_reasons=(),
    )

    with patch(
        "options.repositories.new_series._upsert_reference",
        return_value=(42, validation),
    ):
        resolution = repository.resolve_reference(
            7,
            _reference(observed_at),
            matrix_sealed_at=observed_at,
            next_matrix_time=next_matrix,
        )

    assert resolution.state is NewSeriesState.VALIDATED_ACTIVE
    assert resolution.activate_after == next_matrix
    update_parameters = cursor.execute.call_args_list[-1].args[1]
    assert update_parameters[3] == next_matrix
    connection.commit.assert_called_once_with()


def test_watchlist_activation_schedules_session_open_backfill_atomically():
    repository, connection, cursor = _repository()
    active_from = datetime(2026, 8, 28, 20, 30, tzinfo=UTC)
    session_open = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    work_id = uuid4()
    cursor.fetchone.side_effect = [
        {
            "state": "VALIDATED_ACTIVE",
            "contract_id": 42,
            "activate_after": active_from,
        },
        {"work_id": work_id},
    ]

    returned = repository.activate_filtered_watchlist(
        7,
        active_from=active_from,
        observed_at=active_from,
        session_open=session_open,
        delayed_watermark=active_from,
        work_id=work_id,
        maximum_work_attempts=5,
    )

    assert returned == work_id
    statements = "\n".join(call.args[0] for call in cursor.execute.call_args_list)
    assert "INSERT INTO option_trade_watchlist" in statements
    assert "TRADE_BACKFILL" not in statements
    work_parameters = cursor.execute.call_args_list[2].args[1]
    assert work_parameters[1] == "TRADE_BACKFILL"
    assert "UPDATE option_contract_discoveries" in statements
    connection.commit.assert_called_once_with()


def test_watchlist_activation_retry_returns_existing_backfill_work():
    repository, _, cursor = _repository()
    active_from = datetime(2026, 8, 28, 20, 30, tzinfo=UTC)
    session_open = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    existing_work_id = uuid4()
    cursor.fetchone.side_effect = [
        {
            "state": "WATCHLIST_ACTIVE",
            "contract_id": 42,
            "activate_after": active_from,
        },
        {"work_id": existing_work_id},
    ]

    returned = repository.activate_filtered_watchlist(
        7,
        active_from=active_from,
        observed_at=active_from,
        session_open=session_open,
        delayed_watermark=active_from,
        work_id=uuid4(),
        maximum_work_attempts=5,
    )

    assert returned == existing_work_id
    assert cursor.execute.call_count == 2