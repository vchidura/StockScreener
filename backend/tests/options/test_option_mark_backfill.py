import pytest

from scripts import backfill_option_daily_marks
from options.repositories.catalog import HISTORICAL_BACKFILL_REASON


def test_contract_selection_applies_batch_limit_after_policy_resume(monkeypatch):
    contracts = [
        {"contract_id": contract_id, "underlying": "SPY"}
        for contract_id in range(1, 6)
    ]
    selected = backfill_option_daily_marks._select_pending_contracts(
        contracts,
        frozenset({1, 2}),
        2,
    )

    assert [contract["contract_id"] for contract in selected] == [3, 4]


def test_contract_selection_can_refetch_all_existing_contracts():
    contracts = [{"contract_id": contract_id} for contract_id in range(1, 4)]

    selected = backfill_option_daily_marks._select_pending_contracts(
        contracts,
        frozenset(),
        None,
    )

    assert selected == contracts


def test_contract_selection_rejects_non_positive_batch_limit():
    with pytest.raises(ValueError, match="maximum contracts must be positive"):
        backfill_option_daily_marks._select_pending_contracts([], frozenset(), 0)


def test_completed_policy_contract_query_uses_policy_and_contract_scope(monkeypatch):
    class Cursor:
        def __init__(self):
            self.parameters = None

        def execute(self, query, parameters):
            assert query == backfill_option_daily_marks.SQL_COMPLETED_POLICY_CONTRACTS
            self.parameters = parameters

        def fetchall(self):
            return [{"contract_id": 7}]

    cursor = Cursor()

    class CursorContext:
        def __enter__(self):
            return cursor

        def __exit__(self, exc_type, exc, traceback):
            return False

    monkeypatch.setattr(
        backfill_option_daily_marks,
        "get_db_cursor",
        lambda: CursorContext(),
    )

    result = backfill_option_daily_marks._completed_policy_contract_ids(
        [{"contract_id": 7}, {"contract_id": 8}],
        "b" * 64,
    )

    assert result == frozenset({7})
    assert cursor.parameters == ("b" * 64, [7, 8])


def test_historical_iv_admission_bypasses_expiry_close_filter():
    contract = {
        "strike": 120,
        "exclusion_reasons": [HISTORICAL_BACKFILL_REASON],
    }

    assert backfill_option_daily_marks._within_backfill_universe(
        contract,
        spot=100,
        moneyness_band=0.06,
    )


def test_live_catalog_contract_keeps_expiry_close_filter():
    contract = {"strike": 120, "exclusion_reasons": []}

    assert not backfill_option_daily_marks._within_backfill_universe(
        contract,
        spot=100,
        moneyness_band=0.06,
    )