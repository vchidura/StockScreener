from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import uuid4

from options.repositories.strategies import OptionStrategyRepository
from options.strategies.domain import OptionSide, StructureType


def test_recurring_package_reuses_event_and_records_candidate_occurrence():
    market_time = datetime(2026, 9, 2, 16, 0, tzinfo=timezone.utc)
    candidate_id = uuid4()
    existing_event_id = uuid4()
    leg = SimpleNamespace(
        leg_index=0,
        contract_id=101,
        contract_ticker="O:SPY260904P00600000",
        side=OptionSide.SELL,
        ratio=1,
        multiplier=100,
        model_mark=Decimal("2.00"),
        local_iv=0.25,
        local_gamma=0.01,
        expiration_date=market_time.date() + timedelta(days=2),
        strike=Decimal("600"),
    )
    candidate = SimpleNamespace(
        candidate_id=candidate_id,
        identity_sha256="b" * 64,
        matrix_id=uuid4(),
        underlyer="SPY",
        strategy_name="INCOME_WHEEL",
        strategy_version="1.0",
        policy_sha256="a" * 64,
        structure_type=StructureType.CASH_SECURED_PUT,
        legs=(leg,),
        net_premium=Decimal("2.00"),
        management_policy={},
        market_data_time=market_time,
        observed_time=market_time + timedelta(minutes=15),
        valid_until=market_time + timedelta(minutes=15),
        primary_evidence={"trigger": "test"},
        execution_eligibility=None,
        reason_codes=("EXECUTION_DISABLED",),
    )
    cursor = Mock()
    cursor.fetchone.side_effect = [
        None,
        {"event_id": existing_event_id},
        {"occurrence_id": uuid4()},
    ]

    with patch("options.repositories.strategies.execute_values"):
        OptionStrategyRepository._persist_signal(cursor, candidate)

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert not any("INSERT INTO option_signal_events" in sql for sql in statements)
    assert any("INSERT INTO option_signal_occurrences" in sql for sql in statements)
    assert any("occurrence_count" in sql and "UPDATE option_signal_events" in sql for sql in statements)
    occurrence_call = next(
        call for call in cursor.execute.call_args_list
        if "INSERT INTO option_signal_occurrences" in call.args[0]
    )
    assert occurrence_call.args[1][1:3] == (existing_event_id, candidate_id)