import inspect
import sys
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.domain import (
    AssetType,
    ContractType,
    DecisionContext,
    ExerciseStyle,
    MarkSource,
    OptionContractSnapshot,
    OptionTradeEvent,
)
from options.repositories.snapshots import OptionSnapshotRepository
from options.repositories.trades import OptionTradeRepository


UTC = timezone.utc
HASH = "a" * 64


def _connection_factory(cursor):
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    return factory, connection


def _snapshot():
    market_time = datetime(2026, 8, 28, 20, 0, tzinfo=UTC)
    observed_at = datetime(2026, 8, 28, 20, 15, tzinfo=UTC)
    return OptionContractSnapshot(
        snapshot_id=uuid4(),
        contract_id=1,
        contract_ticker="O:SPY260904C00650000",
        underlyer="SPY",
        provider="polygon",
        contract_type=ContractType.CALL,
        expiration_date=date(2026, 9, 4),
        expiration_cutoff=datetime(2026, 9, 4, 20, 0, tzinfo=UTC),
        calendar_dte=7,
        time_to_expiration_years=7 / 365,
        strike=Decimal("650"),
        shares_per_contract=100,
        exercise_style=ExerciseStyle.AMERICAN,
        spot=Decimal("648.50"),
        spot_market_data_time=market_time,
        bid=None,
        ask=None,
        midpoint=None,
        display_mark=Decimal("4.20"),
        model_mark=Decimal("4.10"),
        mark_market_data_time=market_time,
        mark_source=MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE,
        day_volume=50,
        open_interest=200,
        market_data_time=market_time,
        first_observed_at=observed_at,
        revised_observed_at=None,
        local_iv=0.25,
        local_gamma=0.02,
        local_delta=0.48,
        local_theta_per_day=-0.10,
        local_vega_per_vol_point=0.05,
        local_rho_per_rate_point=0.01,
        intrinsic_value=Decimal("0"),
        extrinsic_value=Decimal("4.10"),
        single_contract_breakeven=Decimal("654.10"),
        provider_iv=None,
        provider_gamma=None,
        risk_free_rate=0.04,
        dividend_yield=0.0,
        iv_converged=True,
        iv_solver="NEWTON",
        iv_iteration_count=4,
        iv_price_error=1e-8,
        iv_failure_reason=None,
        model_version="option_model_v1",
        quality_flags=(),
        batch_id=uuid4(),
        raw_payload_sha256=HASH,
        normalized_payload_sha256=HASH,
    )


def test_snapshot_bulk_write_checks_partition_and_keeps_quote_columns():
    cursor = MagicMock()
    cursor.closed = False
    cursor.fetchone.return_value = {"ready": True}
    cursor.rowcount = 1
    factory, connection = _connection_factory(cursor)
    repository = OptionSnapshotRepository(factory)

    with patch(
        "options.repositories.snapshots.execute_values",
        side_effect=[[{"snapshot_id": _snapshot().snapshot_id}], None],
    ) as bulk_insert:
        snapshot = _snapshot()
        bulk_insert.side_effect = [[{"snapshot_id": snapshot.snapshot_id}], None]
        assert repository.persist(
            [snapshot], AssetType.ETF, "developer_v1", HASH
        ) == 1

    assert bulk_insert.call_count == 2
    registry_sql = bulk_insert.call_args_list[0].args[1]
    sql = bulk_insert.call_args_list[1].args[1]
    values = bulk_insert.call_args_list[1].args[2]
    assert "option_snapshot_fact_keys" in registry_sql
    assert "bid, ask, midpoint" in sql
    assert "ON CONFLICT" in sql
    assert "market_data_time, normalized_payload_sha256" in sql
    assert values[0][4] == "ETF"
    assert values[0][23] == "DEVELOPER_ALIGNED_AGG_CLOSE"
    connection.commit.assert_called_once_with()


def test_trade_write_and_cursor_use_shared_fact_contract_and_monotonic_watermark():
    cursor = MagicMock()
    cursor.closed = False
    cursor.fetchone.return_value = {"ready": True}
    cursor.rowcount = 1
    factory, _ = _connection_factory(cursor)
    repository = OptionTradeRepository(factory)
    now = datetime(2026, 8, 28, 20, 15, tzinfo=UTC)
    event = OptionTradeEvent(
        trade_event_id=uuid4(),
        provider="polygon",
        contract_id=1,
        contract_ticker="O:SPY260904C00650000",
        underlyer="SPY",
        sip_timestamp=now,
        sequence_number=10,
        participant_timestamp=None,
        first_observed_at=now,
        revised_observed_at=None,
        exchange=1,
        conditions=(),
        correction=None,
        price=Decimal("1.25"),
        size=2,
        shares_per_contract=100,
        notional=Decimal("250"),
        payload_sha256=HASH,
        raw_batch_id=uuid4(),
    )

    with patch("options.repositories.trades.execute_values") as bulk_insert:
        assert repository.persist([event]) == 1
    insert_sql = bulk_insert.call_args.args[1]
    insert_values = bulk_insert.call_args.args[2]
    assert "participant_timestamp, payload_sha256" in insert_sql
    assert insert_values[0][0] == event.trade_event_id
    assert insert_values[0][0] != event.raw_batch_id

    assert repository.advance_cursor("polygon", 1, now, 10, 30, "request-1") is True
    cursor_sql = cursor.execute.call_args.args[0]
    assert "EXCLUDED.completed_sip_timestamp" in cursor_sql
    assert ">=" in cursor_sql


def test_market_fact_reads_require_decision_context():
    snapshot_signature = inspect.signature(OptionSnapshotRepository.list_for_batch)
    trade_signature = inspect.signature(OptionTradeRepository.list_for_contract)

    assert snapshot_signature.parameters["context"].default is inspect.Parameter.empty
    assert trade_signature.parameters["context"].default is inspect.Parameter.empty

    now = datetime(2026, 8, 28, 20, 15, tzinfo=UTC)
    context = DecisionContext(market_time=now, observed_time=now)
    assert context.observed_time == now