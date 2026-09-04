from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from options.outcomes import (
    OptionOutcomeLeg,
    delayed_proxy_commission_policy,
    evaluate_delayed_proxy_outcome,
    measurement_checkpoints,
)
from options.outcome_service import OptionOutcomeService
from options.repositories.outcomes import OptionOutcomeRepository
from options.strategies.domain import OptionSide
from unittest.mock import MagicMock, patch


UTC = timezone.utc


def leg(side, entry, exit_, *, ratio=1, batch_id=None):
    market_time = datetime(2026, 8, 31, 20, 30, tzinfo=UTC)
    return OptionOutcomeLeg(
        contract_id=42 + ratio,
        side=side,
        ratio=ratio,
        multiplier=100,
        entry_mark=Decimal(entry),
        exit_mark=Decimal(exit_),
        source_snapshot_id=uuid4(),
        source_batch_id=batch_id or uuid4(),
        source_market_time=market_time,
        source_observed_time=market_time + timedelta(minutes=15),
    )


def test_credit_spread_proxy_return_deducts_round_trip_commission():
    batch_id = uuid4()
    legs = (
        leg(OptionSide.SELL, "3", "2", batch_id=batch_id),
        leg(OptionSide.BUY, "1", "0.5", batch_id=batch_id),
    )

    result = evaluate_delayed_proxy_outcome(
        candidate_id=uuid4(), event_id=uuid4(), measurement_type="30MIN",
        market_time=datetime(2026, 8, 31, 20, 30, tzinfo=UTC),
        observed_time=datetime(2026, 8, 31, 20, 45, tzinfo=UTC),
        capital_at_risk=Decimal("300"), legs=legs,
        policy=delayed_proxy_commission_policy(),
    )

    assert result.entry_net_premium == Decimal("200")
    assert result.exit_net_premium == Decimal("150.0")
    assert result.gross_pnl == Decimal("50.0")
    assert result.estimated_cost == Decimal("2.60")
    assert result.net_pnl == Decimal("47.40")
    assert result.net_return == Decimal("0.158")
    assert result.availability_flag == "RESEARCH_DELAYED_PROXY"
    assert "QUOTE_LIQUIDITY_NOT_AVAILABLE" in result.quality_flags


def test_long_option_proxy_return_uses_capital_at_risk():
    batch_id = uuid4()
    result = evaluate_delayed_proxy_outcome(
        candidate_id=uuid4(), event_id=None, measurement_type="60MIN",
        market_time=datetime(2026, 8, 31, 20, 30, tzinfo=UTC),
        observed_time=datetime(2026, 8, 31, 20, 45, tzinfo=UTC),
        capital_at_risk=Decimal("500"),
        legs=(leg(OptionSide.BUY, "5", "6", batch_id=batch_id),),
        policy=delayed_proxy_commission_policy(),
    )

    assert result.entry_net_premium == Decimal("-500")
    assert result.exit_net_premium == Decimal("-600")
    assert result.gross_pnl == Decimal("100")
    assert result.net_pnl == Decimal("98.70")
    assert result.net_return == Decimal("0.1974")


def test_proxy_outcome_requires_one_coherent_batch():
    with pytest.raises(ValueError, match="coherent source batch"):
        evaluate_delayed_proxy_outcome(
            candidate_id=uuid4(), event_id=None, measurement_type="15MIN",
            market_time=datetime(2026, 8, 31, 20, 30, tzinfo=UTC),
            observed_time=datetime(2026, 8, 31, 20, 45, tzinfo=UTC),
            capital_at_risk=Decimal("100"),
            legs=(
                leg(OptionSide.BUY, "1", "1.1"),
                leg(OptionSide.SELL, "2", "1.9"),
            ),
            policy=delayed_proxy_commission_policy(),
        )


def test_proxy_policy_is_deterministic_and_commission_only():
    first = delayed_proxy_commission_policy()
    repeated = delayed_proxy_commission_policy()

    assert first == repeated
    assert first.commission_per_contract_per_side == Decimal("0.65")
    assert len(first.policy_sha256) == 64


def test_option_outcome_repository_persists_policy_aware_package_return():
    cursor = MagicMock()
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    batch_id = uuid4()
    outcome = evaluate_delayed_proxy_outcome(
        candidate_id=uuid4(), event_id=uuid4(), measurement_type="30MIN",
        market_time=datetime(2026, 8, 31, 20, 30, tzinfo=UTC),
        observed_time=datetime(2026, 8, 31, 20, 45, tzinfo=UTC),
        capital_at_risk=Decimal("300"),
        legs=(
            leg(OptionSide.SELL, "3", "2", batch_id=batch_id),
            leg(OptionSide.BUY, "1", "0.5", batch_id=batch_id),
        ),
        policy=delayed_proxy_commission_policy(),
    )
    repository = OptionOutcomeRepository(factory)

    with patch(
        "options.repositories.outcomes.execute_values",
        return_value=[{"outcome_id": outcome.outcome_id}],
    ) as execute_values:
        assert repository.persist_decay_outcomes((outcome,)) == 1

    sql = " ".join(execute_values.call_args.args[1].split())
    values = execute_values.call_args.args[2][0]
    assert "valuation_policy_sha256" in sql
    assert "source_snapshot_ids" in sql
    assert "candidate_id, measurement_type, valuation_policy_sha256" in sql
    assert values[14:18] == (
        outcome.net_pnl,
        outcome.capital_at_risk,
        outcome.valuation_policy_version,
        outcome.valuation_policy_sha256,
    )


def test_option_outcome_migration_adds_proxy_provenance_contract():
    schema = (
        Path(__file__).resolve().parents[2]
        / "migrations"
        / "000_canonical_schema.sql"
    ).read_text(encoding="utf-8")

    assert "entry_net_premium" in schema
    assert "capital_at_risk > (0)::numeric" in schema
    assert "RESEARCH_DELAYED_PROXY" in schema
    assert "uq_option_decay_candidate_measurement_policy" in schema
    assert "valuation_policy_sha256 ~ '^[0-9a-f]{64}$'" in schema


def test_pending_option_candidates_are_bounded_to_selected_contract_packages():
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    connection = MagicMock(closed=False)
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    repository = OptionOutcomeRepository(factory)
    available_by = datetime(2026, 9, 1, tzinfo=UTC)

    assert repository.list_pending_candidates(
        valuation_policy_sha256="a" * 64,
        available_by=available_by,
    ) == ()

    sql, parameters = cursor.execute.call_args.args
    compact = " ".join(sql.split())
    assert "candidate.candidate_kind IN ('SINGLE_CONTRACT', 'MULTI_LEG')" in compact
    assert "candidate.capital_at_risk > 0" in compact
    assert "outcome.valuation_policy_sha256 = %s" in compact
    assert "HAVING COUNT(DISTINCT outcome.measurement_type) < 5" in compact
    assert parameters == ("a" * 64, available_by, available_by, 60, 1000)


def test_retained_leg_bounds_cover_selected_unexpired_contract_packages():
    cursor = MagicMock()
    cursor.fetchone.return_value = {
        "minimum_strike": Decimal("75"),
        "maximum_strike": Decimal("130"),
        "expiration_through": datetime(2026, 10, 2, tzinfo=UTC).date(),
        "contract_count": 4,
    }
    connection = MagicMock(closed=False)
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    repository = OptionOutcomeRepository(factory)
    available_by = datetime(2026, 9, 1, 20, 15, tzinfo=UTC)

    result = repository.retained_leg_bounds("spy", available_by=available_by)

    assert result["contract_count"] == 4
    sql, parameters = cursor.execute.call_args.args
    compact = " ".join(sql.split())
    assert "candidate.status = 'SELECTED'" in compact
    assert "candidate.observed_time <= %s" in compact
    assert "leg.expiration_date >=" in compact
    assert "AT TIME ZONE 'America/New_York'" in compact
    assert parameters == (
        "SPY", available_by, available_by, available_by, 60, available_by,
    )


def test_checkpoint_legs_require_one_complete_causal_snapshot_batch():
    batch_id = uuid4()
    snapshot_ids = (uuid4(), uuid4())
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        {
            "contract_id": 41,
            "side": "SELL",
            "ratio": 1,
            "multiplier": 100,
            "entry_mark": Decimal("3"),
            "exit_mark": Decimal("2"),
            "snapshot_id": snapshot_ids[0],
            "batch_id": batch_id,
            "mark_market_data_time": datetime(2026, 8, 31, 19, 30, tzinfo=UTC),
            "first_observed_at": datetime(2026, 8, 31, 20, 0, tzinfo=UTC),
        },
        {
            "contract_id": 42,
            "side": "BUY",
            "ratio": 1,
            "multiplier": 100,
            "entry_mark": Decimal("1"),
            "exit_mark": Decimal("0.5"),
            "snapshot_id": snapshot_ids[1],
            "batch_id": batch_id,
            "mark_market_data_time": datetime(2026, 8, 31, 19, 30, tzinfo=UTC),
            "first_observed_at": datetime(2026, 8, 31, 20, 0, tzinfo=UTC),
        },
    ]
    connection = MagicMock(closed=False)
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    repository = OptionOutcomeRepository(factory)
    checkpoint = datetime(2026, 8, 31, 19, 30, tzinfo=UTC)
    available_by = datetime(2026, 8, 31, 20, 0, tzinfo=UTC)

    result = repository.checkpoint_legs(
        uuid4(), checkpoint_time=checkpoint, available_by=available_by
    )

    assert len(result) == 2
    assert {row.source_batch_id for row in result} == {batch_id}
    compact = " ".join(cursor.execute.call_args.args[0].split())
    assert "HAVING COUNT(DISTINCT snapshot.contract_id)" in compact
    assert "snapshot.mark_market_data_time >= %s" in compact
    assert "snapshot.first_observed_at <= %s" in compact
    assert "JOIN eligible_batches AS batch USING (batch_id)" in compact


def test_measurement_checkpoints_use_exchange_close_and_next_open():
    result = measurement_checkpoints(
        datetime(2026, 8, 31, 19, 0, tzinfo=UTC)
    )

    assert result["15MIN"] == datetime(2026, 8, 31, 19, 15, tzinfo=UTC)
    assert result["60MIN"] == datetime(2026, 8, 31, 20, 0, tzinfo=UTC)
    assert result["CLOSE"] == datetime(2026, 8, 31, 20, 0, tzinfo=UTC)
    assert result["NEXT_OPEN"] == datetime(2026, 9, 1, 13, 30, tzinfo=UTC)


def test_close_checkpoint_is_omitted_for_close_time_candidate():
    result = measurement_checkpoints(
        datetime(2026, 8, 31, 20, 0, tzinfo=UTC)
    )

    assert "CLOSE" not in result


def test_option_outcome_service_matures_available_uncompleted_measurements():
    candidate_id = uuid4()
    batch_id = uuid4()

    class Repository:
        def __init__(self):
            self.persisted = []

        def list_pending_candidates(self, **kwargs):
            return ({
                "candidate_id": candidate_id,
                "event_id": uuid4(),
                "market_data_time": datetime(2026, 8, 31, 19, 0, tzinfo=UTC),
                "capital_at_risk": Decimal("500"),
                "completed_measurements": ("15MIN",),
            },)

        def checkpoint_legs(self, candidate_id, *, checkpoint_time, available_by):
            return (OptionOutcomeLeg(
                contract_id=42, side=OptionSide.BUY, ratio=1, multiplier=100,
                entry_mark=Decimal("5"), exit_mark=Decimal("6"),
                source_snapshot_id=uuid4(), source_batch_id=batch_id,
                source_market_time=checkpoint_time,
                source_observed_time=checkpoint_time + timedelta(minutes=15),
            ),)

        def persist_decay_outcomes(self, outcomes):
            self.persisted.extend(outcomes)
            return len(outcomes)

    repository = Repository()
    service = OptionOutcomeService(repository)

    result = service.mature(
        available_by=datetime(2026, 8, 31, 20, 15, tzinfo=UTC)
    )

    assert result.candidates == 1
    assert result.due_measurements == 3
    assert result.available_measurements == 3
    assert result.persisted == 3
    assert result.pending == 0
    assert {row.measurement_type for row in repository.persisted} == {
        "30MIN", "60MIN", "CLOSE",
    }


def test_option_outcome_service_keeps_missing_checkpoint_pending():
    class Repository:
        def list_pending_candidates(self, **kwargs):
            return ({
                "candidate_id": uuid4(), "event_id": None,
                "market_data_time": datetime(2026, 8, 31, 19, 0, tzinfo=UTC),
                "capital_at_risk": Decimal("500"),
                "completed_measurements": (),
            },)

        def checkpoint_legs(self, *args, **kwargs):
            return ()

        def persist_decay_outcomes(self, outcomes):
            assert outcomes == []
            return 0

    result = OptionOutcomeService(Repository()).mature(
        available_by=datetime(2026, 8, 31, 19, 20, tzinfo=UTC)
    )

    assert result.due_measurements == 1
    assert result.available_measurements == 0
    assert result.pending == 1