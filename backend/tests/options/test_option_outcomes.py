from contextlib import contextmanager
import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from options.outcomes import (
    OptionOutcomeLeg,
    delayed_proxy_commission_policy,
    evaluate_delayed_proxy_outcome,
    measurement_checkpoints,
)
from options.outcome_service import OptionOutcomeService
from options.domain import MarkSource
from options.repositories.outcomes import OptionOutcomeRepository
from options.strategies.domain import OptionSide
from unittest.mock import MagicMock, patch


UTC = timezone.utc


def leg(side, entry, exit_, *, ratio=1, batch_id=None):
    market_time = datetime(2026, 8, 31, 20, 30, tzinfo=UTC)
    policy = delayed_proxy_commission_policy()
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
        entry_mark_source=MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE,
        exit_mark_source=MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE,
        entry_valuation_policy_sha256=policy.policy_sha256,
        exit_valuation_policy_sha256=policy.policy_sha256,
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


def test_long_option_price_movement_is_contract_multiplier_times_mark_change():
    batch_id = uuid4()
    result = evaluate_delayed_proxy_outcome(
        candidate_id=uuid4(), event_id=None, measurement_type="60MIN",
        market_time=datetime(2026, 8, 31, 20, 30, tzinfo=UTC),
        observed_time=datetime(2026, 8, 31, 20, 45, tzinfo=UTC),
        capital_at_risk=Decimal("350"),
        legs=(leg(OptionSide.BUY, "3.50", "3.20", batch_id=batch_id),),
        policy=delayed_proxy_commission_policy(),
    )

    assert result.gross_pnl == Decimal("-30.00")
    assert result.estimated_cost == Decimal("1.30")
    assert result.net_pnl == Decimal("-31.30")


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


def test_proxy_outcome_rejects_mixed_valuation_policy_hashes():
    policy = delayed_proxy_commission_policy()
    invalid_leg = replace(
        leg(OptionSide.BUY, "5", "6"),
        exit_valuation_policy_sha256="b" * 64,
    )

    with pytest.raises(ValueError, match="must match the valuation policy"):
        evaluate_delayed_proxy_outcome(
            candidate_id=uuid4(), event_id=None, measurement_type="60MIN",
            market_time=datetime(2026, 8, 31, 20, 30, tzinfo=UTC),
            observed_time=datetime(2026, 8, 31, 20, 45, tzinfo=UTC),
            capital_at_risk=Decimal("500"), legs=(invalid_leg,), policy=policy,
        )


def test_proxy_outcome_rejects_disallowed_mark_source():
    policy = delayed_proxy_commission_policy()
    invalid_leg = replace(
        leg(OptionSide.BUY, "5", "6"),
        exit_mark_source=MarkSource.ADVANCED_NBBO_MIDPOINT,
    )

    with pytest.raises(ValueError, match="exit mark source"):
        evaluate_delayed_proxy_outcome(
            candidate_id=uuid4(), event_id=None, measurement_type="60MIN",
            market_time=datetime(2026, 8, 31, 20, 30, tzinfo=UTC),
            observed_time=datetime(2026, 8, 31, 20, 45, tzinfo=UTC),
            capital_at_risk=Decimal("500"), legs=(invalid_leg,), policy=policy,
        )


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
    assert values[6] == outcome.exit_net_premium
    assert values[7:12] == (
        outcome.net_return,
        outcome.availability_flag,
        list(outcome.quality_flags),
        outcome.entry_net_premium,
        outcome.exit_net_premium,
    )
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
    assert "candidate.candidate_identity" in compact
    assert "ORDER BY required_leg.leg_index" in compact
    assert "LEFT JOIN option_outcome_unavailable_evidence AS unavailable" in compact
    assert "COUNT(DISTINCT unavailable.measurement_type)" in compact
    assert "snapshot.mark_market_data_time >= candidate.market_data_time" in compact
    assert "snapshot.underlying = candidate.underlying" in compact
    assert "snapshot.market_data_time >= candidate.market_data_time" in compact
    assert (
        "HAVING %s OR COUNT(DISTINCT outcome.measurement_type) + "
        "COUNT(DISTINCT unavailable.measurement_type) < 5"
    ) in compact
    assert (
        "CASE WHEN NOT %s THEN COUNT(DISTINCT outcome.measurement_type) + "
        "COUNT(DISTINCT unavailable.measurement_type) END"
    ) in compact
    assert "entry_leg.valuation_policy_sha256 IS DISTINCT FROM %s" in compact
    assert parameters[0].adapted == []
    assert parameters[1:] == (
        "a" * 64, "a" * 64, None, None, available_by, available_by, 60,
        "a" * 64, False, available_by, "a" * 64, "a" * 64, None,
        False, "a" * 64, available_by, False, False, False, False, 1000,
    )


@pytest.mark.parametrize(("as_of", "session_date", "close_at", "next_open"), [
    (datetime(2026, 9, 7, 18, tzinfo=UTC), "2026-09-04",
     "2026-09-04T20:00:00+00:00", "2026-09-08T13:30:00+00:00"),
    (datetime(2026, 11, 27, 20, tzinfo=UTC), "2026-11-27",
     "2026-11-27T18:00:00+00:00", "2026-11-30T14:30:00+00:00"),
])
def test_terminal_queue_filters_due_checkpoints_before_oldest_first_limit(
    as_of, session_date, close_at, next_open,
):
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    connection = MagicMock(closed=False)
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    repository = OptionOutcomeRepository(factory)
    repository.list_pending_candidates(
        valuation_policy_sha256="a" * 64, available_by=as_of, limit=200,
        include_incomplete_packages=True, availability_policy_sha256="b" * 64,
    )

    sql, parameters = cursor.execute.call_args.args
    compact = " ".join(sql.split())
    assert sql.count("%s") == len(parameters)
    clocks = parameters[0].adapted
    assert len(clocks) <= 61
    assert next(row for row in clocks if row["session_date"] == session_date) == {
        "session_date": session_date, "session_close": close_at, "next_open": next_open,
    }
    assert "due.checkpoint_at <= %s" in compact
    assert "completed.measurement_type = due.measurement_type" in compact
    assert "terminal.measurement_type = due.measurement_type" in compact
    assert "terminal.availability_policy_sha256 = %s" in compact
    assert "session.session_close > candidate.market_data_time" in compact
    assert "ORDER BY CASE WHEN %s THEN candidate.market_data_time END ASC" in compact
    assert parameters[-5:] == (True, True, True, True, 200)
    assert parameters[3:5] == ("b" * 64, "b" * 64)


@pytest.mark.skipif(os.getenv("OPTION_OUTCOME_READONLY_TESTS") != "1",
                    reason="explicit read-only PostgreSQL planning check required")
@pytest.mark.parametrize("terminal_enabled", [False, True])
@pytest.mark.parametrize("execute_bounded", [False, True, "configured"])
def test_pending_outcome_query_plans_in_read_only_transaction(terminal_enabled, execute_bounded):
    from database import get_db_connection
    from options.outcome_contracts import OptionOutcomeAvailabilityPolicy

    if execute_bounded == "configured" and not terminal_enabled:
        pytest.skip("configured-window acceptance is scoped to the enabled WP6 path")
    row_limit = 200 if execute_bounded == "configured" or not execute_bounded else 20
    retention_days = 60 if execute_bounded == "configured" or not execute_bounded else 1

    captured = MagicMock()
    captured.fetchall.return_value = []
    mocked_connection = MagicMock(closed=False)
    mocked_connection.cursor.return_value = captured

    @contextmanager
    def factory():
        yield mocked_connection

    OptionOutcomeRepository(factory).list_pending_candidates(
        valuation_policy_sha256=delayed_proxy_commission_policy().policy_sha256,
        available_by=datetime.now(UTC), limit=row_limit,
        retention_days=retention_days,
        include_incomplete_packages=terminal_enabled,
        availability_policy_sha256=OptionOutcomeAvailabilityPolicy().sha256 if terminal_enabled else None,
    )
    sql, parameters = captured.execute.call_args.args
    with get_db_connection() as connection:
        connection.rollback()
        connection.set_session(readonly=True)
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL statement_timeout = '5s'")
                cursor.execute("SHOW transaction_read_only")
                assert cursor.fetchone()[0] == "on"
                cursor.execute("EXPLAIN (FORMAT JSON) " + sql, parameters)
                plan = cursor.fetchone()[0][0]["Plan"]
                assert plan
                if execute_bounded:
                    nodes, pending = [], [plan]
                    while pending:
                        node = pending.pop()
                        nodes.append({key: node[key] for key in (
                            "Node Type", "Relation Name", "Index Name", "Total Cost", "Plan Rows"
                        ) if key in node})
                        pending.extend(node.get("Plans", ()))
                    print("bounded_queue_plan", terminal_enabled,
                          sorted(nodes, key=lambda node: node["Total Cost"], reverse=True)[:12])
                    cursor.execute(sql, parameters)
                    assert len(cursor.fetchall()) <= row_limit
        finally:
            connection.rollback()
            connection.set_session(readonly=False, isolation_level="READ COMMITTED")


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


def test_current_mark_legs_require_observation_after_detection():
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    connection = MagicMock(closed=False)
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    repository = OptionOutcomeRepository(factory)

    assert repository.current_mark_legs(
        uuid4(), available_by=datetime(2026, 9, 4, 20, 0, tzinfo=UTC),
        valuation_policy_sha256="a" * 64,
    ) == ()

    compact = " ".join(cursor.execute.call_args.args[0].split())
    assert "snapshot.mark_market_data_time > leg.entry_market_time" in compact
    assert "HAVING COUNT(DISTINCT snapshot.contract_id)" in compact


def test_current_mark_candidates_prioritize_unmarked_packages():
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    connection = MagicMock(closed=False)
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    repository = OptionOutcomeRepository(factory)
    available_by = datetime(2026, 9, 4, 20, 0, tzinfo=UTC)

    assert repository.list_current_candidates(
        valuation_policy_sha256="a" * 64,
        available_by=available_by,
    ) == ()

    sql, parameters = cursor.execute.call_args.args
    compact = " ".join(sql.split())
    assert "LEFT JOIN option_signal_current_marks AS current_mark" in compact
    assert "snapshot.mark_market_data_time > candidate.market_data_time" in compact
    assert "HAVING COUNT(DISTINCT snapshot.contract_id)" in compact
    assert "PARTITION BY current_mark.candidate_id IS NULL" in compact
    assert "THEN candidate.market_data_time END DESC" in compact
    assert "current_mark.market_time NULLS FIRST" in compact
    assert "WHERE queue_rank <= %s" in compact
    assert "entry_leg.valuation_policy_sha256 IS DISTINCT FROM %s" in compact
    assert "snapshot.valuation_policy_sha256 = %s" in compact
    assert parameters == (
        "a" * 64, available_by, available_by, 60,
        "a" * 64, available_by, "a" * 64, available_by, 1000,
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
            "entry_mark_source": "DEVELOPER_ALIGNED_AGG_CLOSE",
            "exit_mark_source": "DEVELOPER_ALIGNED_AGG_CLOSE",
            "entry_valuation_policy_sha256": "a" * 64,
            "exit_valuation_policy_sha256": "a" * 64,
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
            "entry_mark_source": "DEVELOPER_ALIGNED_AGG_CLOSE",
            "exit_mark_source": "DEVELOPER_ALIGNED_AGG_CLOSE",
            "entry_valuation_policy_sha256": "a" * 64,
            "exit_valuation_policy_sha256": "a" * 64,
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
        uuid4(), checkpoint_time=checkpoint, available_by=available_by,
        valuation_policy_sha256="a" * 64,
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
                "candidate_identity": "a" * 64,
                "event_id": uuid4(),
                "market_data_time": datetime(2026, 8, 31, 19, 0, tzinfo=UTC),
                "capital_at_risk": Decimal("500"),
                "required_contract_ids": (42,),
                "completed_measurements": ("15MIN",),
                "unavailable_measurements": (),
            },)

        def checkpoint_legs(
            self, candidate_id, *, checkpoint_time, available_by,
            valuation_policy_sha256,
        ):
            policy = delayed_proxy_commission_policy()
            assert valuation_policy_sha256 == policy.policy_sha256
            return (OptionOutcomeLeg(
                contract_id=42, side=OptionSide.BUY, ratio=1, multiplier=100,
                entry_mark=Decimal("5"), exit_mark=Decimal("6"),
                source_snapshot_id=uuid4(), source_batch_id=batch_id,
                source_market_time=checkpoint_time,
                source_observed_time=checkpoint_time + timedelta(minutes=15),
                entry_mark_source=MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE,
                exit_mark_source=MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE,
                entry_valuation_policy_sha256=policy.policy_sha256,
                exit_valuation_policy_sha256=policy.policy_sha256,
            ),)

        def persist_decay_outcomes(self, outcomes):
            self.persisted.extend(outcomes)
            return len(outcomes)

        def current_marks_available(self):
            return False

        def delete_non_causal_current_marks(self):
            return 0

        def list_current_candidates(self, **kwargs):
            return ()

        def current_mark_legs(self, *args, **kwargs):
            return ()

        def persist_current_marks(self, outcomes):
            assert outcomes == []
            return 0

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
            assert kwargs["limit"] == 1000
            return ({
                "candidate_id": uuid4(), "event_id": None,
                "candidate_identity": "a" * 64,
                "market_data_time": datetime(2026, 8, 31, 19, 0, tzinfo=UTC),
                "capital_at_risk": Decimal("500"),
                "required_contract_ids": (42,),
                "completed_measurements": (),
                "unavailable_measurements": (),
            },)

        def checkpoint_legs(self, *args, **kwargs):
            return ()

        def persist_decay_outcomes(self, outcomes):
            assert outcomes == []
            return 0

        def current_marks_available(self):
            return False

        def delete_non_causal_current_marks(self):
            return 0

        def list_current_candidates(self, **kwargs):
            return ()

        def current_mark_legs(self, *args, **kwargs):
            return ()

        def persist_current_marks(self, outcomes):
            assert outcomes == []
            return 0

    result = OptionOutcomeService(Repository()).mature(
        available_by=datetime(2026, 8, 31, 19, 20, tzinfo=UTC)
    )

    assert result.due_measurements == 1
    assert result.available_measurements == 0
    assert result.pending == 1


def test_option_outcome_service_persists_only_deadline_expired_missing_package():
    candidate_id = uuid4()

    class Repository:
        def list_pending_candidates(self, **kwargs):
            assert kwargs["limit"] == 200
            return ({
                "candidate_id": candidate_id, "candidate_identity": "a" * 64,
                "event_id": None,
                "market_data_time": datetime(2026, 8, 31, 19, 0, tzinfo=UTC),
                "capital_at_risk": Decimal("500"),
                "required_contract_ids": (41, 42),
                "completed_measurements": (), "unavailable_measurements": (),
            },)

        def checkpoint_legs(self, *args, **kwargs):
            return ()

        def persist_decay_outcomes(self, outcomes):
            assert outcomes == []
            return 0

        def current_marks_available(self):
            return False

    class AvailabilityRepository:
        def persist_unavailable(self, assessments):
            self.assessments = tuple(assessments)
            return SimpleNamespace(inserted=len(self.assessments), existing=0)

    availability_repository = AvailabilityRepository()
    result = OptionOutcomeService(
        Repository(), availability_repository=availability_repository,
        availability_evidence_enabled=True,
    ).mature(available_by=datetime(2026, 8, 31, 20, 1, tzinfo=UTC))

    assert result.due_measurements == 4
    assert result.pending == 3
    assert result.unavailable_measurements == 1
    assert result.unavailable_persisted == 1
    assessment = availability_repository.assessments[0]
    assert assessment.measurement_type == "15MIN"
    assert assessment.status == "UNAVAILABLE"
    assert assessment.missing_contract_ids == (41, 42)


@pytest.mark.parametrize("receipt_offset", [-1, 0, 1])
@pytest.mark.parametrize("processing_delay", [1, 120])
def test_terminal_outcome_receipt_deadline_does_not_depend_on_poll_time(
    receipt_offset, processing_delay,
):
    from options.outcome_contracts import OptionOutcomeAvailabilityPolicy

    candidate_id = uuid4()
    market_time = datetime(2026, 8, 31, 19, tzinfo=UTC)
    checkpoint = market_time + timedelta(minutes=15)
    availability_policy = OptionOutcomeAvailabilityPolicy(maximum_mark_lag_seconds=600)
    deadline = availability_policy.deadline(checkpoint)
    valuation_policy = delayed_proxy_commission_policy()
    observed_at = deadline + timedelta(seconds=receipt_offset)
    outcome_leg = OptionOutcomeLeg(
        contract_id=42, side=OptionSide.BUY, ratio=1, multiplier=100,
        entry_mark=Decimal("5"), exit_mark=Decimal("6"),
        source_snapshot_id=uuid4(), source_batch_id=uuid4(),
        source_market_time=checkpoint, source_observed_time=observed_at,
        entry_mark_source=MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE,
        exit_mark_source=MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE,
        entry_valuation_policy_sha256=valuation_policy.policy_sha256,
        exit_valuation_policy_sha256=valuation_policy.policy_sha256,
    )

    class Repository:
        def list_pending_candidates(self, **kwargs):
            return ({
                "candidate_id": candidate_id, "candidate_identity": "a" * 64,
                "event_id": None, "market_data_time": market_time,
                "capital_at_risk": Decimal("500"), "required_contract_ids": (42,),
                "completed_measurements": ("30MIN", "60MIN", "CLOSE", "NEXT_OPEN"),
                "unavailable_measurements": (),
            },)

        def checkpoint_legs(self, *args, **kwargs):
            assert kwargs["available_by"] == deadline
            assert kwargs["maximum_mark_lag"] == timedelta(seconds=600)
            return (outcome_leg,) if observed_at <= kwargs["available_by"] else ()

        def persist_decay_outcomes(self, outcomes):
            self.outcomes = tuple(outcomes)
            return len(outcomes)

        def current_marks_available(self):
            return False

    repository = Repository()
    unavailable_repository = MagicMock()
    unavailable_repository.persist_unavailable.return_value = SimpleNamespace(inserted=1)
    result = OptionOutcomeService(
        repository, policy=valuation_policy, availability_policy=availability_policy,
        availability_repository=unavailable_repository, availability_evidence_enabled=True,
    ).mature(available_by=deadline + timedelta(minutes=processing_delay))

    assert result.available_measurements == (1 if receipt_offset <= 0 else 0)
    assert result.unavailable_measurements == (1 if receipt_offset > 0 else 0)
    if receipt_offset > 0:
        assessment = unavailable_repository.persist_unavailable.call_args.args[0][0]
        assert assessment.evaluated_at == deadline
        assert assessment.missing_contract_ids == (42,)
        assert repository.outcomes == ()
    else:
        unavailable_repository.persist_unavailable.assert_not_called()
        assert repository.outcomes[0].observed_time == observed_at


def test_option_outcome_service_persists_latest_coherent_current_mark():
    candidate_id = uuid4()
    batch_id = uuid4()

    class Repository:
        def list_pending_candidates(self, **kwargs):
            return ()

        def persist_decay_outcomes(self, outcomes):
            assert outcomes == []
            return 0

        def current_marks_available(self):
            return True

        def delete_non_causal_current_marks(self):
            return 0

        def list_current_candidates(self, **kwargs):
            return ({
                "candidate_id": candidate_id,
                "event_id": uuid4(),
                "market_data_time": datetime(2026, 8, 31, 19, 0, tzinfo=UTC),
                "capital_at_risk": Decimal("500"),
            },)

        def current_mark_legs(
            self, actual_candidate_id, *, available_by,
            valuation_policy_sha256,
        ):
            assert actual_candidate_id == candidate_id
            policy = delayed_proxy_commission_policy()
            assert valuation_policy_sha256 == policy.policy_sha256
            return (OptionOutcomeLeg(
                contract_id=42, side=OptionSide.BUY, ratio=1, multiplier=100,
                entry_mark=Decimal("5"), exit_mark=Decimal("6"),
                source_snapshot_id=uuid4(), source_batch_id=batch_id,
                source_market_time=available_by - timedelta(minutes=15),
                source_observed_time=available_by,
                entry_mark_source=MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE,
                exit_mark_source=MarkSource.DEVELOPER_ALIGNED_AGG_CLOSE,
                entry_valuation_policy_sha256=policy.policy_sha256,
                exit_valuation_policy_sha256=policy.policy_sha256,
            ),)

        def persist_current_marks(self, outcomes):
            self.outcomes = tuple(outcomes)
            return len(self.outcomes)

    repository = Repository()
    result = OptionOutcomeService(repository).mature(
        available_by=datetime(2026, 8, 31, 20, 15, tzinfo=UTC)
    )

    assert result.current_candidates == 1
    assert result.current_persisted == 1
    assert repository.outcomes[0].measurement_type == "CURRENT"
    assert repository.outcomes[0].net_pnl == Decimal("98.70")