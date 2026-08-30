from contextlib import contextmanager
from datetime import timedelta
from decimal import Decimal
from uuid import NAMESPACE_URL, uuid5

from psycopg2.extras import RealDictCursor

from database import get_db_connection
from options.config import load_option_runtime_configuration
from options.domain import ContractType
from options.repositories.strategies import OptionStrategyRepository
from options.strategies.domain import (
    CandidateKind,
    CandidateLeg,
    CandidateStatus,
    OptionCandidate,
    OptionSide,
    StrategyContextSnapshot,
    StrategyContextStatus,
    StructureRiskClass,
    StructureType,
    candidate_identity,
)
from options.strategies.engine import StrategyScanResult
from options.strategies.payoff import evaluate_terminal_payoff
from options.strategies.scenarios import build_scenario_grid


class NoCommitConnection:
    def __init__(self, connection):
        self.connection = connection

    @property
    def closed(self):
        return self.connection.closed

    def cursor(self, *args, **kwargs):
        return self.connection.cursor(*args, **kwargs)

    def commit(self):
        pass

    def rollback(self):
        self.connection.rollback()


def test_selected_candidate_graph_persists_atomically_and_rolls_back():
    configuration = load_option_runtime_configuration()
    with get_db_connection() as connection:
        connection.rollback()
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(
            """
            SELECT run.matrix_id, run.market_time, run.observed_time,
                   snapshot.snapshot_id, snapshot.contract_id,
                   snapshot.contract_ticker, snapshot.expiration_date,
                   snapshot.strike, snapshot.spot,
                   snapshot.time_to_expiration_years
            FROM option_analysis_runs AS run
            JOIN option_chain_snapshots AS snapshot USING (batch_id)
            WHERE run.underlying = 'SPY'
            ORDER BY run.market_time DESC, snapshot.contract_id
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        assert row is not None
        context_id = uuid5(
            NAMESPACE_URL,
            f"option-context:{row['matrix_id']}:{configuration.strategy_policy_sha256}",
        )
        context = StrategyContextSnapshot(
            context_id,
            row["matrix_id"],
            "SPY",
            row["market_time"],
            row["observed_time"],
            StrategyContextStatus.DEGRADED,
            Decimal("100"),
            Decimal("99"),
            100,
            Decimal("100"),
            Decimal("99"),
            20,
            "BULLISH",
            "NOT_APPLICABLE",
            "UNAVAILABLE",
            "NOT_AVAILABLE",
            ("FED_CALENDAR_UNAVAILABLE",),
            (),
            configuration.strategy_policy.strategy_version,
            configuration.strategy_policy_sha256,
        )
        leg = CandidateLeg(
            0,
            row["snapshot_id"],
            row["contract_id"],
            row["contract_ticker"],
            OptionSide.BUY,
            1,
            100,
            row["expiration_date"],
            row["strike"],
            ContractType.CALL,
            row["spot"],
            row["time_to_expiration_years"],
            0.04,
            0.0,
            Decimal("2"),
            0.25,
            0.50,
            0.02,
            -0.05,
            0.10,
            0.02,
            row["market_time"],
            "DEVELOPER_ALIGNED_AGG_CLOSE",
            "black_scholes_european_v1",
            (),
        )
        candidate_id, identity = candidate_identity(
            row["matrix_id"],
            "INCOME_WHEEL",
            configuration.strategy_policy.strategy_version,
            StructureType.LONG_CALL,
            (leg.contract_id,),
            "ROLLBACK_TEST",
        )
        payoff = evaluate_terminal_payoff((leg,))
        candidate = OptionCandidate(
            candidate_id,
            identity,
            row["matrix_id"],
            "INCOME_WHEEL",
            configuration.strategy_policy.strategy_version,
            "SPY",
            CandidateKind.SINGLE_CONTRACT,
            "INCOME_GENERATION",
            ("INCOME",),
            StructureType.LONG_CALL,
            StructureRiskClass.PREMIUM_AT_RISK_DEBIT,
            row["expiration_date"],
            99,
            CandidateStatus.SELECTED,
            "test_metric",
            1.0,
            {"test_metric": 1.0},
            {"test": True},
            (leg,),
            payoff.net_premium,
            None,
            payoff.maximum_loss,
            payoff.maximum_profit,
            payoff.maximum_loss,
            None,
            None,
            payoff.breakevens,
            None,
            ("PAPER_RISK_ENGINE_NOT_IMPLEMENTED",),
            None,
            {},
            configuration.strategy_policy_sha256,
            leg.model_version,
            context.context_snapshot_id,
            None,
            context.market_data_time,
            context.observed_time,
            context.market_data_time + timedelta(minutes=15),
        )
        result = StrategyScanResult(
            (candidate,),
            build_scenario_grid(candidate, configuration.strategy_policy.scenarios),
        )

        @contextmanager
        def factory():
            yield NoCommitConnection(connection)

        repository = OptionStrategyRepository(factory)
        assert repository.persist(context, result) == 1
        cursor.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM option_strategy_candidates WHERE candidate_id = %s) AS candidates,
                (SELECT COUNT(*) FROM option_candidate_legs WHERE candidate_id = %s) AS legs,
                (SELECT COUNT(*) FROM option_scenario_results WHERE candidate_id = %s) AS scenarios,
                (SELECT COUNT(*) FROM option_signal_events WHERE source_candidate_id = %s) AS signals,
                (SELECT COUNT(*) FROM option_signal_occurrences AS occurrence
                    JOIN option_signal_events AS signal USING (event_id)
                    WHERE signal.source_candidate_id = %s) AS occurrences
            """,
            (candidate_id, candidate_id, candidate_id, candidate_id, candidate_id),
        )
        counts = cursor.fetchone()
        assert dict(counts) == {
            "candidates": 1,
            "legs": 1,
            "scenarios": 35,
            "signals": 1,
            "occurrences": 1,
        }
        connection.rollback()