import inspect
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.domain import AnalysisStatus, DecisionContext, OptionAnalysisRun
from options.repositories.analysis import OptionAnalysisRepository


def _run(status=AnalysisStatus.RUNNING, completed_at=None):
    now = datetime(2026, 8, 29, 20, 0, tzinfo=timezone.utc)
    return OptionAnalysisRun(
        matrix_id=uuid4(),
        batch_id=uuid4(),
        underlyer="SPY",
        context=DecisionContext(now, now),
        status=status,
        received_contract_count=100,
        eligible_contract_count=80,
        unknown_reference_count=0,
        iv_attempt_count=80,
        iv_converged_count=78,
        iv_convergence_fraction=0.975,
        quality_reasons=(),
        chain_health_json='{"status":"CLEAN"}',
        policy_version="developer_v1",
        policy_sha256="a" * 64,
        model_version="option_model_v1",
        started_at=now,
        completed_at=completed_at,
    )


def test_analysis_start_persists_decision_context_and_versions():
    cursor = MagicMock()
    cursor.closed = False
    run = _run()
    cursor.fetchone.return_value = {"matrix_id": run.matrix_id}
    connection = MagicMock()
    connection.closed = False
    connection.cursor.return_value = cursor

    @contextmanager
    def factory():
        yield connection

    repository = OptionAnalysisRepository(factory)
    assert repository.start(run) == run.matrix_id

    sql, parameters = cursor.execute.call_args.args
    assert "market_time, observed_time" in sql
    assert run.context.market_time in parameters
    assert run.context.observed_time in parameters
    connection.commit.assert_called_once_with()


def test_analysis_read_requires_context():
    signature = inspect.signature(OptionAnalysisRepository.get)
    assert signature.parameters["context"].default is inspect.Parameter.empty