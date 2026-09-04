import inspect
import sys
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.domain import (
    AssetType,
    DecisionContext,
    OptionUniverseCandidate,
    OptionUniverseMode,
    UniverseRunStatus,
)
from options.repositories.universe import OptionUniverseRepository


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

    return OptionUniverseRepository(factory), connection, cursor


def test_ranked_run_cannot_use_same_session_data():
    repository, _, _ = _repository()
    session = date(2026, 8, 28)
    now = datetime(2026, 8, 28, 20, 30, tzinfo=UTC)

    with pytest.raises(ValueError, match="after as_of_session"):
        repository.create_run(
            uuid4(),
            OptionUniverseMode.RANKED,
            session,
            session,
            "{}",
            "a" * 64,
            now,
            now,
        )


def test_candidate_persistence_keeps_raw_and_component_metrics():
    repository, connection, cursor = _repository()
    cursor.rowcount = 1
    candidate = OptionUniverseCandidate(
        run_id=uuid4(),
        ticker="AAPL",
        asset_type=AssetType.STOCK,
        raw_metrics_json='{"option_adv":1000}',
        component_ranks_json='{"option_adv":1}',
        total_score=0.95,
        eligible=True,
        exclusion_reasons=(),
        candidate_rank=1,
        first_observed_at=datetime(2026, 8, 28, 20, 30, tzinfo=UTC),
    )

    with patch("options.repositories.universe.execute_values") as bulk_insert:
        assert repository.persist_candidates([candidate]) == 1

    sql = bulk_insert.call_args.args[1]
    assert "raw_metrics, component_ranks" in sql
    assert "ON CONFLICT (run_id, ticker) DO NOTHING" in sql
    connection.commit.assert_called_once_with()


def test_active_member_read_requires_context_and_filters_observation_time():
    repository, _, cursor = _repository()
    cursor.fetchall.return_value = []
    now = datetime(2026, 8, 29, 14, 0, tzinfo=UTC)
    context = DecisionContext(market_time=now, observed_time=now)

    assert repository.list_active(date(2026, 8, 29), context) == ()

    sql, parameters = cursor.execute.call_args.args
    assert "member.first_observed_at <= %s" in sql
    assert "run.first_observed_at <= %s" in sql
    assert parameters[-1] == context.observed_time
    signature = inspect.signature(repository.list_active)
    assert signature.parameters["context"].default is inspect.Parameter.empty


def test_complete_run_allows_only_monotonic_terminal_upgrade():
    repository, connection, cursor = _repository()
    cursor.rowcount = 1
    run_id = uuid4()
    completed_at = datetime(2026, 8, 31, 20, 15, tzinfo=UTC)

    repository.complete_run(
        run_id,
        UniverseRunStatus.COMPLETE,
        1.0,
        completed_at,
    )

    sql, parameters = cursor.execute.call_args.args
    assert "status = 'DEGRADED'" in sql
    assert "completeness_fraction < %s" in sql
    assert parameters == (
        "COMPLETE", 1.0, completed_at, run_id, "COMPLETE", 1.0,
    )
    connection.commit.assert_called_once_with()