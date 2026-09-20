from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from options.repositories.stock_behavior_assessments import (
    OptionStockBehaviorAssessmentRepository,
    stock_behavior_assessment_id,
)
from options.stock_behavior_gates import evaluate_option_stock_behavior
from options.strategies.domain import StructureType


def test_stock_behavior_assessment_id_is_retry_stable_and_payload_sensitive():
    arguments = dict(
        snapshot=None,
        candidate_id=uuid4(),
        candidate_identity_sha256="a" * 64,
        matrix_id=uuid4(),
        underlyer="AAPL",
        strategy_name="DIRECTIONAL_LONG_PREMIUM",
        structure_type=StructureType.LONG_CALL,
        directional_thesis="BULLISH",
        option_strategy_version="strategy_v1",
        option_strategy_policy_sha256="b" * 64,
        option_configuration_sha256="c" * 64,
        option_market_policy_sha256="d" * 64,
        option_analysis_policy_sha256="d" * 64,
        option_market_time=datetime(2026, 9, 18, 20, 0, tzinfo=timezone.utc),
        option_observed_at=datetime(2026, 9, 18, 20, 5, tzinfo=timezone.utc),
        stock_market_cutoff=datetime(2026, 9, 18, 20, 0, tzinfo=timezone.utc),
        decision_at=datetime(2026, 9, 18, 20, 5, tzinfo=timezone.utc),
    )
    assessment = evaluate_option_stock_behavior(**arguments)

    assert stock_behavior_assessment_id(assessment) == stock_behavior_assessment_id(
        evaluate_option_stock_behavior(**arguments)
    )
    changed = evaluate_option_stock_behavior(
        **{**arguments, "unavailable_reason": "STOCK_BEHAVIOR_READ_FAILED"}
    )
    assert stock_behavior_assessment_id(assessment) != stock_behavior_assessment_id(changed)


def test_stock_behavior_status_rejects_unbounded_or_naive_windows():
    repository = OptionStockBehaviorAssessmentRepository(
        lambda: (_ for _ in ()).throw(AssertionError("invalid bounds cannot open a connection"))
    )
    now = datetime(2026, 9, 18, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="timezone-aware"):
        repository.status(now.replace(tzinfo=None), now)
    with pytest.raises(ValueError, match="31 days"):
        repository.status(now, now + timedelta(days=31, seconds=1))