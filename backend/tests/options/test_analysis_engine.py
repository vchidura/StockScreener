import sys
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.analytics import OptionAnalysisEngine, UnderlyingMinuteBar
from options.config import load_option_runtime_configuration
from options.data.normalizer import (
    DeveloperNormalizationInput,
    DeveloperOptionNormalizer,
    parse_polygon_snapshot,
)
from options.domain import DecisionContext
from tests.options.test_normalizer import (
    MARKET_TIME,
    OBSERVED_AT,
    UTC,
    _catalog,
    _payload,
)
from decimal import Decimal
from datetime import datetime


def _snapshot():
    configuration = load_option_runtime_configuration(
        {"POLYGON_API_KEY": "test-secret"}, BACKEND_DIR
    )
    raw = parse_polygon_snapshot(_payload(), OBSERVED_AT)
    item = DeveloperNormalizationInput(
        raw,
        _catalog(),
        (UnderlyingMinuteBar(Decimal("100"), MARKET_TIME),),
        datetime(2026, 9, 4, 20, 0, tzinfo=UTC),
        0.04,
        0.0,
    )
    snapshot = DeveloperOptionNormalizer(configuration.policy).normalize(
        uuid4(), (item,)
    ).matrix_snapshots[0]
    return configuration, snapshot


def test_analysis_engine_is_pure_point_in_time_and_fails_incomplete_matrix_closed():
    configuration, snapshot = _snapshot()
    context = DecisionContext(MARKET_TIME, OBSERVED_AT)

    analysis = OptionAnalysisEngine(configuration.policy).analyze(
        uuid4(),
        (snapshot,),
        context,
        received_count=1,
        catalog_matched_count=1,
        unknown_reference_count=0,
        reference_drift_failed=False,
        batch_complete=False,
    )

    assert analysis.chain_health.status == "FAILED"
    assert "INCOMPLETE_MATRIX" in analysis.chain_health.reasons
    assert len(analysis.contracts) == 1
    assert analysis.underlying.total_day_volume == 20


def test_analysis_engine_rejects_future_visible_snapshot():
    configuration, snapshot = _snapshot()
    early_context = DecisionContext(
        MARKET_TIME,
        OBSERVED_AT - timedelta(seconds=1),
    )

    with pytest.raises(ValueError, match="not available"):
        OptionAnalysisEngine(configuration.policy).analyze(
            uuid4(),
            (snapshot,),
            early_context,
            received_count=1,
            catalog_matched_count=1,
            unknown_reference_count=0,
            reference_drift_failed=False,
            batch_complete=True,
        )