from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import UUID

from realtime.screening_repository import (
    BarClocks,
    BarScope,
    select_source_bars,
    select_winning_bars,
)


NOW = datetime(2026, 10, 16, 16, 0, tzinfo=timezone.utc)
TARGET = UUID("11111111-1111-1111-1111-111111111111")
SOURCE = UUID("22222222-2222-2222-2222-222222222222")


def _scope():
    return BarScope(
        cohort_id=TARGET,
        provider_tier="MASSIVE_ADVANCED",
        tickers=("SPY",),
        include_portal_history=True,
        history_params=(
            NOW.date(), NOW.date(), NOW.date(), NOW.date(),
            NOW, NOW, NOW.date(), NOW.date(), NOW.date(), NOW.date(),
            NOW, NOW,
        ),
        history_source_cohort_id=SOURCE,
        history_source_provider_tier="MASSIVE_DEVELOPER",
        history_before=NOW,
        history_binding_sha256="a" * 64,
    )


def test_winning_bars_union_target_with_prior_source_history():
    cursor = MagicMock()
    cursor.fetchall.return_value = []

    select_winning_bars(
        cursor,
        _scope(),
        BarClocks(NOW, NOW),
    )

    statement, params = cursor.execute.call_args.args
    assert "OR (cohort_id=%s AND provider_tier=%s AND bar_end<%s)" in statement
    assert str(TARGET) in params
    assert str(SOURCE) in params
    assert "MASSIVE_ADVANCED" in params
    assert "MASSIVE_DEVELOPER" in params


def test_source_lineage_can_resolve_from_both_bound_cohorts():
    cursor = MagicMock()
    cursor.fetchall.return_value = []

    select_source_bars(
        cursor,
        _scope(),
        [UUID(int=3)],
        BarClocks(NOW, NOW),
    )

    statement, params = cursor.execute.call_args.args
    assert "OR (cohort_id=%s AND provider_tier=%s AND bar_end<%s)" in statement
    assert str(TARGET) in params
    assert str(SOURCE) in params
