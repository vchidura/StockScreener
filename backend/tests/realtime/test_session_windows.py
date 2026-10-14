"""The V2 session window schedule (no v1 forward dependency) equals the reviewed v1 schedule."""
from __future__ import annotations

from datetime import date

import pytest

from realtime.forward_runtime import developer_forward_session_windows
from realtime.session_windows import developer_session_windows


@pytest.mark.parametrize("session", [date(2026, 10, 12), date(2026, 11, 27), date(2026, 3, 9)])
def test_the_v2_schedule_equals_the_v1_schedule(session):
    kwargs = dict(completion_budget_seconds=60, serving_lag_seconds=30)
    v1 = developer_forward_session_windows(session, **kwargs)
    v2 = developer_session_windows(session, **kwargs)
    assert len(v2) == len(v1) > 0
    for old, new in zip(v1, v2):
        assert (new.cutoff, new.available_after, new.deadline, new.serving_expires_at) == (
            old.cutoff, old.available_after, old.deadline, old.serving_expires_at)


@pytest.mark.parametrize("budget", [0, 91, -1, 60.0, True])
def test_budgets_must_be_reviewed_bounded_integers(budget):
    with pytest.raises(ValueError):
        developer_session_windows(date(2026, 10, 12), completion_budget_seconds=budget, serving_lag_seconds=30)
    with pytest.raises(ValueError):
        developer_session_windows(date(2026, 10, 12), completion_budget_seconds=30, serving_lag_seconds=budget)
