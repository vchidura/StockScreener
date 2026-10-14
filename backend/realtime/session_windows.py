"""Fifteen-minute Developer window schedule for a session (pure, no database, no provider).

The V2 service builds its window specs from this, so it does not depend on the un-ported v1 forward
lineage (`forward_runtime`). The schedule is the same reviewed one: the first cutoff is fifteen minutes
after the open, each window is released fifteen minutes after its cutoff, and the completion and serving
budgets are bounded.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

MAX_BUDGET_SECONDS = 90
WINDOW_MINUTES = 15


@dataclass(frozen=True, slots=True)
class DeveloperSessionWindow:
    cutoff: datetime
    available_after: datetime
    deadline: datetime
    serving_expires_at: datetime


def developer_session_windows(session_date: date, *, completion_budget_seconds: int,
                              serving_lag_seconds: int, calendar=None) -> tuple[DeveloperSessionWindow, ...]:
    from options.calendar import OptionExchangeCalendar

    if (type(completion_budget_seconds) is not int or not 1 <= completion_budget_seconds <= MAX_BUDGET_SECONDS
            or type(serving_lag_seconds) is not int or not 1 <= serving_lag_seconds <= MAX_BUDGET_SECONDS):
        raise ValueError("Developer schedule requires reviewed bounded completion/serving budgets")
    calendar = calendar or OptionExchangeCalendar()
    opened, closed = calendar.session_open(session_date), calendar.session_close(session_date)
    step = timedelta(minutes=WINDOW_MINUTES)
    cutoff = opened + step
    windows = []
    while cutoff <= closed:
        release = cutoff + step
        windows.append(DeveloperSessionWindow(
            cutoff, release, release + timedelta(seconds=completion_budget_seconds),
            release + step + timedelta(seconds=serving_lag_seconds)))
        cutoff += step
    return tuple(windows)
