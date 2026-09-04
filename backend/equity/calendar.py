"""Exchange-session timing policies for equity reads."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import exchange_calendars
import pandas as pd


INTERVAL_MINUTES = {"5m": 5, "15m": 15, "30m": 30, "1h": 60}


def latest_expected_market_time(
    now: datetime,
    interval: str,
    *,
    calendar_name: str = "XNYS",
) -> datetime:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if interval not in (*INTERVAL_MINUTES, "1d", "1wk", "1mo"):
        raise ValueError(f"unsupported interval: {interval}")
    now_utc = now.astimezone(timezone.utc)
    calendar = exchange_calendars.get_calendar(calendar_name)
    date = pd.Timestamp(now_utc.date())
    if not calendar.is_session(date):
        previous = calendar.date_to_session(date, direction="previous")
        return _completed_session_time(calendar, previous, interval)

    session_open = calendar.session_open(date).to_pydatetime().astimezone(timezone.utc)
    session_close = calendar.session_close(date).to_pydatetime().astimezone(timezone.utc)
    if now_utc < session_open:
        previous = calendar.previous_session(date)
        return _completed_session_time(calendar, previous, interval)

    if interval == "1d":
        if now_utc >= session_close:
            return session_close
        previous = calendar.previous_session(date)
        return calendar.session_close(previous).to_pydatetime().astimezone(timezone.utc)
    if interval == "1wk":
        candidate = date if now_utc >= session_close else calendar.previous_session(date)
        return _completed_week_close(calendar, candidate)
    if interval == "1mo":
        candidate = date if now_utc >= session_close else calendar.previous_session(date)
        return _completed_month_close(calendar, candidate)

    boundary = min(now_utc, session_close)
    elapsed_minutes = int((boundary - session_open).total_seconds() // 60)
    completed_minutes = (
        elapsed_minutes // INTERVAL_MINUTES[interval]
    ) * INTERVAL_MINUTES[interval]
    if completed_minutes <= 0:
        previous = calendar.previous_session(date)
        return _completed_session_time(calendar, previous, interval)
    if interval == "1h" and now_utc >= session_close:
        return session_close
    return session_open + pd.Timedelta(minutes=completed_minutes).to_pytimedelta()


def _completed_session_time(calendar, session: pd.Timestamp, interval: str) -> datetime:
    if interval == "1wk":
        return _completed_week_close(calendar, session)
    if interval == "1mo":
        return _completed_month_close(calendar, session)
    return calendar.session_close(session).to_pydatetime().astimezone(timezone.utc)


def _completed_week_close(calendar, candidate: pd.Timestamp) -> datetime:
    session = candidate
    while True:
        session_date = session.date()
        monday = pd.Timestamp(session_date - pd.Timedelta(days=session_date.weekday()))
        friday = monday + pd.Timedelta(days=4)
        week_sessions = calendar.sessions_in_range(monday, friday)
        if len(week_sessions) and session == week_sessions[-1]:
            return calendar.session_close(session).to_pydatetime().astimezone(timezone.utc)
        session = calendar.previous_session(session)


def _completed_month_close(calendar, candidate: pd.Timestamp) -> datetime:
    session = candidate
    while True:
        session_date = session.date()
        month_start = pd.Timestamp(date(session_date.year, session_date.month, 1))
        next_month = (
            date(session_date.year + 1, 1, 1)
            if session_date.month == 12
            else date(session_date.year, session_date.month + 1, 1)
        )
        month_end = pd.Timestamp(next_month - timedelta(days=1))
        month_sessions = calendar.sessions_in_range(month_start, month_end)
        if len(month_sessions) and session == month_sessions[-1]:
            return calendar.session_close(session).to_pydatetime().astimezone(timezone.utc)
        session = calendar.previous_session(session)