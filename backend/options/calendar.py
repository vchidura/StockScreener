from __future__ import annotations

from datetime import date, datetime, timezone

import exchange_calendars
import pandas as pd


class OptionExchangeCalendar:
    def __init__(self, calendar_name: str = "XNYS") -> None:
        self.calendar_name = calendar_name
        self._calendar = exchange_calendars.get_calendar(calendar_name)

    def expiration_cutoff(self, expiration_date: date) -> datetime:
        requested = pd.Timestamp(expiration_date)
        session = self._calendar.date_to_session(requested, direction="previous")
        return self._calendar.session_close(session).to_pydatetime().astimezone(timezone.utc)

    def latest_completed_session(self, as_of: datetime) -> date:
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        as_of_utc = as_of.astimezone(timezone.utc)
        session = self._calendar.date_to_session(
            pd.Timestamp(as_of_utc.date()),
            direction="previous",
        )
        if self._calendar.session_close(session).to_pydatetime() > as_of_utc:
            session = self._calendar.previous_session(session)
        return session.date()