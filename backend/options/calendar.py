from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

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

    def session_for_market_time(self, market_time: datetime) -> date:
        if market_time.tzinfo is None or market_time.utcoffset() is None:
            raise ValueError("market_time must be timezone-aware")
        session = pd.Timestamp(market_time.astimezone(timezone.utc).date())
        if not self._calendar.is_session(session):
            raise ValueError("market_time must belong to an exchange session")
        return session.date()

    def session_close(self, session_date: date) -> datetime:
        session = self._calendar.date_to_session(
            pd.Timestamp(session_date), direction="none"
        )
        return self._calendar.session_close(session).to_pydatetime().astimezone(
            timezone.utc
        )

    def next_session_open(self, session_date: date) -> datetime:
        session = self._calendar.date_to_session(
            pd.Timestamp(session_date), direction="none"
        )
        next_session = self._calendar.next_session(session)
        return self._calendar.session_open(next_session).to_pydatetime().astimezone(
            timezone.utc
        )

    def sessions_between(self, start: date, end: date) -> int:
        """Exchange sessions from `start` to `end`, counting `end` and not `start`.

        Consecutive sessions return 1, including across a weekend or holiday. A larger
        value means sessions were missed, which calendar-day distance cannot reveal.
        """
        if end <= start:
            raise ValueError("end must follow start")
        sessions = self._calendar.sessions_in_range(
            pd.Timestamp(start), pd.Timestamp(end)
        )
        return max(len(sessions) - 1, 1)

    def previous_session(self, session_date: date) -> date:
        session = self._calendar.date_to_session(
            pd.Timestamp(session_date), direction="none"
        )
        return self._calendar.previous_session(session).date()

    def trailing_sessions_before(self, before: date, count: int) -> tuple[date, ...]:
        """Return exactly `count` exchange sessions strictly before a date."""
        if count < 1:
            raise ValueError("session count must be positive")
        session = self._calendar.date_to_session(
            pd.Timestamp(before), direction="previous"
        )
        if session.date() >= before:
            session = self._calendar.previous_session(session)
        sessions = [session.date()]
        for _ in range(count - 1):
            session = self._calendar.previous_session(session)
            sessions.append(session.date())
        return tuple(reversed(sessions))

    @staticmethod
    def third_friday(year: int, month: int) -> date:
        """Standard monthly expiration date."""
        for day in range(15, 22):
            candidate = date(year, month, day)
            if candidate.weekday() == 4:
                return candidate
        raise ValueError("no third Friday found")

    @classmethod
    def monthly_expirations(cls, count: int, before: date) -> list[date]:
        """The `count` most recent standard monthly expirations strictly before `before`.

        Not calendar-adjusted: a third Friday that fell on a holiday still names the
        contract series, which is what the reference endpoint is keyed by.
        """
        if count < 1:
            raise ValueError("count must be positive")
        expirations: list[date] = []
        year, month = before.year, before.month
        while len(expirations) < count:
            candidate = cls.third_friday(year, month)
            if candidate < before:
                expirations.append(candidate)
            month -= 1
            if month == 0:
                year, month = year - 1, 12
        return sorted(expirations)

    @staticmethod
    def weekly_expirations(count: int, before: date) -> list[date]:
        """The `count` most recent Friday option-series dates before `before`."""
        if count < 1:
            raise ValueError("count must be positive")
        candidate = before - timedelta(days=1)
        while candidate.weekday() != 4:
            candidate -= timedelta(days=1)
        return sorted(candidate - timedelta(days=7 * index) for index in range(count))

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

    def latest_delayed_slot(
        self,
        as_of: datetime,
        *,
        interval: timedelta = timedelta(minutes=15),
        provider_delay: timedelta = timedelta(minutes=15),
        publication_grace: timedelta = timedelta(seconds=30),
    ) -> datetime | None:
        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise ValueError("as_of must be timezone-aware")
        if interval <= timedelta(0):
            raise ValueError("interval must be positive")
        if provider_delay < timedelta(0) or publication_grace < timedelta(0):
            raise ValueError("provider delay and publication grace must not be negative")
        as_of_utc = as_of.astimezone(timezone.utc)
        session = pd.Timestamp(as_of_utc.date())
        if not self._calendar.is_session(session):
            return None
        session_open = self._calendar.session_open(session).to_pydatetime().astimezone(
            timezone.utc
        )
        session_close = self._calendar.session_close(session).to_pydatetime().astimezone(
            timezone.utc
        )
        observable_time = as_of_utc - provider_delay - publication_grace
        if observable_time < session_open + interval:
            return None
        boundary = min(observable_time, session_close)
        completed_intervals = int(
            (boundary - session_open).total_seconds() // interval.total_seconds()
        )
        return session_open + completed_intervals * interval

    def session_for_slot(self, slot: datetime) -> date:
        if slot.tzinfo is None or slot.utcoffset() is None:
            raise ValueError("slot must be timezone-aware")
        slot_utc = slot.astimezone(timezone.utc)
        session = pd.Timestamp(slot_utc.date())
        if not self._calendar.is_session(session):
            raise ValueError("slot must belong to an exchange session")
        session_open = self._calendar.session_open(session).to_pydatetime().astimezone(
            timezone.utc
        )
        session_close = self._calendar.session_close(session).to_pydatetime().astimezone(
            timezone.utc
        )
        if not session_open < slot_utc <= session_close:
            raise ValueError("slot must follow the session open and not exceed its close")
        return session.date()