from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_NEW_YORK = ZoneInfo("America/New_York")
_HOUR_MS = 3_600_000
_CACHE_LIMIT = 4096


def to_datetime(time_ms: int) -> datetime:
    return _EPOCH + timedelta(milliseconds=time_ms)


class SessionClock:
    """New York session date for a millisecond timestamp.

    The New York UTC offset is a whole number of hours, so the local date is constant within
    each UTC hour and the lookup is cached per hour.
    """

    __slots__ = ("_hours",)

    def __init__(self) -> None:
        self._hours: dict[int, date] = {}

    def session_date(self, time_ms: int) -> date:
        hour = time_ms // _HOUR_MS
        value = self._hours.get(hour)
        if value is None:
            if len(self._hours) >= _CACHE_LIMIT:
                self._hours.clear()
            value = to_datetime(hour * _HOUR_MS).astimezone(_NEW_YORK).date()
            self._hours[hour] = value
        return value
