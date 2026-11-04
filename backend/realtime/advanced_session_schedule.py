"""When a session-bound worker should be awake, from the exchange calendar."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone


UTC = timezone.utc
# Ahead of the activation boundary at open minus 15 minutes.
SESSION_WAKE_LEAD = timedelta(minutes=20)
# Past the final processing deadline at close plus 5:05, leaving room for the
# closing window to re-run once the session's adjusted daily bar arrives.
SESSION_SLEEP_LAG = timedelta(minutes=45)


def session_wake_window(
    checked_at: datetime,
    calendar,
) -> tuple[datetime, datetime]:
    """The current or next (wake, sleep) span; holidays and half-days included."""
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        raise ValueError("session schedule clock must be timezone-aware")
    now = checked_at.astimezone(UTC)
    session = calendar.session_on_or_after(now.date())
    sleep_at = calendar.session_close(session) + SESSION_SLEEP_LAG
    if now >= sleep_at:
        session = calendar.next_session_date(session)
        sleep_at = calendar.session_close(session) + SESSION_SLEEP_LAG
    return calendar.session_open(session) - SESSION_WAKE_LEAD, sleep_at
