"""The developing (forming) bar of a ticker chart interval (user, Nov 12).

The ticker chart draws final bars only, so the bar still forming was missing. This builds it at read time from the
stored final 1-minute bars (RAW, RTH) since the bucket started, plus, for weekly and monthly charts, the period's
completed daily bars (ADJUSTED, the chart's own basis). It is display-only: the response marks it `forming`, the chart
draws it apart, and no pattern, setup, indicator or alert reads it. Buckets follow the stored bars (checked on live Nov
12): intraday buckets start at the session open (09:30 ET; the last hourly bucket is 15:30-16:00), a daily bar spans
the session, a weekly or monthly bar spans its first session's open to its last session's close. When the interval's
final bar for the bucket already exists, there is no forming bar.

Checked on the Nov 11 copy (Nov 12): rebuilt over whole completed buckets, 5m/15m/30m/1h and the derived RAW daily bar
equal the stored final bars. The official daily bar (ADJUSTED, provider REST, the chart's daily basis) also carries the
closing auction, which 1-minute bars do not, so today's developing daily, weekly and monthly bars end at the last minute
bar's close and volume until the official daily bar replaces them after the close.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Callable, Mapping, Sequence

FORMING_INTERVALS = ("5m", "15m", "30m", "1h", "1d", "1wk", "1mo")
_INTRADAY_STEP = {"5m": 5, "15m": 15, "30m": 30, "1h": 60}
_CHART_BASIS = {"1d": "ADJUSTED", "1wk": "ADJUSTED", "1mo": "ADJUSTED"}


@dataclass(frozen=True, slots=True)
class FormingBucket:
    interval: str
    start: datetime  # the bar's start, as the final bar will carry it
    end: datetime
    session_open: datetime  # today's session: the 1m bars come from [max(start, session_open), now)
    session_close: datetime
    period_open: datetime  # weekly/monthly: the completed daily bars come from [period_open, session_open)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("forming bar times must be timezone-aware")
    return value.astimezone(timezone.utc)


def forming_bucket(interval: str, now: datetime, sessions: Callable[[date, date], Sequence[tuple[datetime, datetime]]]) -> FormingBucket | None:
    """The bucket forming at `now`, or None outside a session that has started. `sessions(first, last)` gives each
    XNYS session's (open, close) in UTC for the dates [first, last], in order."""
    if interval not in FORMING_INTERVALS:
        raise ValueError("Unsupported forming bar interval")
    now = _utc(now)
    today = [row for row in sessions(now.date() - timedelta(days=1), now.date() + timedelta(days=1))
             if row[0] <= now < row[1] + timedelta(minutes=90)]
    if not today:
        return None
    session_open, session_close = today[-1]
    clock = min(now, session_close - timedelta(microseconds=1))
    if interval in _INTRADAY_STEP:
        step = timedelta(minutes=_INTRADAY_STEP[interval])
        start = session_open + step * int((clock - session_open) / step)
        return FormingBucket(interval, start, min(start + step, session_close), session_open, session_close, start)
    if interval == "1d":
        return FormingBucket(interval, session_open, session_close, session_open, session_close, session_open)
    local = session_open.date()
    first = local - timedelta(days=local.weekday()) if interval == "1wk" else local.replace(day=1)
    last = first + timedelta(days=6) if interval == "1wk" else (first.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    period = list(sessions(first, last))
    if not period:
        return None
    return FormingBucket(interval, period[0][0], period[-1][1], session_open, session_close, period[0][0])


def aggregate_forming_bar(bucket: FormingBucket, minute_bars: Sequence[Mapping], daily_bars: Sequence[Mapping] = ()) -> dict | None:
    """OHLCV of the bucket from completed daily bars (weekly/monthly) then today's 1m bars, in time order; the chart
    point shape (`ticker_references.chart_points`) plus `forming`, `as_of` and the bar counts. None without bars."""
    minutes = sorted((row for row in minute_bars if bucket.start <= row["bar_start"] < bucket.end
                      and bucket.session_open <= row["bar_start"] < bucket.session_close), key=lambda row: row["bar_start"])
    days = sorted((row for row in daily_bars if bucket.period_open <= row["bar_start"] < bucket.session_open),
                  key=lambda row: row["bar_start"])
    rows = [*days, *minutes]
    if not minutes:
        return None
    return dict(
        time=int(bucket.start.timestamp()),
        open=round(float(rows[0]["open_price"]), 2),
        high=round(max(float(row["high_price"]) for row in rows), 2),
        low=round(min(float(row["low_price"]) for row in rows), 2),
        close=round(float(rows[-1]["close_price"]), 2),
        # 1m volumes can be fractional (odd lots); sum them exactly and truncate once, as `chart_points` does.
        volume=int(sum(Decimal(str(row["volume"])) for row in rows)),
        forming=True,
        as_of=_utc(minutes[-1]["bar_end"]).isoformat(),
        minute_bars=len(minutes),
        daily_bars=len(days),
    )


def load_forming_bar_cursor(cursor, *, instrument_id: int, interval: str, now: datetime, sessions) -> dict:
    """The forming bar of `interval` at `now` from the bar head (central read, INV-4); never fails on missing data."""
    from .equity_bar_repository import load_bar_range_cursor

    bucket = forming_bucket(interval, now, sessions)
    if bucket is None:
        return dict(interval=interval, status="NONE", reason="NO_SESSION_IN_PROGRESS", point=None)
    basis = _CHART_BASIS.get(interval, "RAW_UNADJUSTED")
    final = load_bar_range_cursor(cursor, instrument_ids=(instrument_id,), interval=interval, basis=basis,
                                  start=bucket.start, end=bucket.end)[instrument_id]
    if any(row["bar_start"] == bucket.start and row.get("is_final", True) for row in final):
        return dict(interval=interval, status="NONE", reason="FINAL_BAR_PUBLISHED", point=None)
    minutes = load_bar_range_cursor(cursor, instrument_ids=(instrument_id,), interval="1m", basis="RAW_UNADJUSTED",
                                    start=max(bucket.start, bucket.session_open), end=min(bucket.end, bucket.session_close))[instrument_id]
    daily = ()
    if interval in ("1wk", "1mo") and bucket.period_open < bucket.session_open:
        daily = load_bar_range_cursor(cursor, instrument_ids=(instrument_id,), interval="1d", basis="ADJUSTED",
                                      start=bucket.period_open, end=bucket.session_open)[instrument_id]
    point = aggregate_forming_bar(bucket, minutes, daily)
    if point is None:
        return dict(interval=interval, status="NONE", reason="NO_MINUTE_BARS_YET", point=None)
    return dict(interval=interval, status="FORMING", reason=None, point=point,
                bucket_start=bucket.start.isoformat(), bucket_end=bucket.end.isoformat())


def xnys_sessions(first: date, last: date) -> list[tuple[datetime, datetime]]:
    """(open, close) in UTC of every XNYS session in [first, last]."""
    import exchange_calendars

    calendar = exchange_calendars.get_calendar("XNYS")
    return [
        (calendar.session_open(session).to_pydatetime().astimezone(timezone.utc),
         calendar.session_close(session).to_pydatetime().astimezone(timezone.utc))
        for session in calendar.sessions_in_range(first.isoformat(), last.isoformat())
    ]
