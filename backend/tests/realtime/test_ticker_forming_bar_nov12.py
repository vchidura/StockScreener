"""Nov 12 (user): the ticker chart's developing bar for 5m to 1mo, built from stored final 1m bars (and, for weekly and
monthly, the period's completed daily bars). Buckets match the stored bars: intraday from the 09:30 open (last hourly
bucket 15:30-16:00), daily over the session, weekly/monthly from the first session's open."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from realtime import ticker_forming_bar as forming

UTC = timezone.utc


def sessions(first: date, last: date):
    """Weekday sessions 14:30-21:00 UTC (09:30-16:00 ET in November), Nov 2026, no holidays in range."""
    out, day = [], first
    while day <= last:
        if day.weekday() < 5:
            open_ = datetime(day.year, day.month, day.day, 14, 30, tzinfo=UTC)
            out.append((open_, open_ + timedelta(hours=6, minutes=30)))
        day += timedelta(days=1)
    return out


NOW = datetime(2026, 11, 12, 16, 7, 30, tzinfo=UTC)  # Thursday 11:07:30 ET
OPEN = datetime(2026, 11, 12, 14, 30, tzinfo=UTC)


@pytest.mark.parametrize("interval, start, end", [
    ("5m", OPEN + timedelta(minutes=95), OPEN + timedelta(minutes=100)),
    ("15m", OPEN + timedelta(minutes=90), OPEN + timedelta(minutes=105)),
    ("30m", OPEN + timedelta(minutes=90), OPEN + timedelta(minutes=120)),
    ("1h", OPEN + timedelta(minutes=60), OPEN + timedelta(minutes=120)),
    ("1d", OPEN, OPEN + timedelta(hours=6, minutes=30)),
    ("1wk", datetime(2026, 11, 9, 14, 30, tzinfo=UTC), datetime(2026, 11, 13, 21, 0, tzinfo=UTC)),
    ("1mo", datetime(2026, 11, 2, 14, 30, tzinfo=UTC), datetime(2026, 11, 30, 21, 0, tzinfo=UTC)),
])
def test_buckets_follow_the_stored_bars(interval, start, end):
    bucket = forming.forming_bucket(interval, NOW, sessions)
    assert (bucket.start, bucket.end) == (start, end)
    assert (bucket.session_open, bucket.session_close) == (OPEN, OPEN + timedelta(hours=6, minutes=30))


def test_the_last_hourly_bucket_is_the_half_hour_to_the_close():
    bucket = forming.forming_bucket("1h", datetime(2026, 11, 12, 20, 40, tzinfo=UTC), sessions)
    assert (bucket.start, bucket.end) == (datetime(2026, 11, 12, 20, 30, tzinfo=UTC), datetime(2026, 11, 12, 21, 0, tzinfo=UTC))


def test_no_bucket_before_the_open_or_long_after_the_close():
    assert forming.forming_bucket("5m", datetime(2026, 11, 12, 14, 29, tzinfo=UTC), sessions) is None
    assert forming.forming_bucket("1d", datetime(2026, 11, 12, 23, 0, tzinfo=UTC), sessions) is None
    assert forming.forming_bucket("1d", datetime(2026, 11, 14, 16, 0, tzinfo=UTC), sessions) is None  # Saturday
    # Just after the close the day's bucket still counts until its final bar is published (the close takes ~45 min).
    assert forming.forming_bucket("1d", datetime(2026, 11, 12, 21, 20, tzinfo=UTC), sessions).start == OPEN


def bar(start, minutes, o, h, low, c, v):
    return dict(bar_start=start, bar_end=start + timedelta(minutes=minutes), open_price=Decimal(str(o)),
                high_price=Decimal(str(h)), low_price=Decimal(str(low)), close_price=Decimal(str(c)), volume=v)


def test_aggregate_intraday_bucket_from_its_minutes():
    bucket = forming.forming_bucket("5m", NOW, sessions)
    minutes = [bar(bucket.start + timedelta(minutes=i), 1, 100 + i, 101 + i, 99 + i, 100.5 + i, 10) for i in range(2)]
    stray = bar(bucket.start - timedelta(minutes=1), 1, 1, 1000, 1, 1, 99999)  # outside the bucket
    point = forming.aggregate_forming_bar(bucket, [stray, *minutes])
    assert point["time"] == int(bucket.start.timestamp()) and point["forming"] is True
    assert (point["open"], point["high"], point["low"], point["close"], point["volume"]) == (100, 102, 99, 101.5, 20)
    assert point["as_of"] == (bucket.start + timedelta(minutes=2)).isoformat() and point["minute_bars"] == 2


def test_weekly_bar_adds_the_completed_days_before_today():
    bucket = forming.forming_bucket("1wk", NOW, sessions)
    days = [bar(datetime(2026, 11, d, 14, 30, tzinfo=UTC), 390, 590 + d, 600 + d, 580, 595 + d, 1000) for d in (9, 10, 11)]
    minutes = [bar(OPEN, 1, 607, 615, 603, 612, 5)]
    point = forming.aggregate_forming_bar(bucket, minutes, days)
    assert (point["open"], point["high"], point["low"], point["close"], point["volume"]) == (599, 615, 580, 612, 3005)
    assert point["daily_bars"] == 3 and point["time"] == int(datetime(2026, 11, 9, 14, 30, tzinfo=UTC).timestamp())


def test_no_point_without_minutes_today():
    bucket = forming.forming_bucket("1wk", NOW, sessions)
    days = [bar(datetime(2026, 11, 9, 14, 30, tzinfo=UTC), 390, 1, 2, 1, 2, 10)]
    assert forming.aggregate_forming_bar(bucket, [], days) is None


def test_loader_reads_through_the_central_bar_read_and_stops_at_a_published_final_bar(monkeypatch):
    calls = []
    bucket = forming.forming_bucket("30m", NOW, sessions)
    minutes = [bar(bucket.start + timedelta(minutes=i), 1, 10, 11, 9, 10.5, 1) for i in range(7)]

    def fake_range(cursor, *, instrument_ids, interval, basis, start, end, scope="RTH", as_of=None):
        calls.append((interval, basis, start, end))
        if interval == "1m":
            return {instrument_ids[0]: minutes}
        return {instrument_ids[0]: published}

    import realtime.equity_bar_repository as repository
    monkeypatch.setattr(repository, "load_bar_range_cursor", fake_range)
    published = []
    result = forming.load_forming_bar_cursor(object(), instrument_id=7, interval="30m", now=NOW, sessions=sessions)
    assert result["status"] == "FORMING" and result["point"]["minute_bars"] == 7
    assert calls[0][:2] == ("30m", "RAW_UNADJUSTED") and calls[1][:2] == ("1m", "RAW_UNADJUSTED")
    published = [bar(bucket.start, 30, 1, 1, 1, 1, 1)]
    calls.clear()
    result = forming.load_forming_bar_cursor(object(), instrument_id=7, interval="30m", now=NOW, sessions=sessions)
    assert (result["status"], result["reason"], result["point"]) == ("NONE", "FINAL_BAR_PUBLISHED", None)
    assert len(calls) == 1


def test_weekly_loader_reads_adjusted_daily_bars_of_the_week(monkeypatch):
    calls = []

    def fake_range(cursor, *, instrument_ids, interval, basis, start, end, scope="RTH", as_of=None):
        calls.append((interval, basis))
        return {instrument_ids[0]: [bar(OPEN, 1, 1, 2, 1, 2, 3)] if interval == "1m" else []}

    import realtime.equity_bar_repository as repository
    monkeypatch.setattr(repository, "load_bar_range_cursor", fake_range)
    result = forming.load_forming_bar_cursor(object(), instrument_id=7, interval="1wk", now=NOW, sessions=sessions)
    assert result["status"] == "FORMING"
    assert calls == [("1wk", "ADJUSTED"), ("1m", "RAW_UNADJUSTED"), ("1d", "ADJUSTED")]


def test_unknown_interval_is_refused():
    with pytest.raises(ValueError):
        forming.forming_bucket("1m", NOW, sessions)


def test_route_and_read_contract_exist():
    from pathlib import Path

    api = (Path(forming.__file__).parent / "portal_api.py").read_text(encoding="utf-8")
    repository = (Path(forming.__file__).parent / "ticker_repository.py").read_text(encoding="utf-8")
    assert '@app.get("/api/stock/{ticker}/chart/forming")' in api
    assert 'if contract == "forming":' in repository and 'reason="CLOSING_VIEW"' in repository
