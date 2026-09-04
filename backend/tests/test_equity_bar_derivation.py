import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import exchange_calendars
import pandas as pd


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.derivation import derive_canonical_bars
from equity.domain import (
    BarAvailabilityMode,
    BarSourceKind,
    EquityBarRevision,
    SecurityReferenceRevision,
)


UTC = timezone.utc


def security():
    return SecurityReferenceRevision(
        security_revision_id=uuid4(), security_id=uuid4(), ticker="AAPL",
        active=True, company_name="Apple", security_type="CS", cik=None,
        composite_figi=None, share_class_figi=None, primary_exchange="XNAS",
        sic_code=None, sic_description=None, sector=None, industry=None,
        list_date=None, delisted_date=None, weighted_shares=None, free_float=None,
        free_float_percent=None, market_cap=None, source="TEST",
        effective_from=datetime(2026, 8, 1, tzinfo=UTC),
        observed_at=datetime(2026, 8, 1, tzinfo=UTC),
        payload_sha256="a" * 64, raw_payload_json="{}",
    )


def bar(item, interval, start, end, session_date, index):
    return EquityBarRevision(
        bar_revision_id=uuid4(), security_id=item.security_id, ticker=item.ticker,
        interval=interval, session_date=session_date, bar_start=start, bar_end=end,
        open_price=Decimal(100 + index), high_price=Decimal(102 + index),
        low_price=Decimal(99 + index), close_price=Decimal(101 + index),
        volume=Decimal("1000"), vwap=Decimal("100.5"), transaction_count=10,
        source_kind=BarSourceKind.RECONCILED,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED, is_final=True,
        system_observed_at=end, replay_available_at=None, adjusted=False,
        payload_sha256=f"{index + 1:064x}"[-64:],
    )


def intraday_30m(item, session_date=date(2026, 8, 28)):
    calendar = exchange_calendars.get_calendar("XNYS")
    session = pd.Timestamp(session_date)
    session_open = calendar.session_open(session).to_pydatetime().astimezone(UTC)
    session_close = calendar.session_close(session).to_pydatetime().astimezone(UTC)
    results = []
    start = session_open
    index = 0
    while start < session_close:
        end = min(start + timedelta(minutes=30), session_close)
        results.append(bar(item, "30m", start, end, session_date, index))
        start = end
        index += 1
    return tuple(results)


def daily(item, start=date(2026, 8, 1), end=date(2026, 8, 31)):
    calendar = exchange_calendars.get_calendar("XNYS")
    results = []
    for index, session in enumerate(calendar.sessions_in_range(
        pd.Timestamp(start), pd.Timestamp(end)
    )):
        session_date = session.date()
        session_open = calendar.session_open(session).to_pydatetime().astimezone(UTC)
        session_close = calendar.session_close(session).to_pydatetime().astimezone(UTC)
        results.append(bar(item, "1d", session_open, session_close, session_date, index))
    return tuple(results)


def test_hourly_and_daily_derivation_require_complete_30m_windows():
    item = security()
    source = intraday_30m(item)
    observed_at = source[-1].bar_end

    hourly = derive_canonical_bars(
        item, source, target_interval="1h", observed_at=observed_at,
        ingestion_segment_id=uuid4(),
    )
    daily_result = derive_canonical_bars(
        item, source, target_interval="1d", observed_at=observed_at,
        ingestion_segment_id=uuid4(),
    )
    incomplete = derive_canonical_bars(
        item, source[:-1], target_interval="1d", observed_at=observed_at,
        ingestion_segment_id=uuid4(),
    )

    assert len(hourly) == 7
    assert len(hourly[-1].source_bar_revision_ids) == 1
    assert hourly[-1].bar_end == source[-1].bar_end
    assert len(daily_result) == 1
    assert daily_result[0].source_bar_revision_ids == tuple(
        row.bar_revision_id for row in source
    )
    assert incomplete == ()


def test_monthly_derivation_waits_for_final_xnys_session():
    item = security()
    source = daily(item)
    segment_id = uuid4()

    before_final_session = derive_canonical_bars(
        item, source[:-1], target_interval="1mo",
        observed_at=datetime(2026, 8, 30, 23, 0, tzinfo=UTC),
        ingestion_segment_id=segment_id,
    )
    complete = derive_canonical_bars(
        item, source, target_interval="1mo",
        observed_at=datetime(2026, 8, 31, 21, 0, tzinfo=UTC),
        ingestion_segment_id=segment_id,
    )

    assert before_final_session == ()
    assert len(complete) == 1
    assert complete[0].bar_end == datetime(2026, 8, 31, 20, 0, tzinfo=UTC)
    assert len(complete[0].source_bar_revision_ids) == 21
    assert complete[0].volume == Decimal("21000")


def test_weekly_derivation_accepts_complete_exchange_holiday_week():
    item = security()
    source = daily(item, date(2026, 8, 24), date(2026, 8, 28))

    result = derive_canonical_bars(
        item, source, target_interval="1wk",
        observed_at=datetime(2026, 8, 28, 21, 0, tzinfo=UTC),
        ingestion_segment_id=uuid4(),
    )

    assert len(result) == 1
    assert result[0].source_bar_revision_ids == tuple(row.bar_revision_id for row in source)