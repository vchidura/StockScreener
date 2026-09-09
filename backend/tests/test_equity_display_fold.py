"""Display-only folding of finalized 5m bars into the latest chart candle."""
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.charting import fold_latest_session_bars


UTC = timezone.utc
SESSION_OPEN = datetime(2026, 9, 8, 13, 30, tzinfo=UTC)


def bar(index, *, open_price, high, low, close, volume):
    from datetime import timedelta

    start = SESSION_OPEN + timedelta(minutes=5 * index)
    return {
        "bar_start": start,
        "bar_end": start + timedelta(minutes=5),
        "open_price": open_price,
        "high_price": high,
        "low_price": low,
        "close_price": close,
        "volume": volume,
    }


def full_session():
    return [
        bar(i, open_price=100 + i, high=101 + i, low=99 + i, close=100.5 + i, volume=10 * (i + 1))
        for i in range(78)
    ]


def test_daily_window_folds_open_high_low_close_and_volume():
    folded = fold_latest_session_bars(
        full_session(),
        target_interval="1d",
        observed_at=datetime(2026, 9, 8, 21, 0, tzinfo=UTC),
    )

    assert len(folded) == 1
    row = folded[0]
    assert row["bar_start"] == SESSION_OPEN
    assert row["bar_end"] == datetime(2026, 9, 8, 20, 0, tzinfo=UTC)
    assert row["open_price"] == 100
    assert row["high_price"] == 178
    assert row["low_price"] == 99
    assert row["close_price"] == 177.5
    assert row["volume"] == sum(10 * (i + 1) for i in range(78))
    assert row["derived"] is True
    assert row["provisional"] is False
    assert row["source_interval"] == "5m"


def test_incomplete_session_yields_developing_daily_bar():
    folded = fold_latest_session_bars(
        full_session()[:-1],
        target_interval="1d",
        observed_at=datetime(2026, 9, 8, 19, 55, tzinfo=UTC),
    )

    assert len(folded) == 1
    assert folded[0]["bar_start"] == SESSION_OPEN
    assert folded[0]["bar_end"] == datetime(2026, 9, 8, 19, 55, tzinfo=UTC)
    assert folded[0]["provisional"] is True


def test_developing_hour_uses_only_available_finalized_sources():
    folded = fold_latest_session_bars(
        full_session()[:18],
        target_interval="1h",
        observed_at=datetime(2026, 9, 8, 15, 0, tzinfo=UTC),
    )

    assert len(folded) == 2
    assert folded[-1]["bar_start"] == datetime(2026, 9, 8, 14, 30, tzinfo=UTC)
    assert folded[-1]["bar_end"] == datetime(2026, 9, 8, 15, 0, tzinfo=UTC)
    assert folded[-1]["open_price"] == 112
    assert folded[-1]["close_price"] == 117.5
    assert folded[-1]["provisional"] is True


def test_final_half_hour_closes_the_last_hourly_window():
    folded = fold_latest_session_bars(
        full_session(),
        target_interval="1h",
        observed_at=datetime(2026, 9, 8, 21, 0, tzinfo=UTC),
    )

    assert len(folded) == 7
    assert folded[-1]["bar_start"] == datetime(2026, 9, 8, 19, 30, tzinfo=UTC)
    assert folded[-1]["bar_end"] == datetime(2026, 9, 8, 20, 0, tzinfo=UTC)
    assert folded[-1]["provisional"] is False


def test_early_close_uses_exchange_session_geometry():
    from datetime import timedelta

    early_open = datetime(2026, 11, 27, 14, 30, tzinfo=UTC)
    rows = []
    for index in range(42):
        start = early_open + timedelta(minutes=5 * index)
        rows.append({
            "bar_start": start,
            "bar_end": start + timedelta(minutes=5),
            "open_price": 100 + index,
            "high_price": 101 + index,
            "low_price": 99 + index,
            "close_price": 100.5 + index,
            "volume": 10,
        })

    daily = fold_latest_session_bars(
        rows,
        target_interval="1d",
        observed_at=datetime(2026, 11, 27, 18, 15, tzinfo=UTC),
    )
    hourly = fold_latest_session_bars(
        rows,
        target_interval="1h",
        observed_at=datetime(2026, 11, 27, 18, 15, tzinfo=UTC),
    )

    assert len(daily) == 1 and daily[0]["bar_end"] == datetime(2026, 11, 27, 18, 0, tzinfo=UTC)
    assert daily[0]["provisional"] is False
    assert len(hourly) == 4 and hourly[-1]["bar_start"] == datetime(2026, 11, 27, 17, 30, tzinfo=UTC)
    assert hourly[-1]["bar_end"] == datetime(2026, 11, 27, 18, 0, tzinfo=UTC)
    assert hourly[-1]["provisional"] is False


def test_missing_interior_source_rejects_the_display_candle():
    rows = [row for index, row in enumerate(full_session()[:18]) if index != 14]

    folded = fold_latest_session_bars(
        rows,
        target_interval="1h",
        observed_at=datetime(2026, 9, 8, 21, 0, tzinfo=UTC),
    )

    assert [row["bar_start"] for row in folded] == [SESSION_OPEN]


def test_non_session_date_is_ignored():
    from datetime import timedelta

    saturday = [
        {**row, "bar_start": row["bar_start"] + timedelta(days=4),
         "bar_end": row["bar_end"] + timedelta(days=4)}
        for row in full_session()
    ]

    assert fold_latest_session_bars(
        saturday,
        target_interval="1d",
        observed_at=datetime(2026, 9, 15, tzinfo=UTC),
    ) == ()


@pytest.mark.parametrize("target_interval", ["15m", "30m", "1h", "1d"])
def test_supported_target_intervals(target_interval):
    assert fold_latest_session_bars(
        full_session()[:5],
        target_interval=target_interval,
        observed_at=datetime(2026, 9, 8, 13, 55, tzinfo=UTC),
    )


def test_unsupported_interval_is_rejected():
    with pytest.raises(ValueError):
        fold_latest_session_bars(
            full_session(), target_interval="5m", observed_at=datetime.now(UTC)
        )
