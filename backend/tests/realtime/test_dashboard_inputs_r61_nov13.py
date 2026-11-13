"""R61 (Nov 13): the Dashboard is built from the bars it reads plus their causal sources, not from the whole window
input (the ticker pages' intraday history). Live check, Nov 12 first and close windows at 25 names: payload content
identical, only ids and digests differ; build 36.6 s -> 16.1 s and 37.3 s -> 13.9 s."""
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

import pytest

from realtime.dashboard_projection import dashboard_input_bars, dashboard_inputs, produce_realtime_dashboard
from realtime.lineage_head import inputs_sha256
from realtime.setup_summary_projection import produce_daily_setup_summary
from tests.realtime.test_dashboard_vertical_slice import dashboard_fixture

UTC = timezone.utc
SESSION = date(2026, 11, 13)


def _bar(key, ticker, interval, basis, session_date, sources=()):
    start = datetime(session_date.year, session_date.month, session_date.day, 14, 30, tzinfo=UTC)
    return {"bar_revision_id": key, "ticker": ticker, "interval": interval, "adjustment_basis": basis,
            "session_date": session_date, "bar_start": start, "source_revision_ids": list(sources)}


def test_only_daily_recent_hourly_and_their_sources_are_kept_in_order():
    sessions = [SESSION - timedelta(days=day) for day in range(30, 0, -1)]
    bars = [_bar(f"d{index}", "AAPL", "1d", "ADJUSTED", day) for index, day in enumerate(sessions)]
    bars.append(_bar("r-last", "AAPL", "1d", "RAW_UNADJUSTED", sessions[-1], sources=("m5-a", "m5-b")))
    bars += [_bar("m5-a", "AAPL", "5m", "RAW_UNADJUSTED", sessions[-1]),
             _bar("m5-b", "AAPL", "5m", "RAW_UNADJUSTED", sessions[-1], sources=("m1-x",)),
             _bar("m1-x", "AAPL", "1m", "RAW_UNADJUSTED", sessions[-1])]
    bars.append(_bar("h-recent", "AAPL", "1h", "RAW_UNADJUSTED", sessions[-2], sources=("m1-y",)))
    bars.append(_bar("m1-y", "AAPL", "1m", "RAW_UNADJUSTED", sessions[-2]))
    bars.append(_bar("h-old", "AAPL", "1h", "RAW_UNADJUSTED", sessions[0], sources=("m1-old",)))
    bars.append(_bar("m1-old", "AAPL", "1m", "RAW_UNADJUSTED", sessions[0]))
    bars.append(_bar("h-today", "AAPL", "1h", "RAW_UNADJUSTED", SESSION))
    bars.append(_bar("h-adj", "AAPL", "1h", "ADJUSTED", sessions[-1]))
    bars += [_bar("m15", "AAPL", "15m", "RAW_UNADJUSTED", sessions[-1], sources=("m1-z",)),
             _bar("m1-z", "AAPL", "1m", "RAW_UNADJUSTED", sessions[-1]),
             _bar("m30", "AAPL", "30m", "RAW_UNADJUSTED", sessions[-1])]
    kept = [row["bar_revision_id"] for row in dashboard_input_bars(tuple(bars), SESSION)]
    assert kept == [*(f"d{index}" for index in range(30)), "r-last", "m5-a", "m5-b", "m1-x",
                    "h-recent", "m1-y", "h-today"]


def test_a_missing_source_is_left_to_the_existing_refusal():
    bars = (_bar("r", "AAPL", "1d", "RAW_UNADJUSTED", SESSION, sources=("absent",)),)
    assert [row["bar_revision_id"] for row in dashboard_input_bars(bars, SESSION)] == ["r"]


def test_hourly_sessions_follow_each_tickers_own_last_20_adjusted_sessions():
    day = SESSION - timedelta(days=40)
    bars = (_bar("msft-d", "MSFT", "1d", "ADJUSTED", day),
            _bar("msft-h", "MSFT", "1h", "RAW_UNADJUSTED", day),
            _bar("aapl-h", "AAPL", "1h", "RAW_UNADJUSTED", day))
    assert [row["bar_revision_id"] for row in dashboard_input_bars(bars, SESSION)] == ["msft-d", "msft-h"]


def test_the_fixture_dashboard_content_is_unchanged_by_extra_unread_intraday_bars():
    inputs = dashboard_fixture()
    unread = []
    for row in inputs.bars:
        if row["interval"] == "1d" and row["adjustment_basis"] == "RAW_UNADJUSTED":
            extra = dict(row, bar_revision_id=f"{row['bar_revision_id']}-unread", interval="30m",
                         source_revision_ids=[])
            unread.append(extra)
            break
    padded = replace(inputs, bars=(*inputs.bars, *unread))
    assert len(dashboard_inputs(padded).bars) == len(dashboard_inputs(inputs).bars)
    base, more = produce_realtime_dashboard(inputs).payload, produce_realtime_dashboard(padded).payload
    for key in ("overview", "scan", "scanners", "tickers", "references"):
        assert base[key] == more[key]
    assert base["setup_summary"]["results"] == more["setup_summary"]["results"]


def test_the_setup_run_id_uses_the_memoized_input_digest():
    inputs = dashboard_inputs(dashboard_fixture())
    rows, evidence, _missing = produce_daily_setup_summary(inputs)
    from uuid import NAMESPACE_URL, uuid5
    assert evidence[0]["analysis_run_id"] == str(uuid5(NAMESPACE_URL, f"realtime-daily-summary:{inputs_sha256(inputs)}"))
    assert rows
