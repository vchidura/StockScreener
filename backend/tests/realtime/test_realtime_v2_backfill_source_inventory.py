from datetime import date

from realtime.contracts import EQUITY_VALIDATION_UNIVERSE
from scripts.inventory_realtime_v2_backfill_source import coverage_report, expected_session_counts


def _rows(session_date):
    rows = []
    for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers:
        for (interval, basis), sessions in expected_session_counts(session_date).items():
            for day, count in sessions.items():
                rows.append({
                    "ticker": ticker,
                    "interval": interval,
                    "adjustment_basis": basis,
                    "session_date": day,
                    "bar_count": count,
                    "derived_rows": count if interval in {"15m", "30m", "1h"} else 0,
                    "derived_without_sources": 0,
                    "native_without_segment": 0,
                    "missing_source_revisions": 0,
                })
    return rows


def test_exact_nine_ticker_history_is_sufficient():
    session = date(2026, 10, 14)
    report = coverage_report(_rows(session), session_date=session)

    assert report["complete"]
    assert report["actual_rows"] == report["expected_rows"]
    assert set(report["tickers"]) == set(EQUITY_VALIDATION_UNIVERSE.equity_tickers)


def test_a_missing_bar_and_broken_lineage_are_reported_exactly():
    session = date(2026, 10, 14)
    rows = _rows(session)
    missing = next(
        index for index, row in enumerate(rows)
        if row["ticker"] == "NFLX" and row["interval"] == "5m"
    )
    rows[missing] = {**rows[missing], "bar_count": rows[missing]["bar_count"] - 1}
    broken = next(
        index for index, row in enumerate(rows)
        if row["ticker"] == "AAPL" and row["interval"] == "30m"
    )
    rows[broken] = {**rows[broken], "missing_source_revisions": 1}

    report = coverage_report(rows, session_date=session)

    assert not report["complete"]
    nflx = report["tickers"]["NFLX"]["families"]["5m:RAW_UNADJUSTED"]
    assert nflx["incomplete_sessions"][0]["actual_count"] == nflx["incomplete_sessions"][0]["expected_count"] - 1
    assert report["tickers"]["AAPL"]["lineage"]["missing_source_revisions"] == 1
