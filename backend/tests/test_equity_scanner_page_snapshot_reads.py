import inspect
import asyncio

import main


def test_default_scanner_routes_name_their_durable_snapshot_contracts():
    expected = {
        main.scan_gaps: "SCAN_GAPS_1D",
        main.scan_fvg_endpoint: "SCAN_FVG_1D_50",
        main.scan_ma_crossover: "SCAN_MA_1D_9_21",
        main.scan_momentum: "SCAN_MOMENTUM_1D",
        main.scan_bearish_bounce_endpoint: "SCAN_BEARISH_1D",
        main.scan_fibonacci_endpoint: "SCAN_FIBONACCI_1D_5",
        main.scan_all: "SCAN_ALL_1D_5",
    }

    for function, snapshot_type in expected.items():
        source = inspect.getsource(function)
        assert "MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED" in source
        assert snapshot_type in source


def test_default_ma_route_returns_snapshot_without_computation(monkeypatch):
    monkeypatch.setattr(main, "MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED", True)
    monkeypatch.setattr(
        main, "_portal_snapshot_payload", lambda _: {"source": "snapshot"}
    )

    result = asyncio.run(main.scan_ma_crossover(
        tickers=None,
        short_period=9,
        long_period=21,
        scan_date=None,
        interval="1d",
        refresh=True,
    ))

    assert result == {"source": "snapshot"}


def test_default_streak_routes_return_snapshots(monkeypatch):
    monkeypatch.setattr(main, "MATERIALIZED_SCANNER_PAGE_SNAPSHOTS_ENABLED", True)
    monkeypatch.setattr(main, "_portal_snapshot_payload", lambda value: value)

    streak = asyncio.run(main.scan_streak_endpoint(
        strategy="gaps", days=5, tickers=None,
        short_period=9, long_period=21, refresh=True,
    ))
    summary = asyncio.run(main.scan_streak_summary_endpoint(
        days=3, fib_swing_pct=5.0, refresh=True,
    ))

    assert streak == "STREAK_GAPS_5"
    assert summary == "STREAK_SUMMARY_3_5"