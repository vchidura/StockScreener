import pandas as pd

from equity.portal_scanners import compute_default_scanner_snapshots


def test_default_scanner_snapshot_contracts_exist_for_empty_frames():
    result = compute_default_scanner_snapshots(
        ["AAPL"], {}, {"SPY": pd.DataFrame(), "QQQ": pd.DataFrame()}
    )

    assert set(result) == {
        "SCAN_GAPS_1D", "SCAN_FVG_1D_50", "SCAN_MA_1D_9_21",
        "SCAN_MOMENTUM_1D", "SCAN_BEARISH_1D",
        "SCAN_FIBONACCI_1D_5", "SCAN_ALL_1D_5",
    }
    assert result["SCAN_GAPS_1D"]["total_scanned"] == 1
    assert result["SCAN_GAPS_1D"]["results"] == []
    assert result["SCAN_ALL_1D_5"]["market_regime"]["regime"] == "Unknown"