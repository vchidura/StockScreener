import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.technicals import (
    compute_trade_setup_technicals,
    detect_level_retests,
    exponential_moving_average,
)


def test_exponential_moving_average_preserves_legacy_recursive_formula():
    result = exponential_moving_average(np.array([1.0, 2.0, 3.0]), 2)

    assert result == pytest.approx([1.0, 1.6666666667, 2.5555555556])


def test_exponential_moving_average_rejects_invalid_contracts():
    with pytest.raises(ValueError, match="nonempty one-dimensional"):
        exponential_moving_average(np.array([]), 8)
    with pytest.raises(ValueError, match="span must be positive"):
        exponential_moving_average(np.array([1.0]), 0)


def test_level_retests_preserve_touch_precedence_and_deduplicate_levels():
    close = np.array([100.0, 101.0, 102.0])
    high = np.array([100.5, 102.0, 103.0])
    low = np.array([99.5, 100.0, 101.0])
    levels = [
        {"price": 100.0, "name": "Support", "source": "Test"},
        {"price": 100.0, "name": "Support", "source": "Test"},
    ]

    result = detect_level_retests(
        close, high, low, levels, lookback=3, tolerance_pct=0.5
    )

    assert result == [{
        "level_name": "Support",
        "level_price": 100.0,
        "source": "Test",
        "candle_high": 100.5,
        "candle_low": 99.5,
        "candle_close": 100.0,
        "touch_type": "Low touched",
        "held": False,
        "bounce_pct": 0.0,
        "bars_ago": 2,
    }]


def test_level_retests_reject_mismatched_price_arrays():
    with pytest.raises(ValueError, match="equal length"):
        detect_level_retests(
            np.array([1.0]), np.array([1.0, 2.0]), np.array([1.0]), []
        )


def test_trade_setup_technical_snapshot_exposes_golden_worker_payload():
    close = np.linspace(100.0, 130.0, 250)
    frame = pd.DataFrame({
        "open": close - 0.25,
        "high": close + 1.0,
        "low": close - 1.0,
        "close": close,
        "volume": np.linspace(1_000_000.0, 1_200_000.0, len(close)),
    })

    result = compute_trade_setup_technicals(frame, "1h")
    payload = result.payload()

    assert payload["rsi"] == 100.0
    assert payload["rsi_state"] == "Overbought"
    assert payload["ema8"] == 129.58
    assert payload["ema21"] == 128.8
    assert payload["ema50"] == 127.05
    assert payload["macd"] == 0.8434
    assert payload["historical_volatility_state"] == "QUIET"
    assert payload["range_position_pct"] == 96.9
    assert result.ema_alignment == "Bullish Stack"


def test_worker_technicals_include_latest_finalized_bar_in_completed_metrics():
    close = np.linspace(100.0, 130.0, 50)
    volume = np.linspace(1_000.0, 2_000.0, 50)
    volume[-1] = 10_000.0
    frame = pd.DataFrame({
        "high": close + 1,
        "low": close - 1,
        "close": close,
        "volume": volume,
    })

    legacy = compute_trade_setup_technicals(frame, "30m")
    worker = compute_trade_setup_technicals(
        frame, "30m", input_includes_forming_bar=False
    )

    assert legacy.relative_volume != worker.relative_volume
