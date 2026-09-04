import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.setup_composition import (
    DirectionComposition,
    compose_setup_direction,
    compose_trade_levels,
)
from equity.technicals import (
    EmaConfirmation,
    compute_trade_setup_technicals,
    detect_golden_cross,
)


def upward_technicals():
    close = np.linspace(100.0, 130.0, 250)
    frame = pd.DataFrame({
        "high": close + 1,
        "low": close - 1,
        "close": close,
        "volume": np.linspace(1_000_000.0, 1_200_000.0, len(close)),
    })
    return compute_trade_setup_technicals(frame, "1h")


def test_direction_composition_preserves_legacy_vote_order_and_conviction():
    technicals = upward_technicals()

    result = compose_setup_direction(
        interval="1h",
        technicals=technicals,
        confirmation=EmaConfirmation("Bullish", 129.58, 128.8),
        confirmation_interval="1d",
        primary_retests=(),
        moving_average=None,
        momentum_pullback=None,
        bearish_bounce=None,
        gaps=(),
        fair_value_gaps=(),
        golden_cross=detect_golden_cross(technicals),
    )

    assert result.direction == "Bullish"
    assert result.conviction == "High"
    assert result.bull_signals == 3
    assert result.bear_signals == 1
    assert result.signal_reasons == (
        "EMA Stack: 8 > 21 > 50 (bullish alignment)",
        "Price above VWAP(20) $128.86",
        "Multi-TF: 1d 8/21 EMA aligns bullish with 1h",
        "RSI: 100.0 (overbought — pullback risk)",
    )


def test_direction_composition_marks_equal_votes_conflicted():
    technicals = upward_technicals()

    result = compose_setup_direction(
        interval="1h",
        technicals=technicals,
        confirmation=EmaConfirmation(None, None, None),
        confirmation_interval="1d",
        primary_retests=(),
        moving_average=None,
        momentum_pullback={"grade": "C", "score": 1},
        bearish_bounce={"grade": "A", "score": 5},
        gaps=(),
        fair_value_gaps=(),
        golden_cross=detect_golden_cross(technicals),
    )

    assert result.bull_signals == result.bear_signals == 3
    assert result.direction == "Neutral"
    assert result.conviction == "Conflicted"


def test_directional_brackets_reverse_for_bearish_setup():
    technicals = upward_technicals()
    direction = DirectionComposition(
        direction="Bearish", conviction="High", bull_signals=1, bear_signals=3,
        signal_reasons=(), support_gaps=(), resistance_gaps=(), bull_fvgs=(),
        bear_fvgs=(), zones=(),
    )

    result = compose_trade_levels(
        interval="30m", technicals=technicals, direction=direction,
        primary_retests=(), momentum_pullback=None, bearish_bounce=None,
        fibonacci=None, directional_brackets=True,
    )

    close = float(technicals.close[-1])
    assert result.targets
    assert result.stops
    assert all(target["price"] < close for target in result.targets)
    assert all(stop["price"] > close for stop in result.stops)
    assert result.targets[0]["price"] > result.targets[-1]["price"]
    assert result.stops[0]["price"] < result.stops[-1]["price"]


def test_directional_brackets_fail_closed_for_conflicted_setup():
    technicals = upward_technicals()
    direction = DirectionComposition(
        direction="Neutral", conviction="Conflicted", bull_signals=2, bear_signals=2,
        signal_reasons=(), support_gaps=(), resistance_gaps=(), bull_fvgs=(),
        bear_fvgs=(), zones=(),
    )

    result = compose_trade_levels(
        interval="30m", technicals=technicals, direction=direction,
        primary_retests=(), momentum_pullback=None, bearish_bounce=None,
        fibonacci=None, directional_brackets=True,
    )

    assert result.entries == result.targets == result.stops == ()


def test_directional_brackets_deduplicate_targets_after_rounding():
    technicals = upward_technicals()
    prior_high = round(float(max(technicals.high[:-1])), 2)
    duplicate_atr = (prior_high - float(technicals.close[-1])) / 2
    technicals = replace(technicals, atr14=duplicate_atr)
    direction = DirectionComposition(
        direction="Bullish", conviction="High", bull_signals=3, bear_signals=1,
        signal_reasons=(), support_gaps=(), resistance_gaps=(), bull_fvgs=(),
        bear_fvgs=(), zones=(),
    )

    result = compose_trade_levels(
        interval="30m", technicals=technicals, direction=direction,
        primary_retests=(), momentum_pullback=None, bearish_bounce=None,
        fibonacci=None, directional_brackets=True,
    )

    target_prices = [target["price"] for target in result.targets]
    assert len(target_prices) == len(set(target_prices))


def test_bearish_brackets_exclude_level_equal_to_displayed_close():
    technicals = upward_technicals()
    close = technicals.close.copy()
    close[-1] = 50.424
    technicals = replace(
        technicals,
        close=close,
        ma50=50.419,
        atr14=0.25,
    )
    direction = DirectionComposition(
        direction="Bearish", conviction="High", bull_signals=1, bear_signals=3,
        signal_reasons=(), support_gaps=(), resistance_gaps=(), bull_fvgs=(),
        bear_fvgs=(), zones=(),
    )

    result = compose_trade_levels(
        interval="30m", technicals=technicals, direction=direction,
        primary_retests=(), momentum_pullback=None, bearish_bounce=None,
        fibonacci=None, directional_brackets=True,
    )

    assert all(target["price"] < 50.42 for target in result.targets)
    assert all(stop["price"] > 50.42 for stop in result.stops)
