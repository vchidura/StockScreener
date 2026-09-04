import pandas as pd

from screeners import check_gap_status, identify_gap_down, scan_gap_strategies


def _flat_frame(periods: int = 30, *, frequency: str = "B") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": [100.5] * periods,
            "high": [101.0] * periods,
            "low": [100.0] * periods,
            "close": [100.5] * periods,
            "volume": [1_000_000] * periods,
        },
        index=pd.date_range("2026-01-02", periods=periods, freq=frequency),
    )


def _rising_frame(periods: int = 30) -> pd.DataFrame:
    closes = [90.0 + index * 0.4 for index in range(periods)]
    return pd.DataFrame(
        {
            "open": closes,
            "high": [close + 0.5 for close in closes],
            "low": [close - 0.5 for close in closes],
            "close": closes,
            "volume": [1_000_000] * periods,
        },
        index=pd.date_range("2026-01-02", periods=periods, freq="B"),
    )


def test_unfilled_gap_down_uses_near_resistance_edge() -> None:
    frame = _flat_frame(25)
    frame.iloc[20] = [97.0, 98.0, 96.0, 97.0, 2_000_000]
    frame.iloc[21:] = [97.5, 98.0, 97.0, 97.9, 1_000_000]

    signals = check_gap_status(frame, [], identify_gap_down(frame))

    assert len(signals) == 1
    assert signals[0]["gap_type"] == "At Resistance (Unfilled Gap Down)"
    assert signals[0]["gap_low"] == 98.0


def test_same_session_fade_is_retained_as_gap_event() -> None:
    frame = _flat_frame(25)
    frame.iloc[-1] = [103.0, 104.0, 100.4, 100.7, 2_000_000]

    results = scan_gap_strategies("FADE", frame)

    assert len(results) == 1
    assert results[0]["gap_type"] == "Same-Session Fade (Gap Up)"
    assert results[0]["gap_lifecycle"] == "SAME_SESSION_FADE"
    assert results[0]["fill_pct"] == 100.0
    assert results[0]["first_fill_date"] == frame.index[-1].strftime("%Y-%m-%d")


def test_breakaway_gap_exposes_formation_context() -> None:
    frame = _flat_frame(30)
    frame.iloc[-1] = [104.0, 105.0, 103.5, 104.5, 3_000_000]

    results = scan_gap_strategies("BREAK", frame)

    assert len(results) == 1
    result = results[0]
    assert result["gap_classification"] == "BREAKAWAY"
    assert result["classification_confidence"] == "MODERATE"
    assert "BREAKS_20_BAR_RANGE" in result["classification_reason_codes"]
    assert "ELEVATED_FORMATION_VOLUME" in result["classification_reason_codes"]
    assert result["formation_relative_volume"] == 3.0
    assert result["opening_gap_pct"] > result["full_gap_pct"] > 0


def test_trend_aligned_gap_is_continuation_not_breakaway() -> None:
    frame = _rising_frame(30)
    frame.iloc[-1] = [104.0, 105.0, 103.5, 104.5, 2_000_000]

    result = scan_gap_strategies("RUN", frame)[0]

    assert result["gap_classification"] == "CONTINUATION"
    assert "FORMATION_TREND_BULLISH" in result["classification_reason_codes"]


def test_extended_light_volume_gap_is_exhaustion_watch() -> None:
    frame = _rising_frame(30)
    frame.iloc[-1] = [104.0, 105.0, 103.5, 104.5, 500_000]

    result = scan_gap_strategies("TIRE", frame)[0]

    assert result["gap_classification"] == "EXHAUSTION_WATCH"
    assert "EXTENDED_FROM_20_BAR_MEAN" in result["classification_reason_codes"]
    assert "LIGHT_FORMATION_VOLUME" in result["classification_reason_codes"]


def test_sideways_gap_inside_prior_range_is_common() -> None:
    frame = _flat_frame(30)
    frame.iloc[-10, frame.columns.get_loc("high")] = 110.0
    frame.iloc[-1] = [103.0, 104.0, 102.5, 103.5, 1_000_000]

    result = scan_gap_strategies("COMMON", frame)[0]

    assert result["gap_classification"] == "COMMON"
    assert "BREAKS_20_BAR_RANGE" not in result["classification_reason_codes"]


def test_intraday_scan_ignores_gap_between_bars_in_same_session() -> None:
    frame = _flat_frame(25, frequency="5min")
    frame.iloc[-1] = [104.0, 105.0, 103.5, 104.5, 3_000_000]

    assert scan_gap_strategies("NOISE", frame, interval="5m") == []