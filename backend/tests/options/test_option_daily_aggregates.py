import json
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

BACKEND_DIR = Path(__file__).resolve().parents[2]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from options.errors import OptionProviderError

from test_polygon_developer import _engine, _response


def _bar(epoch_millis: int, close: float = 1.5, volume: int = 100, **extra):
    row = {
        "t": epoch_millis,
        "o": close - 0.1,
        "h": close + 0.2,
        "l": close - 0.3,
        "c": close,
        "v": volume,
    }
    row.update(extra)
    return row


# 2024-09-03 and 2024-09-04 session opens in UTC.
SESSION_ONE = 1725336000000
SESSION_TWO = 1725422400000


def test_daily_aggregates_parse_into_settlement_bars():
    engine, _, transport, _ = _engine(
        [_response({"status": "OK", "results": [_bar(SESSION_ONE), _bar(SESSION_TWO, 1.8, 250, n=42)]})]
    )
    bars = engine.get_option_daily_aggregates(
        "O:SPY240906C00540000", date(2024, 9, 1), date(2024, 9, 30)
    )
    assert len(bars) == 2
    assert bars[0].contract_ticker == "O:SPY240906C00540000"
    assert bars[0].close == Decimal("1.5")
    assert bars[0].volume == 100
    assert bars[0].transaction_count is None
    assert bars[1].close == Decimal("1.8")
    assert bars[1].transaction_count == 42
    assert bars[0].session_date < bars[1].session_date


def test_request_targets_the_daily_aggregate_endpoint():
    engine, _, transport, _ = _engine([_response({"status": "OK", "results": []})])
    engine.get_option_daily_aggregates(
        "O:SPY240906C00540000", date(2024, 7, 8), date(2024, 9, 6)
    )
    url, params = transport.requests[0]
    assert "/v2/aggs/ticker/O:SPY240906C00540000/range/1/day/2024-07-08/2024-09-06" in url
    assert params["sort"] == "asc"
    assert params["adjusted"] == "false"


def test_missing_results_is_an_empty_series_not_an_error():
    """A contract that never traded in the window returns no results key at all."""
    engine, _, _, _ = _engine([_response({"status": "OK"})])
    assert engine.get_option_daily_aggregates(
        "O:SPY240906C00540000", date(2024, 9, 1), date(2024, 9, 6)
    ) == ()


def test_results_of_the_wrong_shape_are_rejected():
    engine, _, _, _ = _engine([_response({"status": "OK", "results": {"c": 1.0}})])
    with pytest.raises(OptionProviderError):
        engine.get_option_daily_aggregates(
            "O:SPY240906C00540000", date(2024, 9, 1), date(2024, 9, 6)
        )


def test_bars_outside_the_requested_window_are_dropped():
    engine, _, _, _ = _engine(
        [_response({"status": "OK", "results": [_bar(SESSION_ONE), _bar(SESSION_TWO)]})]
    )
    bars = engine.get_option_daily_aggregates(
        "O:SPY240906C00540000", date(2024, 9, 3), date(2024, 9, 3)
    )
    assert len(bars) == 1


def test_malformed_rows_are_skipped_rather_than_failing_the_contract():
    engine, _, _, _ = _engine(
        [
            _response(
                {
                    "status": "OK",
                    "results": [
                        {"t": SESSION_ONE, "c": "not-a-number"},
                        _bar(SESSION_TWO),
                    ],
                }
            )
        ]
    )
    bars = engine.get_option_daily_aggregates(
        "O:SPY240906C00540000", date(2024, 9, 1), date(2024, 9, 30)
    )
    assert len(bars) == 1


def test_reversed_window_is_rejected():
    engine, _, _, _ = _engine([_response({"status": "OK", "results": []})])
    with pytest.raises(ValueError):
        engine.get_option_daily_aggregates(
            "O:SPY240906C00540000", date(2024, 9, 6), date(2024, 9, 1)
        )
