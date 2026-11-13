"""G106 A3 (Nov 12 night): gamma exposure / OI walls read from the stored chain capture, behind its switch."""
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock

import realtime.options_gamma_read as gamma

NOW = datetime(2026, 11, 12, 19, 7, tzinfo=timezone.utc)
SESSION = date(2026, 11, 12)
OBSERVED = datetime(2026, 11, 12, 19, 5, 30, tzinfo=timezone.utc)


def _chain(spot=Decimal("100"), today_too=True):
    rows = []
    expiries = [date(2026, 11, 20), date(2026, 12, 18)] + ([SESSION] if today_too else [])
    for expiry in expiries:
        for strike in range(80, 121, 5):
            for kind in ("call", "put"):
                oi = 9000 if strike == 100 else (4000 if strike in (95, 105) else 300 + (strike % 7) * 40)
                rows.append(dict(contract_type=kind, expiration_date=expiry, strike_price=Decimal(strike),
                                 open_interest=oi, local_iv=0.25, implied_volatility=0.26, risk_free_rate=0.04,
                                 dividend_yield=0.0, spot=spot, underlying_price=spot, observed_at=OBSERVED))
    return rows


def _cursor(rows, session=SESSION):
    cursor = MagicMock()
    cursor.fetchone.return_value = (session,)
    cursor.fetchall.return_value = [tuple(row[name] for name in (
        "contract_type", "expiration_date", "strike_price", "open_interest", "local_iv", "implied_volatility",
        "risk_free_rate", "dividend_yield", "spot", "underlying_price", "observed_at")) for row in rows]
    return cursor


def setup_function(_function):
    gamma._CACHE.clear()


def test_switch_is_off_by_default():
    assert gamma.options_gamma_read_enabled({}) is False
    assert gamma.options_gamma_read_enabled({gamma.OPTIONS_GAMMA_READ_SWITCH: "on"}) is True


def test_profile_walls_and_zero_dte_from_a_chain():
    result = gamma.gamma_from_breadth_cursor(_cursor(_chain()), underlyer="SPY", available_by=NOW)
    assert result["available"] is True and result["contract"] == "options-gamma-read-v1"
    total = result["total"]
    assert total["peak_gamma_strike"] == "100"
    assert total["regime_at_spot"] in {"POSITIVE_GAMMA", "NEGATIVE_GAMMA", "UNDETERMINED"}
    assert total["gamma_walls"] and total["gamma_walls"][0]["strike"] == "100"
    assert result["oi_walls"] and result["oi_walls"][0]["center_strike"] in {"95", "100", "105"}
    assert result["zero_dte"] is not None and result["execution_permission"] is False
    assert result["convention"] == "DEALER_LONG_CALLS_SHORT_PUTS"


def test_unavailable_reasons():
    empty = _cursor([])
    empty.fetchone.return_value = (None,)
    assert gamma.gamma_from_breadth_cursor(empty, underlyer="SPY", available_by=NOW)["reason"] == "NO_CHAIN_CAPTURE"
    few = _chain()[:10]
    assert gamma.gamma_from_breadth_cursor(_cursor(few), underlyer="SPY", available_by=NOW)["reason"] == "TOO_FEW_CONTRACTS"
    nospot = [dict(row, spot=None, underlying_price=None) for row in _chain()]
    assert gamma.gamma_from_breadth_cursor(_cursor(nospot), underlyer="SPY", available_by=NOW)["reason"] == "SPOT_UNAVAILABLE"


def test_one_computation_per_capture():
    rows = _chain(today_too=False)
    first = gamma.gamma_from_breadth_cursor(_cursor(rows), underlyer="QQQ", available_by=NOW)
    again = gamma.gamma_from_breadth_cursor(_cursor(rows), underlyer="QQQ", available_by=NOW)
    assert again is first and first["zero_dte"] is None
    later = [dict(row, observed_at=datetime(2026, 11, 12, 19, 10, 30, tzinfo=timezone.utc)) for row in rows]
    assert gamma.gamma_from_breadth_cursor(_cursor(later), underlyer="QQQ", available_by=NOW) is not first


def test_rows_left_from_earlier_captures_are_not_used():
    rows = _chain(today_too=False)
    old = datetime(2026, 11, 12, 15, 11, tzinfo=timezone.utc)
    stale = [dict(row, observed_at=old, spot=Decimal("90"), underlying_price=Decimal("90"), strike_price=Decimal(60),
                  open_interest=999999) for row in rows[:4]]
    result = gamma.gamma_from_breadth_cursor(_cursor(stale + rows), underlyer="IWM", available_by=NOW)
    assert result["spot"] == "100" and result["observed_at"] == OBSERVED.isoformat()
    assert all(wall["center_strike"] != "60" for wall in result["oi_walls"])
