"""G106 A2 (Nov 17 night): IV summary per name and tenor from the stored chain capture, behind its switch.

Fixtures are hand-checked: spot 100, r 4%, q 0, observed 2026-11-12 20:57 UTC, so a 30-day expiry has T = 30/365 and
forward 100 * exp(0.04 * 30/365) = 100.3293; the two strikes bracketing it are 99.5 and 100.5."""
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

import realtime.options_iv_summary_read as ivs
from realtime.portal_api import RealtimePortalRepositories, create_realtime_portal_app
from realtime.portal_compatibility_repository import RealtimeOptionFlowRepository

SESSION = date(2026, 11, 12)
OBSERVED = datetime(2026, 11, 12, 20, 57, tzinfo=timezone.utc)
NOW = datetime(2026, 11, 12, 21, 5, tzinfo=timezone.utc)


def _row(kind, strike, delta, iv, *, expiry, bid=None, ask=None, volume=10, oi=100, observed=OBSERVED, spot="100",
         provider_iv=None, provider_delta=None):
    mid = Decimal("1.00")
    return {
        "contract_type": kind, "expiration_date": expiry, "strike_price": Decimal(str(strike)),
        "day_volume": volume, "open_interest": oi,
        "bid": Decimal(str(bid)) if bid is not None else mid - Decimal("0.05"),
        "ask": Decimal(str(ask)) if ask is not None else mid + Decimal("0.05"),
        "implied_volatility": provider_iv if provider_iv is not None else (None if iv is None else iv + 0.01),
        "delta": provider_delta if provider_delta is not None else delta,
        "local_iv": iv, "local_delta": delta,
        "spot": None if spot is None else Decimal(spot), "underlying_price": Decimal("100.05"),
        "risk_free_rate": 0.04, "dividend_yield": 0.0, "observed_at": observed,
    }


def _hand_checked_expiry(expiry=date(2026, 12, 12)):
    """ATM: median(.21, .19, .22, .20) = .205. Calls 25-delta between (0.20, .24) and (0.30, .22) = .23; puts between
    (-0.30, .27) and (-0.20, .29) = .28; risk reversal -.05. Straddle at 100.5 (nearest the forward 100.329):
    call mid 2.1 + put mid 2.4 = 4.5."""
    return [
        _row("call", 90, 0.85, 0.26, expiry=expiry),
        _row("call", 95, 0.70, 0.24, expiry=expiry),
        _row("call", 99.5, 0.53, 0.21, expiry=expiry, bid=2.5, ask=2.7),
        _row("call", 100.5, 0.47, 0.19, expiry=expiry, bid=2.0, ask=2.2, volume=50, oi=1000),
        _row("call", 105, 0.30, 0.22, expiry=expiry),
        _row("call", 108, 0.20, 0.24, expiry=expiry),
        _row("call", 115, 0.08, 0.30, expiry=expiry),
        _row("put", 85, -0.08, 0.35, expiry=expiry),
        _row("put", 92, -0.20, 0.29, expiry=expiry),
        _row("put", 95, -0.30, 0.27, expiry=expiry),
        _row("put", 99.5, -0.47, 0.22, expiry=expiry, bid=1.9, ask=2.1),
        _row("put", 100.5, -0.53, 0.20, expiry=expiry, bid=2.3, ask=2.5, volume=80, oi=3000),
        # 4.7% in the money: excluded from IV comparisons (American put bias), still counted in volume and OI.
        _row("put", 105, -0.70, 0.21, expiry=expiry),
    ]


def _flat_expiry(days, atm, *, observed=OBSERVED):
    """ATM rows (99.5 and 101.5 bracket every forward up to 55 days: 100.605 at 55) all at `atm`; 25-delta call at
    atm+.02 (deltas .30/.20 at atm+.01/.03), put at atm+.06."""
    expiry = observed.date() + timedelta(days=days)
    return [
        _row("call", 99.5, 0.53, atm, expiry=expiry, observed=observed),
        _row("call", 101.5, 0.47, atm, expiry=expiry, observed=observed),
        _row("call", 104, 0.30, atm + 0.01, expiry=expiry, observed=observed),
        _row("call", 107, 0.20, atm + 0.03, expiry=expiry, observed=observed),
        _row("put", 99.5, -0.47, atm, expiry=expiry, observed=observed),
        _row("put", 101.5, -0.53, atm, expiry=expiry, observed=observed),
        _row("put", 96, -0.30, atm + 0.05, expiry=expiry, observed=observed),
        _row("put", 93, -0.20, atm + 0.07, expiry=expiry, observed=observed),
    ]


def _summary(rows, observed=OBSERVED):
    return ivs.compute_iv_summary(rows, underlyer="SPY", session_date=SESSION, observed_at=observed, source="TEST")


def setup_function(_function):
    ivs._CACHE.clear()


def test_switch_is_off_by_default():
    assert ivs.options_iv_summary_read_enabled({}) is False
    assert ivs.options_iv_summary_read_enabled({ivs.OPTIONS_IV_SUMMARY_READ_SWITCH: "on"}) is True
    assert ivs.options_iv_summary_read_enabled({ivs.OPTIONS_IV_SUMMARY_READ_SWITCH: "RECORD"}) is False


def test_total_variance_interpolation_is_hand_checked():
    e5, e12 = date(2026, 11, 17), date(2026, 11, 24)
    # w = .2^2*5 + (.3^2*12 - .2^2*5) * (7-5)/(12-5) = .4514286 (per 365); iv = sqrt(.4514286 / 7) = .2539484
    result = ivs.interpolate_total_variance([(5 / 365, 0.20, e5), (12 / 365, 0.30, e12)], 7 / 365)
    assert result["method"] == "TOTAL_VARIANCE_INTERPOLATION"
    assert result["iv"] == pytest.approx(0.2539484119, abs=1e-9)
    assert result["bracket"] == ["2026-11-17", "2026-11-24"] and result["reasons"] == []
    exact = ivs.interpolate_total_variance([(5 / 365, 0.20, e5), (12 / 365, 0.30, e12)], 12 / 365)
    assert exact["method"] == "EXACT_EXPIRY" and exact["iv"] == 0.30
    inverted = ivs.interpolate_total_variance([(5 / 365, 0.40, e5), (12 / 365, 0.20, e12)], 7 / 365)
    assert inverted["reasons"] == ["CALENDAR_VARIANCE_DECREASING"]  # .8 > .48: still interpolated, flagged


def test_tenor_outside_coverage_is_flat_within_five_days_else_unavailable():
    last = date(2027, 1, 6)
    flat = ivs.interpolate_total_variance([(30 / 365, 0.2, date(2026, 12, 12)), (55 / 365, 0.18, last)], 60 / 365)
    assert flat["method"] == "FLAT_EXTRAPOLATION" and flat["iv"] == 0.18
    assert flat["reasons"] == ["FLAT_EXTRAPOLATION_AFTER_LAST_EXPIRY:5D"]
    gone = ivs.interpolate_total_variance([(30 / 365, 0.2, date(2026, 12, 12)), (50 / 365, 0.18, last)], 60 / 365)
    assert gone["iv"] is None and gone["method"] == "UNAVAILABLE"
    assert gone["reasons"] == ["TENOR_OUTSIDE_COVERAGE_AFTER_LAST_EXPIRY:10D"]
    early = ivs.interpolate_total_variance([(9 / 365, 0.25, date(2026, 11, 21))], 7 / 365)
    assert early["method"] == "FLAT_EXTRAPOLATION" and early["reasons"] == ["FLAT_EXTRAPOLATION_BEFORE_FIRST_EXPIRY:2D"]
    assert ivs.interpolate_total_variance([], 7 / 365)["reasons"] == ["NO_TERM_POINTS"]


def test_atm_25_delta_and_expected_move_on_one_expiry():
    result = _summary(_hand_checked_expiry())
    assert result["available"] is True and result["contract"] == "options-iv-summary-read-v1"
    (expiry,) = result["expiries"]
    assert expiry["calendar_dte"] == 30 and expiry["forward"] == pytest.approx(100.3293, abs=1e-4)
    assert expiry["atm_iv"] == pytest.approx(0.205)
    assert expiry["call_25_delta_iv"] == pytest.approx(0.23)
    assert expiry["put_25_delta_iv"] == pytest.approx(0.28)
    assert expiry["risk_reversal_25_delta"] == pytest.approx(-0.05)
    assert expiry["straddle_strike"] == "100.5" and expiry["straddle_mid"] == "4.5"
    assert expiry["expected_move_fraction"] == pytest.approx(0.045)
    assert expiry["one_sigma_move_fraction"] == pytest.approx(0.205 * (30 / 365) ** 0.5, abs=1e-6)  # .0587717
    # Provider cross-check: the fixture's provider IV is ours + 1 point everywhere.
    assert expiry["provider_atm_iv"] == pytest.approx(0.215)
    thirty = next(tenor for tenor in result["tenors"] if tenor["days"] == 30)
    assert thirty["atm_method"] == "EXACT_EXPIRY" and thirty["atm_iv"] == pytest.approx(0.205)
    assert thirty["risk_reversal_25_delta"] == pytest.approx(-0.05)
    assert thirty["atm_iv_minus_provider"] == pytest.approx(-0.01)
    # Only one expiry, 30 days out: 7 days is 23 days before it and 60 days 30 after it -> unavailable.
    seven, sixty = result["tenors"][0], result["tenors"][2]
    assert seven["atm_iv"] is None and "ATM:TENOR_OUTSIDE_COVERAGE_BEFORE_FIRST_EXPIRY:23D" in seven["quality_reasons"]
    assert sixty["atm_iv"] is None and sixty["risk_reversal_25_delta"] is None
    # Put/call over every contract, the excluded ITM put included: puts 5 x 10 + 80 = 130, calls 6 x 10 + 50 = 110.
    assert result["put_call"]["put_volume"] == 130 and result["put_call"]["call_volume"] == 110
    assert result["put_call"]["volume_ratio"] == pytest.approx(130 / 110, abs=1e-4)
    assert result["put_call"]["open_interest_ratio"] == pytest.approx((5 * 100 + 3000) / (6 * 100 + 1000), abs=1e-4)
    assert result["execution_permission"] is False


def test_tenors_across_expiries_and_same_day_expiry_left_out():
    rows = (_flat_expiry(0, 0.90) + _flat_expiry(5, 0.20) + _flat_expiry(12, 0.30) + _flat_expiry(30, 0.25)
            + _flat_expiry(55, 0.22))
    result = _summary(rows)
    seven, thirty, sixty = result["tenors"]
    assert seven["atm_method"] == "TOTAL_VARIANCE_INTERPOLATION" and seven["atm_iv"] == pytest.approx(0.253948, abs=1e-6)
    assert seven["atm_bracket"] == ["2026-11-17", "2026-11-24"]
    assert thirty["atm_method"] == "EXACT_EXPIRY" and thirty["atm_iv"] == pytest.approx(0.25)
    assert sixty["atm_method"] == "FLAT_EXTRAPOLATION" and sixty["atm_iv"] == pytest.approx(0.22)
    assert "ATM:FLAT_EXTRAPOLATION_AFTER_LAST_EXPIRY:5D" in sixty["quality_reasons"]
    # 25-delta: call = atm + .02, put = atm + .06 at every expiry; risk reversal at 30 days -.04.
    assert thirty["call_25_delta_iv"] == pytest.approx(0.27) and thirty["put_25_delta_iv"] == pytest.approx(0.31)
    assert thirty["risk_reversal_25_delta"] == pytest.approx(-0.04)
    assert thirty["one_sigma_move_fraction"] == pytest.approx(0.25 * (30 / 365) ** 0.5, abs=1e-6)
    assert "SAME_DAY_EXPIRY_NOT_IN_TENORS" in result["quality_reasons"]
    assert result["expiries"][0]["calendar_dte"] == 0 and len(result["expiries"]) == 5


def test_missing_data_says_why():
    rows = _hand_checked_expiry()
    unpriced = [dict(row, local_iv=None, local_delta=None) for row in rows]
    assert _summary(unpriced)["reason"] == "NO_PRICED_CONTRACTS"
    nospot = [dict(row, spot=None, underlying_price=None) for row in rows]
    assert _summary(nospot)["reason"] == "SPOT_UNAVAILABLE"
    provider_spot = _summary([dict(row, spot=None) for row in rows])
    assert provider_spot["spot"] == "100.05" and "SPOT_FROM_PROVIDER" in provider_spot["quality_reasons"]
    # No 25-delta call neighbours, an unquoted ATM put, one unpriced contract.
    thin = [row for row in rows if not (row["contract_type"] == "call" and row["strike_price"] in (105, 108))]
    thin = [dict(row, bid=None, ask=None) if row["contract_type"] == "put" and row["strike_price"] == Decimal("100.5")
            else row for row in thin]
    thin.append(dict(_row("call", 112, None, None, expiry=date(2026, 12, 12))))
    result = _summary(thin)
    (expiry,) = result["expiries"]
    assert expiry["call_25_delta_iv"] is None and "CALL_25_DELTA_IV_INSUFFICIENT" in expiry["quality_reasons"]
    assert expiry["straddle_strike"] == "99.5"  # 100.5 has no put quote; the next nearest strike with both
    assert "LOCAL_IV_OR_DELTA_MISSING:1" in result["quality_reasons"]
    assert ivs.compute_iv_summary([], underlyer="SPY", session_date=SESSION, observed_at=OBSERVED,
                                  source="TEST")["reason"] == "NO_CHAIN_CAPTURE"


class _Cursor:
    """Answers the three reads of `iv_summary_from_breadth_cursor` (and the daily read) from fixture rows."""

    def __init__(self, rows, *, dict_rows=False, session=SESSION):
        self.rows, self.dict_rows, self.session = rows, dict_rows, session
        self.statements, self._one, self._all = [], None, []

    def execute(self, statement, params=()):
        self.statements.append(statement)
        if statement.startswith("SET"):
            return
        if statement.startswith("SELECT max(session_date)"):
            self._one = {"max": self.session} if self.dict_rows else (self.session,)
        elif statement.startswith("SELECT max(observed_at),count(*)"):
            observed = max((row["observed_at"] for row in self.rows), default=None)
            self._one = {"max": observed, "count": len(self.rows)} if self.dict_rows else (observed, len(self.rows))
        elif "FROM rt.option_breadth_current" in statement and "observed_at=%s" in statement:
            wanted = params[2]
            picked = [row for row in self.rows if row["observed_at"] == wanted]
            self._all = [({name: row[name] for name in ivs.ROW_NAMES} if self.dict_rows
                          else tuple(row[name] for name in ivs.ROW_NAMES)) for row in picked]
        elif "FROM rt.option_breadth_daily" in statement:
            self._all = [tuple(row[name] for name in ivs.ROW_NAMES) for row in self.rows]
        else:
            raise AssertionError(f"unexpected SQL {statement}")

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all


def test_latest_capture_only_and_one_computation_per_capture():
    rows = _hand_checked_expiry()
    stale = [dict(row, observed_at=datetime(2026, 11, 12, 15, 0, tzinfo=timezone.utc), spot=Decimal("90"),
                  strike_price=Decimal("70"), local_iv=0.9) for row in rows[:3]]
    cursor = _Cursor(stale + rows)
    first = ivs.iv_summary_from_breadth_cursor(cursor, underlyer="SPY", available_by=NOW)
    assert first["spot"] == "100" and first["contracts"] == len(rows) and first["observed_at"] == OBSERVED.isoformat()
    again = ivs.iv_summary_from_breadth_cursor(_Cursor(stale + rows), underlyer="SPY", available_by=NOW)
    assert again is first
    later = [dict(row, observed_at=OBSERVED + timedelta(minutes=5)) for row in rows]
    assert ivs.iv_summary_from_breadth_cursor(_Cursor(later), underlyer="SPY", available_by=NOW) is not first
    empty = _Cursor([], session=None)
    assert ivs.iv_summary_from_breadth_cursor(empty, underlyer="SPY", available_by=NOW)["reason"] == "NO_CHAIN_CAPTURE"


def test_past_session_from_the_daily_snapshot():
    result = ivs.iv_summary_from_daily_cursor(_Cursor(_hand_checked_expiry()), underlyer="SPY", session_date=SESSION)
    assert result["available"] is True and result["source"] == "DAILY_SNAPSHOT"
    assert result["expiries"][0]["atm_iv"] == pytest.approx(0.205)
    missing = ivs.iv_summary_from_daily_cursor(_Cursor([]), underlyer="SPY", session_date=SESSION)
    assert missing["reason"] == "NO_DAILY_SNAPSHOT"


class _Store:
    def __init__(self, rows):
        self.cursor_object = _Cursor(rows, dict_rows=True)

    @contextmanager
    def cursor(self):
        yield self.cursor_object


def _client(rows):
    store = _Store(rows)
    app = create_realtime_portal_app(
        RealtimePortalRepositories(None, None, None, option_flow=RealtimeOptionFlowRepository(store.cursor)),
        clock=lambda: NOW,
    )
    return TestClient(app), store


def test_route_is_404_while_the_switch_is_off(monkeypatch):
    monkeypatch.delenv(ivs.OPTIONS_IV_SUMMARY_READ_SWITCH, raising=False)
    client, store = _client(_hand_checked_expiry())
    response = client.get("/api/options/iv-summary", params={"underlyer": "SPY"})
    assert response.status_code == 404 and store.cursor_object.statements == []


def test_route_serves_the_summary_when_on(monkeypatch):
    monkeypatch.setenv(ivs.OPTIONS_IV_SUMMARY_READ_SWITCH, "ON")
    client, store = _client(_hand_checked_expiry())
    response = client.get("/api/options/iv-summary", params={"underlyer": "spy"})
    assert response.status_code == 200
    payload = response.json()
    assert payload["contract"] == "options-iv-summary-read-v1" and payload["underlyer"] == "SPY"
    assert [tenor["days"] for tenor in payload["tenors"]] == [7, 30, 60]
    assert payload["expiries"][0]["atm_iv"] == pytest.approx(0.205)
    assert store.cursor_object.statements[0].startswith("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
    bad = client.get("/api/options/iv-summary", params={"underlyer": "SPY;DROP"})
    assert bad.status_code == 422
