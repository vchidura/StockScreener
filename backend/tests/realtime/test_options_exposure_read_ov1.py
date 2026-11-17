"""G106 OV1 (Nov 16): Options Overview exposure read (gamma and delta per strike, expiry and strike x expiry) from the
stored chain capture, behind its switch. Expected values are computed here by hand from the Black-Scholes formulas."""
import math
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal

import numpy as np
import pytest
from fastapi.testclient import TestClient

import realtime.options_exposure_read as exposure
from options.analytics.gamma_exposure import GammaExposureInput, solve_gamma_flip
from options.analytics.greeks import black_scholes_delta, price_and_greeks
from options.domain import ContractType, DealerConvention
from realtime.portal_api import RealtimePortalRepositories, create_realtime_portal_app
from realtime.portal_compatibility_repository import RealtimeOptionFlowRepository

SESSION = date(2026, 11, 12)
OBSERVED = datetime(2026, 11, 12, 19, 5, 30, tzinfo=timezone.utc)  # 14:05 New York
AFTER_CLOSE = datetime(2026, 11, 12, 21, 5, 30, tzinfo=timezone.utc)  # 16:05 New York
NOW = datetime(2026, 11, 12, 21, 30, tzinfo=timezone.utc)
WEEK = date(2026, 11, 20)  # 8 calendar days: T = 8 / 365 (the capture's whole-day convention)
SPOT = Decimal("100")
ON = {exposure.OPTIONS_EXPOSURE_READ_SWITCH: "ON"}


def contract(kind, strike, oi, *, expiry=WEEK, iv=0.25, volume=0, spot=SPOT, observed=OBSERVED, provider_iv=None,
             provider_gamma=None, provider_delta=None, underlying="SPY", rate=0.04, dividend=0.0):
    return dict(session_date=SESSION, underlying=underlying, contract_type=kind, expiration_date=expiry,
                strike_price=Decimal(str(strike)), open_interest=oi, day_volume=volume, local_iv=iv,
                implied_volatility=provider_iv, gamma=provider_gamma, delta=provider_delta, risk_free_rate=rate,
                dividend_yield=dividend, spot=spot, underlying_price=spot, observed_at=observed)


def hand(kind, strike, *, spot=100.0, days=8, iv=0.25, rate=0.04, dividend=0.0):
    """Black-Scholes gamma and delta, written out (no project code)."""
    years = days / 365.0
    d1 = (math.log(spot / strike) + (rate - dividend + 0.5 * iv * iv) * years) / (iv * math.sqrt(years))
    pdf = math.exp(-0.5 * d1 * d1) / math.sqrt(2 * math.pi)
    cdf = 0.5 * (1 + math.erf(d1 / math.sqrt(2)))
    carry = math.exp(-dividend * years)
    gamma = carry * pdf / (spot * iv * math.sqrt(years))
    return gamma, carry * cdf if kind == "call" else carry * (cdf - 1)


class Breadth:
    """`rt.option_breadth_current` and `rt.option_breadth_captures` for the statements the read issues."""

    def __init__(self, rows, captures=None):
        self.rows = rows
        self.captures = captures if captures is not None else sorted(
            {(row["underlying"], row["observed_at"]) for row in rows})
        self.capture_states = {}
        self.statements = []
        self.result = []

    @contextmanager
    def cursor(self):
        yield self

    def execute(self, sql, params=()):
        self.statements.append(sql)
        self.result = []
        if sql.startswith("SET"):
            return
        if sql.startswith("SELECT max(session_date)"):
            underlyer, available_by = params
            found = [row["session_date"] for row in self.rows
                     if row["underlying"] == underlyer and row["observed_at"] <= available_by]
            self.result = [(max(found) if found else None,)]
        elif sql.startswith("SELECT DISTINCT underlying FROM rt.option_breadth_captures"):
            self.result = [(name,) for name in sorted({name for name, _ in self.captures})]
        elif "GROUP BY observed_at" in sql:
            session, underlyer, available_by = params
            counts = {}
            for row in self.rows:
                if row["session_date"] == session and row["underlying"] == underlyer and row["observed_at"] <= available_by:
                    counts[row["observed_at"]] = counts.get(row["observed_at"], 0) + 1
            self.result = sorted(counts.items(), reverse=True)
        elif sql.startswith("SELECT state,observed_at,contracts,reason FROM rt.option_breadth_captures"):
            session, underlyer, available_by = params
            found = sorted(at for name, at in self.captures if name == underlyer and at <= available_by)
            if found:
                state, reason = self.capture_states.get(found[-1], ("COMPLETE", None))
                self.result = [(state, found[-1], 0, reason)]
        elif sql.startswith("SELECT contract_type,"):
            session, underlyer, observed = params
            self.result = [tuple(row[name] for name in exposure._ROW_NAMES) for row in self.rows
                           if row["session_date"] == session and row["underlying"] == underlyer
                           and row["observed_at"] == observed]
        else:
            raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self):
        return self.result[0] if self.result else None

    def fetchall(self):
        return list(self.result)


def read(rows, *, environment=ON, available_by=NOW, **options):
    return exposure.exposure_from_breadth_cursor(Breadth(rows), underlyer=rows[0]["underlying"] if rows else "SPY",
                                                 available_by=available_by, environment=environment, **options)


def chain(rows, observed=OBSERVED):
    return exposure.build_exposure_chain(rows, underlyer="SPY", session_date=SESSION, observed_at=observed)


def ladder(*, expiries=(SESSION, date(2026, 11, 15), date(2026, 12, 2)), observed=OBSERVED, put_scale=1):
    rows = []
    for expiry in expiries:
        for strike in range(80, 121, 5):
            rows.append(contract("call", strike, 1000 + 400 * max(0, strike - 100), expiry=expiry, observed=observed,
                                 volume=50))
            rows.append(contract("put", strike, put_scale * (1000 + 400 * max(0, 100 - strike)), expiry=expiry,
                                 observed=observed, volume=70))
    return rows


def setup_function(_function):
    exposure._CHAIN_CACHE.clear()
    exposure._RESULT_CACHE.clear()


def test_switches_default_off_and_greeks_flag():
    assert exposure.options_exposure_read_enabled({}) is False
    assert exposure.options_exposure_read_enabled({exposure.OPTIONS_EXPOSURE_READ_SWITCH: "on"}) is True
    assert exposure.enabled_greeks({}) == ("gamma", "delta")
    assert exposure.enabled_greeks({exposure.OPTIONS_EXPOSURE_GREEKS_SWITCH: "gamma"}) == ("gamma",)
    # Phase 2 Greeks are not implemented yet: naming them in the flag does not serve them.
    assert exposure.enabled_greeks({exposure.OPTIONS_EXPOSURE_GREEKS_SWITCH: "gamma,delta,charm,vanna"}) == ("gamma", "delta")
    with pytest.raises(ValueError, match="not enabled"):
        read(ladder(), greek="delta", environment={exposure.OPTIONS_EXPOSURE_GREEKS_SWITCH: "gamma"})
    with pytest.raises(ValueError, match="not enabled"):
        read(ladder(), greek="charm")
    with pytest.raises(ValueError, match="Invalid"):
        read(ladder(), scope="month")


def test_vectorized_delta_and_recomputed_greeks_equal_the_local_model():
    for kind in ("call", "put"):
        for strike, dividend in ((90.0, 0.0), (104.0, 0.013)):
            _price, scalar_delta, scalar_gamma, *_rest = price_and_greeks(
                ContractType.CALL if kind == "call" else ContractType.PUT, spot=100.0, strike=strike, maturity=8 / 365,
                rate=0.04, dividend=dividend, volatility=0.3)
            got = black_scholes_delta(np.array([100.0]), np.array([strike]), np.array([8 / 365]), np.array([0.04]),
                                      np.array([dividend]), np.array([0.3]), np.array([kind == "call"]))[0]
            assert got == pytest.approx(scalar_delta, rel=1e-12)
            built = chain([contract(kind, strike, 10, iv=0.3, dividend=dividend)])
            assert built.delta[0] == pytest.approx(scalar_delta, rel=1e-12)
            assert built.gamma[0] == pytest.approx(scalar_gamma, rel=1e-12)


def test_units_and_dealer_signs_by_hand():
    rows = [contract("call", 105, 1000), contract("put", 95, 2000)]
    call_gamma, call_delta = hand("call", 105)
    put_gamma, put_delta = hand("put", 95)
    gamma = exposure.compute_exposure(chain(rows), greek="gamma", weight="oi", scope="all")
    by_strike = {row["strike"]: row for row in gamma["strikes"]}
    # Dollars of dealer delta change per 1% move: gamma x OI x 100 x spot^2 x 0.01; dealers long calls (+), short puts (-).
    assert by_strike["105"]["call"] == pytest.approx(call_gamma * 1000 * 100 * 100 ** 2 * 0.01, abs=1)
    assert by_strike["95"]["put"] == pytest.approx(-put_gamma * 2000 * 100 * 100 ** 2 * 0.01, abs=1)
    assert by_strike["95"]["call"] == 0 and by_strike["105"]["put"] == 0
    assert gamma["totals"]["net"] == by_strike["105"]["net"] + by_strike["95"]["net"]
    assert gamma["totals"]["net_shares"] == pytest.approx(gamma["totals"]["net"] / 100, abs=1)
    assert gamma["units"] == exposure.UNITS["gamma"] and gamma["execution_permission"] is False
    delta = exposure.compute_exposure(chain(rows), greek="delta", weight="oi", scope="all")
    by_strike = {row["strike"]: row for row in delta["strikes"]}
    # Dollars of dealer delta: delta x OI x 100 x spot. Long calls and short puts are both long delta.
    assert by_strike["105"]["call"] == pytest.approx(call_delta * 1000 * 100 * 100, abs=1) and by_strike["105"]["call"] > 0
    assert by_strike["95"]["put"] == pytest.approx(-put_delta * 2000 * 100 * 100, abs=1) and by_strike["95"]["put"] > 0
    assert delta["totals"]["absolute"] == pytest.approx(by_strike["105"]["call"] + by_strike["95"]["put"], abs=1)


@pytest.mark.parametrize("greek", ["gamma", "delta"])
def test_scopes_and_every_view_adds_up(greek):
    built = chain(ladder())
    results = {scope: exposure.compute_exposure(built, greek=greek, weight="oi", scope=scope) for scope in exposure.SCOPES}
    assert [row["expiration_date"] for row in results["0dte"]["expiries"]] == ["2026-11-12"]
    assert [row["calendar_dte"] for row in results["7d"]["expiries"]] == [0, 3]
    assert [row["calendar_dte"] for row in results["all"]["expiries"]] == [0, 3, 20]
    for result in results.values():
        total = result["totals"]["net"]
        tolerance = len(result["grid"]) * 2
        assert sum(row["net"] for row in result["strikes"]) == pytest.approx(total, abs=tolerance)
        assert sum(row["net"] for row in result["expiries"]) == pytest.approx(total, abs=tolerance)
        assert sum(call + put for _, _, call, put in result["grid"]) == pytest.approx(total, abs=tolerance)
        assert sum(row["share"] for row in result["expiries"]) == pytest.approx(1, abs=1e-3)
        strikes, expiries = result["strikes"], result["expiries"]
        assert all(0 <= s < len(strikes) and 0 <= e < len(expiries) for s, e, _, _ in result["grid"])
    # The share expiring today does not depend on the scope shown.
    everything = results["all"]
    today = everything["expiries"][0]["absolute"] / everything["totals"]["absolute"]
    assert {round(result["expiring_today_share"], 4) for result in results.values()} == {round(today, 4)}


def test_open_interest_plus_volume_weight_is_an_estimate():
    rows = [contract("call", 100, 0, volume=500), contract("put", 100, 1000, volume=200)]
    call_gamma, _ = hand("call", 100)
    put_gamma, _ = hand("put", 100)
    scale = 100 * 100 ** 2 * 0.01
    by_oi = exposure.compute_exposure(chain(rows), greek="gamma", weight="oi", scope="all")
    assert by_oi["totals"]["net"] == pytest.approx(-put_gamma * 1000 * scale, abs=1)
    assert by_oi["coverage"]["skipped"]["zero_weight"] == 1
    assert "VOLUME_WEIGHT_ESTIMATE" not in by_oi["quality_reasons"]
    estimate = exposure.compute_exposure(chain(rows), greek="gamma", weight="oi_volume", scope="all")
    assert estimate["totals"]["net"] == pytest.approx((call_gamma * 500 - put_gamma * 1200) * scale, abs=1)
    assert estimate["coverage"]["skipped"]["zero_weight"] == 0
    assert "VOLUME_WEIGHT_ESTIMATE" in estimate["quality_reasons"]


def test_flip_and_regime_reuse_the_gamma_solver():
    rows = ladder()
    result = exposure.compute_exposure(chain(rows), greek="gamma", weight="oi", scope="all")
    levels = result["levels"]
    # The same contracts, weights and times through `solve_gamma_flip` directly.
    built = chain(rows)
    inputs = tuple(GammaExposureInput(
        contract_id=index, contract_type=ContractType.CALL if row["contract_type"] == "call" else ContractType.PUT,
        expiration_date=row["expiration_date"], strike=row["strike_price"], open_interest=row["open_interest"],
        local_iv=0.25, time_to_expiration_years=float(built.maturity[index]), risk_free_rate=0.04, dividend_yield=0.0,
    ) for index, row in enumerate(rows))
    flip = solve_gamma_flip(inputs, SPOT, convention=DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS)
    assert levels["flip_spot"] == str(round(flip.flip_spot, 2))
    assert levels["regime_at_spot"] == flip.regime_at_spot.value
    # Bars and levels come from one model: the profile's net equals the bars' net.
    assert levels["net_gamma_notional_per_percent"] == pytest.approx(result["totals"]["net"], abs=2)
    heavy_puts = exposure.compute_exposure(chain(ladder(put_scale=3)), greek="gamma", weight="oi", scope="all")
    assert heavy_puts["levels"]["regime_at_spot"] == "NEGATIVE_GAMMA" and heavy_puts["totals"]["net"] < 0
    assert heavy_puts["levels"]["flip_spot"] is None or Decimal(heavy_puts["levels"]["flip_spot"]) > SPOT


def test_call_and_put_walls_tag_the_largest_levels_in_every_greek_view():
    rows = [contract(kind, strike, 300) for strike in range(90, 111, 5) for kind in ("call", "put")]
    rows += [contract("call", 105, 9000), contract("put", 95, 7000)]
    gamma = exposure.compute_exposure(chain(rows), greek="gamma", weight="oi", scope="all")
    delta = exposure.compute_exposure(chain(rows), greek="delta", weight="oi", scope="all")
    assert gamma["levels"]["call_wall"]["strike"] == "105" and gamma["levels"]["put_wall"]["strike"] == "95"
    assert gamma["levels"]["call_wall"]["distance_to_spot_fraction"] == 0.05
    assert delta["levels"] == gamma["levels"]
    tags = {row["strike"]: row["tags"] for row in gamma["largest_levels"]}
    assert "CALL_WALL" in tags["105"] and "PUT_WALL" in tags["95"]
    nets = [abs(row["net"]) for row in gamma["largest_levels"]]
    assert nets == sorted(nets, reverse=True) and len(nets) <= exposure.LARGEST_LEVELS


def test_skipped_contracts_are_counted_by_reason():
    rows = ladder(expiries=(WEEK,))
    rows += [
        contract("call", 101, 0),  # zero open interest
        contract("put", 99, 400, iv=None),  # no IV from us or the provider (deep in the money below intrinsic)
        contract("call", 102, 300, iv=None, provider_iv=0.31),  # our IV missing: the provider's IV, same model
    ]
    result = exposure.compute_exposure(chain(rows), greek="gamma", weight="oi", scope="all")
    coverage = result["coverage"]
    assert coverage["skipped"] == {"zero_weight": 1, "expired": 0, "no_greek": 1}
    assert coverage["provider_iv_contracts"] == 1 and "PROVIDER_IV_FALLBACK" in result["quality_reasons"]
    assert coverage["contributing_contracts"] == len(rows) - 2
    assert coverage["weight_coverage_fraction"] == pytest.approx(1 - 400 / coverage["weight_in_scope"], abs=1e-4)
    assert coverage["strike_low"] == "80" and coverage["strike_high"] == "120" and coverage["expiry_count"] == 1


def test_empty_and_expired_scopes():
    no_today = exposure.compute_exposure(chain(ladder(expiries=(WEEK,))), greek="gamma", weight="oi", scope="0dte")
    assert no_today["available"] is False and no_today["reason"] == "NO_CONTRACTS_IN_SCOPE"
    assert no_today["coverage"]["capture_contracts"] > 0
    # After 16:00 New York today's expiries are over: the close capture has no live 0 DTE contract.
    closed = exposure.compute_exposure(chain(ladder(observed=AFTER_CLOSE), observed=AFTER_CLOSE),
                                       greek="gamma", weight="oi", scope="0dte")
    assert closed["available"] is False and closed["reason"] == "SCOPE_EXPIRED"
    everything = exposure.compute_exposure(chain(ladder(observed=AFTER_CLOSE), observed=AFTER_CLOSE),
                                           greek="gamma", weight="oi", scope="all")
    assert everything["available"] is True and everything["coverage"]["skipped"]["expired"] == 18
    assert everything["expiring_today_share"] == 0.0


def test_unavailable_capture_reasons():
    empty = Breadth([])
    result = exposure.exposure_from_breadth_cursor(empty, underlyer="SPY", available_by=NOW, environment=ON)
    assert result["available"] is False and result["reason"] == "NO_CHAIN_CAPTURE"
    no_spot = [dict(row, spot=None, underlying_price=None) for row in ladder()]
    assert read(no_spot)["reason"] == "SPOT_UNAVAILABLE"


def test_latest_capture_only_and_one_computation_per_capture():
    rows = ladder()
    stale = [dict(row, observed_at=datetime(2026, 11, 12, 15, 11, tzinfo=timezone.utc), strike_price=Decimal(60),
                  open_interest=999999, spot=Decimal(90), underlying_price=Decimal(90)) for row in rows[:4]]
    store = Breadth(stale + rows)
    first = exposure.exposure_from_breadth_cursor(store, underlyer="SPY", available_by=NOW, environment=ON)
    assert first["observed_at"] == OBSERVED.isoformat() and first["spot"] == "100"
    assert all(row["strike"] != "60" for row in first["strikes"])
    assert first["coverage"]["earlier_capture_rows_excluded"] == 4
    assert first["underlyers"] == ["SPY"] and first["greeks_enabled"] == ["gamma", "delta"]
    again = exposure.exposure_from_breadth_cursor(store, underlyer="SPY", available_by=NOW, environment=ON)
    assert again["totals"] is first["totals"]
    delta = exposure.exposure_from_breadth_cursor(store, underlyer="SPY", available_by=NOW, greek="delta", environment=ON)
    assert delta["greek"] == "delta"
    # Rows are fetched once per capture, whatever the Greek, weight or scope.
    assert sum(sql.startswith("SELECT contract_type,") for sql in store.statements) == 1
    later = [dict(row, observed_at=datetime(2026, 11, 12, 19, 10, 30, tzinfo=timezone.utc)) for row in rows]
    store.rows = stale + rows + later
    store.captures = sorted({("SPY", row["observed_at"]) for row in store.rows})
    newer = exposure.exposure_from_breadth_cursor(store, underlyer="SPY", available_by=NOW, environment=ON)
    assert newer["observed_at"] == later[0]["observed_at"].isoformat() and newer["totals"] is not first["totals"]


def test_a_newer_capture_that_did_not_complete_is_named():
    rows = ladder()
    store = Breadth(rows)
    failed_at = datetime(2026, 11, 12, 19, 10, 40, tzinfo=timezone.utc)
    store.captures = [("SPY", OBSERVED), ("SPY", failed_at)]
    store.capture_states[failed_at] = ("FAILED", "PROVIDER_TIMEOUT")
    result = exposure.exposure_from_breadth_cursor(store, underlyer="SPY", available_by=NOW, environment=ON)
    assert result["observed_at"] == OBSERVED.isoformat()
    assert result["capture"] == {"latest_capture_state": "FAILED", "latest_capture_at": failed_at.isoformat(),
                                 "latest_capture_reason": "PROVIDER_TIMEOUT"}
    assert result["quality_reasons"][0] == "LATEST_CAPTURE_NOT_COMPLETE"


def test_provider_greeks_cross_check():
    def with_provider(factor, missing=False):
        rows = []
        for strike in range(90, 111, 5):
            for kind in ("call", "put"):
                gamma, delta = hand(kind, strike)
                rows.append(contract(kind, strike, 1000, provider_gamma=gamma * factor, provider_delta=delta * factor))
        if missing:
            rows[0]["gamma"] = None
        return exposure.compute_exposure(chain(rows), greek="gamma", weight="oi", scope="all")

    same = with_provider(1.0)
    # Equal call and put open interest at every strike: the net is about zero, so the check is made on gross exposure.
    assert same["cross_check"]["difference_fraction"] == pytest.approx(0, abs=1e-6)
    assert same["cross_check"]["net_difference_fraction"] == pytest.approx(0, abs=1e-6)
    assert same["cross_check"]["contracts_compared"] == 10
    assert same["cross_check"]["local_absolute"] == pytest.approx(same["cross_check"]["provider_absolute"], abs=2)
    assert "PROVIDER_CROSS_CHECK_DIVERGES" not in same["quality_reasons"]
    doubled = with_provider(2.0, missing=True)
    assert doubled["cross_check"]["difference_fraction"] == pytest.approx(-0.5, abs=1e-3)
    assert doubled["cross_check"]["provider_missing_contracts"] == 1
    assert "PROVIDER_CROSS_CHECK_DIVERGES" in doubled["quality_reasons"]


def test_spot_against_the_underlying_price_on_the_quotes():
    def with_quote(prices):
        rows = ladder()
        for row, price in zip(rows, prices * len(rows)):
            row["underlying_price"] = price
        return exposure.compute_exposure(chain(rows), greek="gamma", weight="oi", scope="all")

    same = with_quote([SPOT])
    assert same["coverage"]["spot_check"] == {"spot": "100", "quote_underlying_price": "100", "gap_fraction": 0.0}
    assert "SPOT_QUOTE_GAP" not in same["quality_reasons"]
    # Nov 12 QQQ: spot from the 11:05 bar, quotes two minutes later. The median of the rows' prices is the reference.
    behind = with_quote([Decimal("100.17"), Decimal("100.17"), Decimal("100.30")])
    assert behind["coverage"]["spot_check"]["quote_underlying_price"] == "100.17"
    assert behind["coverage"]["spot_check"]["gap_fraction"] == pytest.approx(100 / 100.17 - 1, abs=1e-5)
    assert "SPOT_QUOTE_GAP" in behind["quality_reasons"]
    within = with_quote([Decimal("100.09")])
    assert "SPOT_QUOTE_GAP" not in within["quality_reasons"]
    unknown = with_quote([None])
    assert unknown["coverage"]["spot_check"] == {"spot": "100", "quote_underlying_price": None, "gap_fraction": None}
    assert "SPOT_QUOTE_GAP" not in unknown["quality_reasons"]


def _app(store):
    return create_realtime_portal_app(
        RealtimePortalRepositories(None, None, None, option_flow=RealtimeOptionFlowRepository(store.cursor)),
        clock=lambda: NOW,
    )


def test_route_is_off_by_default(monkeypatch):
    monkeypatch.delenv(exposure.OPTIONS_EXPOSURE_READ_SWITCH, raising=False)
    with TestClient(_app(Breadth(ladder()))) as client:
        assert client.get("/api/options/exposure", params={"underlyer": "SPY"}).status_code == 404


def test_route_contract(monkeypatch):
    monkeypatch.setenv(exposure.OPTIONS_EXPOSURE_READ_SWITCH, "ON")
    monkeypatch.delenv(exposure.OPTIONS_EXPOSURE_GREEKS_SWITCH, raising=False)
    store = Breadth(ladder())
    with TestClient(_app(store)) as client:
        response = client.get("/api/options/exposure", params={"underlyer": "spy", "greek": "gamma", "weight": "oi",
                                                               "scope": "7d"})
        assert response.status_code == 200
        body = response.json()
        assert body["contract"] == "options-exposure-read-v1" and body["available"] is True
        assert body["underlyer"] == "SPY" and body["scope"] == "7d" and body["execution_permission"] is False
        assert set(body) >= {"session_date", "observed_at", "spot", "convention", "units", "totals", "levels", "strikes",
                             "expiries", "grid", "largest_levels", "expiring_today_share", "coverage", "cross_check",
                             "quality_reasons", "definitions", "greeks_enabled", "underlyers", "capture"}
        assert set(body["levels"]) >= {"flip_spot", "regime_at_spot", "call_wall", "put_wall", "gamma_walls"}
        assert set(body["strikes"][0]) == {"strike", "call", "put", "net", "call_open_interest", "put_open_interest"}
        assert client.get("/api/options/exposure", params={"underlyer": "SPY", "greek": "charm"}).status_code == 422
        assert client.get("/api/options/exposure", params={"underlyer": "SPY", "scope": "month"}).status_code == 422
        assert client.get("/api/options/exposure", params={"underlyer": "SPY", "weight": "volume"}).status_code == 422
        assert client.get("/api/options/exposure", params={"underlyer": "S P Y"}).status_code == 422
        past = client.get("/api/options/exposure", params={"underlyer": "SPY", "session_date": "2026-11-12"}).json()
        assert past["session_date"] == "2026-11-12" and past["available"] is True
