"""OA1, OA2 and OF3 (Nov 12, ticker page review): alert leg IV/Greeks at the source time, the model exit estimate,
and Flow rows without a catalog id."""
from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import uuid4

import pytest

from options.analytics.greeks import price_and_greeks
from options.domain import ContractType
from realtime import option_alert_valuation as valuation

TRIGGER = datetime(2026, 11, 11, 14, 43, 33, 710000, tzinfo=timezone.utc)
INPUT = {"ticker": "NVDA", "expiration_date": "2026-11-11", "risk_free_rate": "0.0405", "dividend_yield": "0.0002",
         "model_version": "black-scholes-merton-v1"}


def _mark(kind, spot, strike, at, expiration, iv=0.42, rate=0.0405, dividend=0.0002):
    years = valuation.time_to_expiration_years(expiration, at)
    return price_and_greeks(ContractType(kind), spot=spot, strike=strike, maturity=years, rate=rate,
                            dividend=dividend, volatility=iv)[0]


def _row(**leg):
    mark = _mark("PUT", 173.085, 172.5, TRIGGER, date(2026, 11, 11))
    return {
        "evaluation_id": str(uuid4()), "underlyer": "NVDA", "session_date": "2026-11-11", "detector_id": "O1",
        "triggered_at": TRIGGER.isoformat(), "exit_deadline": "2026-11-11T21:00:00+00:00", "management_policy": {},
        "original_package": {"legs": [{
            "leg_index": 0, "contract_ticker": "O:NVDA261111P00172500", "contract_type": "PUT", "side": "BUY",
            "ratio": 1, "strike": "172.5", "expiration_date": "2026-11-11", "model_mark": f"{mark:.6f}",
            "source_market_time": TRIGGER.isoformat(), "spot": "173.085", "local_iv": None, "local_delta": None,
            "local_gamma": None, **leg,
        }]},
    }


def _inputs(row, value=INPUT):
    return {lookup: value for lookup in valuation.input_lookups([row])}


@pytest.mark.parametrize("expiration,at", [
    (date(2026, 11, 11), TRIGGER),
    (date(2026, 11, 11), datetime(2026, 11, 11, 20, 59, tzinfo=timezone.utc)),
    (date(2026, 11, 11), datetime(2026, 11, 11, 21, 1, tzinfo=timezone.utc)),
    (date(2026, 11, 19), TRIGGER),
    (date(2026, 11, 10), TRIGGER),
])
def test_time_to_expiration_matches_the_projection(expiration, at):
    from realtime.advanced_options_projection import _time_to_expiration_years

    assert valuation.time_to_expiration_years(expiration, at) == _time_to_expiration_years(expiration, at)


def test_oa1_solves_iv_and_greeks_at_the_leg_source_time():
    """Nov 11 live: every O1 leg (16) served IV, Delta and Gamma as Unavailable; the package stores the mark only."""
    row = _row()
    valued = valuation.value_row(row, _inputs(row))
    assert valued["complete"]
    valuation.apply_row(row, valued)
    (leg,) = row["original_package"]["legs"]
    assert leg["local_iv"] == pytest.approx(0.42, abs=1e-4)
    assert -1 < leg["local_delta"] < 0 and leg["local_gamma"] > 0 and leg["local_theta_per_day"] < 0
    assert leg["greeks_reason"] is None and leg["greeks_basis"] == "ALERT_SOURCE_MARK_AND_1M_SPOT"
    assert leg["greeks_market_time"] == TRIGGER.isoformat()
    assert leg["risk_free_rate"] == "0.0405" and leg["model_version"] == "black-scholes-merton-v1"


def test_oa1_says_why_a_leg_has_no_greeks_and_keeps_values_it_carries():
    missing_spot = _row(spot=None)
    valued = valuation.value_row(missing_spot, _inputs(missing_spot))
    assert valued["legs"][0] == {"greeks_reason": "SOURCE_SPOT_UNAVAILABLE"} and not valued["complete"]
    no_input = _row()
    valued = valuation.value_row(no_input, {})
    assert valued["legs"][0]["greeks_reason"] == "VALUATION_INPUT_UNAVAILABLE" and not valued["complete"]
    expired = _row(expiration_date="2026-11-10")
    assert valuation.value_row(expired, _inputs(expired))["legs"][0] == {"greeks_reason": "EXPIRED_AT_SOURCE"}
    carried = _row(local_iv=0.3, local_delta=-0.4)
    assert valuation.value_row(carried, _inputs(carried))["legs"] == [{}]
    assert not valuation.needs_valuation(carried)


def test_oa1_lookup_is_point_in_time_per_leg():
    row = _row()
    ((session, at, ticker, expiration),) = valuation.input_lookups([row])
    assert (session, at, ticker, expiration) == ("2026-11-11", TRIGGER, "NVDA", "2026-11-11")
    assert "s.known_at<=t.at ORDER BY s.known_at DESC LIMIT 1" in valuation.VALUATION_INPUTS_SQL


def test_oa2_model_exit_for_a_bought_0dte_put():
    row = _row()
    valuation.apply_row(row, valuation.value_row(row, _inputs(row)))
    exit_ = row["model_exit"]
    assert exit_["status"] == "AVAILABLE" and exit_["policy_version"] == "options-alert-model-exit-v1"
    assert exit_["basis"] == "EXPECTED_MOVE_TO_EXIT" and exit_["not_a_fill"] is True and exit_["per"] == "SHARE"
    spot, entry = 173.085, float(exit_["entry_price"])
    # A put profits as the stock falls: the target is below the spot, the stop above.
    assert float(exit_["stock_target"]) < spot < float(exit_["stock_stop"])
    move = float(exit_["expected_move"])
    assert float(exit_["stock_target"]) == pytest.approx(spot - move, abs=0.011)
    assert float(exit_["stock_stop"]) == pytest.approx(spot + 0.5 * move, abs=0.011)
    assert float(exit_["stop_price"]) < entry < float(exit_["target_price"])
    assert exit_["reward_risk"] > 0
    # At the 16:00 ET exit a 0 DTE contract is worth its intrinsic value; its breakeven is strike - premium.
    assert float(exit_["target_price_at_exit"]) == pytest.approx(max(172.5 - float(exit_["stock_target"]), 0), abs=1e-3)
    assert float(exit_["breakeven_stock_at_exit"]) == pytest.approx(172.5 - entry, abs=0.006)
    assert exit_["breakeven_sigma_at_exit"] > 0


def test_oa2_applies_only_to_one_bought_contract_without_a_rule():
    for row in (
        _row(side="SELL"),
        dict(_row(), management_policy={"policy_version": "o3-v1"}),
    ):
        valuation.apply_row(row, valuation.value_row(row, _inputs(row)))
        assert "model_exit" not in row
    spread = _row()
    spread["original_package"]["legs"].append(dict(spread["original_package"]["legs"][0], leg_index=1, side="SELL"))
    valuation.apply_row(spread, valuation.value_row(spread, _inputs(spread)))
    assert "model_exit" not in spread
    no_exit = dict(_row(), exit_deadline=None)
    valuation.apply_row(no_exit, valuation.value_row(no_exit, _inputs(no_exit)))
    assert no_exit["model_exit"]["status"] == "UNAVAILABLE" and no_exit["model_exit"]["reason"] == "EXIT_DEADLINE_UNAVAILABLE"
    no_greeks = _row()
    valuation.apply_row(no_greeks, valuation.value_row(no_greeks, {}))
    assert no_greeks["model_exit"]["reason"] == "VALUATION_INPUT_UNAVAILABLE"


def test_oa2_call_with_days_left_values_the_exit_with_time_value():
    at = datetime(2026, 11, 11, 16, 7, 42, tzinfo=timezone.utc)
    expiration = date(2026, 11, 23)
    mark = _mark("CALL", 758.22, 761.0, at, expiration, iv=0.16)
    row = _row(contract_type="CALL", strike="761", expiration_date="2026-11-23", model_mark=f"{mark:.6f}",
               source_market_time=at.isoformat(), spot="758.22", contract_ticker="O:SPY261123C00761000")
    row.update(underlyer="SPY", triggered_at=at.isoformat())
    valuation.apply_row(row, valuation.value_row(row, _inputs(row, dict(INPUT, ticker="SPY", expiration_date="2026-11-23"))))
    exit_ = row["model_exit"]
    assert float(exit_["stock_target"]) > 758.22 > float(exit_["stock_stop"])
    target_intrinsic = max(float(exit_["stock_target"]) - 761.0, 0)
    assert float(exit_["target_price_at_exit"]) > target_intrinsic
    # Production counts whole days for a contract with days left, so a same-day exit has the same maturity as the
    # trigger and the premium is paid back at the trigger's spot.
    assert float(exit_["breakeven_stock_at_exit"]) == pytest.approx(758.22, abs=0.05)


class _ValuationCursor:
    def __init__(self, found=None, broken=False):
        self.found, self.broken, self.statements = found or {}, broken, []

    def execute(self, sql, params=None):
        self.statements.append(sql)
        if sql.startswith(("SAVEPOINT", "RELEASE", "ROLLBACK TO")):
            return
        if self.broken:
            raise RuntimeError("canceling statement due to statement timeout")
        assert "rt.advanced_options_reference_states" in sql
        ordinals, tickers, expirations = params[0], params[3], params[4]
        self.result = [{"ordinal": ordinal, "input": self.found[(ticker, expiration)]}
                       for ordinal, ticker, expiration in zip(ordinals, tickers, expirations)
                       if (ticker, expiration) in self.found]

    def fetchall(self):
        return self.result


def test_repository_serves_valuation_reports_it_and_keeps_complete_values():
    from realtime.portal_compatibility_repository import RealtimeOptionsAlertsPageRepository

    repository = RealtimeOptionsAlertsPageRepository.__new__(RealtimeOptionsAlertsPageRepository)
    row = _row()
    cursor = _ValuationCursor({("NVDA", "2026-11-11"): INPUT})
    result = repository._advanced_alert_valuation_reported(cursor, {"data": {"rows": [row]}})
    served = result["data"]["rows"][0]
    assert served["original_package"]["legs"][0]["local_iv"] == pytest.approx(0.42, abs=1e-4)
    assert served["model_exit"]["status"] == "AVAILABLE"
    assert result["data"]["optional_reads"] == [{"read": "alert_valuation", "status": "AVAILABLE", "reason_code": None,
                                                 "serving_source": "SOURCE_TIME_MODEL"}]
    # Complete values are kept by evaluation id: the next read makes no query.
    again = _ValuationCursor()
    repeat_row = _row()
    repeat_row["evaluation_id"] = row["evaluation_id"]
    repeated = repository._advanced_alert_valuation_reported(again, {"data": {"rows": [repeat_row]}})
    assert repeated["data"]["rows"][0]["original_package"]["legs"][0]["local_iv"] == pytest.approx(0.42, abs=1e-4)
    assert not [sql for sql in again.statements if "rt." in sql]
    # A failed read serves the rows as they were and says so.
    broken = _ValuationCursor(broken=True)
    failed = repository._advanced_alert_valuation_reported(broken, {"data": {"rows": [_row()]}})
    assert failed["data"]["optional_reads"][0]["status"] == "FALLBACK"
    assert failed["data"]["optional_reads"][0]["reason_code"] == "ALERT_VALUATION_UNAVAILABLE"
    assert failed["data"]["rows"][0]["original_package"]["legs"][0]["local_iv"] is None
    assert broken.statements[-1] == "ROLLBACK TO SAVEPOINT options_alert_valuation"


def test_valuation_runs_only_with_its_switch_on(monkeypatch):
    # Nov 12 r95 rehearsal: the API role cannot read rt.advanced_options_reference_states, so OA1/OA2 stay off until
    # that grant exists. Off: no statement, rows unchanged, no optional read listed (the read as before OA1).
    from realtime import portal_compatibility_repository as module

    repository = module.RealtimeOptionsAlertsPageRepository.__new__(module.RealtimeOptionsAlertsPageRepository)
    monkeypatch.delenv(module.OPTIONS_ALERT_VALUATION_SWITCH, raising=False)
    cursor = _ValuationCursor({("NVDA", "2026-11-11"): INPUT})
    envelope = {"data": {"rows": [_row()]}}
    served = repository._advanced_alert_valuation_switched(cursor, envelope)
    assert served is envelope and cursor.statements == []
    assert "optional_reads" not in served["data"]
    assert served["data"]["rows"][0]["original_package"]["legs"][0]["local_iv"] is None
    monkeypatch.setenv(module.OPTIONS_ALERT_VALUATION_SWITCH, "on")
    valued = repository._advanced_alert_valuation_switched(cursor, {"data": {"rows": [_row()]}})
    assert valued["data"]["optional_reads"][0]["status"] == "AVAILABLE"


def test_of3_flow_rows_without_a_catalog_id_carry_null():
    from realtime.option_flow_breadth import _top_contract

    base = {"contract_ticker": "O:QQQ261218C00686000", "contract_type": "CALL", "expiration_date": "2026-12-18",
            "calendar_dte": 37, "strike": "686", "spot": "690.16", "day_volume": 10, "open_interest": 5, "mark": "3.10"}
    assert _top_contract(dict(base, contract_id=0))["contract_id"] is None
    assert _top_contract(dict(base, contract_id=None))["contract_id"] is None
    assert _top_contract(dict(base, contract_id=7123))["contract_id"] == 7123
