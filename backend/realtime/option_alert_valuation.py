"""OA1 and OA2 (Nov 12, ticker page review): read-time valuation of Options alert legs at each alert's source time.

OA1. O1 packages carry the contract and its entry mark only (`advanced_options_projection._alert_run_row` stores
`local_iv`, `local_delta` and `local_gamma` as None), so IV, Delta and Gamma read Unavailable on Latest Run, Day History
and the plan detail. They are solved here with the solver the Flow page uses (`solve_local_greeks`, Black-Scholes-
Merton) from the leg's entry mark, the stock's last 1-minute close at the trigger (R33 source facts, `leg["spot"]`) and
the session's rate and dividend inputs known by the leg's mark time (`rt.advanced_options_reference_states`). Nothing
after the source time is read (INV-3). A leg that cannot be valued says why in `greeks_reason` (INV-8).

OA2. A model exit estimate for a single bought contract whose detector sets no stop or target (O1, matrix R33). The
stock's expected move to the alert's exit deadline is `spot x IV x sqrt(years to exit)`. The target is the option's
value if the stock moves `target_sigma` of that in the trade's direction, the stop its value at `stop_sigma` against,
both valued at the source time with the same IV (before time decay). `target_price_at_exit` is the value if the move is
only reached at the exit deadline (a 0 DTE contract is worth its intrinsic value at the close), and the breakeven says
how large a move by the exit pays the premium. It is a model estimate, not a fill and not a technical level (those
follow the ticker page's TP5).
"""
from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import date, datetime, time as clock_time, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from options.analytics.greeks import OptionValuationInput, price_and_greeks, solve_local_greeks
from options.domain import ContractType

EXCHANGE_TIME_ZONE = ZoneInfo("America/New_York")
GREEK_FIELDS = (
    "local_iv", "local_delta", "local_gamma", "local_theta_per_day",
    "local_vega_per_vol_point", "local_rho_per_rate_point",
)
GREEKS_BASIS = "ALERT_SOURCE_MARK_AND_1M_SPOT"
MODEL_EXIT_POLICY = {
    "version": "options-alert-model-exit-v1",
    "basis": "EXPECTED_MOVE_TO_EXIT",
    "target_sigma": 1.0,
    "stop_sigma": 0.5,
}
MODEL_EXIT_POLICY_FIELDS = {
    "policy_version": MODEL_EXIT_POLICY["version"],
    "basis": MODEL_EXIT_POLICY["basis"],
    "target_sigma": MODEL_EXIT_POLICY["target_sigma"],
    "stop_sigma": MODEL_EXIT_POLICY["stop_sigma"],
    "per": "SHARE",
    "not_a_fill": True,
}
_YEAR_SECONDS = 365.0 * 86400.0


def time_to_expiration_years(expiration: date, at: datetime) -> float | None:
    """The production convention (`advanced_options_projection._time_to_expiration_years`): whole days / 365; a
    contract expiring that day counts the time to its 16:00 ET close; None once expired."""
    days = (expiration - at.astimezone(EXCHANGE_TIME_ZONE).date()).days
    if days > 0:
        return days / 365.0
    if days < 0:
        return None
    close = datetime.combine(expiration, clock_time(16), EXCHANGE_TIME_ZONE)
    remaining = (close - at).total_seconds()
    return remaining / _YEAR_SECONDS if remaining > 0 else None


def _decimal(value) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _float(value) -> float | None:
    parsed = _decimal(value)
    return None if parsed is None else float(parsed)


def _time(value) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else None
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None


def _date(value) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _legs(row) -> list:
    package = row.get("original_package") if isinstance(row, Mapping) else None
    legs = package.get("legs") if isinstance(package, Mapping) else None
    return [leg for leg in legs or () if isinstance(leg, dict)]


def leg_source_time(row, leg) -> datetime | None:
    """The leg's own mark time; the alert's trigger time when the leg does not carry one."""
    return _time(leg.get("source_market_time")) or _time(row.get("triggered_at"))


def needs_valuation(row) -> bool:
    return any(leg.get("local_iv") is None and leg.get("greeks_reason") is None for leg in _legs(row))


def input_lookups(rows) -> list[tuple[str, datetime, str, str]]:
    """(session date, source time, underlyer, expiration) for every leg still without IV."""
    lookups = set()
    for row in rows:
        session, underlyer = _date(row.get("session_date")), row.get("underlyer")
        for leg in _legs(row):
            at, expiration = leg_source_time(row, leg), _date(leg.get("expiration_date"))
            if session and underlyer and at and expiration and leg.get("local_iv") is None:
                lookups.add((session.isoformat(), at, str(underlyer), expiration.isoformat()))
    return sorted(lookups)


VALUATION_INPUTS_SQL = (
    "SELECT t.ordinal,v.input FROM unnest(%s::int[],%s::date[],%s::timestamptz[],%s::text[],%s::text[]) "
    "AS t(ordinal,session,at,ticker,expiration) "
    "CROSS JOIN LATERAL (SELECT s.state_document FROM rt.advanced_options_reference_states AS s "
    "WHERE s.session_date=t.session AND s.known_at<=t.at ORDER BY s.known_at DESC LIMIT 1) AS s "
    "CROSS JOIN LATERAL (SELECT e AS input FROM jsonb_array_elements(s.state_document->'valuation_inputs') AS e "
    "WHERE e->>'ticker'=t.ticker AND e->>'expiration_date'=t.expiration LIMIT 1) AS v"
)


def load_valuation_inputs(cursor, lookups) -> dict[tuple[str, datetime, str, str], Mapping]:
    """The rate and dividend inputs of the session's reference state known by each source time (point in time)."""
    lookups = list(lookups)
    if not lookups:
        return {}
    cursor.execute(VALUATION_INPUTS_SQL, (
        list(range(len(lookups))), [item[0] for item in lookups], [item[1] for item in lookups],
        [item[2] for item in lookups], [item[3] for item in lookups],
    ))
    found = {}
    for fetched in cursor.fetchall():
        values = dict(fetched) if isinstance(fetched, Mapping) else {"ordinal": fetched[0], "input": fetched[1]}
        if isinstance(values.get("input"), Mapping):
            found[lookups[int(values["ordinal"])]] = values["input"]
    return found


def value_leg(row, leg, valuations) -> dict:
    """OA1: the leg's IV and Greeks at its source time, or the reason it has none. A leg already valued is kept."""
    if leg.get("local_iv") is not None:
        return {}
    at = leg_source_time(row, leg)
    session, expiration = _date(row.get("session_date")), _date(leg.get("expiration_date"))
    spot, strike, mark = _decimal(leg.get("spot")), _decimal(leg.get("strike")), _decimal(leg.get("model_mark"))
    kind = str(leg.get("contract_type") or "")
    if at is None or session is None or expiration is None or kind not in ("CALL", "PUT") or not strike or not mark:
        return {"greeks_reason": "LEG_TERMS_INCOMPLETE"}
    if not spot:
        return {"greeks_reason": "SOURCE_SPOT_UNAVAILABLE", "retry": True}
    valuation = valuations.get((session.isoformat(), at, str(row.get("underlyer")), expiration.isoformat()))
    rate = _float((valuation or {}).get("risk_free_rate"))
    dividend = _float((valuation or {}).get("dividend_yield"))
    if rate is None or dividend is None:
        return {"greeks_reason": "VALUATION_INPUT_UNAVAILABLE", "retry": True}
    years = time_to_expiration_years(expiration, at)
    if years is None:
        return {"greeks_reason": "EXPIRED_AT_SOURCE"}
    (result,) = solve_local_greeks((OptionValuationInput(
        contract_type=ContractType(kind), spot=spot, strike=strike, model_mark=mark,
        time_to_expiration_years=years, risk_free_rate=rate, dividend_yield=dividend,
    ),))
    return {
        **{field: getattr(result, field) for field in GREEK_FIELDS},
        "time_to_expiration_years": years,
        "risk_free_rate": str(valuation["risk_free_rate"]),
        "dividend_yield": str(valuation["dividend_yield"]),
        "model_version": str(valuation.get("model_version") or ""),
        "greeks_reason": None if result.failure_reason is None else result.failure_reason.value,
        "greeks_basis": GREEKS_BASIS,
        "greeks_market_time": at.isoformat(),
    }


def _intrinsic(kind: str, spot: float, strike: float) -> float:
    return max(spot - strike, 0.0) if kind == "CALL" else max(strike - spot, 0.0)


def _value(kind: str, spot: float, strike: float, maturity: float | None, rate: float, dividend: float, iv: float):
    if maturity is None or maturity <= 0:
        return _intrinsic(kind, spot, strike)
    return price_and_greeks(ContractType(kind), spot=spot, strike=strike, maturity=maturity, rate=rate,
                            dividend=dividend, volatility=iv)[0]


def _breakeven(kind, mark, strike, maturity, rate, dividend, iv, spot) -> float | None:
    """The stock price at which the contract is worth its entry mark at the exit deadline."""
    if maturity is None or maturity <= 0:
        return strike + mark if kind == "CALL" else strike - mark
    low, high = spot * 0.2, spot * 5.0

    def gap(level):
        return _value(kind, level, strike, maturity, rate, dividend, iv) - mark

    if gap(low) * gap(high) > 0:
        return None
    for _ in range(100):
        middle = (low + high) / 2
        if (gap(middle) > 0) == (gap(high) > 0):
            high = middle
        else:
            low = middle
    return (low + high) / 2


def _money(value: float, places: str = "0.0001") -> str:
    return format(Decimal(str(value)).quantize(Decimal(places)), "f")


def model_exit_estimate(row) -> dict | None:
    """OA2 for a single bought contract with no management policy; None when it does not apply."""
    policy = row.get("management_policy")
    if isinstance(policy, Mapping) and policy.get("policy_version"):
        return None
    legs = _legs(row)
    if len(legs) != 1 or legs[0].get("side") != "BUY" or int(legs[0].get("ratio") or 1) != 1:
        return None
    leg = legs[0]
    unavailable = dict(MODEL_EXIT_POLICY_FIELDS, status="UNAVAILABLE")
    iv, spot, strike = _float(leg.get("local_iv")), _float(leg.get("spot")), _float(leg.get("strike"))
    mark, rate, dividend = _float(leg.get("model_mark")), _float(leg.get("risk_free_rate")), _float(leg.get("dividend_yield"))
    kind, maturity = str(leg.get("contract_type") or ""), leg.get("time_to_expiration_years")
    at, exit_at, expiration = leg_source_time(row, leg), _time(row.get("exit_deadline")), _date(leg.get("expiration_date"))
    if not iv or not spot or not strike or not mark or rate is None or dividend is None or not maturity or at is None:
        return dict(unavailable, reason=leg.get("greeks_reason") or "LEG_VALUATION_UNAVAILABLE")
    if exit_at is None:
        return dict(unavailable, reason="EXIT_DEADLINE_UNAVAILABLE")
    horizon = (exit_at - at).total_seconds() / _YEAR_SECONDS
    if horizon <= 0 or expiration is None:
        return dict(unavailable, reason="EXIT_DEADLINE_PASSED_AT_SOURCE")
    direction = 1 if kind == "CALL" else -1
    expected_move = spot * iv * math.sqrt(horizon)
    stock_target = spot + direction * MODEL_EXIT_POLICY["target_sigma"] * expected_move
    stock_stop = spot - direction * MODEL_EXIT_POLICY["stop_sigma"] * expected_move
    if stock_target <= 0 or stock_stop <= 0:
        return dict(unavailable, reason="EXPECTED_MOVE_OUT_OF_RANGE")
    exit_maturity = time_to_expiration_years(expiration, exit_at)
    target_now = _value(kind, stock_target, strike, float(maturity), rate, dividend, iv)
    stop_now = _value(kind, stock_stop, strike, float(maturity), rate, dividend, iv)
    target_at_exit = _value(kind, stock_target, strike, exit_maturity, rate, dividend, iv)
    breakeven = _breakeven(kind, mark, strike, exit_maturity, rate, dividend, iv, spot)
    risk, reward = mark - stop_now, target_now - mark
    return {
        **MODEL_EXIT_POLICY_FIELDS,
        "status": "AVAILABLE",
        "reason": None,
        "valuation_time": at.isoformat(),
        "exit_at": exit_at.isoformat(),
        "iv": iv,
        "spot": _money(spot, "0.01"),
        "expected_move": _money(expected_move, "0.01"),
        "stock_target": _money(stock_target, "0.01"),
        "stock_stop": _money(stock_stop, "0.01"),
        "entry_price": _money(mark),
        "target_price": _money(target_now),
        "stop_price": _money(stop_now),
        "target_price_at_exit": _money(target_at_exit),
        "reward_risk": None if risk <= 0 else round(reward / risk, 2),
        "breakeven_stock_at_exit": None if breakeven is None else _money(breakeven, "0.01"),
        "breakeven_sigma_at_exit": (
            None if breakeven is None else round(direction * (breakeven - spot) / expected_move, 2)
        ),
    }


def value_row(row, valuations) -> dict:
    """The leg values and model exit for one alert row; `complete` when nothing would change on a later read."""
    legs = _legs(row)
    leg_values = [value_leg(row, leg, valuations) for leg in legs]
    complete = not any(values.get("retry") for values in leg_values)
    for values in leg_values:
        values.pop("retry", None)
    return {"legs": leg_values, "complete": complete}


def apply_row(row, valued) -> None:
    """Adds the leg values the leg does not already carry, then the model exit estimate."""
    for leg, values in zip(_legs(row), valued.get("legs") or ()):
        for key, value in values.items():
            if leg.get(key) is None:
                leg[key] = value
    estimate = model_exit_estimate(row)
    if estimate is not None:
        row["model_exit"] = estimate
