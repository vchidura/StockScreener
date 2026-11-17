"""G106 A2 (Nov 17 night build): IV summary per underlying and tenor, read from the stored chain capture.

One model throughout: our IV and delta (`local_iv`, `local_delta`, solved at capture on the quote midpoint with the
session's stored rate and dividend, `advanced_options_breadth.breadth_local_greeks`) and the same year fraction the
solver used (whole calendar days / 365; a same-day expiry counts the time to 16:00 New York). The provider's IV and
delta go through the same steps as a cross-check (`provider_*`), never in place of ours.

Per expiry, `options.analytics.chain_analysis.analyze_expirations` (the existing analytics, IV comparability policy
`iv_comparability_v1`): ATM IV = median of the comparable IVs at the two strikes bracketing the forward; 25-delta call
and put IV = linear interpolation in delta between the two contracts bracketing +0.25 / -0.25 (gap at most 0.15);
risk reversal = call 25-delta IV minus put 25-delta IV. Expected move to each listed expiry = the ATM straddle (call mid
plus put mid at the listed strike nearest the forward, both quoted) and the IV-based one-sigma move spot x ATM IV x
sqrt(T).

Tenors 7/30/60 calendar days: interpolation in total variance (w = IV^2 T) between the two listed expiries (at least one
day out) bracketing the tenor, separately for each quantity. A tenor outside the listed expiries (the chain capture
stops at 60 days, so 60 is often just past the last expiry) takes the nearest expiry's IV flat when it is at most
`FLAT_EXTRAPOLATION_DAYS` calendar days away (method FLAT_EXTRAPOLATION, named in quality_reasons); further out it is
unavailable (TENOR_OUTSIDE_COVERAGE). Put/call ratios are over every contract of the capture.

Reads the latest capture only (G102 R55: the current-rows table keeps rows of contracts that left the band in earlier
captures), and keeps one computation per capture. Research observation; behind `REALTIME_V2_OPTIONS_IV_SUMMARY_READ` on
the API (off by default: the route answers 404). `iv_summary_from_daily_cursor` computes the same summary for a past
session from `rt.option_breadth_daily` (H1), the input of IV rank and percentile once history exists.
"""
from __future__ import annotations

import math
import os
import threading
from collections import OrderedDict
from datetime import date, datetime
from decimal import Decimal
from statistics import median
from typing import Mapping, Sequence
from uuid import UUID

OPTIONS_IV_SUMMARY_READ_SWITCH = "REALTIME_V2_OPTIONS_IV_SUMMARY_READ"
IV_SUMMARY_CONTRACT = "options-iv-summary-read-v1"
TENOR_DAYS = (7, 30, 60)
FLAT_EXTRAPOLATION_DAYS = 5
WIDE_QUOTE_FRACTION = 0.5
_YEAR_DAYS = 365.0
_CACHE_SIZE = 32
_CACHE: "OrderedDict[tuple, dict]" = OrderedDict()
_CACHE_LOCK = threading.Lock()
_MATRIX = UUID(int=0)
ROW_NAMES = (
    "contract_type", "expiration_date", "strike_price", "day_volume", "open_interest", "bid", "ask",
    "implied_volatility", "delta", "local_iv", "local_delta", "spot", "underlying_price", "risk_free_rate",
    "dividend_yield", "observed_at",
)
_LEGACY_REASONS = {"DEVELOPER_QUOTES_NOT_AVAILABLE"}


def options_iv_summary_read_enabled(environment=None) -> bool:
    value = (os.environ if environment is None else environment).get(OPTIONS_IV_SUMMARY_READ_SWITCH, "OFF")
    return str(value).strip().upper() == "ON"


def _row(values, names=ROW_NAMES):
    return dict(values) if isinstance(values, Mapping) else dict(zip(names, values))


def _scalar(cursor):
    found = cursor.fetchone()
    if found is None:
        return None
    return next(iter(found.values())) if isinstance(found, Mapping) else found[0]


def _num(value) -> str | None:
    return None if value is None else format(Decimal(str(value)).normalize(), "f")


def _round(value, digits=6):
    return None if value is None else round(float(value), digits)


def _unavailable(reason, underlyer, session_date=None, **extra) -> dict:
    return {
        "contract": IV_SUMMARY_CONTRACT, "available": False, "reason": reason, "underlyer": underlyer,
        **({"session_date": session_date.isoformat()} if session_date else {}), **extra,
    }


# ---- reads ---------------------------------------------------------------------------------------------------------

def iv_summary_from_breadth_cursor(
    cursor, *, underlyer: str, available_by: datetime, session_date: date | None = None,
) -> dict:
    """The IV summary for one underlying from its latest stored capture visible at `available_by`."""
    if session_date is None:
        cursor.execute(
            "SELECT max(session_date) FROM rt.option_breadth_current WHERE underlying=%s AND observed_at<=%s",
            (underlyer, available_by),
        )
        session_date = _scalar(cursor)
        if session_date is None:
            return _unavailable("NO_CHAIN_CAPTURE", underlyer)
    cursor.execute(
        "SELECT max(observed_at),count(*) FROM rt.option_breadth_current "
        "WHERE session_date=%s AND underlying=%s AND observed_at<=%s",
        (session_date, underlyer, available_by),
    )
    found = cursor.fetchone()
    observed, count = (tuple(found.values()) if isinstance(found, Mapping) else tuple(found)) if found else (None, 0)
    if observed is None:
        return _unavailable("NO_CHAIN_CAPTURE", underlyer, session_date)
    key = ("current", underlyer, session_date, observed, int(count))
    with _CACHE_LOCK:
        cached = _CACHE.get(key)
        if cached is not None:
            _CACHE.move_to_end(key)
            return cached
    # The latest capture only (R55): rows sharing the underlying's newest observed_at.
    cursor.execute(
        "SELECT " + ",".join(ROW_NAMES) + " FROM rt.option_breadth_current "
        "WHERE session_date=%s AND underlying=%s AND observed_at=%s",
        (session_date, underlyer, observed),
    )
    rows = [_row(values) for values in cursor.fetchall()]
    result = compute_iv_summary(
        rows, underlyer=underlyer, session_date=session_date, observed_at=observed, source="CURRENT_LATEST_CAPTURE",
    )
    with _CACHE_LOCK:
        _CACHE[key] = result
        _CACHE.move_to_end(key)
        while len(_CACHE) > _CACHE_SIZE:
            _CACHE.popitem(last=False)
    return result


def iv_summary_from_daily_cursor(cursor, *, underlyer: str, session_date: date) -> dict:
    """The same summary for a past session from `rt.option_breadth_daily` (its final capture rows)."""
    cursor.execute(
        "SELECT " + ",".join(ROW_NAMES) + " FROM rt.option_breadth_daily "
        "WHERE session_date=%s AND underlying=%s AND final_capture",
        (session_date, underlyer),
    )
    rows = [_row(values) for values in cursor.fetchall()]
    if not rows:
        return _unavailable("NO_DAILY_SNAPSHOT", underlyer, session_date)
    observed = max(row["observed_at"] for row in rows)
    return compute_iv_summary(
        rows, underlyer=underlyer, session_date=session_date, observed_at=observed, source="DAILY_SNAPSHOT",
    )


# ---- computation ---------------------------------------------------------------------------------------------------

def year_fraction(expiration: date, observed_at: datetime) -> float | None:
    """The solver's year fraction (`advanced_options_projection._time_to_expiration_years`)."""
    from .advanced_options_projection import _time_to_expiration_years

    return _time_to_expiration_years(expiration, observed_at)


def interpolate_total_variance(
    points: Sequence[tuple[float, float, date]], target_years: float, *, flat_days: float = FLAT_EXTRAPOLATION_DAYS,
) -> dict:
    """IV at `target_years` from (years, iv, expiry) points: total variance w = iv^2 T, linear in T between the two
    points bracketing the target; flat IV within `flat_days` outside them; otherwise unavailable."""
    usable = sorted((t, iv, expiry) for t, iv, expiry in points if t > 0 and iv is not None and iv > 0)
    if not usable:
        return {"iv": None, "method": "UNAVAILABLE", "bracket": [], "reasons": ["NO_TERM_POINTS"]}
    for t, iv, expiry in usable:
        if abs(t - target_years) < 1e-12:
            return {"iv": iv, "method": "EXACT_EXPIRY", "bracket": [expiry.isoformat()], "reasons": []}
    below = [point for point in usable if point[0] < target_years]
    above = [point for point in usable if point[0] > target_years]
    if below and above:
        (t1, iv1, e1), (t2, iv2, e2) = below[-1], above[0]
        w1, w2 = iv1 * iv1 * t1, iv2 * iv2 * t2
        w = w1 + (w2 - w1) * (target_years - t1) / (t2 - t1)
        return {
            "iv": math.sqrt(w / target_years), "method": "TOTAL_VARIANCE_INTERPOLATION",
            "bracket": [e1.isoformat(), e2.isoformat()],
            "reasons": ["CALENDAR_VARIANCE_DECREASING"] if w2 < w1 else [],
        }
    t, iv, expiry = above[0] if above else below[-1]
    gap_days = abs(t - target_years) * _YEAR_DAYS
    side = "BEFORE_FIRST_EXPIRY" if above else "AFTER_LAST_EXPIRY"
    if gap_days <= flat_days + 1e-9:
        return {"iv": iv, "method": "FLAT_EXTRAPOLATION", "bracket": [expiry.isoformat()],
                "reasons": [f"FLAT_EXTRAPOLATION_{side}:{round(gap_days)}D"]}
    return {"iv": None, "method": "UNAVAILABLE", "bracket": [expiry.isoformat()],
            "reasons": [f"TENOR_OUTSIDE_COVERAGE_{side}:{round(gap_days)}D"]}


def _mid(row) -> Decimal | None:
    bid, ask = row.get("bid"), row.get("ask")
    if bid is None or ask is None:
        return None
    bid, ask = Decimal(str(bid)), Decimal(str(ask))
    return (bid + ask) / 2 if ask >= bid and ask > 0 else None


def _wide(row, mid) -> bool:
    return mid is not None and mid > 0 and float((Decimal(str(row["ask"])) - Decimal(str(row["bid"]))) / mid) > WIDE_QUOTE_FRACTION


def straddle_at_forward(rows: Sequence[Mapping], forward: float) -> dict | None:
    """Call mid + put mid at the listed strike nearest the forward that has both quoted (ties: the lower strike)."""
    by_strike: dict[Decimal, dict[str, Mapping]] = {}
    for row in rows:
        if _mid(row) is not None:
            by_strike.setdefault(Decimal(str(row["strike_price"])), {})[str(row["contract_type"]).lower()] = row
    pairs = [(strike, legs) for strike, legs in by_strike.items() if "call" in legs and "put" in legs]
    if not pairs:
        return None
    strike, legs = min(pairs, key=lambda item: (abs(float(item[0]) - forward), item[0]))
    call_mid, put_mid = _mid(legs["call"]), _mid(legs["put"])
    return {
        "strike": strike, "mid": call_mid + put_mid,
        "wide": _wide(legs["call"], call_mid) or _wide(legs["put"], put_mid),
    }


def _inputs(rows, *, spot, observed_at, provider: bool, carry: tuple[float, float]):
    from options.analytics.chain_analysis import ExpirationContractInput
    from options.domain import ContractType

    inputs = []
    for index, row in enumerate(rows, start=1):
        iv = row.get("implied_volatility") if provider else row.get("local_iv")
        delta = row.get("delta") if provider else row.get("local_delta")
        years = row["_years"]
        if iv is None or delta is None or years is None or not float(iv) > 0:
            continue
        rate = row.get("risk_free_rate")
        dividend = row.get("dividend_yield")
        if provider:
            rate = carry[0] if rate is None else rate
            dividend = carry[1] if dividend is None else dividend
        elif rate is None or dividend is None:
            continue
        inputs.append(ExpirationContractInput(
            contract_id=index,
            contract_type=ContractType.CALL if str(row["contract_type"]).lower() == "call" else ContractType.PUT,
            expiration_date=row["expiration_date"], strike=Decimal(str(row["strike_price"])), spot=spot,
            time_to_expiration_years=float(years), risk_free_rate=float(rate), dividend_yield=float(dividend),
            local_iv=float(iv), local_delta=float(delta), day_volume=row.get("day_volume"),
            open_interest=row.get("open_interest"),
        ))
    return tuple(inputs)


def _analytics(inputs):
    from options.analytics.chain_analysis import analyze_expirations

    if not inputs:
        return {}
    return {item.expiration_date: item for item in analyze_expirations(_MATRIX, inputs)}


def compute_iv_summary(
    rows: Sequence[Mapping], *, underlyer: str, session_date: date, observed_at: datetime, source: str,
) -> dict:
    """The summary from one capture's rows (each a mapping with ROW_NAMES)."""
    if not rows:
        return _unavailable("NO_CHAIN_CAPTURE", underlyer, session_date)
    reasons: list[str] = []
    spot_value = next((row["spot"] for row in rows if row.get("spot")), None)
    if spot_value is None:
        spot_value = next((row["underlying_price"] for row in rows if row.get("underlying_price")), None)
        if spot_value is not None:
            reasons.append("SPOT_FROM_PROVIDER")
    if spot_value is None or not Decimal(str(spot_value)) > 0:
        return _unavailable("SPOT_UNAVAILABLE", underlyer, session_date)
    spot = Decimal(str(spot_value))
    rows = [dict(row, _years=year_fraction(row["expiration_date"], observed_at)) for row in rows]
    expired = sum(1 for row in rows if row["_years"] is None)
    if expired:
        reasons.append(f"EXPIRED_CONTRACTS_IGNORED:{expired}")
    live = [row for row in rows if row["_years"] is not None]
    rates = [float(row["risk_free_rate"]) for row in live if row.get("risk_free_rate") is not None]
    dividends = [float(row["dividend_yield"]) for row in live if row.get("dividend_yield") is not None]
    carry = (median(rates) if rates else 0.0, median(dividends) if dividends else 0.0)
    local_inputs = _inputs(live, spot=spot, observed_at=observed_at, provider=False, carry=carry)
    provider_inputs = _inputs(live, spot=spot, observed_at=observed_at, provider=True, carry=carry)
    unpriced = len(live) - len(local_inputs)
    if unpriced:
        reasons.append(f"LOCAL_IV_OR_DELTA_MISSING:{unpriced}")
    if not local_inputs:
        return _unavailable("NO_PRICED_CONTRACTS", underlyer, session_date, contracts=len(rows))
    local, provider = _analytics(local_inputs), _analytics(provider_inputs)
    by_expiry: dict[date, list[Mapping]] = {}
    for row in live:
        by_expiry.setdefault(row["expiration_date"], []).append(row)

    expiries = []
    for expiry in sorted(by_expiry):
        item, cross = local.get(expiry), provider.get(expiry)
        expiry_rows = by_expiry[expiry]
        years = float(expiry_rows[0]["_years"])
        calendar_dte = (expiry - observed_at.date()).days
        forward = float(item.forward_price) if item is not None and item.forward_price is not None else (
            float(spot) * math.exp((carry[0] - carry[1]) * years))
        straddle = straddle_at_forward(expiry_rows, forward)
        expiry_reasons = [] if item is None else [reason for reason in item.quality_reasons if reason not in _LEGACY_REASONS]
        if item is None:
            expiry_reasons.append("NO_PRICED_CONTRACTS")
        if straddle is None:
            expiry_reasons.append("STRADDLE_UNQUOTED")
        elif straddle["wide"]:
            expiry_reasons.append("WIDE_STRADDLE_QUOTE")
        atm = None if item is None else item.atm_iv
        one_sigma = None if atm is None else float(spot) * atm * math.sqrt(years)
        expiries.append({
            "expiration_date": expiry.isoformat(), "calendar_dte": calendar_dte, "years": _round(years, 8),
            "forward": _round(forward, 4),
            "atm_iv": _round(atm), "provider_atm_iv": _round(None if cross is None else cross.atm_iv),
            "call_25_delta_iv": _round(None if item is None else item.call_25_delta_iv),
            "put_25_delta_iv": _round(None if item is None else item.put_25_delta_iv),
            "risk_reversal_25_delta": _round(None if item is None else item.risk_reversal_25_delta),
            "provider_risk_reversal_25_delta": _round(None if cross is None else cross.risk_reversal_25_delta),
            "straddle_strike": None if straddle is None else _num(straddle["strike"]),
            "straddle_mid": None if straddle is None else _num(straddle["mid"]),
            "expected_move_fraction": None if straddle is None else _round(float(straddle["mid"]) / float(spot)),
            "one_sigma_move": _round(one_sigma, 4),
            "one_sigma_move_fraction": None if atm is None else _round(atm * math.sqrt(years)),
            "contracts": len(expiry_rows),
            "quality_reasons": expiry_reasons,
            "_years": years, "_item": item, "_cross": cross,
        })

    term = [entry for entry in expiries if entry["calendar_dte"] >= 1]
    if len(term) < len(expiries):
        reasons.append("SAME_DAY_EXPIRY_NOT_IN_TENORS")

    def points(pick):
        return [(entry["_years"], pick(entry), date.fromisoformat(entry["expiration_date"])) for entry in term]

    def attr(source_key, name):
        return lambda entry: None if entry[source_key] is None else getattr(entry[source_key], name)

    tenors = []
    for days in TENOR_DAYS:
        target = days / _YEAR_DAYS
        atm = interpolate_total_variance(points(attr("_item", "atm_iv")), target)
        call = interpolate_total_variance(points(attr("_item", "call_25_delta_iv")), target)
        put = interpolate_total_variance(points(attr("_item", "put_25_delta_iv")), target)
        cross = interpolate_total_variance(points(attr("_cross", "atm_iv")), target)
        tenor_reasons = list(dict.fromkeys(
            [f"ATM:{reason}" for reason in atm["reasons"]] + [f"CALL_25:{reason}" for reason in call["reasons"]]
            + [f"PUT_25:{reason}" for reason in put["reasons"]]
        ))
        risk_reversal = None if call["iv"] is None or put["iv"] is None else call["iv"] - put["iv"]
        tenors.append({
            "days": days,
            "atm_iv": _round(atm["iv"]), "atm_method": atm["method"], "atm_bracket": atm["bracket"],
            "call_25_delta_iv": _round(call["iv"]), "put_25_delta_iv": _round(put["iv"]),
            "risk_reversal_25_delta": _round(risk_reversal),
            "provider_atm_iv": _round(cross["iv"]),
            "atm_iv_minus_provider": None if atm["iv"] is None or cross["iv"] is None else _round(atm["iv"] - cross["iv"]),
            "one_sigma_move": None if atm["iv"] is None else _round(float(spot) * atm["iv"] * math.sqrt(target), 4),
            "one_sigma_move_fraction": None if atm["iv"] is None else _round(atm["iv"] * math.sqrt(target)),
            "quality_reasons": tenor_reasons,
        })

    put_volume = sum(int(row.get("day_volume") or 0) for row in live if str(row["contract_type"]).lower() == "put")
    call_volume = sum(int(row.get("day_volume") or 0) for row in live if str(row["contract_type"]).lower() == "call")
    put_oi = sum(int(row.get("open_interest") or 0) for row in live if str(row["contract_type"]).lower() == "put")
    call_oi = sum(int(row.get("open_interest") or 0) for row in live if str(row["contract_type"]).lower() == "call")
    if not call_volume:
        reasons.append("NO_CALL_VOLUME")
    if not call_oi:
        reasons.append("NO_CALL_OPEN_INTEREST")
    for entry in expiries:
        for name in ("_years", "_item", "_cross"):
            entry.pop(name)
    return {
        "contract": IV_SUMMARY_CONTRACT, "available": True, "reason": None, "underlyer": underlyer,
        "session_date": session_date.isoformat(), "observed_at": observed_at.isoformat(), "source": source,
        "spot": _num(spot), "contracts": len(rows), "priced_contracts": len(local_inputs),
        "tenors": tenors, "expiries": expiries,
        "put_call": {
            "volume_ratio": None if not call_volume else _round(put_volume / call_volume, 4),
            "open_interest_ratio": None if not call_oi else _round(put_oi / call_oi, 4),
            "put_volume": put_volume, "call_volume": call_volume,
            "put_open_interest": put_oi, "call_open_interest": call_oi,
        },
        "quality_reasons": reasons,
        "definitions": (
            "Our IV and delta (local, quote midpoint, stored rate and dividend; whole days / 365); provider values as a "
            "cross-check. ATM IV: median comparable IV at the two strikes bracketing the forward. 25-delta: linear in "
            "delta. Tenors: total-variance interpolation between listed expiries (1+ day), flat within "
            f"{FLAT_EXTRAPOLATION_DAYS} days outside, else unavailable. Expected move: ATM straddle mid at the strike "
            "nearest the forward; one-sigma: spot x IV x sqrt(T). Open interest is prior settlement."
        ),
        "execution_permission": False,
    }
