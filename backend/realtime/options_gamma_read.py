"""G106 A3 (Nov 12 night): gamma exposure and open-interest walls per underlying, computed from the stored chain capture
(`rt.option_breadth_current`) at read time and kept per capture, like the R47 breadth row cache.

Uses the existing analytics in `options/analytics` (the older forward pipeline does not run from this code base, user
Nov 12): `build_gamma_profile` (per-strike gamma notional, flip spot, regime at spot), `detect_gamma_walls`,
`detect_oi_wall_clusters`. Open interest is the provider's prior-settlement figure; IV is ours (`local_iv`), else the
provider's. Which side of open interest dealers hold is not observable: the dealer convention is a stated hypothesis
(DEALER_LONG_CALLS_SHORT_PUTS) and only the sign of net gamma depends on it; walls and OI concentration do not.
Research observation only; behind `REALTIME_V2_OPTIONS_GAMMA_READ` on the API (off by default: the route answers 404)."""
from __future__ import annotations

import os
import threading
from collections import OrderedDict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Mapping

OPTIONS_GAMMA_READ_SWITCH = "REALTIME_V2_OPTIONS_GAMMA_READ"
GAMMA_READ_CONTRACT = "options-gamma-read-v1"
_CACHE_SIZE = 32
_CACHE: "OrderedDict[tuple, dict]" = OrderedDict()
_CACHE_LOCK = threading.Lock()
_YEAR_SECONDS = 365.25 * 86400


def options_gamma_read_enabled(environment=None) -> bool:
    value = (os.environ if environment is None else environment).get(OPTIONS_GAMMA_READ_SWITCH, "OFF")
    return str(value).strip().upper() == "ON"


def _num(value) -> str:
    """A strike as plain text: 100, 762.5 (never 1E+2)."""
    return format(Decimal(str(value)).normalize(), "f")


def _row(values, names):
    return dict(values) if isinstance(values, Mapping) else dict(zip(names, values))


def _expiry_close(expiration: date) -> datetime:
    # Standard US equity options stop trading at 16:00 New York; 21:00 UTC is a fixed, slightly late proxy in winter.
    return datetime(expiration.year, expiration.month, expiration.day, 21, tzinfo=timezone.utc)


def gamma_from_breadth_cursor(cursor, *, underlyer: str, available_by: datetime, session_date: date | None = None) -> dict:
    """The gamma read for one underlying at `available_by`, from the latest stored capture visible then."""
    if session_date is None:
        cursor.execute(
            "SELECT max(session_date) FROM rt.option_breadth_current WHERE underlying=%s AND observed_at<=%s",
            (underlyer, available_by),
        )
        found = cursor.fetchone()
        session_date = (found[0] if not isinstance(found, Mapping) else next(iter(found.values()))) if found else None
        if session_date is None:
            return {"contract": GAMMA_READ_CONTRACT, "available": False, "reason": "NO_CHAIN_CAPTURE",
                    "underlyer": underlyer}
    names = ("contract_type", "expiration_date", "strike_price", "open_interest", "local_iv", "implied_volatility",
             "risk_free_rate", "dividend_yield", "spot", "underlying_price", "observed_at")
    cursor.execute(
        "SELECT " + ",".join(names) + " FROM rt.option_breadth_current "
        "WHERE session_date=%s AND underlying=%s AND observed_at<=%s",
        (session_date, underlyer, available_by),
    )
    rows = [_row(values, names) for values in cursor.fetchall()]
    if not rows:
        return {"contract": GAMMA_READ_CONTRACT, "available": False, "reason": "NO_CHAIN_CAPTURE", "underlyer": underlyer,
                "session_date": session_date.isoformat()}
    observed = max(row["observed_at"] for row in rows)
    # The current-rows table keeps rows of contracts that left the band in earlier captures (their observed_at and spot
    # are older; G102 R55): the gamma read uses the latest capture's rows only.
    rows = [row for row in rows if row["observed_at"] == observed]
    key = (underlyer, session_date, len(rows), observed, available_by.date())
    with _CACHE_LOCK:
        cached = _CACHE.get(key)
        if cached is not None:
            _CACHE.move_to_end(key)
            return cached
    result = _compute(rows, underlyer=underlyer, session_date=session_date, observed_at=observed, now=available_by)
    with _CACHE_LOCK:
        _CACHE[key] = result
        _CACHE.move_to_end(key)
        while len(_CACHE) > _CACHE_SIZE:
            _CACHE.popitem(last=False)
    return result


def _compute(rows, *, underlyer, session_date, observed_at, now) -> dict:
    from options.analytics.gamma_exposure import GammaExposureInput, build_gamma_profile, detect_gamma_walls
    from options.analytics.oi_walls import OiWallInput, detect_oi_wall_clusters
    from options.domain import ContractType, DealerConvention, GammaScope

    spot = next((Decimal(str(row["spot"] or row["underlying_price"])) for row in rows
                 if row.get("spot") or row.get("underlying_price")), None)
    if spot is None or spot <= 0:
        return {"contract": GAMMA_READ_CONTRACT, "available": False, "reason": "SPOT_UNAVAILABLE", "underlyer": underlyer,
                "session_date": session_date.isoformat()}
    inputs, oi_by_strike, skipped = [], {}, 0
    for index, row in enumerate(rows, start=1):
        open_interest = int(row["open_interest"] or 0)
        iv = row.get("local_iv") or row.get("implied_volatility")
        expiry = row["expiration_date"]
        years = (_expiry_close(expiry) - now).total_seconds() / _YEAR_SECONDS
        if open_interest <= 0 or not iv or float(iv) <= 0 or years <= 0:
            skipped += 1
            continue
        strike = Decimal(str(row["strike_price"]))
        inputs.append(GammaExposureInput(
            contract_id=index, contract_type=ContractType.CALL if str(row["contract_type"]).lower() == "call" else ContractType.PUT,
            expiration_date=expiry, strike=strike, open_interest=open_interest, local_iv=float(iv),
            time_to_expiration_years=years, risk_free_rate=float(row.get("risk_free_rate") or 0.0),
            dividend_yield=float(row.get("dividend_yield") or 0.0),
        ))
        oi_by_strike[strike] = oi_by_strike.get(strike, 0) + open_interest
    convention = DealerConvention.DEALER_LONG_CALLS_SHORT_PUTS
    if len(inputs) < 20:
        return {"contract": GAMMA_READ_CONTRACT, "available": False, "reason": "TOO_FEW_CONTRACTS", "underlyer": underlyer,
                "session_date": session_date.isoformat(), "contracts": len(inputs)}

    def profile_view(scope):
        profile = build_gamma_profile(tuple(inputs), spot, convention=convention, scope=scope, market_date=now.date(),
                                      minimum_contracts_for_profile=5 if scope is GammaScope.ZERO_DTE else 20)
        walls = detect_gamma_walls(profile, minimum_share=0.05, maximum_distance_fraction=0.1)
        return {
            "net_gamma_notional_per_percent": round(profile.net_gamma_notional_per_percent, 2),
            "absolute_gamma_notional_per_percent": round(profile.absolute_gamma_notional_per_percent, 2),
            "regime_at_spot": profile.flip.regime_at_spot.value,
            "flip_spot": None if profile.flip.flip_spot is None else str(round(profile.flip.flip_spot, 2)),
            "peak_gamma_strike": None if profile.peak_gamma_strike is None else _num(profile.peak_gamma_strike),
            "gamma_walls": [{"strike": _num(wall.strike), "share": round(wall.gamma_share, 4),
                             "distance_to_spot_fraction": round(wall.distance_to_spot_fraction, 4)} for wall in walls[:5]],
            "contracts": profile.contributing_contract_count, "coverage_fraction": round(profile.coverage_fraction, 4),
            "quality_reasons": list(profile.quality_reasons),
        }

    total = profile_view(GammaScope.TOTAL)
    zero_dte = None
    if any(row.expiration_date == now.date() for row in inputs):
        try:
            zero_dte = profile_view(GammaScope.ZERO_DTE)
        except ValueError:
            zero_dte = None
    clusters = detect_oi_wall_clusters(tuple(OiWallInput(k, v) for k, v in oi_by_strike.items()), spot, maximum_clusters=3)
    return {
        "contract": GAMMA_READ_CONTRACT, "available": True, "reason": None, "underlyer": underlyer,
        "session_date": session_date.isoformat(), "observed_at": observed_at.isoformat(), "spot": _num(spot),
        "convention": convention.value, "total": total, "zero_dte": zero_dte,
        "oi_walls": [{"center_strike": _num(c.center_strike), "total_open_interest": c.total_open_interest,
                      "distance_to_spot_fraction": round(c.distance_to_spot_fraction, 4)} for c in clusters],
        "skipped_contracts": skipped,
        "definitions": ("Research observation from the stored chain capture. Open interest is prior settlement; which "
                        "side dealers hold is a stated convention, so only the sign of net gamma depends on it."),
        "execution_permission": False,
    }
