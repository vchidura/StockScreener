"""G106 OV1 (Nov 16): dealer exposure per underlying for the Options Overview page, from the stored chain capture
(`rt.option_breadth_current`), behind `REALTIME_V2_OPTIONS_EXPOSURE_READ` (off by default: the route answers 404).

Design: `docs/g106/G106_OV1_OPTIONS_OVERVIEW.md`. Per contract, exposure = greek x weight x 100 x dealer sign, in
dollars (gamma: per 1% move, x spot^2 x 0.01; delta: x spot), summed per strike, per expiry and per strike x expiry.
The greek is our Black-Scholes model on our IV (`local_iv`; the provider's IV only where ours is missing, counted), the
capture's spot, stored rate and dividend yield, and the capture's own time to expiry (`_time_to_expiration_years` at
`observed_at`, the convention `local_iv` was solved with), so gamma and delta here equal the stored `local_gamma` and
`local_delta`. Weight is prior-settlement open interest, or open interest plus today's volume (an estimate). Levels
(gamma flip, call wall, put wall, gamma walls) are gamma levels from `options.analytics.gamma_exposure` on the same
contracts and weight in every Greek view. Which side dealers hold is not observable: the convention
(DEALER_LONG_CALLS_SHORT_PUTS) is a stated hypothesis. Only the latest capture's rows are used (R55). One computation
per capture, Greek, weight and scope is kept per process. Research observation only; `execution_permission` is false.
"""
from __future__ import annotations

import os
import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Mapping

import numpy as np

OPTIONS_EXPOSURE_READ_SWITCH = "REALTIME_V2_OPTIONS_EXPOSURE_READ"
OPTIONS_EXPOSURE_GREEKS_SWITCH = "REALTIME_V2_OPTIONS_EXPOSURE_GREEKS"
EXPOSURE_READ_CONTRACT = "options-exposure-read-v1"
IMPLEMENTED_GREEKS = ("gamma", "delta")
GREEKS = ("gamma", "delta", "charm", "vanna")
WEIGHTS = ("oi", "oi_volume")
SCOPES = ("0dte", "7d", "all")
DEFAULT_GREEKS = "gamma,delta"
SHARES_PER_CONTRACT = 100
LARGEST_LEVELS = 10
CROSS_CHECK_TOLERANCE = 0.10
UNCOVERED_WEIGHT_TOLERANCE = 0.01
# The capture's spot is the equity bar at its cutoff; the provider's underlying price travels with the option quotes.
# Our IVs and Greeks are inverted at that spot, so a gap between the two moves short-dated levels most (Nov 12 QQQ
# 0DTE: a 17 bp gap gave a 15.6% net gap to the provider's gamma). Flagged from 10 bp, in line with the cross-check.
SPOT_QUOTE_GAP_TOLERANCE = 0.001
CONVENTION = "DEALER_LONG_CALLS_SHORT_PUTS"
# Measured Nov 12 (SPY, the largest chain): a parsed capture ~0.9 MB, one result ~0.5 MB; so at most ~10 MB + ~24 MB
# per API process. 48 results cover the views of the names being watched for the current capture.
_CHAIN_CACHE_SIZE = 12
_RESULT_CACHE_SIZE = 48
_CHAIN_CACHE: "OrderedDict[tuple, ExposureChain]" = OrderedDict()
_RESULT_CACHE: "OrderedDict[tuple, dict]" = OrderedDict()
_CACHE_LOCK = threading.Lock()

UNITS = {
    "gamma": {"value": "USD of dealer delta change per 1% move in the underlying",
              "shares": "shares to hedge per 1% move (value / spot)"},
    "delta": {"value": "USD of dealer delta (delta-equivalent notional)",
              "shares": "delta-equivalent shares (value / spot)"},
}
DEFINITIONS = (
    "Research observation from the stored chain capture, not a trade signal. Exposure = greek x weight x 100 x dealer "
    "sign; dealers are assumed long calls and short puts (a stated convention: open interest does not say who holds "
    "which side). Open interest is the previous settlement; today's changes show only in the open interest + volume "
    "weight, which is an estimate. Coverage is the captured band (about +/-20% of spot, expiries up to about 60 days), "
    "not the full listed chain."
)


def options_exposure_read_enabled(environment=None) -> bool:
    value = (os.environ if environment is None else environment).get(OPTIONS_EXPOSURE_READ_SWITCH, "OFF")
    return str(value).strip().upper() == "ON"


def enabled_greeks(environment=None) -> tuple[str, ...]:
    """The Greeks this process serves: implemented ones named in `REALTIME_V2_OPTIONS_EXPOSURE_GREEKS` (default
    gamma,delta). Phase 2 adds charm and vanna to the implemented set; the flag turns each on separately."""
    value = (os.environ if environment is None else environment).get(OPTIONS_EXPOSURE_GREEKS_SWITCH, DEFAULT_GREEKS)
    named = {item.strip().lower() for item in str(value).split(",") if item.strip()}
    return tuple(greek for greek in IMPLEMENTED_GREEKS if greek in named)


@dataclass(frozen=True)
class ExposureChain:
    """One capture of one underlying, as arrays (one entry per contract), with our gamma and delta at spot."""

    underlyer: str
    session_date: date
    observed_at: datetime
    spot: Decimal
    strike: np.ndarray
    strike_text: tuple[str, ...]
    is_call: np.ndarray
    expiry: np.ndarray
    calendar_dte: np.ndarray
    open_interest: np.ndarray
    day_volume: np.ndarray
    volatility: np.ndarray
    provider_iv_used: np.ndarray
    maturity: np.ndarray
    rate: np.ndarray
    dividend: np.ndarray
    gamma: np.ndarray
    delta: np.ndarray
    provider_gamma: np.ndarray
    provider_delta: np.ndarray
    earlier_capture_rows: int
    quote_underlying_price: Decimal | None = None  # median provider underlying price on the capture's rows

    @property
    def size(self) -> int:
        return int(self.strike.size)


def _num(value) -> str:
    """A strike or price as plain text: 100, 762.5 (never 1E+2)."""
    return format(Decimal(str(value)).normalize(), "f")


def _row(values, names):
    return dict(values) if isinstance(values, Mapping) else dict(zip(names, values))


def _float(value) -> float:
    if value is None or value == "":
        return float("nan")
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")


def _count(value) -> float:
    """Open interest or volume as a weight: a missing value weighs nothing."""
    number = _float(value)
    return number if np.isfinite(number) and number > 0 else 0.0


_ROW_NAMES = ("contract_type", "expiration_date", "strike_price", "open_interest", "day_volume", "local_iv",
              "implied_volatility", "gamma", "delta", "risk_free_rate", "dividend_yield", "spot", "underlying_price",
              "observed_at")


def build_exposure_chain(rows, *, underlyer: str, session_date: date, observed_at: datetime,
                         earlier_capture_rows: int = 0) -> ExposureChain | None:
    """The capture's contracts as arrays, or None when the capture has no spot."""
    from options.analytics.greeks import black_scholes_delta, black_scholes_gamma

    from .advanced_options_projection import _time_to_expiration_years

    spot = next((Decimal(str(row.get("spot") or row.get("underlying_price"))) for row in rows
                 if row.get("spot") or row.get("underlying_price")), None)
    if spot is None or spot <= 0:
        return None
    count = len(rows)
    strike = np.empty(count)
    is_call = np.zeros(count, dtype=bool)
    expiry = np.empty(count, dtype=np.int64)
    dte = np.empty(count, dtype=np.int64)
    open_interest, volume = np.zeros(count), np.zeros(count)
    volatility, provider_iv_used = np.full(count, np.nan), np.zeros(count, dtype=bool)
    maturity, rate, dividend = np.full(count, np.nan), np.zeros(count), np.zeros(count)
    provider_gamma, provider_delta = np.full(count, np.nan), np.full(count, np.nan)
    strike_text = []
    for index, row in enumerate(rows):
        expiration = row["expiration_date"]
        strike[index] = float(row["strike_price"])
        strike_text.append(_num(row["strike_price"]))
        is_call[index] = str(row["contract_type"]).lower() == "call"
        expiry[index] = expiration.toordinal()
        dte[index] = (expiration - session_date).days
        open_interest[index] = _count(row.get("open_interest"))
        volume[index] = _count(row.get("day_volume"))
        local_iv, provider_iv = _float(row.get("local_iv")), _float(row.get("implied_volatility"))
        if np.isfinite(local_iv) and local_iv > 0:
            volatility[index] = local_iv
        elif np.isfinite(provider_iv) and provider_iv > 0:
            volatility[index], provider_iv_used[index] = provider_iv, True
        years = _time_to_expiration_years(expiration, observed_at)
        if years is not None and years > 0:
            maturity[index] = years
        rate[index] = 0.0 if row.get("risk_free_rate") is None else _float(row["risk_free_rate"])
        dividend[index] = 0.0 if row.get("dividend_yield") is None else _float(row["dividend_yield"])
        provider_gamma[index], provider_delta[index] = _float(row.get("gamma")), _float(row.get("delta"))
    spots = np.full(count, float(spot))
    rate, dividend = np.nan_to_num(rate), np.nan_to_num(dividend)
    quoted = sorted(Decimal(str(row["underlying_price"])) for row in rows
                    if row.get("underlying_price") is not None and Decimal(str(row["underlying_price"])) > 0)
    quote_underlying_price = quoted[len(quoted) // 2] if quoted else None
    return ExposureChain(
        underlyer=underlyer, session_date=session_date, observed_at=observed_at, spot=spot, strike=strike,
        strike_text=tuple(strike_text), is_call=is_call, expiry=expiry, calendar_dte=dte, open_interest=open_interest,
        day_volume=volume, volatility=volatility, provider_iv_used=provider_iv_used, maturity=maturity, rate=rate,
        dividend=dividend,
        gamma=black_scholes_gamma(spots, strike, maturity, rate, dividend, volatility),
        delta=black_scholes_delta(spots, strike, maturity, rate, dividend, volatility, is_call),
        provider_gamma=provider_gamma, provider_delta=provider_delta, earlier_capture_rows=earlier_capture_rows,
        quote_underlying_price=quote_underlying_price,
    )


def _weight(chain: ExposureChain, weight: str) -> np.ndarray:
    return chain.open_interest if weight == "oi" else chain.open_interest + chain.day_volume


def _scope(chain: ExposureChain, scope: str) -> np.ndarray:
    if scope == "0dte":
        return chain.calendar_dte == 0
    if scope == "7d":
        return chain.calendar_dte <= 7
    return np.ones(chain.size, dtype=bool)


def _unit(chain: ExposureChain, greek: str, provider: bool = False) -> np.ndarray:
    """Dollars per contract and unit of weight, unsigned by the dealer side."""
    spot = float(chain.spot)
    if greek == "gamma":
        return (chain.provider_gamma if provider else chain.gamma) * SHARES_PER_CONTRACT * spot * spot * 0.01
    return (chain.provider_delta if provider else chain.delta) * SHARES_PER_CONTRACT * spot


def _signs(chain: ExposureChain) -> np.ndarray:
    from options.analytics.gamma_exposure import dealer_signs
    from options.domain import DealerConvention

    call_sign, put_sign = dealer_signs(DealerConvention(CONVENTION))
    return np.where(chain.is_call, call_sign, put_sign)


def _whole(value: float) -> int:
    return int(round(float(value)))


def _iso_day(ordinal: int) -> str:
    return date.fromordinal(int(ordinal)).isoformat()


def gamma_levels(chain: ExposureChain, *, weight: str, scope: str) -> dict:
    """Gamma flip, regime at spot, call wall, put wall, peak and gamma walls, by `build_gamma_profile` and
    `detect_gamma_walls` on the contracts in scope with the chosen weight."""
    from options.analytics.gamma_exposure import GammaExposureInput, build_gamma_profile, detect_gamma_walls
    from options.domain import ContractType, DealerConvention

    weights = _weight(chain, weight)
    usable = _scope(chain, scope) & (weights >= 1) & np.isfinite(chain.maturity) & np.isfinite(chain.volatility)
    inputs = tuple(
        GammaExposureInput(
            contract_id=int(index), contract_type=ContractType.CALL if chain.is_call[index] else ContractType.PUT,
            expiration_date=date.fromordinal(int(chain.expiry[index])), strike=Decimal(chain.strike_text[index]),
            open_interest=int(weights[index]), local_iv=float(chain.volatility[index]),
            time_to_expiration_years=float(chain.maturity[index]), risk_free_rate=float(chain.rate[index]),
            dividend_yield=float(chain.dividend[index]),
        )
        for index in np.flatnonzero(usable)
    )
    profile = build_gamma_profile(inputs, chain.spot, convention=DealerConvention(CONVENTION),
                                  minimum_contracts_for_profile=5 if scope == "0dte" else 20)
    spot = float(chain.spot)

    def level(strike, value):
        return None if strike is None else {
            "strike": _num(strike), "gamma_notional_per_percent": _whole(value),
            "distance_to_spot_fraction": round((float(strike) - spot) / spot, 4),
        }

    call_wall = max(profile.strikes, key=lambda row: (row.call_gamma_notional_per_percent, row.strike), default=None)
    put_wall = max(profile.strikes, key=lambda row: (row.put_gamma_notional_per_percent, -row.strike), default=None)
    walls = detect_gamma_walls(profile, minimum_share=0.05, maximum_distance_fraction=0.1) if profile.strikes else ()
    return {
        "flip_spot": None if profile.flip.flip_spot is None else str(round(profile.flip.flip_spot, 2)),
        "flip_distance_to_spot_fraction": None if profile.flip.flip_spot is None
        else round((float(profile.flip.flip_spot) - spot) / spot, 4),
        "regime_at_spot": profile.flip.regime_at_spot.value,
        "flip_search": {"low": str(round(profile.flip.searched_low_spot, 2)),
                        "high": str(round(profile.flip.searched_high_spot, 2)),
                        "crossings": profile.flip.sign_change_count},
        "call_wall": None if call_wall is None or call_wall.call_gamma_notional_per_percent <= 0
        else level(call_wall.strike, call_wall.call_gamma_notional_per_percent),
        "put_wall": None if put_wall is None or put_wall.put_gamma_notional_per_percent <= 0
        else level(put_wall.strike, put_wall.put_gamma_notional_per_percent),
        "peak_gamma_strike": None if profile.peak_gamma_strike is None else _num(profile.peak_gamma_strike),
        "gamma_walls": [{"strike": _num(wall.strike), "share": round(wall.gamma_share, 4),
                         "distance_to_spot_fraction": round(wall.distance_to_spot_fraction, 4)} for wall in walls[:5]],
        "net_gamma_notional_per_percent": _whole(profile.net_gamma_notional_per_percent),
        "quality_reasons": list(dict.fromkeys((*profile.quality_reasons, *profile.flip.reasons))),
    }


def compute_exposure(chain: ExposureChain, *, greek: str, weight: str, scope: str) -> dict:
    """The exposure view of one capture for one Greek, weight and scope (pure; cached by the caller)."""
    if greek not in IMPLEMENTED_GREEKS:
        raise ValueError(f"Greek {greek} is not implemented")
    if weight not in WEIGHTS or scope not in SCOPES:
        raise ValueError("Invalid exposure weight or scope")
    spot = float(chain.spot)
    weights = _weight(chain, weight)
    signs = _signs(chain)
    unit = _unit(chain, greek)
    value = signs * unit * weights
    in_scope = _scope(chain, scope)
    weighted = in_scope & (weights > 0)
    live = weighted & np.isfinite(chain.maturity)
    contributing = live & np.isfinite(unit)
    head = {
        "contract": EXPOSURE_READ_CONTRACT, "underlyer": chain.underlyer, "session_date": chain.session_date.isoformat(),
        "observed_at": chain.observed_at.isoformat(), "spot": _num(chain.spot), "greek": greek, "weight": weight,
        "scope": scope, "convention": CONVENTION, "units": UNITS[greek], "definitions": DEFINITIONS,
        "execution_permission": False,
    }
    all_expiries = sorted({int(item) for item in chain.expiry})
    coverage = {
        "capture_contracts": chain.size,
        "earlier_capture_rows_excluded": chain.earlier_capture_rows,
        "scope_contracts": int(in_scope.sum()),
        "contributing_contracts": int(contributing.sum()),
        "skipped": {
            "zero_weight": int((in_scope & ~(weights > 0)).sum()),
            "expired": int((weighted & ~np.isfinite(chain.maturity)).sum()),
            "no_greek": int((live & ~np.isfinite(unit)).sum()),
        },
        "weight_in_scope": _whole(weights[live].sum()),
        "weight_with_greeks": _whole(weights[contributing].sum()),
        "weight_coverage_fraction": None if weights[live].sum() <= 0
        else round(float(weights[contributing].sum() / weights[live].sum()), 4),
        "provider_iv_contracts": int((contributing & chain.provider_iv_used).sum()),
        "strike_low": _num(Decimal(str(chain.strike.min()))) if chain.size else None,
        "strike_high": _num(Decimal(str(chain.strike.max()))) if chain.size else None,
        "strike_band_fraction": None if not chain.size else [round(float(chain.strike.min()) / spot - 1, 4),
                                                             round(float(chain.strike.max()) / spot - 1, 4)],
        "expiry_first": _iso_day(all_expiries[0]) if all_expiries else None,
        "expiry_last": _iso_day(all_expiries[-1]) if all_expiries else None,
        "expiry_count": len(all_expiries),
        "spot_check": {
            "spot": _num(chain.spot),
            "quote_underlying_price": None if chain.quote_underlying_price is None
            else _num(chain.quote_underlying_price),
            "gap_fraction": None if not chain.quote_underlying_price
            else round(float(chain.spot / chain.quote_underlying_price) - 1, 5),
        },
    }
    if not contributing.any():
        reason = ("NO_CONTRACTS_IN_SCOPE" if not in_scope.any()
                  else "SCOPE_EXPIRED" if weighted.any() and not live.any() else "NO_EXPOSURE_IN_SCOPE")
        return {**head, "available": False, "reason": reason, "coverage": coverage, "quality_reasons": [reason]}

    # Per strike, per expiry and per strike x expiry, for calls and puts separately (the net is their sum).
    picked = np.flatnonzero(contributing)
    strikes, strike_index = np.unique(chain.strike[picked], return_inverse=True)
    expiries, expiry_index = np.unique(chain.expiry[picked], return_inverse=True)
    values, calls = value[picked], chain.is_call[picked]
    call_values, put_values = np.where(calls, values, 0.0), np.where(calls, 0.0, values)
    call_strike = np.bincount(strike_index, weights=call_values, minlength=strikes.size)
    put_strike = np.bincount(strike_index, weights=put_values, minlength=strikes.size)
    gross_strike = np.bincount(strike_index, weights=np.abs(values), minlength=strikes.size)
    call_oi = np.bincount(strike_index, weights=np.where(calls, chain.open_interest[picked], 0), minlength=strikes.size)
    put_oi = np.bincount(strike_index, weights=np.where(calls, 0, chain.open_interest[picked]), minlength=strikes.size)
    call_expiry = np.bincount(expiry_index, weights=call_values, minlength=expiries.size)
    put_expiry = np.bincount(expiry_index, weights=put_values, minlength=expiries.size)
    gross_expiry = np.bincount(expiry_index, weights=np.abs(values), minlength=expiries.size)
    count_expiry = np.bincount(expiry_index, minlength=expiries.size)
    cell = strike_index * expiries.size + expiry_index
    call_cell = np.bincount(cell, weights=call_values, minlength=strikes.size * expiries.size)
    put_cell = np.bincount(cell, weights=put_values, minlength=strikes.size * expiries.size)
    filled = np.bincount(cell, minlength=strikes.size * expiries.size) > 0
    text_by_strike = {}
    for index in picked:
        text_by_strike.setdefault(float(chain.strike[index]), chain.strike_text[index])
    strike_texts = [text_by_strike[float(item)] for item in strikes]
    absolute = float(np.abs(values).sum())
    net_strike = call_strike + put_strike

    levels = gamma_levels(chain, weight=weight, scope=scope)
    tags: dict[str, list[str]] = {}
    for name, key in (("CALL_WALL", "call_wall"), ("PUT_WALL", "put_wall")):
        if levels.get(key):
            tags.setdefault(levels[key]["strike"], []).append(name)
    if levels.get("peak_gamma_strike"):
        tags.setdefault(levels["peak_gamma_strike"], []).append("PEAK_GAMMA")
    ranked = sorted(range(strikes.size), key=lambda index: (-abs(net_strike[index]), strikes[index]))[:LARGEST_LEVELS]

    # Like for like: the provider's Greek on the contracts where both exist, same weight, sign and scaling. Compared
    # on gross exposure (the net can sit near zero around the flip), and the net difference as a share of that gross.
    provider_unit = _unit(chain, greek, provider=True)
    both = contributing & np.isfinite(provider_unit)
    local_both, provider_both = value[both], (signs * provider_unit * weights)[both]
    provider_gross = float(np.abs(provider_both).sum())
    difference = None if provider_gross == 0 else (float(np.abs(local_both).sum()) - provider_gross) / provider_gross
    net_difference = None if provider_gross == 0 else float(local_both.sum() - provider_both.sum()) / provider_gross

    every = (weights > 0) & np.isfinite(chain.maturity) & np.isfinite(unit)
    every_absolute = float(np.abs(value[every]).sum())
    today_absolute = float(np.abs(value[every & (chain.calendar_dte == 0)]).sum())
    reasons = []
    if weight == "oi_volume":
        reasons.append("VOLUME_WEIGHT_ESTIMATE")
    if coverage["weight_coverage_fraction"] is not None and coverage["weight_coverage_fraction"] < 1 - UNCOVERED_WEIGHT_TOLERANCE:
        reasons.append("WEIGHT_WITHOUT_GREEKS")
    if coverage["provider_iv_contracts"]:
        reasons.append("PROVIDER_IV_FALLBACK")
    spot_gap = coverage["spot_check"]["gap_fraction"]
    if spot_gap is not None and abs(spot_gap) > SPOT_QUOTE_GAP_TOLERANCE:
        reasons.append("SPOT_QUOTE_GAP")
    if any(item is not None and abs(item) > CROSS_CHECK_TOLERANCE for item in (difference, net_difference)):
        reasons.append("PROVIDER_CROSS_CHECK_DIVERGES")
    if contributing.sum() < (5 if scope == "0dte" else 20):
        reasons.append("INSUFFICIENT_CONTRACTS")
    reasons.extend(item for item in levels.pop("quality_reasons") if item not in reasons)

    return {
        **head, "available": True, "reason": None,
        "totals": {
            "net": _whole(net_strike.sum()), "call": _whole(call_strike.sum()), "put": _whole(put_strike.sum()),
            "absolute": _whole(absolute), "net_shares": _whole(net_strike.sum() / spot),
            "contracts": int(contributing.sum()),
        },
        "levels": levels,
        "strikes": [
            {"strike": strike_texts[index], "call": _whole(call_strike[index]), "put": _whole(put_strike[index]),
             "net": _whole(net_strike[index]), "call_open_interest": int(call_oi[index]),
             "put_open_interest": int(put_oi[index])}
            for index in range(strikes.size)
        ],
        "expiries": [
            {"expiration_date": _iso_day(expiries[index]),
             "calendar_dte": (date.fromordinal(int(expiries[index])) - chain.session_date).days,
             "call": _whole(call_expiry[index]), "put": _whole(put_expiry[index]),
             "net": _whole(call_expiry[index] + put_expiry[index]),
             "absolute": _whole(gross_expiry[index]),
             "share": round(float(gross_expiry[index] / absolute), 4) if absolute else 0.0,
             "contracts": int(count_expiry[index])}
            for index in range(expiries.size)
        ],
        # Strike x expiry: [strike index, expiry index, call, put] for every filled cell, into `strikes`/`expiries`.
        "grid": [[int(index // expiries.size), int(index % expiries.size), _whole(call_cell[index]),
                  _whole(put_cell[index])] for index in np.flatnonzero(filled)],
        "largest_levels": [
            {"strike": strike_texts[index], "net": _whole(net_strike[index]), "call": _whole(call_strike[index]),
             "put": _whole(put_strike[index]),
             "share": round(float(gross_strike[index] / absolute), 4) if absolute else 0.0,
             "distance_to_spot_fraction": round((float(strikes[index]) - spot) / spot, 4),
             "tags": tags.get(strike_texts[index], [])}
            for index in ranked
        ],
        "expiring_today_share": round(today_absolute / every_absolute, 4) if every_absolute else 0.0,
        "coverage": coverage,
        "cross_check": {
            "source": "PROVIDER_GREEKS", "contracts_compared": int(both.sum()),
            "provider_missing_contracts": int((contributing & ~np.isfinite(provider_unit)).sum()),
            "local_absolute": _whole(np.abs(local_both).sum()), "provider_absolute": _whole(provider_gross),
            "local_net": _whole(local_both.sum()), "provider_net": _whole(provider_both.sum()),
            "difference_fraction": None if difference is None else round(difference, 4),
            "net_difference_fraction": None if net_difference is None else round(net_difference, 4),
        },
        "quality_reasons": reasons,
    }


def _cache_get(cache, key):
    with _CACHE_LOCK:
        found = cache.get(key)
        if found is not None:
            cache.move_to_end(key)
        return found


def _cache_put(cache, key, value, size) -> None:
    with _CACHE_LOCK:
        cache[key] = value
        cache.move_to_end(key)
        while len(cache) > size:
            cache.popitem(last=False)


def _scalar(found):
    if found is None:
        return None
    return next(iter(found.values())) if isinstance(found, Mapping) else found[0]


def _capture_label(cursor, *, underlyer, session_date, available_by, shown_at) -> tuple[dict | None, list[str]]:
    """R48: a FAILED/PARTIAL capture writes no rows, so the rows shown are the last COMPLETE capture; say so when a
    newer capture did not complete."""
    names = ("state", "observed_at", "contracts", "reason")
    cursor.execute(
        "SELECT state,observed_at,contracts,reason FROM rt.option_breadth_captures "
        "WHERE session_date=%s AND underlying=%s AND observed_at<=%s ORDER BY observed_at DESC LIMIT 1",
        (session_date, underlyer, available_by),
    )
    found = cursor.fetchone()
    if not found:
        return None, []
    item = _row(found, names)
    label = {"latest_capture_state": item["state"], "latest_capture_at": item["observed_at"].isoformat(),
             "latest_capture_reason": item["reason"]}
    if item["state"] != "COMPLETE" and item["observed_at"] > shown_at:
        return label, ["LATEST_CAPTURE_NOT_COMPLETE"]
    return label, []


def _underlyers(cursor, session_date) -> list[str]:
    cursor.execute("SELECT DISTINCT underlying FROM rt.option_breadth_captures WHERE session_date=%s ORDER BY 1",
                   (session_date,))
    return [str(_scalar(row)) for row in cursor.fetchall()]


def exposure_from_breadth_cursor(cursor, *, underlyer: str, available_by: datetime, greek: str = "gamma",
                                 weight: str = "oi", scope: str = "all", session_date: date | None = None,
                                 environment=None) -> dict:
    """The exposure read for one underlying at `available_by`, from the latest stored capture visible then."""
    if greek not in GREEKS or weight not in WEIGHTS or scope not in SCOPES:
        raise ValueError("Invalid exposure Greek, weight or scope")
    greeks = enabled_greeks(environment)
    if greek not in greeks:
        raise ValueError(f"Exposure Greek {greek} is not enabled")
    base = {"contract": EXPOSURE_READ_CONTRACT, "underlyer": underlyer, "greek": greek, "weight": weight,
            "scope": scope, "greeks_enabled": list(greeks), "execution_permission": False}
    if session_date is None:
        cursor.execute(
            "SELECT max(session_date) FROM rt.option_breadth_current WHERE underlying=%s AND observed_at<=%s",
            (underlyer, available_by),
        )
        session_date = _scalar(cursor.fetchone())
        if session_date is None:
            return {**base, "available": False, "reason": "NO_CHAIN_CAPTURE", "quality_reasons": ["NO_CHAIN_CAPTURE"]}
    base.update(session_date=session_date.isoformat(), underlyers=_underlyers(cursor, session_date))
    # The capture's identity: its observed_at and row count (a capture rewrites its underlying's rows in one statement
    # with a new observed_at). Rows left by earlier captures (contracts that left the band) are counted, not used (R55).
    cursor.execute(
        "SELECT observed_at,count(*) FROM rt.option_breadth_current WHERE session_date=%s AND underlying=%s "
        "AND observed_at<=%s GROUP BY observed_at ORDER BY observed_at DESC",
        (session_date, underlyer, available_by),
    )
    groups = [_row(values, ("observed_at", "count")) for values in cursor.fetchall()]
    if not groups:
        return {**base, "available": False, "reason": "NO_CHAIN_CAPTURE", "quality_reasons": ["NO_CHAIN_CAPTURE"]}
    observed, rows_in_capture = groups[0]["observed_at"], int(groups[0]["count"])
    earlier = sum(int(group["count"]) for group in groups[1:])
    label, label_reasons = _capture_label(cursor, underlyer=underlyer, session_date=session_date,
                                          available_by=available_by, shown_at=observed)
    key = (underlyer, session_date, observed, rows_in_capture)
    result = _cache_get(_RESULT_CACHE, (key, greek, weight, scope))
    if result is None:
        chain = _cache_get(_CHAIN_CACHE, key)
        if chain is None:
            cursor.execute(
                "SELECT " + ",".join(_ROW_NAMES) + " FROM rt.option_breadth_current "
                "WHERE session_date=%s AND underlying=%s AND observed_at=%s",
                (session_date, underlyer, observed),
            )
            rows = [_row(values, _ROW_NAMES) for values in cursor.fetchall()]
            chain = build_exposure_chain(rows, underlyer=underlyer, session_date=session_date, observed_at=observed,
                                         earlier_capture_rows=earlier)
            if chain is None:
                return {**base, "available": False, "reason": "SPOT_UNAVAILABLE", "observed_at": observed.isoformat(),
                        "quality_reasons": ["SPOT_UNAVAILABLE"]}
            _cache_put(_CHAIN_CACHE, key, chain, _CHAIN_CACHE_SIZE)
        result = compute_exposure(chain, greek=greek, weight=weight, scope=scope)
        _cache_put(_RESULT_CACHE, (key, greek, weight, scope), result, _RESULT_CACHE_SIZE)
    return {**result, **base, "capture": label,
            "quality_reasons": [*label_reasons, *(item for item in result["quality_reasons"] if item not in label_reasons)]}
