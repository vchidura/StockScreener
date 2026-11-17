"""G106 OV1-F2: the spot the chain capture solves our IV and Greeks at.

Until now breadth solved every contract at the equity spot the scheduler read when the capture began
(`_breadth_spots`: the latest 1m RAW bar known at that moment, in practice the bar at the window's equity cutoff). The
chain's quotes are a minute or two later (pages are fetched one underlying after another), so on Nov 12 the spot sat
5-18 bp away from the quotes for most names (up to 59 bp for TSLA and 62 bp for NVDA). Near-money short-dated calls
then solved to too high an IV and puts to too low a one (or below the no-arbitrage bound: no IV at all), and our 0 DTE
gamma split calls x0.81 / puts x1.42 against the provider (QQQ 11:07 PT).

The spot consistent with the quotes is the one the quotes themselves imply: put-call parity at the strikes nearest the
forward of the nearest listed expiry (F = K + e^(rT) (C - P), the VIX-style forward), brought back to spot with the
same rate and dividend yield and the same year fraction the solve uses (S = F e^(-(r-q)T)). It is read from the very
quotes being solved, so no clock alignment is needed, and it absorbs whatever carry the stored inputs miss for that
expiry. The provider's underlying price in the same snapshot is the fallback (when it is real-time and fresh) and the
cross-check; the equity bar stays the last resort and today's behaviour. Measured offline on 257 stored captures
(Nov 9-16): parity passed in all of them, sat a median 1.8 bp from the provider's price (p90 5 bp), and brought
same-strike call/put IVs together (QQQ median 9.0 -> 0.2 vol points at the parity strikes) and our 0 DTE gamma back to
the provider's (QQQ 11:07 PT: net gap -15.6% -> -0.03% of gross).

`REALTIME_V3_BREADTH_SPOT_SOURCE` on the Options scheduler:
- unset, blank or `BAR`: today's capture, byte for byte (nothing here runs).
- `RECORD`: the capture is unchanged; one `BREADTH_SPOT_RECORD` log line per underlying and capture says what the
  quote spot would be and what it would change (ATM call/put IV agreement, no-arbitrage failures, 0 DTE gamma against
  the provider's, before and after).
- `QUOTE`: our IV and Greeks are solved at the quote spot; the row's `spot` is that solve spot (no new column; the bar
  spot and the source are in the same log line). Any failure here falls back to the bar spot with a reason.
Any other value stops the scheduler at start.
"""
from __future__ import annotations

import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Mapping, Sequence

BREADTH_SPOT_SOURCE_SETTING = "REALTIME_V3_BREADTH_SPOT_SOURCE"
BREADTH_SPOT_RECORD_EVENT = "BREADTH_SPOT_RECORD"

SOURCE_BAR = "BAR"
SOURCE_PARITY = "PARITY"
SOURCE_PROVIDER = "PROVIDER"

BREADTH_SPOT_POLICY = {
    "version": "breadth-spot-v1",
    # Per expiry: the strikes with the smallest |call mid - put mid| (nearest the forward), both sides two-sided.
    "parity_pairs": 5,
    "parity_minimum_pairs": 3,
    # The per-strike forwards must agree: median absolute deviation around their median at most 10 bp.
    "parity_maximum_dispersion_fraction": "0.0010",
    # The nearest expiry that passes, at most this many calendar days out.
    "parity_maximum_days": 14,
    # A same-day expiry in its last 30 minutes passes only if its forwards agree within 1 bp: single-name 0 DTE quotes
    # widen and lag as they run out (Nov 11-16 closes: 2-6 bp, 5-10 bp off the provider), index ETF ones stay tight
    # (under 0.5 bp) and remain the best spot for 0 DTE.
    "parity_last_minutes": 30,
    "parity_last_minutes_maximum_dispersion_fraction": "0.0001",
    # The provider's underlying price: real-time, at most this much older than the capture's newest quote.
    "provider_maximum_age_seconds": 120,
    # Parity and the provider must agree within 50 bp when both exist; a quote spot never moves more than 5% from the
    # bar (the strike band was drawn around the bar).
    "agreement_fraction": "0.0050",
    "maximum_bar_gap_fraction": "0.05",
}

_TENTHS_OF_A_CENT = Decimal("0.0001")


class BreadthSpotMode(str, Enum):
    BAR = "BAR"
    RECORD = "RECORD"
    QUOTE = "QUOTE"


def breadth_spot_mode(environ=None) -> BreadthSpotMode:
    """The switch value; unset or blank is BAR. Anything but BAR, RECORD or QUOTE raises (fails loudly at start)."""
    raw = (os.environ if environ is None else environ).get(BREADTH_SPOT_SOURCE_SETTING)
    value = (raw or "").strip().upper() or BreadthSpotMode.BAR.value
    try:
        return BreadthSpotMode(value)
    except ValueError:
        raise ValueError(f"{BREADTH_SPOT_SOURCE_SETTING} must be BAR, RECORD or QUOTE, not {raw!r}") from None


@dataclass(frozen=True, slots=True)
class ParitySpot:
    spot: Decimal | None
    expiration_date: date | None = None
    forward: float | None = None
    pairs: int = 0
    dispersion_fraction: float | None = None
    strikes: tuple[Decimal, ...] = ()
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class ProviderSpot:
    spot: Decimal | None
    low: Decimal | None = None
    high: Decimal | None = None
    rows: int = 0
    age_seconds: float | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class SpotChoice:
    spot: Decimal | None
    source: str
    reason: str | None
    bar_spot: Decimal | None
    parity: ParitySpot
    provider: ProviderSpot

    @property
    def quote_spot(self) -> Decimal | None:
        """The spot QUOTE solves at (None when it falls back to the bar)."""
        return None if self.source == SOURCE_BAR else self.spot


def valuation_lookup(valuation_inputs: Sequence[Mapping]) -> dict[str, list[tuple[date, Mapping]]]:
    """The stored inputs by ticker, as `breadth_local_greeks` reads them."""
    by_ticker: dict[str, list[tuple[date, Mapping]]] = {}
    for item in valuation_inputs:
        try:
            by_ticker.setdefault(str(item["ticker"]), []).append((date.fromisoformat(str(item["expiration_date"])), item))
        except (KeyError, ValueError):
            continue
    return by_ticker


def nearest_valuation(by_ticker, underlying: str, expiration: date) -> tuple[date, Mapping] | None:
    """The ticker's input for this expiration, else its nearest stored expiration (ties to the earlier one)."""
    choices = by_ticker.get(underlying) or []
    return min(choices, key=lambda item: (abs((item[0] - expiration).days), item[0]), default=None)


def _carry(by_ticker, underlying: str, expiration: date) -> tuple[float, float] | None:
    chosen = nearest_valuation(by_ticker, underlying, expiration)
    if chosen is None:
        return None
    try:
        rate, dividend = float(chosen[1]["risk_free_rate"]), float(chosen[1]["dividend_yield"])
    except (KeyError, TypeError, ValueError):
        return None
    return (rate, dividend) if math.isfinite(rate) and math.isfinite(dividend) else None


def _mid(row) -> float | None:
    bid, ask = row.bid, row.ask
    if bid is None or ask is None or not bid > 0 or not ask >= bid:
        return None
    return float((bid + ask) / 2)


def _median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    return ordered[middle] if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / 2


def _spot_decimal(value: float) -> Decimal:
    return Decimal(repr(value)).quantize(_TENTHS_OF_A_CENT)


def parity_spot(rows, *, underlying: str, valuation_inputs: Sequence[Mapping], observed_at: datetime,
                policy: Mapping = BREADTH_SPOT_POLICY) -> ParitySpot:
    """The spot the quotes imply: put-call parity at the strikes nearest the forward of the nearest expiry that has
    enough two-sided pairs agreeing with each other, discounted to spot with that expiry's stored rate and dividend
    yield over the solve's own year fraction."""
    from .advanced_options_projection import _time_to_expiration_years

    by_ticker = valuation_lookup(valuation_inputs)
    by_expiry: dict[date, dict[Decimal, dict[str, object]]] = {}
    for row in rows:
        by_expiry.setdefault(row.expiration_date, {}).setdefault(row.strike_price, {}).setdefault(
            row.contract_type, []).append(row)
    maximum_dispersion = float(policy["parity_maximum_dispersion_fraction"])
    reasons = []
    for expiration in sorted(by_expiry):
        days = (expiration - observed_at.date()).days
        if days > int(policy["parity_maximum_days"]):
            break
        years = _time_to_expiration_years(expiration, observed_at)
        if years is None:
            continue
        last_minutes = years * 365.0 * 1440.0 < float(policy["parity_last_minutes"])
        carry = _carry(by_ticker, underlying, expiration)
        rate, dividend = carry if carry is not None else (0.0, 0.0)
        pairs = []
        for strike, sides in by_expiry[expiration].items():
            calls, puts = sides.get("call") or [], sides.get("put") or []
            if len(calls) != 1 or len(puts) != 1:
                continue
            call, put = _mid(calls[0]), _mid(puts[0])
            if call is None or put is None:
                continue
            pairs.append((abs(call - put), strike, float(strike) + math.exp(rate * years) * (call - put)))
        pairs.sort(key=lambda item: (item[0], item[1]))
        used = pairs[: int(policy["parity_pairs"])]
        if len(used) < int(policy["parity_minimum_pairs"]):
            reasons.append(f"{expiration.isoformat()}:PAIRS_{len(used)}")
            continue
        forward = _median([item[2] for item in used])
        if not forward > 0:
            reasons.append(f"{expiration.isoformat()}:FORWARD_INVALID")
            continue
        dispersion = _median([abs(item[2] - forward) for item in used]) / forward
        if dispersion > maximum_dispersion:
            reasons.append(f"{expiration.isoformat()}:DISPERSION_{dispersion * 1e4:.1f}BP")
            continue
        if last_minutes and dispersion > float(policy["parity_last_minutes_maximum_dispersion_fraction"]):
            reasons.append(f"{expiration.isoformat()}:LAST_MINUTES_DISPERSION_{dispersion * 1e4:.1f}BP")
            continue
        spot = forward * math.exp(-(rate - dividend) * years)
        if not (math.isfinite(spot) and spot > 0):
            reasons.append(f"{expiration.isoformat()}:SPOT_INVALID")
            continue
        return ParitySpot(
            spot=_spot_decimal(spot), expiration_date=expiration, forward=forward, pairs=len(used),
            dispersion_fraction=dispersion, strikes=tuple(sorted(item[1] for item in used)),
        )
    return ParitySpot(spot=None, reason=("NO_EXPIRY_PASSED " + ",".join(reasons[:4])).strip()[:120])


def provider_spot(rows, *, observed_at: datetime, policy: Mapping = BREADTH_SPOT_POLICY) -> ProviderSpot:
    """The provider's underlying price on the capture's rows (their median), when it is real-time and fresh."""
    prices = sorted(row.underlying_price for row in rows if row.underlying_price is not None and row.underlying_price > 0)
    if not prices:
        return ProviderSpot(spot=None, reason="NO_PROVIDER_PRICE")
    median = prices[len(prices) // 2]
    timeframes = {str(getattr(row, "underlying_timeframe", None) or "").strip().upper() for row in rows} - {""}
    if timeframes - {"REAL-TIME", "REAL_TIME", "REALTIME"}:
        return ProviderSpot(spot=None, low=prices[0], high=prices[-1], rows=len(prices),
                            reason=f"NOT_REAL_TIME:{','.join(sorted(timeframes))}"[:120])
    stamps = sorted(stamp for stamp in (getattr(row, "underlying_price_at", None) for row in rows) if stamp is not None)
    quotes = [row.quote_at for row in rows if row.quote_at is not None]
    age = None
    if stamps:
        newest = max(quotes) if quotes else observed_at
        age = (newest - stamps[len(stamps) // 2]).total_seconds()
        if age > float(policy["provider_maximum_age_seconds"]):
            return ProviderSpot(spot=None, low=prices[0], high=prices[-1], rows=len(prices), age_seconds=age,
                                reason=f"STALE_{int(age)}S")
    return ProviderSpot(spot=median, low=prices[0], high=prices[-1], rows=len(prices), age_seconds=age)


def choose_breadth_spot(rows, *, underlying: str, bar_spot: Decimal | None, valuation_inputs: Sequence[Mapping],
                        observed_at: datetime, policy: Mapping = BREADTH_SPOT_POLICY) -> SpotChoice:
    """The quote spot: parity, else the provider's price, else the bar (with the reason). Never raises."""
    try:
        parity = parity_spot(rows, underlying=underlying, valuation_inputs=valuation_inputs, observed_at=observed_at,
                             policy=policy)
    except Exception as error:  # noqa: BLE001 - a quote spot is optional; the bar remains
        parity = ParitySpot(spot=None, reason=f"PARITY_FAILED:{type(error).__name__}")
    try:
        provider = provider_spot(rows, observed_at=observed_at, policy=policy)
    except Exception as error:  # noqa: BLE001
        provider = ProviderSpot(spot=None, reason=f"PROVIDER_FAILED:{type(error).__name__}")

    def bar(reason):
        return SpotChoice(spot=bar_spot, source=SOURCE_BAR, reason=reason[:160], bar_spot=bar_spot, parity=parity,
                          provider=provider)

    if bar_spot is None or not bar_spot > 0:
        return bar("BAR_SPOT_UNAVAILABLE")
    agreement = Decimal(str(policy["agreement_fraction"]))
    if parity.spot is not None:
        if provider.spot is not None and abs(parity.spot / provider.spot - 1) > agreement:
            return bar(f"PARITY_PROVIDER_DISAGREE:{float(parity.spot / provider.spot - 1) * 1e4:+.1f}BP")
        source, spot = SOURCE_PARITY, parity.spot
    elif provider.spot is not None:
        source, spot = SOURCE_PROVIDER, provider.spot
    else:
        return bar(f"NO_QUOTE_SPOT parity={parity.reason} provider={provider.reason}")
    if abs(spot / bar_spot - 1) > Decimal(str(policy["maximum_bar_gap_fraction"])):
        return bar(f"{source}_FAR_FROM_BAR:{float(spot / bar_spot - 1) * 1e4:+.0f}BP")
    return SpotChoice(spot=spot, source=source, reason=None, bar_spot=bar_spot, parity=parity, provider=provider)


# ---- the record line ------------------------------------------------------------------------------------------------

def _round(value, digits=4):
    return None if value is None or not math.isfinite(value) else round(float(value), digits)


def _bp(value: Decimal | None, base: Decimal | None):
    return None if value is None or base is None or not base > 0 else round(float(value / base - 1) * 1e4, 1)


def _atm_agreement(rows, local, strikes_by_expiry) -> dict[str, object]:
    """|call IV - put IV| (vol points) at the parity strikes of the nearest expiry, and the pairs without both IVs."""
    gaps, missing = [], 0
    for expiration, strikes in strikes_by_expiry.items():
        sides: dict[Decimal, dict[str, float | None]] = {}
        for row in rows:
            if row.expiration_date == expiration and row.strike_price in strikes:
                sides.setdefault(row.strike_price, {})[row.contract_type] = (local.get(row.contract_ticker) or {}).get(
                    "local_iv")
        for item in sides.values():
            call, put = item.get("call"), item.get("put")
            if call is None or put is None:
                missing += 1
            else:
                gaps.append(abs(call - put) * 100)
    return {"median_gap_vol_points": _round(_median(gaps), 2) if gaps else None,
            "max_gap_vol_points": _round(max(gaps), 2) if gaps else None, "pairs": len(gaps),
            "pairs_without_iv": missing}


def _gamma_0dte(rows, local, *, spot: Decimal, session_date: date) -> dict[str, object]:
    """0 DTE dealer gamma per 1% (calls +, puts -, open interest), ours (local_gamma) and the provider's on the same
    contracts, as OV1 compares them; the net gap is a share of the provider's gross."""
    scale = float(spot) * float(spot) * 0.01 * 100
    ours = {"call": 0.0, "put": 0.0}
    theirs = {"call": 0.0, "put": 0.0}
    contracts = 0
    for row in rows:
        if row.expiration_date != session_date or not row.open_interest:
            continue
        gamma = (local.get(row.contract_ticker) or {}).get("local_gamma")
        if gamma is None or row.gamma is None:
            continue
        contracts += 1
        sign = 1.0 if row.contract_type == "call" else -1.0
        ours[row.contract_type] += sign * gamma * row.open_interest * scale
        theirs[row.contract_type] += sign * row.gamma * row.open_interest * scale
    gross = abs(theirs["call"]) + abs(theirs["put"])
    return {
        "contracts": contracts,
        "call_ratio": _round(ours["call"] / theirs["call"], 3) if theirs["call"] else None,
        "put_ratio": _round(ours["put"] / theirs["put"], 3) if theirs["put"] else None,
        "net_gap_of_gross": _round((ours["call"] + ours["put"] - theirs["call"] - theirs["put"]) / gross, 4)
        if gross else None,
    }


def spot_effect(rows, before: Mapping, after: Mapping, *, choice: SpotChoice, session_date: date) -> dict:
    """What moving the solve spot from the bar to the quote spot changes, from the two solves' results."""
    strikes = ({choice.parity.expiration_date: set(choice.parity.strikes)}
               if choice.parity.expiration_date is not None else {})
    no_arbitrage = {
        name: sum(1 for item in solved.values()
                  if (item.get("greeks_reason") or "").startswith(("BELOW_NO_ARBITRAGE", "ABOVE_NO_ARBITRAGE")))
        for name, solved in (("before", before), ("after", after))
    }
    priced = {name: sum(1 for item in solved.values() if item.get("local_iv") is not None)
              for name, solved in (("before", before), ("after", after))}
    return {
        "atm_iv_agreement": {"before": _atm_agreement(rows, before, strikes),
                             "after": _atm_agreement(rows, after, strikes)},
        "no_arbitrage_failures": no_arbitrage,
        "priced_contracts": priced,
        "gamma_0dte": {"before": _gamma_0dte(rows, before, spot=choice.bar_spot or Decimal(0), session_date=session_date),
                       "after": _gamma_0dte(rows, after, spot=choice.quote_spot or choice.bar_spot or Decimal(0),
                                            session_date=session_date)},
    }


def record_document(*, mode: BreadthSpotMode, underlying: str, session_date: date, equity_cutoff: datetime,
                    observed_at: datetime, choice: SpotChoice, used_spot: Decimal | None,
                    effect: Mapping | None, seconds: float | None = None, error: str | None = None) -> dict:
    parity, provider = choice.parity, choice.provider
    return {
        "event": BREADTH_SPOT_RECORD_EVENT, "policy": BREADTH_SPOT_POLICY["version"], "mode": mode.value,
        "underlying": underlying, "session_date": session_date.isoformat(), "equity_cutoff": equity_cutoff.isoformat(),
        "observed_at": observed_at.isoformat(),
        "bar_spot": None if choice.bar_spot is None else str(choice.bar_spot),
        "quote_spot": None if choice.quote_spot is None else str(choice.quote_spot),
        "quote_source": choice.source, "fallback_reason": choice.reason,
        "used_spot": None if used_spot is None else str(used_spot),
        "used_source": SOURCE_BAR if mode is not BreadthSpotMode.QUOTE else choice.source,
        "gap_bp": _bp(choice.bar_spot, choice.quote_spot),
        "parity": {"spot": None if parity.spot is None else str(parity.spot),
                   "expiration_date": None if parity.expiration_date is None else parity.expiration_date.isoformat(),
                   "pairs": parity.pairs, "dispersion_bp": _round((parity.dispersion_fraction or 0) * 1e4, 2)
                   if parity.dispersion_fraction is not None else None,
                   "bar_gap_bp": _bp(choice.bar_spot, parity.spot), "reason": parity.reason},
        "provider": {"spot": None if provider.spot is None else str(provider.spot),
                     "low": None if provider.low is None else str(provider.low),
                     "high": None if provider.high is None else str(provider.high), "rows": provider.rows,
                     "age_seconds": _round(provider.age_seconds, 1), "bar_gap_bp": _bp(choice.bar_spot, provider.spot),
                     "parity_gap_bp": _bp(parity.spot, provider.spot), "reason": provider.reason},
        "effect": effect, "seconds": _round(seconds, 3), "error": error,
    }


def log_record(document: Mapping) -> None:
    """One line on stderr (like O1_DEBIT_VERTICAL_RECORD), never raising."""
    try:
        print(f"{BREADTH_SPOT_RECORD_EVENT} {json.dumps(document, sort_keys=True, default=str)}", file=sys.stderr,
              flush=True)
    except Exception:  # noqa: BLE001 - logging never fails a capture
        pass
