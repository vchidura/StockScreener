"""OS1/OS2 stock-first confirmation, policy v2 (G106 AL5, Nov 17 2026): RECORD on the live Options window.

Today (`canonical_stock_option_participation_v1`, `_load_os` + `assess_canonical_stock_first`): a stock signal (OS1 / OS2
on the last 5m bars) exists only in the one window whose latest bars make it, and is confirmed only by a print of the
LAST 10 MINUTES on a watched contract of the signal's type whose day volume / prior OI >= 3. Replay Nov 10-16
(docs/g106/G106_EVIDENCE.md 'OS1/OS2 near misses'): 340 signals, 157 without any such print, 21 confirmed, 8 alerts; no
unconfirmed signal within 10% of the ratio.

v2 (`canonical_stock_option_participation_v2`):
- the signal stays alive through its validity (30 minutes after its last bar): each window re-detects the signals of the
  last 30 minutes from the session's 5m bars (the same detector, as of each earlier 5m boundary), so a signal is judged in
  every window until it expires; `first_window` tells the window that first saw it;
- a WATCHED contract of the signal's type confirms by its day participation: day volume at its latest print / prior OI
  >= 3, its latest print within 30 minutes of the cutoff, and the O1 liquidity floor (the window's floor policy,
  env-configurable) on that contract;
- the package (ON only) is one bought contract at the window's cutoff mark (as AL2 prices its legs), stored once per
  (signal, contract) event and reused.
An alternative confirmation is RECORDED ONLY (never applied): the same candidates with ask-side (buy) premium and the
ask-side share of classified volume over minimums (Nov 17 user question: on a down day the ratio favours calls, because
puts near the new spot carry large prior OI).

Switch `REALTIME_V3_OS_PARTICIPATION` on `realtime-v3-options-p8-window`: OFF (default, unset) = today byte for byte (no
extra read); RECORD = today's OS confirmations, selection, packages and alerts unchanged, plus one
`OS_PARTICIPATION_RECORD {json}` line per live signal per window and one `OS_PARTICIPATION_WINDOW {json}` line per
window; ON = v2 decides OS1/OS2 (implemented and tested; NOT to be switched on before RECORD evidence); anything else:
the window service refuses to start. Alternative thresholds: `REALTIME_V3_OS_PARTICIPATION_ALT_MIN_ASK_PREMIUM`
(dollars), `REALTIME_V3_OS_PARTICIPATION_ALT_MIN_ASK_SHARE_PCT`, `REALTIME_V3_OS_PARTICIPATION_ALT_MIN_CLASSIFIED_VOLUME`
(non-negative integers, read once per process). Recording never fails the window.

Name-level flow confirmation "flow20" (Nov 17 user question: OS1/OS2 missed the bearish Nov 16; research
docs/g106/G106_DETECTOR_RESEARCH.md 5.3, script `os_flow.py` `bias`), RECORDED in RECORD and ON: per live signal, over the
20 minutes before the window cutoff, across ALL watched contracts of the underlying (both types), the stored prints
(INCLUDED, revision 1) at the quote with HIGH/MEDIUM confidence: bullish premium = ask-side calls + bid-side puts, bearish
= ask-side puts + bid-side calls; net = (bull - bear) x the signal's direction; imbalance = (bull - bear) / (bull + bear);
coverage = classified / all premium of the underlying's prints in the 20 minutes. Confirmed when net >
`REALTIME_V3_OS_PARTICIPATION_FLOW20_MIN_NET_PREMIUM` (dollars, default 0: the research's "flow bias > 0"; it also reported
> $100k), |imbalance| x 100 >= `..._FLOW20_MIN_IMBALANCE_PCT` (default 0) and classified premium >=
`..._FLOW20_MIN_CLASSIFIED_PREMIUM` (default 0). Thresholds are a separate document (`flow20_sha256`), so the v2 policy
hash is unchanged by them. Level context (no extra read; from the signal's own 15 daily bars and its last 5m bar): the
signal bar's close vs the prior day's high, low and close, in ATR14. The opening range is not included (the window loads
only the last 9 5m bars). An ON path exists and stays unused: `REALTIME_V3_OS_PARTICIPATION_CONFIRMATION=FLOW20` (default
RATIO) makes flow20 the v2 confirmation (the confirming contract = the floor-passing candidate of the signal's type with
the largest ask-side premium) and is then stamped into the v2 policy document; anything but RATIO or FLOW20 refuses start.
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from functools import lru_cache
from typing import Callable, Iterable, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from options.dual_origin import activity_liquidity_floor_reasons

from .advanced_options_o1_liquidity_floor import non_negative_int_setting, o1_liquidity_floor_policy
from .contracts import canonical_sha256

OS_PARTICIPATION_SETTING = "REALTIME_V3_OS_PARTICIPATION"
OS_PARTICIPATION_ALT_MIN_ASK_PREMIUM_SETTING = "REALTIME_V3_OS_PARTICIPATION_ALT_MIN_ASK_PREMIUM"
OS_PARTICIPATION_ALT_MIN_ASK_SHARE_PCT_SETTING = "REALTIME_V3_OS_PARTICIPATION_ALT_MIN_ASK_SHARE_PCT"
OS_PARTICIPATION_ALT_MIN_CLASSIFIED_VOLUME_SETTING = "REALTIME_V3_OS_PARTICIPATION_ALT_MIN_CLASSIFIED_VOLUME"
OS_PARTICIPATION_FLOW20_MIN_IMBALANCE_PCT_SETTING = "REALTIME_V3_OS_PARTICIPATION_FLOW20_MIN_IMBALANCE_PCT"
OS_PARTICIPATION_FLOW20_MIN_CLASSIFIED_PREMIUM_SETTING = "REALTIME_V3_OS_PARTICIPATION_FLOW20_MIN_CLASSIFIED_PREMIUM"
OS_PARTICIPATION_FLOW20_MIN_NET_PREMIUM_SETTING = "REALTIME_V3_OS_PARTICIPATION_FLOW20_MIN_NET_PREMIUM"
OS_PARTICIPATION_CONFIRMATION_SETTING = "REALTIME_V3_OS_PARTICIPATION_CONFIRMATION"
OS_PARTICIPATION_RECORD_EVENT = "OS_PARTICIPATION_RECORD"
OS_PARTICIPATION_WINDOW_EVENT = "OS_PARTICIPATION_WINDOW"
OS_PARTICIPATION_POLICY_VERSION = "canonical_stock_option_participation_v2"
OS_PARTICIPATION_V1_POLICY_SHA256 = "29bc5c9a19345c64ef785b6f0670b9187e64237b7dbe9f3c4fe16f608a0e6993"
MINIMUM_VOLUME_OI_RATIO = Decimal(3)
PRINT_RECENCY_SECONDS = 1800
SIGNAL_LOOKBACK_STEPS = 6  # 5m boundaries back from the Equity cutoff: covers the 30-minute validity
EXTENDED_5M_BARS = 9  # the last 30 minutes plus the three bars OS2 reads
ALT_DEFAULTS = {"minimum_ask_premium": 50_000, "minimum_ask_share_pct": 60, "minimum_classified_volume": 200}
ASK, BID, INSIDE, COMPLEX = "AT_OR_ABOVE_ASK", "AT_OR_BELOW_BID", "INSIDE_SPREAD", "EXCLUDED_COMPLEX"
FLOW20_WINDOW_SECONDS = 1200
FLOW20_DEFAULTS = {"minimum_net_premium": 0, "minimum_imbalance_pct": 0, "minimum_classified_premium": 0}
FLOW20_CONFIDENCE = ("HIGH", "MEDIUM")
CONFIRMATIONS = ("RATIO", "FLOW20")

REASON_NO_RECENT_PRINT = "OSP_NO_WATCHED_PRINT_WITHIN_30_MIN"
REASON_BELOW_BASELINE = "OSP_PARTICIPATION_BELOW_BASELINE"
REASON_BELOW_FLOOR = "OSP_CONFIRMING_CONTRACT_BELOW_LIQUIDITY_FLOOR"
REASON_ALT_BELOW_PREMIUM = "OSP_ALT_ASK_PREMIUM_BELOW_POLICY"
REASON_ALT_BELOW_SHARE = "OSP_ALT_ASK_SHARE_BELOW_POLICY"
REASON_ALT_FEW_CLASSIFIED = "OSP_ALT_CLASSIFIED_VOLUME_BELOW_POLICY"
REASON_FLOW20_NOT_CONFIRMED = "OSP_FLOW20_NOT_CONFIRMED"
REASON_FLOW20_NO_CONTRACT = "OSP_FLOW20_NO_CONFIRMING_CONTRACT"


class OSParticipationMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"
    ON = "ON"


def os_participation_mode(environ: Mapping[str, str] | None = None) -> OSParticipationMode:
    """The switch value; unset or blank is OFF. Anything but OFF, RECORD or ON raises (fails loudly, never silently)."""
    raw = (os.environ if environ is None else environ).get(OS_PARTICIPATION_SETTING)
    value = (raw or "").strip().upper() or OSParticipationMode.OFF.value
    try:
        return OSParticipationMode(value)
    except ValueError:
        raise ValueError(f"{OS_PARTICIPATION_SETTING} must be OFF, RECORD or ON, not {raw!r}") from None


@dataclass(frozen=True, slots=True)
class OSParticipationPolicy:
    minimum_prior_open_interest: int
    minimum_day_volume: int
    floor_policy_sha256: str
    minimum_ask_premium: int
    minimum_ask_share_pct: int
    minimum_classified_volume: int
    document: Mapping[str, object] = field(compare=False)
    policy_sha256: str = ""
    # flow20 (Nov 17): recorded in RECORD and ON; the v2 confirmation only with confirmation FLOW20 (unused ON path).
    confirmation: str = "RATIO"
    flow20_minimum_net_premium: int = 0
    flow20_minimum_imbalance_pct: int = 0
    flow20_minimum_classified_premium: int = 0
    flow20_document: Mapping[str, object] | None = field(default=None, compare=False)
    flow20_sha256: str = ""

    @property
    def floor(self):
        from options.dual_origin import ActivityLiquidityFloorPolicy

        return ActivityLiquidityFloorPolicy(minimum_prior_open_interest=self.minimum_prior_open_interest,
                                            minimum_day_volume=self.minimum_day_volume)


def os_participation_policy_from(environ: Mapping[str, str], floor_policy=None) -> OSParticipationPolicy:
    floor_policy = floor_policy or o1_liquidity_floor_policy()
    alt = {
        "minimum_ask_premium": non_negative_int_setting(
            environ, OS_PARTICIPATION_ALT_MIN_ASK_PREMIUM_SETTING, ALT_DEFAULTS["minimum_ask_premium"]),
        "minimum_ask_share_pct": non_negative_int_setting(
            environ, OS_PARTICIPATION_ALT_MIN_ASK_SHARE_PCT_SETTING, ALT_DEFAULTS["minimum_ask_share_pct"]),
        "minimum_classified_volume": non_negative_int_setting(
            environ, OS_PARTICIPATION_ALT_MIN_CLASSIFIED_VOLUME_SETTING, ALT_DEFAULTS["minimum_classified_volume"]),
    }
    if alt["minimum_ask_share_pct"] > 100:
        raise ValueError(f"{OS_PARTICIPATION_ALT_MIN_ASK_SHARE_PCT_SETTING} must be 0-100")
    flow20 = {
        "minimum_net_premium": non_negative_int_setting(
            environ, OS_PARTICIPATION_FLOW20_MIN_NET_PREMIUM_SETTING, FLOW20_DEFAULTS["minimum_net_premium"]),
        "minimum_imbalance_pct": non_negative_int_setting(
            environ, OS_PARTICIPATION_FLOW20_MIN_IMBALANCE_PCT_SETTING, FLOW20_DEFAULTS["minimum_imbalance_pct"]),
        "minimum_classified_premium": non_negative_int_setting(
            environ, OS_PARTICIPATION_FLOW20_MIN_CLASSIFIED_PREMIUM_SETTING,
            FLOW20_DEFAULTS["minimum_classified_premium"]),
    }
    if flow20["minimum_imbalance_pct"] > 100:
        raise ValueError(f"{OS_PARTICIPATION_FLOW20_MIN_IMBALANCE_PCT_SETTING} must be 0-100")
    raw_confirmation = environ.get(OS_PARTICIPATION_CONFIRMATION_SETTING)
    confirmation = (raw_confirmation or "").strip().upper() or "RATIO"
    if confirmation not in CONFIRMATIONS:
        raise ValueError(f"{OS_PARTICIPATION_CONFIRMATION_SETTING} must be RATIO or FLOW20, not {raw_confirmation!r}")
    flow20_document = {
        "version": "os_name_flow_confirmation_flow20_v1",
        "window_seconds": FLOW20_WINDOW_SECONDS,
        "window_basis": "WINDOW_CUTOFF_MINUS_20_MINUTES_EXCLUSIVE_TO_CUTOFF_INCLUSIVE",
        "prints": "STORED_INCLUDED_REVISION_1_PRINTS_OF_ALL_WATCHED_CONTRACTS_OF_THE_UNDERLYING",
        "bullish": "ASK_SIDE_CALLS_PLUS_BID_SIDE_PUTS",
        "bearish": "ASK_SIDE_PUTS_PLUS_BID_SIDE_CALLS",
        "counted_confidence": list(FLOW20_CONFIDENCE),
        "confirmed": "NET_IN_SIGNAL_DIRECTION_ABOVE_MINIMUM_AND_IMBALANCE_AND_CLASSIFIED_AT_LEAST_MINIMUM",
        **flow20,
    }
    document = {
        "version": OS_PARTICIPATION_POLICY_VERSION,
        "base_confirmation_policy_sha256": OS_PARTICIPATION_V1_POLICY_SHA256,
        "minimum_volume_oi_ratio": str(MINIMUM_VOLUME_OI_RATIO),
        "eligible": "WATCHED_CONTRACT_OF_SIGNAL_TYPE_LATEST_PRINT_WITHIN_RECENCY",
        "print_recency_seconds": PRINT_RECENCY_SECONDS,
        "signal_persistence": "REDETECTED_FROM_5M_BARS_THROUGH_SIGNAL_VALIDITY",
        "liquidity_floor_policy_sha256": floor_policy.sha256,
        "minimum_prior_open_interest": floor_policy.minimum_prior_open_interest,
        "minimum_day_volume": floor_policy.minimum_day_volume,
        "package": "SINGLE_CONTRACT_BUY_AT_CUTOFF_MARK",
        "alternative": {"version": "ask_side_participation_record_only_v1", "applied": False, **alt},
    }
    flow20_sha256 = canonical_sha256(flow20_document)
    if confirmation != "RATIO":
        # The unused ON path: flow20 becomes the v2 confirmation, so it is part of the v2 policy (RATIO: unchanged).
        document["confirmation"] = {"basis": confirmation, "flow20_sha256": flow20_sha256, **flow20_document}
    return OSParticipationPolicy(
        minimum_prior_open_interest=floor_policy.minimum_prior_open_interest,
        minimum_day_volume=floor_policy.minimum_day_volume, floor_policy_sha256=floor_policy.sha256,
        document=document, policy_sha256=canonical_sha256(document), **alt,
        confirmation=confirmation, flow20_minimum_net_premium=flow20["minimum_net_premium"],
        flow20_minimum_imbalance_pct=flow20["minimum_imbalance_pct"],
        flow20_minimum_classified_premium=flow20["minimum_classified_premium"],
        flow20_document=flow20_document, flow20_sha256=flow20_sha256)


@lru_cache(maxsize=1)
def os_participation_policy() -> OSParticipationPolicy:
    """The process's policy (with the process's floor thresholds), read once; the window service reads it at start."""
    return os_participation_policy_from(os.environ)


# --- participation per contract ---------------------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class OSParticipationPrint:
    """One stored flow event (qualifying print) on a watched contract."""

    contract_ticker: str
    underlyer: str
    call_put: str  # "C" / "P"
    event_time: datetime
    size: int
    premium: Decimal
    side_class: str
    multi_leg: bool
    day_volume: int | None
    open_interest: int | None
    side_confidence: str = "NONE"  # flow20 (Nov 17)
    revision: int = 1


@dataclass(frozen=True, slots=True)
class ContractParticipation:
    contract_ticker: str
    underlyer: str
    call_put: str
    latest_print_at: datetime
    day_volume: int | None
    open_interest: int | None
    flow_volume: int
    ask_volume: int
    bid_volume: int
    inside_volume: int
    complex_volume: int
    unclassified_volume: int
    flow_premium: Decimal
    ask_premium: Decimal
    bid_premium: Decimal

    @property
    def ratio(self) -> Decimal | None:
        if self.day_volume is None or not self.open_interest:
            return None
        return Decimal(self.day_volume) / Decimal(self.open_interest)

    @property
    def classified_volume(self) -> int:
        return self.ask_volume + self.bid_volume

    @property
    def ask_share(self) -> Decimal | None:
        return None if not self.classified_volume else Decimal(self.ask_volume) / Decimal(self.classified_volume)

    def record(self, cutoff: datetime, floor) -> dict:
        floor_reasons = activity_liquidity_floor_reasons(self.open_interest, self.day_volume, floor)
        return {
            "contract": self.contract_ticker, "ratio": None if self.ratio is None else round(float(self.ratio), 4),
            "open_interest": self.open_interest, "day_volume": self.day_volume,
            "last_print_age_s": round((cutoff - self.latest_print_at).total_seconds(), 1),
            "floor_pass": not floor_reasons, "floor_reasons": list(floor_reasons), "watched": True,
            "flow_volume": self.flow_volume, "ask_volume": self.ask_volume, "bid_volume": self.bid_volume,
            "inside_volume": self.inside_volume, "complex_volume": self.complex_volume,
            "unclassified_volume": self.unclassified_volume,
            "ask_share": None if self.ask_share is None else round(float(self.ask_share), 4),
            "classified_share": None if not self.flow_volume else round(self.classified_volume / self.flow_volume, 4),
            "complex_share": None if not self.flow_volume else round(self.complex_volume / self.flow_volume, 4),
            "unclassified_share": (None if not self.flow_volume
                                   else round(self.unclassified_volume / self.flow_volume, 4)),
            "flow_premium": str(self.flow_premium), "ask_premium": str(self.ask_premium),
            "bid_premium": str(self.bid_premium),
        }


def contract_participation(prints: Iterable[OSParticipationPrint], cutoff: datetime) -> dict[str, ContractParticipation]:
    """Per contract, from the session's prints at or before `cutoff`: the latest print's day volume and OI (the O1
    ratio's inputs) and the day's flow-event volume and premium by aggressor side. Flow events are the stored qualifying
    prints, not every trade; unclassified = stale / no quote / crossed / not classified."""
    by_contract: dict[str, list[OSParticipationPrint]] = {}
    for row in prints:
        if row.event_time <= cutoff:
            by_contract.setdefault(row.contract_ticker, []).append(row)
    result = {}
    for ticker, rows in by_contract.items():
        rows.sort(key=lambda row: (row.event_time, row.day_volume or 0))
        latest = rows[-1]
        volume = Counter()
        premium = Counter()
        for row in rows:
            side = (row.side_class if row.side_class in (ASK, BID, INSIDE, COMPLEX) else "OTHER")
            volume[side] += row.size
            premium[side] += row.premium
        result[ticker] = ContractParticipation(
            contract_ticker=ticker, underlyer=latest.underlyer, call_put=latest.call_put,
            latest_print_at=latest.event_time, day_volume=latest.day_volume, open_interest=latest.open_interest,
            flow_volume=sum(volume.values()), ask_volume=volume[ASK], bid_volume=volume[BID],
            inside_volume=volume[INSIDE], complex_volume=volume[COMPLEX], unclassified_volume=volume["OTHER"],
            flow_premium=sum(premium.values(), Decimal(0)), ask_premium=Decimal(premium[ASK]),
            bid_premium=Decimal(premium[BID]))
    return result


@dataclass(frozen=True, slots=True)
class Flow20Tally:
    """One underlying's stored prints in the 20 minutes up to the cutoff: bullish / bearish classified premium."""

    bull_premium: Decimal = Decimal(0)
    bear_premium: Decimal = Decimal(0)
    total_premium: Decimal = Decimal(0)
    prints: int = 0


def flow20_by_underlying(prints: Iterable[OSParticipationPrint], cutoff: datetime) -> dict[str, Flow20Tally]:
    """Per underlying, over (cutoff - 20 min, cutoff]: revision-1 prints of all its watched contracts. Bullish = ask-side
    calls + bid-side puts, bearish = ask-side puts + bid-side calls, HIGH/MEDIUM confidence (research `os_flow.py`)."""
    start = cutoff - timedelta(seconds=FLOW20_WINDOW_SECONDS)
    sums: dict[str, Counter] = {}
    for row in prints:
        if not start < row.event_time <= cutoff or row.revision != 1:
            continue
        amounts = sums.setdefault(row.underlyer, Counter())
        amounts["total"] += row.premium
        amounts["prints"] += 1
        if row.side_class in (ASK, BID) and row.side_confidence in FLOW20_CONFIDENCE:
            bullish = (row.side_class == ASK) == (row.call_put == "C")
            amounts["bull" if bullish else "bear"] += row.premium
    return {
        name: Flow20Tally(bull_premium=Decimal(amounts["bull"]), bear_premium=Decimal(amounts["bear"]),
                          total_premium=Decimal(amounts["total"]), prints=int(amounts["prints"]))
        for name, amounts in sums.items()
    }


def flow20_record(tally: Flow20Tally | None, direction: int, policy: "OSParticipationPolicy", cutoff: datetime) -> dict:
    """The flow20 measure of one signal: premiums, net in the signal's direction, imbalance, coverage, confirmed."""
    tally = tally or Flow20Tally()
    bull, bear = tally.bull_premium, tally.bear_premium
    classified = bull + bear
    net = (bull - bear) * direction
    imbalance = None if classified <= 0 else (bull - bear) / classified
    reasons = []
    if net <= policy.flow20_minimum_net_premium:
        reasons.append("FLOW20_NET_NOT_ABOVE_MINIMUM")
    if imbalance is None or abs(imbalance) * 100 < policy.flow20_minimum_imbalance_pct:
        reasons.append("FLOW20_IMBALANCE_BELOW_MINIMUM")
    if classified <= 0 or classified < policy.flow20_minimum_classified_premium:
        reasons.append("FLOW20_CLASSIFIED_BELOW_MINIMUM")
    return {
        "window_start": (cutoff - timedelta(seconds=FLOW20_WINDOW_SECONDS)).isoformat(),
        "bull_premium": str(bull), "bear_premium": str(bear), "net_premium": str(net),
        "imbalance": None if imbalance is None else round(float(imbalance), 4),
        "classified_premium": str(classified), "total_premium": str(tally.total_premium),
        "coverage": None if tally.total_premium <= 0 else round(float(classified / tally.total_premium), 4),
        "prints": tally.prints, "confirmed": not reasons, "reasons": reasons,
    }


def signal_levels(signal) -> dict | None:
    """Level context from the signal itself (no read): its last 5m bar's close vs the prior day's high / low / close
    (the last of its 15 daily bars; the prior close the detector used) in the detector's ATR14. None if unavailable."""
    try:
        close, atr = Decimal(signal.bars[-1].close), Decimal(signal.prior_atr14)
        prior = signal.daily_bars[-1]
        levels = {"pdh": Decimal(prior.high), "pdl": Decimal(prior.low), "pdc": Decimal(signal.prior_close)}
        return {
            "close": str(close), "atr14": str(atr), "prior_day": prior.session_date.isoformat(),
            **{key: str(value) for key, value in levels.items()},
            **{f"{key}_distance_atr": round(float((close - value) / atr), 3) for key, value in levels.items()},
        }
    except Exception:  # noqa: BLE001 - context only
        return None


# --- the decision -----------------------------------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class OSSignalView:
    detector: str
    signal_id: str
    underlyer: str
    direction: int
    market_time: datetime
    valid_until: datetime
    first_window: bool
    levels: Mapping[str, object] | None = field(default=None, compare=False)  # record context only


@dataclass(frozen=True, slots=True)
class OSParticipationOutcome:
    signal: OSSignalView
    confirmed_contract: str | None
    alternative_contract: str | None
    reasons: tuple[str, ...]
    alternative_reasons: tuple[str, ...]
    best: Mapping[str, object] | None
    best_alternative: Mapping[str, object] | None
    candidates: int
    packageable: bool
    alternative_packageable: bool
    flow20: Mapping[str, object] | None = None  # the flow20 measure (recorded; the confirmation only with FLOW20)

    @property
    def confirmed(self) -> bool:
        return self.confirmed_contract is not None


def evaluate_os_participation(
    signal: OSSignalView,
    participation: Mapping[str, ContractParticipation],
    *,
    cutoff: datetime,
    policy: OSParticipationPolicy,
    packageable: Callable[[str], bool] = lambda ticker: False,
    flow20: Flow20Tally | None = None,
) -> OSParticipationOutcome:
    """v2 for one live signal at one cutoff (pure). Candidates: the signal's underlying and type, latest print within the
    recency before the cutoff. Confirms: ratio >= 3 and the liquidity floor; the best confirming contract is the highest
    ratio. Alternative (recorded only): floor + ask-side premium + ask share of classified volume."""
    want = "C" if signal.direction == 1 else "P"
    floor = policy.floor
    candidates = [
        row for row in participation.values()
        if row.underlyer == signal.underlyer and row.call_put == want
        and cutoff - timedelta(seconds=PRINT_RECENCY_SECONDS) < row.latest_print_at <= cutoff
    ]
    rated = [row for row in candidates if row.ratio is not None]
    confirming = [row for row in rated if row.ratio >= MINIMUM_VOLUME_OI_RATIO
                  and not activity_liquidity_floor_reasons(row.open_interest, row.day_volume, floor)]
    best = max(confirming or rated, key=lambda row: (row.ratio, row.contract_ticker), default=None)
    if confirming:
        reasons = ()
    elif not rated:
        reasons = (REASON_NO_RECENT_PRINT,)
    elif any(row.ratio >= MINIMUM_VOLUME_OI_RATIO for row in rated):
        reasons = (REASON_BELOW_FLOOR,)
    else:
        reasons = (REASON_BELOW_BASELINE,)
    alternative, alternative_reasons = [], Counter()
    for row in candidates:
        why = list(activity_liquidity_floor_reasons(row.open_interest, row.day_volume, floor))
        if row.ask_premium < policy.minimum_ask_premium:
            why.append(REASON_ALT_BELOW_PREMIUM)
        if row.classified_volume < max(policy.minimum_classified_volume, 1):
            why.append(REASON_ALT_FEW_CLASSIFIED)
        elif row.ask_share * 100 < policy.minimum_ask_share_pct:
            why.append(REASON_ALT_BELOW_SHARE)
        if why:
            alternative_reasons.update(why)
        else:
            alternative.append(row)
    best_alternative = max(alternative or candidates, key=lambda row: (row.ask_premium, row.contract_ticker),
                           default=None)
    confirmed = best.contract_ticker if confirming else None
    alternative_contract = best_alternative.contract_ticker if alternative else None
    flow = flow20_record(flow20, signal.direction, policy, cutoff)
    if policy.confirmation == "FLOW20":
        # The unused ON path (REALTIME_V3_OS_PARTICIPATION_CONFIRMATION=FLOW20): the name's flow confirms; the contract
        # is the floor-passing candidate of the signal's type with the largest ask-side premium.
        liquid = [row for row in candidates
                  if not activity_liquidity_floor_reasons(row.open_interest, row.day_volume, floor)]
        pick = max(liquid, key=lambda row: (row.ask_premium, row.contract_ticker), default=None)
        if not flow["confirmed"]:
            confirmed, reasons = None, (REASON_FLOW20_NOT_CONFIRMED,)
        elif pick is None:
            confirmed, reasons = None, (REASON_FLOW20_NO_CONTRACT,)
        else:
            confirmed, reasons, best = pick.contract_ticker, (), pick
    return OSParticipationOutcome(
        signal=signal, confirmed_contract=confirmed, alternative_contract=alternative_contract, reasons=reasons,
        alternative_reasons=(() if alternative else tuple(sorted(alternative_reasons)) or (REASON_NO_RECENT_PRINT,)),
        best=None if best is None else best.record(cutoff, floor),
        best_alternative=None if best_alternative is None else best_alternative.record(cutoff, floor),
        candidates=len(candidates),
        packageable=bool(confirmed) and packageable(confirmed),
        alternative_packageable=bool(alternative_contract) and packageable(alternative_contract),
        flow20=flow,
    )


# --- live signals through their validity --------------------------------------------------------------------------------

def live_signals(*, security_id, underlyer: str, bars: Sequence, equity_cutoff: datetime, decision_at: datetime,
                 session_close: datetime, current_ids: Iterable[str] = ()) -> list:
    """The OS1/OS2 signals alive at `decision_at`: the detector as of the Equity cutoff and of each earlier 5m boundary
    of the last 30 minutes (each signal once, by id; `bars` = (EquityBarRevision, created_at) with the session's last
    5m bars and the 15 daily bars). Returns (signal, first_window) pairs; first_window = found as of the Equity cutoff
    or among `current_ids` (the window's own v1 signals)."""
    from options.stock_bar_detection import detect_canonical_stock_signals

    seen: dict[str, tuple] = {}
    current = {str(value) for value in current_ids}
    for step in range(SIGNAL_LOOKBACK_STEPS + 1):
        as_of = equity_cutoff - timedelta(minutes=5 * step)
        usable = tuple(item for item in bars if item[0].bar_end <= as_of)
        try:
            found = detect_canonical_stock_signals(
                security_id=security_id, underlyer=underlyer, bars=usable, market_cutoff=as_of,
                decision_at=decision_at, session_close=session_close, detector_ids=("OS1", "OS2"))
        except ValueError:
            continue  # e.g. fewer than 15 daily bars: no signal, as v1
        for signal in found:
            key = str(signal.signal_id)
            if key not in seen:
                seen[key] = (signal, step == 0 or key in current)
    return list(seen.values())


def signal_view(signal, first_window: bool) -> OSSignalView:
    return OSSignalView(detector=signal.detector_id, signal_id=str(signal.signal_id), underlyer=signal.underlyer,
                        direction=int(signal.direction), market_time=signal.market_time,
                        valid_until=signal.valid_until, first_window=first_window, levels=signal_levels(signal))


def os_participation_event_id(detector: str, signal_id: str, contract: str | None) -> UUID:
    return uuid5(NAMESPACE_URL, f"advanced-options-stock-first-v2:{detector}:{signal_id}:{contract or '-'}")


# --- record --------------------------------------------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class OSParticipationLaneRecord:
    """What v2 decided for one lane in one window (carried on the lane inputs, never hashed)."""

    detector: str
    mode: OSParticipationMode
    policy: OSParticipationPolicy | None
    cutoff: datetime | None
    outcomes: tuple[OSParticipationOutcome, ...] = ()
    status: str = "OK"  # OK / INPUTS_UNAVAILABLE:<why> / LANE_BLOCKED
    reasons: tuple[str, ...] = ()


def v1_signal_outcomes(evaluations) -> dict[str, dict]:
    """Per stock signal, today's (v1) outcome from the OS lane's evaluations: CONFIRMED if any pairing was confirmed,
    with SELECTED when one had a package; else the disposition; best v1 pairing ratio."""
    result: dict[str, dict] = {}
    for evaluation in evaluations:
        payload = evaluation.payload
        signal = payload.get("signal") or {}
        decision = payload.get("decision") or {}
        key = str(signal.get("signal_id"))
        item = result.setdefault(key, {"outcome": "UNMATCHED", "selected": False, "pairings": 0, "best_ratio": None})
        item["pairings"] += 1
        disposition = decision.get("disposition")
        if disposition == "CONFIRMED":
            item["outcome"] = "CONFIRMED"
        elif disposition == "UNAVAILABLE" and item["outcome"] != "CONFIRMED" and item["pairings"] == 1:
            item["outcome"] = "UNAVAILABLE"
        item["selected"] |= evaluation.selection_status == "SELECTED"
        ratio = (decision.get("activity") or {}).get("volume_oi_ratio")
        if ratio is not None:
            value = float(ratio)
            item["best_ratio"] = value if item["best_ratio"] is None else max(item["best_ratio"], value)
    return result


def os_participation_documents(record: OSParticipationLaneRecord, v1: Mapping[str, dict]) -> tuple[list[dict], dict]:
    """(one document per live signal, the window document) for one lane."""
    policy = record.policy
    lines = []
    counts = Counter()
    reasons = Counter()
    flow20_signals, flow20_confirmed = Counter(bull=0, bear=0), Counter(bull=0, bear=0)
    for outcome in record.outcomes:
        signal = outcome.signal
        v1_item = v1.get(signal.signal_id)
        v1_outcome = ("NOT_RUN_V2_ON" if record.mode is OSParticipationMode.ON
                      else "NOT_EVALUATED_NO_PAIRING" if v1_item is None and signal.first_window
                      else "NOT_LIVE_IN_V1" if v1_item is None
                      else v1_item["outcome"])
        counts["signals"] += 1
        counts["first_window"] += signal.first_window
        counts["v1_confirmed"] += v1_outcome == "CONFIRMED"
        counts["v1_selected"] += bool(v1_item and v1_item["selected"])
        counts["v2_confirmed"] += outcome.confirmed
        counts["v2_packageable"] += outcome.packageable
        counts["alt_confirmed"] += outcome.alternative_contract is not None
        counts["alt_packageable"] += outcome.alternative_packageable
        side = "bull" if signal.direction == 1 else "bear"
        flow20_signals[side] += 1
        flow20_confirmed[side] += bool(outcome.flow20 and outcome.flow20.get("confirmed"))
        reasons.update(outcome.reasons)
        lines.append({
            "contract": "os-participation-record-v1", "mode": record.mode.value,
            "policy": OS_PARTICIPATION_POLICY_VERSION, "policy_sha256": policy.policy_sha256,
            "cutoff": record.cutoff.isoformat(), "detector": signal.detector, "underlyer": signal.underlyer,
            "direction": signal.direction, "signal_id": signal.signal_id,
            "signal_market_time": signal.market_time.isoformat(), "valid_until": signal.valid_until.isoformat(),
            "first_window": signal.first_window,
            "v1": {"outcome": v1_outcome, "selected": bool(v1_item and v1_item["selected"]),
                   "pairings": 0 if v1_item is None else v1_item["pairings"],
                   "best_ratio": None if v1_item is None else v1_item["best_ratio"]},
            "v2": {"outcome": "CONFIRMED" if outcome.confirmed else "UNMATCHED",
                   "contract": outcome.confirmed_contract, "packageable": outcome.packageable,
                   "reasons": list(outcome.reasons), "candidates": outcome.candidates, "best": outcome.best},
            "alternative": {"outcome": "CONFIRMED" if outcome.alternative_contract else "UNMATCHED", "applied": False,
                            "contract": outcome.alternative_contract, "packageable": outcome.alternative_packageable,
                            "reasons": list(outcome.alternative_reasons), "best": outcome.best_alternative},
            "flow20": outcome.flow20,
            "levels": signal.levels,
        })
    window = {
        "contract": "os-participation-window-v1", "mode": record.mode.value, "detector": record.detector,
        "status": record.status, "lane_reasons": list(record.reasons),
        "cutoff": None if record.cutoff is None else record.cutoff.isoformat(),
        "policy": OS_PARTICIPATION_POLICY_VERSION,
        "policy_sha256": None if policy is None else policy.policy_sha256,
        "minimum_prior_open_interest": None if policy is None else policy.minimum_prior_open_interest,
        "minimum_day_volume": None if policy is None else policy.minimum_day_volume,
        "alternative": None if policy is None else dict(policy.document["alternative"]),
        "v1_signals_evaluated": len(v1), "v1_confirmed_signals": sum(item["outcome"] == "CONFIRMED" for item in v1.values()),
        "v1_selected_signals": sum(item["selected"] for item in v1.values()),
        **{key: counts[key] for key in ("signals", "first_window", "v1_confirmed", "v1_selected", "v2_confirmed",
                                        "v2_packageable", "alt_confirmed", "alt_packageable")},
        "v2_reasons": dict(sorted(reasons.items())),
        "confirmation": None if policy is None else policy.confirmation,
        "flow20_sha256": None if policy is None else policy.flow20_sha256,
        "flow20": None if policy is None else {
            "minimum_net_premium": policy.flow20_minimum_net_premium,
            "minimum_imbalance_pct": policy.flow20_minimum_imbalance_pct,
            "minimum_classified_premium": policy.flow20_minimum_classified_premium,
        },
        "flow20_signals": dict(flow20_signals),
        "flow20_confirmed": dict(flow20_confirmed),
    }
    return lines, window


def log_os_participation(record: OSParticipationLaneRecord | None, evaluations) -> None:
    """One OS_PARTICIPATION_RECORD line per live signal and one OS_PARTICIPATION_WINDOW line. Never raises."""
    if record is None or record.mode is OSParticipationMode.OFF:
        return
    try:
        v1 = {} if record.mode is OSParticipationMode.ON else v1_signal_outcomes(evaluations)
        lines, window = os_participation_documents(record, v1)
        for line in lines:
            print(f"{OS_PARTICIPATION_RECORD_EVENT} {json.dumps(line, sort_keys=True, default=str)}",
                  file=sys.stderr, flush=True)
        print(f"{OS_PARTICIPATION_WINDOW_EVENT} {json.dumps(window, sort_keys=True, default=str)}",
              file=sys.stderr, flush=True)
    except Exception as error:  # noqa: BLE001
        print(f"{OS_PARTICIPATION_WINDOW_EVENT} " + json.dumps({
            "contract": "os-participation-window-v1", "mode": record.mode.value, "detector": record.detector,
            "status": "RECORD_FAILED", "reason": f"{type(error).__name__}: {error}"[:200],
        }, sort_keys=True), file=sys.stderr, flush=True)


__all__ = [
    "ContractParticipation", "Flow20Tally", "OS_PARTICIPATION_CONFIRMATION_SETTING",
    "OS_PARTICIPATION_FLOW20_MIN_CLASSIFIED_PREMIUM_SETTING", "OS_PARTICIPATION_FLOW20_MIN_IMBALANCE_PCT_SETTING",
    "OS_PARTICIPATION_FLOW20_MIN_NET_PREMIUM_SETTING", "flow20_by_underlying", "flow20_record", "signal_levels",
    "OSParticipationLaneRecord", "OSParticipationMode", "OSParticipationOutcome",
    "OSParticipationPolicy", "OSParticipationPrint", "OSSignalView", "OS_PARTICIPATION_RECORD_EVENT",
    "OS_PARTICIPATION_SETTING", "OS_PARTICIPATION_WINDOW_EVENT", "contract_participation",
    "evaluate_os_participation", "live_signals", "log_os_participation", "os_participation_documents",
    "os_participation_event_id", "os_participation_mode", "os_participation_policy", "os_participation_policy_from",
    "signal_view", "v1_signal_outcomes",
]
