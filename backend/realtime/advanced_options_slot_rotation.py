"""G106 AL10 phase 1 (Nov 17 2026): Options watched-set rotation policy, RECORD only.

The V4 watched set is frozen at the open (24 contracts per name). The Nov 17 full-chain RECORD scan
(`runner/chain_scan_record.py`, policy chain-scan-record-v3) found 10-25 tradable contracts per window with an unusual
5-minute premium burst, almost all of them outside the watched set. This module is that scan's policy, cleaned up and
moved into the Options scheduler: every Options window it reads the full-chain breadth rows the scheduler has just
captured, finds tradable contracts with unusual activity that are NOT watched, and records which watched contracts it
would swap out for them (promote / evict), under budgets and protections, with the later outcome of each promotion.

`REALTIME_V3_OPTIONS_SLOT_ROTATION`, read by the Options scheduler (`realtime-v3-options-scheduler`,
`scripts/run_advanced_options_v3_scheduler.py`) at start:
| OFF (default, unset, blank) | nothing: no SQL, no log line, no key in the cycle result (byte-identical) |
| RECORD | after each window's breadth capture: one short read-only transaction, the policy runs on plain rows in memory,
           the decisions are logged only (`OPTIONS_SLOT_ROTATION_RECORD*` lines), never stored or subscribed |
| ON | accepted but behaves as RECORD: phase 1 has no declared reserve to subscribe from, so the scheduler logs
       `OPTIONS_SLOT_ROTATION_ON_UNAVAILABLE {"reason":"NO_DECLARED_RESERVE"}` and one DEGRADED fault at start |
Anything else (any case accepted for the three) and the scheduler refuses to start. Later phases: preparation declares
a reserve superset D of the watched set (phase 2), acquisition subscribes / unsubscribes in place from D (phase 3).

Policy v1 (`POLICY`, hashed as `POLICY_SHA256`):
- tradable: DTE 1-45, |delta| 0.25-0.70 (our local delta, else the provider's), prior OI >= 100, day volume >= 500,
  bid > 0, spread <= 10% of the mid, mid >= $0.25, quote age <= 60 s at the capture;
- candidate: tradable + 5-minute premium (day-volume change x mid x 100) >= $50k + burst (5-minute volume over the
  contract's own average 5-minute volume since the open) >= 3; outside the (simulated) watched set; catalogued
  (`ref.option_contracts` with an `rt.instruments` row); promoted only after 2 consecutive candidate windows;
- side DEMAND / SUPPLY / UNCLEAR from the IV change and the option's move against its delta-implied move; the
  direction is never the contract type alone (UNCLEAR has no direction);
- rank: 5-minute premium desc, burst desc, ticker; swaps stay within the name;
- budgets: <= 2 per name per window, <= 8 per window, <= 6 per name per session, <= 48 per session, <= 50% of a
  name's original watched contracts replaced per session, >= 8 calls and >= 8 puts kept per name (a side already
  under 8 is never reduced further);
- eviction only of a watched contract idle 3 consecutive windows (no breadth volume change), with no flow print in
  15 min (`rt.option_volume_checkpoints.last_event_time`; unknown protects), not a leg of a package created this
  session (`rt.option_packages` + `rt.option_package_legs`; unknown protects), not promoted in the last 6 windows;
  an evicted contract is never promoted again in the session;
- no swaps in the session's first 2 windows, from 30 min before the close (20:30 UTC on a regular winter day), or for
  a name without a COMPLETE capture this window or without the previous window's values (NO_PRIOR_WINDOW after a
  restart, PRIOR_WINDOW_GAP after a missed window).

The rotation is simulated: a confirmed swap changes this process's simulated watched set, budgets and protections
(so later windows see it), never the real one. `breadth_current` keeps only the latest capture, so the previous
window's per-contract values live in memory (`RotationState.priors`, reset each session). The hypothetical reserve
D_hyp(R) (R in 8/16/24) per name is frozen at the name's first evaluated window: the top R statically tradable
contracts outside the real watched set by open interest, then |delta| closeness to 0.5, then ticker; each window
reports how many candidates fall inside it (the coverage a phase-2 reserve of that size would have had).

Recording never fails the cycle: any error logs `OPTIONS_SLOT_ROTATION_RECORD_FAILED {reason}` and one DEGRADED fault.
"""
from __future__ import annotations

import json
import math
import os
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Callable, Collection, Iterable, Iterator, Mapping, Sequence

from .contracts import canonical_sha256
from .fault_journal import emit_fault

UTC = timezone.utc

SLOT_ROTATION_SETTING = "REALTIME_V3_OPTIONS_SLOT_ROTATION"
SLOT_ROTATION_SETTING_EVENT = "OPTIONS_SLOT_ROTATION_SETTING"
SLOT_ROTATION_RECORD_EVENT = "OPTIONS_SLOT_ROTATION_RECORD"
SLOT_ROTATION_RECORD_NAME_EVENT = "OPTIONS_SLOT_ROTATION_RECORD_NAME"
SLOT_ROTATION_OUTCOME_EVENT = "OPTIONS_SLOT_ROTATION_OUTCOME"
SLOT_ROTATION_RECORD_FAILED_EVENT = "OPTIONS_SLOT_ROTATION_RECORD_FAILED"
SLOT_ROTATION_ON_UNAVAILABLE_EVENT = "OPTIONS_SLOT_ROTATION_ON_UNAVAILABLE"
FAULT_SERVICE = "options-scheduler"
# Docker splits a log line over 16 KiB.
LINE_BYTE_LIMIT = 15_000
WINDOW_SECONDS = 300
FETCH_ROWS = 2_000
STATEMENT_TIMEOUT = "20s"

POLICY: dict[str, object] = {
    "version": "options-slot-rotation-v1",
    "source": "chain-scan-record-v3",
    "applied": False,
    "window_seconds": WINDOW_SECONDS,
    "tradable": {
        "min_dte": 1, "max_dte": 45,
        "min_abs_delta": 0.25, "max_abs_delta": 0.70, "delta": "local_delta else provider delta",
        "min_prior_oi": 100, "min_day_volume": 500, "bid_above": 0,
        "max_spread_fraction_of_mid": 0.10, "min_mid": 0.25, "max_quote_age_seconds": 60,
    },
    "candidate": {
        "min_premium_5m": 50_000, "min_burst": 3.0, "burst_average_floor": 1.0,
        "outside_watched_set": True, "catalogued": True, "confirm_windows": 2,
    },
    "rank": ["premium_5m desc", "burst desc", "contract_ticker asc"],
    "side": {
        "demand": "iv up and option above its delta-implied move", "supply": "iv down and option below it",
        "otherwise": "UNCLEAR", "contract_type_alone_is_direction": False,
    },
    "swap": {
        "same_underlying_only": True,
        "max_per_name_per_window": 2, "max_per_window": 8,
        "max_per_name_per_session": 6, "max_per_session": 48,
        "max_replaced_fraction_of_original": 0.5,
        "min_calls_per_name": 8, "min_puts_per_name": 8,
        "eviction_side": "same side first",
    },
    "eviction": {
        "idle_windows": 3, "flow_quiet_minutes": 15, "flow_unknown_protects": True,
        "protect_session_package_legs": True, "packages_unknown_protects": True,
        "protect_promoted_windows": 6, "evicted_never_repromoted": True,
        "rank": ["idle windows desc", "day volume asc", "open interest asc", "contract_ticker asc"],
    },
    "timing": {
        "skip_first_windows": 2, "stop_minutes_before_close": 30,
        "require_complete_capture": True, "require_previous_window": True,
    },
    "outcome_minutes": [15, 30, 60],
    "outcome_tracking_minutes": 90,
    "reserve_hypothesis": {
        "sizes": [8, 16, 24], "frozen_at": "first evaluated window of the name",
        "static_filters": ["DTE", "DELTA", "OI", "NO_BID", "SPREAD", "MID"],
        "rank": ["open interest desc", "abs(|delta|-0.5) asc", "contract_ticker asc"],
        "outside": "real watched set",
    },
}
POLICY_SHA256 = canonical_sha256(POLICY)

_TRADABLE = POLICY["tradable"]
_CANDIDATE = POLICY["candidate"]
_SWAP = POLICY["swap"]
_EVICTION = POLICY["eviction"]
_TIMING = POLICY["timing"]
_RESERVE = POLICY["reserve_hypothesis"]
_OUTCOME_MINUTES = tuple(POLICY["outcome_minutes"])
_D_SIZES = tuple(_RESERVE["sizes"])
_STATIC_FILTERS = frozenset(_RESERVE["static_filters"])
_NO_PRIOR_BLOCKS = frozenset({"NO_PRIOR_WINDOW", "PRIOR_WINDOW_GAP", "WINDOW_REPEATED"})
_UNKNOWN_INPUT_PROTECTIONS = frozenset({"FLOW_UNKNOWN", "PACKAGES_UNKNOWN"})
_NAME_CANDIDATE_LIMIT = 12
_EVICTABLE_SAMPLE = 6


# ---- the switch -------------------------------------------------------------------------------------------------------

class SlotRotationMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"
    ON = "ON"


def slot_rotation_mode(environ: Mapping[str, str] | None = None) -> SlotRotationMode:
    """`REALTIME_V3_OPTIONS_SLOT_ROTATION`; unset or blank is OFF. Anything but OFF, RECORD or ON raises."""
    raw = (os.environ if environ is None else environ).get(SLOT_ROTATION_SETTING)
    return parse_slot_rotation_mode(raw)


def parse_slot_rotation_mode(raw: object) -> SlotRotationMode:
    if isinstance(raw, SlotRotationMode):
        return raw
    if raw is not None and not isinstance(raw, str):
        raise ValueError(f"{SLOT_ROTATION_SETTING} must be OFF, RECORD or ON, not {raw!r}")
    value = (raw or "").strip().upper() or SlotRotationMode.OFF.value
    try:
        return SlotRotationMode(value)
    except ValueError:
        raise ValueError(f"{SLOT_ROTATION_SETTING} must be OFF, RECORD or ON, not {raw!r}") from None


# ---- log lines --------------------------------------------------------------------------------------------------------

def _dumps(document: Mapping[str, object]) -> str:
    return json.dumps(document, sort_keys=True, default=str, separators=(",", ":"))


def render_line(event: str, document: Mapping[str, object], limit: int = LINE_BYTE_LIMIT) -> str:
    """`EVENT {json}` under `limit` bytes: the longest lists are halved (marked `truncated`) until it fits; at worst only
    the scalar fields stay."""
    text = _dumps(document)
    if len(event) + 1 + len(text.encode("utf-8")) <= limit:
        return f"{event} {text}"
    trimmed = dict(document)
    trimmed["truncated"] = True
    while True:
        text = _dumps(trimmed)
        if len(event) + 1 + len(text.encode("utf-8")) <= limit:
            return f"{event} {text}"
        lists = [(len(_dumps({"v": value})), key) for key, value in trimmed.items()
                 if isinstance(value, list) and value]
        if not lists:
            break
        _size, key = max(lists)
        trimmed[key] = trimmed[key][: len(trimmed[key]) // 2]
    scalars = {key: value for key, value in trimmed.items()
               if isinstance(value, (int, float, bool)) or value is None or (isinstance(value, str) and len(value) < 200)}
    text = _dumps(scalars)
    if len(event) + 1 + len(text.encode("utf-8")) > limit:
        text = _dumps({key: scalars.get(key) for key in ("mode", "session", "equity_cutoff", "underlying")}
                      | {"truncated": True})
    return f"{event} {text}"


def _print_line(event: str, document: Mapping[str, object]) -> None:
    print(render_line(event, document), file=sys.stderr, flush=True)


# ---- plain-row helpers ------------------------------------------------------------------------------------------------

_CONTRACT = re.compile(r"^O:([A-Z][A-Z.\-]*?)\d{6}([CP])\d{8}$")


def contract_parts(ticker: str) -> tuple[str | None, str | None]:
    """(underlying, "call"/"put") from an OCC-style ticker, or (None, None)."""
    match = _CONTRACT.match(str(ticker).strip().upper())
    if match is None:
        return None, None
    return match.group(1), "call" if match.group(2) == "C" else "put"


def _number(value) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _count(value) -> int | None:
    number = _number(value)
    return None if number is None else int(number)


def _moment(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _day(value) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


def _round(value, digits):
    return None if value is None else round(value, digits)


# ---- row evaluation (port of chain_scan_record.evaluate) -------------------------------------------------------------

def _intervals_since_open(at: datetime, session_open: datetime) -> float:
    return max((at - session_open).total_seconds() / WINDOW_SECONDS, 1.0)


def evaluate_row(
    row: Mapping[str, object],
    prior: tuple | None,
    *,
    session_date: date,
    session_open: datetime,
    equity_cutoff: datetime,
) -> dict[str, object]:
    """One breadth row: the tradable filters (with the reasons it fails), then, when `prior` is the previous window's
    values of the same contract (`prior_values`), its 5-minute volume and premium, burst, IV change, move against its
    delta and the demand / supply hint. The keys are the replay rows' (`chain_scan_record` v3)."""
    ticker = str(row["contract_ticker"]).strip().upper()
    kind = str(row.get("contract_type") or "").strip().lower()
    bid, ask = _number(row.get("bid")), _number(row.get("ask"))
    mid = (bid + ask) / 2.0 if bid is not None and ask is not None and ask > 0 and ask >= bid else None
    spread = (ask - bid) / mid if mid else None
    delta = _number(row.get("local_delta"))
    delta = _number(row.get("delta")) if delta is None else delta
    iv = _number(row.get("local_iv"))
    iv = _number(row.get("implied_volatility")) if iv is None else iv
    spot = _number(row.get("spot"))
    spot = _number(row.get("underlying_price")) if spot is None else spot
    observed = _moment(row.get("observed_at"))
    quote_at = _moment(row.get("quote_at"))
    expiration = _day(row.get("expiration_date"))
    dte = None if expiration is None else (expiration - session_date).days
    oi, vol = _count(row.get("open_interest")), _count(row.get("day_volume"))
    age = (observed - quote_at).total_seconds() if observed is not None and quote_at is not None else None
    reasons = []
    if dte is None or not _TRADABLE["min_dte"] <= dte <= _TRADABLE["max_dte"]:
        reasons.append("DTE")
    if delta is None or not _TRADABLE["min_abs_delta"] <= abs(delta) <= _TRADABLE["max_abs_delta"]:
        reasons.append("DELTA")
    if not oi or oi < _TRADABLE["min_prior_oi"]:
        reasons.append("OI")
    if not vol or vol < _TRADABLE["min_day_volume"]:
        reasons.append("VOLUME")
    if bid is None or bid <= _TRADABLE["bid_above"]:
        reasons.append("NO_BID")
    if spread is None or spread > _TRADABLE["max_spread_fraction_of_mid"]:
        reasons.append("SPREAD")
    if mid is None or mid < _TRADABLE["min_mid"]:
        reasons.append("MID")
    if age is None or age > _TRADABLE["max_quote_age_seconds"]:
        reasons.append("QUOTE_AGE")
    out: dict[str, object] = {
        "c": ticker, "u": str(row.get("underlying") or "").strip().upper(), "type": kind, "dte": dte,
        "strike": _number(row.get("strike_price")), "vol": vol, "oi": oi, "mid": mid, "spread": spread,
        "delta": delta, "iv": iv, "spot": spot, "obs": observed, "tradable": not reasons, "reasons": reasons,
        "vol_oi": (vol / oi) if vol is not None and oi else None,
    }
    previous = equity_cutoff - timedelta(seconds=WINDOW_SECONDS)
    if (
        prior is None or prior[5] != previous or observed is None or prior[4] is None
        or prior[4] >= observed.timestamp() or vol is None or prior[0] is None
    ):
        return out
    prior_vol, prior_mid, prior_iv, prior_spot, prior_epoch = prior[:5]
    prior_obs = datetime.fromtimestamp(prior_epoch, tz=UTC)
    dvol = max(vol - prior_vol, 0)
    average = prior_vol / _intervals_since_open(prior_obs, session_open) if prior_vol else 0.0
    out["dvol"] = dvol
    out["dprem"] = dvol * (mid or 0.0) * 100.0
    out["burst"] = dvol / max(average, _CANDIDATE["burst_average_floor"])
    if iv is not None and prior_iv is not None:
        out["div"] = (iv - prior_iv) * 100.0  # vol points
    if mid is not None and prior_mid and delta is not None and spot is not None and prior_spot:
        out["pvd"] = ((mid - prior_mid) - delta * (spot - prior_spot)) / prior_mid
    last_at = _moment(row.get("last_trade_at"))
    price = _number(row.get("last_trade_price"))
    if last_at is not None and last_at > prior_obs and price is not None and bid is not None and ask is not None:
        out["last"] = "AT_ASK" if price >= ask else "AT_BID" if price <= bid else "INSIDE"
    div, pvd = out.get("div"), out.get("pvd")
    side = "UNCLEAR"
    if div is not None and pvd is not None:
        side = "DEMAND" if div > 0 and pvd > 0 else "SUPPLY" if div < 0 and pvd < 0 else "UNCLEAR"
    out["side"] = side
    sign = 1 if kind.startswith("c") else -1
    out["dir"] = sign if side == "DEMAND" else -sign if side == "SUPPLY" else 0
    return out


def prior_values(evaluated: Mapping[str, object], equity_cutoff: datetime) -> tuple:
    """What the next window needs of this one: (volume, mid, iv, spot, observed epoch, equity cutoff)."""
    observed = evaluated.get("obs")
    observed = _moment(observed)
    return (evaluated.get("vol"), evaluated.get("mid"), evaluated.get("iv"), evaluated.get("spot"),
            None if observed is None else observed.timestamp(), equity_cutoff)


def is_candidate(evaluated: Mapping[str, object]) -> bool:
    """Tradable with a 5-minute premium and burst over the policy's floors (watched or not)."""
    return bool(
        evaluated.get("tradable")
        and (evaluated.get("dprem") or 0) >= _CANDIDATE["min_premium_5m"]
        and (evaluated.get("burst") or 0) >= _CANDIDATE["min_burst"]
    )


def is_statically_tradable(evaluated: Mapping[str, object]) -> bool:
    """Tradable on the filters a reserve declared before the session could know (no volume or quote-age test)."""
    return not (_STATIC_FILTERS & set(evaluated.get("reasons") or ()))


def candidate_rank(evaluated: Mapping[str, object]) -> tuple:
    return (-(evaluated.get("dprem") or 0.0), -(evaluated.get("burst") or 0.0), str(evaluated["c"]))


def reserve_rank(evaluated: Mapping[str, object]) -> tuple:
    delta = evaluated.get("delta")
    closeness = abs(abs(delta) - 0.5) if delta is not None else 9.0
    return (-(evaluated.get("oi") or 0), closeness, str(evaluated["c"]))


# ---- state ------------------------------------------------------------------------------------------------------------

@dataclass
class RotationState:
    """Everything the policy remembers in this process, for one session and one real watched set."""
    session_date: date | None = None
    base_watched: frozenset = frozenset()
    reset_reason: str | None = None
    priors: dict = field(default_factory=dict)
    name_last_window: dict = field(default_factory=dict)
    windows: int = 0
    original: dict = field(default_factory=dict)
    watched: set = field(default_factory=set)
    candidate_streak: dict = field(default_factory=dict)
    idle_streak: dict = field(default_factory=dict)
    promoted_at: dict = field(default_factory=dict)
    evicted: set = field(default_factory=set)
    swaps_by_name: Counter = field(default_factory=Counter)
    replaced_by_name: Counter = field(default_factory=Counter)
    swaps_total: int = 0
    d_hyp: dict = field(default_factory=dict)
    d_hyp_frozen_at: dict = field(default_factory=dict)
    pending: list = field(default_factory=list)
    catalogued: set = field(default_factory=set)
    seen_candidates: set = field(default_factory=set)
    seen_in_d_hyp: dict = field(default_factory=lambda: {size: set() for size in _D_SIZES})
    promotions_in_d_hyp: Counter = field(default_factory=Counter)
    outcomes_emitted: int = 0

    def begin(self, session_date: date, watched: Iterable[str]) -> None:
        """Start (or keep) the session's state. A new session clears everything; a changed real watched set keeps only
        the per-contract values of the previous window."""
        base = frozenset(str(ticker).strip().upper() for ticker in watched)
        if self.session_date == session_date and self.base_watched == base:
            return
        same_session = self.session_date == session_date
        reason = "WATCHED_SET_CHANGED" if same_session else "NEW_SESSION"
        priors = self.priors if same_session else {}
        last = self.name_last_window if same_session else {}
        self.__init__()  # noqa: PLC2801 - reset every field to its default
        self.session_date, self.base_watched, self.reset_reason = session_date, base, reason
        self.priors, self.name_last_window = priors, last
        self.watched = set(base)
        original: dict[str, set] = {}
        for ticker in base:
            underlying, _side = contract_parts(ticker)
            if underlying is not None:
                original.setdefault(underlying, set()).add(ticker)
        self.original = {underlying: frozenset(values) for underlying, values in original.items()}


# ---- the policy (pure: plain rows in, documents out; mutates only `state`) ------------------------------------------

def _underlying_of(ticker: str, by_contract: Mapping[str, Mapping]) -> str | None:
    underlying, _side = contract_parts(ticker)
    if underlying is None and ticker in by_contract:
        underlying = str(by_contract[ticker].get("u") or "") or None
    return underlying


def _side_of(ticker: str, by_contract: Mapping[str, Mapping]) -> str | None:
    _underlying, side = contract_parts(ticker)
    if side is None and ticker in by_contract:
        kind = str(by_contract[ticker].get("type") or "").lower()
        side = "call" if kind.startswith("c") else "put" if kind.startswith("p") else None
    return side


def _side_ok(counts: Mapping[str, int], *, evict_side: str | None, add_side: str | None) -> bool:
    after = Counter(counts)
    if evict_side is not None:
        after[evict_side] -= 1
    if add_side is not None:
        after[add_side] += 1
    minimum = {"call": _SWAP["min_calls_per_name"], "put": _SWAP["min_puts_per_name"]}
    return all(after[side] >= min(minimum[side], counts.get(side, 0)) for side in ("call", "put"))


def _d_hyp_size(state: RotationState, ticker: str, underlying: str) -> int | None:
    members = state.d_hyp.get(underlying) or ()
    for size in _D_SIZES:
        if ticker in members[:size]:
            return size
    return None


def _select_swaps(
    ranked: Sequence[Mapping],
    *,
    state: RotationState,
    window_blocks: Sequence[str],
    name_blocks: Mapping[str, str],
    catalogued: Mapping[str, bool] | None,
    evictable: Mapping[str, Sequence[str]],
    by_contract: Mapping[str, Mapping],
    side_counts: Mapping[str, Mapping[str, int]],
    confirm_windows: int,
) -> tuple[list[dict], dict[str, str]]:
    """The window's swaps in rank order under every budget, without changing `state`; and each candidate's decision
    (PROMOTE or the first reason it was not)."""
    window_count = 0
    name_window: Counter = Counter()
    name_session = Counter(state.swaps_by_name)
    replaced = Counter(state.replaced_by_name)
    session_total = state.swaps_total
    counts = {name: Counter(value) for name, value in side_counts.items()}
    used: set[str] = set()
    swaps: list[dict] = []
    decisions: dict[str, str] = {}
    for evaluated in ranked:
        ticker, underlying = str(evaluated["c"]), str(evaluated["u"])
        add_side = _side_of(ticker, by_contract)
        reason = None
        if window_blocks:
            reason = window_blocks[0]
        elif name_blocks.get(underlying):
            reason = name_blocks[underlying]
        elif ticker in state.evicted:
            reason = "EVICTED_THIS_SESSION"
        elif state.candidate_streak.get(ticker, 0) < confirm_windows:
            reason = "NOT_CONFIRMED"
        elif catalogued is None or ticker not in catalogued:
            reason = "CATALOGUE_UNKNOWN"
        elif not catalogued[ticker]:
            reason = "NOT_CATALOGUED"
        elif session_total >= _SWAP["max_per_session"]:
            reason = "BUDGET_SESSION"
        elif window_count >= _SWAP["max_per_window"]:
            reason = "BUDGET_WINDOW"
        elif name_session[underlying] >= _SWAP["max_per_name_per_session"]:
            reason = "BUDGET_NAME_SESSION"
        elif name_window[underlying] >= _SWAP["max_per_name_per_window"]:
            reason = "BUDGET_NAME_WINDOW"
        evict = None
        if reason is None:
            pool = [item for item in evictable.get(underlying, ()) if item not in used]
            if not pool:
                reason = "NO_EVICTABLE"
            else:
                original = state.original.get(underlying, frozenset())
                cap = int(math.floor(_SWAP["max_replaced_fraction_of_original"] * len(original)))
                ordered = ([item for item in pool if _side_of(item, by_contract) == add_side]
                           + [item for item in pool if _side_of(item, by_contract) != add_side])
                for item in ordered:
                    if item in original and replaced[underlying] >= cap:
                        reason = reason or "BUDGET_REPLACED_FRACTION"
                        continue
                    if not _side_ok(counts.get(underlying, Counter()), evict_side=_side_of(item, by_contract),
                                    add_side=add_side):
                        reason = reason or "SIDE_MINIMUM"
                        continue
                    evict, reason = item, None
                    break
        if reason is not None:
            decisions[ticker] = reason
            continue
        evicted_row = by_contract.get(evict) or {}
        evict_side = _side_of(evict, by_contract)
        swaps.append({
            "u": underlying, "promote": ticker, "evict": evict,
            "promote_side": add_side, "evict_side": evict_side,
            "evict_original": evict in state.original.get(underlying, frozenset()),
            "evict_idle_windows": state.idle_streak.get(evict, 0),
            "evict_vol": evicted_row.get("vol"), "evict_mid": evicted_row.get("mid"),
            "dprem": round(evaluated.get("dprem") or 0.0), "burst": _round(evaluated.get("burst"), 1),
            "side": evaluated.get("side"), "dir": evaluated.get("dir"), "mid": evaluated.get("mid"),
            "spot": evaluated.get("spot"), "dte": evaluated.get("dte"), "delta": _round(evaluated.get("delta"), 3),
            "d_hyp": _d_hyp_size(state, ticker, underlying),
        })
        decisions[ticker] = "PROMOTE"
        used.add(evict)
        window_count += 1
        name_window[underlying] += 1
        name_session[underlying] += 1
        session_total += 1
        if evict in state.original.get(underlying, frozenset()):
            replaced[underlying] += 1
        side_count = counts.setdefault(underlying, Counter())
        if evict_side is not None:
            side_count[evict_side] -= 1
        if add_side is not None:
            side_count[add_side] += 1
    return swaps, decisions


def _outcomes(
    state: RotationState,
    *,
    by_contract: Mapping[str, Mapping],
    complete: Collection[str],
    equity_cutoff: datetime,
    common: Mapping[str, object],
) -> list[dict]:
    """+15/+30/+60 min of each simulated promotion, from the first window at or past each horizon that has its row."""
    documents, keep = [], []
    tracking = timedelta(minutes=int(POLICY["outcome_tracking_minutes"]))
    for pending in state.pending:
        elapsed = equity_cutoff - pending["cutoff"]
        done = set(pending["done"])
        row = by_contract.get(pending["c"]) if pending["u"] in complete else None
        if row is not None and row.get("mid") is not None:
            evicted = by_contract.get(pending["evict"]) or {}
            for minutes in _OUTCOME_MINUTES:
                if minutes in done or elapsed < timedelta(minutes=minutes):
                    continue
                mid, spot = row.get("mid"), row.get("spot")
                option_return = (mid - pending["mid"]) / pending["mid"] if pending["mid"] else None
                stock_move = (
                    (spot - pending["spot"]) / pending["spot"] * (pending["dir"] or 0)
                    if pending["spot"] and spot is not None else None
                )
                evicted_vol, evicted_mid = evicted.get("vol"), evicted.get("mid")
                documents.append({
                    **common,
                    "contract": pending["c"], "underlying": pending["u"],
                    "promoted_cutoff": pending["cutoff"].isoformat(), "minutes": minutes,
                    "observed_minutes": round(elapsed.total_seconds() / 60.0, 1),
                    "mid_at_promotion": pending["mid"], "mid": mid, "option_return": _round(option_return, 5),
                    "spot_at_promotion": pending["spot"], "spot": spot,
                    "signed_stock_move": _round(stock_move, 6), "dir": pending["dir"], "side": pending["side"],
                    "premium_5m_at_promotion": pending["dprem"], "burst_at_promotion": pending["burst"],
                    "d_hyp": pending["d_hyp"],
                    "evicted": pending["evict"],
                    "evicted_volume_since": (
                        evicted_vol - pending["evict_vol"]
                        if evicted_vol is not None and pending["evict_vol"] is not None else None
                    ),
                    "evicted_option_return": _round(
                        (evicted_mid - pending["evict_mid"]) / pending["evict_mid"]
                        if evicted_mid is not None and pending["evict_mid"] else None, 5,
                    ),
                })
                done.add(minutes)
        pending["done"] = sorted(done)
        if len(done) < len(_OUTCOME_MINUTES) and elapsed < tracking:
            keep.append(pending)
    state.pending = keep
    state.outcomes_emitted += len(documents)
    return documents


def decide_window(
    state: RotationState,
    *,
    session_date: date,
    equity_cutoff: datetime,
    as_of: datetime,
    session_open: datetime,
    session_close: datetime,
    evals: Sequence[Mapping[str, object]],
    names: Collection[str],
    complete: Collection[str],
    watched: Collection[str],
    catalogued: Mapping[str, bool] | None,
    flow_last_print: Mapping[str, datetime] | None,
    package_legs: Collection[str] | None,
    mode: str = SlotRotationMode.RECORD.value,
    window_id: str | None = None,
    contracts: int | None = None,
    reason_counts: Mapping[str, int] | None = None,
    calendar_ok: bool = True,
    input_states: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """One window of the policy over plain evaluated rows (`evaluate_row` shape) of the names whose capture is
    COMPLETE. Updates `state` as if the confirmed swaps were applied (simulation) and returns
    {"summary": doc, "names": [doc], "outcomes": [doc], "swaps": [...], "swaps_now": [...]}."""
    started = time.monotonic()
    state.begin(session_date, watched)
    cutoff = _moment(equity_cutoff)
    complete = frozenset(str(name) for name in complete)
    names = sorted({str(name) for name in names} | set(complete) | set(state.original))
    evals = [row for row in evals if row.get("u") in complete]
    by_contract = {str(row["c"]): row for row in evals}
    window_index = int(round((cutoff - session_open).total_seconds() / WINDOW_SECONDS))
    window_blocks = []
    if window_index <= _TIMING["skip_first_windows"]:
        window_blocks.append("FIRST_WINDOWS")
    if cutoff >= session_close - timedelta(minutes=_TIMING["stop_minutes_before_close"]):
        window_blocks.append("CLOSING")
    if not calendar_ok:
        window_blocks.append("SESSION_CALENDAR_UNAVAILABLE")
    previous = cutoff - timedelta(seconds=WINDOW_SECONDS)
    name_blocks: dict[str, str] = {}
    for name in names:
        if name not in complete:
            name_blocks[name] = "CAPTURE_NOT_COMPLETE"
        elif state.name_last_window.get(name) is None:
            name_blocks[name] = "NO_PRIOR_WINDOW"
        elif state.name_last_window[name] == cutoff:
            name_blocks[name] = "WINDOW_REPEATED"  # the scheduler attaches a window once; kept for replays
        elif state.name_last_window[name] != previous:
            name_blocks[name] = "PRIOR_WINDOW_GAP"
    valid = {name for name in complete if name_blocks.get(name) not in _NO_PRIOR_BLOCKS}

    # The hypothetical reserve, frozen at each name's first evaluated window.
    for name in sorted(complete - set(state.d_hyp)):
        pool = sorted(
            (row for row in evals
             if row["u"] == name and row["c"] not in state.base_watched and is_statically_tradable(row)),
            key=reserve_rank,
        )
        state.d_hyp[name] = tuple(str(row["c"]) for row in pool[: max(_D_SIZES)])
        state.d_hyp_frozen_at[name] = cutoff.isoformat()

    # Hysteresis: consecutive candidate windows outside the (simulated) watched set; consecutive idle windows of
    # watched contracts. A name without the previous window's values starts both again.
    state.candidate_streak = {
        str(row["c"]): state.candidate_streak.get(str(row["c"]), 0) + 1
        for row in evals
        if row["u"] in valid and is_candidate(row) and row["c"] not in state.watched
    }
    idle = {}
    for ticker in state.watched:
        row = by_contract.get(ticker)
        if row is not None and row["u"] in valid and row.get("dvol") == 0:
            idle[ticker] = state.idle_streak.get(ticker, 0) + 1
    state.idle_streak = idle

    # Protections of the watched contracts of complete names.
    flow_window = timedelta(minutes=_EVICTION["flow_quiet_minutes"])
    promoted_window = timedelta(seconds=WINDOW_SECONDS * _EVICTION["protect_promoted_windows"])
    protected: Counter = Counter()
    protected_by_name: dict[str, Counter] = {}
    protected_contracts = protected_only_by_unknown = 0
    evictable: dict[str, list[str]] = {}
    side_counts: dict[str, Counter] = {}
    watched_by_name: Counter = Counter()
    for ticker in sorted(state.watched):
        name = _underlying_of(ticker, by_contract)
        if name is None:
            continue
        watched_by_name[name] += 1
        side = _side_of(ticker, by_contract)
        if side is not None:
            side_counts.setdefault(name, Counter())[side] += 1
        if name not in complete:
            continue
        reasons = []
        if ticker not in by_contract:
            reasons.append("NO_BREADTH_ROW")
        if state.idle_streak.get(ticker, 0) < _EVICTION["idle_windows"]:
            reasons.append("NOT_IDLE")
        if flow_last_print is None:
            reasons.append("FLOW_UNKNOWN")
        else:
            last = flow_last_print.get(ticker)
            if last is not None and last > as_of - flow_window:
                reasons.append("RECENT_FLOW")
        if package_legs is None:
            reasons.append("PACKAGES_UNKNOWN")
        elif ticker in package_legs:
            reasons.append("PACKAGE_LEG")
        promoted = state.promoted_at.get(ticker)
        if promoted is not None and cutoff - promoted < promoted_window:
            reasons.append("RECENTLY_PROMOTED")
        if reasons:
            protected_contracts += 1
            if set(reasons) <= _UNKNOWN_INPUT_PROTECTIONS:
                protected_only_by_unknown += 1
            protected.update(reasons)
            protected_by_name.setdefault(name, Counter()).update(reasons)
        else:
            evictable.setdefault(name, []).append(ticker)

    def eviction_rank(ticker):
        row = by_contract.get(ticker) or {}
        return (-state.idle_streak.get(ticker, 0), row.get("vol") or 0, row.get("oi") or 0, ticker)

    for name in evictable:
        evictable[name].sort(key=eviction_rank)

    candidates = [row for row in evals if is_candidate(row)]
    outside = sorted((row for row in candidates if row["c"] not in state.watched), key=candidate_rank)
    selection = dict(
        state=state, window_blocks=window_blocks, name_blocks=name_blocks, catalogued=catalogued,
        evictable=evictable, by_contract=by_contract, side_counts=side_counts,
    )
    swaps_now, _ = _select_swaps(outside, confirm_windows=1, **selection)
    swaps, decisions = _select_swaps(outside, confirm_windows=int(_CANDIDATE["confirm_windows"]), **selection)

    common = {"mode": mode, "applied": False, "session": str(session_date), "equity_cutoff": cutoff.isoformat()}
    outcomes = _outcomes(state, by_contract=by_contract, complete=complete, equity_cutoff=cutoff, common=common)

    # Coverage of the hypothetical reserve; session totals.
    in_d = {size: 0 for size in _D_SIZES}
    for row in outside:
        size = _d_hyp_size(state, str(row["c"]), str(row["u"]))
        state.seen_candidates.add(str(row["c"]))
        for limit in _D_SIZES:
            if size is not None and size <= limit:
                in_d[limit] += 1
                state.seen_in_d_hyp[limit].add(str(row["c"]))

    # Apply the confirmed swaps to the simulation.
    for swap in swaps:
        promote, evict, name = swap["promote"], swap["evict"], swap["u"]
        state.watched.discard(evict)
        state.watched.add(promote)
        state.promoted_at[promote] = cutoff
        state.evicted.add(evict)
        state.idle_streak.pop(evict, None)
        state.candidate_streak.pop(promote, None)
        state.swaps_by_name[name] += 1
        state.swaps_total += 1
        if swap["evict_original"]:
            state.replaced_by_name[name] += 1
        for limit in _D_SIZES:
            if swap["d_hyp"] is not None and swap["d_hyp"] <= limit:
                state.promotions_in_d_hyp[limit] += 1
        state.pending.append({
            "c": promote, "u": name, "cutoff": cutoff, "mid": swap["mid"], "spot": swap["spot"],
            "dir": swap["dir"], "side": swap["side"], "dprem": swap["dprem"], "burst": swap["burst"],
            "d_hyp": swap["d_hyp"], "evict": evict, "evict_vol": swap["evict_vol"], "evict_mid": swap["evict_mid"],
            "done": [],
        })
    for name in complete:
        state.name_last_window[name] = cutoff
    state.windows += 1

    # Documents.
    catalogue_known = [row for row in outside if catalogued is not None and row["c"] in catalogued]
    catalogued_count = sum(1 for row in catalogue_known if catalogued[row["c"]])
    tradable = [row for row in evals if row.get("tradable")]
    blocks_by_reason: dict[str, list[str]] = {}
    for name, reason in sorted(name_blocks.items()):
        blocks_by_reason.setdefault(reason, []).append(name)
    cap = {
        name: int(math.floor(_SWAP["max_replaced_fraction_of_original"] * len(original)))
        for name, original in state.original.items()
    }
    summary = {
        **common,
        "policy": POLICY["version"], "policy_sha256": POLICY_SHA256,
        "window_id": window_id, "window_index": window_index, "as_of": as_of.isoformat(),
        "state_windows": state.windows, **({"state_reset": state.reset_reason} if state.windows == 1 else {}),
        "captures": {
            "complete": len(complete), "names": len(names),
            "not_complete": sorted(name for name in names if name not in complete),
        },
        "contracts": len(evals) if contracts is None else contracts,
        "watched": len(state.watched), "watched_real": len(state.base_watched),
        "tradable": len(tradable),
        "tradable_watched": sum(1 for row in tradable if row["c"] in state.watched),
        "tradable_watched_real": sum(1 for row in tradable if row["c"] in state.base_watched),
        "candidates": len(candidates),
        "candidates_watched": len(candidates) - len(outside),
        "candidates_watched_real": sum(1 for row in candidates if row["c"] in state.base_watched),
        "candidates_outside": len(outside),
        "candidates_confirmed": sum(
            1 for row in outside if state.candidate_streak.get(row["c"], 0) >= _CANDIDATE["confirm_windows"]
            or decisions.get(row["c"]) == "PROMOTE"
        ),
        "candidates_catalogued": catalogued_count,
        "candidates_catalogue_known": len(catalogue_known),
        "catalogued_share": round(catalogued_count / len(catalogue_known), 4) if catalogue_known else None,
        "candidates_in_d_hyp": {str(size): in_d[size] for size in _D_SIZES},
        "candidate_decisions": dict(sorted(Counter(decisions.values()).items())),
        "swaps_now": len(swaps_now), "swaps_confirmed": len(swaps),
        "evictable": sum(len(value) for value in evictable.values()),
        "protected": dict(sorted(protected.items())),
        "protected_contracts": protected_contracts,
        # Watched contracts that only an unknown input (flow / packages) keeps from being evictable.
        "protected_only_by_unknown": protected_only_by_unknown,
        "budgets_remaining": {
            "session": _SWAP["max_per_session"] - state.swaps_total,
            "window": _SWAP["max_per_window"] - len(swaps),
            "name_session": {
                name: _SWAP["max_per_name_per_session"] - count
                for name, count in sorted(state.swaps_by_name.items())
            },
            "name_replaced": {
                name: cap.get(name, 0) - count for name, count in sorted(state.replaced_by_name.items())
            },
        },
        "blocked": {"window": window_blocks, "names": blocks_by_reason},
        "inputs": dict(input_states or {}),
        "session_totals": {
            "swaps": state.swaps_total,
            "unique_candidates": len(state.seen_candidates),
            "unique_candidates_in_d_hyp": {str(size): len(state.seen_in_d_hyp[size]) for size in _D_SIZES},
            "promotions_in_d_hyp": {str(size): state.promotions_in_d_hyp[size] for size in _D_SIZES},
            "names_with_swaps": len(state.swaps_by_name),
            "outcomes": state.outcomes_emitted,
            "pending_outcomes": len(state.pending),
        },
        **({"filter_reasons": dict(sorted(reason_counts.items()))} if reason_counts else {}),
    }
    name_documents = []
    swaps_by_name: dict[str, list] = {}
    for swap in swaps:
        swaps_by_name.setdefault(swap["u"], []).append(swap)
    now_by_name: dict[str, list] = {}
    for swap in swaps_now:
        now_by_name.setdefault(swap["u"], []).append(swap["promote"])
    outside_by_name: dict[str, list] = {}
    for row in outside:
        outside_by_name.setdefault(str(row["u"]), []).append(row)
    for name in sorted(complete):
        rows = outside_by_name.get(name, [])
        if not rows and name not in swaps_by_name and name not in now_by_name:
            continue
        name_tradable = [row for row in tradable if row["u"] == name]
        sides = side_counts.get(name, Counter())
        name_documents.append({
            **common,
            "underlying": name,
            "block": window_blocks[0] if window_blocks else name_blocks.get(name),
            "watched": watched_by_name.get(name, 0), "calls": sides.get("call", 0), "puts": sides.get("put", 0),
            "original": len(state.original.get(name, ())),
            "tradable": len(name_tradable),
            "tradable_watched": sum(1 for row in name_tradable if row["c"] in state.watched),
            "candidates_outside": len(rows),
            "candidates": [
                {
                    "c": row["c"], "type": row.get("type"), "dte": row.get("dte"), "strike": row.get("strike"),
                    "delta": _round(row.get("delta"), 3), "mid": row.get("mid"),
                    "spread": _round(row.get("spread"), 4), "vol": row.get("vol"), "oi": row.get("oi"),
                    "dvol": row.get("dvol"), "dprem": round(row.get("dprem") or 0.0),
                    "burst": _round(row.get("burst"), 1), "side": row.get("side"), "dir": row.get("dir"),
                    "last": row.get("last"), "streak": state.candidate_streak.get(row["c"], 0),
                    "catalogued": None if catalogued is None else catalogued.get(row["c"]),
                    "d_hyp": _d_hyp_size(state, str(row["c"]), name),
                    "decision": decisions.get(row["c"]),
                }
                for row in rows[:_NAME_CANDIDATE_LIMIT]
            ],
            "promote": [
                {key: swap[key] for key in (
                    "promote", "evict", "promote_side", "evict_side", "evict_original", "evict_idle_windows",
                    "dprem", "burst", "side", "dir", "dte", "delta", "d_hyp",
                )}
                for swap in swaps_by_name.get(name, [])
            ],
            "would_promote_unconfirmed": now_by_name.get(name, []),
            "evictable": len(evictable.get(name, ())),
            "evictable_sample": evictable.get(name, [])[:_EVICTABLE_SAMPLE],
            "protected": dict(sorted(protected_by_name.get(name, Counter()).items())),
            "budget_remaining": {
                "name_session": _SWAP["max_per_name_per_session"] - state.swaps_by_name.get(name, 0),
                "name_replaced": cap.get(name, 0) - state.replaced_by_name.get(name, 0),
            },
            "d_hyp_frozen_at": state.d_hyp_frozen_at.get(name),
        })
    summary["seconds"] = round(time.monotonic() - started, 3)
    return {"summary": summary, "names": name_documents, "outcomes": outcomes, "swaps": swaps,
            "swaps_now": swaps_now}


# ---- the window's reads (one short read-only transaction) ------------------------------------------------------------

CAPTURES_SQL = (
    "SELECT underlying,state,observed_at FROM rt.option_breadth_captures "
    "WHERE session_date=%s AND equity_cutoff=%s ORDER BY underlying,observed_at DESC"
)
ROW_COLUMNS = (
    "contract_ticker", "underlying", "contract_type", "expiration_date", "strike_price", "day_volume",
    "open_interest", "last_trade_price", "last_trade_at", "bid", "ask", "quote_at", "implied_volatility", "delta",
    "local_iv", "local_delta", "spot", "underlying_price", "observed_at",
)
ROWS_SQL = (
    f"SELECT {','.join(ROW_COLUMNS)} FROM rt.option_breadth_current "
    "WHERE session_date=%s AND underlying=ANY(%s) AND observed_at=ANY(%s)"
)
CATALOGUE_SQL = (
    "SELECT DISTINCT contract.contract_ticker FROM ref.option_contracts AS contract "
    "JOIN rt.instruments AS instrument ON instrument.contract_id=contract.contract_id "
    "AND instrument.asset_class='OPTION' WHERE contract.contract_ticker=ANY(%s)"
)
FLOW_SQL = (
    "SELECT contract_ticker,max(last_event_time) FROM rt.option_volume_checkpoints "
    "WHERE session_date=%s AND contract_ticker=ANY(%s) GROUP BY contract_ticker"
)
PACKAGE_LEGS_SQL = (
    "SELECT DISTINCT leg.contract_ticker FROM rt.option_packages AS package "
    "JOIN rt.option_package_legs AS leg USING (candidate_id) "
    "WHERE package.created_at>=%s AND package.created_at<%s AND leg.contract_ticker=ANY(%s)"
)


@dataclass
class WindowRead:
    captures: dict
    complete: dict
    catalogued: dict | None
    flow_last_print: dict | None
    package_legs: set | None
    states: dict
    errors: dict
    seconds: float = 0.0


def _value(row, index: int, name: str):
    return row[name] if isinstance(row, Mapping) else row[index]


def _optional_read(cursor, savepoint: str, sql: str, params) -> list:
    """A read the policy can do without: its failure rolls back to the savepoint and raises to the caller, which
    treats that input as unknown (protective)."""
    cursor.execute(f"SAVEPOINT {savepoint}")
    try:
        cursor.execute(sql, params)
        rows = list(cursor.fetchall())
    except Exception:
        cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
        raise
    cursor.execute(f"RELEASE SAVEPOINT {savepoint}")
    return rows


def read_window_cursor(
    cursor,
    *,
    session_date: date,
    equity_cutoff: datetime,
    watched: Collection[str],
    consume: Callable[[Iterator[dict]], None],
    candidate_tickers: Callable[[], Sequence[str]],
    known_catalogued: Collection[str] = (),
) -> WindowRead:
    """Every read of one window, in the caller's read-only transaction: the window's captures, the COMPLETE captures'
    rows (streamed into `consume`), the catalogue rows of `candidate_tickers()` (asked after `consume`), the flow's last
    print and the session's package legs of the `watched` contracts. The last three may fail alone (unknown)."""
    started = time.monotonic()
    cursor.execute(f"SET LOCAL statement_timeout = '{STATEMENT_TIMEOUT}'")
    cursor.execute(CAPTURES_SQL, (session_date, equity_cutoff))
    captures: dict[str, tuple[str, datetime]] = {}
    for row in cursor.fetchall():
        underlying = str(_value(row, 0, "underlying")).strip().upper()
        if underlying not in captures:  # newest first
            captures[underlying] = (str(_value(row, 1, "state")), _moment(_value(row, 2, "observed_at")))
    complete = {name: observed for name, (state, observed) in captures.items() if state == "COMPLETE"}

    def rows() -> Iterator[dict]:
        if not complete:
            return
        cursor.execute(ROWS_SQL, (session_date, sorted(complete), sorted(set(complete.values()))))
        while True:
            batch = cursor.fetchmany(FETCH_ROWS)
            if not batch:
                return
            for values in batch:
                item = dict(values) if isinstance(values, Mapping) else dict(zip(ROW_COLUMNS, values))
                name = str(item.get("underlying") or "").strip().upper()
                if name in complete and _moment(item.get("observed_at")) == complete[name]:
                    yield item

    consume(rows())
    states: dict[str, str] = {}
    errors: dict[str, str] = {}
    known = {str(ticker) for ticker in known_catalogued}
    wanted = sorted({str(ticker) for ticker in candidate_tickers()} - known)
    catalogued: dict[str, bool] | None = {ticker: True for ticker in known}
    if wanted:
        try:
            found = {str(_value(row, 0, "contract_ticker")) for row in _optional_read(
                cursor, "al10_catalogue", CATALOGUE_SQL, (wanted,),
            )}
            catalogued.update({ticker: ticker in found for ticker in wanted})
            states["catalogue"] = "KNOWN"
        except Exception as error:  # noqa: BLE001 - unknown blocks promotion only
            states["catalogue"] = "UNKNOWN"
            errors["catalogue"] = f"{type(error).__name__}: {error}"[:200]
    else:
        states["catalogue"] = "NOT_NEEDED"
    tickers = sorted({str(ticker) for ticker in watched})
    flow: dict[str, datetime] | None = None
    try:
        found_flow = {
            str(_value(row, 0, "contract_ticker")): _moment(_value(row, 1, "max"))
            for row in _optional_read(cursor, "al10_flow", FLOW_SQL, (session_date, tickers))
        }
        found_flow = {ticker: moment for ticker, moment in found_flow.items() if moment is not None}
        # No checkpoint for any watched contract: the flow is not known to be writing, so it stays unknown.
        states["flow"] = "KNOWN" if found_flow else "EMPTY"
        flow = found_flow or None
    except Exception as error:  # noqa: BLE001 - unknown protects every watched contract
        states["flow"] = "UNKNOWN"
        errors["flow"] = f"{type(error).__name__}: {error}"[:200]
    legs: set[str] | None = None
    try:
        day = datetime(session_date.year, session_date.month, session_date.day, tzinfo=UTC)
        legs = {
            str(_value(row, 0, "contract_ticker"))
            for row in _optional_read(cursor, "al10_packages", PACKAGE_LEGS_SQL,
                                      (day, day + timedelta(days=1), tickers))
        }
        states["packages"] = "KNOWN"
    except Exception as error:  # noqa: BLE001 - unknown protects every watched contract
        states["packages"] = "UNKNOWN"
        errors["packages"] = f"{type(error).__name__}: {error}"[:200]
    return WindowRead(
        captures=captures, complete=complete, catalogued=catalogued, flow_last_print=flow, package_legs=legs,
        states=states, errors=errors, seconds=round(time.monotonic() - started, 3),
    )


class _WindowEvaluator:
    """Evaluates the streamed rows against the previous window's values and keeps only what the policy needs: tradable
    rows, the (real and simulated) watched rows, rows an outcome is waiting for and, for a name whose reserve is not
    frozen yet, its statically tradable rows."""

    def __init__(self, state: RotationState, *, session_date, session_open, equity_cutoff):
        self.state = state
        self.session_date, self.session_open, self.equity_cutoff = session_date, session_open, equity_cutoff
        self.keep = set(state.base_watched) | set(state.watched) | {
            ticker for pending in state.pending for ticker in (pending["c"], pending["evict"])
        }
        self.evals: list[dict] = []
        self.priors: dict = {}
        self.contracts = 0
        self.reasons: Counter = Counter()
        self.as_of: datetime | None = None

    def consume(self, rows: Iterable[Mapping]) -> None:
        for row in rows:
            ticker = str(row["contract_ticker"]).strip().upper()
            evaluated = evaluate_row(
                row, self.state.priors.get(ticker), session_date=self.session_date,
                session_open=self.session_open, equity_cutoff=self.equity_cutoff,
            )
            self.priors[ticker] = prior_values(evaluated, self.equity_cutoff)
            self.contracts += 1
            self.reasons.update(evaluated["reasons"])
            observed = evaluated.get("obs")
            if observed is not None and (self.as_of is None or observed > self.as_of):
                self.as_of = observed
            if (
                evaluated["tradable"] or ticker in self.keep
                or (evaluated["u"] not in self.state.d_hyp and is_statically_tradable(evaluated))
            ):
                self.evals.append(evaluated)

    def candidate_tickers(self) -> list[str]:
        return sorted(
            str(row["c"]) for row in self.evals if is_candidate(row) and row["c"] not in self.state.watched
        )

    def commit_priors(self) -> None:
        self.state.priors.update(self.priors)


def _session_bounds_from_calendar(session_date: date) -> tuple[datetime, datetime]:
    from options.calendar import OptionExchangeCalendar

    calendar = OptionExchangeCalendar()
    return calendar.session_open(session_date), calendar.session_close(session_date)


def _fallback_bounds(session_date: date) -> tuple[datetime, datetime]:
    day = datetime(session_date.year, session_date.month, session_date.day, tzinfo=UTC)
    return day + timedelta(hours=14, minutes=30), day + timedelta(hours=21)


def _failure_reason(error: BaseException) -> str:
    name = type(error).__name__
    if name == "PoolError" or "pool exhausted" in str(error).lower():
        return "NO_CONNECTION"
    return f"{name}"[:60]


def _breadth_complete(breadth) -> set[str] | None:
    """The names breadth captured COMPLETE this window, from the capture summary; None when the summary does not say
    (then the database decides)."""
    if not isinstance(breadth, Mapping):
        return None
    if isinstance(breadth.get("status"), str):
        return set()  # FAILED / NO_UNDERLYINGS / AFTER_ACQUISITION_STOP: no capture this window
    return {
        str(name) for name, value in breadth.items()
        if isinstance(value, Mapping) and value.get("state") == "COMPLETE"
    }


def _breadth_names(breadth) -> set[str]:
    if not isinstance(breadth, Mapping) or isinstance(breadth.get("status"), str):
        return set()
    return {str(name) for name, value in breadth.items() if isinstance(value, Mapping)}


class SlotRotationRecorder:
    """The scheduler's AL10 recorder: one per process (the in-memory state lives here)."""

    def __init__(
        self,
        mode: SlotRotationMode | str,
        *,
        emit: Callable[[str, Mapping[str, object]], None] = _print_line,
        fault: Callable[..., object] = emit_fault,
        session_bounds: Callable[[date], tuple[datetime, datetime]] = _session_bounds_from_calendar,
    ) -> None:
        self.mode = parse_slot_rotation_mode(mode)
        self.state = RotationState()
        self._emit, self._fault, self._session_bounds = emit, fault, session_bounds
        self._bounds: dict[date, tuple[datetime, datetime, bool]] = {}

    def start(self) -> None:
        """The start lines (RECORD and ON only). Never raises."""
        if self.mode is SlotRotationMode.OFF:
            return
        self._safe_emit(SLOT_ROTATION_SETTING_EVENT, {
            "mode": self.mode.value, "setting": SLOT_ROTATION_SETTING, "policy": POLICY,
            "policy_sha256": POLICY_SHA256, "applied": False,
        })
        if self.mode is SlotRotationMode.ON:
            self._safe_emit(SLOT_ROTATION_ON_UNAVAILABLE_EVENT, {
                "reason": "NO_DECLARED_RESERVE", "mode": self.mode.value, "behaves_as": "RECORD",
            })
            self._safe_fault(
                kind="SLOT_ROTATION_ON_UNAVAILABLE", severity="DEGRADED", recoverable=True, action="RECORD_ONLY",
                service=FAULT_SERVICE, key="NO_DECLARED_RESERVE",
                message=f"{SLOT_ROTATION_SETTING}=ON needs a declared reserve (AL10 phase 2); recording only",
                context={"mode": self.mode.value, "policy_sha256": POLICY_SHA256},
            )

    def record_window(
        self,
        provider,
        *,
        session_date: date,
        equity_cutoff: datetime,
        window_id: str | None,
        watched: Collection[str],
        breadth: Mapping[str, object] | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> dict[str, object] | None:
        """OFF: None and nothing else. RECORD / ON: the window's policy run, logged; returns a short summary for the
        cycle result. Never raises."""
        if self.mode is SlotRotationMode.OFF:
            return None
        started = time.monotonic()
        try:
            return self._record(
                provider, session_date=session_date, equity_cutoff=_moment(equity_cutoff), window_id=window_id,
                watched=watched, breadth=breadth, clock=clock, started=started,
            )
        except Exception as error:  # noqa: BLE001 - recording never fails the cycle
            reason = _failure_reason(error)
            detail = f"{type(error).__name__}: {error}"[:300]
            self._failed(reason, detail, session_date=session_date, equity_cutoff=equity_cutoff, window_id=window_id)
            return {"status": "FAILED", "mode": self.mode.value, "reason": reason,
                    "seconds": round(time.monotonic() - started, 3)}

    def _bounds_for(self, session_date: date) -> tuple[datetime, datetime, bool]:
        if session_date not in self._bounds:
            try:
                opened, closed = self._session_bounds(session_date)
                self._bounds = {session_date: (_moment(opened), _moment(closed), True)}
            except Exception as error:  # noqa: BLE001 - swaps are blocked, the window is still recorded
                opened, closed = _fallback_bounds(session_date)
                self._bounds = {session_date: (opened, closed, False)}
                self._safe_fault(
                    kind="SLOT_ROTATION_INPUT_UNAVAILABLE", severity="DEGRADED", recoverable=True,
                    action="BLOCK_SWAPS", service=FAULT_SERVICE, key="calendar",
                    message=f"session calendar unavailable for {session_date}: {type(error).__name__}"[:300],
                    context={"session": str(session_date)},
                )
        return self._bounds[session_date]

    def _record(self, provider, *, session_date, equity_cutoff, window_id, watched, breadth, clock, started):
        from .database_access_v2 import APP_ROLE

        state = self.state
        state.begin(session_date, watched)
        session_open, session_close, calendar_ok = self._bounds_for(session_date)
        evaluator = _WindowEvaluator(
            state, session_date=session_date, session_open=session_open, equity_cutoff=equity_cutoff,
        )
        hint = _breadth_complete(breadth)
        if hint is not None and not hint:
            # The capture summary already says no name is COMPLETE this window: nothing to read.
            read = WindowRead(captures={}, complete={}, catalogued=None, flow_last_print=None, package_legs=None,
                              states={"read": "SKIPPED_NO_COMPLETE_CAPTURE"}, errors={})
        else:
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                read = read_window_cursor(
                    unit.cursor, session_date=session_date, equity_cutoff=equity_cutoff, watched=state.watched,
                    consume=evaluator.consume, candidate_tickers=evaluator.candidate_tickers,
                    known_catalogued=state.catalogued,
                )
        evaluator.commit_priors()
        if read.catalogued:
            state.catalogued.update(ticker for ticker, known in read.catalogued.items() if known)
        for name, error in sorted(read.errors.items()):
            self._safe_fault(
                kind="SLOT_ROTATION_INPUT_UNAVAILABLE", severity="DEGRADED", recoverable=True, action="PROTECT",
                service=FAULT_SERVICE, key=name,
                message=f"slot rotation input {name} unavailable at {equity_cutoff.isoformat()}: {error}"[:300],
                context={"input": name, "equity_cutoff": equity_cutoff.isoformat(), "error": error},
            )
        names = set(read.captures) | _breadth_names(breadth) | set(state.original)
        as_of = evaluator.as_of or max(read.complete.values(), default=None) or _moment(clock())
        decision = decide_window(
            state, session_date=session_date, equity_cutoff=equity_cutoff, as_of=as_of,
            session_open=session_open, session_close=session_close, evals=evaluator.evals, names=names,
            complete=set(read.complete), watched=watched, catalogued=read.catalogued,
            flow_last_print=read.flow_last_print, package_legs=read.package_legs, mode=self.mode.value,
            window_id=window_id, contracts=evaluator.contracts, reason_counts=evaluator.reasons,
            calendar_ok=calendar_ok, input_states=read.states,
        )
        summary = decision["summary"]
        summary["read_seconds"] = read.seconds
        summary["seconds"] = round(time.monotonic() - started, 3)
        self._emit(SLOT_ROTATION_RECORD_EVENT, summary)
        for document in decision["names"]:
            self._emit(SLOT_ROTATION_RECORD_NAME_EVENT, document)
        for document in decision["outcomes"]:
            self._emit(SLOT_ROTATION_OUTCOME_EVENT, document)
        return {
            "status": "RECORDED", "mode": self.mode.value, "applied": False,
            "captures_complete": summary["captures"]["complete"], "candidates": summary["candidates_outside"],
            "swaps_confirmed": summary["swaps_confirmed"], "swaps_now": summary["swaps_now"],
            "blocked": summary["blocked"]["window"], "seconds": summary["seconds"],
        }

    def _failed(self, reason, detail, *, session_date, equity_cutoff, window_id) -> None:
        cutoff = _moment(equity_cutoff)
        self._safe_emit(SLOT_ROTATION_RECORD_FAILED_EVENT, {
            "mode": self.mode.value, "reason": reason, "error": detail, "session": str(session_date),
            "equity_cutoff": None if cutoff is None else cutoff.isoformat(), "window_id": window_id,
            "applied": False,
        })
        self._safe_fault(
            kind="SLOT_ROTATION_RECORD_FAILED", severity="DEGRADED", recoverable=True, action="SKIP_WINDOW",
            service=FAULT_SERVICE, key=reason,
            message=f"slot rotation record failed at {equity_cutoff}: {detail}"[:300],
            context={"reason": reason, "session": str(session_date), "equity_cutoff": str(equity_cutoff)},
        )

    def _safe_emit(self, event, document) -> None:
        try:
            self._emit(event, document)
        except Exception:  # noqa: BLE001
            pass

    def _safe_fault(self, **kwargs) -> None:
        try:
            self._fault(**kwargs)
        except Exception:  # noqa: BLE001
            pass


def start_slot_rotation(mode: SlotRotationMode | str | None = None, **kwargs) -> SlotRotationRecorder | None:
    """The scheduler's start: None for OFF (nothing is logged, nothing runs); otherwise the recorder, after its start
    lines."""
    mode = slot_rotation_mode() if mode is None else parse_slot_rotation_mode(mode)
    if mode is SlotRotationMode.OFF:
        return None
    recorder = SlotRotationRecorder(mode, **kwargs)
    recorder.start()
    return recorder
