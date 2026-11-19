"""G106 AL11 (Nov 18 2026): pre-open selection of the Options watched set by the previous session's volume, RECORD first.

Cause (docs/g106/G106_EVIDENCE.md, Nov 17 AL10 VALUE_REPORT and the Nov 18 AL11 replay): the V4 allocation ranks every
contract by |delta| band first and then by the capture's day volume, but the capture runs 90 s after the open, so a
contract that already traded today carries only its first-minutes volume while one that has not traded yet carries the
whole previous session's volume. The most active contracts rank last inside their band; on Nov 17 the 384 watched
contracts held 10% of the tradable premium (16% of volume) at the close. Replayed on the real captures of Nov 10-17
(six sessions), ranking the tradable band by the previous session's volume (index ETFs keeping today's 0DTE count) held
23.5% / 27.3% of the tradable premium / volume overall (today's code 13.1% / 18.0%) and won on every day, with more
debit-vertical-ready, OS (both sides) and O3 (adjacent strikes) contracts and about as many O1-ready (3x OI) ones.

`REALTIME_V3_OPTIONS_SLOT_SELECTION`, read by the Options lifecycle service (`realtime-v3-options-lifecycle`,
`scripts/run_advanced_options_v3_lifecycle.py`) at start; the selection runs in its CAPACITY_SHARDS stage
(`PostgresAutomaticOptionsPreparationExecutor._allocate_capture`, once per session, after the open capture), AFTER the
AL8 allocation (whatever `REALTIME_V3_OPTIONS_SLOT_SIDE_BALANCE` decided is "today's set" here):
| OFF (default, unset, blank) | today's allocation byte for byte: no database read, no second allocation, no log line |
| RECORD | the allocation used and everything stored are exactly OFF's; the rule's set is computed alongside (the same
           restricted allocation ON would store) and only logged: `OPTIONS_SLOT_SELECTION_RECORD {json}` lines (one
           summary, then one per underlying); never raises (a failure logs `OPTIONS_SLOT_SELECTION_RECORD_FAILED`) |
| ON | the rule's allocation is used and stored, the same lines with `applied` true. Any failure of the rule (input read,
       restricted allocation, a shortfall it cannot repair) logs `OPTIONS_SLOT_SELECTION_RECORD_FAILED` and keeps
       today's allocation: the stage never fails because of AL11 |
Any other value (any case accepted for the three) and the lifecycle service refuses to start; a value other than OFF
with a non-v4 admission profile also refuses.

Rule (`SLOT_SELECTION_POLICY`, per underlying, same capacity: each underlying keeps exactly the number of contracts
today's allocation gave it, 24 at 16 names, the same budget and shard limits):
1. Inputs that exist at CAPACITY_SHARDS: the session's open capture (prior OI, quote, greeks, spot of every captured
   contract) and the previous session's close chain, `rt.option_breadth_current` (the last breadth capture of the
   previous XNYS session: day volume, bid / ask). A name whose previous chain is missing, older than the previous close
   minus `previous_chain_fresh_within_seconds`, or has no volume on any eligible contract keeps today's selection
   (`reason` in its line).
2. Eligible: DTE 1-45 and not a flow-excluded DTE (3-4: no flow lane, so no O1 / OS / O3 input), |delta| 0.25-0.70 on
   the capture's greeks, a two-sided capture quote with bid > 0, and the profile criterion's own spread, moneyness,
   quote-age and deliverable limits for that DTE (so the restricted allocation keeps every pick).
3. Score: previous-session volume, then prior OI, then the contract ticker (deterministic); at most the criterion's
   shortlist bound per side for a DTE band (EXTENSION 22-60: 8 a side), so the resolution keeps every pick.
4. DAILY_EXPIRY and WEEKLY_EXPIRY products (index ETFs and single stocks) keep as many 0DTE contracts as today's
   allocation gives the name (`same_day_contracts` TODAYS_COUNT: 5-6 per ETF on Nov 10-17, single stocks on their expiry days; 0DTE must stay on the stream for detection and the
   coming 0DTE lane D5), taken from the contracts today's resolution already admitted for 0DTE, by the same score,
   calls and puts alternately. The remaining slots follow steps 2-3.
5. Side: one ranking, or, when `REALTIME_V3_OPTIONS_SLOT_SIDE_BALANCE` is ON (AL8), calls and puts alternately (the side
   with fewer picks first), the other side filling when one runs out.
6. A name with fewer eligible contracts than its count is topped up from today's picks (by the same score).
The set is then allocated by the normal V4 allocation over the chosen candidates only (groups, roles, buckets, AL8 side
balance flag, allocation hash as usual); a name the allocation would not fully keep falls back to today's picks and the
allocation runs once more.

Policy v3 (Nov 19 2026, `REALTIME_V3_OPTIONS_SLOT_SELECTION_POLICY` on the same service: unset / blank / `v2` = the
v2 rule above byte for byte, `v3` = this, anything else and the lifecycle service refuses to start). Cause: on Nov 18
the v2 rule held more premium and volume on every name but fewer O1-ready legs (28 vs 51) and one fewer O1 leg with a
vertical partner (20 vs 21): its previous-volume ranking dropped nearly every SPY / QQQ / IWM leg that reached 3x its
open interest (1 vs 18), mostly 1-2 DTE contracts. v3 keeps v2's ranking and, on the protected products (index ETFs
and single stocks: replayed on Nov 10-18, protecting the index ETFs only still lost O1-ready legs on Nov 13 and 18),
first keeps from today's picks (`SLOT_SELECTION_POLICY_V3`):
(a) the legs predicted O1-ready from the 3x OI rule's pre-open inputs (prior OI from the capture, the previous session's
    volume): DTE 1-60 outside the flow-excluded DTEs, prior OI >= 100, previous volume >= 1x prior OI at DTE <= 7 (the
    near weeklies turn their open interest over in a day or two) or >= 3x beyond;
(b) for each of them, the nearest further-OTM leg of the same expiry and side in today's picks with a width of 0.5%-10%
    of spot and prior OI >= 250 and previous volume >= 100 (the debit vertical's partner);
at most `protect_maximum_share_of_target` of the name's slots, best previous volume first. Then today's 0DTE count and
the v2 pool fill the rest under the same shortlist bounds. RECORD lines carry the policy's version and hash; a v3
underlying line also lists the protected legs (and why) and the v2 picks it differs from (`v2_minus_v3` /
`v3_minus_v2`), so the v2 set can be rebuilt from the lines.
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Callable, Iterable, Mapping

from .advanced_options_activity_momentum_c6 import (
    _FLOW_EXCLUDED_DTES,
    V4_ACTIVITY_MOMENTUM_PROFILE_VERSION,
    ActivityMomentumAllocation,
    ActivityMomentumCandidate,
    v4_product_kind,
)
from .advanced_options_selection_c6 import OptionSide
from .contracts import canonical_sha256

SLOT_SELECTION_SETTING = "REALTIME_V3_OPTIONS_SLOT_SELECTION"
SLOT_SELECTION_RECORD_EVENT = "OPTIONS_SLOT_SELECTION_RECORD"
SLOT_SELECTION_RECORD_FAILED_EVENT = "OPTIONS_SLOT_SELECTION_RECORD_FAILED"
SLOT_SELECTION_SETTING_EVENT = "OPTIONS_SLOT_SELECTION_SETTING"
SLOT_SELECTION_POLICY = MappingProxyType({
    "version": "options-slot-selection-v2",
    "score": "PREVIOUS_SESSION_VOLUME_THEN_PRIOR_OI_THEN_TICKER",
    "minimum_dte": 1,
    "maximum_dte": 45,
    "excluded_dtes": tuple(sorted(_FLOW_EXCLUDED_DTES)),
    "minimum_abs_delta": "0.25",
    "maximum_abs_delta": "0.70",
    "same_day_contracts": "TODAYS_COUNT",
    "same_day_products": ("DAILY_EXPIRY", "WEEKLY_EXPIRY"),
    "previous_chain_source": "rt.option_breadth_current",
    "previous_chain_fresh_within_seconds": 3600,
    "side_balance": "WHEN_REALTIME_V3_OPTIONS_SLOT_SIDE_BALANCE_IS_ON",
})
SLOT_SELECTION_POLICY_SHA256 = canonical_sha256(dict(SLOT_SELECTION_POLICY))
# G106 AL11 v3 (Nov 19): which rule the selection runs; unset / blank / v2 = SLOT_SELECTION_POLICY above, unchanged.
SLOT_SELECTION_POLICY_SETTING = "REALTIME_V3_OPTIONS_SLOT_SELECTION_POLICY"
SLOT_SELECTION_POLICY_V3 = MappingProxyType({
    **SLOT_SELECTION_POLICY,
    "version": "options-slot-selection-v3",
    "protect": "TODAYS_PREDICTED_O1_READY_AND_THEIR_VERTICAL_PARTNERS",
    "protect_products": ("DAILY_EXPIRY", "WEEKLY_EXPIRY"),
    "protect_maximum_share_of_target": "0.5",
    "o1_ready_minimum_dte": 1,
    "o1_ready_maximum_dte": 60,
    "o1_ready_minimum_prior_open_interest": 100,
    "o1_ready_short_dte": 7,
    "o1_ready_short_dte_previous_volume_to_oi": "1",
    "o1_ready_previous_volume_to_oi": "3",
    "partner_minimum_width_fraction": "0.005",
    "partner_maximum_width_fraction": "0.10",
    "partner_minimum_prior_open_interest": 250,
    "partner_minimum_previous_volume": 100,
})
SLOT_SELECTION_POLICY_V3_SHA256 = canonical_sha256(dict(SLOT_SELECTION_POLICY_V3))
SLOT_SELECTION_POLICIES = MappingProxyType({"v2": SLOT_SELECTION_POLICY, "v3": SLOT_SELECTION_POLICY_V3})
_MINIMUM_ABS_DELTA = Decimal(SLOT_SELECTION_POLICY["minimum_abs_delta"])
_MAXIMUM_ABS_DELTA = Decimal(SLOT_SELECTION_POLICY["maximum_abs_delta"])
# Docker splits a log line over 16 KiB: one line per underlying (24 contracts each way at most).
_CONTRACT_LIST_LIMIT = 30


class SlotSelectionMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"
    ON = "ON"


def slot_selection_mode(environ: Mapping[str, str] | None = None) -> SlotSelectionMode:
    """`REALTIME_V3_OPTIONS_SLOT_SELECTION`; unset or blank is OFF. Anything but OFF, RECORD or ON raises."""
    raw = (os.environ if environ is None else environ).get(SLOT_SELECTION_SETTING)
    return parse_slot_selection_mode(raw)


def parse_slot_selection_mode(raw: object) -> SlotSelectionMode:
    if isinstance(raw, SlotSelectionMode):
        return raw
    if raw is not None and not isinstance(raw, str):
        raise ValueError(f"{SLOT_SELECTION_SETTING} must be OFF, RECORD or ON, not {raw!r}")
    value = (raw or "").strip().upper() or SlotSelectionMode.OFF.value
    try:
        return SlotSelectionMode(value)
    except ValueError:
        raise ValueError(f"{SLOT_SELECTION_SETTING} must be OFF, RECORD or ON, not {raw!r}") from None


def parse_slot_selection_policy(raw: object) -> Mapping[str, object]:
    """`REALTIME_V3_OPTIONS_SLOT_SELECTION_POLICY`: unset / blank / `v2` (or its version name) = v2, `v3` (or its
    version name) = v3, any case. Anything else raises (the lifecycle service does not start)."""
    if isinstance(raw, Mapping):
        if any(raw is policy for policy in SLOT_SELECTION_POLICIES.values()):
            return raw
        raise ValueError(f"{SLOT_SELECTION_POLICY_SETTING} must be v2 or v3")
    if raw is not None and not isinstance(raw, str):
        raise ValueError(f"{SLOT_SELECTION_POLICY_SETTING} must be v2 or v3, not {raw!r}")
    value = (raw or "").strip().lower() or "v2"
    for key, policy in SLOT_SELECTION_POLICIES.items():
        if value in (key, str(policy["version"])):
            return policy
    raise ValueError(f"{SLOT_SELECTION_POLICY_SETTING} must be v2 or v3, not {raw!r}")


def slot_selection_policy(environ: Mapping[str, str] | None = None) -> Mapping[str, object]:
    """The policy the lifecycle's selection runs (read once at start)."""
    return parse_slot_selection_policy((os.environ if environ is None else environ).get(SLOT_SELECTION_POLICY_SETTING))


def _print_line(event: str, document: Mapping[str, object]) -> None:
    print(f"{event} {json.dumps(document, sort_keys=True, default=str, separators=(',', ':'))}",
          file=sys.stderr, flush=True)


# ------------------------------------------------------------------------------------------------- previous chain
@dataclass(frozen=True)
class PreviousSessionChain:
    """The previous session's close chain (the last breadth capture of that session), by contract ticker."""

    session_date: date
    fresh_after: datetime
    volumes: Mapping[str, int]
    mids: Mapping[str, Decimal]
    newest_observed_at: Mapping[str, datetime]
    total_volume: Mapping[str, int]
    total_premium: Mapping[str, Decimal]

    def state(self, underlying: str) -> str | None:
        """None when the name's previous chain is usable, else the fallback reason."""
        newest = self.newest_observed_at.get(underlying)
        if newest is None:
            return "PREVIOUS_CHAIN_MISSING"
        if newest < self.fresh_after:
            return "PREVIOUS_CHAIN_STALE"
        return None


PREVIOUS_CHAIN_SQL = (
    "SELECT underlying,contract_ticker,expiration_date,day_volume,bid,ask,observed_at "
    "FROM rt.option_breadth_current WHERE session_date=%s AND underlying=ANY(%s)"
)


def previous_session_chain_from_rows(
    rows: Iterable[tuple],
    *,
    previous_session_date: date,
    previous_close: datetime,
    session_date: date,
) -> PreviousSessionChain:
    """Rows of `PREVIOUS_CHAIN_SQL` (underlying, ticker, expiration, day volume, bid, ask, observed_at). Contracts that
    expired before `session_date` count for the freshness clock only."""
    volumes: dict[str, int] = {}
    mids: dict[str, Decimal] = {}
    newest: dict[str, datetime] = {}
    total_volume: Counter = Counter()
    total_premium: dict[str, Decimal] = {}
    for underlying, ticker, expiration, volume, bid, ask, observed_at in rows:
        underlying = str(underlying).upper()
        if observed_at is not None and (underlying not in newest or observed_at > newest[underlying]):
            newest[underlying] = observed_at
        if expiration is not None and expiration < session_date:
            continue
        value = int(volume or 0)
        volumes[str(ticker)] = value
        total_volume[underlying] += value
        if bid is not None and ask is not None and Decimal(ask) > 0:
            mid = (Decimal(bid) + Decimal(ask)) / 2
            mids[str(ticker)] = mid
            total_premium[underlying] = total_premium.get(underlying, Decimal(0)) + mid * value * 100
    fresh_within = timedelta(seconds=int(SLOT_SELECTION_POLICY["previous_chain_fresh_within_seconds"]))
    return PreviousSessionChain(
        session_date=previous_session_date,
        fresh_after=previous_close - fresh_within,
        volumes=MappingProxyType(volumes),
        mids=MappingProxyType(mids),
        newest_observed_at=MappingProxyType(newest),
        total_volume=MappingProxyType(dict(total_volume)),
        total_premium=MappingProxyType(total_premium),
    )


def load_previous_session_chain(cursor, *, session_date: date, underlyings: Iterable[str],
                                calendar=None) -> PreviousSessionChain:
    """One read-only query (Nov 16, 16 names: 17,140 rows, 18 ms on the chain index)."""
    if calendar is None:
        from options.calendar import OptionExchangeCalendar

        calendar = OptionExchangeCalendar("XNYS")
    previous = calendar.previous_session_date(session_date)
    cursor.execute(PREVIOUS_CHAIN_SQL, (previous, sorted({str(value).upper() for value in underlyings})))
    return previous_session_chain_from_rows(
        cursor.fetchall(),
        previous_session_date=previous,
        previous_close=calendar.session_close(previous),
        session_date=session_date,
    )


# ------------------------------------------------------------------------------------------------- rule
def _criterion(profile, dte: int):
    return next((row for row in profile.selection_criteria if row.minimum_dte <= dte <= row.maximum_dte), None)


def slot_selection_eligible(candidate: ActivityMomentumCandidate, profile, session_date: date) -> bool:
    """Rule step 2: the tradable band, held to the profile criterion's own limits for the contract's DTE."""
    dte = candidate.dte_for(session_date)
    if not SLOT_SELECTION_POLICY["minimum_dte"] <= dte <= SLOT_SELECTION_POLICY["maximum_dte"]:
        return False
    if dte in _FLOW_EXCLUDED_DTES or dte > profile.default_discovery_maximum_dte:
        return False
    criterion = _criterion(profile, dte)
    if criterion is None or candidate.deliverable != "STANDARD" or candidate.unavailable_reasons:
        return False
    if candidate.bid is None or candidate.ask is None or candidate.quote_as_of is None or candidate.bid <= 0:
        return False
    if (candidate.observed_at - candidate.quote_as_of).total_seconds() > criterion.maximum_quote_age_seconds:
        return False
    spread = candidate.relative_spread
    if spread is None or spread > criterion.maximum_relative_spread:
        return False
    if candidate.spot is None or abs(candidate.strike - candidate.spot) / candidate.spot \
            > criterion.maximum_moneyness_fraction:
        return False
    if candidate.delta is None or not _MINIMUM_ABS_DELTA <= abs(candidate.delta) <= _MAXIMUM_ABS_DELTA:
        return False
    if (candidate.completed_session_volume is not None
            and candidate.completed_session_volume < criterion.minimum_completed_session_volume):
        return False
    if (criterion.minimum_prior_open_interest is not None and candidate.prior_open_interest is not None
            and candidate.prior_open_interest < criterion.minimum_prior_open_interest):
        return False
    return True


def _score(candidate: ActivityMomentumCandidate, previous: PreviousSessionChain) -> tuple:
    return (
        -int(previous.volumes.get(candidate.contract_ticker, 0)),
        -int(candidate.prior_open_interest or 0),
        candidate.contract_ticker,
    )


def _pick(ranked: list, count: int, chosen: list, *, balance: bool, admit: Callable[[list, object], bool]) -> list:
    """`ranked` (score order) after `chosen`, up to `count` picks, each one `admit`ted (the shortlist bound). One ranking,
    or with `balance` calls and puts alternately (the side with fewer picks so far first; even: the better-scored head),
    the other side filling when one runs out."""
    picks = list(chosen)
    taken = {row.contract_ticker for row in picks}
    lanes = {
        side: [row for row in ranked if row.option_side is side and row.contract_ticker not in taken]
        for side in (OptionSide.CALL, OptionSide.PUT)
    }
    position = {row.contract_ticker: index for index, row in enumerate(ranked)}
    while len(picks) < count:
        for side in lanes:
            while lanes[side] and not admit(picks, lanes[side][0]):
                lanes[side].pop(0)
        heads = [side for side in lanes if lanes[side]]
        if not heads:
            break
        counts = Counter(row.option_side for row in picks)
        if balance and len(heads) == 2 and counts[OptionSide.CALL] != counts[OptionSide.PUT]:
            side = OptionSide.CALL if counts[OptionSide.CALL] < counts[OptionSide.PUT] else OptionSide.PUT
        else:
            side = min(heads, key=lambda value: position[lanes[value][0].contract_ticker])
        picks.append(lanes[side].pop(0))
    return picks


def predicted_o1_ready(row: ActivityMomentumCandidate, previous: PreviousSessionChain, session_date: date,
                       policy: Mapping[str, object] = SLOT_SELECTION_POLICY_V3) -> bool:
    """v3 (a): the 3x OI rule's pre-open inputs say this leg is likely to trade 3x its open interest today."""
    dte = row.dte_for(session_date)
    if not int(policy["o1_ready_minimum_dte"]) <= dte <= int(policy["o1_ready_maximum_dte"]) \
            or dte in _FLOW_EXCLUDED_DTES:
        return False
    open_interest = row.prior_open_interest
    if open_interest is None or open_interest < int(policy["o1_ready_minimum_prior_open_interest"]):
        return False
    ratio = Decimal(str(
        policy["o1_ready_short_dte_previous_volume_to_oi"] if dte <= int(policy["o1_ready_short_dte"])
        else policy["o1_ready_previous_volume_to_oi"]
    ))
    return int(previous.volumes.get(row.contract_ticker, 0)) >= ratio * open_interest


def vertical_partner(leg: ActivityMomentumCandidate, rows: Iterable[ActivityMomentumCandidate],
                     previous: PreviousSessionChain,
                     policy: Mapping[str, object] = SLOT_SELECTION_POLICY_V3) -> ActivityMomentumCandidate | None:
    """v3 (b): the nearest further-OTM leg of the same expiry and side among `rows` within the debit vertical's width
    band (of spot) and liquid on the pre-open inputs; None when there is none."""
    if leg.spot is None or leg.spot <= 0:
        return None
    call = leg.option_side is OptionSide.CALL
    further = sorted(
        (row for row in rows
         if row.contract_ticker != leg.contract_ticker and row.expiration == leg.expiration
         and row.option_side is leg.option_side and (row.strike > leg.strike if call else row.strike < leg.strike)),
        key=lambda row: (abs(row.strike - leg.strike), row.contract_ticker),
    )
    minimum = Decimal(str(policy["partner_minimum_width_fraction"]))
    maximum = Decimal(str(policy["partner_maximum_width_fraction"]))
    for row in further:
        width = abs(row.strike - leg.strike) / leg.spot
        if width < minimum:
            continue
        if width > maximum:
            break
        volume = int(previous.volumes.get(row.contract_ticker, 0))
        if (int(row.prior_open_interest or 0) >= int(policy["partner_minimum_prior_open_interest"])
                and volume >= int(policy["partner_minimum_previous_volume"])):
            return row
    return None


def protected_legs(today: Iterable[ActivityMomentumCandidate], *, target: int, previous: PreviousSessionChain,
                   session_date: date, policy: Mapping[str, object] = SLOT_SELECTION_POLICY_V3,
                   ) -> list[tuple[ActivityMomentumCandidate, str]]:
    """v3 (a) + (b) over today's picks of one name: (leg, O1_READY | PARTNER), O1-ready legs by the v2 score, each
    followed by its partner; at most the policy's share of `target`."""
    today = list(today)
    cap = int(Decimal(target) * Decimal(str(policy["protect_maximum_share_of_target"])))
    out: list[tuple[ActivityMomentumCandidate, str]] = []
    taken: set[str] = set()
    for leg in sorted((row for row in today if predicted_o1_ready(row, previous, session_date, policy)),
                      key=lambda row: _score(row, previous)):
        if len(out) >= cap:
            break
        if leg.contract_ticker not in taken:
            out.append((leg, "O1_READY"))
            taken.add(leg.contract_ticker)
        partner = vertical_partner(leg, today, previous, policy)
        if partner is not None and partner.contract_ticker not in taken and len(out) < cap:
            out.append((partner, "PARTNER"))
            taken.add(partner.contract_ticker)
    return out


@dataclass
class UnderlyingPlan:
    underlying: str
    product: str
    target: int
    off: tuple[str, ...]
    chosen: tuple[str, ...]
    reason: str | None = None
    topped_up: int = 0
    same_day: int = 0
    # v3 only: the protected legs (ticker, why) and the v2 rule's picks for the same name (None under v2).
    protected: tuple[tuple[str, str], ...] = ()
    v2_chosen: tuple[str, ...] | None = None

    @property
    def applied(self) -> bool:
        return self.reason is None


@dataclass
class SlotSelectionPlan:
    session_date: date
    side_balance: bool
    previous: PreviousSessionChain | None
    underlyings: dict[str, UnderlyingPlan] = field(default_factory=dict)
    policy: Mapping[str, object] = field(default_factory=lambda: SLOT_SELECTION_POLICY)

    @property
    def chosen(self) -> frozenset[str]:
        return frozenset(ticker for plan in self.underlyings.values() for ticker in plan.chosen)


def _selected_rows(allocation: ActivityMomentumAllocation) -> dict[str, ActivityMomentumCandidate]:
    tickers = set(allocation.selected_contract_tickers)
    return {
        row.contract_ticker: row
        for group in allocation.selected_groups
        for row in group.candidates
        if row.contract_ticker in tickers
    }


def plan_slot_selection(
    *,
    profile,
    candidates: Iterable[ActivityMomentumCandidate],
    resolution,
    off: ActivityMomentumAllocation,
    session_date: date,
    previous: PreviousSessionChain | None,
    side_balance: bool,
    policy: Mapping[str, object] | None = None,
) -> SlotSelectionPlan:
    """Rules 1-6 per underlying of today's allocation (v3: after the protected legs); a pure function of its inputs."""
    policy = parse_slot_selection_policy(SLOT_SELECTION_POLICY if policy is None else policy)
    v3 = policy is SLOT_SELECTION_POLICY_V3
    off_rows = _selected_rows(off)
    by_underlying: dict[str, list[ActivityMomentumCandidate]] = {}
    for row in candidates:
        by_underlying.setdefault(row.underlying, []).append(row)
    evidence = {row.underlying: row for row in getattr(resolution, "short_expiry_evidence", ()) or ()}
    admitted_same_day: dict[str, list[ActivityMomentumCandidate]] = {}
    for group in getattr(resolution, "groups", ()) or ():
        for row in group.candidates:
            if len(group.candidates) == 1 and row.dte_for(session_date) == 0:
                admitted_same_day.setdefault(row.underlying, []).append(row)
    plan = SlotSelectionPlan(session_date=session_date, side_balance=side_balance, previous=previous, policy=policy)
    for underlying in sorted({row.underlying for row in off_rows.values()}):
        today = sorted(
            (row for row in off_rows.values() if row.underlying == underlying),
            key=lambda row: row.contract_ticker,
        )
        product = v4_product_kind(evidence.get(underlying), session_date)
        entry = UnderlyingPlan(
            underlying=underlying,
            product=product,
            target=len(today),
            off=tuple(row.contract_ticker for row in today),
            chosen=tuple(row.contract_ticker for row in today),
        )
        plan.underlyings[underlying] = entry
        if previous is None:
            entry.reason = "PREVIOUS_CHAIN_UNAVAILABLE"
            continue
        entry.reason = previous.state(underlying)
        if entry.reason is not None:
            continue
        pool = sorted(
            (row for row in by_underlying.get(underlying, ())
             if slot_selection_eligible(row, profile, session_date)),
            key=lambda row: _score(row, previous),
        )
        if not any(previous.volumes.get(row.contract_ticker, 0) > 0 for row in pool):
            entry.reason = "PREVIOUS_VOLUME_EMPTY"
            continue

        def admit(picks, row):
            # The resolution's per-(criterion, side) shortlist bound (EXTENSION 8 a side) keeps every pick allocatable.
            criterion = _criterion(profile, row.dte_for(session_date))
            if criterion is None:
                return False
            used = sum(
                1 for pick in picks
                if pick.option_side is row.option_side
                and _criterion(profile, pick.dte_for(session_date)) is criterion
            )
            return used < criterion.shortlist_limit_per_side

        def rule_picks(start: list) -> tuple[list, int, int]:
            """Today's 0DTE count, then the pool, then today's picks as top-up, after `start` (v3's protected legs).
            Returns (picks, 0DTE picks, topped up)."""
            picks = list(start)
            same_day_count = topped_up = 0
            # Nov 18 (user): every product keeps today's 0DTE count (v1 kept 2 per index ETF, single stocks 0).
            quota = sum(1 for row in today if row.dte_for(session_date) == 0)
            if quota and product in policy["same_day_products"]:
                same_day = sorted(
                    {row.contract_ticker: row for row in admitted_same_day.get(underlying, ())}.values(),
                    key=lambda row: _score(row, previous),
                )
                # Calls and puts alternate among the 0DTE picks themselves (protected legs do not tilt them); the
                # shortlist bound still counts every pick so far.
                same_day_picks = _pick(same_day, min(quota, entry.target - len(picks)), [], balance=True,
                                       admit=lambda chosen, row: admit(list(start) + chosen, row))
                picks += same_day_picks
                same_day_count = len(same_day_picks)
            picks = _pick(pool, entry.target, picks, balance=side_balance, admit=admit)
            if len(picks) < entry.target:
                taken = {row.contract_ticker for row in picks}
                extra = sorted((row for row in today if row.contract_ticker not in taken),
                               key=lambda row: _score(row, previous))
                before = len(picks)
                picks = _pick(extra, entry.target, picks, balance=False, admit=admit)
                topped_up = len(picks) - before
            return picks, same_day_count, topped_up

        protected: list[tuple[ActivityMomentumCandidate, str]] = []
        if v3:
            v2_picks, _, _ = rule_picks([])
            entry.v2_chosen = tuple(sorted(row.contract_ticker for row in v2_picks[: entry.target]))
            if product in policy["protect_products"]:
                protected = protected_legs(today, target=entry.target, previous=previous, session_date=session_date,
                                           policy=policy)
            entry.protected = tuple((row.contract_ticker, why) for row, why in protected)
        picks, entry.same_day, entry.topped_up = rule_picks([row for row, _ in protected])
        entry.chosen = tuple(sorted(row.contract_ticker for row in picks[: entry.target]))
    return plan


def allocate_slot_selection(
    plan: SlotSelectionPlan,
    *,
    candidates: Iterable[ActivityMomentumCandidate],
    resolve: Callable[[tuple[ActivityMomentumCandidate, ...]], object],
    allocate: Callable[[object], ActivityMomentumAllocation],
) -> ActivityMomentumAllocation:
    """The V4 allocation over the chosen candidates only. A name the allocation does not keep in full falls back to
    today's picks (reason SELECTION_SHORTFALL) and the allocation runs once more; a second shortfall raises."""
    rows = tuple(candidates)
    for attempt in range(2):
        chosen = plan.chosen
        restricted = tuple(row for row in rows if row.contract_ticker in chosen)
        allocation = allocate(resolve(restricted))
        selected = set(allocation.selected_contract_tickers)
        short = [entry for entry in plan.underlyings.values() if set(entry.chosen) - selected]
        if not short and selected == set(chosen):
            return allocation
        if attempt:
            raise RuntimeError(
                "slot selection allocation shortfall: " + ",".join(sorted(entry.underlying for entry in short))
            )
        for entry in short:
            entry.reason = "SELECTION_SHORTFALL"
            entry.chosen = entry.off
            entry.topped_up = 0
            entry.same_day = 0
            entry.protected = ()
    raise AssertionError("unreachable")


# ------------------------------------------------------------------------------------------------- record
def _share(numerator, denominator) -> float | None:
    return None if not denominator else round(float(numerator) / float(denominator), 4)


def _dte_mix(rows, session_date) -> dict[str, int]:
    mix = Counter()
    for row in rows:
        dte = row.dte_for(session_date)
        mix["0" if dte == 0 else "1-2" if dte <= 2 else "3-4" if dte <= 4 else "5-21" if dte <= 21 else "22+"] += 1
    return {key: mix[key] for key in ("0", "1-2", "3-4", "5-21", "22+")}


def _sides(rows) -> dict[str, int]:
    counts = Counter(row.option_side for row in rows)
    return {"calls": counts[OptionSide.CALL], "puts": counts[OptionSide.PUT]}


def _coverage(rows, previous: PreviousSessionChain | None, underlying: str | None) -> dict[str, object]:
    if previous is None:
        return {"volume_share": None, "premium_share": None}
    names = {underlying} if underlying is not None else set(previous.total_volume)
    volume = sum(previous.volumes.get(row.contract_ticker, 0) for row in rows)
    premium = sum(
        (previous.mids.get(row.contract_ticker, Decimal(0)) * previous.volumes.get(row.contract_ticker, 0) * 100
         for row in rows),
        Decimal(0),
    )
    return {
        "volume_share": _share(volume, sum(previous.total_volume.get(name, 0) for name in names)),
        "premium_share": _share(premium, sum((previous.total_premium.get(name, Decimal(0)) for name in names),
                                             Decimal(0))),
    }


def _contract(row: ActivityMomentumCandidate, session_date, previous) -> dict[str, object]:
    return {
        "contract": row.contract_ticker,
        "side": row.option_side.value,
        "dte": row.dte_for(session_date),
        "abs_delta": None if row.delta is None else str(abs(row.delta).quantize(Decimal("0.001"))),
        "prior_oi": row.prior_open_interest,
        "previous_volume": None if previous is None else previous.volumes.get(row.contract_ticker),
    }


def slot_selection_documents(
    *,
    mode: SlotSelectionMode,
    plan: SlotSelectionPlan,
    candidates: Iterable[ActivityMomentumCandidate],
    off: ActivityMomentumAllocation,
    selected: ActivityMomentumAllocation,
) -> list[dict[str, object]]:
    """The summary document, then one per underlying: overlap, adds and drops (contract, side, DTE, |delta|, prior OI,
    previous volume), and the expected coverage on the previous session (share of that session's volume and premium,
    on contracts still listed, held by today's set and by the rule's set; in-sample for the rule, which ranks on it)."""
    by_ticker = {row.contract_ticker: row for row in candidates}
    session_date = plan.session_date
    previous = plan.previous
    policy = plan.policy
    v3 = policy is SLOT_SELECTION_POLICY_V3
    common = {
        "mode": mode.value,
        "applied": mode is SlotSelectionMode.ON,
        "session_date": str(session_date),
        "policy_version": policy["version"],
        "policy_sha256": SLOT_SELECTION_POLICY_V3_SHA256 if v3 else SLOT_SELECTION_POLICY_SHA256,
        "side_balance": plan.side_balance,
        "off_allocation_sha256": off.allocation_sha256,
        "selection_allocation_sha256": selected.allocation_sha256,
    }
    lines = []
    off_all, new_all = [], []
    fallbacks: dict[str, str] = {}
    totals = Counter()
    for underlying, entry in sorted(plan.underlyings.items()):
        off_rows = [by_ticker[ticker] for ticker in entry.off if ticker in by_ticker]
        new_rows = [by_ticker[ticker] for ticker in entry.chosen if ticker in by_ticker]
        off_all += off_rows
        new_all += new_rows
        added = sorted(set(entry.chosen) - set(entry.off))
        dropped = sorted(set(entry.off) - set(entry.chosen))
        if entry.reason is not None:
            fallbacks[underlying] = entry.reason
        totals["overlap"] += len(set(entry.chosen) & set(entry.off))
        totals["added"] += len(added)
        totals["dropped"] += len(dropped)
        totals["topped_up"] += entry.topped_up
        totals["same_day"] += entry.same_day
        extra: dict[str, object] = {}
        if v3:
            why = Counter(reason for _, reason in entry.protected)
            totals["protected_o1_ready"] += why["O1_READY"]
            totals["protected_partner"] += why["PARTNER"]
            v2_chosen = set(entry.chosen if entry.v2_chosen is None else entry.v2_chosen)
            extra = {
                "protected": [[ticker, reason] for ticker, reason in entry.protected],
                "v2_minus_v3": sorted(v2_chosen - set(entry.chosen)),
                "v3_minus_v2": sorted(set(entry.chosen) - v2_chosen),
            }
        lines.append({
            **common,
            "part": "underlying",
            "underlying": underlying,
            "product": entry.product,
            "rule_applied": entry.applied,
            "reason": entry.reason,
            "target": entry.target,
            "off_count": len(entry.off),
            "selection_count": len(entry.chosen),
            "overlap": len(set(entry.chosen) & set(entry.off)),
            "added_count": len(added),
            "dropped_count": len(dropped),
            "topped_up": entry.topped_up,
            "same_day": entry.same_day,
            "sides": {"off": _sides(off_rows), "selection": _sides(new_rows)},
            "dte_mix": {"off": _dte_mix(off_rows, session_date), "selection": _dte_mix(new_rows, session_date)},
            "previous_session_coverage": {
                "off": _coverage(off_rows, previous, underlying),
                "selection": _coverage(new_rows, previous, underlying),
            },
            "added": [_contract(by_ticker[ticker], session_date, previous)
                      for ticker in added[:_CONTRACT_LIST_LIMIT] if ticker in by_ticker],
            "dropped": [_contract(by_ticker[ticker], session_date, previous)
                        for ticker in dropped[:_CONTRACT_LIST_LIMIT] if ticker in by_ticker],
            **extra,
        })
    summary = {
        **common,
        "part": "summary",
        "lines": 1 + len(lines),
        "underlyings": len(plan.underlyings),
        "rule_applied_underlyings": sum(1 for entry in plan.underlyings.values() if entry.applied),
        "fallback_underlyings": fallbacks,
        "contracts": {"off": len(off_all), "selection": len(new_all)},
        "overlap": totals["overlap"],
        "added": totals["added"],
        "dropped": totals["dropped"],
        "topped_up": totals["topped_up"],
        "same_day": totals["same_day"],
        "sides": {"off": _sides(off_all), "selection": _sides(new_all)},
        "dte_mix": {"off": _dte_mix(off_all, session_date), "selection": _dte_mix(new_all, session_date)},
        "previous_session": None if previous is None else {
            "session_date": str(previous.session_date),
            "contracts": len(previous.volumes),
            "underlyings": len(previous.newest_observed_at),
            "oldest_newest_observed_at": None if not previous.newest_observed_at
            else str(min(previous.newest_observed_at.values())),
            "fresh_after": str(previous.fresh_after),
        },
        "previous_session_coverage": {
            "off": _coverage(off_all, previous, None),
            "selection": _coverage(new_all, previous, None),
        },
        "policy": dict(policy),
    }
    if v3:
        summary["protected"] = {"o1_ready": totals["protected_o1_ready"], "partner": totals["protected_partner"]}
    return [summary, *lines]


# ------------------------------------------------------------------------------------------------- switch
def allocate_with_slot_selection(
    *,
    mode: SlotSelectionMode | str,
    used: ActivityMomentumAllocation,
    profile,
    resolution,
    candidates: Iterable[ActivityMomentumCandidate],
    session_date: date,
    side_balance: bool,
    previous: Callable[[], PreviousSessionChain],
    resolve: Callable[[tuple[ActivityMomentumCandidate, ...]], object],
    allocate: Callable[[object], ActivityMomentumAllocation],
    emit: Callable[[str, Mapping[str, object]], None] = _print_line,
    policy: Mapping[str, object] | str | None = None,
) -> ActivityMomentumAllocation:
    """The lifecycle's allocation under the switch. `used` is today's (after AL8). OFF: `used`, nothing else (no read,
    no call). RECORD: `used`; the rule's allocation is computed and logged only. ON: the rule's allocation, or `used`
    when the rule fails (logged). Never raises once the mode is valid and the profile is v4. `policy`: v2 (None, the
    default) or v3 (`parse_slot_selection_policy`); an invalid one raises."""
    mode = parse_slot_selection_mode(mode)
    policy = parse_slot_selection_policy(policy)
    if mode is SlotSelectionMode.OFF:
        return used
    if profile.version != V4_ACTIVITY_MOMENTUM_PROFILE_VERSION:
        raise ValueError(f"{SLOT_SELECTION_SETTING} requires the v4 admission profile")
    rows = tuple(candidates)
    try:
        try:
            chain = previous()
        except Exception as error:  # noqa: BLE001 - a missing input keeps today's selection
            chain = None
            emit(SLOT_SELECTION_RECORD_FAILED_EVENT, {
                "mode": mode.value,
                "session_date": str(session_date),
                "stage": "PREVIOUS_CHAIN",
                "error": f"{type(error).__name__}: {error}"[:500],
            })
        plan = plan_slot_selection(
            profile=profile,
            candidates=rows,
            resolution=resolution,
            off=used,
            session_date=session_date,
            previous=chain,
            side_balance=side_balance,
            policy=policy,
        )
        selection = (
            used
            if not any(entry.applied for entry in plan.underlyings.values())
            else allocate_slot_selection(plan, candidates=rows, resolve=resolve, allocate=allocate)
        )
        for document in slot_selection_documents(mode=mode, plan=plan, candidates=rows, off=used,
                                                 selected=selection):
            emit(SLOT_SELECTION_RECORD_EVENT, document)
    except Exception as error:  # noqa: BLE001 - AL11 never fails the stage
        try:
            emit(SLOT_SELECTION_RECORD_FAILED_EVENT, {
                "mode": mode.value,
                "session_date": str(session_date),
                "stage": "SELECTION",
                "kept": "TODAY",
                "error": f"{type(error).__name__}: {error}"[:500],
            })
        except Exception:  # noqa: BLE001
            pass
        return used
    return selection if mode is SlotSelectionMode.ON else used


def log_slot_selection_setting(mode: SlotSelectionMode, policy: Mapping[str, object] | None = None) -> None:
    """One start line on the lifecycle service (the operator's check right after the deploy)."""
    try:
        policy = SLOT_SELECTION_POLICY if policy is None else policy
        _print_line(SLOT_SELECTION_SETTING_EVENT, {
            "mode": mode.value,
            "setting": SLOT_SELECTION_SETTING,
            "policy_version": policy["version"],
            **({} if policy is SLOT_SELECTION_POLICY else {"policy_setting": SLOT_SELECTION_POLICY_SETTING}),
        })
    except Exception:  # noqa: BLE001
        pass
