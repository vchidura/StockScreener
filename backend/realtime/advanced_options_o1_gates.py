"""O1 0DTE gate (G106 AL6) and O1 aggressor-side direction (G106 AL7) on the live Options window (Nov 17 2026).

Research (docs/g106/G106_DETECTOR_RESEARCH.md sections 3-4 and 7 P1-1 / P1-2, Nov 10-16, 5 sessions):
- 0DTE: 49 of the 94 replayed O1 alerts were 0DTE; they lost -54% mean / -90% median to the close (win 0.17) and lost on
  4 of 5 sessions, while the 35 alerts with DTE >= 1 gained +8% / +4% (win 0.64). All 13 entries under $0.25 with a
  spread of 15% or more were 0DTE. Near-spot 0DTE vol/OI is 13-78x every session, so O1's ratio says nothing there.
- Direction: O1's direction is the contract type (call +1, put -1), but O1's call events were net SOLD on Nov 10, 12 and
  16 (ask-side vs bid-side premium). Rule E1 (ratio >= 3, the floor, and the contract ask-dominant over its stored prints
  this session: ask premium >= 2x bid premium and >= $25k, HIGH/MEDIUM confidence) agreed with the day 0.41 vs 0.29 and
  improved the +60 minute option outcome; S1 (bid-dominant, read as "sold", direction = -type) agreed with the day 85% of
  the time (n=26). Sample: 5 sessions, one trend day; RECORD first.

The gates run in the O1 executor (`advanced_options_native_adapters.build_advanced_o1_executor`) after the AL4 liquidity
floor, in this order: floor, then the 0DTE gate, then the aggressor side. Each gate sees the finding the gates before it
left when they are ON (RECORD gates change nothing). An event the 0DTE gate drops is NOT_DETECTED there (reason
`O1ZD_EXPIRES_TODAY`, or `O1ZD_DTE_BELOW_POLICY` for 0 < DTE < a minimum above 1), so it never reaches the window's grouping
or the session alert, exactly like the floor (a later event on another contract still alerts; the same contract stays
dropped all session, its DTE does not change). The aggressor side never changes the detection: ON keeps a BOUGHT event's
alert (its direction = the contract type, now stated: call +1 / put -1) and makes a SOLD or UNCLEAR event an OBSERVATION
without a package (SOLD carries the implied direction -type as an observation; UNCLEAR none).

Switches on the Options window (`realtime-v3-options-p8-window`):

`REALTIME_V3_O1_ZERO_DTE` (policy `option_participation_min_dte_v1`, threshold `REALTIME_V3_O1_MIN_DTE`, default 1):
| OFF (default, unset) | today's O1, byte for byte: no extra work, no log line |
| RECORD | findings and selection unchanged; one `O1_ZERO_DTE_RECORD {json}` line per window: the detected events and the
             alerts the gate would drop (reason, DTE, contract, entry mark), counts, and what selection would pick instead |
| ON | every O1 evaluation carries the v3 finding (`option_participation_finding_v3`, the gate policy hash); events under
       the minimum DTE are NOT_DETECTED / NOT_SELECTED; the same line |

`REALTIME_V3_O1_AGGRESSOR_SIDE` (policy `option_participation_aggressor_side_v1`):
| OFF (default, unset) | today's O1, byte for byte: no extra read, no log line |
| RECORD | findings and selection unchanged; one extra read per window (the session's stored prints of the contracts whose
             event reached O1's ratio); one `O1_AGGRESSOR_RECORD {json}` line per detected event and one
             `O1_AGGRESSOR_WINDOW {json}` line per window |
| ON | v3 finding with the aggressor evidence; BOUGHT keeps the alert with direction = type; SOLD / UNCLEAR become
       OBSERVATION without a package. Implemented and tested; NOT to be switched on before RECORD evidence |

Aggressor classification (per detected event, from the contract's stored prints this session at or before the event:
`rt.option_flow_events`, the window's cohort / source flow / committed offset, INCLUDED, revision 1, as the research):
counted ask premium = AT_OR_ABOVE_ASK and counted bid premium = AT_OR_BELOW_BID, both at HIGH or MEDIUM confidence.
- BOUGHT: ask > bid, ask x 100 >= `REALTIME_V3_O1_AGGRESSOR_MIN_DOMINANCE_PCT` (default 200) x bid, and ask >=
  `REALTIME_V3_O1_AGGRESSOR_MIN_SIDE_PREMIUM` (dollars, default 25,000) - exactly research rule E1 (ask >= 2x bid and
  >= $25k; 200% dominance = an ask share of the classified premium >= 2/3);
- SOLD: the mirror (bid >= 2x ask and >= $25k: research S1);
- UNCLEAR: anything else (no stored print, no classified premium, no dominant side, dominant side under the minimum, or
  the prints could not be read).

Every value: unset or blank = the default (the policy and its hash are the built-in ones); a set value must be a
non-negative integer (digits only); the dominance must be >= 100. Anything else, or a switch other than OFF / RECORD / ON
(any case), and the window service refuses to start (`scripts/run_advanced_options_v3_production.py`). The values are
stamped into the policy documents, so the gate policy hash, the v3 episode ids and every v3 evaluation change with them.
They are read ONCE per process (cached; the window service reads them at start). Recording never fails the window; a gate
error never fails the window (RECORD keeps today's finding; ON leaves the event out as unavailable, fail closed).
"""
from __future__ import annotations

import json
import os
import sys
from bisect import bisect_right
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from functools import lru_cache
from typing import Annotated, Iterable, Literal, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import AwareDatetime, Field, model_validator

from equity.behavior import Contract, Name, Sha256
from options.dual_origin import ActivityFinding, ActivitySource, LiquidityFloorActivityFinding

from .advanced_options_o1_liquidity_floor import non_negative_int_setting

O1_ZERO_DTE_SETTING = "REALTIME_V3_O1_ZERO_DTE"
O1_MIN_DTE_SETTING = "REALTIME_V3_O1_MIN_DTE"
O1_AGGRESSOR_SIDE_SETTING = "REALTIME_V3_O1_AGGRESSOR_SIDE"
O1_AGGRESSOR_MIN_DOMINANCE_PCT_SETTING = "REALTIME_V3_O1_AGGRESSOR_MIN_DOMINANCE_PCT"
O1_AGGRESSOR_MIN_SIDE_PREMIUM_SETTING = "REALTIME_V3_O1_AGGRESSOR_MIN_SIDE_PREMIUM"
O1_ZERO_DTE_RECORD_EVENT = "O1_ZERO_DTE_RECORD"
O1_AGGRESSOR_RECORD_EVENT = "O1_AGGRESSOR_RECORD"
O1_AGGRESSOR_WINDOW_EVENT = "O1_AGGRESSOR_WINDOW"
# Docker splits a log line over 16 KiB: the lists are capped (window alerts first), the counts are always complete.
_RECORD_LIST_LIMIT = 30
_RECORD_EVENT_LIMIT = 40

O1ZD_EXPIRES_TODAY = "O1ZD_EXPIRES_TODAY"
O1ZD_DTE_BELOW_POLICY = "O1ZD_DTE_BELOW_POLICY"
O1AG_NO_STORED_PRINTS = "O1AG_NO_STORED_PRINTS"
O1AG_NO_CLASSIFIED_PREMIUM = "O1AG_NO_CLASSIFIED_PREMIUM"
O1AG_NO_DOMINANT_SIDE = "O1AG_NO_DOMINANT_SIDE"
O1AG_SIDE_PREMIUM_BELOW_POLICY = "O1AG_SIDE_PREMIUM_BELOW_POLICY"
O1AG_INPUTS_UNAVAILABLE = "O1AG_INPUTS_UNAVAILABLE"

ASK, BID, INSIDE, COMPLEX, STALE = (
    "AT_OR_ABOVE_ASK", "AT_OR_BELOW_BID", "INSIDE_SPREAD", "EXCLUDED_COMPLEX", "QUOTE_STALE")


class O1GateMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"
    ON = "ON"


def _mode(environ: Mapping[str, str] | None, key: str) -> O1GateMode:
    raw = (os.environ if environ is None else environ).get(key)
    value = (raw or "").strip().upper() or O1GateMode.OFF.value
    try:
        return O1GateMode(value)
    except ValueError:
        raise ValueError(f"{key} must be OFF, RECORD or ON, not {raw!r}") from None


def o1_zero_dte_mode(environ: Mapping[str, str] | None = None) -> O1GateMode:
    """`REALTIME_V3_O1_ZERO_DTE`; unset or blank is OFF. Anything but OFF, RECORD or ON raises (never silently OFF)."""
    return _mode(environ, O1_ZERO_DTE_SETTING)


def o1_aggressor_side_mode(environ: Mapping[str, str] | None = None) -> O1GateMode:
    """`REALTIME_V3_O1_AGGRESSOR_SIDE`; unset or blank is OFF. Anything but OFF, RECORD or ON raises."""
    return _mode(environ, O1_AGGRESSOR_SIDE_SETTING)


# --- policies -----------------------------------------------------------------------------------------------------------

class MinimumDtePolicy(Contract):
    """G106 AL6: an O1 detection needs at least `minimum_dte` calendar days from the volume session to expiration."""

    version: Literal["option_participation_min_dte_v1"] = "option_participation_min_dte_v1"
    minimum_dte: int = Field(default=1, ge=0, strict=True)
    dte_basis: Literal["CALENDAR_DAYS_EXPIRATION_MINUS_VOLUME_SESSION"] = "CALENDAR_DAYS_EXPIRATION_MINUS_VOLUME_SESSION"
    execution_permission: Literal[False] = False


class AggressorSidePolicy(Contract):
    """G106 AL7: O1's direction from the aggressor side of the contract's stored prints this session (research E1/S1)."""

    version: Literal["option_participation_aggressor_side_v1"] = "option_participation_aggressor_side_v1"
    minimum_dominance_pct: int = Field(default=200, ge=100, strict=True)
    minimum_side_premium: int = Field(default=25_000, ge=0, strict=True)
    counted_side_classes: tuple[Literal["AT_OR_ABOVE_ASK"], Literal["AT_OR_BELOW_BID"]] = (ASK, BID)
    counted_confidence: tuple[Literal["HIGH"], Literal["MEDIUM"]] = ("HIGH", "MEDIUM")
    print_basis: Literal["STORED_INCLUDED_REVISION_1_PRINTS_OF_THE_CONTRACT_THIS_SESSION_AT_OR_BEFORE_THE_EVENT"] = (
        "STORED_INCLUDED_REVISION_1_PRINTS_OF_THE_CONTRACT_THIS_SESSION_AT_OR_BEFORE_THE_EVENT")
    direction_basis: Literal["BOUGHT_IS_TYPE_SOLD_IS_OPPOSITE_OBSERVATION_UNCLEAR_IS_NONE"] = (
        "BOUGHT_IS_TYPE_SOLD_IS_OPPOSITE_OBSERVATION_UNCLEAR_IS_NONE")
    execution_permission: Literal[False] = False


MIN_DTE_POLICY = MinimumDtePolicy()
AGGRESSOR_SIDE_POLICY = AggressorSidePolicy()


class ActivityGatePolicy(Contract):
    """The policy a v3 finding names: the gates that are ON, over the policy of the finding they apply to (v1, or the
    liquidity floor's when it is ON). A gate that is not ON is None, so it is not part of the hash."""

    version: Literal["option_participation_origin_v3_gates"] = "option_participation_origin_v3_gates"
    base_policy_sha256: Sha256
    minimum_dte: MinimumDtePolicy | None = None
    aggressor_side: AggressorSidePolicy | None = None
    gate_order: Literal["LIQUIDITY_FLOOR_THEN_MINIMUM_DTE_THEN_AGGRESSOR_SIDE"] = (
        "LIQUIDITY_FLOOR_THEN_MINIMUM_DTE_THEN_AGGRESSOR_SIDE")
    execution_permission: Literal[False] = False

    @model_validator(mode="after")
    def validate_gates(self):
        if self.minimum_dte is None and self.aggressor_side is None:
            raise ValueError("a gate policy needs at least one gate")
        return self


def o1_min_dte_policy_from(environ: Mapping[str, str]) -> MinimumDtePolicy:
    value = non_negative_int_setting(environ, O1_MIN_DTE_SETTING, MIN_DTE_POLICY.minimum_dte)
    return MIN_DTE_POLICY if value == MIN_DTE_POLICY.minimum_dte else MinimumDtePolicy(minimum_dte=value)


def o1_aggressor_side_policy_from(environ: Mapping[str, str]) -> AggressorSidePolicy:
    default = AGGRESSOR_SIDE_POLICY
    dominance = non_negative_int_setting(
        environ, O1_AGGRESSOR_MIN_DOMINANCE_PCT_SETTING, default.minimum_dominance_pct)
    if dominance < 100:
        raise ValueError(f"{O1_AGGRESSOR_MIN_DOMINANCE_PCT_SETTING} must be at least 100, not {dominance}")
    premium = non_negative_int_setting(environ, O1_AGGRESSOR_MIN_SIDE_PREMIUM_SETTING, default.minimum_side_premium)
    if (dominance, premium) == (default.minimum_dominance_pct, default.minimum_side_premium):
        return default
    return AggressorSidePolicy(minimum_dominance_pct=dominance, minimum_side_premium=premium)


@lru_cache(maxsize=1)
def o1_min_dte_policy() -> MinimumDtePolicy:
    """The process's 0DTE gate policy, read once from the environment (the window service calls this at start)."""
    return o1_min_dte_policy_from(os.environ)


@lru_cache(maxsize=1)
def o1_aggressor_side_policy() -> AggressorSidePolicy:
    """The process's aggressor-side policy, read once from the environment (the window service calls this at start)."""
    return o1_aggressor_side_policy_from(os.environ)


# --- the 0DTE gate --------------------------------------------------------------------------------------------------------

def days_to_expiration(source: ActivitySource) -> int:
    return (source.expiration_date - source.volume_session).days


def minimum_dte_reasons(dte: int, policy: MinimumDtePolicy = MIN_DTE_POLICY) -> tuple[str, ...]:
    """Why a contract is under the minimum DTE; () when it is not."""
    if dte >= policy.minimum_dte:
        return ()
    return (O1ZD_EXPIRES_TODAY,) if dte <= 0 else (O1ZD_DTE_BELOW_POLICY,)


# --- the aggressor side ---------------------------------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class O1AggressorPrint:
    """One stored print (flow event) of a contract this session."""

    contract_ticker: str
    event_time: datetime
    event_id: str
    premium: Decimal
    side_class: str
    side_confidence: str


@dataclass(frozen=True, slots=True)
class O1AggressorTally:
    """The premium of a contract's stored prints this session at or before one O1 event, by aggressor bucket (the
    loader's read, attached to the O1 invocation; the executor classifies it under the process's policy).
    `inputs_reason` is set when the prints could not be read (then every amount is 0)."""

    prints: int = 0
    ask_premium: Decimal = Decimal(0)
    bid_premium: Decimal = Decimal(0)
    low_confidence_premium: Decimal = Decimal(0)
    inside_premium: Decimal = Decimal(0)
    complex_premium: Decimal = Decimal(0)
    stale_premium: Decimal = Decimal(0)
    other_premium: Decimal = Decimal(0)
    first_print_at: datetime | None = None
    last_print_at: datetime | None = None
    inputs_reason: str | None = None

    @property
    def total_premium(self) -> Decimal:
        return (self.ask_premium + self.bid_premium + self.low_confidence_premium + self.inside_premium
                + self.complex_premium + self.stale_premium + self.other_premium)

    def identity(self) -> dict:
        return {
            "prints": self.prints, "ask": str(self.ask_premium), "bid": str(self.bid_premium),
            "low": str(self.low_confidence_premium), "inside": str(self.inside_premium),
            "complex": str(self.complex_premium), "stale": str(self.stale_premium), "other": str(self.other_premium),
            "inputs_reason": self.inputs_reason,
        }


def o1_aggressor_tally(prints: Sequence[O1AggressorPrint], *, policy: AggressorSidePolicy = AGGRESSOR_SIDE_POLICY,
                       inputs_reason: str | None = None) -> O1AggressorTally:
    """The tally of `prints` (one contract's prints at or before the event). Counted ask / bid = the policy's classes at
    the policy's confidence; ask / bid at another confidence are `low_confidence_premium`."""
    if inputs_reason is not None:
        return O1AggressorTally(inputs_reason=inputs_reason)
    amounts = Counter()
    for row in prints:
        premium = Decimal(row.premium)
        if row.side_class in (ASK, BID):
            if row.side_confidence in policy.counted_confidence:
                amounts["ask" if row.side_class == ASK else "bid"] += premium
            else:
                amounts["low"] += premium
        elif row.side_class == INSIDE:
            amounts["inside"] += premium
        elif row.side_class == COMPLEX:
            amounts["complex"] += premium
        elif row.side_class == STALE:
            amounts["stale"] += premium
        else:
            amounts["other"] += premium
    times = [row.event_time for row in prints]
    return O1AggressorTally(
        prints=len(prints), ask_premium=Decimal(amounts["ask"]), bid_premium=Decimal(amounts["bid"]),
        low_confidence_premium=Decimal(amounts["low"]), inside_premium=Decimal(amounts["inside"]),
        complex_premium=Decimal(amounts["complex"]), stale_premium=Decimal(amounts["stale"]),
        other_premium=Decimal(amounts["other"]), first_print_at=min(times, default=None),
        last_print_at=max(times, default=None))


class O1AggressorPrints:
    """The session's stored prints by contract, for the per-event tallies (each event: the prints at or before it)."""

    def __init__(self, prints: Iterable[O1AggressorPrint] = (), inputs_reason: str | None = None):
        self.inputs_reason = inputs_reason
        self._by_contract: dict[str, list[O1AggressorPrint]] = {}
        for row in prints:
            self._by_contract.setdefault(row.contract_ticker, []).append(row)
        for rows in self._by_contract.values():
            rows.sort(key=lambda row: (row.event_time, row.event_id))
        self._times = {key: [row.event_time for row in rows] for key, rows in self._by_contract.items()}

    def tally(self, contract_ticker: str, at: datetime,
              policy: AggressorSidePolicy = AGGRESSOR_SIDE_POLICY) -> O1AggressorTally:
        if self.inputs_reason is not None:
            return o1_aggressor_tally((), policy=policy, inputs_reason=self.inputs_reason)
        rows = self._by_contract.get(contract_ticker, [])
        return o1_aggressor_tally(rows[:bisect_right(self._times.get(contract_ticker, []), at)], policy=policy)


def type_direction(contract_type: str) -> int:
    return 1 if contract_type == "CALL" else -1


def classify_aggressor(tally: O1AggressorTally, policy: AggressorSidePolicy) -> tuple[str, tuple[str, ...]]:
    """(BOUGHT / SOLD / UNCLEAR, reasons). BOUGHT = ask > bid, ask x 100 >= dominance x bid, ask >= the minimum (E1);
    SOLD = the mirror (S1); UNCLEAR otherwise, with why."""
    if tally.inputs_reason is not None:
        return "UNCLEAR", (O1AG_INPUTS_UNAVAILABLE,)
    if tally.prints == 0:
        return "UNCLEAR", (O1AG_NO_STORED_PRINTS,)
    ask, bid = tally.ask_premium, tally.bid_premium
    if ask + bid <= 0:
        return "UNCLEAR", (O1AG_NO_CLASSIFIED_PREMIUM,)
    for side, mine, other in (("BOUGHT", ask, bid), ("SOLD", bid, ask)):
        if mine > other and mine * 100 >= policy.minimum_dominance_pct * other:
            if mine >= policy.minimum_side_premium:
                return side, ()
            return "UNCLEAR", (O1AG_SIDE_PREMIUM_BELOW_POLICY,)
    return "UNCLEAR", (O1AG_NO_DOMINANT_SIDE,)


def _ratio(part: Decimal, whole: Decimal) -> Decimal | None:
    return None if whole <= 0 else (part / whole).quantize(Decimal("0.0001"))


class AggressorSideEvidence(Contract):
    """The aggressor side of one O1 event (stamped into the v3 finding when the gate is ON)."""

    schema_version: Literal["o1_aggressor_side_evidence_v1"] = "o1_aggressor_side_evidence_v1"
    contract_type: Literal["CALL", "PUT"]
    aggressor_class: Literal["BOUGHT", "SOLD", "UNCLEAR"]
    implied_direction: Literal[-1, 1] | None
    reasons: tuple[Name, ...]
    prints: int = Field(ge=0, strict=True)
    ask_premium: Decimal
    bid_premium: Decimal
    low_confidence_premium: Decimal
    inside_premium: Decimal
    complex_premium: Decimal
    stale_premium: Decimal
    other_premium: Decimal
    first_print_at: AwareDatetime | None
    last_print_at: AwareDatetime | None

    @model_validator(mode="after")
    def validate_evidence(self):
        expected = {"BOUGHT": type_direction(self.contract_type), "SOLD": -type_direction(self.contract_type),
                    "UNCLEAR": None}[self.aggressor_class]
        if self.implied_direction != expected:
            raise ValueError("aggressor direction does not match its class")
        if (self.aggressor_class == "UNCLEAR") != bool(self.reasons):
            raise ValueError("only an unclear aggressor side has reasons")
        return self

    def tally(self) -> O1AggressorTally:
        return O1AggressorTally(
            prints=self.prints, ask_premium=self.ask_premium, bid_premium=self.bid_premium,
            low_confidence_premium=self.low_confidence_premium, inside_premium=self.inside_premium,
            complex_premium=self.complex_premium, stale_premium=self.stale_premium, other_premium=self.other_premium,
            first_print_at=self.first_print_at, last_print_at=self.last_print_at,
            inputs_reason=O1AG_INPUTS_UNAVAILABLE if O1AG_INPUTS_UNAVAILABLE in self.reasons else None)


def aggressor_side_evidence(tally: O1AggressorTally, contract_type: str,
                            policy: AggressorSidePolicy = AGGRESSOR_SIDE_POLICY) -> AggressorSideEvidence:
    aggressor_class, reasons = classify_aggressor(tally, policy)
    implied = {"BOUGHT": type_direction(contract_type), "SOLD": -type_direction(contract_type)}.get(aggressor_class)
    return AggressorSideEvidence(
        contract_type=contract_type, aggressor_class=aggressor_class, implied_direction=implied, reasons=reasons,
        prints=tally.prints, ask_premium=tally.ask_premium, bid_premium=tally.bid_premium,
        low_confidence_premium=tally.low_confidence_premium, inside_premium=tally.inside_premium,
        complex_premium=tally.complex_premium, stale_premium=tally.stale_premium, other_premium=tally.other_premium,
        first_print_at=tally.first_print_at, last_print_at=tally.last_print_at)


# --- the v3 finding (ON) ----------------------------------------------------------------------------------------------------

BaseActivityFinding = Annotated[ActivityFinding | LiquidityFloorActivityFinding, Field(discriminator="schema_version")]


class GatedActivityFinding(ActivityFinding):
    """An O1 finding under the gates that are ON (0DTE gate, aggressor side), over the finding they apply to (v1, or v2
    with the liquidity floor ON), which it carries whole. Its policy is the gate policy those parts make."""

    schema_version: Literal["option_participation_finding_v3"] = "option_participation_finding_v3"
    detector_policy_sha256: Sha256
    base_finding: BaseActivityFinding
    contract_type: Literal["CALL", "PUT"]
    dte: int = Field(ge=0, strict=True)
    gate_minimum_dte: MinimumDtePolicy | None = None
    gate_aggressor_side: AggressorSidePolicy | None = None
    gate_reasons: tuple[Name, ...] = ()
    aggressor: AggressorSideEvidence | None = None
    # Set only when the aggressor gate is ON and the side is BOUGHT: the alert's direction (call +1, put -1).
    direction: Literal[-1, 1] | None = None

    def gate_policy(self) -> ActivityGatePolicy:
        return ActivityGatePolicy(base_policy_sha256=self.base_finding.detector_policy_sha256,
                                  minimum_dte=self.gate_minimum_dte, aggressor_side=self.gate_aggressor_side)

    def expected_policy_sha256(self) -> str:
        return self.gate_policy().sha256

    @property
    def permits_package(self) -> bool:
        """The aggressor gate (ON) keeps a package only on a BOUGHT side; without it, nothing changes."""
        return self.gate_aggressor_side is None or (
            self.aggressor is not None and self.aggressor.aggressor_class == "BOUGHT")

    @model_validator(mode="after")
    def validate_gates(self):
        base = self.base_finding
        for name in ("source_sha256", "decision_at", "market_cutoff", "valid_until", "volume_oi_ratio"):
            if getattr(self, name) != getattr(base, name):
                raise ValueError(f"gated finding changed its base {name}")
        dte_reasons = (
            minimum_dte_reasons(self.dte, self.gate_minimum_dte)
            if self.gate_minimum_dte is not None and base.disposition == "DETECTED" else ())
        if dte_reasons:
            if (self.disposition, self.reasons) != ("NOT_DETECTED", dte_reasons):
                raise ValueError("an event under the minimum DTE must be NOT_DETECTED with the gate's reasons")
        elif (self.disposition, self.reasons) != (base.disposition, base.reasons):
            raise ValueError("a gate that does not apply must keep the base disposition and reasons")
        if self.gate_aggressor_side is not None and self.disposition == "DETECTED":
            if self.aggressor is None or self.aggressor.contract_type != self.contract_type:
                raise ValueError("a detected event under the aggressor gate needs its aggressor evidence")
            aggressor_class, reasons = classify_aggressor(self.aggressor.tally(), self.gate_aggressor_side)
            if (aggressor_class, reasons) != (self.aggressor.aggressor_class, self.aggressor.reasons):
                raise ValueError("aggressor class does not match its premiums under the policy")
            direction = self.aggressor.implied_direction if aggressor_class == "BOUGHT" else None
            gate_reasons = dte_reasons + (() if aggressor_class == "BOUGHT" else (f"O1AG_{aggressor_class}",) + reasons)
        else:
            if self.aggressor is not None:
                raise ValueError("aggressor evidence only on a detected event under the aggressor gate")
            direction, gate_reasons = None, dte_reasons
        if self.direction != direction:
            raise ValueError("gated finding direction does not match its aggressor side")
        if self.gate_reasons != gate_reasons:
            raise ValueError("gate reasons do not match the gates")
        return self


def apply_o1_gates(
    source: ActivitySource,
    finding: ActivityFinding,
    *,
    minimum_dte: MinimumDtePolicy | None,
    aggressor_side: AggressorSidePolicy | None,
    tally: O1AggressorTally | None = None,
) -> GatedActivityFinding:
    """The v3 finding for `source` from the finding the gates apply to (v1, or v2 with the floor ON): the 0DTE gate when
    `minimum_dte` is given, then the aggressor side when `aggressor_side` is given (its `tally` required for a detected
    event). The episode is the gate policy's (per policy, as v1 and v2)."""
    if not isinstance(finding, ActivityFinding) or isinstance(finding, GatedActivityFinding):
        raise ValueError("the gates apply to a v1 or v2 O1 finding")
    if finding.source_sha256 != source.sha256:
        raise ValueError("the gates require the finding of this exact source")
    policy = ActivityGatePolicy(base_policy_sha256=finding.detector_policy_sha256, minimum_dte=minimum_dte,
                                aggressor_side=aggressor_side)
    dte = days_to_expiration(source)
    disposition, reasons = finding.disposition, finding.reasons
    dte_reasons = minimum_dte_reasons(dte, minimum_dte) if minimum_dte is not None and disposition == "DETECTED" else ()
    if dte_reasons:
        disposition, reasons = "NOT_DETECTED", dte_reasons
    aggressor, direction, gate_reasons = None, None, dte_reasons
    if aggressor_side is not None and disposition == "DETECTED":
        if tally is None:
            raise ValueError("a detected event under the aggressor gate needs its tally")
        aggressor = aggressor_side_evidence(tally, source.contract_type, aggressor_side)
        if aggressor.aggressor_class == "BOUGHT":
            direction = aggressor.implied_direction
        else:
            gate_reasons = gate_reasons + (f"O1AG_{aggressor.aggressor_class}",) + aggressor.reasons
    episode_id = uuid5(NAMESPACE_URL,
        f"option-participation:{policy.sha256}:{source.security_id}:{source.contract_id}:{source.volume_session}")
    return GatedActivityFinding(
        episode_id=episode_id, source_sha256=finding.source_sha256, detector_policy_sha256=policy.sha256,
        decision_at=finding.decision_at, market_cutoff=finding.market_cutoff, valid_until=finding.valid_until,
        volume_oi_ratio=finding.volume_oi_ratio, disposition=disposition, reasons=reasons, base_finding=finding,
        contract_type=source.contract_type, dte=dte, gate_minimum_dte=minimum_dte, gate_aggressor_side=aggressor_side,
        gate_reasons=gate_reasons, aggressor=aggressor, direction=direction)


# --- RECORD --------------------------------------------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class O1GateEntry:
    """One O1 event as the gates saw it: the disposition entering the gates (after the floor when it is ON), whether
    the floor would drop it (None: floor OFF), its DTE and 0DTE-gate reasons, and its aggressor evidence (only for an
    event detected after the 0DTE gate when that gate is ON; None when the aggressor gate is OFF)."""

    event_id: UUID
    underlyer: str
    contract_ticker: str
    contract_type: str
    expiration_date: date
    variant: str  # "" for a single contract; "O1_DEBIT_VERTICAL:<short contract>" for an AL2 vertical package
    observed_at: datetime
    disposition: str
    below_floor: bool | None
    has_package: bool
    open_interest: int | None
    day_volume: int | None
    volume_oi_ratio: Decimal | None
    entry_mark: Decimal | None
    dte: int
    dte_reasons: tuple[str, ...]
    zero_dte_applied: bool
    aggressor: AggressorSideEvidence | None = None

    @property
    def detected(self) -> bool:
        return self.disposition == "DETECTED"

    @property
    def after_zero_dte(self) -> str:
        """The disposition the aggressor gate sees (the 0DTE gate applied only when ON)."""
        return "NOT_DETECTED" if self.zero_dte_applied and self.detected and self.dte_reasons else self.disposition


def o1_gate_entry(invocation, finding, *, below_floor, minimum_dte: MinimumDtePolicy | None, zero_dte_applied: bool,
                  aggressor: AggressorSideEvidence | None = None) -> O1GateEntry:
    """`finding` = the finding entering the gates; `minimum_dte` = the 0DTE policy when that gate is RECORD or ON."""
    source, package = invocation.source, invocation.package
    variant, entry_mark = "", None
    if package is not None and len(package.legs) == 2:
        variant = f"O1_DEBIT_VERTICAL:{package.legs[1].contract_ticker}"
    if package is not None and package.legs:
        entry_mark = package.legs[0].entry_mark
    dte = days_to_expiration(source)
    return O1GateEntry(
        event_id=invocation.event_id, underlyer=source.underlyer, contract_ticker=source.contract_ticker,
        contract_type=source.contract_type, expiration_date=source.expiration_date, variant=variant,
        observed_at=finding.decision_at, disposition=finding.disposition, below_floor=below_floor,
        has_package=package is not None, open_interest=source.open_interest, day_volume=source.day_volume,
        volume_oi_ratio=finding.volume_oi_ratio, entry_mark=entry_mark, dte=dte,
        dte_reasons=minimum_dte_reasons(dte, minimum_dte) if minimum_dte is not None else (),
        zero_dte_applied=zero_dte_applied, aggressor=aggressor)


def _alerts(entries: Iterable[O1GateEntry]) -> dict[tuple[str, str], O1GateEntry]:
    """The window's alert per contract and package variant: its earliest selected event (the projection's grouping)."""
    alerts: dict[tuple[str, str], O1GateEntry] = {}
    for entry in sorted(entries, key=lambda item: (item.observed_at, str(item.event_id))):
        alerts.setdefault((entry.contract_ticker, entry.variant), entry)
    return alerts


def _instead(without: Mapping, with_: Mapping) -> list[dict]:
    rows = []
    for key in sorted(without):
        first, replacement = without[key], with_.get(key)
        if replacement is not None and replacement.event_id == first.event_id:
            continue
        rows.append({"contract": key[0], "variant": key[1] or None, "alert_event_id": str(first.event_id),
                     "instead_event_id": None if replacement is None else str(replacement.event_id)})
    return rows


def _selectable(entry: O1GateEntry, selection_enabled: bool) -> bool:
    return selection_enabled and entry.has_package


def o1_zero_dte_document(*, mode: O1GateMode, cutoff: datetime, entries: Sequence[O1GateEntry],
                         policy: MinimumDtePolicy, selection_enabled: bool = True) -> dict:
    """The one O1_ZERO_DTE_RECORD document of a window: the detected events (entering the gate) it would drop, the
    alerts it would drop, and what selection would pick instead. `below_floor` tells which of them the floor (RECORD)
    would drop anyway."""
    detected = [e for e in entries if e.detected]
    dropped = [e for e in detected if e.dte_reasons]
    selected = [e for e in detected if _selectable(e, selection_enabled)]
    kept = [e for e in selected if not e.dte_reasons]
    alerts_without, alerts_with = _alerts(selected), _alerts(kept)
    selected_ids = {e.event_id for e in selected}
    alert_ids = {e.event_id for e in alerts_without.values()}
    return {
        "contract": "o1-zero-dte-record-v1",
        "mode": mode.value,
        "policy": policy.version,
        "policy_sha256": policy.sha256,
        "minimum_dte": policy.minimum_dte,
        "cutoff": cutoff.isoformat(),
        "applied": mode is O1GateMode.ON,
        "o1_events": len(entries),
        "detected": len(detected),
        "would_drop": len(dropped),
        "would_drop_selected": sum(e.event_id in selected_ids for e in dropped),
        "would_drop_below_floor_too": sum(e.below_floor is True for e in dropped),
        "selected_without_gate": len(selected),
        "selected_with_gate": len(kept),
        "alerts_without_gate": len(alerts_without),
        "alerts_with_gate": len(alerts_with),
        "alerts_dropped": sum(key not in alerts_with for key in alerts_without),
        "alerts_refilled": sum(key in alerts_with and alerts_with[key].event_id != alerts_without[key].event_id
                               for key in alerts_without),
        "reasons": dict(sorted(Counter(r for e in dropped for r in e.dte_reasons).items())),
        "by_underlying": dict(sorted(Counter(e.underlyer for e in dropped).items())),
        "by_type": dict(sorted(Counter(e.contract_type for e in dropped).items())),
        "events_listed": min(len(dropped), _RECORD_EVENT_LIMIT),
        "events": [
            {
                "event_id": str(e.event_id), "contract_ticker": e.contract_ticker, "variant": e.variant or None,
                "dte": e.dte, "observed_at": e.observed_at.isoformat(), "reasons": list(e.dte_reasons),
                "open_interest": e.open_interest, "day_volume": e.day_volume,
                "volume_oi_ratio": None if e.volume_oi_ratio is None else str(round(e.volume_oi_ratio, 2)),
                "entry_mark": None if e.entry_mark is None else str(e.entry_mark),
                "without_gate": "SELECTED" if e.event_id in selected_ids else "OBSERVATION",
                "with_gate": "NOT_SELECTED", "window_alert": e.event_id in alert_ids, "below_floor": e.below_floor,
            }
            for e in sorted(dropped, key=lambda item: (item.event_id not in alert_ids, item.observed_at,
                                                       str(item.event_id)))
        ][:_RECORD_EVENT_LIMIT],
        "instead": _instead(alerts_without, alerts_with)[:_RECORD_LIST_LIMIT],
    }


def o1_aggressor_documents(*, mode: O1GateMode, cutoff: datetime, entries: Sequence[O1GateEntry],
                           policy: AggressorSidePolicy, selection_enabled: bool = True,
                           status: str = "OK") -> tuple[dict, list[dict]]:
    """(the O1_AGGRESSOR_WINDOW document, one O1_AGGRESSOR_RECORD document per event the aggressor gate sees as
    detected). Today's direction = the contract type (call +1, put -1); implied = BOUGHT +type, SOLD -type, UNCLEAR none.
    `alerts_all_gates` = the alerts if the floor (when evaluated), the 0DTE gate (when evaluated) and this gate were ON."""
    judged = [e for e in entries if e.after_zero_dte == "DETECTED" and e.aggressor is not None]
    selected = [e for e in judged if _selectable(e, selection_enabled)]
    kept = [e for e in selected if e.aggressor.aggressor_class == "BOUGHT"]
    all_gates = [e for e in kept if e.below_floor is not True and not e.dte_reasons]
    alerts_without, alerts_with = _alerts(selected), _alerts(kept)
    selected_ids = {e.event_id for e in selected}
    alert_ids = {e.event_id for e in alerts_without.values()}
    versus = Counter()
    for e in judged:
        implied = e.aggressor.implied_direction
        versus["none" if implied is None else "agrees" if implied == type_direction(e.contract_type) else "opposite"] += 1
    by_type = {
        kind: dict(sorted(Counter(e.aggressor.aggressor_class for e in judged if e.contract_type == kind).items()))
        for kind in ("CALL", "PUT")
    }
    latest: dict[str, O1GateEntry] = {}  # a tally is cumulative: each contract's latest judged event
    for e in sorted(judged, key=lambda item: (item.observed_at, str(item.event_id))):
        latest[e.contract_ticker] = e
    premium = {
        key: str(sum((getattr(e.aggressor, f"{key}_premium") for e in latest.values()), Decimal(0)))
        for key in ("ask", "bid", "inside", "complex", "stale", "low_confidence", "other")
    }
    window = {
        "contract": "o1-aggressor-window-v1",
        "mode": mode.value,
        "status": status,
        "policy": policy.version,
        "policy_sha256": policy.sha256,
        "minimum_dominance_pct": policy.minimum_dominance_pct,
        "minimum_side_premium": policy.minimum_side_premium,
        "cutoff": cutoff.isoformat(),
        "applied": mode is O1GateMode.ON,
        "o1_events": len(entries),
        "detected": len(judged),
        "by_class": dict(sorted(Counter(e.aggressor.aggressor_class for e in judged).items())),
        "by_type": by_type,
        "implied_vs_today": dict(sorted(versus.items())),
        "unclear_reasons": dict(sorted(Counter(
            r for e in judged for r in e.aggressor.reasons).items())),
        "selected_without_gate": len(selected),
        "selected_with_gate": len(kept),
        "alerts_without_gate": len(alerts_without),
        "alerts_with_gate": len(alerts_with),
        "alerts_all_gates": len(_alerts(all_gates)),
        "alerts_refilled": sum(key in alerts_with and alerts_with[key].event_id != alerts_without[key].event_id
                               for key in alerts_without),
        "contracts": len(latest),
        "premium_latest_per_contract": premium,
        "by_underlying": {
            name: dict(sorted(Counter(e.aggressor.aggressor_class for e in judged if e.underlyer == name).items()))
            for name in sorted({e.underlyer for e in judged})
        },
        "instead": _instead(alerts_without, alerts_with)[:_RECORD_LIST_LIMIT],
    }
    events = []
    for e in sorted(judged, key=lambda item: (item.observed_at, str(item.event_id))):
        a = e.aggressor
        classified = a.ask_premium + a.bid_premium
        total = a.tally().total_premium
        events.append({
            "contract": "o1-aggressor-record-v1",
            "mode": mode.value,
            "policy_sha256": policy.sha256,
            "cutoff": cutoff.isoformat(),
            "event_id": str(e.event_id),
            "underlyer": e.underlyer,
            "contract_ticker": e.contract_ticker,
            "variant": e.variant or None,
            "contract_type": e.contract_type,
            "dte": e.dte,
            "observed_at": e.observed_at.isoformat(),
            "today_direction": type_direction(e.contract_type),
            "aggressor_class": a.aggressor_class,
            "implied_direction": a.implied_direction,
            "reasons": list(a.reasons),
            "prints": a.prints,
            "ask_premium": str(a.ask_premium),
            "bid_premium": str(a.bid_premium),
            "classified_premium": str(classified),
            "inside_premium": str(a.inside_premium),
            "complex_premium": str(a.complex_premium),
            "stale_premium": str(a.stale_premium),
            "low_confidence_premium": str(a.low_confidence_premium),
            "other_premium": str(a.other_premium),
            "total_premium": str(total),
            "ask_share": None if (v := _ratio(a.ask_premium, classified)) is None else str(v),
            "bid_share": None if (v := _ratio(a.bid_premium, classified)) is None else str(v),
            "coverage_share": None if (v := _ratio(classified, total)) is None else str(v),
            "volume_oi_ratio": None if e.volume_oi_ratio is None else str(e.volume_oi_ratio),
            "open_interest": e.open_interest,
            "day_volume": e.day_volume,
            "entry_mark": None if e.entry_mark is None else str(e.entry_mark),
            "below_floor": e.below_floor,
            "zero_dte_would_drop": bool(e.dte_reasons),
            "without_gate": "SELECTED" if e.event_id in selected_ids else "OBSERVATION",
            "with_gate": ("SELECTED" if e.event_id in selected_ids and a.aggressor_class == "BOUGHT"
                          else "OBSERVATION"),
            "window_alert": e.event_id in alert_ids,
            "applied": mode is O1GateMode.ON,
        })
    return window, events


def _print_line(event: str, document: Mapping[str, object]) -> None:
    print(f"{event} {json.dumps(document, sort_keys=True, default=str)}", file=sys.stderr, flush=True)


def log_o1_gate_documents(*, zero_dte: Mapping[str, object] | None = None,
                          aggressor: tuple[Mapping[str, object], Sequence[Mapping[str, object]]] | None = None) -> None:
    if zero_dte is not None:
        _print_line(O1_ZERO_DTE_RECORD_EVENT, zero_dte)
    if aggressor is not None:
        window, events = aggressor
        for document in events:
            _print_line(O1_AGGRESSOR_RECORD_EVENT, document)
        _print_line(O1_AGGRESSOR_WINDOW_EVENT, window)


# --- the executor's use ---------------------------------------------------------------------------------------------------

@dataclass(slots=True)
class O1GateRun:
    """One window's gates in the O1 executor (built only when a gate is not OFF; OFF = no object, today's path)."""

    zero_dte_mode: O1GateMode
    aggressor_mode: O1GateMode
    minimum_dte: MinimumDtePolicy | None
    aggressor_side: AggressorSidePolicy | None
    entries: list[O1GateEntry] = field(default_factory=list)
    failures: Counter = field(default_factory=Counter)
    aggressor_status: str = "OK"

    @classmethod
    def for_window(cls) -> "O1GateRun | None":
        """None when both gates are OFF. An invalid switch or threshold raises (the window fails loudly; the window
        service refuses to start with one)."""
        zero_dte, aggressor = o1_zero_dte_mode(), o1_aggressor_side_mode()
        if zero_dte is O1GateMode.OFF and aggressor is O1GateMode.OFF:
            return None
        return cls(
            zero_dte, aggressor,
            None if zero_dte is O1GateMode.OFF else o1_min_dte_policy(),
            None if aggressor is O1GateMode.OFF else o1_aggressor_side_policy(),
        )

    @property
    def any_on(self) -> bool:
        return O1GateMode.ON in (self.zero_dte_mode, self.aggressor_mode)

    def apply(self, invocation, finding, below_floor: bool | None):
        """(the finding to use, whether the gates permit a package). RECORD gates leave `finding` as it is."""
        zero_dte_on = self.zero_dte_mode is O1GateMode.ON
        aggressor = None
        if self.aggressor_side is not None:
            dte_drop = (zero_dte_on and finding.disposition == "DETECTED"
                        and minimum_dte_reasons(days_to_expiration(invocation.source), self.minimum_dte))
            if finding.disposition == "DETECTED" and not dte_drop:
                tally = getattr(invocation, "aggressor", None)
                if tally is None:
                    tally = O1AggressorTally(inputs_reason="O1AG_TALLY_NOT_LOADED")
                if tally.inputs_reason is not None and self.aggressor_status == "OK":
                    self.aggressor_status = f"INPUTS_UNAVAILABLE:{tally.inputs_reason}"
                aggressor = aggressor_side_evidence(tally, invocation.source.contract_type, self.aggressor_side)
        self.entries.append(o1_gate_entry(invocation, finding, below_floor=below_floor, minimum_dte=self.minimum_dte,
                                          zero_dte_applied=zero_dte_on, aggressor=aggressor))
        if not self.any_on:
            return finding, True
        gated = apply_o1_gates(
            invocation.source, finding,
            minimum_dte=self.minimum_dte if zero_dte_on else None,
            aggressor_side=self.aggressor_side if self.aggressor_mode is O1GateMode.ON else None,
            tally=None if aggressor is None else aggressor.tally(),
        )
        return gated, gated.permits_package

    def log(self, cutoff: datetime, selection_enabled: bool) -> None:
        """The record lines. Never raises."""
        try:
            zero_dte = None
            if self.minimum_dte is not None:
                zero_dte = o1_zero_dte_document(mode=self.zero_dte_mode, cutoff=cutoff, entries=self.entries,
                                                policy=self.minimum_dte, selection_enabled=selection_enabled)
                if self.failures:
                    zero_dte["gate_failures"] = dict(sorted(self.failures.items()))
            aggressor = None
            if self.aggressor_side is not None:
                aggressor = o1_aggressor_documents(mode=self.aggressor_mode, cutoff=cutoff, entries=self.entries,
                                                   policy=self.aggressor_side, selection_enabled=selection_enabled,
                                                   status=self.aggressor_status)
                if self.failures:
                    aggressor[0]["gate_failures"] = dict(sorted(self.failures.items()))
            log_o1_gate_documents(zero_dte=zero_dte, aggressor=aggressor)
        except Exception as error:  # noqa: BLE001 - recording never fails the window
            reason = f"{type(error).__name__}: {error}"[:200]
            try:
                if self.minimum_dte is not None:
                    _print_line(O1_ZERO_DTE_RECORD_EVENT, {
                        "contract": "o1-zero-dte-record-v1", "mode": self.zero_dte_mode.value,
                        "status": "RECORD_FAILED", "reason": reason})
                if self.aggressor_side is not None:
                    _print_line(O1_AGGRESSOR_WINDOW_EVENT, {
                        "contract": "o1-aggressor-window-v1", "mode": self.aggressor_mode.value,
                        "status": "RECORD_FAILED", "reason": reason})
            except Exception:  # noqa: BLE001
                pass


__all__ = [
    "AGGRESSOR_SIDE_POLICY", "ActivityGatePolicy", "AggressorSideEvidence", "AggressorSidePolicy",
    "GatedActivityFinding", "MIN_DTE_POLICY", "MinimumDtePolicy", "O1AG_INPUTS_UNAVAILABLE",
    "O1AG_NO_CLASSIFIED_PREMIUM", "O1AG_NO_DOMINANT_SIDE", "O1AG_NO_STORED_PRINTS", "O1AG_SIDE_PREMIUM_BELOW_POLICY",
    "O1AggressorPrint", "O1AggressorPrints", "O1AggressorTally", "O1GateEntry", "O1GateMode", "O1GateRun",
    "O1ZD_DTE_BELOW_POLICY", "O1ZD_EXPIRES_TODAY", "O1_AGGRESSOR_MIN_DOMINANCE_PCT_SETTING",
    "O1_AGGRESSOR_MIN_SIDE_PREMIUM_SETTING", "O1_AGGRESSOR_RECORD_EVENT", "O1_AGGRESSOR_SIDE_SETTING",
    "O1_AGGRESSOR_WINDOW_EVENT", "O1_MIN_DTE_SETTING", "O1_ZERO_DTE_RECORD_EVENT", "O1_ZERO_DTE_SETTING",
    "aggressor_side_evidence", "apply_o1_gates", "classify_aggressor", "days_to_expiration", "log_o1_gate_documents",
    "minimum_dte_reasons", "o1_aggressor_documents", "o1_aggressor_side_mode", "o1_aggressor_side_policy",
    "o1_aggressor_side_policy_from", "o1_aggressor_tally", "o1_gate_entry", "o1_min_dte_policy",
    "o1_min_dte_policy_from", "o1_zero_dte_document", "o1_zero_dte_mode", "type_direction",
]
