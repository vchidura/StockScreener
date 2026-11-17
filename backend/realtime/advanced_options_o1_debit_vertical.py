"""O1 debit verticals for the live (advanced) Options window (G106 AL2, Nov 16 2026).

Today an O1 detection is packaged as one bought contract (`advanced_options_native_loader.options_alert_v1_event_package`:
BUY 1 at the event's NBBO midpoint, capital at risk = premium, exit at the session close). This module builds the
directional debit vertical for the same event:

- long leg = the O1 event's contract (BUY 1); short leg = same underlying, expiry and type, further out of the money
  (call: higher strike, put: lower strike), SELL 1, chosen among the window's watched contracts that are marked at the
  cutoff, nearest strike first;
- both legs priced at the window's cutoff ADVANCED_NBBO marks (`rt.option_mark_evidence`, the marks the window values
  packages with), so the two prices are from one snapshot; net debit = long mark - short mark > 0;
  capital at risk = net debit x multiplier; maximum profit = width - net debit; exit at the session close as today;
- limits from the legacy DIRECTIONAL_DEBIT_SPREAD block, versioned in `options/policies/o1_debit_vertical_v1.json`
  (width 0.5%-10% of spot, reward/risk >= 1, each leg open interest >= 250 and day volume >= 100, DTE <= 60; DTE 0
  is allowed because O1 exits at the session close, the legacy floor of 1 being for multi-day holds);
- a leg that is not marked at this cutoff (R34: no mark; R31: incomplete volume baseline, both in the window's
  `unmarked_instrument_ids`) rejects the vertical with a reason; the event then keeps today's single contract.

Switch `REALTIME_V3_O1_DEBIT_VERTICAL` on the Options window: OFF (default / unset: today's behaviour, byte for byte),
RECORD (build every vertical, log one `O1_DEBIT_VERTICAL_RECORD {json}` line per window, publish today's single-contract
package unchanged), ON (the O1 package is the debit vertical when one qualifies, otherwise today's single contract).
Any other value fails the window service at start and every window that reads it.

Leg liquidity thresholds (Nov 17): `REALTIME_V3_O1_DEBIT_VERTICAL_LEG_MIN_OPEN_INTEREST` and
`REALTIME_V3_O1_DEBIT_VERTICAL_LEG_MIN_DAY_VOLUME` on the same service override the policy file's
`minimum_leg_open_interest` (250) and `minimum_leg_day_volume` (100). Unset or blank = the file's document unchanged
(same policy hash); a set value must be a non-negative integer or the window service refuses to start; it is written
into the policy document before hashing, so the policy hash (in the record line and every vertical's candidate
identity) changes with it. Read once per process with the policy file (`o1_debit_vertical_policy`, cached).
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from .contracts import canonical_sha256

O1_DEBIT_VERTICAL_SETTING = "REALTIME_V3_O1_DEBIT_VERTICAL"
O1_DEBIT_VERTICAL_LEG_MIN_OPEN_INTEREST_SETTING = "REALTIME_V3_O1_DEBIT_VERTICAL_LEG_MIN_OPEN_INTEREST"
O1_DEBIT_VERTICAL_LEG_MIN_DAY_VOLUME_SETTING = "REALTIME_V3_O1_DEBIT_VERTICAL_LEG_MIN_DAY_VOLUME"
O1_DEBIT_VERTICAL_RECORD_EVENT = "O1_DEBIT_VERTICAL_RECORD"
O1_DEBIT_VERTICAL_POLICY_PATH = (
    Path(__file__).parents[1] / "options" / "policies" / "o1_debit_vertical_v1.json"
)
O1_DEBIT_VERTICAL_CANDIDATE_NAMESPACE = "stockscreener:options-alert-o1-debit-vertical-v1"
STRUCTURE_CALL = "CALL_DEBIT_VERTICAL"
STRUCTURE_PUT = "PUT_DEBIT_VERTICAL"
_RECORD_LIST_LIMIT = 60


class O1DebitVerticalMode(str, Enum):
    OFF = "OFF"
    RECORD = "RECORD"
    ON = "ON"


def o1_debit_vertical_mode(environ=None) -> O1DebitVerticalMode:
    """The switch value; unset or blank is OFF. Anything but OFF, RECORD or ON raises (fails loudly, never silently)."""
    raw = (os.environ if environ is None else environ).get(O1_DEBIT_VERTICAL_SETTING)
    value = (raw or "").strip().upper() or O1DebitVerticalMode.OFF.value
    try:
        return O1DebitVerticalMode(value)
    except ValueError:
        raise ValueError(
            f"{O1_DEBIT_VERTICAL_SETTING} must be OFF, RECORD or ON, not {raw!r}"
        ) from None


@dataclass(frozen=True, slots=True)
class O1DebitVerticalPolicy:
    version: str
    minimum_dte: int
    maximum_dte: int
    minimum_width_fraction: Decimal
    maximum_width_fraction: Decimal
    minimum_return_on_risk: Decimal
    minimum_leg_open_interest: int
    minimum_leg_day_volume: int
    policy_sha256: str

    @classmethod
    def from_document(cls, document: Mapping[str, object]) -> "O1DebitVerticalPolicy":
        policy = cls(
            version=str(document["version"]),
            minimum_dte=int(document["minimum_dte"]),
            maximum_dte=int(document["maximum_dte"]),
            minimum_width_fraction=Decimal(str(document["minimum_width_fraction"])),
            maximum_width_fraction=Decimal(str(document["maximum_width_fraction"])),
            minimum_return_on_risk=Decimal(str(document["minimum_return_on_risk"])),
            minimum_leg_open_interest=int(document["minimum_leg_open_interest"]),
            minimum_leg_day_volume=int(document["minimum_leg_day_volume"]),
            policy_sha256=canonical_sha256(dict(document)),
        )
        if (
            not policy.version.strip()
            or policy.minimum_dte < 0
            or policy.maximum_dte < policy.minimum_dte
            or not Decimal(0) < policy.minimum_width_fraction <= policy.maximum_width_fraction < Decimal(1)
            or policy.minimum_return_on_risk <= 0
            or policy.minimum_leg_open_interest < 0
            or policy.minimum_leg_day_volume < 0
        ):
            raise ValueError("O1 debit vertical policy is invalid")
        return policy


def o1_debit_vertical_policy_document(document: Mapping[str, object], environ: Mapping[str, str]) -> dict:
    """The policy document with the environment's leg thresholds written in. Unset = the document unchanged."""
    from .advanced_options_o1_liquidity_floor import non_negative_int_setting

    document = dict(document)
    for key, setting in (
        ("minimum_leg_open_interest", O1_DEBIT_VERTICAL_LEG_MIN_OPEN_INTEREST_SETTING),
        ("minimum_leg_day_volume", O1_DEBIT_VERTICAL_LEG_MIN_DAY_VOLUME_SETTING),
    ):
        value = non_negative_int_setting(environ, setting, int(document[key]))
        if value != document[key]:
            document[key] = value
    return document


def o1_debit_vertical_policy_from(environ: Mapping[str, str]) -> O1DebitVerticalPolicy:
    return O1DebitVerticalPolicy.from_document(o1_debit_vertical_policy_document(
        json.loads(O1_DEBIT_VERTICAL_POLICY_PATH.read_text(encoding="utf-8")), environ))


@lru_cache(maxsize=1)
def o1_debit_vertical_policy() -> O1DebitVerticalPolicy:
    """The process's policy: the file plus the environment's leg thresholds, read once (the window service calls this
    at start)."""
    return o1_debit_vertical_policy_from(os.environ)


@dataclass(frozen=True, slots=True)
class DebitVertical:
    """A priced debit vertical. Money is per share (the package's leg marks); totals multiply by the multiplier."""

    structure_type: str
    direction: int  # +1 bullish (call debit), -1 bearish (put debit)
    long: object  # WatchedContract (advanced_options_o3_inputs)
    short: object
    long_mark: object  # LegMark (advanced_options_o3_producer)
    short_mark: object
    net_debit: Decimal
    width: Decimal
    width_fraction: Decimal
    maximum_profit: Decimal
    breakeven: Decimal
    return_on_risk: Decimal
    spot: Decimal

    @property
    def multiplier(self) -> int:
        return int(self.long.multiplier)

    @property
    def capital_at_risk(self) -> Decimal:
        return self.net_debit * self.multiplier

    @property
    def quote_skew_ms(self) -> int:
        return int(abs((self.long_mark.market_time - self.short_mark.market_time).total_seconds()) * 1000)

    def record(self) -> dict:
        return {
            "structure": self.structure_type,
            "long": self.long.contract_ticker,
            "short": self.short.contract_ticker,
            "long_mark": str(self.long_mark.mark),
            "short_mark": str(self.short_mark.mark),
            "net_debit": str(self.net_debit),
            "width": str(self.width),
            "width_fraction": str(round(self.width_fraction, 5)),
            "max_profit": str(self.maximum_profit),
            "breakeven": str(self.breakeven),
            "reward_risk": str(round(self.return_on_risk, 4)),
            "capital_at_risk": str(self.capital_at_risk),
            "spot": str(self.spot),
            "quote_skew_ms": self.quote_skew_ms,
        }


def usable_mark(contract, marks: Mapping[int, object], unmarked: frozenset[int], cutoff: datetime):
    """The leg's cutoff mark, or None when the window did not mark it (R34 / R31) or the mark is unusable."""
    if contract.instrument_id in unmarked:
        return None
    mark = marks.get(contract.instrument_id)
    if mark is None or mark.market_time > cutoff:
        return None
    if not all(value.is_finite() for value in (mark.bid, mark.ask, mark.mark)):
        return None
    if mark.bid <= 0 or mark.ask < mark.bid or mark.mark <= 0:
        return None
    return mark


def _liquid(contract, quotes: Mapping[int, object], policy: O1DebitVerticalPolicy) -> bool:
    quote = quotes.get(contract.instrument_id)
    return (
        quote is not None
        and quote.open_interest is not None
        and quote.day_volume is not None
        and quote.open_interest >= policy.minimum_leg_open_interest
        and quote.day_volume >= policy.minimum_leg_day_volume
    )


def _price(long, short, long_mark, quotes, marks, unmarked, *, spot, cutoff, policy) -> DebitVertical | str:
    short_mark = usable_mark(short, marks, unmarked, cutoff)
    if short_mark is None:
        return "O1DV_SHORT_LEG_UNMARKED"
    if not _liquid(short, quotes, policy):
        return "O1DV_SHORT_LEG_LIQUIDITY_BELOW_POLICY"
    net_debit = long_mark.mark - short_mark.mark
    if net_debit <= 0:
        return "O1DV_NON_POSITIVE_DEBIT"
    width = abs(short.strike - long.strike)
    maximum_profit = width - net_debit
    if maximum_profit <= 0:
        return "O1DV_DEBIT_NOT_BELOW_WIDTH"
    return_on_risk = maximum_profit / net_debit
    if return_on_risk < policy.minimum_return_on_risk:
        return "O1DV_REWARD_RISK_BELOW_POLICY"
    call = long.call_put == "C"
    return DebitVertical(
        structure_type=STRUCTURE_CALL if call else STRUCTURE_PUT,
        direction=1 if call else -1,
        long=long,
        short=short,
        long_mark=long_mark,
        short_mark=short_mark,
        net_debit=net_debit,
        width=width,
        width_fraction=width / spot,
        maximum_profit=maximum_profit,
        breakeven=long.strike + net_debit if call else long.strike - net_debit,
        return_on_risk=return_on_risk,
        spot=spot,
    )


def select_debit_vertical(
    long,
    contracts: Iterable[object],
    marks: Mapping[int, object],
    quotes: Mapping[int, object],
    *,
    unmarked: Iterable[int],
    spot: Decimal | None,
    session_date: date,
    cutoff: datetime,
    policy: O1DebitVerticalPolicy,
) -> DebitVertical | str:
    """The debit vertical for one O1 event's contract, or the stable reason it is not built (never raises on data).

    Short legs are tried nearest strike first; strikes closer than the minimum width are skipped, the search stops at
    the maximum width. When no short leg qualifies the reason is the nearest width-eligible strike's reason."""
    if long is None:
        return "O1DV_LONG_LEG_NOT_WATCHED"
    unmarked = frozenset(int(value) for value in unmarked)
    dte = (long.expiration - session_date).days
    if not policy.minimum_dte <= dte <= policy.maximum_dte:
        return "O1DV_DTE_OUTSIDE_POLICY"
    long_mark = usable_mark(long, marks, unmarked, cutoff)
    if long_mark is None:
        return "O1DV_LONG_LEG_UNMARKED"
    if not _liquid(long, quotes, policy):
        return "O1DV_LONG_LEG_LIQUIDITY_BELOW_POLICY"
    if spot is None or not spot.is_finite() or spot <= 0:
        return "O1DV_SPOT_UNAVAILABLE"
    call = long.call_put == "C"
    further = sorted(
        (
            row for row in contracts
            if row.underlying == long.underlying
            and row.expiration == long.expiration
            and row.call_put == long.call_put
            and row.multiplier == long.multiplier
            and row.instrument_id != long.instrument_id
            and (row.strike > long.strike if call else row.strike < long.strike)
        ),
        key=lambda row: (abs(row.strike - long.strike), row.contract_ticker),
    )
    if not further:
        return "O1DV_NO_FURTHER_OTM_WATCHED_CONTRACT"
    first_reason = None
    for short in further:
        fraction = abs(short.strike - long.strike) / spot
        if fraction < policy.minimum_width_fraction:
            continue
        if fraction > policy.maximum_width_fraction:
            break
        priced = _price(long, short, long_mark, quotes, marks, unmarked, spot=spot, cutoff=cutoff, policy=policy)
        if isinstance(priced, str):
            first_reason = first_reason or priced
            continue
        return priced
    return first_reason or "O1DV_NO_SHORT_WITHIN_WIDTH_POLICY"


def o1_debit_vertical_candidate_id(cohort_id, event_id) -> UUID:
    return uuid5(NAMESPACE_URL, f"{O1_DEBIT_VERTICAL_CANDIDATE_NAMESPACE}:{cohort_id}:{event_id}")


def is_o1_debit_vertical_package(package) -> bool:
    """An O1 debit vertical is recognised by its candidate identity (like the v1 single contract)."""
    legs = getattr(package, "legs", ()) or ()
    candidate_id = getattr(package, "candidate_id", None)
    return (
        len(legs) == 2
        and candidate_id is not None
        and candidate_id == o1_debit_vertical_candidate_id(
            getattr(package, "cohort_id", None), getattr(package, "event_id", None),
        )
    )


def debit_vertical_structure(package) -> tuple[str, int] | None:
    """(structure type, direction) of an O1 debit vertical package from its BUY leg; None for any other package."""
    if not is_o1_debit_vertical_package(package):
        return None
    bought = next((leg for leg in package.legs if getattr(leg.side, "value", leg.side) == "BUY"), None)
    if bought is None:
        return None
    call = getattr(bought.contract_type, "value", bought.contract_type) == "CALL"
    return (STRUCTURE_CALL if call else STRUCTURE_PUT), (1 if call else -1)


def o1_debit_vertical_package(
    vertical: DebitVertical,
    *,
    event_id: UUID,
    cohort_id: UUID,
    matrix_id: UUID,
    session_date: date,
    cutoff: datetime,
    policy: O1DebitVerticalPolicy,
    valuation_policy_sha256: str,
):
    """The two-leg package: leg 0 BUY the event's contract, leg 1 SELL the short leg, each at its cutoff mark."""
    from options.calendar import OptionExchangeCalendar
    from options.domain import ContractType
    from options.strategies.domain import OptionSide

    from .option_portal_adapter import RealtimeOptionPackage, RealtimeOptionPackageLeg
    from .preflight import ProviderTier

    ordered = ((0, vertical.long, vertical.long_mark, OptionSide.BUY), (1, vertical.short, vertical.short_mark, OptionSide.SELL))
    entry_time = max(vertical.long_mark.market_time, vertical.short_mark.market_time)
    identity = {
        "policy": policy.version,
        "policy_sha256": policy.policy_sha256,
        "cohort_id": str(cohort_id),
        "event_id": str(event_id),
        "cutoff": cutoff.isoformat(),
        "structure": vertical.structure_type,
        "legs": [
            {
                "side": side.value,
                "contract_id": int(contract.contract_id),
                "catalog_version_id": int(contract.catalog_version_id),
                "mark": str(mark.mark),
                "mark_market_time": mark.market_time.isoformat(),
                "mark_evidence_id": str(mark.evidence_id),
            }
            for _, contract, mark, side in ordered
        ],
    }
    multiplier = Decimal(vertical.multiplier)
    # Package premium convention (options/outcomes.py `_package_premium`): SELL +, BUY -, so a debit is negative.
    entry_net_premium = sum(
        (Decimal(1) if side is OptionSide.SELL else Decimal(-1)) * mark.mark * 1 * multiplier
        for _, _contract, mark, side in ordered
    )
    close = OptionExchangeCalendar().session_close(session_date)
    return RealtimeOptionPackage(
        candidate_id=o1_debit_vertical_candidate_id(cohort_id, event_id),
        event_id=event_id,
        matrix_id=matrix_id,
        candidate_identity_sha256=canonical_sha256(identity),
        cohort_id=cohort_id,
        provider_tier=ProviderTier.MASSIVE_ADVANCED,
        entry_market_time=entry_time,
        entry_net_premium=entry_net_premium,
        capital_at_risk=vertical.capital_at_risk,
        exit_deadline=close if close > entry_time else None,
        legs=tuple(
            RealtimeOptionPackageLeg(
                leg_index=index,
                instrument_id=int(contract.instrument_id),
                contract_id=int(contract.contract_id),
                catalog_version_id=int(contract.catalog_version_id),
                contract_ticker=str(contract.contract_ticker),
                side=side,
                ratio=1,
                multiplier=int(contract.multiplier),
                strike=contract.strike,
                expiration_date=contract.expiration,
                contract_type=ContractType.CALL if contract.call_put == "C" else ContractType.PUT,
                entry_mark=mark.mark,
                entry_market_time=mark.market_time,
                valuation_policy_sha256=valuation_policy_sha256,
            )
            for index, contract, mark, side in ordered
        ),
    )


@dataclass(frozen=True, slots=True)
class O1DebitVerticalOutcome:
    """What the vertical rule decided for one O1 event in one window; `used` = the invocation's package is it."""

    contract_ticker: str
    vertical: DebitVertical | None = None
    reason: str | None = None
    used: bool = False

    def __post_init__(self) -> None:
        if (self.vertical is None) == (self.reason is None):
            raise ValueError("O1 debit vertical outcome needs exactly one of vertical or reason")
        if self.used and self.vertical is None:
            raise ValueError("a rejected O1 debit vertical cannot be used")


@dataclass(frozen=True, slots=True)
class O1VerticalInputs:
    """Everything the rule reads for one window: watched contracts by ticker, cutoff marks, leg activity, spots."""

    contracts: tuple
    by_ticker: Mapping[str, object]
    marks: Mapping[int, object]
    quotes: Mapping[int, object]
    spots: Mapping[str, Decimal]


def _positive(value) -> Decimal | None:
    if value is None:
        return None
    try:
        value = Decimal(str(value))
    except ArithmeticError:
        return None
    return value if value.is_finite() and value > 0 else None


def spots_from_rolling_state(rolling_state) -> dict[str, Decimal]:
    """The underlying's price in the window's rolling state: the V3 snapshot tick's price (`UnderlyingSnapshotTick`,
    the live window's ticks), else the older tick's last trade, else its quote midpoint."""
    spots = {}
    for tick in getattr(rolling_state, "underlying_ticks", ()) or ():
        price = _positive(getattr(getattr(tick, "evidence", None), "price", None))
        trade = getattr(tick, "trade", None)
        quote = getattr(tick, "quote", None)
        if price is not None:
            pass
        elif trade is not None and _positive(trade.price) is not None:
            price = trade.price
        elif (
            quote is not None and quote.bid is not None and quote.ask is not None
            and quote.bid > 0 and quote.ask >= quote.bid
        ):
            price = (quote.bid + quote.ask) / 2
        if price is not None:
            spots[str(tick.ticker).strip().upper()] = Decimal(price)
    return spots


def load_o1_vertical_inputs_cursor(cursor, plan) -> O1VerticalInputs:
    """Read-only: the window's watched contracts, their cutoff marks and activity, and the underlyings' spots.

    Reuses the O3 producer's readers (same SQL O3 runs at every cutoff); nothing here changes O3."""
    from .advanced_options_o3_producer import (
        leg_quotes_from_plan,
        load_leg_marks_cursor,
        load_watched_contracts_cursor,
    )

    contracts = load_watched_contracts_cursor(cursor, plan)
    unmarked = set(int(value) for value in plan.unmarked_instrument_ids)
    # Marked at this cutoff = one of the window's selected contracts the window did not leave unmarked.
    selected = {int(row.instrument_id) for row in plan.enrollment.activity_selection.contracts}
    marked_ids = [
        row.instrument_id for row in contracts
        if row.instrument_id in selected and row.instrument_id not in unmarked
    ]
    marks = load_leg_marks_cursor(cursor, marked_ids, plan.cutoff) if marked_ids else {}
    quotes = leg_quotes_from_plan(plan, contracts, marks)
    return O1VerticalInputs(
        contracts=contracts,
        by_ticker={row.contract_ticker: row for row in contracts},
        marks=marks,
        quotes=quotes,
        spots=spots_from_rolling_state(plan.rolling_state),
    )


def load_o1_vertical_inputs_guarded(cursor, plan) -> tuple[O1VerticalInputs | None, str | None]:
    """`load_o1_vertical_inputs_cursor` behind a savepoint: a failed read leaves the vertical rejected for this window
    (every event keeps today's single contract) and the loader's transaction usable."""
    try:
        cursor.execute("SAVEPOINT o1_debit_vertical_inputs")
    except Exception as error:  # noqa: BLE001 - no savepoint, no extra read: the window goes on as today
        return None, f"O1DV_INPUTS_UNAVAILABLE:{type(error).__name__}"
    try:
        inputs = load_o1_vertical_inputs_cursor(cursor, plan)
    except Exception as error:  # noqa: BLE001
        cursor.execute("ROLLBACK TO SAVEPOINT o1_debit_vertical_inputs")
        return None, f"O1DV_INPUTS_UNAVAILABLE:{type(error).__name__}"
    cursor.execute("RELEASE SAVEPOINT o1_debit_vertical_inputs")
    return inputs, None


def debit_vertical_for_event(row: Mapping[str, object], inputs: O1VerticalInputs | None, plan, *,
                             policy: O1DebitVerticalPolicy, inputs_reason: str | None = None) -> DebitVertical | str:
    """The vertical for one O1 flow-event row of `_load_o1`, or its reason. Never raises on market data."""
    if inputs is None:
        return inputs_reason or "O1DV_INPUTS_UNAVAILABLE"
    try:
        long = inputs.by_ticker.get(str(row["contract_ticker"]).strip().upper())
        return select_debit_vertical(
            long,
            inputs.contracts,
            inputs.marks,
            inputs.quotes,
            unmarked=plan.unmarked_instrument_ids,
            spot=None if long is None else inputs.spots.get(long.underlying),
            session_date=plan.control_plane.session_date,
            cutoff=plan.cutoff,
            policy=policy,
        )
    except (ValueError, TypeError, ArithmeticError, KeyError, AttributeError) as error:
        return f"O1DV_BUILD_FAILED:{type(error).__name__}"


def o1_debit_vertical_record(
    *,
    mode: O1DebitVerticalMode,
    cutoff: datetime,
    entries: Sequence[tuple[UUID, str, O1DebitVerticalOutcome | None]],
    policy: O1DebitVerticalPolicy | None = None,
) -> dict:
    """One window's RECORD document. `entries`: (event id, O1 disposition, outcome) per evaluated O1 event.

    Events on one contract share the cutoff marks, so the lists group by contract (and short leg / reason)."""
    policy = policy or o1_debit_vertical_policy()
    built: dict[tuple, dict] = {}
    rejected: dict[tuple, dict] = {}
    reasons: Counter[str] = Counter()
    detected_reasons: Counter[str] = Counter()
    counts = Counter()
    for event_id, disposition, outcome in entries:
        counts["o1_events"] += 1
        detected = disposition == "DETECTED"
        counts["detected"] += int(detected)
        if outcome is None:
            counts["not_built_stored_package"] += 1
            continue
        if outcome.vertical is not None:
            counts["built"] += 1
            counts["built_detected"] += int(detected)
            counts["used"] += int(outcome.used and detected)
            key = (outcome.contract_ticker, outcome.vertical.short.contract_ticker)
            item = built.setdefault(key, {
                **outcome.vertical.record(), "events": 0, "detected": 0, "first_event_id": str(event_id),
                "used": False,
            })
        else:
            reasons[outcome.reason] += 1
            detected_reasons[outcome.reason] += int(detected)
            key = (outcome.contract_ticker, outcome.reason)
            item = rejected.setdefault(key, {
                "long": outcome.contract_ticker, "reason": outcome.reason, "events": 0, "detected": 0,
                "first_event_id": str(event_id),
            })
        item["events"] += 1
        item["detected"] += int(detected)
        if outcome.vertical is not None and outcome.used and detected:
            item["used"] = True
    return {
        "contract": "o1-debit-vertical-record-v1",
        "mode": mode.value,
        "policy": policy.version,
        "policy_sha256": policy.policy_sha256,
        "minimum_leg_open_interest": policy.minimum_leg_open_interest,
        "minimum_leg_day_volume": policy.minimum_leg_day_volume,
        "cutoff": cutoff.isoformat(),
        "o1_events": counts["o1_events"],
        "detected": counts["detected"],
        "built": counts["built"],
        "built_detected": counts["built_detected"],
        "used": counts["used"],
        "stored_package_reused": counts["not_built_stored_package"],
        "rejected": dict(sorted(reasons.items())),
        "rejected_detected": dict(sorted((k, v) for k, v in detected_reasons.items() if v)),
        "verticals": [built[key] for key in sorted(built)][:_RECORD_LIST_LIMIT],
        "rejections": [rejected[key] for key in sorted(rejected)][:_RECORD_LIST_LIMIT],
    }


def log_o1_debit_vertical_record(document: Mapping[str, object]) -> None:
    print(
        f"{O1_DEBIT_VERTICAL_RECORD_EVENT} {json.dumps(document, sort_keys=True, default=str)}",
        file=sys.stderr,
        flush=True,
    )


__all__ = [
    "DebitVertical", "O1DebitVerticalMode", "O1DebitVerticalOutcome", "O1DebitVerticalPolicy", "O1VerticalInputs",
    "O1_DEBIT_VERTICAL_LEG_MIN_DAY_VOLUME_SETTING", "O1_DEBIT_VERTICAL_LEG_MIN_OPEN_INTEREST_SETTING",
    "O1_DEBIT_VERTICAL_RECORD_EVENT", "O1_DEBIT_VERTICAL_SETTING", "o1_debit_vertical_policy_document",
    "o1_debit_vertical_policy_from", "debit_vertical_for_event",
    "debit_vertical_structure", "is_o1_debit_vertical_package", "load_o1_vertical_inputs_cursor",
    "log_o1_debit_vertical_record", "o1_debit_vertical_candidate_id", "o1_debit_vertical_mode",
    "o1_debit_vertical_package", "o1_debit_vertical_policy", "o1_debit_vertical_record", "select_debit_vertical",
    "spots_from_rolling_state", "usable_mark",
]
