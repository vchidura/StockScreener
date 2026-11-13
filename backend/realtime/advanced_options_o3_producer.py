"""O3 input producer, night 2 (matrix R44; design `docs/g102/G102_O3_INPUTS_DESIGN.md` steps 3-5 and section 8).

Night 1 (`advanced_options_o3_inputs`) builds the credit verticals O3 may judge from the window's watched contracts. This
module turns each one into exactly what the O3 lane reads (`advanced_options_native_loader._load_o3`) and what
`options/credit_detection.assess_o3_credit` accepts, the way the developer path does it
(`option_package_context.load_forward_technical_package_context`):

- legs: the cutoff NBBO midpoint of each leg (`rt.option_mark_evidence`, ADVANCED_NBBO) as the model mark, with IV and
  Greeks solved at the mark's time by the solver OA1 uses (`solve_local_greeks`), the stock's last 1-minute close at the
  cutoff as spot, and the session reference state's rate, dividend yield and model version per expiry;
- the candidate: SPREAD_RANGE_LOCATOR, PUT_CREDIT_VERTICAL (bullish) or CALL_CREDIT_VERTICAL (bearish), payoff from the
  strategy engine's own `evaluate_terminal_payoff`, identity from `candidate_identity`, policy `strategy_o3_credit_v2`;
- the decision: `assess_options_credit_canonical_trend` on the O1 activity source of a leg (the window's own O1 flow
  events) and the canonical 30m trend evidence built from stored 30m and daily bars (`build_canonical_trend_evidence`);
- the invalidation: 30m structural levels (`materialize_equity_evidence` -> `bind_direct_structural_levels`);
- persistence: the two-leg package, the `ADVANCED_OPTIONS_O3_INPUT` detector version, one CONFIRMED event per candidate
  at `trigger_at = cutoff` (event id and dedup derived from cohort, candidate and cutoff, so a window retry writes
  nothing new) and its `O3_NATIVE_INVOCATION` input with the retained invocation document.

O3 itself is run on every record before it is stored (`assess_o3_credit`), so the lane never meets an input it refuses
(the O3 executor does not catch per-invocation errors). Behind `REALTIME_V3_O3_INPUTS` (off by default); the producer
never fails the window: any error leaves O3 BLOCKED as today, with the reason in the log line.
"""
from __future__ import annotations

import json
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Callable, Mapping, Sequence
from uuid import NAMESPACE_URL, UUID, uuid5

from .advanced_options_o3_inputs import (
    O3_INPUTS_CONTRACT,
    CreditVertical,
    LegQuote,
    O3InputPolicy,
    WatchedContract,
    o3_inputs_enabled,
    select_o3_candidates,
)
from .contracts import canonical_sha256

O3_INPUT_DETECTOR_ID = "ADVANCED_OPTIONS_O3_INPUT"
O3_INPUT_DETECTOR_VERSION = "o3-inputs-r44-v1"
O3_INPUT_PAYLOAD_SCHEMA = "advanced_options_o3_input_v1"
O3_INPUT_PRODUCER_CONTRACT = "advanced-options-o3-producer-v1"
STRATEGY_POLICY_PATH = Path(__file__).parents[1] / "options" / "policies" / "strategy_o3_credit_v2.json"
VALUATION_POLICY_PATH = Path(__file__).parents[1] / "options" / "policies" / "valuation_advanced_nbbo_v1.json"
UTC = timezone.utc


@lru_cache(maxsize=1)
def o3_strategy_policy():
    from options.config import load_strategy_policy

    return load_strategy_policy(STRATEGY_POLICY_PATH)


@lru_cache(maxsize=1)
def o3_valuation_policy():
    from options.config import load_valuation_policy

    return load_valuation_policy(VALUATION_POLICY_PATH)


def o3_input_policy() -> O3InputPolicy:
    raw = json.loads(STRATEGY_POLICY_PATH.read_text(encoding="utf-8"))
    return O3InputPolicy.from_strategy_document(raw)


@dataclass(frozen=True, slots=True)
class LegMark:
    """The leg's ADVANCED_NBBO mark evidence at or before the cutoff (the row the O3 marks gate reads)."""

    evidence_id: UUID
    instrument_id: int
    bid: Decimal
    ask: Decimal
    mark: Decimal
    market_time: datetime
    observed_time: datetime
    payload_sha256: str


@dataclass(frozen=True, slots=True)
class ValuationInput:
    risk_free_rate: Decimal
    dividend_yield: Decimal
    model_version: str


@dataclass(frozen=True, slots=True)
class LegValuation:
    time_to_expiration_years: float
    risk_free_rate: float
    dividend_yield: float
    model_version: str
    local_iv: float
    local_delta: float
    local_gamma: float
    local_theta_per_day: float
    local_vega_per_vol_point: float
    local_rho_per_rate_point: float
    iv_solver: str
    iv_iteration_count: int
    iv_price_error: float | None


def value_leg(
    contract: WatchedContract,
    mark: Decimal,
    *,
    spot: Decimal,
    at: datetime,
    valuation: ValuationInput,
) -> LegValuation | str:
    """IV and Greeks of one leg at its mark's time (OA1's solver and maturity convention), or a stable reason code."""
    from options.analytics.greeks import OptionValuationInput, solve_local_greeks
    from options.domain import ContractType

    from .option_alert_valuation import time_to_expiration_years

    years = time_to_expiration_years(contract.expiration, at)
    if years is None or years <= 0:
        return "O3_LEG_EXPIRED_AT_MARK"
    (result,) = solve_local_greeks((OptionValuationInput(
        contract_type=ContractType.CALL if contract.call_put == "C" else ContractType.PUT,
        spot=spot, strike=contract.strike, model_mark=mark, time_to_expiration_years=years,
        risk_free_rate=float(valuation.risk_free_rate), dividend_yield=float(valuation.dividend_yield),
    ),))
    greeks = (result.local_iv, result.local_delta, result.local_gamma, result.local_theta_per_day,
              result.local_vega_per_vol_point, result.local_rho_per_rate_point)
    if not result.converged or any(value is None for value in greeks) or result.solver is None:
        reason = result.failure_reason.value if result.failure_reason is not None else "NOT_CONVERGED"
        return f"O3_LEG_IV_UNAVAILABLE:{reason}"
    return LegValuation(
        time_to_expiration_years=years, risk_free_rate=float(valuation.risk_free_rate),
        dividend_yield=float(valuation.dividend_yield), model_version=valuation.model_version,
        local_iv=float(result.local_iv), local_delta=float(result.local_delta), local_gamma=float(result.local_gamma),
        local_theta_per_day=float(result.local_theta_per_day),
        local_vega_per_vol_point=float(result.local_vega_per_vol_point),
        local_rho_per_rate_point=float(result.local_rho_per_rate_point),
        iv_solver=getattr(result.solver, "value", str(result.solver)), iv_iteration_count=int(result.iteration_count),
        iv_price_error=None if result.price_error is None else float(result.price_error),
    )


@dataclass(frozen=True, slots=True)
class O3Parts:
    candidate: object  # OptionCandidate
    snapshots: tuple  # OptionContractSnapshot per leg, in leg order
    package: object  # RealtimeOptionPackage
    event_id: UUID


def o3_event_id(cohort_id: UUID, candidate_id: UUID, cutoff: datetime) -> UUID:
    return uuid5(NAMESPACE_URL, f"stockscreener:{O3_INPUT_DETECTOR_ID}:{cohort_id}:{candidate_id}:{cutoff.isoformat()}")


def build_o3_parts(
    vertical: CreditVertical,
    *,
    cohort_id: UUID,
    matrix_id: UUID,
    cutoff: datetime,
    decision_at: datetime,
    spot: Decimal,
    spot_time: datetime,
    marks: Mapping[int, LegMark],
    quotes: Mapping[int, LegQuote],
    valuations: Mapping[int, LegValuation],
    session_close: datetime,
    batch_id: UUID,
) -> O3Parts:
    """The candidate (legs: 0 = SELL short anchor, 1 = BUY wing), each leg's snapshot and the two-leg package."""
    from options.calendar import OptionExchangeCalendar
    from options.domain import ContractType, ExerciseStyle, MarkSource, OptionContractSnapshot
    from options.strategies.domain import (
        CandidateKind, CandidateLeg, CandidateStatus, OptionCandidate, OptionSide, StructureRiskClass, StructureType,
        candidate_identity,
    )
    from options.strategies.payoff import evaluate_terminal_payoff
    from options.strategies.registry import REGISTRY_BY_NAME

    from .option_portal_adapter import RealtimeOptionPackage, RealtimeOptionPackageLeg
    from .preflight import ProviderTier

    strategy = o3_strategy_policy()
    valuation_policy = o3_valuation_policy()
    registration = REGISTRY_BY_NAME["SPREAD_RANGE_LOCATOR"]
    structure = StructureType(vertical.structure_type)
    ordered = ((0, vertical.short, OptionSide.SELL), (1, vertical.long, OptionSide.BUY))
    candidate_id, identity = candidate_identity(
        matrix_id, registration.strategy_name, strategy.policy.strategy_version, structure,
        tuple(contract.contract_id for _, contract, _ in ordered), "credit_to_width",
    )
    calendar = OptionExchangeCalendar()
    legs, snapshots = [], []
    for index, contract, side in ordered:
        mark, quote, valued = marks[contract.instrument_id], quotes[contract.instrument_id], valuations[contract.instrument_id]
        kind = ContractType.CALL if contract.call_put == "C" else ContractType.PUT
        snapshot_id = uuid5(candidate_id, f"o3-leg-snapshot:{index}")
        legs.append(CandidateLeg(
            leg_index=index, snapshot_id=snapshot_id, contract_id=contract.contract_id,
            contract_ticker=contract.contract_ticker, side=side, ratio=1, multiplier=contract.multiplier,
            expiration_date=contract.expiration, strike=contract.strike, contract_type=kind, spot=spot,
            time_to_expiration_years=valued.time_to_expiration_years, risk_free_rate=valued.risk_free_rate,
            dividend_yield=valued.dividend_yield, model_mark=mark.mark, local_iv=valued.local_iv,
            local_delta=valued.local_delta, local_gamma=valued.local_gamma,
            local_theta_per_day=valued.local_theta_per_day, local_vega_per_vol_point=valued.local_vega_per_vol_point,
            local_rho_per_rate_point=valued.local_rho_per_rate_point, source_market_time=mark.market_time,
            mark_source=MarkSource.ADVANCED_NBBO_MIDPOINT.value, model_version=valued.model_version, quality_flags=(),
            valuation_policy_version=valuation_policy.policy_version,
            valuation_policy_sha256=valuation_policy.policy_sha256,
        ))
        intrinsic = max(spot - contract.strike, Decimal(0)) if kind is ContractType.CALL else max(contract.strike - spot, Decimal(0))
        record = {
            "contract": O3_INPUTS_CONTRACT, "evidence_id": str(mark.evidence_id), "contract_id": contract.contract_id,
            "mark": str(mark.mark), "bid": str(mark.bid), "ask": str(mark.ask), "market_time": mark.market_time.isoformat(),
            "spot": str(spot), "spot_time": spot_time.isoformat(), "day_volume": quote.day_volume,
            "open_interest": quote.open_interest,
        }
        snapshots.append(OptionContractSnapshot(
            snapshot_id=snapshot_id, contract_id=contract.contract_id, contract_ticker=contract.contract_ticker,
            underlyer=contract.underlying, provider="MASSIVE_ADVANCED", contract_type=kind,
            expiration_date=contract.expiration, expiration_cutoff=calendar.session_close(contract.expiration),
            calendar_dte=(contract.expiration - cutoff.date()).days,
            time_to_expiration_years=valued.time_to_expiration_years, strike=contract.strike,
            shares_per_contract=contract.multiplier, exercise_style=ExerciseStyle.AMERICAN, spot=spot,
            spot_market_data_time=spot_time, bid=mark.bid, ask=mark.ask, midpoint=mark.mark, display_mark=mark.mark,
            model_mark=mark.mark, mark_market_data_time=mark.market_time,
            mark_source=MarkSource.ADVANCED_NBBO_MIDPOINT, day_volume=quote.day_volume,
            open_interest=quote.open_interest, market_data_time=mark.market_time,
            first_observed_at=max(mark.observed_time, mark.market_time, spot_time),
            revised_observed_at=None, local_iv=valued.local_iv, local_gamma=valued.local_gamma,
            local_delta=valued.local_delta, local_theta_per_day=valued.local_theta_per_day,
            local_vega_per_vol_point=valued.local_vega_per_vol_point,
            local_rho_per_rate_point=valued.local_rho_per_rate_point, intrinsic_value=intrinsic,
            extrinsic_value=mark.mark - intrinsic, single_contract_breakeven=None, provider_iv=None, provider_gamma=None,
            risk_free_rate=valued.risk_free_rate, dividend_yield=valued.dividend_yield, iv_converged=True,
            iv_solver=valued.iv_solver, iv_iteration_count=valued.iv_iteration_count,
            iv_price_error=valued.iv_price_error, iv_failure_reason=None, model_version=valued.model_version,
            quality_flags=(), batch_id=batch_id, raw_payload_sha256=mark.payload_sha256,
            normalized_payload_sha256=canonical_sha256(record),
            valuation_policy_version=valuation_policy.policy_version,
            valuation_policy_sha256=valuation_policy.policy_sha256,
        ))
    legs = tuple(legs)
    payoff = evaluate_terminal_payoff(legs)
    source_time = min(leg.source_market_time for leg in legs)
    market_time = max(leg.source_market_time for leg in legs)
    credit = strategy.policy.credit
    valid_until = min(source_time + timedelta(seconds=int(credit.maximum_source_age_seconds)), session_close)
    if valid_until <= market_time:
        valid_until = market_time + timedelta(seconds=1)
    return_on_risk = (
        float(payoff.maximum_profit / payoff.maximum_loss)
        if payoff.maximum_profit is not None and payoff.maximum_loss else None
    )
    candidate = OptionCandidate(
        candidate_id=candidate_id, identity_sha256=identity, matrix_id=matrix_id,
        strategy_name=registration.strategy_name, strategy_version=strategy.policy.strategy_version,
        underlyer=vertical.underlying, candidate_kind=CandidateKind.MULTI_LEG,
        strategy_archetype=registration.strategy_archetype, persona_tags=registration.persona_tags,
        structure_type=structure,
        structure_risk_class=(StructureRiskClass.DEFINED_RISK_CREDIT if payoff.net_premium > 0
                              else StructureRiskClass.PREMIUM_AT_RISK_DEBIT),
        expiration_date=vertical.expiration, rank=vertical.rank, status=CandidateStatus.SELECTED,
        primary_metric_name="credit_to_width", primary_metric_value=float(vertical.credit_to_width),
        rank_components={
            "credit_to_width": float(vertical.credit_to_width), "return_on_risk": return_on_risk,
            "minimum_leg_open_interest": min(quotes[leg.instrument_id].open_interest or 0 for leg in (vertical.short, vertical.long)),
            "minimum_leg_volume": min(quotes[leg.instrument_id].day_volume or 0 for leg in (vertical.short, vertical.long)),
            "ordered_contract_ids": [vertical.short.contract_id, vertical.long.contract_id],
        },
        primary_evidence={
            "producer": O3_INPUT_PRODUCER_CONTRACT, "vertical_identity_sha256": vertical.identity_sha256,
            "mark_evidence_ids": [str(marks[leg.instrument_id].evidence_id) for leg in (vertical.short, vertical.long)],
            "spot_time": spot_time.isoformat(),
        },
        legs=legs, net_premium=payoff.net_premium, collateral_required=None, capital_at_risk=payoff.maximum_loss,
        maximum_profit=payoff.maximum_profit, maximum_loss=payoff.maximum_loss, return_on_collateral=None,
        return_on_risk=return_on_risk, breakevens=payoff.breakevens, execution_eligibility=None, reason_codes=(),
        management_policy_version=None, management_policy={}, policy_sha256=strategy.sha256,
        model_version=legs[0].model_version, context_snapshot_id=None, iv_context_id=None,
        market_data_time=market_time, observed_time=max(decision_at, market_time), valid_until=valid_until,
    )
    event_id = o3_event_id(cohort_id, candidate_id, cutoff)
    package = RealtimeOptionPackage(
        candidate_id=candidate_id, event_id=event_id, matrix_id=matrix_id, candidate_identity_sha256=identity,
        cohort_id=cohort_id, provider_tier=ProviderTier.MASSIVE_ADVANCED, entry_market_time=market_time,
        # Package premium convention (options/outcomes.py `_package_premium`): SELL +, BUY -, so a credit is positive.
        entry_net_premium=payoff.net_premium, capital_at_risk=payoff.maximum_loss,
        exit_deadline=session_close if session_close > market_time else None,
        legs=tuple(RealtimeOptionPackageLeg(
            leg_index=index, instrument_id=contract.instrument_id, contract_id=contract.contract_id,
            catalog_version_id=contract.catalog_version_id, contract_ticker=contract.contract_ticker, side=side,
            ratio=1, multiplier=contract.multiplier, strike=contract.strike, expiration_date=contract.expiration,
            contract_type=ContractType.CALL if contract.call_put == "C" else ContractType.PUT,
            entry_mark=marks[contract.instrument_id].mark,
            entry_market_time=marks[contract.instrument_id].market_time,
            valuation_policy_sha256=_session_valuation_policy_sha256(),
        ) for index, contract, side in ordered),
    )
    return O3Parts(candidate, tuple(snapshots), package, event_id)


def _session_valuation_policy_sha256() -> str:
    from .advanced_options_native_loader import session_valuation_policy_sha256

    return session_valuation_policy_sha256()


@dataclass(frozen=True, slots=True)
class O3InputRecord:
    """One CONFIRMED O3 input: the parts, the decision, the invalidation and the retained invocation document."""

    parts: O3Parts
    decision: object
    activity_contract_id: int
    activity_instrument_id: int
    activity_mark: LegMark
    structural_invalidation: Decimal
    document: dict


def _error_reason(error: BaseException) -> str:
    """A short stable reason: the validation message of a pydantic error, else the error's last meaningful line."""
    errors = getattr(error, "errors", None)
    if callable(errors):
        try:
            first = errors()[0]
            return str(first.get("msg") or type(error).__name__)[:90]
        except Exception:  # noqa: BLE001 - fall back to the text
            pass
    lines = [line.strip() for line in str(error).splitlines() if line.strip() and "errors.pydantic.dev" not in line]
    return (lines[-1] if lines else type(error).__name__)[:90]


@dataclass(frozen=True, slots=True)
class O3UnderlyingOutcome:
    """Per underlying and cutoff: what the producer considered and why nothing qualified (the NO_INPUT record)."""

    underlying: str
    considered: int
    confirmed: int
    rejections: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class O3InputBuild:
    records: tuple[O3InputRecord, ...]
    rejections: tuple[tuple[str, int], ...]
    candidates_considered: int
    outcomes: tuple[O3UnderlyingOutcome, ...] = ()
    stock_instruments: tuple[tuple[str, int], ...] = ()


def assemble_o3_inputs(
    *,
    contracts: Sequence[WatchedContract],
    quotes: Mapping[int, LegQuote],
    marks: Mapping[int, LegMark],
    activity_sources: Sequence[object],
    spot_by_underlying: Mapping[str, tuple[Decimal, datetime]],
    valuation_inputs: Mapping[tuple[str, date], ValuationInput],
    stock_evidence: Callable[[str, datetime | None], object | None],
    structural_levels: Callable[[str, object], tuple[dict, object] | None],
    cohort_id: UUID,
    matrix_id: UUID,
    session_date: date,
    cutoff: datetime,
    decision_at: datetime,
    session_close: datetime,
    batch_id: UUID,
    policy: O3InputPolicy | None = None,
) -> O3InputBuild:
    """Design steps 1-5 without the database: every credit vertical that O3 accepts at this cutoff, or why not.

    `activity_sources` are the window's O1 `ActivitySource`s; a vertical needs one on a leg (the developer path's
    "dated activity source"), the one with the highest day volume / open interest. `stock_evidence(underlyer,
    required_window)` returns the canonical 30m trend evidence; `structural_levels(underlyer, stock)` returns the
    developer path's levels dict and the security for `bind_direct_structural_levels`."""
    from options.credit_detection import assess_o3_credit
    from options.detector_collection import bind_direct_structural_levels
    from options.dual_origin import assess_options_credit_canonical_trend
    from options.intraday_participation import latest_completed_30m_window

    from .advanced_options_native_adapters import AdvancedO3Invocation, advanced_o3_invocation_document

    policy = policy or o3_input_policy()
    rejections: Counter[str] = Counter()
    by_underlying: dict[str, Counter] = {}
    considered: Counter[str] = Counter()
    confirmed: Counter[str] = Counter()
    spots_only = {key: value[0] for key, value in spot_by_underlying.items()}
    candidates = []
    for underlying in sorted({row.underlying for row in contracts}):
        selection = select_o3_candidates(
            tuple(row for row in contracts if row.underlying == underlying), quotes, session_date=session_date,
            cutoff=cutoff, spot_by_underlying=spots_only, policy=policy,
        )
        by_underlying[underlying] = Counter(dict(selection.rejections))
        if underlying not in spots_only:
            by_underlying[underlying]["O3_SPOT_UNAVAILABLE"] += 1
        considered[underlying] += len(selection.candidates)
        candidates.extend(selection.candidates)

    def reject(underlying, reason):
        by_underlying.setdefault(underlying, Counter())[reason] += 1

    maximum_source_age = int(o3_strategy_policy().policy.credit.maximum_source_age_seconds)
    sources_by_contract: dict[int, list] = {}
    for source in activity_sources:
        sources_by_contract.setdefault(int(source.contract_id), []).append(source)
    stock_cache: dict[tuple[str, datetime | None], object | None] = {}
    levels_cache: dict[tuple[str, datetime | None], tuple[dict, object] | None] = {}
    records = []
    for vertical in candidates:
        legs = (vertical.short, vertical.long)
        triggers = [
            source
            for leg in legs
            for source in sources_by_contract.get(leg.contract_id, ())
            if source.open_interest and source.day_volume is not None and source.market_time <= cutoff
        ]
        if not triggers:
            reject(vertical.underlying, "O3_DATED_ACTIVITY_SOURCE_UNAVAILABLE")
            continue
        # Developer rule: the leg source with the highest day volume / open interest; the latest event of that contract.
        activity = max(triggers, key=lambda row: (Decimal(row.day_volume) / Decimal(row.open_interest),
                                                    row.market_time, -int(row.contract_id)))
        spot, spot_time = spot_by_underlying[vertical.underlying]
        valuations, failed = {}, None
        for leg in legs:
            valuation = valuation_inputs.get((leg.underlying, leg.expiration))
            if valuation is None:
                failed = "O3_VALUATION_INPUT_UNAVAILABLE"
                break
            mark = marks.get(leg.instrument_id)
            if mark is None:
                failed = "O3_LEG_MARK_UNAVAILABLE"
                break
            valued = value_leg(leg, mark.mark, spot=spot, at=mark.market_time, valuation=valuation)
            if isinstance(valued, str):
                failed = valued.split(":")[0]
                break
            valuations[leg.instrument_id] = valued
        if failed is not None:
            reject(vertical.underlying, failed)
            continue
        # O3's own clock rule (scheduled_cycle <= decision_at < valid_until), named here: the candidate is valid for
        # the policy's source age after its oldest leg mark, and never past the session close.
        oldest = min(marks[leg.instrument_id].market_time for leg in legs)
        if oldest + timedelta(seconds=maximum_source_age) <= decision_at:
            reject(vertical.underlying, "O3_LEG_MARK_STALE")
            continue
        if session_close <= decision_at:
            reject(vertical.underlying, "O3_SESSION_CLOSED_AT_DECISION")
            continue
        required_window = latest_completed_30m_window(min(cutoff, activity.market_time))
        key = (vertical.underlying, required_window)
        if key not in stock_cache:
            stock_cache[key] = stock_evidence(vertical.underlying, required_window)
        stock = stock_cache[key]
        decision = assess_options_credit_canonical_trend(
            activity, stock, direction=vertical.direction, market_cutoff=cutoff, decision_at=decision_at,
        )
        if decision.disposition != "CONFIRMED":
            for reason in decision.reasons or (f"O3_DECISION_{decision.disposition}",):
                reject(vertical.underlying, f"O3_DECISION:{reason}")
            continue
        if key not in levels_cache:
            levels_cache[key] = structural_levels(vertical.underlying, stock)
        bound = levels_cache[key]
        if bound is None:
            reject(vertical.underlying, "O3_STRUCTURAL_LEVELS_UNAVAILABLE")
            continue
        levels, security = bound
        try:
            parts = build_o3_parts(
                vertical, cohort_id=cohort_id, matrix_id=matrix_id, cutoff=cutoff, decision_at=decision_at, spot=spot,
                spot_time=spot_time, marks=marks, quotes=quotes, valuations=valuations, session_close=session_close,
                batch_id=batch_id,
            )
            technical = bind_direct_structural_levels(
                levels, security=security, direction=vertical.direction, spot=spot, received_at=decision_at,
                valid_until=decision.valid_until,
            )
            invalidation = next(level.price for level in technical.levels if level.role == "INVALIDATION")
            assess_o3_credit(
                parts.candidate, decision, activity_contract_id=int(activity.contract_id), snapshots=parts.snapshots,
                structural_invalidation=invalidation, scheduled_cycle=cutoff,
            )
        except (ValueError, TypeError, ArithmeticError, StopIteration) as error:
            reject(vertical.underlying, f"O3_ASSESSMENT:{_error_reason(error)}")
            continue
        activity_leg = next(leg for leg in legs if leg.contract_id == int(activity.contract_id))
        invocation = AdvancedO3Invocation(
            event_id=parts.event_id, candidate=parts.candidate, decision=decision,
            activity_contract_id=int(activity.contract_id), snapshots=parts.snapshots,
            structural_invalidation=invalidation, scheduled_cycle=cutoff, package=parts.package,
        )
        records.append(O3InputRecord(
            parts=parts, decision=decision, activity_contract_id=int(activity.contract_id),
            activity_instrument_id=activity_leg.instrument_id, activity_mark=marks[activity_leg.instrument_id],
            structural_invalidation=invalidation, document=advanced_o3_invocation_document(invocation),
        ))
        confirmed[vertical.underlying] += 1
    for counts in by_underlying.values():
        rejections.update(counts)
    outcomes = tuple(
        O3UnderlyingOutcome(underlying, considered[underlying], confirmed[underlying],
                            tuple(sorted(by_underlying.get(underlying, Counter()).items())))
        for underlying in sorted(set(by_underlying) | set(considered))
    )
    return O3InputBuild(tuple(records), tuple(sorted(rejections.items())), len(candidates), outcomes)


# --- database -------------------------------------------------------------------------------------------------------

def _detector_version_identity() -> dict:
    return {
        "detector_id": O3_INPUT_DETECTOR_ID, "version": O3_INPUT_DETECTOR_VERSION,
        "producer": O3_INPUT_PRODUCER_CONTRACT, "strategy_policy_sha256": o3_strategy_policy().sha256,
    }


def ensure_o3_input_detector_version_cursor(cursor) -> str:
    identity = _detector_version_identity()
    detector_sha256 = canonical_sha256(identity)
    from psycopg2.extras import Json

    cursor.execute(
        "INSERT INTO rt.detector_versions (detector_sha256,detector_id,version,code_sha256,policy_sha256,"
        "requirements,output_kind) VALUES (%s,%s,%s,%s,%s,%s,'SIGNAL') ON CONFLICT DO NOTHING",
        (detector_sha256, O3_INPUT_DETECTOR_ID, O3_INPUT_DETECTOR_VERSION, canonical_sha256({"code": O3_INPUT_PRODUCER_CONTRACT}),
         o3_strategy_policy().sha256, Json({"canonical_marks": True, "execution_permission": False})),
    )
    return detector_sha256


def persist_o3_input_record_cursor(cursor, record: O3InputRecord, *, cohort_id: UUID, cutoff: datetime,
                                   decision_at: datetime, detector_sha256: str) -> bool:
    """Package, event and input in the caller's transaction; returns False when the event already exists (retry)."""
    from psycopg2.extras import Json

    from .option_portal_repository import persist_realtime_option_package_cursor

    candidate_id = record.parts.candidate.candidate_id
    # The package repository reads its rows back as tuples (`_database_identity` iterates the row), so it gets a plain
    # cursor on the same transaction; a dict cursor would make every write look like an identity conflict.
    with cursor.connection.cursor() as plain:
        persist_realtime_option_package_cursor(plain, record.parts.package)
    dedup = canonical_sha256({"cohort_id": str(cohort_id), "candidate_id": str(candidate_id), "cutoff": cutoff.isoformat(),
                              "detector": O3_INPUT_DETECTOR_ID})
    cursor.execute(
        "INSERT INTO rt.detector_events (event_id,dedup_sha256,cohort_id,provider_tier,detector_sha256,instrument_id,"
        "trigger_identity,trigger_at,decision_at,valid_until,disposition,direction,reason_codes,readiness_epoch,"
        "evidence_sha256,payload_schema,payload) VALUES (%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,%s,%s,'CONFIRMED',%s,"
        "ARRAY[]::text[],1,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING event_id",
        (str(record.parts.event_id), dedup, str(cohort_id), detector_sha256, record.activity_instrument_id,
         f"O3:{candidate_id}:{cutoff.isoformat()}", cutoff, decision_at, record.decision.valid_until,
         int(record.decision.direction), canonical_sha256(record.document),
         O3_INPUT_PAYLOAD_SCHEMA,
         Json({"detector_id": "O3", "candidate_id": str(candidate_id), "disposition": "CONFIRMED",
               "producer": O3_INPUT_PRODUCER_CONTRACT})),
    )
    inserted = cursor.fetchone() is not None
    if inserted:
        cursor.execute(
            "INSERT INTO rt.detector_event_inputs (event_id,input_role,input_key,source_kind,option_mark_evidence_id,"
            "input_sha256,available_at,received_at,retained_input) VALUES (%s,'O3_NATIVE_INVOCATION',%s,'OPTION_MARK',"
            "%s,%s,%s,%s,%s)",
            (str(record.parts.event_id), str(candidate_id), str(record.activity_mark.evidence_id),
             canonical_sha256(record.document), record.activity_mark.observed_time,
             max(decision_at, record.activity_mark.observed_time), Json(record.document)),
        )
    return inserted


O3_NO_INPUT_DISPOSITION = "NO_INPUT"


def persist_o3_no_input_cursor(cursor, outcome: O3UnderlyingOutcome, *, instrument_id: int, cohort_id: UUID,
                               cutoff: datetime, decision_at: datetime, detector_sha256: str) -> bool:
    """The producer evaluated this underlying at this cutoff and nothing qualified: one NO_INPUT row with the counts
    and reasons (INV-8), so the lane can report zero inputs instead of 'no producer'. Idempotent like the events."""
    from psycopg2.extras import Json

    payload = {"detector_id": "O3", "producer": O3_INPUT_PRODUCER_CONTRACT, "underlying": outcome.underlying,
               "considered": outcome.considered, "confirmed": outcome.confirmed,
               "rejections": dict(outcome.rejections)}
    event_id = uuid5(NAMESPACE_URL, f"stockscreener:{O3_INPUT_DETECTOR_ID}:no-input:{cohort_id}:{outcome.underlying}:"
                                    f"{cutoff.isoformat()}")
    dedup = canonical_sha256({"cohort_id": str(cohort_id), "underlying": outcome.underlying, "cutoff": cutoff.isoformat(),
                              "detector": O3_INPUT_DETECTOR_ID, "disposition": O3_NO_INPUT_DISPOSITION})
    cursor.execute(
        "INSERT INTO rt.detector_events (event_id,dedup_sha256,cohort_id,provider_tier,detector_sha256,instrument_id,"
        "trigger_identity,trigger_at,decision_at,disposition,reason_codes,readiness_epoch,evidence_sha256,"
        "payload_schema,payload) VALUES (%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,%s,%s,%s,1,%s,%s,%s) "
        "ON CONFLICT DO NOTHING RETURNING event_id",
        (str(event_id), dedup, str(cohort_id), detector_sha256, int(instrument_id),
         f"O3-NO-INPUT:{outcome.underlying}:{cutoff.isoformat()}", cutoff, decision_at, O3_NO_INPUT_DISPOSITION,
         sorted(reason for reason, _ in outcome.rejections)[:20], canonical_sha256(payload), O3_INPUT_PAYLOAD_SCHEMA,
         Json(payload)),
    )
    return cursor.fetchone() is not None


def _row(value, names):
    return dict(value) if isinstance(value, Mapping) else dict(zip(names, value))


def load_stock_instruments_cursor(cursor, securities: Mapping[str, object]) -> tuple[tuple[str, int], ...]:
    found = []
    for ticker, security in sorted(securities.items()):
        if security is None:
            continue
        cursor.execute(
            "SELECT instrument_id FROM rt.instruments WHERE security_id=%s AND contract_id IS NULL "
            "ORDER BY instrument_id LIMIT 1",
            (str(security.security_id),),
        )
        value = cursor.fetchone()
        if value is not None:
            found.append((ticker, int(_row(value, ("instrument_id",))["instrument_id"])))
    return tuple(found)


_OCC_TICKER = re.compile(r"^O:([A-Z][A-Z0-9.]*?)\d{6}[CP]\d{8}$")


def occ_underlying(contract_ticker: str) -> str | None:
    """The root of a provider OCC ticker (`O:SPY261120C00650000` -> `SPY`); the catalog stores no underlying ticker."""
    found = _OCC_TICKER.match(str(contract_ticker))
    return found.group(1) if found else None


def load_watched_contracts_cursor(cursor, plan) -> tuple[WatchedContract, ...]:
    """The window's slot-set contracts (both channels collapse to one instrument) with their catalog terms."""
    members = {int(member.instrument_id): int(member.catalog_version_id) for member in plan.control_plane.slot_set.members}
    if not members:
        return ()
    names = ("instrument_id", "contract_id", "version_id", "contract_ticker", "call_put", "strike", "expiration",
             "multiplier")
    ids = sorted(members)
    cursor.execute(
        "SELECT instrument.instrument_id,contract.contract_id,version.version_id,contract.contract_ticker,"
        "version.call_put,version.strike,version.expiration,version.multiplier "
        "FROM unnest(%s::bigint[],%s::bigint[]) AS target(instrument_id,version_id) "
        "JOIN rt.instruments AS instrument ON instrument.instrument_id=target.instrument_id "
        "JOIN ref.option_contracts AS contract ON contract.contract_id=instrument.contract_id "
        "JOIN ref.option_contract_versions AS version ON version.version_id=target.version_id "
        "AND version.contract_id=contract.contract_id",
        (ids, [members[value] for value in ids]),
    )
    rows = [_row(value, names) for value in cursor.fetchall()]
    allowed = set(plan.control_plane.universe.option_underlyings)
    contracts = {}
    for row in rows:
        underlying = occ_underlying(row["contract_ticker"])
        if underlying is None or underlying not in allowed:
            continue
        contracts[int(row["instrument_id"])] = WatchedContract(
            instrument_id=int(row["instrument_id"]), contract_id=int(row["contract_id"]),
            catalog_version_id=int(row["version_id"]), contract_ticker=str(row["contract_ticker"]),
            underlying=underlying, call_put=str(row["call_put"]).upper()[:1],
            strike=Decimal(str(row["strike"])), expiration=row["expiration"],
            multiplier=int(Decimal(str(row["multiplier"]))),
        )
    return tuple(contracts[key] for key in sorted(contracts))


def load_leg_marks_cursor(cursor, instrument_ids, cutoff: datetime) -> dict[int, LegMark]:
    names = ("evidence_id", "instrument_id", "bid", "ask", "mark", "market_time", "observed_time", "payload_sha256")
    cursor.execute(
        "SELECT DISTINCT ON (instrument_id) evidence_id::text,instrument_id,bid,ask,mark,market_time,observed_time,"
        "payload_sha256 FROM rt.option_mark_evidence WHERE provider_tier='MASSIVE_ADVANCED' "
        "AND source_kind='ADVANCED_NBBO' AND instrument_id=ANY(%s) AND market_time<=%s "
        "ORDER BY instrument_id,market_time DESC,evidence_id",
        (sorted({int(value) for value in instrument_ids}), cutoff),
    )
    marks = {}
    for value in cursor.fetchall():
        row = _row(value, names)
        if row["bid"] is None or row["ask"] is None or row["mark"] is None:
            continue
        marks[int(row["instrument_id"])] = LegMark(
            evidence_id=UUID(str(row["evidence_id"])), instrument_id=int(row["instrument_id"]),
            bid=Decimal(str(row["bid"])), ask=Decimal(str(row["ask"])), mark=Decimal(str(row["mark"])),
            market_time=row["market_time"], observed_time=row["observed_time"], payload_sha256=str(row["payload_sha256"]),
        )
    return marks


def leg_quotes_from_plan(plan, contracts, marks) -> dict[int, LegQuote]:
    """Day volume from the window's rolling state (cumulative per contract), open interest from its reference state."""
    volumes = {
        row.checkpoint.contract_ticker: int(row.checkpoint.cumulative_volume)
        for row in plan.rolling_state.contract_volumes
    }
    open_interest = {
        int(row.contract_id): row.open_interest
        for row in plan.reference_state.open_interest
        if row.open_interest is not None
    }
    quotes = {}
    for contract in contracts:
        mark = marks.get(contract.instrument_id)
        quotes[contract.instrument_id] = LegQuote(
            instrument_id=contract.instrument_id,
            bid=None if mark is None else mark.bid,
            ask=None if mark is None else mark.ask,
            market_time=None if mark is None else mark.market_time,
            day_volume=volumes.get(contract.contract_ticker),
            open_interest=open_interest.get(contract.contract_id),
        )
    return quotes


def valuation_inputs_from_plan(plan) -> dict[tuple[str, date], ValuationInput]:
    return {
        (row.ticker, row.expiration_date): ValuationInput(
            Decimal(str(row.risk_free_rate)), Decimal(str(row.dividend_yield)), str(row.model_version),
        )
        for row in plan.reference_state.valuation_inputs
    }


def load_spots_cursor(cursor, stock_instruments, *, cutoff: datetime, known_by: datetime) -> dict[str, tuple[Decimal, datetime]]:
    """The stock's last realtime 1-minute close at or before the cutoff, known by `known_by` (OA1 / R33 rule), read
    through the bar repository (the only module that names the bar tables)."""
    from .equity_bar_repository import load_known_bar_revisions_cursor

    instruments = dict(stock_instruments)
    if not instruments:
        return {}
    loaded = load_known_bar_revisions_cursor(
        cursor, instrument_ids=sorted(set(instruments.values())), interval="1m", basis="RAW_UNADJUSTED",
        known_by=known_by, start=cutoff - timedelta(minutes=15), end=cutoff, scopes=("RTH",),
    )
    spots = {}
    for ticker, instrument_id in sorted(instruments.items()):
        rows = [row for row in loaded.get(instrument_id, ())
                if str(row.get("source_kind")) == "REALTIME_STREAM" and row["bar_end"] > cutoff - timedelta(minutes=15)]
        if rows:
            last = max(rows, key=lambda row: (row["bar_end"], row["observed_at"]))
            spots[ticker] = (Decimal(str(last["close_price"])), last["bar_end"])
    return spots


def load_trend_bars_cursor(cursor, *, security_id: UUID, cutoff: datetime, known_by: datetime) -> tuple[list, list]:
    """Final RTH unadjusted 30m bars (enough for 202) and daily bars (enough for 21) known by `known_by`."""
    from .equity_bar_repository import load_known_bar_revisions_cursor

    cursor.execute(
        "SELECT instrument_id FROM rt.instruments WHERE security_id=%s AND contract_id IS NULL", (str(security_id),),
    )
    instrument_ids = tuple(int(_row(value, ("instrument_id",))["instrument_id"]) for value in cursor.fetchall())
    out = []
    for interval, days in (("30m", 45), ("1d", 50)):
        loaded = load_known_bar_revisions_cursor(
            cursor, instrument_ids=instrument_ids, interval=interval, basis="RAW_UNADJUSTED", known_by=known_by,
            start=cutoff - timedelta(days=days), end=cutoff, scopes=("RTH",),
        )
        rows = {}
        for values in loaded.values():
            for row in values:
                if not row["is_final"] or str(row["security_id"]) != str(security_id):
                    continue
                if max(row["created_at"], row["observed_at"]) > known_by:
                    continue
                rows[row["bar_start"]] = {**row, "system_observed_at": row["observed_at"]}
        out.append([rows[key] for key in sorted(rows)])
    return out[0], out[1]


def split_clear_cursor(cursor, *, security_id: UUID, start: date, end: date, known_by: datetime) -> bool:
    cursor.execute(
        "SELECT 1 FROM ref.corporate_action_coverage AS coverage WHERE coverage.security_id=%s "
        "AND coverage.action_type='SPLIT' AND coverage.window_start<=%s AND coverage.window_end>=%s "
        "AND coverage.received_at<=%s AND NOT EXISTS (SELECT 1 FROM ref.corporate_action_revisions AS action "
        "WHERE action.security_id=coverage.security_id AND action.action_type='SPLIT' "
        "AND action.effective_date BETWEEN %s AND %s AND action.observed_at<=%s) LIMIT 1",
        (str(security_id), start, end, known_by, start, end, known_by),
    )
    return cursor.fetchone() is not None


def load_security_cursor(cursor, ticker: str, at: datetime):
    from .developer_cycle_loader import _security

    cursor.execute(
        "SELECT revision.revision_id,revision.security_id,revision.ticker,security.asset_type,revision.exchange,"
        "revision.effective_from,revision.observed_at,revision.payload_sha256 FROM ref.security_revisions AS revision "
        "JOIN ref.securities AS security ON security.security_id=revision.security_id WHERE revision.ticker=%s "
        "AND revision.effective_from<=%s AND (revision.effective_to IS NULL OR revision.effective_to>%s) "
        "ORDER BY revision.effective_from DESC LIMIT 1",
        (ticker, at, at),
    )
    row = cursor.fetchone()
    if row is None:
        return None
    return _security(tuple(row.values()) if isinstance(row, Mapping) else tuple(row))


@dataclass
class O3ProducerResult:
    status: str
    written: int = 0
    existing: int = 0
    records: int = 0
    considered: int = 0
    rejections: tuple[tuple[str, int], ...] = ()
    reason: str | None = None
    timings: dict = field(default_factory=dict)

    def log_line(self) -> str:
        return json.dumps({"event": "advanced_options_o3_inputs", "status": self.status, "written": self.written,
                           "existing": self.existing, "records": self.records, "considered": self.considered,
                           "rejections": dict(self.rejections), "reason": self.reason, "timings": self.timings},
                          sort_keys=True)


def build_o3_inputs_cursor(cursor, plan, *, decision_at: datetime) -> O3InputBuild:
    """Read everything one window's O3 inputs need (read-only) and assemble them."""
    from equity.domain import BarSourceKind  # noqa: F401  (import check: bar conversion below needs equity)
    from equity.materialization import materialize_equity_evidence
    from options.calendar import OptionExchangeCalendar
    from options.intraday_participation import build_canonical_trend_evidence

    from .advanced_options_native_loader import _equity_bar, _load_o1

    cutoff = plan.cutoff
    session_date = plan.control_plane.session_date
    session_close = OptionExchangeCalendar().session_close(session_date)
    contracts = load_watched_contracts_cursor(cursor, plan)
    marks = load_leg_marks_cursor(cursor, [row.instrument_id for row in contracts], cutoff)
    quotes = leg_quotes_from_plan(plan, contracts, marks)
    o1, _identity = _load_o1(cursor, plan, [])
    sources = tuple(invocation.source for invocation in o1.invocations)
    underlyings = sorted({row.underlying for row in contracts})
    securities = {ticker: load_security_cursor(cursor, ticker, cutoff) for ticker in underlyings}
    stock_instruments = load_stock_instruments_cursor(cursor, securities)
    spots = load_spots_cursor(cursor, stock_instruments, cutoff=cutoff, known_by=decision_at)
    bars_cache: dict[str, tuple[list, list]] = {}

    def bars(underlyer):
        if underlyer not in bars_cache:
            security = securities.get(underlyer)
            bars_cache[underlyer] = (
                ([], []) if security is None
                else load_trend_bars_cursor(cursor, security_id=security.security_id, cutoff=cutoff, known_by=decision_at)
            )
        return bars_cache[underlyer]

    def stock_evidence(underlyer, required_window):
        security = securities.get(underlyer)
        if security is None:
            return None
        trend, daily = bars(underlyer)
        window = [row for row in trend if required_window is not None and row["bar_end"] <= required_window][-202:]
        start = window[0]["session_date"] if window else session_date
        return build_canonical_trend_evidence(
            security_id=security.security_id, underlyer=underlyer, session_date=session_date,
            required_window=required_window, trend_bars=trend, daily_bars=daily,
            split_clear=split_clear_cursor(cursor, security_id=security.security_id, start=start, end=session_date,
                                           known_by=decision_at),
            received_at=decision_at, session_close=session_close,
        )

    def structural_levels(underlyer, stock):
        security = securities.get(underlyer)
        if security is None or stock is None or stock.status != "READY":
            return None
        trend, _daily = bars(underlyer)
        window = [row for row in trend if row["bar_end"] <= stock.required_window][-202:]
        if not window or str(window[-1]["bar_revision_id"]) != str(stock.trend_last_bar_revision_id):
            return None
        revisions = tuple(_equity_bar(row) for row in window)
        materialized = materialize_equity_evidence(
            analysis_run_id=uuid5(plan.rolling_state.state_id, f"o3-structural:{underlyer}"), security=security,
            interval="30m", bars=revisions, observed_at=decision_at, level_directions=(1, -1),
        )
        return dict(interval="30m", market_time=revisions[-1].bar_end, computed_at=decision_at,
                    bar_revision_ids=tuple(row.bar_revision_id for row in revisions),
                    levels=materialized.directional_levels or {}), security

    from dataclasses import replace

    build = assemble_o3_inputs(
        contracts=contracts, quotes=quotes, marks=marks, activity_sources=sources, spot_by_underlying=spots,
        valuation_inputs=valuation_inputs_from_plan(plan), stock_evidence=stock_evidence,
        structural_levels=structural_levels, cohort_id=plan.control_plane.cohort_id,
        matrix_id=plan.rolling_state.state_id, session_date=session_date, cutoff=cutoff, decision_at=decision_at,
        session_close=session_close, batch_id=plan.source_flow.source_flow_id,
    )
    return replace(build, stock_instruments=stock_instruments)


def o3_decision_time(plan) -> datetime:
    """The decision time: the Equity window's accepted input time, never before the cutoff (stable across retries)."""
    known_by = getattr(plan.equity_input_receipt, "known_by", None)
    return max(plan.cutoff, known_by) if known_by is not None else plan.cutoff


def produce_o3_inputs(provider, plan, *, role: str, write: bool = True) -> O3ProducerResult:
    """Design step 5 in its own transaction before the window's input-ready preflight. Never raises."""
    import time as clock

    started = clock.monotonic()
    try:
        decision_at = o3_decision_time(plan)
        with provider.transaction(role, read_only=not write, isolation_level="REPEATABLE READ") as unit:
            from psycopg2.extras import RealDictCursor

            with unit.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                build = build_o3_inputs_cursor(cursor, plan, decision_at=decision_at)
                built = clock.monotonic()
                written = existing = no_input = 0
                if write:
                    detector_sha256 = ensure_o3_input_detector_version_cursor(cursor)
                    for record in build.records:
                        if persist_o3_input_record_cursor(cursor, record, cohort_id=plan.control_plane.cohort_id,
                                                          cutoff=plan.cutoff, decision_at=decision_at,
                                                          detector_sha256=detector_sha256):
                            written += 1
                        else:
                            existing += 1
                    instruments = dict(build.stock_instruments)
                    for outcome in build.outcomes:
                        if outcome.confirmed or outcome.underlying not in instruments:
                            continue
                        no_input += int(persist_o3_no_input_cursor(
                            cursor, outcome, instrument_id=instruments[outcome.underlying],
                            cohort_id=plan.control_plane.cohort_id, cutoff=plan.cutoff, decision_at=decision_at,
                            detector_sha256=detector_sha256,
                        ))
        result = O3ProducerResult(
            "COMPLETE", written=written, existing=existing, records=len(build.records),
            considered=build.candidates_considered, rejections=build.rejections,
            timings={"build_s": round(built - started, 3), "total_s": round(clock.monotonic() - started, 3),
                     "no_input_rows": no_input},
        )
    except Exception as error:  # noqa: BLE001 - the producer never fails the window (design failure rule)
        reason = f"O3_INPUTS_UNAVAILABLE:{type(error).__name__}"
        result = O3ProducerResult("UNAVAILABLE", reason=reason,
                                  timings={"total_s": round(clock.monotonic() - started, 3)})
        print(json.dumps({"event": "advanced_options_o3_inputs_failed", "reason": reason,
                          "detail": str(error).splitlines()[-1][:200] if str(error) else ""}), file=sys.stderr, flush=True)
    print(result.log_line(), file=sys.stderr, flush=True)
    return result


__all__ = [
    "LegMark", "LegValuation", "O3InputBuild", "O3InputRecord", "O3Parts", "O3ProducerResult", "ValuationInput",
    "assemble_o3_inputs", "build_o3_inputs_cursor", "build_o3_parts", "o3_event_id", "o3_inputs_enabled",
    "persist_o3_input_record_cursor", "produce_o3_inputs", "value_leg",
]
