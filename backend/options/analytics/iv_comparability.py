"""Which contract IVs may be compared or aggregated (policy `iv_comparability_v1`).

The solver in `greeks.py` is European. Two properties of a stored `local_iv` make it
unsafe to compare across contracts, or against realized volatility, without a check:

1. Exercise-style bias. American puts carry an early-exercise premium that a European
   model reads as extra volatility. Measured with
   `backend/scripts/measure_iv_american_bias.py` (Cox-Ross-Rubinstein reference in
   `american_reference.py`, 800 steps, lattice error removed with a Black-Scholes
   control variate, SPY-like spot 600, r = 4.3%, q = 1.3%, volatility 15-60%, 1-90 days):

   - at the money and out of the money the bias is at most 0.3 volatility points up to
     60 days (0.4 at 90 days; it grows with r and with maturity);
   - puts up to 1% in the money (strike <= forward * 1.01) add at most 0.25 points over
     the at-the-money level (0.45 at one day with r = 5.5% and q = 0, the worst case
     measured). Beyond the band, maximum over the volatility grid: 2% in the money
     reaches 3.9 points at one day; 5% reaches 6.3 points at 7 days; 10% reaches 22
     points at 7 days and 4.5 at 90 days;
   - calls are negligible only while the carry r - q is at least one point (<= 0.14
     points even 15% in the money at q = 3.3%). At q = 4.3% (carry 0) a 5% in-the-money
     call at one day is +17 points and 10% / 7 days is +15.5; q = 6% and 8% are worse.
     The mirror holds for puts (McDonald-Schroder symmetry): at q - r >= 1 point an
     in-the-money put is negligible (<= 0.29 points, 15% in the money, q = 5.3%).
     The stock universe (q at most about 1.5%, r about 4%) always has carry >= 2.5
     points, so there calls are comparable at any depth; the rule is carry aware so a
     high-yield underlying or a low-rate regime cannot slip through.

   A contract is therefore exposed to the bias when it is a put with r - q > -carry_floor
   or a call with r - q < carry_floor, and it is comparable only if its in-the-money
   depth (fraction of the forward) is at most `itm_tolerance`.

2. Quote resolution. The mark moves in half-cent steps (design doc, "IV solver
   evaluation"), so one step changes IV by resolution / vega_per_vol_point volatility
   points. This was checked against the real solver (median relative deviation 0.01%).
   Requiring that step to move IV by at most `maximum_quote_noise_vol_points` (one
   point, the solver's own `maximum_iv_uncertainty` scale) gives
   `minimum_vega_per_vol_point = 0.005 / 1.0 = 0.005` USD/share/vol point. On the +/-15%
   corridor grid this keeps 78% of the cases; the dropped ones are far wings and
   one- or two-day options whose price is a few half-cents. The inverse-variance weight
   (vega / resolution)^2 lets a consumer that averages favor precise contracts.

Nothing here changes a stored IV. Consumers keep stored values and ask this module whether
a value may enter a comparison; an excluded contract carries explicit reason codes so the
caller can report degraded evidence instead of dropping it silently.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Iterable, Sequence

from options.analytics.greeks import IvFailureReason
from options.domain import ContractType

POLICY_VERSION = "iv_comparability_v1"
_BOUNDARY_EPSILON = 1e-12


class IvComparabilityReason(str, Enum):
    ITM_PUT_AMERICAN_BIAS = "ITM_PUT_AMERICAN_BIAS"
    ITM_CALL_AMERICAN_BIAS = "ITM_CALL_AMERICAN_BIAS"
    LOW_VEGA = "LOW_VEGA"
    NOT_IDENTIFIABLE = "NOT_IDENTIFIABLE"
    IV_UNAVAILABLE = "IV_UNAVAILABLE"
    INVALID_INPUT = "INVALID_INPUT"


@dataclass(frozen=True, slots=True)
class IvComparabilityPolicy:
    version: str = POLICY_VERSION
    itm_tolerance: float = 0.01
    carry_floor: float = 0.01
    quote_resolution: float = 0.005
    maximum_quote_noise_vol_points: float = 1.0

    def __post_init__(self) -> None:
        for name in ("itm_tolerance", "carry_floor", "quote_resolution", "maximum_quote_noise_vol_points"):
            value = getattr(self, name)
            if type(value) is not float or not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be a finite non-negative float")
        if self.quote_resolution <= 0 or self.maximum_quote_noise_vol_points <= 0:
            raise ValueError("quote resolution and noise budget must be positive")

    @property
    def minimum_vega_per_vol_point(self) -> float:
        return self.quote_resolution / self.maximum_quote_noise_vol_points


DEFAULT_IV_COMPARABILITY_POLICY = IvComparabilityPolicy()


@dataclass(frozen=True, slots=True)
class IvComparability:
    comparable: bool
    reasons: tuple[IvComparabilityReason, ...]
    forward: float | None = None
    itm_depth: float | None = None
    vega_per_vol_point: float | None = None
    quote_noise_vol_points: float | None = None
    weight: float = 0.0

    @property
    def reason_codes(self) -> tuple[str, ...]:
        return tuple(reason.value for reason in self.reasons)


@dataclass(frozen=True, slots=True)
class IvComparabilitySummary:
    policy_version: str
    considered_count: int
    comparable_count: int
    excluded_count: int
    reason_counts: tuple[tuple[str, int], ...]

    @property
    def status(self) -> str:
        """COMPARABLE, DEGRADED (some contracts excluded) or UNAVAILABLE (none usable)."""
        if self.comparable_count == 0:
            return "UNAVAILABLE"
        return "DEGRADED" if self.excluded_count else "COMPARABLE"

    def to_dict(self) -> dict[str, object]:
        return {
            "policy_version": self.policy_version,
            "status": self.status,
            "considered": self.considered_count,
            "comparable": self.comparable_count,
            "excluded": self.excluded_count,
            "reasons": dict(self.reason_counts),
        }


def classify_iv_comparability(
    *,
    contract_type: ContractType,
    spot: float | Decimal,
    strike: float | Decimal,
    time_to_expiration_years: float,
    risk_free_rate: float,
    dividend_yield: float,
    local_iv: float | None,
    local_vega_per_vol_point: float | None = None,
    failure_reason: IvFailureReason | str | None = None,
    policy: IvComparabilityPolicy = DEFAULT_IV_COMPARABILITY_POLICY,
) -> IvComparability:
    """Classify one contract's IV for comparison; vega is derived from the IV when absent."""
    if failure_reason is not None:
        solver_reason = (
            IvComparabilityReason.NOT_IDENTIFIABLE
            if str(getattr(failure_reason, "value", failure_reason)) == IvFailureReason.IV_NOT_IDENTIFIABLE.value
            else IvComparabilityReason.IV_UNAVAILABLE
        )
        return IvComparability(False, (solver_reason,))
    if local_iv is None or not math.isfinite(local_iv) or local_iv <= 0:
        return IvComparability(False, (IvComparabilityReason.IV_UNAVAILABLE,))
    try:
        spot_value, strike_value = float(spot), float(strike)
    except (TypeError, ValueError):
        return IvComparability(False, (IvComparabilityReason.INVALID_INPUT,))
    inputs = (spot_value, strike_value, time_to_expiration_years, risk_free_rate, dividend_yield)
    if (
        any(not math.isfinite(value) for value in inputs)
        or spot_value <= 0 or strike_value <= 0 or time_to_expiration_years <= 0
    ):
        return IvComparability(False, (IvComparabilityReason.INVALID_INPUT,))

    carry = risk_free_rate - dividend_yield
    forward = spot_value * math.exp(carry * time_to_expiration_years)
    ratio = strike_value / forward
    reasons: list[IvComparabilityReason] = []
    if contract_type is ContractType.PUT:
        itm_depth = ratio - 1.0
        exposed = carry > -policy.carry_floor + _BOUNDARY_EPSILON
        reason = IvComparabilityReason.ITM_PUT_AMERICAN_BIAS
    else:
        itm_depth = 1.0 - ratio
        exposed = carry < policy.carry_floor - _BOUNDARY_EPSILON
        reason = IvComparabilityReason.ITM_CALL_AMERICAN_BIAS
    if exposed and itm_depth > policy.itm_tolerance * (1.0 + _BOUNDARY_EPSILON):
        reasons.append(reason)

    vega = local_vega_per_vol_point
    if vega is None:
        vega = _vega_per_vol_point(
            spot_value, strike_value, time_to_expiration_years, risk_free_rate, dividend_yield, local_iv
        )
    if vega is None or not math.isfinite(vega):
        return IvComparability(False, (*reasons, IvComparabilityReason.INVALID_INPUT), forward, itm_depth)
    noise = policy.quote_resolution / vega if vega > 0 else math.inf
    if vega < policy.minimum_vega_per_vol_point:
        reasons.append(IvComparabilityReason.LOW_VEGA)
    comparable = not reasons
    weight = (vega / policy.quote_resolution) ** 2 if comparable else 0.0
    return IvComparability(comparable, tuple(reasons), forward, itm_depth, vega, noise, weight)


def classify_snapshot_iv(
    snapshot,
    policy: IvComparabilityPolicy = DEFAULT_IV_COMPARABILITY_POLICY,
) -> IvComparability:
    """Classify a stored snapshot or candidate leg (duck-typed on the snapshot field names)."""
    contract_type = ContractType(getattr(snapshot.contract_type, "value", snapshot.contract_type))
    failure = None
    if not getattr(snapshot, "iv_converged", True):
        failure = getattr(snapshot, "iv_failure_reason", None) or IvFailureReason.IV_CONVERGENCE_FAILED
    return classify_iv_comparability(
        contract_type=contract_type,
        spot=snapshot.spot,
        strike=snapshot.strike,
        time_to_expiration_years=snapshot.time_to_expiration_years,
        risk_free_rate=snapshot.risk_free_rate,
        dividend_yield=snapshot.dividend_yield,
        local_iv=snapshot.local_iv,
        local_vega_per_vol_point=snapshot.local_vega_per_vol_point,
        failure_reason=failure,
        policy=policy,
    )


def comparison_iv(
    snapshot,
    policy: IvComparabilityPolicy = DEFAULT_IV_COMPARABILITY_POLICY,
) -> tuple[float | None, tuple[str, ...]]:
    """The snapshot's IV when it may be compared (for example with realized volatility), else
    None and `IV_NOT_COMPARABLE_<reason>` codes that fit a ready/unavailable measurement."""
    verdict = classify_snapshot_iv(snapshot, policy)
    if verdict.comparable:
        return float(snapshot.local_iv), ()
    return None, tuple(f"IV_NOT_COMPARABLE_{code}" for code in verdict.reason_codes)


def summarize_iv_comparability(
    results: Iterable[IvComparability],
    policy: IvComparabilityPolicy = DEFAULT_IV_COMPARABILITY_POLICY,
) -> IvComparabilitySummary:
    """Counts of considered/comparable/excluded contracts; a contract with two reasons counts in both."""
    items = tuple(results)
    reasons = Counter(code for item in items for code in item.reason_codes)
    comparable = sum(item.comparable for item in items)
    return IvComparabilitySummary(
        policy.version, len(items), comparable, len(items) - comparable, tuple(sorted(reasons.items()))
    )


def vega_weighted_mean(values: Sequence[float], results: Sequence[IvComparability]) -> float | None:
    """Inverse-variance weighted mean over comparable contracts; None when none is usable."""
    if len(values) != len(results):
        raise ValueError("values and comparability results must align")
    weight_sum = value_sum = 0.0
    for value, result in zip(values, results):
        if result.comparable and result.weight > 0 and math.isfinite(value):
            weight_sum += result.weight
            value_sum += result.weight * value
    return value_sum / weight_sum if weight_sum > 0 else None


def _vega_per_vol_point(spot, strike, maturity, rate, dividend, volatility) -> float | None:
    try:
        root_time = math.sqrt(maturity)
        d1 = (
            math.log(spot / strike) + (rate - dividend + 0.5 * volatility * volatility) * maturity
        ) / (volatility * root_time)
        density = math.exp(-0.5 * d1 * d1) / math.sqrt(2.0 * math.pi)
        return spot * math.exp(-dividend * maturity) * density * root_time * 0.01
    except (ValueError, OverflowError, ZeroDivisionError):
        return None
