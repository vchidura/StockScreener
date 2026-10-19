"""Provider-free P3C plans for Advanced Options acquisition and valuation."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from options.analytics.greeks import (
    LocalGreeksResult,
    OptionValuationInput,
    solve_local_greeks,
)
from options.config import ValuationPolicy
from options.model_inputs import ExpirationValuationCandidate

from .acquisition_ledger import (
    ProviderRequestLane,
    ProviderRequestReservation,
)
from .advanced_options_control_plane import (
    AdvancedOptionsControlPlanePlan,
    AdvancedOptionsPolicyPins,
)
from .contracts import canonical_sha256
from .option_discovery_scope import RealtimeOptionDiscoveryScope
from .materialization import MaterializedQuote
from .option_portal_adapter import (
    RealtimeOptionPackage,
    RealtimeOptionPackageMark,
    RealtimePackageMarkStatus,
    materialize_realtime_option_package_mark,
)
from .preflight import ProviderTier
from .provider_budget import ProviderBudgetScope
from .reference_facts import (
    ReferenceFact,
    ReferenceFactRelation,
    validate_reference_facts,
)


DISCOVERY_ENDPOINT = "OPTIONS_CONTRACT_DISCOVERY"
OPEN_INTEREST_ENDPOINT = "OPTIONS_DAILY_OPEN_INTEREST"
REPAIR_ENDPOINT = "OPTIONS_REST_ALIGNMENT"
ADVANCED_OPTIONS_GREEKS_MODEL = "black_scholes_european_v2"


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: str, name: str) -> str:
    if len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


@dataclass(frozen=True, slots=True)
class AdvancedOptionsReferenceClocks:
    session_date: date
    planned_at: datetime
    market_cutoff: datetime
    known_at: datetime
    authorized_until: datetime
    prior_oi_session: date

    def __post_init__(self) -> None:
        planned = _utc(self.planned_at, "planned_at")
        cutoff = _utc(self.market_cutoff, "market_cutoff")
        known = _utc(self.known_at, "known_at")
        authorized = _utc(self.authorized_until, "authorized_until")
        if (
            cutoff.date() != self.session_date
            or not planned <= cutoff <= known <= authorized
            or self.prior_oi_session >= self.session_date
        ):
            raise ValueError("Advanced Options reference clocks are inconsistent")
        object.__setattr__(self, "planned_at", planned)
        object.__setattr__(self, "market_cutoff", cutoff)
        object.__setattr__(self, "known_at", known)
        object.__setattr__(self, "authorized_until", authorized)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsRepairWindow:
    gap_id: UUID
    underlyer: str
    requested_from: datetime
    requested_to: datetime
    reason_code: str

    def __post_init__(self) -> None:
        underlyer = self.underlyer.strip().upper()
        reason = self.reason_code.strip().upper()
        start = _utc(self.requested_from, "requested_from")
        end = _utc(self.requested_to, "requested_to")
        if not underlyer or not reason or end <= start:
            raise ValueError("Advanced Options repair window is invalid")
        object.__setattr__(self, "underlyer", underlyer)
        object.__setattr__(self, "reason_code", reason)
        object.__setattr__(self, "requested_from", start)
        object.__setattr__(self, "requested_to", end)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsProducerPlan:
    plan_id: UUID
    plan_sha256: str
    control_plane_plan_id: UUID
    discovery_scope: RealtimeOptionDiscoveryScope
    clocks: AdvancedOptionsReferenceClocks
    provider_budget_sha256: str
    requests: tuple[ProviderRequestReservation, ...]
    repair_windows: tuple[AdvancedOptionsRepairWindow, ...]
    acquisition_authorized: bool = False
    repair_execution_authorized: bool = False


def build_advanced_options_producer_plan(
    *,
    control_plane: AdvancedOptionsControlPlanePlan,
    discovery_scope: RealtimeOptionDiscoveryScope,
    provider_budget: ProviderBudgetScope,
    clocks: AdvancedOptionsReferenceClocks,
    endpoint_response_byte_limits: Mapping[str, int],
    repair_windows: tuple[AdvancedOptionsRepairWindow, ...] = (),
) -> AdvancedOptionsProducerPlan:
    if not isinstance(control_plane, AdvancedOptionsControlPlanePlan):
        raise TypeError("Advanced Options control plane is invalid")
    if not control_plane.capability_only:
        raise ValueError("Advanced Options producer plan requires inert P1 scope")
    if (
        not isinstance(discovery_scope, RealtimeOptionDiscoveryScope)
        or discovery_scope.provider_tier is not ProviderTier.MASSIVE_ADVANCED
        or discovery_scope.profile_id != control_plane.profile_id
        or discovery_scope.profile_sha256 != control_plane.profile_sha256
        or discovery_scope.universe != control_plane.universe
    ):
        raise ValueError("Advanced Options discovery scope does not match")
    if (
        provider_budget.sha256 != control_plane.provider_budget.sha256
        or clocks.session_date != control_plane.session_date
        or clocks.planned_at < control_plane.planned_at
        or clocks.authorized_until > provider_budget.authorized_until
        or clocks.known_at >= control_plane.expires_at
    ):
        raise ValueError("Advanced Options provider budget or clocks changed")
    limits = dict(endpoint_response_byte_limits)
    if set(limits) != {
        DISCOVERY_ENDPOINT,
        OPEN_INTEREST_ENDPOINT,
        REPAIR_ENDPOINT,
    } or any(type(value) is not int or value <= 0 for value in limits.values()):
        raise ValueError("Advanced Options endpoint byte limits are incomplete")
    underlyings = control_plane.universe.option_underlyings
    repairs = tuple(sorted(
        repair_windows,
        key=lambda row: (
            row.requested_from,
            row.requested_to,
            row.underlyer,
            str(row.gap_id),
        ),
    ))
    if any(
        repair.underlyer not in underlyings
        or repair.requested_from < clocks.planned_at
        or repair.requested_to > clocks.market_cutoff
        for repair in repairs
    ):
        raise ValueError("Advanced Options repair window is outside reviewed scope")
    request_specs = [
        (
            ProviderRequestLane.OPTIONS_CHAIN,
            DISCOVERY_ENDPOINT,
            underlyer,
            None,
        )
        for underlyer in underlyings
    ]
    request_specs.extend(
        (
            ProviderRequestLane.REFERENCE,
            OPEN_INTEREST_ENDPOINT,
            underlyer,
            None,
        )
        for underlyer in underlyings
    )
    request_specs.extend(
        (
            ProviderRequestLane.OPTIONS_ALIGNMENT,
            REPAIR_ENDPOINT,
            repair.underlyer,
            repair,
        )
        for repair in repairs
    )
    if (
        len(request_specs) > provider_budget.request_ceiling
        or sum(limits[endpoint] for _, endpoint, _, _ in request_specs)
        > provider_budget.decoded_byte_ceiling
    ):
        raise ValueError("Advanced Options producer plan exceeds provider budget")
    requests = []
    for lane, endpoint, underlyer, repair in request_specs:
        request_identity = {
            "contract": "advanced-options-provider-request-v1",
            "control_plane_plan_id": control_plane.plan_id,
            "discovery_scope_sha256": discovery_scope.sha256,
            "clocks_sha256": clocks.sha256,
            "provider_budget_sha256": provider_budget.sha256,
            "lane": lane,
            "endpoint_class": endpoint,
            "underlyer": underlyer,
            "repair_window": repair,
        }
        request_sha256 = canonical_sha256(request_identity)
        reservation = ProviderRequestReservation(
            request_id=uuid5(
                NAMESPACE_URL,
                f"stockscreener:advanced-options-request:{request_sha256}",
            ),
            account_alias=provider_budget.account_alias,
            lane=lane,
            endpoint_class=endpoint,
            request_sha256=request_sha256,
            reserved_at=clocks.planned_at,
            cohort_id=control_plane.cohort_id,
        )
        requests.append(provider_budget.bind(
            reservation,
            reserved_response_bytes=limits[endpoint],
        ))
    identity = {
        "contract": "advanced-options-producer-plan-v1",
        "control_plane_plan_id": control_plane.plan_id,
        "discovery_scope_sha256": discovery_scope.sha256,
        "clocks_sha256": clocks.sha256,
        "provider_budget_sha256": provider_budget.sha256,
        "request_ids": tuple(row.request_id for row in requests),
        "repair_windows": repairs,
        "acquisition_authorized": False,
        "repair_execution_authorized": False,
    }
    plan_sha256 = canonical_sha256(identity)
    return AdvancedOptionsProducerPlan(
        plan_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-producer:{plan_sha256}",
        ),
        plan_sha256=plan_sha256,
        control_plane_plan_id=control_plane.plan_id,
        discovery_scope=discovery_scope,
        clocks=clocks,
        provider_budget_sha256=provider_budget.sha256,
        requests=tuple(requests),
        repair_windows=repairs,
    )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsOpenInterestObservation:
    contract_id: int
    contract_ticker: str
    underlyer: str
    open_interest: int
    settlement_session: date
    observed_session: date
    observed_at: datetime
    received_at: datetime
    payload_sha256: str

    def __post_init__(self) -> None:
        ticker = self.contract_ticker.strip().upper()
        underlyer = self.underlyer.strip().upper()
        observed = _utc(self.observed_at, "observed_at")
        received = _utc(self.received_at, "received_at")
        if (
            type(self.contract_id) is not int
            or self.contract_id <= 0
            or not ticker.startswith("O:")
            or not underlyer
            or type(self.open_interest) is not int
            or self.open_interest < 0
            or received < observed
        ):
            raise ValueError("Advanced Options OI observation is invalid")
        object.__setattr__(self, "contract_ticker", ticker)
        object.__setattr__(self, "underlyer", underlyer)
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "received_at", received)
        object.__setattr__(
            self,
            "payload_sha256",
            _sha256(self.payload_sha256, "payload_sha256"),
        )


def build_advanced_options_oi_facts(
    plan: AdvancedOptionsProducerPlan,
    observations: tuple[AdvancedOptionsOpenInterestObservation, ...],
) -> tuple[ReferenceFact, ...]:
    if not observations:
        raise ValueError("Advanced Options OI facts cannot be empty")
    facts = []
    for observation in observations:
        if (
            observation.underlyer
            not in plan.discovery_scope.universe.option_underlyings
            or observation.settlement_session != plan.clocks.prior_oi_session
            or observation.observed_session != plan.clocks.session_date
            or observation.received_at > plan.clocks.known_at
        ):
            raise ValueError("Advanced Options OI clock or scope mismatch")
        digest = canonical_sha256({
            "contract": "advanced-options-oi-baseline-v1",
            "producer_plan_id": plan.plan_id,
            "observation": observation,
        })
        facts.append(ReferenceFact.build(
            ReferenceFactRelation.OI_BASELINE,
            "OI_BASELINE",
            observation.contract_ticker,
            baseline_id=str(uuid5(
                NAMESPACE_URL,
                f"stockscreener:advanced-options-oi:{digest}",
            )),
            contract_id=observation.contract_id,
            settlement_session=observation.settlement_session,
            open_interest=observation.open_interest,
            source="massive",
            date_basis="PROVIDER_CHAIN_SNAPSHOT_PRIOR_SESSION",
            observed_session=observation.observed_session,
            observed_at=observation.observed_at,
            received_at=observation.received_at,
            payload_sha256=observation.payload_sha256,
            supersedes_baseline_id=None,
        ))
    result = tuple(facts)
    validate_reference_facts(result)
    return result


@dataclass(frozen=True, slots=True)
class AdvancedOptionsSpotEvidence:
    instrument_id: int
    spot: Decimal
    market_time: datetime
    observed_time: datetime
    payload_sha256: str

    def __post_init__(self) -> None:
        market = _utc(self.market_time, "market_time")
        observed = _utc(self.observed_time, "observed_time")
        if (
            type(self.instrument_id) is not int
            or self.instrument_id <= 0
            or type(self.spot) is not Decimal
            or not self.spot.is_finite()
            or self.spot <= 0
            or observed < market
        ):
            raise ValueError("Advanced Options spot evidence is invalid")
        object.__setattr__(self, "market_time", market)
        object.__setattr__(self, "observed_time", observed)
        object.__setattr__(
            self,
            "payload_sha256",
            _sha256(self.payload_sha256, "payload_sha256"),
        )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsGreeksEvidence:
    evidence_id: UUID
    contract_id: int
    mark_evidence_id: UUID
    model_version: str
    model_bundle_sha256: str
    valuation_policy_sha256: str
    rate_observation_ids: tuple[UUID, ...]
    rate_payload_sha256: tuple[str, ...]
    dividend_payload_sha256: str
    spot_payload_sha256: str
    result: LocalGreeksResult
    evidence_sha256: str


@dataclass(frozen=True, slots=True)
class AdvancedOptionsValuationBatch:
    status: str
    reason_codes: tuple[str, ...]
    package_mark: RealtimeOptionPackageMark | None
    greeks: tuple[AdvancedOptionsGreeksEvidence, ...]
    batch_sha256: str


def build_advanced_options_valuation_batch(
    *,
    package: RealtimeOptionPackage,
    quotes: Mapping[int, MaterializedQuote],
    spot_evidence: Mapping[int, AdvancedOptionsSpotEvidence],
    valuation_inputs: Mapping[date, ExpirationValuationCandidate],
    checked_at: datetime,
    policy: ValuationPolicy,
    pins: AdvancedOptionsPolicyPins,
) -> AdvancedOptionsValuationBatch:
    checked = _utc(checked_at, "checked_at")
    if policy.policy_sha256 != pins.valuation_policy_sha256:
        raise ValueError("Advanced Options valuation policy pin changed")
    mark_result = materialize_realtime_option_package_mark(
        package,
        quotes,
        checked_at=checked,
        policy=policy,
    )
    if mark_result.status is not RealtimePackageMarkStatus.READY:
        digest = canonical_sha256({
            "contract": "advanced-options-valuation-unavailable-v1",
            "candidate_id": package.candidate_id,
            "policy_sha256": policy.policy_sha256,
            "reasons": mark_result.reason_codes,
            "checked_at": checked,
        })
        return AdvancedOptionsValuationBatch(
            "UNAVAILABLE",
            mark_result.reason_codes,
            None,
            (),
            digest,
        )
    mark = mark_result.mark
    by_instrument = {
        evidence.instrument_id: evidence for evidence in mark.mark_evidence
    }
    valuation_rows = []
    evidence_inputs = []
    for leg in package.legs:
        candidate = valuation_inputs.get(leg.expiration_date)
        spot = spot_evidence.get(leg.instrument_id)
        quote = by_instrument[leg.instrument_id]
        selected_at = (
            _utc(candidate.selected_at, "valuation selected_at")
            if candidate is not None
            else None
        )
        if (
            candidate is None
            or spot is None
            or candidate.expiration_date != leg.expiration_date
            or candidate.valuation_date != package.entry_market_time.date()
            or candidate.spot != spot.spot
            or selected_at > checked
            or spot.observed_time > checked
            or abs(
                (quote.market_time - spot.market_time).total_seconds()
            ) > policy.maximum_option_spot_skew_seconds
            or not candidate.rate_observation_ids
            or not candidate.rate_payload_sha256
            or len(candidate.rate_observation_ids)
            != len(candidate.rate_payload_sha256)
        ):
            raise ValueError("Advanced Options valuation references are incomplete")
        for digest in candidate.rate_payload_sha256:
            _sha256(digest, "rate_payload_sha256")
        _sha256(
            candidate.dividend_payload_sha256,
            "dividend_payload_sha256",
        )
        days = (leg.expiration_date - candidate.valuation_date).days
        if days <= 0:
            raise ValueError("Advanced Options valuation expiry is not future")
        valuation_rows.append(OptionValuationInput(
            contract_type=leg.contract_type,
            spot=spot.spot,
            strike=leg.strike,
            model_mark=quote.mark,
            time_to_expiration_years=float(days / 365.0),
            risk_free_rate=float(candidate.risk_free_rate),
            dividend_yield=float(candidate.dividend_yield),
        ))
        evidence_inputs.append((leg, quote, spot, candidate))
    results = solve_local_greeks(tuple(valuation_rows))
    greeks = []
    for result, (leg, quote, spot, candidate) in zip(
        results,
        evidence_inputs,
    ):
        identity = {
            "contract": "advanced-options-greeks-evidence-v1",
            "candidate_id": package.candidate_id,
            "contract_id": leg.contract_id,
            "mark_evidence_id": quote.evidence_id,
            "model_version": ADVANCED_OPTIONS_GREEKS_MODEL,
            "model_bundle_sha256": pins.model_bundle_sha256,
            "valuation_policy_sha256": policy.policy_sha256,
            "rate_observation_ids": candidate.rate_observation_ids,
            "rate_payload_sha256": candidate.rate_payload_sha256,
            "dividend_payload_sha256": candidate.dividend_payload_sha256,
            "spot_payload_sha256": spot.payload_sha256,
            "result": result,
        }
        evidence_sha256 = canonical_sha256(identity)
        greeks.append(AdvancedOptionsGreeksEvidence(
            evidence_id=uuid5(
                NAMESPACE_URL,
                f"stockscreener:advanced-options-greeks:{evidence_sha256}",
            ),
            contract_id=leg.contract_id,
            mark_evidence_id=quote.evidence_id,
            model_version=ADVANCED_OPTIONS_GREEKS_MODEL,
            model_bundle_sha256=pins.model_bundle_sha256,
            valuation_policy_sha256=policy.policy_sha256,
            rate_observation_ids=candidate.rate_observation_ids,
            rate_payload_sha256=candidate.rate_payload_sha256,
            dividend_payload_sha256=candidate.dividend_payload_sha256,
            spot_payload_sha256=spot.payload_sha256,
            result=result,
            evidence_sha256=evidence_sha256,
        ))
    batch_sha256 = canonical_sha256({
        "contract": "advanced-options-valuation-batch-v1",
        "candidate_id": package.candidate_id,
        "mark_evidence_batch_id": mark.evidence_batch_id,
        "greeks_evidence": tuple(row.evidence_sha256 for row in greeks),
        "checked_at": checked,
    })
    return AdvancedOptionsValuationBatch(
        "READY",
        (),
        mark,
        tuple(greeks),
        batch_sha256,
    )
