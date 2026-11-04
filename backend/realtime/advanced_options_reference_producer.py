"""Canonical PostgreSQL producer for one Advanced Options reference generation."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from types import SimpleNamespace
from typing import Callable
from uuid import NAMESPACE_URL, UUID, uuid5

from options.calendar import OptionExchangeCalendar

from options.model_inputs import (
    DividendCashFlow,
    RiskFreeRateObservation,
    TREASURY_PRIOR_SESSION_PUBLICATION_POLICY,
    prepare_expiration_valuation_candidate,
)
from psycopg2 import Error as PsycopgError
from psycopg2.extras import RealDictCursor

from .advanced_options_control_plane import (
    AdvancedOptionsEnrollmentPlan,
    advanced_options_enrollment_plan_payload,
)
from .advanced_options_flow_policy import (
    AdvancedOptionsContractScope,
    flow_contract_scope_matches_data_membership,
)
from .advanced_options_reference_repository import (
    persist_advanced_options_reference_state_cursor,
)
from .advanced_options_reference_state import (
    AdvancedOptionsAdjustedDailyBarIdentity,
    AdvancedOptionsEquityMetric,
    AdvancedOptionsOpenInterestReference,
    AdvancedOptionsReferenceState,
    EquityMetricKind,
    ReferenceAvailability,
    build_advanced_options_reference_state,
    build_historical_volatility_fact,
    dividend_coverage_reference_from_window,
    valuation_reference_from_candidate,
)
from .contracts import canonical_json, canonical_sha256
from .database_access_v2 import APP_ROLE, V2ConnectionProvider
from .equity_history import equity_history_coordinates, load_equity_history_cursor
from .reference_repository import load_dividend_reference_cursor


REFERENCE_PRODUCER_VERSION = "advanced-options-reference-producer-v1"
_RETRYABLE_POSTGRES_SQLSTATES = frozenset({"40001", "40P01"})
_TREASURY_SOURCE = "us_treasury_par_yield_v1"
_TREASURY_FRESHNESS_POLICY = TREASURY_PRIOR_SESSION_PUBLICATION_POLICY
_DIVIDEND_MAXIMUM_AGE_SECONDS = 86_400
_OI_SOURCE = "massive"
_OI_DATE_BASIS = "PROVIDER_CHAIN_SNAPSHOT_PRIOR_SESSION"


def _utc(value: datetime, name: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _sha256(value: str, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


class ReferenceProducerStatus(str, Enum):
    READY = "READY"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True, slots=True)
class AdvancedOptionsReferenceProducerEvidence:
    member_kind: str
    member_key: str
    availability: ReferenceAvailability
    evidence_sha256: str
    observed_at: datetime | None = None
    known_at: datetime | None = None
    valid_until: datetime | None = None
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        kind = self.member_kind.strip().upper()
        key = self.member_key.strip()
        reasons = tuple(sorted({
            value.strip().upper()
            for value in self.reason_codes
            if value.strip()
        }))
        if not kind or not key:
            raise ValueError("reference producer evidence identity is invalid")
        if (
            self.availability is ReferenceAvailability.AVAILABLE
            and reasons
        ) or (
            self.availability is ReferenceAvailability.UNAVAILABLE
            and not reasons
        ):
            raise ValueError("reference producer evidence availability is invalid")
        observed = (
            None
            if self.observed_at is None
            else _utc(self.observed_at, "observed_at")
        )
        known = (
            None
            if self.known_at is None
            else _utc(self.known_at, "known_at")
        )
        valid = (
            None
            if self.valid_until is None
            else _utc(self.valid_until, "valid_until")
        )
        if (
            observed is not None
            and known is not None
            and observed > known
        ) or (
            known is not None
            and valid is not None
            and known >= valid
        ):
            raise ValueError("reference producer evidence clocks are not causal")
        object.__setattr__(self, "member_kind", kind)
        object.__setattr__(self, "member_key", key)
        object.__setattr__(
            self,
            "evidence_sha256",
            _sha256(self.evidence_sha256, "evidence_sha256"),
        )
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "known_at", known)
        object.__setattr__(self, "valid_until", valid)
        object.__setattr__(self, "reason_codes", reasons)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsReferenceProducerPlan:
    plan_id: UUID
    plan_sha256: str
    enrollment: AdvancedOptionsEnrollmentPlan
    contract_scope: AdvancedOptionsContractScope
    data_membership_sha256: str
    reference_generation: int
    known_at: datetime
    valid_until: datetime
    discovery_scheduled_cycle: datetime
    prior_oi_session: date
    treasury_freshness_policy: str
    dividend_maximum_age_seconds: int
    model_version: str
    reference_policy_sha256: str
    source_clocks_sha256: str
    history_source_cohort_id: UUID | None = None
    provider_calls_authorized: bool = False
    runtime_actions_authorized: bool = False

    @property
    def control_plane(self):
        return self.enrollment.control_plane

    @property
    def zero_effect_counts(self) -> dict[str, int]:
        return {
            name: 0
            for name in (
                "provider",
                "socket",
                "service",
                "stages_v4",
                "publication",
                "delivery",
                "repair",
                "backfill",
                "replay",
                "archive",
                "retention",
            )
        }


@dataclass(frozen=True, slots=True)
class AdvancedOptionsReferenceProducerPreflight:
    status: ReferenceProducerStatus
    plan_id: UUID
    preflight_sha256: str
    evidence: tuple[AdvancedOptionsReferenceProducerEvidence, ...]
    state: AdvancedOptionsReferenceState | None

    @property
    def reason_codes(self) -> tuple[str, ...]:
        return tuple(sorted({
            reason
            for row in self.evidence
            if not _accepted_unavailable_metric(row)
            for reason in row.reason_codes
        }))


class AdvancedOptionsReferencePreflightBlocked(ValueError):
    def __init__(
        self,
        result: AdvancedOptionsReferenceProducerPreflight,
    ) -> None:
        self.result = result
        super().__init__(
            "Advanced Options reference producer preflight is blocked: "
            + ",".join(result.reason_codes)
        )


def _plan_identity(
    *,
    enrollment: AdvancedOptionsEnrollmentPlan,
    contract_scope: AdvancedOptionsContractScope,
    data_membership_sha256: str,
    reference_generation: int,
    known_at: datetime,
    valid_until: datetime,
    discovery_scheduled_cycle: datetime,
    prior_oi_session: date,
    treasury_freshness_policy: str,
    dividend_maximum_age_seconds: int,
    model_version: str,
    reference_policy_sha256: str,
    source_clocks_sha256: str,
    history_source_cohort_id: UUID | None,
) -> dict[str, object]:
    control = enrollment.control_plane
    return {
        "contract": REFERENCE_PRODUCER_VERSION,
        "enrollment_sha256": enrollment.enrollment_sha256,
        "cohort_id": control.cohort_id,
        "cohort_sha256": control.cohort_sha256,
        "manifest_id": control.manifest_id,
        "manifest_sha256": control.manifest_sha256,
        "profile_id": control.profile_id,
        "profile_sha256": control.profile_sha256,
        "slot_set_version": control.slot_set.version,
        "slot_set_sha256": control.slot_set_sha256,
        "slot_member_sha256": control.slot_set.member_sha256,
        "data_membership_sha256": data_membership_sha256,
        "contract_scope_sha256": contract_scope.sha256,
        "reference_generation": reference_generation,
        "session_date": control.session_date,
        "known_at": known_at,
        "valid_until": valid_until,
        "discovery_scheduled_cycle": discovery_scheduled_cycle,
        "prior_oi_session": prior_oi_session,
        "treasury_freshness_policy": treasury_freshness_policy,
        "dividend_maximum_age_seconds": dividend_maximum_age_seconds,
        "model_version": model_version,
        "model_bundle_sha256": control.pins.model_bundle_sha256,
        "valuation_policy_sha256": control.pins.valuation_policy_sha256,
        "reference_policy_sha256": reference_policy_sha256,
        "source_clocks_sha256": source_clocks_sha256,
        "history_source_cohort_id": history_source_cohort_id,
        "provider_calls_authorized": False,
        "runtime_actions_authorized": False,
    }


def _validate_plan(
    plan: AdvancedOptionsReferenceProducerPlan,
) -> dict[str, object]:
    if not isinstance(plan, AdvancedOptionsReferenceProducerPlan):
        raise TypeError("Advanced Options reference producer plan is invalid")
    advanced_options_enrollment_plan_payload(plan.enrollment)
    control = plan.control_plane
    if (
        plan.data_membership_sha256
        != plan.enrollment.activity_selection.selection_sha256
        or not flow_contract_scope_matches_data_membership(
            plan.contract_scope,
            plan.enrollment.activity_selection.contracts,
            session_date=control.session_date,
            manifest_sha256=control.manifest_sha256,
            slot_sha256=control.slot_set_sha256,
            flow_policy_sha256=control.pins.flow_policy_sha256,
        )
    ):
        raise ValueError("reference producer contract scope changed")
    known = _utc(plan.known_at, "known_at")
    valid = _utc(plan.valid_until, "valid_until")
    discovery = _utc(
        plan.discovery_scheduled_cycle,
        "discovery_scheduled_cycle",
    )
    if (
        type(plan.reference_generation) is not int
        or plan.reference_generation <= 0
        or not isinstance(plan.prior_oi_session, date)
        or plan.prior_oi_session >= control.session_date
        or discovery.date() != control.session_date
        or not control.planned_at <= discovery <= known < valid
        or known > plan.enrollment.activity_selection.valid_until
        or valid > control.expires_at
        or plan.treasury_freshness_policy != _TREASURY_FRESHNESS_POLICY
        or type(plan.dividend_maximum_age_seconds) is not int
        or plan.dividend_maximum_age_seconds <= 0
        or (
            plan.history_source_cohort_id is not None
            and (
                not isinstance(plan.history_source_cohort_id, UUID)
                or plan.history_source_cohort_id.int == 0
                or plan.history_source_cohort_id == control.cohort_id
            )
        )
        or not plan.model_version.strip()
        or plan.provider_calls_authorized
        or plan.runtime_actions_authorized
    ):
        raise ValueError("reference producer clocks or authority changed")
    source_clocks_sha256 = canonical_sha256({
        "session_date": control.session_date,
        "slot_effective_at": control.slot_set.effective_at,
        "discovery_scheduled_cycle": discovery,
        "prior_oi_session": plan.prior_oi_session,
        "known_at": known,
        "valid_until": valid,
    })
    reference_policy_sha256 = canonical_sha256({
        "contract": "advanced-options-reference-policy-v1",
        "enrollment_sha256": plan.enrollment.enrollment_sha256,
        "data_membership_sha256": plan.data_membership_sha256,
        "contract_scope_sha256": plan.contract_scope.sha256,
        "catalog_sha256": control.pins.catalog_sha256,
        "flow_policy_sha256": control.pins.flow_policy_sha256,
        "model_bundle_sha256": control.pins.model_bundle_sha256,
        "valuation_policy_sha256": control.pins.valuation_policy_sha256,
        "treasury_source": _TREASURY_SOURCE,
        "treasury_freshness_policy": plan.treasury_freshness_policy,
        "dividend_maximum_age_seconds": (
            plan.dividend_maximum_age_seconds
        ),
        "oi_source": _OI_SOURCE,
        "oi_date_basis": _OI_DATE_BASIS,
        "model_version": plan.model_version,
    })
    if (
        plan.source_clocks_sha256 != source_clocks_sha256
        or plan.reference_policy_sha256 != reference_policy_sha256
    ):
        raise ValueError("reference producer policy or source clocks changed")
    identity = _plan_identity(
        enrollment=plan.enrollment,
        contract_scope=plan.contract_scope,
        data_membership_sha256=plan.data_membership_sha256,
        reference_generation=plan.reference_generation,
        known_at=known,
        valid_until=valid,
        discovery_scheduled_cycle=discovery,
        prior_oi_session=plan.prior_oi_session,
        treasury_freshness_policy=plan.treasury_freshness_policy,
        dividend_maximum_age_seconds=plan.dividend_maximum_age_seconds,
        model_version=plan.model_version,
        reference_policy_sha256=reference_policy_sha256,
        source_clocks_sha256=source_clocks_sha256,
        history_source_cohort_id=plan.history_source_cohort_id,
    )
    expected_sha256 = canonical_sha256(identity)
    expected_id = uuid5(
        NAMESPACE_URL,
        f"stockscreener:advanced-options-reference-producer:{expected_sha256}",
    )
    if (
        plan.plan_sha256 != expected_sha256
        or plan.plan_id != expected_id
    ):
        raise ValueError("reference producer plan identity changed")
    return identity


def build_advanced_options_reference_producer_plan(
    *,
    enrollment: AdvancedOptionsEnrollmentPlan,
    contract_scope: AdvancedOptionsContractScope,
    reference_generation: int,
    known_at: datetime,
    valid_until: datetime,
    discovery_scheduled_cycle: datetime,
    prior_oi_session: date,
    model_version: str,
    history_source_cohort_id: UUID | None = None,
    treasury_freshness_policy: str = _TREASURY_FRESHNESS_POLICY,
    dividend_maximum_age_seconds: int = _DIVIDEND_MAXIMUM_AGE_SECONDS,
) -> AdvancedOptionsReferenceProducerPlan:
    advanced_options_enrollment_plan_payload(enrollment)
    known = _utc(known_at, "known_at")
    valid = _utc(valid_until, "valid_until")
    discovery = _utc(
        discovery_scheduled_cycle,
        "discovery_scheduled_cycle",
    )
    source_clocks_sha256 = canonical_sha256({
        "session_date": enrollment.control_plane.session_date,
        "slot_effective_at": (
            enrollment.control_plane.slot_set.effective_at
        ),
        "discovery_scheduled_cycle": discovery,
        "prior_oi_session": prior_oi_session,
        "known_at": known,
        "valid_until": valid,
    })
    data_membership_sha256 = (
        enrollment.activity_selection.selection_sha256
    )
    reference_policy_sha256 = canonical_sha256({
        "contract": "advanced-options-reference-policy-v1",
        "enrollment_sha256": enrollment.enrollment_sha256,
        "data_membership_sha256": data_membership_sha256,
        "contract_scope_sha256": contract_scope.sha256,
        "catalog_sha256": enrollment.control_plane.pins.catalog_sha256,
        "flow_policy_sha256": (
            enrollment.control_plane.pins.flow_policy_sha256
        ),
        "model_bundle_sha256": (
            enrollment.control_plane.pins.model_bundle_sha256
        ),
        "valuation_policy_sha256": (
            enrollment.control_plane.pins.valuation_policy_sha256
        ),
        "treasury_source": _TREASURY_SOURCE,
        "treasury_freshness_policy": treasury_freshness_policy,
        "dividend_maximum_age_seconds": dividend_maximum_age_seconds,
        "oi_source": _OI_SOURCE,
        "oi_date_basis": _OI_DATE_BASIS,
        "model_version": model_version,
    })
    identity = _plan_identity(
        enrollment=enrollment,
        contract_scope=contract_scope,
        data_membership_sha256=data_membership_sha256,
        reference_generation=reference_generation,
        known_at=known,
        valid_until=valid,
        discovery_scheduled_cycle=discovery,
        prior_oi_session=prior_oi_session,
        treasury_freshness_policy=treasury_freshness_policy,
        dividend_maximum_age_seconds=dividend_maximum_age_seconds,
        model_version=model_version,
        reference_policy_sha256=reference_policy_sha256,
        source_clocks_sha256=source_clocks_sha256,
        history_source_cohort_id=history_source_cohort_id,
    )
    plan_sha256 = canonical_sha256(identity)
    plan = AdvancedOptionsReferenceProducerPlan(
        plan_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:advanced-options-reference-producer:{plan_sha256}",
        ),
        plan_sha256=plan_sha256,
        enrollment=enrollment,
        contract_scope=contract_scope,
        data_membership_sha256=data_membership_sha256,
        reference_generation=reference_generation,
        known_at=known,
        valid_until=valid,
        discovery_scheduled_cycle=discovery,
        prior_oi_session=prior_oi_session,
        treasury_freshness_policy=treasury_freshness_policy,
        dividend_maximum_age_seconds=dividend_maximum_age_seconds,
        model_version=model_version.strip(),
        reference_policy_sha256=reference_policy_sha256,
        source_clocks_sha256=source_clocks_sha256,
        history_source_cohort_id=history_source_cohort_id,
    )
    _validate_plan(plan)
    return plan


def advanced_options_reference_producer_plan_payload(
    plan: AdvancedOptionsReferenceProducerPlan,
) -> dict[str, object]:
    identity = _validate_plan(plan)
    return {
        "contract_version": REFERENCE_PRODUCER_VERSION,
        "plan_id": str(plan.plan_id),
        "plan_sha256": plan.plan_sha256,
        "identity": json.loads(canonical_json(identity)),
        "effect_counts": plan.zero_effect_counts,
    }


def _evidence(
    kind: str,
    key: str,
    availability: ReferenceAvailability,
    payload: object,
    *,
    observed_at: datetime | None = None,
    known_at: datetime | None = None,
    valid_until: datetime | None = None,
    reason_codes: tuple[str, ...] = (),
) -> AdvancedOptionsReferenceProducerEvidence:
    return AdvancedOptionsReferenceProducerEvidence(
        member_kind=kind,
        member_key=key,
        availability=availability,
        evidence_sha256=canonical_sha256(payload),
        observed_at=observed_at,
        known_at=known_at,
        valid_until=valid_until,
        reason_codes=reason_codes,
    )


def _accepted_unavailable_metric(
    row: AdvancedOptionsReferenceProducerEvidence,
) -> bool:
    accepted = {
        EquityMetricKind.TOTAL_FLOAT.value: "TOTAL_FLOAT_UNAVAILABLE",
        EquityMetricKind.SHORT_INTEREST_PERCENT.value: (
            "SHORT_INTEREST_PERCENT_UNAVAILABLE"
        ),
    }
    metric = row.member_key.rsplit(":", 1)[-1]
    reason = accepted.get(metric)
    return (
        row.member_kind == "EQUITY_METRIC"
        and row.availability is ReferenceAvailability.UNAVAILABLE
        and reason is not None
        and row.reason_codes == (reason,)
    )


def _result(
    plan: AdvancedOptionsReferenceProducerPlan,
    evidence: list[AdvancedOptionsReferenceProducerEvidence],
    state: AdvancedOptionsReferenceState | None,
) -> AdvancedOptionsReferenceProducerPreflight:
    ordered = tuple(sorted(
        evidence,
        key=lambda row: (row.member_kind, row.member_key),
    ))
    status = (
        ReferenceProducerStatus.READY
        if state is not None
        and all(
            row.availability is ReferenceAvailability.AVAILABLE
            or _accepted_unavailable_metric(row)
            for row in ordered
        )
        else ReferenceProducerStatus.BLOCKED
    )
    digest = canonical_sha256({
        "contract": "advanced-options-reference-preflight-v1",
        "plan_sha256": plan.plan_sha256,
        "status": status,
        "evidence": ordered,
        "state_id": None if state is None else state.state_id,
        "state_sha256": None if state is None else state.state_sha256,
    })
    return AdvancedOptionsReferenceProducerPreflight(
        status=status,
        plan_id=plan.plan_id,
        preflight_sha256=digest,
        evidence=ordered,
        state=state,
    )


def _load_active_scope(
    cursor,
    plan: AdvancedOptionsReferenceProducerPlan,
    evidence: list[AdvancedOptionsReferenceProducerEvidence],
):
    control = plan.control_plane
    cursor.execute(
        "SELECT cohort.cohort_sha256,cohort.manifest_id::text,"
        "cohort.configuration_sha256,cohort.model_bundle_sha256,"
        "cohort.valuation_sha256,cohort.mode,"
        "manifest.manifest_sha256,manifest.universe_sha256,"
        "manifest.profile_id::text,manifest.manifest_kind,"
        "manifest.option_underlying_count,profile.profile_sha256,"
        "profile.provider_tier,current.version,current.activated_at,"
        "version.member_sha256 "
        "FROM rt.cohorts AS cohort "
        "JOIN rt.subscription_manifests AS manifest "
        "ON manifest.manifest_id=cohort.manifest_id "
        "JOIN rt.capability_profiles AS profile "
        "ON profile.profile_id=manifest.profile_id "
        "LEFT JOIN rt.slot_set_current AS current "
        "ON current.cohort_id=cohort.cohort_id "
        "LEFT JOIN rt.slot_set_versions AS version "
        "ON version.cohort_id=current.cohort_id "
        "AND version.version=current.version "
        "WHERE cohort.cohort_id=%s",
        (str(control.cohort_id),),
    )
    header = cursor.fetchone()
    expected = (
        control.cohort_sha256,
        str(control.manifest_id),
        (
            plan.enrollment.enrollment_sha256
            if control.stored_identity is None
            else control.stored_identity.configuration_sha256
        ),
        control.pins.model_bundle_sha256,
        control.pins.valuation_policy_sha256,
        (
            "SHADOW"
            if control.stored_identity is None
            else control.stored_identity.mode
        ),
        control.manifest_sha256,
        control.universe.sha256,
        str(control.profile_id),
        "MARKET_OPTIONS",
        len(control.universe.option_underlyings),
        control.profile_sha256,
        "MASSIVE_ADVANCED",
        control.slot_set.version,
        control.slot_set.effective_at,
        control.slot_set.member_sha256,
    )
    actual = None if header is None else tuple(header.values())
    if actual != expected:
        raise ValueError("stored reference producer cohort or slot changed")
    evidence.append(_evidence(
        "CONTROL_PLANE",
        str(control.cohort_id),
        ReferenceAvailability.AVAILABLE,
        actual,
        observed_at=control.slot_set.effective_at,
        known_at=plan.known_at,
        valid_until=plan.valid_until,
    ))
    cursor.execute(
        "SELECT member.instrument_id,member.channel,member.purpose,"
        "member.priority,member.catalog_version_id,contract.contract_id,"
        "contract.contract_ticker,contract.underlying_security_id::text,"
        "version.expiration,version.eligibility,version.effective_from,"
        "version.effective_to,version.observed_at,version.payload_sha256 "
        "FROM rt.slot_set_current AS current "
        "JOIN rt.slot_set_members AS member "
        "ON member.cohort_id=current.cohort_id "
        "AND member.version=current.version "
        "JOIN rt.instruments AS instrument "
        "ON instrument.instrument_id=member.instrument_id "
        "JOIN ref.option_contracts AS contract "
        "ON contract.contract_id=instrument.contract_id "
        "JOIN ref.option_contract_versions AS version "
        "ON version.version_id=member.catalog_version_id "
        "AND version.contract_id=contract.contract_id "
        "WHERE current.cohort_id=%s "
        "ORDER BY contract.contract_ticker,member.channel",
        (str(control.cohort_id),),
    )
    rows = tuple(cursor.fetchall())
    selected = {
        row.contract_ticker: row
        for row in plan.enrollment.activity_selection.contracts
    }
    grouped: dict[str, list] = {}
    for row in rows:
        grouped.setdefault(str(row["contract_ticker"]), []).append(row)
    if set(grouped) != set(selected):
        raise ValueError("active reference contract membership changed")
    contracts = []
    for ticker in sorted(grouped):
        members = grouped[ticker]
        selected_row = selected[ticker]
        first = members[0]
        if (
            len(members) != 2
            or {str(row["channel"]) for row in members} != {"T", "Q"}
            or {str(row["purpose"]) for row in members} != {"PRIMARY"}
            or any(
                int(row["instrument_id"]) != selected_row.instrument_id
                or int(row["contract_id"]) != selected_row.contract_id
                or int(row["catalog_version_id"])
                != selected_row.catalog_version_id
                for row in members
            )
            or str(first["eligibility"]) != "VALIDATED_ACTIVE"
            or first["effective_from"] > plan.known_at
            or (
                first["effective_to"] is not None
                and first["effective_to"] <= plan.known_at
            )
            or first["observed_at"] > plan.known_at
        ):
            raise ValueError("active reference contract identity or clocks changed")
        contracts.append(first)
        evidence.append(_evidence(
            "ACTIVE_CONTRACT",
            ticker,
            ReferenceAvailability.AVAILABLE,
            tuple(tuple(row.values()) for row in members),
            observed_at=first["observed_at"],
            known_at=plan.known_at,
            valid_until=plan.valid_until,
        ))
    cursor.execute(
        "SELECT contract_scope_sha256,scope_version,session_date,"
        "manifest_sha256,slot_sha256,advanced_flow_policy_sha256 "
        "FROM rt.option_flow_contract_scopes "
        "WHERE contract_scope_sha256=%s",
        (plan.contract_scope.sha256,),
    )
    scope_header = cursor.fetchone()
    cursor.execute(
        "SELECT contract_ticker,underlyer,expiration,universe_tier,dte_lane "
        "FROM rt.option_flow_contract_scope_members "
        "WHERE contract_scope_sha256=%s ORDER BY contract_ticker",
        (plan.contract_scope.sha256,),
    )
    scope_members = tuple(cursor.fetchall())
    expected_scope_header = (
        plan.contract_scope.sha256,
        plan.contract_scope.version,
        plan.contract_scope.session_date,
        plan.contract_scope.manifest_sha256,
        plan.contract_scope.slot_sha256,
        plan.contract_scope.flow_policy_sha256,
    )
    expected_scope_members = tuple(
        (
            row.contract_ticker,
            row.underlyer,
            row.expiration,
            row.universe_tier.value,
            row.dte_lane.value,
        )
        for row in plan.contract_scope.contracts
    )
    if (
        scope_header is None
        or tuple(scope_header.values()) != expected_scope_header
        or tuple(tuple(row.values()) for row in scope_members)
        != expected_scope_members
    ):
        raise ValueError("stored reference contract scope changed")
    evidence.append(_evidence(
        "CONTRACT_SCOPE",
        plan.contract_scope.sha256,
        ReferenceAvailability.AVAILABLE,
        (expected_scope_header, expected_scope_members),
        known_at=plan.known_at,
        valid_until=plan.valid_until,
    ))
    return tuple(contracts)


_OI_SEMANTIC_FIELDS = (
    "contract_id",
    "settlement_session",
    "open_interest",
    "source",
    "date_basis",
    "observed_session",
)

_OI_SCOPE_FIELDS = tuple(
    name for name in _OI_SEMANTIC_FIELDS if name != "open_interest"
)


def _coalesce_terminal_open_interest(candidates, observed_session):
    superseded = {
        str(row["supersedes_baseline_id"])
        for row in candidates
        if row["supersedes_baseline_id"] is not None
    }
    terminal = tuple(
        row
        for row in candidates
        if str(row["baseline_id"]) not in superseded
        and row["observed_session"] == observed_session
    )
    if len(terminal) <= 1:
        return terminal
    scopes = {
        tuple(row[name] for name in _OI_SCOPE_FIELDS)
        for row in terminal
    }
    if len(scopes) != 1:
        return terminal
    latest_clock = max(
        (row["received_at"], row["observed_at"])
        for row in terminal
    )
    latest = tuple(
        row
        for row in terminal
        if (row["received_at"], row["observed_at"]) == latest_clock
    )
    if len(latest) <= 1:
        return latest
    identities = {
        tuple(row[name] for name in _OI_SEMANTIC_FIELDS)
        for row in latest
    }
    if len(identities) != 1:
        return latest
    return (max(
        latest,
        key=lambda row: str(row["baseline_id"]),
    ),)


def _load_open_interest(
    cursor,
    plan: AdvancedOptionsReferenceProducerPlan,
    contracts,
    evidence: list[AdvancedOptionsReferenceProducerEvidence],
):
    contract_ids = [int(row["contract_id"]) for row in contracts]
    cursor.execute(
        "SELECT baseline_id::text,contract_id,settlement_session,"
        "open_interest,source,date_basis,observed_session,observed_at,"
        "received_at,payload_sha256,supersedes_baseline_id::text "
        "FROM ref.oi_baselines WHERE contract_id=ANY(%s) "
        "AND observed_at<=%s AND received_at<=%s "
        "ORDER BY contract_id,received_at,observed_at,baseline_id",
        (contract_ids, plan.known_at, plan.known_at),
    )
    rows = tuple(cursor.fetchall())
    by_contract: dict[int, list] = {}
    for row in rows:
        by_contract.setdefault(int(row["contract_id"]), []).append(row)
    data_contracts = {
        row.contract_ticker: row
        for row in plan.enrollment.activity_selection.contracts
    }
    references = []
    missing = False
    for contract in contracts:
        contract_id = int(contract["contract_id"])
        candidates = by_contract.get(contract_id, [])
        terminal = _coalesce_terminal_open_interest(
            candidates,
            plan.control_plane.session_date,
        )
        if len(terminal) != 1:
            missing = True
            evidence.append(_evidence(
                "DATED_OPEN_INTEREST",
                str(contract_id),
                ReferenceAvailability.UNAVAILABLE,
                tuple(tuple(row.values()) for row in candidates),
                known_at=plan.known_at,
                valid_until=plan.valid_until,
                reason_codes=(
                    "DATED_OI_MISSING"
                    if not terminal
                    else "DATED_OI_AMBIGUOUS_OR_SUPERSEDED"
                ,),
            ))
            continue
        row = terminal[0]
        if (
            row["settlement_session"] != plan.prior_oi_session
            or str(row["source"]) != _OI_SOURCE
            or str(row["date_basis"]) != _OI_DATE_BASIS
            or not row["observed_at"] <= row["received_at"] <= plan.known_at
            or (
                row["supersedes_baseline_id"] is not None
                and str(row["supersedes_baseline_id"])
                not in {str(value["baseline_id"]) for value in candidates}
            )
        ):
            raise ValueError("dated OI scope, supersession, or clocks changed")
        data_contract = data_contracts[str(contract["contract_ticker"])]
        reference = AdvancedOptionsOpenInterestReference(
            contract_id=contract_id,
            contract_ticker=str(contract["contract_ticker"]),
            underlyer=data_contract.underlyer,
            availability=ReferenceAvailability.AVAILABLE,
            known_at=plan.known_at,
            baseline_id=UUID(str(row["baseline_id"])),
            settlement_session=row["settlement_session"],
            observed_session=row["observed_session"],
            open_interest=int(row["open_interest"]),
            source=str(row["source"]),
            date_basis=str(row["date_basis"]),
            observed_at=row["observed_at"],
            received_at=row["received_at"],
            payload_sha256=str(row["payload_sha256"]),
            supersedes_baseline_id=(
                None
                if row["supersedes_baseline_id"] is None
                else UUID(str(row["supersedes_baseline_id"]))
            ),
        )
        references.append(reference)
        evidence.append(_evidence(
            "DATED_OPEN_INTEREST",
            str(contract_id),
            ReferenceAvailability.AVAILABLE,
            reference,
            observed_at=reference.observed_at,
            known_at=plan.known_at,
            valid_until=plan.valid_until,
        ))
    return tuple(references), missing


def _load_treasury_curve(
    cursor,
    plan: AdvancedOptionsReferenceProducerPlan,
    evidence: list[AdvancedOptionsReferenceProducerEvidence],
):
    cursor.execute(
        "WITH eligible AS ("
        "SELECT * FROM ref.risk_free_rate_observations "
        "WHERE source=%s AND rate_date<=%s "
        "AND source_observed_at IS NOT NULL AND received_at<=%s"
        "), latest AS (SELECT MAX(rate_date) AS rate_date FROM eligible) "
        "SELECT DISTINCT ON (tenor_days) eligible.* "
        "FROM eligible JOIN latest USING(rate_date) "
        "ORDER BY tenor_days,received_at DESC,observation_id LIMIT 65",
        (_TREASURY_SOURCE, plan.control_plane.session_date, plan.known_at),
    )
    rows = tuple(cursor.fetchall())
    if not rows:
        evidence.append(_evidence(
            "TREASURY_CURVE",
            plan.control_plane.session_date.isoformat(),
            ReferenceAvailability.UNAVAILABLE,
            (),
            known_at=plan.known_at,
            valid_until=plan.valid_until,
            reason_codes=("TREASURY_CURVE_MISSING",),
        ))
        return (), None
    rate_date = rows[0]["rate_date"]
    expected_rate_date = OptionExchangeCalendar().previous_session_date(
        plan.control_plane.session_date
    )
    if (
        len(rows) > 64
        or rate_date != expected_rate_date
        or len({int(row["tenor_days"]) for row in rows}) != len(rows)
        or any(
            row["rate_date"] != rate_date
            or str(row["source_key"])
            != f"{rate_date.isoformat()}:{int(row['tenor_days'])}d"
            or not row["source_observed_at"]
            <= row["received_at"]
            <= plan.known_at
            for row in rows
        )
    ):
        raise ValueError("Treasury curve identity, freshness, or clocks changed")
    curve = tuple(
        RiskFreeRateObservation(
            rate_observation_id=UUID(str(row["observation_id"])),
            rate_date=row["rate_date"],
            tenor_days=int(row["tenor_days"]),
            annual_rate=float(row["annual_rate"]),
            source=str(row["source"]),
            source_key=str(row["source_key"]),
            source_observed_at=row["source_observed_at"],
            first_observed_at=row["received_at"],
            availability_mode="LIVE_OBSERVED",
            replay_available_at=None,
            payload_sha256=str(row["payload_sha256"]),
        )
        for row in rows
    )
    evidence.append(_evidence(
        "TREASURY_CURVE",
        rate_date.isoformat(),
        ReferenceAvailability.AVAILABLE,
        curve,
        observed_at=max(row.source_observed_at for row in curve),
        known_at=plan.known_at,
        valid_until=plan.valid_until,
    ))
    return curve, rate_date


def _load_discovery_spots(
    cursor,
    plan: AdvancedOptionsReferenceProducerPlan,
    contracts,
    evidence: list[AdvancedOptionsReferenceProducerEvidence],
):
    control = plan.control_plane
    cursor.execute(
        "SELECT discovery_batch_id::text,underlying_security_id::text,"
        "profile_id::text,profile_sha256,universe_sha256,provider_tier,"
        "provider,underlyer,scheduled_cycle,expiration_from,"
        "expiration_through,spot_price,policy_sha256,started_at,"
        "completed_at,evidence_sha256 FROM rt.option_discovery_batches "
        "WHERE profile_id=%s AND scheduled_cycle=%s "
        "AND underlyer=ANY(%s) ORDER BY underlyer,discovery_batch_id",
        (
            str(control.profile_id),
            plan.discovery_scheduled_cycle,
            list(control.universe.option_underlyings),
        ),
    )
    rows = tuple(cursor.fetchall())
    if len(rows) != len(control.universe.option_underlyings):
        evidence.append(_evidence(
            "DISCOVERY_SPOT",
            "ACTIVE_UNDERLYINGS",
            ReferenceAvailability.UNAVAILABLE,
            tuple(tuple(row.values()) for row in rows),
            known_at=plan.known_at,
            valid_until=plan.valid_until,
            reason_codes=("PRE_SESSION_DISCOVERY_SCOPE_MISSING",),
        ))
        return {}
    expirations: dict[str, list[date]] = {}
    data_contracts = {
        row.contract_ticker: row
        for row in plan.enrollment.activity_selection.contracts
    }
    for contract in contracts:
        entry = data_contracts[
            str(contract["contract_ticker"])
        ]
        expirations.setdefault(entry.underlyer, []).append(
            entry.expiration_date
        )
    spots = {}
    for row in rows:
        underlyer = str(row["underlyer"])
        if (
            underlyer in spots
            or str(row["profile_id"]) != str(control.profile_id)
            or str(row["profile_sha256"]) != control.profile_sha256
            or str(row["universe_sha256"]) != control.universe.sha256
            or str(row["provider_tier"]) != "MASSIVE_ADVANCED"
            or str(row["provider"]) != "massive"
            or str(row["policy_sha256"]) != control.manifest_policy.sha256
            or row["completed_at"] > plan.known_at
            or row["expiration_from"] > min(expirations[underlyer])
            or row["expiration_through"] < max(expirations[underlyer])
        ):
            raise ValueError("pre-session discovery identity or clocks changed")
        spots[underlyer] = (
            Decimal(row["spot_price"]),
            UUID(str(row["underlying_security_id"])),
        )
        evidence.append(_evidence(
            "DISCOVERY_SPOT",
            underlyer,
            ReferenceAvailability.AVAILABLE,
            tuple(row.values()),
            observed_at=row["completed_at"],
            known_at=plan.known_at,
            valid_until=plan.valid_until,
        ))
    return spots


def _load_bars_and_metrics(
    cursor,
    plan: AdvancedOptionsReferenceProducerPlan,
    spots,
    evidence: list[AdvancedOptionsReferenceProducerEvidence],
):
    volatility = []
    metrics = []
    missing = False
    required_sessions = OptionExchangeCalendar().trailing_sessions_before(
        plan.control_plane.session_date,
        31,
    )
    for ticker in plan.control_plane.universe.option_underlyings:
        security_id = spots[ticker][1]
        history = load_equity_history_cursor(
            cursor,
            cohort_id=(
                plan.history_source_cohort_id
                or plan.enrollment.equity_plan.cohort_id
            ),
            security_id=security_id,
            ticker=ticker,
            interval="1d",
            adjusted=True,
            sessions=required_sessions,
            checked_at=plan.known_at,
        )
        rows = history.rows
        if (
            len(rows) != 31
            or len({row["session_date"] for row in rows}) != 31
            or tuple(row["session_date"] for row in rows)
            != required_sessions
            or any(row["stale_adjustment_lineage"] for row in rows)
            or any(
                (row["bar_start"], row["bar_end"])
                != equity_history_coordinates(
                    OptionExchangeCalendar(), "1d", row["session_date"],
                )[0]
                for row in rows
            )
        ):
            missing = True
            evidence.append(_evidence(
                "HISTORICAL_VOLATILITY",
                f"{ticker}:HV20/HV30",
                ReferenceAvailability.UNAVAILABLE,
                tuple(tuple(row.values()) for row in rows),
                known_at=plan.known_at,
                valid_until=plan.valid_until,
                reason_codes=("FINAL_ADJUSTED_DAILY_BAR_COVERAGE_MISSING",),
            ))
            continue
        bars = tuple(
            AdvancedOptionsAdjustedDailyBarIdentity(
                bar_revision_id=UUID(str(row["bar_revision_id"])),
                security_id=security_id,
                ticker=ticker,
                session_date=row["session_date"],
                adjusted_close=Decimal(row["close_price"]),
                market_close_at=row["bar_end"],
                known_at=row["observed_at"],
                adjustment_lineage_sha256=canonical_sha256({
                    "source_kind": row["source_kind"],
                    "adjustment_basis": row["adjustment_basis"],
                    "source_revision_ids": tuple(row["source_revision_ids"]),
                    "supersedes_revision_id": row["supersedes_revision_id"],
                }),
                finality_sha256=canonical_sha256({
                    "bar_revision_id": row["bar_revision_id"],
                    "is_final": row["is_final"],
                    "bar_end": row["bar_end"],
                    "observed_at": row["observed_at"],
                    "current_updated_at": row["updated_at"],
                    "history_binding_sha256": history.binding_sha256,
                }),
                payload_sha256=str(row["payload_sha256"]),
            )
            for row in rows
        )
        for window, members in ((20, bars[-21:]), (30, bars)):
            fact = build_historical_volatility_fact(
                security_id=security_id,
                ticker=ticker,
                window_sessions=window,
                market_cutoff=members[-1].market_close_at,
                known_at=plan.known_at,
                bars=members,
            )
            volatility.append(fact)
            evidence.append(_evidence(
                "HISTORICAL_VOLATILITY",
                f"{ticker}:HV{window}",
                ReferenceAvailability.AVAILABLE,
                fact,
                observed_at=max(row.known_at for row in members),
                known_at=plan.known_at,
                valid_until=plan.valid_until,
            ))
        adv_rows = rows[-30:]
        adv_sessions = tuple(row["session_date"] for row in adv_rows)
        adv_member_sha256 = canonical_sha256(tuple(
            (
                row["bar_revision_id"],
                str(row["volume"]),
                row["payload_sha256"],
            )
            for row in adv_rows
        ))
        adv_metric = AdvancedOptionsEquityMetric(
            security_id=security_id,
            ticker=ticker,
            metric_kind=EquityMetricKind.AVERAGE_DAILY_VOLUME_30,
            as_of_session=adv_sessions[-1],
            availability=ReferenceAvailability.AVAILABLE,
            known_at=plan.known_at,
            value=sum(
                (Decimal(row["volume"]) for row in adv_rows),
                Decimal("0"),
            ) / Decimal("30"),
            source="rt.bar_current",
            source_key=f"{ticker}:ADJUSTED:1d:ADV30",
            observed_at=max(row["observed_at"] for row in adv_rows),
            received_at=max(row["observed_at"] for row in adv_rows),
            payload_sha256=adv_member_sha256,
            calculation_version="average-daily-volume-30-v1",
            member_sessions=adv_sessions,
            member_sha256=adv_member_sha256,
        )
        metrics.append(adv_metric)
        evidence.append(_evidence(
            "EQUITY_METRIC",
            f"{ticker}:{adv_metric.metric_kind.value}",
            ReferenceAvailability.AVAILABLE,
            adv_metric,
            observed_at=adv_metric.observed_at,
            known_at=plan.known_at,
            valid_until=plan.valid_until,
        ))
        cursor.execute(
            "SELECT revision_id::text,observed_at,payload_sha256,free_float "
            "FROM ref.security_revisions WHERE security_id=%s "
            "AND ticker=%s AND effective_from<=%s AND observed_at<=%s "
            "AND (effective_to IS NULL OR effective_to>%s) "
            "ORDER BY effective_from DESC,observed_at DESC,revision_id LIMIT 2",
            (
                str(security_id),
                ticker,
                plan.known_at,
                plan.known_at,
                plan.known_at,
            ),
        )
        security_rows = tuple(cursor.fetchall())
        float_rows = tuple(
            row for row in security_rows if row["free_float"] is not None
        )
        if not float_rows:
            metric = AdvancedOptionsEquityMetric(
                security_id=security_id,
                ticker=ticker,
                metric_kind=EquityMetricKind.TOTAL_FLOAT,
                as_of_session=adv_sessions[-1],
                availability=ReferenceAvailability.UNAVAILABLE,
                known_at=plan.known_at,
                reason_codes=("TOTAL_FLOAT_UNAVAILABLE",),
            )
            metrics.append(metric)
            evidence.append(_evidence(
                "EQUITY_METRIC",
                f"{ticker}:{EquityMetricKind.TOTAL_FLOAT.value}",
                ReferenceAvailability.UNAVAILABLE,
                metric,
                known_at=plan.known_at,
                valid_until=plan.valid_until,
                reason_codes=("TOTAL_FLOAT_UNAVAILABLE",),
            ))
        else:
            row = float_rows[0]
            metric = AdvancedOptionsEquityMetric(
                security_id=security_id,
                ticker=ticker,
                metric_kind=EquityMetricKind.TOTAL_FLOAT,
                as_of_session=adv_sessions[-1],
                availability=ReferenceAvailability.AVAILABLE,
                known_at=plan.known_at,
                value=Decimal(row["free_float"]),
                source="ref.security_revisions",
                source_key=str(row["revision_id"]),
                observed_at=row["observed_at"],
                received_at=row["observed_at"],
                payload_sha256=str(row["payload_sha256"]),
            )
            metrics.append(metric)
            evidence.append(_evidence(
                "EQUITY_METRIC",
                f"{ticker}:{metric.metric_kind.value}",
                ReferenceAvailability.AVAILABLE,
                metric,
                observed_at=metric.observed_at,
                known_at=plan.known_at,
                valid_until=plan.valid_until,
            ))
        cursor.execute(
            "SELECT report_id::text,observed_at,payload_sha256,"
            "metrics->>'short_interest_percent' AS value "
            "FROM ref.fundamental_reports WHERE security_id=%s "
            "AND availability_time<=%s AND observed_at<=%s "
            "AND metrics ? 'short_interest_percent' "
            "ORDER BY availability_time DESC,observed_at DESC,report_id "
            "LIMIT 2",
            (str(security_id), plan.known_at, plan.known_at),
        )
        report_rows = tuple(cursor.fetchall())
        if not report_rows:
            metric = AdvancedOptionsEquityMetric(
                security_id=security_id,
                ticker=ticker,
                metric_kind=EquityMetricKind.SHORT_INTEREST_PERCENT,
                as_of_session=adv_sessions[-1],
                availability=ReferenceAvailability.UNAVAILABLE,
                known_at=plan.known_at,
                reason_codes=("SHORT_INTEREST_PERCENT_UNAVAILABLE",),
            )
            metrics.append(metric)
            evidence.append(_evidence(
                "EQUITY_METRIC",
                f"{ticker}:{EquityMetricKind.SHORT_INTEREST_PERCENT.value}",
                ReferenceAvailability.UNAVAILABLE,
                metric,
                known_at=plan.known_at,
                valid_until=plan.valid_until,
                reason_codes=("SHORT_INTEREST_PERCENT_UNAVAILABLE",),
            ))
        else:
            row = report_rows[0]
            metric = AdvancedOptionsEquityMetric(
                security_id=security_id,
                ticker=ticker,
                metric_kind=EquityMetricKind.SHORT_INTEREST_PERCENT,
                as_of_session=adv_sessions[-1],
                availability=ReferenceAvailability.AVAILABLE,
                known_at=plan.known_at,
                value=Decimal(str(row["value"])),
                source="ref.fundamental_reports",
                source_key=str(row["report_id"]),
                observed_at=row["observed_at"],
                received_at=row["observed_at"],
                payload_sha256=str(row["payload_sha256"]),
            )
            metrics.append(metric)
            evidence.append(_evidence(
                "EQUITY_METRIC",
                f"{ticker}:{metric.metric_kind.value}",
                ReferenceAvailability.AVAILABLE,
                metric,
                observed_at=metric.observed_at,
                known_at=plan.known_at,
                valid_until=plan.valid_until,
            ))
    return tuple(volatility), tuple(metrics), missing


def _load_valuations(
    cursor,
    plan: AdvancedOptionsReferenceProducerPlan,
    contracts,
    spots,
    curve,
    rate_date,
    evidence: list[AdvancedOptionsReferenceProducerEvidence],
):
    valuations = []
    missing = False
    by_underlyer: dict[str, set[date]] = {}
    data_contracts = {
        row.contract_ticker: row
        for row in plan.enrollment.activity_selection.contracts
    }
    for contract in contracts:
        entry = data_contracts[
            str(contract["contract_ticker"])
        ]
        by_underlyer.setdefault(entry.underlyer, set()).add(
            entry.expiration_date
        )
    for ticker in plan.control_plane.universe.option_underlyings:
        spot, security_id = spots[ticker]
        expirations = tuple(sorted(by_underlyer[ticker]))
        maximum_expiration = max(expirations)
        window = load_dividend_reference_cursor(
            cursor,
            security_id=security_id,
            ticker=ticker,
            window_start=plan.control_plane.session_date,
            window_end=maximum_expiration,
            known_by=plan.known_at,
        )
        if window.status not in {
            "AVAILABLE",
            "KNOWN_NO_ANNOUNCED_DIVIDEND",
        }:
            missing = True
            evidence.append(_evidence(
                "DIVIDEND_COVERAGE",
                ticker,
                ReferenceAvailability.UNAVAILABLE,
                window,
                known_at=plan.known_at,
                valid_until=plan.valid_until,
                reason_codes=(
                    window.reason_codes
                    or ("DIVIDEND_COVERAGE_UNAVAILABLE",)
                ),
            ))
            continue
        coverage = dividend_coverage_reference_from_window(window)
        evidence.append(_evidence(
            "DIVIDEND_COVERAGE",
            ticker,
            ReferenceAvailability.AVAILABLE,
            coverage,
            observed_at=coverage.observed_at,
            known_at=plan.known_at,
            valid_until=plan.valid_until,
        ))
        evidence.append(_evidence(
            "CORPORATE_ACTION_COVERAGE",
            ticker,
            ReferenceAvailability.AVAILABLE,
            (
                coverage.coverage_id,
                coverage.response_sha256,
                tuple(row.sha256 for row in coverage.actions),
            ),
            observed_at=coverage.observed_at,
            known_at=plan.known_at,
            valid_until=plan.valid_until,
        ))
        flows = tuple(
            DividendCashFlow(
                row.ex_dividend_date,
                row.split_adjusted_cash_amount
                or row.original_cash_amount,
            )
            for row in window.dividends
            if (
                row.split_adjusted_cash_amount
                or row.original_cash_amount
            ) is not None
        )
        for expiration in expirations:
            candidate = prepare_expiration_valuation_candidate(
                curve=curve,
                expected_rate_date=rate_date,
                valuation_date=plan.control_plane.session_date,
                expiration_date=expiration,
                spot=spot,
                selected_at=plan.known_at,
                maximum_age_seconds=plan.dividend_maximum_age_seconds,
                cash_flows=tuple(
                    row for row in flows if row.ex_date <= expiration
                ),
                dividend_coverage_start=window.window_start,
                dividend_coverage_end=window.window_end,
                dividend_coverage_complete=True,
                dividend_received_at=coverage.received_at,
                dividend_source_key=(
                    f"{ticker}:DIVIDEND:{coverage.coverage_id}"
                ),
                dividend_payload_sha256=coverage.payload_sha256,
                treasury_freshness_policy=(
                    plan.treasury_freshness_policy
                ),
                dividend_maximum_age_seconds=(
                    plan.dividend_maximum_age_seconds
                ),
            )
            valuation = valuation_reference_from_candidate(
                candidate,
                security_id=security_id,
                ticker=ticker,
                curve=curve,
                dividend_coverage=coverage,
                model_version=plan.model_version,
                model_bundle_sha256=(
                    plan.control_plane.pins.model_bundle_sha256
                ),
                valuation_policy_sha256=(
                    plan.control_plane.pins.valuation_policy_sha256
                ),
            )
            valuations.append(valuation)
            evidence.append(_evidence(
                "VALUATION_MODEL_INPUT",
                f"{ticker}:{expiration.isoformat()}",
                ReferenceAvailability.AVAILABLE,
                valuation,
                observed_at=max(
                    coverage.observed_at,
                    *(row.observed_at for row in valuation.rate_observations),
                ),
                known_at=plan.known_at,
                valid_until=plan.valid_until,
            ))
    return tuple(valuations), missing


def load_opening_reference_inputs_cursor(
    cursor,
    *,
    session_date: date,
    known_at: datetime,
    valid_until: datetime,
    underlyings: tuple[str, ...],
    contracts,
    spots: dict[str, tuple[Decimal, UUID]],
    history_cohort_id: UUID,
    model_bundle_sha256: str,
    valuation_policy_sha256: str,
    model_version: str = "black-scholes-merton-v1",
):
    """Read HV, valuation and metric inputs for an opening reference state.

    Session activation has no enrollment plan yet, so this supplies only the
    scope the producer's own readers use. Requires a dictionary cursor.
    """
    scope = SimpleNamespace(
        control_plane=SimpleNamespace(
            session_date=session_date,
            universe=SimpleNamespace(option_underlyings=underlyings),
            pins=SimpleNamespace(
                model_bundle_sha256=model_bundle_sha256,
                valuation_policy_sha256=valuation_policy_sha256,
            ),
        ),
        enrollment=SimpleNamespace(
            activity_selection=SimpleNamespace(contracts=contracts),
        ),
        known_at=known_at,
        valid_until=valid_until,
        history_source_cohort_id=history_cohort_id,
        treasury_freshness_policy=_TREASURY_FRESHNESS_POLICY,
        dividend_maximum_age_seconds=_DIVIDEND_MAXIMUM_AGE_SECONDS,
        model_version=model_version,
    )
    evidence: list[AdvancedOptionsReferenceProducerEvidence] = []
    curve, rate_date = _load_treasury_curve(cursor, scope, evidence)
    volatility, metrics, metric_missing = _load_bars_and_metrics(
        cursor,
        scope,
        spots,
        evidence,
    )
    valuations, valuation_missing = (
        _load_valuations(
            cursor,
            scope,
            tuple(
                {"contract_ticker": row.contract_ticker}
                for row in contracts
            ),
            spots,
            curve,
            rate_date,
            evidence,
        )
        if curve and rate_date is not None
        else ((), True)
    )
    missing = tuple(sorted({
        reason
        for row in evidence
        if not _accepted_unavailable_metric(row)
        for reason in row.reason_codes
    }))
    if metric_missing or valuation_missing or not curve:
        return (), (), (), missing or ("REFERENCE_INPUTS_INCOMPLETE",)
    return volatility, valuations, metrics, missing


def assemble_advanced_options_reference_state_cursor(
    cursor,
    plan: AdvancedOptionsReferenceProducerPlan,
) -> AdvancedOptionsReferenceProducerPreflight:
    _validate_plan(plan)
    evidence: list[AdvancedOptionsReferenceProducerEvidence] = []
    contracts = _load_active_scope(cursor, plan, evidence)
    open_interest, oi_missing = _load_open_interest(
        cursor,
        plan,
        contracts,
        evidence,
    )
    curve, rate_date = _load_treasury_curve(cursor, plan, evidence)
    spots = _load_discovery_spots(cursor, plan, contracts, evidence)
    volatility, metrics, metric_missing = _load_bars_and_metrics(
        cursor,
        plan,
        spots,
        evidence,
    ) if spots else ((), (), True)
    valuations, valuation_missing = (
        _load_valuations(
            cursor,
            plan,
            contracts,
            spots,
            curve,
            rate_date,
            evidence,
        )
        if spots and curve and rate_date is not None
        else ((), True)
    )
    if (
        oi_missing
        or not curve
        or not spots
        or metric_missing
        or valuation_missing
    ):
        return _result(plan, evidence, None)
    state = build_advanced_options_reference_state(
        reference_generation=plan.reference_generation,
        session_date=plan.control_plane.session_date,
        known_at=plan.known_at,
        valid_until=plan.valid_until,
        reference_policy_sha256=plan.reference_policy_sha256,
        data_membership_sha256=plan.data_membership_sha256,
        contract_scope_sha256=plan.contract_scope.sha256,
        underlyings=plan.control_plane.universe.option_underlyings,
        open_interest=open_interest,
        historical_volatility=volatility,
        valuation_inputs=valuations,
        equity_metrics=metrics,
    )
    cursor.execute(
        "SELECT state_id::text,state_sha256 FROM "
        "rt.advanced_options_reference_states "
        "WHERE reference_generation=%s",
        (plan.reference_generation,),
    )
    existing = cursor.fetchone()
    if existing is not None and (
        UUID(str(existing["state_id"])) != state.state_id
        or str(existing["state_sha256"]) != state.state_sha256
    ):
        raise ValueError("reference producer generation diverged")
    evidence.append(_evidence(
        "REFERENCE_GENERATION",
        str(plan.reference_generation),
        ReferenceAvailability.AVAILABLE,
        (
            state.state_id,
            state.state_sha256,
            plan.reference_policy_sha256,
            plan.source_clocks_sha256,
        ),
        known_at=plan.known_at,
        valid_until=plan.valid_until,
    ))
    return _result(plan, evidence, state)


class PostgresAdvancedOptionsReferenceProducer:
    def __init__(
        self,
        provider: V2ConnectionProvider,
        plan: AdvancedOptionsReferenceProducerPlan,
        *,
        maximum_attempts: int = 3,
        retry_initial_seconds: float = 0.05,
        wait: Callable[[float], None] = time.sleep,
    ) -> None:
        if not isinstance(provider, V2ConnectionProvider):
            raise TypeError("Advanced Options reference provider is invalid")
        _validate_plan(plan)
        if (
            type(maximum_attempts) is not int
            or maximum_attempts < 1
            or retry_initial_seconds < 0
            or not callable(wait)
        ):
            raise ValueError("Advanced Options reference retry is invalid")
        self._provider = provider
        self._plan = plan
        self._maximum_attempts = maximum_attempts
        self._retry_initial_seconds = retry_initial_seconds
        self._wait = wait

    def _assemble(self, unit) -> AdvancedOptionsReferenceProducerPreflight:
        with unit.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            return assemble_advanced_options_reference_state_cursor(
                cursor,
                self._plan,
            )

    def preflight(self) -> AdvancedOptionsReferenceProducerPreflight:
        with self._provider.transaction(
            APP_ROLE,
            read_only=True,
            isolation_level="REPEATABLE READ",
        ) as unit:
            return self._assemble(unit)

    def produce(self) -> AdvancedOptionsReferenceState:
        for attempt in range(1, self._maximum_attempts + 1):
            try:
                with self._provider.transaction(
                    APP_ROLE,
                    isolation_level="SERIALIZABLE",
                ) as unit:
                    result = self._assemble(unit)
                    if (
                        result.status is not ReferenceProducerStatus.READY
                        or result.state is None
                    ):
                        raise AdvancedOptionsReferencePreflightBlocked(result)
                    return persist_advanced_options_reference_state_cursor(
                        unit.cursor,
                        result.state,
                    )
            except PsycopgError as error:
                if (
                    error.pgcode not in _RETRYABLE_POSTGRES_SQLSTATES
                    or attempt == self._maximum_attempts
                ):
                    raise
                self._wait(
                    self._retry_initial_seconds * (2 ** (attempt - 1))
                )
        raise AssertionError("unreachable reference producer retry state")
