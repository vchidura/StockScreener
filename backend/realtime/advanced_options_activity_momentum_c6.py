"""C6 activity/momentum admission profile and deterministic allocator."""
from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from types import MappingProxyType

from .advanced_options_selection_c6 import (
    AdvancedOptionsSelectionC6Policy,
    C6AllocationCoverage,
    C6SelectionMember,
    OptionSide,
    load_advanced_options_selection_c6_policy,
    load_strategy_policy_identity,
    option_ticker_parts,
)
from .contracts import canonical_sha256


UTC = timezone.utc
LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION = "c6_activity_momentum_v1"
ACTIVITY_MOMENTUM_PROFILE_VERSION = "c6_activity_momentum_v2"
SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION = (
    "c6_activity_momentum_v3"
)
LEGACY_ACTIVITY_MOMENTUM_PROFILE_PATH = (
    Path(__file__).resolve().parent
    / "policies"
    / "advanced_options_activity_momentum_c6_v1.json"
)
DEFAULT_ACTIVITY_MOMENTUM_PROFILE_PATH = (
    Path(__file__).resolve().parent
    / "policies"
    / "advanced_options_activity_momentum_c6_v2.json"
)
SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_PATH = (
    Path(__file__).resolve().parent
    / "policies"
    / "advanced_options_activity_momentum_c6_v3.json"
)
ACTIVITY_MOMENTUM_PROFILE_PATHS = MappingProxyType({
    LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION: (
        LEGACY_ACTIVITY_MOMENTUM_PROFILE_PATH
    ),
    ACTIVITY_MOMENTUM_PROFILE_VERSION: (
        DEFAULT_ACTIVITY_MOMENTUM_PROFILE_PATH
    ),
    SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION: (
        SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_PATH
    ),
})
DEFAULT_ACTIVITY_MOMENTUM_STRATEGY_POLICY_PATH = (
    Path(__file__).resolve().parents[1]
    / "options"
    / "policies"
    / "strategy_o3_credit_v2.json"
)
_V1_ROOT_KEYS = frozenset({
    "version",
    "status",
    "purpose",
    "compatibility",
    "provider_capacity",
    "allocation",
    "live_baskets",
    "role_priority",
    "strategy_group_resolvers",
    "ranking",
    "flow_boundary",
    "pagination",
    "coverage_persistence",
    "deferrals",
})
_V2_ROOT_KEYS = frozenset({
    "version",
    "status",
    "purpose",
    "compatibility",
    "provider_capacity",
    "allocation",
    "discovery",
    "coverage_targets",
    "selection_criteria",
    "bounds",
    "strategy_group_resolvers",
    "routing_requirements",
    "flow_boundary",
    "coverage_persistence",
    "deferrals",
})
_V3_ROOT_KEYS = frozenset({
    *_V2_ROOT_KEYS,
    "expiry_preferences",
    "ranking_anchors",
})
_BOUNDED_ACTIVITY_MOMENTUM_PROFILE_VERSIONS = frozenset({
    ACTIVITY_MOMENTUM_PROFILE_VERSION,
    SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION,
})
_EXPECTED_ROLES = (
    "PARTICIPATION_CONTEXT",
    "SHORT_DATED_SWEEP_LIKE",
    "DIRECTIONAL_MOMENTUM",
    "STRATEGY_REQUIRED",
)


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _text(
    value: object,
    name: str,
    *,
    maximum: int = 160,
    upper: bool = True,
) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be text")
    normalized = value.strip()
    if upper:
        normalized = normalized.upper()
    if not normalized or len(normalized) > maximum:
        raise ValueError(f"{name} is invalid")
    return normalized


def _digest(value: object, name: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


class CoverageState(str, Enum):
    FULL = "FULL"
    PARTIAL = "PARTIAL"
    UNCOVERED = "UNCOVERED"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class ActivityMomentumBasket:
    basket_id: str
    priority: int
    minimum_dte: int
    maximum_dte: int
    roles: tuple[str, ...]
    remaining_budget_only: bool
    call_priority: bool
    put_coverage_required_when_available: bool

    def __post_init__(self) -> None:
        roles = tuple(
            _text(value, "basket role") for value in self.roles
        )
        if (
            type(self.priority) is not int
            or self.priority <= 0
            or type(self.minimum_dte) is not int
            or type(self.maximum_dte) is not int
            or not 0 <= self.minimum_dte <= self.maximum_dte <= 60
            or not roles
            or len(roles) != len(set(roles))
            or set(roles) - set(_EXPECTED_ROLES)
            or any(
                type(value) is not bool
                for value in (
                    self.remaining_budget_only,
                    self.call_priority,
                    self.put_coverage_required_when_available,
                )
            )
        ):
            raise ValueError("activity/momentum basket is invalid")
        object.__setattr__(
            self,
            "basket_id",
            _text(self.basket_id, "basket id"),
        )
        object.__setattr__(self, "roles", roles)


@dataclass(frozen=True, slots=True)
class ActivityMomentumSelectionCriterion:
    criterion_id: str
    basket_id: str
    roles: tuple[str, ...]
    minimum_dte: int
    maximum_dte: int
    target_dte: int
    target_absolute_delta: Decimal
    target_moneyness_fraction: Decimal
    maximum_moneyness_fraction: Decimal
    maximum_relative_spread: Decimal
    maximum_quote_age_seconds: int
    minimum_completed_session_volume: int
    minimum_prior_open_interest: int | None
    shortlist_limit_per_side: int
    missing_greeks_fallback: str
    criteria_owner: str
    delta_is_ranking_anchor: bool

    def __post_init__(self) -> None:
        roles = tuple(sorted({
            _text(value, "selection criterion role")
            for value in self.roles
        }))
        decimal_values = (
            self.target_absolute_delta,
            self.target_moneyness_fraction,
            self.maximum_moneyness_fraction,
            self.maximum_relative_spread,
        )
        if (
            not roles
            or set(roles) - set(_EXPECTED_ROLES)
            or not 0 <= self.minimum_dte
            <= self.target_dte
            <= self.maximum_dte
            <= 60
            or any(
                not isinstance(value, Decimal)
                or not value.is_finite()
                for value in decimal_values
            )
            or not Decimal("0")
            < self.target_absolute_delta
            <= Decimal("1")
            or self.target_moneyness_fraction < 0
            or not Decimal("0")
            < self.maximum_moneyness_fraction
            <= Decimal("1")
            or not Decimal("0")
            < self.maximum_relative_spread
            <= Decimal("2")
            or type(self.maximum_quote_age_seconds) is not int
            or self.maximum_quote_age_seconds <= 0
            or type(self.minimum_completed_session_volume) is not int
            or self.minimum_completed_session_volume < 0
            or (
                self.minimum_prior_open_interest is not None
                and (
                    type(self.minimum_prior_open_interest) is not int
                    or self.minimum_prior_open_interest < 0
                )
            )
            or type(self.shortlist_limit_per_side) is not int
            or self.shortlist_limit_per_side <= 0
            or type(self.delta_is_ranking_anchor) is not bool
        ):
            raise ValueError(
                "activity/momentum selection criterion is invalid"
            )
        object.__setattr__(
            self,
            "criterion_id",
            _text(self.criterion_id, "selection criterion id"),
        )
        object.__setattr__(
            self,
            "basket_id",
            _text(self.basket_id, "selection criterion basket"),
        )
        object.__setattr__(self, "roles", roles)
        object.__setattr__(
            self,
            "missing_greeks_fallback",
            _text(
                self.missing_greeks_fallback,
                "missing Greeks fallback",
            ),
        )
        object.__setattr__(
            self,
            "criteria_owner",
            _text(self.criteria_owner, "criteria owner"),
        )


@dataclass(frozen=True, slots=True)
class ActivityMomentumConsumerRequirement:
    consumer_name: str
    roles: tuple[str, ...]
    required_inputs: tuple[str, ...]
    event_kinds: tuple[str, ...]
    cadence_ms: int
    complete_groups_only: bool
    enabled: bool

    def __post_init__(self) -> None:
        roles = tuple(sorted({
            _text(value, "consumer role") for value in self.roles
        }))
        required_inputs = tuple(sorted({
            _text(value, "consumer required input")
            for value in self.required_inputs
        }))
        event_kinds = tuple(sorted({
            _text(value, "consumer event kind")
            for value in self.event_kinds
        }))
        if (
            not roles
            or set(roles) - set(_EXPECTED_ROLES)
            or not required_inputs
            or not event_kinds
            or set(event_kinds) - {"T", "Q"}
            or type(self.cadence_ms) is not int
            or self.cadence_ms < 0
            or type(self.complete_groups_only) is not bool
            or type(self.enabled) is not bool
        ):
            raise ValueError(
                "activity/momentum consumer requirement is invalid"
            )
        object.__setattr__(
            self,
            "consumer_name",
            _text(
                self.consumer_name,
                "consumer name",
                maximum=128,
                upper=False,
            ),
        )
        object.__setattr__(self, "roles", roles)
        object.__setattr__(
            self,
            "required_inputs",
            required_inputs,
        )
        object.__setattr__(self, "event_kinds", event_kinds)


@dataclass(frozen=True, slots=True)
class ActivityMomentumProfile:
    version: str
    profile_sha256: str
    selection_policy_version: str
    selection_policy_sha256: str
    flow_policy_sha256: str
    provider_cluster: str
    default_connection_limit: int
    quote_contract_ceiling: int
    active_quote_contract_budget: int
    quote_slot_reserve: int
    channels: tuple[str, ...]
    baskets: tuple[ActivityMomentumBasket, ...]
    role_priority: tuple[str, ...]
    supported_group_strategies: tuple[str, ...]
    rejected_group_sample_limit: int
    maximum_page_results: int
    effective_provider_project_ceiling: int
    established_measured_capacity: int | None
    default_contracts_per_underlying: int
    maximum_contracts_per_underlying: int
    short_basket_contract_target: int
    core_basket_contract_target: int
    default_discovery_minimum_dte: int
    default_discovery_maximum_dte: int
    strategy_discovery_maximum_dte: int
    maximum_candidate_pool_per_underlying: int
    maximum_shortlisted_candidates_per_underlying: int
    maximum_group_enumerations_per_underlying: int
    maximum_complete_groups_per_underlying: int
    rejection_reason_sample_limit: int
    selection_criteria: tuple[
        ActivityMomentumSelectionCriterion,
        ...,
    ]
    consumer_requirements: tuple[
        ActivityMomentumConsumerRequirement,
        ...,
    ]
    same_day_enabled: bool
    supported_last_trading_conventions: tuple[str, ...]
    short_near_expiry_contract_target: int
    short_end_of_week_contract_target: int
    delta_targets_are_ranking_anchors: bool

    def __post_init__(self) -> None:
        supported_versions = {
            LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION,
            ACTIVITY_MOMENTUM_PROFILE_VERSION,
            SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION,
        }
        if (
            self.version not in supported_versions
            or not self.baskets
            or tuple(row.priority for row in self.baskets)
            != tuple(range(1, len(self.baskets) + 1))
            or self.role_priority != _EXPECTED_ROLES
            or not self.supported_group_strategies
            or self.channels != ("T", "Q")
            or self.provider_cluster != "OPTIONS"
            or self.default_connection_limit != 1
            or self.quote_contract_ceiling != 1000
            or not 0 < self.active_quote_contract_budget
            <= self.quote_contract_ceiling
            or not 0 < self.effective_provider_project_ceiling
            <= self.quote_contract_ceiling
            or (
                self.established_measured_capacity is not None
                and (
                    type(self.established_measured_capacity) is not int
                    or self.established_measured_capacity <= 0
                )
            )
            or not 0 < self.default_contracts_per_underlying
            <= self.maximum_contracts_per_underlying
            or self.maximum_contracts_per_underlying
            > self.quote_contract_ceiling
            or self.short_basket_contract_target < 0
            or self.core_basket_contract_target < 0
            or not 0 <= self.default_discovery_minimum_dte
            <= self.default_discovery_maximum_dte
            <= self.strategy_discovery_maximum_dte
            <= 60
            or type(self.rejected_group_sample_limit) is not int
            or self.rejected_group_sample_limit <= 0
            or type(self.rejection_reason_sample_limit) is not int
            or self.rejection_reason_sample_limit <= 0
            or self.maximum_page_results != 250
            or type(self.same_day_enabled) is not bool
            or type(self.delta_targets_are_ranking_anchors) is not bool
            or any(
                type(value) is not int or value < 0
                for value in (
                    self.short_near_expiry_contract_target,
                    self.short_end_of_week_contract_target,
                )
            )
            or any(
                type(value) is not int or value <= 0
                for value in (
                    self.maximum_candidate_pool_per_underlying,
                    self.maximum_shortlisted_candidates_per_underlying,
                    self.maximum_group_enumerations_per_underlying,
                    self.maximum_complete_groups_per_underlying,
                )
            )
        ):
            raise ValueError("activity/momentum profile is invalid")
        if self.version in _BOUNDED_ACTIVITY_MOMENTUM_PROFILE_VERSIONS:
            if (
                self.default_contracts_per_underlying != 8
                or self.maximum_contracts_per_underlying != 12
                or self.short_basket_contract_target != 4
                or self.core_basket_contract_target != 4
                or self.default_discovery_maximum_dte != 21
                or self.strategy_discovery_maximum_dte != 60
                or not self.selection_criteria
                or not self.consumer_requirements
            ):
                raise ValueError(
                    "activity/momentum v2 addendum profile is invalid"
                )
            if (
                self.version
                == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
                and (
                    not self.same_day_enabled
                    or not self.supported_last_trading_conventions
                    or self.short_near_expiry_contract_target != 2
                    or self.short_end_of_week_contract_target != 2
                    or not self.delta_targets_are_ranking_anchors
                )
            ):
                raise ValueError(
                    "activity/momentum v3 expiry profile is invalid"
                )
        elif (
            self.active_quote_contract_budget != 900
            or self.quote_slot_reserve != 100
            or self.selection_criteria
            or self.consumer_requirements
        ):
            raise ValueError(
                "legacy activity/momentum profile is invalid"
            )
        conventions = tuple(sorted({
            _text(
                value,
                "last trading convention",
                maximum=80,
            )
            for value in self.supported_last_trading_conventions
        }))
        if (
            self.version
            != SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
            and (
                conventions
                or self.same_day_enabled
                or self.short_near_expiry_contract_target
                or self.short_end_of_week_contract_target
                or self.delta_targets_are_ranking_anchors
            )
        ):
            raise ValueError(
                "legacy activity/momentum expiry settings changed"
            )
        object.__setattr__(
            self,
            "supported_last_trading_conventions",
            conventions,
        )
        for value, name in (
            (self.profile_sha256, "profile_sha256"),
            (
                self.selection_policy_sha256,
                "selection_policy_sha256",
            ),
            (self.flow_policy_sha256, "flow_policy_sha256"),
        ):
            _digest(value, name)

    @property
    def basket_map(self) -> Mapping[str, ActivityMomentumBasket]:
        return MappingProxyType({
            row.basket_id: row for row in self.baskets
        })

    @property
    def role_rank(self) -> Mapping[str, int]:
        return MappingProxyType({
            role: index
            for index, role in enumerate(self.role_priority)
        })

    def criterion_for(
        self,
        *,
        dte: int,
        roles: tuple[str, ...],
    ) -> ActivityMomentumSelectionCriterion | None:
        role_values = set(roles)
        return next(
            (
                row
                for row in self.selection_criteria
                if row.minimum_dte <= dte <= row.maximum_dte
                and role_values & set(row.roles)
            ),
            None,
        )

    def discovery_maximum_dte(
        self,
        *,
        enabled_strategies: Iterable[str],
        required_contract_tickers: Iterable[str],
    ) -> int:
        if self.version == LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION:
            return self.strategy_discovery_maximum_dte
        return (
            self.strategy_discovery_maximum_dte
            if tuple(enabled_strategies)
            or tuple(required_contract_tickers)
            else self.default_discovery_maximum_dte
        )

    def basket_for(
        self,
        *,
        dte: int,
        roles: tuple[str, ...],
    ) -> ActivityMomentumBasket | None:
        for basket in self.baskets:
            if (
                basket.minimum_dte <= dte <= basket.maximum_dte
                and set(roles) & set(basket.roles)
            ):
                return basket
        return None


def load_activity_momentum_profile(
    path: Path | None = None,
    *,
    selection_policy: (
        AdvancedOptionsSelectionC6Policy | None
    ) = None,
    version: str | None = None,
    profile_sha256: str | None = None,
) -> ActivityMomentumProfile:
    if path is not None and version is not None:
        raise ValueError(
            "activity/momentum profile path and version are exclusive"
        )
    if version is not None:
        try:
            resolved_path = ACTIVITY_MOMENTUM_PROFILE_PATHS[version]
        except KeyError as error:
            raise ValueError(
                "activity/momentum profile version is unsupported"
            ) from error
    else:
        resolved_path = (
            DEFAULT_ACTIVITY_MOMENTUM_PROFILE_PATH
            if path is None
            else path
        )
    value = json.loads(resolved_path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("activity/momentum profile keys changed")
    loaded_version = str(value.get("version"))
    expected_keys = {
        LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION: _V1_ROOT_KEYS,
        ACTIVITY_MOMENTUM_PROFILE_VERSION: _V2_ROOT_KEYS,
        SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION: _V3_ROOT_KEYS,
    }.get(loaded_version)
    expected_status = {
        LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION: (
            "C6_RUNTIME_LOADED"
        ),
        ACTIVITY_MOMENTUM_PROFILE_VERSION: (
            "C6_ADDENDUM_RUNTIME_LOADED"
        ),
        SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION: (
            "C6_WEEKLY_EXPIRY_CALIBRATION_RUNTIME_LOADED"
        ),
    }.get(loaded_version)
    if (
        loaded_version
        not in {
            LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION,
            ACTIVITY_MOMENTUM_PROFILE_VERSION,
            SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION,
        }
        or frozenset(value) != expected_keys
        or value.get("status") != expected_status
        or (
            version is not None
            and loaded_version != version
        )
    ):
        raise ValueError("activity/momentum profile identity changed")
    loaded_sha256 = canonical_sha256(value)
    if (
        profile_sha256 is not None
        and loaded_sha256 != _digest(
            profile_sha256,
            "profile_sha256",
        )
    ):
        raise ValueError("activity/momentum profile hash changed")
    compatibility = value.get("compatibility")
    capacity = value.get("provider_capacity")
    allocation = value.get("allocation")
    group_resolvers = value.get("strategy_group_resolvers")
    flow = value.get("flow_boundary")
    persistence = value.get("coverage_persistence")
    deferrals = value.get("deferrals")
    versioned_sections = (
        (
            value.get("ranking"),
            value.get("pagination"),
        )
        if loaded_version == LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION
        else (
            value.get("discovery"),
            value.get("coverage_targets"),
            value.get("bounds"),
        )
    )
    if not all(
        isinstance(item, dict)
        for item in (
            compatibility,
            capacity,
            allocation,
            group_resolvers,
            flow,
            persistence,
            deferrals,
            *versioned_sections,
        )
    ):
        raise ValueError("activity/momentum profile sections are invalid")
    ranking = value.get("ranking") or {}
    pagination = value.get("pagination") or {}
    discovery = value.get("discovery") or {}
    coverage_targets = value.get("coverage_targets") or {}
    bounds = value.get("bounds") or {}
    expiry_preferences = value.get("expiry_preferences") or {}
    ranking_anchors = value.get("ranking_anchors") or {}
    base = (
        selection_policy
        or load_advanced_options_selection_c6_policy()
    )
    if (
        compatibility.get("selection_policy_version")
        != base.version
        or compatibility.get("selection_policy_sha256")
        != base.policy_sha256
        or compatibility.get("flow_policy_sha256")
        != base.flow_policy_sha256
        or compatibility.get("mutate_compatible_policies") is not False
        or compatibility.get("exact_four_supported") is not True
        or capacity.get("wildcard_subscriptions") is not False
        or capacity.get(
            "reconnect_requires_simultaneous_connection"
        )
        is not False
        or capacity.get(
            "stocks_connection_consumes_options_allowance"
        )
        is not False
        or capacity.get("logical_shards_consume_connections") is not False
        or allocation.get("complete_groups_only") is not True
        or allocation.get("unused_share_redistribution") is not True
        or allocation.get("candidate_output_caps_apply") is not False
        or group_resolvers.get("enabled_by_default") != []
        or group_resolvers.get("complete_groups_only") is not True
        or (
            loaded_version == LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION
            and ranking.get(
                "provider_ordering_affects_selection"
            )
            is not False
        )
        or flow.get("dte_3_4")
        != "CAPTURE_MARKET_AND_MOMENTUM_FLOW_UNAVAILABLE"
        or flow.get("unknown_flow_underlying")
        != "CAPTURE_MARKET_AND_MOMENTUM_FLOW_UNAVAILABLE"
        or (
            loaded_version == LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION
            and (
                pagination.get("follow_next_url") is not True
                or pagination.get(
                    "total_request_budget_required"
                )
                is not True
                or pagination.get(
                    "total_decoded_byte_budget_required"
                )
                is not True
                or pagination.get("deadline_required") is not True
            )
        )
        or persistence.get(
            "normalized_per_underlying_role_rows"
        )
        is not True
        or persistence.get("immutable_allocation_document") is not True
        or deferrals.get("intraday_rotation") is not False
        or deferrals.get("future_50ms_detector") is not False
        or not isinstance(deferrals.get("silent_accumulation"), dict)
        or deferrals["silent_accumulation"].get(
            "capture_enabled"
        )
        is not False
        or deferrals["silent_accumulation"].get(
            "history_jobs_enabled"
        )
        is not False
        or deferrals["silent_accumulation"].get(
            "detector_enabled"
        )
        is not False
        or deferrals["silent_accumulation"].get("live_quote_slots") != 0
    ):
        raise ValueError("activity/momentum profile contract changed")
    if loaded_version in _BOUNDED_ACTIVITY_MOMENTUM_PROFILE_VERSIONS:
        if (
            compatibility.get("stored_v1_decode_supported") is not True
            or allocation.get(
                "explicit_expansion_required_above_default"
            )
            is not True
            or discovery.get("provider_expiry_filter_required")
            is not True
            or discovery.get("provider_strike_filter_required")
            is not True
            or discovery.get(
                "strategy_extension_requires_enabled_strategy_or_required_group"
            )
            is not True
            or discovery.get("future_foundation_live_enabled")
            is not False
            or coverage_targets.get("minimum_fill_required")
            is not False
            or persistence.get("persist_effective_budget") is not True
            or persistence.get(
                "persist_expansion_configuration"
            )
            is not True
            or persistence.get(
                "persist_ranking_evidence_and_clocks"
            )
            is not True
            or persistence.get(
                "persist_filter_shortlist_admission_counts"
            )
            is not True
            or persistence.get("persist_complete_groups") is not True
            or persistence.get(
                "persist_consumer_eligibility"
            )
            is not True
            or deferrals.get("additional_provider_feeds") is not False
        ):
            raise ValueError(
                "activity/momentum v2 profile contract changed"
            )
        if (
            loaded_version
            == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
            and (
                compatibility.get("stored_v2_decode_supported")
                is not True
                or expiry_preferences.get("short_near_target_dte")
                != 2
                or expiry_preferences.get(
                    "near_expiry_unique_contract_target"
                )
                != 2
                or expiry_preferences.get(
                    "end_of_week_unique_contract_target"
                )
                != 2
                or expiry_preferences.get("listed_expirations_only")
                is not True
                or expiry_preferences.get(
                    "provider_weekly_label_required"
                )
                is not False
                or expiry_preferences.get("same_day_enabled")
                is not True
                or not isinstance(
                    expiry_preferences.get(
                        "supported_last_trading_conventions"
                    ),
                    list,
                )
                or expiry_preferences.get(
                    "unsupported_convention_behavior"
                )
                != "UNAVAILABLE_DEPENDENT_COVERAGE"
                or ranking_anchors.get(
                    "delta_targets_are_hard_cutoffs"
                )
                is not False
                or ranking_anchors.get("calibration_state")
                != "OFFLINE_SEED_NOT_PRODUCTION_VALIDATED"
                or persistence.get(
                    "persist_resolved_expiry_evidence"
                )
                is not True
                or persistence.get(
                    "persist_last_trading_evidence"
                )
                is not True
                or persistence.get(
                    "persist_selected_counts_by_expiry_and_side"
                )
                is not True
            )
        ):
            raise ValueError(
                "activity/momentum v3 profile contract changed"
            )
        basket_values = (
            {
                "id": "SHORT_DATED_ACTIVITY",
                "priority": 1,
                "minimum_dte": 0,
                "maximum_dte": 5,
                "roles": [
                    "PARTICIPATION_CONTEXT",
                    "SHORT_DATED_SWEEP_LIKE",
                    "DIRECTIONAL_MOMENTUM",
                ],
                "remaining_budget_only": False,
                "call_priority": True,
                "put_coverage_required_when_available": True,
            },
            {
                "id": "CORE_MOMENTUM",
                "priority": 2,
                "minimum_dte": 6,
                "maximum_dte": 21,
                "roles": [
                    "PARTICIPATION_CONTEXT",
                    "DIRECTIONAL_MOMENTUM",
                ],
                "remaining_budget_only": False,
                "call_priority": False,
                "put_coverage_required_when_available": True,
            },
            {
                "id": "STRATEGY_EXTENSION",
                "priority": 3,
                "minimum_dte": 22,
                "maximum_dte": 60,
                "roles": ["STRATEGY_REQUIRED"],
                "remaining_budget_only": True,
                "call_priority": False,
                "put_coverage_required_when_available": False,
            },
        )
    else:
        basket_values = value.get("live_baskets")
        if not isinstance(basket_values, list):
            raise ValueError("activity/momentum baskets are invalid")
    baskets = tuple(
        ActivityMomentumBasket(
            basket_id=str(row["id"]),
            priority=int(row["priority"]),
            minimum_dte=int(row["minimum_dte"]),
            maximum_dte=int(row["maximum_dte"]),
            roles=tuple(str(item) for item in row["roles"]),
            remaining_budget_only=(
                row.get("remaining_budget_only") is True
            ),
            call_priority=row.get("call_priority") is True,
            put_coverage_required_when_available=(
                row.get("put_coverage_required_when_available") is True
            ),
        )
        for row in basket_values
    )
    criteria = tuple(
        ActivityMomentumSelectionCriterion(
            criterion_id=str(row["id"]),
            basket_id=str(row["basket_id"]),
            roles=tuple(str(item) for item in row["roles"]),
            minimum_dte=int(row["minimum_dte"]),
            maximum_dte=int(row["maximum_dte"]),
            target_dte=int(row["target_dte"]),
            target_absolute_delta=Decimal(
                str(row["target_absolute_delta"])
            ),
            target_moneyness_fraction=Decimal(
                str(row["target_moneyness_fraction"])
            ),
            maximum_moneyness_fraction=Decimal(
                str(row["maximum_moneyness_fraction"])
            ),
            maximum_relative_spread=Decimal(
                str(row["maximum_relative_spread"])
            ),
            maximum_quote_age_seconds=int(
                row["maximum_quote_age_seconds"]
            ),
            minimum_completed_session_volume=int(
                row["minimum_completed_session_volume"]
            ),
            minimum_prior_open_interest=(
                None
                if row["minimum_prior_open_interest"] is None
                else int(row["minimum_prior_open_interest"])
            ),
            shortlist_limit_per_side=int(
                row["shortlist_limit_per_side"]
            ),
            missing_greeks_fallback=str(
                row.get(
                    "missing_greeks_fallback",
                    "MONEYNESS_THEN_EXACT_TICKER",
                )
            ),
            criteria_owner=str(
                row.get("criteria_owner", "PROFILE")
            ),
            delta_is_ranking_anchor=bool(
                row.get("delta_is_ranking_anchor", False)
            ),
        )
        for row in value.get("selection_criteria", ())
    )
    consumer_requirements = tuple(
        ActivityMomentumConsumerRequirement(
            consumer_name=str(row["consumer_name"]),
            roles=tuple(str(item) for item in row["roles"]),
            required_inputs=tuple(
                str(item) for item in row["required_inputs"]
            ),
            event_kinds=tuple(
                str(item) for item in row["event_kinds"]
            ),
            cadence_ms=int(row["cadence_ms"]),
            complete_groups_only=bool(
                row["complete_groups_only"]
            ),
            enabled=bool(row["enabled"]),
        )
        for row in value.get("routing_requirements", ())
    )
    return ActivityMomentumProfile(
        version=loaded_version,
        profile_sha256=loaded_sha256,
        selection_policy_version=str(
            compatibility["selection_policy_version"]
        ),
        selection_policy_sha256=str(
            compatibility["selection_policy_sha256"]
        ),
        flow_policy_sha256=str(
            compatibility["flow_policy_sha256"]
        ),
        provider_cluster=str(capacity["provider_cluster"]),
        default_connection_limit=int(
            capacity["default_connection_limit"]
        ),
        quote_contract_ceiling=int(
            capacity["quote_contract_ceiling"]
        ),
        active_quote_contract_budget=int(
            capacity.get(
                "active_quote_contract_budget",
                capacity.get("configured_total_budget"),
            )
        ),
        quote_slot_reserve=int(capacity["quote_slot_reserve"]),
        channels=tuple(str(item) for item in capacity["channels"]),
        baskets=baskets,
        role_priority=tuple(
            str(item)
            for item in value.get(
                "role_priority",
                _EXPECTED_ROLES,
            )
        ),
        supported_group_strategies=tuple(
            str(item) for item in group_resolvers["supported"]
        ),
        rejected_group_sample_limit=int(
            persistence.get(
                "bounded_rejected_group_samples",
                bounds.get("bounded_rejected_group_samples"),
            )
        ),
        maximum_page_results=int(
            pagination.get(
                "maximum_page_results",
                bounds.get("maximum_page_results"),
            )
        ),
        effective_provider_project_ceiling=int(
            capacity["effective_provider_project_ceiling"]
            if "effective_provider_project_ceiling" in capacity
            else capacity["active_quote_contract_budget"]
        ),
        established_measured_capacity=(
            None
            if capacity.get("established_measured_capacity") is None
            else int(capacity["established_measured_capacity"])
        ),
        default_contracts_per_underlying=int(
            allocation[
                "default_contracts_per_eligible_underlying"
            ]
            if "default_contracts_per_eligible_underlying"
            in allocation
            else capacity["active_quote_contract_budget"]
        ),
        maximum_contracts_per_underlying=int(
            allocation["maximum_contracts_per_underlying"]
            if "maximum_contracts_per_underlying" in allocation
            else capacity["active_quote_contract_budget"]
        ),
        short_basket_contract_target=int(
            coverage_targets.get(
                "short_basket_unique_contracts",
                0,
            )
        ),
        core_basket_contract_target=int(
            coverage_targets.get(
                "core_basket_unique_contracts",
                0,
            )
        ),
        default_discovery_minimum_dte=int(
            discovery.get("default_minimum_dte", 0)
        ),
        default_discovery_maximum_dte=int(
            discovery.get("default_maximum_dte", 60)
        ),
        strategy_discovery_maximum_dte=int(
            discovery.get("strategy_extension_maximum_dte", 60)
        ),
        maximum_candidate_pool_per_underlying=int(
            bounds.get(
                "maximum_candidate_pool_per_underlying",
                10_000,
            )
        ),
        maximum_shortlisted_candidates_per_underlying=int(
            bounds.get(
                "maximum_shortlisted_candidates_per_underlying",
                10_000,
            )
        ),
        maximum_group_enumerations_per_underlying=int(
            bounds.get(
                "maximum_group_enumerations_per_underlying",
                10_000,
            )
        ),
        maximum_complete_groups_per_underlying=int(
            bounds.get(
                "maximum_complete_groups_per_underlying",
                10_000,
            )
        ),
        rejection_reason_sample_limit=int(
            bounds.get(
                "bounded_rejection_reason_samples",
                persistence.get(
                    "bounded_rejected_group_samples",
                    20,
                ),
            )
        ),
        selection_criteria=criteria,
        consumer_requirements=consumer_requirements,
        same_day_enabled=(
            expiry_preferences.get("same_day_enabled") is True
        ),
        supported_last_trading_conventions=tuple(
            str(item)
            for item in expiry_preferences.get(
                "supported_last_trading_conventions",
                (),
            )
        ),
        short_near_expiry_contract_target=int(
            expiry_preferences.get(
                "near_expiry_unique_contract_target",
                0,
            )
        ),
        short_end_of_week_contract_target=int(
            expiry_preferences.get(
                "end_of_week_unique_contract_target",
                0,
            )
        ),
        delta_targets_are_ranking_anchors=(
            loaded_version
            == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
            and ranking_anchors.get(
                "delta_targets_are_hard_cutoffs"
            )
            is False
        ),
    )


@dataclass(frozen=True, slots=True)
class ActivityMomentumCandidate:
    contract_id: int
    contract_ticker: str
    underlying: str
    expiration: date
    option_side: OptionSide
    strike: Decimal
    multiplier: Decimal
    deliverable: str
    catalog_version_id: int
    catalog_payload_sha256: str
    observed_at: datetime
    spot: Decimal | None
    bid: Decimal | None
    ask: Decimal | None
    quote_as_of: datetime | None
    completed_session_volume: int | None
    volume_as_of: datetime | None
    prior_open_interest: int | None
    open_interest_as_of: datetime | None
    delta: Decimal | None
    implied_volatility: Decimal | None
    greeks_as_of: datetime | None
    source_evidence_references: tuple[str, ...]
    flow_eligible: bool
    flow_unavailable_reason: str | None
    unavailable_reasons: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        ticker = _text(
            self.contract_ticker,
            "contract ticker",
            maximum=48,
        )
        underlying, side = option_ticker_parts(ticker)
        observed = _utc(self.observed_at, "candidate observed_at")
        timestamps = {}
        for name in (
            "quote_as_of",
            "volume_as_of",
            "open_interest_as_of",
            "greeks_as_of",
        ):
            value = getattr(self, name)
            timestamps[name] = (
                None if value is None else _utc(value, name)
            )
        if (
            type(self.contract_id) is not int
            or self.contract_id <= 0
            or type(self.catalog_version_id) is not int
            or self.catalog_version_id <= 0
            or underlying != _text(
                self.underlying,
                "underlying",
                maximum=16,
            )
            or side is not self.option_side
            or not isinstance(self.expiration, date)
            or isinstance(self.expiration, datetime)
            or self.expiration < observed.date()
            or not isinstance(self.strike, Decimal)
            or not self.strike.is_finite()
            or self.strike <= 0
            or not isinstance(self.multiplier, Decimal)
            or not self.multiplier.is_finite()
            or self.multiplier <= 0
            or (
                self.spot is not None
                and (
                    not isinstance(self.spot, Decimal)
                    or not self.spot.is_finite()
                    or self.spot <= 0
                )
            )
            or (self.bid is None) != (self.ask is None)
            or (
                self.bid is not None
                and (
                    not isinstance(self.bid, Decimal)
                    or not self.bid.is_finite()
                    or not isinstance(self.ask, Decimal)
                    or not self.ask.is_finite()
                    or self.bid < 0
                    or self.ask is None
                    or self.ask <= 0
                    or self.ask < self.bid
                    or timestamps["quote_as_of"] is None
                )
            )
            or (
                self.completed_session_volume is not None
                and self.completed_session_volume < 0
            )
            or (
                self.completed_session_volume is None
                and timestamps["volume_as_of"] is not None
                or self.completed_session_volume is not None
                and timestamps["volume_as_of"] is None
            )
            or (
                self.prior_open_interest is not None
                and self.prior_open_interest < 0
            )
            or (
                self.prior_open_interest is None
                and timestamps["open_interest_as_of"] is not None
                or self.prior_open_interest is not None
                and timestamps["open_interest_as_of"] is None
            )
            or (
                self.delta is not None
                and (
                    not isinstance(self.delta, Decimal)
                    or not self.delta.is_finite()
                    or not Decimal("-1")
                    <= self.delta
                    <= Decimal("1")
                )
            )
            or (
                self.implied_volatility is not None
                and (
                    not isinstance(
                        self.implied_volatility,
                        Decimal,
                    )
                    or not self.implied_volatility.is_finite()
                    or self.implied_volatility < 0
                )
            )
            or (
                self.delta is None
                and self.implied_volatility is None
                and timestamps["greeks_as_of"] is not None
                or (
                    self.delta is not None
                    or self.implied_volatility is not None
                )
                and timestamps["greeks_as_of"] is None
            )
            or type(self.flow_eligible) is not bool
            or self.flow_eligible
            is (self.flow_unavailable_reason is not None)
            or any(
                value is not None and value > observed
                for value in timestamps.values()
            )
        ):
            raise ValueError("activity/momentum candidate is invalid")
        evidence = tuple(sorted({
            _text(
                value,
                "source evidence reference",
                maximum=256,
                upper=False,
            )
            for value in self.source_evidence_references
        }))
        reasons = tuple(sorted({
            _text(value, "candidate unavailable reason")
            for value in self.unavailable_reasons
        }))
        object.__setattr__(self, "contract_ticker", ticker)
        object.__setattr__(self, "underlying", underlying)
        object.__setattr__(
            self,
            "deliverable",
            _text(self.deliverable, "deliverable", maximum=80),
        )
        object.__setattr__(self, "observed_at", observed)
        for name, value in timestamps.items():
            object.__setattr__(self, name, value)
        object.__setattr__(
            self,
            "source_evidence_references",
            evidence,
        )
        if self.flow_unavailable_reason is not None:
            object.__setattr__(
                self,
                "flow_unavailable_reason",
                _text(
                    self.flow_unavailable_reason,
                    "flow unavailable reason",
                ),
            )
        object.__setattr__(self, "unavailable_reasons", reasons)
        _digest(
            self.catalog_payload_sha256,
            "catalog_payload_sha256",
        )

    @property
    def calendar_dte(self) -> int:
        return (self.expiration - self.observed_at.date()).days

    def dte_for(self, session_date: date) -> int:
        if not isinstance(session_date, date):
            raise TypeError("session_date must be a date")
        return (self.expiration - session_date).days

    @property
    def relative_spread(self) -> Decimal | None:
        if self.bid is None or self.ask is None:
            return None
        midpoint = (self.bid + self.ask) / Decimal("2")
        return (
            None
            if midpoint <= 0
            else (self.ask - self.bid) / midpoint
        )

    @property
    def completed_session_notional(self) -> Decimal | None:
        if (
            self.completed_session_volume is None
            or self.bid is None
            or self.ask is None
        ):
            return None
        midpoint = (self.bid + self.ask) / Decimal("2")
        return (
            midpoint
            * self.multiplier
            * self.completed_session_volume
        )

    @property
    def missing_inputs(self) -> tuple[str, ...]:
        values = []
        if self.bid is None:
            values.append("MISSING_DATED_QUOTE")
        if self.completed_session_volume is None:
            values.append("MISSING_COMPLETED_SESSION_VOLUME")
        if self.prior_open_interest is None:
            values.append("MISSING_PRIOR_OPEN_INTEREST")
        if self.delta is None:
            values.append("MISSING_DELTA")
        if self.implied_volatility is None:
            values.append("MISSING_IMPLIED_VOLATILITY")
        return tuple(values)


def activity_momentum_candidate_payload(
    candidate: ActivityMomentumCandidate,
) -> dict[str, object]:
    return {
        "contract_id": candidate.contract_id,
        "contract_ticker": candidate.contract_ticker,
        "underlying": candidate.underlying,
        "expiration": candidate.expiration.isoformat(),
        "calendar_dte": candidate.calendar_dte,
        "option_side": candidate.option_side.value,
        "strike": str(candidate.strike),
        "multiplier": str(candidate.multiplier),
        "deliverable": candidate.deliverable,
        "catalog_version_id": candidate.catalog_version_id,
        "catalog_payload_sha256": candidate.catalog_payload_sha256,
        "observed_at": candidate.observed_at.isoformat(),
        "spot": None if candidate.spot is None else str(candidate.spot),
        "bid": None if candidate.bid is None else str(candidate.bid),
        "ask": None if candidate.ask is None else str(candidate.ask),
        "quote_as_of": (
            None
            if candidate.quote_as_of is None
            else candidate.quote_as_of.isoformat()
        ),
        "completed_session_volume": (
            candidate.completed_session_volume
        ),
        "volume_as_of": (
            None
            if candidate.volume_as_of is None
            else candidate.volume_as_of.isoformat()
        ),
        "prior_open_interest": candidate.prior_open_interest,
        "open_interest_as_of": (
            None
            if candidate.open_interest_as_of is None
            else candidate.open_interest_as_of.isoformat()
        ),
        "delta": None if candidate.delta is None else str(candidate.delta),
        "implied_volatility": (
            None
            if candidate.implied_volatility is None
            else str(candidate.implied_volatility)
        ),
        "greeks_as_of": (
            None
            if candidate.greeks_as_of is None
            else candidate.greeks_as_of.isoformat()
        ),
        "source_evidence_references": list(
            candidate.source_evidence_references
        ),
        "flow_eligible": candidate.flow_eligible,
        "flow_unavailable_reason": (
            candidate.flow_unavailable_reason
        ),
        "missing_inputs": list(candidate.missing_inputs),
        "unavailable_reasons": list(candidate.unavailable_reasons),
    }


def activity_momentum_candidate_from_payload(
    value: object,
) -> ActivityMomentumCandidate:
    if not isinstance(value, Mapping):
        raise TypeError("activity/momentum candidate must be an object")
    candidate = ActivityMomentumCandidate(
        contract_id=int(value["contract_id"]),
        contract_ticker=str(value["contract_ticker"]),
        underlying=str(value["underlying"]),
        expiration=date.fromisoformat(str(value["expiration"])),
        option_side=OptionSide(str(value["option_side"])),
        strike=Decimal(str(value["strike"])),
        multiplier=Decimal(str(value["multiplier"])),
        deliverable=str(value["deliverable"]),
        catalog_version_id=int(value["catalog_version_id"]),
        catalog_payload_sha256=str(
            value["catalog_payload_sha256"]
        ),
        observed_at=datetime.fromisoformat(str(value["observed_at"])),
        spot=(
            None
            if value.get("spot") is None
            else Decimal(str(value["spot"]))
        ),
        bid=(
            None
            if value.get("bid") is None
            else Decimal(str(value["bid"]))
        ),
        ask=(
            None
            if value.get("ask") is None
            else Decimal(str(value["ask"]))
        ),
        quote_as_of=(
            None
            if value.get("quote_as_of") is None
            else datetime.fromisoformat(str(value["quote_as_of"]))
        ),
        completed_session_volume=(
            None
            if value.get("completed_session_volume") is None
            else int(value["completed_session_volume"])
        ),
        volume_as_of=(
            None
            if value.get("volume_as_of") is None
            else datetime.fromisoformat(str(value["volume_as_of"]))
        ),
        prior_open_interest=(
            None
            if value.get("prior_open_interest") is None
            else int(value["prior_open_interest"])
        ),
        open_interest_as_of=(
            None
            if value.get("open_interest_as_of") is None
            else datetime.fromisoformat(
                str(value["open_interest_as_of"])
            )
        ),
        delta=(
            None
            if value.get("delta") is None
            else Decimal(str(value["delta"]))
        ),
        implied_volatility=(
            None
            if value.get("implied_volatility") is None
            else Decimal(str(value["implied_volatility"]))
        ),
        greeks_as_of=(
            None
            if value.get("greeks_as_of") is None
            else datetime.fromisoformat(str(value["greeks_as_of"]))
        ),
        source_evidence_references=tuple(
            str(item)
            for item in value["source_evidence_references"]
        ),
        flow_eligible=value["flow_eligible"] is True,
        flow_unavailable_reason=(
            None
            if value.get("flow_unavailable_reason") is None
            else str(value["flow_unavailable_reason"])
        ),
        unavailable_reasons=tuple(
            str(item) for item in value["unavailable_reasons"]
        ),
    )
    if activity_momentum_candidate_payload(candidate) != dict(value):
        raise ValueError("activity/momentum candidate payload changed")
    return candidate


@dataclass(frozen=True, slots=True)
class ActivityMomentumGroup:
    group_id: str
    group_version: str
    underlying: str
    roles: tuple[str, ...]
    candidates: tuple[ActivityMomentumCandidate, ...]
    requirement_identities: tuple[str, ...]
    matching_strategies: tuple[str, ...] = ()
    contract_roles: tuple[
        tuple[str, tuple[str, ...]],
        ...
    ] = ()
    target_delta: Decimal | None = None
    target_moneyness: Decimal | None = None
    target_dte: int | None = None

    def __post_init__(self) -> None:
        underlying = _text(
            self.underlying,
            "group underlying",
            maximum=16,
        )
        roles = tuple(sorted({
            _text(value, "group role") for value in self.roles
        }))
        candidates = tuple(sorted(
            self.candidates,
            key=lambda row: row.contract_ticker,
        ))
        identities = tuple(sorted({
            _text(
                value,
                "requirement identity",
                maximum=256,
                upper=False,
            )
            for value in self.requirement_identities
        }))
        strategies = tuple(sorted({
            _text(value, "matching strategy")
            for value in self.matching_strategies
        }))
        contract_roles = tuple(sorted(
            (
                _text(ticker, "contract role ticker", maximum=48),
                tuple(sorted({
                    _text(role, "contract role")
                    for role in role_values
                })),
            )
            for ticker, role_values in self.contract_roles
        ))
        if (
            len({ticker for ticker, _ in contract_roles})
            != len(contract_roles)
            or {
                ticker for ticker, _ in contract_roles
            }
            - {
                row.contract_ticker for row in candidates
            }
            or any(not role_values for _, role_values in contract_roles)
        ):
            raise ValueError(
                "activity/momentum contract roles are invalid"
            )
        if (
            not roles
            or set(roles) - set(_EXPECTED_ROLES)
            or not candidates
            or len({
                row.contract_ticker for row in candidates
            })
            != len(candidates)
            or {row.underlying for row in candidates} != {underlying}
            or not identities
            or (
                self.target_delta is not None
                and (
                    not isinstance(self.target_delta, Decimal)
                    or not self.target_delta.is_finite()
                    or not Decimal("-1")
                    <= self.target_delta
                    <= Decimal("1")
                )
            )
            or (
                self.target_moneyness is not None
                and (
                    not isinstance(
                        self.target_moneyness,
                        Decimal,
                    )
                    or not self.target_moneyness.is_finite()
                    or self.target_moneyness < 0
                )
            )
            or (
                self.target_dte is not None
                and (
                    type(self.target_dte) is not int
                    or not 0 <= self.target_dte <= 730
                )
            )
        ):
            raise ValueError("activity/momentum group is invalid")
        object.__setattr__(
            self,
            "group_id",
            _text(
                self.group_id,
                "group id",
                maximum=256,
                upper=False,
            ),
        )
        object.__setattr__(
            self,
            "group_version",
            _text(
                self.group_version,
                "group version",
                maximum=128,
                upper=False,
            ),
        )
        object.__setattr__(self, "underlying", underlying)
        object.__setattr__(self, "roles", roles)
        object.__setattr__(self, "candidates", candidates)
        object.__setattr__(
            self,
            "requirement_identities",
            identities,
        )
        object.__setattr__(
            self,
            "matching_strategies",
            strategies,
        )
        object.__setattr__(
            self,
            "contract_roles",
            contract_roles,
        )

    @property
    def contract_tickers(self) -> tuple[str, ...]:
        return tuple(row.contract_ticker for row in self.candidates)

    def dte_bounds(self, session_date: date) -> tuple[int, int]:
        values = tuple(
            row.dte_for(session_date) for row in self.candidates
        )
        return min(values), max(values)


def activity_momentum_group_payload(
    group: ActivityMomentumGroup,
) -> dict[str, object]:
    return {
        "group_id": group.group_id,
        "group_version": group.group_version,
        "underlying": group.underlying,
        "roles": list(group.roles),
        "requirement_identities": list(
            group.requirement_identities
        ),
        "matching_strategies": list(
            group.matching_strategies
        ),
        "contract_roles": [
            {
                "contract_ticker": ticker,
                "roles": list(roles),
            }
            for ticker, roles in group.contract_roles
        ],
        "target_delta": (
            None
            if group.target_delta is None
            else str(group.target_delta)
        ),
        "target_moneyness": (
            None
            if group.target_moneyness is None
            else str(group.target_moneyness)
        ),
        "target_dte": group.target_dte,
        "contract_tickers": list(group.contract_tickers),
        "candidates": [
            activity_momentum_candidate_payload(row)
            for row in group.candidates
        ],
    }


def activity_momentum_group_from_payload(
    value: object,
) -> ActivityMomentumGroup:
    if not isinstance(value, Mapping):
        raise TypeError("activity/momentum group must be an object")
    group = ActivityMomentumGroup(
        group_id=str(value["group_id"]),
        group_version=str(value["group_version"]),
        underlying=str(value["underlying"]),
        roles=tuple(str(item) for item in value["roles"]),
        candidates=tuple(
            activity_momentum_candidate_from_payload(row)
            for row in value["candidates"]
        ),
        requirement_identities=tuple(
            str(item) for item in value["requirement_identities"]
        ),
        matching_strategies=tuple(
            str(item)
            for item in value.get("matching_strategies", ())
        ),
        contract_roles=tuple(
            (
                str(row["contract_ticker"]),
                tuple(str(item) for item in row["roles"]),
            )
            for row in value.get("contract_roles", ())
        ),
        target_delta=(
            None
            if value.get("target_delta") is None
            else Decimal(str(value["target_delta"]))
        ),
        target_moneyness=(
            None
            if value.get("target_moneyness") is None
            else Decimal(str(value["target_moneyness"]))
        ),
        target_dte=(
            None
            if value.get("target_dte") is None
            else int(value["target_dte"])
        ),
    )
    if activity_momentum_group_payload(group) != dict(value):
        raise ValueError("activity/momentum group payload changed")
    return group


@dataclass(frozen=True, slots=True)
class ActivityMomentumLastTradingEvidence:
    expiration: date
    last_trading_session_date: date
    last_trading_at: datetime
    convention: str
    evidence_reference: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.expiration, date)
            or isinstance(self.expiration, datetime)
            or not isinstance(self.last_trading_session_date, date)
            or isinstance(self.last_trading_session_date, datetime)
            or self.last_trading_session_date > self.expiration
        ):
            raise ValueError("last-trading date evidence is invalid")
        object.__setattr__(
            self,
            "last_trading_at",
            _utc(self.last_trading_at, "last_trading_at"),
        )
        object.__setattr__(
            self,
            "convention",
            _text(
                self.convention,
                "last trading convention",
                maximum=80,
            ),
        )
        object.__setattr__(
            self,
            "evidence_reference",
            _text(
                self.evidence_reference,
                "last trading evidence reference",
                maximum=256,
                upper=False,
            ),
        )


@dataclass(frozen=True, slots=True)
class ActivityMomentumExpiryCalendarEvidence:
    underlying: str
    target_session_date: date
    calendar_name: str
    week_end_session_date: date
    product_convention: str
    last_trading_evidence: tuple[
        ActivityMomentumLastTradingEvidence,
        ...,
    ]
    evidence_references: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.target_session_date, date)
            or isinstance(self.target_session_date, datetime)
            or not isinstance(self.week_end_session_date, date)
            or isinstance(self.week_end_session_date, datetime)
            or self.week_end_session_date < self.target_session_date
            or (
                self.week_end_session_date
                - self.target_session_date
            ).days
            > 6
        ):
            raise ValueError("expiry calendar dates are invalid")
        last_trading = tuple(sorted(
            self.last_trading_evidence,
            key=lambda row: row.expiration,
        ))
        if len({
            row.expiration for row in last_trading
        }) != len(last_trading):
            raise ValueError("last-trading expiry evidence is duplicated")
        object.__setattr__(
            self,
            "underlying",
            _text(
                self.underlying,
                "expiry calendar underlying",
                maximum=16,
            ),
        )
        object.__setattr__(
            self,
            "calendar_name",
            _text(
                self.calendar_name,
                "expiry calendar name",
                maximum=40,
            ),
        )
        object.__setattr__(
            self,
            "product_convention",
            _text(
                self.product_convention,
                "product convention",
                maximum=80,
            ),
        )
        object.__setattr__(
            self,
            "last_trading_evidence",
            last_trading,
        )
        object.__setattr__(
            self,
            "evidence_references",
            tuple(sorted({
                _text(
                    value,
                    "expiry calendar evidence reference",
                    maximum=256,
                    upper=False,
                )
                for value in self.evidence_references
            })),
        )


@dataclass(frozen=True, slots=True)
class ActivityMomentumShortExpiryEvidence:
    underlying: str
    target_session_date: date
    preparation_at: datetime
    calendar_name: str
    week_end_session_date: date
    product_convention: str
    candidate_expiries: tuple[date, ...]
    eligible_expiries: tuple[date, ...]
    near_expiry: date | None
    end_of_week_expiry: date | None
    resolution_reason: str
    near_end_of_week_coincide: bool
    single_expiry: bool
    same_day_enabled: bool
    last_trading_evidence: tuple[
        ActivityMomentumLastTradingEvidence,
        ...,
    ]
    evidence_references: tuple[str, ...]
    shortfall_reasons: tuple[str, ...]
    selected_counts: tuple[
        tuple[date, OptionSide, int],
        ...,
    ] = ()
    ranking_anchor_contract_count: int = 0
    fallback_contract_count: int = 0
    displaced_coverage_contract_count: int = 0

    def __post_init__(self) -> None:
        candidate_expiries = tuple(sorted(set(
            self.candidate_expiries
        )))
        eligible_expiries = tuple(sorted(set(
            self.eligible_expiries
        )))
        if (
            not isinstance(self.target_session_date, date)
            or isinstance(self.target_session_date, datetime)
            or not isinstance(self.week_end_session_date, date)
            or isinstance(self.week_end_session_date, datetime)
            or self.week_end_session_date < self.target_session_date
            or not set(eligible_expiries).issubset(
                candidate_expiries
            )
            or any(
                (expiry - self.target_session_date).days
                not in range(0, 6)
                for expiry in candidate_expiries
            )
            or (
                self.near_expiry is not None
                and self.near_expiry not in eligible_expiries
            )
            or (
                self.end_of_week_expiry is not None
                and self.end_of_week_expiry
                not in eligible_expiries
            )
            or self.near_end_of_week_coincide
            is not (
                self.near_expiry is not None
                and self.near_expiry == self.end_of_week_expiry
            )
            or self.single_expiry is not (
                len(eligible_expiries) == 1
            )
            or type(self.same_day_enabled) is not bool
            or any(
                type(value) is not int or value < 0
                for value in (
                    self.ranking_anchor_contract_count,
                    self.fallback_contract_count,
                    self.displaced_coverage_contract_count,
                )
            )
        ):
            raise ValueError("short-expiry evidence is invalid")
        selected_counts = tuple(sorted(
            self.selected_counts,
            key=lambda row: (row[0], row[1].value),
        ))
        if any(
            not isinstance(expiry, date)
            or isinstance(expiry, datetime)
            or not isinstance(side, OptionSide)
            or type(count) is not int
            or count <= 0
            for expiry, side, count in selected_counts
        ):
            raise ValueError("short-expiry selected counts are invalid")
        object.__setattr__(
            self,
            "underlying",
            _text(
                self.underlying,
                "short-expiry underlying",
                maximum=16,
            ),
        )
        object.__setattr__(
            self,
            "preparation_at",
            _utc(self.preparation_at, "preparation_at"),
        )
        object.__setattr__(
            self,
            "calendar_name",
            _text(
                self.calendar_name,
                "short-expiry calendar name",
                maximum=40,
            ),
        )
        object.__setattr__(
            self,
            "product_convention",
            _text(
                self.product_convention,
                "short-expiry product convention",
                maximum=80,
            ),
        )
        object.__setattr__(
            self,
            "resolution_reason",
            _text(
                self.resolution_reason,
                "short-expiry resolution reason",
            ),
        )
        object.__setattr__(
            self,
            "candidate_expiries",
            candidate_expiries,
        )
        object.__setattr__(
            self,
            "eligible_expiries",
            eligible_expiries,
        )
        object.__setattr__(
            self,
            "last_trading_evidence",
            tuple(sorted(
                self.last_trading_evidence,
                key=lambda row: row.expiration,
            )),
        )
        object.__setattr__(
            self,
            "evidence_references",
            tuple(sorted({
                _text(
                    value,
                    "short-expiry evidence reference",
                    maximum=256,
                    upper=False,
                )
                for value in self.evidence_references
            })),
        )
        object.__setattr__(
            self,
            "shortfall_reasons",
            tuple(sorted({
                _text(value, "short-expiry shortfall reason")
                for value in self.shortfall_reasons
            })),
        )
        object.__setattr__(self, "selected_counts", selected_counts)


def _last_trading_evidence_payload(
    evidence: ActivityMomentumLastTradingEvidence,
) -> dict[str, object]:
    return {
        "expiration": evidence.expiration.isoformat(),
        "last_trading_session_date": (
            evidence.last_trading_session_date.isoformat()
        ),
        "last_trading_at": evidence.last_trading_at.isoformat(),
        "convention": evidence.convention,
        "evidence_reference": evidence.evidence_reference,
    }


def _last_trading_evidence_from_payload(
    value: object,
) -> ActivityMomentumLastTradingEvidence:
    if not isinstance(value, Mapping):
        raise TypeError("last-trading evidence must be an object")
    evidence = ActivityMomentumLastTradingEvidence(
        expiration=date.fromisoformat(str(value["expiration"])),
        last_trading_session_date=date.fromisoformat(
            str(value["last_trading_session_date"])
        ),
        last_trading_at=datetime.fromisoformat(
            str(value["last_trading_at"])
        ),
        convention=str(value["convention"]),
        evidence_reference=str(value["evidence_reference"]),
    )
    if _last_trading_evidence_payload(evidence) != dict(value):
        raise ValueError("last-trading evidence payload changed")
    return evidence


def activity_momentum_short_expiry_evidence_payload(
    evidence: ActivityMomentumShortExpiryEvidence,
) -> dict[str, object]:
    return {
        "underlying": evidence.underlying,
        "target_session_date": (
            evidence.target_session_date.isoformat()
        ),
        "preparation_at": evidence.preparation_at.isoformat(),
        "calendar_name": evidence.calendar_name,
        "week_end_session_date": (
            evidence.week_end_session_date.isoformat()
        ),
        "product_convention": evidence.product_convention,
        "candidate_expiries": [
            value.isoformat() for value in evidence.candidate_expiries
        ],
        "eligible_expiries": [
            value.isoformat() for value in evidence.eligible_expiries
        ],
        "near_expiry": (
            None
            if evidence.near_expiry is None
            else evidence.near_expiry.isoformat()
        ),
        "end_of_week_expiry": (
            None
            if evidence.end_of_week_expiry is None
            else evidence.end_of_week_expiry.isoformat()
        ),
        "resolution_reason": evidence.resolution_reason,
        "near_end_of_week_coincide": (
            evidence.near_end_of_week_coincide
        ),
        "single_expiry": evidence.single_expiry,
        "same_day_enabled": evidence.same_day_enabled,
        "last_trading_evidence": [
            _last_trading_evidence_payload(row)
            for row in evidence.last_trading_evidence
        ],
        "evidence_references": list(evidence.evidence_references),
        "shortfall_reasons": list(evidence.shortfall_reasons),
        "selected_counts": [
            {
                "expiration": expiration.isoformat(),
                "option_side": side.value,
                "count": count,
            }
            for expiration, side, count in evidence.selected_counts
        ],
        "ranking_anchor_contract_count": (
            evidence.ranking_anchor_contract_count
        ),
        "fallback_contract_count": evidence.fallback_contract_count,
        "displaced_coverage_contract_count": (
            evidence.displaced_coverage_contract_count
        ),
    }


def activity_momentum_short_expiry_evidence_from_payload(
    value: object,
) -> ActivityMomentumShortExpiryEvidence:
    if not isinstance(value, Mapping):
        raise TypeError("short-expiry evidence must be an object")
    evidence = ActivityMomentumShortExpiryEvidence(
        underlying=str(value["underlying"]),
        target_session_date=date.fromisoformat(
            str(value["target_session_date"])
        ),
        preparation_at=datetime.fromisoformat(
            str(value["preparation_at"])
        ),
        calendar_name=str(value["calendar_name"]),
        week_end_session_date=date.fromisoformat(
            str(value["week_end_session_date"])
        ),
        product_convention=str(value["product_convention"]),
        candidate_expiries=tuple(
            date.fromisoformat(str(item))
            for item in value["candidate_expiries"]
        ),
        eligible_expiries=tuple(
            date.fromisoformat(str(item))
            for item in value["eligible_expiries"]
        ),
        near_expiry=(
            None
            if value.get("near_expiry") is None
            else date.fromisoformat(str(value["near_expiry"]))
        ),
        end_of_week_expiry=(
            None
            if value.get("end_of_week_expiry") is None
            else date.fromisoformat(
                str(value["end_of_week_expiry"])
            )
        ),
        resolution_reason=str(value["resolution_reason"]),
        near_end_of_week_coincide=bool(
            value["near_end_of_week_coincide"]
        ),
        single_expiry=bool(value["single_expiry"]),
        same_day_enabled=bool(value["same_day_enabled"]),
        last_trading_evidence=tuple(
            _last_trading_evidence_from_payload(row)
            for row in value["last_trading_evidence"]
        ),
        evidence_references=tuple(
            str(item) for item in value["evidence_references"]
        ),
        shortfall_reasons=tuple(
            str(item) for item in value["shortfall_reasons"]
        ),
        selected_counts=tuple(
            (
                date.fromisoformat(str(row["expiration"])),
                OptionSide(str(row["option_side"])),
                int(row["count"]),
            )
            for row in value["selected_counts"]
        ),
        ranking_anchor_contract_count=int(
            value["ranking_anchor_contract_count"]
        ),
        fallback_contract_count=int(
            value["fallback_contract_count"]
        ),
        displaced_coverage_contract_count=int(
            value["displaced_coverage_contract_count"]
        ),
    )
    if (
        activity_momentum_short_expiry_evidence_payload(evidence)
        != dict(value)
    ):
        raise ValueError("short-expiry evidence payload changed")
    return evidence


@dataclass(frozen=True, slots=True)
class ActivityMomentumResolution:
    discovered_count: int
    filtered_count: int
    candidate_pool_count: int
    shortlisted_count: int
    group_enumeration_count: int
    shortlisted_candidates: tuple[
        ActivityMomentumCandidate,
        ...,
    ]
    groups: tuple[ActivityMomentumGroup, ...]
    rejection_counts: tuple[tuple[str, int], ...]
    ranking_as_of: tuple[datetime, ...]
    short_expiry_evidence: tuple[
        ActivityMomentumShortExpiryEvidence,
        ...,
    ] = ()
    short_delta_override: Decimal | None = None

    def __post_init__(self) -> None:
        counts = (
            self.discovered_count,
            self.filtered_count,
            self.candidate_pool_count,
            self.shortlisted_count,
            self.group_enumeration_count,
        )
        if (
            any(type(value) is not int or value < 0 for value in counts)
            or self.filtered_count > self.discovered_count
            or self.candidate_pool_count > self.filtered_count
            or self.shortlisted_count > self.candidate_pool_count
            or self.group_enumeration_count != len(self.groups)
            or self.shortlisted_count
            != len(self.shortlisted_candidates)
            or any(
                type(count) is not int or count <= 0
                for _, count in self.rejection_counts
            )
            or (
                self.short_delta_override is not None
                and (
                    not isinstance(
                        self.short_delta_override,
                        Decimal,
                    )
                    or not self.short_delta_override.is_finite()
                    or not Decimal("0")
                    < self.short_delta_override
                    <= Decimal("1")
                )
            )
        ):
            raise ValueError(
                "activity/momentum resolution is invalid"
            )
        object.__setattr__(
            self,
            "shortlisted_candidates",
            tuple(sorted(
                self.shortlisted_candidates,
                key=lambda row: row.contract_ticker,
            )),
        )
        object.__setattr__(
            self,
            "groups",
            tuple(sorted(
                self.groups,
                key=lambda row: row.group_id,
            )),
        )
        object.__setattr__(
            self,
            "rejection_counts",
            tuple(sorted(
                (
                    _text(reason, "resolution rejection reason"),
                    count,
                )
                for reason, count in self.rejection_counts
            )),
        )
        object.__setattr__(
            self,
            "ranking_as_of",
            tuple(sorted({
                _utc(value, "ranking_as_of")
                for value in self.ranking_as_of
            })),
        )
        expiry_evidence = tuple(sorted(
            self.short_expiry_evidence,
            key=lambda row: row.underlying,
        ))
        if len({
            row.underlying for row in expiry_evidence
        }) != len(expiry_evidence):
            raise ValueError("short-expiry evidence is duplicated")
        object.__setattr__(
            self,
            "short_expiry_evidence",
            expiry_evidence,
        )


def _criterion_candidate_rank(
    candidate: ActivityMomentumCandidate,
    criterion: ActivityMomentumSelectionCriterion,
    *,
    session_date: date,
    short_delta_override: Decimal | None = None,
) -> tuple[object, ...]:
    spread = candidate.relative_spread
    notional = candidate.completed_session_notional
    target_delta = (
        short_delta_override
        if (
            criterion.criterion_id == "SHORT_ACTIVITY"
            and short_delta_override is not None
        )
        else criterion.target_absolute_delta
    )
    if candidate.delta is not None:
        relevance_distance = abs(
            abs(candidate.delta)
            - target_delta
        )
        missing_delta = False
    elif candidate.spot is not None:
        relevance_distance = abs(
            abs(candidate.strike - candidate.spot)
            / candidate.spot
            - criterion.target_moneyness_fraction
        )
        missing_delta = True
    else:
        relevance_distance = Decimal("Infinity")
        missing_delta = True
    return (
        candidate.bid is None,
        Decimal("Infinity") if spread is None else spread,
        candidate.prior_open_interest is None,
        -(
            Decimal("-1")
            if notional is None
            else notional
        ),
        -(
            -1
            if candidate.completed_session_volume is None
            else candidate.completed_session_volume
        ),
        missing_delta,
        relevance_distance,
        abs(
            candidate.dte_for(session_date)
            - criterion.target_dte
        ),
        candidate.contract_ticker,
    )


def _resolve_short_expiry_evidence(
    profile: ActivityMomentumProfile,
    *,
    session_date: date,
    preparation_at: datetime,
    selected: tuple[ActivityMomentumCandidate, ...],
    eligible: tuple[
        tuple[
            ActivityMomentumCandidate,
            ActivityMomentumSelectionCriterion,
        ],
        ...,
    ],
    calendars: Mapping[
        str,
        ActivityMomentumExpiryCalendarEvidence,
    ],
    same_day_enabled: bool,
) -> tuple[ActivityMomentumShortExpiryEvidence, ...]:
    underlyings = tuple(sorted({
        row.underlying for row in selected
    }))
    values = []
    for underlying in underlyings:
        calendar = calendars.get(underlying)
        candidate_expiries = tuple(sorted({
            row.expiration
            for row in selected
            if row.underlying == underlying
            and 0 <= row.dte_for(session_date) <= 5
        }))
        eligible_expiries = tuple(sorted({
            row.expiration
            for row, criterion in eligible
            if row.underlying == underlying
            and criterion.criterion_id == "SHORT_ACTIVITY"
        }))
        shortfalls = []
        if calendar is None:
            near_expiry = None
            end_of_week_expiry = None
            calendar_name = "UNAVAILABLE"
            week_end_session_date = session_date
            product_convention = "UNAVAILABLE"
            last_trading_evidence = ()
            evidence_references = ()
            shortfalls.append("EXPIRY_CALENDAR_EVIDENCE_UNAVAILABLE")
        else:
            calendar_name = calendar.calendar_name
            week_end_session_date = calendar.week_end_session_date
            product_convention = calendar.product_convention
            last_trading_evidence = tuple(
                row
                for row in calendar.last_trading_evidence
                if row.expiration in candidate_expiries
            )
            evidence_references = calendar.evidence_references
            near_expiry = (
                None
                if not eligible_expiries
                else min(
                    eligible_expiries,
                    key=lambda expiry: (
                        abs((expiry - session_date).days - 2),
                        (expiry - session_date).days,
                        expiry,
                    ),
                )
            )
            end_of_week_candidates = tuple(
                expiry
                for expiry in eligible_expiries
                if session_date
                <= expiry
                <= calendar.week_end_session_date
            )
            end_of_week_expiry = (
                None
                if not end_of_week_candidates
                else max(end_of_week_candidates)
            )
            if end_of_week_expiry is None:
                shortfalls.append(
                    "LISTED_END_OF_WEEK_EXPIRY_UNAVAILABLE"
                )
        if not candidate_expiries:
            shortfalls.append("LISTED_SHORT_EXPIRY_UNAVAILABLE")
        if not eligible_expiries:
            shortfalls.append("ELIGIBLE_SHORT_EXPIRY_UNAVAILABLE")
        if near_expiry is None:
            shortfalls.append("NEAR_EXPIRY_UNAVAILABLE")
        if (
            0 in {
                (expiry - session_date).days
                for expiry in candidate_expiries
            }
            and not same_day_enabled
        ):
            shortfalls.append("SAME_DAY_DISABLED")
        if not eligible_expiries:
            resolution_reason = "NO_ELIGIBLE_LISTED_SHORT_EXPIRY"
        elif len(eligible_expiries) == 1:
            resolution_reason = "SINGLE_LISTED_SHORT_EXPIRY"
        elif (
            near_expiry is not None
            and near_expiry == end_of_week_expiry
        ):
            resolution_reason = "NEAR_AND_END_OF_WEEK_COINCIDE"
        elif end_of_week_expiry is None:
            resolution_reason = "NEAR_ONLY_END_OF_WEEK_UNAVAILABLE"
        else:
            resolution_reason = "DISTINCT_NEAR_AND_END_OF_WEEK"
        values.append(ActivityMomentumShortExpiryEvidence(
            underlying=underlying,
            target_session_date=session_date,
            preparation_at=preparation_at,
            calendar_name=calendar_name,
            week_end_session_date=week_end_session_date,
            product_convention=product_convention,
            candidate_expiries=candidate_expiries,
            eligible_expiries=eligible_expiries,
            near_expiry=near_expiry,
            end_of_week_expiry=end_of_week_expiry,
            resolution_reason=resolution_reason,
            near_end_of_week_coincide=(
                near_expiry is not None
                and near_expiry == end_of_week_expiry
            ),
            single_expiry=len(eligible_expiries) == 1,
            same_day_enabled=same_day_enabled,
            last_trading_evidence=last_trading_evidence,
            evidence_references=evidence_references,
            shortfall_reasons=tuple(shortfalls),
        ))
    return tuple(values)


def _expiry_preference_protected_tickers(
    profile: ActivityMomentumProfile,
    *,
    session_date: date,
    eligible: tuple[
        tuple[
            ActivityMomentumCandidate,
            ActivityMomentumSelectionCriterion,
        ],
        ...,
    ],
    evidence: tuple[ActivityMomentumShortExpiryEvidence, ...],
    short_delta_override: Decimal | None,
) -> frozenset[str]:
    protected: set[str] = set()
    eligible_by_underlying = {}
    for row in eligible:
        eligible_by_underlying.setdefault(
            row[0].underlying,
            [],
        ).append(row)
    for expiry_evidence in evidence:
        targets: tuple[tuple[date, int], ...]
        if expiry_evidence.near_expiry is None:
            targets = ()
        elif expiry_evidence.near_end_of_week_coincide:
            targets = ((
                expiry_evidence.near_expiry,
                profile.short_basket_contract_target,
            ),)
        elif expiry_evidence.end_of_week_expiry is None:
            targets = ((
                expiry_evidence.near_expiry,
                profile.short_basket_contract_target,
            ),)
        else:
            targets = (
                (
                    expiry_evidence.near_expiry,
                    profile.short_near_expiry_contract_target,
                ),
                (
                    expiry_evidence.end_of_week_expiry,
                    profile.short_end_of_week_contract_target,
                ),
            )
        for expiration, target in targets:
            rows = sorted(
                (
                    row
                    for row in eligible_by_underlying.get(
                        expiry_evidence.underlying,
                        (),
                    )
                    if row[0].expiration == expiration
                    and row[1].criterion_id == "SHORT_ACTIVITY"
                ),
                key=lambda row: _criterion_candidate_rank(
                    row[0],
                    row[1],
                    session_date=session_date,
                    short_delta_override=short_delta_override,
                ),
            )
            chosen = []
            for side in (OptionSide.CALL, OptionSide.PUT):
                value = next(
                    (
                        row
                        for row in rows
                        if row[0].option_side is side
                        and row not in chosen
                    ),
                    None,
                )
                if value is not None and len(chosen) < target:
                    chosen.append(value)
            for row in rows:
                if len(chosen) >= target:
                    break
                if row not in chosen:
                    chosen.append(row)
            protected.update(
                row[0].contract_ticker for row in chosen
            )
    return frozenset(protected)


def _shortlist_activity_momentum_candidates(
    profile: ActivityMomentumProfile,
    *,
    session_date: date,
    candidates: Iterable[ActivityMomentumCandidate],
    enabled_strategies: tuple[str, ...],
    required_contract_tickers: frozenset[str],
    strategy_policy_path: Path,
    preparation_at: datetime | None = None,
    expiry_calendars: tuple[
        ActivityMomentumExpiryCalendarEvidence,
        ...,
    ] = (),
    same_day_enabled: bool | None = None,
    short_delta_override: Decimal | None = None,
) -> tuple[
    tuple[ActivityMomentumCandidate, ...],
    int,
    int,
    Counter[str],
    tuple[ActivityMomentumGroup, ...],
    tuple[ActivityMomentumShortExpiryEvidence, ...],
]:
    selected = tuple(sorted(
        candidates,
        key=lambda row: row.contract_ticker,
    ))
    if not selected:
        return (), 0, 0, Counter(), (), ()
    if len({
        row.contract_ticker for row in selected
    }) != len(selected):
        raise ValueError(
            "activity/momentum candidates contain duplicates"
        )
    by_ticker = {
        row.contract_ticker: row for row in selected
    }
    if not required_contract_tickers.issubset(by_ticker):
        raise ValueError("required strategy contract is unavailable")
    calendars = {
        row.underlying: row for row in expiry_calendars
    }
    if len(calendars) != len(expiry_calendars):
        raise ValueError("expiry calendar evidence is duplicated")
    if (
        profile.version
        == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
    ):
        if preparation_at is None:
            raise ValueError(
                "v3 expiry selection requires preparation_at"
            )
        resolved_preparation_at = _utc(
            preparation_at,
            "preparation_at",
        )
        if any(
            row.target_session_date != session_date
            or row.underlying not in {
                candidate.underlying for candidate in selected
            }
            for row in expiry_calendars
        ):
            raise ValueError(
                "expiry calendar target session changed"
            )
        resolved_same_day_enabled = (
            profile.same_day_enabled
            if same_day_enabled is None
            else same_day_enabled
        )
        if type(resolved_same_day_enabled) is not bool:
            raise TypeError("same_day_enabled must be boolean")
    else:
        if (
            preparation_at is not None
            or expiry_calendars
            or same_day_enabled is not None
            or short_delta_override is not None
        ):
            raise ValueError(
                "v1/v2 expiry selection inputs changed"
            )
        resolved_preparation_at = max(
            row.observed_at for row in selected
        )
        resolved_same_day_enabled = False
    eligible: list[
        tuple[
            ActivityMomentumCandidate,
            ActivityMomentumSelectionCriterion,
        ]
    ] = []
    rejected: Counter[str] = Counter()
    extension_enabled = bool(
        enabled_strategies or required_contract_tickers
    )
    for candidate in selected:
        dte = candidate.dte_for(session_date)
        criterion = next(
            (
                row
                for row in profile.selection_criteria
                if row.minimum_dte <= dte <= row.maximum_dte
            ),
            None,
        )
        reason = None
        if criterion is None:
            reason = "DTE_OUTSIDE_ENABLED_DISCOVERY"
        elif dte > profile.default_discovery_maximum_dte and (
            not extension_enabled
            or candidate.contract_ticker
            not in required_contract_tickers
            and not enabled_strategies
        ):
            reason = "STRATEGY_EXTENSION_DISABLED"
        elif candidate.deliverable != "STANDARD":
            reason = "UNSUPPORTED_DELIVERABLE"
        elif candidate.unavailable_reasons:
            reason = "CATALOG_OR_CAPABILITY_UNAVAILABLE"
        elif (
            candidate.bid is None
            or candidate.ask is None
            or candidate.quote_as_of is None
        ):
            reason = "DATED_QUOTE_UNAVAILABLE"
        elif (
            candidate.observed_at - candidate.quote_as_of
        ).total_seconds() > criterion.maximum_quote_age_seconds:
            reason = "DATED_QUOTE_STALE"
        elif (
            profile.version
            == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
            and 0 <= dte <= 5
            and calendars.get(candidate.underlying) is None
        ):
            reason = "SHORT_EXPIRY_CALENDAR_EVIDENCE_UNAVAILABLE"
        elif (
            profile.version
            == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
            and 0 <= dte <= 5
            and calendars[
                candidate.underlying
            ].product_convention
            not in profile.supported_last_trading_conventions
        ):
            reason = "SHORT_EXPIRY_PRODUCT_CONVENTION_UNSUPPORTED"
        elif (
            profile.version
            == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
            and dte == 0
            and not resolved_same_day_enabled
        ):
            reason = "SAME_DAY_DISABLED"
        elif (
            profile.version
            == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
            and dte == 0
        ):
            calendar = calendars.get(candidate.underlying)
            last_trading = (
                None
                if calendar is None
                else next(
                    (
                        row
                        for row in calendar.last_trading_evidence
                        if row.expiration == candidate.expiration
                    ),
                    None,
                )
            )
            if (
                calendar is None
                or calendar.product_convention
                not in profile.supported_last_trading_conventions
                or last_trading is None
                or last_trading.convention
                != calendar.product_convention
            ):
                reason = (
                    "SAME_DAY_LAST_TRADING_EVIDENCE_UNAVAILABLE"
                )
            elif (
                last_trading.last_trading_session_date
                != session_date
            ):
                reason = "SAME_DAY_NOT_LAST_TRADING_SESSION"
            elif (
                resolved_preparation_at
                > last_trading.last_trading_at
            ):
                reason = "SAME_DAY_LAST_TRADING_PASSED"
        elif (
            candidate.relative_spread is None
            or candidate.relative_spread
            > criterion.maximum_relative_spread
        ):
            reason = "ROLE_SPREAD_LIMIT"
        elif candidate.spot is None:
            reason = "SPOT_UNAVAILABLE_FOR_STRIKE_FILTER"
        elif (
            abs(candidate.strike - candidate.spot)
            / candidate.spot
            > criterion.maximum_moneyness_fraction
        ):
            reason = "ROLE_STRIKE_DISTANCE_LIMIT"
        elif (
            candidate.completed_session_volume is not None
            and candidate.completed_session_volume
            < criterion.minimum_completed_session_volume
        ):
            reason = "ROLE_VOLUME_LIMIT"
        elif (
            criterion.minimum_prior_open_interest is not None
            and candidate.prior_open_interest is not None
            and candidate.prior_open_interest
            < criterion.minimum_prior_open_interest
        ):
            reason = "ROLE_OPEN_INTEREST_LIMIT"
        if reason is not None:
            rejected[reason] += 1
            continue
        eligible.append((candidate, criterion))

    expiry_evidence = (
        _resolve_short_expiry_evidence(
            profile,
            session_date=session_date,
            preparation_at=resolved_preparation_at,
            selected=selected,
            eligible=tuple(eligible),
            calendars=calendars,
            same_day_enabled=resolved_same_day_enabled,
        )
        if profile.version
        == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
        else ()
    )
    expiry_protected_tickers = (
        _expiry_preference_protected_tickers(
            profile,
            session_date=session_date,
            eligible=tuple(eligible),
            evidence=expiry_evidence,
            short_delta_override=short_delta_override,
        )
        if expiry_evidence
        else frozenset()
    )
    pooled: list[
        tuple[
            ActivityMomentumCandidate,
            ActivityMomentumSelectionCriterion,
        ]
    ] = []
    by_underlying: dict[
        str,
        list[
            tuple[
                ActivityMomentumCandidate,
                ActivityMomentumSelectionCriterion,
            ]
        ],
    ] = {}
    for row in eligible:
        by_underlying.setdefault(row[0].underlying, []).append(row)
    for underlying, rows in sorted(by_underlying.items()):
        required_rows = tuple(
            row
            for row in rows
            if row[0].contract_ticker
            in required_contract_tickers
        )
        expiry_rows = tuple(
            row
            for row in rows
            if row[0].contract_ticker
            in expiry_protected_tickers
            and row[0].contract_ticker
            not in {
                required_row[0].contract_ticker
                for required_row in required_rows
            }
        )
        if (
            len(required_rows)
            > profile.maximum_candidate_pool_per_underlying
        ):
            raise ValueError(
                "required strategy contracts exceed candidate pool bound"
            )
        ordered = sorted(
            rows,
            key=lambda row: _criterion_candidate_rank(
                row[0],
                row[1],
                session_date=session_date,
                short_delta_override=short_delta_override,
            ),
        )
        keep = list(required_rows)
        keep_tickers = {
            row[0].contract_ticker for row in keep
        }
        for row in expiry_rows:
            if len(keep) >= (
                profile.maximum_candidate_pool_per_underlying
            ):
                rejected[
                    "EXPIRY_PREFERENCE_CANDIDATE_POOL_SHORTFALL"
                ] += 1
                continue
            keep.append(row)
            keep_tickers.add(row[0].contract_ticker)
        for row in ordered:
            if len(keep) >= (
                profile.maximum_candidate_pool_per_underlying
            ):
                break
            if row[0].contract_ticker not in keep_tickers:
                keep.append(row)
                keep_tickers.add(row[0].contract_ticker)
        rejected["CANDIDATE_POOL_BOUND"] += len(rows) - len(keep)
        pooled.extend(keep)

    pooled_by_ticker = {
        row[0].contract_ticker: row for row in pooled
    }
    protected_groups = (
        ()
        if not pooled
        else _resolve_activity_momentum_groups(
            profile,
            session_date=session_date,
            candidates=tuple(
                row[0] for row in pooled
            ),
            enabled_strategies=enabled_strategies,
            required_contract_tickers=(
                required_contract_tickers
            ),
            strategy_policy_path=strategy_policy_path,
            rejection_counts=rejected,
            include_coverage_groups=False,
            include_strategy_groups=True,
            short_delta_override=short_delta_override,
        )
    )
    accepted_protected_groups = []
    protected_tickers: set[str] = set()
    protected_by_underlying: dict[str, set[str]] = {}
    protected_by_bucket: Counter[
        tuple[str, str, OptionSide]
    ] = Counter()

    def explicit_required_group(
        group: ActivityMomentumGroup,
    ) -> bool:
        return any(
            "REQUIRED_LEG" in roles
            for _, roles in group.contract_roles
        )

    for group in sorted(
        protected_groups,
        key=lambda row: (
            0 if explicit_required_group(row) else 1,
            _group_rank(
                row,
                profile,
                session_date=session_date,
            ),
        ),
    ):
        incremental = tuple(
            ticker
            for ticker in group.contract_tickers
            if ticker not in protected_tickers
        )
        underlying_tickers = protected_by_underlying.setdefault(
            group.underlying,
            set(),
        )
        exceeds_underlying = (
            len(underlying_tickers | set(incremental))
            > profile.maximum_shortlisted_candidates_per_underlying
        )
        bucket_increments: Counter[
            tuple[str, str, OptionSide]
        ] = Counter()
        exceeds_bucket = False
        for ticker in incremental:
            candidate, criterion = pooled_by_ticker[ticker]
            key = (
                candidate.underlying,
                criterion.criterion_id,
                candidate.option_side,
            )
            bucket_increments[key] += 1
            if (
                protected_by_bucket[key]
                + bucket_increments[key]
                > criterion.shortlist_limit_per_side
            ):
                exceeds_bucket = True
        if exceeds_underlying or exceeds_bucket:
            if explicit_required_group(group):
                raise ValueError(
                    "required strategy group exceeds shortlist bound"
                )
            rejected[
                "COMPLETE_GROUP_UNAVAILABLE_SHORTLIST_BOUND"
            ] += 1
            continue
        accepted_protected_groups.append(group)
        protected_tickers.update(incremental)
        underlying_tickers.update(incremental)
        protected_by_bucket.update(bucket_increments)
    protected_tickers.update(expiry_protected_tickers)

    shortlisted: list[
        tuple[
            ActivityMomentumCandidate,
            ActivityMomentumSelectionCriterion,
        ]
    ] = []
    shortlist_buckets: dict[
        tuple[str, str, OptionSide],
        list[
            tuple[
                ActivityMomentumCandidate,
                ActivityMomentumSelectionCriterion,
            ]
        ],
    ] = {}
    for row in pooled:
        shortlist_buckets.setdefault(
            (
                row[0].underlying,
                row[1].criterion_id,
                row[0].option_side,
            ),
            [],
        ).append(row)
    for _key, rows in sorted(shortlist_buckets.items()):
        ordered = sorted(
            rows,
            key=lambda row: _criterion_candidate_rank(
                row[0],
                row[1],
                session_date=session_date,
                short_delta_override=short_delta_override,
            ),
        )
        limit = rows[0][1].shortlist_limit_per_side
        protected_rows = tuple(
            row
            for row in ordered
            if row[0].contract_ticker in protected_tickers
        )
        if len(protected_rows) > limit:
            raise ValueError(
                "protected strategy legs exceed shortlist bound"
            )
        keep = list(protected_rows)
        keep_tickers = {
            row[0].contract_ticker for row in keep
        }
        for row in ordered:
            if len(keep) >= limit:
                break
            if row[0].contract_ticker not in keep_tickers:
                keep.append(row)
                keep_tickers.add(row[0].contract_ticker)
        shortlisted.extend(keep)
        rejected["ROLE_SHORTLIST_BOUND"] += max(
            0,
            len(ordered) - len(keep),
        )

    bounded: list[
        tuple[
            ActivityMomentumCandidate,
            ActivityMomentumSelectionCriterion,
        ]
    ] = []
    shortlisted_by_underlying: dict[
        str,
        list[
            tuple[
                ActivityMomentumCandidate,
                ActivityMomentumSelectionCriterion,
            ]
        ],
    ] = {}
    for row in shortlisted:
        shortlisted_by_underlying.setdefault(
            row[0].underlying,
            [],
        ).append(row)
    for underlying, rows in sorted(
        shortlisted_by_underlying.items()
    ):
        protected_rows = tuple(
            row
            for row in rows
            if row[0].contract_ticker
            in protected_tickers
        )
        if (
            len(protected_rows)
            > profile.maximum_shortlisted_candidates_per_underlying
        ):
            raise ValueError(
                "protected strategy contracts exceed shortlist bound"
            )
        ordered = sorted(
            rows,
            key=lambda row: _criterion_candidate_rank(
                row[0],
                row[1],
                session_date=session_date,
                short_delta_override=short_delta_override,
            ),
        )
        keep = list(protected_rows)
        keep_tickers = {
            row[0].contract_ticker for row in keep
        }
        for row in ordered:
            if len(keep) >= (
                profile.maximum_shortlisted_candidates_per_underlying
            ):
                break
            if row[0].contract_ticker not in keep_tickers:
                keep.append(row)
                keep_tickers.add(row[0].contract_ticker)
        rejected["UNDERLYING_SHORTLIST_BOUND"] += (
            len(rows) - len(keep)
        )
        bounded.extend(keep)
    for key in tuple(rejected):
        if rejected[key] == 0:
            del rejected[key]
    return (
        tuple(sorted(
            (row[0] for row in bounded),
            key=lambda row: row.contract_ticker,
        )),
        len(eligible),
        len(pooled),
        rejected,
        tuple(sorted(
            accepted_protected_groups,
            key=lambda row: row.group_id,
        )),
        expiry_evidence,
    )


def _resolve_activity_momentum_groups(
    profile: ActivityMomentumProfile,
    *,
    session_date: date,
    candidates: Iterable[ActivityMomentumCandidate],
    enabled_strategies: Iterable[str] = (),
    required_contract_tickers: Iterable[str] = (),
    strategy_policy_path: Path = (
        DEFAULT_ACTIVITY_MOMENTUM_STRATEGY_POLICY_PATH
    ),
    rejection_counts: Counter[str] | None = None,
    include_coverage_groups: bool = True,
    include_strategy_groups: bool = True,
    short_delta_override: Decimal | None = None,
) -> tuple[ActivityMomentumGroup, ...]:
    selected = tuple(sorted(
        candidates,
        key=lambda row: row.contract_ticker,
    ))
    if not selected:
        raise ValueError("activity/momentum candidates are empty")
    strategies = tuple(sorted({
        _text(value, "enabled strategy")
        for value in enabled_strategies
    }))
    unsupported = (
        set(strategies) - set(profile.supported_group_strategies)
    )
    if unsupported:
        raise ValueError(
            f"unsupported activity strategy groups: {sorted(unsupported)}"
        )
    required = frozenset(
        _text(value, "required contract ticker", maximum=48)
        for value in required_contract_tickers
    )
    by_ticker = {
        row.contract_ticker: row for row in selected
    }
    if not required.issubset(by_ticker):
        raise ValueError("required strategy contract is unavailable")
    strategy_document = json.loads(
        strategy_policy_path.read_text(encoding="utf-8")
    )
    if not isinstance(strategy_document, dict):
        raise ValueError("activity strategy policy is invalid")
    strategy_identity = load_strategy_policy_identity(
        strategy_policy_path
    )
    groups: dict[str, ActivityMomentumGroup] = {}
    groups_by_underlying: Counter[str] = Counter()

    def add_group(
        rows,
        *,
        roles,
        matching=(),
        contract_roles=(),
        target_dte=None,
        target_delta=None,
        target_moneyness=None,
        label,
    ):
        values = tuple(sorted(
            rows,
            key=lambda row: row.contract_ticker,
        ))
        if not values:
            return
        underlying = values[0].underlying
        required_group = label.startswith("EXPLICIT_REQUIRED:")
        if (
            groups_by_underlying[underlying]
            >= profile.maximum_group_enumerations_per_underlying
            or groups_by_underlying[underlying]
            >= profile.maximum_complete_groups_per_underlying
        ):
            if required_group:
                raise ValueError(
                    "required strategy groups exceed profile bound"
                )
            if rejection_counts is not None:
                rejection_counts["GROUP_ENUMERATION_BOUND"] += 1
            return
        maximum_dte = max(
            row.dte_for(session_date) for row in values
        )
        criterion = profile.criterion_for(
            dte=maximum_dte,
            roles=tuple(sorted(roles)),
        )
        resolved_target_dte = (
            target_dte
            if target_dte is not None
            else (
                None
                if criterion is None
                else criterion.target_dte
            )
        )
        resolved_target_delta = (
            target_delta
            if target_delta is not None
            else (
                Decimal("0.50")
                if criterion is None
                else (
                    short_delta_override
                    if (
                        criterion.criterion_id
                        == "SHORT_ACTIVITY"
                        and short_delta_override is not None
                    )
                    else criterion.target_absolute_delta
                )
            )
        )
        resolved_target_moneyness = (
            target_moneyness
            if target_moneyness is not None
            else (
                Decimal("0.02")
                if criterion is None
                else criterion.target_moneyness_fraction
            )
        )
        tickers = tuple(row.contract_ticker for row in values)
        group_identity = {
            "profile_sha256": profile.profile_sha256,
            "strategy_policy_sha256": (
                strategy_identity.content_sha256
            ),
            "label": label,
            "roles": tuple(sorted(roles)),
            "matching_strategies": tuple(sorted(matching)),
            "contract_tickers": tickers,
        }
        if (
            profile.version
            == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
        ):
            group_identity["short_delta_override"] = (
                None
                if short_delta_override is None
                else str(short_delta_override)
            )
        group_id = canonical_sha256(group_identity)
        groups[group_id] = ActivityMomentumGroup(
            group_id=group_id,
            group_version=(
                "c6-activity-momentum-resolver-v2"
                if profile.version
                == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
                else "c6-activity-momentum-resolver-v1"
            ),
            underlying=values[0].underlying,
            roles=tuple(sorted(roles)),
            candidates=values,
            requirement_identities=(
                profile.profile_sha256,
                strategy_identity.content_sha256,
            ),
            matching_strategies=tuple(sorted(matching)),
            contract_roles=tuple(contract_roles),
            target_delta=resolved_target_delta,
            target_moneyness=resolved_target_moneyness,
            target_dte=resolved_target_dte,
        )
        groups_by_underlying[underlying] += 1

    long_policy = dict(
        strategy_document.get("long_premium") or {}
    )
    long_min = int(long_policy.get("minimum_dte", 1))
    long_max = int(long_policy.get("maximum_dte", 45))
    required_by_underlying: dict[
        str,
        list[ActivityMomentumCandidate],
    ] = {}
    if include_strategy_groups:
        for ticker in required:
            candidate = by_ticker[ticker]
            required_by_underlying.setdefault(
                candidate.underlying,
                [],
            ).append(candidate)
        for underlying, rows in required_by_underlying.items():
            add_group(
                rows,
                roles=("STRATEGY_REQUIRED",),
                contract_roles=tuple(
                    (row.contract_ticker, ("REQUIRED_LEG",))
                    for row in rows
                ),
                label=f"EXPLICIT_REQUIRED:{underlying}",
            )

    for candidate in selected:
        dte = candidate.dte_for(session_date)
        roles = []
        if include_coverage_groups and 0 <= dte <= 5:
            roles.extend((
                "PARTICIPATION_CONTEXT",
                "DIRECTIONAL_MOMENTUM",
            ))
            if candidate.option_side is OptionSide.CALL:
                roles.append("SHORT_DATED_SWEEP_LIKE")
        elif include_coverage_groups and 6 <= dte <= 21:
            roles.extend((
                "PARTICIPATION_CONTEXT",
                "DIRECTIONAL_MOMENTUM",
            ))
        matching = ()
        if (
            include_strategy_groups
            and
            "DIRECTIONAL_LONG_PREMIUM" in strategies
            and long_min <= dte <= long_max
        ):
            matching = ("DIRECTIONAL_LONG_PREMIUM",)
            if not roles:
                roles.append("STRATEGY_REQUIRED")
        if roles:
            add_group(
                (candidate,),
                roles=roles,
                matching=matching,
                contract_roles=((
                    candidate.contract_ticker,
                    (
                        "CALL_ANCHOR"
                        if candidate.option_side is OptionSide.CALL
                        else "PUT_ANCHOR",
                    ),
                ),),
                label="INDIVIDUAL_ACTIVITY",
            )

    def pair_groups(
        strategy_name,
        *,
        minimum_dte,
        maximum_dte,
        minimum_width_fraction,
        maximum_width_fraction,
        call_roles,
        put_roles,
    ):
        buckets: dict[
            tuple[str, date, OptionSide],
            list[ActivityMomentumCandidate],
        ] = {}
        for candidate in selected:
            dte = candidate.dte_for(session_date)
            if minimum_dte <= dte <= maximum_dte:
                buckets.setdefault(
                    (
                        candidate.underlying,
                        candidate.expiration,
                        candidate.option_side,
                    ),
                    [],
                ).append(candidate)
        for key, rows in sorted(buckets.items()):
            ordered = sorted(rows, key=lambda row: row.strike)
            for lower, upper in zip(ordered, ordered[1:]):
                spot = lower.spot or upper.spot
                if spot is None:
                    continue
                width = (upper.strike - lower.strike) / spot
                if not (
                    minimum_width_fraction
                    <= width
                    <= maximum_width_fraction
                ):
                    continue
                role_values = (
                    call_roles
                    if key[2] is OptionSide.CALL
                    else put_roles
                )
                add_group(
                    (lower, upper),
                    roles=(
                        "DIRECTIONAL_MOMENTUM",
                        "STRATEGY_REQUIRED",
                    ),
                    matching=(strategy_name,),
                    contract_roles=(
                        (
                            lower.contract_ticker,
                            (role_values[0],),
                        ),
                        (
                            upper.contract_ticker,
                            (role_values[1],),
                        ),
                    ),
                    label=f"{strategy_name}:{key}",
                )

    if (
        include_strategy_groups
        and "DIRECTIONAL_DEBIT_SPREAD" in strategies
    ):
        policy = dict(
            strategy_document.get("debit_spread") or {}
        )
        pair_groups(
            "DIRECTIONAL_DEBIT_SPREAD",
            minimum_dte=int(policy["minimum_dte"]),
            maximum_dte=int(policy["maximum_dte"]),
            minimum_width_fraction=Decimal(
                str(policy["minimum_width_fraction"])
            ),
            maximum_width_fraction=Decimal(
                str(policy["maximum_width_fraction"])
            ),
            call_roles=("LONG_ANCHOR", "SHORT_WING"),
            put_roles=("SHORT_WING", "LONG_ANCHOR"),
        )
    if (
        include_strategy_groups
        and "SPREAD_RANGE_LOCATOR" in strategies
    ):
        credit = dict(strategy_document.get("credit") or {})
        pair_groups(
            "SPREAD_RANGE_LOCATOR",
            minimum_dte=int(credit["minimum_entry_dte"]),
            maximum_dte=int(credit["maximum_entry_dte"]),
            minimum_width_fraction=Decimal("0"),
            maximum_width_fraction=Decimal(str(
                dict(strategy_document.get("spreads") or {}).get(
                    "maximum_center_distance_fraction",
                    "0.02",
                )
            )),
            call_roles=("SHORT_ANCHOR", "PROTECTIVE_WING"),
            put_roles=("PROTECTIVE_WING", "SHORT_ANCHOR"),
        )
    if include_strategy_groups and "INCOME_WHEEL" in strategies:
        wheel = dict(strategy_document.get("income_wheel") or {})
        for candidate in selected:
            dte = candidate.dte_for(session_date)
            if (
                candidate.option_side is OptionSide.PUT
                and int(wheel["minimum_dte"])
                <= dte
                <= int(wheel["maximum_dte"])
            ):
                add_group(
                    (candidate,),
                    roles=("STRATEGY_REQUIRED",),
                    matching=("INCOME_WHEEL",),
                    contract_roles=((
                        candidate.contract_ticker,
                        ("PUT_ANCHOR",),
                    ),),
                    label="INCOME_WHEEL",
                )
    if (
        include_strategy_groups
        and "VOLATILITY_SMILE_DISTORTION" in strategies
    ):
        minimum_strikes = int(
            dict(strategy_document.get("smile") or {})[
                "minimum_strikes"
            ]
        )
        buckets: dict[
            tuple[str, date, OptionSide],
            list[ActivityMomentumCandidate],
        ] = {}
        for candidate in selected:
            if 0 <= candidate.dte_for(session_date) <= 60:
                buckets.setdefault(
                    (
                        candidate.underlying,
                        candidate.expiration,
                        candidate.option_side,
                    ),
                    [],
                ).append(candidate)
        for key, rows in sorted(buckets.items()):
            if len(rows) < minimum_strikes:
                continue
            ranked = sorted(
                rows,
                key=lambda row: (
                    Decimal("Infinity")
                    if row.spot is None
                    else abs(row.strike - row.spot),
                    row.contract_ticker,
                ),
            )[:minimum_strikes]
            add_group(
                ranked,
                roles=("STRATEGY_REQUIRED",),
                matching=("VOLATILITY_SMILE_DISTORTION",),
                contract_roles=tuple(
                    (row.contract_ticker, ("SURFACE_NEIGHBOR",))
                    for row in ranked
                ),
                label=f"VOLATILITY_SMILE_DISTORTION:{key}",
            )
    return tuple(
        groups[group_id] for group_id in sorted(groups)
    )


def resolve_activity_momentum_selection(
    profile: ActivityMomentumProfile,
    *,
    session_date: date,
    candidates: Iterable[ActivityMomentumCandidate],
    enabled_strategies: Iterable[str] = (),
    required_contract_tickers: Iterable[str] = (),
    strategy_policy_path: Path = (
        DEFAULT_ACTIVITY_MOMENTUM_STRATEGY_POLICY_PATH
    ),
    preparation_at: datetime | None = None,
    expiry_calendars: Iterable[
        ActivityMomentumExpiryCalendarEvidence
    ] = (),
    same_day_enabled: bool | None = None,
    short_delta_override: Decimal | None = None,
) -> ActivityMomentumResolution:
    selected = tuple(sorted(
        candidates,
        key=lambda row: row.contract_ticker,
    ))
    strategies = tuple(sorted({
        _text(value, "enabled strategy")
        for value in enabled_strategies
    }))
    required = frozenset(
        _text(value, "required contract ticker", maximum=48)
        for value in required_contract_tickers
    )
    calendars = tuple(expiry_calendars)
    if (
        short_delta_override is not None
        and (
            not isinstance(short_delta_override, Decimal)
            or not short_delta_override.is_finite()
            or not Decimal("0")
            < short_delta_override
            <= Decimal("1")
        )
    ):
        raise ValueError("short delta override is invalid")
    if (
        short_delta_override is not None
        and profile.version
        != SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
    ):
        raise ValueError(
            "short delta override requires the v3 profile"
        )
    if (
        profile.version
        == LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION
    ):
        if (
            preparation_at is not None
            or calendars
            or same_day_enabled is not None
            or short_delta_override is not None
        ):
            raise ValueError(
                "legacy activity/momentum selection inputs changed"
            )
        groups = _resolve_activity_momentum_groups(
            profile,
            session_date=session_date,
            candidates=selected,
            enabled_strategies=strategies,
            required_contract_tickers=required,
            strategy_policy_path=strategy_policy_path,
        )
        shortlisted = selected
        filtered_count = len(selected)
        pooled_count = len(selected)
        rejection_counts: Counter[str] = Counter()
        expiry_evidence = ()
    else:
        (
            shortlisted,
            filtered_count,
            pooled_count,
            rejection_counts,
            protected_groups,
            expiry_evidence,
        ) = _shortlist_activity_momentum_candidates(
            profile,
            session_date=session_date,
            candidates=selected,
            enabled_strategies=strategies,
            required_contract_tickers=required,
            strategy_policy_path=strategy_policy_path,
            preparation_at=preparation_at,
            expiry_calendars=calendars,
            same_day_enabled=same_day_enabled,
            short_delta_override=short_delta_override,
        )
        coverage_groups = (
            ()
            if not shortlisted
            else _resolve_activity_momentum_groups(
                profile,
                session_date=session_date,
                candidates=shortlisted,
                enabled_strategies=(),
                required_contract_tickers=(),
                strategy_policy_path=strategy_policy_path,
                rejection_counts=rejection_counts,
                include_coverage_groups=True,
                include_strategy_groups=False,
                short_delta_override=short_delta_override,
            )
        )
        combined = list(protected_groups)
        combined_ids = {
            row.group_id for row in protected_groups
        }
        group_counts = Counter(
            row.underlying for row in protected_groups
        )
        for group in sorted(
            coverage_groups,
            key=lambda row: _group_rank(
                row,
                profile,
                session_date=session_date,
            ),
        ):
            if group.group_id in combined_ids:
                continue
            if (
                group_counts[group.underlying]
                >= profile.maximum_complete_groups_per_underlying
            ):
                rejection_counts["GROUP_ENUMERATION_BOUND"] += 1
                continue
            combined.append(group)
            combined_ids.add(group.group_id)
            group_counts[group.underlying] += 1
        groups = tuple(sorted(
            combined,
            key=lambda row: row.group_id,
        ))
    ranking_clocks = tuple(
        value
        for candidate in shortlisted
        for value in (
            candidate.observed_at,
            candidate.quote_as_of,
            candidate.volume_as_of,
            candidate.open_interest_as_of,
            candidate.greeks_as_of,
        )
        if value is not None
    )
    return ActivityMomentumResolution(
        discovered_count=len(selected),
        filtered_count=filtered_count,
        candidate_pool_count=pooled_count,
        shortlisted_count=len(shortlisted),
        group_enumeration_count=len(groups),
        shortlisted_candidates=shortlisted,
        groups=groups,
        rejection_counts=tuple(rejection_counts.items()),
        ranking_as_of=ranking_clocks,
        short_expiry_evidence=expiry_evidence,
        short_delta_override=short_delta_override,
    )


def resolve_activity_momentum_groups(
    profile: ActivityMomentumProfile,
    *,
    session_date: date,
    candidates: Iterable[ActivityMomentumCandidate],
    enabled_strategies: Iterable[str] = (),
    required_contract_tickers: Iterable[str] = (),
    strategy_policy_path: Path = (
        DEFAULT_ACTIVITY_MOMENTUM_STRATEGY_POLICY_PATH
    ),
    preparation_at: datetime | None = None,
    expiry_calendars: Iterable[
        ActivityMomentumExpiryCalendarEvidence
    ] = (),
    same_day_enabled: bool | None = None,
    short_delta_override: Decimal | None = None,
) -> tuple[ActivityMomentumGroup, ...]:
    return resolve_activity_momentum_selection(
        profile,
        session_date=session_date,
        candidates=candidates,
        enabled_strategies=enabled_strategies,
        required_contract_tickers=required_contract_tickers,
        strategy_policy_path=strategy_policy_path,
        preparation_at=preparation_at,
        expiry_calendars=expiry_calendars,
        same_day_enabled=same_day_enabled,
        short_delta_override=short_delta_override,
    ).groups


@dataclass(frozen=True, slots=True)
class ActivityMomentumCoverage:
    underlying: str
    role: str
    state: CoverageState
    planned_group_count: int
    admitted_group_count: int
    planned_contract_count: int
    admitted_contract_count: int
    ready_contract_count: int
    missing_oi_contract_count: int
    reason_codes: tuple[str, ...]
    rejected_group_samples: tuple[str, ...]

    def __post_init__(self) -> None:
        counts = (
            self.planned_group_count,
            self.admitted_group_count,
            self.planned_contract_count,
            self.admitted_contract_count,
            self.ready_contract_count,
            self.missing_oi_contract_count,
        )
        if (
            any(type(value) is not int or value < 0 for value in counts)
            or self.admitted_group_count > self.planned_group_count
            or self.admitted_contract_count > self.planned_contract_count
            or self.ready_contract_count > self.admitted_contract_count
            or self.missing_oi_contract_count
            > self.planned_contract_count
        ):
            raise ValueError("activity/momentum coverage is invalid")
        object.__setattr__(
            self,
            "underlying",
            _text(self.underlying, "coverage underlying", maximum=16),
        )
        object.__setattr__(
            self,
            "role",
            _text(self.role, "coverage role"),
        )
        object.__setattr__(
            self,
            "reason_codes",
            tuple(sorted({
                _text(value, "coverage reason")
                for value in self.reason_codes
            })),
        )
        object.__setattr__(
            self,
            "rejected_group_samples",
            tuple(sorted({
                _text(
                    value,
                    "rejected group sample",
                    maximum=256,
                    upper=False,
                )
                for value in self.rejected_group_samples
            })),
        )


def activity_momentum_coverage_payload(
    coverage: ActivityMomentumCoverage,
) -> dict[str, object]:
    return {
        "underlying": coverage.underlying,
        "role": coverage.role,
        "state": coverage.state.value,
        "planned_group_count": coverage.planned_group_count,
        "admitted_group_count": coverage.admitted_group_count,
        "planned_contract_count": coverage.planned_contract_count,
        "admitted_contract_count": coverage.admitted_contract_count,
        "ready_contract_count": coverage.ready_contract_count,
        "missing_oi_contract_count": (
            coverage.missing_oi_contract_count
        ),
        "reason_codes": list(coverage.reason_codes),
        "rejected_group_samples": list(
            coverage.rejected_group_samples
        ),
    }


def activity_momentum_coverage_from_payload(
    value: object,
) -> ActivityMomentumCoverage:
    if not isinstance(value, Mapping):
        raise TypeError("activity/momentum coverage must be an object")
    coverage = ActivityMomentumCoverage(
        underlying=str(value["underlying"]),
        role=str(value["role"]),
        state=CoverageState(str(value["state"])),
        planned_group_count=int(value["planned_group_count"]),
        admitted_group_count=int(value["admitted_group_count"]),
        planned_contract_count=int(value["planned_contract_count"]),
        admitted_contract_count=int(value["admitted_contract_count"]),
        ready_contract_count=int(value["ready_contract_count"]),
        missing_oi_contract_count=int(
            value["missing_oi_contract_count"]
        ),
        reason_codes=tuple(
            str(item) for item in value["reason_codes"]
        ),
        rejected_group_samples=tuple(
            str(item) for item in value["rejected_group_samples"]
        ),
    )
    if activity_momentum_coverage_payload(coverage) != dict(value):
        raise ValueError("activity/momentum coverage payload changed")
    return coverage


@dataclass(frozen=True, slots=True)
class ActivityMomentumAdmissionEvidence:
    configured_total_budget: int
    effective_total_budget: int
    configured_contracts_per_underlying: int
    effective_per_underlying_limit: int
    expansion_enabled: bool
    established_measured_capacity: int | None
    effective_provider_project_ceiling: int
    discovered_count: int
    filtered_count: int
    candidate_pool_count: int
    shortlisted_count: int
    group_enumeration_count: int
    admitted_contract_count: int
    rejection_counts: tuple[tuple[str, int], ...]
    ranking_as_of: tuple[datetime, ...]

    def __post_init__(self) -> None:
        counts = (
            self.configured_total_budget,
            self.effective_total_budget,
            self.configured_contracts_per_underlying,
            self.effective_per_underlying_limit,
            self.effective_provider_project_ceiling,
            self.discovered_count,
            self.filtered_count,
            self.candidate_pool_count,
            self.shortlisted_count,
            self.group_enumeration_count,
            self.admitted_contract_count,
        )
        if (
            any(type(value) is not int or value < 0 for value in counts)
            or self.configured_total_budget <= 0
            or self.configured_contracts_per_underlying <= 0
            or self.configured_contracts_per_underlying
            > self.effective_per_underlying_limit
            or self.effective_per_underlying_limit <= 0
            or self.effective_provider_project_ceiling <= 0
            or self.effective_total_budget
            > self.configured_total_budget
            or self.effective_total_budget
            > self.effective_provider_project_ceiling
            or self.admitted_contract_count
            > self.effective_total_budget
            or self.filtered_count > self.discovered_count
            or self.candidate_pool_count > self.filtered_count
            or self.shortlisted_count > self.candidate_pool_count
            or (
                self.established_measured_capacity is not None
                and (
                    type(self.established_measured_capacity) is not int
                    or self.established_measured_capacity <= 0
                    or self.effective_total_budget
                    > self.established_measured_capacity
                )
            )
            or type(self.expansion_enabled) is not bool
            or any(
                type(count) is not int or count <= 0
                for _, count in self.rejection_counts
            )
        ):
            raise ValueError(
                "activity/momentum admission evidence is invalid"
            )
        object.__setattr__(
            self,
            "rejection_counts",
            tuple(sorted(
                (
                    _text(reason, "admission rejection reason"),
                    count,
                )
                for reason, count in self.rejection_counts
            )),
        )
        object.__setattr__(
            self,
            "ranking_as_of",
            tuple(sorted({
                _utc(value, "admission ranking_as_of")
                for value in self.ranking_as_of
            })),
        )


def activity_momentum_admission_evidence_payload(
    evidence: ActivityMomentumAdmissionEvidence,
) -> dict[str, object]:
    return {
        "configured_total_budget": evidence.configured_total_budget,
        "effective_total_budget": evidence.effective_total_budget,
        "configured_contracts_per_underlying": (
            evidence.configured_contracts_per_underlying
        ),
        "effective_per_underlying_limit": (
            evidence.effective_per_underlying_limit
        ),
        "expansion_enabled": evidence.expansion_enabled,
        "established_measured_capacity": (
            evidence.established_measured_capacity
        ),
        "effective_provider_project_ceiling": (
            evidence.effective_provider_project_ceiling
        ),
        "counts": {
            "discovered": evidence.discovered_count,
            "filtered": evidence.filtered_count,
            "candidate_pool": evidence.candidate_pool_count,
            "shortlisted": evidence.shortlisted_count,
            "groups_enumerated": evidence.group_enumeration_count,
            "admitted_contracts": evidence.admitted_contract_count,
        },
        "rejection_counts": [
            {"reason_code": reason, "count": count}
            for reason, count in evidence.rejection_counts
        ],
        "ranking_as_of": [
            value.isoformat() for value in evidence.ranking_as_of
        ],
    }


def activity_momentum_admission_evidence_from_payload(
    value: object,
) -> ActivityMomentumAdmissionEvidence:
    if not isinstance(value, Mapping):
        raise TypeError(
            "activity/momentum admission evidence must be an object"
        )
    counts = value.get("counts")
    if not isinstance(counts, Mapping):
        raise TypeError(
            "activity/momentum admission counts must be an object"
        )
    evidence = ActivityMomentumAdmissionEvidence(
        configured_total_budget=int(
            value["configured_total_budget"]
        ),
        effective_total_budget=int(value["effective_total_budget"]),
        configured_contracts_per_underlying=int(
            value["configured_contracts_per_underlying"]
        ),
        effective_per_underlying_limit=int(
            value["effective_per_underlying_limit"]
        ),
        expansion_enabled=bool(value["expansion_enabled"]),
        established_measured_capacity=(
            None
            if value.get("established_measured_capacity") is None
            else int(value["established_measured_capacity"])
        ),
        effective_provider_project_ceiling=int(
            value["effective_provider_project_ceiling"]
        ),
        discovered_count=int(counts["discovered"]),
        filtered_count=int(counts["filtered"]),
        candidate_pool_count=int(counts["candidate_pool"]),
        shortlisted_count=int(counts["shortlisted"]),
        group_enumeration_count=int(counts["groups_enumerated"]),
        admitted_contract_count=int(counts["admitted_contracts"]),
        rejection_counts=tuple(
            (
                str(row["reason_code"]),
                int(row["count"]),
            )
            for row in value["rejection_counts"]
        ),
        ranking_as_of=tuple(
            datetime.fromisoformat(str(item))
            for item in value["ranking_as_of"]
        ),
    )
    if activity_momentum_admission_evidence_payload(
        evidence
    ) != dict(value):
        raise ValueError(
            "activity/momentum admission evidence payload changed"
        )
    return evidence


@dataclass(frozen=True, slots=True)
class ActivityMomentumConsumerEligibility:
    consumer_name: str
    enabled: bool
    roles: tuple[str, ...]
    required_inputs: tuple[str, ...]
    event_kinds: tuple[str, ...]
    cadence_ms: int
    complete_groups_only: bool
    group_ids: tuple[str, ...]
    contract_tickers: tuple[str, ...]

    def __post_init__(self) -> None:
        group_ids = tuple(sorted({
            _text(
                value,
                "consumer group id",
                maximum=256,
                upper=False,
            )
            for value in self.group_ids
        }))
        tickers = tuple(sorted({
            _text(
                value,
                "consumer contract ticker",
                maximum=48,
            )
            for value in self.contract_tickers
        }))
        requirement = ActivityMomentumConsumerRequirement(
            consumer_name=self.consumer_name,
            roles=self.roles,
            required_inputs=self.required_inputs,
            event_kinds=self.event_kinds,
            cadence_ms=self.cadence_ms,
            complete_groups_only=self.complete_groups_only,
            enabled=self.enabled,
        )
        if not self.enabled and (group_ids or tickers):
            raise ValueError(
                "disabled activity consumer has live workload"
            )
        object.__setattr__(
            self,
            "consumer_name",
            requirement.consumer_name,
        )
        object.__setattr__(self, "roles", requirement.roles)
        object.__setattr__(
            self,
            "required_inputs",
            requirement.required_inputs,
        )
        object.__setattr__(
            self,
            "event_kinds",
            requirement.event_kinds,
        )
        object.__setattr__(self, "group_ids", group_ids)
        object.__setattr__(
            self,
            "contract_tickers",
            tickers,
        )


def activity_momentum_consumer_eligibility_payload(
    eligibility: ActivityMomentumConsumerEligibility,
) -> dict[str, object]:
    return {
        "consumer_name": eligibility.consumer_name,
        "enabled": eligibility.enabled,
        "roles": list(eligibility.roles),
        "required_inputs": list(eligibility.required_inputs),
        "event_kinds": list(eligibility.event_kinds),
        "cadence_ms": eligibility.cadence_ms,
        "complete_groups_only": (
            eligibility.complete_groups_only
        ),
        "group_ids": list(eligibility.group_ids),
        "contract_tickers": list(
            eligibility.contract_tickers
        ),
    }


def activity_momentum_consumer_eligibility_from_payload(
    value: object,
) -> ActivityMomentumConsumerEligibility:
    if not isinstance(value, Mapping):
        raise TypeError(
            "activity/momentum consumer eligibility must be an object"
        )
    eligibility = ActivityMomentumConsumerEligibility(
        consumer_name=str(value["consumer_name"]),
        enabled=bool(value["enabled"]),
        roles=tuple(str(item) for item in value["roles"]),
        required_inputs=tuple(
            str(item) for item in value["required_inputs"]
        ),
        event_kinds=tuple(
            str(item) for item in value["event_kinds"]
        ),
        cadence_ms=int(value["cadence_ms"]),
        complete_groups_only=bool(
            value["complete_groups_only"]
        ),
        group_ids=tuple(str(item) for item in value["group_ids"]),
        contract_tickers=tuple(
            str(item) for item in value["contract_tickers"]
        ),
    )
    if activity_momentum_consumer_eligibility_payload(
        eligibility
    ) != dict(value):
        raise ValueError(
            "activity/momentum consumer eligibility payload changed"
        )
    return eligibility


@dataclass(frozen=True, slots=True)
class ActivityMomentumAllocation:
    allocation_sha256: str
    profile_version: str
    profile_sha256: str
    session_date: date
    rotation_key: str
    active_contract_budget: int
    quote_contract_ceiling: int
    quote_slot_reserve: int
    initial_share: int
    ready_underlyings: tuple[str, ...]
    eligible_underlyings: tuple[str, ...]
    selected_groups: tuple[ActivityMomentumGroup, ...]
    selected_contract_tickers: tuple[str, ...]
    rejected_groups: tuple[tuple[str, tuple[str, ...]], ...]
    coverage: tuple[ActivityMomentumCoverage, ...]
    admission_evidence: (
        ActivityMomentumAdmissionEvidence | None
    ) = None
    consumer_eligibility: tuple[
        ActivityMomentumConsumerEligibility,
        ...,
    ] = ()
    short_expiry_evidence: tuple[
        ActivityMomentumShortExpiryEvidence,
        ...,
    ] = ()
    short_delta_override: Decimal | None = None

    def __post_init__(self) -> None:
        selected = tuple(sorted(
            self.selected_groups,
            key=lambda row: row.group_id,
        ))
        tickers = tuple(sorted({
            ticker
            for group in selected
            for ticker in group.contract_tickers
        }))
        if (
            self.profile_version
            not in {
                LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION,
                ACTIVITY_MOMENTUM_PROFILE_VERSION,
                SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION,
            }
            or self.active_contract_budget < 0
            or self.quote_contract_ceiling <= 0
            or self.quote_slot_reserve < 0
            or self.active_contract_budget
            + self.quote_slot_reserve
            > self.quote_contract_ceiling
            or len(tickers) > self.active_contract_budget
            or tickers != self.selected_contract_tickers
        ):
            raise ValueError("activity/momentum allocation is invalid")
        if (
            self.profile_version
            == LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION
            and (
                self.active_contract_budget <= 0
                or self.admission_evidence is not None
                or self.consumer_eligibility
            )
        ):
            raise ValueError(
                "legacy activity/momentum allocation changed"
            )
        if (
            self.profile_version
            in _BOUNDED_ACTIVITY_MOMENTUM_PROFILE_VERSIONS
            and (
                self.admission_evidence is None
                or self.admission_evidence.effective_total_budget
                != self.active_contract_budget
                or self.admission_evidence.admitted_contract_count
                != len(tickers)
                or any(
                    len({
                        ticker
                        for group in selected
                        if group.underlying == underlying
                        for ticker in group.contract_tickers
                    })
                    > self.admission_evidence.effective_per_underlying_limit
                    for underlying in self.eligible_underlyings
                )
            )
        ):
            raise ValueError(
                "activity/momentum addendum evidence changed"
            )
        expiry_evidence = tuple(sorted(
            self.short_expiry_evidence,
            key=lambda row: row.underlying,
        ))
        if (
            self.profile_version
            == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
        ):
            if (
                len({
                    row.underlying for row in expiry_evidence
                })
                != len(expiry_evidence)
                or set(
                    row.underlying for row in expiry_evidence
                )
                - set(self.ready_underlyings)
                or (
                    self.short_delta_override is not None
                    and (
                        not isinstance(
                            self.short_delta_override,
                            Decimal,
                        )
                        or not self.short_delta_override.is_finite()
                        or not Decimal("0")
                        < self.short_delta_override
                        <= Decimal("1")
                    )
                )
            ):
                raise ValueError(
                    "activity/momentum v3 expiry evidence changed"
                )
        elif expiry_evidence or self.short_delta_override is not None:
            raise ValueError(
                "v1/v2 activity/momentum expiry evidence changed"
            )
        consumers = tuple(sorted(
            self.consumer_eligibility,
            key=lambda row: row.consumer_name,
        ))
        if (
            len({
                row.consumer_name for row in consumers
            }) != len(consumers)
            or any(
                set(row.contract_tickers) - set(tickers)
                for row in consumers
            )
        ):
            raise ValueError(
                "activity/momentum consumer eligibility is invalid"
            )
        object.__setattr__(
            self,
            "consumer_eligibility",
            consumers,
        )
        object.__setattr__(
            self,
            "short_expiry_evidence",
            expiry_evidence,
        )
        _digest(self.profile_sha256, "profile_sha256")
        expected = canonical_sha256(
            activity_momentum_allocation_payload(
                self,
                include_identity=False,
            )
        )
        if self.allocation_sha256 != expected:
            raise ValueError(
                "activity/momentum allocation identity changed"
            )
        object.__setattr__(self, "selected_groups", selected)


def activity_momentum_allocation_payload(
    allocation: ActivityMomentumAllocation,
    *,
    include_identity: bool = True,
) -> dict[str, object]:
    value: dict[str, object] = {
        "schema": allocation.profile_version,
        "profile_version": allocation.profile_version,
        "profile_sha256": allocation.profile_sha256,
        "session_date": allocation.session_date.isoformat(),
        "rotation_key": allocation.rotation_key,
        "active_contract_budget": allocation.active_contract_budget,
        "quote_contract_ceiling": allocation.quote_contract_ceiling,
        "quote_slot_reserve": allocation.quote_slot_reserve,
        "initial_share": allocation.initial_share,
        "ready_underlyings": list(allocation.ready_underlyings),
        "eligible_underlyings": list(
            allocation.eligible_underlyings
        ),
        "selected_group_ids": [
            row.group_id for row in allocation.selected_groups
        ],
        "selected_groups": [
            activity_momentum_group_payload(row)
            for row in allocation.selected_groups
        ],
        "selected_contract_tickers": list(
            allocation.selected_contract_tickers
        ),
        "selected_contract_count": len(
            allocation.selected_contract_tickers
        ),
        "unused_active_slots": (
            allocation.active_contract_budget
            - len(allocation.selected_contract_tickers)
        ),
        "rejected_groups": [
            {
                "group_id": group_id,
                "reason_codes": list(reasons),
            }
            for group_id, reasons in allocation.rejected_groups
        ],
        "coverage": [
            activity_momentum_coverage_payload(row)
            for row in allocation.coverage
        ],
        "future_workload": {
            "silent_accumulation_quote_slots": 0,
            "silent_accumulation_history_jobs": 0,
            "silent_accumulation_detector_work": 0,
            **(
                {"future_50ms_detector_evaluations": 0}
                if allocation.profile_version
                in _BOUNDED_ACTIVITY_MOMENTUM_PROFILE_VERSIONS
                else {}
            ),
        },
    }
    if (
        allocation.profile_version
        in _BOUNDED_ACTIVITY_MOMENTUM_PROFILE_VERSIONS
    ):
        if allocation.admission_evidence is None:
            raise ValueError(
                "activity/momentum addendum evidence is absent"
            )
        value["admission_evidence"] = (
            activity_momentum_admission_evidence_payload(
                allocation.admission_evidence
            )
        )
        value["consumer_eligibility"] = [
            activity_momentum_consumer_eligibility_payload(row)
            for row in allocation.consumer_eligibility
        ]
    if (
        allocation.profile_version
        == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
    ):
        value["short_expiry_evidence"] = [
            activity_momentum_short_expiry_evidence_payload(row)
            for row in allocation.short_expiry_evidence
        ]
        value["short_delta_override"] = (
            None
            if allocation.short_delta_override is None
            else str(allocation.short_delta_override)
        )
    if include_identity:
        value["allocation_sha256"] = allocation.allocation_sha256
    return value


def activity_momentum_allocation_from_payload(
    value: object,
) -> ActivityMomentumAllocation:
    if (
        not isinstance(value, Mapping)
        or value.get("schema")
        not in {
            LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION,
            ACTIVITY_MOMENTUM_PROFILE_VERSION,
            SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION,
        }
        or value.get("profile_version") != value.get("schema")
    ):
        raise ValueError("activity/momentum allocation contract changed")
    allocation = ActivityMomentumAllocation(
        allocation_sha256=str(value["allocation_sha256"]),
        profile_version=str(value["profile_version"]),
        profile_sha256=str(value["profile_sha256"]),
        session_date=date.fromisoformat(str(value["session_date"])),
        rotation_key=str(value["rotation_key"]),
        active_contract_budget=int(value["active_contract_budget"]),
        quote_contract_ceiling=int(value["quote_contract_ceiling"]),
        quote_slot_reserve=int(value["quote_slot_reserve"]),
        initial_share=int(value["initial_share"]),
        ready_underlyings=tuple(
            str(item) for item in value["ready_underlyings"]
        ),
        eligible_underlyings=tuple(
            str(item) for item in value["eligible_underlyings"]
        ),
        selected_groups=tuple(
            activity_momentum_group_from_payload(row)
            for row in value["selected_groups"]
        ),
        selected_contract_tickers=tuple(
            str(item)
            for item in value["selected_contract_tickers"]
        ),
        rejected_groups=tuple(
            (
                str(row["group_id"]),
                tuple(str(item) for item in row["reason_codes"]),
            )
            for row in value["rejected_groups"]
        ),
        coverage=tuple(
            activity_momentum_coverage_from_payload(row)
            for row in value["coverage"]
        ),
        admission_evidence=(
            None
            if value.get("admission_evidence") is None
            else activity_momentum_admission_evidence_from_payload(
                value["admission_evidence"]
            )
        ),
        consumer_eligibility=tuple(
            activity_momentum_consumer_eligibility_from_payload(
                row
            )
            for row in value.get("consumer_eligibility", ())
        ),
        short_expiry_evidence=tuple(
            activity_momentum_short_expiry_evidence_from_payload(
                row
            )
            for row in value.get("short_expiry_evidence", ())
        ),
        short_delta_override=(
            None
            if value.get("short_delta_override") is None
            else Decimal(str(value["short_delta_override"]))
        ),
    )
    if activity_momentum_allocation_payload(allocation) != dict(value):
        raise ValueError("activity/momentum allocation payload changed")
    return allocation


def c6_members_from_activity_allocation(
    allocation: ActivityMomentumAllocation,
    *,
    catalog_members: Mapping[str, C6SelectionMember] | None = None,
) -> tuple[C6SelectionMember, ...]:
    candidates: dict[str, ActivityMomentumCandidate] = {}
    strategies: dict[str, set[str]] = {}
    roles: dict[str, set[str]] = {}
    reasons: dict[str, set[str]] = {}
    evidence: dict[str, set[str]] = {}
    for group in allocation.selected_groups:
        for candidate in group.candidates:
            candidates.setdefault(
                candidate.contract_ticker,
                candidate,
            )
            strategies.setdefault(
                candidate.contract_ticker,
                set(),
            ).update(group.matching_strategies)
            roles.setdefault(
                candidate.contract_ticker,
                set(),
            ).update(
                dict(group.contract_roles).get(
                    candidate.contract_ticker,
                    group.roles,
                )
            )
            reasons.setdefault(
                candidate.contract_ticker,
                set(),
            ).update({
                "ACTIVITY_MOMENTUM_FAIR_SHARE",
                "BASKET_DTE_"
                f"{min(row.dte_for(allocation.session_date) for row in group.candidates)}",
            })
            evidence.setdefault(
                candidate.contract_ticker,
                set(),
            ).update(candidate.source_evidence_references)
            evidence[candidate.contract_ticker].update(
                group.requirement_identities
            )
    catalog = {} if catalog_members is None else dict(catalog_members)
    return tuple(
        C6SelectionMember(
            contract_id=(
                candidate.contract_id
                if ticker not in catalog
                else catalog[ticker].contract_id
            ),
            contract_ticker=candidate.contract_ticker,
            underlying=candidate.underlying,
            expiration=candidate.expiration,
            strike=candidate.strike,
            option_side=candidate.option_side,
            multiplier=(
                candidate.multiplier
                if ticker not in catalog
                else catalog[ticker].multiplier
            ),
            deliverable=(
                candidate.deliverable
                if ticker not in catalog
                else catalog[ticker].deliverable
            ),
            catalog_version_id=(
                candidate.catalog_version_id
                if ticker not in catalog
                else catalog[ticker].catalog_version_id
            ),
            catalog_payload_sha256=(
                candidate.catalog_payload_sha256
                if ticker not in catalog
                else catalog[ticker].catalog_payload_sha256
            ),
            matching_strategies=tuple(sorted(
                strategies[ticker]
            )),
            contract_roles=tuple(sorted(roles[ticker])),
            selection_reasons=tuple(sorted(reasons[ticker])),
            evidence_references=tuple(sorted(evidence[ticker])),
            flow_eligible=candidate.flow_eligible,
        )
        for ticker, candidate in sorted(candidates.items())
    )


def c6_coverage_from_activity_allocation(
    allocation: ActivityMomentumAllocation,
) -> tuple[C6AllocationCoverage, ...]:
    return tuple(
        C6AllocationCoverage(
            underlying=row.underlying,
            role=row.role,
            state=row.state.value,
            planned_group_count=row.planned_group_count,
            admitted_group_count=row.admitted_group_count,
            planned_contract_count=row.planned_contract_count,
            admitted_contract_count=row.admitted_contract_count,
            ready_contract_count=row.ready_contract_count,
            missing_oi_contract_count=(
                row.missing_oi_contract_count
            ),
            reason_codes=row.reason_codes,
            rejected_group_samples=row.rejected_group_samples,
        )
        for row in allocation.coverage
    )


def _candidate_rank(
    candidate: ActivityMomentumCandidate,
    group: ActivityMomentumGroup,
    *,
    session_date: date,
) -> tuple[object, ...]:
    spread = candidate.relative_spread
    notional = candidate.completed_session_notional
    if (
        group.target_delta is not None
        and candidate.delta is not None
    ):
        target_distance = abs(
            abs(candidate.delta) - abs(group.target_delta)
        )
    elif (
        group.target_moneyness is not None
        and candidate.spot is not None
    ):
        target_distance = abs(
            abs(candidate.strike - candidate.spot)
            / candidate.spot
            - group.target_moneyness
        )
    else:
        target_distance = Decimal("Infinity")
    expiry_distance = (
        abs(candidate.dte_for(session_date) - group.target_dte)
        if group.target_dte is not None
        else 0
    )
    return (
        bool(candidate.unavailable_reasons),
        candidate.bid is None,
        Decimal("Infinity") if spread is None else spread,
        candidate.prior_open_interest is None,
        -(
            Decimal("-1")
            if notional is None
            else notional
        ),
        -(
            -1
            if candidate.completed_session_volume is None
            else candidate.completed_session_volume
        ),
        target_distance,
        expiry_distance,
        candidate.contract_ticker,
    )


def _group_rank(
    group: ActivityMomentumGroup,
    profile: ActivityMomentumProfile,
    *,
    session_date: date,
) -> tuple[object, ...]:
    _minimum_dte, maximum_dte = group.dte_bounds(session_date)
    basket = profile.basket_for(
        dte=maximum_dte,
        roles=group.roles,
    )
    basket_priority = (
        999 if basket is None else basket.priority
    )
    role_priority = min(
        profile.role_rank[role] for role in group.roles
    )
    candidate_ranks = tuple(
        _candidate_rank(
            row,
            group,
            session_date=session_date,
        )
        for row in group.candidates
    )
    side_priority = (
        min(
            (
                0
                if row.option_side is OptionSide.CALL
                else 1
            )
            for row in group.candidates
        )
        if basket is not None and basket.call_priority
        else 0
    )
    return (
        basket_priority,
        role_priority,
        side_priority,
        candidate_ranks,
        group.contract_tickers,
        group.group_id,
    )


def _consumer_eligibility(
    profile: ActivityMomentumProfile,
    selected_groups: Iterable[ActivityMomentumGroup],
) -> tuple[ActivityMomentumConsumerEligibility, ...]:
    groups = tuple(selected_groups)
    values = []
    for requirement in profile.consumer_requirements:
        matching = tuple(
            group
            for group in groups
            if set(group.roles) & set(requirement.roles)
        )
        enabled = requirement.enabled
        values.append(ActivityMomentumConsumerEligibility(
            consumer_name=requirement.consumer_name,
            enabled=enabled,
            roles=requirement.roles,
            required_inputs=requirement.required_inputs,
            event_kinds=requirement.event_kinds,
            cadence_ms=requirement.cadence_ms,
            complete_groups_only=(
                requirement.complete_groups_only
            ),
            group_ids=(
                tuple(group.group_id for group in matching)
                if enabled
                else ()
            ),
            contract_tickers=(
                tuple(sorted({
                    ticker
                    for group in matching
                    for ticker in group.contract_tickers
                }))
                if enabled
                else ()
            ),
        ))
    return tuple(sorted(
        values,
        key=lambda row: row.consumer_name,
    ))


def allocate_activity_momentum(
    profile: ActivityMomentumProfile,
    *,
    session_date: date,
    rotation_key: str,
    ready_underlyings: Iterable[str],
    groups: Iterable[ActivityMomentumGroup],
    active_contract_budget: int | None = None,
    per_underlying_contract_limit: int | None = None,
    measured_capacity_limit: int | None = None,
    effective_provider_project_ceiling: int | None = None,
    resolution: ActivityMomentumResolution | None = None,
) -> ActivityMomentumAllocation:
    if not isinstance(profile, ActivityMomentumProfile):
        raise TypeError("activity/momentum profile is invalid")
    configured_budget = (
        profile.active_quote_contract_budget
        if active_contract_budget is None
        else active_contract_budget
    )
    if (
        type(configured_budget) is not int
        or configured_budget <= 0
        or configured_budget > profile.active_quote_contract_budget
    ):
        raise ValueError("activity/momentum contract budget is invalid")
    if (
        profile.version
        == LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION
    ):
        if (
            per_underlying_contract_limit is not None
            or measured_capacity_limit is not None
            or effective_provider_project_ceiling is not None
            or resolution is not None
        ):
            raise ValueError(
                "legacy activity/momentum allocation inputs changed"
            )
        configured_per_underlying = profile.quote_contract_ceiling
        per_underlying_limit = profile.quote_contract_ceiling
        measured_limit = None
        provider_ceiling = profile.active_quote_contract_budget
        expansion_enabled = False
    else:
        configured_per_underlying = (
            profile.default_contracts_per_underlying
            if per_underlying_contract_limit is None
            else per_underlying_contract_limit
        )
        per_underlying_limit = (
            profile.maximum_contracts_per_underlying
        )
        measured_limit = (
            profile.established_measured_capacity
            if measured_capacity_limit is None
            else measured_capacity_limit
        )
        provider_ceiling = (
            profile.effective_provider_project_ceiling
            if effective_provider_project_ceiling is None
            else effective_provider_project_ceiling
        )
        if (
            type(configured_per_underlying) is not int
            or not 1
            <= configured_per_underlying
            <= profile.maximum_contracts_per_underlying
            or (
                measured_limit is not None
                and (
                    type(measured_limit) is not int
                    or measured_limit <= 0
                )
            )
            or type(provider_ceiling) is not int
            or not 0
            < provider_ceiling
            <= profile.effective_provider_project_ceiling
        ):
            raise ValueError(
                "activity/momentum effective capacity is invalid"
            )
        expansion_enabled = (
            configured_per_underlying
            > profile.default_contracts_per_underlying
        )
    ready = tuple(sorted({
        _text(value, "ready underlying", maximum=16)
        for value in ready_underlyings
    }))
    if (
        profile.version
        == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
        and resolution is None
    ):
        raise ValueError(
            "v3 allocation requires resolved expiry evidence"
        )
    grouped: dict[str, list[ActivityMomentumGroup]] = {
        underlying: [] for underlying in ready
    }
    rejected: dict[str, tuple[str, ...]] = {}
    planned = []
    for group in groups:
        if not isinstance(group, ActivityMomentumGroup):
            raise TypeError("activity/momentum group must be typed")
        planned.append(group)
        reasons = []
        if group.underlying not in grouped:
            reasons.append("UNDERLYING_NOT_READY")
        if any(
            candidate.unavailable_reasons
            for candidate in group.candidates
        ):
            reasons.append("GROUP_CAPABILITY_UNAVAILABLE")
        minimum_dte, maximum_dte = group.dte_bounds(session_date)
        basket = profile.basket_for(
            dte=maximum_dte,
            roles=group.roles,
        )
        if basket is None or minimum_dte < basket.minimum_dte:
            reasons.append("LIVE_DTE_OR_ROLE_UNAVAILABLE")
        if (
            maximum_dte >= 120
            or "SILENT_ACCUMULATION" in group.roles
        ):
            reasons.append("SILENT_ACCUMULATION_DISABLED")
        if reasons:
            rejected[group.group_id] = tuple(sorted(set(reasons)))
            continue
        grouped[group.underlying].append(group)
    for underlying in grouped:
        grouped[underlying].sort(
            key=lambda row: _group_rank(
                row,
                profile,
                session_date=session_date,
            )
        )
    eligible = tuple(sorted(
        underlying
        for underlying, values in grouped.items()
        if values
    ))
    if (
        profile.version
        == LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION
    ):
        budget = configured_budget
    else:
        limits = [
            configured_per_underlying * len(eligible),
            configured_budget,
            provider_ceiling,
        ]
        if measured_limit is not None:
            limits.append(measured_limit)
        budget = min(limits)
    initial_share = (
        0
        if not eligible
        else min(
            configured_per_underlying,
            budget // len(eligible),
        )
    )
    rotation_seed = canonical_sha256({
        "profile_sha256": profile.profile_sha256,
        "session_date": session_date,
        "rotation_key": _text(
            rotation_key,
            "rotation key",
            maximum=256,
            upper=False,
        ),
        "underlyings": eligible,
    })
    if eligible:
        offset = int(rotation_seed[:16], 16) % len(eligible)
        rotated = eligible[offset:] + eligible[:offset]
    else:
        rotated = ()
    selected_groups = []
    selected_tickers: set[str] = set()
    remaining = {
        underlying: list(grouped[underlying])
        for underlying in eligible
    }
    used_by_underlying = {underlying: 0 for underlying in eligible}
    expiry_evidence_by_underlying = {
        row.underlying: row
        for row in (
            ()
            if resolution is None
            else resolution.short_expiry_evidence
        )
    }

    def incremental(group: ActivityMomentumGroup) -> int:
        return len(set(group.contract_tickers) - selected_tickers)

    def take(
        underlying: str,
        group: ActivityMomentumGroup,
    ) -> int:
        incremental = len(
            set(group.contract_tickers) - selected_tickers
        )
        remaining[underlying].remove(group)
        selected_groups.append(group)
        selected_tickers.update(group.contract_tickers)
        return incremental

    def within_v3_basket_limit(
        underlying: str,
        group: ActivityMomentumGroup,
    ) -> bool:
        if (
            profile.version
            != SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
        ):
            return True
        basket = profile.basket_for(
            dte=group.dte_bounds(session_date)[1],
            roles=group.roles,
        )
        if basket is None or basket not in profile.baskets[:2]:
            return True
        target = (
            profile.short_basket_contract_target
            if basket == profile.baskets[0]
            else profile.core_basket_contract_target
        )
        selected_for_basket = {
            ticker
            for selected_group in selected_groups
            if selected_group.underlying == underlying
            and profile.basket_for(
                dte=selected_group.dte_bounds(session_date)[1],
                roles=selected_group.roles,
            )
            == basket
            for ticker in selected_group.contract_tickers
        }
        return len(
            selected_for_basket | set(group.contract_tickers)
        ) <= target

    for underlying in rotated:
        queue = remaining[underlying]
        if (
            profile.version
            == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
        ):
            explicitly_required = tuple(
                group
                for group in queue
                if any(
                    "REQUIRED_LEG" in roles
                    for _, roles in group.contract_roles
                )
            )
            for group in explicitly_required:
                slot_cost = incremental(group)
                if (
                    used_by_underlying[underlying] + slot_cost
                    <= initial_share
                ):
                    used_by_underlying[underlying] += take(
                        underlying,
                        group,
                    )
            expiry_evidence = expiry_evidence_by_underlying.get(
                underlying
            )
            expiry_targets = []
            if (
                expiry_evidence is not None
                and expiry_evidence.near_expiry is not None
            ):
                if expiry_evidence.near_end_of_week_coincide:
                    expiry_targets.append((
                        expiry_evidence.near_expiry,
                        profile.short_basket_contract_target,
                    ))
                elif (
                    expiry_evidence.end_of_week_expiry is None
                ):
                    expiry_targets.append((
                        expiry_evidence.near_expiry,
                        profile.short_basket_contract_target,
                    ))
                else:
                    expiry_targets.extend((
                        (
                            expiry_evidence.near_expiry,
                            profile.short_near_expiry_contract_target,
                        ),
                        (
                            expiry_evidence.end_of_week_expiry,
                            profile.short_end_of_week_contract_target,
                        ),
                    ))
            for expiration, target in expiry_targets:
                while True:
                    selected_at_expiry = {
                        candidate.contract_ticker
                        for selected_group in selected_groups
                        if selected_group.underlying == underlying
                        for candidate in selected_group.candidates
                        if candidate.expiration == expiration
                        and 0
                        <= candidate.dte_for(session_date)
                        <= 5
                    }
                    if len(selected_at_expiry) >= target:
                        break
                    matching = tuple(
                        group
                        for group in queue
                        if len(group.candidates) == 1
                        and group.candidates[0].expiration
                        == expiration
                        and 0
                        <= group.candidates[0].dte_for(
                            session_date
                        )
                        <= 5
                        and used_by_underlying[underlying]
                        + incremental(group)
                        <= initial_share
                    )
                    missing_side = next(
                        (
                            side
                            for side in (
                                OptionSide.CALL,
                                OptionSide.PUT,
                            )
                            if any(
                                group.candidates[0].option_side
                                is side
                                for group in matching
                            )
                            and not any(
                                candidate.option_side is side
                                for selected_group
                                in selected_groups
                                if selected_group.underlying
                                == underlying
                                for candidate
                                in selected_group.candidates
                                if candidate.expiration
                                == expiration
                            )
                        ),
                        None,
                    )
                    group = next(
                        (
                            row
                            for row in matching
                            if (
                                missing_side is None
                                or row.candidates[0].option_side
                                is missing_side
                            )
                        ),
                        None,
                    )
                    if group is None:
                        break
                    used_by_underlying[underlying] += take(
                        underlying,
                        group,
                    )
        bootstrap = []
        basket_bootstrap = (
            profile.baskets[1:2]
            if profile.version
            == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
            else (
                profile.baskets[:2]
                if profile.version
                == ACTIVITY_MOMENTUM_PROFILE_VERSION
                else (None,)
            )
        )
        for target_basket in basket_bootstrap:
            for side in (OptionSide.CALL, OptionSide.PUT):
                candidate = next(
                    (
                        group
                        for group in queue
                        if any(
                            row.option_side is side
                            for row in group.candidates
                        )
                        and (
                            (
                                basket := profile.basket_for(
                                    dte=group.dte_bounds(
                                        session_date
                                    )[1],
                                    roles=group.roles,
                                )
                            )
                            is not None
                        )
                        and (
                            target_basket is None
                            or basket.basket_id
                            == target_basket.basket_id
                        )
                        and (
                            profile.version
                            in _BOUNDED_ACTIVITY_MOMENTUM_PROFILE_VERSIONS
                            or side is OptionSide.CALL
                            and basket.call_priority
                            or side is OptionSide.PUT
                            and basket.put_coverage_required_when_available
                        )
                    ),
                    None,
                )
                if (
                    candidate is not None
                    and candidate not in bootstrap
                ):
                    bootstrap.append(candidate)
        for group in bootstrap:
            slot_cost = incremental(group)
            if (
                used_by_underlying[underlying] + slot_cost
                <= initial_share
            ):
                used_by_underlying[underlying] += take(
                    underlying,
                    group,
                )
        if (
            profile.version
            in _BOUNDED_ACTIVITY_MOMENTUM_PROFILE_VERSIONS
        ):
            short_target = min(
                profile.short_basket_contract_target,
                (initial_share + 1) // 2,
            )
            core_target = min(
                profile.core_basket_contract_target,
                initial_share - short_target,
            )
            for basket, target in zip(
                profile.baskets[:2],
                (short_target, core_target),
            ):
                while True:
                    basket_tickers = {
                        ticker
                        for selected_group in selected_groups
                        if selected_group.underlying == underlying
                        and (
                            selected_basket := profile.basket_for(
                                dte=selected_group.dte_bounds(
                                    session_date
                                )[1],
                                roles=selected_group.roles,
                            )
                        )
                        is not None
                        and selected_basket.basket_id
                        == basket.basket_id
                        for ticker in selected_group.contract_tickers
                    }
                    if len(basket_tickers) >= target:
                        break
                    group = next(
                        (
                            row
                            for row in remaining[underlying]
                            if (
                                row_basket := profile.basket_for(
                                    dte=row.dte_bounds(
                                        session_date
                                    )[1],
                                    roles=row.roles,
                                )
                            )
                            is not None
                            and row_basket.basket_id
                            == basket.basket_id
                            and used_by_underlying[underlying]
                            + incremental(row)
                            <= initial_share
                        ),
                        None,
                    )
                    if group is None:
                        break
                    used_by_underlying[underlying] += take(
                        underlying,
                        group,
                    )
        while remaining[underlying]:
            group = next(
                (
                    row
                    for row in remaining[underlying]
                    if used_by_underlying[underlying]
                    + incremental(row)
                    <= initial_share
                    and within_v3_basket_limit(
                        underlying,
                        row,
                    )
                ),
                None,
            )
            if group is None:
                break
            slot_cost = incremental(group)
            used_by_underlying[underlying] += take(
                underlying,
                group,
            )

    while len(selected_tickers) < budget:
        progressed = False
        for underlying in rotated:
            queue = remaining[underlying]
            if not queue:
                continue
            group = next(
                (
                    row
                    for row in queue
                    if used_by_underlying[underlying]
                    + incremental(row)
                    <= per_underlying_limit
                    and len(selected_tickers)
                    + incremental(row)
                    <= budget
                    and within_v3_basket_limit(
                        underlying,
                        row,
                    )
                ),
                None,
            )
            if group is None:
                continue
            slot_cost = incremental(group)
            used_by_underlying[underlying] += take(
                underlying,
                group,
            )
            progressed = True
        if not progressed:
            break

    selected_ids = {row.group_id for row in selected_groups}
    for underlying in eligible:
        for group in grouped[underlying]:
            if group.group_id not in selected_ids:
                rejected.setdefault(
                    group.group_id,
                    ("BUDGET_DEFERRED_COMPLETE_GROUP",),
                )

    resolved_expiry_evidence = []
    if (
        profile.version
        == SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
    ):
        if resolution is None:
            raise ValueError("v3 expiry resolution is absent")
        shortlisted_by_ticker = {
            row.contract_ticker: row
            for row in resolution.shortlisted_candidates
        }
        for expiry_evidence in resolution.short_expiry_evidence:
            selected_candidates = {
                candidate.contract_ticker: candidate
                for group in selected_groups
                if group.underlying == expiry_evidence.underlying
                for candidate in group.candidates
                if 0 <= candidate.dte_for(session_date) <= 5
            }
            selected_counts = Counter(
                (row.expiration, row.option_side)
                for row in selected_candidates.values()
            )
            shortfalls = set(expiry_evidence.shortfall_reasons)
            if (
                len(selected_candidates)
                < profile.short_basket_contract_target
            ):
                shortfalls.add("SHORT_BASKET_SELECTION_SHORTFALL")
            near_count = sum(
                count
                for (expiration, _side), count
                in selected_counts.items()
                if expiration == expiry_evidence.near_expiry
            )
            end_of_week_count = sum(
                count
                for (expiration, _side), count
                in selected_counts.items()
                if expiration
                == expiry_evidence.end_of_week_expiry
            )
            if (
                expiry_evidence.near_expiry is not None
                and expiry_evidence.near_end_of_week_coincide
                and near_count
                < profile.short_basket_contract_target
            ):
                shortfalls.add(
                    "COINCIDENT_EXPIRY_SELECTION_SHORTFALL"
                )
            elif (
                expiry_evidence.near_expiry is not None
                and near_count
                < profile.short_near_expiry_contract_target
            ):
                shortfalls.add("NEAR_EXPIRY_SELECTION_SHORTFALL")
            if (
                expiry_evidence.end_of_week_expiry is not None
                and not expiry_evidence.near_end_of_week_coincide
                and end_of_week_count
                < profile.short_end_of_week_contract_target
            ):
                shortfalls.add(
                    "END_OF_WEEK_SELECTION_SHORTFALL"
                )
            eligible_sides = {
                row.option_side
                for row in resolution.shortlisted_candidates
                if row.underlying == expiry_evidence.underlying
                and 0 <= row.dte_for(session_date) <= 5
            }
            selected_sides = {
                row.option_side
                for row in selected_candidates.values()
            }
            if not eligible_sides.issubset(selected_sides):
                shortfalls.add("SHORT_SIDE_COVERAGE_SHORTFALL")
            required_tickers = {
                ticker
                for group in selected_groups
                if group.underlying == expiry_evidence.underlying
                and any(
                    "REQUIRED_LEG" in roles
                    for _, roles in group.contract_roles
                )
                for ticker in group.contract_tickers
            }
            selected_coverage_tickers = {
                candidate.contract_ticker
                for group in selected_groups
                if group.underlying == expiry_evidence.underlying
                for candidate in group.candidates
                if 0 <= candidate.dte_for(session_date) <= 21
            }
            desired_coverage = min(
                initial_share,
                profile.short_basket_contract_target
                + profile.core_basket_contract_target,
            )
            displaced = min(
                len(required_tickers),
                max(
                    0,
                    desired_coverage
                    - len(selected_coverage_tickers),
                ),
            )
            if displaced:
                shortfalls.add(
                    "REQUIRED_GROUP_DISPLACED_COVERAGE"
                )
            resolved_expiry_evidence.append(replace(
                expiry_evidence,
                shortfall_reasons=tuple(shortfalls),
                selected_counts=tuple(
                    (
                        expiration,
                        side,
                        count,
                    )
                    for (expiration, side), count
                    in selected_counts.items()
                ),
                ranking_anchor_contract_count=sum(
                    row.delta is not None
                    for row in selected_candidates.values()
                ),
                fallback_contract_count=sum(
                    row.delta is None
                    for row in selected_candidates.values()
                ),
                displaced_coverage_contract_count=displaced,
            ))
        if set(selected_tickers) - set(shortlisted_by_ticker):
            raise ValueError(
                "v3 allocation selected outside the shortlist"
            )
    expiry_shortfalls_by_underlying = {
        row.underlying: row.shortfall_reasons
        for row in resolved_expiry_evidence
    }
    coverage_rows = []
    for underlying in ready:
        roles = sorted({
            role
            for group in planned
            if group.underlying == underlying
            for role in group.roles
        } or set(profile.role_priority))
        for role in roles:
            planned_groups = tuple(
                group
                for group in planned
                if group.underlying == underlying
                and role in group.roles
            )
            admitted_groups = tuple(
                group
                for group in selected_groups
                if group.underlying == underlying
                and role in group.roles
            )
            planned_contracts = {
                row.contract_ticker
                for group in planned_groups
                for row in group.candidates
            }
            admitted_contracts = {
                row.contract_ticker
                for group in admitted_groups
                for row in group.candidates
            }
            ready_contracts = {
                row.contract_ticker
                for group in admitted_groups
                for row in group.candidates
                if not row.unavailable_reasons
            }
            missing_oi = {
                row.contract_ticker
                for group in planned_groups
                for row in group.candidates
                if row.prior_open_interest is None
            }
            rejected_ids = tuple(
                group.group_id
                for group in planned_groups
                if group.group_id not in selected_ids
            )
            if not planned_groups:
                state = CoverageState.UNCOVERED
                reasons = ("ROLE_HAS_NO_ELIGIBLE_GROUP",)
            elif len(admitted_groups) == len(planned_groups):
                state = CoverageState.FULL
                reasons = (
                    ("MISSING_PRIOR_OPEN_INTEREST",)
                    if missing_oi
                    else ()
                )
            elif admitted_groups:
                state = CoverageState.PARTIAL
                reasons = ("BUDGET_PARTIAL_COVERAGE",)
            else:
                state = CoverageState.UNAVAILABLE
                reasons = ("ROLE_NOT_ADMITTED",)
            if (
                role
                in {
                    "PARTICIPATION_CONTEXT",
                    "SHORT_DATED_SWEEP_LIKE",
                    "DIRECTIONAL_MOMENTUM",
                }
                and expiry_shortfalls_by_underlying.get(underlying)
            ):
                reasons = tuple(sorted({
                    *reasons,
                    *expiry_shortfalls_by_underlying[underlying],
                }))
            coverage_rows.append(ActivityMomentumCoverage(
                underlying=underlying,
                role=role,
                state=state,
                planned_group_count=len(planned_groups),
                admitted_group_count=len(admitted_groups),
                planned_contract_count=len(planned_contracts),
                admitted_contract_count=len(admitted_contracts),
                ready_contract_count=len(ready_contracts),
                missing_oi_contract_count=len(missing_oi),
                reason_codes=reasons,
                rejected_group_samples=tuple(
                    sorted(rejected_ids)[
                        :profile.rejected_group_sample_limit
                    ]
                ),
            ))
    rejection_counts = Counter(
        reason
        for reasons in rejected.values()
        for reason in reasons
    )
    planned_candidates = {
        row.contract_ticker: row
        for group in planned
        for row in group.candidates
    }
    if resolution is not None:
        if tuple(sorted(
            group.group_id for group in resolution.groups
        )) != tuple(sorted(group.group_id for group in planned)):
            raise ValueError(
                "activity/momentum resolution groups changed"
            )
        rejection_counts.update(dict(
            resolution.rejection_counts
        ))
        discovered_count = resolution.discovered_count
        filtered_count = resolution.filtered_count
        candidate_pool_count = resolution.candidate_pool_count
        shortlisted_count = resolution.shortlisted_count
        group_enumeration_count = (
            resolution.group_enumeration_count
        )
        ranking_as_of = resolution.ranking_as_of
    else:
        discovered_count = len(planned_candidates)
        filtered_count = len(planned_candidates)
        candidate_pool_count = len(planned_candidates)
        shortlisted_count = len(planned_candidates)
        group_enumeration_count = len(planned)
        ranking_as_of = tuple(
            value
            for candidate in planned_candidates.values()
            for value in (
                candidate.observed_at,
                candidate.quote_as_of,
                candidate.volume_as_of,
                candidate.open_interest_as_of,
                candidate.greeks_as_of,
            )
            if value is not None
        )
    admission_evidence = (
        None
        if profile.version
        == LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION
        else ActivityMomentumAdmissionEvidence(
            configured_total_budget=configured_budget,
            effective_total_budget=budget,
            configured_contracts_per_underlying=(
                configured_per_underlying
            ),
            effective_per_underlying_limit=(
                per_underlying_limit
            ),
            expansion_enabled=expansion_enabled,
            established_measured_capacity=measured_limit,
            effective_provider_project_ceiling=provider_ceiling,
            discovered_count=discovered_count,
            filtered_count=filtered_count,
            candidate_pool_count=candidate_pool_count,
            shortlisted_count=shortlisted_count,
            group_enumeration_count=group_enumeration_count,
            admitted_contract_count=len(selected_tickers),
            rejection_counts=tuple(
                rejection_counts.most_common(
                    profile.rejection_reason_sample_limit
                )
            ),
            ranking_as_of=ranking_as_of,
        )
    )
    values = {
        "profile_version": profile.version,
        "profile_sha256": profile.profile_sha256,
        "session_date": session_date,
        "rotation_key": rotation_seed,
        "active_contract_budget": budget,
        "quote_contract_ceiling": profile.quote_contract_ceiling,
        "quote_slot_reserve": (
            profile.quote_contract_ceiling - budget
        ),
        "initial_share": initial_share,
        "ready_underlyings": ready,
        "eligible_underlyings": eligible,
        "selected_groups": tuple(sorted(
            selected_groups,
            key=lambda row: row.group_id,
        )),
        "selected_contract_tickers": tuple(
            sorted(selected_tickers)
        ),
        "rejected_groups": tuple(sorted(
            (
                group_id,
                reasons,
            )
            for group_id, reasons in rejected.items()
        )),
        "coverage": tuple(sorted(
            coverage_rows,
            key=lambda row: (row.underlying, row.role),
        )),
        "admission_evidence": admission_evidence,
        "consumer_eligibility": (
            ()
            if profile.version
            == LEGACY_ACTIVITY_MOMENTUM_PROFILE_VERSION
            else _consumer_eligibility(
                profile,
                selected_groups,
            )
        ),
        "short_expiry_evidence": tuple(
            resolved_expiry_evidence
        ),
        "short_delta_override": (
            None
            if resolution is None
            else resolution.short_delta_override
        ),
    }
    unsigned = ActivityMomentumAllocation.__new__(
        ActivityMomentumAllocation
    )
    object.__setattr__(unsigned, "allocation_sha256", "0" * 64)
    for name, value in values.items():
        object.__setattr__(unsigned, name, value)
    digest = canonical_sha256(
        activity_momentum_allocation_payload(
            unsigned,
            include_identity=False,
        )
    )
    return ActivityMomentumAllocation(
        allocation_sha256=digest,
        **values,
    )


def compare_activity_momentum_profiles(
    *,
    session_date: date,
    candidates: Iterable[ActivityMomentumCandidate],
    v2_resolution: ActivityMomentumResolution,
    v2_allocation: ActivityMomentumAllocation,
    v3_resolution: ActivityMomentumResolution,
    v3_allocation: ActivityMomentumAllocation,
) -> dict[str, object]:
    if (
        v2_allocation.profile_version
        != ACTIVITY_MOMENTUM_PROFILE_VERSION
        or v3_allocation.profile_version
        != SUCCESSOR_ACTIVITY_MOMENTUM_PROFILE_VERSION
    ):
        raise ValueError(
            "profile comparison requires v2 and v3 allocations"
        )
    candidate_rows = tuple(sorted(
        candidates,
        key=lambda row: row.contract_ticker,
    ))
    if (
        not isinstance(session_date, date)
        or isinstance(session_date, datetime)
        or len({
            row.contract_ticker for row in candidate_rows
        })
        != len(candidate_rows)
    ):
        raise ValueError("profile comparison fixture is invalid")

    def summarize(
        resolution: ActivityMomentumResolution,
        allocation: ActivityMomentumAllocation,
    ) -> dict[str, object]:
        candidates = {
            candidate.contract_ticker: candidate
            for group in allocation.selected_groups
            for candidate in group.candidates
        }
        accepted_counts = Counter(
            (
                (
                    "SHORT_ACTIVITY"
                    if 0 <= candidate.dte_for(session_date) <= 5
                    else "CORE_ACTIVITY"
                    if 6 <= candidate.dte_for(session_date) <= 21
                    else "STRATEGY_EXTENSION"
                ),
                candidate.option_side.value,
                candidate.expiration.isoformat(),
            )
            for candidate in candidates.values()
        )
        rejected_counts = Counter(
            (
                (
                    "SHORT_ACTIVITY"
                    if 0 <= candidate.dte_for(session_date) <= 5
                    else "CORE_ACTIVITY"
                    if 6 <= candidate.dte_for(session_date) <= 21
                    else "STRATEGY_EXTENSION"
                ),
                candidate.option_side.value,
                candidate.expiration.isoformat(),
            )
            for candidate in candidate_rows
            if candidate.contract_ticker not in candidates
        )
        spreads = sorted(
            candidate.relative_spread
            for candidate in candidates.values()
            if candidate.relative_spread is not None
        )
        deltas = sorted(
            abs(candidate.delta)
            for candidate in candidates.values()
            if candidate.delta is not None
        )
        routed_event_invocation_units = sum(
            len(row.contract_tickers) * len(row.event_kinds)
            for row in allocation.consumer_eligibility
            if row.enabled
        )
        uncovered = sum(
            row.planned_contract_count
            - row.admitted_contract_count
            for row in allocation.coverage
        )
        return {
            "accepted_by_role_side_expiry": [
                {
                    "role": role,
                    "side": side,
                    "expiration": expiration,
                    "count": count,
                }
                for (role, side, expiration), count
                in sorted(accepted_counts.items())
            ],
            "rejected_by_role_side_expiry": [
                {
                    "role": role,
                    "side": side,
                    "expiration": expiration,
                    "count": count,
                }
                for (role, side, expiration), count
                in sorted(rejected_counts.items())
            ],
            "rejected_by_reason": [
                {"reason": reason, "count": count}
                for reason, count in resolution.rejection_counts
            ],
            "relative_spread_distribution": {
                "count": len(spreads),
                "minimum": (
                    None if not spreads else str(spreads[0])
                ),
                "maximum": (
                    None if not spreads else str(spreads[-1])
                ),
            },
            "absolute_delta_distribution": {
                "count": len(deltas),
                "minimum": None if not deltas else str(deltas[0]),
                "maximum": None if not deltas else str(deltas[-1]),
            },
            "fallback_contract_count": sum(
                candidate.delta is None
                for candidate in candidates.values()
            ),
            "coverage_loss_contract_count": uncovered,
            "preparation_work_counts": {
                "discovered": resolution.discovered_count,
                "filtered": resolution.filtered_count,
                "candidate_pool": resolution.candidate_pool_count,
                "shortlisted": resolution.shortlisted_count,
                "groups_enumerated": (
                    resolution.group_enumeration_count
                ),
                "admitted_contracts": len(candidates),
            },
            "routed_event_invocation_units": (
                routed_event_invocation_units
            ),
        }

    v2 = summarize(v2_resolution, v2_allocation)
    v3 = summarize(v3_resolution, v3_allocation)
    return {
        "schema": "c6-activity-momentum-v2-v3-comparison-v1",
        "evidence_class": (
            "OFFLINE_CALIBRATION_NOT_PRODUCTION_CAPACITY_EVIDENCE"
        ),
        "v2_profile_sha256": v2_allocation.profile_sha256,
        "v3_profile_sha256": v3_allocation.profile_sha256,
        "target_session_date": session_date.isoformat(),
        "fixture_candidate_count": len(candidate_rows),
        "v2": v2,
        "v3": v3,
        "routed_event_invocation_unit_difference": (
            int(v3["routed_event_invocation_units"])
            - int(v2["routed_event_invocation_units"])
        ),
        "production_superiority_claimed": False,
        "safe_capacity_claimed": False,
    }
