"""Configured Advanced Options C6 selection, capacity and shard state."""
from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from uuid import NAMESPACE_URL, UUID, uuid5

from psycopg2.extras import Json

from .advanced_options_flow_policy import (
    DEFAULT_ADVANCED_OPTIONS_FLOW_POLICY_PATH,
    load_advanced_options_flow_policy,
)
from .contracts import canonical_sha256


UTC = timezone.utc
DEFAULT_ADVANCED_OPTIONS_SELECTION_C6_POLICY_PATH = (
    Path(__file__).resolve().parent
    / "policies"
    / "advanced_options_selection_c6_v1.json"
)
ADVANCED_OPTIONS_SELECTION_C6_VERSION = "advanced-options-selection-c6-v1"
ADVANCED_OPTIONS_SELECTION_GENERATION_VERSION = (
    "advanced-options-selection-generation-c6-v1"
)
ADVANCED_OPTIONS_JOURNAL_RECORD_VERSION = (
    "advanced-options-generic-journal-record-c6-v1"
)
ADVANCED_OPTIONS_CONSUMER_STATE_VERSION = (
    "advanced-options-consumer-state-c6-v1"
)
EXPECTED_FUTURE_DETECTOR_IDS = frozenset({
    "VOLATILITY_CRUSH",
    "GAMMA_SQUEEZE_BURST",
    "SILENT_ACCUMULATION",
    "TERM_STRUCTURE_INVERSION",
    "EQUITY_OPTION_CONVERGENCE",
    "PUT_CALL_PARITY_DEVIATION",
    "VIX_VVIX_DIVERGENCE",
})
_OPTION_TICKER = re.compile(
    r"^O:([A-Z]{1,6})(\d{6})([CP])(\d{8})$"
)
_ROOT_KEYS = frozenset({
    "version",
    "status",
    "purpose",
    "runtime_activation",
    "compatibility",
    "universe",
    "strategy_coverage",
    "deferred_strategy_families",
    "future_detector_foundation",
    "requirement_resolution",
    "readiness",
    "flow_scope",
    "capacity",
    "persistence",
    "lifecycle",
    "output_boundary",
})
_FUTURE_FOUNDATION_KEYS = frozenset({
    "version",
    "status",
    "documentation",
    "defaults",
    "activation",
    "required_resolved_fields",
    "generic_ingress",
    "history_and_state",
    "detectors",
    "acceptance_fixture_ids",
})


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _text(value: object, name: str, *, maximum: int = 160) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be text")
    normalized = value.strip().upper()
    if not normalized or len(normalized) > maximum:
        raise ValueError(f"{name} is invalid")
    return normalized


def _identity_text(
    value: object,
    name: str,
    *,
    maximum: int = 256,
) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be text")
    normalized = value.strip()
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


def _string_tuple(
    value: object,
    name: str,
    *,
    allow_empty: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a list")
    values = tuple(
        _text(item, name, maximum=128)
        for item in value
    )
    if (
        (not allow_empty and not values)
        or len(values) != len(set(values))
    ):
        raise ValueError(f"{name} is invalid")
    return values


class OptionSide(str, Enum):
    CALL = "CALL"
    PUT = "PUT"


class C6ReadinessState(str, Enum):
    AVAILABLE = "AVAILABLE"
    WARMING = "WARMING"
    FAILED = "FAILED"
    UNAVAILABLE = "UNAVAILABLE"
    NOT_REQUIRED = "NOT_REQUIRED"


class C6ShardState(str, Enum):
    PLANNED = "PLANNED"
    WARMING = "WARMING"
    READY = "READY"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"


class FutureRequirementState(str, Enum):
    DISABLED = "DISABLED"
    UNAVAILABLE = "UNAVAILABLE"
    CAPTURE_ONLY = "CAPTURE_ONLY"
    DETECTOR_ENABLED = "DETECTOR_ENABLED"


class SourceTimestampUnit(str, Enum):
    SECOND = "SECOND"
    MILLISECOND = "MILLISECOND"
    MICROSECOND = "MICROSECOND"
    NANOSECOND = "NANOSECOND"

    @property
    def nanoseconds(self) -> int:
        return {
            SourceTimestampUnit.SECOND: 1_000_000_000,
            SourceTimestampUnit.MILLISECOND: 1_000_000,
            SourceTimestampUnit.MICROSECOND: 1_000,
            SourceTimestampUnit.NANOSECOND: 1,
        }[self]


@dataclass(frozen=True, slots=True)
class C6StrategyCoverage:
    objective: str
    registry_names: tuple[str, ...]
    requirement_sections: tuple[str, ...]
    contract_roles: tuple[str, ...]
    preserve_complete_leg_sets: bool

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "objective",
            _text(self.objective, "strategy objective"),
        )
        for name in (
            "registry_names",
            "requirement_sections",
            "contract_roles",
        ):
            values = tuple(
                _text(value, name, maximum=128)
                for value in getattr(self, name)
            )
            if not values or len(values) != len(set(values)):
                raise ValueError(f"{name} is invalid")
            object.__setattr__(self, name, values)
        if type(self.preserve_complete_leg_sets) is not bool:
            raise TypeError("preserve_complete_leg_sets must be boolean")


@dataclass(frozen=True, slots=True)
class FutureDetectorDeclaration:
    detector_id: str
    contract_coverage: Mapping[str, object]
    feeds: tuple[str, ...]
    shared_state: tuple[str, ...]
    unresolved_policy: tuple[str, ...]

    def __post_init__(self) -> None:
        detector_id = _text(
            self.detector_id,
            "future detector id",
        )
        feeds = tuple(
            _text(value, "future detector feed")
            for value in self.feeds
        )
        shared = tuple(
            _text(value, "future detector shared state")
            for value in self.shared_state
        )
        unresolved = tuple(
            _text(value, "future detector unresolved policy")
            for value in self.unresolved_policy
        )
        if (
            not isinstance(self.contract_coverage, Mapping)
            or not feeds
            or len(feeds) != len(set(feeds))
            or not shared
            or len(shared) != len(set(shared))
            or len(unresolved) != len(set(unresolved))
        ):
            raise ValueError("future detector declaration is invalid")
        object.__setattr__(self, "detector_id", detector_id)
        object.__setattr__(
            self,
            "contract_coverage",
            MappingProxyType(dict(self.contract_coverage)),
        )
        object.__setattr__(self, "feeds", feeds)
        object.__setattr__(self, "shared_state", shared)
        object.__setattr__(self, "unresolved_policy", unresolved)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsSelectionC6Policy:
    version: str
    policy_sha256: str
    flow_policy_sha256: str
    strategies: tuple[C6StrategyCoverage, ...]
    future_detectors: tuple[FutureDetectorDeclaration, ...]
    required_resolved_fields: tuple[str, ...]
    source_kinds: tuple[str, ...]
    requirements_registered: bool
    capture_enabled_by_default: bool
    detector_enabled_by_default: bool

    def __post_init__(self) -> None:
        if self.version != ADVANCED_OPTIONS_SELECTION_C6_VERSION:
            raise ValueError("C6 selection policy version changed")
        _digest(self.policy_sha256, "C6 selection policy_sha256")
        _digest(self.flow_policy_sha256, "C6 flow_policy_sha256")
        strategy_names = tuple(
            name
            for strategy in self.strategies
            for name in strategy.registry_names
        )
        detector_ids = {
            row.detector_id for row in self.future_detectors
        }
        if (
            not self.strategies
            or len(strategy_names) != len(set(strategy_names))
            or detector_ids != EXPECTED_FUTURE_DETECTOR_IDS
            or not self.required_resolved_fields
            or len(self.required_resolved_fields)
            != len(set(self.required_resolved_fields))
            or not self.source_kinds
            or len(self.source_kinds) != len(set(self.source_kinds))
            or self.requirements_registered is not True
            or self.capture_enabled_by_default is not False
            or self.detector_enabled_by_default is not False
        ):
            raise ValueError("C6 selection policy foundation is invalid")

    @property
    def strategy_map(self) -> Mapping[str, C6StrategyCoverage]:
        return MappingProxyType({
            name: strategy
            for strategy in self.strategies
            for name in strategy.registry_names
        })

    @property
    def future_detector_map(
        self,
    ) -> Mapping[str, FutureDetectorDeclaration]:
        return MappingProxyType({
            row.detector_id: row for row in self.future_detectors
        })


def load_advanced_options_selection_c6_policy(
    path: Path = DEFAULT_ADVANCED_OPTIONS_SELECTION_C6_POLICY_PATH,
    *,
    flow_policy_path: Path = DEFAULT_ADVANCED_OPTIONS_FLOW_POLICY_PATH,
) -> AdvancedOptionsSelectionC6Policy:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or frozenset(value) != _ROOT_KEYS:
        raise ValueError("C6 selection policy keys changed")
    if (
        value.get("version") != ADVANCED_OPTIONS_SELECTION_C6_VERSION
        or value.get("status") != "C6_RUNTIME_LOADED"
    ):
        raise ValueError("C6 selection policy identity changed")
    runtime_activation = value.get("runtime_activation")
    compatibility = value.get("compatibility")
    foundation = value.get("future_detector_foundation")
    if (
        not isinstance(runtime_activation, dict)
        or runtime_activation.get("enabled") is not True
        or runtime_activation.get(
            "changes_existing_detector_acceptance"
        )
        is not False
        or not isinstance(compatibility, dict)
        or not isinstance(foundation, dict)
        or frozenset(foundation) != _FUTURE_FOUNDATION_KEYS
        or compatibility.get("mutate_existing_flow_policy") is not False
        or compatibility.get("exact_four")
        != "SUPPORTED_REGRESSION_CONFIGURATION"
        or compatibility.get("session_state_owner")
        != "SessionRuntimeStatePort"
    ):
        raise ValueError("C6 compatibility contract changed")
    flow_policy = load_advanced_options_flow_policy(flow_policy_path)
    expected_flow_sha256 = _digest(
        compatibility.get("flow_policy_sha256"),
        "compatibility flow_policy_sha256",
    )
    if flow_policy.sha256 != expected_flow_sha256:
        raise ValueError("frozen Advanced Options flow policy identity changed")

    strategy_values = value.get("strategy_coverage")
    if not isinstance(strategy_values, list) or not strategy_values:
        raise ValueError("C6 strategy coverage is invalid")
    strategies = []
    for row in strategy_values:
        if not isinstance(row, dict):
            raise ValueError("C6 strategy coverage row is invalid")
        strategies.append(C6StrategyCoverage(
            objective=str(row["objective"]),
            registry_names=_string_tuple(
                row["registry_names"],
                "strategy registry names",
            ),
            requirement_sections=_string_tuple(
                row["requirement_sections"],
                "strategy requirement sections",
            ),
            contract_roles=_string_tuple(
                row["contract_roles"],
                "strategy contract roles",
            ),
            preserve_complete_leg_sets=bool(
                row.get("preserve_complete_leg_sets", False)
            ),
        ))

    detector_values = foundation.get("detectors")
    if not isinstance(detector_values, list):
        raise ValueError("future detector declarations are invalid")
    detectors = []
    for row in detector_values:
        if not isinstance(row, dict):
            raise ValueError("future detector declaration is invalid")
        detectors.append(FutureDetectorDeclaration(
            detector_id=str(row["id"]),
            contract_coverage=dict(row["contract_coverage"]),
            feeds=_string_tuple(row["feeds"], "future detector feeds"),
            shared_state=_string_tuple(
                row["shared_state"],
                "future detector shared state",
            ),
            unresolved_policy=_string_tuple(
                row["unresolved_policy"],
                "future detector unresolved policy",
                allow_empty=True,
            ),
        ))
    defaults = foundation.get("defaults")
    ingress = foundation.get("generic_ingress")
    if not isinstance(defaults, dict) or not isinstance(ingress, dict):
        raise ValueError("future detector defaults are invalid")
    return AdvancedOptionsSelectionC6Policy(
        version=str(value["version"]),
        policy_sha256=canonical_sha256(value),
        flow_policy_sha256=expected_flow_sha256,
        strategies=tuple(strategies),
        future_detectors=tuple(detectors),
        required_resolved_fields=_string_tuple(
            foundation["required_resolved_fields"],
            "future detector resolved fields",
        ),
        source_kinds=_string_tuple(
            ingress["source_kinds"],
            "generic ingress source kinds",
        ),
        requirements_registered=(
            defaults.get("requirements_registered") is True
        ),
        capture_enabled_by_default=(
            defaults.get("capture_enabled") is True
        ),
        detector_enabled_by_default=(
            defaults.get("detector_enabled") is True
        ),
    )


@dataclass(frozen=True, slots=True)
class StrategyPolicyIdentity:
    version: str
    content_sha256: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "version",
            _identity_text(self.version, "strategy policy version"),
        )
        _digest(self.content_sha256, "strategy policy content_sha256")


def load_strategy_policy_identity(path: Path) -> StrategyPolicyIdentity:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or not isinstance(
        value.get("strategy_version"),
        str,
    ):
        raise ValueError("strategy policy identity is invalid")
    return StrategyPolicyIdentity(
        version=str(value["strategy_version"]),
        content_sha256=canonical_sha256(value),
    )


@dataclass(frozen=True, slots=True)
class FutureDetectorActivation:
    detector_id: str
    capture_enabled: bool = False
    detector_enabled: bool = False
    model_accepted: bool = False
    dependencies_ready: bool = False
    resolved_fields: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        detector_id = _text(self.detector_id, "future detector id")
        fields = frozenset(
            _text(value, "future resolved field")
            for value in self.resolved_fields
        )
        if (
            type(self.capture_enabled) is not bool
            or type(self.detector_enabled) is not bool
            or type(self.model_accepted) is not bool
            or type(self.dependencies_ready) is not bool
            or self.detector_enabled and not self.capture_enabled
        ):
            raise ValueError("future detector activation is invalid")
        object.__setattr__(self, "detector_id", detector_id)
        object.__setattr__(self, "resolved_fields", fields)


@dataclass(frozen=True, slots=True)
class ResolvedFutureRequirement:
    detector_id: str
    declaration_sha256: str
    state: FutureRequirementState
    feeds: tuple[str, ...]
    missing_inputs: tuple[str, ...]
    capture_enabled: bool
    detector_enabled: bool

    def __post_init__(self) -> None:
        _digest(self.declaration_sha256, "future declaration_sha256")
        if (
            self.state is FutureRequirementState.DISABLED
            and (self.feeds or self.capture_enabled or self.detector_enabled)
        ):
            raise ValueError("disabled future requirement adds workload")
        if (
            self.state is FutureRequirementState.CAPTURE_ONLY
            and (not self.capture_enabled or self.detector_enabled)
        ):
            raise ValueError("capture-only future requirement is invalid")
        if (
            self.state is FutureRequirementState.DETECTOR_ENABLED
            and (not self.capture_enabled or not self.detector_enabled)
        ):
            raise ValueError("enabled future detector is invalid")


@dataclass(frozen=True, slots=True)
class ResolvedSelectionRequirements:
    selection_policy_version: str
    selection_policy_sha256: str
    strategy_policy: StrategyPolicyIdentity
    enabled_strategies: tuple[str, ...]
    strategy_objectives: tuple[str, ...]
    contract_roles: tuple[str, ...]
    unavailable_strategies: tuple[str, ...]
    future_requirements: tuple[ResolvedFutureRequirement, ...]

    @property
    def active_future_feeds(self) -> tuple[str, ...]:
        return tuple(sorted({
            feed
            for row in self.future_requirements
            for feed in row.feeds
        }))


def resolve_selection_requirements(
    policy: AdvancedOptionsSelectionC6Policy,
    *,
    strategy_policy: StrategyPolicyIdentity,
    enabled_strategies: Iterable[str],
    future_activations: Iterable[FutureDetectorActivation] = (),
) -> ResolvedSelectionRequirements:
    if not isinstance(policy, AdvancedOptionsSelectionC6Policy):
        raise TypeError("C6 selection policy is invalid")
    if not isinstance(strategy_policy, StrategyPolicyIdentity):
        raise TypeError("strategy policy identity is invalid")
    enabled = tuple(sorted({
        _text(value, "enabled strategy")
        for value in enabled_strategies
    }))
    available = policy.strategy_map
    selected = tuple(
        available[name] for name in enabled if name in available
    )
    unavailable = tuple(name for name in enabled if name not in available)
    activation_map = {}
    for row in future_activations:
        if not isinstance(row, FutureDetectorActivation):
            raise TypeError("future detector activation must be typed")
        if row.detector_id in activation_map:
            raise ValueError("future detector activation is duplicated")
        activation_map[row.detector_id] = row
    unknown = set(activation_map) - set(policy.future_detector_map)
    if unknown:
        raise ValueError(
            f"unknown future detector activation: {sorted(unknown)}"
        )
    future = []
    required = frozenset(policy.required_resolved_fields)
    for declaration in policy.future_detectors:
        activation = activation_map.get(
            declaration.detector_id,
            FutureDetectorActivation(declaration.detector_id),
        )
        missing = tuple(sorted(
            required - activation.resolved_fields
        ))
        declaration_sha256 = canonical_sha256({
            "id": declaration.detector_id,
            "contract_coverage": dict(declaration.contract_coverage),
            "feeds": declaration.feeds,
            "shared_state": declaration.shared_state,
            "unresolved_policy": declaration.unresolved_policy,
        })
        if not activation.capture_enabled:
            state = FutureRequirementState.DISABLED
            feeds = ()
            detector_enabled = False
        elif missing:
            state = FutureRequirementState.UNAVAILABLE
            feeds = ()
            detector_enabled = False
        elif activation.detector_enabled:
            if not activation.model_accepted or not activation.dependencies_ready:
                state = FutureRequirementState.UNAVAILABLE
                feeds = ()
                detector_enabled = False
                missing = tuple(sorted({
                    *missing,
                    *(
                        ()
                        if activation.model_accepted
                        else ("ACCEPTED_MODEL",)
                    ),
                    *(
                        ()
                        if activation.dependencies_ready
                        else ("READY_DEPENDENCIES",)
                    ),
                }))
            else:
                state = FutureRequirementState.DETECTOR_ENABLED
                feeds = declaration.feeds
                detector_enabled = True
        else:
            state = FutureRequirementState.CAPTURE_ONLY
            feeds = declaration.feeds
            detector_enabled = False
        future.append(ResolvedFutureRequirement(
            detector_id=declaration.detector_id,
            declaration_sha256=declaration_sha256,
            state=state,
            feeds=feeds,
            missing_inputs=missing,
            capture_enabled=bool(feeds),
            detector_enabled=detector_enabled,
        ))
    return ResolvedSelectionRequirements(
        selection_policy_version=policy.version,
        selection_policy_sha256=policy.policy_sha256,
        strategy_policy=strategy_policy,
        enabled_strategies=enabled,
        strategy_objectives=tuple(sorted({
            row.objective for row in selected
        })),
        contract_roles=tuple(sorted({
            role for row in selected for role in row.contract_roles
        })),
        unavailable_strategies=unavailable,
        future_requirements=tuple(future),
    )


def option_ticker_parts(ticker: str) -> tuple[str, OptionSide]:
    value = _text(ticker, "option contract ticker", maximum=48)
    match = _OPTION_TICKER.fullmatch(value)
    if match is None:
        raise ValueError("option contract ticker is invalid")
    return (
        match.group(1),
        OptionSide.CALL if match.group(3) == "C" else OptionSide.PUT,
    )


@dataclass(frozen=True, slots=True)
class C6SelectionMember:
    contract_id: int
    contract_ticker: str
    underlying: str
    expiration: date
    strike: Decimal
    option_side: OptionSide
    multiplier: Decimal
    deliverable: str
    catalog_version_id: int
    catalog_payload_sha256: str
    matching_strategies: tuple[str, ...]
    contract_roles: tuple[str, ...]
    selection_reasons: tuple[str, ...]
    evidence_references: tuple[str, ...]
    flow_eligible: bool

    def __post_init__(self) -> None:
        ticker = _text(
            self.contract_ticker,
            "contract ticker",
            maximum=48,
        )
        ticker_underlying, ticker_side = option_ticker_parts(ticker)
        underlying = _text(self.underlying, "underlying", maximum=16)
        for name in (
            "matching_strategies",
            "contract_roles",
            "selection_reasons",
        ):
            values = tuple(sorted({
                _text(value, name, maximum=160)
                for value in getattr(self, name)
            }))
            if name in {"contract_roles", "selection_reasons"} and not values:
                raise ValueError(f"{name} cannot be empty")
            object.__setattr__(self, name, values)
        evidence_references = tuple(sorted({
            _identity_text(
                value,
                "evidence_references",
                maximum=256,
            )
            for value in self.evidence_references
        }))
        object.__setattr__(
            self,
            "evidence_references",
            evidence_references,
        )
        if (
            type(self.contract_id) is not int
            or self.contract_id <= 0
            or type(self.catalog_version_id) is not int
            or self.catalog_version_id <= 0
            or ticker_underlying != underlying
            or ticker_side is not self.option_side
            or not isinstance(self.expiration, date)
            or not isinstance(self.strike, Decimal)
            or not self.strike.is_finite()
            or self.strike <= 0
            or not isinstance(self.multiplier, Decimal)
            or not self.multiplier.is_finite()
            or self.multiplier <= 0
            or type(self.flow_eligible) is not bool
        ):
            raise ValueError("C6 selection member is invalid")
        object.__setattr__(self, "contract_ticker", ticker)
        object.__setattr__(self, "underlying", underlying)
        object.__setattr__(
            self,
            "deliverable",
            _text(self.deliverable, "deliverable", maximum=80),
        )
        _digest(
            self.catalog_payload_sha256,
            "catalog_payload_sha256",
        )

    @property
    def member_sha256(self) -> str:
        return canonical_sha256(c6_selection_member_payload(self))


def c6_selection_member_payload(
    member: C6SelectionMember,
) -> dict[str, object]:
    return {
        "contract_id": member.contract_id,
        "contract_ticker": member.contract_ticker,
        "underlying": member.underlying,
        "expiration": member.expiration.isoformat(),
        "strike": str(member.strike),
        "option_side": member.option_side.value,
        "multiplier": str(member.multiplier),
        "deliverable": member.deliverable,
        "catalog_version_id": member.catalog_version_id,
        "catalog_payload_sha256": member.catalog_payload_sha256,
        "matching_strategies": list(member.matching_strategies),
        "contract_roles": list(member.contract_roles),
        "selection_reasons": list(member.selection_reasons),
        "evidence_references": list(member.evidence_references),
        "flow_eligible": member.flow_eligible,
    }


def c6_selection_member_from_payload(
    value: object,
) -> C6SelectionMember:
    if not isinstance(value, Mapping):
        raise TypeError("C6 selection member must be an object")
    member = C6SelectionMember(
        contract_id=int(value["contract_id"]),
        contract_ticker=str(value["contract_ticker"]),
        underlying=str(value["underlying"]),
        expiration=date.fromisoformat(str(value["expiration"])),
        strike=Decimal(str(value["strike"])),
        option_side=OptionSide(str(value["option_side"])),
        multiplier=Decimal(str(value["multiplier"])),
        deliverable=str(value["deliverable"]),
        catalog_version_id=int(value["catalog_version_id"]),
        catalog_payload_sha256=str(
            value["catalog_payload_sha256"]
        ),
        matching_strategies=tuple(
            str(item) for item in value["matching_strategies"]
        ),
        contract_roles=tuple(
            str(item) for item in value["contract_roles"]
        ),
        selection_reasons=tuple(
            str(item) for item in value["selection_reasons"]
        ),
        evidence_references=tuple(
            str(item) for item in value["evidence_references"]
        ),
        flow_eligible=value["flow_eligible"] is True,
    )
    if c6_selection_member_payload(member) != dict(value):
        raise ValueError("C6 selection member payload changed")
    return member


@dataclass(frozen=True, slots=True)
class C6UnderlyingReadiness:
    underlying: str
    catalog: C6ReadinessState
    open_interest: C6ReadinessState
    history: C6ReadinessState
    feed_states: tuple[tuple[str, C6ReadinessState], ...]
    reason_codes: tuple[str, ...]
    observed_at: datetime
    source_as_of: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "underlying",
            _text(self.underlying, "underlying", maximum=16),
        )
        observed = _utc(self.observed_at, "readiness observed_at")
        source = (
            None
            if self.source_as_of is None
            else _utc(self.source_as_of, "readiness source_as_of")
        )
        feeds = tuple(sorted(
            (
                _text(name, "feed capability"),
                state,
            )
            for name, state in self.feed_states
        ))
        reasons = tuple(sorted({
            _text(value, "readiness reason", maximum=128)
            for value in self.reason_codes
        }))
        if (
            any(not isinstance(state, C6ReadinessState) for _, state in feeds)
            or len({name for name, _ in feeds}) != len(feeds)
            or source is not None
            and source > observed
        ):
            raise ValueError("C6 underlying readiness is invalid")
        object.__setattr__(self, "observed_at", observed)
        object.__setattr__(self, "source_as_of", source)
        object.__setattr__(self, "feed_states", feeds)
        object.__setattr__(self, "reason_codes", reasons)

    @property
    def ready(self) -> bool:
        required = (
            self.catalog,
            self.open_interest,
            self.history,
            *(
                state
                for _, state in self.feed_states
                if state is not C6ReadinessState.NOT_REQUIRED
            ),
        )
        return all(
            state is C6ReadinessState.AVAILABLE
            for state in required
        )


def c6_underlying_readiness_payload(
    readiness: C6UnderlyingReadiness,
) -> dict[str, object]:
    return {
        "underlying": readiness.underlying,
        "catalog": readiness.catalog.value,
        "open_interest": readiness.open_interest.value,
        "history": readiness.history.value,
        "feed_states": {
            name: state.value for name, state in readiness.feed_states
        },
        "reason_codes": list(readiness.reason_codes),
        "observed_at": readiness.observed_at.isoformat(),
        "source_as_of": (
            None
            if readiness.source_as_of is None
            else readiness.source_as_of.isoformat()
        ),
        "ready": readiness.ready,
    }


def c6_underlying_readiness_from_payload(
    value: object,
) -> C6UnderlyingReadiness:
    if not isinstance(value, Mapping) or not isinstance(
        value.get("feed_states"),
        Mapping,
    ):
        raise TypeError("C6 readiness must be an object")
    readiness = C6UnderlyingReadiness(
        underlying=str(value["underlying"]),
        catalog=C6ReadinessState(str(value["catalog"])),
        open_interest=C6ReadinessState(str(value["open_interest"])),
        history=C6ReadinessState(str(value["history"])),
        feed_states=tuple(
            (str(name), C6ReadinessState(str(state)))
            for name, state in value["feed_states"].items()
        ),
        reason_codes=tuple(
            str(item) for item in value["reason_codes"]
        ),
        observed_at=datetime.fromisoformat(str(value["observed_at"])),
        source_as_of=(
            None
            if value.get("source_as_of") is None
            else datetime.fromisoformat(str(value["source_as_of"]))
        ),
    )
    if c6_underlying_readiness_payload(readiness) != dict(value):
        raise ValueError("C6 readiness payload changed")
    return readiness


@dataclass(frozen=True, slots=True)
class C6ConnectionCapacity:
    account_alias: str
    provider_cluster: str
    configured_cluster_connection_limit: int
    provider_cluster_connection_limit: int
    active_cluster_connections: int
    proposed_physical_connections: int
    quote_contract_ceiling: int
    active_quote_contract_budget: int
    quote_slot_reserve: int
    maximum_contracts_per_logical_shard: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "account_alias",
            _text(self.account_alias, "account alias"),
        )
        object.__setattr__(
            self,
            "provider_cluster",
            _text(self.provider_cluster, "provider cluster"),
        )
        positive = (
            self.configured_cluster_connection_limit,
            self.provider_cluster_connection_limit,
            self.proposed_physical_connections,
            self.quote_contract_ceiling,
            self.active_quote_contract_budget,
            self.maximum_contracts_per_logical_shard,
        )
        if (
            self.provider_cluster != "OPTIONS"
            or any(
                type(value) is not int or value <= 0
                for value in positive
            )
            or type(self.active_cluster_connections) is not int
            or self.active_cluster_connections < 0
            or type(self.quote_slot_reserve) is not int
            or self.quote_slot_reserve < 0
            or self.active_quote_contract_budget
            + self.quote_slot_reserve
            > self.quote_contract_ceiling
        ):
            raise ValueError("C6 connection capacity is invalid")

    @property
    def effective_cluster_connection_limit(self) -> int:
        return min(
            self.configured_cluster_connection_limit,
            self.provider_cluster_connection_limit,
        )


@dataclass(frozen=True, slots=True)
class C6CapacityDecision:
    account_alias: str
    provider_cluster: str
    effective_cluster_connection_limit: int
    active_cluster_connections: int
    proposed_physical_connections: int
    required_cluster_connections: int
    quote_contract_ceiling: int
    active_quote_contract_budget: int
    quote_slot_reserve: int
    selected_quote_contracts: int
    logical_shard_count: int
    accepted: bool
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "account_alias",
            _text(self.account_alias, "account alias"),
        )
        object.__setattr__(
            self,
            "provider_cluster",
            _text(self.provider_cluster, "provider cluster"),
        )
        counts = (
            self.effective_cluster_connection_limit,
            self.active_cluster_connections,
            self.proposed_physical_connections,
            self.required_cluster_connections,
            self.quote_contract_ceiling,
            self.active_quote_contract_budget,
            self.quote_slot_reserve,
            self.selected_quote_contracts,
            self.logical_shard_count,
        )
        reasons = tuple(sorted({
            _text(value, "capacity reason")
            for value in self.reason_codes
        }))
        expected_accepted = (
            self.required_cluster_connections
            <= self.effective_cluster_connection_limit
            and self.selected_quote_contracts
            <= self.active_quote_contract_budget
            and self.active_quote_contract_budget
            + self.quote_slot_reserve
            <= self.quote_contract_ceiling
        )
        if (
            self.provider_cluster != "OPTIONS"
            or type(self.effective_cluster_connection_limit) is not int
            or self.effective_cluster_connection_limit <= 0
            or any(
                type(value) is not int or value < 0
                for value in counts[1:]
            )
            or self.proposed_physical_connections <= 0
            or self.quote_contract_ceiling <= 0
            or self.active_quote_contract_budget <= 0
            or self.logical_shard_count <= 0
            or self.required_cluster_connections
            != (
                self.active_cluster_connections
                + self.proposed_physical_connections
            )
            or type(self.accepted) is not bool
            or self.accepted != expected_accepted
            or self.accepted and reasons
            or not self.accepted and not reasons
        ):
            raise ValueError("C6 capacity decision is invalid")
        object.__setattr__(self, "reason_codes", reasons)


class C6CapacityError(ValueError):
    def __init__(self, decision: C6CapacityDecision) -> None:
        self.decision = decision
        super().__init__(
            "C6 Options plan exceeds provider-cluster connection or "
            "quote-slot capacity"
        )


@dataclass(frozen=True, slots=True)
class C6PhysicalSubscription:
    subscription_id: UUID
    account_alias: str
    provider_cluster: str
    session_date: date
    owner_id: UUID
    journal_generation: UUID
    contract_tickers: tuple[str, ...]
    member_sha256: str

    def __post_init__(self) -> None:
        tickers = tuple(sorted(
            _text(value, "subscription contract ticker", maximum=48)
            for value in self.contract_tickers
        ))
        for ticker in tickers:
            option_ticker_parts(ticker)
        if (
            not all(
                isinstance(value, UUID) and value.int
                for value in (
                    self.subscription_id,
                    self.owner_id,
                    self.journal_generation,
                )
            )
            or not tickers
            or len(tickers) != len(set(tickers))
            or not isinstance(self.session_date, date)
            or self.member_sha256
            != canonical_sha256({"contract_tickers": tickers})
        ):
            raise ValueError("C6 physical subscription is invalid")
        object.__setattr__(
            self,
            "account_alias",
            _text(self.account_alias, "account alias"),
        )
        object.__setattr__(
            self,
            "provider_cluster",
            _text(self.provider_cluster, "provider cluster"),
        )
        if self.provider_cluster != "OPTIONS":
            raise ValueError("C6 physical subscription cluster changed")
        object.__setattr__(self, "contract_tickers", tickers)


def c6_physical_subscription_payload(
    subscription: C6PhysicalSubscription,
) -> dict[str, object]:
    return {
        "subscription_id": str(subscription.subscription_id),
        "account_alias": subscription.account_alias,
        "provider_cluster": subscription.provider_cluster,
        "session_date": subscription.session_date.isoformat(),
        "owner_id": str(subscription.owner_id),
        "journal_generation": str(subscription.journal_generation),
        "contract_tickers": list(subscription.contract_tickers),
        "member_sha256": subscription.member_sha256,
    }


def c6_physical_subscription_from_payload(
    value: object,
) -> C6PhysicalSubscription:
    if not isinstance(value, Mapping):
        raise TypeError("C6 physical subscription must be an object")
    subscription = C6PhysicalSubscription(
        subscription_id=UUID(str(value["subscription_id"])),
        account_alias=str(value["account_alias"]),
        provider_cluster=str(value["provider_cluster"]),
        session_date=date.fromisoformat(str(value["session_date"])),
        owner_id=UUID(str(value["owner_id"])),
        journal_generation=UUID(str(value["journal_generation"])),
        contract_tickers=tuple(
            str(item) for item in value["contract_tickers"]
        ),
        member_sha256=str(value["member_sha256"]),
    )
    if c6_physical_subscription_payload(subscription) != dict(value):
        raise ValueError("C6 physical subscription payload changed")
    return subscription


@dataclass(frozen=True, slots=True)
class C6SubscriptionShard:
    shard_id: UUID
    shard_slot: int
    physical_subscription_id: UUID
    contract_tickers: tuple[str, ...]
    member_sha256: str

    def __post_init__(self) -> None:
        tickers = tuple(sorted(
            _text(value, "logical shard contract ticker", maximum=48)
            for value in self.contract_tickers
        ))
        if (
            not isinstance(self.shard_id, UUID)
            or not self.shard_id.int
            or not isinstance(self.physical_subscription_id, UUID)
            or not self.physical_subscription_id.int
            or type(self.shard_slot) is not int
            or self.shard_slot < 1
            or not tickers
            or len(tickers) != len(set(tickers))
            or self.member_sha256
            != canonical_sha256({"contract_tickers": tickers})
        ):
            raise ValueError("C6 logical subscription shard is invalid")
        for ticker in tickers:
            option_ticker_parts(ticker)
        object.__setattr__(self, "contract_tickers", tickers)


def c6_subscription_shard_payload(
    shard: C6SubscriptionShard,
) -> dict[str, object]:
    return {
        "shard_id": str(shard.shard_id),
        "shard_slot": shard.shard_slot,
        "physical_subscription_id": str(
            shard.physical_subscription_id
        ),
        "contract_tickers": list(shard.contract_tickers),
        "member_sha256": shard.member_sha256,
    }


def c6_subscription_shard_from_payload(
    value: object,
) -> C6SubscriptionShard:
    if not isinstance(value, Mapping):
        raise TypeError("C6 logical shard must be an object")
    shard = C6SubscriptionShard(
        shard_id=UUID(str(value["shard_id"])),
        shard_slot=int(value["shard_slot"]),
        physical_subscription_id=UUID(
            str(value["physical_subscription_id"])
        ),
        contract_tickers=tuple(
            str(item) for item in value["contract_tickers"]
        ),
        member_sha256=str(value["member_sha256"]),
    )
    if c6_subscription_shard_payload(shard) != dict(value):
        raise ValueError("C6 logical shard payload changed")
    return shard


@dataclass(frozen=True, slots=True)
class C6AllocationCoverage:
    underlying: str
    role: str
    state: str
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
            self.state not in {
                "FULL",
                "PARTIAL",
                "UNCOVERED",
                "UNAVAILABLE",
            }
            or any(type(value) is not int or value < 0 for value in counts)
            or self.admitted_group_count > self.planned_group_count
            or self.admitted_contract_count > self.planned_contract_count
            or self.ready_contract_count > self.admitted_contract_count
        ):
            raise ValueError("C6 allocation coverage is invalid")


def c6_allocation_coverage_payload(
    coverage: C6AllocationCoverage,
) -> dict[str, object]:
    return {
        "underlying": coverage.underlying,
        "role": coverage.role,
        "state": coverage.state,
        "planned_group_count": coverage.planned_group_count,
        "admitted_group_count": coverage.admitted_group_count,
        "planned_contract_count": coverage.planned_contract_count,
        "admitted_contract_count": coverage.admitted_contract_count,
        "ready_contract_count": coverage.ready_contract_count,
        "missing_oi_contract_count": coverage.missing_oi_contract_count,
        "reason_codes": list(coverage.reason_codes),
        "rejected_group_samples": list(
            coverage.rejected_group_samples
        ),
    }


def c6_allocation_coverage_from_payload(
    value: object,
) -> C6AllocationCoverage:
    if not isinstance(value, Mapping):
        raise TypeError("C6 allocation coverage must be an object")
    coverage = C6AllocationCoverage(
        underlying=str(value["underlying"]),
        role=str(value["role"]),
        state=str(value["state"]),
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
    if c6_allocation_coverage_payload(coverage) != dict(value):
        raise ValueError("C6 allocation coverage payload changed")
    return coverage


def _layout_shards(
    members: tuple[C6SelectionMember, ...],
    *,
    maximum_contracts_per_shard: int,
    previous_shards: tuple[C6SubscriptionShard, ...],
) -> tuple[tuple[int, tuple[str, ...]], ...]:
    by_underlying: dict[str, tuple[str, ...]] = {}
    for underlying in sorted({row.underlying for row in members}):
        tickers = tuple(
            row.contract_ticker
            for row in members
            if row.underlying == underlying
        )
        if len(tickers) > maximum_contracts_per_shard:
            raise ValueError(
                f"underlying {underlying} exceeds per-shard contract maximum"
            )
        by_underlying[underlying] = tickers
    previous_slot_by_ticker = {
        ticker: shard.shard_slot
        for shard in previous_shards
        for ticker in shard.contract_tickers
    }
    bins: dict[int, list[str]] = {}
    assigned: set[str] = set()
    for underlying, tickers in by_underlying.items():
        slots = {
            previous_slot_by_ticker[ticker]
            for ticker in tickers
            if ticker in previous_slot_by_ticker
        }
        if len(slots) == 1:
            slot = next(iter(slots))
            current = bins.setdefault(slot, [])
            if len(current) + len(tickers) <= maximum_contracts_per_shard:
                current.extend(tickers)
                assigned.add(underlying)
    next_slot = 1
    for underlying, tickers in by_underlying.items():
        if underlying in assigned:
            continue
        slot = next(
            (
                candidate
                for candidate in sorted(bins)
                if len(bins[candidate]) + len(tickers)
                <= maximum_contracts_per_shard
            ),
            None,
        )
        if slot is None:
            while next_slot in bins:
                next_slot += 1
            slot = next_slot
            bins[slot] = []
        bins[slot].extend(tickers)
    return tuple(
        (slot, tuple(sorted(tickers)))
        for slot, tickers in sorted(bins.items())
        if tickers
    )


def build_c6_subscription_shards(
    members: Iterable[C6SelectionMember],
    *,
    session_date: date,
    capacity: C6ConnectionCapacity,
    previous_shards: Iterable[C6SubscriptionShard] = (),
    physical_owner_id: UUID | None = None,
    journal_generation: UUID | None = None,
) -> tuple[
    C6PhysicalSubscription,
    tuple[C6SubscriptionShard, ...],
    C6CapacityDecision,
]:
    selected = tuple(sorted(
        members,
        key=lambda row: row.contract_ticker,
    ))
    previous = tuple(previous_shards)
    if not selected or not isinstance(capacity, C6ConnectionCapacity):
        raise ValueError("C6 shard selection is empty")
    layouts = _layout_shards(
        selected,
        maximum_contracts_per_shard=(
            capacity.maximum_contracts_per_logical_shard
        ),
        previous_shards=previous,
    )
    required_connections = (
        capacity.active_cluster_connections
        + capacity.proposed_physical_connections
    )
    reasons = []
    if (
        required_connections
        > capacity.effective_cluster_connection_limit
    ):
        reasons.append("OPTIONS_CLUSTER_CONNECTION_CAPACITY_EXCEEDED")
    if len(selected) > capacity.active_quote_contract_budget:
        reasons.append("OPTIONS_ACTIVE_QUOTE_BUDGET_EXCEEDED")
    if (
        capacity.active_quote_contract_budget
        + capacity.quote_slot_reserve
        > capacity.quote_contract_ceiling
    ):
        reasons.append("OPTIONS_QUOTE_CEILING_EXCEEDED")
    accepted = not reasons
    decision = C6CapacityDecision(
        account_alias=capacity.account_alias,
        provider_cluster=capacity.provider_cluster,
        effective_cluster_connection_limit=(
            capacity.effective_cluster_connection_limit
        ),
        active_cluster_connections=(
            capacity.active_cluster_connections
        ),
        proposed_physical_connections=(
            capacity.proposed_physical_connections
        ),
        required_cluster_connections=required_connections,
        quote_contract_ceiling=capacity.quote_contract_ceiling,
        active_quote_contract_budget=(
            capacity.active_quote_contract_budget
        ),
        quote_slot_reserve=capacity.quote_slot_reserve,
        selected_quote_contracts=len(selected),
        logical_shard_count=len(layouts),
        accepted=accepted,
        reason_codes=tuple(reasons),
    )
    if not accepted:
        raise C6CapacityError(decision)
    subscription_id = uuid5(
        NAMESPACE_URL,
        "stockscreener:c6-options-physical-subscription:"
        f"{capacity.account_alias}:{capacity.provider_cluster}",
    )
    physical = C6PhysicalSubscription(
        subscription_id=subscription_id,
        account_alias=capacity.account_alias,
        provider_cluster=capacity.provider_cluster,
        session_date=session_date,
        owner_id=(
            uuid5(
                NAMESPACE_URL,
                "stockscreener:c6-options-physical-owner:"
                f"{session_date.isoformat()}:{subscription_id}",
            )
            if physical_owner_id is None
            else physical_owner_id
        ),
        journal_generation=(
            uuid5(
                NAMESPACE_URL,
                "stockscreener:c6-options-physical-journal:"
                f"{session_date.isoformat()}:{subscription_id}",
            )
            if journal_generation is None
            else journal_generation
        ),
        contract_tickers=tuple(
            row.contract_ticker for row in selected
        ),
        member_sha256=canonical_sha256({
            "contract_tickers": tuple(
                row.contract_ticker for row in selected
            )
        }),
    )
    previous_by_slot = {
        shard.shard_slot: shard for shard in previous
    }
    shards = []
    for slot, tickers in layouts:
        prior = previous_by_slot.get(slot)
        shard_id = (
            prior.shard_id
            if prior is not None
            else uuid5(
                NAMESPACE_URL,
                "stockscreener:c6-options-logical-shard:"
                f"{capacity.account_alias}:"
                f"{capacity.provider_cluster}:{slot}",
            )
        )
        shards.append(C6SubscriptionShard(
            shard_id=shard_id,
            shard_slot=slot,
            physical_subscription_id=physical.subscription_id,
            contract_tickers=tickers,
            member_sha256=canonical_sha256({
                "contract_tickers": tickers
            }),
        ))
    return physical, tuple(shards), decision


@dataclass(frozen=True, slots=True)
class C6SelectionPlan:
    generation_id: UUID
    generation_sha256: str
    session_date: date
    desired_revision_id: UUID
    desired_snapshot_sha256: str
    selection_policy_version: str
    selection_policy_sha256: str
    flow_policy_sha256: str
    admission_profile_version: str
    admission_profile_sha256: str
    allocation_sha256: str
    allocation_document: Mapping[str, object]
    strategy_policies: tuple[StrategyPolicyIdentity, ...]
    catalog_identity: str
    reference_identity: str
    source_as_of: datetime
    broad_underlyings: tuple[str, ...]
    flow_underlyings: tuple[str, ...]
    members: tuple[C6SelectionMember, ...]
    readiness: tuple[C6UnderlyingReadiness, ...]
    coverage: tuple[C6AllocationCoverage, ...]
    physical_subscription: C6PhysicalSubscription
    shards: tuple[C6SubscriptionShard, ...]
    capacity: C6CapacityDecision
    future_requirements: tuple[ResolvedFutureRequirement, ...] = ()

    def __post_init__(self) -> None:
        source_as_of = _utc(self.source_as_of, "selection source_as_of")
        broad = tuple(sorted({
            _text(value, "broad underlying", maximum=16)
            for value in self.broad_underlyings
        }))
        flow = tuple(sorted({
            _text(value, "flow underlying", maximum=16)
            for value in self.flow_underlyings
        }))
        members = tuple(sorted(
            self.members,
            key=lambda row: row.contract_ticker,
        ))
        readiness = tuple(sorted(
            self.readiness,
            key=lambda row: row.underlying,
        ))
        shards = tuple(sorted(
            self.shards,
            key=lambda row: row.shard_slot,
        ))
        coverage = tuple(sorted(
            self.coverage,
            key=lambda row: (row.underlying, row.role),
        ))
        allocation_document = MappingProxyType(
            dict(self.allocation_document)
        )
        member_tickers = {
            row.contract_ticker for row in members
        }
        if (
            not isinstance(self.generation_id, UUID)
            or not self.generation_id.int
            or not isinstance(self.desired_revision_id, UUID)
            or not self.desired_revision_id.int
            or not broad
            or not members
            or self.selection_policy_version
            != ADVANCED_OPTIONS_SELECTION_C6_VERSION
            or not self.strategy_policies
            or not isinstance(
                self.physical_subscription,
                C6PhysicalSubscription,
            )
            or self.physical_subscription.session_date
            != self.session_date
            or self.physical_subscription.account_alias
            != self.capacity.account_alias
            or self.physical_subscription.provider_cluster
            != self.capacity.provider_cluster
            or any(
                not isinstance(row, StrategyPolicyIdentity)
                for row in self.strategy_policies
            )
            or any(
                not isinstance(row, ResolvedFutureRequirement)
                for row in self.future_requirements
            )
            or {row.underlying for row in members} != set(broad)
            or not set(flow).issubset(broad)
            or {
                row.underlying for row in members if row.flow_eligible
            }
            != set(flow)
            or {row.underlying for row in readiness} != set(broad)
            or not coverage
            or any(
                row.underlying not in set(broad)
                for row in coverage
            )
            or allocation_document.get("allocation_sha256")
            != self.allocation_sha256
            or (
                self.admission_profile_version
                in {
                    "c6_activity_momentum_v1",
                    "c6_activity_momentum_v2",
                    "c6_activity_momentum_v3",
                }
                and (
                    allocation_document.get(
                        "active_contract_budget"
                    )
                    != self.capacity.active_quote_contract_budget
                    or allocation_document.get(
                        "quote_contract_ceiling"
                    )
                    != self.capacity.quote_contract_ceiling
                    or allocation_document.get(
                        "quote_slot_reserve"
                    )
                    != self.capacity.quote_slot_reserve
                )
            )
            or {
                ticker
                for shard in shards
                for ticker in shard.contract_tickers
            }
            != member_tickers
            or set(self.physical_subscription.contract_tickers)
            != member_tickers
            or sum(len(row.contract_tickers) for row in shards)
            != len(member_tickers)
            or any(
                row.physical_subscription_id
                != self.physical_subscription.subscription_id
                for row in shards
            )
            or not self.capacity.accepted
        ):
            raise ValueError("C6 selection plan scope is invalid")
        for name in (
            "desired_snapshot_sha256",
            "selection_policy_sha256",
            "flow_policy_sha256",
            "admission_profile_sha256",
            "allocation_sha256",
        ):
            _digest(getattr(self, name), name)
        object.__setattr__(self, "source_as_of", source_as_of)
        object.__setattr__(
            self,
            "catalog_identity",
            _identity_text(
                self.catalog_identity,
                "catalog identity",
            ),
        )
        object.__setattr__(
            self,
            "reference_identity",
            _identity_text(
                self.reference_identity,
                "reference identity",
            ),
        )
        object.__setattr__(self, "broad_underlyings", broad)
        object.__setattr__(self, "flow_underlyings", flow)
        object.__setattr__(self, "members", members)
        object.__setattr__(self, "readiness", readiness)
        object.__setattr__(self, "coverage", coverage)
        object.__setattr__(
            self,
            "allocation_document",
            allocation_document,
        )
        object.__setattr__(self, "shards", shards)
        expected = canonical_sha256(
            c6_selection_plan_payload(self, include_identity=False)
        )
        expected_id = uuid5(
            NAMESPACE_URL,
            f"stockscreener:c6-options-selection:{expected}",
        )
        if (
            self.generation_sha256 != expected
            or self.generation_id != expected_id
        ):
            raise ValueError("C6 selection generation identity changed")

    @property
    def ready_underlyings(self) -> tuple[str, ...]:
        return tuple(
            row.underlying for row in self.readiness if row.ready
        )

    def product_ready(
        self,
        required_underlyings: Iterable[str],
        *,
        complete_membership_required: bool = False,
    ) -> bool:
        required = {
            _text(value, "product underlying", maximum=16)
            for value in required_underlyings
        }
        ready = set(self.ready_underlyings)
        if complete_membership_required:
            return required == set(self.broad_underlyings) == ready
        return required.issubset(ready)


def _future_requirement_payload(
    row: ResolvedFutureRequirement,
) -> dict[str, object]:
    return {
        "detector_id": row.detector_id,
        "declaration_sha256": row.declaration_sha256,
        "state": row.state.value,
        "feeds": list(row.feeds),
        "missing_inputs": list(row.missing_inputs),
        "capture_enabled": row.capture_enabled,
        "detector_enabled": row.detector_enabled,
    }


def _capacity_payload(
    capacity: C6CapacityDecision,
) -> dict[str, object]:
    return {
        "account_alias": capacity.account_alias,
        "provider_cluster": capacity.provider_cluster,
        "effective_cluster_connection_limit": (
            capacity.effective_cluster_connection_limit
        ),
        "active_cluster_connections": (
            capacity.active_cluster_connections
        ),
        "proposed_physical_connections": (
            capacity.proposed_physical_connections
        ),
        "required_cluster_connections": (
            capacity.required_cluster_connections
        ),
        "quote_contract_ceiling": capacity.quote_contract_ceiling,
        "active_quote_contract_budget": (
            capacity.active_quote_contract_budget
        ),
        "quote_slot_reserve": capacity.quote_slot_reserve,
        "selected_quote_contracts": (
            capacity.selected_quote_contracts
        ),
        "logical_shard_count": capacity.logical_shard_count,
        "accepted": capacity.accepted,
        "reason_codes": list(capacity.reason_codes),
    }


def c6_selection_plan_payload(
    plan: C6SelectionPlan,
    *,
    include_identity: bool = True,
) -> dict[str, object]:
    value: dict[str, object] = {
        "schema": ADVANCED_OPTIONS_SELECTION_GENERATION_VERSION,
        "session_date": plan.session_date.isoformat(),
        "desired_revision_id": str(plan.desired_revision_id),
        "desired_snapshot_sha256": plan.desired_snapshot_sha256,
        "selection_policy": {
            "version": plan.selection_policy_version,
            "sha256": plan.selection_policy_sha256,
        },
        "flow_policy_sha256": plan.flow_policy_sha256,
        "admission_profile": {
            "version": plan.admission_profile_version,
            "sha256": plan.admission_profile_sha256,
        },
        "allocation_sha256": plan.allocation_sha256,
        "allocation_document": dict(plan.allocation_document),
        "strategy_policies": [
            {
                "version": row.version,
                "content_sha256": row.content_sha256,
            }
            for row in plan.strategy_policies
        ],
        "catalog_identity": plan.catalog_identity,
        "reference_identity": plan.reference_identity,
        "source_as_of": plan.source_as_of.isoformat(),
        "broad_underlyings": list(plan.broad_underlyings),
        "flow_underlyings": list(plan.flow_underlyings),
        "members": [
            c6_selection_member_payload(row) for row in plan.members
        ],
        "readiness": [
            c6_underlying_readiness_payload(row)
            for row in plan.readiness
        ],
        "coverage": [
            c6_allocation_coverage_payload(row)
            for row in plan.coverage
        ],
        "physical_subscription": (
            c6_physical_subscription_payload(
                plan.physical_subscription
            )
        ),
        "shards": [
            c6_subscription_shard_payload(row) for row in plan.shards
        ],
        "capacity": _capacity_payload(plan.capacity),
        "future_requirements": [
            _future_requirement_payload(row)
            for row in plan.future_requirements
        ],
    }
    if include_identity:
        value["generation_id"] = str(plan.generation_id)
        value["generation_sha256"] = plan.generation_sha256
    return value


def build_c6_selection_plan(
    *,
    session_date: date,
    desired_revision_id: UUID,
    desired_snapshot_sha256: str,
    selection_policy: AdvancedOptionsSelectionC6Policy,
    members: Iterable[C6SelectionMember],
    readiness: Iterable[C6UnderlyingReadiness],
    capacity: C6ConnectionCapacity,
    strategy_policies: Iterable[StrategyPolicyIdentity],
    catalog_identity: str,
    reference_identity: str,
    source_as_of: datetime,
    admission_profile_version: str | None = None,
    admission_profile_sha256: str | None = None,
    allocation_document: Mapping[str, object] | None = None,
    allocation_coverage: Iterable[C6AllocationCoverage] = (),
    physical_owner_id: UUID | None = None,
    journal_generation: UUID | None = None,
    future_requirements: Iterable[ResolvedFutureRequirement] = (),
    previous_shards: Iterable[C6SubscriptionShard] = (),
) -> C6SelectionPlan:
    selected = tuple(sorted(
        members,
        key=lambda row: row.contract_ticker,
    ))
    ready = tuple(sorted(
        readiness,
        key=lambda row: row.underlying,
    ))
    strategy_values = tuple(sorted(
        strategy_policies,
        key=lambda row: (row.version, row.content_sha256),
    ))
    future_values = tuple(sorted(
        future_requirements,
        key=lambda row: row.detector_id,
    ))
    physical, shards, capacity_decision = build_c6_subscription_shards(
        selected,
        session_date=session_date,
        capacity=capacity,
        previous_shards=previous_shards,
        physical_owner_id=physical_owner_id,
        journal_generation=journal_generation,
    )
    profile_version = (
        "c6_exact_four_regression_v1"
        if admission_profile_version is None
        else _identity_text(
            admission_profile_version,
            "admission profile version",
        )
    )
    profile_sha256 = (
        canonical_sha256({
            "version": profile_version,
            "selection_policy_sha256": selection_policy.policy_sha256,
        })
        if admission_profile_sha256 is None
        else _digest(
            admission_profile_sha256,
            "admission_profile_sha256",
        )
    )
    if allocation_document is None:
        allocation_unsigned = {
            "schema": profile_version,
            "profile_sha256": profile_sha256,
            "selected_contract_tickers": [
                row.contract_ticker for row in selected
            ],
            "reason": "EXACT_REGRESSION_OR_PRESELECTED_COMPATIBILITY",
        }
        allocation_digest = canonical_sha256(allocation_unsigned)
        allocation_value = {
            **allocation_unsigned,
            "allocation_sha256": allocation_digest,
        }
    else:
        allocation_value = dict(allocation_document)
        allocation_digest = _digest(
            allocation_value.get("allocation_sha256"),
            "allocation_sha256",
        )
    coverage_values = tuple(sorted(
        allocation_coverage,
        key=lambda row: (row.underlying, row.role),
    ))
    if not coverage_values:
        coverage_values = tuple(
            C6AllocationCoverage(
                underlying=underlying,
                role="CONFIGURED_OPENING_ADMISSION",
                state="FULL",
                planned_group_count=sum(
                    row.underlying == underlying for row in selected
                ),
                admitted_group_count=sum(
                    row.underlying == underlying for row in selected
                ),
                planned_contract_count=sum(
                    row.underlying == underlying for row in selected
                ),
                admitted_contract_count=sum(
                    row.underlying == underlying for row in selected
                ),
                ready_contract_count=sum(
                    row.underlying == underlying for row in selected
                ),
                missing_oi_contract_count=0,
                reason_codes=(),
                rejected_group_samples=(),
            )
            for underlying in sorted({
                row.underlying for row in selected
            })
        )
    values = {
        "session_date": session_date,
        "desired_revision_id": desired_revision_id,
        "desired_snapshot_sha256": desired_snapshot_sha256,
        "selection_policy_version": selection_policy.version,
        "selection_policy_sha256": selection_policy.policy_sha256,
        "flow_policy_sha256": selection_policy.flow_policy_sha256,
        "admission_profile_version": profile_version,
        "admission_profile_sha256": profile_sha256,
        "allocation_sha256": allocation_digest,
        "allocation_document": allocation_value,
        "strategy_policies": strategy_values,
        "catalog_identity": _identity_text(
            catalog_identity,
            "catalog identity",
        ),
        "reference_identity": _identity_text(
            reference_identity,
            "reference identity",
        ),
        "source_as_of": source_as_of,
        "broad_underlyings": tuple(sorted({
            row.underlying for row in selected
        })),
        "flow_underlyings": tuple(sorted({
            row.underlying for row in selected if row.flow_eligible
        })),
        "members": selected,
        "readiness": ready,
        "coverage": coverage_values,
        "physical_subscription": physical,
        "shards": shards,
        "capacity": capacity_decision,
        "future_requirements": future_values,
    }
    unsigned = C6SelectionPlan.__new__(C6SelectionPlan)
    object.__setattr__(unsigned, "generation_id", UUID(int=1))
    object.__setattr__(unsigned, "generation_sha256", "0" * 64)
    for name, value in values.items():
        object.__setattr__(unsigned, name, value)
    digest = canonical_sha256(
        c6_selection_plan_payload(unsigned, include_identity=False)
    )
    return C6SelectionPlan(
        generation_id=uuid5(
            NAMESPACE_URL,
            f"stockscreener:c6-options-selection:{digest}",
        ),
        generation_sha256=digest,
        **values,
    )


def c6_selection_plan_from_payload(
    value: object,
) -> C6SelectionPlan:
    if not isinstance(value, Mapping):
        raise TypeError("C6 selection plan must be an object")
    selection_policy = value["selection_policy"]
    admission_profile = value["admission_profile"]
    capacity_value = value["capacity"]
    if (
        value.get("schema")
        != ADVANCED_OPTIONS_SELECTION_GENERATION_VERSION
        or not isinstance(selection_policy, Mapping)
        or not isinstance(admission_profile, Mapping)
        or not isinstance(capacity_value, Mapping)
    ):
        raise ValueError("C6 selection plan contract changed")
    future = tuple(
        ResolvedFutureRequirement(
            detector_id=str(row["detector_id"]),
            declaration_sha256=str(row["declaration_sha256"]),
            state=FutureRequirementState(str(row["state"])),
            feeds=tuple(str(item) for item in row["feeds"]),
            missing_inputs=tuple(
                str(item) for item in row["missing_inputs"]
            ),
            capture_enabled=row["capture_enabled"] is True,
            detector_enabled=row["detector_enabled"] is True,
        )
        for row in value["future_requirements"]
    )
    plan = C6SelectionPlan(
        generation_id=UUID(str(value["generation_id"])),
        generation_sha256=str(value["generation_sha256"]),
        session_date=date.fromisoformat(str(value["session_date"])),
        desired_revision_id=UUID(str(value["desired_revision_id"])),
        desired_snapshot_sha256=str(
            value["desired_snapshot_sha256"]
        ),
        selection_policy_version=str(selection_policy["version"]),
        selection_policy_sha256=str(selection_policy["sha256"]),
        flow_policy_sha256=str(value["flow_policy_sha256"]),
        admission_profile_version=str(
            admission_profile["version"]
        ),
        admission_profile_sha256=str(
            admission_profile["sha256"]
        ),
        allocation_sha256=str(value["allocation_sha256"]),
        allocation_document=dict(value["allocation_document"]),
        strategy_policies=tuple(
            StrategyPolicyIdentity(
                str(row["version"]),
                str(row["content_sha256"]),
            )
            for row in value["strategy_policies"]
        ),
        catalog_identity=str(value["catalog_identity"]),
        reference_identity=str(value["reference_identity"]),
        source_as_of=datetime.fromisoformat(str(value["source_as_of"])),
        broad_underlyings=tuple(
            str(item) for item in value["broad_underlyings"]
        ),
        flow_underlyings=tuple(
            str(item) for item in value["flow_underlyings"]
        ),
        members=tuple(
            c6_selection_member_from_payload(row)
            for row in value["members"]
        ),
        readiness=tuple(
            c6_underlying_readiness_from_payload(row)
            for row in value["readiness"]
        ),
        coverage=tuple(
            c6_allocation_coverage_from_payload(row)
            for row in value["coverage"]
        ),
        physical_subscription=(
            c6_physical_subscription_from_payload(
                value["physical_subscription"]
            )
        ),
        shards=tuple(
            c6_subscription_shard_from_payload(row)
            for row in value["shards"]
        ),
        capacity=C6CapacityDecision(
            account_alias=str(capacity_value["account_alias"]),
            provider_cluster=str(
                capacity_value["provider_cluster"]
            ),
            effective_cluster_connection_limit=int(
                capacity_value[
                    "effective_cluster_connection_limit"
                ]
            ),
            active_cluster_connections=int(
                capacity_value["active_cluster_connections"]
            ),
            proposed_physical_connections=int(
                capacity_value["proposed_physical_connections"]
            ),
            required_cluster_connections=int(
                capacity_value["required_cluster_connections"]
            ),
            quote_contract_ceiling=int(
                capacity_value["quote_contract_ceiling"]
            ),
            active_quote_contract_budget=int(
                capacity_value["active_quote_contract_budget"]
            ),
            quote_slot_reserve=int(
                capacity_value["quote_slot_reserve"]
            ),
            selected_quote_contracts=int(
                capacity_value["selected_quote_contracts"]
            ),
            logical_shard_count=int(
                capacity_value["logical_shard_count"]
            ),
            accepted=capacity_value["accepted"] is True,
            reason_codes=tuple(
                str(item) for item in capacity_value["reason_codes"]
            ),
        ),
        future_requirements=future,
    )
    if c6_selection_plan_payload(plan) != dict(value):
        raise ValueError("C6 selection plan payload changed")
    return plan


@dataclass(frozen=True, slots=True)
class C6JournalRecord:
    source_kind: str
    source_schema: str
    instrument_key: str
    raw_timestamp_value: int
    raw_timestamp_unit: SourceTimestampUnit
    received_at: datetime
    source_sequence: str | None = None
    trade_id: str | None = None
    conditions: tuple[str, ...] = ()
    exchange: str | None = None
    correction_code: str | None = None
    cancellation: bool = False
    payload: Mapping[str, object] = field(
        default_factory=lambda: MappingProxyType({})
    )

    def __post_init__(self) -> None:
        received = _utc(self.received_at, "journal received_at")
        conditions = tuple(
            str(value) for value in self.conditions
        )
        if (
            type(self.raw_timestamp_value) is not int
            or self.raw_timestamp_value < 0
            or not isinstance(
                self.raw_timestamp_unit,
                SourceTimestampUnit,
            )
            or len(conditions) != len(set(conditions))
            or type(self.cancellation) is not bool
            or not isinstance(self.payload, Mapping)
        ):
            raise ValueError("C6 journal record is invalid")
        object.__setattr__(
            self,
            "source_kind",
            _text(self.source_kind, "source kind"),
        )
        object.__setattr__(
            self,
            "source_schema",
            _identity_text(self.source_schema, "source schema"),
        )
        object.__setattr__(
            self,
            "instrument_key",
            _identity_text(self.instrument_key, "instrument key"),
        )
        object.__setattr__(self, "received_at", received)
        object.__setattr__(self, "conditions", conditions)
        object.__setattr__(
            self,
            "payload",
            MappingProxyType(dict(self.payload)),
        )

    @property
    def event_time_nanoseconds(self) -> int:
        return (
            self.raw_timestamp_value
            * self.raw_timestamp_unit.nanoseconds
        )


def c6_journal_record_payload(
    record: C6JournalRecord,
) -> dict[str, object]:
    return {
        "schema": ADVANCED_OPTIONS_JOURNAL_RECORD_VERSION,
        "source_kind": record.source_kind,
        "source_schema": record.source_schema,
        "instrument_key": record.instrument_key,
        "raw_timestamp_value": record.raw_timestamp_value,
        "raw_timestamp_unit": record.raw_timestamp_unit.value,
        "received_at": record.received_at.isoformat(),
        "source_sequence": record.source_sequence,
        "trade_id": record.trade_id,
        "conditions": list(record.conditions),
        "exchange": record.exchange,
        "correction_code": record.correction_code,
        "cancellation": record.cancellation,
        "payload": dict(record.payload),
    }


def c6_journal_record_from_payload(
    value: object,
) -> C6JournalRecord:
    if (
        not isinstance(value, Mapping)
        or value.get("schema") != ADVANCED_OPTIONS_JOURNAL_RECORD_VERSION
    ):
        raise ValueError("C6 journal record contract changed")
    record = C6JournalRecord(
        source_kind=str(value["source_kind"]),
        source_schema=str(value["source_schema"]),
        instrument_key=str(value["instrument_key"]),
        raw_timestamp_value=int(value["raw_timestamp_value"]),
        raw_timestamp_unit=SourceTimestampUnit(
            str(value["raw_timestamp_unit"])
        ),
        received_at=datetime.fromisoformat(str(value["received_at"])),
        source_sequence=(
            None
            if value.get("source_sequence") is None
            else str(value["source_sequence"])
        ),
        trade_id=(
            None
            if value.get("trade_id") is None
            else str(value["trade_id"])
        ),
        conditions=tuple(
            str(item) for item in value["conditions"]
        ),
        exchange=(
            None
            if value.get("exchange") is None
            else str(value["exchange"])
        ),
        correction_code=(
            None
            if value.get("correction_code") is None
            else str(value["correction_code"])
        ),
        cancellation=value["cancellation"] is True,
        payload=dict(value["payload"]),
    )
    if c6_journal_record_payload(record) != dict(value):
        raise ValueError("C6 journal record payload changed")
    return record


def c6_event_span_milliseconds(
    records: Iterable[C6JournalRecord],
) -> Decimal:
    values = tuple(
        row.event_time_nanoseconds for row in records
    )
    if not values:
        raise ValueError("event span requires records")
    return Decimal(max(values) - min(values)) / Decimal(1_000_000)


@dataclass(frozen=True, slots=True)
class C6ConsumerState:
    consumer_name: str
    state_version: str
    committed_offset: int
    event_watermark_ns: int | None
    history_start: datetime | None
    history_end: datetime | None
    missing_feed_states: tuple[str, ...]
    updated_at: datetime

    def __post_init__(self) -> None:
        updated = _utc(self.updated_at, "consumer updated_at")
        start = (
            None
            if self.history_start is None
            else _utc(self.history_start, "history_start")
        )
        end = (
            None
            if self.history_end is None
            else _utc(self.history_end, "history_end")
        )
        missing = tuple(sorted({
            _text(value, "missing feed state")
            for value in self.missing_feed_states
        }))
        if (
            type(self.committed_offset) is not int
            or self.committed_offset < 0
            or self.event_watermark_ns is not None
            and (
                type(self.event_watermark_ns) is not int
                or self.event_watermark_ns < 0
            )
            or (start is None) != (end is None)
            or start is not None and start > end
        ):
            raise ValueError("C6 consumer state is invalid")
        object.__setattr__(
            self,
            "consumer_name",
            _identity_text(self.consumer_name, "consumer name"),
        )
        object.__setattr__(
            self,
            "state_version",
            _identity_text(
                self.state_version,
                "consumer state version",
            ),
        )
        object.__setattr__(self, "history_start", start)
        object.__setattr__(self, "history_end", end)
        object.__setattr__(self, "missing_feed_states", missing)
        object.__setattr__(self, "updated_at", updated)


def c6_consumer_state_payload(
    state: C6ConsumerState,
) -> dict[str, object]:
    return {
        "schema": ADVANCED_OPTIONS_CONSUMER_STATE_VERSION,
        "consumer_name": state.consumer_name,
        "state_version": state.state_version,
        "committed_offset": state.committed_offset,
        "event_watermark_ns": state.event_watermark_ns,
        "history_start": (
            None
            if state.history_start is None
            else state.history_start.isoformat()
        ),
        "history_end": (
            None
            if state.history_end is None
            else state.history_end.isoformat()
        ),
        "missing_feed_states": list(state.missing_feed_states),
        "updated_at": state.updated_at.isoformat(),
    }


def persist_c6_selection_cursor(
    cursor,
    *,
    session_binding_id: UUID,
    plan: C6SelectionPlan,
) -> None:
    if (
        not isinstance(session_binding_id, UUID)
        or not session_binding_id.int
        or not isinstance(plan, C6SelectionPlan)
    ):
        raise TypeError("C6 selection persistence inputs are invalid")
    document = c6_selection_plan_payload(plan)
    cursor.execute(
        "INSERT INTO rt.advanced_options_selection_generations ("
        "selection_generation_id,selection_generation_sha256,"
        "session_binding_id,session_date,desired_revision_id,"
        "desired_snapshot_sha256,selection_policy_version,"
        "selection_policy_sha256,flow_policy_sha256,"
        "admission_profile_version,admission_profile_sha256,"
        "allocation_sha256,"
        "broad_underlyings,flow_underlyings,capacity_document,"
        "allocation_document,selection_document"
        ") VALUES ("
        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
        ") "
        "ON CONFLICT (session_binding_id) DO NOTHING",
        (
            str(plan.generation_id),
            plan.generation_sha256,
            str(session_binding_id),
            plan.session_date,
            str(plan.desired_revision_id),
            plan.desired_snapshot_sha256,
            plan.selection_policy_version,
            plan.selection_policy_sha256,
            plan.flow_policy_sha256,
            plan.admission_profile_version,
            plan.admission_profile_sha256,
            plan.allocation_sha256,
            list(plan.broad_underlyings),
            list(plan.flow_underlyings),
            Json(_capacity_payload(plan.capacity)),
            Json(dict(plan.allocation_document)),
            Json(document),
        ),
    )
    cursor.execute(
        "SELECT selection_generation_id::text,"
        "selection_generation_sha256,selection_document "
        "FROM rt.advanced_options_selection_generations "
        "WHERE session_binding_id=%s",
        (str(session_binding_id),),
    )
    stored = cursor.fetchone()
    if (
        stored is None
        or str(stored[0]) != str(plan.generation_id)
        or str(stored[1]) != plan.generation_sha256
        or dict(stored[2]) != document
    ):
        raise ValueError("C6 persisted selection replay diverged")
    for member in plan.members:
        cursor.execute(
            "INSERT INTO rt.advanced_options_selection_members ("
            "selection_generation_id,contract_id,contract_ticker,"
            "underlying,flow_eligible,member_sha256,member_document"
            ") VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (
                str(plan.generation_id),
                member.contract_id,
                member.contract_ticker,
                member.underlying,
                member.flow_eligible,
                member.member_sha256,
                Json(c6_selection_member_payload(member)),
            ),
        )
    for coverage in plan.coverage:
        coverage_document = c6_allocation_coverage_payload(coverage)
        cursor.execute(
            "INSERT INTO rt.advanced_options_selection_coverage ("
            "selection_generation_id,underlying,role,state,"
            "planned_group_count,admitted_group_count,"
            "planned_contract_count,admitted_contract_count,"
            "ready_contract_count,missing_oi_contract_count,"
            "reason_codes,rejected_group_samples,coverage_document"
            ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT DO NOTHING",
            (
                str(plan.generation_id),
                coverage.underlying,
                coverage.role,
                coverage.state,
                coverage.planned_group_count,
                coverage.admitted_group_count,
                coverage.planned_contract_count,
                coverage.admitted_contract_count,
                coverage.ready_contract_count,
                coverage.missing_oi_contract_count,
                list(coverage.reason_codes),
                list(coverage.rejected_group_samples),
                Json(coverage_document),
            ),
        )
    physical = plan.physical_subscription
    cursor.execute(
        "INSERT INTO rt.advanced_options_physical_subscriptions ("
        "selection_generation_id,subscription_id,account_alias,"
        "provider_cluster,owner_id,journal_generation,member_sha256,"
        "contract_tickers,subscription_document"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT DO NOTHING",
        (
            str(plan.generation_id),
            str(physical.subscription_id),
            physical.account_alias,
            physical.provider_cluster,
            str(physical.owner_id),
            str(physical.journal_generation),
            physical.member_sha256,
            list(physical.contract_tickers),
            Json(c6_physical_subscription_payload(physical)),
        ),
    )
    cursor.execute(
        "INSERT INTO rt.advanced_options_physical_subscription_states ("
        "selection_generation_id,subscription_id,state,message_count,"
        "byte_count,reconnect_count,gap_document,reason_codes,"
        "observed_at"
        ") VALUES (%s,%s,'PLANNED',0,0,0,'[]'::jsonb,'{}',%s) "
        "ON CONFLICT DO NOTHING",
        (
            str(plan.generation_id),
            str(physical.subscription_id),
            plan.source_as_of,
        ),
    )
    for shard in plan.shards:
        cursor.execute(
            "INSERT INTO rt.advanced_options_subscription_shards ("
            "selection_generation_id,shard_id,shard_slot,"
            "physical_subscription_id,member_sha256,"
            "contract_tickers,shard_document"
            ") VALUES (%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT DO NOTHING",
            (
                str(plan.generation_id),
                str(shard.shard_id),
                shard.shard_slot,
                str(shard.physical_subscription_id),
                shard.member_sha256,
                list(shard.contract_tickers),
                Json(c6_subscription_shard_payload(shard)),
            ),
        )
        cursor.execute(
            "INSERT INTO rt.advanced_options_shard_states ("
            "selection_generation_id,shard_id,state,message_count,"
            "byte_count,reason_codes,observed_at"
            ") VALUES (%s,%s,'PLANNED',0,0,'{}',%s) "
            "ON CONFLICT DO NOTHING",
            (
                str(plan.generation_id),
                str(shard.shard_id),
                plan.source_as_of,
            ),
        )
    for readiness in plan.readiness:
        persist_c6_underlying_readiness_cursor(
            cursor,
            selection_generation_id=plan.generation_id,
            readiness=readiness,
        )


def load_c6_selection_cursor(
    cursor,
    *,
    session_binding_id: UUID,
) -> C6SelectionPlan | None:
    cursor.execute(
        "SELECT selection_document "
        "FROM rt.advanced_options_selection_generations "
        "WHERE session_binding_id=%s",
        (str(session_binding_id),),
    )
    row = cursor.fetchone()
    return (
        None
        if row is None
        else c6_selection_plan_from_payload(row[0])
    )


def persist_c6_underlying_readiness_cursor(
    cursor,
    *,
    selection_generation_id: UUID,
    readiness: C6UnderlyingReadiness,
) -> None:
    document = c6_underlying_readiness_payload(readiness)
    cursor.execute(
        "INSERT INTO rt.advanced_options_underlying_readiness ("
        "selection_generation_id,underlying,catalog_state,"
        "open_interest_state,history_state,ready,reason_codes,"
        "readiness_document,observed_at"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (selection_generation_id,underlying) DO UPDATE SET "
        "catalog_state=EXCLUDED.catalog_state,"
        "open_interest_state=EXCLUDED.open_interest_state,"
        "history_state=EXCLUDED.history_state,ready=EXCLUDED.ready,"
        "reason_codes=EXCLUDED.reason_codes,"
        "readiness_document=EXCLUDED.readiness_document,"
        "observed_at=EXCLUDED.observed_at "
        "WHERE rt.advanced_options_underlying_readiness.observed_at"
        "<=EXCLUDED.observed_at",
        (
            str(selection_generation_id),
            readiness.underlying,
            readiness.catalog.value,
            readiness.open_interest.value,
            readiness.history.value,
            readiness.ready,
            list(readiness.reason_codes),
            Json(document),
            readiness.observed_at,
        ),
    )


def persist_c6_physical_subscription_state_cursor(
    cursor,
    *,
    selection_generation_id: UUID,
    subscription_id: UUID,
    state: C6ShardState,
    message_count: int,
    byte_count: int,
    reconnect_count: int,
    gaps: Iterable[Mapping[str, object]],
    reason_codes: Iterable[str],
    observed_at: datetime,
) -> None:
    if (
        any(
            type(value) is not int or value < 0
            for value in (
                message_count,
                byte_count,
                reconnect_count,
            )
        )
        or not isinstance(state, C6ShardState)
    ):
        raise ValueError("C6 physical subscription counters are invalid")
    checked = _utc(observed_at, "physical subscription observed_at")
    gap_values = [dict(value) for value in gaps]
    reasons = sorted({
        _text(value, "shard reason")
        for value in reason_codes
    })
    cursor.execute(
        "INSERT INTO rt.advanced_options_physical_subscription_states ("
        "selection_generation_id,subscription_id,state,message_count,"
        "byte_count,reconnect_count,gap_document,reason_codes,"
        "observed_at"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (selection_generation_id,subscription_id) "
        "DO UPDATE SET "
        "state=EXCLUDED.state,message_count=EXCLUDED.message_count,"
        "byte_count=EXCLUDED.byte_count,"
        "reconnect_count=EXCLUDED.reconnect_count,"
        "gap_document=EXCLUDED.gap_document,"
        "reason_codes=EXCLUDED.reason_codes,"
        "observed_at=EXCLUDED.observed_at "
        "WHERE rt.advanced_options_physical_subscription_states.observed_at"
        "<=EXCLUDED.observed_at "
        "AND rt.advanced_options_physical_subscription_states.message_count"
        "<=EXCLUDED.message_count "
        "AND rt.advanced_options_physical_subscription_states.byte_count"
        "<=EXCLUDED.byte_count "
        "AND rt.advanced_options_physical_subscription_states.reconnect_count"
        "<=EXCLUDED.reconnect_count",
        (
            str(selection_generation_id),
            str(subscription_id),
            state.value,
            message_count,
            byte_count,
            reconnect_count,
            Json(gap_values),
            reasons,
            checked,
        ),
    )


def persist_c6_shard_state_cursor(
    cursor,
    *,
    selection_generation_id: UUID,
    shard_id: UUID,
    state: C6ShardState,
    message_count: int,
    byte_count: int,
    reason_codes: Iterable[str],
    observed_at: datetime,
) -> None:
    if (
        any(
            type(value) is not int or value < 0
            for value in (message_count, byte_count)
        )
        or not isinstance(state, C6ShardState)
    ):
        raise ValueError("C6 logical shard counters are invalid")
    checked = _utc(observed_at, "logical shard observed_at")
    reasons = sorted({
        _text(value, "logical shard reason")
        for value in reason_codes
    })
    cursor.execute(
        "INSERT INTO rt.advanced_options_shard_states ("
        "selection_generation_id,shard_id,state,message_count,"
        "byte_count,reason_codes,observed_at"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (selection_generation_id,shard_id) DO UPDATE SET "
        "state=EXCLUDED.state,message_count=EXCLUDED.message_count,"
        "byte_count=EXCLUDED.byte_count,"
        "reason_codes=EXCLUDED.reason_codes,"
        "observed_at=EXCLUDED.observed_at "
        "WHERE rt.advanced_options_shard_states.observed_at"
        "<=EXCLUDED.observed_at "
        "AND rt.advanced_options_shard_states.message_count"
        "<=EXCLUDED.message_count "
        "AND rt.advanced_options_shard_states.byte_count"
        "<=EXCLUDED.byte_count",
        (
            str(selection_generation_id),
            str(shard_id),
            state.value,
            message_count,
            byte_count,
            reasons,
            checked,
        ),
    )


def persist_c6_consumer_state_cursor(
    cursor,
    *,
    selection_generation_id: UUID,
    shard_id: UUID,
    state: C6ConsumerState,
) -> None:
    document = c6_consumer_state_payload(state)
    cursor.execute(
        "INSERT INTO rt.advanced_options_consumer_states ("
        "selection_generation_id,shard_id,consumer_name,state_version,"
        "committed_offset,event_watermark_ns,state_document,updated_at"
        ") VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (selection_generation_id,shard_id,consumer_name) "
        "DO UPDATE SET "
        "state_version=EXCLUDED.state_version,"
        "committed_offset=EXCLUDED.committed_offset,"
        "event_watermark_ns=EXCLUDED.event_watermark_ns,"
        "state_document=EXCLUDED.state_document,"
        "updated_at=EXCLUDED.updated_at "
        "WHERE rt.advanced_options_consumer_states.committed_offset"
        "<=EXCLUDED.committed_offset "
        "AND (rt.advanced_options_consumer_states.event_watermark_ns "
        "IS NULL OR EXCLUDED.event_watermark_ns IS NULL OR "
        "rt.advanced_options_consumer_states.event_watermark_ns"
        "<=EXCLUDED.event_watermark_ns)",
        (
            str(selection_generation_id),
            str(shard_id),
            state.consumer_name,
            state.state_version,
            state.committed_offset,
            state.event_watermark_ns,
            Json(document),
            state.updated_at,
        ),
    )


def c6_selection_health_cursor(
    cursor,
    *,
    session_binding_id: UUID,
) -> dict[str, object] | None:
    cursor.execute(
        "SELECT selection_generation_id::text,"
        "selection_generation_sha256,broad_underlyings,"
        "flow_underlyings,capacity_document,"
        "admission_profile_version,admission_profile_sha256,"
        "allocation_sha256 "
        "FROM rt.advanced_options_selection_generations "
        "WHERE session_binding_id=%s",
        (str(session_binding_id),),
    )
    selection = cursor.fetchone()
    if selection is None:
        return None
    generation_id = str(selection[0])
    cursor.execute(
        "SELECT underlying,ready,readiness_document "
        "FROM rt.advanced_options_underlying_readiness "
        "WHERE selection_generation_id=%s ORDER BY underlying",
        (generation_id,),
    )
    underlyings = [
        {
            "underlying": str(underlying),
            "ready": ready is True,
            "readiness": dict(document),
        }
        for underlying, ready, document in cursor.fetchall()
    ]
    cursor.execute(
        "SELECT coverage_document "
        "FROM rt.advanced_options_selection_coverage "
        "WHERE selection_generation_id=%s "
        "ORDER BY underlying,role",
        (generation_id,),
    )
    coverage = [
        dict(row[0]) for row in cursor.fetchall()
    ]
    cursor.execute(
        "SELECT subscription.subscription_id::text,"
        "subscription.owner_id::text,"
        "subscription.journal_generation::text,"
        "subscription.contract_tickers,state.state,"
        "state.message_count,state.byte_count,state.reconnect_count,"
        "state.gap_document,state.reason_codes,state.observed_at "
        "FROM rt.advanced_options_physical_subscriptions "
        "AS subscription "
        "LEFT JOIN rt.advanced_options_physical_subscription_states "
        "AS state ON state.selection_generation_id="
        "subscription.selection_generation_id "
        "AND state.subscription_id=subscription.subscription_id "
        "WHERE subscription.selection_generation_id=%s",
        (generation_id,),
    )
    physical_row = cursor.fetchone()
    physical = (
        None
        if physical_row is None
        else {
            "subscription_id": str(physical_row[0]),
            "owner_id": str(physical_row[1]),
            "journal_generation": str(physical_row[2]),
            "contract_tickers": list(physical_row[3]),
            "state": (
                None
                if physical_row[4] is None
                else str(physical_row[4])
            ),
            "message_count": (
                None
                if physical_row[5] is None
                else int(physical_row[5])
            ),
            "byte_count": (
                None
                if physical_row[6] is None
                else int(physical_row[6])
            ),
            "reconnect_count": (
                None
                if physical_row[7] is None
                else int(physical_row[7])
            ),
            "gaps": (
                []
                if physical_row[8] is None
                else list(physical_row[8])
            ),
            "reason_codes": (
                []
                if physical_row[9] is None
                else list(physical_row[9])
            ),
            "observed_at": (
                None
                if physical_row[10] is None
                else physical_row[10].isoformat()
            ),
        }
    )
    cursor.execute(
        "SELECT shard.shard_id::text,shard.shard_slot,"
        "shard.physical_subscription_id::text,"
        "shard.contract_tickers,state.state,state.message_count,"
        "state.byte_count,state.reason_codes,state.observed_at "
        "FROM rt.advanced_options_subscription_shards AS shard "
        "LEFT JOIN rt.advanced_options_shard_states AS state "
        "ON state.selection_generation_id="
        "shard.selection_generation_id "
        "AND state.shard_id=shard.shard_id "
        "WHERE shard.selection_generation_id=%s "
        "ORDER BY shard.shard_slot",
        (generation_id,),
    )
    shards = [
        {
            "shard_id": str(row[0]),
            "shard_slot": int(row[1]),
            "physical_subscription_id": str(row[2]),
            "contract_tickers": list(row[3]),
            "state": None if row[4] is None else str(row[4]),
            "message_count": (
                None if row[5] is None else int(row[5])
            ),
            "byte_count": None if row[6] is None else int(row[6]),
            "reason_codes": (
                [] if row[7] is None else list(row[7])
            ),
            "observed_at": (
                None if row[8] is None else row[8].isoformat()
            ),
        }
        for row in cursor.fetchall()
    ]
    return {
        "selection_generation_id": generation_id,
        "selection_generation_sha256": str(selection[1]),
        "broad_underlyings": list(selection[2]),
        "flow_underlyings": list(selection[3]),
        "capacity": dict(selection[4]),
        "admission_profile_version": str(selection[5]),
        "admission_profile_sha256": str(selection[6]),
        "allocation_sha256": str(selection[7]),
        "underlyings": underlyings,
        "coverage": coverage,
        "ready_underlying_count": sum(
            row["ready"] for row in underlyings
        ),
        "physical_subscription": physical,
        "shards": shards,
        "physical_failed": (
            physical is not None
            and physical["state"] == C6ShardState.FAILED.value
        ),
        "failed_shard_count": sum(
            row["state"] == C6ShardState.FAILED.value
            for row in shards
        ),
    }
