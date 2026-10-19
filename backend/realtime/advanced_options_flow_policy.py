"""Immutable multi-profile retention policy for Advanced Options flow."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
import re
from types import MappingProxyType
from typing import Mapping

from .contracts import canonical_sha256


DEFAULT_ADVANCED_OPTIONS_FLOW_POLICY_PATH = (
    Path(__file__).resolve().parent
    / "policies"
    / "advanced_options_flow_v1.json"
)
EXPECTED_UNDERLYINGS = frozenset({
    "AAPL",
    "AMZN",
    "IWM",
    "MSFT",
    "NFLX",
    "NVDA",
    "QQQ",
    "SPY",
    "TSLA",
})


class FlowUniverseTier(str, Enum):
    ETF = "ETF"
    CORE_MEGA_CAP = "CORE_MEGA_CAP"
    HIGH_BETA_HIGH_PREMIUM = "HIGH_BETA_HIGH_PREMIUM"


class FlowDteLane(str, Enum):
    ULTRA_SHORT = "ULTRA_SHORT"
    CORE_SWING = "CORE_SWING"
    POSITION = "POSITION"


class FlowProfileKind(str, Enum):
    ULTRA_SHORT = "ULTRA_SHORT"
    BLOCK = "BLOCK"
    SWEEP = "SWEEP"
    ACCUMULATION = "ACCUMULATION"


class FlowThresholdMode(str, Enum):
    OR = "OR"


_PROFILE_DTE_LANES = {
    FlowProfileKind.ULTRA_SHORT: frozenset({FlowDteLane.ULTRA_SHORT}),
    FlowProfileKind.BLOCK: frozenset({
        FlowDteLane.CORE_SWING,
        FlowDteLane.POSITION,
    }),
    FlowProfileKind.SWEEP: frozenset({FlowDteLane.CORE_SWING}),
    FlowProfileKind.ACCUMULATION: frozenset({
        FlowDteLane.CORE_SWING,
        FlowDteLane.POSITION,
    }),
}
_CUMULATIVE_CONTRACT_PROFILES = frozenset({
    FlowProfileKind.ULTRA_SHORT,
    FlowProfileKind.ACCUMULATION,
})
_OPTION_CONTRACT_PATTERN = re.compile(
    r"^O:([A-Z]{1,6})(\d{6})[CP]\d{8}$"
)


@dataclass(frozen=True, slots=True)
class FlowDteBand:
    lane: FlowDteLane
    minimum_dte: int
    maximum_dte: int

    def __post_init__(self) -> None:
        if (
            type(self.minimum_dte) is not int
            or type(self.maximum_dte) is not int
            or self.minimum_dte < 0
            or self.maximum_dte < self.minimum_dte
            or self.maximum_dte > 60
        ):
            raise ValueError("flow DTE band is invalid")


@dataclass(frozen=True, slots=True)
class FlowExcludedDteBand:
    minimum_dte: int
    maximum_dte: int
    reason_code: str

    def __post_init__(self) -> None:
        reason = self.reason_code.strip().upper()
        if (
            type(self.minimum_dte) is not int
            or type(self.maximum_dte) is not int
            or self.minimum_dte < 0
            or self.maximum_dte < self.minimum_dte
            or self.maximum_dte > 60
            or not reason
        ):
            raise ValueError("excluded flow DTE band is invalid")
        object.__setattr__(self, "reason_code", reason)


@dataclass(frozen=True, slots=True)
class FlowUniverseMembership:
    tier: FlowUniverseTier
    underlyings: tuple[str, ...]

    def __post_init__(self) -> None:
        values = tuple(sorted(value.strip().upper() for value in self.underlyings))
        if not values or len(values) != len(set(values)) or any(not value for value in values):
            raise ValueError("flow universe membership is invalid")
        object.__setattr__(self, "underlyings", values)


@dataclass(frozen=True, slots=True)
class FlowTierThreshold:
    tier: FlowUniverseTier
    minimum_contracts: int
    minimum_print_premium: Decimal
    minimum_cluster_prints: int | None = None
    minimum_distinct_exchanges: int | None = None
    minimum_cluster_premium: Decimal | None = None
    minimum_cumulative_contracts: int | None = None

    def __post_init__(self) -> None:
        if (
            type(self.minimum_contracts) is not int
            or self.minimum_contracts <= 0
            or not isinstance(self.minimum_print_premium, Decimal)
            or not self.minimum_print_premium.is_finite()
            or self.minimum_print_premium <= 0
        ):
            raise ValueError("flow print threshold is invalid")
        cluster = (
            self.minimum_cluster_prints,
            self.minimum_distinct_exchanges,
            self.minimum_cluster_premium,
        )
        if any(value is not None for value in cluster):
            if (
                type(self.minimum_cluster_prints) is not int
                or self.minimum_cluster_prints <= 0
                or type(self.minimum_distinct_exchanges) is not int
                or self.minimum_distinct_exchanges <= 0
                or not isinstance(self.minimum_cluster_premium, Decimal)
                or not self.minimum_cluster_premium.is_finite()
                or self.minimum_cluster_premium <= 0
            ):
                raise ValueError("flow cluster threshold is incomplete")
        if (
            self.minimum_cumulative_contracts is not None
            and (
                not any(value is not None for value in cluster)
                or type(self.minimum_cumulative_contracts) is not int
                or self.minimum_cumulative_contracts <= 0
            )
        ):
            raise ValueError("flow cumulative contract threshold is invalid")

    @property
    def has_cluster_gate(self) -> bool:
        return self.minimum_cluster_prints is not None


@dataclass(frozen=True, slots=True)
class FlowRetentionProfile:
    kind: FlowProfileKind
    dte_lanes: tuple[FlowDteLane, ...]
    threshold_mode: FlowThresholdMode
    tier_thresholds: tuple[FlowTierThreshold, ...]
    window_seconds_by_lane: tuple[tuple[FlowDteLane, int], ...] = ()
    directional_side_classes: tuple[str, ...] = ()
    directional_confidences: tuple[str, ...] = ()
    retain_inside_spread_as_context: bool = True

    def __post_init__(self) -> None:
        lanes = tuple(sorted(self.dte_lanes, key=lambda value: value.value))
        if (
            not lanes
            or len(lanes) != len(set(lanes))
            or set(lanes) != _PROFILE_DTE_LANES[self.kind]
        ):
            raise ValueError("flow profile DTE lanes are invalid")
        tiers = tuple(row.tier for row in self.tier_thresholds)
        if (
            len(tiers) != len(set(tiers))
            or set(tiers) != set(FlowUniverseTier)
        ):
            raise ValueError("flow profile tier thresholds are incomplete")
        windows = dict(self.window_seconds_by_lane)
        if (
            len(windows) != len(self.window_seconds_by_lane)
            or any(
                lane not in lanes or type(seconds) is not int or seconds <= 0
                for lane, seconds in self.window_seconds_by_lane
            )
        ):
            raise ValueError("flow profile windows are invalid")
        clustered = self.kind is not FlowProfileKind.BLOCK
        if clustered != bool(windows):
            raise ValueError("flow profile cluster window scope is invalid")
        if any(row.has_cluster_gate != clustered for row in self.tier_thresholds):
            raise ValueError("flow profile cluster thresholds changed shape")
        cumulative_required = self.kind in _CUMULATIVE_CONTRACT_PROFILES
        if any(
            (row.minimum_cumulative_contracts is not None)
            is not cumulative_required
            for row in self.tier_thresholds
        ):
            raise ValueError(
                "flow profile cumulative contract threshold scope is invalid"
            )
        directional_sides = tuple(self.directional_side_classes)
        directional_confidences = tuple(self.directional_confidences)
        allowed_sides = {"AT_OR_ABOVE_ASK", "AT_OR_BELOW_BID"}
        allowed_confidences = {"HIGH", "MEDIUM"}
        if (
            not directional_sides
            or set(directional_sides) - allowed_sides
            or len(directional_sides) != len(set(directional_sides))
            or not directional_confidences
            or set(directional_confidences) - allowed_confidences
            or len(directional_confidences)
            != len(set(directional_confidences))
            or type(self.retain_inside_spread_as_context) is not bool
        ):
            raise ValueError("flow directional evidence policy is invalid")
        object.__setattr__(self, "dte_lanes", lanes)
        object.__setattr__(
            self,
            "tier_thresholds",
            tuple(sorted(self.tier_thresholds, key=lambda row: row.tier.value)),
        )
        object.__setattr__(
            self,
            "window_seconds_by_lane",
            tuple(sorted(windows.items(), key=lambda row: row[0].value)),
        )
        object.__setattr__(
            self,
            "directional_side_classes",
            tuple(sorted(directional_sides)),
        )
        object.__setattr__(
            self,
            "directional_confidences",
            tuple(sorted(directional_confidences)),
        )

    def threshold_for(self, tier: FlowUniverseTier) -> FlowTierThreshold:
        return next(row for row in self.tier_thresholds if row.tier is tier)

    def window_for(self, lane: FlowDteLane) -> int | None:
        return dict(self.window_seconds_by_lane).get(lane)


@dataclass(frozen=True, slots=True)
class FlowOpeningOverlay:
    minimum_prior_open_interest: int
    unusual_volume_oi_ratio: float
    high_conviction_volume_oi_ratio: float
    directional_side_classes: tuple[str, ...]
    directional_confidences: tuple[str, ...]
    retain_inside_spread_as_context: bool

    def __post_init__(self) -> None:
        sides = tuple(self.directional_side_classes)
        confidences = tuple(self.directional_confidences)
        if (
            type(self.minimum_prior_open_interest) is not int
            or self.minimum_prior_open_interest <= 0
            or not 1 < self.unusual_volume_oi_ratio
            < self.high_conviction_volume_oi_ratio
            or type(self.retain_inside_spread_as_context) is not bool
            or len(sides) != len(set(sides))
            or len(confidences) != len(set(confidences))
        ):
            raise ValueError("flow opening overlay thresholds are invalid")
        if set(sides) != {
            "AT_OR_ABOVE_ASK",
            "AT_OR_BELOW_BID",
        } or set(confidences) != {"HIGH", "MEDIUM"}:
            raise ValueError("flow opening overlay direction policy is invalid")
        object.__setattr__(
            self,
            "directional_side_classes",
            tuple(sorted(sides)),
        )
        object.__setattr__(
            self,
            "directional_confidences",
            tuple(sorted(confidences)),
        )


@dataclass(frozen=True, slots=True)
class FlowCandidateCaps:
    global_maximum: int
    maximum_per_underlying: int
    contract_profile_dedup_seconds: int
    profile_budgets: tuple[tuple[FlowProfileKind, int], ...]

    def __post_init__(self) -> None:
        budgets = dict(self.profile_budgets)
        if (
            type(self.global_maximum) is not int
            or self.global_maximum <= 0
            or type(self.maximum_per_underlying) is not int
            or self.maximum_per_underlying <= 0
            or type(self.contract_profile_dedup_seconds) is not int
            or self.contract_profile_dedup_seconds <= 0
            or len(budgets) != len(self.profile_budgets)
            or set(budgets) != set(FlowProfileKind)
            or any(type(value) is not int or value <= 0 for value in budgets.values())
            or sum(budgets.values()) != self.global_maximum
        ):
            raise ValueError("flow candidate caps are invalid")
        object.__setattr__(
            self,
            "profile_budgets",
            tuple(sorted(budgets.items(), key=lambda row: row[0].value)),
        )


@dataclass(frozen=True, slots=True)
class AdvancedOptionsFlowPolicy:
    version: str
    universe: tuple[FlowUniverseMembership, ...]
    dte_bands: tuple[FlowDteBand, ...]
    excluded_dte_bands: tuple[FlowExcludedDteBand, ...]
    profiles: tuple[FlowRetentionProfile, ...]
    opening_overlay: FlowOpeningOverlay
    candidate_caps: FlowCandidateCaps

    def __post_init__(self) -> None:
        version = self.version.strip()
        if version != "advanced-options-flow-v1":
            raise ValueError("Advanced Options flow policy version is invalid")
        tiers = tuple(row.tier for row in self.universe)
        underlyings = tuple(
            underlying
            for membership in self.universe
            for underlying in membership.underlyings
        )
        if (
            len(tiers) != len(set(tiers))
            or set(tiers) != set(FlowUniverseTier)
            or len(underlyings) != len(set(underlyings))
            or frozenset(underlyings) != EXPECTED_UNDERLYINGS
        ):
            raise ValueError("Advanced Options flow universe changed")
        lanes = tuple(row.lane for row in self.dte_bands)
        if len(lanes) != len(set(lanes)) or set(lanes) != set(FlowDteLane):
            raise ValueError("Advanced Options flow DTE lanes are incomplete")
        covered = set()
        for band in (*self.dte_bands, *self.excluded_dte_bands):
            values = set(range(band.minimum_dte, band.maximum_dte + 1))
            if covered & values:
                raise ValueError("Advanced Options flow DTE bands overlap")
            covered.update(values)
        if covered != set(range(61)):
            raise ValueError("Advanced Options flow DTE coverage must be exact for 0-60")
        kinds = tuple(row.kind for row in self.profiles)
        if len(kinds) != len(set(kinds)) or set(kinds) != set(FlowProfileKind):
            raise ValueError("Advanced Options flow profiles are incomplete")
        known_lanes = set(lanes)
        if any(
            set(profile.dte_lanes) - known_lanes for profile in self.profiles
        ):
            raise ValueError("Advanced Options flow profile names an unknown DTE lane")
        object.__setattr__(self, "version", version)
        object.__setattr__(
            self,
            "universe",
            tuple(sorted(self.universe, key=lambda row: row.tier.value)),
        )
        object.__setattr__(
            self,
            "dte_bands",
            tuple(sorted(self.dte_bands, key=lambda row: row.minimum_dte)),
        )
        object.__setattr__(
            self,
            "excluded_dte_bands",
            tuple(
                sorted(
                    self.excluded_dte_bands,
                    key=lambda row: row.minimum_dte,
                )
            ),
        )
        object.__setattr__(
            self,
            "profiles",
            tuple(sorted(self.profiles, key=lambda row: row.kind.value)),
        )

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)

    def tier_for(self, underlyer: str) -> FlowUniverseTier:
        normalized = underlyer.strip().upper()
        for membership in self.universe:
            if normalized in membership.underlyings:
                return membership.tier
        raise ValueError("underlyer is outside the Advanced Options flow universe")

    def lane_for(self, dte: int) -> FlowDteLane | None:
        if type(dte) is not int or not 0 <= dte <= 60:
            raise ValueError("DTE must be an integer in [0, 60]")
        for band in self.dte_bands:
            if band.minimum_dte <= dte <= band.maximum_dte:
                return band.lane
        return None

    def profile(self, kind: FlowProfileKind) -> FlowRetentionProfile:
        return next(row for row in self.profiles if row.kind is kind)


def _sha256(value: str, name: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise ValueError(f"{name} must be a SHA-256 digest")
    return value


@dataclass(frozen=True, slots=True)
class AdvancedOptionsContractScopeEntry:
    contract_ticker: str
    underlyer: str
    expiration: date
    universe_tier: FlowUniverseTier
    dte_lane: FlowDteLane

    def __post_init__(self) -> None:
        contract = self.contract_ticker.strip().upper()
        underlyer = self.underlyer.strip().upper()
        match = _OPTION_CONTRACT_PATTERN.fullmatch(contract)
        if match is None:
            raise ValueError("Advanced Options contract ticker is invalid")
        try:
            contract_expiration = datetime.strptime(
                match.group(2),
                "%y%m%d",
            ).date()
        except ValueError:
            raise ValueError(
                "Advanced Options contract expiration is invalid"
            ) from None
        if not underlyer or match.group(1) != underlyer:
            raise ValueError(
                "Advanced Options contract ownership is inconsistent"
            )
        if (
            not isinstance(self.expiration, date)
            or contract_expiration != self.expiration
        ):
            raise ValueError(
                "Advanced Options contract expiration is inconsistent"
            )
        object.__setattr__(self, "contract_ticker", contract)
        object.__setattr__(self, "underlyer", underlyer)


@dataclass(frozen=True, slots=True)
class AdvancedOptionsContractScope:
    version: str
    session_date: date
    manifest_sha256: str
    slot_sha256: str
    flow_policy_sha256: str
    contracts: tuple[AdvancedOptionsContractScopeEntry, ...]

    def __post_init__(self) -> None:
        version = self.version.strip()
        contracts = tuple(
            sorted(self.contracts, key=lambda row: row.contract_ticker)
        )
        tickers = tuple(row.contract_ticker for row in contracts)
        if (
            version != "advanced-options-contract-scope-v1"
            or not isinstance(self.session_date, date)
            or not contracts
            or len(tickers) != len(set(tickers))
        ):
            raise ValueError("Advanced Options contract scope is invalid")
        object.__setattr__(self, "version", version)
        object.__setattr__(
            self,
            "manifest_sha256",
            _sha256(self.manifest_sha256, "manifest_sha256"),
        )
        object.__setattr__(
            self,
            "slot_sha256",
            _sha256(self.slot_sha256, "slot_sha256"),
        )
        object.__setattr__(
            self,
            "flow_policy_sha256",
            _sha256(self.flow_policy_sha256, "flow_policy_sha256"),
        )
        object.__setattr__(self, "contracts", contracts)

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)

    @property
    def contract_map(
        self,
    ) -> Mapping[str, AdvancedOptionsContractScopeEntry]:
        return MappingProxyType({
            row.contract_ticker: row for row in self.contracts
        })

    def validate_policy(
        self,
        policy: AdvancedOptionsFlowPolicy,
    ) -> None:
        if not isinstance(policy, AdvancedOptionsFlowPolicy):
            raise TypeError("Advanced Options flow policy is invalid")
        if self.flow_policy_sha256 != policy.sha256:
            raise ValueError(
                "contract scope flow policy identity does not match"
            )
        for entry in self.contracts:
            dte = (entry.expiration - self.session_date).days
            if (
                policy.tier_for(entry.underlyer) is not entry.universe_tier
                or policy.lane_for(dte) is not entry.dte_lane
            ):
                raise ValueError(
                    "contract scope tier or DTE lane does not match policy"
                )


_ROOT_KEYS = frozenset({
    "version",
    "universe_tiers",
    "dte_lanes",
    "excluded_dte_bands",
    "profiles",
    "opening_overlay",
    "candidate_caps",
})
_PROFILE_KEYS = frozenset({
    "kind",
    "dte_lanes",
    "threshold_mode",
    "tier_thresholds",
    "window_seconds_by_lane",
    "directional_side_classes",
    "directional_confidences",
    "retain_inside_spread_as_context",
})
_TIER_THRESHOLD_KEYS = frozenset({
    "tier",
    "minimum_contracts",
    "minimum_print_premium",
    "minimum_cluster_prints",
    "minimum_distinct_exchanges",
    "minimum_cluster_premium",
    "minimum_cumulative_contracts",
})
_DTE_BAND_KEYS = frozenset({"lane", "minimum_dte", "maximum_dte"})
_EXCLUDED_DTE_BAND_KEYS = frozenset({
    "minimum_dte",
    "maximum_dte",
    "reason_code",
})
_OPENING_OVERLAY_KEYS = frozenset({
    "minimum_prior_open_interest",
    "unusual_volume_oi_ratio",
    "high_conviction_volume_oi_ratio",
    "directional_side_classes",
    "directional_confidences",
    "retain_inside_spread_as_context",
})
_CANDIDATE_CAP_KEYS = frozenset({
    "global_maximum",
    "maximum_per_underlying",
    "contract_profile_dedup_seconds",
    "profile_budgets",
})


def _money(value: object) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("flow policy money must be finite")
    return result


def _tier_threshold(value: object) -> FlowTierThreshold:
    if not isinstance(value, dict) or frozenset(value) != _TIER_THRESHOLD_KEYS:
        raise ValueError("flow tier threshold keys changed")
    return FlowTierThreshold(
        tier=FlowUniverseTier(str(value["tier"])),
        minimum_contracts=int(value["minimum_contracts"]),
        minimum_print_premium=_money(value["minimum_print_premium"]),
        minimum_cluster_prints=(
            None
            if value["minimum_cluster_prints"] is None
            else int(value["minimum_cluster_prints"])
        ),
        minimum_distinct_exchanges=(
            None
            if value["minimum_distinct_exchanges"] is None
            else int(value["minimum_distinct_exchanges"])
        ),
        minimum_cluster_premium=(
            None
            if value["minimum_cluster_premium"] is None
            else _money(value["minimum_cluster_premium"])
        ),
        minimum_cumulative_contracts=(
            None
            if value["minimum_cumulative_contracts"] is None
            else int(value["minimum_cumulative_contracts"])
        ),
    )


def _profile(value: object) -> FlowRetentionProfile:
    if not isinstance(value, dict) or frozenset(value) != _PROFILE_KEYS:
        raise ValueError("flow retention profile keys changed")
    windows = value["window_seconds_by_lane"]
    if (
        not isinstance(windows, dict)
        or type(value["retain_inside_spread_as_context"]) is not bool
    ):
        raise ValueError("flow retention profile windows must be an object")
    return FlowRetentionProfile(
        kind=FlowProfileKind(str(value["kind"])),
        dte_lanes=tuple(FlowDteLane(str(item)) for item in value["dte_lanes"]),
        threshold_mode=FlowThresholdMode(str(value["threshold_mode"])),
        tier_thresholds=tuple(
            _tier_threshold(item) for item in value["tier_thresholds"]
        ),
        window_seconds_by_lane=tuple(
            (FlowDteLane(str(lane)), int(seconds))
            for lane, seconds in windows.items()
        ),
        directional_side_classes=tuple(
            str(item) for item in value["directional_side_classes"]
        ),
        directional_confidences=tuple(
            str(item) for item in value["directional_confidences"]
        ),
        retain_inside_spread_as_context=value[
            "retain_inside_spread_as_context"
        ],
    )


def load_advanced_options_flow_policy(
    path: Path = DEFAULT_ADVANCED_OPTIONS_FLOW_POLICY_PATH,
) -> AdvancedOptionsFlowPolicy:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or frozenset(value) != _ROOT_KEYS:
        raise ValueError("Advanced Options flow policy keys changed")
    tiers = value["universe_tiers"]
    if not isinstance(tiers, dict) or set(tiers) != {
        tier.value for tier in FlowUniverseTier
    }:
        raise ValueError("Advanced Options flow universe tier keys changed")
    memberships = tuple(
        FlowUniverseMembership(
            FlowUniverseTier(str(tier)),
            tuple(str(item) for item in underlyings),
        )
        for tier, underlyings in tiers.items()
    )
    if not isinstance(value["dte_lanes"], list) or any(
        not isinstance(item, dict)
        or frozenset(item) != _DTE_BAND_KEYS
        for item in value["dte_lanes"]
    ):
        raise ValueError("Advanced Options flow DTE lane keys changed")
    dte_bands = tuple(
        FlowDteBand(
            FlowDteLane(str(item["lane"])),
            int(item["minimum_dte"]),
            int(item["maximum_dte"]),
        )
        for item in value["dte_lanes"]
    )
    if not isinstance(value["excluded_dte_bands"], list) or any(
        not isinstance(item, dict)
        or frozenset(item) != _EXCLUDED_DTE_BAND_KEYS
        for item in value["excluded_dte_bands"]
    ):
        raise ValueError("Advanced Options excluded DTE keys changed")
    excluded = tuple(
        FlowExcludedDteBand(
            int(item["minimum_dte"]),
            int(item["maximum_dte"]),
            str(item["reason_code"]),
        )
        for item in value["excluded_dte_bands"]
    )
    opening = value["opening_overlay"]
    caps = value["candidate_caps"]
    if (
        not isinstance(opening, dict)
        or frozenset(opening) != _OPENING_OVERLAY_KEYS
        or type(opening["retain_inside_spread_as_context"]) is not bool
    ):
        raise ValueError("Advanced Options opening overlay keys changed")
    if not isinstance(caps, dict) or frozenset(caps) != _CANDIDATE_CAP_KEYS:
        raise ValueError("Advanced Options candidate cap keys changed")
    return AdvancedOptionsFlowPolicy(
        version=str(value["version"]),
        universe=memberships,
        dte_bands=dte_bands,
        excluded_dte_bands=excluded,
        profiles=tuple(_profile(item) for item in value["profiles"]),
        opening_overlay=FlowOpeningOverlay(
            minimum_prior_open_interest=int(
                opening["minimum_prior_open_interest"]
            ),
            unusual_volume_oi_ratio=float(
                opening["unusual_volume_oi_ratio"]
            ),
            high_conviction_volume_oi_ratio=float(
                opening["high_conviction_volume_oi_ratio"]
            ),
            directional_side_classes=tuple(
                str(item) for item in opening["directional_side_classes"]
            ),
            directional_confidences=tuple(
                str(item) for item in opening["directional_confidences"]
            ),
            retain_inside_spread_as_context=opening[
                "retain_inside_spread_as_context"
            ],
        ),
        candidate_caps=FlowCandidateCaps(
            global_maximum=int(caps["global_maximum"]),
            maximum_per_underlying=int(caps["maximum_per_underlying"]),
            contract_profile_dedup_seconds=int(
                caps["contract_profile_dedup_seconds"]
            ),
            profile_budgets=tuple(
                (FlowProfileKind(str(kind)), int(limit))
                for kind, limit in caps["profile_budgets"].items()
            ),
        ),
    )
