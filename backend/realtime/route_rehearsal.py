from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Mapping
from uuid import UUID

from .contracts import canonical_sha256
from .preflight import ProviderTier


PORTAL_SURFACES = (
    "TICKER_PAGE",
    "SCREENER",
    "EQUITY_SNAPSHOTS",
    "STOCK_ALERTS",
    "OPTIONS_ALERTS",
    "OPTION_MARKS",
)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class PortalRouteTarget:
    route_id: str
    source: str
    cohort_id: UUID | None = None
    provider_tier: ProviderTier | None = None

    def __post_init__(self) -> None:
        if not self.route_id.strip() or self.source not in {"RETAINED", "REALTIME"}:
            raise ValueError("invalid portal route target")
        if self.source == "REALTIME" and (
            self.cohort_id is None or self.provider_tier is None
        ):
            raise ValueError("realtime route requires cohort and provider tier")
        if self.source == "RETAINED" and (
            self.cohort_id is not None or self.provider_tier is not None
        ):
            raise ValueError("retained route cannot inherit realtime identity")


@dataclass(frozen=True, slots=True)
class PortalRouteState:
    generation: int
    effective_at: datetime
    targets: tuple[tuple[str, PortalRouteTarget], ...]

    def __post_init__(self) -> None:
        if type(self.generation) is not int or self.generation <= 0:
            raise ValueError("portal route generation must be positive")
        if tuple(surface for surface, _ in self.targets) != PORTAL_SURFACES:
            raise ValueError("portal route must cover every surface in canonical order")
        if len({target for _, target in self.targets}) != 1:
            raise ValueError("mixed portal routes are prohibited")
        object.__setattr__(self, "effective_at", _utc(self.effective_at, "effective_at"))

    @property
    def target(self) -> PortalRouteTarget:
        return self.targets[0][1]

    @property
    def sha256(self) -> str:
        return canonical_sha256(self)


@dataclass(frozen=True, slots=True)
class PortalRouteRehearsal:
    initial: PortalRouteState
    forward: PortalRouteState
    reverse: PortalRouteState
    forward_results: tuple[tuple[str, bool], ...]
    reverse_results: tuple[tuple[str, bool], ...]
    passed: bool
    persisted: bool = False
    activation_authorized: bool = False


def build_route_state(
    target: PortalRouteTarget,
    *,
    generation: int,
    effective_at: datetime,
) -> PortalRouteState:
    return PortalRouteState(
        generation=generation,
        effective_at=effective_at,
        targets=tuple((surface, target) for surface in PORTAL_SURFACES),
    )


def rehearse_forward_reverse_routes(
    initial: PortalRouteState,
    target: PortalRouteTarget,
    *,
    effective_at: datetime,
    probe: Callable[[PortalRouteState], Mapping[str, bool]],
) -> PortalRouteRehearsal:
    effective = _utc(effective_at, "effective_at")
    if target == initial.target:
        raise ValueError("forward route target must differ from the initial route")
    if effective < initial.effective_at:
        raise ValueError("route rehearsal cannot move effective time backward")
    forward = build_route_state(
        target,
        generation=initial.generation + 1,
        effective_at=effective,
    )
    forward_result = _validate_probe(probe(forward))
    reverse = build_route_state(
        initial.target,
        generation=forward.generation + 1,
        effective_at=effective,
    )
    reverse_result = _validate_probe(probe(reverse))
    return PortalRouteRehearsal(
        initial=initial,
        forward=forward,
        reverse=reverse,
        forward_results=forward_result,
        reverse_results=reverse_result,
        passed=all(value for _, value in (*forward_result, *reverse_result)),
    )


def select_portal_repository_bundle(
    state: PortalRouteState,
    *,
    retained,
    realtime,
):
    return realtime if state.target.source == "REALTIME" else retained


def _validate_probe(result: Mapping[str, bool]) -> tuple[tuple[str, bool], ...]:
    if set(result) != set(PORTAL_SURFACES):
        raise ValueError("route probe must cover every portal surface")
    if any(type(value) is not bool for value in result.values()):
        raise TypeError("route probe results must be booleans")
    return tuple((surface, result[surface]) for surface in PORTAL_SURFACES)