"""Shared ownership and scope rules for the continuous Advanced V3 runtime."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from typing import Mapping
from uuid import UUID

from .advanced_equity_lifecycle import AdvancedEquityLifecyclePlanner
from .advanced_realtime_desired_state import DesiredMemberKind
from .advanced_realtime_desired_state_repository import (
    load_effective_desired_state_reconciliation_cursor,
)


UTC = timezone.utc
ADVANCED_REALTIME_V3_COMPOSE_PROFILE = "advanced-realtime-v3"
ADVANCED_REALTIME_V3_RUNTIME_RESOURCE = "advanced-realtime-v3:runtime"


class AdvancedRealtimeRoleGroup(str, Enum):
    PROVIDER = "PROVIDER"
    PROCESSING = "PROCESSING"
    PRODUCT = "PRODUCT"
    OPTIONS = "OPTIONS"
    MAINTENANCE = "MAINTENANCE"
    SERVING = "SERVING"


@dataclass(frozen=True, slots=True)
class AdvancedRealtimeRoleSpec:
    name: str
    group: AdvancedRealtimeRoleGroup
    provider_credential: bool = False
    app_database: bool = False
    api_database: bool = False
    admin_database: bool = False

    def __post_init__(self) -> None:
        if (
            not self.name
            or not isinstance(self.group, AdvancedRealtimeRoleGroup)
            or self.provider_credential
            and not self.app_database
            or self.api_database
            and (
                self.provider_credential
                or self.app_database
                or self.admin_database
            )
            or self.admin_database
            and not self.app_database
        ):
            raise ValueError("Advanced V3 role specification is invalid")


ADVANCED_REALTIME_V3_ROLES = (
    AdvancedRealtimeRoleSpec(
        "capability-refresh",
        AdvancedRealtimeRoleGroup.PROVIDER,
        provider_credential=True,
        app_database=True,
    ),
    AdvancedRealtimeRoleSpec(
        "equity-preparation",
        AdvancedRealtimeRoleGroup.PROVIDER,
        provider_credential=True,
        app_database=True,
    ),
    AdvancedRealtimeRoleSpec(
        "controller",
        AdvancedRealtimeRoleGroup.PROVIDER,
        provider_credential=True,
        app_database=True,
    ),
    AdvancedRealtimeRoleSpec(
        "equity-ingress",
        AdvancedRealtimeRoleGroup.PROVIDER,
        provider_credential=True,
        app_database=True,
    ),
    AdvancedRealtimeRoleSpec(
        "options-lifecycle",
        AdvancedRealtimeRoleGroup.PROVIDER,
        provider_credential=True,
        app_database=True,
    ),
    AdvancedRealtimeRoleSpec(
        "options-acquisition",
        AdvancedRealtimeRoleGroup.PROVIDER,
        provider_credential=True,
        app_database=True,
    ),
    AdvancedRealtimeRoleSpec(
        "bridge",
        AdvancedRealtimeRoleGroup.PROCESSING,
        app_database=True,
    ),
    AdvancedRealtimeRoleSpec(
        "finalizer",
        AdvancedRealtimeRoleGroup.PROCESSING,
        app_database=True,
    ),
    AdvancedRealtimeRoleSpec(
        "scheduler",
        AdvancedRealtimeRoleGroup.PROCESSING,
        app_database=True,
    ),
    AdvancedRealtimeRoleSpec(
        "detectors",
        AdvancedRealtimeRoleGroup.PROCESSING,
        app_database=True,
    ),
    AdvancedRealtimeRoleSpec(
        "canonical-stock-alerts",
        AdvancedRealtimeRoleGroup.PRODUCT,
        app_database=True,
    ),
    AdvancedRealtimeRoleSpec(
        "publication-retry",
        AdvancedRealtimeRoleGroup.PRODUCT,
        app_database=True,
    ),
    AdvancedRealtimeRoleSpec(
        "options-ingress",
        AdvancedRealtimeRoleGroup.OPTIONS,
        app_database=True,
    ),
    AdvancedRealtimeRoleSpec(
        "options-market-state",
        AdvancedRealtimeRoleGroup.OPTIONS,
        app_database=True,
    ),
    AdvancedRealtimeRoleSpec(
        "options-flow",
        AdvancedRealtimeRoleGroup.OPTIONS,
        app_database=True,
    ),
    AdvancedRealtimeRoleSpec(
        "options-window",
        AdvancedRealtimeRoleGroup.OPTIONS,
        app_database=True,
    ),
    AdvancedRealtimeRoleSpec(
        "retention",
        AdvancedRealtimeRoleGroup.MAINTENANCE,
        app_database=True,
        admin_database=True,
    ),
    AdvancedRealtimeRoleSpec(
        "api",
        AdvancedRealtimeRoleGroup.SERVING,
        api_database=True,
    ),
    AdvancedRealtimeRoleSpec(
        "frontend",
        AdvancedRealtimeRoleGroup.SERVING,
    ),
)
ADVANCED_REALTIME_V3_ROLE_BY_NAME = {
    role.name: role for role in ADVANCED_REALTIME_V3_ROLES
}


@dataclass(frozen=True, slots=True)
class AdvancedRealtimeSessionScope:
    session_date: date
    cohort_id: UUID
    revision_id: UUID
    snapshot_sha256: str
    equity_members: tuple[str, ...]
    option_underlyings: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            self.cohort_id.int == 0
            or self.revision_id.int == 0
            or len(self.snapshot_sha256) != 64
            or not self.equity_members
            or not set(self.option_underlyings).issubset(
                self.equity_members
            )
        ):
            raise ValueError("Advanced V3 session scope is invalid")


def role_restart_delay(attempt: int) -> int:
    if type(attempt) is not int or attempt < 1:
        raise ValueError("Advanced V3 restart attempt must be positive")
    return min(60, 2 ** min(attempt - 1, 6))


def load_advanced_realtime_session_scope_cursor(
    cursor,
    *,
    session_date: date,
) -> AdvancedRealtimeSessionScope | None:
    reconciliation = load_effective_desired_state_reconciliation_cursor(
        cursor,
        session_date=session_date,
    )
    if (
        reconciliation is None
        or reconciliation.state != "READY"
        or reconciliation.snapshot is None
    ):
        return None
    ready = {
        (row.ticker, row.kind)
        for row in reconciliation.preparations
        if row.enabled and row.status.value == "READY"
    }
    equity_members = tuple(sorted(
        ticker
        for ticker, kind in ready
        if kind in {
            DesiredMemberKind.EQUITY,
            DesiredMemberKind.SECTOR_ETF,
        }
    ))
    option_underlyings = tuple(sorted(
        ticker
        for ticker, kind in ready
        if kind is DesiredMemberKind.OPTION_UNDERLYING
    ))
    if not equity_members:
        return None
    snapshot = reconciliation.snapshot
    return AdvancedRealtimeSessionScope(
        session_date=snapshot.session_date,
        cohort_id=snapshot.cohort_id,
        revision_id=snapshot.revision_id,
        snapshot_sha256=snapshot.snapshot_sha256,
        equity_members=equity_members,
        option_underlyings=option_underlyings,
    )


def load_current_advanced_realtime_session_scope_cursor(
    cursor,
    *,
    checked_at: datetime,
    drain_seconds: int = 30,
    finality_delay_seconds: int = 5,
    completion_budget_seconds: int = 60,
) -> AdvancedRealtimeSessionScope | None:
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        raise ValueError("Advanced V3 scope clock must be timezone-aware")
    planner = AdvancedEquityLifecyclePlanner(
        clock=lambda: checked_at.astimezone(UTC),
    )
    lifecycle = planner.plan(
        drain_seconds=drain_seconds,
        finality_delay_seconds=finality_delay_seconds,
        completion_budget_seconds=completion_budget_seconds,
    )
    return load_advanced_realtime_session_scope_cursor(
        cursor,
        session_date=lifecycle.processing_session_date,
    )


def advanced_realtime_v3_topology_payload(
    commands: Mapping[str, tuple[str, ...] | list[str]],
) -> dict[str, object]:
    command_names = set(commands)
    expected_names = set(ADVANCED_REALTIME_V3_ROLE_BY_NAME)
    if command_names != expected_names:
        raise ValueError(
            "Advanced V3 command topology changed: "
            f"missing={sorted(expected_names - command_names)},"
            f"extra={sorted(command_names - expected_names)}"
        )
    return {
        "contract": "advanced-realtime-v3-topology-v1",
        "compose_profile": ADVANCED_REALTIME_V3_COMPOSE_PROFILE,
        "roles": [
            {
                "name": role.name,
                "group": role.group.value,
                "provider_credential": role.provider_credential,
                "app_database": role.app_database,
                "api_database": role.api_database,
                "admin_database": role.admin_database,
            }
            for role in ADVANCED_REALTIME_V3_ROLES
        ],
    }
