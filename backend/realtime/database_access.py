from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any

from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from .migrations import EXPECTED_DATABASE_NAME


_ROLE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{2,62}$")
_PROCESS_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_.:-]{2,127}$")
EXPECTED_REALTIME_LINEAGE_NAME = "advanced_realtime_v1"


class RealtimeDatabasePrincipal(str, Enum):
    RUNTIME = "RUNTIME"
    API = "API"


class RealtimeDatabaseRole(str, Enum):
    CONTROL = "CONTROL"
    INGRESS = "INGRESS"
    COMPUTE = "COMPUTE"
    REFERENCES = "REFERENCES"
    PUBLICATION = "PUBLICATION"
    READER = "READER"
    MAINTENANCE = "MAINTENANCE"
    # V2 options capability roles (relation_registry `options_ingress` / `options_compute`); the lower-cased
    # value is the capability role name V2StageDatabase hands to the stage transaction.
    OPTIONS_INGRESS = "OPTIONS_INGRESS"
    OPTIONS_COMPUTE = "OPTIONS_COMPUTE"


ROLE_PRINCIPALS = {
    RealtimeDatabaseRole.CONTROL: RealtimeDatabasePrincipal.RUNTIME,
    RealtimeDatabaseRole.INGRESS: RealtimeDatabasePrincipal.RUNTIME,
    RealtimeDatabaseRole.COMPUTE: RealtimeDatabasePrincipal.RUNTIME,
    RealtimeDatabaseRole.REFERENCES: RealtimeDatabasePrincipal.RUNTIME,
    RealtimeDatabaseRole.PUBLICATION: RealtimeDatabasePrincipal.RUNTIME,
    RealtimeDatabaseRole.READER: RealtimeDatabasePrincipal.API,
    RealtimeDatabaseRole.MAINTENANCE: RealtimeDatabasePrincipal.RUNTIME,
    RealtimeDatabaseRole.OPTIONS_INGRESS: RealtimeDatabasePrincipal.RUNTIME,
    RealtimeDatabaseRole.OPTIONS_COMPUTE: RealtimeDatabasePrincipal.RUNTIME,
}


@dataclass(frozen=True, slots=True)
class RealtimePoolBudget:
    minimum_connections: int
    maximum_connections: int

    def __post_init__(self) -> None:
        if (
            type(self.minimum_connections) is not int
            or type(self.maximum_connections) is not int
            or self.minimum_connections < 0
            or self.maximum_connections <= 0
            or self.minimum_connections > self.maximum_connections
        ):
            raise ValueError("invalid realtime connection pool budget")


@dataclass(frozen=True, slots=True)
class RealtimeProcessDatabasePlan:
    process_name: str
    role: RealtimeDatabaseRole
    pool_budget: RealtimePoolBudget

    def __post_init__(self) -> None:
        normalized = self.process_name.strip().lower()
        if not _PROCESS_NAME_PATTERN.fullmatch(normalized):
            raise ValueError("invalid realtime database process name")
        if not isinstance(self.role, RealtimeDatabaseRole):
            raise TypeError("realtime database process role is invalid")
        if not isinstance(self.pool_budget, RealtimePoolBudget):
            raise TypeError("realtime database process pool budget is invalid")
        object.__setattr__(self, "process_name", normalized)

    @property
    def principal(self) -> RealtimeDatabasePrincipal:
        return ROLE_PRINCIPALS[self.role]


@dataclass(frozen=True, slots=True)
class RealtimeUnitOfWork:
    role: RealtimeDatabaseRole
    principal: RealtimeDatabasePrincipal
    connection: Any
    cursor: Any
    read_only: bool


def planned_connection_ceiling(
    process_counts: Mapping[RealtimeDatabasePrincipal, int],
    budgets: Mapping[RealtimeDatabasePrincipal, RealtimePoolBudget],
    *,
    server_max_connections: int,
    reserved_connections: int,
) -> int:
    if (
        type(server_max_connections) is not int
        or type(reserved_connections) is not int
        or server_max_connections <= 0
        or not 0 <= reserved_connections < server_max_connections
    ):
        raise ValueError("invalid PostgreSQL connection capacity")
    if set(process_counts) != set(budgets):
        raise ValueError("process counts and pool budgets must cover the same principals")
    if any(type(count) is not int or count < 0 for count in process_counts.values()):
        raise ValueError("realtime process counts must be nonnegative integers")
    ceiling = sum(
        process_counts[principal] * budget.maximum_connections
        for principal, budget in budgets.items()
    )
    if ceiling > server_max_connections - reserved_connections:
        raise ValueError("realtime connection ceiling exceeds PostgreSQL capacity")
    return ceiling


def planned_process_connection_ceiling(
    process_plans: Iterable[RealtimeProcessDatabasePlan],
    *,
    server_max_connections: int,
    reserved_connections: int,
) -> int:
    plans = tuple(process_plans)
    if not plans:
        raise ValueError("realtime database process plans cannot be empty")
    if any(not isinstance(plan, RealtimeProcessDatabasePlan) for plan in plans):
        raise TypeError("invalid realtime database process plan")
    process_names = tuple(plan.process_name for plan in plans)
    if len(process_names) != len(set(process_names)):
        raise ValueError("realtime database process plans must be unique")
    principal_counts = {
        principal: sum(plan.principal is principal for plan in plans)
        for principal in RealtimeDatabasePrincipal
    }
    principal_budgets = {
        principal: RealtimePoolBudget(
            0,
            sum(
                plan.pool_budget.maximum_connections
                for plan in plans
                if plan.principal is principal
            ),
        )
        for principal in RealtimeDatabasePrincipal
        if principal_counts[principal]
    }
    return planned_connection_ceiling(
        {principal: 1 for principal in principal_budgets},
        principal_budgets,
        server_max_connections=server_max_connections,
        reserved_connections=reserved_connections,
    )


class RoleScopedConnectionProvider:
    def __init__(
        self,
        principal_pools: Mapping[RealtimeDatabasePrincipal, Any],
        role_names: Mapping[RealtimeDatabaseRole, str],
        *,
        statement_timeout_ms: int = 5_000,
    ) -> None:
        if not principal_pools:
            raise ValueError("role-scoped provider requires at least one principal pool")
        if type(statement_timeout_ms) is not int or statement_timeout_ms <= 0:
            raise ValueError("statement timeout must be a positive integer")
        normalized_names = {}
        for role, role_name in role_names.items():
            if not isinstance(role, RealtimeDatabaseRole):
                raise TypeError("database role mapping contains an invalid role")
            normalized = role_name.strip()
            if not _ROLE_NAME_PATTERN.fullmatch(normalized):
                raise ValueError("database capability role name is invalid")
            principal = ROLE_PRINCIPALS[role]
            if principal not in principal_pools:
                raise ValueError(f"database role {role.value} has no principal pool")
            normalized_names[role] = normalized
        if not normalized_names:
            raise ValueError("role-scoped provider requires at least one database role")
        self._principal_pools = dict(principal_pools)
        self._role_names = normalized_names
        self._statement_timeout_ms = statement_timeout_ms

    @contextmanager
    def transaction(
        self,
        role: RealtimeDatabaseRole,
        *,
        read_only: bool | None = None,
        dict_cursor: bool = False,
        isolation_level: str | None = None,
    ):
        role_name = self._role_names.get(role)
        if role_name is None:
            raise ValueError(f"database role {role.value} is not configured for this process")
        principal = ROLE_PRINCIPALS[role]
        effective_read_only = role is RealtimeDatabaseRole.READER if read_only is None else read_only
        if role is RealtimeDatabaseRole.READER and not effective_read_only:
            raise ValueError("reader role transactions must be read-only")
        normalized_isolation = (
            isolation_level.strip().upper() if isolation_level is not None else None
        )
        if normalized_isolation not in {None, "REPEATABLE READ", "SERIALIZABLE"}:
            raise ValueError("unsupported realtime transaction isolation level")
        pool = self._principal_pools[principal]
        connection = pool.getconn()
        try:
            cursor_factory = RealDictCursor if dict_cursor else None
            with connection.cursor(cursor_factory=cursor_factory) as cursor:
                access_mode = "READ ONLY" if effective_read_only else "READ WRITE"
                cursor.execute(
                    f"SET TRANSACTION ISOLATION LEVEL {normalized_isolation}, {access_mode}"
                    if normalized_isolation is not None
                    else f"SET TRANSACTION {access_mode}"
                )
                cursor.execute(
                    sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(role_name))
                )
                cursor.execute(
                    "SELECT set_config('statement_timeout', %s, true)",
                    (f"{self._statement_timeout_ms}ms",),
                )
                yield RealtimeUnitOfWork(
                    role=role,
                    principal=principal,
                    connection=connection,
                    cursor=cursor,
                    read_only=effective_read_only,
                )
            connection.commit()
        except Exception:
            if not connection.closed:
                connection.rollback()
            raise
        finally:
            pool.putconn(connection, close=bool(connection.closed))


def wire_process_connection_provider(
    process_plan: RealtimeProcessDatabasePlan,
    *,
    principal_pool: Any,
    role_name: str,
    statement_timeout_ms: int = 5_000,
) -> RoleScopedConnectionProvider:
    if principal_pool is None:
        raise ValueError("realtime database process requires an injected principal pool")
    provider = RoleScopedConnectionProvider(
        {process_plan.principal: principal_pool},
        {process_plan.role: role_name},
        statement_timeout_ms=statement_timeout_ms,
    )
    verify_realtime_pool_target(
        principal_pool,
        role_name=role_name,
        statement_timeout_ms=statement_timeout_ms,
    )
    return provider


def verify_realtime_pool_target(
    principal_pool: Any,
    *,
    role_name: str,
    statement_timeout_ms: int = 5_000,
) -> None:
    normalized_role = role_name.strip()
    if not _ROLE_NAME_PATTERN.fullmatch(normalized_role):
        raise ValueError("database capability role name is invalid")
    if type(statement_timeout_ms) is not int or statement_timeout_ms <= 0:
        raise ValueError("statement timeout must be a positive integer")
    connection = principal_pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute(
                "SELECT set_config('statement_timeout', %s, true)",
                (f"{statement_timeout_ms}ms",),
            )
            cursor.execute("SELECT current_database()")
            database_row = cursor.fetchone()
            database_name = str(database_row[0]) if database_row is not None else ""
            if database_name != EXPECTED_DATABASE_NAME:
                raise RuntimeError(
                    "realtime pool targets the wrong database: "
                    f"expected {EXPECTED_DATABASE_NAME}, got {database_name or 'UNKNOWN'}"
                )
            cursor.execute(
                sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(normalized_role))
            )
            cursor.execute(
                "SELECT lineage_name, database_name::text "
                "FROM rt.database_identity WHERE lineage_name=%s",
                (EXPECTED_REALTIME_LINEAGE_NAME,),
            )
            identity = cursor.fetchone()
            if identity is None or tuple(str(value) for value in identity) != (
                EXPECTED_REALTIME_LINEAGE_NAME,
                EXPECTED_DATABASE_NAME,
            ):
                raise RuntimeError("realtime pool database identity does not match source")
    finally:
        if not connection.closed:
            connection.rollback()
        principal_pool.putconn(connection, close=bool(connection.closed))