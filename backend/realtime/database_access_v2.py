"""V2 database access with a deferred security phase.

While security is deferred the runtime connects as the database owner and no role is switched;
transactions still declare the capability role and principal profile they would use, and a
mismatch is recorded so the hardening phase cannot reveal a wrong-role write for the first time.
Once hardened, each transaction runs `SET LOCAL ROLE <prefix>_<capability>` on the pool of the
stage's principal. The security phase ledger (rt.security_phase_events) gates both modes: a
deferred runtime refuses a database already hardened and a hardened runtime refuses a database
whose privileges were not audited.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from psycopg2 import Error as PsycopgError, sql
from psycopg2.extras import RealDictCursor

from .migrations import (
    EXPECTED_V2_DATABASE_NAME,
    SECURITY_MODE_DEFERRED,
    SECURITY_MODE_HARDENED,
    SECURITY_MODES_V2,
)
from .relation_registry import CAPABILITY_ROLES, PRINCIPAL_PROFILES
from .security_v2 import build_role_names_v2
from .stage_graph import STAGES_V1, StageDefinition, principal_profiles_required

EXPECTED_V2_LINEAGE_NAME = "advanced_realtime_v2"
DISPOSABLE_V2_PREFIX = "test_realtime_v2_"
OWNER_POOL = "owner"
_PHASE_FOR_MODE = {SECURITY_MODE_DEFERRED: "DEFERRED", SECURITY_MODE_HARDENED: "AUDITED"}
_ISOLATION_LEVELS = {None, "REPEATABLE READ", "SERIALIZABLE"}


@dataclass(frozen=True, slots=True)
class V2AccessConfig:
    security_mode: str = SECURITY_MODE_DEFERRED
    role_prefix: str | None = None
    statement_timeout_ms: int = 5_000
    allow_disposable: bool = False

    def __post_init__(self) -> None:
        if self.security_mode not in SECURITY_MODES_V2:
            raise ValueError(f"security mode must be one of {', '.join(SECURITY_MODES_V2)}")
        if type(self.statement_timeout_ms) is not int or self.statement_timeout_ms <= 0:
            raise ValueError("statement timeout must be a positive integer")
        if self.security_mode == SECURITY_MODE_HARDENED:
            if not self.role_prefix:
                raise ValueError("hardened access requires the capability role prefix")
            build_role_names_v2(self.role_prefix)


@dataclass(frozen=True, slots=True)
class RoleViolation:
    capability_role: str
    principal_profile: str
    reason: str


@dataclass(slots=True)
class V2UnitOfWork:
    capability_role: str
    principal_profile: str
    connection: Any
    cursor: Any
    read_only: bool


FENCE_SQLSTATE = "RT001"


class WriteFenceRejected(RuntimeError):
    """A fenced transaction was rolled back because the work it belongs to is no longer valid
    (for example its stage timed out); none of its writes were committed."""


class CommitFence(Protocol):
    def verify(self, cursor: Any) -> None:
        """Run as the last statement of the transaction; raise to roll everything back."""


@dataclass(frozen=True, slots=True)
class StageRunFence:
    """Commits only while the stage run is RUNNING (rt.fence_stage_run). The check and the commit are
    in one transaction and the run row stays locked through COMMIT, so a stage that timed out can
    never land ordinary data after the coordinator recorded the timeout."""

    stage_run_id: UUID

    def verify(self, cursor: Any) -> None:
        try:
            cursor.execute("SELECT rt.fence_stage_run(%s)", (str(self.stage_run_id),))
        except PsycopgError as error:
            if getattr(error, "pgcode", None) == FENCE_SQLSTATE:
                raise WriteFenceRejected(
                    f"stage run {self.stage_run_id} is not RUNNING; its writes were rolled back") from None
            raise


def pools_required(enabled_stages: Iterable[str], config: V2AccessConfig,
                   stages: Mapping[str, StageDefinition] = STAGES_V1) -> frozenset[str]:
    """Pool keys a plan needs. Deferred access uses one owner pool for every stage; hardened
    access needs only the principals of the enabled stages (an equity-only plan never opens the
    options principal)."""
    enabled = frozenset(enabled_stages)
    if config.security_mode == SECURITY_MODE_DEFERRED:
        return frozenset({OWNER_POOL}) if enabled else frozenset()
    return principal_profiles_required(enabled, stages)


CONNECTION_HEADROOM = 0.70


def orchestrator_pool_size(enabled_stages: Iterable[str], stages: Mapping[str, StageDefinition] = STAGES_V1) -> int:
    """Connections one orchestrator process can hold: a lane per principal profile, a resident loop
    per INTERVAL/CONTINUOUS stage, plus the coordinator and its lease heartbeat."""
    enabled = frozenset(enabled_stages)
    resident = sum(1 for name in enabled if stages[name].schedule_kind in ("INTERVAL", "CONTINUOUS"))
    return len(principal_profiles_required(enabled, stages)) + resident + 2


def plan_connection_budget(planned: Mapping[str, int], *, server_max_connections: int,
                           reserved_connections: int, headroom: float = CONNECTION_HEADROOM) -> int:
    """Fail when the whole deployment (every consumer's pool maximum) exceeds headroom of the usable
    server connections; return the allowed ceiling. Consumers are for example the orchestrator, API,
    archive worker and maintenance, each with its pool maximum."""
    if (type(server_max_connections) is not int or type(reserved_connections) is not int
            or server_max_connections <= 0 or not 0 <= reserved_connections < server_max_connections):
        raise ValueError("invalid PostgreSQL connection capacity")
    if not 0 < headroom <= 1 or not planned or any(type(count) is not int or count <= 0 for count in planned.values()):
        raise ValueError("connection budget requires positive pool maximums and a headroom in (0, 1]")
    allowed = int((server_max_connections - reserved_connections) * headroom)
    total = sum(planned.values())
    if total > allowed:
        raise ValueError(f"planned {total} connections exceed the {allowed} allowed "
                         f"({headroom:.0%} of {server_max_connections - reserved_connections} usable)")
    return allowed


def verify_server_connection_budget(pool: Any, planned: Mapping[str, int], *, reserved_connections: int = 5,
                                    headroom: float = CONNECTION_HEADROOM) -> int:
    """Read max_connections and superuser_reserved_connections from the server and enforce the plan."""
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_setting('max_connections')::int, "
                           "current_setting('superuser_reserved_connections')::int")
            maximum, superuser_reserved = cursor.fetchone()
    finally:
        if not connection.closed:
            connection.rollback()
        pool.putconn(connection, close=bool(connection.closed))
    return plan_connection_budget(planned, server_max_connections=int(maximum),
                                  reserved_connections=max(int(reserved_connections), int(superuser_reserved)),
                                  headroom=headroom)


def _verification_role(config: V2AccessConfig, principal_profile: str | None) -> str | None:
    """The capability role a hardened pool assumes to read the identity and phase tables. A NOINHERIT
    principal cannot read them itself, so it checks as `control` (runtime principals) or `reader` (api)."""
    if config.security_mode != SECURITY_MODE_HARDENED or principal_profile is None or not config.role_prefix:
        return None
    memberships = PRINCIPAL_PROFILES.get(principal_profile, ())
    role = next((name for name in ("control", "reader") if name in memberships), None)
    return build_role_names_v2(config.role_prefix).name(role) if role else None


def verify_pool_target_v2(pool: Any, config: V2AccessConfig, principal_profile: str | None = None) -> str:
    """Check the database name, lineage identity and security phase; return the database name.
    `principal_profile` names the principal a hardened pool logs in as, so the check runs under a
    capability role that is allowed to read the identity and phase tables."""
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")
            verification_role = _verification_role(config, principal_profile)
            if verification_role is not None:
                cursor.execute(sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(verification_role)))
            cursor.execute("SELECT current_database()")
            database = str(cursor.fetchone()[0])
            allowed = database == EXPECTED_V2_DATABASE_NAME or (
                config.allow_disposable and database.startswith(DISPOSABLE_V2_PREFIX))
            if not allowed:
                raise RuntimeError(
                    f"realtime v2 pool targets the wrong database: expected {EXPECTED_V2_DATABASE_NAME}, "
                    f"got {database or 'UNKNOWN'}")
            cursor.execute("SELECT lineage_name, database_name::text FROM rt.database_identity")
            identity = cursor.fetchall()
            if [tuple(str(value) for value in row) for row in identity] != [(EXPECTED_V2_LINEAGE_NAME, database)]:
                raise RuntimeError("realtime v2 database identity does not match source")
            cursor.execute("SELECT phase FROM rt.security_phase_events ORDER BY event_id DESC LIMIT 1")
            row = cursor.fetchone()
            phase = row[0] if row else None
            required = _PHASE_FOR_MODE[config.security_mode]
            if phase != required:
                raise RuntimeError(
                    f"{config.security_mode} access requires security phase {required}, database is {phase}")
        return database
    finally:
        if not connection.closed:
            connection.rollback()
        pool.putconn(connection, close=bool(connection.closed))


class V2ConnectionProvider:
    def __init__(self, pools: Mapping[str, Any], config: V2AccessConfig) -> None:
        if not pools:
            raise ValueError("v2 connection provider requires at least one pool")
        valid = {OWNER_POOL, *PRINCIPAL_PROFILES}
        unknown = set(pools) - valid
        if unknown:
            raise ValueError(f"unknown v2 pool keys: {sorted(unknown)}")
        if config.security_mode == SECURITY_MODE_DEFERRED and set(pools) != {OWNER_POOL}:
            raise ValueError("deferred security uses a single owner pool")
        if config.security_mode == SECURITY_MODE_HARDENED and OWNER_POOL in pools:
            raise ValueError("hardened security must not hold an owner pool")
        self._pools = dict(pools)
        self._config = config
        self._roles = build_role_names_v2(config.role_prefix) if config.role_prefix else None
        self.role_violations: list[RoleViolation] = []

    @property
    def config(self) -> V2AccessConfig:
        return self._config

    def verify(self) -> str:
        databases = {verify_pool_target_v2(pool, self._config, None if key == OWNER_POOL else key)
                     for key, pool in self._pools.items()}
        if len(databases) != 1:
            raise RuntimeError("v2 pools target different databases")
        return databases.pop()

    def _check_role(self, capability_role: str, principal_profile: str) -> None:
        if capability_role not in CAPABILITY_ROLES:
            raise ValueError(f"unknown capability role {capability_role}")
        if principal_profile not in PRINCIPAL_PROFILES:
            raise ValueError(f"unknown principal profile {principal_profile}")
        if capability_role in PRINCIPAL_PROFILES[principal_profile]:
            return
        reason = f"{principal_profile} is not a member of {capability_role}"
        if self._config.security_mode == SECURITY_MODE_HARDENED:
            raise PermissionError(reason)
        self.role_violations.append(RoleViolation(capability_role, principal_profile, reason))

    @contextmanager
    def transaction(self, capability_role: str, principal_profile: str, *,
                    read_only: bool = False, isolation_level: str | None = None,
                    fence: CommitFence | None = None, dict_cursor: bool = False):
        self._check_role(capability_role, principal_profile)
        if fence is not None and read_only:
            raise ValueError("a read-only transaction has no writes to fence")
        isolation = isolation_level.strip().upper() if isolation_level else None
        if isolation not in _ISOLATION_LEVELS:
            raise ValueError("unsupported realtime transaction isolation level")
        deferred = self._config.security_mode == SECURITY_MODE_DEFERRED
        pool_key = OWNER_POOL if deferred else principal_profile
        pool = self._pools.get(pool_key)
        if pool is None:
            raise RuntimeError(f"no v2 pool is configured for {pool_key}")
        connection = pool.getconn()
        try:
            with (connection.cursor(cursor_factory=RealDictCursor) if dict_cursor
                  else connection.cursor()) as cursor:
                mode = "READ ONLY" if read_only else "READ WRITE"
                cursor.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation}, {mode}" if isolation
                               else f"SET TRANSACTION {mode}")
                if not deferred:
                    cursor.execute(sql.SQL("SET LOCAL ROLE {}").format(
                        sql.Identifier(self._roles.name(capability_role))))
                cursor.execute("SELECT set_config('statement_timeout', %s, true)",
                               (f"{self._config.statement_timeout_ms}ms",))
                yield V2UnitOfWork(capability_role, principal_profile, connection, cursor, read_only)
                if fence is not None:
                    fence.verify(cursor)
            connection.commit()
        except Exception:
            if not connection.closed:
                connection.rollback()
            raise
        finally:
            pool.putconn(connection, close=bool(connection.closed))
