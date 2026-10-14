from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Protocol

from .bootstrap_manifest import RealtimeBootstrapPlan
from .migrations import load_realtime_migrations, plan_realtime_migrations
from .security import RealtimeRoleNames, build_role_names, security_policy_sha256


@dataclass(frozen=True, slots=True)
class BootstrapRoleState:
    name: str
    login: bool
    inherit: bool


@dataclass(frozen=True, slots=True)
class BootstrapProvisionState:
    database_exists: bool
    roles: tuple[BootstrapRoleState, ...]
    missing_local_secret_names: tuple[str, ...]


class BootstrapProvisionOperations(Protocol):
    def status(self, database: str, roles: RealtimeRoleNames) -> BootstrapProvisionState: ...
    def create_principals(self, roles: RealtimeRoleNames) -> None: ...
    def drop_created_principals(self, roles: RealtimeRoleNames) -> None: ...
    def create_database(self, database: str) -> None: ...


class BootstrapMigrationOperations(Protocol):
    def read_applied(self, database: str) -> Mapping[str, str]: ...
    def apply_migration(self, database: str, migration) -> None: ...


class BootstrapSecurityOperations(Protocol):
    def apply_and_audit(
        self,
        database: str,
        roles: RealtimeRoleNames,
    ) -> Mapping[str, object]: ...


def _role_names(state: BootstrapProvisionState) -> frozenset[str]:
    return frozenset(role.name for role in state.roles)


def execute_provision_stage(
    plan: RealtimeBootstrapPlan,
    operations: BootstrapProvisionOperations,
) -> dict[str, object]:
    roles = build_role_names(plan.role_prefix)
    before = operations.status(plan.database, roles)
    if before.missing_local_secret_names:
        raise RuntimeError("realtime principal secret set is incomplete")
    existing = _role_names(before)
    principals = frozenset(roles.principal_names)
    unexpected = existing - frozenset(roles.all_names)
    if unexpected:
        raise RuntimeError("realtime provision status returned unexpected roles")
    present_principals = existing & principals
    if present_principals not in {frozenset(), principals}:
        raise RuntimeError("realtime principal set is partially provisioned")
    for role in before.roles:
        if role.name in roles.capability_names and role.login:
            raise RuntimeError("realtime capability role unexpectedly has LOGIN")
        if role.name == roles.runtime and role.inherit:
            raise RuntimeError("realtime runtime principal must use NOINHERIT")
    principals_created = False
    database_created = False
    if not before.database_exists:
        if not present_principals:
            operations.create_principals(roles)
            principals_created = True
        try:
            operations.create_database(plan.database)
            database_created = True
        except Exception:
            if principals_created:
                operations.drop_created_principals(roles)
            raise
    elif present_principals != principals:
        raise RuntimeError("existing realtime database has no complete principal set")
    after = operations.status(plan.database, roles)
    after_names = _role_names(after)
    if (
        not after.database_exists
        or not principals.issubset(after_names)
        or after.missing_local_secret_names
    ):
        raise RuntimeError("realtime provision stage did not reconcile its target")
    principal_states = {
        role.name: {"login": role.login, "inherit": role.inherit}
        for role in after.roles
        if role.name in principals
    }
    return {
        "database": plan.database,
        "database_created": database_created,
        "principals_created": principals_created,
        "principal_count": len(principals),
        "capability_role_count": len(after_names - principals),
        "principal_states": principal_states,
        "provider_requests": 0,
    }


def execute_migrations_stage(
    plan: RealtimeBootstrapPlan,
    operations: BootstrapMigrationOperations,
) -> dict[str, object]:
    catalog = load_realtime_migrations()
    before = plan_realtime_migrations(
        plan.database,
        operations.read_applied(plan.database),
        catalog,
    )
    applied_now = []
    for migration in before.pending:
        operations.apply_migration(plan.database, migration)
        applied_now.append(migration.version)
    after = plan_realtime_migrations(
        plan.database,
        operations.read_applied(plan.database),
        catalog,
    )
    if after.pending:
        raise RuntimeError("realtime migrations remain pending after apply")
    return {
        "database": plan.database,
        "schema_sha256": after.schema_sha256,
        "applied_versions": after.applied_versions,
        "applied_now": tuple(applied_now),
        "pending_versions": (),
        "provider_requests": 0,
    }


def execute_security_stage(
    plan: RealtimeBootstrapPlan,
    operations: BootstrapSecurityOperations,
) -> dict[str, object]:
    roles = build_role_names(plan.role_prefix)
    report = dict(operations.apply_and_audit(plan.database, roles))
    if report.get("policy_sha256") != security_policy_sha256():
        raise RuntimeError("realtime security policy hash changed after apply")
    checks = report.get("checks")
    if not isinstance(checks, Mapping) or not checks or not all(checks.values()):
        raise RuntimeError("realtime security checks are incomplete after apply")
    return {
        "database": plan.database,
        "policy_version": report.get("policy_version"),
        "policy_sha256": report["policy_sha256"],
        "database_owner": report.get("database_owner"),
        "security_check_count": len(checks),
        "provider_requests": 0,
    }
