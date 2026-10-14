from __future__ import annotations

from contextlib import closing
from typing import Mapping

import psycopg2

from realtime.bootstrap_foundation import (
    BootstrapProvisionState,
    BootstrapRoleState,
    execute_migrations_stage,
    execute_provision_stage,
    execute_security_stage,
)
from realtime.bootstrap_manifest import (
    BootstrapStageName,
    RealtimeBootstrapPlan,
    RealtimeBootstrapStagePlan,
)
from realtime.migrations import RealtimeMigration
from realtime.security import (
    RealtimeRoleNames,
    apply_database_security,
    audit_database_security,
    ensure_capability_roles,
    grant_principal_memberships,
    validate_production_roles,
)
from scripts import (
    apply_realtime_migrations,
    apply_realtime_security,
    provision_realtime_database,
)


class ScriptProvisionOperations:
    def __init__(self, settings: Mapping[str, str]) -> None:
        self.settings = dict(settings)

    def status(self, database: str, roles: RealtimeRoleNames) -> BootstrapProvisionState:
        state = provision_realtime_database._provisioning_status(
            self.settings,
            database,
            roles,
        )
        return BootstrapProvisionState(
            database_exists=bool(state["database_exists"]),
            roles=tuple(
                BootstrapRoleState(
                    name=str(role["name"]),
                    login=bool(role["login"]),
                    inherit=bool(role["inherit"]),
                )
                for role in state["existing_roles"]
            ),
            missing_local_secret_names=tuple(state["missing_local_secret_names"]),
        )

    def create_principals(self, roles: RealtimeRoleNames) -> None:
        provision_realtime_database._create_principals(
            self.settings,
            roles,
            provision_realtime_database._principal_passwords(),
        )

    def drop_created_principals(self, roles: RealtimeRoleNames) -> None:
        provision_realtime_database._drop_new_principals(self.settings, roles)

    def create_database(self, database: str) -> None:
        provision_realtime_database._create_database(self.settings, database)


class ScriptMigrationOperations:
    def __init__(self, settings: Mapping[str, str]) -> None:
        self.settings = dict(settings)

    def _connection(self, database: str):
        return psycopg2.connect(
            dbname=database,
            user=self.settings["POSTGRES_ADMIN_USER"],
            password=self.settings["POSTGRES_ADMIN_PASSWORD"],
            host=self.settings["DB_HOST"],
            port=self.settings["DB_PORT"],
            application_name="realtime_bootstrap_migrations",
            options="-c timezone=UTC",
        )

    def read_applied(self, database: str) -> Mapping[str, str]:
        with closing(self._connection(database)) as connection:
            return apply_realtime_migrations._read_applied(connection)

    def apply_migration(self, database: str, migration: RealtimeMigration) -> None:
        with closing(self._connection(database)) as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SET LOCAL statement_timeout = '30s'")
                    cursor.execute("SET LOCAL lock_timeout = '5s'")
                    cursor.execute(migration.sql)
                    cursor.execute(
                        "INSERT INTO rt.schema_migrations "
                        "(version, checksum_sha256) VALUES (%s, %s)",
                        (migration.version, migration.checksum_sha256),
                    )
                connection.commit()
            except Exception:
                connection.rollback()
                raise


class ScriptSecurityOperations:
    def __init__(self, settings: Mapping[str, str]) -> None:
        self.settings = dict(settings)

    def apply_and_audit(
        self,
        database: str,
        roles: RealtimeRoleNames,
    ) -> Mapping[str, object]:
        with closing(psycopg2.connect(
            dbname=database,
            user=self.settings["POSTGRES_ADMIN_USER"],
            password=self.settings["POSTGRES_ADMIN_PASSWORD"],
            host=self.settings["DB_HOST"],
            port=self.settings["DB_PORT"],
            application_name="realtime_bootstrap_security",
            options="-c timezone=UTC",
        )) as connection:
            try:
                ensure_capability_roles(connection, roles)
                validate_production_roles(
                    connection,
                    roles,
                    allow_locked_principals=True,
                )
                grant_principal_memberships(connection, roles)
                apply_database_security(connection, roles)
                report = audit_database_security(connection, roles)
                connection.commit()
                return report
            except Exception:
                connection.rollback()
                raise


class ScriptFoundationStageExecutor:
    def __init__(
        self,
        *,
        provision_operations=None,
        migration_operations=None,
        security_operations=None,
    ) -> None:
        settings = None
        if provision_operations is None:
            settings = provision_realtime_database._admin_settings()
            provision_operations = ScriptProvisionOperations(settings)
        if migration_operations is None:
            settings = settings or apply_realtime_migrations._settings()
            migration_operations = ScriptMigrationOperations(settings)
        if security_operations is None:
            settings = settings or apply_realtime_security._settings()
            security_operations = ScriptSecurityOperations(settings)
        self.provision_operations = provision_operations
        self.migration_operations = migration_operations
        self.security_operations = security_operations

    def execute(
        self,
        stage: RealtimeBootstrapStagePlan,
        plan: RealtimeBootstrapPlan,
    ) -> Mapping[str, object]:
        if stage.name is BootstrapStageName.PROVISION:
            return execute_provision_stage(plan, self.provision_operations)
        if stage.name is BootstrapStageName.MIGRATIONS:
            return execute_migrations_stage(plan, self.migration_operations)
        if stage.name is BootstrapStageName.SECURITY:
            return execute_security_stage(plan, self.security_operations)
        raise NotImplementedError(
            f"bootstrap stage adapter is not implemented: {stage.name.value}"
        )
