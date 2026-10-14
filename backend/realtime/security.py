from __future__ import annotations

import re
from dataclasses import dataclass, fields
from typing import Iterable

from psycopg2 import sql

from .contracts import canonical_sha256


SECURITY_POLICY_VERSION = "realtime_security_v1"
_ROLE_PREFIX = re.compile(r"^[a-z][a-z0-9_]{2,30}$")

CAPABILITY_SUFFIXES = (
    "owner",
    "control",
    "ingress",
    "compute",
    "references",
    "publication",
    "reader",
    "maintenance",
)
PRINCIPAL_SUFFIXES = ("migrator", "runtime", "api")

PRINCIPAL_MEMBERSHIPS = {
    "migrator": ("owner",),
    "runtime": (
        "control",
        "ingress",
        "compute",
        "references",
        "publication",
        "maintenance",
    ),
    "api": ("reader",),
}

REFERENCE_TABLES = (
    "ref.securities",
    "ref.security_revisions",
    "ref.corporate_action_revisions",
    "ref.corporate_action_coverage",
    "ref.option_contracts",
    "ref.option_contract_versions",
    "ref.corporate_action_revisions",
    "ref.corporate_action_coverage",
    "ref.oi_baselines",
    "ref.market_event_revisions",
    "ref.market_event_coverage",
    "ref.fundamental_reports",
    "ref.fundamental_report_coverage",
    "ref.risk_free_rate_observations",
)
CONTROL_TABLES = (
    "rt.database_identity",
    "rt.database_security_policies",
    "rt.instruments",
    "rt.capability_profiles",
    "rt.capability_checks",
    "rt.subscription_manifests",
    "rt.subscription_members",
    "rt.option_manifest_selection_audits",
    "rt.option_manifest_contract_dispositions",
    "rt.option_manifest_coverage",
    "rt.cohorts",
    "rt.worker_leases",
    "rt.readiness_current",
    "rt.readiness_transitions",
    "rt.stream_sessions",
    "rt.stream_gaps",
    "rt.provider_tier_transitions",
    "rt.seed_manifests",
    "rt.seed_manifest_members",
    "rt.seed_current",
    "rt.provider_tier_cohorts",
    "rt.provider_tier_activations",
    "rt.portal_cohort_current",
)
ACQUISITION_TABLES = (
    "rt.developer_acquisition_batches",
    "rt.developer_acquisition_facts",
    "rt.developer_forward_inputs",
)
OPTION_DISCOVERY_TABLES = (
    "rt.option_discovery_batches",
    "rt.option_discovery_pages",
    "rt.option_discovery_coverage",
)
OPTION_SELECTION_TABLES = (
    "rt.option_manifest_selection_audits",
    "rt.option_manifest_contract_dispositions",
    "rt.option_manifest_coverage",
)
OPTION_STAGING_TABLES = (
    "rt.option_pre_session_runs",
    "rt.option_pre_session_work",
    "rt.option_pre_session_packages",
    "rt.option_pre_session_account_usage",
    "rt.option_pre_session_finalizations",
)
OPTION_STAGING_ACQUISITION_TABLES = OPTION_STAGING_TABLES[:4]
MATERIALIZATION_TABLES = (
    "rt.developer_forward_work",
    "rt.developer_forward_artifacts",
    "rt.developer_forward_releases",
    "rt.developer_forward_current",
    "rt.quote_current",
    "rt.trade_current",
    "rt.bar_revisions",
    "rt.bar_current",
    "rt.consumer_checkpoints",
    "rt.materialization_jobs",
    "rt.archive_segments",
    "rt.option_packages",
    "rt.option_package_legs",
    "rt.option_mark_evidence",
    "rt.option_package_mark_revisions",
    "rt.option_package_mark_inputs",
    "rt.option_package_mark_current",
    "rt.feature_definitions",
    "rt.equity_evidence_source_windows",
    "rt.equity_evidence",
    "rt.equity_evidence_dependencies",
    "rt.equity_evidence_inputs",
    "rt.feature_snapshots",
    "rt.feature_current",
    "rt.equity_evidence_current",
    "rt.detector_versions",
    "rt.detector_events",
    "rt.detector_event_inputs",
    "rt.detector_state",
    "rt.screening_generations",
    "rt.screening_rows",
    "rt.screening_current",
    "rt.detector_datasets",
    "rt.detector_evaluations",
    "rt.alert_management_assessments",
    "rt.alert_management_current",
    "rt.portal_snapshots",
    "rt.portal_snapshot_current",
    "rt.alert_current",
    "rt.alert_outbox",
)
OWNER_ONLY_ACTION_TABLES = (
    "rt.local_mock_requests",
    "rt.local_mock_request_inputs",
    "rt.local_mock_dispositions",
    "rt.local_mock_fills",
)
MIGRATION_TABLES = ("rt.schema_migrations",)
ALL_RUNTIME_TABLES = (
    REFERENCE_TABLES
    + CONTROL_TABLES
    + ACQUISITION_TABLES
    + OPTION_DISCOVERY_TABLES
    + OPTION_STAGING_TABLES
    + MATERIALIZATION_TABLES
)
ALL_DATABASE_TABLES = ALL_RUNTIME_TABLES + OWNER_ONLY_ACTION_TABLES + MIGRATION_TABLES

INGRESS_READ_TABLES = REFERENCE_TABLES + (
    "rt.database_identity",
    "rt.instruments",
    "rt.capability_profiles",
    "rt.capability_checks",
    "rt.subscription_manifests",
    "rt.subscription_members",
    "rt.cohorts",
    "rt.worker_leases",
    "rt.readiness_current",
    "rt.stream_sessions",
    "rt.stream_gaps",
    "rt.provider_tier_transitions",
    "rt.seed_manifests",
    "rt.seed_manifest_members",
    "rt.seed_current",
    "rt.provider_tier_cohorts",
    "rt.provider_tier_activations",
) + ACQUISITION_TABLES

API_READ_TABLES = (
    "rt.developer_forward_artifacts",
    "rt.developer_forward_releases",
    "rt.developer_forward_current",
    "ref.securities",
    "ref.security_revisions",
    "ref.corporate_action_revisions",
    "ref.corporate_action_coverage",
    "ref.option_contracts",
    "ref.option_contract_versions",
    "rt.database_identity",
    "rt.database_security_policies",
    "rt.instruments",
    "rt.subscription_manifests",
    "rt.subscription_members",
    "rt.cohorts",
    "rt.readiness_current",
    "rt.readiness_transitions",
    "rt.provider_tier_transitions",
    "rt.seed_manifests",
    "rt.seed_manifest_members",
    "rt.seed_current",
    "rt.provider_tier_cohorts",
    "rt.provider_tier_activations",
    "rt.portal_cohort_current",
    "rt.quote_current",
    "rt.trade_current",
    "rt.bar_revisions",
    "rt.bar_current",
    "rt.option_packages",
    "rt.option_package_legs",
    "rt.option_mark_evidence",
    "rt.option_package_mark_revisions",
    "rt.option_package_mark_inputs",
    "rt.option_package_mark_current",
    "rt.feature_definitions",
    "rt.equity_evidence_source_windows",
    "rt.equity_evidence",
    "rt.equity_evidence_dependencies",
    "rt.equity_evidence_inputs",
    "rt.feature_snapshots",
    "rt.feature_current",
    "rt.equity_evidence_current",
    "rt.detector_versions",
    "rt.detector_events",
    "rt.detector_event_inputs",
    "rt.detector_state",
    "rt.screening_generations",
    "rt.screening_rows",
    "rt.screening_current",
    "rt.detector_datasets",
    "rt.detector_evaluations",
    "rt.alert_management_assessments",
    "rt.alert_management_current",
    "rt.portal_snapshots",
    "rt.portal_snapshot_current",
    "rt.alert_current",
)

INSERT_GRANTS = {
    "control": (
        "rt.capability_profiles",
        "rt.capability_checks",
        "rt.subscription_manifests",
        "rt.subscription_members",
        "rt.option_manifest_selection_audits",
        "rt.option_manifest_contract_dispositions",
        "rt.option_manifest_coverage",
        "rt.cohorts",
        "rt.worker_leases",
        "rt.readiness_current",
        "rt.readiness_transitions",
        "rt.provider_tier_transitions",
        "rt.provider_tier_cohorts",
        "rt.provider_tier_activations",
        "rt.portal_cohort_current",
        "rt.option_pre_session_finalizations",
    ),
    "ingress": (
        "rt.worker_leases",
        "rt.stream_sessions",
        "rt.stream_gaps",
    ) + ACQUISITION_TABLES,
    "compute": (
        "rt.developer_forward_work",
        "rt.developer_forward_artifacts",
        "rt.worker_leases",
        "rt.readiness_current",
        "rt.quote_current",
        "rt.trade_current",
        "rt.bar_revisions",
        "rt.bar_current",
        "rt.consumer_checkpoints",
        "rt.materialization_jobs",
        "rt.option_packages",
        "rt.option_package_legs",
        "rt.option_mark_evidence",
        "rt.option_package_mark_revisions",
        "rt.option_package_mark_inputs",
        "rt.option_package_mark_current",
        "rt.feature_definitions",
        "rt.equity_evidence_source_windows",
        "rt.equity_evidence",
        "rt.equity_evidence_dependencies",
        "rt.equity_evidence_inputs",
        "rt.feature_snapshots",
        "rt.feature_current",
        "rt.equity_evidence_current",
        "rt.detector_versions",
        "rt.detector_events",
        "rt.detector_event_inputs",
        "rt.detector_state",
        "rt.screening_generations",
        "rt.screening_rows",
        "rt.screening_current",
        "rt.detector_datasets",
        "rt.detector_evaluations",
        "rt.alert_management_assessments",
        "rt.alert_management_current",
        "rt.alert_current",
        "rt.alert_outbox",
    ),
    "publication": (
        "rt.developer_forward_releases",
        "rt.developer_forward_current",
        "rt.portal_snapshots",
        "rt.portal_snapshot_current",
    ),
    "references": REFERENCE_TABLES
    + (
        "rt.instruments",
        "rt.seed_manifests",
        "rt.seed_manifest_members",
        "rt.seed_current",
    )
    + OPTION_DISCOVERY_TABLES
    + OPTION_STAGING_ACQUISITION_TABLES,
    "maintenance": (
        "rt.worker_leases",
        "rt.stream_gaps",
        "rt.materialization_jobs",
        "rt.archive_segments",
    ),
}
UPDATE_GRANTS = {
    "control": (
        "rt.worker_leases",
        "rt.readiness_current",
        "rt.portal_cohort_current",
    ),
    "ingress": ("rt.worker_leases", "rt.stream_sessions", "rt.stream_gaps"),
    "compute": (
        "rt.developer_forward_work",
        "rt.worker_leases",
        "rt.readiness_current",
        "rt.quote_current",
        "rt.trade_current",
        "rt.bar_current",
        "rt.consumer_checkpoints",
        "rt.materialization_jobs",
        "rt.option_package_mark_current",
        "rt.feature_current",
        "rt.equity_evidence_current",
        "rt.detector_state",
        "rt.screening_current",
        "rt.alert_management_current",
        "rt.alert_current",
    ),
    "publication": (
        "rt.developer_forward_current",
        "rt.portal_snapshot_current",
        "rt.alert_outbox",
    ),
    "references": ("rt.seed_current", "rt.option_pre_session_work", "rt.option_pre_session_account_usage"),
    "maintenance": (
        "rt.worker_leases",
        "rt.stream_gaps",
        "rt.materialization_jobs",
    ),
}
DELETE_GRANTS = {
    "control": ("rt.worker_leases",),
    "ingress": ("rt.worker_leases",),
    "compute": ("rt.worker_leases",),
    "maintenance": ("rt.worker_leases",),
}
SEQUENCE_GRANTS = {
    "control": ("rt.readiness_transitions_transition_id_seq",),
    "references": ("rt.instruments_instrument_id_seq",),
}


@dataclass(frozen=True, slots=True)
class RealtimeRoleNames:
    owner: str
    control: str
    ingress: str
    compute: str
    references: str
    publication: str
    reader: str
    maintenance: str
    migrator: str
    runtime: str
    api: str

    @property
    def capability_names(self) -> tuple[str, ...]:
        return tuple(getattr(self, suffix) for suffix in CAPABILITY_SUFFIXES)

    @property
    def principal_names(self) -> tuple[str, ...]:
        return tuple(getattr(self, suffix) for suffix in PRINCIPAL_SUFFIXES)

    @property
    def all_names(self) -> tuple[str, ...]:
        return self.capability_names + self.principal_names


def build_role_names(prefix: str) -> RealtimeRoleNames:
    normalized = prefix.strip()
    if not _ROLE_PREFIX.fullmatch(normalized):
        raise ValueError(
            "role prefix must be 3-31 lowercase letters, digits or underscores"
        )
    values = {
        field.name: f"{normalized}_{field.name}" for field in fields(RealtimeRoleNames)
    }
    if any(len(name) > 63 for name in values.values()):
        raise ValueError("generated PostgreSQL role name exceeds 63 characters")
    return RealtimeRoleNames(**values)


def security_policy() -> dict[str, object]:
    return {
        "version": SECURITY_POLICY_VERSION,
        "owner_role": "owner",
        "capability_roles": list(CAPABILITY_SUFFIXES[1:]),
        "principal_profiles": {
            key: list(value) for key, value in PRINCIPAL_MEMBERSHIPS.items()
        },
        "public_access": "NONE",
        "object_grants": "EXPLICIT_TABLE_ALLOWLIST_V1",
    }


def security_policy_sha256() -> str:
    return canonical_sha256(security_policy())


def _identifier_list(names: Iterable[str]) -> sql.Composed:
    return sql.SQL(", ").join(sql.Identifier(name) for name in names)


def _relation(name: str) -> sql.Composed:
    schema_name, relation_name = name.split(".", 1)
    return sql.SQL("{}.{}").format(
        sql.Identifier(schema_name),
        sql.Identifier(relation_name),
    )


def _relation_list(names: Iterable[str]) -> sql.Composed:
    return sql.SQL(", ").join(_relation(name) for name in names)


def _role_exists(cursor, name: str) -> tuple[bool, bool]:
    cursor.execute(
        "SELECT rolcanlogin FROM pg_roles WHERE rolname = %s",
        (name,),
    )
    row = cursor.fetchone()
    return (row is not None, bool(row[0]) if row is not None else False)


def create_disposable_roles(connection, roles: RealtimeRoleNames) -> None:
    if not all(name.startswith("test_rt_") for name in roles.all_names):
        raise ValueError("disposable roles must use the test_rt_ prefix")
    with connection.cursor() as cursor:
        existing = [name for name in roles.all_names if _role_exists(cursor, name)[0]]
        if existing:
            raise RuntimeError(
                "disposable role already exists; refusing ownership: "
                + ", ".join(existing)
            )
        for name in roles.all_names:
            inheritance = (
                sql.SQL(" NOINHERIT")
                if name == roles.runtime
                else sql.SQL("")
            )
            cursor.execute(
                sql.SQL("CREATE ROLE {} NOLOGIN{}").format(
                    sql.Identifier(name),
                    inheritance,
                )
            )
        for principal_suffix, capability_suffixes in PRINCIPAL_MEMBERSHIPS.items():
            principal = getattr(roles, principal_suffix)
            for capability_suffix in capability_suffixes:
                capability = getattr(roles, capability_suffix)
                cursor.execute(
                    sql.SQL("GRANT {} TO {}").format(
                        sql.Identifier(capability),
                        sql.Identifier(principal),
                    )
                )


def ensure_capability_roles(connection, roles: RealtimeRoleNames) -> None:
    with connection.cursor() as cursor:
        for name in roles.capability_names:
            exists, can_login = _role_exists(cursor, name)
            if exists and can_login:
                raise RuntimeError(f"capability role unexpectedly has LOGIN: {name}")
            if not exists:
                cursor.execute(
                    sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(name))
                )


def grant_principal_memberships(connection, roles: RealtimeRoleNames) -> None:
    with connection.cursor() as cursor:
        for principal_suffix, capability_suffixes in PRINCIPAL_MEMBERSHIPS.items():
            principal = getattr(roles, principal_suffix)
            for capability_suffix in capability_suffixes:
                capability = getattr(roles, capability_suffix)
                cursor.execute(
                    sql.SQL("GRANT {} TO {}").format(
                        sql.Identifier(capability),
                        sql.Identifier(principal),
                    )
                )


def validate_production_roles(
    connection,
    roles: RealtimeRoleNames,
    *,
    allow_locked_principals: bool = False,
) -> None:
    with connection.cursor() as cursor:
        for name in roles.capability_names:
            exists, can_login = _role_exists(cursor, name)
            if not exists or can_login:
                raise RuntimeError(
                    f"capability role must exist with NOLOGIN: {name}"
                )
        for name in roles.principal_names:
            exists, can_login = _role_exists(cursor, name)
            if not exists or (not can_login and not allow_locked_principals):
                raise RuntimeError(f"principal role must exist with LOGIN: {name}")
        cursor.execute(
            "SELECT rolinherit FROM pg_roles WHERE rolname = %s",
            (roles.runtime,),
        )
        runtime = cursor.fetchone()
        if runtime is None or runtime[0]:
            raise RuntimeError(
                f"runtime principal role must use NOINHERIT: {roles.runtime}"
            )


def _grant_tables(cursor, privilege: str, role: str, tables: tuple[str, ...]) -> None:
    if not tables:
        return
    cursor.execute(
        sql.SQL("GRANT {} ON TABLE {} TO {}").format(
            sql.SQL(privilege),
            _relation_list(tables),
            sql.Identifier(role),
        )
    )


def _revoke_tables(cursor, privilege: str, role: str, tables: tuple[str, ...]) -> None:
    if not tables:
        return
    cursor.execute(
        sql.SQL("REVOKE {} ON TABLE {} FROM {}").format(
            sql.SQL(privilege),
            _relation_list(tables),
            sql.Identifier(role),
        )
    )


def apply_database_security(connection, roles: RealtimeRoleNames) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT policy FROM rt.database_security_policies "
            "WHERE policy_version = %s",
            (SECURITY_POLICY_VERSION,),
        )
        row = cursor.fetchone()
        if row is None or row[0] != security_policy():
            raise RuntimeError("database security policy does not match source")
        cursor.execute("SELECT current_database()")
        database_name = cursor.fetchone()[0]
        cursor.execute(
            sql.SQL("ALTER DATABASE {} OWNER TO {}").format(
                sql.Identifier(database_name),
                sql.Identifier(roles.owner),
            )
        )
        for schema_name in ("ref", "rt"):
            cursor.execute(
                sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(roles.owner),
                )
            )
        cursor.execute(
            "SELECT n.nspname, c.relname, c.relkind "
            "FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace "
            "WHERE n.nspname IN ('ref', 'rt') "
            "AND c.relkind IN ('r', 'p', 'S') ORDER BY n.nspname, c.relname"
        )
        for schema_name, object_name, kind in cursor.fetchall():
            object_kind = "SEQUENCE" if kind == "S" else "TABLE"
            cursor.execute(
                sql.SQL("ALTER {} {}.{} OWNER TO {}").format(
                    sql.SQL(object_kind),
                    sql.Identifier(schema_name),
                    sql.Identifier(object_name),
                    sql.Identifier(roles.owner),
                )
            )
        cursor.execute(
            sql.SQL(
                "ALTER FUNCTION rt.reject_immutable_change() OWNER TO {}"
            ).format(sql.Identifier(roles.owner))
        )
        cursor.execute(
            sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(
                sql.Identifier(database_name)
            )
        )
        cursor.execute("REVOKE ALL ON SCHEMA public, ref, rt FROM PUBLIC")
        cursor.execute(
            "REVOKE ALL ON ALL TABLES IN SCHEMA ref, rt FROM PUBLIC"
        )
        cursor.execute(
            "REVOKE ALL ON ALL SEQUENCES IN SCHEMA ref, rt FROM PUBLIC"
        )
        cursor.execute(
            "REVOKE ALL ON ALL FUNCTIONS IN SCHEMA ref, rt FROM PUBLIC"
        )
        cursor.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(database_name),
                _identifier_list(roles.principal_names),
            )
        )
        runtime_capabilities = roles.capability_names[1:]
        cursor.execute(
            sql.SQL("GRANT USAGE ON SCHEMA ref, rt TO {}").format(
                _identifier_list(runtime_capabilities)
            )
        )
        _revoke_tables(cursor, "SELECT", roles.ingress, ALL_RUNTIME_TABLES)
        for suffix in ("control", "compute", "references", "publication", "maintenance"):
            _grant_tables(
                cursor,
                "SELECT",
                getattr(roles, suffix),
                ALL_RUNTIME_TABLES,
            )
        _grant_tables(cursor, "SELECT", roles.ingress, INGRESS_READ_TABLES)
        _grant_tables(cursor, "SELECT", roles.reader, API_READ_TABLES)
        for suffix, tables in INSERT_GRANTS.items():
            _grant_tables(cursor, "INSERT", getattr(roles, suffix), tables)
        for suffix, tables in UPDATE_GRANTS.items():
            _grant_tables(cursor, "UPDATE", getattr(roles, suffix), tables)
        for suffix, tables in DELETE_GRANTS.items():
            _grant_tables(cursor, "DELETE", getattr(roles, suffix), tables)
        for suffix, sequences in SEQUENCE_GRANTS.items():
            cursor.execute(
                sql.SQL("GRANT USAGE, SELECT ON SEQUENCE {} TO {}").format(
                    _relation_list(sequences),
                    sql.Identifier(getattr(roles, suffix)),
                )
            )
        cursor.execute(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA ref "
                "REVOKE ALL ON TABLES FROM PUBLIC"
            ).format(sql.Identifier(roles.owner))
        )
        cursor.execute(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA rt "
                "REVOKE ALL ON TABLES FROM PUBLIC"
            ).format(sql.Identifier(roles.owner))
        )


def audit_database_security(connection, roles: RealtimeRoleNames) -> dict[str, object]:
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_database()")
        database_name = cursor.fetchone()[0]
        cursor.execute(
            "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = %s",
            (database_name,),
        )
        database_owner = cursor.fetchone()[0]
        cursor.execute(
            "SELECT nspname, pg_get_userbyid(nspowner) FROM pg_namespace "
            "WHERE nspname IN ('ref', 'rt') ORDER BY nspname"
        )
        schema_owners = dict(cursor.fetchall())

        def table_privilege(role: str, table: str, privilege: str) -> bool:
            cursor.execute(
                "SELECT has_table_privilege(%s, %s, %s)",
                (role, table, privilege),
            )
            return bool(cursor.fetchone()[0])

        def schema_privilege(role: str, schema_name: str, privilege: str) -> bool:
            cursor.execute(
                "SELECT has_schema_privilege(%s, %s, %s)",
                (role, schema_name, privilege),
            )
            return bool(cursor.fetchone()[0])

        checks = {
            "database_owner": database_owner == roles.owner,
            "ref_owner": schema_owners.get("ref") == roles.owner,
            "rt_owner": schema_owners.get("rt") == roles.owner,
            "reader_reads_readiness": table_privilege(
                roles.reader, "rt.readiness_current", "SELECT"
            ),
            "api_cannot_write_streams": not table_privilege(
                roles.api, "rt.stream_sessions", "INSERT"
            ),
            "api_cannot_create": not schema_privilege(roles.api, "rt", "CREATE"),
            "ingress_writes_streams": table_privilege(
                roles.ingress, "rt.stream_sessions", "INSERT"
            ),
            "ingress_writes_acquisition_batches": table_privilege(
                roles.ingress, "rt.developer_acquisition_batches", "INSERT"
            ),
            "ingress_reads_acquisition_facts": table_privilege(
                roles.ingress, "rt.developer_acquisition_facts", "SELECT"
            ),
            "references_write_discovery_batches": table_privilege(
                roles.references, "rt.option_discovery_batches", "INSERT"
            ),
            "references_write_discovery_pages": table_privilege(
                roles.references, "rt.option_discovery_pages", "INSERT"
            ),
            "references_write_discovery_coverage": table_privilege(
                roles.references, "rt.option_discovery_coverage", "INSERT"
            ),
            "ingress_cannot_write_discovery": not table_privilege(
                roles.ingress, "rt.option_discovery_batches", "INSERT"
            ),
            "control_cannot_write_discovery": not table_privilege(
                roles.control, "rt.option_discovery_batches", "INSERT"
            ),
            "compute_cannot_write_discovery": not table_privilege(
                roles.compute, "rt.option_discovery_batches", "INSERT"
            ),
            "reader_cannot_read_discovery": not table_privilege(
                roles.reader, "rt.option_discovery_batches", "SELECT"
            ),
            "control_writes_selection_audits": table_privilege(
                roles.control, "rt.option_manifest_selection_audits", "INSERT"
            ),
            "control_writes_selection_dispositions": table_privilege(
                roles.control, "rt.option_manifest_contract_dispositions", "INSERT"
            ),
            "control_writes_selection_coverage": table_privilege(
                roles.control, "rt.option_manifest_coverage", "INSERT"
            ),
            "references_cannot_write_selection": not table_privilege(
                roles.references, "rt.option_manifest_selection_audits", "INSERT"
            ),
            "ingress_cannot_read_selection": not table_privilege(
                roles.ingress, "rt.option_manifest_selection_audits", "SELECT"
            ),
            "reader_cannot_read_selection": not table_privilege(
                roles.reader, "rt.option_manifest_selection_audits", "SELECT"
            ),
            "compute_reads_acquisition_facts": table_privilege(
                roles.compute, "rt.developer_acquisition_facts", "SELECT"
            ),
            "ingress_cannot_read_bar_current": not table_privilege(
                roles.ingress, "rt.bar_current", "SELECT"
            ),
            "ingress_cannot_read_option_marks": not table_privilege(
                roles.ingress, "rt.option_mark_evidence", "SELECT"
            ),
            "ingress_cannot_read_features": not table_privilege(
                roles.ingress, "rt.feature_current", "SELECT"
            ),
            "ingress_cannot_read_detectors": not table_privilege(
                roles.ingress, "rt.detector_events", "SELECT"
            ),
            "ingress_cannot_read_portal": not table_privilege(
                roles.ingress, "rt.portal_snapshots", "SELECT"
            ),
            "runtime_requires_capability_role": not table_privilege(
                roles.runtime, "rt.stream_sessions", "INSERT"
            ),
            "runtime_cannot_create": not schema_privilege(
                roles.runtime, "rt", "CREATE"
            ),
            "public_cannot_use_rt": not schema_privilege("public", "rt", "USAGE"),
        }
        cursor.execute(
            "SELECT pg_has_role(%s, %s, 'MEMBER')",
            (roles.migrator, roles.owner),
        )
        checks["migrator_can_assume_owner"] = bool(cursor.fetchone()[0])
        cursor.execute(
            "SELECT NOT rolinherit FROM pg_roles WHERE rolname = %s",
            (roles.runtime,),
        )
        runtime_noinherit = cursor.fetchone()
        checks["runtime_noinherit"] = bool(
            runtime_noinherit is not None and runtime_noinherit[0]
        )
    failed = sorted(name for name, passed in checks.items() if not passed)
    if failed:
        raise RuntimeError("realtime database security audit failed: " + ", ".join(failed))
    return {
        "policy_version": SECURITY_POLICY_VERSION,
        "policy_sha256": security_policy_sha256(),
        "database_owner": database_owner,
        "checks": checks,
    }


def drop_disposable_roles(connection, roles: RealtimeRoleNames) -> None:
    if not all(name.startswith("test_rt_") for name in roles.all_names):
        raise ValueError("refusing to drop non-disposable roles")
    with connection.cursor() as cursor:
        for principal_suffix, capability_suffixes in PRINCIPAL_MEMBERSHIPS.items():
            principal = getattr(roles, principal_suffix)
            for capability_suffix in capability_suffixes:
                capability = getattr(roles, capability_suffix)
                cursor.execute(
                    sql.SQL("REVOKE {} FROM {}").format(
                        sql.Identifier(capability),
                        sql.Identifier(principal),
                    )
                )
        for name in roles.principal_names + tuple(reversed(roles.capability_names)):
            cursor.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(name)))