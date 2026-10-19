"""Exact equity-only compatibility contract for the accepted V2 schema through 024."""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping

from .contracts import canonical_sha256
from .migrations import (
    V2_MIGRATIONS_DIR,
    RealtimeMigration,
    load_realtime_migrations,
    schema_fingerprint,
)
from .relation_registry import RELATIONS, grants, security_policy


CONTRACT_ID = "equity_live024_compatibility_v1"
LIVE024_SCHEMA_SHA256 = (
    "5b25f185dd703cb84ef1553e7f9ac5dc9e5c9ed530cee06823079bfedc97f6ef"
)
LIVE024_SECURITY_POLICY_SHA256 = (
    "97c10c03fb69574e3bbd1d886250da5d5aba1bcb3c0920ef4b8fa5b023262abc"
)
LIVE024_REGISTRY_SHA256 = (
    "053b3208454832de91b929fdd90fb1e6c7a2d5323251e6ba9e55a7c021f7210d"
)
LIVE024_MIGRATION_VERSIONS = (
    "000_realtime_baseline",
    "001_realtime_security_policy",
    "002_realtime_kept_tables",
    "003_realtime_control_plane",
    "004_realtime_orchestration",
    "005_realtime_acquisition_bars",
    "006_realtime_evidence",
    "007_realtime_portal",
    "008_realtime_options_flow",
    "009_realtime_partition_maintenance",
    "010_realtime_kept_constraints",
    "011_realtime_session_close_coverage",
    "012_realtime_session_close_terminal_contract",
    "013_realtime_session_close_pointer_order",
    "014_realtime_provider_budget_reservations",
    "015_realtime_acquisition_watermark_progress",
    "016_realtime_product_scoped_tier_bindings",
    "017_realtime_advanced_stream_accounting",
    "018_realtime_advanced_stage_catalog",
    "019_realtime_active_capability_profiles",
    "020_realtime_advanced_reference_scope",
    "021_realtime_cohort_history_bindings",
    "022_realtime_advanced_rest_repair_bars",
    "023_realtime_advanced_stream_overlay",
    "024_realtime_session_archive_build_identity",
)
POST024_RELATIONS = frozenset({
    "rt.advanced_options_flow_package_evidence",
    "rt.advanced_options_reference_cycle_bindings",
    "rt.advanced_options_reference_session_freezes",
    "rt.advanced_options_reference_state_members",
    "rt.advanced_options_reference_states",
    "rt.advanced_options_rolling_checkpoints",
    "rt.option_flow_cluster_members",
    "rt.option_flow_contract_scope_members",
    "rt.option_flow_contract_scopes",
    "rt.option_flow_event_profiles",
    "rt.option_flow_policies",
    "rt.option_source_flows",
})
SYMBOLS = ("SPY",)
CHANNELS = ("AM",)
ENABLED_STAGES = (
    "EQUITY_INPUT_GATE",
    "EQUITY_MATERIALIZE",
    "EQUITY_PROJECTION",
)
OPERATOR_PATHS = (
    "scripts/preflight_realtime_v2_advanced_canary.py",
    "scripts/provision_realtime_v2_advanced_equity_scope.py",
    "scripts/qualify_realtime_v2_advanced_docker.py",
    "realtime/equity_live024_compatibility.py",
    "realtime/security_v2.py",
    "tests/realtime/v2_database.py",
    "tests/realtime/test_v2_pipeline_rehearsal_db.py",
)
RUNTIME_PATHS = (
    "realtime/advanced_equity_control_plane.py",
    "realtime/advanced_equity_journal_bridge.py",
    "realtime/advanced_gateway.py",
    "realtime/advanced_ingress_plan.py",
    "realtime/advanced_ingress_repository.py",
    "realtime/advanced_ingress_runtime_profile.py",
    "realtime/advanced_scheduler.py",
    "realtime/equity_live024_compatibility.py",
    "realtime/normalizer.py",
    "realtime/orchestration_repository.py",
    "realtime/relation_registry.py",
    "realtime/stage_runners_v2.py",
    "scripts/run_realtime_v2_advanced_qualification_worker.py",
)


@dataclass(frozen=True, slots=True)
class Live024RegistryContract:
    relation_names: tuple[str, ...]
    object_grants: Mapping[str, Mapping[str, tuple[str, ...]]]
    security_policy: Mapping[str, object]
    sha256: str


def live024_migrations(
    migrations_dir: Path = V2_MIGRATIONS_DIR,
) -> tuple[RealtimeMigration, ...]:
    catalog = load_realtime_migrations(migrations_dir)
    selected = catalog[: len(LIVE024_MIGRATION_VERSIONS)]
    if tuple(migration.version for migration in selected) != LIVE024_MIGRATION_VERSIONS:
        raise RuntimeError("source does not contain the exact live-024 migration vector")
    if schema_fingerprint(selected) != LIVE024_SCHEMA_SHA256:
        raise RuntimeError("source live-024 migration checksums differ from the accepted schema")
    return selected


def live024_migration_rows(
    migrations_dir: Path = V2_MIGRATIONS_DIR,
) -> tuple[dict[str, str], ...]:
    return tuple({
        "version": migration.version,
        "checksum_sha256": migration.checksum_sha256,
    } for migration in live024_migrations(migrations_dir))


def _registry_identity() -> dict[str, object]:
    unexpected_missing = POST024_RELATIONS - RELATIONS.keys()
    if unexpected_missing:
        raise RuntimeError(
            "source registry lacks known post-024 relations: "
            + ", ".join(sorted(unexpected_missing))
        )
    baseline = {
        name: spec
        for name, spec in RELATIONS.items()
        if name not in POST024_RELATIONS
    }
    baseline_grants = {
        name: privileges
        for name, privileges in grants().items()
        if name in baseline
    }
    return {
        "contract_id": CONTRACT_ID,
        "relations": {
            name: asdict(spec)
            for name, spec in sorted(baseline.items())
        },
        "grants": {
            name: {
                privilege: tuple(role_names)
                for privilege, role_names in sorted(privileges.items())
            }
            for name, privileges in sorted(baseline_grants.items())
        },
        "security_policy": security_policy(),
    }


def live024_registry_contract() -> Live024RegistryContract:
    identity = _registry_identity()
    digest = canonical_sha256(identity)
    if digest != LIVE024_REGISTRY_SHA256:
        raise RuntimeError("source registry no longer matches the pinned live-024 contract")
    return Live024RegistryContract(
        relation_names=tuple(identity["relations"]),
        object_grants=identity["grants"],
        security_policy=identity["security_policy"],
        sha256=digest,
    )


def source_bundle_identity(
    backend_dir: Path,
    relative_paths: tuple[str, ...],
) -> dict[str, object]:
    files = {}
    for relative_path in relative_paths:
        path = backend_dir / Path(relative_path)
        if not path.is_file():
            raise RuntimeError(f"bound source file is unavailable: {relative_path}")
        files[relative_path] = hashlib.sha256(path.read_bytes()).hexdigest()
    return {
        "files": files,
        "sha256": canonical_sha256(files),
    }


def contract_metadata(backend_dir: Path) -> dict[str, object]:
    registry = live024_registry_contract()
    return {
        "contract_id": CONTRACT_ID,
        "schema_sha256": LIVE024_SCHEMA_SHA256,
        "migrations": live024_migration_rows(),
        "registry_sha256": registry.sha256,
        "security_policy_sha256": LIVE024_SECURITY_POLICY_SHA256,
        "security_policy_document_sha256": canonical_sha256(
            registry.security_policy
        ),
        "symbols": SYMBOLS,
        "channels": CHANNELS,
        "provider_tier": "MASSIVE_ADVANCED",
        "feed": "STOCKS",
        "options_pipeline": "DISABLED",
        "target": 1,
        "stage_catalog_version": "stages_v3",
        "enabled_stages": ENABLED_STAGES,
        "operator": source_bundle_identity(backend_dir, OPERATOR_PATHS),
        "runtime": source_bundle_identity(backend_dir, RUNTIME_PATHS),
    }


def _normalized_rows(cursor, statement: str) -> tuple[tuple[object, ...], ...]:
    cursor.execute(statement)
    return tuple(
        tuple(row.values()) if isinstance(row, Mapping) else tuple(row)
        for row in cursor.fetchall()
    )


def _row_values(row) -> tuple[object, ...]:
    return tuple(row.values()) if isinstance(row, Mapping) else tuple(row)


def schema_inventory_cursor(cursor) -> dict[str, object]:
    inventory = {
        "relations": _normalized_rows(
            cursor,
            "SELECT n.nspname,c.relname,c.relkind "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname IN ('ref','rt') "
            "AND c.relkind IN ('r','p','v','m','S') "
            "ORDER BY 1,2,3",
        ),
        "columns": _normalized_rows(
            cursor,
            "SELECT table_schema,table_name,column_name,ordinal_position,"
            "data_type,udt_schema,udt_name,is_nullable,column_default "
            "FROM information_schema.columns "
            "WHERE table_schema IN ('ref','rt') ORDER BY 1,2,4",
        ),
        "constraints": _normalized_rows(
            cursor,
            "SELECT n.nspname,c.relname,k.conname,k.contype,"
            "pg_get_constraintdef(k.oid,true) "
            "FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname IN ('ref','rt') ORDER BY 1,2,3",
        ),
        "indexes": _normalized_rows(
            cursor,
            "SELECT schemaname,tablename,indexname,indexdef "
            "FROM pg_indexes WHERE schemaname IN ('ref','rt') "
            "ORDER BY 1,2,3",
        ),
        "functions": _normalized_rows(
            cursor,
            "SELECT n.nspname,p.oid::regprocedure::text,"
            "pg_get_functiondef(p.oid) "
            "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
            "WHERE n.nspname IN ('ref','rt') ORDER BY 1,2",
        ),
        "triggers": _normalized_rows(
            cursor,
            "SELECT n.nspname,c.relname,t.tgname,pg_get_triggerdef(t.oid,true) "
            "FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname IN ('ref','rt') AND NOT t.tgisinternal "
            "ORDER BY 1,2,3",
        ),
    }
    return {**inventory, "sha256": canonical_sha256(inventory)}


def acl_inventory_cursor(
    cursor,
    *,
    role_names: Mapping[str, str],
) -> dict[str, object]:
    normalized_roles = {database_name: suffix for suffix, database_name in role_names.items()}

    def normalize(value):
        return normalized_roles.get(value, value)

    cursor.execute(
        "SELECT n.nspname,c.relname,c.relkind,pg_get_userbyid(c.relowner) "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname IN ('ref','rt') "
        "AND c.relkind IN ('r','p','v','m','S') ORDER BY 1,2,3"
    )
    owners = tuple(
        (schema, relation, kind, normalize(owner))
        for schema, relation, kind, owner in map(_row_values, cursor.fetchall())
    )
    cursor.execute(
        "SELECT pg_get_userbyid(datdba) FROM pg_database "
        "WHERE datname=current_database()"
    )
    database_owner = normalize(_row_values(cursor.fetchone())[0])
    cursor.execute(
        "SELECT nspname,pg_get_userbyid(nspowner) FROM pg_namespace "
        "WHERE nspname IN ('ref','rt') ORDER BY nspname"
    )
    schema_owners = tuple(
        (schema, normalize(owner))
        for schema, owner in map(_row_values, cursor.fetchall())
    )
    cursor.execute(
        "SELECT p.oid::regprocedure::text,pg_get_userbyid(p.proowner) "
        "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
        "WHERE n.nspname='rt' ORDER BY 1"
    )
    function_owners = tuple(
        (signature, normalize(owner))
        for signature, owner in map(_row_values, cursor.fetchall())
    )
    cursor.execute(
        "SELECT n.nspname,c.relname,c.relkind,privilege_type,"
        "pg_get_userbyid(grantee) "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace,"
        "LATERAL aclexplode(COALESCE(c.relacl,acldefault("
        "CASE WHEN c.relkind='S' THEN 'S'::\"char\" ELSE 'r'::\"char\" END,"
        "c.relowner))) "
        "WHERE n.nspname IN ('ref','rt') ORDER BY 1,2,3,4,5"
    )
    relation_acl = tuple(
        (schema, relation, kind, privilege, normalize(grantee))
        for schema, relation, kind, privilege, grantee in map(
            _row_values,
            cursor.fetchall(),
        )
    )
    cursor.execute(
        "SELECT r.rolname,r.rolcanlogin,r.rolinherit,r.rolsuper,"
        "r.rolcreatedb,r.rolcreaterole,r.rolreplication,r.rolbypassrls "
        "FROM pg_roles r WHERE r.rolname=ANY(%s) ORDER BY r.rolname",
        (list(normalized_roles),),
    )
    roles = tuple(
        (normalize(name), *attributes)
        for name, *attributes in map(_row_values, cursor.fetchall())
    )
    cursor.execute(
        "SELECT r.name AS role_name,p.name AS privilege_name,"
        "has_database_privilege(r.name,current_database(),p.name) AS granted "
        "FROM unnest(%s::text[]) AS r(name),"
        "unnest(ARRAY['CONNECT','CREATE','TEMPORARY']) AS p(name) "
        "ORDER BY r.name,p.name",
        (list(normalized_roles),),
    )
    database_privileges = tuple(
        (normalize(role), privilege, granted)
        for role, privilege, granted in map(_row_values, cursor.fetchall())
    )
    cursor.execute(
        "SELECT r.name AS role_name,s.name AS schema_name,"
        "p.name AS privilege_name,"
        "has_schema_privilege(r.name,s.name,p.name) AS granted "
        "FROM unnest(%s::text[]) AS r(name),"
        "unnest(ARRAY['public','ref','rt']) AS s(name),"
        "unnest(ARRAY['USAGE','CREATE']) AS p(name) "
        "ORDER BY r.name,s.name,p.name",
        (list(normalized_roles),),
    )
    schema_privileges = tuple(
        (normalize(role), schema, privilege, granted)
        for role, schema, privilege, granted in map(
            _row_values,
            cursor.fetchall(),
        )
    )
    cursor.execute(
        "SELECT p.oid::regprocedure::text AS function_signature,"
        "r.name AS role_name,"
        "has_function_privilege(r.name,p.oid,'EXECUTE') AS granted "
        "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace,"
        "unnest(%s::text[]) AS r(name) "
        "WHERE n.nspname='rt' ORDER BY 1,2",
        (list(normalized_roles),),
    )
    function_privileges = tuple(
        (signature, normalize(role), granted)
        for signature, role, granted in map(_row_values, cursor.fetchall())
    )
    cursor.execute(
        "SELECT m.name AS member_name,r.name AS role_name,"
        "pg_has_role(m.name,r.name,'MEMBER') AS granted "
        "FROM unnest(%s::text[]) AS m(name),"
        "unnest(%s::text[]) AS r(name) "
        "WHERE m.name<>r.name ORDER BY m.name,r.name",
        (list(normalized_roles), list(normalized_roles)),
    )
    role_memberships = tuple(
        (normalize(member), normalize(role), granted)
        for member, role, granted in map(_row_values, cursor.fetchall())
    )
    inventory = {
        "owners": owners,
        "database_owner": database_owner,
        "schema_owners": schema_owners,
        "function_owners": function_owners,
        "relation_acl": relation_acl,
        "roles": roles,
        "database_privileges": database_privileges,
        "schema_privileges": schema_privileges,
        "function_privileges": function_privileges,
        "role_memberships": role_memberships,
    }
    return {**inventory, "sha256": canonical_sha256(inventory)}


def _contract_relation(row, relation_names: set[str]) -> bool:
    return f"{row[0]}.{row[1]}" in relation_names or row[2] == "S"


def live024_schema_contract_inventory(
    inventory: Mapping[str, object],
) -> dict[str, object]:
    relation_names = set(live024_registry_contract().relation_names)
    normalized = {
        "relations": tuple(
            row
            for row in inventory["relations"]
            if _contract_relation(row, relation_names)
        ),
        "columns": tuple(
            row
            for row in inventory["columns"]
            if f"{row[0]}.{row[1]}" in relation_names
        ),
        "constraints": tuple(
            row
            for row in inventory["constraints"]
            if f"{row[0]}.{row[1]}" in relation_names
        ),
        "indexes": tuple(
            row
            for row in inventory["indexes"]
            if f"{row[0]}.{row[1]}" in relation_names
        ),
        "functions": tuple(inventory["functions"]),
        "triggers": tuple(
            row
            for row in inventory["triggers"]
            if f"{row[0]}.{row[1]}" in relation_names
        ),
    }
    return {**normalized, "sha256": canonical_sha256(normalized)}


def live024_acl_contract_inventory(
    inventory: Mapping[str, object],
) -> dict[str, object]:
    relation_names = set(live024_registry_contract().relation_names)
    normalized = {
        "owners": tuple(
            row
            for row in inventory["owners"]
            if _contract_relation(row, relation_names)
        ),
        "database_owner": inventory["database_owner"],
        "schema_owners": tuple(inventory["schema_owners"]),
        "function_owners": tuple(inventory["function_owners"]),
        "relation_acl": tuple(
            row
            for row in inventory["relation_acl"]
            if _contract_relation(row, relation_names)
        ),
        "roles": tuple(inventory["roles"]),
        "database_privileges": tuple(inventory["database_privileges"]),
        "schema_privileges": tuple(inventory["schema_privileges"]),
        "function_privileges": tuple(inventory["function_privileges"]),
        "role_memberships": tuple(inventory["role_memberships"]),
    }
    return {**normalized, "sha256": canonical_sha256(normalized)}


def _load_bound_json(
    path: Path,
    expected_sha256: str,
    *,
    maximum_bytes: int = 8 * 1024 * 1024,
) -> tuple[dict[str, object], str]:
    if path.stat().st_size > maximum_bytes:
        raise ValueError("compatibility evidence exceeds its verification bound")
    body = path.read_bytes()
    digest = hashlib.sha256(body).hexdigest()
    if digest != expected_sha256.lower():
        raise ValueError("compatibility evidence hash does not match its approved binding")
    report = json.loads(body)
    if not isinstance(report, dict):
        raise ValueError("compatibility evidence must be a JSON object")
    return report, digest


def load_live_metadata_capture(
    path: Path,
    expected_sha256: str,
) -> tuple[dict[str, object], str, dict[str, str]]:
    report, digest = _load_bound_json(
        path,
        expected_sha256,
        maximum_bytes=64 * 1024 * 1024,
    )
    if (
        report.get("status") != "PASS_READ_ONLY_CAPTURE"
        or report.get("compatibility_gate") != "CAPTURE_COMPATIBLE"
        or report.get("blockers") != []
        or report.get("contract_id") != CONTRACT_ID
        or report.get("schema_sha256") != LIVE024_SCHEMA_SHA256
        or report.get("registry_sha256") != LIVE024_REGISTRY_SHA256
        or tuple(report.get("expected_migrations") or ())
        != live024_migration_rows()
        or tuple(report.get("live_migrations") or ())
        != live024_migration_rows()
        or report.get("provider_requests") != 0
        or report.get("live_database_writes") != 0
        or report.get("live_ddl") != 0
        or report.get("live_profile_activation_performed") is not False
        or report.get("execution_eligible") is not False
        or report.get("compatibility_proven") is not False
    ):
        raise ValueError("live metadata report is not exact read-only live-024 evidence")
    schema = live024_schema_contract_inventory(report["schema_inventory"])
    acl = live024_acl_contract_inventory(report["acl_inventory"])
    return report, digest, {
        "schema_inventory_sha256": schema["sha256"],
        "acl_inventory_sha256": acl["sha256"],
    }


def load_qualification_report(
    path: Path,
    expected_sha256: str,
) -> tuple[dict[str, object], str]:
    report, digest = _load_bound_json(path, expected_sha256)
    phases = report.get("phases") or ()
    negative_cases = phases[0].get("negative_cases") if phases else None
    if (
        report.get("status") != "PASS_EQUITY_LIVE024_COMPATIBILITY"
        or report.get("contract_id") != CONTRACT_ID
        or report.get("schema_sha256") != LIVE024_SCHEMA_SHA256
        or tuple(report.get("migrations") or ()) != live024_migration_rows()
        or report.get("registry_sha256") != LIVE024_REGISTRY_SHA256
        or report.get("provider_requests") != 0
        or report.get("live_database_writes") != 0
        or report.get("live_profile_activated") is not False
        or report.get("publication_performed") is not False
        or report.get("cleanup_verified") is not True
        or report.get("counter_preservation_verified") is not True
        or report.get("live_inventory_match") is not True
        or not report.get("live_metadata_report_sha256")
        or report.get("schema_inventory_sha256")
        != report.get("live_schema_inventory_sha256")
        or report.get("acl_inventory_sha256")
        != report.get("live_acl_inventory_sha256")
        or (report.get("receipt_equality") or {}).get("equal") is not True
        or (report.get("metadata_evidence") or {}).get("status")
        != "PASS_REAL_STOCK_METADATA"
        or not negative_cases
        or not all(
            (negative_cases.get(name) or {}).get("rejected") is True
            for name in ("runtime_ddl", "new_options_receipt", "counter_reset")
        )
        or negative_cases.get("api_write_and_ddl_rejected") is not True
    ):
        raise ValueError("qualification report is not exact live-024 compatibility evidence")
    required_hashes = (
        "schema_inventory_sha256",
        "acl_inventory_sha256",
        "security_policy_sha256",
        "operator_bundle_sha256",
        "runtime_bundle_sha256",
        "runtime_profile_sha256",
        "runtime_policy_sha256",
        "image_id",
    )
    if any(not report.get(name) for name in required_hashes):
        raise ValueError("qualification report is missing a required pinned identity")
    return report, digest


def load_live_preflight_report(
    path: Path,
    expected_sha256: str,
) -> tuple[dict[str, object], str]:
    report, digest = _load_bound_json(path, expected_sha256)
    status = report.get("status")
    history = report.get("history_check") or {}
    bridge = report.get("bridge_check") or {}
    complete = (
        status
        == "SCHEMA_SECURITY_HISTORY_BRIDGE_COMPATIBLE_METADATA_GATES_REQUIRED"
        and history.get("status") == "PASS"
        and bridge.get("status") == "PASS"
        and report.get("bootstrap_eligible") is False
        and report.get("permitted_actions") == []
    )
    bootstrap = (
        status == "HISTORY_BOOTSTRAP_REQUIRED"
        and history.get("status") == "HISTORY_BOOTSTRAP_REQUIRED"
        and history.get("bootstrap_required") is True
        and history.get("permitted_action")
        == "SEPARATELY_AUTHORIZED_HISTORY_BOOTSTRAP"
        and bridge.get("status") == "NOT_RUN"
        and bridge.get("reason_codes") == ["HISTORY_BOOTSTRAP_REQUIRED"]
        and report.get("bootstrap_eligible") is True
        and report.get("permitted_actions")
        == ["SEPARATELY_AUTHORIZED_HISTORY_BOOTSTRAP"]
    )
    if (
        not (complete or bootstrap)
        or report.get("pre_provisioning_admission") != history
        or report.get("post_bootstrap_prearm_verification") != bridge
        or report.get("contract_id") != CONTRACT_ID
        or report.get("schema_sha256") != LIVE024_SCHEMA_SHA256
        or report.get("blockers") != []
        or report.get("provider_requests") != 0
        or report.get("live_database_writes") != 0
        or report.get("live_profile_activation_performed") is not False
        or report.get("execution_eligible") is not False
    ):
        raise ValueError(
            "preflight report is not exact read-only admission evidence"
        )
    if (
        not report.get("qualification_report_sha256")
        or not report.get("checked_at")
        or not report.get("arm_deadline")
        or not report.get("schema_inventory_sha256")
        or not report.get("acl_inventory_sha256")
        or not report.get("security_policy_sha256")
        or not report.get("runtime_profile_sha256")
        or not report.get("runtime_policy_sha256")
    ):
        raise ValueError("preflight report does not bind qualification evidence")
    return report, digest
