"""V2 role creation, grant application and audit, all derived from the relation registry.

This module is the final hardening phase. The structural baseline (sql_v2) creates no role or
grant and records the phase DEFERRED; apply_database_security_v2 records APPLIED and
record_security_audit_v2 records AUDITED only after a clean exact-privilege audit.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass

from psycopg2 import sql

from .relation_registry import (
    CAPABILITY_ROLES,
    PRINCIPAL_PROFILES,
    RELATIONS,
    SECURITY_POLICY_VERSION,
    grants,
    security_policy,
)
from .stage_graph import RESIDENT_SCHEDULES, STAGES_V1

_ROLE_PREFIX = re.compile(r"^[a-z][a-z0-9_]{2,30}$")
CAPABILITY_SUFFIXES = ("owner", *CAPABILITY_ROLES)
PRINCIPAL_SUFFIXES = tuple(PRINCIPAL_PROFILES)
# Principals that must not inherit privileges: they SET ROLE to one capability at a time.
NOINHERIT_PRINCIPALS = ("runtime_data", "runtime_options", "archivist")
SEQUENCE_GRANTS = {
    "control": ("rt.readiness_transitions_transition_id_seq",),
    "references": ("rt.instruments_instrument_id_seq",),
}
# Roles whose stages run once per window (and so have a stage run to fence their writes against).
FENCED_STAGE_ROLES = tuple(sorted({
    stage.capability_role for stage in STAGES_V1.values() if stage.schedule_kind not in RESIDENT_SCHEDULES
}))
EXECUTE_GRANTS = {
    "rt.ensure_partitions(text, date)": ("partition_admin", "maintenance"),
    "rt.ensure_partitions_all(date)": ("partition_admin", "maintenance"),
    "rt.partition_health(date)": ("partition_admin", "maintenance"),
    "rt.fence_stage_run(uuid)": FENCED_STAGE_ROLES,
}
PRIVILEGES = ("SELECT", "INSERT", "UPDATE", "DELETE")


@dataclass(frozen=True, slots=True)
class RoleNamesV2:
    prefix: str

    def name(self, suffix: str) -> str:
        return f"{self.prefix}_{suffix}"

    @property
    def capability_names(self) -> tuple[str, ...]:
        return tuple(self.name(suffix) for suffix in CAPABILITY_SUFFIXES)

    @property
    def principal_names(self) -> tuple[str, ...]:
        return tuple(self.name(suffix) for suffix in PRINCIPAL_SUFFIXES)

    @property
    def all_names(self) -> tuple[str, ...]:
        return self.capability_names + self.principal_names


def build_role_names_v2(prefix: str) -> RoleNamesV2:
    normalized = prefix.strip()
    if not _ROLE_PREFIX.fullmatch(normalized):
        raise ValueError("role prefix must be 3-31 lowercase letters, digits or underscores")
    roles = RoleNamesV2(normalized)
    if any(len(name) > 63 for name in roles.all_names):
        raise ValueError("generated PostgreSQL role name exceeds 63 characters")
    return roles


def _identifier_list(names) -> sql.Composed:
    return sql.SQL(", ").join(sql.Identifier(name) for name in names)


def _relation(name: str) -> sql.Composed:
    schema_name, relation_name = name.split(".", 1)
    return sql.SQL("{}.{}").format(sql.Identifier(schema_name), sql.Identifier(relation_name))


def _relation_list(names) -> sql.Composed:
    return sql.SQL(", ").join(_relation(name) for name in names)


def create_disposable_roles_v2(connection, roles: RoleNamesV2) -> None:
    if not roles.prefix.startswith("test_rt_"):
        raise ValueError("disposable roles must use the test_rt_ prefix")
    with connection.cursor() as cursor:
        cursor.execute("SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)", (list(roles.all_names),))
        existing = [row[0] for row in cursor.fetchall()]
        if existing:
            raise RuntimeError("disposable role already exists; refusing ownership: " + ", ".join(existing))
        for suffix in (*CAPABILITY_SUFFIXES, *PRINCIPAL_SUFFIXES):
            noinherit = sql.SQL(" NOINHERIT") if suffix in NOINHERIT_PRINCIPALS else sql.SQL("")
            cursor.execute(sql.SQL("CREATE ROLE {} NOLOGIN{}").format(sql.Identifier(roles.name(suffix)), noinherit))
        for principal, memberships in PRINCIPAL_PROFILES.items():
            for capability in memberships:
                cursor.execute(sql.SQL("GRANT {} TO {}").format(
                    sql.Identifier(roles.name(capability)), sql.Identifier(roles.name(principal))))


def drop_disposable_roles_v2(connection, roles: RoleNamesV2) -> None:
    if not roles.prefix.startswith("test_rt_"):
        raise ValueError("only disposable roles can be dropped")
    with connection.cursor() as cursor:
        for name in reversed(roles.all_names):
            cursor.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(name)))


def apply_database_security_v2(connection, roles: RoleNamesV2) -> None:
    owner = roles.name("owner")
    with connection.cursor() as cursor:
        cursor.execute("SELECT policy FROM rt.database_security_policies WHERE policy_version = %s",
                       (SECURITY_POLICY_VERSION,))
        row = cursor.fetchone()
        if row is None or row[0] != security_policy():
            raise RuntimeError("database security policy does not match source")
        cursor.execute("SELECT current_database()")
        database_name = cursor.fetchone()[0]
        cursor.execute(sql.SQL("ALTER DATABASE {} OWNER TO {}").format(
            sql.Identifier(database_name), sql.Identifier(owner)))
        for schema_name in ("ref", "rt"):
            cursor.execute(sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(
                sql.Identifier(schema_name), sql.Identifier(owner)))
        cursor.execute(
            "SELECT n.nspname, c.relname, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname IN ('ref', 'rt') AND c.relkind IN ('r', 'p', 'S') ORDER BY 1, 2")
        for schema_name, object_name, kind in cursor.fetchall():
            cursor.execute(sql.SQL("ALTER {} {}.{} OWNER TO {}").format(
                sql.SQL("SEQUENCE" if kind == "S" else "TABLE"), sql.Identifier(schema_name),
                sql.Identifier(object_name), sql.Identifier(owner)))
        cursor.execute(
            "SELECT p.oid::regprocedure::text FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
            "WHERE n.nspname = 'rt'")
        for (signature,) in cursor.fetchall():
            cursor.execute(sql.SQL("ALTER FUNCTION {} OWNER TO {}").format(
                sql.SQL(signature), sql.Identifier(owner)))
        cursor.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(sql.Identifier(database_name)))
        cursor.execute("REVOKE ALL ON SCHEMA public, ref, rt FROM PUBLIC")
        cursor.execute("REVOKE ALL ON ALL TABLES IN SCHEMA ref, rt FROM PUBLIC")
        cursor.execute("REVOKE ALL ON ALL SEQUENCES IN SCHEMA ref, rt FROM PUBLIC")
        cursor.execute("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA ref, rt FROM PUBLIC")
        cursor.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
            sql.Identifier(database_name), _identifier_list(roles.principal_names)))
        runtime_capabilities = [roles.name(suffix) for suffix in CAPABILITY_ROLES]
        cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA ref, rt TO {}").format(_identifier_list(runtime_capabilities)))
        by_privilege: dict[tuple[str, str], list[str]] = {}
        for relation, privileges in grants().items():
            for privilege, role_suffixes in privileges.items():
                for suffix in role_suffixes:
                    by_privilege.setdefault((privilege, suffix), []).append(relation)
        for (privilege, suffix), relations in sorted(by_privilege.items()):
            cursor.execute(sql.SQL("GRANT {} ON TABLE {} TO {}").format(
                sql.SQL(privilege), _relation_list(sorted(relations)), sql.Identifier(roles.name(suffix))))
        for suffix, sequences in SEQUENCE_GRANTS.items():
            cursor.execute(sql.SQL("GRANT USAGE, SELECT ON SEQUENCE {} TO {}").format(
                _relation_list(sequences), sql.Identifier(roles.name(suffix))))
        for signature, suffixes in EXECUTE_GRANTS.items():
            cursor.execute(sql.SQL("GRANT EXECUTE ON FUNCTION {} TO {}").format(
                sql.SQL(signature), _identifier_list(roles.name(suffix) for suffix in suffixes)))
        for schema_name in ("ref", "rt"):
            cursor.execute(sql.SQL("ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {} REVOKE ALL ON TABLES FROM PUBLIC")
                           .format(sql.Identifier(owner), sql.Identifier(schema_name)))
        _record_phase(cursor, "APPLIED", {"role_prefix": roles.prefix})


def current_security_phase_v2(connection) -> str | None:
    """Latest ledger phase (DEFERRED, APPLIED or AUDITED); None before the 001 migration."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT phase FROM rt.security_phase_events ORDER BY event_id DESC LIMIT 1")
        row = cursor.fetchone()
    return row[0] if row else None


def record_security_audit_v2(connection, results: list[tuple[str, str]]) -> None:
    """Record AUDITED only when every exact-privilege check passed."""
    failures = [name for name, status in results if not status.startswith("PASS")]
    if failures:
        raise RuntimeError("security audit failed: " + ", ".join(failures[:8]))
    with connection.cursor() as cursor:
        _record_phase(cursor, "AUDITED", {"checks": len(results)})


def _record_phase(cursor, phase: str, detail: dict[str, object]) -> None:
    cursor.execute(
        "INSERT INTO rt.security_phase_events (policy_version, policy_sha256, phase, detail) "
        "SELECT policy_version, policy_sha256, %s, %s::jsonb FROM rt.database_security_policies "
        "WHERE policy_version = %s", (phase, json.dumps(detail), SECURITY_POLICY_VERSION))


def ensure_roles_v2(connection, roles: RoleNamesV2) -> list[str]:
    """Create any missing capability or principal role and verify the existing ones; re-runnable.

    All roles are NOLOGIN. LOGIN principals and their credentials are provisioned separately, after
    the credential model is decided (never from this module). An existing role whose attributes differ
    from the policy is an error, never silently altered. Returns the names created in this call.
    """
    created: list[str] = []
    with connection.cursor() as cursor:
        cursor.execute("SELECT rolname, rolcanlogin, rolinherit FROM pg_roles WHERE rolname = ANY(%s)",
                       (list(roles.all_names),))
        existing = {name: (can_login, inherit) for name, can_login, inherit in cursor.fetchall()}
        for suffix in (*CAPABILITY_SUFFIXES, *PRINCIPAL_SUFFIXES):
            name = roles.name(suffix)
            wanted_inherit = suffix not in NOINHERIT_PRINCIPALS
            if name in existing:
                if existing[name] != (False, wanted_inherit):
                    raise RuntimeError(f"existing role {name} does not match the security policy attributes")
                continue
            noinherit = sql.SQL("") if wanted_inherit else sql.SQL(" NOINHERIT")
            cursor.execute(sql.SQL("CREATE ROLE {} NOLOGIN{}").format(sql.Identifier(name), noinherit))
            created.append(name)
        for principal, memberships in PRINCIPAL_PROFILES.items():
            for capability in memberships:
                cursor.execute(sql.SQL("GRANT {} TO {}").format(
                    sql.Identifier(roles.name(capability)), sql.Identifier(roles.name(principal))))
    return created


def harden_database_v2(connection, roles: RoleNamesV2) -> tuple[list[str], list[tuple[str, str]]]:
    """Final security phase: ensure roles, apply grants, audit, and record AUDITED only on a clean audit.

    Re-runnable. A failure leaves the ledger at APPLIED (or DEFERRED), which a hardened runtime
    refuses (database_access_v2), so a half-hardened database fails closed. Returns (created roles,
    audit results).
    """
    try:
        created = ensure_roles_v2(connection, roles)
        connection.commit()
        apply_database_security_v2(connection, roles)
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    audit = audit_database_security_v2(connection, roles)
    if all(status.startswith("PASS") for _, status in audit):
        record_security_audit_v2(connection, audit)
        connection.commit()
    else:
        connection.rollback()
    return created, audit


def audit_database_security_v2(connection, roles: RoleNamesV2) -> list[tuple[str, str]]:
    """Return (check, PASS|FAIL detail). Compares live privileges with the registry exactly."""
    results: list[tuple[str, str]] = []
    expected = grants()
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT n.nspname || '.' || c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname IN ('ref','rt') AND c.relkind IN ('r','p') AND NOT c.relispartition")
        relations = sorted(row[0] for row in cursor.fetchall())
        mismatches: list[str] = []
        for suffix in CAPABILITY_ROLES:
            role = roles.name(suffix)
            for privilege in PRIVILEGES:
                cursor.execute(
                    "SELECT n.nspname || '.' || c.relname, has_table_privilege(%s, c.oid, %s) FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname IN ('ref','rt') AND c.relkind IN ('r','p') AND NOT c.relispartition",
                    (role, privilege))
                for relation, has_privilege in cursor.fetchall():
                    wanted = suffix in expected.get(relation, {}).get(privilege, ())
                    if bool(has_privilege) != wanted:
                        mismatches.append(f"{suffix}:{privilege}:{relation}:{'missing' if wanted else 'extra'}")
        results.append(("capability_privileges_match_registry",
                        "PASS" if not mismatches else f"FAIL {mismatches[:8]} (+{max(0, len(mismatches) - 8)})"))
        cursor.execute(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace, "
            "LATERAL aclexplode(c.relacl) a WHERE n.nspname IN ('ref','rt') AND a.grantee = 0")
        results.append(("no_public_table_privileges", "PASS" if cursor.fetchone()[0] == 0 else "FAIL"))
        cursor.execute(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname IN ('ref','rt') AND c.relkind IN ('r','p','S') "
            "AND pg_get_userbyid(c.relowner) <> %s", (roles.name("owner"),))
        results.append(("all_relations_owned_by_owner", "PASS" if cursor.fetchone()[0] == 0 else "FAIL"))

        def has_role(principal: str, capability: str) -> bool:
            cursor.execute("SELECT pg_has_role(%s, %s, 'MEMBER')", (roles.name(principal), roles.name(capability)))
            return bool(cursor.fetchone()[0])

        results.append(("runtime_options_cannot_use_data_roles", "PASS" if not any(
            has_role("runtime_options", role) for role in ("ingress", "compute", "references", "publication",
                                                           "maintenance", "partition_admin", "owner")) else "FAIL"))
        results.append(("runtime_data_cannot_use_options_roles", "PASS" if not any(
            has_role("runtime_data", role) for role in ("options_ingress", "options_compute", "partition_admin",
                                                        "owner")) else "FAIL"))
        results.append(("archivist_can_ddl_only_via_owner", "PASS" if has_role("archivist", "owner")
                        and has_role("archivist", "partition_admin") and not has_role("archivist", "compute")
                        else "FAIL"))
        results.append(("api_is_read_only", "PASS" if not any(
            privilege != "SELECT" and "reader" in roles_map.get(privilege, ())
            for roles_map in expected.values() for privilege in roles_map) else "FAIL"))
        for relation, spec in RELATIONS.items():
            if spec.domain == "equity" and any(role.startswith("options_") for role in (*spec.insert, *spec.update)):
                results.append((f"equity_table_not_writable_by_options:{relation}", "FAIL"))
            if spec.domain == "options" and any(role in ("compute", "ingress") for role in (*spec.insert, *spec.update)):
                results.append((f"options_table_not_writable_by_data:{relation}", "FAIL"))
        cursor.execute(
            "SELECT has_function_privilege(%s, 'rt.ensure_partitions(text, date)', 'EXECUTE'), "
            "has_function_privilege(%s, 'rt.ensure_partitions(text, date)', 'EXECUTE')",
            (roles.name("partition_admin"), roles.name("reader")))
        granted, denied = cursor.fetchone()
        results.append(("partition_function_execute_is_restricted", "PASS" if granted and not denied else "FAIL"))
        cursor.execute(
            "SELECT bool_and(has_function_privilege(role.name, 'rt.fence_stage_run(uuid)', 'EXECUTE')) "
            "FROM unnest(%s::text[]) AS role(name)", ([roles.name(suffix) for suffix in FENCED_STAGE_ROLES],))
        fenced = cursor.fetchone()[0]
        cursor.execute("SELECT has_function_privilege(%s, 'rt.fence_stage_run(uuid)', 'EXECUTE')",
                       (roles.name("reader"),))
        reader_can = cursor.fetchone()[0]
        results.append(("stage_fence_execute_is_restricted", "PASS" if fenced and not reader_can else "FAIL"))
        cursor.execute("SELECT phase FROM rt.security_phase_events ORDER BY event_id DESC LIMIT 1")
        phase = cursor.fetchone()
        results.append(("security_phase_ledger_records_hardening",
                        "PASS" if phase and phase[0] in ("APPLIED", "AUDITED") else f"FAIL phase={phase}"))
    return results
