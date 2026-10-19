"""The V2 role model (admin, app, api) against a recording cursor: no database, no role is created.

The statements `security_v2` issues are rendered with a plain-text stand-in for psycopg2.sql (identifier
quoting needs a live connection) and compared with what the relation registry says each role may do.
"""
from __future__ import annotations

import re

import pytest

from realtime import security_v2
from realtime.relation_registry import APP_ROLE, RELATIONS, grants, security_policy
from realtime.security_v2 import (
    EXECUTE_GRANTS,
    SEQUENCE_GRANTS,
    apply_database_security_v2,
    apply_equity_live024_database_security_v2,
    audit_database_security_v2,
    build_role_names_v2,
    create_disposable_roles_v2,
    ensure_roles_v2,
)
from realtime.equity_live024_compatibility import POST024_RELATIONS

PREFIX = "test_rt_x"
ROLES = build_role_names_v2(PREFIX)
ADMIN, APP, API = (ROLES.name(name) for name in ("admin", "app", "api"))
DATABASE = "test_realtime_v2_x"


class _Query(str):
    def format(self, *args):
        return _Query(str.format(self, *args))

    def join(self, items):
        return _Query(str(self).join(str(item) for item in items))


class _PlainSql:
    """Stands in for psycopg2.sql: the same calls, rendered as text."""

    @staticmethod
    def SQL(text):
        return _Query(text)

    @staticmethod
    def Identifier(*names):
        return _Query(".".join(f'"{name}"' for name in names))


class _Cursor:
    def __init__(self, answer, log):
        self._answer, self._log, self._rows = answer, log, []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, parameters=None):
        text = " ".join(str(statement).split())
        self._log.append((text, parameters))
        self._rows = list(self._answer(text, parameters))

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _Connection:
    def __init__(self, answer=lambda text, parameters: ()):
        self.log, self._answer = [], answer

    def cursor(self):
        return _Cursor(self._answer, self.log)

    @property
    def statements(self):
        return [text for text, _ in self.log]


@pytest.fixture(autouse=True)
def plain_sql(monkeypatch):
    monkeypatch.setattr(security_v2, "sql", _PlainSql)


def _apply_answer(policy=None):
    policy = security_policy() if policy is None else policy

    def answer(text, parameters):
        if text.startswith("SELECT policy FROM rt.database_security_policies"):
            return [(policy,)]
        if text == "SELECT current_database()":
            return [(DATABASE,)]
        if text.startswith("SELECT n.nspname, c.relname, c.relkind"):
            return [("rt", "instruments", "r"), ("rt", "readiness_transitions_transition_id_seq", "S")]
        if text.startswith("SELECT p.oid::regprocedure::text"):
            return [("rt.fence_stage_run(uuid)",)]
        return []

    return answer


def _granted_tables(statements):
    found: dict[tuple[str, str], set[str]] = {}
    for text in statements:
        match = re.fullmatch(r'GRANT (SELECT|INSERT|UPDATE|DELETE) ON TABLE (.+) TO "(\w+)"', text)
        if match:
            relations = {name.replace('"', "") for name in match.group(2).split(", ")}
            found.setdefault((match.group(1), match.group(3)), set()).update(relations)
    return found


def test_apply_grants_exactly_what_the_registry_says_and_nothing_to_admin_or_public():
    connection = _Connection(_apply_answer())
    apply_database_security_v2(connection, ROLES)
    statements = connection.statements
    found = _granted_tables(statements)
    expected: dict[tuple[str, str], set[str]] = {}
    for relation, privileges in grants().items():
        for privilege, roles in privileges.items():
            for role in roles:
                expected.setdefault((privilege, ROLES.name(role)), set()).add(relation)
    assert found == expected
    assert {role for _, role in found} == {APP, API}
    assert all(privilege == "SELECT" for privilege, role in found if role == API)
    assert {privilege for privilege, role in found if role == APP} == {"SELECT", "INSERT", "UPDATE", "DELETE"}
    assert found[("INSERT", APP)] == found[("UPDATE", APP)]  # append-only is the trigger's job, not a privilege
    assert not found[("INSERT", APP)] & {name for name, spec in RELATIONS.items() if spec.retention == "OWNER_ONLY"}
    assert not any(text.startswith("GRANT") and ADMIN in text for text in statements)  # admin owns, never granted
    assert f'ALTER DATABASE "{DATABASE}" OWNER TO "{ADMIN}"' in statements
    assert f'ALTER TABLE "rt"."instruments" OWNER TO "{ADMIN}"' in statements
    assert f'ALTER SEQUENCE "rt"."readiness_transitions_transition_id_seq" OWNER TO "{ADMIN}"' in statements
    assert f'ALTER FUNCTION rt.fence_stage_run(uuid) OWNER TO "{ADMIN}"' in statements


def test_live024_apply_uses_the_exact_historical_registry_not_a_domain_filter():
    connection = _Connection(_apply_answer())

    apply_equity_live024_database_security_v2(connection, ROLES)

    granted = {
        relation
        for relations in _granted_tables(connection.statements).values()
        for relation in relations
    }
    assert not granted & POST024_RELATIONS
    assert "rt.option_flow_events" in granted
    assert "rt.advanced_ingress_plans" in granted


def test_apply_resets_then_grants_database_schema_sequence_and_function_privileges():
    connection = _Connection(_apply_answer())
    apply_database_security_v2(connection, ROLES)
    statements = connection.statements
    runtime = f'"{APP}", "{API}"'
    first_grant = min(index for index, text in enumerate(statements) if text.startswith("GRANT SELECT ON TABLE"))
    for reset in (f"REVOKE ALL ON ALL TABLES IN SCHEMA ref, rt FROM {runtime}",
                  f"REVOKE ALL ON ALL SEQUENCES IN SCHEMA ref, rt FROM {runtime}",
                  f"REVOKE ALL ON ALL FUNCTIONS IN SCHEMA ref, rt FROM {runtime}",
                  f'REVOKE ALL ON DATABASE "{DATABASE}" FROM {runtime}'):
        assert statements.index(reset) < first_grant, reset  # a rerun keeps nothing the registry dropped
    assert 'REVOKE ALL ON ALL TABLES IN SCHEMA ref, rt FROM PUBLIC' in statements
    assert f'GRANT CONNECT ON DATABASE "{DATABASE}" TO {runtime}' in statements
    assert f'GRANT USAGE ON SCHEMA ref, rt TO {runtime}' in statements
    temporary = [text for text in statements if "TEMPORARY" in text]
    assert temporary == [f'GRANT TEMPORARY ON DATABASE "{DATABASE}" TO "{APP}"']  # api never creates temp tables
    for suffix, sequences in SEQUENCE_GRANTS.items():
        rendered = ", ".join(".".join(f'"{part}"' for part in name.split(".")) for name in sequences)
        assert f'GRANT USAGE, SELECT ON SEQUENCE {rendered} TO "{ROLES.name(suffix)}"' in statements
    for signature, suffixes in EXECUTE_GRANTS.items():
        assert suffixes == (APP_ROLE,)
        assert f'GRANT EXECUTE ON FUNCTION {signature} TO "{APP}"' in statements
    assert connection.log[-1][1][0] == "APPLIED"


def test_apply_refuses_a_database_whose_stored_policy_differs_from_the_source():
    other = {**security_policy(), "runtime_roles": ["app"]}
    connection = _Connection(_apply_answer(other))
    with pytest.raises(RuntimeError, match="does not match source"):
        apply_database_security_v2(connection, ROLES)
    assert not any(text.startswith(("GRANT", "REVOKE", "ALTER")) for text in connection.statements)


def _audit_answer(*, extra=(), missing=(), membership=(), app_temp=True, api_temp=False, phase="APPLIED"):
    expected = grants()

    def answer(text, parameters):
        if "has_table_privilege(%s, c.oid, %s)" in text:
            role, privilege = parameters
            suffix = role.removeprefix(PREFIX + "_")
            rows = []
            for relation in RELATIONS:
                has = suffix in expected[relation].get(privilege, ())
                if (suffix, privilege, relation) in extra:
                    has = True
                if (suffix, privilege, relation) in missing:
                    has = False
                rows.append((relation, has))
            return rows
        if "aclexplode(c.relacl)" in text or "pg_get_userbyid(c.relowner)" in text:
            return [(0,)]
        if "pg_has_role" in text:
            member, role = (name.removeprefix(PREFIX + "_") for name in parameters[:2])
            return [((member, role) in membership,)]
        if "rolsuper" in text:
            return [(0,)]
        if "has_schema_privilege" in text:
            return [(False,)]
        if "has_database_privilege" in text:
            return [(app_temp, api_temp)]
        if "has_function_privilege" in text:
            return [(True, False)]
        if "FROM rt.security_phase_events" in text:
            return [(phase,)] if phase else []
        raise AssertionError(f"unscripted audit query: {text}")

    return answer


def test_a_clean_audit_passes_every_check_and_names_them():
    results = audit_database_security_v2(_Connection(_audit_answer()), ROLES)
    assert all(status == "PASS" for _, status in results), results
    assert [name for name, _ in results] == [
        "runtime_privileges_match_registry", "no_public_table_privileges", "all_relations_owned_by_admin",
        "roles_share_no_membership", "roles_hold_no_administrative_attributes",
        "runtime_roles_cannot_create_in_schemas", "only_app_may_create_temporary_tables", "api_is_read_only",
        "partition_function_execute_is_restricted", "stage_fence_execute_is_restricted",
        "security_phase_ledger_records_hardening"]


def test_the_audit_reports_an_extra_or_a_missing_privilege_exactly():
    extra = {("api", "INSERT", "rt.snapshot_catalog"), ("app", "SELECT", "rt.schema_migrations")}
    missing = {("app", "DELETE", "rt.worker_leases"), ("app", "UPDATE", "rt.window_runs")}
    results = dict(audit_database_security_v2(_Connection(_audit_answer(extra=extra, missing=missing)), ROLES))
    verdict = results["runtime_privileges_match_registry"]
    assert verdict.startswith("FAIL")
    for detail in ("api:INSERT:rt.snapshot_catalog:extra", "app:SELECT:rt.schema_migrations:extra",
                   "app:DELETE:rt.worker_leases:missing", "app:UPDATE:rt.window_runs:missing"):
        assert detail in verdict, detail
    assert {name for name, status in results.items() if status != "PASS"} == {"runtime_privileges_match_registry"}


def test_the_audit_fails_on_role_membership_missing_temp_and_a_database_not_hardened_yet():
    results = dict(audit_database_security_v2(
        _Connection(_audit_answer(membership={("app", "admin")}, app_temp=False, phase="DEFERRED")), ROLES))
    assert results["roles_share_no_membership"] == "FAIL"
    assert results["only_app_may_create_temporary_tables"] == "FAIL"
    assert results["security_phase_ledger_records_hardening"].startswith("FAIL")
    assert results["runtime_privileges_match_registry"] == "PASS"
    leaky = dict(audit_database_security_v2(_Connection(_audit_answer(api_temp=True)), ROLES))
    assert leaky["only_app_may_create_temporary_tables"] == "FAIL"


def _roles_answer(existing):
    def answer(text, parameters):
        if text.startswith("SELECT rolname, rolcanlogin, rolinherit FROM pg_roles"):
            return existing
        if text.startswith("SELECT rolname FROM pg_roles"):
            return [(row[0],) for row in existing]
        return []

    return answer


def test_ensure_roles_creates_the_three_nologin_roles_with_no_membership_and_is_rerunnable():
    connection = _Connection(_roles_answer([]))
    assert ensure_roles_v2(connection, ROLES) == [ADMIN, APP, API]
    assert [text for text in connection.statements if text.startswith("CREATE ROLE")] == [
        f'CREATE ROLE "{name}" NOLOGIN' for name in (ADMIN, APP, API)]
    assert not any(text.startswith("GRANT") for text in connection.statements)
    again = _Connection(_roles_answer([(name, False, True) for name in (ADMIN, APP, API)]))
    assert ensure_roles_v2(again, ROLES) == []
    assert not any(text.startswith("CREATE") for text in again.statements)
    partial = _Connection(_roles_answer([(ADMIN, False, True)]))
    assert ensure_roles_v2(partial, ROLES) == [APP, API]


@pytest.mark.parametrize("attributes", [(True, True), (False, False)])
def test_ensure_roles_refuses_an_existing_role_whose_attributes_differ_and_never_alters_it(attributes):
    connection = _Connection(_roles_answer([(APP, *attributes)]))
    with pytest.raises(RuntimeError, match="does not match the security policy"):
        ensure_roles_v2(connection, ROLES)
    assert not any(text.startswith(("ALTER", "CREATE")) for text in connection.statements)


def test_disposable_roles_are_only_for_test_prefixes_and_are_three_independent_nologin_roles():
    connection = _Connection(_roles_answer([]))
    create_disposable_roles_v2(connection, ROLES)
    assert [text for text in connection.statements if text.startswith(("CREATE", "GRANT"))] == [
        f'CREATE ROLE "{name}" NOLOGIN' for name in (ADMIN, APP, API)]
    with pytest.raises(ValueError, match="test_rt_"):
        create_disposable_roles_v2(_Connection(), build_role_names_v2("stocks_realtime_v2"))
    with pytest.raises(RuntimeError, match="already exists"):
        create_disposable_roles_v2(_Connection(_roles_answer([(APP, False, True)])), ROLES)
