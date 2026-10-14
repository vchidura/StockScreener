from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from realtime.migrations import (
    PHASE_SECURITY,
    PHASE_STRUCTURE,
    SECURITY_MODE_DEFERRED,
    SECURITY_MODE_HARDENED,
    SECURITY_MODES_V2,
    V2_MIGRATIONS_DIR,
    load_realtime_migrations,
    schema_fingerprint,
    v2_provisioning_phases,
    validate_target_database_v2,
)
from realtime.relation_registry import (
    CAPABILITY_ROLES,
    PARTITION_POLICY_SEEDS,
    PARTITIONED_RELATIONS,
    PRINCIPAL_PROFILES,
    RELATIONS,
    SECURITY_PHASES,
    grants,
    security_policy,
)
from realtime.security_v2 import build_role_names_v2, record_security_audit_v2

SQL_DIR = Path(V2_MIGRATIONS_DIR)
_CREATE_TABLE = re.compile(r"^CREATE TABLE ((?:ref|rt)\.[a-z_0-9]+)", re.MULTILINE)


def _sql(name: str) -> str:
    return (SQL_DIR / name).read_text(encoding="utf-8")


def _all_sql() -> str:
    return "\n".join(path.read_text(encoding="utf-8") for path in sorted(SQL_DIR.glob("*.sql")))


def test_v2_migration_chain_is_contiguous_and_fingerprinted():
    migrations = load_realtime_migrations(SQL_DIR)
    assert migrations[0].version == "000_realtime_baseline"
    assert [migration.sequence for migration in migrations] == list(range(len(migrations)))
    assert len(schema_fingerprint(migrations)) == 64


def test_every_sql_table_is_in_the_registry_and_back():
    created = set(_CREATE_TABLE.findall(_all_sql()))
    assert created == set(RELATIONS)


def test_registry_partitioned_relations_match_sql_declarations():
    sql_text = _all_sql()
    declared = set()
    for match in re.finditer(r"^CREATE TABLE ((?:ref|rt)\.[a-z_0-9]+) \((.*?)^\)([^;]*);", sql_text,
                             re.MULTILINE | re.DOTALL):
        if "PARTITION BY RANGE" in match.group(3):
            declared.add(match.group(1))
    assert declared == set(PARTITIONED_RELATIONS)
    assert set(PARTITION_POLICY_SEEDS) == set(PARTITIONED_RELATIONS)


def test_partition_policy_seed_rows_match_registry():
    text = _sql("009_realtime_partition_maintenance.sql")
    rows = re.findall(
        r"\('(rt\.[a-z_]+)', '(DAILY|WEEKLY|MONTHLY)', '[a-z_]+', '(?:timestamptz|date)', "
        r"'([A-Z_0-9]+)', '([A-Z_]+)', (\d+),", text)
    parsed = {name: (scheme, retention, kind, int(value)) for name, scheme, retention, kind, value in rows}
    assert parsed == PARTITION_POLICY_SEEDS


def test_sql_security_policy_equals_source_policy():
    text = _sql("001_realtime_security_policy.sql")
    document = text.split("$policy$")[1]
    assert json.loads(document) == security_policy()


def test_no_default_partition_is_ever_created():
    assert "DEFAULT" not in " ".join(
        line for line in _all_sql().splitlines() if "PARTITION OF" in line.upper())
    assert "PARTITION OF" in _sql("009_realtime_partition_maintenance.sql")


def test_stage_catalog_seeds_keep_options_out_of_equity_prerequisites():
    text = _sql("004_realtime_orchestration.sql")
    rows = re.findall(r"\('([A-Z_]+)', 'stages_v1', '(shared|equity|options)', ARRAY\[([^\]]*)\]", text)
    stages = {name: (domain, re.findall(r"'([A-Z_]+)'", prerequisites)) for name, domain, prerequisites in rows}
    assert set(stages) == {
        "REFERENCE", "EQUITY_INGEST", "EQUITY_EVIDENCE", "PORTAL_PUBLISH", "STRATEGY_PUBLISH",
        "STOCK_ALERTS", "OPTIONS_ACQUIRE", "OPTIONS_ADMISSION", "OPTIONS_CYCLE", "OPTIONS_FLOW",
    }
    for name, (domain, prerequisites) in stages.items():
        assert name not in prerequisites
        if domain != "options":
            assert all(stages[item][0] != "options" for item in prerequisites), name

    def depth(name: str, seen=()):
        assert name not in seen, "prerequisite cycle"
        return 1 + max((depth(item, (*seen, name)) for item in stages[name][1]), default=0)

    assert max(depth(name) for name in stages) == 4
    gates = re.findall(r"\('([A-Z_]+)', 'stages_v1'.*?, (true|false)\)", text)
    assert {name for name, flag in gates if flag == "true"} == {"OPTIONS_CYCLE"}


def test_registry_invariants_and_separation_of_duties():
    assert "owner" not in CAPABILITY_ROLES
    for name, spec in RELATIONS.items():
        privileges = grants()[name]
        if spec.domain == "equity":
            assert not any(role.startswith("options_") for role in (*spec.insert, *spec.update, *spec.delete)), name
        if spec.domain == "options":
            assert not any(role in {"compute", "ingress"} for role in (*spec.insert, *spec.update)), name
        assert "reader" not in {role for privilege, roles in privileges.items() if privilege != "SELECT"
                                for role in roles}, name
    for principal, memberships in PRINCIPAL_PROFILES.items():
        assert set(memberships) <= {"owner", *CAPABILITY_ROLES}
    assert not set(PRINCIPAL_PROFILES["runtime_options"]) & {"ingress", "compute", "references", "publication"}
    assert not set(PRINCIPAL_PROFILES["runtime_data"]) & {"options_ingress", "options_compute"}


def test_immutable_bodies_have_triggers_and_local_mock_is_owner_only():
    text = _all_sql()
    for table in ("rt.local_mock_requests", "rt.local_mock_fills"):
        assert RELATIONS[table].retention == "OWNER_ONLY"
        assert not any(grants()[table].values())
    assert text.count("reject_immutable_change") > 100


def test_target_validation_accepts_only_the_v2_name_or_disposable_v2_names():
    assert validate_target_database_v2("stocks_realtime_v2", "stocks_realtime_v2") == "stocks_realtime_v2"
    assert validate_target_database_v2("test_realtime_v2_x", "test_realtime_v2_x", allow_disposable=True)
    for database in ("stocks_realtime_v1", "test_realtime_v1x", "postgres"):
        with pytest.raises(ValueError):
            validate_target_database_v2(database, database, allow_disposable=True)
    with pytest.raises(ValueError):
        validate_target_database_v2("test_realtime_v2_x", "test_realtime_v2_x")
    with pytest.raises(ValueError):
        validate_target_database_v2("stocks_realtime_v2", "other")


def test_role_names_use_prefix_and_stay_within_postgres_limits():
    roles = build_role_names_v2("stocks_realtime_v2")
    assert roles.name("runtime_options") == "stocks_realtime_v2_runtime_options"
    assert all(len(name) <= 63 for name in roles.all_names)
    assert len(set(roles.all_names)) == len(roles.all_names)
    with pytest.raises(ValueError):
        build_role_names_v2("Bad Prefix")


def test_security_mode_is_validated_and_deferred_is_the_default():
    assert SECURITY_MODES_V2 == (SECURITY_MODE_DEFERRED, SECURITY_MODE_HARDENED)
    assert validate_target_database_v2(
        "test_realtime_v2_x", "test_realtime_v2_x", allow_disposable=True,
        security_mode=SECURITY_MODE_HARDENED) == "test_realtime_v2_x"
    with pytest.raises(ValueError):
        validate_target_database_v2("stocks_realtime_v2", "stocks_realtime_v2", security_mode="relaxed")
    # The security mode never widens which database may be targeted.
    with pytest.raises(ValueError):
        validate_target_database_v2("stocks_realtime_v1", "stocks_realtime_v1", security_mode="deferred")
    assert v2_provisioning_phases() == (PHASE_STRUCTURE,)
    assert v2_provisioning_phases(SECURITY_MODE_DEFERRED) == (PHASE_STRUCTURE,)
    assert v2_provisioning_phases(SECURITY_MODE_HARDENED) == (PHASE_STRUCTURE, PHASE_SECURITY)
    with pytest.raises(ValueError):
        v2_provisioning_phases("relaxed")


def test_structural_baseline_creates_no_role_grant_or_ownership_change():
    statements = [
        line.strip().upper() for line in _all_sql().splitlines()
        if not line.strip().startswith("--")
    ]
    assert not [line for line in statements if line.startswith(("GRANT ", "CREATE ROLE", "ALTER ROLE", "SET ROLE"))]
    assert not [line for line in statements if "OWNER TO" in line or "ALTER DEFAULT PRIVILEGES" in line]
    # The only privilege statements allowed are the REVOKEs that keep the SECURITY DEFINER
    # partition helpers private until the hardening phase grants them.
    revokes = [line for line in statements if line.startswith("REVOKE ")]
    assert revokes and all("FROM PUBLIC" in line and "FUNCTION RT." in line for line in revokes)


def test_security_phase_ledger_matches_registry_phases_and_is_seeded_deferred():
    text = _sql("001_realtime_security_policy.sql")
    phases = re.search(r"CHECK \(phase IN \(([^)]*)\)\)", text)
    assert tuple(re.findall(r"'([A-Z]+)'", phases.group(1))) == SECURITY_PHASES
    seed = text[text.index("INSERT INTO rt.security_phase_events"):]
    assert "'DEFERRED'" in seed
    assert "rt.security_phase_events" in RELATIONS
    assert not any(grants()["rt.security_phase_events"].get(privilege) for privilege in ("INSERT", "UPDATE", "DELETE"))


class _Cursor:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, statement, parameters=()):
        self.log.append((statement, parameters))


class _Connection:
    def __init__(self):
        self.log = []

    def cursor(self):
        return _Cursor(self.log)


def test_audit_is_recorded_only_when_every_check_passed():
    connection = _Connection()
    with pytest.raises(RuntimeError, match="capability_privileges_match_registry"):
        record_security_audit_v2(connection, [("capability_privileges_match_registry", "FAIL missing"),
                                              ("no_public_table_privileges", "PASS")])
    assert connection.log == []
    record_security_audit_v2(connection, [("no_public_table_privileges", "PASS")])
    statement, parameters = connection.log[-1]
    assert "INSERT INTO rt.security_phase_events" in statement
    assert parameters[0] == "AUDITED"
    assert json.loads(parameters[1]) == {"checks": 1}


def test_validation_script_plan_reports_phases_for_each_security_mode(monkeypatch, capsys):
    import importlib.util

    path = Path(__file__).resolve().parents[2] / "scripts" / "validate_realtime_v2_schema.py"
    spec = importlib.util.spec_from_file_location("validate_realtime_v2_schema_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    base = ["validate", "--database", "test_realtime_v2_plan", "--confirm-database-name", "test_realtime_v2_plan"]
    for mode, expected in (("deferred", ["STRUCTURE"]), ("hardened", ["STRUCTURE", "SECURITY"])):
        monkeypatch.setattr("sys.argv", [*base, "--security-mode", mode])
        assert module.main() == 0
        plan = json.loads(capsys.readouterr().out.splitlines()[0])
        assert plan["mode"] == "PLAN" and plan["phases"] == expected and plan["security_mode"] == mode
