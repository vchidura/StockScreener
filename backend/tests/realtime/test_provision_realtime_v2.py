"""V2 provisioning script: argument gates offline, plus an opt-in run on disposable databases."""
from __future__ import annotations

import importlib
import json
from contextlib import closing

import pytest

from realtime.relation_registry import RELATIONS
from realtime.security_v2 import build_role_names_v2, drop_disposable_roles_v2
from v2_database import _connect, _settings, db_tests_enabled, drop_database_if_exists

provision = importlib.import_module("scripts.provision_realtime_v2_database")
BASE = ["--database", "test_realtime_v2_x", "--confirm-database-name", "test_realtime_v2_x", "--allow-disposable"]


harden = importlib.import_module("scripts.harden_realtime_v2_database")


def test_hardening_script_gates_targets_authorization_and_role_names():
    base = ["--database", "test_realtime_v2_x", "--confirm-database-name", "test_realtime_v2_x", "--allow-disposable"]
    assert harden.parse_args(base).role_prefix == "test_rt_x"
    with pytest.raises(SystemExit):
        harden.parse_args([*base, "--role-prefix", "stocks_realtime_v2"])
    with pytest.raises(ValueError):
        harden.parse_args(["--database", "stocks_realtime_v1", "--confirm-database-name", "stocks_realtime_v1",
                           "--allow-disposable"])
    production = ["--database", "stocks_realtime_v2", "--confirm-database-name", "stocks_realtime_v2"]
    assert harden.parse_args(production).role_prefix == "stocks_realtime_v2"  # plan needs no authorization
    with pytest.raises(SystemExit):
        harden.parse_args([*production, "--execute"])
    assert harden.parse_args([*production, "--execute", "--authorization-reference", "approved"]).execute


@pytest.mark.skipif(not db_tests_enabled(), reason="DB-backed variant is opt-in (STOCKS_V2_DB_TESTS=1)")
def test_hardening_is_rerunnable_fail_closed_and_verified_by_the_runtime_gate(capsys):
    from psycopg2.pool import ThreadedConnectionPool

    from realtime.database_access_v2 import V2AccessConfig, verify_pool_target_v2

    name, prefix = "test_realtime_v2_b5", "test_rt_b5"
    roles = build_role_names_v2(prefix)
    drop_database_if_exists(name)
    argv = ["--database", name, "--confirm-database-name", name, "--allow-disposable"]
    try:
        assert provision.main([*argv, "--execute"]) == 0
        capsys.readouterr()
        settings = _settings()
        pool = ThreadedConnectionPool(1, 2, dbname=name, user=settings["POSTGRES_ADMIN_USER"],
                                      password=settings["POSTGRES_ADMIN_PASSWORD"], host=settings["DB_HOST"],
                                      port=settings["DB_PORT"])
        hardened = V2AccessConfig(security_mode="hardened", role_prefix=prefix, allow_disposable=True)
        try:
            with pytest.raises(RuntimeError, match="requires security phase AUDITED"):
                verify_pool_target_v2(pool, hardened)  # still deferred: a hardened runtime refuses
            assert harden.main([*argv]) == 0 and "No database changes" in capsys.readouterr().out
            assert harden.main([*argv, "--execute"]) == 0
            first = json.loads(capsys.readouterr().out)
            assert first["status"] == "HARDENED" and first["phase_after"] == "AUDITED"
            assert first["roles_created"] == len(roles.all_names) and first["audit"]["failures"] == []
            assert harden.main([*argv, "--execute"]) == 0  # re-run: nothing to create, re-applied, re-audited
            again = json.loads(capsys.readouterr().out)
            assert again["roles_created"] == 0 and again["phase_after"] == "AUDITED"
            assert verify_pool_target_v2(pool, hardened) == name
        finally:
            pool.closeall()
        # An existing role that violates the policy (a LOGIN app role) is refused, never altered.
        with closing(_connect(_settings(), "postgres")) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(f'ALTER ROLE "{roles.name("app")}" LOGIN')
        with pytest.raises(RuntimeError, match="does not match the security policy"):
            harden.main([*argv, "--execute"])
    finally:
        drop_database_if_exists(name)
        with closing(_connect(_settings(), "postgres")) as connection:
            connection.autocommit = True
            drop_disposable_roles_v2(connection, roles)


def test_script_gates_targets_authorization_and_security_scope():
    assert provision.parse_args(BASE).execute is False
    with pytest.raises(ValueError):
        provision.parse_args(["--database", "stocks_realtime_v1", "--confirm-database-name", "stocks_realtime_v1",
                              "--allow-disposable"])
    with pytest.raises(ValueError):
        provision.parse_args(["--database", "test_realtime_v2_x", "--confirm-database-name", "test_realtime_v2_x"])
    production = ["--database", "stocks_realtime_v2", "--confirm-database-name", "stocks_realtime_v2"]
    assert provision.parse_args(production).execute is False  # a plan needs no authorization reference
    with pytest.raises(SystemExit):
        provision.parse_args([*production, "--execute"])
    assert provision.parse_args([*production, "--execute", "--authorization-reference", "approved-2026-10-12"])
    with pytest.raises(SystemExit):
        provision.parse_args([*production, "--security-mode", "hardened"])


@pytest.mark.skipif(not db_tests_enabled(), reason="DB-backed variant is opt-in (STOCKS_V2_DB_TESTS=1)")
def test_deferred_provisioning_is_idempotent_and_ends_in_the_deferred_phase(capsys):
    name = "test_realtime_v2_b3"
    drop_database_if_exists(name)
    argv = ["--database", name, "--confirm-database-name", name, "--allow-disposable"]
    try:
        assert provision.main(argv) == 0 and "No database changes" in capsys.readouterr().out
        assert provision.main([*argv, "--execute"]) == 0
        report = json.loads(capsys.readouterr().out)
        assert report["status"] == "PROVISIONED" and report["security_phase"] == "DEFERRED"
        assert report["structure"]["failures"] == [] and len(report["applied_now"]) == 11
        with pytest.raises(RuntimeError, match="already exists"):
            provision.main([*argv, "--execute"])
        assert provision.main([*argv, "--execute", "--resume"]) == 0
        again = json.loads(capsys.readouterr().out)
        assert again["applied_now"] == [] and again["status"] == "PROVISIONED"
        with closing(_connect(_settings(), name)) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                           "WHERE n.nspname IN ('ref','rt') AND c.relkind IN ('r','p') AND NOT c.relispartition")
            assert cursor.fetchone()[0] == len(RELATIONS)
    finally:
        drop_database_if_exists(name)


@pytest.mark.skipif(not db_tests_enabled(), reason="DB-backed variant is opt-in (STOCKS_V2_DB_TESTS=1)")
def test_hardened_phase_runs_last_on_a_disposable_database(capsys):
    name = "test_realtime_v2_b4"
    drop_database_if_exists(name)
    roles = build_role_names_v2("test_rt_b4")
    try:
        assert provision.main(["--database", name, "--confirm-database-name", name, "--allow-disposable",
                               "--security-mode", "hardened", "--execute"]) == 0
        report = json.loads(capsys.readouterr().out)
        assert report["phases"] == ["STRUCTURE", "SECURITY"] and report["security_phase"] == "AUDITED"
        assert report["security"]["failures"] == []
    finally:
        drop_database_if_exists(name)
        with closing(_connect(_settings(), "postgres")) as connection:
            connection.autocommit = True
            drop_disposable_roles_v2(connection, roles)
