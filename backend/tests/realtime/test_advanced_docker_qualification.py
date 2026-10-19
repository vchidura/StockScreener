import importlib
import json
import subprocess
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from psycopg2 import sql
from psycopg2.pool import ThreadedConnectionPool

from realtime.advanced_ingress_runtime_profile import load_advanced_ingress_runtime_catalog
from realtime.advanced_scheduler import discover_advanced_window_recovery_cursor
from realtime.database_access_v2 import APP_ROLE, V2AccessConfig, V2ConnectionProvider
from realtime.orchestration_repository import PostgresOrchestrationRepository, WindowSpec
from realtime.security_v2 import (
    build_role_names_v2,
    drop_disposable_roles_v2,
    harden_equity_live024_database_v2,
)
from v2_database import (
    _connect,
    _settings,
    disposable_live024_database,
    requires_v2_database,
)


host = importlib.import_module("scripts.qualify_realtime_v2_advanced_docker")
worker = importlib.import_module("scripts.run_realtime_v2_advanced_qualification_worker")
RUN_ID = UUID("11111111-1111-1111-1111-111111111111")


def test_qualification_plan_is_provider_free_and_resource_bounded(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(host, "execute", lambda *_args: pytest.fail("PLAN cannot execute"))
    output = tmp_path / "absent"
    assert host.main(["--output-directory", str(output), "--run-id", str(RUN_ID)]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["status"] == "PLAN"
    assert plan["cpus"] == 2
    assert plan["memory_bytes"] == 2 * 1024**3
    assert plan["maximum_workload_seconds_after_build"] == 600
    assert plan["provider_requests_authorized"] == 0
    assert plan["symbols"] == ["SPY"]
    assert plan["contract_id"] == "equity_live024_compatibility_v1"
    assert plan["schema_sha256"] == (
        "5b25f185dd703cb84ef1553e7f9ac5dc9e5c9ed530cee06823079bfedc97f6ef"
    )
    assert not output.exists()


def test_container_receives_only_owned_secret_and_no_provider_or_shared_env(tmp_path):
    plan = host.qualification_plan(RUN_ID)
    command = host.container_arguments(plan, tmp_path / "input.json", tmp_path / "secret", tmp_path, "cold")
    assert command[command.index("--memory") + 1] == str(2 * 1024**3)
    assert command[command.index("--cpus") + 1] == "2"
    assert "--env-file" not in command
    assert "POLYGON_API_KEY" not in " ".join(command)
    assert ".env" not in " ".join(command)
    assert command.count("--mount") == 3
    assert "--read-only" in command


def test_qualification_plan_records_an_explicit_disposable_session():
    plan = host.qualification_plan(
        RUN_ID,
        fixture_session_date=date(2026, 10, 16),
    )

    assert plan["fixture_session_date"] == "2026-10-16"


def test_reuse_plan_names_the_existing_candidate_without_building(tmp_path, monkeypatch, capsys):
    previous = tmp_path / "previous.json"
    previous.write_text(json.dumps({"image": "qualification:existing"}), encoding="utf-8")
    monkeypatch.setattr(host, "execute", lambda *_args, **_kwargs: pytest.fail("PLAN cannot execute"))
    assert host.main([
        "--output-directory", str(tmp_path / "new"), "--reuse-image-report", str(previous),
    ]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["image"] == "qualification:existing"
    assert plan["reused_image_report"] == str(previous)


@pytest.mark.parametrize("field", ["database", "role_prefix", "symbols", "enabled_stages"])
def test_worker_refuses_scope_expansion(field, monkeypatch):
    for name in worker.PROHIBITED_ENV:
        monkeypatch.delenv(name, raising=False)
    plan = host.qualification_plan(RUN_ID)
    dsn = f"dbname={plan['database']} user={plan['role_prefix']}_worker"
    worker.validate_scope(plan, dsn)
    plan[field] = ["AAPL"] if field in {"symbols", "enabled_stages"} else "stocks_realtime_v2"
    with pytest.raises(ValueError):
        worker.validate_scope(plan, dsn)


def test_worker_refuses_unowned_credentials(monkeypatch):
    plan = host.qualification_plan(RUN_ID)
    dsn = f"dbname={plan['database']} user={plan['role_prefix']}_worker"
    monkeypatch.setenv("POLYGON_API_KEY", "synthetic-unowned-value")
    with pytest.raises(RuntimeError, match="unowned"):
        worker.validate_scope(plan, dsn)
    monkeypatch.delenv("POLYGON_API_KEY")
    with pytest.raises(ValueError, match="unowned login"):
        worker.validate_scope(plan, f"dbname={plan['database']} user=postgres")


def test_worker_rejects_image_environment_or_test_paths(tmp_path):
    worker.validate_image_paths(tmp_path)
    (tmp_path / ".env").write_text("synthetic", encoding="utf-8")
    with pytest.raises(RuntimeError, match="unowned path"):
        worker.validate_image_paths(tmp_path)
    (tmp_path / ".env").unlink()
    (tmp_path / "tests").mkdir()
    with pytest.raises(RuntimeError, match="unowned path"):
        worker.validate_image_paths(tmp_path)


def test_cleanup_never_removes_an_unowned_container(monkeypatch):
    plan = host.qualification_plan(RUN_ID)
    monkeypatch.setattr(host, "inspect_container", lambda _name: {"Config": {"Labels": {}}})
    monkeypatch.setattr(host, "docker", lambda *_args, **_kwargs: pytest.fail("unowned Docker mutation"))
    with pytest.raises(RuntimeError, match="not owned"):
        host.remove_owned_container(plan)


@pytest.mark.parametrize(("code", "error"), [(0, ""), (1, "daemon unavailable")])
def test_image_tag_must_be_proven_new(code, error, monkeypatch):
    monkeypatch.setattr(
        host.subprocess, "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], code, "", error),
    )
    with pytest.raises(RuntimeError):
        host.require_new_image("qualification:new")


@pytest.mark.parametrize("message", ["Error: No such object: owned", "error: no such object: owned"])
def test_absent_container_response_is_case_insensitive(message, monkeypatch):
    monkeypatch.setattr(host.subprocess, "run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 1, "", message))
    assert host.inspect_container("owned") is None


def test_failed_worker_stderr_is_persisted_before_cleanup(tmp_path, monkeypatch):
    plan = host.qualification_plan(RUN_ID)
    container_id = "a" * 64
    monkeypatch.setattr(host, "docker", lambda *_args, **_kwargs: container_id)
    monkeypatch.setattr(host, "inspect_container", lambda _name: {
        "Id": container_id,
        "HostConfig": {"Memory": host.MEMORY_BYTES, "NanoCpus": host.CPUS * 1_000_000_000},
        "State": {"Running": False, "ExitCode": 1, "OOMKilled": False},
    })
    monkeypatch.setattr(host.subprocess, "run", lambda *_args, **_kwargs:
        subprocess.CompletedProcess([], 0, "", "synthetic worker stderr"))
    with pytest.raises(RuntimeError, match="synthetic worker stderr"):
        host.run_phase(plan, tmp_path / "input", tmp_path / "secret", tmp_path,
                       "cold", host.time.monotonic() + 10)
    assert (tmp_path / "cold.log").read_text(encoding="utf-8") == "synthetic worker stderr"


@requires_v2_database
def test_projection_only_qualification_workload_runs_on_disposable_postgresql(tmp_path):
    plan = host.qualification_plan(uuid4())
    roles = build_role_names_v2(plan["role_prefix"])
    owned = False
    login = None
    worker_pool = None
    try:
        with disposable_live024_database(plan["database"]) as pool:
            (
                world,
                seeded,
                cutoff,
                generations,
                history_rows,
                metadata_evidence,
            ) = host.seed_workload(
                pool,
                session_date=date(2026, 10, 16),
            )
            assert world.tickers == ("SPY",)
            assert history_rows > 0
            assert metadata_evidence["status"] == "PASS_REAL_STOCK_METADATA"
            connection = pool.getconn()
            try:
                owned = True
                _, audit = harden_equity_live024_database_v2(
                    connection,
                    roles,
                )
                assert all(state.startswith("PASS") for _, state in audit)
            finally:
                pool.putconn(connection)
            login, dsn = host.create_worker_login(_settings(), plan["database"], roles)
            worker_pool = ThreadedConnectionPool(1, 8, dsn=dsn, options="-c timezone=UTC")
            provider = V2ConnectionProvider(
                {APP_ROLE: worker_pool},
                V2AccessConfig(security_mode="hardened", role_prefix=roles.prefix, allow_disposable=True),
            )
            provider.verify()
            repository = PostgresOrchestrationRepository(provider)
            repository.upsert_readiness(
                seeded.cohort_id, "REFERENCE", "stages_v3", "READY", (),
                "synthetic-existing-seed", datetime.now(timezone.utc),
            )
            manifest = host.workload_manifest(
                plan, world, seeded, cutoff, generations, history_rows,
                datetime.now(timezone.utc) + timedelta(seconds=600),
            )
            profile = load_advanced_ingress_runtime_catalog(
                Path(host.BACKEND_DIR) / "realtime" / "policies" / "advanced_equity_ingress_v1.json",
            ).profile_for_target(1)
            before = worker.snapshot(provider)
            assert before["rt.equity_evidence"] == 0
            journals = worker.replay(provider, manifest, tmp_path, profile.settings)
            assert len(journals) == 2
            release = datetime.fromisoformat(manifest["release_at"])
            spec = WindowSpec(
                seeded.cohort_id, "MASSIVE_ADVANCED", manifest["window_key"],
                release, cutoff, datetime.fromisoformat(manifest["deadline"]),
            )
            window = repository.ensure_window(seeded.plan_id, spec)
            worker.finalize(provider, manifest, window)
            measured = worker.run_window(provider, manifest, window)
            assert measured["seconds"] > 0
            after = worker.snapshot(provider)
            assert after["rt.equity_evidence"] > 0
            with provider.transaction(APP_ROLE, read_only=True) as unit:
                recovery = discover_advanced_window_recovery_cursor(
                    unit.cursor, cohort_id=seeded.cohort_id, cutoff=cutoff,
                    checked_at=datetime.now(timezone.utc),
                )
            assert recovery.complete
            retry = repository.ensure_window(
                seeded.plan_id,
                WindowSpec(seeded.cohort_id, "MASSIVE_ADVANCED", manifest["window_key"] + "-retry1",
                           release, cutoff, spec.deadline),
            )
            worker.run_window(provider, manifest, retry, recovery=recovery)
            assert worker.snapshot(provider) == after
            worker_pool.closeall()
            worker_pool = None
    finally:
        if worker_pool is not None:
            worker_pool.closeall()
        if owned:
            with closing(_connect(_settings(), "postgres")) as connection:
                connection.autocommit = True
                if login is not None:
                    with connection.cursor() as cursor:
                        cursor.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(login)))
                drop_disposable_roles_v2(connection, roles)
