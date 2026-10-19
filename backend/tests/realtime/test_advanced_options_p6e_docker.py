import json
from uuid import UUID

import pytest

from scripts import qualify_advanced_options_p6e_docker as host
from scripts import run_advanced_options_p6e_container as worker


RUN_ID = UUID("11111111-1111-1111-1111-111111111111")


def test_p6e_plan_is_provider_free_bounded_and_inert(
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.setattr(
        host,
        "execute",
        lambda *_args: pytest.fail("plan mode executed"),
    )
    assert host.main([
        "--output-directory",
        str(tmp_path / "absent"),
        "--run-id",
        str(RUN_ID),
    ]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["provider_requests_authorized"] == 0
    assert not plan["publication_authorized"]
    assert not plan["archive_retention_authorized"]
    assert not plan["execution_authorized"]
    assert not plan["delivery_authorized"]
    assert plan["cpus"] == 2
    assert plan["memory_bytes"] == 2 * 1024**3
    assert not (tmp_path / "absent").exists()


def test_container_is_no_network_and_receives_no_credentials(tmp_path):
    plan = host.qualification_plan(RUN_ID)
    arguments = host._container_arguments(
        plan,
        tmp_path / "manifest.json",
        tmp_path,
    )

    assert arguments[arguments.index("--network") + 1] == "none"
    assert "--env-file" not in arguments
    assert not any("DSN" in value or "API_KEY" in value for value in arguments)
    assert "--read-only" in arguments
    assert ("--cap-drop", "ALL") == (
        arguments[arguments.index("--cap-drop")],
        arguments[arguments.index("--cap-drop") + 1],
    )


def test_worker_rejects_scope_or_unowned_environment(monkeypatch):
    plan = host.qualification_plan(RUN_ID)
    for name in worker.PROHIBITED_ENV:
        monkeypatch.delenv(name, raising=False)
    worker.validate_scope(plan)
    changed = dict(plan, provider_requests_authorized=1)
    with pytest.raises(ValueError, match="scope changed"):
        worker.validate_scope(changed)
    monkeypatch.setenv("POLYGON_API_KEY", "unowned")
    with pytest.raises(RuntimeError, match="unowned"):
        worker.validate_scope(plan)
