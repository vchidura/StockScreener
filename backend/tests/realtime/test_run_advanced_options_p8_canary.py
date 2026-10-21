from __future__ import annotations

import json
from datetime import datetime, timezone

from scripts import run_advanced_options_p8_canary as script


UTC = timezone.utc
CHECKED = datetime(2026, 10, 21, 18, 30, tzinfo=UTC)


class _FakeExecutor:
    def __init__(self, port, **kwargs):
        self.port = port
        self.kwargs = kwargs

    def plan(self, package):
        return {"mode": "PLAN", "package": package}

    def execute(self, package, *, checked_at, stored_reads_authorized):
        return {
            "mode": "EXECUTE",
            "package": package,
            "checked_at": checked_at.isoformat(),
            "stored_reads_authorized": stored_reads_authorized,
            "port": self.port,
        }


def _base_args(package, qualification, output):
    return [
        "--package",
        str(package),
        "--qualification-report",
        str(qualification),
        "--expected-qualification-semantic-sha256",
        "a" * 64,
        "--expected-qualification-file-sha256",
        "b" * 64,
        "--expected-package-sha256",
        "c" * 64,
        "--source-commit",
        "d" * 40,
        "--output",
        str(output),
    ]


def test_cli_plan_has_no_runtime_factory_requirement(tmp_path, monkeypatch):
    package = tmp_path / "package.json"
    qualification = tmp_path / "qualification.json"
    output = tmp_path / "plan.json"
    package.write_text('{"package_sha256":"c"}', encoding="utf-8")
    qualification.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(script, "P8CanaryExecutor", _FakeExecutor)

    assert script.main(_base_args(package, qualification, output)) == 0
    assert json.loads(output.read_text(encoding="utf-8")) == {
        "mode": "PLAN",
        "package": {"package_sha256": "c"},
    }


def test_cli_execute_uses_one_composed_runtime_port(
    tmp_path,
    monkeypatch,
):
    package = tmp_path / "package.json"
    qualification = tmp_path / "qualification.json"
    output = tmp_path / "execute.json"
    package.write_text('{"package_sha256":"c"}', encoding="utf-8")
    qualification.write_text("{}", encoding="utf-8")
    runtime_port = object()
    monkeypatch.setattr(script, "P8CanaryExecutor", _FakeExecutor)
    monkeypatch.setattr(
        script,
        "build_advanced_options_p8_runtime",
        lambda **kwargs: {
            "factory": "repository-owned",
            "port": runtime_port is not None,
            "kwargs": sorted(kwargs),
        },
    )
    args = [
        *_base_args(package, qualification, output),
        "--execute",
        "--checked-at",
        CHECKED.isoformat(),
    ]

    assert script.main(args) == 0
    stored = json.loads(output.read_text(encoding="utf-8"))
    assert stored["mode"] == "EXECUTE"
    assert stored["checked_at"] == CHECKED.isoformat()
    assert stored["stored_reads_authorized"] is False
    assert stored["port"]["factory"] == "repository-owned"
    assert stored["port"]["kwargs"] == [
        "package",
        "package_path",
        "repository_root",
        "stored_reads_authorized",
    ]
