import importlib
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree

import pytest


script = importlib.import_module("scripts.rehearse_realtime_v2_advanced_recovery")


def _runner(monkeypatch, *, exit_code=0, outcome=None):
    def run(command, **kwargs):
        assert "--realtime-no-network" in command
        assert "backend.tests.realtime.offline_io_guard" in command
        assert kwargs["env"]["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] == "1"
        assert kwargs["env"]["PYTEST_ADDOPTS"] == ""
        assert kwargs["timeout"] == 120
        junit = Path(next(value.split("=", 1)[1] for value in command if value.startswith("--junitxml=")))
        root = ElementTree.Element("testsuites")
        suite = ElementTree.SubElement(root, "testsuite")
        nodes = tuple(dict.fromkeys(node for _, _, cases in script.SCENARIOS for node in cases))
        for node in nodes:
            file, name = node.split("::", 1)
            case = ElementTree.SubElement(suite, "testcase", classname=Path(file).stem, name=name)
            if outcome is not None:
                ElementTree.SubElement(case, outcome)
        ElementTree.ElementTree(root).write(junit, encoding="utf-8")
        return SimpleNamespace(returncode=exit_code)

    monkeypatch.setattr(script.subprocess, "run", run)


def test_plan_does_not_run_tests_or_create_a_report(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(script, "run_rehearsal", lambda: pytest.fail("plan must not execute"))
    path = tmp_path / "absent" / "report.json"

    assert script.main(["--report-path", str(path)]) == 0

    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "PLAN"
    assert len(report["scenarios"]) == 16
    assert report["live_qualified"] is False
    assert not path.parent.exists()


def test_execution_requires_new_report_and_preserves_existing_evidence(tmp_path):
    with pytest.raises(SystemExit):
        script.parse_args(["--execute"])
    path = tmp_path / "report.json"
    path.write_text("immutable", encoding="utf-8")
    with pytest.raises(SystemExit):
        script.parse_args(["--execute", "--report-path", str(path)])
    assert path.read_text(encoding="utf-8") == "immutable"
    temporary = tmp_path / "new.json.tmp"
    temporary.write_text("in-progress", encoding="utf-8")
    with pytest.raises(SystemExit):
        script.parse_args(["--execute", "--report-path", str(tmp_path / "new.json")])
    assert temporary.read_text(encoding="utf-8") == "in-progress"


def test_pass_report_is_offline_only_and_written_persistently(tmp_path, monkeypatch, capsys):
    _runner(monkeypatch)
    path = tmp_path / "report.json"

    assert script.main(["--execute", "--report-path", str(path)]) == 0

    report = json.loads(path.read_text(encoding="utf-8"))
    assert report["status"] == "PASS_OFFLINE"
    assert report["contract_version"] == "advanced-equity-p8-offline-v2"
    assert report["production_ready"] is False
    assert report["external_network_allowed"] is False
    assert report["postgresql_connections_allowed"] is False
    assert report["publication_allowed"] is False
    assert len(report["not_qualified"]) == 6
    assert all(row["status"] == "PASS_OFFLINE" for row in report["scenarios"])
    assert json.loads(capsys.readouterr().out)["report_path"] == str(path)


@pytest.mark.parametrize("outcome", ["failure", "error", "skipped"])
def test_failed_errored_or_skipped_cases_cannot_pass(outcome, monkeypatch):
    _runner(monkeypatch, outcome=outcome)

    report = script.run_rehearsal()

    assert report["status"] == "FAIL_OFFLINE"
    assert all(row["status"] == "FAIL_OFFLINE" for row in report["scenarios"])


def test_pytest_nonzero_exit_cannot_pass_even_with_passing_xml(monkeypatch):
    _runner(monkeypatch, exit_code=2)

    report = script.run_rehearsal()

    assert report["status"] == "FAIL_OFFLINE"
    assert report["runner_exit_code"] == 2


def test_missing_or_uncollected_cases_cannot_pass(tmp_path, monkeypatch):
    path = tmp_path / "empty.xml"
    path.write_text("<testsuites/>", encoding="utf-8")
    assert script._case_results(path) == {}
    monkeypatch.setattr(script.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0))

    report = script.run_rehearsal()

    assert report["status"] == "FAIL_OFFLINE"
    assert report["runner_failure"] == "JUNIT_REPORT_MISSING"
    assert report["test_case_count"] == 0
    assert all(row["tests"][0]["outcomes"] == ["NOT_COLLECTED"] for row in report["scenarios"])


def test_all_parameterized_outcomes_must_pass(tmp_path):
    path = tmp_path / "results.xml"
    path.write_text(
        '<testsuites><testsuite>'
        '<testcase classname="backend.tests.realtime.test_fault" name="test_retry[dns]"/>'
        '<testcase classname="backend.tests.realtime.test_fault" name="test_retry[tcp]"><failure/></testcase>'
        '</testsuite></testsuites>',
        encoding="utf-8",
    )

    assert script._case_results(path) == {"test_fault.py::test_retry": ["PASSED", "FAILED"]}


def test_timeout_is_an_explicit_failed_report(monkeypatch):
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], 120)

    monkeypatch.setattr(script.subprocess, "run", timeout)

    report = script.run_rehearsal()

    assert report["status"] == "FAIL_OFFLINE"
    assert report["runner_failure"] == "REHEARSAL_TIMEOUT"
    assert report["runner_exit_code"] is None


def test_invalid_junit_is_an_explicit_failed_report(monkeypatch):
    def corrupt(command, **kwargs):
        path = Path(next(value.split("=", 1)[1] for value in command if value.startswith("--junitxml=")))
        path.write_text("<invalid", encoding="utf-8")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(script.subprocess, "run", corrupt)

    report = script.run_rehearsal()

    assert report["status"] == "FAIL_OFFLINE"
    assert report["runner_failure"] == "JUNIT_REPORT_INVALID"
