import sys
import pytest
from types import SimpleNamespace

from scripts import run_option_worker


def configuration(*, package=False, unavailable=False, launch=None):
    return SimpleNamespace(
        stock_behavior_shadow_launch=launch,
        settings=SimpleNamespace(
            package_assessments_enabled=package,
            outcome_unavailable_evidence_enabled=unavailable,
        ),
    )


def test_continuous_validation_callback_is_absent_when_all_evidence_writers_disabled():
    assert run_option_worker._continuous_validation_callback(configuration()) is None


def test_continuous_validation_callback_reports_enabled_package_framework(monkeypatch):
    report = {
        "storage_state": "READY_ENABLED",
        "framework_storage_ready": True,
        "assessment_count": 12,
        "outcome_unavailable_count": 2,
        "policy_usage": {"assessment_count": 12},
    }
    monkeypatch.setitem(
        sys.modules,
        "report_option_package_assessments",
        SimpleNamespace(build_report=lambda hours: report),
    )
    written = []
    monkeypatch.setattr(
        run_option_worker,
        "_write_validation_artifact",
        lambda path, payload: written.append((path, payload)),
    )
    callback = run_option_worker._continuous_validation_callback(
        configuration(package=True)
    )

    result = callback(None)

    assert result == {"package_outcomes": report}
    assert written[0][0].as_posix().endswith(
        "backups/options-package-assessments/status.json"
    )
    assert written[0][1] == report


def test_evidence_runtime_monitor_is_bounded_and_samples_failures():
    from options.evidence_runtime import EvidenceRuntimeMonitor

    readings = iter(range(1000, 2000))
    times = iter(range(1000))
    monitor = EvidenceRuntimeMonitor(memory=lambda: next(readings), timer=lambda: next(times))
    for _ in range(102):
        assert monitor.run("STOCK_BEHAVIOR", lambda: "done") == "done"
    with pytest.raises(RuntimeError):
        monitor.run("PACKAGE", lambda: (_ for _ in ()).throw(RuntimeError("failed")))
    summary = monitor.summary()
    assert summary["sample_count"] == 100
    assert summary["memory_sample_count"] == 100
    assert summary["stages"]["PACKAGE"]["failures"] == 1
    assert summary["stages"]["PACKAGE"]["p95_elapsed_seconds"] == 1
    assert summary["stages"]["PACKAGE"]["maximum_rss_delta_bytes"] == 1
    assert summary["acceptance"] == "BASELINE_AND_PEAK_MEASUREMENT_REQUIRED"


def test_memory_probe_failure_does_not_fail_evidence_work():
    from options.evidence_runtime import EvidenceRuntimeMonitor

    monitor = EvidenceRuntimeMonitor(memory=lambda: (_ for _ in ()).throw(OSError("unavailable")))
    assert monitor.run("PACKAGE", lambda: 42) == 42
    assert monitor.summary()["state"] == "PENDING_REALTIME_EVIDENCE"


def test_continuous_report_attaches_measured_but_not_accepted_memory(monkeypatch):
    from options.evidence_runtime import EvidenceRuntimeMonitor

    monitor = EvidenceRuntimeMonitor(memory=lambda: 100)
    monitor.run("STOCK_BEHAVIOR", lambda: None)
    report = {"launch": {"state": "RUNNING"}}
    monkeypatch.setitem(sys.modules, "report_option_stock_behavior_assessments", SimpleNamespace(build_report=lambda *_: report))
    written = []
    monkeypatch.setattr(run_option_worker, "_write_validation_artifact", lambda path, payload: written.append(payload))
    launch = SimpleNamespace(artifact_destination="backups/test.json", launch_id="test", sha256="a" * 64)
    run_option_worker._continuous_validation_callback(configuration(launch=launch), monitor)(None)
    evidence = written[0]["realtime_evidence_validations"]["WORKER_MEMORY_OVERHEAD"]
    assert evidence["state"] == "MEASURED"
    assert evidence["reason"] == "BASELINE_AND_PEAK_MEASUREMENT_REQUIRED"