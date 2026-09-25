from datetime import datetime, timezone
import json
from types import SimpleNamespace

from scripts import run_option_outcome_worker as worker


NOW = datetime(2026, 9, 23, 18, 0, tzinfo=timezone.utc)


def write_detector(path, *, status="RECORDED", cycle="2026-09-23T17:30:00+00:00"):
    path.write_text(json.dumps({"version": "option_detector_operational_status_v1",
        "dataset_id": "fixture-v1", "attempts": [{"scheduled_cycle": cycle,
            "finished_at": "2026-09-23T17:59:00+00:00", "status": status,
            "run_id": "run-one"}]}), encoding="utf-8")


def outcomes():
    return SimpleNamespace(candidates=200, due_measurements=1000,
        available_measurements=20, persisted=20, pending=0,
        current_candidates=50, current_persisted=50,
        unavailable_measurements=2, unavailable_persisted=2)


def test_outcome_worker_processes_each_finished_cycle_once(tmp_path):
    detector = tmp_path / "detector.json"
    status = tmp_path / "status.json"
    write_detector(detector)
    calls = []
    service = SimpleNamespace(mature=lambda **kwargs: calls.append(kwargs) or outcomes())

    first = worker.process_once(service, clock=lambda: NOW,
        detector_status=detector, status_path=status)
    second = worker.process_once(service, clock=lambda: NOW,
        detector_status=detector, status_path=status)

    assert first["state"] == "COMPLETE" and first["completed_cycle"] == "2026-09-23T17:30:00+00:00"
    assert first["persisted"] == 20 and first["current_persisted"] == 50
    assert second["state"] == "WAITING_FOR_NEW_CYCLE"
    assert calls == [{"available_by": NOW, "limit": 1000}]


def test_outcome_worker_waits_while_detector_cycle_is_running(tmp_path):
    detector = tmp_path / "detector.json"
    status = tmp_path / "status.json"
    detector.write_text(json.dumps({"version": "option_detector_operational_status_v1",
        "dataset_id": "fixture-v1", "attempts": [{"scheduled_cycle": NOW.isoformat(),
            "started_at": NOW.isoformat(), "status": "RUNNING"}]}), encoding="utf-8")
    service = SimpleNamespace(mature=lambda **_: (_ for _ in ()).throw(
        AssertionError("running detector cycle cannot trigger outcomes")))

    result = worker.process_once(service, clock=lambda: NOW,
        detector_status=detector, status_path=status)

    assert result["state"] == "WAITING_FOR_FINISHED_CYCLE"


def test_outcome_worker_accepts_finished_incomplete_cycle(tmp_path):
    detector = tmp_path / "detector.json"
    status = tmp_path / "status.json"
    write_detector(detector, status="INCOMPLETE")
    service = SimpleNamespace(mature=lambda **_: outcomes())

    result = worker.process_once(service, clock=lambda: NOW,
        detector_status=detector, status_path=status)

    assert result["state"] == "COMPLETE" and result["source_status"] == "INCOMPLETE"
