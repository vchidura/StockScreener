"""Crash -> wait (Nov 6 B3, B11, B9c): a refused plan or a failed cycle reports its cause and waits instead of exiting
into a Docker restart loop; a P8 role failure names its root cause first."""
import importlib
import json
import threading
from datetime import date
from types import SimpleNamespace
from uuid import uuid4

import pytest

from realtime.advanced_ingress_repository import AdvancedIngressAdmissionError
from realtime.advanced_options_p8_runtime import _failure_reason

controller = importlib.import_module("scripts.run_realtime_v2_advanced_controller")
scheduler = importlib.import_module("scripts.run_advanced_options_v3_scheduler")


class _Stop:
    def __init__(self):
        self.waits = []

    def wait(self, seconds):
        self.waits.append(seconds)
        return False

    def is_set(self):
        return False


def test_a_refused_plan_reports_and_waits(capsys):
    stop, plan_id = _Stop(), uuid4()
    controller._wait_for_admissible_plan(
        stop, plan_id, AdvancedIngressAdmissionError("advanced ingress plan is BLOCKED_CONFIGURATION"), 0.2)
    line = json.loads(capsys.readouterr().out.strip())
    assert line["status"] == "WAITING_FOR_ADMISSIBLE_PLAN"
    assert line["plan_id"] == str(plan_id)
    assert "BLOCKED_CONFIGURATION" in line["reason"]
    assert stop.waits == [1.0]


def test_a_p8_role_failure_names_its_innermost_cause():
    try:
        try:
            raise ValueError("snapshot binding is stale")
        except ValueError as cause:
            raise RuntimeError("P8 ACQUISITION pass failed") from cause
    except RuntimeError as error:
        assert _failure_reason(error) == "ValueError: snapshot binding is stale"
    assert _failure_reason(KeyError("x")) == "KeyError: 'x'"


def _scheduler_harness(monkeypatch, outcomes):
    """Runs the scheduler's main loop over scripted cycle outcomes; the last outcome stops it."""
    stop = threading.Event()
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    monkeypatch.setattr(scheduler, "validate_target_database_v2", lambda *a, **k: None)
    pool = SimpleNamespace(closed=False)
    pool.closeall = lambda: setattr(pool, "closed", True)
    monkeypatch.setattr(scheduler, "_provider", lambda: (object(), pool))
    monkeypatch.setattr(scheduler.signal, "signal", lambda *a: None)
    monkeypatch.setattr(scheduler.threading, "Event", lambda: stop)
    monkeypatch.setattr(scheduler, "AdvancedEquityLifecyclePlanner", lambda: SimpleNamespace(
        plan=lambda **_: SimpleNamespace(processing_session_date=date(2026, 11, 9))))
    remaining = list(outcomes)

    def cycle(*_args, **_kwargs):
        outcome = remaining.pop(0)
        if not remaining:
            stop.set()
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    monkeypatch.setattr(scheduler, "run_continuous_options_schedule_cycle", cycle)
    argv = ["--database", "stocks_realtime_v2", "--confirm-database-name", "stocks_realtime_v2",
            "--poll-seconds", "0.001", "--continuous", "--execute"]
    return argv, pool


def test_a_failed_options_cycle_waits_and_the_next_cycle_runs(monkeypatch, capsys):
    argv, pool = _scheduler_harness(monkeypatch, [RuntimeError("snapshot refused"), {"status": "BOUND"}])
    assert scheduler.main(argv) == 0
    lines = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]
    assert lines[0] == {"status": "CYCLE_FAILED_WAITING", "consecutive_failures": 1,
                        "reason": "RuntimeError: snapshot refused"}
    assert lines[1] == {"status": "BOUND"}
    assert pool.closed


def test_a_run_of_failed_cycles_still_exits(monkeypatch, capsys):
    failures = [RuntimeError(f"pool broken {n}") for n in range(scheduler.MAXIMUM_CONSECUTIVE_CYCLE_FAILURES + 1)]
    argv, pool = _scheduler_harness(monkeypatch, failures)
    with pytest.raises(RuntimeError, match="pool broken 19"):
        scheduler.main(argv)
    assert pool.closed
    assert capsys.readouterr().out.count("CYCLE_FAILED_WAITING") == scheduler.MAXIMUM_CONSECUTIVE_CYCLE_FAILURES
