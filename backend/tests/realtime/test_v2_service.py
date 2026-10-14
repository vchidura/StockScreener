"""V2 service driver: window polling and independent resident stage loops (offline, in-memory)."""
from __future__ import annotations

import importlib
import threading
import time
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest

from orchestration_fakes import InMemoryOrchestrationRepository
from realtime.orchestration_repository import ServicePlan, WindowSpec
from realtime.stage_graph import STAGES_V1
from realtime.stage_orchestrator import StageOrchestrator, StageOutcome
from realtime.stage_runners_v2 import build_runners, unwired_stages, wired_stages
from realtime.v2_service import V2Service, window_specs_for_session

NOW = datetime(2026, 10, 12, 15, 0, tzinfo=timezone.utc)
ALL = frozenset(STAGES_V1)


class Calendar:
    def session_open(self, session_date):
        return datetime(2026, 10, 12, 13, 30, tzinfo=timezone.utc)

    def session_close(self, session_date):
        return datetime(2026, 10, 12, 20, 0, tzinfo=timezone.utc)


def _service(repository_cls=InMemoryOrchestrationRepository, runners=None, specs=None, clock=None):
    plan = ServicePlan(uuid4(), uuid4(), date(2026, 10, 12), "stages_v1", ALL, "a" * 64, NOW + timedelta(hours=8))
    repository = repository_cls(plan)
    repository.upsert_readiness(plan.cohort_id, "REFERENCE", "stages_v1", "READY", (), None, NOW)
    table = {name: (lambda context: StageOutcome.complete()) for name in ALL}
    table.update(runners or {})
    orchestrator = StageOrchestrator(repository, table, clock=clock or (lambda: NOW))
    if specs is None:
        specs = [WindowSpec(plan.cohort_id, "MASSIVE_DEVELOPER", f"w{index}", NOW - timedelta(minutes=10 * (2 - index)),
                            NOW - timedelta(minutes=10 * (2 - index) + 15), NOW + timedelta(minutes=30))
                 for index in range(4)]
    return V2Service(orchestrator, repository, plan.plan_id, specs, clock=lambda: NOW,
                     resident_intervals={"REFERENCE": 0.01, "OPTIONS_FLOW": 0.01}), repository, plan


def test_window_specs_follow_the_developer_schedule_and_keep_the_key_stable():
    cohort = uuid4()
    specs = window_specs_for_session(cohort, date(2026, 10, 12), "MASSIVE_DEVELOPER", completion_budget_seconds=60,
                                     serving_lag_seconds=30, calendar=Calendar())
    assert specs and len({spec.window_key for spec in specs}) == len(specs)
    assert all(spec.cutoff <= spec.release_at < spec.deadline for spec in specs)
    assert specs[0].cutoff == datetime(2026, 10, 12, 13, 45, tzinfo=timezone.utc)
    assert specs[0].window_key == "2026-10-12T13:45:00+00:00" and specs[0].window_id == specs[0].window_id


def test_poll_runs_due_windows_in_order_and_leaves_future_windows_alone():
    service, repository, plan = _service()
    report = service.poll_windows()
    assert [window.window_state for window in report.windows] == ["COMPLETE"] * 3  # w3 is released in the future
    assert [row["key"] for row in sorted(repository.windows.values(), key=lambda row: row["release"])] == [
        "w0", "w1", "w2"]
    assert service.poll_windows().windows == []  # nothing left to claim: terminal windows are not re-run


def test_a_failing_window_does_not_stop_the_next_window():
    class Flaky(InMemoryOrchestrationRepository):
        def claim_window(self, window_id, owner_id, now, lease_seconds):
            if self.windows[window_id]["key"] == "w1":
                raise ConnectionError("database restarted")
            return super().claim_window(window_id, owner_id, now, lease_seconds)

    service, _repository, _plan = _service(Flaky)
    report = service.poll_windows()
    assert report.window_errors == {"w1": "ConnectionError"}
    assert [window.window_state for window in report.windows] == ["COMPLETE", "COMPLETE"]


def test_resident_ticks_report_failures_through_readiness_and_isolate_repository_errors():
    def flow_down(context):
        raise ConnectionError("feed down")

    service, repository, plan = _service(runners={"OPTIONS_FLOW": flow_down})
    service._orchestrator.start(plan.plan_id)
    assert service.tick_resident_once("OPTIONS_FLOW") == "FAILED"
    assert repository.readiness(plan.cohort_id)["OPTIONS_FLOW"] == "BLOCKED"
    assert service.tick_resident_once("REFERENCE") == "COMPLETE"

    class Broken(InMemoryOrchestrationRepository):
        def upsert_readiness(self, cohort_id, stage_id, *args):
            if stage_id == "OPTIONS_FLOW":
                raise ConnectionError("options principal is locked")
            return super().upsert_readiness(cohort_id, stage_id, *args)

    service, _repository, plan = _service(Broken)
    service._orchestrator.start(plan.plan_id)
    assert service.tick_resident_once("OPTIONS_FLOW") == "ERROR"
    assert service.resident_errors == {"OPTIONS_FLOW": "ConnectionError"}
    assert service.tick_resident_once("REFERENCE") == "COMPLETE"


def test_a_stuck_reference_refresh_never_delays_windows_or_the_options_flow_loop():
    release_reference = threading.Event()
    flow_ticks, reference_started = [], threading.Event()

    def stuck_reference(context):
        reference_started.set()
        release_reference.wait(timeout=30)
        return StageOutcome.complete()

    def flow(context):
        flow_ticks.append(time.monotonic())
        return StageOutcome.complete()

    service, repository, plan = _service(runners={"REFERENCE": stuck_reference, "OPTIONS_FLOW": flow})
    service._orchestrator.start(plan.plan_id)
    stop = threading.Event()
    runner = threading.Thread(target=service.run, args=(stop,), kwargs={"poll_seconds": 0.01}, daemon=True)
    runner.start()
    try:
        assert reference_started.wait(timeout=10)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and not (
                len(flow_ticks) >= 3 and sum(row["state"] == "COMPLETE" for row in repository.windows.values()) >= 3):
            time.sleep(0.01)
        assert len(flow_ticks) >= 3
        assert sum(row["state"] == "COMPLETE" for row in repository.windows.values()) == 3
    finally:
        release_reference.set()
        stop.set()
        runner.join(timeout=15)
    assert not runner.is_alive()


def test_runner_bindings_fail_closed_until_a_stage_is_wired():
    options = {name for name in ALL if name.startswith("OPTIONS_")}
    assert wired_stages() == ALL - options | {"OPTIONS_FLOW"}
    assert unwired_stages() == options - {"OPTIONS_FLOW"}
    with pytest.raises(LookupError, match="OPTIONS_ACQUIRE"):
        build_runners({"OPTIONS_ACQUIRE"}, None)
    with pytest.raises(LookupError, match="OPTIONS_CYCLE"):
        build_runners({"EQUITY_EVIDENCE", "OPTIONS_CYCLE"}, None)
    assert set(build_runners(set(wired_stages()), None)) == wired_stages()


def test_entry_script_refuses_hardened_runtime_unconfirmed_names_and_unauthorized_mutation():
    script = importlib.import_module("scripts.run_realtime_v2_orchestrator")
    base = ["--database", "test_realtime_v2_x", "--confirm-database-name", "test_realtime_v2_x",
            "--allow-disposable", "--plan-id", str(uuid4())]
    assert script.parse_args(["plan", *base]).mode == "plan"
    with pytest.raises(SystemExit):
        script.parse_args(["once", *base])
    with pytest.raises(SystemExit):
        script.parse_args(["plan", *base, "--security-mode", "hardened"])
    with pytest.raises(ValueError):
        script.parse_args(["plan", "--database", "test_realtime_v2_x", "--confirm-database-name",
                           "test_realtime_v2_x", "--plan-id", str(uuid4())])
    with pytest.raises(ValueError):
        script.parse_args(["plan", "--database", "stocks_realtime_v1", "--confirm-database-name",
                           "stocks_realtime_v1", "--allow-disposable", "--plan-id", str(uuid4())])
    assert script.parse_args(["once", *base, "--execute"]).execute is True
