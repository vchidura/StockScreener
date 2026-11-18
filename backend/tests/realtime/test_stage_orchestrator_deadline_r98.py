"""R98 (Nov 17 19:27): an Options V3 window whose deadline passed mid-run, before its input gate started, made
`record_missed` insert MISSED rows that rt.validate_advanced_options_v3_stage_effect refuses (RT003: no consumed
cycle authority). The DatabaseError ended the WINDOW role and left the window RUNNING past its deadline.

Now the window closes MISSED: the repository records no row for a gated stages_v5 stage whose cycle authority was
never consumed, and any database error while closing a window past its deadline is logged and reported as a fault
instead of escaping. These tests drive windows past their deadline mid-run against an in-memory repository that
models the V3 trigger, then show the next window (and the WINDOW role loop) runs normally.
"""
from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import psycopg2

from realtime import advanced_options_p8_production as production
from realtime import advanced_options_p8_roles as roles_module
from realtime.advanced_options_service import OPTIONS_V3_STAGE_ORDER
from realtime.fault_journal import parse_fault_line
from realtime.orchestration_repository import (
    V5_TARGET_WINDOW_EFFECT_STAGES,
    PostgresOrchestrationRepository,
    ServicePlan,
    WindowClaim,
    WindowRecord,
    WindowSpec,
)
from realtime.stage_graph import STAGE_CATALOG_VERSION_V5, STAGES_V5
from realtime.stage_orchestrator import (
    DEADLINE_CLOSE_FAILED,
    WINDOW_DEADLINE_RECORD_FAILED,
    StageOrchestrator,
    StageOutcome,
)

from orchestration_fakes import InMemoryOrchestrationRepository

UTC = timezone.utc
RELEASE = datetime(2026, 11, 17, 19, 22, 7, tzinfo=UTC)
CRASHED_AT = datetime(2026, 11, 17, 19, 27, 25, tzinfo=UTC)
GATED = sorted(V5_TARGET_WINDOW_EFFECT_STAGES)
UNGATED = ["OPTIONS_ACQUIRE", "OPTIONS_ADMISSION", "OPTIONS_REFERENCE"]
WINDOW_STAGES = sorted([*UNGATED, *GATED])
TRIGGER_MESSAGE = "stages_v5 target-window effect requires a consumed cycle authority"


class _TriggerRefused(psycopg2.DatabaseError):
    """What psycopg2 raises for the trigger's RAISE ... USING ERRCODE = 'RT003' (an unknown SQLSTATE class)."""

    pgcode = "RT003"


class _V3Database(InMemoryOrchestrationRepository):
    """The in-memory repository plus the V3 rules it leaves out: OPTIONS_INPUT_GATE's start consumes the window's
    cycle authority (PostgresOrchestrationRepository.start_stage), and until then the trigger refuses any other row
    of a gated stages_v5 stage. `fixed` mirrors the fixed PostgresOrchestrationRepository.record_missed (no row, None);
    without it record_missed reaches the trigger, as every record_missed did before R98."""

    def __init__(self, plan, *, fixed: bool) -> None:
        super().__init__(plan, STAGES_V5)
        self.fixed = fixed
        self.consumed: set = set()
        self.finish_error: Exception | None = None

    def _insert(self, claim, stage_id, version, attempt, state, **kwargs):
        if (version == STAGE_CATALOG_VERSION_V5 and stage_id in V5_TARGET_WINDOW_EFFECT_STAGES
                and state != "UNAVAILABLE" and claim.window.window_id not in self.consumed):
            raise _TriggerRefused(TRIGGER_MESSAGE)
        return super()._insert(claim, stage_id, version, attempt, state, **kwargs)

    def start_stage(self, claim, stage, version, attempt, maximum_attempts, now):
        with self.lock:
            self._fence(claim)
            if version == STAGE_CATALOG_VERSION_V5 and stage.stage_id == "OPTIONS_INPUT_GATE":
                self.consumed.add(claim.window.window_id)
            return super().start_stage(claim, stage, version, attempt, maximum_attempts, now)

    def record_missed(self, claim, stage_id, version, reason, now):
        if self.fixed and version == STAGE_CATALOG_VERSION_V5 and stage_id in V5_TARGET_WINDOW_EFFECT_STAGES:
            with self.lock:
                self._fence(claim)
                if claim.window.window_id not in self.consumed:
                    return None
        return super().record_missed(claim, stage_id, version, reason, now)

    def finish_window(self, claim, state, now):
        if self.finish_error is not None and state == "MISSED":
            raise self.finish_error
        return super().finish_window(claim, state, now)


class _Clock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class _Harness:
    def __init__(self, *, fixed: bool) -> None:
        self.plan = ServicePlan(uuid4(), uuid4(), date(2026, 11, 17), STAGE_CATALOG_VERSION_V5,
                                frozenset(OPTIONS_V3_STAGE_ORDER), "a" * 64, RELEASE + timedelta(hours=2))
        self.repository = _V3Database(self.plan, fixed=fixed)
        self.clock = _Clock(RELEASE)
        self.invoked: list[tuple[str, str]] = []
        self.stall_next_reference = False
        self.repository.upsert_readiness(self.plan.cohort_id, "REFERENCE", STAGE_CATALOG_VERSION_V5, "READY", (),
                                         None, RELEASE)

    def window(self, cutoff: datetime, *, release_delay=timedelta(seconds=127)) -> WindowRecord:
        release = cutoff + release_delay
        spec = WindowSpec(self.plan.cohort_id, "MASSIVE_ADVANCED", f"E:{cutoff:%Y-%m-%dT%H:%M}", release, cutoff,
                          release + timedelta(minutes=5))
        return self.repository.ensure_window(self.plan.plan_id, spec)

    def orchestrator(self) -> StageOrchestrator:
        def runner(name):
            def run(context):
                self.invoked.append((context.window.window_key, name))
                if name == "OPTIONS_REFERENCE" and self.stall_next_reference:
                    # The network outage: this stage returns after the window's deadline, before the input gate.
                    self.stall_next_reference = False
                    self.clock.value = CRASHED_AT
                return StageOutcome.complete()
            return run

        # As build_advanced_options_v3_runtime_composition composes it.
        return StageOrchestrator(self.repository, {name: runner(name) for name in OPTIONS_V3_STAGE_ORDER},
                                 stages=STAGES_V5, catalog_version=STAGE_CATALOG_VERSION_V5, clock=self.clock,
                                 parallel_lanes=False)

    def states(self, window) -> dict[str, str]:
        return {stage: record.state for stage, record in self.repository.stage_states(window.window_id).items()}

    def window_state(self, window) -> str:
        return self.repository.windows[window.window_id]["state"]


def _lines(text: str) -> list[dict]:
    return [json.loads(line) for line in text.splitlines() if line.startswith("{")]


def _faults(text: str) -> list[dict]:
    return [record for record in map(parse_fault_line, text.splitlines()) if record is not None]


def test_a_v3_window_past_its_deadline_before_the_input_gate_closes_missed_and_the_next_runs(capsys):
    harness = _Harness(fixed=True)
    missed = harness.window(datetime(2026, 11, 17, 19, 20, tzinfo=UTC))
    later = harness.window(datetime(2026, 11, 17, 19, 25, tzinfo=UTC))
    orchestrator = harness.orchestrator()
    harness.stall_next_reference = True

    report = orchestrator.run_window(missed.window_id)

    assert (report.status, report.window_state) == ("FINISHED", "MISSED")
    assert harness.window_state(missed) == "MISSED"
    # The session stages hold their rows; the gated stages hold none, so no target-window effect was recorded.
    assert harness.states(missed) == dict.fromkeys(UNGATED, "COMPLETE")
    assert missed.window_id not in harness.repository.consumed
    assert sorted(name for name, state in report.executed if state == "MISSED") == GATED
    readiness = {stage: harness.repository.ready[(harness.plan.cohort_id, stage)] for stage in GATED}
    assert set(readiness.values()) == {("BLOCKED", ("WINDOW_DEADLINE_PASSED",))}
    output = capsys.readouterr()
    assert _lines(output.out) == [{
        "event": "window_deadline_stages_missed_without_rows",
        "window_id": str(missed.window_id),
        "window_key": "E:2026-11-17T19:20",
        "stages": GATED,
        "reason": "WINDOW_CYCLE_AUTHORITY_NOT_CONSUMED",
    }]
    assert _faults(output.err) == []

    following = orchestrator.run_window(later.window_id)

    assert (following.status, following.window_state) == ("FINISHED", "COMPLETE")
    assert harness.states(later) == dict.fromkeys(WINDOW_STAGES, "COMPLETE")
    assert capsys.readouterr().out == ""


def test_a_trigger_refusal_while_closing_a_missed_window_is_reported_and_does_not_escape(capsys):
    harness = _Harness(fixed=False)  # record_missed reaches the trigger, as before R98
    stalled = harness.window(datetime(2026, 11, 17, 19, 20, tzinfo=UTC))
    claimed_late = harness.window(datetime(2026, 11, 17, 19, 15, tzinfo=UTC))
    later = harness.window(datetime(2026, 11, 17, 19, 25, tzinfo=UTC))
    orchestrator = harness.orchestrator()
    harness.stall_next_reference = True

    first = orchestrator.run_window(stalled.window_id)
    first_output = capsys.readouterr()
    second = orchestrator.run_window(claimed_late.window_id)  # claimed after its deadline: nothing runs
    second_output = capsys.readouterr()

    for report, window in ((first, stalled), (second, claimed_late)):
        assert (report.status, report.window_state) == ("FINISHED", "MISSED")
        assert harness.window_state(window) == "MISSED"
    assert harness.states(stalled) == dict.fromkeys(UNGATED, "COMPLETE")
    assert harness.states(claimed_late) == dict.fromkeys(UNGATED, "MISSED")
    assert not any(key == claimed_late.window_key for key, _name in harness.invoked)
    lines = _lines(first_output.out)
    assert [(line["event"], line["step"], line["stage_id"], line["sqlstate"]) for line in lines] == [
        ("window_deadline_record_failed", "RECORD_MISSED", stage, "RT003") for stage in GATED
    ]
    assert {line["window_key"] for line in lines} == {"E:2026-11-17T19:20"}
    assert all(line["error"] == f"_TriggerRefused:{TRIGGER_MESSAGE}" for line in lines)
    first_faults = _faults(first_output.err)
    second_faults = _faults(second_output.err)
    assert [fault["kind"] for fault in first_faults] == [WINDOW_DEADLINE_RECORD_FAILED] * len(GATED)
    assert {fault["severity"] for fault in first_faults} == {"ERROR"}
    assert [fault["context"]["stage_id"] for fault in first_faults] == GATED
    # A stable key: the same refusal in another window folds into the same journal entries.
    assert [fault["fingerprint"] for fault in second_faults] == [fault["fingerprint"] for fault in first_faults]
    assert len({fault["fingerprint"] for fault in first_faults}) == len(GATED)

    following = orchestrator.run_window(later.window_id)

    assert (following.status, following.window_state) == ("FINISHED", "COMPLETE")
    assert harness.states(later) == dict.fromkeys(WINDOW_STAGES, "COMPLETE")
    assert _faults(capsys.readouterr().err) == []


def test_a_database_error_closing_the_window_leaves_it_for_self_heal_and_does_not_escape(capsys):
    harness = _Harness(fixed=True)
    missed = harness.window(datetime(2026, 11, 17, 19, 20, tzinfo=UTC))
    later = harness.window(datetime(2026, 11, 17, 19, 25, tzinfo=UTC))
    harness.repository.finish_error = psycopg2.OperationalError("server closed the connection unexpectedly")
    orchestrator = harness.orchestrator()
    harness.stall_next_reference = True

    report = orchestrator.run_window(missed.window_id)

    assert (report.status, report.window_state) == (DEADLINE_CLOSE_FAILED, None)
    assert harness.window_state(missed) == "RUNNING"  # its lease ends; self-heal WINDOW_PAST_DEADLINE reports it
    output = capsys.readouterr()
    failed = [line for line in _lines(output.out) if line["event"] == "window_deadline_record_failed"]
    assert [(line["step"], line["stage_id"], line["sqlstate"]) for line in failed] == [
        ("FINISH_WINDOW", None, "OperationalError")
    ]
    assert [(fault["kind"], fault["context"]["step"]) for fault in _faults(output.err)] == [
        (WINDOW_DEADLINE_RECORD_FAILED, "FINISH_WINDOW")
    ]

    harness.repository.finish_error = None
    following = orchestrator.run_window(later.window_id)

    assert (following.status, following.window_state) == ("FINISHED", "COMPLETE")


def test_a_v3_window_finishing_before_its_deadline_writes_no_deadline_record(capsys):
    harness = _Harness(fixed=True)
    window = harness.window(datetime(2026, 11, 17, 19, 20, tzinfo=UTC))

    report = harness.orchestrator().run_window(window.window_id)

    assert (report.status, report.window_state) == ("FINISHED", "COMPLETE")
    assert harness.states(window) == dict.fromkeys(WINDOW_STAGES, "COMPLETE")
    output = capsys.readouterr()
    assert output.out == "" and _faults(output.err) == []


def test_the_window_role_keeps_running_when_a_window_misses_its_deadline_mid_run(monkeypatch, capsys):
    harness = _Harness(fixed=False)  # the worst case: the refusal reaches the orchestrator
    missed = harness.window(datetime(2026, 11, 17, 19, 20, tzinfo=UTC))
    later = harness.window(datetime(2026, 11, 17, 19, 25, tzinfo=UTC))
    orchestrator = harness.orchestrator()
    harness.stall_next_reference = True
    stop = threading.Event()

    def serial(*, binding, **_kwargs):
        # run_v3_serial_window -> build_and_run_p8_serial_window, from the orchestrator call on.
        report = orchestrator.run_window(binding.window_id)
        if binding.window_id == later.window_id:
            stop.set()
        return {
            "status": report.status,
            "window_state": report.window_state,
            "stage_order": list(OPTIONS_V3_STAGE_ORDER),
            "executed": [list(row) for row in report.executed],
            "lane_errors": dict(report.lane_errors),
        }

    bindings = (SimpleNamespace(window_id=missed.window_id), SimpleNamespace(window_id=later.window_id))
    monkeypatch.setattr(roles_module, "validate_advanced_options_v3_runtime_identity", lambda *_args: None)
    monkeypatch.setattr(roles_module, "_v3_resident_dependencies",
                        lambda *_args, **_kwargs: {"FLOW": {"status": "COMPLETE"}})
    monkeypatch.setattr(production, "load_v3_pending_window_bindings", lambda **_kwargs: bindings)
    monkeypatch.setattr(production, "run_v3_serial_window", serial)
    package = {"orchestration": {"catalog_version": STAGE_CATALOG_VERSION_V5}}

    result = roles_module._run_window(
        package=package,
        bundle={"orchestration": dict(package["orchestration"])},
        binding=bindings[0],
        provider=object(),
        pool=object(),
        environment={},
        stop_event=stop,
    )

    assert result["status"] == "COMPLETE"
    assert result["windows"][str(missed.window_id)]["window_state"] == "MISSED"
    assert result["windows"][str(later.window_id)]["window_state"] == "COMPLETE"
    assert harness.window_state(missed) == "MISSED"
    assert {fault["kind"] for fault in _faults(capsys.readouterr().err)} == {WINDOW_DEADLINE_RECORD_FAILED}


# --- PostgresOrchestrationRepository.record_missed ------------------------------------------------------------


class _Cursor:
    def __init__(self, *results) -> None:
        self.statements: list[str] = []
        self._results = list(results)

    def execute(self, statement, _parameters=None) -> None:
        self.statements.append(" ".join(statement.split()))

    def fetchone(self):
        return self._results.pop(0)


class _Provider:
    def __init__(self, cursor: _Cursor) -> None:
        self.cursor = cursor

    @contextmanager
    def transaction(self, *, read_only: bool = False):
        assert not read_only
        yield SimpleNamespace(cursor=self.cursor)


def _claim() -> WindowClaim:
    window = WindowRecord(uuid4(), uuid4(), uuid4(), "MASSIVE_ADVANCED", "E:2026-11-17T19:20", RELEASE,
                          RELEASE - timedelta(seconds=127), RELEASE + timedelta(minutes=5), "RUNNING")
    return WindowClaim(window, uuid4(), 3, RELEASE + timedelta(minutes=5))


def _missed_row(claim, stage_id, version):
    return (uuid4(), claim.window.window_id, stage_id, version, 1, "MISSED", None, ["WINDOW_DEADLINE_PASSED"],
            None, None, None)


def _kinds(statements) -> list[str]:
    return [statement.split(" ", 1)[0] + (" authority" if "advanced_options_authority_receipts" in statement
                                          else " fence" if "FROM rt.window_runs" in statement
                                          else " attempt" if "max(attempt)" in statement else "")
            for statement in statements]


def test_record_missed_records_no_row_for_a_gated_v5_stage_without_a_consumed_cycle_authority():
    claim = _claim()
    for stage_id in GATED:
        cursor = _Cursor((1,), None)

        recorded = PostgresOrchestrationRepository(_Provider(cursor)).record_missed(
            claim, stage_id, STAGE_CATALOG_VERSION_V5, "WINDOW_DEADLINE_PASSED", CRASHED_AT)

        assert recorded is None
        assert _kinds(cursor.statements) == ["SELECT fence", "SELECT authority"]
        assert "receipt.authority_kind = 'WINDOW_CYCLE'" in cursor.statements[1]


def test_record_missed_records_the_row_once_the_cycle_authority_was_consumed():
    claim = _claim()
    cursor = _Cursor((1,), (1,), (2,), _missed_row(claim, "OPTIONS_CYCLE", STAGE_CATALOG_VERSION_V5))

    recorded = PostgresOrchestrationRepository(_Provider(cursor)).record_missed(
        claim, "OPTIONS_CYCLE", STAGE_CATALOG_VERSION_V5, "WINDOW_DEADLINE_PASSED", CRASHED_AT)

    assert (recorded.stage_id, recorded.state) == ("OPTIONS_CYCLE", "MISSED")
    assert _kinds(cursor.statements) == ["SELECT fence", "SELECT authority", "SELECT attempt", "INSERT"]


def test_record_missed_is_unchanged_for_every_other_stage():
    claim = _claim()
    for stage_id, version in (("OPTIONS_REFERENCE", STAGE_CATALOG_VERSION_V5), ("OPTIONS_CYCLE", "stages_v4"),
                              ("EQUITY_MATERIALIZE", "stages_v2")):
        cursor = _Cursor((1,), (1,), _missed_row(claim, stage_id, version))

        recorded = PostgresOrchestrationRepository(_Provider(cursor)).record_missed(
            claim, stage_id, version, "WINDOW_DEADLINE_PASSED", CRASHED_AT)

        assert recorded.stage_id == stage_id
        assert _kinds(cursor.statements) == ["SELECT fence", "SELECT attempt", "INSERT"]
