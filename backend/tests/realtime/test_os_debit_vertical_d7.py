"""G106 D7 (Nov 18 night): the OS1 / OS2 debit-spread variant, RECORD only, behind REALTIME_V3_OS_DEBIT_VERTICAL.

The O1 rule (`select_debit_vertical`) gives every OS invocation the vertical it would carry; one OS_DEBIT_VERTICAL_RECORD
line per window and lane. OFF reads nothing, attaches nothing and logs nothing; RECORD changes no identity, package,
evaluation or alert."""
import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

import realtime.advanced_options_native_loader as loader
import realtime.advanced_options_o1_debit_vertical as o1dv
from realtime.advanced_options_native_adapters import AdvancedOptionsLaneInputs, build_advanced_os_executor
from realtime.advanced_options_os_debit_vertical import (
    OS_DEBIT_VERTICAL_RECORD_EVENT,
    OS_DEBIT_VERTICAL_SETTING,
    OSDebitVerticalLane,
    OSDebitVerticalMode,
    attach_os_debit_verticals,
    log_os_debit_vertical_record,
    os_debit_vertical_document,
    os_debit_vertical_mode,
    os_debit_vertical_outcomes,
)
from realtime.preflight import DetectorFamily
from test_o1_debit_vertical import (
    CONTRACTS,
    CUTOFF,
    INPUTS,
    LONG_CALL,
    LONG_PUT,
    SHORT_CALL,
    SHORT_PUT,
    _plan as _o1_plan,
)
from test_os_participation import _context, _os1_fixture

EVENT_A = UUID("00000000-0000-4000-8000-0000000000a1")
EVENT_B = UUID("00000000-0000-4000-8000-0000000000b2")


@pytest.fixture(autouse=True)
def _unset(monkeypatch):
    monkeypatch.delenv(OS_DEBIT_VERTICAL_SETTING, raising=False)
    for key in (o1dv.O1_DEBIT_VERTICAL_LEG_MIN_OPEN_INTEREST_SETTING, o1dv.O1_DEBIT_VERTICAL_LEG_MIN_DAY_VOLUME_SETTING):
        monkeypatch.delenv(key, raising=False)
    policy = o1dv.o1_debit_vertical_policy
    policy.cache_clear()
    yield
    policy.cache_clear()


def _signal(direction, underlyer="AAPL", signal_id="sig-1"):
    return SimpleNamespace(signal_id=signal_id, underlyer=underlyer, direction=direction, detector_id="OS1")


def _v1(contract, direction, event_id=EVENT_A, package=None):
    return SimpleNamespace(event_id=event_id, signal=_signal(direction),
                           source=SimpleNamespace(contract_ticker=contract.contract_ticker), package=package)


def _v2(contract, direction, confirmed=True, event_id=EVENT_B):
    outcome = SimpleNamespace(confirmed=confirmed, confirmed_contract=contract.contract_ticker if confirmed else None)
    return SimpleNamespace(event_id=event_id, signal=_signal(direction, signal_id="sig-2"), outcome=outcome,
                           package=None, policy_sha256="p" * 64)


# --- switch ----------------------------------------------------------------------------------------------------------

def test_switch_values_and_loud_failure():
    assert os_debit_vertical_mode({}) is OSDebitVerticalMode.OFF
    assert os_debit_vertical_mode({OS_DEBIT_VERTICAL_SETTING: " "}) is OSDebitVerticalMode.OFF
    assert os_debit_vertical_mode({OS_DEBIT_VERTICAL_SETTING: "record"}) is OSDebitVerticalMode.RECORD
    for bad in ("ON", "YES", "1"):
        with pytest.raises(ValueError, match=OS_DEBIT_VERTICAL_SETTING):
            os_debit_vertical_mode({OS_DEBIT_VERTICAL_SETTING: bad})


def test_window_service_start_checks_the_switch():
    source = (Path(loader.__file__).parents[1] / "scripts"
              / "run_advanced_options_v3_production.py").read_text(encoding="utf-8")
    start = source.index("if kind is AdvancedOptionsProcessKind.WINDOW:")
    assert "os_debit_vertical_mode()" in source[start:source.index("provider = None", start)]


# --- the rule on OS invocations ----------------------------------------------------------------------------------------

def test_v1_and_v2_invocations_get_the_o1_rules_vertical():
    policy = o1dv.o1_debit_vertical_policy()
    call, put, v2 = os_debit_vertical_outcomes(
        (_v1(LONG_CALL, 1), _v1(LONG_PUT, -1, event_id=EVENT_B), _v2(LONG_CALL, 1)), INPUTS, _o1_plan(),
        policy=policy)
    assert call.pairing == "V1_O1_EVENT" and call.vertical.short == SHORT_CALL and call.reason is None
    assert call.vertical.net_debit == o1dv.select_debit_vertical(
        LONG_CALL, CONTRACTS, INPUTS.marks, INPUTS.quotes, unmarked=(), spot=INPUTS.spots["AAPL"],
        session_date=_o1_plan().control_plane.session_date, cutoff=CUTOFF, policy=policy).net_debit
    assert put.vertical.short == SHORT_PUT and put.direction == -1
    assert v2.pairing == "V2_PARTICIPATION" and v2.vertical.short == SHORT_CALL and v2.signal_id == "sig-2"


def test_reasons_are_stable_and_nothing_raises():
    policy = o1dv.o1_debit_vertical_policy()
    plan = _o1_plan(unmarked=(SHORT_CALL.instrument_id,))
    unconfirmed, unmarked_short, no_inputs, unknown = os_debit_vertical_outcomes(
        (_v2(LONG_CALL, 1, confirmed=False), _v1(LONG_CALL, 1), _v1(LONG_PUT, -1),
         SimpleNamespace(event_id=EVENT_A, signal=_signal(1), source=SimpleNamespace(contract_ticker="O:ZZZ"),
                         package=None)),
        INPUTS, plan, policy=policy)
    assert unconfirmed.reason == "OSDV_NO_CONTRACT"
    assert unmarked_short.reason == "O1DV_SHORT_LEG_UNMARKED"
    assert no_inputs.vertical is not None  # the put's legs are marked
    assert unknown.reason == "O1DV_LONG_LEG_NOT_WATCHED"
    (missing,) = os_debit_vertical_outcomes((_v1(LONG_CALL, 1),), None, plan, policy=policy,
                                            inputs_reason="O1DV_INPUTS_UNAVAILABLE:OperationalError")
    assert missing.reason == "O1DV_INPUTS_UNAVAILABLE:OperationalError"
    broken = replace(INPUTS, spots=None)
    (failed,) = os_debit_vertical_outcomes((_v1(LONG_CALL, 1),), broken, _o1_plan(), policy=policy)
    assert failed.reason == "O1DV_BUILD_FAILED:AttributeError"


def test_package_today_is_named():
    package = SimpleNamespace(legs=(object(),), candidate_id=None, cohort_id=None, event_id=None)
    (outcome,) = os_debit_vertical_outcomes((_v1(LONG_CALL, 1, package=package),), INPUTS, _o1_plan(),
                                            policy=o1dv.o1_debit_vertical_policy())
    assert outcome.package_structure == "SINGLE_CONTRACT"


# --- the loader attaches, never changes the lanes --------------------------------------------------------------------

class _Cursor:
    def __init__(self):
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append(sql)


def test_attach_reuses_the_o1_inputs_and_keeps_every_lane_equal(monkeypatch):
    reads = []
    monkeypatch.setattr(o1dv, "load_o1_vertical_inputs_guarded", lambda cursor, plan: reads.append(1) or (INPUTS, None))
    os1 = AdvancedOptionsLaneInputs((_v1(LONG_CALL, 1),))
    os2 = AdvancedOptionsLaneInputs(blocked_reason_codes=("OS2_X",))
    attached = attach_os_debit_verticals(_Cursor(), _o1_plan(), {DetectorFamily.OS1: os1, DetectorFamily.OS2: os2},
                                         o1_vertical={"inputs": INPUTS})
    assert reads == []
    assert attached[DetectorFamily.OS1] == os1 and attached[DetectorFamily.OS2] == os2
    assert attached[DetectorFamily.OS1].invocations == os1.invocations
    lane = attached[DetectorFamily.OS1].os_vertical
    assert lane.status == "OK" and lane.inputs_source == "O1_LANE" and lane.outcomes[0].vertical is not None
    assert attached[DetectorFamily.OS2].os_vertical.status == "LANE_BLOCKED"
    assert os1.os_vertical is None  # the original lane object is untouched


def test_attach_reads_once_when_the_o1_lane_did_not(monkeypatch):
    reads = []
    monkeypatch.setattr(o1dv, "load_o1_vertical_inputs_guarded", lambda cursor, plan: reads.append(1) or (INPUTS, None))
    os1 = AdvancedOptionsLaneInputs((_v1(LONG_CALL, 1),))
    os2 = AdvancedOptionsLaneInputs((_v1(LONG_PUT, -1),))
    attached = attach_os_debit_verticals(_Cursor(), _o1_plan(), {DetectorFamily.OS1: os1, DetectorFamily.OS2: os2},
                                         o1_vertical={})
    assert reads == [1] and attached[DetectorFamily.OS1].os_vertical.inputs_source == "OWN_READ"
    empty = attach_os_debit_verticals(_Cursor(), _o1_plan(), {DetectorFamily.OS1: AdvancedOptionsLaneInputs()},
                                      o1_vertical={})
    assert reads == [1] and empty[DetectorFamily.OS1].os_vertical.inputs_source == "NONE"


def test_attach_failure_is_a_status_not_an_exception(monkeypatch):
    def broken():
        raise RuntimeError("policy")

    monkeypatch.setattr(o1dv, "o1_debit_vertical_policy", broken)
    os1 = AdvancedOptionsLaneInputs((_v1(LONG_CALL, 1),))
    attached = attach_os_debit_verticals(_Cursor(), _o1_plan(), {DetectorFamily.OS1: os1}, o1_vertical={})
    assert attached[DetectorFamily.OS1] == os1
    assert attached[DetectorFamily.OS1].os_vertical.status == "RECORD_FAILED:RuntimeError"


def _assemble_lanes(monkeypatch, value):
    """The assembler's D7 step with every lane loader stubbed: (os1, os2, o1 calls with vertical_out)."""
    if value is None:
        monkeypatch.delenv(OS_DEBIT_VERTICAL_SETTING, raising=False)
    else:
        monkeypatch.setenv(OS_DEBIT_VERTICAL_SETTING, value)
    o1_calls = []
    os1 = AdvancedOptionsLaneInputs((_v1(LONG_CALL, 1),))

    def load_o1(cursor, plan, evidence, vertical_out=None):
        o1_calls.append(vertical_out)
        if vertical_out is not None:
            vertical_out["inputs"] = INPUTS
        return AdvancedOptionsLaneInputs(), ("o1",)

    def load_os(cursor, plan, *, detector, **_kwargs):
        return (os1 if detector is DetectorFamily.OS1 else AdvancedOptionsLaneInputs()), (detector.value,)

    captured = {}

    def stop_after_os(cursor, plan, evidence):
        raise _Stop

    class _Stop(Exception):
        pass

    original_attach = loader.attach_os_debit_verticals

    def attach(*args, **kwargs):
        result = original_attach(*args, **kwargs)
        captured.update(result)
        return result

    monkeypatch.setattr(loader, "_validate_plan", lambda plan: None)
    monkeypatch.setattr(loader, "_verify_scope", lambda cursor, plan, evidence: None)
    monkeypatch.setattr(loader, "_load_equity_context", lambda cursor, plan, evidence: ({}, (), ()))
    monkeypatch.setattr(loader, "_load_o1", load_o1)
    monkeypatch.setattr(loader, "_load_os", load_os)
    monkeypatch.setattr(loader, "_load_o3", stop_after_os)
    monkeypatch.setattr(loader, "attach_os_debit_verticals", attach)
    monkeypatch.setattr(loader, "os_participation_mode", lambda: loader.OSParticipationMode.OFF)
    with pytest.raises(_Stop):
        loader.assemble_advanced_options_native_inputs_cursor(_Cursor(), _o1_plan())
    return o1_calls, captured


def test_assembler_off_calls_o1_as_today_and_attaches_nothing(monkeypatch):
    o1_calls, captured = _assemble_lanes(monkeypatch, None)
    assert o1_calls == [None] and captured == {}


def test_assembler_record_reuses_o1_inputs_and_attaches(monkeypatch):
    o1_calls, captured = _assemble_lanes(monkeypatch, "RECORD")
    assert o1_calls == [{"inputs": INPUTS}]
    lane = captured[DetectorFamily.OS1].os_vertical
    assert lane.inputs_source == "O1_LANE" and lane.outcomes[0].vertical.short == SHORT_CALL


# --- the record ------------------------------------------------------------------------------------------------------

def test_document_aligns_outcomes_with_evaluations():
    policy = o1dv.o1_debit_vertical_policy()
    outcomes = os_debit_vertical_outcomes(
        (_v1(LONG_CALL, 1), _v2(LONG_CALL, 1, confirmed=False), _v1(LONG_PUT, -1, event_id=EVENT_B)), INPUTS,
        _o1_plan(), policy=policy)
    lane = OSDebitVerticalLane(outcomes=outcomes, policy_version=policy.version, policy_sha256=policy.policy_sha256,
                               inputs_source="O1_LANE")
    evaluations = [SimpleNamespace(selection_status=status) for status in ("NOT_SELECTED", "OBSERVATION", "SELECTED")]
    document = os_debit_vertical_document("OS1", CUTOFF, lane, evaluations)
    assert document["mode"] == "RECORD" and document["applied"] is False and document["execution_permission"] is False
    assert (document["invocations"], document["built"], document["built_selected"]) == (3, 2, 1)
    assert document["by_status"] == {"NOT_SELECTED": 1, "OBSERVATION": 1, "SELECTED": 1}
    assert document["rejected"] == {"OSDV_NO_CONTRACT": 1}
    first = document["items"][0]
    assert first["status"] == "SELECTED" and first["long"] == LONG_PUT.contract_ticker
    assert first["vertical"]["short"] == SHORT_PUT.contract_ticker and first["vertical"]["structure"] == "PUT_DEBIT_VERTICAL"
    assert document["policy_sha256"] == policy.policy_sha256 and document["truncated"] is False


def test_off_logs_nothing_and_record_logs_one_line(capsys):
    log_os_debit_vertical_record("OS1", CUTOFF, None, ())
    assert capsys.readouterr().err == ""
    log_os_debit_vertical_record("OS2", CUTOFF, OSDebitVerticalLane(status="LANE_BLOCKED"), ())
    (line,) = capsys.readouterr().err.strip().splitlines()
    event, payload = line.split(" ", 1)
    assert event == OS_DEBIT_VERTICAL_RECORD_EVENT
    assert json.loads(payload)["status"] == "LANE_BLOCKED" and json.loads(payload)["detector"] == "OS2"


def test_a_broken_record_is_a_failed_line(capsys):
    broken = OSDebitVerticalLane(outcomes=(object(),))
    log_os_debit_vertical_record("OS1", CUTOFF, broken, ())
    (line,) = capsys.readouterr().err.strip().splitlines()
    assert line.startswith(OS_DEBIT_VERTICAL_RECORD_EVENT) and '"status": "RECORD_FAILED"' in line


def test_executor_results_are_identical_and_record_adds_one_line(capsys):
    invocation = _os1_fixture()
    capsys.readouterr()
    cutoff = invocation.signal.received_at + timedelta(minutes=1)
    plain = AdvancedOptionsLaneInputs((invocation,))
    outcomes = os_debit_vertical_outcomes((invocation,), None, _o1_plan(), policy=o1dv.o1_debit_vertical_policy(),
                                          inputs_reason="O1DV_INPUTS_UNAVAILABLE")
    recorded = replace(plain, os_vertical=OSDebitVerticalLane(outcomes=outcomes))
    assert plain == recorded
    today = build_advanced_os_executor(DetectorFamily.OS1, plain)(_context(cutoff))
    assert capsys.readouterr().err == ""
    with_record = build_advanced_os_executor(DetectorFamily.OS1, recorded)(_context(cutoff))
    assert with_record == today
    (line,) = capsys.readouterr().err.strip().splitlines()
    event, payload = line.split(" ", 1)
    document = json.loads(payload)
    assert event == OS_DEBIT_VERTICAL_RECORD_EVENT and document["detector"] == "OS1"
    assert document["invocations"] == 1 and document["items"][0]["status"] == today.evaluations[0].selection_status
    assert document["items"][0]["reason"] == "O1DV_INPUTS_UNAVAILABLE"


def test_blocked_executor_logs_the_lane_status(capsys):
    blocked = AdvancedOptionsLaneInputs(blocked_reason_codes=("OS1_X",),
                                        os_vertical=OSDebitVerticalLane(status="LANE_BLOCKED"))
    invocation = _os1_fixture()
    capsys.readouterr()
    result = build_advanced_os_executor(DetectorFamily.OS1, blocked)(_context(invocation.signal.received_at))
    assert result.reason_codes == ("OS1_X",)
    (line,) = capsys.readouterr().err.strip().splitlines()
    assert line.startswith(OS_DEBIT_VERTICAL_RECORD_EVENT) and '"status": "LANE_BLOCKED"' in line
