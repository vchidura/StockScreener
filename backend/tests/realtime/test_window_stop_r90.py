"""G102 R90 (Nov 18 night): the Options window and a requested stop.

Unconditional: a stop requested during a pass of the release loop starts no further attempt (the loop already waits on
the stop event between passes). REALTIME_V3_WINDOW_SAFE_STOP=ON: an attempt that has not claimed its window ends
at the next pre-stage checkpoint (the window stays PENDING for the next container), the role logs when it saw the stop,
and the WINDOW role gets up to 170 s (inside its 180 s compose grace) to finish a window it has claimed. OFF (default):
no checkpoint is passed, 60 s as before."""
import inspect
import json
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

import realtime.advanced_options_p8_production as production
import realtime.advanced_options_p8_roles as roles_module
import realtime.advanced_options_p8_runtime as runtime
import realtime.advanced_options_p8_window as window_module
from realtime.advanced_options_p8_window import (
    WINDOW_SAFE_STOP_SETTING,
    AdvancedOptionsWindowStopRequested,
    _stop_checkpoint,
    build_and_run_p8_serial_window,
    window_safe_stop_enabled,
)
from realtime.advanced_options_service import AdvancedOptionsProcessKind
from realtime.stage_graph import STAGE_CATALOG_VERSION_V5

FIRST = UUID("11111111-2222-3333-4444-555555555555")
SECOND = UUID("11111111-2222-3333-4444-666666666666")


def _role(monkeypatch, serial, *, environment=None, windows=(FIRST, SECOND), session_end_in=timedelta(hours=1)):
    package = {"orchestration": {"catalog_version": STAGE_CATALOG_VERSION_V5}}
    bundle = {"orchestration": dict(package["orchestration"])}
    if session_end_in is not None:
        bundle["window"] = {"stop_utc": (datetime.now(timezone.utc) + session_end_in).isoformat()}
    monkeypatch.setattr(roles_module, "validate_advanced_options_v3_runtime_identity", lambda *_args: None)
    monkeypatch.setattr(roles_module, "_v3_resident_dependencies",
                        lambda *_args, **_kwargs: {"FLOW": {"status": "COMPLETE"}})
    monkeypatch.setattr(roles_module, "_BLOCKED_WINDOW_RETRY_SECONDS", 0.01)
    monkeypatch.setattr(production, "run_v3_serial_window", serial)
    bindings = tuple(SimpleNamespace(window_id=value) for value in windows)
    monkeypatch.setattr(production, "load_v3_pending_window_bindings", lambda **_kwargs: bindings)
    return lambda stop_event: roles_module._run_window(
        package=package, bundle=bundle, binding=bindings[0], provider=object(), pool=object(),
        environment={} if environment is None else environment, stop_event=stop_event,
    )


# --- switch ----------------------------------------------------------------------------------------------------------

def test_switch_values_and_loud_failure():
    assert window_safe_stop_enabled({}) is False
    assert window_safe_stop_enabled({WINDOW_SAFE_STOP_SETTING: " off "}) is False
    assert window_safe_stop_enabled({WINDOW_SAFE_STOP_SETTING: "on"}) is True
    for bad in ("RECORD", "YES", "1"):
        with pytest.raises(ValueError, match=WINDOW_SAFE_STOP_SETTING):
            window_safe_stop_enabled({WINDOW_SAFE_STOP_SETTING: bad})


def test_window_service_start_checks_the_switch():
    source = (Path(window_module.__file__).parents[1] / "scripts"
              / "run_advanced_options_v3_production.py").read_text(encoding="utf-8")
    start = source.index("if kind is AdvancedOptionsProcessKind.WINDOW:")
    assert "window_safe_stop_enabled()" in source[start:source.index("provider = None", start)]


# --- unconditional: the release loop ---------------------------------------------------------------------------------

def test_a_stop_during_a_pass_starts_no_further_attempt(monkeypatch):
    stopped = threading.Event()
    attempts = []

    def serial(**kwargs):
        attempts.append(kwargs["binding"].window_id)
        stopped.set()  # the stop arrives while the first window runs
        return {"status": "FINISHED", "window_state": "COMPLETE"}

    result = _role(monkeypatch, serial)(stopped)
    assert attempts == [FIRST]
    assert result["status"] == "COMPLETE" and list(result["windows"]) == [str(FIRST)]


@pytest.mark.parametrize("session_end_in", [timedelta(seconds=-1), None])
def test_at_the_sessions_end_the_pass_runs_as_today(monkeypatch, session_end_in):
    # The service loop stops the role at the bundle's stop_utc; that stop finishes the pass (and, with the switch ON,
    # passes no checkpoint), as before. A bundle without stop_utc behaves as today too.
    stopped = threading.Event()
    attempts = []

    def serial(**kwargs):
        attempts.append(kwargs["binding"].window_id)
        if "stop_requested" in kwargs:
            assert kwargs["stop_requested"]() is False
        stopped.set()
        return {"status": "FINISHED", "window_state": "COMPLETE"}

    _role(monkeypatch, serial, session_end_in=session_end_in,
          environment={WINDOW_SAFE_STOP_SETTING: "ON"})(stopped)
    assert attempts == [FIRST, SECOND]


def test_without_a_stop_every_window_of_the_pass_runs(monkeypatch):
    stopped = threading.Event()
    attempts = []

    def serial(**kwargs):
        attempts.append(kwargs["binding"].window_id)
        if len(attempts) == 2:
            stopped.set()
        return {"status": "FINISHED", "window_state": "COMPLETE"}

    _role(monkeypatch, serial)(stopped)
    assert attempts == [FIRST, SECOND]


def test_off_passes_no_checkpoint_and_logs_nothing_more(monkeypatch, capsys):
    stopped = threading.Event()
    seen = []

    def serial(**kwargs):
        seen.append(kwargs)
        stopped.set()
        return {"status": "FINISHED", "window_state": "COMPLETE"}

    _role(monkeypatch, serial)(stopped)
    assert "stop_requested" not in seen[0]
    assert "advanced_options_window_stop" not in capsys.readouterr().out


# --- ON: the pre-claim checkpoints -----------------------------------------------------------------------------------

def test_on_ends_an_unclaimed_attempt_and_says_so(monkeypatch, capsys):
    stopped = threading.Event()
    seen = []

    def serial(**kwargs):
        seen.append(kwargs)
        assert kwargs["stop_requested"]() is False
        stopped.set()
        assert kwargs["stop_requested"]() is True
        raise AdvancedOptionsWindowStopRequested("ROLLING")

    result = _role(monkeypatch, serial, environment={WINDOW_SAFE_STOP_SETTING: "ON"})(stopped)
    assert len(seen) == 1
    assert result["windows"] == {str(FIRST): {"status": "STOPPED",
                                              "reason_codes": ["STOP_REQUESTED_BEFORE_CLAIM:ROLLING"]}}
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    attempt = next(line for line in lines if line.get("event") == "advanced_options_window_attempt")
    assert attempt["status"] == "STOPPED"
    stop = next(line for line in lines if line.get("event") == "advanced_options_window_stop")
    assert stop["stopped_before_claim"] == [str(FIRST)] and stop["observed_at"]


def test_checkpoint_raises_only_on_a_requested_stop():
    _stop_checkpoint(None, "START")
    _stop_checkpoint(lambda: False, "START")
    with pytest.raises(AdvancedOptionsWindowStopRequested) as raised:
        _stop_checkpoint(lambda: True, "MARKS")
    assert raised.value.step == "MARKS" and "MARKS" in str(raised.value)


def test_a_stopped_attempt_touches_nothing_at_its_first_checkpoint():
    with pytest.raises(AdvancedOptionsWindowStopRequested) as raised:
        build_and_run_p8_serial_window(
            provider=None, binding=None, package=None, flow_environment=None, contract_scope=None,
            source_flow=None, dependency_evidence=None, stop_requested=lambda: True,
        )
    assert raised.value.step == "START"


def test_every_checkpoint_is_before_the_claim():
    source = inspect.getsource(build_and_run_p8_serial_window)
    claim = source.index("report = orchestrator.run_window(binding.window_id)")
    readiness = source.index("repository.upsert_readiness(")
    steps = ("START", "REFERENCE", "ROLLING", "MARKS", "O3_INPUTS", "NATIVE_PREFLIGHT", "BEFORE_CLAIM")
    positions = [source.index(f'_stop_checkpoint(stop_requested, "{step}")') for step in steps]
    assert positions == sorted(positions) and positions[-1] < readiness < claim
    assert source.count("_stop_checkpoint(") == len(steps)


def test_the_serial_window_passes_the_checkpoint_through():
    source = inspect.getsource(production.run_v3_serial_window)
    assert "stop_requested=None," in source and "stop_requested=stop_requested," in source


# --- ON: the claimed window's stop bound -----------------------------------------------------------------------------

def test_claimed_window_stop_bound():
    on = {WINDOW_SAFE_STOP_SETTING: "ON"}
    assert runtime._role_stop_seconds(AdvancedOptionsProcessKind.WINDOW, {}) == runtime.P8_ROLE_STOP_SECONDS == 60
    assert runtime._role_stop_seconds(AdvancedOptionsProcessKind.WINDOW, on) == 170
    for kind in (AdvancedOptionsProcessKind.FLOW, AdvancedOptionsProcessKind.MARKET_STATE,
                 AdvancedOptionsProcessKind.INGRESS):
        assert runtime._role_stop_seconds(kind, on) == 60
    assert runtime.P8_WINDOW_CLAIMED_STOP_SECONDS < 180  # inside the window's compose stop grace


def test_service_loop_uses_the_bound():
    source = inspect.getsource(runtime.run_p8_docker_service)
    assert "stop_seconds = _role_stop_seconds(kind, service_environment)" in source
    assert "stop_deadline = time.monotonic() + stop_seconds" in source
    # The stop line is the WINDOW's with the switch ON only (every other role and OFF: the bound is 60 s, no line).
    assert "if stop_seconds != P8_ROLE_STOP_SECONDS:" in source


# --- ON: the stop signal handler takes no lock (the R90 root cause) --------------------------------------------------

def _relay(**kwargs):
    from scripts.run_advanced_options_v3_production import relayed_stop_handler

    stop, session, done = threading.Event(), threading.Event(), threading.Event()
    active = [session]
    handler, thread = relayed_stop_handler(stop, active, poll_seconds=0.01, done=done, **kwargs)
    return stop, session, active, handler, thread, done


def test_relayed_handler_takes_no_lock_and_the_relay_stops_the_session():
    stop, session, _active, handler, thread, done = _relay()
    try:
        # The deadlock: a second SIGTERM while the first handler holds the event's condition lock. The relayed handler
        # never touches it, so it returns at once even with the lock held in this very thread.
        with session._cond:  # noqa: SLF001 - the lock Event.set() takes
            with stop._cond:  # noqa: SLF001
                handler(15, None)
                handler(15, None)
        assert stop.wait(2.0) and session.wait(2.0)
    finally:
        done.set()
        thread.join(2.0)
    assert not thread.is_alive()


def test_relay_also_stops_a_session_started_just_after_the_signal():
    stop, _session, active, handler, thread, done = _relay()
    try:
        handler(15, None)
        assert stop.wait(2.0)
        later = threading.Event()
        active[0] = later  # the service loop checked `stop` just before the signal and starts one more session
        assert later.wait(2.0)
    finally:
        done.set()
        thread.join(2.0)


def test_no_signal_no_stop():
    stop, session, _active, _handler, thread, done = _relay()
    try:
        assert not stop.wait(0.1) and not session.is_set()
    finally:
        done.set()
        thread.join(2.0)


def test_continuous_service_uses_the_relay_only_for_the_window_with_the_switch_on():
    import scripts.run_advanced_options_v3_production as production_script

    source = inspect.getsource(production_script._docker_service_main)
    start = source.index("def request_stop(*_unused):")
    block = source[start:source.index("while not stop.is_set():", start)]
    # OFF (and every other kind): today's handler, unchanged.
    assert "signal_handler = request_stop" in block
    assert "if kind is AdvancedOptionsProcessKind.WINDOW:" in block and "if window_safe_stop_enabled():" in block
    assert "signal_handler, _relay = relayed_stop_handler(stop, active_stop)" in block
    assert "signal.signal(signal.SIGTERM, signal_handler)" in block
