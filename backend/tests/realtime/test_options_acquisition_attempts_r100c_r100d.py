"""G102 R100c (operator attempt extension without id reuse) and R100d (restart backoff after a local failure).

Nov 18 2026: a local crash loop (the decoder refused mixed status frames, the dead pump filled the handoff) restarted
the Options acquisition every ~30 s and spent all 12 connection attempts in 5.3 min (14:37:50-14:43:08); the recovery
lowered `connection_attempts` by hand, re-used attempt numbers and collided in the journal.
"""
from __future__ import annotations

import json
import os
import threading
from datetime import timedelta
from uuid import uuid5

import pytest

from realtime import advanced_options_acquisition_runtime as runtime
from realtime.advanced_equity_ingress import (
    AdvancedJournalDecodeError,
    AdvancedJournalProgress,
)
from realtime.advanced_gateway import GatewayControlError, GatewayOverload
from realtime.advanced_options_acquisition_runtime import (
    LOCAL_BACKOFF_SETTING,
    LOCAL_FAILURES,
    MAXIMUM_EFFECTIVE_CONNECTION_ATTEMPTS,
    OPERATOR_EXTRA_CONNECTION_ATTEMPTS,
    build_acquisition_binding,
    effective_connection_attempt_ceiling,
    local_backoff_enabled,
    local_failure_backoff_seconds,
    local_failure_class,
    run_acquisition,
)
from realtime.advanced_options_ingress import (
    _MAXIMUM_DERIVED_CONNECTION_ATTEMPTS,
    _journal_session_ids,
)
from scripts import extend_options_acquisition_attempts as extend
from tests.realtime.test_advanced_options_acquisition_runtime import (
    CONTRACTS,
    NOW,
    _authority,
    _install_runtime_fakes,
    _Provider,
    _RejectedConnection,
    _run_until_stopped,
)


_PUBLISHERS = []


class _TrackedPublisher(runtime._AcquisitionStatusPublisher):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        _PUBLISHERS.append(self)


def _stop_leftover_publishers():
    # A run that ends on a pump error leaves its 1 s status heartbeat thread running until the process exits (as in
    # production, where the process exits at once). Between two in-process "restarts" it would overwrite the file.
    while _PUBLISHERS:
        publisher = _PUBLISHERS.pop()
        if publisher._thread.is_alive():
            publisher._stop.set()
            publisher._thread.join(5)


@pytest.fixture(autouse=True)
def _environment(monkeypatch):
    monkeypatch.setenv("REALTIME_V3_OPTIONS_JOURNAL_GUARD", "OFF")
    monkeypatch.delenv(LOCAL_BACKOFF_SETTING, raising=False)
    monkeypatch.setattr(runtime, "_AcquisitionStatusPublisher", _TrackedPublisher)
    yield
    _stop_leftover_publishers()


def _binding(ceiling=12):
    return build_acquisition_binding(
        source_sha256="1" * 64,
        backend_image_id=f"sha256:{'2' * 64}",
        authority=_authority(),
        contract_tickers=CONTRACTS,
        connection_attempt_ceiling=ceiling,
        # The live preparation's budgets (each socket that ends on a crash burns its 4 MiB byte reservation).
        decoded_message_ceiling=1_000_000,
        decoded_byte_ceiling=1_073_741_824,
    )


def _attempt_id(binding, number):
    return uuid5(binding.stream_session_id, f"connection-attempt:{number}")


class _NoWaitEvent(threading.Event):
    """A stop event whose waits return at once and are logged (with the sockets) in one shared order."""

    def __init__(self, log, *, stop_on_wait=False):
        super().__init__()
        self.log = log
        self.stop_on_wait = stop_on_wait

    def wait(self, timeout=None):
        self.log.append(("wait", timeout))
        if self.stop_on_wait:
            self.set()
        return self.is_set()


def _gateway_class(log, error_factory):
    """A provider-free gateway: logs each socket's stream session id, then fails the session with error_factory."""

    class _Gateway:
        def __init__(self, _configuration, _manifest, _buffer, *, session_id_factory, **_kwargs):
            self._session_id_factory = session_id_factory
            self.received_messages = 0
            self.received_bytes = 0

        async def run_session(self, **_kwargs):
            session_id = self._session_id_factory()
            log.append(("connect", session_id))
            raise error_factory(session_id)

    return _Gateway


def _transient(_session_id):
    return ConnectionError("provider-free disconnect")


def _handoff_overload(session_id):
    # Nov 18: the dead pump stopped draining, the 20 s handoff wait ran out.
    return GatewayOverload("ADVANCED_HANDOFF_MAXIMUM_EVENTS_EXCEEDED", session_id, 0, 0)


def _dead_pump_class(error):
    class _DeadPump:
        def __init__(self, *_args, on_progress=None, **_kwargs):
            self.on_progress = on_progress
            self._error = None

        def start(self):
            if self.on_progress is not None:
                self.on_progress(AdvancedJournalProgress())

        def request_stop(self):
            pass

        def join(self, _timeout=None):
            raise error

    return _DeadPump


def _run(binding, tmp_path, stop_event):
    try:
        return run_acquisition(
            binding,
            provider=_Provider(),
            api_key="provider-free",
            journal_root=tmp_path / "journal",
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "socket.lock",
            stop_event=stop_event,
            clock=lambda: NOW,
            monotonic_ns=lambda: 100,
        )
    finally:
        _stop_leftover_publishers()


def _status(tmp_path):
    return json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))


def _write_status(tmp_path, value):
    (tmp_path / "status.json").write_text(json.dumps(value), encoding="utf-8")


def _seed_status(tmp_path, binding, **fields):
    _write_status(tmp_path, {
        "schema": runtime.ACQUISITION_STATUS_SCHEMA,
        "binding_sha256": binding.binding_sha256,
        "owner_id": str(binding.owner_id),
        "journal_generation": str(binding.authority.journal_generation),
        "state": "FAILED_NO_EVIDENCE",
        "connection_attempts": 0,
        "decoded_messages": 0,
        "decoded_bytes": 0,
        "transport_gaps": [],
        **fields,
    })


def _connects(log):
    return [item[1] for item in log if item[0] == "connect"]


def _spend_transient(binding, tmp_path, monkeypatch, log):
    """One process run whose every socket drops (transport): spends attempts up to the ceiling, captures nothing."""
    monkeypatch.setattr(runtime, "AdvancedGateway", _gateway_class(log, _transient))
    with pytest.raises(RuntimeError, match="no T/Q evidence"):
        _run(binding, tmp_path, _NoWaitEvent(log))


# --------------------------------------------------------------------------------------------------- R100c runtime


def test_absent_extension_keeps_todays_bounds_ids_and_status(tmp_path, monkeypatch, capsys):
    binding = _binding(12)
    _install_runtime_fakes(monkeypatch, binding)
    log = []

    _spend_transient(binding, tmp_path, monkeypatch, log)
    stored = _status(tmp_path)

    assert _connects(log) == [_attempt_id(binding, number) for number in range(1, 13)]
    # Today's in-process reconnect waits, unchanged.
    assert [item[1] for item in log if item[0] == "wait"] == [
        min(30.0, 0.5 * (2 ** (number - 1))) for number in range(1, 13)
    ]
    assert stored["connection_attempts"] == 12
    assert stored["state"] == "FAILED_NO_EVIDENCE"
    assert OPERATOR_EXTRA_CONNECTION_ATTEMPTS not in stored
    assert LOCAL_FAILURES not in stored
    assert "ACQUISITION_ATTEMPT_EXTENSION" not in capsys.readouterr().err

    # A restart with the ceiling spent opens no socket (as today).
    log.clear()
    _spend_transient(binding, tmp_path, monkeypatch, log)
    assert _connects(log) == []
    assert _status(tmp_path)["connection_attempts"] == 12
    assert effective_connection_attempt_ceiling(binding, {}) == 12
    assert effective_connection_attempt_ceiling(binding, {OPERATOR_EXTRA_CONNECTION_ATTEMPTS: 0}) == 12


def test_extension_continues_numbering_13_on_with_fresh_ingress_accepted_ids(tmp_path, monkeypatch, capsys):
    binding = _binding(12)
    _install_runtime_fakes(monkeypatch, binding)
    log = []
    _spend_transient(binding, tmp_path, monkeypatch, log)
    spent = set(_connects(log))
    capsys.readouterr()

    stored = _status(tmp_path)
    stored[OPERATOR_EXTRA_CONNECTION_ATTEMPTS] = 3
    _write_status(tmp_path, stored)
    log.clear()
    _spend_transient(binding, tmp_path, monkeypatch, log)
    extended = _connects(log)
    after = _status(tmp_path)
    err = capsys.readouterr().err

    assert extended == [_attempt_id(binding, number) for number in (13, 14, 15)]
    assert not spent & set(extended)
    assert set(extended) <= _journal_session_ids((binding.stream_session_id,))
    assert after["connection_attempts"] == 15
    assert after[OPERATOR_EXTRA_CONNECTION_ATTEMPTS] == 3  # the publisher kept it on every write
    line = next(row for row in err.splitlines() if row.startswith("ACQUISITION_ATTEMPT_EXTENSION "))
    document = json.loads(line.split(" ", 1)[1])
    assert document["effective_connection_attempt_ceiling"] == 15
    assert document["binding_connection_attempt_ceiling"] == 12
    assert document[OPERATOR_EXTRA_CONNECTION_ATTEMPTS] == 3
    assert document["next_attempt"] == 13 and document["remaining_attempts"] == 3
    assert document["capped"] is False

    # The extension is spent: the next start opens no socket.
    log.clear()
    _spend_transient(binding, tmp_path, monkeypatch, log)
    assert _connects(log) == []
    assert _status(tmp_path)["connection_attempts"] == 15


def test_extension_is_capped_at_64_far_below_the_ingress_256(tmp_path, monkeypatch, capsys):
    binding = _binding(12)
    _install_runtime_fakes(monkeypatch, binding)
    assert MAXIMUM_EFFECTIVE_CONNECTION_ATTEMPTS == 64 < _MAXIMUM_DERIVED_CONNECTION_ATTEMPTS == 256
    assert effective_connection_attempt_ceiling(binding, {OPERATOR_EXTRA_CONNECTION_ATTEMPTS: 1000}) == 64
    assert effective_connection_attempt_ceiling(binding, {OPERATOR_EXTRA_CONNECTION_ATTEMPTS: 52}) == 64
    assert effective_connection_attempt_ceiling(binding, {OPERATOR_EXTRA_CONNECTION_ATTEMPTS: 51}) == 63
    # Never below the binding's own ceiling.
    assert effective_connection_attempt_ceiling(_binding(70), {OPERATOR_EXTRA_CONNECTION_ATTEMPTS: 5}) == 70

    _seed_status(tmp_path, binding, connection_attempts=12, **{OPERATOR_EXTRA_CONNECTION_ATTEMPTS: 1000})
    log = []
    _spend_transient(binding, tmp_path, monkeypatch, log)

    assert _connects(log) == [_attempt_id(binding, number) for number in range(13, 65)]
    assert set(_connects(log)) <= _journal_session_ids((binding.stream_session_id,))
    assert _status(tmp_path)["connection_attempts"] == 64
    line = next(
        row for row in capsys.readouterr().err.splitlines() if row.startswith("ACQUISITION_ATTEMPT_EXTENSION ")
    )
    assert json.loads(line.split(" ", 1)[1])["capped"] is True


@pytest.mark.parametrize("value", [-1, True, "3", None, 2.5])
def test_malformed_extension_fails_closed_like_any_changed_status(tmp_path, value):
    binding = _binding(12)
    _seed_status(tmp_path, binding, connection_attempts=12, **{OPERATOR_EXTRA_CONNECTION_ATTEMPTS: value})

    with pytest.raises(ValueError, match="durable status changed"):
        runtime._load_status(tmp_path / "status.json", binding)


def test_extension_of_another_session_never_carries_over(tmp_path):
    binding = _binding(12)
    _seed_status(
        tmp_path,
        binding,
        journal_generation=str(uuid5(binding.stream_session_id, "last-session")),
        connection_attempts=15,
        **{OPERATOR_EXTRA_CONNECTION_ATTEMPTS: 3},
    )

    status = runtime._load_status(tmp_path / "status.json", binding)
    assert status == {"connection_attempts": 0, "decoded_messages": 0, "decoded_bytes": 0}
    assert effective_connection_attempt_ceiling(binding, status) == 12


def test_publisher_keeps_the_extension_on_pause_and_on_complete(tmp_path, monkeypatch, capsys):
    binding = _binding(12)
    _install_runtime_fakes(monkeypatch, binding)
    _seed_status(tmp_path, binding, connection_attempts=12, **{OPERATOR_EXTRA_CONNECTION_ATTEMPTS: 2})

    paused = _run_until_stopped(binding, tmp_path)
    stored = _status(tmp_path)

    assert paused["state"] == stored["state"] == "PAUSED"
    assert stored["connection_attempts"] == 13
    assert stored["stream_session_id"] == str(_attempt_id(binding, 13))
    assert stored[OPERATOR_EXTRA_CONNECTION_ATTEMPTS] == 2
    assert stored["last_event_offset"] > 0

    current = [NOW]

    def reach_stop():
        current[0] = binding.authority.acquisition_stop

    complete = _run_until_stopped(binding, tmp_path, clock=lambda: current[0], on_timer=reach_stop)
    stored = _status(tmp_path)

    assert complete["state"] == stored["state"] == "COMPLETE"
    assert stored["connection_attempts"] == 14
    assert stored[OPERATOR_EXTRA_CONNECTION_ATTEMPTS] == 2
    assert "semantic_sha256" in stored
    assert capsys.readouterr().err.count("ACQUISITION_ATTEMPT_EXTENSION ") == 2


# --------------------------------------------------------------------------------------------------- R100d runtime


def test_switch_is_off_by_default_and_refuses_unknown_values():
    assert local_backoff_enabled({}) is False
    assert local_backoff_enabled({LOCAL_BACKOFF_SETTING: ""}) is False
    assert local_backoff_enabled({LOCAL_BACKOFF_SETTING: " off "}) is False
    assert local_backoff_enabled({LOCAL_BACKOFF_SETTING: "ON"}) is True
    with pytest.raises(ValueError, match="OFF or ON"):
        local_backoff_enabled({LOCAL_BACKOFF_SETTING: "RECORD"})


def test_process_start_checks_and_announces_the_switch(capsys):
    from scripts.run_advanced_options_v3_acquisition import announce_local_backoff

    assert announce_local_backoff({}) is False
    assert announce_local_backoff({LOCAL_BACKOFF_SETTING: " "}) is False
    assert capsys.readouterr().err == ""  # unset stays silent, as before
    assert announce_local_backoff({LOCAL_BACKOFF_SETTING: "OFF"}) is False
    assert announce_local_backoff({LOCAL_BACKOFF_SETTING: "ON"}) is True
    lines = capsys.readouterr().err.splitlines()
    assert [json.loads(line.split(" ", 1)[1])["mode"] for line in lines] == ["OFF", "ON"]
    assert all(line.startswith("ACQUISITION_LOCAL_BACKOFF_SETTING ") for line in lines)
    assert json.loads(lines[1].split(" ", 1)[1])["backoff_seconds"] == [15.0, 30.0, 60.0, 120.0, 240.0, 300.0]
    with pytest.raises(ValueError, match="OFF or ON"):
        announce_local_backoff({LOCAL_BACKOFF_SETTING: "on-ish"})


def test_invalid_switch_refuses_before_any_socket(tmp_path, monkeypatch):
    binding = _binding(12)
    _install_runtime_fakes(monkeypatch, binding)
    log = []
    monkeypatch.setattr(runtime, "AdvancedGateway", _gateway_class(log, _transient))
    monkeypatch.setenv(LOCAL_BACKOFF_SETTING, "YES")

    with pytest.raises(ValueError, match=LOCAL_BACKOFF_SETTING):
        _run(binding, tmp_path, _NoWaitEvent(log))
    assert log == []
    assert not (tmp_path / "status.json").exists()


def test_backoff_doubles_from_15_s_and_stops_at_300_s():
    assert [local_failure_backoff_seconds(k) for k in range(0, 9)] == [
        0.0, 15.0, 30.0, 60.0, 120.0, 240.0, 300.0, 300.0, 300.0,
    ]
    assert local_failure_backoff_seconds(10_000) == 300.0


def test_local_failure_classes():
    session = _attempt_id(_binding(12), 1)
    assert local_failure_class(AdvancedJournalDecodeError("ADVANCED_OPTIONS_QUARANTINED")) == (
        "PUMP:ADVANCED_OPTIONS_QUARANTINED"
    )
    assert local_failure_class(AdvancedJournalDecodeError("ADVANCED_JOURNAL_PUMP_FAILED")) == (
        "PUMP:ADVANCED_JOURNAL_PUMP_FAILED"
    )
    assert local_failure_class(_handoff_overload(session)) == "HANDOFF:ADVANCED_HANDOFF_MAXIMUM_EVENTS_EXCEEDED"
    assert local_failure_class(ValueError("x"), "FAILED_INTERNAL") == "INTERNAL:ValueError"
    # Provider, safety ceilings and transport are not local.
    assert local_failure_class(GatewayOverload("ADVANCED_MESSAGE_SIZE_EXCEEDED", session, 0, 0)) is None
    assert local_failure_class(GatewayOverload("ADVANCED_SESSION_BYTE_CEILING_EXCEEDED", session, 0, 0)) is None
    assert local_failure_class(GatewayControlError("ADVANCED_AUTH_REJECTED", ("max_connections",)), "FAILED_PERMANENT") is None
    assert local_failure_class(ConnectionError("drop")) is None
    assert local_failure_class(RuntimeError("Options acquisition captured no T/Q evidence")) is None


def _nov18_restart(binding, tmp_path, monkeypatch, log):
    """One container start of Nov 18's loop: the socket opens, the dead pump's handoff overflows, the pump error ends it."""
    monkeypatch.setattr(runtime, "AdvancedGateway", _gateway_class(log, _handoff_overload))
    monkeypatch.setattr(
        runtime,
        "AdvancedOptionsJournalPump",
        _dead_pump_class(AdvancedJournalDecodeError("ADVANCED_OPTIONS_QUARANTINED")),
    )
    with pytest.raises(AdvancedJournalDecodeError, match="ADVANCED_OPTIONS_QUARANTINED"):
        _run(binding, tmp_path, _NoWaitEvent(log))


def test_off_records_nothing_and_restarts_reconnect_at_once(tmp_path, monkeypatch, capsys):
    binding = _binding(12)
    _install_runtime_fakes(monkeypatch, binding)
    log = []

    for _ in range(3):
        _nov18_restart(binding, tmp_path, monkeypatch, log)
    stored = _status(tmp_path)

    assert log == [("connect", _attempt_id(binding, number)) for number in (1, 2, 3)]
    assert stored["connection_attempts"] == 3
    assert stored["state"] == "FAILED_PERMANENT"
    assert stored["failure_code"] == "ADVANCED_HANDOFF_MAXIMUM_EVENTS_EXCEEDED"
    assert LOCAL_FAILURES not in stored
    assert "ACQUISITION_LOCAL_FAILURE" not in capsys.readouterr().err


def test_on_records_local_failures_and_waits_before_each_restart_socket(tmp_path, monkeypatch, capsys):
    binding = _binding(12)
    _install_runtime_fakes(monkeypatch, binding)
    monkeypatch.setenv(LOCAL_BACKOFF_SETTING, "ON")
    log = []

    _nov18_restart(binding, tmp_path, monkeypatch, log)
    first = _status(tmp_path)[LOCAL_FAILURES]
    assert first["count"] == first["total"] == 1
    assert first["pending"] is True
    assert first["last_class"] == "PUMP:ADVANCED_OPTIONS_QUARANTINED"
    assert first["last_attempt"] == 1
    assert first["last_at_utc"] == NOW.isoformat()
    assert log == [("connect", _attempt_id(binding, 1))]

    for _ in range(6):
        _nov18_restart(binding, tmp_path, monkeypatch, log)
    stored = _status(tmp_path)
    err = capsys.readouterr().err

    # Each restart waits BEFORE its socket: 15, 30, 60, 120, 240, 300 s.
    assert log[1:] == [
        item
        for number, seconds in zip(range(2, 8), (15.0, 30.0, 60.0, 120.0, 240.0, 300.0))
        for item in (("wait", seconds), ("connect", _attempt_id(binding, number)))
    ]
    assert stored["connection_attempts"] == 7
    assert stored[LOCAL_FAILURES]["count"] == stored[LOCAL_FAILURES]["total"] == 7
    assert stored[LOCAL_FAILURES]["pending"] is True
    backoffs = [
        json.loads(row.split(" ", 1)[1])
        for row in err.splitlines()
        if row.startswith("ACQUISITION_LOCAL_FAILURE_BACKOFF ")
    ]
    assert [row["wait_seconds"] for row in backoffs] == [15.0, 30.0, 60.0, 120.0, 240.0, 300.0]
    assert [row["next_attempt"] for row in backoffs] == [2, 3, 4, 5, 6, 7]
    assert backoffs[0]["consecutive_local_failures"] == 1
    assert backoffs[0]["last_class"] == "PUMP:ADVANCED_OPTIONS_QUARANTINED"
    failures = [
        json.loads(row.split(" ", 1)[1])
        for row in err.splitlines()
        if row.startswith("ACQUISITION_LOCAL_FAILURE ")
    ]
    assert [row["next_backoff_seconds"] for row in failures] == [15.0, 30.0, 60.0, 120.0, 240.0, 300.0, 300.0]


def test_on_with_the_ceiling_spent_does_not_wait(tmp_path, monkeypatch):
    binding = _binding(12)
    _install_runtime_fakes(monkeypatch, binding)
    monkeypatch.setenv(LOCAL_BACKOFF_SETTING, "ON")
    _seed_status(tmp_path, binding, connection_attempts=12, **{LOCAL_FAILURES: {"count": 4, "total": 4, "pending": True}})
    log = []

    _spend_transient(binding, tmp_path, monkeypatch, log)

    assert log == []


def test_a_stop_during_the_backoff_pauses_and_the_next_start_connects_at_once(tmp_path, monkeypatch):
    binding = _binding(12)
    _install_runtime_fakes(monkeypatch, binding)
    monkeypatch.setenv(LOCAL_BACKOFF_SETTING, "ON")
    _seed_status(
        tmp_path,
        binding,
        connection_attempts=3,
        **{LOCAL_FAILURES: {"count": 3, "total": 3, "pending": True, "last_class": "PUMP:X"}},
    )
    log = []
    monkeypatch.setattr(runtime, "AdvancedGateway", _gateway_class(log, _transient))

    report = _run(binding, tmp_path, _NoWaitEvent(log, stop_on_wait=True))
    stored = _status(tmp_path)

    assert log == [("wait", 60.0)]
    assert report["state"] == stored["state"] == "PAUSED"
    assert stored["connection_attempts"] == 3
    assert stored[LOCAL_FAILURES]["count"] == 3
    assert stored[LOCAL_FAILURES]["pending"] is False

    log.clear()
    with pytest.raises(RuntimeError, match="no T/Q evidence"):
        _run(binding, tmp_path, _NoWaitEvent(log))
    assert log[0] == ("connect", _attempt_id(binding, 4))


def test_a_successful_capture_resets_the_count(tmp_path, monkeypatch):
    binding = _binding(12)
    _install_runtime_fakes(monkeypatch, binding)
    monkeypatch.setenv(LOCAL_BACKOFF_SETTING, "ON")
    _seed_status(
        tmp_path,
        binding,
        connection_attempts=5,
        **{LOCAL_FAILURES: {"count": 5, "total": 5, "pending": False, "last_class": "PUMP:X"}},
    )

    report = _run_until_stopped(binding, tmp_path)
    stored = _status(tmp_path)

    assert report["state"] == stored["state"] == "PAUSED"
    assert stored["last_event_offset"] > 0
    assert stored[LOCAL_FAILURES]["count"] == 0
    assert stored[LOCAL_FAILURES]["total"] == 5
    assert stored[LOCAL_FAILURES]["reset_at_utc"] == NOW.isoformat()


def test_off_carries_an_existing_record_unchanged(tmp_path, monkeypatch):
    binding = _binding(12)
    _install_runtime_fakes(monkeypatch, binding)
    record = {"count": 5, "total": 5, "pending": True, "last_class": "PUMP:X"}
    _seed_status(tmp_path, binding, connection_attempts=5, **{LOCAL_FAILURES: record})

    _run_until_stopped(binding, tmp_path)

    assert _status(tmp_path)[LOCAL_FAILURES] == record


def test_on_keeps_todays_transport_reconnects(tmp_path, monkeypatch):
    binding = _binding(3)
    _install_runtime_fakes(monkeypatch, binding)
    monkeypatch.setenv(LOCAL_BACKOFF_SETTING, "ON")
    log = []

    _spend_transient(binding, tmp_path, monkeypatch, log)

    assert log == [
        ("connect", _attempt_id(binding, 1)), ("wait", 0.5),
        ("connect", _attempt_id(binding, 2)), ("wait", 1.0),
        ("connect", _attempt_id(binding, 3)), ("wait", 2.0),
    ]
    assert LOCAL_FAILURES not in _status(tmp_path)


def test_on_never_reconnects_into_a_dead_pump(tmp_path, monkeypatch):
    def dead_after_drop(enabled):
        log = []
        root = tmp_path / ("on" if enabled else "off")
        root.mkdir()
        pump_holder = []
        base = _dead_pump_class(AdvancedJournalDecodeError("ADVANCED_JOURNAL_PUMP_FAILED"))

        class _Pump(base):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                pump_holder.append(self)

        def drop_with_dead_pump(_session_id):
            pump_holder[0]._error = RuntimeError("Advanced journal commit is not readable")
            return ConnectionError("1006")

        monkeypatch.setattr(runtime, "AdvancedOptionsJournalPump", _Pump)
        monkeypatch.setattr(runtime, "AdvancedGateway", _gateway_class(log, drop_with_dead_pump))
        if enabled:
            monkeypatch.setenv(LOCAL_BACKOFF_SETTING, "ON")
        else:
            monkeypatch.delenv(LOCAL_BACKOFF_SETTING, raising=False)
        with pytest.raises(AdvancedJournalDecodeError, match="ADVANCED_JOURNAL_PUMP_FAILED"):
            _run(binding, root, _NoWaitEvent(log))
        return _connects(log), _status(root)

    binding = _binding(4)
    _install_runtime_fakes(monkeypatch, binding)

    off_connects, off_status = dead_after_drop(False)
    on_connects, on_status = dead_after_drop(True)

    assert len(off_connects) == 4 and off_status["connection_attempts"] == 4  # today: every attempt burns
    assert on_connects == [_attempt_id(binding, 1)]
    assert on_status["connection_attempts"] == 1
    assert on_status[LOCAL_FAILURES]["last_class"] == "PUMP:ADVANCED_JOURNAL_PUMP_FAILED"
    assert on_status[LOCAL_FAILURES]["pending"] is True


def test_on_provider_rejection_is_not_a_local_failure(tmp_path, monkeypatch):
    binding = _binding(12)
    _install_runtime_fakes(monkeypatch, binding)
    monkeypatch.setenv(LOCAL_BACKOFF_SETTING, "ON")

    with pytest.raises(GatewayControlError, match="ADVANCED_AUTH_REJECTED"):
        run_acquisition(
            binding,
            provider=_Provider(),
            api_key="provider-free",
            journal_root=tmp_path / "journal",
            status_path=tmp_path / "status.json",
            lock_path=tmp_path / "socket.lock",
            stop_event=threading.Event(),
            connection_factory=lambda _: _RejectedConnection(),
            clock=lambda: NOW,
            monotonic_ns=lambda: 100,
            diagnostic=True,
        )

    stored = _status(tmp_path)
    assert stored["state"] == "FAILED_PERMANENT"
    assert LOCAL_FAILURES not in stored


def _restart_schedule(backoff_on, *, attempts=12, socket_seconds=30.0):
    """Seconds after the first start at which each attempt opens its socket, for a loop where every socket ends ~30 s
    later on a local failure (Nov 18: connect, 25 s without a drained frame, handoff overload, pump error, restart)."""
    starts, clock, consecutive = [], 0.0, 0
    while len(starts) < attempts:
        if backoff_on:
            clock += local_failure_backoff_seconds(consecutive)
        starts.append(clock)
        clock += socket_seconds
        consecutive += 1
    return starts


def test_nov18_arithmetic_attempts_spent_in_the_loop():
    off = _restart_schedule(False)
    on = _restart_schedule(True)

    # OFF (today): all 12 within 5.5 min (Nov 18 log: 14:37:50 -> 14:43:08 = 5.3 min).
    assert off[-1] + 30 == 360.0
    # ON: 0, 45, 105, 195, 345, 615, 945, 1275, 1605, 1935, 2265, 2595 s.
    assert on == [0.0, 45.0, 105.0, 195.0, 345.0, 615.0, 945.0, 1275.0, 1605.0, 1935.0, 2265.0, 2595.0]
    assert sum(start < 8 * 60 for start in on) == 5  # in the 8 min that spent 12 on Nov 18
    assert sum(start < 10 * 60 for start in on) == 5
    assert sum(start < 30 * 60 for start in on) == 9
    assert on[5] / 60 == pytest.approx(10.25)  # the 6th attempt opens after 10 min
    assert on[-1] / 60 == pytest.approx(43.25)  # the 12th after 43 min instead of 5.3 min


# --------------------------------------------------------------------------------------------------- R100c CLI


GENERATION = "33333333-4444-5555-6666-777777777777"  # _authority()'s journal generation


def _cli_setup(tmp_path, **fields):
    binding = _binding(12)
    status = {
        "schema": runtime.ACQUISITION_STATUS_SCHEMA,
        "binding_sha256": binding.binding_sha256,
        "owner_id": str(binding.owner_id),
        "journal_generation": GENERATION,
        "state": "FAILED_NO_EVIDENCE",
        "connection_attempts": 12,
        "decoded_messages": 12384,
        "decoded_bytes": 50331648,
        "stream_session_id": str(_attempt_id(binding, 12)),
        "runtime_heartbeat_at_utc": (NOW - timedelta(seconds=60)).isoformat(),
        "observed_at_utc": (NOW - timedelta(seconds=60)).isoformat(),
        "transport_gaps": [],
        "forbidden_effects": {"broker": 0},
        **fields,
    }
    path = tmp_path / "reports" / "options-acquisition-status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(runtime_json(status))
    journal_root = tmp_path / "journals"
    current = journal_root / "options-acquisition" / GENERATION
    current.mkdir(parents=True)
    (current / "options.sqlite").write_bytes(b"journal")
    older = journal_root / "options-acquisition" / "0a2a1d55-0000-5000-8000-000000000001"
    older.mkdir()
    (older / "options.sqlite").write_bytes(b"old journal")
    stamp = (NOW - timedelta(days=1)).timestamp()
    os.utime(older / "options.sqlite", (stamp, stamp))
    os.utime(older, (stamp, stamp))
    return binding, path, journal_root


def runtime_json(value):
    return (json.dumps(value, indent=2, sort_keys=True, default=str) + "\n").encode("utf-8")


def _cli(path, journal_root, *args, now=NOW):
    return extend.main(
        ["--status-file", str(path), "--journal-root", str(journal_root), *args],
        now=now,
    )


def _printed(capsys):
    return json.loads(capsys.readouterr().out)


def test_cli_dry_run_by_default_writes_nothing(tmp_path, capsys):
    _binding_, path, journal_root = _cli_setup(tmp_path)
    before = path.read_bytes()

    assert _cli(path, journal_root, "--add", "4") == 0
    document = _printed(capsys)

    assert path.read_bytes() == before
    assert document["result"] == "DRY_RUN"
    assert document["before"] == {
        "connection_attempts": 12,
        OPERATOR_EXTRA_CONNECTION_ATTEMPTS: 0,
        "effective_connection_attempt_ceiling": 12,
        "remaining_attempts": 0,
    }
    assert document["after"] == {
        "connection_attempts": 12,
        OPERATOR_EXTRA_CONNECTION_ATTEMPTS: 4,
        "effective_connection_attempt_ceiling": 16,
        "remaining_attempts": 4,
    }
    assert document["new_attempt_numbers"] == [13, 14, 15, 16]


def test_cli_apply_writes_only_the_field_atomically_and_the_runtime_honours_it(tmp_path, capsys):
    binding, path, journal_root = _cli_setup(tmp_path)
    original = json.loads(path.read_text(encoding="utf-8"))

    assert _cli(path, journal_root, "--add", "4", "--apply") == 0
    assert _printed(capsys)["result"] == "APPLIED"
    written = json.loads(path.read_text(encoding="utf-8"))
    assert written == {**original, OPERATOR_EXTRA_CONNECTION_ATTEMPTS: 4}
    assert path.read_bytes() == runtime_json(written)  # the runtime's own format
    assert list(path.parent.iterdir()) == [path]  # no temp file left

    assert _cli(path, journal_root, "--add", "2", "--apply") == 0
    document = _printed(capsys)
    assert document["before"][OPERATOR_EXTRA_CONNECTION_ATTEMPTS] == 4
    assert document["after"]["effective_connection_attempt_ceiling"] == 18
    assert document["new_attempt_numbers"] == [17, 18]
    written = json.loads(path.read_text(encoding="utf-8"))
    assert written["connection_attempts"] == 12  # never lowered (never touched)

    loaded = runtime._load_status(path, binding)
    assert effective_connection_attempt_ceiling(binding, loaded) == 18


@pytest.mark.parametrize(
    ("fields", "args", "reason"),
    [
        ({}, ("--add", "0"), "--add must be"),
        ({}, ("--add", "-3"), "--add must be"),
        ({OPERATOR_EXTRA_CONNECTION_ATTEMPTS: 50}, ("--add", "3"), "exceeds the cap 64"),
        ({}, ("--add", "53"), "exceeds the cap 64"),
        ({OPERATOR_EXTRA_CONNECTION_ATTEMPTS: -1}, ("--add", "1"), "malformed"),
        ({"state": "COMPLETE"}, ("--add", "1"), "COMPLETE"),
        (
            {"runtime_heartbeat_at_utc": (NOW - timedelta(days=1)).isoformat()},
            ("--add", "1"),
            "not from today",
        ),
        (
            {"runtime_heartbeat_at_utc": (NOW - timedelta(seconds=2)).isoformat()},
            ("--add", "1"),
            "stop it first",
        ),
        ({"journal_generation": "0a2a1d55-0000-5000-8000-000000000001"}, ("--add", "1"), "not the newest"),
        ({"journal_generation": "0a2a1d55-0000-5000-8000-000000000009"}, ("--add", "1"), "no journal"),
        ({"schema": "something-else"}, ("--add", "1"), "not an Options acquisition status"),
        ({"connection_attempts": "12"}, ("--add", "1"), "connection_attempts is malformed"),
    ],
)
def test_cli_refusals_write_nothing(tmp_path, capsys, fields, args, reason):
    _binding_, path, journal_root = _cli_setup(tmp_path, **fields)
    before = path.read_bytes()

    assert _cli(path, journal_root, *args, "--apply") == 2
    document = _printed(capsys)

    assert document["result"] == "REFUSED"
    assert reason in document["reason"]
    assert path.read_bytes() == before


def test_cli_refuses_a_missing_status_file_or_journal_folder(tmp_path, capsys):
    _binding_, path, journal_root = _cli_setup(tmp_path)

    assert _cli(tmp_path / "missing.json", journal_root, "--add", "1") == 2
    assert "missing" in _printed(capsys)["reason"]
    assert _cli(path, tmp_path / "no-journals", "--add", "1") == 2
    assert "journal folder" in _printed(capsys)["reason"]
