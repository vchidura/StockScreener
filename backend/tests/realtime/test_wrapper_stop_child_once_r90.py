"""G102 R90 (Nov 18 night): the fenced container wrapper and the child's SIGTERM.

Today the wrapper's signal handler and its loop's stop branch each send the child a SIGTERM, microseconds apart; a child
whose handler calls Event.set() deadlocks when the second lands inside the first one's Event.set() (reproduced on the
Options window service). REALTIME_V2_STOP_CHILD_ONCE=ON sends one; unset/OFF sends two as before."""
import os
import signal
import threading
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

import scripts.run_realtime_v2_container as wrapper


class _Pool:
    def __init__(self, *args, **kwargs):
        pass

    def closeall(self):
        pass


class _Provider:
    def __init__(self, *args, **kwargs):
        pass

    def verify(self):
        return "stub"

    @contextmanager
    def transaction(self, *args, **kwargs):
        yield SimpleNamespace(cursor=None)


class _Heartbeat:
    def __init__(self, *args, **kwargs):
        pass

    def start(self):
        return None

    def mark_running(self):
        pass

    def close(self):
        pass


class _Child:
    """A child that ends by the stop signal once it has received one (returncode -15)."""

    def __init__(self):
        self.terminations = 0
        self.returncode = None

    def poll(self):
        return self.returncode

    def terminate(self):
        self.terminations += 1

    def wait(self, timeout=None):
        self.returncode = -int(signal.SIGTERM)
        return self.returncode

    def kill(self):
        pass


def _stop_with_sigterm(monkeypatch, environment):
    monkeypatch.setattr(wrapper, "ThreadedConnectionPool", _Pool)
    monkeypatch.setattr(wrapper, "V2ConnectionProvider", _Provider)
    monkeypatch.setattr(wrapper, "PostgresLeaseHeartbeat", _Heartbeat)
    child = _Child()

    def factory(command, env):
        # The container is stopped a moment after the child started.
        threading.Timer(0.2, lambda: os.kill(os.getpid(), signal.SIGTERM)).start()
        return child

    prior = signal.getsignal(signal.SIGTERM)
    code = wrapper.run_fenced_child(
        "options-p8-window", ["python", "child"], {"REALTIME_V2_APP_DSN": "stub", **environment},
        process_factory=factory,
    )
    assert signal.getsignal(signal.SIGTERM) == prior  # the wrapper restores the handler it replaced
    return code, child


@pytest.mark.parametrize("environment", [{}, {"REALTIME_V2_STOP_CHILD_ONCE": "OFF"}, {"REALTIME_V2_STOP_CHILD_ONCE": " "}])
def test_off_sends_two_sigterms_as_today(monkeypatch, environment):
    code, child = _stop_with_sigterm(monkeypatch, environment)
    assert code == 0 and child.terminations == 2


@pytest.mark.parametrize("value", ["ON", " on "])
def test_on_sends_one(monkeypatch, value):
    code, child = _stop_with_sigterm(monkeypatch, {"REALTIME_V2_STOP_CHILD_ONCE": value})
    assert code == 0 and child.terminations == 1


def test_an_invalid_value_refuses_to_start():
    assert wrapper.stop_child_once_enabled({}) is False
    assert wrapper.stop_child_once_enabled({"REALTIME_V2_STOP_CHILD_ONCE": "off"}) is False
    assert wrapper.stop_child_once_enabled({"REALTIME_V2_STOP_CHILD_ONCE": " On "}) is True
    for bad in ("YES", "1", "RECORD"):
        with pytest.raises(ValueError, match="REALTIME_V2_STOP_CHILD_ONCE"):
            wrapper.stop_child_once_enabled({"REALTIME_V2_STOP_CHILD_ONCE": bad})
