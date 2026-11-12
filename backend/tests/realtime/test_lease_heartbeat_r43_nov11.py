"""R43 (Nov 11): Stock Alerts and retention exited 0 mid-session with nothing in their logs. A fenced role's lease
heartbeat stopped the role on the first database error during a renewal, without saying so, and a failed service-state
write ended the heartbeat thread while the role kept running. Now: one retry while the lease has time left, the reason is
logged before the role stops, and a failed state write is logged without ending renewals."""
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest
from psycopg2 import OperationalError

from realtime import advanced_realtime_fencing as fencing
from realtime.database_access_v2 import V2ConnectionProvider

UTC = timezone.utc
NOW = datetime(2026, 11, 11, 18, 45, tzinfo=UTC)
OWNER = UUID(int=43)


class _Provider(V2ConnectionProvider):
    def __init__(self):  # no pools: the renewal function is replaced below
        pass

    @contextmanager
    def transaction(self, *_args, **_kwargs):
        yield SimpleNamespace(cursor=object())


class _NoWait:
    def wait(self, _seconds):
        return False

    def set(self):
        pass


def _heartbeat(monkeypatch, outcomes, *, lease_left=30.0):
    """outcomes: what each renewal does in turn: an exception instance to raise, None, or 'ok'."""
    calls = []

    def renew(_cursor, lease, *, checked_at, lease_seconds):
        calls.append(checked_at)
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        if outcome is None:
            return None
        return fencing.DurableRuntimeLease(lease.resource_key, lease.owner_id, lease.owner_epoch,
                                           checked_at + timedelta(seconds=lease_seconds))

    monkeypatch.setattr(fencing, "renew_durable_runtime_lease_cursor", renew)
    stop = SimpleNamespace(flag=False, set=lambda: setattr(stop, "flag", True))
    beat = fencing.PostgresLeaseHeartbeat(_Provider(), resource_key="advanced-realtime-v3:role:canonical-stock-alerts",
                                          owner_id=OWNER, stop_event=stop, clock=lambda: NOW)
    beat.lease = fencing.DurableRuntimeLease("advanced-realtime-v3:role:canonical-stock-alerts", OWNER, 7,
                                             NOW + timedelta(seconds=lease_left))
    beat._stop = _NoWait()
    return beat, stop, calls


def test_one_database_error_is_retried_and_the_lease_kept(monkeypatch, caplog):
    beat, stop, calls = _heartbeat(monkeypatch, [OperationalError("server closed the connection"), "ok"])
    with caplog.at_level(logging.WARNING, logger=fencing.__name__):
        renewed, reason = beat._renew()
    assert renewed is not None and reason is None and len(calls) == 2 and not stop.flag
    assert "durable_lease_renewal_retry" in caplog.text and "OperationalError" in caplog.text


def test_two_errors_stop_the_role_and_say_why(monkeypatch, caplog):
    beat, stop, _ = _heartbeat(monkeypatch, [OperationalError("timeout"), OperationalError("timeout again")])
    beat._stop = SimpleNamespace(wait=lambda _s, _n=iter([False, False]): next(_n))  # loop wait, then the retry wait
    with caplog.at_level(logging.WARNING, logger=fencing.__name__):
        beat._run()
    assert beat.lost and stop.flag
    assert "durable_lease_lost" in caplog.text and "RENEWAL_ERROR OperationalError: timeout" in caplog.text
    assert "retry OperationalError: timeout again" in caplog.text


def test_a_refused_renewal_is_final_and_logged(monkeypatch, caplog):
    beat, stop, calls = _heartbeat(monkeypatch, [None])
    beat._stop = SimpleNamespace(wait=lambda _s, _n=iter([False, True]): next(_n))
    with caplog.at_level(logging.WARNING, logger=fencing.__name__):
        beat._run()
    assert beat.lost and stop.flag and len(calls) == 1
    assert "LEASE_NOT_RENEWED" in caplog.text


def test_no_retry_when_the_lease_is_about_to_end(monkeypatch):
    beat, _, calls = _heartbeat(monkeypatch, [OperationalError("timeout"), "ok"], lease_left=1.5)
    renewed, reason = beat._renew()
    assert renewed is None and "no retry" in reason and len(calls) == 1


def test_a_failed_state_write_does_not_end_renewals(monkeypatch, caplog):
    beat, stop, calls = _heartbeat(monkeypatch, ["ok", "ok"])
    beat._runtime_kind = "COMPOSE"

    def failing_record():
        raise OperationalError("could not write service state")

    beat._record = failing_record
    beat._stop = SimpleNamespace(wait=lambda _s, _n=iter([False, False, True]): next(_n))
    with caplog.at_level(logging.WARNING, logger=fencing.__name__):
        beat._run()
    assert len(calls) == 2 and not beat.lost and not stop.flag
    assert caplog.text.count("durable_lease_state_record_failed") == 2


@pytest.mark.parametrize("module", ["scripts.run_realtime_v2_retention_service", "scripts.run_realtime_v2_container"])
def test_stop_signals_are_logged(module):
    import importlib
    source = importlib.import_module(module).__file__
    text = open(source, encoding="utf-8").read()
    assert ("retention_stop_signal" in text) or ("fenced_role_stop_signal" in text)
