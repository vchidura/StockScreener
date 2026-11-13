"""R19.1 (Nov 12 21:32 UTC): the Options lifecycle exited on a SerializationFailure in the preparation cycle's first
transaction (expire, mark stale provider requests ambiguous, claim). That transaction is one whole unit with no provider
work, so a serialization failure or deadlock now repeats it; other errors are raised as before."""
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import pytest

import realtime.advanced_options_lifecycle as lifecycle


class _Conflict(Exception):
    def __init__(self, pgcode):
        super().__init__(f"pgcode {pgcode}")
        self.pgcode = pgcode


class _Provider:
    def __init__(self):
        self.transactions = 0

    @contextmanager
    def transaction(self, *_args, **_kwargs):
        self.transactions += 1
        yield SimpleNamespace(cursor=object())


def _patch(monkeypatch, failures):
    calls = {"expire": 0, "mark": 0, "claim": 0}

    def expire(cursor, **_kwargs):
        calls["expire"] += 1
        return ()

    def mark(cursor, **_kwargs):
        calls["mark"] += 1
        if failures:
            raise failures.pop(0)

    def claim(cursor, **_kwargs):
        calls["claim"] += 1
        return None

    monkeypatch.setattr(lifecycle, "expire_obsolete_advanced_options_preparation_cursor", expire)
    monkeypatch.setattr(lifecycle, "mark_stale_provider_requests_ambiguous_cursor", mark)
    monkeypatch.setattr(lifecycle, "claim_advanced_options_preparation_cursor", claim)
    monkeypatch.setattr(lifecycle.retry_transient.__globals__["time"], "sleep", lambda _seconds: None)
    return calls


def _run(provider):
    return lifecycle.run_advanced_options_preparation_cycle(
        provider, object(), owner_id=UUID(int=1), checked_at=datetime(2026, 11, 12, 21, 32, tzinfo=timezone.utc)
    )


def test_a_serialization_failure_in_the_claim_transaction_is_retried(monkeypatch):
    calls = _patch(monkeypatch, [_Conflict("40001"), _Conflict("40P01")])
    provider = _Provider()
    report = _run(provider)
    assert report["status"] == "IDLE"
    assert provider.transactions == 3
    assert calls == {"expire": 3, "mark": 3, "claim": 1}


def test_other_errors_are_raised_at_once(monkeypatch):
    _patch(monkeypatch, [ValueError("invalid ledger row")])
    provider = _Provider()
    with pytest.raises(ValueError, match="invalid ledger row"):
        _run(provider)
    assert provider.transactions == 1


def test_five_conflicts_in_a_row_raise_the_last(monkeypatch):
    _patch(monkeypatch, [_Conflict("40001") for _ in range(5)])
    provider = _Provider()
    with pytest.raises(_Conflict):
        _run(provider)
    assert provider.transactions == 5
