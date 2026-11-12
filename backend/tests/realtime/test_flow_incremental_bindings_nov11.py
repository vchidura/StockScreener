"""V3-1.F (Nov 11): the flow role re-read and re-decoded every downstream binding of the session twice a second
(`_FlowVolumeBaselineReconciler._refresh`), so its CPU grew from 13% to 43% of a core over the session. With
REALTIME_V3_FLOW_LOOP=V3 it reads only the bindings after the last one it saw, checked as a continuation of it."""
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from realtime import advanced_options_p8_roles as roles
from realtime import advanced_options_session_state as state_module

SESSION = SimpleNamespace(binding_sha256="s" * 64)


def chain(count):
    rows, previous = [], None
    for sequence in range(1, count + 1):
        sha = f"{sequence:02d}" * 32
        rows.append(dict(sequence=sequence, binding_sha256=sha, previous_binding_sha256=previous,
                         session_binding_sha256=SESSION.binding_sha256,
                         production_binding={"volume_baselines": []}))
        previous = sha
    return rows


class _Cursor:
    """rt.advanced_options_downstream_heads / _bindings for one session, answering the incremental query."""

    def __init__(self, rows, head=None):
        self.rows, self.head, self.result = rows, head, []

    def execute(self, sql, params):
        after = params[2]
        head_seq, head_sha = self.head if self.head is not None else (
            len(self.rows), self.rows[-1]["binding_sha256"] if self.rows else None)
        tail = [row for row in self.rows if after < row["sequence"] <= head_seq]
        self.result = ([(head_seq, head_sha, row["sequence"], row["binding_sha256"], row) for row in tail]
                       or [(head_seq, head_sha, None, None, None)])

    def fetchall(self):
        return self.result


def repo(rows, head=None):
    cursor = _Cursor(rows, head)

    @contextmanager
    def transaction(*_args, **_kwargs):
        yield SimpleNamespace(cursor=cursor)

    return SimpleNamespace(
        _load_stored=lambda: SimpleNamespace(session_binding=SESSION, session_binding_id="b"),
        _provider=SimpleNamespace(transaction=transaction), _monotonic=lambda: 0.0,
        _head_sequence=0, _head_binding=None, _head_checked_at=None,
    )


@pytest.fixture(autouse=True)
def plain_payloads(monkeypatch):
    monkeypatch.setattr(state_module, "p8_downstream_binding_from_payload", lambda row: SimpleNamespace(**row))


def after(self, sequence, sha):
    return state_module.PostgresContinuousSessionStateRepository.load_downstream_bindings_after(
        self, SESSION, after_sequence=sequence, after_binding_sha256=sha)


def test_every_prefix_continues_to_the_same_tail():
    rows = chain(5)
    for k in range(0, 6):
        sha = None if k == 0 else rows[k - 1]["binding_sha256"]
        got = after(repo(rows), k, sha)
        assert [b.sequence for b in got] == list(range(k + 1, 6))


def test_a_continuation_from_a_binding_not_in_the_chain_is_refused():
    rows = chain(4)
    with pytest.raises(RuntimeError, match="chain diverged"):
        after(repo(rows), 2, "f" * 64)


def test_a_head_that_does_not_end_the_chain_is_refused():
    rows = chain(3)
    with pytest.raises(RuntimeError, match="not the chain tail"):
        after(repo(rows, head=(3, "e" * 64)), 1, rows[0]["binding_sha256"])
    with pytest.raises(RuntimeError, match="not the chain tail"):  # the head moved back below what we hold
        after(repo(rows, head=(1, rows[0]["binding_sha256"])), 2, rows[1]["binding_sha256"])


def test_nothing_new_returns_nothing_and_keeps_the_head():
    rows = chain(3)
    self = repo(rows)
    assert after(self, 3, rows[2]["binding_sha256"]) == ()
    with pytest.raises(ValueError):
        after(self, 0, "x" * 64)


def test_the_reconciler_reads_only_new_bindings_in_v3_mode(monkeypatch):
    monkeypatch.setattr(roles, "RawJournalReader", lambda _path: SimpleNamespace(offsets_at=lambda _cutoff: (5, 4)))
    rows = [SimpleNamespace(**row) for row in chain(3)]
    calls = []

    class State:
        def load_preopen_session_binding(self, _bundle):
            return SESSION

        def load_downstream_bindings(self, _session):
            raise AssertionError("the full read is not used in V3 mode")

        def load_downstream_bindings_after(self, _session, *, after_sequence, after_binding_sha256):
            calls.append((after_sequence, after_binding_sha256))
            return tuple(row for row in rows if row.sequence > after_sequence)

    reconciler = roles._FlowVolumeBaselineReconciler(session_state=State(), bundle={}, source=SimpleNamespace(path=1),
                                                     refresh_seconds=0, incremental=True)
    reconciler._refresh()
    rows.append(SimpleNamespace(**chain(4)[3]))
    reconciler._refresh()
    assert calls == [(0, None), (3, rows[2].binding_sha256)]
    assert (reconciler._seen_sequence, reconciler._seen_sha256) == (4, rows[3].binding_sha256)


def test_the_switch_defaults_to_p8(monkeypatch):
    monkeypatch.setattr(roles, "RawJournalReader", lambda _path: None)
    assert roles.flow_loop_mode({}) == "P8" and roles.flow_loop_mode({"REALTIME_V3_FLOW_LOOP": "v3"}) == "V3"
    assert roles.flow_loop_mode({"REALTIME_V3_FLOW_LOOP": "yes"}) == "P8"
    full_only = roles._FlowVolumeBaselineReconciler(
        session_state=SimpleNamespace(load_preopen_session_binding=lambda _b: SESSION, load_downstream_bindings=lambda _s: ()),
        bundle={}, source=SimpleNamespace(path=1), incremental=True)
    assert full_only._incremental is False  # a state without the incremental read keeps the full one
