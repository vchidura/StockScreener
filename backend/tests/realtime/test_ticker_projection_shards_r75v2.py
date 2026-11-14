"""R75-v2 (Nov 14): ticker-group projection workers. The protocol runs in-process (threads over real pipes) with a fake
projection; byte parity with the serial projection on live data is the rehearsal's job."""
import multiprocessing
import threading
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from realtime import ticker_projection_shards as shards
from realtime.screening_repository import _bar_tickers, shard_tickers
from realtime.ticker_projection import RealtimeTickerWorkspaceProjection, TickerProjectionCancelled

UTC = timezone.utc
CUTOFF = datetime(2026, 11, 13, 20, 50, tzinfo=UTC)
TICKERS = ("XOM", "AAPL", "SPY", "MSFT", "GS", "QQQ", "AMD", "NVDA", "TSLA", "V", "WMT")


@pytest.mark.parametrize("count", [1, 2, 3, 4, 8, 11, 12])
def test_groups_are_balanced_contiguous_blocks_in_serial_order(count):
    groups = [shard_tickers(TICKERS, index, count) for index in range(count)]
    assert [ticker for group in groups for ticker in group] == sorted(TICKERS)
    sizes = [len(group) for group in groups]
    assert max(sizes) - min(sizes) <= 1


def test_group_arguments_are_checked():
    for index, count in ((0, 0), (2, 2), (-1, 3)):
        with pytest.raises(ValueError):
            shard_tickers(TICKERS, index, count)
    with pytest.raises(ValueError):
        shard_tickers(("A", "A"), 0, 1)


def test_bar_scope_keeps_member_order_for_the_group():
    members = tuple(dict(ticker=ticker) for ticker in TICKERS)
    assert _bar_tickers(members, None) == TICKERS
    group = _bar_tickers(members, (1, 3))
    assert set(group) == set(shard_tickers(TICKERS, 1, 3))
    assert list(group) == [ticker for ticker in TICKERS if ticker in group]


@pytest.mark.parametrize("raw,expected", [("", 0), ("0", 0), ("1", 0), ("2", 2), ("4", 4), ("20", 8), ("x", 0)])
def test_switch(raw, expected):
    assert shards.ticker_projection_shards({shards.SHARDS_SWITCH: raw}) == expected
    assert shards.ticker_projection_shards({}) == 0


def test_no_pool_without_the_switch(monkeypatch):
    monkeypatch.delenv(shards.SHARDS_SWITCH, raising=False)
    assert shards.shard_pool() is None


def test_pair_follows_the_scheduler_warm_rule():
    pair = shards.ShardPair(loader_factory=object, state_factory=object)
    loader, state, warm = pair.take("c1", CUTOFF)
    assert warm is False
    pair.settle("c1", CUTOFF, loader, state, warm=False, complete=True)
    later = CUTOFF + timedelta(minutes=5)
    assert pair.take("c1", later) == (loader, state, True)
    # a retry of an older or equal cutoff gets a fresh pair and leaves the warm one alone
    retry = pair.take("c1", CUTOFF)
    assert retry[2] is False and retry[0] is not loader
    pair.settle("c1", CUTOFF, retry[0], retry[1], warm=False, complete=True)
    assert pair.loader is loader and pair.cutoff == CUTOFF
    # a failed warm cycle drops the pair; a completed one advances it
    pair.settle("c1", later, loader, state, warm=True, complete=True)
    assert pair.cutoff == later
    pair.settle("c1", later + timedelta(minutes=5), loader, state, warm=True, complete=False)
    assert pair.loader is None and pair.take("c1", later + timedelta(minutes=10))[2] is False


# ---- in-process protocol: threads over real pipes, fake loaders and a fake projection -----------------------------


class _ThreadProcess:
    def __init__(self, target, args, name, daemon):
        self._thread = threading.Thread(target=target, args=args, name=name, daemon=daemon)
        self.exitcode = None

    def start(self):
        self._thread.start()

    def is_alive(self):
        return self._thread.is_alive()

    def join(self, timeout=None):
        self._thread.join(timeout)

    def terminate(self):
        pass


class _SharedEnd:
    """The worker thread's pipe end: the pool closes its copy after starting a real process, which here is shared."""

    def __init__(self, connection):
        self._connection = connection

    def close(self):
        pass

    def __getattr__(self, name):
        return getattr(self._connection, name)


class _ThreadContext:
    Process = _ThreadProcess

    @staticmethod
    def Pipe(duplex=True):  # noqa: N802 - the multiprocessing context API
        parent, child = multiprocessing.Pipe(duplex=duplex)
        return parent, _SharedEnd(child)


def _inputs(cutoff=CUTOFF, unavailable=()):
    return SimpleNamespace(
        members=tuple(dict(ticker=ticker, security_id=f"s-{ticker}") for ticker in TICKERS),
        cutoff=cutoff, bars=(), unavailable=frozenset(unavailable),
    )


def _fake_project(inputs, *, state=None, should_stop=None, member_filter=None, screening_override=None, **_):
    if should_stop is not None and should_stop():
        raise TickerProjectionCancelled("cancelled")
    tickers = sorted(member["ticker"] for member in inputs.members)
    if member_filter is not None:
        tickers = [ticker for ticker in tickers if ticker in member_filter]
    if "BOOM" in getattr(inputs, "unavailable", ()) and member_filter:
        raise RuntimeError("worker exploded")
    if state is not None and hasattr(state, "calls"):
        state.calls += 1
    return RealtimeTickerWorkspaceProjection(
        snapshots=tuple(f"snap:{ticker}" for ticker in tickers if ticker not in inputs.unavailable),
        materializations=tuple(f"mat:{ticker}:{interval}" for ticker in tickers for interval in ("1d", "1h")
                               if ticker not in inputs.unavailable),
        pattern_materializations=tuple(f"pat:{ticker}" for ticker in tickers if ticker not in inputs.unavailable),
        summary_charts={ticker: {"1d": ticker} for ticker in tickers if ticker not in inputs.unavailable},
        screening_generation=screening_override if screening_override is not None else ("SCREEN", inputs.cutoff),
        unavailable_members=tuple(dict(ticker=ticker, reason="X") for ticker in tickers if ticker in inputs.unavailable),
    )


class _State:
    def __init__(self):
        self.calls = 0
        self.stats = {}


class _Loader:
    created = []

    def __init__(self):
        self.loads = []
        self.last_report = SimpleNamespace(mode="full")
        _Loader.created.append(self)

    def load(self, cursor, *, cohort_id, cutoff, include_portal_history, bar_shard, **_):
        assert include_portal_history is True
        self.loads.append((cutoff, bar_shard))
        return _Loader.inputs_by_cutoff[cutoff]


class _Database:
    def transaction(self):
        class _Unit:
            cursor = object()

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        return _Unit()


def _target(connection, index, count):
    worker = shards.ShardWorker(
        index, count, database_factory=_Database,
        reference_loaders=lambda: (lambda *_a: ((), ()), lambda *_a: ((), ())),
        pair=shards.ShardPair(loader_factory=_Loader, state_factory=_State),
    )
    shards._serve(connection, worker)


@pytest.fixture
def pool(monkeypatch):
    monkeypatch.setattr(shards, "project_realtime_ticker_workspace", _fake_project)
    _Loader.created = []
    _Loader.inputs_by_cutoff = {}
    made = shards.TickerProjectionShardPool(3, context=_ThreadContext(), target=_target)
    yield made
    made.reset()


def _cycle(pool, inputs, scope_cutoff=None, should_stop=None):
    _Loader.inputs_by_cutoff[inputs.cutoff] = inputs
    pool.prepare(dict(cohort_id="c1", cutoff=scope_cutoff or inputs.cutoff))
    return pool.project(inputs, state=_State(), should_stop=should_stop)


def test_sharded_cycle_equals_the_serial_projection(pool):
    inputs = _inputs()
    serial = _fake_project(inputs)
    sharded = _cycle(pool, inputs)
    assert sharded == serial
    assert list(sharded.summary_charts) == list(serial.summary_charts)
    assert sorted(load[1] for loader in _Loader.created for load in loader.loads) == [(0, 3), (1, 3), (2, 3)]
    assert [worker["tickers"] for worker in pool.last_cycle["workers"]] == [4, 4, 3]


def test_workers_stay_warm_across_windows_and_keep_warm_through_a_retry(pool):
    first = _cycle(pool, _inputs())
    assert all(worker["warm"] is False for worker in pool.last_cycle["workers"])
    later = _inputs(CUTOFF + timedelta(minutes=5))
    assert _cycle(pool, later) == _fake_project(later)
    assert all(worker["warm"] is True for worker in pool.last_cycle["workers"])
    assert len(_Loader.created) == 3  # the same three loaders served both windows
    _cycle(pool, _inputs())  # a retry of the older cutoff: fresh pairs, the warm ones untouched
    assert all(worker["warm"] is False for worker in pool.last_cycle["workers"])
    newest = _inputs(CUTOFF + timedelta(minutes=10))
    _cycle(pool, newest)
    assert all(worker["warm"] is True for worker in pool.last_cycle["workers"])
    assert first.screening_generation == ("SCREEN", CUTOFF)


def test_unavailable_members_merge_in_serial_order_and_all_unavailable_fails(pool):
    inputs = _inputs(unavailable=("GS", "XOM"))
    assert _cycle(pool, inputs) == _fake_project(inputs)
    everyone = _inputs(CUTOFF + timedelta(minutes=5), unavailable=TICKERS)
    with pytest.raises(ValueError, match="no available member: AAPL"):
        _cycle(pool, everyone)


def test_a_worker_error_fails_the_cycle_after_every_reply_and_keeps_the_pool(pool):
    _cycle(pool, _inputs())
    workers = list(pool._workers)
    broken = _inputs(CUTOFF + timedelta(minutes=5), unavailable=("BOOM",))
    with pytest.raises(shards.ShardWorkerFailed, match="worker exploded"):
        _cycle(pool, broken)
    assert pool._workers == workers and all(worker.process.is_alive() for worker in workers)
    # the next window still runs on the same workers
    after = _inputs(CUTOFF + timedelta(minutes=10))
    assert _cycle(pool, after) == _fake_project(after)


def test_cancellation_resets_the_pool(pool):
    _cycle(pool, _inputs())
    first_workers = list(pool._workers)
    with pytest.raises(TickerProjectionCancelled):
        _cycle(pool, _inputs(CUTOFF + timedelta(minutes=5)), should_stop=lambda: True)
    assert pool._workers == []
    later = _inputs(CUTOFF + timedelta(minutes=10))
    assert _cycle(pool, later) == _fake_project(later)
    assert pool._workers != first_workers


def test_a_prepared_cycle_that_never_projects_is_discarded(pool):
    _Loader.inputs_by_cutoff[CUTOFF] = _inputs()
    pool.prepare(dict(cohort_id="c1", cutoff=CUTOFF))
    time.sleep(0.2)  # the coordinator failed before projecting (e.g. its own load raised)
    later = _inputs(CUTOFF + timedelta(minutes=5))
    assert _cycle(pool, later) == _fake_project(later)


def test_project_requires_prepare(pool):
    with pytest.raises(RuntimeError, match="not prepared"):
        pool.project(_inputs(), state=_State())
