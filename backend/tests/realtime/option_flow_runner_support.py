"""Offline builders for the OPTIONS_FLOW runner tests: a pinned-semantics test double, scripted scenarios
for the mock feed, an in-memory stand-in for the options database (checkpoints, gaps, the writer's COPY
statements) and a real StageOrchestrator over the in-memory repository. No provider, socket or file I/O.
"""
from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

from option_flow_support import (
    CALL,
    CANCEL,
    OTHER,
    PAYLOAD,
    PUT,
    REGULAR,
    REVIEW,
    T0,
    policy,
    quote,
    trade,
)
from orchestration_fakes import InMemoryOrchestrationRepository
from realtime.option_flow.conditions import ConditionSemantics, build_semantics, parse_payload, parse_review
from realtime.option_flow.consumer import ConsumerLimits
from realtime.option_flow.mock_feed import MockOptionFeed
from realtime.option_flow.volume import BaselineKind
from realtime.option_flow.writer import CHECKPOINT_COLUMNS, CLUSTER_COLUMNS, EVENT_COLUMNS
from realtime.orchestration_repository import ServicePlan
from realtime.runner_environment import RunnerEnvironment
from realtime.stage_graph import STAGES_V1
from realtime.stage_orchestrator import StageOrchestrator, StageOutcome
from realtime.stage_runners_options import OptionsFlowSettings, options_flow_runner

SESSION = date(2026, 10, 12)
NOW = datetime(2026, 10, 12, 15, 0, tzinfo=timezone.utc)
ALL = frozenset(STAGES_V1)
IWM = "O:IWM261218C00200000"
DIA = "O:DIA261218C00400000"


def pinned_semantics() -> ConditionSemantics:
    """TEST DOUBLE shaped like a live pin: the synthetic fixture with its provenance relabelled so the runner
    accepts it. It exists only so the runner's full path can be exercised offline; the synthetic fixture
    itself is refused (see test_stage_runners_options)."""
    payload = json.loads(PAYLOAD.read_text(encoding="utf-8"))
    review = json.loads(REVIEW.read_text(encoding="utf-8"))
    payload["provenance"] = review["provenance"] = "LIVE_CAPTURED"
    return build_semantics(parse_payload(payload), parse_review(review))


def scenario_messages(base_ms: int) -> list[dict]:
    """Quotes, volume, a venue burst that becomes a cluster, a cancel that supersedes a persisted print and a
    cancelled burst. Times are relative to `base_ms`."""
    messages = [quote(seq=1, time_ms=base_ms - 100, bid=4.9, ask=5.0),
                quote(symbol=PUT, seq=1, time_ms=base_ms - 100, bid=2.0, ask=2.1),
                quote(symbol=OTHER, seq=1, time_ms=base_ms - 100)]
    messages += [trade(seq=seq, time_ms=base_ms + seq, price=1.0, size=10) for seq in range(1, 4)]
    messages += [trade(seq=10 + index, time_ms=base_ms + 100 + index * 100, price=5.0, size=100,
                       exchange=302 + index) for index in range(3)]
    messages.append(trade(seq=20, time_ms=base_ms + 600, price=5.0, size=150, exchange=305))
    messages.append(trade(seq=21, time_ms=base_ms + 700, price=5.0, size=150, exchange=305, conditions=(CANCEL,)))
    messages.append(trade(seq=24, time_ms=base_ms + 950, price=4.95, size=102, exchange=306))
    messages.append(trade(symbol=PUT, seq=1, time_ms=base_ms + 1000, price=2.1, size=300, exchange=310))
    messages += [trade(symbol=OTHER, seq=1 + index, time_ms=base_ms + 1100 + index * 50, price=5.0, size=100,
                       exchange=311 + index) for index in range(3)]
    messages.append(trade(symbol=IWM, seq=30, time_ms=base_ms + 1300, price=5.0, size=100, exchange=330))
    messages.append(trade(symbol=IWM, seq=30, time_ms=base_ms + 1300, price=5.0, size=100, exchange=330,
                          conditions=(CANCEL,)))
    burst = [(40 + index, base_ms + 1400 + index * 50, 320 + index) for index in range(3)]
    messages += [trade(symbol=DIA, seq=seq, time_ms=time_ms, price=5.0, size=100, exchange=exchange)
                 for seq, time_ms, exchange in burst]
    messages += [trade(symbol=DIA, seq=seq, time_ms=time_ms, price=5.0, size=100, exchange=exchange,
                       conditions=(CANCEL,)) for seq, time_ms, exchange in burst]
    messages += [trade(seq=100 + index, time_ms=base_ms + 3000 + index * 40, price=5.0, size=100,
                       exchange=300 + index % 6) for index in range(12)]
    return messages


def scenario_feed(base_ms: int = T0, *, frame_size: int = 1) -> MockOptionFeed:
    feed = MockOptionFeed()
    feed.extend(scenario_messages(base_ms), frame_size=frame_size)
    return feed


def settings(**overrides) -> OptionsFlowSettings:
    values = dict(
        shard_count=2, flush_rows=1, flush_interval_seconds=1_000_000.0, initial_baseline=BaselineKind.SESSION_COMPLETE,
        limits=ConsumerLimits(batch_size=4, max_messages=10_000, max_seconds=1_000_000.0),
        history_per_contract=64, checkpoint_interval_ms=1_000, checkpoint_interval_seconds=0.0)
    values.update(overrides)
    return OptionsFlowSettings(**values)


def parse_rows(lines, columns) -> list[dict]:
    return [dict(zip(columns, line.rstrip("\n").split("\t"))) for line in lines]


# --- the in-memory options database ----------------------------------------------------------------

@dataclass
class FakeOptionsDatabase:
    """Applies a transaction's effects on commit, so a rolled-back flush leaves nothing behind."""

    checkpoints: dict = field(default_factory=dict)
    gaps: dict = field(default_factory=dict)
    events: list = field(default_factory=list)
    clusters: list = field(default_factory=list)
    volume: list = field(default_factory=list)
    event_keys: dict = field(default_factory=dict)
    cluster_keys: dict = field(default_factory=dict)
    volume_store: dict = field(default_factory=dict)
    transactions: list = field(default_factory=list)
    commit_log: list = field(default_factory=list)
    oi_rows: list = field(default_factory=list)
    instruments: dict = field(default_factory=dict)
    diverge_events: bool = False
    fail_gap_insert: Exception | None = None
    fail_checkpoint_insert: Exception | None = None
    commits: int = 0
    rollbacks: int = 0

    def transaction(self, role="app", *, read_only=False, dict_cursor=False, isolation_level=None, fence=None):
        assert role == "app", role  # V2 has one runtime role; the stage's work area selects nothing
        return self._transaction(role, read_only)

    @contextmanager
    def _transaction(self, role, read_only):
        self.transactions.append((role, read_only))
        effects: list = []
        cursor = _FakeCursor(self, effects)
        try:
            yield SimpleNamespace(cursor=cursor, read_only=read_only)
        except BaseException:
            self.rollbacks += 1
            raise
        for effect in effects:
            effect()
        self.commits += 1
        self.commit_log.append((role, read_only, frozenset(cursor.tables)))

    def roles(self, *, write=None):
        return {role for role, read_only in self.transactions if write is None or write == (not read_only)}

    def event_rows(self):
        return parse_rows(self.events, EVENT_COLUMNS)

    def cluster_rows(self):
        return parse_rows(self.clusters, CLUSTER_COLUMNS)

    def volume_rows(self):
        """The final state per (cohort, session, contract), with the writer's regression guard applied."""
        return parse_rows(sorted(self.volume_store.values()), CHECKPOINT_COLUMNS)


class _FakeCursor:
    def __init__(self, database: FakeOptionsDatabase, effects: list) -> None:
        self._db = database
        self._effects = effects
        self._staged: dict[str, list[str]] = {}
        self._rows: list = []
        self._divergent: dict[str, list] = {}
        self._pending: dict[str, dict] = {}
        self.tables: set[str] = set()  # the tables this transaction wrote, to tell a gap write from a flush
        self.rowcount = 0

    def close(self) -> None:
        return None

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows

    def copy_expert(self, statement, file):
        stage = statement.split()[1]
        self._staged[stage] = [line + "\n" for line in file.read().split("\n") if line]

    def _keyed_insert(self, table, lines, sink, keys, width):
        """INSERT ... ON CONFLICT DO NOTHING over an immutable table: an identical replay inserts nothing,
        a different row under the same key is remembered for the divergence check."""
        pending = self._pending.setdefault(table, {})
        fresh = []
        for line in lines:
            key = tuple(line.rstrip("\n").split("\t")[:width])
            stored = keys.get(key, pending.get(key))
            if stored is None:
                fresh.append((key, line))
                pending[key] = line
            elif stored != line:
                self._divergent.setdefault(table, []).append(key)
        self._effects.append(lambda: [(sink.append(line), keys.__setitem__(key, line)) for key, line in fresh])
        self.rowcount = len(fresh)

    def execute(self, statement, parameters=None):
        text = " ".join(statement.split())
        db = self._db
        if text.startswith(("INSERT INTO ", "UPDATE ")):
            self.tables.add(text.split()[2] if text.startswith("INSERT") else text.split()[1])
        if text.startswith("CREATE TEMP TABLE"):
            return
        if text.startswith("INSERT INTO rt.option_flow_events"):
            self._keyed_insert("rt.option_flow_events", self._staged.get("_stage_option_flow_events", []),
                               db.events, db.event_keys, 2)
        elif text.startswith("INSERT INTO rt.option_flow_clusters"):
            self._keyed_insert("rt.option_flow_clusters", self._staged.get("_stage_option_flow_clusters", []),
                               db.clusters, db.cluster_keys, 3)
        elif text.startswith("INSERT INTO rt.option_volume_checkpoints"):
            written = []
            for line in self._staged.get("_stage_option_volume_checkpoints", []):
                columns = line.rstrip("\n").split("\t")
                stored = db.volume_store.get(tuple(columns[:3]))
                if stored is None or int(columns[4]) >= int(stored.rstrip("\n").split("\t")[4]):
                    written.append((tuple(columns[:3]), line))
            self._effects.append(lambda: [(db.volume.append(line), db.volume_store.__setitem__(key, line))
                                          for key, line in written])
            self.rowcount = len(written)
        elif "IS DISTINCT FROM" in text:
            table = "rt.option_flow_events" if "rt.option_flow_events" in text else "rt.option_flow_clusters"
            rows = [tuple(key) for key in self._divergent.get(table, [])]
            if db.diverge_events and table == "rt.option_flow_events":
                rows = [(CALL, 1, "2026-10-12 14:30:00+00", 1)]
            self._rows = rows[:5]
        elif text.startswith("INSERT INTO rt.consumer_checkpoints"):
            if db.fail_checkpoint_insert is not None:
                raise db.fail_checkpoint_insert
            key = (parameters[0], parameters[1], parameters[2], parameters[3])
            offset, allow = parameters[6], parameters[-1]
            stored = db.checkpoints.get(key)
            if stored is not None and not allow and offset < stored[0]:
                self.rowcount = 0
                return
            payload = getattr(parameters[10], "adapted", parameters[10])
            row = (offset, parameters[4], parameters[5], parameters[8], parameters[9], bytes(payload))
            self._effects.append(lambda key=key, row=row: db.checkpoints.__setitem__(key, row))
            self.rowcount = 1
        elif text.startswith("UPDATE rt.consumer_checkpoints SET committed_offset"):
            offset, scope = parameters[0], tuple(parameters[1:4])
            keys = [key for key, row in db.checkpoints.items() if key[:3] == scope and row[0] <= parameters[4]]
            self._effects.append(lambda keys=keys, offset=offset: [
                db.checkpoints.__setitem__(key, (offset, *db.checkpoints[key][1:])) for key in keys])
            self.rowcount = len(keys)
        elif text.startswith("SELECT shard_id, committed_offset"):
            self._rows = [(key[3], row[0], row[1], row[2], row[3], row[4], row[5])
                          for key, row in sorted(db.checkpoints.items(), key=lambda item: item[0][3])
                          if key[:3] == tuple(parameters)]
        elif text.startswith("INSERT INTO rt.stream_gaps"):
            if db.fail_gap_insert is not None:
                raise db.fail_gap_insert
            key = parameters[0]
            fresh = key not in db.gaps
            if fresh:
                self._effects.append(lambda key=key, parameters=tuple(parameters): db.gaps.__setitem__(key, parameters))
            self.rowcount = 1 if fresh else 0
        elif text.startswith("SELECT DISTINCT ON (c.contract_ticker) c.contract_ticker, i.instrument_id"):
            wanted = set(parameters[0])
            self._rows = [(ticker, identity) for ticker, identity in sorted(db.instruments.items()) if ticker in wanted]
        elif text.startswith("SELECT DISTINCT ON (c.contract_ticker) c.contract_ticker, b.open_interest"):
            self._rows = list(db.oi_rows)
        else:
            raise AssertionError(f"unexpected statement: {text[:80]}")


# --- the orchestrator -------------------------------------------------------------------------------

class Clock:
    def __init__(self, value: datetime = NOW) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


class MonotonicClock:
    """Injected bounded-slice clock; `step` advances it on every read so a time bound is deterministic."""

    def __init__(self, step: float = 0.0) -> None:
        self.value = 0.0
        self.step = step

    def __call__(self) -> float:
        self.value += self.step
        return self.value


@dataclass
class Harness:
    orchestrator: StageOrchestrator
    repository: InMemoryOrchestrationRepository
    plan: ServicePlan
    environment: RunnerEnvironment
    database: FakeOptionsDatabase

    def tick(self) -> StageOutcome:
        return self.orchestrator.tick_resident(self.plan.plan_id, "OPTIONS_FLOW")

    def readiness(self):
        return self.repository.ready[(self.plan.cohort_id, "OPTIONS_FLOW")]


def harness(feed, *, database: FakeOptionsDatabase | None = None, flow_settings: OptionsFlowSettings | None = None,
            runners: dict | None = None, oi_loader=None, semantics: ConditionSemantics | None = None,
            flow_policy=None, enabled=ALL, clock: MonotonicClock | None = None,
            environment_overrides: dict | None = None, cohort_id=None) -> Harness:
    database = database if database is not None else FakeOptionsDatabase()
    plan = ServicePlan(uuid4(), cohort_id or uuid4(), SESSION, "stages_v1", frozenset(enabled), "a" * 64,
                       NOW + timedelta(hours=8))
    repository = InMemoryOrchestrationRepository(plan)
    repository.upsert_readiness(plan.cohort_id, "REFERENCE", "stages_v1", "READY", (), None, NOW)
    values = dict(
        options_flow_source=feed,
        options_flow_semantics=pinned_semantics() if semantics is None else semantics,
        options_flow_policy=policy(sweep_window_seconds=5, minimum_sweep_prints=3) if flow_policy is None else flow_policy,
        options_flow_settings=settings() if flow_settings is None else flow_settings,
        options_flow_oi_loader=oi_loader if oi_loader is not None else (lambda context, session: {}),
        options_flow_clock=clock if clock is not None else MonotonicClock(),
    )
    values.update(environment_overrides or {})
    environment = RunnerEnvironment(database, **values)
    table = {name: (lambda context: StageOutcome.complete()) for name in enabled}
    table["OPTIONS_FLOW"] = options_flow_runner(environment)
    table.update(runners or {})
    orchestrator = StageOrchestrator(repository, table, database=database, clock=Clock())
    return Harness(orchestrator, repository, plan, environment, database)
