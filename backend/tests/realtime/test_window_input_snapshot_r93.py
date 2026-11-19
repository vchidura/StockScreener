"""G102 R93 (Nov 18 night): the Options window reads its inputs in a REPEATABLE READ READ ONLY snapshot behind
REALTIME_V3_WINDOW_INPUT_SNAPSHOT (OFF / ON).

OFF parity: every producer opens exactly today's transactions (one SERIALIZABLE read-write transaction that assembles
and persists; the native load in SERIALIZABLE READ ONLY) and returns the same value. ON: the assemble runs in a
REPEATABLE READ READ ONLY transaction, the persist alone in SERIALIZABLE, and the result is identical."""
import inspect
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from psycopg2 import Error as PsycopgError

import realtime.advanced_options_mark_producer as mark_module
import realtime.advanced_options_p8_window as window_module
import realtime.advanced_options_reference_producer as reference_module
import realtime.advanced_options_rolling_producer as rolling_module
from advanced_options_reference_support import reference_state
from advanced_options_rolling_support import rolling_state
from realtime.advanced_options_mark_producer import (
    AdvancedOptionsMarkPreflightBlocked,
    AdvancedOptionsMarkStatus,
    PostgresAdvancedOptionsMarkProducer,
    freeze_advanced_options_mark_producer_plan,
)
from realtime.advanced_options_native_loader import (
    PostgresAdvancedOptionsNativeInputLoader,
    freeze_advanced_options_native_input_plan,
)
from realtime.advanced_options_reference_producer import (
    AdvancedOptionsReferencePreflightBlocked,
    AdvancedOptionsReferenceProducerPreflight,
    PostgresAdvancedOptionsReferenceProducer,
    ReferenceProducerStatus,
)
from realtime.advanced_options_rolling_producer import (
    AdvancedOptionsRollingPreflightBlocked,
    AdvancedOptionsRollingProducerPreflight,
    PostgresAdvancedOptionsRollingProducer,
    RollingProducerStatus,
)
from realtime.advanced_options_window_snapshot import (
    WINDOW_INPUT_SNAPSHOT_SETTING,
    SnapshotReadProvider,
    window_input_reads,
    window_input_snapshot_enabled,
)
from realtime.database_access_v2 import APP_ROLE, V2ConnectionProvider
from test_advanced_options_mark_producer import _plan as _mark_plan, _preflight as _mark_preflight
from test_advanced_options_native_loader import _plan as _native_plan, _preflight as _native_preflight
from test_advanced_options_reference_producer import _plan as _reference_plan
from test_advanced_options_rolling_producer import _plan as _rolling_plan


class _RetryableError(PsycopgError):
    pgcode = "40001"


class _Recorder(V2ConnectionProvider):
    """Records every transaction (read_only, isolation) and which one each assemble / persist ran in."""

    def __init__(self, fail_write_once=False):
        self.transactions = []
        self.events = []
        self.current = None
        self.fail_write_once = fail_write_once

    @contextmanager
    def transaction(self, role=APP_ROLE, *, read_only=False, isolation_level=None, **kwargs):
        index = len(self.transactions)
        self.transactions.append((read_only, isolation_level))
        if self.fail_write_once and not read_only:
            self.fail_write_once = False
            raise _RetryableError("could not serialize access")
        self.current = index
        try:
            yield SimpleNamespace(cursor=object(), connection=object(), index=index)
        finally:
            self.current = None


@pytest.fixture(autouse=True)
def _switch_unset(monkeypatch):
    monkeypatch.delenv(WINDOW_INPUT_SNAPSHOT_SETTING, raising=False)


def _set(monkeypatch, value):
    if value is None:
        monkeypatch.delenv(WINDOW_INPUT_SNAPSHOT_SETTING, raising=False)
    else:
        monkeypatch.setenv(WINDOW_INPUT_SNAPSHOT_SETTING, value)


# --- switch ----------------------------------------------------------------------------------------------------------

def test_switch_values_and_loud_failure():
    assert window_input_snapshot_enabled({}) is False
    assert window_input_snapshot_enabled({WINDOW_INPUT_SNAPSHOT_SETTING: " "}) is False
    assert window_input_snapshot_enabled({WINDOW_INPUT_SNAPSHOT_SETTING: "off"}) is False
    assert window_input_snapshot_enabled({WINDOW_INPUT_SNAPSHOT_SETTING: " On "}) is True
    for bad in ("YES", "1", "RECORD", "SNAPSHOT", "TRUE"):
        with pytest.raises(ValueError, match=WINDOW_INPUT_SNAPSHOT_SETTING):
            window_input_snapshot_enabled({WINDOW_INPUT_SNAPSHOT_SETTING: bad})


def test_window_service_start_checks_the_switch():
    source = (Path(reference_module.__file__).parents[1] / "scripts"
              / "run_advanced_options_v3_production.py").read_text(encoding="utf-8")
    start = source.index("if kind is AdvancedOptionsProcessKind.WINDOW:")
    end = source.index("provider = None", start)
    assert "window_input_snapshot_enabled()" in source[start:end]


# --- the read proxy --------------------------------------------------------------------------------------------------

def test_reads_proxy_moves_only_default_isolation_reads():
    provider = _Recorder()
    proxy = SnapshotReadProvider(provider)
    with proxy.transaction(APP_ROLE, read_only=True):
        pass
    with proxy.transaction(APP_ROLE, read_only=True, dict_cursor=True):
        pass
    with proxy.transaction(APP_ROLE, read_only=True, isolation_level="SERIALIZABLE"):
        pass
    with proxy.transaction(APP_ROLE):
        pass
    with proxy.transaction(APP_ROLE, isolation_level="SERIALIZABLE"):
        pass
    assert provider.transactions == [
        (True, "REPEATABLE READ"), (True, "REPEATABLE READ"), (True, "SERIALIZABLE"), (False, None),
        (False, "SERIALIZABLE"),
    ]
    assert proxy.wrapped is provider


def test_off_reads_are_the_provider_itself(monkeypatch):
    provider = _Recorder()
    assert window_input_reads(provider) is provider
    _set(monkeypatch, "OFF")
    assert window_input_reads(provider) is provider
    _set(monkeypatch, "ON")
    proxy = window_input_reads(provider)
    assert isinstance(proxy, SnapshotReadProvider) and window_input_reads(proxy) is proxy


def test_window_routes_its_default_isolation_reads_through_the_proxy():
    source = inspect.getsource(window_module.build_and_run_p8_serial_window)
    assert "reads = window_input_reads(provider)" in source
    assert "with reads.transaction(APP_ROLE, read_only=True) as unit:" in source
    for call in ("_equity_receipt(\n        reads,", "run_f0dte_window(provider=reads,",
                 "_flow_runtime_policy_sha256(\n            reads,", "evidence_builder=_evidence_builder(\n            reads,",
                 "record_options_alert_context(\n        reads,"):
        assert call in source, call
    # The producers keep the provider: they name their own isolation.
    for producer in ("PostgresAdvancedOptionsReferenceProducer(\n        provider,",
                     "PostgresAdvancedOptionsRollingProducer(\n        provider,",
                     "PostgresAdvancedOptionsMarkProducer(\n        provider,",
                     "PostgresAdvancedOptionsNativeInputLoader(\n        provider,"):
        assert producer in source, producer


# --- producers: OFF parity and ON shape --------------------------------------------------------------------------------

def _run_reference(monkeypatch, value, *, ready=True, fail_write_once=False):
    _set(monkeypatch, value)
    provider = _Recorder(fail_write_once=fail_write_once)
    waits = []
    producer = PostgresAdvancedOptionsReferenceProducer(provider, _reference_plan(), maximum_attempts=2,
                                                        retry_initial_seconds=0.01, wait=waits.append)
    state = reference_state()
    result = AdvancedOptionsReferenceProducerPreflight(
        status=ReferenceProducerStatus.READY if ready else ReferenceProducerStatus.BLOCKED,
        plan_id=producer._plan.plan_id, preflight_sha256="a" * 64, evidence=(), state=state if ready else None)

    def assemble(unit):
        provider.events.append(("assemble", unit.index))
        return result

    def persist(cursor, value):
        provider.events.append(("persist", provider.current))
        return value

    monkeypatch.setattr(producer, "_assemble", assemble)
    monkeypatch.setattr(reference_module, "persist_advanced_options_reference_state_cursor", persist)
    return producer, provider, state, waits


def test_reference_off_is_one_serializable_transaction(monkeypatch):
    for value in (None, "OFF", " off "):
        producer, provider, state, _ = _run_reference(monkeypatch, value)
        assert producer.produce() == state
        assert provider.transactions == [(False, "SERIALIZABLE")]
        assert provider.events == [("assemble", 0), ("persist", 0)]


def test_reference_on_assembles_in_a_snapshot_and_writes_alone(monkeypatch):
    off_producer, *_ = _run_reference(monkeypatch, "OFF")
    off = off_producer.produce()
    producer, provider, state, _ = _run_reference(monkeypatch, "ON")
    assert producer.produce() == state == off
    assert provider.transactions == [(True, "REPEATABLE READ"), (False, "SERIALIZABLE")]
    assert provider.events == [("assemble", 0), ("persist", 1)]


def test_reference_on_blocked_never_opens_the_write(monkeypatch):
    producer, provider, _state, _ = _run_reference(monkeypatch, "ON", ready=False)
    with pytest.raises(AdvancedOptionsReferencePreflightBlocked):
        producer.produce()
    assert provider.transactions == [(True, "REPEATABLE READ")]
    assert provider.events == [("assemble", 0)]


def test_reference_on_retries_a_serialization_failure_of_the_write(monkeypatch):
    producer, provider, state, waits = _run_reference(monkeypatch, "ON", fail_write_once=True)
    assert producer.produce() == state
    assert provider.transactions == [(True, "REPEATABLE READ"), (False, "SERIALIZABLE"),
                                     (True, "REPEATABLE READ"), (False, "SERIALIZABLE")]
    assert waits == [0.01]


def _run_rolling(monkeypatch, value, *, ready=True, fail_write_once=False):
    _set(monkeypatch, value)
    provider = _Recorder(fail_write_once=fail_write_once)
    waits = []
    plan = _rolling_plan()
    producer = PostgresAdvancedOptionsRollingProducer(provider, plan, maximum_attempts=2,
                                                      retry_initial_seconds=0.01, wait=waits.append)
    state = rolling_state(cohort_id=plan.control_plane.cohort_id, journal_generation=plan.source_flow.journal_generation)
    result = AdvancedOptionsRollingProducerPreflight(
        status=RollingProducerStatus.READY if ready else RollingProducerStatus.BLOCKED,
        plan_id=plan.plan_id, preflight_sha256="a" * 64, evidence=(), state=state if ready else None)

    def assemble(unit):
        provider.events.append(("assemble", unit.index))
        return result

    def persist(cursor, value):
        provider.events.append(("persist", provider.current))
        return value

    monkeypatch.setattr(producer, "_assemble", assemble)
    monkeypatch.setattr(rolling_module, "persist_advanced_options_rolling_state_cursor", persist)
    return producer, provider, state, waits


def test_rolling_off_parity_and_on_shape(monkeypatch):
    producer, provider, state, _ = _run_rolling(monkeypatch, None)
    off = producer.produce()
    assert off == state and provider.transactions == [(False, "SERIALIZABLE")]
    assert provider.events == [("assemble", 0), ("persist", 0)]
    producer, provider, state, _ = _run_rolling(monkeypatch, "ON")
    assert producer.produce() == off
    assert provider.transactions == [(True, "REPEATABLE READ"), (False, "SERIALIZABLE")]
    assert provider.events == [("assemble", 0), ("persist", 1)]


def test_rolling_on_blocked_and_retry(monkeypatch):
    producer, provider, _state, _ = _run_rolling(monkeypatch, "ON", ready=False)
    with pytest.raises(AdvancedOptionsRollingPreflightBlocked):
        producer.produce()
    assert provider.transactions == [(True, "REPEATABLE READ")]
    producer, provider, state, waits = _run_rolling(monkeypatch, "ON", fail_write_once=True)
    assert producer.produce() == state and waits == [0.01] and len(provider.transactions) == 4


def test_rolling_on_keeps_a_diverged_checkpoint_a_diverged_retry(monkeypatch):
    from realtime.advanced_options_rolling_repository import AdvancedOptionsWindowRetryDiverged

    producer, provider, _state, _ = _run_rolling(monkeypatch, "ON")

    def diverged(cursor, value):
        raise AdvancedOptionsWindowRetryDiverged("persisted rolling checkpoint conflicts")

    monkeypatch.setattr(rolling_module, "persist_advanced_options_rolling_state_cursor", diverged)
    with pytest.raises(AdvancedOptionsWindowRetryDiverged):
        producer.produce()
    assert provider.transactions == [(True, "REPEATABLE READ"), (False, "SERIALIZABLE")]


def _run_marks(monkeypatch, value, *, blocked=False, fail_write_once=False):
    _set(monkeypatch, value)
    plan = _mark_plan()
    preflight = _mark_preflight(plan)
    frozen = freeze_advanced_options_mark_producer_plan(plan, preflight)
    provider = _Recorder(fail_write_once=fail_write_once)
    waits = []
    producer = PostgresAdvancedOptionsMarkProducer(provider, frozen, maximum_attempts=2,
                                                   retry_initial_seconds=0.01, wait=waits.append)
    marks = ("mark-a", "mark-b")
    result = SimpleNamespace(status=AdvancedOptionsMarkStatus.BLOCKED if blocked else AdvancedOptionsMarkStatus.READY,
                             marks=marks, reason_codes=("X",))

    def assemble(unit):
        provider.events.append(("assemble", unit.index))
        return result

    def persist(cursor, mark):
        provider.events.append(("persist", provider.current, mark))
        return f"stored:{mark}"

    monkeypatch.setattr(producer, "_assemble", assemble)
    monkeypatch.setattr(mark_module, "persist_realtime_quote_evidence_cursor", persist)
    return producer, provider, waits


def test_marks_off_parity_and_on_shape(monkeypatch):
    producer, provider, _ = _run_marks(monkeypatch, None)
    off = producer.produce()
    assert off == ("stored:mark-a", "stored:mark-b")
    assert provider.transactions == [(False, "SERIALIZABLE")]
    assert provider.events == [("assemble", 0), ("persist", 0, "mark-a"), ("persist", 0, "mark-b")]
    producer, provider, _ = _run_marks(monkeypatch, "ON")
    assert producer.produce() == off
    assert provider.transactions == [(True, "REPEATABLE READ"), (False, "SERIALIZABLE")]
    assert provider.events == [("assemble", 0), ("persist", 1, "mark-a"), ("persist", 1, "mark-b")]


def test_marks_on_blocked_and_retry(monkeypatch):
    producer, provider, _ = _run_marks(monkeypatch, "ON", blocked=True)
    with pytest.raises(AdvancedOptionsMarkPreflightBlocked):
        producer.produce()
    assert provider.transactions == [(True, "REPEATABLE READ")]
    producer, provider, waits = _run_marks(monkeypatch, "ON", fail_write_once=True)
    assert producer.produce() == ("stored:mark-a", "stored:mark-b") and waits == [0.01]


def test_native_load_reads_in_a_snapshot_only_when_on(monkeypatch):
    bundles = {}
    for value, isolation in ((None, "SERIALIZABLE"), ("OFF", "SERIALIZABLE"), ("ON", "REPEATABLE READ")):
        _set(monkeypatch, value)
        plan = _native_plan()
        preflight = _native_preflight(plan)
        frozen = freeze_advanced_options_native_input_plan(plan, preflight)
        provider = _Recorder()
        loader = PostgresAdvancedOptionsNativeInputLoader(provider, frozen)
        monkeypatch.setattr(loader, "_assemble", lambda _unit, preflight=preflight: preflight)
        bundles[value] = loader.load()
        assert provider.transactions == [(True, isolation)], value
    assert bundles[None] == bundles["OFF"] == bundles["ON"]


def test_preflights_are_unchanged_snapshots_in_both_modes(monkeypatch):
    for value in (None, "ON"):
        _set(monkeypatch, value)
        provider = _Recorder()
        producer = PostgresAdvancedOptionsReferenceProducer(provider, _reference_plan())
        monkeypatch.setattr(producer, "_assemble", lambda _unit: "preflight")
        assert producer.preflight() == "preflight"
        assert provider.transactions == [(True, "REPEATABLE READ")]
