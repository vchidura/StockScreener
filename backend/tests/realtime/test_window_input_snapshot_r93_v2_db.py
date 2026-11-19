"""G102 R93 (Nov 18 night) on the real schema: the reference producer's state is the same with
REALTIME_V3_WINDOW_INPUT_SNAPSHOT ON and OFF, and only OFF's assemble holds SIRead predicate locks.

One disposable database (dropped after). For two reference generations the producer runs in both modes on the same plan,
in both orders: the second run's persist compares its state with the row the first one stored and raises on any
difference ("persisted reference state conflicts"), so a pass means byte-identical state documents."""
from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

from option_flow_support import advanced_policy
from realtime.advanced_options_control_plane import persist_advanced_options_enrollment_cursor
from realtime.advanced_options_reference_producer import (
    PostgresAdvancedOptionsReferenceProducer,
    build_advanced_options_reference_producer_plan,
)
from realtime.advanced_options_window_snapshot import WINDOW_INPUT_SNAPSHOT_SETTING
from realtime.database_access_v2 import OWNER_POOL, V2AccessConfig, V2ConnectionProvider
from realtime.option_source_flow import persist_option_contract_scope_cursor, persist_option_flow_policy_cursor
from tests.realtime.test_advanced_options_reference_producer import _mixed_enrollment
from tests.realtime.test_advanced_options_reference_producer_v2_db import (
    _mixed_contract_scope,
    _persist_mixed_enrollment_inputs,
    _seed_reference_inputs,
)
from tests.realtime.test_option_slot_sets_v2_db import cursor
from tests.realtime.v2_database import disposable_v2_database, requires_v2_database

pytestmark = requires_v2_database


def _produce(provider, plan, monkeypatch, value, probes):
    if value is None:
        monkeypatch.delenv(WINDOW_INPUT_SNAPSHOT_SETTING, raising=False)
    else:
        monkeypatch.setenv(WINDOW_INPUT_SNAPSHOT_SETTING, value)
    producer = PostgresAdvancedOptionsReferenceProducer(provider, plan, retry_initial_seconds=0)
    original = producer._assemble

    def assemble(unit):
        result = original(unit)
        with unit.connection.cursor() as probe:
            probe.execute(
                "SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only'),"
                "(SELECT count(*) FROM pg_locks WHERE mode='SIReadLock' AND pid=pg_backend_pid())")
            probes.append((value, *probe.fetchone()))
        return result

    producer._assemble = assemble
    return producer.produce()


def test_reference_state_is_identical_on_and_off_and_on_holds_no_siread_locks(monkeypatch):
    with disposable_v2_database(f"test_realtime_v2_r93_{uuid4().hex[:8]}") as database_pool:
        flow_policy = advanced_policy()
        enrollment = _mixed_enrollment(flow_policy_sha256=flow_policy.sha256)
        scope = _mixed_contract_scope(enrollment, flow_policy)
        control = enrollment.control_plane

        def plan_for(generation):
            return build_advanced_options_reference_producer_plan(
                enrollment=enrollment, contract_scope=scope, reference_generation=generation,
                known_at=control.planned_at, valid_until=control.expires_at,
                discovery_scheduled_cycle=control.planned_at,
                prior_oi_session=control.session_date - timedelta(days=1), model_version="black-scholes-merton-v1",
            )

        first, second = plan_for(1), plan_for(2)
        with cursor(database_pool) as database_cursor:
            _persist_mixed_enrollment_inputs(database_cursor, enrollment)
            persist_advanced_options_enrollment_cursor(database_cursor, enrollment)
            persist_option_flow_policy_cursor(database_cursor, flow_policy)
            persist_option_contract_scope_cursor(database_cursor, scope)
            _seed_reference_inputs(database_cursor, enrollment, first, ticker="SPY")
            _seed_reference_inputs(database_cursor, enrollment, first, ticker="AMD", include_treasury=False)
        provider = V2ConnectionProvider({OWNER_POOL: database_pool}, V2AccessConfig(allow_disposable=True))
        probes = []

        on_first = _produce(provider, first, monkeypatch, "ON", probes)
        off_second_run = _produce(provider, first, monkeypatch, None, probes)  # compares with ON's stored row
        off_first = _produce(provider, second, monkeypatch, "OFF", probes)
        on_second_run = _produce(provider, second, monkeypatch, "ON", probes)  # compares with OFF's stored row

        assert on_first == off_second_run and on_first.state_sha256 == off_second_run.state_sha256
        assert off_first == on_second_run and off_first.state_sha256 == on_second_run.state_sha256
        assert on_first.open_interest and len(on_first.open_interest) == 3
        with cursor(database_pool) as database_cursor:
            database_cursor.execute(
                "SELECT reference_generation,count(*) FROM rt.advanced_options_reference_states "
                "GROUP BY reference_generation ORDER BY 1")
            assert [tuple(row) if not isinstance(row, dict) else tuple(row.values())
                    for row in database_cursor.fetchall()] == [(1, 1), (2, 1)]

        by_mode = {}
        for value, isolation, read_only, locks in probes:
            by_mode.setdefault(value, []).append((isolation, read_only, int(locks)))
        assert all(row == ("repeatable read", "on", 0) for row in by_mode["ON"]), by_mode
        off_rows = by_mode[None] + by_mode["OFF"]
        assert all(isolation == "serializable" and read_only == "off" for isolation, read_only, _ in off_rows)
        assert all(locks > 0 for _isolation, _read_only, locks in off_rows), off_rows
        print(f"R93_DB_PARITY probes={probes}")
