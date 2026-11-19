"""G102 R99 (Nov 18): the close-hour window sweep's SQL on a disposable V2 database (opt-in: STOCKS_V2_DB_TESTS=1)."""
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from uuid import uuid4

import pytest

from realtime import window_sweep as ws
from v2_database import disposable_v2_database, insert_cohort, requires_v2_database

pytestmark = requires_v2_database
UTC = timezone.utc
NOW = datetime(2026, 11, 18, 22, 46, tzinfo=UTC)  # Wednesday 17:46 New York: Nov 18 is settled
TRIGGER = "trg_rt_advanced_options_v3_stage_effect_authorized"


@pytest.fixture(scope="module")
def pool():
    with disposable_v2_database(f"test_realtime_v2_night_o_r99_{uuid4().hex[:8]}") as value:
        yield value


def _plan(cursor, cohort, session, catalog):
    plan = uuid4()
    cursor.execute(
        "INSERT INTO rt.service_plans (plan_id, cohort_id, session_date, stage_catalog_version, enabled_stages, "
        "config_sha256, deadline, scope_reference) VALUES (%s,%s,%s,%s,%s,%s,%s,'r99 fixture')",
        (str(plan), str(cohort), session, catalog,
         ["OPTIONS_CYCLE"] if catalog == "stages_v5" else ["EQUITY_MATERIALIZE"], uuid4().hex + uuid4().hex,
         datetime.combine(session, time(23), UTC)))
    return plan


def _window(cursor, plan, cohort, key, cutoff, *, state="PENDING", lease=None):
    window = uuid4()
    owned = state == "RUNNING" or lease is not None
    cursor.execute(
        "INSERT INTO rt.window_runs (window_id, plan_id, cohort_id, provider_tier, window_key, release_at, cutoff, "
        "deadline, state, owner_id, owner_epoch, lease_until) VALUES (%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,%s,%s,%s,%s)",
        (str(window), str(plan), str(cohort), key, cutoff + timedelta(minutes=2), cutoff, cutoff + timedelta(minutes=7),
         state, str(uuid4()) if owned else None, 1 if owned else 0, lease))
    return str(window)


def _stage(cursor, window, stage, catalog, state, started):
    cursor.execute(
        "INSERT INTO rt.stage_runs (stage_run_id, window_id, stage_id, stage_version, attempt, state, started_at, "
        "finished_at, updated_at) VALUES (%s,%s,%s,%s,1,%s,%s,%s,%s)",
        (str(uuid4()), window, stage, catalog, state, started,
         None if state == "RUNNING" else started + timedelta(seconds=20), started))


def _seed(connection):
    ids = {}
    with connection.cursor() as cursor:
        cursor.execute(f"ALTER TABLE rt.stage_runs DISABLE TRIGGER {TRIGGER}")
        old = date(2026, 11, 13)
        cohort = insert_cohort(cursor, datetime(2026, 11, 13, 13, tzinfo=UTC))
        plan = _plan(cursor, cohort, old, "stages_v5")
        ids["pending"] = _window(cursor, plan, cohort, "E:2026-11-13T17:00:00+00:00",
                                 datetime(2026, 11, 13, 17, tzinfo=UTC))
        cutoff = datetime(2026, 11, 13, 17, 5, tzinfo=UTC)
        ids["dead"] = _window(cursor, plan, cohort, "E:2026-11-13T17:05:00+00:00", cutoff, state="RUNNING",
                              lease=cutoff + timedelta(minutes=6))
        _stage(cursor, ids["dead"], "OPTIONS_ACQUIRE", "stages_v5", "COMPLETE", cutoff + timedelta(minutes=2))
        _stage(cursor, ids["dead"], "OPTIONS_CYCLE", "stages_v5", "RUNNING", cutoff + timedelta(minutes=2))
        cutoff = datetime(2026, 11, 13, 17, 10, tzinfo=UTC)
        ids["live"] = _window(cursor, plan, cohort, "E:2026-11-13T17:10:00+00:00", cutoff, state="RUNNING",
                              lease=NOW + timedelta(minutes=5))
        ids["gap"] = _window(cursor, plan, cohort, "E:2026-11-13T17:15:00+00:00",
                             datetime(2026, 11, 13, 17, 15, tzinfo=UTC), state="RETAINED_GAP")
        ids["complete"] = _window(cursor, plan, cohort, "E:2026-11-13T17:20:00+00:00",
                                  datetime(2026, 11, 13, 17, 20, tzinfo=UTC), state="COMPLETE")
        equity = insert_cohort(cursor, datetime(2026, 11, 13, 13, tzinfo=UTC))
        eplan = _plan(cursor, equity, old, "stages_v3")
        cutoff = datetime(2026, 11, 13, 15, 15, tzinfo=UTC)
        ids["equity"] = _window(cursor, eplan, equity, "advanced-20261113T151500Z", cutoff, state="RUNNING",
                                lease=cutoff + timedelta(minutes=4))
        _stage(cursor, ids["equity"], "EQUITY_MATERIALIZE", "stages_v3", "RUNNING", cutoff + timedelta(seconds=40))
        today = date(2026, 11, 18)
        tcohort = insert_cohort(cursor, datetime(2026, 11, 18, 13, tzinfo=UTC))
        tplan = _plan(cursor, tcohort, today, "stages_v3")
        ids["r77"] = _window(cursor, tplan, tcohort, "advanced-20261118T210000Z", datetime(2026, 11, 18, 21, tzinfo=UTC))
        ids["today"] = _window(cursor, tplan, tcohort, "advanced-20261118T204500Z",
                               datetime(2026, 11, 18, 20, 45, tzinfo=UTC))
        ids["future"] = _window(cursor, tplan, tcohort, "advanced-20261118T224500Z", NOW - timedelta(minutes=1))
        cursor.execute(f"ALTER TABLE rt.stage_runs ENABLE TRIGGER {TRIGGER}")
    connection.commit()
    return ids


def _states(connection, ids):
    with connection.cursor() as cursor:
        cursor.execute("SELECT window_id::text, state, lease_until IS NULL FROM rt.window_runs WHERE window_id = ANY(%s::uuid[])",
                       (list(ids.values()),))
        by_id = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
        cursor.execute("SELECT w.window_id::text, s.stage_id, s.state, s.reason_codes, s.finished_at IS NOT NULL "
                       "FROM rt.stage_runs s JOIN rt.window_runs w USING (window_id) WHERE w.window_id = ANY(%s::uuid[]) "
                       "ORDER BY 1, 2", (list(ids.values()),))
        stages = cursor.fetchall()
    connection.rollback()
    return {name: by_id[window] for name, window in ids.items()}, stages


def _sweep(pool, mode, act_flag, at=NOW):
    reader = pool.getconn()
    writer = pool.getconn()
    lines, faults = [], []
    try:
        reader.set_session(readonly=True, isolation_level="REPEATABLE READ")

        @contextmanager
        def write_transaction():
            with writer:
                with writer.cursor() as cursor:
                    yield cursor

        with reader.cursor() as cursor:
            summary = ws.run_window_sweep(cursor, now=at, mode=mode, act_flag=act_flag,
                                          write_transaction=write_transaction,
                                          emit_line=lambda event, doc: lines.append((event, doc)),
                                          emit=lambda **kwargs: faults.append(kwargs))
        reader.rollback()
    finally:
        reader.set_session(readonly=False, isolation_level=None)
        pool.putconn(reader)
        pool.putconn(writer)
    return summary, lines, faults


def test_record_act_and_idempotence_on_the_real_tables(pool):
    connection = pool.getconn()
    try:
        ids = _seed(connection)
        before = _states(connection, ids)
        summary, lines, faults = _sweep(pool, "RECORD", False)
        assert _states(connection, ids) == before
        records = {doc["window_key"] for event, doc in lines if event == ws.WINDOW_SWEEP_RECORD_EVENT}
        assert records == {"E:2026-11-13T17:00:00+00:00", "E:2026-11-13T17:05:00+00:00",
                           "advanced-20261113T151500Z", "advanced-20261118T204500Z"}
        assert summary["skipped"] == {"LIVE_LEASE": 1, "R77_CLOSE_RECOVERY": 1}
        assert summary["eligible_by_kind"] == {"EQUITY": 2, "OPTIONS": 2}

        summary, _lines, _faults = _sweep(pool, "ACT", False)  # no --act: records only
        assert summary["act_refused"] is True and _states(connection, ids) == before

        summary, lines, faults = _sweep(pool, "ACT", True)
        assert summary["closed"] == 4 and summary["stage_runs_failed"] == 2 and summary["not_closed"] == {}
        states, stages = _states(connection, ids)
        assert {name: state for name, (state, _l) in states.items()} == {
            "pending": "MISSED", "dead": "MISSED", "equity": "MISSED", "today": "MISSED",
            "live": "RUNNING", "gap": "RETAINED_GAP", "complete": "COMPLETE", "r77": "PENDING", "future": "PENDING"}
        assert all(lease_cleared for name, (_s, lease_cleared) in states.items()
                   if name in ("pending", "dead", "equity", "today"))
        assert states["live"][1] is False
        by_stage = {(window, stage): (state, reasons, finished) for window, stage, state, reasons, finished in stages}
        assert by_stage[(ids["dead"], "OPTIONS_CYCLE")] == ("FAILED", ["OWNER_LOST"], True)
        assert by_stage[(ids["dead"], "OPTIONS_ACQUIRE")][0] == "COMPLETE"
        assert by_stage[(ids["equity"], "EQUITY_MATERIALIZE")] == ("FAILED", ["OWNER_LOST"], True)
        assert {f["severity"] for f in faults} == {"ACTED"}

        after = _states(connection, ids)
        summary, lines, _faults = _sweep(pool, "ACT", True)
        assert summary["eligible"] == 0 and summary["closed"] == 0 and _states(connection, ids) == after

        # After the next open R77's close window is released (and yesterday's leases have expired).
        summary, _lines, _faults = _sweep(pool, "RECORD", False, at=datetime(2026, 11, 19, 14, 31, tzinfo=UTC))
        assert summary["eligible"] == 3 and summary["skipped"] == {}  # r77, the expired lease, the late window
    finally:
        connection.rollback()
        pool.putconn(connection)
