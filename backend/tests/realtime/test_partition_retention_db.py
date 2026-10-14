"""DB-backed checks of the ordinary-retention partition executor on disposable V2 databases.

Opt in with STOCKS_V2_DB_TESTS=1 and run WITHOUT the offline I/O guard plugin. Each scenario uses a fixed
as_of date (never later than today) so the expectations do not depend on the day the test runs, and its own
relation, so the order of the tests only matters where stated. The databases are always dropped.
"""
from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import psycopg2
import pytest

from realtime.partition_retention import (
    ELIGIBLE_RELATIONS,
    PartitionRetentionExecutor,
    PostgresPartitionRetentionRepository,
    retention_cutoff,
)
from scripts import run_realtime_v2_retention as script
from scripts.validate_realtime_v2_schema import _connect, _settings
from v2_database import disposable_v2_database, insert_cohort, requires_v2_database

pytestmark = requires_v2_database

DATABASE = "test_realtime_v2_ret_db"
FULL_DATABASE = "test_realtime_v2_ret_full"
UTC = timezone.utc
NOT_ORDINARY = ("rt.bar_revisions", "rt.portal_snapshot_kept", "rt.option_flow_events", "rt.option_flow_clusters")


@pytest.fixture(scope="module")
def pool():
    with disposable_v2_database(DATABASE) as database_pool:
        yield database_pool


def connect_to(database):
    """A connection factory: credentials are loaded per call and never held by a test."""
    return lambda: _connect(_settings(), database)


def executor(database=DATABASE) -> PartitionRetentionExecutor:
    return PartitionRetentionExecutor(PostgresPartitionRetentionRepository(connect_to(database)))


@contextmanager
def _cursor(pool):
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            yield cursor
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        pool.putconn(connection)


def _digest() -> str:
    return uuid4().hex + uuid4().hex


def _at(year, month, day, hour=12) -> datetime:
    return datetime(year, month, day, hour, tzinfo=UTC)


def _attached(pool, relation) -> set[str]:
    with _cursor(pool) as cursor:
        cursor.execute(
            "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
            "WHERE i.inhparent = %s::regclass", (relation,))
        return {row[0] for row in cursor.fetchall()}


def _ensure(pool, relation, day) -> None:
    with _cursor(pool) as cursor:
        cursor.execute("SELECT rt.ensure_partitions(%s, %s)", (relation, day))


def _count(pool, statement, parameters=()) -> int:
    with _cursor(pool) as cursor:
        cursor.execute(statement, parameters)
        return cursor.fetchone()[0]


def _result(report, relation):
    return {item.relation: item for item in report.policies}[relation]


def _dropped(report, relation) -> set[str]:
    return {item.partition for item in _result(report, relation).dropped}


def _new_cohort(pool) -> str:
    with _cursor(pool) as cursor:
        return str(insert_cohort(cursor, datetime.now(UTC)))


# ---- option_discovery_pages: daily HOT_TTL, the batch evidence stays ---------------------------------------


def _seed_batch(pool) -> str:
    batch, security, now = str(uuid4()), str(uuid4()), datetime.now(UTC).replace(microsecond=0)
    with _cursor(pool) as cursor:
        cohort = insert_cohort(cursor, now)
        cursor.execute(
            "SELECT m.profile_id FROM rt.cohorts c JOIN rt.subscription_manifests m USING (manifest_id) "
            "WHERE c.cohort_id=%s", (str(cohort),))
        profile = cursor.fetchone()[0]
        cursor.execute(
            "INSERT INTO ref.securities (security_id, asset_type, source_origin, created_at) "
            "VALUES (%s, 'STOCK', 'fixture', %s)", (security, now))
        cursor.execute(
            "INSERT INTO rt.option_discovery_batches (discovery_batch_id, underlying_security_id, profile_id, "
            "profile_sha256, universe_sha256, provider_tier, provider, underlyer, scheduled_cycle, "
            "expiration_from, expiration_through, maximum_dte, spot_price, strike_corridor_fraction, "
            "strike_min, strike_max, request_filter_sha256, policy_sha256, started_at, completed_at, "
            "page_count, row_count, response_bytes, evidence_sha256) VALUES "
            "(%s,%s,%s,%s,%s,'MASSIVE_DEVELOPER','massive','SPY',%s,%s,%s,30,%s,%s,%s,%s,%s,%s,%s,%s,1,1,64,%s)",
            (batch, security, str(profile), _digest(), _digest(), now, now.date(), now.date() + timedelta(days=30),
             Decimal("100"), Decimal("0.1"), Decimal("90"), Decimal("110"), _digest(), _digest(), now, now,
             _digest()))
    return batch


def _insert_page(cursor, batch, page_number, received_at) -> None:
    cursor.execute(
        "INSERT INTO rt.option_discovery_pages (discovery_batch_id, page_number, row_count, response_bytes, "
        "payload_sha256, received_at, terminal, request_filter_sha256, request_metadata) "
        "VALUES (%s, %s, 1, %s, %s, %s, true, %s, '{}'::jsonb)",
        (batch, page_number, psycopg2.Binary(b"{}"), _digest()[:64], received_at, _digest()[:64]))


def test_expired_daily_page_partitions_are_dropped_at_the_boundary_and_the_batch_evidence_stays(pool):
    relation, as_of = "rt.option_discovery_pages", date(2026, 6, 15)  # 14 days: cutoff 2026-06-01
    _ensure(pool, relation, date(2026, 4, 20))
    _ensure(pool, relation, date(2026, 5, 20))
    batch = _seed_batch(pool)
    stamps = {1: _at(2026, 4, 25), 2: _at(2026, 5, 31), 3: _at(2026, 6, 1), 4: _at(2026, 6, 2)}
    with _cursor(pool) as cursor:
        for number, stamp in stamps.items():
            _insert_page(cursor, batch, number, stamp)
    before = _attached(pool, relation)
    assert {"option_discovery_pages_p20260531", "option_discovery_pages_p20260601"} <= before

    dry = executor().run(as_of_date=as_of, relations=[relation])
    assert dry.ok and not dry.dropped and _attached(pool, relation) == before
    planned = {item.partition for item in _result(dry, relation).would_drop}
    assert "option_discovery_pages_p20260531" in planned and "option_discovery_pages_p20260601" not in planned

    report = executor().run(execute=True, as_of_date=as_of, relations=[relation])
    assert report.ok and not report.failed
    dropped = _dropped(report, relation)
    assert dropped == planned and len(dropped) == 33
    remaining = _attached(pool, relation)
    assert remaining == before - dropped
    assert {"option_discovery_pages_p20260601", "option_discovery_pages_p20260602"} <= remaining
    with _cursor(pool) as cursor:
        cursor.execute("SELECT page_number FROM rt.option_discovery_pages WHERE discovery_batch_id = %s "
                       "ORDER BY page_number", (batch,))
        assert [row[0] for row in cursor.fetchall()] == [3, 4]
        cursor.execute("SELECT page_count, response_bytes, evidence_sha256 FROM rt.option_discovery_batches "
                       "WHERE discovery_batch_id = %s", (batch,))
        row = cursor.fetchone()
    assert row[0] == 1 and row[1] == 64 and len(row[2].strip()) == 64
    assert _count(pool, "SELECT count(*) FROM pg_inherits WHERE inhparent = %s::regclass AND inhdetachpending",
                  (relation,)) == 0

    again = executor().run(execute=True, as_of_date=as_of, relations=[relation])
    assert again.ok and not again.dropped and _attached(pool, relation) == remaining


# ---- a KEPT_3M monthly relation ----------------------------------------------------------------------------


def _diagnostic(cursor, cohort, created_at) -> None:
    cursor.execute(
        "INSERT INTO rt.normalization_diagnostics (diagnostic_id, cohort_id, underlyer, bar_count, pages_fetched, "
        "pagination_complete, bar_acquisition, price_basis, raw_rows, usable_mark_count, candidates, created_at) "
        "VALUES (%s, %s, 'SPY', 1, 1, true, 'FETCHED', 'NOT_APPLICABLE', 1, 1, 1, %s)",
        (str(uuid4()), cohort, created_at))


def test_monthly_partitions_are_dropped_only_when_wholly_older_than_three_months(pool):
    relation = "rt.normalization_diagnostics"
    cohort = _new_cohort(pool)
    _ensure(pool, relation, date(2026, 1, 20))
    _ensure(pool, relation, date(2026, 3, 15))
    with _cursor(pool) as cursor:
        for stamp in (_at(2026, 1, 10), _at(2026, 2, 10), _at(2026, 3, 20)):
            _diagnostic(cursor, cohort, stamp)
    # 2026-06-15: cutoff 2026-03-15. January and February end before it; March straddles it and stays whole.
    report = executor().run(execute=True, as_of_date=date(2026, 6, 15), relations=[relation])
    result = _result(report, relation)
    assert report.ok and result.cutoff == date(2026, 3, 15)
    assert {"normalization_diagnostics_p20260101", "normalization_diagnostics_p20260201",
            "normalization_diagnostics_p20251201"} <= {item.partition for item in result.dropped}
    march = [item for item in result.kept if item.partition == "normalization_diagnostics_p20260301"]
    assert march and march[0].reason == "STRADDLES_CUTOFF"
    assert _count(pool, "SELECT count(*) FROM rt.normalization_diagnostics WHERE cohort_id = %s", (cohort,)) == 1

    # 2026-07-01: cutoff 2026-04-01 equals March's upper bound, so the whole partition is now expired.
    boundary = executor().run(execute=True, as_of_date=date(2026, 7, 1), relations=[relation])
    assert boundary.ok and "normalization_diagnostics_p20260301" in _dropped(boundary, relation)
    assert _count(pool, "SELECT count(*) FROM rt.normalization_diagnostics WHERE cohort_id = %s", (cohort,)) == 0


# ---- evidence bodies and their catalog ---------------------------------------------------------------------


def _evidence_world(pool):
    now = datetime.now(UTC)
    cohort = _new_cohort(pool)
    security = str(uuid4())
    with _cursor(pool) as cursor:
        cursor.execute("INSERT INTO ref.securities (security_id, asset_type, source_origin, created_at) "
                       "VALUES (%s, 'STOCK', 'fixture', %s)", (security, now))
        cursor.execute("INSERT INTO rt.instruments (asset_class, security_id) VALUES ('EQUITY', %s) "
                       "RETURNING instrument_id", (security,))
        instrument = cursor.fetchone()[0]
    return cohort, security, instrument


def _catalog_row(cursor, world, durable_at) -> str:
    cohort, security, instrument = world
    evidence = str(uuid4())
    cursor.execute(
        "INSERT INTO rt.evidence_catalog (cohort_id, evidence_id, evidence_key, provider_tier, evidence_type, "
        "interval, security_id, instrument_id, market_time, durable_at) "
        "VALUES (%s, %s, %s, 'MASSIVE_DEVELOPER', 'TRADE_SETUP', '1d', %s, %s, %s, %s)",
        (cohort, evidence, uuid4().hex, security, instrument, durable_at - timedelta(hours=1), durable_at))
    return evidence


def _input_row(cursor, world, evidence, durable_at) -> None:
    cursor.execute(
        "INSERT INTO rt.equity_evidence_inputs (cohort_id, evidence_id, input_role, input_order, bar_revision_id, "
        "provider_tier, input_sha256, durable_at) VALUES (%s, %s, 'PRIMARY_BAR', 0, %s, 'MASSIVE_DEVELOPER', %s, %s)",
        (world[0], evidence, str(uuid4()), _digest()[:64], durable_at))


def _current_row(cursor, world, evidence, durable_at) -> None:
    cohort, security, instrument = world
    cursor.execute(
        "INSERT INTO rt.equity_evidence_current (cohort_id, provider_tier, security_id, instrument_id, "
        "evidence_type, lifecycle_key, model_variant, interval, evidence_id, market_time, observed_at, "
        "payload_sha256, quality_state) VALUES (%s, 'MASSIVE_DEVELOPER', %s, %s, 'TRADE_SETUP', %s, 'v1', '1d', "
        "%s, %s, %s, %s, 'COMPLETE')",
        (cohort, security, instrument, uuid4().hex, evidence, durable_at, durable_at, _digest()[:64]))


def _catalog_ids(pool, world) -> set[str]:
    with _cursor(pool) as cursor:
        cursor.execute("SELECT evidence_id FROM rt.evidence_catalog WHERE cohort_id = %s", (world[0],))
        return {str(row[0]) for row in cursor.fetchall()}


def test_evidence_catalog_rows_are_pruned_with_their_partitions_unless_a_body_elsewhere_still_references_them(pool):
    relations = ["rt.equity_evidence", "rt.equity_evidence_inputs"]
    for relation in relations:
        _ensure(pool, relation, date(2026, 3, 15))
    world = _evidence_world(pool)
    feb, early_march, late_march = _at(2026, 2, 10), _at(2026, 3, 2), _at(2026, 3, 20)
    with _cursor(pool) as cursor:
        plain = _catalog_row(cursor, world, feb)                        # input in February: goes with the partition
        _input_row(cursor, world, plain, feb)
        _current_row(cursor, world, plain, feb)
        orphan = _catalog_row(cursor, world, _at(2026, 2, 14))          # no referencing row at all
        straddler = _catalog_row(cursor, world, _at(2026, 2, 12))       # its input is in the retained March partition
        _input_row(cursor, world, straddler, early_march)
        recent = _catalog_row(cursor, world, late_march)
    assert _catalog_ids(pool, world) == {plain, orphan, straddler, recent}

    report = executor().run(execute=True, as_of_date=date(2026, 6, 15), relations=relations)
    assert report.ok
    assert {"equity_evidence_inputs_p20260201", "equity_evidence_p20260201"} <= (
        _dropped(report, "rt.equity_evidence_inputs") | _dropped(report, "rt.equity_evidence"))
    assert "equity_evidence_inputs_p20260301" in {item.partition for item in
                                                  _result(report, "rt.equity_evidence_inputs").kept}
    prune = {item.catalog: item for item in report.catalogs}["rt.evidence_catalog"]
    assert prune.status == "PRUNED" and prune.rows_deleted == 2 and prune.before == date(2026, 3, 15)
    assert _catalog_ids(pool, world) == {straddler, recent}
    # The declared ON DELETE CASCADE of the current pointer is the only cascade, and it is the schema's.
    assert _count(pool, "SELECT count(*) FROM rt.equity_evidence_current WHERE cohort_id = %s", (world[0],)) == 0
    assert _count(pool, "SELECT count(*) FROM rt.equity_evidence_inputs WHERE cohort_id = %s", (world[0],)) == 1

    # Once March is whole-expired its input partition goes, nothing references the straddler, and both are pruned.
    later = executor().run(execute=True, as_of_date=date(2026, 7, 15), relations=relations)
    assert later.ok and "equity_evidence_inputs_p20260301" in _dropped(later, "rt.equity_evidence_inputs")
    assert {item.catalog: item for item in later.catalogs}["rt.evidence_catalog"].rows_deleted == 2
    assert _catalog_ids(pool, world) == set()


# ---- portal snapshots: hot bodies and the shared snapshot catalog ------------------------------------------


def _snapshot(cursor, cohort, generated_at, *, kept=False) -> str:
    snapshot = str(uuid4())
    cursor.execute(
        "INSERT INTO rt.snapshot_catalog (snapshot_id, cohort_id, provider_tier, snapshot_class, snapshot_type, "
        "snapshot_key, session_date, source_generation, manifest_sha256, payload_sha256, generated_at) "
        "VALUES (%s, %s, 'MASSIVE_DEVELOPER', %s, 'fixture', %s, %s, 1, %s, %s, %s)",
        (snapshot, cohort, "SESSION_CLOSE" if kept else "EPHEMERAL_PORTAL", uuid4().hex, generated_at.date(),
         _digest()[:64], _digest()[:64], generated_at))
    if kept:
        cursor.execute(
            "INSERT INTO rt.portal_snapshot_kept (snapshot_id, session_date, cohort_id, snapshot_type, snapshot_key, "
            "source_manifest, payload) VALUES (%s, %s, %s, 'fixture', 'k', '{}'::jsonb, '{}'::jsonb)",
            (snapshot, generated_at.date(), cohort))
    else:
        cursor.execute(
            "INSERT INTO rt.portal_snapshot_hot (snapshot_id, generated_at, cohort_id, snapshot_type, snapshot_key, "
            "source_manifest, payload) VALUES (%s, %s, %s, 'fixture', 'k', '{}'::jsonb, '{}'::jsonb)",
            (snapshot, generated_at, cohort))
    return snapshot


def _snapshot_state(pool, snapshot) -> tuple[int, int]:
    return (_count(pool, "SELECT count(*) FROM rt.snapshot_catalog WHERE snapshot_id = %s", (snapshot,)),
            _count(pool, "SELECT (SELECT count(*) FROM rt.portal_snapshot_hot WHERE snapshot_id = %s) + "
                         "(SELECT count(*) FROM rt.portal_snapshot_kept WHERE snapshot_id = %s)",
                   (snapshot, snapshot)))


def test_hot_snapshot_partitions_drop_at_three_days_and_only_ephemeral_catalog_rows_follow(pool):
    relation = "rt.portal_snapshot_hot"
    cohort = _new_cohort(pool)
    _ensure(pool, relation, date(2026, 6, 10))
    _ensure(pool, "rt.portal_snapshot_kept", date(2026, 6, 1))
    with _cursor(pool) as cursor:
        old = _snapshot(cursor, cohort, _at(2026, 6, 8))
        boundary = _snapshot(cursor, cohort, _at(2026, 6, 11))
        inside = _snapshot(cursor, cohort, _at(2026, 6, 12))
        session_close = _snapshot(cursor, cohort, _at(2026, 6, 1), kept=True)
    report = executor().run(execute=True, as_of_date=date(2026, 6, 15), relations=[relation])  # cutoff 2026-06-12
    assert report.ok and _result(report, relation).cutoff == date(2026, 6, 12)
    dropped = _dropped(report, relation)
    assert {"portal_snapshot_hot_p20260608", "portal_snapshot_hot_p20260611"} <= dropped
    assert "portal_snapshot_hot_p20260612" not in dropped
    assert _snapshot_state(pool, old) == (0, 0) and _snapshot_state(pool, boundary) == (0, 0)
    assert _snapshot_state(pool, inside) == (1, 1)
    assert _snapshot_state(pool, session_close) == (1, 1)
    prune = {item.catalog: item for item in report.catalogs}["rt.snapshot_catalog"]
    assert prune.status == "PRUNED" and prune.relations == (relation,) and prune.rows_deleted == 2


def test_a_hot_partition_that_another_table_references_fails_closed_and_is_dropped_once_released(pool):
    relation = "rt.portal_snapshot_hot"
    cohort = _new_cohort(pool)
    _ensure(pool, relation, date(2026, 8, 10))
    with _cursor(pool) as cursor:
        referenced = _snapshot(cursor, cohort, _at(2026, 8, 8))
        free = _snapshot(cursor, cohort, _at(2026, 8, 9))
        cursor.execute(
            "CREATE TABLE rt.zz_retention_probe (snapshot_id uuid NOT NULL, generated_at timestamptz NOT NULL, "
            "FOREIGN KEY (snapshot_id, generated_at) REFERENCES rt.portal_snapshot_hot (snapshot_id, generated_at))")
        cursor.execute("INSERT INTO rt.zz_retention_probe VALUES (%s, %s)", (referenced, _at(2026, 8, 8)))
    try:
        report = executor().run(execute=True, as_of_date=date(2026, 8, 15), relations=[relation])
        assert not report.ok
        assert [alarm.code for alarm in report.all_alarms] == ["RETENTION_PARTITION_REFERENCED"]
        assert report.all_alarms[0].relation == f"{relation}/portal_snapshot_hot_p20260808"
        result = _result(report, relation)
        assert [item.partition for item in result.failed] == ["portal_snapshot_hot_p20260808"]
        assert "portal_snapshot_hot_p20260809" in _dropped(report, relation)
        assert "portal_snapshot_hot_p20260808" in _attached(pool, relation)
        assert _count(pool, "SELECT count(*) FROM pg_inherits WHERE inhparent = %s::regclass AND inhdetachpending",
                      (relation,)) == 0
        assert _snapshot_state(pool, referenced) == (1, 1)    # body and catalog row are untouched
        assert _snapshot_state(pool, free) == (0, 0)
        assert _count(pool, "SELECT count(*) FROM rt.zz_retention_probe") == 1

        with _cursor(pool) as cursor:
            cursor.execute("DELETE FROM rt.zz_retention_probe")
        released = executor().run(execute=True, as_of_date=date(2026, 8, 15), relations=[relation])
        assert released.ok and "portal_snapshot_hot_p20260808" in _dropped(released, relation)
        assert _snapshot_state(pool, referenced) == (0, 0)
    finally:
        with _cursor(pool) as cursor:
            cursor.execute("DROP TABLE IF EXISTS rt.zz_retention_probe")


# ---- refusals, leftovers and idempotency ------------------------------------------------------------------


def test_a_policy_row_that_differs_from_the_registry_and_a_misaligned_partition_each_drop_nothing(pool):
    relation = "rt.raw_payload_chunks"
    _ensure(pool, relation, date(2026, 2, 10))
    before = _attached(pool, relation)
    with _cursor(pool) as cursor:
        cursor.execute("UPDATE rt.partition_policies SET window_value = 1 WHERE relation_name = %s", (relation,))
    try:
        report = executor().run(execute=True, as_of_date=date(2026, 6, 15), relations=[relation])
    finally:
        with _cursor(pool) as cursor:
            cursor.execute("UPDATE rt.partition_policies SET window_value = 14 WHERE relation_name = %s", (relation,))
    assert [alarm.code for alarm in report.all_alarms] == ["PARTITION_POLICY_UNSUPPORTED"]
    assert not report.policies and _attached(pool, relation) == before

    with _cursor(pool) as cursor:
        cursor.execute("CREATE TABLE rt.raw_payload_chunks_p20990101 PARTITION OF rt.raw_payload_chunks "
                       "FOR VALUES FROM ('2099-01-01 04:00:00+00') TO ('2099-01-02 04:00:00+00')")
    try:
        report = executor().run(execute=True, as_of_date=date(2026, 6, 15), relations=[relation])
        assert [alarm.code for alarm in report.all_alarms] == ["PARTITION_LOOKUP_FAILED"]
        assert not report.dropped and _attached(pool, relation) == before | {"raw_payload_chunks_p20990101"}
    finally:
        with _cursor(pool) as cursor:
            cursor.execute("DROP TABLE rt.raw_payload_chunks_p20990101")
    ok = executor().run(execute=True, as_of_date=date(2026, 6, 15), relations=[relation])
    assert ok.ok and len(_dropped(ok, relation)) == len([name for name in before if name < "raw_payload_chunks_p20260601"])


def _detach_only(database, relation, partition) -> None:
    connection = connect_to(database)()
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(f"ALTER TABLE {relation} DETACH PARTITION rt.{partition} CONCURRENTLY")
    finally:
        connection.close()


def test_a_table_left_detached_by_an_interrupted_run_is_finished_and_one_holding_newer_rows_is_kept(pool):
    relation = "rt.raw_payloads"
    _ensure(pool, relation, date(2026, 2, 10))
    _detach_only(DATABASE, relation, "raw_payloads_p20260207")
    assert "raw_payloads_p20260207" not in _attached(pool, relation)
    assert _count(pool, "SELECT count(*) FROM pg_class WHERE relname = 'raw_payloads_p20260207'") == 1
    with _cursor(pool) as cursor:
        cursor.execute("CREATE TABLE rt.raw_payloads_p20260101 (LIKE rt.raw_payloads INCLUDING DEFAULTS)")
        cursor.execute("INSERT INTO rt.raw_payloads_p20260101 (payload_id, arrived_at, content_sha256, byte_length, "
                       "chunk_count) VALUES (%s, %s, %s, 1, 1)", (str(uuid4()), _at(2026, 3, 1), _digest()[:64]))
        cursor.execute("CREATE TABLE rt.raw_payloads_p20251201 (unrelated integer)")
    try:
        dry = executor().run(as_of_date=date(2026, 6, 15), relations=[relation])
        assert {item.partition for item in _result(dry, relation).would_drop} >= {
            "raw_payloads_p20260207", "raw_payloads_p20260101"}
        assert "raw_payloads_p20251201" not in {item.partition for item in _result(dry, relation).partitions}
        assert _count(pool, "SELECT count(*) FROM pg_class WHERE relname = 'raw_payloads_p20260207'") == 1

        report = executor().run(execute=True, as_of_date=date(2026, 6, 15), relations=[relation])
        assert [alarm.code for alarm in report.all_alarms] == ["RETENTION_DETACHED_DROP_FAILED"]
        assert report.all_alarms[0].relation == f"{relation}/raw_payloads_p20260101"
        assert "raw_payloads_p20260207" in _dropped(report, relation)
        assert _count(pool, "SELECT count(*) FROM pg_class WHERE relname = 'raw_payloads_p20260207'") == 0
        assert _count(pool, "SELECT count(*) FROM rt.raw_payloads_p20260101") == 1
        assert _count(pool, "SELECT count(*) FROM pg_class WHERE relname = 'raw_payloads_p20251201'") == 1
        assert not _attached(pool, relation) & {f"raw_payloads_p202602{day:02d}" for day in range(7, 25)}
    finally:
        with _cursor(pool) as cursor:
            cursor.execute("DROP TABLE IF EXISTS rt.raw_payloads_p20260101, rt.raw_payloads_p20251201")


# ---- everything, end to end through the script ------------------------------------------------------------


def _listing(database):
    repository = PostgresPartitionRetentionRepository(connect_to(database))
    return {relation: {item.name: item for item in repository.partitions(relation)}
            for relation in (*ELIGIBLE_RELATIONS, *NOT_ORDINARY)}


def _script_json(capsys, *extra) -> tuple[int, dict]:
    code = script.main(["--database", FULL_DATABASE, "--confirm-database-name", FULL_DATABASE, *extra])
    output = capsys.readouterr().out
    return code, json.loads(output[:output.rindex("}") + 1])


def test_the_script_drops_every_eligible_relations_expired_partitions_and_leaves_every_other_class_alone(capsys):
    today = datetime.now(UTC).date()
    with disposable_v2_database(FULL_DATABASE) as full_pool:
        with _cursor(full_pool) as cursor:
            cursor.execute("SELECT relation_name, scheme FROM rt.partition_policies ORDER BY relation_name")
            policies = cursor.fetchall()
            for relation, scheme in policies:
                cursor.execute("SELECT rt.ensure_partitions(%s, %s)", (
                    relation, today - timedelta(days=60 if scheme == "DAILY" else 300)))
        before = _listing(FULL_DATABASE)
        assert all(before[relation] for relation in NOT_ORDINARY)

        code, dry = _script_json(capsys)
        assert code == 0 and dry["mode"] == "DRY_RUN" and dry["ok"] and dry["would_drop"] > 0
        assert dry["dropped"] == 0
        assert {name: set(items) for name, items in _listing(FULL_DATABASE).items()} == {
            name: set(items) for name, items in before.items()}
        assert {entry["relation"] for entry in dry["skipped"]} >= set(NOT_ORDINARY)

        code, run = _script_json(capsys, "--execute")
        assert code == 0 and run["mode"] == "EXECUTE" and run["ok"] and run["failed"] == 0
        assert run["dropped"] == dry["would_drop"] and not run["alarms"]
        assert all(item["status"] == "PRUNED" for item in run["catalogs"]) and len(run["catalogs"]) == 3

        after = _listing(FULL_DATABASE)
        for relation in NOT_ORDINARY:
            assert set(after[relation]) == set(before[relation]), relation
        with _cursor(full_pool) as cursor:
            cursor.execute("SELECT relation_name, window_kind, window_value FROM rt.partition_policies")
            windows = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}
        removed_total = 0
        for relation in ELIGIBLE_RELATIONS:
            cutoff = retention_cutoff(*windows[relation], today)
            removed = set(before[relation]) - set(after[relation])
            removed_total += len(removed)
            assert removed and all(before[relation][name].upper <= cutoff for name in removed), relation
            assert all(item.upper > cutoff for item in after[relation].values()), relation
        assert removed_total == run["dropped"]

        code, final = _script_json(capsys)
        assert code == 0 and final["would_drop"] == 0 and final["ok"]
