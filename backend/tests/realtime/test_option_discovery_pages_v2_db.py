"""rt.option_discovery_pages is daily-partitioned HOT_TTL replay material under permanent batch evidence."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import psycopg2
import pytest

from v2_database import disposable_v2_database, insert_cohort, requires_v2_database

pytestmark = requires_v2_database

DATABASE = "test_realtime_v2_d2"
UTC = timezone.utc
PAGES = "rt.option_discovery_pages"

INSERT_PAGE = (
    "INSERT INTO rt.option_discovery_pages (discovery_batch_id, page_number, row_count, response_bytes, "
    "payload_sha256, received_at, terminal, request_filter_sha256, request_metadata) "
    "VALUES (%s, %s, 1, %s, %s, %s, true, %s, '{}'::jsonb) "
    "ON CONFLICT (discovery_batch_id, page_number, received_at) DO NOTHING"
)


@pytest.fixture(scope="module")
def pool():
    with disposable_v2_database(DATABASE) as database_pool:
        yield database_pool


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


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _seed_batch(pool) -> str:
    batch, security, now = str(uuid4()), str(uuid4()), _now()
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


def _ensure_partitions(pool, today) -> None:
    with _cursor(pool) as cursor:
        cursor.execute("SELECT rt.ensure_partitions(%s, %s)", (PAGES, today))


def _insert_page(cursor, batch: str, page_number: int, received_at: datetime, body: bytes = b"{}") -> int:
    cursor.execute(INSERT_PAGE, (batch, page_number, psycopg2.Binary(body), _digest()[:64], received_at, _digest()[:64]))
    return cursor.rowcount


def test_pages_are_a_daily_hot_ttl_partitioned_relation_under_the_batch_evidence(pool):
    with _cursor(pool) as cursor:
        cursor.execute("SELECT relkind, pg_get_partkeydef(oid) FROM pg_class WHERE oid = %s::regclass", (PAGES,))
        assert cursor.fetchone() == ("p", "RANGE (received_at)")
        cursor.execute(
            "SELECT array_agg(a.attname ORDER BY k.ord) FROM pg_index i "
            "CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) "
            "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum "
            "WHERE i.indrelid = %s::regclass AND i.indisprimary", (PAGES,))
        assert cursor.fetchone()[0] == ["discovery_batch_id", "page_number", "received_at"]
        cursor.execute(
            "SELECT tgname FROM pg_trigger WHERE tgrelid = %s::regclass AND tgparentid = 0 AND NOT tgisinternal "
            "ORDER BY tgname", (PAGES,))
        assert [row[0] for row in cursor.fetchall()] == [
            "trg_rt_option_discovery_pages_immutable", "trg_rt_option_discovery_pages_no_truncate"]
        cursor.execute(
            "SELECT confrelid::regclass::text FROM pg_constraint WHERE conrelid = %s::regclass AND contype = 'f'",
            (PAGES,))
        assert [row[0] for row in cursor.fetchall()] == ["rt.option_discovery_batches"]
        cursor.execute(
            "SELECT scheme, key_column, retention_class, window_kind, window_value, requires_archive "
            "FROM rt.partition_policies WHERE relation_name = %s", (PAGES,))
        assert cursor.fetchone() == ("DAILY", "received_at", "HOT_TTL", "CALENDAR_DAYS", 14, False)
        cursor.execute(
            "SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
            "WHERE i.inhparent = %s::regclass AND pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT'", (PAGES,))
        assert cursor.fetchone()[0] == 0


def test_a_page_routes_to_its_daily_partition_and_a_replay_is_not_duplicated(pool):
    now = _now()
    _ensure_partitions(pool, now.date())
    batch = _seed_batch(pool)
    with _cursor(pool) as cursor:
        assert _insert_page(cursor, batch, 1, now) == 1
        assert _insert_page(cursor, batch, 1, now) == 0
        cursor.execute(
            "SELECT pg_get_expr(c.relpartbound, c.oid) FROM rt.option_discovery_pages p "
            "JOIN pg_class c ON c.oid = p.tableoid WHERE p.discovery_batch_id = %s", (batch,))
        bounds = [row[0] for row in cursor.fetchall()]
    assert len(bounds) == 1 and now.date().isoformat() in bounds[0]
    # The same page with a different received_at is a second row. The repository read-back sees it as
    # extra rows and fails closed instead of accepting a divergent replay.
    with _cursor(pool) as cursor:
        assert _insert_page(cursor, batch, 1, now + timedelta(seconds=1)) == 1
        cursor.execute("SELECT count(*) FROM rt.option_discovery_pages WHERE discovery_batch_id = %s", (batch,))
        assert cursor.fetchone()[0] == 2


def test_the_database_rejects_edits_truncation_orphans_and_unpartitioned_pages(pool):
    now = _now()
    _ensure_partitions(pool, now.date())
    batch = _seed_batch(pool)
    with _cursor(pool) as cursor:
        _insert_page(cursor, batch, 1, now)

    def rejected(error, statement, parameters=()):
        with pytest.raises(error):
            with _cursor(pool) as cursor:
                cursor.execute(statement, parameters)

    rejected(psycopg2.errors.RaiseException, "UPDATE rt.option_discovery_pages SET row_count = 2")
    rejected(psycopg2.errors.RaiseException, "DELETE FROM rt.option_discovery_pages")
    rejected(psycopg2.errors.RaiseException, "TRUNCATE rt.option_discovery_pages")
    with pytest.raises(psycopg2.errors.ForeignKeyViolation):
        with _cursor(pool) as cursor:
            _insert_page(cursor, str(uuid4()), 1, now)
    with pytest.raises(psycopg2.errors.CheckViolation):
        with _cursor(pool) as cursor:
            _insert_page(cursor, batch, 41, now)
    with pytest.raises(psycopg2.errors.CheckViolation):
        with _cursor(pool) as cursor:
            _insert_page(cursor, batch, 2, now - timedelta(days=400))


def test_dropping_an_expired_daily_partition_removes_page_bodies_and_keeps_the_batch_evidence(pool):
    now = _now()
    old = now - timedelta(days=30)
    _ensure_partitions(pool, old.date())
    batch = _seed_batch(pool)
    with _cursor(pool) as cursor:
        assert _insert_page(cursor, batch, 1, old, body=b"x" * 4096) == 1
        cursor.execute(
            "SELECT c.relname FROM rt.option_discovery_pages p JOIN pg_class c ON c.oid = p.tableoid "
            "WHERE p.discovery_batch_id = %s", (batch,))
        partition = cursor.fetchone()[0]
        cursor.execute(f"ALTER TABLE rt.option_discovery_pages DETACH PARTITION rt.{partition}")
        cursor.execute(f"DROP TABLE rt.{partition}")
    with _cursor(pool) as cursor:
        cursor.execute("SELECT count(*) FROM rt.option_discovery_pages WHERE discovery_batch_id = %s", (batch,))
        assert cursor.fetchone()[0] == 0
        cursor.execute(
            "SELECT evidence_sha256, response_bytes FROM rt.option_discovery_batches WHERE discovery_batch_id = %s",
            (batch,))
        row = cursor.fetchone()
    assert row is not None and len(row[0].strip()) == 64 and row[1] == 64
