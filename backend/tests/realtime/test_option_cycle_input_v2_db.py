"""The cycle-input loader port against a real (disposable) V2 database: batches persisted through the existing
repository are re-resolved and verified by `option_cycle_input`/`option_discovery_verify`, and the HOT_TTL page
partition can be dropped without invalidating the permanent batch evidence."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest

from realtime.option_cycle_input import (
    build_option_cycle_input, load_verified_option_cycle_input, encode_option_cycle_input,
    verify_option_cycle_input_against_tables,
)
from realtime.option_discovery_repository import RealtimeOptionDiscoveryEvidence, persist_realtime_option_discovery_cursor
from realtime.option_discovery_verify import read_discovery_batches
from test_option_discovery_repository import OBSERVED_AT, PROFILE_ID, SECURITY_ID, _result, _scope
from v2_database import disposable_v2_database, requires_v2_database

pytestmark = requires_v2_database

DATABASE = "test_realtime_v2_oid_cycle"
PAGES = "rt.option_discovery_pages"
CONFIGURATION_SHA = "c" * 64
CODE_SHA = "e" * 64


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


def _seed(cursor, evidence):
    now = datetime(2026, 10, 5, 13, 0, tzinfo=timezone.utc)
    scope = evidence.scope
    cursor.execute(
        "INSERT INTO rt.capability_profiles (profile_id, config_sha256, schema_sha256, account_alias, "
        "capabilities_sha256, universe_sha256, checked_at, expires_at, status, profile_sha256, "
        "provider_tier, security_policy_sha256, preflight_mode, provider_requests, action_authorized) "
        "VALUES (%s,%s,%s,'fixture',%s,%s,%s,%s,'PASS',%s,%s,%s,'PASSIVE',false,false)",
        (str(PROFILE_ID), _digest(), _digest(), _digest(), scope.universe.sha256, now, now + timedelta(days=30),
         scope.profile_sha256, scope.provider_tier.value, _digest()))
    cursor.execute(
        "INSERT INTO ref.securities (security_id, asset_type, source_origin, created_at) VALUES (%s, 'STOCK', 'fixture', %s)",
        (str(SECURITY_ID), now - timedelta(days=40)))
    cursor.execute(
        "INSERT INTO ref.security_revisions (revision_id, security_id, ticker, calendar_id, effective_from, "
        "observed_at, payload_sha256) VALUES (%s, %s, 'AAPL', 'XNYS', %s, %s, %s)",
        (str(uuid4()), str(SECURITY_ID), now - timedelta(days=30), now - timedelta(days=30), _digest()))
    cursor.execute("SELECT rt.ensure_partitions(%s, %s)", (PAGES, date(2026, 10, 5)))


def test_a_cycle_input_built_from_evidence_verifies_against_the_stored_rows_and_survives_page_pruning(pool):
    evidence = RealtimeOptionDiscoveryEvidence(_scope(), SECURITY_ID, _result())
    built = build_option_cycle_input(
        [evidence], configuration_sha256=CONFIGURATION_SHA, code_sha256=CODE_SHA, cohort_id=uuid4())
    text, digest = encode_option_cycle_input(built)
    with _cursor(pool) as cursor:
        _seed(cursor, evidence)
        persist_realtime_option_discovery_cursor(cursor, evidence)
    with _cursor(pool) as cursor:
        result = verify_option_cycle_input_against_tables(cursor, built)
        assert (result.batches_verified, result.batches_with_pages, result.batches_pages_pruned) == (1, 1, 0)
        loaded, again = load_verified_option_cycle_input(cursor, text, digest, require_pages=True)
        assert loaded == built and again == result
        stored = read_discovery_batches(cursor, [evidence.result.batch.batch_id])[evidence.result.batch.batch_id]
        assert stored.evidence_sha256 == evidence.evidence_sha256 and len(stored.coverage) == 8
        assert [page.page_number for page in stored.pages] == [1, 2]
        assert sum(page.response_bytes for page in stored.pages) == evidence.response_bytes
        entry = built.underlyings[0]
        for changed in (replace(entry, discovery_evidence_sha256="0" * 64), replace(entry, row_count=entry.row_count + 1),
                        replace(entry, coverage_sha256="1" * 64), replace(entry, payload_chain_sha256="2" * 64)):
            with pytest.raises(ValueError):
                verify_option_cycle_input_against_tables(cursor, replace(built, underlyings=(changed,)))
        with pytest.raises(ValueError, match="is not stored"):
            verify_option_cycle_input_against_tables(cursor, replace(built, underlyings=(
                replace(entry, discovery_batch_id=uuid4()),)))
    with _cursor(pool) as cursor:
        cursor.execute(
            "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
            "WHERE i.inhparent = %s::regclass AND pg_get_expr(c.relpartbound, c.oid) LIKE %s",
            (PAGES, f"FOR VALUES FROM ('{OBSERVED_AT.date().isoformat()}%"))
        partition = cursor.fetchone()[0]
        cursor.execute(f"ALTER TABLE {PAGES} DETACH PARTITION rt.{partition}")
        cursor.execute(f"DROP TABLE rt.{partition}")
    with _cursor(pool) as cursor:
        result = verify_option_cycle_input_against_tables(cursor, built)
        assert (result.batches_verified, result.batches_with_pages, result.batches_pages_pruned) == (1, 0, 1)
        with pytest.raises(ValueError, match="required but no longer stored"):
            verify_option_cycle_input_against_tables(cursor, built, require_pages=True)
