"""Measures the screening reader's first-load verification on nine names (124k bars) in a disposable database.

Opt in with STOCKS_V2_DB_TESTS=1 and STOCKS_V2_DB_MEASURE=1 and run WITHOUT the offline I/O guard plugin:
    STOCKS_V2_DB_TESTS=1 STOCKS_V2_DB_MEASURE=1 pytest backend/tests/realtime/test_screening_reference_v2_db.py -s
The nine-name fixture's bars (about 124k rows) go through `persist_realtime_equity_bars_cursor` into the real
V2 tables of test_realtime_v2_ref_screening9 (always dropped), the ticker projection's own screening generation
is persisted by `persist_screening_generation_cursor`, and the reader loads it: the first load recomputes
every member's lineage heads from `rt.bar_current`, a repeated load of the generation does not. Credentials
stay inside tests/realtime/v2_database.py: nothing here receives, stores or prints them.
"""
from __future__ import annotations

import os
import statistics
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from time import perf_counter
from uuid import NAMESPACE_URL, uuid5

import pytest

from realtime.equity_bar_repository import persist_realtime_equity_bars_cursor
from realtime.portal_compatibility_repository import RealtimeScreeningRepository
from realtime.portal_projection_repository import persist_screening_generation_cursor
from realtime.ticker_projection import _fact, project_realtime_ticker_workspace
from tests.realtime.test_ticker_reference_v2_db import OBSERVED_FLOOR, _Pool
from tests.realtime.test_ticker_vertical_slice import ticker_fixture
from tests.realtime.v2_database import disposable_v2_database, insert_cohort, requires_v2_database

pytestmark = [
    requires_v2_database,
    pytest.mark.skipif(os.environ.get("STOCKS_V2_DB_MEASURE") != "1", reason="set STOCKS_V2_DB_MEASURE=1 to measure"),
]
DATABASE_NAME = "test_realtime_v2_ref_screening9"
UTC = timezone.utc


def test_nine_name_first_load_verification_cost():
    source = ticker_fixture()
    now = datetime.now(UTC)
    with disposable_v2_database(DATABASE_NAME) as connection_pool:
        pool = _Pool(connection_pool)
        instruments = {}
        with pool.transaction() as cursor:
            for member in source.members:
                cursor.execute(
                    "INSERT INTO ref.securities (security_id, asset_type, source_origin, created_at) "
                    "VALUES (%s, 'STOCK', 'fixture', %s)",
                    (str(member["security_id"]), member["observed_at"] - timedelta(days=600)))
                cursor.execute(
                    "INSERT INTO ref.security_revisions (revision_id, security_id, ticker, calendar_id, effective_from, "
                    "observed_at, payload_sha256) VALUES (%s, %s, %s, 'XNYS', %s, %s, %s)",
                    (str(member["security_revision_id"]), str(member["security_id"]), member["ticker"],
                     member["effective_from"], member["observed_at"], member["payload_sha256"]))
                cursor.execute("INSERT INTO rt.instruments (asset_class, security_id) VALUES ('EQUITY', %s) "
                               "RETURNING instrument_id", (str(member["security_id"]),))
                instruments[member["ticker"]] = cursor.fetchone()["instrument_id"]
            cohort = insert_cohort(cursor, now)
            cursor.execute(
                "SELECT manifest.manifest_sha256 FROM rt.cohorts AS cohort JOIN rt.subscription_manifests AS manifest "
                "ON manifest.manifest_id=cohort.manifest_id WHERE cohort.cohort_id=%s", (str(cohort),))
            manifest_sha256 = cursor.fetchone()["manifest_sha256"]
        remap = {row["bar_revision_id"]: uuid5(NAMESPACE_URL, f"{cohort}:{row['bar_revision_id']}") for row in source.bars}
        bars = tuple(
            dict(row, cohort_id=cohort, instrument_id=instruments[row["ticker"]],
                 bar_revision_id=remap[row["bar_revision_id"]],
                 source_revision_ids=tuple(remap[value] for value in row["source_revision_ids"]),
                 observed_at=max(row["observed_at"], OBSERVED_FLOOR))
            for row in source.bars)
        inputs = replace(
            source, cohort_id=cohort, bars=bars, source_manifest_sha256=manifest_sha256,
            members=tuple(dict(member, instrument_id=instruments[member["ticker"]]) for member in source.members))
        started = perf_counter()
        facts = [_fact(row, inputs) for row in bars]
        with pool.plain() as cursor:
            for offset in range(0, len(facts), 1000):
                persist_realtime_equity_bars_cursor(cursor, facts[offset:offset + 1000])
            cursor.execute("ANALYZE rt.bar_current")
            cursor.execute("ANALYZE rt.bar_revisions")
        print(f"\nloaded {len(bars)} bars in {perf_counter() - started:.1f} s")
        generation = project_realtime_ticker_workspace(inputs).screening_generation
        with pool.plain() as cursor:
            persist_screening_generation_cursor(cursor, generation)
        with pool.transaction() as cursor:
            cursor.execute(
                "SELECT octet_length(generation.manifest::text) AS manifest_bytes, "
                "(SELECT sum(octet_length(stored.payload::text)) FROM rt.screening_rows AS stored "
                "WHERE stored.generation_id=generation.generation_id) AS rows_bytes, "
                "(SELECT count(*) FROM rt.bar_current WHERE cohort_id=%s) AS current_bars "
                "FROM rt.screening_generations AS generation WHERE generation.cohort_id=%s",
                (str(cohort), str(cohort)))
            sizes = dict(cursor.fetchone())
        print(f"stored: {sizes}")

        def first_load():
            repository = RealtimeScreeningRepository(pool.transaction)
            with pool.transaction() as cursor:
                started = perf_counter()
                repository._load(cursor, str(cohort), now + timedelta(days=1))
                return perf_counter() - started, repository

        loads = [first_load() for _ in range(3)]
        first = [seconds for seconds, _ in loads]
        repository = loads[0][1]
        with pool.transaction() as cursor:
            started = perf_counter()
            repository._load(cursor, str(cohort), now + timedelta(days=1))
            cached = perf_counter() - started
            started = perf_counter()
            RealtimeScreeningRepository(pool.transaction, bar_loader=None)._load(cursor, str(cohort), now + timedelta(days=1))
            stored_checks = perf_counter() - started
        print(f"first load {[round(value, 3) for value in first]} s (median {statistics.median(first):.3f}); "
              f"cached {cached:.3f} s; stored-identity checks alone {stored_checks:.3f} s")
        captured = []

        class Recorder:
            def __init__(self, cursor):
                self._cursor = cursor

            def __getattr__(self, name):
                return getattr(self._cursor, name)

            def execute(self, statement, parameters=None):
                captured.append((statement, parameters))
                return self._cursor.execute(statement, parameters)

        probe = RealtimeScreeningRepository(pool.transaction)
        real_loader = probe._bar_loader
        probe._bar_loader = lambda cursor, **options: real_loader(Recorder(cursor), **options)
        with pool.transaction() as cursor:
            probe._load(cursor, str(cohort), now + timedelta(days=1))
            statement, parameters = next(item for item in captured if item[0].startswith("SELECT target.security_id"))
            cursor.execute("EXPLAIN (ANALYZE, BUFFERS) " + statement, parameters)
            print("verification statement plan:\n" + "\n".join(row["QUERY PLAN"] for row in cursor.fetchall()))
        assert statistics.median(first) < 5.0 and cached < statistics.median(first)
