"""G102 A4 on a disposable database: `rt.bar_head` follows `bar-selection-v1` across cohorts and tiers, a rerun reads
bars as they stood at its cutoff, and the backfill reaches the same head as the writer."""
from __future__ import annotations

from contextlib import closing
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from equity.domain import BarAvailabilityMode, BarSessionScope, BarSourceKind, EquityBarRevision
from realtime.equity_bar_adapter import RealtimeEquityBarFact, adapt_developer_equity_bar
from realtime.equity_bar_repository import (
    backfill_bar_head_cursor,
    load_bar_range_cursor,
    persist_realtime_equity_bars_cursor,
)
from realtime.preflight import ProviderTier
from tests.realtime.test_equity_interval_finalizer_v2_db import _seed_member
from v2_database import disposable_v2_database, insert_cohort, requires_v2_database

pytestmark = requires_v2_database
UTC = timezone.utc
START = datetime(2026, 11, 6, 15, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def pool():
    with disposable_v2_database("test_realtime_v2_bar_head") as pool:
        yield pool


def _revision(security_id, ticker, *, interval="5m", kind=BarSourceKind.NATIVE_REST, observed=60, close="101",
              adjusted=False, start=START):
    end = start + {"5m": timedelta(minutes=5), "1wk": timedelta(days=5)}[interval]
    return EquityBarRevision(
        bar_revision_id=uuid4(), security_id=security_id, ticker=ticker, interval=interval,
        session_date=start.date(), bar_start=start, bar_end=end, open_price=Decimal("100"),
        high_price=Decimal("102"), low_price=Decimal("99"), close_price=Decimal(close), volume=Decimal("1000"),
        vwap=Decimal("100.5"), transaction_count=10, source_kind=kind,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED, is_final=True,
        system_observed_at=end + timedelta(seconds=observed), replay_available_at=None, adjusted=adjusted,
        payload_sha256=uuid4().hex + uuid4().hex, ingestion_segment_id=uuid4(), session_scope=BarSessionScope.RTH,
        source_bar_revision_ids=(uuid4(),) if kind is BarSourceKind.DERIVED else (),
    )


def _write(pool, revision, cohort_id, instrument_id, tier=ProviderTier.MASSIVE_DEVELOPER):
    fact = (adapt_developer_equity_bar(revision, cohort_id=cohort_id, instrument_id=instrument_id)
            if tier is ProviderTier.MASSIVE_DEVELOPER else
            RealtimeEquityBarFact(cohort_id=cohort_id, provider_tier=tier, instrument_id=instrument_id,
                                  revision=revision))
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            persist_realtime_equity_bars_cursor(cursor, (fact,))
        connection.commit()
    finally:
        pool.putconn(connection)
    return revision.bar_revision_id


def _head(pool, instrument_id, *, interval="5m", basis="RAW_UNADJUSTED", as_of=None):
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            rows = load_bar_range_cursor(cursor, instrument_ids=(instrument_id,), interval=interval, basis=basis,
                                         start=START - timedelta(days=10), end=START + timedelta(days=7), as_of=as_of)
        connection.rollback()
    finally:
        pool.putconn(connection)
    return [row["bar_revision_id"] for row in rows[instrument_id]]


@pytest.fixture
def member(pool):
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            security_id, instrument_id = _seed_member(cursor, f"T{uuid4().hex[:6].upper()}", START - timedelta(days=30))
            developer = insert_cohort(cursor, START - timedelta(days=30))
            advanced = insert_cohort(cursor, START - timedelta(days=1))
        connection.commit()
    finally:
        pool.putconn(connection)
    return security_id, instrument_id, developer, advanced


def test_precedence_wins_over_tier_and_a_later_observation_wins_within_a_source(pool, member):
    security_id, instrument_id, developer, advanced = member
    rest = _write(pool, _revision(security_id, "X", observed=5), developer, instrument_id)
    assert _head(pool, instrument_id) == [rest]
    # A later Advanced derivation of the same 5m bar ranks below the provider's REST bar (precedence first).
    _write(pool, _revision(security_id, "X", kind=BarSourceKind.DERIVED, observed=60), advanced, instrument_id,
           ProviderTier.MASSIVE_ADVANCED)
    assert _head(pool, instrument_id) == [rest]
    # A later REST revision of the same source replaces it, in another cohort too.
    later = _write(pool, _revision(security_id, "X", observed=120, close="101.5"), advanced, instrument_id)
    assert _head(pool, instrument_id) == [later]


def test_a_rerun_reads_the_bar_as_it_stood_at_its_cutoff(pool, member):
    security_id, instrument_id, developer, advanced = member
    first = _write(pool, _revision(security_id, "Y", observed=5), developer, instrument_id)
    later = _write(pool, _revision(security_id, "Y", observed=600), developer, instrument_id)
    end = START + timedelta(minutes=5)
    assert _head(pool, instrument_id) == [later]
    assert _head(pool, instrument_id, as_of=end + timedelta(seconds=300)) == [first]
    assert _head(pool, instrument_id, as_of=end + timedelta(seconds=600)) == [later]
    assert _head(pool, instrument_id, as_of=end) == []  # not yet observed


def test_weekly_adjusted_bars_keep_the_derived_bar_over_a_later_provider_bar(pool, member):
    """The rehearsal case: AAPL's derived weekly bars must not be displaced by later provider weekly bars."""
    security_id, instrument_id, developer, advanced = member
    week = START.replace(hour=13, minute=30) - timedelta(days=4)
    derived = _write(pool, _revision(security_id, "Z", interval="1wk", kind=BarSourceKind.DERIVED, observed=60,
                                     adjusted=True, start=week), developer, instrument_id)
    _write(pool, _revision(security_id, "Z", interval="1wk", observed=3600, adjusted=True, start=week, close="101.8"),
           advanced, instrument_id)
    assert _head(pool, instrument_id, interval="1wk", basis="ADJUSTED") == [derived]


def test_the_backfill_reaches_the_same_head_as_the_writer(pool, member):
    security_id, instrument_id, developer, advanced = member
    _write(pool, _revision(security_id, "W", observed=5), developer, instrument_id)
    _write(pool, _revision(security_id, "W", kind=BarSourceKind.DERIVED, observed=60), advanced, instrument_id,
           ProviderTier.MASSIVE_ADVANCED)
    winner = _write(pool, _revision(security_id, "W", observed=90), developer, instrument_id)
    written = _head(pool, instrument_id)
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            # The state the migration starts from: each cohort's own current pointer in rt.bar_current (the latest
            # revision it wrote per key). The writer no longer fills that table.
            cursor.execute(
                "INSERT INTO rt.bar_current (cohort_id,provider_tier,instrument_id,interval,bar_start,session_scope,"
                "adjustment_basis,source_kind,bar_revision_id,bar_end,observed_at,updated_at) "
                "SELECT DISTINCT ON (cohort_id,interval,bar_start,session_scope,adjustment_basis) cohort_id,"
                "provider_tier,instrument_id,interval,bar_start,session_scope,adjustment_basis,source_kind,"
                "bar_revision_id,bar_end,observed_at,observed_at FROM rt.bar_revisions WHERE instrument_id=%s "
                "ORDER BY cohort_id,interval,bar_start,session_scope,adjustment_basis,observed_at DESC",
                (instrument_id,))
            cursor.execute("DELETE FROM rt.bar_head WHERE instrument_id=%s", (instrument_id,))
            assert backfill_bar_head_cursor(cursor, instrument_id=instrument_id) == 1
            assert backfill_bar_head_cursor(cursor, instrument_id=instrument_id) == 0  # idempotent
        connection.commit()
    finally:
        pool.putconn(connection)
    assert written == _head(pool, instrument_id) == [winner]
