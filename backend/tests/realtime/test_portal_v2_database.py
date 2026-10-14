"""DB-backed portal checks on the V2 baseline (opt-in: STOCKS_V2_DB_TESTS=1, no offline I/O guard).

Uses the disposable database test_realtime_v2_p1 only; it is always dropped. The settings never leave
the helper: tests receive the connection pool and nothing derived from the admin credentials."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from equity.calendar import latest_expected_market_time
from realtime.portal_compatibility_repository import (
    SNAPSHOT_RELATION,
    load_current_release,
    load_release,
    load_session_close_releases,
)
from realtime.portal_projection import build_portal_snapshot
from realtime.lineage_head import reference_set, workspace_set
from realtime.portal_projection_repository import (
    RELEASE_CLASS_SESSION_CLOSE,
    RETENTION_HOT_AND_KEPT,
    PortalReleaseReceipt,
    persist_portal_snapshot_cursor,
    portal_session_date,
    publish_portal_release_cursor,
    session_close_snapshot,
)
from realtime.preflight import ProviderTier
from realtime.ticker_projection import (
    TICKER_WORKSPACE_SUMMARY_VERSION,
    TICKER_WORKSPACE_VERSION,
)
from realtime.ticker_repository import RealtimeTickerWorkspaceRepository
from tests.realtime.v2_database import (
    disposable_v2_database,
    insert_cohort,
    insert_service_plan,
    requires_v2_database,
)

pytestmark = requires_v2_database

DATABASE_NAME = "test_realtime_v2_p1"
UTC = timezone.utc


class _Pool:
    """Hides the pool behind a repr that carries nothing sensitive."""

    def __init__(self, pool):
        self._pool = pool

    def __repr__(self) -> str:
        return "<disposable pool>"

    @contextmanager
    def transaction(self):
        connection = self._pool.getconn()
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                yield cursor
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            self._pool.putconn(connection)


@pytest.fixture(scope="module")
def pool():
    with disposable_v2_database(DATABASE_NAME) as connection_pool:
        yield _Pool(connection_pool)


@dataclass(frozen=True)
class World:
    cohort: UUID
    manifest_sha256: str
    plan: UUID
    securities: dict
    read_at: datetime
    expected: datetime


def _digest() -> str:
    return uuid4().hex + uuid4().hex


def new_world(pool, tickers=("AAPL", "MSFT")) -> World:
    now = datetime.now(UTC)
    read_at = now + timedelta(minutes=10)
    securities = {}
    with pool.transaction() as cursor:
        cohort = insert_cohort(cursor, now)
        plan = insert_service_plan(cursor, cohort, {"PORTAL_PUBLISH"}, now)
        cursor.execute(
            "SELECT manifest.manifest_sha256 FROM rt.cohorts AS cohort "
            "JOIN rt.subscription_manifests AS manifest ON manifest.manifest_id=cohort.manifest_id "
            "WHERE cohort.cohort_id=%s", (str(cohort),))
        manifest_sha256 = cursor.fetchone()["manifest_sha256"]
        for ticker in tickers:
            security, revision, revision_sha = uuid4(), uuid4(), _digest()
            cursor.execute(
                "INSERT INTO ref.securities (security_id, asset_type, source_origin, created_at) "
                "VALUES (%s, 'STOCK', 'fixture', %s)", (str(security), now - timedelta(days=40)))
            cursor.execute(
                "INSERT INTO ref.security_revisions (revision_id, security_id, ticker, calendar_id, "
                "effective_from, observed_at, payload_sha256) VALUES (%s, %s, %s, 'XNYS', %s, %s, %s)",
                (str(revision), str(security), ticker, now - timedelta(days=30), now - timedelta(days=30),
                 revision_sha))
            securities[ticker] = dict(security_id=str(security), ticker=ticker,
                                      security_revision_id=str(revision), payload_sha256=revision_sha)
    return World(cohort, manifest_sha256, plan, securities, read_at,
                 latest_expected_market_time(read_at - timedelta(minutes=15), "1d"))


def new_receipt(pool, world: World, cutoff: datetime) -> PortalReleaseReceipt:
    window, receipt = uuid4(), uuid4()
    with pool.transaction() as cursor:
        cursor.execute(
            "INSERT INTO rt.window_runs (window_id, plan_id, cohort_id, provider_tier, window_key, "
            "release_at, cutoff, deadline) VALUES (%s, %s, %s, 'MASSIVE_DEVELOPER', %s, %s, %s, %s)",
            (str(window), str(world.plan), str(world.cohort), f"window-{window}", cutoff, cutoff,
             cutoff + timedelta(hours=1)))
        cursor.execute(
            "INSERT INTO rt.cycle_receipts (receipt_id, window_id, cohort_id, receipt_kind, cutoff, "
            "reference_generation, universe_sha256, input_sha256, build_id) "
            "VALUES (%s, %s, %s, 'EQUITY_CYCLE', %s, 1, %s, %s, 'build-p1')",
            (str(receipt), str(window), str(world.cohort), cutoff, _digest(), _digest()))
    return PortalReleaseReceipt(receipt, world.cohort, ProviderTier.MASSIVE_DEVELOPER, cutoff, "9" * 64)


def workspace(world: World, ticker, *, generated_at, generation, price):
    stamp = world.expected.isoformat()
    manifest = dict(version=TICKER_WORKSPACE_VERSION, cohort_id=str(world.cohort),
                    provider_tier="MASSIVE_DEVELOPER", source_generation=generation,
                    source_manifest_sha256=world.manifest_sha256, reference=world.securities[ticker])
    payload = dict(
        version=TICKER_WORKSPACE_VERSION, ticker=ticker, provider_fetch_performed=False,
        execution_permission=False, quote=dict(as_of=stamp, price=price, source="1d"),
        multi=dict(as_of=stamp, ticker=ticker, setups={}, errors={}, setup_read_metrics={}, setup_sources={}),
        evidence={}, context=dict(cohort=dict(cohort_id=str(world.cohort))),
        chart_market_times={"1d": stamp}, missing_producers={},
    )
    return build_portal_snapshot(
        cohort_id=world.cohort, provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        snapshot_type=TICKER_WORKSPACE_VERSION, snapshot_key=ticker, source_generation=generation,
        source_manifest=manifest, payload=payload, generated_at=generated_at)


def summary(world: World, workspaces, *, generated_at, generation):
    stamp = world.expected.isoformat()
    references = [row.source_manifest["reference"] for row in workspaces]
    members = [dict(ticker=row.snapshot_key, snapshot_id=str(row.snapshot_id),
                    payload_sha256=row.payload_sha256) for row in workspaces]
    manifest = dict(
        version=TICKER_WORKSPACE_SUMMARY_VERSION, cohort_id=str(world.cohort),
        provider_tier="MASSIVE_DEVELOPER", source_generation=generation,
        source_manifest_sha256=world.manifest_sha256,
        reference_set=reference_set(references), workspace_set=workspace_set(members),
    )
    payload = dict(
        version=TICKER_WORKSPACE_SUMMARY_VERSION, cohort_id=str(world.cohort),
        session=str(world.expected.date()), market_time=stamp, generated_at=generated_at.isoformat(),
        overview=[], setup=dict(interval="1d", source=TICKER_WORKSPACE_VERSION, analysis_run_id=None,
                                computed_at=generated_at.isoformat(), market_time=stamp, count=0, results=[]),
        missing_producers={}, references=references, workspaces=members,
        provider_fetch_performed=False, execution_permission=False,
    )
    return build_portal_snapshot(
        cohort_id=world.cohort, provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        snapshot_type=TICKER_WORKSPACE_SUMMARY_VERSION, snapshot_key="PLATFORM",
        source_generation=generation, source_manifest=manifest, payload=payload, generated_at=generated_at)


def reader(pool) -> RealtimeTickerWorkspaceRepository:
    return RealtimeTickerWorkspaceRepository(pool.transaction)


def drop_hot_partition_of(pool, snapshot):
    """What the maintenance job does when a hot day leaves its window: detach and drop the
    partition, prune the catalog rows whose body went with it, then keep partitions ahead."""
    with pool.transaction() as cursor:
        cursor.execute(
            "SELECT tableoid::regclass::text AS partition FROM rt.portal_snapshot_hot WHERE snapshot_id=%s",
            (str(snapshot.snapshot_id),))
        partition = cursor.fetchone()["partition"]
        schema, name = partition.split(".")
        cursor.execute(sql.SQL("ALTER TABLE rt.portal_snapshot_hot DETACH PARTITION {}.{}").format(
            sql.Identifier(schema), sql.Identifier(name)))
        cursor.execute(sql.SQL("DROP TABLE {}.{}").format(sql.Identifier(schema), sql.Identifier(name)))
        cursor.execute(
            "DELETE FROM rt.snapshot_catalog AS catalog WHERE catalog.snapshot_class='EPHEMERAL_PORTAL' "
            "AND NOT EXISTS (SELECT 1 FROM rt.portal_snapshot_hot AS hot "
            "WHERE hot.snapshot_id=catalog.snapshot_id AND hot.generated_at=catalog.generated_at)")
        cursor.execute("SELECT rt.ensure_partitions('rt.portal_snapshot_hot', CURRENT_DATE)")
    return partition


def test_hot_snapshots_land_in_daily_partitions_and_are_readable_through_the_catalog(pool):
    world = new_world(pool)
    generated = datetime.now(UTC) - timedelta(minutes=2)
    snapshots = [workspace(world, ticker, generated_at=generated, generation=7, price=100.0 + index)
                 for index, ticker in enumerate(("AAPL", "MSFT"))]
    with pool.transaction() as cursor:
        for snapshot in snapshots:
            assert persist_portal_snapshot_cursor(cursor, snapshot) is None
            persist_portal_snapshot_cursor(cursor, snapshot)  # idempotent replay
    with pool.transaction() as cursor:
        cursor.execute("SELECT count(*) AS rows FROM rt.snapshot_catalog WHERE cohort_id=%s", (str(world.cohort),))
        assert cursor.fetchone()["rows"] == 2
        cursor.execute(
            "SELECT tableoid::regclass::text AS partition, count(*) AS rows FROM rt.portal_snapshot_hot "
            "WHERE cohort_id=%s GROUP BY 1", (str(world.cohort),))
        partitions = cursor.fetchall()
        assert len(partitions) == 1 and partitions[0]["rows"] == 2
        assert partitions[0]["partition"].startswith("rt.portal_snapshot_hot_p")
        cursor.execute("SELECT count(*) AS rows FROM rt.portal_snapshot_kept WHERE cohort_id=%s", (str(world.cohort),))
        assert cursor.fetchone()["rows"] == 0
        cursor.execute(
            "SELECT snapshot.snapshot_id, snapshot.payload_sha256, snapshot.source_manifest_sha256, "
            "snapshot.payload, snapshot.snapshot_class FROM " + SNAPSHOT_RELATION +
            " WHERE snapshot.snapshot_id=%s", (str(snapshots[0].snapshot_id),))
        row = cursor.fetchone()
    assert row["snapshot_class"] == "EPHEMERAL_PORTAL"
    assert row["payload"]["quote"]["price"] == 100.0
    assert row["payload_sha256"] == snapshots[0].payload_sha256


def test_release_publication_swaps_the_pointer_and_older_releases_stay_readable(pool):
    world = new_world(pool)
    first_at = datetime.now(UTC) - timedelta(minutes=2)
    second_at = first_at + timedelta(minutes=1)
    first = [workspace(world, ticker, generated_at=first_at, generation=7, price=100.0)
             for ticker in ("AAPL", "MSFT")]
    second_aapl = workspace(world, "AAPL", generated_at=second_at, generation=8, price=105.0)
    first_receipt, second_receipt = new_receipt(pool, world, first_at), new_receipt(pool, world, second_at)
    ticker_reader = reader(pool)

    with pool.transaction() as cursor:
        for snapshot in first:
            persist_portal_snapshot_cursor(cursor, snapshot)
        release_one = publish_portal_release_cursor(cursor, first_receipt, first)
    assert release_one.pointer_swapped
    etag_one = ticker_reader.release_etag(available_by=world.read_at, cohort_id=world.cohort)
    assert ticker_reader.list_tickers(available_by=world.read_at, cohort_id=world.cohort) == ("AAPL", "MSFT")
    quote = ticker_reader.read("AAPL", "quote", cohort_id=world.cohort, available_by=world.read_at,
                               read_mode="retained")
    assert quote["price"] == 100.0

    with pool.transaction() as cursor:
        persist_portal_snapshot_cursor(cursor, second_aapl)
        release_two = publish_portal_release_cursor(cursor, second_receipt, [second_aapl])
    assert release_two.pointer_swapped and release_two.release_id != release_one.release_id
    entries = {row["snapshot_key"]: row for row in release_two.manifest["snapshots"]}
    assert entries["AAPL"]["snapshot_id"] == str(second_aapl.snapshot_id)
    assert entries["MSFT"]["snapshot_id"] == str(first[1].snapshot_id)  # carried forward from release one

    assert ticker_reader.read("AAPL", "quote", cohort_id=world.cohort, available_by=world.read_at,
                              read_mode="retained")["price"] == 105.0
    assert ticker_reader.read("MSFT", "quote", cohort_id=world.cohort, available_by=world.read_at,
                              read_mode="retained")["price"] == 100.0
    assert ticker_reader.release_etag(available_by=world.read_at, cohort_id=world.cohort) != etag_one

    # Read by release: the first release still names exactly the first cycle's snapshots.
    with pool.transaction() as cursor:
        cursor.execute("SELECT release_id, cutoff FROM rt.portal_release_current WHERE cohort_id=%s",
                       (str(world.cohort),))
        pointer = cursor.fetchone()
        assert str(pointer["release_id"]) == str(release_two.release_id)
        assert load_current_release(cursor, world.cohort).release_id == release_two.release_id
        old = load_release(cursor, release_one.release_id, release_one.cutoff)
        assert old.release_class == "INTRADAY" and old.entry(TICKER_WORKSPACE_VERSION, "AAPL")["snapshot_id"] == str(
            first[0].snapshot_id)
        cursor.execute(
            "SELECT snapshot.payload->'quote'->>'price' AS price FROM " + SNAPSHOT_RELATION +
            " WHERE snapshot.snapshot_id=%s", (old.entry(TICKER_WORKSPACE_VERSION, "AAPL")["snapshot_id"],))
        assert cursor.fetchone()["price"] == "100.0"

    # A release from an earlier cutoff is recorded but never takes the pointer back.
    stale_receipt = new_receipt(pool, world, first_at - timedelta(minutes=5))
    with pool.transaction() as cursor:
        stale = publish_portal_release_cursor(cursor, stale_receipt, [first[0]])
    assert stale.pointer_swapped is False
    with pool.transaction() as cursor:
        assert load_current_release(cursor, world.cohort).release_id == release_two.release_id


def test_pin_older_than_the_hot_window_returns_the_current_view_with_a_notice(pool):
    world = new_world(pool)
    now = datetime.now(UTC)
    old = workspace(world, "AAPL", generated_at=now - timedelta(days=2), generation=6, price=90.0)
    current = workspace(world, "AAPL", generated_at=now - timedelta(minutes=1), generation=9, price=110.0)
    receipt = new_receipt(pool, world, now - timedelta(minutes=1))
    ticker_reader = reader(pool)
    with pool.transaction() as cursor:
        persist_portal_snapshot_cursor(cursor, old)
        persist_portal_snapshot_cursor(cursor, current)
        publish_portal_release_cursor(cursor, receipt, [current])

    in_window = ticker_reader.read("AAPL", "publication", cohort_id=world.cohort, snapshot_id=old.snapshot_id,
                                   available_by=world.read_at, read_mode="retained")
    assert in_window["snapshot_id"] == str(old.snapshot_id) and "notice" not in in_window

    dropped = drop_hot_partition_of(pool, old)
    assert dropped.startswith("rt.portal_snapshot_hot_p")
    expired = reader(pool).read("AAPL", "publication", cohort_id=world.cohort, snapshot_id=old.snapshot_id,
                                available_by=world.read_at, read_mode="retained")
    assert expired["snapshot_id"] == str(current.snapshot_id)
    assert expired["notice_code"] == "PINNED_WORKSPACE_EXPIRED"
    assert expired["requested_snapshot_id"] == str(old.snapshot_id)
    assert "hot window" in expired["notice"]
    unpinned = reader(pool).read("AAPL", "publication", cohort_id=world.cohort,
                                 available_by=world.read_at, read_mode="retained")
    assert unpinned["snapshot_id"] == expired["snapshot_id"] and "notice" not in unpinned
    with pool.transaction() as cursor:
        cursor.execute("SELECT count(*) AS rows FROM rt.snapshot_catalog WHERE snapshot_id=%s",
                       (str(current.snapshot_id),))
        assert cursor.fetchone()["rows"] == 1


def test_session_close_release_is_self_contained_after_the_hot_window(pool):
    world = new_world(pool)
    now = datetime.now(UTC)
    generated = now - timedelta(days=2)
    workspaces = [workspace(world, ticker, generated_at=generated, generation=7, price=100.0)
                  for ticker in ("AAPL", "MSFT")]
    hot_summary = summary(world, workspaces, generated_at=generated, generation=7)
    receipt = new_receipt(pool, world, generated)
    session = portal_session_date(hot_summary)
    with pool.transaction() as cursor:
        for snapshot in workspaces:
            persist_portal_snapshot_cursor(cursor, snapshot)
        kept = persist_portal_snapshot_cursor(cursor, hot_summary, retention=RETENTION_HOT_AND_KEPT)
        publish_portal_release_cursor(cursor, receipt, (*workspaces, hot_summary))
        close = publish_portal_release_cursor(
            cursor, receipt, [kept], release_class=RELEASE_CLASS_SESSION_CLOSE, session_date=session)
    assert kept == session_close_snapshot(hot_summary) and kept.snapshot_id != hot_summary.snapshot_id
    assert close.pointer_swapped is False and close.release_class == "SESSION_CLOSE"

    with pool.transaction() as cursor:
        frozen = load_session_close_releases(cursor, world.cohort, session)
        assert [row.release_id for row in frozen] == [close.release_id]
        assert frozen[0].entry(TICKER_WORKSPACE_SUMMARY_VERSION, "PLATFORM")["snapshot_id"] == str(kept.snapshot_id)
        cursor.execute("SELECT count(*) AS rows FROM rt.portal_snapshot_kept WHERE cohort_id=%s", (str(world.cohort),))
        assert cursor.fetchone()["rows"] == 1  # per-ticker workspaces are never kept

    # The whole hot window passes: hot bodies and their catalog rows are gone.
    drop_hot_partition_of(pool, workspaces[0])
    with pool.transaction() as cursor:
        cursor.execute("SELECT count(*) AS rows FROM rt.snapshot_catalog WHERE cohort_id=%s "
                       "AND snapshot_class='EPHEMERAL_PORTAL'", (str(world.cohort),))
        assert cursor.fetchone()["rows"] == 0
    publication = reader(pool).workspace_publication(
        available_by=world.read_at, cohort_id=world.cohort, snapshot_id=kept.snapshot_id, read_mode="retained")
    assert publication["publication"]["snapshot_id"] == str(kept.snapshot_id)
    assert [row["ticker"] for row in publication["workspaces"]] == ["AAPL", "MSFT"]
    assert publication["setup_evidence_refs"] == {}
    with pytest.raises(LookupError):
        reader(pool).workspace_publication(
            available_by=world.read_at, cohort_id=world.cohort, snapshot_id=hot_summary.snapshot_id,
            read_mode="retained")


def insert_screening(pool, world: World, generated_at: datetime):
    generation = uuid4()
    with pool.transaction() as cursor:
        cursor.execute(
            "INSERT INTO rt.screening_generations (generation_id, cohort_id, provider_tier, session_date, "
            "source_manifest_sha256, expected_members, actual_members, status, generated_at, manifest) "
            "VALUES (%s, %s, 'MASSIVE_DEVELOPER', %s, %s, 2, 2, 'COMPLETE', %s, '{}'::jsonb)",
            (str(generation), str(world.cohort), generated_at.date(), world.manifest_sha256, generated_at))
        cursor.execute(
            "INSERT INTO rt.screening_current (cohort_id, generation_id, session_date, generated_at) "
            "VALUES (%s, %s, %s, %s)", (str(world.cohort), str(generation), generated_at.date(), generated_at))


def test_current_view_reader_statements_execute_on_the_v2_schema(pool):
    from types import SimpleNamespace
    from realtime.dashboard_repository import DashboardPublicationUnavailable, RealtimeDashboardRepository
    from realtime.stock_alert_projection import STOCK_ALERTS_VERSION
    from realtime.stock_alert_repository import RealtimeStockAlertViewRepository
    from realtime.strategy_repository import (
        RealtimeDailyStrategyRepository,
        StrategyPublicationUnavailable,
        load_retained_strategy_history_cursor,
    )

    world = new_world(pool)
    generated = datetime.now(UTC) - timedelta(minutes=1)
    workspaces = [workspace(world, ticker, generated_at=generated, generation=7, price=100.0)
                  for ticker in ("AAPL", "MSFT")]
    hot_summary = summary(world, workspaces, generated_at=generated, generation=7)
    refs = [row.source_manifest["reference"] for row in workspaces]
    alerts = build_portal_snapshot(
        cohort_id=world.cohort, provider_tier=ProviderTier.MASSIVE_DEVELOPER,
        snapshot_type=STOCK_ALERTS_VERSION, snapshot_key="PLATFORM", source_generation=7,
        source_manifest=dict(version=STOCK_ALERTS_VERSION, cohort_id=str(world.cohort),
                             source_manifest_sha256=world.manifest_sha256, source_generation=7,
                             provider_tier="MASSIVE_DEVELOPER", reference_set=reference_set(refs)),
        payload=dict(version=STOCK_ALERTS_VERSION, references=refs, provider_fetch_performed=False,
                     publication_permission=False, execution_permission=False),
        generated_at=generated)
    receipt = new_receipt(pool, world, generated)
    with pool.transaction() as cursor:
        for snapshot in (*workspaces, hot_summary, alerts):
            persist_portal_snapshot_cursor(cursor, snapshot)
        publish_portal_release_cursor(cursor, receipt, (*workspaces, hot_summary, alerts))
    insert_screening(pool, world, generated)
    ticker_reader = reader(pool)

    # Current mode joins the reader-visible screening tier; summary resolves its hot workspaces.
    assert ticker_reader.read("AAPL", "quote", cohort_id=world.cohort, available_by=world.read_at)["price"] == 100.0
    status = ticker_reader.materialization_status(available_by=world.read_at, cohort_id=world.cohort)
    assert status["ticker_count"] == 2 and status["status"] == "READY"
    with pool.transaction() as cursor:
        row = ticker_reader._summary_snapshot(
            cursor, cutoff=world.read_at, cohort_id=world.cohort, snapshot_id=None,
            snapshot_type=TICKER_WORKSPACE_SUMMARY_VERSION)
    assert row["snapshot_class"] == "EPHEMERAL_PORTAL" and len(row["payload"]["workspaces"]) == 2
    stored = RealtimeStockAlertViewRepository(pool.transaction)._snapshot(world.read_at, world.cohort)
    assert stored[0]["version"] == STOCK_ALERTS_VERSION

    with pytest.raises(DashboardPublicationUnavailable):
        RealtimeDashboardRepository(pool.transaction).read(
            "latest", available_by=world.read_at, cohort_id=world.cohort, snapshot_id=uuid4())
    with pytest.raises(StrategyPublicationUnavailable):
        RealtimeDailyStrategyRepository(pool.transaction).read(
            available_by=world.read_at, cohort_id=world.cohort, snapshot_id=uuid4())
    inputs = SimpleNamespace(cohort_id=world.cohort, provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                             session=generated.date(), cutoff=world.read_at, source_manifest_sha256=world.manifest_sha256)
    with pool.transaction() as cursor:
        assert load_retained_strategy_history_cursor(cursor, inputs) == ()


def test_release_publication_rejects_unpersisted_snapshots_and_rolls_back_atomically(pool):
    world = new_world(pool)
    generated = datetime.now(UTC) - timedelta(minutes=1)
    snapshot = workspace(world, "AAPL", generated_at=generated, generation=7, price=100.0)
    receipt = new_receipt(pool, world, generated)
    with pytest.raises(ValueError, match="not persisted as published"):
        with pool.transaction() as cursor:
            publish_portal_release_cursor(cursor, receipt, [snapshot])
    with pytest.raises(ValueError, match="identity conflict"):
        with pool.transaction() as cursor:
            persist_portal_snapshot_cursor(cursor, snapshot)
            tampered = build_portal_snapshot(
                cohort_id=world.cohort, provider_tier=ProviderTier.MASSIVE_DEVELOPER,
                snapshot_type=snapshot.snapshot_type, snapshot_key=snapshot.snapshot_key,
                source_generation=7, source_manifest=dict(snapshot.source_manifest), payload=dict(snapshot.payload),
                generated_at=generated)
            persist_portal_snapshot_cursor(cursor, replace(tampered, payload_sha256="f" * 64))
    with pool.transaction() as cursor:
        cursor.execute("SELECT count(*) AS rows FROM rt.snapshot_catalog WHERE cohort_id=%s", (str(world.cohort),))
        assert cursor.fetchone()["rows"] == 0
        cursor.execute("SELECT count(*) AS rows FROM rt.portal_releases WHERE cohort_id=%s", (str(world.cohort),))
        assert cursor.fetchone()["rows"] == 0
        cursor.execute("SELECT count(*) AS rows FROM rt.portal_release_current WHERE cohort_id=%s",
                       (str(world.cohort),))
        assert cursor.fetchone()["rows"] == 0
