"""DB-backed by-reference Ticker Workspace: publish a compact workspace and read it back through the reader.

Opt in with STOCKS_V2_DB_TESTS=1 and run WITHOUT the offline I/O guard plugin. One ticker of the nine-name
fixture is written to the real V2 tables (bars through `persist_realtime_equity_bars_cursor`, evidence through
`compute_realtime_ticker_cycle`, snapshots and the release through `publish_realtime_ticker_cycle`) in the
disposable database test_realtime_v2_ref_reader, which is always dropped. Credentials stay inside
tests/realtime/v2_database.py: nothing here receives, stores or prints them.
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from time import perf_counter
from unittest.mock import MagicMock
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest
from psycopg2.extras import RealDictCursor

from realtime.database_access import RealtimeDatabaseRole
from realtime.equity_bar_repository import persist_realtime_equity_bars_cursor
from realtime.portal_projection import build_portal_snapshot
from realtime.portal_projection_repository import (
    PortalReleaseReceipt, persist_portal_snapshot_cursor, publish_portal_release_cursor,
)
from realtime.preflight import ProviderTier
import realtime.ticker_repository as ticker_repository
from realtime.ticker_projection import TICKER_WORKSPACE_VERSION, _fact
from realtime.ticker_repository import (
    RealtimeTickerWorkspaceRepository, TickerPublicationUnavailable, compute_realtime_ticker_cycle,
    publish_realtime_ticker_cycle,
)
from realtime.worker_database import RealtimeProcessDatabase
from tests.realtime.test_ticker_vertical_slice import (
    as_stored_json, old_embedded_charts, old_embedded_evidence, ticker_fixture,
)
from tests.realtime.v2_database import (
    disposable_v2_database, insert_cohort, insert_service_plan, requires_v2_database,
)

pytestmark = requires_v2_database

DATABASE_NAME = "test_realtime_v2_ref_reader"
TICKER = "AAPL"
UTC = timezone.utc
# Bars of the fixture were "observed" at their own bar end, up to five years back; the baseline keeps bar
# partitions for 400 days, so backfilled history is observed no earlier than this.
OBSERVED_FLOOR = datetime(2026, 9, 20, tzinfo=UTC)
CHART_INTERVALS = ("1m", "5m", "15m", "30m", "1h", "1d", "1wk", "1mo")


class _Pool:
    """Hides the pool behind a repr that carries nothing sensitive. `transaction()` yields mapping rows
    (what the reader uses); `plain()` yields tuple rows (what the compute and publication writers use)."""

    def __init__(self, pool):
        self._pool = pool

    def __repr__(self) -> str:
        return "<disposable pool>"

    @contextmanager
    def _cursor(self, **options):
        connection = self._pool.getconn()
        try:
            with connection.cursor(**options) as cursor:
                yield cursor
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            self._pool.putconn(connection)

    def transaction(self):
        return self._cursor(cursor_factory=RealDictCursor)

    def plain(self):
        return self._cursor()


class _TimedCursor:
    """Records how long each reader statement spends in the database, by statement kind."""

    LABELS = (("SELECT cur.interval", "bars"), ("SELECT evidence.evidence_id AS", "evidence_bodies"),
              ("SELECT dependency.evidence_id", "evidence_dependencies"))

    def __init__(self, cursor, record):
        self._cursor, self._record, self._label = cursor, record, None

    def __getattr__(self, name):
        return getattr(self._cursor, name)

    def execute(self, statement, parameters=None):
        self._label = next((label for prefix, label in self.LABELS if statement.startswith(prefix)), "other")
        started = perf_counter()
        try:
            return self._cursor.execute(statement, parameters)
        finally:
            self._record.setdefault(self._label, []).append(
                (perf_counter() - started, statement, parameters))

    def fetchall(self):
        started = perf_counter()
        rows = self._cursor.fetchall()
        self._record.setdefault(f"{self._label}:fetch", []).append((perf_counter() - started, "", None))
        return rows


@dataclass(frozen=True)
class World:
    cohort: UUID
    plan: UUID
    inputs: object
    cycle: object
    workspace: object
    read_at: datetime
    instrument_id: int


@pytest.fixture(scope="module")
def database():
    with disposable_v2_database(DATABASE_NAME) as connection_pool:
        pool = _Pool(connection_pool)
        seed_reference(pool)
        yield pool


@lru_cache(maxsize=1)
def base_inputs():
    inputs = ticker_fixture()
    member = next(row for row in inputs.members if row["ticker"] == TICKER)
    return replace(
        inputs, members=(member,), bars=tuple(row for row in inputs.bars if row["ticker"] == TICKER),
        actions=tuple(row for row in inputs.actions if row["ticker"] == TICKER),
        coverage=tuple(row for row in inputs.coverage if row["security_id"] == member["security_id"]),
    )


INSTRUMENT = {}


def seed_reference(pool):
    """The security, its current revision and an equity instrument, shared by every world of this module."""
    member = base_inputs().members[0]
    with pool.transaction() as cursor:
        cursor.execute(
            "INSERT INTO ref.securities (security_id, asset_type, source_origin, created_at) "
            "VALUES (%s, 'STOCK', 'fixture', %s)", (str(member["security_id"]), member["observed_at"] - timedelta(days=600)))
        cursor.execute(
            "INSERT INTO ref.security_revisions (revision_id, security_id, ticker, calendar_id, effective_from, "
            "observed_at, payload_sha256) VALUES (%s, %s, %s, 'XNYS', %s, %s, %s)",
            (str(member["security_revision_id"]), str(member["security_id"]), TICKER, member["effective_from"],
             member["observed_at"], member["payload_sha256"]))
        cursor.execute("INSERT INTO rt.instruments (asset_class, security_id) VALUES ('EQUITY', %s) "
                       "RETURNING instrument_id", (str(member["security_id"]),))
        INSTRUMENT["id"] = cursor.fetchone()["instrument_id"]
        # The fixture's session is in the past; the baseline pre-creates hot partitions around today.
        cursor.execute("SELECT rt.ensure_partitions('rt.portal_snapshot_hot', %s)", (base_inputs().cutoff.date(),))


def _digest() -> str:
    return uuid4().hex + uuid4().hex


def new_receipt(pool, plan, cohort, cutoff) -> PortalReleaseReceipt:
    window, receipt = uuid4(), uuid4()
    with pool.transaction() as cursor:
        cursor.execute(
            "INSERT INTO rt.window_runs (window_id, plan_id, cohort_id, provider_tier, window_key, "
            "release_at, cutoff, deadline) VALUES (%s, %s, %s, 'MASSIVE_DEVELOPER', %s, %s, %s, %s)",
            (str(window), str(plan), str(cohort), f"window-{window}", cutoff, cutoff, cutoff + timedelta(hours=1)))
        cursor.execute(
            "INSERT INTO rt.cycle_receipts (receipt_id, window_id, cohort_id, receipt_kind, cutoff, "
            "reference_generation, universe_sha256, input_sha256, build_id) "
            "VALUES (%s, %s, %s, 'EQUITY_CYCLE', %s, 1, %s, %s, 'build-ref')",
            (str(receipt), str(window), str(cohort), cutoff, _digest(), _digest()))
    return PortalReleaseReceipt(receipt, cohort, ProviderTier.MASSIVE_DEVELOPER, cutoff, "9" * 64)


def process_database(role):
    database = object.__new__(RealtimeProcessDatabase)
    database._process_plan = MagicMock(role=role)
    return database


def publish_world(pool) -> World:
    """A fresh cohort with one ticker's bars, evidence and a published release (the real writers)."""
    now = datetime.now(UTC)
    with pool.transaction() as cursor:
        cohort = insert_cohort(cursor, now)
        plan = insert_service_plan(cursor, cohort, {"PORTAL_PUBLISH"}, now)
        cursor.execute(
            "SELECT manifest.manifest_sha256 FROM rt.cohorts AS cohort JOIN rt.subscription_manifests AS manifest "
            "ON manifest.manifest_id=cohort.manifest_id WHERE cohort.cohort_id=%s", (str(cohort),))
        manifest_sha256 = cursor.fetchone()["manifest_sha256"]
    source = base_inputs()
    # Bar identity is global in the baseline, so every world gets its own revision ids (and lineage links).
    remap = {row["bar_revision_id"]: uuid5(NAMESPACE_URL, f"{cohort}:{row['bar_revision_id']}") for row in source.bars}
    bars = tuple(
        dict(row, cohort_id=cohort, instrument_id=INSTRUMENT["id"], bar_revision_id=remap[row["bar_revision_id"]],
             source_revision_ids=tuple(remap[value] for value in row["source_revision_ids"]),
             observed_at=max(row["observed_at"], OBSERVED_FLOOR))
        for row in source.bars
    )
    inputs = replace(
        source, cohort_id=cohort, bars=bars, source_manifest_sha256=manifest_sha256,
        members=(dict(source.members[0], instrument_id=INSTRUMENT["id"]),),
    )
    facts = [_fact(row, inputs) for row in bars]
    with pool.plain() as cursor:
        for offset in range(0, len(facts), 1000):
            persist_realtime_equity_bars_cursor(cursor, facts[offset:offset + 1000])
        # Autovacuum keeps these statistics fresh in production; a bulk load in a test must do it by hand.
        cursor.execute("ANALYZE rt.bar_current")
        cursor.execute("ANALYZE rt.bar_revisions")
    cycle = compute(pool, inputs)
    receipt = new_receipt(pool, plan, cohort, inputs.cutoff)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(RealtimeProcessDatabase, "transaction", _transaction(pool))
        publish_realtime_ticker_cycle(process_database(RealtimeDatabaseRole.PUBLICATION), cycle, receipt=receipt)
    workspace = next(row for row in cycle.projection.snapshots if row.snapshot_key == TICKER)
    return World(cohort, plan, inputs, cycle, workspace, datetime.now(UTC) + timedelta(minutes=10), INSTRUMENT["id"])


def _transaction(pool):
    @contextmanager
    def transaction(database, **options):
        with (pool.transaction() if options.get("dict_cursor") else pool.plain()) as cursor:
            yield cursor

    return transaction


def compute(pool, inputs):
    """The EQUITY_EVIDENCE half (bars as loaded, evidence and the screening generation persisted) for `inputs`."""
    scope = dict(cohort_id=inputs.cohort_id, session=inputs.session, cutoff=inputs.cutoff,
                 source_generation=inputs.source_generation)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(RealtimeProcessDatabase, "transaction", _transaction(pool))
        patch.setattr(ticker_repository, "load_realtime_screening_input", lambda cursor, **kwargs: inputs)
        patch.setattr(ticker_repository, "_load_ticker_calendar_inputs", lambda cursor, members, cutoff: ((), ()))
        patch.setattr(ticker_repository, "_load_ticker_fundamental_inputs", lambda cursor, members, cutoff: ((), ()))
        return compute_realtime_ticker_cycle(process_database(RealtimeDatabaseRole.COMPUTE), **scope)


@pytest.fixture(scope="module")
def world(database):
    return publish_world(database)


def reader(pool) -> RealtimeTickerWorkspaceRepository:
    return RealtimeTickerWorkspaceRepository(pool.transaction)


def read(pool, world, contract="quote", **options):
    return reader(pool).read(TICKER, contract, available_by=world.read_at, cohort_id=world.cohort,
                             read_mode="retained", **options)


def republish(pool, world, mutate, *, minutes=1):
    """Publish a later workspace for the same ticker whose payload `mutate` changed (a published reference
    to something the tables do not hold)."""
    payload = deepcopy(world.workspace.payload)
    mutate(payload)
    generated_at = world.workspace.generated_at + timedelta(minutes=minutes)
    tampered = build_portal_snapshot(
        cohort_id=world.cohort, provider_tier=ProviderTier.MASSIVE_DEVELOPER, snapshot_type=TICKER_WORKSPACE_VERSION,
        snapshot_key=TICKER, source_generation=world.workspace.source_generation,
        source_manifest=world.workspace.source_manifest, payload=payload, generated_at=generated_at)
    receipt = new_receipt(pool, world.plan, world.cohort, generated_at)
    with pool.plain() as cursor:
        persist_portal_snapshot_cursor(cursor, tampered)
        publish_portal_release_cursor(cursor, receipt, (tampered,))


def test_the_stored_workspace_is_compact_and_hashed_as_stored(database, world):
    with database.transaction() as cursor:
        cursor.execute(
            "SELECT snapshot.payload ? 'charts' AS charts, snapshot.payload ? 'evidence' AS evidence, "
            "snapshot.payload ? 'chart_refs' AS chart_refs, octet_length(snapshot.payload::text) AS payload_bytes, "
            "octet_length(snapshot.source_manifest::text) AS manifest_bytes, catalog.payload_sha256 "
            "FROM rt.snapshot_catalog AS catalog JOIN rt.portal_snapshot_hot AS snapshot "
            "ON snapshot.snapshot_id=catalog.snapshot_id AND snapshot.generated_at=catalog.generated_at "
            "WHERE catalog.snapshot_id=%s", (str(world.workspace.snapshot_id),))
        row = cursor.fetchone()
    assert (row["charts"], row["evidence"], row["chart_refs"]) == (False, False, True)
    assert row["payload_bytes"] < 100_000 and row["manifest_bytes"] < 16_384
    assert row["payload_sha256"] == world.workspace.payload_sha256
    print(f"stored workspace: payload {row['payload_bytes']} B, manifest {row['manifest_bytes']} B (jsonb text)")


def test_reader_composes_the_charts_and_evidence_the_payload_used_to_embed(database, world):
    from realtime.ticker_references import compose_evidence

    stored = as_stored_json(world.workspace.payload)
    manifest = as_stored_json(world.workspace.source_manifest)
    loaded = reader(database)._snapshot(TICKER, world.read_at, world.cohort, None, "retained")
    assert "charts" not in loaded and "evidence" not in loaded
    assert loaded["evidence_refs"].keys() == {"30m", "1h", "1d", "1wk", "1mo"}
    with database.transaction() as cursor:
        bodies = ticker_repository._verified_evidence_bodies(
            cursor, manifest, stored, cohort_id=world.cohort, cutoff=world.read_at)
    assert as_stored_json(compose_evidence(stored["evidence_index"], bodies)) == as_stored_json(
        old_embedded_evidence(world.workspace, world.cycle.projection))


@pytest.mark.parametrize("interval", CHART_INTERVALS)
def test_chart_reads_equal_the_embedded_charts_through_the_real_bar_tables(database, world, interval):
    group = f"{interval}:{'ADJUSTED' if interval in ('1d', '1wk', '1mo') else 'RAW_UNADJUSTED'}"
    expected = old_embedded_charts(TICKER, {interval: group})[interval]
    chart = read(database, world, "chart", interval=interval, period="5y")
    assert len(chart) > 0 and as_stored_json(chart) == as_stored_json(expected[-len(chart):])
    assert len(expected) == world.workspace.source_manifest["bar_lineage"]["groups"][group]["count"]


def test_a_workspace_loads_without_reading_any_bar_until_a_chart_is_requested(database, world):
    record = {}

    @contextmanager
    def cursor():
        with database.transaction() as real:
            yield _TimedCursor(real, record)

    repository = RealtimeTickerWorkspaceRepository(cursor)
    repository.read(TICKER, "quote", available_by=world.read_at, cohort_id=world.cohort, read_mode="retained")
    assert "bars" not in record and "evidence_bodies" in record
    repository.read(TICKER, "chart", interval="1d", period="5y", available_by=world.read_at,
                    cohort_id=world.cohort, read_mode="retained")
    assert len(record["bars"]) == 1 and record["bars"][0][2][5:7] == ("1d", "ADJUSTED")


def test_the_compute_stage_persists_the_screening_generation_the_workspace_names(database, world):
    assert world.cycle.projection.screening_generation is None
    with database.transaction() as cursor:
        cursor.execute(
            "SELECT generation.generation_id, generation.status, generation.provider_tier, generation.session_date, "
            "generation.generated_at, generation.expected_members, generation.actual_members, "
            "generation.manifest->>'generation' AS digest, "
            "(SELECT count(*) FROM rt.screening_rows AS stored WHERE stored.generation_id=generation.generation_id) "
            "AS stored_rows, (SELECT count(*) FROM rt.screening_generations AS every WHERE every.cohort_id=%s) "
            "AS generations FROM rt.screening_current AS pointer "
            "JOIN rt.screening_generations AS generation ON generation.generation_id=pointer.generation_id "
            "WHERE pointer.cohort_id=%s", (str(world.cohort), str(world.cohort)))
        row = cursor.fetchone()
    assert (row["status"], row["provider_tier"], row["session_date"], row["generated_at"]) == (
        "COMPLETE", "MASSIVE_DEVELOPER", world.inputs.session, world.inputs.cutoff)
    assert (row["expected_members"], row["actual_members"], row["stored_rows"], row["generations"]) == (1, 1, 1, 1)
    assert row["digest"] == world.workspace.payload["context"]["cohort"]["generation"]


def test_the_persisted_generation_passes_the_screening_reader_identity_and_hash_checks(database, world):
    from realtime.portal_compatibility_repository import RealtimeScreeningRepository

    with database.transaction() as cursor:
        payload = RealtimeScreeningRepository(database.transaction)._load(cursor, str(world.cohort), world.read_at)
    assert [row["ticker"] for row in payload["rows"]] == [TICKER]
    assert payload["generation"] == world.workspace.payload["context"]["cohort"]["generation"]
    # The manifest it was verified against holds heads, not bars: bytes of one stored generation (one name).
    with database.transaction() as cursor:
        cursor.execute("SELECT octet_length(manifest::text) AS manifest_bytes FROM rt.screening_generations "
                       "WHERE cohort_id=%s", (str(world.cohort),))
        assert cursor.fetchone()["manifest_bytes"] < 24 * 1024 + 3 * 1024 * 2


def test_the_screening_reader_recomposes_the_hourly_lineage_from_the_bar_tables(database, world):
    from realtime.portal_compatibility_repository import RealtimeScreeningRepository
    from research.screening import SnapshotRowRequest

    digest = world.workspace.payload["context"]["cohort"]["generation"]
    security_id = str(world.inputs.members[0]["security_id"])
    detail = RealtimeScreeningRepository(database.transaction).hourly_details(
        SnapshotRowRequest(generation=digest, security_id=security_id),
        available_by=world.read_at, cohort_id=world.cohort)
    lineage = detail["lineage"]
    hourly = {str(row["bar_revision_id"]): row for row in world.inputs.bars
              if row["interval"] == "1h" and row["adjustment_basis"] == "RAW_UNADJUSTED"}
    assert len(lineage["bars"]) == 23 and set(lineage["bars"]) <= set(hourly)
    assert lineage["selected_bar_id"] == lineage["bars"][-1] and lineage["reconstructed_from_30m"] == {}
    assert lineage["derived_source_revision_ids"] == {
        identity: [str(source) for source in hourly[identity]["source_revision_ids"]] for identity in lineage["bars"]}


def test_a_bar_revised_after_the_generation_fails_the_screening_reader_closed(database):
    from realtime.portal_compatibility_repository import (
        RealtimeScreeningRepository, ScreeningPublicationUnavailable,
    )

    world = publish_world(database)
    last = max((row for row in world.inputs.bars if row["interval"] == "1d" and row["adjustment_basis"] == "ADJUSTED"),
               key=lambda row: row["bar_start"])
    fact = _fact(last, world.inputs)
    with database.plain() as cursor:
        persist_realtime_equity_bars_cursor(cursor, (replace(fact, revision=replace(
            fact.revision, bar_revision_id=uuid4(), payload_sha256="a" * 64,
            supersedes_bar_revision_id=fact.revision.bar_revision_id, system_observed_at=datetime.now(UTC))),))

    def load(repository):
        with database.transaction() as cursor:
            return repository._load(cursor, str(world.cohort), world.read_at)

    with pytest.raises(ScreeningPublicationUnavailable, match="republish required"):
        load(RealtimeScreeningRepository(database.transaction))
    # The stored identity and hash checks alone (no bar tables attached) still pass: only the bars drifted.
    assert load(RealtimeScreeningRepository(database.transaction, bar_loader=None))["rows"]


def test_a_replayed_cycle_persists_nothing_new_and_a_later_cycle_advances_the_pointer(database):
    world = publish_world(database)

    def stored():
        with database.transaction() as cursor:
            cursor.execute(
                "SELECT (SELECT count(*) FROM rt.screening_generations WHERE cohort_id=%s) AS generations, "
                "(SELECT count(*) FROM rt.screening_rows AS stored JOIN rt.screening_generations AS generation "
                "ON generation.generation_id=stored.generation_id WHERE generation.cohort_id=%s) AS stored_rows, "
                "(SELECT generated_at FROM rt.screening_current WHERE cohort_id=%s) AS current_at",
                (str(world.cohort),) * 3)
            return dict(cursor.fetchone())

    first = stored()
    assert (first["generations"], first["stored_rows"], first["current_at"]) == (1, 1, world.inputs.cutoff)
    compute(database, world.inputs)
    assert stored() == first
    later = replace(world.inputs, cutoff=world.inputs.cutoff + timedelta(minutes=1))
    compute(database, later)
    advanced = stored()
    assert (advanced["generations"], advanced["stored_rows"], advanced["current_at"]) == (2, 2, later.cutoff)


def test_current_mode_and_the_hot_summary_resolve_the_compact_workspace(database, world):
    quote = reader(database).read(TICKER, "quote", available_by=world.read_at, cohort_id=world.cohort)
    assert quote == as_stored_json(world.workspace.payload["quote"])
    publication = reader(database).workspace_publication(available_by=world.read_at, cohort_id=world.cohort)
    assert set(publication["setup_evidence_refs"]) == {TICKER}
    assert publication["setup_evidence_refs"][TICKER]["evidence_type"] == "TRADE_SETUP"
    listing = reader(database).read(TICKER, "publication", available_by=world.read_at, cohort_id=world.cohort)
    assert listing["snapshot_id"] == str(world.workspace.snapshot_id)


def test_bars_appended_after_publication_do_not_disturb_the_published_lineage(database):
    world = publish_world(database)
    last = max((row for row in world.inputs.bars if row["interval"] == "5m"), key=lambda row: row["bar_start"])
    fact = _fact(last, world.inputs)
    later = replace(fact, revision=replace(
        fact.revision, bar_revision_id=uuid4(), payload_sha256="b" * 64,
        bar_start=fact.revision.bar_end, bar_end=fact.revision.bar_end + timedelta(minutes=5),
        system_observed_at=datetime.now(UTC)))
    with database.plain() as cursor:
        persist_realtime_equity_bars_cursor(cursor, (later,))
    chart = read(database, world, "chart", interval="5m", period="5y")
    assert chart[-1]["time"] == int(fact.revision.bar_start.timestamp())


def test_a_bar_revised_after_publication_fails_closed(database):
    world = publish_world(database)
    last = max((row for row in world.inputs.bars if row["interval"] == "1d" and row["adjustment_basis"] == "ADJUSTED"),
               key=lambda row: row["bar_start"])
    fact = _fact(last, world.inputs)
    revised = replace(fact, revision=replace(
        fact.revision, bar_revision_id=uuid4(), payload_sha256="c" * 64,
        supersedes_bar_revision_id=fact.revision.bar_revision_id, system_observed_at=datetime.now(UTC)))
    with database.plain() as cursor:
        persist_realtime_equity_bars_cursor(cursor, (revised,))
    with pytest.raises(TickerPublicationUnavailable, match="bar lineage changed; republish required"):
        read(database, world, "chart", interval="1d", period="5y")
    # The other intervals and the workspace itself still verify and serve.
    assert read(database, world, "chart", interval="5m", period="5y")
    assert read(database, world)["price"] == world.workspace.payload["quote"]["price"]


def test_a_bar_retired_after_publication_fails_closed(database):
    world = publish_world(database)
    with database.transaction() as cursor:
        cursor.execute(
            "DELETE FROM rt.bar_current WHERE cohort_id=%s AND interval='1h' AND adjustment_basis='RAW_UNADJUSTED' "
            "AND bar_start=(SELECT min(bar_start) FROM rt.bar_current WHERE cohort_id=%s AND interval='1h')",
            (str(world.cohort), str(world.cohort)))
        assert cursor.rowcount == 1
    with pytest.raises(TickerPublicationUnavailable, match="bar lineage changed"):
        read(database, world, "chart", interval="1h", period="5y")
    assert read(database, world, "chart", interval="30m", period="5y")


@pytest.mark.parametrize("defect", ["missing", "hash"])
def test_evidence_the_release_references_but_the_tables_do_not_hold_fails_closed(database, defect):
    world = publish_world(database)
    victim = next(ref for rows in world.workspace.payload["evidence_index"].values() for ref in rows
                  if ref["evidence_type"] != "TRADE_SETUP")

    def mutate(payload):
        for rows in payload["evidence_index"].values():
            for ref in rows:
                if ref["evidence_id"] == victim["evidence_id"]:
                    if defect == "missing":
                        ref["evidence_id"] = str(uuid4())
                    else:
                        ref["payload_sha256"] = "0" * 64

    republish(database, world, mutate)
    with pytest.raises(ValueError, match="immutable identity/hash"):
        read(database, world)


def test_first_and_cached_load_times_of_the_workspace_and_each_chart(database, world):
    """Measures the reader on one ticker's retained bars (13,804 rows over eight intervals) in the real tables."""
    record = {}

    def timed():
        @contextmanager
        def cursor():
            with database.transaction() as real:
                yield _TimedCursor(real, record)
        return cursor()

    samples = []
    for _ in range(3):
        record.clear()
        repository = RealtimeTickerWorkspaceRepository(timed)
        timings = {}

        def timed_read(label, contract, **options):
            started = perf_counter()
            repository.read(TICKER, contract, available_by=world.read_at, cohort_id=world.cohort,
                            read_mode="retained", **options)
            timings[label] = perf_counter() - started

        timed_read("workspace first load", "quote")
        timed_read("workspace cached", "quote")
        for interval in CHART_INTERVALS:
            timed_read(f"chart {interval} first load", "chart", interval=interval, period="5y")
        timed_read("chart 5m cached", "chart", interval="5m", period="5y")
        timings["all eight charts first load"] = sum(
            value for label, value in timings.items() if label.startswith("chart") and label.endswith("first load"))
        samples.append(timings)
    median = {label: sorted(sample[label] for sample in samples)[len(samples) // 2] for label in samples[0]}
    print("reader timings, median of 3 (ms): " + ", ".join(f"{label}={value * 1000:.1f}" for label, value in median.items()))
    assert all(median[label] < 5.0 for label in median) and median["chart 5m cached"] < median["chart 5m first load"]
    five = next(entry for entry in record["bars"] if entry[2][5] == "5m")
    with database.transaction() as cursor:
        cursor.execute("EXPLAIN (ANALYZE, BUFFERS) " + five[1], five[2])
        print("5m bar query plan:\n" + "\n".join(row["QUERY PLAN"] for row in cursor.fetchall()))


def _audit(database, world):
    from scripts import audit_realtime_ticker_history as history

    connection = database._pool.getconn()
    try:
        return history.run_audit(connection, database=DATABASE_NAME, cohort_id=world.cohort,
                                 session_date=world.inputs.session)["by_reference_integrity"]
    finally:
        connection.rollback()
        connection.set_session(readonly=False, isolation_level="READ COMMITTED")
        database._pool.putconn(connection)


def test_the_audit_confirms_that_the_current_workspaces_references_resolve(database):
    world = publish_world(database)
    integrity = _audit(database, world)
    refs = sum(len(rows) for rows in world.workspace.payload["evidence_index"].values())
    assert integrity["workspaces"] == 1 and integrity["evidence_referenced"] == refs > 0
    assert integrity["evidence_missing"] == integrity["evidence_mismatched"] == 0
    assert integrity["chart_groups"] == len(CHART_INTERVALS) and integrity["chart_groups_drifted"] == []


def test_the_audit_reports_a_revised_or_retired_bar_as_chart_drift(database):
    world = publish_world(database)
    last = max((row for row in world.inputs.bars if row["interval"] == "1d" and row["adjustment_basis"] == "ADJUSTED"),
               key=lambda row: row["bar_start"])
    fact = _fact(last, world.inputs)
    with database.plain() as cursor:
        persist_realtime_equity_bars_cursor(cursor, (replace(fact, revision=replace(
            fact.revision, bar_revision_id=uuid4(), payload_sha256="d" * 64,
            supersedes_bar_revision_id=fact.revision.bar_revision_id, system_observed_at=datetime.now(UTC))),))
    with database.transaction() as cursor:
        cursor.execute(
            "DELETE FROM rt.bar_current WHERE cohort_id=%s AND interval='1h' AND adjustment_basis='RAW_UNADJUSTED' "
            "AND bar_start=(SELECT min(bar_start) FROM rt.bar_current WHERE cohort_id=%s AND interval='1h')",
            (str(world.cohort), str(world.cohort)))
    drifted = _audit(database, world)["chart_groups_drifted"]
    assert {(row["interval"], row["published_count"] - row["current_count"]) for row in drifted} == {
        ("1d", 0), ("1h", 1)}


@pytest.mark.parametrize("defect", ["missing", "hash"])
def test_the_audit_reports_evidence_the_release_pins_but_the_tables_do_not_hold(database, defect):
    world = publish_world(database)
    victim = next(ref for rows in world.workspace.payload["evidence_index"].values() for ref in rows
                  if ref["evidence_type"] != "TRADE_SETUP")

    def mutate(payload):
        for rows in payload["evidence_index"].values():
            for ref in rows:
                if ref["evidence_id"] == victim["evidence_id"]:
                    if defect == "missing":
                        ref["evidence_id"] = str(uuid4())
                    else:
                        ref["payload_sha256"] = "0" * 64

    republish(database, world, mutate)
    integrity = _audit(database, world)
    assert (integrity["evidence_missing"], integrity["evidence_mismatched"]) == (
        (1, 0) if defect == "missing" else (0, 1))
