from contextlib import closing, contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import re
from unittest.mock import patch
from uuid import uuid4

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
import pytest
from dotenv import load_dotenv

from equity.behavior import StockBehaviorSnapshot
from equity.behavior_producer import produce_stock_behavior_snapshots
from equity.behavior_sources import (
    ADJUSTED_DAILY_HISTORY_POLICY,
    BEHAVIOR_SOURCE_SELECTION_POLICY,
)
from equity.domain import (
    DecisionWatermark, EquityEvidence, EvidenceRole, EvidenceType,
    LifecycleStatus, QualityState,
)
from equity.polygon import sha256_json
from equity.repositories import EquityEvidenceRepository, LEGACY_CONTEXT_COLUMNS, _evidence_values
from test_equity_behavior import snapshot_payload, stored_behavior
from test_equity_behavior_producer import ActionRepository, BarRepository, inputs
from test_equity_materialization_repositories import _context


BACKEND_DIR = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(
    os.getenv("EQUITY_BEHAVIOR_POSTGRES_TESTS") != "1",
    reason="explicit isolated PostgreSQL integration opt-in required",
)


def connection(schema, *, admin=False):
    load_dotenv(BACKEND_DIR / ".env")
    user_key, password_key = ("POSTGRES_ADMIN_USER", "POSTGRES_ADMIN_PASSWORD") if admin else ("DB_USER", "DB_PASSWORD")
    return psycopg2.connect(
        dbname=os.environ["DB_NAME"], host=os.environ["DB_HOST"], port=os.getenv("DB_PORT", "5432"),
        user=os.environ[user_key], password=os.environ[password_key], connect_timeout=10,
        application_name="equity-behavior-isolated-test",
        options=f"-c search_path={schema} -c timezone=UTC -c statement_timeout=15000 -c lock_timeout=2000",
    )


def repository(schema):
    @contextmanager
    def factory():
        with closing(connection(schema)) as current:
            yield current
    return EquityEvidenceRepository(factory)


def migration_sql(schema):
    return (BACKEND_DIR / "migrations" / "044_equity_stock_behavior_contract.sql").read_text(encoding="utf-8").replace("public.", f'"{schema}".')


def correction_sql(schema):
    return (BACKEND_DIR / "migrations" / "045_stock_behavior_adjusted_schema_identifier.sql").read_text(encoding="utf-8").replace("public.", f'"{schema}".')


@pytest.fixture(scope="module")
def isolated_schema():
    schema = "equity_behavior_test_" + uuid4().hex
    baseline = (BACKEND_DIR / "migrations" / "000_canonical_schema.sql").read_text(encoding="utf-8")
    def table(name):
        return re.search(rf"CREATE TABLE public\.{name} \(.*?\n\);", baseline, re.DOTALL).group(0)
    original = _context()
    with closing(connection(schema, admin=True)) as admin:
        try:
            with admin.cursor() as cursor:
                cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
                for name in ("equity_context_snapshots", "equity_evidence", "equity_context_evidence"):
                    cursor.execute(table(name).replace("public.", f'"{schema}".'))
                cursor.execute("ALTER TABLE equity_context_snapshots ADD PRIMARY KEY (equity_context_snapshot_id), "
                    "ADD UNIQUE (ticker, strategy_horizon, market_time, observed_at, context_policy_sha256)")
                cursor.execute("ALTER TABLE equity_evidence ADD PRIMARY KEY (evidence_id), ADD UNIQUE (evidence_key)")
                cursor.execute("ALTER TABLE equity_context_evidence ADD PRIMARY KEY (equity_context_snapshot_id, evidence_id), "
                    "ADD FOREIGN KEY (equity_context_snapshot_id) REFERENCES equity_context_snapshots(equity_context_snapshot_id) ON DELETE CASCADE, "
                    "ADD FOREIGN KEY (evidence_id) REFERENCES equity_evidence(evidence_id)")
                cursor.execute("CREATE TABLE option_context_snapshots (context_snapshot_id uuid PRIMARY KEY, "
                    "equity_context_snapshot_id uuid REFERENCES equity_context_snapshots(equity_context_snapshot_id))")
                cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(sql.Identifier(schema), sql.Identifier(os.environ["DB_USER"])))
                cursor.execute(sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {} TO {}").format(sql.Identifier(schema), sql.Identifier(os.environ["DB_USER"])))
            admin.commit()
            repo = repository(schema)
            watermark = DecisionWatermark(original.market_time, original.observed_at)
            repo.persist_context(original, ())
            assert repo.get_context_as_of(original.ticker, original.strategy_horizon, watermark) == original
            with repo._cursor() as cursor:
                cursor.execute(f"SELECT {', '.join(LEGACY_CONTEXT_COLUMNS)} FROM equity_context_snapshots")
                before = cursor.fetchone()
            with admin.cursor() as cursor:
                cursor.execute(migration_sql(schema))
                cursor.execute(migration_sql(schema))
                cursor.execute(correction_sql(schema))
                cursor.execute("SELECT convalidated FROM pg_constraint WHERE conrelid = 'equity_context_snapshots'::regclass "
                    "AND conname = 'ck_equity_context_behavior_contract'")
                assert cursor.fetchone() == (False,)
            admin.commit()
            with repo._cursor() as cursor:
                cursor.execute(f"SELECT {', '.join(LEGACY_CONTEXT_COLUMNS)} FROM equity_context_snapshots")
                assert cursor.fetchone() == before
            assert repo.get_context_as_of(original.ticker, original.strategy_horizon, watermark) == original
            yield schema, original
        finally:
            admin.rollback()
            with admin.cursor() as cursor:
                cursor.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
            admin.commit()
            with admin.cursor() as cursor:
                cursor.execute("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = %s)", (schema,))
                assert cursor.fetchone() == (False,)


def fresh_snapshot():
    now = datetime.now(timezone.utc) - timedelta(seconds=1)
    payload = snapshot_payload()
    payload.update(computed_at=now, available_at=now, market_time=now - timedelta(minutes=16),
                   valid_until=now + timedelta(minutes=30))
    for component in payload["components"]:
        for source in component["sources"]:
            source.update(market_time=payload["market_time"], observed_at=now - timedelta(minutes=1),
                          recorded_at=now, received_at=now, valid_until=now + timedelta(hours=1))
            if source["interval"] == "1d":
                source.update(
                    policy_sha256=ADJUSTED_DAILY_HISTORY_POLICY.sha256,
                    price_basis="PROVIDER_SPLIT_ADJUSTED",
                    source_manifest_sha256="d" * 64,
                )
            else:
                source["policy_sha256"] = BEHAVIOR_SOURCE_SELECTION_POLICY.sha256
    return StockBehaviorSnapshot.model_validate(payload)


def insert_behavior(schema, snapshot, **overrides):
    row = stored_behavior(snapshot)
    row.pop("created_at")
    row.update(context_kind="STOCK_BEHAVIOR", status="DEGRADED", context_policy_version=snapshot.profile)
    row.update(overrides)
    with repository(schema)._cursor() as cursor:
        cursor.execute(sql.SQL("INSERT INTO equity_context_snapshots ({}) VALUES ({})").format(
            sql.SQL(", ").join(map(sql.Identifier, row)), sql.SQL(", ").join(sql.Placeholder() for _ in row),
        ), tuple(row.values()))


def test_old_writer_api_and_foreign_key_survive_six_column_expansion(isolated_schema):
    from equity.api import equity_context_history

    schema, original = isolated_schema
    repo = repository(schema)
    newer = _context(observed_at=original.observed_at + timedelta(seconds=1))
    repo.persist_context(newer, ())
    repo.persist_context(newer, ())
    assert repo.get_context_as_of("AAPL", newer.strategy_horizon, DecisionWatermark(newer.market_time, newer.observed_at)) == newer
    with repo._cursor() as cursor:
        cursor.execute("INSERT INTO option_context_snapshots VALUES (%s, %s)", (uuid4(), newer.equity_context_snapshot_id))
        cursor.execute("SELECT count(*) AS count FROM pg_attribute WHERE attrelid = 'equity_context_snapshots'::regclass AND attnum > 0 AND NOT attisdropped")
        assert cursor.fetchone()["count"] == len(LEGACY_CONTEXT_COLUMNS) + 6
    with patch("database.get_db_cursor", repo._cursor):
        history = equity_context_history("AAPL", limit=50)
    assert history["count"] == 2
    assert all(set(row) == set(LEGACY_CONTEXT_COLUMNS) for row in history["results"])


def test_real_behavior_reader_and_legacy_isolation(isolated_schema):
    from equity.api import equity_context_history

    schema, _ = isolated_schema
    snapshot = fresh_snapshot()
    insert_behavior(schema, snapshot)
    repo = repository(schema)
    with repo._cursor() as cursor:
        cursor.execute("SELECT clock_timestamp() AS observed_at")
        observed_at = cursor.fetchone()["observed_at"]
    watermark = DecisionWatermark(snapshot.market_time, observed_at)
    assert repo.get_behavior_as_of(snapshot.security_id, watermark, profile=snapshot.profile,
        definition_sha256=snapshot.definition_sha256, policy_sha256=snapshot.policy_sha256) == snapshot
    assert repo.get_context_as_of(snapshot.ticker, snapshot.profile, watermark) is None
    with patch("database.get_db_cursor", repo._cursor):
        assert equity_context_history(snapshot.ticker, limit=50)["results"] == []


@pytest.mark.parametrize("mutation", ["hash", "clock", "profile", "missing", "future"])
def test_database_rejects_inconsistent_behavior_envelope(isolated_schema, mutation):
    schema, _ = isolated_schema
    snapshot = fresh_snapshot()
    overrides = {}
    if mutation == "hash": overrides["behavior_payload_sha256"] = "0" * 64
    if mutation == "clock": overrides["observed_at"] = snapshot.available_at - timedelta(seconds=1)
    if mutation == "profile": overrides["strategy_horizon"] = "INTRADAY_30M"
    if mutation == "missing": overrides["behavior_computed_at"] = None
    if mutation == "future": overrides["observed_at"] = snapshot.available_at + timedelta(days=1)
    with pytest.raises(psycopg2.Error):
        insert_behavior(schema, snapshot, **overrides)


def test_behavior_immutable_but_legacy_mutations_remain_possible(isolated_schema):
    schema, original = isolated_schema
    snapshot = fresh_snapshot()
    insert_behavior(schema, snapshot)
    repo = repository(schema)
    for statement, parameters in (
        ("UPDATE equity_context_snapshots SET ticker = ticker WHERE equity_context_snapshot_id = %s", (snapshot.snapshot_id,)),
        ("DELETE FROM equity_context_snapshots WHERE equity_context_snapshot_id = %s", (snapshot.snapshot_id,)),
        ("TRUNCATE equity_context_snapshots CASCADE", ()),
        ("UPDATE equity_context_snapshots SET context_kind = 'STOCK_BEHAVIOR' WHERE equity_context_snapshot_id = %s", (original.equity_context_snapshot_id,)),
    ):
        with pytest.raises(psycopg2.Error, match="immutable|truncation|reinterpreted"):
            with repo._cursor() as cursor:
                cursor.execute(statement, parameters)
    disposable = _context(ticker="TEST")
    repo.persist_context(disposable, ())
    with repo._cursor() as cursor:
        cursor.execute("UPDATE equity_context_snapshots SET summary = %s::jsonb WHERE equity_context_snapshot_id = %s", ('{"checked":true}', disposable.equity_context_snapshot_id))
        assert cursor.rowcount == 1
        cursor.execute("DELETE FROM equity_context_snapshots WHERE equity_context_snapshot_id = %s", (disposable.equity_context_snapshot_id,))
        assert cursor.rowcount == 1


def test_database_rejects_legacy_evidence_link_for_new_behavior(isolated_schema):
    schema, _ = isolated_schema
    payload = fresh_snapshot().model_dump(mode="python")
    payload_sha256 = sha256_json({})
    for component in payload["components"]:
        component["sources"][0]["payload_sha256"] = payload_sha256
    snapshot = StockBehaviorSnapshot.model_validate(payload)
    legacy = replace(
        behavior_evidence(snapshot)[0], source_name="EQUITY_FEATURES",
        source_version="equity_features_v1", payload_schema_version="1.0",
    )
    evidence_columns = (
        "evidence_id", "evidence_key", "lifecycle_key", "evidence_type",
        "evidence_role", "security_id", "ticker", "interval", "direction",
        "lifecycle_status", "strength", "market_time", "observed_at", "valid_until",
        "source_name", "source_version", "payload_schema_version", "analysis_run_id",
        "latest_bar_revision_id", "security_revision_id", "fundamental_report_ids",
        "source_revision_ids", "quality_state", "quality_codes",
        "qualification_revision_id", "payload", "payload_sha256",
    )
    context_row = stored_behavior(snapshot)
    context_row.pop("created_at")
    context_row.update(
        context_kind="STOCK_BEHAVIOR", status="DEGRADED",
        context_policy_version=snapshot.profile,
    )
    with pytest.raises(psycopg2.Error, match="dedicated immutable source evidence"):
        with repository(schema)._cursor() as cursor:
            cursor.execute(sql.SQL("INSERT INTO equity_evidence ({}) VALUES ({})").format(
                sql.SQL(", ").join(map(sql.Identifier, evidence_columns)),
                sql.SQL(", ").join(sql.Placeholder() for _ in evidence_columns),
            ), _evidence_values(legacy))
            cursor.execute(sql.SQL("INSERT INTO equity_context_snapshots ({}) VALUES ({})").format(
                sql.SQL(", ").join(map(sql.Identifier, context_row)),
                sql.SQL(", ").join(sql.Placeholder() for _ in context_row),
            ), tuple(context_row.values()))
            cursor.execute(
                "INSERT INTO equity_context_evidence VALUES (%s, %s, 'REGIME', 0)",
                (snapshot.snapshot_id, legacy.evidence_id),
            )


def test_migration_lock_is_bounded_and_validation_allows_existing_writer(isolated_schema):
    schema, original = isolated_schema
    with closing(connection(schema)) as writer, closing(connection(schema, admin=True)) as admin:
        with writer.cursor() as cursor:
            cursor.execute("UPDATE equity_context_snapshots SET summary = summary WHERE equity_context_snapshot_id = %s", (original.equity_context_snapshot_id,))
        with pytest.raises(psycopg2.errors.LockNotAvailable):
            with admin.cursor() as cursor:
                cursor.execute(migration_sql(schema))
        admin.rollback()
        with admin.cursor() as cursor:
            cursor.execute("ALTER TABLE equity_context_snapshots VALIDATE CONSTRAINT ck_equity_context_behavior_contract")
        admin.commit()
        writer.rollback()
        admin.autocommit = True
        with admin.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("CREATE INDEX CONCURRENTLY idx_equity_behavior_asof ON equity_context_snapshots "
                "(security_id, strategy_horizon, behavior_definition_sha256, context_policy_sha256, market_time DESC, observed_at DESC) "
                "WHERE context_kind = 'STOCK_BEHAVIOR'")
            cursor.execute("SELECT indisvalid, indisready FROM pg_index WHERE indexrelid = 'idx_equity_behavior_asof'::regclass")
            assert cursor.fetchone() == {"indisvalid": True, "indisready": True}
            cursor.execute("SELECT convalidated FROM pg_constraint WHERE conrelid = 'equity_context_snapshots'::regclass "
                "AND conname = 'ck_equity_context_behavior_contract'")
            assert cursor.fetchone()["convalidated"] is True


def behavior_evidence(snapshot):
    payload_sha256 = sha256_json({})
    rows = []
    for component in snapshot.components:
        source = component.sources[0]
        adjusted = source.interval == "1d"
        rows.append(EquityEvidence(
            evidence_id=source.evidence_id, evidence_key=f"behavior-test:{source.evidence_id}",
            lifecycle_key=f"behavior-test:{snapshot.ticker}:{source.interval}",
            evidence_type=EvidenceType.FEATURE_SNAPSHOT, evidence_role=EvidenceRole.REGIME,
            security_id=snapshot.security_id, ticker=snapshot.ticker, interval=source.interval,
            direction=None, lifecycle_status=LifecycleStatus.SNAPSHOT, strength=None,
            market_time=source.market_time, observed_at=source.observed_at,
            valid_until=source.valid_until,
            source_name=(
                "STOCK_BEHAVIOR_ADJUSTED_DAILY" if adjusted
                else "STOCK_BEHAVIOR_RAW_SOURCE"
            ),
            source_version=(
                "provider_adjusted_daily_history_v1" if adjusted
                else "behavior_feature_source_v1"
            ),
            payload_schema_version=(
                "stock_behavior_adjusted_1d_v1" if adjusted
                else "stock_behavior_raw_source_v1"
            ),
            analysis_run_id=None, latest_bar_revision_id=None,
            security_revision_id=snapshot.security_revision_id,
            fundamental_report_ids=(), source_revision_ids=(),
            quality_state=QualityState.COMPLETE, quality_codes=(),
            qualification_revision_id=None, payload_json="{}", payload_sha256=payload_sha256,
        ))
    return tuple(rows)


def test_real_writer_retry_reader_and_evidence_immutability(isolated_schema):
    schema, _ = isolated_schema
    payload = fresh_snapshot().model_dump(mode="python")
    payload_sha256 = sha256_json({})
    for component in payload["components"]:
        component["sources"][0]["payload_sha256"] = payload_sha256
    template = StockBehaviorSnapshot.model_validate(payload)
    evidence = behavior_evidence(template)

    def factory(clocks):
        current = template.model_dump(mode="python")
        received_at = max(datetime.now(timezone.utc), *clocks.values())
        current.update(computed_at=received_at, available_at=received_at)
        for component in current["components"]:
            source = component["sources"][0]
            source["recorded_at"] = clocks[source["evidence_id"]]
            source["received_at"] = received_at
        return StockBehaviorSnapshot.model_validate(current)

    repo = repository(schema)
    stored, inserted = repo.persist_behavior(factory, derived_evidence=evidence)
    assert inserted is True
    repeated, inserted = repo.persist_behavior(factory, derived_evidence=evidence)
    assert inserted is False and repeated == stored
    watermark = DecisionWatermark(stored.market_time, datetime.now(timezone.utc))
    assert repo.get_behavior_as_of(
        stored.security_id, watermark, profile=stored.profile,
        definition_sha256=stored.definition_sha256, policy_sha256=stored.policy_sha256,
    ) == stored

    with closing(connection(schema)) as current:
        for statement, parameters, message in (
            ("DELETE FROM equity_context_evidence WHERE equity_context_snapshot_id=%s", (stored.snapshot_id,), "links are immutable"),
            ("INSERT INTO equity_context_evidence VALUES (%s,%s,'REGIME',99)", (stored.snapshot_id, evidence[0].evidence_id), "links are immutable"),
            ("UPDATE equity_evidence SET quality_codes=quality_codes WHERE evidence_id=%s", (evidence[0].evidence_id,), "source evidence is immutable"),
            ("DELETE FROM equity_evidence WHERE evidence_id=%s", (evidence[0].evidence_id,), "source evidence is immutable"),
        ):
            with pytest.raises(psycopg2.Error, match=message):
                with current.cursor() as cursor:
                    cursor.execute(statement, parameters)
            current.rollback()
        for table in ("equity_context_evidence", "equity_evidence"):
            with pytest.raises(psycopg2.Error, match="prevents evidence truncation"):
                with current.cursor() as cursor:
                    cursor.execute(sql.SQL("TRUNCATE {} CASCADE").format(sql.Identifier(table)))
            current.rollback()


def test_real_producer_persists_and_retries_atomically(isolated_schema):
    schema, _ = isolated_schema
    securities, raw_reads, adjusted_reads, coverage = inputs()
    current = datetime.now(timezone.utc)
    raw_reads = [replace(
        read, evidence=replace(read.evidence, valid_until=current + timedelta(hours=2)),
    ) for read in raw_reads]
    actual = repository(schema)
    with actual._cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS rows FROM equity_context_snapshots WHERE context_kind='STOCK_BEHAVIOR'")
        contexts_before = cursor.fetchone()["rows"]
        cursor.execute("SELECT COUNT(*) AS rows FROM equity_context_evidence")
        links_before = cursor.fetchone()["rows"]
        cursor.execute("SELECT COUNT(*) AS rows FROM equity_evidence WHERE source_name LIKE 'STOCK_BEHAVIOR_%'")
        evidence_before = cursor.fetchone()["rows"]

    class ProducerRepository:
        def read_behavior_feature_sources(self, *args, **kwargs):
            return tuple(raw_reads)

        def persist_behavior(self, *args, **kwargs):
            return actual.persist_behavior(*args, **kwargs)

    arguments = {
        "securities": securities,
        "watermark": DecisionWatermark(current, current),
        "evidence_repository": ProducerRepository(),
        "bar_repository": BarRepository(adjusted_reads),
        "corporate_action_repository": ActionRepository(coverage),
        "received_at": current,
    }
    first = produce_stock_behavior_snapshots(**arguments)
    repeated = produce_stock_behavior_snapshots(**arguments)

    assert (first.inserted, first.existing, first.skipped, first.failed) == (2, 0, (), ())
    assert (repeated.inserted, repeated.existing, repeated.skipped, repeated.failed) == (0, 2, (), ())
    with actual._cursor() as cursor:
        cursor.execute("SELECT COUNT(*) AS rows FROM equity_context_snapshots WHERE context_kind='STOCK_BEHAVIOR'")
        assert cursor.fetchone()["rows"] == contexts_before + 2
        cursor.execute("SELECT COUNT(*) AS rows FROM equity_context_evidence")
        assert cursor.fetchone()["rows"] == links_before + 7
        cursor.execute("SELECT COUNT(*) AS rows FROM equity_evidence WHERE source_name LIKE 'STOCK_BEHAVIOR_%'")
        assert cursor.fetchone()["rows"] == evidence_before + 6