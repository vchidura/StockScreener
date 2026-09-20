from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import psycopg2
import pytest
from dotenv import load_dotenv
from psycopg2 import sql

from options.repositories.stock_behavior_assessments import (
    OptionStockBehaviorAssessmentRepository,
    stock_behavior_assessment_id,
)
from options.stock_behavior_gates import evaluate_option_stock_behavior
from options.strategies.domain import StructureType


BACKEND_DIR = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(
    os.getenv("OPTION_STOCK_BEHAVIOR_POSTGRES_TESTS") != "1",
    reason="explicit isolated PostgreSQL integration opt-in required",
)


def connection(*, admin=False, schema="public"):
    load_dotenv(BACKEND_DIR / ".env")
    user_key, password_key = (
        ("POSTGRES_ADMIN_USER", "POSTGRES_ADMIN_PASSWORD")
        if admin else ("DB_USER", "DB_PASSWORD")
    )
    if not all(os.getenv(key) for key in ("DB_NAME", "DB_HOST", user_key, password_key)):
        raise RuntimeError("configured PostgreSQL credentials are required for isolated tests")
    return psycopg2.connect(
        dbname=os.environ["DB_NAME"], host=os.environ["DB_HOST"],
        port=os.getenv("DB_PORT", "5432"), user=os.environ[user_key],
        password=os.environ[password_key], connect_timeout=10,
        application_name="option-stock-behavior-isolated-test",
        options=(
            f"-c search_path={schema} -c timezone=UTC "
            "-c statement_timeout=15000 -c lock_timeout=10000"
        ),
    )


@pytest.fixture(scope="module")
def isolated_schema():
    schema = "option_stock_behavior_test_" + uuid4().hex
    migration = (
        BACKEND_DIR / "migrations" / "046_option_stock_behavior_assessments.sql"
    ).read_text(encoding="utf-8").replace("public.", f'"{schema}".')
    launch_migration = (
        BACKEND_DIR / "migrations" / "047_option_stock_behavior_launch_identity.sql"
    ).read_text(encoding="utf-8").replace("public.", f'"{schema}".')
    with closing(connection(admin=True)) as admin:
        try:
            with admin.cursor() as cursor:
                cursor.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
                cursor.execute(sql.SQL("""
                    CREATE TABLE {}.option_ingestion_runs (
                        batch_id uuid PRIMARY KEY,
                        underlying varchar(16) NOT NULL,
                        scheduled_cycle timestamptz NOT NULL,
                        configuration_sha256 character(64) NOT NULL,
                        policy_sha256 character(64) NOT NULL
                    )
                """).format(sql.Identifier(schema)))
                cursor.execute(sql.SQL("""
                    CREATE TABLE {}.option_analysis_runs (
                        matrix_id uuid PRIMARY KEY,
                        batch_id uuid NOT NULL REFERENCES {}.option_ingestion_runs(batch_id),
                        underlying varchar(16) NOT NULL,
                        market_time timestamptz NOT NULL,
                        observed_time timestamptz NOT NULL,
                        policy_sha256 character(64) NOT NULL
                    )
                """).format(sql.Identifier(schema), sql.Identifier(schema)))
                cursor.execute(sql.SQL("""
                    CREATE TABLE {}.option_strategy_candidates (
                        candidate_id uuid PRIMARY KEY,
                        candidate_identity character(64) NOT NULL,
                        matrix_id uuid NOT NULL REFERENCES {}.option_analysis_runs(matrix_id),
                        underlying varchar(16) NOT NULL,
                        strategy_version varchar(64) NOT NULL,
                        policy_sha256 character(64) NOT NULL,
                        market_data_time timestamptz NOT NULL,
                        observed_time timestamptz NOT NULL
                    )
                """).format(sql.Identifier(schema), sql.Identifier(schema)))
                cursor.execute(sql.SQL("""
                    CREATE TABLE {}.equity_context_snapshots (
                        equity_context_snapshot_id uuid PRIMARY KEY,
                        context_kind varchar(32) NOT NULL,
                        ticker varchar(16) NOT NULL,
                        market_time timestamptz NOT NULL,
                        observed_at timestamptz NOT NULL,
                        valid_until timestamptz NOT NULL
                    )
                """).format(sql.Identifier(schema)))
                cursor.execute(migration)
                cursor.execute(migration)
                cursor.execute(launch_migration)
                cursor.execute(launch_migration)
                for statement in (
                    "GRANT USAGE ON SCHEMA {} TO {}",
                    "GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA {} TO {}",
                    "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {} TO {}",
                ):
                    cursor.execute(sql.SQL(statement).format(
                        sql.Identifier(schema), sql.Identifier(os.environ["DB_USER"]),
                    ))
            admin.commit()
            yield schema
        finally:
            admin.rollback()
            with admin.cursor() as cursor:
                cursor.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(schema),
                ))
            admin.commit()
            with admin.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = %s)",
                    (schema,),
                )
                assert cursor.fetchone()[0] is False


def repository(schema):
    @contextmanager
    def factory():
        with closing(connection(schema=schema)) as current:
            yield current

    return OptionStockBehaviorAssessmentRepository(factory)


def assessment(schema, *, reason="STOCK_BEHAVIOR_SNAPSHOT_UNAVAILABLE"):
    decision_at = datetime.now(timezone.utc) - timedelta(seconds=2)
    scheduled_cycle = decision_at - timedelta(minutes=6)
    market_time = decision_at - timedelta(minutes=5)
    batch_id = uuid4()
    matrix_id = uuid4()
    candidate_id = uuid4()
    with closing(connection(schema=schema)) as current:
        with current.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO option_ingestion_runs (
                    batch_id, underlying, scheduled_cycle,
                    configuration_sha256, policy_sha256
                ) VALUES (%s, 'AAPL', %s, %s, %s)
                """,
                (batch_id, scheduled_cycle, "c" * 64, "d" * 64),
            )
            cursor.execute(
                """
                INSERT INTO option_analysis_runs (
                    matrix_id, batch_id, underlying, market_time,
                    observed_time, policy_sha256
                ) VALUES (%s, %s, 'AAPL', %s, %s, %s)
                """,
                (matrix_id, batch_id, market_time, decision_at, "d" * 64),
            )
            cursor.execute(
                """
                INSERT INTO option_strategy_candidates (
                    candidate_id, candidate_identity, matrix_id, underlying,
                    strategy_version, policy_sha256, market_data_time, observed_time
                ) VALUES (%s, %s, %s, 'AAPL', 'strategy_v1', %s, %s, %s)
                """,
                (
                    candidate_id, "a" * 64, matrix_id, "b" * 64,
                    market_time, decision_at,
                ),
            )
        current.commit()
    return evaluate_option_stock_behavior(
        None, candidate_id=candidate_id, candidate_identity_sha256="a" * 64,
        matrix_id=matrix_id, underlyer="AAPL",
        strategy_name="DIRECTIONAL_LONG_PREMIUM",
        structure_type=StructureType.LONG_CALL, directional_thesis="BULLISH",
        option_strategy_version="strategy_v1",
        option_strategy_policy_sha256="b" * 64,
        option_configuration_sha256="c" * 64,
        option_market_policy_sha256="d" * 64,
        option_analysis_policy_sha256="d" * 64,
        launch_id="options-stock-behavior-test-v1",
        launch_manifest_sha256="e" * 64,
        option_market_time=market_time,
        option_observed_at=decision_at,
        stock_market_cutoff=scheduled_cycle,
        decision_at=decision_at, unavailable_reason=reason,
    )


def insert_raw(cursor, row, *, assessment_id=None, recorded_at=None):
    cursor.execute(
        """
        INSERT INTO option_stock_behavior_assessments (
            assessment_id, candidate_id, candidate_identity, matrix_id,
            stock_snapshot_id, underlying, launch_id,
            launch_manifest_sha256, option_strategy_version,
            option_strategy_policy_sha256, option_configuration_sha256,
            option_market_policy_sha256, option_analysis_policy_sha256,
            option_market_time, option_observed_at, stock_market_cutoff,
            detector_policy_version,
            detector_policy_sha256, decision_at, disposition,
            payload_text, payload_sha256, recorded_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        RETURNING recorded_at
        """,
        (
            assessment_id or stock_behavior_assessment_id(row), row.candidate_id,
            row.candidate_identity_sha256, row.matrix_id, row.stock_snapshot_id,
            row.underlyer, row.launch_id, row.launch_manifest_sha256,
            row.option_strategy_version,
            row.option_strategy_policy_sha256, row.option_configuration_sha256,
            row.option_market_policy_sha256, row.option_analysis_policy_sha256,
            row.option_market_time, row.option_observed_at, row.stock_market_cutoff,
            row.detector_policy_version, row.detector_policy_sha256,
            row.decision_at, row.disposition, row.canonical_json(), row.sha256,
            recorded_at or row.decision_at,
        ),
    )
    return cursor.fetchone()[0]


def test_postgres_repository_retry_and_database_clock(isolated_schema):
    row = assessment(isolated_schema)
    repo = repository(isolated_schema)
    lineage = repo.get_matrix_lineage(row.matrix_id)
    assert lineage is not None
    assert lineage.configuration_sha256 == row.option_configuration_sha256
    assert lineage.market_policy_sha256 == row.option_market_policy_sha256
    assert lineage.analysis_policy_sha256 == row.option_analysis_policy_sha256
    assert lineage.scheduled_cycle == row.stock_market_cutoff
    first = repo.persist((row,))
    retry = repo.persist((row,))
    assert (first.inserted, first.existing) == (1, 0)
    assert (retry.inserted, retry.existing) == (0, 1)
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            cursor.execute(
                "SELECT payload_text, payload_sha256, recorded_at "
                "FROM option_stock_behavior_assessments WHERE assessment_id = %s",
                (stock_behavior_assessment_id(row),),
            )
            payload, payload_sha256, recorded_at = cursor.fetchone()
    assert payload == row.canonical_json() and payload_sha256 == row.sha256
    assert recorded_at > row.decision_at
    usage = repo.launch_usage(
        row.decision_at - timedelta(minutes=1),
        row.decision_at + timedelta(minutes=1),
        row.launch_manifest_sha256,
    )
    assert usage.assessment_count == 1
    assert usage.payload_bytes == len(row.canonical_json().encode("utf-8"))
    assert usage.unavailable_count == 1
    assert usage.p95_decision_lag_seconds is not None
    assert usage.p95_decision_lag_seconds > 0
    status = repo.status(
        row.decision_at - timedelta(minutes=1), datetime.now(timezone.utc) + timedelta(minutes=1),
    )
    assert status.assessment_count >= 1
    assert status.candidate_count >= 1
    assert status.eligible_count + status.blocked_count + status.unavailable_count == status.assessment_count
    assert status.latest_decision_at is not None and status.latest_recorded_at is not None


def test_postgres_concurrent_retry_preserves_one_exact_row(isolated_schema):
    row = assessment(isolated_schema)
    barrier = Barrier(2)

    def persist_once():
        barrier.wait(timeout=10)
        return repository(isolated_schema).persist((row,))

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [future.result(timeout=20) for future in (
            executor.submit(persist_once), executor.submit(persist_once),
        )]
    assert sorted(result.inserted for result in results) == [0, 1]
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM option_stock_behavior_assessments "
                "WHERE candidate_id = %s",
                (row.candidate_id,),
            )
            assert cursor.fetchone()[0] == 1


def test_postgres_repository_rejects_conflicting_existing_payload(isolated_schema):
    requested = assessment(isolated_schema)
    conflicting = evaluate_option_stock_behavior(
        None,
        candidate_id=requested.candidate_id,
        candidate_identity_sha256=requested.candidate_identity_sha256,
        matrix_id=requested.matrix_id, underlyer=requested.underlyer,
        strategy_name=requested.strategy_name,
        structure_type=StructureType(requested.structure_type),
        directional_thesis=requested.directional_thesis,
        option_strategy_version=requested.option_strategy_version,
        option_strategy_policy_sha256=requested.option_strategy_policy_sha256,
        option_configuration_sha256=requested.option_configuration_sha256,
        option_market_policy_sha256=requested.option_market_policy_sha256,
        option_analysis_policy_sha256=requested.option_analysis_policy_sha256,
        launch_id=requested.launch_id,
        launch_manifest_sha256=requested.launch_manifest_sha256,
        option_market_time=requested.option_market_time,
        option_observed_at=requested.option_observed_at,
        stock_market_cutoff=requested.stock_market_cutoff,
        decision_at=requested.decision_at,
        unavailable_reason="STOCK_BEHAVIOR_READ_FAILED",
    )
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            insert_raw(
                cursor, conflicting,
                assessment_id=stock_behavior_assessment_id(requested),
            )
        current.commit()
    with pytest.raises(ValueError, match="differs from requested evidence"):
        repository(isolated_schema).persist((requested,))


def test_postgres_guards_mutation_truncate_and_parent_identity(isolated_schema):
    row = assessment(isolated_schema)
    repository(isolated_schema).persist((row,))
    for statement in (
        "UPDATE option_stock_behavior_assessments SET underlying=underlying WHERE candidate_id=%s",
        "DELETE FROM option_stock_behavior_assessments WHERE candidate_id=%s",
    ):
        with closing(connection(schema=isolated_schema)) as current:
            with current.cursor() as cursor, pytest.raises(psycopg2.Error, match="immutable"):
                cursor.execute(statement, (row.candidate_id,))
            current.rollback()
    with closing(connection(admin=True, schema=isolated_schema)) as current:
        with current.cursor() as cursor, pytest.raises(psycopg2.Error, match="immutable"):
            cursor.execute("TRUNCATE option_stock_behavior_assessments")
        current.rollback()
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            cursor.execute(
                "SELECT candidate_identity, matrix_id, underlying FROM option_strategy_candidates "
                "WHERE candidate_id=%s",
                (row.candidate_id,),
            )
            assert cursor.fetchone() == ("a" * 64, row.matrix_id, "AAPL")


def test_postgres_failed_identity_insert_rolls_back(isolated_schema):
    row = assessment(isolated_schema)
    invalid = row.model_copy(update={"candidate_identity_sha256": "f" * 64})
    with pytest.raises(psycopg2.Error, match="identity or clocks disagree"):
        repository(isolated_schema).persist((invalid,))
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM option_stock_behavior_assessments "
                "WHERE candidate_id=%s",
                (row.candidate_id,),
            )
            assert cursor.fetchone()[0] == 0


def test_postgres_failed_matrix_lineage_insert_rolls_back(isolated_schema):
    row = assessment(isolated_schema)
    invalid = row.model_copy(update={"option_configuration_sha256": "f" * 64})
    with pytest.raises(psycopg2.Error, match="matrix lineage disagrees"):
        repository(isolated_schema).persist((invalid,))
    with closing(connection(schema=isolated_schema)) as current:
        with current.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM option_stock_behavior_assessments "
                "WHERE candidate_id=%s",
                (row.candidate_id,),
            )
            assert cursor.fetchone()[0] == 0