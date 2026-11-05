from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import psycopg2
import pytest
from psycopg2.extras import Json, RealDictCursor

from equity.stock_alert_results import retain_record
from realtime.canonical_stock_alert_product import (
    canonical_stock_alert_contexts,
    produce_canonical_stock_alert_batch,
)
from realtime.canonical_stock_alert_repository import (
    _context_reference_rows_cursor,
    load_canonical_stock_alert_projection_cursor,
    persist_canonical_stock_alert_batch_cursor,
)
from realtime.runner_environment import StockAlertProjectionRequest
from research.stock_idea_engine import digest
from tests.realtime.test_canonical_stock_alert_product import (
    _ready_context_inputs,
)
from tests.realtime.v2_database import (
    disposable_v2_database,
    insert_cohort,
    insert_service_plan,
    requires_v2_database,
)


pytestmark = requires_v2_database
UTC = timezone.utc


@contextmanager
def cursor(pool):
    connection = pool.getconn()
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as value:
            yield value
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        pool.putconn(connection)


def test_v2_stock_alert_source_schema_round_trip_and_guards():
    database_name = (
        "test_realtime_v2_stock_alert_source_"
        + uuid4().hex[:8]
    )
    instance_id = "intraday-quality-v2"
    now = datetime(2026, 10, 27, 17, tzinfo=UTC)
    with disposable_v2_database(database_name) as pool:
        with cursor(pool) as database_cursor:
            database_cursor.execute(
                "SET LOCAL search_path TO rt,ref,pg_catalog"
            )
            database_cursor.execute(
                "INSERT INTO rt.stock_alert_result_instances "
                "(instance_id,payload) VALUES (%s,%s)",
                (
                    instance_id,
                    Json({
                        "instance_id": instance_id,
                        "stream": "intraday",
                    }),
                ),
            )
            database_cursor.execute(
                "INSERT INTO rt.stock_alert_result_records "
                "(instance_id,kind,record_id,payload_sha256,payload) "
                "VALUES (%s,'observation','record-1',%s,%s)",
                (
                    instance_id,
                    "a" * 64,
                    Json({"record_id": "record-1"}),
                ),
            )
            database_cursor.execute(
                "INSERT INTO rt.stock_alert_result_storage_identities "
                "(instance_id,kind,record_id,source_payload_sha256,"
                "storage_payload_sha256) "
                "VALUES (%s,'observation','record-1',%s,%s)",
                (instance_id, "a" * 64, "a" * 64),
            )
            database_cursor.execute(
                "INSERT INTO rt.stock_alert_result_revisions "
                "(instance_id,alert_id,source_as_of,"
                "payload_sha256,payload) VALUES (%s,%s,%s,%s,%s)",
                (
                    instance_id,
                    "alert-1",
                    now,
                    "b" * 64,
                    Json({"alert_id": "alert-1"}),
                ),
            )
            database_cursor.execute(
                "INSERT INTO rt.stock_alert_result_streams "
                "(stream,instance_id,source_as_of,source_sha256,"
                "metadata,checked_at,imported_at,error) "
                "VALUES ('intraday',%s,%s,%s,%s,%s,%s,NULL)",
                (
                    instance_id,
                    now,
                    "c" * 64,
                    Json({"status": "AVAILABLE"}),
                    now,
                    now,
                ),
            )
            source_payload = {
                "record_id": "signed-zero",
                "value": -0.0,
            }
            retain_record(
                database_cursor,
                instance_id,
                "publication_evidence",
                "signed-zero",
                source_payload,
            )
            database_cursor.execute(
                "SELECT record.payload,"
                "identity.source_payload_sha256,"
                "identity.storage_payload_sha256 "
                "FROM rt.stock_alert_result_records AS record "
                "JOIN rt.stock_alert_result_storage_identities AS identity "
                "USING(instance_id,kind,record_id) "
                "WHERE record.instance_id=%s "
                "AND record.kind='publication_evidence' "
                "AND record.record_id='signed-zero'",
                (instance_id,),
            )
            storage = database_cursor.fetchone()
            assert storage["source_payload_sha256"] == digest(
                source_payload
            )
            assert storage["storage_payload_sha256"] == digest(
                storage["payload"]
            )
            database_cursor.execute(
                "SELECT count(*) FROM rt.stock_alert_result_instances"
            )
            assert database_cursor.fetchone()["count"] == 1
            database_cursor.execute("SAVEPOINT immutable_check")
            with pytest.raises(psycopg2.errors.RaiseException):
                database_cursor.execute(
                    "UPDATE rt.stock_alert_result_instances "
                    "SET instance_id=instance_id "
                    "WHERE instance_id=%s",
                    (instance_id,),
                )
            database_cursor.execute(
                "ROLLBACK TO SAVEPOINT immutable_check"
            )
            database_cursor.execute(
                "UPDATE rt.stock_alert_result_streams "
                "SET checked_at=%s,error='TEST' "
                "WHERE stream='intraday'",
                (now,),
            )
            database_cursor.execute(
                "SELECT error FROM rt.stock_alert_result_streams "
                "WHERE stream='intraday'"
            )
            assert database_cursor.fetchone()["error"] == "TEST"


def test_v2_canonical_stock_alert_batch_matches_exact_receipt():
    database_name = (
        "test_realtime_v2_stock_alert_binding_"
        + uuid4().hex[:8]
    )
    now = datetime.now(UTC)
    cutoff = now - timedelta(minutes=1)
    with disposable_v2_database(database_name) as pool:
        with cursor(pool) as database_cursor:
            cohort_id = insert_cohort(database_cursor, cutoff)
            plan_id = insert_service_plan(
                database_cursor,
                cohort_id,
                ["EQUITY_EVIDENCE", "STOCK_ALERTS"],
                cutoff,
            )
            window_id, receipt_id, batch_id = uuid4(), uuid4(), uuid4()
            database_cursor.execute(
                "INSERT INTO rt.window_runs(" 
                "window_id,plan_id,cohort_id,provider_tier,window_key," 
                "release_at,cutoff,deadline,state) VALUES(" 
                "%s,%s,%s,'MASSIVE_DEVELOPER',%s,%s,%s,%s,'COMPLETE')",
                (
                    str(window_id), str(plan_id), str(cohort_id),
                    f"stock-alert-{window_id}", cutoff, cutoff,
                    now + timedelta(minutes=5),
                ),
            )
            database_cursor.execute(
                "INSERT INTO rt.cycle_receipts(" 
                "receipt_id,window_id,cohort_id,receipt_kind,cutoff," 
                "reference_generation,universe_sha256,input_sha256," 
                "build_id,known_by) VALUES("
                "%s,%s,%s,'INPUT_READY',%s,1,%s,%s,%s,%s)",
                (
                    str(receipt_id), str(window_id), str(cohort_id),
                    cutoff, "a" * 64, "b" * 64,
                    "stock-alert-binding-test-v1",
                    now,
                ),
            )
            database_cursor.execute(
                "INSERT INTO rt.detector_versions("
                "detector_sha256,detector_id,version,code_sha256,"
                "policy_sha256,requirements,output_kind) "
                "VALUES(%s,'canonical-equity-stock-alert-v1','1',"
                "%s,%s,%s,'ALERT')",
                (
                    "c" * 64,
                    "d" * 64,
                    "e" * 64,
                    Json({"receipt": "INPUT_READY"}),
                ),
            )
            database_cursor.execute("SAVEPOINT divergent_binding")
            with pytest.raises(psycopg2.errors.RaiseException):
                database_cursor.execute(
                    "INSERT INTO rt.canonical_stock_alert_batches("
                    "batch_id,cohort_id,window_id,input_receipt_id,cutoff,"
                    "receipt_input_sha256,detector_input_sha256,"
                    "detector_sha256,status,evaluation_count,match_count,"
                    "no_match_count,unavailable_count,error_count,"
                    "batch_sha256,batch_document,completed_at) VALUES("
                    "%s,%s,%s,%s,%s,%s,%s,%s,'COMPLETE',0,0,0,0,0,"
                    "%s,%s,%s)",
                    (
                        str(batch_id),
                        str(cohort_id),
                        str(window_id),
                        str(receipt_id),
                        cutoff,
                        "0" * 64,
                        "f" * 64,
                        "c" * 64,
                        "1" * 64,
                        Json({"status": "COMPLETE"}),
                        now,
                    ),
                )
            database_cursor.execute("ROLLBACK TO SAVEPOINT divergent_binding")
            database_cursor.execute(
                "INSERT INTO rt.canonical_stock_alert_batches("
                "batch_id,cohort_id,window_id,input_receipt_id,cutoff,"
                "receipt_input_sha256,detector_input_sha256,"
                "detector_sha256,status,evaluation_count,match_count,"
                "no_match_count,unavailable_count,error_count,"
                "batch_sha256,batch_document,completed_at) VALUES("
                "%s,%s,%s,%s,%s,%s,%s,%s,'COMPLETE',0,0,0,0,0,"
                "%s,%s,%s)",
                (
                    str(batch_id),
                    str(cohort_id),
                    str(window_id),
                    str(receipt_id),
                    cutoff,
                    "b" * 64,
                    "f" * 64,
                    "c" * 64,
                    "1" * 64,
                    Json({"status": "COMPLETE"}),
                    now,
                ),
            )
            database_cursor.execute("SAVEPOINT immutable_binding")
            with pytest.raises(psycopg2.errors.RaiseException):
                database_cursor.execute(
                    "UPDATE rt.canonical_stock_alert_batches "
                    "SET receipt_input_sha256=receipt_input_sha256 "
                    "WHERE batch_id=%s",
                    (str(batch_id),),
                )
            database_cursor.execute("ROLLBACK TO SAVEPOINT immutable_binding")


def test_v2_canonical_stock_alert_batch_persists_and_replays():
    database_name = (
        "test_realtime_v2_canonical_stock_alert_"
        + uuid4().hex[:8]
    )
    inputs, context_members, _events, event_coverage = (
        _ready_context_inputs()
    )
    cutoff = inputs.cutoff
    known_by = max(
        inputs.recorded_by + timedelta(seconds=1),
        datetime.now(UTC) + timedelta(seconds=1),
    )
    with disposable_v2_database(database_name) as pool:
        with cursor(pool) as database_cursor:
            cohort_id = insert_cohort(database_cursor, cutoff)
            plan_id = insert_service_plan(
                database_cursor,
                cohort_id,
                ["EQUITY_EVIDENCE", "STOCK_ALERTS"],
                cutoff,
            )
            window_id, receipt_id = uuid4(), uuid4()
            database_cursor.execute(
                "INSERT INTO rt.window_runs("
                "window_id,plan_id,cohort_id,provider_tier,window_key,"
                "release_at,cutoff,deadline,state) VALUES("
                "%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,'COMPLETE')",
                (
                    str(window_id),
                    str(plan_id),
                    str(cohort_id),
                    f"canonical-stock-alert-{window_id}",
                    cutoff,
                    cutoff,
                    known_by + timedelta(minutes=5),
                ),
            )
            database_cursor.execute(
                "INSERT INTO rt.cycle_receipts("
                "receipt_id,window_id,cohort_id,receipt_kind,cutoff,"
                "reference_generation,universe_sha256,input_sha256,"
                "build_id,created_at,known_by) VALUES("
                "%s,%s,%s,'INPUT_READY',%s,1,%s,%s,%s,%s,%s)",
                (
                    str(receipt_id),
                    str(window_id),
                    str(cohort_id),
                    cutoff,
                    "a" * 64,
                    "b" * 64,
                    "canonical-stock-alert-test-v1",
                    inputs.recorded_by,
                    inputs.recorded_by,
                ),
            )
            rebound = replace(inputs, cohort_id=cohort_id)
            all_members = (*rebound.members, *context_members)
            for member in all_members:
                database_cursor.execute(
                    "INSERT INTO ref.securities("
                    "security_id,asset_type,source_origin,created_at) "
                    "VALUES(%s,%s,'canonical-test',%s)",
                    (
                        str(member["security_id"]),
                        (
                            "ETF"
                            if member["ticker"] == "SPY"
                            else "STOCK"
                        ),
                        cutoff - timedelta(days=1),
                    ),
                )
                database_cursor.execute(
                    "INSERT INTO rt.instruments("
                    "instrument_id,asset_class,security_id) "
                    "VALUES(%s,'EQUITY',%s)",
                    (
                        int(member["instrument_id"]),
                        str(member["security_id"]),
                    ),
                )
            for row in event_coverage:
                database_cursor.execute(
                    "INSERT INTO ref.market_event_coverage("
                    "coverage_id,security_id,event_type,window_start,"
                    "window_end,source,verdict,checked_at,valid_until,"
                    "evidence_sha256) VALUES("
                    "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        str(row["coverage_id"]),
                        (
                            None
                            if row["security_id"] is None
                            else str(row["security_id"])
                        ),
                        row["event_type"],
                        row["window_start"],
                        row["window_end"],
                        row["source"],
                        row["verdict"],
                        row["checked_at"],
                        row["valid_until"],
                        row["evidence_sha256"],
                    ),
                )
            for bar in rebound.bars:
                database_cursor.execute(
                    "INSERT INTO rt.bar_catalog("
                    "bar_revision_id,cohort_id,provider_tier,"
                    "instrument_id,interval,bar_start,bar_end,"
                    "observed_at,session_date) VALUES("
                    "%s,%s,'MASSIVE_ADVANCED',%s,'5m',%s,%s,%s,%s)",
                    (
                        str(bar["bar_revision_id"]),
                        str(cohort_id),
                        next(
                            int(member["instrument_id"])
                            for member in all_members
                            if member["ticker"] == bar["ticker"]
                        ),
                        bar["bar_start"],
                        bar["bar_end"],
                        bar["observed_at"],
                        bar["session_date"],
                    ),
                )
            retained_events, retained_coverage = (
                _context_reference_rows_cursor(
                    database_cursor,
                    rebound,
                )
            )
            assert retained_events == ()
            assert len(retained_coverage) == 2
            batch = produce_canonical_stock_alert_batch(
                rebound,
                window_id=window_id,
                input_receipt_id=receipt_id,
                receipt_input_sha256="b" * 64,
                contexts=canonical_stock_alert_contexts(
                    rebound,
                    context_members=context_members,
                    market_events=retained_events,
                    market_event_coverage=retained_coverage,
                ),
            )
            first = persist_canonical_stock_alert_batch_cursor(
                database_cursor,
                batch,
            )
            second = persist_canonical_stock_alert_batch_cursor(
                database_cursor,
                batch,
            )
            assert first == second
            known_by = max(
                known_by,
                datetime.now(UTC) + timedelta(seconds=1),
            )
            projection = load_canonical_stock_alert_projection_cursor(
                database_cursor,
                StockAlertProjectionRequest(
                    cohort_id=cohort_id,
                    window_id=window_id,
                    input_receipt_id=receipt_id,
                    cutoff=cutoff,
                    input_sha256="b" * 64,
                    known_by=known_by,
                ),
            )
            assert projection["batch"]["batch_sha256"] == (
                batch.batch_sha256
            )
            assert len(projection["evaluations"]) == 2
            aapl = next(
                row
                for row in projection["evaluations"]
                if row["ticker"] == "AAPL"
            )
            assert aapl["disposition"] == "MATCH"
            assert aapl["alert"]["non_actionable"] is False
            database_cursor.execute(
                "SELECT retained_input,input_sha256 "
                "FROM rt.detector_event_inputs "
                "WHERE event_id=%s AND input_role='EQUITY_CONTEXT'",
                (aapl["event_id"],),
            )
            retained_context = database_cursor.fetchone()
            assert (
                retained_context["retained_input"]["source"][
                    "payload_sha256"
                ]
                == retained_context["input_sha256"]
                == aapl["payload"]["context_source_sha256"]
            )
            assert {
                row["role"]
                for row in retained_context["retained_input"][
                    "evidence"
                ]
            } >= {
                "MARKET_BAR",
                "SECTOR_MAPPING",
                "SECTOR_BENCHMARK_REFERENCE",
                "SECTOR_BAR",
                "MARKET_EVENT_COVERAGE",
            }
