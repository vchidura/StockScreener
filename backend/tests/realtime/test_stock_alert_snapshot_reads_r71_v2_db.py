"""R71 (Nov 15): the receipt list, claim and state check SQL against the real schema."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import psycopg2
from psycopg2.extras import Json, RealDictCursor

from realtime.canonical_stock_alert_repository import (
    canonical_stock_alert_state_fingerprint_cursor,
    claim_canonical_stock_alert_receipt_cursor,
    list_due_canonical_stock_alert_receipts_cursor,
    load_due_canonical_stock_alert_receipts_cursor,
)
from tests.realtime.v2_database import (
    disposable_v2_database,
    insert_cohort,
    insert_service_plan,
    requires_v2_database,
)


pytestmark = requires_v2_database
UTC = timezone.utc
DETECTOR = "d" * 64


def _seed(cursor, now):
    cohort_id = insert_cohort(cursor, now)
    plan_id = insert_service_plan(cursor, cohort_id, ["EQUITY_EVIDENCE", "STOCK_ALERTS"], now)
    receipts = []
    for minutes in (5, 10):
        window_id, receipt_id = uuid4(), uuid4()
        cutoff = now + timedelta(minutes=minutes)
        cursor.execute(
            "INSERT INTO rt.window_runs(window_id,plan_id,cohort_id,provider_tier,window_key,"
            "release_at,cutoff,deadline,state) VALUES(%s,%s,%s,'MASSIVE_ADVANCED',%s,%s,%s,%s,'COMPLETE')",
            (str(window_id), str(plan_id), str(cohort_id), f"r71-{window_id}", cutoff, cutoff,
             cutoff + timedelta(minutes=5)),
        )
        cursor.execute(
            "INSERT INTO rt.cycle_receipts(receipt_id,window_id,cohort_id,receipt_kind,cutoff,"
            "reference_generation,universe_sha256,input_sha256,build_id,created_at,known_by) VALUES("
            "%s,%s,%s,'INPUT_READY',%s,1,%s,%s,'r71-test-v1',%s,%s)",
            (str(receipt_id), str(window_id), str(cohort_id), cutoff, "a" * 64, "b" * 64, cutoff, cutoff),
        )
        cursor.execute(
            "INSERT INTO rt.stage_runs (stage_run_id,window_id,stage_id,stage_version,state,finished_at) "
            "SELECT %s,%s,stage_id,version,'COMPLETE',%s FROM rt.stage_catalog "
            "WHERE stage_id='EQUITY_MATERIALIZE' ORDER BY version DESC LIMIT 1",
            (str(uuid4()), str(window_id), cutoff),
        )
        receipts.append((str(receipt_id), str(window_id)))
    cursor.execute(
        "INSERT INTO rt.detector_versions (detector_sha256,detector_id,version,code_sha256,policy_sha256,"
        "requirements,output_kind) VALUES (%s,'STOCK_ALERT','r71-v1',%s,%s,'{}'::jsonb,'ALERT')",
        (DETECTOR, "1" * 64, "2" * 64),
    )
    return cohort_id, receipts


def _state(cursor, cohort_id, key, revision, now):
    cursor.execute(
        "INSERT INTO rt.detector_state(detector_sha256,cohort_id,state_key,revision,state_schema,state_sha256,"
        "state_payload,updated_at) VALUES(%s,%s,%s,%s,'r71-state-v1',%s,%s,%s) "
        "ON CONFLICT(detector_sha256,cohort_id,state_key) DO UPDATE SET revision=EXCLUDED.revision,"
        "state_sha256=EXCLUDED.state_sha256,state_payload=EXCLUDED.state_payload,updated_at=EXCLUDED.updated_at",
        (DETECTOR, str(cohort_id), key, revision, f"{revision:064x}", Json({"revision": revision}), now),
    )


def test_list_claim_and_state_fingerprint_on_the_real_schema():
    now = datetime(2026, 11, 13, 14, 30, tzinfo=UTC)
    with disposable_v2_database("test_realtime_v2_r71_" + uuid4().hex[:8]) as pool:
        first = pool.getconn()
        second = pool.getconn()
        try:
            with first.cursor(cursor_factory=RealDictCursor) as cursor:
                cohort_id, receipts = _seed(cursor, now)
                _state(cursor, cohort_id, "AAPL", 1, now)
            first.commit()
            session_date = now.date()

            # The snapshot list sees both receipts in cutoff order, inside a READ ONLY transaction (no row lock).
            first.set_session(isolation_level="REPEATABLE READ", readonly=True)
            with first.cursor(cursor_factory=RealDictCursor) as cursor:
                listed = list_due_canonical_stock_alert_receipts_cursor(
                    cursor, cohort_id=cohort_id, session_date=session_date, limit=8)
                built_from = canonical_stock_alert_state_fingerprint_cursor(
                    cursor, detector_sha256=DETECTOR, cohort_id=cohort_id)
            first.commit()
            first.set_session(isolation_level="SERIALIZABLE", readonly=False)
            assert [row["receipt_id"] for row in listed] == [item[0] for item in receipts]

            # Another transaction holds the first receipt's window row: the claim skips it (SKIP LOCKED).
            with second.cursor() as holder:
                holder.execute("SELECT 1 FROM rt.window_runs WHERE window_id=%s FOR UPDATE", (receipts[0][1],))
                with first.cursor(cursor_factory=RealDictCursor) as cursor:
                    assert claim_canonical_stock_alert_receipt_cursor(
                        cursor, receipt_id=receipts[0][0], cohort_id=cohort_id, session_date=session_date) is None
                first.rollback()
            second.rollback()

            # Free again: claimed; the state check equals the snapshot's while nothing changed.
            with first.cursor(cursor_factory=RealDictCursor) as cursor:
                claimed = claim_canonical_stock_alert_receipt_cursor(
                    cursor, receipt_id=receipts[0][0], cohort_id=cohort_id, session_date=session_date)
                assert claimed is not None and claimed["receipt_id"] == receipts[0][0]
                assert canonical_stock_alert_state_fingerprint_cursor(
                    cursor, detector_sha256=DETECTOR, cohort_id=cohort_id) == built_from
                cursor.execute(
                    "SELECT count(*) AS n FROM pg_locks WHERE mode='SIReadLock' AND pid=pg_backend_pid()")
                assert cursor.fetchone()["n"] < 64
            first.rollback()

            # A state written after the snapshot changes the fingerprint (the batch would be rebuilt).
            with second.cursor() as writer:
                _state(writer, cohort_id, "AAPL", 2, now + timedelta(minutes=1))
            second.commit()
            with first.cursor(cursor_factory=RealDictCursor) as cursor:
                assert canonical_stock_alert_state_fingerprint_cursor(
                    cursor, detector_sha256=DETECTOR, cohort_id=cohort_id) != built_from
                # The locked r102 query still returns the same due receipts.
                locked = load_due_canonical_stock_alert_receipts_cursor(
                    cursor, cohort_id=cohort_id, session_date=session_date, limit=8)
                assert [row["receipt_id"] for row in locked] == [item[0] for item in receipts]
            first.rollback()
        finally:
            for connection in (first, second):
                if not connection.closed:
                    connection.rollback()
                pool.putconn(connection)
