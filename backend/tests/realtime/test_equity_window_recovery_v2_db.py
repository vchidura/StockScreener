from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from realtime.orchestration_repository import (
    EQUITY_WINDOW_BUILD_ID, equity_window_recovery_cursor,
)
from v2_database import disposable_v2_database, insert_cohort, requires_v2_database

pytestmark = requires_v2_database
CUTOFF = datetime(2026, 10, 15, 20, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def pool():
    with disposable_v2_database(f"test_realtime_v2_recovery_{uuid4().hex[:10]}") as value:
        yield value


@pytest.mark.parametrize("state,wrong_universe", (
    ("COMPLETE", False), ("FAILED", False), ("COMPLETE", True),
))
def test_recovery_uses_matching_durable_receipt_not_window_status_alone(
    pool, state, wrong_universe
):
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cohort = insert_cohort(cursor, CUTOFF - timedelta(hours=8))
            cursor.execute(
                "SELECT manifest.universe_sha256 FROM rt.cohorts cohort "
                "JOIN rt.subscription_manifests manifest USING (manifest_id) "
                "WHERE cohort.cohort_id=%s",
                (str(cohort),),
            )
            universe = cursor.fetchone()[0]
            plan, window = uuid4(), uuid4()
            cursor.execute(
                "INSERT INTO rt.service_plans (plan_id,cohort_id,session_date,"
                "stage_catalog_version,enabled_stages,config_sha256,deadline,scope_reference) "
                "VALUES (%s,%s,%s,'stages_v2',ARRAY['EQUITY_INGEST'],%s,%s,'fixture')",
                (str(plan), str(cohort), CUTOFF.date(), "a" * 64, CUTOFF + timedelta(hours=1)),
            )
            cursor.execute(
                "INSERT INTO rt.window_runs (window_id,plan_id,cohort_id,provider_tier,"
                "window_key,release_at,cutoff,deadline,state,updated_at) "
                "VALUES (%s,%s,%s,'MASSIVE_DEVELOPER',%s,%s,%s,%s,%s,%s)",
                (
                    str(window), str(plan), str(cohort), CUTOFF.isoformat(),
                    CUTOFF + timedelta(minutes=15), CUTOFF,
                    CUTOFF + timedelta(minutes=20), state, CUTOFF + timedelta(minutes=16),
                ),
            )
            cursor.execute(
                "INSERT INTO rt.cycle_receipts (receipt_id,window_id,cohort_id,receipt_kind,"
                "cutoff,reference_generation,universe_sha256,input_sha256,build_id) "
                "VALUES (%s,%s,%s,'INPUT_READY',%s,1,%s,%s,%s)",
                (
                    str(uuid4()), str(window), str(cohort), CUTOFF,
                    "0" * 64 if wrong_universe else universe, "b" * 64,
                    EQUITY_WINDOW_BUILD_ID,
                ),
            )
            if wrong_universe:
                with pytest.raises(ValueError, match="does not match"):
                    equity_window_recovery_cursor(cursor, cohort, CUTOFF)
            else:
                result = equity_window_recovery_cursor(cursor, cohort, CUTOFF)
                assert result["has_input_receipt"]
                assert result["input_receipt"].input_sha256 == "b" * 64
                assert result["input_known_by"] == CUTOFF + timedelta(minutes=20)
                assert result["complete"] == (state == "COMPLETE")
                if state == "FAILED":
                    assert result["retry_not_before"] == CUTOFF + timedelta(minutes=21)
    finally:
        connection.rollback()
        pool.putconn(connection)
