from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from realtime.advanced_realtime_fencing import (
    DurableLeaseLost,
    OPTIONS_PHYSICAL_SOCKET_RESOURCE,
    RUNTIME_GROUP_RESOURCE,
    assert_durable_runtime_lease_cursor,
    claim_durable_runtime_lease_cursor,
    record_advanced_realtime_service_state_cursor,
    renew_durable_runtime_lease_cursor,
)
from tests.realtime.test_option_slot_sets_v2_db import cursor
from scripts.report_advanced_realtime_v3_status import (
    build_advanced_realtime_v3_status_cursor,
)
from tests.realtime.v2_database import (
    disposable_v2_database,
    requires_v2_database,
)


UTC = timezone.utc
NOW = datetime(2026, 11, 2, 14, 0, tzinfo=UTC)
pytestmark = requires_v2_database


@pytest.mark.parametrize(
    "resource_key",
    (RUNTIME_GROUP_RESOURCE, OPTIONS_PHYSICAL_SOCKET_RESOURCE),
)
def test_native_and_compose_share_one_durable_fence(resource_key):
    with disposable_v2_database(
        f"test_realtime_v2_c8_fence_{uuid4().hex[:8]}"
    ) as pool:
        native_owner = uuid4()
        compose_owner = uuid4()
        with cursor(pool) as database_cursor:
            native = claim_durable_runtime_lease_cursor(
                database_cursor,
                resource_key=resource_key,
                owner_id=native_owner,
                checked_at=NOW,
                lease_seconds=30,
            )
            blocked = claim_durable_runtime_lease_cursor(
                database_cursor,
                resource_key=resource_key,
                owner_id=compose_owner,
                checked_at=NOW + timedelta(seconds=1),
                lease_seconds=30,
            )
            database_cursor.connection.commit()

        assert native is not None
        assert blocked is None

        with cursor(pool) as database_cursor:
            renewed = renew_durable_runtime_lease_cursor(
                database_cursor,
                native,
                checked_at=NOW + timedelta(seconds=10),
                lease_seconds=30,
            )
            database_cursor.connection.commit()

        assert renewed is not None
        assert renewed.owner_epoch == native.owner_epoch

        with cursor(pool) as database_cursor:
            takeover = claim_durable_runtime_lease_cursor(
                database_cursor,
                resource_key=resource_key,
                owner_id=compose_owner,
                checked_at=NOW + timedelta(seconds=41),
                lease_seconds=30,
            )
            old_renewal = renew_durable_runtime_lease_cursor(
                database_cursor,
                renewed,
                checked_at=NOW + timedelta(seconds=41),
                lease_seconds=30,
            )
            database_cursor.connection.commit()

        assert takeover is not None
        assert takeover.owner_id == compose_owner
        assert takeover.owner_epoch == native.owner_epoch + 1
        assert old_renewal is None

        with cursor(pool) as database_cursor:
            with pytest.raises(DurableLeaseLost):
                assert_durable_runtime_lease_cursor(
                    database_cursor,
                    renewed,
                    checked_at=NOW + timedelta(seconds=42),
                )


def test_service_state_is_fenced_by_current_owner_epoch():
    with disposable_v2_database(
        f"test_realtime_v2_c8_service_state_{uuid4().hex[:8]}"
    ) as pool:
        owner = uuid4()
        with cursor(pool) as database_cursor:
            lease = claim_durable_runtime_lease_cursor(
                database_cursor,
                resource_key=RUNTIME_GROUP_RESOURCE,
                owner_id=owner,
                checked_at=NOW,
                lease_seconds=30,
            )
            record_advanced_realtime_service_state_cursor(
                database_cursor,
                role_name="runtime",
                runtime_kind="COMPOSE",
                process_identity="compose:runtime:test",
                state="RUNNING",
                health="HEALTHY",
                lease=lease,
                observed_at=NOW,
                detail={"fixture": "c8"},
            )
            database_cursor.execute(
                "SELECT owner_id::text,owner_epoch,state,health "
                "FROM rt.advanced_realtime_service_states "
                "WHERE role_name='runtime'"
            )
            stored = database_cursor.fetchone()
            status = build_advanced_realtime_v3_status_cursor(
                database_cursor,
                checked_at=NOW,
            )
            database_cursor.connection.commit()

        assert stored == (
            str(owner),
            lease.owner_epoch,
            "RUNNING",
            "HEALTHY",
        )
        assert status["runtime_services"][0]["role"] == "runtime"
        assert status["runtime_services"][0]["health"] == "HEALTHY"
