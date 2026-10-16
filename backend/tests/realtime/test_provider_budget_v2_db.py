from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from threading import Barrier
from uuid import uuid4

import pytest

from realtime.acquisition_ledger import (
    ProviderRequestLane,
    ProviderRequestReservation,
)
from realtime.provider_budget import (
    ProviderBudgetExceeded,
    ProviderBudgetScope,
    reserve_coordinated_provider_request_cursor,
)
from v2_database import disposable_v2_database, requires_v2_database


pytestmark = requires_v2_database


@pytest.fixture
def pool():
    with disposable_v2_database(
        f"test_realtime_v2_budget_{uuid4().hex[:8]}"
    ) as value:
        yield value


def test_concurrent_v2_lanes_cannot_spend_the_same_last_reservation(pool):
    session = date(2026, 10, 16)
    scope = ProviderBudgetScope(
        account_alias="MASSIVE_DEVELOPER",
        session_date=session,
        authorization_reference="standing-v1",
        provider_authorization_reference="subscription",
        expected_cost="no incremental cost",
        request_ceiling=1,
        decoded_byte_ceiling=2_001,
        lanes=frozenset({
            ProviderRequestLane.EQUITY_CURRENT,
            ProviderRequestLane.REFERENCE,
        }),
        authorized_until=datetime(
            2026, 10, 17, tzinfo=timezone.utc
        ),
    )
    barrier = Barrier(2)

    def reserve(lane):
        connection = pool.getconn()
        try:
            barrier.wait(timeout=10)
            with connection.cursor() as cursor:
                admission = reserve_coordinated_provider_request_cursor(
                    cursor,
                    scope,
                    ProviderRequestReservation(
                        request_id=uuid4(),
                        account_alias=scope.account_alias,
                        lane=lane,
                        endpoint_class="BUDGET_TEST",
                        request_sha256=uuid4().hex * 2,
                        reserved_at=datetime.now(timezone.utc),
                    ),
                    reserved_response_bytes=2_001,
                )
            connection.commit()
            return ("RESERVED", admission.request_id)
        except ProviderBudgetExceeded:
            connection.rollback()
            return ("BLOCKED", None)
        finally:
            pool.putconn(connection)

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(
            reserve,
            (
                ProviderRequestLane.EQUITY_CURRENT,
                ProviderRequestLane.REFERENCE,
            ),
        ))

    assert sorted(row[0] for row in outcomes) == ["BLOCKED", "RESERVED"]
    connection = pool.getconn()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT budget_scope_sha256, budget_session_date, "
                "authorization_reference, reserved_response_bytes, "
                "account_request_ceiling, account_decoded_byte_ceiling "
                "FROM rt.provider_request_ledger"
            )
            rows = tuple(cursor.fetchall())
        connection.rollback()
    finally:
        pool.putconn(connection)
    assert rows == ((
        scope.sha256,
        session,
        scope.authorization_reference,
        2_001,
        1,
        2_001,
    ),)
