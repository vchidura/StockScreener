from datetime import date, datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from realtime.acquisition_ledger import (
    ProviderRequestLane,
    ProviderRequestReservation,
)
from realtime.advanced_ingress_repository import (
    reserve_advanced_incremental_request_cursor,
)
from realtime.provider_budget import (
    ProviderBudgetExceeded,
    ProviderBudgetScope,
    account_provider_budget_status_cursor,
    provider_budget_status_cursor,
    reserve_coordinated_provider_request_cursor,
)


NOW = datetime(2026, 10, 16, 14, tzinfo=timezone.utc)


def _scope(**changes):
    values = dict(
        account_alias="MASSIVE_DEVELOPER",
        session_date=date(2026, 10, 16),
        authorization_reference="standing-v1",
        provider_authorization_reference="subscription",
        expected_cost="no incremental cost",
        request_ceiling=2,
        decoded_byte_ceiling=4_002,
        lanes=frozenset({
            ProviderRequestLane.EQUITY_CURRENT,
            ProviderRequestLane.EQUITY_HISTORY,
        }),
        authorized_until=datetime(
            2026, 10, 17, tzinfo=timezone.utc
        ),
    )
    values.update(changes)
    return ProviderBudgetScope(**values)


def _reservation(**changes):
    values = dict(
        request_id=uuid4(),
        account_alias="MASSIVE_DEVELOPER",
        lane=ProviderRequestLane.EQUITY_CURRENT,
        endpoint_class="EQUITY_INCREMENTAL",
        request_sha256="a" * 64,
        reserved_at=NOW,
    )
    values.update(changes)
    return ProviderRequestReservation(**values)


def test_scope_binds_authorization_and_exact_account_limits():
    scope = _scope()
    bound = scope.bind(_reservation(), reserved_response_bytes=2_001)

    assert bound.budget_scope_sha256 == scope.sha256
    assert bound.budget_session_date == scope.session_date
    assert bound.authorization_reference == "standing-v1"
    assert bound.account_request_ceiling == 2
    with pytest.raises(ValueError, match="outside"):
        scope.bind(
            _reservation(lane=ProviderRequestLane.OPTIONS_CHAIN),
            reserved_response_bytes=1,
        )


def test_scope_separates_local_allowance_from_shared_account_limit():
    scope = _scope(
        account_request_ceiling=10,
        account_decoded_byte_ceiling=20_000,
    )
    bound = scope.bind(
        _reservation(),
        reserved_response_bytes=2_001,
    )

    assert scope.request_ceiling == 2
    assert scope.effective_account_request_ceiling == 10
    assert bound.account_request_ceiling == 10
    assert bound.account_decoded_byte_ceiling == 20_000


def test_admission_reserves_under_account_lock_and_returns_updated_status():
    cursor = MagicMock()
    reservation = _reservation()
    stored = (
        str(reservation.request_id),
        "MASSIVE_DEVELOPER",
        "EQUITY_CURRENT",
        None,
        None,
        None,
        "EQUITY_INCREMENTAL",
        "a" * 64,
        1,
        _scope().sha256,
        date(2026, 10, 16),
        "standing-v1",
        2_001,
        2,
        4_002,
    )
    cursor.fetchone.side_effect = [
        None,
        (0, 0, 0, 0, 0, 0, None, None, None, None),
        stored,
    ]

    admission = reserve_coordinated_provider_request_cursor(
        cursor,
        _scope(),
        reservation,
        reserved_response_bytes=2_001,
    )

    assert admission.newly_reserved
    assert admission.status.requests_reserved == 1
    assert admission.status.bytes_reserved == 2_001
    assert "pg_advisory_xact_lock" in cursor.execute.call_args_list[0].args[0]


def test_admission_rejects_request_or_byte_ceiling_exhaustion():
    for status, message in (
        ((2, 2_000, 0, 2, 0, 0, 2, 2, 4_002, 4_002), "request"),
        ((1, 3_000, 0, 1, 0, 0, 2, 2, 4_002, 4_002), "decoded-byte"),
    ):
        cursor = MagicMock()
        cursor.fetchone.side_effect = [None, status]
        with pytest.raises(ProviderBudgetExceeded, match=message):
            reserve_coordinated_provider_request_cursor(
                cursor,
                _scope(),
                _reservation(),
                reserved_response_bytes=2_001,
            )


def test_status_refuses_conflicting_scope_ceilings():
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        1,
        2_001,
        0,
        1,
        0,
        0,
        2,
        3,
        4_002,
        4_002,
    )
    with pytest.raises(ValueError, match="ceilings conflict"):
        provider_budget_status_cursor(cursor, _scope())


def test_status_is_bound_to_the_exact_authorized_budget_scope():
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        0,
        0,
        0,
        0,
        0,
        0,
        None,
        None,
        None,
        None,
    )
    scope = _scope()

    status = provider_budget_status_cursor(cursor, scope)

    assert status.requests_reserved == 0
    query, parameters = cursor.execute.call_args.args
    assert "budget_scope_sha256=%s" in query
    assert parameters == (
        scope.account_alias,
        scope.session_date,
        scope.sha256,
    )


def test_account_status_includes_all_coordinated_scopes_and_states():
    cursor = MagicMock()
    cursor.fetchone.return_value = (
        4,
        3_500,
        1,
        1,
        1,
        1,
        10,
        10,
        10_000,
        10_000,
    )

    status = account_provider_budget_status_cursor(
        cursor,
        _scope(request_ceiling=10, decoded_byte_ceiling=10_000),
    )

    assert status.requests_reserved == 4
    assert status.bytes_reserved == 3_500
    assert (status.reserved, status.complete, status.failed, status.ambiguous) == (
        1,
        1,
        1,
        1,
    )
    query, parameters = cursor.execute.call_args.args
    assert "budget_scope_sha256 IS NOT NULL" in query
    assert "budget_scope_sha256=%s" not in query
    assert parameters == ("MASSIVE_DEVELOPER", date(2026, 10, 16))


def test_advanced_incremental_budget_exhaustion_blocks_before_reservation():
    cursor = MagicMock()
    plan_id = uuid4()
    cohort_id = uuid4()
    scope = _scope(
        account_alias="MASSIVE_ADVANCED",
        provider_authorization_reference="stocks-advanced",
        lanes=frozenset({ProviderRequestLane.EQUITY_HISTORY}),
    )
    reservation = _reservation(
        account_alias="MASSIVE_ADVANCED",
        lane=ProviderRequestLane.EQUITY_HISTORY,
        cohort_id=cohort_id,
    )
    cursor.fetchone.side_effect = [
        (
            cohort_id,
            scope.session_date,
            scope.request_ceiling,
            scope.decoded_byte_ceiling,
            scope.authorized_until,
            "ACTIVE",
            "READY",
            scope.authorization_reference,
            scope.request_ceiling,
            0,
        ),
        None,
    ]

    with pytest.raises(ProviderBudgetExceeded, match="budget is exhausted"):
        reserve_advanced_incremental_request_cursor(
            cursor,
            scope,
            reservation,
            plan_id=plan_id,
            reserved_response_bytes=2_001,
        )

    assert not any(
        "UPDATE rt.advanced_ingress_plan_usage" in call.args[0]
        for call in cursor.execute.call_args_list
    )


def test_advanced_repair_rejects_exhausted_account_without_usage_update():
    cursor = MagicMock()
    plan_id = uuid4()
    cohort_id = uuid4()
    scope = _scope(
        account_alias="MASSIVE_ADVANCED",
        provider_authorization_reference="stocks-advanced",
        request_ceiling=2,
        decoded_byte_ceiling=4_002,
        account_request_ceiling=3,
        account_decoded_byte_ceiling=10_000,
        lanes=frozenset({ProviderRequestLane.EQUITY_HISTORY}),
    )
    reservation = _reservation(
        account_alias="MASSIVE_ADVANCED",
        lane=ProviderRequestLane.EQUITY_HISTORY,
        cohort_id=cohort_id,
    )
    cursor.fetchone.side_effect = [
        (
            cohort_id,
            scope.session_date,
            scope.request_ceiling,
            scope.decoded_byte_ceiling,
            scope.authorized_until,
            "ACTIVE",
            "READY",
            scope.authorization_reference,
            0,
            0,
        ),
        None,
        None,
        (0, 0, 0, 0, 0, 0, None, None, None, None),
        (3, 6_003, 0, 3, 0, 0, 3, 3, 10_000, 10_000),
    ]

    with pytest.raises(
        ProviderBudgetExceeded,
        match="account request ceiling",
    ):
        reserve_advanced_incremental_request_cursor(
            cursor,
            scope,
            reservation,
            plan_id=plan_id,
            reserved_response_bytes=2_001,
        )

    statements = [
        call.args[0] for call in cursor.execute.call_args_list
    ]
    assert not any(
        "INSERT INTO rt.provider_request_ledger" in statement
        for statement in statements
    )
    assert not any(
        "UPDATE rt.advanced_ingress_plan_usage" in statement
        for statement in statements
    )
