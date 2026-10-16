from datetime import date, datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from realtime.acquisition_ledger import (
    ProviderRequestLane,
    ProviderRequestReservation,
)
from realtime.provider_budget import (
    ProviderBudgetExceeded,
    ProviderBudgetScope,
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


def test_status_refuses_conflicting_account_ceilings():
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
