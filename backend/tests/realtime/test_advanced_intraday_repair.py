from datetime import date, datetime, timezone
from unittest.mock import MagicMock
from uuid import UUID

from realtime.acquisition_ledger import (
    PriceBasis,
    ProviderRequestCompletion,
    ProviderRequestLane,
    ProviderRequestOutcome,
    ProviderRequestReservation,
    ProviderRequestState,
    RecordedProviderRequest,
    RequestCompleteness,
)
from realtime.provider_budget import ProviderBudgetScope
from scripts import repair_realtime_v2_advanced_intraday as repair


UTC = timezone.utc
NOW = datetime(2026, 10, 16, 18, 20, tzinfo=UTC)
REQUEST_ID = UUID("b797dcef-dd8f-5b03-84b0-87f4d3d7dfbe")
COHORT_ID = UUID("713278f5-e83d-55f5-98c3-c930be03e9c8")
PAYLOAD_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")


def test_complete_request_replays_before_budget_scope_rebinding(monkeypatch):
    request_sha256 = "1" * 64
    authorization_reference = (
        "advanced-equity-v2-20261016:intraday-rest-repair-retry1"
    )
    scope = ProviderBudgetScope(
        account_alias="MASSIVE_ADVANCED",
        session_date=date(2026, 10, 16),
        authorization_reference=authorization_reference,
        provider_authorization_reference="stocks-advanced",
        expected_cost="no incremental charge expected",
        request_ceiling=2,
        decoded_byte_ceiling=8_000_000,
        lanes=frozenset({ProviderRequestLane.EQUITY_CURRENT}),
        authorized_until=NOW.replace(hour=23, minute=59, second=59),
    )
    reservation = ProviderRequestReservation(
        request_id=REQUEST_ID,
        account_alias=scope.account_alias,
        lane=ProviderRequestLane.EQUITY_CURRENT,
        endpoint_class="ADVANCED_INTRADAY_REPAIR",
        request_sha256=request_sha256,
        reserved_at=NOW,
        cohort_id=COHORT_ID,
    )
    recorded_reservation = ProviderRequestReservation(
        request_id=REQUEST_ID,
        account_alias=scope.account_alias,
        lane=ProviderRequestLane.EQUITY_CURRENT,
        endpoint_class="ADVANCED_INTRADAY_REPAIR",
        request_sha256=request_sha256,
        reserved_at=NOW,
        cohort_id=COHORT_ID,
        budget_scope_sha256="2" * 64,
        budget_session_date=scope.session_date,
        authorization_reference=authorization_reference,
        reserved_response_bytes=repair.RESERVED_BYTES_PER_REQUEST,
        account_request_ceiling=scope.request_ceiling,
        account_decoded_byte_ceiling=scope.decoded_byte_ceiling,
    )
    completion = ProviderRequestCompletion(
        state=ProviderRequestState.COMPLETE,
        completed_at=NOW,
        started_at=NOW,
        outcome=ProviderRequestOutcome.OK,
        http_status=200,
        response_bytes=2,
        row_count=285,
        pages_fetched=1,
        requested_from=datetime(2026, 10, 16, 13, 30, tzinfo=UTC),
        requested_to=datetime(2026, 10, 16, 18, 15, tzinfo=UTC),
        price_basis=PriceBasis.RAW_FINALIZED_MINUTE_CLOSE_V1,
        completeness=RequestCompleteness.COMPLETE,
        payload_id=PAYLOAD_ID,
    )
    recorded = RecordedProviderRequest(recorded_reservation, completion)
    load_recorded = MagicMock(return_value=recorded)
    load_payload = MagicMock(return_value=b"{}")
    reserve = MagicMock()
    monkeypatch.setattr(
        repair,
        "load_recorded_provider_request_cursor",
        load_recorded,
    )
    monkeypatch.setattr(repair, "load_raw_payload_cursor", load_payload)
    monkeypatch.setattr(
        repair,
        "reserve_coordinated_provider_request_cursor",
        reserve,
    )

    admission, replay = repair._reserve_or_load_sealed_response_cursor(
        MagicMock(),
        scope,
        reservation,
    )

    assert admission is None
    assert replay == (recorded, b"{}")
    reserve.assert_not_called()
