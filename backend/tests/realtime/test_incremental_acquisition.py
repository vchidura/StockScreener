from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

from realtime.incremental_acquisition import (
    IncrementalEquitySource,
    IncrementalRequestPlan,
    latest_qualified_cutoff,
    plan_incremental_requests_cursor,
)
from realtime.acquisition_ledger import ProviderRequestLane
from realtime.provider_budget import ProviderBudgetScope


UTC = timezone.utc
COHORT = UUID("11111111-1111-1111-1111-111111111111")


def _identity(instrument_id, ticker):
    return SimpleNamespace(
        instrument_id=instrument_id,
        ticker=ticker,
        security=SimpleNamespace(security_id=UUID(int=instrument_id)),
    )


def test_qualified_cutoff_tracks_interval_and_provider_delay():
    known = datetime(2026, 10, 16, 15, 2, tzinfo=UTC)

    assert latest_qualified_cutoff(
        date(2026, 10, 16),
        "1m",
        known_at=known,
        provider_delay=timedelta(minutes=15),
    ) == datetime(2026, 10, 16, 14, 47, tzinfo=UTC)
    assert latest_qualified_cutoff(
        date(2026, 10, 16),
        "5m",
        known_at=known,
        provider_delay=timedelta(minutes=15),
    ) == datetime(2026, 10, 16, 14, 45, tzinfo=UTC)


def test_initial_startup_plans_session_open_to_latest_cutoff():
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    cutoff = datetime(2026, 10, 16, 15, 0, tzinfo=UTC)

    plans = plan_incremental_requests_cursor(
        cursor,
        cohort_id=COHORT,
        identities=(_identity(1, "AAPL"), _identity(2, "MSFT")),
        session_date=date(2026, 10, 16),
        interval="5m",
        cutoff=cutoff,
    )

    assert len(plans) == 2
    assert {plan.requested_from for plan in plans} == {
        datetime(2026, 10, 16, 13, 30, tzinfo=UTC)
    }
    assert all(plan.requested_to == cutoff for plan in plans)
    assert all(plan.expected_bars == 18 for plan in plans)


def test_complete_watermark_uses_bounded_overlap_and_gap_rewinds_to_complete():
    cutoff = datetime(2026, 10, 16, 15, 0, tzinfo=UTC)
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        (
            1,
            datetime(2026, 10, 16, 14, 58, tzinfo=UTC),
            datetime(2026, 10, 16, 14, 58, tzinfo=UTC),
            "COMPLETE",
        ),
        (
            2,
            datetime(2026, 10, 16, 14, 50, tzinfo=UTC),
            datetime(2026, 10, 16, 14, 59, tzinfo=UTC),
            "GAP",
        ),
    ]

    plans = plan_incremental_requests_cursor(
        cursor,
        cohort_id=COHORT,
        identities=(_identity(1, "AAPL"), _identity(2, "MSFT")),
        session_date=date(2026, 10, 16),
        interval="1m",
        cutoff=cutoff,
    )

    assert plans[0].requested_from == datetime(
        2026, 10, 16, 14, 56, tzinfo=UTC
    )
    assert plans[1].requested_from == datetime(
        2026, 10, 16, 14, 48, tzinfo=UTC
    )
    assert plans[1].prior_examined_bar_end > plans[1].prior_complete_bar_end


def test_fully_complete_member_needs_no_request():
    cursor = MagicMock()
    cutoff = datetime(2026, 10, 16, 15, 0, tzinfo=UTC)
    cursor.fetchall.return_value = [(1, cutoff, cutoff, "COMPLETE")]

    assert plan_incremental_requests_cursor(
        cursor,
        cohort_id=COHORT,
        identities=(_identity(1, "AAPL"),),
        session_date=date(2026, 10, 16),
        interval="1m",
        cutoff=cutoff,
    ) == ()


def test_retry_attempt_has_a_distinct_provider_request_identity():
    start = datetime(2026, 10, 16, 13, 30, tzinfo=UTC)
    end = start + timedelta(minutes=5)
    identity = _identity(1, "SPY")
    plan = IncrementalRequestPlan(
        COHORT,
        1,
        identity.security.security_id,
        "SPY",
        date(2026, 10, 16),
        "1m",
        start,
        end,
        2,
        None,
        None,
        None,
    )
    scope = ProviderBudgetScope(
        account_alias="MASSIVE_ADVANCED",
        session_date=date(2026, 10, 16),
        authorization_reference="owner-approved",
        provider_authorization_reference="stocks-advanced",
        expected_cost="standing allocation",
        request_ceiling=2,
        decoded_byte_ceiling=4_098,
        lanes=frozenset({ProviderRequestLane.EQUITY_HISTORY}),
        authorized_until=datetime(2026, 10, 17, tzinfo=UTC),
    )

    def source(attempt=1):
        return IncrementalEquitySource(
            SimpleNamespace(),
            SimpleNamespace(),
            scope,
            (identity,),
            (plan,),
            response_byte_limit=2_048,
            clock=lambda: end,
            endpoint_class="MASSIVE_STOCKS_AGGREGATES_1M",
            eligibility_policy_version="advanced-real-time-closed-bar-v1",
            request_attempt=attempt,
        )

    first = source(1)._request_identity(plan)
    default = source()._request_identity(plan)
    second = source(2)._request_identity(plan)

    assert first == default
    assert first[:3] != second[:3]
    assert first[3] != second[3]
