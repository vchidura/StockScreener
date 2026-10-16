import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from options.data.polygon_http import PolygonHttpResponse
from realtime.equity_v2_backfill_source import (
    APPROVED_REQUEST_CEILING,
    APPROVED_TICKERS,
    AccountingPolygonTransport,
    build_approved_gap_plan,
)


def test_approved_gap_plan_is_exactly_four_requests_per_ticker():
    plan = build_approved_gap_plan()

    assert plan.expected_provider_requests == APPROVED_REQUEST_CEILING == 4 * len(APPROVED_TICKERS)
    assert plan.expected_count("1d", "ADJUSTED") == 3
    assert plan.expected_count("1wk", "ADJUSTED") == 202
    assert plan.expected_count("1m", "RAW_UNADJUSTED") == 1_170
    assert plan.expected_count("5m", "RAW_UNADJUSTED") == 234
    assert plan.expected_facts_per_ticker == 1_750


def test_accounting_transport_records_response_without_credentials():
    class Transport:
        def get(self, url, params):
            return PolygonHttpResponse(200, json.dumps({"results": [{}, {}]}).encode())

    now = datetime(2026, 10, 15, 15, 0, tzinfo=timezone.utc)
    accounting = AccountingPolygonTransport(
        Transport(),
        cohort_id=uuid4(),
        clock=lambda: now,
        request_ceiling=1,
    )

    accounting.get(
        "https://api.massive.com/v2/aggs/ticker/AAPL/range/1/day/2026-10-12/2026-10-14",
        {"adjusted": "true"},
    )

    assert len(accounting.requests) == 1
    recorded = accounting.requests[0]
    assert recorded.completion.response_bytes > 0
    assert recorded.completion.row_count == 2
    assert "apiKey" not in recorded.reservation.request_sha256
    with pytest.raises(RuntimeError, match="ceiling"):
        accounting.get("https://api.massive.com/x", {})


def test_accounting_transport_replays_success_from_cache_without_provider(tmp_path):
    class Transport:
        calls = 0

        def get(self, url, params):
            self.calls += 1
            return PolygonHttpResponse(200, json.dumps({"results": [{"t": 1}]}).encode())

    now = datetime(2026, 10, 15, 15, 0, tzinfo=timezone.utc)
    delegate = Transport()
    first = AccountingPolygonTransport(
        delegate,
        cohort_id=uuid4(),
        clock=lambda: now,
        cache_directory=tmp_path,
    )
    response = first.get("https://api.massive.com/a", {"adjusted": "true"})
    second = AccountingPolygonTransport(
        delegate,
        cohort_id=first.cohort_id,
        clock=lambda: now,
        cache_directory=tmp_path,
    )
    repeated = second.get("https://api.massive.com/a", {"adjusted": "true"})

    assert response.body == repeated.body
    assert delegate.calls == 1
    assert first.external_requests == 1 and first.cache_hits == 0
    assert second.external_requests == 0 and second.cache_hits == 1
