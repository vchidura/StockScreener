import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from equity.domain import BarAvailabilityMode
from equity.polygon import (
    PolygonEquityClient,
    conservative_filing_availability,
    normalize_fundamental_reports,
    normalize_corporate_actions,
    normalize_grouped_daily_bars,
    normalize_native_bars,
    normalize_security_reference,
    sha256_json,
)


UTC = timezone.utc


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, *, params, headers, timeout):
        self.calls.append((url, params, headers, timeout))
        return self.responses.pop(0)


def ticker_payload():
    return {
        "active": True,
        "cik": "0000320193",
        "composite_figi": "BBG000B9XRY4",
        "market_cap": 2_800_000_000_000,
        "name": "Apple Inc.",
        "primary_exchange": "XNAS",
        "share_class_figi": "BBG001S5N8V8",
        "sic_code": "3571",
        "sic_description": "ELECTRONIC COMPUTERS",
        "ticker": "AAPL",
        "type": "CS",
        "weighted_shares_outstanding": 15_000_000_000,
    }


def security():
    return normalize_security_reference(
        ticker_payload(),
        observed_at=datetime(2026, 8, 10, 20, 0, tzinfo=UTC),
        source_as_of_date=date(2026, 8, 10),
        float_payload={"free_float": 14_500_000_000, "free_float_percent": 96.67},
    )


def test_client_fetches_reference_with_bearer_auth():
    session = FakeSession([FakeResponse({"results": ticker_payload()})])
    client = PolygonEquityClient("test-key", session=session)

    result = client.fetch_ticker_overview("aapl", as_of_date=date(2026, 8, 10))

    assert result["name"] == "Apple Inc."
    url, params, headers, timeout = session.calls[0]
    assert url.endswith("/v3/reference/tickers/AAPL")
    assert params == {"date": "2026-08-10"}
    assert headers == {"Authorization": "Bearer test-key"}
    assert timeout == 30


def test_client_paginates_only_on_approved_hosts():
    session = FakeSession([
        FakeResponse({
            "results": [{"ticker": "AAPL"}],
            "next_url": "https://api.massive.com/stocks/vX/float?cursor=next",
        }),
        FakeResponse({"results": [{"ticker": "MSFT"}]}),
    ])
    client = PolygonEquityClient("test-key", session=session)

    rows = client.fetch_float(["AAPL", "MSFT"])

    assert [row["ticker"] for row in rows] == ["AAPL", "MSFT"]
    assert session.calls[1][1] == {}
    assert session.calls[1][2] == {"Authorization": "Bearer test-key"}

    with pytest.raises(ValueError, match="approved HTTPS host"):
        PolygonEquityClient._validate_url("https://example.com/leak")


def test_client_lists_historical_common_stocks_with_pagination():
    session = FakeSession([
        FakeResponse({
            "results": [{"ticker": "AAPL", "type": "CS"}],
            "next_url": "https://api.massive.com/v3/reference/tickers?cursor=next",
        }),
        FakeResponse({"results": [{"ticker": "MSFT", "type": "CS"}]}),
    ])
    client = PolygonEquityClient("test-key", session=session)

    rows = client.fetch_tickers_as_of(date(2024, 1, 2))

    assert [row["ticker"] for row in rows] == ["AAPL", "MSFT"]
    assert session.calls[0][1] == {
        "date": "2024-01-02", "market": "stocks", "type": "CS",
        "active": "true", "limit": 1000, "sort": "ticker", "order": "asc",
    }
    assert session.calls[1][1] == {}


def test_client_fetches_unadjusted_grouped_daily_rows():
    session = FakeSession([FakeResponse({
        "results": [{"T": "AAPL", "c": 200.0, "v": 1_000_000}]
    })])
    client = PolygonEquityClient("test-key", session=session)

    rows = client.fetch_grouped_daily(date(2024, 1, 2))

    assert rows[0]["T"] == "AAPL"
    url, params, headers, _ = session.calls[0]
    assert url.endswith("/v2/aggs/grouped/locale/us/market/stocks/2024-01-02")
    assert params == {"adjusted": "false"}
    assert headers == {"Authorization": "Bearer test-key"}


def test_client_fetches_bounded_splits_and_dividends():
    session = FakeSession([
        FakeResponse({"results": []}), FakeResponse({"results": []}),
    ])
    client = PolygonEquityClient("test-key", session=session)

    client.fetch_splits(date(2024, 1, 1), date(2024, 1, 31))
    client.fetch_dividends(date(2024, 1, 1), date(2024, 1, 31))

    assert session.calls[0][1]["execution_date.gte"] == "2024-01-01"
    assert session.calls[0][1]["execution_date.lte"] == "2024-01-31"
    assert session.calls[1][1]["ex_dividend_date.gte"] == "2024-01-01"
    assert session.calls[1][1]["ex_dividend_date.lte"] == "2024-01-31"


def test_normalized_split_is_replay_explicit_and_idempotent():
    security_id = uuid4()
    observed_at = datetime(2026, 8, 31, tzinfo=UTC)
    row = {
        "id": "split-1", "ticker": "AAPL", "execution_date": "2024-01-10",
        "split_from": 1, "split_to": 2,
    }

    first = normalize_corporate_actions(
        [row], security_ids={"AAPL": security_id}, action_type="SPLIT",
        observed_at=observed_at,
        availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
    )[0]
    second = normalize_corporate_actions(
        [row], security_ids={"AAPL": security_id}, action_type="SPLIT",
        observed_at=observed_at + timedelta(minutes=1),
        availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
    )[0]

    assert first.corporate_action_id == second.corporate_action_id
    assert first.replay_available_at == datetime(2024, 1, 10, tzinfo=UTC)
    assert first.split_from == 1 and first.split_to == 2


def test_grouped_daily_bar_uses_session_bounds_and_replay_availability():
    security_id = uuid4()
    observed_at = datetime(2026, 8, 31, tzinfo=UTC)

    result = normalize_grouped_daily_bars(
        [{
            "T": "AAPL", "o": 200, "h": 205, "l": 198, "c": 204,
            "v": 1_000_000, "vw": 202, "n": 1000,
        }],
        session_date=date(2024, 1, 2),
        security_ids={"AAPL": security_id},
        observed_at=observed_at,
        ingestion_segment_id=uuid4(),
        availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
    )[0]

    assert result.interval == "1d"
    assert result.bar_start == datetime(2024, 1, 2, 14, 30, tzinfo=UTC)
    assert result.bar_end == datetime(2024, 1, 2, 21, 0, tzinfo=UTC)
    assert result.replay_available_at == result.bar_end
    assert result.quality_codes == ("GROUPED_DAILY_EXACT_TICKER_V2",)


def test_grouped_daily_bar_does_not_collapse_preferred_symbol_case() -> None:
    result = normalize_grouped_daily_bars(
        [
            {"T": "BCPC", "o": 170, "h": 172, "l": 168, "c": 171, "v": 1000},
            {"T": "BCpC", "o": 24, "h": 25, "l": 23, "c": 24, "v": 100},
        ],
        session_date=date(2024, 1, 2),
        security_ids={"BCPC": uuid4()},
        observed_at=datetime(2026, 8, 31, tzinfo=UTC),
        ingestion_segment_id=uuid4(),
        availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
    )

    assert len(result) == 1
    assert result[0].ticker == "BCPC"
    assert result[0].close_price == 171
    assert result[0].payload_sha256 != sha256_json({
        "T": "BCPC", "o": 170, "h": 172, "l": 168, "c": 171, "v": 1000,
    })


def test_security_normalization_includes_name_identity_float_and_sector():
    result = security()

    assert result.company_name == "Apple Inc."
    assert result.cik == "0000320193"
    assert result.primary_exchange == "XNAS"
    assert result.free_float == 14_500_000_000
    assert result.market_cap == 2_800_000_000_000
    assert result.sector is not None
    assert len(result.payload_sha256) == 64


def test_date_only_filing_is_available_next_regular_session_open():
    availability = conservative_filing_availability(date(2026, 8, 7))

    assert availability == datetime(2026, 8, 10, 13, 30, tzinfo=UTC)


def test_statement_normalizer_merges_reports_and_preserves_filing_quality():
    common = {
        "cik": "0000320193",
        "tickers": ["AAPL"],
        "period_end": "2026-06-27",
        "filing_date": "2026-08-07",
        "fiscal_year": 2026,
        "fiscal_quarter": 3,
        "timeframe": "quarterly",
    }
    reports = normalize_fundamental_reports(
        security(),
        income_rows=[{
            **common,
            "revenue": 100_000,
            "operating_income": 30_000,
            "ebitda": 35_000,
            "net_income_loss_attributable_common_shareholders": 25_000,
        }],
        balance_rows=[{
            **common,
            "cash_and_equivalents": 50_000,
            "debt_current": 5_000,
            "long_term_debt_and_capital_lease_obligations": 20_000,
        }],
        cash_flow_rows=[{
            **common,
            "net_cash_from_operating_activities": 40_000,
            "purchase_of_property_plant_and_equipment": -10_000,
        }],
        filing_rows=[{
            "accession_number": "0000320193-26-000001",
            "cik": "0000320193",
            "filing_date": "2026-08-07",
            "form_type": "10-Q",
            "ticker": "AAPL",
        }],
        observed_at=datetime(2026, 8, 10, 15, 0, tzinfo=UTC),
    )

    assert len(reports) == 1
    report = reports[0]
    assert report.accession_number == "0000320193-26-000001"
    assert report.availability_time == datetime(2026, 8, 10, 13, 30, tzinfo=UTC)
    assert '"free_cash_flow":30000.0' in report.metrics_json
    assert report.quality_codes == ("FILING_AVAILABILITY_DATE_ONLY",)


def test_native_30m_bar_is_persistable_only_after_window_end():
    start_ms = int(datetime(2026, 8, 28, 13, 30, tzinfo=UTC).timestamp() * 1000)
    row = {
        "t": start_ms,
        "o": 100,
        "h": 102,
        "l": 99,
        "c": 101,
        "v": 5000,
        "vw": 100.5,
        "n": 250,
    }

    before = normalize_native_bars(
        uuid4(), "AAPL", "30m", [row],
        observed_at=datetime(2026, 8, 28, 13, 59, tzinfo=UTC),
        adjusted=False,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
    )
    after = normalize_native_bars(
        uuid4(), "AAPL", "30m", [row],
        observed_at=datetime(2026, 8, 28, 14, 0, tzinfo=UTC),
        adjusted=False,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
    )

    assert before == ()
    assert len(after) == 1
    assert after[0].bar_end == datetime(2026, 8, 28, 14, 0, tzinfo=UTC)
    assert after[0].transaction_count == 250


def test_native_clock_aligned_1h_bars_are_rejected():
    client = PolygonEquityClient("test-key", session=FakeSession([]))

    with pytest.raises(ValueError, match="unsupported native interval"):
        client.fetch_native_bars(
            "AAPL", "1h", date(2026, 8, 28), date(2026, 8, 28)
        )
    with pytest.raises(ValueError, match="unsupported native interval"):
        normalize_native_bars(
            uuid4(), "AAPL", "1h", [],
            observed_at=datetime(2026, 8, 28, 20, 0, tzinfo=UTC),
            adjusted=False,
            availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
        )


def test_native_monthly_bar_waits_for_final_exchange_session_close():
    row = {
        "t": int(datetime(2026, 8, 1, 4, 0, tzinfo=UTC).timestamp() * 1000),
        "o": 100, "h": 110, "l": 95, "c": 108, "v": 500000, "n": 10000,
    }

    partial = normalize_native_bars(
        uuid4(), "AAPL", "1mo", [row],
        observed_at=datetime(2026, 8, 30, 20, 0, tzinfo=UTC),
        adjusted=False,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
    )
    complete = normalize_native_bars(
        uuid4(), "AAPL", "1mo", [row],
        observed_at=datetime(2026, 8, 31, 20, 0, tzinfo=UTC),
        adjusted=False,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
    )

    assert partial == ()
    assert len(complete) == 1
    assert complete[0].bar_start == datetime(2026, 8, 3, 13, 30, tzinfo=UTC)
    assert complete[0].bar_end == datetime(2026, 8, 31, 20, 0, tzinfo=UTC)
    assert complete[0].session_date == date(2026, 8, 31)


def test_client_requests_polygon_month_timespan():
    session = FakeSession([FakeResponse({"results": []})])
    client = PolygonEquityClient("test-key", session=session)

    client.fetch_native_bars("AAPL", "1mo", date(2026, 7, 1), date(2026, 7, 31))

    assert "/range/1/month/" in session.calls[0][0]


def test_historical_bar_uses_simulated_bar_end_availability_not_download_time():
    start = datetime(2025, 8, 28, 13, 30, tzinfo=UTC)
    rows = normalize_native_bars(
        uuid4(), "AAPL", "30m",
        [{
            "t": int(start.timestamp() * 1000),
            "o": 100, "h": 102, "l": 99, "c": 101, "v": 5000,
        }],
        observed_at=datetime(2026, 8, 30, 12, 0, tzinfo=UTC),
        adjusted=False,
        availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
    )

    assert rows[0].system_observed_at == datetime(2026, 8, 30, 12, 0, tzinfo=UTC)
    assert rows[0].replay_available_at == start + timedelta(minutes=30)


def test_live_and_replay_versions_have_distinct_bar_identities():
    start = datetime(2026, 8, 28, 13, 30, tzinfo=UTC)
    payload = [{
        "t": int(start.timestamp() * 1000),
        "o": 100, "h": 102, "l": 99, "c": 101, "v": 5000,
    }]
    security_id = uuid4()
    live = normalize_native_bars(
        security_id, "AAPL", "30m", payload,
        observed_at=start + timedelta(minutes=31), adjusted=False,
        availability_mode=BarAvailabilityMode.LIVE_OBSERVED,
    )[0]
    replay = normalize_native_bars(
        security_id, "AAPL", "30m", payload,
        observed_at=datetime(2026, 8, 30, tzinfo=UTC), adjusted=False,
        availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
    )[0]

    assert live.bar_revision_id != replay.bar_revision_id
    assert live.payload_sha256 == replay.payload_sha256