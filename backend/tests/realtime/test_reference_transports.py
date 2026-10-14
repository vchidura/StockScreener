import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from urllib.parse import urlsplit

import pytest

from realtime.contracts import CANARY_UNIVERSE
from realtime.reference_facts import ReferenceFactRelation
from realtime.reference_seed import SeedStatus
from realtime.reference_repository import persist_reference_result
from realtime.reference_service import ReferenceSeedRequest
from realtime.reference_service import ProviderEntitlementUnavailable
from realtime.reference_transports import (
    FederalReserveReferenceTransport,
    FinnhubReferenceTransport,
    MassiveReferenceTransport,
    ReferenceHttpResponse,
    ReferenceTransportLimits,
    build_live_reference_service,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)


def _response(payload) -> ReferenceHttpResponse:
    body = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")
    return ReferenceHttpResponse(200, body)


def _fomc_html() -> bytes:
    return b"""
    <h4><a>2026 FOMC Meetings</a></h4>
    <div class="fomc-meeting__month"><strong>December</strong></div>
    <div class="fomc-meeting__date">8-9</div>
    <div id="lastUpdate">Last Update: September 30, 2026</div>
    """


def _treasury_xml() -> bytes:
    return b"""<?xml version="1.0" encoding="utf-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom"
          xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"
          xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
      <updated>2026-10-05T11:00:00Z</updated>
      <entry><content><m:properties>
        <d:NEW_DATE>2026-10-05T00:00:00Z</d:NEW_DATE>
        <d:BC_1MONTH>4.20</d:BC_1MONTH><d:BC_1YEAR>4.00</d:BC_1YEAR>
      </m:properties></content></entry>
    </feed>"""


class RoutingHttp:
    def __init__(self):
        self.calls = []

    def get(self, url, params, headers):
        params = dict(params)
        headers = dict(headers)
        self.calls.append((url, params, headers))
        parsed = urlsplit(url)
        path = parsed.path
        if parsed.hostname == "api.massive.com":
            if path.startswith("/v3/reference/tickers/"):
                return _response({"results": {"ticker": path.rsplit("/", 1)[-1]}})
            if path in {"/stocks/v1/splits", "/stocks/v1/dividends"}:
                return _response({"results": []})
            if path in {
                "/stocks/financials/v1/income-statements",
                "/stocks/financials/v1/balance-sheets",
                "/stocks/financials/v1/cash-flow-statements",
                "/stocks/filings/vX/index",
            }:
                return _response({"results": []})
            if path == "/v3/reference/options/contracts":
                underlyer = params["underlying_ticker"]
                return _response({
                    "results": [{"ticker": f"O:{underlyer}261218C00000001"}],
                    "next_url": "https://api.massive.com/v3/reference/options/contracts?cursor=unused",
                })
            if path.startswith("/v3/snapshot/options/"):
                underlyer = path.split("/")[-2]
                contract_ticker = path.rsplit("/", 1)[-1]
                return _response({
                    "results": {
                        "details": {"ticker": contract_ticker},
                        "open_interest": 100,
                    },
                })
        if parsed.hostname == "finnhub.io":
            return _response({"earningsCalendar": []})
        if parsed.hostname == "www.federalreserve.gov":
            return _response(_fomc_html())
        if parsed.hostname == "home.treasury.gov":
            return _response(_treasury_xml())
        raise AssertionError(f"unexpected fake HTTP request: {url}")


class QueueHttp:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params, headers):
        self.calls.append((url, dict(params), dict(headers)))
        return self.responses.pop(0)


def _request():
    return ReferenceSeedRequest(
        CANARY_UNIVERSE,
        NOW.date(),
        NOW,
        NOW + timedelta(minutes=1),
        NOW + timedelta(hours=6),
    )


def test_live_service_wiring_is_inert_until_run_and_uses_bounded_fake_http():
    http = RoutingHttp()
    sleeps = []
    service = build_live_reference_service(
        massive_api_key="massive-secret",
        finnhub_api_key="finnhub-secret",
        etf_tickers=frozenset({"SPY", "QQQ", "IWM"}),
        http=http,
        limits=ReferenceTransportLimits(maximum_requests=44),
        finnhub_sleep=sleeps.append,
    )

    assert http.calls == []

    result = service.run_once(_request())

    assert result.manifest.status is SeedStatus.READY
    assert len(result.manifest.members) == 42
    assert result.provider_requests == 44
    assert result.response_bytes > 0
    assert {
        (member.kind.value, member.key) for member in result.manifest.members
    } <= {
        (fact.member_kind, fact.member_key) for fact in result.facts
    }
    massive_calls = [call for call in http.calls if urlsplit(call[0]).hostname == "api.massive.com"]
    assert massive_calls
    assert all(call[2] == {"Authorization": "Bearer massive-secret"} for call in massive_calls)
    assert all("apiKey" not in call[1] for call in massive_calls)
    assert not any("/financials/" in urlsplit(call[0]).path for call in massive_calls)
    assert not any("/filings/" in urlsplit(call[0]).path for call in massive_calls)
    catalog_calls = [
        call for call in massive_calls
        if urlsplit(call[0]).path == "/v3/reference/options/contracts"
    ]
    assert len(catalog_calls) == 8
    assert all(call[1]["expiration_date.gte"] == "2026-10-05" for call in catalog_calls)
    assert all(call[1]["expiration_date.lte"] == "2026-12-04" for call in catalog_calls)
    assert all(call[1]["contract_type"] == "call" for call in catalog_calls)
    assert all(call[1]["limit"] == "1" for call in catalog_calls)
    snapshot_calls = [
        call for call in massive_calls
        if urlsplit(call[0]).path.startswith("/v3/snapshot/options/")
    ]
    assert len(snapshot_calls) == 8
    assert all(call[1] == {} for call in snapshot_calls)
    finnhub_calls = [call for call in http.calls if urlsplit(call[0]).hostname == "finnhub.io"]
    assert len(finnhub_calls) == 2
    assert all(call[2] == {"X-Finnhub-Token": "finnhub-secret"} for call in finnhub_calls)
    assert sleeps == [2.1]
    finnhub_coverage = {
        fact.member_key: dict(fact.values)["verdict"]
        for fact in result.facts
        if fact.relation is ReferenceFactRelation.MARKET_EVENT_COVERAGE
        and dict(fact.values)["source"] == "finnhub"
    }
    assert finnhub_coverage["AAPL"] == "KNOWN_NO_EVENT"
    assert finnhub_coverage["SPY"] == "NOT_APPLICABLE"

    repeated = service.run_once(_request())
    assert repeated.manifest.seed_sha256 == result.manifest.seed_sha256
    assert repeated.provider_requests == result.provider_requests
    assert sleeps == [2.1, 2.1]


def test_complete_fake_transport_result_persists_facts_before_atomic_promotion():
    http = RoutingHttp()
    result = build_live_reference_service(
        massive_api_key="massive-secret",
        finnhub_api_key="finnhub-secret",
        etf_tickers=frozenset({"SPY", "QQQ", "IWM"}),
        http=http,
        finnhub_sleep=lambda _: None,
    ).run_once(_request())
    cursor = MagicMock()
    security_identities = {
        dict(fact.values)["security_id"]: (
            dict(fact.values)["asset_type"],
            dict(fact.values)["source_origin"],
        )
        for fact in result.facts
        if fact.relation is ReferenceFactRelation.SECURITY
    }
    contract_identities = {
        dict(fact.values)["contract_id"]: (
            dict(fact.values)["underlying_security_id"],
            dict(fact.values)["provider"],
            dict(fact.values)["contract_ticker"],
            dict(fact.values)["source_origin"],
        )
        for fact in result.facts
        if fact.relation is ReferenceFactRelation.OPTION_CONTRACT
    }
    connection = MagicMock()
    connection.cursor.return_value.__enter__.return_value = cursor
    action_facts = {dict(fact.values)["action_id"]: fact.ordered_values() for fact in result.facts
                    if fact.relation is ReferenceFactRelation.CORPORATE_ACTION}
    coverage_facts = {dict(fact.values)["coverage_id"]: fact.ordered_values() for fact in result.facts
                      if fact.relation is ReferenceFactRelation.CORPORATE_ACTION_COVERAGE}

    def execute(statement, parameters=None):
        if statement.startswith("SELECT asset_type"):
            cursor.fetchone.return_value = security_identities[parameters[0]]
        elif statement.startswith("SELECT underlying_security_id"):
            cursor.fetchone.return_value = contract_identities[parameters[0]]
        elif statement.startswith("SELECT seed_sha256"):
            cursor.fetchone.return_value = (result.manifest.seed_sha256,)
        elif statement.startswith("SELECT generation"):
            cursor.fetchone.return_value = None
        elif statement.startswith("SELECT action_id"):
            cursor.fetchone.return_value = action_facts[parameters[0]]
        elif statement.startswith("SELECT coverage_id"):
            cursor.fetchone.return_value = coverage_facts[parameters[0]]

    cursor.execute.side_effect = execute

    persist_reference_result(connection, result, generation=1)

    statements = [call.args[0] for call in cursor.execute.call_args_list]
    manifest_index = next(
        index for index, statement in enumerate(statements)
        if statement.startswith("INSERT INTO rt.seed_manifests")
    )
    current_index = next(
        index for index, statement in enumerate(statements)
        if "INSERT INTO rt.seed_current" in statement
    )
    assert manifest_index > 0
    assert any("ref.security_revisions" in row for row in statements[:manifest_index])
    assert any("ref.market_event_coverage" in row for row in statements[:manifest_index])
    assert any("ref.risk_free_rate_observations" in row for row in statements[:manifest_index])
    assert current_index > manifest_index
    assert connection.commit.call_count == 1
    assert connection.rollback.call_count == 0


def test_massive_transport_paginates_only_on_the_approved_host():
    http = QueueHttp(
        _response({
            "results": [{"ticker": "AAPL"}],
            "next_url": "https://api.massive.com/stocks/v1/splits?cursor=next",
        }),
        _response({"results": [{"ticker": "MSFT"}]}),
    )
    transport = MassiveReferenceTransport("secret", http)

    rows = transport.fetch_splits("AAPL", NOW.date(), NOW.date())

    assert [row["ticker"] for row in rows] == ["AAPL", "MSFT"]
    assert len(http.calls) == 2
    assert http.calls[0][1]["ticker"] == "AAPL"
    assert http.calls[1][1] == {}

    hostile = QueueHttp(_response({
        "results": [],
        "next_url": "https://example.com/stocks/v1/splits?cursor=stolen",
    }))
    with pytest.raises(ValueError, match="approved HTTPS host"):
        MassiveReferenceTransport("secret", hostile).fetch_splits(
            "AAPL", NOW.date(), NOW.date()
        )
    assert len(hostile.calls) == 1


def test_reference_transport_exposes_provider_entitlement_denial():
    transport = MassiveReferenceTransport(
        "secret",
        QueueHttp(ReferenceHttpResponse(403, b"{}")),
    )

    with pytest.raises(ProviderEntitlementUnavailable):
        transport.fetch_income_statements(("AAPL",), NOW.date())


def test_transport_request_page_row_and_byte_limits_fail_closed():
    request_limited = FinnhubReferenceTransport(
        "secret",
        QueueHttp(_response({"earningsCalendar": []})),
        limits=ReferenceTransportLimits(maximum_requests=1),
        sleep=lambda _: None,
    )
    with pytest.raises(ValueError, match="request limit"):
        request_limited.fetch(NOW.date(), NOW.date() + timedelta(days=30))

    page_limited = MassiveReferenceTransport(
        "secret",
        QueueHttp(_response({
            "results": [],
            "next_url": "https://api.massive.com/stocks/v1/splits?cursor=next",
        })),
        limits=ReferenceTransportLimits(maximum_pages=1),
    )
    with pytest.raises(ValueError, match="page limit"):
        page_limited.fetch_splits("AAPL", NOW.date(), NOW.date())

    row_limited = MassiveReferenceTransport(
        "secret",
        QueueHttp(_response({"results": [{"id": 1}, {"id": 2}]})),
        limits=ReferenceTransportLimits(maximum_rows=1),
    )
    with pytest.raises(ValueError, match="row limit"):
        row_limited.fetch_splits("AAPL", NOW.date(), NOW.date())

    byte_limited = FederalReserveReferenceTransport(
        QueueHttp(ReferenceHttpResponse(200, b"x" * 11)),
        limits=ReferenceTransportLimits(
            maximum_response_bytes=10,
            maximum_total_bytes=10,
        ),
    )
    with pytest.raises(ValueError, match="byte limit"):
        byte_limited.fetch()