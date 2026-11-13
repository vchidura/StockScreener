"""R60 (Nov 13): the earnings and fundamentals refreshes were sized for the 25-name list. At 45 members (35 companies)
fundamentals failed on the 2 MB per-response cap (one income-statement request for every company) and earnings refused
the last three companies (two Finnhub requests each, 64-request cap)."""
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from urllib.parse import urlsplit
from uuid import UUID

from equity.domain import SecurityReferenceRevision
from realtime import earnings_calendar_refresh as ecr
from realtime import fundamentals_refresh as fr
from realtime.reference_transports import (
    FUNDAMENTAL_FILING_FORMS,
    FinnhubReferenceTransport,
    MassiveReferenceTransport,
    ReferenceHttpResponse,
)

UTC = timezone.utc
NOW = datetime(2026, 11, 13, 7, 50, tzinfo=UTC)


def _security(index: int) -> SecurityReferenceRevision:
    return SecurityReferenceRevision(
        security_revision_id=UUID(int=1000 + index), security_id=UUID(int=2000 + index), ticker=f"T{index:02d}",
        active=True, company_name=None, security_type="CS", cik=None, composite_figi=None, share_class_figi=None,
        primary_exchange="XNYS", sic_code=None, sic_description=None, sector=None, industry=None, list_date=None,
        delisted_date=None, weighted_shares=Decimal(1), free_float=None, free_float_percent=None, market_cap=None,
        source="massive", effective_from=NOW - timedelta(days=30), observed_at=NOW - timedelta(days=30),
        payload_sha256="a" * 64, raw_payload_json="{}",
    )


class _Http:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, params, headers):
        self.calls.append((urlsplit(url).path, dict(params)))
        return ReferenceHttpResponse(200, json.dumps(self.payload).encode("utf-8"))


def test_fundamentals_are_fetched_in_groups_of_the_batch_size_and_every_company_gets_coverage():
    securities = {security.ticker: security for security in map(_security, range(35))}
    groups = {name: [] for name in ("income", "balance", "cash", "filings")}

    def recorder(name):
        def fetch(tickers, as_of):
            assert as_of == NOW.date()
            groups[name].append(tuple(tickers))
            return ()
        return fetch

    facts, counts = fr.collect_fundamental_facts(
        securities, fetch_income_statements=recorder("income"), fetch_balance_sheets=recorder("balance"),
        fetch_cash_flow_statements=recorder("cash"), fetch_filing_index=recorder("filings"),
        created_at=NOW, valid_until=NOW + timedelta(days=3),
    )
    for calls in groups.values():
        assert [len(group) for group in calls] == [8, 8, 8, 8, 3]
        assert [ticker for group in calls for ticker in group] == sorted(securities)
    assert set(counts) == set(securities)
    assert sum(fact.relation.name == "FUNDAMENTAL_COVERAGE" for fact in facts) == 35


def test_the_filing_index_asks_only_for_the_forms_the_normalizer_matches():
    http = _Http({"results": []})
    MassiveReferenceTransport("k", http).fetch_filing_index(("AAPL", "MSFT"), date(2026, 11, 13))
    (path, params), = http.calls
    assert path == "/stocks/filings/vX/index"
    assert params["form_type.any_of"] == "10-K,10-Q" and FUNDAMENTAL_FILING_FORMS == ("10-K", "10-Q")
    assert params["ticker.any_of"] == "AAPL,MSFT" and params["filing_date.lte"] == "2026-11-13"


def test_statement_endpoints_are_unchanged():
    http = _Http({"results": []})
    MassiveReferenceTransport("k", http).fetch_income_statements(("AAPL",), date(2026, 11, 13))
    (_path, params), = http.calls
    assert "form_type.any_of" not in params and params["tickers.any_of"] == "AAPL"


def test_the_earnings_refresh_transport_is_sized_for_two_requests_per_company(monkeypatch, tmp_path):
    key = tmp_path / "finnhub"
    key.write_text("k", encoding="utf-8")
    built = {}

    def capture(provider, *, clock, fetch, **_kwargs):
        built["limits"] = fetch.__self__.limits
        return {"earnings_calendar": "CURRENT"}

    monkeypatch.setattr(ecr, "refresh_earnings_calendar", capture)
    assert ecr.run_earnings_calendar_refresh(object(), clock=lambda: NOW, key_file=key) == {
        "earnings_calendar": "CURRENT"}
    assert isinstance(built["limits"].maximum_requests, int)
    assert built["limits"].maximum_requests >= 2 * 400


def test_a_finnhub_transport_with_the_refresh_cap_answers_41_companies():
    http = _Http({"earningsCalendar": []})
    transport = FinnhubReferenceTransport(
        "k", http, sleep=lambda _seconds: None,
        limits=__import__("realtime.reference_transports", fromlist=["x"]).ReferenceTransportLimits(
            maximum_requests=ecr.EARNINGS_MAXIMUM_REQUESTS),
    )
    for index in range(41):
        transport.fetch(date(2026, 11, 12), date(2026, 12, 28), symbol=f"T{index:02d}")
    assert len(http.calls) == 82


def test_the_fundamentals_refresh_transport_caps_cover_a_400_name_list(monkeypatch, tmp_path):
    built = {}

    def capture(provider, *, clock, transport, **_kwargs):
        built["limits"] = transport.limits
        return {"fundamentals": "CURRENT"}

    monkeypatch.setattr(fr, "refresh_fundamentals", capture)
    assert fr.run_fundamentals_refresh(object(), clock=lambda: NOW, api_key="k") == {"fundamentals": "CURRENT"}
    limits = built["limits"]
    groups = -(-400 // fr.FUNDAMENTALS_BATCH)
    assert limits.maximum_requests >= 4 * groups * 2
    assert limits.maximum_response_bytes == 2_000_000
    assert limits.maximum_total_bytes >= 400 * 200_000
