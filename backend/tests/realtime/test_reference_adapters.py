from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from market_events import FomcCalendarSnapshot, FomcMeeting
from realtime.contracts import CANARY_UNIVERSE
from realtime.reference_adapters import (
    FederalReserveReferenceAdapter,
    FinnhubReferenceAdapter,
    MassiveReferenceAdapter,
    TreasuryReferenceAdapter,
    _corporate_action_coverage_fact,
    _corporate_action_fact,
    _security_fact,
)
from realtime.reference_facts import ReferenceFactRelation
from realtime.reference_seed import ReferenceKind, SeedMemberStatus, SeedStatus
from realtime.reference_service import (
    ProviderEntitlementUnavailable,
    ReferenceSeedRequest,
    ReferenceSeedService,
)


UTC = timezone.utc
NOW = datetime(2026, 10, 5, 12, 0, tzinfo=UTC)


def _request():
    return ReferenceSeedRequest(
        CANARY_UNIVERSE,
        NOW.date(),
        NOW,
        NOW.replace(minute=1),
        NOW.replace(hour=18),
    )


def _treasury_xml():
    return """<?xml version="1.0" encoding="utf-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom"
          xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"
          xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
      <updated>2026-10-05T11:00:00Z</updated>
      <entry><content><m:properties>
        <d:NEW_DATE>2026-10-05T00:00:00Z</d:NEW_DATE>
        <d:BC_1MONTH>4.20</d:BC_1MONTH><d:BC_1YEAR>4.00</d:BC_1YEAR>
      </m:properties></content></entry>
    </feed>"""


def _dividend_payload():
    return dict(id="dividend-aapl-2026", ticker="AAPL", cash_amount=.26, split_adjusted_cash_amount=.13, frequency=4,
        currency="USD", distribution_type="recurring", declaration_date="2026-10-01",
        ex_dividend_date="2026-10-12", record_date="2026-10-12", pay_date="2026-10-15")


def test_shared_dividend_context_preserves_original_and_split_adjusted_amounts_and_clocks():
    from realtime.dividend_reference import dividend_observation
    fact = _corporate_action_fact("AAPL", "DIVIDEND", _dividend_payload(), _request())
    row = dict(fact.values)
    context = dividend_observation(row, known_by=NOW)
    assert context.original_cash_amount == Decimal("0.26")
    assert context.split_adjusted_cash_amount == Decimal("0.13")
    assert context.currency == "USD"
    assert context.distribution_type == "recurring"
    assert context.frequency == 4
    assert context.provider_id == "dividend-aapl-2026"
    assert context.ex_dividend_date.isoformat() == "2026-10-12"
    assert context.observed_at == context.received_at == NOW
    assert context.payload_sha256 == row["payload_sha256"]
    assert context.quality_codes == ()


def test_shared_dividend_context_keeps_missing_metadata_unknown():
    from realtime.dividend_reference import dividend_observation
    payload = _dividend_payload()
    del payload["currency"]
    del payload["distribution_type"]
    row = dict(_corporate_action_fact("AAPL", "DIVIDEND", payload, _request()).values)
    context = dividend_observation(row, known_by=NOW)
    assert context.currency is None
    assert context.distribution_type == "unknown"
    assert set(context.quality_codes) == {"DIVIDEND_CURRENCY_UNAVAILABLE", "DIVIDEND_CLASSIFICATION_UNAVAILABLE"}


@pytest.mark.parametrize("fault", ["hash", "ticker", "amount", "ex_date", "observed", "received"])
def test_shared_dividend_context_rejects_identity_or_known_by_drift(fault):
    from realtime.dividend_reference import dividend_observation
    row = dict(_corporate_action_fact("AAPL", "DIVIDEND", _dividend_payload(), _request()).values)
    if fault in {"observed", "received"}:
        row[f"{fault}_at"] = NOW + timedelta(seconds=1)
    else:
        row[{"hash": "payload_sha256", "ticker": "ticker", "amount": "cash_amount", "ex_date": "ex_date"}[fault]] = {
            "hash": "0" * 64, "ticker": "MSFT", "amount": Decimal("99"), "ex_date": NOW.date(),
        }[fault]
    with pytest.raises(ValueError):
        dividend_observation(row, known_by=NOW)


@pytest.mark.parametrize("kind,status", [("populated", "AVAILABLE"), ("empty", "KNOWN_NO_ANNOUNCED_DIVIDEND"),
    ("missing", "NOT_CHECKED"), ("stale", "STALE"), ("partial", "PARTIAL")])
def test_dividend_coverage_keeps_unknown_stale_and_known_empty_states_distinct(kind, status):
    from realtime.dividend_reference import dividend_reference_window
    from realtime.reference_adapters import _security_id
    start, end = NOW.date(), (NOW + timedelta(days=20)).date()
    payload = _dividend_payload()
    if kind == "partial":
        del payload["distribution_type"]
    response = () if kind == "empty" else (payload,)
    rows = tuple(dict(_corporate_action_fact("AAPL", "DIVIDEND", raw, _request()).values) for raw in response)
    coverage = dict(_corporate_action_coverage_fact("AAPL", "DIVIDEND", response, start, end, _request()).values)
    result = dividend_reference_window(security_id=_security_id("AAPL"), ticker="AAPL", window_start=start, window_end=end,
        known_by=NOW + timedelta(days=1) if kind == "stale" else NOW, coverage=None if kind == "missing" else coverage, rows=rows)
    assert result.status == status
    assert result.future_coverage_complete is False
    if kind != "missing":
        assert result.refresh_due_at == NOW + timedelta(days=1)
        assert result.coverage_id == coverage["coverage_id"]
    if kind == "empty":
        assert result.dividends == ()
    if kind == "stale":
        assert result.reason_codes == ("DIVIDEND_QUERY_REFRESH_DUE",)


@pytest.mark.parametrize("fault", ["count", "receipt_hash", "response_hash", "security", "clock", "facts"])
def test_dividend_coverage_fails_closed_on_receipt_or_fact_drift(fault):
    from realtime.dividend_reference import dividend_reference_window
    from realtime.reference_adapters import _security_id
    from realtime.contracts import canonical_sha256
    start, end = NOW.date(), (NOW + timedelta(days=20)).date()
    response = (_dividend_payload(),)
    rows = tuple(dict(_corporate_action_fact("AAPL", "DIVIDEND", raw, _request()).values) for raw in response)
    coverage = dict(_corporate_action_coverage_fact("AAPL", "DIVIDEND", response, start, end, _request()).values)
    if fault == "count":
        coverage["response_action_count"] += 1
    elif fault == "receipt_hash":
        coverage["payload_sha256"] = "0" * 64
    elif fault == "response_hash":
        coverage["response_sha256"] = "0" * 64
    elif fault == "security":
        rows[0]["ticker"] = "MSFT"
    elif fault == "clock":
        rows[0]["received_at"] += timedelta(seconds=1)
    else:
        rows = ()
    if fault in {"count", "response_hash"}:
        coverage["payload_sha256"] = canonical_sha256({key: coverage[key] for key in ("source_key", "response_action_count", "response_sha256")})
    result = dividend_reference_window(security_id=_security_id("AAPL"), ticker="AAPL", window_start=start, window_end=end,
        known_by=NOW, coverage=coverage, rows=rows)
    assert result.status == "UNAVAILABLE"
    assert result.dividends == ()


def test_dividend_coverage_nine_member_sample_is_independent_and_never_claims_future_completeness():
    from realtime.contracts import EQUITY_VALIDATION_UNIVERSE
    from realtime.dividend_reference import dividend_reference_window
    from realtime.reference_adapters import _security_id
    start, end = NOW.date(), (NOW + timedelta(days=30)).date()
    for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers:
        payload = dict(_dividend_payload(), id=f"dividend-{ticker}", ticker=ticker)
        if ticker == "TSLA":
            response = ()
        elif ticker == "NFLX":
            payload["distribution_type"] = ["invalid"]
            response = (payload,)
        else:
            response = (payload,)
        rows = tuple(dict(_corporate_action_fact(ticker, "DIVIDEND", raw, _request()).values) for raw in response)
        receipt = dict(_corporate_action_coverage_fact(ticker, "DIVIDEND", response, start, end, _request()).values)
        window = dividend_reference_window(security_id=_security_id(ticker), ticker=ticker, window_start=start,
            window_end=end, known_by=NOW, coverage=receipt, rows=rows)
        assert window.status == ("KNOWN_NO_ANNOUNCED_DIVIDEND" if ticker == "TSLA" else "PARTIAL" if ticker == "NFLX" else "AVAILABLE")
        assert window.future_coverage_complete is False
        assert all(row.ticker == ticker for row in window.dividends)


def test_dividend_corrections_keep_old_receipts_and_amounts_immutable():
    from realtime.dividend_reference import dividend_reference_window
    from realtime.reference_adapters import _security_id
    start, end = NOW.date(), (NOW + timedelta(days=20)).date()
    original = _dividend_payload()
    corrected = dict(original, cash_amount=.27)
    later = replace(_request(), created_at=NOW + timedelta(hours=1), effective_at=NOW + timedelta(hours=1))
    old_fact = dict(_corporate_action_fact("AAPL", "DIVIDEND", original, _request()).values)
    new_fact = dict(_corporate_action_fact("AAPL", "DIVIDEND", corrected, later).values)
    old_receipt = dict(_corporate_action_coverage_fact("AAPL", "DIVIDEND", (original,), start, end, _request()).values)
    new_receipt = dict(_corporate_action_coverage_fact("AAPL", "DIVIDEND", (corrected,), start, end, later).values)
    old_window = dividend_reference_window(security_id=_security_id("AAPL"), ticker="AAPL", window_start=start,
        window_end=end, known_by=later.created_at, coverage=old_receipt, rows=(old_fact,), coverage_id=old_receipt["coverage_id"])
    new_window = dividend_reference_window(security_id=_security_id("AAPL"), ticker="AAPL", window_start=start,
        window_end=end, known_by=later.created_at, coverage=new_receipt, rows=(new_fact,), coverage_id=new_receipt["coverage_id"])
    assert old_window.dividends[0].original_cash_amount == Decimal("0.26")
    assert new_window.dividends[0].original_cash_amount == Decimal("0.27")
    assert old_window.dividends[0].action_id != new_window.dividends[0].action_id
    assert old_window.coverage_id != new_window.coverage_id
    future = dividend_reference_window(security_id=_security_id("AAPL"), ticker="AAPL", window_start=start,
        window_end=end, known_by=NOW, coverage=new_receipt, rows=(new_fact,))
    assert future.status == "UNAVAILABLE"
    assert future.dividends == ()


def test_dividend_receipt_order_ambiguity_is_unavailable_not_fabricated_empty_coverage():
    from realtime.dividend_reference import dividend_reference_window
    from realtime.reference_adapters import _security_id
    start, end = NOW.date(), (NOW + timedelta(days=20)).date()
    response = (_dividend_payload(), dict(_dividend_payload(), id="special-dividend", distribution_type="special"))
    receipt = dict(_corporate_action_coverage_fact("AAPL", "DIVIDEND", response, start, end, _request()).values)
    rows = tuple(dict(_corporate_action_fact("AAPL", "DIVIDEND", raw, _request()).values) for raw in reversed(response))
    result = dividend_reference_window(security_id=_security_id("AAPL"), ticker="AAPL", window_start=start,
        window_end=end, known_by=NOW, coverage=receipt, rows=rows)
    assert result.status == "UNAVAILABLE"
    assert result.reason_codes == ("DIVIDEND_RECEIPT_FACT_PARITY_UNVERIFIED",)
    assert result.future_coverage_complete is False


def test_same_session_security_refresh_appends_a_new_immutable_revision():
    request = _request()
    payload = {"ticker": "AAPL", "active": True, "type": "CS"}
    first = dict(_security_fact("AAPL", payload, request).values)
    second = dict(_security_fact("AAPL", payload, replace(request,
        created_at=request.created_at + timedelta(minutes=1),
        effective_at=request.effective_at + timedelta(minutes=1),
        valid_until=request.valid_until + timedelta(minutes=1))).values)

    assert first["security_id"] == second["security_id"]
    assert first["effective_from"] < second["effective_from"]
    assert first["revision_id"] != second["revision_id"]
    assert first["effective_to"] < second["effective_to"]


def _massive():
    statement = {
        "tickers": ["AAPL"], "cik": "1", "period_end": "2026-09-30",
        "timeframe": "quarterly", "fiscal_year": 2026, "fiscal_quarter": 3,
        "filing_date": "2026-10-01",
    }
    return MassiveReferenceAdapter(
        fetch_security=lambda ticker, _: {
            "ticker": ticker, "active": True, "name": f"{ticker} Incorporated",
            "type": "ETF" if ticker in {"SPY", "QQQ", "IWM"} else "CS",
            "primary_exchange": "XNAS", "sic_code": "3571",
            "sic_description": "Electronic Computers", "list_date": "1980-12-12",
            "market_cap": 1000000000, "weighted_shares_outstanding": 1000000,
            "cik": str(("AAPL", "MSFT", "NVDA", "AMZN", "TSLA", "SPY", "QQQ", "IWM", "NFLX").index(ticker) + 1),
        },
        fetch_splits=lambda ticker, *_: (
            (
                {
                    "id": "split-aapl-2026",
                    "ticker": "AAPL",
                    "execution_date": "2026-11-01",
                    "split_from": 1,
                    "split_to": 2,
                },
            )
            if ticker == "AAPL"
            else ()
        ),
        fetch_dividends=lambda *_: (),
        fetch_option_catalog=lambda ticker, _: ({"ticker": f"O:{ticker}261218C00000001"},),
        fetch_open_interest=lambda _, contract_tickers, __: (
            {"details": {"ticker": contract_tickers[0]}, "open_interest": 100},
        ),
        fetch_income_statements=lambda *_: ({
            **statement, "revenue": 100000000, "gross_profit": 40000000,
            "operating_income": 25000000, "diluted_earnings_per_share": 2.5,
        },),
        fetch_balance_sheets=lambda *_: ({**statement, "total_equity": 75000000},),
        fetch_cash_flow_statements=lambda *_: ({
            **statement, "net_cash_from_operating_activities": 20000000,
            "purchase_of_property_plant_and_equipment": -2000000,
        },),
        fetch_filing_index=lambda *_: ({
            "cik": "0000000001", "filing_date": "2026-10-01", "form_type": "10-Q",
            "accession_number": "0000000001-26-000001",
        },),
    )


def _finnhub():
    earnings = [
        {
            "symbol": ticker,
            "date": "2026-10-20",
            "hour": "amc",
            "year": 2026,
            "quarter": 3,
        }
        for ticker in ("AAPL", "MSFT", "NVDA", "AMZN", "TSLA")
    ]
    return FinnhubReferenceAdapter(
        lambda *_: earnings,
        etf_tickers=frozenset({"SPY", "QQQ", "IWM"}),
    )


def _federal_reserve():
    return FederalReserveReferenceAdapter(
        lambda: FomcCalendarSnapshot(
            NOW,
            (FomcMeeting(2026, 1, NOW.date()),),
        )
    )


def test_source_specific_adapters_build_complete_ready_seed():
    service = ReferenceSeedService(
        (
            _massive(),
            _finnhub(),
            _federal_reserve(),
            TreasuryReferenceAdapter(lambda _: _treasury_xml()),
        )
    )

    result = service.run_once(_request())

    assert result.manifest.status is SeedStatus.READY
    assert len(result.manifest.members) == 50
    assert result.provider_requests == 47
    assert result.response_bytes > 0
    earnings = {
        row.key: row.status
        for row in result.manifest.members
        if row.kind is ReferenceKind.EARNINGS_COVERAGE
    }
    assert earnings["AAPL"] is SeedMemberStatus.READY
    assert earnings["SPY"] is SeedMemberStatus.NOT_APPLICABLE
    assert {fact.relation for fact in result.facts} == set(ReferenceFactRelation)
    aapl = next(
        dict(fact.values) for fact in result.facts
        if fact.relation is ReferenceFactRelation.SECURITY and fact.member_key == "AAPL"
    )
    assert aapl["company_name"] == "AAPL Incorporated"
    assert aapl["sector"] is not None
    assert aapl["industry"] == "Electronic Computers"
    assert aapl["list_date"].isoformat() == "1980-12-12"
    assert aapl["market_cap"] == 1000000000
    fundamental = [
        dict(fact.values) for fact in result.facts
        if fact.relation is ReferenceFactRelation.FUNDAMENTAL_REPORT
    ]
    assert len(fundamental) == 1
    assert fundamental[0]["metrics"]["revenue"] == 100000000
    assert {
        (member.kind.value, member.key)
        for member in result.manifest.members
    } <= {
        (fact.member_kind, fact.member_key)
        for fact in result.facts
    }


def test_massive_adapter_marks_empty_catalog_or_oi_unavailable():
    adapter = MassiveReferenceAdapter(
        fetch_security=lambda ticker, _: {"ticker": ticker},
        fetch_splits=lambda *_: (),
        fetch_dividends=lambda *_: (),
        fetch_option_catalog=lambda *_: (),
        fetch_open_interest=lambda *_: (),
    )

    generation = adapter.collect(_request())

    unavailable = [row for row in generation.members if row.status is SeedMemberStatus.UNAVAILABLE]
    assert len(unavailable) == 16
    assert {row.kind for row in unavailable} == {
        ReferenceKind.OPTION_CATALOG,
        ReferenceKind.OI_COVERAGE,
    }


def test_massive_adapter_uses_reviewed_corporate_action_lookback():
    windows = []
    adapter = MassiveReferenceAdapter(
        fetch_security=lambda ticker, _: {"ticker": ticker},
        fetch_splits=lambda ticker, start, end: windows.append((ticker, start, end)) or (),
        fetch_dividends=lambda *_: (),
        fetch_option_catalog=lambda ticker, _: ({"ticker": f"O:{ticker}261218C00000001"},),
        fetch_open_interest=lambda _, contract_tickers, __: (
            {"details": {"ticker": contract_tickers[0]}, "open_interest": 100},
        ),
        corporate_action_lookback_days=450,
    )

    adapter.collect(_request())

    assert len(windows) == len(CANARY_UNIVERSE.equity_tickers)
    assert all(end - start == timedelta(days=815) for _, start, end in windows)


def test_reference_adapters_are_deterministic_for_identical_source_payloads():
    request = _request()

    assert _massive().collect(request).generation_sha256 == _massive().collect(request).generation_sha256
    assert _finnhub().collect(request).generation_sha256 == _finnhub().collect(request).generation_sha256
    assert TreasuryReferenceAdapter(lambda _: _treasury_xml()).collect(request).generation_sha256 == TreasuryReferenceAdapter(lambda _: _treasury_xml()).collect(request).generation_sha256


def test_financial_entitlement_unavailable_does_not_block_required_reference_seed():
    massive = _massive()
    massive.fetch_income_statements = lambda *_: (_ for _ in ()).throw(
        ProviderEntitlementUnavailable("financials unavailable")
    )
    service = ReferenceSeedService((
        massive,
        _finnhub(),
        _federal_reserve(),
        TreasuryReferenceAdapter(lambda _: _treasury_xml()),
    ))

    result = service.run_once(_request())

    assert result.manifest.status is SeedStatus.READY
    fundamentals = [
        member for member in result.manifest.members
        if member.kind is ReferenceKind.FUNDAMENTALS
    ]
    assert len(fundamentals) == len(CANARY_UNIVERSE.equity_tickers)
    assert all(member.status is SeedMemberStatus.UNAVAILABLE for member in fundamentals)
    assert all(
        member.reason_codes == ("POLYGON_FINANCIALS_ENTITLEMENT_UNAVAILABLE",)
        for member in fundamentals
    )
    assert not any(
        fact.relation in {
            ReferenceFactRelation.FUNDAMENTAL_REPORT,
            ReferenceFactRelation.FUNDAMENTAL_COVERAGE,
        }
        for fact in result.facts
    )