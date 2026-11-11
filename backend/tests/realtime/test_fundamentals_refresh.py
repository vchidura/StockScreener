"""G37 (Nov 10): the Financials tab's statements, refreshed for the desired companies; only report and coverage facts
are written, and each window's workspace keeps just the periods the tab shows."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID

from equity.domain import SecurityReferenceRevision
from realtime import fundamentals_refresh as fr
from realtime.ticker_projection import FUNDAMENTAL_PERIODS_KEPT, _recent_fundamental_periods

UTC = timezone.utc
NOW = datetime(2026, 11, 10, 22, 0, tzinfo=UTC)


def _security(ticker="AAPL"):
    return SecurityReferenceRevision(
        security_revision_id=UUID(int=1), security_id=UUID(int=2), ticker=ticker, active=True, company_name="Apple",
        security_type="CS", cik="0000320193", composite_figi=None, share_class_figi=None, primary_exchange="XNAS",
        sic_code=None, sic_description=None, sector=None, industry=None, list_date=None, delisted_date=None,
        weighted_shares=Decimal(1), free_float=None, free_float_percent=None, market_cap=None, source="massive",
        effective_from=NOW - timedelta(days=30), observed_at=NOW - timedelta(days=30), payload_sha256="a" * 64,
        raw_payload_json="{}",
    )


def _row(**values):
    return {"cik": "0000320193", "tickers": ["AAPL"], "period_end": "2026-09-26", "timeframe": "quarterly",
            "fiscal_year": 2026, "fiscal_quarter": 4, "filing_date": "2026-11-03", **values}


def test_only_report_and_coverage_facts_are_collected():
    facts, counts = fr.collect_fundamental_facts(
        {"AAPL": _security()},
        fetch_income_statements=lambda tickers, as_of: (_row(revenue=114.5e9, diluted_earnings_per_share=2.1),),
        fetch_balance_sheets=lambda tickers, as_of: (_row(total_equity=112.1e9),),
        fetch_cash_flow_statements=lambda tickers, as_of: (_row(net_cash_from_operating_activities=50e9),),
        fetch_filing_index=lambda tickers, as_of: (),
        created_at=NOW, valid_until=NOW + timedelta(days=3),
    )
    assert counts == {"AAPL": 1}
    assert {fact.relation.name for fact in facts} == {"FUNDAMENTAL_REPORT", "FUNDAMENTAL_COVERAGE"}
    report = dict(next(fact for fact in facts if fact.relation.name == "FUNDAMENTAL_REPORT").values)
    assert report["security_revision_id"] == UUID(int=1)
    assert report["metrics"]["revenue"] == 114.5e9 and report["metrics"]["total_equity"] == 112.1e9
    coverage = dict(next(fact for fact in facts if fact.relation.name == "FUNDAMENTAL_COVERAGE").values)
    assert coverage["report_count"] == 1 and coverage["window_end"] == date(2026, 11, 10)


def test_a_company_without_filings_still_gets_coverage_saying_so():
    facts, counts = fr.collect_fundamental_facts(
        {"AAPL": _security()}, fetch_income_statements=lambda *_: (), fetch_balance_sheets=lambda *_: (),
        fetch_cash_flow_statements=lambda *_: (), fetch_filing_index=lambda *_: (),
        created_at=NOW, valid_until=NOW + timedelta(days=3),
    )
    assert counts == {"AAPL": 0}
    assert [fact.relation.name for fact in facts] == ["FUNDAMENTAL_COVERAGE"]


def test_a_failure_never_raises_and_reports_only_its_type():
    class Broken:
        def transaction(self, *_args, **_kwargs):
            raise ConnectionError("secret-detail")

    result = fr.run_fundamentals_refresh(Broken(), clock=lambda: NOW, api_key="k")
    assert result == {"fundamentals": "FAILED", "error_type": "ConnectionError"}


def test_the_workspace_keeps_the_newest_filing_and_recent_periods():
    rows = []
    for index in range(20):
        quarter_end = (date(2026, 9, 26) - timedelta(days=91 * index)).isoformat()
        rows.append({"timeframe": "quarterly", "period_end": quarter_end, "filing_date": "x", "revenue": index,
                     "availability_time": f"2026-{12 - index // 4:02d}"})
    restated = dict(rows[0], revenue=-1, availability_time="2025-01")
    kept = _recent_fundamental_periods(rows + [restated])
    assert len(kept) == FUNDAMENTAL_PERIODS_KEPT["quarterly"]
    assert kept[0]["period_end"] == "2026-09-26" and kept[0]["revenue"] == 0


def test_workspace_over_the_bound_keeps_the_newest_financials_that_fit():
    """Nov 10 (live): Financials pushed 15 stock workspaces to 134-149 KB, over the 128 KiB bound, and every
    window dropped them. The workspace now keeps the newest periods that fit and labels the trim."""
    import pytest
    from realtime.contracts import canonical_json
    from realtime.ticker_projection import _fit_fundamentals_to_bound

    def report(period, timeframe="quarterly"):
        return dict(period_end=period, timeframe=timeframe, revenue=1.0, notes="x" * 900)

    reports = [report(f"20{year}-{month:02d}-28") for year in range(26, 20, -1) for month in (12, 9, 6, 3)]
    payload = dict(ticker="AMZN", charts="c" * 5000, profile=dict(ticker="AMZN", fundamental_reports=reports))
    full = len(canonical_json(payload).encode("ascii"))
    limit = full - 6000

    fitted = _fit_fundamentals_to_bound(payload, name="Ticker workspace AMZN", limit=limit)

    kept = fitted["profile"]["fundamental_reports"]
    assert len(canonical_json(fitted).encode("ascii")) <= limit
    assert 0 < len(kept) < len(reports) and kept == reports[:len(kept)]
    assert fitted["profile"]["fundamental_reports_status"] == "TRIMMED_TO_WORKSPACE_BOUND"
    assert fitted["profile"]["fundamental_reports_available"] == len(reports)
    assert fitted["charts"] == payload["charts"] and payload["profile"]["fundamental_reports"] is reports
    assert _fit_fundamentals_to_bound(payload, name="x", limit=full) is payload
    with pytest.raises(ValueError, match="by-reference payload"):
        _fit_fundamentals_to_bound(dict(payload, profile=dict(payload["profile"], fundamental_reports=[])),
                                   name="x", limit=1000)
