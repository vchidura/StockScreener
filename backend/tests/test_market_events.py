from datetime import date, datetime, timedelta, timezone
from contextlib import contextmanager

import pytest

from market_events import (
    FINNHUB_EARNINGS_URL,
    FINNHUB_REQUEST_INTERVAL_SECONDS,
    FomcCalendarSnapshot,
    FomcMeeting,
    FinnhubEarningsClient,
    FinnhubRateLimitError,
    PublicMarketEventMaterializer,
    build_public_calendar_document,
    parse_fomc_calendar,
)
from options.event_calendar import parse_event_calendar_document
from options.repositories.market_events import OptionEventCalendarPersistResult
from options.repositories.market_events import OptionMarketEventRepository
from security_types import is_earnings_applicable_security_type


UTC = timezone.utc
OBSERVED_AT = datetime(2026, 9, 11, 15, 0, tzinfo=UTC)
WINDOW_START = datetime(2026, 9, 10, 4, 0, tzinfo=UTC)
WINDOW_END = datetime(2026, 10, 26, 3, 59, tzinfo=UTC)


class _Response:
    def __init__(self, payload=None, text="", status_code=200, headers=None):
        self.payload = payload
        self.text = text
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")
        return None

    def json(self):
        return self.payload


class _Session:
    def __init__(self, response):
        self.responses = list(response) if isinstance(response, list) else [response]
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


def _fomc_html():
    return """
    <h4><a id="42828">2026 FOMC Meetings</a></h4>
    <div class="fomc-meeting__month col-xs-5"><strong>September</strong></div>
    <div class="fomc-meeting__date col-xs-4">15-16*</div>
    <div class="fomc-meeting__month col-xs-5"><strong>October</strong></div>
    <div class="fomc-meeting__date col-xs-4">27-28</div>
    <div class="lastUpdate" id="lastUpdate">Last Update: August 19, 2026</div>
    """


def _earnings_row(report_date="2026-10-22"):
    return {
        "date": report_date,
        "epsActual": None,
        "epsEstimate": 1.5,
        "hour": "amc",
        "quarter": 4,
        "revenueActual": None,
        "revenueEstimate": 100,
        "symbol": "AAPL",
        "year": 2026,
    }


def test_finnhub_earnings_client_keeps_token_out_of_query_parameters():
    row = _earnings_row("2026-09-22")
    session = _Session(_Response({"earningsCalendar": [row]}))

    rows = FinnhubEarningsClient("secret", session).fetch(
        date(2026, 9, 11), date(2026, 10, 9)
    )

    assert rows == (row,)
    url, kwargs = session.calls[0]
    assert url == FINNHUB_EARNINGS_URL
    assert kwargs["params"] == {"from": "2026-09-11", "to": "2026-10-09"}
    assert kwargs["headers"] == {"X-Finnhub-Token": "secret"}
    assert kwargs["allow_redirects"] is False


def test_finnhub_earnings_client_paces_bounded_request_windows():
    sleeps = []
    first = _earnings_row("2026-09-11")
    second = _earnings_row("2026-10-22")
    session = _Session([
        _Response({"earningsCalendar": [first]}),
        _Response({"earningsCalendar": [second]}),
        _Response({"earningsCalendar": [first, second]}),
    ])

    rows = FinnhubEarningsClient(
        "secret", session, sleep=sleeps.append
    ).fetch(date(2026, 9, 10), date(2026, 10, 26))

    assert len(rows) == 2
    assert [call[1]["params"] for call in session.calls] == [
        {"from": "2026-09-10", "to": "2026-10-09"},
        {"from": "2026-10-10", "to": "2026-10-26"},
        {"from": "2026-09-10", "to": "2026-10-26"},
    ]
    assert sleeps == [
        FINNHUB_REQUEST_INTERVAL_SECONDS,
        FINNHUB_REQUEST_INTERVAL_SECONDS,
    ]
    assert FINNHUB_REQUEST_INTERVAL_SECONDS == 2.1


def test_finnhub_earnings_client_rejects_incomplete_bounded_response():
    first = _earnings_row("2026-09-11")
    second = _earnings_row("2026-10-22")
    session = _Session([
        _Response({"earningsCalendar": [first]}),
        _Response({"earningsCalendar": []}),
        _Response({"earningsCalendar": [first, second]}),
    ])

    with pytest.raises(ValueError, match="global and bounded"):
        FinnhubEarningsClient("secret", session, sleep=lambda _: None).fetch(
            date(2026, 9, 10), date(2026, 10, 26)
        )


def test_finnhub_earnings_client_exposes_retry_after_without_retrying():
    session = _Session(_Response(
        status_code=429,
        headers={"Retry-After": "17"},
    ))

    with pytest.raises(FinnhubRateLimitError) as error:
        FinnhubEarningsClient("secret", session).fetch(
            date(2026, 9, 11), date(2026, 9, 12)
        )

    assert error.value.retry_after_seconds == 17
    assert len(session.calls) == 1


@pytest.mark.parametrize("row", [
    {**_earnings_row(), "symbol": ""},
    {**_earnings_row(), "date": "not-a-date"},
    {**_earnings_row(), "date": "2026-10-27"},
])
def test_finnhub_earnings_client_rejects_unscoped_rows(row):
    client = FinnhubEarningsClient(
        "secret", _Session(_Response({"earningsCalendar": [row]}))
    )

    with pytest.raises(ValueError, match="Finnhub earnings row"):
        client.fetch(date(2026, 9, 11), date(2026, 10, 26))


def test_finnhub_earnings_client_rejects_incomplete_response_shape():
    client = FinnhubEarningsClient("secret", _Session(_Response({"error": "limited"})))

    with pytest.raises(ValueError, match="invalid shape"):
        client.fetch(date(2026, 9, 11), date(2026, 10, 26))


def test_fomc_calendar_parser_uses_decision_day_and_page_update():
    snapshot = parse_fomc_calendar(_fomc_html())

    assert snapshot.source_updated_at == datetime(2026, 8, 19, tzinfo=UTC)
    assert snapshot.meetings == (
        FomcMeeting(2026, 1, date(2026, 9, 16)),
        FomcMeeting(2026, 2, date(2026, 10, 28)),
    )


def test_fomc_calendar_parser_handles_cross_month_abbreviations():
    snapshot = parse_fomc_calendar("""
        <h4><a>2023 FOMC Meetings</a></h4>
        <div class="fomc-meeting__month"><strong>Jan/Feb</strong></div>
        <div class="fomc-meeting__date">31-1</div>
    """)

    assert snapshot.meetings == (FomcMeeting(2023, 1, date(2023, 2, 1)),)


def test_public_calendar_document_covers_full_company_universe_and_global_fed():
    document = build_public_calendar_document(
        company_tickers=("MSFT", "AAPL"),
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        observed_at=OBSERVED_AT,
        earnings_rows=(_earnings_row(),),
        fomc=FomcCalendarSnapshot(
            datetime(2026, 8, 19, tzinfo=UTC),
            (FomcMeeting(2026, 6, date(2026, 9, 16)),),
        ),
    )
    batch = parse_event_calendar_document(document, received_at=OBSERVED_AT)

    assert len(batch.coverage) == 3
    assert {
        row.affected_underlying for row in batch.coverage
        if row.event_type.value == "EARNINGS"
    } == {"AAPL", "MSFT"}
    assert {row.event_type.value for row in batch.events} == {
        "EARNINGS", "FED_RATE_DECISION",
    }


def test_missing_earnings_hour_is_unknown_precision():
    row = {**_earnings_row(), "hour": None}
    document = build_public_calendar_document(
        company_tickers=("AAPL",),
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        observed_at=OBSERVED_AT,
        earnings_rows=(row,),
        fomc=None,
    )

    assert document["events"][0]["confidence"] == "UNKNOWN"
    assert document["events"][0]["scheduled_time"].endswith("16:00:00Z")


def test_earnings_identity_survives_a_provider_date_revision():
    common = {
        "company_tickers": ("AAPL",),
        "window_start": WINDOW_START,
        "window_end": WINDOW_END + timedelta(days=10),
        "observed_at": OBSERVED_AT,
        "fomc": None,
    }
    first = build_public_calendar_document(
        **common, earnings_rows=(_earnings_row("2026-10-22"),)
    )
    revised = build_public_calendar_document(
        **common, earnings_rows=(_earnings_row("2026-10-29"),)
    )

    assert first["events"][0]["source_key"] == revised["events"][0]["source_key"]
    assert first["events"][0]["scheduled_time"] != revised["events"][0]["scheduled_time"]


def test_non_company_security_types_do_not_require_earnings_coverage():
    assert is_earnings_applicable_security_type("CS") is True
    assert is_earnings_applicable_security_type(None) is True
    assert is_earnings_applicable_security_type("ETF") is False
    assert is_earnings_applicable_security_type("ETN") is False


def test_materializer_retains_fed_coverage_when_finnhub_key_is_missing():
    class FomcClient:
        def fetch(self):
            return FomcCalendarSnapshot(
                datetime(2026, 8, 19, tzinfo=UTC),
                (FomcMeeting(2026, 6, date(2026, 9, 16)),),
            )

    class Repository:
        def __init__(self):
            self.coverage = ()
            self.events = ()

        def persist_batch(self, coverage, events):
            self.coverage = tuple(coverage)
            self.events = tuple(events)
            return OptionEventCalendarPersistResult(len(coverage), len(events))

        def latest_active_events(self, **kwargs):
            return ()

    repository = Repository()
    result = PublicMarketEventMaterializer(
        earnings_client=None,
        fomc_client=FomcClient(),
        repository=repository,
    ).materialize(
        ("AAPL", "MSFT"),
        start=date(2026, 9, 10),
        end=date(2026, 10, 26),
        observed_at=OBSERVED_AT,
    )

    assert result.coverage_count == 1
    assert result.fomc_event_count == 1
    assert result.earnings_event_count == 0
    assert result.reasons == ("FINNHUB_API_KEY_MISSING",)
    assert repository.coverage[0].event_type.value == "FED_RATE_DECISION"


def test_materializer_withholds_earnings_coverage_for_malformed_provider_rows():
    class EarningsClient:
        def fetch(self, start, end):
            return ({"symbol": "AAPL", "date": "2026-10-22"},)

    class FomcClient:
        def fetch(self):
            return FomcCalendarSnapshot(
                datetime(2026, 8, 19, tzinfo=UTC),
                (FomcMeeting(2026, 6, date(2026, 9, 16)),),
            )

    class Repository:
        def latest_active_events(self, **kwargs):
            return ()

        def persist_batch(self, coverage, events):
            assert {row.event_type.value for row in coverage} == {
                "FED_RATE_DECISION"
            }
            return OptionEventCalendarPersistResult(len(coverage), len(events))

    result = PublicMarketEventMaterializer(
        earnings_client=EarningsClient(),
        fomc_client=FomcClient(),
        repository=Repository(),
    ).materialize(
        ("AAPL",),
        start=date(2026, 9, 10),
        end=date(2026, 10, 26),
        observed_at=OBSERVED_AT,
    )

    assert result.coverage_count == 1
    assert result.reasons == ("FINNHUB_EARNINGS_INVALID:TypeError",)


def test_complete_poll_reconciles_removed_and_moved_earnings():
    class EarningsClient:
        def fetch(self, start, end):
            return (
                {**_earnings_row("2026-10-23"), "symbol": "MSFT"},
                _earnings_row("2026-10-24"),
            )

    class FomcClient:
        def fetch(self):
            return FomcCalendarSnapshot(None, ())

    class Repository:
        def __init__(self):
            self.events = ()

        def latest_active_events(self, **kwargs):
            return (
                {
                    "source_key": "finnhub:earnings:AAPL:2026:q4",
                    "affected_underlying": "AAPL",
                    "scheduled_time": datetime(2026, 10, 22, 20, 15, tzinfo=UTC),
                    "confidence": "ESTIMATED",
                    "status": "SCHEDULED",
                },
                {
                    "source_key": "finnhub:earnings:GOOGL:2026:q4",
                    "affected_underlying": "GOOGL",
                    "scheduled_time": datetime(2026, 10, 21, 20, 15, tzinfo=UTC),
                    "confidence": "ESTIMATED",
                    "status": "SCHEDULED",
                },
            )

        def persist_batch(self, coverage, events):
            self.events = tuple(events)
            return OptionEventCalendarPersistResult(len(coverage), len(events))

    repository = Repository()
    result = PublicMarketEventMaterializer(
        earnings_client=EarningsClient(),
        fomc_client=FomcClient(),
        repository=repository,
    ).materialize(
        ("AAPL", "GOOGL", "MSFT"),
        start=date(2026, 9, 10),
        end=date(2026, 10, 26),
        observed_at=OBSERVED_AT,
    )

    by_key = {str(event.source_key): event for event in repository.events}
    assert by_key["finnhub:earnings:AAPL:2026:q4"].status.value == "REVISED"
    assert by_key["finnhub:earnings:GOOGL:2026:q4"].status.value == "CANCELED"
    assert by_key["finnhub:earnings:MSFT:2026:q4"].status.value == "SCHEDULED"
    assert result.earnings_event_count == 3


def test_empty_earnings_response_never_proves_clear_coverage():
    class EarningsClient:
        def fetch(self, start, end):
            return ()

    class FomcClient:
        def fetch(self):
            return FomcCalendarSnapshot(None, ())

    class Repository:
        def persist_batch(self, coverage, events):
            return OptionEventCalendarPersistResult(len(coverage), len(events))

    result = PublicMarketEventMaterializer(
        earnings_client=EarningsClient(),
        fomc_client=FomcClient(),
        repository=Repository(),
    ).materialize(
        ("AAPL",),
        start=date(2026, 9, 10),
        end=date(2026, 10, 26),
        observed_at=OBSERVED_AT,
    )

    assert result.coverage_count == 1
    assert result.earnings_event_count == 0
    assert result.reasons == ("FINNHUB_EARNINGS_EMPTY",)


def test_rate_limited_earnings_never_proves_clear_coverage():
    class EarningsClient:
        def fetch(self, start, end):
            raise FinnhubRateLimitError(30)

    class FomcClient:
        def fetch(self):
            return FomcCalendarSnapshot(None, ())

    class Repository:
        def persist_batch(self, coverage, events):
            assert {row.event_type.value for row in coverage} == {
                "FED_RATE_DECISION"
            }
            return OptionEventCalendarPersistResult(len(coverage), len(events))

    result = PublicMarketEventMaterializer(
        earnings_client=EarningsClient(),
        fomc_client=FomcClient(),
        repository=Repository(),
    ).materialize(
        ("AAPL",),
        start=date(2026, 9, 10),
        end=date(2026, 10, 26),
        observed_at=OBSERVED_AT,
    )

    assert result.coverage_count == 1
    assert result.earnings_event_count == 0
    assert result.reasons == (
        "FINNHUB_EARNINGS_FAILED:FinnhubRateLimitError",
    )


def test_latest_active_events_reads_newest_causal_revision(rolled_back_connection):
    factory, connection = rolled_back_connection
    first = parse_event_calendar_document(
        build_public_calendar_document(
            company_tickers=("AAPL",),
            window_start=WINDOW_START,
            window_end=WINDOW_END,
            observed_at=OBSERVED_AT,
            earnings_rows=(_earnings_row("2026-10-22"),),
            fomc=None,
        ),
        received_at=OBSERVED_AT,
    )
    repository = OptionMarketEventRepository(factory)
    repository.persist_batch(first.coverage, first.events)
    later = OBSERVED_AT + timedelta(minutes=1)
    canceled_document = build_public_calendar_document(
        company_tickers=("AAPL",),
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        observed_at=later,
        earnings_rows=(_earnings_row("2026-10-22"),),
        fomc=None,
    )
    canceled_document["events"][0]["status"] = "CANCELED"
    canceled = parse_event_calendar_document(
        canceled_document, received_at=later
    )
    repository.persist_batch(canceled.coverage, canceled.events)

    active = repository.latest_active_events(
        source="public_calendar_v1",
        event_type="EARNINGS",
        affected_underlyings=("AAPL",),
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        observed_at=later,
    )

    assert active == ()
    connection.rollback()