from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
import inspect
import json
from uuid import NAMESPACE_URL, uuid5
from zoneinfo import ZoneInfo

import pytest
from options.data.polygon_http import PolygonHttpResponse
from realtime.equity_validation_source import bind_equity_validation_adjustment_identity
from realtime.contracts import EQUITY_VALIDATION_UNIVERSE, UniverseManifest
from realtime.equity_validation_history import EQUITY_VALIDATION_HISTORY_POLICY
from realtime.equity_validation_source import (
    EquityValidationIdentity,
    EquityValidationSourceLimits,
    PolygonEquityValidationHistorySource,
    _aggregate_path_identity,
    build_equity_validation_acquisition_plan,
)
from realtime.equity_portal_history import build_realtime_equity_portal_history_plan
from tests.realtime.test_equity_validation_history import _bar, _security


class QueueTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params):
        self.calls.append((url, dict(params)))
        return self.responses.pop(0)


def _identities():
    return tuple(
        EquityValidationIdentity(ticker, _security(ticker), index)
        for index, ticker in enumerate(
            EQUITY_VALIDATION_UNIVERSE.equity_tickers,
            start=1,
        )
    )


def test_equity_validation_acquisition_plan_is_exact_and_bounded():
    plan = build_equity_validation_acquisition_plan(date(2026, 10, 2))

    assert len(plan.adjusted_sessions) == 283
    assert plan.adjusted_sessions[-1] == date(2026, 10, 2)
    assert plan.raw_start < plan.raw_end
    assert len(plan.raw_sessions) == 90
    assert plan.expected_raw_daily_bars_per_ticker == 90
    assert plan.expected_raw_hourly_bars_per_ticker >= 90 * 4
    assert plan.expected_raw_thirty_minute_bars_per_ticker >= 90 * 7
    assert plan.expected_provider_requests == 283 + len(
        EQUITY_VALIDATION_UNIVERSE.equity_tickers
    )
    assert plan.expected_provider_requests == 292
    assert plan.policy_sha256 == EQUITY_VALIDATION_HISTORY_POLICY.sha256


def test_equity_validation_daily_plan_refreshes_one_session_and_nine_tickers():
    plan = build_equity_validation_acquisition_plan(
        date(2026, 10, 2),
        daily=True,
    )

    assert plan.adjusted_sessions == (date(2026, 10, 2),)
    assert len(plan.raw_sessions) == 90
    assert plan.expected_provider_requests == 10


@pytest.mark.parametrize("defect", [None, "before_release", "missing_minute"])
def test_forward_minute_collection_is_exact_live_observed_and_release_bound(defect):
    cutoff = datetime(2026, 10, 2, 13, 45, tzinfo=timezone.utc)
    observed_at = cutoff + timedelta(minutes=15, seconds=1)
    opened = cutoff - timedelta(minutes=15)
    rows = [dict(t=int((opened + timedelta(minutes=index)).timestamp() * 1000),
        o=100, h=101, l=99, c=100, v=100, vw=100, n=1) for index in range(15)]
    if defect == "missing_minute":
        rows.pop(3)
    transport = QueueTransport(tuple(PolygonHttpResponse(200, json.dumps({"results": rows}).encode()) for _ in _identities()))
    cohort_id = uuid5(NAMESPACE_URL, "forward-minute-new-cohort")
    source = PolygonEquityValidationHistorySource(transport, _identities(), cohort_id=cohort_id,
        clock=lambda: cutoff if defect == "before_release" else observed_at)
    if defect:
        with pytest.raises(ValueError, match="Developer release" if defect == "before_release" else "coverage is incomplete"):
            source.collect_forward_minute_inputs(cutoff)
        assert len(transport.calls) == (0 if defect == "before_release" else 1)
    else:
        facts = source.collect_forward_minute_inputs(cutoff)
        assert len(facts) == 9 * 15
        assert source.provider_requests == 9
        assert {row.cohort_id for row in facts} == {cohort_id}
        assert {row.revision.ticker for row in facts} == set(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
        assert all(row.revision.availability_mode.value == "LIVE_OBSERVED" for row in facts)
        assert all(row.revision.system_observed_at == observed_at for row in facts)
        assert all(row.revision.bar_end <= cutoff for row in facts)


def test_forward_context_acquisition_keeps_true_observation_clock_and_complete_prior_sessions():
    import exchange_calendars
    import pandas as pd

    cutoff = datetime(2026, 10, 2, 13, 45, tzinfo=timezone.utc)
    observed_at = cutoff + timedelta(minutes=15, seconds=1)
    calendar = exchange_calendars.get_calendar("XNYS")
    previous = calendar.sessions[calendar.sessions < pd.Timestamp(cutoff.date())][-21:]
    rows = []
    for session in previous:
        start = calendar.session_open(session).to_pydatetime()
        close = calendar.session_close(session).to_pydatetime()
        while start < close:
            rows.append(dict(t=int(start.timestamp() * 1000), o=100, h=101, l=99, c=100, v=100, vw=100, n=1))
            start += timedelta(minutes=30)
    rows.append(dict(t=int((cutoff - timedelta(minutes=15)).timestamp() * 1000),
        o=100, h=101, l=99, c=100, v=100, vw=100, n=1))
    transport = QueueTransport(tuple(PolygonHttpResponse(200, json.dumps({"results": rows}).encode()) for _ in _identities()))
    source = PolygonEquityValidationHistorySource(transport, _identities(),
        cohort_id=uuid5(NAMESPACE_URL, "new-forward-context"), clock=lambda: observed_at)
    facts = source.collect_forward_context_inputs(cutoff)
    assert source.provider_requests == 9
    assert {row.revision.ticker for row in facts} == set(EQUITY_VALIDATION_UNIVERSE.equity_tickers)
    assert all(row.revision.system_observed_at == observed_at for row in facts)
    assert all(row.revision.availability_mode.value == "LIVE_OBSERVED" for row in facts)
    assert all(row.revision.bar_end <= cutoff for row in facts)
    for ticker in EQUITY_VALIDATION_UNIVERSE.equity_tickers:
        assert sum(row.revision.ticker == ticker and row.revision.interval == "30m" for row in facts) >= 202
        assert sum(row.revision.ticker == ticker and row.revision.interval == "1d" for row in facts) == 21


def test_v3_portal_history_collects_adjusted_daily_range_once_per_ticker():
    identities = _identities()
    identity = identities[0]
    plan = build_realtime_equity_portal_history_plan(date(2026, 10, 8))
    rows = [
        dict(
            t=int(datetime.combine(
                session,
                datetime.min.time(),
                tzinfo=ZoneInfo("America/New_York"),
            ).timestamp() * 1000),
            o=100, h=101, l=99, c=100, v=1000, vw=100, n=10,
        )
        for session in plan.adjusted_sessions
    ]
    transport = QueueTransport((
        PolygonHttpResponse(200, json.dumps({"results": rows}).encode()),
    ))
    source = PolygonEquityValidationHistorySource(
        transport, identities, cohort_id=uuid5(NAMESPACE_URL, "v3-adjusted-daily-range"),
    )

    facts = source._collect_adjusted_daily_identity(
        identity, plan=plan, observed_at=datetime(2026, 10, 10, tzinfo=timezone.utc),
    )

    assert len(transport.calls) == 1
    assert "/range/1/day/" in transport.calls[0][0]
    assert "/grouped/" not in transport.calls[0][0]
    assert len(facts) == 283
    assert tuple(fact.revision.session_date for fact in facts) == plan.adjusted_sessions
    assert all(fact.revision.adjusted for fact in facts)

    drifted = [dict(row) for row in rows]
    drifted[0]["t"] -= 24 * 60 * 60 * 1000
    bad_source = PolygonEquityValidationHistorySource(
        QueueTransport((
            PolygonHttpResponse(200, json.dumps({"results": drifted}).encode()),
        )),
        identities,
        cohort_id=uuid5(NAMESPACE_URL, "v3-adjusted-daily-clock-drift"),
    )
    with pytest.raises(ValueError, match="session clock drift"):
        bad_source._collect_adjusted_daily_identity(
            identity,
            plan=plan,
            observed_at=datetime(2026, 10, 10, tzinfo=timezone.utc),
        )


def test_v3_portal_history_collects_native_adjusted_weekly_range():
    identities = _identities()
    identity = identities[0]
    plan = build_realtime_equity_portal_history_plan(date(2026, 10, 8))
    rows = []
    for session in plan.weekly_sessions:
        iso = session.isocalendar()
        provider_start = datetime.combine(
            date.fromisocalendar(iso.year, iso.week, 7),
            datetime.min.time(),
            tzinfo=ZoneInfo("America/New_York"),
        )
        rows.append(dict(
            t=int(provider_start.timestamp() * 1000),
            o=100, h=101, l=99, c=100, v=5000, vw=100, n=50,
        ))
    transport = QueueTransport((
        PolygonHttpResponse(200, json.dumps({"results": rows}).encode()),
    ))
    source = PolygonEquityValidationHistorySource(
        transport, identities, cohort_id=uuid5(NAMESPACE_URL, "v3-adjusted-weekly-range"),
    )

    facts = source._collect_native_weekly_identity(
        identity, plan=plan, observed_at=datetime(2026, 10, 10, tzinfo=timezone.utc),
    )

    assert len(transport.calls) == 1
    assert "/range/1/week/" in transport.calls[0][0]
    first_iso = plan.weekly_start.isocalendar()
    last_iso = plan.weekly_end.isocalendar()
    request_start = date.fromisocalendar(first_iso.year, first_iso.week, 7)
    request_end = date.fromisocalendar(last_iso.year, last_iso.week, 7)
    assert f"/{request_start.isoformat()}/{request_end.isoformat()}" in transport.calls[0][0]
    assert len(facts) == 261
    assert tuple(fact.revision.session_date for fact in facts) == plan.weekly_sessions
    assert all(fact.revision.adjusted for fact in facts)


def test_v3_range_dispatch_is_owned_only_by_portal_history_collector():
    portal_source = inspect.getsource(PolygonEquityValidationHistorySource.collect_portal_history)
    validation_source = inspect.getsource(PolygonEquityValidationHistorySource.collect)

    assert "_collect_adjusted_daily_identity" in portal_source
    assert "_collect_native_weekly_identity" in portal_source
    assert 'policy.version == "realtime-equity-portal-history-v2"' in portal_source
    assert "_collect_adjusted_daily_identity" not in validation_source


def test_adjusted_daily_identity_includes_adjustment_basis():
    source = _bar(
        "AAPL",
        1,
        "1d",
        datetime(2026, 10, 2, 13, 30, tzinfo=timezone.utc),
        datetime(2026, 10, 2, 20, 0, tzinfo=timezone.utc),
        adjusted=True,
        index=1,
    ).revision

    first = bind_equity_validation_adjustment_identity(source)
    repeated = bind_equity_validation_adjustment_identity(source)

    assert first.bar_revision_id == repeated.bar_revision_id
    assert first.bar_revision_id != source.bar_revision_id
    assert "ADJUSTMENT_BASIS_IDENTITY_V1" in first.quality_codes


def test_equity_history_retries_only_transient_provider_responses():
    sleeps = []
    source = PolygonEquityValidationHistorySource(
        QueueTransport((
            PolygonHttpResponse(429, b"{}", (("Retry-After", "5"),)),
            PolygonHttpResponse(200, b'{"results": []}'),
        )),
        _identities(),
        cohort_id=uuid5(NAMESPACE_URL, "equity-validation-retry-test"),
        sleep=sleeps.append,
    )

    assert source._get("/test", {}) == ()
    assert source.provider_requests == 2
    assert sleeps == [5.0]


def test_equity_history_parallelism_is_explicitly_bounded():
    assert EquityValidationSourceLimits().maximum_workers == 4
    with pytest.raises(ValueError, match="workers must be in"):
        EquityValidationSourceLimits(maximum_workers=9)


def test_equity_history_reserves_request_budget_before_concurrent_dispatch():
    from concurrent.futures import ThreadPoolExecutor

    transport = QueueTransport(
        PolygonHttpResponse(200, b'{"results": []}') for _ in range(36)
    )
    source = PolygonEquityValidationHistorySource(
        transport, _identities(),
        cohort_id=uuid5(NAMESPACE_URL, "recovery-concurrent-request-cap"),
        limits=EquityValidationSourceLimits(maximum_requests=36),
    )

    def attempt(index):
        try:
            source._get(f"/test/{index}", {})
            return True
        except ValueError as error:
            assert "request bound" in str(error)
            return False

    with ThreadPoolExecutor(max_workers=8) as executor:
        outcomes = tuple(executor.map(attempt, range(64)))

    assert sum(outcomes) == 36
    assert len(transport.calls) == source.provider_requests == 36


def test_equity_history_reserves_response_bytes_before_dispatch():
    transport = QueueTransport((PolygonHttpResponse(200, b'{"results": []}'),))
    source = PolygonEquityValidationHistorySource(
        transport, _identities(), cohort_id=uuid5(NAMESPACE_URL, "byte-reservation"),
        limits=EquityValidationSourceLimits(maximum_response_bytes=32, maximum_response_bytes_each=32),
    )
    assert source._get("/test", {}) == ()
    with pytest.raises(ValueError, match="response-byte budget"):
        source._get("/test", {})
    assert len(transport.calls) == source.provider_requests == 1


@pytest.mark.parametrize("defect", [None, "missing_minute", "wrong_daily_session"])
def test_session_recovery_is_one_day_native_only_and_preserves_receipt_time(defect):
    calendar = pytest.importorskip("exchange_calendars").get_calendar("XNYS")
    session = date(2026, 10, 9)
    opened = calendar.session_open(str(session)).to_pydatetime()
    observed = datetime(2026, 10, 10, 22, tzinfo=timezone.utc)

    class RecoveryTransport:
        def __init__(self):
            self.calls = []

        def get(self, url, params):
            self.calls.append((url, params))
            width = 5 if "/5/minute/" in url else 1
            count = 1 if "/day/" in url else 390 // width
            rows = [dict(t=int((opened + timedelta(minutes=width * index)).timestamp() * 1000),
                         o=100, h=102, l=99, c=101, v=1000, vw=101, n=10)
                    for index in range(count)]
            if defect == "missing_minute" and "/1/minute/" in url:
                rows.pop(3)
            if defect == "wrong_daily_session" and "/day/" in url:
                rows[0]["t"] -= 24 * 60 * 60 * 1000
            return PolygonHttpResponse(200, json.dumps({"results": rows}).encode())

    transport = RecoveryTransport()
    source = PolygonEquityValidationHistorySource(
        transport, _identities(), cohort_id=uuid5(NAMESPACE_URL, "one-session-recovery"),
        limits=EquityValidationSourceLimits(maximum_requests=36, maximum_response_bytes=32 * 1024 * 1024,
                                          maximum_response_bytes_each=2 * 1024 * 1024),
        clock=lambda: observed,
    )
    if defect:
        with pytest.raises(ValueError, match="coverage is incomplete"):
            source.collect_session_recovery(session)
    else:
        result = source.collect_session_recovery(session)
        assert result.provider_requests == len(transport.calls) == 27
        assert len(result.facts) == 9 * (390 + 78 + 1)
        assert all(fact.revision.system_observed_at == observed for fact in result.facts)
        assert all(fact.revision.session_date == session for fact in result.facts)
        assert all(fact.revision.availability_mode.value == "HISTORICAL_RECONSTRUCTED" for fact in result.facts)
        assert all("/week/" not in url and "/month/" not in url for url, _ in transport.calls)
        assert {fact.revision.interval for fact in result.facts} == {"1m", "5m", "1d"}


def test_future_portal_source_accepts_injected_universe_but_legacy_collect_stays_fixed():
    universe = UniverseManifest(
        version="test-two-equities-v1",
        equity_tickers=("AAPL", "MSFT"),
        option_underlyings=(),
    )
    identities = tuple(
        EquityValidationIdentity(ticker, _security(ticker), index)
        for index, ticker in enumerate(universe.equity_tickers, start=1)
    )
    source = PolygonEquityValidationHistorySource(
        QueueTransport(()),
        identities,
        cohort_id=uuid5(NAMESPACE_URL, "injected-universe-source-test"),
        universe=universe,
    )

    assert source.universe == universe
    with pytest.raises(ValueError, match="fixed universe"):
        source.collect(
            date(2026, 10, 2),
            split_coverage={},
            split_actions=(),
        )


def test_equity_history_follows_bounded_same_path_pagination():
    start_ms = int(datetime(2026, 8, 19, 13, 30, tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(datetime(2026, 10, 2, tzinfo=timezone.utc).timestamp() * 1000)
    path = "/v2/aggs/ticker/AAPL/range/30/minute/2026-05-27/2026-10-02"
    next_path = f"/v2/aggs/ticker/AAPL/range/30/minute/{start_ms}/{end_ms}"
    next_url = (
        f"https://api.massive.com{next_path}?cursor=second&apiKey=must-not-persist"
    )
    transport = QueueTransport((
        PolygonHttpResponse(
            200,
            ('{"results": [{"value": 1}], "next_url": "' + next_url + '"}').encode(),
        ),
        PolygonHttpResponse(200, b'{"results": [{"value": 2}]}'),
    ))
    source = PolygonEquityValidationHistorySource(
        transport,
        _identities(),
        cohort_id=uuid5(NAMESPACE_URL, "equity-validation-pagination-test"),
    )

    assert source._get(path, {"limit": "1"}) == (
        {"value": 1},
        {"value": 2},
    )
    assert transport.calls == [
        (f"https://api.massive.com{path}", {"limit": "1"}),
        (f"https://api.massive.com{next_path}", {"cursor": "second"}),
    ]
    assert source.provider_requests == 2


def test_equity_history_rejects_hostile_pagination():
    source = PolygonEquityValidationHistorySource(
        QueueTransport((
            PolygonHttpResponse(
                200,
                b'{"results": [], "next_url": "https://example.com/v2/aggs/ticker/AAPL/range/30/minute/2026-05-27/2026-10-02?cursor=stolen"}',
            ),
        )),
        _identities(),
        cohort_id=uuid5(NAMESPACE_URL, "equity-validation-hostile-pagination-test"),
    )

    with pytest.raises(ValueError, match="pagination identity drifted"):
        source._get(
            "/v2/aggs/ticker/AAPL/range/30/minute/2026-05-27/2026-10-02",
            {},
        )


def test_equity_aggregate_path_identity_rejects_scope_drift():
    expected = _aggregate_path_identity(
        "/v2/aggs/ticker/AAPL/range/30/minute/2026-05-27/2026-10-02"
    )
    assert expected == ("AAPL", date(2026, 5, 27), date(2026, 10, 2))
    with pytest.raises(ValueError, match="endpoint is invalid"):
        _aggregate_path_identity(
            "/v2/aggs/ticker/AAPL/range/30/minute/earlier/2026-10-02"
        )


def test_equity_aggregate_path_identity_supports_explicit_five_minute_scope():
    path = "/v2/aggs/ticker/AAPL/range/5/minute/2026-10-02/2026-10-02"

    assert _aggregate_path_identity(path, 5) == (
        "AAPL",
        date(2026, 10, 2),
        date(2026, 10, 2),
    )
    with pytest.raises(ValueError, match="path shape"):
        _aggregate_path_identity(path)


def test_portal_history_collects_one_complete_native_five_minute_session():
    calendar = pytest.importorskip("exchange_calendars").get_calendar("XNYS")
    session = pytest.importorskip("pandas").Timestamp("2026-10-02")
    opened = calendar.session_open(session).to_pydatetime().astimezone(timezone.utc)
    closed = calendar.session_close(session).to_pydatetime().astimezone(timezone.utc)
    rows = []
    cursor = opened
    while cursor < closed:
        rows.append({
            "t": int(cursor.timestamp() * 1000),
            "o": 100,
            "h": 102,
            "l": 99,
            "c": 101,
            "v": 1000,
        })
        cursor += timedelta(minutes=5)
    transport = QueueTransport((
        PolygonHttpResponse(
            200,
            ("{\"results\":" + json.dumps(rows) + "}").encode(),
        ),
    ))
    source = PolygonEquityValidationHistorySource(
        transport,
        _identities(),
        cohort_id=uuid5(NAMESPACE_URL, "realtime-portal-native-five-test"),
        clock=lambda: datetime(2026, 10, 2, 23, 0, tzinfo=timezone.utc),
    )
    full_plan = build_realtime_equity_portal_history_plan(date(2026, 10, 2))
    counts = tuple(
        (interval, basis, 78 if (interval, basis) == ("5m", "RAW_UNADJUSTED") else count)
        for interval, basis, count in full_plan.expected_counts_per_ticker
    )
    plan = replace(
        full_plan,
        raw_sessions=(date(2026, 10, 2),),
        raw_start=date(2026, 10, 2),
        raw_end=date(2026, 10, 2),
        expected_counts_per_ticker=counts,
    )

    facts = source._collect_native_five_minute_identity(
        _identities()[0],
        plan=plan,
        observed_at=datetime(2026, 10, 2, 23, 0, tzinfo=timezone.utc),
    )

    assert len(facts) == 78
    assert all(fact.revision.interval == "5m" for fact in facts)
    assert all(fact.revision.adjusted is False for fact in facts)
    assert "/range/5/minute/" in transport.calls[0][0]
    assert transport.calls[0][1] == {
        "adjusted": "false",
        "sort": "asc",
        "limit": "50000",
    }


def test_equity_history_rejects_nonadvancing_range_pagination():
    path = "/v2/aggs/ticker/AAPL/range/30/minute/2026-05-27/2026-10-02"
    source = PolygonEquityValidationHistorySource(
        QueueTransport((
            PolygonHttpResponse(
                200,
                (
                    '{"results": [], "next_url": "https://api.massive.com'
                    + path
                    + '?cursor=repeated"}'
                ).encode(),
            ),
        )),
        _identities(),
        cohort_id=uuid5(NAMESPACE_URL, "equity-validation-nonadvancing-test"),
    )

    with pytest.raises(ValueError, match="range did not advance"):
        source._get(path, {})