from contextlib import contextmanager
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from urllib.parse import urlparse
from uuid import UUID

import pytest

from equity.domain import BarAvailabilityMode, BarSourceKind
from equity.polygon import normalize_native_bars
from realtime.advanced_equity_preparation import (
    AdvancedEquityPreparationResident,
    BoundedPolygonEquityPreparationClient,
    EquityPreparationChunk,
    EquityPreparationBlockedError,
    EquityPreparationClaim,
    EquityPreparationFetchRequest,
    EquityPreparationLimits,
    EquityPreparationSourceError,
    EquityPreparationStage,
    EquityPreparationUnavailableError,
    MassiveEquityPreparationSource,
    advanced_equity_live_priority_cursor,
    preparation_retry_delay_seconds,
    _advanced_rest_facts,
)
from realtime.advanced_realtime_desired_state import DesiredMemberKind
from realtime.advanced_realtime_desired_state_repository import (
    _equity_preparation_targets,
)
from realtime.equity_portal_history import (
    build_realtime_equity_portal_history_plan,
)


UTC = timezone.utc


def _claim(**changes):
    values = {
        "revision_id": UUID(int=1),
        "intent_id": UUID(int=6),
        "security_id": UUID(int=2),
        "member_kind": DesiredMemberKind.EQUITY,
        "ticker": "XLK",
        "stage": EquityPreparationStage.NATIVE_INTRADAY,
        "stage_order": 30,
        "target": {},
        "watermark": {},
        "coverage": {"available_intervals": {}},
        "attempts": 1,
        "failure_streak": 0,
        "lease_owner": UUID(int=3),
        "lease_epoch": 1,
        "lease_until": datetime(2026, 10, 30, 22, 5, tzinfo=UTC),
        "source_cohort_id": UUID(int=4),
        "instrument_id": 10,
    }
    values.update(changes)
    return EquityPreparationClaim(**values)


def test_live_session_priority_performs_no_database_or_provider_work():
    class NoDatabase:
        def transaction(self, *_args, **_kwargs):
            raise AssertionError("live priority cannot open a preparation transaction")

    class NoSource:
        def fetch(self, _request):
            raise AssertionError("live priority cannot call the provider")

    resident = AdvancedEquityPreparationResident(
        NoDatabase(),
        NoSource(),
        owner_id=UUID(int=5),
        clock=lambda: datetime(2026, 10, 30, 15, 0, tzinfo=UTC),
    )

    result = resident.run_once()

    assert result.status == "LIVE_PRIORITY"
    assert result.reason == "LIFECYCLE_MARKET_HOURS"


def test_recent_stocks_ingress_has_priority_before_claiming_work():
    cursor = MagicMock()
    cursor.fetchone.return_value = (True,)

    assert advanced_equity_live_priority_cursor(
        cursor,
        checked_at=datetime(2026, 10, 30, 12, 0, tzinfo=UTC),
    )
    statement = cursor.execute.call_args.args[0]
    assert "FROM rt.stream_sessions" in statement
    assert "heartbeat_at" in statement
    assert "feed='STOCKS'" in statement


def test_retry_backoff_uses_failure_streak_not_total_attempts():
    claim = _claim(
        attempts=100,
        failure_streak=0,
    )
    limits = EquityPreparationLimits(
        retry_initial_seconds=15,
        retry_maximum_seconds=900,
    )

    assert claim.attempts == 100
    assert preparation_retry_delay_seconds(
        limits,
        failure_streak=claim.failure_streak,
    ) == 15
    assert preparation_retry_delay_seconds(
        limits,
        failure_streak=5,
    ) == 480
    assert issubclass(
        EquityPreparationUnavailableError,
        EquityPreparationSourceError,
    )


def test_preparation_target_reuses_complete_portal_history_contract():
    targets = {
        stage: target
        for stage, _order, target in _equity_preparation_targets(
            SimpleNamespace(
                effective_session_date=date(2026, 11, 2),
            )
        )
    }

    adjusted = targets["ADJUSTED_DAILY"]
    assert tuple(adjusted["interval_sessions"]) == (
        "1d",
        "1wk",
        "1mo",
    )
    assert adjusted["expected_counts"] == {
        "1d:ADJUSTED": 283,
        "1wk:ADJUSTED": 261,
        "1mo:ADJUSTED": 60,
    }
    history = build_realtime_equity_portal_history_plan(
        date(2026, 10, 30),
        equity_ticker_count=1,
    )
    assert targets["NATIVE_INTRADAY"]["expected_counts"] == {
        "1m:RAW_UNADJUSTED": history.expected_count(
            "1m",
            "RAW_UNADJUSTED",
        ),
        "5m:RAW_UNADJUSTED": history.expected_count(
            "5m",
            "RAW_UNADJUSTED",
        ),
    }
    assert targets["TECHNICAL_WARMUP"][
        "required_adjusted_intervals"
    ] == adjusted["expected_counts"]


@pytest.mark.parametrize(
    (
        "interval",
        "session",
        "provider_start",
        "request_start",
        "request_end",
    ),
    (
        (
            "1wk",
            date(2025, 8, 22),
            datetime(2025, 8, 17, 4, 0, tzinfo=UTC),
            date(2025, 8, 17),
            date(2025, 8, 22),
        ),
        (
            "1mo",
            date(2026, 8, 31),
            datetime(2026, 8, 1, 4, 0, tzinfo=UTC),
            date(2026, 8, 1),
            date(2026, 8, 31),
        ),
    ),
)
def test_adjusted_extension_chunks_use_provider_bucket_boundaries(
    interval,
    session,
    provider_start,
    request_start,
    request_end,
):
    class FakeClient:
        def __init__(self):
            self.calls = []

        def fetch_native_bars(
            self,
            ticker,
            requested_interval,
            start,
            end,
            *,
            adjusted,
        ):
            self.calls.append((
                ticker,
                requested_interval,
                start,
                end,
                adjusted,
            ))
            return ({
                "t": int(provider_start.timestamp() * 1000),
                "o": 100,
                "h": 102,
                "l": 99,
                "c": 101,
                "v": 5000,
            },)

    client = FakeClient()
    claim = _claim(
        stage=EquityPreparationStage.ADJUSTED_DAILY,
        stage_order=20,
        target={
            "interval_sessions": {
                interval: [session.isoformat()],
            }
        },
    )
    request = EquityPreparationFetchRequest(
        claim,
        datetime(2026, 9, 1, tzinfo=UTC),
        EquityPreparationLimits(),
    )

    chunk = MassiveEquityPreparationSource(client).fetch(request)

    assert client.calls == [(
        "XLK",
        interval,
        request_start,
        request_end,
        True,
    )]
    assert chunk.complete
    assert chunk.coverage_after["available_intervals"] == {
        f"{interval}:ADJUSTED": 1,
    }
    assert len(chunk.bars) == 1


def test_bootstrap_weekly_revalidation_replaces_full_interval_without_resetting_progress():
    sessions = (date(2026, 10, 9), date(2026, 10, 16), date(2026, 10, 23), date(2026, 10, 30))

    class FakeClient:
        def fetch_native_bars(self, ticker, interval, start, end, *, adjusted):
            assert (ticker, interval, start, end, adjusted) == (
                "XLK", "1wk", date(2026, 10, 4), date(2026, 10, 30), True,
            )
            return tuple({
                "t": int(datetime(2026, 10, day, 4, tzinfo=UTC).timestamp() * 1000),
                "o": 100, "h": 102, "l": 99, "c": 101, "v": 5000,
            } for day in (4, 11, 18, 25))

    claim = _claim(
        stage=EquityPreparationStage.ADJUSTED_DAILY, stage_order=20,
        target={"interval_sessions": {"1wk": [value.isoformat() for value in sessions]}},
        watermark={"interval_index": 0, "session_index": 2},
        coverage={"available_intervals": {"1wk:ADJUSTED": 2}},
    )
    request = EquityPreparationFetchRequest(
        claim, datetime(2026, 11, 2, 13, tzinfo=UTC), EquityPreparationLimits(),
        history_sessions=sessions[2:], revalidate_weekly_history=True,
    )

    chunk = MassiveEquityPreparationSource(FakeClient()).fetch(request)

    assert chunk.complete
    assert len(chunk.bars) == 4
    assert tuple(fact.revision.session_date for fact in chunk.bars) == sessions
    assert chunk.watermark_after == {"interval_index": 1, "session_index": 0}
    assert chunk.coverage_after == {"available_intervals": {"1wk:ADJUSTED": 4}}
    assert chunk.source_lineage["coordinate_contract"] == "MASSIVE_WEEK_SUNDAY_START_V2"
    assert chunk.source_lineage["whole_interval_revalidated"] is True


def test_preparation_rest_ids_are_stable_and_scoped_to_cohort_and_basis():
    rows = ({"t": 1793332800000, "o": 100, "h": 102, "l": 99, "c": 101, "v": 5000},)
    observed = datetime(2026, 11, 2, 13, tzinfo=UTC)
    adjusted = normalize_native_bars(
        UUID(int=2), "XLK", "1d", rows, observed_at=observed, adjusted=True,
        availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
        ingestion_segment_id=UUID(int=55),
    )
    raw = normalize_native_bars(
        UUID(int=2), "XLK", "1d", rows, observed_at=observed, adjusted=False,
        availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
        ingestion_segment_id=UUID(int=55),
    )
    claim = _claim()
    original_id = adjusted[0].bar_revision_id
    (first,) = _advanced_rest_facts(adjusted, claim=claim)
    (replayed,) = _advanced_rest_facts(adjusted, claim=claim)
    (other_cohort,) = _advanced_rest_facts(adjusted, claim=_claim(source_cohort_id=UUID(int=99)))
    (other_basis,) = _advanced_rest_facts(raw, claim=claim)

    assert first.revision.bar_revision_id == replayed.revision.bar_revision_id
    assert len({first.revision.bar_revision_id, other_cohort.revision.bar_revision_id,
                other_basis.revision.bar_revision_id, original_id}) == 4
    assert adjusted[0].bar_revision_id == original_id
    assert first.revision.system_observed_at == observed
    assert first.revision.payload_sha256 == adjusted[0].payload_sha256


def test_bootstrap_derivation_uses_real_identity_clock_and_checks_exact_source_identity(monkeypatch):
    checked = datetime(2026, 11, 2, 13, tzinfo=UTC)
    opened = datetime(2026, 10, 30, 13, 30, tzinfo=UTC)
    claim = _claim(
        stage=EquityPreparationStage.DERIVED_INTERVALS,
        target={"target_intervals": ["15m"], "source_sessions": ["2026-10-30"]},
    )
    revisions = normalize_native_bars(
        claim.security_id, claim.ticker, "5m", tuple({
            "t": int((opened + timedelta(minutes=5 * offset)).timestamp() * 1000),
            "o": 100, "h": 102, "l": 99, "c": 101, "v": 5000,
        } for offset in range(3)),
        observed_at=checked, adjusted=False,
        availability_mode=BarAvailabilityMode.HISTORICAL_RECONSTRUCTED,
        ingestion_segment_id=UUID(int=55),
    )
    sources = _advanced_rest_facts(revisions, claim=claim)
    output = replace(sources[0], revision=replace(
        sources[0].revision, interval="15m", bar_end=opened + timedelta(minutes=15),
        source_kind=BarSourceKind.DERIVED,
        source_bar_revision_ids=tuple(fact.revision.bar_revision_id for fact in sources),
    ))
    security = SimpleNamespace(
        active=True, security_id=claim.security_id, ticker=claim.ticker,
        security_revision_id=UUID(int=88),
    )
    load_identity = MagicMock(return_value=security)
    monkeypatch.setattr(
        "realtime.advanced_equity_preparation.load_current_five_minute_facts_cursor",
        lambda *_args, **_kwargs: sources,
    )
    monkeypatch.setattr("realtime.advanced_equity_preparation.load_derivation_security_cursor", load_identity)
    monkeypatch.setattr(
        "realtime.advanced_equity_preparation.derive_realtime_interval_facts",
        lambda *_args, **_kwargs: (output,),
    )

    @contextmanager
    def transaction(*_args, **_kwargs):
        yield SimpleNamespace(cursor=MagicMock())

    resident = AdvancedEquityPreparationResident(
        SimpleNamespace(transaction=transaction), SimpleNamespace(), owner_id=UUID(int=5),
        bootstrap_history=lambda *_args: (None, ()), bootstrap_revision_id=claim.revision_id,
    )
    request = EquityPreparationFetchRequest(claim, checked, EquityPreparationLimits())
    chunk = resident._derive(request)

    assert load_identity.call_args.kwargs["window_end"] == checked
    assert load_identity.call_args.kwargs["as_of"] == checked
    assert chunk.bars[0].revision.bar_end == opened + timedelta(minutes=15)
    assert chunk.source_lineage["security_revision_id"] == str(security.security_revision_id)
    assert chunk.source_lineage["security_reference_clock"] == checked.isoformat()
    security.security_id = UUID(int=99)
    with pytest.raises(EquityPreparationBlockedError, match="REFERENCE_IDENTITY_DIVERGED"):
        resident._derive(request)


class _JsonResponse:
    status_code = 200

    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


class _WholeMarketPaginatingSession:
    """Paginates corporate actions forever unless the ticker filter is sent."""

    def __init__(self, overview):
        self.overview = overview
        self.calls = []

    def get(self, url, *, params, headers, timeout):
        self.calls.append((urlparse(url).path, dict(params)))
        path = urlparse(url).path
        if path.startswith("/v3/reference/tickers/"):
            return _JsonResponse({"results": self.overview})
        if params.get("ticker") == self.overview["ticker"]:
            return _JsonResponse({"results": []})
        return _JsonResponse({
            "results": [{"ticker": "ZZZ"}],
            "next_url": f"https://api.massive.com{path}?cursor=next",
        })


def _reference_chunk(overview):
    session = _WholeMarketPaginatingSession(overview)
    source = MassiveEquityPreparationSource(
        BoundedPolygonEquityPreparationClient("test-key", session=session)
    )
    chunk = source.fetch(
        EquityPreparationFetchRequest(
            _claim(
                ticker=overview["ticker"],
                stage=EquityPreparationStage.REFERENCE,
                stage_order=10,
                security_id=None,
                target={
                    "history_through_session": "2026-10-30",
                    "window_start": "2026-05-03",
                    "window_end": "2026-10-30",
                },
            ),
            datetime(2026, 11, 2, 12, 0, tzinfo=UTC),
            EquityPreparationLimits(),
        )
    )
    return chunk, session.calls


def test_provider_fence_runs_before_each_network_request_and_can_stop_a_chunk():
    session = _WholeMarketPaginatingSession({"ticker": "SPY"})
    client = BoundedPolygonEquityPreparationClient("test-key", session=session)
    calls = []

    def fence(timeout_seconds):
        calls.append(timeout_seconds)
        if len(calls) == 2:
            raise EquityPreparationBlockedError("LEASE_HEADROOM_EXHAUSTED")

    client.begin_chunk(maximum_requests=3, maximum_response_bytes=8192, before_request=fence)
    client._request("https://api.massive.com/v3/reference/tickers/SPY", {})
    with pytest.raises(EquityPreparationBlockedError, match="LEASE_HEADROOM_EXHAUSTED"):
        client._request("https://api.massive.com/v3/reference/tickers/SPY", {})
    assert len(session.calls) == 1
    assert calls == [client.timeout_seconds, client.timeout_seconds]
    assert client.chunk_metrics()[0] == 1
    client.begin_chunk(maximum_requests=1, maximum_response_bytes=8192)
    client._request("https://api.massive.com/v3/reference/tickers/SPY", {})
    assert len(session.calls) == 2
    assert len(calls) == 2


ETF_OVERVIEW = {"ticker": "SPY", "type": "ETF", "active": True}
STOCK_OVERVIEW = {
    "ticker": "AAPL",
    "type": "CS",
    "active": True,
    "sic_code": "3571",
}


@pytest.mark.parametrize("overview", (ETF_OVERVIEW, STOCK_OVERVIEW))
def test_reference_chunk_needs_exactly_three_ticker_filtered_requests(overview):
    chunk, calls = _reference_chunk(overview)

    assert chunk.complete is True
    assert chunk.provider_requests == 3
    assert [path for path, _params in calls] == [
        f"/v3/reference/tickers/{overview['ticker']}",
        "/stocks/v1/splits",
        "/stocks/v1/dividends",
    ]
    assert calls[1][1]["ticker"] == overview["ticker"]
    assert calls[2][1]["ticker"] == overview["ticker"]
    assert chunk.coverage_after["corporate_actions_ready"] is True


def test_reference_chunk_fails_closed_on_filtered_corporate_action_drift():
    class DriftingSession(_WholeMarketPaginatingSession):
        def get(self, url, *, params, headers, timeout):
            if "splits" in url:
                return _JsonResponse({"results": [{"ticker": "ZZZ"}]})
            return super().get(
                url,
                params=params,
                headers=headers,
                timeout=timeout,
            )

    source = MassiveEquityPreparationSource(
        BoundedPolygonEquityPreparationClient(
            "test-key",
            session=DriftingSession(ETF_OVERVIEW),
        )
    )
    with pytest.raises(
        EquityPreparationSourceError,
        match="REFERENCE_SOURCE_REQUEST_FAILED",
    ):
        source.fetch(
            EquityPreparationFetchRequest(
                _claim(
                    ticker="SPY",
                    stage=EquityPreparationStage.REFERENCE,
                    stage_order=10,
                    security_id=None,
                    target={
                        "history_through_session": "2026-10-30",
                        "window_start": "2026-05-03",
                        "window_end": "2026-10-30",
                    },
                ),
                datetime(2026, 11, 2, 12, 0, tzinfo=UTC),
                EquityPreparationLimits(),
            )
        )


@pytest.mark.parametrize(
    ("overview", "sector_ready", "sector_not_applicable"),
    (
        (ETF_OVERVIEW, False, True),
        ({**ETF_OVERVIEW, "ticker": "VIX", "type": "INDEX"}, False, True),
        (STOCK_OVERVIEW, True, False),
        ({**STOCK_OVERVIEW, "sic_code": None}, False, False),
    ),
)
def test_reference_coverage_records_sector_applicability_explicitly(
    overview,
    sector_ready,
    sector_not_applicable,
):
    chunk, _calls = _reference_chunk(overview)

    assert chunk.coverage_after["sector_ready"] is sector_ready
    assert (
        chunk.coverage_after["sector_not_applicable"]
        is sector_not_applicable
    )


def _warmup_request(reference_coverage):
    stage_rows = [
        ("REFERENCE", "COMPLETE", reference_coverage),
        (
            "ADJUSTED_DAILY",
            "COMPLETE",
            {"available_intervals": {"1d:ADJUSTED": 2}},
        ),
        ("NATIVE_INTRADAY", "COMPLETE", {"available_intervals": {}}),
        (
            "DERIVED_INTERVALS",
            "COMPLETE",
            {"available_intervals": {"1h:RAW_UNADJUSTED": 2}},
        ),
    ]
    cursor = MagicMock()
    cursor.fetchall.return_value = stage_rows

    @contextmanager
    def transaction(*_args, **_kwargs):
        yield SimpleNamespace(cursor=cursor)

    resident = AdvancedEquityPreparationResident(
        SimpleNamespace(transaction=transaction),
        MagicMock(),
        owner_id=UUID(int=9),
    )
    return resident._warmup(
        EquityPreparationFetchRequest(
            _claim(
                ticker="SPY",
                stage=EquityPreparationStage.TECHNICAL_WARMUP,
                stage_order=50,
                target={
                    "required_adjusted_daily_bars": 2,
                    "required_derived_hourly_bars": 2,
                    "required_adjusted_intervals": {},
                    "declared_products": ["EQUITY_BARS"],
                },
            ),
            datetime(2026, 11, 2, 12, 0, tzinfo=UTC),
            EquityPreparationLimits(),
        )
    )


def test_warmup_accepts_recorded_sector_not_applicable_member():
    chunk = _warmup_request({
        "sector_ready": False,
        "sector_not_applicable": True,
        "corporate_actions_ready": True,
    })

    assert chunk.complete is True
    assert chunk.coverage_after["sector_ready"] is False
    assert chunk.coverage_after["sector_not_applicable"] is True


def test_warmup_keeps_sector_requirement_for_stocks_and_old_coverage():
    with pytest.raises(
        EquityPreparationUnavailableError,
        match="SECTOR_REFERENCE_UNAVAILABLE",
    ):
        _warmup_request({
            "sector_ready": False,
            "sector_not_applicable": False,
            "corporate_actions_ready": True,
        })
    with pytest.raises(
        EquityPreparationUnavailableError,
        match="SECTOR_REFERENCE_UNAVAILABLE",
    ):
        _warmup_request({
            "sector_ready": False,
            "corporate_actions_ready": True,
        })
    chunk = _warmup_request({
        "sector_ready": True,
        "sector_not_applicable": False,
        "corporate_actions_ready": True,
    })
    assert chunk.coverage_after["sector_ready"] is True
    assert chunk.coverage_after["sector_not_applicable"] is False


def test_preparation_chunk_enforces_request_byte_fact_and_watermark_bounds():
    request = EquityPreparationFetchRequest(
        _claim(),
        datetime(2026, 10, 30, 22, 0, tzinfo=UTC),
        EquityPreparationLimits(
            maximum_requests_per_chunk=1,
            maximum_response_bytes_per_chunk=10,
        ),
    )
    lineage = {
        "provider": "FAKE",
        "request_sha256": "a" * 64,
        "response_sha256": "b" * 64,
    }

    with pytest.raises(ValueError, match="bounded contract"):
        EquityPreparationChunk(
            EquityPreparationStage.NATIVE_INTRADAY,
            {"offset": 1},
            {"available_intervals": {}},
            lineage,
            False,
            provider_requests=2,
        ).validate(request)
    with pytest.raises(ValueError, match="advance its watermark"):
        EquityPreparationChunk(
            EquityPreparationStage.NATIVE_INTRADAY,
            {},
            {"available_intervals": {}},
            lineage,
            False,
        ).validate(request)


def test_preparation_coverage_is_monotonic_across_chunks():
    request = EquityPreparationFetchRequest(
        _claim(
            watermark={"offset": 6},
            coverage={"available_intervals": {"5m:RAW_UNADJUSTED": 6}},
        ),
        datetime(2026, 10, 30, 22, 0, tzinfo=UTC),
        EquityPreparationLimits(),
    )

    with pytest.raises(ValueError, match="cannot move backwards"):
        EquityPreparationChunk(
            EquityPreparationStage.NATIVE_INTRADAY,
            {"offset": 12},
            {"available_intervals": {"5m:RAW_UNADJUSTED": 5}},
            {
                "provider": "FAKE",
                "request_sha256": "a" * 64,
                "response_sha256": "b" * 64,
            },
            True,
        ).validate(request)
