from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from urllib.parse import urlparse
from uuid import UUID

import pytest

from realtime.advanced_equity_preparation import (
    AdvancedEquityPreparationResident,
    BoundedPolygonEquityPreparationClient,
    EquityPreparationChunk,
    EquityPreparationClaim,
    EquityPreparationFetchRequest,
    EquityPreparationLimits,
    EquityPreparationSourceError,
    EquityPreparationStage,
    EquityPreparationUnavailableError,
    MassiveEquityPreparationSource,
    advanced_equity_live_priority_cursor,
    preparation_retry_delay_seconds,
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
            datetime(2025, 8, 24, 4, 0, tzinfo=UTC),
            date(2025, 8, 24),
            date(2025, 8, 24),
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
