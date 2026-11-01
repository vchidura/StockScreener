from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from realtime.advanced_equity_preparation import (
    AdvancedEquityPreparationResident,
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
