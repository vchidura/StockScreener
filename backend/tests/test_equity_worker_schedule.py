import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts.run_equity_worker import (
    HEARTBEAT_SECONDS,
    ingest_due_interval,
    latest_completed_slot,
    latest_due_slot,
    mature_prospective_scanner_outcomes,
    parser,
    repair_recent_sessions,
    repair_session_bounds,
)
from equity.orchestration import IngestionCoverageError


UTC = timezone.utc


def test_30m_slot_uses_xnys_open_and_only_completed_window():
    assert latest_completed_slot(
        datetime(2026, 8, 28, 13, 59, tzinfo=UTC), "30m"
    ) == datetime(2026, 8, 27, 20, 0, tzinfo=UTC)
    assert latest_completed_slot(
        datetime(2026, 8, 28, 14, 0, tzinfo=UTC), "30m"
    ) == datetime(2026, 8, 28, 14, 0, tzinfo=UTC)


def test_15m_slot_and_weekend_handling():
    assert latest_completed_slot(
        datetime(2026, 8, 28, 13, 45, tzinfo=UTC), "15m"
    ) == datetime(2026, 8, 28, 13, 45, tzinfo=UTC)
    assert latest_completed_slot(
        datetime(2026, 8, 29, 15, 0, tzinfo=UTC), "15m"
    ) == datetime(2026, 8, 28, 20, 0, tzinfo=UTC)


def test_5m_slot_uses_only_completed_xnys_window():
    assert latest_completed_slot(
        datetime(2026, 8, 28, 13, 34, tzinfo=UTC), "5m"
    ) == datetime(2026, 8, 27, 20, 0, tzinfo=UTC)
    assert latest_completed_slot(
        datetime(2026, 8, 28, 13, 35, tzinfo=UTC), "5m"
    ) == datetime(2026, 8, 28, 13, 35, tzinfo=UTC)


def test_latest_due_slot_schedules_derived_intervals_at_completed_boundaries():
    assert latest_due_slot(
        datetime(2026, 8, 28, 14, 30, tzinfo=UTC), "1h"
    ) == datetime(2026, 8, 28, 14, 30, tzinfo=UTC)
    assert latest_due_slot(
        datetime(2026, 8, 28, 20, 0, tzinfo=UTC), "1d"
    ) == datetime(2026, 8, 28, 20, 0, tzinfo=UTC)
    assert latest_due_slot(
        datetime(2026, 8, 30, 20, 0, tzinfo=UTC), "1mo"
    ) == datetime(2026, 7, 31, 20, 0, tzinfo=UTC)


def test_latest_due_slot_respects_delayed_provider_watermark():
    delay = timedelta(minutes=15)

    assert latest_due_slot(
        datetime(2026, 8, 31, 13, 57, tzinfo=UTC),
        "5m",
        provider_delay=delay,
    ) == datetime(2026, 8, 31, 13, 40, tzinfo=UTC)
    assert latest_due_slot(
        datetime(2026, 8, 31, 13, 57, tzinfo=UTC),
        "15m",
        provider_delay=delay,
    ) == datetime(2026, 8, 28, 20, 0, tzinfo=UTC)
    assert latest_due_slot(
        datetime(2026, 8, 31, 14, 0, tzinfo=UTC),
        "15m",
        provider_delay=delay,
    ) == datetime(2026, 8, 31, 13, 45, tzinfo=UTC)


def test_latest_due_slot_catches_up_prior_session_after_utc_midnight():
    assert latest_due_slot(
        datetime(2026, 9, 2, 2, 30, tzinfo=UTC),
        "30m",
        provider_delay=timedelta(minutes=15),
    ) == datetime(2026, 9, 1, 20, 0, tzinfo=UTC)


def test_latest_due_slot_catches_up_prior_session_on_weekend():
    assert latest_due_slot(
        datetime(2026, 9, 5, 15, 0, tzinfo=UTC),
        "5m",
        provider_delay=timedelta(minutes=15),
    ) == datetime(2026, 9, 4, 20, 0, tzinfo=UTC)


def test_latest_due_slot_rejects_negative_provider_delay():
    try:
        latest_due_slot(
            datetime(2026, 8, 31, 14, 0, tzinfo=UTC),
            "5m",
            provider_delay=timedelta(minutes=-1),
        )
    except ValueError as exc:
        assert str(exc) == "provider_delay must not be negative"
    else:
        raise AssertionError("negative provider delay must fail")


def test_worker_once_flag_is_opt_in():
    assert parser().parse_args([]).once is False
    assert parser().parse_args(["--once"]).once is True
    assert parser().parse_args(["--repair-sessions", "3"]).repair_sessions == 3
    assert HEARTBEAT_SECONDS > 0


def test_repair_session_bounds_skip_weekends_and_holidays():
    assert repair_session_bounds(
        datetime(2026, 9, 8, 13, 35, tzinfo=UTC),
        3,
        provider_delay=timedelta(minutes=15),
    ) == (datetime(2026, 9, 2).date(), datetime(2026, 9, 4).date())


def test_repair_recent_sessions_refetches_native_and_bounds_derivations():
    from types import SimpleNamespace

    class Service:
        def __init__(self):
            self.native_calls = []
            self.derived_calls = []

        def ingest_native_interval(self, revisions, **kwargs):
            self.native_calls.append((revisions, kwargs))
            return SimpleNamespace(
                bar_count=10, inserted_count=0, missing_tickers=()
            )

        def derive_interval(self, revisions, **kwargs):
            self.derived_calls.append((revisions, kwargs))
            return SimpleNamespace(
                bar_count=10, inserted_count=0, missing_tickers=()
            )

    service = Service()
    now = datetime(2026, 9, 8, 21, 0, tzinfo=UTC)

    repair_recent_sessions(
        service,
        ("AAPL",),
        now=now,
        session_count=3,
        provider_delay=timedelta(minutes=15),
        intervals=("5m", "15m", "30m", "1h", "1d", "1wk", "1mo"),
    )

    assert [call[1]["interval"] for call in service.native_calls] == [
        "5m", "15m", "30m"
    ]
    assert all(call[1]["start"] == datetime(2026, 9, 3).date() for call in service.native_calls)
    assert all(call[1]["end"] == datetime(2026, 9, 8).date() for call in service.native_calls)
    assert [call[1]["target_interval"] for call in service.derived_calls] == [
        "1h", "1d", "1wk", "1mo"
    ]
    assert [call[1]["source_limit_per_ticker"] for call in service.derived_calls] == [
        39, 39, 8, 26
    ]
    assert all(call[1]["include_history"] is True for call in service.derived_calls)


def test_continuous_worker_retries_native_coverage_failure(caplog):
    class Service:
        def ingest_native_interval(self, *args, **kwargs):
            raise IngestionCoverageError(
                "native 5m ingestion coverage failed: 48/386 tickers unavailable"
            )

    slot = datetime(2026, 9, 2, 20, 45, tzinfo=UTC)

    assert ingest_due_interval(
        Service(), (), interval="5m", slot=slot, observed_at=slot, once=False
    ) is None
    assert "publication was skipped and the cycle will retry" in caplog.text


def test_worker_once_surfaces_native_coverage_failure():
    class Service:
        def ingest_native_interval(self, *args, **kwargs):
            raise IngestionCoverageError("provider coverage failed")

    slot = datetime(2026, 9, 2, 20, 45, tzinfo=UTC)

    with pytest.raises(IngestionCoverageError, match="provider coverage failed"):
        ingest_due_interval(
            Service(), (), interval="5m", slot=slot, observed_at=slot, once=True
        )


def test_early_close_never_creates_slot_after_session_close():
    assert latest_completed_slot(
        datetime(2026, 11, 27, 20, 0, tzinfo=UTC), "30m"
    ) == datetime(2026, 11, 27, 18, 0, tzinfo=UTC)


def test_worker_matures_both_composite_return_modes_prospectively():
    class Service:
        def __init__(self):
            self.calls = []

        def evaluate_directional_outcomes(self, policy, horizon_key, **kwargs):
            self.calls.append((policy, horizon_key, kwargs))
            return object()

    service = Service()
    available_by = datetime(2026, 9, 1, 20, 15, tzinfo=UTC)

    results = mature_prospective_scanner_outcomes(
        service, "1d", available_by=available_by
    )

    assert len(results) == 42
    assert len(service.calls) == 42
    assert {
        policy.policy_key.removesuffix(":SECTOR_PRIMARY").rsplit(":", 1)[-1]
        for policy, _, _ in service.calls
    } == {"SIGNED", "RECOMMENDATION_PLAN"}
    assert all(
        policy.policy_key.endswith(":SECTOR_PRIMARY")
        for policy, _, _ in service.calls
    )
    assert all(
        kwargs == {"available_by": available_by, "prospective_only": True}
        for _, _, kwargs in service.calls
    )


def test_worker_skips_scanner_outcomes_for_non_scanner_interval():
    class Service:
        def evaluate_directional_outcomes(self, *args, **kwargs):
            raise AssertionError("non-scanner interval must not evaluate outcomes")

    assert mature_prospective_scanner_outcomes(
        Service(), "15m", available_by=datetime(2026, 9, 1, tzinfo=UTC)
    ) == ()


class _StopWorkerLoop(Exception):
    pass


def _stub_worker_dependencies(monkeypatch, intervals):
    from types import SimpleNamespace

    import scripts.run_equity_worker as worker

    class _Analysis:
        def fail_stale_runs(self, *, stale_after):
            return ()

        def latest_published_market_times(self, requested):
            return {}

    class _Ingestion:
        def fail_stale_segments(self, *, stale_after):
            return ()

    monkeypatch.setattr(worker, "INTERVALS", intervals)
    monkeypatch.setattr(worker, "get_selected_tickers", lambda active_only=True: ("AAPL",))
    monkeypatch.setattr(worker, "EquityAnalysisRepository", _Analysis)
    monkeypatch.setattr(worker, "EquityIngestionRepository", _Ingestion)
    monkeypatch.setattr(worker, "PolygonEquityClient", lambda: object())
    monkeypatch.setattr(
        worker, "EquityMaterializationService",
        lambda client, native_fetch_workers=None: object(),
    )
    monkeypatch.setattr(
        worker, "load_or_refresh_reference",
        lambda *args, **kwargs: SimpleNamespace(revisions=(), universe_run_id="universe"),
    )
    monkeypatch.setattr(
        worker, "latest_due_slot",
        lambda now, interval, provider_delay=None: datetime(2026, 9, 8, 20, 0, tzinfo=UTC),
    )
    monkeypatch.setattr(
        worker.time, "sleep",
        lambda _seconds: (_ for _ in ()).throw(_StopWorkerLoop()),
    )
    return worker


def test_failing_interval_does_not_starve_later_intervals(monkeypatch):
    worker = _stub_worker_dependencies(monkeypatch, ("30m", "1h", "1d", "1wk"))
    processed = []

    def materialize(service, reference, *, interval, slot, observed_at, once):
        processed.append(interval)
        if interval == "1h":
            raise RuntimeError("hourly close bucket failed")
        return True

    monkeypatch.setattr(worker, "materialize_due_interval", materialize)

    with pytest.raises(_StopWorkerLoop):
        worker.run_worker()

    assert processed == ["30m", "1h", "1d", "1wk"]


def test_failed_interval_is_not_marked_complete_and_backs_off(monkeypatch):
    worker = _stub_worker_dependencies(monkeypatch, ("1h", "1d"))
    monkeypatch.setattr(worker, "INTERVAL_RETRY_SECONDS", 300)
    attempts = []

    def materialize(service, reference, *, interval, slot, observed_at, once):
        attempts.append(interval)
        if interval == "1h":
            raise RuntimeError("still failing")
        return True

    monkeypatch.setattr(worker, "materialize_due_interval", materialize)

    sleeps = {"count": 0}

    def sleep(_seconds):
        sleeps["count"] += 1
        if sleeps["count"] >= 2:
            raise _StopWorkerLoop()

    monkeypatch.setattr(worker.time, "sleep", sleep)

    with pytest.raises(_StopWorkerLoop):
        worker.run_worker()

    # 1h backs off after failing, 1d completes once and is not repeated.
    assert attempts == ["1h", "1d"]


def test_retryable_interval_result_backs_off_without_starving_later_interval(monkeypatch):
    worker = _stub_worker_dependencies(monkeypatch, ("1h", "1d"))
    monkeypatch.setattr(worker, "INTERVAL_RETRY_SECONDS", 300)
    attempts = []

    def materialize(service, reference, *, interval, slot, observed_at, once):
        attempts.append(interval)
        return interval != "1h"

    monkeypatch.setattr(worker, "materialize_due_interval", materialize)
    sleeps = {"count": 0}

    def sleep(_seconds):
        sleeps["count"] += 1
        if sleeps["count"] >= 2:
            raise _StopWorkerLoop()

    monkeypatch.setattr(worker.time, "sleep", sleep)

    with pytest.raises(_StopWorkerLoop):
        worker.run_worker()

    assert attempts == ["1h", "1d"]


def test_one_shot_reports_failure_after_attempting_later_intervals(monkeypatch):
    worker = _stub_worker_dependencies(monkeypatch, ("1h", "1d", "1wk"))
    attempts = []

    def materialize(service, reference, *, interval, slot, observed_at, once):
        attempts.append(interval)
        if interval == "1h":
            raise RuntimeError("hourly failed")
        return True

    monkeypatch.setattr(worker, "materialize_due_interval", materialize)

    with pytest.raises(RuntimeError, match="1h@.*RuntimeError"):
        worker.run_worker(once=True)

    assert attempts == ["1h", "1d", "1wk"]


def test_one_shot_reports_non_terminal_after_attempting_daily(monkeypatch):
    worker = _stub_worker_dependencies(monkeypatch, ("1h", "1d"))
    attempts = []

    def materialize(service, reference, *, interval, slot, observed_at, once):
        attempts.append(interval)
        return interval != "1h"

    monkeypatch.setattr(worker, "materialize_due_interval", materialize)

    with pytest.raises(RuntimeError, match="1h@.*NON_TERMINAL"):
        worker.run_worker(once=True)

    assert attempts == ["1h", "1d"]
