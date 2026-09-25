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
    load_retained_reference,
    mature_prospective_scanner_outcomes,
    parser,
    refresh_behavior_adjusted_daily,
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
    assert parser().parse_args([]).behavior_shadow is False
    assert parser().parse_args(["--behavior-shadow"]).behavior_shadow is True
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


def test_canonical_publication_does_not_run_downstream_work():
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from scripts.run_equity_worker import publish_due_interval

    service = MagicMock()
    service.ingest_native_interval.return_value = SimpleNamespace(
        bar_count=1, inserted_count=1, missing_tickers=())
    publication = SimpleNamespace(status="COMPLETE", selected=1, missing=0)
    service.publish_canonical_interval.return_value = publication
    reference = SimpleNamespace(revisions=("AAPL",))
    slot = datetime(2026, 9, 21, 14, 0, tzinfo=UTC)
    assert publish_due_interval(service, reference, interval="30m", slot=slot,
        observed_at=slot + timedelta(minutes=15), once=False) is publication
    service.materialize_interval.assert_not_called()
    service.evaluate_directional_outcomes.assert_not_called()
    service.refresh_behavior_adjusted_daily.assert_not_called()


def test_stage_schedules_do_not_queue_or_share_completion():
    from scripts.equity_worker_stages import StageSchedule

    now = datetime(2026, 9, 21, 16, 0, tzinfo=UTC)
    native = StageSchedule(("5m", "30m"))
    analysis = StageSchedule(("5m", "30m"))
    assert analysis.next({"5m": now}, now) == ("5m", now)
    native.finish("5m", now, success=True, now=now)
    later = now + timedelta(minutes=5)
    assert native.next({"5m": later}, later) == ("5m", later)
    assert analysis.completed == {}
    native.finish("5m", later, success=False, now=later)
    assert native.next({"5m": later, "30m": now}, later) == ("30m", now)
    assert native.next({"5m": later + timedelta(minutes=5)}, later) == ("5m", later + timedelta(minutes=5))


def test_analysis_stage_never_ingests_or_matures_outcomes():
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from scripts.equity_worker_stages import analyze_published_interval, stage_commands

    service = MagicMock()
    service.materialize_interval.return_value.status = "COMPLETE"
    now = datetime(2026, 9, 21, 16, 0, tzinfo=UTC)
    assert analyze_published_interval(service, SimpleNamespace(revisions=("AAPL",), universe_run_id="universe"),
        interval="30m", slot=now, observed_at=now)
    service.ingest_native_interval.assert_not_called()
    service.evaluate_directional_outcomes.assert_not_called()
    assert set(stage_commands()) == {"native", "derived", "30m-analysis", "hourly-analysis", "short-analysis", "daily-analysis", "maintenance"}


def test_stage_publication_read_is_bounded_readonly_and_preserves_failed_latest(monkeypatch):
    from contextlib import contextmanager
    from unittest.mock import MagicMock
    from scripts import equity_worker_stages as stages

    cursor = MagicMock()
    cursor.fetchall.return_value = [{"interval": "30m", "market_time": datetime(2026, 9, 21, 16, 0, tzinfo=UTC),
        "observed_at": datetime(2026, 9, 21, 16, 15, tzinfo=UTC), "status": "FAILED"}]
    @contextmanager
    def connection():
        yield cursor
    monkeypatch.setattr(stages, "get_db_cursor", connection)
    assert stages.publication_watermarks(("30m",))["30m"]["status"] == "FAILED"
    queries = [call.args[0] for call in cursor.execute.call_args_list]
    assert queries[:2] == ["SET TRANSACTION READ ONLY", "SET LOCAL statement_timeout = '5s'"]
    assert "DISTINCT ON (interval)" in queries[-1]
    assert "status IN" not in queries[-1]


def test_stage_targets_require_canonical_dependencies_and_provider_delay(monkeypatch):
    from scripts import equity_worker_stages as stages

    now = datetime(2026, 9, 21, 14, 45, tzinfo=UTC)
    monkeypatch.setattr(stages.worker, "PROVIDER_DELAY_MINUTES", 15)
    assert stages.stage_targets("native", now, {})["30m"] == now - timedelta(minutes=15)
    assert stages.stage_targets("derived", now, {}) == {}
    source = {"30m": {"market_time": now - timedelta(minutes=15), "status": "COMPLETE"}}
    assert stages.stage_targets("derived", now, source)["1h"] == now - timedelta(minutes=15)
    assert stages.stage_targets("30m-analysis", now, source)["30m"] == source["30m"]["market_time"]
    source["30m"]["status"] = "FAILED"
    assert "30m" not in stages.stage_targets("30m-analysis", now, source)
    assert "1h" not in stages.stage_targets("derived", now, source)
    monkeypatch.setattr(stages.worker, "PROVIDER_DELAY_MINUTES", 0)
    assert stages.stage_targets("native", now, {})["15m"] == now


def _stub_stage_loop(monkeypatch, *, iterations=3):
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from scripts import equity_worker_stages as stages

    clock = [datetime(2026, 9, 21, 16, 15, tzinfo=UTC)]
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0]
    service = MagicMock()
    service.analysis_repository.latest_published_market_times.return_value = {}
    service.evaluate_directional_outcomes.return_value = SimpleNamespace(due=1, persisted=1, pending=0)
    reference = SimpleNamespace(revisions=("AAPL",), universe_run_id="universe")
    monkeypatch.setattr(stages, "datetime", Clock)
    monkeypatch.setattr(stages, "check_stage_parent", lambda: None)
    monkeypatch.setattr(stages, "EquityMaterializationService", lambda *args, **kwargs: service)
    monkeypatch.setattr(stages, "PolygonEquityClient", lambda: object())
    monkeypatch.setattr(stages.worker, "INTERVALS", ("5m", "30m", "1h", "1d"))
    monkeypatch.setattr(stages.worker, "PROVIDER_DELAY_MINUTES", 15)
    monkeypatch.setattr(stages.worker, "get_selected_tickers", lambda **kwargs: ("AAPL",))
    monkeypatch.setattr(stages.worker, "load_or_refresh_reference", lambda *args: reference)
    monkeypatch.setattr(stages.worker, "load_retained_reference", lambda *args: reference)
    monkeypatch.setattr(stages.worker, "behavior_shadow_tickers", lambda: ("AAPL",))
    monkeypatch.setattr(stages, "publication_watermarks", lambda intervals: {interval: dict(
        market_time=datetime(2026, 9, 21, 16, 0, tzinfo=UTC), status="COMPLETE") for interval in intervals})
    states = []
    monkeypatch.setattr(stages, "write_stage_status", lambda stage, state: states.append(dict(state)))
    sleeps = []
    def advance(seconds):
        sleeps.append(seconds)
        if len(sleeps) >= iterations:
            raise _StopWorkerLoop()
        clock[0] += timedelta(seconds=15)
    monkeypatch.setattr(stages.time, "sleep", advance)
    return stages, service, states


def test_native_stage_continues_with_downstream_unavailable_and_current_job_clock(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    stages, service, states = _stub_stage_loop(monkeypatch)
    monkeypatch.setattr(stages, "publication_watermarks", lambda intervals: {})
    publish = MagicMock(return_value=SimpleNamespace(status="COMPLETE"))
    monkeypatch.setattr(stages.worker, "publish_due_interval", publish)
    with pytest.raises(_StopWorkerLoop):
        stages.run_stage("native")
    assert [call.kwargs["interval"] for call in publish.call_args_list] == ["30m", "5m"]
    assert publish.call_args_list[1].kwargs["observed_at"] > publish.call_args_list[0].kwargs["observed_at"]
    service.materialize_interval.assert_not_called()
    service.evaluate_directional_outcomes.assert_not_called()
    service.ingestion_repository.fail_stale_segments.assert_called_once_with(
        stale_after=timedelta(0), intervals=("30m", "5m"), dataset="EQUITY_BARS")
    assert any(state["state"] == "BUSY" for state in states)


@pytest.mark.parametrize("failure", ["exception", "unavailable"])
def test_maintenance_failure_backs_off_without_starving_outcomes(monkeypatch, failure):
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    stages, service, states = _stub_stage_loop(monkeypatch)
    refresh = MagicMock(side_effect=RuntimeError("unavailable")) if failure == "exception" else MagicMock(
        return_value=(SimpleNamespace(status="IDENTITY_UNAVAILABLE"),))
    context = MagicMock(return_value={"status": "ALREADY_PRESENT"})
    monkeypatch.setattr(stages.worker, "refresh_behavior_adjusted_daily", refresh)
    monkeypatch.setattr(stages.worker, "refresh_current_daily_signals", context, raising=False)
    with pytest.raises(_StopWorkerLoop):
        stages.run_stage("maintenance", behavior_shadow=True)
    refresh.assert_called_once()
    context.assert_not_called()
    assert service.evaluate_directional_outcomes.call_count == 2
    assert service.evaluate_directional_outcomes.call_args.kwargs["limit"] == 100
    assert service.evaluate_directional_outcomes.call_args.kwargs["prospective_only"] is True
    service.materialize_interval.assert_not_called()


def test_stage_requires_supervisor_identity(monkeypatch):
    from scripts.equity_worker_stages import check_stage_parent

    monkeypatch.delenv("EQUITY_STAGE_PARENT_PID", raising=False)
    monkeypatch.delenv("EQUITY_STAGE_PARENT_CREATED", raising=False)
    with pytest.raises(RuntimeError, match="managed supervisor"):
        check_stage_parent()


def test_stage_status_resolves_venv_child_identity_and_rejects_unrelated_pid(monkeypatch, tmp_path):
    import json
    from types import SimpleNamespace
    from scripts import equity_worker_stages as stages

    now = datetime.now(UTC).isoformat()
    (tmp_path / "service.json").write_text(json.dumps(dict(pid=1, process_created_at=1,
        checked_at=now, state="RUNNING", components={"native": dict(pid=2, state="RUNNING", started_at=now)})))
    (tmp_path / "native.json").write_text(json.dumps(dict(pid=3, stage="native", state="BUSY", checked_at=now)))
    monkeypatch.setattr(stages, "ROOT", tmp_path)
    process = SimpleNamespace(pid=3, create_time=lambda: 1, parents=lambda: [SimpleNamespace(pid=2)],
        cmdline=lambda: ["python", "run_equity_worker.py", "--stage", "native"])
    monkeypatch.setattr("psutil.Process", lambda identity: process)
    assert stages.status()["stages"]["native"]["state"] == "BUSY"
    process.parents = lambda: [SimpleNamespace(pid=4)]
    assert stages.status()["stages"]["native"]["state"] == "UNVERIFIED"


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


def test_behavior_adjusted_refresh_uses_latest_completed_daily_session():
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    service = MagicMock()
    service.behavior_shadow_enabled = True
    reference = SimpleNamespace(revisions=("AAPL",))
    observed_at = datetime(2026, 9, 18, 14, 0, tzinfo=UTC)

    refresh_behavior_adjusted_daily(
        service, reference, observed_at=observed_at,
    )

    call = service.refresh_behavior_adjusted_daily.call_args
    assert call.args == (("AAPL",),)
    assert call.kwargs["session_date"] == datetime(2026, 9, 17).date()
    assert call.kwargs["observed_at"] >= observed_at

    service.reset_mock()
    results = refresh_behavior_adjusted_daily(
        service, reference, observed_at=observed_at, session_count=10,
    )
    assert len(results) == service.refresh_behavior_adjusted_daily.call_count == 10
    dates = [call.kwargs["session_date"] for call in service.refresh_behavior_adjusted_daily.call_args_list]
    assert dates[0] == datetime(2026, 9, 3).date()
    assert dates[-1] == datetime(2026, 9, 17).date()


def test_behavior_bootstrap_reference_read_never_calls_provider(monkeypatch):
    import scripts.run_equity_worker as worker

    class ReferenceRepository:
        def list_securities_as_of(self, tickers, watermark):
            return ()

    class UniverseRepository:
        def get_latest_as_of(self, watermark):
            return None

    monkeypatch.setattr(worker, "EquityReferenceRepository", ReferenceRepository)
    monkeypatch.setattr(worker, "EquityUniverseRepository", UniverseRepository)

    assert load_retained_reference(("AAPL",), datetime(2026, 9, 18, tzinfo=UTC)) is None


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
    monkeypatch.setattr(worker, "refresh_current_daily_signals", lambda **kwargs: {"status": "ALREADY_PRESENT"}, raising=False)
    monkeypatch.setattr(worker, "get_selected_tickers", lambda active_only=True: ("AAPL",))
    monkeypatch.setattr(worker, "EquityAnalysisRepository", _Analysis)
    monkeypatch.setattr(worker, "EquityIngestionRepository", _Ingestion)
    monkeypatch.setattr(worker, "PolygonEquityClient", lambda: object())
    monkeypatch.setattr(
        worker, "EquityMaterializationService",
        lambda client, **kwargs: object(),
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


@pytest.mark.parametrize("context_status", ["PUBLISHED", "BUSY", "WAITING_FOR_COMPLETE_DAILY_PUBLICATION", "exception"])
def test_daily_analysis_never_requires_legacy_context(monkeypatch, context_status):
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    import scripts.run_equity_worker as worker

    service = MagicMock()
    service.publish_canonical_interval.return_value = SimpleNamespace(
        status="COMPLETE", selected=1, missing=0,
    )
    service.materialize_interval.return_value = SimpleNamespace(status="COMPLETE")
    monkeypatch.setattr(worker, "ingest_due_interval", lambda *args, **kwargs: SimpleNamespace(
        bar_count=1, inserted_count=1, missing_tickers=(),
    ))
    monkeypatch.setattr(worker, "mature_prospective_scanner_outcomes", lambda *args, **kwargs: ())

    refresh = MagicMock(side_effect=RuntimeError("legacy unavailable")) if context_status == "exception" else MagicMock(return_value={"status": context_status})
    monkeypatch.setattr(worker, "refresh_current_daily_signals", refresh, raising=False)
    now = datetime(2026, 9, 8, 20, 0, tzinfo=UTC)
    finished = worker.materialize_due_interval(
        service, SimpleNamespace(revisions=(object(),), universe_run_id="universe"),
        interval="1d", slot=now, observed_at=now, once=True,
    )
    assert finished is True
    service.materialize_interval.assert_called_once()
    refresh.assert_not_called()
    from scripts.equity_worker_stages import analyze_published_interval
    assert analyze_published_interval(service, SimpleNamespace(revisions=(object(),), universe_run_id="universe"),
        interval="1d", slot=now, observed_at=now)
    refresh.assert_not_called()


@pytest.mark.parametrize("failure", ["BUSY", "exception"])
def test_retired_context_is_not_invoked_by_one_shot(monkeypatch, failure):
    from unittest.mock import MagicMock
    worker = _stub_worker_dependencies(monkeypatch, ("1d",))
    materialize = MagicMock(return_value=True)
    refresh = MagicMock(side_effect=RuntimeError("legacy unavailable")) if failure == "exception" else MagicMock(return_value={"status": failure})
    monkeypatch.setattr(worker, "materialize_due_interval", materialize)
    monkeypatch.setattr(worker, "refresh_current_daily_signals", refresh)
    worker.run_worker(once=True)
    materialize.assert_called_once()
    refresh.assert_not_called()


def test_behavior_adjusted_refresh_failure_never_blocks_legacy_analysis(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    import scripts.run_equity_worker as worker

    service = MagicMock()
    service.behavior_shadow_enabled = True
    service.publish_canonical_interval.return_value = SimpleNamespace(
        status="COMPLETE", selected=1, missing=0,
    )
    service.refresh_behavior_adjusted_daily.side_effect = RuntimeError("adjusted unavailable")
    service.materialize_interval.return_value = SimpleNamespace(status="COMPLETE")
    monkeypatch.setattr(worker, "ingest_due_interval", lambda *args, **kwargs: SimpleNamespace(
        bar_count=1, inserted_count=1, missing_tickers=(),
    ))
    monkeypatch.setattr(worker, "mature_prospective_scanner_outcomes", lambda *args, **kwargs: ())
    slot = datetime(2026, 9, 18, 14, 0, tzinfo=UTC)

    finished = worker.materialize_due_interval(
        service, SimpleNamespace(revisions=(object(),), universe_run_id="universe"),
        interval="30m", slot=slot, observed_at=slot, once=True,
    )

    assert finished is True
    service.materialize_interval.assert_called_once()


def test_worker_does_not_restart_retired_context_when_analysis_is_current(monkeypatch):
    from unittest.mock import MagicMock

    worker = _stub_worker_dependencies(monkeypatch, ("1d",))
    slot = datetime(2026, 9, 8, 20, 0, tzinfo=UTC)
    monkeypatch.setattr(
        worker.EquityAnalysisRepository, "latest_published_market_times",
        lambda self, intervals: {"1d": slot},
    )
    refresh = MagicMock(return_value={"status": "PUBLISHED"})
    materialize = MagicMock()
    monkeypatch.setattr(worker, "refresh_current_daily_signals", refresh)
    monkeypatch.setattr(worker, "materialize_due_interval", materialize)

    worker.run_worker(once=True)

    refresh.assert_not_called()
    materialize.assert_not_called()


def test_retired_context_is_not_invoked_while_intervals_continue(monkeypatch):
    from unittest.mock import MagicMock

    worker = _stub_worker_dependencies(monkeypatch, ("1d", "1wk"))
    materialize = MagicMock(return_value=True)
    refresh = MagicMock(side_effect=RuntimeError("context failed"))
    monkeypatch.setattr(worker, "materialize_due_interval", materialize)
    monkeypatch.setattr(worker, "refresh_current_daily_signals", refresh)
    sleeps = {"count": 0}

    def sleep(_seconds):
        sleeps["count"] += 1
        if sleeps["count"] == 2:
            raise _StopWorkerLoop()

    monkeypatch.setattr(worker.time, "sleep", sleep)
    with pytest.raises(_StopWorkerLoop):
        worker.run_worker()

    assert materialize.call_count == 2
    refresh.assert_not_called()


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
